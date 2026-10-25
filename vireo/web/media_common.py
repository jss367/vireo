"""Shared media source selection, publication guards and response helpers.

Domain routes import these primitives; this module imports no route owners."""

from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import os
import sqlite3
from collections.abc import Iterator, Mapping
from typing import TYPE_CHECKING, BinaryIO

from app_paths import media_root
from camera_denoise import render_cache_fields as _camera_render_cache_fields
from flask import (
    Response,
    make_response,
    request,
)

if TYPE_CHECKING:
    from sqlite3 import Row

    from db import Database
    from PIL import Image
    from services.service_types import JsonDict
    from web.web_types import AppConfig, GetDb


log = logging.getLogger(__name__)


# None is a meaningful captured recipe; this sentinel means no recipe guard.
_UNSET = object()


class _ArtifactResponseError(RuntimeError):
    """Carry a producer's non-success Flask response to equal-key waiters."""

    def __init__(self, response: Response) -> None:
        super().__init__(f"artifact producer returned HTTP {response.status_code}")
        self.status_code = response.status_code
        self.data = response.get_data()
        self.mimetype = response.mimetype

    def to_response(self) -> Response:
        return Response(
            self.data, status=self.status_code, mimetype=self.mimetype,
        )


def _shed_prefetch_response() -> Response:
    """Return a non-cacheable empty response for declined speculative work."""
    response = Response(status=204)
    response.headers["Cache-Control"] = "no-store"
    return response


_PAIRED_PREVIEW_DIRNAME = "paired"


_PAIRED_PREVIEW_TTL_SEC = 15 * 60


def _paired_render_state_hash(
    photo: Row, size: int, pair_source: str, pair_source_path: str, recipe: JsonDict | None,
) -> str:
    """Signature hash for a paired-source render's shadow cache filename.

    Includes the paired source file's mtime/size, the recipe (whose effect
    depends on the RAW render), the edit-math version, and the request size
    so a swapped source file or an updated recipe yields a distinct
    filename. Recipe invalidation does not remove these shadow files and
    their TTL sweep only fires on the next publish, so keying by state is
    what prevents the shadow cache from returning bytes rendered from a
    prior state.
    """
    from edit_recipe import recipe_to_json
    from image_edits import EDIT_MATH_VERSION

    source_state = None
    if pair_source_path:
        try:
            stat = os.stat(pair_source_path)
        except OSError:
            source_state = None
        else:
            source_state = {
                "size": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
            }
    signature = json.dumps(
        {
            "photo_id": int(photo["id"]),
            "size": size,
            "pair_source": pair_source,
            "source_state": source_state,
            "recipe": recipe_to_json(recipe) if recipe else None,
            "edit_math_version": EDIT_MATH_VERSION,
            **_camera_render_cache_fields(photo, recipe),
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(signature).hexdigest()[:16]


def _send_opened_file(handle: BinaryIO, path: str, mimetype: str) -> Response:
    """``send_file`` an already-open ``handle`` with the validators of ``path``.

    A response sent from a file object carries no ETag or Last-Modified:
    Werkzeug derives both from a path. Routes that must open the file under
    a lock (so eviction cannot unlink it before the response has an fd)
    still owe the client the validators a later ``send_file(path)`` of the
    same file sends, or the first copy a browser gets can never be
    revalidated. Build them the way Werkzeug does for a path: Last-Modified
    is the file's mtime and the ETag is ``"<mtime>-<size>-<adler32 of the
    path>"``, read from the open handle, so the cold and warm responses
    for one file agree and a revalidation of either is a 304.
    """
    from zlib import adler32

    from flask import current_app, send_file

    stat = os.fstat(handle.fileno())
    # Flask's send_file joins a path onto the app root before Werkzeug
    # hashes it; an absolute path is left as it is.
    sent_path = os.path.join(current_app.root_path, path)
    check = adler32(sent_path.encode()) & 0xFFFFFFFF
    return send_file(
        handle,
        mimetype=mimetype,
        etag=f"{stat.st_mtime}-{stat.st_size}-{check}",
        last_modified=stat.st_mtime,
    )


def _source_changed_response() -> _ArtifactResponseError:
    return _ArtifactResponseError(make_response(("Photo source changed", 404)))


@contextlib.contextmanager
def _preparation_publication(
    db: Database, photo_id: int, prepare_source: Row | None, captured_recipe: object = _UNSET,
) -> Iterator[None]:
    """Hold a catalog-writer transaction while a preparation render publishes.

    Refuses late preparation writes after deletion/reimport, so a recycled
    photo ID never receives the previous owner's pixels. Decode and encode
    stay outside this short transaction. ``prepare_source=None`` (an
    interactive request) publishes unguarded.

    ``captured_recipe`` is the edit recipe the render decoded against. When
    passed, publication is also refused if the stored recipe has changed
    since — a user save mid-render invalidated the cache we would overwrite,
    and the ordinary preview filename is not recipe-keyed, so publishing the
    old pixels would serve a different recipe at the next lightbox hit.
    """
    if prepare_source is None:
        yield
        return
    from offline_cache import photo_source_matches

    check_recipe = captured_recipe is not _UNSET

    def check_source() -> None:
        if not photo_source_matches(prepare_source, db.photos.get(photo_id)):
            raise _source_changed_response()
        if check_recipe and db.get_photo_edit_recipe(photo_id) != captured_recipe:
            raise _source_changed_response()

    if db.in_transaction:
        check_source()
        yield
    else:
        with db.transaction():
            db.begin_immediate()
            check_source()
            yield


def _requested_pair_source(
    get_db: GetDb, config: AppConfig, photo: Row, folder_path: str,
) -> tuple[str | None, str | None]:
    """Resolve an explicit RAW/JPEG display choice for a paired photo.

    Paired files remain one catalog record, but browser image requests can
    select which physical source supplies the pixels with
    ``?source=jpeg`` or ``?source=raw``. Invalid and non-paired choices
    fall back to the established canonical rendering path so old clients
    and ordinary photos are unchanged. Live files are preferred, with the
    offline-original cache as the fallback; a known pair missing both
    returns the source name with no path so routes can fail explicitly
    instead of showing RAW pixels under a JPEG label.
    """
    requested = (request.args.get("source") or "").strip().lower()
    if requested not in {"jpeg", "raw"}:
        return None, None

    from image_loader import RAW_EXTENSIONS

    primary_ext = os.path.splitext(photo["filename"])[1].lower()
    if primary_ext not in RAW_EXTENSIONS or not photo["companion_path"]:
        return None, None

    def _offline_path(column: str) -> str | None:
        try:
            row = get_db().caches.offline_original_get(photo["id"])
        except sqlite3.Error:
            log.warning("Could not read offline original for photo %s", photo["id"], exc_info=True)
            return None
        if not row or not row[column]:
            return None
        cached = row[column]
        if not os.path.isabs(cached):
            vireo_dir = media_root(config["THUMB_CACHE_DIR"])
            cached = os.path.join(vireo_dir, cached)
        return cached if os.path.isfile(cached) else None

    if requested == "raw":
        primary = os.path.join(folder_path, photo["filename"])
        if os.path.isfile(primary):
            return "raw", primary
        return "raw", _offline_path("original_path")

    companion = os.path.join(folder_path, photo["companion_path"])
    companion_ext = os.path.splitext(companion)[1].lower()
    if companion_ext not in {".jpg", ".jpeg"}:
        return "jpeg", None
    if os.path.isfile(companion):
        return "jpeg", companion
    return "jpeg", _offline_path("companion_path")


def _comparison_media_subject(
    db: Database, photo: JsonDict | Row, args: Mapping[str, str], effective: JsonDict,
) -> tuple[JsonDict, JsonDict]:
    """Validate selected-subject media without falling back to primary evidence."""
    from focus_crop import valid_subject_box
    from subject_ranking import SubjectComparison, cached_analysis, resolve_subject
    try:
        detection_id = int(args.get("detection_id", ""))
    except (TypeError, ValueError):
        raise ValueError("Invalid detection_id") from None
    detection, _ = resolve_subject(db, photo["id"], SubjectComparison(subject_overrides={photo["id"]: detection_id}))
    if detection is None:
        raise ValueError("Selected subject identity is unresolved; refresh the comparison")

    if not valid_subject_box(detection):
        raise ValueError("Selected subject coordinates are invalid")
    key, analysis = cached_analysis(db, photo, detection, effective)
    requested_key = args.get("analysis_key")
    if not requested_key or analysis is None or key != requested_key:
        raise ValueError("Selected subject analysis is unavailable or stale; refresh the comparison")
    return detection, analysis


def _focus_source_image(
    config: AppConfig, photo: JsonDict, folder: JsonDict | Row | None, source_choice: str,
) -> tuple[Image.Image | None, str | None]:
    """``(image, source)`` the focus crop is cut from, or ``(None, None)``.

    ``working`` requires the working copy, with no original fallback.
    ``auto`` permits original fallback. A working copy is capped at
    ``working_copy_max_size``, so on a large sensor its pixels are not
    the original's: when the reviewer asked for ``original``, or left it
    to ``auto`` and the copy is downsized against the catalog
    dimensions, the original is decoded instead (JPEG-first, the
    embedded full-size JPEG of a RAW) and used when it is larger.
    """
    from focus_crop import prefer_original
    from image_loader import load_image, load_working_image

    folders = {photo["folder_id"]: folder["path"]} if folder else {}
    original_path = os.path.join(folder["path"], photo["filename"]) if folder else None
    img, source = None, None
    if source_choice != "original":
        img, source = load_working_image(
            dict(photo), media_root(config["THUMB_CACHE_DIR"]), max_size=None,
            folders=None if source_choice == "working" else folders,
            record_access=True, return_source=True,
        )
    wants_original = source_choice == "original" or (
        source_choice == "auto" and img is not None and source == "working_copy"
        and prefer_original(img.size, photo)
    )
    if wants_original and original_path and os.path.isfile(original_path):
        original = load_image(original_path, max_size=None)
        if original is not None and (img is None or max(original.size) > max(img.size)):
            img, source = original, "original"
    return img, source
