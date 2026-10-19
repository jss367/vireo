"""Image and mask serving.

The browser-facing pixel routes: ``/thumbnails/<filename>``, the per-photo
``/photos/<id>/...`` renders (``crop``, ``full``, ``preview``,
``edit-mask-preview``, ``edit-preview``, ``original``), and the SAM mask PNGs
(``/masks/<filename>`` and ``/api/masks/<pid>/<variant>.png``) with the
per-photo variant listing that points at them (``/api/photos/<pid>/masks``).
Every route answers only for photos in the active workspace.

The helpers only these routes use live here too: the paired RAW/JPEG source
selection (``?source=jpeg|raw``) and its source-keyed renders, the
equal-key artifact-flight response carrier, the full-resolution render cache
paths and mtime pegging, and the working-copy cache-hit sender.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import math
import os
import re
import sqlite3
import tempfile
import time

from artifact_flight import (
    ArtifactProducerFailed,
    atomic_write_bytes,
    original_artifact_flights,
    preview_artifact_flights,
    preview_prefetch_slots,
)
from camera_denoise import cache_matches as _camera_cache_matches
from camera_denoise import render_cache_fields as _camera_render_cache_fields
from file_replace import replace_file
from flask import (
    Blueprint,
    Response,
    jsonify,
    make_response,
    request,
    send_from_directory,
)
from preview_cache import PREVIEW_TIER_SIZES, paired_preview_ready
from preview_cache import (
    evict_if_over_quota as evict_preview_cache_if_over_quota,
)
from preview_materializer import (
    PreviewMaterializationError,
    materialize_preview,
)
from render_source import (
    companion_image_can_replace_raw_result as _companion_image_can_replace_raw_result,
)
from render_source import (
    has_current_working_copy_failure as _has_current_working_copy_failure,
)
from render_source import (
    image_is_smaller_than_expected as _image_is_smaller_than_expected,
)
from render_source import (
    image_size_after_exif_orientation as _image_size_after_exif_orientation,
)
from render_source import (
    recipe_render_source as _recipe_render_source,
)
from render_source import (
    recipe_source_dimensions as _recipe_source_dimensions,
)
from render_source import (
    record_working_copy_failure as _record_working_copy_failure,
)
from render_source import (
    scaled_recipe_source_dimensions as _scaled_recipe_source_dimensions,
)
from services.original_photo import (
    EditRenderSource,
    OriginalExtraction,
    OriginalPhoto,
    UneditedSource,
)
from services.original_photo import (
    peg_display_cache_mtime as _peg_display_cache_mtime,
)
from services.retained_cache import reader_guard
from web.responses import photo_not_found_error
from working_copy_cache import (
    evict_if_over_quota as evict_working_copy_cache_if_over_quota,
)
from working_copy_cache import (
    touch_working_copy_access,
    working_copy_publication_guard,
    working_copy_quota_bytes,
)

log = logging.getLogger(__name__)


# Sentinel for "argument not provided" where None is a meaningful recipe value
# (an unedited photo has recipe=None). Lets _preparation_publication tell "no
# recipe guard requested" apart from "the captured recipe was None".
_UNSET = object()


# ``serve_crop_preview``'s source loader returns this when the photo has no
# usable working copy and its folder row is gone (a 404, not a decode failure).
_CROP_FOLDER_MISSING = object()


class _ArtifactResponseError(RuntimeError):
    """Carry a producer's non-success Flask response to equal-key waiters."""

    def __init__(self, response):
        super().__init__(f"artifact producer returned HTTP {response.status_code}")
        self.status_code = response.status_code
        self.data = response.get_data()
        self.mimetype = response.mimetype

    def to_response(self):
        return Response(
            self.data, status=self.status_code, mimetype=self.mimetype,
        )


def _shed_prefetch_response():
    """Return a non-cacheable empty response for declined speculative work."""
    response = Response(status=204)
    response.headers["Cache-Control"] = "no-store"
    return response


# Paired previews use source-state filenames and their own cache index, while
# sharing the ordinary preview disk quota. Full-resolution paired RAW renders
# still use transient artifacts for request coordination.
_PAIRED_PREVIEW_DIRNAME = "paired"
_PAIRED_PREVIEW_TTL_SEC = 15 * 60


def _paired_preview_dir(preview_dir):
    return os.path.join(preview_dir, _PAIRED_PREVIEW_DIRNAME)


def _paired_render_state_hash(
    photo, size, pair_source, pair_source_path, recipe,
):
    """Signature hash for a paired-source render's shadow cache filename.

    Includes the paired source file's mtime/size, the recipe (whose effect
    depends on the RAW render), the edit-math version, and the request size
    so a swapped source file or an updated recipe yields a distinct
    filename. Recipe invalidation does not remove these shadow files and
    their TTL sweep only fires on the next publish, so keying by state is
    what prevents the shadow cache from returning bytes rendered from a
    prior state.
    """
    from image_edits import EDIT_MATH_VERSION, recipe_to_json

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


def _paired_preview_path(
    preview_dir, photo_id, size, pair_source, state_hash,
):
    return os.path.join(
        _paired_preview_dir(preview_dir),
        f"{photo_id}_{size}_{pair_source}_{state_hash}.jpg",
    )


def _paired_original_dir(vireo_dir):
    return os.path.join(vireo_dir, "originals", _PAIRED_PREVIEW_DIRNAME)


def _paired_original_path(vireo_dir, photo_id, pair_source, state_hash):
    return os.path.join(
        _paired_original_dir(vireo_dir),
        f"{photo_id}_{pair_source}_{state_hash}.jpg",
    )


def _fresh_paired_artifact(path):
    """Return whether a non-empty paired artifact is still inside its TTL."""
    try:
        return (
            os.path.getsize(path) > 0
            and os.path.getmtime(path) >= time.time() - _PAIRED_PREVIEW_TTL_SEC
        )
    except OSError:
        return False


def _sweep_stale_paired_previews(paired_dir):
    """Best-effort removal of paired-preview cache files older than the TTL.

    Paired renders are only kept around long enough for a browser warmup
    and the follow-up visible request to share one decode; anything older
    is dead weight, so we sweep before publishing a new file rather than
    growing an unbounded shadow cache.
    """
    if not os.path.isdir(paired_dir):
        return
    cutoff = time.time() - _PAIRED_PREVIEW_TTL_SEC
    try:
        entries = os.listdir(paired_dir)
    except OSError:
        return
    for name in entries:
        path = os.path.join(paired_dir, name)
        try:
            if os.path.getmtime(path) < cutoff:
                os.remove(path)
        except OSError:
            continue


def _is_preview_cache_invalid(db, photo_id, size):
    return db.caches.preview_invalidated(photo_id, size)

def _full_resolution_render_signature(photo, recipe, file_state=None):
    """Describe every catalogued input to a cached inspection render.

    The source size/mtime pair is refreshed by scans, the canonical recipe
    JSON includes content-addressed local-mask references, and the edit-math
    version invalidates bytes produced by older rendering code. Hashing the
    signature into the JPEG filename lets /original safely reuse prepared
    renders without a database table or cross-recipe cache race.
    """
    from image_edits import EDIT_MATH_VERSION, recipe_to_json

    return {
        "photo_id": int(photo["id"]),
        "source_size": photo["file_size"],
        "source_mtime": photo["file_mtime"],
        "filename": photo["filename"],
        "companion_path": photo["companion_path"],
        "file_state": file_state,
        "recipe": recipe_to_json(recipe),
        "edit_math_version": EDIT_MATH_VERSION,
        **_camera_render_cache_fields(photo, recipe),
    }

def _full_resolution_render_path(
    vireo_dir, photo, recipe, file_state=None,
):
    signature = json.dumps(
        _full_resolution_render_signature(
            photo, recipe, file_state,
        ),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    cache_key = hashlib.sha256(signature).hexdigest()[:16]
    return os.path.join(
        vireo_dir, "originals", f"{photo['id']}_{cache_key}.jpg",
    )

def _peg_render_mtime_to_source(cache_path, photo):
    """Align a freshly written render's mtime to the photo's source.

    ``_prepared_full_resolution_render`` rejects renders whose mtime
    predates ``photos.file_mtime``. Renders are written with the wall
    clock, so a source whose ``file_mtime`` is in the *future* — clock
    skew on the machine that wrote it, archives that preserve future
    timestamps — would fail that gate immediately and re-render on
    every single request. ``serve_thumbnail`` already documents and
    guards this exact trap; the cost here is a full-resolution decode
    plus edit pipeline per request, so it matters more.

    Use this for *signature-keyed* prepared renders only — the
    signature already encodes the recipe and edit-math version, so
    pegging to ``photos.file_mtime`` alone is enough. For the unedited
    RAW display cache, use :func:`_peg_display_cache_mtime` instead;
    that cache-hit check compares the file against
    ``max(mtime(source), mtime(companion))``, so pegging to the row's
    ``file_mtime`` here would fail the check on every request when a
    paired companion has a later mtime than the RAW row.
    """
    source_mtime = photo["file_mtime"] if photo is not None else None
    if source_mtime is None:
        return
    with contextlib.suppress(OSError, TypeError, ValueError):
        os.utime(cache_path, (float(source_mtime), float(source_mtime)))

def _serve_trusted_working_copy(path):
    """Serve a working-copy cache hit and stamp it as recently used.

    Callers hold ``working_copy_publication_guard`` — the same lock the
    quota pass takes — so the stamp cannot land between eviction's
    directory scan and its unlink.
    """
    from flask import send_file

    touch_working_copy_access(path)
    return send_file(path, mimetype="image/jpeg")


def _send_opened_file(handle, path, mimetype):
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

def _prepared_full_resolution_render(
    vireo_dir, photo, recipe, file_state=None,
):
    cache_path = _full_resolution_render_path(
        vireo_dir, photo, recipe, file_state,
    )
    try:
        if not os.path.isfile(cache_path) or os.path.getsize(cache_path) <= 0:
            return None
        # Same freshness gate ``serve_thumbnail`` uses. The signature
        # hash already covers the normal invalidation case (a scan that
        # updates ``file_mtime`` computes a different cache path), so
        # this check is a no-op for legitimate renders — their mtime is
        # the write time, which is by definition ``>= file_mtime``.
        # It exists to reject a *surviving* render left over when a
        # rowid is recycled and the new photo's signature happens to
        # collide with a previous owner's: ``purge_cached_files_for_
        # recycled_id`` backdates any undeletable survivor to mtime 0,
        # and without this check ``/original`` would happily send the
        # previous owner's pixels since the existence-and-size probe
        # can't distinguish them from a fresh render.
        source_mtime = photo["file_mtime"]
        if (
            source_mtime is not None
            and os.path.getmtime(cache_path) < float(source_mtime)
        ):
            return None
    except OSError:
        return None
    return cache_path


def _run_original_artifact_flight(artifact_key, producer, consumer):
    """Run one ``/original`` artifact flight, shedding declined prefetches."""
    speculative = request.args.get("prefetch") == "1"
    speculative_slot = False
    if speculative:
        speculative_slot = preview_prefetch_slots.acquire(blocking=False)
        if not speculative_slot:
            return _shed_prefetch_response()

    try:
        result = original_artifact_flights.run(
            artifact_key,
            producer,
            consumer,
            join=not speculative,
        )
        if result.skipped:
            return _shed_prefetch_response()
        return result.value
    except _ArtifactResponseError as exc:
        return exc.to_response()
    except ArtifactProducerFailed as exc:
        if isinstance(exc.__cause__, _ArtifactResponseError):
            return exc.__cause__.to_response()
        raise
    finally:
        if speculative_slot:
            preview_prefetch_slots.release()


def _source_changed_response():
    return _ArtifactResponseError(make_response(("Photo source changed", 404)))


@contextlib.contextmanager
def _preparation_publication(db, photo_id, prepare_source, captured_recipe=_UNSET):
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

    def check_source():
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


class _OriginalPhotoRequest:
    """One ``/photos/<id>/original`` request after its catalog lookups.

    The coordinator: it holds the request's inputs and runs the phases in
    order. The Flask-free steps live in ``services.original_photo``
    (``OriginalPhoto`` for the source probes, ``EditRenderSource`` for an
    edited render's source, ``UneditedSource`` for the unedited original,
    ``OriginalExtraction`` for an on-demand rendition); the responses for an
    explicit RAW pair view and for an extracted rendition are built by
    ``_PairedRawOriginal`` and ``_ExtractedOriginal``.

    ``view`` is the registered ``serve_original_photo`` view, re-entered by
    equal-key artifact flights. The unedited path's resolved source and
    extraction targets are filled in by ``_resolve_unedited_source`` and
    ``serve_unedited``.
    """

    def __init__(
        self,
        db,
        photo_id,
        photo,
        folder_path,
        vireo_dir,
        recipe,
        primary_is_raw,
        *,
        artifact_flight_guarded,
        prepare_source,
        view,
    ):
        self.db = db
        self.photo_id = photo_id
        self.photo = photo
        self.folder_path = folder_path
        self.vireo_dir = vireo_dir
        self.recipe = recipe
        self.primary_is_raw = primary_is_raw
        self.artifact_flight_guarded = artifact_flight_guarded
        self.prepare_source = prepare_source
        self._view = view
        self.original = OriginalPhoto(
            db, photo_id, photo, folder_path, vireo_dir, recipe, primary_is_raw,
        )
        self.file_state = None
        self.trusted_wc_path = None
        self.source = None
        self.extraction = None

    def _preparation_publication(self):
        return _preparation_publication(
            self.db, self.photo_id, self.prepare_source,
            captured_recipe=self.recipe,
        )

    def _reenter(self, *, guarded):
        response = make_response(
            self._view(
                self.photo_id, _artifact_flight_guarded=guarded, _prepare_source=self.prepare_source,
            )
        )
        if response.status_code >= 400:
            raise _ArtifactResponseError(response)
        return response

    def _serve_trusted_working_copy_if_present(self):
        with working_copy_publication_guard():
            if os.path.isfile(self.trusted_wc_path):
                return _serve_trusted_working_copy(self.trusted_wc_path)
        return None

    def serve_paired_raw(self, pair_source, pair_source_path):
        """Serve an explicit ``?source=raw`` view of a paired photo."""
        return _PairedRawOriginal(self, pair_source, pair_source_path).serve()

    def collect_file_state(self):
        self.file_state = self.original.collect_file_state()

    def coordinate_canonical_flight(self):
        """Share one cache-miss render between equal-key requests.

        Every remaining cache-miss path resolves to one real destination,
        so coordinate by that path: edit signatures naturally stay
        separate, while equal preloads and visible 1:1 requests share one
        expensive extraction.
        """
        if self.recipe:
            artifact_path = _full_resolution_render_path(
                self.vireo_dir, self.photo, self.recipe, self.file_state,
            )
        elif self.primary_is_raw:
            artifact_path = os.path.join(
                self.vireo_dir, "originals", f"{self.photo_id}.display.jpg",
            )
        else:
            artifact_path = os.path.join(
                self.vireo_dir, "working", f"{self.photo_id}.jpg",
            )
        return _run_original_artifact_flight(
            os.path.abspath(artifact_path),
            lambda: self._reenter(guarded=True),
            lambda: self._reenter(guarded=False),
        )

    def trusted_full_res_working_copy_path(self):
        return self.original.trusted_full_res_working_copy_path()

    # -- Edited renders --------------------------------------------------

    def serve_edited(self):
        """Render the edit recipe at full resolution and serve the cache."""
        from image_loader import RAW_EXTENSIONS

        edit_source = EditRenderSource(self.original, self.trusted_wc_path)
        source = edit_source.select()
        if (
            (self.primary_is_raw or self.trusted_wc_path is None)
            and source.ext in RAW_EXTENSIONS
            and _has_current_working_copy_failure(
                self.photo,
                self.vireo_dir,
                trust_existing_working_copy=False,
                live_source_path=source.path,
                folder_path=self.folder_path,
            )
        ):
            log.info(
                "Skipping edited original-image extraction for photo %s; "
                "RAW working-copy extraction already failed for current source mtime",
                self.photo_id,
            )
            return "Could not load image", 500
        img = edit_source.load(source)
        if img is None:
            _record_working_copy_failure(self.db, self.photo, source.path)
            return "Could not load image", 500
        return self._publish_edited_render(img)

    def _publish_edited_render(self, img):
        import config as cfg
        import local_masks
        from flask import send_file
        from image_edits import apply_recipe_to_loaded_image

        photo_id = self.photo_id
        img = apply_recipe_to_loaded_image(
            img, self.recipe,
            camera_metadata=self.photo,
            native_size=_recipe_source_dimensions(self.photo),
            local_mask=local_masks.load_snapshot(
                self.vireo_dir, photo_id, self.recipe,
            ),
        )
        originals_dir = os.path.join(self.vireo_dir, "originals")
        cache_path = _full_resolution_render_path(
            self.vireo_dir, self.photo, self.recipe, self.file_state,
        )
        os.makedirs(originals_dir, exist_ok=True)
        quality = cfg.load().get("working_copy_quality", 92)
        fd, tmp_path = tempfile.mkstemp(
            prefix=f".{photo_id}.", suffix=".jpg.tmp", dir=originals_dir,
        )
        os.close(fd)
        try:
            img.save(tmp_path, format="JPEG", quality=quality)
            with self._preparation_publication(), reader_guard("full-resolution"):
                replace_file(tmp_path, cache_path)
                _peg_render_mtime_to_source(cache_path, self.photo)
                response = send_file(cache_path, mimetype="image/jpeg")
        except Exception:  # broad: remove the unpublished temporary image, then re-raise
            with contextlib.suppress(OSError):
                os.unlink(tmp_path)
            raise
        img.close()
        return response

    # -- Unedited originals ----------------------------------------------

    def serve_unedited(self):
        """Serve the unedited original, extracting a rendition on a miss."""
        from flask import send_file

        response = self._resolve_unedited_source()
        if response is not None:
            return response
        source = self.source

        # For browser-native formats without a working copy, serve directly
        ext = source.resolved_ext
        if ext in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp") and not self.photo["working_copy_path"] and os.path.exists(source.image_path):
            return send_file(source.image_path)

        # Extract full-res working copy (on-demand upgrade)
        self.extraction = OriginalExtraction(
            self.original, source, self._preparation_publication,
        )
        extracted = _ExtractedOriginal(self, self.extraction)
        tmp_path = self.extraction.extract_to_private_tmp(self.extraction.source_for_extraction)
        if tmp_path:
            return extracted.serve(tmp_path)

        response = extracted.serve_companion_extraction()
        if response is not None:
            return response

        return self._serve_decoded_original()

    def _resolve_unedited_source(self):
        from flask import send_file

        self.source = source = UneditedSource.resolve(self.original)

        if self.primary_is_raw:
            with reader_guard("full-resolution"):
                if source.display_cache_is_current():
                    return send_file(source.display_cache_path, mimetype="image/jpeg")

        if (
            self.primary_is_raw
            and self.trusted_wc_path
            and not os.path.isfile(source.image_path)
            and not source.companion_for_extraction
        ):
            # Preserve offline behavior without repeatedly retrying a missing
            # RAW. A camera-rendered display cache or companion still wins
            # above when available; otherwise the edit-quality working copy
            # is the best usable full-resolution fallback.
            #
            # Hold the publication guard through ``send_file`` so a concurrent
            # quota reduction cannot unlink the only usable rendition between
            # this check and the file open — mirrors the non-RAW cache-hit
            # branch above.
            response = self._serve_trusted_working_copy_if_present()
            if response is not None:
                return response

        return self._route_around_raw_failure()

    def _route_around_raw_failure(self):
        if self.source.route_around_raw_failure():
            if self.primary_is_raw and self.trusted_wc_path:
                # Same eviction race as the non-RAW cache-hit branch:
                # revalidate under the publication guard before opening.
                response = self._serve_trusted_working_copy_if_present()
                if response is not None:
                    return response
            log.info(
                "Skipping original-image extraction for photo %s; RAW working-copy "
                "extraction already failed for current source mtime",
                self.photo_id,
            )
            return "Could not load image", 500
        return None

    def _serve_decoded_original(self):
        """Fallback: serve via load_image."""
        img = self.source.decode()
        if img is None:
            _record_working_copy_failure(self.db, self.photo, self.source.image_path)
            if self.primary_is_raw and self.trusted_wc_path:
                # Source/offline bytes are unavailable. A working copy is less
                # faithful to the camera rendition, but remains the best usable
                # full-resolution fallback and preserves offline behavior.
                #
                # Hold the publication guard through ``send_file`` so a
                # concurrent quota reduction cannot unlink the fallback
                # between validation and open — mirrors the guarded returns
                # above. If it was evicted mid-flight, fall through to the
                # 500 rather than raising inside ``send_file``.
                response = self._serve_trusted_working_copy_if_present()
                if response is not None:
                    return response
            return "Could not load image", 500
        return self._cache_decoded_original(img)

    def _cache_decoded_original(self, img):
        from flask import send_file

        photo_id = self.photo_id
        if self.primary_is_raw:
            cache_path = self.source.display_cache_path
            cache_dir = os.path.dirname(cache_path)
            tmp_prefix = f".{photo_id}.display."
        else:
            cache_path = _full_resolution_render_path(
                self.vireo_dir, self.photo, self.recipe, self.file_state,
            )
            cache_dir = os.path.dirname(cache_path)
            tmp_prefix = f".{photo_id}."
        os.makedirs(cache_dir, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(
            prefix=tmp_prefix,
            suffix=".jpg.tmp",
            dir=cache_dir,
        )
        os.close(fd)
        try:
            img.save(tmp_path, format="JPEG", quality=self.extraction.quality)
            with self._preparation_publication(), reader_guard("full-resolution"):
                replace_file(tmp_path, cache_path)
                if self.primary_is_raw:
                    # The unedited RAW display cache-hit check compares
                    # against ``max(mtime(image_path), mtime(companion))``.
                    # Pegging to ``photo['file_mtime']`` alone (as the
                    # signature-keyed prepared render does) would fail that
                    # check on every request when the paired companion is
                    # newer than the RAW row.
                    _peg_display_cache_mtime(
                        cache_path, (self.source.image_path, self.source.companion_for_extraction),
                    )
                else:
                    _peg_render_mtime_to_source(cache_path, self.photo)
                response = send_file(cache_path, mimetype="image/jpeg")
        except Exception:  # broad: remove the unpublished temporary image, then re-raise
            with contextlib.suppress(OSError):
                os.unlink(tmp_path)
            raise
        finally:
            img.close()
        return response


class _PairedRawOriginal:
    """An explicit ``?source=raw`` view of a paired photo's ``/original``.

    Owns the requested pair source and its file; reads the request's photo,
    recipe and flight state, and re-enters the view through the request.
    """

    def __init__(self, original_request, pair_source, pair_source_path):
        self.photo = original_request.photo
        self.photo_id = original_request.photo_id
        self.vireo_dir = original_request.vireo_dir
        self.recipe = original_request.recipe
        self.artifact_flight_guarded = original_request.artifact_flight_guarded
        self._reenter = original_request._reenter
        self.pair_source = pair_source
        self.pair_source_path = pair_source_path

    def serve(self):
        """Serve an explicit ``?source=raw`` view of a paired photo.

        Paired RAW originals coordinate through a source-aware shadow
        cache: a speculative /original?source=raw&prefetch=1 warmup and
        the follow-up visible /original?source=raw request use distinct
        URLs (prefetch vs no prefetch) so the browser cannot coalesce
        them, and the RAW decode is expensive enough that racing two
        concurrent producers doubles disk and CPU cost. Key the shadow
        by render/source state so a swapped RAW file or an updated
        recipe never serves stale bytes.
        """
        from flask import send_file

        paired_original_state = _paired_render_state_hash(
            self.photo, 0, self.pair_source, self.pair_source_path, self.recipe,
        )
        paired_original_cache = _paired_original_path(
            self.vireo_dir, self.photo_id, self.pair_source, paired_original_state,
        )
        with reader_guard("full-resolution"):
            if _fresh_paired_artifact(paired_original_cache):
                return send_file(paired_original_cache, mimetype="image/jpeg")

        def _render_paired_original():
            return self._render_paired_original(paired_original_cache)

        if self.artifact_flight_guarded:
            return _render_paired_original()

        def _paired_consumer():
            # Producer already published the shadow-cache file, so a
            # re-entering waiter hits the cache-hit check above and
            # serves it via send_file — no second decode.
            return self._reenter(guarded=True)

        return _run_original_artifact_flight(
            os.path.abspath(paired_original_cache),
            _render_paired_original,
            _paired_consumer,
        )

    def _render_paired_original(self, paired_original_cache):
        import io

        import config as cfg
        from image_loader import (
            RAW_DECODE_LINEAR,
            load_image,
        )
        load_kwargs = (
            {"raw_decode": RAW_DECODE_LINEAR}
            if self.pair_source == "raw" else {}
        )
        img = load_image(
            self.pair_source_path, max_size=None, **load_kwargs
        )
        if img is None:
            raise _ArtifactResponseError(
                make_response(("Could not load image", 500)),
            )
        if self.recipe:
            import local_masks
            from image_edits import apply_recipe_to_loaded_image

            img = apply_recipe_to_loaded_image(
                img,
                self.recipe,
                camera_metadata=self.photo,
                native_size=_recipe_source_dimensions(self.photo),
                local_mask=local_masks.load_snapshot(
                    self.vireo_dir, self.photo_id, self.recipe,
                ),
            )
        buf = io.BytesIO()
        img.save(
            buf,
            format="JPEG",
            quality=cfg.load().get("working_copy_quality", 92),
        )
        img.close()
        data = buf.getvalue()
        # Publish the rendered bytes to the shadow cache so an
        # equal-key follower serves them from disk instead of
        # decoding the same source. Best-effort: a full or read-only
        # disk must never turn a successful producer into a 500.
        try:
            with reader_guard("full-resolution"):
                _sweep_stale_paired_previews(
                    _paired_original_dir(self.vireo_dir),
                )
                atomic_write_bytes(data, paired_original_cache)
        except Exception:  # broad: cache publication is best-effort; the bytes are served from memory
            log.warning(
                "Failed to publish paired original cache %s",
                paired_original_cache, exc_info=True,
            )
        return make_response(
            Response(data, mimetype="image/jpeg"),
        )


class _ExtractedOriginal:
    """Serve an unedited ``/original`` rendition extracted on demand.

    ``extraction`` owns the rendition's targets and publication; this class
    decides what to serve: the published cache file, the trusted working
    copy when the RAW extraction came out undersized, or a transient stream
    when the rendition does not fit the working-copy quota. It reads the
    request's photo, resolved source and trusted working copy and owns no
    state of its own: the tempfile path and size flow through the calls.
    """

    def __init__(self, original_request, extraction):
        self.db = original_request.db
        self.photo = original_request.photo
        self.photo_id = original_request.photo_id
        self.vireo_dir = original_request.vireo_dir
        self.primary_is_raw = original_request.primary_is_raw
        self.trusted_wc_path = original_request.trusted_wc_path
        self._preparation_publication = original_request._preparation_publication
        self.source = original_request.source
        self.extraction = extraction

    def serve(self, tmp_path):
        # Update DB so future requests are fast; also backfill
        # dimensions if missing so the full-res shortcut works next time
        from PIL import Image as _PILImage
        if self.primary_is_raw:
            # RAW display cache goes through ``wc_abs`` immediately so
            # subsequent checks (undersized retry, trusted-wc override)
            # can peek at the just-written file directly.
            self.extraction.publish(tmp_path)
            tmp_path = None
            peek_path = self.extraction.wc_abs
        else:
            # Non-RAW: keep the rendition private until cacheability
            # is decided in ``_serve_generated_original``. Peek at the
            # tempfile itself for its dimensions.
            peek_path = tmp_path
        with _PILImage.open(peek_path) as upgraded:
            uw, uh = upgraded.size
        tmp_path, uw, uh = self.extraction.replace_undersized_extraction(tmp_path, uw, uh)
        if self.primary_is_raw and self.trusted_wc_path:
            expected_w, expected_h = _scaled_recipe_source_dimensions(self.photo)
            display_is_near_full = (
                expected_w <= 0
                or expected_h <= 0
                or (
                    uw >= expected_w * 0.99
                    and uh >= expected_h * 0.99
                )
            )
            if not display_is_near_full:
                # RAW extraction can report success after falling back to
                # a preview-sized embedded JPEG. Do not persist that as
                # the 1:1 display cache when a trusted full-res copy is
                # already available, and mark the RAW retry so later
                # requests take the fast fallback path.
                #
                # Hold the publication guard through ``send_file`` so a
                # concurrent quota reduction cannot unlink the fallback
                # between validation and open — mirrors the guarded
                # cache-hit returns above. If the trusted copy was
                # already evicted, keep the undersized display cache
                # rather than destroying it and 500ing: falling through
                # serves ``wc_abs`` via ``_serve_generated_original``,
                # and the retry mark is redundant when there is no
                # trusted fallback for a later request to take.
                with working_copy_publication_guard():
                    if os.path.isfile(self.trusted_wc_path):
                        with contextlib.suppress(OSError):
                            os.unlink(self.extraction.wc_abs)
                        _record_working_copy_failure(
                            self.db, self.photo, self.extraction.source_for_extraction,
                        )
                        return _serve_trusted_working_copy(self.trusted_wc_path)
        return self._serve_generated_original(tmp_path, uw, uh)

    def serve_companion_extraction(self):
        """Extract the companion after extraction failed on a RAW source.

        extract_working_copy failed on a RAW source: try the full-res
        companion JPEG as a fallback before giving up. This catches
        unsupported RAW variants (libraw can't demosaic, no usable
        embedded JPEG) on RAW+JPEG rows — without it, a usable sidecar
        JPEG would be ignored and the request would 500.
        """
        from image_loader import RAW_EXTENSIONS

        if (
            self.source.resolved_ext in RAW_EXTENSIONS
            and self.source.companion_for_extraction
            and self.source.companion_for_extraction != self.extraction.source_for_extraction
        ):
            companion_tmp = self.extraction.extract_to_private_tmp(self.source.companion_for_extraction)
            if companion_tmp:
                from PIL import Image as _PILImage
                if self.primary_is_raw:
                    self.extraction.publish(companion_tmp)
                    companion_tmp = None
                    peek_path = self.extraction.wc_abs
                else:
                    peek_path = companion_tmp
                with _PILImage.open(peek_path) as upgraded:
                    uw, uh = upgraded.size
                log.info(
                    "RAW extraction failed for photo %s original; served "
                    "companion JPEG instead", self.photo_id,
                )
                return self._serve_generated_original(companion_tmp, uw, uh)
        return None

    def _serve_generated_original(self, tmp_path, uw, uh):
        # Lock order matches the existing publication path: working-copy
        # guard first, then SQLite, including nested extraction commits.
        try:
            with working_copy_publication_guard(), self._preparation_publication():
                return self._serve_generated_original_current(tmp_path, uw, uh)
        except BaseException:  # broad: remove any unpublished temporary render, then re-raise
            if tmp_path:
                with contextlib.suppress(OSError):
                    os.unlink(tmp_path)
            raise

    def _serve_generated_original_current(self, tmp_path, uw, uh):
        """Publish to the cache, or stream the private tmp transiently.

        ``tmp_path`` is the private rendition from
        ``extract_to_private_tmp``. The RAW branch has always already
        published (its callers publish immediately so their
        ``PIL.Image.open`` size peek reads a known path); it passes
        ``tmp_path=None`` and this call just serves ``wc_abs``.
        Otherwise, the cacheable branch publishes and serves from
        ``wc_abs`` (pinning against concurrent eviction with an open
        fd); the non-cacheable branch keeps the file at a per-request
        transient path and streams from there — ``wc_abs`` is never
        touched, so concurrent waiters cannot collide there.
        """
        from flask import send_file

        if self.primary_is_raw:
            return send_file(self.extraction.wc_abs, mimetype="image/jpeg")

        # Decide cacheability under the publication/eviction guard using
        # a freshly reloaded quota. If a settings save raised
        # ``working_copy_cache_max_mb`` while this slow extraction was
        # encoding, a budget snapshot captured at request start would
        # be stale: reusing it here would treat a rendition that now
        # fits as non-cacheable and stamp a new
        # ``working_copy_evicted_mtime`` after the settings handler
        # already cleared markers, suppressing backfill for that row
        # until another quota bump or source-mtime change. Reloading
        # under the guard also serializes the marker write with the
        # settings handler's clear (which acquires the same guard when
        # raising the quota) so a race cannot leave a stale marker.
        with working_copy_publication_guard():
            current_budget = working_copy_quota_bytes()
            try:
                generated_size = os.path.getsize(tmp_path)
            except OSError:
                generated_size = current_budget + 1
            cacheable = (
                current_budget > 0
                and generated_size <= current_budget
            )
            if cacheable:
                # Publish first, then open the file BEFORE we commit the
                # row that makes ``wc_abs`` visible to concurrent
                # eviction passes. A peer thread that runs
                # ``evict_if_over_quota`` between the commit here and
                # the open below can select this just-written file as
                # the oldest and unlink it before Flask ever has an fd
                # on it, turning a successful render into a 500.
                # Opening first: POSIX keeps the bytes readable through
                # an unlink; Windows makes the open fd itself prevent
                # unlink so eviction of this specific file just no-ops.
                self.extraction.publish(tmp_path)
                try:
                    rendition_fh = open(self.extraction.wc_abs, "rb")  # noqa: SIM115 — closed by send_file's response
                except OSError:
                    log.exception(
                        "Failed to open just-written working copy %s",
                        self.extraction.wc_abs,
                    )
                    return "Could not load image", 500
                self.extraction.commit_generated_original(uw, uh, tracked=True)
            else:
                # Non-cacheable: keep the rendition in ``tmp_path`` (a
                # per-request private tempfile) and never publish to
                # ``wc_abs`` at all. Publishing would only invite two
                # races under a zero or undersized quota: waiters would
                # compete to move ``wc_abs`` to their own transient
                # location and every waiter but the winner would
                # ``os.replace(wc_abs, ...)`` a missing file (500), and
                # a concurrent eviction pass would see and immediately
                # unlink the "orphan" it cannot reconcile against any
                # catalog row. The DB still records
                # ``working_copy_path=NULL`` so future requests know
                # they must regenerate rather than expect a cache hit.
                # The response still needs a decoded JPEG, but a zero
                # quota (or one smaller than this single rendition)
                # must not turn that response into a persistent cache
                # entry.
                self.extraction.commit_untracked_generated_original(uw, uh)

        if cacheable:
            # The on-demand route is also a cache writer. Apply the same
            # oldest-first ceiling as scanner/backfill generation. The
            # open fd above pins the rendition against this call's own
            # enforcement pass too. Validators are wc_abs's, as a warm hit sends.
            response = _send_opened_file(rendition_fh, self.extraction.wc_abs, "image/jpeg")
            try:
                evict_working_copy_cache_if_over_quota(self.db, self.vireo_dir)
            except Exception:
                # broad: serving the successfully generated image is more useful
                # than turning a transient maintenance error into a 500;
                # startup and later writes will retry enforcement.
                log.exception(
                    "Working-copy quota enforcement failed after "
                    "on-demand write"
                )
            return response

        return self._stream_transient_rendition(tmp_path)

    def _stream_transient_rendition(self, tmp_path):
        """Stream a non-cacheable rendition from a transient path.

        Non-cacheable rendition: relocate ``tmp_path`` into
        ``originals/`` so a stream interrupted by a process kill is
        reclaimed by ``_sweep_abandoned_transient_originals`` on the
        next startup. ``tmp_path`` is unique per waiter (mkstemp), so
        the move never collides with a peer's tempfile. Windows can
        delete the private file after its response handle closes
        (unlinking an open file is not supported there).
        """
        transient_dir = os.path.join(self.vireo_dir, "originals")
        os.makedirs(transient_dir, exist_ok=True)
        fd, transient_path = tempfile.mkstemp(
            prefix=f".{self.photo_id}.transient.",
            suffix=".jpg",
            dir=transient_dir,
        )
        os.close(fd)
        try:
            replace_file(tmp_path, transient_path)
        except OSError:
            log.exception(
                "Failed to relocate rendition %s to transient path %s",
                tmp_path, transient_path,
            )
            with contextlib.suppress(OSError):
                os.unlink(tmp_path)
            with contextlib.suppress(OSError):
                os.unlink(transient_path)
            return "Could not load image", 500
        try:
            rendition_fh = open(transient_path, "rb")  # noqa: SIM115 — closed by _stream_rendition's finally
        except OSError:
            log.exception(
                "Failed to open just-written non-cacheable "
                "rendition %s", transient_path,
            )
            with contextlib.suppress(OSError):
                os.unlink(transient_path)
            return "Could not load image", 500

        def _stream_rendition():
            try:
                while chunk := rendition_fh.read(1024 * 1024):
                    yield chunk
            finally:
                with contextlib.suppress(OSError):  # close of a read-only handle
                    rendition_fh.close()
                with contextlib.suppress(OSError):
                    os.unlink(transient_path)

        return Response(_stream_rendition(), mimetype="image/jpeg")


def _send_cached_thumbnail(directory, fname):
    resp = make_response(send_from_directory(directory, fname))
    resp.cache_control.public = True
    resp.cache_control.max_age = 24 * 60 * 60  # 1 day
    return resp


class _ThumbnailRequest:
    """One ``/thumbnails/<filename>`` request after its catalog lookups.

    The cache target is chosen by ``select_cache_target`` and may move to the
    ``_regen`` sidecar in ``serve_fresh_cache``; the self-heal source and
    helpers are filled in by ``prepare_self_heal`` and
    ``resolve_render_source``.
    """

    def __init__(
        self,
        db,
        photo_id,
        photo,
        folder_row,
        filename,
        thumb_dir,
        pair_source,
        pair_source_path,
    ):
        self.db = db
        self.photo_id = photo_id
        self.photo = photo
        self.folder_row = folder_row
        self.filename = filename
        self.thumb_dir = thumb_dir
        self.pair_source = pair_source
        self.pair_source_path = pair_source_path
        self.cache_recipe = None
        self.cache_filename = None
        self.thumb_path = None
        # Set when a locked stale thumbnail forces regeneration to the
        # ``<id>_regen.jpg`` sidecar; the real ``<id>.jpg`` stays stale.
        self.served_from_regen_sidecar = False
        self.selected_source_mtime = None
        self.live_source = None
        self.vireo_dir = None
        self.folders = None
        self.render_recipe = None
        self.thumb_size = None
        self.source = None
        self.using_working_copy = False
        self._cfg = None
        self._generate_thumbnail = None
        self._retry_after_working_copy_eviction = None
        self._retry_with_working_copy = None

    def select_cache_target(self):
        self.cache_recipe = (
            None if self.pair_source == "jpeg"
            else self.db.get_photo_edit_recipe(self.photo_id)
        )
        self.cache_filename = (
            f"{self.photo_id}_{self.pair_source}.jpg"
            if self.pair_source else self.filename
        )
        self.thumb_path = os.path.join(self.thumb_dir, self.cache_filename)
        try:
            self.selected_source_mtime = (
                os.path.getmtime(self.pair_source_path)
                if self.pair_source_path else self.photo["file_mtime"]
            )
        except OSError:
            self.selected_source_mtime = self.photo["file_mtime"]

    def _is_fresh(self, cached_mtime):
        # Treat as fresh if ``file_mtime`` is unknown (legacy rows
        # pre-mtime tracking — we have no signal to invalidate
        # against, so prefer the fast path over false-positive
        # regeneration).
        return (
            self.selected_source_mtime is None
            or cached_mtime >= self.selected_source_mtime
        ) and _camera_cache_matches(self.thumb_path, self.photo, self.cache_recipe)

    def serve_fresh_cache(self):
        """Serve the cached thumbnail when it is current, or clear it.

        Returns ``None`` when the caller should regenerate into
        ``self.thumb_path``.
        """
        # Collapse existence + freshness probe into a single ``getmtime``
        # so a concurrent ``Clear cache`` (or parallel regeneration) that
        # unlinks the file between two separate syscalls can't surface as
        # a 500. ``FileNotFoundError`` is the cache-miss signal; bind the
        # exception narrowly so unrelated OSErrors still surface.
        try:
            cached_mtime = os.path.getmtime(self.thumb_path)
        except FileNotFoundError:
            cached_mtime = None
        if cached_mtime is None:
            return None
        if self._is_fresh(cached_mtime):
            return _send_cached_thumbnail(self.thumb_dir, self.cache_filename)
        log.info(
            "Thumbnail for photo %s is stale (cached mtime %s, "
            "source file_mtime %s, or changed camera profile) — regenerating",
            self.photo_id, cached_mtime, self.selected_source_mtime,
        )
        # ``generate_thumbnail`` short-circuits when the destination
        # file already exists (an "already done" optimization), so a
        # stale copy left in place would defeat the regen path. Unlink
        # before falling through; the regen will write a fresh JPEG
        # at the same path.
        try:
            os.remove(self.thumb_path)
        except OSError:
            return self._serve_regen_sidecar()
        return None

    def _serve_regen_sidecar(self):
        """Move the cache target to the ``_regen`` sidecar.

        We can't unlink the stale file (Windows lock, permissions,
        antivirus quarantine) and we must not regenerate over it:
        ``generate_thumbnail`` would short-circuit on the existing file and
        the ``os.utime`` below would then mark the stale image fresh
        forever.

        Serving it is not an option either. This branch fires for recycled
        rowids, so those pixels belong to a *different photo* — and if the
        lock persists, every subsequent request serves them too. Showing
        another bird is worse than showing nothing.

        Regenerate to a sidecar and serve that instead. The
        ``{photo_id}_*`` shape means the recycled-id purge and the delete
        cleanup already sweep it.
        """
        stem, ext = os.path.splitext(self.cache_filename)
        self.cache_filename = f"{stem}_regen{ext}"
        self.served_from_regen_sidecar = True
        self.thumb_path = os.path.join(self.thumb_dir, self.cache_filename)
        log.warning(
            "Could not unlink stale thumbnail %s; regenerating to "
            "%s rather than serving the previous owner's pixels.",
            os.path.join(self.thumb_dir, f"{stem}{ext}"), self.cache_filename,
            exc_info=True,
        )
        try:
            sidecar_mtime = os.path.getmtime(self.thumb_path)
        except FileNotFoundError:
            sidecar_mtime = None
        if sidecar_mtime is not None and self._is_fresh(sidecar_mtime):
            return _send_cached_thumbnail(self.thumb_dir, self.cache_filename)
        # A stale sidecar has to go before we fall through, for
        # the same reason as the original: ``generate_thumbnail``
        # short-circuits on an existing destination, so leaving it
        # would return the previous owner's pixels and the
        # ``os.utime(result, ...)`` below would then mark them
        # fresh forever.
        if sidecar_mtime is not None:
            with contextlib.suppress(OSError):
                os.remove(self.thumb_path)
            if os.path.exists(self.thumb_path):
                # Both the real thumbnail and the sidecar are
                # stale and undeletable, so there is nowhere in
                # the cache we can safely write. Every remaining
                # option would serve another photo's pixels;
                # answer honestly instead. The next request
                # retries both unlinks.
                log.error(
                    "Photo %s has a stale thumbnail and a stale "
                    "regeneration sidecar, neither of which could "
                    "be removed (%s). Serving no thumbnail rather "
                    "than the previous owner's pixels; clear the "
                    "cache in Settings > Storage to recover.",
                    self.photo_id, self.thumb_path,
                )
                return "", 404
        return None

    def prepare_self_heal(self):
        self.live_source = os.path.join(
            self.folder_row["path"], self.photo["filename"],
        )

        import config as cfg
        from thumbnails import (
            _retry_thumbnail_after_working_copy_eviction,
            _retry_thumbnail_with_working_copy,
            generate_thumbnail,
        )
        self._cfg = cfg
        self._generate_thumbnail = generate_thumbnail
        self._retry_after_working_copy_eviction = (
            _retry_thumbnail_after_working_copy_eviction
        )
        self._retry_with_working_copy = _retry_thumbnail_with_working_copy
        self.vireo_dir = os.path.dirname(self.thumb_dir)
        # Look up the photo's folder path directly rather than via
        # ``get_folder_tree()``: the tree filter excludes folders whose
        # status is ``'missing'``, which would leave the canonical-path
        # helper with an empty mapping and silently fall back to
        # ``os.path.join('', photo['filename'])`` — a CWD-relative path
        # that could read or persist a thumbnail derived from an
        # unrelated same-named file in the server's working directory.
        # The routes already decided the photo may be shown (workspace
        # membership, or duplicate-group membership on the Duplicates
        # page), so a direct ``folders.get`` lookup here is safe and
        # status-agnostic.
        self.folders = (
            {self.folder_row["id"]: self.folder_row["path"]}
            if self.folder_row else {}
        )

    def resolve_render_source(self):
        recipe = self.db.get_photo_edit_recipe(self.photo_id)
        # Edit recipes and local masks are stored in the primary RAW's
        # coordinate space. A developed companion may already be cropped,
        # rotated, or resized, so applying that geometry again would render
        # the selected JPEG incorrectly. JPEG pair views intentionally show
        # the companion as-authored; RAW pair views retain the catalog edit.
        self.render_recipe = None if self.pair_source == "jpeg" else recipe
        self.thumb_size = self._cfg.load().get("thumbnail_size", 400)
        if self.pair_source_path:
            self.source = self.pair_source_path
            self.using_working_copy = False
        else:
            # Resolve source via the canonical-path helper so we prefer the
            # JPEG working copy over the original RAW. This makes the
            # self-heal work for cameras whose RAW format libraw cannot
            # decode (the working copy was extracted from the embedded JPEG
            # at scan time).
            self.source, self.using_working_copy = _recipe_render_source(
                self.photo, self.render_recipe, self.thumb_size,
                self.vireo_dir, self.folders,
            )

    def would_retry_failed_raw_decode(self):
        return (
            not self.using_working_copy
            and self.pair_source != "jpeg"
            and _has_current_working_copy_failure(
                self.photo,
                self.vireo_dir,
                trust_existing_working_copy=False,
                live_source_path=self.live_source,
                folder_path=self.folder_row["path"],
            )
        )

    def generate(self):
        """Generate the thumbnail, then walk the decode fallbacks."""
        # Derive the decode mode from the primary photo's extension
        # rather than source so a future change to render-source
        # resolution cannot silently bypass RAW_DECODE_LINEAR
        # for a RAW primary. Without this, EDIT_MATH_VERSION's cache
        # purge regenerates edited-RAW thumbnails through the default
        # JPEG-first decode and grid thumbnails diverge from previews
        # / exports (which preserve highlights).
        from image_loader import RAW_DECODE_LINEAR, RAW_EXTENSIONS
        photo = self.photo
        render_recipe = self.render_recipe
        raw_decode = (
            RAW_DECODE_LINEAR
            if (
                self.pair_source == "raw"
                or (
                    render_recipe
                    and os.path.splitext(photo["filename"])[1].lower()
                    in RAW_EXTENSIONS
                )
            )
            else None
        )
        min_source_size = None
        if raw_decode and os.path.splitext(self.source)[1].lower() in RAW_EXTENSIONS:
            load_max_size = (
                None
                if render_recipe and render_recipe.get("crop")
                else self.thumb_size
            )
            min_source_size = _scaled_recipe_source_dimensions(
                photo, load_max_size,
            )
        result = self._generate_thumbnail(
            self.photo_id,
            self.source,
            self.thumb_dir,
            size=self.thumb_size,
            recipe=render_recipe,
            camera_metadata=photo,
            raw_decode=raw_decode,
            min_source_size=min_source_size,
            native_size=(
                _recipe_source_dimensions(photo) if render_recipe else None
            ),
            cache_name=self.cache_filename,
        )
        if not result and self.using_working_copy:
            result, self.source = self._retry_after_working_copy_eviction(
                photo,
                self.source,
                self.thumb_dir,
                self.thumb_size,
                self._cfg.load().get("thumbnail_quality", 85),
                render_recipe,
                self.folder_row["path"],
                self.vireo_dir,
                cache_name=self.cache_filename,
            )
        if (
            not result
            and os.path.splitext(self.source)[1].lower() in RAW_EXTENSIONS
            and self.pair_source != "raw"
        ):
            result = self._generate_from_companion(result)
        if (
            not result
            and render_recipe
            and os.path.splitext(self.source)[1].lower() in RAW_EXTENSIONS
            and self.pair_source != "raw"
        ):
            result = self._retry_with_working_copy(
                self.db,
                photo,
                self.source,
                self.thumb_dir,
                self.thumb_size,
                self._cfg.load().get("thumbnail_quality", 85),
                render_recipe,
                self.vireo_dir,
                cache_name=self.cache_filename,
            )
            if result:
                self.source = result
        return result

    def _generate_from_companion(self, result):
        """Fall back to the companion JPEG after a failed RAW decode.

        libraw couldn't demosaic the RAW (unsupported variant, corrupt file,
        no usable embedded JPEG). Try the companion JPEG before 404'ing so a
        RAW+JPEG row whose RAW can't be decoded still gets a grid thumbnail —
        mirrors the companion fallback in serve_preview / serve_original.
        Returns ``result`` unchanged when there is no usable companion.
        """
        companion_rel = self.photo["companion_path"]
        if not companion_rel:
            return result
        companion_abs = os.path.join(self.folder_row["path"], companion_rel)
        if not (os.path.exists(companion_abs) and companion_abs != self.source):
            return result
        log.info(
            "Thumbnail self-heal RAW decode failed for "
            "photo %s; falling back to companion JPEG",
            self.photo_id,
        )
        _record_working_copy_failure(self.db, self.photo, self.source)
        result = self._generate_thumbnail(
            self.photo_id,
            companion_abs,
            self.thumb_dir,
            size=self.thumb_size,
            recipe=self.render_recipe,
            camera_metadata=self.photo,
            native_size=(
                _recipe_source_dimensions(self.photo)
                if self.render_recipe else None
            ),
            # Must match the primary call's target: when a
            # locked stale thumbnail redirected us to the
            # sidecar, generating to the default
            # ``<id>.jpg`` would short-circuit on that
            # locked file and the os.utime below would pin
            # the previous owner's pixels as fresh.
            cache_name=self.cache_filename,
        )
        if result:
            self.source = companion_abs
        return result

    def peg_mtime_to_source(self, result):
        """Peg the regenerated thumbnail's mtime to the source file_mtime.

        This keeps the staleness invariant — ``cached_mtime >= file_mtime``
        — holding on the next request. ``generate_thumbnail`` writes with
        the current wall clock; if ``file_mtime`` is in the future (clock
        skew on the source machine, archives that preserve future
        filesystem timestamps), the next request would compare
        ``time.time() < file_mtime`` and treat the just-written file as
        stale, triggering a regeneration loop on every fetch. The ``photo``
        dict was loaded from the row above, so its ``file_mtime`` is the
        canonical source-of-truth value.
        """
        if self.selected_source_mtime is None:
            return
        try:
            os.utime(
                result, (self.selected_source_mtime, self.selected_source_mtime),
            )
        except OSError:
            # Best-effort: a touch failure is non-fatal; the worst
            # case is the next request regenerates again. We don't
            # 404 the user over a chmod / quota issue.
            log.debug(
                "Could not align thumb mtime to source for photo %s",
                self.photo_id, exc_info=True,
            )

    def persist_thumb_path(self):
        """Persist on-disk presence for the coverage dashboard.

        The dashboard's coverage query (`thumb_path IS NOT NULL`) then
        reflects this regeneration. Stored value is the bare filename,
        matching ``thumbnails.generate_all``.

        Skipped when we fell back to the ``_regen`` sidecar: the real
        ``<id>.jpg`` is still the previous owner's locked, stale file.
        Recording it as done would tell the coverage dashboard and
        ``backfill_thumb_paths`` this photo has a valid thumbnail, so
        nothing would regenerate it once the lock clears — a pill claiming
        work is finished when the next run would not be a no-op is exactly
        what CORE_PHILOSOPHY forbids. Leaving the column NULL keeps the
        photo in the "needs a thumbnail" set, which is the truth.
        """
        if self.pair_source or self.served_from_regen_sidecar:
            return
        try:
            self.db.photos.set_thumb_path(self.photo_id, f"{self.photo_id}.jpg")
        except sqlite3.Error:
            # Coverage column drift is recoverable via the backfill job;
            # don't fail the request if the UPDATE racks up a transient
            # SQLite error.
            log.exception("Failed to persist thumb_path for photo %s", self.photo_id)


class _EditPreviewRequest:
    """One ``/photos/<id>/edit-preview`` request after its recipe is parsed.

    ``recipe`` is the normalized edit recipe and ``display_recipe`` the copy
    actually rendered (without the crop unless ``apply_crop``). The selected
    source and its decode bound are filled in by ``load_source``; the
    recovery steps move ``canonical`` when they swap in another file.
    """

    def __init__(
        self,
        db,
        photo_id,
        photo,
        folder_row,
        vireo_dir,
        recipe,
        display_recipe,
        size,
        apply_crop,
        analysis=False,
    ):
        self.db = db
        self.photo_id = photo_id
        self.photo = photo
        self.folder_row = folder_row
        self.vireo_dir = vireo_dir
        self.recipe = recipe
        self.display_recipe = display_recipe
        self.size = size
        self.apply_crop = apply_crop
        self.analysis = analysis
        self.original_abs = os.path.join(
            folder_row["path"], photo["filename"],
        )
        self.canonical = None
        self.using_working_copy = False
        self.selected_ext = None
        self.load_max_size = None
        self.native_dims = None
        self.source_failure_current = False

    def load(self):
        """Decode the edit source with every recovery path; ``None`` on failure.

        Retries the original when an evicted working copy vanished, swaps an
        undersized RAW decode for a larger companion JPEG, and falls back to
        the companion when the RAW will not decode. A failure the RAW already
        recorded for this source mtime is not retried or re-recorded.
        """
        from image_loader import RAW_EXTENSIONS

        photo = self.photo
        img = self.load_source()
        if self.source_failure_current:
            log.info(
                "Skipping edit-preview generation for photo %s; RAW "
                "working-copy extraction already failed for current source mtime",
                self.photo_id,
            )
            return None
        if img is None and self.using_working_copy:
            img = self.retry_original_source()
        if (
            img is not None
            and self.selected_ext in RAW_EXTENSIONS
            and photo["width"]
            and photo["height"]
        ):
            img = self.replace_undersized_raw_decode(img)
        if img is None and self.selected_ext in RAW_EXTENSIONS:
            img = self.load_companion_after_raw_failure()
        if img is None:
            _record_working_copy_failure(self.db, photo, self.canonical)
        return img

    def load_source(self):
        """Select the edit source and decode it; ``None`` when it failed.

        If the original volume is offline, the working copy is the only
        local edit source. Hold the publication/eviction guard from source
        selection through Pillow decode so quota enforcement cannot unlink it
        in the exists/open window. Once load_image returns, the decoded PIL
        image no longer depends on the path.
        """
        source_guard = contextlib.nullcontext()
        if self.photo["working_copy_path"] and not os.path.exists(self.original_abs):
            source_guard = working_copy_publication_guard()
        with source_guard:
            return self._select_and_load_source()

    def _source_recipe(self):
        """The recipe ``_recipe_render_source`` selects the source with.

        Resolve the source with the actual recipe (not None) so
        _recipe_render_source applies its RAW-primary gating: a RAW photo
        with a legacy JPEG working copy must use the RAW source here too, so
        the in-progress edit preview matches the saved preview/export bytes
        from the highlight-preserving decode. Auto Tone strips tonal
        adjustments to sample neutral pixels; for a RAW primary that can
        leave recipe empty, which would normally short-circuit
        _recipe_render_source to the canonical working copy. Pass a sentinel
        non-empty recipe in that case so the same RAW-primary gating applies
        and analysis reads the highlight-preserved RAW decode rather than
        clipped legacy working-copy bytes. The same sentinel applies when the
        request asks for more pixels than the (size-capped) working copy
        holds — the editor's 100% zoom requests a native-resolution render,
        and short-circuiting to a 4096-capped working copy would silently
        serve half-resolution pixels as "1:1".
        """
        from image_edits import SCHEMA_VERSION
        from image_loader import RAW_EXTENSIONS

        photo = self.photo
        recipe = self.recipe
        source_recipe = recipe
        if not recipe:
            from render_source import working_copy_satisfies_recipe_render

            undersized_wc = photo["working_copy_path"] and (
                not working_copy_satisfies_recipe_render(
                    photo, recipe, self.size, self.vireo_dir,
                )
            )
            if (
                self.analysis or undersized_wc
                or os.path.splitext(photo["filename"])[1].lower() in RAW_EXTENSIONS
            ):
                source_recipe = {"version": SCHEMA_VERSION}
        return source_recipe

    def _select_and_load_source(self):
        from image_loader import (
            RAW_DECODE_LINEAR,
            RAW_EXTENSIONS,
            load_image,
        )

        photo = self.photo
        folder_row = self.folder_row
        source_recipe = self._source_recipe()
        canonical, using_working_copy = _recipe_render_source(
            photo, source_recipe, self.size, self.vireo_dir,
            {folder_row["id"]: folder_row["path"]},
        )
        selected_ext = os.path.splitext(canonical)[1].lower()
        self.canonical = canonical
        self.using_working_copy = using_working_copy
        self.selected_ext = selected_ext
        self.source_failure_current = (
            not using_working_copy
            and selected_ext in RAW_EXTENSIONS
            and _has_current_working_copy_failure(
                photo,
                self.vireo_dir,
                trust_existing_working_copy=False,
                live_source_path=canonical,
                folder_path=folder_row["path"],
            )
        )
        raw_decode = (
            RAW_DECODE_LINEAR
            if selected_ext in RAW_EXTENSIONS
            else None
        )
        load_kwargs = {"raw_decode": raw_decode} if raw_decode else {}
        self.native_dims = _recipe_source_dimensions(photo)
        self.load_max_size = self._source_load_max_size()
        img = None
        if not self.source_failure_current:
            if using_working_copy:
                # Record access on the working copy whenever we
                # read it as an edit-preview source. Without the
                # touch the interactive editor's repeated
                # ``/edit-preview`` requests never advance the
                # WC's recency and an actively edited photo
                # stays the oldest eviction candidate. The touch
                # runs under the guard because it races
                # ``_evict_once``'s identity check, which would
                # otherwise skip this file as "replaced" and
                # silently overshoot a lowered quota.
                #
                # The decode stays OUTSIDE the guard: it is a
                # process-wide lock and the editor fires these
                # on every slider move, so holding it across
                # Pillow would serialize the whole app's image
                # reads behind one preview. If eviction unlinks
                # the copy in the exists/open window, ``img`` is
                # None and the original-source retry below
                # recovers. The offline-only ``source_guard``
                # below still wraps the decode for the case
                # where the WC is the only local source; the
                # guard is an RLock, so the touch reentering it
                # from there is safe.
                with working_copy_publication_guard():
                    touch_working_copy_access(canonical)
            img = load_image(
                canonical, max_size=self.load_max_size, **load_kwargs,
            )
        return img

    def _source_load_max_size(self):
        """The long edge to decode the selected source at.

        A cropped output needs more source pixels than its requested long
        edge. Scale the source request by the crop ratio, then let the recipe
        renderer crop and cap the result to ``size``. At 100% this naturally
        reaches the full source and stays truly 1:1 without forcing a native
        decode for every fitted slider update.
        """
        from image_loader import RAW_EXTENSIONS
        from render_source import rendered_recipe_long_edge

        size = self.size
        recipe = self.recipe
        canonical = self.canonical
        load_max_size = size
        if self.apply_crop and recipe.get("crop"):
            selected_dims = self.native_dims
            try:
                from PIL import Image as _PILImage

                if self.selected_ext not in RAW_EXTENSIONS:
                    with _PILImage.open(canonical) as selected_image:
                        candidate_dims = _image_size_after_exif_orientation(
                            selected_image,
                        )
                    if all(candidate_dims):
                        selected_dims = candidate_dims
            except Exception:
                # broad: some RAW formats cannot be opened by Pillow. Their
                # stored native dimensions remain the best available
                # decode bound.
                log.debug("Could not read dimensions of %s", canonical, exc_info=True)
            if all(selected_dims):
                selected_source_long = max(selected_dims)
                rendered_long = rendered_recipe_long_edge(
                    selected_dims[0], selected_dims[1], recipe,
                )
                if rendered_long > 0:
                    load_max_size = min(
                        selected_source_long,
                        int(math.ceil(
                            size * selected_source_long / rendered_long
                        )),
                    )
            else:
                load_max_size = None
        return load_max_size

    def retry_original_source(self):
        """Decode the original after the working copy vanished; may be None.

        Quota enforcement can unlink the selected working copy after
        _recipe_render_source returned but before load_image opens it. Retry
        the original source once so an otherwise healthy edit preview does
        not become a transient 500 during eviction; mirrors the crop and
        preview materializer recovery paths.
        """
        from image_loader import (
            RAW_DECODE_LINEAR,
            RAW_EXTENSIONS,
            load_image,
        )

        original_abs = self.original_abs
        img = None
        original_ext = os.path.splitext(original_abs)[1].lower()
        original_is_raw = original_ext in RAW_EXTENSIONS
        original_failure_current = (
            original_is_raw
            and _has_current_working_copy_failure(
                self.photo,
                self.vireo_dir,
                trust_existing_working_copy=False,
                live_source_path=original_abs,
                folder_path=self.folder_row["path"],
            )
        )
        if (
            os.path.abspath(original_abs)
            != os.path.abspath(self.canonical)
            and os.path.isfile(original_abs)
            and not original_failure_current
        ):
            fallback_raw_decode = (
                RAW_DECODE_LINEAR
                if original_is_raw else None
            )
            fallback_kwargs = (
                {"raw_decode": fallback_raw_decode}
                if fallback_raw_decode else {}
            )
            img = load_image(
                original_abs,
                max_size=self.load_max_size,
                **fallback_kwargs,
            )
            if img is not None:
                self.canonical = original_abs
                self.selected_ext = original_ext
                self.using_working_copy = False
        return img

    def replace_undersized_raw_decode(self, img):
        """Swap an undersized RAW decode for the companion JPEG if larger.

        _load_raw falls back to an embedded camera JPEG when libraw can't
        demosaic; that preview is often a fraction of sensor resolution, so
        non-None can still be undersized. Mirror serve_preview's cache-miss
        undersized check so the in-progress editor doesn't render against a
        clipped downscaled preview when a full-size companion JPEG is sitting
        next to the RAW.
        """
        from image_loader import load_image

        photo = self.photo
        expected_w, expected_h = _scaled_recipe_source_dimensions(
            photo, self.load_max_size,
        )
        if _image_is_smaller_than_expected(img, expected_w, expected_h):
            companion_rel = photo["companion_path"]
            if companion_rel:
                companion_abs = os.path.join(
                    self.folder_row["path"], companion_rel,
                )
                if (
                    os.path.exists(companion_abs)
                    and companion_abs != self.canonical
                ):
                    companion_img = load_image(
                        companion_abs, max_size=self.load_max_size,
                    )
                    if _companion_image_can_replace_raw_result(
                        companion_img, img, expected_w, expected_h,
                    ):
                        log.info(
                            "RAW decode for photo %s edit-preview "
                            "returned undersized embedded preview "
                            "(%dx%d, expected %dx%d); falling "
                            "back to companion JPEG",
                            self.photo_id, img.size[0], img.size[1],
                            expected_w, expected_h,
                        )
                        img.close()
                        img = companion_img
                        self.canonical = companion_abs
                    elif companion_img is not None:
                        companion_img.close()
        return img

    def load_companion_after_raw_failure(self):
        """Decode the companion JPEG after the RAW failed; may be None.

        Same companion fallback as serve_preview's cache-miss path: an
        unsupported RAW shouldn't kill the in-progress edit preview when a
        usable companion JPEG sits next to it.
        """
        from image_loader import load_image

        img = None
        companion_rel = self.photo["companion_path"]
        if companion_rel:
            companion_abs = os.path.join(self.folder_row["path"], companion_rel)
            if (
                os.path.exists(companion_abs)
                and companion_abs != self.canonical
            ):
                log.info(
                    "RAW decode failed for photo %s edit-preview; "
                    "falling back to companion JPEG", self.photo_id,
                )
                _record_working_copy_failure(self.db, self.photo, self.canonical)
                img = load_image(companion_abs, max_size=self.load_max_size)
                if img is not None:
                    self.canonical = companion_abs
        return img

    def render(self, img):
        """Apply the display recipe to the decoded source."""
        from image_edits import apply_recipe_to_loaded_image, detail_render_scale
        from render_source import rendered_recipe_long_edge

        native_dims = self.native_dims
        recipe = self.recipe
        # Detail scale must reflect the saved cropped render. This matters
        # especially in crop-edit mode, where the preview itself is still
        # uncropped: a tighter crop must not make the saved output's
        # sharpen/NR stronger than the preview showed.
        preview_detail_scale = None
        if native_dims and native_dims[0] and native_dims[1]:
            saved_native_long = float(rendered_recipe_long_edge(
                native_dims[0], native_dims[1], recipe,
            ))
            if saved_native_long > 0:
                saved_rendered_long = min(float(self.size), saved_native_long)
                preview_detail_scale = detail_render_scale(
                    (saved_rendered_long, saved_rendered_long),
                    native_dims,
                    recipe,
                )
        import local_masks
        return apply_recipe_to_loaded_image(
            img, self.display_recipe, max_size=self.size,
            camera_metadata=self.photo,
            native_size=native_dims,
            detail_scale=preview_detail_scale,
            local_mask=local_masks.load_snapshot(
                self.vireo_dir, self.photo_id, self.display_recipe,
            ),
        )


def _load_active_mask(db, photo_id):
    """The photo's active SAM mask as a PIL 'L' image, or None."""
    import io

    from PIL import Image

    for _attempt in range(3):
        variant = db.masks_features.active_variant(photo_id)
        mask_row = db.masks_features.get_mask(photo_id, variant) if variant else None
        if not mask_row or not mask_row.get("path"):
            return None
        try:
            with open(mask_row["path"], "rb") as handle:
                mask_bytes = handle.read()
            with Image.open(io.BytesIO(mask_bytes)) as img:
                return img.convert("L").copy()
        except (OSError, ValueError):
            continue
    return None


def render_edit_preview_job(payload, output_path):
    """Child-process entry: all decode/render work, without constructing Flask."""
    import config as cfg
    import cv2
    from db import Database
    from detail_backend import detail_thread_budget
    from float_image import FloatImage
    from image_edits import RecipeError

    cfg.CONFIG_PATH = payload['config_path']
    cv2.setNumThreads(payload.get('threads', 2))
    if payload.get('clear_source_cache'):
        import image_loader
        with image_loader._linear_cache_lock:
            image_loader._linear_cache.clear()
    with Database(payload['db_path'], initialize_schema=False) as db:
        edit = _EditPreviewRequest(
            db, payload['photo_id'], payload['photo'], payload['folder'],
            payload['vireo_dir'], payload['recipe'], payload['display_recipe'],
            payload['size'], payload['apply_crop'],
        )
        source = edit.load()
        if source is None:
            return 500, 'Could not load image', ''
        source_kind = 'linear' if isinstance(source, FloatImage) and source.encoding == 'linear' else 'srgb'
        rendered = None
        try:
            with detail_thread_budget(payload.get("threads", 2)):
                rendered = edit.render(source)
            rendered.save(output_path, format='JPEG', quality=payload['quality'])
        except RecipeError as error:
            return 400, str(error), ''
        finally:
            if rendered is not None and rendered is not source:
                rendered.close()
            source.close()
    return 200, '', source_kind


@contextlib.contextmanager
def _edit_preview_source_guard(photo, folder, cancelled):
    # The eviction guard is process-local. Keep ownership in the parent when
    # the working copy is the only source; the child must not race its eviction.
    working_copy = photo['working_copy_path']
    if working_copy:
        with working_copy_publication_guard(cancelled=cancelled.is_set):
            if not os.path.exists(os.path.join(folder['path'], photo['filename'])):
                yield
                return
    yield


def _edit_mask_preview_geometry(photo, normalized, native_dims, size, apply_crop, snapshot):
    """Source dimensions and feather scale for ``/photos/<id>/edit-mask-preview``.

    Returns ``(source_dims, preview_detail_scale)``: the size the local
    weight map is rendered at (falling back to the snapshot's own size when
    the photo's dimensions are unknown) and the detail scale of the saved
    render, or ``None`` when it cannot be derived.
    """
    from image_edits import detail_render_scale
    from render_source import rendered_recipe_long_edge

    source_size = size
    if apply_crop and normalized.get("crop") and all(native_dims):
        native_source_long = max(native_dims)
        rendered_long = rendered_recipe_long_edge(
            native_dims[0], native_dims[1], normalized,
        )
        if rendered_long > 0:
            source_size = min(
                native_source_long,
                int(math.ceil(size * native_source_long / rendered_long)),
            )
    source_dims = _scaled_recipe_source_dimensions(photo, source_size)
    if not source_dims[0] or not source_dims[1]:
        source_dims = snapshot.size
    # Feather scale must reflect the saved cropped render in both modes.
    # In crop-edit mode the overlay itself is aligned with the uncropped
    # preview, but its feather still needs to match the pixels the saved
    # render will actually weight.
    preview_detail_scale = None
    if native_dims and native_dims[0] and native_dims[1]:
        saved_native_long = float(rendered_recipe_long_edge(
            native_dims[0], native_dims[1], normalized,
        ))
        if saved_native_long > 0:
            saved_rendered_long = min(float(size), saved_native_long)
            preview_detail_scale = detail_render_scale(
                (saved_rendered_long, saved_rendered_long),
                native_dims,
                normalized,
            )
    return source_dims, preview_detail_scale


def _tinted_mask_png(weight):
    """Encode a local weight map as the editor's tinted RGBA PNG overlay."""
    import io

    import numpy as np
    from PIL import Image

    height, width = weight.shape
    rgba = np.zeros((height, width, 4), dtype=np.uint8)
    # Accent teal at up to ~60% opacity where the subject weight is 1.
    rgba[..., 0] = 112
    rgba[..., 1] = 199
    rgba[..., 2] = 186
    rgba[..., 3] = np.clip(weight * 150.0 + 0.5, 0, 255).astype(np.uint8)
    buf = io.BytesIO()
    Image.fromarray(rgba, "RGBA").save(buf, format="PNG")
    return buf.getvalue()


def _requested_pair_source(get_db, config, photo, folder_path):
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

    def _offline_path(column):
        try:
            row = get_db().caches.offline_original_get(photo["id"])
        except sqlite3.Error:
            log.warning("Could not read offline original for photo %s", photo["id"], exc_info=True)
            return None
        if not row or not row[column]:
            return None
        cached = row[column]
        if not os.path.isabs(cached):
            vireo_dir = os.path.dirname(config["THUMB_CACHE_DIR"])
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


def _serve_photo_thumbnail(get_db, config, db, photo_id, photo, filename):
    """Serve or self-heal the thumbnail of a photo the caller may show."""
    thumb_dir = config["THUMB_CACHE_DIR"]
    folder_row = db.folders.get(photo["folder_id"])
    if not folder_row:
        return "", 404
    pair_source, pair_source_path = _requested_pair_source(
        get_db, config, photo, folder_row["path"],
    )
    if pair_source and not pair_source_path:
        return "", 404

    thumb = _ThumbnailRequest(
        db,
        photo_id,
        photo,
        folder_row,
        filename,
        thumb_dir,
        pair_source,
        pair_source_path,
    )
    thumb.select_cache_target()
    response = thumb.serve_fresh_cache()
    if response is not None:
        return response

    # Self-heal path: regenerate on miss (or stale) when the photo
    # still exists.
    thumb.prepare_self_heal()
    try:
        thumb.resolve_render_source()
        if thumb.would_retry_failed_raw_decode():
            log.info(
                "Skipping thumbnail self-heal for photo %s; selected source "
                "would retry a RAW decode that already failed for current "
                "source mtime",
                photo_id,
            )
            return "", 404
        result = thumb.generate()
    except Exception:  # broad: self-heal runs any decoder; a failure is logged and answers 404
        log.exception(
            "Thumbnail self-heal failed for photo %s (source=%s)",
            photo_id, photo["filename"],
        )
        return "", 404

    if not result:
        # generate_thumbnail logged the reason (unreadable source,
        # unsupported format, etc.). Nothing else to do here.
        _record_working_copy_failure(db, photo, thumb.source)
        return "", 404

    thumb.peg_mtime_to_source(result)
    thumb.persist_thumb_path()
    return _send_cached_thumbnail(thumb_dir, thumb.cache_filename)


def _mask_file_is_db_backed(get_db, filename, mask_path):
    """Whether a mask file on disk is actually this photo's.

    ``masks/<id>.png`` and ``masks/<id>.<variant>.png`` are keyed by
    bare photo id, and ``photos.id`` recycles freed rowids, so file
    existence alone does not mean the file belongs to the photo now
    holding that id. ``photo_masks`` rows cascade away with the photo,
    so requiring one is what distinguishes "this photo's mask" from
    "a mask the previous owner of this rowid left behind" — and unlike
    a timestamp check it can't be fooled by a stale file that happens
    to be newer than the new photo's source.

    Callers only reach this with an id-keyed filename; ``serve_mask``
    refuses the rest because it cannot scope them to a workspace.
    """
    m = re.match(r"^(\d+)[._]", filename)
    if not m:
        return True
    pid = int(m.group(1))
    db = get_db()
    real = os.path.realpath(mask_path)
    for mask in db.masks_features.list_masks_for_photo(pid):
        if mask["path"] and os.path.realpath(mask["path"]) == real:
            return True
    mask_path = db.masks_features.photo_mask_path(pid)
    return bool(mask_path and os.path.realpath(mask_path) == real)


def _read_mask_bytes(mask_path):
    """Read an immutable mask generation, or lose a cleanup race.

    POSIX keeps an already-open unlinked file readable, while Windows
    refuses to unlink an open handle. The only race to recover from is
    cleanup winning before ``open``; callers then re-resolve the current
    database path and retry without waiting for a long-running writer.
    """
    try:
        with open(mask_path, "rb") as handle:
            return handle.read()
    except OSError:
        return None


def _load_crop_source(config, db, photo):
    """Decode the crop source: the working copy first, then the original.

    Returns ``_CROP_FOLDER_MISSING`` when there is no working copy and
    the photo's folder row is gone, and ``None`` when nothing decoded.
    """
    from image_loader import load_image

    # Try working copy first, fall back to original
    vireo_dir = os.path.dirname(config["THUMB_CACHE_DIR"])
    image_path = None
    using_working_copy = False
    if photo["working_copy_path"]:
        wc = os.path.join(vireo_dir, photo["working_copy_path"])
        if os.path.exists(wc):
            image_path = wc
            using_working_copy = True
    if image_path is None:
        folder = db.folders.get(photo["folder_id"])
        if not folder:
            return _CROP_FOLDER_MISSING
        image_path = os.path.join(folder["path"], photo["filename"])

    if using_working_copy:
        # Stamp access recency — same pattern as
        # ``/photos/<id>/original`` and ``/photos/<id>/edit-preview``.
        # Without this, repeated interactive crop previews leave the
        # working copy's atime unchanged and it stays first in the
        # quota eviction queue even while the user is actively
        # working on the photo.
        #
        # Only the touch runs under the guard, and deliberately so.
        # ``working_copy_publication_guard`` is a process-wide lock
        # that the quota pass holds across a full scandir of the
        # cache; holding it across a decode as well would serialize
        # every interactive image read in the app against every
        # other one and against eviction, which is exactly the
        # stall this PR set out to remove. A `utime` under the lock
        # costs microseconds. If eviction unlinks the file between
        # the touch and the open, the decode returns None and the
        # original-source fallback below handles it — that path
        # already exists for precisely this race.
        with working_copy_publication_guard():
            touch_working_copy_access(image_path)
    preview_size = 1024 if request.args.get("detection_id") is not None else None
    img = load_image(image_path, max_size=preview_size)
    if img is None and using_working_copy:
        # Quota enforcement can unlink the working copy after the
        # existence check above but before Pillow opens it. Re-resolve
        # the primary source once so an otherwise healthy crop request
        # does not become a transient 500 during eviction.
        folder = db.folders.get(photo["folder_id"])
        original_path = (
            os.path.join(folder["path"], photo["filename"])
            if folder else None
        )
        if original_path and os.path.isfile(original_path):
            img = load_image(original_path, max_size=preview_size)
    return img


def _crop_to_detection(img, dets, requested_detection):
    """Crop to the primary (or requested) subject box with its padding."""
    import config as cfg

    det_box = None
    if dets:
        det_row = dets[0]
        det_box = {
            "x": det_row["box_x"], "y": det_row["box_y"],
            "w": det_row["box_w"], "h": det_row["box_h"],
        }
    if requested_detection is not None:
        from subjects import suggested_crop
        det_box = suggested_crop(det_row)
    if det_box:
        iw, ih = img.size
        padding = 0 if requested_detection is not None else cfg.load().get("detection_padding", 0.2)
        pad_w = det_box["w"] * padding
        pad_h = det_box["h"] * padding
        x1 = max(0, int((det_box["x"] - pad_w) * iw))
        y1 = max(0, int((det_box["y"] - pad_h) * ih))
        x2 = min(iw, int((det_box["x"] + det_box["w"] + pad_w) * iw))
        y2 = min(ih, int((det_box["y"] + det_box["h"] + pad_h) * ih))
        crop = img.crop((x1, y1, x2, y2))
        if requested_detection is not None or (crop.size[0] >= 50 and crop.size[1] >= 50):
            img = crop
    return img


def _focus_mask_bytes(db_path, db, photo_id):
    """Only read the recorded mask inside this catalog's mask directory."""
    mask_path = db.masks_features.photo_mask_path(photo_id)
    if not mask_path:
        return None
    masks_dir = os.path.realpath(os.path.join(os.path.dirname(db_path), "masks"))
    abs_mask = os.path.realpath(mask_path)
    inside = abs_mask == masks_dir or abs_mask.startswith(masks_dir + os.sep)
    return _read_mask_bytes(abs_mask) if inside else None


def _focus_source_image(config, photo, folder, source_choice):
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
            dict(photo), os.path.dirname(config["THUMB_CACHE_DIR"]), max_size=None,
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


def _paired_cache_path_for_request(
    preview_dir, photo, photo_id, size, pair_source, pair_source_path,
    render_recipe, bypass_cache,
):
    """The source-keyed shadow cache path, or ``None`` for ordinary previews."""
    # Paired renders coordinate through a distinct source-aware artifact
    # so a browser warmup (prefetch=1) and the visible request that
    # follows can share one decode instead of racing two — the paired
    # URLs necessarily differ (prefetch vs no prefetch) so the browser
    # cannot coalesce them and the server must.
    # Key the paired shadow cache by render/source state so a changed
    # edit recipe or a swapped paired source produces a distinct
    # filename. Otherwise repeatedly accessed entries would keep
    # returning bytes rendered from the previous state indefinitely —
    # regular preview-cache invalidation does not remove these source-keyed
    # files. Old states remain accounted for until LRU eviction.
    return (
        _paired_preview_path(
            preview_dir,
            photo_id,
            size,
            pair_source,
            _paired_render_state_hash(
                photo, size, pair_source, pair_source_path, render_recipe,
            ),
        )
        if bypass_cache
        else None
    )


def _serve_cached_preview(
    db, photo_id, size, cache_path, paired_cache_path, vireo_dir, *,
    bypass_cache, stale_after_failed_invalidation,
    skip_untracked_preview_adoption,
):
    """Serve a tracked, adoptable or paired cache hit; ``None`` on a miss."""
    from flask import send_file

    # Cache hit (tracked): touch and serve. The touch is best-effort
    # bookkeeping — under concurrent traffic SQLite can raise
    # OperationalError: database is locked, but that shouldn't turn a
    # valid cache hit into a 500 when the JPEG is right there on disk.
    if (
        not bypass_cache
        and not stale_after_failed_invalidation
        and db.caches.preview_get(photo_id, size)
        and os.path.exists(cache_path)
    ):
        with contextlib.suppress(sqlite3.Error):
            db.caches.preview_touch(photo_id, size)
        return send_file(cache_path, mimetype="image/jpeg")

    # Cache hit (on-disk but untracked): lazy adoption.
    # caches.preview_insert uses time.time() for last_access_at, so the
    # adopted entry is ranked as freshly-accessed in the LRU in a single
    # commit (instead of insert-with-mtime-then-touch-to-now).
    # Read bytes into memory before evicting: eviction may delete the
    # file we just adopted (e.g. preview_cache_max_mb=0), but we can
    # still serve the response from memory — mirrors the miss path.
    if (
        not bypass_cache
        and not stale_after_failed_invalidation
        and not skip_untracked_preview_adoption
        and os.path.exists(cache_path)
    ):
        with open(cache_path, "rb") as f:
            data = f.read()
        try:
            db.caches.preview_insert(photo_id, size, len(data))
            evict_preview_cache_if_over_quota(db, vireo_dir)
        except Exception:
            # broad: the bytes are already in memory; bookkeeping must not
            # turn a cache hit into a 500.
            log.warning("Could not adopt untracked preview for photo %s", photo_id, exc_info=True)
        return Response(data, mimetype="image/jpeg")

    # Paired-render cache hit: an earlier equal-key producer already
    # decoded and atomically published these bytes to the paired shadow
    # cache, so waiters skip repeating that work.
    if (
        paired_cache_path
        and paired_preview_ready(db, paired_cache_path)
    ):
        with contextlib.suppress(sqlite3.Error):
            db.caches.paired_preview_touch(os.path.basename(paired_cache_path))
        return send_file(paired_cache_path, mimetype="image/jpeg")
    return None


def _materialize_requested_preview(
    db, photo, photo_id, size, folder_row, vireo_dir, cache_path,
    render_recipe, bypass_cache, pair_source, pair_source_path,
    _prepare_source,
):
    """Render the preview; ``None`` when the source could not be decoded."""
    import config as cfg

    preview_quality = cfg.load().get("preview_quality", 90)
    try:
        return materialize_preview(
            db,
            photo,
            folder_row["path"],
            size=size,
            vireo_dir=vireo_dir,
            preview_quality=preview_quality,
            recipe=render_recipe,
            cache_path=None if bypass_cache else cache_path,
            pair_source=pair_source,
            pair_source_path=pair_source_path,
            coordinate=False,
            publish_best_effort=True,
            publication_guard=lambda: _preparation_publication(
                db, photo_id, _prepare_source,
                captured_recipe=render_recipe,
            ),
        )
    except PreviewMaterializationError:
        return None


class _PreviewServer:
    """The ``/photos/<id>/preview`` and ``/full`` cache path behind ``_serve_preview``.

    Built once per blueprint around the per-app state its steps share: the
    request database getter, the app config (``THUMB_CACHE_DIR`` is read when
    a request runs), and the app's ``RenderCache`` preview-invalidation set
    and clearer. ``_serve_preview`` re-enters itself through the equal-key
    artifact flight.
    """

    def __init__(
        self, get_db, config, *, invalid_preview_cache_paths,
        clear_preview_cache_invalid,
    ):
        self._get_db = get_db
        self._config = config
        self._invalid_preview_cache_paths = invalid_preview_cache_paths
        self._clear_preview_cache_invalid = clear_preview_cache_invalid


    def allowed_preview_sizes(self):
        """Allowlist for /photos/<id>/preview?size=N.

        Includes the fixed tier plus the user-configured preview_max_size
        so /full can delegate here. Reads workspace-effective config so
        a per-workspace preview_max_size override is honored.
        """
        import config as cfg
        effective = self._get_db().get_effective_config(cfg.load())
        fixed = set(PREVIEW_TIER_SIZES)
        pm = effective.get("preview_max_size") or 1920
        if pm == 0:
            return fixed  # 0 = "full" — handled by /original path
        return fixed | {int(pm)}

    def _reconcile_preview_cache_file(
        self, db, photo, photo_id, size, cache_path, recipe, render_recipe, bypass_cache,
    ):
        """Drop corrupt, invalidated and stale untracked preview files.

        Returns ``(stale_after_failed_invalidation,
        skip_untracked_preview_adoption)``: whether the cached file is still
        known stale (it could not be removed) and whether an on-disk file
        that failed removal must not be adopted as a cache hit.
        """
        # Reject corrupt zero-byte cache files (prior write interrupted).
        # Treat them as a miss so the regeneration path below produces a
        # real preview.
        if (
            not bypass_cache
            and os.path.exists(cache_path)
            and os.path.getsize(cache_path) == 0
        ):
            with contextlib.suppress(OSError):
                os.remove(cache_path)
            db.caches.preview_delete(photo_id, size)  # no-op if no row

        skip_untracked_preview_adoption = False
        stale_after_failed_invalidation = (
            cache_path in self._invalid_preview_cache_paths
            or _is_preview_cache_invalid(db, photo_id, size)
            or (os.path.exists(cache_path) and not _camera_cache_matches(cache_path, photo, render_recipe))
        )
        if (
            not bypass_cache
            and stale_after_failed_invalidation
            and os.path.exists(cache_path)
        ):
            try:
                os.remove(cache_path)
            except OSError:
                skip_untracked_preview_adoption = True
                log.warning(
                    "Failed to remove previously invalidated preview cache %s",
                    cache_path, exc_info=True,
                )
            else:
                self._invalid_preview_cache_paths.discard(cache_path)
                self._clear_preview_cache_invalid(db, photo_id, size)
                db.caches.preview_delete(photo_id, size)
                stale_after_failed_invalidation = False
        elif not bypass_cache and stale_after_failed_invalidation:
            self._invalid_preview_cache_paths.discard(cache_path)
            self._clear_preview_cache_invalid(db, photo_id, size)
            db.caches.preview_delete(photo_id, size)
            stale_after_failed_invalidation = False
        if (
            not bypass_cache
            and recipe
            and os.path.exists(cache_path)
            and not db.caches.preview_get(photo_id, size)
        ):
            try:
                os.remove(cache_path)
            except OSError:
                skip_untracked_preview_adoption = True
                log.warning(
                    "Failed to remove stale untracked preview cache %s",
                    cache_path, exc_info=True,
                )
        return stale_after_failed_invalidation, skip_untracked_preview_adoption

    def _coordinate_preview_flight(
        self, photo_id, size, cache_path, paired_cache_path, bypass_cache,
        _prepare_source,
    ):
        """Run the cache-miss render under the equal-key artifact flight."""
        artifact_key = os.path.abspath(
            paired_cache_path if bypass_cache else cache_path,
        )
        speculative = request.args.get("prefetch") == "1"
        speculative_slot = False
        if speculative:
            speculative_slot = preview_prefetch_slots.acquire(blocking=False)
            if not speculative_slot:
                return _shed_prefetch_response()

        def coordinated_request(*, guarded):
            response = make_response(
                self._serve_preview(
                    photo_id, size, _artifact_flight_guarded=guarded,
                    _prepare_source=_prepare_source,
                )
            )
            if response.status_code >= 400:
                raise _ArtifactResponseError(response)
            return response

        try:
            result = preview_artifact_flights.run(
                artifact_key,
                lambda: coordinated_request(guarded=True),
                lambda: coordinated_request(guarded=False),
                join=not speculative,
            )
            if result.skipped:
                return _shed_prefetch_response()
            return result.value
        except _ArtifactResponseError as exc:
            return exc.to_response()
        except ArtifactProducerFailed as exc:
            if isinstance(exc.__cause__, _ArtifactResponseError):
                return exc.__cause__.to_response()
            if isinstance(exc.__cause__, PreviewMaterializationError):
                return "Could not load image", 500
            raise
        finally:
            if speculative_slot:
                preview_prefetch_slots.release()

    def _record_rendered_preview(
        self, db, photo_id, photo, size, cache_path, paired_cache_path, vireo_dir,
        bypass_cache, rendered,
    ):
        """Bookkeeping after a render: clear invalidation, evict, publish paired bytes."""
        if not bypass_cache and rendered.published:
            self._invalid_preview_cache_paths.discard(cache_path)
            self._clear_preview_cache_invalid(db, photo_id, size)
            try:
                evict_preview_cache_if_over_quota(db, vireo_dir)
            except Exception:  # broad: eviction is maintenance; the render is already in memory
                log.warning("Preview cache eviction failed after render", exc_info=True)
        # Publish the paired-render bytes so an equal-key follower still
        # inside preview_artifact_flights.run() finds a ready artifact when
        # it re-enters _serve_preview instead of decoding the same source.
        # Best-effort: a full or read-only disk must never turn a successful
        # producer render into a 500.
        if bypass_cache and paired_cache_path and rendered.data is not None:
            try:
                # Publication and registration share the source-identity
                # guard: a deletion/reimport during decoding must not attach
                # the old pixels to the new photo's durable cache entry.
                with _preparation_publication(db, photo_id, photo):
                    atomic_write_bytes(rendered.data, paired_cache_path)
                    db.caches.paired_preview_insert(
                        photo_id, os.path.basename(paired_cache_path), len(rendered.data),
                    )
                evict_preview_cache_if_over_quota(db, vireo_dir)
            except Exception:  # broad: paired-cache publication is best-effort; the render is served from memory
                log.warning(
                    "Failed to publish paired preview cache %s",
                    paired_cache_path, exc_info=True,
                )

    def _serve_preview(
        self, photo_id, size, *, _artifact_flight_guarded=False, _prepare_source=None,
    ):
        """Serve a preview at the given size, using the preview_cache LRU.

        This is the single code path behind both /photos/<id>/preview and
        /photos/<id>/full. Callers have already validated size.

        ``_prepare_source`` is the photo row full-resolution preparation
        selected: the render publishes only while the catalog still holds
        that source (see ``_preparation_publication``).
        """
        from flask import send_file

        # Confirm the photo still exists before any cache return so that a
        # deleted photo can't be served from a stale per-size cache (and so
        # SQLite id reuse can't surface the wrong image). verify_workspace
        # keeps cross-workspace photos from being served here at all
        # (mirrors serve_thumbnail).
        db = self._get_db()
        photo = db.photos.get(photo_id, verify_workspace=True)
        if not photo:
            return "Not found", 404
        if _prepare_source is not None:
            from offline_cache import photo_source_matches

            if not photo_source_matches(_prepare_source, photo):
                return "Photo source changed", 404

        vireo_dir = os.path.dirname(self._config["THUMB_CACHE_DIR"])
        preview_dir = os.path.join(vireo_dir, "previews")
        cache_path = os.path.join(preview_dir, f"{photo_id}_{size}.jpg")
        recipe = db.get_photo_edit_recipe(photo_id)
        folder_row = db.folders.get(photo["folder_id"])
        if not folder_row:
            return "Not found", 404
        pair_source, pair_source_path = _requested_pair_source(
            self._get_db, self._config, photo, folder_row["path"],
        )
        if pair_source and not pair_source_path:
            return "Not found", 404
        # Recipes and local masks use primary-RAW coordinates. The selected
        # developed JPEG may have different geometry and should be displayed
        # as-authored rather than receiving the RAW edit a second time.
        render_recipe = None if pair_source == "jpeg" else recipe
        # The established preview cache is keyed only by (photo_id, size).
        # Explicit paired-source views bypass it so RAW and JPEG pixels can
        # never contaminate one another. Both indexes share the preview disk LRU.
        bypass_cache = pair_source is not None
        paired_cache_path = _paired_cache_path_for_request(
            preview_dir, photo, photo_id, size, pair_source, pair_source_path,
            render_recipe, bypass_cache,
        )

        stale_after_failed_invalidation, skip_untracked_preview_adoption = (
            self._reconcile_preview_cache_file(
                db, photo, photo_id, size, cache_path, recipe, render_recipe,
                bypass_cache,
            )
        )
        cached = _serve_cached_preview(
            db, photo_id, size, cache_path, paired_cache_path, vireo_dir,
            bypass_cache=bypass_cache,
            stale_after_failed_invalidation=stale_after_failed_invalidation,
            skip_untracked_preview_adoption=skip_untracked_preview_adoption,
        )
        if cached is not None:
            return cached

        # Cache miss: coordinate every durable preview producer in this
        # process before decoding.  The recursive producer re-runs all cache
        # and invalidation checks under the flight; equal-key waiters then
        # re-enter once more and serve the atomically published JPEG through
        # their own Flask request context.  Paired renders are coordinated
        # too, keyed by the source-aware paired path so RAW and JPEG variants
        # of the same photo never share a flight — the shared artifact for
        # waiters live at that paired path, isolated from ordinary previews.
        if not _artifact_flight_guarded:
            return self._coordinate_preview_flight(
                photo_id, size, cache_path, paired_cache_path, bypass_cache,
                _prepare_source,
            )

        rendered = _materialize_requested_preview(
            db, photo, photo_id, size, folder_row, vireo_dir, cache_path,
            render_recipe, bypass_cache, pair_source, pair_source_path,
            _prepare_source,
        )
        if rendered is None:
            return "Could not load image", 500

        self._record_rendered_preview(
            db, photo_id, photo, size, cache_path, paired_cache_path, vireo_dir,
            bypass_cache, rendered,
        )
        if rendered.data is not None:
            return Response(rendered.data, mimetype="image/jpeg")
        return send_file(cache_path, mimetype="image/jpeg")


def create_media_blueprint(
    get_db,
    json_error,
    db_path,
    config,
    *,
    invalid_preview_cache_paths,
    clear_preview_cache_invalid,
    render_edit_preview,
    cancel_edit_preview,
):
    """Build the image- and mask-serving blueprint.

    ``config`` is the Flask app's config mapping; ``THUMB_CACHE_DIR`` (and
    the Vireo data directory beside it) is read when a request runs rather
    than when the app is built. ``db_path`` locates the ``masks`` directory
    beside the catalog.

    ``invalid_preview_cache_paths`` (the app's one set of preview files that
    could not be unlinked) and ``clear_preview_cache_invalid`` are the app's
    ``services.render_cache.RenderCache`` methods: the edit-recipe render-cache
    invalidation writes the same state ``/photos/<id>/preview`` reads.

    ``create_app`` also looks up the registered ``serve_original_photo`` view
    so the prepare-full-resolution job renders through this canonical path.
    """
    blueprint = Blueprint("media", __name__)
    previews = _PreviewServer(
        get_db, config,
        invalid_preview_cache_paths=invalid_preview_cache_paths,
        clear_preview_cache_invalid=clear_preview_cache_invalid,
    )

    @blueprint.route("/thumbnails/<filename>")
    def serve_thumbnail(filename):
        """Serve a 400px JPEG thumbnail for a photo, regenerating on miss.

        Self-heal: if the thumbnail file is missing on disk but the photo
        still exists in the database, we (re)generate the thumbnail in
        the request thread, persist ``photos.thumb_path`` so the coverage
        dashboard sees it, and serve it. The user never sees a broken
        image when the source is recoverable — matching the project's
        "no rm -rf fixes; the app self-heals" rule.

        Stale-cache guard: ``photos.id`` is INTEGER PRIMARY KEY *without*
        AUTOINCREMENT, so SQLite reuses the highest freed rowids on the
        next insert. Combined with delete paths that don't unlink cached
        files (``audit.remove_orphans``, folder consolidation in
        ``Database.consolidate_folders``, companion-pair dedup in
        ``scanner._pair_raw_jpeg_companions``), a JPEG at ``<id>.jpg`` can
        outlive its original photo and then be served as the thumbnail
        for an entirely different photo that later inherits that ID.
        Compare the cached file's mtime against the photo's
        ``file_mtime`` and treat predates-source as a miss so the regen
        path produces a fresh JPEG. This is correctness rooted in the
        data, independent of whether every delete site cleans up.

        404s remain when:
          * the filename isn't ``<int>.jpg`` (malformed request);
          * the photo no longer exists in the DB (stale cache from a
            previous delete — handled by ``pipeline.prune_results``
            going forward);
          * the source image is unreadable (e.g. RAW decode failure).
            ``generate_thumbnail`` already logs the underlying cause.
        """
        try:
            photo_id = int(filename.replace(".jpg", ""))
        except ValueError:
            log.warning("Thumbnail request with non-numeric filename: %s", filename)
            return "", 404

        db = get_db()
        # Workspace-scoped lookup hoisted before any cache mutation. A
        # thumbnail URL only makes sense for a photo the active
        # workspace can actually see, and the staleness branch below
        # may unlink the cached file — that mutation must not run for
        # a photo outside the active workspace, otherwise an ill-aimed
        # request from workspace B can wipe workspace A's cache as a
        # side effect. Doing this lookup once also lets the freshness
        # check use ``photo["file_mtime"]`` directly instead of a
        # separate SELECT. (Without this scope,
        # ``get_canonical_image_path`` would also receive an empty
        # folders dict for a cross-workspace photo and fall back to
        # ``os.path.join('', photo['filename'])`` — a CWD-relative
        # path that could read or persist a thumbnail derived from an
        # unrelated same-named file.)
        photo = db.photos.get(photo_id, verify_workspace=True)
        if not photo:
            # Stale URL — the photo was deleted, or it lives in a folder
            # that isn't linked to this workspace. 404 is the right
            # answer; cache pruning is the upstream fix (PR #758). We
            # do NOT touch the cached file here — another workspace may
            # legitimately own it.
            return "", 404
        return _serve_photo_thumbnail(get_db, config, db, photo_id, photo, filename)

    @blueprint.route("/thumbnails/duplicate/<filename>")
    def serve_duplicate_thumbnail(filename):
        """``serve_thumbnail`` for a member of a duplicate group, in any workspace.

        The duplicate scan is library-wide, so the Duplicates page shows
        copies the active workspace does not, and ``/thumbnails/<filename>``
        404s on those. This route replaces the workspace check with
        duplicate-group membership (a photo sharing its ``file_hash`` with
        another row), which is exactly the set that page can show; any
        other photo still 404s. The folder comes from ``folders.get`` by id,
        which is not workspace-scoped, so the source path never falls back
        to a CWD-relative join.
        """
        try:
            photo_id = int(filename.replace(".jpg", ""))
        except ValueError:
            log.warning("Thumbnail request with non-numeric filename: %s", filename)
            return "", 404

        db = get_db()
        if not db.duplicates.is_group_member(photo_id):
            return "", 404
        photo = db.photos.get(photo_id)
        if not photo:
            return "", 404
        return _serve_photo_thumbnail(get_db, config, db, photo_id, photo, filename)

    @blueprint.route("/masks/<filename>")
    def serve_mask(filename):
        """Serve mask PNG files.

        Legacy callers (e.g. ``openInspect`` in pipeline_review.html) still
        request ``/masks/{photo_id}.png``. Variant-aware extraction now
        writes ``{photo_id}.{variant}.png``, so when the literal filename
        isn't on disk and the request looks like ``{photo_id}.png``, fall
        back to the photo's active mask via ``photo_masks`` so the inspect
        panel keeps working without a round-trip API call.
        """
        masks_dir = os.path.join(os.path.dirname(db_path), "masks")
        mask_path = os.path.join(masks_dir, filename)
        id_match = re.match(r"^(\d+)[._]", filename)
        if not id_match:
            # Every mask writer names files ``<photo_id>[.<variant>].png``.
            # A file without that prefix can't be tied to a photo, so it
            # can't be scoped to the active workspace -- refuse it.
            return "", 404

        pid = int(id_match.group(1))
        # Same boundary as /api/masks/<pid>/<variant>.png: a mask is only
        # served for a photo the active workspace can see.
        if get_db().photos.get(pid, verify_workspace=True) is None:
            return "", 404
        if os.path.exists(mask_path) and _mask_file_is_db_backed(
            get_db, filename, mask_path,
        ):
            mask_bytes = _read_mask_bytes(mask_path)
            if mask_bytes is not None:
                return Response(mask_bytes, mimetype="image/png")

        if re.match(r"^(\d+)\.png$", filename):
            db = get_db()
            # A committed immutable generation can be unlinked after the DB
            # lookup but before open. Re-resolve after that narrow race; do
            # not block this HTTP request on full model regeneration.
            for _attempt in range(3):
                active = db.masks_features.active_variant(pid)
                if active:
                    mask = db.masks_features.get_mask(pid, active)
                    if mask and mask.get("path"):
                        masks_dir_real = os.path.realpath(masks_dir)
                        abs_path = os.path.realpath(mask["path"])
                        if (
                            abs_path == masks_dir_real
                            or abs_path.startswith(masks_dir_real + os.sep)
                        ):
                            mask_bytes = _read_mask_bytes(abs_path)
                            if mask_bytes is not None:
                                return Response(
                                    mask_bytes, mimetype="image/png",
                                )
        return "", 404

    # Variant strings live in URLs and must not be path-traversal vectors.
    # Allow only alnum / dash / underscore — covers every variant we
    # actually emit (sam2-small, sam2-large, sam3-small, unknown) and
    # excludes `.`, `/`, and `..` outright.
    _MASK_VARIANT_RE = re.compile(r"^[A-Za-z0-9_-]+$")

    @blueprint.route("/api/masks/<int:pid>/<variant>.png")
    def api_serve_mask(pid, variant):
        """Serve the mask file recorded on the ``photo_masks`` row.

        Reads the stored ``photo_masks.path`` instead of reconstructing
        ``{pid}.{variant}.png`` so migrated legacy masks (backfilled as
        ``variant='unknown'`` pointing at the old ``{pid}.png`` filename)
        remain viewable in the lightbox.

        Defense in depth: the variant must (1) match the conservative
        whitelist regex, (2) correspond to an actual ``photo_masks`` row,
        and (3) the stored path must resolve inside the masks directory
        (so an attacker-controlled or corrupted DB row can't escape).
        """
        if not _MASK_VARIANT_RE.match(variant):
            return "", 404
        for _attempt in range(3):
            db = get_db()
            if db.photos.get(pid, verify_workspace=True) is None:
                return "", 404
            mask = db.masks_features.get_mask(pid, variant)
            if mask is None or not mask.get("path"):
                return "", 404
            masks_dir = os.path.realpath(
                os.path.join(os.path.dirname(db_path), "masks")
            )
            abs_path = os.path.realpath(mask["path"])
            if not (abs_path == masks_dir
                    or abs_path.startswith(masks_dir + os.sep)):
                return "", 404
            mask_bytes = _read_mask_bytes(abs_path)
            if mask_bytes is not None:
                # Response owns the bytes, so cleanup after open cannot turn
                # this request into a transient 404 or truncated stream.
                return Response(mask_bytes, mimetype="image/png")
        return "", 404

    @blueprint.route("/api/photos/<int:pid>/masks")
    def api_photo_masks(pid):
        """List a photo's available SAM mask variants and the active one.

        Powers the lightbox variant-toggle dropdown: the UI fetches this
        when opening / navigating to a photo and builds one option per
        returned variant plus a default "active" option.
        """
        db = get_db()
        if db.photos.get(pid, verify_workspace=True) is None:
            return photo_not_found_error()
        masks = db.masks_features.list_masks_for_photo(pid)
        active = db.masks_features.active_variant(pid)
        return jsonify({
            "photo_id": pid,
            "active": active,
            "variants": [
                {
                    "variant": m["variant"],
                    "url": f"/api/masks/{pid}/{m['variant']}.png",
                    "created_at": m["created_at"],
                }
                for m in masks
            ],
        })

    @blueprint.route("/photos/<int:photo_id>/crop")
    def serve_crop_preview(photo_id):
        """Serve the cropped region that would be sent to BioCLIP."""
        import config as cfg
        from PIL import Image

        db = get_db()
        # verify_workspace: don't serve image bytes for photos hidden from
        # the active workspace (mirrors serve_thumbnail).
        photo = db.photos.get(photo_id, verify_workspace=True)
        if not photo:
            return "Not found", 404

        # Get primary detection box — global detections table, threshold
        # resolved from workspace-effective config in get_detections.
        dets = db.get_detections(photo_id)
        requested_detection = request.args.get("detection_id")
        if requested_detection is not None:
            try:
                requested_detection = int(requested_detection)
            except ValueError:
                return json_error("Invalid detection_id")
            dets = [d for d in dets if d["id"] == requested_detection
                    and d["category"] == "animal" and d["detector_model"] != "full-image"]
            if not dets:
                return json_error("Subject not found", 404)

        img = _load_crop_source(config, db, photo)
        if img is _CROP_FOLDER_MISSING:
            return "Not found", 404
        if img is None:
            return "Could not load image", 500

        img = _crop_to_detection(img, dets, requested_detection)

        if requested_detection is not None and request.args.get("suggested") == "1":
            exposure_ev = db.detections.subject_exposure_ev(requested_detection)
            if exposure_ev is not None:
                from image_edits import apply_recipe
                img = apply_recipe(img, {"adjustments": {"exposure": exposure_ev}})
        img.thumbnail((800, 800), Image.LANCZOS)
        import io

        preview_quality = cfg.load().get("preview_quality", 90)
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=preview_quality)
        buf.seek(0)
        response = Response(buf.read(), mimetype="image/jpeg")
        if requested_detection is not None:
            # URLs include the analysis source fingerprint. Reusing a crop in
            # the subject strip or toggling correction can use browser cache.
            response.cache_control.private = True
            response.cache_control.max_age = 3600 if request.args.get("v") else 0
            response.add_etag()
            response.make_conditional(request)
        return response

    @blueprint.route("/photos/<int:photo_id>/focus-crop")
    def serve_focus_crop(photo_id):
        """An unresampled window around the scorer's focus point.

        Auto follows workspace eye scoring, then the primary subject or frame;
        ``center=x,y`` uses normalized coordinates in the unedited upright
        source raster, before any crop/rotation recipe. ``source`` picks the pixels: ``auto``
        (default) serves the working copy unless it is downsized against the
        catalog dimensions and the original can be decoded, ``working`` keeps
        the working copy, ``original`` insists on the original (404 when it
        is not on disk). Scale 1 means pixels of the decoded upright raster;
        ``X-Vireo-Focus-Original-Scale`` says how those relate to the
        original when the catalog knows its dimensions.
        """
        import io

        import config as cfg
        from burst_evidence import (
            DEFAULT_FOCUS_CROP_SIZE,
            MAX_FOCUS_CROP_SIZE,
            MIN_FOCUS_CROP_SIZE,
            numeric_query_arg,
        )
        from focus_crop import (
            focus_headers,
            focus_point,
            focus_subject_detections,
            focus_window,
            original_scale,
            parse_center,
            shade_focus_mask,
        )
        from PIL import Image
        from scoring import _normalize_scoring_config

        db = get_db()
        photo = db.photos.get(photo_id, verify_workspace=True)
        if not photo:
            return "Not found", 404
        try:
            size = numeric_query_arg(request.args, "size", DEFAULT_FOCUS_CROP_SIZE,
                                     int, MIN_FOCUS_CROP_SIZE, MAX_FOCUS_CROP_SIZE)
        except ValueError as exc:
            return json_error(str(exc))
        basis = (request.args.get("basis") or "auto").strip().lower()
        if basis not in {"auto", "eye", "subject", "frame"}:
            return json_error("basis must be auto, eye, subject or frame")
        source_choice = (request.args.get("source") or "auto").strip().lower()
        if source_choice not in {"auto", "working", "original"}:
            return json_error("source must be auto, working or original")
        point = None
        if request.args.get("center") is not None:
            try:
                point = parse_center(request.args.get("center"))
            except ValueError as exc:
                return json_error(str(exc))
        effective = db.get_effective_config(cfg.load())
        eye_enabled = bool(_normalize_scoring_config(effective).get("eye_detect_enabled", False))
        boxes = (
            focus_subject_detections(db, photo_id)
            if point is None and basis in {"auto", "subject"} else []
        )
        try:
            center, used = focus_point(photo, boxes, basis, eye_enabled=eye_enabled, point=point)
        except ValueError as exc:
            return json_error(str(exc), 404)
        img, source = _focus_source_image(config, photo, db.folders.get(photo["folder_id"]), source_choice)
        if img is None:
            if source_choice in {"working", "original"}:
                return json_error(f"The {source_choice} image is not available", 404)
            return "Could not load image", 500
        window, center = focus_window(img.size, center, size)
        crop = img.crop(window).convert("RGB")
        mask_bytes = _focus_mask_bytes(db_path, db, photo_id) if request.args.get("mask") == "1" else None
        if mask_bytes is not None:
            crop = shade_focus_mask(crop, mask_bytes, img.size, window, Image)
        buf = io.BytesIO()
        crop.save(buf, format="JPEG", quality=effective.get("preview_quality", 90))
        response = Response(buf.getvalue(), mimetype="image/jpeg")
        response.headers.update(focus_headers(
            used, center, window, img.size, source, mask_bytes is not None,
            scale=original_scale(img.size, photo),
        ))
        response.cache_control.private = True
        response.cache_control.max_age = 0
        return response

    @blueprint.route("/photos/<int:photo_id>/full")
    def serve_full_photo(photo_id):
        """Serve a display-sized preview (alias for /preview at preview_max_size).

        Reads workspace-effective config so a per-workspace preview_max_size
        override is honored. preview_max_size == 0 historically meant "full"
        — we route to /original rather than generate a preview. Using a
        separate read/fallback (instead of `cfg.get(...) or 1920`) keeps
        the 0 sentinel reachable.
        """
        import config as cfg
        from flask import redirect

        effective = get_db().get_effective_config(cfg.load())
        pm = effective.get("preview_max_size")
        if pm == 0:
            params = []
            source = (request.args.get("source") or "").strip().lower()
            if source in {"jpeg", "raw"}:
                params.append(f"source={source}")
            # Forward prefetch so speculative warmups keep their nonblocking
            # slot on the /original path — otherwise the redirect launders
            # them into interactive requests and starts an expensive decode
            # while the current image is still on screen.
            if request.args.get("prefetch") == "1":
                params.append("prefetch=1")
            suffix = f"?{'&'.join(params)}" if params else ""
            return redirect(f"/photos/{photo_id}/original{suffix}")
        return previews._serve_preview(photo_id, int(pm or 1920))

    @blueprint.route("/photos/<int:photo_id>/preview")
    def serve_photo_preview(photo_id, *, _prepare_source=None):
        """Serve a JPEG preview at a chosen max-size.

        Cache is LRU-tracked in the preview_cache table; on-disk files that
        predate this scheme are adopted lazily on first access.

        Query params:
          size: int — max dimension (longest side). Must be in
                allowed_preview_sizes() to avoid unbounded cache growth.
        """
        from flask import request

        try:
            size = int(request.args.get("size", "1920"))
        except ValueError:
            return "Invalid size", 400
        if size not in previews.allowed_preview_sizes():
            return "Unsupported size", 400
        return previews._serve_preview(photo_id, size, _prepare_source=_prepare_source)

    @blueprint.route("/photos/<int:photo_id>/edit-mask-preview")
    def serve_edit_mask_preview(photo_id):
        """Serve a recipe's transformed, feathered local-weight map as a
        tinted RGBA PNG aligned with the editor preview.

        The existing lightbox mask endpoint serves the live photo_masks file
        untouched; this one shows the pixels the renderer actually weights —
        the recipe's snapshot after geometry and feathering — so the editor
        overlay can never disagree with the saved render.

        A recipe with no local section previews the photo's active mask
        instead (the one the first local slider would freeze), feathered by
        the ``feather`` arg, so Show Mask works before any local edit.
        """
        db = get_db()
        photo = db.photos.get(photo_id, verify_workspace=True)
        if not photo:
            return "Not found", 404
        try:
            size = int(request.args.get("size", "1920"))
        except ValueError:
            return "Invalid size", 400
        # The overlay is stretched over the displayed editor image, which the
        # client caps at the overlay-preview size, so allocating a native-res
        # RGBA buffer here would be hundreds of MB to over 1 GB with no
        # visible benefit. Keep the mask endpoint at the overlay cap; only
        # /edit-preview needs the raised limit for true 1:1 zoom.
        size = max(256, min(3840, size))

        raw_recipe = request.args.get("recipe")
        if raw_recipe:
            try:
                recipe = json.loads(raw_recipe)
            except (TypeError, ValueError):
                return "Invalid recipe", 400
        else:
            recipe = db.get_photo_edit_recipe(photo_id) or {}

        import local_masks
        from image_edits import (
            LOCAL_FEATHER_RANGE,
            RecipeError,
            local_weight_map,
            normalize_recipe,
        )
        try:
            normalized = normalize_recipe(recipe) or {}
        except RecipeError as e:
            return str(e), 400
        preview_feather = None
        if not normalized.get("local"):
            try:
                preview_feather = float(request.args.get("feather", "0"))
            except ValueError:
                return "Invalid feather", 400
            lo, hi = LOCAL_FEATHER_RANGE
            if not math.isfinite(preview_feather) or not (
                lo <= preview_feather <= hi
            ):
                return "Invalid feather", 400
        apply_crop = request.args.get("apply_crop") == "1"
        # Crop editing uses the whole source; after the crop is committed the
        # editor displays the real cropped render and the overlay must follow.
        display_recipe = dict(normalized)
        if not apply_crop:
            display_recipe.pop("crop", None)

        vireo_dir = os.path.dirname(config["THUMB_CACHE_DIR"])
        if preview_feather is None:
            snapshot = local_masks.load_snapshot(vireo_dir, photo_id, normalized)
            if snapshot is None:
                return "No usable edit-mask snapshot", 404
        else:
            snapshot = _load_active_mask(db, photo_id)
            if snapshot is None:
                return "Photo has no active subject mask", 404
        native_dims = _recipe_source_dimensions(photo)
        source_dims, preview_detail_scale = _edit_mask_preview_geometry(
            photo, normalized, native_dims, size, apply_crop, snapshot,
        )
        weight = local_weight_map(
            snapshot, source_dims, display_recipe,
            native_size=native_dims,
            detail_scale=preview_detail_scale,
            feather=preview_feather,
        )
        if weight is None:
            return "Mask does not fit this photo", 404

        return Response(_tinted_mask_png(weight), mimetype="image/png")

    @blueprint.route('/api/edit-preview/cancel', methods=['POST'])
    def api_cancel_edit_preview():
        from services.preview_workers import parse_request

        body = request.get_json(silent=True) or {}
        if not isinstance(body, dict):
            return json_error('Expected a preview session and sequence')
        try:
            session, sequence = parse_request(body.get('session'), str(body.get('sequence', '')))
        except ValueError as error:
            return json_error(str(error))
        cancel_edit_preview(session, sequence)
        return '', 204

    @blueprint.route("/photos/<int:photo_id>/edit-preview")
    def serve_photo_edit_preview(photo_id):
        """Serve a preview for an in-progress or committed edit recipe.

        Crop editing intentionally uses the whole transformed source. Once a
        crop is committed, ``apply_crop=1`` returns the actual cropped pixels
        so the editor can fit the finished frame in the stage.
        """
        import io

        import config as cfg

        db = get_db()
        photo = db.photos.get(photo_id, verify_workspace=True)
        if not photo:
            return "Not found", 404
        try:
            size = int(request.args.get("size", "1920"))
        except ValueError:
            return "Invalid size", 400
        # 16384 (not 3840) so the editor's 100% zoom can request a
        # native-resolution render and be a true 1:1 view.
        size = max(256, min(16384, size))

        raw_recipe = request.args.get("recipe")
        if raw_recipe:
            try:
                recipe = json.loads(raw_recipe)
            except (TypeError, ValueError):
                return "Invalid recipe", 400
        else:
            recipe = db.get_photo_edit_recipe(photo_id) or {}
        if not isinstance(recipe, dict):
            return "Invalid recipe", 400

        try:
            from image_edits import RecipeError, normalize_recipe
            recipe = normalize_recipe(recipe) or {}
            apply_crop = request.args.get("apply_crop") == "1"
            display_recipe = dict(recipe)
            if not apply_crop:
                display_recipe.pop("crop", None)
            vireo_dir = os.path.dirname(config["THUMB_CACHE_DIR"])
            folder_row = db.folders.get(photo["folder_id"])
            if not folder_row:
                return "Not found", 404
            if not config.get('EDIT_PREVIEW_IN_PROCESS', False):
                import uuid

                from services.preview_workers import parse_request

                try:
                    session, sequence = parse_request(
                        request.args.get('preview_session', uuid.uuid4().hex),
                        request.args.get('preview_seq', '0'),
                    )
                except ValueError as error:
                    return str(error), 400
                payload = {
                    'db_path': db_path, 'config_path': str(cfg.CONFIG_PATH),
                    'photo_id': photo_id, 'photo': dict(photo), 'folder': dict(folder_row),
                    'vireo_dir': vireo_dir, 'recipe': recipe, 'display_recipe': display_recipe,
                    'size': size, 'apply_crop': apply_crop,
                    'quality': cfg.load().get('preview_quality', 90),
                    'clear_source_cache': config.get('EDIT_PREVIEW_CLEAR_SOURCE_CACHE', False),
                    'threads': config.get('EDIT_PREVIEW_THREADS', 2),
                }
                status, body, source_kind = render_edit_preview(
                    payload, session, sequence,
                    guard=lambda cancelled: _edit_preview_source_guard(photo, folder_row, cancelled),
                )
                response = Response(body, status=status, mimetype='image/jpeg' if status == 200 else 'text/plain')
                response.headers['Cache-Control'] = 'no-store'
                if source_kind:
                    response.headers['X-Vireo-Preview-Source'] = source_kind
                return response
            # Explicit in-process test seam for source/fallback monkeypatches.
            edit = _EditPreviewRequest(
                db,
                photo_id,
                photo,
                folder_row,
                vireo_dir,
                recipe,
                display_recipe,
                size,
                apply_crop,
            )
            img = edit.load()
            if img is None:
                return "Could not load image", 500
            img = edit.render(img)
        except RecipeError as e:
            return str(e), 400

        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=cfg.load().get("preview_quality", 90))
        img.close()
        return Response(buf.getvalue(), mimetype="image/jpeg")

    @blueprint.route("/api/photos/<int:photo_id>/auto-tone")
    def api_auto_tone(photo_id):
        """Fit Auto Tone to the photo as framed by ``recipe``, in ``style``.

        Returns the fitted exposure, highlights, shadows, contrast, whites,
        blacks, vibrance and saturation, whether metering weighted the
        subject (and from its mask or detection box), and plain-language
        notes on what changed. Reads the same highlight-preserving source as
        the edit preview; the recipe's tonal values do not affect the fit.
        """
        import auto_tone

        db = get_db()
        photo = db.photos.get(photo_id, verify_workspace=True)
        if not photo:
            return json_error("Photo not found", 404)
        try:
            recipe = json.loads(request.args.get("recipe") or "{}")
        except (TypeError, ValueError):
            return json_error("Invalid recipe")
        if not isinstance(recipe, dict):
            return json_error("Invalid recipe")
        style = request.args.get("style") or "balanced"
        if style not in auto_tone.STYLES:
            return json_error("Unknown Auto Tone style")

        from image_edits import RecipeError, normalize_recipe
        try:
            recipe = normalize_recipe(recipe) or {}
        except RecipeError as e:
            return json_error(str(e))
        # Source selection and decode size depend only on the frame.
        frame_recipe = {
            key: recipe[key]
            for key in ("version", "rotation", "flip", "straighten", "crop")
            if key in recipe
        }
        folder_row = db.folders.get(photo["folder_id"])
        if not folder_row:
            return json_error("Photo not found", 404)
        edit = _EditPreviewRequest(
            db,
            photo_id,
            photo,
            folder_row,
            os.path.dirname(config["THUMB_CACHE_DIR"]),
            frame_recipe,
            frame_recipe,
            auto_tone.SOURCE_LONG_EDGE,
            True,
            analysis=True,
        )
        img = edit.load()
        if img is None:
            return json_error("Could not load image", 500)
        box = None
        mask = _load_active_mask(db, photo_id)
        detections = [
            detection for detection in db.get_detections(photo_id)
            if detection["category"] == "animal" and detection["detector_model"] != "full-image"
        ]
        if detections:
            primary = detections[0]
            box = {
                "x": primary["box_x"], "y": primary["box_y"],
                "w": primary["box_w"], "h": primary["box_h"],
            }
        try:
            result = auto_tone.fit_loaded_image(
                img, recipe, native_size=edit.native_dims, mask=mask, box=box,
                style=style,
            )
        finally:
            img.close()
            if mask is not None:
                mask.close()
        return jsonify(result)

    @blueprint.route("/photos/<int:photo_id>/original")
    def serve_original_photo(photo_id, *, _artifact_flight_guarded=False, _prepare_source=None):
        """Serve full-resolution image for 1:1 zoom."""
        from flask import send_file

        db = get_db()

        # verify_workspace: mirrors serve_thumbnail — full-res bytes must not
        # leak across workspaces.
        photo = db.photos.get(photo_id, verify_workspace=True)
        if not photo:
            return "Not found", 404

        vireo_dir = os.path.dirname(config["THUMB_CACHE_DIR"])
        recipe = db.get_photo_edit_recipe(photo_id)
        from image_loader import RAW_EXTENSIONS

        primary_is_raw = (
            os.path.splitext(photo["filename"])[1].lower() in RAW_EXTENSIONS
        )

        folder = db.folders.get(photo["folder_id"])
        if not folder:
            return "Not found", 404

        original = _OriginalPhotoRequest(
            db,
            photo_id,
            photo,
            folder["path"],
            vireo_dir,
            recipe,
            primary_is_raw,
            artifact_flight_guarded=_artifact_flight_guarded,
            prepare_source=_prepare_source,
            view=serve_original_photo,
        )

        pair_source, pair_source_path = _requested_pair_source(
            get_db, config, photo, folder["path"],
        )
        if pair_source and not pair_source_path:
            return "Not found", 404
        if pair_source_path:
            # Explicit pair views bypass the canonical prepared-render cache.
            # The companion is already the photographer's developed result.
            # Its geometry can differ from the RAW, so never apply the RAW's
            # recipe or local mask in that coordinate space.
            if pair_source == "jpeg":
                return send_file(pair_source_path)
            return original.serve_paired_raw(pair_source, pair_source_path)

        original.collect_file_state()
        with reader_guard("full-resolution"):
            prepared_render = _prepared_full_resolution_render(
                vireo_dir, photo, recipe, original.file_state,
            )
            if prepared_render:
                return send_file(prepared_render, mimetype="image/jpeg")

        # Paired-source requests returned above using their source/edit-signed
        # transient artifact.
        if not _artifact_flight_guarded:
            return original.coordinate_canonical_flight()

        original.trusted_wc_path = original.trusted_full_res_working_copy_path()

        if recipe:
            return original.serve_edited()

        # A RAW working copy is an edit-quality, highlight-preserving source.
        # It deliberately looks flatter/darker than the camera-rendered JPEG
        # used by thumbnails and previews, so it must not be used as the
        # unedited lightbox rendition while the source is available.
        if original.trusted_wc_path and not primary_is_raw:
            # The dimension check above intentionally happens without holding
            # the publication lock, but quota eviction can unlink the file in
            # the gap before ``send_file`` opens it. Revalidate and open under
            # the shared guard. ``send_file`` opens eagerly; once it returns,
            # POSIX keeps the fd readable after unlink and Windows prevents
            # eviction from unlinking the open handle.
            response = original._serve_trusted_working_copy_if_present()
            if response is not None:
                return response

        return original.serve_unedited()
    return blueprint
