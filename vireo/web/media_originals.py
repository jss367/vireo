"""Full-resolution originals, RAW/JPEG selection, prepared renders and extraction.

OriginalRoutes serves /original; source/decode steps remain in services.original_photo."""

from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import os
import tempfile
import time
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, Protocol, cast

from app_paths import media_root
from artifact_flight import (
    ArtifactProducerFailed,
    atomic_write_bytes,
    original_artifact_flights,
    preview_prefetch_slots,
)
from config import config_value
from file_replace import replace_file
from flask import (
    Response,
    make_response,
    request,
)
from render_fingerprint import render_cache_fields as _camera_render_cache_fields
from render_source import (
    has_current_working_copy_failure as _has_current_working_copy_failure,
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
from web.media_common import (
    _PAIRED_PREVIEW_DIRNAME,
    _PAIRED_PREVIEW_TTL_SEC,
    _ArtifactResponseError,
    _paired_render_state_hash,
    _preparation_publication,
    _requested_pair_source,
    _send_opened_file,
    _shed_prefetch_response,
)
from working_copy_cache import (
    evict_if_over_quota as evict_working_copy_cache_if_over_quota,
)
from working_copy_cache import (
    touch_working_copy_access,
    working_copy_publication_guard,
    working_copy_quota_bytes,
)

if TYPE_CHECKING:
    from contextlib import AbstractContextManager
    from sqlite3 import Row

    from db import Database
    from flask.typing import ResponseReturnValue
    from float_image import FloatImage
    from PIL import Image
    from services.service_types import JsonDict
    from web.web_types import AppConfig, GetDb


log = logging.getLogger(__name__)


class _OriginalView(Protocol):
    """The registered ``serve_original_photo`` view, re-entered by flights."""

    def __call__(
        self, photo_id: int, *, _artifact_flight_guarded: bool = False, _prepare_source: Row | None = None,
    ) -> ResponseReturnValue: ...


def _paired_original_dir(vireo_dir: str) -> str:
    return os.path.join(vireo_dir, "originals", _PAIRED_PREVIEW_DIRNAME)


def _paired_original_path(vireo_dir: str, photo_id: int, pair_source: str, state_hash: str) -> str:
    return os.path.join(
        _paired_original_dir(vireo_dir),
        f"{photo_id}_{pair_source}_{state_hash}.jpg",
    )


def _fresh_paired_artifact(path: str) -> bool:
    """Return whether a non-empty paired artifact is still inside its TTL."""
    try:
        return (
            os.path.getsize(path) > 0
            and os.path.getmtime(path) >= time.time() - _PAIRED_PREVIEW_TTL_SEC
        )
    except OSError:
        return False


def _sweep_stale_paired_previews(paired_dir: str) -> None:
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


def _full_resolution_render_signature(photo: Row, recipe: JsonDict | None, file_state: JsonDict | None = None) -> JsonDict:
    """Describe every catalogued input to a cached inspection render.

    The source size/mtime pair is refreshed by scans, the canonical recipe
    JSON includes content-addressed local-mask references, and the edit-math
    version invalidates bytes produced by older rendering code. Hashing the
    signature into the JPEG filename lets /original safely reuse prepared
    renders without a database table or cross-recipe cache race.
    """
    from edit_recipe import recipe_to_json
    from image_edits import EDIT_MATH_VERSION

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
    vireo_dir: str, photo: Row, recipe: JsonDict | None, file_state: JsonDict | None = None,
) -> str:
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


def _peg_render_mtime_to_source(cache_path: str, photo: Row) -> None:
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


def _serve_trusted_working_copy(path: str) -> Response:
    """Serve a working-copy cache hit and stamp it as recently used.

    Callers hold ``working_copy_publication_guard`` — the same lock the
    quota pass takes — so the stamp cannot land between eviction's
    directory scan and its unlink.
    """
    from flask import send_file

    touch_working_copy_access(path)
    return send_file(path, mimetype="image/jpeg")


def _prepared_full_resolution_render(
    vireo_dir: str, photo: Row, recipe: JsonDict | None, file_state: JsonDict | None = None,
) -> str | None:
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


def _run_original_artifact_flight(
    artifact_key: str, producer: Callable[[], Response], consumer: Callable[[], Response],
) -> Response:
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
        # A flight that ran (or joined one) carries its response.
        return cast(Response, result.value)
    except _ArtifactResponseError as exc:
        return exc.to_response()
    except ArtifactProducerFailed as exc:
        if isinstance(exc.__cause__, _ArtifactResponseError):
            return exc.__cause__.to_response()
        raise
    finally:
        if speculative_slot:
            preview_prefetch_slots.release()


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
        db: Database,
        photo_id: int,
        photo: Row,
        folder_path: str,
        vireo_dir: str,
        recipe: JsonDict | None,
        primary_is_raw: bool,
        *,
        artifact_flight_guarded: bool,
        prepare_source: Row | None,
        view: _OriginalView,
    ) -> None:
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
        self.file_state: JsonDict | None = None
        # Set by serve_original_photo before the unedited and edited paths.
        self.trusted_wc_path: str | None = None
        # Late-bound: _resolve_unedited_source and serve_unedited set these
        # before any reader runs.
        self.source = cast("UneditedSource", None)
        self.extraction = cast("OriginalExtraction", None)

    def _preparation_publication(self) -> AbstractContextManager[None]:
        return _preparation_publication(
            self.db, self.photo_id, self.prepare_source,
            captured_recipe=self.recipe,
        )

    def _reenter(self, *, guarded: bool) -> Response:
        response = make_response(
            self._view(
                self.photo_id, _artifact_flight_guarded=guarded, _prepare_source=self.prepare_source,
            )
        )
        if response.status_code >= 400:
            raise _ArtifactResponseError(response)
        return response

    def _serve_trusted_working_copy_if_present(self) -> Response | None:
        # Callers check trusted_wc_path first.
        trusted_wc_path = cast(str, self.trusted_wc_path)
        with working_copy_publication_guard():
            if os.path.isfile(trusted_wc_path):
                return _serve_trusted_working_copy(trusted_wc_path)
        return None

    def serve_paired_raw(self, pair_source: str, pair_source_path: str) -> Response:
        """Serve an explicit ``?source=raw`` view of a paired photo."""
        return _PairedRawOriginal(self, pair_source, pair_source_path).serve()

    def collect_file_state(self) -> None:
        self.file_state = self.original.collect_file_state()

    def coordinate_canonical_flight(self) -> Response:
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

    def trusted_full_res_working_copy_path(self) -> str | None:
        return self.original.trusted_full_res_working_copy_path()

    # -- Edited renders --------------------------------------------------

    def serve_edited(self) -> tuple[str, int] | Response:
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

    def _publish_edited_render(self, img: Image.Image | FloatImage) -> Response:
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
        quality = config_value(cfg.load(), "working_copy_quality")
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

    def serve_unedited(self) -> Response | tuple[str, int]:
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

    def _resolve_unedited_source(self) -> Response | tuple[str, int] | None:
        from flask import send_file

        self.source = source = UneditedSource.resolve(self.original)

        if self.primary_is_raw:
            with reader_guard("full-resolution"):
                if source.display_cache_is_current():
                    # A current display cache has a path.
                    return send_file(cast(str, source.display_cache_path), mimetype="image/jpeg")

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

    def _route_around_raw_failure(self) -> Response | tuple[str, int] | None:
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

    def _serve_decoded_original(self) -> Response | tuple[str, int]:
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

    def _cache_decoded_original(self, img: Image.Image | FloatImage) -> Response:
        from flask import send_file

        photo_id = self.photo_id
        if self.primary_is_raw:
            # UneditedSource.resolve sets it for a RAW primary.
            cache_path = cast(str, self.source.display_cache_path)
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

    def __init__(self, original_request: _OriginalPhotoRequest, pair_source: str, pair_source_path: str) -> None:
        self.photo = original_request.photo
        self.photo_id = original_request.photo_id
        self.vireo_dir = original_request.vireo_dir
        self.recipe = original_request.recipe
        self.artifact_flight_guarded = original_request.artifact_flight_guarded
        self._reenter = original_request._reenter
        self.pair_source = pair_source
        self.pair_source_path = pair_source_path

    def serve(self) -> Response:
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

        def _render_paired_original() -> Response:
            return self._render_paired_original(paired_original_cache)

        if self.artifact_flight_guarded:
            return _render_paired_original()

        def _paired_consumer() -> Response:
            # Producer already published the shadow-cache file, so a
            # re-entering waiter hits the cache-hit check above and
            # serves it via send_file — no second decode.
            return self._reenter(guarded=True)

        return _run_original_artifact_flight(
            os.path.abspath(paired_original_cache),
            _render_paired_original,
            _paired_consumer,
        )

    def _render_paired_original(self, paired_original_cache: str) -> Response:
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
            quality=config_value(cfg.load(), "working_copy_quality"),
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

    def __init__(self, original_request: _OriginalPhotoRequest, extraction: OriginalExtraction) -> None:
        self.db = original_request.db
        self.photo = original_request.photo
        self.photo_id = original_request.photo_id
        self.vireo_dir = original_request.vireo_dir
        self.primary_is_raw = original_request.primary_is_raw
        self.trusted_wc_path = original_request.trusted_wc_path
        self._preparation_publication = original_request._preparation_publication
        self.source = original_request.source
        self.extraction = extraction

    def serve(self, tmp_path: str) -> Response | tuple[str, int]:
        # Update DB so future requests are fast; also backfill
        # dimensions if missing so the full-res shortcut works next time
        from PIL import Image as _PILImage
        # The still-private rendition, or None once it is published.
        private_tmp: str | None = tmp_path
        if self.primary_is_raw:
            # RAW display cache goes through ``wc_abs`` immediately so
            # subsequent checks (undersized retry, trusted-wc override)
            # can peek at the just-written file directly.
            self.extraction.publish(tmp_path)
            private_tmp = None
            peek_path = self.extraction.wc_abs
        else:
            # Non-RAW: keep the rendition private until cacheability
            # is decided in ``_serve_generated_original``. Peek at the
            # tempfile itself for its dimensions.
            peek_path = tmp_path
        with _PILImage.open(peek_path) as upgraded:
            uw, uh = upgraded.size
        private_tmp, uw, uh = self.extraction.replace_undersized_extraction(private_tmp, uw, uh)
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
        return self._serve_generated_original(private_tmp, uw, uh)

    def serve_companion_extraction(self) -> Response | tuple[str, int] | None:
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

    def _serve_generated_original(self, tmp_path: str | None, uw: int, uh: int) -> Response | tuple[str, int]:
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

    def _serve_generated_original_current(self, tmp_path: str | None, uw: int, uh: int) -> Response | tuple[str, int]:
        """Publish to the cache, or stream the private tmp transiently.

        ``tmp_path`` is the private rendition from ``extract_to_private_tmp``.
        The RAW branch has always already published (its callers publish
        immediately so their ``PIL.Image.open`` size peek reads a known path);
        it passes ``tmp_path=None`` and this call just serves ``wc_abs``.
        Otherwise, the cacheable branch publishes and serves from ``wc_abs``
        (pinning against concurrent eviction with an open fd); the
        non-cacheable branch keeps the file at a per-request transient path
        and streams from there — ``wc_abs`` is never touched, so concurrent
        waiters cannot collide there.
        """
        from flask import send_file

        if self.primary_is_raw:
            return send_file(self.extraction.wc_abs, mimetype="image/jpeg")
        tmp_path = cast(str, tmp_path)  # Only the RAW branch above is passed None.

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

    def _stream_transient_rendition(self, tmp_path: str) -> tuple[str, int] | Response:
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

        def _stream_rendition() -> Iterator[bytes]:
            try:
                while chunk := rendition_fh.read(1024 * 1024):
                    yield chunk
            finally:
                with contextlib.suppress(OSError):  # close of a read-only handle
                    rendition_fh.close()
                with contextlib.suppress(OSError):
                    os.unlink(transient_path)

        return Response(_stream_rendition(), mimetype="image/jpeg")


class OriginalRoutes:
    """Per-app dependencies for this group of media views."""

    def __init__(self, get_db: GetDb, config: AppConfig) -> None:
        self._get_db = get_db
        self._config = config

    def serve_original_photo(self, photo_id: int, *, _artifact_flight_guarded: bool = False, _prepare_source: Row | None = None) -> ResponseReturnValue:
        """Serve full-resolution image for 1:1 zoom."""
        from flask import send_file

        db = self._get_db()

        # verify_workspace: mirrors serve_thumbnail — full-res bytes must not
        # leak across workspaces.
        photo = db.photos.get(photo_id, verify_workspace=True)
        if not photo:
            return "Not found", 404

        vireo_dir = media_root(self._config["THUMB_CACHE_DIR"])
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
            view=self.serve_original_photo,
        )

        pair_source, pair_source_path = _requested_pair_source(
            self._get_db, self._config, photo, folder["path"],
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
            # A resolved pair path always names its source.
            return original.serve_paired_raw(cast(str, pair_source), pair_source_path)

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
