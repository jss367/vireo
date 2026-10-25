"""Display-sized previews and /full, paired caches and preview artifact coordination."""

from __future__ import annotations

import contextlib
import logging
import os
import sqlite3
from typing import TYPE_CHECKING, cast

from app_paths import media_root
from artifact_flight import (
    ArtifactProducerFailed,
    atomic_write_bytes,
    preview_artifact_flights,
    preview_prefetch_slots,
)
from camera_denoise import cache_matches as _camera_cache_matches
from flask import (
    Response,
    make_response,
    request,
)
from preview_cache import PREVIEW_TIER_SIZES, paired_preview_ready
from preview_cache import (
    evict_if_over_quota as evict_preview_cache_if_over_quota,
)
from preview_materializer import (
    PreviewMaterializationError,
    materialize_preview,
)
from web.media_common import (
    _PAIRED_PREVIEW_DIRNAME,
    _ArtifactResponseError,
    _focus_source_image,
    _paired_render_state_hash,
    _preparation_publication,
    _requested_pair_source,
    _shed_prefetch_response,
)

if TYPE_CHECKING:
    from sqlite3 import Row

    from db import Database
    from flask.typing import ResponseReturnValue
    from preview_materializer import PreviewMaterialization
    from services.service_types import JsonDict
    from web.web_types import AppConfig, ClearPreviewCacheInvalid, GetDb


log = logging.getLogger(__name__)


def _paired_preview_dir(preview_dir: str) -> str:
    return os.path.join(preview_dir, _PAIRED_PREVIEW_DIRNAME)


def _paired_preview_path(
    preview_dir: str, photo_id: int, size: int, pair_source: str, state_hash: str,
) -> str:
    return os.path.join(
        _paired_preview_dir(preview_dir),
        f"{photo_id}_{size}_{pair_source}_{state_hash}.jpg",
    )


def _is_preview_cache_invalid(db: Database, photo_id: int, size: int) -> bool:
    return db.caches.preview_invalidated(photo_id, size)


def _paired_cache_path_for_request(
    preview_dir: str, photo: Row, photo_id: int, size: int, pair_source: str | None, pair_source_path: str | None,
    render_recipe: JsonDict | None, bypass_cache: bool,
) -> str | None:
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
    if not bypass_cache:
        return None
    # Only an explicit pair view bypasses the cache, and it names both.
    pair_source, pair_source_path = cast(str, pair_source), cast(str, pair_source_path)
    return _paired_preview_path(
        preview_dir,
        photo_id,
        size,
        pair_source,
        _paired_render_state_hash(
            photo, size, pair_source, pair_source_path, render_recipe,
        ),
    )


def _serve_cached_preview(
    db: Database, photo_id: int, size: int, cache_path: str, paired_cache_path: str | None, vireo_dir: str, *,
    bypass_cache: bool, stale_after_failed_invalidation: bool,
    skip_untracked_preview_adoption: bool,
) -> Response | None:
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
    db: Database, photo: Row, photo_id: int, size: int, folder_row: Row, vireo_dir: str, cache_path: str,
    render_recipe: JsonDict | None, bypass_cache: bool, pair_source: str | None, pair_source_path: str | None,
    _prepare_source: Row | None,
) -> PreviewMaterialization | None:
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
        self, get_db: GetDb, config: AppConfig, *, invalid_preview_cache_paths: set[str],
        clear_preview_cache_invalid: ClearPreviewCacheInvalid,
    ) -> None:
        self._get_db = get_db
        self._config = config
        self._invalid_preview_cache_paths = invalid_preview_cache_paths
        self._clear_preview_cache_invalid = clear_preview_cache_invalid


    def allowed_preview_sizes(self) -> set[int]:
        """Allowlist for /photos/<id>/preview?size=N.

        Includes the fixed tier plus the user-configured preview_max_size
        so /full can delegate here. Reads workspace-effective config so
        a per-workspace preview_max_size override is honored.
        """
        import config as cfg
        effective = self._get_db().get_effective_config(cfg.load())
        fixed: set[int] = set(PREVIEW_TIER_SIZES)
        pm = effective.get("preview_max_size") or 1920
        if pm == 0:
            return fixed  # 0 = "full" — handled by /original path
        return fixed | {int(pm)}

    def _serve_source_frame(self, photo_id: int, size: int) -> ResponseReturnValue:
        """Serve the cached upright, unedited frame used for manual placement."""
        import config as cfg
        from services.source_frame_preview import SourceFrameUnavailable, source_frame_bytes

        db = self._get_db()
        effective = db.get_effective_config(cfg.load())
        try:
            data = source_frame_bytes(
                db, photo_id, size, media_root(self._config["THUMB_CACHE_DIR"]),
                effective.get("preview_quality", 90),
                lambda photo, folder: _focus_source_image(self._config, photo, folder, "auto"),
            )
        except SourceFrameUnavailable:
            return "Full frame unavailable; refresh and try again", 404
        except ArtifactProducerFailed as error:
            if not isinstance(error.__cause__, SourceFrameUnavailable):
                raise
            return "Full frame unavailable; refresh and try again", 404
        response = Response(data, mimetype="image/jpeg")
        response.cache_control.private = True
        response.cache_control.max_age = 0
        response.add_etag()
        return response.make_conditional(request)

    def _reconcile_preview_cache_file(
        self, db: Database, photo: Row, photo_id: int, size: int, cache_path: str, recipe: JsonDict | None, render_recipe: JsonDict | None, bypass_cache: bool,
    ) -> tuple[bool, bool]:
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
        self, photo_id: int, size: int, cache_path: str, paired_cache_path: str | None, bypass_cache: bool,
        _prepare_source: Row | None,
    ) -> Response | tuple[str, int]:
        """Run the cache-miss render under the equal-key artifact flight."""
        # A cache-bypassing pair view always has its shadow-cache path.
        artifact_key = os.path.abspath(
            cast(str, paired_cache_path) if bypass_cache else cache_path,
        )
        speculative = request.args.get("prefetch") == "1"
        speculative_slot = False
        if speculative:
            speculative_slot = preview_prefetch_slots.acquire(blocking=False)
            if not speculative_slot:
                return _shed_prefetch_response()

        def coordinated_request(*, guarded: bool) -> Response:
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
            # A flight that ran (or joined one) carries its response.
            return cast(Response, result.value)
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
        self, db: Database, photo_id: int, photo: Row, size: int, cache_path: str, paired_cache_path: str | None, vireo_dir: str,
        bypass_cache: bool, rendered: PreviewMaterialization,
    ) -> None:
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
        self, photo_id: int, size: int, *, _artifact_flight_guarded: bool = False, _prepare_source: Row | None = None,
    ) -> tuple[str, int] | Response:
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

        vireo_dir = media_root(self._config["THUMB_CACHE_DIR"])
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

    def serve_full_photo(self, photo_id: int) -> ResponseReturnValue:
        """Serve a display-sized preview (alias for /preview at preview_max_size).

        Reads workspace-effective config so a per-workspace preview_max_size
        override is honored. preview_max_size == 0 historically meant "full"
        — we route to /original rather than generate a preview. Using a
        separate read/fallback (instead of `cfg.get(...) or 1920`) keeps
        the 0 sentinel reachable.
        """
        import config as cfg
        from flask import redirect

        effective = self._get_db().get_effective_config(cfg.load())
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
        return self._serve_preview(photo_id, int(pm or 1920))

    def serve_photo_preview(self, photo_id: int, *, _prepare_source: Row | None = None) -> ResponseReturnValue:
        """Serve a JPEG preview at a chosen max-size.

        Cache is LRU-tracked in the preview_cache table; on-disk files that
        predate this scheme are adopted lazily on first access.

        Query params:
          size: int — max dimension (longest side). Must be in
                allowed_preview_sizes() to avoid unbounded cache growth.
          view: source — cached upright, unedited full frame for inspection.
        """
        from flask import request

        try:
            size = int(request.args.get("size", "1920"))
        except ValueError:
            return "Invalid size", 400
        if size not in self.allowed_preview_sizes():
            return "Unsupported size", 400
        if request.args.get("view") == "source":
            return self._serve_source_frame(photo_id, size)
        return self._serve_preview(photo_id, size, _prepare_source=_prepare_source)
