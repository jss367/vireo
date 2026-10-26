"""Workspace-scoped and duplicate-review thumbnails, cache validation and self-healing."""

from __future__ import annotations

import contextlib
import logging
import os
import sqlite3
from collections.abc import Callable
from typing import TYPE_CHECKING, cast

from app_paths import media_root
from camera_denoise import cache_matches as _camera_cache_matches
from config import config_value
from flask import (
    Response,
    make_response,
    send_from_directory,
)
from render_source import (
    has_current_working_copy_failure as _has_current_working_copy_failure,
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
from services.same_shot_duplicates import is_duplicate_review_member
from web.media_common import _requested_pair_source

if TYPE_CHECKING:
    from sqlite3 import Row

    from db import Database
    from flask.typing import ResponseReturnValue
    from services.scan_work import ConfigLoader
    from services.service_types import JsonDict
    from web.web_types import AppConfig, GetDb, GetRunner


log = logging.getLogger(__name__)


def _send_cached_thumbnail(directory: str, fname: str) -> Response:
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
        db: Database,
        photo_id: int,
        photo: Row,
        folder_row: Row,
        filename: str,
        thumb_dir: str,
        pair_source: str | None,
        pair_source_path: str | None,
    ) -> None:
        self.db = db
        self.photo_id = photo_id
        self.photo = photo
        self.folder_row = folder_row
        self.filename = filename
        self.thumb_dir = thumb_dir
        self.pair_source = pair_source
        self.pair_source_path = pair_source_path
        self.cache_recipe: JsonDict | None = None
        # Late-bound: select_cache_target sets the cache target, and
        # prepare_self_heal / resolve_render_source the self-heal inputs,
        # before anything reads them.
        self.cache_filename = cast(str, None)
        self.thumb_path = cast(str, None)
        # Set when a locked stale thumbnail forces regeneration to the
        # ``<id>_regen.jpg`` sidecar; the real ``<id>.jpg`` stays stale.
        self.served_from_regen_sidecar = False
        self.selected_source_mtime: float | None = None
        self.live_source = cast(str, None)
        self.vireo_dir = cast(str, None)
        self.folders = cast("dict[int, str]", None)
        self.render_recipe: JsonDict | None = None
        self.thumb_size = cast(int, None)
        self.source = cast(str, None)
        self.using_working_copy = False
        # The lazily imported config module and thumbnail generators.
        self._cfg = cast("ConfigLoader", None)
        self._generate_thumbnail = cast("Callable[..., str | None]", None)
        self._retry_after_working_copy_eviction = cast(
            "Callable[..., tuple[str | None, str]]", None,
        )
        self._retry_with_working_copy = cast("Callable[..., str | None]", None)

    def select_cache_target(self) -> None:
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

    def _is_fresh(self, cached_mtime: float) -> bool:
        # Treat as fresh if ``file_mtime`` is unknown (legacy rows
        # pre-mtime tracking — we have no signal to invalidate
        # against, so prefer the fast path over false-positive
        # regeneration).
        return (
            self.selected_source_mtime is None
            or cached_mtime >= self.selected_source_mtime
        ) and _camera_cache_matches(self.thumb_path, self.photo, self.cache_recipe)

    def serve_fresh_cache(self) -> Response | tuple[str, int] | None:
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

    def _serve_regen_sidecar(self) -> Response | tuple[str, int] | None:
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

    def prepare_self_heal(self) -> None:
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
        self.vireo_dir = media_root(self.thumb_dir)
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

    def resolve_render_source(self) -> None:
        recipe = self.db.get_photo_edit_recipe(self.photo_id)
        # Edit recipes and local masks are stored in the primary RAW's
        # coordinate space. A developed companion may already be cropped,
        # rotated, or resized, so applying that geometry again would render
        # the selected JPEG incorrectly. JPEG pair views intentionally show
        # the companion as-authored; RAW pair views retain the catalog edit.
        self.render_recipe = None if self.pair_source == "jpeg" else recipe
        self.thumb_size = config_value(self._cfg.load(), "thumbnail_size")
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

    def would_retry_failed_raw_decode(self) -> bool:
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

    def generate(self) -> str | None:
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
                config_value(self._cfg.load(), "thumbnail_quality"),
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
                config_value(self._cfg.load(), "thumbnail_quality"),
                render_recipe,
                self.vireo_dir,
                cache_name=self.cache_filename,
            )
            if result:
                self.source = result
        return result

    def _generate_from_companion(self, result: str | None) -> str | None:
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

    def peg_mtime_to_source(self, result: str) -> None:
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

    def persist_thumb_path(self) -> None:
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


def _serve_photo_thumbnail(get_db: GetDb, config: AppConfig, db: Database, photo_id: int, photo: Row, filename: str) -> tuple[str, int] | Response:
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


class ThumbnailRoutes:
    """Per-app dependencies for this group of media views."""

    def __init__(self, get_db: GetDb, config: AppConfig, get_runner: GetRunner) -> None:
        self._get_db = get_db
        self._config = config
        self._get_runner = get_runner

    def serve_thumbnail(self, filename: str) -> ResponseReturnValue:
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

        db = self._get_db()
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
        return _serve_photo_thumbnail(self._get_db, self._config, db, photo_id, photo, filename)

    def serve_duplicate_thumbnail(self, filename: str) -> ResponseReturnValue:
        """``serve_thumbnail`` for a member of a duplicate group, in any workspace.

        The duplicate scan is library-wide, so the Duplicates page shows
        copies the active workspace does not, and ``/thumbnails/<filename>``
        404s on those. This route replaces the workspace check with
        exact-duplicate membership or a still-current same-shot group from
        the latest completed scan. Other photos still 404. The folder comes from ``folders.get`` by id,
        which is not workspace-scoped, so the source path never falls back
        to a CWD-relative join.
        """
        try:
            photo_id = int(filename.replace(".jpg", ""))
        except ValueError:
            log.warning("Thumbnail request with non-numeric filename: %s", filename)
            return "", 404

        db = self._get_db()
        if not is_duplicate_review_member(db, photo_id, self._get_runner().list_jobs):
            return "", 404
        photo = db.photos.get(photo_id)
        if not photo:
            return "", 404
        return _serve_photo_thumbnail(self._get_db, self._config, db, photo_id, photo, filename)
