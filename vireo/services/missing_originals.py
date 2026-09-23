"""Missing Originals scans, their per-app result cache, and folder health.

``/api/photos/missing`` reports photos whose original file is gone from disk.
Finding them walks every linked folder, which is slow on NAS/SMB libraries, so
the walk runs as a ``missing_originals_scan`` job and its result is cached per
``(db_path, workspace_id, folder_id)``. Every route that removes or moves
photo rows invalidates that cache, and so does the background folder-health
loop when a folder flips between ok and missing.

Nothing here touches ``request``; the request parser lives in
``web.request_args.request_missing_originals_folder_id``. ``MissingOriginals``
is the one stateful piece: it owns the cache, the in-flight scan table, the
error/backoff records and the invalidation generations, so ``create_app``
builds a single instance and injects its bound methods.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from datetime import UTC, datetime

from db import Database, MissingPhotosCancelled
from photo_payload import attach_nested_edit_recipes

log = logging.getLogger(__name__)

STALE_SECONDS = 30 * 60
BACKOFF_SECONDS = 30 * 60
HEAVY_JOB_TYPES = {
    "scan",
    "pipeline",
    "thumbnails",
    "previews",
    "move-photos",
    "move-folder",
    "sync",
    "classify",
    "precompute-embeddings",
    "cull",
    "develop",
    "extract-masks",
    "regroup",
    "import",
    "import-full",
    "import-in-place",
    "ingest",
    "import-photos",
    "batch-delete",
    "duplicate-scan",
    "offline-cache",
    # Navbar's new-images probe walks the same folders a missing-originals
    # scan would; letting them run concurrently can double the filesystem
    # load on slow NAS/SMB libraries.
    "new_images_walk",
    # Folder-scoped and workspace-wide missing-originals scans have
    # distinct cache keys, so the same-key in-flight coalescing does
    # not catch a workspace scan started while a folder scan is
    # running (or vice versa). Treat any in-flight
    # missing_originals_scan as heavy work so automatic reruns
    # don't kick off a second filesystem walk over the same tree.
    "missing_originals_scan",
    # audit.verify_hashes walks every workspace source file and
    # hashes readable ones — the same NAS/SMB trees a Missing
    # Originals scan touches. Letting the 30-minute automatic
    # missing-originals timer fire during verification would
    # double the I/O on those slow volumes.
    "verify-hashes",
    # Card cleanup reads only archive copies that match one card, but
    # those reads still hit the same NAS/SMB trees.
    "card-cleanup-verify",
}


def _utc_iso_now():
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def resolve_folder_id(db, folder_id):
    """Validate a raw ``folder_id`` scope against the active workspace.

    Returns ``None`` for a missing/empty value (workspace-wide scope) or the
    integer folder id. Raises ``ValueError`` for a non-integer and
    ``LookupError`` when the folder is not linked into the active workspace.
    """
    if folder_id in (None, ""):
        return None
    try:
        folder_id = int(folder_id)
    except (TypeError, ValueError):
        raise ValueError("folder_id must be an integer") from None
    linked = db.conn.execute(
        "SELECT 1 FROM workspace_folders WHERE workspace_id = ? AND folder_id = ?",
        (db._active_workspace_id, folder_id),
    ).fetchone()
    if not linked:
        raise LookupError("folder not found")
    return folder_id


def cache_key(db, folder_id):
    return (db._db_path, db._active_workspace_id, folder_id)


class MissingOriginals:
    """Per-app Missing Originals scan state and the folder-health loop.

    ``config`` is the app's config mapping (``THUMB_CACHE_DIR`` is read at
    scan time) and ``get_runner`` returns the app's ``JobRunner``.
    """

    def __init__(self, *, db_path, config, get_runner):
        self.db_path = db_path
        self._config = config
        self._get_runner = get_runner
        self.lock = threading.Lock()
        self.cache = {}
        self.inflight = {}
        self.errors = {}
        # Monotonic per-key counter bumped whenever the cache is invalidated
        # while a scan is in flight. Each scan snapshots this at start; if
        # the counter has advanced by the time it finishes, the scan's
        # results are from a pre-invalidation view of the library and must
        # be discarded so deleted photos don't reappear in the banner/modal.
        self.generation = {}

    def payload(self, db, folder_id):
        key = cache_key(db, folder_id)
        now = time.monotonic()
        with self.lock:
            entry = self.cache.get(key)
            inflight = self.inflight.get(key)
            err = self.errors.get(key)
            # An error recorded after the last cached scan means a later
            # refresh failed. Returning the pre-refresh photo list as a
            # fresh "ready" result would hide the failure — and worse,
            # let the user delete rows whose originals may have been
            # restored between scans. When a scan is in flight the UI
            # already shows "pending", so still surface the stale
            # entry then; otherwise prefer the error state.
            cache_superseded_by_error = (
                entry is not None
                and err is not None
                and not inflight
                and err["set_at"] > entry["set_at"]
            )
            if entry is not None and not cache_superseded_by_error:
                status = "pending" if inflight else "ready"
                photos = entry["photos"]
                checked_at = entry["checked_at"]
                stale = now - entry["set_at"] > STALE_SECONDS
                error = None
            elif inflight:
                status = "pending"
                photos = []
                checked_at = None
                stale = False
                error = None
            elif err is not None:
                status = "error"
                photos = []
                checked_at = err["checked_at"]
                stale = False
                error = err["error"]
            else:
                status = "not_ready"
                photos = []
                checked_at = None
                stale = False
                error = None
            backoff_seconds = 0
            if err is not None:
                backoff_seconds = max(0, int(err["backoff_until"] - now))
            return {
                "status": status,
                "pending": bool(inflight),
                "checked_at": checked_at,
                "stale": stale,
                "error": error,
                "job_id": inflight if isinstance(inflight, str) else None,
                "photos": photos,
                "backoff_seconds": backoff_seconds,
                "workspace_id": db._active_workspace_id,
                "folder_id": folder_id,
            }

    def build_rows(
        self,
        db,
        folder_id=None,
        progress_callback=None,
        cancel_callback=None,
    ):
        thumb_dir = self._config["THUMB_CACHE_DIR"]
        vireo_dir = os.path.dirname(thumb_dir)
        preview_dir = os.path.join(vireo_dir, "previews")
        working_dir = os.path.join(vireo_dir, "working")

        def check_cancelled():
            if cancel_callback is not None and cancel_callback():
                raise MissingPhotosCancelled("missing originals scan cancelled")

        # Index preview cache once. The endpoint is polled from the navbar,
        # so per-photo `glob(preview_dir, f"{pid}_*.jpg")` was O(missing ×
        # cache_size) readdirs per request. Build {pid} from a single
        # listdir and check the set in O(1) per row.
        preview_pids: set[int] = set()
        try:
            check_cancelled()
            with os.scandir(preview_dir) as it:
                for entry in it:
                    check_cancelled()
                    name = entry.name
                    # Match `{id}.jpg` (legacy full preview) or `{id}_{size}.jpg`
                    # (sized variant). Anything else is not part of the per-photo
                    # cache and should be ignored.
                    if not name.endswith(".jpg"):
                        continue
                    head = name[:-4].split("_", 1)[0]
                    if head.isdigit():
                        preview_pids.add(int(head))
        except FileNotFoundError:
            pass  # cache dir hasn't been created yet — no previews

        out = []
        for row in db.get_missing_photos(
            folder_id=folder_id,
            progress_callback=progress_callback,
            cancel_callback=cancel_callback,
        ):
            check_cancelled()
            pid = row["id"]
            src = os.path.join(row["folder_path"], row["filename"])
            stem, _ext = os.path.splitext(src)
            # Working copy: the DB path wins when set, but legacy rows from
            # before working_copy_path was tracked can still have a file at
            # the default <vireo>/working/<id>.jpg location — and the batch
            # delete path cleans that up regardless of the DB column. If we
            # only consulted the column the badge would lie about what's
            # about to be removed.
            wc_rel = row["working_copy_path"]
            default_wc = os.path.join(working_dir, f"{pid}.jpg")
            if wc_rel:
                has_wc = os.path.isfile(os.path.join(vireo_dir, wc_rel))
            else:
                has_wc = os.path.isfile(default_wc)
            out.append({
                "id": pid,
                "filename": row["filename"],
                "extension": row["extension"],
                "folder_id": row["folder_id"],
                "folder_path": row["folder_path"],
                "timestamp": row["timestamp"],
                "file_size": row["file_size"],
                "has_thumb": os.path.isfile(os.path.join(thumb_dir, f"{pid}.jpg")),
                "has_preview": pid in preview_pids,
                "has_working_copy": has_wc,
                "has_xmp_sidecar": (
                    os.path.isfile(stem + ".xmp")
                    or os.path.isfile(stem + ".XMP")
                    or os.path.isfile(src + ".xmp")
                    or os.path.isfile(src + ".XMP")
                ),
            })
        attach_nested_edit_recipes(db, out)
        return out

    def heavy_job_active(self):
        for job in self._get_runner().list_jobs():
            if job.get("status") not in (
                "running", "pausing", "paused", "queued",
            ):
                continue
            if job.get("type") in HEAVY_JOB_TYPES:
                return True
        return False

    def invalidate(self, workspace_ids=None):
        """Drop cached Missing Originals results for this app's database.

        Photos are shared across workspaces (a folder can be linked into
        more than one), so a photo-row removal must clear every
        workspace cache that could still list it — scoping to the
        active workspace lets other workspaces keep serving stale
        ready payloads until their next scan.

        ``workspace_ids`` narrows the invalidation to those workspace
        ids. Use it on workspace create/delete to clear entries that
        could otherwise be served to a later workspace that reuses a
        SQLite rowid.
        """
        ws_filter = None if workspace_ids is None else {int(w) for w in workspace_ids}
        with self.lock:
            for store in (
                self.cache,
                self.errors,
            ):
                for key in list(store.keys()):
                    if key[0] != self.db_path:
                        continue
                    if ws_filter is not None and key[1] not in ws_filter:
                        continue
                    store.pop(key, None)
            # Bump generation for every in-flight scan under this DB so
            # its completion path refuses to write its stale
            # pre-invalidation snapshot back into the cache.
            for key in list(self.inflight.keys()):
                if key[0] != self.db_path:
                    continue
                if ws_filter is not None and key[1] not in ws_filter:
                    continue
                self.generation[key] = (
                    self.generation.get(key, 0) + 1
                )

    def start_scan(self, db, folder_id=None, automatic=False):
        key = cache_key(db, folder_id)
        scan_started_at = time.monotonic()
        now = scan_started_at
        token = object()
        suppressed_reason = None
        reuse_existing = False
        fresh_cache = False
        with self.lock:
            inflight = self.inflight.get(key)
            if inflight:
                reuse_existing = True
            entry = self.cache.get(key)
            # Gate on when the last scan STARTED, not when it finished. The
            # navbar re-arms its 30-minute automatic timer from POST time,
            # so a scan that takes real wall-clock time to walk the disk
            # leaves ``set_at`` well under the threshold when the next tick
            # arrives — every other automatic scan would otherwise be
            # skipped, and deletions could stay undiscovered for nearly an
            # hour. Legacy entries without ``started_at`` fall back to
            # ``set_at``.
            if (
                not reuse_existing
                and automatic
                and entry is not None
                and now - entry.get("started_at", entry["set_at"])
                < STALE_SECONDS
            ):
                fresh_cache = True
            err = self.errors.get(key)
            if (
                not reuse_existing
                and not fresh_cache
                and automatic
                and err is not None
                and now < err["backoff_until"]
            ):
                suppressed_reason = "backoff"
        if reuse_existing:
            return self.payload(db, folder_id)
        if fresh_cache:
            return self.payload(db, folder_id)
        if automatic and suppressed_reason is None and self.heavy_job_active():
            suppressed_reason = "heavy_job_active"
        if suppressed_reason is not None:
            payload = self.payload(db, folder_id)
            payload["suppressed"] = True
            payload["reason"] = suppressed_reason
            if suppressed_reason == "heavy_job_active":
                payload["status"] = "skipped"
            return payload
        scan_generation = 0
        with self.lock:
            inflight = self.inflight.get(key)
            if inflight:
                reuse_existing = True
            else:
                self.inflight[key] = token
                scan_generation = self.generation.get(key, 0)
        if reuse_existing:
            return self.payload(db, folder_id)

        runner = self._get_runner()
        ws_id = db._active_workspace_id
        db_file = db._db_path
        scope_label = "workspace" if folder_id is None else f"folder #{folder_id}"

        def work(job):
            thread_db = None
            try:
                thread_db = Database(db_file)
                if ws_id is not None:
                    thread_db.set_active_workspace(ws_id)

                def progress(payload):
                    current = int(payload.get("photos_considered") or 0)
                    total = int(payload.get("total_photos") or 0)
                    missing_found = int(payload.get("missing_found") or 0)
                    folders_checked = int(payload.get("folders_checked") or 0)
                    current_folder = payload.get("current_folder") or ""
                    job["progress"]["current"] = current
                    job["progress"]["total"] = total
                    job["progress"]["current_file"] = current_folder
                    phase = (
                        f"{folders_checked:,} folders checked, "
                        f"{current:,} photos considered, "
                        f"{missing_found:,} missing"
                    )
                    runner.push_event(job["id"], "progress", {
                        "current": current,
                        "total": total,
                        "current_file": current_folder,
                        "folders_checked": folders_checked,
                        "missing_found": missing_found,
                        "phase": phase,
                    })

                def cancel_check():
                    return runner.is_cancelled(job["id"])

                photos = self.build_rows(
                    thread_db,
                    folder_id=folder_id,
                    progress_callback=progress,
                    cancel_callback=cancel_check,
                )
                if cancel_check():
                    return {"cancelled": True, "scope": scope_label}
                checked_at = _utc_iso_now()
                stale = False
                with self.lock:
                    current_gen = self.generation.get(key, 0)
                    if cancel_check():
                        return {"cancelled": True, "scope": scope_label}
                    if current_gen != scan_generation:
                        # A batch delete (or other invalidation) fired
                        # while this scan was walking the disk. Its photo
                        # list reflects the pre-delete library, so writing
                        # it back would resurrect just-removed photos in
                        # the banner. Drop the result and let the next
                        # scan recompute.
                        stale = True
                    else:
                        self.cache[key] = {
                            "photos": photos,
                            "checked_at": checked_at,
                            "set_at": time.monotonic(),
                            "started_at": scan_started_at,
                        }
                        self.errors.pop(key, None)
                return {
                    "missing_count": len(photos),
                    "checked_at": checked_at,
                    "scope": scope_label,
                    "stale": stale,
                }
            except MissingPhotosCancelled:
                raise
            except Exception as exc:
                checked_at = _utc_iso_now()
                with self.lock:
                    current_gen = self.generation.get(key, 0)
                    if current_gen == scan_generation:
                        self.errors[key] = {
                            "error": str(exc) or exc.__class__.__name__,
                            "checked_at": checked_at,
                            "set_at": time.monotonic(),
                            "backoff_until": (
                                time.monotonic()
                                + BACKOFF_SECONDS
                            ),
                        }
                raise
            finally:
                if thread_db is not None:
                    thread_db.close()
                with self.lock:
                    if self.inflight.get(key) in (
                        token,
                        job["id"],
                    ):
                        self.inflight.pop(key, None)

        try:
            job_id = runner.start(
                "missing_originals_scan",
                work,
                workspace_id=ws_id,
                config={"scope": scope_label, "folder_id": folder_id},
                ephemeral=False,
                counts_for_badge=True,
            )
        except Exception:
            with self.lock:
                if self.inflight.get(key) is token:
                    self.inflight.pop(key, None)
            raise

        with self.lock:
            if self.inflight.get(key) is token:
                self.inflight[key] = job_id
        payload = self.payload(db, folder_id)
        if payload.get("status") != "ready":
            payload["job_id"] = job_id
            payload["pending"] = True
            payload["status"] = "pending"
        return payload

    def folder_health_loop(self):
        """Periodically check folder health."""
        time.sleep(30)  # Initial delay
        while True:
            health_db = None
            try:
                health_db = Database(self.db_path)
                changed = health_db.check_folder_health()
                if changed:
                    log.info("Folder health check: %d folder(s) changed status", changed)
                    # A background ok↔missing flip would otherwise leave a
                    # ready /api/photos/missing cache serving the pre-flip
                    # photo list: the modal/banner could offer to delete
                    # rows whose folder just went offline, or hide ghosts
                    # from a folder that just came back, until a later
                    # rescan replaced the entry.
                    self.invalidate()
            except Exception:
                log.debug("Folder health check failed", exc_info=True)
            finally:
                if health_db is not None:
                    health_db.close()
            time.sleep(600)  # 10 minutes
