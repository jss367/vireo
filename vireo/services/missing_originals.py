"""Missing Originals scans, their per-app result cache, and folder health.

``get_missing_photos`` is the scan itself: the active workspace's photos in
present folders (optionally one folder's subtree), checked against one
directory listing per folder. ``get_folder_health_version`` reads the version
the folder-health loop bumps.

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
import unicodedata
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, NotRequired, TypedDict

import dir_listing_cache
from db import _SQLITE_PARAM_CHUNK_SIZE as SQLITE_PARAM_CHUNK_SIZE
from db import Database, MissingPhotosCancelled
from photo_payload import attach_nested_edit_recipes

if TYPE_CHECKING:
    from jobs import JobRunner
    from services.service_types import JobDict, JobResult, JsonDict

log = logging.getLogger(__name__)

# Photos between progress reports while a scan walks its folders.
_MISSING_PHOTOS_PROGRESS_INTERVAL = 200


def _present_names_via_listing(
    folder_path: str, listing_pass: dir_listing_cache.ListingPass,
    check_cancelled: Callable[[], None],
) -> set[str]:
    """NFC names present in ``folder_path``, read through ``listing_pass``.

    Mirrors the direct ``os.scandir`` read (``_present_names_via_scandir``): a
    symlink whose target is gone does not count as present. A remembered
    listing cannot vouch for a target, so symlinks are checked live whether
    the listing was reused or just read. Raises ``OSError`` like the direct
    read.
    """
    listing = dir_listing_cache.read_directory(
        folder_path, listing_pass, on_entry=check_cancelled,
    )
    names: set[str] = set()
    for name in listing.names:
        if name in listing.symlinks and not os.path.exists(
            os.path.join(folder_path, name)
        ):
            continue
        names.add(_nfc(name))
    return names


def _present_names_via_scandir(
    folder_path: str, check_cancelled: Callable[[], None],
) -> set[str]:
    """NFC names present in ``folder_path``, read with ``os.scandir``.

    A symlink whose target is gone does not count as present. Raises
    ``OSError`` when the folder cannot be read.
    """
    names_set: set[str] = set()
    with os.scandir(folder_path) as it:
        for entry in it:
            check_cancelled()
            # Broken symlinks: scandir returns the basename
            # even when the target is gone, but the prior
            # os.path.exists check returned False. Filter
            # them so missing originals tracked via
            # symlinks still surface. is_symlink() uses
            # cached lstat from scandir, so non-symlinks
            # don't pay an extra stat.
            if entry.is_symlink() and not os.path.exists(entry.path):
                continue
            names_set.add(_nfc(entry.name))
    return names_set


def _scan_missing_photos(
    rows: Sequence[Any], progress_callback: Callable[[JsonDict], object] | None,
    listing_pass: dir_listing_cache.ListingPass | None,
    check_cancelled: Callable[[], None],
) -> list[Any]:
    """The rows of ``get_missing_photos`` whose file is gone from disk."""
    # One readdir per folder instead of one stat per photo. On a 50k-photo
    # library across a network volume the per-photo `os.path.exists` was
    # costing minutes; a single scandir + set-membership check is orders
    # of magnitude faster and the dominant call site for this endpoint.
    # Misses fall back to a single os.path.exists to honor FS-specific
    # case rules without unconditionally case-folding (which would
    # silently collapse distinct files on case-sensitive volumes).
    folder_online: dict[int, bool] = {}
    folder_names: dict[int, set[str] | None] = {}
    missing: list[Any] = []
    photos_considered = 0
    folders_checked = 0
    reported_folders: set[int] = set()
    progress_callback_enabled = True

    def report_progress(current_folder: str) -> None:
        nonlocal progress_callback_enabled
        if progress_callback is None or not progress_callback_enabled:
            return
        try:
            progress_callback({
                "folders_checked": folders_checked,
                "photos_considered": photos_considered,
                "missing_found": len(missing),
                "total_photos": len(rows),
                "current_folder": current_folder,
            })
        except Exception:
            progress_callback_enabled = False
            log.exception("Missing photos progress callback failed")

    def report_photo_progress(current_folder: str) -> None:
        if photos_considered % _MISSING_PHOTOS_PROGRESS_INTERVAL == 0:
            report_progress(current_folder)

    for row in rows:
        check_cancelled()
        fid = row["folder_id"]
        if fid not in folder_online:
            folder_online[fid] = os.path.isdir(row["folder_path"])
        if not folder_online[fid]:
            # Whole folder is offline — surfaced by missing-folders flow.
            if fid not in reported_folders:
                reported_folders.add(fid)
                folders_checked += 1
                report_progress(row["folder_path"])
            continue
        if fid not in folder_names:
            try:
                if listing_pass is not None:
                    folder_names[fid] = _present_names_via_listing(
                        row["folder_path"], listing_pass, check_cancelled,
                    )
                else:
                    folder_names[fid] = _present_names_via_scandir(
                        row["folder_path"], check_cancelled,
                    )
            except OSError:
                # Folder vanished between isdir and scandir, or unreadable;
                # treat the same as "folder offline" so we don't bulk-flag
                # every photo as a ghost.
                folder_names[fid] = None
            if fid not in reported_folders:
                reported_folders.add(fid)
                folders_checked += 1
                report_progress(row["folder_path"])
        names = folder_names[fid]
        photos_considered += 1
        if names is None:
            report_photo_progress(row["folder_path"])
            continue
        if _nfc(row["filename"]) in names:
            report_photo_progress(row["folder_path"])
            continue
        # NFC miss: defer to the kernel for case rules. On case-insensitive
        # volumes (APFS default, NTFS) os.path.exists resolves a
        # case-mismatched name; on case-sensitive volumes (most Linux
        # filesystems) it correctly reports the file as absent.
        check_cancelled()
        if not os.path.exists(os.path.join(row["folder_path"], row["filename"])):
            missing.append(row)
        report_photo_progress(row["folder_path"])
    check_cancelled()
    report_progress("")
    return missing


def _nfc(name: str) -> str:
    """NFC-normalize a filename for byte-exact comparison against scandir output.

    macOS APFS stores names as written but compares with normalization, so the
    DB row may be NFC while a filename on disk is NFD (or vice versa). NFC on
    both sides makes set-membership reliable across that mismatch.

    Case is intentionally NOT folded here: case-sensitivity depends on the
    underlying filesystem (APFS default and NTFS are case-insensitive; ext4
    and most network mounts are not). Unconditional lowercasing would collapse
    distinct files on case-sensitive volumes; ``get_missing_photos`` instead
    falls back to ``os.path.exists`` on miss, deferring case rules to the kernel.
    """
    return unicodedata.normalize("NFC", name)


def get_folder_health_version(db: Database) -> int:
    """Return the monotonic version for folder-health-visible changes."""
    value = db.meta.get("folder_health_version")
    return int(value or 0)


def missing_photos_subtree_clause(
    db: Database, folder_id: int | None, params: list[Any],
) -> str | None:
    """The SQL restricting ``get_missing_photos`` to ``folder_id``'s subtree.

    Appends its parameters to ``params``. Returns ``""`` for the whole
    workspace and ``None`` when the folder has no subtree to scan.
    """
    subtree_clause = ""
    if folder_id is not None:
        # Restrict to the folder subtree. Path-prefix expansion (the same
        # helper used by add_workspace_folder etc.) covers legacy rows
        # whose parent_id is NULL — a plain recursive walk over parent_id
        # would silently drop those subfolders.
        subtree_ids = db.folders.subtree_ids_by_path(folder_id)
        if not subtree_ids:
            return None
        if len(subtree_ids) <= SQLITE_PARAM_CHUNK_SIZE:
            placeholders = ",".join("?" for _ in subtree_ids)
            subtree_clause = f" AND f.id IN ({placeholders})"
            params.extend(subtree_ids)
        else:
            # A workspace root with thousands of descendant folders would
            # overflow SQLITE_MAX_VARIABLE_NUMBER (999 on legacy builds)
            # in a single IN(...) clause. Stage the ids in a
            # connection-local temp table and join through that instead.
            db.stats.stage_scope_ids("missing_subtree_ids", subtree_ids)
            subtree_clause = (
                " AND f.id IN (SELECT id FROM missing_subtree_ids)"
            )
    return subtree_clause


def get_missing_photos(
    db: Database,
    folder_id: int | None = None,
    progress_callback: Callable[[JsonDict], object] | None = None,
    cancel_callback: Callable[[], object] | None = None,
    listing_pass: dir_listing_cache.ListingPass | None = None,
) -> list[Any]:
    """Return photos whose source file is missing from disk.

    Scoped to the active workspace. Skips photos in folders flagged
    ``'missing'`` — those are surfaced by ``folders.missing`` and
    listing them per-photo would just duplicate that signal at high cost.

    When ``folder_id`` is given, the result is further restricted to
    that folder and every folder beneath it in the tree. This backs the
    "rescan a specific folder" flow — the user asked about one folder,
    so the deleted-original review must not surface ghosts from unrelated
    parts of the library. Descendant discovery goes through
    ``folders.subtree_ids_by_path`` so legacy rows whose ``parent_id`` is
    NULL still count as descendants of their path-prefixed root. ``None``
    (the default) keeps the whole-workspace behavior.

    Folder DB ``status`` is updated asynchronously by a 10-minute health
    loop, so a freshly unmounted volume can still show ``status='ok'``
    when this query runs. To avoid surfacing thousands of "ghosts" for
    a temporarily offline drive (and offering them up for bulk delete),
    we also treat any folder whose root no longer resolves on disk as
    if it were already flagged missing. Resolution is cached per folder
    within the call so a 1000-photo folder doesn't stat the same root
    a thousand times.

    Each row carries ``folder_path``, ``timestamp``, and
    ``working_copy_path`` so the caller can render rich UI without
    joining again.

    ``progress_callback`` is optional. When supplied, it receives dicts
    containing ``folders_checked``, ``photos_considered``, ``missing_found``,
    ``total_photos``, and ``current_folder``. Callback exceptions are
    logged and ignored so progress reporting cannot abort detection.

    ``cancel_callback`` is optional. When supplied, it is polled between
    filesystem operations and may abort the scan by returning true.

    ``listing_pass`` is optional (a :class:`dir_listing_cache.ListingPass`).
    When supplied, each folder is read through it: an unchanged folder's
    remembered listing is reused instead of read from disk again, and its
    symlinks are still checked live.
    """
    def check_cancelled() -> None:
        if cancel_callback is not None and cancel_callback():
            raise MissingPhotosCancelled("missing photos scan cancelled")

    check_cancelled()
    params: list[Any] = [db.require_workspace_id()]
    subtree_clause = missing_photos_subtree_clause(db, folder_id, params)
    if subtree_clause is None:
        return []
    check_cancelled()
    rows = db.folders.photos_in_present_folders(
        subtree_clause, params,
    )
    check_cancelled()
    return _scan_missing_photos(
        rows, progress_callback, listing_pass, check_cancelled,
    )


# ``(db_path, workspace_id, folder_id)``: one Missing Originals scan scope.
CacheKey = tuple[str, int | None, int | None]


class CacheEntry(TypedDict):
    """A finished scan's photo list in ``MissingOriginals.cache``."""

    photos: list[JsonDict]
    checked_at: str
    set_at: float
    # Entries written before scans recorded their start time lack it.
    started_at: NotRequired[float]


class ScanError(TypedDict):
    """A failed scan and its backoff in ``MissingOriginals.errors``."""

    error: str
    checked_at: str
    set_at: float
    backoff_until: float


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


def _utc_iso_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


# Any: a raw query-string or JSON value; ``int()`` decides whether it is usable.
def resolve_folder_id(db: Database, folder_id: Any) -> int | None:
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
    linked = db.workspace_folders.visible_ids(db.active_workspace_id, [folder_id])
    if not linked:
        raise LookupError("folder not found")
    return folder_id


def cache_key(db: Database, folder_id: int | None) -> CacheKey:
    return (db._db_path, db.active_workspace_id, folder_id)


@dataclass(frozen=True)
class _ScanRun:
    """One started Missing Originals scan, as its work function sees it."""

    key: CacheKey
    token: object
    generation: int
    started_at: float
    runner: JobRunner
    ws_id: int | None
    db_file: str
    folder_id: int | None
    automatic: bool
    scope_label: str


def _scan_progress_callback(runner: JobRunner, job: JobDict) -> Callable[[JsonDict], None]:
    """Build the ``get_missing_photos`` progress callback for a scan job."""

    def progress(payload: JsonDict) -> None:
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

    return progress


class MissingOriginals:
    """Per-app Missing Originals scan state and the folder-health loop.

    ``config`` is the app's config mapping (``THUMB_CACHE_DIR`` is read at
    scan time) and ``get_runner`` returns the app's ``JobRunner``.
    """

    # Any: the Flask app config holds heterogeneous settings values.
    def __init__(
        self, *, db_path: str, config: Mapping[str, Any], get_runner: Callable[[], JobRunner],
    ) -> None:
        self.db_path = db_path
        self._config = config
        self._get_runner = get_runner
        self.lock = threading.Lock()
        self.cache: dict[CacheKey, CacheEntry] = {}
        # A started scan's placeholder token until ``runner.start`` returns
        # its job id.
        self.inflight: dict[CacheKey, object] = {}
        self.errors: dict[CacheKey, ScanError] = {}
        # Monotonic per-key counter bumped whenever the cache is invalidated
        # while a scan is in flight. Each scan snapshots this at start; if
        # the counter has advanced by the time it finishes, the scan's
        # results are from a pre-invalidation view of the library and must
        # be discarded so deleted photos don't reappear in the banner/modal.
        self.generation: dict[CacheKey, int] = {}

    def payload(self, db: Database, folder_id: int | None) -> JsonDict:
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
                "workspace_id": db.active_workspace_id,
                "folder_id": folder_id,
            }

    def build_rows(
        self,
        db: Database,
        folder_id: int | None = None,
        progress_callback: Callable[[JsonDict], None] | None = None,
        cancel_callback: Callable[[], bool] | None = None,
        listing_pass: dir_listing_cache.ListingPass | None = None,
    ) -> list[JsonDict]:
        thumb_dir = self._config["THUMB_CACHE_DIR"]
        vireo_dir = os.path.dirname(thumb_dir)
        preview_dir = os.path.join(vireo_dir, "previews")
        working_dir = os.path.join(vireo_dir, "working")

        def check_cancelled() -> None:
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

        out: list[JsonDict] = []
        for row in get_missing_photos(
            db,
            folder_id=folder_id,
            progress_callback=progress_callback,
            cancel_callback=cancel_callback,
            listing_pass=listing_pass,
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

    def heavy_job_active(self) -> bool:
        for job in self._get_runner().list_jobs():
            if job.get("status") not in (
                "running", "pausing", "paused", "queued",
            ):
                continue
            if job.get("type") in HEAVY_JOB_TYPES:
                return True
        return False

    def invalidate(self, workspace_ids: Iterable[int] | None = None) -> None:
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

    def start_scan(self, db: Database, folder_id: int | None = None, automatic: bool = False) -> JsonDict:
        key = cache_key(db, folder_id)
        scan_started_at = time.monotonic()
        token = object()
        early_payload = self._skipped_scan_payload(
            db, key, folder_id, automatic, scan_started_at,
        )
        if early_payload is not None:
            return early_payload
        scan_generation = 0
        reuse_existing = False
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
        ws_id = db.active_workspace_id
        scan = _ScanRun(
            key=key,
            token=token,
            generation=scan_generation,
            started_at=scan_started_at,
            runner=runner,
            ws_id=ws_id,
            db_file=db._db_path,
            folder_id=folder_id,
            automatic=automatic,
            scope_label=(
                "workspace" if folder_id is None else f"folder #{folder_id}"
            ),
        )

        def work(job: JobDict) -> JobResult:
            return self._run_scan_job(job, scan)

        try:
            job_id = runner.start(
                "missing_originals_scan",
                work,
                workspace_id=ws_id,
                config={"scope": scan.scope_label, "folder_id": folder_id},
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

    def _skipped_scan_payload(
        self, db: Database, key: CacheKey, folder_id: int | None, automatic: bool, now: float,
    ) -> JsonDict | None:
        """The payload to answer with when no new scan should start, or None.

        A scan already in flight is reused; an automatic scan is skipped
        while the cache is fresh, during an error backoff, and while heavy
        work is running.
        """
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
        return None

    def _run_scan_job(self, job: JobDict, scan: _ScanRun) -> JsonDict:
        """The ``missing_originals_scan`` work function."""
        key = scan.key
        runner = scan.runner
        thread_db = None
        try:
            thread_db = Database(scan.db_file)
            if scan.ws_id is not None:
                thread_db.set_active_workspace(scan.ws_id)

            progress = _scan_progress_callback(runner, job)

            def cancel_check() -> bool:
                return runner.is_cancelled(job["id"])

            # The 30-minute automatic check reuses the listing of every
            # folder unchanged since it was last read; a check the user
            # asked for reads every folder, and records what it read.
            listing_pass = dir_listing_cache.ListingPass(
                dir_listing_cache.get_shared(), reuse=scan.automatic,
            )
            photos = self.build_rows(
                thread_db,
                folder_id=scan.folder_id,
                progress_callback=progress,
                cancel_callback=cancel_check,
                listing_pass=listing_pass,
            )
            if cancel_check():
                return {"cancelled": True, "scope": scan.scope_label}
            checked_at = _utc_iso_now()
            stale = False
            with self.lock:
                current_gen = self.generation.get(key, 0)
                if cancel_check():
                    return {"cancelled": True, "scope": scan.scope_label}
                if current_gen != scan.generation:
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
                        "started_at": scan.started_at,
                    }
                    self.errors.pop(key, None)
            return {
                "missing_count": len(photos),
                "checked_at": checked_at,
                "scope": scan.scope_label,
                "stale": stale,
                # Which folders were read from disk and which were
                # reused unchanged since the last look.
                "folders_read": listing_pass.read,
                "folders_unchanged": listing_pass.unchanged,
            }
        except MissingPhotosCancelled:
            raise
        except Exception as exc:
            self._record_scan_error(scan, exc)
            raise
        finally:
            if thread_db is not None:
                thread_db.close()
            with self.lock:
                if self.inflight.get(key) in (
                    scan.token,
                    job["id"],
                ):
                    self.inflight.pop(key, None)

    def _record_scan_error(self, scan: _ScanRun, exc: BaseException) -> None:
        """Record a failed scan and its backoff, unless invalidated since."""
        checked_at = _utc_iso_now()
        with self.lock:
            current_gen = self.generation.get(scan.key, 0)
            if current_gen == scan.generation:
                self.errors[scan.key] = {
                    "error": str(exc) or exc.__class__.__name__,
                    "checked_at": checked_at,
                    "set_at": time.monotonic(),
                    "backoff_until": (
                        time.monotonic()
                        + BACKOFF_SECONDS
                    ),
                }

    def folder_health_loop(self) -> None:
        """Periodically check folder health."""
        time.sleep(30)  # Initial delay
        while True:
            health_db = None
            try:
                health_db = Database(self.db_path)
                changed = health_db.folders.check_health()
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
