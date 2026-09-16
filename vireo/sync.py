"""Sync engine: reconcile database and XMP sidecars."""

import logging
import os
import threading
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

from db import KEYWORD_SOURCE_UNKNOWN
from keyword_identity import resolve_import_path, validate_import_locations
from keyword_normalization import keyword_match_key
from xmp import SidecarEditor, read_hierarchical_keywords, read_keywords

log = logging.getLogger(__name__)

# Publishing a sidecar is network-latency bound -- temp-file create, fsync,
# ACL and xattr copy, rename -- not CPU bound, so writes to different files
# overlap almost perfectly. Eight keeps a NAS busy without flooding an SMB
# connection's request queue, and the GIL is released for every one of those
# syscalls.
_SYNC_MAX_WORKERS = 8


def _resolve_xmp_paths(db, photo_ids, folder_paths=None):
    """Map photo ids to sidecar paths with two queries instead of 2N.

    Resolving one photo at a time ran the recursive folder-tree CTE and a
    full photo-detail SELECT per photo; ``sync_from_xmp`` already hoists the
    folder map. Photos whose row no longer exists are absent from the result,
    which is how the caller detects them. A photo whose folder is not in the
    active workspace keeps the historical behaviour of resolving against an
    empty folder path, so it fails the accessibility check rather than
    silently writing somewhere else.

    ``folder_paths`` overrides the workspace-scoped folder map. The
    pre-transfer sync for a pending NAS archive passes one built from every
    catalog folder because the staging tree may have been unlinked from its
    owning workspace -- the transfer is defined by a path on disk, not by
    workspace membership -- and the ordinary map would resolve it to an empty
    directory and then fail "folder not accessible".
    """
    if folder_paths is not None:
        folders = folder_paths
    else:
        folders = {f["id"]: f["path"] for f in db.get_folder_tree()}
    paths = {}
    for photo_id, (folder_id, filename) in db.get_photo_filenames(photo_ids).items():
        base = os.path.splitext(filename)[0]
        paths[photo_id] = os.path.join(folders.get(folder_id, ""), base + ".xmp")
    return paths


def _sync_flags_to_xmp_enabled(db):
    """Return whether the active workspace should write flags to XMP."""
    try:
        import config as cfg

        return bool(db.get_effective_config(cfg.load()).get("sync_flags_to_xmp", False))
    except Exception:
        log.warning("Failed to read sync_flags_to_xmp config", exc_info=True)
        return False


def _write_assigned_location_to_xmp_enabled(db):
    """Return whether the active workspace should write assigned GPS to XMP."""
    try:
        import config as cfg

        return bool(
            db.get_effective_config(cfg.load()).get(
                "write_assigned_location_to_xmp", False
            )
        )
    except Exception:
        log.warning("Failed to read write_assigned_location_to_xmp config", exc_info=True)
        return False


_KEYWORD_CHANGE_TYPES = ("keyword_add", "keyword_remove", "keyword_remove_flat")


def _select_changes(changes, change_ids):
    """Restrict ``changes`` to ``change_ids`` plus their paired keyword changes.

    Auto-includes any unselected pending keyword_add / keyword_remove
    changes that share a (photo_id, normalized key) with a selected one.
    Both remove_keywords() (for keyword_remove) and the add-canonicalization
    pass in ``_remove_planned_keywords`` match by normalized key, so a
    rename's paired add(clean) + remove(legacy variant) split across two
    syncs lets each half clobber the sidecar entry the other half writes --
    the add-only sync strips the legacy ``<rdf:li>`` before writing the
    clean spelling, and a later remove-only sync strips the clean spelling
    under the same normalized match. Sync both sides together whenever the
    user picks either.
    """
    selected_ids = {int(cid) for cid in change_ids}
    kw_index = defaultdict(list)
    for c in changes:
        if c["change_type"] in _KEYWORD_CHANGE_TYPES and c["value"]:
            key = (c["photo_id"], keyword_match_key(c["value"]))
            kw_index[key].append(c["id"])
    for c in changes:
        if c["id"] not in selected_ids:
            continue
        if c["change_type"] not in _KEYWORD_CHANGE_TYPES or not c["value"]:
            continue
        key = (c["photo_id"], keyword_match_key(c["value"]))
        selected_ids.update(kw_index[key])
    return [c for c in changes if c["id"] in selected_ids]


@dataclass
class _PhotoSyncPlan:
    """Everything one photo's pending changes ask us to write to its sidecar."""

    keywords_to_add: set = field(default_factory=set)
    keywords_to_remove: set = field(default_factory=set)
    # ``keyword_remove_flat`` is queued by ``repair_duplicate_photo_species``
    # when a detached root spelling still appears as an ancestor segment of a
    # preserved hierarchy leaf. A regular hierarchical ``keyword_remove``
    # would strip that preserved ``lr:hierarchicalSubject`` entry; flat-only
    # removal touches only the stale ``dc:subject`` line.
    keywords_to_remove_flat: set = field(default_factory=set)
    rating: int | None = None
    flag: str | None = None
    edit_recipe_json: str | None = None
    sync_location: bool = False
    cleanup_location: bool = False
    # (change_id, change_token) per supported change. The clear runs after
    # the sidecar write, by which time a reused rowid can name a different
    # row -- see Database.clear_pending_by_token.
    supported_changes: list = field(default_factory=list)
    unsupported_changes: list = field(default_factory=list)


def _plan_photo_sync(photo_changes, sync_flags, sync_locations):
    """Fold one photo's pending changes into a ``_PhotoSyncPlan``."""
    plan = _PhotoSyncPlan()
    for c in photo_changes:
        kind = c["change_type"]
        if kind == "keyword_add":
            plan.keywords_to_add.add(c["value"])
        elif kind == "keyword_remove":
            plan.keywords_to_remove.add(c["value"])
        elif kind == "keyword_remove_flat":
            plan.keywords_to_remove_flat.add(c["value"])
        elif kind == "rating":
            plan.rating = int(c["value"])
        elif kind == "flag":
            if not sync_flags:
                plan.unsupported_changes.append(c)
                continue
            plan.flag = c["value"] or "none"
        elif kind == "location":
            if sync_locations:
                plan.sync_location = True
            else:
                plan.cleanup_location = True
        elif kind == "edit_recipe":
            plan.edit_recipe_json = c["value"] or ""
        else:
            continue
        plan.supported_changes.append((c["id"], c["change_token"]))
    return plan


def _remove_planned_keywords(editor, plan):
    """Strip sidecar keywords the plan removes or is about to re-add.

    Removals run BEFORE additions. remove_keywords() compares by normalized
    match key, so a remove of `‘apapane` matches any `<rdf:li>` whose text
    normalizes to `apapane` -- including a clean `apapane` we would otherwise
    have just added. A rename that queues remove `‘apapane` and add `apapane`
    for the same photo would then have its newly-written clean entry stripped
    along with the old quoted one, clearing pending changes and leaving the
    sidecar without the keyword. Applying the remove first strips only the
    pre-existing quoted variant; the subsequent add_keywords then writes the
    clean spelling.

    Removals are split by whether they're paired with an add for the same
    normalized key. A paired remove+add is a normalization-only rename (e.g.
    remove `‘Birds` + add `Birds`); hierarchical mode would then strip
    unrelated hierarchies like `Animals|Birds|Hawk` because remove_keywords()
    matches by any pipe-segment key. Use flat-only removal for those paired
    removes so the rename only touches the flat `dc:subject` legacy entry.
    Solo removes keep hierarchical semantics so real keyword deletions still
    drop pipe-segment matches.
    """
    if plan.keywords_to_remove or plan.keywords_to_remove_flat:
        paired_keys = {keyword_match_key(kw) for kw in plan.keywords_to_add}
        paired_keys.discard("")
        paired_removes = {
            kw for kw in plan.keywords_to_remove
            if keyword_match_key(kw) in paired_keys
        }
        solo_removes = plan.keywords_to_remove - paired_removes
        if solo_removes:
            editor.remove_keywords(solo_removes)
        # Merge repair-queued flat-only removes with the rename-paired flat
        # removes: both take exactly the ``hierarchical=False`` code path.
        flat_removes = paired_removes | plan.keywords_to_remove_flat
        if flat_removes:
            editor.remove_keywords(flat_removes, hierarchical=False)

    # Strip any sidecar dc:subject entry that normalizes to a keyword we're
    # about to add. add_keywords() dedupes with an exact-string set
    # difference, so a pure keyword_add for `apapane` against a legacy
    # sidecar `‘apapane` would append a second <rdf:li>. Canonicalizing
    # first collapses variants into the clean spelling written next. Use the
    # flat-only mode: a hierarchical remove (which drops any entry whose
    # segment matches) would delete unrelated hierarchies such as
    # `Animals|Birds|Hawk` when we add flat `Birds`. ``keep_exact`` leaves an
    # entry that is already the clean spelling alone, so re-syncing a sidecar
    # that already carries the keyword stays a no-op instead of deleting and
    # re-appending the same text.
    if plan.keywords_to_add:
        editor.remove_keywords(
            plan.keywords_to_add, hierarchical=False, keep_exact=True,
        )


def _write_photo_sync(xmp_path, plan, assigned_location=None, create_missing_sidecars=False):
    """Apply a ``_PhotoSyncPlan`` to the photo's sidecar, in dependency order.

    Every mutation lands in one ``SidecarEditor``, so the sidecar is parsed
    once and republished once no matter how many change types the photo
    queued. The ordering below still matters: it decides what the single
    published tree contains.

    ``assigned_location`` is passed in rather than looked up here because the
    writers run on a pool thread and the SQLite connection belongs to the
    caller's thread.

    ``create_missing_sidecars`` only affects a rating-only photo, the one
    mutation that otherwise declines to create a sidecar; see
    ``SidecarEditor.set_rating``.
    """
    editor = SidecarEditor(xmp_path)
    _remove_planned_keywords(editor, plan)

    # Apply keyword additions after removals so a same-photo remove+add
    # pair does not cancel out (see _remove_planned_keywords).
    if plan.keywords_to_add:
        editor.add_keywords(
            flat_keywords=plan.keywords_to_add, hierarchical_keywords=set()
        )

    # Apply the flag before the rating: a flag creates a sidecar if needed,
    # while a rating intentionally only updates existing ones.
    if plan.flag is not None:
        editor.set_pick_flag(plan.flag)

    if plan.sync_location:
        loc = assigned_location
        if loc and loc.get("latitude") is not None and loc.get("longitude") is not None:
            editor.set_gps_location(
                loc["latitude"],
                loc["longitude"],
                source=loc.get("source") or "assigned",
            )
        else:
            editor.remove_vireo_gps_location()
    elif plan.cleanup_location:
        editor.remove_vireo_gps_location()

    if plan.edit_recipe_json is not None:
        editor.set_edit_recipe(plan.edit_recipe_json)

    # Apply the rating after every operation that can create a sidecar.
    # Rating alone intentionally remains a no-op for missing XMP, but a
    # selected keyword, flag, location, or edit write should make the
    # same-photo rating persist rather than silently clear it.
    if plan.rating is not None:
        editor.set_rating(plan.rating, create=create_missing_sidecars)

    # One publish for the whole photo. Nothing is written when no mutation
    # changed anything -- re-syncing an already-correct sidecar costs a read.
    editor.commit()


# How many distinct failure reasons a sync reports up to the job layer. A NAS
# that rejects every write produces one reason repeated thousands of times;
# the summary exists to name the cause, not to reproduce the log.
_MAX_REPORTED_FAILURE_REASONS = 5


def _failure_reason(exc):
    """Path-free failure cause for grouping identical failures across photos.

    ``str(OSError)`` renders as ``[Errno 13] Permission denied: '/path/to.xmp'``
    -- the trailing per-file path makes every entry unique, so counting raw
    error strings would turn one NAS-wide EACCES into thousands of distinct
    ``(1 photo)`` reasons and defeat the summary. Rebuild the message from
    ``errno`` / ``strerror`` so the per-photo path drops out.
    """
    if isinstance(exc, OSError) and exc.strerror:
        if exc.errno is not None:
            return f"[Errno {exc.errno}] {exc.strerror}"
        return exc.strerror
    return str(exc)


def _sync_result(synced, failures):
    """Build the sync result, telling the job layer whether it actually worked.

    ``ok`` / ``errors`` are the JobRunner's partial-failure convention: a run
    that wrote 10 sidecars and failed on 2,230 must land in history as
    "failed", not "completed", so the UI cannot report success over a NAS
    that rejected every write.
    """
    # Count each (reason, photo_id) pair once: a photo with two queued
    # unsupported changes of the same type produces two failure records with
    # identical reasons, but the summary reports "photos", not records.
    seen = set()
    counts = Counter()
    for f in failures:
        reason = f.get("reason") or f["error"]
        key = (reason, f.get("photo_id"))
        if key in seen:
            continue
        seen.add(key)
        counts[reason] += 1
    reasons = [
        f"{reason} ({count} photo{'s' if count != 1 else ''})"
        for reason, count in counts.most_common(_MAX_REPORTED_FAILURE_REASONS)
    ]
    remaining = len(counts) - len(reasons)
    if remaining > 0:
        reasons.append(f"...and {remaining} more distinct error(s)")
    return {
        "synced": synced,
        "failed": len(failures),
        "failures": failures,
        "ok": not failures,
        "errors": reasons,
    }


def sync_to_xmp(db, progress_callback=None, change_ids=None, create_missing_sidecars=False,
                folder_paths=None, require_workspace_membership=True):
    """Write pending changes to XMP sidecars.

    Args:
        db: Database instance
        progress_callback: optional callable(current, total)
        change_ids: optional pending_changes ids to sync. When provided, any
            other queued changes are left pending.
        create_missing_sidecars: write a sidecar for a rating-only photo
            instead of skipping it. Off for the ordinary sync job, which
            would otherwise litter a sidecar beside every rated photo and
            can retry later anyway. On for the sync that runs before a NAS
            transfer, where "later" does not exist: the transfer deletes the
            local originals, and a cleared-but-unwritten rating is gone.
        folder_paths: optional ``{folder_id: path}`` map used to resolve
            sidecar paths in place of the active workspace's folder tree.
            The pre-transfer sync for a pending NAS archive passes one
            covering every catalog folder because the staging tree may have
            been unlinked from its owning workspace -- membership is not
            what defines the transfer, the path is -- and the workspace-
            scoped map would otherwise fail every photo as "folder not
            accessible".
        require_workspace_membership: gate the assigned-location lookup on
            the photo being visible in the active workspace. Off for the
            pre-transfer sync of a pending NAS archive: the same
            unlinked-staging shape that ``folder_paths`` covers for path
            resolution also breaks ``get_assigned_photo_location``, whose
            default verification would refuse an unlinked photo and fail
            every queued ``location`` change with "photo not in workspace"
            -- another way "Sync metadata and send to NAS" would silently
            miss a supported change on files it can otherwise reach.

    Returns:
        dict with synced, failed, failures counts
    """
    changes = db.get_pending_changes()
    if change_ids is not None:
        changes = _select_changes(changes, change_ids)
    if not changes:
        return _sync_result(0, [])

    by_photo = defaultdict(list)
    for c in changes:
        by_photo[c["photo_id"]].append(c)

    sync_flags = _sync_flags_to_xmp_enabled(db)
    sync_locations = _write_assigned_location_to_xmp_enabled(db)

    # Everything that needs the database happens here, on the caller's
    # thread: the sidecar writers below run on a pool and must not touch the
    # connection.
    xmp_paths = _resolve_xmp_paths(db, list(by_photo), folder_paths=folder_paths)
    prepare_failures = {}
    plans = {}
    folder_accessible = {}
    canonical_folders = {}
    for photo_id, photo_changes in by_photo.items():
        xmp_path = xmp_paths.get(photo_id)
        if not xmp_path:
            prepare_failures[photo_id] = {
                "photo_id": photo_id, "error": "photo not found in DB",
            }
            continue

        # Check if the folder exists (NAS might be offline). Cache the answer
        # per folder: on a slow or offline mount this is a network round trip,
        # and a folder holds thousands of photos. Resolve symlinks here too so
        # the sidecar grouping below sees one canonical folder per folder
        # rather than one syscall per photo.
        folder = os.path.dirname(xmp_path)
        if folder not in folder_accessible:
            folder_accessible[folder] = os.path.isdir(folder)
            if folder_accessible[folder]:
                try:
                    canonical_folders[folder] = os.path.realpath(folder)
                except OSError:
                    canonical_folders[folder] = folder
        if not folder_accessible[folder]:
            prepare_failures[photo_id] = {
                "photo_id": photo_id,
                "error": f"folder not accessible: {folder}",
                # Strip the per-folder path so many photos on an offline NAS
                # summarise as one cause instead of one per subfolder.
                "reason": "folder not accessible",
            }
            continue

        try:
            plans[photo_id] = _plan_photo_sync(
                photo_changes, sync_flags, sync_locations,
            )
        except Exception as e:
            # A malformed queue row -- a rating whose value is NULL or not an
            # integer, which the schema permits -- must fail its own photo, as
            # it did when planning ran inside the per-photo try, rather than
            # abort every other photo's write.
            prepare_failures[photo_id] = {
                "photo_id": photo_id,
                "error": str(e),
                "reason": _failure_reason(e),
            }

    locations = {}
    if sync_locations:
        for photo_id, plan in list(plans.items()):
            if not plan.sync_location:
                continue
            try:
                locations[photo_id] = db.get_assigned_photo_location(
                    photo_id,
                    verify_workspace=require_workspace_membership,
                )
            except Exception as e:
                # Historically this lookup ran inside the per-photo try, so a
                # photo the workspace can no longer see failed alone rather
                # than aborting the run.
                del plans[photo_id]
                prepare_failures[photo_id] = {
                    "photo_id": photo_id,
                    "error": str(e),
                    "reason": _failure_reason(e),
                }

    # Group photos that may share a sidecar, so no two threads publish the
    # same file: a RAW and a JPEG with one basename share one .xmp, and
    # aliases spell the same file differently -- a folder reached through a
    # symlink, or components differing only in case on APFS/SMB/NTFS. The key
    # is the realpath'd folder joined to the basename, then case-folded whole:
    # ``realpath`` preserves the spelling of every component and ``normcase``
    # only lowercases on Windows, so folding the basename alone would leave
    # two folder rows spelled ``/mnt/Photos`` and ``/mnt/PHOTOS`` in separate
    # groups even though the share treats them as one directory.
    #
    # Grouping decides ordering only; each photo is still written through its
    # own path. That makes an over-grouping harmless: two names a
    # case-sensitive filesystem keeps distinct (`Masse.xmp` / `Maße.xmp`,
    # which case-fold alike) are written to their own files, one after the
    # other, instead of in parallel. Deciding instead to keep one path per
    # group would silently write one photo's metadata into the other's
    # sidecar, and knowing which case applies would mean probing the
    # filesystem's case sensitivity -- a heuristic that is wrong for any
    # mount whose in-mount path components carry no letters, and an extra
    # round trip per folder on the NAS this loop exists to keep fast.
    by_sidecar = defaultdict(list)
    for photo_id in plans:
        xmp_path = xmp_paths[photo_id]
        canonical_folder = canonical_folders.get(
            os.path.dirname(xmp_path), os.path.dirname(xmp_path),
        )
        canonical_key = os.path.normcase(
            os.path.join(canonical_folder, os.path.basename(xmp_path))
        ).casefold()
        by_sidecar[canonical_key].append((photo_id, xmp_path))

    sidecar_locks = {}
    sidecar_locks_guard = threading.Lock()

    def lock_for(xmp_path):
        """Return the lock guarding whatever file ``xmp_path`` resolves to.

        Grouping keys on the folder and basename, which catches the aliases
        that arise from the catalog itself -- a RAW and a JPEG, case variants,
        a symlinked folder. It cannot catch a sidecar path that is itself a
        symlink to a different photo's sidecar: those basenames differ, so the
        two land in different groups and run on different workers, while
        ``_write_tree_atomic`` resolves the link and replaces the same file.
        Resolving here costs nothing the publish was not already paying (it
        resolves too) and it happens on the pool thread, not in the serial
        prepare loop. The key is case-folded for the same reason the group key
        is: ``realpath`` keeps each component's spelling, so two case
        spellings of one directory would otherwise take different locks.
        """
        key = os.path.normcase(os.path.realpath(xmp_path)).casefold()
        with sidecar_locks_guard:
            lock = sidecar_locks.get(key)
            if lock is None:
                lock = sidecar_locks[key] = threading.Lock()
        return lock

    def write_sidecar_group(canonical_key):
        """Write every photo queued against one sidecar; never raises."""
        outcomes = {}
        for photo_id, xmp_path in by_sidecar[canonical_key]:
            try:
                # One lock at a time and never nested, so no worker can
                # deadlock against another.
                with lock_for(xmp_path):
                    _write_photo_sync(
                        xmp_path, plans[photo_id], locations.get(photo_id),
                        create_missing_sidecars=create_missing_sidecars,
                    )
            except Exception as e:  # recorded per photo, as before
                outcomes[photo_id] = e
            else:
                outcomes[photo_id] = None
        return outcomes

    results = {}
    total = len(by_photo)
    # Photos that failed preparation are finished work: an offline NAS
    # otherwise leaves the panel reading "0 of 2,240" while the run walks
    # through every one of them.
    completed = len(prepare_failures)
    if progress_callback and completed:
        progress_callback(completed, total)
    if by_sidecar:
        workers = min(_SYNC_MAX_WORKERS, len(by_sidecar))
        with ThreadPoolExecutor(
            max_workers=workers, thread_name_prefix="xmp-sync",
        ) as pool:
            futures = [pool.submit(write_sidecar_group, p) for p in by_sidecar]
            for future in as_completed(futures):
                outcomes = future.result()
                results.update(outcomes)
                completed += len(outcomes)
                if progress_callback:
                    progress_callback(completed, total)

    # Report in queue order regardless of the order the pool finished in.
    synced = 0
    failures = []
    synced_tokens = []
    # Rows predating the change_token column carry NULL, and `IN (NULL)`
    # matches nothing -- clearing those by token would leave them queued
    # forever, rewritten by every later sync. They keep the id-based clear,
    # which is exactly the behaviour they have always had.
    synced_legacy_ids = []
    for photo_id in by_photo:
        if photo_id in prepare_failures:
            failures.append(prepare_failures[photo_id])
        plan = plans.get(photo_id)
        if plan is None:
            continue
        error = results.get(photo_id)
        if error is not None:
            failures.append({
                "photo_id": photo_id,
                "error": str(error),
                "reason": _failure_reason(error),
            })
            log.warning("Failed to sync photo %d: %s", photo_id, error)
            continue
        if plan.supported_changes:
            synced += 1
            for change_id, token in plan.supported_changes:
                if token:
                    synced_tokens.append(token)
                else:
                    synced_legacy_ids.append(change_id)
        for c in plan.unsupported_changes:
            failures.append({
                "photo_id": photo_id,
                "change_id": c["id"],
                "error": f"unsupported change type: {c['change_type']}",
            })

    # Clear by token, not by row id. ``pending_changes.id`` is a bare
    # ``INTEGER PRIMARY KEY``, so SQLite re-issues a deleted row's rowid to the
    # next insert -- and ``queue_flag_change_if_enabled`` deletes the old flag
    # row before inserting the new value. A flag changed while the sidecar
    # write was in flight (as long as the storage takes) could therefore land
    # on the id this run selected, and clearing by id would delete the user's
    # newest edit without ever having written it.
    if synced_tokens:
        db.clear_pending_by_token(
            synced_tokens, clear_equivalent_flat_removals=True,
        )
    if synced_legacy_ids:
        db.clear_pending(
            synced_legacy_ids, clear_equivalent_flat_removals=True,
        )

    log.info("Sync complete: %d synced, %d failed", synced, len(failures))
    return _sync_result(synced, failures)


def sync_from_xmp(db, photo_ids):
    """Re-read XMP sidecars and update database keywords.

    Args:
        db: Database instance
        photo_ids: list of photo ids to re-sync
    """
    folders = {f["id"]: f["path"] for f in db.get_folder_tree()}

    for photo_id in photo_ids:
        photo = db.get_photo(photo_id)
        if not photo:
            continue

        folder_path = folders.get(photo["folder_id"], "")
        base = os.path.splitext(photo["filename"])[0]
        xmp_path = os.path.join(folder_path, base + ".xmp")

        if not os.path.exists(xmp_path):
            continue

        # Serialize the sidecar read, DB reconciliation, and mtime stamp as
        # one writer transaction. Background migrations use the same SQLite
        # writer lock, so they cannot act on a pre-reconciliation association
        # snapshot between this read and the final xmp_mtime update.
        db.conn.execute("BEGIN IMMEDIATE")
        try:
            # Read current XMP keywords. Compare with a normalized match key on
            # both sides so an XMP variant like `‘apapane` matches a DB row
            # stored as `apapane` (add_keyword normalizes on insert). A plain
            # `.lower()` comparison would treat them as different names, making
            # the add-side an INSERT-OR-IGNORE no-op and then prune the DB tag
            # because the raw DB name is not in the raw XMP set -- leaving the
            # photo untagged.
            #
            # Skip XMP entries whose normalized match key is empty (e.g. a
            # lone ASCII or smart quote). add_keyword() now raises ValueError
            # for names that normalize to empty, so keeping such entries would
            # abort the whole sidecar reconcile on a malformed edge-quote
            # keyword instead of ignoring it and processing the rest.
            xmp_keywords = read_keywords(xmp_path)
            pending_removals = db.get_pending_keyword_removal_keys(photo_id)
            pending_hierarchical_removals = db.get_pending_keyword_removal_keys(
                photo_id, hierarchical=True,
            )
            pending_flat_only_removals = (
                pending_removals - pending_hierarchical_removals
            )
            xmp_keywords_by_key = {}
            for kw in xmp_keywords:
                key = keyword_match_key(kw)
                if not key or key in pending_removals:
                    continue
                xmp_keywords_by_key.setdefault(key, kw)

            # Get current DB keywords
            db_keywords = db.get_photo_keywords(photo_id)
            db_keywords_by_key = {
                keyword_match_key(k["name"]): k for k in db_keywords
            }
            # Confirmed imported paths remain valid even if the linked place
            # was subsequently renamed. Preserve the resolved association
            # during the removal pass as well as the add pass.
            aliases_by_key = defaultdict(set)
            hierarchical_keywords = [
                hierarchy for hierarchy in read_hierarchical_keywords(xmp_path)
                if not any(keyword_match_key(part) in pending_hierarchical_removals
                           for part in hierarchy.split('|'))
            ]
            validate_import_locations(db, photo_id, list(xmp_keywords_by_key.values()),
                                      hierarchical_keywords, additive=False)
            for hierarchy in hierarchical_keywords:
                parts = hierarchy.split('|')
                resolved = resolve_import_path(db, parts)
                if resolved is not None:
                    aliases_by_key[keyword_match_key(parts[-1])].add(resolved)
            resolved_ids = set()

            # A confirmed hierarchical alias is sufficient even when the
            # sidecar omits its flat dc:subject entry. Process both sources
            # so the removal pass cannot discard that linked location.
            for kw_key in xmp_keywords_by_key.keys() | aliases_by_key.keys():
                aliases = aliases_by_key.get(kw_key, set())
                if not aliases and kw_key in db_keywords_by_key:
                    continue
                imported_ids = sorted(aliases) if aliases else [
                    db.add_keyword(xmp_keywords_by_key[kw_key], _commit=False, _resolve_alias=True)
                ]
                for kid in imported_ids:
                    resolved_ids.add(kid)
                    # Reconciling from a sidecar cannot establish authorship.
                    db.tag_photo(
                        photo_id,
                        kid,
                        source=KEYWORD_SOURCE_UNKNOWN,
                        _commit=False,
                    )

            for kw in db_keywords:
                kw_key = keyword_match_key(kw["name"])
                preserve_hierarchy = (
                    kw["parent_id"] is not None
                    and kw_key in pending_flat_only_removals
                )
                if kw_key not in xmp_keywords_by_key and kw['id'] not in resolved_ids and not preserve_hierarchy:
                    db.untag_photo(photo_id, kw["id"], _commit=False)

            # Update xmp_mtime in the same transaction as reconciliation.
            xmp_mtime = os.path.getmtime(xmp_path)
            db.conn.execute(
                "UPDATE photos SET xmp_mtime = ? WHERE id = ?",
                (xmp_mtime, photo_id),
            )
            db.conn.commit()
        except Exception:
            db.conn.rollback()
            raise

        log.info(
            "Synced XMP -> DB for photo %d: %d keywords", photo_id, len(xmp_keywords)
        )
