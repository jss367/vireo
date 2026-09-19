"""Sync engine: reconcile database and XMP sidecars."""

import json
import logging
import os
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

from db import KEYWORD_SOURCE_UNKNOWN
from keyword_identity import (
    drop_stale_vireo_location_keywords,
    filter_removed_import_aliases,
    keyword_paths,
    path_key,
    resolve_import_path,
    resolve_merge_target,
    validate_import_locations,
)
from keyword_normalization import keyword_match_key
from xmp import SidecarEditor, read_hierarchical_keywords, read_keywords

log = logging.getLogger(__name__)

# Publishing a sidecar is network-latency bound -- temp-file create, fsync,
# ACL and xattr copy, rename -- not CPU bound, so writes to different files
# overlap almost perfectly. Eight keeps a NAS busy without flooding an SMB
# connection's request queue, and the GIL is released for every one of those
# syscalls.
_SYNC_MAX_WORKERS = 8

# Bound replay after interruption without a SQLite commit for every sidecar.
# Check elapsed time at completed group boundaries, so a stalled write never
# gets acknowledged. The row limit also bounds batches on fast local storage.
_SYNC_CHECKPOINT_SECONDS = 5.0
_SYNC_CHECKPOINT_CHANGES = 500


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

    Sync-only grants added by tracked-merge collision handling let a
    sibling workspace's remapped edit resolve its survivor's sidecar
    without gaining library membership on every other photo in the folder.
    Kept photo-scoped end-to-end: ``get_sync_only_photo_paths`` returns
    ``{photo_id: folder_path}``, and only the granted photo's row picks up
    that path -- an unrelated photo sitting in the same folder with its
    own inaccessible pending edit resolves to no path and fails
    "folder not accessible", exactly as it did before the grant. A
    caller-supplied ``folder_paths`` is trusted as-is and skips both
    layers -- callers who need the sync-only grants pass them in.
    """
    if folder_paths is not None:
        folders = folder_paths
        granted_paths = {}
    else:
        folders = {f["id"]: f["path"] for f in db.get_folder_tree()}
        # Photo-scoped, never unioned into ``folders``: widening the grant
        # back to folder scope would let an unrelated photo in the same
        # folder resolve to a valid sidecar path on the next sync.
        granted_paths = db.get_sync_only_photo_paths()
    paths = {}
    for photo_id, (folder_id, filename) in db.get_photo_filenames(photo_ids).items():
        base = os.path.splitext(filename)[0]
        folder_path = folders.get(folder_id) or granted_paths.get(photo_id, "")
        paths[photo_id] = os.path.join(folder_path, base + ".xmp")
    return paths


def _xmp_sync_setting_enabled(db, key):
    """Return whether the active workspace enables one XMP-write setting.

    A config file that cannot be read must not abort a sync: every one of
    these settings is off by default, and defaulting to "don't write" leaves
    the queue intact for the next run instead of writing something the user
    may not have asked for.
    """
    try:
        import config as cfg

        return bool(db.get_effective_config(cfg.load()).get(key, False))
    except Exception:
        log.warning("Failed to read %s config", key, exc_info=True)
        return False


def _xmp_sync_setting_state(db, key):
    """Return ``"on"``/``"off"``/``"unknown"`` for one XMP-write setting.

    ``"unknown"`` means the config read failed (parse error or IO error).
    Callers that gate destructive cleanup on the setting must not treat
    ``"unknown"`` as an explicit off: a location-keywords cleanup, for
    instance, would strip the previously-written marker and keywords and
    clear the pending row, so a later config fix would not requeue anything.
    Callers only gating writes can keep treating ``"unknown"`` as "don't
    write" (per ``_xmp_sync_setting_enabled`` above) -- that path leaves
    the queue alone.

    Uses ``config.load_strict`` rather than ``config.load``: the ordinary
    loader catches parse/IO exceptions and returns ``DEFAULTS``, so a
    corrupt config would silently return the default False for this key,
    which is exactly the destructive off/on we need to distinguish from a
    read failure.
    """
    try:
        import config as cfg

        val = bool(db.get_effective_config(cfg.load_strict()).get(key, False))
        return "on" if val else "off"
    except Exception:
        log.warning("Failed to read %s config", key, exc_info=True)
        return "unknown"


def _sync_flags_to_xmp_enabled(db):
    """Return whether the active workspace should write flags to XMP."""
    return _xmp_sync_setting_enabled(db, "sync_flags_to_xmp")


def _write_assigned_location_to_xmp_enabled(db):
    """Return whether the active workspace should write assigned GPS to XMP."""
    return _xmp_sync_setting_enabled(db, "write_assigned_location_to_xmp")


def _write_location_keywords_to_xmp_enabled(db):
    """Return whether the active workspace should write location keywords."""
    return _xmp_sync_setting_enabled(db, "write_location_keywords_to_xmp")


def _write_location_keywords_to_xmp_state(db):
    """Return ``"on"``/``"off"``/``"unknown"`` for the location-keywords setting.

    Cleanup is destructive (drops the marker and every entry it names), so
    we need to tell an explicit False from a transient config read failure.
    """
    return _xmp_sync_setting_state(db, "write_location_keywords_to_xmp")


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
    groups = [set(ids) for ids in kw_index.values()]
    for c in changes:
        if c['change_type'] == 'keyword_merge':
            merge = json.loads(c['value'])
            group = {c['id']}
            for path in (merge['source_path'], merge['target_path']):
                group.update(kw_index.get((c['photo_id'], keyword_match_key(path[-1])), []))
            groups.append(group)
    # Include transitive pairs when several merges are still waiting to sync.
    previous_count = -1
    while previous_count != len(selected_ids):
        previous_count = len(selected_ids)
        for group in groups:
            if group & selected_ids:
                selected_ids.update(group)
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
    hierarchy_replacements: dict = field(default_factory=dict)
    hierarchies_to_add: set = field(default_factory=set)
    keyword_merges: list = field(default_factory=list)
    rating: int | None = None
    flag: str | None = None
    edit_recipe_json: str | None = None
    sync_location: bool = False
    cleanup_location: bool = False
    # The keyword half of a ``location`` change, gated by its own setting:
    # GPS puts the photo on a map, keywords put the place name in Lightroom's
    # keyword list, and the user can want either without the other.
    sync_location_keywords: bool = False
    cleanup_location_keywords: bool = False
    # (change_id, change_token) per supported change. The clear runs after
    # the sidecar write, by which time a reused rowid can name a different
    # row -- see Database.clear_pending_by_token.
    supported_changes: list = field(default_factory=list)
    unsupported_changes: list = field(default_factory=list)


def _plan_photo_sync(photo_changes, sync_flags, sync_locations,
                     sync_location_keywords_state="off"):
    """Fold one photo's pending changes into a ``_PhotoSyncPlan``.

    ``sync_location_keywords_state`` is a tri-state:

    - ``"on"``      -- the queued ``location`` change writes keywords.
    - ``"off"``     -- the queued ``location`` change runs cleanup.
    - ``"unknown"`` -- config read failed; leave the ``location`` change
      unsupported so a future sync with a readable config can decide.
      Silently running cleanup here would strip the previously-written
      marker and keywords and clear the pending row, so a later config
      fix would not requeue anything.
    """
    plan = _PhotoSyncPlan()
    for c in photo_changes:
        kind = c["change_type"]
        if kind == "keyword_add":
            plan.keywords_to_add.add(c["value"])
            plan.keywords_to_remove.discard(c["value"])
            plan.keywords_to_remove_flat.discard(c["value"])
        elif kind == "keyword_remove":
            plan.keywords_to_remove.add(c["value"])
            plan.keywords_to_add.discard(c["value"])
        elif kind == "keyword_remove_flat":
            plan.keywords_to_remove_flat.add(c["value"])
            plan.keywords_to_add.discard(c["value"])
        elif kind == "keyword_merge":
            plan.keyword_merges.append(json.loads(c['value']))
        elif kind == "rating":
            plan.rating = int(c["value"])
        elif kind == "flag":
            if not sync_flags:
                plan.unsupported_changes.append(c)
                continue
            plan.flag = c["value"] or "none"
        elif kind == "location":
            if sync_location_keywords_state == "unknown":
                # A transient malformed config must not be interpreted as
                # an explicit off: cleanup would strip the previously-
                # written marker and keywords and clear the pending row,
                # stranding the sidecars until a manual backfill.
                plan.unsupported_changes.append(c)
                continue
            if sync_locations:
                plan.sync_location = True
            else:
                plan.cleanup_location = True
            if sync_location_keywords_state == "on":
                plan.sync_location_keywords = True
            else:
                plan.cleanup_location_keywords = True
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


def _write_photo_sync(xmp_path, plan, assigned_location=None, location_path=None,
                      create_missing_sidecars=False):
    """Apply a ``_PhotoSyncPlan`` to the photo's sidecar, in dependency order.

    Every mutation lands in one ``SidecarEditor``, so the sidecar is parsed
    once and republished once no matter how many change types the photo
    queued. The ordering below still matters: it decides what the single
    published tree contains.

    ``assigned_location`` and ``location_path`` are passed in rather than
    looked up here because the writers run on a pool thread and the SQLite
    connection belongs to the caller's thread.

    ``create_missing_sidecars`` only affects a rating-only photo, the one
    mutation that otherwise declines to create a sidecar; see
    ``SidecarEditor.set_rating``.
    """
    editor = SidecarEditor(xmp_path)
    if plan.sync_location_keywords and plan.keyword_merges:
        # Remove the old marker-owned entries before merge rewrites create
        # the new hierarchy. Otherwise that hierarchy appears pre-existing
        # to set_location_keywords and loses its cleanup ownership.
        editor.remove_vireo_location_keywords()
    if plan.hierarchy_replacements:
        editor.replace_keyword_hierarchies(plan.hierarchy_replacements)
    _remove_planned_keywords(editor, plan)

    # Apply keyword additions after removals so a same-photo remove+add
    # pair does not cancel out (see _remove_planned_keywords).
    if plan.keywords_to_add:
        # An ordinary ``keyword_add`` for the same leaf a prior location
        # write authored (say, the user adds "Paris" while the photo's
        # location has always been "France|Paris") would land on an entry
        # already in ``dc:subject`` -- ``add_keywords`` is a no-op there --
        # and a later ``remove_vireo_location_keywords`` or a place-change
        # in this same sync would then strip the leaf under the marker's
        # flat ownership claim, leaving the user's keyword absent. Transfer
        # ownership away from the location marker BEFORE the add, so the
        # subsequent cleanup respects the transfer and leaves the flat
        # entry alone. Hierarchical ownership is untouched because
        # ``keyword_add`` writes ``dc:subject`` only.
        editor.release_location_flat_ownership_for(plan.keywords_to_add)
        editor.add_keywords(
            flat_keywords=plan.keywords_to_add, hierarchical_keywords=plan.hierarchies_to_add
        )

    # Apply the flag before the rating: a flag creates a sidecar if needed,
    # while a rating intentionally only updates existing ones.
    if plan.flag is not None:
        editor.set_pick_flag(plan.flag)

    # Location keywords go in after the queued keyword adds and removes so
    # that a hierarchical remove sharing a segment with the place chain (say,
    # a removed keyword named after the town) cannot strip the hierarchy this
    # write is putting back.
    if plan.sync_location_keywords:
        # An empty chain is the "no location any more" case, and
        # set_location_keywords() routes it to the same removal the disabled
        # setting takes.
        editor.set_location_keywords(location_path or ())
    elif plan.cleanup_location_keywords:
        editor.remove_vireo_location_keywords()

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


def _plan_merged_keyword_hierarchies(db, plans):
    """Apply durable merge work only to its recorded photo and keyword identity."""
    if not any(plan.keyword_merges for plan in plans.values()):
        return
    rows = db.conn.execute('SELECT id, name, parent_id FROM keywords').fetchall()
    paths = keyword_paths(rows)
    location_leaves = db.get_photo_location_keyword_ids([
        pid for pid, plan in plans.items() if plan.sync_location_keywords
    ])
    for photo_id, plan in plans.items():
        if not plan.keyword_merges:
            continue
        tagged = db.get_photo_keywords(photo_id)
        tagged_ids = {k['id'] for k in tagged}
        tagged_names = {keyword_match_key(k['name']) for k in tagged}
        tagged_paths = {path_key(paths[k['id']]) for k in tagged}
        for merge in plan.keyword_merges:
            source_path = merge['source_path']
            target_id = resolve_merge_target(db, merge)
            target_path = paths.get(target_id)
            old_hierarchy = '|'.join(source_path)
            if target_id in tagged_ids:
                plan.hierarchy_replacements[old_hierarchy] = '|'.join(target_path)
                if target_id != location_leaves.get(photo_id):
                    plan.keywords_to_add.add(target_path[-1])
                    if len(target_path) > 1:
                        plan.hierarchies_to_add.add('|'.join(target_path))
            else:
                if path_key(source_path) not in tagged_paths:
                    plan.hierarchy_replacements[old_hierarchy] = None
                if target_path and keyword_match_key(target_path[-1]) not in tagged_names:
                    plan.keywords_to_add.discard(target_path[-1])
                    plan.keywords_to_remove_flat.add(target_path[-1])
                if target_path and path_key(target_path) not in tagged_paths:
                    plan.hierarchy_replacements['|'.join(target_path)] = None
                # Keep an unrelated same-named tag when its own hierarchy is
                # still assigned to this photo.
                if target_path and keyword_match_key(target_path[-1]) in tagged_names:
                    plan.keywords_to_remove.discard(target_path[-1])
            if keyword_match_key(source_path[-1]) not in tagged_names:
                plan.keywords_to_remove_flat.add(source_path[-1])


def sync_to_xmp(db, progress_callback=None, change_ids=None, create_missing_sidecars=False,
                folder_paths=None, require_workspace_membership=True, status_callback=None):
    """Write pending changes to XMP sidecars.

    Args:
        db: Database instance
        progress_callback: optional callable(current, total)
        status_callback: optional callable(dict) with processed (current), total,
            synced and failed photo counts and a monotonic checkpoint count.
            Called on the caller's thread; a checkpoint means successful queue
            cleanup committed. A photo with supported and unsupported changes
            can count as both synced and failed.
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
    # Capture and claim in one short writer transaction. Cancellation on
    # another connection must either win before the snapshot or leave an
    # opposing intent behind it. Claims survive an interrupted write.
    with db.conn:
        db.conn.execute("UPDATE pending_changes SET id = id WHERE 0")
        changes = db.get_pending_changes()
        if change_ids is not None:
            changes = _select_changes(changes, change_ids)
        db.conn.executemany(
            "INSERT OR IGNORE INTO pending_change_sync_attempts(change_id) VALUES (?)",
            [(c["id"],) for c in changes
             if c["change_type"] in ("keyword_add", "keyword_remove")],
        )
    if not changes:
        return _sync_result(0, [])

    by_photo = defaultdict(list)
    for c in changes:
        by_photo[c["photo_id"]].append(c)

    sync_flags = _sync_flags_to_xmp_enabled(db)
    sync_locations = _write_assigned_location_to_xmp_enabled(db)
    # Tri-state so a config read failure ("unknown") is not treated as an
    # explicit off -- cleanup would otherwise strip the marker on every
    # queued location change and clear the pending row, stranding the
    # sidecars until manual backfill. See _plan_photo_sync.
    sync_location_keywords_state = _write_location_keywords_to_xmp_state(db)
    sync_location_keywords = sync_location_keywords_state == "on"

    # Everything that needs the database happens here, on the caller's
    # thread: the sidecar writers below run on a pool and must not touch the
    # connection.
    xmp_paths = _resolve_xmp_paths(db, list(by_photo), folder_paths=folder_paths)
    prepare_failures = {}
    plans = {}
    folder_accessible = {}
    for photo_id, photo_changes in by_photo.items():
        xmp_path = xmp_paths.get(photo_id)
        if not xmp_path:
            prepare_failures[photo_id] = {
                "photo_id": photo_id, "error": "photo not found in DB",
            }
            continue

        # Check if the folder exists (NAS might be offline). Cache the answer
        # per folder: on a slow or offline mount this is a network round trip,
        # and a folder holds thousands of photos.
        folder = os.path.dirname(xmp_path)
        if folder not in folder_accessible:
            folder_accessible[folder] = os.path.isdir(folder)
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
                sync_location_keywords_state,
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

    _plan_merged_keyword_hierarchies(db, plans)

    # Names, unlike coordinates, come out of one batched query: a shoot
    # shares a place, and a 38,000-photo backfill cannot afford a chain walk
    # per photo. No workspace check here -- ``get_pending_changes`` is already
    # workspace-scoped, and the sidecar path these names are written to was
    # resolved under the same membership and sync-only rules above.
    location_paths = {}
    if sync_location_keywords:
        keyword_photo_ids = [
            photo_id for photo_id, plan in plans.items()
            if plan.sync_location_keywords
        ]
        location_paths = db.get_photo_location_paths(keyword_photo_ids)

    locations = {}
    if sync_locations:
        for photo_id, plan in list(plans.items()):
            if not plan.sync_location:
                continue
            try:
                locations[photo_id] = db.get_assigned_photo_location(
                    photo_id,
                    verify_workspace=require_workspace_membership,
                    # A sync-only grant authorizes writing this sidecar;
                    # the path map above already honors it, and the
                    # membership test here would otherwise refuse the same
                    # photo and leave the edit queued forever.
                    allow_sync_only=True,
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

    # Resolve the actual sidecar, including a sidecar that is itself a
    # symlink. Do this in parallel, just as publishing does, rather than
    # adding serial network round trips to preparation. Case folding only
    # coalesces scheduling; each write still uses its own original path.
    def sidecar_key(path):
        try:
            return os.path.normcase(os.path.realpath(path)), None
        except (OSError, ValueError) as error:
            return None, error

    canonical_keys = {}
    if plans:
        paths = list(dict.fromkeys(xmp_paths[pid] for pid in plans))
        with ThreadPoolExecutor(max_workers=min(_SYNC_MAX_WORKERS, len(paths))) as pool:
            resolved = dict(zip(paths, pool.map(sidecar_key, paths), strict=True))
        for pid in list(plans):
            key, error = resolved[xmp_paths[pid]]
            if error is not None:
                prepare_failures[pid] = {
                    "photo_id": pid, "error": str(error), "reason": _failure_reason(error),
                }
                del plans[pid]
            else:
                canonical_keys[xmp_paths[pid]] = key

    by_sidecar = defaultdict(list)
    for change in changes:
        pid = change["photo_id"]
        if pid not in plans:
            continue
        key = canonical_keys[xmp_paths[pid]].casefold()
        runs = by_sidecar[key]
        if not runs or runs[-1][0] != pid:
            runs.append((pid, []))
        runs[-1][1].append(change)

    # Keep contiguous per-photo runs in queue order. Folding the entire
    # photo first turns RAW=1, JPEG=2, RAW=3 into RAW=3, JPEG=2.
    sidecar_plans = {}
    for key, runs in by_sidecar.items():
        photo_ids = {pid for pid, _ in runs}
        ordered = []
        for pid, run_changes in runs:
            if len(runs) == len(photo_ids):
                plan = plans[pid]
            else:
                plan = _plan_photo_sync(run_changes, sync_flags, sync_locations,
                                        sync_location_keywords_state)
                _plan_merged_keyword_hierarchies(db, {pid: plan})
            ordered.append((pid, xmp_paths[pid], plan))
        sidecar_plans[key] = ordered

    def write_sidecar_group(canonical_key):
        """Write every photo queued against one sidecar; never raises."""
        ordered = sidecar_plans[canonical_key]
        outcomes = dict.fromkeys(pid for pid, _, _ in ordered)
        failed_paths = {}
        for photo_id, xmp_path, plan in ordered:
            path = canonical_keys[xmp_path]
            if path in failed_paths:
                continue
            try:
                _write_photo_sync(
                    xmp_path, plan, locations.get(photo_id),
                    location_paths.get(photo_id),
                    create_missing_sidecars=create_missing_sidecars,
                )
            except Exception as e:
                failed_paths[path] = e
        # A folded scheduling group can contain independent files. Retry
        # the whole sequence only for actual aliases of a failed sidecar.
        # Check after writing too: a successful write may have created a
        # previously missing case alias on a case-insensitive volume.
        for failed_path, error in failed_paths.items():
            for photo_id, xmp_path, _ in ordered:
                path = canonical_keys[xmp_path]
                aliases = path == failed_path
                if not aliases:
                    try:
                        aliases = os.path.samefile(path, failed_path)
                    except (OSError, ValueError):
                        aliases = False
                if aliases:
                    outcomes[photo_id] = error
        return outcomes

    results = {}
    total = len(by_photo)
    completed = len(prepare_failures)
    synced = 0
    failed = len(prepare_failures)
    synced_tokens = []
    synced_legacy_ids = []
    checkpoint = 0
    last_checkpoint = time.monotonic()

    def flush_completed():
        nonlocal checkpoint, last_checkpoint
        if not synced_tokens and not synced_legacy_ids:
            return
        # Immutable tokens protect edits replaced while the write was in flight.
        if synced_tokens:
            db.clear_pending_by_token(
                synced_tokens, clear_equivalent_flat_removals=True,
            )
            synced_tokens.clear()
        if synced_legacy_ids:
            # A legacy rowid may now name a newly queued, tokened edit.
            db.clear_pending(
                synced_legacy_ids, expected_tokens=[None] * len(synced_legacy_ids),
                clear_equivalent_flat_removals=True,
            )
            synced_legacy_ids.clear()
        checkpoint += 1
        last_checkpoint = time.monotonic()

    def report_progress():
        if status_callback:
            status_callback({
                "current": completed, "total": total, "synced": synced,
                "failed": failed, "checkpoint": checkpoint,
            })
        if progress_callback:
            progress_callback(completed, total)

    report_progress()
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
                for photo_id, error in outcomes.items():
                    plan = plans[photo_id]
                    if error is not None:
                        failed += 1
                        continue
                    if plan.unsupported_changes:
                        failed += 1
                    if plan.supported_changes:
                        synced += 1
                        for change_id, token in plan.supported_changes:
                            if token:
                                synced_tokens.append(token)
                            else:
                                synced_legacy_ids.append(change_id)
                # Never checkpoint a partially completed sidecar group.
                if (len(synced_tokens) + len(synced_legacy_ids) >= _SYNC_CHECKPOINT_CHANGES
                        or time.monotonic() - last_checkpoint >= _SYNC_CHECKPOINT_SECONDS):
                    flush_completed()
                report_progress()

    flush_completed()
    report_progress()

    # Report failures in queue order regardless of worker completion order.
    failures = []
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
        for c in plan.unsupported_changes:
            failures.append({
                "photo_id": photo_id,
                "change_id": c["id"],
                "error": f"unsupported change type: {c['change_type']}",
            })

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
            sidecar_hierarchies = read_hierarchical_keywords(xmp_path)
            # Vireo-written location keywords for a place the user has since
            # changed describe the sidecar's past, not the photo's present.
            xmp_keywords, sidecar_hierarchies = drop_stale_vireo_location_keywords(
                db, photo_id, xmp_path, xmp_keywords, sidecar_hierarchies,
            )
            pending_removals = db.get_pending_keyword_removal_keys(photo_id)
            pending_hierarchical_removals = db.get_pending_keyword_removal_keys(
                photo_id, hierarchical=True,
            )
            # Chained, not re-read: the alias filter runs on the hierarchy
            # list the stale-location filter already pruned, so a queued
            # location change still suppresses the sidecar's old place.
            xmp_keywords, imported_hierarchies = filter_removed_import_aliases(
                db, photo_id, xmp_keywords, sidecar_hierarchies,
                pending_removals, pending_hierarchical_removals,
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
                hierarchy for hierarchy in imported_hierarchies
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
                # A location is assigned in Vireo and only copied outward, and
                # whether it appears in the sidecar at all depends on a
                # setting. Pruning it here would let a reconcile silently
                # unassign the place -- including every place assigned while
                # location keyword writes were off.
                if kw["type"] == "location":
                    continue
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
