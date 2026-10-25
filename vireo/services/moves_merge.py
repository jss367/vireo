"""Folding a staged tree into its archive, and the other moves-domain reads.

``merge_staged_tree_into_archive`` reconciles the catalog after a staged
folder subtree has been copied into a tracked archive: it composes
``MovesMergeRepository.merge_staged_tree_into_archive`` with the workspace
folder links, the location-keyword transfer and the new-images cache.
``prune_ws_nonroot_links_outside_roots`` is the scoped-merge link prune it
hands the repository. ``get_sync_only_photo_paths`` resolves the active
workspace's sync-only grants for the sync engine, and
``query_move_rule_matches`` reads the photos a move rule selects under the
workspace-effective detector floor.

``Database._move_location_state_for_merge`` and
``_carry_keyword_associations_for_merge`` stay on ``Database`` with the
keyword workflows: the moves repository factory binds the second by name, and
both re-tag through ``Database.tag_photo``, the keyword-provenance contract's
writer.
"""

from __future__ import annotations

from functools import partial
from typing import TYPE_CHECKING, Any

import config as cfg
from db import KEYWORD_SOURCE_CONFLICT_SQL

if TYPE_CHECKING:
    from collections.abc import Mapping

    from db import Database
    from services.service_types import JsonDict


def prune_ws_nonroot_links_outside_roots(db: Database, workspace_id: int, path: str) -> None:
    """Drop non-root links that could re-materialize ``path``'s subtree.

    Prunes uncovered non-root links that are ``path`` itself, strict
    descendants of ``path``, OR strict ancestors of ``path``. Ancestors
    matter because ``_materialize_workspace_descendants`` walks the whole
    subtree below every linked folder — a surviving non-root ancestor
    like ``/archive`` (left over from a restricted scan; see
    ``scanner.py`` ``_restrict_root_paths``) would immediately re-insert
    ``/archive/USA`` and any sibling like ``/archive/USA/2027`` after
    the caller pruned them, defeating a scoped merge into a workspace
    rooted at ``/archive/USA/2026``.
    """
    prune_ids = db.workspace_folders.prune_nonroot_links_outside_roots(
        workspace_id, path)
    if prune_ids:
        db.invalidate_new_images_cache_for_workspace(workspace_id)


def get_sync_only_photo_paths(db: Database, workspace_id: int | None = None) -> dict[int, str]:
    """Return ``{photo_id: folders.path}`` for the workspace's sync-only grants.

    ``_resolve_xmp_paths`` uses this map to resolve one granted photo's
    sidecar at a time, keyed by the photo id -- never by the folder id.
    A folder-keyed union with the workspace's folder tree would widen
    the grant back to folder scope: an unrelated photo sitting in the
    same folder with its own inaccessible pending edit would get its
    sidecar written on the next sync, silently, without the workspace
    gaining a grant of its own. Absent from every browse/library query,
    which still joins on ``workspace_folders`` alone.
    """
    if workspace_id is None:
        workspace_id = db.require_workspace_id()
    return db.moves_merge.sync_only_photo_paths(workspace_id)


def merge_staged_tree_into_archive(db: Database, staged_root_id: int, archive_path: str) -> JsonDict:
    """Fold a staged folder subtree into an existing tracked archive.

    The on-disk rsync merge has already happened: files that were under the
    staged root now also live under ``archive_path``. This reconciles the
    catalog so staged folder/photo rows become rows under the existing
    archive, with no duplicate ``folders.path`` and correct ``parent_id``.

    For each staged folder (root-first), the target path is the staged path
    rebased from the staged root onto ``archive_path``:

    * Target has no folder row -> repoint the staged row to the target path,
      fix its ``parent_id`` to the (now-existing) target-parent folder, and
      link it to the active workspace as a non-root (an existing archive
      root ancestor already covers it). Every staged photo in that folder is
      a newly-archived photo.
    * Target already has a folder row -> move the staged folder's photos
      into it (dropping any whose filename already exists there as an
      identical archived file), then delete the now-empty staged folder row.
      Each moved (not dropped) photo is a newly-archived photo.

    Returns a counts dict (all defined as the user-facing summary reads
    them):

    * ``new_photos`` — total staged photos newly placed into the archive,
      counting BOTH photos reparented into brand-new folders AND photos
      moved into a pre-existing target folder. This is the headline number.
    * ``new_folders`` — folders created under the archive (the staged folder
      had no pre-existing target row).
    * ``merged_folders`` — staged folders folded into a pre-existing target
      folder.
    * ``already_present`` — identical-filename staged photos dropped because
      the target folder already held that filename AND the target row's
      recorded bytes-identity (``file_hash``, falling back to ``file_size``)
      matches what's currently on disk. A filename collision whose target
      row is stale (its recorded bytes-identity doesn't match the on-disk
      file that rsync just copied into place) is treated as a phantom
      replacement instead — see the collision loop below.
    * ``dropped_photo_ids`` — staged photo ids that were deleted during
      the merge (``already_present`` collisions plus phantom target-row
      replacements). The caller passes these to
      ``cleanup_cached_files_for_deleted_photos`` so orphaned thumbnail /
      preview / working-copy files can't be inherited by a later import
      that reuses one of the freed SQLite rowids.
    * ``preserved_edit_count`` — pending edits that were queued against
      a photo the collision loop is about to delete (either the staged
      row on a byte-identical collision, or the phantom target row on a
      replacement) and were reparented onto the surviving row before
      ON DELETE CASCADE on ``pending_changes.photo_id`` could drop them.
      Reported up so a NAS transfer's residual check can add them to
      the "still need a sync" count rather than silently losing them.
    * ``preserved_off_staging_identities`` — active-workspace subset of
      the above whose survivor is NOT one of the staged photo ids the
      caller captured before the merge. Reported as a list of
      ``pending_changes.staged_scope`` identity keys (``change_token`` or
      ``("id", id)``) so the caller can filter out edits its pre-transfer
      drain already classified as undeliverable (a flag under
      ``sync_flags_to_xmp`` off) and NOT count them as "queued during
      transfer" — they existed before, were considered, and were
      deliberately not written to XMP. Sibling-workspace edits are
      omitted for the same reason: this sync would not have written
      them either. The remaining in-staging remaps (phantom-target and
      intra-staged) are already found by the by-photo residual re-read
      scoped to the captured staged ids, so adding them here as well
      would report one edit as two.

    Every queued change type has catalog state written at edit time
    that the queue row does not carry, and all of it has to travel with
    the remap or the survivor ends up disagreeing with the sidecar the
    next sync writes:

    * ``location`` -> the photo's ``type='location'`` keyword links
      (``_move_location_state_for_merge``)
    * ``keyword_add`` / ``keyword_remove`` -> the ``photo_keywords``
      association (``moves_merge.reconcile_conflicting_keyword_edits``);
      ``keyword_remove_flat`` is sidecar-only and has none
    * ``rating`` / ``flag`` -> the ``photos`` columns
      (``moves_merge.transfer_review_state``)
    * ``edit_recipe`` -> the ``photo_edit_recipes`` row
      (``moves_merge.transfer_edit_recipe``)

    Each resolves a competing change on the two rows by queue
    chronology, ``(created_at, id)``, and drops the older row so
    ``_plan_photo_sync`` is not left picking between two rows of the
    same type at random. Competition is judged across every workspace:
    the catalog state and the sidecar are both global, so two opposing
    intents on one photo cannot both be honored no matter who queued
    them.

    Side effect worth knowing about: when a remapped pending edit is
    owned by a workspace other than the active one, the survivor's
    photo is recorded in ``workspace_sync_only_photos`` for that
    workspace so the sync engine can resolve the sidecar path.
    ``workspace_folders`` is not touched -- a real library link would
    make every other photo in the survivor's folder visible in the
    sibling workspace and persist past the sync. See
    ``moves_merge.link_survivor_for_sibling_edits``.
    """
    def case_insensitive_root(path: str) -> str | None:
        # Late-bound so a patched ``move._case_insensitive_root`` applies,
        # and imported at the point of use as before.
        from move import _case_insensitive_root
        return _case_insensitive_root(path)

    return db.moves_merge.merge_staged_tree_into_archive(
        staged_root_id, archive_path,
        workspace_id_fn=db.require_workspace_id,
        root_ancestor_exists=db.workspace_folders.root_ancestor_exists,
        root_descendant_exists=db.workspace_folders.root_descendant_exists,
        prune_nonroot_links_outside_roots=partial(prune_ws_nonroot_links_outside_roots, db),
        materialize_workspace_descendants=db._materialize_workspace_descendants,
        add_workspace_folder=db.add_workspace_folder,
        add_workspace_folder_no_commit=db._add_workspace_folder_no_commit,
        case_insensitive_root=case_insensitive_root,
        move_location_state=db._move_location_state_for_merge,
        invalidate_new_images=db.invalidate_new_images_cache_for_workspaces,
        update_folder_counts=db.folders.update_counts,
        KEYWORD_SOURCE_CONFLICT_SQL=KEYWORD_SOURCE_CONFLICT_SQL,
    )


def query_move_rule_matches(db: Database, criteria: Mapping[str, Any]) -> list[int]:
    """Return photo IDs matching move rule criteria.

    Criteria keys (all optional, AND logic):
      rating_min, flag, species, folder_ids,
      has_predictions, imported_before
    """
    workspace_id = db.require_workspace_id()
    move_min_conf = None
    if "has_predictions" in criteria:
        # Apply the workspace-effective detector_confidence floor so the
        # rule matches what the UI actually shows: a photo whose only
        # predictions sit on below-threshold detections must NOT count
        # as "has predictions".
        move_min_conf = db.get_effective_config(cfg.load()).get(
            "detector_confidence", 0.2
        )
    return db.moves_merge.query_rule_matches(criteria, workspace_id, move_min_conf)
