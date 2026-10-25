"""Workspace folder membership: linking, unlinking and reading a workspace's folders.

A workspace sees the folders linked to it in ``workspace_folders``. Linking a
folder (``add_workspace_folder``) links its known descendants too, and every
read of a workspace's folders first materializes descendants discovered since
(``materialize_workspace_descendants``); unlinking records the removal so a
later scan does not quietly restore it (``remember_workspace_folder_removals``).
``add_folder`` inserts a folder row and links it to the active workspace.
``move_folders_to_workspace`` hands folders and their workspace-scoped data to
another workspace.

Every change to a workspace's folder set changes what the new-images walk
counts, so each one invalidates ``Database``'s process cache for the
workspaces it touched, through the façade's public invalidators and at the
same point relative to the commit as before. The no-commit link
(``add_workspace_folder_no_commit``) leaves both the commit and the
invalidation to its caller.

The SQL is ``WorkspaceFolderRepository``'s (``db.workspace_folders``);
``Database.get_folder_subtree_ids`` stays on the façade because
``PhotoRepository`` binds it by name.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Iterable

    from db import Database
    from services.service_types import JsonDict


def add_workspace_folder_no_commit(
        db: Database, workspace_id: int, folder_id: int, *, is_root: bool = True,
        restore_removed: bool = False) -> None:
    """Link a folder + descendants to a workspace WITHOUT committing.

    Same body as ``add_workspace_folder`` minus the ``commit()`` and cache
    invalidation. Intended for callers that already run inside a larger
    try/except+rollback transaction (e.g. ``merge_staged_tree_into_
    archive``) where a mid-body commit would break rollback safety by
    persisting a preceding UPDATE that an outer failure was meant to
    undo. The caller is responsible for committing (and for invalidating
    the workspace's new-images cache) after its own transaction closes.
    Internal merges preserve removed descendants unless the caller
    explicitly asks to restore them.
    """
    repo = db.workspace_folders
    folder_ids = db.folders.subtree_ids_by_path(folder_id)
    if not restore_removed:
        removed = repo.removed_ids(workspace_id)
        # The directly imported/scanned folder is intentional. Known
        # descendants need their own scan or explicit add to restore
        # them; registering a parent must not resurrect missing rows.
        folder_ids = [fid for fid in folder_ids if fid == folder_id or fid not in removed]
    repo.add_no_commit(
        workspace_id, folder_id, folder_ids,
        is_root=is_root, restore_removed=restore_removed)


def add_workspace_folder(
        db: Database, workspace_id: int, folder_id: int, *, is_root: bool = True,
        restore_removed: bool = True) -> None:
    """Link a folder and descendants, restoring explicit removals by default.

    Scanner registration passes ``restore_removed=False``: only the
    directly visited folder restores membership, not removed descendants.
    """
    add_workspace_folder_no_commit(
        db, workspace_id, folder_id, is_root=is_root, restore_removed=restore_removed)
    db.workspace_folders.commit()
    # The folder's untracked files now count toward this workspace's
    # new-images backlog. Drop any stale cached payload so the next read
    # recomputes against the updated folder set.
    db.invalidate_new_images_cache_for_workspace(workspace_id)


def add_workspace_folder_exact(
        db: Database, workspace_id: int, folder_id: int, *, is_root: bool = False) -> None:
    """Link exactly one folder to a workspace, without its descendants.

    Scanner-owned restricted scopes use this when the caller has already
    selected the exact folders being touched. The regular
    :func:`add_workspace_folder` deliberately materializes a whole known
    subtree for user-selected roots; using it here could attach unrelated
    catalog folders that happen to sit below the same filesystem parent.
    """
    db.workspace_folders.add_exact(
        workspace_id, folder_id, is_root=is_root)
    db.invalidate_new_images_cache_for_workspace(workspace_id)


def remember_workspace_folder_removals(
        db: Database, workspace_id: int, folder_ids: Iterable[int], *,
        recursive: bool = False) -> None:
    """Record removals in the caller's unlink/delete transaction."""
    repo = db.workspace_folders
    folder_ids = list(folder_ids)
    roots = repo.removal_root_ids(folder_ids) if recursive else set()
    repo.remember_removals(workspace_id, folder_ids, roots, recursive=recursive)


def remove_workspace_folder(db: Database, workspace_id: int, folder_id: int) -> None:
    """Unlink a single folder from a workspace."""
    remember_workspace_folder_removals(db, workspace_id, [folder_id])
    db.workspace_folders.remove(workspace_id, folder_id)
    # The folder no longer contributes to this workspace's new-images
    # backlog. Drop the cached payload so the banner reflects the change.
    db.invalidate_new_images_cache_for_workspace(workspace_id)


def remove_workspace_folder_tree(db: Database, workspace_id: int, folder_id: int) -> None:
    """Unlink a folder and its path descendants from a workspace."""
    folder_ids = db.folders.subtree_ids_by_path(folder_id)
    remember_workspace_folder_removals(db, workspace_id, folder_ids, recursive=True)
    db.workspace_folders.remove_tree(workspace_id, folder_ids)
    # The folder no longer contributes to this workspace's new-images
    # backlog. Drop the cached payload so the banner reflects the change.
    db.invalidate_new_images_cache_for_workspace(workspace_id)


def materialize_workspace_descendants(db: Database, workspace_id: int) -> None:
    """Ensure linked folders include all known path descendants.

    Also picks up descendants whose ``folders.path`` was rebased under
    ``local-folders/`` by staging: a pure ``folders.path`` walk from the
    ancestor root no longer reaches them, but their
    ``local_folder_mappings.source_path`` still records the original
    location and lets us bridge the gap.
    """
    repo = db.workspace_folders
    candidate_ids = repo.unlinked_descendant_ids(workspace_id)
    for root_path in repo.linked_paths(workspace_id):
        candidate_ids.update(db.folders.local_source_descendant_ids(root_path))
    if candidate_ids:
        candidate_ids -= repo.linked_ids(workspace_id)
    candidate_ids -= repo.removed_ids(workspace_id)
    if not candidate_ids:
        return
    repo.link_descendants(workspace_id, candidate_ids)
    db.invalidate_new_images_cache_for_workspace(workspace_id)


def get_workspace_folders(db: Database, workspace_id: int) -> list[sqlite3.Row]:
    """Return all explicit folder links for a workspace.

    Parent folders are recursive roots: if a linked folder has known
    descendants in ``folders``, keep those descendants linked internally so
    existing workspace-scoped photo queries continue to work.
    """
    materialize_workspace_descendants(db, workspace_id)
    return db.workspace_folders.list_folders(workspace_id)


def get_workspace_root_folder_ids(db: Database, workspace_id: int | None = None) -> list[int]:
    """Return just the ids of the workspace's user-facing roots.

    ``get_workspace_folder_roots`` computes a per-root subtree photo
    count with a correlated prefix scan over ``photos`` — sub-second on
    a small catalog, ~0.75s on an 88k-photo library. Pollers that only
    need to know *which* roots exist (the Work Locally blocker poll runs
    every 15s on Browse) must not pay for counts they discard.

    Defaults to the active workspace (raising ``RuntimeError`` when none
    is active, like every workspace-scoped accessor); an explicit
    ``workspace_id`` is accepted for parity with
    ``get_workspace_folder_roots``, whose callers already hold one.
    """
    if workspace_id is None:
        workspace_id = db.require_workspace_id()
    # Preserve the recovery side effect of get_workspace_folder_roots:
    # a descendant can be discovered after its recursive ancestor was
    # linked, and blocker polling must make that relationship visible to
    # workspace_local_root_ids even though this fast path skips photo
    # counts. This only writes when an unmaterialized descendant exists.
    materialize_workspace_descendants(db, workspace_id)
    return db.workspace_folders.root_ids(workspace_id)


def get_workspace_folder_roots(db: Database, workspace_id: int) -> list[sqlite3.Row]:
    """Return user-facing workspace roots, hiding covered descendants.

    Each row carries ``workspace_photo_count``: the number of photos this
    root contributes to the workspace, counting the whole subtree (the
    root plus all of its descendant folders that are linked to this
    workspace), not just photos sitting directly in the root. This matches
    what the user actually sees in the workspace — visibility is scoped by
    ``workspace_folders`` membership, so a descendant detached from the
    workspace is correctly excluded. The ``folders.photo_count`` column is
    a direct-only count and would read as a misleading "0 photos" for a
    root whose images all live in subfolders.

    Descendants whose ``folders.path`` has been rebased under
    ``local-folders/`` by staging are matched via
    ``local_folder_mappings.source_path`` too, otherwise the ancestor
    workspace's root would underreport its photos while
    ``workspace_folders`` still makes them visible.
    """
    materialize_workspace_descendants(db, workspace_id)
    return db.workspace_folders.roots(workspace_id)


def workspace_has_folder_link(db: Database, folder_id: int, workspace_id: int | None = None) -> bool:
    """True iff the workspace has a real or inherited folder link.

    Excludes ``workspace_photos`` grants, which only make a photo
    visible, not the whole folder. Folder-wide mutations such as
    ``/api/folders/<id>/relocate`` must gate on this view so a workspace
    holding only a photo-specific grant for one photo cannot rewrite
    paths for the hidden sibling photos owned by other workspaces.
    """
    if workspace_id is None:
        workspace_id = db.require_workspace_id()
    return db.workspace_folders.has_folder_link(
        workspace_id, folder_id,
    )


def get_audit_root_paths(db: Database, workspace_id: int | None = None) -> list[str]:
    """Paths of the active workspace's audit scan roots.

    Returns real ``workspace_folders`` roots only -- folders a workspace
    reaches solely through a ``workspace_photos`` grant are excluded,
    even when they would otherwise appear parentless in
    ``folders.tree``. The audit treats the result as storage roots to
    walk the filesystem under, and including a grant-only folder would
    let ``/api/audit/untracked`` surface hidden sibling files that
    ``/api/audit/import-untracked`` would then import under a real
    workspace link.
    """
    if workspace_id is None:
        workspace_id = db.require_workspace_id()
    return db.workspace_folders.audit_root_paths(workspace_id)


def get_workspace_extensions(db: Database) -> list[str]:
    """Return distinct lowercased file extensions for photos in the
    active workspace, sorted alphabetically.

    Used by the smart-collection rule editor to populate the Extension
    value dropdown — a free-text input silently failed when users typed
    ``JPG`` instead of ``.jpg`` or vice versa. Lowercasing here means the
    UI never has to think about case, and storing-side variations
    (``.jpg`` vs ``.JPG`` from older imports) collapse into one option.
    Empty/NULL extensions are skipped. A RAW+JPEG pair contributes both
    formats, since the extension rule matches a photo by either file.

    Folders whose status is not ``'ok'`` or ``'partial'`` are excluded so
    the dropdown stays consistent with ``_build_collection_query``, which
    joins on the same status filter. Otherwise an extension found only in
    a missing folder would appear as a selectable option but match zero
    photos when used in a rule — exactly the silent-failure mode this
    change is meant to prevent.
    """
    ws = db.require_workspace_id()
    return db.workspace_folders.extensions(ws)


def move_folders_to_workspace(
        db: Database, source_ws_id: int, target_ws_id: int, folder_ids: list[int]) -> JsonDict:
    """Move folders and their workspace-scoped data to another workspace.

    Moves: workspace_folders rows, pending_changes, prediction_review,
    photo_preferences, and species_highlights. Detections and predictions
    are global (no workspace_id), so they follow the folder via
    workspace_folders membership rather than being reassigned. Collections
    and edit_history stay behind.

    Returns:
        dict with keys: folders_moved, pending_changes_moved,
        photo_preferences_moved
    """
    from db import _path_for_subtree_match

    if not db.workspaces.get(source_ws_id):
        raise ValueError(f"Source workspace {source_ws_id} not found")
    if not db.workspaces.get(target_ws_id):
        raise ValueError(f"Target workspace {target_ws_id} not found")
    if source_ws_id == target_ws_id:
        raise ValueError("Source and target workspace are the same")

    source_folders = get_workspace_folders(db, source_ws_id)
    source_folder_ids = {f["id"] for f in source_folders}
    for fid in folder_ids:
        if fid not in source_folder_ids:
            raise ValueError(
                f"Folder {fid} does not belong to source workspace {source_ws_id}"
            )

    if not folder_ids:
        return {
            "folders_moved": 0,
            "pending_changes_moved": 0,
            "photo_preferences_moved": 0,
            "species_highlights_moved": 0,
        }

    selected_folder_ids = set(folder_ids)
    source_folder_paths = {
        folder["id"]: _path_for_subtree_match(folder["path"])
        for folder in source_folders
        if folder["path"]
    }
    remaining_source_paths = [
        path
        for fid, path in source_folder_paths.items()
        if fid not in selected_folder_ids
    ]
    for fid in selected_folder_ids:
        selected_path = source_folder_paths.get(fid)
        if selected_path and any(
            selected_path.startswith(path + "/") for path in remaining_source_paths
        ):
            raise ValueError(
                "Cannot move a folder that is covered by another source "
                "workspace folder; move the covering folder or remove it first"
            )

    moved_folder_ids = []
    seen_folder_ids = set()
    for fid in folder_ids:
        for subtree_id in db.folders.subtree_ids_by_path(fid):
            if subtree_id in source_folder_ids and subtree_id not in seen_folder_ids:
                seen_folder_ids.add(subtree_id)
                moved_folder_ids.append(subtree_id)

    (
        pending_changes_moved,
        photo_preferences_moved,
        species_highlights_moved,
    ) = db.workspace_folders.move_folders(
        source_ws_id, target_ws_id, folder_ids, moved_folder_ids,
    )

    # Folders changed membership for BOTH workspaces, so each workspace's
    # new-images backlog needs to be recomputed on the next read.
    db.invalidate_new_images_cache_for_workspaces([source_ws_id, target_ws_id])

    return {
        "folders_moved": len(folder_ids),
        "pending_changes_moved": pending_changes_moved,
        "photo_preferences_moved": photo_preferences_moved,
        "species_highlights_moved": species_highlights_moved,
    }


def add_folder(
        db: Database, path: str, name: str | None = None, parent_id: int | None = None, *,
        workspace_root: bool = True, link_to_workspace: bool = True) -> int:
    """Insert a folder. Automatically links it to the active workspace.

    ``workspace_root`` controls whether that automatic link is a
    user-facing workspace root. Scanner-discovered descendants pass
    ``False`` so recursive roots do not expand over time.

    ``link_to_workspace`` controls whether the auto-link happens at
    all. Restricted scans (a subfolder of an existing archive tree)
    pass ``False`` for the parent chain leading up to the restrict
    roots: those parents only need to exist for ``folders.parent_id``
    integrity, and letting ``add_workspace_folder`` fire would
    subtree-cascade every pre-existing descendant of the destination
    into the active workspace. See PR #1107 review.

    Returns the folder id.
    """
    folder_id = db.folders.add(path, name, parent_id)
    # Auto-link to active workspace
    workspace_id = db.active_workspace_id
    if link_to_workspace and workspace_id is not None:
        add_workspace_folder(
            db, workspace_id,
            folder_id,
            is_root=workspace_root,
            restore_removed=False,
        )
    return folder_id


def workspace_unlinked_folder_count(db: Database, folder_paths: Iterable[str]) -> int:
    """Count distinct paths in ``folder_paths`` whose folders are not
    linked to the active workspace.

    A folder path is "unlinked" when either no row exists in ``folders``
    for that path or a row exists but no ``workspace_folders`` entry
    connects it to the active workspace.

    Used by the import-mode pipeline plan to decide whether a scan over
    already-imported files would be a real no-op for the active
    workspace. ``scanner.scan`` calls ``_ensure_folder`` (which calls
    ``add_folder``) for each walked directory, and ``add_folder``
    auto-links the folder to the active workspace via
    ``workspace_folders``. So when the user re-imports files that were
    indexed in a different workspace, scan still mutates state by
    attaching folders to the active workspace — and the plan must
    report that as ``will-run`` instead of claiming ``done-prior``.
    """
    if not folder_paths:
        return 0
    ws = db.require_workspace_id()
    unique = list({p for p in folder_paths if p})
    if not unique:
        return 0
    return db.workspace_folders.unlinked_folder_count(ws, unique)
