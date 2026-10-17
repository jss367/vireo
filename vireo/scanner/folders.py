"""The folder rows one ``scan()`` creates, links and stamps.

``_ScanFolders`` owns the folder cache, the workspace-root rules for a
restricted scan, and the scan scope the ``'partial'`` / ``'ok'`` status
stamps apply to.
"""

import os
from pathlib import Path

from image_loader import is_excluded_scan_path


class _ScanFolders:
    """Folder rows for the scan: create, link to the workspace, stamp status."""

    def __init__(
        self, db, root_path, *, restrict_dirs, effective_restrict_dirs,
        register_restrict_dirs_as_roots, catalog_folder_path, folder_aliases,
    ):
        self.db = db
        self.root_path = root_path
        self.restrict_dirs = restrict_dirs
        self.register_restrict_dirs_as_roots = register_restrict_dirs_as_roots
        self.catalog_folder_path = catalog_folder_path

        # Build folder cache: path -> folder_id
        self.folder_cache = {}
        self.folder_aliases = folder_aliases

        # When the scan is restricted to specific subfolders, those subfolders —
        # not the broad scan root — are the user-facing workspace roots. A
        # templated copy-import lands files in ``<destination>/<template>/...``
        # dirs and passes those leaf dirs as ``restrict_dirs`` while keeping the
        # destination base as ``root`` only for parent-chain creation. Promoting
        # the base to a workspace root would make the new-images walk treat every
        # un-imported sibling under it as "new" (e.g. a whole archive of past
        # shoots sharing the destination). Mark the restricted dirs as roots
        # instead; the base stays linked but is_root=0. See
        # ``new_images.mapped_roots``. ``effective_restrict_dirs`` (bundle-filtered)
        # is the set actually enumerated, so root marking matches what was scanned.
        self._effective_restrict_paths = None
        self._restrict_root_paths = None
        if restrict_dirs is not None:
            self._effective_restrict_paths = {
                os.path.normpath(str(d)) for d in effective_restrict_dirs
            }
            self._restrict_root_paths = (
                self._effective_restrict_paths
                if register_restrict_dirs_as_roots else set()
            )

        # Track folders whose scan touched them (so we can flag them 'partial'
        # if anything between the pre-pass and scan completion dies midway) and
        # the outer scan scope as a fallback. The scope matters when
        # ``touched_folder_ids`` is empty — e.g. a pre-pass XMP commit that
        # aborts before the main loop has added any folder, or a successful
        # no-op incremental scan that processes zero files.
        self.touched_folder_ids = set()
        self.scoped_paths = {str(root_path)}
        if restrict_dirs is not None:
            self.scoped_paths.update(str(d) for d in restrict_dirs)

    def ensure_folder(self, folder_path):
        """Ensure a folder and all its parents exist in the DB. Returns folder_id."""
        db = self.db
        root_path = self.root_path
        folder_str = str(folder_path)
        if folder_str in self.folder_cache:
            return self.folder_cache[folder_str]

        parent_id = None
        if folder_path != root_path:
            parent_id = self.ensure_folder(folder_path.parent)

        if self._restrict_root_paths is not None:
            is_ws_root = os.path.normpath(folder_str) in self._restrict_root_paths
        else:
            is_ws_root = (folder_path == root_path)

        # In restricted mode, the scan root (``root_path``) and every
        # intermediate ancestor between it and ``restrict_dirs`` exist
        # only to satisfy the ``folders.parent_id`` chain — they are not
        # part of what the user asked to import. Linking them to the
        # workspace would fire ``_add_workspace_folder_no_commit`` and
        # its path-prefix subtree cascade, pulling every pre-existing
        # cataloged descendant of ``root_path`` (unrelated archive
        # subtrees from prior scans / other workspaces) into the active
        # workspace. Only the ``_restrict_root_paths`` themselves should
        # link. See PR #1107 review (line 1186).
        normalized_folder = os.path.normpath(folder_str)
        is_restrict_target = (
            self._effective_restrict_paths is not None
            and normalized_folder in self._effective_restrict_paths
        )
        link_to_ws = (
            self._effective_restrict_paths is None
            or (self.register_restrict_dirs_as_roots and is_restrict_target)
        )

        folder_id = db.add_folder(
            path=self.catalog_folder_path(
                db, folder_str, aliases=self.folder_aliases,
            ),
            name=folder_path.name,
            parent_id=parent_id,
            workspace_root=is_ws_root,
            link_to_workspace=link_to_ws,
        )
        if (
            is_restrict_target
            and not self.register_restrict_dirs_as_roots
            and db.active_workspace_id is not None
        ):
            # Snapshot imports and metadata repair select exact leaf folders
            # below an existing workspace root. Link only that leaf: the
            # regular subtree-linking API would also attach unrelated known
            # descendants that this restricted scan never touched.
            db.add_workspace_folder_exact(
                db.active_workspace_id, folder_id, is_root=False,
            )
        self.folder_cache[folder_str] = folder_id
        return folder_id

    def touch_folder(self, folder_path):
        """``ensure_folder``, recording it as reached by the per-file loop."""
        folder_id = self.ensure_folder(folder_path)
        self.touched_folder_ids.add(folder_id)
        return folder_id

    def update_folder_status(self, new_status, only_from_partial):
        """Stamp folders in the scan scope with ``new_status``.

        Applies to every folder matched by ``scoped_paths`` (outer roots)
        OR ``touched_folder_ids`` (folder rows the main loop has reached).
        When ``only_from_partial`` is True, restricts the UPDATE to rows
        already in ``'partial'`` — used on the success path so completed
        scans don't clobber ``'missing'`` or future statuses.
        """
        db = self.db
        scoped_paths = self.scoped_paths
        touched_folder_ids = self.touched_folder_ids
        db.scanning.set_folder_status(
            new_status, paths=scoped_paths, folder_ids=touched_folder_ids,
            only_from_partial=only_from_partial,
        )
        db.commit_with_retry()

    def register_scan_targets(self):
        # Eagerly register the explicit scan targets so they end up linked
        # to the active workspace even when zero photos are inserted (e.g.
        # every file is in skip_paths because the photos are already in
        # the global photos table). Without this, importing a folder
        # whose contents are already known silently skipped the
        # workspace_folders link and the folder never became visible in
        # the active workspace. Inside the try so a DB failure here still
        # routes through the partial-status recovery path.
        self.ensure_folder(self.root_path)
        if self.restrict_dirs is not None:
            for d in self.restrict_dirs:
                dp = Path(d)
                # Same bundle guard as the discovery loop — never
                # register or stat a path inside an other-app data bundle,
                # even when the caller stuffed one into ``restrict_dirs``.
                if is_excluded_scan_path(dp):
                    continue
                if dp.is_dir():
                    self.ensure_folder(dp)
