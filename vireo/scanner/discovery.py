"""Discovery: the files one ``scan()`` will consider.

``_Discovery`` walks the root (recursively or flat), the caller's
``restrict_dirs``, or filters a frozen manifest, and records which
``restrict_dirs`` survived the bundle guard so the working-copy pass
scopes itself to what was actually visited.
"""

import errno
import logging
import os
from pathlib import Path

from image_loader import (
    SUPPORTED_EXTENSIONS,
    is_excluded_scan_path,
    safe_iter_dir,
    safe_scan_walk,
)

log = logging.getLogger(__name__)


class _Discovery:
    """Enumerates the scan's image files; owns what the walk found."""

    def __init__(
        self, root, root_path, frozen_files, *, signals,
        restrict_dirs, restrict_files, skip_paths, recursive,
        permission_error_callback,
    ):
        self.root = root
        self.root_path = root_path
        self.frozen_files = frozen_files
        self.signals = signals
        self.restrict_dirs = restrict_dirs
        self.restrict_files = restrict_files
        self.skip_paths = skip_paths
        self.recursive = recursive
        self.permission_error_callback = permission_error_callback
        self.image_files = []
        self.excluded_frozen = 0
        self.effective_restrict_dirs = []

    def discover_files(self):
        # Discover all image files (incremental enumeration for progress reporting)
        # unless the caller already froze an all-source manifest. Copy the input:
        # scan sorts its work queue and callers may retain their source mapping.
        log.info("Discovering files in %s ...", self.root)
        self.signals.check_cancelled()
        if self.frozen_files is None and self.signals.status_callback:
            self.signals.emit_status("Discovering files...")
        self.excluded_frozen = 0
        if self.frozen_files is not None:
            self._filter_frozen_manifest()
        else:
            self.image_files = []

        # Tracks restrict_dirs entries that survive the bundle guard, so the
        # working-copy extraction pass below scopes its SQL query to the same
        # set the discovery loop actually visited. Without this, a stale folder
        # row inside an excluded bundle (carried over from before the guard)
        # would be re-touched by ``_extract_working_copies`` reading
        # ``folder_path/filename`` — re-tripping the macOS TCC prompt this guard
        # exists to avoid.
        self.effective_restrict_dirs = []
        if self.frozen_files is not None:
            # The manifest already applied the file filters. Retain only the
            # directory scope used by the later working-copy extraction pass;
            # do not enumerate those directories a second time.
            self.effective_restrict_dirs = [
                d for d in (self.restrict_dirs or [])
                if not is_excluded_scan_path(Path(d))
            ]
        elif self.restrict_dirs is not None:
            self._discover_restricted()
        elif self.recursive:
            self._discover_recursive()
        else:
            self._discover_flat()
        self.image_files.sort()
        self.signals.check_cancelled()

    def _filter_frozen_manifest(self):
        # A frozen manifest was captured before any per-source scan began, so
        # a later source can wait minutes behind earlier ones. In that window
        # a nested child dir, mount, or symlink under this source may be
        # replaced with (or into) a macOS app-managed library — a swap the
        # app-level pre-scan mount-identity check cannot see, because it
        # only revalidates the source root. Rerun the bundle guard on every
        # frozen path here so the later ``image_path.stat()`` cannot follow
        # a replacement into ``Photos Library.photoslibrary`` (or a sibling
        # excluded bundle) and re-trip the TCC prompt this guard exists to
        # avoid — or catalog files from a substituted subtree.
        self.image_files = []
        for path in self.frozen_files:
            candidate = Path(path)
            if is_excluded_scan_path(candidate):
                self.excluded_frozen += 1
                continue
            self.image_files.append(candidate)
        if self.excluded_frozen:
            log.warning(
                "Frozen manifest for %s: %d path(s) now resolve into an "
                "excluded app-managed bundle and were skipped",
                self.root, self.excluded_frozen,
            )

    # os.walk + onerror, not Path.rglob: rglob silently skips any
    # subdir that raises during enumeration, so a TCC-denied folder
    # turns into "Found 0 images" with no signal that anything went
    # wrong. macOS TCC raises EPERM ("Operation not permitted");
    # POSIX mode-bit denials raise EACCES. Both must be reported,
    # not swallowed. Other OSError flavors are still surfaced via
    # log so we don't mask unexpected I/O problems. Defined here
    # (not nested in a branch) so the restrict_dirs path below can
    # also surface denials through the same callback.
    #
    # Partial-success (skip the denied dir, keep scanning siblings)
    # is *opt-in* via permission_error_callback. Without a callback,
    # re-raise the PermissionError: existing OSError-aware callers
    # (e.g. pipeline_job repair scan's `except (OSError, RuntimeError)`
    # unreachable counter) rely on the failure to surface, and silent
    # skipping would let a denied folder be reported as successfully
    # repaired. Callers that want to continue past denials must pass
    # the callback to acknowledge they've taken responsibility for
    # streaming the denial somewhere actionable.
    def _on_walk_error(self, err):
        if err.errno in (errno.EPERM, errno.EACCES):
            denied = err.filename or str(self.root_path)
            log.warning(
                "Permission denied enumerating %s — skipping. "
                "On macOS this usually means TCC (Privacy & Security "
                "→ Files and Folders / Removable Volumes) needs to "
                "grant Vireo access.", denied,
            )
            if self.permission_error_callback is not None:
                self.permission_error_callback(denied)
                return
            raise err
        else:
            log.warning("os.walk error at %s: %s", err.filename, err)

    def _discover_restricted(self):
        # Only enumerate files in the specified directories (non-recursive).
        # root is still used as the folder hierarchy root for ``_ScanFolders.ensure_folder``.
        restrict_files_set = set(self.restrict_files) if self.restrict_files is not None else None
        skip_paths = self.skip_paths
        image_files = self.image_files
        # Heartbeat counter, same interval as the recursive branch below.
        # A restricted dir is not necessarily small — the import job's
        # duplicate-folder link scan points this at archive day-folders
        # holding a whole card's worth of files, and on a network mount
        # that enumeration runs for minutes. Without the emit the caller
        # has nothing to show between "Discovering files..." and the
        # finished count.
        checked = 0
        for d in self.restrict_dirs:
            self.signals.check_cancelled()
            dp = Path(d)
            # The outer ``is_excluded_scan_path(root_path)`` guard above only
            # covers ``root``. restrict_dirs entries can independently point
            # into an other-app data bundle — e.g. a stale folder row from
            # before this guard, or a duplicate the caller built from
            # workspace_folders pointing at
            # ``~/Pictures/Photos Library.photoslibrary/originals``. Calling
            # ``dp.is_dir()`` / ``dp.iterdir()`` on that subtree would still
            # trip the macOS "access data from other apps" TCC prompt this
            # guard exists to avoid. Reject before any filesystem access.
            if is_excluded_scan_path(dp):
                log.info(
                    "Skipping other-app data bundle in restrict_dirs: %s", dp,
                )
                continue
            self.effective_restrict_dirs.append(d)
            if dp.is_dir():
                # safe_iter_dir mirrors iterdir() but drops excluded
                # bundle children (direct ``Photos Library.photoslibrary``
                # entries or symlinks pointing into one) before the
                # is_file()/suffix filter below would stat them — that
                # stat alone would re-trip the macOS "access data from
                # other apps" TCC prompt this guard exists to avoid.
                # Permission denials route through _on_walk_error: when
                # ``permission_error_callback`` is registered the helper
                # logs + invokes it and returns, so the generator yields
                # nothing and the for-loop falls through to the next
                # ``d``. Without a callback, _on_walk_error re-raises so
                # callers like pipeline_job's repair scan (``except
                # (OSError, RuntimeError)``) keep their loud failure
                # semantics — we deliberately don't catch that raise.
                #
                # Consume the generator directly rather than materializing
                # with ``list(...)``: on a slow network mount the scandir
                # walk that backs safe_iter_dir takes minutes for a full
                # day-folder, and materializing here would delay every
                # heartbeat below until after that wait — the same silent
                # hang the heartbeat exists to break (PR #1385 Codex /
                # CodeRabbit review).
                for f in safe_iter_dir(str(dp), onerror=self._on_walk_error):
                    self.signals.check_cancelled()
                    checked += 1
                    if checked % 500 == 0 and self.signals.status_callback:
                        self.signals.emit_status(
                            f"Discovering files... ({len(image_files)} found)"
                        )
                    # Filter restricted imports before statting a sibling:
                    # an unreadable collision candidate is unrelated to
                    # the successfully landed files we are cataloging.
                    if (f.suffix.lower() in SUPPORTED_EXTENSIONS
                            and not f.name.startswith(".")
                            and (skip_paths is None or str(f) not in skip_paths)
                            and (restrict_files_set is None
                                 or str(f) in restrict_files_set)
                            and f.is_file()):
                        image_files.append(f)

    def _discover_recursive(self):
        skip_paths = self.skip_paths
        image_files = self.image_files
        checked = 0
        # safe_scan_walk replaces os.walk + prune_scan_dirs. It excludes
        # other-app data bundles (e.g. "Photos Library.photoslibrary"
        # sitting in ~/Pictures) without ever stat-following a symlink
        # to one — os.walk's classification call follows symlinks and
        # would re-trip the macOS "access data from other apps" TCC
        # prompt for a child like ``LibraryAlias -> Photos
        # Library.photoslibrary`` before prune_scan_dirs could reject it.
        # Pass cancel_check into safe_scan_walk so it polls between
        # scandir entries as well: a single very large directory (a
        # media dump with 1M+ files) can otherwise consume the whole
        # ``os.scandir`` loop before yielding, leaving the per-yield
        # ``check_cancelled()`` below unreachable until the walker
        # finishes filling its dirs/nondirs lists.
        for dirpath, _dirnames, filenames in safe_scan_walk(
            str(self.root_path), onerror=self._on_walk_error,
            cancel_check=self.signals.cancel_check,
        ):
            self.signals.check_cancelled()
            for name in filenames:
                checked += 1
                if checked % 100 == 0:
                    self.signals.check_cancelled()
                if checked % 500 == 0 and self.signals.status_callback:
                    self.signals.emit_status(
                        f"Discovering files... ({len(image_files)} found)"
                    )
                ext = os.path.splitext(name)[1].lower()
                if (ext in SUPPORTED_EXTENSIONS
                        and not name.startswith(".")):
                    full = os.path.join(dirpath, name)
                    if skip_paths is not None and full in skip_paths:
                        continue
                    # os.walk includes broken symlinks in `filenames`,
                    # but the pre-pass below calls image_path.stat()
                    # which would raise FileNotFoundError and abort
                    # the whole scan. The previous Path.rglob path
                    # filtered these via is_file(); preserve that.
                    # os.path.isfile follows symlinks and returns
                    # False (not raise) for dangling targets.
                    if not os.path.isfile(full):
                        continue
                    image_files.append(Path(full))

    def _discover_flat(self):
        skip_paths = self.skip_paths
        image_files = self.image_files
        # safe_iter_dir mirrors iterdir() but drops excluded bundle
        # children before the is_file() filter below would stat
        # them. A normal root like ~/Pictures can hold ``Photos
        # Library.photoslibrary`` (or a symlink to one) as a direct
        # child; a bare iterdir + is_file() would stat that bundle
        # and re-trip the macOS "access data from other apps" TCC
        # prompt this guard exists to avoid, even though the
        # extension filter would have rejected it afterwards.
        # Permission denials route through _on_walk_error (callback
        # → empty entries, no callback → re-raise) the same way the
        # restrict_dirs branch above does.
        entries = list(safe_iter_dir(str(self.root_path), onerror=self._on_walk_error))
        for checked, f in enumerate(entries, 1):
            if checked % 100 == 0:
                self.signals.check_cancelled()
            if checked % 500 == 0 and self.signals.status_callback:
                self.signals.emit_status(
                    f"Discovering files... ({len(image_files)} found)"
                )
            if (f.is_file()
                    and f.suffix.lower() in SUPPORTED_EXTENSIONS
                    and not f.name.startswith(".")
                    and (skip_paths is None or str(f) not in skip_paths)):
                image_files.append(f)

    def working_copy_scope(self):
        if self.restrict_dirs is not None:
            # Use the bundle-filtered list — see ``effective_restrict_dirs``
            # above. Reusing the raw ``restrict_dirs`` here would let a
            # stale DB row inside an excluded bundle re-enter the
            # working-copy extractor, which reads ``folder_path/filename``
            # and re-trips the macOS TCC prompt the scan loop's guard
            # already skipped.
            return [(str(d), "exact") for d in self.effective_restrict_dirs]
        if not self.recursive:
            return [(str(self.root_path), "exact")]
        return [str(self.root_path)]
