"""Mount-table classification and mount-identity checks for deletes.

The Trash and permanent-delete paths must never make an in-process
filesystem call against an unhealthy network share: an SMB ``stat`` or
``rename`` can wait in the kernel indefinitely. This module answers, without
touching the shares, which paths live on a network mount
(``network_volume_roots`` reads the kernel mount table through ``mount``;
``path_on_network_volume`` and ``deepest_network_root_for_path`` classify a
path against that snapshot), and whether a file that looks gone is really
gone from a live volume rather than hidden by a detached mount
(``snapshot_parent_device`` before the operation, ``path_confirmed_gone``
after it).

``services.trash`` and ``services.photo_deletion`` call these through the
module (``volumes.path_on_network_volume(...)``), so a test that patches a
name here reaches every caller.
"""

from __future__ import annotations

import os
import posixpath
import stat
import subprocess
import sys
from collections.abc import Callable, Collection, Iterable, Mapping

import remote_setup
from proc import no_window_kwargs
from volume_reachability import MOUNT_QUERY_TIMEOUT_SECS

# One mount-table snapshot's network roots, or ``None`` when the mount table
# could not be read (callers then fail closed and treat every macOS path as
# network-backed). ``NetworkVolumeRoots`` adds the live ``/Volumes`` roots
# from the same snapshot; a plain collection carries no liveness evidence.
type NetworkRoots = Collection[str] | None

# ``subprocess.run`` as ``network_volume_roots`` calls it.
type RunCommand = Callable[..., subprocess.CompletedProcess[str]]


class NetworkVolumeRoots(set[str]):
    """Network roots plus the live /Volumes roots from one mount snapshot."""

    def __init__(
        self, network_roots: Iterable[str] = (), mounted_volume_roots: Iterable[str] = (),
    ) -> None:
        super().__init__(network_roots)
        self.mounted_volume_roots = frozenset(mounted_volume_roots)


def mounted_volume_roots(mounts: Iterable[Mapping[str, str]]) -> set[str]:
    """Return live top-level ``/Volumes/<name>`` roots from parsed mounts."""
    roots = set()
    for mount in mounts:
        normalized = posixpath.normpath(mount["mount_point"])
        parts = normalized.split("/")
        if len(parts) == 3 and parts[1] == "Volumes" and parts[2]:
            roots.add(normalized)
    return roots


def volume_root_for_path(filepath: str) -> str | None:
    """Return ``/Volumes/<name>`` for a path on a macOS mounted volume."""
    try:
        normalized = os.path.normpath(os.path.abspath(filepath))
    except (OSError, TypeError, ValueError):
        return None
    parts = normalized.split(os.sep)
    if len(parts) < 4 or parts[0] != "" or parts[1] != "Volumes" or not parts[2]:
        return None
    return os.sep.join(parts[:3])


def network_volume_roots(run: RunCommand = subprocess.run) -> NetworkVolumeRoots | set[str] | None:
    """Return mounted macOS network-volume roots without touching the shares.

    ``mount`` reads the kernel mount table, so this stays responsive even when
    an SMB server is unhealthy.  ``None`` means the mount table could not be
    read; callers fail closed and treat ``/Volumes`` paths as network-backed
    rather than risking an unbounded in-process filesystem call.
    """
    if sys.platform != "darwin":
        return set()
    try:
        result = run(
            ["mount"], capture_output=True, text=True,
            timeout=MOUNT_QUERY_TIMEOUT_SECS,
            **no_window_kwargs(),
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    output = result.stdout or ""
    mounts = remote_setup.parse_mount_table(output)
    return NetworkVolumeRoots(
        # ``mount`` always reports macOS/POSIX paths.  Keep parsing independent
        # of the host running the test suite (notably Windows' ``ntpath``).
        (
            posixpath.normpath(mount["mount_point"])
            for mount in mounts
            if remote_setup.mount_type_is_network_or_unknown(
                mount["fs_type"],
            )
        ),
        mounted_volume_roots(mounts),
    )


def expand_first_symlink_prefix(filepath: str) -> str | None:
    """Expand one local symlink prefix without resolving its target.

    ``realpath`` follows the target and can block when that target is an
    unhealthy network share. Reading the first symlink itself only touches
    its local directory entry; the unvisited suffix is then appended
    lexically so network-volume classification stays free of share I/O.
    """
    try:
        normalized = os.path.normpath(os.path.abspath(filepath))
    except (OSError, TypeError, ValueError):
        return None
    drive, tail = os.path.splitdrive(normalized)
    parts = [part for part in tail.split(os.sep) if part]
    prefix = drive + os.sep
    for index, part in enumerate(parts):
        prefix = os.path.join(prefix, part)
        try:
            target = os.readlink(prefix)
        except OSError:
            continue
        if os.name == "nt":
            # Windows junctions commonly expose their substitution path
            # through os.readlink() with an extended-length prefix. Strip it
            # so comparisons against ordinary drive or UNC mount roots use
            # the same spelling.
            if target.startswith("\\\\?\\UNC\\"):
                target = "\\\\" + target[8:]
            elif target.startswith("\\\\?\\"):
                target = target[4:]
        if not os.path.isabs(target):
            target = os.path.join(os.path.dirname(prefix), target)
        return os.path.normpath(os.path.join(target, *parts[index + 1:]))
    return None


def path_on_network_volume(filepath: str, network_roots: NetworkRoots) -> bool:
    """Whether ``filepath`` should avoid in-process mounted-volume I/O."""
    normalized = os.path.normpath(os.path.abspath(filepath))
    if network_roots is None:
        # Discovery failed, so there is no trustworthy evidence that any
        # candidate is local. Route every macOS path through bounded Finder
        # handling instead of risking an in-process stat on a custom mount.
        # Preserve the explicit /Volumes fallback on non-macOS test hosts.
        return (
            sys.platform == "darwin"
            or volume_root_for_path(normalized) is not None
        )
    for _depth in range(16):
        for root in network_roots:
            try:
                if os.path.commonpath((normalized, root)) == root:
                    return True
            except ValueError:
                continue
        if sys.platform == "darwin":
            volume_root = volume_root_for_path(normalized)
            if volume_root is not None:
                mounted_roots = getattr(
                    network_roots, "mounted_volume_roots", None,
                )
                # A detached network mount disappears from the snapshot but
                # leaves its /Volumes directory behind. Conversely, a live
                # local USB/APFS root in the same snapshot must retain the
                # local-trash path. Plain sets carry no liveness evidence,
                # so continue to fail closed for them.
                if mounted_roots is None or volume_root not in mounted_roots:
                    return True
        if sys.platform != "darwin":
            return False
        expanded = expand_first_symlink_prefix(normalized)
        if expanded is None:
            return False
        normalized = expanded
    # A symlink loop or unusually deep chain cannot be classified safely.
    # Fail closed on macOS so no subsequent stat reaches a possible share.
    return True


def deepest_network_root_for_path(filepath: str, network_roots: NetworkRoots) -> str | None:
    """Return the *deepest* ``network_roots`` entry ``filepath`` resolves into.

    ``path_on_network_volume`` answers the yes/no membership question and
    short-circuits on the first matching root, which is enough for routing
    decisions but not for reachability probing.  When mounts are nested —
    e.g. a still-reachable ``/Volumes/NAS`` share with a detached
    ``/Volumes/NAS/archive`` share mounted underneath it — the caller must
    probe reachability of the *exact* mount the path depends on rather than
    any reachable ancestor; otherwise a healthy outer mount would vouch for
    a nested inner mount that is actually gone, and Finder's false
    ``missing`` result would prune catalog rows for photos that reappear on
    reconnect.  Returns ``None`` when no root matches (either directly or
    via symlink expansion, mirroring ``path_on_network_volume``'s traversal).
    """
    if not network_roots:
        return None
    normalized = os.path.normpath(os.path.abspath(filepath))
    for _depth in range(16):
        best = None
        best_len = -1
        for root in network_roots:
            try:
                if (
                    os.path.commonpath((normalized, root)) == root
                    and len(root) > best_len
                ):
                    # Longest matching root wins so nested mounts probe the
                    # inner share rather than an outer one that happens to
                    # be iterated first from the roots set.
                    best = root
                    best_len = len(root)
            except ValueError:
                continue
        if best is not None:
            return best
        if sys.platform != "darwin":
            return None
        expanded = expand_first_symlink_prefix(normalized)
        if expanded is None:
            return None
        normalized = expanded
    return None


def snapshot_parent_device(filepath: str) -> int | None:
    """Return the parent directory's ``st_dev`` for later mount verification.

    Callers snapshot this before any deletion attempt and pass it to
    :func:`path_confirmed_gone` afterwards. A network mount that
    disconnects mid-op leaves the mount-point directory visible on the
    underlying local filesystem, so ``os.path.isdir`` alone reports the
    parent as live even though the actual volume is gone. Comparing the
    pre-op and post-op ``st_dev`` catches that drop. ``None`` means no
    baseline could be captured, so the device check is skipped.
    """
    parent = os.path.dirname(filepath) or os.sep
    try:
        return os.stat(parent).st_dev
    except OSError:
        return None


def path_confirmed_gone(filepath: str, expected_parent_dev: int | None = None) -> bool:
    """Return True only when the file is verifiably absent from a live volume.

    ``os.path.exists`` returning False is ambiguous on network volumes: it
    also returns False when the underlying stat call errors out because the
    mount went away mid-operation. Treating that as "successfully moved to
    Trash" would prune the catalog row for a photo that reappears when the
    mount comes back. Confirm both that the file is missing *and* that the
    parent directory is still reachable so a genuine live-volume delete is
    accepted while a mid-flight disconnect is preserved as a failure.

    When ``expected_parent_dev`` is provided (from
    :func:`snapshot_parent_device`), also require the parent's current
    ``st_dev`` to match — a network mount that vanishes can leave its
    mount-point directory in place on the underlying local filesystem, so
    the ``os.path.isdir`` check alone would incorrectly accept the file
    as gone.
    """
    if os.path.exists(filepath):
        return False
    parent = os.path.dirname(filepath) or os.sep
    try:
        parent_stat = os.stat(parent)
    except OSError:
        return False
    if not stat.S_ISDIR(parent_stat.st_mode):
        return False
    return not (
        expected_parent_dev is not None
        and parent_stat.st_dev != expected_parent_dev
    )
