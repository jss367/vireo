"""Moving originals to the Trash without hanging on an unhealthy share.

``trash_paths`` is the one Trash implementation behind batch delete,
duplicate-loser cleanup, the stray-sidecar delete and post-move source
cleanup. It classifies every path against the kernel mount table first
(``services.volumes``), so a path on a network share never reaches an
in-process filesystem call: those go straight to one time-bounded Finder
subprocess per batch. Local paths on a mounted volume take a direct
same-volume move into ``/Volumes/<name>/.Trashes/<uid>``, then
``send2trash``, then Finder.

A path counts as successful only when its file verifiably left a live
volume. A "missing" answer is ambiguous on a detached network mount (its
mount-point directory stays behind on the local disk), so it is accepted
only after the mount table, an out-of-process reachability probe of the
path's own mount and a second Finder look all agree; local paths must keep
their parent's device. Anything else stays a failure, so callers keep the
catalog row of a photo that would reappear when the share comes back.

``TrashService`` is the object the delete paths hold: ``create_app`` builds
one and passes it to ``PhotoDeletion`` and to the audit, duplicates and
move-cleanup blueprints, and route tests inject a fake through
``create_app(trash=...)`` (``vireo/tests/fake_trash.py``).

Inside the run, collaborators are reached through their modules
(``volumes.<name>``, ``volume_reachability.network_root_reachable``) and this
module's own globals, so a unit test that patches one OS boundary there
reaches the whole run.
"""

from __future__ import annotations

import concurrent.futures
import enum
import logging
import os
import subprocess
import sys
import uuid
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final, Literal

import volume_reachability
from file_replace import replace_file
from proc import no_window_kwargs
from services import volumes
from sql_chunks import chunked

if TYPE_CHECKING:
    from services.service_types import JsonDict
    from services.volumes import NetworkRoots

log = logging.getLogger(__name__)

FINDER_TRASH_TIMEOUT_SECS = 30
FINDER_TRASH_BATCH_SIZE = 20

# ``(moved, successful paths, failures)``: how many files moved to the Trash,
# every path whose requested end state holds (moved or already gone), and a
# ``{"path", "error"}`` dict per path that did not get there.
type PathOutcome = tuple[int, set[str], list[JsonDict]]

# ``(current, total, filename)`` after each path is settled.
type TrashProgress = Callable[[int, int, str], None]


class _Unset(enum.Enum):
    TOKEN = enum.auto()


# Distinct from ``None`` so ``trash_paths`` can tell "caller didn't pass
# network_roots" (re-query is safe) apart from "caller's own mount query
# already failed and it is passing that fail-closed signal through"
# (re-querying would overwrite the caller's classification with a stale
# or empty set the moment the share detaches, reclassifying an
# already-known custom mount point as local and reintroducing the
# unbounded-I/O hang this routing exists to prevent).
NETWORK_ROOTS_UNSET: Final = _Unset.TOKEN


class TrashService:
    """The Trash and the mount checks every delete of originals goes through.

    ``trash_paths`` moves files to the Trash (see the module docstring). The
    other methods are the ``services.volumes`` checks the callers make around
    it: the duplicates routes classify losers against one mount-table
    snapshot (``network_volume_roots``, ``path_on_network_volume``) before
    any stat, and permanent delete brackets each ``os.remove`` with
    ``snapshot_parent_device`` / ``path_confirmed_gone``.
    """

    def trash_paths(
        self, filepaths: Iterable[str], progress_callback: TrashProgress | None = None,
        already_missing_out: set[str] | None = None,
        network_roots: NetworkRoots | Literal[_Unset.TOKEN] = NETWORK_ROOTS_UNSET,
    ) -> PathOutcome:
        """:func:`trash_paths`: ``(moved, successful paths, failures)``."""
        return trash_paths(
            filepaths, progress_callback=progress_callback,
            already_missing_out=already_missing_out, network_roots=network_roots,
        )

    def network_volume_roots(self) -> NetworkRoots:
        """One mount-table snapshot, or ``None`` when it could not be read."""
        return volumes.network_volume_roots()

    def path_on_network_volume(self, filepath: str, network_roots: NetworkRoots) -> bool:
        """Whether ``filepath`` must avoid in-process filesystem calls."""
        return volumes.path_on_network_volume(filepath, network_roots)

    def snapshot_parent_device(self, filepath: str) -> int | None:
        """The parent directory's ``st_dev``, taken before a delete."""
        return volumes.snapshot_parent_device(filepath)

    def path_confirmed_gone(self, filepath: str, expected_parent_dev: int | None = None) -> bool:
        """Whether ``filepath`` is verifiably absent from a live volume."""
        return volumes.path_confirmed_gone(filepath, expected_parent_dev)


def missing_paths_via_finder(
    filepaths: Iterable[str | os.PathLike[str]], timeout: float = FINDER_TRASH_TIMEOUT_SECS,
) -> tuple[set[str], set[str], list[JsonDict]]:
    """Boundedly confirm which paths remain absent according to Finder."""
    filepaths = [os.fspath(path) for path in filepaths]
    if not filepaths:
        return set(), set(), []
    result = subprocess.run(
        [
            "osascript",
            "-e", "on run argv",
            "-e", "set statuses to {}",
            "-e", "repeat with posixPath in argv",
            "-e", "set statusValue to \"error\"",
            "-e", "try",
            "-e", "set fileRef to POSIX file (contents of posixPath)",
            "-e", "tell application \"Finder\"",
            "-e", "if exists fileRef then",
            "-e", "set statusValue to \"exists\"",
            "-e", "else",
            "-e", "set statusValue to \"missing\"",
            "-e", "end if",
            "-e", "end tell",
            "-e", "end try",
            "-e", "set end of statuses to statusValue",
            "-e", "end repeat",
            "-e", "set AppleScript's text item delimiters to linefeed",
            "-e", "return statuses as text",
            "-e", "end run",
            "--",
            *filepaths,
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
        **no_window_kwargs(),
    )
    if result.returncode != 0:
        raise OSError(
            result.stderr.strip()
            or f"Finder existence check failed ({result.returncode})"
        )
    statuses = result.stdout.splitlines()
    if len(statuses) != len(filepaths):
        raise OSError("Finder returned an invalid existence response")
    missing = set()
    existing = set()
    failures = []
    for filepath, outcome in zip(filepaths, statuses, strict=True):
        if outcome == "missing":
            missing.add(filepath)
        elif outcome == "exists":
            existing.add(filepath)
        else:
            failures.append({
                "path": filepath, "error": "Finder existence check failed",
            })
    return missing, existing, failures


def move_to_volume_trash(filepath: str) -> bool:
    """Move one file directly into a local mounted volume's Trash directory.

    Finder ultimately performs a same-volume move into
    ``/Volumes/<name>/.Trashes/<uid>``. Doing that move directly avoids the
    legacy Carbon ``send2trash`` failure on removable drives. Network mounts
    must be filtered by :func:`trash_paths` before calling this helper because
    their rename syscall can block indefinitely. Returns ``True`` only when
    the move completed; callers retain their normal trash fallbacks on
    ``False``.
    """
    if sys.platform != "darwin":
        return False
    volume_root = volumes.volume_root_for_path(filepath)
    if volume_root is None:
        return False

    trash_dir = os.path.join(volume_root, ".Trashes", str(os.getuid()))
    try:
        os.makedirs(trash_dir, mode=0o700, exist_ok=True)
        # Refuse a redirected Trash directory. The destination must remain on
        # the same mounted volume as the source.
        real_trash = os.path.realpath(trash_dir)
        if os.path.commonpath((volume_root, real_trash)) != volume_root:
            raise OSError("volume Trash directory resolves outside its volume")

        basename = os.path.basename(filepath)
        stem, ext = os.path.splitext(basename)
        # Atomically reserve the destination name with O_CREAT|O_EXCL so two
        # concurrent moves for same-named files from different folders can't
        # both observe an empty slot and then rename to the same path — POSIX
        # rename would silently replace one file, permanently losing a photo
        # the user meant to send to Trash.
        candidate = os.path.join(trash_dir, basename)
        reserved = None
        reserved_stat = None
        for _ in range(32):
            try:
                fd = os.open(
                    candidate, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600,
                )
                try:
                    reserved_stat = os.fstat(fd)
                finally:
                    os.close(fd)
                reserved = candidate
                break
            except FileExistsError:
                candidate = os.path.join(
                    trash_dir, f"{stem} {uuid.uuid4().hex[:8]}{ext}",
                )
        if reserved is None or reserved_stat is None:
            raise OSError(
                f"could not reserve a unique Trash destination in {trash_dir}",
            )
        try:
            # ``os.replace`` (not ``os.rename``) so the O_EXCL placeholder we
            # just reserved is overwritten. POSIX rename replaces silently,
            # but Windows rename raises when the destination exists.
            replace_file(filepath, reserved)
        except OSError:
            # A network filesystem can commit a rename but lose the success
            # response. Never unlink ``reserved`` unless it is still the exact
            # placeholder we created; otherwise that path may now be the photo.
            try:
                current_stat = os.lstat(reserved)
                try:
                    os.lstat(filepath)
                    source_still_exists = True
                except FileNotFoundError:
                    source_still_exists = False
                if not source_still_exists:
                    return True
                placeholder_unchanged = (
                    reserved_stat.st_ino
                    and current_stat.st_dev == reserved_stat.st_dev
                    and current_stat.st_ino == reserved_stat.st_ino
                    and current_stat.st_size == reserved_stat.st_size
                )
                if placeholder_unchanged:
                    os.unlink(reserved)
            except OSError:
                pass
            raise
        return True
    except OSError as exc:
        log.debug("Direct volume Trash move failed for %s: %s", filepath, exc)
        return False


def trash_via_finder(
    filepaths: str | os.PathLike[str] | Iterable[str | os.PathLike[str]],
    timeout: float = FINDER_TRASH_TIMEOUT_SECS,
) -> tuple[set[str], set[str], list[JsonDict]]:
    """Trash paths via one bounded Finder call with per-item outcomes.

    Fallback for when send2trash fails (e.g. external volumes where the
    legacy Carbon API can't locate .Trashes). macOS-only: on Linux/Windows
    ``send2trash`` already implements the platform trash spec, so there is no
    equivalent fallback. Raising here (instead of spawning a doomed
    ``osascript``) lets the caller surface the original send2trash failure.

    The script catches each path's error and continues so a retry containing
    files already moved by a timed-out earlier batch cannot abort before later
    files. Returns ``(moved_paths, missing_paths, failures)``. A "missing"
    outcome (Finder saw ``sourceExists=false`` but the parent directory still
    exists) is reported separately so the caller can revalidate mount
    identity before accepting it: an unmounted network volume leaves its
    mount-point directory in place on the underlying local FS, so Finder's
    ``parentExists`` check alone cannot distinguish a genuine delete from a
    silently detached mount.
    """
    if sys.platform != "darwin":
        raise OSError("Finder trash fallback is only available on macOS")
    if isinstance(filepaths, (str, bytes, os.PathLike)):
        paths = [os.fspath(filepaths)]
    else:
        paths = [os.fspath(path) for path in filepaths]
    if not paths:
        return set(), set(), []
    result = subprocess.run(
        [
            "osascript",
            "-e", "on run argv",
            "-e", "set statuses to {}",
            "-e", "repeat with posixPath in argv",
            "-e", "set statusValue to \"error\"",
            "-e", "set fileRef to POSIX file (contents of posixPath)",
            "-e", "try",
            "-e", "tell application \"Finder\" to delete fileRef",
            "-e", "set statusValue to \"moved\"",
            "-e", "on error",
            "-e", "try",
            "-e", (
                "set parentPath to do shell script \"/usr/bin/dirname \" & "
                "quoted form of (contents of posixPath)"
            ),
            "-e", "set parentRef to POSIX file parentPath",
            "-e", "tell application \"Finder\"",
            "-e", "set sourceExists to exists fileRef",
            "-e", "set parentExists to exists parentRef",
            "-e", "end tell",
            "-e", (
                "if (not sourceExists) and parentExists then "
                "set statusValue to \"missing\""
            ),
            "-e", "end try",
            "-e", "end try",
            "-e", "set end of statuses to statusValue",
            "-e", "end repeat",
            "-e", "set AppleScript's text item delimiters to linefeed",
            "-e", "return statuses as text",
            "-e", "end run",
            "--",
            *paths,
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
        **no_window_kwargs(),
    )
    if result.returncode != 0:
        raise OSError(result.stderr.strip() or f"Finder trash failed ({result.returncode})")
    statuses = result.stdout.splitlines()
    if len(statuses) != len(paths):
        raise OSError(
            "Finder trash returned an invalid per-file status response"
        )
    moved_paths = set()
    missing_paths = set()
    failures = []
    for filepath, outcome in zip(paths, statuses, strict=True):
        if outcome == "moved":
            moved_paths.add(filepath)
        elif outcome == "missing":
            missing_paths.add(filepath)
        else:
            failures.append({
                "path": filepath, "error": "Finder Trash failed",
            })
    return moved_paths, missing_paths, failures


def trash_paths(
    filepaths: Iterable[str], progress_callback: TrashProgress | None = None,
    already_missing_out: set[str] | None = None,
    network_roots: NetworkRoots | Literal[_Unset.TOKEN] = NETWORK_ROOTS_UNSET,
) -> PathOutcome:
    """Move paths to Trash and return ``(moved, successful, failures)``.

    Missing paths are successful (the requested end state already holds) but
    are not counted as moved. On macOS mounted volumes we try a direct,
    same-volume rename first. Network volumes skip all in-process move APIs:
    an SMB ``rename(2)`` can wait in the kernel indefinitely, so those paths
    go directly to the time-bounded Finder subprocess. Remaining paths use
    send2trash individually so failures can be attributed, then one Finder
    process per bounded batch.

    ``already_missing_out``, when a mutable set, is populated with every
    path treated as successful because the requested end state already
    held (local preflight found it absent, or Finder reported it missing
    on a still-mounted volume). Callers surface those to users as
    "already missing" rather than as trashed, so a duplicate-cleanup that
    finds every loser already gone can return an explicit terminal
    result instead of a silent ``{trashed: 0}``.

    ``network_roots`` lets a caller reuse a mount-table classification it
    already performed. When omitted we re-query
    ``volumes.network_volume_roots()``. Callers whose own mount query
    already failed should pass ``None`` explicitly: that preserves the
    fail-closed classification they made against ``/Volumes`` paths instead
    of us re-querying and — if the second query succeeds with the share now
    detached — silently reclassifying an already-known custom mount point
    (``/Users/me/mnt/photos``) as local, which is the exact case we routed
    through Finder in the first place.
    """
    ordered = list(dict.fromkeys(filepaths))
    if network_roots is _Unset.TOKEN:
        network_roots = volumes.network_volume_roots()
    run = _TrashRun(ordered, progress_callback, already_missing_out)

    local_paths = run.route_network_paths_to_finder(network_roots)
    run.snapshot_parent_devices(local_paths)
    run.preflight_and_move_local_paths(local_paths)
    if run.fallback:
        run.send_fallback_to_trash()

    for finder_batch in chunked(
        run.finder_candidates, size=FINDER_TRASH_BATCH_SIZE,
    ):
        try:
            run.trash_finder_batch(finder_batch)
        except subprocess.TimeoutExpired:
            log.warning(
                "Finder Trash timed out after %ss for %d file(s)",
                FINDER_TRASH_TIMEOUT_SECS, len(finder_batch),
            )
            for filepath in finder_batch:
                run.send_errors[filepath] = (
                    f"Finder Trash timed out after "
                    f"{FINDER_TRASH_TIMEOUT_SECS}s"
                )
        except Exception as exc:  # broad: Finder's AppleScript bridge raises undocumented errors; every file gets the reason
            log.warning("Finder Trash failed for a file batch", exc_info=True)
            for filepath in finder_batch:
                run.send_errors[filepath] = str(exc) or "Finder Trash failed"
        for filepath in finder_batch:
            run.report_processed(filepath)

    return run.moved, run.successful, run.collect_failures()


@dataclass
class _FinderMissingEvidence:
    """One Finder batch's independent checks on its "missing" outcomes."""

    batch_network_roots: NetworkRoots = None
    reachable_network_roots: set[str] = field(default_factory=set)
    path_to_deepest_root: dict[str, str] = field(default_factory=dict)
    confirmed_network_missing: set[str] = field(default_factory=set)
    finder_recheck_errors: set[str] = field(default_factory=set)


class _TrashRun:
    """Per-call state for one :func:`trash_paths` invocation."""

    def __init__(
        self, ordered: list[str], progress_callback: TrashProgress | None,
        already_missing_out: set[str] | None,
    ) -> None:
        self.ordered = ordered
        self.progress_callback = progress_callback
        self.already_missing_out = already_missing_out
        self.successful: set[str] = set()
        self.moved = 0
        self.fallback: list[str] = []
        self.preflight_errors: dict[str, str] = {}
        self.finder_candidates: list[str] = []
        self.network_finder_candidates: set[str] = set()
        self.send_errors: dict[str, str] = {}
        self.processed: set[str] = set()
        self.parent_devs: dict[str, int | None] = {}

    def report_processed(self, filepath: str) -> None:
        if filepath in self.processed:
            return
        self.processed.add(filepath)
        if self.progress_callback:
            self.progress_callback(
                len(self.processed), len(self.ordered),
                os.path.basename(filepath),
            )

    def _mark_already_missing(self, filepath: str) -> None:
        self.successful.add(filepath)
        if self.already_missing_out is not None:
            self.already_missing_out.add(filepath)

    def route_network_paths_to_finder(self, network_roots: NetworkRoots) -> list[str]:
        """Return the local paths; queue network paths for Finder.

        Classify paths using the kernel mount table before any source or
        parent stat.  Those metadata calls can themselves block indefinitely
        while an unhealthy SMB mount is reconnecting, so network candidates
        must go straight to the bounded Finder subprocess.
        """
        local_paths = []
        for filepath in self.ordered:
            if volumes.path_on_network_volume(filepath, network_roots):
                self.finder_candidates.append(filepath)
                self.network_finder_candidates.add(filepath)
                self.send_errors[filepath] = (
                    "Network volume Trash operation failed"
                )
            else:
                local_paths.append(filepath)
        return local_paths

    def snapshot_parent_devices(self, local_paths: Sequence[str]) -> None:
        """Snapshot each local parent's st_dev before we touch anything.

        A network mount that vanishes mid-batch can leave the mount-point
        directory visible on the underlying local FS, so ``os.path.isdir``
        alone would accept the file as gone. Comparing pre-op vs post-op
        st_dev catches the mount drop even when the directory still stats
        cleanly.
        """
        self.parent_devs = {
            path: volumes.snapshot_parent_device(path) for path in local_paths
        }

    def preflight_and_move_local_paths(self, local_paths: Sequence[str]) -> None:
        for filepath in local_paths:
            if not os.path.isfile(filepath):
                self._settle_absent_local_path(filepath)
                self.report_processed(filepath)
                continue
            if move_to_volume_trash(filepath):
                self.successful.add(filepath)
                self.moved += 1
                self.report_processed(filepath)
            else:
                self.fallback.append(filepath)

    def _settle_absent_local_path(self, filepath: str) -> None:
        # ``os.path.isfile`` returning False is ambiguous on network
        # volumes — it also happens when the underlying stat fails
        # because the mount is already disconnected. Only treat the
        # path as "already gone" when the parent directory is still
        # reachable AND its device matches the pre-op snapshot;
        # otherwise preserve as a failure so the caller doesn't prune
        # the catalog row for a photo that reappears when the mount
        # comes back.
        if volumes.path_confirmed_gone(filepath, self.parent_devs.get(filepath)):
            log.warning("File already missing: %s", filepath)
            self._mark_already_missing(filepath)
        else:
            self.preflight_errors[filepath] = (
                "Source path is unreachable"
            )
            log.warning(
                "Trash preflight: source unreachable for %s", filepath,
            )

    def send_fallback_to_trash(self) -> None:
        from send2trash import send2trash as _trash
        for filepath in self.fallback:
            try:
                _trash(filepath)
                self.successful.add(filepath)
                self.moved += 1
                self.report_processed(filepath)
            except BaseException as exc:  # broad: per-file boundary; platform Trash APIs raise undocumented errors, recorded per file
                if isinstance(exc, (KeyboardInterrupt, GeneratorExit)):
                    raise
                # Some platform Trash APIs can complete the move and still
                # report a post-operation error. Only trust the "not there"
                # signal when we can positively verify it — a disconnected
                # network volume also makes ``os.path.exists`` return False
                # (the underlying stat fails), which would otherwise mask a
                # real failure and prune the catalog row for a photo that
                # reappears when the mount comes back.
                if volumes.path_confirmed_gone(
                    filepath, self.parent_devs.get(filepath),
                ):
                    self.successful.add(filepath)
                    self.moved += 1
                    self.report_processed(filepath)
                    continue
                self.send_errors[filepath] = str(exc)
                if sys.platform == "darwin":
                    self.finder_candidates.append(filepath)
                else:
                    self.report_processed(filepath)

    def trash_finder_batch(self, finder_batch: Sequence[str]) -> None:
        finder_moved_paths, finder_missing_paths, finder_failures = (
            trash_via_finder(finder_batch)
        )
        self.moved += len(finder_moved_paths)
        self.successful.update(finder_moved_paths)
        evidence = self._gather_missing_evidence(finder_missing_paths)
        self._settle_finder_missing(finder_missing_paths, evidence)
        for failure in finder_failures:
            self.send_errors[failure["path"]] = failure["error"]

    def _gather_missing_evidence(self, finder_missing_paths: set[str]) -> _FinderMissingEvidence:
        evidence = _FinderMissingEvidence()
        # Query the mount table at most once per batch. A retry that
        # contains many paths already moved by an earlier timed-out
        # Finder call comes back with every path in ``missing`` — doing
        # a fresh ``mount`` subprocess per path could otherwise burn up
        # to ``MOUNT_QUERY_TIMEOUT_SECS`` × ``len(batch)`` seconds and
        # undermine the bounded batch behaviour this code establishes.
        batch_network_missing = any(
            path in self.network_finder_candidates
            for path in finder_missing_paths
        )
        evidence.batch_network_roots = (
            volumes.network_volume_roots() if batch_network_missing else None
        )
        if batch_network_missing and evidence.batch_network_roots is not None:
            paths_still_on_network = {
                path for path in finder_missing_paths
                if path in self.network_finder_candidates
                and volumes.path_on_network_volume(
                    path, evidence.batch_network_roots,
                )
            }
            self._probe_deepest_network_roots(
                paths_still_on_network, evidence,
            )
            if paths_still_on_network:
                self._recheck_missing_via_finder(
                    paths_still_on_network, evidence,
                )
        return evidence

    def _probe_deepest_network_roots(
        self, paths_still_on_network: set[str], evidence: _FinderMissingEvidence,
    ) -> None:
        """Probe each still-relevant mount root with a bounded
        out-of-process ``stat`` — a signal independent of Finder's
        exists-cache — so a still-listed but unreachable SMB server
        cannot make ``missing`` outcomes look legitimate. Run the
        probes concurrently so a Finder batch spanning many
        unavailable shares completes within one probe timeout
        rather than accumulating ``len(distinct_roots)`` ×
        ``MOUNT_QUERY_TIMEOUT_SECS`` serially — a full 20-item
        batch across unreachable roots would otherwise add up to
        ~100 seconds of hang time before the Finder recheck.
        """
        # Associate each path with the *deepest* mount root it
        # resolves into so nested mounts probe reachability of the
        # inner share rather than an outer one that happens to be
        # iterated first. Set iteration is order-independent, so
        # picking the first match could otherwise validate a
        # detached inner mount using a live outer one and prune
        # rows for photos that reappear on reconnect.
        for path in paths_still_on_network:
            root = volumes.deepest_network_root_for_path(
                path, evidence.batch_network_roots,
            )
            if root is not None:
                evidence.path_to_deepest_root[path] = root
        distinct_roots = list(set(evidence.path_to_deepest_root.values()))
        if distinct_roots:
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=len(distinct_roots),
            ) as executor:
                probe_results = executor.map(
                    volume_reachability.network_root_reachable, distinct_roots,
                )
                for root, is_reachable in zip(
                    distinct_roots, probe_results, strict=True,
                ):
                    if is_reachable:
                        evidence.reachable_network_roots.add(root)

    def _recheck_missing_via_finder(
        self, paths_still_on_network: set[str], evidence: _FinderMissingEvidence,
    ) -> None:
        try:
            (
                evidence.confirmed_network_missing,
                reappeared_paths,
                recheck_failures,
            ) = missing_paths_via_finder(paths_still_on_network)
            for path in reappeared_paths:
                evidence.finder_recheck_errors.add(path)
                self.send_errors[path] = (
                    "Source reappeared during Trash operation"
                )
            for failure in recheck_failures:
                evidence.finder_recheck_errors.add(failure["path"])
                self.send_errors[failure["path"]] = failure["error"]
        except subprocess.TimeoutExpired:
            for path in paths_still_on_network:
                evidence.finder_recheck_errors.add(path)
                self.send_errors[path] = (
                    "Finder existence check timed out"
                )
        except Exception as exc:  # broad: Finder's AppleScript bridge raises undocumented errors
            # Fail safe: every path in the batch is reported as a
            # send error, so nothing is pruned on a failed recheck.
            log.warning("Finder existence recheck failed", exc_info=True)
            for path in paths_still_on_network:
                evidence.finder_recheck_errors.add(path)
                self.send_errors[path] = (
                    str(exc) or "Finder existence check failed"
                )

    def _settle_finder_missing(
        self, finder_missing_paths: set[str], evidence: _FinderMissingEvidence,
    ) -> None:
        for missing_path in finder_missing_paths:
            if self._finder_missing_is_trustworthy(missing_path, evidence):
                self._mark_already_missing(missing_path)
            else:
                if missing_path not in evidence.finder_recheck_errors:
                    self.send_errors[missing_path] = (
                        "Source path is unreachable"
                    )
                log.warning(
                    "Rejecting Finder 'missing' outcome for %s: "
                    "underlying mount appears to have detached",
                    missing_path,
                )

    def _finder_missing_is_trustworthy(self, filepath: str, evidence: _FinderMissingEvidence) -> bool:
        """Reject Finder's "missing" outcome when the underlying mount is gone.

        Finder reports "missing" when ``sourceExists=false`` but
        ``parentExists=true``. An unmounted network volume leaves its
        mount-point directory in place on the underlying local FS, so the
        parent-exists check alone cannot distinguish a genuine delete from a
        silently detached mount. Accepting "missing" in that case would
        prune the catalog row for a photo that reappears on remount.

        For network-classified paths we require three independent signals:
        (1) the mount table (already re-queried once per Finder batch by
        the caller and passed in via ``current_network_roots``) still
        lists a root the path resolves into; (2) an out-of-process
        ``stat`` probe on the *deepest* matching root the path resolves
        into — separate from Finder's cache — responds within its bounded
        timeout, so a still-listed but unreachable SMB server cannot
        masquerade as "empty" and, crucially, a reachable ancestor mount
        cannot vouch for a nested inner mount that is actually gone; and
        (3) Finder's second-look ``exists`` query also confirmed the path
        as missing. Doing the mount recheck and reachability probes per
        batch instead of per path keeps the worst-case cost bounded — a
        20-item retry of already-moved paths would otherwise spawn one
        ``mount`` (or ``stat``) subprocess per path. For local fallbacks
        we reuse the parent-device snapshot check that guards the
        send2trash path already.
        """
        if filepath in self.network_finder_candidates:
            current_network_roots = evidence.batch_network_roots
            if current_network_roots is None:
                # Mount discovery failed on the recheck — we cannot
                # confirm the volume is still mounted, so refuse to trust
                # "missing" and let the caller retry.
                return False
            if not volumes.path_on_network_volume(filepath, current_network_roots):
                return False
            if filepath not in evidence.confirmed_network_missing:
                return False
            # Require the *exact* mount the path depends on — not just any
            # reachable ancestor — to respond to the out-of-process stat
            # probe. When a detached inner share is nested beneath a
            # reachable outer share (e.g. ``/Volumes/NAS/archive`` under a
            # still-live ``/Volumes/NAS``), only the deepest match tells us
            # whether the photo could reappear on reconnect.
            deepest_root = evidence.path_to_deepest_root.get(filepath)
            if deepest_root is None:
                return False
            return deepest_root in evidence.reachable_network_roots
        return volumes.path_confirmed_gone(filepath, self.parent_devs.get(filepath))

    def collect_failures(self) -> list[JsonDict]:
        failures = []
        for filepath in self.ordered:
            if filepath in self.successful:
                continue
            error = (
                self.preflight_errors.get(filepath)
                or self.send_errors.get(filepath)
                or "Trash operation failed"
            )
            failures.append({"path": filepath, "error": error})
            log.warning("Trash failed for %s: %s", filepath, error)
        return failures
