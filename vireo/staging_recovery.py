"""Verified recovery helpers for abandoned pipeline staging folders.

Old import-through-process runs staged photos under
``<vireo_dir>/staging/pipeline-*`` before archiving them. If a run failed or
was cancelled, those folders may be the only remaining copy of files that did
not make it to the archive. This module never deletes based on catalog state
alone. The catalog only names where a staged file's archive copy should be:
a staged file counts as verified when the archive file at that path is a
different file with the same bytes, compared in full.

Cleanup removes verified files one at a time, each right after its own
comparison, and then the directories that are empty. Anything the pass could
not read, could not match, or does not understand (a link, a special file,
an unreadable subfolder) stays on disk and is reported.
"""

from __future__ import annotations

import os
import stat
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import IO, TYPE_CHECKING, Any

if TYPE_CHECKING:
    from db import Database

# Both files are read in chunks of this size, so memory stays flat for any
# file size and a paused or cancelled job is noticed within one chunk.
COMPARE_CHUNK_BYTES = 4 * 1024 * 1024

# Entries that are not verified are returned by name up to this many; the
# counts always cover all of them. A job result is re-serialized on every
# status poll, so it must not grow with the folder.
DETAIL_LIMIT = 200

PauseCallback = Callable[[], None]
ProgressCallback = Callable[[int, int, str], None]


def _missing_mount_root(path: str) -> str | None:
    """Return a likely unavailable mount root for common archive locations."""
    posix = os.path.expanduser(path).replace("\\", "/")
    parts = posix.split("/")
    if len(parts) >= 3 and parts[0] == "" and parts[1] in {"Volumes", "mnt"}:
        root = f"/{parts[1]}/{parts[2]}"
        return root if not os.path.lexists(root) else None
    if len(parts) >= 4 and parts[0] == "" and parts[1] == "media":
        root = f"/media/{parts[2]}/{parts[3]}"
        return root if not os.path.lexists(root) else None
    return None


def _is_relative_to(path: str, root: str) -> bool:
    try:
        return os.path.commonpath([
            os.path.realpath(path),
            os.path.realpath(root),
        ]) == os.path.realpath(root)
    except (OSError, ValueError):
        return False


@dataclass(frozen=True)
class StagingEntry:
    cleanup_root: str
    source_root: str


def staging_base(vireo_dir: str) -> str:
    return os.path.join(vireo_dir, "staging")


def _entry_for_pipeline_dir(
    path: str, pause_callback: PauseCallback | None = None,
) -> StagingEntry:
    """Return cleanup root and likely import source root for one pipeline dir."""
    children = []
    direct_files = []
    try:
        with os.scandir(path) as entries:
            for entry in entries:
                if pause_callback:
                    pause_callback()
                if entry.is_dir():
                    children.append(entry.path)
                elif entry.is_file():
                    direct_files.append(entry.path)
    except OSError:
        pass
    # local_processing.staging_root created pipeline-*/<final-destination-name>.
    # If the parent contains exactly that one leaf and no direct files, use the
    # leaf as the import source so a recovery import preserves the original
    # destination base semantics.
    source_root = children[0] if len(children) == 1 and not direct_files else path
    return StagingEntry(cleanup_root=path, source_root=source_root)


def discover_orphaned_staging(vireo_dir: str) -> list[dict]:
    """List abandoned pipeline staging roots with cheap file/size summaries."""
    base = staging_base(vireo_dir)
    if not os.path.isdir(base):
        return []
    entries: list[dict] = []
    for name in sorted(os.listdir(base)):
        if not name.startswith("pipeline-"):
            continue
        cleanup_root = os.path.join(base, name)
        if not os.path.isdir(cleanup_root):
            continue
        entry = _entry_for_pipeline_dir(cleanup_root)
        file_count = 0
        total_bytes = 0
        # Count what a verification pass compares: regular files anywhere
        # under the cleanup root, links not followed.
        for root, _dirs, files in os.walk(cleanup_root):
            for filename in files:
                path = os.path.join(root, filename)
                try:
                    st = os.lstat(path)
                except OSError:
                    continue
                if not stat.S_ISREG(st.st_mode):
                    continue
                file_count += 1
                total_bytes += st.st_size
        entries.append({
            "path": entry.cleanup_root,
            "source_root": entry.source_root,
            "name": os.path.basename(entry.cleanup_root),
            "file_count": file_count,
            "bytes": total_bytes,
        })
    return entries


def _resolve_entry(
    vireo_dir: str, cleanup_root: str, pause_callback: PauseCallback | None = None,
) -> StagingEntry:
    base = staging_base(vireo_dir)
    root = os.path.realpath(cleanup_root)
    if not _is_relative_to(root, base):
        raise ValueError("staging path is outside the Vireo staging directory")
    # Only the retired flow's own folders, directly under the staging base:
    # the base also holds the staging trees of imports still waiting for
    # their archive transfer, which are not this module's to clean.
    if (
        not os.path.basename(root).startswith("pipeline-")
        or os.path.dirname(root) != os.path.realpath(base)
    ):
        raise ValueError("staging path is not a pipeline staging directory")
    if not os.path.isdir(root):
        raise ValueError("staging path was not found")
    return _entry_for_pipeline_dir(root, pause_callback=pause_callback)


def _catalog_candidates(
    db: Database, filename: str, size: int, staging_root: str,
) -> list[dict[str, Any]]:
    ws_id = db.require_workspace_id()
    rows = db.photo_visibility.name_size_matches(ws_id, filename, size)
    out = []
    for row in rows:
        folder_path = row["folder_path"]
        if folder_path and _is_relative_to(folder_path, staging_root):
            continue
        out.append(dict(row))
    return out


def _archive_file_status(
    path: str,
    expected_size: int,
    listing_cache: dict[str, set[str] | OSError] | None = None,
) -> tuple[str, str | None]:
    """Whether a file of ``expected_size`` is listed at ``path``.

    ``present`` says only that: the caller compares contents before it calls
    the staged file verified.
    """
    mount_root = _missing_mount_root(path) or _missing_mount_root(os.path.dirname(path))
    if mount_root:
        return "unreachable", f"archive mount root is unavailable: {mount_root}"
    folder = os.path.dirname(path)
    cache = listing_cache if listing_cache is not None else {}
    cached = cache.get(folder)
    if cached is None:
        try:
            cached = set(os.listdir(folder))
        except OSError as exc:
            cached = exc
        cache[folder] = cached
    if isinstance(cached, OSError):
        return "unreachable", f"archive folder is not enumerable: {folder} ({cached})"
    names = cached
    basename = os.path.basename(path)
    if basename not in names:
        return "missing", f"archive folder is reachable but {basename} is not present"
    try:
        st = os.stat(path)
    except OSError as exc:
        return "unreachable", f"archive file could not be stat'ed: {path} ({exc})"
    if st.st_size != expected_size:
        return "size_mismatch", (
            f"archive file size differs: expected {expected_size}, got {st.st_size}"
        )
    return "present", None


def _path_parts(path: str) -> list[str]:
    normalized = os.path.normpath(path).replace("\\", "/")
    return [part for part in normalized.split("/") if part and part != "."]


def _expected_archive_suffix(entry: StagingEntry, staged_file: str) -> list[str]:
    rel_dir = os.path.relpath(os.path.dirname(staged_file), entry.source_root)
    parts: list[str] = []
    if os.path.realpath(entry.source_root) != os.path.realpath(entry.cleanup_root):
        parts.append(os.path.basename(entry.source_root))
    if rel_dir not in ("", "."):
        parts.extend(_path_parts(rel_dir))
    return parts


def _folder_matches_staged_path(folder_path: str, expected_suffix: list[str]) -> bool:
    if not expected_suffix:
        return True
    folder_parts = _path_parts(folder_path)
    return (
        len(folder_parts) >= len(expected_suffix)
        and folder_parts[-len(expected_suffix):] == expected_suffix
    )


def _infer_destination(source_root: str, staged_file: str, folder_path: str) -> str:
    rel_dir = os.path.relpath(os.path.dirname(staged_file), source_root)
    if rel_dir in ("", "."):
        return folder_path
    rel_parts = [p for p in rel_dir.split(os.sep) if p and p != "."]
    folder_parts = os.path.normpath(folder_path).split(os.sep)
    if len(folder_parts) >= len(rel_parts) and folder_parts[-len(rel_parts):] == rel_parts:
        base_parts = folder_parts[:-len(rel_parts)]
        if not base_parts:
            return os.sep
        return os.sep.join(base_parts) or os.sep
    return folder_path


@dataclass(frozen=True)
class StagedFile:
    """A regular file found in the staging folder, as it was when listed."""

    path: str
    rel_path: str
    size: int
    # (device, inode, size, mtime_ns). A file is compared and deleted only
    # while it still has this identity.
    identity: tuple[int, int, int, int]


@dataclass
class StagingScan:
    """What one walk of a staging folder found."""

    files: list[StagedFile] = field(default_factory=list)
    # Entries the walk could not read or will not touch, as detail dicts.
    unchecked: list[dict[str, Any]] = field(default_factory=list)
    # Directories that were listed in full, each parent before its children.
    # Only these are candidates for removal once they are empty.
    directories: list[str] = field(default_factory=list)


def _identity(st: os.stat_result) -> tuple[int, int, int, int]:
    return (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns)


def _rel_path(entry: StagingEntry, path: str) -> str:
    """Path shown to the user: relative to the import source when inside it."""
    try:
        inside = os.path.commonpath([path, entry.source_root]) == entry.source_root
    except ValueError:
        inside = False
    rel = os.path.relpath(path, entry.source_root if inside else entry.cleanup_root)
    return os.path.basename(path) if rel == "." else rel


def _unchecked(entry: StagingEntry, path: str, reason: str) -> dict[str, Any]:
    return {
        "path": path,
        "rel_path": _rel_path(entry, path),
        "size": None,
        "status": "unchecked",
        "reason": reason,
        "archive_path": None,
    }


def _classify_entry(
    entry: StagingEntry, path: str, scan: StagingScan,
    found: list[StagedFile], pending: list[str],
) -> None:
    """Sort one directory entry into a file to compare, a folder to walk, or
    something the pass leaves alone and reports."""
    try:
        st = os.lstat(path)
    except OSError as exc:
        scan.unchecked.append(_unchecked(entry, path, f"could not be examined ({exc})"))
        return
    if stat.S_ISLNK(st.st_mode) or os.path.isjunction(path):
        # Following a link could reach files outside the staging folder, and
        # the link's target is not a staged copy.
        scan.unchecked.append(_unchecked(
            entry, path, "is a link, which Vireo does not follow or delete",
        ))
    elif stat.S_ISDIR(st.st_mode):
        pending.append(path)
    elif stat.S_ISREG(st.st_mode):
        found.append(StagedFile(path, _rel_path(entry, path), st.st_size, _identity(st)))
    else:
        scan.unchecked.append(_unchecked(entry, path, "is not a regular file"))


def _scan_staging(entry: StagingEntry, pause: PauseCallback) -> StagingScan:
    """List everything under the cleanup root, pausing before each entry.

    Unlike photo discovery, recovery includes hidden and non-photo files.
    A folder that cannot be listed is reported and never removed, so nothing
    inside it is deleted unseen.
    """
    scan = StagingScan()
    pending = [entry.cleanup_root]
    while pending:
        pause()
        root = pending.pop()
        found: list[StagedFile] = []
        try:
            with os.scandir(root) as entries:
                for item in entries:
                    pause()
                    _classify_entry(entry, item.path, scan, found, pending)
            scan.directories.append(root)
        except OSError as exc:
            scan.unchecked.append(_unchecked(
                entry, root, f"folder could not be read ({exc})",
            ))
        scan.files.extend(sorted(found, key=lambda staged: staged.path))
    return scan


def _compare_streams(
    staged_file: IO[bytes], archive_file: IO[bytes], pause: PauseCallback,
) -> tuple[str, str | None]:
    """Read both files to the end, stopping at the first difference.

    A failed read of the staged file is answered here; a failed read of the
    archive file is left to the caller, which knows that path.
    """
    while True:
        pause()
        try:
            staged_chunk = staged_file.read(COMPARE_CHUNK_BYTES)
        except OSError as exc:
            return "unchecked", f"staged file could not be read ({exc})"
        archive_chunk = archive_file.read(COMPARE_CHUNK_BYTES)
        if staged_chunk != archive_chunk:
            return "unaccounted", (
                "the archive file with this name and size has different contents; "
                "keep this copy until you have compared the two"
            )
        if not staged_chunk:
            return "verified", None


def _compare_contents(
    staged: StagedFile, archive_path: str, pause: PauseCallback,
) -> tuple[str, str | None]:
    """Compare the staged file with the archive file, byte for byte."""
    try:
        with open(staged.path, "rb") as staged_file:
            if _identity(os.fstat(staged_file.fileno())) != staged.identity:
                return "unchecked", "staged file changed while it was being checked"
            try:
                with open(archive_path, "rb") as archive_file:
                    return _compare_streams(staged_file, archive_file, pause)
            except OSError as exc:
                return "unreachable", (
                    f"archive file could not be read: {archive_path} ({exc})"
                )
    except OSError as exc:
        return "unchecked", f"staged file could not be read ({exc})"


def _archive_copy_status(
    staged: StagedFile,
    archive_path: str,
    listing_cache: dict[str, set[str] | OSError],
    pause: PauseCallback,
) -> tuple[str, str | None]:
    """Whether ``archive_path`` holds a second copy of the staged file."""
    status, reason = _archive_file_status(archive_path, staged.size, listing_cache)
    if status != "present":
        return status, reason
    try:
        archive_stat = os.stat(archive_path)
    except OSError as exc:
        return "unreachable", f"archive file could not be stat'ed: {archive_path} ({exc})"
    if (archive_stat.st_dev, archive_stat.st_ino) == staged.identity[:2]:
        # A catalog row or a link that leads back into staging names the
        # same file twice; deleting it would delete the only copy.
        return "unaccounted", (
            "the cataloged archive path is this staged file itself, not a second copy"
        )
    return _compare_contents(staged, archive_path, pause)


def _delete_compared_file(staged: StagedFile) -> tuple[str, str | None]:
    """Remove a file that has just compared equal to its archive copy.

    The file is kept when it no longer has the identity it had when it was
    listed: the comparison then says nothing about what is on disk now.
    """
    try:
        if _identity(os.lstat(staged.path)) != staged.identity:
            return "unchecked", "staged file changed after it was compared, so it was kept"
        os.unlink(staged.path)
    except FileNotFoundError:
        return "gone", None
    except OSError as exc:
        return "delete_failed", f"matches the archive but could not be deleted ({exc})"
    return "deleted", None


def _remove_empty_directories(directories: list[str]) -> list[tuple[str, str]]:
    """Remove the listed directories that are empty, children first.

    ``os.rmdir`` refuses a directory that still holds anything, so a file
    that was kept, or one that appeared after the walk, keeps its folders.
    Returns ``(path, error)`` for each directory that stayed.
    """
    failures: list[tuple[str, str]] = []
    for directory in reversed(directories):
        try:
            os.rmdir(directory)
        except FileNotFoundError:
            continue
        except OSError as exc:
            failures.append((directory, str(exc)))
    return failures


_PROBLEM_STATUSES = ("unaccounted", "unreachable", "unchecked", "delete_failed")


class _StagingPass:
    """One walk of a staging folder that compares every file with the archive.

    With ``delete`` set, each file is removed right after its own comparison,
    so nothing is deleted on the strength of an earlier pass.
    """

    def __init__(
        self, db: Database, entry: StagingEntry, *, delete: bool,
        pause_callback: PauseCallback | None,
        progress_callback: ProgressCallback | None,
    ) -> None:
        self.db = db
        self.entry = entry
        self.delete = delete
        self.pause: PauseCallback = pause_callback or (lambda: None)
        self.progress = progress_callback
        self.counts = dict.fromkeys(("verified", *_PROBLEM_STATUSES), 0)
        self.verified_bytes = 0
        self.deleted = 0
        self.deleted_bytes = 0
        self.problems: list[dict[str, Any]] = []
        self.inferred: dict[str, int] = {}
        self.listing_cache: dict[str, set[str] | OSError] = {}

    def run(self) -> dict[str, Any]:
        scan = _scan_staging(self.entry, self.pause)
        for detail in scan.unchecked:
            self._record(detail)
        total = len(scan.files)
        for index, staged in enumerate(scan.files, 1):
            self.pause()
            detail = self._check(staged)
            if self.delete and detail["status"] == "verified":
                detail["status"], detail["reason"] = _delete_compared_file(staged)
            self._record(detail)
            if self.progress:
                self.progress(index, total, staged.rel_path)
        result = self._result(scan)
        if self.delete:
            self._finish_delete(result, scan)
        return result

    def _check(self, staged: StagedFile) -> dict[str, Any]:
        """Find the archive copy the catalog names and compare it in full."""
        detail: dict[str, Any] = {
            "path": staged.path,
            "rel_path": staged.rel_path,
            "size": staged.size,
            "status": "unaccounted",
            "reason": "no cataloged archive photo matches this filename and size",
            "archive_path": None,
        }
        candidates = _catalog_candidates(
            self.db, os.path.basename(staged.path), staged.size, self.entry.cleanup_root,
        )
        expected_suffix = _expected_archive_suffix(self.entry, staged.path)
        saw_unreachable = False
        reason = None
        for candidate in candidates:
            folder_path = candidate["folder_path"]
            if not _folder_matches_staged_path(folder_path, expected_suffix):
                reason = reason or (
                    "a cataloged photo has this filename and size, but in a "
                    "different folder than this staging path maps to"
                )
                continue
            archive_path = os.path.join(folder_path, candidate["filename"])
            status, why = _archive_copy_status(
                staged, archive_path, self.listing_cache, self.pause,
            )
            if status == "verified":
                detail.update(status="verified", reason=None, archive_path=archive_path)
                dest = _infer_destination(self.entry.source_root, staged.path, folder_path)
                self.inferred[dest] = self.inferred.get(dest, 0) + 1
                return detail
            if status == "unchecked":
                # The staged file itself could not be read, so no other
                # candidate can be compared with it either.
                detail.update(status="unchecked", reason=why)
                return detail
            saw_unreachable = saw_unreachable or status == "unreachable"
            reason = why
        if saw_unreachable:
            detail["status"] = "unreachable"
        if reason:
            detail["reason"] = reason
        return detail

    def _record(self, detail: dict[str, Any]) -> None:
        status = detail["status"]
        size = detail["size"] or 0
        if status in ("verified", "deleted", "delete_failed", "gone"):
            self.counts["verified"] += 1
            self.verified_bytes += size
        if status == "deleted":
            self.deleted += 1
            self.deleted_bytes += size
        if status in _PROBLEM_STATUSES:
            self.counts[status] += 1
            if len(self.problems) < DETAIL_LIMIT:
                self.problems.append(detail)

    def _status(self) -> str:
        for status, name in (
            ("delete_failed", "delete_failed"),
            ("unreachable", "unreachable"),
            ("unaccounted", "needs_import"),
            ("unchecked", "unchecked"),
        ):
            if self.counts[status]:
                return name
        return "safe_to_delete"

    def _result(self, scan: StagingScan) -> dict[str, Any]:
        status = self._status()
        problem_total = sum(self.counts[name] for name in _PROBLEM_STATUSES)
        inferred = max(self.inferred.items(), key=lambda kv: kv[1])[0] if self.inferred else None
        return {
            "path": self.entry.cleanup_root,
            "source_root": self.entry.source_root,
            "name": os.path.basename(self.entry.cleanup_root),
            "file_count": len(scan.files),
            "bytes": sum(staged.size for staged in scan.files),
            "verified": self.counts["verified"],
            "verified_bytes": self.verified_bytes,
            "unaccounted": self.counts["unaccounted"],
            "unreachable": self.counts["unreachable"],
            "unchecked": self.counts["unchecked"],
            "details": self.problems,
            "details_total": problem_total,
            "can_delete": status == "safe_to_delete",
            "status": status,
            "inferred_destination": inferred,
        }

    def _finish_delete(self, result: dict[str, Any], scan: StagingScan) -> None:
        """Remove the emptied directories and describe what the delete did."""
        failures = _remove_empty_directories(scan.directories)
        folder_removed = not os.path.lexists(self.entry.cleanup_root)
        if not folder_removed and not result["details_total"] and failures:
            # Every file matched and was removed, yet a directory stayed:
            # say which one and why, so "kept" is never unexplained.
            path, error = failures[0]
            self._record(_unchecked(
                self.entry, path, f"folder could not be removed ({error})",
            ))
            result.update(self._result(scan))
        failed = [d for d in self.problems if d["status"] == "delete_failed"]
        result.update({
            "can_delete": False,
            "status": "deleted" if folder_removed else result["status"],
            "deleted": self.deleted,
            "deleted_bytes": self.deleted_bytes,
            "delete_failed": self.counts["delete_failed"],
            "kept": result["details_total"],
            "folder_removed": folder_removed,
            "ok": not self.counts["delete_failed"],
            "errors": [f"{d['rel_path']}: {d['reason']}" for d in failed[:50]],
            "errors_total": self.counts["delete_failed"],
        })


def resolve_staging_root(vireo_dir: str, cleanup_root: str) -> str:
    """Return the real path of a pipeline staging folder, or raise ValueError
    when ``cleanup_root`` is not one under this installation's staging base."""
    return _resolve_entry(vireo_dir, cleanup_root).cleanup_root


def verify_orphaned_staging(
    db: Database, vireo_dir: str, cleanup_root: str,
    pause_callback: PauseCallback | None = None,
    progress_callback: ProgressCallback | None = None,
) -> dict[str, Any]:
    """Compare every staged file with its archive copy; change nothing.

    ``verified`` counts the files whose archive copy has the same bytes.
    ``can_delete`` is true only when that is every entry in the folder.
    """
    entry = _resolve_entry(vireo_dir, cleanup_root, pause_callback=pause_callback)
    return _StagingPass(
        db, entry, delete=False,
        pause_callback=pause_callback, progress_callback=progress_callback,
    ).run()


def delete_verified_staging(
    db: Database, vireo_dir: str, cleanup_root: str,
    pause_callback: PauseCallback | None = None,
    progress_callback: ProgressCallback | None = None,
) -> dict[str, Any]:
    """Delete the staged files that match the archive, and nothing else.

    Runs its own comparison: each file is deleted right after it compares
    equal to its archive copy, and every other entry stays. The folder is
    removed only when that leaves it empty (``folder_removed``); otherwise
    ``details`` names what was kept and why. ``ok`` is false when a file
    that matched could not be deleted.
    """
    entry = _resolve_entry(vireo_dir, cleanup_root, pause_callback=pause_callback)
    return _StagingPass(
        db, entry, delete=True,
        pause_callback=pause_callback, progress_callback=progress_callback,
    ).run()
