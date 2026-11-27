"""Removing a moved folder's originals: only what verification covered.

Deleting the whole source tree removes whatever is in it at that moment. A
file written into the source after the copy was verified (a sidecar from a
metadata sync, a photo dropped in from another app) was never copied, and
removing the tree would destroy its only copy.

``record_source_tree`` lists the source just before verification starts.
The verifiers fail the move for any entry in that list that is missing or
different at the destination, so once verification passes every recorded
entry has a proven copy. ``remove_verified_originals`` deletes an entry only
when it is in the record and its ``lstat`` identity (inode, size, mtime,
ctime, mode) is unchanged since, then removes the directories that emptied.
Everything else stays where it is and is named in the result.

On Windows ``st_ctime`` is the creation time, so there the identity cannot
see a file rewritten in place after verification with the same size and its
modification time put back. Every other change (a different size or
modification time, or a replacement written through a temporary file, which
gets a new file index) is seen on every platform.
"""

import os
import stat
from dataclasses import dataclass, field

from .mounted_verify import _file_signature
from .transfer import FINDER_METADATA_FILES

# How many names a report carries. The totals are always exact.
LEFTOVER_NAME_LIMIT = 20


@dataclass
class SourceCleanup:
    """What ``remove_verified_originals`` did to one source tree.

    ``kept`` are entries left in place because verification never covered
    them or they changed after it; ``failed`` are ``(name, reason)`` pairs
    for entries that should have been deleted and could not be. Names are
    paths relative to the source folder, in sorted order.
    """

    removed: int = 0
    kept: list[str] = field(default_factory=list)
    failed: list[tuple[str, str]] = field(default_factory=list)

    @property
    def complete(self):
        return not self.kept and not self.failed

    def describe(self):
        """One sentence per kind of leftover, or None when nothing is left."""
        parts = []
        if self.kept:
            parts.append(
                f"{_count(len(self.kept), 'file')} in the source folder "
                f"{'was' if len(self.kept) == 1 else 'were'} added or changed "
                f"after the copy was verified, so Vireo did not delete "
                f"{'it' if len(self.kept) == 1 else 'them'}: {_names(self.kept)}")
        if self.failed:
            parts.append(
                f"{_count(len(self.failed), 'copied original')} could not be "
                f"deleted: {_names(self._failed_lines())}")
        return ". ".join(parts) or None

    def report(self):
        """The JSON-safe form for a job result: totals plus bounded names."""
        return {
            "kept_total": len(self.kept),
            "kept": self.kept[:LEFTOVER_NAME_LIMIT],
            "failed_total": len(self.failed),
            "failed": self._failed_lines()[:LEFTOVER_NAME_LIMIT],
        }

    def _failed_lines(self):
        return [f"{name} ({reason})" for name, reason in self.failed]


def _count(number, noun):
    return f"{number} {noun}{'' if number == 1 else 's'}"


def _names(names):
    shown = ", ".join(names[:LEFTOVER_NAME_LIMIT])
    extra = len(names) - LEFTOVER_NAME_LIMIT
    return f"{shown}, and {extra} more" if extra > 0 else shown


def _raise(error):
    raise error


def _entries(root, directories, files):
    """The names in one directory a move copies as single entries.

    ``os.walk`` lists a symlink to a directory among the directories and
    never descends into it; the copy and the verifiers treat it as one
    opaque link, so it is an entry here too.
    """
    links = [name for name in directories
             if os.path.islink(os.path.join(root, name))]
    return [*files, *links]


def record_source_tree(src_path):
    """Map each entry under ``src_path`` to its ``lstat`` identity.

    Keys are paths relative to ``src_path``. A directory that cannot be
    listed raises ``OSError``: an entry missing from the record would be
    left behind later, but a silent gap here would also hide that the
    verifier may not have seen it either.
    """
    record = {}
    for root, directories, files in os.walk(src_path, onerror=_raise):
        for name in _entries(root, directories, files):
            path = os.path.join(root, name)
            record[os.path.relpath(path, src_path)] = _file_signature(path)
    return record


def _is_finder_metadata(path, name):
    """A regular ``.DS_Store``: Finder's window layout, not the user's data.

    Finder rewrites it whenever the folder is open, so it routinely changes
    or appears during a move, and a merge never copies it in the first
    place. It is discarded with the source rather than reported as a file
    the move left behind.
    """
    if name not in FINDER_METADATA_FILES:
        return False
    try:
        return stat.S_ISREG(os.lstat(path).st_mode)
    except OSError:
        return False


def _remove_entry(path, relative, name, record, cleanup):
    """Delete one source entry if it is the one verification covered."""
    try:
        unchanged = record.get(relative) == _file_signature(path)
    except FileNotFoundError:
        return
    except OSError as exc:
        cleanup.failed.append((relative, str(exc)))
        return
    if not unchanged and not _is_finder_metadata(path, name):
        cleanup.kept.append(relative)
        return
    try:
        os.unlink(path)
    except FileNotFoundError:
        return
    except OSError as exc:
        cleanup.failed.append((relative, str(exc)))
        return
    cleanup.removed += 1


def remove_verified_originals(src_path, record, progress=None):
    """Delete the recorded, unchanged entries under ``src_path``.

    Walks bottom-up so each directory is removed once its contents are
    gone. A directory still holding something is left in place; the entry
    that holds it open is already in ``kept`` or ``failed``. ``progress``,
    when given, is called as ``progress(done, total, name)`` before each
    entry and once more, with an empty name, after the last.

    Returns a ``SourceCleanup``. Never raises for a single entry: one
    undeletable file must not stop the rest of the cleanup, since the
    catalog already points at the destination.
    """
    cleanup = SourceCleanup()
    total = len(record)
    unreadable = []
    done = 0
    for root, directories, files in os.walk(
            src_path, topdown=False, onerror=unreadable.append):
        for name in _entries(root, directories, files):
            path = os.path.join(root, name)
            relative = os.path.relpath(path, src_path)
            if progress:
                progress(done, max(total, done + 1), name)
            done += 1
            _remove_entry(path, relative, name, record, cleanup)
        _remove_directory(root, src_path, cleanup)
    if progress:
        progress(done, max(total, done), "")
    for error in unreadable:
        relative = os.path.relpath(error.filename or src_path, src_path)
        cleanup.failed.append((relative, error.strerror or str(error)))
    _keep_unreported_entries(src_path, cleanup)
    cleanup.kept.sort()
    cleanup.failed.sort()
    return cleanup


def _keep_unreported_entries(src_path, cleanup):
    """Name anything still in the source that the pass above did not report.

    A file written while the cleanup ran lands in a directory the walk had
    already listed. It keeps that directory in place, so the report must
    name it or the user is left with a folder and no explanation.
    """
    if not os.path.lexists(src_path):
        return
    reported = set(cleanup.kept)
    reported.update(name for name, _reason in cleanup.failed)
    for root, directories, files in os.walk(src_path, onerror=lambda _error: None):
        for name in _entries(root, directories, files):
            relative = os.path.relpath(os.path.join(root, name), src_path)
            if relative not in reported:
                cleanup.kept.append(relative)


def _remove_directory(directory, src_path, cleanup):
    """Remove ``directory`` if it emptied; report it only if it should have."""
    try:
        os.rmdir(directory)
    except FileNotFoundError:
        return
    except OSError as exc:
        try:
            still_holds_entries = bool(os.listdir(directory))
        except OSError:
            still_holds_entries = False
        if still_holds_entries:
            return
        relative = os.path.relpath(directory, src_path)
        cleanup.failed.append((
            "the source folder itself" if relative == "." else relative, str(exc)))
