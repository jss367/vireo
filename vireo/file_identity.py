"""Filesystem identity checks used before cataloging and destructive cleanup."""

import os
import threading
from concurrent.futures import Future, TimeoutError

_lock = threading.Lock()
_probes = {}


def catalog_folder_aliases(db):
    """Build a lexical lookup once for a scan, without probing every folder."""
    aliases = {}
    for row in db.conn.execute("SELECT path FROM folders"):
        aliases.setdefault(os.path.normpath(row["path"]).casefold(), []).append(row["path"])
    return aliases


def catalog_folder_path(db, path, *, aliases=None):
    """Normalize aliases and reuse the spelling of an existing catalog folder."""
    supplied = os.fspath(path)
    if aliases is None:
        exact = db.conn.execute("SELECT path FROM folders WHERE path=?", (supplied,)).fetchone()
        if exact:
            return exact["path"]
        aliases = catalog_folder_aliases(db)
    elif supplied in aliases.get(os.path.normpath(supplied).casefold(), ()):
        return supplied
    canonical = os.path.realpath(os.path.abspath(supplied))
    candidates = aliases.get(os.path.normpath(canonical).casefold(), ())
    for existing in candidates:
        try:
            if os.path.samefile(existing, canonical):
                return existing
        except OSError:
            if existing == canonical:
                return existing
    return canonical


def distinct_existing_file(path, other, *, timeout=None, allow_missing_source=False):
    """Prove two regular files are distinct; errors/timeouts fail closed.

    Network calls run in capped daemon probes. A wedged mount must neither
    hang a request nor accumulate an unbounded number of worker threads.
    ``allow_missing_source`` permits an absent source with an existing winner
    to proceed to a separate bounded missing-file check. It does not prove
    that the source's catalog row can be removed.
    """
    def check():
        import stat

        try:
            b = os.stat(other)
            if not stat.S_ISREG(b.st_mode):
                return False
            try:
                a = os.stat(path)
            except FileNotFoundError:
                return allow_missing_source
            return (stat.S_ISREG(a.st_mode)
                    and (a.st_dev, a.st_ino) != (b.st_dev, b.st_ino))
        except OSError:
            return False

    if timeout is None:
        return check()
    key = (path, other, allow_missing_source)
    with _lock:
        future = _probes.get(key)
        if future is None:
            if len(_probes) >= 4:
                return False
            future = Future()
            _probes[key] = future

            def worker():
                try:
                    future.set_result(check())
                except BaseException as exc:
                    future.set_exception(exc)
                finally:
                    with _lock:
                        _probes.pop(key, None)

            threading.Thread(target=worker, daemon=True).start()
    try:
        return future.result(timeout=timeout)
    except (TimeoutError, OSError):
        return False
