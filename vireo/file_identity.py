"""Filesystem identity checks used before cataloging and destructive cleanup."""

import os
import threading
from concurrent.futures import Future, TimeoutError

_lock = threading.Lock()
_probes = {}


def catalog_folder_aliases(db):
    """Build a lexical lookup once for a scan, without probing every folder."""
    aliases = {}
    for row in db.local_folders.catalog_paths():
        aliases.setdefault(os.path.normpath(row["path"]).casefold(), []).append(row["path"])
    return aliases


def catalog_folder_path(db, path, *, aliases=None):
    """Normalize aliases and reuse the spelling of an existing catalog folder."""
    supplied = os.fspath(path)
    if aliases is None:
        # ``folders.path`` uses binary collation, so an exact match is the
        # stored spelling itself.
        if db.folders.id_by_path(supplied) is not None:
            return supplied
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


def catalog_scan_root(db, path):
    """The catalog spelling ``scanner.scan`` uses for the scan root ``path``.

    Made absolute, then mapped by ``catalog_folder_path``: a case, symlink or
    separator alias of a cataloged folder becomes that folder's stored path.
    Code that later asks which folders a scan covered resolves the root here,
    so it matches the folder rows the scan wrote instead of the spelling the
    user typed.
    """
    return catalog_scan_roots(db, [path])[0]


def catalog_scan_roots(db, paths):
    """``catalog_scan_root`` for each of ``paths``, in order.

    Any code that looks up catalog rows by a folder the user supplied (a
    working-copy scope, a cache invalidation) maps the folder here first: a
    scan reached through a symlink, or a case alias of a cataloged folder,
    files its rows under the resolved or stored spelling, never the one the
    user picked. Paths already stored verbatim cost one indexed lookup; the
    alias table is built at most once, on the first path that is not.
    """
    aliases = None
    resolved = []
    for path in paths:
        supplied = os.path.abspath(os.fspath(path))
        if db.folders.id_by_path(supplied) is None:
            if aliases is None:
                aliases = catalog_folder_aliases(db)
            supplied = catalog_folder_path(db, supplied, aliases=aliases)
        resolved.append(supplied)
    return resolved


def catalog_file_paths(db, paths):
    """The catalog spelling of each file in ``paths``: its folder mapped by
    ``catalog_scan_roots``, its filename kept. For looking up the photo rows
    a scan wrote for files the caller knows by another spelling.
    """
    paths = [os.path.abspath(os.fspath(p)) for p in paths]
    folders = list(dict.fromkeys(os.path.dirname(p) for p in paths))
    spelled = dict(zip(folders, catalog_scan_roots(db, folders), strict=True))
    return [
        os.path.join(spelled[os.path.dirname(p)], os.path.basename(p))
        for p in paths
    ]


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
                    # broad: delivered to the waiting caller through the future.
                    future.set_exception(exc)
                finally:
                    with _lock:
                        _probes.pop(key, None)

            threading.Thread(target=worker, daemon=True).start()
    try:
        return future.result(timeout=timeout)
    except (TimeoutError, OSError):
        return False
