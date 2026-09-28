"""Directory listings reused while a directory's modification time holds.

Adding, removing or renaming an entry changes its parent directory's
modification time, so a directory whose ``st_mtime_ns``, inode and device
match the last time we listed it still holds exactly the names we saw then.
Inodes are only unique within a device, so ``st_dev`` is part of the
identity too: a NAS or removable drive remounted at the same path can
otherwise present the same ``st_ino`` and ``st_mtime_ns`` as the previous
volume while holding different files. The new-images walk and the
automatic missing-originals scan both re-read every library folder on a
timer; on an SMB share a cold listing runs at a few hundred entries a
second while a directory ``stat`` costs tens of milliseconds, so
re-listing only the directories that changed turns a many-minute pass
into seconds.

A cached listing holds raw entries (name, is-directory without following
symlinks, is-symlink) and no policy: each consumer applies its own filters
on every pass, and anything a listing cannot vouch for — a symlink's target
still existing, where it points — is re-checked live by the consumer.

Three guards keep a reused listing from hiding a change:

* A directory modified within :data:`RACY_SECONDS` of when we stat'ed it is
  not cached. Two things can make such a read disagree with the mtime it
  is filed under: a coarse-timestamp filesystem (FAT/exFAT cards record 2s)
  can take a second change inside the same tick without moving the mtime,
  and the macOS SMB client serves a directory's listing from its own cache
  for up to 60s (``dir_cache_max`` in ``nsmb.conf``), so a fresh ``stat``
  can sit beside a listing from before the change.
* Every listing is re-read after :data:`MAX_AGE_SECONDS`, so a change the
  mtime missed (a server that does not update it, a skewed clock) cannot
  stay hidden for more than a day.
* An explicit user recheck bypasses reuse (``ListingPass(reuse=False)`` or
  :meth:`DirListingCache.clear`), so "Check again" really looks again.
  :meth:`clear` also bumps a generation counter; a pass whose ``scandir``
  was already in flight at the clear can still call :meth:`store`, but
  its write is dropped because the generation it captured at :meth:`begin`
  no longer matches, so a stale listing cannot leak back into the cache.
"""
import os
import threading
import time
from collections import OrderedDict
from typing import NamedTuple

RACY_SECONDS = 120.0
MAX_AGE_SECONDS = 24 * 60 * 60
# Bounds memory: an entry costs roughly one string per file it lists.
MAX_DIRECTORIES = 100_000


class DirListing(NamedTuple):
    names: tuple
    dirs: frozenset
    symlinks: frozenset

    @classmethod
    def from_entries(cls, entries):
        """Build from ``(name, is_dir, is_symlink)`` triples."""
        names = []
        dirs = set()
        symlinks = set()
        for name, is_dir, is_symlink in entries:
            names.append(name)
            if is_dir:
                dirs.add(name)
            if is_symlink:
                symlinks.add(name)
        return cls(tuple(names), frozenset(dirs), frozenset(symlinks))

    def entries(self, dirpath):
        """Yield ``os.DirEntry``-shaped objects for a replayed listing."""
        for name in self.names:
            yield CachedEntry(
                name, os.path.join(dirpath, name),
                name in self.dirs, name in self.symlinks,
            )


class CachedEntry:
    """The subset of ``os.DirEntry`` the scan walkers use, from a listing."""

    __slots__ = ("name", "path", "_is_dir", "_is_symlink")

    def __init__(self, name, path, is_dir, is_symlink):
        self.name = name
        self.path = path
        self._is_dir = is_dir
        self._is_symlink = is_symlink

    def is_dir(self, follow_symlinks=True):
        if follow_symlinks and self._is_symlink:
            return os.path.isdir(self.path)
        return self._is_dir

    def is_symlink(self):
        return self._is_symlink


class _Stored(NamedTuple):
    mtime_ns: int
    ino: int
    dev: int
    listed_at: float  # monotonic
    listing: DirListing


def _key(path):
    return os.path.normpath(os.fspath(path))


class DirListingCache:
    def __init__(self, *, racy_seconds=RACY_SECONDS,
                 max_age_seconds=MAX_AGE_SECONDS,
                 max_directories=MAX_DIRECTORIES,
                 wall_clock=time.time, monotonic=time.monotonic):
        self._racy_seconds = racy_seconds
        self._max_age_seconds = max_age_seconds
        self._max_directories = max_directories
        self._wall_clock = wall_clock
        self._monotonic = monotonic
        self._entries = OrderedDict()
        self._lock = threading.Lock()
        # Bumped by :meth:`clear`. A store attempt tagged with an older
        # generation is dropped so a scan whose read began before the clear
        # cannot repopulate the cache with what is now a stale listing.
        self._generation = 0

    def snapshot_generation(self):
        with self._lock:
            return self._generation

    def lookup(self, path, st):
        """Return the listing stored for ``path`` if ``st`` still vouches
        for it, else None."""
        key = _key(path)
        with self._lock:
            stored = self._entries.get(key)
            if stored is None:
                return None
            if (
                stored.mtime_ns != st.st_mtime_ns
                or stored.ino != st.st_ino
                or stored.dev != st.st_dev
                or self._monotonic() - stored.listed_at > self._max_age_seconds
            ):
                del self._entries[key]
                return None
            self._entries.move_to_end(key)
            return stored.listing

    def store(self, path, st, stat_wall_time, entries, generation=None):
        """Remember ``entries`` — ``(name, is_dir, is_symlink)`` triples read
        after ``st`` was taken at ``stat_wall_time`` — unless the directory
        changed too recently for its mtime to be trusted. ``generation``, when
        given, must still match: an intervening :meth:`clear` bumps it, so a
        pass whose ``scandir`` was already in flight at the clear does not
        write its stale listing back."""
        key = _key(path)
        if stat_wall_time - st.st_mtime_ns / 1e9 < self._racy_seconds:
            with self._lock:
                if generation is not None and generation != self._generation:
                    return
                self._entries.pop(key, None)
            return
        stored = _Stored(
            st.st_mtime_ns, st.st_ino, st.st_dev, self._monotonic(),
            DirListing.from_entries(entries),
        )
        with self._lock:
            if generation is not None and generation != self._generation:
                return
            self._entries[key] = stored
            self._entries.move_to_end(key)
            while len(self._entries) > self._max_directories:
                self._entries.popitem(last=False)

    def clear(self):
        with self._lock:
            self._entries.clear()
            self._generation += 1

    def wall_clock(self):
        return self._wall_clock()


class ListingPass:
    """One scan's use of a :class:`DirListingCache`.

    Counts how many directories were read from disk and how many were
    reused unchanged, so the job can say which it did. ``reuse=False``
    reads every directory (an explicit recheck) but still records what it
    read for later passes. ``cache=None`` reads everything and records
    nothing.
    """

    def __init__(self, cache=None, reuse=True):
        self.cache = cache
        self.reuse = reuse and cache is not None
        self.read = 0
        self.unchanged = 0

    def begin(self, path):
        """``stat`` ``path`` and return ``(token, listing)``: ``listing`` is
        the reusable cached listing or None, and ``token`` must be handed to
        :meth:`finish` after a fresh read. Raises ``OSError`` from the stat."""
        if self.cache is not None:
            wall = self.cache.wall_clock()
            generation = self.cache.snapshot_generation()
        else:
            wall = time.time()
            generation = None
        st = os.stat(path)
        listing = self.cache.lookup(path, st) if self.reuse else None
        if listing is not None:
            self.unchanged += 1
        return (st, wall, generation), listing

    def finish(self, path, token, entries):
        """Record a complete fresh read of ``path``."""
        self.read += 1
        if self.cache is not None:
            st, wall, generation = token
            self.cache.store(path, st, wall, entries, generation=generation)

    def failed(self):
        """Count a fresh read that did not complete (nothing is stored)."""
        self.read += 1


def read_directory(path, listing_pass, on_entry=None):
    """List ``path`` through ``listing_pass``: the cached listing when the
    directory is unchanged, otherwise a fresh ``os.scandir`` that is then
    recorded. Returns a :class:`DirListing`; raises ``OSError`` if the
    directory cannot be stat'ed or read. ``on_entry`` is called once per
    fresh or replayed entry."""
    token, listing = listing_pass.begin(path)
    if listing is not None:
        if on_entry is not None:
            for _name in listing.names:
                on_entry()
        return listing
    entries = []
    try:
        with os.scandir(path) as it:
            for entry in it:
                if on_entry is not None:
                    on_entry()
                entries.append((
                    entry.name,
                    entry.is_dir(follow_symlinks=False),
                    entry.is_symlink(),
                ))
    except BaseException:
        listing_pass.failed()
        raise
    listing_pass.finish(path, token, entries)
    return DirListing.from_entries(entries)


_shared = DirListingCache()


def get_shared():
    """The process-wide cache shared by the new-images walk and the
    missing-originals scan, so either one's pass warms the other."""
    return _shared
