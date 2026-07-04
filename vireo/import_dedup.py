"""Shared duplicate-identity logic for card/folder imports.

Vireo's historical import duplicate gate was exact: SHA-256 every source
file and compare against ``photos.file_hash``. Correct, but importing from
an SD card meant reading every byte of the card — often twice, once for the
preview's duplicate scan and again at ingest — before a single file was
copied. At card-reader speeds that is minutes of pure I/O for a check that
Lightroom-style importers answer from metadata in seconds.

This module implements the metadata-first gate shared by the import preview
(``/api/import/check-duplicates``), ``ingest()``, and the local-processing
preflight helpers, so every duplicate count the UI shows is computed by
exactly the same rules ingest applies:

* Heuristic mode (default): a source file is a duplicate when a cataloged
  photo matches its (filename, byte size, capture time to the second).
  The capture time comes from EXIF only — never file mtime — so the key is
  a property of the file's bytes, not of how it was copied around. When
  the timestamp is missing or looks like a placeholder (epoch-era year,
  exactly-midnight clock), the file falls back to the exact content hash —
  and even then only when some cataloged photo has the same byte size,
  because a size with no cataloged twin cannot be an exact duplicate.
* Verify mode (``verify_by_hash=True``): the historical behavior — hash
  every non-empty source file and compare against cataloged hashes.

Failure modes, by construction:

* False positive (skipping a real photo) needs another photo with the same
  filename AND byte size AND to-the-second capture time. Same-second bursts
  of fixed-size uncompressed RAW frames are disambiguated by filename;
  placeholder clocks are excluded by the trustworthiness rule.
* False negative (re-copying a renamed true duplicate) is self-healing:
  the post-import scan hashes everything on local disk and the duplicates
  page flags the extra copy.
"""

import contextlib
import logging
import os
import re
from datetime import datetime
from pathlib import Path

from grouping import read_exif_timestamp
from scanner import EMPTY_FILE_SHA256, compute_file_hash

log = logging.getLogger(__name__)

# Camera clocks that were never set report epoch-era dates (1970 Unix,
# 1980 FAT) or a firmware default like 2015-01-01/2021-01-01 starting at
# midnight. A pre-1990 year can't be a digital photo; an exactly-midnight
# clock (to the second) is overwhelmingly a placeholder rather than a real
# 00:00:00 exposure. Both fall back to the exact content check.
_MIN_TRUSTWORTHY_YEAR = 1990

_EXIFTOOL_DT_RE = re.compile(
    r"(\d{4}):(\d{2}):(\d{2})[ T](\d{2}):(\d{2}):(\d{2})"
)


def parse_metadata_timestamp(value):
    if not value:
        return None
    match = _EXIFTOOL_DT_RE.search(str(value))
    if not match:
        return None
    try:
        return datetime(
            int(match.group(1)),
            int(match.group(2)),
            int(match.group(3)),
            int(match.group(4)),
            int(match.group(5)),
            int(match.group(6)),
        )
    except ValueError:
        return None


def metadata_capture_timestamp(metadata):
    for group_name in ("EXIF", "XMP", "QuickTime", "Composite"):
        group = metadata.get(group_name)
        if not isinstance(group, dict):
            continue
        for key in (
            "DateTimeOriginal",
            "CreateDate",
            "DateTimeDigitized",
            "SubSecDateTimeOriginal",
            "MediaCreateDate",
            "TrackCreateDate",
        ):
            dt = parse_metadata_timestamp(group.get(key))
            if dt is not None:
                return dt
    return None


def source_capture_timestamps(files):
    """Resolve EXIF capture timestamps for source files. No mtime fallback.

    Uses the lightweight Pillow/exifread reader first, then batches the
    leftovers (HEIC, video, exotic RAW) through ExifTool. Files with no
    readable capture timestamp map to None — deliberately NOT file mtime,
    because mtime is a property of the copy, not of the photo, and this
    map feeds duplicate identity. Callers that want mtime for folder
    planning apply their own fallback (see ingest._source_file_timestamps).
    """
    timestamps = {}
    missing = []

    for source_file in files:
        exif_dt = None
        with contextlib.suppress(OSError, ValueError):
            exif_dt = read_exif_timestamp(str(source_file))
        if exif_dt is None:
            missing.append(source_file)
        else:
            timestamps[source_file] = exif_dt

    if missing:
        try:
            from metadata import extract_metadata

            metadata_by_path = extract_metadata(
                [str(path) for path in missing],
                restricted_tags=[
                    "-DateTimeOriginal",
                    "-CreateDate",
                    "-DateTimeDigitized",
                    "-SubSecDateTimeOriginal",
                    "-MediaCreateDate",
                    "-TrackCreateDate",
                ],
            )
        except Exception:
            log.debug(
                "Could not read ExifTool metadata for capture timestamps",
                exc_info=True,
            )
            metadata_by_path = {}

        for source_file in missing:
            metadata = metadata_by_path.get(str(source_file), {})
            timestamps[source_file] = metadata_capture_timestamp(metadata)

    return timestamps


def timestamp_is_trustworthy(dt):
    """True when a capture timestamp is a safe duplicate-identity signal."""
    return (
        dt is not None
        and dt.year >= _MIN_TRUSTWORTHY_YEAR
        and not (dt.hour == 0 and dt.minute == 0 and dt.second == 0)
    )


def metadata_key(filename, file_size, dt):
    """Duplicate-identity key for a source file with a trusted timestamp.

    Second precision (not sub-second): the catalog stores sub-seconds when
    the camera provides them, but the lightweight source-side readers are
    second-precision, so both sides truncate. Filename keeps same-second
    same-size burst frames apart. Casefolded because FAT card readers and
    APFS disagree about filename case.
    """
    return (filename.casefold(), int(file_size), dt.strftime("%Y-%m-%dT%H:%M:%S"))


def stored_metadata_key(filename, file_size, ts_text):
    """metadata_key equivalent for a photos-table row, or None.

    ``photos.timestamp`` is ISO text ("2026-03-28T14:30:12.340000" or
    without the fraction); the first 19 chars are the to-the-second form.
    Applies the same trustworthiness rule as the source side via string
    comparison so building the index doesn't parse a datetime per row.
    """
    if not filename or not file_size or not ts_text or len(ts_text) < 19:
        return None
    if ts_text[:4] < str(_MIN_TRUSTWORTHY_YEAR) or ts_text[11:19] == "00:00:00":
        return None
    return (filename.casefold(), int(file_size), ts_text[:19])


class CatalogIndex:
    """Immutable duplicate-identity view of the photos table.

    Build once per import operation and share across DuplicateChecker
    instances (the preview, the preflight, and ingest each need fresh
    seen-state, but the catalog side is identical).
    """

    __slots__ = (
        "known_hashes",
        "known_keys",
        "hash_sizes",
        "unkeyed_hash_sizes",
        "sizes_complete",
    )

    def __init__(self, known_hashes=None, known_keys=None, hash_sizes=None,
                 unkeyed_hash_sizes=None, sizes_complete=False):
        self.known_hashes = known_hashes or set()
        self.known_keys = known_keys or set()
        # Byte sizes of cataloged photos that carry a file_hash: the
        # fallback content check only reads files whose size has a
        # cataloged twin. sizes_complete guards the shortcut — if any
        # hashed row is missing file_size, size absence proves nothing
        # and every fallback candidate gets hashed.
        self.hash_sizes = hash_sizes or set()
        # Sizes of hashed rows that could NOT produce a metadata key
        # (timestamp missing or placeholder). For a source file of one of
        # these sizes, a key miss cannot prove novelty — the cataloged
        # twin would never key-match — so the checker falls through to
        # the exact content check even when the source metadata is
        # trusted.
        self.unkeyed_hash_sizes = unkeyed_hash_sizes or set()
        self.sizes_complete = sizes_complete

    @classmethod
    def from_db(cls, db):
        known_hashes = set()
        known_keys = set()
        hash_sizes = set()
        unkeyed_hash_sizes = set()
        sizes_complete = True
        rows = db.conn.execute(
            "SELECT filename, file_size, timestamp, file_hash FROM photos"
        ).fetchall()
        for row in rows:
            key = stored_metadata_key(
                row["filename"], row["file_size"], row["timestamp"],
            )
            if key is not None:
                known_keys.add(key)
            file_hash = row["file_hash"]
            if file_hash is not None:
                known_hashes.add(file_hash)
                if row["file_size"]:
                    hash_sizes.add(int(row["file_size"]))
                    if key is None:
                        unkeyed_hash_sizes.add(int(row["file_size"]))
                else:
                    sizes_complete = False
        return cls(
            known_hashes, known_keys, hash_sizes, unkeyed_hash_sizes,
            sizes_complete,
        )

    @classmethod
    def from_hashes(cls, hashes):
        """Index over a bare hash set (no sizes known → no size shortcut)."""
        return cls(known_hashes=set(hashes), sizes_complete=False)


class DuplicateChecker:
    """Stateful "would ingest skip this source file?" oracle.

    One instance per pass that mirrors ingest's accumulator semantics:
    identities recorded via record()/check_and_record() are treated as
    known by later match() calls, exactly like ingest treats files it has
    already copied. Share one instance across a multi-source ingest loop;
    build a fresh one (same CatalogIndex) for each independent prediction
    pass so predictions don't pollute each other.

    ``times_cache`` may be a shared dict so several checkers over the same
    card (preflight then ingest) read each file's EXIF header only once.
    """

    def __init__(self, index, verify_by_hash=False, times_cache=None):
        self.index = index
        self.verify_by_hash = verify_by_hash
        self._seen_hashes = set()
        self._seen_keys = set()
        self._seen_sizes = set()
        # size -> {str paths} of files recorded WITHOUT any identity
        # (no trusted metadata, body never read). A later same-size file
        # in the fallback path promotes these to hashed identities so
        # byte-identical intra-batch twins with no usable metadata are
        # still deduplicated exactly — only the colliding pair pays for
        # content reads, not the whole batch.
        self._seen_unhashed = {}
        # str(path) -> datetime|None; EXIF-only capture times.
        self._times = times_cache if times_cache is not None else {}
        # str(path) -> str|None; content hash (None for zero-byte files,
        # which carry no duplicate identity anywhere in Vireo).
        self._hashes = {}
        # Set when hashes with unknown sizes are merged in (legacy
        # extra_known_hashes callers): the size shortcut can no longer
        # prove a miss, so every fallback candidate gets hashed.
        self._unsized_hashes = False

    def add_known_hashes(self, hashes):
        self._seen_hashes.update(hashes)
        if hashes:
            self._unsized_hashes = True

    def prepare(self, files):
        """Batch-resolve EXIF capture times (no-op in verify mode)."""
        if self.verify_by_hash:
            return
        pending = [f for f in files if str(f) not in self._times]
        if not pending:
            return
        resolved = source_capture_timestamps(pending)
        for source_file, dt in resolved.items():
            self._times[str(source_file)] = dt

    def capture_time(self, source_file):
        """EXIF capture time (or None), resolving lazily if not prepared."""
        path_str = str(source_file)
        if path_str not in self._times:
            self._times[path_str] = source_capture_timestamps(
                [Path(path_str)]
            ).get(Path(path_str))
        return self._times[path_str]

    def content_hash(self, source_file):
        """Cached content hash; None for zero-byte files. May raise OSError."""
        path_str = str(source_file)
        if path_str not in self._hashes:
            file_hash = compute_file_hash(path_str)
            if file_hash == EMPTY_FILE_SHA256 and os.path.getsize(path_str) == 0:
                file_hash = None
            self._hashes[path_str] = file_hash
        return self._hashes[path_str]

    def _metadata_key_of(self, source_file, size):
        dt = self.capture_time(source_file)
        if not timestamp_is_trustworthy(dt):
            return None
        return metadata_key(Path(str(source_file)).name, size, dt)

    def _size_could_match(self, size):
        if self._unsized_hashes or not self.index.sizes_complete:
            return True
        return (
            size in self.index.hash_sizes
            or size in self._seen_sizes
            or size in self._seen_unhashed
        )

    def _promote_unhashed(self, size):
        """Hash recorded-but-unhashed files of ``size`` into _seen_hashes."""
        pending = self._seen_unhashed.pop(size, None)
        if not pending:
            return
        for path_str in pending:
            with contextlib.suppress(OSError):
                file_hash = self.content_hash(path_str)
                if file_hash is not None:
                    self._seen_hashes.add(file_hash)
                    self._seen_sizes.add(size)

    def match(self, source_file):
        """Return the matched identity token if ``source_file`` duplicates a
        known photo, else None.

        Tokens are ('key', metadata_key) or ('hash', content_hash) — opaque
        to most callers, but ingest maps them to the destination folders
        holding the original so the post-import scan can link those
        folders into the workspace. May raise OSError (unreadable file).
        """
        size = os.stat(str(source_file)).st_size
        if size == 0:
            return None

        if self.verify_by_hash:
            file_hash = self.content_hash(source_file)
            if file_hash is not None and (
                file_hash in self.index.known_hashes
                or file_hash in self._seen_hashes
            ):
                return ("hash", file_hash)
            return None

        key = self._metadata_key_of(source_file, size)
        if key is not None:
            if key in self.index.known_keys or key in self._seen_keys:
                return ("key", key)
            if (
                self.index.sizes_complete
                and not self._unsized_hashes
                and size not in self.index.unkeyed_hash_sizes
            ):
                # Trusted metadata with no cataloged twin: a duplicate of
                # a cataloged photo would carry the catalog's own
                # filename, size and capture time, so a key miss means
                # "new" without reading the file body. (Renamed true
                # duplicates slip through here and are caught by the
                # post-import scan.)
                return None
            # Some cataloged photo of this exact size has a hash but no
            # usable timestamp (or hash sizes aren't fully known) — a key
            # miss can't prove novelty, so fall through to the exact
            # content check.

        # Metadata missing or placeholder — poke harder with the exact
        # content check, but only when some known photo could plausibly
        # match by size.
        if not self._size_could_match(size):
            return None
        self._promote_unhashed(size)
        file_hash = self.content_hash(source_file)
        if file_hash is not None and (
            file_hash in self.index.known_hashes
            or file_hash in self._seen_hashes
        ):
            return ("hash", file_hash)
        return None

    def record(self, source_file):
        """Register a non-duplicate file's identity as known.

        Call after deciding to keep/copy the file so later occurrences of
        the same content are treated as duplicates (ingest's intra-batch
        and cross-source accumulator semantics). Returns the tokens
        recorded so callers can attach bookkeeping (e.g. the destination
        folder the copy landed in) to each. May raise OSError.
        """
        size = os.stat(str(source_file)).st_size
        if size == 0:
            # Zero-byte files carry no duplicate identity anywhere in
            # Vireo; every empty file would otherwise alias every other.
            return ()

        tokens = []
        if self.verify_by_hash:
            file_hash = self.content_hash(source_file)
            if file_hash is not None:
                self._seen_hashes.add(file_hash)
                tokens.append(("hash", file_hash))
            return tuple(tokens)

        key = self._metadata_key_of(source_file, size)
        if key is not None:
            self._seen_keys.add(key)
            tokens.append(("key", key))
        # If the fallback already paid for a hash, keep it — it's free
        # and lets a later byte-identical file with different metadata
        # context (or an explicit content check) match exactly.
        file_hash = self._hashes.get(str(source_file))
        if file_hash is not None:
            self._seen_hashes.add(file_hash)
            self._seen_sizes.add(size)
            tokens.append(("hash", file_hash))
        elif key is None:
            # No identity at all (untrusted metadata, body never read).
            # Park the size so a later same-size fallback candidate
            # promotes this file to a hashed identity — a byte-identical
            # twin necessarily has the same size and equally-untrusted
            # metadata, so it lands in the fallback path and finds it.
            self._seen_unhashed.setdefault(size, set()).add(str(source_file))
        return tuple(tokens)

    def check_and_record(self, source_file):
        """True if duplicate; otherwise record its identity and return False."""
        if self.match(source_file) is not None:
            return True
        self.record(source_file)
        return False
