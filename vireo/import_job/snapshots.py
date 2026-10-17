"""Source snapshots and landed-photo fingerprints.

What a retry or resume uses to prove a carried photo or a selected
source file is still the same bytes it was when the run recorded it.
"""

import hashlib
import json
from pathlib import Path


def _capture_source_snapshots(files, sources):
    """Return ``{source_str: {"count": N, "signature": HEX}}`` per source.

    Persisted in the import result so a recovery retry can verify each
    source's file set still matches what the parent ran against. Without
    this check, retrying against ``/mnt/card`` after the card was
    ejected and a different one mounted at the same path would silently
    import every file on the new card (they have no prior catalog
    entry, so ``skip_duplicates`` doesn't gate them). Files added to
    the same card between the failed run and the retry produce the
    same mismatch — the "Retry failed files" button should not import
    files the user's original import never saw.

    Signature is a sha256 over the sorted ``(relative_posix_path,
    size, mtime_ns)`` list of files under each source root; a healthy
    file reports its ``st_size`` and ``st_mtime_ns``, an unreadable
    file renders both as ``-1`` so a stat failure produces a distinct
    signature from a successful stat at the same path. ``st_mtime_ns``
    is included alongside size so a same-size in-place replacement
    (edit that preserves byte count, or a rewritten SD-card file at
    the identical path and length) is caught by the drift check —
    ``size`` alone would treat it as unchanged and let the retry copy
    the new bytes as if they were the parent's failed files. Files
    not under a source root (empty in practice — discovery only yields
    paths under the source) are skipped.

    Called from the import job at DISCOVERY time, before any copy work
    starts, so the persisted snapshot reflects the source as the
    parent first observed it. Snapshotting at completion time instead
    would let a card ejected or momentarily unreadable mid-copy record
    ``-1`` for successfully-discovered files, then refuse the natural
    "reinsert the card and retry" recovery even though the source is
    unchanged.
    """
    if not sources:
        return {}
    snapshots = {}
    for src in sources:
        src_path = Path(src)
        src_str = str(src)
        # Deduplicate by relative path so overlapping sources (a case the
        # Import page explicitly supports — e.g. selecting both ``/card``
        # and ``/card/DCIM``) don't double-count nested files. The combined
        # ``files`` list contains each nested file twice (once from each
        # enumeration), so a plain append would put two identical entries
        # in this source's snapshot; the retry re-enumerates each source
        # alone and produces one entry per file, so the parent's doubled
        # snapshot would refuse an unchanged card. Keying by ``rel.as_posix()``
        # collapses the twin discoveries into the single-source view retry
        # validation reconstructs. See PR #1387 Codex review.
        entries_by_rel = {}
        for f in files:
            try:
                rel = f.relative_to(src_path)
            except ValueError:
                continue
            rel_str = rel.as_posix()
            if rel_str in entries_by_rel:
                continue
            try:
                st = f.stat()
                size = st.st_size
                mtime_ns = st.st_mtime_ns
            except OSError:
                size = -1
                mtime_ns = -1
            entries_by_rel[rel_str] = (rel_str, size, mtime_ns)
        entries = sorted(entries_by_rel.values())
        payload = json.dumps(entries, separators=(",", ":"))
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        snapshots[src_str] = {"count": len(entries), "signature": digest}
    return snapshots


def _fingerprint_for_row(row):
    """Format one photo row into a stable identity string.

    Format: ``folder_path/filename|s=SIZE|h=HASH``. Path alone is not
    enough: SQLite is free to reuse a freed ``photos.id`` on the next
    insert, so a delete-then-import cycle can put an unrelated file at
    the same path under the same numeric ID. ``file_size`` (always
    populated by the copy pass) and ``file_hash`` (populated by
    ``scanner.scan`` after the copy verifies) together identify the
    file's bytes; a genuine retry sees identical values, a same-path
    imposter almost never does. Missing hash/size render as empty
    segments so the format stays comparable between parent-capture time
    and retry-verify time even when the scanner hasn't stamped a hash
    (both sides observe the same NULL and match). Returns ``None`` when
    the row is missing the path fields the retry needs to compare
    against.
    """
    folder_path = row["folder_path"] or ""
    filename = row["filename"] or ""
    if not folder_path or not filename:
        return None
    size = row["file_size"]
    file_hash = row["file_hash"] or ""
    size_str = "" if size is None else str(size)
    return f"{folder_path}/{filename}|s={size_str}|h={file_hash}"


def _capture_photo_fingerprints(db, photo_ids):
    """Capture ``{id: fingerprint_string}`` for each landed photo.

    Persisted in the import result so a retry can verify each carried ID
    is still the same file. ``photos.id`` is an ``INTEGER PRIMARY KEY``
    without ``AUTOINCREMENT``, so a delete-then-import cycle can reuse
    an ID for an unrelated photo; without a stable-identity check the
    retry's after-import chain (and any ``after_process_move``) would
    silently pick up that unrelated row. See ``_fingerprint_for_row``
    for the string format. Keys are stringified for JSON storage.
    """
    if not photo_ids:
        return {}
    fingerprints = {}
    for chunk in _chunks(sorted(int(pid) for pid in photo_ids)):
        rows = db.imports.landed_fingerprint_rows(chunk)
        for row in rows:
            fp = _fingerprint_for_row(row)
            if fp is None:
                continue
            fingerprints[str(row["id"])] = fp
    return fingerprints


def _chunks(items, size=500):
    """Yield ``items`` split into lists of up to ``size`` elements.

    Kept module-private so callers here don't reach into ``db._chunks``.
    500 keeps well under SQLite's default 999 bound-parameter cap while
    holding fingerprint round-trips to one per few hundred photos.
    """
    buf = []
    for item in items:
        buf.append(item)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf
