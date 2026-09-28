"""Free up card space: verified scan → preview → delete for import sources.

Spec: docs/superpowers/specs/2026-08-07-card-cleanup-design.md

The safety invariant, enforced immediately before every unlink:
(1) the card file's re-hashed bytes equal the manifest hash, and
(2) a fresh catalog query finds a row with hash_status='ok' whose
    archive file is outside the source tree, is not the card file
    itself (device+inode), and stats exactly at the cataloged
    file_size/file_mtime baseline. Archive bytes are never re-read —
    the archive lives on a VPN'd SMB mount, and a re-read of the whole
    deletable set would cost more than the import this tool reclaims.
"""
import contextlib
import errno
import json
import os
import stat as stat_mod
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path

import path_guard
from file_replace import replace_file
from image_loader import (
    SUPPORTED_EXTENSIONS,
    ScanCancelled,
    is_excluded_scan_path,
    safe_iter_dir,
    safe_scan_walk,
)
from scanner import compute_fd_hash, compute_file_hash

MANIFEST_SCHEMA_VERSION = 1
MANIFEST_MAX_AGE_DAYS = 7
# Bumped whenever the manifest is rewritten (scan or verify). The delete
# endpoint requires the client's confirmed revision to match — a verify
# that runs between the confirmation and the delete POST would otherwise
# let deletion sweep newly-promoted files the user never saw.
INITIAL_MANIFEST_REVISION = 1

# O_NONBLOCK doesn't exist on Windows; fall back to 0 (a no-op flag) so
# the open call succeeds. The FIFO-open block this flag defends against
# is a POSIX concern — Windows can't be tricked into open() blocking on
# a FIFO the same way, so a plain O_RDONLY there is safe.
O_NONBLOCK = getattr(os, "O_NONBLOCK", 0)

# Codex P1: os.open on Windows defaults to text mode. compute_fd_hash
# reads through os.read, and the CRT would translate CRLF↔LF and treat
# Ctrl-Z (0x1A) as EOF in text mode — producing a different digest from
# compute_file_hash's "rb" open for any RAW/JPEG file containing those
# bytes. Scoped verification would then flag valid archives as corrupt,
# and delete_verified's identical card-file open would skip valid
# deletions. O_BINARY is a POSIX no-op (getattr fallback).
O_BINARY = getattr(os, "O_BINARY", 0)


class ManifestError(Exception):
    """Manifest missing, expired, or failing validation.

    http_status lets the endpoint distinguish gone/expired (404,
    "re-scan the card") from corrupt/invalid (400) without string
    matching.
    """

    def __init__(self, message, http_status=400):
        super().__init__(message)
        self.http_status = http_status


def manifest_path(manifest_dir, scan_job_id):
    # Job ids are runner-generated, but basename() keeps a hostile id
    # from escaping the manifest directory.
    return os.path.join(
        manifest_dir, f"{os.path.basename(str(scan_job_id))}.json"
    )


def write_manifest(manifest_dir, manifest):
    """Atomic write (sibling temp + os.replace): a crash mid-scan can
    never leave a truncated manifest that a later delete trusts."""
    os.makedirs(manifest_dir, exist_ok=True)
    path = manifest_path(manifest_dir, manifest["scan_job_id"])
    fd, tmp = tempfile.mkstemp(
        prefix=f".{os.path.basename(path)}.", suffix=".tmp",
        dir=manifest_dir,
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(manifest, f)
        replace_file(tmp, path)
    except Exception:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise
    return path


def prune_manifests(manifest_dir, max_age_days=MANIFEST_MAX_AGE_DAYS):
    """Remove manifests older than max_age_days, plus any orphaned
    ``*.tmp`` write-manifest temp files (a hard crash between
    ``mkstemp`` and ``os.replace`` would otherwise leave them
    forever)."""
    if not os.path.isdir(manifest_dir):
        return
    cutoff = time.time() - max_age_days * 86400
    for name in os.listdir(manifest_dir):
        if not (name.endswith(".json") or name.endswith(".tmp")):
            continue
        full = os.path.join(manifest_dir, name)
        with contextlib.suppress(OSError):
            if os.path.getmtime(full) < cutoff:
                os.unlink(full)


def classify_source_files(source, recursive=True, onerror=None, should_cancel=None):
    """One walk over the card; returns (candidates, ignored), both sorted.

    Mirrors discover_source_files' file_types="both" filter — parity is
    pinned by a test — with one deliberate divergence: symlinks are
    rejected here even though discovery follows them (see the loop
    comment). The candidate set is therefore a subset of discovery's,
    which is the direction the spec requires ("the deletable set can
    never exceed what import considers a photo"). Also returns the
    non-photo files so the preview can show an "ignored, never touched"
    bucket without a second walk (discover_source_files drops them).
    """
    source_path = Path(source)
    if is_excluded_scan_path(source_path):
        if onerror is not None:
            onerror(PermissionError(
                errno.EACCES, "source is an excluded data bundle",
                str(source_path)))
        return [], []
    if not source_path.is_dir():
        if onerror is not None:
            onerror(FileNotFoundError(
                errno.ENOENT, "source is not an accessible directory",
                str(source_path)))
        return [], []
    if recursive:
        def _walk():
            for dirpath, _dirnames, filenames in safe_scan_walk(
                    str(source_path), onerror=onerror, cancel_check=should_cancel):
                for name in filenames:
                    yield Path(dirpath) / name
        entries = _walk()
    else:
        entries = safe_iter_dir(str(source_path), onerror=onerror)
    candidates, ignored = [], []
    for f in entries:
        if should_cancel is not None and should_cancel():
            raise ScanCancelled("card discovery cancelled")
        # Symlinks resolve to bytes stored elsewhere. If we followed one
        # (Path.is_file does), the size/hash recorded here would be the
        # target's, but os.remove(path) unlinks only the link — no card
        # space is reclaimed and delete_verified would credit the
        # target's full size as "deleted bytes". Reject at classification
        # so a symlink can never enter the deletable set.
        if f.is_symlink() or not f.is_file():
            continue
        if (f.suffix.lower() in SUPPORTED_EXTENSIONS
                and not f.name.startswith(".")):
            candidates.append(f)
        else:
            ignored.append(f)
    if should_cancel is not None and should_cancel():
        raise ScanCancelled("card discovery cancelled")
    return sorted(candidates), sorted(ignored)


def load_manifest(manifest_dir, scan_job_id,
                  max_age_days=MANIFEST_MAX_AGE_DAYS):
    """Load + validate. Everything here must pass before any delete."""
    path = manifest_path(manifest_dir, scan_job_id)
    if not os.path.exists(path):
        raise ManifestError(
            "manifest expired — re-scan the card", http_status=404)
    try:
        with open(path, encoding="utf-8") as f:
            manifest = json.load(f)
    except (OSError, ValueError) as e:
        raise ManifestError(
            "manifest unreadable or corrupt — re-scan the card") from e
    if (not isinstance(manifest, dict)
            or manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION):
        raise ManifestError("manifest schema not recognized — re-scan the card")
    revision = manifest.get("revision", INITIAL_MANIFEST_REVISION)
    if (isinstance(revision, bool) or not isinstance(revision, int)
            or revision < INITIAL_MANIFEST_REVISION):
        raise ManifestError("manifest revision invalid — re-scan the card")
    # Normalize pre-revision manifests at the read boundary so every caller
    # sees the same initial value, including the API response and verify job.
    manifest["revision"] = revision
    source_root = manifest.get("source_root")
    if not source_root or not os.path.isabs(str(source_root)):
        raise ManifestError("manifest missing source root — re-scan the card")
    try:
        created = datetime.fromisoformat(manifest.get("created_at"))
    except (TypeError, ValueError) as e:
        raise ManifestError(
            "manifest timestamp invalid — re-scan the card") from e
    # fromisoformat accepts a naive stamp like "2026-08-08T12:00:00";
    # subtracting a naive from an aware datetime raises TypeError, so
    # reject naive timestamps up front. Corrupt manifests must surface
    # as ManifestError (HTTP 400), never a bare TypeError bubbling up.
    if created.tzinfo is None or created.utcoffset() is None:
        raise ManifestError(
            "manifest timestamp invalid — re-scan the card")
    age = datetime.now(UTC) - created
    # Age is enforced here — at request time — not only by the
    # scan-start prune.
    if age.total_seconds() > max_age_days * 86400:
        raise ManifestError(
            "manifest expired — re-scan the card", http_status=404)
    entries = manifest.get("entries")
    if not isinstance(entries, list):
        raise ManifestError("manifest entries malformed — re-scan the card")
    # path_guard.path_contains() returns True on "can't tell" — the
    # strict direction for a guard that *disqualifies* on containment.
    # Here containment is a *required* condition for the entry to be
    # accepted, so that polarity is inverted: an unresolvable path must
    # fail closed (rejected), not fail open (accepted). Resolve both
    # sides ourselves and use contains_resolved() directly; a realpath
    # failure is treated as "outside" rather than "inside" (OSError from
    # an unreadable path, ValueError from e.g. an embedded null byte).
    # Case-fold acceptance inside contains_resolved is still fine here —
    # a case-swapped path within the root is genuinely still within it,
    # and the delete job's per-file gates are the deeper defense.
    # Null bytes are rejected explicitly rather than via realpath's
    # exception: POSIX realpath raises ValueError on them but Windows'
    # ntpath.realpath swallows the error and returns the string
    # unchanged, which would let the path sail through validation and
    # crash later at the stat/unlink. Uniform check, uniform outcome.
    if "\x00" in str(source_root):
        raise ManifestError(
            "manifest source root unresolvable — re-scan the card")
    try:
        root_real = os.path.realpath(source_root)
    except (OSError, ValueError) as e:
        raise ManifestError(
            "manifest source root unresolvable — re-scan the card") from e
    for entry in entries:
        if not isinstance(entry, dict):
            raise ManifestError(
                "manifest entries malformed — re-scan the card")
        if entry.get("bucket") != "deletable":
            continue
        if not entry.get("path"):
            raise ManifestError(
                "manifest entries malformed — re-scan the card")
        if (not isinstance(entry.get("size"), int)
                or not isinstance(entry.get("mtime_ns"), int)
                or not isinstance(entry.get("hash"), str)
                or not entry.get("hash")):
            raise ManifestError(
                "manifest entries malformed — re-scan the card")
        # Same explicit null-byte rejection as the source root above —
        # Windows realpath would pass the string through unchanged.
        if "\x00" in str(entry.get("path", "")):
            raise ManifestError(
                "manifest entry outside its source root — re-scan the card")
        try:
            child_real = os.path.realpath(str(entry.get("path", "")))
        except (OSError, ValueError) as e:
            raise ManifestError(
                "manifest entry outside its source root — re-scan the card"
            ) from e
        if not path_guard.contains_resolved(root_real, child_real):
            raise ManifestError(
                "manifest entry outside its source root — re-scan the card")
    return manifest


KEEP_NOT_IN_CATALOG = "not in catalog — not imported yet"
# The card-cleanup page keys its scoped verification callout off this exact
# reason.  It intentionally does not send users through the workspace-wide
# integrity audit.
KEEP_NOT_VERIFIED = "archive copy has not been checksum-verified"
# Codex P2: distinguished from KEEP_NOT_VERIFIED so the callout does NOT
# offer to re-verify these — the archive rows have already been checked
# and the verdict was modified/corrupt/unreadable. Re-running verify
# reproduces the same bad verdict rather than establishing an "ok" copy;
# the remedy lives on the Audit page (accept current hash, restore from
# backup, or investigate). The Audit-callout tail must NOT appear here or
# these files would be counted alongside the truly-unchecked ones.
KEEP_ARCHIVE_HASH_FAILED = (
    "archive copy failed a prior integrity check — see the Audit page"
)
KEEP_INSIDE_SOURCE = "only catalog copy is inside the selected source"
KEEP_ARCHIVE_UNREACHABLE = "archive file not reachable"
KEEP_ARCHIVE_CHANGED = "archive file changed since verification"
KEEP_UNREADABLE = "could not read card file"

SKIP_ALREADY_GONE = "already gone from the card"
SKIP_CHANGED = "changed on the card since the scan"
SKIP_CONTENT_CHANGED = "content changed on the card since the scan"
SKIP_OUTSIDE_SOURCE = "path no longer resolves inside the scanned source"
SKIP_SYMLINK = "path is now a symlink — refuses to follow at delete time"
SKIP_NOT_REGULAR = (
    "path is not a regular file — refuses to open at delete time")


def qualify_rows(rows, source_root_real, card_path, contains_check=None):
    """Archive-side test from the spec's safety invariant.

    Returns (archive_path, None) for the first row that passes, else
    (None, keep_reason). The archive stat happens HERE, fresh, on every
    call — callers may cache rows, never this function's result.

    ``contains_check`` is an optional bound containment predicate from
    ``path_guard.make_case_folded_check(source_root_real)`` — the
    scan/delete loops pass one so per-file catalog work does not reprobe
    the source filesystem on every candidate. Single-shot callers can
    omit it; the fallback matches the plain ``contains_resolved`` call.
    """
    if not rows:
        return None, KEEP_NOT_IN_CATALOG
    if contains_check is None:
        def contains_check(child_real):
            return path_guard.contains_resolved(source_root_real, child_real)
    # Codex P2: KEEP_NOT_VERIFIED reads to the user as "the audit hasn't
    # run yet — running it would unblock this", so it must apply only when
    # a NULL hash_status row is why the file is being kept. Rows with a
    # non-NULL, non-"ok" status (modified/corrupt/unreadable) have already
    # been verified; re-running verification reproduces the same verdict
    # and the remedy lives on the Audit page, so those rows fall back to
    # KEEP_ARCHIVE_HASH_FAILED instead.
    #
    # Priority (also Codex P2): a specific reason from an "ok" row that
    # failed a later gate USED TO win over KEEP_NOT_VERIFIED, but that hid
    # entries whose unchecked sibling row could still be verified.
    # KEEP_NOT_VERIFIED now takes precedence whenever we saw a
    # never-checked row that verify could still try — the unchecked row
    # might have a different archive path that clears the same gate.
    # verify_manifest_archives itself does not persist a bad verdict when
    # the archive fails a gate before hashing, so trying it is free of
    # side effects; the ok-row's specific reason is only preferred when
    # no unchecked row is available to give verify a fresh attempt.
    saw_never_checked = False
    saw_check_failed = False
    reason = None
    try:
        card_st = os.stat(card_path)
    except OSError:
        return None, KEEP_UNREADABLE
    for row in rows:
        if row["hash_status"] != "ok":
            if row["hash_status"] is None:
                saw_never_checked = True
            else:
                saw_check_failed = True
            continue
        if not row["folder_path"]:
            continue
        archive_path = os.path.join(row["folder_path"], row["filename"])
        try:
            archive_real = os.path.realpath(archive_path)
        except (OSError, ValueError):
            # Containment is unknown, not disproven — a row we can't
            # even resolve must not be treated as "inside the source"
            # (which would be silently wrong) or crash the whole job.
            reason = KEEP_ARCHIVE_UNREACHABLE
            continue
        if contains_check(archive_real):
            reason = KEEP_INSIDE_SOURCE
            continue
        try:
            ast = os.stat(archive_path)
        except (OSError, ValueError):
            reason = KEEP_ARCHIVE_UNREACHABLE
            continue
        # samefile semantics without a second round trip: a mount alias
        # that survived realpath + case-folding still shares dev+inode.
        if (ast.st_dev, ast.st_ino) == (card_st.st_dev, card_st.st_ino):
            reason = KEEP_INSIDE_SOURCE
            continue
        if row["file_size"] is None or row["file_mtime"] is None:
            reason = KEEP_ARCHIVE_CHANGED
            continue
        # Exact equality — the audit's 1s window classifies an
        # already-detected mismatch and certifies nothing here; a false
        # negative just keeps a file.
        if (ast.st_size != row["file_size"]
                or ast.st_mtime != row["file_mtime"]):
            reason = KEEP_ARCHIVE_CHANGED
            continue
        # Bind the containment decision to the statted object (Codex P1):
        # realpath() and os.stat() above are two separate path walks. A
        # parent directory swapped between them means containment
        # approved one target while the metadata gate measured another —
        # and the dev/inode check only rejects the *candidate* itself,
        # not some other in-source file the redirect landed on.
        # Re-resolve after the stat and require the resolution unchanged
        # and still outside the source. A double swap inside this
        # microsecond window is the accepted residual, same class as the
        # stat-to-remove gap at the unlink.
        try:
            recheck_real = os.path.realpath(archive_path)
        except (OSError, ValueError):
            reason = KEEP_ARCHIVE_UNREACHABLE
            continue
        if recheck_real != archive_real or contains_check(recheck_real):
            reason = KEEP_INSIDE_SOURCE
            continue
        return archive_path, None
    # NULL-hash-status wins over a specific ok-row reason (Codex P2): the
    # unchecked row is verify's next lever, so the entry must land in the
    # KEEP_NOT_VERIFIED bucket the callout keys off. Otherwise the reason
    # already reflects the closest-to-success signal we have, and
    # KEEP_ARCHIVE_HASH_FAILED only surfaces when no ok row spoke up and
    # only check-failed rows remain.
    if saw_never_checked:
        reason = KEEP_NOT_VERIFIED
    elif reason is None:
        if saw_check_failed:
            reason = KEEP_ARCHIVE_HASH_FAILED
        else:
            # No non-ok rows and no ok row got a specific reason — the
            # only shapes left are ok rows with an empty folder_path. Kept
            # for a stat-shaped reason we can't state precisely.
            reason = KEEP_ARCHIVE_UNREACHABLE
    return None, reason


_ROWS_BY_HASH_SQL = """
    SELECT p.id, p.folder_id, p.filename, p.file_size, p.file_mtime,
           p.hash_status,
           f.path AS folder_path
    FROM photos p LEFT JOIN folders f ON f.id = p.folder_id
    WHERE p.file_hash = ?
"""


def fetch_rows_by_hash(db, file_hash):
    return db.conn.execute(_ROWS_BY_HASH_SQL, (file_hash,)).fetchall()


def _record_unchecked_hash_verdict(db, row, expected_hash, status,
                                   file_size=None, file_mtime=None):
    """Persist a scoped-verification verdict only for the row we read.

    Hashing can take long enough for a concurrent catalog refresh to replace
    or repurpose the photo row.  Binding the update to its original identity,
    baseline, hash, and unchecked state prevents the old descriptor's digest
    from certifying (or flagging) that newer row.
    """
    now = datetime.now().isoformat()
    assignments = "hash_status = ?, hash_checked_at = ?"
    values = [status, now]
    if file_size is not None and file_mtime is not None:
        assignments += ", file_size = ?, file_mtime = ?"
        values.extend([file_size, file_mtime])
    values.extend([
        row["id"], expected_hash, row["folder_id"], row["filename"],
        row["file_size"], row["file_mtime"],
    ])
    cursor = db.conn.execute(
        f"UPDATE photos SET {assignments} "
        "WHERE id = ? AND file_hash = ? AND hash_status IS NULL "
        "AND folder_id IS ? AND filename = ? "
        "AND file_size IS ? AND file_mtime IS ?",
        values,
    )
    return cursor.rowcount == 1


def _load_catalog_by_hash(db):
    """One pass over the catalog for the scan — a per-file SELECT over an
    unindexed file_hash column would rescan the photos table for every
    card file."""
    rows = db.conn.execute("""
        SELECT p.filename, p.file_size, p.file_mtime, p.hash_status,
               p.file_hash, f.path AS folder_path
        FROM photos p LEFT JOIN folders f ON f.id = p.folder_id
        WHERE p.file_hash IS NOT NULL
    """).fetchall()
    by_hash = {}
    for row in rows:
        by_hash.setdefault(row["file_hash"], []).append(row)
    return by_hash


def _card_gate_promotion_block(entry):
    """Return a SKIP_* reason if the card file no longer matches the
    manifest baseline in a way delete_verified would refuse; return
    ``None`` when promotion is safe to attempt or the check cannot make
    a firm judgment (missing path / transient OSError). Deliberately
    parallels the cheap card-side gates at the top of
    ``delete_verified``.
    """
    path = entry.get("path")
    if not path:
        return None
    try:
        st = os.lstat(path)
    except FileNotFoundError:
        return SKIP_ALREADY_GONE
    except OSError:
        return None
    if stat_mod.S_ISLNK(st.st_mode):
        return SKIP_SYMLINK
    if not stat_mod.S_ISREG(st.st_mode):
        return SKIP_NOT_REGULAR
    if (st.st_size != entry.get("size")
            or st.st_mtime_ns != entry.get("mtime_ns")):
        return SKIP_CHANGED
    return None


def verify_manifest_archives(db, manifest, manifest_dir, progress_cb=None,
                             should_cancel=None):
    """Verify only archive copies needed by one card-cleanup preview.

    One viable archive row is read per distinct pending content hash.  A
    successful row is enough to authorize every matching card entry; unrelated
    workspace photos and redundant archive copies are not read.  The manifest
    is then atomically refreshed so the existing preview can be used without a
    second card scan.  Destructive-time card and archive gates remain in
    ``delete_verified``.
    """
    source_root = manifest["source_root"]
    contains_check = path_guard.make_case_folded_check(source_root)
    stats = {
        "hashes_total": 0, "hashes_processed": 0,
        "archive_files_read": 0, "verified": 0,
        "modified": 0, "corrupt": 0, "unreadable": 0,
        "cancelled": False, "remaining": 0,
        "unblocked_files": 0, "unblocked_bytes": 0,
    }

    def checkpoint():
        if should_cancel is not None and should_cancel():
            stats["cancelled"] = True
            stats["remaining"] = stats["hashes_total"] - stats["hashes_processed"]
            # Promotions are only visible after the manifest is published.
            stats["unblocked_files"] = 0
            stats["unblocked_bytes"] = 0
            return True
        return False

    # Codex P2: a prior delete_verified run on this scan unlinks card
    # files but never rewrites the manifest, so its deletable entries
    # sit here even though their card paths are gone. If we left them
    # in, the totals recomputed below would still credit their bytes
    # to the deletable bucket and the refreshed UI would re-enable
    # Delete asking the user to confirm counts that include files
    # already deleted from the card. Drop entries whose card path is
    # gone; a transient stat error (mount hiccup, EACCES) preserves
    # the entry so a NAS blip cannot shrink the preview.
    #
    # Codex P2 (follow-up): the FileNotFoundError-only check misses
    # files delete_verified skipped for size/mtime/type mismatches
    # (SKIP_CHANGED / SKIP_NOT_REGULAR / SKIP_SYMLINK). lstat still
    # succeeds for those files, so the stale entry would stay in the
    # deletable bucket, inflate totals, and re-enable Delete for a
    # count the delete-time gates will refuse. Mirror the cheap gates
    # here so the refreshed preview matches what delete would accept.
    # Content-changed-but-metadata-matching is still caught by
    # delete_verified's own hash pass — expensive re-hashing here
    # would defeat the point of scoped verification.
    #
    # Codex P2 (follow-up 2): dropping a changed-but-still-present
    # card entry hid a file that still exists on the card, so the
    # kept-list and totals undercounted what would remain after
    # deletion. Reclassify those entries into the kept bucket with
    # the corresponding SKIP_* reason instead; only a confirmed-
    # absent path (FileNotFoundError above) leaves the manifest.
    #
    # Pending entries with a removed card file are handled in the
    # promotion loop below via _card_gate_promotion_block's
    # SKIP_ALREADY_GONE path, which lets scoped verify still hash
    # the archive and refresh the catalog row before dropping the
    # entry from the manifest. Keeping that dispatch in one place
    # (rather than short-circuiting here) preserves the DB-refresh
    # side effect other card scans keyed off the same hash rely on.
    surviving = []
    for entry in manifest["entries"]:
        if checkpoint():
            return stats
        if entry.get("bucket") != "deletable":
            surviving.append(entry)
            continue
        path = entry.get("path")
        if not path:
            surviving.append(entry)
            continue
        try:
            st = os.lstat(path)
        except FileNotFoundError:
            continue
        except OSError:
            surviving.append(entry)
            continue
        if stat_mod.S_ISLNK(st.st_mode):
            reclassify_reason = SKIP_SYMLINK
        elif not stat_mod.S_ISREG(st.st_mode):
            reclassify_reason = SKIP_NOT_REGULAR
        elif (st.st_size != entry.get("size")
                or st.st_mtime_ns != entry.get("mtime_ns")):
            reclassify_reason = SKIP_CHANGED
        else:
            surviving.append(entry)
            continue
        entry["bucket"] = "kept"
        entry["reason"] = reclassify_reason
        entry.pop("archive_path", None)
        surviving.append(entry)
    manifest["entries"] = surviving

    pending_by_hash = {}
    for entry in manifest["entries"]:
        if checkpoint():
            return stats
        if (entry.get("bucket") == "kept"
                and entry.get("reason") == KEEP_NOT_VERIFIED
                and entry.get("hash")):
            pending_by_hash.setdefault(entry["hash"], []).append(entry)

    pending = list(pending_by_hash.items())
    stats["hashes_total"] = len(pending)
    failure_reasons = {}
    # qualify_rows deliberately prioritizes any NULL-status row so a viable
    # independent copy remains retryable. Once this run establishes that no
    # such copy remains, however, a NULL same-file/source alias must not mask
    # either the terminal "inside source" result or a failed independent
    # archive. Record that final reason for the refresh below.
    terminal_reasons = {}

    for i, (expected_hash, card_entries) in enumerate(pending):
        if checkpoint():
            return stats
        if progress_cb is not None:
            progress_cb(
                i + 1, len(pending),
                os.path.basename(card_entries[0]["path"]),
            )

        rows = fetch_rows_by_hash(db, expected_hash)
        unchecked = [row for row in rows if row["hash_status"] is None]
        outcome_reason = KEEP_ARCHIVE_UNREACHABLE
        verified = False
        saw_inside_source_alias = False
        saw_terminal_failure = any(
            row["hash_status"] not in (None, "ok") for row in rows
        )
        saw_retryable_external = False
        for row in unchecked:
            folder_path = row["folder_path"]
            if not folder_path:
                if _record_unchecked_hash_verdict(
                        db, row, expected_hash, "unreadable"):
                    stats["unreadable"] += 1
                    saw_terminal_failure = True
                else:
                    outcome_reason = KEEP_ARCHIVE_CHANGED
                    saw_retryable_external = True
                continue
            archive_path = os.path.join(folder_path, row["filename"])
            try:
                archive_real = os.path.realpath(archive_path)
            except (OSError, ValueError):
                # Codex P2: keep hash_status NULL when we can't even
                # reach the archive path. Marking unreadable here is
                # permanent — after a transiently-down mount comes
                # back, a re-scan would classify the row as a prior
                # integrity failure and route users to the
                # workspace-wide Audit page instead of a targeted
                # verify retry.
                stats["unreadable"] += 1
                outcome_reason = KEEP_ARCHIVE_UNREACHABLE
                saw_retryable_external = True
                continue
            if contains_check(archive_real):
                outcome_reason = KEEP_INSIDE_SOURCE
                saw_inside_source_alias = True
                continue
            # CodeRabbit: pin the checked object via a descriptor rather
            # than re-opening the pathname. A concurrent rename that swaps
            # the archive for a FIFO between our S_ISREG check and the
            # hash-open would leave compute_file_hash blocking forever
            # inside open() — cancellation is checked only at file
            # boundaries, so the verify worker would hang. Opening with
            # O_NONBLOCK bypasses the FIFO-open block; fstat then rejects
            # anything that is not a regular file, and every subsequent
            # read/stat operates on this pinned fd so a later rename
            # cannot slip a different object under us.
            try:
                archive_fd = os.open(
                    archive_path, os.O_RDONLY | O_NONBLOCK | O_BINARY)
            except (OSError, ValueError):
                # Same rationale as the realpath branch above (Codex
                # P2): a stat that can't see the file — the common
                # symptom of a disconnected NAS mount — means the
                # archive is unreachable, not corrupt. Leave the row
                # unchecked so targeted verification can retry once
                # the mount returns; reserve 'unreadable' for files
                # we actually reached but could not read.
                stats["unreadable"] += 1
                outcome_reason = KEEP_ARCHIVE_UNREACHABLE
                saw_retryable_external = True
                continue
            try:
                try:
                    before = os.fstat(archive_fd)
                except OSError:
                    # We opened the object but immediately failed to
                    # stat it (extremely rare — a race on the mount
                    # itself). Treat as reached-but-unreadable rather
                    # than unreachable; the fd itself would have
                    # failed at open() in the mount-down case.
                    #
                    # Codex P2 (follow-up): outcome_reason must match
                    # what qualify_rows will return for the row we
                    # just persisted as 'unreadable' — KEEP_ARCHIVE_
                    # HASH_FAILED. Leaving it at the initial KEEP_
                    # ARCHIVE_UNREACHABLE lets the promotion-loop
                    # override downgrade qualify_rows' terminal
                    # verdict, hiding the failure and stripping the
                    # Audit-page guidance the UI shows for
                    # KEEP_ARCHIVE_HASH_FAILED.
                    if _record_unchecked_hash_verdict(
                            db, row, expected_hash, "unreadable"):
                        stats["unreadable"] += 1
                        outcome_reason = KEEP_ARCHIVE_HASH_FAILED
                        saw_terminal_failure = True
                    else:
                        outcome_reason = KEEP_ARCHIVE_CHANGED
                        saw_retryable_external = True
                    continue
                if not stat_mod.S_ISREG(before.st_mode):
                    # The object is present but is a directory/FIFO/
                    # device swapped in on top of the path — not
                    # "unreachable" (we reached it), just not something
                    # we can hash. Marking unreadable is intentional;
                    # a subsequent audit is the right remedy.
                    #
                    # Codex P2 (follow-up): see the fstat branch above.
                    if _record_unchecked_hash_verdict(
                            db, row, expected_hash, "unreadable"):
                        stats["unreadable"] += 1
                        outcome_reason = KEEP_ARCHIVE_HASH_FAILED
                        saw_terminal_failure = True
                    else:
                        outcome_reason = KEEP_ARCHIVE_CHANGED
                        saw_retryable_external = True
                    continue

                # An outside-path hardlink to a card file is not a second
                # copy.  Compare against the fd's dev/inode — the same
                # object every later step here operates on.
                same_as_card = False
                for entry in card_entries:
                    try:
                        card_st = os.stat(entry["path"])
                    except OSError:
                        continue
                    if ((before.st_dev, before.st_ino)
                            == (card_st.st_dev, card_st.st_ino)):
                        same_as_card = True
                        break
                if same_as_card:
                    outcome_reason = KEEP_INSIDE_SOURCE
                    saw_inside_source_alias = True
                    continue

                try:
                    actual_hash = compute_fd_hash(archive_fd)
                    after = os.fstat(archive_fd)
                    after_real = os.path.realpath(archive_path)
                except (OSError, ValueError):
                    # Codex P2 (follow-up): see the earlier fstat
                    # branch — outcome_reason must reflect the
                    # persisted 'unreadable' verdict so the
                    # promotion loop does not override qualify_rows'
                    # KEEP_ARCHIVE_HASH_FAILED with the initial
                    # KEEP_ARCHIVE_UNREACHABLE.
                    if _record_unchecked_hash_verdict(
                            db, row, expected_hash, "unreadable"):
                        stats["unreadable"] += 1
                        outcome_reason = KEEP_ARCHIVE_HASH_FAILED
                        saw_terminal_failure = True
                    else:
                        outcome_reason = KEEP_ARCHIVE_CHANGED
                        saw_retryable_external = True
                    continue
                stats["archive_files_read"] += 1
            finally:
                os.close(archive_fd)

            # Codex P1: the pinned fd's fstat describes the object we
            # actually hashed, but an atomic replace under the same
            # name during compute_fd_hash would leave that fd pointing
            # at the old (now-unlinked) inode. before/after fstat then
            # still match, and after_real == archive_real, but the
            # pathname now resolves to a fresh object. A replacement
            # that preserves size+mtime could then pass qualify_rows
            # later and let the card copy be deleted against an
            # archive whose bytes we never read. Re-stat the pathname
            # and require its dev/inode still match the fd's — a
            # mismatch means the successor is a different file.
            try:
                after_path_st = os.stat(archive_path)
            except (OSError, ValueError):
                after_path_st = None

            # Do not certify a pathname that moved or changed while it was
            # being read.  A later retry can verify the stable object.
            if (after_real != archive_real
                    or contains_check(after_real)
                    or after_path_st is None
                    or (after_path_st.st_dev, after_path_st.st_ino)
                    != (after.st_dev, after.st_ino)
                    or (before.st_dev, before.st_ino, before.st_size,
                        before.st_mtime_ns)
                    != (after.st_dev, after.st_ino, after.st_size,
                        after.st_mtime_ns)):
                outcome_reason = KEEP_ARCHIVE_CHANGED
                saw_retryable_external = True
                continue

            if actual_hash == expected_hash:
                if _record_unchecked_hash_verdict(
                        db, row, expected_hash, "ok",
                        file_size=after.st_size, file_mtime=after.st_mtime):
                    stats["verified"] += 1
                    verified = True
                    break
                outcome_reason = KEEP_ARCHIVE_CHANGED
                saw_retryable_external = True
                continue

            db_mtime = row["file_mtime"]
            if (db_mtime is not None
                    and abs(after.st_mtime - db_mtime) > 1.0):
                status = "modified"
            else:
                status = "corrupt"
            if _record_unchecked_hash_verdict(
                    db, row, expected_hash, status):
                stats[status] += 1
                outcome_reason = KEEP_ARCHIVE_HASH_FAILED
                saw_terminal_failure = True
            else:
                outcome_reason = KEEP_ARCHIVE_CHANGED
                saw_retryable_external = True

        db.conn.commit()
        stats["hashes_processed"] += 1
        if not verified:
            failure_reasons[expected_hash] = outcome_reason
            if not saw_retryable_external:
                if saw_terminal_failure:
                    terminal_reasons[expected_hash] = (
                        KEEP_ARCHIVE_HASH_FAILED)
                elif saw_inside_source_alias:
                    terminal_reasons[expected_hash] = KEEP_INSIDE_SOURCE

    # Only previously-unverified entries can change here.  Existing
    # deletable rows are not re-audited or invalidated by this scoped job.
    missing_pending_entry_ids = set()
    for expected_hash, card_entries in pending_by_hash.items():
        if checkpoint():
            return stats
        rows = fetch_rows_by_hash(db, expected_hash)
        for entry in card_entries:
            if checkpoint():
                return stats
            # Codex P2 (follow-up 3): qualify_rows only inspects catalog
            # rows — it cannot see that this card file has been replaced,
            # resized, or turned into a symlink since the scan. Promoting
            # a changed pending entry to the deletable bucket would let
            # its bytes land in the refreshed totals the user confirms,
            # even though delete_verified would later skip it
            # (SKIP_CHANGED / SKIP_SYMLINK / SKIP_NOT_REGULAR) and
            # actually unlink nothing. Reproduce the delete-time card
            # gates here so promotion honors the same baseline. Content
            # drift (bytes changed with size + mtime intact) still lands
            # in delete_verified's full-hash pass — repeating that here
            # would defeat the point of scoped verification.
            promote_block = _card_gate_promotion_block(entry)
            if promote_block == SKIP_ALREADY_GONE:
                # A prior delete or external removal means this file no
                # longer exists on the card. Do not retain its historical
                # bytes in the refreshed kept totals.
                missing_pending_entry_ids.add(id(entry))
                continue
            if promote_block is not None:
                entry["bucket"] = "kept"
                entry["reason"] = promote_block
                entry.pop("archive_path", None)
                continue
            archive_path, reason = qualify_rows(
                rows, source_root, entry["path"], contains_check)
            if archive_path is not None:
                entry["bucket"] = "deletable"
                entry["archive_path"] = archive_path
                entry.pop("reason", None)
                stats["unblocked_files"] += 1
                stats["unblocked_bytes"] += entry.get("size", 0)
            elif reason == KEEP_NOT_VERIFIED:
                # Codex P2: qualify_rows saw a viable unchecked row
                # after our attempt, so verify still has a lever here.
                # Overwriting with this run's transient
                # KEEP_ARCHIVE_UNREACHABLE/CHANGED would push the entry
                # out of the KEEP_NOT_VERIFIED bucket that the verify
                # endpoint's pending-reason filter and the UI callout
                # key off; once the NAS mount returns or the file
                # settles, targeted verification would then report
                # "nothing needs checking" until the user re-scans the
                # card. Keep the entry in the retry-eligible bucket.
                #
                # Codex P2 (follow-up): the exception is when this
                # run proved every unchecked row for the hash is a
                # same-source alias / hardlink of the card file — no
                # independent archive copy exists, so there IS no
                # future retry that can change the outcome. Preserving
                # KEEP_NOT_VERIFIED there traps the entry in an
                # infinite verify loop (qualify_rows keeps returning
                # KEEP_NOT_VERIFIED off the still-NULL alias rows).
                # Adopt the terminal inside-source reason instead.
                entry["reason"] = terminal_reasons.get(
                    expected_hash, KEEP_NOT_VERIFIED)
            elif reason == KEEP_ARCHIVE_HASH_FAILED:
                # A reached row whose fstat/read failed was persisted as
                # unreadable. That terminal catalog verdict must win over a
                # transient run-local fallback so the preview offers the
                # Audit remedy instead of neither retry nor repair guidance.
                entry["reason"] = reason
            elif expected_hash in failure_reasons:
                entry["reason"] = failure_reasons[expected_hash]
            else:
                entry["reason"] = reason

    if missing_pending_entry_ids:
        manifest["entries"] = [
            entry for entry in manifest["entries"]
            if id(entry) not in missing_pending_entry_ids
        ]

    totals = {
        "deletable": {"count": 0, "bytes": 0},
        "kept": {"count": 0, "bytes": 0},
        "ignored": {"count": 0},
    }
    for entry in manifest["entries"]:
        if checkpoint():
            return stats
        bucket = entry.get("bucket")
        if bucket == "ignored":
            totals["ignored"]["count"] += 1
        elif bucket in ("deletable", "kept"):
            totals[bucket]["count"] += 1
            totals[bucket]["bytes"] += entry.get("size", 0)
    manifest["totals"] = totals
    # Bump before write: the delete endpoint's revision check compares
    # against whatever is on disk, so the new manifest must carry a fresh
    # number the moment it lands. Missing on old manifests (pre-revision
    # schema) is treated as the initial revision — the next write is the
    # first observable change.
    if checkpoint():
        return stats
    manifest["revision"] = int(
        manifest.get("revision", INITIAL_MANIFEST_REVISION)) + 1
    write_manifest(manifest_dir, manifest)
    return stats


def scan_card(db, source, recursive, manifest_dir, scan_job_id,
              progress_cb=None, should_cancel=None):
    source_root_real = os.path.realpath(source)
    contains_check = path_guard.make_case_folded_check(source_root_real)
    walk_errors = []
    try:
        candidates, ignored = classify_source_files(
            source, recursive=recursive, should_cancel=should_cancel,
            onerror=lambda e: walk_errors.append(str(e)))
    except ScanCancelled:
        return {"cancelled": True}
    if should_cancel is not None and should_cancel():
        return {"cancelled": True}
    by_hash = _load_catalog_by_hash(db)
    entries = []
    totals = {
        "deletable": {"count": 0, "bytes": 0},
        "kept": {"count": 0, "bytes": 0},
        "ignored": {"count": len(ignored)},
    }
    for i, f in enumerate(candidates):
        if should_cancel is not None and should_cancel():
            return {"cancelled": True}
        if progress_cb is not None:
            progress_cb(i + 1, len(candidates), f.name)
        try:
            st = os.stat(f)
        except OSError as e:
            # No stat → no size known; count only.
            entries.append({
                "path": str(f), "bucket": "kept",
                "reason": f"{KEEP_UNREADABLE}: {e}",
            })
            totals["kept"]["count"] += 1
            continue
        try:
            file_hash = compute_file_hash(str(f))
        except OSError as e:
            # Stat succeeded but read/hash failed (permission, transient
            # read error). st.st_size is truthful; crediting it to
            # kept.bytes keeps the preview honest — a wholesale-unreadable
            # card would otherwise report gigabytes as "0 bytes kept".
            entries.append({
                "path": str(f), "bucket": "kept",
                "size": st.st_size,
                "reason": f"{KEEP_UNREADABLE}: {e}",
            })
            totals["kept"]["count"] += 1
            totals["kept"]["bytes"] += st.st_size
            continue
        entry = {
            "path": str(f), "size": st.st_size,
            "mtime_ns": st.st_mtime_ns, "hash": file_hash,
        }
        archive_path, reason = qualify_rows(
            by_hash.get(file_hash, []), source_root_real, str(f),
            contains_check)
        if archive_path is not None:
            entry.update(bucket="deletable", archive_path=archive_path)
            totals["deletable"]["count"] += 1
            totals["deletable"]["bytes"] += st.st_size
        else:
            entry.update(bucket="kept", reason=reason)
            totals["kept"]["count"] += 1
            totals["kept"]["bytes"] += st.st_size
        entries.append(entry)
    for f in ignored:
        if should_cancel is not None and should_cancel():
            return {"cancelled": True}
        entries.append({"path": str(f), "bucket": "ignored"})
    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "scan_job_id": scan_job_id,
        "created_at": datetime.now(UTC).isoformat(),
        "source_root": source_root_real,
        "recursive": bool(recursive),
        "entries": entries,
        "walk_errors": walk_errors,
        "totals": totals,
        "revision": INITIAL_MANIFEST_REVISION,
    }
    if should_cancel is not None and should_cancel():
        return {"cancelled": True}
    write_manifest(manifest_dir, manifest)
    # Job-result flag only — deliberately NOT part of the persisted
    # manifest (added after write_manifest). A completed scan's manifest
    # on disk has no "cancelled" key; don't "fix" this into the schema.
    manifest["cancelled"] = False
    return manifest


def delete_verified(db, manifest, progress_cb=None, should_cancel=None):
    """Delete the manifest's deletable bucket, re-proving the invariant
    per file immediately before each unlink. Never reads the kept or
    ignored buckets.

    The manifest must come from load_manifest (validated); raw dicts are
    not a supported input.
    """
    # Use the manifest's source_root verbatim (Codex P1 review) — it was
    # realpath'd at scan time (see scan_card) and persisted as the
    # canonical anchor. Re-resolving here would follow a post-scan symlink
    # swap of the root itself (e.g. /Volumes/CARD/DCIM renamed and replaced
    # by a symlink pointing at /elsewhere), re-anchoring every containment
    # check to the attacker-controlled target and letting a matching file
    # there pass every gate and be unlinked outside the scanned card.
    source_root_real = manifest["source_root"]
    contains_check = path_guard.make_case_folded_check(source_root_real)
    deletable = [e for e in manifest["entries"]
                 if e.get("bucket") == "deletable"]
    summary = {
        "deleted": 0, "deleted_bytes": 0,
        "skipped": [], "failed": [],
        "cancelled": False, "remaining": 0,
    }
    for i, entry in enumerate(deletable):
        if should_cancel is not None and should_cancel():
            summary["cancelled"] = True
            summary["remaining"] = len(deletable) - i
            break
        path = entry["path"]
        if progress_cb is not None:
            progress_cb(i + 1, len(deletable), os.path.basename(path))
        # Card gate: cheap stat pre-check, then full re-hash. Size+mtime
        # alone cannot detect a swapped card or same-size replacement.
        #
        # lstat, not stat: scan rejects symlinks at classification, but a
        # post-scan swap of the pathname for a symlink to a byte-identical
        # file would let os.stat follow the link — every content gate
        # would then operate on the target, and os.remove would unlink
        # only the link while the delete summary credited the target's
        # full bytes as reclaimed. Rejecting symlinks here keeps the
        # delete anchored to the object the scan hashed.
        try:
            st = os.lstat(path)
        except FileNotFoundError:
            summary["skipped"].append(
                {"path": path, "reason": SKIP_ALREADY_GONE})
            continue
        except OSError as e:
            summary["failed"].append({"path": path, "error": str(e)})
            continue
        if stat_mod.S_ISLNK(st.st_mode):
            summary["skipped"].append(
                {"path": path, "reason": SKIP_SYMLINK})
            continue
        # Codex P2: not just symlinks — any non-regular replacement must
        # be rejected BEFORE compute_file_hash. A scanned zero-byte photo
        # replaced by a FIFO (or socket/device) with size 0 and mtime
        # forced back to the manifest value would pass the S_ISLNK reject
        # and the size/mtime check, then compute_file_hash would block
        # forever opening the pipe — cancellation is checked only at file
        # boundaries, so the delete job hangs.
        if not stat_mod.S_ISREG(st.st_mode):
            summary["skipped"].append(
                {"path": path, "reason": SKIP_NOT_REGULAR})
            continue
        if (st.st_size != entry["size"]
                or st.st_mtime_ns != entry["mtime_ns"]):
            summary["skipped"].append(
                {"path": path, "reason": SKIP_CHANGED})
            continue
        # Archive gate 1 is deliberately before the only full card read.
        # It cheaply rejects an archive that is already unavailable.  The
        # same archive test runs again after hashing, so neither side's
        # verdict can go stale during the other side's potentially slow I/O.
        archive_path, reason = qualify_rows(
            fetch_rows_by_hash(db, entry["hash"]), source_root_real, path,
            contains_check)
        if archive_path is None:
            summary["skipped"].append({"path": path, "reason": reason})
            continue

        # One full card read, after the first archive round trip.  This
        # catches ordinary changes plus same-size/same-mtime replacements;
        # putting it here preserves that protection without reading every
        # card file twice.
        #
        # CodeRabbit: hash the descriptor we open here rather than
        # re-opening the pathname. A rename that swaps the card file for
        # a FIFO after the S_ISREG check on ``st`` but before
        # compute_file_hash would leave the worker blocking inside
        # open() — cancellation is checked at file boundaries only, so
        # the delete job hangs. O_NONBLOCK survives the FIFO open;
        # fstat then re-proves regular-file + dev/inode identity against
        # the scanned baseline before any bytes are read.
        try:
            card_fd = os.open(path, os.O_RDONLY | O_NONBLOCK | O_BINARY)
        except FileNotFoundError:
            summary["skipped"].append(
                {"path": path, "reason": SKIP_ALREADY_GONE})
            continue
        except OSError as e:
            summary["failed"].append({"path": path, "error": str(e)})
            continue
        try:
            try:
                fst = os.fstat(card_fd)
            except OSError as e:
                summary["failed"].append({"path": path, "error": str(e)})
                continue
            if not stat_mod.S_ISREG(fst.st_mode):
                summary["skipped"].append(
                    {"path": path, "reason": SKIP_NOT_REGULAR})
                continue
            if ((fst.st_dev, fst.st_ino) != (st.st_dev, st.st_ino)
                    or fst.st_size != entry["size"]
                    or fst.st_mtime_ns != entry["mtime_ns"]):
                summary["skipped"].append(
                    {"path": path, "reason": SKIP_CHANGED})
                continue
            try:
                current_hash = compute_fd_hash(card_fd)
            except OSError as e:
                summary["failed"].append({"path": path, "error": str(e)})
                continue
        finally:
            os.close(card_fd)
        if current_hash != entry["hash"]:
            summary["skipped"].append(
                {"path": path, "reason": SKIP_CONTENT_CHANGED})
            continue

        # Archive gate 2 makes the archive the last full-copy condition
        # checked before unlink.  If it disappears while the card is being
        # read, deletion is skipped.
        archive_path, reason = qualify_rows(
            fetch_rows_by_hash(db, current_hash), source_root_real, path,
            contains_check)
        if archive_path is None:
            summary["skipped"].append({"path": path, "reason": reason})
            continue
        # Path gate (Codex P1): a parent directory swapped for a symlink
        # DURING the card hash can redirect this pathname to a
        # byte-identical file outside the scanned source — bytes hash
        # identically, both archive gates pass, but os.remove would
        # unlink the external file. Both containment and the final
        # inode/symlink recheck must run AFTER the final hash, not before
        # it. Residual race (swap between these checks and the unlink)
        # is microseconds; full immunity would need dir_fd/O_NOFOLLOW
        # traversal, out of proportion here.
        if not contains_check(os.path.realpath(path)):
            summary["skipped"].append(
                {"path": path, "reason": SKIP_OUTSIDE_SOURCE})
            continue
        # lstat, not stat: a scanned regular file swapped for a symlink
        # would otherwise be followed to its (byte-identical) target,
        # pass every identity check on the target's stats, and then
        # os.remove would unlink only the link while the summary credits
        # the target's full size. lstat sees the link itself, whose
        # inode/size/mtime cannot match the scanned file's baseline.
        try:
            st2 = os.lstat(path)
        except FileNotFoundError:
            summary["skipped"].append(
                {"path": path, "reason": SKIP_ALREADY_GONE})
            continue
        except OSError as e:
            summary["failed"].append({"path": path, "error": str(e)})
            continue
        if stat_mod.S_ISLNK(st2.st_mode):
            summary["skipped"].append(
                {"path": path, "reason": SKIP_SYMLINK})
            continue
        # Codex P2 (mirror of gate 1): a swap for a FIFO/socket/device
        # after the final hash would pass the S_ISLNK reject and the
        # dev/inode baseline (a new inode fails that check anyway), but
        # defence-in-depth: keep both gates identical so an added
        # code path can't relax one without the other.
        if not stat_mod.S_ISREG(st2.st_mode):
            summary["skipped"].append(
                {"path": path, "reason": SKIP_NOT_REGULAR})
            continue
        if ((st2.st_dev, st2.st_ino) != (st.st_dev, st.st_ino)
                or st2.st_size != entry["size"]
                or st2.st_mtime_ns != entry["mtime_ns"]):
            summary["skipped"].append(
                {"path": path, "reason": SKIP_CHANGED})
            continue
        try:
            os.remove(path)
        except OSError as e:
            summary["failed"].append({"path": path, "error": str(e)})
            continue
        summary["deleted"] += 1
        summary["deleted_bytes"] += entry["size"]
    return summary
