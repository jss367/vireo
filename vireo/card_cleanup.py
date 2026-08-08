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
from image_loader import (
    SUPPORTED_EXTENSIONS,
    is_excluded_scan_path,
    safe_iter_dir,
    safe_scan_walk,
)
from scanner import compute_file_hash

MANIFEST_SCHEMA_VERSION = 1
MANIFEST_MAX_AGE_DAYS = 7


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
        os.replace(tmp, path)
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


def classify_source_files(source, recursive=True, onerror=None):
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
                    str(source_path), onerror=onerror):
                for name in filenames:
                    yield Path(dirpath) / name
        entries = _walk()
    else:
        entries = safe_iter_dir(str(source_path), onerror=onerror)
    candidates, ignored = [], []
    for f in entries:
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
KEEP_NOT_VERIFIED = (
    "not verified by a checksummed import — run the integrity audit"
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


def qualify_rows(rows, source_root_real, card_path):
    """Archive-side test from the spec's safety invariant.

    Returns (archive_path, None) for the first row that passes, else
    (None, keep_reason). The archive stat happens HERE, fresh, on every
    call — callers may cache rows, never this function's result.
    """
    if not rows:
        return None, KEEP_NOT_IN_CATALOG
    reason = KEEP_NOT_VERIFIED
    try:
        card_st = os.stat(card_path)
    except OSError:
        return None, KEEP_UNREADABLE
    for row in rows:
        if row["hash_status"] != "ok":
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
        if path_guard.contains_resolved(source_root_real, archive_real):
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
        return archive_path, None
    return None, reason


_ROWS_BY_HASH_SQL = """
    SELECT p.filename, p.file_size, p.file_mtime, p.hash_status,
           f.path AS folder_path
    FROM photos p LEFT JOIN folders f ON f.id = p.folder_id
    WHERE p.file_hash = ?
"""


def fetch_rows_by_hash(db, file_hash):
    return db.conn.execute(_ROWS_BY_HASH_SQL, (file_hash,)).fetchall()


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


def scan_card(db, source, recursive, manifest_dir, scan_job_id,
              progress_cb=None, should_cancel=None):
    source_root_real = os.path.realpath(source)
    walk_errors = []
    candidates, ignored = classify_source_files(
        source, recursive=recursive,
        onerror=lambda e: walk_errors.append(str(e)))
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
            file_hash = compute_file_hash(str(f))
        except OSError as e:
            entries.append({
                "path": str(f), "bucket": "kept",
                "reason": f"{KEEP_UNREADABLE}: {e}",
            })
            totals["kept"]["count"] += 1
            continue
        entry = {
            "path": str(f), "size": st.st_size,
            "mtime_ns": st.st_mtime_ns, "hash": file_hash,
        }
        archive_path, reason = qualify_rows(
            by_hash.get(file_hash, []), source_root_real, str(f))
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
    }
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
    source_root_real = os.path.realpath(manifest["source_root"])
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
        if (st.st_size != entry["size"]
                or st.st_mtime_ns != entry["mtime_ns"]):
            summary["skipped"].append(
                {"path": path, "reason": SKIP_CHANGED})
            continue
        try:
            current_hash = compute_file_hash(path)
        except OSError as e:
            summary["failed"].append({"path": path, "error": str(e)})
            continue
        if current_hash != entry["hash"]:
            summary["skipped"].append(
                {"path": path, "reason": SKIP_CONTENT_CHANGED})
            continue
        # Archive gate: fresh rows, fresh stat — never reused from the
        # scan or from an earlier deletion in this run. Keyed on
        # current_hash (not entry["hash"]) — the equality check just
        # above proves them equal, but this makes the lookup
        # self-evidently correct without needing to trace back to that
        # check.
        archive_path, reason = qualify_rows(
            fetch_rows_by_hash(db, current_hash), source_root_real, path)
        if archive_path is None:
            summary["skipped"].append({"path": path, "reason": reason})
            continue
        # Unlink gate (Codex P1 review): the archive gate above can take
        # SMB-round-trip time, and os.remove resolves the pathname again.
        # If another writer replaced this name meanwhile (camera reusing
        # a filename, sync tool), the bytes at `path` are no longer the
        # ones the card gate hashed. Re-stat and require the same inode
        # AND the same manifest baseline immediately before the unlink —
        # shrinking the race window from network-seconds to the
        # stat-to-remove gap.
        #
        # Also (second Codex P1): a parent directory swapped for a
        # symlink can redirect this pathname to a byte-identical file
        # OUTSIDE the card, which passes every content gate above. The
        # deletion must stay anchored beneath the scanned source root,
        # re-proven at deletion time. Residual race (swap between these
        # checks and the unlink) is microseconds; full immunity would
        # need dir_fd/O_NOFOLLOW traversal, out of proportion here.
        if not path_guard.contains_resolved(
                source_root_real, os.path.realpath(path)):
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
