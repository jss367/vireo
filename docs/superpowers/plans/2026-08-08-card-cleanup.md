# Free Up Card Space (Card Cleanup) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Delete files from a memory card only after verifying — per file, at the moment of deletion — that the identical bytes were checksum-verified into the archive and the archive copy is metadata-unchanged since.

**Architecture:** Two new background jobs (`card-cleanup-scan`, `card-cleanup-delete`) run through the existing `JobRunner`, sharing one archive-side qualifying test in a new `vireo/card_cleanup.py` module. The scan walks the card, hashes every candidate once, matches by content hash against the global catalog, and writes an atomic on-disk manifest; the delete job re-validates both sides (card re-hash, fresh archive stat) immediately before every `os.remove`. The import endpoint's case-aware containment guard is extracted to a shared `vireo/path_guard.py` so the overlap guard uses identical decisions.

**Tech Stack:** Python 3 / Flask, SQLite (`vireo/db.py` `Database`), `JobRunner` SSE jobs, pytest with tmp_path fixtures, vanilla JS in `vireo/templates/import.html`.

**Spec:** `docs/superpowers/specs/2026-08-07-card-cleanup-design.md` — read the "Safety invariant" section before starting. Every gate in this plan traces to it.

---

## File structure

| File | Responsibility |
|---|---|
| Create `vireo/path_guard.py` | Filesystem-aware path containment (case-insensitivity hardened), extracted from the import endpoint's PR #1107 guard. No Vireo imports — pure `os`/`sys`. |
| Create `vireo/card_cleanup.py` | Manifest IO + validation, card walk/classify, the archive-side qualifying test, `scan_card()`, `delete_verified()`. Imports `image_loader`, `scanner`, `path_guard`. Never imports Flask. |
| Modify `vireo/app.py` | Rewire the import destination guard to `path_guard` (behavior-preserving); add `POST /api/card-cleanup/scan`, `POST /api/card-cleanup/delete`, `GET /api/card-cleanup/<scan_job_id>/manifest`. |
| Modify `vireo/templates/import.html` | "Free up card space" UI section. |
| Create `vireo/tests/test_path_guard.py` | Unit tests for containment helper. |
| Create `vireo/tests/test_card_cleanup.py` | Unit tests for manifest IO, classify, qualify, scan, delete. |
| Create `vireo/tests/test_card_cleanup_api.py` | Endpoint tests (fixture mirrors `app_and_db` from `vireo/tests/test_jobs_api.py`). |

Existing symbols you will reuse (verify each exists before writing code that calls it):

- `vireo/image_loader.py:33-35` — `RAW_EXTENSIONS`, `IMAGE_EXTENSIONS`, `SUPPORTED_EXTENSIONS`; `:73` `is_excluded_scan_path`; `:243` `safe_iter_dir`; `:288` `safe_scan_walk`.
- `vireo/scanner.py:105` — `compute_file_hash(file_path)` → sha256 hex digest.
- `vireo/ingest.py:286` — `discover_source_files(source_dir, file_types, recursive, onerror)` (parity target only — do not modify).
- `vireo/db.py:5242` — `Database.add_photo(folder_id, filename, extension, file_size, file_mtime, ..., file_hash=None)`; `:3238` `add_folder(path, ...)`; `:3527` `update_photo_hash_check(photo_id, status, file_hash=None, ...)`.
- `vireo/app.py` ~25641–25750 — the inline `_case_insensitive_platform` / `_casenorm` / `_fs_is_case_insensitive` block and the realpath containment loop you will extract.
- `vireo/app.py` ~19010–19036 — the `verify-hashes` endpoint: the pattern to mirror for job work functions (thread `Database`, `set_active_workspace`, throttled `runner.push_event` progress, `runner.is_cancelled` cancel check, `runner.start(...)`).
- `vireo/labels.py:288` — `_atomic_write_text`: the temp-file + `os.replace` pattern (copy the pattern; do not import labels).
- `vireo/tests/test_jobs_api.py:7156-7250` — the PR #1107 guard tests (`test_import_photos_rejects_destination_inside_source`, `test_import_photos_inconclusive_case_probe_rejects_case_collision`). They must pass unchanged after Task 1.

Conventions: sibling modules use flat imports (`from scanner import compute_file_hash`), matching the rest of the package. Tests run from the repo root.

---

### Task 1: Extract `path_guard.py` from the import endpoint (behavior-preserving)

**Files:**
- Create: `vireo/path_guard.py`
- Create: `vireo/tests/test_path_guard.py`
- Modify: `vireo/app.py` (~25641–25750, the destination-inside-source block)

- [ ] **Step 1: Write the failing tests**

Create `vireo/tests/test_path_guard.py`:

```python
"""Unit tests for the case-aware containment helper.

Extracted from the import endpoint's destination-inside-source guard
(PR #1107). The endpoint-level tests in test_jobs_api.py remain the
behavior-preservation net; these pin the helper's own contract.
"""
import os
import sys

import pytest

from path_guard import contains_resolved, fs_is_case_insensitive, path_contains


def test_contains_equal_and_nested(tmp_path):
    root = str(tmp_path / "card")
    os.makedirs(root)
    assert path_contains(root, root)
    assert path_contains(root, os.path.join(root, "DCIM", "IMG_0001.NEF"))
    assert not path_contains(root, str(tmp_path / "archive"))


def test_contains_prefix_is_not_containment(tmp_path):
    # /card-extra is NOT inside /card even though the string is a prefix.
    root = str(tmp_path / "card")
    os.makedirs(root)
    assert not path_contains(root, str(tmp_path / "card-extra" / "x.jpg"))


def test_contains_follows_symlinks(tmp_path):
    root = tmp_path / "card"
    root.mkdir()
    link = tmp_path / "alias"
    try:
        os.symlink(str(root), str(link))
    except (OSError, NotImplementedError):
        pytest.skip("symlinks unsupported on this filesystem")
    assert path_contains(str(root), str(link / "IMG_0001.NEF"))


@pytest.mark.skipif(
    sys.platform in ("darwin", "win32"),
    reason="Linux-only probe: darwin/win32 always case-fold",
)
def test_inconclusive_probe_casefolds(tmp_path):
    # Numeric-only entries: the probe cannot case-swap, so it must fall
    # back to case-insensitive (the strict direction) and the case-swapped
    # child is treated as contained. Mirrors the PR #1107 endpoint test.
    root = tmp_path / "Card-BAR"
    root.mkdir()
    (root / "100").mkdir()
    assert fs_is_case_insensitive(str(root)) is True
    assert contains_resolved(str(root), str(tmp_path / "card-bar" / "x"))


@pytest.mark.skipif(
    sys.platform in ("darwin", "win32"),
    reason="Linux-only: needs a genuinely case-sensitive filesystem",
)
def test_case_sensitive_fs_distinguishes(tmp_path):
    root = tmp_path / "CardABC"
    root.mkdir()
    (root / "alpha.txt").write_text("x")
    if fs_is_case_insensitive(str(root)):
        pytest.skip("tmp filesystem is case-insensitive")
    assert not contains_resolved(str(root), str(tmp_path / "cardabc" / "x"))


def test_darwin_always_casefolds(tmp_path):
    if sys.platform not in ("darwin", "win32"):
        pytest.skip("case-insensitive-platform path")
    root = tmp_path / "Card"
    root.mkdir()
    swapped = str(tmp_path / "card" / "IMG.NEF")
    assert contains_resolved(str(root), swapped)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest vireo/tests/test_path_guard.py -v`
Expected: FAIL/ERROR with `ModuleNotFoundError: No module named 'path_guard'`

- [ ] **Step 3: Create `vireo/path_guard.py`**

Move the logic verbatim from `vireo/app.py` — same comparisons, same fallbacks. Copy the two long explanatory comments (the platform rationale and the probe docstring) from app.py into this module; they cite PR #1107 and belong with the code:

```python
"""Filesystem-aware path containment.

Extracted from the import endpoint's destination-inside-source guard
(PR #1107) so the card-cleanup overlap guard makes identical decisions.
``realpath`` alone is not enough: macOS/Windows default filesystems are
case-insensitive but realpath does not case-normalize, and FAT/exFAT
removable media on Linux are case-insensitive under a case-sensitive
parent. Inconclusive probes fall back to case-folding — the strict
direction for a containment guard.
"""
import os
import sys


def is_case_insensitive_platform():
    return sys.platform in ("darwin", "win32")


def fs_is_case_insensitive(path):
    # (moved verbatim from app.py's _fs_is_case_insensitive, including
    # its docstring)
    ...


def contains_resolved(root_real, child_real):
    """True if ``child_real`` equals or lies inside ``root_real``.

    Both arguments must already be realpath'd. Case sensitivity is
    decided per root: unconditional case-fold on darwin/win32; on Linux,
    probe the root's actual filesystem.
    """
    if is_case_insensitive_platform() or fs_is_case_insensitive(root_real):
        root_cmp = root_real.casefold().rstrip(os.sep)
        child_cmp = child_real.casefold()
    else:
        root_cmp = root_real.rstrip(os.sep)
        child_cmp = child_real
    return child_cmp == root_cmp or child_cmp.startswith(root_cmp + os.sep)


def path_contains(root, child):
    """realpath both sides, then ``contains_resolved``.

    Unresolvable paths return True — for a guard that *disqualifies*
    when contained, "can't tell" must be the strict direction.
    """
    try:
        root_real = os.path.realpath(root)
        child_real = os.path.realpath(child)
    except OSError:
        return True
    return contains_resolved(root_real, child_real)
```

`fs_is_case_insensitive` is the app.py closure body unchanged (the `os.listdir` try/except returning True, the per-entry case-swap probe, `os.path.exists` → False, `os.path.samefile` → its result, OSError → True).

Note the short-circuit preserves current behavior: on darwin/win32 the probe never runs, exactly like `_case_insensitive_platform or (...)` in app.py.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest vireo/tests/test_path_guard.py -v`
Expected: PASS (platform-specific tests skip as marked)

- [ ] **Step 5: Rewire app.py to use the helper**

In the import-photos endpoint block (~25641), delete the inline `_case_insensitive_platform`, `_casenorm`, and `_fs_is_case_insensitive` definitions and replace the containment loop body so it reads:

```python
        try:
            dest_real = os.path.realpath(destination)
        except OSError as e:
            return json_error(f"destination cannot be resolved: {e}")
        for s in sources:
            try:
                source_real = os.path.realpath(s)
            except OSError:
                # Source unresolvable — the os.path.isdir check above
                # already handled non-existent sources; nothing more to say.
                continue
            if path_guard.contains_resolved(source_real, dest_real):
                return json_error(
                    f"destination cannot be inside a source directory "
                    f"(destination={destination!r}, source={s!r}); "
                    f"formatting the card would erase the archive copy"
                )
```

Add `import path_guard` with app.py's other top-level flat sibling imports. Keep the big explanatory comment above the loop (trim only what moved into path_guard's module docstring). If `_casenorm` or `_fs_is_case_insensitive` have any *other* call sites in app.py (grep first), point them at `path_guard` too.

- [ ] **Step 6: Run the behavior-preservation net**

Run: `python -m pytest vireo/tests/test_jobs_api.py -k "destination_inside_source or case_probe" -v && python -m pytest vireo/tests/test_path_guard.py -v`
Expected: PASS — the endpoint tests unchanged, proving the extraction preserved guard decisions.

- [ ] **Step 7: Commit**

```bash
git add vireo/path_guard.py vireo/tests/test_path_guard.py vireo/app.py
git commit -m "refactor: extract case-aware path containment into path_guard

Behavior-preserving extraction of the import endpoint's PR #1107
destination-inside-source guard, so card cleanup can share it."
```

---

### Task 2: Manifest IO — atomic write, validated load, prune

**Files:**
- Create: `vireo/card_cleanup.py`
- Create: `vireo/tests/test_card_cleanup.py`

- [ ] **Step 1: Write the failing tests**

Create `vireo/tests/test_card_cleanup.py`:

```python
"""Unit tests for card cleanup: manifest IO, classification, and the
scan/delete safety gates. Spec:
docs/superpowers/specs/2026-08-07-card-cleanup-design.md
"""
import json
import os
from datetime import datetime, timedelta, timezone

import pytest

import card_cleanup
from card_cleanup import (
    ManifestError, load_manifest, prune_manifests, write_manifest,
)


def _manifest(tmp_path, **overrides):
    m = {
        "schema_version": card_cleanup.MANIFEST_SCHEMA_VERSION,
        "scan_job_id": "scan-1",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source_root": str(tmp_path / "card"),
        "recursive": True,
        "entries": [],
        "walk_errors": [],
        "totals": {},
    }
    m.update(overrides)
    return m


def test_write_then_load_roundtrip(tmp_path):
    mdir = str(tmp_path / "manifests")
    (tmp_path / "card").mkdir()
    write_manifest(mdir, _manifest(tmp_path))
    loaded = load_manifest(mdir, "scan-1")
    assert loaded["scan_job_id"] == "scan-1"


def test_write_is_atomic_no_leftover_tmp(tmp_path):
    mdir = str(tmp_path / "manifests")
    write_manifest(mdir, _manifest(tmp_path))
    names = os.listdir(mdir)
    assert names == ["scan-1.json"]


def test_load_missing_manifest_is_404(tmp_path):
    with pytest.raises(ManifestError) as exc:
        load_manifest(str(tmp_path), "nope")
    assert exc.value.http_status == 404


def test_load_rejects_corrupt_json(tmp_path):
    mdir = tmp_path / "manifests"
    mdir.mkdir()
    (mdir / "scan-1.json").write_text("{truncated")
    with pytest.raises(ManifestError) as exc:
        load_manifest(str(mdir), "scan-1")
    assert exc.value.http_status == 400


def test_load_rejects_unknown_schema(tmp_path):
    mdir = str(tmp_path / "manifests")
    write_manifest(mdir, _manifest(tmp_path, schema_version=99))
    with pytest.raises(ManifestError):
        load_manifest(mdir, "scan-1")


def test_load_rejects_missing_source_root(tmp_path):
    mdir = str(tmp_path / "manifests")
    write_manifest(mdir, _manifest(tmp_path, source_root=""))
    with pytest.raises(ManifestError):
        load_manifest(mdir, "scan-1")


def test_load_rejects_deletable_entry_outside_source_root(tmp_path):
    (tmp_path / "card").mkdir()
    entry = {
        "path": str(tmp_path / "elsewhere" / "x.nef"),
        "size": 1, "mtime_ns": 1, "hash": "h", "bucket": "deletable",
    }
    mdir = str(tmp_path / "manifests")
    write_manifest(mdir, _manifest(tmp_path, entries=[entry]))
    with pytest.raises(ManifestError):
        load_manifest(mdir, "scan-1")


def test_load_rejects_expired_manifest_at_request_time(tmp_path):
    (tmp_path / "card").mkdir()
    old = (datetime.now(timezone.utc) - timedelta(days=8)).isoformat()
    mdir = str(tmp_path / "manifests")
    write_manifest(mdir, _manifest(tmp_path, created_at=old))
    with pytest.raises(ManifestError) as exc:
        load_manifest(mdir, "scan-1")
    assert exc.value.http_status == 404
    assert "re-scan" in str(exc.value)


def test_prune_removes_only_old_manifests(tmp_path):
    mdir = tmp_path / "manifests"
    mdir.mkdir()
    old_file = mdir / "old.json"
    new_file = mdir / "new.json"
    old_file.write_text("{}")
    new_file.write_text("{}")
    eight_days = 8 * 86400
    stale = os.stat(old_file).st_mtime - eight_days
    os.utime(old_file, (stale, stale))
    prune_manifests(str(mdir))
    assert not old_file.exists()
    assert new_file.exists()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest vireo/tests/test_card_cleanup.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'card_cleanup'`

- [ ] **Step 3: Implement manifest IO in `vireo/card_cleanup.py`**

```python
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
import tempfile
import time
from datetime import datetime, timezone
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
    if not os.path.isdir(manifest_dir):
        return
    cutoff = time.time() - max_age_days * 86400
    for name in os.listdir(manifest_dir):
        if not name.endswith(".json"):
            continue
        full = os.path.join(manifest_dir, name)
        with contextlib.suppress(OSError):
            if os.path.getmtime(full) < cutoff:
                os.unlink(full)


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
        raise ManifestError(f"manifest unreadable or corrupt: {e}") from e
    if (not isinstance(manifest, dict)
            or manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION):
        raise ManifestError("manifest schema not recognized — re-scan the card")
    source_root = manifest.get("source_root")
    if not source_root or not os.path.isabs(str(source_root)):
        raise ManifestError("manifest missing source root — re-scan the card")
    try:
        created = datetime.fromisoformat(manifest.get("created_at"))
    except (TypeError, ValueError):
        raise ManifestError("manifest missing timestamp — re-scan the card")
    age = datetime.now(timezone.utc) - created
    # Age is enforced here — at request time — not only by the
    # scan-start prune.
    if age.total_seconds() > max_age_days * 86400:
        raise ManifestError(
            "manifest expired — re-scan the card", http_status=404)
    entries = manifest.get("entries")
    if not isinstance(entries, list):
        raise ManifestError("manifest entries malformed — re-scan the card")
    for entry in entries:
        if entry.get("bucket") != "deletable":
            continue
        if not path_guard.path_contains(source_root, str(entry.get("path", ""))):
            raise ManifestError(
                "manifest entry outside its source root — re-scan the card")
    return manifest
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest vireo/tests/test_card_cleanup.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add vireo/card_cleanup.py vireo/tests/test_card_cleanup.py
git commit -m "feat: card-cleanup manifest IO — atomic writes, validated load, expiry"
```

---

### Task 3: Card walk + classification (with discovery parity)

**Files:**
- Modify: `vireo/card_cleanup.py`
- Modify: `vireo/tests/test_card_cleanup.py`

- [ ] **Step 1: Write the failing tests** (append to `test_card_cleanup.py`)

```python
from card_cleanup import classify_source_files


def _make_card(tmp_path):
    card = tmp_path / "card"
    (card / "DCIM" / "100").mkdir(parents=True)
    (card / "DCIM" / "100" / "IMG_0001.NEF").write_bytes(b"raw-one")
    (card / "DCIM" / "100" / "IMG_0002.JPG").write_bytes(b"jpg-two")
    (card / "DCIM" / "100" / "IMG_0001.XMP").write_bytes(b"sidecar")
    (card / "DCIM" / "100" / ".hidden.jpg").write_bytes(b"dot")
    (card / "MISC" / "sub").mkdir(parents=True)
    (card / "MISC" / "sub" / "firmware.bin").write_bytes(b"fw")
    return card


def test_classify_buckets(tmp_path):
    card = _make_card(tmp_path)
    candidates, ignored = classify_source_files(str(card))
    cand_names = {p.name for p in candidates}
    ign_names = {p.name for p in ignored}
    assert cand_names == {"IMG_0001.NEF", "IMG_0002.JPG"}
    assert ign_names == {"IMG_0001.XMP", ".hidden.jpg", "firmware.bin"}


def test_classify_parity_with_discover_source_files(tmp_path):
    # The deletable set may never exceed what an import would consider a
    # photo — pin our filter to discovery's, byte for byte.
    from ingest import discover_source_files
    card = _make_card(tmp_path)
    candidates, _ = classify_source_files(str(card), recursive=True)
    assert candidates == discover_source_files(
        str(card), file_types="both", recursive=True)
    candidates_flat, _ = classify_source_files(str(card), recursive=False)
    assert candidates_flat == discover_source_files(
        str(card), file_types="both", recursive=False)


def test_classify_missing_source_reports_onerror(tmp_path):
    errors = []
    candidates, ignored = classify_source_files(
        str(tmp_path / "nope"), onerror=errors.append)
    assert candidates == [] and ignored == []
    assert len(errors) == 1 and isinstance(errors[0], OSError)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest vireo/tests/test_card_cleanup.py -k classify -v`
Expected: FAIL with `ImportError: cannot import name 'classify_source_files'`

- [ ] **Step 3: Implement `classify_source_files`** (add to `card_cleanup.py`)

This mirrors `discover_source_files` (`ingest.py:286`) — same excluded-bundle handling, same synthetic-OSError contract for unwalkable roots, same filter — but keeps the non-matching files instead of dropping them:

```python
def classify_source_files(source, recursive=True, onerror=None):
    """One walk over the card; returns (candidates, ignored), both sorted.

    Mirrors discover_source_files' file_types="both" filter exactly —
    parity is pinned by a test — but also returns the non-photo files so
    the preview can show an "ignored, never touched" bucket without a
    second walk (discover_source_files drops them).
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
        if not f.is_file():
            continue
        if (f.suffix.lower() in SUPPORTED_EXTENSIONS
                and not f.name.startswith(".")):
            candidates.append(f)
        else:
            ignored.append(f)
    return sorted(candidates), sorted(ignored)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest vireo/tests/test_card_cleanup.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add vireo/card_cleanup.py vireo/tests/test_card_cleanup.py
git commit -m "feat: card-cleanup walk/classify with discovery filter parity"
```

---

### Task 4: The archive-side qualifying test + `scan_card`

**Files:**
- Modify: `vireo/card_cleanup.py`
- Modify: `vireo/tests/test_card_cleanup.py`

- [ ] **Step 1: Write the failing tests** (append to `test_card_cleanup.py`)

These use a real `Database` on a temp file, with archive files on disk so stats work. The helper builds one cataloged, hash-verified archive photo whose bytes match a card file:

```python
from scanner import compute_file_hash as _sha


# The `db` fixture comes from vireo/tests/conftest.py (~line 158) — a
# Database on a temp file. Do not redefine it here.


def _archive_photo(db, tmp_path, name="IMG_0001.NEF", content=b"raw-one",
                   hash_status="ok", folder="archive/2026/2026-08-01"):
    """Create an archive file on disk + its cataloged, verified row."""
    folder_path = tmp_path / folder
    folder_path.mkdir(parents=True, exist_ok=True)
    f = folder_path / name
    f.write_bytes(content)
    st = os.stat(f)
    fid = db.add_folder(str(folder_path))
    pid = db.add_photo(
        folder_id=fid, filename=name, extension=os.path.splitext(name)[1],
        file_size=st.st_size, file_mtime=st.st_mtime,
        file_hash=_sha(str(f)),
    )
    if hash_status is not None:
        db.update_photo_hash_check(pid, hash_status)
    return f, pid


def _card_file(tmp_path, name="IMG_0001.NEF", content=b"raw-one"):
    card = tmp_path / "card" / "DCIM"
    card.mkdir(parents=True, exist_ok=True)
    f = card / name
    f.write_bytes(content)
    return f


def _scan(db, tmp_path, **kwargs):
    return card_cleanup.scan_card(
        db, str(tmp_path / "card"), True,
        str(tmp_path / "manifests"), "scan-1", **kwargs)


def _entries(result, bucket):
    return [e for e in result["entries"] if e["bucket"] == bucket]


def test_scan_verified_file_is_deletable(db, tmp_path):
    archive_file, _ = _archive_photo(db, tmp_path)
    _card_file(tmp_path)
    result = _scan(db, tmp_path)
    deletable = _entries(result, "deletable")
    assert len(deletable) == 1
    assert deletable[0]["archive_path"] == str(archive_file)
    assert result["totals"]["deletable"]["count"] == 1
    # Manifest landed on disk and revalidates.
    loaded = load_manifest(str(tmp_path / "manifests"), "scan-1")
    assert loaded["source_root"] == os.path.realpath(str(tmp_path / "card"))


def test_scan_uncataloged_file_kept(db, tmp_path):
    _card_file(tmp_path, content=b"never-imported")
    result = _scan(db, tmp_path)
    kept = _entries(result, "kept")
    assert len(kept) == 1 and "not in catalog" in kept[0]["reason"]


def test_scan_null_hash_status_kept_with_audit_remedy(db, tmp_path):
    # Scan-cataloged archives: file_hash set, hash_status NULL. Kept —
    # and the reason must point at the remedy, or the tool reads broken.
    _archive_photo(db, tmp_path, hash_status=None)
    _card_file(tmp_path)
    result = _scan(db, tmp_path)
    kept = _entries(result, "kept")
    assert len(kept) == 1
    assert "integrity audit" in kept[0]["reason"]


def test_scan_archive_file_missing_kept(db, tmp_path):
    archive_file, _ = _archive_photo(db, tmp_path)
    _card_file(tmp_path)
    os.unlink(archive_file)
    result = _scan(db, tmp_path)
    assert len(_entries(result, "kept")) == 1
    assert len(_entries(result, "deletable")) == 0


def test_scan_archive_mtime_off_baseline_kept(db, tmp_path):
    # Exact equality, not the audit's 1s window: any drift keeps the file.
    archive_file, _ = _archive_photo(db, tmp_path)
    _card_file(tmp_path)
    st = os.stat(archive_file)
    os.utime(archive_file, (st.st_atime, st.st_mtime + 0.5))
    result = _scan(db, tmp_path)
    assert len(_entries(result, "deletable")) == 0
    assert "changed since verification" in _entries(result, "kept")[0]["reason"]


def test_scan_self_match_inside_source_never_qualifies(db, tmp_path):
    # The only catalog copy lives inside the selected source tree: the
    # file must NOT be deletable (it would be deleting the archive copy).
    _archive_photo(db, tmp_path, folder="card/DCIM")
    result = _scan(db, tmp_path)
    assert len(_entries(result, "deletable")) == 0
    kept = _entries(result, "kept")
    assert len(kept) == 1 and "inside the selected source" in kept[0]["reason"]


def test_qualify_rejects_hardlink_alias_of_card_file(db, tmp_path):
    # Mount-alias/samefile gate: the cataloged "archive copy" lives
    # outside the source tree by path, but it's a hardlink to the card
    # file itself — same dev+inode. Deleting the card file would leave
    # the archive path as the only name for bytes we just proved existed
    # twice; the spec says such a row never qualifies.
    card = _card_file(tmp_path)
    folder_path = tmp_path / "archive" / "2026"
    folder_path.mkdir(parents=True)
    alias = folder_path / "IMG_0001.NEF"
    try:
        os.link(card, alias)
    except (OSError, NotImplementedError):
        pytest.skip("hardlinks unsupported on this filesystem")
    st = os.stat(alias)
    fid = db.add_folder(str(folder_path))
    pid = db.add_photo(
        folder_id=fid, filename="IMG_0001.NEF", extension=".NEF",
        file_size=st.st_size, file_mtime=st.st_mtime,
        file_hash=_sha(str(alias)),
    )
    db.update_photo_hash_check(pid, "ok")
    result = _scan(db, tmp_path)
    assert len(_entries(result, "deletable")) == 0
    kept = _entries(result, "kept")
    assert len(kept) == 1 and "inside the selected source" in kept[0]["reason"]


def test_scan_duplicate_card_files_both_deletable(db, tmp_path):
    _archive_photo(db, tmp_path)
    _card_file(tmp_path, name="IMG_0001.NEF")
    _card_file(tmp_path, name="IMG_0001_copy.NEF")
    result = _scan(db, tmp_path)
    assert len(_entries(result, "deletable")) == 2


def test_scan_two_rows_one_qualifying_is_deletable(db, tmp_path):
    # Same hash cataloged twice; only one row passes → still deletable,
    # preview shows the passing row's path.
    bad_archive, _ = _archive_photo(db, tmp_path, folder="archive/a")
    good_archive, _ = _archive_photo(db, tmp_path, folder="archive/b")
    os.unlink(bad_archive)
    _card_file(tmp_path)
    result = _scan(db, tmp_path)
    deletable = _entries(result, "deletable")
    assert len(deletable) == 1
    assert deletable[0]["archive_path"] == str(good_archive)


def test_scan_cancellation_writes_no_manifest(db, tmp_path):
    _archive_photo(db, tmp_path)
    _card_file(tmp_path)
    result = _scan(db, tmp_path, should_cancel=lambda: True)
    assert result["cancelled"] is True
    assert not os.path.exists(
        card_cleanup.manifest_path(str(tmp_path / "manifests"), "scan-1"))


def test_scan_hashes_each_card_file_once(db, tmp_path, monkeypatch):
    _archive_photo(db, tmp_path)
    _card_file(tmp_path)
    calls = []
    real = card_cleanup.compute_file_hash

    def counting(path, *a, **kw):
        calls.append(str(path))
        return real(path, *a, **kw)

    monkeypatch.setattr(card_cleanup, "compute_file_hash", counting)
    _scan(db, tmp_path)
    card_calls = [p for p in calls if "card" in p]
    assert len(card_calls) == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest vireo/tests/test_card_cleanup.py -k scan -v`
Expected: FAIL with `AttributeError: ... no attribute 'scan_card'`

- [ ] **Step 3: Implement the qualifying test and `scan_card`** (add to `card_cleanup.py`)

The qualifying test is ONE function used by both scan and delete — the spec's invariant restated in code once, so the two jobs cannot drift. It takes rows (scan passes prefetched rows; delete passes a fresh query) and always stats the archive fresh.

Deliberate substitution vs. the spec: the spec names `DuplicateChecker(CatalogIndex.from_db(db), verify_by_hash=True)` as the matching mechanism, but `CatalogIndex` stores only hash *sets* while the qualifying test needs full rows (`hash_status`, size/mtime baseline, folder path) — which the spec itself acknowledges requires a `photos WHERE file_hash = ?` lookup anyway. Querying rows directly is semantically identical for membership (only rows with a stored `file_hash` can carry `hash_status='ok'`) and skips a redundant layer; the hash-once rule the spec attaches to the checker is satisfied by `scan_card` computing each card hash exactly once (pinned by `test_scan_hashes_each_card_file_once`).

```python
KEEP_NOT_IN_CATALOG = "not in catalog — not imported yet"
KEEP_NOT_VERIFIED = (
    "not verified by a checksummed import — run the integrity audit"
)
KEEP_INSIDE_SOURCE = "only catalog copy is inside the selected source"
KEEP_ARCHIVE_UNREACHABLE = "archive file not reachable"
KEEP_ARCHIVE_CHANGED = "archive file changed since verification"
KEEP_UNREADABLE = "could not read card file"


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
        if path_guard.contains_resolved(
                source_root_real, os.path.realpath(archive_path)):
            reason = KEEP_INSIDE_SOURCE
            continue
        try:
            ast = os.stat(archive_path)
        except OSError:
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
        source, recursive=recursive, onerror=lambda e: walk_errors.append(str(e)))
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
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source_root": source_root_real,
        "recursive": bool(recursive),
        "entries": entries,
        "walk_errors": walk_errors,
        "totals": totals,
    }
    write_manifest(manifest_dir, manifest)
    manifest["cancelled"] = False
    return manifest
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest vireo/tests/test_card_cleanup.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add vireo/card_cleanup.py vireo/tests/test_card_cleanup.py
git commit -m "feat: card-cleanup scan — hash-once matching, archive qualifying test"
```

---

### Task 5: `delete_verified` — the gates at the destructive moment

**Files:**
- Modify: `vireo/card_cleanup.py`
- Modify: `vireo/tests/test_card_cleanup.py`

- [ ] **Step 1: Write the failing tests** (append to `test_card_cleanup.py`)

```python
def _scan_then_delete(db, tmp_path, mutate=None, should_cancel=None):
    scan = _scan(db, tmp_path)
    assert scan["totals"]["deletable"]["count"] >= 1
    manifest = load_manifest(str(tmp_path / "manifests"), "scan-1")
    if mutate is not None:
        mutate()
    return card_cleanup.delete_verified(
        db, manifest, should_cancel=should_cancel)


def test_delete_happy_path_two_duplicates_both_deleted(db, tmp_path):
    # Spec: two identical card files matching one archive photo — both
    # deletable, both deleted.
    _archive_photo(db, tmp_path)
    card_a = _card_file(tmp_path, name="IMG_0001.NEF")
    card_b = _card_file(tmp_path, name="IMG_0001_copy.NEF")
    summary = _scan_then_delete(db, tmp_path)
    assert summary["deleted"] == 2
    assert not card_a.exists() and not card_b.exists()
    assert summary["skipped"] == [] and summary["failed"] == []


def test_delete_skips_file_changed_since_scan(db, tmp_path):
    _archive_photo(db, tmp_path)
    card = _card_file(tmp_path)

    def rewrite():
        card.write_bytes(b"new-shot-reusing-name")

    summary = _scan_then_delete(db, tmp_path, mutate=rewrite)
    assert summary["deleted"] == 0
    assert len(summary["skipped"]) == 1
    assert card.exists()


def test_delete_rehash_catches_same_size_same_mtime_swap(db, tmp_path):
    # Same byte count, mtime forced back to the manifest value: only the
    # delete-time re-hash can catch this (FAT mtimes are 2s-granular).
    _archive_photo(db, tmp_path)
    card = _card_file(tmp_path)  # content b"raw-one", 7 bytes
    st = os.stat(card)

    def swap():
        card.write_bytes(b"raw-two")  # also 7 bytes
        os.utime(card, ns=(st.st_atime_ns, st.st_mtime_ns))

    summary = _scan_then_delete(db, tmp_path, mutate=swap)
    assert summary["deleted"] == 0
    assert len(summary["skipped"]) == 1
    assert card.exists()


def test_delete_archive_removed_after_scan_skips(db, tmp_path):
    archive_file, _ = _archive_photo(db, tmp_path)
    card = _card_file(tmp_path)
    summary = _scan_then_delete(
        db, tmp_path, mutate=lambda: os.unlink(archive_file))
    assert summary["deleted"] == 0
    assert card.exists()
    assert "archive" in summary["skipped"][0]["reason"]


def test_delete_no_stat_reuse_across_duplicates(db, tmp_path):
    # Two identical card files; archive copy vanishes after the first
    # deletion. The second file's own fresh stat must fail — a cached
    # scan-time (or first-delete-time) result would wrongly authorize it.
    archive_file, _ = _archive_photo(db, tmp_path)
    card_a = _card_file(tmp_path, name="IMG_0001.NEF")
    card_b = _card_file(tmp_path, name="IMG_0002.NEF")
    scan = _scan(db, tmp_path)
    assert scan["totals"]["deletable"]["count"] == 2
    manifest = load_manifest(str(tmp_path / "manifests"), "scan-1")

    deleted_once = []
    real_remove = os.remove

    def remove_then_kill_archive(path, *a, **kw):
        real_remove(path, *a, **kw)
        if not deleted_once:
            deleted_once.append(path)
            real_remove(archive_file)

    import unittest.mock
    with unittest.mock.patch.object(
            card_cleanup.os, "remove", remove_then_kill_archive):
        summary = card_cleanup.delete_verified(db, manifest)
    assert summary["deleted"] == 1
    assert len(summary["skipped"]) == 1
    assert card_a.exists() != card_b.exists()  # exactly one survived


def test_delete_cancellation_honest_summary(db, tmp_path):
    _archive_photo(db, tmp_path)
    _card_file(tmp_path, name="IMG_0001.NEF")
    _card_file(tmp_path, name="IMG_0002.NEF")
    calls = []

    def cancel_after_first():
        calls.append(1)
        return len(calls) > 1

    summary = _scan_then_delete(
        db, tmp_path, should_cancel=cancel_after_first)
    assert summary["cancelled"] is True
    assert summary["deleted"] + summary["remaining"] == 2


def test_delete_vanished_card_file_counts_skipped(db, tmp_path):
    _archive_photo(db, tmp_path)
    card = _card_file(tmp_path)
    summary = _scan_then_delete(db, tmp_path, mutate=lambda: os.unlink(card))
    assert summary["deleted"] == 0
    assert "already gone" in summary["skipped"][0]["reason"]


def test_delete_per_file_failure_continues(db, tmp_path):
    # A permission error on one file is recorded as failed; the job
    # moves on and still deletes the rest.
    _archive_photo(db, tmp_path)
    card_a = _card_file(tmp_path, name="IMG_0001.NEF")
    card_b = _card_file(tmp_path, name="IMG_0002.NEF")
    scan = _scan(db, tmp_path)
    assert scan["totals"]["deletable"]["count"] == 2
    manifest = load_manifest(str(tmp_path / "manifests"), "scan-1")
    real_remove = os.remove
    failed_path = str(card_a)

    def failing_remove(path, *a, **kw):
        if str(path) == failed_path:
            raise PermissionError(13, "read-only card", failed_path)
        real_remove(path, *a, **kw)

    import unittest.mock
    with unittest.mock.patch.object(
            card_cleanup.os, "remove", failing_remove):
        summary = card_cleanup.delete_verified(db, manifest)
    assert summary["deleted"] == 1
    assert len(summary["failed"]) == 1
    assert summary["failed"][0]["path"] == failed_path
    assert card_a.exists() and not card_b.exists()


def test_delete_only_touches_deletable_bucket(db, tmp_path):
    _archive_photo(db, tmp_path)
    _card_file(tmp_path)
    stray = _card_file(tmp_path, name="IMG_KEEP.NEF", content=b"unimported")
    summary = _scan_then_delete(db, tmp_path)
    assert summary["deleted"] == 1
    assert stray.exists()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest vireo/tests/test_card_cleanup.py -k delete -v`
Expected: FAIL with `AttributeError: ... no attribute 'delete_verified'`

- [ ] **Step 3: Implement `delete_verified`** (add to `card_cleanup.py`)

```python
def delete_verified(db, manifest, progress_cb=None, should_cancel=None):
    """Delete the manifest's deletable bucket, re-proving the invariant
    per file immediately before each unlink. Never reads the kept or
    ignored buckets."""
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
        try:
            st = os.stat(path)
        except FileNotFoundError:
            summary["skipped"].append(
                {"path": path, "reason": "already gone from the card"})
            continue
        except OSError as e:
            summary["failed"].append({"path": path, "error": str(e)})
            continue
        if (st.st_size != entry["size"]
                or st.st_mtime_ns != entry["mtime_ns"]):
            summary["skipped"].append(
                {"path": path, "reason": "changed on the card since the scan"})
            continue
        try:
            current_hash = compute_file_hash(path)
        except OSError as e:
            summary["failed"].append({"path": path, "error": str(e)})
            continue
        if current_hash != entry["hash"]:
            summary["skipped"].append(
                {"path": path,
                 "reason": "content changed on the card since the scan"})
            continue
        # Archive gate: fresh rows, fresh stat — never reused from the
        # scan or from an earlier deletion in this run.
        archive_path, reason = qualify_rows(
            fetch_rows_by_hash(db, entry["hash"]), source_root_real, path)
        if archive_path is None:
            summary["skipped"].append({"path": path, "reason": reason})
            continue
        try:
            os.remove(path)
        except OSError as e:
            summary["failed"].append({"path": path, "error": str(e)})
            continue
        summary["deleted"] += 1
        summary["deleted_bytes"] += entry["size"]
    return summary
```

- [ ] **Step 4: Run the full module's tests**

Run: `python -m pytest vireo/tests/test_card_cleanup.py vireo/tests/test_path_guard.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add vireo/card_cleanup.py vireo/tests/test_card_cleanup.py
git commit -m "feat: card-cleanup delete — re-verified card and archive gates per unlink"
```

---

### Task 6: Endpoints and job wiring in app.py

**Files:**
- Modify: `vireo/app.py`
- Create: `vireo/tests/test_card_cleanup_api.py`

Before coding: read the `verify-hashes` endpoint (`vireo/app.py` ~18990–19036) end to end and mirror how it obtains `active_ws`, constructs the thread `Database`, calls `set_active_workspace`, throttles `push_event`, and wires `is_cancelled`. One deliberate deviation: verify-hashes never closes its thread `Database`; the card-cleanup work functions should wrap the body in `try/finally: thread_db.close()` — that is an improvement, not a copy. `json_error` already accepts a status parameter (`json_error(msg, status=400)`, `vireo/app.py:3273`), so use it for 404/409 too.

- [ ] **Step 1: Write the failing endpoint tests**

Create `vireo/tests/test_card_cleanup_api.py`. The `app_and_db` fixture comes from `vireo/tests/conftest.py:168` — the new file gets it for free; do NOT redefine it (a local copy would shadow the shared one). Note it pre-seeds folder rows `/photos/2024` and `/photos/2024/January`; those paths don't exist on disk and pass the overlap guard harmlessly, but they do sit in `folders`, so don't assert on folder counts. Copy only the job-completion-wait helper pattern from `vireo/tests/test_jobs_api.py`. Then:

```python
def _make_verified_pair(db, tmp_path):
    # archive file + verified row + matching card file (bytes b"raw-one")
    ...  # same helpers as test_card_cleanup.py: _archive_photo/_card_file


def test_scan_rejects_missing_source(app_and_db):
    app, _ = app_and_db
    resp = app.test_client().post(
        "/api/card-cleanup/scan", json={"source": "/nope/missing"})
    assert resp.status_code == 400


def test_scan_rejects_archive_overlap(app_and_db, tmp_path):
    # Source equals a cataloged folder root → 400 before any job starts.
    app, db = app_and_db
    archive = tmp_path / "archive"
    archive.mkdir()
    db.add_folder(str(archive))
    resp = app.test_client().post(
        "/api/card-cleanup/scan", json={"source": str(archive)})
    assert resp.status_code == 400
    assert "removable media" in resp.get_json()["error"]
    # ... a source that CONTAINS the archive root:
    resp = app.test_client().post(
        "/api/card-cleanup/scan", json={"source": str(tmp_path)})
    assert resp.status_code == 400
    # ... and a source INSIDE the archive root:
    sub = archive / "2026"
    sub.mkdir()
    resp = app.test_client().post(
        "/api/card-cleanup/scan", json={"source": str(sub)})
    assert resp.status_code == 400


def test_scan_rejects_case_swapped_overlap(app_and_db, tmp_path):
    # Case-swapped source naming the same directory as a cataloged root
    # must be rejected on case-insensitive platforms (spec: overlap
    # fail-fast under the containment rules, not string realpath).
    import sys
    if sys.platform not in ("darwin", "win32"):
        pytest.skip("needs an unconditionally case-folding platform")
    app, db = app_and_db
    archive = tmp_path / "Archive"
    archive.mkdir()
    db.add_folder(str(archive))
    swapped = str(tmp_path / "archive")
    resp = app.test_client().post(
        "/api/card-cleanup/scan", json={"source": swapped})
    assert resp.status_code == 400


def test_scan_then_manifest_then_delete_end_to_end(app_and_db, tmp_path):
    app, db = app_and_db
    client = app.test_client()
    _make_verified_pair(db, tmp_path)
    resp = client.post("/api/card-cleanup/scan",
                       json={"source": str(tmp_path / "card")})
    assert resp.status_code == 200
    scan_job_id = resp.get_json()["job_id"]
    _wait_for_job(client, scan_job_id)          # completed
    resp = client.get(f"/api/card-cleanup/{scan_job_id}/manifest")
    assert resp.status_code == 200
    assert resp.get_json()["totals"]["deletable"]["count"] == 1
    resp = client.post("/api/card-cleanup/delete",
                       json={"scan_job_id": scan_job_id})
    assert resp.status_code == 200
    _wait_for_job(client, resp.get_json()["job_id"])
    assert not (tmp_path / "card" / "DCIM" / "IMG_0001.NEF").exists()


def test_delete_unknown_scan_job_404(app_and_db):
    app, _ = app_and_db
    resp = app.test_client().post(
        "/api/card-cleanup/delete", json={"scan_job_id": "nope"})
    assert resp.status_code == 404


def test_delete_refuses_cancelled_scan(app_and_db, tmp_path):
    # A scan that was cancelled must not authorize a delete, even with a
    # manifest present. Seed job_history directly (also proves the
    # history fallback path is consulted for status, not just existence).
    app, db = app_and_db
    db.conn.execute(
        "INSERT INTO job_history (id, type, status, started_at) "
        "VALUES (?, ?, ?, ?)",
        ("scan-c", "card-cleanup-scan", "cancelled", "2026-08-08T00:00:00"))
    db.conn.commit()
    resp = app.test_client().post(
        "/api/card-cleanup/delete", json={"scan_job_id": "scan-c"})
    assert resp.status_code == 400


def test_delete_after_restart_uses_history_and_disk_manifest(
        app_and_db, tmp_path):
    # Restart recovery: the runner no longer knows the scan job, but the
    # job_history row and the on-disk manifest are enough to delete.
    # Seed both directly instead of monkeypatching runner eviction.
    app, db = app_and_db
    client = app.test_client()
    _make_verified_pair(db, tmp_path)
    import card_cleanup
    manifest_dir = app.config["CARD_CLEANUP_DIR"]  # expose in create_app
    card = tmp_path / "card" / "DCIM" / "IMG_0001.NEF"
    scan = card_cleanup.scan_card(
        db, str(tmp_path / "card"), True, manifest_dir, "scan-r")
    assert scan["totals"]["deletable"]["count"] == 1
    db.conn.execute(
        "INSERT INTO job_history (id, type, status, started_at) "
        "VALUES (?, ?, ?, ?)",
        ("scan-r", "card-cleanup-scan", "completed", "2026-08-08T00:00:00"))
    db.conn.commit()
    resp = client.post(
        "/api/card-cleanup/delete", json={"scan_job_id": "scan-r"})
    assert resp.status_code == 200
    _wait_for_job(client, resp.get_json()["job_id"])
    assert not card.exists()


def test_delete_expired_manifest_404(app_and_db, tmp_path):
    # Complete a scan, then age the manifest header past 7 days by
    # rewriting created_at; the delete endpoint must reject it at
    # request time.
    ...
    assert resp.status_code == 404
    assert "re-scan" in resp.get_json()["error"]


def test_delete_concurrent_delete_409(app_and_db, tmp_path):
    # Start a delete, then POST a second while the first is running
    # (use a card with enough files, or monkeypatch a slow gate).
    ...
    assert second.status_code == 409
```

Fill in the `...` bodies following the fixture's conventions; each test must assert the exact status code and, where shown, the error copy.

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest vireo/tests/test_card_cleanup_api.py -v`
Expected: FAIL with 404s (routes don't exist)

- [ ] **Step 3: Implement the endpoints**

In `create_app`, next to the other path setup, add:

```python
    card_cleanup_dir = os.path.join(
        os.path.dirname(os.path.abspath(db_path)), "card_cleanup")
    app.config["CARD_CLEANUP_DIR"] = card_cleanup_dir  # tests reach it here
```

Add `import card_cleanup` beside the other flat sibling imports. Place the three routes near the audit/job endpoints:

```python
    @app.route("/api/card-cleanup/scan", methods=["POST"])
    def api_card_cleanup_scan():
        body = request.get_json(silent=True) or {}
        source = body.get("source")
        recursive = bool(body.get("recursive", True))
        if not source or not isinstance(source, str):
            return json_error("source required")
        if not os.path.isabs(source):
            return json_error("source must be an absolute path")
        if not os.path.isdir(source):
            return json_error("source is not an accessible directory")
        db = _get_db()
        # Overlap fail-fast, across all workspaces (folders is global):
        # this tool is for removable media, not the archive. The per-file
        # guard in card_cleanup.qualify_rows is the real invariant; this
        # is the clear early error.
        source_real = os.path.realpath(source)
        for row in db.conn.execute("SELECT path FROM folders").fetchall():
            froot = row["path"]
            if not froot:
                continue
            froot_real = os.path.realpath(froot)
            if (path_guard.contains_resolved(source_real, froot_real)
                    or path_guard.contains_resolved(froot_real, source_real)):
                return json_error(
                    "the selected source overlaps the cataloged archive "
                    f"folder {froot!r}; this tool is for removable media "
                    "like memory cards, not archive folders")
        card_cleanup.prune_manifests(card_cleanup_dir)
        # ... mirror verify-hashes: resolve active_ws, then:

        def work(job):
            # mirror verify-hashes' thread Database + set_active_workspace
            # + finally-close pattern exactly; inside:
            def progress_cb(current, total, filename):
                if current % 10 != 0 and current not in (1, total):
                    return
                runner.push_event(job["id"], "progress", {
                    "current": current, "total": total,
                    "current_file": filename,
                    "phase": "Verifying card files against the archive",
                })
            return card_cleanup.scan_card(
                thread_db, source, recursive, card_cleanup_dir, job["id"],
                progress_cb=progress_cb,
                should_cancel=lambda: runner.is_cancelled(job["id"]),
            )

        job_id = runner.start(
            "card-cleanup-scan", work,
            config={"source": source, "recursive": recursive},
            workspace_id=active_ws)
        return jsonify({"job_id": job_id})

    @app.route("/api/card-cleanup/<scan_job_id>/manifest")
    def api_card_cleanup_manifest(scan_job_id):
        try:
            manifest = card_cleanup.load_manifest(
                card_cleanup_dir, scan_job_id)
        except card_cleanup.ManifestError as e:
            return jsonify({"error": str(e)}), e.http_status
        return jsonify(manifest)

    @app.route("/api/card-cleanup/delete", methods=["POST"])
    def api_card_cleanup_delete():
        body = request.get_json(silent=True) or {}
        scan_job_id = body.get("scan_job_id")
        if not scan_job_id or not isinstance(scan_job_id, str):
            return json_error("scan_job_id required")
        db = _get_db()
        # Scan job must exist and be completed. Works across restart:
        # fall back to job_history when the runner no longer holds it.
        job = runner.get(scan_job_id)
        if job is None:
            row = db.conn.execute(
                "SELECT type, status FROM job_history WHERE id = ?",
                (scan_job_id,)).fetchone()
            job = dict(row) if row is not None else None
        if job is None or job.get("type") != "card-cleanup-scan":
            return jsonify({"error": "unknown scan job"}), 404
        if job.get("status") != "completed":
            return json_error(
                "scan did not complete — re-scan the card")
        # One delete per manifest. (TOCTOU race between two simultaneous
        # POSTs is acceptable: the per-file gates make a double-delete
        # skip, not double-fire.)
        for j in runner.list_jobs():
            if (j.get("type") == "card-cleanup-delete"
                    and j.get("status") in ("queued", "running")
                    and (j.get("config") or {}).get("scan_job_id")
                    == scan_job_id):
                return jsonify({"error":
                    "a delete for this scan is already running"}), 409
        try:
            manifest = card_cleanup.load_manifest(
                card_cleanup_dir, scan_job_id)
        except card_cleanup.ManifestError as e:
            return jsonify({"error": str(e)}), e.http_status
        if not any(e.get("bucket") == "deletable"
                   for e in manifest["entries"]):
            return json_error("nothing to delete — no verified files")

        def work(job):
            # same thread-Database mirror as the scan; inside:
            def progress_cb(current, total, filename):
                runner.push_event(job["id"], "progress", {
                    "current": current, "total": total,
                    "current_file": filename,
                    "phase": "Deleting verified files from the card",
                })
            return card_cleanup.delete_verified(
                thread_db, manifest,
                progress_cb=progress_cb,
                should_cancel=lambda: runner.is_cancelled(job["id"]),
            )

        job_id = runner.start(
            "card-cleanup-delete", work,
            config={"scan_job_id": scan_job_id},
            workspace_id=active_ws)
        return jsonify({"job_id": job_id})
```

The `# mirror verify-hashes` comments are instructions, not code — replace them with the exact resource-handling pattern from that endpoint (thread `Database(db_path)`, `set_active_workspace(active_ws)` when set, close in `finally`). Delete-job progress is NOT throttled: deletions are the events the user is watching.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest vireo/tests/test_card_cleanup_api.py -v`
Expected: PASS

- [ ] **Step 5: Run the regression net**

Run: `python -m pytest vireo/tests/test_jobs_api.py vireo/tests/test_app.py -q`
Expected: PASS (no regressions from the app.py changes; compare any failures against the known pre-existing failures on this machine before blaming this change)

- [ ] **Step 6: Commit**

```bash
git add vireo/app.py vireo/tests/test_card_cleanup_api.py
git commit -m "feat: card-cleanup scan/delete/manifest endpoints with overlap fail-fast"
```

---

### Task 7: "Free up card space" UI on the import page

**Files:**
- Modify: `vireo/templates/import.html`

No automated UI tests — the backend behavior is fully covered; this task is wiring, verified manually.

- [ ] **Step 1: Study the page's existing patterns**

Read how import.html starts jobs and streams progress: the `fetch('/api/jobs/import-photos', ...)` POST (~line 3759), `new EventSource('/api/jobs/' + jobId + '/stream')` (~3785), `pollJobResult` (~2106), and the card-safety pill markup (~4061–4086). Reuse the page's existing CSS classes and collapsible-section markup; do not invent a new visual language.

- [ ] **Step 2: Add the section**

Two entry points, per the spec's UX section:

- The collapsed section below the import form (detailed next).
- A link/button **next to the existing card-safety pill** (~4061–4086) that appears when an import run finishes or is cancelled — the "Do NOT format the card yet" state is exactly when the user needs this tool. Clicking it expands the section and pre-fills the source input with the import's source path (available in the import job's config/result the pill already renders from).

The collapsed "Free up card space" section below the import form contains:

1. A folder input reusing the page's existing source-picker control, plus a Recursive checkbox (default on).
2. **Scan card** button → `POST /api/card-cleanup/scan` → progress via EventSource on the job stream (phase text + current/total) → on completion `GET /api/card-cleanup/<id>/manifest`.
3. Preview render from the manifest:
   - Three rows: `{deletable.count} files / {formatBytes(deletable.bytes)} verified in the archive — safe to delete`, `{kept.count} files / {bytes} not verified — will be kept`, `{ignored.count} files ignored (not photo files Vireo imports)`.
   - Each row expands (`<details>`) to the file list; kept rows show `entry.reason`.
   - If `walk_errors` is non-empty: a warning banner — "Some folders could not be read; this preview is incomplete. Files that were not seen will never be deleted." with the error list.
4. **Delete verified files** button (disabled when deletable count is 0) → confirmation dialog with exactly the spec's copy:
   - "Deletion is permanent — memory cards have no trash."
   - "After deletion, the archive holds the only copy of these photos."
   - "What 'verified' means: each file's archive copy passed a checksum check (at import or integrity audit) and is confirmed unchanged since by size and timestamp; the card copy itself is re-checksummed at the moment of deletion. Archive bytes are not re-downloaded."
5. On confirm → `POST /api/card-cleanup/delete` → per-file progress → final summary from the job result: deleted count/bytes, skipped list with reasons, failed list with errors, and "cancelled — N remaining" when applicable. Errors from the POST (404 expired → offer "Re-scan"; 409 → "already running") render inline, not as alerts.

Number formatting: reuse the page's existing byte-formatting helper if present (grep for `formatBytes`/`humanSize`); add a small one only if none exists.

- [ ] **Step 3: Verify in the running app**

Follow the memory note "Verifying browse UI live" (HOME override bypasses the single-instance lock). Create a fake card directory with a few files matching an archived workspace, run:

`python vireo/app.py --db <temp-copy> --port 8081`

Walk the full flow: scan → preview shows correct buckets → delete → files gone from card, kept files intact → re-scan shows 0 deletable. Also confirm the overlap 400 renders its message when pointing the picker at an archive folder.

- [ ] **Step 4: Commit**

```bash
git add vireo/templates/import.html
git commit -m "feat: Free up card space UI on the import page"
```

---

### Task 8: Full test pass and PR

- [ ] **Step 1: Run the required suite from CLAUDE.md**

Run: `python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py vireo/tests/test_card_cleanup.py vireo/tests/test_card_cleanup_api.py vireo/tests/test_path_guard.py -q`

Expected: PASS, modulo the 4 known pre-existing failures on this machine (see memory note "Local test-suite baseline" — verify any failure predates this branch before treating it as yours). Full `vireo/tests` takes ~23 minutes; the list above is the required subset.

- [ ] **Step 2: Create the PR**

```bash
git push -u origin clear-imported-photos-from-card
gh pr create --base main --title "Free up card space: verified card cleanup (scan → preview → delete)" --body "$(cat <<'EOF'
## What

Implements docs/superpowers/specs/2026-08-07-card-cleanup-design.md: a
standalone tool that deletes files from a memory card only when the
identical bytes are checksum-verified in the archive — per file, enforced
at the moment of deletion.

- `vireo/path_guard.py`: behavior-preserving extraction of the import
  endpoint's case-aware containment guard (PR #1107) — endpoint tests
  unchanged.
- `vireo/card_cleanup.py`: scan (walk/classify with discovery parity,
  hash-once catalog matching, archive qualifying test), atomic validated
  manifests with 7-day expiry, delete with per-unlink card re-hash and
  fresh archive re-validation.
- Endpoints: POST /api/card-cleanup/scan (with cross-workspace
  source/archive overlap fail-fast), GET .../manifest, POST
  /api/card-cleanup/delete (expiry at request time, one delete per
  manifest).
- Import-page UI section with honest preview buckets and the exact
  confirmation copy from the spec.

## Tests

[paste the pytest summary line from Step 1]

EOF
)"
```

- [ ] **Step 3: Hand off for review**

Reference @superpowers:finishing-a-development-branch for the merge flow; the repo's PR agent re-reviews on push.
