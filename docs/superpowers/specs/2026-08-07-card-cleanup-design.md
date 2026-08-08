# Free up card space (card cleanup) — design

**Date:** 2026-08-07
**Status:** Spec-review approved (2026-08-07); amended twice same day
after external review (first: delete-time revalidation, overlap guard,
metadata-anchored guarantee, manifest lifecycle, own-walk discovery;
second: no stat caching across removals, filesystem-aware containment +
`samefile`, exact-mtime baseline, delete-time expiry, atomic + validated
manifests); awaiting maintainer sign-off before implementation planning
**Scope:** New feature: delete files from a local import source (memory
card) only after verifying, per file and at deletion time, that the card
file's bytes match a hash that was checksum-verified into the archive and
that the archive copy is confirmed unchanged since that verification by
metadata (see "Safety invariant" for the exact guarantee). Two new job
types plus an import-page UI section. No changes to the import pipeline
itself.

## Problem

A long import (e.g. 40 hours of travel photos over a slow link) can be
stopped partway — the import is batch-committed and crash-safe, and a re-run
skips everything already cataloged — but the user gets no help clearing the
card afterward:

- The import does not persist per-file source paths; the card-path →
  archive-file mapping exists only in memory during the run.
- Files are imported in destination-folder (chronological) order, not card
  order, so no card region corresponds to "the imported part."
- The only safety signal is the all-or-nothing `safe_to_format` pill, which
  correctly says "Do NOT format the card yet" after a partial run.

So a traveler who imported half a card and needs the space back has no safe
option: manually guessing which files landed risks deleting the only copy.

## Goals

1. After any partial (or complete, or historical) import, the user can
   point Vireo at the card and delete exactly the files whose content was
   checksum-verified into the archive and whose archive copy is confirmed
   unchanged (by metadata) at deletion time — nothing else.
2. The preview shown before deletion is computed from ground truth at scan
   time (card bytes + current catalog), never from a stale record of what a
   past run claimed. This is the CORE_PHILOSOPHY transparency rule: the
   numbers must mean what the user reads them as.
3. The card stays usable throughout: the user can keep shooting between scan
   and delete; new or changed files are never touched.

## Non-goals (v1 scope cuts)

- **Local sources only.** Memory cards are local mounts; deleting files on a
  remote/SSH source is out.
- **No delete-as-you-go during import.** This tool's verify-then-delete core
  is the building block if that is ever wanted; it is not part of v1.
- **No persistence of per-file import provenance.** The scan recomputes
  everything from the card and catalog, so no schema changes are needed.
- **No empty-directory cleanup** on the card — files only. Cameras recreate
  their DCIM structure as needed.
- **No card formatting.** Vireo deletes individual verified files; formatting
  remains a user action on their own machine.

## Approaches considered

**A. Standalone scan-verify-delete tool (chosen).** Re-scan the card, match
each file against the catalog by content hash, preview, then delete only the
verified set. Works regardless of when/how the files were imported (this
run, last week, another machine), needs no schema changes, and reuses the
matching machinery `ingest()` already has. Cost: a re-hash pass over the
card — local reads, minutes not hours.

**B. Delete-as-you-go during import.** Frees space *during* the import, but
partially erases the card mid-run, is unavailable for the run the user is
already in, and the remote path is not always hash-verified. Rejected for
v1; A is its prerequisite anyway.

**C. Run-scoped cleanup** (persist per-file source paths during import,
offer "delete what this run imported"). Needs schema changes, goes stale if
the card is touched between runs, and covers only one run. Strictly worse
than A, which computes the same answer from ground truth at deletion time.

## Design

### Safety invariant

Everything below serves one rule, enforced **at the destructive moment**,
not just at preview time. A card file is deleted only if, immediately
before its `os.remove`:

1. **Card side (full strength):** its bytes, re-hashed from the card by
   the delete job, equal the manifest hash. The card is local, so this
   costs one extra local read per file and defeats same-size/same-mtime
   replacement and card swaps outright.
2. **Archive side (metadata-anchored):** a fresh catalog query by that
   hash finds at least one row where (a) `hash_status = 'ok'`, (b) the
   row's archive path is **outside the source tree** under the
   filesystem-aware containment rules of the overlap guard below, plus an
   `os.path.samefile` check against the card file where available (same
   device+inode means the "archive copy" *is* the card file), and (c) a
   fresh `stat` of the archive file — performed immediately before this
   specific `os.remove`, never reused from the scan or from an earlier
   deletion in the same run — matches the cataloged
   `file_size`/`file_mtime` baseline exactly. Exact equality, not the
   audit's 1-second window: that tolerance (`audit.py:398`) only
   classifies an already-detected hash mismatch as modified-vs-corrupt
   and certifies nothing here, and a false negative from strict equality
   merely keeps a file (safe, and remedied by a rescan).

What this promises: the deleted bytes were checksum-verified into the
archive, and the archive copy is metadata-unchanged since the scan that
recorded the matching hash. What it deliberately does not promise: a
byte-for-byte re-read of the archive at deletion time. The archive is SMB
over Tailscale; re-reading tens of GB over a VPN would take longer than
the import time this feature exists to reclaim, so the guarantee is
weakened to metadata-unchanged-since-verification — stated as such in the
spec and reflected in the UI copy.

The stale-verdict hole this could open is closed from both directions:
if the archive file was edited *and rescanned*, the refreshed `file_hash`
no longer matches the card hash (no match → kept); if edited and *not*
rescanned, its current mtime/size no longer match the cataloged baseline
(stat gate fails → kept). The residual risk is a same-size,
identical-mtime content swap on the archive — detectable only by
re-reading bytes, and accepted.

**Overlap guard.** Matching searches the global catalog, so without a
guard a selected source could verify *itself*: pick a cataloged archive
folder (or a card imported in place) and each file's own catalog row would
mark it deletable — the tool would erase the archive copy. Two levels:

- **Containment is filesystem-aware, not string realpath.** `realpath`
  alone does not close this hole: macOS/Windows default filesystems are
  case-insensitive but `realpath` doesn't case-normalize, so
  `/Volumes/card` and `/Volumes/Card` compare unequal while naming the
  same directory, and FAT/exFAT removable media on Linux are
  case-insensitive under a case-sensitive parent. The import
  destination-inside-source guard already solves exactly this
  (`_casenorm` + `_fs_is_case_insensitive`, `app.py:25641` area, PR
  #1107): case-fold unconditionally on darwin/win32, probe the actual
  filesystem on Linux, treat inconclusive probes as case-insensitive
  (the strict direction). Extract that logic into a shared helper and
  use it for every containment comparison in this feature. The
  extraction is a behavior-preserving refactor of the import endpoint's
  guard — it must not change that guard's decisions, and the existing
  PR #1107 tests (or equivalent) must still pass against the shared
  helper.
- **Fail fast at scan start:** reject the scan with a clear error if the
  source root equals, contains, or lies inside any cataloged folder root
  under those containment rules — across all workspaces, matching the
  per-file guard, which is already global because it queries `photos`.
  This tool is for removable media, not the archive.
- **Per-file invariant (the real guard, at scan and again at delete):** a
  catalog row qualifies only if its archive path is outside the source
  tree under the same containment rules, and additionally not
  `os.path.samefile` with the card file (device+inode identity catches
  mount aliases that survive both realpath and case-folding). This
  per-file check is what rule 2(b) above enforces.

### UX flow

A "Free up card space" section on the import page, plus an entry point next
to the existing card-safety pill after an import finishes or is cancelled
(the "Do NOT format the card yet" state is exactly when the user needs this
tool). The user picks the card folder with the existing source picker and
starts a **scan**. When the scan job finishes, the page shows a preview:

> **1,842 files / 61 GB** verified in the archive — safe to delete
> **2,105 files / 70 GB** not in the archive — will be kept
> **214 files** ignored (not photo files Vireo imports)

Each bucket expands to its file list (path, size, and for the verified
bucket the matched archive path; for kept files, the reason). A **Delete
verified files** button opens a confirmation dialog that states plainly:

- Deletion is permanent — memory cards have no trash.
- After deletion, the archive holds the only copy of these photos.
- What "verified" means: each file's archive copy passed a checksum check
  (at import or integrity audit) and is confirmed unchanged since by size
  and timestamp; the card copy itself is re-checksummed at the moment of
  deletion. Archive bytes are not re-downloaded.

Confirming starts the **delete** job with live per-file progress. The final
summary reports exact counts: deleted, kept, skipped-because-changed, and
failed, each with reasons.

### Scan job

A new `JobRunner` job type (working name `card_cleanup_scan`), started via
`POST /api/card-cleanup/scan` with `{source, recursive}`. SSE progress via
the existing `/api/jobs/<id>/stream`.

Phases:

1. **Discover** — the scan performs one walk using the same machinery
   discovery uses (`safe_scan_walk`/`safe_iter_dir` with the same
   data-bundle exclusions) and classifies every regular file with
   discovery's own predicate — `SUPPORTED_EXTENSIONS` (i.e.
   `file_types="both"`, independent of any import-config filter) and the
   leading-dot rule. Matching files are candidates; everything else is
   bucketed **ignored** and never touched. `discover_source_files`
   (`ingest.py:286`) itself returns only supported photo files, so it
   cannot produce the ignored list — but a unit test asserts the scan's
   candidate set equals `discover_source_files` output on the same tree,
   so the two filters cannot drift. Walk errors (unreadable subtree,
   permission denied) are collected via `onerror`, surfaced in the result,
   and mark the preview **incomplete** in the UI — undiscovered files are
   never deleted (they are simply absent from the manifest), but totals
   must not present themselves as a full accounting of the card.
2. **Hash & match** — each candidate is content-hashed on the card and
   matched against the global photo catalog by hash, using the strict
   `verify_by_hash` identity from `vireo/import_dedup.py`
   (`DuplicateChecker(CatalogIndex.from_db(db), verify_by_hash=True)`) —
   not the metadata (filename/size/EXIF-time) shortcut. A metadata match is
   not sufficient grounds to delete someone's only other copy. The scan
   calls `match()` only — never `record()`/`check_and_record()` — so
   card-only twin files cannot make each other look "known" the way
   ingest's seen-state accumulation would. Each card file is hashed
   exactly once: the manifest needs the hash for every file (kept bucket
   included, for the delete-time card gate and honest preview), so the
   scan hashes first and checks catalog membership with that hash (or
   reuses the checker's `content_hash` cache) rather than letting
   `match()` hash internally a second time.
3. **Archive check** — `match()` returns an opaque hash token, not a photo
   row, so a matched hash is followed by a `photos WHERE file_hash = ?`
   lookup. The file counts as **deletable** only if at least one matching
   row passes the full qualifying test from the safety invariant:
   `hash_status = 'ok'`, archive path outside the source tree (and not
   `samefile` with the card file) under the overlap guard's containment
   rules, and a `stat` of the archive file matching the cataloged
   `file_size`/`file_mtime` baseline exactly. The preview shows the first
   row that passes. One
   stat round-trip per file over the SMB mount — never a re-read of
   archive bytes. The same test is repeated per file at delete time; here
   it exists so the preview is honest.

Every other candidate is **kept**, with a per-file reason: not in catalog,
not integrity-verified (`hash_status` not `'ok'`), archive file missing or
changed since verification (size/mtime off baseline), only catalog copy is
inside the selected source tree, unreadable on card. Photos cataloged by an archive *scan*
rather than a verified import have `file_hash` but NULL `hash_status`, so
whole scan-cataloged archives land in this bucket — safely conservative,
but the keep-reason copy must point at the remedy ("not verified by a
checksummed import — run the integrity audit") so the tool doesn't read as
broken.

**Manifest storage and lifecycle.** The manifest — a header with the
resolved source root (so the delete job's overlap guard travels with the
manifest it validates, even across a restart), then per file: card path,
size, `mtime_ns`, content hash, bucket, matched archive path or
keep-reason — can hold tens of thousands of entries, and job results are
the wrong home for it: `job_history.result` is a TEXT blob fetched by
list endpoints, completed jobs are evicted from in-memory lookup by
`_prune_finished_jobs` (`jobs.py:766`), and history is workspace-filtered
while this feature is global. So the manifest is written as a JSON file to
`~/.vireo/card_cleanup/<scan_job_id>.json`, and the job *result* carries
only the bucket totals, the resolved source root, and the manifest path.
The delete job reads the manifest from disk, which also makes it work
across an app restart.

The manifest is written atomically (temp file + rename, the pattern the
codebase already uses for promotions) so a crash mid-scan can never leave
a truncated manifest that a later delete trusts. On load, the delete job
validates before acting: schema version recognized, header source root
present, and every deletable entry's card path contained within that
source root (under the overlap guard's containment rules) — a manifest
that fails any check is rejected as corrupt, and nothing is deleted.

Lifecycle: each scan start prunes manifest files older than 7 days, and
the 7-day limit is *also* enforced when deletion is requested — a delete
against a manifest older than 7 days is rejected even if no scan ran in
between to prune it. Either way the user gets "manifest expired — re-scan
the card" (a re-scan is minutes, and a fresh manifest is strictly safer
than an old one). The scan is cancellable at file boundaries; a cancelled
scan writes no manifest and the UI says so.

Duplicate files on the card (two identical copies matching one archive
photo) are both deletable — the rule is content-based, not one-to-one.

### Delete job

`POST /api/card-cleanup/delete` with `{scan_job_id}` starts
`card_cleanup_delete`. It loads the manifest file written by the scan and
refuses to start if the scan job is missing, unfinished, or cancelled, if
the manifest file is gone (expired), or if its deletable bucket is empty.
Only one delete job per scan manifest may run at a time. Because the
manifest lives on disk, delete-after-restart works: the endpoint validates
the scan job against `job_history` when it is no longer in memory.

For each **deletable** manifest entry, immediately before deletion:

1. **Card gate (stat, then re-hash)** — re-`stat` the card file; if size
   or `mtime_ns` differs from the manifest, **skip** (cheap pre-check).
   Then re-hash the card file and require the hash to equal the manifest
   hash — size+mtime alone cannot detect a swapped card or a same-size
   replacement, especially with FAT/exFAT timestamp granularity. The card
   is local, so this second read is fast. Any mismatch → **skipped**, with
   reason. New files are simply absent from the manifest, so the user can
   keep shooting between scan and delete.
2. **Archive gate (re-validated now, not trusted from the scan)** —
   re-query `photos WHERE file_hash = ?` and require at least one row to
   pass the qualifying test from the safety invariant: `hash_status =
   'ok'`, path outside the source tree and not `samefile` with the card
   file, fresh archive `stat` exactly matching the cataloged size/mtime
   baseline. If the mount is down, the file was deleted from the archive,
   or the baseline no longer matches, the card file is **skipped**, not
   deleted. The scan-time check only made the preview honest; this one is
   the guarantee.
3. **Delete** — `os.remove`. Per-file errors (read-only card, vanished
   file) are recorded as **failed** with the OS error; the job continues.

Catalog *row* lookups for duplicate card files sharing a hash may be
cached within the run; the archive **stat may not** — it must be repeated
immediately before every individual removal, because a cached stat could
authorize deleting the second duplicate after the archive vanished
following the first. One SMB stat round-trip per removal; archive bytes
are never re-read — see the safety invariant for the explicit tradeoff.

Progress is per-file over SSE. Cancellation stops at a file boundary;
already-deleted files stay deleted and the summary honestly reports
deleted vs. remaining. Directories are left in place.

The delete job never re-reads the manifest's *kept* or *ignored* buckets —
they exist only for the preview.

### Endpoints

- `POST /api/card-cleanup/scan` — body `{source: str, recursive: bool}`.
  Validates that the source path exists and is a directory, and rejects it
  (400, with copy explaining this tool is for removable media) if it
  equals, contains, or lies inside any cataloged folder root under the
  overlap guard's containment rules — the guard's fail-fast half. Returns
  the job id.
- `POST /api/card-cleanup/delete` — body `{scan_job_id}`. Returns the job
  id. 409 if a delete for that manifest is already running; 400 for an
  unfinished/cancelled scan or a manifest that fails load-time validation
  (corrupt, wrong schema, entries outside the source root); 404 for an
  unknown scan job, a pruned manifest file, or a manifest older than 7
  days (age is checked at request time, not only at prune time), with
  "re-scan the card" copy.
- Progress and results ride the existing job endpoints (stream, status,
  history).

Both jobs are global (photos and their hashes are global, not
workspace-scoped), matching how the catalog works.

### Error handling

- Unreadable card file at scan time → kept, reason "could not read".
- Archive stat failure (mount down, permission) → kept, reason "archive
  file not reachable"; the scan completes and says how many files were
  unverifiable so the user knows the mount was the problem.
- Card unmounted mid-delete → per-file failures accumulate; the job
  finishes with an honest failure count rather than aborting silently.
- Archive mount down at delete time → every archive gate fails, every file
  is skipped with "archive not reachable", nothing is deleted; the summary
  makes the cause obvious rather than reporting a sea of per-file skips.
- Scan manifest older than the card's current state is handled entirely by
  the per-file card gate (stat + re-hash); there is no root-level
  signature check, because the whole point is that the user keeps shooting
  on the card.

## Testing

Unit tests with temp directories (no real card or SMB mount):

- Bucket assignment: verified / not-in-catalog / ignored non-photo files.
- Scan candidate set equals `discover_source_files` output on the same
  tree (filter-parity test, so the two predicates cannot drift).
- Hash match but `hash_status` ≠ `'ok'` → kept.
- Hash match but archive file missing, or size/mtime off the cataloged
  baseline → kept.
- Overlap: scan of a source equal to / containing / inside a cataloged
  folder root → 400, including a case-swapped variant of the root on a
  case-insensitive filesystem. A catalog row whose path is inside the
  source tree (self-match via symlinked or in-place catalog) never
  qualifies a file, at scan or delete; a row that is `samefile` with the
  card file (mount alias) never qualifies it either.
- Card gate: file modified between scan and delete → skipped; same-size
  same-mtime content replacement → caught by the delete-time re-hash and
  skipped.
- Archive gate at delete time: archive file removed or mutated after the
  scan → skipped; archive mount unreachable → all skipped, none deleted.
- No stat reuse: two identical card files, archive copy removed after the
  first deletion → the second is skipped (its own fresh stat fails, even
  with the catalog row cached).
- Delete after app restart: manifest loads from disk, scan job validated
  via history, deletion proceeds.
- Manifest expiry: pruned manifest → delete returns 404; a manifest older
  than 7 days is rejected at delete-request time even when never pruned;
  scan-start prune removes only files older than 7 days.
- Manifest validation: truncated/corrupt JSON, unknown schema version, or
  a deletable entry whose path falls outside the header source root →
  delete refuses to start, nothing deleted.
- Cancellation mid-delete → already-deleted files gone, summary counts
  correct.
- Two identical card files matching one archive photo → both deletable,
  both deleted.
- Delete refuses to start on a cancelled or missing scan job.
- Per-file delete failure (permission) → recorded as failed, job continues.
- Endpoint validation: nonexistent source dir, concurrent delete → 409.
