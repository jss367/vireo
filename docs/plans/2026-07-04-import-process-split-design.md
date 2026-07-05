# Import / Process Split

## Problem

Local-processing imports are one monolithic pipeline job: stage the card into
`~/.vireo/staging/pipeline-<id>/`, catalog the staged copies, run every GPU
stage (thumbnails, previews, detect, classify, masks, eyes, misses, regroup),
and only then rsync the staged tree to the archive and repoint the catalog.
Photos become visible in the workspace at their real paths only if the *last*
stage succeeds.

The week of 2026-06-30 → 2026-07-04 showed what that costs. Four consecutive
runs of the same card died at or before the archive stage (destination-overlap
guard, content-conflict guard, and twice the rsync stall watchdog false-firing
on buffered output — fixed in v0.14.0, #1094). Each failure ran
`_deindex_staging()` (`pipeline_job.py:5010`), deleting the staged catalog
rows and every derived result keyed to them. Each retry re-staged 45 GB and
redid ~6 hours of GPU work. The user's experience: "I keep importing and the
folders never show up." Meanwhile ~114 GB of orphaned staging accumulated in
`~/.vireo/staging/`.

`_deindex_staging()` is not a bug — it protects against a real trap. The
import duplicate gate (`import_dedup.CatalogIndex`) skips files already
cataloged, so if a dead run's rows survived, a retry would skip every file as
"already imported" and publish an empty archive while the only real copies sat
in abandoned staging. But it is a blunt fix: it discards hours of derived data
to solve a bookkeeping problem.

The root cause of both the trap and the blunt fix is one architectural fact:
**we catalog photos at a temporary path before they exist at their final
one.** Everything downstream — the deindex hack, the all-or-nothing archive,
the invisible folders, the wasted retries, the staging debris — follows from
that.

## Goal

Split the pipeline into two first-class, independently retryable jobs:

- **Import** — get files from the card into the archive, verified, and into
  the catalog at their final paths. Fast, I/O-only, safety-critical. When it
  finishes, the folders are in the workspace and the card is safe to format.
- **Process** — compute derived data (working copies, thumbnails, previews,
  detect, classify, masks, eyes, misses, regroup) for already-cataloged
  photos. GPU-heavy, resumable, idempotent. Failure loses nothing but
  incremental progress.

Keep the one-click "card → reviewed" flow via **process strategies**: a named
preset of processing stages, selectable from a menu on the import page
("After import: …") and on the process page for standalone runs.

## The core invariant

> A photo row is created only when its file verifiably exists at its final
> archive path.

This single rule dissolves the current failure modes:

- The duplicate gate becomes always-truthful: "known hash → skip" is safe
  because known means *actually in the archive*, never "in a staging dir that
  may get abandoned". `_deindex_staging()` and its four call sites are deleted
  with nothing replacing them.
- Import becomes incrementally committed: each file is copied, verified, and
  cataloged (per file or per folder batch). A run that dies halfway leaves a
  valid partial catalog; the retry's dedup gate skips exactly the files that
  made it and copies the rest. No unwinding, ever.
- Folders appear in the workspace as they land — during the import, not hours
  later. This is the direct fix for "I imported and nothing showed up."
- Processing failures cannot touch the catalog because processing never owns
  catalog rows.

## Design decisions

- **No staging directory.** Import copies card → archive destination
  directly (rsync, with the v0.14.0 pty watchdog), applying the
  `%Y/%Y-%m-%d` folder template. Staging existed so GPU stages could read
  from fast local disk, but the JPG-centric working-copy shift already moved
  every pixel operation onto working copies in `~/.vireo`. Processing needs
  the original exactly once — to extract the working copy — and import can do
  that extraction *while the card is still the source* (fast local read),
  writing archive copy + working copy in one pass. After import, no stage
  reads originals from the NAS at all.
- **Verify before catalog.** After each file lands, verify it (size +
  content hash — the `verify_by_hash` machinery already exists), then insert
  the photo row. The import page's "safe to format card" indicator appears
  only when every source file is hash-verified at the destination — a status
  pill that means exactly what it says (CORE_PHILOSOPHY: no black boxes).
- **Import is a scan with a copy in front.** The existing scanner already
  knows how to create folder rows with parent chains, link them into the
  active workspace, extract metadata, and build working copies. Import
  becomes: copy+verify a file, then hand it to the scanner's per-file path.
  An interrupted import self-heals via a plain rescan of the destination —
  the recovery used on 2026-07-04 (this design's origin story) becomes the
  designed behavior instead of a lucky accident.
- **Process runs on cataloged photos, scoped by folder / collection /
  new-since.** The stages already exist and most already skip cached work
  (thumbnails "already cached", classify `cached` counters, mask
  fingerprints). The process job makes per-photo resumability a contract:
  re-running a strategy after a crash re-does only what's missing.
- **Process strategies are data, not code forks.** A strategy is a named
  set of stage flags — the `PipelineParams` skip flags
  (`skip_extract_masks`, `skip_regroup`, `skip_classify`, …) already express
  this; strategies give the combinations names and a menu:

  | Strategy | Stages |
  |---|---|
  | Full pipeline | everything: thumbs, previews, detect, classify, masks, eyes, misses, regroup |
  | Cull-ready | thumbs, previews, detect, classify — enough to review and rate |
  | Quick look | thumbs + previews only |
  | None | import only; process later by hand |

  Per-workspace default via `workspaces.config_overrides` (the mechanism the
  SAM/DINO variant overrides already use). Custom strategies are a later
  nice-to-have; four fixed presets cover the real workflows now.
- **Chaining, not coupling.** "After import: Full pipeline" means the import
  job's completion enqueues a process job scoped to the photos it imported.
  Two rows in job history, two retryable units. If processing fails, the
  photos stay imported and visible; the retry is the process job alone.
- **Remote (SSH) archives fit the same shape.** Import rsyncs card →
  `remote_path/subpath` and catalogs at `mount_path/subpath` (the mapping
  `resolve_remote_archive` already defines), extracting working copies from
  the card during the same pass. Since processing only touches working
  copies, the remote case stops needing local staging too. Verification uses
  the existing `--checksum` dry-run machinery from `move.py`.
- **Batching disappears.** Staging batching existed because a card could
  exceed local free space. With no staging copy, the only space consumed
  locally is working copies (small JPEGs); the storage preflight reduces to
  "does the archive have room" plus a working-copy estimate.

## What gets deleted

Solo-user app: no migration shims, dead paths go away.

- `_deindex_staging()` and all four call sites in `pipeline_job.py`.
- The archive stage, staging-dir creation, staging storage math
  (`staging_enough`, `batch_count`, …) in `pipeline_plan.py`.
- The destination-overlap fatal ("must land at a new archive path") and the
  staged-tree merge special cases — importing *into* a managed folder is now
  the normal case, guarded per-file by the dedup gate instead of per-tree by
  refusal.
- Startup self-heal: on boot, detect orphaned `~/.vireo/staging/pipeline-*`
  dirs and surface a one-click cleanup (the app repairs its own broken
  state; no manual `rm` instructions).

`move_folder()` and the Move page are untouched — moving archived folders is
a different operation and stays as-is.

## Failure modes

| Failure | Today | After split |
|---|---|---|
| rsync stalls / NAS flakes | 6 h of GPU work discarded, retry redoes all of it | import retry copies the missing files; nothing else re-runs |
| Card yanked mid-import | staged rows deindexed, staging orphaned | partial catalog is valid; re-insert card, retry skips what landed |
| App quits mid-import | same as above | rescan destination folder; scanner self-heals the catalog |
| GPU stage crashes | whole job failed, archive skipped, deindex | photos already imported and visible; re-run strategy, cached stages skip |
| Cancel | deindex — all derived work deleted | import: stops after current file, catalog valid; process: stops, progress kept |
| Retry after any of the above | full re-stage + full re-process | only the missing piece runs |

## UI

- **Import page** (split out of the current pipeline wizard's Stage 1):
  source card(s), destination, folder template, duplicate preview (existing
  `/api/import/check-duplicates`), an "After import" strategy menu, and
  per-folder progress ("2026-07-03: 1,240/1,984 copied · 1,102 verified").
  The completion state shows the folders that were created — the exact
  answer to "did my photos make it in?" — and the safe-to-format pill.
- **Process page**: scope picker (folders / collection / new-since-last-run),
  strategy menu, per-stage rows with honest counts ("Already done" = the
  next run would be a no-op given the current model/label selections — the
  existing transparency rule, unchanged).
- Both pages share the job progress/SSE plumbing that exists today.

## Sequencing

1. **PR 1 — Process job.** Factor the GPU stages into a process job that
   runs on existing photos with a strategy parameter. Collection-scoped
   pipeline runs already do 80% of this; the work is making per-photo skip
   behavior a tested contract and adding the strategy presets.
2. **PR 2 — Import job.** Direct-to-archive copy + verify + incremental
   catalog via the scanner's per-file path; working-copy extraction during
   import; safe-to-format tracking.
3. **PR 3 — Chaining + pages.** "After import" menu, import-completion
   enqueues the process job, split the wizard into Import and Process pages.
4. **PR 4 — Demolition.** Remove staging, `_deindex_staging`, batching,
   archive stage, overlap fatals; add the orphaned-staging self-heal.

Each PR lands independently; the current pipeline keeps working until PR 3
flips the UI.

## Open questions

- **Remote-archive verification cost.** A `--checksum` dry-run over SSH reads
  every byte on the NAS side. Acceptable for a per-import verify (it runs
  once, and the NAS-side CPU does the hashing)? Or is size+mtime enough when
  the transfer itself already ran with rsync's own integrity checking, with
  hash-verify as an opt-in (`verify_by_hash` already models this choice)?
- **EXIF-accept / rapid-review flows.** The batch EXIF-accept and
  rapid-review pages consume pipeline results mid-run today. They should
  attach to the process job's results; confirm nothing assumes import and
  processing share one job id.
- **Strategy for the new-images banner.** The new-images → pipeline flow
  (2026-04-22 design) scopes a run to a snapshot of un-imported files. It
  maps cleanly onto import-then-process, but the snapshot plumbing passes
  file paths through `scan_roots` — verify the import job accepts the same
  restriction.
