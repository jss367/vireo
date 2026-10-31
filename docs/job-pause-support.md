# Pausing background jobs

Use **Pause** on a running job on the Jobs page, then **Resume** to continue
with its current progress. **Pausing…** means the worker is finishing its
current photo, batch, request, or analysis stage. **Paused** means it reached
a safe checkpoint. Resume requires the same running Vireo process; this is
not a checkpoint that survives restarting the application.

## Supported jobs

| Work | Pause boundary |
| --- | --- |
| Prepare full resolution, generate previews, cache originals for offline use, release local originals | Between photos |
| Generate thumbnails, develop photos, extract subject masks | Between photos |
| Export photos, export for iNaturalist, move selected photos | Between photos and after export metadata finishes; active subprocesses finish and moved-folder counts are reconciled before a pause is confirmed |
| Send to NAS and move entire folders | Between preflight checks and local files; active rsync copies and remote verification processes are stopped and reaped before pausing. Resume retains completed files and restarts interrupted subprocess work. Transfer serialization locks are released while paused. |
| Verify photo hashes | Between files, after committing the completed batch prefix |
| Adjust capture time | Between photos, after the metadata write and catalog update |
| Scan for duplicates | Between duplicate groups |
| Score sharpness | Between burst groups, saved scores, and automatic flags |
| Analyze photos for culling | During metadata loading, between scene-hash calculations and species groups, and before saving the final result |
| Regroup encounters and bursts | Between loading, grouping, and saving stages |
| Verify installed models | Between models |
| Fetch regional species labels | Between request progress updates |
| Import and organize photos | During discovery, between metadata batches of at most 100 files, and between copies; combined imports also use the scanner's pause coordination |
| Import Lightroom catalog keywords | During catalog reading and merging, and between photos after committing completed keyword writes; source catalog read locks are released before pausing |
| Verify abandoned import staging folders | During directory enumeration and between files |
| Scan memory cards, verify archive copies, delete verified card files | During card discovery, between files or archive hash groups |
| Classify photos | Between photos and stages; label embedding computation checkpoints and releases shared cache locks before pausing |
| Precompute label embeddings | Between label batches, after saving resumable progress and releasing shared cache locks |
| Publish a website | During preparation, before the final replacement of published files |
| Scan folders, repair metadata, import in place, import photos, process pipelines | Existing coordinated checkpoints |

Cancelling a paused job wakes it so it can exit and perform its normal cleanup.
Completed work remains subject to the job's usual cancellation behavior.
An accepted pause during the final item is honored before the runner publishes
completion, after the worker has released its resources. Completed file writes
remain intact while the job waits for Resume or Cancel.

## Exceptions

These jobs deliberately remain without Pause in the current implementation:

| Work | Reason |
| --- | --- |
| Download models, taxonomy, or Darktable | Downloads and setup delegate to external downloaders, active network streams, or installation steps. Their progress notifications do not establish a point where all work has stopped. Taxonomy installation also performs large database transactions. Supporting pause requires separate download and installation coordination. |
| Synchronize metadata to XMP sidecars | A job holds the shared synchronization lock while collecting and writing changes. Parking inside that operation would block other synchronization jobs. The lock and change snapshots need a resumable batch protocol. |
| In-place imports from a saved new-photo list | These imports hold a shared lock for the entire operation so concurrent imports cannot reuse the same saved list. Pausing would block those imports and delay their cancellation. Ordinary in-place imports support pause. |
| Work Locally: stage, synchronize, or discard, for either folders or workspaces | These transitions reserve shared folder or workspace state while switching between remote and local files. An indefinite pause can block other operations or leave a transition pending. |
| Batch deletion from the library or disk | Disk deletion and catalog reconciliation span multiple phases. Pausing between them could leave the catalog pointing to files already removed for an indefinite period. Verified memory-card deletion has a separate per-file protocol and supports pause. |
| Create panoramas, analyze subjects, retry analysis | These paths delegate to external tools or analysis requests without coordinated pause checkpoints. |
| Retained cache maintenance and cleanup | File deletion and catalog reconciliation run together without resumable batch checkpoints. |
| Automatic startup backfills, missing-original scans, and new-image discovery | These are internal, ephemeral maintenance or discovery tasks rather than user-managed jobs. |

Website publishing's final replacement step is also uninterrupted, even when
Pause was available earlier in preparation. Once that commit step begins,
new pause and cancellation requests are rejected.
The final import handoff follows the same rule: pending requests are honored
before creating the import collection and starting follow-up processing;
after the handoff begins, control the follow-up job through its own controls.

Folder transfers also have an uninterrupted final commit: updating catalog
paths and removing the source originals. Pause and Cancel are unavailable
during that step, and the Jobs page explains why. Any pause during verification
invalidates the earlier checks; the transfer repeats verification after Resume
before entering its commit. Cancel before commit retains the source originals.
Date-organized folder moves commit one photo at a time, so photos already moved
remain moved after Cancel.

Send to NAS releases the global metadata synchronization lock after its optional
sidecar-writing phase. A pause during that phase waits for it to finish. The
long copy and verification do not keep the sidecar lock held.

## Adding a new job

Declare each job type in `vireo/job_runner/pause_policy.py`. Supported jobs
default to pausable; exceptions must have an explanation that appears on the
Jobs page. Unknown types without an explicit `pausable` declaration are rejected
by both runner launch APIs. The production-launcher audit test also requires
literal job types to have a central policy. Dynamic families (such as model
downloads) must explicitly declare their policy at the launcher.

Use
`runner.is_cancelled(job_id)` at a safe checkpoint when the worker handles
cancellation itself, or `JobLaunch.checkpoint(job)` when cancellation should
unwind the worker. Both retain the worker's local state during a pause.

`job_runner.work_control.WorkControl` provides shared checkpoints, a nonblocking
subprocess-stop probe, and an atomic commit boundary. `SerializedWork` releases
and reacquires transfer locks around a pause. Read-only external commands can
use `run_controlled_process`, which stops and reaps the subprocess before
parking and restarts the command on Resume. A caller that verifies files before
deleting originals must check the control's pause generation and reverify after
any pause, including a request racing the commit boundary.

Test Pause → Resume and Pause → Cancel using observable checkpoints. Verify
that subprocesses have exited, locks are available, completed writes persist,
and an indefinite pause cannot make stale verification authorize deletion.

Do not park while holding a database write transaction, a shared lock, an
inference permit, or while another worker or subprocess is still active.
Use `runner.cancellation_requested(job_id)` for cancellation-only probes inside
those operations. Multi-worker jobs must coordinate all participants before
publishing the paused state. Include `pausing` and `paused` in checks that
prevent another job from changing the same reserved state.
