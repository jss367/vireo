# Pipeline concurrency + queue — design

**Date:** 2026-05-26
**Branch:** pipeline-concurrency-regression
**Status:** design agreed, awaiting implementation plan

## Goal

Replace today's hard "one pipeline at a time" UI lock with a resource-aware concurrency model: up to two pipelines execute at once, an unbounded FIFO queue holds the rest, and stages of the running pipelines overlap on whichever resources are free. The user can pile up many imports and walk away.

## Motivation

Today: the Start button is disabled with "A pipeline is running…" while another is active (`pipeline.html:2215–2223`). Lock is JS-only, doesn't survive page reload, doesn't see other tabs.

The lock was introduced in commit `03d23a39` (2026-03-28, pipeline page redesign). It exists for real reasons — concurrent GPU sessions OOM, regroup races on workspace state, the UI can't render two pipelines at once. But it overshoots: stages have *different* resource profiles, and one pipeline's classify leaves disk I/O and CPU cores idle. A second pipeline's scan / thumbnails / previews could fill that gap.

The win is bounded — `pipeline_job.py` already overlaps stages internally via queues, so one pipeline already saturates its bottleneck resource. The marginal gain from a second is the overlap of *one pipeline's GPU phase with another pipeline's I/O/CPU phase*, not a 2× speedup. Worth doing for the babysit-the-machine UX win as much as for the throughput.

## Concurrency model

**Two execution slots, unbounded queue.** Up to two pipelines run concurrently; the rest sit in a FIFO queue and start as slots open. Picked over per-resource-only gating because the third pipeline can't do useful work (it'd just queue on the GPU lock), and capping at 2 makes UI and cancellation reasoning much simpler.

**Single GPU semaphore (size 1), released between batches.** Every GPU-using stage (`classify`, `detect`, `eye_keypoints`, `extract_masks`) wraps its per-batch inference call in `with gpu_lock:`. Two GPU stages never run simultaneously; B's scan / thumbnails / previews / regroup run free while A holds the GPU lock for a batch. Lock is released between batches so A's 10-minute classify doesn't completely block B — they alternate at batch granularity.

**Per-workspace regroup lock.** A `workspace_regroup_locks: dict[int, threading.Lock]` acquired by the regroup stage. Two pipelines targeting the same workspace can run every stage concurrently except regroup, which serialises. This is the only stage in the existing code that genuinely races on workspace-scoped state.

**Lock order (write down to prevent deadlocks):**
1. `_progress_lock` (outermost)
2. `JobRunner._lock`
3. `workspace_regroup_locks[ws]`
4. `gpu_lock` (innermost)

## Shared model cache

Today `loaded_models` lives inside one pipeline job's stack. Two pipelines = two ONNX session loads of the same file = 2× VRAM. Fix: a process-wide `ModelCache` singleton.

**Lifecycle:** refcounted with idle eviction.

- `cache.acquire(model_id, variant)` returns a context-manager handle; refcount++ on enter, refcount-- on exit.
- When refcount hits 0, a 5-minute `threading.Timer` is armed. If nothing reacquires before it fires, the session is closed and VRAM freed. Subsequent `acquire` cancels the pending timer.
- Keyed by `(model_id, variant)`. Different models / variants are independent entries.

Independently useful: rapid re-runs (queue 5 imports back-to-back) pay the model load cost once instead of five times.

## Persistence

**Queued pipelines persist in the existing `job_history` table** with `status='queued'`. No new table.

- `job_history.config` (JSON column, already exists per `vireo/jobs.py:47`) holds the full POST body that was sent to `/api/pipeline/start`.
- Job IDs are generated at *enqueue* time (`f"pipeline-{int(time.time() * 1000)}"`) and carry through to running and terminal states. One ID per logical run.
- Pruning by `_JOB_RETENTION_SECS` already keys on `finished_at`, so queued rows are not at risk.

**Startup sweep** in `JobRunner.__init__`:
- Any `status='running'` rows from before this process started → mark `'failed'` with an "interrupted by restart" message (their threads are gone).
- Any `status='queued'` rows → the scheduler picks them up naturally as slots open.

## Queue at enqueue time

What's frozen per queued entry:

- Full POST body (`sources`, `destination`, `file_types`, `model_ids`, `recursive`, `skip_*` toggles, etc.) snapshotted into `config`.
- Resolved `workspace_id`. If the user requested `_destWorkspaceMode === 'new'`, the workspace is **created eagerly at enqueue time** (not at execution time). The new workspace's ID is what's stored. Reason: name-collision errors should surface immediately, not 20 minutes later when the slot opens.
- Models referenced by ID only. Deleted/missing model at execution time → fails fast in the existing `model_loader` stage with the same error as today.

The user can change UI settings freely after enqueueing; the queued run is unaffected.

## Scheduler

Event-driven, with a startup sweep as the fallback path.

- After any pipeline reaches a terminal state (`completed` | `failed` | `cancelled`), the runner checks: if `active_pipeline_count < 2`, attempt to promote the oldest `status='queued'` row.
- Promotion is a conditional UPDATE: `UPDATE job_history SET status='running' WHERE id=? AND status='queued'`. If `rowcount == 1`, spawn the worker thread with the persisted `config`. If `rowcount == 0` (because Cancel landed first), quietly move to the next candidate.
- Same event also fires on enqueue when a slot is free (covers the "queue is empty and a new run comes in" path).
- On startup, the same promotion loop runs once to drain queued rows up to the slot cap.

## `POST /api/pipeline/start` becomes a thin wrapper

Same request/response shape as today, but two paths:

```python
def api_pipeline_start():
    params = build_params(request.json)
    if active_pipeline_count() < SLOT_CAP:
        job_id = enqueue_and_promote_immediately(params)
    else:
        job_id = enqueue_only(params)
    return jsonify(job_id=job_id, queued=(status_is_queued))
```

Clients don't have to care which happened — the SSE stream at `/api/jobs/<id>/stream` already emits events whenever the job starts, including from `queued` → `running` transition.

## UI

**Pipeline page** (`pipeline.html`):

- `_pipelineRunning` JS guard is removed. The Start button is no longer disabled because a pipeline is running.
- Button label flips based on backend state: `"Start Pipeline"` when a slot is free now, `"Queue Pipeline"` when both slots are full.
- A small status line near the button: `"2 running • 3 queued — see Jobs"` (link to `/jobs`). Hidden when nothing is running and queue is empty.
- The main pills + progress area shows "this tab's most recently launched run." If this tab hasn't launched one this session, fall back to the most recently started run.
- Currently-shown run on this tab is tracked by JS variable (`_focusedJobId`). Tab-local; tabs don't fight each other.

**Jobs page** (`/jobs`):

- Add `'queued'` to the status filter and the list renderer (new dot color / label).
- Each queued row gets a **Cancel** button (action: `UPDATE job_history SET status='cancelled', finished_at=? WHERE id=? AND status='queued'`).
- New **"Cancel all queued"** button at the top of the list when ≥1 queued row exists. One UPDATE statement, one button.
- Detail pane for a queued row shows the persisted `config` blob in a readable form (sources, destination, model IDs, toggles).

**No new "queue strip" inline on the pipeline page.** The existing `/jobs` page already has the list/detail layout for this. The pipeline page just shows the count and links over.

## Cancellation

1. **Stop a running pipeline** — same as today. Abort event set, worker drains, run finishes with `status='cancelled'`. **No effect on queued runs.** Independent configurations; cancelling one says nothing about the others.

2. **Cancel a queued pipeline** — atomic `UPDATE` on the row. No thread to interrupt. Row stays in history with empty progress/steps as the tell.

3. **Running pipeline fails** — same: queued runs continue, next one starts as the slot opens. The user gets the existing failure toast. No "pause queue on failure" mode.

4. **Cancel all queued** — bulk UPDATE. Does not affect running pipelines.

5. **Promotion race** — solved by the conditional UPDATE described above. A Cancel click that lands between "scheduler picks this row" and "thread spawns" wins because the UPDATE matches zero rows.

## Edge cases

- **Same source path on both running pipelines.** Both scanners insert by hash; SQLite serialises, dedupe is idempotent. Wasted I/O but no corruption.
- **Same destination directory.** Two pipelines copying into the same folder template can produce filename collisions. The existing ingest code's collision handling already covers this (rename-on-collision); we don't add new logic. *Risk note:* the user should avoid this on purpose, but we don't block it.
- **Both pipelines hit `model_loader` at once.** The `ModelCache` handles this — second `acquire` sees the loaded session, refcount goes to 2.
- **Cap of 2 + many queued + heavy classify.** Queued rows pile up in `job_history` until consumed. The "Cancel all queued" escape hatch covers user regret.

## Non-goals

- Configurable slot cap (it's `SLOT_CAP = 2`, a module constant — bumpable later if measurement supports it).
- Reordering queued rows. FIFO only.
- Pausing the queue without cancelling.
- Cross-workspace prioritisation, fairness, or scheduling beyond FIFO.
- Touching `/api/jobs/scan`, `/api/jobs/classify`, etc. — standalone non-pipeline jobs remain unchanged.

## Risks / open implementation questions for the plan

- **Hoisting `loaded_models` out of `pipeline_job.py`.** The existing structure deeply assumes per-job-stack model dicts. The cache rewrite is the largest piece of work in this design; everything else is glue.
- **GPU lock granularity.** "Per-batch" is the right shape but each GPU stage in `pipeline_job.py` has slightly different batch loops. The plan needs to identify the precise wrap points stage-by-stage and verify none of them hold the lock across long Python work.
- **Workspace switching UX while two pipelines run on different workspaces.** Today the navbar shows the active workspace; the page renders that workspace's state. We're not solving multi-workspace UI in this design — both pipelines may run regardless of which workspace the user is currently viewing, but the pipeline page's "this tab's run" focus may show a run that's against a workspace the user isn't currently in. Probably fine, but worth a smoke test.
- **Tests.** Concurrency is hard to test deterministically. Plan should include: a unit test of the `ModelCache` lifecycle (acquire, release, idle eviction, race), an integration test of "start two pipelines, cancel one, second completes," and a startup-sweep test (queued rows persisted, run picks them up).

## Order of implementation

The plan should sequence work so each step is independently testable and useful:

1. **`ModelCache`** with refcounted acquire/release and idle eviction. Land standalone; one pipeline at a time still works, just with shared cached models.
2. **GPU semaphore** wrapped around each GPU stage's per-batch loop. Still one pipeline at a time — no behaviour change, just adds the lock that will gate concurrent pipelines later.
3. **Per-workspace regroup lock.** Same — added but not yet exercised concurrently.
4. **Server-side queue.** Status `'queued'`, scheduler, conditional promotion, startup sweep. `POST /api/pipeline/start` becomes wrapper. Slot cap still effectively 1 (UI still disables Start).
5. **UI changes.** Remove `_pipelineRunning` guard, flip button label, add "N running • M queued" indicator, add Cancel / Cancel-all on `/jobs`, add `'queued'` to status filter.
6. **Lift SLOT_CAP from 1 to 2.** The actual switch-on. Everything above must be stable first.

Each step lands as its own PR. Steps 1–3 ship in any order before step 4. Steps 5 and 6 can land together or separately.
