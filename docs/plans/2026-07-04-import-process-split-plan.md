# Import / Process Split Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Split the monolithic local-processing pipeline into an import job (copy → verify → catalog at final paths) and a process job (GPU stages on cataloged photos), chained via named process strategies.

**Architecture:** Four independent PRs (see the design doc's Sequencing). This plan gives full task-by-task detail for **PR 1 (process job + strategies)**, which is grounded in code verified today. PRs 2–4 are specified at task level; each gets its own `-plan.md` written **after** its predecessor lands, because their exact shape depends on what the earlier PRs expose.

**Tech Stack:** Python 3, Flask, SQLite (raw cursor, no ORM), Jinja2 + vanilla JS, pytest.

**Design doc:** `docs/plans/2026-07-04-import-process-split-design.md`

**Test command (run before each PR):**
```bash
python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py -v
```
(Some tests on main are known-flaky as of 2026-04-22 — compare failures against a clean main run before blaming your change.)

**Key existing code to know:**
- `vireo/pipeline_job.py:79` — `PipelineParams`. Skip flags that exist today: `skip_extract_masks`, `skip_regroup`, `skip_classify`, `skip_eye_keypoints`. There is **no** `skip_detect` or `skip_misses`; misses are gated by `pipeline_cfg["miss_enabled"]` (line ~4970).
- `vireo/pipeline_job.py:905` — `skip_scan = collection_id is not None`. A collection-scoped run already does exactly what the process job needs: every GPU stage resolves its photo set via `get_collection_photos(collection_id, per_page=999999)`.
- `vireo/pipeline_job.py:2124` — import runs create an ad-hoc collection: `add_collection(name, json.dumps([{"field": "photo_ids", "value": ids}]))`. Folder-scoped process runs reuse this exact pattern.
- `vireo/app.py:16463` — `POST /api/jobs/pipeline` builds `PipelineParams` from the request body.
- `vireo/db.py` — `get_effective_config(cfg.load())` merges `workspaces.config_overrides` (JSON column) over global config; the `"pipeline"` sub-dict override pattern is already used for `sam2_variant` etc.

---

## Phase 1 — PR 1: Process job + strategies

A "process job" is a collection-scoped pipeline run (that machinery exists). This PR adds: named strategy presets that expand to stage flags, a folder scope, a per-workspace default strategy, and regression tests that pin the per-photo resume contract.

### Task 1.0: Reconnaissance — map every stage gate (no code changes)

**Files:** read `vireo/pipeline_job.py` only.

**Step 1:** For each stage function in `run_pipeline_job` (thumbnails, previews, detect, classify, extract_masks, eye_keypoints, misses, regroup), record what gates it: which `params.skip_*` flag, which config key, and whether it early-returns when `collection_id` is None. Specifically answer: **is detect gated by `skip_classify`, or does it always run?** (Detect writes `detections` rows the classify stage consumes; grep the detect stage's entry conditions around line 3164/3208.)

**Step 2:** Write the findings as a comment block at the top of the new `vireo/process_strategies.py` in Task 1.1. If detect is NOT skippable when classify is skipped, the `quick_look` strategy needs a new `skip_detect` param — add it in Task 1.2; if `skip_classify` already prevents detect, Task 1.2 shrinks to just `skip_misses`.

### Task 1.1: Strategy presets module

**Files:**
- Create: `vireo/process_strategies.py`
- Test: `vireo/tests/test_process_strategies.py` (new)

**Step 1: Write the failing test**

```python
"""Strategy presets are pure data: name -> PipelineParams overrides."""
import pytest

from process_strategies import STRATEGIES, resolve_strategy


def test_known_strategies():
    assert set(STRATEGIES) == {"full", "cull_ready", "quick_look"}


def test_full_skips_nothing():
    flags = resolve_strategy("full")
    assert not any(v for k, v in flags.items() if k.startswith("skip_"))
    assert flags["miss_enabled"] is True


def test_cull_ready_skips_expensive_extras():
    flags = resolve_strategy("cull_ready")
    assert flags["skip_extract_masks"] is True
    assert flags["skip_eye_keypoints"] is True
    assert flags["miss_enabled"] is False
    # classify and regroup stay on: review pages need predictions + encounters
    assert flags["skip_classify"] is False
    assert flags["skip_regroup"] is False


def test_quick_look_is_thumbs_and_previews_only():
    flags = resolve_strategy("quick_look")
    assert flags["skip_classify"] is True
    assert flags["skip_extract_masks"] is True
    assert flags["skip_eye_keypoints"] is True
    assert flags["skip_regroup"] is True
    assert flags["miss_enabled"] is False


def test_unknown_strategy_raises():
    with pytest.raises(ValueError, match="unknown strategy"):
        resolve_strategy("yolo")
```

Note the deliberate deviation from the design table: `cull_ready` keeps **regroup** (cheap, no GPU, and `pipeline_review` requires encounters to render) — record this in the module docstring.

**Step 2: Run it — expect FAIL** (`ModuleNotFoundError`).

Run: `python -m pytest vireo/tests/test_process_strategies.py -v`

**Step 3: Implement `vireo/process_strategies.py`**

```python
"""Named processing-stage presets ("process strategies").

A strategy is pure data: a dict of PipelineParams skip-flag overrides plus
``miss_enabled`` (misses are config-gated, not param-gated). The API layer
expands a strategy name server-side so the import page, the process page,
and import→process chaining all share one vocabulary.

Deviation from the design table: cull_ready keeps regroup — it is cheap
(no GPU) and pipeline_review requires encounters.
"""

_BASE = {
    "skip_classify": False,
    "skip_extract_masks": False,
    "skip_eye_keypoints": False,
    "skip_regroup": False,
    "miss_enabled": True,
}

STRATEGIES = {
    "full": {},
    "cull_ready": {
        "skip_extract_masks": True,
        "skip_eye_keypoints": True,
        "miss_enabled": False,
    },
    "quick_look": {
        "skip_classify": True,
        "skip_extract_masks": True,
        "skip_eye_keypoints": True,
        "skip_regroup": True,
        "miss_enabled": False,
    },
}


def resolve_strategy(name):
    """Expand a strategy name into stage-flag overrides. Raises ValueError."""
    if name not in STRATEGIES:
        raise ValueError(
            f"unknown strategy: {name!r} (expected one of {sorted(STRATEGIES)})"
        )
    return {**_BASE, **STRATEGIES[name]}
```

(If Task 1.0 found detect needs its own gate for quick_look, add `"skip_detect": True` to `quick_look` and `False` to `_BASE`, and extend the test.)

**Step 4: Run it — expect PASS.**

**Step 5: Commit** — `feat: add process strategy presets`

### Task 1.2: `miss_enabled` (and `skip_detect` if needed) become per-run overrides

**Files:**
- Modify: `vireo/pipeline_job.py` (`PipelineParams` at :79; the misses gate at ~:4970)
- Test: `vireo/tests/test_pipeline_job.py`

**Step 1: Failing test** — construct `PipelineParams(miss_enabled=False)` and assert the misses stage records "skipped" even when workspace config has `miss_enabled: true`. Follow the existing skip-flag test patterns in `test_pipeline_job.py` (grep `skip_extract_masks` there for the harness style).

**Step 2:** Run — expect FAIL (unexpected keyword).

**Step 3:** Add `miss_enabled: bool | None = None` to `PipelineParams` (None = defer to config, preserving today's behavior). At the misses gate, compute the effective value and **short-circuit before calling `compute_misses_for_workspace` when disabled** — mirror the existing skip pattern at `pipeline_job.py:4943-4946` so the stage records `skipped` (not `completed / 0 photos evaluated`). Just injecting the value into the config the helper sees would still trip the "completed" path, because `compute_misses_for_workspace` returns 0 rather than raising when `miss_enabled` is False (`vireo/misses.py:381`) and the current stage code stamps completion using that count.

Insert the guard **immediately after** the existing `if params.skip_regroup or params.skip_classify or ... :` block at `pipeline_job.py:4936-4946` (still before the `stages["misses"]["status"] = "running"` at :4948, so the "skipped" record wins over any transient "running" write):

```python
# effective miss_enabled: per-run PipelineParams override wins over
# workspace config, mirroring how other skip_* flags override
# workspace defaults.
effective_cfg = thread_db.get_effective_config(cfg.load())
pipeline_cfg = effective_cfg.get("pipeline", {})
miss_enabled = (
    params.miss_enabled
    if params.miss_enabled is not None
    else pipeline_cfg.get("miss_enabled", True)
)
if not miss_enabled:
    # Match the "skipped" contract used at :4943 for skip_regroup /
    # skip_classify: stage status = "skipped", step summary = "Skipped".
    # Do NOT fall through to compute_misses_for_workspace: it returns 0
    # when disabled and the existing completion path would then stamp
    # "0 photos evaluated", which reads as "misses ran and found none"
    # rather than "misses were disabled" in the job UI.
    stages["misses"]["status"] = "skipped"
    runner.update_step(job["id"], "misses", status="completed",
                       summary="Skipped")
    _update_stages(runner, job["id"], stages)
    return
```

Because the guard returns before `compute_misses_for_workspace` is reached, no `miss` rows are written and the `miss_computed_at` cache marker at `pipeline_job.py:4991` is never stamped — which is what `pipeline_review`'s "current-run misses" shortcut needs (a stale marker would surface old miss flags as fresh). The existing `pipeline_cfg`/`thread_db` reads that happen inside the current `try:` block (`:4959-4963`) move up above the guard so both branches share them; alternatively, keep them inside `try:` and duplicate the two-line load — pick whichever the harness style in `test_pipeline_job.py` reads cleaner against.

Do the same shape for `skip_detect` if Task 1.0 concluded it's needed.

**Step 4:** Run — PASS. Also run the full `test_pipeline_job.py` file. Add a second assertion to the failing test (or a peer test) that with workspace `miss_enabled: True` and `PipelineParams(miss_enabled=False)`, `compute_misses_for_workspace` is not invoked at all (patch/spy it), no `miss` rows are written for the run's collection, and `miss_computed_at` is not stamped — this pins the "override short-circuits before compute" property that a local-variable-only fix would silently break.

**Step 5: Commit** — `feat: per-run miss_enabled override on PipelineParams`

### Task 1.3: API accepts `strategy`

**Files:**
- Modify: `vireo/app.py` (`api_job_pipeline`, :16463)
- Test: `vireo/tests/test_jobs_api.py`

**Step 1: Failing tests**

```python
def test_pipeline_strategy_expands_flags(client, ...):
    resp = client.post("/api/jobs/pipeline", json={
        "collection_id": cid, "strategy": "quick_look",
    })
    assert resp.status_code == 200
    # job config records the expanded flags AND the strategy name
    job = get_job_config(resp.get_json()["job_id"])
    assert job["strategy"] == "quick_look"
    assert job["skip_classify"] is True


def test_pipeline_unknown_strategy_400(client, ...):
    resp = client.post("/api/jobs/pipeline", json={
        "collection_id": cid, "strategy": "yolo",
    })
    assert resp.status_code == 400
    assert "unknown strategy" in resp.get_json()["error"]


def test_pipeline_explicit_flags_beat_strategy(client, ...):
    # A caller may pin one flag on top of a strategy; explicit wins.
    resp = client.post("/api/jobs/pipeline", json={
        "collection_id": cid, "strategy": "full", "skip_regroup": True,
    })
    job = get_job_config(resp.get_json()["job_id"])
    assert job["skip_regroup"] is True
```

Match the file's existing fixture/client conventions rather than inventing new ones.

**Step 2:** Run — FAIL.

**Step 3:** In `api_job_pipeline`, before `PipelineParams` is built: if `body.get("strategy")`, call `resolve_strategy` (400 on `ValueError` via the route's existing `json_error` helper), apply the expansion as *defaults*, then let any explicitly-present body keys override. Record `strategy` in the job config for history/UI.

**Step 4:** Run — PASS.

**Step 5: Commit** — `feat: /api/jobs/pipeline accepts a strategy name`

### Task 1.4: Folder-scoped process runs

**Files:**
- Modify: `vireo/app.py` (`api_job_pipeline`)
- Test: `vireo/tests/test_jobs_api.py`

**Step 1: Failing tests** — cover both the flat and recursive cases so a flat `folder_id IN (...)` implementation cannot pass:

- POST `{"folder_ids": [fid], "strategy": "quick_look"}` (no `collection_id`, no `source`). Expect 200; the created job's config carries a `collection_id` for an ad-hoc collection whose photos are exactly the folder's photos.
- Build a fixture with a workspace root folder and at least one child folder (both linked to the workspace) holding its own photos. POST `{"folder_ids": [<root_id>], ...}`. Expect the ad-hoc collection's photo set to include **both** the root's direct photos and every descendant's photos — matching how the rest of the app treats folder scopes (see `Database.get_workspace_folder_roots` at `vireo/db.py:1669` and the recursive expansion in `get_folder_subtree_ids` at `vireo/db.py:2106`, which is what folder-scoped missing-photo checks and workspace-root counting use).
- Folder not linked to the active workspace → 404 (mirror the guard in `api_folder_rescan`, `app.py:12700`; note that `get_folder_subtree_ids` itself already refuses to walk out of the active workspace, so unrelated descendants can never leak in).

**Step 2:** Run — FAIL.

**Step 3:** Implement in the route:
- Reject any folder not linked to the active workspace (before subtree expansion, using the same guard shape as `api_folder_rescan`).
- For each requested folder, expand to its active-workspace subtree with `db.get_folder_subtree_ids(folder_id)` and union the results (dedup — a workspace can link both a root and a nested folder, so the same descendant can appear twice).
- Resolve photo ids off the unioned set: `SELECT id FROM photos WHERE folder_id IN (...)` scoped by the workspace. A flat `folder_id IN (:folder_ids)` over the raw request would only catch photos hanging directly off the selected folders and miss the bulk of a normal dated archive tree.
- Create the ad-hoc collection exactly like `pipeline_job.py:2124` does (`add_collection(name, json.dumps([{"field": "photo_ids", "value": ids}]))`, name like `"Process <folder basename> <timestamp>"`), and proceed as a collection run. No new pipeline machinery.

**Step 4:** Run — PASS.

**Step 5: Commit** — `feat: folder-scoped process runs via ad-hoc collections`

### Task 1.5: Per-workspace default strategy

**Files:**
- Modify: `vireo/app.py` (wherever workspace config overrides are saved — grep `config_overrides` routes)
- Test: `vireo/tests/test_config.py` or `tests/test_workspaces.py` (follow where `sam2_variant` override tests live)

**Step 1: Failing test** — set `{"pipeline": {"default_strategy": "cull_ready"}}` in workspace overrides; assert `get_effective_config` surfaces it and that an invalid name is rejected at save time (400).

**Step 2–4:** standard TDD loop. Validation at save reuses `resolve_strategy`.

**Step 5: Commit** — `feat: per-workspace default process strategy`

### Task 1.6: Pin the resume contract (regression tests only)

**Files:**
- Test: `vireo/tests/test_pipeline_job.py`

The design's promise is "re-running a strategy re-does only missing work." Much of the skip behavior exists (thumbnail cache, classify `cached` counters, mask fingerprints) but is not pinned as a contract.

**Step 1:** Write a test that runs a collection pipeline twice back-to-back with the same params over a small fixture set and asserts the second run's stage results report `generated == 0` / full cached counts for thumbnails, previews, and classify. Use the existing pipeline test harness in `test_pipeline_job.py` (it already fakes models — copy the pattern of the nearest end-to-end test there).

**Step 2:** Run it. **If it fails, the failure is the finding** — file the gap and fix the specific stage's skip check (each fix is its own commit). If it passes, it's now a regression guard.

**Step 3: Commit** — `test: pin per-photo resume contract for process runs`

### Task 1.7: Full suite + PR

Run the standard test command, then `gh pr create --base main` titled "Process job: strategy presets, folder scope, workspace default (import/process split PR 1)". Reference the design doc and PR #1101. Push review fixes to the same branch.

---

## Phase 2 — PR 2: Import job (task-level; write its own plan doc first)

The import job copies card → archive directly, verifies, and catalogs incrementally at final paths. **Before implementing, write `docs/plans/<date>-import-job-plan.md`** at Task-1-level granularity; the tasks below are its skeleton.

- **Task 2.1** — `vireo/import_job.py`: a `run_import_job` that walks `ingest.discover_source_files(source, file_types, recursive)`, plans destinations via `ingest.build_destination_path(exif_ts, folder_template)`, and filters through `import_dedup.DuplicateChecker` (the same gate `/api/import/check-duplicates` uses, so previews stay truthful).
- **Task 2.2** — per-file commit loop: copy (rsync per batch or `_copy_and_verify` from `move.py` per file), hash-verify, then index *that file* through the scanner's single-file path so folder rows + workspace links + working copy happen exactly as a rescan would. The photo row must not exist before verification passes (the design's core invariant). Photos gain nothing new: `file_hash`, `hash_checked_at`, `hash_status` columns already exist.
- **Task 2.3** — working-copy extraction during import reads from the *source* (card) path while writing the archive copy, so the NAS is never re-read. Verify `scanner`'s working-copy extractor can be pointed at an alternate read path; if not, extract first, move into place keyed by the new photo id.
- **Task 2.4** — job type `"import"` in `JobRunner` + `POST /api/jobs/import` (source(s), destination or remote target, template, file_types, after-import strategy name stored in config for PR 3). Progress = per-folder copied/verified counts.
- **Task 2.5** — "safe to format card" result field: true iff every discovered source file is either hash-verified at the destination or was skipped as a verified duplicate.
- **Task 2.6** — interruption tests: kill the job mid-run (cancel), assert the catalog contains only verified files, then re-run and assert the dedup gate skips exactly what landed and copies the rest. This is the test that proves `_deindex_staging` is unnecessary for the new path.
- **Task 2.7** — remote (SSH) destination: reuse `build_remote_move_spec` / `rsync_dest_spec` from `move.py`; catalog at `mount_path` per `resolve_remote_archive`'s mapping. Verification via the existing `--checksum` dry-run (`_remote_verify_complete`) — resolve the design doc's open question on cost here.

## Phase 3 — PR 3: Chaining + page split (task-level)

- **Task 3.1** — import-completion hook enqueues `/api/jobs/pipeline` with `{collection_id: <ad-hoc collection of imported photo ids>, strategy: <after-import choice>}`. Two rows in job history; the import result links to the process job id.
- **Task 3.2** — Import page: extract the wizard's Stage-1 import path from `pipeline.html` into `import.html` (source, destination, template, duplicate preview via `/api/import/check-duplicates`, "After import" strategy menu defaulting to the workspace default, per-folder progress, safe-to-format pill).
- **Task 3.3** — Process page: scope picker (folders / collection / new-images snapshot), strategy menu, per-stage rows with honest cached counts. Status text follows CORE_PHILOSOPHY — "Already done" must mean the next run is a no-op *given the current selections* (reuse the readiness endpoints that already take selections).
- **Task 3.4** — workspace `tabs` config: replace the `pipeline` tab with `import` + `process` (solo-user app: just change the default and update Julius's workspaces in place, no migration shim).
- **Task 3.5** — user-first E2E pass with Playwright: import a fixture card into a temp archive, watch folders appear mid-import, cancel processing, re-run, verify nothing re-copies.

## Phase 4 — PR 4: Demolition (task-level)

- **Task 4.1** — remove `local_processing` from `PipelineParams` and `api_job_pipeline`; delete `_deindex_staging` (4 call sites), the archive stage, and staging creation in `pipeline_job.py`.
- **Task 4.2** — delete staging math from `pipeline_plan.py` (`staging_*`, `batch_count`, `batching_required`) and the destination-overlap / must-be-new-archive-path fatals.
- **Task 4.3** — startup self-heal: on boot, if `~/.vireo/staging/pipeline-*` dirs exist, surface a one-click cleanup card (list dir sizes; delete on confirm). No auto-delete.
- **Task 4.4** — sweep tests: `test_pipeline_job.py` staging/archive tests convert to import-job tests or die with the code they tested.

---

## Execution notes

- One worktree per PR; review between phases (the review bot re-reviews on push; fixes go to the same branch).
- Phases must land in order — 3 depends on 1+2, 4 depends on 3 being the UI.
- The currently-shipping pipeline keeps working until Phase 3 flips the UI; there is no flag-day.
