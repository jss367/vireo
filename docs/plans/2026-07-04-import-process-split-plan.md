# Import / Process Split Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Split the monolithic local-processing pipeline into an import job (copy → verify → catalog at final paths) and a process job (GPU stages on cataloged photos), chained via named process strategies.

**Architecture:** Four independent PRs (see the design doc's Sequencing). This plan gives full task-by-task detail for **PR 1 (process job + strategies)**, which is grounded in code verified today. PRs 2–4 are specified at task level; each gets its own `-plan.md` written **after** its predecessor lands, because their exact shape depends on what the earlier PRs expose.

**Tech Stack:** Python 3, Flask, SQLite (raw cursor, no ORM), Jinja2 + vanilla JS, pytest.

**Design doc:** `docs/plans/2026-07-04-import-process-split-design.md`

**Test command (run before each PR):**
```bash
python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py vireo/tests/test_process_strategies.py vireo/tests/test_pipeline_job.py -v
```
(Some tests on main are known-flaky as of 2026-04-22 — compare failures against a clean main run before blaming your change.)

`vireo/tests/test_process_strategies.py` is created in Task 1.1 and `vireo/tests/test_pipeline_job.py` gains new coverage in Tasks 1.2 and 1.6. Both files are in the command above so Task 1.7's gate exercises the contracts this plan is trying to pin — omit either and PR 1 can green-light a change that breaks the new strategy module or the pipeline-stage resume/`miss_enabled` invariants. Until Task 1.1 lands, `test_process_strategies.py` doesn't exist yet; pytest reports it as an error, which is expected and disappears once the module is committed.

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

**Scope note — the design doc's "None" / import-only choice is *not* a process strategy.** `STRATEGIES` only enumerates *processing* presets. "Do nothing after import" is a decision at the import→process boundary, not a stage-flag preset, and does not belong in this whitelist. It is represented as `None` (nullable) in two places that this PR touches:

- **Workspace default (Task 1.5):** `pipeline.default_strategy` accepts `None` (unset). `None` means "no automatic processing after import"; the save-time validator accepts `None` and any name in `STRATEGIES`, and rejects everything else.
- **Import→process chaining (PR 3, Task 3.1):** the import job stores the after-import choice in its own config; the completion hook only enqueues `/api/jobs/pipeline` when that choice is a real strategy name. `None` short-circuits the hook and never reaches `/api/jobs/pipeline`, so the strategy whitelist there can stay strict (a request with `strategy: null` or `strategy: "none"` is still a 400 — the "no process" case is expressed by not calling the endpoint at all).

This keeps the strategy vocabulary strict for the process job while preserving the import-only path the design doc calls out.

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

**Step 3:** Add `miss_enabled: bool | None = None` to `PipelineParams` (None = defer to config, preserving today's behavior). The override must be **bidirectional** — it has to work in both directions:

- **strategy disables + workspace enables** (e.g. `cull_ready` on a default workspace): short-circuit *before* `compute_misses_for_workspace` so the stage records `skipped`. A local variable alone would only gate the cache marker at `pipeline_job.py:4991`; the compute call still reads `pipeline_cfg["miss_enabled"]` from workspace config (`vireo/misses.py:381`) and would evaluate + write miss flags anyway.
- **strategy enables + workspace disables** (a user runs `full` on a workspace where misses are turned off): the guard doesn't fire (effective value is True), so we fall through to the existing compute path — but that path reads `pipeline_cfg["miss_enabled"]`, which is still False from workspace config, and `compute_misses_for_workspace` returns 0. The stage would then stamp `completed / 0 photos evaluated`, which reads as "misses ran and found none." Fix by **also injecting the effective value into `pipeline_cfg` before the guard**, so both the guard and the downstream compute call see the same effective value.

Insert the guard **immediately after** the existing `if params.skip_regroup or params.skip_classify or ... :` block at `pipeline_job.py:4936-4946`, still before the `stages["misses"]["status"] = "running"` at :4948 so the "skipped" record wins over any transient "running" write.

The guard reads `thread_db.get_effective_config(cfg.load())`, but in the current `miss_stage` **neither `thread_db` nor `cfg` are in scope at :4946** — they are only constructed and imported inside the later `try:` block (`import config as cfg` at :4955; `thread_db = Database(db_path)` at :4959; `thread_db.set_active_workspace(workspace_id)` at :4960; `effective_cfg = thread_db.get_effective_config(cfg.load())` at :4962). Inserting the guard as-is would `NameError` on every collection-scoped run that reached it. This task therefore **hoists the imports and DB/config setup above the transient "running" write** (wrapped in their own try/except so a setup failure is recorded via `errors.append` exactly as the existing block does) and **deletes** the now-duplicated lines from the later try block. The full replacement for lines 4948-4963 is:

```python
# Hoisted from the try: block below so the miss_enabled guard can
# read effective config before the transient "running" status is
# written. The existing try/except at ~:4997 no longer needs these
# imports/loads — delete lines 4955-4963 in that block after this
# insert (do NOT leave the original load in place, or the compute
# call further down will still see the un-overridden workspace value
# in the strategy-enables-but-workspace-disables case).
try:
    from datetime import UTC, datetime

    import config as cfg
    from misses import compute_misses_for_workspace
    from pipeline import load_results_raw, save_results_raw

    thread_db = Database(db_path)
    thread_db.set_active_workspace(workspace_id)

    effective_cfg = thread_db.get_effective_config(cfg.load())
    pipeline_cfg = effective_cfg.get("pipeline", {})
except Exception as e:
    # Preserve the failed-stage recording contract used by the
    # existing try/except at ~:4997 — mark the stage failed BEFORE
    # returning. The transient "running" write is *below* this guard,
    # so without stamping "failed" here the stage would stay
    # "pending"; the pipeline finalizer treats absence-of-failed as
    # success and would wrongly mark the whole job completed despite
    # a fatal setup error (cfg.load, Database(...), or any import
    # raising). Mirror the three writes the old handler did:
    # stages[...] status, runner.update_step(status="failed", error),
    # and _update_stages so the SSE/UI stream matches.
    stages["misses"]["status"] = "failed"
    runner.update_step(job["id"], "misses", status="failed", error=str(e))
    errors.append(f"[misses] Fatal: {e}")
    log.exception("Pipeline miss-detection setup failed")
    _update_stages(runner, job["id"], stages)
    return

# effective miss_enabled: per-run PipelineParams override wins over
# workspace config, mirroring how other skip_* flags override
# workspace defaults. Inject the effective value into pipeline_cfg
# *before* the guard so both branches — short-circuit-skip AND
# fall-through-to-compute — see the same value: compute_misses_for_workspace
# reads pipeline_cfg["miss_enabled"] itself (vireo/misses.py:381), so a
# strategy that enables misses on a workspace where they're disabled
# would otherwise get a silent 0 from compute.
if params.miss_enabled is not None:
    pipeline_cfg = {**pipeline_cfg, "miss_enabled": params.miss_enabled}
miss_enabled = pipeline_cfg.get("miss_enabled", True)
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

stages["misses"]["status"] = "running"
runner.update_step(job["id"], "misses", status="running")
_update_stages(runner, job["id"], stages)
```

Then in the existing try block that starts at :4952, delete the hoisted lines (`import config as cfg`, `from misses import ...`, `from pipeline import ...`, `from datetime import ...`, `thread_db = Database(...)`, `thread_db.set_active_workspace(...)`, `effective_cfg = ...`, `pipeline_cfg = ...`) — they now live above the guard. The `miss_enabled = pipeline_cfg.get("miss_enabled", True)` at :4970 is also gone (the effective value was computed above the guard and is still in scope). The `compute_misses_for_workspace(thread_db, pipeline_cfg, ...)` call at :4972 stays as-is; it now sees the injected `pipeline_cfg`.

Because the guard returns before `compute_misses_for_workspace` is reached, no `miss` rows are written and the `miss_computed_at` cache marker at `pipeline_job.py:4991` is never stamped — which is what `pipeline_review`'s "current-run misses" shortcut needs (a stale marker would surface old miss flags as fresh).

Do the same shape for `skip_detect` if Task 1.0 concluded it's needed.

**Step 4:** Run — PASS. Also run the full `test_pipeline_job.py` file. Add two peer assertions that pin the *bidirectional* override contract:

- **Skip direction:** workspace `miss_enabled: True` + `PipelineParams(miss_enabled=False)` → `compute_misses_for_workspace` is not invoked at all (patch/spy it), no `miss` rows are written for the run's collection, and `miss_computed_at` is not stamped. This pins the "override short-circuits before compute" property that a local-variable-only fix would silently break.
- **Enable direction:** workspace `miss_enabled: False` + `PipelineParams(miss_enabled=True)` → `compute_misses_for_workspace` IS invoked, and the `pipeline_cfg` it receives has `miss_enabled: True` (assert on the call args). This pins the injection property — without it, the compute call reads workspace-False, returns 0, and the stage reports "0 photos evaluated" for a run the user explicitly asked to include misses in.
- **Setup-failure direction:** patch one of the hoisted setup calls (e.g. `Database.__init__`, `cfg.load`, or `thread_db.set_active_workspace`) to raise. Assert that the misses stage records `status == "failed"` (not "pending"), `runner.update_step` was called with `status="failed"` and the exception text as `error`, and the pipeline finalizer marks the whole job failed. Without this, a fatal setup exception silently produces a "successful" job with a pending misses stage — the regression this bidirectional guard would otherwise introduce compared to the original inline try/except.

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


def test_pipeline_null_strategy_400(client, ...):
    # Scope Note (Task 1.1): the "no process" case is expressed by
    # NOT calling /api/jobs/pipeline. A present-but-null strategy must
    # 400 so the server never silently falls through to default/full
    # processing when a caller thought they were opting out. Distinct
    # from "unknown strategy" — null is a shape error, not a name error.
    resp = client.post("/api/jobs/pipeline", json={
        "collection_id": cid, "strategy": None,
    })
    assert resp.status_code == 400


def test_pipeline_none_string_strategy_400(client, ...):
    # The literal string "none" is not a valid strategy name either
    # (STRATEGIES only holds full / cull_ready / quick_look). This pins
    # the "strict whitelist" half of the Scope Note.
    resp = client.post("/api/jobs/pipeline", json={
        "collection_id": cid, "strategy": "none",
    })
    assert resp.status_code == 400


def test_pipeline_omitted_strategy_uses_body_params(client, ...):
    # No `strategy` key at all -> the route builds PipelineParams from
    # the body as usual (this is how folder/collection runs without a
    # preset keep working). Distinguishing "omitted" from "null" is
    # exactly why Task 1.3 Step 3 must check key presence, not truthiness.
    resp = client.post("/api/jobs/pipeline", json={"collection_id": cid})
    assert resp.status_code == 200
    job = get_job_config(resp.get_json()["job_id"])
    assert job.get("strategy") is None


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

**Step 3:** In `api_job_pipeline`, before `PipelineParams` is built:

- **Check key presence, not truthiness.** Use `if "strategy" in body:` — *not* `if body.get("strategy"):`. `body.get("strategy")` treats `strategy: null`, `strategy: ""`, and an omitted key identically, which would silently enqueue the default/full processing path for a caller who sent `strategy: null` thinking it meant "no process." The Scope Note above promises `strategy: null` is a 400; only omission means "use body params directly."
- **When the key is present:** reject non-strings (including `None`) with 400 via the route's existing `json_error` helper (message shape: `"strategy must be a string, got null"` / `"...got int"`), then call `resolve_strategy` — which 400s on unknown names, including the literal string `"none"`. Apply the expansion as *defaults*, then let any explicitly-present body keys override (that's the `test_pipeline_explicit_flags_beat_strategy` contract).
- Record `strategy` in the job config for history/UI (still `None` when the key was omitted).

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

**Step 1: Failing tests** — cover the nullable-default contract from the Task 1.1 Scope Note (the workspace default is what PR 3's completion hook reads to decide whether to enqueue processing at all):

- Set `{"pipeline": {"default_strategy": "cull_ready"}}` in workspace overrides; assert `get_effective_config` surfaces it.
- Set `{"pipeline": {"default_strategy": None}}` (or omit the key entirely — both mean "no automatic processing after import"); assert the save succeeds and `get_effective_config` returns `None` for `default_strategy`. This is the property PR 3's chaining hook depends on to short-circuit before calling `/api/jobs/pipeline`; if it 400s, the "import only" user flow is unreachable.
- Set `{"pipeline": {"default_strategy": "yolo"}}`; assert 400 at save.
- Set `{"pipeline": {"default_strategy": "none"}}`; assert 400 — the string `"none"` is *not* the null sentinel. The "no process" case uses JSON `null`, matching the `/api/jobs/pipeline` contract from Task 1.3 (which also rejects `strategy: "none"`). A single vocabulary keeps the workspace default, the API body, and PR 3's chaining hook consistent.

**Step 2–4:** standard TDD loop. Validation at save short-circuits `None` before the whitelist check (`resolve_strategy(None)` raises `ValueError` because `None not in STRATEGIES` — do not pass `None` through it; treat `None` as a boundary sentinel meaning "unset," accept it, and call `resolve_strategy` only when a real string was supplied). Everything else reuses `resolve_strategy`.

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

- **Task 3.1** — import-completion hook. The after-import choice comes from the import job's config (defaulting to the workspace's `pipeline.default_strategy` from Task 1.5, which is nullable per the Task 1.1 Scope Note). **Short-circuit before enqueueing when the choice is `None`** — this is the "import only" path; the hook logs the skip and returns without calling `/api/jobs/pipeline`. This is required because Task 1.3 makes `strategy: null` a 400, so a naive "always enqueue with the raw choice" would either fail the completion hook or create a failed follow-up job for every import-only run. **Only when the choice is a real strategy name** does the hook POST `/api/jobs/pipeline` with `{collection_id: <ad-hoc collection of imported photo ids>, strategy: <name>}`. Two rows in job history when processing runs; one row (the import) when the import-only path is taken. When enqueued, the import result links to the process job id. Test both branches: `after_import: null` → no follow-up job, no failed job, hook records "skipped: import-only"; `after_import: "cull_ready"` → follow-up job appears with the expected strategy and collection id.
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
