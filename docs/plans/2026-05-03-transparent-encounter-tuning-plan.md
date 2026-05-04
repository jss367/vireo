# Transparent Encounter Tuning Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make the encounter-grouping algorithm fully visible (per-cut-point scores with component breakdown) and tunable (every weight + threshold) from the pipeline review page's left sidebar, with live re-grouping on slider change.

**Architecture:** Three pieces. (1) `vireo/encounters.py` gains an opt-in `emit_trace=True` mode that records every adjacent-pair S_enc score, its 5 components, the gap, and the cut/keep decision; threaded through `pipeline.run_full_pipeline` and the existing `/api/pipeline/regroup-live` endpoint. (2) `pipeline_review.html` left sidebar exposes all ~12 grouping knobs grouped by stage (Cut / Merge / Burst), always visible, wired to existing `onGroupingChange` debounced live regroup. (3) New always-visible "Algorithm trace" panel in the sidebar shows the focused encounter's per-cut-point readout; clicking a card sets focus; new `/api/pipeline/save-grouping-defaults` persists the current weights to `~/.vireo/config.json`.

**Tech Stack:** Python 3 / Flask / vanilla JS / Jinja2. Tests via `pytest`. Manual UI verification with Playwright (per `feedback_user_first_testing` memory).

---

## Quick reference

**Existing endpoints already in place:**
- `POST /api/pipeline/regroup-live` (`vireo/app.py:10127`) — accepts `{config: {...}}`, runs `run_full_pipeline`, returns serialized results. We extend its response.

**Existing UI hooks already in place:**
- `onGroupingChange()` (`vireo/templates/pipeline_review.html:1536`) — debounced 500ms, calls `doRegroupLive()` (line 1565) which POSTs to `/api/pipeline/regroup-live` and re-renders.
- `getGroupingConfig()` returns the current slider values to the endpoint. We extend it to include the new sliders.

**Files we will touch:**
- `vireo/encounters.py` — add trace emission
- `vireo/pipeline.py` — propagate trace into serialized results
- `vireo/app.py` — add save-defaults endpoint
- `vireo/templates/pipeline_review.html` — sidebar sliders, trace panel, focus selection, save button
- `vireo/tests/test_encounters.py` — trace emission tests
- `vireo/tests/test_app.py` — save-defaults endpoint test

**Test command (per CLAUDE.md):**
```bash
python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py vireo/tests/test_encounters.py -v
```

---

## Task 1: `compute_s_enc` returns components when asked

**Files:**
- Modify: `vireo/encounters.py:166-222` (`compute_s_enc`)
- Test: `vireo/tests/test_encounters.py`

**Step 1: Write the failing test**

Append to `vireo/tests/test_encounters.py`:

```python
def test_compute_s_enc_returns_components_when_asked():
    from encounters import compute_s_enc
    photo_a = {
        "timestamp": "2026-03-07T11:32:04",
        "latitude": 33.7, "longitude": -118.0,
        "focal_length": 600.0,
    }
    photo_b = {
        "timestamp": "2026-03-07T11:32:09",
        "latitude": 33.7, "longitude": -118.0,
        "focal_length": 600.0,
    }
    score, components = compute_s_enc(photo_a, photo_b, return_components=True)
    assert isinstance(score, float)
    assert set(components.keys()) >= {"time", "subj", "global", "species", "meta"}
    # Each component is a dict {value, weight, used}
    assert components["time"]["value"] >= 0.0
    assert components["time"]["weight"] == 0.35  # default w_time
    assert components["time"]["used"] is True   # both photos have timestamps
    assert components["species"]["used"] is False  # neither has species_top5
```

**Step 2: Run test to verify it fails**

```bash
cd /Users/julius/conductor/workspaces/vireo/tashkent
python -m pytest vireo/tests/test_encounters.py::test_compute_s_enc_returns_components_when_asked -v
```
Expected: FAIL — `compute_s_enc` does not accept `return_components` keyword.

**Step 3: Modify `compute_s_enc`**

Change the signature and return structure. Replace the function body (`vireo/encounters.py:166-222`) with:

```python
def compute_s_enc(photo_a, photo_b, config=None, return_components=False):
    """Compute the combined encounter similarity score S_enc(a, b).

    When return_components=True, returns (score, components_dict) where
    components_dict maps each signal name to {value, weight, used}.
    """
    cfg = {**DEFAULTS, **(config or {})}

    ts_a = _parse_timestamp(photo_a.get("timestamp"))
    ts_b = _parse_timestamp(photo_b.get("timestamp"))
    dt = _time_delta_seconds(ts_a, ts_b)

    st = sim_time(dt, tau=cfg["tau_enc"])
    ss = sim_embedding(photo_a.get("dino_subject_embedding"), photo_b.get("dino_subject_embedding"))
    sg = sim_embedding(photo_a.get("dino_global_embedding"), photo_b.get("dino_global_embedding"))
    sp = sim_species(photo_a.get("species_top5"), photo_b.get("species_top5"))
    sm = sim_meta(photo_a, photo_b)

    used = {
        "time": dt != float("inf"),
        "subj": (photo_a.get("dino_subject_embedding") is not None
                 and photo_b.get("dino_subject_embedding") is not None),
        "global": (photo_a.get("dino_global_embedding") is not None
                   and photo_b.get("dino_global_embedding") is not None),
        "species": bool(photo_a.get("species_top5") and photo_b.get("species_top5")),
        "meta": True,
    }
    weight_keys = {"time": "w_time", "subj": "w_subj", "global": "w_global",
                   "species": "w_species", "meta": "w_meta"}
    values = {"time": st, "subj": ss, "global": sg, "species": sp, "meta": sm}

    total_weight = sum(cfg[weight_keys[k]] for k, u in used.items() if u)
    if total_weight == 0:
        s_enc = 0.0
    else:
        s_enc = sum(cfg[weight_keys[k]] * values[k] for k, u in used.items() if u) / total_weight

    if not return_components:
        return s_enc

    components = {
        k: {"value": float(values[k]), "weight": float(cfg[weight_keys[k]]), "used": bool(used[k])}
        for k in values
    }
    return s_enc, components
```

**Step 4: Run test to verify it passes**

```bash
python -m pytest vireo/tests/test_encounters.py::test_compute_s_enc_returns_components_when_asked -v
```
Expected: PASS.

**Step 5: Run the full encounters test file to verify no regression**

```bash
python -m pytest vireo/tests/test_encounters.py -v
```
Expected: all existing tests PASS. (The default code path is unchanged because `return_components` defaults to False.)

**Step 6: Commit**

```bash
git add vireo/encounters.py vireo/tests/test_encounters.py
git commit -m "encounters: opt-in component breakdown from compute_s_enc"
```

---

## Task 2: `cut_microsegments` emits per-pair trace

**Files:**
- Modify: `vireo/encounters.py:228-304` (`cut_microsegments`)
- Test: `vireo/tests/test_encounters.py`

**Step 1: Write the failing test**

Append:

```python
def test_cut_microsegments_emits_trace():
    from encounters import cut_microsegments
    # 3 photos: two close-in-time, one far apart -> hard time cut between #2 and #3
    photos = [
        {"timestamp": "2026-03-07T11:32:00", "latitude": 33.7, "longitude": -118.0, "focal_length": 600.0},
        {"timestamp": "2026-03-07T11:32:05", "latitude": 33.7, "longitude": -118.0, "focal_length": 600.0},
        {"timestamp": "2026-03-07T11:40:00", "latitude": 33.7, "longitude": -118.0, "focal_length": 600.0},
    ]
    segments, trace = cut_microsegments(photos, emit_trace=True)
    assert len(segments) == 2  # cut between #2 and #3
    assert len(trace) == 2  # one entry per adjacent pair
    # Pair 0->1: kept (small gap, no cut)
    assert trace[0]["pair_index"] == 0
    assert trace[0]["decision"] == "kept"
    assert trace[0]["dt_seconds"] == 5.0
    assert "components" in trace[0]
    # Pair 1->2: hard time cut
    assert trace[1]["pair_index"] == 1
    assert trace[1]["decision"] == "cut_time"
    assert trace[1]["dt_seconds"] == 475.0
```

**Step 2: Run test, verify it fails**

```bash
python -m pytest vireo/tests/test_encounters.py::test_cut_microsegments_emits_trace -v
```
Expected: FAIL — function doesn't accept `emit_trace`.

**Step 3: Modify `cut_microsegments`**

Replace the body of `cut_microsegments` (`vireo/encounters.py:228-304`) — keep the existing logic, add a parallel `trace` accumulator:

```python
def cut_microsegments(photos, config=None, emit_trace=False):
    cfg = {**DEFAULTS, **(config or {})}

    if len(photos) <= 1:
        if emit_trace:
            return ([photos] if photos else []), []
        return [photos] if photos else []

    sorted_photos = sorted(
        photos,
        key=lambda p: _parse_timestamp(p.get("timestamp")) or datetime.min,
    )

    cuts = set()
    recent_scores = []
    trace = [] if emit_trace else None

    for i in range(len(sorted_photos) - 1):
        if emit_trace:
            score, components = compute_s_enc(
                sorted_photos[i], sorted_photos[i + 1], config=cfg, return_components=True
            )
        else:
            score = compute_s_enc(sorted_photos[i], sorted_photos[i + 1], config=cfg)
            components = None

        ts_a = _parse_timestamp(sorted_photos[i].get("timestamp"))
        ts_b = _parse_timestamp(sorted_photos[i + 1].get("timestamp"))
        dt = _time_delta_seconds(ts_a, ts_b)

        bid_a = sorted_photos[i].get("burst_id")
        bid_b = sorted_photos[i + 1].get("burst_id")
        decision = None

        if bid_a is not None and bid_b is not None and bid_a == bid_b:
            decision = "burst_id_kept"
            recent_scores.append(score)
            if len(recent_scores) > 3:
                recent_scores.pop(0)
        elif dt > cfg["hard_cut_time"]:
            cuts.add(i)
            recent_scores = []
            decision = "cut_time"
        elif score < cfg["hard_cut_score"]:
            cuts.add(i)
            recent_scores = []
            decision = "cut_score"
        else:
            recent_scores.append(score)
            if len(recent_scores) > 3:
                recent_scores.pop(0)
            if len(recent_scores) >= 3:
                below = sum(1 for s in recent_scores if s < cfg["soft_cut_score"])
                if below >= 2:
                    cuts.add(i)
                    recent_scores = []
                    decision = "cut_soft"
            if decision is None:
                decision = "kept"

        if emit_trace:
            trace.append({
                "pair_index": i,
                "score": float(score),
                "dt_seconds": float(dt) if dt != float("inf") else None,
                "decision": decision,
                "components": components,
                "thresholds": {
                    "hard_cut_time": cfg["hard_cut_time"],
                    "hard_cut_score": cfg["hard_cut_score"],
                    "soft_cut_score": cfg["soft_cut_score"],
                },
            })

    segments = []
    start = 0
    for i in sorted(cuts):
        segments.append(sorted_photos[start:i + 1])
        start = i + 1
    segments.append(sorted_photos[start:])
    segments = [seg for seg in segments if seg]

    if emit_trace:
        return segments, trace
    return segments
```

**Step 4: Run test**

```bash
python -m pytest vireo/tests/test_encounters.py::test_cut_microsegments_emits_trace -v
```
Expected: PASS.

**Step 5: Run full encounters tests for regression check**

```bash
python -m pytest vireo/tests/test_encounters.py -v
```
Expected: all PASS.

**Step 6: Commit**

```bash
git add vireo/encounters.py vireo/tests/test_encounters.py
git commit -m "encounters: emit per-pair cut-point trace on demand"
```

---

## Task 3: `segment_encounters` propagates trace into encounter dicts

**Files:**
- Modify: `vireo/encounters.py:464-502` (`segment_encounters`)
- Modify: `vireo/encounters.py:397-427` (`merge_microsegments`) — needs to know which microsegment each merged-segment came from so we can map trace entries to the final encounter
- Test: `vireo/tests/test_encounters.py`

**Step 1: Failing test**

```python
def test_segment_encounters_attaches_trace_to_each_encounter():
    from encounters import segment_encounters
    # 4 photos: two pairs separated by big time gap -> 2 encounters of 2 photos each
    photos = [
        {"timestamp": "2026-03-07T11:32:00", "latitude": 33.7, "longitude": -118.0, "focal_length": 600.0},
        {"timestamp": "2026-03-07T11:32:05", "latitude": 33.7, "longitude": -118.0, "focal_length": 600.0},
        {"timestamp": "2026-03-07T11:50:00", "latitude": 33.7, "longitude": -118.0, "focal_length": 600.0},
        {"timestamp": "2026-03-07T11:50:05", "latitude": 33.7, "longitude": -118.0, "focal_length": 600.0},
    ]
    encounters = segment_encounters(photos, emit_trace=True)
    assert len(encounters) == 2
    for enc in encounters:
        assert "trace" in enc
        # 2 photos -> 1 internal pair
        assert len(enc["trace"]) == 1
        assert enc["trace"][0]["decision"] == "kept"
```

**Step 2: Run, verify it fails**

```bash
python -m pytest vireo/tests/test_encounters.py::test_segment_encounters_attaches_trace_to_each_encounter -v
```

**Step 3: Modify `segment_encounters`**

Add `emit_trace` param. After getting microsegments and trace, mapping is straightforward: each microsegment `m` at position `j` covers a contiguous range of sorted photos `[start_j, end_j]`. The pairs *inside* it are `[start_j .. end_j - 1]`. Pairs *between* microsegments correspond to cuts.

Then `merge_microsegments` may merge adjacent microsegments — when it merges, the boundary pair (a `cut_*` decision in the trace) gets re-classified as part of the final encounter. We tag it as `"merged_back"` in trace.

Replace the relevant section of `segment_encounters` (`vireo/encounters.py:464-502`):

```python
def segment_encounters(photos, config=None, emit_trace=False):
    if emit_trace:
        microsegments, full_trace = cut_microsegments(photos, config=config, emit_trace=True)
    else:
        microsegments = cut_microsegments(photos, config=config)
        full_trace = None
    log.info("Pass 1: %d microsegments from %d photos", len(microsegments), len(photos))

    # Track which microsegments get merged so we can re-tag boundary trace entries
    if emit_trace:
        merged, merge_map = _merge_microsegments_with_map(microsegments, config=config)
    else:
        merged = merge_microsegments(microsegments, config=config)
        merge_map = None
    log.info("Pass 2: %d encounters after merging", len(merged))

    encounters = []
    pair_cursor = 0  # index into full_trace
    micro_cursor = 0  # index into microsegments

    for enc_idx, seg in enumerate(merged):
        species = encounter_species_label(seg)
        first_ts = _segment_timestamp(seg, "first")
        last_ts = _segment_timestamp(seg, "last")
        enc = {
            "photos": seg,
            "species": species,
            "photo_count": len(seg),
            "time_range": (
                first_ts.isoformat() if first_ts else None,
                last_ts.isoformat() if last_ts else None,
            ),
        }
        if emit_trace:
            # Walk the microsegments that compose this merged encounter
            n_micros = merge_map[enc_idx]
            enc_trace = []
            for _ in range(n_micros):
                m = microsegments[micro_cursor]
                # Internal pairs of this microsegment
                for _ in range(len(m) - 1):
                    enc_trace.append(full_trace[pair_cursor])
                    pair_cursor += 1
                micro_cursor += 1
                # If there's a boundary to the next microsegment AND that next
                # microsegment is also part of this same merged encounter,
                # mark it merged_back.
                if pair_cursor < len(full_trace) and micro_cursor < len(microsegments):
                    boundary = dict(full_trace[pair_cursor])
                    if _ < n_micros - 1:
                        boundary["decision"] = "merged_back"
                        enc_trace.append(boundary)
                        pair_cursor += 1
            enc["trace"] = enc_trace
        encounters.append(enc)

    return encounters


def _merge_microsegments_with_map(segments, config=None):
    """Like merge_microsegments but also returns merge_map: list[int] giving
    the number of original microsegments fused into each final segment.
    """
    cfg = {**DEFAULTS, **(config or {})}
    if len(segments) <= 1:
        return segments, [1] * len(segments)
    merged = [segments[0]]
    counts = [1]
    for seg in segments[1:]:
        last_a = _segment_timestamp(merged[-1], "last")
        first_b = _segment_timestamp(seg, "first")
        gap = _time_delta_seconds(last_a, first_b)
        did_merge = False
        if gap <= cfg["merge_max_gap"]:
            s_seg = compute_s_seg(merged[-1], seg, config=cfg)
            if s_seg > cfg["merge_score"]:
                merged[-1] = merged[-1] + seg
                counts[-1] += 1
                did_merge = True
        if not did_merge:
            merged.append(seg)
            counts.append(1)
    return merged, counts
```

**Step 4: Run target test**

```bash
python -m pytest vireo/tests/test_encounters.py::test_segment_encounters_attaches_trace_to_each_encounter -v
```
Expected: PASS.

**Step 5: Add second test for the merge case**

```python
def test_segment_encounters_marks_merged_back_boundaries():
    """When two microsegments get merged, the boundary pair should be tagged merged_back."""
    from encounters import segment_encounters
    # Construct a sequence that will form 2 microsegments and then merge them.
    # We need: a soft-cut between #3 and #4 (3-of-3 below soft threshold),
    # then a small gap so merge fires.
    # Simplest synthetic: shared lat/lng, focal length identical, no embeddings,
    # tight time spacing so soft-cut is unlikely. Easier path: monkey-patch
    # config so hard_cut_score is very high (everything cuts), then a small
    # merge_score so everything merges back.
    photos = [
        {"timestamp": f"2026-03-07T11:32:0{i}", "latitude": 33.7, "longitude": -118.0, "focal_length": 600.0}
        for i in range(4)
    ]
    cfg = {"hard_cut_score": 1.5, "merge_score": -1.0, "merge_max_gap": 60.0}
    encounters = segment_encounters(photos, config=cfg, emit_trace=True)
    assert len(encounters) == 1  # all merged back
    enc = encounters[0]
    decisions = [t["decision"] for t in enc["trace"]]
    # 3 internal pairs across 4 microsegments: 3 of them are boundaries that got merged_back
    assert decisions.count("merged_back") == 3
```

```bash
python -m pytest vireo/tests/test_encounters.py::test_segment_encounters_marks_merged_back_boundaries -v
```
Expected: PASS.

**Step 6: Run full encounters file**

```bash
python -m pytest vireo/tests/test_encounters.py -v
```
Expected: all PASS.

**Step 7: Commit**

```bash
git add vireo/encounters.py vireo/tests/test_encounters.py
git commit -m "encounters: attach per-pair trace to encounter dicts"
```

---

## Task 4: Pipeline + regroup-live endpoint surface the trace

**Files:**
- Modify: `vireo/pipeline.py` — `run_grouping`, `run_full_pipeline`, `serialize_results`
- Modify: `vireo/app.py:10127-10167` (`api_pipeline_regroup_live`)
- Test: `vireo/tests/test_app.py` (or `test_pipeline.py`)

**Step 1: Find serialize_results**

```bash
grep -n "def serialize_results\|def run_grouping\|def run_full_pipeline" /Users/julius/conductor/workspaces/vireo/tashkent/vireo/pipeline.py
```

Note the line numbers; the implementing engineer should read the surrounding context.

**Step 2: Failing test**

In `vireo/tests/test_app.py`, add (after a similar existing endpoint test):

```python
def test_regroup_live_returns_per_encounter_trace(client_with_pipeline_results):
    """regroup-live response should include encounter['trace'] with cut-point details."""
    client = client_with_pipeline_results
    resp = client.post("/api/pipeline/regroup-live", json={"config": {}})
    assert resp.status_code == 200
    data = resp.get_json()
    assert "encounters" in data
    for enc in data["encounters"]:
        # Single-photo encounter has empty trace; multi-photo has one entry per internal pair
        if enc["photo_count"] >= 2:
            assert "trace" in enc
            assert len(enc["trace"]) == enc["photo_count"] - 1
            sample = enc["trace"][0]
            assert "score" in sample
            assert "decision" in sample
            assert "components" in sample
```

If `client_with_pipeline_results` doesn't exist, look for existing fixtures that prepare a workspace with photos+features (search `test_app.py` for `regroup-live` and copy the fixture pattern; or use the existing `test_pipeline_job.py` fixtures and adapt).

**Step 3: Run test, verify it fails**

```bash
python -m pytest vireo/tests/test_app.py::test_regroup_live_returns_per_encounter_trace -v
```
Expected: FAIL — `trace` not in response.

**Step 4: Thread `emit_trace` through pipeline.py**

Modify `run_grouping`:

```python
def run_grouping(photos, config=None, emit_trace=False):
    from bursts import segment_bursts_for_encounters
    from encounters import segment_encounters
    encounters = segment_encounters(photos, config=config, emit_trace=emit_trace)
    encounters = segment_bursts_for_encounters(encounters, config=config)
    ...
    return encounters
```

Modify `run_full_pipeline` to accept `emit_trace` and forward.

Modify `serialize_results` (find it via grep) to pass through the `trace` field on each encounter dict — likely it's an explicit dict construction, so add `"trace": enc.get("trace")`.

**Step 5: Modify the endpoint to enable trace**

In `vireo/app.py:10155`:

```python
results = run_full_pipeline(photos, config=pipeline_cfg, emit_trace=True)
```

(Always emit; cost is negligible — a few hundred bytes per encounter.)

**Step 6: Run target test**

```bash
python -m pytest vireo/tests/test_app.py::test_regroup_live_returns_per_encounter_trace -v
```
Expected: PASS.

**Step 7: Run the project's full test suite per CLAUDE.md**

```bash
python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py vireo/tests/test_encounters.py -v
```
Expected: all PASS (or only pre-existing failures from `project_preexisting_test_failures` memory).

**Step 8: Commit**

```bash
git add vireo/pipeline.py vireo/app.py vireo/tests/test_app.py
git commit -m "pipeline: surface per-encounter cut-point trace in regroup-live"
```

---

## Task 5: Save-grouping-defaults endpoint

**Files:**
- Modify: `vireo/app.py` — add new route near the regroup-live one (~line 10168)
- Test: `vireo/tests/test_app.py`

**Step 1: Failing test**

```python
def test_save_grouping_defaults_persists_to_config(tmp_path, client):
    import config as cfg
    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    payload = {"pipeline": {"w_species": 0.40, "hard_cut_score": 0.55, "tau_enc": 30.0}}
    resp = client.post("/api/pipeline/save-grouping-defaults", json=payload)
    assert resp.status_code == 200
    saved = cfg.load()
    assert saved["pipeline"]["w_species"] == 0.40
    assert saved["pipeline"]["hard_cut_score"] == 0.55
    assert saved["pipeline"]["tau_enc"] == 30.0
```

(Adapt to the existing `client` fixture pattern in `test_app.py`.)

**Step 2: Run, fail**

```bash
python -m pytest vireo/tests/test_app.py::test_save_grouping_defaults_persists_to_config -v
```

**Step 3: Implement endpoint**

After `api_pipeline_regroup_live` in `vireo/app.py`:

```python
@app.route("/api/pipeline/save-grouping-defaults", methods=["POST"])
def api_pipeline_save_grouping_defaults():
    """Persist current grouping weights to global config."""
    import config as cfg
    body = request.get_json(silent=True) or {}
    new_pipeline = body.get("pipeline", {})
    if not isinstance(new_pipeline, dict):
        return json_error("pipeline must be an object")
    # Whitelist — only allow known grouping keys
    allowed = {
        "w_time", "w_subj", "w_global", "w_species", "w_meta",
        "tau_enc", "hard_cut_time", "hard_cut_score", "soft_cut_score",
        "merge_score", "merge_max_gap", "merge_tau",
        "burst_time_gap", "burst_phash_dist", "burst_emb_dist",
    }
    rejected = [k for k in new_pipeline if k not in allowed]
    if rejected:
        return json_error(f"unknown keys: {rejected}")
    raw = cfg.load()
    raw.setdefault("pipeline", {}).update(new_pipeline)
    cfg.save(raw)
    return jsonify({"saved": new_pipeline})
```

**Step 4: Run, pass**

```bash
python -m pytest vireo/tests/test_app.py::test_save_grouping_defaults_persists_to_config -v
```

**Step 5: Commit**

```bash
git add vireo/app.py vireo/tests/test_app.py
git commit -m "pipeline: endpoint to persist grouping defaults to config"
```

---

## Task 6: Sidebar — full weight panel, always visible

**Files:**
- Modify: `vireo/templates/pipeline_review.html:940-979` (sidebar grouping section)
- Modify: `vireo/templates/pipeline_review.html` — `getGroupingConfig()` (search for it; should be near `onGroupingChange`)
- Modify: `vireo/templates/pipeline_review.html` — `applyGroupingDefaults()` (search; resets sliders)

**Step 1: Open the sidebar always (remove the `display:none`)**

Change line 940 from:
```html
<div class="sidebar-section" id="sidebarGrouping" style="display:none">
```
to:
```html
<div class="sidebar-section" id="sidebarGrouping">
```

**Step 2: Replace the contents with the expanded slider set**

Replace lines 941-978 with three subsections. Use the same slider HTML shape as existing rows.

```html
<div class="sidebar-title">Grouping Algorithm</div>

<div class="slider-section-header">Encounter cut — signal weights</div>
<div class="slider-section-hint">How much each signal contributes to the similarity score between adjacent photos. Re-normalized over signals that have data.</div>
<div class="slider-group">
  <div class="slider-row">
    <span class="slider-label" title="Weight of time-proximity in S_enc">Time</span>
    <input type="range" min="0" max="100" value="35" step="1" id="slWTime" oninput="onGroupingChange(this)">
    <span class="slider-val" id="valWTime">0.35</span>
  </div>
  <div class="slider-row">
    <span class="slider-label" title="Weight of DINO subject embedding">Subject embed</span>
    <input type="range" min="0" max="100" value="35" step="1" id="slWSubj" oninput="onGroupingChange(this)">
    <span class="slider-val" id="valWSubj">0.35</span>
  </div>
  <div class="slider-row">
    <span class="slider-label" title="Weight of DINO global embedding">Global embed</span>
    <input type="range" min="0" max="100" value="15" step="1" id="slWGlobal" oninput="onGroupingChange(this)">
    <span class="slider-val" id="valWGlobal">0.15</span>
  </div>
  <div class="slider-row">
    <span class="slider-label" title="Weight of per-photo species agreement (Bhattacharyya overlap of top-5)">Species</span>
    <input type="range" min="0" max="100" value="10" step="1" id="slWSpecies" oninput="onGroupingChange(this)">
    <span class="slider-val" id="valWSpecies">0.10</span>
  </div>
  <div class="slider-row">
    <span class="slider-label" title="Weight of GPS + focal length similarity">Meta (GPS+FL)</span>
    <input type="range" min="0" max="100" value="5" step="1" id="slWMeta" oninput="onGroupingChange(this)">
    <span class="slider-val" id="valWMeta">0.05</span>
  </div>
</div>

<div class="slider-section-header">Encounter cut — thresholds</div>
<div class="slider-group">
  <div class="slider-row">
    <span class="slider-label" title="Time constant for time-similarity decay (seconds). Lower = closer-in-time pairs needed.">τ time (s)</span>
    <input type="range" min="5" max="120" value="40" step="1" id="slTauEnc" oninput="onGroupingChange(this)">
    <span class="slider-val" id="valTauEnc">40</span>
  </div>
  <div class="slider-row">
    <span class="slider-label" title="Force a cut if the gap between two photos exceeds this (seconds)">Hard cut: time</span>
    <input type="range" min="30" max="600" value="180" step="10" id="slHardCutTime" oninput="onGroupingChange(this)">
    <span class="slider-val" id="valHardCutTime">180</span>
  </div>
  <div class="slider-row">
    <span class="slider-label" title="Force a cut if S_enc drops below this">Hard cut: score</span>
    <input type="range" min="0" max="100" value="42" step="1" id="slEncCut" oninput="onGroupingChange(this)">
    <span class="slider-val" id="valEncCut">0.42</span>
  </div>
  <div class="slider-row">
    <span class="slider-label" title="Soft-cut threshold: 2 of last 3 scores below this triggers a cut">Soft cut: score</span>
    <input type="range" min="0" max="100" value="52" step="1" id="slSoftCut" oninput="onGroupingChange(this)">
    <span class="slider-val" id="valSoftCut">0.52</span>
  </div>
</div>

<div class="slider-section-header">Encounter merge</div>
<div class="slider-section-hint">Stitch adjacent microsegments back together if they look like the same encounter</div>
<div class="slider-group">
  <div class="slider-row">
    <span class="slider-label" title="Merge if S_seg between segments exceeds this">Merge score</span>
    <input type="range" min="0" max="100" value="62" step="1" id="slEncMerge" oninput="onGroupingChange(this)">
    <span class="slider-val" id="valEncMerge">0.62</span>
  </div>
  <div class="slider-row">
    <span class="slider-label" title="Only consider merge if time gap ≤ this (seconds)">Max gap (s)</span>
    <input type="range" min="5" max="300" value="60" step="5" id="slMergeMaxGap" oninput="onGroupingChange(this)">
    <span class="slider-val" id="valMergeMaxGap">60</span>
  </div>
  <div class="slider-row">
    <span class="slider-label" title="Time decay constant for merge gap scoring">τ merge (s)</span>
    <input type="range" min="5" max="120" value="20" step="1" id="slMergeTau" oninput="onGroupingChange(this)">
    <span class="slider-val" id="valMergeTau">20</span>
  </div>
</div>

<div class="slider-section-header">Bursts</div>
<div class="slider-section-hint">Group rapid-fire shots within an encounter</div>
<div class="slider-group">
  <div class="slider-row">
    <span class="slider-label" title="Max seconds between consecutive shots to stay in the same burst">Time gap (s)</span>
    <input type="range" min="1" max="30" value="3" step="1" id="slBurstTime" oninput="onGroupingChange(this)">
    <span class="slider-val" id="valBurstTime">3</span>
  </div>
  <div class="slider-row">
    <span class="slider-label" title="Max perceptual hash distance — lower means more visually similar">pHash dist</span>
    <input type="range" min="1" max="32" value="12" step="1" id="slBurstPhash" oninput="onGroupingChange(this)">
    <span class="slider-val" id="valBurstPhash">12</span>
  </div>
  <div class="slider-row">
    <span class="slider-label" title="Max embedding distance — higher tolerates more visually different shots">Emb. distance</span>
    <input type="range" min="0" max="100" value="20" step="1" id="slBurstEmb" oninput="onGroupingChange(this)">
    <span class="slider-val" id="valBurstEmb">0.20</span>
  </div>
</div>

<div class="button-row">
  <button class="reset-btn" onclick="resetGroupingDefaults()">Reset</button>
  <button class="save-btn" onclick="saveGroupingDefaults()">Save as default</button>
</div>
```

**Step 3: Update `getGroupingConfig`** (search for it in the same file)

It currently returns the 5 existing sliders' values. Extend to include all 12. Each should map to its `pipeline.*` config key. The format the backend expects (see `regroup-live` and Task 5's whitelist):

```js
function getGroupingConfig() {
  function pct(id) { return parseFloat(document.getElementById(id).value) / 100.0; }
  function num(id) { return parseFloat(document.getElementById(id).value); }
  return {
    w_time: pct('slWTime'),
    w_subj: pct('slWSubj'),
    w_global: pct('slWGlobal'),
    w_species: pct('slWSpecies'),
    w_meta: pct('slWMeta'),
    tau_enc: num('slTauEnc'),
    hard_cut_time: num('slHardCutTime'),
    hard_cut_score: pct('slEncCut'),
    soft_cut_score: pct('slSoftCut'),
    merge_score: pct('slEncMerge'),
    merge_max_gap: num('slMergeMaxGap'),
    merge_tau: num('slMergeTau'),
    burst_time_gap: num('slBurstTime'),
    burst_phash_dist: num('slBurstPhash'),
    burst_emb_dist: pct('slBurstEmb'),
  };
}
```

**Step 4: Update `updateSliderDisplay`** (search) so each new slider shows the formatted value (`0.35`, `40`, etc.) — follow the existing pattern.

**Step 5: Update `applyGroupingDefaults`** (search) to reset all 12 sliders to the defaults from `vireo/encounters.py:21-38`.

**Step 6: Add `saveGroupingDefaults` function**

```js
function saveGroupingDefaults() {
  var config = getGroupingConfig();
  safeFetch('/api/pipeline/save-grouping-defaults', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({pipeline: config}),
  }, { toast: 'Saved as defaults' });
}
```

**Step 7: Manual smoke test**

```bash
python vireo/app.py --db ~/.vireo/vireo.db --port 8080
```

Open `http://localhost:8080/pipeline-review`. Verify:
- Left sidebar shows all 12 sliders organized into 4 subsections.
- Dragging any slider triggers a regroup (encounter cards re-render after ~500ms debounce).
- "Save as default" button shows a toast.

**Step 8: Commit**

```bash
git add vireo/templates/pipeline_review.html
git commit -m "pipeline-review: full grouping algorithm sliders in sidebar"
```

---

## Task 7: Algorithm trace panel — display focused encounter's cut-point readout

**Files:**
- Modify: `vireo/templates/pipeline_review.html` — add trace panel + focus selection
- Add CSS for the trace rows

**Step 1: Add the trace panel HTML to the sidebar**

Insert after the Bursts section, before the buttons:

```html
<div class="slider-section-header">Trace — focused encounter</div>
<div class="slider-section-hint" id="traceFocusLabel">Click an encounter to see how it was formed</div>
<div id="algorithmTrace" class="algo-trace"></div>
```

**Step 2: Add CSS** (alongside existing sidebar styles)

```css
.algo-trace { font-size: 11px; line-height: 1.35; }
.algo-trace .trace-row {
  display: grid; grid-template-columns: 1fr auto auto; gap: 6px;
  padding: 3px 4px; border-bottom: 1px solid var(--border-subtle);
}
.algo-trace .trace-row.cut { background: rgba(220, 50, 50, 0.08); }
.algo-trace .trace-row.merged-back { background: rgba(80, 160, 80, 0.08); }
.algo-trace .trace-row.kept { }
.algo-trace .trace-decision { font-weight: 600; }
.algo-trace .trace-components {
  font-size: 10px; color: var(--muted); padding: 2px 4px;
}
.algo-trace .trace-bar {
  height: 3px; background: var(--accent); border-radius: 2px;
}
```

**Step 3: Add focus + render functions**

```js
var _focusedEncounterIdx = null;

function setFocusedEncounter(idx) {
  _focusedEncounterIdx = idx;
  // Highlight the focused card
  document.querySelectorAll('.encounter-card.focused').forEach(function(el) {
    el.classList.remove('focused');
  });
  var card = document.querySelector('.encounter-card[data-encounter-index="' + idx + '"]');
  if (card) card.classList.add('focused');
  renderAlgorithmTrace();
}

function renderAlgorithmTrace() {
  var panel = document.getElementById('algorithmTrace');
  var label = document.getElementById('traceFocusLabel');
  if (!panel) return;
  if (_focusedEncounterIdx === null || !pipelineResults) {
    panel.innerHTML = '';
    label.textContent = 'Click an encounter to see how it was formed';
    return;
  }
  var enc = pipelineResults.encounters[_focusedEncounterIdx];
  if (!enc || !enc.trace || enc.trace.length === 0) {
    panel.innerHTML = '<div class="trace-empty">Single-photo encounter — no internal cut points.</div>';
    label.textContent = 'Encounter ' + (_focusedEncounterIdx + 1);
    return;
  }
  label.textContent = 'Encounter ' + (_focusedEncounterIdx + 1) + ' — ' + enc.trace.length + ' adjacent pair' + (enc.trace.length > 1 ? 's' : '');
  var html = '';
  enc.trace.forEach(function(t, i) {
    var rowCls = 'trace-row';
    if (t.decision && t.decision.indexOf('cut_') === 0) rowCls += ' cut';
    else if (t.decision === 'merged_back') rowCls += ' merged-back';
    else rowCls += ' kept';
    var dt = (t.dt_seconds === null) ? '∞' : t.dt_seconds.toFixed(1) + 's';
    html += '<div class="' + rowCls + '">';
    html += '<span>pair ' + (i + 1) + ' · ' + dt + '</span>';
    html += '<span>S=' + t.score.toFixed(3) + '</span>';
    html += '<span class="trace-decision">' + t.decision + '</span>';
    html += '</div>';
    // Component breakdown
    if (t.components) {
      var parts = [];
      ['time','subj','global','species','meta'].forEach(function(k) {
        var c = t.components[k];
        if (!c) return;
        var marker = c.used ? '' : '·'; // · means signal not available
        parts.push(k + marker + '=' + c.value.toFixed(2) + '×' + c.weight.toFixed(2));
      });
      html += '<div class="trace-components">' + parts.join(' · ') + '</div>';
    }
  });
  panel.innerHTML = html;
}
```

**Step 4: Wire encounter cards to set focus on click**

Find the encounter-card render function (search for `encounter-card` in the template's JS). Add `data-encounter-index="' + i + '"` to each card. Add an onclick handler — but be careful not to swallow existing photo-click handlers; use a header click region:

```js
// In the encounter card render: add to the .encounter-meta or .encounter-header element
'<div class="encounter-header" onclick="setFocusedEncounter(' + i + ')">' + ...existing header content... + '</div>'
```

Add a CSS rule so `.encounter-card.focused` is visually distinct (e.g., `border-left: 3px solid var(--accent);`).

**Step 5: Auto-focus on initial render**

In `renderResults()` (search), after rendering, if `_focusedEncounterIdx === null && pipelineResults.encounters.length > 0`, call `setFocusedEncounter(0)`. After regroup, if the previously focused index is out of range, fall back to 0.

**Step 6: Manual UI verification with Playwright**

This is required per `feedback_user_first_testing` memory.

```bash
python vireo/app.py --db ~/.vireo/vireo.db --port 8080
```

In a separate terminal, drive Playwright:

```python
# scratch script
from playwright.sync_api import sync_playwright
with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    page = browser.new_page()
    page.goto("http://localhost:8080/pipeline-review")
    # Wait for results
    page.wait_for_selector(".encounter-card", timeout=30000)
    # Click the second encounter card
    page.locator(".encounter-card").nth(1).click()
    # Verify trace panel updated
    trace_text = page.locator("#algorithmTrace").inner_text()
    print(trace_text[:500])
    # Drag a slider — programmatically set value, dispatch input event
    page.evaluate("""() => {
      const sl = document.getElementById('slWSpecies');
      sl.value = '40';
      sl.dispatchEvent(new Event('input'));
    }""")
    # Wait for regroup
    page.wait_for_timeout(1500)
    # Verify the encounter list re-rendered (count probably changed)
    new_count = page.locator(".encounter-card").count()
    print("encounters after w_species=0.40:", new_count)
    browser.close()
```

Hand-verify: focused card has visible left-border highlight. Trace panel shows per-pair rows with decisions. After raising w_species, the encounter that contained mixed Lesser Scaup + Ruddy Duck splits into two.

**Step 7: Commit**

```bash
git add vireo/templates/pipeline_review.html
git commit -m "pipeline-review: algorithm trace panel for focused encounter"
```

---

## Task 8: Final verification + PR

**Step 1: Full test run**

```bash
python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py vireo/tests/test_encounters.py -v
```
Note any failures against `project_preexisting_test_failures` memory; only investigate ones you introduced.

**Step 2: Force-add the plan doc** (per `project_plan_docs_force_add` memory)

```bash
git add -f docs/plans/2026-05-03-transparent-encounter-tuning-plan.md
git commit -m "docs: transparent encounter tuning plan"
```

**Step 3: Push and open PR**

```bash
git push -u origin transparent-encounter-tuning
gh pr create --base main --title "Transparent encounter tuning" --body "$(cat <<'EOF'
## Summary

Surfaces the encounter-grouping algorithm in the pipeline review page's left sidebar:

- All 12 grouping knobs (5 weights, 4 cut thresholds, 3 merge knobs) are sliders, always visible, organized by pipeline stage.
- New "Trace" panel shows per-cut-point S_enc + component breakdown for the focused encounter — click an encounter to focus it.
- Slider drags trigger a debounced live regroup (existing `regroup-live` endpoint).
- "Save as default" button persists weights to `~/.vireo/config.json`.

Backend changes are additive: `compute_s_enc(... return_components=True)` and `segment_encounters(... emit_trace=True)` are opt-in; default behavior unchanged.

## Test plan

- [x] `vireo/tests/test_encounters.py` — component breakdown, per-pair trace, merged-back tagging
- [x] `vireo/tests/test_app.py` — regroup-live response includes trace, save-defaults persists
- [x] Manual: open pipeline-review, drag w_species slider from 0.10 to 0.40, observe the 11-photo Bolsa Chica encounter split into two by species
- [x] Manual: click each encounter card → trace panel updates with per-pair scores

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Notes on tradeoffs and pitfalls

- **Trace serialization size.** Each pair entry is ~300 bytes. A workspace with 5000 photos and ~200 encounters of avg 25 photos = ~24 pairs/encounter × 200 = 4800 entries × 300B = ~1.5 MB extra in the regroup-live response. Acceptable. If it ever becomes a concern, add a `?include_trace=0` flag.
- **Soft-cut tagging and merge_back.** The trace decision is the *first* decision applied. A pair that was soft-cut and then merged-back should show `merged_back` (post-merge truth). The implementation in Task 3 handles this by overwriting the decision when the merge map says so.
- **Focus persistence across regroups.** The encounter list shape changes after regroup. Falling back to index 0 is fine for a first cut; if it feels jarring, a follow-up could try to keep focus on the encounter that contains the previously focused encounter's first photo.
- **YAGNI guardrail.** Resist adding "advanced mode" toggle, presets, A/B comparison, or persisted per-workspace overrides. The user wants radical transparency, not yet a pro-mode UI.
