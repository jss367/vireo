# Eye-Focus Detection Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a per-photo eye-focus signal that replaces body-level sharpness in `quality_composite` when an eye is confidently localized, with a parallel `reject_eye_soft` hard-reject rule. See `docs/plans/2026-04-16-eye-focus-detection-design.md` for validated design.

**Architecture:** Two new ONNX keypoint models (SuperAnimal-Quadruped, SuperAnimal-Bird) taxonomy-routed from `species_top5[0]`. New pipeline stage between masking and scoring. Four new nullable columns on `photos` (`eye_x`, `eye_y`, `eye_conf`, `eye_tenengrad`) — all raw values, normalized `eye_focus_score` computed ephemerally at scoring time (mirrors how `subject_tenengrad` / `focus_score` work today). Ships RTMPose-animal first as an integration spike to de-risk the downstream pipeline before touching SuperAnimal export.

**Tech Stack:** Python 3, SQLite (WAL), ONNX Runtime, Flask, Jinja2, vanilla JS. Models: `mmpose` + `mmdeploy` (RTMPose export), `deeplabcut` (SuperAnimal export), `onnxruntime` (inference). Testing: `pytest`, Playwright for UI.

---

## Important Context for the Implementer

**Workflow rules (non-negotiable, from `CLAUDE.md`):**
- You are in a worktree (`.worktrees/eye-focus-detection`) on branch `claude/eye-focus-detection`. Do **not** modify files outside this worktree.
- `docs/plans/` is gitignored. Commit plan/design docs with `git add -f`.
- Commit after each task passes. Never skip `--no-verify`. Never amend.
- Before creating a PR, run:
  ```bash
  python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py \
    vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py \
    vireo/tests/test_darktable_api.py vireo/tests/test_config.py -v
  ```

**Patterns to mirror (these already exist — read them before writing similar code):**
- ONNX model loading, download, and session cache: `vireo/detector.py` (MegaDetector), `vireo/masking.py` (SAM2), `vireo/dino_embed.py` (DINOv2). Use the same `_session` / `_lock` / `_download_lock` / `ensure_*_weights` structure.
- Multi-scale Tenengrad and mask-region sharpness: `vireo/quality.py:53-138`. The helpers `_tenengrad` and `_multiscale_tenengrad` are reusable.
- DB migration and ALTER TABLE pattern: `vireo/db.py:347` for `subject_tenengrad`. Match.
- Pipeline stages and progress reporting: `vireo/pipeline.py`. Each stage is a function that iterates photos and reports progress via the `JobRunner` callback.
- ONNX export script pattern: `scripts/export_onnx.py`. Every model has a `_ModelWrapper` class + `export_*()` function + entry in `_EXPORT_FUNCTIONS` and `ALL_MODELS`.
- Pipeline UI cards for model-download status: `vireo/templates/pipeline.html` "Extract Features" card (SAM2/DINOv2 download status, from commit `e3eba78`).
- Settings UI for tunable thresholds: `vireo/templates/settings.html`.

**Test conventions:**
- DB tests use tmp_path and a fresh `Database(tmp_path/"x.db")`.
- App tests isolate config via `cfg.CONFIG_PATH = str(tmp_path/"config.json")`.
- Keypoint-model inference is mocked at the `onnxruntime.InferenceSession.run` level — do not require weights in CI tests.
- End-to-end tests involving real model weights are dev-only and live under `tests/e2e/`, skipped when weights are absent.

---

## Milestone 1: Database Schema

Add four nullable columns to `photos`. Migration style matches the existing `subject_tenengrad` ALTER at `vireo/db.py:347`.

### Task 1.1: Write failing migration test

**Files:**
- Modify test: `vireo/tests/test_db.py`

**Step 1: Add test**

```python
def test_photos_has_eye_focus_columns(tmp_path):
    db = Database(str(tmp_path / "x.db"))
    cols = {row[1] for row in db.conn.execute("PRAGMA table_info(photos)")}
    assert "eye_x" in cols
    assert "eye_y" in cols
    assert "eye_conf" in cols
    assert "eye_tenengrad" in cols
```

**Step 2: Run test, expect FAIL**

```
python -m pytest vireo/tests/test_db.py::test_photos_has_eye_focus_columns -v
```

Expected output: `FAILED` because the columns don't exist.

### Task 1.2: Implement migration

**Files:**
- Modify: `vireo/db.py` — add ALTER TABLE statements alongside the existing `subject_tenengrad` migration at line ~347.

**Step 1: Add to the migration block**

Find the block containing `ALTER TABLE photos ADD COLUMN subject_tenengrad REAL`. Add four adjacent migrations, each wrapped in the same try/except OperationalError pattern used for existing columns:

```python
try:
    self.conn.execute("ALTER TABLE photos ADD COLUMN eye_x REAL")
except sqlite3.OperationalError:
    pass
try:
    self.conn.execute("ALTER TABLE photos ADD COLUMN eye_y REAL")
except sqlite3.OperationalError:
    pass
try:
    self.conn.execute("ALTER TABLE photos ADD COLUMN eye_conf REAL")
except sqlite3.OperationalError:
    pass
try:
    self.conn.execute("ALTER TABLE photos ADD COLUMN eye_tenengrad REAL")
except sqlite3.OperationalError:
    pass
```

**Step 2: Run test, expect PASS**

```
python -m pytest vireo/tests/test_db.py::test_photos_has_eye_focus_columns -v
```

**Step 3: Commit**

```bash
git add vireo/db.py vireo/tests/test_db.py
git commit -m "db: add eye_x, eye_y, eye_conf, eye_tenengrad columns to photos"
```

### Task 1.3: Write CRUD test

**Files:**
- Modify test: `vireo/tests/test_db.py`

**Step 1: Add test**

```python
def test_update_photo_eye_fields_roundtrip(tmp_path, sample_photo_row):
    db = Database(str(tmp_path / "x.db"))
    photo_id = db.insert_photo(sample_photo_row)
    db.update_photo(photo_id, eye_x=123.4, eye_y=56.7, eye_conf=0.82, eye_tenengrad=18450.2)
    row = db.conn.execute(
        "SELECT eye_x, eye_y, eye_conf, eye_tenengrad FROM photos WHERE id=?",
        (photo_id,),
    ).fetchone()
    assert row == (123.4, 56.7, 0.82, 18450.2)

def test_update_photo_eye_fields_accept_null(tmp_path, sample_photo_row):
    db = Database(str(tmp_path / "x.db"))
    photo_id = db.insert_photo(sample_photo_row)
    db.update_photo(photo_id, eye_x=None, eye_y=None, eye_conf=None, eye_tenengrad=None)
    row = db.conn.execute(
        "SELECT eye_x, eye_y, eye_conf, eye_tenengrad FROM photos WHERE id=?",
        (photo_id,),
    ).fetchone()
    assert row == (None, None, None, None)
```

If `sample_photo_row` fixture does not exist, add a conftest fixture that returns a minimal row dict matching the `photos` schema — copy the shape from an existing photo-insert test in the same file.

**Step 2: Run tests, expect FAIL**

Tests fail because `update_photo` doesn't accept these keyword arguments (the method uses a `_UNSET` sentinel pattern — see `db.py:2562`).

### Task 1.4: Implement CRUD

**Files:**
- Modify: `vireo/db.py` — extend `update_photo` signature and SQL around line 2562–2580.

**Step 1: Add params and SQL fragments**

Add `eye_x=_UNSET`, `eye_y=_UNSET`, `eye_conf=_UNSET`, `eye_tenengrad=_UNSET` to the `update_photo` signature. Add matching entries to the dict that drives SQL generation:

```python
"eye_x": eye_x,
"eye_y": eye_y,
"eye_conf": eye_conf,
"eye_tenengrad": eye_tenengrad,
```

**Step 2: Run tests, expect PASS**

```
python -m pytest vireo/tests/test_db.py::test_update_photo_eye_fields_roundtrip \
                 vireo/tests/test_db.py::test_update_photo_eye_fields_accept_null -v
```

**Step 3: Commit**

```bash
git add vireo/db.py vireo/tests/test_db.py
git commit -m "db: accept eye_* fields in update_photo"
```

---

## Milestone 2: Taxonomy Routing Helper

Pure function: given a top-1 iNat taxon ID, return `'Aves'`, `'Mammalia'`, or `None` based on the lineage stored in the `taxa` table.

### Task 2.1: Write failing test

**Files:**
- Modify test: `vireo/tests/test_taxonomy.py`

**Step 1: Add test**

```python
def test_classify_to_keypoint_group_bird(tmp_path):
    db = Database(str(tmp_path / "x.db"))
    # Insert a minimal Aves lineage: Aves class → Passeriformes order → ... → species
    db.conn.execute("INSERT INTO taxa (inat_id, name, rank, kingdom) VALUES (3, 'Aves', 'class', 'Animalia')")
    db.conn.execute("INSERT INTO taxa (inat_id, name, rank, kingdom, parent_id) VALUES (7019, 'Passeriformes', 'order', 'Animalia', 3)")
    db.conn.execute("INSERT INTO taxa (inat_id, name, rank, kingdom, parent_id) VALUES (12345, 'Cardinalis cardinalis', 'species', 'Animalia', 7019)")
    db.conn.commit()
    assert classify_to_keypoint_group(db, 12345) == "Aves"

def test_classify_to_keypoint_group_mammal(tmp_path):
    db = Database(str(tmp_path / "x.db"))
    db.conn.execute("INSERT INTO taxa (inat_id, name, rank, kingdom) VALUES (40151, 'Mammalia', 'class', 'Animalia')")
    db.conn.execute("INSERT INTO taxa (inat_id, name, rank, kingdom, parent_id) VALUES (42158, 'Carnivora', 'order', 'Animalia', 40151)")
    db.conn.execute("INSERT INTO taxa (inat_id, name, rank, kingdom, parent_id) VALUES (42048, 'Vulpes vulpes', 'species', 'Animalia', 42158)")
    db.conn.commit()
    assert classify_to_keypoint_group(db, 42048) == "Mammalia"

def test_classify_to_keypoint_group_fish_returns_none(tmp_path):
    db = Database(str(tmp_path / "x.db"))
    db.conn.execute("INSERT INTO taxa (inat_id, name, rank, kingdom) VALUES (47178, 'Actinopterygii', 'class', 'Animalia')")
    db.conn.execute("INSERT INTO taxa (inat_id, name, rank, kingdom, parent_id) VALUES (47179, 'Perciformes', 'order', 'Animalia', 47178)")
    db.conn.execute("INSERT INTO taxa (inat_id, name, rank, kingdom, parent_id) VALUES (99999, 'Somefish somefish', 'species', 'Animalia', 47179)")
    db.conn.commit()
    assert classify_to_keypoint_group(db, 99999) is None

def test_classify_to_keypoint_group_unknown_returns_none(tmp_path):
    db = Database(str(tmp_path / "x.db"))
    assert classify_to_keypoint_group(db, 999999) is None
```

Inspect `vireo/taxonomy.py` to confirm the actual schema of the `taxa` table (column names for parent, rank). If the table stores `ancestry_ids` as a path instead of `parent_id`, adjust the test fixtures accordingly and the implementation below.

**Step 2: Run, expect FAIL**

### Task 2.2: Implement helper

**Files:**
- Modify: `vireo/taxonomy.py`

**Step 1: Add function**

```python
def classify_to_keypoint_group(db, inat_id):
    """Walk taxon lineage; return 'Aves' or 'Mammalia' if in ancestry, else None.

    Used to route keypoint-model selection. Returns None for fish, reptiles,
    insects, invertebrates, or if the taxon is absent from the taxonomy.
    """
    if inat_id is None:
        return None
    seen = set()
    current = inat_id
    while current is not None and current not in seen:
        seen.add(current)
        row = db.conn.execute(
            "SELECT name, rank, parent_id FROM taxa WHERE inat_id=?",
            (current,),
        ).fetchone()
        if row is None:
            return None
        name, rank, parent_id = row
        if rank == "class" and name in ("Aves", "Mammalia"):
            return name
        current = parent_id
    return None
```

Adjust the column names if the actual schema uses `ancestry_ids` or a different parent field — the test fixtures above and this implementation must match whatever `load_taxa_from_file` actually writes.

**Step 2: Run, expect PASS**

```
python -m pytest vireo/tests/test_taxonomy.py -v -k classify_to_keypoint_group
```

**Step 3: Commit**

```bash
git add vireo/taxonomy.py vireo/tests/test_taxonomy.py
git commit -m "taxonomy: add classify_to_keypoint_group helper"
```

---

## Milestone 3: RTMPose-animal ONNX Export (Integration Spike)

This is a dev-only operation in `scripts/export_onnx.py`. It produces the first working ONNX keypoint model for Vireo. End-to-end validation is manual — the CI tests in later milestones use mocked ONNX sessions.

### Task 3.1: Add RTMPose export function

**Files:**
- Modify: `scripts/export_onnx.py`

**Step 1: Add the export function**

Add after the SAM2 section:

```python
# ---------------------------------------------------------------------------
# RTMPose-animal (MMPose, AP-10K keypoints)
# ---------------------------------------------------------------------------

def export_rtmpose_animal(output_dir, opset, validate=False):
    """Export RTMPose-s trained on AP-10K to ONNX via mmdeploy.

    RTMPose returns heatmaps for 17 keypoints including left_eye (idx 0)
    and right_eye (idx 1). Top-down: takes a cropped image, returns
    per-keypoint (x, y, conf) after heatmap decoding (done at inference
    time, not inside the ONNX graph).

    Input:  (1, 3, 256, 256) float32
    Output: (1, 17, 64, 64) float32 heatmaps (simcc variant may differ)
    """
    import mmpose
    from mmpose.apis import init_model
    from mmdeploy.apis import torch2onnx

    model_id = "rtmpose-animal"
    log.info("Exporting %s...", model_id)

    # Use the official RTMPose-s AP-10K config + checkpoint
    config = (
        "https://raw.githubusercontent.com/open-mmlab/mmpose/main/"
        "configs/animal_2d_keypoint/rtmpose/ap10k/"
        "rtmpose-s_8xb64-210e_ap10k-256x256.py"
    )
    checkpoint = (
        "https://download.openmmlab.com/mmpose/v1/projects/rtmposev1/"
        "rtmpose-s_simcc-ap10k_pt-aic-coco_210e-256x256-7a041aa1_20230206.pth"
    )

    out_dir = _ensure_dir(os.path.join(output_dir, model_id))
    onnx_path = os.path.join(out_dir, "model.onnx")

    # Use mmdeploy's torch2onnx helper — it handles the simcc head correctly
    # (RTMPose uses coordinate classification, not pure heatmaps).
    deploy_cfg = {
        "onnx_config": {
            "type": "onnx",
            "export_params": True,
            "keep_initializers_as_inputs": False,
            "opset_version": opset,
            "save_file": onnx_path,
            "input_names": ["pixel_values"],
            "output_names": ["simcc_x", "simcc_y"],
            "input_shape": [256, 256],
        },
        "codebase_config": {"type": "mmpose", "task": "PoseDetection"},
        "backend_config": {"type": "onnxruntime"},
    }

    # Download a sample image to use as calibration input
    import urllib.request
    sample_path = os.path.join(out_dir, "_sample.jpg")
    urllib.request.urlretrieve(
        "https://raw.githubusercontent.com/open-mmlab/mmpose/main/tests/data/ap10k/000000000017.jpg",
        sample_path,
    )

    torch2onnx(
        sample_path,
        out_dir,
        "model",
        deploy_cfg=deploy_cfg,
        model_cfg=config,
        model_checkpoint=checkpoint,
        device="cpu",
    )

    config_out = {
        "input_size": [1, 3, 256, 256],
        "mean": [123.675, 116.28, 103.53],
        "std": [58.395, 57.12, 57.375],
        "keypoints": [  # AP-10K keypoint order
            "left_eye", "right_eye", "nose", "neck", "root_of_tail",
            "left_shoulder", "left_elbow", "left_front_paw",
            "right_shoulder", "right_elbow", "right_front_paw",
            "left_hip", "left_knee", "left_back_paw",
            "right_hip", "right_knee", "right_back_paw",
        ],
        "output_type": "simcc",  # RTMPose uses simcc, not heatmaps
    }
    _save_json(os.path.join(out_dir, "config.json"), config_out)

    if os.path.exists(sample_path):
        os.remove(sample_path)

    log.info("  %s export complete: %s", model_id, out_dir)
    return onnx_path
```

**Step 2: Register in ALL_MODELS and dispatcher**

```python
ALL_MODELS = [
    # ... existing ...
    "rtmpose-animal",
]

_EXPORT_FUNCTIONS = {
    # ... existing ...
    "rtmpose-animal": export_rtmpose_animal,
}
```

Add the help text entry in `main()`'s epilog string.

**Step 3: Add `[export]` extras if needed**

Check `pyproject.toml`'s `[export]` optional-dependencies group. Add `mmpose`, `mmdeploy`, `mmcv` with appropriate version pins if not already present. RTMPose requires `mmcv>=2.0.0` and `mmengine`.

**Step 4: Commit**

```bash
git add scripts/export_onnx.py pyproject.toml
git commit -m "export: add RTMPose-animal ONNX export"
```

### Task 3.2: Run the export manually and validate

**Step 1:** Install export extras in a throwaway venv:

```bash
pip install -e ".[export]"
```

**Step 2:** Run:

```bash
python scripts/export_onnx.py --model rtmpose-animal --output-dir /tmp/vireo-export
```

**Step 3:** Verify output:

```bash
ls /tmp/vireo-export/rtmpose-animal/
# Expect: model.onnx, config.json
```

**Step 4:** Manual sanity check — load the ONNX model and a real animal photo, decode keypoints, verify the eye coord lands on the eye visually. Save a Python scratch script under `scripts/validate_rtmpose.py` for this (gitignored, not committed).

**Step 5:** Upload to HF if validation passes:

```bash
huggingface-cli upload jss367/vireo-onnx-models /tmp/vireo-export/rtmpose-animal rtmpose-animal --repo-type model
```

No commit — output artifacts don't get checked in.

---

## Milestone 4: `vireo/keypoints.py` — Loading, Running, Decoding

New module. Follows the `detector.py` shape: `ensure_*_weights`, `_load_session`, `detect_keypoints`. Two functions because we have two (eventually three) models.

### Task 4.1: Write failing test for heatmap/simcc decoding

**Files:**
- Create test: `vireo/tests/test_keypoints.py`

**Step 1: Add decoding test**

```python
import numpy as np
from vireo.keypoints import decode_simcc

def test_decode_simcc_picks_argmax():
    # simcc_x, simcc_y shape: (1, K, size*2) where size is output spatial dim
    K = 17
    simcc_x = np.zeros((1, K, 512), dtype=np.float32)
    simcc_y = np.zeros((1, K, 512), dtype=np.float32)
    # Eye 0 at (100, 50), input image is 256x256 (simcc splat factor = 2.0)
    simcc_x[0, 0, 200] = 1.0  # x=200/2 = 100 px
    simcc_y[0, 0, 100] = 1.0  # y=100/2 = 50 px
    kps = decode_simcc(simcc_x, simcc_y, input_size=256, simcc_split_ratio=2.0)
    # kps shape: (K, 3) — x, y, conf
    assert kps.shape == (K, 3)
    np.testing.assert_allclose(kps[0, :2], [100.0, 50.0], atol=0.5)
    assert kps[0, 2] == 1.0  # max confidence
```

**Step 2: Run, expect FAIL** (`ImportError: vireo.keypoints`).

### Task 4.2: Implement decoding

**Files:**
- Create: `vireo/keypoints.py`

**Step 1: Write module skeleton**

```python
"""Animal keypoint detection via ONNX Runtime.

Top-down keypoint models take a cropped image (MegaDetector bbox) and return
per-keypoint (x, y, conf) in image-pixel coordinates. Used to localize the
animal's eye for eye-focus scoring.

Models:
    rtmpose-animal       — AP-10K, integration spike
    superanimal-quadruped — DLC 3.x, production mammals
    superanimal-bird      — DLC 3.x, production birds

All models load from ~/.vireo/models/<name>/model.onnx with a sibling
config.json describing input size, normalization, and keypoint names.
"""
import json
import logging
import os
import threading

import numpy as np

log = logging.getLogger(__name__)

MODELS_DIR = os.path.expanduser("~/.vireo/models")

_sessions = {}
_locks = {}
_download_locks = {}


def decode_simcc(simcc_x, simcc_y, input_size, simcc_split_ratio=2.0):
    """Decode RTMPose simcc-format outputs to (K, 3) keypoints.

    Returns array of shape (num_keypoints, 3) with columns (x, y, conf)
    in input-image pixel space.
    """
    # Strip batch dim
    sx = simcc_x[0]  # (K, size_x)
    sy = simcc_y[0]  # (K, size_y)
    idx_x = np.argmax(sx, axis=1)
    idx_y = np.argmax(sy, axis=1)
    conf_x = sx[np.arange(sx.shape[0]), idx_x]
    conf_y = sy[np.arange(sy.shape[0]), idx_y]
    conf = np.minimum(conf_x, conf_y)
    x = idx_x / simcc_split_ratio
    y = idx_y / simcc_split_ratio
    return np.stack([x, y, conf], axis=1).astype(np.float32)
```

**Step 2: Run test, expect PASS.**

**Step 3: Commit**

```bash
git add vireo/keypoints.py vireo/tests/test_keypoints.py
git commit -m "keypoints: add decode_simcc helper with unit test"
```

### Task 4.3: Write failing test for `ensure_keypoint_weights`

**Step 1: Add test with mock**

```python
import os
from unittest.mock import patch

def test_ensure_keypoint_weights_short_circuits_if_present(tmp_path, monkeypatch):
    from vireo import keypoints as kp
    monkeypatch.setattr(kp, "MODELS_DIR", str(tmp_path))
    model_dir = tmp_path / "rtmpose-animal"
    model_dir.mkdir()
    (model_dir / "model.onnx").write_bytes(b"fake")
    (model_dir / "config.json").write_text("{}")
    # Should not trigger a download
    path = kp.ensure_keypoint_weights("rtmpose-animal")
    assert path == str(model_dir / "model.onnx")
```

**Step 2: Run, expect FAIL** (function doesn't exist).

### Task 4.4: Implement weights download

**Step 1: Add to `vireo/keypoints.py`**

```python
def _model_path(model_name):
    return os.path.join(MODELS_DIR, model_name)


def ensure_keypoint_weights(model_name, progress_callback=None):
    """Ensure ONNX weights for ``model_name`` are on disk.

    Downloads from the Vireo HF repo if absent. Returns path to model.onnx.
    Raises RuntimeError on download failure.
    """
    target = _model_path(model_name)
    onnx_path = os.path.join(target, "model.onnx")
    config_path = os.path.join(target, "config.json")
    if os.path.isfile(onnx_path) and os.path.isfile(config_path):
        return onnx_path

    lock = _download_locks.setdefault(model_name, threading.Lock())
    with lock:
        if os.path.isfile(onnx_path) and os.path.isfile(config_path):
            return onnx_path
        os.makedirs(target, exist_ok=True)
        if progress_callback:
            progress_callback(
                f"Downloading {model_name} (first run only)...", 0, 1
            )
        log.info("%s weights missing — downloading from Hugging Face", model_name)
        try:
            from huggingface_hub import hf_hub_download
            from models import ONNX_REPO
            for filename in ("model.onnx", "config.json"):
                downloaded = hf_hub_download(
                    repo_id=ONNX_REPO,
                    filename=filename,
                    subfolder=model_name,
                )
                dest = os.path.join(target, filename)
                if downloaded != dest:
                    import shutil
                    shutil.copy2(downloaded, dest)
        except Exception as e:
            raise RuntimeError(
                f"Failed to download {model_name} weights: {e}"
            ) from e
        log.info("%s weights downloaded", model_name)
        if progress_callback:
            progress_callback(f"{model_name} ready", 1, 1)
    return onnx_path
```

**Step 2: Run test, expect PASS.**

**Step 3: Commit**

```bash
git add vireo/keypoints.py vireo/tests/test_keypoints.py
git commit -m "keypoints: add ensure_keypoint_weights download helper"
```

### Task 4.5: Write failing test for `detect_keypoints`

**Step 1: Add test with mocked ONNX session**

```python
from unittest.mock import MagicMock, patch
from PIL import Image

def test_detect_keypoints_returns_named_keypoints(tmp_path, monkeypatch):
    from vireo import keypoints as kp
    monkeypatch.setattr(kp, "MODELS_DIR", str(tmp_path))
    model_dir = tmp_path / "rtmpose-animal"
    model_dir.mkdir()
    (model_dir / "model.onnx").write_bytes(b"fake")
    (model_dir / "config.json").write_text(json.dumps({
        "input_size": [1, 3, 256, 256],
        "mean": [123.675, 116.28, 103.53],
        "std": [58.395, 57.12, 57.375],
        "keypoints": ["left_eye", "right_eye", "nose", "neck", "root_of_tail",
                      "left_shoulder", "left_elbow", "left_front_paw",
                      "right_shoulder", "right_elbow", "right_front_paw",
                      "left_hip", "left_knee", "left_back_paw",
                      "right_hip", "right_knee", "right_back_paw"],
        "output_type": "simcc",
    }))

    # Mock onnxruntime session: return simcc where left_eye is at (128, 128)
    K = 17
    simcc_x = np.zeros((1, K, 512), dtype=np.float32)
    simcc_y = np.zeros((1, K, 512), dtype=np.float32)
    simcc_x[0, 0, 256] = 0.9  # left_eye x = 128 px
    simcc_y[0, 0, 256] = 0.9  # left_eye y = 128 px
    mock_sess = MagicMock()
    mock_sess.run.return_value = [simcc_x, simcc_y]
    monkeypatch.setattr(kp, "_load_session", lambda name: mock_sess)

    # Image: 512x512, bbox covering the center (0,0,512,512)
    img = Image.new("RGB", (512, 512), color=(128, 128, 128))
    bbox = (0, 0, 512, 512)
    result = kp.detect_keypoints(img, bbox, "rtmpose-animal")
    # Eye should land at ~(256, 256) in image coords after reverse-scaling
    # from 256x256 crop
    assert any(k["name"] == "left_eye" for k in result)
    left = [k for k in result if k["name"] == "left_eye"][0]
    assert abs(left["x"] - 256) < 5
    assert abs(left["y"] - 256) < 5
    assert left["conf"] > 0.85
```

**Step 2: Run, expect FAIL.**

### Task 4.6: Implement `detect_keypoints`

**Step 1: Add to `vireo/keypoints.py`**

```python
def _load_session(model_name):
    """Get cached onnxruntime session, loading on first use."""
    if model_name in _sessions:
        return _sessions[model_name]
    lock = _locks.setdefault(model_name, threading.Lock())
    with lock:
        if model_name in _sessions:
            return _sessions[model_name]
        import onnxruntime as ort
        onnx_path = os.path.join(MODELS_DIR, model_name, "model.onnx")
        if not os.path.isfile(onnx_path):
            raise FileNotFoundError(
                f"Keypoint model {model_name!r} not found at {onnx_path}. "
                "Call ensure_keypoint_weights() first."
            )
        _sessions[model_name] = ort.InferenceSession(
            onnx_path, providers=["CPUExecutionProvider"]
        )
    return _sessions[model_name]


def _load_config(model_name):
    config_path = os.path.join(MODELS_DIR, model_name, "config.json")
    with open(config_path) as f:
        return json.load(f)


def detect_keypoints(image, bbox, model_name):
    """Run a keypoint model on the bbox crop; return per-keypoint (x, y, conf).

    Args:
        image: PIL.Image (original-resolution RGB).
        bbox: (x0, y0, x1, y1) in image-pixel space (MegaDetector output).
        model_name: one of 'rtmpose-animal', 'superanimal-quadruped',
                    'superanimal-bird'.

    Returns:
        list of {"name": str, "x": float, "y": float, "conf": float},
        one entry per keypoint the model produces. Coordinates are in the
        original image's pixel space.
    """
    from PIL import Image
    cfg = _load_config(model_name)
    input_h, input_w = cfg["input_size"][2], cfg["input_size"][3]
    mean = np.array(cfg["mean"], dtype=np.float32).reshape(3, 1, 1)
    std = np.array(cfg["std"], dtype=np.float32).reshape(3, 1, 1)
    keypoint_names = cfg["keypoints"]

    x0, y0, x1, y1 = bbox
    crop = image.crop((x0, y0, x1, y1))
    crop_w, crop_h = crop.size
    # Aspect-ratio-preserving resize + pad to input_w x input_h
    scale = min(input_w / crop_w, input_h / crop_h)
    new_w = int(round(crop_w * scale))
    new_h = int(round(crop_h * scale))
    resized = crop.resize((new_w, new_h), Image.BILINEAR)
    # Pad (top-left alignment; record offset for inverse map)
    padded = Image.new("RGB", (input_w, input_h), color=(0, 0, 0))
    padded.paste(resized, (0, 0))

    arr = np.array(padded, dtype=np.float32).transpose(2, 0, 1)  # CHW
    arr = (arr - mean) / std
    arr = arr[np.newaxis, :, :, :]

    session = _load_session(model_name)
    outputs = session.run(None, {"pixel_values": arr})

    output_type = cfg.get("output_type", "heatmap")
    if output_type == "simcc":
        simcc_x, simcc_y = outputs
        kps_input_space = decode_simcc(
            simcc_x, simcc_y, input_size=input_w, simcc_split_ratio=2.0
        )
    else:
        heatmaps = outputs[0]  # (1, K, H', W')
        kps_input_space = decode_heatmaps(heatmaps, input_size=input_w)

    # Inverse map: input-space → crop-space → image-space
    result = []
    for i, name in enumerate(keypoint_names):
        x_in, y_in, conf = kps_input_space[i]
        x_crop = x_in / scale
        y_crop = y_in / scale
        x_img = x_crop + x0
        y_img = y_crop + y0
        result.append({
            "name": name,
            "x": float(x_img),
            "y": float(y_img),
            "conf": float(conf),
        })
    return result


def decode_heatmaps(heatmaps, input_size):
    """Decode (1, K, H', W') heatmaps to (K, 3) keypoints in input-image pixels.

    Simple argmax + subpixel quadratic refinement. Suitable for the SuperAnimal
    heatmap-head output.
    """
    hm = heatmaps[0]  # (K, H', W')
    K, H, W = hm.shape
    flat = hm.reshape(K, -1)
    idx = np.argmax(flat, axis=1)
    ys = (idx // W).astype(np.float32)
    xs = (idx % W).astype(np.float32)
    conf = flat[np.arange(K), idx]
    # Subpixel refinement via quadratic fit — skip here for simplicity; can add later.
    # Rescale to input-image pixels
    xs *= input_size / W
    ys *= input_size / H
    return np.stack([xs, ys, conf], axis=1).astype(np.float32)
```

**Step 2: Run test, expect PASS.**

**Step 3: Commit**

```bash
git add vireo/keypoints.py vireo/tests/test_keypoints.py
git commit -m "keypoints: add detect_keypoints with inverse coordinate mapping"
```

---

## Milestone 5: `compute_eye_tenengrad` in `quality.py`

Reuse the existing `_multiscale_tenengrad` over a window around the eye.

### Task 5.1: Write failing test

**Files:**
- Modify test: `vireo/tests/test_quality.py`

**Step 1: Add test**

```python
def test_compute_eye_tenengrad_uses_window_around_eye():
    from vireo.quality import compute_eye_tenengrad
    from PIL import Image
    import numpy as np

    img = Image.new("L", (400, 400), color=128)
    # Draw a sharp edge pattern centered on (200, 200)
    arr = np.array(img)
    arr[180:220, 180:220] = np.tile([0, 255] * 20, (40, 1))
    img = Image.fromarray(arr, mode="L").convert("RGB")

    bbox = (100, 100, 300, 300)  # 200x200 bbox
    eye_xy = (200.0, 200.0)
    result = compute_eye_tenengrad(img, eye_xy, bbox, k=0.08)
    # Window is 0.08 * 200 = 16 px around (200, 200) → fully inside the edge pattern
    assert result > 1000  # non-trivial sharpness signal


def test_compute_eye_tenengrad_clamps_to_image_bounds():
    from vireo.quality import compute_eye_tenengrad
    from PIL import Image

    img = Image.new("RGB", (100, 100), color=(128, 128, 128))
    # Eye at corner — window would extend past the image
    bbox = (0, 0, 100, 100)
    result = compute_eye_tenengrad(img, (2.0, 2.0), bbox, k=0.1)
    # Should not raise; should return a finite float (uniform gray → 0)
    assert result == 0.0
```

**Step 2: Run, expect FAIL.**

### Task 5.2: Implement

**Files:**
- Modify: `vireo/quality.py`

**Step 1: Add function**

```python
def compute_eye_tenengrad(image, eye_xy, bbox, k=0.08):
    """Multi-scale Tenengrad in a window around an eye keypoint.

    Window side length = k * min(bbox_w, bbox_h), clamped to image bounds.

    Args:
        image: PIL.Image (original resolution).
        eye_xy: (x, y) in image-pixel space.
        bbox: (x0, y0, x1, y1) MegaDetector bbox, image-pixel space.
        k: window size as fraction of min(bbox_w, bbox_h). Default 0.08.

    Returns:
        float — raw multi-scale Tenengrad in the window, 0.0 if window
        is empty after clamping.
    """
    x, y = eye_xy
    x0, y0, x1, y1 = bbox
    w_side = max(8, int(round(k * min(x1 - x0, y1 - y0))))
    half = w_side // 2
    img_w, img_h = image.size
    wx0 = max(0, int(x - half))
    wy0 = max(0, int(y - half))
    wx1 = min(img_w, int(x + half))
    wy1 = min(img_h, int(y + half))
    if wx1 <= wx0 or wy1 <= wy0:
        return 0.0
    window = image.crop((wx0, wy0, wx1, wy1))
    gray = _to_grayscale_array(window)
    mask = np.ones_like(gray, dtype=bool)
    return round(_multiscale_tenengrad(gray, mask), 2)
```

**Step 2: Run tests, expect PASS.**

**Step 3: Commit**

```bash
git add vireo/quality.py vireo/tests/test_quality.py
git commit -m "quality: add compute_eye_tenengrad for windowed eye sharpness"
```

---

## Milestone 6: Pipeline Stage — `detect_eye_keypoints`

Runs between masking and scoring. For each photo: route by species, run model, decode, apply three-gate policy, persist.

### Task 6.1: Write failing test for skip-when-weights-absent

**Files:**
- Modify test: `vireo/tests/test_pipeline.py`

**Step 1: Add test**

```python
def test_eye_keypoint_stage_skips_when_weights_absent(tmp_path, monkeypatch):
    from vireo import keypoints as kp
    from vireo.pipeline import detect_eye_keypoints_stage
    monkeypatch.setattr(kp, "MODELS_DIR", str(tmp_path / "empty"))
    db = Database(str(tmp_path / "x.db"))
    # insert a photo with species/mask — use fixture or inline
    pid = db.insert_photo({...})  # fill in
    detect_eye_keypoints_stage(db, config={"eye_detect_enabled": True})
    row = db.conn.execute(
        "SELECT eye_x, eye_y, eye_conf, eye_tenengrad FROM photos WHERE id=?",
        (pid,),
    ).fetchone()
    assert row == (None, None, None, None)
```

**Step 2: Run, expect FAIL** (function doesn't exist).

### Task 6.2: Implement stage skeleton

**Files:**
- Modify: `vireo/pipeline.py`

**Step 1: Add the stage function**

```python
def detect_eye_keypoints_stage(db, config, progress_callback=None):
    """Pipeline stage: detect eye keypoints and persist raw tenengrad.

    For each photo with a subject mask and an in-scope species, run the
    routed keypoint model. Apply the three-gate trust policy. Persist
    (eye_x, eye_y, eye_conf, eye_tenengrad) for gated-through photos.
    """
    from vireo import keypoints as kp
    from vireo.quality import compute_eye_tenengrad
    from vireo.taxonomy import classify_to_keypoint_group
    from PIL import Image

    if not config.get("eye_detect_enabled", True):
        log.info("Eye-focus detection disabled by config; skipping stage")
        return

    C = config.get("eye_classifier_conf_gate", 0.5)
    T = config.get("eye_detection_conf_gate", 0.5)
    k_window = config.get("eye_window_k", 0.08)

    photos = db.list_photos_for_eye_keypoint_stage()  # new helper — see 6.3
    total = len(photos)
    for i, photo in enumerate(photos):
        if progress_callback:
            progress_callback("Eye keypoints", i, total)
        try:
            _process_photo_for_eye(db, photo, config, C=C, T=T, k_window=k_window)
        except Exception as e:
            log.warning("Eye keypoint detection failed for photo %s: %s",
                        photo["id"], e, exc_info=True)


def _process_photo_for_eye(db, photo, config, *, C, T, k_window):
    from vireo import keypoints as kp
    from vireo.quality import compute_eye_tenengrad
    from vireo.taxonomy import classify_to_keypoint_group
    from PIL import Image

    # Gate 1: classifier + class
    if (photo.get("species_conf") or 0) < C:
        return
    group = classify_to_keypoint_group(db, photo.get("species_top1_taxon_id"))
    if group is None:
        return
    model_name = {
        "Aves": "superanimal-bird",
        "Mammalia": "superanimal-quadruped",
    }[group]

    # Gate 2: weights present
    try:
        kp.ensure_keypoint_weights(model_name)  # does NOT download here;
        # at pipeline-run time, user must have already opted in via the
        # pipeline page card. If the download has not happened, raise a
        # short-circuit that skips without error.
    except Exception:
        return
    if not os.path.isfile(os.path.join(kp.MODELS_DIR, model_name, "model.onnx")):
        return

    # Load image + bbox + mask
    image = Image.open(photo["path"]).convert("RGB")
    bbox = (photo["bbox_x0"], photo["bbox_y0"], photo["bbox_x1"], photo["bbox_y1"])
    mask = load_mask(photo["mask_path"])  # bool array (H, W)

    # Run keypoint model
    kps = kp.detect_keypoints(image, bbox, model_name)

    # Gate 3 + 4: eye conf >= T AND inside mask
    eye_candidates = []
    for k_point in kps:
        if k_point["name"] not in ("left_eye", "right_eye"):
            continue
        if k_point["conf"] < T:
            continue
        mx, my = int(k_point["x"]), int(k_point["y"])
        if not (0 <= mx < mask.shape[1] and 0 <= my < mask.shape[0]):
            continue
        if not mask[my, mx]:
            continue
        eye_candidates.append(k_point)

    if not eye_candidates:
        return

    # Per-eye tenengrad, max wins
    best = None
    best_score = -1.0
    for eye in eye_candidates:
        score = compute_eye_tenengrad(image, (eye["x"], eye["y"]), bbox, k=k_window)
        if score > best_score:
            best_score = score
            best = eye

    db.update_photo(
        photo["id"],
        eye_x=best["x"],
        eye_y=best["y"],
        eye_conf=best["conf"],
        eye_tenengrad=best_score,
    )
```

**Step 2: Run test, expect PASS.**

**Step 3: Commit**

```bash
git add vireo/pipeline.py vireo/tests/test_pipeline.py
git commit -m "pipeline: add detect_eye_keypoints_stage with three-gate policy"
```

### Task 6.3: Add `list_photos_for_eye_keypoint_stage`

**Files:**
- Modify: `vireo/db.py`

Add a helper that returns photos eligible for the stage — have a subject mask, have a species classification. The exact fields returned should be enough for `_process_photo_for_eye` to do its work without additional DB calls per photo. Match the shape/pattern of similar iteration helpers already in `db.py`.

Commit.

### Task 6.4: Wire stage into pipeline orchestration

**Files:**
- Modify: `vireo/pipeline.py` — add the stage call after masking, before scoring.
- Modify: `vireo/pipeline_job.py` — add progress reporting for the new stage.

Commit.

### Task 6.5: Test the full three-gate matrix

Write parameterized pytest cases for each gate:
- Gate 1 fail: classifier conf below C → stage writes nothing.
- Gate 1 fail: species is Actinopterygii → stage writes nothing.
- Gate 2 fail: weights missing → stage writes nothing.
- Gate 3 fail: both eye keypoints below T → stage writes nothing.
- Gate 4 fail: eye keypoint outside mask → stage writes nothing.
- All gates pass → eye_x, eye_y, eye_conf, eye_tenengrad populated.
- Max-over-eyes: left has higher tenengrad than right → left wins.

Commit.

---

## Milestone 7: Scoring Integration

Composite replacement + `reject_eye_soft` rule, both gated on `eye_tenengrad IS NOT NULL` (which in turn only gets populated when all four gates passed in the pipeline stage — so downstream scoring doesn't re-check gates).

### Task 7.1: Write failing test for composite-replacement

**Files:**
- Modify test: `vireo/tests/test_scoring.py`

```python
def test_composite_uses_eye_when_eye_tenengrad_present():
    from vireo.scoring import score_encounter
    photos = [
        # Photo A: has high subject_tenengrad, low eye_tenengrad
        {..., "subject_tenengrad": 50000, "eye_tenengrad": 5000},
        # Photo B: low subject_tenengrad, high eye_tenengrad
        {..., "subject_tenengrad": 5000, "eye_tenengrad": 50000},
    ]
    encounter = {"photos": photos}
    score_encounter(encounter)
    # When eye_tenengrad is present, it replaces subject_tenengrad in focus term.
    # So B should score higher than A on focus_score.
    assert photos[1]["focus_score"] > photos[0]["focus_score"]


def test_composite_uses_subject_when_eye_tenengrad_null():
    # With no eye_tenengrad, scoring matches pre-feature behavior exactly.
    photos = [
        {..., "subject_tenengrad": 50000, "eye_tenengrad": None},
        {..., "subject_tenengrad": 5000, "eye_tenengrad": None},
    ]
    encounter = {"photos": photos}
    score_encounter(encounter)
    assert photos[0]["focus_score"] > photos[1]["focus_score"]
```

### Task 7.2: Implement composite branch

**Files:**
- Modify: `vireo/scoring.py`

Modify the focus-score computation inside `score_encounter` so that when a photo has non-null `eye_tenengrad`, the encounter-percentile focus normalization uses the `eye_tenengrad` value instead of `subject_tenengrad`. Two implementation options:

- **Option A (clean):** compute two separate percentile normalizations — one over `subject_tenengrad`, one over `eye_tenengrad` — and for each photo use whichever corresponds to what's populated.
- **Option B (single):** build a per-photo `focus_source_value` = `eye_tenengrad if not None else subject_tenengrad`, then normalize that within encounter.

Prefer Option A: keeps `subject_focus_score` semantics unchanged for photos without an eye signal, and means the `eye_focus` percentile is computed against peer photos that *also* have an eye signal, which is the fairer comparison.

Commit.

### Task 7.3: Write failing test for `reject_eye_soft`

```python
def test_reject_eye_soft_fires_when_eye_present_and_soft():
    photos = [
        {..., "subject_tenengrad": 50000, "eye_tenengrad": 1000},  # soft eye
        {..., "subject_tenengrad": 50000, "eye_tenengrad": 50000},  # sharp eye
    ]
    encounter = {"photos": photos}
    score_encounter(encounter, config={"reject_eye_focus": 0.35})
    assert any("eye_soft" in r for r in photos[0]["reject_reasons"])
    assert not any("eye_soft" in r for r in photos[1].get("reject_reasons", []))


def test_reject_eye_soft_does_not_fire_when_eye_null():
    photos = [{..., "subject_tenengrad": 1000, "eye_tenengrad": None}]
    encounter = {"photos": photos}
    score_encounter(encounter, config={"reject_eye_focus": 0.35})
    assert not any("eye_soft" in r for r in photos[0].get("reject_reasons", []))
```

### Task 7.4: Implement `reject_eye_soft`

**Files:**
- Modify: `vireo/scoring.py`

In `score_encounter`, after computing `eye_focus_score`, if `eye_tenengrad` is not null AND `eye_focus_score < config["reject_eye_focus"]`, append `f"eye_soft (E={eye_focus_score:.3f} < {cfg['reject_eye_focus']})"` to `reject_reasons`. Ensure `photo["label"] = "REJECT"` logic still works.

Add default `"reject_eye_focus": 0.35` to the `DEFAULTS` dict.

Commit.

---

## Milestone 8: UI — Crosshair Overlay in Lightbox

Small visible signal at `(eye_x, eye_y)` in the review lightbox.

### Task 8.1: Read existing lightbox code

**Step 1: Search for lightbox implementation.**

```
grep -l "lightbox" vireo/templates/
```

Open the relevant template(s) and read the coordinate-space conventions in the JS: how it positions markers on zoomed/panned images. The crosshair must transform from image-pixel space `(eye_x, eye_y)` into lightbox-screen coords using the same transform the lightbox already uses.

### Task 8.2–8.5: Implement crosshair

Small SVG circle or CSS-only marker element, positioned via absolute-position + the existing image→screen transform. Visible regardless of zoom level. Hide when `eye_x` or `eye_y` is null.

Playwright test (or a simpler manual verification if Playwright is not already set up for Vireo) that the crosshair element is present and positioned correctly when a photo with `eye_x` is loaded.

Commit.

---

## Milestone 9: UI — Pipeline Page Stage Card

New card on `/pipeline` showing the keypoint-model download status, mirroring SAM2/DINOv2 cards.

### Task 9.1: Read existing card pattern

Open `vireo/templates/pipeline.html` and find the "Extract Features" card (added in commit `e3eba78`). Understand the download-status states and the JS that drives them.

### Task 9.2: Add the card

Two download buttons — one for SuperAnimal-Quadruped, one for SuperAnimal-Bird (each independently downloadable; if one is present and one is not, the card reflects that). Card state "Ready" when both are present.

Add a `GET /api/models/keypoints/status` endpoint in `vireo/app.py` that returns `{"superanimal_quadruped": "ready"|"missing", "superanimal_bird": "ready"|"missing"}`. Add `POST /api/models/keypoints/<name>/download` endpoint that triggers `ensure_keypoint_weights(name)` in a background job, following the SAM2 download pattern.

Commit.

---

## Milestone 10: Settings UI

Surface the four new thresholds.

### Task 10.1: Add fields to `vireo/templates/settings.html`

Following the existing input-with-default pattern:

- `eye_detect_enabled` (boolean toggle)
- `eye_classifier_conf_gate` (0.0–1.0 float, default 0.5)
- `eye_detection_conf_gate` (0.0–1.0 float, default 0.5)
- `eye_window_k` (0.02–0.25 float, default 0.08)
- `reject_eye_focus` (0.0–1.0 float, default 0.35)

### Task 10.2: Ensure `get_effective_config` passes these through

Config flows from `vireo/config.py` → workspace overrides in `workspaces.config_overrides`. New keys are just new dictionary entries; should flow through without code changes. Verify with an integration test that a workspace override of `reject_eye_focus` affects scoring output.

Commit.

---

## Milestone 11: SuperAnimal-Quadruped Export + Integration

Add the real mammal model. At this point the pipeline, scoring, UI, and DB are proven via RTMPose — we just swap in a more capable model.

### Task 11.1: Add `export_superanimal_quadruped` to `scripts/export_onnx.py`

DeepLabCut 3.x publishes PyTorch weights on HuggingFace. The export needs a wrapper that exposes the raw `forward` producing heatmaps, avoiding DLC's inference wrapper. Use `dlclibrary.load_superanimal_model("superanimal_quadruped")` or equivalent. Match the RTMPose export's structure — input shape, normalization, keypoint-name list, `output_type: "heatmap"`.

**Config fields** specific to SuperAnimal-Quadruped: `"keypoints": [...]` — use the DLC-documented order for this model (contains `left_eye`, `right_eye` among ~39 names).

### Task 11.2: Run export and validate

```bash
python scripts/export_onnx.py --model superanimal-quadruped --output-dir /tmp/vireo-export --validate
```

Validation compares decoded keypoint coordinates (not raw heatmaps) between PyTorch reference and ONNX Runtime output on a sample image. Tolerance: ±1 pixel.

### Task 11.3: Upload weights

```bash
huggingface-cli upload jss367/vireo-onnx-models \
    /tmp/vireo-export/superanimal-quadruped \
    superanimal-quadruped --repo-type model
```

### Task 11.4: Wire routing

In `keypoints.py`'s `detect_keypoints`, ensure the `model_name` path for `"superanimal-quadruped"` works with the heatmap output (not simcc). The `decode_heatmaps` function handles this.

In `pipeline.py`'s `_process_photo_for_eye`, the mapping is already:

```python
{"Aves": "superanimal-bird", "Mammalia": "superanimal-quadruped"}
```

Nothing to change unless a bug surfaces.

### Task 11.5: End-to-end test with a real mammal photo

Add a dev-only test under `tests/e2e/test_eye_keypoints.py` (skip when weights absent) that runs the full stage on a real mammal photo shipped under `tests/fixtures/` and asserts the eye was found within a hand-labeled ±5 px region.

Commit.

---

## Milestone 12: SuperAnimal-Bird Export + Integration

Parallel structure to Milestone 11. Model: SuperAnimal-Bird. Output format: heatmaps (same decode path). Real-image e2e test uses a bird photo.

Commit.

---

## Milestone 13: Final Integration and PR

### Task 13.1: Full test suite

```bash
python -m pytest tests/ vireo/tests/ -q
```

All tests must pass. Dev-only e2e tests skip cleanly in CI.

### Task 13.2: Manual dogfood run

Start Vireo on a real library, run the pipeline, verify:
1. Pipeline page shows the new Eye-focus detection card with download buttons.
2. After downloading SuperAnimal-Quadruped, the stage runs in the pipeline.
3. A mammal photo in the review lightbox shows the eye crosshair.
4. A fish photo has no crosshair (fell back to subject_focus).
5. A back-of-head mammal photo has no crosshair (eye conf below T).
6. The `eye_soft` reject reason appears on photos with sharp body + soft eye.

Note any issues in PR description.

### Task 13.3: Open PR

```bash
git push -u origin claude/eye-focus-detection
gh pr create --title "Eye-focus detection" --body "$(cat <<'EOF'
## Summary
- New per-photo eye-focus signal replaces body-level sharpness in the composite when a SuperAnimal keypoint model confidently localizes the eye.
- New `reject_eye_soft` hard-reject rule catches "sharp body, soft eye" frames — formerly scored as REVIEW.
- Two new ONNX models (SuperAnimal-Quadruped, SuperAnimal-Bird), opt-in download, taxonomy-routed from `species_top5[0]`.
- RTMPose-animal shipped as an integration spike; may be removed in a follow-up.
- Four new nullable columns on `photos` (`eye_x`, `eye_y`, `eye_conf`, `eye_tenengrad`). Safe migration; no rescoring on upgrade.

## Design and plan
- `docs/plans/2026-04-16-eye-focus-detection-design.md`
- `docs/plans/2026-04-16-eye-focus-detection-plan.md`

## Test plan
- [ ] `python -m pytest tests/ vireo/tests/ -q` passes
- [ ] Dev-only e2e tests pass locally with model weights present
- [ ] Manual dogfood on real library: crosshair appears on mammal portraits
- [ ] Manual dogfood: fish photos unchanged (fallback path)
- [ ] Manual dogfood: back-of-head shots unchanged (gate 3 catches low eye conf)
- [ ] Manual dogfood: eye_soft reject reason appears correctly on sharp-body soft-eye frames

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

### Task 13.4: Respond to review

Push fixes to the same branch (`claude/eye-focus-detection`). The pr-agent handles re-review. Squash-merge when approved.

---

## Open Items Deferred to V1.1

- Score-panel badge showing eye-based vs body-based focus scores with the numerical comparison.
- Per-workspace toggle UI to disable eye-focus detection (the config key already supports per-workspace override; just not surfaced in settings yet).
- Subpixel refinement in `decode_heatmaps` (quadratic fit around argmax) — deferred until real-image tests show the need.
- Decision on whether to keep RTMPose-animal shipped as a smaller/faster alternative or remove it once SuperAnimal integration is proven.
