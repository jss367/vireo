"""End-to-end eye-keypoint pipeline test with real ONNX weights.

Skipped in CI: ``tests/e2e`` is behind ``--ignore=tests/e2e`` in pyproject.toml
and each test individually skips when the weights or fixture photo aren't
present locally. This test exists so an engineer validating the SuperAnimal
export can run one command and confirm a real mammal frame produces an eye
coordinate inside the expected region.

How to run (from a dev machine with weights downloaded):

    # 1. Export weights into ~/.vireo/models via the dev export script or
    #    the Pipeline page's Download button.
    # 2. Drop a reference mammal portrait into tests/fixtures/e2e_mammal.jpg
    #    — a frame where the eye is clearly in the upper-center of the
    #    bounding box.
    # 3. Set the hand-labeled ground truth eye coord in EXPECTED_EYE below.
    # 4. Run:
    #       pytest tests/e2e/test_eye_keypoints.py -v

The fixture image + expected coordinate are environment-specific; committing
them here would make the test either brittle (different library) or heavy
(binary in git). Keep the fixture local.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "vireo"))

# Hand-labeled ground truth — overwrite with your fixture's actual eye coord.
# A ±5-px radius matches the plan's Milestone 11.5 acceptance bar.
EXPECTED_EYE = {"x": 1200, "y": 850}
EXPECTED_RADIUS_PX = 5

_FIXTURE_PATH = os.path.join(
    os.path.dirname(__file__), "..", "fixtures", "e2e_mammal.jpg"
)


def _weights_present(model_name):
    from keypoints import MODELS_DIR
    return (
        os.path.isfile(os.path.join(MODELS_DIR, model_name, "model.onnx"))
        and os.path.isfile(os.path.join(MODELS_DIR, model_name, "config.json"))
    )


pytestmark = pytest.mark.skipif(
    not os.path.isfile(_FIXTURE_PATH),
    reason=(
        "Fixture tests/fixtures/e2e_mammal.jpg not present — see module "
        "docstring for setup."
    ),
)


def test_stage_finds_eye_on_reference_mammal(tmp_path):
    """Full pipeline stage on a real mammal frame locates the eye.

    Uses SuperAnimal-Quadruped if present, else RTMPose-animal, else skips.
    Asserts the stored (eye_x, eye_y) lands within EXPECTED_RADIUS_PX of the
    hand-labeled ground truth.
    """
    if _weights_present("superanimal-quadruped"):
        expected_class = "Mammalia"
    elif _weights_present("rtmpose-animal"):
        # RTMPose-animal is classified as 'Mammalia' for routing purposes
        # even though its training set is broader — the eye-focus stage
        # doesn't know which backbone is behind the model name.
        expected_class = "Mammalia"
    else:
        pytest.skip("No keypoint weights on disk; export + place under ~/.vireo/models")

    from db import Database
    from PIL import Image
    from pipeline import detect_eye_keypoints_stage

    db = Database(str(tmp_path / "e2e.db"))
    ws_id = db._active_workspace_id
    folder_dir = tmp_path / "photos"
    folder_dir.mkdir()
    img = Image.open(_FIXTURE_PATH).convert("RGB")
    img.save(folder_dir / "mammal.jpg")
    fid = db.add_folder(str(folder_dir), name="photos")
    db.add_workspace_folder(ws_id, fid)
    pid = db.add_photo(
        fid, "mammal.jpg", ".jpg", os.path.getsize(_FIXTURE_PATH), 1.0,
        timestamp="2026-04-16T10:00:00",
        width=img.width, height=img.height,
    )

    # Full-frame mask stand-in — makes the mask gate trivially pass on a
    # fixture without a SAM2 run. The stage's gate-4 check just needs the
    # eye to fall inside this mask.
    import numpy as np
    mask = np.ones((img.height, img.width), dtype=np.uint8) * 255
    mask_path = tmp_path / "mask.png"
    Image.fromarray(mask, mode="L").save(mask_path)
    db.update_photo_pipeline_features(pid, mask_path=str(mask_path))

    # Bbox covers the subject area of the fixture — loose enough to include
    # the eye. If your fixture's subject is in a different region, tune this.
    det_ids = db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.8, "h": 0.8}, "confidence": 0.95}],
        detector_model="MegaDetector",
    )
    db.add_prediction(
        det_ids[0],
        species="Vulpes vulpes",
        confidence=0.92,
        model="bioclip-2.5",
        category="match",
        taxonomy={
            "kingdom": "Animalia", "phylum": "Chordata",
            "class": expected_class, "order": "Carnivora",
            "family": "Canidae", "genus": "Vulpes",
            "scientific_name": "Vulpes vulpes",
        },
    )

    detect_eye_keypoints_stage(db, config={"eye_detect_enabled": True})

    row = db.conn.execute(
        "SELECT eye_x, eye_y, eye_conf, eye_tenengrad FROM photos WHERE id=?",
        (pid,),
    ).fetchone()
    assert row[0] is not None, (
        "eye_x not populated — stage rejected at a gate. Inspect "
        "logs or widen bbox/lower conf gates if the model truly found no eye."
    )

    dx = row[0] - EXPECTED_EYE["x"]
    dy = row[1] - EXPECTED_EYE["y"]
    assert (dx * dx + dy * dy) <= (EXPECTED_RADIUS_PX * EXPECTED_RADIUS_PX), (
        f"Detected eye ({row[0]:.1f}, {row[1]:.1f}) is outside ±{EXPECTED_RADIUS_PX}px "
        f"of expected ({EXPECTED_EYE['x']}, {EXPECTED_EYE['y']})."
    )
    assert row[3] > 0.0  # eye_tenengrad should be positive on a real photo
