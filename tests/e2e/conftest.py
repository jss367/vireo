import json
import os
import sys
import threading

import pytest
from PIL import Image
from werkzeug.serving import make_server

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'vireo'))


def _seed_classifier_model(home_dir):
    """Stage a fake bioclip-2 model on disk + active selection in models.json.

    The pipeline page's plan endpoint resolves model state via models.get_models(),
    which only reports a model as "downloaded" when its files are actually
    present. With no model registered, _classify_plan returns "Will skip"
    regardless of seeded predictions — masking the user-facing intent the
    tests verify ("Already done" when classifications exist). bioclip-2's
    model_str is in tol_supported, so the plan resolves the labels
    fingerprint to TOL_SENTINEL even with no label sets configured.
    """
    model_dir = os.path.join(home_dir, ".vireo", "models", "bioclip-2")
    os.makedirs(model_dir)
    for fname in (
        "image_encoder.onnx",
        "image_encoder.onnx.data",
        "text_encoder.onnx",
        "text_encoder.onnx.data",
        "tokenizer.json",
        "config.json",
        "tol_embeddings.npy",
        "tol_classes.json",
    ):
        with open(os.path.join(model_dir, fname), "w") as f:
            f.write("")
    # Mark verify-skipped so get_models() reports state="unverified" → downloaded.
    with open(os.path.join(model_dir, ".verify_skipped"), "w") as f:
        f.write("test")
    with open(os.path.join(home_dir, ".vireo", "models.json"), "w") as f:
        json.dump({"models": [], "active_model": "bioclip-2"}, f)


def seed_e2e_data(db, thumb_dir):
    """Seed database with data for E2E tests."""
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)

    f1 = db.add_folder("/photos/park", name="park")
    f2 = db.add_folder("/photos/yard", name="yard")

    photos = []
    for fid, name, ts in [
        (f1, "hawk1.jpg", "2024-03-10T08:00:00"),
        (f1, "hawk2.jpg", "2024-03-10T08:01:00"),
        (f1, "hawk3.jpg", "2024-03-10T08:02:00"),
        (f2, "robin1.jpg", "2024-06-15T10:00:00"),
        (f2, "robin2.jpg", "2024-06-15T10:01:00"),
    ]:
        pid = db.add_photo(
            folder_id=fid, filename=name, extension=".jpg",
            file_size=1000, file_mtime=1.0, timestamp=ts,
        )
        photos.append(pid)
        Image.new("RGB", (100, 100), color="green").save(
            os.path.join(thumb_dir, f"{pid}.jpg")
        )

    db.update_photo_rating(photos[0], 4)

    k1 = db.add_keyword("Red-tailed Hawk", is_species=True)
    k2 = db.add_keyword("American Robin", is_species=True)
    db.tag_photo(photos[0], k1)
    db.tag_photo(photos[3], k2)

    db.create_workspace("Field Work")

    # Add detections and predictions so the review page has content. Also
    # record classifier_runs so the pipeline plan reports Classify as
    # "Already done" — the table the plan consults, distinct from predictions.
    from labels_fingerprint import TOL_SENTINEL
    species_for_photo = [
        "Red-tailed Hawk", "Red-tailed Hawk", "Red-tailed Hawk",
        "American Robin", "American Robin",
    ]
    for pid, species in zip(photos, species_for_photo, strict=True):
        det_ids = db.save_detections(pid, [
            {"box": {"x": 0.1, "y": 0.1, "w": 0.5, "h": 0.5},
             "confidence": 0.95, "category": "animal"},
        ], detector_model="test-detector")
        db.add_prediction(
            detection_id=det_ids[0],
            species=species,
            confidence=0.92,
            model="BioCLIP-2",
            labels_fingerprint=TOL_SENTINEL,
        )
        db.record_classifier_run(
            detection_id=det_ids[0],
            classifier_model="BioCLIP-2",
            labels_fingerprint=TOL_SENTINEL,
            prediction_count=1,
        )

    return {"photos": photos, "folders": [f1, f2]}


@pytest.fixture()
def live_server(tmp_path, monkeypatch):
    """Start an isolated Flask server with seeded E2E data."""
    monkeypatch.setenv("HOME", str(tmp_path))

    # Import after HOME is patched so top-level logging setup uses tmp_path
    import config as cfg
    from app import create_app
    from db import Database

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    # Pin models.py paths to tmp_path so a model staged for the test is
    # resolvable regardless of when models.py was first imported in the session.
    import models
    monkeypatch.setattr(
        models, "CONFIG_PATH", str(tmp_path / ".vireo" / "models.json"),
    )
    monkeypatch.setattr(
        models, "DEFAULT_MODELS_DIR", str(tmp_path / ".vireo" / "models"),
    )
    _seed_classifier_model(str(tmp_path))

    db_path = str(tmp_path / "test.db")
    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir)

    db = Database(db_path)
    seed_data = seed_e2e_data(db, thumb_dir)

    app = create_app(db_path=db_path, thumb_cache_dir=thumb_dir)

    server = make_server("127.0.0.1", 0, app)
    port = server.socket.getsockname()[1]
    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()

    try:
        yield {
            "url": f"http://127.0.0.1:{port}",
            "app": app,
            "db": db,
            "data": seed_data,
        }
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()
        if hasattr(app, "_cleanup_app_resources"):
            app._cleanup_app_resources()
        db.close()
