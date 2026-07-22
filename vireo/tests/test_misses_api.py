"""Tests for the /api/misses Flask endpoints.

Covers the three routes: list (grouped and category-filtered),
bulk reject, and per-photo unflag.
"""

import json
import os

import pytest
from PIL import Image


@pytest.fixture
def db_with_misses(tmp_path, monkeypatch):
    """Flask app + DB seeded with one photo in each miss category.

    Returns (app, db, {"no_subject": id, "clipped": id, "oof": id}).
    """
    monkeypatch.setenv("HOME", str(tmp_path))
    import config as cfg
    from app import create_app
    from db import Database

    cfg.CONFIG_PATH = str(tmp_path / "config.json")

    db_path = str(tmp_path / "test.db")
    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir)

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder("/photos/fake", name="fake")

    p_ns = db.add_photo(
        folder_id=fid, filename="ns.jpg", extension=".jpg",
        file_size=100, file_mtime=1.0, timestamp="2026-04-22T10:00:00",
    )
    p_clip = db.add_photo(
        folder_id=fid, filename="clip.jpg", extension=".jpg",
        file_size=100, file_mtime=2.0, timestamp="2026-04-22T10:00:01",
    )
    p_oof = db.add_photo(
        folder_id=fid, filename="oof.jpg", extension=".jpg",
        file_size=100, file_mtime=3.0, timestamp="2026-04-22T10:00:02",
    )

    db.conn.execute(
        "UPDATE photos SET miss_no_subject=1, miss_computed_at='2026-04-22' "
        "WHERE id=?", (p_ns,),
    )
    db.conn.execute(
        "UPDATE photos SET miss_clipped=1, miss_computed_at='2026-04-22' "
        "WHERE id=?", (p_clip,),
    )
    db.conn.execute(
        "UPDATE photos SET miss_oof=1, miss_computed_at='2026-04-22' "
        "WHERE id=?", (p_oof,),
    )
    db.conn.commit()

    for pid in (p_ns, p_clip, p_oof):
        Image.new("RGB", (100, 100)).save(os.path.join(thumb_dir, f"{pid}.jpg"))

    app = create_app(db_path=db_path, thumb_cache_dir=thumb_dir)
    return app, db, {"no_subject": p_ns, "clipped": p_clip, "oof": p_oof}


@pytest.fixture
def client(db_with_misses):
    app, _, _ = db_with_misses
    return app.test_client()


@pytest.fixture
def clipped_photo_id(db_with_misses):
    _, _, ids = db_with_misses
    return ids["clipped"]


def test_api_misses_returns_grouped_counts(client, db_with_misses):
    r = client.get("/api/misses")
    assert r.status_code == 200
    data = r.get_json()
    assert set(data.keys()) == {"no_subject", "clipped", "oof"}
    assert isinstance(data["clipped"], list)
    assert len(data["no_subject"]) == 1
    assert len(data["clipped"]) == 1
    assert len(data["oof"]) == 1


def test_api_misses_filter_by_category(client, db_with_misses):
    r = client.get("/api/misses?category=clipped")
    assert r.status_code == 200
    data = r.get_json()
    assert data["category"] == "clipped"
    assert all(p["miss_clipped"] == 1 for p in data["photos"])
    assert len(data["photos"]) == 1


def test_api_misses_filters_by_collection_and_browse_attributes(
    client, db_with_misses,
):
    _, db, ids = db_with_misses
    collection_id = db.add_collection(
        "Review subset",
        json.dumps([{
            "field": "photo_ids",
            "value": [ids["no_subject"], ids["clipped"]],
        }]),
    )
    db.update_photo_rating(ids["no_subject"], 2)
    db.update_photo_rating(ids["clipped"], 5)
    db.set_color_label(ids["clipped"], "red")
    keyword_id = db.add_keyword("keeper")
    db.tag_photo(ids["clipped"], keyword_id)

    r = client.get(
        f"/api/misses?collection_id={collection_id}&rating_min=4"
        "&color_label=red&keyword=keeper"
    )
    assert r.status_code == 200
    data = r.get_json()
    assert data["no_subject"] == []
    assert [p["id"] for p in data["clipped"]] == [ids["clipped"]]
    assert data["oof"] == []


def test_api_misses_accepts_universal_filter_rules(client, db_with_misses):
    _, db, ids = db_with_misses
    db.update_photo_rating(ids["no_subject"], 2)
    db.update_photo_rating(ids["clipped"], 5)
    rules = {
        "mode": "all",
        "rules": [
            {"field": "rating", "op": ">=", "value": 4},
            {"field": "filename", "op": "contains", "value": "clip"},
        ],
    }

    r = client.post("/api/misses", json={"rules": rules})

    assert r.status_code == 200
    data = r.get_json()
    assert data["no_subject"] == []
    assert [p["id"] for p in data["clipped"]] == [ids["clipped"]]
    assert data["oof"] == []


def test_api_misses_actions_accept_universal_filter_rules(
    client, db_with_misses,
):
    _, db, ids = db_with_misses
    db.conn.execute(
        "UPDATE photos SET miss_clipped=1 WHERE id=?", (ids["no_subject"],)
    )
    db.conn.commit()
    rules = {
        "mode": "all",
        "rules": [{"field": "photo_ids", "op": "in", "value": [ids["clipped"]]}],
    }

    reject = client.post(
        "/api/misses/reject",
        data=json.dumps({"category": "clipped", "rules": rules}),
        content_type="application/json",
    )

    assert reject.status_code == 200
    assert reject.get_json()["rejected"] == 1
    assert db.get_photo(ids["clipped"])["flag"] == "rejected"
    assert db.get_photo(ids["no_subject"])["flag"] != "rejected"


def test_api_misses_rejects_malformed_universal_rules(client):
    r = client.get("/api/misses", query_string={"rules": "not-json"})
    assert r.status_code == 400
    assert r.get_json()["error"] == "rules and visual must be valid JSON"


def test_api_misses_post_requires_object_body(client):
    r = client.post("/api/misses", json=[])
    assert r.status_code == 400
    assert r.get_json()["error"] == "request body must be a JSON object"


def test_api_misses_reject_and_recompute_honor_collection_filter(
    client, db_with_misses,
):
    _, db, ids = db_with_misses
    # Put two photos in the same category, but only one in the filter scope.
    db.conn.execute(
        "UPDATE photos SET miss_clipped=1 WHERE id=?", (ids["no_subject"],)
    )
    db.conn.commit()
    collection_id = db.add_collection(
        "Only clipped",
        json.dumps([{"field": "photo_ids", "value": [ids["clipped"]]}]),
    )

    recompute = client.post(
        "/api/misses/recompute",
        data=json.dumps({"collection_id": collection_id}),
        content_type="application/json",
    )
    assert recompute.status_code == 200
    assert recompute.get_json()["updated"] == 1

    # Re-seed the persisted category after recompute; this assertion exercises
    # reject scoping independently of the classifier's fixture-derived result.
    db.conn.execute(
        "UPDATE photos SET miss_clipped=1 WHERE id IN (?, ?)",
        (ids["no_subject"], ids["clipped"]),
    )
    db.conn.commit()

    reject = client.post(
        "/api/misses/reject",
        data=json.dumps({
            "category": "clipped",
            "collection_id": collection_id,
        }),
        content_type="application/json",
    )
    assert reject.status_code == 200
    assert reject.get_json()["rejected"] == 1
    flags = {
        row["id"]: row["flag"]
        for row in db.conn.execute(
            "SELECT id, flag FROM photos WHERE id IN (?, ?)",
            (ids["no_subject"], ids["clipped"]),
        )
    }
    assert flags[ids["clipped"]] == "rejected"
    assert flags[ids["no_subject"]] != "rejected"


def test_api_misses_rejects_unknown_collection(client):
    r = client.get("/api/misses?collection_id=999999")
    assert r.status_code == 400
    assert r.get_json()["error"] == "collection not found"


def test_api_bulk_reject_sets_flag(client, db_with_misses):
    r = client.post(
        "/api/misses/reject",
        data=json.dumps({"category": "clipped"}),
        content_type="application/json",
    )
    assert r.status_code == 200
    body = r.get_json()
    assert body["rejected"] >= 1
    assert body["category"] == "clipped"


def test_api_unflag_miss_clears_boolean(client, db_with_misses, clipped_photo_id):
    r = client.post(
        f"/api/misses/{clipped_photo_id}/unflag",
        data=json.dumps({"category": "clipped"}),
        content_type="application/json",
    )
    assert r.status_code == 200
    assert r.get_json() == {"ok": True}


def test_api_misses_rejects_invalid_category_on_list(client, db_with_misses):
    r = client.get("/api/misses?category=bogus")
    assert r.status_code == 400


def test_api_misses_rejects_invalid_category_on_reject(client, db_with_misses):
    r = client.post(
        "/api/misses/reject",
        data=json.dumps({"category": "bogus"}),
        content_type="application/json",
    )
    assert r.status_code == 400


def test_api_misses_rejects_invalid_category_on_unflag(client, db_with_misses, clipped_photo_id):
    r = client.post(
        f"/api/misses/{clipped_photo_id}/unflag",
        data=json.dumps({"category": "bogus"}),
        content_type="application/json",
    )
    assert r.status_code == 400


def test_api_misses_config_returns_thresholds(client, db_with_misses):
    r = client.get("/api/misses/config")
    assert r.status_code == 200
    data = r.get_json()
    assert data["detector_confidence"] == pytest.approx(0.2)
    assert data["miss_det_confidence"] == pytest.approx(0.20)
    assert data["miss_classifier_override_conf"] == pytest.approx(0.8)


@pytest.mark.parametrize("path", ["/api/misses/preview", "/api/misses/recompute"])
def test_api_misses_threshold_endpoints_reject_non_object_json(client, path):
    r = client.post(path, data=json.dumps(["not", "an", "object"]),
                    content_type="application/json")
    assert r.status_code == 400
    assert "JSON object" in r.get_json()["error"]


def test_api_misses_preview_uses_classifier_rescue_override(tmp_path, monkeypatch):
    """Preview should recalculate categories without persisting DB flags."""
    monkeypatch.setenv("HOME", str(tmp_path))
    import config as cfg
    from app import create_app
    from db import Database

    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    db_path = str(tmp_path / "test.db")
    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir)

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder("/photos/birds", name="birds")
    pid = db.add_photo(
        fid, "weak-bird.jpg", extension=".jpg", file_size=100,
        file_mtime=1.0, timestamp="2026-04-22T10:00:00",
    )
    det_id = db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.2, "w": 0.3, "h": 0.4},
          "confidence": 0.04, "category": "animal"}],
        detector_model="megadetector-v6",
    )[0]
    db.add_prediction(
        det_id, species="Likely Bird", confidence=0.70,
        model="BioCLIP-2.5",
    )

    app = create_app(db_path=db_path, thumb_cache_dir=thumb_dir)
    client = app.test_client()

    default_r = client.post(
        "/api/misses/preview",
        data=json.dumps({}),
        content_type="application/json",
    )
    assert default_r.status_code == 200
    default_payload = default_r.get_json()
    assert [p["id"] for p in default_payload["no_subject"]] == [pid]
    assert default_payload["no_subject"][0]["detection_conf"] == pytest.approx(0.04)
    assert json.loads(default_payload["no_subject"][0]["detection_box"]) == {
        "x": 0.1, "y": 0.2, "w": 0.3, "h": 0.4,
    }

    rescued_r = client.post(
        "/api/misses/preview",
        data=json.dumps({"miss_classifier_override_conf": 0.65}),
        content_type="application/json",
    )
    assert rescued_r.status_code == 200
    assert rescued_r.get_json()["no_subject"] == []
    row = db.conn.execute(
        "SELECT miss_no_subject, miss_computed_at FROM photos WHERE id=?",
        (pid,),
    ).fetchone()
    assert row["miss_no_subject"] is None
    assert row["miss_computed_at"] is None


def test_api_misses_recompute_can_save_workspace_defaults(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    import config as cfg
    from app import create_app
    from db import Database

    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    db_path = str(tmp_path / "test.db")
    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir)

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder("/photos/birds", name="birds")
    pid = db.add_photo(
        fid, "weak-bird.jpg", extension=".jpg", file_size=100,
        file_mtime=1.0, timestamp="2026-04-22T10:00:00",
    )
    det_id = db.save_detections(
        pid,
        [{"box": {"x": 0.1, "y": 0.2, "w": 0.3, "h": 0.4},
          "confidence": 0.04, "category": "animal"}],
        detector_model="megadetector-v6",
    )[0]
    db.add_prediction(
        det_id, species="Likely Bird", confidence=0.70,
        model="BioCLIP-2.5",
    )

    app = create_app(db_path=db_path, thumb_cache_dir=thumb_dir)
    client = app.test_client()
    r = client.post(
        "/api/misses/recompute",
        data=json.dumps({
            "miss_classifier_override_conf": 0.65,
            "detector_confidence": 0.05,
            "save_defaults": True,
        }),
        content_type="application/json",
    )
    assert r.status_code == 200
    data = r.get_json()
    assert data["updated"] == 1
    assert data["saved_defaults"] is True

    row = db.conn.execute(
        "SELECT miss_no_subject FROM photos WHERE id=?", (pid,),
    ).fetchone()
    assert row["miss_no_subject"] == 0

    effective = db.get_effective_config(cfg.load())
    assert effective["detector_confidence"] == pytest.approx(0.05)
    assert effective["pipeline"]["miss_classifier_override_conf"] == pytest.approx(0.65)


def test_api_misses_recompute_preserves_custom_derived_thresholds(
    client, db_with_misses,
):
    """Derived burst/singleton thresholds should change only with their slider."""
    import config as cfg

    _, db, _ = db_with_misses
    db.update_workspace(db._active_workspace_id, config_overrides={
        "pipeline": {
            "miss_det_confidence": 0.4,
            "miss_det_confidence_burst": 0.05,
            "miss_bbox_area_min": 0.01,
            "miss_bbox_area_min_singleton": 0.001,
        }
    })

    r = client.post(
        "/api/misses/recompute",
        data=json.dumps({"miss_oof_ratio": 0.42, "save_defaults": True}),
        content_type="application/json",
    )
    assert r.status_code == 200
    data = r.get_json()
    assert data["config"]["miss_det_confidence_burst"] == pytest.approx(0.05)
    assert data["config"]["miss_bbox_area_min_singleton"] == pytest.approx(0.001)

    effective = db.get_effective_config(cfg.load())
    assert effective["pipeline"]["miss_det_confidence_burst"] == pytest.approx(0.05)
    assert effective["pipeline"]["miss_bbox_area_min_singleton"] == pytest.approx(
        0.001
    )
    assert effective["pipeline"]["miss_oof_ratio"] == pytest.approx(0.42)


def test_api_bulk_reject_records_edit_history(client, db_with_misses):
    """Bulk reject must write a batch `flag` edit_history entry so the change
    is undoable and shows up in the audit log, matching /api/batch/flag."""
    _, db, ids = db_with_misses
    r = client.post(
        "/api/misses/reject",
        data=json.dumps({"category": "clipped"}),
        content_type="application/json",
    )
    assert r.status_code == 200
    assert r.get_json()["rejected"] == 1

    history = db.get_edit_history(limit=5, offset=0)
    assert history, "bulk reject did not record an edit_history entry"
    entry = history[0]
    assert entry["action_type"] == "flag"
    assert entry["new_value"] == "rejected"
    assert entry["is_batch"] == 1
    assert entry["item_count"] == 1
    assert "category=clipped" in (entry["description"] or "")


def test_api_bulk_reject_undo_restores_original_null_flag(client, db_with_misses):
    """Undoing bulk reject on a row whose original flag was NULL must
    restore NULL, not an empty string — otherwise the row lands in a
    non-canonical state outside (none/flagged/rejected or NULL)."""
    _, db, ids = db_with_misses
    pid = ids["clipped"]
    # Force the flag to NULL; bulk_reject's selection predicate treats NULL
    # the same as non-rejected, so the row still participates in the reject.
    db.conn.execute("UPDATE photos SET flag=NULL WHERE id=?", (pid,))
    db.conn.commit()
    before = db.conn.execute(
        "SELECT flag FROM photos WHERE id=?", (pid,)
    ).fetchone()["flag"]
    assert before is None

    r = client.post(
        "/api/misses/reject",
        data=json.dumps({"category": "clipped"}),
        content_type="application/json",
    )
    assert r.status_code == 200

    assert db.conn.execute(
        "SELECT flag FROM photos WHERE id=?", (pid,)
    ).fetchone()["flag"] == "rejected"

    entry = db.undo_last_edit()
    assert entry is not None

    after = db.conn.execute(
        "SELECT flag FROM photos WHERE id=?", (pid,)
    ).fetchone()["flag"]
    assert after is None, (
        "undo must restore NULL, not an empty string — saw: %r" % after
    )
    assert any(
        c["photo_id"] == pid
        and c["change_type"] == "flag"
        and c["value"] == "none"
        for c in db.get_pending_changes()
    )


def test_api_bulk_reject_no_matches_skips_edit_history(client, db_with_misses):
    """If nothing matches (empty category), no edit_history entry is written —
    avoids cluttering the undo log with no-op rows."""
    _, db, _ = db_with_misses
    before = len(db.get_edit_history(limit=50, offset=0))
    r = client.post(
        "/api/misses/reject",
        data=json.dumps({"category": "clipped",
                         "since": "2099-01-01T00:00:00+00:00"}),
        content_type="application/json",
    )
    assert r.status_code == 200
    assert r.get_json()["rejected"] == 0
    after = len(db.get_edit_history(limit=50, offset=0))
    assert before == after


def test_api_misses_since_restricts_to_recent_run(client, db_with_misses):
    """`?since=<ts>` filters the grouped response to photos computed at-or-after
    the timestamp. Used by the pipeline-review step to scope the grid to the
    current run."""
    _, db, ids = db_with_misses
    # Age the clipped and oof rows; keep no_subject on the "new" timestamp.
    db.conn.execute(
        "UPDATE photos SET miss_computed_at='2026-04-20T00:00:00+00:00' "
        "WHERE id IN (?, ?)", (ids["clipped"], ids["oof"]),
    )
    db.conn.commit()

    r = client.get("/api/misses?since=2026-04-21T00:00:00%2B00:00")
    assert r.status_code == 200
    data = r.get_json()
    assert len(data["no_subject"]) == 1
    assert data["clipped"] == []
    assert data["oof"] == []

    r2 = client.get("/api/misses?category=clipped&since=2026-04-21T00:00:00%2B00:00")
    assert r2.status_code == 200
    assert r2.get_json()["photos"] == []


def test_api_misses_rejects_visual_collection(client, db_with_misses):
    """The legacy Misses ``collection_id`` shim evaluates ``rules`` only.

    A visual collection would silently widen the miss scope to every metadata
    match instead of the visually matched subset. The shared-bar page sends
    rules + visual directly, but old API callers still need this boundary.
    """
    _, db, _ = db_with_misses
    visual_cid = db.add_collection(
        "Visual", json.dumps([{"field": "rating", "op": ">=", "value": 3}]),
        visual_json=json.dumps({"prompt": "bird", "strength": "balanced"}),
    )

    r = client.get(f"/api/misses?collection_id={visual_cid}")
    assert r.status_code == 400
    assert "visual collections" in r.get_json()["error"]

    r_prev = client.post(
        "/api/misses/preview",
        data=json.dumps({"collection_id": visual_cid}),
        content_type="application/json",
    )
    assert r_prev.status_code == 400
