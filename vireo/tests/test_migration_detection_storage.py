import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


def _build_legacy_db(path):
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE folders (id INTEGER PRIMARY KEY, path TEXT);
        CREATE TABLE photos (id INTEGER PRIMARY KEY, folder_id INTEGER,
                             filename TEXT, timestamp TEXT, rating INTEGER,
                             UNIQUE(folder_id, filename));
        CREATE TABLE workspaces (id INTEGER PRIMARY KEY, name TEXT UNIQUE,
                                 config_overrides TEXT, ui_state TEXT,
                                 last_opened_at TEXT);
        CREATE TABLE workspace_folders (
            workspace_id INTEGER, folder_id INTEGER,
            PRIMARY KEY (workspace_id, folder_id)
        );
        CREATE TABLE detections (
            id INTEGER PRIMARY KEY, photo_id INTEGER, workspace_id INTEGER,
            box_x REAL, box_y REAL, box_w REAL, box_h REAL,
            detector_confidence REAL, category TEXT, detector_model TEXT,
            created_at TEXT
        );
        CREATE TABLE predictions (
            id INTEGER PRIMARY KEY, detection_id INTEGER, species TEXT,
            confidence REAL, model TEXT,
            status TEXT DEFAULT 'pending', reviewed_at TEXT,
            individual TEXT, group_id TEXT,
            vote_count INTEGER, total_votes INTEGER,
            created_at TEXT
        );
        INSERT INTO folders VALUES (1, '/p');
        INSERT INTO photos (id, folder_id, filename) VALUES
            (10, 1, 'a.jpg'), (11, 1, 'b.jpg');
        INSERT INTO workspaces (id, name) VALUES (1, 'A'), (2, 'B');
        INSERT INTO workspace_folders VALUES (1, 1), (2, 1);
        -- photo 10 detected in both workspaces (same box -> dedupes)
        INSERT INTO detections (id, photo_id, workspace_id, box_x, box_y, box_w, box_h,
                                detector_confidence, category, detector_model, created_at)
          VALUES (100, 10, 1, 0.1, 0.1, 0.4, 0.4, 0.92, 'animal', 'megadetector-v6', 't1'),
                 (200, 10, 2, 0.1, 0.1, 0.4, 0.4, 0.92, 'animal', 'megadetector-v6', 't1'),
                 -- photo 11 only in workspace A, different box
                 (300, 11, 1, 0.2, 0.2, 0.5, 0.5, 0.71, 'animal', 'megadetector-v6', 't1');
        INSERT INTO predictions (id, detection_id, species, model,
                                 status, individual, group_id)
          VALUES (1, 100, 'Robin',  'bioclip-2', 'approved', 'Ruby', 'pair-01'),
                 (2, 200, 'Robin',  'bioclip-2', 'pending',  NULL,   NULL),
                 (3, 300, 'Sparrow','bioclip-2', 'rejected', NULL,   NULL);
    """)
    conn.commit()
    conn.close()


def test_full_migration(tmp_path):
    db_path = str(tmp_path / "legacy.db")
    _build_legacy_db(db_path)

    from db import Database
    db = Database(db_path)

    # detections: one row per unique box; workspace_id column gone
    det_cols = {r[1] for r in db.conn.execute(
        "PRAGMA table_info(detections)"
    ).fetchall()}
    assert "workspace_id" not in det_cols
    photo10_rows = db.conn.execute(
        "SELECT id FROM detections WHERE photo_id=10"
    ).fetchall()
    assert len(photo10_rows) == 1
    canonical_10 = photo10_rows[0]["id"]

    # predictions re-pointed to canonical detection; legacy columns gone
    pred_cols = {r[1] for r in db.conn.execute(
        "PRAGMA table_info(predictions)"
    ).fetchall()}
    for legacy in ("status", "individual", "group_id", "reviewed_at",
                   "vote_count", "total_votes", "model"):
        assert legacy not in pred_cols, f"legacy column {legacy} still present"
    photo10_preds = db.conn.execute(
        "SELECT id, detection_id FROM predictions "
        "WHERE detection_id = ? ORDER BY id",
        (canonical_10,),
    ).fetchall()
    # Predictions 1 and 2 both pointed at what is now the canonical detection.
    # Task 9 re-points pred 2's detection_id from 200 -> canonical_10, then
    # Task 12's rewrite copies predictions through `INSERT OR IGNORE` keyed on
    # (detection_id, classifier_model, labels_fingerprint, species). Preds 1
    # and 2 share that tuple exactly, so the lower-id row (pred 1) wins and
    # pred 2 is dropped. Workspace 2's "pending" review state for that
    # prediction is correctly represented by absence from prediction_review.
    assert {p["id"] for p in photo10_preds} == {1}

    # review state landed in prediction_review
    reviews = db.conn.execute(
        "SELECT prediction_id, workspace_id, status, individual "
        "FROM prediction_review ORDER BY prediction_id, workspace_id"
    ).fetchall()
    review_map = {(r["prediction_id"], r["workspace_id"]):
                  (r["status"], r["individual"]) for r in reviews}
    # pred 1 was approved in ws 1, with individual "Ruby"
    assert review_map[(1, 1)] == ("approved", "Ruby")
    # pred 2 was pending in ws 2 -> absence (not in review_map). Pred 2 itself
    # is also gone (see dedupe comment above), which is why its workspace-2
    # pending state stays absent.
    assert (2, 2) not in review_map
    # pred 3 rejected in ws 1
    assert review_map[(3, 1)] == ("rejected", None)

    # detector_runs backfilled for every (photo, model)
    run_keys = {(r["photo_id"], r["detector_model"]) for r in db.conn.execute(
        "SELECT photo_id, detector_model FROM detector_runs"
    ).fetchall()}
    assert (10, "megadetector-v6") in run_keys
    assert (11, "megadetector-v6") in run_keys
