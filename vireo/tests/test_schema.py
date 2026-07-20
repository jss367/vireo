import sqlite3
import threading

import pytest
import schema
from db import Database
from detection_id import detection_id


def test_ensure_schema_applies_registry_and_validation(tmp_path):
    db_path = str(tmp_path / "vireo.db")

    schema.ensure_schema(db_path)

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 8
        assert conn.execute(
            "SELECT value FROM db_meta WHERE key='schema_manager'"
        ).fetchone()[0] == "registry-v1"


def test_initialized_connection_does_not_run_schema_creation(tmp_path, monkeypatch):
    db_path = str(tmp_path / "vireo.db")
    schema.ensure_schema(db_path)

    def fail_if_called(_self):
        raise AssertionError("request connection attempted schema initialization")

    monkeypatch.setattr(Database, "_create_tables", fail_if_called)
    with Database(db_path, initialize_schema=False) as db:
        assert db._active_workspace_id is not None


def test_failed_registry_migration_rolls_back_version_and_data(tmp_path, monkeypatch):
    db_path = str(tmp_path / "vireo.db")
    schema.ensure_schema(db_path)

    def fail_after_write(conn):
        conn.execute(
            "INSERT INTO db_meta(key, value) VALUES ('partial_migration', 'bad')"
        )
        raise RuntimeError("simulated interruption")

    migration = schema.Migration(9, "interrupted", fail_after_write)
    monkeypatch.setattr(schema, "MIGRATIONS", (*schema.MIGRATIONS, migration))

    with pytest.raises(RuntimeError, match="simulated interruption"):
        schema.ensure_schema(db_path)

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 8
        assert conn.execute(
            "SELECT 1 FROM db_meta WHERE key='partial_migration'"
        ).fetchone() is None


def test_concurrent_schema_startup_is_serialized(tmp_path):
    db_path = str(tmp_path / "vireo.db")
    errors = []

    def initialize():
        try:
            schema.ensure_schema(db_path)
        except Exception as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    threads = [threading.Thread(target=initialize) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert not errors
    assert all(not thread.is_alive() for thread in threads)
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 8


def test_navigation_restore_changes_only_consolidated_default(tmp_path):
    import json

    db_path = str(tmp_path / "vireo.db")
    with Database(db_path) as db:
        workspace_id = db._active_workspace_id
        db.conn.execute(
            "UPDATE workspaces SET tabs=? WHERE id=?",
            (json.dumps(schema._PRIMARY_WORKFLOW_TABS), workspace_id),
        )
        custom_id = db.create_workspace("Custom")
        db.conn.execute(
            "UPDATE workspaces SET tabs=? WHERE id=?",
            (json.dumps(["browse", "logs"]), custom_id),
        )
        db.conn.commit()

    # Reproduce a database that completed the now-reverted migration 6.
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO db_meta(key, value) VALUES (?, ?)",
            ("navigation_consolidated", "1"),
        )
        conn.execute("PRAGMA user_version = 6")

    schema.ensure_schema(db_path)

    with sqlite3.connect(db_path) as conn:
        rows = dict(conn.execute("SELECT name, tabs FROM workspaces").fetchall())
    assert json.loads(rows["Default"]) == schema._LEGACY_DEFAULT_TABS
    assert json.loads(rows["Custom"]) == ["browse", "logs"]
    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            "SELECT value FROM db_meta WHERE key='navigation_consolidated'"
        ).fetchone()[0] == "0"


def test_navigation_restore_only_touches_workspaces_v6_changed(tmp_path):
    """Preserve user-customized workspaces even when their tabs match the compact set.

    When migration 6 recorded which workspaces it rewrote, migration 7 must
    revert only those rows. A workspace the user manually customized to the
    same four-tab set (or that already matched it before v6 ran) was never
    touched by v6 and must not be clobbered by the restore.
    """
    import json

    db_path = str(tmp_path / "vireo.db")
    with Database(db_path) as db:
        default_id = db._active_workspace_id
        db.conn.execute(
            "UPDATE workspaces SET tabs=? WHERE id=?",
            (json.dumps(schema._PRIMARY_WORKFLOW_TABS), default_id),
        )
        # A second workspace whose tabs happen to match the compact set but
        # that v6 never modified (e.g. the user set it manually).
        untouched_id = db.create_workspace("UserCompact")
        db.conn.execute(
            "UPDATE workspaces SET tabs=? WHERE id=?",
            (json.dumps(schema._PRIMARY_WORKFLOW_TABS), untouched_id),
        )
        db.conn.commit()

    # Reproduce a database that completed v6 with per-row tracking of the
    # single workspace it actually changed.
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO db_meta(key, value) VALUES (?, ?)",
            ("navigation_consolidated", "1"),
        )
        conn.execute(
            "INSERT OR REPLACE INTO db_meta(key, value) VALUES (?, ?)",
            ("navigation_consolidated_ids", json.dumps([default_id])),
        )
        conn.execute("PRAGMA user_version = 6")

    schema.ensure_schema(db_path)

    with sqlite3.connect(db_path) as conn:
        rows = dict(conn.execute("SELECT name, tabs FROM workspaces").fetchall())
    assert json.loads(rows["Default"]) == schema._LEGACY_DEFAULT_TABS
    assert json.loads(rows["UserCompact"]) == schema._PRIMARY_WORKFLOW_TABS
    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            "SELECT value FROM db_meta WHERE key='navigation_consolidated'"
        ).fetchone()[0] == "0"
        assert conn.execute(
            "SELECT value FROM db_meta WHERE key='navigation_consolidated_ids'"
        ).fetchone() is None


def test_navigation_consolidation_records_changed_ids(tmp_path, monkeypatch):
    """Migration 6 stores the workspace IDs it rewrites so v7 can be precise."""
    import json

    db_path = str(tmp_path / "vireo.db")
    with Database(db_path) as db:
        default_id = db._active_workspace_id
        db.conn.execute(
            "UPDATE workspaces SET tabs=? WHERE id=?",
            (json.dumps(schema._LEGACY_DEFAULT_TABS), default_id),
        )
        custom_id = db.create_workspace("Custom")
        db.conn.execute(
            "UPDATE workspaces SET tabs=? WHERE id=?",
            (json.dumps(["browse", "logs"]), custom_id),
        )
        db.conn.commit()

    # Run only migrations up through v6 so we can observe exactly what
    # v6 records before v7 would clear it.
    monkeypatch.setattr(
        schema,
        "MIGRATIONS",
        tuple(m for m in schema.MIGRATIONS if m.version <= 6),
    )
    schema.ensure_schema(db_path)

    with sqlite3.connect(db_path) as conn:
        recorded = conn.execute(
            "SELECT value FROM db_meta WHERE key='navigation_consolidated_ids'"
        ).fetchone()
        marker = conn.execute(
            "SELECT value FROM db_meta WHERE key='navigation_consolidated'"
        ).fetchone()
    assert marker is not None and marker[0] == "1"
    assert recorded is not None
    assert json.loads(recorded[0]) == [default_id]


def test_legacy_megadetector_alias_merge_preserves_predictions_and_reviews(tmp_path):
    """Skipped detector-key upgrades must not duplicate subjects or lose decisions."""
    import json

    db_path = str(tmp_path / "vireo.db")
    schema.ensure_schema(db_path)

    with Database(db_path, initialize_schema=False) as db:
        workspace_id = db._active_workspace_id
        folder_id = db.add_folder(str(tmp_path / "photos"), name="photos")
        photo_id = db.add_photo(
            folder_id,
            "bird.jpg",
            ".jpg",
            1,
            1.0,
            timestamp="2026-01-01T00:00:00",
            width=100,
            height=100,
        )
        empty_photo_id = db.add_photo(
            folder_id,
            "empty.jpg",
            ".jpg",
            1,
            1.0,
            timestamp="2026-01-02T00:00:00",
            width=100,
            height=100,
        )
        db.conn.executemany(
            """
            INSERT INTO detections (
              id, photo_id, detector_model, box_x, box_y, box_w, box_h,
              detector_confidence, category, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (100, photo_id, "megadetector-v6", 0.10002, 0.2, 0.3, 0.4, 0.8, "animal", "2026-04-26T00:00:00"),
                (101, photo_id, "MegaDetector", 0.10001, 0.2, 0.3, 0.4, 0.9, "animal", "2026-04-23T00:00:00"),
                # A second pre-global-cache row for the same legacy box.
                (102, photo_id, "MegaDetector", 0.10003, 0.2, 0.3, 0.4, 0.85, "animal", "2026-04-23T00:01:00"),
                # Legacy-only geometry must survive under the canonical name.
                (103, photo_id, "MegaDetector", 0.6, 0.2, 0.2, 0.2, 0.7, "animal", "2026-04-23T00:02:00"),
            ],
        )
        db.conn.executemany(
            """
            INSERT INTO predictions (
              id, detection_id, classifier_model, labels_fingerprint,
              species, confidence, category, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (200, 100, "BioCLIP-2.5", "birds", "Robin", 0.8, "match", "2026-04-26T01:00:00"),
                (201, 101, "BioCLIP-2.5", "birds", "Robin", 0.9, "match", "2026-04-23T01:00:00"),
                (202, 102, "iNat21", "tol", "Sparrow", 0.7, "conflict", "2026-04-23T01:01:00"),
                (203, 103, "BioCLIP-2.5", "birds", "Hawk", 0.6, "new", "2026-04-23T01:02:00"),
            ],
        )
        db.conn.executemany(
            """
            INSERT INTO prediction_review (
              prediction_id, workspace_id, status, reviewed_at
            ) VALUES (?, ?, ?, ?)
            """,
            [
                (200, workspace_id, "pending", "2026-04-26T02:00:00"),
                (201, workspace_id, "accepted", "2026-04-27T02:00:00"),
                (202, workspace_id, "accepted", "2026-04-27T02:01:00"),
                (203, workspace_id, "accepted", "2026-04-27T02:02:00"),
            ],
        )
        db.conn.executemany(
            """
            INSERT INTO classifier_runs (
              detection_id, classifier_model, labels_fingerprint,
              run_at, prediction_count
            ) VALUES (?, ?, ?, ?, ?)
            """,
            [
                (100, "BioCLIP-2.5", "birds", "2026-04-26T03:00:00", 1),
                (101, "BioCLIP-2.5", "birds", "2026-04-27T03:00:00", 1),
                (102, "iNat21", "tol", "2026-04-27T03:01:00", 1),
            ],
        )
        db.conn.executemany(
            """
            INSERT INTO detector_runs (
              photo_id, detector_model, run_at, box_count
            ) VALUES (?, ?, ?, ?)
            """,
            [
                (photo_id, "MegaDetector", "2026-04-23T04:00:00", 3),
                (photo_id, "megadetector-v6", "2026-04-26T04:00:00", 1),
                (empty_photo_id, "MegaDetector", "2026-04-23T04:01:00", 0),
            ],
        )
        db.conn.execute(
            """
            INSERT INTO photo_masks (
              photo_id, variant, path, created_at, detector_model,
              prompt_x, prompt_y, prompt_w, prompt_h
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                photo_id, "sam2-small", "/masks/bird.png", 1,
                "MegaDetector", 0.10002, 0.2, 0.3, 0.4,
            ),
        )
        db.conn.executemany(
            """
            INSERT INTO edit_history (
              id, workspace_id, action_type, description, new_value
            ) VALUES (?, ?, ?, ?, ?)
            """,
            [
                (300, workspace_id, "prediction_accept", "accepted Robin", "1"),
                (301, workspace_id, "keyword_add", "added Sparrow", "1"),
                (302, workspace_id, "prediction_accept", "accepted subject Sparrow", "1"),
            ],
        )
        db.conn.executemany(
            """
            INSERT INTO edit_history_items (
              id, edit_id, photo_id, old_value, new_value
            ) VALUES (?, ?, ?, ?, ?)
            """,
            [
                (400, 300, photo_id, "201", "1"),
                (
                    401,
                    301,
                    photo_id,
                    json.dumps(
                        {
                            "prediction_id": 202,
                            "prediction_status": "pending",
                        }
                    ),
                    "1",
                ),
                (
                    402,
                    302,
                    photo_id,
                    json.dumps(
                        {
                            "prediction_ids": [201, 202, 203, 999],
                            "no_tag": True,
                        }
                    ),
                    "1",
                ),
            ],
        )
        db.conn.commit()

    # Reproduce a catalog that has completed v7 but skipped the old, unversioned
    # detector-key normalization.
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA user_version = 7")

    schema.ensure_schema(db_path)

    primary_detection_id = detection_id(
        photo_id, "megadetector-v6", (0.10002, 0.2, 0.3, 0.4), "animal",
    )
    hawk_detection_id = detection_id(
        photo_id, "megadetector-v6", (0.6, 0.2, 0.2, 0.2), "animal",
    )
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        detections = conn.execute(
            """
            SELECT id, detector_model, detector_confidence, box_x
            FROM detections WHERE photo_id = ? ORDER BY id
            """,
            (photo_id,),
        ).fetchall()
        assert {r["id"] for r in detections} == {
            primary_detection_id, hawk_detection_id,
        }
        assert {r["detector_model"] for r in detections} == {"megadetector-v6"}
        primary = next(r for r in detections if r["id"] == primary_detection_id)
        assert primary["box_x"] == pytest.approx(0.10002)
        assert primary["detector_confidence"] == pytest.approx(0.9)

        predictions = conn.execute(
            """
            SELECT p.id, p.detection_id, p.species, p.confidence, r.status
            FROM predictions p
            LEFT JOIN prediction_review r
              ON r.prediction_id = p.id AND r.workspace_id = ?
            WHERE p.detection_id IN (?, ?)
            ORDER BY p.species
            """,
            (workspace_id, primary_detection_id, hawk_detection_id),
        ).fetchall()
        by_species = {r["species"]: r for r in predictions}
        assert set(by_species) == {"Hawk", "Robin", "Sparrow"}
        assert by_species["Robin"]["confidence"] == pytest.approx(0.9)
        assert by_species["Robin"]["status"] == "accepted"
        assert by_species["Sparrow"]["detection_id"] == primary_detection_id
        assert by_species["Sparrow"]["status"] == "accepted"
        assert by_species["Hawk"]["detection_id"] == hawk_detection_id
        assert by_species["Hawk"]["status"] == "accepted"

        bare_history = conn.execute("SELECT old_value FROM edit_history_items WHERE id = 400").fetchone()[0]
        json_history = json.loads(conn.execute("SELECT old_value FROM edit_history_items WHERE id = 401").fetchone()[0])
        subject_history = json.loads(conn.execute("SELECT old_value FROM edit_history_items WHERE id = 402").fetchone()[0])
        assert bare_history == str(by_species["Robin"]["id"])
        assert json_history["prediction_id"] == by_species["Sparrow"]["id"]
        assert subject_history == {
            "prediction_ids": [
                by_species["Robin"]["id"],
                by_species["Sparrow"]["id"],
                by_species["Hawk"]["id"],
                999,
            ],
            "no_tag": True,
        }

        mask_model = conn.execute(
            "SELECT detector_model FROM photo_masks WHERE photo_id = ?",
            (photo_id,),
        ).fetchone()[0]
        assert mask_model == "megadetector-v6"

        detector_runs = conn.execute(
            """
            SELECT photo_id, detector_model, box_count
            FROM detector_runs
            WHERE photo_id IN (?, ?)
            ORDER BY photo_id
            """,
            (photo_id, empty_photo_id),
        ).fetchall()
        assert [(r["photo_id"], r["detector_model"], r["box_count"]) for r in detector_runs] == [
            (photo_id, "megadetector-v6", 2),
            (empty_photo_id, "megadetector-v6", 0),
        ]
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 8
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []

    with Database(db_path, initialize_schema=False) as migrated_db:
        assert migrated_db.find_stale_masks() == []
        rerun_ids = migrated_db.write_detection_batch(
            photo_id,
            "megadetector-v6",
            [
                {
                    "box": {"x": 0.10002, "y": 0.2, "w": 0.3, "h": 0.4},
                    "confidence": 0.9,
                    "category": "animal",
                },
                {
                    "box": {"x": 0.6, "y": 0.2, "w": 0.2, "h": 0.2},
                    "confidence": 0.7,
                    "category": "animal",
                },
            ],
        )
        assert set(rerun_ids) == {primary_detection_id, hawk_detection_id}
        remaining = migrated_db.conn.execute(
            """
            SELECT COUNT(*)
            FROM predictions p
            JOIN prediction_review r ON r.prediction_id = p.id
            WHERE p.detection_id IN (?, ?) AND r.status = 'accepted'
            """,
            (primary_detection_id, hawk_detection_id),
        ).fetchone()[0]
        assert remaining == 3


def test_legacy_megadetector_zero_box_run_is_normalized_without_detections(tmp_path):
    """A legacy empty-scene run has no detection row to drive the main merge."""
    db_path = str(tmp_path / "vireo.db")
    schema.ensure_schema(db_path)

    with Database(db_path, initialize_schema=False) as db:
        folder_id = db.add_folder(str(tmp_path / "photos"), name="photos")
        photo_id = db.add_photo(
            folder_id,
            "empty.jpg",
            ".jpg",
            1,
            1.0,
            timestamp="2026-01-02T00:00:00",
            width=100,
            height=100,
        )
        db.conn.execute(
            """
            INSERT INTO detector_runs (
              photo_id, detector_model, run_at, box_count
            ) VALUES (?, 'MegaDetector', '2026-04-23T04:01:00', 0)
            """,
            (photo_id,),
        )
        db.conn.commit()

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA user_version = 7")

    schema.ensure_schema(db_path)

    with sqlite3.connect(db_path) as conn:
        assert conn.execute(
            """
            SELECT detector_model, box_count FROM detector_runs
            WHERE photo_id = ?
            """,
            (photo_id,),
        ).fetchone() == ("megadetector-v6", 0)
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 8
