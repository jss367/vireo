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
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 11
        assert conn.execute(
            "SELECT value FROM db_meta WHERE key='schema_manager'"
        ).fetchone()[0] == "registry-v1"
        assert [row[1] for row in conn.execute('PRAGMA table_info(location_gps_reviews)')] == [
            'photo_id', 'fingerprint', 'reviewed_at',
        ]


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

    migration = schema.Migration(12, "interrupted", fail_after_write)
    monkeypatch.setattr(schema, "MIGRATIONS", (*schema.MIGRATIONS, migration))

    with pytest.raises(RuntimeError, match="simulated interruption"):
        schema.ensure_schema(db_path)

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 11
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
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 11


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
        # Mask prompt matches the exact coordinates of a *legacy* row (id=101),
        # not the canonical survivor (id=100). Without a prompt remap the mask
        # would be flagged stale after the merge because 0.10001 != 0.10002.
        db.conn.execute(
            """
            INSERT INTO photo_masks (
              photo_id, variant, path, created_at, detector_model,
              prompt_x, prompt_y, prompt_w, prompt_h
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                photo_id, "sam2-small", "/masks/bird.png", 1,
                "MegaDetector", 0.10001, 0.2, 0.3, 0.4,
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

        mask_row = conn.execute(
            """
            SELECT detector_model, prompt_x, prompt_y, prompt_w, prompt_h
            FROM photo_masks WHERE photo_id = ?
            """,
            (photo_id,),
        ).fetchone()
        assert mask_row["detector_model"] == "megadetector-v6"
        # Prompt coords must be realigned to the survivor detection's exact
        # coordinates so find_stale_masks / count_extract_stale keep matching.
        assert mask_row["prompt_x"] == pytest.approx(0.10002)
        assert mask_row["prompt_y"] == pytest.approx(0.2)
        assert mask_row["prompt_w"] == pytest.approx(0.3)
        assert mask_row["prompt_h"] == pytest.approx(0.4)

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
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 11
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


def test_legacy_merge_realigns_masks_to_existing_survivor_row_coords(tmp_path):
    """When a content-addressed canonical row already occupies ``survivor_id`` and a
    lower-id canonical duplicate exists with different raw box coordinates, mask
    prompts must be realigned to the retained row's coordinates. Using the lower-id
    duplicate's coordinates (the previous behaviour) would leave every mask stale
    against ``find_stale_masks``/``count_extract_stale``.
    """
    db_path = str(tmp_path / "vireo.db")
    schema.ensure_schema(db_path)

    occupant_coords = (0.10002, 0.2, 0.3, 0.4)
    source_coords = (0.10001, 0.2, 0.3, 0.4)

    with Database(db_path, initialize_schema=False) as db:
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
        occupant_id = detection_id(
            photo_id, "megadetector-v6", occupant_coords, "animal",
        )
        # Guard: the retained row must have a non-trivial content-addressed id so
        # the lower-id duplicate wins `min(..., key=row.id)`.
        assert occupant_id != 100

        db.conn.executemany(
            """
            INSERT INTO detections (
              id, photo_id, detector_model, box_x, box_y, box_w, box_h,
              detector_confidence, category, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                # Lower-id canonical duplicate — becomes ``source``.
                (100, photo_id, "megadetector-v6",
                 *source_coords, 0.8, "animal", "2026-04-24T00:00:00"),
                # Existing content-addressed canonical row — becomes ``occupant``.
                (occupant_id, photo_id, "megadetector-v6",
                 *occupant_coords, 0.9, "animal", "2026-04-26T00:00:00"),
                # A legacy row so this group is included in the merge.
                (101, photo_id, "MegaDetector",
                 *source_coords, 0.85, "animal", "2026-04-23T00:00:00"),
            ],
        )
        # Mask stored at the retained row's exact coordinates. A remap targeting
        # ``source``'s coordinates would break equality against every remaining
        # detection.
        db.conn.execute(
            """
            INSERT INTO photo_masks (
              photo_id, variant, path, created_at, detector_model,
              prompt_x, prompt_y, prompt_w, prompt_h
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (photo_id, "sam2-small", "/masks/bird.png", 1,
             "megadetector-v6", *occupant_coords),
        )
        db.conn.commit()

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA user_version = 7")

    schema.ensure_schema(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        detections = conn.execute(
            "SELECT id, box_x, box_y, box_w, box_h FROM detections WHERE photo_id = ?",
            (photo_id,),
        ).fetchall()
        # Only the pre-existing content-addressed row remains.
        assert [r["id"] for r in detections] == [occupant_id]
        assert detections[0]["box_x"] == pytest.approx(occupant_coords[0])

        mask = conn.execute(
            "SELECT prompt_x, prompt_y, prompt_w, prompt_h FROM photo_masks WHERE photo_id = ?",
            (photo_id,),
        ).fetchone()
        assert (
            mask["prompt_x"],
            mask["prompt_y"],
            mask["prompt_w"],
            mask["prompt_h"],
        ) == pytest.approx(occupant_coords)

    with Database(db_path, initialize_schema=False) as migrated_db:
        assert migrated_db.find_stale_masks() == []


def test_legacy_merge_prompt_remap_leaves_other_detector_masks_alone(tmp_path):
    """The prompt remap must only touch MegaDetector-family masks.

    ``find_stale_masks`` requires both ``detector_model`` and ``prompt_xywh``
    equality with the photo's primary detection. If another model's mask
    happens to share the loser MegaDetector box's exact coordinates, blindly
    rewriting its prompt would leave the mask no longer matching any row for
    its own detector, so a valid cache entry would be flagged stale and
    deleted/re-extracted.
    """
    db_path = str(tmp_path / "vireo.db")
    schema.ensure_schema(db_path)

    canonical_coords = (0.10002, 0.2, 0.3, 0.4)
    legacy_coords = (0.10001, 0.2, 0.3, 0.4)

    with Database(db_path, initialize_schema=False) as db:
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
        db.conn.executemany(
            """
            INSERT INTO detections (
              id, photo_id, detector_model, box_x, box_y, box_w, box_h,
              detector_confidence, category, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                # MegaDetector alias pair to drive the merge — the canonical
                # row at ``canonical_coords`` survives and the legacy row at
                # ``legacy_coords`` becomes the loser.
                (100, photo_id, "megadetector-v6",
                 *canonical_coords, 0.8, "animal", "2026-04-26T00:00:00"),
                (101, photo_id, "MegaDetector",
                 *legacy_coords, 0.85, "animal", "2026-04-23T00:00:00"),
                # An unrelated detector's box that coincidentally sits at
                # the same coordinates as the loser MegaDetector row.
                # Its higher confidence makes it the photo's primary
                # detection, so find_stale_masks compares against it.
                (200, photo_id, "grounding-dino",
                 *legacy_coords, 0.95, "animal", "2026-04-24T00:00:00"),
            ],
        )
        db.conn.executemany(
            """
            INSERT INTO photo_masks (
              photo_id, variant, path, created_at, detector_model,
              prompt_x, prompt_y, prompt_w, prompt_h
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                # Legacy MegaDetector mask — must be realigned to the
                # canonical survivor's coordinates.
                (photo_id, "sam2-small", "/masks/bird-mega.png", 1,
                 "MegaDetector", *legacy_coords),
                # Non-MegaDetector mask that already matches its own
                # primary detection at ``legacy_coords``. The migration
                # must not touch this row.
                (photo_id, "sam2-large", "/masks/bird-dino.png", 1,
                 "grounding-dino", *legacy_coords),
            ],
        )
        db.conn.commit()

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA user_version = 7")

    schema.ensure_schema(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        masks = {
            row["variant"]: row
            for row in conn.execute(
                """
                SELECT variant, detector_model,
                       prompt_x, prompt_y, prompt_w, prompt_h
                FROM photo_masks WHERE photo_id = ?
                """,
                (photo_id,),
            )
        }
        mega = masks["sam2-small"]
        assert mega["detector_model"] == "megadetector-v6"
        assert (
            mega["prompt_x"], mega["prompt_y"],
            mega["prompt_w"], mega["prompt_h"],
        ) == pytest.approx(canonical_coords)

        dino = masks["sam2-large"]
        assert dino["detector_model"] == "grounding-dino"
        assert (
            dino["prompt_x"], dino["prompt_y"],
            dino["prompt_w"], dino["prompt_h"],
        ) == pytest.approx(legacy_coords)
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []

    with Database(db_path, initialize_schema=False) as migrated_db:
        # The grounding-dino mask matches its own primary detection and
        # must not be flagged stale by the alias merge. The MegaDetector
        # mask remains stale because the primary detection is dino, which
        # is a normal (unrelated) staleness — not caused by the merge.
        stale = migrated_db.find_stale_masks()
        stale_variants = {row["variant"] for row in stale}
        assert "sam2-large" not in stale_variants


def test_legacy_merge_review_metadata_follows_winning_row(tmp_path):
    """A conflicting prediction_review row must copy every merged column from
    the same source row that wins the status/timestamp comparison. Otherwise a
    newer rejected review from one row can end up alongside the older accepted
    row's ``individual``/``group_id``/vote counts, and grouped-accept logic
    then retags other photos with the loser row's metadata.
    """
    canonical_coords = (0.10002, 0.2, 0.3, 0.4)
    legacy_coords = (0.10001, 0.2, 0.3, 0.4)

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
        db.conn.executemany(
            """
            INSERT INTO detections (
              id, photo_id, detector_model, box_x, box_y, box_w, box_h,
              detector_confidence, category, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (100, photo_id, "megadetector-v6",
                 *canonical_coords, 0.9, "animal", "2026-04-26T00:00:00"),
                (101, photo_id, "MegaDetector",
                 *legacy_coords, 0.9, "animal", "2026-04-23T00:00:00"),
            ],
        )
        # Two predictions with matching (classifier_model, labels_fingerprint,
        # species) — one on the canonical detection, one on the legacy alias —
        # collapse to a single survivor prediction during the merge.
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
            ],
        )
        # Both predictions carry a prediction_review row for the SAME workspace,
        # so the merge UPSERT hits the ON CONFLICT branch. The legacy row (201)
        # has the more recent reviewed_at, so it wins the merge.
        db.conn.executemany(
            """
            INSERT INTO prediction_review (
              prediction_id, workspace_id, status, reviewed_at,
              individual, group_id, vote_count, total_votes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (200, workspace_id, "accepted", "2026-04-27T02:00:00",
                 "bob", "groupB", 3, 5),
                (201, workspace_id, "rejected", "2026-06-01T02:00:00",
                 "alice", "groupA", 1, 2),
            ],
        )
        db.conn.commit()

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA user_version = 7")

    schema.ensure_schema(db_path)

    survivor_detection_id = detection_id(
        photo_id, "megadetector-v6", canonical_coords, "animal",
    )
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        survivor_prediction = conn.execute(
            """
            SELECT id FROM predictions
            WHERE detection_id = ? AND species = 'Robin'
            """,
            (survivor_detection_id,),
        ).fetchone()
        assert survivor_prediction is not None
        review = conn.execute(
            """
            SELECT status, reviewed_at, individual, group_id,
                   vote_count, total_votes
            FROM prediction_review
            WHERE prediction_id = ? AND workspace_id = ?
            """,
            (survivor_prediction["id"], workspace_id),
        ).fetchone()
        # The legacy row wins on reviewed_at, so every merged field must come
        # from it — the auxiliary metadata cannot latch onto the accepted row.
        assert review["status"] == "rejected"
        assert review["reviewed_at"] == "2026-06-01T02:00:00"
        assert review["individual"] == "alice"
        assert review["group_id"] == "groupA"
        assert review["vote_count"] == 1
        assert review["total_votes"] == 2


def test_legacy_merge_review_pending_loses_to_decided_metadata(tmp_path):
    """When one review is 'pending' and the other is decided, the decided row
    wins the status merge; every other merged column must follow it, so a
    pending row's NULL metadata cannot outrank a decided row's group tag.
    """
    canonical_coords = (0.10002, 0.2, 0.3, 0.4)
    legacy_coords = (0.10001, 0.2, 0.3, 0.4)

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
        db.conn.executemany(
            """
            INSERT INTO detections (
              id, photo_id, detector_model, box_x, box_y, box_w, box_h,
              detector_confidence, category, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (100, photo_id, "megadetector-v6",
                 *canonical_coords, 0.9, "animal", "2026-04-26T00:00:00"),
                (101, photo_id, "MegaDetector",
                 *legacy_coords, 0.9, "animal", "2026-04-23T00:00:00"),
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
            ],
        )
        # Canonical (existing) is decided; legacy (excluded) is pending with a
        # newer timestamp. The decided row must still win — and carry its own
        # metadata even though the pending row's is NULL.
        db.conn.executemany(
            """
            INSERT INTO prediction_review (
              prediction_id, workspace_id, status, reviewed_at,
              individual, group_id, vote_count, total_votes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (200, workspace_id, "accepted", "2026-04-27T02:00:00",
                 "bob", "groupB", 3, 5),
                (201, workspace_id, "pending", "2026-06-01T02:00:00",
                 None, None, None, None),
            ],
        )
        db.conn.commit()

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA user_version = 7")

    schema.ensure_schema(db_path)

    survivor_detection_id = detection_id(
        photo_id, "megadetector-v6", canonical_coords, "animal",
    )
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        survivor_prediction = conn.execute(
            """
            SELECT id FROM predictions
            WHERE detection_id = ? AND species = 'Robin'
            """,
            (survivor_detection_id,),
        ).fetchone()
        review = conn.execute(
            """
            SELECT status, reviewed_at, individual, group_id,
                   vote_count, total_votes
            FROM prediction_review
            WHERE prediction_id = ? AND workspace_id = ?
            """,
            (survivor_prediction["id"], workspace_id),
        ).fetchone()
        # Decided beats pending regardless of timestamp; and every merged
        # column keeps the decided row's values (not COALESCE'd from pending).
        assert review["status"] == "accepted"
        assert review["reviewed_at"] == "2026-04-27T02:00:00"
        assert review["individual"] == "bob"
        assert review["group_id"] == "groupB"
        assert review["vote_count"] == 3
        assert review["total_votes"] == 5


def test_legacy_merge_rekeys_canonical_rows_without_content_ids(tmp_path):
    """Catalogs that ran the short-lived unversioned rename may already carry
    ``detector_model = 'megadetector-v6'`` yet keep the pre-rename rowid instead
    of the content-addressed id. The migration must re-key those rows even when
    no literal ``MegaDetector`` alias remains — otherwise the next detector
    rerun UPSERTs under the true content-addressed id and deletes the old
    canonical row, cascading its predictions/reviews.
    """
    box = (0.10002, 0.2, 0.3, 0.4)
    other_box = (0.6, 0.2, 0.2, 0.2)

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
        expected_primary_id = detection_id(
            photo_id, "megadetector-v6", box, "animal",
        )
        expected_other_id = detection_id(
            photo_id, "megadetector-v6", other_box, "animal",
        )
        # Guard: force ids that cannot collide with the true content-addressed
        # values, so the migration is genuinely re-keying.
        assert 500 not in (expected_primary_id, expected_other_id)
        assert 501 not in (expected_primary_id, expected_other_id)

        db.conn.executemany(
            """
            INSERT INTO detections (
              id, photo_id, detector_model, box_x, box_y, box_w, box_h,
              detector_confidence, category, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (500, photo_id, "megadetector-v6",
                 *box, 0.9, "animal", "2026-04-26T00:00:00"),
                (501, photo_id, "megadetector-v6",
                 *other_box, 0.7, "animal", "2026-04-26T00:00:00"),
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
                (600, 500, "BioCLIP-2.5", "birds", "Robin",
                 0.9, "match", "2026-04-26T01:00:00"),
                (601, 501, "BioCLIP-2.5", "birds", "Hawk",
                 0.6, "new", "2026-04-26T01:00:00"),
            ],
        )
        db.conn.executemany(
            """
            INSERT INTO prediction_review (
              prediction_id, workspace_id, status, reviewed_at
            ) VALUES (?, ?, ?, ?)
            """,
            [
                (600, workspace_id, "accepted", "2026-04-27T02:00:00"),
                (601, workspace_id, "accepted", "2026-04-27T02:01:00"),
            ],
        )
        db.conn.commit()

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA user_version = 7")

    schema.ensure_schema(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        detections = conn.execute(
            "SELECT id, detector_model FROM detections WHERE photo_id = ? ORDER BY id",
            (photo_id,),
        ).fetchall()
        assert {r["id"] for r in detections} == {
            expected_primary_id, expected_other_id,
        }
        assert {r["detector_model"] for r in detections} == {"megadetector-v6"}

        # Reviews and predictions must remain attached to the re-keyed detections.
        surviving_predictions = conn.execute(
            """
            SELECT p.species, r.status
            FROM predictions p
            JOIN prediction_review r
              ON r.prediction_id = p.id AND r.workspace_id = ?
            WHERE p.detection_id IN (?, ?)
            ORDER BY p.species
            """,
            (workspace_id, expected_primary_id, expected_other_id),
        ).fetchall()
        assert [(r["species"], r["status"]) for r in surviving_predictions] == [
            ("Hawk", "accepted"),
            ("Robin", "accepted"),
        ]

    # After the migration, rerunning the detector must be a no-op that keeps
    # every prediction/review — this is the regression the reviewer flagged:
    # without re-keying, `write_detection_batch` would delete the old row.
    with Database(db_path, initialize_schema=False) as migrated_db:
        rerun_ids = migrated_db.write_detection_batch(
            photo_id,
            "megadetector-v6",
            [
                {
                    "box": {"x": box[0], "y": box[1], "w": box[2], "h": box[3]},
                    "confidence": 0.9,
                    "category": "animal",
                },
                {
                    "box": {"x": other_box[0], "y": other_box[1],
                            "w": other_box[2], "h": other_box[3]},
                    "confidence": 0.7,
                    "category": "animal",
                },
            ],
        )
        assert set(rerun_ids) == {expected_primary_id, expected_other_id}
        remaining = migrated_db.conn.execute(
            """
            SELECT COUNT(*)
            FROM predictions p
            JOIN prediction_review r ON r.prediction_id = p.id
            WHERE p.detection_id IN (?, ?) AND r.status = 'accepted'
            """,
            (expected_primary_id, expected_other_id),
        ).fetchone()[0]
        assert remaining == 2


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
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 11


def test_legacy_merge_prompt_remap_skips_when_other_detection_matches(tmp_path):
    """Skip the mask prompt realignment when another retained megadetector-v6
    detection on the same photo still sits at the loser's exact coordinates.

    ``find_stale_masks`` compares only ``(detector_model, prompt_xywh)`` and
    ignores category, so a mask that already matches that other detection
    would remain fresh against it. Rewriting the mask to this alias group's
    survivor coordinates would silently point it at a different subject and
    turn a valid cache entry stale.
    """
    canonical_coords = (0.10002, 0.2, 0.3, 0.4)
    legacy_coords = (0.10001, 0.2, 0.3, 0.4)

    db_path = str(tmp_path / "vireo.db")
    schema.ensure_schema(db_path)

    with Database(db_path, initialize_schema=False) as db:
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
        # Unrelated megadetector-v6 detection whose coords equal the loser's
        # (e.g. same box under a different category). Its id must fall outside
        # ``legacy_detection_merge`` so it counts as retained.
        other_detection_id = detection_id(
            photo_id, "megadetector-v6", legacy_coords, "person",
        )
        db.conn.executemany(
            """
            INSERT INTO detections (
              id, photo_id, detector_model, box_x, box_y, box_w, box_h,
              detector_confidence, category, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                # MegaDetector alias pair — canonical survives, legacy loses.
                (100, photo_id, "megadetector-v6",
                 *canonical_coords, 0.8, "animal", "2026-04-26T00:00:00"),
                (101, photo_id, "MegaDetector",
                 *legacy_coords, 0.85, "animal", "2026-04-23T00:00:00"),
                # Retained megadetector-v6 row co-located with the loser.
                (other_detection_id, photo_id, "megadetector-v6",
                 *legacy_coords, 0.95, "person", "2026-04-24T00:00:00"),
            ],
        )
        # Mask stored under the loser's exact coordinates, model
        # ``megadetector-v6`` (so both the ``other`` retained detection and
        # the loser row share its ``(detector_model, prompt_xywh)`` identity).
        db.conn.execute(
            """
            INSERT INTO photo_masks (
              photo_id, variant, path, created_at, detector_model,
              prompt_x, prompt_y, prompt_w, prompt_h
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (photo_id, "sam2-small", "/masks/bird.png", 1,
             "megadetector-v6", *legacy_coords),
        )
        db.conn.commit()

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA user_version = 7")

    schema.ensure_schema(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        mask = conn.execute(
            """
            SELECT detector_model, prompt_x, prompt_y, prompt_w, prompt_h
            FROM photo_masks WHERE photo_id = ?
            """,
            (photo_id,),
        ).fetchone()
        # The mask must stay pinned to the loser coordinates because the
        # retained ``other`` detection still matches it. Remapping to the
        # alias survivor would make it stale against every remaining row.
        assert mask["detector_model"] == "megadetector-v6"
        assert (
            mask["prompt_x"], mask["prompt_y"],
            mask["prompt_w"], mask["prompt_h"],
        ) == pytest.approx(legacy_coords)
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []


def test_legacy_merge_prompt_remap_moves_when_other_detection_is_not_primary(tmp_path):
    """Remap the mask to the survivor coords when a retained megadetector-v6
    row at the loser coordinates is lower-confidence than the merged survivor.

    ``find_stale_masks`` picks a single primary detection per photo — the
    highest-confidence non-full-image row, tie-broken by smallest id — and
    only that row's coordinates keep the mask fresh. If a lower-confidence
    retained detection happens to sit at the loser coords, the alias survivor
    is still the primary; leaving the mask pinned to the loser coords would
    immediately flag it stale, so the migration must move it to the survivor.
    """
    canonical_coords = (0.20002, 0.3, 0.4, 0.5)
    legacy_coords = (0.20001, 0.3, 0.4, 0.5)

    db_path = str(tmp_path / "vireo.db")
    schema.ensure_schema(db_path)

    with Database(db_path, initialize_schema=False) as db:
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
        # A retained megadetector-v6 row co-located with the loser but at a
        # confidence lower than the merged alias survivor. It is NOT the
        # post-merge primary, so leaving the mask at its coords would make it
        # stale against the true primary (the alias survivor).
        other_detection_id = detection_id(
            photo_id, "megadetector-v6", legacy_coords, "person",
        )
        db.conn.executemany(
            """
            INSERT INTO detections (
              id, photo_id, detector_model, box_x, box_y, box_w, box_h,
              detector_confidence, category, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                # MegaDetector alias pair — survivor's max confidence is 0.9.
                (200, photo_id, "megadetector-v6",
                 *canonical_coords, 0.85, "animal", "2026-04-26T00:00:00"),
                (201, photo_id, "MegaDetector",
                 *legacy_coords, 0.9, "animal", "2026-04-23T00:00:00"),
                # Retained non-primary detection at the loser coords.
                (other_detection_id, photo_id, "megadetector-v6",
                 *legacy_coords, 0.3, "person", "2026-04-24T00:00:00"),
            ],
        )
        db.conn.execute(
            """
            INSERT INTO photo_masks (
              photo_id, variant, path, created_at, detector_model,
              prompt_x, prompt_y, prompt_w, prompt_h
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (photo_id, "sam2-small", "/masks/bird.png", 1,
             "megadetector-v6", *legacy_coords),
        )
        db.conn.commit()

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA user_version = 7")

    schema.ensure_schema(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        mask = conn.execute(
            """
            SELECT detector_model, prompt_x, prompt_y, prompt_w, prompt_h
            FROM photo_masks WHERE photo_id = ?
            """,
            (photo_id,),
        ).fetchone()
        # Migrated: the mask must move to the survivor coordinates because
        # the retained detection at the loser coords is not the primary.
        assert mask["detector_model"] == "megadetector-v6"
        assert (
            mask["prompt_x"], mask["prompt_y"],
            mask["prompt_w"], mask["prompt_h"],
        ) == pytest.approx(canonical_coords)

        # And find_stale_masks agrees: the mask is fresh against the
        # post-merge primary (the alias survivor) at its new prompt coords.
        stale_ids = conn.execute(
            """
            SELECT pm.rowid FROM photo_masks pm
            WHERE NOT EXISTS (
              SELECT 1 FROM detections d
              WHERE d.id = (
                SELECT d2.id FROM detections d2
                WHERE d2.photo_id = pm.photo_id
                  AND d2.detector_model != 'full-image'
                ORDER BY d2.detector_confidence DESC, d2.id ASC
                LIMIT 1
              )
                AND d.detector_model = pm.detector_model
                AND d.box_x = pm.prompt_x AND d.box_y = pm.prompt_y
                AND d.box_w = pm.prompt_w AND d.box_h = pm.prompt_h
            )
            """,
        ).fetchall()
        assert stale_ids == []
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []


def test_legacy_merge_null_species_predictions_collapse_to_one_row(tmp_path):
    """A migrated legacy/canonical pair whose classifier rows carry
    ``species IS NULL`` must produce exactly one NULL-species prediction on
    the survivor, not two.

    SQLite treats NULLs as distinct in UNIQUE constraints, so the main
    prediction INSERT's ON CONFLICT branch never fires for NULL species and
    a naive UPSERT would insert a duplicate row. ``legacy_prediction_merge``
    then maps review/history to ``MIN(new_p.id)`` and the extra row is left
    unmapped — surfacing as an additional cached prediction with the loser's
    (potentially higher) confidence.
    """
    canonical_coords = (0.10002, 0.2, 0.3, 0.4)
    legacy_coords = (0.10001, 0.2, 0.3, 0.4)

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
        db.conn.executemany(
            """
            INSERT INTO detections (
              id, photo_id, detector_model, box_x, box_y, box_w, box_h,
              detector_confidence, category, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (100, photo_id, "megadetector-v6",
                 *canonical_coords, 0.9, "animal", "2026-04-26T00:00:00"),
                (101, photo_id, "MegaDetector",
                 *legacy_coords, 0.9, "animal", "2026-04-23T00:00:00"),
            ],
        )
        # Both classifier rows have species=NULL — the ambiguous "no
        # confident label" output that classifiers cache alongside a
        # confidence score. Same classifier_model/labels_fingerprint means
        # they represent the same run identity on their respective detections.
        db.conn.executemany(
            """
            INSERT INTO predictions (
              id, detection_id, classifier_model, labels_fingerprint,
              species, confidence, category, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (200, 100, "BioCLIP-2.5", "birds", None,
                 0.4, "match", "2026-04-26T01:00:00"),
                (201, 101, "BioCLIP-2.5", "birds", None,
                 0.9, "match", "2026-04-23T01:00:00"),
            ],
        )
        db.conn.execute(
            """
            INSERT INTO prediction_review (
              prediction_id, workspace_id, status, reviewed_at
            ) VALUES (?, ?, ?, ?)
            """,
            (201, workspace_id, "accepted", "2026-04-27T02:00:00"),
        )
        db.conn.commit()

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA user_version = 7")

    schema.ensure_schema(db_path)

    survivor_detection_id = detection_id(
        photo_id, "megadetector-v6", canonical_coords, "animal",
    )
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT id, species, confidence, created_at
            FROM predictions
            WHERE detection_id = ?
              AND classifier_model = 'BioCLIP-2.5'
              AND labels_fingerprint = 'birds'
            """,
            (survivor_detection_id,),
        ).fetchall()
        # Exactly one NULL-species row survives on the merged detection.
        assert len(rows) == 1
        survivor_prediction = rows[0]
        assert survivor_prediction["species"] is None
        # Winning confidence and earliest created_at are merged in.
        assert survivor_prediction["confidence"] == pytest.approx(0.9)
        assert survivor_prediction["created_at"] == "2026-04-23T01:00:00"

        # The legacy prediction's review must carry over to the surviving row.
        review = conn.execute(
            """
            SELECT status FROM prediction_review
            WHERE prediction_id = ? AND workspace_id = ?
            """,
            (survivor_prediction["id"], workspace_id),
        ).fetchone()
        assert review is not None
        assert review["status"] == "accepted"
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []


def test_ensure_schema_backs_up_before_pending_migrations(tmp_path):
    """An existing DB with pending registry migrations is snapshotted before
    any migration runs, so a bad migration can't destroy the only copy."""
    import os

    db_path = str(tmp_path / "vireo.db")
    schema.ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA user_version = 7")

    schema.ensure_schema(db_path)

    latest = schema.MIGRATIONS[-1].version
    backup_path = f"{db_path}.pre-v{latest}.bak"
    assert os.path.exists(backup_path)
    # The snapshot reflects the pre-migration state.
    with sqlite3.connect(backup_path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 7
        assert conn.execute("SELECT COUNT(*) FROM workspaces").fetchone()[0] >= 1


def test_ensure_schema_no_backup_for_fresh_or_current_db(tmp_path):
    """No snapshot for a brand-new DB, and none when the schema is already
    at the latest version (normal startup must not accrete backups)."""
    import glob

    db_path = str(tmp_path / "vireo.db")
    schema.ensure_schema(db_path)
    assert glob.glob(f"{db_path}*.bak") == []

    schema.ensure_schema(db_path)
    assert glob.glob(f"{db_path}*.bak") == []


def test_ensure_schema_prunes_older_backups(tmp_path):
    """Only the most recent pre-migration snapshot is kept."""
    import os

    db_path = str(tmp_path / "vireo.db")
    schema.ensure_schema(db_path)
    latest = schema.MIGRATIONS[-1].version
    stale_backup = f"{db_path}.pre-v{latest - 1}.bak"
    with open(stale_backup, "w") as f:
        f.write("old snapshot")
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA user_version = 7")

    schema.ensure_schema(db_path)

    assert os.path.exists(f"{db_path}.pre-v{latest}.bak")
    assert not os.path.exists(stale_backup)


def test_ensure_schema_keeps_older_backups_when_snapshot_fails(tmp_path, monkeypatch):
    """If the pre-migration snapshot can't be written (VACUUM INTO fails,
    volume full, etc.), an older `.pre-v*.bak` from a previous successful
    run must survive — otherwise the upgrade completes with no recovery
    snapshot at all."""
    import os

    db_path = str(tmp_path / "vireo.db")
    schema.ensure_schema(db_path)
    latest = schema.MIGRATIONS[-1].version
    stale_backup = f"{db_path}.pre-v{latest - 1}.bak"
    with open(stale_backup, "w") as f:
        f.write("older snapshot")
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA user_version = 7")

    # Simulate _snapshot_before_migrations failing silently (its except
    # branch already swallows sqlite3.Error / OSError and logs a warning).
    monkeypatch.setattr(schema, "_snapshot_before_migrations", lambda *a, **k: None)

    schema.ensure_schema(db_path)

    assert not os.path.exists(f"{db_path}.pre-v{latest}.bak")
    assert os.path.exists(stale_backup)


def test_ensure_schema_newer_db_raises_incompatible_database_error(tmp_path):
    """Opening a DB stamped by a newer Vireo raises the friendly
    IncompatibleDatabaseError (caught by main's guided-exit handler) instead
    of an anonymous RuntimeError crash — and does not mutate the file."""
    from db import IncompatibleDatabaseError

    db_path = str(tmp_path / "vireo.db")
    schema.ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA user_version = 99")

    with pytest.raises(IncompatibleDatabaseError) as excinfo:
        schema.ensure_schema(db_path)

    assert "newer" in str(excinfo.value)
    assert excinfo.value.db_path == db_path
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 99


def test_load_taxonomy_cli_refuses_newer_db(tmp_path, monkeypatch, capsys):
    """``--load-taxonomy`` must not run legacy DDL against a catalog stamped
    by a newer Vireo. Previously it bypassed ensure_schema and constructed
    Database(args.db) directly, mutating the file and burying the version
    mismatch in an OperationalError; the guided-exit handler should now fire
    the same structured signal here as it does for the normal startup path."""
    import json as _json

    import app as vireo_app

    db_path = str(tmp_path / "vireo.db")
    schema.ensure_schema(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA user_version = 99")

    monkeypatch.setattr(
        "sys.argv",
        ["vireo", "--db", db_path, "--thumb-dir", str(tmp_path / "thumbs"),
         "--load-taxonomy"],
    )
    # Guard against actually touching the DB or the network if the fix
    # regresses and control ever reaches Database(args.db)/load_taxonomy.
    def _boom(*_a, **_kw):  # pragma: no cover - regression guard
        raise AssertionError("--load-taxonomy touched the DB before ensure_schema")

    monkeypatch.setattr("db.Database", _boom)

    with pytest.raises(SystemExit) as excinfo:
        vireo_app.main()
    assert excinfo.value.code == 3

    captured = capsys.readouterr()
    payload = _json.loads(captured.err.strip().splitlines()[-1])
    assert payload["error"] == "incompatible_database"
    assert payload["newer"] is True
    assert payload["db_path"] == db_path

    with sqlite3.connect(db_path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 99


def test_ensure_schema_keeps_later_version_snapshots(tmp_path):
    """Restoring an older live database while a later-version backup remains
    (e.g. `.pre-v9.bak` beside a v7 file, then launching this v8 build) must
    keep the newer snapshot — it may hold the user's only copy of edits made
    under that later schema. Only strictly older snapshots may be pruned."""
    import os

    db_path = str(tmp_path / "vireo.db")
    schema.ensure_schema(db_path)
    latest = schema.MIGRATIONS[-1].version
    older_backup = f"{db_path}.pre-v{latest - 1}.bak"
    newer_backup = f"{db_path}.pre-v{latest + 1}.bak"
    with open(older_backup, "w") as f:
        f.write("older snapshot")
    with open(newer_backup, "w") as f:
        f.write("later-version snapshot")
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA user_version = 7")

    schema.ensure_schema(db_path)

    assert os.path.exists(f"{db_path}.pre-v{latest}.bak")
    assert not os.path.exists(older_backup)
    # The later-version backup must survive — it may be irreplaceable.
    assert os.path.exists(newer_backup)


def test_split_grouping_history_snapshots_migration(tmp_path):
    """v10 moves inline pipeline_grouping snapshots to edit_history_payloads.

    Each such row carried the workspace's whole before/after encounter
    lists in ``edit_history.new_value``, so every undo-status poll and
    history prune scanned those blobs.
    """
    import json

    from services.grouping_history import load_grouping_change

    db_path = str(tmp_path / "vireo.db")
    snapshot = {
        "before": [{"photo_ids": [1, 2, 3], "bursts": [{"photo_ids": [1, 2, 3]}]}],
        "after": [
            {"photo_ids": [1, 2], "bursts": [{"photo_ids": [1, 2]}]},
            {"photo_ids": [3], "bursts": [{"photo_ids": [3]}]},
        ],
    }
    photo_edit = {"action_type": "flag", "new_value": "flagged"}
    with Database(db_path) as db:
        ws_id = db._ws_id()
        db.conn.execute(
            "INSERT INTO edit_history (workspace_id, action_type, description, new_value) "
            "VALUES (?, 'pipeline_grouping', 'Detached burst', ?)",
            (ws_id, json.dumps({**snapshot, "photo_edit": photo_edit})),
        )
        db.conn.execute(
            "INSERT INTO edit_history (workspace_id, action_type, description, new_value) "
            "VALUES (?, 'pipeline_grouping', 'Retired', ?)",
            (ws_id, json.dumps({"photo_edit": photo_edit, "photo_only": True})),
        )
        db.conn.execute(
            "INSERT INTO edit_history (workspace_id, action_type, description, new_value) "
            "VALUES (?, 'flag', 'Set flag to flagged', 'flagged')",
            (ws_id,),
        )
        db.conn.commit()
        db.conn.execute("PRAGMA user_version = 9")

    schema.ensure_schema(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 11
        rows = conn.execute(
            "SELECT eh.id, eh.action_type, eh.new_value, p.payload "
            "FROM edit_history eh LEFT JOIN edit_history_payloads p ON p.edit_id = eh.id "
            "ORDER BY eh.id"
        ).fetchall()
        split, retired, flag = rows
        assert json.loads(split["new_value"]) == {"photo_edit": photo_edit}
        assert json.loads(split["payload"]) == snapshot
        # Already-small rows are left alone.
        assert json.loads(retired["new_value"]) == {"photo_edit": photo_edit, "photo_only": True}
        assert retired["payload"] is None
        assert flag["new_value"] == "flagged" and flag["payload"] is None
        indexes = {r[1] for r in conn.execute("PRAGMA index_list(edit_history)")}
        assert "idx_edit_history_ws_undone_created" in indexes
        item_indexes = {r[1] for r in conn.execute("PRAGMA index_list(edit_history_items)")}
        assert "idx_edit_history_items_edit" in item_indexes

    with Database(db_path, initialize_schema=False) as db:
        entry = db.conn.execute(
            "SELECT * FROM edit_history WHERE description = 'Detached burst'"
        ).fetchone()
        assert load_grouping_change(db, entry) == {**snapshot, "photo_edit": photo_edit}
        # Deleting the history row removes its snapshot with it.
        db.conn.execute("DELETE FROM edit_history WHERE id = ?", (entry["id"],))
        db.conn.commit()
        assert db.conn.execute(
            "SELECT COUNT(*) FROM edit_history_payloads"
        ).fetchone()[0] == 0
