"""A schema migration never deletes what only a Process run can make again.

Detections, predictions, subject masks and the measurements taken from them
(``schema.DERIVED_DATA_TABLES``) cost hours of model time to rebuild, so a
migration that stops trusting some of them sets the rows aside
(``schema.set_aside_rows`` / ``set_aside_masks``) instead of deleting them.

Two things hold that:

* the migration runner refuses any ``DELETE`` or ``DROP TABLE`` the running
  migration has not declared in ``Migration.deletes`` (a SQLite authorizer,
  so it is checked when the statement is prepared, with the real table name);
* the tests here pin every declaration, and fail on one that names a
  derived-data table or a table whose deleted rows cascade into one.

To let a migration delete from a derived-data table, add the pair to
``DERIVED_DELETE_EXEMPTIONS`` with the reason the rows are rebuilt without the
user running anything. There are none today.
"""
import contextlib
import sqlite3

import pytest
import schema
from canonical_schema import SCHEMA_VERSION, SET_ASIDE_TABLES
from db import Database
from repositories.masks_features import SET_ASIDE_MASK_MSG
from services import photo_ingest, stage_counts, workspace_folders

# (migration version, table) -> why deleting derived data there is safe.
DERIVED_DELETE_EXEMPTIONS: dict[tuple[int, str], str] = {}

# Every delete a migration declares. A new entry here is a reviewed decision
# that the rows are not needed afterwards.
DECLARED_DELETES = {
    29: {"old_current_flag_attribution"},
    42: {"species_highlights", "species_choice_losers"},
    43: {"species_highlights", "species_choice_losers"},
    46: {"species_highlights", "species_choice_losers"},
    48: {"species_highlights", "species_choice_losers"},
    52: {"species_highlights", "species_choice_losers"},
}

BOX = (0.1, 0.2, 0.3, 0.4)


@pytest.fixture
def catalog(tmp_path):
    path = str(tmp_path / "vireo.db")
    schema.ensure_schema(path)
    return path


def _run(path, migration, monkeypatch):
    monkeypatch.setattr(schema, "MIGRATIONS", (migration,))
    schema.ensure_schema(path)


def _next(apply, **kwargs):
    return schema.Migration(SCHEMA_VERSION + 1, "next", apply, **kwargs)


# -- the guard -----------------------------------------------------------------


@pytest.mark.parametrize("statement", [
    "DELETE FROM photo_masks",
    "DELETE FROM detection_subjects WHERE detection_id IN (SELECT id FROM detections)",
    "DELETE FROM keywords WHERE 0",
    "DROP TABLE subject_focus",
    "DROP TABLE IF EXISTS photo_embeddings",
])
def test_migration_that_deletes_without_declaring_it_is_refused(catalog, monkeypatch, statement):
    def apply(conn):
        conn.execute("INSERT INTO db_meta(key, value) VALUES ('half_done', '1')")
        conn.execute(statement)

    with pytest.raises(schema.DestructiveMigrationError) as raised:
        _run(catalog, _next(apply), monkeypatch)

    table = statement.split()[-1] if "DROP" in statement else statement.split()[2]
    assert table in str(raised.value)
    with sqlite3.connect(catalog) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        assert conn.execute("SELECT 1 FROM db_meta WHERE key='half_done'").fetchone() is None
        assert conn.execute(
            "SELECT 1 FROM sqlite_master WHERE name IN ('subject_focus', 'photo_embeddings')"
        ).fetchall() == [(1,), (1,)]


def test_refusal_does_not_depend_on_a_row_matching_or_on_how_the_name_is_built(catalog, monkeypatch):
    def apply(conn):
        for table in ("detection_subjects",):
            conn.execute(f"DELETE FROM {table} WHERE detection_id = -1")

    with pytest.raises(schema.DestructiveMigrationError, match="set_aside_rows"):
        _run(catalog, _next(apply), monkeypatch)


def test_refusal_survives_a_migration_that_swallows_the_error(catalog, monkeypatch):
    def apply(conn):
        with contextlib.suppress(sqlite3.DatabaseError):
            conn.execute("DELETE FROM predictions")

    with pytest.raises(schema.DestructiveMigrationError, match="predictions"):
        _run(catalog, _next(apply), monkeypatch)
    with sqlite3.connect(catalog) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION


def test_declared_delete_temp_tables_and_trigger_deletes_pass(catalog, monkeypatch):
    def apply(conn):
        conn.execute("CREATE TABLE scratch_copy (id INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO scratch_copy VALUES (1)")
        conn.execute("DELETE FROM scratch_copy")
        conn.execute("DROP TABLE scratch_copy")
        conn.execute("CREATE TEMP TABLE working (id INTEGER)")
        conn.execute("DELETE FROM working")
        conn.execute("DROP TABLE working")
        # ``invalidate_current_flag_attribution`` deletes when a flag changes.
        conn.execute("UPDATE photos SET flag='flagged' WHERE 0")

    _run(catalog, _next(apply, deletes={"scratch_copy": "made and removed by this migration"}), monkeypatch)
    with sqlite3.connect(catalog) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION + 1


def test_guard_ends_with_the_migration_run(catalog):
    with Database(catalog) as db:
        db.conn.execute("PRAGMA user_version=57")
        schema._apply_pending(db.conn)
        db.conn.execute("DELETE FROM photo_masks")
        assert not schema._GUARDS


# -- the declarations ----------------------------------------------------------


def test_every_declared_delete_is_pinned_here_with_a_reason():
    declared = {m.version: set(m.deletes) for m in schema.MIGRATIONS if m.deletes}
    assert declared == DECLARED_DELETES, (
        "A migration's deletes changed. Deleting rows in a migration is a reviewed "
        "decision: update DECLARED_DELETES in this file with it."
    )
    for migration in schema.MIGRATIONS:
        for table, reason in migration.deletes.items():
            assert reason.strip(), f"migration {migration.version} gives no reason for deleting {table}"


def _cascade_targets(conn, table):
    """Tables that lose rows when a ``table`` row is deleted, through any chain of cascades."""
    children: dict[str, set[str]] = {}
    for (name,) in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall():
        for fk in conn.execute(f"PRAGMA foreign_key_list('{name}')").fetchall():
            if fk[6] == "CASCADE":
                children.setdefault(fk[2], set()).add(name)
    seen: set[str] = set()
    stack = [table]
    while stack:
        for child in children.get(stack.pop(), ()):
            if child not in seen:
                seen.add(child)
                stack.append(child)
    return seen


def test_no_migration_may_delete_derived_data(catalog):
    with sqlite3.connect(catalog) as conn:
        for migration in schema.MIGRATIONS:
            for table in migration.deletes:
                lost = ({table} | _cascade_targets(conn, table)) & schema.DERIVED_DATA_TABLES
                unexplained = {
                    name for name in lost
                    if not DERIVED_DELETE_EXEMPTIONS.get((migration.version, name), "").strip()
                }
                assert not unexplained, (
                    f"Migration {migration.version} ({migration.name}) deletes from {table}, "
                    f"which removes rows of {sorted(unexplained)}. Only a Process run makes "
                    "those again: set them aside with schema.set_aside_rows or "
                    "schema.set_aside_masks, or add the pair to DERIVED_DELETE_EXEMPTIONS "
                    "with the reason they are rebuilt without the user running anything."
                )
    stale = [key for key in DERIVED_DELETE_EXEMPTIONS
             if key[1] not in {m.version: m.deletes for m in schema.MIGRATIONS}.get(key[0], {})]
    assert not stale, f"DERIVED_DELETE_EXEMPTIONS names deletes no migration declares: {stale}"


def test_derived_data_tables_exist_and_set_aside_tables_hold_their_columns(catalog):
    with sqlite3.connect(catalog) as conn:
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        assert tables >= schema.DERIVED_DATA_TABLES
        assert set(SET_ASIDE_TABLES) <= schema.DERIVED_DATA_TABLES
        schema._validate_set_aside_schema(conn)
        for source, target in SET_ASIDE_TABLES.items():
            kept = {row[1] for row in conn.execute(f"PRAGMA table_info({target})")}
            assert kept >= {row[1] for row in conn.execute(f"PRAGMA table_info({source})")}, (
                f"{target} lacks a column of {source}: add it to create_set_aside_analysis "
                "and to a migration, or a set-aside would lose that value"
            )


# -- the migrations that used to delete -----------------------------------------


def _seed(path, tmp_path):
    """Three analyzed photos: a MegaDetector box, a reviewer box, and a RAW paired with a JPEG."""
    photos = {}
    with Database(path) as db:
        folder = workspace_folders.add_folder(db, str(tmp_path / "photos"))
        for name, model in (("detector", "megadetector-v6"), ("reviewer", "reviewer-selected"),
                            ("paired", "megadetector-v6")):
            pid = photo_ingest.add_photo(db, folder, f"{name}.jpg", ".jpg", 100, 1.0)
            db.commit()
            conn = db.conn
            detection = conn.execute(
                "INSERT INTO detections (photo_id, detector_model, runtime_fingerprint, box_x, box_y, "
                "box_w, box_h, detector_confidence, category) VALUES (?, ?, 'legacy', ?, ?, ?, ?, 0.9, 'animal')",
                (pid, model, *BOX),
            ).lastrowid
            mask_path = f"/masks/{name}.png"
            conn.execute(
                "INSERT INTO photo_masks (photo_id, variant, path, created_at, detector_model, prompt_x, "
                "prompt_y, prompt_w, prompt_h, subject_tenengrad) VALUES (?, 'sam2-base', ?, 0, ?, ?, ?, ?, ?, 5.5)",
                (pid, mask_path, model, *BOX),
            )
            conn.execute(
                "INSERT INTO photo_masks (photo_id, variant, path, created_at, detector_model, prompt_x, "
                "prompt_y, prompt_w, prompt_h) VALUES (?, 'sam2-large', ?, 0, ?, ?, ?, ?, ?)",
                (pid, f"/masks/{name}.large.png", model, *BOX),
            )
            conn.execute(
                "UPDATE photos SET mask_path=?, active_mask_variant='sam2-base', subject_tenengrad=5.5, "
                "quality_score=0.7, dino_subject_embedding=?, dino_embedding_variant='dinov2-base' WHERE id=?",
                (mask_path, name.encode(), pid),
            )
            conn.execute("INSERT INTO photo_subject_state (photo_id, detection_id) VALUES (?, ?)", (pid, detection))
            conn.execute(
                "INSERT INTO detection_subjects (detection_id, source_key, crop, quality_score, exposure_ev, "
                "features) VALUES (?, 'k1', '{}', 0.7, 0.0, '{}')", (detection,))
            conn.execute(
                "INSERT INTO subject_raw_analysis (detection_id, recipe, report_json, created_at) "
                "VALUES (?, 'r', '{}', 0)", (detection,))
            conn.execute(
                "INSERT INTO subject_ranking_analysis (detection_id, source_key, features, mask_png) "
                "VALUES (?, 'k1', '{}', ?)", (detection, b"png"))
            conn.execute(
                "INSERT INTO predictions (detection_id, classifier_model, species, confidence) "
                "VALUES (?, 'bioclip', 'Mallard', 0.9)", (detection,))
            photos[name] = (pid, detection)
        db.conn.execute("UPDATE photos SET companion_path='paired.nef.jpg' WHERE id=?", (photos["paired"][0],))
        db.commit()
    return photos


def _derived_counts(conn):
    """Rows of each derived-data table, counting the ones set aside."""
    counts = {}
    for table in sorted(schema.DERIVED_DATA_TABLES):
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        if table in SET_ASIDE_TABLES:
            count += conn.execute(f"SELECT COUNT(*) FROM {SET_ASIDE_TABLES[table]}").fetchone()[0]
        counts[table] = count
    return counts


def _migrate_from(path, version):
    with sqlite3.connect(path) as conn:
        conn.execute(f"PRAGMA user_version={version}")
    schema.ensure_schema(path)


@pytest.mark.parametrize("version", [12, 44, 46, 48, 49, 50, 53])
def test_migrating_an_analyzed_catalog_keeps_every_derived_row(catalog, tmp_path, version):
    _seed(catalog, tmp_path)
    with sqlite3.connect(catalog) as conn:
        before = _derived_counts(conn)
    assert all(before[table] for table in ("detections", "photo_masks", "predictions", *SET_ASIDE_TABLES))

    _migrate_from(catalog, version)

    with sqlite3.connect(catalog) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        assert _derived_counts(conn) == before


def test_detector_migration_sets_masks_and_measurements_aside(catalog, tmp_path):
    photos = _seed(catalog, tmp_path)
    pid, detection = photos["detector"]

    _migrate_from(catalog, 53)

    with sqlite3.connect(catalog) as conn:
        conn.row_factory = sqlite3.Row
        photo = conn.execute("SELECT * FROM photos WHERE id=?", (pid,)).fetchone()
        # Every reader sees the photo as not analyzed.
        assert (photo["mask_path"], photo["active_mask_variant"], photo["subject_tenengrad"],
                photo["quality_score"], photo["dino_subject_embedding"]) == (None,) * 5
        active, other = conn.execute(
            "SELECT * FROM photo_masks WHERE photo_id=? ORDER BY variant", (pid,)).fetchall()
        assert (active["set_aside_by"], active["set_aside_active"], active["set_aside_embedding"],
                active["set_aside_embedding_variant"], active["subject_tenengrad"]) == (
                    54, 1, b"detector", "dinov2-base", 5.5)
        assert "frame" in active["set_aside_reason"]
        assert (other["set_aside_by"], other["set_aside_active"], other["set_aside_embedding"]) == (54, 0, None)
        for source, target in SET_ASIDE_TABLES.items():
            assert conn.execute(f"SELECT 1 FROM {source} WHERE detection_id=?", (detection,)).fetchone() is None
            kept = conn.execute(f"SELECT * FROM {target} WHERE detection_id=?", (detection,)).fetchone()
            assert (kept["set_aside_by"], kept["set_aside_reason"]) == (54, active["set_aside_reason"])
        assert conn.execute(
            "SELECT quality_score FROM set_aside_detection_subjects WHERE detection_id=?", (detection,)
        ).fetchone()[0] == 0.7
        # The reviewer's photo is another migration's business.
        reviewer = conn.execute(
            "SELECT set_aside_by FROM photo_masks WHERE photo_id=?", (photos["reviewer"][0],)).fetchall()
        assert {row[0] for row in reviewer} == {None}


@pytest.mark.parametrize("version, by, photo", [
    (46, 47, "paired"), (49, 50, "paired"), (50, 51, "reviewer"),
])
def test_companion_and_reviewer_migrations_set_aside_instead_of_deleting(catalog, tmp_path, version, by, photo):
    photos = _seed(catalog, tmp_path)
    pid, detection = photos[photo]
    with sqlite3.connect(catalog) as conn:
        conn.execute(f"PRAGMA user_version={version}")
    with Database(catalog) as db:
        # Run only the migration under test.
        migration = next(m for m in schema.MIGRATIONS if m.version == by)
        guard_run = pytest.MonkeyPatch()
        guard_run.setattr(schema, "MIGRATIONS", (migration,))
        try:
            schema._apply_pending(db.conn)
        finally:
            guard_run.undo()
        row = db.conn.execute(
            "SELECT set_aside_by, set_aside_active, set_aside_embedding FROM photo_masks "
            "WHERE photo_id=? AND variant='sam2-base'", (pid,)).fetchone()
        assert tuple(row) == (by, 1, photo.encode())
        assert db.conn.execute("SELECT mask_path FROM photos WHERE id=?", (pid,)).fetchone()[0] is None
        assert db.conn.execute(
            "SELECT set_aside_by FROM set_aside_detection_subjects WHERE detection_id=?", (detection,)
        ).fetchone()[0] == by
        assert db.conn.execute(
            "SELECT COUNT(*) FROM detection_subjects WHERE detection_id=?", (detection,)).fetchone()[0] == 0


def test_migration_49_sets_every_box_measurement_aside(catalog, tmp_path):
    photos = _seed(catalog, tmp_path)
    with sqlite3.connect(catalog) as conn:
        conn.execute("PRAGMA user_version=48")
    with Database(catalog) as db:
        migration = next(m for m in schema.MIGRATIONS if m.version == 49)
        patch = pytest.MonkeyPatch()
        patch.setattr(schema, "MIGRATIONS", (migration,))
        try:
            schema._apply_pending(db.conn)
        finally:
            patch.undo()
        assert db.conn.execute("SELECT COUNT(*) FROM detection_subjects").fetchone()[0] == 0
        kept = db.conn.execute("SELECT detection_id, set_aside_by FROM set_aside_detection_subjects").fetchall()
        assert sorted(tuple(row) for row in kept) == sorted((d, 49) for _, d in photos.values())
        # Masks are not this migration's subject.
        assert db.conn.execute("SELECT COUNT(*) FROM photo_masks WHERE set_aside_by IS NOT NULL").fetchone()[0] == 0


def test_set_aside_is_not_repeated_by_a_later_migration(catalog, tmp_path):
    photos = _seed(catalog, tmp_path)
    pid, _ = photos["paired"]

    _migrate_from(catalog, 46)

    with sqlite3.connect(catalog) as conn:
        row = conn.execute(
            "SELECT set_aside_by, set_aside_active, set_aside_embedding FROM photo_masks "
            "WHERE photo_id=? AND variant='sam2-base'", (pid,)).fetchone()
    # Migration 54 also covers this MegaDetector mask, and finds it already
    # set aside: the first record keeps the embedding the photo had then.
    assert row == (47, 1, b"paired")


# -- using a set-aside mask again -------------------------------------------------


@pytest.fixture
def analyzed(db, tmp_path):
    """One photo with an active mask and embedding, as extraction leaves it."""
    folder = workspace_folders.add_folder(db, str(tmp_path / "photos"))
    pid = photo_ingest.add_photo(db, folder, "bird.jpg", ".jpg", 100, 1.0)
    db.detections.save(pid, [{
        "box": dict(zip("xywh", BOX, strict=True)), "confidence": 0.9, "category": "animal",
    }], detector_model="megadetector-v6")
    detection = db.detections.get(pid, 0)[0]
    db.masks_features.upsert_mask(pid, "sam2-base", "/masks/bird.png", "megadetector-v6", BOX)
    db.masks_features.set_active_variant(pid, "sam2-base", 0.2)
    db.conn.execute(
        "UPDATE photos SET dino_subject_embedding=?, dino_embedding_variant='dinov2-base' WHERE id=?",
        (b"embedding", pid))
    db.commit()
    return pid, detection["id"]


def _set_aside(db, pid):
    cleared = schema.set_aside_masks(db.conn, "m.photo_id=?", (pid,), version=99, reason="Test reason")
    db.commit()
    return cleared


def _state(db, pid):
    return tuple(db.conn.execute(
        "SELECT mask_path, active_mask_variant, dino_subject_embedding FROM photos WHERE id=?", (pid,)
    ).fetchone())


def test_set_aside_mask_is_kept_inactive_and_cannot_be_activated(db, analyzed):
    pid, _ = analyzed

    assert _set_aside(db, pid) == [pid]

    assert _state(db, pid) == (None, None, None)
    mask = db.masks_features.get_mask(pid, "sam2-base")
    assert (mask["path"], mask["set_aside_by"], mask["set_aside_reason"], mask["set_aside_active"],
            mask["set_aside_embedding"]) == ("/masks/bird.png", 99, "Test reason", 1, b"embedding")
    with pytest.raises(ValueError, match="set aside by an upgrade"):
        db.masks_features.set_active_variant(pid, "sam2-base", 0.2)
    assert str(SET_ASIDE_MASK_MSG).endswith("against the original")
    assert db.masks_features.count_set_aside() == [
        {"set_aside_by": 99, "set_aside_reason": "Test reason", "masks": 1, "active": 1}]


def test_proven_set_aside_mask_becomes_active_again_with_its_embedding(db, analyzed):
    pid, _ = analyzed
    before = _state(db, pid)
    _set_aside(db, pid)

    assert db.masks_features.reactivate_set_aside(pid, "sam2-base", 0.2) is True
    db.commit()

    assert _state(db, pid) == before
    mask = db.masks_features.get_mask(pid, "sam2-base")
    assert (mask["set_aside_by"], mask["set_aside_reason"], mask["set_aside_active"],
            mask["set_aside_embedding"], mask["set_aside_embedding_variant"]) == (None, None, 0, None, None)
    assert db.masks_features.count_set_aside() == []
    assert db.masks_features.reactivate_set_aside(pid, "sam2-base", 0.2) is False


def test_set_aside_mask_stays_aside_when_its_embedding_no_longer_fits(db, analyzed):
    pid, _ = analyzed
    _set_aside(db, pid)
    db.conn.execute("UPDATE photos SET dino_embedding_variant='dinov2-large' WHERE id=?", (pid,))

    assert db.masks_features.reactivate_set_aside(pid, "sam2-base", 0.2) is False
    assert _state(db, pid) == (None, None, None)
    assert db.masks_features.get_mask(pid, "sam2-base")["set_aside_by"] == 99


def test_set_aside_mask_that_was_not_active_stays_aside(db, analyzed):
    pid, _ = analyzed
    db.masks_features.upsert_mask(pid, "sam2-large", "/masks/bird.large.png", "megadetector-v6", BOX)
    db.commit()
    _set_aside(db, pid)

    assert db.masks_features.get_mask(pid, "sam2-large")["set_aside_active"] == 0
    assert db.masks_features.reactivate_set_aside(pid, "sam2-large", 0.2) is False
    assert _state(db, pid) == (None, None, None)


def test_set_aside_mask_of_another_subject_stays_aside(db, analyzed):
    pid, detection = analyzed
    _set_aside(db, pid)
    db.conn.execute("UPDATE detections SET box_x=0.5 WHERE id=?", (detection,))

    assert db.masks_features.reactivate_set_aside(pid, "sam2-base", 0.2) is False
    assert db.masks_features.get_mask(pid, "sam2-base")["set_aside_by"] == 99


def test_new_mask_replaces_a_set_aside_one(db, analyzed):
    pid, _ = analyzed
    _set_aside(db, pid)

    db.masks_features.upsert_mask(pid, "sam2-base", "/masks/bird.new.png", "megadetector-v6", BOX)
    db.masks_features.set_active_variant(pid, "sam2-base", 0.2)

    mask = db.masks_features.get_mask(pid, "sam2-base")
    assert (mask["path"], mask["set_aside_by"], mask["set_aside_active"], mask["set_aside_embedding"]) == (
        "/masks/bird.new.png", None, 0, None)


def test_plan_counts_set_aside_masks_among_the_pending_ones(db, analyzed):
    pid, _ = analyzed
    assert stage_counts.count_set_aside_masks(db, "sam2-base", detector_confidence=0.2) == 0

    _set_aside(db, pid)

    assert stage_counts.count_set_aside_masks(db, "sam2-base", detector_confidence=0.2) == 1
    assert stage_counts.count_photos_pending_masks(
        db, min_conf=0.2, sam2_variant="sam2-base")["pending"] == 1
    # Another variant's plan has nothing to reuse, and neither does a
    # photo whose subject moved since.
    assert stage_counts.count_set_aside_masks(db, "sam2-large", detector_confidence=0.2) == 0
    db.conn.execute("UPDATE detections SET box_x=0.5 WHERE photo_id=?", (pid,))
    assert stage_counts.count_set_aside_masks(db, "sam2-base", detector_confidence=0.2) == 0


def test_plan_text_names_the_set_aside_masks_among_the_work():
    from pipeline_plan import _extract_summary

    assert _extract_summary(5, 0, 0, 9, 3) == (
        "Will extract masks for 5 photos (9 eligible). 3 of them have a mask an upgrade set "
        "aside: each is checked against the original and reused without SAM if it still matches"
    )
    assert _extract_summary(5, 0, 0, 9) == "Will extract masks for 5 photos (9 eligible)"


def test_dropping_a_rebuilt_table_passes_once_declared(tmp_path):
    """Migration 29 rebuilds ``current_flag_attribution`` and drops the renamed original."""
    path = str(tmp_path / "vireo.db")
    schema.ensure_schema(path)
    with sqlite3.connect(path) as conn:
        conn.execute("DROP TRIGGER IF EXISTS invalidate_current_flag_attribution")
        conn.execute("DROP TABLE current_flag_attribution")
        conn.execute(
            "CREATE TABLE current_flag_attribution (photo_id INTEGER PRIMARY KEY, "
            "workspace_id INTEGER NOT NULL, flag TEXT NOT NULL, source TEXT, reason TEXT, "
            "description TEXT, created_at TEXT)")
        conn.execute("PRAGMA user_version=28")

    schema.ensure_schema(path)

    with sqlite3.connect(path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        nullable = {row[1]: not row[3] for row in conn.execute("PRAGMA table_info(current_flag_attribution)")}
        assert nullable["workspace_id"]
        assert conn.execute(
            "SELECT 1 FROM sqlite_master WHERE name='old_current_flag_attribution'").fetchone() is None
