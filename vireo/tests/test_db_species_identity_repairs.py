"""Behavior pins for the species-identity repair domain of ``Database``.

``species_identity_repair`` and ``label_source_identities`` decide what to
change and own their transactions; the SQL they run lives in
``repositories/species_identity_repairs.py`` behind the
``db.species_identity_repairs`` accessor. These tests pin each repository
method on a real catalog, check that none of them commits, and keep both
modules (and ``species_identity``) free of ``.conn`` uses.
"""

import ast
import inspect
import json
import sqlite3
import textwrap
from pathlib import Path

import pytest
from db import Database
from repositories.species_identity_repairs import SpeciesIdentityRepairRepository
from services import photo_ingest


@pytest.fixture
def det(db, tmp_path):
    folder = db.add_folder(str(tmp_path), name="Photos")
    pid = photo_ingest.add_photo(db, folder, "photo.jpg", ".jpg", 1000, 1.0, timestamp="2026-09-01T10:00:00")
    return db.detections.save(pid, [{"box": {"x": .1, "y": .1, "w": .5, "h": .5},
                                     "confidence": .99}], detector_model="megadetector-v6")[0]


def _prediction(db, det, species, model, fingerprint, scientific_name=None):
    taxonomy = {"scientific_name": scientific_name} if scientific_name else None
    db.predictions.add(det, species, .9, model, labels_fingerprint=fingerprint, taxonomy=taxonomy)
    return db.conn.execute(
        "SELECT * FROM predictions WHERE species = ? AND classifier_model = ?", (species, model),
    ).fetchone()


def _ids(rows):
    return sorted(r["id"] for r in rows)


# -- candidate reads ------------------------------------------------------------


def test_bioclip_correction_candidates_select_custom_unstamped_rows(db, det):
    custom = _prediction(db, det, "Red-crowned Amazon", "BioCLIP-2.5", "custom", "Wrong name")
    _prediction(db, det, "Red-crowned Amazon", "BioCLIP-2", "tol", "Wrong name")
    _prediction(db, det, "Red-crowned Amazon", "iNat21", "custom", "Wrong name")
    _prediction(db, det, "Red-crowned Amazon", "BioCLIP-3", "custom", "Amazona viridigenalis")

    rows = db.species_identity_repairs.bioclip_correction_candidates(
        "red-crowned amazon", "Amazona viridigenalis",
    )

    assert _ids(rows) == [custom["id"]]
    assert set(rows[0].keys()) == {
        "id", "species", "detection_id", "classifier_model", "labels_fingerprint",
        "scientific_name", "source_taxon_id",
    }


def test_unstamped_reads_by_fingerprint_and_legacy(db, det):
    fp = _prediction(db, det, "Robin", "BioCLIP-2.5", "abc123")
    legacy = _prediction(db, det, "Robin", "BioCLIP-2", "legacy")
    _prediction(db, det, "Robin", "BioCLIP-3", "legacy", "Turdus migratorius")
    _prediction(db, det, "Wren", "iNat21", "legacy")
    repairs = db.species_identity_repairs

    assert _ids(repairs.unstamped_predictions_for_fingerprint("abc123")) == [fp["id"]]
    assert repairs.has_unstamped_prediction_for_fingerprint("abc123")
    assert not repairs.has_unstamped_prediction_for_fingerprint("other")
    assert _ids(repairs.unstamped_legacy_predictions()) == [legacy["id"]]
    assert repairs.has_unstamped_legacy_prediction()
    assert repairs.unstamped_legacy_species() == ["Robin"]


# -- applying a change ----------------------------------------------------------


def test_stamp_record_retire_and_clear(db, det):
    row = _prediction(db, det, "Robin", "BioCLIP-2.5", "abc123")
    db.conn.execute(
        "INSERT INTO classifier_runs (detection_id, classifier_model, labels_fingerprint, runtime_fingerprint) "
        "VALUES (?, 'BioCLIP-2.5', 'abc123', 'fresh')", (det,),
    )
    db.conn.execute("UPDATE workspaces SET last_group_fingerprint = 'fp'")
    db.commit()
    repairs = db.species_identity_repairs
    planned = {k: row[k] for k in (
        "scientific_name", "source_taxon_id", "species", "classifier_model",
        "labels_fingerprint", "detection_id",
    )}

    assert repairs.stamp_prediction_identity(row["id"], "Turdus migratorius", 12727, **planned) == 1
    # A second stamp from the same (now stale) plan matches nothing.
    assert repairs.stamp_prediction_identity(row["id"], "Turdus migratorius", 12727, **planned) == 0
    repairs.record_repair(row["id"], "rid", '{"b": 1}', '{"a": 2}', "why")
    repairs.retire_classifier_runs(det, "BioCLIP-2.5", "abc123")
    repairs.clear_group_fingerprints()

    stamped = db.conn.execute(
        "SELECT scientific_name, source_taxon_id FROM predictions WHERE id = ?", (row["id"],),
    ).fetchone()
    assert tuple(stamped) == ("Turdus migratorius", 12727)
    audit = db.conn.execute(
        "SELECT prediction_id, resolution_identity, before_json, after_json, reason "
        "FROM species_identity_repairs"
    ).fetchall()
    assert [tuple(r) for r in audit] == [(row["id"], "rid", '{"b": 1}', '{"a": 2}', "why")]
    assert db.conn.execute(
        "SELECT runtime_fingerprint FROM classifier_runs WHERE detection_id = ?", (det,),
    ).fetchone()[0] == "legacy"
    assert db.conn.execute(
        "SELECT COUNT(*) FROM workspaces WHERE last_group_fingerprint IS NOT NULL"
    ).fetchone()[0] == 0
    db.rollback()


# -- label-list identities ------------------------------------------------------


def test_label_fingerprints_skip_the_sentinels_in_order(db):
    for fp in ("zz", "tol", "aa", "legacy"):
        db.conn.execute(
            "INSERT INTO labels_fingerprints (fingerprint, sources_json) VALUES (?, ?)", (fp, f'["{fp}"]'),
        )
    rows = db.species_identity_repairs.label_fingerprints("tol", "legacy")
    assert [tuple(r) for r in rows] == [("aa", '["aa"]'), ("zz", '["zz"]')]


def test_label_identities_and_markers(db):
    repairs = db.species_identity_repairs
    repairs.record_label_identities([("fp1", "Robin", 12727, "Turdus migratorius")])
    repairs.record_label_identities([("fp1", "Robin", 12727, "Turdus migratorius ")])
    assert [tuple(r) for r in repairs.label_identities()] == [
        ("Robin", 12727, "Turdus migratorius "),
    ]
    db.meta.set("label_source_identities:fp1", json.dumps({"unidentified": ["Redhead"]}))
    db.meta.set("label_source_identities:legacy", "{}")
    db.meta.set("other", "x")
    assert repairs.marker_values("label_source_identities:", "label_source_identities:legacy") == [
        json.dumps({"unidentified": ["Redhead"]}),
    ]


def test_repository_writes_never_commit(db, det):
    row = _prediction(db, det, "Robin", "BioCLIP-2.5", "abc123")
    repairs = db.species_identity_repairs
    repairs.record_label_identities([("fp1", "Robin", 12727, "Turdus migratorius")])
    repairs.record_repair(row["id"], "rid", "{}", "{}", "why")
    repairs.clear_group_fingerprints()
    assert db.in_transaction
    path = db.conn.execute("PRAGMA database_list").fetchone()[2]
    other = sqlite3.connect(path)
    try:
        assert other.execute("SELECT COUNT(*) FROM label_source_identities").fetchone()[0] == 0
        assert other.execute("SELECT COUNT(*) FROM species_identity_repairs").fetchone()[0] == 0
    finally:
        other.close()
    db.rollback()


# -- structure ------------------------------------------------------------------


def test_species_identity_repairs_is_a_fresh_repository_on_the_connection_per_access(db):
    first, second = db.species_identity_repairs, db.species_identity_repairs
    assert isinstance(first, SpeciesIdentityRepairRepository)
    assert first is not second
    assert first.conn is db.conn


def test_accessor_builds_through_the_factory_not_the_connection():
    accessor = Database.__dict__["species_identity_repairs"]
    assert isinstance(accessor, property)
    source = textwrap.dedent(inspect.getsource(accessor.fget))
    attrs = {
        node.attr
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }
    assert "_species_identity_repair_repository" in attrs
    assert "conn" not in attrs


@pytest.mark.parametrize(
    "module", ["species_identity.py", "species_identity_repair.py", "label_source_identities.py"],
)
def test_identity_modules_run_no_sql_on_a_database_connection(module):
    path = Path(__file__).resolve().parent.parent / module
    tree = ast.parse(path.read_text(encoding="utf-8"))
    uses = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute) and node.attr == "conn"
    ]
    assert uses == []
