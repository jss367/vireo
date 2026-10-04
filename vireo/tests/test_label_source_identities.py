"""Taxon identities recovered for label lists saved before lists recorded them."""

import json
from unittest.mock import patch

import pytest
from db import Database
from job_summaries import describe_result
from label_source_identities import MARKER_PREFIX, backfill, pending_label_sets
from labels import SpeciesLabels, fetch_species_list, load_merged_labels
from labels_fingerprint import compute_fingerprint
from species_identity import SpeciesResolver

REDHEAD = {"taxon_id": 7056, "scientific_name": "Aythya americana",
           "common_name": "Redhead", "rank": "species"}
POCHARD = {"taxon_id": 7054, "scientific_name": "Aythya ferina",
           "common_name": "Common Pochard", "rank": "species"}
MALLARD = {"taxon_id": 6930, "scientific_name": "Anas platyrhynchos",
           "common_name": "Mallard", "rank": "species"}


@pytest.fixture
def db(tmp_path):
    database = Database(str(tmp_path / "photos.db"))
    for entry in (REDHEAD, POCHARD, MALLARD):
        database.conn.execute(
            "INSERT INTO taxa (inat_id, name, common_name, rank) VALUES (?, ?, ?, ?)",
            (entry["taxon_id"], entry["scientific_name"], entry["common_name"], entry["rank"]),
        )
    # A verified taxonomy that recorded "Redhead" as an alternate name of the
    # Common Pochard too, so the bare label cannot be resolved by name.
    database.set_meta("common_name_identity_version", "1")
    database.set_meta("ambiguous_common_names", json.dumps(["redhead"]))
    database.conn.commit()
    yield database
    database.close()


@pytest.fixture
def legacy_list(tmp_path, monkeypatch, db):
    """A label list saved before lists recorded identities, as classified."""
    labels_dir = tmp_path / "labels"
    labels_dir.mkdir()
    monkeypatch.setattr("labels.LABELS_DIR", str(labels_dir))
    path = labels_dir / "california-birds.txt"
    path.write_text("Mallard\nRedhead\n", encoding="utf-8")
    (labels_dir / "california-birds.json").write_text(json.dumps({
        "name": "California birds", "place_id": 14, "place_name": "California",
        "taxon_groups": ["birds"], "observation_filter": "research",
        "species_count": 2, "labels_file": str(path),
    }))
    labels = load_merged_labels([{"labels_file": str(path)}])
    fingerprint = compute_fingerprint(labels)
    db.upsert_labels_fingerprint(fingerprint, path.name, [str(path)], len(labels))
    return {"path": path, "fingerprint": fingerprint}


def _detection(db, tmp_path, name):
    folder = db.add_folder(str(tmp_path), name="Photos")
    pid = db.add_photo(folder, name, ".jpg", 1000, 1.0, timestamp="2026-09-01T10:00:00")
    return db.save_detections(pid, [{"box": {"x": .1, "y": .1, "w": .5, "h": .5},
                                     "confidence": .99}], detector_model="megadetector-v6")[0]


def _fetch(names, identities):
    calls = []

    def fetch(place_id, taxon_groups, observation_filter, strict=False):
        calls.append((place_id, tuple(taxon_groups), observation_filter, strict))
        return SpeciesLabels(names, identities)

    fetch.calls = calls
    return fetch


def _row(db, prediction_id):
    return db.conn.execute("SELECT * FROM predictions WHERE id = ?", (prediction_id,)).fetchone()


def _prediction_id(db, det, model, fingerprint):
    return db.conn.execute(
        "SELECT id FROM predictions WHERE detection_id = ? AND classifier_model = ? "
        "AND labels_fingerprint = ?", (det, model, fingerprint),
    ).fetchone()["id"]


def test_backfill_puts_a_legacy_label_and_the_native_prediction_under_one_species(db, tmp_path, legacy_list):
    fp = legacy_list["fingerprint"]
    det = _detection(db, tmp_path, "duck.jpg")
    db.add_prediction(det, "Redhead", .51, "BioCLIP-2.5", labels_fingerprint=fp,
                      taxonomy={"scientific_name": POCHARD["scientific_name"]})
    db.add_prediction(det, "Redhead", .5, "iNat21 (EVA-02 Large)", labels_fingerprint="tol",
                      taxonomy={"scientific_name": REDHEAD["scientific_name"]})
    bioclip = _prediction_id(db, det, "BioCLIP-2.5", fp)
    native = _prediction_id(db, det, "iNat21 (EVA-02 Large)", "tol")
    resolver = SpeciesResolver(db=db)
    assert resolver.prediction(_row(db, bioclip)).key != resolver.prediction(_row(db, native)).key

    fetch = _fetch(["Mallard", "Redhead"], {"Mallard": MALLARD, "Redhead": REDHEAD})
    result = backfill(db, fetch=fetch)

    assert fetch.calls == [(14, ("birds",), "research", True)]
    assert result["ok"] is True
    assert result["labels_identified"] == 2
    assert result["predictions_updated"] == 1
    row = _row(db, bioclip)
    assert row["species"] == "Redhead"
    assert row["confidence"] == .51
    assert row["source_taxon_id"] == REDHEAD["taxon_id"]
    assert row["scientific_name"] == REDHEAD["scientific_name"]
    resolver = SpeciesResolver(db=db)
    assert resolver.prediction(row).key == resolver.prediction(_row(db, native)).key == "taxon:7056"
    audit = db.conn.execute(
        "SELECT before_json, after_json, reason FROM species_identity_repairs WHERE prediction_id = ?",
        (bioclip,),
    ).fetchone()
    assert json.loads(audit["before_json"])["scientific_name"] == POCHARD["scientific_name"]
    assert json.loads(audit["after_json"])["source_taxon_id"] == REDHEAD["taxon_id"]
    assert audit["reason"] == "label-list-source-identity"


def test_backfill_does_not_change_the_label_list_or_its_fingerprint(db, tmp_path, legacy_list):
    det = _detection(db, tmp_path, "duck.jpg")
    db.add_prediction(det, "Redhead", .51, "BioCLIP-2.5", labels_fingerprint=legacy_list["fingerprint"])
    meta_path = legacy_list["path"].with_suffix(".json")
    before = (legacy_list["path"].read_bytes(), meta_path.read_bytes())

    backfill(db, fetch=_fetch(["Redhead"], {"Redhead": REDHEAD}))

    assert (legacy_list["path"].read_bytes(), meta_path.read_bytes()) == before
    labels = load_merged_labels([{"labels_file": str(legacy_list["path"])}])
    assert compute_fingerprint(labels) == legacy_list["fingerprint"]


def test_rows_written_after_the_backfill_get_the_identity_from_every_write_path(db, tmp_path, legacy_list):
    fp = legacy_list["fingerprint"]
    first = _detection(db, tmp_path, "one.jpg")
    db.add_prediction(first, "Redhead", .6, "BioCLIP-2.5", labels_fingerprint=fp)
    backfill(db, fetch=_fetch(["Redhead"], {"Redhead": REDHEAD}))

    later = _detection(db, tmp_path, "two.jpg")
    db.add_prediction(later, "Redhead", .7, "BioCLIP-2.5", labels_fingerprint=fp)
    assert _row(db, _prediction_id(db, later, "BioCLIP-2.5", fp))["source_taxon_id"] == 7056

    # The classify job's own INSERT OR REPLACE path.
    db.conn.execute(
        "INSERT OR REPLACE INTO predictions (detection_id, classifier_model, labels_fingerprint, "
        "species, confidence) VALUES (?, 'BioCLIP-2.5', ?, 'Redhead', .8)", (later, fp),
    )
    assert _row(db, _prediction_id(db, later, "BioCLIP-2.5", fp))["source_taxon_id"] == 7056

    # A refresh that rewrites the output without a taxonomy.
    db.add_prediction(first, "Redhead", .65, "BioCLIP-2.5", labels_fingerprint=fp, refresh_output=True)
    refreshed = _row(db, _prediction_id(db, first, "BioCLIP-2.5", fp))
    assert refreshed["confidence"] == .65
    assert refreshed["source_taxon_id"] == 7056

    # Other label sets and explicit source identities are untouched.
    db.add_prediction(later, "Redhead", .4, "BioCLIP-2.5", labels_fingerprint="0123456789ab")
    assert _row(db, _prediction_id(db, later, "BioCLIP-2.5", "0123456789ab"))["source_taxon_id"] is None


def test_backfill_runs_once_per_label_set(db, tmp_path, legacy_list):
    det = _detection(db, tmp_path, "duck.jpg")
    db.add_prediction(det, "Redhead", .51, "BioCLIP-2.5", labels_fingerprint=legacy_list["fingerprint"])
    backfill(db, fetch=_fetch(["Redhead"], {"Redhead": REDHEAD}))
    marker = json.loads(db.get_meta(MARKER_PREFIX + legacy_list["fingerprint"]))
    assert marker == {"labels": 2, "identified": 1, "predictions_updated": 1, "unidentified": ["Mallard"]}

    assert pending_label_sets(db) == []
    second = _fetch(["Redhead"], {"Redhead": REDHEAD})
    assert backfill(db, fetch=second)["label_sets"] == 0
    assert second.calls == []


def test_a_name_two_taxa_share_in_the_place_stays_unidentified(db, tmp_path, legacy_list):
    det = _detection(db, tmp_path, "duck.jpg")
    db.add_prediction(det, "Redhead", .51, "BioCLIP-2.5", labels_fingerprint=legacy_list["fingerprint"])
    # The fetch qualifies a shared name, so the bare legacy prompt has no
    # single taxon behind it.
    fetch = _fetch(
        ["Mallard", "Redhead (Aythya americana)", "Redhead (Aythya ferina)"],
        {"Mallard": MALLARD, "Redhead (Aythya americana)": REDHEAD,
         "Redhead (Aythya ferina)": POCHARD},
    )
    result = backfill(db, fetch=fetch)

    assert result["labels_identified"] == 1
    assert result["unidentified_labels"] == ["Redhead"]
    assert result["predictions_updated"] == 0
    assert _row(db, _prediction_id(db, det, "BioCLIP-2.5", legacy_list["fingerprint"]))["source_taxon_id"] is None


def test_a_list_edited_since_classification_is_not_matched(db, tmp_path, legacy_list):
    det = _detection(db, tmp_path, "duck.jpg")
    db.add_prediction(det, "Redhead", .51, "BioCLIP-2.5", labels_fingerprint=legacy_list["fingerprint"])
    legacy_list["path"].write_text("Mallard\nRedhead\nCanvasback\n", encoding="utf-8")

    assert pending_label_sets(db) == []


def test_an_unreachable_list_is_reported_and_retried_next_time(db, tmp_path, legacy_list):
    det = _detection(db, tmp_path, "duck.jpg")
    db.add_prediction(det, "Redhead", .51, "BioCLIP-2.5", labels_fingerprint=legacy_list["fingerprint"])

    def offline(*args, **kwargs):
        raise RuntimeError("Could not fetch page 1 of Birds species from iNaturalist")

    result = backfill(db, fetch=offline)

    assert result["ok"] is False
    assert result["errors"] == [
        "california-birds.txt: Could not fetch page 1 of Birds species from iNaturalist",
    ]
    assert db.get_meta(MARKER_PREFIX + legacy_list["fingerprint"]) is None
    assert [s["fingerprint"] for s in pending_label_sets(db)] == [legacy_list["fingerprint"]]


def test_a_list_without_its_query_is_not_looked_up(db, tmp_path, legacy_list):
    # A hand-made list records no place to re-query; its labels stay as they
    # are rather than being matched against some other region's species.
    meta_path = legacy_list["path"].with_suffix(".json")
    det = _detection(db, tmp_path, "duck.jpg")
    db.add_prediction(det, "Redhead", .51, "BioCLIP-2.5", labels_fingerprint=legacy_list["fingerprint"])
    meta = json.loads(meta_path.read_text())
    meta.pop("place_id")
    meta_path.write_text(json.dumps(meta))
    fetch = _fetch([], {})

    result = backfill(db, fetch=fetch)

    assert fetch.calls == []
    assert result["labels_identified"] == 0
    assert result["unidentified_labels_total"] == 2


def test_a_list_classified_before_merging_folded_case_variants_is_matched(db, tmp_path, legacy_list):
    # Older code fingerprinted the file as read, before merging collapsed
    # case-only duplicates, so today's merge rebuilds a different fingerprint.
    from labels import read_label_file
    path = legacy_list["path"]
    path.write_text("Mallard\nRedhead\nredhead\n", encoding="utf-8")
    as_read = compute_fingerprint(read_label_file(str(path)))
    assert as_read != compute_fingerprint(load_merged_labels([{"labels_file": str(path)}]))
    db.upsert_labels_fingerprint(as_read, path.name, [str(path)], 3)
    det = _detection(db, tmp_path, "duck.jpg")
    db.add_prediction(det, "Redhead", .51, "BioCLIP-2.5", labels_fingerprint=as_read)

    assert as_read in [s["fingerprint"] for s in pending_label_sets(db)]
    backfill(db, fetch=_fetch(["Redhead"], {"Redhead": REDHEAD}))
    assert _row(db, _prediction_id(db, det, "BioCLIP-2.5", as_read))["source_taxon_id"] == 7056


def test_predictions_older_than_fingerprints_take_the_identity_every_list_agrees_on(db, tmp_path, legacy_list):
    old = _detection(db, tmp_path, "old.jpg")
    db.add_prediction(old, "Redhead", .51, "BioCLIP-2.5")  # labels_fingerprint='legacy'
    db.add_prediction(old, "Mallard", .2, "BioCLIP-2.5")
    db.add_prediction(old, "Redhead", .5, "iNat21 (EVA-02 Large)",
                      taxonomy={"scientific_name": REDHEAD["scientific_name"]})
    det = _detection(db, tmp_path, "duck.jpg")
    db.add_prediction(det, "Redhead", .6, "BioCLIP-2.5", labels_fingerprint=legacy_list["fingerprint"])

    result = backfill(db, fetch=_fetch(["Redhead"], {"Redhead": REDHEAD}))

    bioclip = db.conn.execute(
        "SELECT * FROM predictions WHERE detection_id = ? AND classifier_model = 'BioCLIP-2.5' "
        "AND species = 'Redhead'", (old,)).fetchone()
    assert bioclip["source_taxon_id"] == 7056
    assert result["legacy_predictions_updated"] == 1
    reason = db.conn.execute(
        "SELECT reason FROM species_identity_repairs WHERE prediction_id = ?", (bioclip["id"],),
    ).fetchone()["reason"]
    assert reason == "label-list-consensus-identity"
    # Mallard was left unidentified by the only list holding it.
    mallard = db.conn.execute(
        "SELECT source_taxon_id FROM predictions WHERE detection_id = ? AND species = 'Mallard'", (old,),
    ).fetchone()
    assert mallard["source_taxon_id"] is None
    # The native iNat21 row keeps its own identity evidence.
    native = db.conn.execute(
        "SELECT source_taxon_id FROM predictions WHERE detection_id = ? AND classifier_model LIKE 'iNat%'",
        (old,),
    ).fetchone()
    assert native["source_taxon_id"] is None
    resolver = SpeciesResolver(db=db)
    assert resolver.prediction(bioclip).key == "taxon:7056"


def test_lists_that_disagree_leave_an_old_prediction_unidentified(db, tmp_path, legacy_list):
    # A list that records identities names a different "Redhead".
    labels_dir = legacy_list["path"].parent
    other = labels_dir / "europe.txt"
    other.write_text("Redhead\n", encoding="utf-8")
    from labels import _text_identity
    (labels_dir / "europe.json").write_text(json.dumps({
        "name": "Europe", "labels_file": str(other), "labels_text_sha256": _text_identity(["Redhead"]),
        "label_identities": {"Redhead": POCHARD},
    }))
    old = _detection(db, tmp_path, "old.jpg")
    db.add_prediction(old, "Redhead", .51, "BioCLIP-2.5")
    det = _detection(db, tmp_path, "duck.jpg")
    db.add_prediction(det, "Redhead", .6, "BioCLIP-2.5", labels_fingerprint=legacy_list["fingerprint"])

    result = backfill(db, fetch=_fetch(["Redhead"], {"Redhead": REDHEAD}))

    assert result["legacy_predictions_updated"] == 0
    assert db.conn.execute(
        "SELECT source_taxon_id FROM predictions WHERE detection_id = ?", (old,),
    ).fetchone()["source_taxon_id"] is None


def test_old_predictions_alone_start_the_startup_job(db, tmp_path, monkeypatch, legacy_list):
    from labels import _text_identity
    labels_dir = legacy_list["path"].parent
    modern = labels_dir / "modern.txt"
    modern.write_text("Redhead\n", encoding="utf-8")
    (labels_dir / "modern.json").write_text(json.dumps({
        "name": "Modern", "labels_file": str(modern), "labels_text_sha256": _text_identity(["Redhead"]),
        "label_identities": {"Redhead": REDHEAD},
    }))
    old = _detection(db, tmp_path, "old.jpg")
    db.add_prediction(old, "Redhead", .51, "BioCLIP-2.5")
    assert pending_label_sets(db) == []
    monkeypatch.setattr("labels.fetch_species_list", _fetch(["Redhead"], {"Redhead": REDHEAD}))
    app = _app(tmp_path, monkeypatch, db)

    app._kickoff_label_identity_backfill()

    job = _wait_for_job(app._job_runner, "label-list-species-ids")
    assert job is not None and job["status"] == "completed", job
    assert db.conn.execute(
        "SELECT source_taxon_id FROM predictions WHERE detection_id = ?", (old,),
    ).fetchone()["source_taxon_id"] == 7056


def test_strict_fetch_raises_instead_of_returning_a_partial_list():
    with patch("labels.urllib.request.urlopen", side_effect=OSError("offline")), \
            patch("time.sleep"):
        with pytest.raises(RuntimeError, match="page 1"):
            fetch_species_list(14, ["birds"], strict=True)
        assert list(fetch_species_list(14, ["birds"])) == []


def test_job_summary_names_counts_failures_and_unidentified_labels():
    described = describe_result("label-list-species-ids", {
        "label_sets": 2, "labels": 2098, "labels_identified": 2051,
        "predictions_updated": 41250, "unidentified_labels": ["Redhead"],
        "unidentified_labels_total": 47, "errors": ["hawaii.txt: offline"], "ok": False,
    })
    assert described["summary"] == (
        "Species IDs found for 2,051 of 2,098 labels in 2 label lists; "
        "41,250 predictions updated; 1 list failed to reach iNaturalist"
    )
    assert "hawaii.txt: offline" in described["details"]
    assert any(line.startswith("47 labels without a single matching iNaturalist species (showing 1)")
               for line in described["details"])
    legacy = describe_result("label-list-species-ids", {
        "label_sets": 1, "labels": 10, "labels_identified": 10, "predictions_updated": 25,
        "legacy_predictions_updated": 5, "errors": [], "ok": True,
    })
    assert ("5 predictions older than label-list tracking matched by a name every one "
            "of your label lists agrees on") in legacy["details"]


def _app(tmp_path, monkeypatch, db):
    import config as cfg
    import models
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path / "vireo-models"))
    monkeypatch.setattr(models, "CONFIG_PATH", str(tmp_path / "models.json"))
    from app import create_app
    thumbs = tmp_path / "thumbs"
    thumbs.mkdir()
    return create_app(db_path=str(tmp_path / "photos.db"), thumb_cache_dir=str(thumbs), api_token="t")


def _wait_for_job(runner, job_type, deadline_s=5):
    import time
    deadline = time.time() + deadline_s
    while time.time() < deadline:
        jobs = [j for j in runner.list_jobs() if j["type"] == job_type]
        if jobs and jobs[0]["status"] in ("completed", "failed"):
            return jobs[0]
        time.sleep(0.05)
    return None


def test_startup_job_recovers_identities_in_the_background(db, tmp_path, monkeypatch, legacy_list):
    det = _detection(db, tmp_path, "duck.jpg")
    db.add_prediction(det, "Redhead", .51, "BioCLIP-2.5", labels_fingerprint=legacy_list["fingerprint"])
    monkeypatch.setattr("labels.fetch_species_list", _fetch(["Redhead"], {"Redhead": REDHEAD}))
    app = _app(tmp_path, monkeypatch, db)

    app._kickoff_label_identity_backfill()

    job = _wait_for_job(app._job_runner, "label-list-species-ids")
    assert job is not None and job["status"] == "completed", job
    assert job.get("ephemeral") is True
    row = _row(db, _prediction_id(db, det, "BioCLIP-2.5", legacy_list["fingerprint"]))
    assert row["source_taxon_id"] == REDHEAD["taxon_id"]


def test_startup_job_is_not_started_without_work(db, tmp_path, monkeypatch, legacy_list):
    app = _app(tmp_path, monkeypatch, db)

    app._kickoff_label_identity_backfill()

    assert [j for j in app._job_runner.list_jobs() if j["type"] == "label-list-species-ids"] == []


def test_legacy_only_predictions_query_their_saved_lists(db, tmp_path, legacy_list):
    """Pre-fingerprint rows recover without any newer prediction to trigger lookup."""
    from label_source_identities import LEGACY_MARKER

    old = _detection(db, tmp_path, "old.jpg")
    db.add_prediction(old, "Redhead", .51, "BioCLIP-2.5")
    fetch = _fetch(["Redhead"], {"Redhead": REDHEAD})
    assert pending_label_sets(db) == []
    result = backfill(db, fetch=fetch)
    assert len(fetch.calls) == 1
    assert result["legacy_predictions_updated"] == 1
    assert db.get_meta(LEGACY_MARKER) is not None
    assert _row(db, _prediction_id(db, old, "BioCLIP-2.5", "legacy"))["source_taxon_id"] == 7056


def test_failed_legacy_only_lookup_is_retried(db, tmp_path, legacy_list):
    """An unavailable legacy-only list never creates a completed-consensus marker."""
    from label_source_identities import LEGACY_MARKER

    old = _detection(db, tmp_path, "old.jpg")
    db.add_prediction(old, "Redhead", .51, "BioCLIP-2.5")

    def offline(*args, **kwargs):
        raise RuntimeError("offline")

    result = backfill(db, fetch=offline)
    assert result["ok"] is False
    assert db.get_meta(LEGACY_MARKER) is None
    assert _row(db, _prediction_id(db, old, "BioCLIP-2.5", "legacy"))["source_taxon_id"] is None
    assert backfill(db, fetch=_fetch(["Redhead"], {"Redhead": REDHEAD}))["legacy_predictions_updated"] == 1


def test_failed_pending_list_defers_legacy_consensus_until_retry(db, tmp_path, legacy_list):
    """A successful list cannot stamp old rows while a conflicting list is offline."""
    from label_source_identities import LEGACY_MARKER

    path = legacy_list["path"].parent / "europe.txt"
    path.write_text("Redhead\n")
    path.with_suffix(".json").write_text(json.dumps({
        "name": "Europe", "labels_file": str(path), "place_id": 1, "taxon_groups": ["birds"],
    }))
    fingerprint = compute_fingerprint(load_merged_labels([{"labels_file": str(path)}]))
    db.upsert_labels_fingerprint(fingerprint, path.name, [str(path)], 1)
    old = _detection(db, tmp_path, "old.jpg")
    db.add_prediction(old, "Redhead", .51, "BioCLIP-2.5")
    current = _detection(db, tmp_path, "current.jpg")
    for fp in (fingerprint, legacy_list["fingerprint"]):
        db.add_prediction(current, "Redhead", .51, "BioCLIP-2.5", labels_fingerprint=fp)

    def fetch(place_id, *args, **kwargs):
        if place_id == 1:
            raise RuntimeError("offline")
        return SpeciesLabels(["Redhead"], {"Redhead": REDHEAD})

    result = backfill(db, fetch=fetch)
    assert result["ok"] is False
    assert db.get_meta(LEGACY_MARKER) is None
    assert _row(db, _prediction_id(db, old, "BioCLIP-2.5", "legacy"))["source_taxon_id"] is None

    def retry(place_id, *args, **kwargs):
        return SpeciesLabels(["Redhead"], {"Redhead": POCHARD if place_id == 1 else REDHEAD})

    result = backfill(db, fetch=retry)
    assert result["ok"] is True
    assert result["legacy_predictions_updated"] == 0
    assert db.get_meta(LEGACY_MARKER) is not None
    assert _row(db, _prediction_id(db, old, "BioCLIP-2.5", "legacy"))["source_taxon_id"] is None


@pytest.mark.parametrize("other,identified", [(POCHARD, False), (REDHEAD, True)])
def test_merged_prompt_checks_case_folded_sources(tmp_path, other, identified):
    """Every source of a folded prompt contributes to its identity decision."""
    from label_source_identities import plan_label_set

    paths = [tmp_path / "one.txt", tmp_path / "two.txt"]
    for path, name in zip(paths, ["Redhead", "redhead"], strict=True):
        path.write_text(name + "\n")
    metas = [{"labels_file": str(path)} for path in paths]
    planned, unidentified = plan_label_set(
        {"metas": metas, "labels": load_merged_labels(metas)},
        {str(paths[0]): {"Redhead": REDHEAD}, str(paths[1]): {"redhead": other}},
    )
    assert bool(planned) == identified
    assert bool(unidentified) != identified


def test_unidentified_folded_source_vetoes_a_merged_identity(tmp_path):
    """One identified source cannot override another source's ambiguous prompt."""
    from label_source_identities import plan_label_set

    paths = [tmp_path / "one.txt", tmp_path / "two.txt"]
    for path, name in zip(paths, ["Redhead", "redhead"], strict=True):
        path.write_text(name + "\n")
    metas = [{"labels_file": str(path)} for path in paths]
    planned, unidentified = plan_label_set(
        {"metas": metas, "labels": load_merged_labels(metas)},
        {str(paths[0]): {"Redhead": REDHEAD}, str(paths[1]): {}},
    )
    assert not planned
    assert len(unidentified) == 1


@pytest.mark.parametrize("changed_file", ["text", "sidecar"])
def test_source_change_during_lookup_keeps_old_fingerprint_unstamped(db, tmp_path, legacy_list, changed_file):
    """Network lookup results never stamp an old fingerprint after its files change."""
    from label_source_identities import LEGACY_MARKER

    det = _detection(db, tmp_path, "duck.jpg")
    fingerprint = legacy_list["fingerprint"]
    db.add_prediction(det, "Redhead", .51, "BioCLIP-2.5", labels_fingerprint=fingerprint)

    def fetch(*args, **kwargs):
        if changed_file == "text":
            legacy_list["path"].write_text("Mallard\nRedhead\nCanvasback\n")
        else:
            path = legacy_list["path"].with_suffix(".json")
            meta = json.loads(path.read_text())
            meta["place_id"] = 1
            path.write_text(json.dumps(meta))
        return SpeciesLabels(["Redhead"], {"Redhead": REDHEAD})

    result = backfill(db, fetch=fetch)
    assert result["ok"] is False
    assert "changed" in result["errors"][0]
    assert db.get_meta(MARKER_PREFIX + fingerprint) is None
    assert db.get_meta(LEGACY_MARKER) is None
    assert _row(db, _prediction_id(db, det, "BioCLIP-2.5", fingerprint))["source_taxon_id"] is None
    assert db.conn.execute("SELECT COUNT(*) FROM label_source_identities").fetchone()[0] == 0


@pytest.mark.parametrize("taxonomy", [
    {"scientific_name": "Aythya ferina"}, {"genus": "Aythya"},
])
def test_legacy_native_bioclip_taxonomy_is_not_repaired_from_list_consensus(db, tmp_path, legacy_list, taxonomy):
    """The legacy sentinel loses mode provenance, so existing ToL taxonomy is protected."""
    old = _detection(db, tmp_path, "native.jpg")
    db.add_prediction(old, "Redhead", .51, "BioCLIP-2.5", taxonomy=taxonomy)
    pred_id = _prediction_id(db, old, "BioCLIP-2.5", "legacy")
    before = dict(_row(db, pred_id))
    db.conn.execute(
        "INSERT INTO classifier_runs (detection_id, classifier_model, labels_fingerprint, runtime_fingerprint) "
        "VALUES (?, 'BioCLIP-2.5', 'legacy', 'native-runtime')", (old,),
    )
    db.conn.commit()
    result = backfill(db, fetch=_fetch(["Redhead"], {"Redhead": REDHEAD}))
    assert result["predictions_updated"] == 0
    assert dict(_row(db, pred_id)) == before
    assert db.conn.execute("SELECT runtime_fingerprint FROM classifier_runs WHERE detection_id=?", (old,)).fetchone()[0] == 'native-runtime'


@pytest.mark.parametrize("changed_file", ["text", "sidecar"])
def test_legacy_only_source_change_during_lookup_leaves_consensus_retryable(db, tmp_path, legacy_list, changed_file):
    """Legacy-only lookup cannot commit an identity from changing source evidence."""
    from label_source_identities import LEGACY_MARKER

    old = _detection(db, tmp_path, "old.jpg")
    db.add_prediction(old, "Redhead", .51, "BioCLIP-2.5")

    def fetch(*args, **kwargs):
        if changed_file == 'text':
            legacy_list['path'].write_text('Mallard\nRedhead\nCanvasback\n')
        else:
            path = legacy_list['path'].with_suffix('.json')
            meta = json.loads(path.read_text())
            meta['place_id'] = 1
            path.write_text(json.dumps(meta))
        return SpeciesLabels(['Redhead'], {'Redhead': REDHEAD})

    result = backfill(db, fetch=fetch)
    assert result['ok'] is False
    assert db.get_meta(LEGACY_MARKER) is None
    assert _row(db, _prediction_id(db, old, 'BioCLIP-2.5', 'legacy'))['source_taxon_id'] is None


@pytest.mark.parametrize("tracked", [False, True])
def test_replaced_sidecar_after_catalog_read_uses_current_query(db, tmp_path, legacy_list, monkeypatch, tracked):
    """A catalog read never pairs its old query with a newer sidecar digest."""
    from label_source_identities import LEGACY_MARKER
    from labels import get_saved_labels

    det = _detection(db, tmp_path, "old-query.jpg")
    fingerprint = legacy_list["fingerprint"] if tracked else "legacy"
    db.add_prediction(det, "Redhead", .51, "BioCLIP-2.5", labels_fingerprint=fingerprint)
    original = get_saved_labels

    def replace_after_read():
        metas = original()
        path = legacy_list["path"].with_suffix(".json")
        current = json.loads(path.read_text())
        current["place_id"] = 1
        path.write_text(json.dumps(current))
        return metas

    monkeypatch.setattr("labels.get_saved_labels", replace_after_read)
    fetch = _fetch(["Redhead"], {"Redhead": POCHARD})
    result = backfill(db, fetch=fetch)
    assert result["ok"] is True
    assert fetch.calls and all(call[0] == 1 for call in fetch.calls)
    assert _row(db, _prediction_id(db, det, "BioCLIP-2.5", fingerprint))["source_taxon_id"] == 7054
    if not tracked:
        assert db.get_meta(LEGACY_MARKER) is not None


@pytest.mark.parametrize("tracked", [False, True])
def test_cancel_during_final_lookup_does_not_commit_repairs(db, tmp_path, legacy_list, tracked):
    from label_source_identities import LEGACY_MARKER

    det = _detection(db, tmp_path, "cancelled.jpg")
    fingerprint = legacy_list["fingerprint"] if tracked else "legacy"
    db.add_prediction(det, "Redhead", .51, "BioCLIP-2.5", labels_fingerprint=fingerprint)
    cancelled = False

    def fetch(*args, **kwargs):
        nonlocal cancelled
        cancelled = True
        return SpeciesLabels(["Redhead"], {"Redhead": REDHEAD})

    result = backfill(db, fetch=fetch, cancel_check=lambda: cancelled)
    assert result["cancelled"] is True
    assert result["predictions_updated"] == 0
    assert db.get_meta(MARKER_PREFIX + fingerprint) is None
    assert db.get_meta(LEGACY_MARKER) is None
    assert _row(db, _prediction_id(db, det, "BioCLIP-2.5", fingerprint))["source_taxon_id"] is None
    assert db.conn.execute("SELECT COUNT(*) FROM label_source_identities").fetchone()[0] == 0
