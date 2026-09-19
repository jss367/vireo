import json
import shutil
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from encounter_eval.algorithms import run_algorithm
from encounter_eval.common import Group, digest, validate_groups, write_json
from encounter_eval.library import FeatureReader, open_library, plan_sessions, prepare, read_bundle
from encounter_eval.runner import evaluate, parameter_trials, tune
from encounter_eval.scoring import score, summarize


def photo(i, species="a", *, second=None, disagreement=False, missing=False):
    def subject(did, taxon, x):
        sources = [{"mode": "exclusive", "model": "m1", "predictions": [{"taxon": taxon, "score": .98}]}]
        if disagreement:
            sources.append({"mode": "exclusive", "model": "m2", "predictions": [{"taxon": "other", "score": .98}]})
        return {"id": did, "detector_model": "megadetector-v6", "detector_confidence": .95,
                "category": "animal", "box_x": x, "box_y": .1, "box_w": .2, "box_h": .2, "sources": sources}
    subjects = [] if missing else [subject(i * 10, species, .1)]
    if second:
        subjects.append(subject(i * 10 + 1, second, .7))
    return {"id": i, "folder_id": 1, "timestamp": (datetime(2026, 1, 1) + timedelta(seconds=i / 10)).isoformat(),
            "filename": f"{i:03}.jpg", "evidence": subjects,
            "species_top5": [] if missing else [(species, .98, "m1")], "species_keys": {species: species}}


def test_species_switch_with_detector_miss_does_not_bridge_runs():
    photos = [photo(i, "a" if i < 6 else "b", missing=i == 5) for i in range(12)]
    groups = run_algorithm("sequence", photos)
    assert groups[0].roster == ("a",)
    assert groups[-1].roster == ("b",)
    assert len(groups) == 2


def test_detector_miss_in_stable_run_is_repaired():
    groups = run_algorithm("sequence", [photo(i, missing=i == 5) for i in range(12)])
    assert len(groups) == 1
    assert groups[0].roster == ("a",)


def test_one_frame_second_species_survives_smoothing():
    groups = run_algorithm("sequence", [photo(i, second="b" if i == 5 else None) for i in range(12)])
    assert [g.roster for g in groups] == [("a",), ("a", "b"), ("a",)]
    assert groups[1].photo_ids == (5,)


def test_source_disagreement_and_unclassified_box_stay_unresolved():
    photos = [photo(i) for i in range(12)]
    photos[5] = photo(5, disagreement=True)
    groups = run_algorithm("sequence", photos)
    assert next(g for g in groups if 5 in g.photo_ids).roster is None
    photos[5] = photo(5, second="b")
    photos[5]["evidence"][1]["sources"] = []
    assert next(g for g in run_algorithm("sequence", photos) if 5 in g.photo_ids).roster is None


def test_two_same_species_subjects_do_not_force_a_boundary():
    groups = run_algorithm("sequence", [photo(i, second="a" if i == 5 else None) for i in range(12)])
    assert len(groups) == 1


def test_missing_context_does_not_manufacture_empty_or_infinite_continuity():
    photos = [photo(i, missing=1 <= i <= 20) for i in range(22)]
    groups = run_algorithm("sequence", photos, {"context_frames": 2})
    assert next(g for g in groups if 10 in g.photo_ids).roster is None
    assert all(g.roster != () for g in groups)


def test_pure_but_wrong_roster_fails_and_partial_tags_do_not_assert_absence():
    photos = [photo(1)]
    answer = {"1": {"taxa": ["a"], "complete": True, "sources": ["manual"]}}
    wrong = summarize(score(photos, answer, [Group((1,), ("b",), "test")]))
    assert wrong["exact_roster_accuracy"] == 0
    assert wrong["counts"]["incorrect_additions"] == 1
    answer["1"]["complete"] = False
    partial = summarize(score(photos, answer, [Group((1,), ("a", "b"), "test")]))
    assert partial["exact_roster_accuracy"] is None
    assert partial["incorrect_additions_per_1000"] is None
    assert partial["counts"]["unverified_additions"] == 1


def test_abstention_and_fragmentation_are_not_free():
    photos = [photo(i) for i in range(10)]
    answers = {str(i): {"taxa": ["a"], "complete": True, "sources": ["manual"]} for i in range(10)}
    good = summarize(score(photos, answers, [Group(tuple(range(10)), ("a",), "test")]))
    fragmented = summarize(score(photos, answers, [Group((i,), ("a",), "test") for i in range(10)]))
    unknown = summarize(score(photos, answers, [Group(tuple(range(10)), None, "test")]))
    assert good["objective"] < fragmented["objective"] < unknown["objective"]
    assert unknown["positive_recall"] == 0


def test_order_and_membership_validation():
    with pytest.raises(ValueError, match="exactly-once"):
        validate_groups([photo(1), photo(2)], [Group((2, 1), ("a",), "test")])


def test_reader_is_read_only_and_hides_labels_before_feature_preparation(library, tmp_path, monkeypatch):
    import config
    from pipeline import load_photo_features

    monkeypatch.setattr(config, "load", lambda: pytest.fail("Mutable config must not be read"))
    conn = open_library(library)
    with pytest.raises(sqlite3.OperationalError, match="readonly"):
        conn.execute("DELETE FROM photos")
    features = load_photo_features(FeatureReader(conn, 1), photo_ids=list(range(1, 13)), effective_config={})
    assert all(p["confirmed_species"] is None for p in features)
    conn.close()
    manifest = prepare(library, tmp_path / "run")
    bundle = read_bundle(tmp_path / "run", manifest["sessions"][0])
    assert len(bundle["photos"]) == 12
    assert bundle["answers"]["1"]["taxa"] == ["inat:101"]
    assert not bundle["answers"]["1"]["complete"]
    assert "confirmed_species" not in bundle["photos"][0]
    assert "flag" not in bundle["photos"][0]
    assert "answers" not in bundle["photos"][0]
    # "Spotted Redshank" is carried by exactly one taxon in this catalog, so
    # the preferred-name fallback resolves it even unstamped (see #1694).
    assert bundle["photos"][0]["evidence"][0]["sources"][0]["predictions"][0]["taxon"] == "inat:101"


def test_new_run_picks_up_label_change_but_retained_inputs_remain_consistent(library, tmp_path):
    first = prepare(library, tmp_path / "first")
    bundle = read_bundle(tmp_path / "first", first["sessions"][0])
    conn = sqlite3.connect(library)
    conn.execute("UPDATE photo_keywords SET keyword_id=2 WHERE photo_id=1")
    conn.commit()
    conn.close()
    second = prepare(library, tmp_path / "second")
    newer = read_bundle(tmp_path / "second", second["sessions"][0])
    assert first["data_digest"] != second["data_digest"]
    assert bundle["photos"] == newer["photos"]
    assert bundle["answers"] != newer["answers"]
    assert read_bundle(tmp_path / "first", first["sessions"][0]) == bundle


def test_source_taxon_ids_preserve_distinct_species_with_the_same_name(library, tmp_path):
    conn = sqlite3.connect(library)
    conn.execute("ALTER TABLE predictions ADD COLUMN source_taxon_id INTEGER")
    conn.execute("ALTER TABLE keywords ADD COLUMN source_taxon_id INTEGER")
    conn.execute("UPDATE keywords SET name='Shared name', source_taxon_id=100+id")
    conn.execute("UPDATE predictions SET species='Shared name',source_taxon_id=100+id WHERE id IN (1,2)")
    conn.execute("UPDATE photo_keywords SET keyword_id=2 WHERE photo_id=2")
    conn.commit()
    conn.close()
    output = tmp_path / "run"
    manifest = prepare(library, output)
    bundle = read_bundle(output, manifest["sessions"][0])
    for photo, expected in zip(bundle["photos"][:2], ("inat:101", "inat:102"), strict=True):
        assert bundle["answers"][str(photo["id"])]["taxa"] == [expected]
        assert photo["species_keys"][photo["species_top5"][0][3]] == expected
        assert photo["evidence"][0]["sources"][0]["predictions"][0]["taxon"] == expected


def test_preserves_unlabeled_neighbors_and_multiple_subject_sources(library, tmp_path):
    conn = sqlite3.connect(library)
    conn.execute("DELETE FROM photo_keywords WHERE photo_id=6")
    conn.execute("INSERT INTO predictions VALUES(100,1,'Glossy Ibis',NULL,.99,'model-b','labels-2','2026-01-03')")
    conn.execute("INSERT INTO predictions VALUES(101,1,'Glossy Ibis',NULL,1,'model-a','stale','2025-01-01')")
    conn.commit()
    conn.close()
    manifest = prepare(library, tmp_path / "run", complete_folders=[1])
    bundle = read_bundle(tmp_path / "run", manifest["sessions"][0])
    assert len(bundle["photos"]) == 12 and "6" not in bundle["answers"]
    sources = bundle["photos"][0]["evidence"][0]["sources"]
    assert {s["model"] for s in sources} == {"model-a", "model-b"}
    assert "stale" not in {s["labels_fingerprint"] for s in sources}
    assert bundle["answers"]["1"]["complete"]


def test_day_and_duplicate_split_membership():
    rows = [{"id": i, "folder_id": i, "folder_path": str(i), "timestamp": day + "T12:00:00",
             "filename": "a.jpg", "file_hash": file_hash}
            for i, day, file_hash in [(1, "2026-01-01", "abc"), (2, "2026-01-02", "abc"), (3, "2026-01-01", None)]]
    sessions = plan_sessions(rows, 42)
    assert len({s["partition"] for s in sessions}) == 1
    assert len({s["partition_key"] for s in sessions}) == 1


def test_split_registry_does_not_reshuffle_and_quarantines_cross_partition_duplicates(library, tmp_path):
    registry = tmp_path / "splits.json"
    write_json(registry, {"seed": 42, "days": {"2026-01-01": "train", "2026-01-02": "test"}})
    conn = sqlite3.connect(library)
    conn.execute("UPDATE photos SET file_hash='duplicate' WHERE id=1")
    conn.execute("UPDATE photos SET file_hash='duplicate',timestamp='2026-01-02T12:00:00' WHERE id=12")
    conn.commit()
    conn.close()
    manifest = prepare(library, tmp_path / "run", split_registry=registry)
    assert manifest["inventory"]["quarantined_day_clusters"] == 1
    assert manifest["sessions"] == []


def test_search_is_deterministic_and_bounded():
    space = {"a": [1, 2, 3], "b": [4, 5]}
    assert parameter_trials(space, "random", 3, 42) == parameter_trials(space, "random", 3, 42)
    assert len(parameter_trials(space, "grid", 100, 42)) == 6
    assert len({digest(t) for t in parameter_trials(space, "random", 100, 42)}) == 6


def test_production_adapter_baseline_prefers_winning_identity_over_shared_display_name():
    top5 = [("Shared", .9, "m1", "inat:200"), ("Shared", .2, "m1", "inat:201")]
    photos = [{"id": i, "folder_id": 1, "filename": f"{i:03}.jpg",
               "timestamp": (datetime(2026, 1, 1) + timedelta(seconds=i / 10)).isoformat(),
               "evidence": [], "species_top5": top5,
               "species_keys": {"inat:200": "inat:200", "inat:201": "inat:201"}}
              for i in range(3)]
    groups = run_algorithm("production", photos)
    assert [g.roster for g in groups] == [("inat:200",)]


def test_production_adapter_uses_actual_loader_and_grouping(library, tmp_path):
    from encounters import segment_encounters
    from pipeline import load_photo_features

    output = tmp_path / "run"
    manifest = prepare(library, output)
    bundle = read_bundle(output, manifest["sessions"][0])
    conn = open_library(library)
    raw = load_photo_features(FeatureReader(conn, 1), photo_ids=list(range(1, 13)),
                              config=manifest["config"], effective_config=manifest["config"])
    conn.close()
    actual = segment_encounters(raw, manifest["grouping_config"])
    adapted = run_algorithm("production", bundle["photos"], grouping_config=manifest["grouping_config"])
    assert [g.photo_ids for g in adapted] == [tuple(p["id"] for p in g["photos"]) for g in actual]


def test_trial_cache_and_tuning_never_score_test_partition(library, tmp_path, monkeypatch):
    output = tmp_path / "run"
    manifest = prepare(library, output)
    entry = manifest["sessions"][0]
    manifest["sessions"] = [{**entry, "partition": "train"}, {**entry, "partition": "development"},
                            {**entry, "partition": "test", "path": "must-not-read.json"}]
    result, trials = tune(output, manifest, trials=2, seconds=60, min_coverage=0)
    assert not result["test_partition_evaluated"]
    assert all(r["partition"] != "test" for r in trials)
    assert result["winner"]
    previous = evaluate(output, manifest, "sequence", {}, "train")
    monkeypatch.setattr("encounter_eval.runner.run_algorithm", lambda *args: pytest.fail("Cached trial reran"))
    assert evaluate(output, manifest, "sequence", {}, "train") == previous


def test_report_escapes_labels_and_filenames(library, tmp_path):
    from encounter_eval.report import write_report

    conn = sqlite3.connect(library)
    conn.execute("UPDATE photos SET filename='<script>alert(1)</script>.jpg' WHERE id=6")
    conn.commit()
    conn.close()
    output = tmp_path / "run"
    manifest = prepare(library, output)
    baseline = evaluate(output, manifest, "production", {}, "all")
    candidate = evaluate(output, manifest, "sequence", {}, "all")
    write_report(output, manifest, [baseline, candidate], candidate=candidate)
    report = (output / "report.html").read_text()
    assert "<script>" not in report
    assert "&lt;script&gt;" in report


def test_cli_complete_comparison_and_resume(library, tmp_path, capsys):
    from encounter_eval.cli import main

    output = tmp_path / "run"
    assert main(["compare", "--db", str(library), "--output", str(output), "--partition", "all"]) == 0
    assert (output / "report.html").is_file()
    assert main(["compare", "--resume", str(output), "--partition", "all"]) == 0
    manifest = json.loads((output / "manifest.json").read_text())
    manifest["code"]["source_digest"] = "modified"
    write_json(output / "manifest.json", manifest)
    assert main(["compare", "--resume", str(output), "--partition", "all"]) == 2
    assert "Source/environment changed" in capsys.readouterr().err


def test_prepared_manifest_can_resume_before_cli_postprocessing(library, tmp_path):
    from encounter_eval.cli import main

    output = tmp_path / "run"
    prepared = prepare(library, output)
    saved = json.loads((output / "manifest.json").read_text())
    assert saved == prepared
    assert saved["code"]["source_digest"]
    assert main(["compare", "--resume", str(output), "--partition", "all"]) == 0
    assert (output / "report.html").is_file()


@pytest.mark.parametrize("size", [30, 100, 256])
@pytest.mark.parametrize("seed", [0, 42])
def test_growing_random_budget_preserves_prior_trials(size, seed):
    space = {"a": list(range(size))}
    previous = []
    for budget in (19, 22, 31, size + 1):
        trials = parameter_trials(space, "random", budget, seed)
        assert trials[:len(previous)] == previous
        assert len({digest(t) for t in trials}) == min(size, budget)
        previous = trials


def test_detector_run_reads_follow_each_loaded_session(library):
    from pipeline import load_photo_features

    conn = open_library(library)
    try:
        reader = FeatureReader(conn, 1)
        load_photo_features(reader, photo_ids=[1, 6], effective_config={})
        # Includes a completed zero-box run, but excludes other sessions.
        assert reader.get_detector_run_photo_ids("megadetector-v6") == {1, 6}
        load_photo_features(reader, photo_ids=[7, 12], effective_config={})
        assert reader.get_detector_run_photo_ids("megadetector-v6") == {7, 12}
        assert reader.get_detector_run_photo_ids("another-detector") == set()
    finally:
        conn.close()


@pytest.mark.parametrize(("model", "fingerprint", "source_id", "expected"), [
    ("custom-model", "custom", None, "name:unknown custom label"),
    ("custom-model", "tol", None, "inat:102"),
    ("iNat21", "custom", None, "inat:102"),
    ("custom-model", "custom", 101, "inat:101"),
])
def test_raw_predictions_share_production_identity_policy(library, tmp_path, model, fingerprint, source_id, expected):
    conn = sqlite3.connect(library)
    conn.execute("ALTER TABLE predictions ADD COLUMN source_taxon_id INTEGER")
    conn.execute("""UPDATE predictions SET species='Unknown custom label',
        scientific_name='Plegadis falcinellus', classifier_model=?, labels_fingerprint=?, source_taxon_id=?
        WHERE id=1""", (model, fingerprint, source_id))
    conn.commit()
    conn.close()
    output = tmp_path / "run"
    manifest = prepare(library, output)
    first = read_bundle(output, manifest["sessions"][0])["photos"][0]
    assert first["evidence"][0]["sources"][0]["predictions"][0]["taxon"] == expected
    entry = first["species_top5"][0]
    assert first["species_keys"][entry[3]] == expected


@pytest.mark.parametrize("ambiguous", [False, True])
def test_keyword_alias_cannot_resolve_untrusted_prediction_name(library, tmp_path, ambiguous):
    """A `keywords` row naming a taxon must never make an untrusted
    prediction name resolve to it.

    Both parametrizations put the name beyond the resolver's own reach and
    then check the keyword alias does not sneak it back in. `ambiguous=True`
    uses a stamped index that recorded the collision; `ambiguous=False` uses
    an unstamped catalog where a second taxon carries the same preferred
    name, which is the case #1694's fallback deliberately still refuses.
    Without that second taxon the unstamped half would resolve on the
    preferred name alone and stop testing the alias at all.
    """
    from taxonomy import COMMON_NAME_IDENTITY_VERSION

    conn = sqlite3.connect(library)
    if ambiguous:
        conn.execute("INSERT INTO db_meta VALUES('common_name_identity_version', ?)", (str(COMMON_NAME_IDENTITY_VERSION),))
        conn.execute("INSERT INTO db_meta VALUES('ambiguous_common_names', ?)", (json.dumps(["spotted redshank", "glossy ibis"]),))
    else:
        conn.execute("INSERT INTO taxa VALUES(3,103,'Tringa totanus','Spotted Redshank','species')")
        conn.execute("INSERT INTO taxa VALUES(4,104,'Plegadis chihi','Glossy Ibis','species')")
    conn.commit()
    conn.close()
    output = tmp_path / "run"
    manifest = prepare(library, output)
    bundle = read_bundle(output, manifest["sessions"][0])
    first = bundle["photos"][0]
    assert bundle["answers"]["1"]["taxa"] == ["inat:101"]
    assert first["evidence"][0]["sources"][0]["predictions"][0]["taxon"] == "name:spotted redshank"
    entry = first["species_top5"][0]
    assert first["species_keys"][entry[3]] == "name:spotted redshank"
    for algorithm in ("production", "sequence"):
        result = evaluate(output, manifest, algorithm, {}, "all")
        assert result["metrics"]["positive_recall"] == 0


def test_independent_tuning_only_searches_effective_default_parameters(library, tmp_path):
    output = tmp_path / "run"
    manifest = prepare(library, output)
    entry = manifest["sessions"][0]
    manifest["sessions"] = [{**entry, "partition": partition} for partition in ("train", "development")]
    result, evaluations = tune(output, manifest, algorithm="independent", trials=50, seconds=60, min_coverage=0)
    trained = [r for r in evaluations if r["algorithm"] == "independent" and r["partition"] == "train"]
    assert result["completed_trials"] == len(trained) == 12
    assert set(result["search_space"]) == {"confidence", "margin"}
    assert all(set(r["params"]) == {"confidence", "margin"} for r in trained)
    assert len({(r["params"]["confidence"], r["params"]["margin"]) for r in trained}) == 12


def test_default_split_registry_is_scoped_to_library_and_workspace(library, tmp_path):
    other_library = tmp_path / "other.db"
    shutil.copy2(library, other_library)
    first = prepare(library, tmp_path / "first")
    registry_path = Path(first["split_registry_path"])
    registry = json.loads(registry_path.read_text())
    registry["days"]["2026-01-01"] = "quarantined"
    write_json(registry_path, registry)
    repeated = prepare(library, tmp_path / "repeated", workspace=1)
    assert repeated["split_registry_path"] == first["split_registry_path"]
    assert repeated["sessions"] == []
    other = prepare(other_library, tmp_path / "other")
    assert other["sessions"]
    conn = sqlite3.connect(library)
    conn.execute("INSERT INTO workspaces VALUES(2, 'Another workspace')")
    conn.execute("INSERT INTO workspace_folders VALUES(2, 1)")
    conn.commit()
    conn.close()
    workspace = prepare(library, tmp_path / "workspace", workspace=2)
    assert workspace["sessions"]
    assert len({m["split_registry_path"] for m in (first, other, workspace)}) == 3
    assert json.loads(registry_path.read_text())["days"]["2026-01-01"] == "quarantined"
