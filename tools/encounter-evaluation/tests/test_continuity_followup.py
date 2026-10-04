import copy
import json

import pytest
from test_label_benchmark import retained_scope

from encounter_eval.common import Group, configure_repo, digest
from encounter_eval.continuity_experiments import apply_candidate, candidates, prepare_baseline
from encounter_eval.continuity_followup import comparison_counts, pair_references, run

configure_repo()


def photos(*, weak=True):
    result = []
    for i in range(3):
        middle = i == 1
        name, taxon, confidence = (
            ("Other", "2", 0.85) if middle and not weak else ("Bird", "1", 0.65 if middle else 0.99)
        )
        detection = {
            "id": i,
            "detector_model": "megadetector-v6",
            "category": "animal",
            "detector_confidence": 0.05 if middle and weak else 0.9,
            "box_x": 0.2,
            "box_y": 0.2,
            "box_w": 0.3,
            "box_h": 0.3,
            "sources": [
                {
                    "model": "one",
                    "mode": "exclusive",
                    "predictions": [{"name": name, "score": confidence, "taxon": "inat:" + taxon}],
                }
            ],
        }
        result.append(
            {
                "id": i,
                "folder_id": 1,
                "timestamp": f"2026-01-01T00:00:00.{i * 3}00",
                "subject_present": not (middle and weak),
                "subject_absent": middle and weak,
                "species_top5": [] if middle and weak else [(name, confidence, "one", "taxon:" + taxon)],
                "species_keys": {"taxon:" + taxon: "inat:" + taxon},
                "evidence": [detection],
            }
        )
    return result


def spec(name):
    return next(s for s in candidates() if s["id"] == name)


def test_native_evidence_preserves_taxon_namespaces():
    from encounter_eval.continuity import native_evidence

    p = photos()
    predictions = p[1]["evidence"][0]["sources"][0]["predictions"]
    predictions.append({"name": "Local bird", "score": 0.9, "taxon": "taxon:1"})
    evidence, identities = native_evidence(p)
    assert [entry[3] for entry in evidence[1][0]["predictions"]] == ["inat:1", "taxon:1"]
    assert identities == {"inat:1": "inat:1", "taxon:1": "taxon:1"}


def test_local_taxon_with_same_number_cannot_support_inaturalist_anchors():
    p = photos()
    p[1]["evidence"][0]["sources"][0]["predictions"] = [
        {"name": "Local bird", "score": 0.99, "taxon": "taxon:1"}
    ]
    original = digest(p)
    prepared = prepare_baseline(p, {})
    assert prepared[0][1]["subject_absent"]
    after = apply_candidate(prepared, {}, spec("weak-lower-confidence"))
    assert after[1]["subject_absent"]
    assert digest(p) == original


def test_continuity_restores_canonical_identity_after_recovery():
    p = photos()
    p[1]["evidence"][0]["sources"][0]["predictions"][0]["score"] = 0.99
    original = digest(p)
    recovered = prepare_baseline(p, {})[0][1]
    assert not recovered["subject_absent"]
    assert recovered["species_keys"][recovered["species_top5"][0][3]] == "inat:1"
    assert digest(p) == original


def test_lower_matching_confidence_recovers_without_mutating_evidence():
    p = photos()
    original = digest(p)
    prepared = prepare_baseline(p, {})
    assert prepared[0][1]["subject_absent"]
    after = apply_candidate(prepared, {}, spec("weak-lower-confidence"))
    assert after[1]["subject_uncertain"] and not after[1]["subject_absent"]
    assert after[1]["species_top5"] == []
    assert after[1]["grouping_species_top5"][0][1] == 0.65
    assert digest(p) == original
    assert prepared[0][1]["subject_absent"]


@pytest.mark.parametrize(
    "change", ["conflicting_model", "folder", "empty_box", "too_long", "visual_conflict", "disabled"]
)
def test_relaxed_weak_rules_keep_independent_guards(change):
    p = photos()
    config = {}
    if change == "conflicting_model":
        p[1]["evidence"][0]["sources"].append(
            {"model": "two", "mode": "exclusive", "predictions": [{"name": "Other", "taxon": "inat:2", "score": 0.99}]}
        )
    elif change == "folder":
        p[1]["folder_id"] = 2
    elif change == "empty_box":
        p[1]["evidence"] = []
    elif change == "too_long":
        p[2]["timestamp"] = "2026-01-01T00:01:00"
    elif change == "visual_conflict":
        p[0]["dino_global_embedding"] = [1.0, 0.0]
        p[1]["dino_global_embedding"] = [0.0, 1.0]
    else:
        config = {"pipeline": {"weak_detection_rescue_enabled": False}}
    after = apply_candidate(prepare_baseline(p, config), config, spec("weak-lower-confidence"))
    assert after[1]["subject_absent"]


def test_relaxed_support_threshold_also_applies_to_conflicting_classifiers():
    # An independent full-image classifier predicts a different species at
    # 0.65 — above the weak-lower-confidence candidate's own support
    # threshold (0.6/0.2) but below the fixed production veto (0.8/0.6).
    # The candidate must veto the rescue so lowering the support threshold
    # is symmetric for contradictory evidence.
    p = photos()
    p[1]["evidence"].append(
        {
            "id": 99,
            "detector_model": "full-image",
            "category": "animal",
            "detector_confidence": 0.0,
            "box_x": 0.0,
            "box_y": 0.0,
            "box_w": 1.0,
            "box_h": 1.0,
            "sources": [
                {
                    "model": "two",
                    "mode": "exclusive",
                    "predictions": [{"name": "Other", "score": 0.65, "taxon": "inat:2"}],
                }
            ],
        }
    )
    after = apply_candidate(prepare_baseline(p, {}), {}, spec("weak-lower-confidence"))
    assert after[1]["subject_absent"]


def test_anchor_only_abstains_and_does_not_manufacture_classifier_scores():
    p = photos()
    p[1]["evidence"][0]["sources"] = []
    after = apply_candidate(prepare_baseline(p, {}), {}, spec("weak-anchor-context"))
    assert after[1]["subject_uncertain"]
    assert after[1]["grouping_species_top5"] == []
    assert after[1]["experimental_continuity"]["support"] == "anchor_context"


def test_larger_isolated_window_preserves_original_prediction_and_model_veto():
    p = photos(weak=False)
    prepared = prepare_baseline(p, {})
    assert "grouping_species_top5" not in prepared[0][1]
    after = apply_candidate(prepared, {}, spec("isolated-longer-span"))
    assert after[1]["grouping_species_top5"] == []
    assert after[1]["species_top5"][0][0] == "Other"
    p[1]["evidence"][0]["sources"].append({**copy.deepcopy(p[1]["evidence"][0]["sources"][0]), "model": "two"})
    after = apply_candidate(prepare_baseline(p, {}), {}, spec("isolated-longer-span"))
    assert "experimental_continuity" not in after[1]


def test_known_labels_do_not_change_candidate_decisions():
    p = photos()
    after = apply_candidate(prepare_baseline(p, {}), {}, spec("weak-lower-confidence"))
    p[1]["confirmed_species"] = "Something completely different"
    changed = apply_candidate(prepare_baseline(p, {}), {}, spec("weak-lower-confidence"))
    assert after[1]["grouping_species_top5"] == changed[1]["grouping_species_top5"]


def test_same_label_references_do_not_claim_human_review_or_animal_identity():
    p = photos()
    answers = {str(i): {"taxa": ["inat:1"], "sources": ["manual"], "complete": False} for i in range(3)}
    refs = pair_references(p, answers)
    assert len(refs) == 2 and all(not r["human_reviewed"] for r in refs)
    answers["1"]["taxa"].append("inat:2")
    assert pair_references(p, answers) == []


def test_safety_counts_catch_losses_hidden_by_gains_and_longer_boundary_joins():
    p = photos()
    p[1]["timestamp"] = "2026-01-01T00:00:05"
    p[2]["timestamp"] = "2026-01-01T00:00:06"
    answers = {
        str(i): {"taxa": ["inat:1" if i == 0 else "inat:2"], "sources": ["manual"], "complete": False} for i in range(3)
    }
    baseline = [Group((0,), ("inat:1",), ""), Group((1, 2), None, "")]
    candidate = [Group((0, 1, 2), ("inat:2",), "")]
    counts = comparison_counts(p, answers, candidate, baseline, pair_references(p, answers))
    assert counts["lost_previously_recovered_labels"] == 1
    assert counts["new_differing_label_joins"] == 1


def test_followup_never_reads_consumed_test_partition_and_preserves_inputs(tmp_path, monkeypatch):
    import encounter_eval.continuity_followup as followup

    scope = retained_scope(tmp_path)
    hashes = {p: p.read_bytes() for p in scope.rglob("*") if p.is_file()}
    original = followup.read_bundle

    def read(path, entry):
        assert entry["partition"] != "test"
        return original(path, entry)

    monkeypatch.setattr(followup, "read_bundle", read)
    output = tmp_path / "result"
    result = run([scope], output)
    assert result["selected"]["id"] == "current" and not result["test_evaluated"]
    assert all(p.read_bytes() == v for p, v in hashes.items())
    checks = json.loads((output / "inferred-regression-checks.json").read_text())
    assert not checks["human_reviews"] and len(checks["references"]) == 4
    with pytest.raises(FileExistsError):
        run([scope], output)


def test_missing_review_case_cannot_silently_pass(tmp_path):
    scope = retained_scope(tmp_path)
    with pytest.raises(ValueError, match="every reviewed"):
        run(
            [scope],
            tmp_path / "result",
            constraints=[
                {
                    "id": "unknown",
                    "workspace": 1,
                    "session": "missing",
                    "ids": [99],
                    "expected_groups": [[99]],
                }
            ],
        )


def test_current_grouping_cannot_fall_back_through_a_failing_reviewed_boundary(tmp_path):
    # The retained fixture's training session groups photos 10, 11 and 12 into
    # one encounter; a constraint that expects a split between 11 and 12
    # therefore fails on the current baseline, and run must refuse rather than
    # freeze a selection whose reviewed_cases carry passed: false.
    scope = retained_scope(tmp_path)
    with pytest.raises(ValueError, match="reviewed boundary"):
        run(
            [scope],
            tmp_path / "result",
            constraints=[
                {
                    "id": "split-11-12",
                    "workspace": 1,
                    "session": "train",
                    "partition": "train",
                    "ids": [10, 11, 12],
                    "expected_groups": [[10, 11], [12]],
                }
            ],
        )


def test_final_test_constraints_are_rejected(tmp_path):
    scope = retained_scope(tmp_path)
    with pytest.raises(ValueError, match="final-test"):
        run([scope], tmp_path / "result", constraints=[{"id": "test", "partition": "test"}])


def test_new_split_of_same_labels_requires_review():
    from encounter_eval.continuity_followup import _changed_cases

    p = photos()
    answers = {str(i): {"taxa": ["inat:1"], "sources": ["manual"], "complete": False} for i in range(3)}
    before = [Group((0, 1, 2), ("inat:1",), "")]
    after = [Group((0,), ("inat:1",), ""), Group((1, 2), ("inat:1",), "")]
    cases = _changed_cases(p, answers, before, after, {"session": "test"}, set())
    assert cases[0]["needs_review"]


def test_ambiguous_report_exports_a_replayable_snapshot(tmp_path):
    import gzip

    from encounter_eval.algorithms import run_algorithm
    from encounter_eval.common import code_identity, encode, write_json
    from encounter_eval.continuity_followup import _changed_cases
    from encounter_eval.continuity_followup_report import build
    from encounter_eval.grouping_dataset import check, import_reviews
    from encounter_eval.library import read_bundle

    scope = retained_scope(tmp_path)
    manifest = json.loads((scope / "manifest.json").read_text())
    entry = manifest["sessions"][0]
    bundle = read_bundle(scope, entry)
    middle = bundle["photos"][1]
    middle.update(subject_present=False, subject_absent=True, species_top5=[])
    middle["evidence"][0]["detector_confidence"] = 0.05
    middle["evidence"][0]["sources"][0]["predictions"][0]["score"] = 0.65
    del bundle["answers"][str(middle["id"])]
    (scope / entry["path"]).write_bytes(gzip.compress(encode(bundle).encode()))
    entry["digest"] = digest(bundle)
    write_json(scope / "manifest.json", manifest)
    prepared = prepare_baseline(bundle["photos"], {})
    selected = spec("weak-lower-confidence")
    after = apply_candidate(prepared, {}, selected)
    before_groups = run_algorithm("production", prepared[0])
    after_groups = run_algorithm("production", after)
    context = {
        "workspace": 1,
        "session": entry["id"],
        "partition": "train",
        "scope": str(scope),
        "input_digest": entry["digest"],
    }
    cases = _changed_cases(after, bundle["answers"], before_groups, after_groups, context, set())
    assert len(cases) == 1 and cases[0]["needs_review"]
    comparison = tmp_path / "comparison"
    comparison.mkdir()
    measured = {"train": {"counts": {"photos": 3}}, "development": {"counts": {"photos": 0}}}
    write_json(comparison / "summary.json", {"selected": selected, "baseline": measured, "selected_metrics": measured})
    write_json(comparison / "changed-cases.json", {"cases": cases})
    frozen_manifest = json.loads((scope / "manifest.json").read_text())
    write_json(
        comparison / "search-design.json",
        {
            "source": code_identity(configure_repo()),
            "scopes": [{"path": str(scope), "manifest_digest": digest(frozen_manifest)}],
        },
    )
    report = tmp_path / "review"
    result = build(comparison, report)
    assert result["cases"] == 1
    content = (report / "Review uncertain encounter sequences.html").read_text()
    assert "__COMPARISON_DATA__" not in content
    created = json.loads((report / "comparison-summary.json").read_text())["created_at"]
    export = tmp_path / "decisions.json"
    write_json(
        export,
        {
            "comparison_created_at": created,
            "decisions": [
                {
                    "case_id": cases[0]["id"],
                    "decision": "good",
                    "notes": "fixture",
                    "updated_at": "2026-10-04T18:00:00+00:00",
                }
            ],
        },
    )
    dataset = tmp_path / "reviews.sqlite"
    assert import_reviews(dataset, report, export)["imported"] == 1
    replay = check(dataset)
    assert replay["counts"]["passed_cases"] == 1


def test_report_rejects_source_drifted_from_frozen_comparison(tmp_path):
    from encounter_eval.common import code_identity, write_json
    from encounter_eval.continuity_followup_report import build

    scope = retained_scope(tmp_path)
    manifest = json.loads((scope / "manifest.json").read_text())
    entry = manifest["sessions"][0]
    comparison = tmp_path / "comparison"
    comparison.mkdir()
    measured = {"train": {"counts": {"photos": 1}}, "development": {"counts": {"photos": 0}}}
    write_json(
        comparison / "summary.json",
        {"selected": spec("weak-lower-confidence"), "baseline": measured, "selected_metrics": measured},
    )
    cases = [
        {
            "id": "case-0",
            "needs_review": True,
            "scope": str(scope),
            "session": entry["id"],
            "input_digest": entry["digest"],
            "ids": [0],
            "before": [],
            "after": [],
        }
    ]
    write_json(comparison / "changed-cases.json", {"cases": cases})
    drifted = dict(code_identity(configure_repo()))
    drifted["source_digest"] = "0" * 64
    write_json(comparison / "search-design.json", {"source": drifted})
    with pytest.raises(ValueError, match="Current checkout differs"):
        build(comparison, tmp_path / "report")


def test_acceptable_rejects_candidates_that_add_more_incorrect_additions():
    from encounter_eval.continuity_followup import acceptable

    baseline = {
        "metrics": {
            "counts": {
                "positive_labels": 10,
                "recovered_positive_labels": 6,
                "incorrect_additions": 2,
                "different_label_short_joins": 0,
                "unverified_additions": 0,
            },
            "objective": 0.4,
        },
        "reviewed_cases": [],
    }
    improved_recall_but_worse_additions = {
        "metrics": {
            "counts": {
                "positive_labels": 10,
                "recovered_positive_labels": 9,
                "incorrect_additions": 3,
                "different_label_short_joins": 0,
                "unverified_additions": 0,
            },
            "objective": 0.1,
        },
        "reviewed_cases": [],
    }
    assert not acceptable(improved_recall_but_worse_additions, baseline)
    # The same gain with no new known-wrong additions is accepted.
    unchanged_additions = {
        "metrics": {
            "counts": {
                "positive_labels": 10,
                "recovered_positive_labels": 9,
                "incorrect_additions": 2,
                "different_label_short_joins": 0,
                "unverified_additions": 0,
            },
            "objective": 0.1,
        },
        "reviewed_cases": [],
    }
    assert acceptable(unchanged_additions, baseline)


def test_report_rejects_retained_manifest_drift(tmp_path):
    from encounter_eval.common import code_identity, write_json
    from encounter_eval.continuity_followup_report import build

    scope = retained_scope(tmp_path)
    manifest = json.loads((scope / "manifest.json").read_text())
    entry = manifest["sessions"][0]
    comparison = tmp_path / "comparison"
    comparison.mkdir()
    measured = {"train": {"counts": {"photos": 1}}, "development": {"counts": {"photos": 0}}}
    write_json(
        comparison / "summary.json",
        {"selected": spec("weak-lower-confidence"), "baseline": measured, "selected_metrics": measured},
    )
    cases = [
        {
            "id": "case-0",
            "needs_review": True,
            "scope": str(scope),
            "session": entry["id"],
            "input_digest": entry["digest"],
            "ids": [0],
            "before": [],
            "after": [],
        }
    ]
    write_json(comparison / "changed-cases.json", {"cases": cases})
    write_json(
        comparison / "search-design.json",
        {
            "source": code_identity(configure_repo()),
            "scopes": [{"path": str(scope), "manifest_digest": digest(manifest)}],
        },
    )
    # Mutate the manifest on disk after freezing the comparison without
    # changing the per-session bundle digests the replay check inspects.
    manifest["taxonomy_display"] = {"inat:1": "Renamed bird"}
    write_json(scope / "manifest.json", manifest)
    with pytest.raises(ValueError, match="Retained scope manifest changed"):
        build(comparison, tmp_path / "report")


def test_duplicate_sessions_with_differing_inputs_are_rejected(tmp_path):
    # Two scopes may list the same session with the same photo IDs under the
    # same source_library yet disagree on their manifest configuration or
    # per-session digest, which would silently drop the second scope's
    # evidence and grouping even though it is not interchangeable. The run
    # must refuse rather than let selection depend on scope argument order.
    scope_a = retained_scope(tmp_path / "a")
    scope_b = retained_scope(tmp_path / "b")
    manifest_b = json.loads((scope_b / "manifest.json").read_text())
    manifest_b["config"] = {"different": True}
    (scope_b / "manifest.json").write_text(json.dumps(manifest_b))
    with pytest.raises(ValueError, match="Duplicate sessions"):
        run([scope_a, scope_b], tmp_path / "result")


def test_identical_duplicate_scopes_are_deduplicated_silently(tmp_path):
    # When two scopes share the same session with matching inputs and
    # manifest, dropping the second one leaves selection unchanged.
    scope_a = retained_scope(tmp_path / "a")
    scope_b = retained_scope(tmp_path / "b")
    (scope_b / "manifest.json").write_bytes((scope_a / "manifest.json").read_bytes())
    result = run([scope_a, scope_b], tmp_path / "result")
    assert result["selected"]["id"] == "current"


def test_code_identity_covers_review_html_templates(tmp_path):
    # Report generation reads the HTML template beside the Python modules;
    # a template-only edit must change the frozen source digest so the
    # provenance guard refuses a drifted checkout. Build a repository-shaped
    # tree under tmp_path rather than mutating the shared source checkout,
    # so this test does not interfere with concurrent code_identity() calls
    # or leave stray files behind if interrupted.
    from encounter_eval.common import code_identity

    repo = tmp_path / "repo"
    (repo / "vireo").mkdir(parents=True)
    (repo / "vireo" / "encounters.py").write_bytes(b"# stub\n")
    src = repo / "tools" / "encounter-evaluation" / "src" / "encounter_eval"
    src.mkdir(parents=True)
    (src / "common.py").write_bytes(b"# stub\n")
    before = code_identity(repo)["source_digest"]
    (src / "continuity_review.html").write_bytes(b"<!-- template identity fixture -->\n")
    after = code_identity(repo)["source_digest"]
    assert before != after
