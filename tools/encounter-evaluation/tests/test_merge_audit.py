import gzip
import json

import pytest

from encounter_eval.common import configure_repo, digest, encode
from encounter_eval.merge_audit import build, join_reason, results, wilson

configure_repo()


def _photo(pid, second, name, taxon, score):
    detection = {
        "id": pid,
        "detector_model": "megadetector-v6",
        "category": "animal",
        "detector_confidence": 0.9,
        "box_x": 0.2,
        "box_y": 0.2,
        "box_w": 0.3,
        "box_h": 0.3,
        "sources": [
            {"model": "m", "mode": "exclusive", "predictions": [{"name": name, "taxon": taxon, "score": score}]}
        ],
    }
    return {
        "id": pid,
        "folder_id": 1,
        "filename": f"{pid}.jpg",
        "timestamp": f"2026-01-01T00:00:{second:02}",
        "subject_present": True,
        "subject_absent": False,
        "species_top5": [(name, score, "m", "taxon:" + taxon.split(":")[1])],
        "species_keys": {"taxon:" + taxon.split(":")[1]: taxon},
        "evidence": [detection],
    }


@pytest.fixture
def scope(tmp_path):
    """Train: a differing-tag pair the classifier sees as one species (joined).
    Development: the same tags with confident different species (cut).
    Test: an unreadable bundle, which proves held-out sessions are never opened."""
    root = tmp_path / "scope"
    (root / "inputs").mkdir(parents=True)
    entries = []
    sessions = {
        "train": [
            _photo(1, 0, "Teal", "inat:1", 0.99),
            _photo(2, 2, "Teal", "inat:1", 0.99),
            _photo(3, 4, "Teal", "inat:1", 0.99),
        ],
        "development": [_photo(11, 0, "Teal", "inat:1", 0.99), _photo(12, 2, "Heron", "inat:2", 0.99)],
    }
    tags = {1: "inat:1", 2: "inat:2", 3: "inat:2", 11: "inat:1", 12: "inat:2"}
    for partition, photos in sessions.items():
        answers = {str(p["id"]): {"taxa": [tags[p["id"]]], "sources": ["manual"], "complete": False} for p in photos}
        presentation = {str(p["id"]): {"filename": p["filename"], "file_hash": f"h{p['id']}"} for p in photos}
        bundle = {"photos": photos, "answers": answers, "presentation": presentation}
        path = f"inputs/{partition}.json.gz"
        with gzip.open(root / path, "wt") as handle:
            handle.write(encode(bundle))
        entries.append({"id": partition, "partition": partition, "path": path, "digest": digest(bundle)})
    (root / "inputs/test.json.gz").write_bytes(b"not a bundle")
    entries.append({"id": "test", "partition": "test", "path": "inputs/test.json.gz", "digest": "x"})
    manifest = {
        "sessions": entries,
        "workspace": 1,
        "config": {},
        "grouping_config": {},
        "source_library": "fixture",
        "taxonomy_display": {"inat:1": "Teal", "inat:2": "Heron"},
    }
    (root / "manifest.json").write_text(json.dumps(manifest))
    return root


def test_build_lists_only_joined_differing_pairs_and_skips_test(scope, tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))  # no cached previews
    summary = build([scope], tmp_path / "audit", context=1)
    assert summary["joined_pairs"] == 1 and summary["differing_label_pairs"] == 2
    [case] = json.loads((tmp_path / "audit/merge-audit.json").read_text())
    assert case["pair"] == [1, 2] and case["partition"] == "train"
    assert case["labels"] == [["Teal"], ["Heron"]]
    assert case["reason"] == "same_confident_species"
    assert [p["id"] for p in case["photos"]] == [1, 2, 3]
    assert [p["pair_side"] for p in case["photos"]] == [0, 1, None]
    assert case["encounter"]["window"] == [1, 3]
    assert case["photos"][0]["predictions"] == [{"model": "m", "species": "Teal", "confidence": 0.99}]
    page = (tmp_path / "audit/Review merged encounters.html").read_text()
    assert "__AUDIT_DATA__" not in page and case["id"] in page
    written = json.loads((tmp_path / "audit/audit-summary.json").read_text())
    assert written["test_sessions_loaded"] == 0 and written["counts"]["sessions"] == 2
    assert json.loads((tmp_path / "audit/missing-previews.json").read_text())["photo_ids"] == [1, 2, 3]


def test_join_reason_distinguishes_evidence():
    teal, heron = {"species": "Teal"}, {"species": "Heron"}
    assert join_reason({"decision": "merged_back"}, teal, heron) == "merged_back"
    assert join_reason({"decision": "burst_id_kept"}, None, None) == "burst_id"
    assert join_reason({"decision": "kept"}, None, None) == "no_confident_species"
    assert join_reason({"decision": "kept"}, teal, None) == "one_confident_species"
    assert join_reason({"decision": "kept"}, teal, teal) == "same_confident_species"
    assert join_reason({"decision": "kept"}, teal, heron) == "different_confident_species"


def _export(audit, decisions):
    created = json.loads((audit / "audit-summary.json").read_text())["created_at"]
    path = audit.parent / "export.json"
    path.write_text(json.dumps({"audit_created_at": created, "decisions": decisions}))
    return path


@pytest.mark.parametrize(
    ("kind", "expected"), [("keep", [[1, 2]]), ("tag_error", [[1, 2]]), ("split", [[1], [2]]), ("unsure", None)]
)
def test_results_write_explicit_pair_constraints(scope, tmp_path, kind, expected):
    audit = tmp_path / "audit"
    build([scope], audit)
    [case] = json.loads((audit / "merge-audit.json").read_text())
    decision = {"case_id": case["id"], "decision": kind, "notes": "n", "updated_at": "2026-10-04T12:00:00+00:00"}
    result = results(audit, _export(audit, [decision]), tmp_path / "out")
    constraints = json.loads((tmp_path / "out/merge-audit-constraints.json").read_text())
    if expected is None:
        assert constraints == [] and result["overall"]["answered"] == 0
        assert result["estimated_wrong_merges"] is None
        return
    [constraint] = constraints
    assert constraint["ids"] == [1, 2] and constraint["expected_groups"] == expected
    assert constraint["workspace"] == 1 and constraint["session"] == "train"
    assert [p["filename"] for p in constraint["photos"]] == ["1.jpg", "2.jpg"]
    assert result["overall"]["wrong_merge_rate"] == (1.0 if kind == "split" else 0.0)
    corrections = json.loads((tmp_path / "out/tag-corrections.json").read_text())
    assert len(corrections) == (kind == "tag_error")


def test_constraints_replay_through_boundary_scoring(scope, tmp_path):
    from encounter_eval.grouping_dataset import boundary_errors

    audit = tmp_path / "audit"
    build([scope], audit)
    [case] = json.loads((audit / "merge-audit.json").read_text())
    decision = {"case_id": case["id"], "decision": "split", "updated_at": "2026-10-04T12:00:00+00:00"}
    results(audit, _export(audit, [decision]), tmp_path / "out")
    [constraint] = json.loads((tmp_path / "out/merge-audit-constraints.json").read_text())
    # The current grouping joins the pair, so a reviewed split is an incorrect merge.
    errors = boundary_errors(constraint["ids"], constraint["expected_groups"], [[1, 2, 3]])
    assert errors == {"reviewed_splits": 1, "incorrect_merges": 1}


def test_results_extend_existing_constraints_without_duplicates(scope, tmp_path):
    audit = tmp_path / "audit"
    build([scope], audit)
    [case] = json.loads((audit / "merge-audit.json").read_text())
    decision = {"case_id": case["id"], "decision": "keep", "updated_at": "2026-10-04T12:00:00+00:00"}
    existing = tmp_path / "existing.json"
    existing.write_text(json.dumps([{"id": "earlier", "ids": [5, 6], "expected_groups": [[5, 6]]}]))
    results(audit, _export(audit, [decision]), tmp_path / "out", existing=existing)
    combined = json.loads((tmp_path / "out/reviewed-constraints.json").read_text())
    assert [c["id"] for c in combined] == ["earlier", "merge-audit-" + case["id"]]
    existing.write_text(json.dumps(combined))
    with pytest.raises(ValueError, match="already present"):
        results(audit, _export(audit, [decision]), tmp_path / "again", existing=existing)


@pytest.mark.parametrize("change", ["other_audit", "unknown_case", "unknown_decision", "naive_time", "duplicate"])
def test_results_reject_mismatched_exports(scope, tmp_path, change):
    audit = tmp_path / "audit"
    build([scope], audit)
    [case] = json.loads((audit / "merge-audit.json").read_text())
    decision = {"case_id": case["id"], "decision": "keep", "updated_at": "2026-10-04T12:00:00+00:00"}
    decisions = [decision]
    if change == "unknown_case":
        decision["case_id"] = "missing"
    elif change == "unknown_decision":
        decision["decision"] = "good"
    elif change == "naive_time":
        decision["updated_at"] = "2026-10-04T12:00:00"
    elif change == "duplicate":
        decisions = [decision, dict(decision)]
    path = _export(audit, decisions)
    if change == "other_audit":
        data = json.loads(path.read_text())
        data["audit_created_at"] = "2020-01-01T00:00:00+00:00"
        path.write_text(json.dumps(data))
    with pytest.raises(ValueError):
        results(audit, path, tmp_path / "out")


def test_wilson_interval_bounds_small_samples():
    assert wilson(0, 0) is None
    low, high = wilson(0, 10)
    assert low == 0 and 0.2 < high < 0.35
    low, high = wilson(5, 10)
    assert low < 0.5 < high


def test_results_do_not_extrapolate_answered_rate_over_abstentions(scope, tmp_path):
    audit = tmp_path / "audit"
    build([scope], audit)
    cases = json.loads((audit / "merge-audit.json").read_text())
    second = dict(cases[0], id="another-pair")
    (audit / "merge-audit.json").write_text(json.dumps(cases + [second]))
    summary = json.loads((audit / "audit-summary.json").read_text())
    summary["counts"]["joined_pairs"] = 2
    (audit / "audit-summary.json").write_text(json.dumps(summary))
    decisions = [
        {"case_id": case["id"], "decision": kind, "updated_at": "2026-10-05T06:00:00+00:00"}
        for case, kind in zip(cases + [second], ["split", "unsure"], strict=True)
    ]
    result = results(audit, _export(audit, decisions), tmp_path / "out")
    assert result["overall"]["answered"] == 1
    assert result["overall"]["wrong_merge_rate"] == 1.0
    assert result["estimated_wrong_merges"] is None
    assert result["constraints_written"] == 1


@pytest.mark.parametrize("expected", [[[2], [1]], [[3, 2], [1]]])
def test_results_reject_conflicting_existing_boundaries(scope, tmp_path, expected):
    audit = tmp_path / "audit"
    build([scope], audit)
    [case] = json.loads((audit / "merge-audit.json").read_text())
    decision = {"case_id": case["id"], "decision": "keep", "updated_at": "2026-10-05T06:00:00+00:00"}
    existing = tmp_path / "existing.json"
    existing.write_text(json.dumps([{
        "id": "earlier-review", "workspace": case["workspace"], "session": case["session"],
        "ids": [pid for group in expected for pid in group], "expected_groups": expected,
    }]))
    with pytest.raises(ValueError, match="Conflicting constraints"):
        results(audit, _export(audit, [decision]), tmp_path / "out", existing=existing)
    assert not (tmp_path / "out").exists()
