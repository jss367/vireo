import gzip
import json

import pytest

from encounter_eval.common import digest, encode
from encounter_eval.label_benchmark import candidates, run


def retained_scope(tmp_path):
    scope = tmp_path / "scope"
    (scope / "inputs").mkdir(parents=True)
    entries = []
    for day, partition in enumerate(("train", "development", "test"), 1):
        photos, answers, presentation = [], {}, {}
        for i in range(3):
            pid = day * 10 + i
            d = {
                "id": pid,
                "detector_model": "megadetector-v6",
                "category": "animal",
                "detector_confidence": 0.8,
                "box_x": 0.2,
                "box_y": 0.2,
                "box_w": 0.3,
                "box_h": 0.3,
                "sources": [
                    {
                        "model": "m",
                        "mode": "exclusive",
                        "predictions": [{"name": "Bird", "taxon": "inat:1", "score": 0.99}],
                    }
                ],
            }
            photos.append(
                {
                    "id": pid,
                    "folder_id": day,
                    "filename": f"{pid}.jpg",
                    "timestamp": f"2026-01-0{day}T00:00:00.{i}00",
                    "subject_present": True,
                    "subject_absent": False,
                    "species_top5": [("Bird", 0.99, "m", "taxon:1")],
                    "species_keys": {"taxon:1": "inat:1"},
                    "evidence": [d],
                }
            )
            answers[str(pid)] = {"taxa": ["inat:1"], "sources": ["manual"], "complete": False}
            presentation[str(pid)] = {"filename": f"{pid}.jpg", "file_hash": f"hash-{pid}"}
        bundle = {"photos": photos, "answers": answers, "presentation": presentation}
        filename = f"inputs/{partition}.json.gz"
        with gzip.open(scope / filename, "wt") as f:
            f.write(encode(bundle))
        entries.append({"id": partition, "partition": partition, "path": filename, "digest": digest(bundle)})
    (scope / "manifest.json").write_text(
        json.dumps(
            {"sessions": entries, "workspace": 1, "config": {}, "source_library": "fixture", "grouping_config": {}}
        )
    )
    return scope


def test_default_candidate_inventory_preserves_the_experiment():
    specs = candidates()
    assert len(specs) == 34 and len({s["id"] for s in specs}) == 34


def test_training_never_loads_final_test_answers_without_request(tmp_path, monkeypatch):
    import encounter_eval.label_benchmark as benchmark

    scope = retained_scope(tmp_path)
    original = benchmark.read_bundle

    def read(path, entry):
        assert entry["partition"] != "test"
        return original(path, entry)

    monkeypatch.setattr(benchmark, "read_bundle", read)
    result = run([scope], tmp_path / "results", trials=1)
    assert not result["test_evaluated"]
    assert not (tmp_path / "results/test-results.json").exists()


def test_candidate_is_frozen_before_final_test_and_evidence_is_immutable(tmp_path, monkeypatch):
    import encounter_eval.label_benchmark as benchmark

    scope = retained_scope(tmp_path)
    output = tmp_path / "results"
    original = benchmark.read_bundle
    hashes = {p: p.read_bytes() for p in scope.rglob("*") if p.is_file()}

    def read(path, entry):
        if entry["partition"] == "test":
            frozen = json.loads((output / "frozen-selection.json").read_text())
            assert not frozen["test_outcomes_seen"]
        return original(path, entry)

    monkeypatch.setattr(benchmark, "read_bundle", read)
    result = run([scope], output, trials=1, evaluate_test=True)
    assert result["test_evaluated"] and result["test_selected"]["positive_recall"] == 1
    assert all(p.read_bytes() == content for p, content in hashes.items())
    with pytest.raises(FileExistsError):
        run([scope], output, trials=1)


def test_final_test_review_constraints_are_rejected(tmp_path):
    scope = retained_scope(tmp_path)
    with pytest.raises(ValueError, match="final-test"):
        run([scope], tmp_path / "results", constraints=[{"partition": "test"}])
