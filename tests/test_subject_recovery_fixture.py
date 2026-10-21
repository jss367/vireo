"""Safety checks for the reproducible subject-recovery scratch preparation."""
import argparse
import hashlib
import importlib.util
import json
import sqlite3
from pathlib import Path

import pytest
from canonical_schema import CanonicalSchema

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/verify_subject_analysis_recovery.py"
spec = importlib.util.spec_from_file_location("subject_recovery_fixture", SCRIPT)
fixture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture)


def fixture_arguments(tmp_path):
    live = tmp_path / "live"
    live.mkdir()
    original = live / "duck.jpg"
    original.write_bytes(b"source photo bytes")
    original.with_suffix(".xmp").write_text("<xmp:Rating>0</xmp:Rating>")
    catalog = live / "catalog.db"
    conn = sqlite3.connect(catalog)
    CanonicalSchema(conn).create_tables()
    conn.execute("INSERT INTO workspaces(id,name) VALUES(5,'Birds')")
    conn.execute("INSERT INTO folders(id,path,name) VALUES(1,?,'Originals')", (str(live),))
    conn.execute("INSERT INTO workspace_folders(workspace_id,folder_id,is_root) VALUES(5,1,1)")
    conn.execute("""INSERT INTO photos(id,folder_id,filename,extension,file_size,file_mtime,file_hash,flag,rating)
                    VALUES(7,1,'duck.jpg','.jpg',?,?,?,'flagged',4)""",
                 (original.stat().st_size, original.stat().st_mtime, hashlib.sha256(original.read_bytes()).hexdigest()))
    conn.commit()
    conn.close()
    models = tmp_path / "models" / "tiny"
    models.mkdir(parents=True)
    (models / "model.onnx").write_bytes(b"model fixture")
    config = live / "config.json"
    config.write_text(json.dumps({"detector_confidence": .2, "hf_token": "must-not-copy"}))
    membership = tmp_path / "membership.json"
    membership.write_text(json.dumps({"encounter": {"photo_ids": [7]}, "photos": [{"id": 7}]}))
    return argparse.Namespace(catalog=catalog, config=config, models=models.parent,
                              model_variants=["tiny"], membership=membership,
                              workspace=5, output=tmp_path / "scratch")


def test_prepare_copies_exact_members_without_live_catalog_or_source_changes(tmp_path):
    args = fixture_arguments(tmp_path)
    before = fixture.snapshot(args.catalog)
    original_before = fixture.snapshot(args.catalog.parent / "duck.jpg")
    fixture.prepare(args)
    assert fixture.snapshot(args.catalog) == before
    assert fixture.snapshot(args.catalog.parent / "duck.jpg") == original_before
    conn = sqlite3.connect(args.output / "catalog.db")
    row = conn.execute("SELECT id,flag,rating,working_copy_path FROM photos").fetchone()
    assert row == (7, "flagged", 4, None)
    assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    source = Path(conn.execute("SELECT path FROM folders WHERE id=1").fetchone()[0]) / "duck.jpg"
    assert source.is_relative_to(args.output)
    assert source.read_bytes() == b"source photo bytes"
    assert source.stat().st_mtime_ns == original_before["mtime_ns"]
    assert json.loads((args.output / "config.json").read_text()) == {
        "detector_confidence": .2, "sync_flags_to_xmp": False,
    }
    fixture.check(argparse.Namespace(scratch=args.output))


def test_prepare_refuses_existing_output_or_live_catalog_directory(tmp_path):
    args = fixture_arguments(tmp_path)
    args.output.mkdir()
    with pytest.raises(ValueError, match="new directory"):
        fixture.prepare(args)
    args.output = args.catalog.parent / "scratch"
    with pytest.raises(ValueError, match="outside"):
        fixture.prepare(args)
    assert not args.output.exists()


def test_prepare_rejects_guessed_or_duplicate_membership(tmp_path):
    args = fixture_arguments(tmp_path)
    args.membership.write_text(json.dumps({"encounter": {"photo_ids": [7, 8]}, "photos": [{"id": 7}]}))
    with pytest.raises(ValueError, match="exact saved"):
        fixture.prepare(args)
    assert not args.output.exists()


def test_inference_guard_denies_live_writes_but_allows_readonly_catalog(tmp_path, monkeypatch):
    import os
    import sys
    path = SCRIPT.with_name("run_subject_retry_verification.py")
    guard_spec = importlib.util.spec_from_file_location("retry_verification_runner", path)
    runner = importlib.util.module_from_spec(guard_spec)
    guard_spec.loader.exec_module(runner)
    hooks = []
    monkeypatch.setattr(sys, "addaudithook", hooks.append)
    protected = tmp_path / "live"
    runner.block_original_writes([protected])
    guard = hooks[0]
    catalog = str(protected / "catalog.db")
    guard("open", (catalog, "r", 0))
    guard("sqlite3.connect", ("file:" + catalog + "?mode=ro",))
    guard("open", (str(tmp_path / "scratch/catalog.db"), "w", os.O_CREAT | os.O_RDWR))
    for event, args in [
        ("open", (catalog, "w", os.O_CREAT | os.O_RDWR)),
        ("sqlite3.connect", (catalog,)),
        ("os.remove", (catalog, -1)),
        ("os.rename", (catalog, str(tmp_path / "copy.db"), -1, -1)),
        ("os.utime", (catalog, None, None, -1)),
        ("socket.connect", (object(), ("example.com", 443))),
    ]:
        with pytest.raises(PermissionError):
            guard(event, args)


def test_check_detects_changed_copy_without_modifying_original(tmp_path):
    args = fixture_arguments(tmp_path)
    fixture.prepare(args)
    copied = args.output / "originals" / "1" / "duck.jpg"
    copied.write_bytes(b"changed copy")
    with pytest.raises(ValueError, match="Source or source-copy"):
        fixture.check(argparse.Namespace(scratch=args.output))
    assert (args.catalog.parent / "duck.jpg").read_bytes() == b"source photo bytes"


def test_seed_refuses_a_different_saved_encounter(tmp_path):
    args = fixture_arguments(tmp_path)
    fixture.prepare(args)
    saved = tmp_path / "wrong-saved-review.json"
    saved.write_text(json.dumps({"encounters": [{"photo_ids": [8]}], "photos": [{"id": 8}]}))
    with pytest.raises(ValueError, match="exact original encounter"):
        fixture.seed(argparse.Namespace(scratch=args.output, saved_review=saved))


def test_check_detects_live_catalog_measurement_changes(tmp_path):
    args = fixture_arguments(tmp_path)
    fixture.prepare(args)
    with sqlite3.connect(args.catalog) as conn:
        conn.execute("UPDATE photos SET width=100 WHERE id=7")
    with pytest.raises(ValueError, match="Selected live catalog rows changed"):
        fixture.check(argparse.Namespace(scratch=args.output))


def test_prepare_limits_prediction_reviews_to_copied_workspace(tmp_path):
    args = fixture_arguments(tmp_path)
    with sqlite3.connect(args.catalog) as conn:
        conn.execute("INSERT INTO workspaces(id,name) VALUES(6,'Other workspace')")
        conn.execute("""INSERT INTO detections(id,photo_id,detector_model,box_x,box_y,box_w,box_h,detector_confidence,category)
                        VALUES(20,7,'test-model',.1,.1,.5,.5,.9,'animal')""")
        conn.execute("""INSERT INTO predictions(id,detection_id,classifier_model,labels_fingerprint,species,confidence)
                        VALUES(30,20,'test-classifier','labels','Duck',.9)""")
        conn.executemany("INSERT INTO prediction_review(prediction_id,workspace_id,status) VALUES(30,?,?)",
                         [(5, "accepted"), (6, "rejected")])
    fixture.prepare(args)
    with sqlite3.connect(args.output / "catalog.db") as conn:
        assert conn.execute("SELECT workspace_id,status FROM prediction_review").fetchall() == [(5, "accepted")]
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []


@pytest.mark.parametrize("fault,message", [
    ("decision", "Photographer decisions changed"),
    ("unselected", "Unselected photo rows changed"),
    ("grouping", "Saved encounter or burst membership changed"),
    ("keep", "Existing photographer keep was lost"),
    ("ranking", "valid cached ranking generation changed"),
])
def test_preservation_checks_fail_under_python_optimization(tmp_path, fault, message):
    import subprocess
    import sys
    from copy import deepcopy

    before = {1: {"flag": "none", "rating": 0, "width": 10},
              2: {"flag": "none", "rating": 0, "width": 10},
              216716: {"flag": "flagged", "rating": 0, "width": 10}}
    review = {"photos": [{"id": 1, "label": "REVIEW"}, {"id": 216716, "label": "KEEP"}],
              "encounters": [{"photo_ids": [1, 216716], "bursts": [{"photo_ids": [1, 216716]}]}]}
    after = deepcopy(before)
    refreshed = deepcopy(review)
    if fault == "decision":
        after[1]["flag"] = "rejected"
    elif fault == "unselected":
        after[2]["width"] = 20
    elif fault == "grouping":
        refreshed["encounters"][0]["bursts"][0]["photo_ids"] = [1]
    elif fault == "keep":
        refreshed["photos"][1]["label"] = "REVIEW"
    payload = {"before_rows": before, "after_rows": after, "ids": [1],
               "expected": {pid: [row["flag"], row["rating"]] for pid, row in before.items()},
               "before_review": review, "after_review": refreshed, "persisted_review": deepcopy(refreshed)}
    program = """import importlib.util,json,sys
from pathlib import Path
spec=importlib.util.spec_from_file_location('runner',sys.argv[1])
runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
payload=json.loads(sys.argv[2])
for key in ('before_rows','after_rows','expected'):
    payload[key]={int(pid):value for pid,value in payload[key].items()}
payload['expected']={pid:tuple(value) for pid,value in payload['expected'].items()}
if sys.argv[4]=='ranking':
    runner.verify_ranking_preservation({(1,'key'):'original'},{(1,'key'):'changed'},None)
else:
    runner.verify_preservation(**payload)
Path(sys.argv[3]).write_text('success')
"""
    success = tmp_path / "success.txt"
    completed = subprocess.run([sys.executable, "-O", "-c", program,
                                str(SCRIPT.with_name("run_subject_retry_verification.py")),
                                json.dumps(payload), str(success), fault], capture_output=True, text=True)
    assert completed.returncode != 0 and message in completed.stderr
    assert not success.exists()
