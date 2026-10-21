#!/usr/bin/env python3
"""Run selected-photo retry against prepared scratch data and installed models.

Uses actual inference. The implementation may be a separate checkout so the
verification commit need not duplicate a pending retry implementation. Source
files, live catalog/config/cache, and network writes are denied in this process.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace


def block_original_writes(roots: list[Path]) -> None:
    protected = [root.resolve() for root in roots]

    def contains(value: object) -> bool:
        if not isinstance(value, (str, bytes, os.PathLike)):
            return False
        path = Path(os.fsdecode(value).split("?", 1)[0].removeprefix("file:")).resolve()
        return any(path == root or root in path.parents for root in protected)

    def guard(event: str, args: tuple) -> None:
        if event == "socket.connect":
            raise PermissionError("Network disabled during isolated subject recovery")
        if event == "open" and contains(args[0]):
            mode, flags = args[1], args[2]
            if (isinstance(mode, str) and any(char in mode for char in "wax+")) or flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND):
                raise PermissionError(f"Protected original write: {args[0]}")
        if event in {"os.remove", "os.mkdir", "os.rmdir", "os.chmod", "os.utime", "os.truncate"} and contains(args[0]):
            raise PermissionError(f"Protected original mutation: {event}")
        if event in {"os.rename", "os.link", "os.symlink"} and any(contains(value) for value in args[:2]):
            raise PermissionError(f"Protected original mutation: {event}")
        if event == "sqlite3.connect" and contains(args[0]) and "mode=ro" not in str(args[0]):
            raise PermissionError("Live catalog connections must be explicitly read-only")

    sys.addaudithook(guard)


def photo_rows(db: object) -> dict[int, dict]:
    rows = db.conn.execute("SELECT * FROM photos ORDER BY id").fetchall()
    return {row["id"]: dict(row) for row in rows}


def row_digest(row: dict) -> str:
    values = {key: value.hex() if isinstance(value, bytes) else value for key, value in row.items()}
    return hashlib.sha256(json.dumps(values, sort_keys=True).encode()).hexdigest()


def group_members(data: dict) -> list:
    return [{"photo_ids": encounter["photo_ids"], "bursts": [
        burst["photo_ids"] if isinstance(burst, dict) else burst for burst in encounter.get("bursts", [])
    ]} for encounter in data["encounters"]]


def export_outcomes(report: dict, destination: Path) -> None:
    before = {photo["photo_id"]: photo for photo in report["before"]["photos"]}
    after = {photo["id"]: photo for photo in report["selected_photos_after"]}
    outcomes = {photo["photo_id"]: photo for photo in report["result"]["photos"]}
    timings = {photo["photo_id"]: photo for photo in report["photo_timings"]}
    rows = []
    floor = report.get("detector_confidence", .2)
    for pid in report["requested_photo_ids"]:
        filename = before[pid]["filename"]
        fresh = next((call["detections"] for call in report["inference_calls"] if call["stage"] == "detection" and call["filename"] == filename), None)
        rows.append({
            "Photo ID": pid, "Filename": filename,
            "Required stages before retry": ";".join(before[pid]["required_stages"]),
            "Stages actually run": ";".join(outcomes[pid].get("stages_run", [])),
            "Model inference seconds": timings[pid]["inference_seconds"],
            "Fresh raw detector boxes": len(fresh) if fresh is not None else "",
            "Selected confidence floor": floor,
            "Fresh animal boxes above selected floor": sum(box["category"] == "animal" and box["confidence"] >= floor for box in fresh) if fresh is not None else "",
            "Retry outcome": outcomes[pid]["status"],
            "Analysis eligible after": after[pid]["selection_eligible"],
            "Suggestion after": after[pid]["label"],
            "Reasons after": ";".join(after[pid]["analysis_reasons"]),
        })
    with destination.open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--implementation", type=Path, default=Path(__file__).resolve().parents[1])
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--photo-ids", type=int, nargs="+")
    selection.add_argument("--all-saved-members", action="store_true")
    parser.add_argument("--max-photos", type=int, default=3)
    parser.add_argument("--invalidate-features", type=int)
    parser.add_argument("--concurrent-decision", type=int)
    parser.add_argument("--incomplete-ranking", type=int)
    parser.add_argument("--manual-duck-box", type=int)
    parser.add_argument("--report-name", default="pilot")
    args = parser.parse_args()
    root = args.scratch.resolve()
    manifest = json.loads((root / "manifest.json").read_text())
    ids = manifest["photo_ids"] if args.all_saved_members else args.photo_ids or [216692, 216716, 216713]
    if not 1 <= args.max_photos <= 200 or not 1 <= len(ids) <= args.max_photos or not set(ids) <= set(manifest["photo_ids"]):
        parser.error("Selection must fit the explicit limit and exact saved membership (maximum 200)")
    if args.concurrent_decision is not None and args.concurrent_decision not in ids:
        parser.error("Concurrent decision fixture must target a selected scratch frame")
    checkout = args.implementation.resolve()
    implementation_commit = subprocess.check_output(["git", "-C", str(checkout), "rev-parse", "HEAD"], text=True).strip()
    implementation = root / "implementations" / implementation_commit
    if not implementation.exists():
        implementation.mkdir(parents=True)
        archive = subprocess.check_output(["git", "-C", str(checkout), "archive", implementation_commit, "vireo"])
        subprocess.run(["tar", "-xf", "-", "-C", str(implementation)], input=archive, check=True)
    sys.path.insert(0, str(implementation / "vireo"))
    sys.dont_write_bytecode = True
    block_original_writes([Path(manifest["catalog"]).parent, root / "originals"])
    logging.basicConfig(filename=root / f"{args.report_name}-inference.log", level=logging.INFO)
    import config as cfg
    cfg.CONFIG_PATH = str(root / "config.json")
    import detector
    import dino_embed
    import keypoints
    import masking
    import onnx_runtime
    import resource_ledger
    from db import Database
    from pipeline import load_results, load_results_raw
    from services.analysis_retry import explain_selected, run_retry

    resource_ledger._DEFAULT_LEDGER = resource_ledger.ResourceLedger(4)
    onnx_runtime.get_providers = lambda: ["CPUExecutionProvider"]
    detector.MEGADETECTOR_ONNX_DIR = str(root / "models" / "megadetector-v6")
    detector.MEGADETECTOR_ONNX_PATH = str(root / "models" / "megadetector-v6" / "model.onnx")
    masking._sam2_model_dir = lambda variant: str(root / "models" / variant)
    dino_embed._dinov2_model_path = lambda variant: (
        str(root / "models" / f"dinov2-{variant}"), str(root / "models" / f"dinov2-{variant}" / "model.onnx"),
    )
    keypoints.MODELS_DIR = str(root / "models")
    db_path = str(root / "catalog.db")

    def database() -> Database:
        db = Database(db_path)
        db.set_active_workspace(manifest["workspace_id"])
        return db

    db = database()
    effective = db.get_effective_config(cfg.load())
    if not masking.sam2_status(effective["pipeline"]["sam2_variant"])["ready"]:
        raise RuntimeError("Selected SAM weights are not copied; downloads are prohibited")
    if args.manual_duck_box is not None:
        from services.detection_store import write_detection_batch
        from subjects import select_primary
        pid = args.manual_duck_box
        if ids != [pid]:
            parser.error("Manual positive verification must select only its one fixture photo")
        # Explicit verification fixture, distinct from automatic model output.
        # The agent visually checked the duck-sized person box before this run;
        # these coordinates were not supplied or confirmed by the user.
        candidate = db.conn.execute("SELECT * FROM detections WHERE photo_id=? AND category='person' ORDER BY detector_confidence DESC LIMIT 1", (pid,)).fetchone()
        if candidate is None:
            raise RuntimeError("Manual fixture needs the observed duck-sized person box")
        chosen = write_detection_batch(db, pid, "manual-verification-box", [{
            "box": {key: candidate["box_" + key] for key in "xywh"}, "confidence": 1.0, "category": "animal",
        }], "manual-fixture")
        select_primary(db, pid, chosen[0])
    if args.invalidate_features is not None:
        if args.invalidate_features not in ids:
            parser.error("Only selected scratch features can be invalidated")
        db.subjects.clear_primary_features(args.invalidate_features)
        db.commit_with_retry()
    incomplete_generation = None
    if args.incomplete_ranking is not None:
        from subject_ranking import SubjectComparison, analysis_key, resolve_subject, source_path
        pid = args.incomplete_ranking
        if pid != args.invalidate_features or pid not in ids:
            parser.error("Incomplete generation fixture must match the selected invalidated photo")
        photo = db.photos.get(pid, verify_workspace=True)
        detection, _ = resolve_subject(db, pid, SubjectComparison())
        if detection is None:
            raise ValueError("Incomplete generation fixture requires an existing selected animal")
        key = analysis_key(source_path(db, photo), detection, effective)
        mask = db.masks_features.get_mask(pid, effective["pipeline"]["sam2_variant"])
        db.subjects.save_ranking_analysis(detection["id"], key, {"subject_size": .04}, Path(mask["path"]).read_bytes())
        db.commit_with_retry()
        incomplete_generation = {"photo_id": pid, "detection_id": detection["id"], "source_key": key}
    ranking_before = {(row["detection_id"], row["source_key"]): row_digest(dict(row))
                      for row in db.conn.execute("SELECT * FROM subject_ranking_analysis")}
    before_rows = photo_rows(db)
    before_review = load_results(str(root), manifest["workspace_id"], db=db)
    before = explain_selected(db, ids, effective)
    db.close()
    calls = []
    real_detect, real_mask = detector.detect_animals, masking.generate_mask
    decision_written = threading.Event()
    concurrent = []

    def write_concurrent_decision() -> None:
        started = time.monotonic()
        with sqlite3.connect(db_path) as conn:
            conn.execute("UPDATE photos SET flag='rejected',rating=2 WHERE id=?", (args.concurrent_decision,))
        concurrent.append({"photo_id": args.concurrent_decision, "flag": "rejected", "rating": 2,
                           "seconds_from_retry_start": round(started - retry_start, 4)})
        decision_written.set()

    def observed_detect(path: str) -> list | None:
        started = time.monotonic()
        writer = None
        if args.concurrent_decision is not None and not decision_written.is_set():
            writer = threading.Thread(target=write_concurrent_decision)
            writer.start()
        output = real_detect(path)
        if writer is not None:
            writer.join(timeout=30)
            if not decision_written.is_set():
                raise RuntimeError("Concurrent scratch decision did not finish")
        calls.append({"stage": "detection", "filename": Path(path).name,
                      "seconds": round(time.monotonic() - started, 4), "detections": output})
        return output

    def observed_mask(*values: object, **options: object) -> object:
        started = time.monotonic()
        output = real_mask(*values, **options)
        calls.append({"stage": "segmentation", "filename": job["progress"].get("current_file"),
                      "seconds": round(time.monotonic() - started, 4),
                      "mask_fraction": float(output.mean()) if output is not None else None})
        return output

    detector.detect_animals, masking.generate_mask = observed_detect, observed_mask
    job = {"id": f"scratch-{args.report_name}", "progress": {}, "errors": []}
    retry_start = time.monotonic()
    result = run_retry(SimpleNamespace(thread_db=database, checkpoint=lambda job: None), job, ids, effective, str(root))
    elapsed = time.monotonic() - retry_start
    db = database()
    after_rows = photo_rows(db)
    after_review = load_results(str(root), manifest["workspace_id"], db=db)
    persisted_review = load_results_raw(str(root), manifest["workspace_id"])
    expected = {pid: (row["flag"], row["rating"]) for pid, row in before_rows.items()}
    if args.concurrent_decision is not None:
        expected[args.concurrent_decision] = ("rejected", 2)
    assert all((row["flag"], row["rating"]) == expected[pid] for pid, row in after_rows.items())
    assert all(row_digest(row) == row_digest(after_rows[pid]) for pid, row in before_rows.items() if pid not in ids)
    assert group_members(before_review) == group_members(after_review) == group_members(persisted_review)
    assert next(p["label"] for p in after_review["photos"] if p["id"] == 216716) == "KEEP"
    timings = []
    for pid in ids:
        measured = [call for call in calls if call.get("filename") == after_rows[pid]["filename"]]
        timings.append({"photo_id": pid, "inference_seconds": round(sum(call["seconds"] for call in measured), 4),
                        "actual_inference_stages": [call["stage"] for call in measured]})
    ranking_after = {(row["detection_id"], row["source_key"]): row_digest(dict(row))
                     for row in db.conn.execute("SELECT * FROM subject_ranking_analysis")}
    repair_key = (incomplete_generation["detection_id"], incomplete_generation["source_key"]) if incomplete_generation else None
    assert all(value == ranking_after.get(key) for key, value in ranking_before.items() if key != repair_key)
    repaired = False
    if repair_key is not None:
        from subject_readiness import REQUIRED_QUALITY_FIELDS
        generation = db.subjects.ranking_analysis(*repair_key)
        repaired = (generation is not None and all(generation["features"].get(field) is not None for field in REQUIRED_QUALITY_FIELDS)
                    and ranking_before[repair_key] != ranking_after.get(repair_key))
        assert repaired
    report = {"implementation_commit": implementation_commit,
              "retry_service_sha256": hashlib.sha256((implementation / "vireo/services/analysis_retry.py").read_bytes()).hexdigest(),
              "execution_provider": "CPUExecutionProvider", "cpu_thread_budget": 4,
              "detector_confidence": effective.get("detector_confidence", .2),
              "requested_photo_ids": ids, "manual_subject_intervention": args.manual_duck_box,
              "invalidated_scratch_features": args.invalidate_features,
              "incomplete_ranking_fixture": incomplete_generation,
              "incomplete_ranking_repaired": repaired,
              "other_ranking_generations_unchanged": True,
              "existing_ranking_generations_checked": len(ranking_before) - (1 if repair_key else 0),
              "before": before, "result": result, "elapsed_seconds": round(elapsed, 4),
              "inference_calls": calls, "photo_timings": timings, "concurrent_decision": concurrent,
              "decisions_preserved": True, "unselected_photo_rows_unchanged": True,
              "saved_grouping_preserved": True, "existing_keep": "KEEP",
              "before_summary": before_review["summary"], "after_summary": after_review["summary"],
              "selected_photos_after": [{key: p.get(key) for key in ("id", "label", "analysis_status", "analysis_reasons", "quality_composite", "selection_eligible", "keep_status", "mask_path")} for p in after_review["photos"] if p["id"] in ids]}
    (root / f"{args.report_name}-result.json").write_text(json.dumps(report, indent=2) + "\n")
    export_outcomes(report, root / f"{args.report_name}-outcomes.csv")
    print(json.dumps({key: report[key] for key in ("implementation_commit", "requested_photo_ids", "elapsed_seconds", "decisions_preserved", "unselected_photo_rows_unchanged", "existing_keep", "after_summary")}, indent=2))
    db.close()


if __name__ == "__main__":
    main()
