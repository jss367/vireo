#!/usr/bin/env python3
"""Read-only stage accountability from an exact saved encounter membership.

Never instantiate Database: its startup migrations/repairs write to the catalog.
Only SQLite mode=ro is used. Output is a separate CSV, with no photo decisions
or metadata changed. Saved cached subjects and current stage rows describe
separate observations; unknown historical provenance stays explicitly unknown.
"""
import argparse
import csv
import hashlib
import json
import sqlite3
from pathlib import Path


def input_fingerprint(file_hash):
    if not isinstance(file_hash, str) or len(file_hash) != 64 or any(c not in "0123456789abcdef" for c in file_hash):
        return None
    block = {"recipe": "vireo-detector-source-v1", "sources": [
        {"role": "original", "sha256": file_hash},
    ]}
    return hashlib.sha256(json.dumps(block, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def inspect(conn, photo, threshold):
    pid = photo["id"]
    row = conn.execute("SELECT p.*,f.path AS folder_path FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.id=?", (pid,)).fetchone()
    if row is None:
        return {"photo_id": pid, "filename": photo["filename"], "finding": "catalog_photo_unavailable"}
    runs = conn.execute("SELECT * FROM detector_runs WHERE photo_id=?", (pid,)).fetchall()
    dets = conn.execute("SELECT * FROM detections WHERE photo_id=? AND detector_model!='full-image'", (pid,)).fetchall()
    animals = [d for d in dets if d["category"] == "animal"]
    retained = [d for d in animals if d["detector_confidence"] >= threshold]
    masks = conn.execute("SELECT * FROM photo_masks WHERE photo_id=?", (pid,)).fetchall()
    analyses = conn.execute("SELECT ds.* FROM detection_subjects ds JOIN detections d ON d.id=ds.detection_id WHERE d.photo_id=?", (pid,)).fetchall()
    fingerprint = input_fingerprint(row["file_hash"])
    mask = masks[0] if masks else None
    original = Path(row["folder_path"]) / row["filename"]
    try:
        stat = original.stat()
        source_status = "available_stat_matches_catalog" if stat.st_size == row["file_size"] and abs(stat.st_mtime - row["file_mtime"]) < .001 else "available_stat_changed"
    except OSError:
        source_status = "unavailable_now"
    finding = "retained_animal_and_mask" if retained and masks else "no_retained_animal_at_current_floor" if not retained else "mask_stage_unknown"
    if masks and not retained:
        finding = "weak_or_historical_mask_below_current_floor"
    return {
        "photo_id": pid, "filename": row["filename"],
        "cached_subjects": len(photo.get("subjects", [])), "cached_label": photo.get("label"),
        "cached_flag": photo.get("flag"), "current_flag": row["flag"],
        "detector_runs": len(runs), "raw_boxes": len(dets),
        "animal_boxes": len(animals), "retained_animal_boxes": len(retained),
        "max_animal_confidence": max((d["detector_confidence"] for d in animals), default=None),
        "retained_person_boxes": sum(d["category"] == "person" and d["detector_confidence"] >= threshold for d in dets),
        "runtime_fingerprints": ";".join(sorted({str(r["runtime_fingerprint"]) for r in runs})),
        "input_matches_catalog_hash": fingerprint is not None and bool(runs) and all(r["input_fingerprint"] == fingerprint for r in runs),
        "run_box_counts_match": bool(runs) and all(r["box_count"] == sum(d["detector_model"] == r["detector_model"] for d in dets) for r in runs),
        "source_status": source_status, "source_bytes_rehashed": False,
        "subject_analysis_rows": len(analyses), "mask_count": len(masks),
        "mask_variant": mask["variant"] if mask else None,
        "mask_file_available": bool(mask and Path(mask["path"]).is_file()),
        "active_mask_variant": row["active_mask_variant"], "dino_variant": row["dino_embedding_variant"],
        "subject_features_available": all(row[k] is not None for k in ("subject_size", "subject_tenengrad", "crop_complete")),
        "dino_subject_available": row["dino_subject_embedding"] is not None,
        "dino_global_available": row["dino_global_embedding"] is not None,
        "historical_photo_stage_outcome": "unknown_not_recorded_per_photo",
        "finding": finding,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True, type=Path)
    parser.add_argument("--membership", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--threshold", type=float, default=.2)
    args = parser.parse_args()
    if args.output.resolve() in (args.db.resolve(), args.membership.resolve()):
        parser.error("Output must be separate from the read-only catalog and saved membership")
    saved = json.loads(args.membership.read_text())
    photos = saved["photos"]
    ids = [p["id"] for p in photos]
    if len(set(ids)) != len(ids) or ids != saved["encounter"]["photo_ids"]:
        raise ValueError("Membership must match the exact saved encounter photo_ids, once each")
    with sqlite3.connect(args.db.resolve().as_uri() + "?mode=ro", uri=True) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        conn.execute("BEGIN")
        rows = [inspect(conn, photo, args.threshold) for photo in photos]
    fields = list(dict.fromkeys(key for row in rows for key in row))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="") as out:
        writer = csv.DictWriter(out, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} photo observations to {args.output}")


if __name__ == "__main__":
    main()
