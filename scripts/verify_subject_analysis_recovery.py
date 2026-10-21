#!/usr/bin/env python3
"""Prepare and inspect an isolated copy of a saved photo encounter.

The source catalog is opened with SQLite mode=ro/query_only; model inference
must use the new catalog, copied source files and copied model files. No app
startup or live-library Database object is used during preparation.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

REPOSITORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPOSITORY / "vireo"))


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(8 * 1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def clone_file(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if sys.platform == "darwin":
        try:
            subprocess.run(["cp", "-c", "-p", str(source), str(destination)], check=True, capture_output=True)
            return
        except subprocess.CalledProcessError:
            pass
    shutil.copy2(source, destination)


def snapshot(path: Path) -> dict:
    stat = path.stat()
    return {"path": str(path), "size": stat.st_size, "mtime_ns": stat.st_mtime_ns, "sha256": digest(path)}


def readonly_catalog(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    conn.execute("BEGIN")
    return conn


def copy_rows(source: sqlite3.Connection, target: sqlite3.Connection, table: str,
              where: str = "", parameters: tuple = ()) -> None:
    columns = {r[1] for r in target.execute(f"PRAGMA table_info({table})")}
    if not columns or source.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is None:
        return
    for row in source.execute(f"SELECT * FROM {table}" + (f" WHERE {where}" if where else ""), parameters):
        values = {key: value for key, value in dict(row).items() if key in columns}
        keys = ",".join(f'"{key}"' for key in values)
        marks = ",".join("?" for _ in values)
        target.execute(f"INSERT OR REPLACE INTO {table} ({keys}) VALUES ({marks})", tuple(values.values()))


def catalog_snapshot(conn: sqlite3.Connection, ids: list[int]) -> dict:
    """Hash exact selected catalog rows, avoiding mutable WAL/file-byte proxies."""
    marks = ",".join("?" for _ in ids)
    parameters = tuple(ids)
    tables = {}
    for table in ("photos", "detections", "detector_runs", "photo_masks", "photo_subject_choices", "photo_subject_state", "photo_keywords", "current_flag_attribution"):
        if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is None:
            continue
        key = "id" if table == "photos" else "photo_id"
        rows = []
        for row in conn.execute(f"SELECT * FROM {table} WHERE {key} IN ({marks})", parameters):
            rows.append({key: value.hex() if isinstance(value, bytes) else value for key, value in dict(row).items()})
        encoded = sorted(json.dumps(row, sort_keys=True) for row in rows)
        tables[table] = {"rows": len(rows), "sha256": hashlib.sha256(json.dumps(encoded).encode()).hexdigest()}
    return {"tables": tables, "sha256": hashlib.sha256(json.dumps(tables, sort_keys=True).encode()).hexdigest()}


def prepare(args: argparse.Namespace) -> None:
    from canonical_schema import CanonicalSchema

    started = time.monotonic()
    destination = args.output.resolve()
    if destination.exists():
        raise ValueError("Output must be a new directory; refusing to overwrite any existing files")
    forbidden = [args.catalog.resolve().parent, args.models.resolve()]
    if any(destination == parent or parent in destination.parents for parent in forbidden):
        raise ValueError("Scratch output must be outside the original catalog and model directories")
    saved = json.loads(args.membership.read_text())
    ids = saved["encounter"]["photo_ids"]
    if len(ids) != len(set(ids)) or ids != [p["id"] for p in saved["photos"]]:
        raise ValueError("Membership must match the exact saved encounter photo_ids, once each")
    source = readonly_catalog(args.catalog)
    marks = ",".join("?" for _ in ids)
    parameters = tuple(ids)
    rows = source.execute(
        f"SELECT p.*,f.path AS folder_path FROM photos p JOIN folders f ON f.id=p.folder_id WHERE p.id IN ({marks})",
        parameters,
    ).fetchall()
    if {r["id"] for r in rows} != set(ids):
        raise ValueError("Some exact encounter members are missing from the source catalog")
    if any(not (Path(r["folder_path"]) / r["filename"]).is_file() for r in rows):
        raise ValueError("Some source originals are unavailable; no inference fixture was created")
    destination.mkdir(parents=True)
    target = sqlite3.connect(destination / "catalog.db")
    target.row_factory = sqlite3.Row
    CanonicalSchema(target).create_tables()
    target.execute("PRAGMA foreign_keys=OFF")
    folder_ids = {row["folder_id"] for row in rows}
    folder_marks = ",".join("?" for _ in folder_ids)
    copy_rows(source, target, "workspaces", "id=?", (args.workspace,))
    copy_rows(source, target, "folders", f"id IN ({folder_marks})", tuple(folder_ids))
    copy_rows(source, target, "workspace_folders", f"workspace_id=? AND folder_id IN ({folder_marks})", (args.workspace, *folder_ids))
    copy_rows(source, target, "photos", f"id IN ({marks})", parameters)
    for table in ("detections", "detector_runs", "photo_masks", "photo_subject_choices", "photo_subject_state", "current_flag_attribution"):
        copy_rows(source, target, table, f"photo_id IN ({marks})", parameters)
    detection_ids = tuple(r[0] for r in target.execute("SELECT id FROM detections"))
    if detection_ids:
        detection_marks = ",".join("?" for _ in detection_ids)
        for table in ("predictions", "detection_subjects", "subject_raw_analysis", "classifier_runs", "subject_ranking_analysis"):
            copy_rows(source, target, table, f"detection_id IN ({detection_marks})", detection_ids)
    copy_rows(source, target, "keywords")
    taxon_ids = {row[0] for row in target.execute("SELECT taxon_id FROM keywords WHERE taxon_id IS NOT NULL")}
    taxon_ids.update(row[0] for row in target.execute("SELECT source_taxon_id FROM predictions WHERE source_taxon_id IS NOT NULL"))
    if taxon_ids:
        taxon_marks = ",".join("?" for _ in taxon_ids)
        copy_rows(source, target, "taxa", f"""id IN (
            WITH RECURSIVE wanted(id,parent_id) AS (
                SELECT id,parent_id FROM taxa WHERE id IN ({taxon_marks})
                UNION SELECT t.id,t.parent_id FROM taxa t JOIN wanted w ON t.id=w.parent_id
            ) SELECT id FROM wanted)""", tuple(taxon_ids))
    copy_rows(source, target, "photo_keywords", f"photo_id IN ({marks})", parameters)
    prediction_ids = tuple(row[0] for row in target.execute("SELECT id FROM predictions"))
    if prediction_ids:
        prediction_marks = ",".join("?" for _ in prediction_ids)
        copy_rows(source, target, "prediction_review", f"workspace_id=? AND prediction_id IN ({prediction_marks})", (args.workspace, *prediction_ids))
    catalog_baseline = catalog_snapshot(source, ids)
    originals = []
    copies = []
    decisions = []
    for row in rows:
        original = Path(row["folder_path"]) / row["filename"]
        baseline = snapshot(original)
        original_copy = destination / "originals" / str(row["folder_id"]) / row["filename"]
        clone_file(original, original_copy)
        copied = snapshot(original_copy)
        if any(baseline[key] != copied[key] for key in ("size", "mtime_ns", "sha256")):
            raise ValueError(f"Source copy differs for photo {row['id']}")
        baseline.update(photo_id=row["id"], catalog_sha256=row["file_hash"])
        originals.append(baseline)
        copies.append(copied)
        decisions.append({"photo_id": row["id"], "flag": row["flag"], "rating": row["rating"]})
        for sidecar in (original.with_suffix(".xmp"), original.with_suffix(".XMP"), Path(str(original) + ".xmp")):
            if sidecar.is_file():
                originals.append(snapshot(sidecar))
                sidecar_copy = original_copy.parent / sidecar.name
                clone_file(sidecar, sidecar_copy)
                copies.append(snapshot(sidecar_copy))
        # Every path consumed by inference is either a scratch copy or absent.
        target.execute("UPDATE photos SET working_copy_path=NULL, companion_path=NULL, thumb_path=NULL WHERE id=?", (row["id"],))
    for folder_id in folder_ids:
        target.execute("UPDATE folders SET path=?,parent_id=NULL WHERE id=?", (str(destination / "originals" / str(folder_id)), folder_id))
    for mask in target.execute("SELECT * FROM photo_masks").fetchall():
        copied_mask = destination / "masks" / Path(mask["path"]).name
        clone_file(Path(mask["path"]), copied_mask)
        target.execute("UPDATE photo_masks SET path=? WHERE photo_id=? AND variant=?", (str(copied_mask), mask["photo_id"], mask["variant"]))
        target.execute("UPDATE photos SET mask_path=? WHERE id=? AND active_mask_variant=?", (str(copied_mask), mask["photo_id"], mask["variant"]))
    violations = target.execute("PRAGMA foreign_key_check").fetchall()
    if violations:
        raise ValueError(f"Scratch catalog has dangling foreign-key references: {len(violations)}")
    target.commit()
    target.close()
    source.close()
    source_config = json.loads(args.config.read_text())
    # Retain only workflow settings; credentials and unrelated filesystem paths
    # are deliberately absent from the isolated inference configuration.
    config = {key: source_config[key] for key in ("pipeline", "detector_confidence", "detection_padding") if key in source_config}
    config["sync_flags_to_xmp"] = False
    (destination / "config.json").write_text(json.dumps(config, indent=2) + "\n")
    for variant in args.model_variants:
        for model_file in (args.models / variant).iterdir():
            if model_file.is_file():
                clone_file(model_file, destination / "models" / variant / model_file.name)
    report = {
        "photo_ids": ids, "workspace_id": args.workspace,
        "catalog": str(args.catalog.resolve()), "scratch_catalog": str(destination / "catalog.db"),
        "catalog_baseline": catalog_baseline,
        "source_baselines": originals, "copy_baselines": copies, "decision_baseline": decisions,
        "catalog_hashes_match_original_bytes": all(x.get("catalog_sha256") == x["sha256"] for x in originals if "photo_id" in x),
        "prepared_seconds": round(time.monotonic() - started, 3),
        "source_photo_bytes": sum(x["size"] for x in originals if "photo_id" in x),
    }
    (destination / "manifest.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({key: report[key] for key in ("scratch_catalog", "prepared_seconds", "source_photo_bytes", "catalog_hashes_match_original_bytes")}, indent=2))


def check(args: argparse.Namespace) -> None:
    root = args.scratch.resolve()
    manifest = json.loads((root / "manifest.json").read_text())
    baselines = manifest["source_baselines"] + manifest["copy_baselines"]
    if "saved_review_baseline" in manifest:
        baselines.append(manifest["saved_review_baseline"])
    for baseline in baselines:
        current = snapshot(Path(baseline["path"]))
        if any(current[key] != baseline[key] for key in ("size", "mtime_ns", "sha256")):
            raise ValueError(f"Source or source-copy content/mtime changed: {baseline['path']}")
    conn = readonly_catalog(Path(manifest["catalog"]))
    if "catalog_baseline" in manifest and catalog_snapshot(conn, manifest["photo_ids"]) != manifest["catalog_baseline"]:
        raise ValueError("Selected live catalog rows changed since snapshot")
    for decision in manifest["decision_baseline"]:
        row = conn.execute("SELECT flag,rating FROM photos WHERE id=?", (decision["photo_id"],)).fetchone()
        if row is None or (row["flag"], row["rating"]) != (decision["flag"], decision["rating"]):
            raise ValueError(f"Live decision changed since snapshot: {decision['photo_id']}")
    conn.close()
    print(json.dumps({"source_and_copy_bytes_mtimes_unchanged": True, "live_decisions_unchanged": True, "selected_catalog_digest_unchanged": "catalog_baseline" in manifest, "photos": len(manifest["photo_ids"])}))


def seed(args: argparse.Namespace) -> None:
    """Copy an exact saved encounter into scratch and refresh its suggestions."""
    import config as cfg
    from db import Database
    from pipeline import load_results, save_results_raw

    root = args.scratch.resolve()
    manifest = json.loads((root / "manifest.json").read_text())
    saved = json.loads(args.saved_review.read_text())
    ids = manifest["photo_ids"]
    encounters = [saved["encounter"]] if "encounter" in saved else saved["encounters"]
    encounter = next((value for value in encounters if value.get("photo_ids") == ids), None)
    if encounter is None:
        raise ValueError("Saved review does not contain the exact original encounter membership")
    photos = {value["id"]: value for value in saved["photos"]}
    if not set(ids) <= photos.keys():
        raise ValueError("Saved review is missing exact encounter members")
    cfg.CONFIG_PATH = str(root / "config.json")
    db = Database(str(root / "catalog.db"))
    db.set_active_workspace(manifest["workspace_id"])
    save_results_raw({"encounters": [encounter], "photos": [photos[pid] for pid in ids], "summary": {}}, str(root), manifest["workspace_id"])
    refreshed = load_results(str(root), manifest["workspace_id"], db=db)
    (root / "baseline-saved-review.json").write_text(json.dumps(refreshed, indent=2) + "\n")
    manifest["saved_review_baseline"] = snapshot(args.saved_review.resolve())
    (root / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    db.close()
    print(json.dumps(refreshed["summary"], indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    prep = sub.add_parser("prepare")
    prep.add_argument("--catalog", required=True, type=Path)
    prep.add_argument("--config", required=True, type=Path)
    prep.add_argument("--models", required=True, type=Path)
    prep.add_argument("--membership", type=Path, default=REPOSITORY / "docs/investigations/mandarin-duck-saved-membership.json")
    prep.add_argument("--workspace", type=int, default=5)
    prep.add_argument("--output", type=Path, required=True)
    prep.add_argument("--model-variants", nargs="+", default=["megadetector-v6", "sam2-large", "dinov2-vit-b14"])
    prep.set_defaults(action=prepare)
    saved = sub.add_parser("seed")
    saved.add_argument("--scratch", required=True, type=Path)
    saved.add_argument("--saved-review", required=True, type=Path)
    saved.set_defaults(action=seed)
    verify = sub.add_parser("check")
    verify.add_argument("--scratch", required=True, type=Path)
    verify.set_defaults(action=check)
    args = parser.parse_args()
    args.action(args)


if __name__ == "__main__":
    main()
