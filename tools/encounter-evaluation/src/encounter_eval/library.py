"""Read-only capture of live library evidence, with labels in separate records."""

from __future__ import annotations

import gzip
import json
import sqlite3
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path

from .common import code_identity, configure_repo, digest, encode, write_json


def normalize(name):
    return " ".join(str(name or "").casefold().split())


class Taxonomy:
    def __init__(self, conn):
        aliases = defaultdict(set)
        self.display = {}
        for row in conn.execute("SELECT id, inat_id, name, common_name, rank FROM taxa"):
            if row["rank"] != "species":
                continue
            key = f"inat:{row['inat_id']}" if row["inat_id"] else f"taxon:{row['id']}"
            self.display[key] = row["common_name"] or row["name"]
            for name in (row["name"], row["common_name"]):
                if name:
                    aliases[normalize(name)].add(key)
        self.by_id = {}
        for row in conn.execute("SELECT id, inat_id FROM taxa WHERE rank = 'species'"):
            self.by_id[row["id"]] = f"inat:{row['inat_id']}" if row["inat_id"] else f"taxon:{row['id']}"
        for row in conn.execute("SELECT name, taxon_id FROM keywords WHERE taxon_id IS NOT NULL"):
            if row["taxon_id"] in self.by_id:
                aliases[normalize(row["name"])].add(self.by_id[row["taxon_id"]])
        self.aliases = {name: next(iter(keys)) for name, keys in aliases.items() if len(keys) == 1}

    def key(self, name, scientific_name=None):
        for candidate in (scientific_name, name):
            if candidate and normalize(candidate) in self.aliases:
                return self.aliases[normalize(candidate)]
        return "name:" + normalize(scientific_name or name)

    def prediction_key(self, name, identity):
        """Translate a production-resolved identity into evaluation taxonomy keys."""
        if identity and identity.startswith("taxon:"):
            return "inat:" + identity.removeprefix("taxon:")
        if identity and identity.startswith("scientific:"):
            return self.key(identity.removeprefix("scientific:"))
        if identity:
            # Keep unresolved names unresolved: keyword aliases cannot upgrade
            # the production resolver's deliberately conservative decision.
            return identity
        return "name:" + normalize(name)


class FeatureReader:
    """Minimal read interface for the real pipeline loader; no app initialization.

    SQL matches Database's detection helpers. Keywords are hidden BEFORE weak
    rescue and other feature preparation, not merely removed afterward.
    """

    def __init__(self, conn, workspace):
        self.conn = conn
        self.workspace = workspace

    def _ws_id(self):
        return self.workspace

    def get_species_keywords_for_photos(self, photo_ids, *, include_identities=False):
        return {}

    def get_meta(self, key):
        row = self.conn.execute("SELECT value FROM db_meta WHERE key = ?", (key,)).fetchone()
        return row[0] if row else None

    def get_detections_for_photos(self, photo_ids, min_conf, detector_model=None):
        result = defaultdict(list)
        ids = list(dict.fromkeys(photo_ids))
        for start in range(0, len(ids), 400):
            chunk = ids[start:start + 400]
            sql = f"SELECT * FROM detections WHERE photo_id IN ({','.join('?' for _ in chunk)}) AND detector_confidence >= ?"
            params = [*chunk, min_conf]
            if detector_model is not None:
                sql += " AND detector_model = ?"
                params.append(detector_model)
            from subjects import primary_order_sql
            for r in self.conn.execute(sql + " ORDER BY photo_id, " + primary_order_sql(), params):
                result[r["photo_id"]].append({
                    "id": r["id"], "x": r["box_x"], "y": r["box_y"], "w": r["box_w"], "h": r["box_h"],
                    "confidence": r["detector_confidence"], "category": r["category"],
                    "detector_model": r["detector_model"],
                })
        return result

    def get_detector_run_photo_ids(self, detector_model):
        # Evaluation always loads explicit session IDs. The production loader
        # stages this scope before requesting detector completion evidence.
        rows = self.conn.execute(
            """SELECT dr.photo_id FROM detector_runs dr WHERE dr.detector_model = ?
            AND dr.photo_id IN (SELECT id FROM pipeline_scope_ids)
            AND (dr.box_count = 0 OR EXISTS (SELECT 1 FROM detections d
                WHERE d.photo_id = dr.photo_id AND d.detector_model = dr.detector_model))""",
            (detector_model,),
        )
        return {r[0] for r in rows}


def timestamp(value):
    try:
        dt = datetime.fromisoformat(value)
        return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)
    except (ValueError, TypeError):
        return None


def split_for(key, seed):
    bucket = int(digest([seed, key])[:8], 16) % 100
    return "train" if bucket < 60 else "development" if bucket < 80 else "test"


def plan_sessions(rows, seed, gap_seconds=1800):
    """Same capture day across folders stays together; exact hashes also link days.

    No gold labels influence ordering, split membership, or gap decisions.
    Unhashed near-duplicates with different capture dates require manual audit.
    """
    parent = {}

    def find(key):
        parent.setdefault(key, key)
        while parent[key] != key:
            parent[key] = parent[parent[key]]
            key = parent[key]
        return key

    def join(a, b):
        a, b = find(a), find(b)
        parent[max(a, b)] = min(a, b)

    hashes = {}
    streams = defaultdict(list)
    for row in rows:
        dt = timestamp(row["timestamp"])
        day = dt.date().isoformat() if dt else "undated:" + row["folder_path"]
        row["day"] = day
        find(day)
        if row.get("file_hash"):
            prior = hashes.setdefault(row["file_hash"], day)
            join(day, prior)
        streams[(row["folder_id"], day)].append(row)
    sessions = []
    for (_folder, _day), photos in sorted(streams.items()):
        photos.sort(key=lambda p: (timestamp(p["timestamp"]) or datetime.max.replace(tzinfo=UTC),
                                   p["filename"], p["id"]))
        chunks = [[]]
        for photo in photos:
            if chunks[-1]:
                previous = timestamp(chunks[-1][-1]["timestamp"])
                current = timestamp(photo["timestamp"])
                if previous and current and (current - previous).total_seconds() > gap_seconds:
                    chunks.append([])
            chunks[-1].append(photo)
        for chunk in chunks:
            first = chunk[0]
            partition_key = find(first["day"])
            sessions.append({
                "id": digest([first["folder_path"], first["day"], first["filename"]])[:24],
                "partition_key": partition_key, "partition": split_for(partition_key, seed),
                "photos": chunk,
            })
    return sorted(sessions, key=lambda s: (s["partition_key"], s["id"]))


def open_library(path):
    # mode=ro prevents main-database writes while allowing connection-local TEMP
    # scopes required by load_photo_features. Never use immutable=1 on a live WAL.
    conn = sqlite3.connect(Path(path).expanduser().resolve().as_uri() + "?mode=ro", uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    conn.execute("BEGIN")
    # New production loaders use source identities. Overlay legacy tables with
    # connection-local views rather than migrating the photographer's library.
    for table in ("predictions", "keywords"):
        columns = {r["name"] for r in conn.execute(f"PRAGMA main.table_info({table})")}
        if columns and "source_taxon_id" not in columns:
            conn.execute(f"CREATE TEMP VIEW {table} AS SELECT *, NULL AS source_taxon_id FROM main.{table}")
    # Subject analysis was added after older evaluation catalogs were captured.
    # Empty TEMP views preserve their confidence ordering and absent suggestions,
    # while current catalogs retain their real analysis and manual choices.
    for table, columns in {
        "detection_subjects": ("detection_id", "crop", "quality_score", "exposure_ev"),
        "photo_subject_choices": ("photo_id", "detection_id"),
    }.items():
        if not conn.execute(f"PRAGMA main.table_info({table})").fetchone():
            projection = ", ".join(f"NULL AS {column}" for column in columns)
            conn.execute(f"CREATE TEMP VIEW {table} AS SELECT {projection} WHERE 0")
    return conn


def _raw_evidence(reader, taxonomy):
    from species_identity import SpeciesResolver

    conn = reader.conn
    resolver = SpeciesResolver(db=reader)
    subjects = {}
    for row in conn.execute("SELECT d.* FROM detections d JOIN pipeline_scope_ids s ON s.id = d.photo_id ORDER BY d.id"):
        d = dict(row)
        d["sources"] = {}
        subjects[d["id"]] = d
    runs = {}
    for row in conn.execute("""SELECT cr.* FROM classifier_runs cr JOIN detections d ON d.id = cr.detection_id
                            JOIN pipeline_scope_ids s ON s.id = d.photo_id"""):
        runs[(row["detection_id"], row["classifier_model"], row["labels_fingerprint"])] = dict(row)
    # Same most-recent fingerprint policy as production; retain every candidate
    # per detection/source instead of capping across otherwise distinct models.
    sql = """SELECT pr.* FROM predictions pr JOIN detections d ON d.id = pr.detection_id
        JOIN pipeline_scope_ids s ON s.id = d.photo_id
        WHERE pr.labels_fingerprint = (SELECT pr2.labels_fingerprint FROM predictions pr2
            WHERE pr2.detection_id = pr.detection_id AND pr2.classifier_model = pr.classifier_model
            ORDER BY pr2.created_at DESC, pr2.id DESC LIMIT 1)
        ORDER BY pr.detection_id, pr.classifier_model, pr.confidence DESC, pr.id"""
    for row in conn.execute(sql):
        r = dict(row)
        if not r["species"]:
            continue
        source = subjects[r["detection_id"]]["sources"].setdefault(r["classifier_model"], {
            "model": r["classifier_model"], "labels_fingerprint": r["labels_fingerprint"],
            "mode": "exclusive", "mode_assumption": True, "predictions": [],
            "run": runs.get((r["detection_id"], r["classifier_model"], r["labels_fingerprint"])),
        })
        identity = resolver.prediction(r)
        taxon = taxonomy.prediction_key(identity.display_name, identity.key)
        source["predictions"].append({"taxon": taxon,
                                      "name": r["species"], "score": r["confidence"]})
    by_photo = defaultdict(list)
    for subject in subjects.values():
        subject["sources"] = list(subject["sources"].values())
        by_photo[subject["photo_id"]].append(subject)
    return by_photo


def prepare(db_path, output, *, workspace=None, seed=42, max_sessions=None,
            complete_folders=(), label_source="all", config=None, split_registry=None, repo=None):
    """Materialize a consistent comparison, then close the live DB before trials."""
    source_identity = code_identity(configure_repo(repo))

    from config import DEFAULTS
    from encounters import DEFAULTS as GROUP_DEFAULTS
    from pipeline import load_photo_features

    cfg = {"detector_confidence": DEFAULTS["detector_confidence"],
           "classification_threshold": DEFAULTS["classification_threshold"],
           "pipeline": dict(DEFAULTS.get("pipeline", {}))}
    if config:
        unknown = set(config) - {"detector_confidence", "classification_threshold", "pipeline"}
        if unknown:
            raise ValueError(f"Unsupported config keys: {sorted(unknown)}")
        cfg.update({k: v for k, v in config.items() if k != "pipeline"})
        cfg["pipeline"].update(config.get("pipeline", {}))
    # No embedding variant filter by default: the real grouping implementation
    # tolerates mixed shapes. A supplied variant can explicitly constrain inputs.
    if not (config or {}).get("pipeline", {}).get("dinov2_variant"):
        cfg["pipeline"].pop("dinov2_variant", None)
    grouping = {**GROUP_DEFAULTS, **{k: v for k, v in cfg["pipeline"].items() if k in GROUP_DEFAULTS}}
    output = Path(output)
    (output / "inputs").mkdir(parents=True, exist_ok=True)
    conn = open_library(db_path)
    try:
        workspaces = [dict(r) for r in conn.execute("SELECT id, name FROM workspaces ORDER BY id")]
        if workspace is None:
            if len(workspaces) != 1:
                choices = ", ".join(f"{w['id']}: {w['name']}" for w in workspaces)
                raise ValueError(f"Choose --workspace explicitly ({choices})")
            workspace = workspaces[0]["id"]
        if workspace not in {w["id"] for w in workspaces}:
            raise ValueError(f"Unknown workspace {workspace}")
        namespace = digest([str(Path(db_path).expanduser().resolve()), workspace])[:16]
        registry_path = (Path(split_registry) if split_registry else
                         output.parent / f"split-membership-{namespace}.json")
        registry = json.loads(registry_path.read_text()) if registry_path.exists() else {"seed": seed, "days": {}}
        if registry["seed"] != seed:
            raise ValueError("Split registry uses a different seed; reuse that seed or choose a separate registry")
        rows = [dict(r) for r in conn.execute("""SELECT p.id, p.folder_id, p.filename, p.timestamp,
            p.file_hash, p.thumb_path, f.path AS folder_path FROM photos p
            JOIN folders f ON f.id = p.folder_id JOIN workspace_folders wf ON wf.folder_id = f.id
            WHERE wf.workspace_id = ? ORDER BY p.id""", (workspace,))]
        unknown_folders = set(complete_folders) - {r["folder_id"] for r in rows}
        if unknown_folders:
            raise ValueError(f"Complete-label folders not found in selected workspace: {sorted(unknown_folders)}")
        taxonomy = Taxonomy(conn)
        labels = defaultdict(lambda: {"taxa": set(), "sources": set()})
        for r in conn.execute("""SELECT pk.photo_id, pk.source, k.name, k.taxon_id, k.source_taxon_id FROM photo_keywords pk
            JOIN keywords k ON k.id = pk.keyword_id LEFT JOIN taxa t ON t.id = k.taxon_id
            JOIN photos p ON p.id = pk.photo_id JOIN workspace_folders wf ON wf.folder_id = p.folder_id
            WHERE wf.workspace_id = ? AND (k.is_species = 1 OR k.type = 'taxonomy')
            AND (t.rank = 'species' OR t.rank IS NULL)""", (workspace,)):
            if label_source == "manual" and r["source"] != "manual":
                continue
            key = f"inat:{r['source_taxon_id']}" if r["source_taxon_id"] else taxonomy.by_id.get(r["taxon_id"], taxonomy.key(r["name"]))
            labels[r["photo_id"]]["taxa"].add(key)
            labels[r["photo_id"]]["sources"].add(r["source"] or "unknown")
        sessions = plan_sessions(rows, seed)
        # Preserve existing split membership as data grows. A newly discovered
        # duplicate linking previous partitions is quarantined, never reassigned.
        linked_days = defaultdict(set)
        for session in sessions:
            linked_days[session["partition_key"]].update(p["day"] for p in session["photos"])
        conflicts = set()
        for key, days in linked_days.items():
            prior = {registry["days"][d] for d in days if d in registry["days"]}
            partition = next(iter(prior)) if len(prior) == 1 else split_for(key, seed)
            if len(prior) > 1 or "quarantined" in prior:
                conflicts.add(key)
                partition = "quarantined"
            for day in days:
                registry["days"][day] = partition
        for session in sessions:
            session["partition"] = registry["days"][session["photos"][0]["day"]]
        eligible = [s for s in sessions if any(p["id"] in labels for p in s["photos"])
                    and s["partition"] != "quarantined"]
        # Limits select whole sessions by a seeded hash, never by label outcome.
        eligible.sort(key=lambda s: digest([seed, s["id"]]))
        selected = eligible[:max_sessions] if max_sessions is not None else eligible
        inventory = {"workspace_photos": len(rows), "tagged_photos": len(labels),
                     "total_sessions": len(sessions), "eligible_sessions": len(eligible),
                     "selected_sessions": len(selected), "quarantined_day_clusters": len(conflicts)}
        entries = []
        counts = Counter()
        reader = FeatureReader(conn, workspace)
        for i, session in enumerate(selected):
            metadata = {p["id"]: p for p in session["photos"]}
            photos = load_photo_features(reader, config=cfg, photo_ids=list(metadata), effective_config=cfg)
            order = {pid: index for index, pid in enumerate(metadata)}
            photos.sort(key=lambda p: order[p["id"]])
            raw = _raw_evidence(reader, taxonomy)
            answers, presentation = {}, {}
            for photo in photos:
                pid = photo["id"]
                # Allowlist the inference contract: no flags, ratings, keywords,
                # review status, file paths, or other label-derived information.
                allowed = {"id", "folder_id", "filename", "timestamp", "latitude", "longitude", "focal_length",
                           "burst_id", "dino_subject_embedding", "dino_global_embedding", "species_top5",
                           "subjects", "detection_box", "detection_conf", "subject_absent", "subject_present",
                           "subject_uncertain", "weak_detection_context"}
                for key in list(photo):
                    if key not in allowed:
                        del photo[key]
                photo["timestamp"] = timestamp(photo["timestamp"]).isoformat() if timestamp(photo["timestamp"]) else None
                photo["evidence"] = raw.get(pid, [])
                # A static taxonomy lookup is independent of per-photo answers.
                # Key by the identity production distinguishes (source taxon id
                # first, display name only when no identity is carried), so two
                # source taxa that share a display name each keep their own key.
                photo["species_keys"] = {}
                for entry in photo["species_top5"]:
                    identity = entry[3] if len(entry) > 3 else None
                    key = taxonomy.prediction_key(entry[0], identity)
                    photo["species_keys"][identity or entry[0]] = key
                meta = metadata[pid]
                presentation[str(pid)] = {"filename": meta["filename"], "folder": meta["folder_path"],
                                           "thumbnail": meta["thumb_path"], "file_hash": meta["file_hash"]}
                if pid in labels:
                    label = labels[pid]
                    answers[str(pid)] = {"taxa": sorted(label["taxa"]), "sources": sorted(label["sources"]),
                                         "complete": meta["folder_id"] in complete_folders}
                    counts["labeled_photos"] += 1
                    counts["manual_only_label_photos"] += label["sources"] == {"manual"}
                    counts["unknown_or_mixed_provenance_photos"] += label["sources"] != {"manual"}
                    counts["multiple_species_label_photos"] += len(label["taxa"]) > 1
                    counts["complete_roster_photos"] += meta["folder_id"] in complete_folders
                    counts["labeled_photos_with_predictions"] += any(d["sources"] for d in raw.get(pid, []))
                counts["photos"] += 1
                counts["photos_with_predictions"] += any(d["sources"] for d in raw.get(pid, []))
            bundle = {"photos": photos, "answers": answers, "presentation": presentation}
            content = encode(bundle)
            name = f"inputs/{session['id']}.json.gz"
            with gzip.GzipFile(filename=str(output / name), mode="wb", mtime=0) as handle:
                handle.write(content.encode())
            entries.append({"id": session["id"], "partition": session["partition"], "path": name,
                            "digest": digest(bundle), "photo_count": len(photos), "label_count": len(answers)})
            if (i + 1) % 10 == 0:
                print(f"Prepared {i + 1}/{len(selected)} sessions", flush=True)
        manifest = {"format_version": 1, "created_at": datetime.now(UTC).isoformat(), "code": source_identity,
                    "workspace": workspace, "seed": seed, "config": cfg, "grouping_config": grouping,
                    "label_source": label_source, "complete_folders": sorted(complete_folders),
                    "inventory": {**inventory, **dict(counts)}, "sessions": entries,
                    "taxonomy_display": taxonomy.display, "split_registry_digest": digest(registry),
                    "split_registry_path": str(registry_path.resolve()),
                    "data_digest": digest([entries, cfg, label_source, sorted(complete_folders)]),
                    "limitations": ["Labels are positive-only unless folder completeness is explicitly declared.",
                        "Exclusive classifier mode is assumed; custom multi-label sources need an adapter.",
                        "Latest stored source results may be stale or use label lists chosen after the shoot.",
                        "Cross-date near-duplicates without matching file hashes require split audit.",
                        "Capture streams use folders; multiple cameras in one folder are not separated."]}
    finally:
        conn.rollback()
        conn.close()
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    write_json(registry_path, registry)
    write_json(output / "manifest.json", manifest)
    return manifest


def read_bundle(output, entry):
    with gzip.open(Path(output) / entry["path"], "rt", encoding="utf-8") as handle:
        bundle = json.load(handle)
    if digest(bundle) != entry["digest"]:
        raise ValueError(f"Input changed or corrupted: {entry['path']}")
    return bundle
