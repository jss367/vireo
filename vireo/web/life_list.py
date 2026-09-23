"""Life List: the species list, taxonomy explorer, and list exports.

The ``/api/life-list/*`` routes back the Life List page: the numbered
species list with each species' curated photos, single-species photo
pagination, the taxonomy completeness explorer (class tree, genus leaves,
flat per-rank lists), and the JSON / CSV / text / file-list downloads.
"""

from __future__ import annotations

import csv
import io
import json
from datetime import UTC, datetime

from flask import Blueprint, jsonify, make_response, request

_EXPLORER_RANKS = ["order", "family", "genus", "species"]
_EXPLORER_CHILD_RANK = {"class": "order", "order": "family",
                        "family": "genus", "genus": "species"}


def _build_explorer_payload(db, root_id=None):
    """Completeness tree (down to genus) for one class. Species leaves load
    separately via _build_explorer_species. Honest about the two failure
    states: taxonomy-not-downloaded and unmatched species."""
    root = (db.get_explorer_root() if root_id is None
            else db.get_taxon_by_id(root_id))
    if root is None:
        # Taxonomy absent (default lookup found no Aves class) -> not ready.
        return {"taxonomy_ready": False, "valid_root": False, "root": None,
                "summary": {}, "nodes": [],
                "unmatched_species": {"count": 0, "names": []},
                "uncounted_identifications": {
                    "count": 0, "scoped_count": 0, "workspace_count": 0,
                    "scoped": [], "workspace": [],
                },
                "classes": []}
    if root["rank"] != "class":
        # Explorer roots must be class-rank; an order/family/etc. root produces a
        # semantically confusing summary (e.g. an order counting itself). The
        # taxonomy exists, but this root is not valid.
        return {"taxonomy_ready": True, "valid_root": False, "root": None,
                "summary": {}, "nodes": [],
                "unmatched_species": {"count": 0, "names": []},
                "uncounted_identifications": {
                    "count": 0, "scoped_count": 0, "workspace_count": 0,
                    "scoped": [], "workspace": [],
                },
                "classes": []}

    rows = db.get_taxon_subtree(root["id"])
    found = db.get_life_list_taxon_ids()

    # Build node index + children lists.
    nodes = {r["id"]: {**r, "children": [], "found_species": 0,
                       "total_species": 0} for r in rows}
    for n in nodes.values():
        pid = n["parent_id"]
        if pid in nodes and pid != n["id"]:
            nodes[pid]["children"].append(n)

    # Post-order rollup of species totals/found.
    def rollup(node):
        if node["rank"] == "species":
            node["total_species"] = 1
            node["found_species"] = 1 if node["id"] in found else 0
        else:
            for c in node["children"]:
                rollup(c)
                node["total_species"] += c["total_species"]
                node["found_species"] += c["found_species"]
    rollup(nodes[root["id"]])

    # Per-rank summary across the whole class.
    summary = {r: {"found": 0, "total": 0} for r in _EXPLORER_RANKS}
    for n in nodes.values():
        if n["rank"] in summary:
            summary[n["rank"]]["total"] += 1
            if n["found_species"] > 0:
                summary[n["rank"]]["found"] += 1

    def to_out(node):
        child_rank = _EXPLORER_CHILD_RANK.get(node["rank"])
        kids = list(node["children"])
        found_children = sum(1 for c in kids if c["found_species"] > 0)
        out = {
            "id": node["id"], "name": node["name"],
            "common_name": node["common_name"], "rank": node["rank"],
            "found_species": node["found_species"],
            "total_species": node["total_species"],
            "child_rank": child_rank,
            "found_children": found_children,
            "total_children": len(kids),
        }
        # Materialize tree down to genus; species leaves load on demand.
        if node["rank"] != "genus":
            out["children"] = [to_out(c) for c in sorted(
                kids, key=lambda c: (c["found_species"] == 0,
                                     (c["common_name"] or c["name"]).lower()))]
        else:
            out["children"] = []  # species fetched via leaf endpoint
        return out

    top = [to_out(c) for c in sorted(
        nodes[root["id"]]["children"],
        key=lambda c: (c["found_species"] == 0,
                       (c["common_name"] or c["name"]).lower()))]

    uncounted = db.get_life_list_uncounted_identifications()
    scoped_uncounted = [
        entry for entry in uncounted
        if entry["class"] is not None and entry["class"]["id"] == root["id"]
    ]
    # An unlinked label (or a taxon broader than class, such as the plant
    # phylum behind "Trees") cannot honestly be assigned to the selected
    # class. Surface it separately as workspace-wide taxonomy cleanup instead
    # of making mammals/reptiles/plants look like missing Birds entries.
    workspace_uncounted = [
        entry for entry in uncounted if entry["class"] is None
    ]
    visible_uncounted = scoped_uncounted + workspace_uncounted

    # A class represented only by a broad identification still belongs in the
    # selector. Otherwise (for example) a workspace containing only the genus
    # "Sheep" would have no Mammals option in which to review that label.
    class_seed_taxa = set(found)
    class_seed_taxa.update(
        entry["taxon_id"] for entry in uncounted
        if entry["taxon_id"] is not None
    )
    classes = db.get_classes_for_taxa(class_seed_taxa)
    # Always include the default Aves class in the selector, regardless of the
    # currently selected root. `get_classes_for_taxa(found)` only returns classes
    # the user has actually tagged in, so a user who tags only non-bird species
    # and then picks that class would otherwise lose Birds from the selector with
    # no in-page way back to the default view.
    default_root = root if root_id is None else db.get_explorer_root()
    if default_root is not None and not any(
        c["id"] == default_root["id"] for c in classes
    ):
        classes.insert(0, {"id": default_root["id"],
                           "name": default_root["name"],
                           "common_name": default_root.get("common_name")})
    return {
        "taxonomy_ready": True,
        "valid_root": True,
        "root": {"id": root["id"], "name": root["name"],
                 "common_name": root.get("common_name"), "rank": root["rank"]},
        "summary": summary,
        "nodes": top,
        # Retain the old name-only field for API compatibility. New clients use
        # the structured, reasoned field below.
        "unmatched_species": {
            "count": len(visible_uncounted),
            "names": [entry["name"] for entry in visible_uncounted[:200]],
        },
        "uncounted_identifications": {
            "count": len(visible_uncounted),
            "scoped_count": len(scoped_uncounted),
            "workspace_count": len(workspace_uncounted),
            "scoped": scoped_uncounted[:200],
            "workspace": workspace_uncounted[:200],
        },
        "classes": classes,
    }


def _build_explorer_species(db, genus_id):
    """Found+missing species directly under a genus, found ones with a
    representative photo. Found sorted first, then missing; each alphabetical."""
    rows = [r for r in db.get_taxon_subtree(genus_id, max_depth=1)
            if r["rank"] == "species"]
    found = db.get_life_list_taxon_ids()
    found_ids = [r["id"] for r in rows if r["id"] in found]
    photos = db.get_life_list_best_photo_by_taxon(found_ids)
    species = []
    for r in rows:
        is_found = r["id"] in found
        species.append({
            "id": r["id"], "name": r["name"], "common_name": r["common_name"],
            "found": is_found,
            "photo": photos.get(r["id"]) if is_found else None,
        })
    species.sort(key=lambda s: (not s["found"],
                                (s["common_name"] or s["name"]).lower()))
    return {"genus_id": genus_id, "species": species}


def _build_explorer_rank(db, rank, root_id=None):
    """Flat list of all taxa at `rank` under the class root, each flagged
    found/unfound. Server-authoritative so it agrees with the summary chips."""
    if rank not in _EXPLORER_RANKS:
        rank = "family"
    root = (db.get_explorer_root() if root_id is None
            else db.get_taxon_by_id(root_id))
    empty = {"taxonomy_ready": root is not None, "valid_root": False,
             "rank": rank, "root": None, "found": 0, "total": 0, "items": []}
    if root is None:
        empty["taxonomy_ready"] = False
        return empty
    if root["rank"] != "class":
        return empty

    rows = db.get_taxon_subtree(root["id"])
    found = db.get_life_list_taxon_ids()
    nodes = {r["id"]: {**r, "children": [], "found_species": 0,
                       "total_species": 0}
             for r in rows}
    for n in nodes.values():
        pid = n["parent_id"]
        if pid in nodes and pid != n["id"]:
            nodes[pid]["children"].append(n)

    def rollup(node):
        if node["rank"] == "species":
            node["total_species"] = 1
            node["found_species"] = 1 if node["id"] in found else 0
        else:
            for c in node["children"]:
                rollup(c)
                node["total_species"] += c["total_species"]
                node["found_species"] += c["found_species"]
    rollup(nodes[root["id"]])

    def order_label(node):
        cur = node["parent_id"]
        while cur in nodes and nodes[cur]["rank"] != "order":
            cur = nodes[cur]["parent_id"]
        if cur in nodes and nodes[cur]["rank"] == "order":
            o = nodes[cur]
            return o["common_name"] or o["name"]
        return None

    targets = [n for n in nodes.values() if n["rank"] == rank]
    found_species_ids = [n["id"] for n in targets
                         if rank == "species" and n["found_species"] > 0]
    photos = (db.get_life_list_best_photo_by_taxon(found_species_ids)
              if found_species_ids else {})

    items = []
    for n in targets:
        is_found = n["found_species"] > 0
        items.append({
            "id": n["id"], "name": n["name"], "common_name": n["common_name"],
            "rank": rank, "found": is_found,
            "found_species": n["found_species"],
            "total_species": n["total_species"],
            "order": order_label(n),
            "photo": (photos.get(n["id"])
                      if (rank == "species" and is_found) else None),
        })
    items.sort(key=lambda i: (not i["found"],
                              (i["common_name"] or i["name"]).lower()))
    return {
        "taxonomy_ready": True, "valid_root": True, "rank": rank,
        "root": {"id": root["id"], "name": root["name"],
                 "common_name": root.get("common_name"), "rank": root["rank"]},
        "found": sum(1 for i in items if i["found"]), "total": len(items),
        "items": items,
    }


def create_life_list_blueprint(get_db, json_error, *, build_life_list_payload):
    """Build the life-list blueprint.

    ``build_life_list_payload(db, photos_per_species=12, photo_offset=0,
    species_filter=None)`` assembles the numbered species list with each
    species' curated photos. The export blueprint's website publishing
    builds the same payload, so both receive the one function. It stays in
    ``create_app`` and is injected rather than moved because it ranks photos
    with the highlight scoring and species canonicalization helpers that
    ``app.py`` shares with the Highlights and photo routes, and this module
    cannot import them from ``app`` without a circular import.
    """
    blueprint = Blueprint("life_list", __name__)

    @blueprint.route("/api/life-list")
    def api_life_list():
        db = get_db()
        payload = build_life_list_payload(
            db,
            photos_per_species=request.args.get("photos_per_species", 100, type=int),
        )
        return jsonify(payload)

    @blueprint.route("/api/life-list/species")
    def api_life_list_species():
        db = get_db()
        species = (request.args.get("species") or "").strip()
        if not species:
            return json_error("species required")
        offset = max(0, request.args.get("offset", 0, type=int))
        limit = max(1, min(request.args.get("limit", 100, type=int), 500))
        payload = build_life_list_payload(
            db,
            photos_per_species=limit,
            photo_offset=offset,
            species_filter=species,
        )
        if not payload["species"]:
            return json_error("species not found", 404)
        entry = payload["species"][0]
        return jsonify({
            "species": entry["species"],
            "photos": entry["photos"],
            "photo_count": entry["photo_count"],
            "loaded_count": entry["loaded_count"],
            "has_more": entry["has_more"],
        })

    @blueprint.route("/api/life-list/explorer")
    def api_life_list_explorer():
        db = get_db()
        root_id = request.args.get("root", type=int)  # None -> default Aves
        return jsonify(_build_explorer_payload(db, root_id=root_id))

    @blueprint.route("/api/life-list/explorer/species")
    def api_life_list_explorer_species():
        db = get_db()
        genus_id = request.args.get("genus", type=int)
        if not genus_id:
            return jsonify({"genus_id": None, "species": []})
        return jsonify(_build_explorer_species(db, genus_id))

    @blueprint.route("/api/life-list/explorer/rank")
    def api_life_list_explorer_rank():
        db = get_db()
        rank = request.args.get("rank", "family")
        root_id = request.args.get("root", type=int)
        return jsonify(_build_explorer_rank(db, rank, root_id=root_id))

    _LIFE_LIST_EXPORT_FORMATS = {
        "json": "json",
        "csv": "csv",
        "txt": "txt",
        "text": "txt",
        "file": "files",
        "files": "files",
        "filenames": "files",
        "file-list": "files",
    }

    def _life_list_export_bool_arg(name, default=False):
        raw = request.args.get(name)
        if raw is None:
            return default
        return str(raw).strip().lower() in {"1", "true", "yes", "on"}

    def _strip_life_list_locations(payload):
        for entry in payload.get("species", []):
            entry["locations"] = []

    def _life_list_export_photos(entry, mode):
        if mode == "all":
            return entry.get("photos") or []
        best = entry.get("best")
        return [best] if best else []

    _LIFE_LIST_SPECIES_CSV_COLUMNS = (
        "number",
        "species",
        "scientific_name",
        "common_name",
        "photo_count",
        "first_seen",
        "last_seen",
        "locations",
        "best_photo_id",
        "best_filename",
    )
    _LIFE_LIST_PHOTO_CSV_COLUMNS = (
        "number",
        "species",
        "scientific_name",
        "common_name",
        "photo_id",
        "filename",
        "timestamp",
        "is_life_list_photo",
        "quality_score",
        "locations",
    )

    def _life_list_export_response(body, mimetype, extension):
        today = datetime.now(UTC).date().isoformat()
        resp = make_response(body)
        resp.headers["Content-Type"] = mimetype
        resp.headers["Content-Disposition"] = (
            f'attachment; filename="vireo-life-list-{today}.{extension}"'
        )
        return resp

    def _life_list_csv_cell(value):
        if value is None:
            return ""
        text = str(value)
        stripped = text.lstrip(" \t\r\n")
        if stripped[:1] in {"=", "+", "-", "@"}:
            return "'" + text
        return text

    def _life_list_species_csv(payload, fieldnames=None):
        out = io.StringIO()
        fieldnames = fieldnames or _LIFE_LIST_SPECIES_CSV_COLUMNS
        writer = csv.DictWriter(out, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for entry in payload.get("species", []):
            best = entry.get("best") or {}
            writer.writerow({
                "number": entry.get("number") or "",
                "species": _life_list_csv_cell(entry.get("species")),
                "scientific_name": _life_list_csv_cell(entry.get("scientific_name")),
                "common_name": _life_list_csv_cell(entry.get("common_name")),
                "photo_count": entry.get("photo_count") or 0,
                "first_seen": _life_list_csv_cell(entry.get("first_seen")),
                "last_seen": _life_list_csv_cell(entry.get("last_seen")),
                "locations": _life_list_csv_cell(
                    "; ".join(entry.get("locations") or [])
                ),
                "best_photo_id": best.get("id") or "",
                "best_filename": _life_list_csv_cell(best.get("filename")),
            })
        return out.getvalue()

    def _life_list_photos_csv(payload, photo_mode, fieldnames=None):
        out = io.StringIO()
        fieldnames = fieldnames or _LIFE_LIST_PHOTO_CSV_COLUMNS
        writer = csv.DictWriter(out, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for entry in payload.get("species", []):
            for photo in _life_list_export_photos(entry, photo_mode):
                writer.writerow({
                    "number": entry.get("number") or "",
                    "species": _life_list_csv_cell(entry.get("species")),
                    "scientific_name": _life_list_csv_cell(entry.get("scientific_name")),
                    "common_name": _life_list_csv_cell(entry.get("common_name")),
                    "photo_id": photo.get("id") or "",
                    "filename": _life_list_csv_cell(photo.get("filename")),
                    "timestamp": _life_list_csv_cell(photo.get("timestamp")),
                    "is_life_list_photo": "yes" if photo.get("is_life_list_photo") else "no",
                    "quality_score": photo.get("quality_score")
                    if photo.get("quality_score") is not None else "",
                    "locations": _life_list_csv_cell(
                        "; ".join(entry.get("locations") or [])
                    ),
                })
        return out.getvalue()

    def _life_list_text(payload):
        lines = []
        for entry in payload.get("species", []):
            name = entry.get("species") or ""
            sci = entry.get("scientific_name")
            if sci and sci != name:
                name = f"{name} ({sci})"
            parts = [
                f"#{entry.get('number') or ''} {name}".strip(),
                f"{entry.get('photo_count') or 0} photo"
                + ("" if entry.get("photo_count") == 1 else "s"),
            ]
            if entry.get("first_seen"):
                parts.append(f"first seen {entry['first_seen']}")
            if entry.get("last_seen") and entry.get("last_seen") != entry.get("first_seen"):
                parts.append(f"latest {entry['last_seen']}")
            if entry.get("locations"):
                parts.append("locations: " + "; ".join(entry["locations"]))
            lines.append(" - ".join(parts))
        return "\n".join(lines) + ("\n" if lines else "")

    def _life_list_file_list(payload, photo_mode):
        seen = set()
        lines = []
        for entry in payload.get("species", []):
            for photo in _life_list_export_photos(entry, photo_mode):
                photo_id = photo.get("id")
                if photo_id in seen:
                    continue
                seen.add(photo_id)
                filename = photo.get("filename")
                if not filename:
                    continue
                lines.append(filename)
        return "\n".join(lines) + ("\n" if lines else "")

    @blueprint.route("/api/life-list/export")
    def api_life_list_export():
        fmt = (request.args.get("format") or "json").strip().lower()
        fmt = _LIFE_LIST_EXPORT_FORMATS.get(fmt)
        if not fmt:
            return json_error("format must be json, csv, txt, or files")

        detail = (request.args.get("detail") or "species").strip().lower()
        if detail not in {"species", "photos"}:
            return json_error("detail must be species or photos")

        photo_mode = (request.args.get("photos") or "best").strip().lower()
        if photo_mode not in {"best", "all"}:
            return json_error("photos must be best or all")

        csv_columns = None
        if "columns" in request.args:
            if fmt != "csv":
                return json_error("columns is only supported for csv exports")
            available_columns = (
                _LIFE_LIST_PHOTO_CSV_COLUMNS
                if detail == "photos"
                else _LIFE_LIST_SPECIES_CSV_COLUMNS
            )
            requested_columns = {
                column.strip()
                for column in request.args.get("columns", "").split(",")
                if column.strip()
            }
            if not requested_columns:
                return json_error("select at least one csv column")
            unknown_columns = requested_columns.difference(available_columns)
            if unknown_columns:
                return json_error(
                    "unknown csv columns: " + ", ".join(sorted(unknown_columns))
                )
            csv_columns = [
                column for column in available_columns
                if column in requested_columns
            ]

        db = get_db()
        uses_photo_scope = fmt == "files" or (fmt == "csv" and detail == "photos")
        payload_photos_per_species = (
            None if uses_photo_scope and photo_mode == "all"
            else request.args.get("photos_per_species", 12, type=int)
        )
        payload = build_life_list_payload(
            db,
            photos_per_species=payload_photos_per_species,
        )
        if not _life_list_export_bool_arg("include_locations"):
            _strip_life_list_locations(payload)

        if fmt == "json":
            body = json.dumps(payload, indent=2, ensure_ascii=False) + "\n"
            return _life_list_export_response(body, "application/json", "json")
        if fmt == "csv":
            body = (
                _life_list_photos_csv(payload, photo_mode, csv_columns)
                if detail == "photos"
                else _life_list_species_csv(payload, csv_columns)
            )
            return _life_list_export_response(body, "text/csv; charset=utf-8", "csv")
        if fmt == "files":
            return _life_list_export_response(
                _life_list_file_list(payload, photo_mode),
                "text/plain; charset=utf-8",
                "txt",
            )
        return _life_list_export_response(
            _life_list_text(payload),
            "text/plain; charset=utf-8",
            "txt",
        )

    return blueprint
