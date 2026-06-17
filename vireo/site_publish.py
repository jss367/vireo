"""Export Vireo workspace data as static-site-ready JSON and images."""

from __future__ import annotations

import copy
import json
import os
import re
from datetime import UTC, datetime
from pathlib import Path

from export import (
    _developed_can_satisfy_size,
    _DevelopedDirIndex,
    _find_developed_output,
    _resolve_source,
)
from image_loader import load_image

_SLUG_RE = re.compile(r"[^a-z0-9]+")
_PRIVATE_PHOTO_FIELDS = {"mask_path"}


def slugify(value, fallback="item"):
    """Return a lowercase URL/file safe slug."""
    slug = _SLUG_RE.sub("-", str(value or "").lower()).strip("-")
    return slug or fallback


def _photo_refs(life_list, highlights):
    refs = {}

    def add(photo, context):
        if not photo or not photo.get("id"):
            return
        refs.setdefault(photo["id"], []).append((photo, context))

    for species in life_list.get("species", []):
        context = species.get("species") or "life-list"
        add(species.get("best"), context)
        for photo in species.get("photos") or []:
            add(photo, context)

    for bucket in highlights.get("buckets", []):
        context = bucket.get("species") or "highlights"
        for photo in bucket.get("photos") or []:
            add(photo, context)

    unidentified = highlights.get("unidentified") or {}
    for photo in unidentified.get("photos") or []:
        add(photo, "unidentified")

    return refs


def _rel_image_path(photo, context):
    stem = os.path.splitext(photo.get("filename") or "")[0]
    name = slugify(stem, fallback=f"photo-{photo['id']}")
    prefix = slugify(context, fallback="photo")
    return f"images/photos/{prefix}-{photo['id']}-{name}.jpg"


def _developed_required_size(photo, max_size):
    if max_size is None:
        return None
    original_w = photo.get("width")
    original_h = photo.get("height")
    if original_w and original_h:
        return min(max_size, max(original_w, original_h))
    return max_size


def _export_image(db, vireo_dir, photo, rel_path, destination, options, folders, index):
    source = None
    folder_path = folders.get(photo.get("folder_id"), "")
    developed_dir = options.get("developed_dir") or ""
    max_size = options.get("max_size")
    if max_size is not None:
        max_size = int(max_size)
    if developed_dir:
        source = _find_developed_output(
            photo.get("filename") or "",
            folder_path,
            developed_dir,
            index,
        )
        required_size = _developed_required_size(photo, max_size)
        if source and not _developed_can_satisfy_size(source, photo, required_size):
            source = None

    if not source:
        wc_max = int(options.get("working_copy_max_size", 4096))
        use_working_copy = bool(max_size) and max_size <= wc_max
        source = _resolve_source(photo, vireo_dir, folders, use_working_copy)

    if not source or not os.path.isfile(source):
        return False, f"{photo.get('filename') or photo.get('id')}: source file missing"

    out_path = Path(destination) / rel_path
    out_path.parent.mkdir(parents=True, exist_ok=True)
    img = load_image(source, max_size=max_size)
    if img is None:
        return False, f"{photo.get('filename') or photo.get('id')}: failed to load image"
    try:
        img.save(out_path, "JPEG", quality=int(options.get("quality", 88)))
    finally:
        img.close()
    return True, None


def _write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
        f.write("\n")


def _strip_private_photo_fields(highlights):
    for bucket in highlights.get("buckets", []):
        for photo in bucket.get("photos") or []:
            for field in _PRIVATE_PHOTO_FIELDS:
                photo.pop(field, None)

    unidentified = highlights.get("unidentified") or {}
    for photo in unidentified.get("photos") or []:
        for field in _PRIVATE_PHOTO_FIELDS:
            photo.pop(field, None)


def publish_site(db, vireo_dir, destination, life_list, highlights=None, options=None, progress_cb=None):
    """Write JSON manifests and optimized photos for a static website.

    Args:
        db: active-workspace Database instance.
        vireo_dir: Vireo data directory, used to resolve working copies.
        destination: absolute output directory.
        life_list: payload matching ``/api/life-list``.
        highlights: payload matching ``/api/highlights``.
        options: max_size, quality, working_copy_max_size, developed_dir,
            include_locations.
        progress_cb: optional callback(current, total, current_file).
    """
    options = options or {}
    highlights = highlights or {"buckets": [], "meta": {}}
    include_locations = bool(options.get("include_locations", True))

    destination_path = Path(destination)
    data_dir = destination_path / "data"
    destination_path.mkdir(parents=True, exist_ok=True)

    published_life_list = copy.deepcopy(life_list)
    published_highlights = copy.deepcopy(highlights)
    _strip_private_photo_fields(published_highlights)
    if not include_locations:
        for entry in published_life_list.get("species", []):
            entry["locations"] = []

    refs = _photo_refs(published_life_list, published_highlights)
    photo_ids = sorted(refs)
    photos_map = db.get_photos_by_ids(photo_ids) if photo_ids else {}
    folders = {f["id"]: f["path"] for f in db.get_folder_tree()}
    index = _DevelopedDirIndex()
    exported = 0
    errors = []

    total = len(photo_ids)
    for i, photo_id in enumerate(photo_ids, start=1):
        db_photo_row = photos_map.get(photo_id)
        if not db_photo_row:
            errors.append(f"Photo {photo_id} not found in database")
            if progress_cb:
                progress_cb(i, total, "")
            continue
        db_photo = dict(db_photo_row)

        context = refs[photo_id][0][1]
        rel_path = _rel_image_path(db_photo, context)
        ok, err = _export_image(
            db,
            vireo_dir,
            db_photo,
            rel_path,
            destination,
            options,
            folders,
            index,
        )
        if ok:
            exported += 1
            for ref, _context in refs[photo_id]:
                ref["image"] = rel_path
        elif err:
            errors.append(err)

        if progress_cb:
            progress_cb(i, total, db_photo.get("filename") or "")

    generated_at = datetime.now(UTC).isoformat()
    site_manifest = {
        "schema_version": 1,
        "generated_at": generated_at,
        "sections": {
            "life_list": "data/life-list.json",
            "highlights": "data/highlights.json",
        },
        "counts": {
            "life_list_species": published_life_list.get("meta", {}).get("species_count", 0),
            "life_list_photos": published_life_list.get("meta", {}).get("photo_count", 0),
            "highlight_buckets": len(published_highlights.get("buckets", [])),
            "exported_images": exported,
        },
    }
    published_life_list.setdefault("meta", {})["generated_at"] = generated_at
    published_highlights.setdefault("meta", {})["generated_at"] = generated_at

    _write_json(data_dir / "site.json", site_manifest)
    _write_json(data_dir / "life-list.json", published_life_list)
    _write_json(data_dir / "highlights.json", published_highlights)

    return {
        "destination": destination,
        "data_files": [
            "data/site.json",
            "data/life-list.json",
            "data/highlights.json",
        ],
        "exported_images": exported,
        "errors": errors,
    }
