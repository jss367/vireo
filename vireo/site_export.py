"""Portable, complete workspace exports with shared photos and album manifests."""

from __future__ import annotations

import copy
import json
import shutil
from datetime import UTC, datetime
from pathlib import Path
from tempfile import mkdtemp

from export import _DevelopedDirIndex, _get_photo_exif_data, load_export_image
from site_publish import _write_json, slugify

README = """# Photo site export

This folder contains a snapshot of the active Vireo workspace. All catalogued
photos are included, regardless of publication, classification, flags, or album
membership. Unavailable photos remain in the metadata with an error and no image.
See site.json for completion status and errors. A run with errors is incomplete.

- photos/: full-resolution JPEG renders including saved edits, quality 95.
  These are rendered copies, not original RAW files or an editable catalog backup.
- photos.json: every photo's ID, original filename, capture date, rating, flag,
  species, keywords, album IDs, saved edits, and available title/caption/notes metadata.
  Location keywords and coordinates are included only when requested.
- albums/<collection ID>-<name>/album.json: each saved collection as an album,
  including its definition and its membership at export time. Photos in multiple
  albums are stored once; each album references the shared photo ID and image.
  Empty albums are preserved. Unresolvable collections have status "error" and
  unknown membership (null), never a silently broadened or empty result.
- life-list.json: the complete life list in canonical JSON, with no per-species
  photo limit. It follows the Life List page's species inclusion rules.
- site.json: format version, counts, album index, export options, and errors.

All image and manifest paths are relative to this export's root directory.
Photo and collection IDs disambiguate duplicate names. An image value of null
means the image could not be exported; its error explains why. Life-list photos
outside the export snapshot also have a null image.

Each export creates a new folder and leaves existing exports untouched.
Cancellation or a fatal failure removes the new, incomplete folder.
"""


def _caption_fields(exif_data):
    try:
        data = json.loads(exif_data) if isinstance(exif_data, str) else exif_data
    except (ValueError, TypeError):
        data = {}
    data = data if isinstance(data, dict) else {}
    # Scanner metadata is grouped by ExifTool namespace; older imports may
    # contain flat tags. Prefer XMP text, then IPTC and EXIF fallbacks.
    candidates = [data.get(group, {}) for group in ("XMP", "IPTC", "EXIF")] + [data]
    fields = {}
    for name, keys in {
        "title": ("XMP:Title", "Title", "ObjectName"),
        "caption": ("XMP:Description", "Description", "ImageDescription", "Caption-Abstract"),
        "notes": ("UserComment", "Comment"),
    }.items():
        fields[name] = next((
            tags[k] for tags in candidates if isinstance(tags, dict)
            for k in keys if isinstance(tags.get(k), str)
        ), None)
    return fields


def export_site(db, vireo_dir, destination, *, build_life_list, resolve_visual,
                options=None, progress_cb=None, checkpoint=None, begin_commit=None):
    """Export the active workspace into a new child folder of destination.

    Photo queries are batched; decoded images are released after each file.
    A failed photo or collection produces a usable but explicitly incomplete
    export. Fatal errors and cancellation remove only this run's new folder.
    """
    options = options or {}
    include_locations = options.get("include_locations", False)

    def progress(current, total, name, phase):
        if checkpoint:
            checkpoint()
        if progress_cb:
            progress_cb(current, total, name, phase)

    progress(0, 0, "", "Preparing site export")
    photo_ids = sorted(db.query_photo_ids([], include_offline_folders=True))
    eligible = set(photo_ids)
    total = len(photo_ids)
    albums = []
    memberships = {}
    errors = []
    for row in db.get_collections():
        progress(0, total, row["name"], "Reading albums")
        album = {"id": row["id"], "name": row["name"], "status": "complete"}
        album["manifest"] = f"albums/{row['id']}-{slugify(row['name'])[:80]}/album.json"
        try:
            rules = json.loads(row["rules"])
            visual = json.loads(row["visual_json"]) if row["visual_json"] else None
            album.update(rules=rules, visual=visual)
            if visual is not None:
                info, ids, _ = resolve_visual(
                    db, rules, visual, include_offline_folders=True,
                )
                if ids is None:
                    raise ValueError(f"Visual search unavailable: {info['status']}")
            else:
                ids = db.query_photo_ids(rules, include_offline_folders=True)
            album["photo_ids"] = sorted(eligible.intersection(ids))
            for pid in album["photo_ids"]:
                memberships.setdefault(pid, []).append(row["id"])
        except (ValueError, TypeError, KeyError) as exc:
            album.update(status="error", photo_ids=None, error=str(exc))
            errors.append(f"Album {row['name']}: {exc}")
        albums.append(album)

    progress(0, total, "", "Reading life list")
    life_list = copy.deepcopy(build_life_list(db, photos_per_species=None))
    # The Browse tree omits offline folders. Keep their real paths here so
    # missing sources are reported (or a developed/working copy can be used).
    folders = {f["id"]: f["path"] for f in db.conn.execute(
        "SELECT f.id, f.path FROM folders f "
        "JOIN workspace_folders wf ON wf.folder_id = f.id WHERE wf.workspace_id = ?",
        (db._ws_id(),),
    )}
    index = _DevelopedDirIndex()
    parent = Path(destination)
    parent.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    output = Path(mkdtemp(prefix=f"site-export-{stamp}-", dir=parent))
    images = {}
    exported = 0
    try:
        (output / "photos").mkdir()
        with (output / "photos.json").open("w", encoding="utf-8") as stream:
            stream.write('[\n')
            current = 0
            for offset in range(0, total, 200):
                batch = photo_ids[offset:offset + 200]
                photos = db.get_photos_by_ids(batch)
                recipes = db.get_photo_edit_recipes(batch)
                exif = _get_photo_exif_data(db, batch)
                keywords = db.get_keywords_for_photos(batch)
                species = db.get_species_keywords_for_photos(batch, include_identities=True)
                locations = db.get_effective_photo_locations(
                    batch, verify_workspace=False,
                ) if include_locations else {}
                for pid in batch:
                    photo = dict(photos[pid]) if pid in photos else {"id": pid}
                    filename = photo.get("filename") or f"photo-{pid}"
                    progress(current, total, filename, "Exporting photos")
                    record = {key: photo.get(key) for key in (
                        "id", "filename", "timestamp", "rating", "flag",
                    )}
                    record.update(_caption_fields(exif.get(pid)))
                    record.update(
                        album_ids=memberships.get(pid, []),
                        species=species.get(pid, []),
                        keywords=[k for k in keywords.get(pid, [])
                                  if include_locations or k.get("type") != "location"],
                        edits=recipes.get(pid), image=None,
                    )
                    if include_locations:
                        location = locations.get(pid) or {}
                        record.update(latitude=location.get("latitude"), longitude=location.get("longitude"))
                    path = f"photos/{pid}-{slugify(Path(filename).stem)[:80]}.jpg"
                    try:
                        if pid not in photos:
                            raise ValueError("Photo was removed during export")
                        img = load_export_image(
                            photo, vireo_dir, folders, recipe=recipes.get(pid),
                            exif_data=exif.get(pid), max_size=None,
                            wc_max=options.get("working_copy_max_size", 4096),
                            developed_dir=options.get("developed_dir", ""),
                            developed_index=index,
                        )
                        try:
                            img.save(output / path, "JPEG", quality=95)
                        finally:
                            img.close()
                        record["image"] = path
                        images[pid] = path
                        exported += 1
                    except Exception as exc:
                        (output / path).unlink(missing_ok=True)
                        record["error"] = str(exc)
                        errors.append(f"Photo {pid} ({filename}): {exc}")
                    if current:
                        stream.write(',\n')
                    json.dump(record, stream, ensure_ascii=False)
                    current += 1
                    progress(current, total, filename, "Exporting photos")
            stream.write('\n]\n')

        progress(total, total, "", "Writing export manifests")
        for album in albums:
            album["photos"] = (
                [{"id": pid, "image": images.get(pid)} for pid in album["photo_ids"]]
                if album["photo_ids"] is not None else None
            )
            _write_json(output / album["manifest"], album)
        for entry in life_list.get("species", []):
            if not include_locations:
                entry["locations"] = []
            for photo in [entry.get("best"), *entry.get("photos", [])]:
                if photo:
                    photo["image"] = images.get(photo["id"])
        _write_json(output / "life-list.json", life_list)
        _write_json(output / "site.json", {
            "schema_version": 1,
            "generated_at": datetime.now(UTC).isoformat(),
            "workspace_id": db._ws_id(),
            "status": "incomplete" if errors else "complete",
            "include_locations": include_locations,
            "photo_count": total,
            "exported_images": exported,
            "photos": "photos.json", "life_list": "life-list.json",
            "albums": [{k: a[k] for k in ("id", "name", "manifest", "status")} for a in albums],
            "errors": errors,
        })
        (output / "README.md").write_text(README, encoding="utf-8")
        progress(total, total, "", "Finishing site export")
        if begin_commit and not begin_commit():
            from web.background_jobs import JobCancelled
            raise JobCancelled("Site export cancelled")
        return {
            "ok": not errors,
            "destination": str(output), "exported_images": exported,
            "photo_count": total, "album_count": len(albums), "errors": errors,
        }
    except BaseException:
        shutil.rmtree(output)
        raise
