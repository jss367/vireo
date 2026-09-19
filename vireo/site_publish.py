"""Export Vireo workspace data as static-site-ready JSON and images."""

from __future__ import annotations

import contextlib
import copy
import json
import os
import re
import shutil
import threading
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryFile, mkdtemp
from weakref import WeakValueDictionary

from export import (
    _DevelopedDirIndex,
    _get_photo_exif_data,
    load_export_image,
)

_SLUG_RE = re.compile(r"[^a-z0-9]+")
_PRIVATE_PHOTO_FIELDS = {"mask_path"}
_PUBLISH_LOCKS = WeakValueDictionary()
_PUBLISH_LOCKS_GUARD = threading.Lock()


class PublishRecoveryError(RuntimeError):
    """A failed publish whose backup files must be retained for recovery."""


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


def _export_image(vireo_dir, photo, rel_path, destination, options, folders, index,
                  recipe, exif_data):
    max_size = options.get("max_size")
    if max_size is not None:
        max_size = int(max_size)
    try:
        img = load_export_image(
            photo, vireo_dir, folders, recipe=recipe, exif_data=exif_data,
            max_size=max_size, wc_max=int(options.get("working_copy_max_size", 4096)),
            developed_dir=options.get("developed_dir") or "", developed_index=index,
        )
    except Exception as exc:
        return False, f"{photo.get('filename') or photo.get('id')}: {exc}"
    try:
        out_path = Path(destination) / rel_path
        out_path.parent.mkdir(parents=True, exist_ok=True)
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
    highlights.pop("folders", None)

    for bucket in highlights.get("buckets", []):
        for photo in bucket.get("photos") or []:
            for field in _PRIVATE_PHOTO_FIELDS:
                photo.pop(field, None)

    unidentified = highlights.get("unidentified") or {}
    for photo in unidentified.get("photos") or []:
        for field in _PRIVATE_PHOTO_FIELDS:
            photo.pop(field, None)


def publish_site(db, vireo_dir, destination, life_list, highlights=None, options=None,
                 progress_cb=None, cancel_check=None, begin_commit=None):
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
        cancel_check: optional callable; stop before the next photo when true.
        begin_commit: optional callable that honors a pending pause, rejects
            cancellation, and prevents later pause/cancellation through job
            completion; false aborts before replacing any published files.
    """
    Path(destination).mkdir(parents=True, exist_ok=True)
    staging = mkdtemp(prefix=".vireo-publish-", dir=destination)
    retain_backup = False
    try:
        return _publish_site(
            db, vireo_dir, destination, staging, life_list, highlights, options,
            progress_cb, cancel_check, begin_commit,
        )
    except PublishRecoveryError:
        retain_backup = True
        raise
    finally:
        if not retain_backup:
            shutil.rmtree(staging, ignore_errors=True)


def _publish_site(db, vireo_dir, destination, staging, life_list, highlights,
                  options, progress_cb, cancel_check, begin_commit):
    options = options or {}
    highlights = highlights or {"buckets": [], "meta": {}}
    include_locations = bool(options.get("include_locations", False))

    destination_path = Path(destination)
    staging_path = Path(staging)
    data_dir = staging_path / "data"
    image_paths = []

    published_life_list = copy.deepcopy(life_list)
    published_highlights = copy.deepcopy(highlights)
    _strip_private_photo_fields(published_highlights)
    if not include_locations:
        for entry in published_life_list.get("species", []):
            entry["locations"] = []

    refs = _photo_refs(published_life_list, published_highlights)
    photo_ids = sorted(refs)
    photos_map = db.get_photos_by_ids(photo_ids) if photo_ids else {}
    recipes = db.get_photo_edit_recipes(photo_ids)
    exif_data = _get_photo_exif_data(db, photo_ids)
    folders = {f["id"]: f["path"] for f in db.get_folder_tree()}
    index = _DevelopedDirIndex()
    exported = 0
    errors = []

    total = len(photo_ids)
    for i, photo_id in enumerate(photo_ids, start=1):
        if cancel_check and cancel_check():
            break
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
            vireo_dir,
            db_photo,
            rel_path,
            staging,
            options,
            folders,
            index,
            recipes.get(photo_id),
            exif_data.get(photo_id),
        )
        if ok:
            exported += 1
            image_paths.append(rel_path)
            for ref, _context in refs[photo_id]:
                ref["image"] = rel_path
        elif err:
            errors.append(err)

        if progress_cb:
            progress_cb(i, total, db_photo.get("filename") or "")

    # Keep the previous manifests when cancellation leaves this publish
    # incomplete; otherwise they would advertise photos we never exported.
    if cancel_check and cancel_check():
        return {"destination": destination, "data_files": [],
                "exported_images": exported, "errors": errors}

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

    data_files = ["data/site.json", "data/life-list.json", "data/highlights.json"]
    if not _commit_site(destination_path, staging_path, image_paths + data_files,
                        cancel_check, begin_commit):
        return {"destination": destination, "data_files": [],
                "exported_images": exported, "errors": errors}

    return {
        "destination": destination,
        "data_files": data_files,
        "exported_images": exported,
        "errors": errors,
    }


def _commit_site(destination_path, staging_path, paths, cancel_check=None, begin_commit=None):
    """Serialize commits to one destination, with rollback on write failure."""
    key = os.path.normcase(os.path.realpath(destination_path))
    with _PUBLISH_LOCKS_GUARD:
        lock = _PUBLISH_LOCKS.setdefault(key, threading.Lock())
    while not lock.acquire(timeout=0.1):
        if cancel_check and cancel_check():
            return False
    try:
        return _commit_site_locked(destination_path, staging_path, paths,
                                   cancel_check, begin_commit)
    finally:
        lock.release()


def _commit_site_locked(destination_path, staging_path, paths, cancel_check, begin_commit):
    # Check every destination before modifying any published content. Existing
    # files may be writable even when their directories cannot be modified.
    for rel_path in paths:
        if cancel_check and cancel_check():
            return False
        out_path = destination_path / rel_path
        out_path.parent.mkdir(parents=True, exist_ok=True)
        if out_path.is_symlink() and not out_path.exists():
            raise ValueError(f"Publish destination is a broken symbolic link: {out_path}")
        if out_path.exists():
            os.close(os.open(out_path, os.O_WRONLY))
        else:
            with TemporaryFile(dir=out_path.parent):
                pass

    # Reserve the previous generation before touching any live bytes. Backups
    # live under the writable staging root, so existing writable files in
    # protected child directories remain supported. A backup failure leaves
    # the published site unchanged.
    originals = {}
    backup_dir = staging_path / "rollback"
    backup_dir.mkdir()
    for index, rel_path in enumerate(paths):
        if cancel_check and cancel_check():
            return False
        out_path = destination_path / rel_path
        backup = None
        if out_path.exists():
            backup = backup_dir / str(index)
            shutil.copyfile(out_path, backup)
            with backup.open("rb+") as saved:
                os.fsync(saved.fileno())
            shutil.copystat(out_path, backup)
        originals[rel_path] = backup
    _write_json(staging_path / "recovery.json", {
        rel: str(backup.relative_to(staging_path)) if backup else None
        for rel, backup in originals.items()
    })

    # Keep staging cancellable, then coordinate writes with the job runner's
    # lock so a late Stop cannot interrupt the published files.
    can_commit = begin_commit() if begin_commit is not None else not (
        cancel_check and cancel_check()
    )
    if not can_commit:
        return False

    modified = []
    try:
        for rel_path in paths:
            # Include the current file: copyfile may truncate it before raising.
            modified.append(rel_path)
            # Keep the existing inode's ownership, ACLs, and mode.
            shutil.copyfile(staging_path / rel_path, destination_path / rel_path)
    except BaseException as error:
        # Discard the staged NEW bytes first to make room for restoration
        # after ENOSPC. Never discard a backup until recovery has succeeded.
        for rel_path in paths:
            with contextlib.suppress(OSError):
                (staging_path / rel_path).unlink()
        failed = []
        for rel_path in reversed(modified):
            out_path = destination_path / rel_path
            backup = originals[rel_path]
            try:
                if backup is None:
                    out_path.unlink(missing_ok=True)
                else:
                    # Restore in place, including under protected directories.
                    # This also preserves ownership/ACLs on the original inode.
                    with backup.open("rb") as saved, out_path.open("wb") as live:
                        shutil.copyfileobj(saved, live)
                        live.flush()
                        os.fsync(live.fileno())
                    shutil.copystat(backup, out_path)
            except OSError:
                failed.append(rel_path)
        if failed:
            raise PublishRecoveryError(
                f"Publishing failed ({error}); could not restore {len(failed)} file(s). "
                f"Previous site files and recovery.json are preserved in {staging_path}"
            ) from error
        raise

    return True
