"""Photo export with resize, quality control, and template-based naming."""

import logging
import os
import re

from image_loader import load_image

log = logging.getLogger(__name__)

# Characters not allowed in filenames (covers Windows + macOS + Linux)
_UNSAFE_RE = re.compile(r'[<>:"/|?*\\]')


def sanitize_filename(name):
    """Replace filesystem-unsafe characters with underscores."""
    return _UNSAFE_RE.sub("_", name)


def resolve_template(template, photo, species=None, seq=1):
    """Resolve a naming template against photo metadata.

    Args:
        template: naming template with {variable} placeholders
        photo: dict with filename, timestamp, rating, folder_name
        species: species name string or None (falls back to "unknown")
        seq: sequence number (1-based)

    Returns:
        Resolved path string (may contain '/' for subdirectories)
    """
    stem = os.path.splitext(photo["filename"])[0]
    ts = photo.get("timestamp") or ""

    if ts:
        date_part = ts[:10]
        time_part = ts[11:19].replace(":", "") if len(ts) >= 19 else "000000"
    else:
        date_part = "unknown-date"
        time_part = "000000"

    species_name = species or "unknown"

    replacements = {
        "original": stem,
        "date": date_part,
        "datetime": f"{date_part}_{time_part}",
        "species": sanitize_filename(species_name),
        "rating": str(photo.get("rating") or 0),
        "seq": f"{seq:04d}",
        "folder": sanitize_filename(photo.get("folder_name") or ""),
    }

    result = template
    for key, value in replacements.items():
        result = result.replace("{" + key + "}", value)

    return result


def export_photos(db, vireo_dir, photo_ids, destination, options=None, progress_cb=None):
    """Export photos to a destination directory with optional resize and renaming.

    Args:
        db: Database instance
        vireo_dir: path to ~/.vireo/
        photo_ids: list of photo IDs to export
        destination: absolute path to output directory
        options: dict with keys:
            naming_template: str (default "{original}")
            max_size: int or None -- max long-edge pixels
            quality: int 1-100 (default 92)
            working_copy_max_size: int -- the cap used when generating
                working copies (default 4096); used to decide whether
                the working copy can satisfy the requested max_size.
        progress_cb: optional callback(current, total, current_file)

    Returns:
        dict with keys: exported (int), errors (list of str), destination (str)
    """
    options = options or {}
    template = options.get("naming_template", "{original}")
    max_size = options.get("max_size")
    if max_size is not None:
        max_size = int(max_size)
    quality = options.get("quality", 92)
    wc_max_raw = options.get("working_copy_max_size")
    wc_max = int(wc_max_raw) if wc_max_raw is not None else 4096

    os.makedirs(destination, exist_ok=True)

    photos_map = db.get_photos_by_ids(photo_ids)
    folders = {f["id"]: f["path"] for f in db.get_folder_tree()}

    # Get species keywords for all photos in one query
    species_map = db.get_species_keywords_for_photos(photo_ids)

    # Track sequence numbers per subdirectory
    seq_counters = {}
    exported = 0
    errors = []

    for i, pid in enumerate(photo_ids):
        photo = photos_map.get(pid)
        if not photo:
            errors.append(f"Photo {pid} not found in database")
            if progress_cb:
                progress_cb(i + 1, len(photo_ids), "")
            continue

        # Resolve source path.  Use the working copy only when resizing to
        # a size the working copy can satisfy (i.e. max_size <= wc cap).
        # Otherwise use the original to avoid silent downscaling.
        use_wc = bool(max_size) and max_size <= wc_max
        source_path = _resolve_source(photo, vireo_dir, folders, use_working_copy=use_wc)
        if not source_path or not os.path.isfile(source_path):
            errors.append(f"{photo['filename']}: source file missing")
            if progress_cb:
                progress_cb(i + 1, len(photo_ids), photo["filename"])
            continue

        # Get species (first species keyword, or None)
        species_list = species_map.get(pid, [])
        species = species_list[0] if species_list else None

        # Build photo dict for template
        folder_path = folders.get(photo["folder_id"], "")
        photo_info = {
            "filename": photo["filename"],
            "timestamp": photo["timestamp"],
            "rating": photo["rating"],
            "folder_name": os.path.basename(folder_path),
        }

        # Determine subdirectory for sequence counter
        # Render template once to extract the directory part
        subdir_key = os.path.dirname(
            resolve_template(template, photo_info, species=species, seq=0)
        )
        seq_counters.setdefault(subdir_key, 0)
        seq_counters[subdir_key] += 1
        seq = seq_counters[subdir_key]

        # Resolve final output path
        rel_path = resolve_template(template, photo_info, species=species, seq=seq)
        # Guard against path traversal: strip leading slashes/dots so that
        # absolute paths and ".." segments cannot escape the destination dir.
        rel_path_safe = os.path.normpath(rel_path).lstrip(os.sep + ".")
        out_path = os.path.join(destination, rel_path_safe + ".jpg")
        # Final containment check: resolved path must start with destination.
        # dest_real may already end with os.sep when destination is a root dir
        # (e.g. "/" on POSIX), so avoid doubling the separator.
        dest_real = os.path.realpath(destination)
        out_real = os.path.realpath(out_path)
        dest_prefix = dest_real if dest_real.endswith(os.sep) else dest_real + os.sep
        if not out_real.startswith(dest_prefix) and out_real != dest_real:
            errors.append(f"{photo['filename']}: unsafe output path rejected")
            if progress_cb:
                progress_cb(i + 1, len(photo_ids), photo["filename"])
            continue

        # Handle collisions
        out_path = _deduplicate_path(out_path)

        # Ensure subdirectory exists
        out_dir = os.path.dirname(out_path)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)

        # Load, resize, and save
        try:
            img = load_image(source_path, max_size=max_size or None)
            if img is None:
                errors.append(f"{photo['filename']}: failed to load image")
                if progress_cb:
                    progress_cb(i + 1, len(photo_ids), photo["filename"])
                continue
            img.save(out_path, "JPEG", quality=quality)
            img.close()
            exported += 1
        except Exception as exc:
            log.warning("Export failed for %s: %s", photo["filename"], exc)
            errors.append(f"{photo['filename']}: {exc}")

        if progress_cb:
            progress_cb(i + 1, len(photo_ids), photo["filename"])

    return {"exported": exported, "errors": errors, "destination": destination}


def _resolve_source(photo, vireo_dir, folders, use_working_copy=False):
    """Return the best available source path for a photo.

    When use_working_copy is True (resize is requested), prefers the working
    copy so RAW files are served from a pre-decoded JPEG (faster).  When
    use_working_copy is False (full-resolution export), always uses the
    original file to avoid silently downscaling via a capped working copy.

    photo is a sqlite3.Row (supports [] but not .get()), so we use
    bracket access with a guard for the optional working_copy_path field.
    """
    if use_working_copy:
        wc_path = photo["working_copy_path"]
        if wc_path:
            wc = os.path.join(vireo_dir, wc_path)
            if os.path.exists(wc):
                return wc
    folder_path = folders.get(photo["folder_id"], "")
    return os.path.join(folder_path, photo["filename"])


def _deduplicate_path(path):
    """Append _2, _3, etc. if path already exists."""
    if not os.path.exists(path):
        return path
    stem, ext = os.path.splitext(path)
    counter = 2
    while os.path.exists(f"{stem}_{counter}{ext}"):
        counter += 1
    return f"{stem}_{counter}{ext}"
