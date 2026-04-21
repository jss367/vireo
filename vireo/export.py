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
            developed_dir: str -- optional path to darktable-developed
                outputs (mirrors darktable_output_dir config). Export
                prefers a developed JPG/TIFF at
                <developed_dir>/<folder_id>/<stem>.<ext> (or at the
                default <folder>/developed/<stem>.<ext>) over
                re-decoding the RAW. The folder_id nesting matches the
                develop job's write convention and keeps lookups one-to-one
                even when two source folders share a basename. As a legacy
                fallback, <developed_dir>/<stem>.<ext> is also probed so
                libraries developed before the folder_id convention was
                introduced still pick up their developed outputs.
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
    try:
        wc_max = int(options.get("working_copy_max_size", 4096))
    except (ValueError, TypeError):
        wc_max = 4096
    developed_dir = options.get("developed_dir") or ""

    os.makedirs(destination, exist_ok=True)

    photos_map = db.get_photos_by_ids(photo_ids)
    folders = {f["id"]: f["path"] for f in db.get_folder_tree()}

    # Get species keywords for all photos in one query
    species_map = db.get_species_keywords_for_photos(photo_ids)

    # Track sequence numbers per subdirectory
    seq_counters = {}
    exported = 0
    errors = []

    # Per-export cache of developed-directory scans. Keyed by directory
    # path; each value is the (stem, ext_lower) → absolute-path map that
    # _find_developed_output would otherwise rebuild for every photo.
    # Large exports routinely probe the same directory N times; caching
    # keeps that cost O(1) per photo after the first hit.
    developed_index = _DevelopedDirIndex()

    for i, pid in enumerate(photo_ids):
        photo = photos_map.get(pid)
        if not photo:
            errors.append(f"Photo {pid} not found in database")
            if progress_cb:
                progress_cb(i + 1, len(photo_ids), "")
            continue

        # Resolve source path.  Precedence:
        #   1. darktable-developed output ("perfected" rendering) — takes
        #      priority over RAW so Export ships what the user sees after
        #      Develop, not a fresh libraw decode of the RAW.
        #   2. working copy when resizing to a size it can satisfy.
        #   3. original file (default; also used for full-res exports).
        folder_path = folders.get(photo["folder_id"], "")
        source_path = _find_developed_output(
            photo["filename"],
            photo["folder_id"],
            folder_path,
            developed_dir,
            developed_index,
        )
        if not source_path:
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


_PREFERRED_DEVELOPED_EXTS = ("jpg", "jpeg", "tiff", "tif")


class _DevelopedDirIndex:
    """Lazy, per-export cache of directory listings for developed lookups.

    Each directory is scanned with os.listdir once and indexed as
    (exact stem, lowercased ext) → absolute path. Subsequent lookups
    against the same directory are O(1), which avoids turning the
    per-photo probe into quadratic work on large exports where many
    photos share a developed directory.
    """

    def __init__(self):
        self._cache = {}

    def lookup(self, base, stem):
        entries = self._cache.get(base)
        if entries is None:
            entries = {}
            try:
                names = os.listdir(base)
            except OSError:
                names = []
            # Stem match must be case-sensitive to avoid collisions
            # between photos whose names differ only by case. Extension
            # match is case-insensitive so developed files written as
            # .JPG / .TIFF are still picked up.
            for name in names:
                ent_stem, ent_ext = os.path.splitext(name)
                ext_key = ent_ext[1:].lower() if ent_ext.startswith(".") else ent_ext.lower()
                entries.setdefault((ent_stem, ext_key), os.path.join(base, name))
            self._cache[base] = entries
        for ext in _PREFERRED_DEVELOPED_EXTS:
            path = entries.get((stem, ext))
            if path and os.path.isfile(path):
                return path
        return None


def _find_developed_output(filename, folder_id, folder_path, developed_dir, index=None):
    """Return the path to a darktable-developed output for this photo, or None.

    Lookup locations are probed in order:

      * <developed_dir>/<folder_id>/<stem>.<ext> — matches how the develop
        job writes when darktable_output_dir is configured (the flat output
        dir is nested per folder_id to avoid collisions).
      * <folder_path>/developed/<stem>.<ext> — the default develop-job
        location, naturally disambiguated because each source folder has
        its own developed/ subdir.
      * <developed_dir>/<stem>.<ext> — legacy flat layout used by older
        versions of the develop job. Probed last so that any new
        folder-scoped output wins, but kept so libraries developed before
        the folder_id convention still light up their developed render on
        export.

    Extensions are matched case-insensitively so exports still pick up
    developed files written with uppercase extensions — e.g. IMG_0001.JPG
    — which can happen on case-sensitive filesystems when
    darktable_output_format is configured with uppercase, or for files
    placed manually. Stems are matched case-sensitively so that two photos
    whose filenames differ only by case (e.g. Bird1.CR3 and bird1.CR3 in
    the same folder on a case-sensitive filesystem) resolve to distinct
    developed files.

    JPG is preferred over TIFF when both exist.

    Pass `index` (a _DevelopedDirIndex) to amortize directory scans
    across many photos in the same export.
    """
    stem = os.path.splitext(filename)[0]
    candidates = []
    if developed_dir and folder_id is not None:
        candidates.append(os.path.join(developed_dir, str(folder_id)))
    if folder_path:
        candidates.append(os.path.join(folder_path, "developed"))
    if developed_dir:
        candidates.append(developed_dir)
    if index is None:
        index = _DevelopedDirIndex()
    for base in candidates:
        hit = index.lookup(base, stem)
        if hit:
            return hit
    return None


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
