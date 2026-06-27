"""Photo export with resize, quality control, and template-based naming."""

import contextlib
import hashlib
import json
import logging
import os
import re
import shutil

from image_edits import apply_recipe_to_loaded_image
from image_loader import RAW_DECODE_PRESERVE_HIGHLIGHTS, RAW_EXTENSIONS, load_image

log = logging.getLogger(__name__)

# Characters not allowed in filenames (covers Windows + macOS + Linux)
_UNSAFE_RE = re.compile(r'[<>:"/|?*\\]')
_EXIF_ORIENTATION_TAG = 274
_OUTPUT_FORMATS = {
    "jpg": {"extension": "jpg", "pil_format": "JPEG", "quality": True},
    "jpeg": {"extension": "jpg", "pil_format": "JPEG", "quality": True},
    "png": {"extension": "png", "pil_format": "PNG", "quality": False},
    "tif": {"extension": "tiff", "pil_format": "TIFF", "quality": False},
    "tiff": {"extension": "tiff", "pil_format": "TIFF", "quality": False},
}


def sanitize_filename(name):
    """Replace filesystem-unsafe characters with underscores."""
    return _UNSAFE_RE.sub("_", name)


def normalize_output_format(output_format):
    """Return export format metadata for a user/API format value."""
    fmt = str(output_format or "jpg").strip().lower()
    if fmt not in _OUTPUT_FORMATS:
        supported = ", ".join(sorted({"jpg", "png", "tiff"}))
        raise ValueError(f"format must be one of: {supported}")
    return _OUTPUT_FORMATS[fmt]


def normalize_quality(quality, default=92):
    """Return an integer JPEG quality in Pillow's accepted 1-100 range."""
    if quality in (None, ""):
        quality = default
    if isinstance(quality, bool):
        raise ValueError("quality must be an integer from 1 to 100")
    try:
        value = int(quality)
    except (TypeError, ValueError) as exc:
        raise ValueError("quality must be an integer from 1 to 100") from exc
    if value < 1 or value > 100:
        raise ValueError("quality must be an integer from 1 to 100")
    return value


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
            format: str -- output format: jpg, png, or tiff (default jpg)
            quality: int 1-100 (default 92)
            working_copy_max_size: int -- the cap used when generating
                working copies (default 4096); used to decide whether
                the working copy can satisfy the requested max_size.
            developed_dir: str -- optional path to darktable-developed
                outputs (mirrors darktable_output_dir config). Export
                prefers a developed JPG/TIFF at
                <developed_dir>/<path_key>/<stem>.<ext> (or at the
                default <folder>/developed/<stem>.<ext>) over
                re-decoding the RAW. `path_key` is a stable hash of the
                source folder's path (see `developed_folder_key`), so the
                per-folder nesting matches the develop job's write
                convention, keeps lookups one-to-one when two source
                folders share a basename, and survives SQLite row-id
                reuse after folder deletion. As a legacy fallback,
                <developed_dir>/<stem>.<ext> is also probed so libraries
                developed before the per-folder nesting convention was
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
    format_info = normalize_output_format(
        options.get("format", options.get("output_format", "jpg"))
    )
    output_ext = format_info["extension"]
    quality = normalize_quality(options.get("quality", 92))
    try:
        wc_max = int(options.get("working_copy_max_size", 4096))
    except (ValueError, TypeError):
        wc_max = 4096
    developed_dir = options.get("developed_dir") or ""

    os.makedirs(destination, exist_ok=True)

    photos_map = db.get_photos_by_ids(photo_ids)
    folders = {f["id"]: f["path"] for f in db.get_folder_tree()}
    exif_data_map = _get_photo_exif_data(db, photo_ids)

    # Get species keywords for all photos in one query
    species_map = db.get_species_keywords_for_photos(photo_ids)
    edit_recipes = db.get_photo_edit_recipes(photo_ids)

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
        recipe = edit_recipes.get(pid)
        exif_data = exif_data_map.get(pid)
        source_path = None
        for dev_candidate in _iter_developed_outputs(
            photo["filename"],
            folder_path,
            developed_dir,
            developed_index,
            preferred_exts=_developed_ext_preference(output_ext),
        ):
            # Guard against silent downscaling: darktable's develop job
            # can write the output at --width=N, so a developed file may
            # be smaller than the original. Keep trying lower-preference
            # developed candidates before falling through to the working
            # copy / original source.
            if _developed_can_satisfy_size(
                dev_candidate, photo, max_size, recipe, exif_data=exif_data
            ):
                source_path = dev_candidate
                break
        if not source_path:
            use_wc = _working_copy_can_satisfy_export(
                photo, recipe, max_size, wc_max, vireo_dir, exif_data=exif_data
            )
            source_path = None
            if not use_wc:
                source_path = _companion_can_satisfy_export(
                    photo, folder_path, recipe, max_size, exif_data=exif_data
                )
            if not source_path:
                source_path = _resolve_source(
                    photo, vireo_dir, folders, use_working_copy=use_wc,
                )
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
        out_path = os.path.join(destination, rel_path_safe + f".{output_ext}")
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
            load_max_size = (
                None if recipe and recipe.get("crop") else (max_size or None)
            )
            source_is_raw = (
                os.path.splitext(source_path)[1].lower() in RAW_EXTENSIONS
            )
            raw_decode = (
                RAW_DECODE_PRESERVE_HIGHLIGHTS if recipe and source_is_raw else None
            )
            load_kwargs = {"raw_decode": raw_decode} if raw_decode else {}
            img = load_image(source_path, max_size=load_max_size, **load_kwargs)
            if source_is_raw:
                # RAW decode either failed outright (`img is None`) or
                # silently fell back to the embedded JPEG. ``_load_raw``
                # returns ``raw.extract_thumb()`` when libraw cannot
                # demosaic the RAW; that preview can be much smaller than
                # the full-size companion JPEG, so the export would
                # quietly produce undersized bytes for unsupported RAW+JPEG
                # files. Compare *both* loaded dimensions against the
                # source's expected dimensions (capped by
                # ``load_max_size`` when set) — a long-edge-only check
                # accepts e.g. 6000x3376 embedded previews for 6000x4000
                # photos, dropping short-edge content.
                needs_companion = img is None
                expected_w, expected_h = 0, 0
                if img is not None:
                    orig_w, orig_h = _recipe_source_dimensions(photo, exif_data)
                    expected_w, expected_h = orig_w, orig_h
                    if (
                        load_max_size
                        and expected_w > 0
                        and expected_h > 0
                    ):
                        long_edge = max(expected_w, expected_h)
                        if long_edge > load_max_size:
                            scale = load_max_size / long_edge
                            expected_w = round(expected_w * scale)
                            expected_h = round(expected_h * scale)
                    # Allow a 1px slack for rounding between RAW decoder
                    # output and stored dimensions.
                    if (
                        expected_w > 0
                        and expected_h > 0
                        and (
                            img.size[0] + 1 < expected_w
                            or img.size[1] + 1 < expected_h
                        )
                    ):
                        needs_companion = True
                if needs_companion:
                    companion_fallback = _companion_can_satisfy_export(
                        photo, folder_path, recipe, max_size,
                        exif_data=exif_data, skip_raw_primary=False,
                    )
                    if companion_fallback:
                        companion_img = load_image(
                            companion_fallback, max_size=load_max_size,
                        )
                        if companion_img is not None and (
                            img is None
                            or max(companion_img.size) > max(img.size)
                        ):
                            if img is None:
                                log.info(
                                    "RAW decode failed for %s; falling back "
                                    "to companion JPEG",
                                    photo["filename"],
                                )
                            else:
                                log.info(
                                    "RAW decode fell back to undersized "
                                    "embedded JPEG (%dx%d, expected %dx%d) "
                                    "for %s; using companion JPEG instead",
                                    img.size[0], img.size[1],
                                    expected_w, expected_h,
                                    photo["filename"],
                                )
                                img.close()
                            img = companion_img
                        elif companion_img is not None:
                            companion_img.close()
            if img is None:
                errors.append(f"{photo['filename']}: failed to load image")
                if progress_cb:
                    progress_cb(i + 1, len(photo_ids), photo["filename"])
                continue
            if recipe:
                img = apply_recipe_to_loaded_image(img, recipe, max_size=max_size)
            _save_export_image(img, out_path, format_info, quality)
            img.close()
            exported += 1
        except Exception as exc:
            log.warning("Export failed for %s: %s", photo["filename"], exc)
            errors.append(f"{photo['filename']}: {exc}")

        if progress_cb:
            progress_cb(i + 1, len(photo_ids), photo["filename"])

    return {"exported": exported, "errors": errors, "destination": destination}


def _save_export_image(img, out_path, format_info, quality):
    """Save a rendered export image in the requested output format."""
    pil_format = format_info["pil_format"]
    save_img = img
    if pil_format == "JPEG" and img.mode not in ("RGB", "L"):
        save_img = img.convert("RGB")
    save_kwargs = {}
    if format_info["quality"]:
        save_kwargs["quality"] = quality
    elif pil_format == "TIFF":
        save_kwargs["compression"] = "tiff_lzw"
    try:
        save_img.save(out_path, pil_format, **save_kwargs)
    finally:
        if save_img is not img:
            save_img.close()


_PREFERRED_DEVELOPED_EXTS = ("jpg", "jpeg", "tiff", "tif")
_TIFF_FIRST_DEVELOPED_EXTS = ("tiff", "tif", "jpg", "jpeg")


def _developed_ext_preference(output_ext):
    """Return source developed-output preference for the requested export type."""
    if output_ext != "jpg":
        return _TIFF_FIRST_DEVELOPED_EXTS
    return _PREFERRED_DEVELOPED_EXTS


def _get_photo_exif_data(db, photo_ids):
    """Return a photo_id -> exif_data map without bloating list photo queries."""
    if not photo_ids or not hasattr(db, "conn"):
        return {}
    out = {}
    for i in range(0, len(photo_ids), 999):
        chunk = photo_ids[i:i + 999]
        placeholders = ",".join("?" for _ in chunk)
        rows = db.conn.execute(
            f"SELECT id, exif_data FROM photos WHERE id IN ({placeholders})",
            list(chunk),
        ).fetchall()
        for row in rows:
            out[row["id"]] = row["exif_data"]
    return out


def _recipe_source_dimensions(photo, exif_data=None):
    """Return original dimensions as load_image sees them after EXIF transpose."""
    try:
        width = int(photo["width"] or 0)
        height = int(photo["height"] or 0)
    except (KeyError, IndexError, TypeError, ValueError):
        return 0, 0
    if width > 0 and height > 0 and _orientation_swaps_axes(_exif_orientation(exif_data)):
        return height, width
    return width, height


def _exif_orientation(exif_data):
    if not exif_data:
        return None
    if isinstance(exif_data, str):
        try:
            metadata = json.loads(exif_data)
        except (TypeError, ValueError):
            return None
    elif isinstance(exif_data, dict):
        metadata = exif_data
    else:
        return None
    if not isinstance(metadata, dict):
        return None
    for group in ("EXIF", "IFD0", "TIFF", "File"):
        values = metadata.get(group)
        if isinstance(values, dict) and "Orientation" in values:
            return values["Orientation"]
    return metadata.get("Orientation")


def _orientation_swaps_axes(orientation):
    if orientation is None or isinstance(orientation, bool):
        return False
    if isinstance(orientation, int | float):
        return int(orientation) in (5, 6, 7, 8)
    text = str(orientation).strip().lower()
    if not text:
        return False
    try:
        return int(text) in (5, 6, 7, 8)
    except ValueError:
        return "90" in text or "270" in text


def _image_size_after_exif_orientation(img):
    width, height = img.size
    orientation = None
    with contextlib.suppress(Exception):
        orientation = img.getexif().get(_EXIF_ORIENTATION_TAG)
    if _orientation_swaps_axes(orientation):
        return height, width
    return width, height


def _recipe_result_long_edge(width, height, recipe):
    """Return the rendered long edge after right-angle rotation and crop."""
    rotation = (recipe or {}).get("rotation", 0)
    if rotation in (90, 270):
        width, height = height, width
    crop = (recipe or {}).get("crop") if recipe else None
    if crop:
        return max(float(crop["w"]) * width, float(crop["h"]) * height)
    return max(width, height)


def _developed_can_satisfy_size(dev_path, photo, max_size, recipe=None, exif_data=None):
    """Return True if the developed file is large enough for this export.

    The develop job may have written a downscaled output (`--width` is
    honored by darktable-cli), so preferring it unconditionally would
    silently ship a smaller image than the user asked for. This guard
    compares the developed file's long edge against:

      * the requested `max_size` when resize is in effect, or
      * the original photo's stored dimensions when a full-resolution
        export is requested.

    If we can't determine the required size (no max_size and no stored
    dimensions on the photo row), fall back to preferring the developed
    output so the primary "ship the perfected render" feature keeps
    working for libraries scanned before the dimension columns were
    populated.
    """
    from PIL import Image

    try:
        with Image.open(dev_path) as img:
            dev_w, dev_h = _image_size_after_exif_orientation(img)
    except Exception:
        return True
    dev_long = _recipe_result_long_edge(dev_w, dev_h, recipe)
    original_w, original_h = _recipe_source_dimensions(photo, exif_data)
    if original_w and original_h:
        required_long = _recipe_result_long_edge(original_w, original_h, recipe)
        if max_size is not None:
            required_long = min(max_size, required_long)
        return dev_long >= required_long
    if max_size is not None:
        return dev_long >= max_size
    return True


def developed_folder_key(folder_path):
    """Return a stable filesystem-safe key for the given source folder path.

    Derived from the folder's canonical path rather than its SQLite row id
    so the key survives folder churn. `folders.id` is an INTEGER PRIMARY
    KEY, which SQLite happily reuses after a row is deleted, and
    `delete_folder` does not clean the external developed directory. Using
    the row id as an on-disk key therefore risked a freshly-added folder
    silently inheriting stale developed files left on disk by a deleted
    folder whose id it reused. Hashing the path sidesteps that entirely:
    distinct paths always get distinct keys, and a re-scan of the same
    path resolves to the same key (so its existing developed outputs are
    correctly picked up again).

    Because the key is derived from the *current* path, any operation
    that rewrites a folder's path (e.g. `/api/jobs/move-folder`) must
    also rebase the corresponding developed subdirectory on disk — see
    `relocate_developed_dir` for the rebase helper used by move.
    """
    if not folder_path:
        return ""
    return hashlib.sha1(folder_path.encode("utf-8")).hexdigest()[:16]


def relocate_developed_dir(developed_dir, old_folder_path, new_folder_path):
    """Rebase a folder's developed-output subdir after its path changes.

    The configured `darktable_output_dir` layout is flat, so each source
    folder is nested under `developed_folder_key(folder_path)`. That key
    is path-derived for safety against SQLite row-id reuse, but it means
    a folder move (which rewrites `folders.path`) orphans the old
    subdirectory. Without this rebase, export would silently fall back
    to re-decoding the RAW for every previously-developed photo in the
    moved folder.

    Returns True if a directory was renamed, False otherwise (no
    developed_dir configured, nothing to move, or the target already
    exists — in the last case the caller can decide what to do). Never
    raises; failures are logged and treated as a no-op so a filesystem
    hiccup here doesn't also fail the folder move.
    """
    if not developed_dir or not old_folder_path or not new_folder_path:
        return False
    if old_folder_path == new_folder_path:
        return False
    old_key = developed_folder_key(old_folder_path)
    new_key = developed_folder_key(new_folder_path)
    if not old_key or not new_key or old_key == new_key:
        return False
    old_subdir = os.path.join(developed_dir, old_key)
    new_subdir = os.path.join(developed_dir, new_key)
    if not os.path.isdir(old_subdir):
        return False
    if os.path.exists(new_subdir):
        # Target already exists — this is the merge case (e.g.
        # `/api/folders/<id>/relocate` routing through
        # `db._merge_into_existing`). Move individual files into the
        # target so reassigned photos still resolve to their developed
        # render instead of being stranded under the old key. On
        # filename collision the target wins, matching the DB merge's
        # drop-source-on-collision policy.
        try:
            for name in os.listdir(old_subdir):
                src_file = os.path.join(old_subdir, name)
                dst_file = os.path.join(new_subdir, name)
                if os.path.exists(dst_file):
                    if os.path.isdir(src_file) and not os.path.islink(src_file):
                        shutil.rmtree(src_file)
                    else:
                        os.remove(src_file)
                else:
                    os.rename(src_file, dst_file)
            os.rmdir(old_subdir)
            return True
        except OSError as exc:
            log.warning(
                "Failed to merge developed dir %s into %s: %s",
                old_subdir, new_subdir, exc,
            )
            return False
    try:
        os.rename(old_subdir, new_subdir)
        return True
    except OSError as exc:
        log.warning(
            "Failed to relocate developed dir %s -> %s: %s",
            old_subdir, new_subdir, exc,
        )
        return False


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

    def _entries_for_base(self, base):
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
            #
            # When two files in the same directory share a stem and
            # differ only by extension case (e.g. bird1.jpg vs bird1.JPG
            # on a case-sensitive filesystem), prefer the file whose
            # extension is already the canonical lowercase form — that's
            # what the develop job writes when `darktable_output_format`
            # is left at its default — and break any remaining ties by
            # iterating sorted(names) so the winner is stable across
            # runs rather than depending on os.listdir order.
            for name in sorted(names):
                ent_stem, ent_ext = os.path.splitext(name)
                raw_ext = ent_ext[1:] if ent_ext.startswith(".") else ent_ext
                ext_key = raw_ext.lower()
                key = (ent_stem, ext_key)
                existing = entries.get(key)
                if existing is None:
                    entries[key] = os.path.join(base, name)
                    continue
                existing_ext = os.path.splitext(existing)[1][1:]
                if raw_ext == ext_key and existing_ext != ext_key:
                    entries[key] = os.path.join(base, name)
            self._cache[base] = entries
        return entries

    def iter_matches(self, base, stem, preferred_exts=None):
        entries = self._entries_for_base(base)
        for ext in preferred_exts or _PREFERRED_DEVELOPED_EXTS:
            path = entries.get((stem, ext))
            if path and os.path.isfile(path):
                yield path

    def lookup(self, base, stem, preferred_exts=None):
        for path in self.iter_matches(base, stem, preferred_exts=preferred_exts):
            return path
        return None


def _iter_developed_outputs(
    filename, folder_path, developed_dir, index=None, preferred_exts=None,
):
    """Yield darktable-developed outputs for this photo in preference order.

    Lookup locations are probed in order:

      * <developed_dir>/<path_key>/<stem>.<ext> — matches how the develop
        job writes when darktable_output_dir is configured (the flat
        output dir is nested per source-folder so basename collisions
        stay one-to-one). `path_key` is derived from the folder path
        rather than its SQLite row id, so the on-disk key survives row
        deletion without risking a reused id silently inheriting stale
        outputs — see `developed_folder_key`.
      * <folder_path>/developed/<stem>.<ext> — the default develop-job
        location, naturally disambiguated because each source folder has
        its own developed/ subdir.
      * <developed_dir>/<stem>.<ext> — legacy flat layout used by older
        versions of the develop job. Probed last so that any new
        folder-scoped output wins, but kept so libraries developed before
        the per-folder nesting convention still light up their developed
        render on export.

    Extensions are matched case-insensitively so exports still pick up
    developed files written with uppercase extensions — e.g. IMG_0001.JPG
    — which can happen on case-sensitive filesystems when
    darktable_output_format is configured with uppercase, or for files
    placed manually. Stems are matched case-sensitively so that two photos
    whose filenames differ only by case (e.g. Bird1.CR3 and bird1.CR3 in
    the same folder on a case-sensitive filesystem) resolve to distinct
    developed files.

    JPG is preferred over TIFF when both exist unless the caller passes a
    TIFF-first preference for TIFF exports.

    Pass `index` (a _DevelopedDirIndex) to amortize directory scans
    across many photos in the same export.
    """
    stem = os.path.splitext(filename)[0]
    candidates = []
    if developed_dir and folder_path:
        candidates.append(os.path.join(developed_dir, developed_folder_key(folder_path)))
    if folder_path:
        candidates.append(os.path.join(folder_path, "developed"))
    if developed_dir:
        candidates.append(developed_dir)
    if index is None:
        index = _DevelopedDirIndex()
    for base in candidates:
        yield from index.iter_matches(base, stem, preferred_exts=preferred_exts)


def _find_developed_output(
    filename, folder_path, developed_dir, index=None, preferred_exts=None,
):
    """Return the first darktable-developed output for this photo, or None."""
    for path in _iter_developed_outputs(
        filename, folder_path, developed_dir, index, preferred_exts=preferred_exts,
    ):
        return path
    return None


def _working_copy_can_satisfy_export(
    photo, recipe, max_size, wc_max, vireo_dir, exif_data=None
):
    """Return True when the working copy can preserve requested export pixels."""
    if not max_size or max_size <= 0:
        return False
    if max_size > wc_max:
        return False
    # For RAW primaries with an edit recipe, the working copy is unreliable:
    # libraries built before the highlight-preserving RAW decode landed
    # carry working copies derived from clipped sources (camera JPEG or the
    # JPEG-first RAW path), and EDIT_MATH_VERSION purges previews/thumbnails
    # but not working copies. Reusing such a copy would silently apply the
    # recipe to clipped bytes. Force the export path back to the RAW so the
    # later load_image() call gets RAW_DECODE_PRESERVE_HIGHLIGHTS.
    if recipe and os.path.splitext(photo["filename"])[1].lower() in RAW_EXTENSIONS:
        return False
    wc_rel = photo["working_copy_path"]
    if not wc_rel:
        return False
    wc_path = os.path.join(vireo_dir, wc_rel)
    if not os.path.exists(wc_path):
        return False
    try:
        from PIL import Image
        with Image.open(wc_path) as wc_img:
            wc_w, wc_h = wc_img.size
    except Exception:
        return False

    wc_render_long = _recipe_result_long_edge(wc_w, wc_h, recipe)
    crop = (recipe or {}).get("crop") if recipe else None

    width, height = _recipe_source_dimensions(photo, exif_data)
    if not crop:
        if width > 0 and height > 0:
            required_long = min(max_size, max(width, height))
        else:
            required_long = max_size
        return wc_render_long >= required_long

    if width <= 0 or height <= 0:
        # Missing dimensions: prefer the original over silently exporting a
        # cropped derivative from an undersized working copy.
        return False

    original_render_long = _recipe_result_long_edge(width, height, recipe)
    if original_render_long <= 0:
        return False
    required_long = min(max_size, original_render_long)
    return wc_render_long >= required_long


def _companion_can_satisfy_export(
    photo, folder_path, recipe, max_size, exif_data=None,
    *, skip_raw_primary=True,
):
    """Return a full-resolution companion path when it can satisfy edited export.

    By default RAW primaries are skipped so the export decodes the RAW with
    ``RAW_DECODE_PRESERVE_HIGHLIGHTS`` instead of the camera JPEG (whose
    highlights are already clipped). Pass ``skip_raw_primary=False`` to get
    the companion path as a fallback when the RAW decode itself fails — a
    rendered camera JPEG is still better than a failed export.
    """
    if not recipe:
        return None
    if (
        skip_raw_primary
        and os.path.splitext(photo["filename"])[1].lower() in RAW_EXTENSIONS
    ):
        return None
    companion_rel = photo["companion_path"]
    if not companion_rel or not folder_path:
        return None
    companion = os.path.join(folder_path, companion_rel)
    if not os.path.isfile(companion):
        return None
    try:
        from PIL import Image
        with Image.open(companion) as img:
            comp_w, comp_h = _image_size_after_exif_orientation(img)
    except Exception:
        return None

    original_w, original_h = _recipe_source_dimensions(photo, exif_data)
    if original_w <= 0 or original_h <= 0:
        return None
    required_long = _recipe_result_long_edge(original_w, original_h, recipe)
    if max_size is not None:
        required_long = min(max_size, required_long)
    if _recipe_result_long_edge(comp_w, comp_h, recipe) >= required_long:
        return companion
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
