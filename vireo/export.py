"""Photo export with resize, quality control, and template-based naming."""

import contextlib
import hashlib
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time

from image_edits import apply_recipe_to_loaded_image
from image_loader import RAW_DECODE_LINEAR, RAW_EXTENSIONS, load_image
from proc import no_window_kwargs
from render_source import (
    companion_image_can_replace_raw_result,
    image_is_smaller_than_expected,
    rendered_recipe_long_edge,
    scaled_recipe_source_dimensions,
)
from render_source import (
    image_size_after_exif_orientation as _image_size_after_exif_orientation,
)
from render_source import (
    recipe_source_dimensions as _recipe_source_dimensions,
)
from working_copy_cache import working_copy_publication_guard

log = logging.getLogger(__name__)

# Characters not allowed in filenames (covers Windows + macOS + Linux)
_UNSAFE_RE = re.compile(r'[<>:"/|?*\\]')
_WINDOWS_RESERVED_SUBFOLDER_RE = re.compile(
    r"^(?:con|prn|aux|nul|conin\$|conout\$|com[1-9¹²³]|lpt[1-9¹²³])(?:\..*)?$",
    re.IGNORECASE,
)
_OUTPUT_FORMATS = {
    "jpg": {"extension": "jpg", "pil_format": "JPEG", "quality": True},
    "jpeg": {"extension": "jpg", "pil_format": "JPEG", "quality": True},
    "png": {"extension": "png", "pil_format": "PNG", "quality": False},
    "tif": {"extension": "tiff", "pil_format": "TIFF", "quality": False},
    "tiff": {"extension": "tiff", "pil_format": "TIFF", "quality": False},
}
EXPORT_METADATA_FIELDS = frozenset({
    "species",
    "capture_date",
    "capture_time",
    "rating",
    "location",
    "camera",
})


class ExportPreflightError(RuntimeError):
    """Raised when collision behavior cannot be verified safely."""


def reveal_exported_files(paths):
    """Show successful exports in the platform's file manager.

    A single export is selected when the platform supports it. For a batch,
    open each resolved output directory so the user is not sent to an image
    viewer and exports beside originals remain discoverable across folders.
    Returns true when every file-manager command was dispatched successfully.
    """
    existing_paths = [
        os.path.abspath(path)
        for path in paths or []
        if path and os.path.isfile(path)
    ]
    if not existing_paths:
        return False

    if len(existing_paths) == 1:
        targets = [(existing_paths[0], False)]
    else:
        candidate_dirs = [os.path.dirname(path) for path in existing_paths]
        # Preserve export order while avoiding duplicate file-manager windows.
        targets = [(path, True) for path in dict.fromkeys(candidate_dirs)]

    all_revealed = True
    for target, is_directory in targets:
        try:
            if sys.platform == "darwin":
                command = (
                    ["open", "--", target]
                    if is_directory
                    else ["open", "-R", "--", target]
                )
            elif sys.platform.startswith("win"):
                command = (
                    ["explorer", target]
                    if is_directory
                    else ["explorer", f"/select,{target}"]
                )
            else:
                folder = target if is_directory else os.path.dirname(target)
                command = ["xdg-open", os.path.abspath(folder)]
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
                **no_window_kwargs(),
            )
            # Explorer commonly returns 1 even after opening successfully.
            if not sys.platform.startswith("win") and result.returncode != 0:
                all_revealed = False
        except (OSError, subprocess.TimeoutExpired):
            all_revealed = False
    return all_revealed


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


def normalize_metadata_fields(fields):
    """Validate and deduplicate requested export metadata fields."""
    if fields in (None, ""):
        return []
    if not isinstance(fields, (list, tuple, set)):
        raise ValueError("metadata_fields must be a list")
    normalized = []
    for field in fields:
        if not isinstance(field, str) or field not in EXPORT_METADATA_FIELDS:
            supported = ", ".join(sorted(EXPORT_METADATA_FIELDS))
            raise ValueError(f"metadata_fields entries must be one of: {supported}")
        if field not in normalized:
            normalized.append(field)
    return normalized


DEFAULT_EXPORT_SUBFOLDER = "exported"

MAX_EXPORT_SUBFOLDER_LEN = 120
# Cross-platform component byte cap. ext4/HFS+/APFS all limit a path
# component to 255 bytes, so multi-byte names (CJK, emoji) that pass the
# character count above can still be uncreatable. Leave a small margin
# below 255 so a following extension doesn't push a rendered file name
# over on the odd filesystem that measures per-component encoded lengths.
MAX_EXPORT_SUBFOLDER_BYTES = 240


def normalize_subfolder_name(name):
    """Validate a user-chosen export subfolder name (one path component).

    ``None`` and blank strings fall back to the historical "exported"
    default so requests that never send the field keep their behavior.
    """
    if name is None:
        return DEFAULT_EXPORT_SUBFOLDER
    if not isinstance(name, str):
        raise ValueError("subfolder_name must be a string")
    name = name.strip()
    if not name:
        return DEFAULT_EXPORT_SUBFOLDER
    if len(name) > MAX_EXPORT_SUBFOLDER_LEN:
        raise ValueError(
            f"subfolder_name must be {MAX_EXPORT_SUBFOLDER_LEN} characters or fewer"
        )
    if len(name.encode("utf-8")) > MAX_EXPORT_SUBFOLDER_BYTES:
        raise ValueError(
            f"subfolder_name must be {MAX_EXPORT_SUBFOLDER_BYTES} bytes or fewer when encoded"
        )
    if name in (".", ".."):
        raise ValueError("subfolder_name must not be '.' or '..'")
    if name.endswith("."):
        raise ValueError("subfolder_name must not end with a period")
    if _WINDOWS_RESERVED_SUBFOLDER_RE.fullmatch(name):
        raise ValueError("subfolder_name must not be a reserved Windows device name")
    if _UNSAFE_RE.search(name) or any(ord(ch) < 32 for ch in name):
        raise ValueError(
            'subfolder_name must not contain path separators or < > : " | ? *'
        )
    return name


def normalize_max_size(max_size):
    """Validate the optional max long-edge resize; None disables resizing."""
    if max_size in (None, "", 0):
        return None
    if isinstance(max_size, bool):
        raise ValueError("max_size must be a positive integer")
    try:
        value = int(max_size)
    except (TypeError, ValueError) as exc:
        raise ValueError("max_size must be a positive integer") from exc
    if value < 1 or value > 50000:
        raise ValueError("max_size must be between 1 and 50000")
    return value


MAX_EXPORT_PRESET_NAME_LEN = 80

EXPORT_PRESET_SETTING_KEYS = frozenset({
    "destination",
    "export_to_subfolder",
    "subfolder_name",
    "reveal_after_export",
    "format",
    "max_size",
    "quality",
    "naming_template",
    "metadata_fields",
})


def normalize_export_preset_name(name):
    """Validate a saved-export-preset display name."""
    if not isinstance(name, str) or not name.strip():
        raise ValueError("preset name is required")
    name = name.strip()
    if len(name) > MAX_EXPORT_PRESET_NAME_LEN:
        raise ValueError(
            f"preset name must be {MAX_EXPORT_PRESET_NAME_LEN} characters or fewer"
        )
    # Names travel in the DELETE route path, so keep them URL-path safe.
    # Reject "." and ".." explicitly: encodeURIComponent leaves them
    # unchanged and browser URL resolution collapses the segment, so the
    # UI cannot address such a preset for deletion.
    if name in (".", ".."):
        raise ValueError("preset name must not be '.' or '..'")
    if "/" in name or "\\" in name or any(ord(ch) < 32 for ch in name):
        raise ValueError("preset name must not contain slashes or control characters")
    return name


def normalize_export_preset_settings(settings):
    """Validate and normalize a saved export preset's settings dict.

    Presets snapshot the full export dialog, so every field the
    /api/jobs/export endpoint accepts is validated here with the same
    rules, and unknown keys are rejected rather than silently stored.
    """
    if not isinstance(settings, dict):
        raise ValueError("preset settings must be an object")
    unknown = sorted(set(settings) - EXPORT_PRESET_SETTING_KEYS)
    if unknown:
        raise ValueError(f"unknown preset settings: {', '.join(unknown)}")

    destination = settings.get("destination", "")
    if not isinstance(destination, str):
        raise ValueError("destination must be a string")
    destination = destination.strip()
    if destination and not os.path.isabs(destination):
        raise ValueError("destination must be an absolute path")

    export_to_subfolder = settings.get("export_to_subfolder", False)
    if not isinstance(export_to_subfolder, bool):
        raise ValueError("export_to_subfolder must be a boolean")
    reveal_after_export = settings.get("reveal_after_export", False)
    if not isinstance(reveal_after_export, bool):
        raise ValueError("reveal_after_export must be a boolean")

    naming_template = settings.get("naming_template", "{original}")
    if not isinstance(naming_template, str):
        raise ValueError("naming_template must be a string")
    naming_template = naming_template.strip() or "{original}"

    raw_subfolder_name = settings.get("subfolder_name")
    if export_to_subfolder:
        subfolder_name = normalize_subfolder_name(raw_subfolder_name)
    else:
        # A disabled subfolder value is inert at export time. Preserve a
        # valid dormant name for later toggling, but normalize invalid stale
        # UI/config values to the safe default instead of rejecting the
        # whole preset.
        try:
            subfolder_name = normalize_subfolder_name(raw_subfolder_name)
        except ValueError:
            subfolder_name = DEFAULT_EXPORT_SUBFOLDER

    return {
        "destination": destination,
        "export_to_subfolder": export_to_subfolder,
        "subfolder_name": subfolder_name,
        "reveal_after_export": reveal_after_export,
        "format": normalize_output_format(settings.get("format", "jpg"))["extension"],
        "max_size": normalize_max_size(settings.get("max_size")),
        "quality": normalize_quality(settings.get("quality", 92)),
        "naming_template": naming_template,
        "metadata_fields": normalize_metadata_fields(settings.get("metadata_fields")),
    }


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


def load_export_image(photo, vireo_dir, folders, *, recipe=None, exif_data=None,
                      max_size=None, wc_max=4096, developed_dir="",
                      developed_index=None, output_ext="jpg"):
    """Load the final export pixels, including edits and source fallbacks.

    The caller owns the returned image and must close it. Both photo exports
    and website publishing use this renderer so working-copy sizing, RAW
    fallback, local masks, and developed-output selection stay consistent.
    """
    pid = photo["id"]
    # A TIFF requested from a RAW should retain sensor precision even when no
    # edits are saved. The sentinel also prevents selecting a JPEG working copy
    # solely because it has enough pixels for a resized TIFF export.
    if (
        not recipe and output_ext in ("tif", "tiff")
        and os.path.splitext(photo["filename"])[1].lower() in RAW_EXTENSIONS
    ):
        recipe = {"version": 1}
    if developed_index is None:
        developed_index = _DevelopedDirIndex()
    # Resolve source path.  Precedence:
    #   1. darktable-developed output ("perfected" rendering) — takes
    #      priority over RAW so Export ships what the user sees after
    #      Develop, not a fresh libraw decode of the RAW.
    #   2. working copy when resizing to a size it can satisfy.
    #   3. original file (default; also used for full-res exports).
    folder_path = folders.get(photo["folder_id"], "")
    load_max_size = (
        None if recipe and recipe.get("crop") else (max_size or None)
    )

    def _load_selected_source(
        path, *, _recipe=recipe, _load_max_size=load_max_size,
    ):
        is_raw = os.path.splitext(path)[1].lower() in RAW_EXTENSIONS
        raw_decode = (
            RAW_DECODE_LINEAR if _recipe and is_raw else None
        )
        load_kwargs = {"raw_decode": raw_decode} if raw_decode else {}
        return (
            load_image(path, max_size=_load_max_size, **load_kwargs),
            is_raw,
        )

    original_abs = os.path.join(folder_path, photo["filename"])
    wc_rel = photo["working_copy_path"]
    wc_abs = os.path.join(vireo_dir, wc_rel) if wc_rel else None
    pin_offline_working_copy = bool(
        wc_abs and not os.path.isfile(original_abs)
    )
    source_guard = (
        working_copy_publication_guard()
        if pin_offline_working_copy else contextlib.nullcontext()
    )
    pinned_img = None
    pinned_source_is_raw = False
    pinned_decode_attempted = False
    with source_guard:
        source_path = _select_export_source(
            photo=photo,
            folder_path=folder_path,
            folders=folders,
            recipe=recipe,
            max_size=max_size,
            wc_max=wc_max,
            vireo_dir=vireo_dir,
            developed_dir=developed_dir,
            developed_index=developed_index,
            output_ext=output_ext,
            exif_data=exif_data,
        )
        if source_path and not os.path.isfile(source_path):
            # A working copy can be evicted between _select_export_source
            # returning and this pre-loop existence check (concurrent
            # scanner/on-demand write, a config-driven quota shrink).
            # Fall back to the primary source so a photo whose original
            # is on disk is never dropped as "source file missing" merely
            # because its resized-export shortcut vanished mid-flight.
            fallback = _fallback_source_after_wc_eviction(
                source_path, photo, folder_path, vireo_dir,
            )
            if fallback:
                log.info(
                    "Working copy %s evicted before export of %s; using "
                    "primary source %s",
                    source_path, photo["filename"], fallback,
                )
                source_path = fallback
        if (
            pin_offline_working_copy
            and source_path
            and os.path.abspath(source_path) == os.path.abspath(wc_abs)
            and os.path.isfile(source_path)
        ):
            # The original volume is offline, so retry-to-original cannot
            # recover if quota eviction wins after source selection. Keep
            # the working copy pinned through validation and decode; the
            # resulting PIL image is independent of the path afterward.
            pinned_decode_attempted = True
            pinned_img, pinned_source_is_raw = _load_selected_source(
                source_path,
            )
    if (
        not source_path
        or (pinned_img is None and not os.path.isfile(source_path))
    ):
        raise FileNotFoundError("source file missing")

    img = None
    try:
        if pinned_decode_attempted:
            img = pinned_img
            source_is_raw = pinned_source_is_raw
        else:
            img, source_is_raw = _load_selected_source(source_path)
        if img is None:
            fallback_source = _fallback_source_after_wc_eviction(
                source_path, photo, folder_path, vireo_dir,
            )
            if fallback_source:
                log.info(
                    "Working copy for %s vanished mid-export; "
                    "falling back to original %s",
                    photo["filename"], fallback_source,
                )
                source_path = fallback_source
                img, source_is_raw = _load_selected_source(source_path)
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
                expected_w, expected_h = scaled_recipe_source_dimensions(
                    photo, load_max_size, exif_data,
                )
                if image_is_smaller_than_expected(img, expected_w, expected_h):
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
                    # Prefer companion when it covers img on both
                    # axes — a long-edge-only check misses cases
                    # like a 6000x3376 embedded preview "tying" a
                    # 6000x4000 sidecar and losing the short-edge
                    # content.
                    if companion_image_can_replace_raw_result(
                        companion_img, img, expected_w, expected_h,
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
                                "for %s; using companion JPEG (%dx%d) "
                                "instead",
                                img.size[0], img.size[1],
                                expected_w, expected_h,
                                photo["filename"],
                                companion_img.size[0],
                                companion_img.size[1],
                            )
                            img.close()
                        img = companion_img
                    elif companion_img is not None:
                        companion_img.close()
        if img is None:
            raise ValueError("failed to load image")
        if recipe:
            import local_masks
            img = apply_recipe_to_loaded_image(
                img, recipe, max_size=max_size,
                camera_metadata={
                    **dict(photo),
                    "exif_data": exif_data if exif_data is not None else _photo_value(photo, "exif_data"),
                },
                native_size=_recipe_source_dimensions(photo, exif_data),
                local_mask=local_masks.load_snapshot(
                    vireo_dir, pid, recipe,
                ),
            )
        return img
    except BaseException:
        if img is not None:
            img.close()
        raise


def export_photos(db, vireo_dir, photo_ids, destination=None, options=None,
                  progress_cb=None, cancel_check=None, cancel_only_check=None):
    """Export photos with optional resize and renaming.

    Args:
        db: Database instance
        vireo_dir: path to ~/.vireo/
        photo_ids: list of photo IDs to export
        destination: absolute path to a shared output directory. When empty,
            each photo is exported beside its original.
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
            metadata_fields: list of metadata names to embed in each rendered
                file. Supported values are species, capture_date,
                capture_time, rating, location, and camera. The default is an
                empty list, preserving the existing metadata-free export.
            export_to_subfolder: bool -- create a subfolder beneath the
                shared destination or each original photo's folder.
            subfolder_name: str -- name of that subfolder (a single path
                component, validated by ``normalize_subfolder_name``).
                Defaults to ``exported`` when omitted or blank.
            collect_files: bool -- include every exported path in the result.
                Defaults to false so large background exports keep a compact
                job result.
        progress_cb: optional callback(current, total, current_file)
        cancel_check: optional callable; stop before the next photo when true.
        cancel_only_check: non-parking cancellation probe for polling a live
            metadata subprocess. Defaults to cancel_check for existing callers.

    Returns:
        dict with the export count, errors, destination mode, and resolved
        output directories. ``destination`` remains a string for compatibility;
        when beside-original exports span multiple folders, the complete list
        is available in ``destinations``. When collect_files is true, ``files``
        contains only successful output paths and is not guaranteed to remain
        positionally aligned with photo_ids.
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
    metadata_fields = normalize_metadata_fields(options.get("metadata_fields"))
    subfolder = (
        normalize_subfolder_name(options.get("subfolder_name"))
        if options.get("export_to_subfolder") else ""
    )
    collect_files = bool(options.get("collect_files", False))

    if destination:
        os.makedirs(destination, exist_ok=True)

    photos_map = db.get_photos_by_ids(photo_ids)
    folders = {f["id"]: f["path"] for f in db.get_folder_tree()}
    exif_data_map = _get_photo_exif_data(db, photo_ids)
    camera_data_map = (
        _get_photo_camera_data(db, photo_ids) if "camera" in metadata_fields else {}
    )
    location_map = (
        db.get_effective_photo_locations(photo_ids, verify_workspace=False)
        if "location" in metadata_fields else {}
    )

    # Get species keywords for all photos in one query
    species_map = db.get_species_keywords_for_photos(photo_ids)
    edit_recipes = db.get_photo_edit_recipes(photo_ids)

    # Track sequence numbers per subdirectory
    seq_counters = {}
    exported = 0
    exported_files = [] if collect_files else None
    errors = []
    metadata_jobs = []
    resolved_destinations = []
    resolved_destination_set = set()

    # A custom destination is known even if every individual photo later
    # fails. Beside-original destinations are collected as each catalog folder
    # is resolved below because a single export can span several folders.
    if destination:
        custom_destination = (
            os.path.join(destination, subfolder) if subfolder else destination
        )
        custom_destination = os.path.normpath(custom_destination)
        resolved_destinations.append(custom_destination)
        resolved_destination_set.add(custom_destination)

    # Per-export cache of developed-directory scans. Keyed by directory
    # path; each value is the (stem, ext_lower) → absolute-path map that
    # _find_developed_output would otherwise rebuild for every photo.
    # Large exports routinely probe the same directory N times; caching
    # keeps that cost O(1) per photo after the first hit.
    developed_index = _DevelopedDirIndex()

    for i, pid in enumerate(photo_ids):
        if cancel_check and cancel_check():
            break
        photo = photos_map.get(pid)
        if not photo:
            errors.append(f"Photo {pid} not found in database")
            if progress_cb:
                progress_cb(i + 1, len(photo_ids), "")
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

        # Resolve this photo's destination. An empty shared destination means
        # "beside the original", so selections spanning folders stay beside
        # their respective originals instead of being collapsed together.
        destination_base = destination or folder_path
        if not destination_base:
            errors.append(f"{photo['filename']}: original folder unavailable")
            if progress_cb:
                progress_cb(i + 1, len(photo_ids), photo["filename"])
            continue
        if not destination and not os.path.isdir(destination_base):
            # Do not recreate an offline volume's catalog path on the local
            # filesystem merely because a working copy can still be read.
            errors.append(f"{photo['filename']}: original folder unavailable")
            if progress_cb:
                progress_cb(i + 1, len(photo_ids), photo["filename"])
            continue
        photo_destination = (
            os.path.join(destination_base, subfolder)
            if subfolder else destination_base
        )
        photo_destination = os.path.normpath(photo_destination)
        if photo_destination not in resolved_destination_set:
            resolved_destinations.append(photo_destination)
            resolved_destination_set.add(photo_destination)

        # Determine subdirectory for sequence counter
        # Render template once to extract the directory part
        subdir_key = (
            photo_destination,
            os.path.dirname(
                resolve_template(template, photo_info, species=species, seq=0)
            ),
        )
        seq_counters.setdefault(subdir_key, 0)
        seq_counters[subdir_key] += 1
        seq = seq_counters[subdir_key]

        # Resolve final output path
        rel_path = resolve_template(template, photo_info, species=species, seq=seq)
        # Guard against path traversal: strip leading slashes/dots so that
        # absolute paths and ".." segments cannot escape the destination dir.
        rel_path_safe = os.path.normpath(rel_path).lstrip(os.sep + ".")
        out_path = os.path.join(
            photo_destination, rel_path_safe + f".{output_ext}"
        )
        # Final containment check: resolved path must start with destination.
        # dest_real may already end with os.sep when destination is a root dir
        # (e.g. "/" on POSIX), so avoid doubling the separator.
        dest_real = os.path.realpath(photo_destination)
        out_real = os.path.realpath(out_path)
        dest_prefix = dest_real if dest_real.endswith(os.sep) else dest_real + os.sep
        if not out_real.startswith(dest_prefix) and out_real != dest_real:
            errors.append(f"{photo['filename']}: unsafe output path rejected")
            if progress_cb:
                progress_cb(i + 1, len(photo_ids), photo["filename"])
            continue

        # Load, resize, and save
        claimed_out_path = None
        try:
            img = load_export_image(
                photo, vireo_dir, folders, recipe=edit_recipes.get(pid),
                exif_data=exif_data_map.get(pid), max_size=max_size,
                wc_max=wc_max, developed_dir=developed_dir,
                developed_index=developed_index, output_ext=output_ext,
            )
            try:
                out_path, output_stream = _claim_export_path(out_path)
                claimed_out_path = out_path
                try:
                    _save_export_image(
                        img, output_stream, format_info, quality,
                    )
                finally:
                    output_stream.close()
            finally:
                img.close()
            if metadata_fields:
                try:
                    metadata_args = _export_metadata_args(
                        metadata_fields,
                        photo,
                        species_list,
                        camera_data_map.get(pid, {}),
                        location_map.get(pid),
                    )
                except Exception:
                    # A checked metadata option is part of the requested
                    # output contract. Do not leave behind an apparently
                    # successful but silently untagged derivative.
                    with contextlib.suppress(OSError):
                        os.unlink(out_path)
                    raise
                if metadata_args:
                    metadata_jobs.append((out_path, photo["filename"], metadata_args))
                else:
                    exported += 1
                    if exported_files is not None:
                        exported_files.append(out_path)
            else:
                exported += 1
                if exported_files is not None:
                    exported_files.append(out_path)
        except Exception as exc:
            if claimed_out_path:
                with contextlib.suppress(OSError):
                    os.unlink(claimed_out_path)
            log.warning("Export failed for %s: %s", photo["filename"], exc)
            errors.append(f"{photo['filename']}: {exc}")

        if progress_cb:
            progress_cb(i + 1, len(photo_ids), photo["filename"])

    if metadata_jobs:
        metadata_exported, metadata_errors = _write_export_metadata_batch(
            metadata_jobs, cancel_check=cancel_only_check or cancel_check,
        )
        # The subprocess has exited, so a pending pause can now park safely.
        # Keep completed outputs in the result even if cancellation arrived.
        if cancel_check:
            cancel_check()
        exported += metadata_exported
        errors.extend(metadata_errors)
        if exported_files is not None:
            exported_files.extend(
                out_path
                for out_path, _filename, _args in metadata_jobs
                if os.path.isfile(out_path)
            )

    # For the common one-directory case, make the long-standing singular field
    # useful even when the caller selected "beside originals" (whose request
    # value is intentionally empty). Multiple beside-original roots cannot be
    # represented honestly by one path, so retain the empty sentinel there and
    # expose every resolved root through ``destinations``.
    result_destination = (
        resolved_destinations[0]
        if len(resolved_destinations) == 1
        else destination
    )
    result = {
        "exported": exported,
        "errors": errors,
        "destination": result_destination,
        "destinations": resolved_destinations,
        "destination_mode": "custom" if destination else "original",
        "subfolder": subfolder,
    }
    if exported_files is not None:
        result["files"] = exported_files
    return result


def _save_export_image(img, output, format_info, quality):
    """Save a rendered export image in the requested output format."""
    pil_format = format_info["pil_format"]
    save_img = img
    if pil_format == "JPEG" and img.mode not in ("RGB", "L"):
        save_img = img.convert("RGB")
    save_kwargs = {}
    if img.info.get("icc_profile"):
        save_kwargs["icc_profile"] = img.info["icc_profile"]
    if format_info["quality"]:
        save_kwargs["quality"] = quality
    elif pil_format == "TIFF":
        save_kwargs["compression"] = "tiff_lzw"
    try:
        save_img.save(output, pil_format, **save_kwargs)
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


def _get_photo_camera_data(db, photo_ids):
    """Return promoted camera/exposure fields for an export selection."""
    if not photo_ids or not hasattr(db, "conn"):
        return {}
    out = {}
    for i in range(0, len(photo_ids), 999):
        chunk = photo_ids[i:i + 999]
        placeholders = ",".join("?" for _ in chunk)
        rows = db.conn.execute(
            f"""SELECT id, camera_make, camera_model, lens, focal_length,
                       aperture, shutter_speed, iso
                FROM photos WHERE id IN ({placeholders})""",
            list(chunk),
        ).fetchall()
        for row in rows:
            out[row["id"]] = dict(row)
    return out


def _photo_value(photo, key, default=None):
    """Read a value from either a sqlite Row or a plain mapping."""
    try:
        return photo[key]
    except (KeyError, IndexError, TypeError):
        return default


def _export_timestamp_parts(timestamp):
    """Return date/time fields for a catalog ISO timestamp."""
    value = str(timestamp or "").strip()
    match = re.match(
        r"^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})"
        r"(?:\.(\d+))?(Z|[+-]\d{2}:\d{2})?$",
        value,
    )
    if not match:
        return None, None, None, None, None, None
    year, month, day, hour, minute, second, subsecond, offset = match.groups()
    if offset == "Z":
        offset = "+00:00"
    date = f"{year}:{month}:{day}"
    capture_time = f"{hour}:{minute}:{second}{offset or ''}"
    suffix = (f".{subsecond}" if subsecond else "") + (offset or "")
    xmp_datetime = f"{year}-{month}-{day}T{hour}:{minute}:{second}{suffix}"
    # EXIF stores the offset in separate tags, so keep its base timestamp
    # timezone-free even though IPTC and XMP carry the suffix inline.
    exif_datetime = f"{date} {hour}:{minute}:{second}"
    return date, capture_time, exif_datetime, xmp_datetime, subsecond, offset


def _metadata_assignment(tag, value):
    """Build an ExifTool assignment while rejecting line-oriented arguments."""
    text = str(value).replace("\r", " ").replace("\n", " ")
    return f"-{tag}={text}"


def _export_metadata_args(fields, photo, species, camera_data, location_data=None):
    """Build ExifTool assignments for one rendered export."""
    args = []
    selected = set(fields)

    if "species" in selected:
        for index, name in enumerate(species):
            operator = "=" if index == 0 else "+="
            clean = str(name).replace("\r", " ").replace("\n", " ")
            args.extend([
                f"-XMP-dc:Subject{operator}{clean}",
                f"-IPTC:Keywords{operator}{clean}",
            ])

    (
        date,
        capture_time,
        exif_datetime,
        xmp_datetime,
        subsecond,
        offset,
    ) = _export_timestamp_parts(_photo_value(photo, "timestamp"))
    if "capture_date" in selected and date:
        args.append(_metadata_assignment("IPTC:DateCreated", date))
    if "capture_time" in selected and capture_time:
        args.append(_metadata_assignment("IPTC:TimeCreated", capture_time))
    if (
        "capture_date" in selected
        and "capture_time" in selected
        and exif_datetime
    ):
        args.extend([
            _metadata_assignment("EXIF:DateTimeOriginal", exif_datetime),
            _metadata_assignment("EXIF:CreateDate", exif_datetime),
            _metadata_assignment("XMP-exif:DateTimeOriginal", xmp_datetime),
            _metadata_assignment("XMP-xmp:CreateDate", xmp_datetime),
        ])
        if subsecond:
            args.extend([
                _metadata_assignment("EXIF:SubSecTimeOriginal", subsecond),
                _metadata_assignment("EXIF:SubSecTimeDigitized", subsecond),
            ])
        if offset:
            args.extend([
                _metadata_assignment("EXIF:OffsetTimeOriginal", offset),
                _metadata_assignment("EXIF:OffsetTimeDigitized", offset),
            ])

    if "rating" in selected:
        rating = _photo_value(photo, "rating")
        if rating is not None:
            args.append(_metadata_assignment("XMP-xmp:Rating", int(rating)))

    if "location" in selected:
        location_source = location_data if location_data is not None else photo
        latitude = _photo_value(location_source, "latitude")
        longitude = _photo_value(location_source, "longitude")
        if latitude is not None and longitude is not None:
            latitude = float(latitude)
            longitude = float(longitude)
            args.extend([
                _metadata_assignment("EXIF:GPSLatitude", abs(latitude)),
                _metadata_assignment(
                    "EXIF:GPSLatitudeRef", "N" if latitude >= 0 else "S"
                ),
                _metadata_assignment("EXIF:GPSLongitude", abs(longitude)),
                _metadata_assignment(
                    "EXIF:GPSLongitudeRef", "E" if longitude >= 0 else "W"
                ),
            ])

    if "camera" in selected:
        camera_tags = (
            ("EXIF:Make", camera_data.get("camera_make")),
            ("EXIF:Model", camera_data.get("camera_model")),
            ("EXIF:LensModel", camera_data.get("lens")),
            ("EXIF:FocalLength", camera_data.get("focal_length")),
            ("EXIF:FNumber", camera_data.get("aperture")),
            ("EXIF:ExposureTime", camera_data.get("shutter_speed")),
            ("EXIF:ISO", camera_data.get("iso")),
        )
        args.extend(
            _metadata_assignment(tag, value)
            for tag, value in camera_tags
            if value not in (None, "")
        )

    # A selected field can legitimately be unavailable for one photo. In
    # that case there is nothing to write and the image itself still exports.
    return args


def _exiftool_argfile_path(path):
    """Encode a path as one ExifTool argfile C-string line."""
    escaped = str(path).replace("\\", "\\\\")
    escaped = escaped.replace("\r", "\\r").replace("\n", "\\n")
    return f"#[CSTR]{escaped}"


def _run_cancellable_metadata(command, payload, timeout, cancel_check):
    """Run ExifTool while polling cancellation, and always reap its process."""
    with subprocess.Popen(
        command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, text=True, encoding="utf-8", **no_window_kwargs(),
    ) as process:
        deadline = time.monotonic() + timeout
        try:
            while True:
                if cancel_check():
                    return None
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise subprocess.TimeoutExpired(command, timeout)
                try:
                    stdout, stderr = process.communicate(
                        input=payload, timeout=min(0.1, remaining),
                    )
                except subprocess.TimeoutExpired:
                    # communicate retains its buffered input across retries.
                    payload = None
                    continue
                return subprocess.CompletedProcess(command, process.returncode, stdout, stderr)
        finally:
            if process.poll() is None:
                process.kill()
            process.communicate()


def _write_export_metadata_batch(jobs, cancel_check=None):
    """Write per-file metadata commands through one ExifTool process."""
    from metadata import _exiftool_command, find_exiftool

    if cancel_check and cancel_check():
        return _fail_export_metadata_jobs(jobs, "Export cancelled before metadata completed")

    exiftool = find_exiftool()
    if not exiftool:
        detail = "ExifTool is required to include export metadata"
        return _fail_export_metadata_jobs(jobs, detail)

    argfile_lines = []
    marker_prefix = "VIREO_METADATA_STATUS_"
    for index, (out_path, _filename, args) in enumerate(jobs, start=1):
        argfile_lines.extend([
            "-overwrite_original",
            "-n",
            *args,
            _exiftool_argfile_path(os.path.abspath(out_path)),
            "-echo3",
            f"{marker_prefix}{index}_${{status}}",
            f"-execute{index}",
        ])

    timeout = max(120, min(3600, len(jobs) * 5))
    try:
        command = [*_exiftool_command(exiftool), "-@", "-"]
        payload = "\n".join(argfile_lines) + "\n"
        if cancel_check is not None:
            result = _run_cancellable_metadata(command, payload, timeout, cancel_check)
            if result is None:
                for out_path, _filename, _args in jobs:
                    with contextlib.suppress(OSError):
                        os.unlink(out_path + "_exiftool_tmp")
                return _fail_export_metadata_jobs(
                    jobs, "Export cancelled before metadata completed",
                )
        else:
            result = subprocess.run(
                command, input=payload, capture_output=True, text=True,
                encoding="utf-8", timeout=timeout, **no_window_kwargs(),
            )
    except subprocess.TimeoutExpired:
        return _fail_export_metadata_jobs(jobs, "ExifTool metadata write timed out")
    except OSError as exc:
        return _fail_export_metadata_jobs(
            jobs, f"ExifTool could not start: {exc}"
        )
    except UnicodeError as exc:
        return _fail_export_metadata_jobs(
            jobs, f"ExifTool metadata could not be encoded: {exc}"
        )

    statuses = {
        int(index): int(status)
        for index, status in re.findall(
            rf"^{marker_prefix}(\d+)_(\d+)$", result.stdout, re.MULTILINE,
        )
    }
    invocation_failed = result.returncode != 0 and not statuses
    error_lines = [line.strip() for line in result.stderr.splitlines() if line.strip()]
    exported = 0
    errors = []
    for index, (out_path, filename, _args) in enumerate(jobs, start=1):
        status = statuses.get(index)
        if not invocation_failed and status == 0:
            exported += 1
            continue
        detail = next(
            (line for line in error_lines if out_path in line),
            error_lines[0] if error_lines else "ExifTool could not write export metadata",
        )
        with contextlib.suppress(OSError):
            os.unlink(out_path)
        errors.append(f"{filename}: {detail}")
    return exported, errors


def _fail_export_metadata_jobs(jobs, detail):
    """Remove derivatives for a failed metadata batch and return job errors."""
    errors = []
    for out_path, filename, _args in jobs:
        with contextlib.suppress(OSError):
            os.unlink(out_path)
        errors.append(f"{filename}: {detail}")
    return 0, errors


def _recipe_result_dimensions(width, height, recipe):
    """Return rendered dimensions after right-angle rotation and crop."""
    rotation = (recipe or {}).get("rotation", 0)
    if rotation in (90, 270):
        width, height = height, width
    crop = (recipe or {}).get("crop") if recipe else None
    if crop:
        width = float(crop["w"]) * width
        height = float(crop["h"]) * height
    return width, height


def _scale_dimensions_to_max(width, height, max_size):
    if max_size is None:
        return width, height
    long_edge = max(width, height)
    if long_edge > max_size:
        scale = max_size / long_edge
        width = round(width * scale)
        height = round(height * scale)
    return width, height


def _recipe_result_long_edge(width, height, recipe):
    """Return the rendered long edge after right-angle rotation and crop."""
    return rendered_recipe_long_edge(width, height, recipe)


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


def _relocate_stem_files(old_subdir, new_subdir, stem, listing_cache=None,
                         preserve_source=False):
    """Rename files matching ``stem`` from ``old_subdir`` to ``new_subdir``.

    Shared helper for both developed-output layouts (configured
    ``darktable_output_dir`` and the default ``<folder>/developed/``).

    ``listing_cache`` is an optional dict mapping ``old_subdir`` to its
    cached ``os.listdir`` result (or ``None`` for a missing/unreadable
    directory). It also remembers the first destination of each relocated
    file so another catalog photo with the same stem can copy that render to
    a different destination after the source has moved. Reuses cached
    listings across per-photo calls so
    ``move_folder_by_date`` — which routes every photo in a source folder
    through ``move_photos`` — doesn't rescan the same developed
    directory once per photo (a quadratic hazard on large libraries).

    When ``preserve_source`` is true, matching renders are copied rather
    than moved because another catalog photo with the same stem still
    resolves them from the source folder.

    Returns the number of files relocated. Never raises.
    """
    if not old_subdir or not new_subdir or old_subdir == new_subdir:
        return 0
    if listing_cache is not None and old_subdir in listing_cache:
        names = listing_cache[old_subdir]
    else:
        names = None
        if os.path.isdir(old_subdir):
            try:
                names = os.listdir(old_subdir)
            except OSError as exc:
                log.warning(
                    "Failed to list developed dir %s: %s", old_subdir, exc,
                )
                names = None
        if listing_cache is not None:
            listing_cache[old_subdir] = names
    if not names:
        return 0
    relocated = 0
    for name in names:
        entry_stem = os.path.splitext(name)[0]
        # Stem match is case-sensitive to match the read side in
        # `_DevelopedDirIndex`, which uses case-sensitive stems so two
        # photos differing only in case on a case-sensitive filesystem
        # don't collide onto each other's developed render.
        if entry_stem != stem:
            continue
        src_file = os.path.join(old_subdir, name)
        prior_destination = None
        relocation_key = ("relocated-developed-file", old_subdir, name)
        if not os.path.exists(src_file):
            # A prior per-photo call may already have moved this shared-stem
            # render (e.g. a RAW+JPEG pair). If the rows fan out to different
            # date folders, the later destination needs its own copy; the
            # render is only already in place when both rows share a target.
            if listing_cache is not None:
                prior_destination = listing_cache.get(relocation_key)
            if not prior_destination or not os.path.isfile(prior_destination):
                continue
        dst_file = os.path.join(new_subdir, name)
        if os.path.exists(dst_file):
            created_destination = None
            if listing_cache is not None:
                created_destination = listing_cache.get(relocation_key)
            if not preserve_source and os.path.exists(src_file) \
                    and created_destination == dst_file:
                # An earlier same-stem row copied this exact render to the
                # shared destination while another source row still needed
                # it. The final row can now remove the source without
                # overwriting the known-good copy we created.
                try:
                    os.remove(src_file)
                    relocated += 1
                except OSError as exc:
                    log.warning(
                        "Failed to remove relocated developed file %s: %s",
                        src_file, exc,
                    )
                continue
            # Preserve the existing render at the destination — matches
            # the collision policy used by `move_photos` for the photo
            # file itself (skip rather than overwrite).
            log.warning(
                "Skipping developed relocate %s -> %s: destination exists",
                src_file, dst_file,
            )
            continue
        try:
            os.makedirs(new_subdir, exist_ok=True)
            if prior_destination:
                shutil.copy2(prior_destination, dst_file)
            elif preserve_source:
                shutil.copy2(src_file, dst_file)
                if listing_cache is not None:
                    listing_cache.setdefault(relocation_key, dst_file)
            else:
                # Date-organized moves commonly cross from local storage to a
                # mounted archive. Keep the fast atomic rename on one
                # filesystem, then explicitly copy+unlink when rename fails.
                # Handling unlink separately matters: the destination copy is
                # complete at that point, so a locked/read-only source must
                # not make the outer failure cleanup delete the only render
                # the newly-repointed catalog row can resolve.
                try:
                    os.rename(src_file, dst_file)
                except OSError:
                    shutil.copy2(src_file, dst_file)
                    try:
                        os.unlink(src_file)
                    except OSError as unlink_exc:
                        log.warning(
                            "Copied developed file %s -> %s but failed to "
                            "remove source: %s",
                            src_file, dst_file, unlink_exc,
                        )
                if listing_cache is not None:
                    listing_cache.setdefault(relocation_key, dst_file)
            relocated += 1
        except OSError as exc:
            log.warning(
                "Failed to relocate developed file %s -> %s: %s",
                src_file, dst_file, exc,
            )
            # shutil.copy2 (and shutil.move's EXDEV fallback) can create
            # ``dst_file`` and then fail partway — a full disk, a flaky
            # mounted archive, or a lost network share. The photo row was
            # already repointed at this destination before the relocate
            # call, so a truncated/corrupt render sitting at the new key
            # would be picked up by ``_iter_developed_outputs`` and served
            # to exports and full-resolution instead of the intact source
            # render (or a clean fallback to the RAW). Delete the partial
            # so lookup falls back correctly.
            if os.path.lexists(dst_file):
                try:
                    os.remove(dst_file)
                except OSError as cleanup_exc:
                    log.warning(
                        "Failed to remove partial developed file %s: %s",
                        dst_file, cleanup_exc,
                    )
    if relocated:
        try:
            if not os.listdir(old_subdir):
                os.rmdir(old_subdir)
                # Keep the cached names for the rest of this batch. A later
                # same-stem photo may need to copy a render from the first
                # destination even though the source directory is now gone.
        except OSError:
            pass
    return relocated


def relocate_developed_file(developed_dir, old_folder_path, new_folder_path,
                            stem, listing_cache=None, preserve_source=False):
    """Rebase a single photo's developed outputs after its folder changes.

    Sibling to `relocate_developed_dir` for per-photo moves (e.g. date-
    organized folder moves fan photos from one source folder into many
    date destinations, so the whole-subdir rename doesn't apply). Moves
    every developed file whose stem matches ``stem`` from the old key's
    subdir into the new key's subdir. Extensions are enumerated from disk
    rather than a fixed list so a develop job configured for an unusual
    output format still gets its render moved.

    ``listing_cache`` is an optional dict shared across per-photo calls
    to amortize the ``os.listdir`` of the old-key subdir. Passing one is
    important for ``move_folder_by_date``, which fans a source folder's
    photos through many per-destination ``move_photos`` calls; without it
    the same developed subdir would be listed once per moved photo.

    ``preserve_source`` copies instead of moving while another same-stem
    catalog photo remains in the old folder.

    Returns the number of files relocated. Never raises; a filesystem
    hiccup here logs a warning and is treated as a no-op so it doesn't
    also fail the move itself.
    """
    if not developed_dir or not old_folder_path or not new_folder_path \
            or not stem:
        return 0
    if old_folder_path == new_folder_path:
        return 0
    old_key = developed_folder_key(old_folder_path)
    new_key = developed_folder_key(new_folder_path)
    if not old_key or not new_key or old_key == new_key:
        return 0
    old_subdir = os.path.join(developed_dir, old_key)
    new_subdir = os.path.join(developed_dir, new_key)
    return _relocate_stem_files(
        old_subdir, new_subdir, stem, listing_cache, preserve_source,
    )


def relocate_default_developed_file(old_folder_path, new_folder_path, stem,
                                    listing_cache=None, preserve_source=False):
    """Rebase a photo's default-location developed render after a move.

    When ``darktable_output_dir`` is unset, the develop job writes to
    ``<folder>/developed/<stem>.<ext>``. The catalog's export/full-
    resolution lookup then probes ``<folder>/developed/`` first (see
    ``_iter_developed_outputs``), so a per-photo move that leaves the
    render under the old source folder orphans it — the app silently
    falls back to the RAW/original. This helper rebases the render to
    the destination folder's ``developed/`` subdir to match.

    A whole-folder move (``move_folder``) naturally carries the
    ``developed/`` subdir along in the recursive copy; this helper is
    specifically for per-photo moves (``move_photos``,
    ``move_folder_by_date``) where photos fan out to different
    destinations and the source subdir stays behind.

    ``listing_cache`` — see ``relocate_developed_file``.
    ``preserve_source`` copies instead of moving while another same-stem
    catalog photo remains in the old folder.

    Returns the number of files relocated. Never raises.
    """
    if not old_folder_path or not new_folder_path or not stem:
        return 0
    if old_folder_path == new_folder_path:
        return 0
    old_subdir = os.path.join(old_folder_path, "developed")
    new_subdir = os.path.join(new_folder_path, "developed")
    return _relocate_stem_files(
        old_subdir, new_subdir, stem, listing_cache, preserve_source,
    )


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
    photo, recipe, max_size, wc_max, vireo_dir, exif_data=None, folder_path=None
):
    """Return True when the working copy can preserve requested export pixels."""
    if not max_size or max_size <= 0:
        return False
    if max_size > wc_max:
        return False
    # For RAW primaries with an edit recipe, the working copy is unreliable
    # while the RAW source is available: libraries built before the
    # highlight-preserving RAW decode landed carry working copies derived
    # from clipped sources (camera JPEG or the JPEG-first RAW path), and
    # EDIT_MATH_VERSION purges previews/thumbnails but not working copies.
    # Reusing such a copy would silently apply the recipe to clipped bytes.
    # If the RAW source is offline/missing, though, the working copy is the
    # only local fallback for resized exports.
    if (
        recipe
        and os.path.splitext(photo["filename"])[1].lower() in RAW_EXTENSIONS
        and folder_path
        and os.path.exists(os.path.join(folder_path, photo["filename"]))
    ):
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
    ``RAW_DECODE_LINEAR`` instead of the camera JPEG (whose
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
    required_w, required_h = _recipe_result_dimensions(
        original_w, original_h, recipe,
    )
    comp_render_w, comp_render_h = _recipe_result_dimensions(
        comp_w, comp_h, recipe,
    )
    required_w, required_h = _scale_dimensions_to_max(
        required_w, required_h, max_size,
    )
    comp_render_w, comp_render_h = _scale_dimensions_to_max(
        comp_render_w, comp_render_h, max_size,
    )
    if comp_render_w + 1 >= required_w and comp_render_h + 1 >= required_h:
        return companion
    return None


def _fallback_source_after_wc_eviction(
    source_path, photo, folder_path, vireo_dir,
):
    """Return the original path when a working-copy source vanished mid-export.

    Quota eviction can unlink ``working/<id>.jpg`` between
    ``_select_export_source`` and ``load_image``, turning an otherwise valid
    export into a "failed to load image" error. When the failed source is a
    working copy that is now missing (as opposed to a corrupt-but-present
    file), fall back to the photo's original file so the export still
    succeeds.
    """
    if not vireo_dir or not folder_path:
        return None
    working_root = os.path.join(os.path.abspath(vireo_dir), "working")
    try:
        abs_source = os.path.abspath(source_path)
    except (TypeError, ValueError):
        return None
    if not abs_source.startswith(working_root + os.sep):
        return None
    if os.path.isfile(source_path):
        return None
    fallback = os.path.join(folder_path, photo["filename"])
    if not fallback or not os.path.isfile(fallback):
        return None
    return fallback


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


def _select_export_source(
    *, photo, folder_path, folders, recipe, max_size, wc_max, vireo_dir,
    developed_dir, developed_index, output_ext, exif_data,
):
    """Resolve the source candidate used before export allocates a filename."""
    for dev_candidate in _iter_developed_outputs(
        photo["filename"],
        folder_path,
        developed_dir,
        developed_index,
        preferred_exts=_developed_ext_preference(output_ext),
    ):
        if _developed_can_satisfy_size(
            dev_candidate, photo, max_size, recipe, exif_data=exif_data,
        ):
            return dev_candidate

    use_wc = _working_copy_can_satisfy_export(
        photo, recipe, max_size, wc_max, vireo_dir,
        exif_data=exif_data, folder_path=folder_path,
    )
    if not use_wc:
        primary_path = (
            os.path.join(folder_path, photo["filename"])
            if folder_path else ""
        )
        primary_is_raw = (
            os.path.splitext(photo["filename"])[1].lower() in RAW_EXTENSIONS
        )
        companion = _companion_can_satisfy_export(
            photo,
            folder_path,
            recipe,
            max_size,
            exif_data=exif_data,
            skip_raw_primary=(
                not primary_is_raw or os.path.isfile(primary_path)
            ),
        )
        if companion:
            return companion
    return _resolve_source(
        photo, vireo_dir, folders, use_working_copy=use_wc,
    )


def _deduplicate_path(
    path, reserved_paths=None, path_key=None, is_reserved=None,
):
    """Append _2, _3, etc. if a path exists or is reserved by this batch."""
    reserved_paths = reserved_paths or set()
    path_key = path_key or (lambda candidate: candidate)

    def reserved(candidate):
        if is_reserved is not None:
            return is_reserved(candidate)
        return path_key(candidate) in reserved_paths

    if not os.path.lexists(path) and not reserved(path):
        return path
    stem, ext = os.path.splitext(path)
    counter = 2
    while (
        os.path.lexists(f"{stem}_{counter}{ext}")
        or reserved(f"{stem}_{counter}{ext}")
    ):
        counter += 1
    return f"{stem}_{counter}{ext}"


def _claim_export_path(path):
    """Atomically claim a non-overwriting output path and open stream."""
    out_dir = os.path.dirname(path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    stem, ext = os.path.splitext(path)
    counter = 1
    while True:
        candidate = path if counter == 1 else f"{stem}_{counter}{ext}"
        try:
            return candidate, open(candidate, "xb")
        except FileExistsError:
            counter += 1


def _nearest_existing_directory(path):
    """Return the directory whose filesystem will contain a planned path."""
    existing_path = os.path.realpath(path)
    while not os.path.isdir(existing_path):
        parent = os.path.dirname(existing_path)
        if parent == existing_path:
            break
        existing_path = parent
    return existing_path


def _destination_path_identity(path):
    """Return a stable identity for an existing destination directory."""
    existing_path = _nearest_existing_directory(path)
    try:
        destination_stat = os.stat(existing_path)
    except OSError:
        return ("path", os.path.normcase(os.path.realpath(existing_path)))
    return (
        "filesystem",
        destination_stat.st_dev,
        destination_stat.st_ino,
    )


class _DestinationPathReservations:
    """Mirror planned paths on the destination volume to detect aliases."""

    def __init__(self, destination):
        self.destination = os.path.realpath(destination)
        probe_path = os.path.realpath(destination)
        while not os.path.isdir(probe_path):
            parent = os.path.dirname(probe_path)
            if parent == probe_path:
                break
            probe_path = parent
        try:
            self._temporary_directory = tempfile.TemporaryDirectory(
                prefix=".VireoExportReservation-", dir=probe_path,
            )
        except OSError as exc:
            raise ExportPreflightError(
                "Vireo could not verify filename collisions on the destination "
                "volume. Check the folder permissions and try again."
            ) from exc
        self.root = self._temporary_directory.name

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        self.close()

    def close(self):
        self._temporary_directory.cleanup()
        self.root = None

    def matches_destination(self, destination):
        """Check whether destination can see this reservation's probe marker."""
        marker_name = os.path.basename(self.root)
        return os.path.isdir(os.path.join(os.path.realpath(destination), marker_name))

    def _relative_path(self, candidate, destination=None):
        destination = os.path.realpath(destination or self.destination)
        relative = os.path.relpath(os.path.realpath(candidate), destination)
        if relative == os.pardir or relative.startswith(os.pardir + os.sep):
            return None
        return relative

    def contains(self, candidate, destination=None):
        relative = self._relative_path(candidate, destination)
        if relative is None:
            return False
        return os.path.exists(os.path.join(self.root, relative))

    def add(self, candidate, destination=None):
        relative = self._relative_path(candidate, destination)
        if relative is None:
            return
        shadow_path = os.path.join(self.root, relative)
        os.makedirs(os.path.dirname(shadow_path), exist_ok=True)
        try:
            with open(shadow_path, "xb"):
                pass
        except FileExistsError:
            # A filesystem alias can race between contains() and add(); either
            # spelling represents the same reserved destination.
            pass


def preview_export_renames(db, photo_ids, destination=None, options=None):
    """Return filename changes that export collision handling would make.

    This preflight mirrors export's source, destination, template, sequence,
    and deduplication rules without rendering output files. A later filesystem
    change can still introduce a new collision, so the dialog also states the
    non-overwrite policy permanently.
    """
    options = options or {}
    template = options.get("naming_template", "{original}")
    output_ext = normalize_output_format(
        options.get("format", options.get("output_format", "jpg"))
    )["extension"]
    max_size = options.get("max_size")
    if max_size is not None:
        max_size = int(max_size)
    try:
        wc_max = int(options.get("working_copy_max_size", 4096))
    except (ValueError, TypeError):
        wc_max = 4096
    vireo_dir = options.get("vireo_dir") or ""
    developed_dir = options.get("developed_dir") or ""
    subfolder = (
        normalize_subfolder_name(options.get("subfolder_name"))
        if options.get("export_to_subfolder") else ""
    )

    photos_map = db.get_photos_by_ids(photo_ids)
    folders = {folder["id"]: folder["path"] for folder in db.get_folder_tree()}
    species_map = db.get_species_keywords_for_photos(photo_ids)
    exif_data_map = _get_photo_exif_data(db, photo_ids)
    edit_recipes = db.get_photo_edit_recipes(photo_ids)
    developed_index = _DevelopedDirIndex()
    seq_counters = {}
    destination_reservations = {}
    renames = []

    for pid in photo_ids:
        photo = photos_map.get(pid)
        if not photo:
            continue

        folder_path = folders.get(photo["folder_id"], "")
        source_path = _select_export_source(
            photo=photo,
            folder_path=folder_path,
            folders=folders,
            recipe=edit_recipes.get(pid),
            max_size=max_size,
            wc_max=wc_max,
            vireo_dir=vireo_dir,
            developed_dir=developed_dir,
            developed_index=developed_index,
            output_ext=output_ext,
            exif_data=exif_data_map.get(pid),
        )
        if not source_path or not os.path.isfile(source_path):
            continue

        destination_base = destination or folder_path
        if not destination_base:
            continue
        if not destination and not os.path.isdir(destination_base):
            continue
        photo_destination = os.path.normpath(
            os.path.join(destination_base, subfolder)
            if subfolder else destination_base
        )

        species_list = species_map.get(pid, [])
        species = species_list[0] if species_list else None
        photo_info = {
            "filename": photo["filename"],
            "timestamp": photo["timestamp"],
            "rating": photo["rating"],
            "folder_name": os.path.basename(folder_path),
        }
        subdir_key = (
            photo_destination,
            os.path.dirname(
                resolve_template(template, photo_info, species=species, seq=0)
            ),
        )
        seq_counters.setdefault(subdir_key, 0)
        seq_counters[subdir_key] += 1
        rel_path = resolve_template(
            template,
            photo_info,
            species=species,
            seq=seq_counters[subdir_key],
        )
        rel_path_safe = os.path.normpath(rel_path).lstrip(os.sep + ".")
        requested_path = os.path.join(
            photo_destination, rel_path_safe + f".{output_ext}"
        )

        dest_real = os.path.realpath(photo_destination)
        requested_real = os.path.realpath(requested_path)
        dest_prefix = dest_real if dest_real.endswith(os.sep) else dest_real + os.sep
        if (
            not requested_real.startswith(dest_prefix)
            and requested_real != dest_real
        ):
            continue

        reservation_destination = _nearest_existing_directory(
            os.path.dirname(requested_path)
        )
        reservation_key = _destination_path_identity(reservation_destination)
        reservation_group = destination_reservations.setdefault(
            reservation_key, []
        )
        reservations = next((
            candidate for candidate in reservation_group
            if (
                candidate.matches_destination(reservation_destination)
                if hasattr(candidate, "matches_destination")
                else os.path.realpath(candidate.destination)
                == os.path.realpath(reservation_destination)
            )
        ), None)
        if reservations is None:
            reservations = _DestinationPathReservations(
                reservation_destination
            )
            reservation_group.append(reservations)

        def is_reserved(
            candidate,
            current_destination=reservation_destination,
            current_reservations=reservations,
        ):
            return current_reservations.contains(candidate, current_destination)

        export_path = _deduplicate_path(
            requested_path,
            is_reserved=is_reserved,
        )
        reservations.add(export_path, reservation_destination)
        if export_path != requested_path:
            renames.append({
                "photo_id": pid,
                "requested_name": os.path.basename(requested_path),
                "export_name": os.path.basename(export_path),
                "destination": os.path.dirname(export_path),
            })

    for reservation_group in destination_reservations.values():
        for reservations in reservation_group:
            reservations.close()
    return renames
