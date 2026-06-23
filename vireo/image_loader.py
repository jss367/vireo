"""Load images from various formats (JPEG, PNG, TIFF, NEF, CR2, ARW, etc.).

Performance notes:
- RAW decode is the bottleneck (~1.7s full, ~0.5s half-size for a 45MP NEF)
- We use half_size=True when the target is ≤ half the sensor resolution (3x faster)
- PIL resize and JPEG encode are negligible (<0.15s)
- libraw (via rawpy) is already C — Rust/numba won't help here

RAW strategy (JPEG-first):
- Modern cameras embed a full-resolution JPEG in the RAW file (the same image
  the camera would produce in RAW+JPEG mode). For a photo organizer, that's
  both faster to decode and sufficient in quality.
- It also works for RAW variants libraw cannot decode. Example: Nikon Z 8
  "High Efficiency*" (HE*) files use TicoRAW compression that libraw 0.22
  cannot decode. The embedded JPEG is our only path for those files.
- We prefer the embedded JPEG whenever it meets the requested size, and
  fall back to it when demosaic-based decode raises.
"""

import io
import logging
import os
from pathlib import Path

from PIL import Image, ImageOps

log = logging.getLogger(__name__)

RAW_EXTENSIONS = {".nef", ".cr2", ".cr3", ".arw", ".raf", ".dng", ".rw2", ".orf"}
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".tiff", ".tif", ".bmp", ".webp"}
SUPPORTED_EXTENSIONS = IMAGE_EXTENSIONS | RAW_EXTENSIONS

# macOS "package" directories that hold OTHER apps' managed data. Walking
# into them triggers Sequoia's "<app> would like to access data from other
# apps" (kTCCServiceSystemPolicyAppData) consent prompt — and because such a
# bundle holds thousands of files, the prompt reappears on every file the
# walk touches, so clicking Allow never makes it stop. The contents are also
# app-managed derivatives we'd never want to ingest. Any directory walker
# that traverses user-chosen roots (~/Pictures by default contains
# "Photos Library.photoslibrary") must prune these. Matched case-insensitively.
_EXCLUDED_DIR_SUFFIXES = (
    ".photoslibrary",          # Apple Photos
    ".photolibrary",           # legacy iPhoto / older Photos
    ".migratedphotolibrary",
    ".aplibrary",              # Aperture
    ".migratedaplibrary",
)
_EXCLUDED_DIR_NAMES = frozenset({"photo booth library"})


def is_excluded_scan_dir(name):
    """Return True if a directory basename is an other-app data bundle that
    directory walkers must not descend into (see _EXCLUDED_DIR_* above)."""
    lower = name.lower()
    return lower in _EXCLUDED_DIR_NAMES or lower.endswith(_EXCLUDED_DIR_SUFFIXES)


def is_excluded_scan_path(path):
    """Return True if *path* is, or sits inside, an excluded bundle.

    Used as the root-level guard for every walker that accepts a
    user-chosen path. A leaf-only check is insufficient: a user can
    select a child of the bundle directly (e.g.
    ``~/Pictures/Photos Library.photoslibrary/originals``), and stale
    folder rows from before this guard existed can carry the same
    shape. Either way, opening it still trips the macOS TCC
    "access data from other apps" prompt — so we reject the whole
    subtree, not just the bundle root.

    Also resolves symlinks before checking. A user-selected root may be a
    symlink whose literal path components don't name the bundle (e.g.
    ``~/PhotoLib -> Photos Library.photoslibrary``); ``Path.is_dir()`` and
    ``os.walk()`` follow the link into the protected bundle regardless, so
    skipping the resolve would let the walkers re-trip the macOS TCC prompt.

    Non-path inputs (e.g. JSON primitives like ``123`` or ``True`` that
    sneak through ``body.get("root")`` before the route's directory check)
    are treated as "not excluded" rather than raising. ``Path(int)`` raises
    ``TypeError``; without this guard the route would return 500 instead of
    the 400 the subsequent ``os.path.isdir`` check produces.
    """
    try:
        p = Path(path)
    except TypeError:
        return False
    if any(is_excluded_scan_dir(part) for part in p.parts):
        return True
    # Resolve symlinks. os.path.realpath() never raises (returns its input
    # for missing paths) and resolves intermediate links too, so an alias
    # anywhere in the chain (e.g. ``~/Aliases/MyLib/originals`` where
    # ``MyLib`` links to the bundle) still gets caught.
    try:
        resolved = Path(os.path.realpath(str(p)))
    except OSError:
        return False
    if resolved == p:
        return False
    return any(is_excluded_scan_dir(part) for part in resolved.parts)


def prune_scan_dirs(dirnames):
    """Mutate an ``os.walk`` *dirnames* list in place, removing excluded
    bundles so the walk never recurses into them. Returns the removed names
    (useful for logging what was skipped).

    Note: by the time this runs, ``os.walk`` has already called
    ``DirEntry.is_dir()`` on every child to populate ``dirnames`` — and that
    call follows symlinks, so a child like ``LibraryAlias ->
    Photos Library.photoslibrary`` is stat'ed against the bundle target
    *before* pruning. Use :func:`safe_scan_walk` for user-chosen roots so
    the symlink target is never stat-followed.
    """
    removed = [d for d in dirnames if is_excluded_scan_dir(d)]
    if removed:
        dirnames[:] = [d for d in dirnames if d not in removed]
    return removed


def _symlink_target_is_excluded(entry):
    """Return True if *entry* is a symlink whose target basename names an
    excluded bundle. Uses ``os.readlink`` (purely textual — never follows
    the link), so a link pointing at ``Photos Library.photoslibrary`` can
    be classified without statting the protected bundle target."""
    try:
        if not entry.is_symlink():
            return False
    except OSError:
        return False
    try:
        target = os.readlink(entry.path)
    except OSError:
        return False
    if not target:
        return False
    # Strip trailing separators before taking the basename so a link target
    # spelled ``.../Photos Library.photoslibrary/`` is still matched.
    target_name = os.path.basename(target.rstrip("/").rstrip("\\"))
    return is_excluded_scan_dir(target_name)


def safe_scan_walk(top, onerror=None):
    """Yield ``(dirpath, dirnames, filenames)`` like ``os.walk(top,
    followlinks=False)``, but never stat-following a symlinked excluded
    bundle.

    The stock ``os.walk`` classifies each child by calling
    ``DirEntry.is_dir()``, which follows symlinks. If a user-chosen root
    contains ``LibraryAlias -> Photos Library.photoslibrary``, that
    classification stat alone reaches into the protected bundle and
    re-trips the macOS "access data from other apps" TCC prompt — even
    though the subsequent :func:`prune_scan_dirs` would remove the entry
    from recursion. We need to detect symlinks pointing at excluded
    bundles *before* any stat that follows them; ``os.readlink`` is the
    only call here that touches a symlink, and it returns the literal
    target string without resolving it.

    Direct-name exclusion (``is_excluded_scan_dir``) is also applied
    here, so callers don't need a separate ``prune_scan_dirs`` step on
    ``dirnames``. Classification uses ``follow_symlinks=False``, matching
    ``os.walk(followlinks=False)``'s recursion behaviour — symlinks to
    non-excluded directories are surfaced in ``filenames`` (not
    ``dirnames``) and never recursed into. Callers that already filter
    ``filenames`` with ``os.path.isfile`` (which returns False for
    directories, including symlinked dirs) discard those entries
    automatically.
    """
    try:
        scandir_it = os.scandir(top)
    except OSError as exc:
        if onerror is not None:
            onerror(exc)
        return
    dirs = []
    nondirs = []
    skipped = []
    with scandir_it:
        for entry in scandir_it:
            name = entry.name
            # Name-based exclusion catches direct bundle entries
            # (``Photos Library.photoslibrary``) without any stat.
            if is_excluded_scan_dir(name):
                skipped.append(name)
                continue
            # Symlink whose target names an excluded bundle. os.readlink
            # is textual and never follows the link, so this is safe even
            # when the target is a protected macOS bundle.
            if _symlink_target_is_excluded(entry):
                skipped.append(name)
                continue
            try:
                entry_is_dir = entry.is_dir(follow_symlinks=False)
            except OSError as exc:
                if onerror is not None:
                    onerror(exc)
                entry_is_dir = False
            if entry_is_dir:
                dirs.append(name)
            else:
                nondirs.append(name)
    if skipped:
        log.info(
            "Skipping other-app data bundle(s) under %s: %s",
            top, ", ".join(skipped),
        )
    yield top, dirs, nondirs
    for subdir in dirs:
        yield from safe_scan_walk(os.path.join(top, subdir), onerror=onerror)


def load_image(file_path, max_size=1024):
    """Load an image file and return a PIL Image, resized to max_size.

    Supports JPEG, PNG, TIFF, and RAW formats (NEF, CR2, ARW, etc.).
    For RAW files, prefers the embedded full-res JPEG preview when it meets
    the requested size; falls back to demosaic-based decode otherwise.
    Returns None if the file cannot be loaded.

    For RAW files we retry once on transient libraw I/O errors. NAS volumes
    occasionally fail mid-read under burst access (4 concurrent thumbnail
    requests is enough to trip a slow share), and a single retry typically
    succeeds — much cheaper than asking the user to refresh and recover
    from a cached 404.

    Args:
        file_path: Path to the image file
        max_size: Maximum dimension (longest side). None or 0 for full resolution.

    Returns:
        PIL.Image.Image or None
    """
    path = Path(file_path)
    ext = path.suffix.lower()

    if ext not in SUPPORTED_EXTENSIONS:
        return None

    try:
        if ext in RAW_EXTENSIONS:
            img = _load_raw_with_retry(path, max_size)
        else:
            with Image.open(str(path)) as opened:
                img = ImageOps.exif_transpose(opened)
                img = img.convert("RGB")

        if img is None:
            return None

        if max_size and max_size > 0 and max(img.size) > max_size:
            img.thumbnail((max_size, max_size), Image.LANCZOS)

        return img
    except Exception as e:
        log.warning("Failed to load image: %s — %s", file_path, e)
        return None


def _load_raw_with_retry(path, max_size):
    """Wrap _load_raw with a single retry on transient libraw I/O errors.

    Only retries on LibRawIOError — other libraw errors (UnsupportedFormat,
    DataError) are deterministic for a given file and won't recover. The
    retry is sequential (no backoff) since these failures are usually
    contention-related and resolve immediately.
    """
    try:
        return _load_raw(path, max_size)
    except Exception as e:
        # Identify libraw I/O errors by class name so we don't have to
        # import rawpy at module scope (it's only present when a RAW
        # actually loads). The class is rawpy._rawpy.LibRawIOError.
        if type(e).__name__ != "LibRawIOError":
            raise
        log.info("Transient libraw I/O error on %s; retrying once", path)
        return _load_raw(path, max_size)


def _load_standard(path, max_size):
    """Load a standard image file (JPEG, PNG, TIFF, etc.) via PIL.

    Opens the file, converts to RGB, and resizes to max_size if needed.
    This is the fast path — no RAW decoding involved.

    Args:
        path: path to the image file
        max_size: maximum dimension (longest side). None or 0 for full resolution.

    Returns:
        PIL.Image.Image or None
    """
    try:
        with Image.open(str(path)) as opened:
            img = ImageOps.exif_transpose(opened)
            img = img.convert("RGB")
        if max_size and max_size > 0 and max(img.size) > max_size:
            img.thumbnail((max_size, max_size), Image.LANCZOS)
        return img
    except Exception as e:
        log.warning("Failed to load standard image: %s — %s", path, e)
        return None


def get_canonical_image_path(photo, vireo_dir, folders):
    """Return the canonical image path for a photo — the root of the pyramid.

    Preference order:
      1. working copy JPEG (if photo.working_copy_path is set and file exists)
      2. source file (folder.path + '/' + photo.filename)

    If working_copy_path is set but the file is missing, logs a warning and
    falls back to source. Callers should still handle missing source files.

    Args:
        photo: dict with working_copy_path, folder_id, filename
        vireo_dir: path to ~/.vireo/
        folders: {folder_id: folder_path} mapping

    Returns:
        str path (may or may not exist — caller checks)
    """
    # Support both dict and sqlite3.Row (no .get() on Row).
    def _pget(key):
        try:
            return photo[key]
        except (KeyError, IndexError):
            return None

    wc_rel = _pget("working_copy_path")
    if wc_rel:
        wc_abs = os.path.join(vireo_dir, wc_rel)
        if os.path.exists(wc_abs):
            return wc_abs
        log.warning(
            "Canonical path: working copy missing for photo %s at %s; "
            "falling back to source", _pget("id"), wc_abs,
        )
    folder_path = folders.get(photo["folder_id"], "")
    return os.path.join(folder_path, photo["filename"])


def load_working_image(photo, vireo_dir, max_size=1024, folders=None):
    """Load a photo's working image — the fast path for all pixel operations.

    Uses the pre-extracted working copy JPEG if available,
    otherwise falls back to loading the original file directly.

    Args:
        photo: photo dict with working_copy_path, folder_id, filename
        vireo_dir: path to ~/.vireo/
        max_size: maximum dimension (longest side). None for full resolution.
        folders: optional {folder_id: path} mapping (required when working_copy_path is NULL)

    Returns:
        PIL.Image.Image or None
    """
    if photo.get("working_copy_path"):
        wc_path = os.path.join(vireo_dir, photo["working_copy_path"])
        if os.path.exists(wc_path):
            return _load_standard(wc_path, max_size)

    # No working copy — load original (may be JPEG or RAW)
    if folders is None:
        return None
    folder_path = folders.get(photo["folder_id"], "")
    source_path = os.path.join(folder_path, photo["filename"])
    return load_image(source_path, max_size)


def extract_working_copy(source_path, output_path, max_size=4096, quality=92):
    """Extract a JPEG working copy from an image file.

    Args:
        source_path: path to source image (RAW or JPEG)
        output_path: where to save the working copy JPEG
        max_size: max dimension (longest side). 0 or None for full resolution.
        quality: JPEG quality (1-95)

    Returns:
        True on success, False on failure
    """
    try:
        img = load_image(source_path, max_size=max_size or None)
        if img is None:
            return False
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        img.save(output_path, "JPEG", quality=quality)
        return True
    except Exception:
        log.warning("Failed to extract working copy from %s", source_path,
                    exc_info=True)
        return False


def _load_raw(path, max_size):
    """Load a RAW file using JPEG-first strategy.

    1. Try the embedded JPEG preview; use it if it's big enough for max_size.
    2. Otherwise demosaic via rawpy.postprocess().
    3. If postprocess raises (e.g. libraw 0.22 can't decode Nikon HE*/TicoRAW),
       fall back to the embedded JPEG even if smaller than max_size.
    """
    import rawpy

    with rawpy.imread(str(path)) as raw:
        embedded = _extract_embedded_jpeg(raw)

        # JPEG-first: if the embedded preview is large enough for the request,
        # use it and skip the slower RAW decode entirely.
        if (embedded is not None and max_size and max_size > 0
                and max(embedded.size) >= max_size):
            return embedded

        # Otherwise demosaic the sensor data, falling back to the embedded
        # JPEG if libraw can't decode this RAW variant.
        try:
            return _postprocess_raw(raw, max_size)
        except Exception as e:
            if embedded is not None:
                # Only claim "full camera output" when the embedded JPEG
                # actually matches the sensor's active dimensions on both
                # axes (e.g. Nikon HE*/TicoRAW). A long-edge-only check
                # would still mislabel cropped/aspect-mismatched previews
                # like 6000×3376 against a 6000×4000 sensor.
                sensor_dims = sorted(
                    (raw.sizes.width, raw.sizes.height), reverse=True
                )
                embedded_dims = sorted(embedded.size, reverse=True)
                qualifier = (
                    ", full camera output"
                    if sensor_dims[0]
                    and embedded_dims[0] >= sensor_dims[0]
                    and embedded_dims[1] >= sensor_dims[1]
                    else ""
                )
                log.info(
                    "libraw cannot decode %s (%s); using embedded JPEG "
                    "(%dx%d%s)",
                    path, e, embedded.size[0], embedded.size[1], qualifier,
                )
                return embedded
            raise


def _extract_embedded_jpeg(raw):
    """Return the embedded JPEG preview as a PIL Image, or None if unavailable."""
    import rawpy
    try:
        thumb = raw.extract_thumb()
    except Exception:
        return None
    if thumb.format != rawpy.ThumbFormat.JPEG:
        return None
    try:
        img = Image.open(io.BytesIO(thumb.data))
        img.load()
        # Apply EXIF orientation so portrait/rotated RAW files are upright.
        # Without this, cameras that record orientation in the EXIF header
        # (which is common in embedded JPEGs) would be returned sideways.
        img = ImageOps.exif_transpose(img)
        return img.convert("RGB")
    except Exception:
        return None


def _postprocess_raw(raw, max_size):
    """Demosaic raw sensor data into a PIL Image.

    Uses half-size decode when the target fits, which is ~3x faster and still
    produces ~4000x2700 for a 45MP sensor.
    """
    use_half = False
    if max_size and max_size > 0:
        sensor_long = max(raw.sizes.width, raw.sizes.height)
        half_long = sensor_long // 2
        if max_size <= half_long:
            use_half = True
    rgb = raw.postprocess(half_size=use_half)
    return Image.fromarray(rgb)
