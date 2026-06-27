"""Load images from various formats (JPEG, PNG, TIFF, NEF, CR2, ARW, etc.).

Performance notes:
- RAW decode is the bottleneck (~1.7s full, ~0.5s half-size for a 45MP NEF)
- We use half_size=True when the target is ≤ half the sensor resolution (3x faster)
- PIL resize and JPEG encode are negligible (<0.15s)
- libraw (via rawpy) is already C — Rust/numba won't help here

RAW strategy:
- Modern cameras embed a full-resolution JPEG in the RAW file (the same image
  the camera would produce in RAW+JPEG mode). For a photo organizer, that's
  both faster to decode and sufficient in quality.
- It also works for RAW variants libraw cannot decode. Example: Nikon Z 8
  "High Efficiency*" (HE*) files use TicoRAW compression that libraw 0.22
  cannot decode. The embedded JPEG is our only path for those files.
- Browsing paths use the JPEG-first strategy: prefer the embedded JPEG whenever
  it meets the requested size, and fall back to it when demosaic-based decode
  raises.
- Edit-quality working copies use RAW_DECODE_PRESERVE_HIGHLIGHTS: demosaic the
  RAW with auto-bright disabled and highlight blending enabled, falling back to
  the embedded JPEG only when libraw cannot decode the file.
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
RAW_DECODE_JPEG_FIRST = "jpeg_first"
RAW_DECODE_PRESERVE_HIGHLIGHTS = "preserve_highlights"
_RAW_DECODE_MODES = {RAW_DECODE_JPEG_FIRST, RAW_DECODE_PRESERVE_HIGHLIGHTS}

# macOS "package" directories that hold OTHER apps' managed data. Walking
# into them triggers Sequoia's "<app> would like to access data from other
# apps" (kTCCServiceSystemPolicyAppData) consent prompt — and because such a
# bundle holds thousands of files, the prompt reappears on every file the
# walk touches, so clicking Allow never makes it stop. The contents are also
# app-managed derivatives or media-library internals we'd never want to ingest.
# Any directory walker
# that traverses user-chosen roots (~/Pictures by default contains
# "Photos Library.photoslibrary"; ~/Music can contain
# "Music Library.musiclibrary") must prune these. Matched case-insensitively.
_EXCLUDED_DIR_SUFFIXES = (
    ".photoslibrary",          # Apple Photos
    ".musiclibrary",           # Apple Music
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

    Also follows symlinks textually. A user-selected root may be a
    symlink whose literal path components don't name the bundle (e.g.
    ``~/PhotoLib -> Photos Library.photoslibrary``); ``Path.is_dir()``
    and ``os.walk()`` would follow the link into the protected bundle
    regardless, so the walkers must reject these before any stat that
    follows the link.

    We do NOT use ``os.path.realpath`` for that resolution. ``realpath``
    walks the resolved chain by ``lstat``-ing every component along the
    way — including the bundle target itself once a link points at it —
    and the very reason this guard exists is to avoid any stat that
    reaches into the protected bundle. Instead we walk the path one
    component at a time, using ``os.path.islink`` (which ``lstat``s only
    the link node — these live outside the bundle when the user picked
    an alias like ``~/PhotoLibAlias``) and ``os.readlink`` (purely
    textual — reads just the link's stored target string). Neither call
    stats anything below a resolved link, so even a directly selected
    alias like ``~/PhotoLibAlias -> Photos Library.photoslibrary``
    never reaches into the protected bundle.

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
    parts = p.parts
    if not parts:
        return False
    # Walk component by component. Each iteration appends one literal
    # part of the original path, then follows any symlink chain from the
    # accumulated location purely textually. If a chain target's
    # components name an excluded bundle, we stop immediately — so for
    # ``~/Aliases/MyLib/originals`` (MyLib → bundle), we detect the
    # bundle while processing ``MyLib`` and never construct or stat
    # ``MyLib/originals`` against the resolved target.
    current = parts[0]
    for i, part in enumerate(parts):
        if i > 0:
            current = os.path.join(current, part)
        resolved = _follow_symlink_chain_textually(current)
        if resolved is None:
            return True
        current = resolved
    return False


def _follow_symlink_chain_textually(path_str, max_depth=40):
    """Follow the symlink chain starting at *path_str* using only
    ``os.path.islink`` + ``os.readlink``. Returns the chain's terminal
    path (or *path_str* unchanged when nothing is a link) — or ``None``
    if any link in the chain points at or into an excluded bundle.

    ``os.path.islink`` ``lstat``s the link node itself, never the
    resolved target; ``os.readlink`` only reads the link's stored
    target bytes. Together they never stat anything under a resolved
    link, which is what lets this helper classify
    ``~/PhotoLibAlias -> Photos Library.photoslibrary`` without
    reaching into the protected bundle.
    """
    current = path_str
    visited = set()
    for _ in range(max_depth):
        if current in visited:
            return current
        visited.add(current)
        try:
            if not os.path.islink(current):
                return current
            target = os.readlink(current)
        except OSError:
            return current
        if not os.path.isabs(target):
            target = os.path.join(os.path.dirname(current), target)
        target = os.path.normpath(target)
        if any(is_excluded_scan_dir(part) for part in Path(target).parts):
            return None
        current = target
    # Depth cap reached without terminating in a non-link. Fail closed:
    # the caller treats ``None`` as "excluded", so a chain longer than
    # ``max_depth`` that might still resolve into a protected bundle never
    # gets allowed past the guard.
    return None


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
    """Return True if *entry* is a symlink whose target sits in (or whose
    symlink chain reaches) an excluded bundle. Uses ``os.readlink`` /
    ``os.path.islink`` only — never a stat that follows the link — so a
    link pointing into a protected bundle is classified without statting
    the bundle target.

    Two shapes are caught, neither matched by a basename-only check:

    1. A file-named link whose target path names an excluded bundle
       directly, e.g.
       ``IMG.jpg -> ../Photos Library.photoslibrary/originals/IMG.jpg`` —
       the immediate target's parts include the bundle suffix.
    2. A chained link whose immediate target is a *plain* path that
       itself contains, or resolves through, another link into the
       bundle, e.g. ``LibraryAlias -> MidAlias`` where
       ``MidAlias -> Photos Library.photoslibrary`` (or a file-named
       variant ``IMG.jpg -> MidAlias/originals/IMG.jpg``). Without
       chasing the chain, the immediate target ``MidAlias`` looks
       benign, but ``os.path.isfile`` / ``Path.is_file`` would follow
       both hops and re-trip the macOS TCC prompt.

    The chain is followed via :func:`is_excluded_scan_path`, which walks
    components one at a time using textual ``islink``+``readlink``. That
    component-by-component walk keeps each ``lstat`` confined to the
    link node — it never resolves an intermediate link far enough to
    touch the protected bundle.

    Relative targets are joined against the link's parent and
    normalized (still purely textual — ``os.path.normpath`` does not
    stat) so ``..`` segments resolve before classification.
    """
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
    if not os.path.isabs(target):
        target = os.path.join(os.path.dirname(entry.path), target)
    target = os.path.normpath(target)
    return is_excluded_scan_path(target)


def safe_iter_dir(top, onerror=None):
    """Yield ``Path`` objects for direct children of *top*, skipping
    excluded bundles by name and symlinks whose target sits inside one.

    Use this instead of ``Path.iterdir()`` (or ``os.scandir``) in
    non-recursive walks where the caller will then call ``Path.is_file()``
    / ``Path.suffix`` / ``stat()`` on each entry. Those calls follow
    symlinks, so a child like ``LibraryAlias -> Photos
    Library.photoslibrary`` — or a direct bundle child ``Photos
    Library.photoslibrary`` itself — would stat the bundle target and
    re-trip the macOS "access data from other apps" TCC prompt this
    guard exists to avoid, even though the caller's extension/name
    filter would have rejected the entry afterwards.

    Classification uses ``DirEntry.is_dir(follow_symlinks=False)`` and
    ``os.readlink`` (purely textual) — never a stat that follows a link
    into a protected bundle.
    """
    try:
        scandir_it = os.scandir(top)
    except OSError as exc:
        if onerror is not None:
            onerror(exc)
        return
    skipped = []
    with scandir_it:
        for entry in scandir_it:
            if is_excluded_scan_dir(entry.name):
                skipped.append(entry.name)
                continue
            if _symlink_target_is_excluded(entry):
                skipped.append(entry.name)
                continue
            yield Path(entry.path)
    if skipped:
        log.info(
            "Skipping other-app data bundle(s) under %s: %s",
            top, ", ".join(skipped),
        )


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


def load_image(file_path, max_size=1024, raw_decode=RAW_DECODE_JPEG_FIRST):
    """Load an image file and return a PIL Image, resized to max_size.

    Supports JPEG, PNG, TIFF, and RAW formats (NEF, CR2, ARW, etc.).
    For RAW files, ``raw_decode`` controls whether browsing gets the fast
    JPEG-first path or edit-quality renders demosaic the RAW with highlight
    preservation settings before falling back to an embedded JPEG.
    Returns None if the file cannot be loaded.

    For RAW files we retry once on transient libraw I/O errors. NAS volumes
    occasionally fail mid-read under burst access (4 concurrent thumbnail
    requests is enough to trip a slow share), and a single retry typically
    succeeds — much cheaper than asking the user to refresh and recover
    from a cached 404.

    Args:
        file_path: Path to the image file
        max_size: Maximum dimension (longest side). None or 0 for full resolution.
        raw_decode: RAW_DECODE_JPEG_FIRST (default) or
            RAW_DECODE_PRESERVE_HIGHLIGHTS.

    Returns:
        PIL.Image.Image or None
    """
    path = Path(file_path)
    ext = path.suffix.lower()

    if ext not in SUPPORTED_EXTENSIONS:
        return None
    if raw_decode not in _RAW_DECODE_MODES:
        raise ValueError(f"raw_decode must be one of: {', '.join(sorted(_RAW_DECODE_MODES))}")

    try:
        if ext in RAW_EXTENSIONS:
            img = _load_raw_with_retry(path, max_size, raw_decode=raw_decode)
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


def _load_raw_with_retry(path, max_size, raw_decode=RAW_DECODE_JPEG_FIRST):
    """Wrap _load_raw with a single retry on transient libraw I/O errors.

    Only retries on LibRawIOError — other libraw errors (UnsupportedFormat,
    DataError) are deterministic for a given file and won't recover. The
    retry is sequential (no backoff) since these failures are usually
    contention-related and resolve immediately.
    """
    try:
        return _load_raw(path, max_size, raw_decode=raw_decode)
    except Exception as e:
        # Identify libraw I/O errors by class name so we don't have to
        # import rawpy at module scope (it's only present when a RAW
        # actually loads). The class is rawpy._rawpy.LibRawIOError.
        if type(e).__name__ != "LibRawIOError":
            raise
        log.info("Transient libraw I/O error on %s; retrying once", path)
        return _load_raw(path, max_size, raw_decode=raw_decode)


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
        img = load_image(
            source_path,
            max_size=max_size or None,
            raw_decode=RAW_DECODE_PRESERVE_HIGHLIGHTS,
        )
        if img is None:
            return False
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        img.save(output_path, "JPEG", quality=quality)
        return True
    except Exception:
        log.warning("Failed to extract working copy from %s", source_path,
                    exc_info=True)
        return False


def _load_raw(path, max_size, raw_decode=RAW_DECODE_JPEG_FIRST):
    """Load a RAW file using the requested decode strategy.

    JPEG-first:
      1. Try the embedded JPEG preview; use it if it's big enough for max_size.
      2. Otherwise demosaic via rawpy.postprocess().
      3. If postprocess raises (e.g. libraw 0.22 can't decode Nikon HE*/TicoRAW),
         fall back to the embedded JPEG even if smaller than max_size.

    Preserve-highlights:
      1. Demosaic the RAW with auto-bright disabled and highlight blending on.
      2. Fall back to the embedded JPEG only if libraw cannot decode the RAW.
    """
    import rawpy

    with rawpy.imread(str(path)) as raw:
        embedded = _extract_embedded_jpeg(raw)

        # JPEG-first: if the embedded preview is large enough for the request,
        # use it and skip the slower RAW decode entirely.
        if (
            raw_decode == RAW_DECODE_JPEG_FIRST
            and embedded is not None
            and max_size
            and max_size > 0
            and max(embedded.size) >= max_size
        ):
            return embedded

        # Otherwise demosaic the sensor data, falling back to the embedded
        # JPEG if libraw can't decode this RAW variant.
        try:
            return _postprocess_raw(
                raw,
                max_size,
                preserve_highlights=raw_decode == RAW_DECODE_PRESERVE_HIGHLIGHTS,
            )
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


def _postprocess_raw(raw, max_size, preserve_highlights=False):
    """Demosaic raw sensor data into a PIL Image.

    Uses half-size decode when the target fits, which is ~3x faster and still
    produces ~4000x2700 for a 45MP sensor.
    """
    import rawpy

    use_half = False
    if max_size and max_size > 0:
        sensor_long = max(raw.sizes.width, raw.sizes.height)
        half_long = sensor_long // 2
        if max_size <= half_long:
            use_half = True
    kwargs = {"half_size": use_half}
    if preserve_highlights:
        kwargs.update({
            "use_camera_wb": True,
            "no_auto_bright": True,
            "bright": 1.0,
            "highlight_mode": rawpy.HighlightMode.Blend,
        })
    rgb = raw.postprocess(**kwargs)
    return Image.fromarray(rgb)
