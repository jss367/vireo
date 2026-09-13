"""Load images from various formats (JPEG, PNG, TIFF, NEF, CR2, ARW, etc.).

Performance notes:
- RAW decode is the bottleneck (~1.0s full, ~0.5s half-size for a 45MP NEF)
- We use half_size=True when the target is ≤ half the sensor resolution (3x faster)
- Full-size decodes demosaic with PPG, not libraw's default AHD: ~1.0s
  instead of ~1.7s, for output that matches to 40-52 dB PSNR
  (see :func:`_postprocess_raw`)
- PIL resize and JPEG encode are negligible (<0.15s)
- libraw (via rawpy) is already C — Rust/numba won't help here
- The libraw inside the rawpy wheel is built without OpenMP, so demosaic is
  single-threaded (measured 1.00x CPU/wall on a 16-core M3 Max). Decoding
  many RAWs in parallel is therefore worth more than optimizing one decode.

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

import contextlib
import io
import logging
import os
import tempfile
from pathlib import Path

from PIL import Image, ImageOps

log = logging.getLogger(__name__)


class ScanCancelled(RuntimeError):
    """Raised when a directory walker's ``cancel_check`` callable returns
    truthy. Defined here (not in scanner) so lower-level walkers like
    :func:`safe_scan_walk` can raise cancellation without importing
    scanner and creating a cycle. ``scanner.ScanCancelled`` is an alias
    for this class."""


RAW_EXTENSIONS = {".nef", ".cr2", ".cr3", ".arw", ".raf", ".dng", ".rw2", ".orf"}
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".tiff", ".tif", ".bmp", ".webp"}
SUPPORTED_EXTENSIONS = IMAGE_EXTENSIONS | RAW_EXTENSIONS
RAW_DECODE_JPEG_FIRST = "jpeg_first"
RAW_DECODE_CAMERA_RENDERED = "camera_rendered"
RAW_DECODE_PRESERVE_HIGHLIGHTS = "preserve_highlights"
_RAW_DECODE_MODES = {
    RAW_DECODE_JPEG_FIRST,
    RAW_DECODE_CAMERA_RENDERED,
    RAW_DECODE_PRESERVE_HIGHLIGHTS,
}

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
    seen = set()
    with scandir_it:
        for entry in scandir_it:
            if entry.name in seen:
                continue
            seen.add(entry.name)
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


def safe_scan_walk(top, onerror=None, cancel_check=None, on_scandir_batch=None,
                   on_entry=None):
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

    ``cancel_check`` is polled at the start of each visited directory and
    again every ~256 scandir entries within a directory. A single
    extremely large directory (a media dump with 1M+ files) can otherwise
    keep this loop consuming the whole ``os.scandir`` iterator before
    yielding, so a caller that only checks between yields cannot pause
    or cancel until the enumeration finishes. When the check returns
    truthy the walker raises :class:`ScanCancelled` from the current
    ``next()``; callers do not need a separate per-yield poll for that
    case.

    ``on_entry`` is invoked (with no arguments) for *every* directory entry
    received — a cheap liveness heartbeat for watchdogs that must tell a
    slow-but-streaming network listing apart from one that is wedged.

    ``on_scandir_batch`` is invoked (with no arguments) at the same
    per-256-entry checkpoint. The Jobs UI's discovery heartbeat piggybacks
    on the outer per-yield loop, which cannot advance while the walker is
    still filling one directory's buffer; without this hook a single very
    large directory would appear stalled after the initial discovery event
    until the whole enumeration finishes.
    """
    if cancel_check is not None and cancel_check():
        raise ScanCancelled("directory walk cancelled")
    try:
        scandir_it = os.scandir(top)
    except OSError as exc:
        if onerror is not None:
            onerror(exc)
        return
    dirs = []
    nondirs = []
    skipped = []
    seen = set()
    try:
        with scandir_it:
            for i, entry in enumerate(scandir_it):
                if on_entry is not None:
                    on_entry()
                # Poll cancellation while we're still filling the buffer.
                # A directory with millions of entries would otherwise finish
                # the whole ``scandir`` loop before yielding, leaving pause
                # and cancel stuck for the full enumeration. 256 is small
                # enough that a big-mount stat batch still gets checked
                # promptly, cheap enough that the check adds no measurable
                # overhead to normal folders.
                if i and (i & 0xFF) == 0:
                    if cancel_check is not None and cancel_check():
                        raise ScanCancelled("directory walk cancelled")
                    # Same checkpoint fires a heartbeat: the outer per-yield
                    # progress loop can't advance while this scandir call is
                    # still buffering, so a single huge directory would look
                    # stalled to the Jobs UI without this.
                    if on_scandir_batch is not None:
                        on_scandir_batch()
                name = entry.name
                if name in seen:
                    continue
                seen.add(name)
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
    except OSError as exc:
        # Mirror ``os.walk``: an error raised *while iterating* (not just
        # when opening) is reported and the directory is abandoned. macOS
        # raises ``ENOTCONN`` from the iterator when an SMB share drops
        # mid-walk; without this the whole walk died with a traceback.
        if onerror is not None:
            onerror(exc)
        return
    if skipped:
        log.info(
            "Skipping other-app data bundle(s) under %s: %s",
            top, ", ".join(skipped),
        )
    yield top, dirs, nondirs
    for subdir in dirs:
        yield from safe_scan_walk(
            os.path.join(top, subdir), onerror=onerror, cancel_check=cancel_check,
            on_scandir_batch=on_scandir_batch, on_entry=on_entry,
        )


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
        raw_decode: RAW_DECODE_JPEG_FIRST (default),
            RAW_DECODE_CAMERA_RENDERED, or RAW_DECODE_PRESERVE_HIGHLIGHTS.

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


def _record_working_copy_access(wc_path):
    """Stamp a working copy as recently used for quota-eviction ordering.

    Held under ``working_copy_publication_guard`` like every other access
    stamp, so it cannot land between the quota pass's directory scan and
    its unlink. Imported lazily to keep ``image_loader`` free of a
    module-level dependency on the cache layer.
    """
    from working_copy_cache import (
        touch_working_copy_access,
        working_copy_publication_guard,
    )

    with working_copy_publication_guard():
        touch_working_copy_access(wc_path)


def _restore_working_copy_access_time(wc_path, preserved_times_ns):
    """Put a working copy's atime back after an opt-out read decoded it.

    ``_evict_once`` uses ``max(mtime, atime)`` as the recency key.
    ``load_working_image(record_access=False)`` documents that its
    callers (background library traversals like sharpness, classify,
    culling) want to leave that key alone — but PIL's ``open`` on a
    strictatime or default relatime filesystem advances the atime as a
    side effect of the read. Restoring it neutralizes that side effect
    so a batch job that walks every working copy doesn't refresh every
    file's recency and drown out the interactive touches from
    ``touch_working_copy_access``.

    A concurrent publisher's write shows up as an mtime change; skip
    the restore in that case so their write isn't erased and the fresh
    recency their publish carries is preserved. Best effort: this runs
    on a request/job serving pixels, so a failed stat/utime costs only
    eviction-ordering accuracy and must never fail the caller.
    """
    try:
        post_stat = os.stat(wc_path)
    except OSError:
        return
    if post_stat.st_mtime_ns != preserved_times_ns[1]:
        return
    with contextlib.suppress(OSError):
        os.utime(wc_path, ns=preserved_times_ns)


def load_working_image(
    photo, vireo_dir, max_size=1024, folders=None, *, return_source=False,
    record_access=False,
):
    """Load a photo's working image — the fast path for all pixel operations.

    Uses the pre-extracted working copy JPEG if available,
    otherwise falls back to loading the original file directly.

    Args:
        photo: photo dict with working_copy_path, folder_id, filename
        vireo_dir: path to ~/.vireo/
        max_size: maximum dimension (longest side). None for full resolution.
        folders: optional {folder_id: path} mapping (required when working_copy_path is NULL)
        record_access: stamp the working copy as recently used when it is
            the source. Opt-in rather than automatic: quota eviction orders
            by recency, and a background job that walks the whole library
            (classification, sharpness, culling) would stamp every copy and
            flatten the signal into noise. Request paths serving a user
            looking at a photo pass True; job callers leave it False.

    Returns:
        PIL.Image.Image or None. When ``return_source=True``, returns an
        ``(image, source_kind)`` pair whose source kind is ``working_copy``
        or ``original``. This lets delayed artifact publishers preserve the
        provenance of the pixels they actually consumed even if the catalog
        row changes afterward.
    """
    def _result(image, source_kind):
        if return_source:
            return image, source_kind
        return image

    if photo.get("working_copy_path"):
        wc_path = os.path.join(vireo_dir, photo["working_copy_path"])
        if os.path.exists(wc_path):
            # Snapshot atime before the decode so a job caller that
            # opted out of recording access does not accidentally
            # advance the eviction key. ``_evict_once`` sorts by
            # ``max(mtime, atime)``; on strictatime mounts (and on
            # relatime once the daily bump condition trips) PIL's
            # ``open`` inside ``_load_standard`` advances ``atime``
            # even when we pass ``record_access=False``. Without a
            # restore, a background library traversal — sharpness,
            # classify, culling — would refresh every copy it walks
            # and flatten the LRU signal the interactive callers
            # rely on. Snapshot before the read so we can put atime
            # back to whatever it was; a concurrent publisher shows
            # up as an mtime change, and we skip the restore in that
            # case so their write isn't erased.
            #
            # Known residual: if an interactive read stamps this same
            # copy between our snapshot and our restore, the restore
            # puts the older value back and that stamp is lost, so a
            # photo the user just viewed can look staler than it is and
            # be evicted early (it regenerates on demand — no data loss).
            # Not worth a third layer of compensation on top of an
            # overload we intend to remove: the fix is to stop deriving
            # recency from stat metadata at all and keep it in a
            # ``working_copy_access(photo_id, accessed_at)`` table, the
            # shape the preview cache already uses. Tracked as a
            # follow-up rather than grown here.
            preserved_times_ns = None
            if not record_access:
                try:
                    pre_stat = os.stat(wc_path)
                    preserved_times_ns = (
                        pre_stat.st_atime_ns, pre_stat.st_mtime_ns,
                    )
                except OSError:
                    preserved_times_ns = None
            working_image = _load_standard(wc_path, max_size)
            if working_image is not None:
                if record_access:
                    # Only the touch takes
                    # ``working_copy_publication_guard``, so it cannot
                    # land between the quota pass's directory scan and
                    # its unlink (an identity-skipped file leaves the
                    # cache above a lowered quota).
                    #
                    # The decode deliberately stays outside that guard.
                    # It is a process-wide lock that eviction holds
                    # across a scandir of the entire cache, so guarding
                    # every interactive decode with it would serialize
                    # all of the app's image reads against each other
                    # and against eviction — the exact stall this work
                    # is meant to remove. The residual races are benign:
                    # an unlink in the exists/open window falls through
                    # to the original source below, and a publisher
                    # replacing the path between decode and touch costs
                    # one misattributed recency stamp on a file that is
                    # being actively used either way.
                    _record_working_copy_access(wc_path)
                elif preserved_times_ns is not None:
                    _restore_working_copy_access_time(
                        wc_path, preserved_times_ns,
                    )
                return _result(working_image, "working_copy")
            if preserved_times_ns is not None:
                # Even a failed decode may have bumped atime; put it
                # back so eviction doesn't see a cold file as fresh
                # just because we tried to read it.
                _restore_working_copy_access_time(
                    wc_path, preserved_times_ns,
                )

    # No usable working copy — load original (may be JPEG or RAW). This also
    # closes the exists/open race with quota eviction: a failed working-copy
    # open must not strand direct callers when ``folders`` can resolve the
    # still-available source.
    if folders is None:
        return _result(None, "original")
    folder_path = folders.get(photo["folder_id"], "")
    source_path = os.path.join(folder_path, photo["filename"])
    return _result(load_image(source_path, max_size), "original")


def extract_working_copy(
    source_path,
    output_path,
    max_size=4096,
    quality=92,
    raw_decode=RAW_DECODE_PRESERVE_HIGHLIGHTS,
    publication_guard=None,
    on_publish=None,
):
    """Extract a JPEG working copy from an image file.

    Args:
        source_path: path to source image (RAW or JPEG)
        output_path: where to save the working copy JPEG
        max_size: max dimension (longest side). 0 or None for full resolution.
        quality: JPEG quality (1-95)
        raw_decode: RAW decode strategy. Working copies default to the
            highlight-preserving edit source; display renditions can request
            RAW_DECODE_CAMERA_RENDERED without changing that edit source.
        publication_guard: optional context-manager factory that serializes
            publication to a shared canonical cache path with eviction.
        on_publish: optional callback ``on_publish(output_path)`` invoked
            immediately after the atomic replace, WHILE the publication
            guard is still held. Callers use this to capture the file's
            fingerprint (mtime, inode, size) as part of the atomic publish
            step — a post-return stat would be racy, because another
            publisher can atomically replace the same canonical path in
            the gap between guard release and the caller's next action.
            Exceptions from the callback are swallowed with a warning so
            they cannot fail an otherwise successful publish.

    Returns:
        True on success, False on failure
    """
    img = None
    tmp_path = None
    try:
        img = load_image(
            source_path,
            max_size=max_size or None,
            raw_decode=raw_decode,
        )
        if img is None:
            return False
        output_dir = os.path.dirname(output_path)
        os.makedirs(output_dir, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(
            prefix=f".{os.path.basename(output_path)}.",
            suffix=".jpg.tmp",
            dir=output_dir,
        )
        os.close(fd)
        img.save(tmp_path, "JPEG", quality=quality)
        guard = publication_guard() if publication_guard else contextlib.nullcontext()
        with guard:
            os.replace(tmp_path, output_path)
            if on_publish is not None:
                try:
                    on_publish(output_path)
                except Exception:
                    log.warning(
                        "extract_working_copy on_publish callback raised for %s",
                        output_path, exc_info=True,
                    )
        tmp_path = None
        return True
    except Exception:
        log.warning("Failed to extract working copy from %s", source_path,
                    exc_info=True)
        return False
    finally:
        if tmp_path:
            try:
                os.remove(tmp_path)
            except OSError:
                log.warning(
                    "Could not remove partial working-copy tempfile %s",
                    tmp_path, exc_info=True,
                )
        if img is not None:
            with contextlib.suppress(Exception):
                img.close()


def _load_raw(path, max_size, raw_decode=RAW_DECODE_JPEG_FIRST):
    """Load a RAW file using the requested decode strategy.

    JPEG-first:
      1. Try the embedded JPEG preview; use it if it's big enough for max_size.
      2. Otherwise demosaic via rawpy.postprocess().
      3. If postprocess raises (e.g. libraw 0.22 can't decode Nikon HE*/TicoRAW),
         fall back to the embedded JPEG even if smaller than max_size.

    Camera-rendered:
      1. Use a near-full embedded JPEG for full-resolution display requests.
      2. Otherwise follow the JPEG-first demosaic/fallback behavior.

    Preserve-highlights:
      1. Demosaic the RAW with auto-bright disabled and highlight blending on.
      2. Fall back to the embedded JPEG only if libraw cannot decode the RAW.
    """
    import rawpy

    with rawpy.imread(str(path)) as raw:
        embedded = _extract_embedded_jpeg(raw)

        # JPEG-first browsing uses the embedded preview when it covers the
        # requested size. Full-resolution camera-rendered browsing also accepts
        # a preview whose two axes are within 1% of the active sensor area; many
        # cameras omit a narrow border (for example 8256x5504 vs 8288x5520).
        # That JPEG is the rendition used by thumbnails and fit-to-window views,
        # so retaining it at 1:1 prevents a visible tone jump.
        sensor_dims = sorted((raw.sizes.width, raw.sizes.height), reverse=True)
        embedded_dims = sorted(embedded.size, reverse=True) if embedded else None
        embedded_is_near_full = bool(
            embedded_dims
            and sensor_dims[0]
            and sensor_dims[1]
            and embedded_dims[0] >= sensor_dims[0] * 0.99
            and embedded_dims[1] >= sensor_dims[1] * 0.99
        )
        embedded_covers_request = bool(
            embedded is not None
            and max_size
            and max_size > 0
            and max(embedded.size) >= max_size
        )
        if (
            raw_decode in (RAW_DECODE_JPEG_FIRST, RAW_DECODE_CAMERA_RENDERED)
            and embedded is not None
            and (
                embedded_covers_request
                or (
                    raw_decode == RAW_DECODE_CAMERA_RENDERED
                    and not max_size
                    and embedded_is_near_full
                )
            )
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

    Full-size decodes use PPG rather than libraw's default AHD: on 45MP
    D850 NEFs read from local disk, AHD takes 1.70-1.74s against PPG's
    1.00-1.03s, so roughly 0.7s of the ~1.1s demosaic disappears. Output
    matches AHD to 40-52 dB PSNR and is indistinguishable at 1:1 even on
    demosaic worst cases — dense foliage and blown-highlight frond edges.
    Half-size decodes bin rather than demosaic, so the algorithm choice
    doesn't reach them; passing it unconditionally keeps one code path.
    """
    import rawpy

    use_half = False
    if max_size and max_size > 0:
        sensor_long = max(raw.sizes.width, raw.sizes.height)
        half_long = sensor_long // 2
        if max_size <= half_long:
            use_half = True
    kwargs = {
        "half_size": use_half,
        "demosaic_algorithm": rawpy.DemosaicAlgorithm.PPG,
    }
    if preserve_highlights:
        kwargs.update({
            "use_camera_wb": True,
            "no_auto_bright": True,
            "bright": 1.0,
            "highlight_mode": rawpy.HighlightMode.Blend,
        })
    rgb = raw.postprocess(**kwargs)
    return Image.fromarray(rgb)
