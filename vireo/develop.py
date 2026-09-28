"""Wrapper for darktable-cli to develop RAW photos."""

import contextlib
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import threading

try:
    from .proc import no_window_kwargs
except ImportError:
    from proc import no_window_kwargs

log = logging.getLogger(__name__)

_DIAG_MAX_CHARS = 500
_NIKON_HE_COMPRESSION_VALUES = {13, 14}
# Bytes 8..10 of a type-2 AppImage. They sit in the ELF header's EI_PAD
# region, which real toolchains leave zeroed, so this does not collide with
# an ordinary executable.
_APPIMAGE_MAGIC = b"AI\x02"

# Per-binary result of the "does FUSE work here?" probe. Keys are binary paths
# and values are:
#   "direct"          — FUSE works, run the AppImage as-is.
#   "extracted"       — persistent tree lives at _APPIMAGE_APPRUN_CACHE[binary];
#                       run that AppRun directly.
#   "extract-per-run" — extraction itself failed, so we fall back to setting
#                       APPIMAGE_EXTRACT_AND_RUN=1 on every call. This still
#                       works, just at the ~178 MB-unpack-per-photo cost we
#                       hoped to avoid.
# Populated on the first develop_photo call for a binary and reused thereafter
# so a batch of N photos pays the probe cost (and, in "extracted" mode, the
# unpack cost) once, not N times.
_APPIMAGE_MODE_CACHE = {}

# For binaries cached as "extracted", the path to the persistent AppRun inside
# the one-time extraction tree. Kept separate so the mode cache stays a simple
# string map (matching how it is inspected in tests) and so a caller can find
# out which entrypoint will be invoked without reaching into the mode value.
_APPIMAGE_APPRUN_CACHE = {}

# Where --appimage-extract lands its output. AppImage type-2 runtimes
# unconditionally create a directory literally named "squashfs-root" in the
# working directory; we set the working directory to <binary>.extracted so
# every extraction ends up isolated next to its source AppImage.
_APPIMAGE_EXTRACT_SUBDIR = "squashfs-root"

# Marker file inside <binary>.extracted/ recording the source AppImage's
# fingerprint at the moment extraction succeeded. --appimage-extract preserves
# AppRun's embedded build timestamp (from when darktable was built), while a
# freshly downloaded AppImage carries a download-time mtime — so AppRun is
# typically OLDER than the source it came from. Comparing the two mtimes
# directly would therefore invalidate every fresh extraction on its next
# reuse and re-unpack the ~500 MB tree per photo. The marker sidesteps that:
# we record what the source looked like at extraction time and reuse only
# while it still matches.
_APPIMAGE_SOURCE_MARKER = ".vireo-source-fingerprint"

# Cap the one-time --appimage-extract subprocess. A 178 MB darktable AppImage
# unpacks in ~5–15 s on a spinning disk; 300 s is generous headroom without
# hanging a develop job forever if the extraction is truly wedged.
_APPIMAGE_EXTRACT_TIMEOUT_SECONDS = 300

# Ratio of extracted-tree size to AppImage size on disk. Measured against
# darktable: a ~178 MB AppImage unpacks to ~500 MB, so ~3x; the multiplier
# adds headroom so a marginal disk that would just barely fit triggers the
# per-run fallback instead of half-extracting and failing mid-batch.
_APPIMAGE_EXTRACTION_SIZE_MULTIPLIER = 3.5

# Per-binary lock guarding the "check for stale extraction, delete, extract,
# publish" sequence in _extract_appimage_once. Two concurrent develop jobs on
# a FUSE-less host would otherwise both observe an empty mode cache and race
# on the shared <binary>.extracted directory, so one worker could rmtree or
# execute the other's partial tree. The outer lock only protects insertion
# into the dict; the returned per-binary Lock is held for the extraction
# itself so concurrent callers for the SAME binary serialize while callers
# for DIFFERENT binaries do not block one another.
_APPIMAGE_EXTRACT_LOCKS_LOCK = threading.Lock()
_APPIMAGE_EXTRACT_LOCKS = {}


def _get_extract_lock(binary):
    """Return (and lazily create) the per-binary extraction lock."""
    with _APPIMAGE_EXTRACT_LOCKS_LOCK:
        lock = _APPIMAGE_EXTRACT_LOCKS.get(binary)
        if lock is None:
            lock = threading.Lock()
            _APPIMAGE_EXTRACT_LOCKS[binary] = lock
        return lock


def _has_space_for_extraction(binary, extract_dir_parent):
    """True when there is plausibly enough free disk to unpack ``binary``.

    Returns True on OSError so a transient stat failure does not block
    extraction on a working disk; the extraction itself will surface a real
    ENOSPC as a subprocess failure that the caller already handles.
    """
    try:
        source_size = os.path.getsize(binary)
        free_bytes = shutil.disk_usage(extract_dir_parent).free
    except OSError:
        return True
    return free_bytes >= int(source_size * _APPIMAGE_EXTRACTION_SIZE_MULTIPLIER)

# Substrings that identify an AppImage FUSE failure in the runtime's own
# error output — lowercase and matched case-insensitively so kernel messages
# in either case are covered. When we see one of these on a nonzero exit,
# retrying with APPIMAGE_EXTRACT_AND_RUN=1 is what turns "fails outright"
# into "runs". Other nonzero exits (bad RAW, missing style, etc.) must NOT
# trigger the fallback: reopening subprocess.run with the extract flag on an
# unrelated darktable error would waste a full unpack and still fail.
_APPIMAGE_FUSE_FAILURE_MARKERS = (
    "appimages require fuse",
    "libfuse.so.2",
    "libfuse.so",
    "fusermount",
    "cannot mount appimage",
    "dlopen(): error loading libfuse",
)


def _looks_like_appimage_fuse_failure(stdout, stderr):
    """True when subprocess output points at a missing/broken FUSE, not darktable.

    Only these strings — printed by the AppImage runtime itself before darktable
    starts — should trigger the APPIMAGE_EXTRACT_AND_RUN retry. A darktable
    exit code with no FUSE marker is a real develop failure and must surface
    as-is instead of being retried under an environment change that would
    silently unpack ~178 MB.
    """
    combined = ((stdout or "") + "\n" + (stderr or "")).lower()
    return any(marker in combined for marker in _APPIMAGE_FUSE_FAILURE_MARKERS)


def _is_appimage(path):
    """True when ``path`` is a type-2 AppImage bundle.

    Detected by magic bytes rather than filename: users commonly rename an
    AppImage (~/.local/bin/darktable) or keep a lowercase .appimage suffix,
    and find_darktable hands back any configured file as-is. A missed
    detection launches darktable's GUI and stalls a headless export for the
    full 120s timeout with an error naming nothing the user could act on.

    Never raises: a missing path, a directory, or an unreadable file is
    simply "not an AppImage".
    """
    try:
        with open(path, "rb") as f:
            return f.read(11)[8:11] == _APPIMAGE_MAGIC
    except OSError:
        return False


def _apprun_path_for(binary):
    """Where _extract_appimage_once would place ``binary``'s AppRun."""
    return os.path.join(binary + ".extracted", _APPIMAGE_EXTRACT_SUBDIR, "AppRun")


def _source_fingerprint(source_binary):
    """Identity used to detect an in-place replacement of ``source_binary``.

    Combines mtime (nanoseconds) and size: mtime alone can be forged by
    os.utime, size alone misses same-size rebuilds, together they identify
    the file well enough to spot an actual reinstall.
    """
    st = os.stat(source_binary)
    return f"{st.st_mtime_ns}:{st.st_size}"


def _marker_path_for_apprun(apprun):
    """Location of the source-fingerprint marker for ``apprun``'s extraction.

    ``apprun`` sits at ``<extract_dir>/squashfs-root/AppRun``; the marker
    lives at ``<extract_dir>/`` so it is a peer of the squashfs-root that
    ``--appimage-extract`` creates, and so a full rmtree of ``extract_dir``
    removes it alongside the tree.
    """
    return os.path.join(
        os.path.dirname(os.path.dirname(apprun)), _APPIMAGE_SOURCE_MARKER,
    )


def _is_reusable_apprun(apprun, source_binary):
    """True when ``apprun`` is a valid extraction of ``source_binary``.

    Reuse is gated on the marker file written when extraction completed, not
    on a comparison of the two mtimes: ``--appimage-extract`` preserves
    AppRun's embedded build timestamp (from when darktable was built),
    whereas the source AppImage carries a fresh download-time mtime, so
    AppRun is typically OLDER than the source it was extracted from. An
    mtime comparison would therefore invalidate every fresh extraction on
    its next reuse and re-unpack the ~500 MB tree per photo. Instead we
    persist the source's fingerprint at extraction time and reuse only
    while it still matches, so an in-place source replacement (in-place
    Vireo update, manual overwrite) still invalidates the tree.

    A missing or unreadable marker counts as "not reusable" so a partial
    extraction, a tree seeded outside this module, or a marker cleared
    on disk all force a rebuild rather than silently reusing state whose
    provenance is unknown.
    """
    try:
        if not (os.path.isfile(apprun) and os.access(apprun, os.X_OK)):
            return False
        with open(_marker_path_for_apprun(apprun)) as f:
            stored = f.read().strip()
        return bool(stored) and stored == _source_fingerprint(source_binary)
    except OSError:
        return False


def _extract_appimage_once(binary):
    """Extract ``binary`` once and return the path to its AppRun, or None.

    Called on the FUSE-less fallback path: instead of setting
    APPIMAGE_EXTRACT_AND_RUN=1 on every develop_photo() invocation — which
    makes the AppImage runtime unpack the whole ~178 MB squashfs into a temp
    dir and delete it again for every photo — we run --appimage-extract once
    at ``<binary>.extracted/squashfs-root/`` and invoke that AppRun for every
    photo thereafter. AppRun sets up CAMLIBS/IOLIBS/GIO_EXTRA_MODULES for the
    extracted binary just as the AppImage runtime does for the mounted one,
    so the resulting darktable-cli has the same environment either way.

    Reuses an existing extraction if AppRun is still there, executable, and
    the persisted source fingerprint still matches; a source AppImage
    replaced in place (Vireo installed a newer build, manual overwrite)
    invalidates the tree and re-extracts. Returns ``None`` when the
    extraction cannot be produced (subprocess failure, unwritable dir,
    truncated output); the caller falls back to APPIMAGE_EXTRACT_AND_RUN=1
    on that binary so develop still succeeds, just at the per-photo cost.
    """
    extract_dir = binary + ".extracted"
    apprun = _apprun_path_for(binary)

    # Fast path — a valid tree from a prior call satisfies the request
    # without touching the lock. Two workers racing on this fast path is
    # safe because both would read the same on-disk state.
    if _is_reusable_apprun(apprun, binary):
        return apprun

    # Serialize the delete → extract → publish sequence per binary. Without
    # this, two concurrent develop jobs both observe an empty extraction,
    # both rmtree() the shared directory, both write into it, and one can
    # end up executing the other's partial output. A per-binary Lock means
    # different binaries still extract in parallel.
    with _get_extract_lock(binary):
        # Re-check under the lock: another worker may have finished the
        # extraction while we were blocked, in which case we reuse it.
        if _is_reusable_apprun(apprun, binary):
            return apprun

        parent_dir = os.path.dirname(extract_dir) or "."
        if not _has_space_for_extraction(binary, parent_dir):
            log.warning(
                "Not enough free disk in %s to unpack %s (~%.1fx its size); "
                "falling back to per-photo APPIMAGE_EXTRACT_AND_RUN",
                parent_dir, binary, _APPIMAGE_EXTRACTION_SIZE_MULTIPLIER,
            )
            return None

        # Stale or absent — clear anything left behind (a half-finished
        # previous extraction, or a tree from an older AppImage version)
        # before extracting fresh, so a partial write cannot masquerade as
        # a valid AppRun.
        shutil.rmtree(extract_dir, ignore_errors=True)
        try:
            os.makedirs(extract_dir, exist_ok=True)
        except OSError as e:
            log.warning("Could not create AppImage extract dir %s: %s", extract_dir, e)
            return None

        try:
            result = subprocess.run(
                [binary, "--appimage-extract"],
                capture_output=True, text=True,
                timeout=_APPIMAGE_EXTRACT_TIMEOUT_SECONDS,
                cwd=extract_dir, **no_window_kwargs(),
            )
        except (subprocess.TimeoutExpired, OSError) as e:
            log.warning("AppImage --appimage-extract failed for %s: %s", binary, e)
            shutil.rmtree(extract_dir, ignore_errors=True)
            return None

        if (
            result.returncode != 0
            or not os.path.isfile(apprun)
            or not os.access(apprun, os.X_OK)
        ):
            log.warning(
                "AppImage --appimage-extract left no usable AppRun at %s "
                "(exit=%s)", apprun, result.returncode,
            )
            shutil.rmtree(extract_dir, ignore_errors=True)
            return None

        # Record the source's fingerprint so _is_reusable_apprun can decide
        # whether this tree still matches without comparing AppRun's mtime
        # (which the extract runtime preserves as darktable's build
        # timestamp and is therefore older than the source AppImage). If
        # the marker cannot be written the tree is unsafe to reuse, so
        # discard it rather than let a subsequent call read a stale or
        # absent marker as "the source must have changed" and re-unpack
        # every photo.
        try:
            with open(
                os.path.join(extract_dir, _APPIMAGE_SOURCE_MARKER), "w"
            ) as f:
                f.write(_source_fingerprint(binary))
        except OSError as e:
            log.warning(
                "AppImage extraction succeeded but source-fingerprint marker "
                "could not be written for %s: %s; discarding tree so the "
                "caller falls back to per-photo extraction",
                binary, e,
            )
            shutil.rmtree(extract_dir, ignore_errors=True)
            return None
        return apprun


def _format_subprocess_diag(stdout, stderr):
    """Combine stdout and stderr into a single short diagnostic string.

    Prefers whichever stream carries output. When both are present, labels
    them so readers know which channel each line came from. Caps total length
    at the last _DIAG_MAX_CHARS characters.
    """
    out = (stdout or "").strip()
    err = (stderr or "").strip()
    if out and err:
        combined = f"stdout: {out}\nstderr: {err}"
    else:
        combined = out or err
    if len(combined) > _DIAG_MAX_CHARS:
        combined = "…" + combined[-_DIAG_MAX_CHARS:]
    return combined


def _mtime_or_missing(path):
    """mtime of ``path``, or -1 if it cannot be stat'd.

    Keeps ``darktable_search_paths`` from raising when a file disappears
    between the listdir that found it and the sort that orders it.
    """
    try:
        return os.path.getmtime(path)
    except OSError:
        return -1.0


def darktable_tools_dir():
    """Where Vireo installs darktable itself; probed by darktable_search_paths.

    A function rather than a module constant so tests (and any caller that
    relocates HOME) can patch os.path.expanduser and still be obeyed; a
    constant would freeze the expansion at import time.

    Built with os.path.join, not by embedding "/" in the literal, so Windows
    gets native backslashes throughout: the settings panel shows this path in
    the "checked here" list, and the mixed-separator form from expanding a
    forward-slash literal reads as broken.
    """
    return os.path.join(os.path.expanduser("~"), ".vireo", "tools", "darktable")


def darktable_uses_tools_dir():
    """True on the platforms where we install darktable into our own tools dir.

    darktable_search_paths() branches on this same predicate, so the two cannot
    drift: callers that need to name the directory (the status route telling
    the user where we looked, the downloader deciding where to write) ask here
    instead of re-deriving the platform test.
    """
    return os.name != "nt" and sys.platform != "darwin"


def darktable_search_paths():
    """Filesystem locations probed for darktable-cli, in priority order.

    These are the *additional* locations ``find_darktable`` checks after the
    configured path and ``shutil.which("darktable-cli")``; both of those take
    precedence and neither appears in this list. The list is platform-specific
    and holds only paths that could plausibly exist on this machine — on Linux
    it is the AppImages actually present in ~/.vireo/tools/darktable, so it is
    empty until we have installed one.

    Exposed so the UI can tell the user where we looked instead of repeating a
    bare "not found". Because $PATH is the probe most likely to explain a miss
    (Homebrew, distro packages), any caller showing this list to a user must
    mention the $PATH probe alongside it rather than presenting these entries
    as the whole search.
    """
    candidates = []
    # Windows is os.name == "nt" *and* sys.platform == "win32", and macOS is
    # neither, so these branches are mutually exclusive on every real platform
    # and their order is immaterial there. The tools-dir branch leads because
    # its predicate is the shared darktable_uses_tools_dir(); that predicate
    # already excludes os.name == "nt", so
    # test_find_darktable_detects_standard_windows_install — which patches
    # os.name on its own, leaving sys.platform as the host's value — still
    # reaches the Windows candidates on a Mac.
    if darktable_uses_tools_dir():
        # Linux: an AppImage we installed ourselves. Newest mtime wins, since
        # installers are kept and this directory accumulates versions.
        #
        # The exec bit is the hand-off marker: a download cancelled during
        # digest verification (or right before hand_off) leaves the AppImage
        # at its final .AppImage path with the default 0o644 mode urllib
        # writes.  Reporting it here would make /api/darktable/status say
        # darktable is installed, hide the Try again affordance, and hand a
        # non-executable file to darktable-cli for every RAW export.  Gating
        # on os.X_OK matches what hand_off actually sets — no separate marker
        # file to drift or clean up.
        tools_dir = darktable_tools_dir()
        if os.path.isdir(tools_dir):
            appimages = [
                os.path.join(tools_dir, n)
                for n in os.listdir(tools_dir)
                if n.endswith(".AppImage")
                and os.access(os.path.join(tools_dir, n), os.X_OK)
            ]
            appimages.sort(key=_mtime_or_missing, reverse=True)
            candidates.extend(appimages)
    elif os.name == "nt":
        for env_var in ("PROGRAMFILES", "PROGRAMFILES(X86)", "PROGRAMW6432"):
            base = os.environ.get(env_var)
            if base:
                candidates.extend([
                    os.path.join(base, "darktable", "bin", "darktable-cli.exe"),
                    os.path.join(base, "darktable", "darktable-cli.exe"),
                ])
    else:
        # macOS: the .app bundle, system-wide or per-user.
        candidates.extend([
            "/Applications/darktable.app/Contents/MacOS/darktable-cli",
            os.path.expanduser("~/Applications/darktable.app/Contents/MacOS/darktable-cli"),
        ])
    return candidates


def find_darktable(configured_path):
    """Find the darktable-cli binary.

    Args:
        configured_path: user-configured path from config, or empty string

    Returns:
        absolute path to darktable-cli, or None if not found

    Note:
        Returned path is resolved via os.path.realpath. On macOS the Homebrew
        cask installs darktable-cli as a symlink at /usr/local/bin/darktable-cli
        pointing into /Applications/darktable.app/Contents/MacOS/. Invoking via
        the symlink dies in dt_init ("can't init develop system") because
        darktable locates its bundled resources (Resources/share/darktable/,
        camera profiles, etc.) by walking up from argv[0]; under /usr/local/bin
        that walk finds nothing. Resolving the symlink first makes every call
        go through the real bundle path.
    """
    if configured_path and os.path.isfile(configured_path):
        return os.path.realpath(configured_path)
    found = shutil.which("darktable-cli")
    if found:
        return os.path.realpath(found)
    for candidate in darktable_search_paths():
        if os.path.isfile(candidate):
            return os.path.realpath(candidate)
    return None


def find_dng_converter(configured_path):
    """Find Adobe DNG Converter or another compatible DNG converter binary."""
    if configured_path:
        if os.path.isfile(configured_path):
            return os.path.realpath(configured_path)
        return None

    candidates = [
        shutil.which("Adobe DNG Converter"),
        shutil.which("Adobe DNG Converter.exe"),
    ]

    # Adobe DNG Converter on Windows has shipped under a few layouts: the
    # current installer drops the binary directly in
    # "Program Files\Adobe DNG Converter", while older 32-bit builds nest it
    # one level deeper under "Program Files (x86)\Adobe\Adobe DNG Converter".
    for env_var in ("PROGRAMFILES", "PROGRAMFILES(X86)", "PROGRAMW6432"):
        program_files = os.environ.get(env_var)
        if not program_files:
            continue
        candidates.append(
            os.path.join(program_files, "Adobe DNG Converter", "Adobe DNG Converter.exe")
        )
        candidates.append(
            os.path.join(
                program_files,
                "Adobe",
                "Adobe DNG Converter",
                "Adobe DNG Converter.exe",
            )
        )

    candidates.append("/Applications/Adobe DNG Converter.app/Contents/MacOS/Adobe DNG Converter")

    for candidate in candidates:
        if candidate and os.path.isfile(candidate):
            return os.path.realpath(candidate)
    return None


def build_command(darktable_bin, input_path, output_path, style=None, width=None):
    """Build the darktable-cli command list.

    Args:
        darktable_bin: path to a darktable-cli binary or to a darktable
            AppImage bundle (detected by content, so its name is irrelevant)
        input_path: path to input RAW file
        output_path: path for output file
        style: optional darktable style name
        width: optional max output width in pixels

    Returns:
        list of command arguments: the binary, then "darktable-cli" when the
        binary is an AppImage, then the input and output paths, then any
        optional flags.
    """
    cmd = [darktable_bin]
    if _is_appimage(darktable_bin):
        # darktable ships a multi-binary AppImage whose AppRun selects the
        # binary from argv[1]. The alternative (a symlink named darktable-cli)
        # does not survive find_darktable's os.path.realpath, which would
        # silently launch the GUI and hang the job.
        cmd.append("darktable-cli")
    cmd.extend([input_path, output_path])
    if style:
        cmd.extend(["--style", style])
    if width:
        cmd.extend(["--width", str(width)])
    return cmd


def output_path_for_photo(filename, output_dir, output_format):
    """Build the output file path for a given photo.

    Args:
        filename: original filename (e.g. "bird.CR3")
        output_dir: directory for developed outputs
        output_format: output format ("jpg" or "tiff")

    Returns:
        full output path
    """
    stem = os.path.splitext(filename)[0]
    return os.path.join(output_dir, f"{stem}.{output_format}")


def _nested_get(metadata, group, tag):
    if not isinstance(metadata, dict):
        return None
    group_data = metadata.get(group)
    if isinstance(group_data, dict) and tag in group_data:
        return group_data.get(tag)
    return None


def _parse_nef_compression(value):
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value).strip().lower()
    if text.isdigit():
        return int(text)
    if "high efficiency" in text:
        return 14 if "*" in text or "star" in text else 13
    return None


def _metadata_nef_compression(metadata):
    for group in ("Nikon", "MakerNotes", "EXIF", "File"):
        value = _nested_get(metadata, group, "NEFCompression")
        parsed = _parse_nef_compression(value)
        if parsed is not None:
            return parsed
    return None


def _read_nef_compression_with_exiftool(input_path):
    try:
        from metadata import extract_metadata
    except ImportError:
        log.debug("metadata module unavailable; cannot read NEFCompression", exc_info=True)
        return None

    extracted = extract_metadata([input_path], restricted_tags=["-NEFCompression"])
    return _metadata_nef_compression(extracted.get(input_path))


def is_nikon_high_efficiency_nef(input_path, metadata=None):
    """Return True when a NEF uses Nikon's darktable-unsupported HE/HE* mode."""
    if os.path.splitext(input_path)[1].lower() != ".nef":
        return False

    compression = _metadata_nef_compression(metadata)
    if compression is None:
        compression = _read_nef_compression_with_exiftool(input_path)
    return compression in _NIKON_HE_COMPRESSION_VALUES


def convert_to_dng(dng_converter_bin, input_path, output_dir):
    """Convert a RAW file to DNG, returning a result dict like develop_photo."""
    binary = find_dng_converter(dng_converter_bin)
    if not binary:
        return {
            "success": False,
            "output_path": "",
            "error": (
                "Adobe DNG Converter not found or not configured. "
                "You will need to download it from Adobe."
            ),
        }

    os.makedirs(output_dir, exist_ok=True)
    stem = os.path.splitext(os.path.basename(input_path))[0]
    output_path = os.path.join(output_dir, f"{stem}.dng")
    if os.path.exists(output_path):
        with contextlib.suppress(OSError):
            os.unlink(output_path)

    cmd = [binary, "-dng1.4", "-d", output_dir, input_path]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=180, **no_window_kwargs()
        )
        if result.returncode != 0:
            diag = _format_subprocess_diag(result.stdout, result.stderr)
            return {
                "success": False,
                "output_path": output_path,
                "error": f"DNG converter exited with code {result.returncode}: {diag}",
            }
        if not os.path.isfile(output_path):
            alt_output_path = os.path.join(output_dir, f"{stem}.DNG")
            if os.path.isfile(alt_output_path):
                output_path = alt_output_path
        if not os.path.isfile(output_path):
            diag = _format_subprocess_diag(result.stdout, result.stderr)
            suffix = f": {diag}" if diag else ""
            return {
                "success": False,
                "output_path": output_path,
                "error": f"DNG converter did not create {os.path.basename(output_path)}{suffix}",
            }
        return {"success": True, "output_path": output_path, "error": None}
    except subprocess.TimeoutExpired:
        return {"success": False, "output_path": output_path, "error": "DNG converter timed out after 180 seconds"}
    except FileNotFoundError:
        return {"success": False, "output_path": output_path, "error": f"DNG converter binary not found at {binary}"}


def develop_photo(
    darktable_bin,
    input_path,
    output_path,
    style=None,
    width=None,
    auto_convert_dng=False,
    dng_converter_bin="",
    metadata=None,
):
    """Develop a single photo using darktable-cli.

    Args:
        darktable_bin: path to darktable-cli (empty string = auto-detect)
        input_path: path to input RAW file
        output_path: path for output file
        style: optional darktable style name
        width: optional max width in pixels

    Returns:
        dict with keys: success (bool), output_path (str), error (str or None)
    """
    binary = find_darktable(darktable_bin)
    if not binary:
        return {"success": False, "output_path": output_path, "error": "darktable-cli not found or not configured"}

    if not os.path.isfile(input_path):
        return {"success": False, "output_path": output_path, "error": f"Input file not found: {input_path}"}

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    darktable_input = input_path
    tmp_dir = None

    if auto_convert_dng and is_nikon_high_efficiency_nef(input_path, metadata=metadata):
        tmp_dir = tempfile.TemporaryDirectory(prefix="vireo-dng-")
        conversion = convert_to_dng(dng_converter_bin, input_path, tmp_dir.name)
        if not conversion["success"]:
            tmp_dir.cleanup()
            return {
                "success": False,
                "output_path": output_path,
                "error": (
                    "Nikon High Efficiency NEF detected, but DNG conversion failed: "
                    f"{conversion['error']}"
                ),
            }
        darktable_input = conversion["output_path"]

    cmd = build_command(binary, darktable_input, output_path, style=style, width=width)

    try:
        log.info("Developing %s -> %s", os.path.basename(input_path), output_path)
        is_appimage = _is_appimage(binary)

        # AppImages need FUSE2 to self-mount, and many current distros ship
        # only FUSE3. APPIMAGE_EXTRACT_AND_RUN=1 makes the runtime bypass FUSE
        # and unpack the whole ~178 MB squashfs into a temp dir — a working
        # workaround when FUSE is missing, but a heavy cost when it is not,
        # because this function runs once per photo. So probe once per binary
        # and cache: try direct execution first, and on a real FUSE failure
        # switch to a persistent extraction (_extract_appimage_once) whose
        # AppRun the batch reuses without unpacking anything again. The old
        # APPIMAGE_EXTRACT_AND_RUN behaviour only survives as a last-resort
        # fallback if the one-time extraction itself fails.
        def _run(cmd_to_run, env):
            return subprocess.run(
                cmd_to_run, capture_output=True, text=True, timeout=120,
                env=env, **no_window_kwargs()
            )

        env = dict(os.environ)
        cmd_to_run = cmd
        cached_mode = _APPIMAGE_MODE_CACHE.get(binary) if is_appimage else None

        if is_appimage and cached_mode == "extracted":
            apprun = _APPIMAGE_APPRUN_CACHE.get(binary)
            # Same predicate as _extract_appimage_once, so a source AppImage
            # replaced in place (in-place Vireo update, manual overwrite)
            # invalidates the cached tree here too — the source-fingerprint
            # marker no longer matches — instead of silently running the
            # old darktable until the process restarts.
            if apprun and _is_reusable_apprun(apprun, binary):
                # Reuse the one-time extraction: swap the AppImage entry point
                # for its AppRun and keep every following argv slot (including
                # the "darktable-cli" selector build_command inserted). No
                # env-var toggle needed — AppRun runs the extracted binary
                # with the full CAMLIBS/IOLIBS/GIO_EXTRA_MODULES setup.
                cmd_to_run = [apprun] + cmd[1:]
            else:
                # Extraction is stale (source replaced) or gone (user cleaned
                # tmp, reinstall, whatever). Reset and re-probe from scratch;
                # _extract_appimage_once will re-run --appimage-extract for
                # the new source under the per-binary lock.
                _APPIMAGE_MODE_CACHE.pop(binary, None)
                _APPIMAGE_APPRUN_CACHE.pop(binary, None)
                cached_mode = None
        elif is_appimage and cached_mode == "extract-per-run":
            env["APPIMAGE_EXTRACT_AND_RUN"] = "1"

        result = _run(cmd_to_run, env)

        if (
            is_appimage
            and cached_mode is None
            and result.returncode != 0
            and _looks_like_appimage_fuse_failure(result.stdout, result.stderr)
        ):
            # FUSE is the problem. Extract once to a persistent tree so this
            # cost is paid a single time for the whole batch, then rerun
            # against the extracted AppRun.
            log.info(
                "AppImage FUSE probe failed for %s; extracting once", binary,
            )
            apprun = _extract_appimage_once(binary)
            if apprun is not None:
                extracted_cmd = [apprun] + cmd[1:]
                result = _run(extracted_cmd, env)
                if result.returncode == 0:
                    _APPIMAGE_MODE_CACHE[binary] = "extracted"
                    _APPIMAGE_APPRUN_CACHE[binary] = apprun
            else:
                # Extraction itself failed — dir not writable, subprocess
                # blew up, etc. Fall back to the runtime workaround so
                # develop still succeeds; the cost is per-photo unpacking,
                # but that beats reporting the whole batch as broken.
                log.warning(
                    "AppImage extraction failed for %s; falling back to "
                    "APPIMAGE_EXTRACT_AND_RUN per photo", binary,
                )
                env["APPIMAGE_EXTRACT_AND_RUN"] = "1"
                result = _run(cmd, env)
                if result.returncode == 0:
                    _APPIMAGE_MODE_CACHE[binary] = "extract-per-run"
        elif is_appimage and cached_mode is None and result.returncode == 0:
            _APPIMAGE_MODE_CACHE[binary] = "direct"

        if result.returncode != 0:
            # darktable-cli writes init/IO failures to stdout, not stderr.
            diag = _format_subprocess_diag(result.stdout, result.stderr)
            return {
                "success": False,
                "output_path": output_path,
                "error": f"darktable-cli exited with code {result.returncode}: {diag}",
            }
        if not os.path.isfile(output_path):
            return {"success": False, "output_path": output_path, "error": "Output file was not created"}
        return {"success": True, "output_path": output_path, "error": None}
    except subprocess.TimeoutExpired:
        return {"success": False, "output_path": output_path, "error": "darktable-cli timed out after 120 seconds"}
    except FileNotFoundError:
        return {"success": False, "output_path": output_path, "error": f"darktable-cli binary not found at {binary}"}
    finally:
        if tmp_dir is not None:
            tmp_dir.cleanup()
