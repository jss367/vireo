"""Wrapper for darktable-cli to develop RAW photos."""

import contextlib
import logging
import os
import shutil
import subprocess
import tempfile

try:
    from .proc import no_window_kwargs
except ImportError:
    from proc import no_window_kwargs

log = logging.getLogger(__name__)

_DIAG_MAX_CHARS = 500
_NIKON_HE_COMPRESSION_VALUES = {13, 14}


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
        darktable_bin: path to darktable-cli binary
        input_path: path to input RAW file
        output_path: path for output file
        style: optional darktable style name
        width: optional max output width in pixels

    Returns:
        list of command arguments
    """
    cmd = [darktable_bin, input_path, output_path]
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
    except Exception:
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
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=120, **no_window_kwargs()
        )
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
