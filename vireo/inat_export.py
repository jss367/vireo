"""Prepare edited JPEGs for iNaturalist's browser uploader.

The browser cannot pre-populate a website file picker.  This module provides
the token-free handoff instead: render a new JPEG, embed only the metadata the
user selected, and leave the source photo untouched.
"""

from __future__ import annotations

import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
from contextlib import suppress
from pathlib import Path

from export import export_photos, sanitize_filename
from file_replace import replace_file
from metadata import _exiftool_command, find_exiftool
from proc import no_window_kwargs

_TIMESTAMP_RE = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})"
    r"(?:[T ](?P<time>\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?))?"
    r"(?P<offset>Z|[+-]\d{2}:?\d{2})?$"
)


class InatExportError(RuntimeError):
    """Raised when an iNaturalist-ready JPEG cannot be produced."""


def _exif_datetime(value: str | None) -> tuple[str | None, str | None]:
    """Convert an ISO-ish catalog timestamp to ExifTool date and offset."""
    if not value:
        return None, None
    match = _TIMESTAMP_RE.match(str(value).strip())
    if not match:
        return None, None
    date_part = match.group("date").replace("-", ":")
    time_part = match.group("time") or "00:00:00"
    time_part = time_part.split(".", 1)[0]
    if len(time_part) == 5:
        time_part += ":00"
    offset = match.group("offset")
    if offset == "Z":
        offset = "+00:00"
    elif offset and len(offset) == 5:
        offset = f"{offset[:3]}:{offset[3:]}"
    return f"{date_part} {time_part}", offset


def _metadata_args(metadata: dict) -> list[str]:
    """Return ExifTool assignments for the explicitly included fields."""
    args = ["-all="]

    taxon = str(metadata.get("taxon_name") or "").strip()
    if taxon:
        # iNaturalist recognizes taxon names in common title/keyword fields.
        # Write both IPTC and XMP forms for compatibility with its uploader.
        args.extend([
            f"-IPTC:ObjectName={taxon}",
            f"-XMP-dc:Title={taxon}",
            f"-XMP-dc:Subject={taxon}",
        ])

    timestamp = str(metadata.get("timestamp") or "").strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", timestamp):
        args.append(f"-IPTC:DateCreated={timestamp.replace('-', ':')}")
    else:
        exif_date, offset = _exif_datetime(timestamp)
        if exif_date:
            args.extend([
                f"-EXIF:DateTimeOriginal={exif_date}",
                f"-EXIF:CreateDate={exif_date}",
            ])
            if offset:
                args.extend([
                    f"-EXIF:OffsetTimeOriginal={offset}",
                    f"-EXIF:OffsetTimeDigitized={offset}",
                ])

    latitude = metadata.get("latitude")
    longitude = metadata.get("longitude")
    if latitude is not None and longitude is not None:
        latitude = float(latitude)
        longitude = float(longitude)
        args.extend([
            f"-EXIF:GPSLatitude={abs(latitude):.8f}",
            f"-EXIF:GPSLatitudeRef={'N' if latitude >= 0 else 'S'}",
            f"-EXIF:GPSLongitude={abs(longitude):.8f}",
            f"-EXIF:GPSLongitudeRef={'E' if longitude >= 0 else 'W'}",
        ])

    description = str(metadata.get("description") or "").strip()
    if description:
        args.extend([
            f"-IPTC:Caption-Abstract={description}",
            f"-XMP-dc:Description={description}",
        ])
    return args


def write_inat_metadata(path: str, metadata: dict) -> None:
    """Replace metadata on ``path`` with the selected iNaturalist fields."""
    exiftool = find_exiftool()
    if not exiftool:
        raise InatExportError(
            "ExifTool is required to create an iNaturalist-ready JPEG. "
            "Install or repair ExifTool in Settings."
        )
    command = [
        *_exiftool_command(exiftool),
        "-overwrite_original",
        *_metadata_args(metadata),
        "--",
        path,
    ]
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=120,
            **no_window_kwargs(),
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise InatExportError(f"Could not write photo metadata: {exc}") from exc
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "ExifTool failed").strip()
        raise InatExportError(f"Could not write photo metadata: {detail}")


def _destination_name_limit(destination: str) -> int:
    """Return the destination filesystem's filename-component limit."""
    try:
        return int(os.pathconf(destination or os.curdir, "PC_NAME_MAX"))
    except (AttributeError, OSError, ValueError):
        return 255


def _truncate_utf8(value: str, max_bytes: int) -> str:
    """Truncate text without splitting a UTF-8 code point."""
    encoded = value.encode("utf-8")
    if len(encoded) <= max_bytes:
        return value
    return encoded[:max_bytes].decode("utf-8", errors="ignore")


def _reserve_destination(path: str, *, preserve_suffix: str = "") -> str:
    """Atomically reserve a deduplicated destination with an empty file."""
    destination = os.path.dirname(path)
    stem, extension = os.path.splitext(os.path.basename(path))
    if preserve_suffix and stem.endswith(preserve_suffix):
        variable_stem = stem[:-len(preserve_suffix)]
    else:
        variable_stem = stem
        preserve_suffix = ""
    name_limit = _destination_name_limit(destination)
    index = 1
    while True:
        dedupe_suffix = "" if index == 1 else f"_{index}"
        fixed_name = f"{preserve_suffix}{dedupe_suffix}{extension}"
        max_stem_bytes = name_limit - len(fixed_name.encode("utf-8"))
        if max_stem_bytes < 1:
            raise OSError("Destination filename limit is too small")
        bounded_stem = _truncate_utf8(variable_stem, max_stem_bytes)
        candidate = os.path.join(
            destination,
            f"{bounded_stem}{fixed_name}",
        )
        try:
            fd = os.open(
                candidate,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                0o600,
            )
        except FileExistsError:
            index += 1
            continue
        os.close(fd)
        return candidate


def export_inat_photo(
    db,
    vireo_dir: str,
    photo_id: int,
    destination: str,
    metadata: dict,
    *,
    quality: int = 92,
    working_copy_max_size: int = 4096,
    developed_dir: str = "",
) -> str:
    """Render one photo, add selected metadata, and return its final path."""
    photo = db.get_photo(photo_id, verify_workspace=True)
    if not photo:
        raise InatExportError("Photo not found in the active workspace")

    os.makedirs(destination, exist_ok=True)
    temp_root = os.path.join(vireo_dir, "inat-exports")
    os.makedirs(temp_root, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="export-", dir=temp_root) as temp_dir:
        result = export_photos(
            db=db,
            vireo_dir=vireo_dir,
            photo_ids=[photo_id],
            destination=temp_dir,
            options={
                "naming_template": "{original}",
                "format": "jpg",
                "quality": quality,
                "working_copy_max_size": working_copy_max_size,
                "developed_dir": developed_dir,
                "collect_files": True,
            },
        )
        files = result.get("files") or []
        if not files:
            detail = "; ".join(result.get("errors") or [])
            raise InatExportError(detail or "Photo could not be rendered")

        rendered_path = files[0]
        write_inat_metadata(rendered_path, metadata)

        original_stem = Path(photo["filename"]).stem
        filename = sanitize_filename(f"{original_stem}-iNaturalist.jpg")
        final_path = None
        staged_path = None
        try:
            final_path = _reserve_destination(
                os.path.join(destination, filename),
                preserve_suffix="-iNaturalist",
            )
            stage_fd, staged_path = tempfile.mkstemp(
                prefix=".inat-", suffix=".tmp", dir=destination,
            )
            os.close(stage_fd)
            shutil.copy2(rendered_path, staged_path)
            # Preserve the normal umask-derived mode of the rendered export;
            # mkstemp starts the staging file at owner-only permissions.
            os.chmod(
                staged_path,
                stat.S_IMODE(os.stat(rendered_path).st_mode),
            )
            replace_file(staged_path, final_path)
            staged_path = None
        except OSError as exc:
            for cleanup_path in (staged_path, final_path):
                if cleanup_path:
                    with suppress(FileNotFoundError):
                        os.unlink(cleanup_path)
            raise InatExportError(f"Could not save exported photo: {exc}") from exc
    return final_path


def reveal_inat_exports(paths: list[str], destination: str) -> bool:
    """Reveal exported JPEGs in the platform file manager."""
    target = paths[0] if len(paths) == 1 else destination
    try:
        if sys.platform == "darwin":
            command = ["open", "-R", "--", target]
        elif sys.platform.startswith("win"):
            command = (
                ["explorer", f"/select,{target}"]
                if len(paths) == 1
                else ["explorer", destination]
            )
        else:
            folder = destination if len(paths) != 1 else os.path.dirname(target)
            command = ["xdg-open", os.path.abspath(folder)]
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
            **no_window_kwargs(),
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    # Explorer commonly returns 1 after successfully dispatching a window.
    return sys.platform.startswith("win") or result.returncode == 0
