"""Capture-time and timezone repair helpers."""

import json
import os
import re
import shutil
import subprocess
from datetime import datetime, timedelta

from metadata import extract_metadata

OFFSET_RE = re.compile(r"^([+-])(\d{2}):(\d{2})$")


def parse_offset_minutes(value):
    """Return UTC offset minutes for '+HH:MM' / '-HH:MM', or None."""
    if value is None:
        return None
    match = OFFSET_RE.match(str(value).strip())
    if not match:
        return None
    sign, hh, mm = match.groups()
    hours = int(hh)
    minutes = int(mm)
    if hours > 14 or minutes > 59:
        return None
    total = hours * 60 + minutes
    return -total if sign == "-" else total


def validate_offset(value):
    if parse_offset_minutes(value) is None:
        raise ValueError("offset must use +HH:MM or -HH:MM")
    return str(value).strip()


def _has_key(row, key):
    keys = row.keys() if hasattr(row, "keys") else row
    return key in keys


def _exif_group(photo):
    raw = photo["exif_data"] if _has_key(photo, "exif_data") else None
    if not raw:
        return {}
    try:
        metadata = json.loads(raw)
    except (TypeError, ValueError):
        return {}
    if not isinstance(metadata, dict):
        return {}
    exif = metadata.get("EXIF")
    return exif if isinstance(exif, dict) else {}


def _capture_datetime(photo):
    exif = _exif_group(photo)
    value = exif.get("DateTimeOriginal") or exif.get("CreateDate")
    if value:
        try:
            dt = datetime.strptime(str(value), "%Y:%m:%d %H:%M:%S")
            subsec = exif.get("SubSecTimeOriginal") or exif.get("SubSecTime")
            if subsec is not None:
                digits = "".join(ch for ch in str(subsec).strip() if ch.isdigit())
                if digits:
                    dt = dt.replace(microsecond=int(digits[:6].ljust(6, "0")))
            return dt
        except (TypeError, ValueError):
            pass

    value = photo["timestamp"] if _has_key(photo, "timestamp") else None
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def _capture_offset(photo):
    exif = _exif_group(photo)
    return (
        exif.get("OffsetTimeOriginal")
        or exif.get("OffsetTime")
        or exif.get("OffsetTimeDigitized")
    )


def resolve_shift_minutes(mode, target_offset=None, shift_minutes=None, sample_photos=None):
    """Resolve user intent into a signed minute shift."""
    if mode == "manual":
        try:
            return int(shift_minutes or 0)
        except (TypeError, ValueError) as exc:
            raise ValueError("shift_minutes must be an integer") from exc

    if mode != "preserve_instant":
        raise ValueError("mode must be preserve_instant or manual")

    target_minutes = parse_offset_minutes(target_offset)
    if target_minutes is None:
        raise ValueError("target_offset is required for preserve_instant")

    sample_photos = sample_photos or []
    for photo in sample_photos:
        current_minutes = parse_offset_minutes(_capture_offset(photo))
        if current_minutes is not None:
            return target_minutes - current_minutes

    raise ValueError("current offset not found; use a manual time shift")


def format_preview_timestamp(dt):
    if dt is None:
        return None
    base = dt.strftime("%Y-%m-%d %H:%M:%S")
    if dt.microsecond:
        frac = f"{dt.microsecond:06d}".rstrip("0")
        return f"{base}.{frac}"
    return base


def build_capture_time_preview(photos, *, mode, target_offset=None, shift_minutes=None, limit=5):
    """Return sample before/after rows for the selected photos."""
    sample = list(photos)[: max(1, int(limit))]
    resolved_shift = resolve_shift_minutes(
        mode,
        target_offset=target_offset,
        shift_minutes=shift_minutes,
        sample_photos=sample,
    )
    target_offset = validate_offset(target_offset) if target_offset else None

    rows = []
    for photo in sample:
        dt = _capture_datetime(photo)
        current_offset = _capture_offset(photo)
        after = dt + timedelta(minutes=resolved_shift) if dt is not None else None
        rows.append(
            {
                "photo_id": photo["id"],
                "filename": photo["filename"],
                "before_time": format_preview_timestamp(dt),
                "before_offset": current_offset,
                "after_time": format_preview_timestamp(after),
                "after_offset": target_offset or current_offset,
            }
        )

    return {
        "mode": mode,
        "shift_minutes": resolved_shift,
        "target_offset": target_offset,
        "samples": rows,
    }


def _shift_arg(minutes):
    op = "+=" if minutes >= 0 else "-="
    total = abs(int(minutes))
    hours, mins = divmod(total, 60)
    return f"-AllDates{op}0:0:0 {hours}:{mins}:0"


def _photo_paths(db, photo):
    folder = db.conn.execute(
        "SELECT path FROM folders WHERE id = ?", (photo["folder_id"],)
    ).fetchone()
    if not folder:
        return []
    paths = [os.path.join(folder["path"], photo["filename"])]
    companion = photo["companion_path"] if _has_key(photo, "companion_path") else None
    if companion:
        paths.append(os.path.join(folder["path"], companion))
    return paths


def _timestamp_from_exif_group(exif_group):
    dto = exif_group.get("DateTimeOriginal") or exif_group.get("CreateDate")
    if not dto:
        return None
    try:
        dt = datetime.strptime(str(dto), "%Y:%m:%d %H:%M:%S")
        subsec = exif_group.get("SubSecTimeOriginal") or exif_group.get("SubSecTime")
        if subsec is not None:
            subsec_str = str(subsec).strip()
            if subsec_str.isdigit():
                dt = dt.replace(microsecond=int(subsec_str[:6].ljust(6, "0")))
        return dt.isoformat()
    except (TypeError, ValueError):
        return None


def _refresh_photo_metadata(db, photo_id, primary_path):
    metadata = extract_metadata([primary_path]).get(primary_path)
    if not metadata:
        return
    exif = metadata.get("EXIF", {})
    timestamp = _timestamp_from_exif_group(exif)
    file_mtime = os.path.getmtime(primary_path) if os.path.exists(primary_path) else None

    updates = ["exif_data=?"]
    params = [json.dumps(metadata)]
    if timestamp is not None:
        updates.append("timestamp=?")
        params.append(timestamp)
    if file_mtime is not None:
        updates.append("file_mtime=?")
        params.append(file_mtime)
    params.append(photo_id)
    db.conn.execute(f"UPDATE photos SET {', '.join(updates)} WHERE id=?", params)


def adjust_capture_time(
    db,
    photo_ids,
    *,
    mode,
    target_offset=None,
    shift_minutes=None,
    keep_backups=True,
    progress_callback=None,
    cancel_check=None,
):
    """Apply a capture-time correction to selected photos via ExifTool."""
    if shutil.which("exiftool") is None:
        raise RuntimeError("exiftool is not installed")

    photos = []
    for pid in photo_ids:
        row = db.get_photo(pid, verify_workspace=True)
        if row:
            photos.append(row)
    if not photos:
        raise ValueError("no photos found")

    preview = build_capture_time_preview(
        photos,
        mode=mode,
        target_offset=target_offset,
        shift_minutes=shift_minutes,
        limit=1,
    )
    resolved_shift = preview["shift_minutes"]
    target_offset = preview["target_offset"]

    written = 0
    failed = 0
    failures = []
    total = len(photos)
    for index, photo in enumerate(photos, start=1):
        if cancel_check and cancel_check():
            break
        paths = [p for p in _photo_paths(db, photo) if os.path.exists(p)]
        if not paths:
            failed += 1
            failures.append({"photo_id": photo["id"], "error": "source file not found"})
            if progress_callback:
                progress_callback(index, total, photo["filename"])
            continue

        cmd = ["exiftool"]
        if not keep_backups:
            cmd.append("-overwrite_original")
        if resolved_shift:
            cmd.append(_shift_arg(resolved_shift))
        if target_offset:
            cmd.extend(
                [
                    f"-OffsetTime={target_offset}",
                    f"-OffsetTimeOriginal={target_offset}",
                    f"-OffsetTimeDigitized={target_offset}",
                ]
            )
        cmd.append("--")
        cmd.extend(paths)

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=120,
            )
            if result.returncode not in (0, 1):
                raise RuntimeError((result.stderr or result.stdout or "ExifTool failed").strip())
            _refresh_photo_metadata(db, photo["id"], paths[0])
            db.conn.commit()
            written += 1
        except Exception as exc:
            failed += 1
            failures.append({"photo_id": photo["id"], "filename": photo["filename"], "error": str(exc)})
        if progress_callback:
            progress_callback(index, total, photo["filename"])

    return {
        "updated": written,
        "failed": failed,
        "failures": failures[:20],
        "shift_minutes": resolved_shift,
        "target_offset": target_offset,
        "backup_files": bool(keep_backups),
    }
