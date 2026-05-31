"""Capture-time and timezone repair helpers."""

import json
import logging
import os
import re
import shutil
import subprocess
from datetime import datetime, timedelta

from metadata import extract_metadata

log = logging.getLogger(__name__)

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
    if minutes > 59:
        return None
    if sign == "+" and (hours > 14 or (hours == 14 and minutes > 0)):
        return None
    if sign == "-" and (hours > 12 or (hours == 12 and minutes > 0)):
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


def _normalize_subsec(subsec):
    if subsec is None:
        return None
    digits = "".join(ch for ch in str(subsec).strip() if ch.isdigit())
    if not digits:
        return None
    return int(digits[:6].ljust(6, "0"))


def _normalize_inputs(mode, target_offset, shift_minutes):
    """Validate user inputs and return (target_minutes, manual_shift, target_offset_str).

    In ``manual`` mode ``target_offset`` is intentionally ignored so a stale
    or prefilled value can't silently overwrite ``OffsetTime*`` tags when the
    user only asked for a minute shift.
    """
    if mode == "manual":
        try:
            manual_shift = int(shift_minutes or 0)
        except (TypeError, ValueError) as exc:
            raise ValueError("shift_minutes must be an integer") from exc
        target_minutes = None
        target_offset_str = None
    elif mode == "preserve_instant":
        target_minutes = parse_offset_minutes(target_offset)
        if target_minutes is None:
            raise ValueError("target_offset is required for preserve_instant")
        manual_shift = None
        target_offset_str = validate_offset(target_offset)
    else:
        raise ValueError("mode must be preserve_instant or manual")
    return target_minutes, manual_shift, target_offset_str


def _photo_shift_minutes(mode, target_minutes, manual_shift, photo):
    """Return the signed minute shift for a single photo, or raise ValueError."""
    if mode == "manual":
        return manual_shift
    current_minutes = parse_offset_minutes(_capture_offset(photo))
    if current_minutes is None:
        raise ValueError("current offset not found; use a manual time shift")
    return target_minutes - current_minutes


def _offset_tags_already_target(photo, target_offset):
    """Return True when all OffsetTime* tags are present and match target."""
    if not target_offset:
        return True
    exif = _exif_group(photo)
    values = [
        exif.get("OffsetTime"),
        exif.get("OffsetTimeOriginal"),
        exif.get("OffsetTimeDigitized"),
    ]
    return all(value and str(value).strip() == target_offset for value in values)


def _is_noop_adjustment(mode, target_offset, photo_shift, photo):
    if photo_shift != 0:
        return False
    if mode == "manual":
        return True
    return _offset_tags_already_target(photo, target_offset)


def format_preview_timestamp(dt):
    if dt is None:
        return None
    base = dt.strftime("%Y-%m-%d %H:%M:%S")
    if dt.microsecond:
        frac = f"{dt.microsecond:06d}".rstrip("0")
        return f"{base}.{frac}"
    return base


def build_capture_time_preview(photos, *, mode, target_offset=None, shift_minutes=None, limit=5):
    """Return sample before/after rows for the selected photos.

    For ``preserve_instant`` each sampled photo gets its own shift based on its
    own current offset, so a selection with mixed offsets is shown accurately
    instead of being collapsed onto a single shift derived from the first photo.
    """
    sample = list(photos)[: max(1, int(limit))]
    target_minutes, manual_shift, target_offset_str = _normalize_inputs(
        mode, target_offset, shift_minutes
    )

    rows = []
    shifts_seen = []
    for photo in sample:
        dt = _capture_datetime(photo)
        current_offset = _capture_offset(photo)
        try:
            shift = _photo_shift_minutes(mode, target_minutes, manual_shift, photo)
        except ValueError:
            shift = None
        after = (
            dt + timedelta(minutes=shift)
            if (dt is not None and shift is not None)
            else None
        )
        if shift is not None:
            shifts_seen.append(shift)
        rows.append(
            {
                "photo_id": photo["id"],
                "filename": photo["filename"],
                "before_time": format_preview_timestamp(dt),
                "before_offset": current_offset,
                "after_time": format_preview_timestamp(after),
                "after_offset": target_offset_str or current_offset,
                "shift_minutes": shift,
            }
        )

    if mode == "preserve_instant" and not shifts_seen:
        raise ValueError("current offset not found; use a manual time shift")

    unanimous = bool(shifts_seen) and all(s == shifts_seen[0] for s in shifts_seen)
    summary_shift = shifts_seen[0] if unanimous else None

    return {
        "mode": mode,
        "shift_minutes": summary_shift,
        "shifts_vary": not unanimous and len(shifts_seen) > 1,
        "target_offset": target_offset_str,
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
        microsecond = _normalize_subsec(subsec)
        if microsecond is not None:
            dt = dt.replace(microsecond=microsecond)
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
    update_clause = ", ".join(updates)
    db.conn.execute("UPDATE photos SET " + update_clause + " WHERE id=?", params)


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

    target_minutes, manual_shift, target_offset_str = _normalize_inputs(
        mode, target_offset, shift_minutes
    )

    photos = []
    for pid in photo_ids:
        row = db.get_photo(pid, verify_workspace=True)
        if row:
            photos.append(row)
    if not photos:
        raise ValueError("no photos found")

    log.info("Starting capture-time adjustment for %d photo(s)", len(photos))
    written = 0
    skipped = 0
    failed = 0
    failures = []
    total = len(photos)
    shifts_used = []
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

        try:
            photo_shift = _photo_shift_minutes(mode, target_minutes, manual_shift, photo)
        except ValueError as exc:
            failed += 1
            failures.append(
                {"photo_id": photo["id"], "filename": photo["filename"], "error": str(exc)}
            )
            if progress_callback:
                progress_callback(index, total, photo["filename"])
            continue

        has_companion = _has_key(photo, "companion_path") and bool(photo["companion_path"])
        if (
            not has_companion
            and _is_noop_adjustment(mode, target_offset_str, photo_shift, photo)
        ):
            skipped += 1
            shifts_used.append(photo_shift)
            if progress_callback:
                progress_callback(index, total, photo["filename"])
            continue

        cmd = ["exiftool"]
        if not keep_backups:
            cmd.append("-overwrite_original")
        if photo_shift:
            cmd.append(_shift_arg(photo_shift))
        if target_offset_str:
            cmd.extend(
                [
                    f"-OffsetTime={target_offset_str}",
                    f"-OffsetTimeOriginal={target_offset_str}",
                    f"-OffsetTimeDigitized={target_offset_str}",
                ]
            )
        cmd.append("--")
        cmd.extend(paths)

        try:
            log.info(
                "Running ExifTool capture-time adjustment for photo %s on %d file(s)",
                photo["id"],
                len(paths),
            )
            log.debug("ExifTool command: %s", cmd)
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
            log.debug("ExifTool stdout: %s", result.stdout.strip())
            if result.stderr:
                log.debug("ExifTool stderr: %s", result.stderr.strip())
            written += 1
            shifts_used.append(photo_shift)
        except (subprocess.TimeoutExpired, RuntimeError, OSError) as exc:
            log.warning("Capture-time adjustment failed for photo %s: %s", photo["id"], exc)
            failed += 1
            failures.append({"photo_id": photo["id"], "filename": photo["filename"], "error": str(exc)})
        except Exception as exc:
            log.exception("Unexpected capture-time adjustment error for photo %s", photo["id"])
            failed += 1
            failures.append({"photo_id": photo["id"], "filename": photo["filename"], "error": str(exc)})
        if progress_callback:
            progress_callback(index, total, photo["filename"])

    unanimous = bool(shifts_used) and all(s == shifts_used[0] for s in shifts_used)
    summary_shift = shifts_used[0] if unanimous else None
    log.info(
        "Capture-time adjustment finished: %d updated, %d skipped, %d failed",
        written,
        skipped,
        failed,
    )

    return {
        "updated": written,
        "skipped": skipped,
        "failed": failed,
        "failures": failures[:20],
        "shift_minutes": summary_shift,
        "shifts_vary": not unanimous and len(shifts_used) > 1,
        "target_offset": target_offset_str,
        "backup_files": bool(keep_backups),
    }
