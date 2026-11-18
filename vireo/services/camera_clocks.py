"""The Audit's Camera Clocks check over the active workspace.

Reads every photo's stored clock evidence (``db.audit.camera_clock_rows``),
groups it by folder and camera, and runs ``camera_clock.analyze_camera`` on
each group with the local time zone at each GPS position (``time_zones``).

A folder with too few GPS fixes to measure its camera's clock borrows the
clock error measured for the same camera in the nearest folder by date, within
``NEARBY_DAYS``. It only ranks the readings offered for that folder: the
finding still shows every reading and says where the hint came from.
"""

from __future__ import annotations

import os
from collections import Counter, defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING

import camera_clock
import time_zones
from camera_clock import CLOCK_ERROR_MIN_MINUTES, ClockFrame
from capture_time import capture_time_token

if TYPE_CHECKING:
    import sqlite3

    from db import Database
    from services.service_types import JsonDict

NEARBY_DAYS = 21


@dataclass(frozen=True)
class _FolderClock:
    folder_id: int
    folder_path: str
    camera: str | None
    day: date
    error: int


def _parse_exif_time(value: object) -> datetime | None:
    """``'2026:01:25 12:53:59'`` (any trailing fraction or zone ignored) -> datetime."""
    if not isinstance(value, str) or len(value) < 19:
        return None
    try:
        return datetime.strptime(value[:19], "%Y:%m:%d %H:%M:%S")
    except ValueError:
        return None


def _with_subseconds(captured: datetime, subsec: object) -> datetime:
    digits = "".join(ch for ch in str(subsec) if ch.isdigit()) if subsec is not None else ""
    if not digits:
        return captured
    return captured.replace(microsecond=int(digits[:6].ljust(6, "0")))


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def _camera_name(row: sqlite3.Row) -> str | None:
    model = str(row["model"]).strip() if row["model"] else ""
    make = str(row["make"]).strip() if row["make"] else ""
    name = model or make
    if row["serial"] not in (None, ""):
        return f"{name or 'Camera'} #{row['serial']}"
    return name or None


def frame_from_row(row: sqlite3.Row) -> ClockFrame | None:
    """A ``ClockFrame`` from one ``camera_clock_rows`` row, or None without a capture time."""
    captured = _parse_exif_time(row["date_time_original"] or row["create_date"])
    if captured is None:
        return None
    shutter = _number(row["shutter_count"])
    latitude, longitude = _number(row["gps_latitude"]), _number(row["gps_longitude"])
    return ClockFrame(
        ref=row["id"],
        filename=row["filename"],
        captured=_with_subseconds(captured, row["subsec"] or row["subsec_fallback"]),
        offset_minutes=camera_clock.parse_offset(row["offset_original"] or row["offset_time"] or row["offset_digitized"]),
        camera=_camera_name(row),
        shutter_count=int(shutter) if shutter is not None else None,
        gps_utc=_parse_exif_time(row["gps_date_time"]),
        latitude=latitude,
        longitude=longitude,
    )


def _main_day(frames: Iterable[ClockFrame]) -> date:
    return Counter(f.captured.date() for f in frames).most_common(1)[0][0]


def _nearby_error(measured: list[_FolderClock], folder_id: int, day: date) -> tuple[int, str] | None:
    candidates = [
        m for m in measured
        if m.folder_id != folder_id and abs((m.day - day).days) <= NEARBY_DAYS
    ]
    if not candidates:
        return None
    best = min(candidates, key=lambda m: abs((m.day - day).days))
    return best.error, f"{best.day.isoformat()} ({os.path.basename(best.folder_path) or best.folder_path})"


def check_camera_clocks(db: Database) -> JsonDict:
    """Every camera-clock problem in the active workspace, one per folder and camera.

    Returns ``{"findings": [...], "location_available": bool}``. Each finding
    carries ``folder_id``, ``folder_path`` and the fields of
    ``ClockProblem.to_json`` with photo ids as each fix's ``refs``.
    """
    groups: dict[tuple[int, str | None], list[ClockFrame]] = defaultdict(list)
    folder_paths: dict[int, str] = {}
    states = {}
    for row in db.audit.camera_clock_rows():
        states[row["id"]] = (row["id"], row["timestamp"], row["file_mtime"], row["file_hash"], row["clock_state"])
        frame = frame_from_row(row)
        if frame is None:
            continue
        groups[(row["folder_id"], frame.camera)].append(frame)
        folder_paths[row["folder_id"]] = row["folder_path"]

    measured_by_camera: dict[str | None, list[_FolderClock]] = defaultdict(list)
    for (folder_id, camera), frames in groups.items():
        estimate = camera_clock.estimate_clock_error(frames)
        if estimate is None or any(jump.kind == "reset" for run in camera_clock.shot_sequences(frames)
                                   for jump in camera_clock.find_jumps(run)):
            continue
        error = estimate[0] if abs(estimate[0]) >= CLOCK_ERROR_MIN_MINUTES else 0
        measured_by_camera[camera].append(
            _FolderClock(folder_id, folder_paths[folder_id], camera, _main_day(frames), error)
        )

    location = time_zones.available()
    zone_lookup = time_zones.local_zone if location else None
    findings = []
    for (folder_id, camera), frames in groups.items():
        nearby = None
        if camera is not None:
            nearby = _nearby_error(measured_by_camera.get(camera, []), folder_id, _main_day(frames))
        problem = camera_clock.analyze_camera(frames, zone_lookup=zone_lookup, nearby_error=nearby)
        if problem is None:
            continue
        payload = problem.to_json(include_refs=True)
        for fix in payload["fixes"]:
            for step in [fix, *fix.get("dependent_fixes", [])]:
                step["expected_state"] = capture_time_token(states[pid] for pid in step["refs"])
        findings.append({
            "folder_id": folder_id,
            "folder_path": folder_paths[folder_id],
            **payload,
        })
    findings.sort(key=lambda f: (str(f["folder_path"]), str(f["camera"] or "")))
    return {"findings": findings, "location_available": location}
