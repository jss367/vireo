"""Review short, geographically implausible excursions in one camera's shot order."""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Hashable, Iterator
from itertools import groupby, pairwise
from typing import TYPE_CHECKING

from camera_clock import ClockFrame, shot_sequences
from location_review import distance_meters, has_usable_coordinates
from services.camera_clocks import frame_from_row

if TYPE_CHECKING:
    from db import Database
    from services.service_types import JsonDict


def _point(frame: ClockFrame) -> dict[str, float | None]:
    return {"latitude": frame.latitude, "longitude": frame.longitude}


def _elapsed(first: ClockFrame, last: ClockFrame) -> float:
    if first.offset_minutes != last.offset_minutes:
        return -1
    return (last.captured - first.captured).total_seconds()


def _finding(before: ClockFrame, run: list[ClockFrame], after: ClockFrame, folder: str,
             reviews: dict[Hashable, str | None]) -> JsonDict | None:
    first, last = run[0], run[-1]
    old, anchor, end = _point(first), _point(before), _point(after)
    if not all(map(has_usable_coordinates, (old, anchor, end))):
        return None
    outbound, inbound = _elapsed(before, first), _elapsed(last, after)
    if min(outbound, inbound) < 0 or not 0 <= _elapsed(before, after) <= 300:
        return None
    if any(_elapsed(a, b) < 0 for a, b in pairwise(run)):
        return None
    outward, back = distance_meters(anchor, old), distance_meters(old, end)
    if min(outward, back) < 1000 or distance_meters(anchor, end) > 100:
        return None
    if max(outward / max(1, outbound), back / max(1, inbound)) < 100:
        return None
    neighbor = before if outbound < inbound else after
    ids = [frame.ref for frame in run]
    evidence = [folder, before.ref, after.ref, anchor, old, end, ids, neighbor.ref]
    key = hashlib.sha256(json.dumps(evidence).encode()).hexdigest()
    previous_reviews = {reviews.get(pid) for pid in ids}
    return dict(
        kind="excursion",
        key=key,
        ids=ids,
        folder=folder,
        first=first.filename,
        last=last.filename,
        camera=first.camera,
        before=before.filename,
        after=after.filename,
        distance=round(outward),
        seconds=round(outbound, 2),
        back=round(back),
        return_seconds=round(inbound, 2),
        original=old,
        suggested=_point(neighbor),
        neighbor=neighbor.filename,
        reviewed=previous_reviews == {key},
        conflict=previous_reviews not in ({None}, {key}),
    )


def _sequence_findings(sequence: list[ClockFrame], folder: str,
                       reviews: dict[Hashable, str | None]) -> Iterator[JsonDict]:
    runs = [list(run) for _, run in groupby(sequence, lambda f: (f.latitude, f.longitude))]
    explained = set()
    for i in range(1, len(runs) - 1):
        finding = _finding(runs[i - 1][-1], runs[i], runs[i + 1][0], folder, reviews)
        if finding:
            yield finding
            explained.update((i - 1, i))
    for i, (left, right) in enumerate(pairwise(runs)):
        before, after = left[-1], right[0]
        start, end = _point(before), _point(after)
        seconds = _elapsed(before, after)
        if i in explained or seconds < 0 or not all(map(has_usable_coordinates, (start, end))):
            continue
        meters = distance_meters(start, end)
        if meters >= 1000 and meters / max(1, seconds) >= 350:
            yield dict(
                kind="jump",
                folder=folder,
                camera=before.camera,
                before=before.filename,
                after=after.filename,
                original=start,
                suggested=end,
                distance=round(meters),
                seconds=round(seconds, 2),
                reviewed=False,
            )


def check_gps_jumps(db: Database, folder_id: int | None = None) -> list[JsonDict]:
    groups = defaultdict(list)
    reviews = {}
    for row in db.audit.camera_clock_rows(folder_id):
        frame = frame_from_row(row)
        if frame and frame.camera:
            groups[row["folder_id"], row["folder_path"], frame.camera].append(frame)
            reviews[frame.ref] = row["gps_review"]
    findings = []
    for (fid, folder, _camera), frames in groups.items():
        for sequence in shot_sequences(frames):
            for finding in _sequence_findings(sequence, folder, reviews):
                findings.append({**finding, "folder_id": fid})
    current = {f.get("key") for f in findings if f["reviewed"]}
    saved = db.audit.saved_gps_reviews(folder_id)
    for (fid, key), items in groupby(saved, lambda row: (row["folder_id"], row["evidence"])):
        if key in current:
            continue
        rows = list(items)
        first = rows[0]
        findings.append(
            dict(
                kind="saved",
                folder_id=fid,
                key=key,
                folder=first["folder"],
                first=first["filename"],
                last=rows[-1]["filename"],
                ids=[r["photo_id"] for r in rows],
                reviewed=True,
                conflict=False,
                suggested={"latitude": first["latitude"], "longitude": first["longitude"]},
            )
        )
    return findings
