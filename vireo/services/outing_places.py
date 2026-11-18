"""Infer a park from direct GPS matches elsewhere on a continuous walk."""

from __future__ import annotations

from dataclasses import dataclass

import places
from location_review import capture_time, distance_meters, has_usable_coordinates

# Allow GPS jitter between burst frames, but break at driving speeds, long
# pauses, and sparse jumps where the intervening route is unknown.
MAX_GAP_SECONDS = 30 * 60
MAX_STEP_M = 1000
GPS_JITTER_M = 125
MAX_SPEED_M_S = 3
MAX_PARK_REACH_M = 5000


@dataclass
class Outing:
    place: dict
    members: list
    anchors: list
    inferred: list


def _role(place: dict | None) -> str | None:
    if not place or place.get("country_conflict"):
        return None
    types = set(place.get("types") or ())
    if place.get("source") == "park" or types.intersection(places.NEARBY_PLACE_TYPES):
        return "anchor"
    named = types.intersection(places.AREA_TYPE_PRIORITY)
    if place.get("source") == "area" and named and named <= places.BROAD_AREA_TYPES | {"locality", "postal_town"}:
        return "broad"
    return None


def _offer(run: list[dict]) -> Outing | None:
    anchors = [frame for frame in run if frame["role"] == "anchor"]
    broad = [frame for frame in run if frame["role"] == "broad"]
    if not broad or len({frame["place"]["place_id"] for frame in anchors}) != 1:
        return None
    if len({code for frame in run if (code := places.place_country_code(frame["place"]))}) > 1:
        return None
    place = anchors[0]["place"]
    pin = {"latitude": place.get("lat"), "longitude": place.get("lng")}
    if not has_usable_coordinates(pin) or any(distance_meters(pin, f) > MAX_PARK_REACH_M for f in run):
        return None
    return Outing(place, [f["id"] for f in run], [f["id"] for f in anchors], [f["id"] for f in broad])


def outing_offers(frames) -> list[Outing]:
    """Return only unambiguous runs. An unresolved dated frame breaks a run."""
    dated = []
    for frame in frames:
        time = capture_time(frame["timestamp"])
        if time is not None:
            dated.append((time.tzinfo is not None, time, frame))
    # Coordinates break timestamp ties the same way for file paths and photo IDs.
    dated.sort(key=lambda item: (item[0], item[1], _point_key(item[2])))
    offers = []
    run: list[dict] = []
    reference = None
    for _, time, frame in dated:
        role = _role(frame["place"]) if has_usable_coordinates(frame) else None
        if run and (role is None or not _continues_walk(run[-1], reference, frame, time)):
            if offer := _offer(run):
                offers.append(offer)
            run = []
            reference = None
        if role:
            current = {**frame, "time": time, "role": role}
            run.append(current)
            # Check accumulated displacement as well as each step, so a car
            # photographed in bursts cannot take a fresh jitter allowance
            # every few seconds.
            if reference is None or distance_meters(reference, current) >= 2 * GPS_JITTER_M:
                reference = current
    if offer := _offer(run):
        offers.append(offer)
    return offers


def _point_key(frame):
    if has_usable_coordinates(frame):
        return float(frame["latitude"]), float(frame["longitude"])
    return 0.0, 0.0


def _continues_walk(previous, reference, frame, time):
    if (previous["time"].tzinfo is None) != (time.tzinfo is None):
        return False
    seconds = (time - previous["time"]).total_seconds()
    allowed_step = min(MAX_STEP_M, GPS_JITTER_M + seconds * MAX_SPEED_M_S)
    if seconds > MAX_GAP_SECONDS or distance_meters(previous, frame) > allowed_step:
        return False
    elapsed = (time - reference["time"]).total_seconds()
    return distance_meters(reference, frame) <= GPS_JITTER_M + elapsed * MAX_SPEED_M_S
