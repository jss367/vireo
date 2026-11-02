"""Location suggestions for an import, chosen before anything is copied.

The import page asks for suggestions once its preview settles
(``/api/import/location-suggestions``): Vireo reads each source file's GPS,
groups the files by the ~110 m reverse-geocode grid cell, and suggests one
place per cell; cells with the same suggestion form one group. The user
accepts, renames or skips each group, and the import request carries the
result as ``location_choices``. After cataloging,
:func:`apply_location_choices` gives each new photo the choice its own GPS
cell belongs to, so nothing is tagged that the user did not see and choose.

A cell's suggestion comes from, in order:

``your_photos``
    the location you gave the most cataloged photos within
    :data:`FOOTPRINT_RADIUS_M`;
``your_place``
    the nearest of your location keywords whose own map point lies within
    :data:`PLACE_RADIUS_M`, or a Google place you already have as a keyword;
``google_park`` / ``google_area``
    ``places.reverse_geocode`` (the nearest well-known park, else the named
    area), through the shared grid cache.

Cells with no suggestion form one unnamed group the user can still name.

When Google can only name a county or broader for a cell (a trail deep in a
national forest), such cells are split into outings, one group each, and each
group also lists ``nearby_places``: visited parks and natural features from
``places.nearby_named_places`` whose map pin is within reach of every photo
in the group, with the closest and farthest photo's distance to it. They are
offered, never chosen: Google's pins are points, so Vireo cannot tell whether
the photos were taken inside the place. Choosing one tags the whole group
with that place's keyword and leaves the photos' own GPS as measured.
"""

from __future__ import annotations

import json
import logging
import math
import re
import sqlite3
import threading
import time
from collections import Counter
from typing import TYPE_CHECKING, Any

import places
from db import Database
from location_review import distance_meters
from services.gps_locations import (
    location_keyword_photo_ids,
    resolve_place_for_point,
    reverse_geocode_grid,
    summarize_details,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence

    from services.service_types import JsonDict

log = logging.getLogger(__name__)

# How far your already-tagged photos reach. A walk along a river or trail
# leaves photos every few meters, so a new frame within this distance of
# them was almost certainly taken at the same spot.
FOOTPRINT_RADIUS_M = 300
# How far one of your location keywords' own map point reaches.
PLACE_RADIUS_M = 300

# One card's worth and then some; the suggestions map every path to its cell.
MAX_SUGGESTION_PATHS = 100_000
# Cells bound the in-memory file map; groups separately bound provider detail
# requests and write transactions when applying the user's choices.
MAX_CHOICE_CELLS = MAX_SUGGESTION_PATHS
MAX_CHOICES = 1000
MAX_LOCATION_NAME = 200

# How far a nearby named place's map pin may be from every photo of a group,
# and how many such places a group offers. A recreation area's pin sits at its
# visitor center, often kilometers from its trails.
NEARBY_RADIUS_M = 5000
# Broadly named cells (all "San Diego County") are split into outings: cells
# within this distance of each other, directly or through a chain of cells,
# are one outing. Two outings in one county must not share a nearby place.
OUTING_LINK_M = 2000
# Cell-to-cell distance checks the outing split may make before the preview
# is refused, as previews over the group limit are. Ordinary cards make a few
# thousand; dense spots near a pole could otherwise make billions.
MAX_OUTING_COMPARISONS = 2_000_000
MAX_NEARBY_PLACES = 6
# Nearby lookups are made once per bucket of this many degrees (about 2 km),
# from its center, not once per ~110 m cell: one walk spans dozens of cells.
_NEARBY_BUCKET_DEG = 0.02
# Nearby answers are kept for this process, so previewing the same card again
# doesn't pay Google again; a day old, they are asked for again.
_NEARBY_CACHE_TTL_S = 24 * 3600
_NEARBY_CACHE_MAX = 4096

_SOURCE_STRENGTH = ("your_photos", "your_place", "google_park", "google_area")
_CELL_RE = re.compile(r"^-?\d{1,6},-?\d{1,6}$")
_M_PER_DEG_LAT = 111_320.0


def cell_key(lat: float, lng: float) -> str:
    """The grid cell a point belongs to, as ``"<lat_grid>,<lng_grid>"``."""
    lat_grid, lng_grid = reverse_geocode_grid(lat, lng)
    return f"{lat_grid},{lng_grid}"


def gps_point(grouped: Mapping[str, Any]) -> tuple[float, float] | None:
    """A file's (lat, lng) from grouped ExifTool output, read as the scanner
    reads it (Composite first, then EXIF), or None when unusable."""
    composite = grouped.get("Composite") or {}
    exif = grouped.get("EXIF") or {}
    lat = composite.get("GPSLatitude")
    if lat is None:
        lat = exif.get("GPSLatitude")
    lng = composite.get("GPSLongitude")
    if lng is None:
        lng = exif.get("GPSLongitude")
    return _finite_point(lat, lng)


def _finite_point(lat: object, lng: object) -> tuple[float, float] | None:
    if lat is None or lng is None or isinstance(lat, bool) or isinstance(lng, bool):
        return None
    try:
        lat_f, lng_f = float(lat), float(lng)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if not (math.isfinite(lat_f) and math.isfinite(lng_f)):
        return None
    if not (-90 <= lat_f <= 90 and -180 <= lng_f <= 180):
        return None
    return lat_f, lng_f


def read_source_gps(
    paths: Sequence[str], *,
    progress: Callable[[int, int], None] | None = None,
    checkpoint: Callable[[], None] | None = None,
) -> dict[str, tuple[float, float]]:
    """``{path: (lat, lng)}`` for the source files that carry usable GPS."""
    from metadata import extract_metadata

    grouped_by_path = extract_metadata(
        list(paths),
        # -fast2 skips maker notes, which hold no GPS; it halves a RAW read.
        restricted_tags=["-GPSLatitude", "-GPSLongitude", "-fast2"],
        progress_callback=progress,
        checkpoint=checkpoint,
    )
    out = {}
    for path, grouped in grouped_by_path.items():
        point = gps_point(grouped)
        if point is not None:
            out[path] = point
    return out


# Spatial bucket -> [(lat, lng, keyword_id, photo_id)]; photo_id is None for
# a keyword's own map point.
type _PointIndex = dict[tuple[int, int, int], list[tuple[float, float, int, int | None]]]
type _NearbyKey = tuple[int, int]

_nearby_cache: dict[tuple[int, int, str | None], tuple[float, list[JsonDict]]] = {}
_nearby_cache_lock = threading.Lock()


def _box_lng_ranges(min_lng: float, max_lng: float, pad: float) -> list[tuple[float, float]]:
    """The longitude ranges a box padded by ``pad`` degrees covers on the globe.

    Past the antimeridian the box continues on the other side, as a second
    range; a pad that wraps all the way round (near a pole) is every longitude.
    """
    low, high = min_lng - pad, max_lng + pad
    if high - low >= 360.0:
        return [(-180.0, 180.0)]
    ranges = [(max(low, -180.0), min(high, 180.0))]
    if low < -180.0:
        ranges.append((low + 360.0, 180.0))
    if high > 180.0:
        ranges.append((-180.0, high - 360.0))
    return ranges


class _NearbyPlaces:
    """Your tagged photos and located keywords around the import's cells.

    Points are indexed in reach-sized cubes of Earth-centered space
    (:func:`_earth_point`), so the 27 cubes around a spot hold everything
    within reach of it, at the poles and across the antimeridian alike.
    """

    def __init__(self, db: Database, points: Iterable[tuple[float, float]]) -> None:
        points = list(points)
        self._photos: _PointIndex = {}
        self._keywords: _PointIndex = {}
        if not points:
            return
        reach = max(FOOTPRINT_RADIUS_M, PLACE_RADIUS_M)
        lat_pad = reach / _M_PER_DEG_LAT
        min_lat = max(-90.0, min(lat for lat, _ in points) - lat_pad)
        max_lat = min(90.0, max(lat for lat, _ in points) + lat_pad)
        widest = max(abs(min_lat), abs(max_lat))
        # A degree of longitude is shortest at the box's poleward edge; a box
        # reaching a pole covers every longitude.
        cos_widest = math.cos(math.radians(widest))
        lng_pad = 360.0 if widest >= 90.0 or cos_widest <= 0 else lat_pad / cos_widest
        lngs = [lng for _, lng in points]
        for min_lng, max_lng in _box_lng_ranges(min(lngs), max(lngs), lng_pad):
            box = (min_lat, max_lat, min_lng, max_lng)
            for row in db.locations.tagged_location_points(*box):
                self._add(self._photos, row["latitude"], row["longitude"],
                          row["keyword_id"], row["photo_id"])
            for row in db.locations.located_keyword_points(*box):
                self._add(self._keywords, row["latitude"], row["longitude"], row["id"], None)

    @staticmethod
    def _cube(lat: float, lng: float) -> tuple[int, int, int]:
        side = max(FOOTPRINT_RADIUS_M, PLACE_RADIUS_M)
        x, y, z = _earth_point(lat, lng)
        return math.floor(x / side), math.floor(y / side), math.floor(z / side)

    def _add(self, index: _PointIndex, lat: object, lng: object,
             keyword_id: int, photo_id: int | None) -> None:
        point = _finite_point(lat, lng)
        if point is not None:
            index.setdefault(self._cube(*point), []).append((*point, keyword_id, photo_id))

    def _within(
        self, index: _PointIndex, lat: float, lng: float, radius_m: float,
    ) -> Iterator[tuple[float, int, int | None]]:
        """Yield ``(distance_m, keyword_id, photo_id)`` for points within reach.

        Straight-line distance never exceeds map distance, so a point within
        ``radius_m`` (at most a cube side) is in this cube or a neighbor.
        """
        cx, cy, cz = self._cube(lat, lng)
        here = {"latitude": lat, "longitude": lng}
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                for dz in (-1, 0, 1):
                    for p_lat, p_lng, keyword_id, photo_id in index.get((cx + dx, cy + dy, cz + dz), ()):
                        distance = distance_meters(here, {"latitude": p_lat, "longitude": p_lng})
                        if distance <= radius_m:
                            yield distance, keyword_id, photo_id

    def suggest(self, lat: float, lng: float) -> JsonDict | None:
        voters: dict[int, set[int | None]] = {}
        for _, keyword_id, photo_id in self._within(self._photos, lat, lng, FOOTPRINT_RADIUS_M):
            voters.setdefault(keyword_id, set()).add(photo_id)
        if voters:
            keyword_id = max(voters, key=lambda k: len(voters[k]))
            return {"kind": "keyword", "keyword_id": keyword_id,
                    "source": "your_photos", "photo_ids": voters[keyword_id]}
        nearest = min(
            self._within(self._keywords, lat, lng, PLACE_RADIUS_M), default=None,
        )
        if nearest is not None:
            return {"kind": "keyword", "keyword_id": nearest[1],
                    "source": "your_place", "distance_m": round(nearest[0])}
        return None


def _google_suggestion(
    db: Database, lat: float, lng: float, api_key: str, language: str | None,
    grid_cache: dict,
) -> tuple[JsonDict | None, str | None, bool]:
    """``(suggestion, unresolved reason, whether Google named only a county or broader)``."""
    details, reason = resolve_place_for_point(db, lat, lng, api_key, language, grid_cache)
    if details is None:
        return None, reason, False
    broad = places.is_broad_area(details)
    existing = db.locations.find_place_keyword(details["place_id"])
    if existing is not None:
        return {"kind": "keyword", "keyword_id": existing["id"], "source": "your_place"}, None, broad
    source = "google_park" if details.get("source") == "park" else "google_area"
    return {
        "kind": "google",
        "place_id": details["place_id"],
        "name": details.get("name") or "",
        "summary": summarize_details(details),
        "source": source,
    }, None, broad


def _wrap_lng(lng: float) -> float:
    """Longitude in [-180, 180): 180 and -180 are one meridian."""
    return (lng + 180.0) % 360.0 - 180.0


def _nearby_bucket(lat: float, lng: float) -> _NearbyKey:
    # At a pole every longitude is the same point: one lookup, whatever EXIF says.
    lng = 0.0 if abs(lat) >= 90.0 else _wrap_lng(lng)
    return math.floor(lat / _NEARBY_BUCKET_DEG), math.floor(lng / _NEARBY_BUCKET_DEG)


def _nearby_center(bucket: _NearbyKey) -> tuple[float, float]:
    """A bucket's center, kept on the globe for Google: the bucket holding a
    pole (latitude exactly 90) would otherwise center past it."""
    lat = (bucket[0] + 0.5) * _NEARBY_BUCKET_DEG
    lng = (bucket[1] + 0.5) * _NEARBY_BUCKET_DEG
    return min(90.0, max(-90.0, lat)), _wrap_lng(lng)


def _cached_nearby(bucket: _NearbyKey, language: str | None) -> list[JsonDict] | None:
    with _nearby_cache_lock:
        hit = _nearby_cache.get((*bucket, language))
    if hit is None or time.monotonic() - hit[0] > _NEARBY_CACHE_TTL_S:
        return None
    return hit[1]


def _remember_nearby(bucket: _NearbyKey, language: str | None, found: list[JsonDict]) -> None:
    with _nearby_cache_lock:
        _nearby_cache.pop((*bucket, language), None)
        while len(_nearby_cache) >= _NEARBY_CACHE_MAX:
            # Insertion order is age order: re-inserted keys were popped first.
            del _nearby_cache[next(iter(_nearby_cache))]
        _nearby_cache[(*bucket, language)] = (time.monotonic(), found)


def _lookup_nearby(
    bucket: _NearbyKey, api_key: str, language: str | None,
) -> tuple[list[JsonDict], str | None]:
    """The named places around a bucket's center, and why that list may be
    empty or incomplete (``no_api_key``, ``transient_error``), if it may."""
    # Only an API key makes a cached result usable: clicking a nearby place
    # fetches its details through Google at import time (see
    # :func:`_google_details`), which would fail without a key, so a key
    # removed since the lookup must suppress the cached buttons too.
    if not api_key:
        return [], "no_api_key"
    cached = _cached_nearby(bucket, language)
    if cached is not None:
        return cached, None
    lat, lng = _nearby_center(bucket)
    try:
        found = places.nearby_named_places(lat, lng, api_key, language=language)
    except places.PlacesPartialResult as partial:
        # Offer what answered, say the rest didn't, and ask again next time.
        log.warning("some nearby searches failed for lat=%s lng=%s", lat, lng)
        return partial.places, "transient_error"
    except places.PlacesTransientError:
        log.warning("nearby named places lookup failed for lat=%s lng=%s", lat, lng)
        return [], "transient_error"
    _remember_nearby(bucket, language, found)
    return found, None


def _location_path(db: Database, keyword_id: int) -> tuple[str, list[str]] | None:
    """A location keyword's name and its broadest-first parent names."""
    row = db.locations.keyword_place_row(keyword_id)
    if row is None or row["type"] != "location":
        return None
    parents = []
    parent_id = row["parent_id"]
    for _ in range(10):
        if parent_id is None:
            break
        parent = db.locations.keyword_place_row(parent_id)
        if parent is None or parent["type"] != "location":
            break
        parents.append(parent["name"])
        parent_id = parent["parent_id"]
    parents.reverse()
    return row["name"], parents


def location_options(db: Database) -> list[JsonDict]:
    """Every location keyword as ``{"id", "name", "label"}`` for a picker.

    ``label`` is the name, qualified by as many parents as it takes to tell
    same-named places apart ("Laguna Lake · San Luis Obispo"), and is unique
    ignoring case; places that still read alike get their keyword id.
    """
    rows = {row["id"]: row for row in db.locations.location_keyword_rows()}

    def parents_of(keyword_id: int) -> list[str]:
        names, parent_id = [], rows[keyword_id]["parent_id"]
        for _ in range(10):
            parent = rows.get(parent_id)
            if parent is None:
                break
            names.append(parent["name"])
            parent_id = parent["parent_id"]
        return names

    options = []
    for keyword_id, row in rows.items():
        parents = parents_of(keyword_id)
        options.append({"id": keyword_id, "name": row["name"], "parents": parents})
    # Reserve labels already chosen: a literal name can itself contain the
    # breadcrumb separator that qualifies another option. The picker matches
    # without case, so uniqueness must hold without case too.
    max_depth = max((len(o["parents"]) for o in options), default=0)
    for depth in range(max_depth + 1):
        labels = Counter(
            o.get("label", _option_label(o, depth)).casefold() for o in options)
        for option in options:
            label = _option_label(option, depth)
            if "label" not in option and labels[label.casefold()] == 1:
                option["label"] = label
    used = {o["label"].casefold() for o in options if "label" in o}
    for option in sorted(options, key=lambda o: o["id"]):
        if "label" in option:
            continue
        suffix = f' [#{option["id"]}]'
        label = _option_label(option, max_depth) + suffix
        while label.casefold() in used:
            label += suffix
        option["label"] = label
        used.add(label.casefold())
    for option in options:
        del option["parents"]
    options.sort(key=lambda o: o["label"].casefold())
    return options


def _option_label(option: JsonDict, depth: int) -> str:
    return " · ".join([option["name"], *option["parents"][:depth]])


def _group_key(suggestion: JsonDict | None) -> str:
    if suggestion is None:
        return "none"
    if suggestion["kind"] == "keyword":
        return f"keyword:{suggestion['keyword_id']}"
    return f"google:{suggestion['place_id']}"


def _cells_of(gps_by_path: Mapping[str, tuple[float, float]]) -> dict[str, JsonDict]:
    cells: dict[str, JsonDict] = {}
    for path, (lat, lng) in gps_by_path.items():
        cell = cells.setdefault(
            cell_key(lat, lng), {"paths": [], "points": [], "lat": 0.0, "lng": 0.0})
        cell["paths"].append(path)
        cell["points"].append((lat, lng))
        cell["lat"] += lat
        cell["lng"] += lng
    for cell in cells.values():
        cell["lat"] /= len(cell["paths"])
        cell["lng"] /= len(cell["paths"])
    return cells


def _new_group(key: str, suggestion: JsonDict | None) -> JsonDict:
    return {
        "key": key,
        "suggestion": suggestion,
        "cells": [],
        "file_count": 0,
        "lat_sum": 0.0,
        "lng_sum": 0.0,
        "sample_paths": [],
        "photo_ids": set(),
        "reasons": Counter(),
        "broad_cells": [],
    }


def _add_cell(
    group: JsonDict, key: str, cell: JsonDict,
    suggestion: JsonDict | None, reason: str | None,
) -> None:
    count = len(cell["paths"])
    group["cells"].append(key)
    group["file_count"] += count
    group["lat_sum"] += cell["lat"] * count
    group["lng_sum"] += cell["lng"] * count
    for path in cell["paths"]:
        if len(group["sample_paths"]) >= 4:
            break
        group["sample_paths"].append(path)
    if suggestion is not None:
        current = group["suggestion"]
        if (_SOURCE_STRENGTH.index(suggestion["source"])
                < _SOURCE_STRENGTH.index(current["source"])):
            group["suggestion"] = {**current, "source": suggestion["source"]}
        group["photo_ids"].update(suggestion.get("photo_ids", ()))
    elif reason:
        group["reasons"][reason] += 1


def _finish_group(db: Database, group: JsonDict) -> JsonDict | None:
    suggestion = group["suggestion"]
    if suggestion is not None and suggestion["kind"] == "keyword":
        named = _location_path(db, suggestion["keyword_id"])
        if named is None:
            return None
        suggestion = {**suggestion, "name": named[0], "parents": named[1]}
        suggestion.pop("photo_ids", None)
        if group["photo_ids"]:
            # Distinct photos: neighboring cells see many of the same ones.
            suggestion["photo_count"] = len(group["photo_ids"])
    count = group["file_count"]
    return {
        "key": group["key"],
        "suggestion": suggestion,
        "cells": group["cells"],
        "file_count": count,
        "latitude": group["lat_sum"] / count,
        "longitude": group["lng_sum"] / count,
        "sample_paths": group["sample_paths"],
        "unresolved_reasons": dict(group["reasons"]),
    }


# The Earth radius distance_meters uses.
_EARTH_RADIUS_M = 6_371_000


def _earth_point(lat: float, lng: float) -> tuple[float, float, float]:
    """A point's Earth-centered coordinates in meters.

    Straight-line distance never exceeds distance along the surface, and at
    the link distance the two differ by microns, so it measures closeness the
    same way at the poles and across the antimeridian as anywhere else.
    """
    phi, lam = math.radians(lat), math.radians(lng)
    return (_EARTH_RADIUS_M * math.cos(phi) * math.cos(lam),
            _EARTH_RADIUS_M * math.cos(phi) * math.sin(lam),
            _EARTH_RADIUS_M * math.sin(phi))


class OutingWorkLimit(Exception):
    """Splitting into outings would take more than :data:`MAX_OUTING_COMPARISONS`."""


class _ComparisonBudget:
    """The distance checks one preview's outing split may still make."""

    def __init__(self) -> None:
        self.left = MAX_OUTING_COMPARISONS

    def spend(self) -> None:
        self.left -= 1
        if self.left < 0:
            raise OutingWorkLimit


# Cube side for the outing index: any two points in one cube are within the
# link distance of each other (the cube's diagonal), so a cube is linked
# whole, and points within the link distance are at most two cubes apart.
_OUTING_CUBE_M = OUTING_LINK_M / math.sqrt(3)
_CUBE_REACH = math.ceil(math.sqrt(3))
# Each unordered pair of neighboring cubes is visited once, from the cube
# that sorts first.
_FORWARD_CUBES = [
    (dx, dy, dz)
    for dx in range(-_CUBE_REACH, _CUBE_REACH + 1)
    for dy in range(-_CUBE_REACH, _CUBE_REACH + 1)
    for dz in range(-_CUBE_REACH, _CUBE_REACH + 1)
    if (dx, dy, dz) > (0, 0, 0)
]


type _CubeMembers = list[tuple[str, tuple[float, float, float]]]


def _cubes_link(members: _CubeMembers, others: _CubeMembers, budget: _ComparisonBudget) -> bool:
    """Whether any point of one cube is within the link distance of the other's,
    stopping at the first."""
    for _, a in members:
        for _, b in others:
            budget.spend()
            if math.dist(a, b) <= OUTING_LINK_M:
                return True
    return False


def _outings(
    cells: Mapping[str, JsonDict], keys: Sequence[str], budget: _ComparisonBudget,
) -> list[list[str]]:
    """Split cells into chains of cells within :data:`OUTING_LINK_M` of each other.

    Distance is straight-line between Earth-centered points
    (:func:`_earth_point`), indexed in cubes small enough that each cube's
    cells are linked without measuring. Two cubes are measured against each
    other only while their outings are still apart, and only until the first
    link, so dense spots (many cells collapse into a few cubes near a pole)
    cost one pass. Each check spends from ``budget``, which one preview shares
    across all its groups, and raises :class:`OutingWorkLimit` once spent.
    Outings come back in the order of their
    first cell.
    """
    parent = {key: key for key in keys}

    def root(key: str) -> str:
        while parent[key] != key:
            parent[key] = parent[parent[key]]
            key = parent[key]
        return key

    cubes: dict[tuple[int, int, int], _CubeMembers] = {}
    for key in keys:
        point = _earth_point(cells[key]["lat"], cells[key]["lng"])
        cube = cubes.setdefault((math.floor(point[0] / _OUTING_CUBE_M),
                                 math.floor(point[1] / _OUTING_CUBE_M),
                                 math.floor(point[2] / _OUTING_CUBE_M)), [])
        if cube:
            parent[root(key)] = root(cube[0][0])
        cube.append((key, point))
    for (x, y, z), members in cubes.items():
        for dx, dy, dz in _FORWARD_CUBES:
            others = cubes.get((x + dx, y + dy, z + dz))
            if not others or root(members[0][0]) == root(others[0][0]):
                continue
            if _cubes_link(members, others, budget):
                parent[root(others[0][0])] = root(members[0][0])
    outings: dict[str, list[str]] = {}
    for key in keys:
        outings.setdefault(root(key), []).append(key)
    return list(outings.values())


def _beyond_any_pin(points: Iterable[tuple[float, float]]) -> bool:
    """Whether no single pin can be within :data:`NEARBY_RADIUS_M` of all points.

    Points all within that reach of one pin are within twice it of each
    other, so a point farther than that from the first one rules every pin
    out. Measured on the map, so it holds across the antimeridian.
    """
    points = iter(points)
    first = next(points, None)
    if first is None:
        return False
    origin = {"latitude": first[0], "longitude": first[1]}
    return any(
        distance_meters(origin, {"latitude": lat, "longitude": lng}) > 2 * NEARBY_RADIUS_M
        for lat, lng in points
    )


def _pin_reach(place: JsonDict, points: Iterable[tuple[float, float]]) -> tuple[int, int] | None:
    """``(closest, farthest)`` meters from a place's pin to the points, or None
    once one point is beyond :data:`NEARBY_RADIUS_M`."""
    pin = {"latitude": place["lat"], "longitude": place["lng"]}
    closest, farthest = math.inf, 0.0
    for lat, lng in points:
        distance = distance_meters({"latitude": lat, "longitude": lng}, pin)
        if distance > NEARBY_RADIUS_M:
            return None
        closest, farthest = min(closest, distance), max(farthest, distance)
    return round(closest), round(farthest)


class ImportLocationSuggester:
    """Names an import's GPS cells one at a time, then groups them.

    ``cell_keys`` lists the cells; :meth:`name_cell` suggests one, then
    :meth:`find_nearby` looks up each of :meth:`nearby_keys` (the only steps
    that may call Google), so a caller can stop between lookups;
    :meth:`result` returns what :func:`suggest_import_locations` documents.
    """

    def __init__(
        self, db: Database, gps_by_path: Mapping[str, tuple[float, float]], *,
        api_key: str, language: str | None,
    ) -> None:
        self._db = db
        self._api_key = api_key
        self._language = language
        self._cells = _cells_of(gps_by_path)
        self._nearby = _NearbyPlaces(
            db, ((c["lat"], c["lng"]) for c in self._cells.values()))
        self._grid_cache: dict = {}
        self._groups: dict[str, JsonDict] = {}
        self._nearby_found: dict[_NearbyKey, tuple[list[JsonDict], str | None]] = {}
        self._nearby_over_limit = False
        self._named: dict[str, tuple[JsonDict | None, str | None, bool]] = {}
        self._outings_split = False
        self._outing_work_exceeded = False
        self.cell_keys = list(self._cells)

    def name_cell(self, key: str) -> None:
        cell = self._cells[key]
        suggestion = self._nearby.suggest(cell["lat"], cell["lng"])
        reason, broad = None, False
        if suggestion is None:
            suggestion, reason, broad = _google_suggestion(
                self._db, cell["lat"], cell["lng"], self._api_key, self._language,
                self._grid_cache,
            )
        self._named[key] = (suggestion, reason, broad)
        group_key = _group_key(suggestion)
        group = self._groups.setdefault(group_key, _new_group(group_key, suggestion))
        _add_cell(group, key, cell, suggestion, reason)
        if broad:
            group["broad_cells"].append(key)

    def _split_outings(self) -> None:
        """Give each outing among broadly named cells its own group.

        A county-wide name groups every spot in the county; its nearby places
        only make sense for one outing. The broad cells of a group are split
        by :func:`_outings`; a group that is one outing and nothing else keeps
        its key, and each other outing is keyed by its first cell, so a
        repeated preview of the same files keeps the user's per-group edits.
        """
        if self._outings_split:
            return
        self._outings_split = True
        regrouped: dict[str, JsonDict] = {}
        budget = _ComparisonBudget()
        for key, group in self._groups.items():
            try:
                outings = (_outings(self._cells, group["broad_cells"], budget)
                           if group["broad_cells"] else [])
            except OutingWorkLimit:
                # Refused in result(), before any Nearby Search.
                self._outing_work_exceeded = True
                return
            if len(outings) == 1 and len(outings[0]) == len(group["cells"]):
                regrouped[key] = group
                continue
            broad = {cell for outing in outings for cell in outing}
            rest = [cell for cell in group["cells"] if cell not in broad]
            parts = [(key, rest)] if rest else []
            parts += [(f"{key}@{min(outing)}", outing) for outing in outings]
            for part_key, cells in parts:
                part = regrouped[part_key] = _new_group(part_key, self._named[cells[0]][0])
                for cell_key_ in cells:
                    suggestion, reason, is_broad = self._named[cell_key_]
                    _add_cell(part, cell_key_, self._cells[cell_key_], suggestion, reason)
                    if is_broad:
                        part["broad_cells"].append(cell_key_)
        self._groups = regrouped

    def nearby_keys(self) -> list[_NearbyKey]:
        """The buckets to look up after naming: those of cells Google named broadly.

        Returns an empty list when the preview is over its work budget, so
        :meth:`result` refuses it without paying for any provider lookups:
        without the cap, 100,000 broad cells spread into distinct 2 km
        buckets could still drive up to 200,000 paid Nearby Searches before
        refusal, whether as 100,000 groups refused by :meth:`result` or as
        one group of many spread cells :meth:`result` would accept.
        """
        self._split_outings()
        if len(self._groups) > MAX_CHOICES or self._outing_work_exceeded:
            return []
        buckets: list[_NearbyKey] = []
        seen: set[_NearbyKey] = set()
        for group in self._groups.values():
            for key in group["broad_cells"]:
                bucket = _nearby_bucket(self._cells[key]["lat"], self._cells[key]["lng"])
                if bucket in seen:
                    continue
                seen.add(bucket)
                if len(seen) > MAX_CHOICES:
                    self._nearby_over_limit = True
                    return []
                buckets.append(bucket)
        return buckets

    def find_nearby(self, bucket: _NearbyKey) -> None:
        """Look up one bucket's named places (the only other step that may call Google)."""
        self._nearby_found[bucket] = _lookup_nearby(bucket, self._api_key, self._language)

    def _nearby_places(self, group: JsonDict) -> tuple[list[JsonDict], str | None]:
        """A broadly named outing's nearby places, nearest first, or why there are none.

        A place is offered only when its pin is within ``NEARBY_RADIUS_M`` of
        every photo in the group, so choosing it, which tags the whole group,
        never names a photo taken far from it. The reason is set whenever a
        lookup for the group failed, even beside places from the lookups that
        answered. Each entry carries
        ``distance_m`` and ``farthest_m``: the closest and farthest of the
        group's measured photo points from the pin.
        """
        points = {point for key in group["broad_cells"] for point in self._cells[key]["points"]}
        candidates: dict[str, JsonDict] = {}
        reasons: Counter[str] = Counter()
        for key in group["broad_cells"]:
            cell = self._cells[key]
            looked_up = self._nearby_found.get(_nearby_bucket(cell["lat"], cell["lng"]))
            found, reason = looked_up if looked_up is not None else ([], "not_looked_up")
            if reason:
                reasons[reason] += 1
            for place in found:
                candidates.setdefault(place["place_id"], place)
        candidates.pop((group["suggestion"] or {}).get("place_id") or "", None)
        if candidates and _beyond_any_pin(points):
            # No pin can cover this outing; skip measuring each candidate.
            return [], "spread_out"
        offered = []
        for place in candidates.values():
            reach = _pin_reach(place, points)
            if reach is not None:
                closest, farthest = reach
                offered.append({
                    "place_id": place["place_id"], "name": place["name"],
                    "type": place["type"], "distance_m": closest, "farthest_m": farthest,
                })
        offered.sort(key=lambda p: (p["distance_m"], p["name"]))
        # A failed lookup is reported even beside places from the lookups
        # that answered: the list may be missing the nearest place.
        reason = reasons.most_common(1)[0][0] if reasons else None
        return offered[:MAX_NEARBY_PLACES], reason

    def result(self) -> JsonDict:
        self._split_outings()
        if getattr(self, "_outing_work_exceeded", False):
            return {
                "groups": [], "file_cells": {},
                "work_limit_error": (
                    "These photos' GPS spots are too densely packed for Vireo to group "
                    "into outings. Preview fewer files, or turn off GPS location suggestions."
                ),
            }
        if len(self._groups) > MAX_CHOICES or getattr(self, "_nearby_over_limit", False):
            return {
                "groups": [], "file_cells": {},
                "work_limit_error": (
                    f"GPS locations support at most {MAX_CHOICES:,} groups per import. "
                    "Preview fewer files, or turn off GPS location suggestions."
                ),
            }
        finished = []
        for group in self._groups.values():
            done = _finish_group(self._db, group)
            if done is None:
                # The suggested keyword vanished mid-request; offer the cells
                # unnamed, but keep the group's own key so each vanished group
                # stays distinct from the unnamed group (and from each other):
                # the frontend stores per-group edits by that key.
                done = _finish_group(self._db, {**group, "suggestion": None})
            if done is not None and group["broad_cells"]:
                done["nearby_places"], reason = self._nearby_places(group)
                if reason:
                    done["nearby_unresolved"] = reason
            finished.append(done)
        finished.sort(key=lambda g: (g["suggestion"] is None, -g["file_count"]))
        return {
            "groups": finished,
            "file_cells": {
                path: key for key, cell in self._cells.items() for path in cell["paths"]
            },
        }


def suggest_import_locations(
    db: Database, gps_by_path: Mapping[str, tuple[float, float]], *,
    api_key: str, language: str | None,
    cancel_check: Callable[[], bool] | None = None,
) -> JsonDict | None:
    """Group source files by GPS cell and suggest a place for each group.

    Returns ``{"groups": [...], "file_cells": {path: cell}}``, largest group
    first and the unnamed group last, or None when ``cancel_check`` stopped
    it. Each group's ``suggestion`` is None or a dict with ``kind``
    (``"keyword"`` with ``keyword_id``, ``name`` and ``parents``, or
    ``"google"`` with ``place_id``, ``name`` and ``summary``) and ``source``.
    Cells Google could name only as a county or broader are split into
    outings (:data:`OUTING_LINK_M`), one group each, and each such group
    also carries ``nearby_places``: places whose pin is within
    :data:`NEARBY_RADIUS_M` of every photo in it (``place_id``, ``name``,
    ``type``, and ``distance_m`` / ``farthest_m``, the closest and farthest
    photo from the pin), nearest first, plus ``nearby_unresolved``
    (``no_api_key``, ``transient_error`` or ``spread_out``) when a lookup
    could not run, so the list is empty or may be incomplete, or no pin could
    reach the whole outing.
    Over-budget previews return empty groups and ``work_limit_error``;
    callers must require fewer files or explicit GPS opt-out before import.
    """
    suggester = ImportLocationSuggester(
        db, gps_by_path, api_key=api_key, language=language)
    for key in suggester.cell_keys:
        if cancel_check is not None and cancel_check():
            return None
        suggester.name_cell(key)
    for bucket in suggester.nearby_keys():
        if cancel_check is not None and cancel_check():
            return None
        suggester.find_nearby(bucket)
    return suggester.result()


class LocationChoiceError(ValueError):
    """A ``location_choices`` entry the import must reject."""


def _parse_choice_target(item: Mapping[str, Any]) -> JsonDict:
    kind = item.get("kind")
    if kind == "keyword":
        keyword_id = item.get("keyword_id")
        if isinstance(keyword_id, bool) or not isinstance(keyword_id, int) or keyword_id <= 0:
            raise LocationChoiceError("a chosen location needs a keyword_id")
        return {"kind": "keyword", "keyword_id": keyword_id}
    name = item.get("name")
    if not isinstance(name, str) or not name.strip():
        raise LocationChoiceError("a chosen location needs a name")
    name = name.strip()
    if len(name) > MAX_LOCATION_NAME:
        raise LocationChoiceError(
            f"location names must be {MAX_LOCATION_NAME} characters or fewer")
    if "|" in name:
        raise LocationChoiceError('location names cannot contain "|"')
    if kind == "google":
        place_id = item.get("place_id")
        if not isinstance(place_id, str) or not place_id.strip() or len(place_id) > 300:
            raise LocationChoiceError("a Google location needs a place_id")
        return {"kind": "google", "place_id": place_id.strip(), "name": name}
    if kind == "new":
        # Kept in the request's own shape, so a retry can send it back. The
        # map point is optional, but only as a pair, and never malformed.
        lat, lng = item.get("latitude"), item.get("longitude")
        if lat is not None or lng is not None:
            point = _finite_point(lat, lng)
            if point is None:
                raise LocationChoiceError(
                    "a new location's latitude and longitude must both be valid")
            lat, lng = point
        return {"kind": "new", "name": name, "latitude": lat, "longitude": lng}
    raise LocationChoiceError("location choice kind must be keyword, google or new")


def parse_location_choices(raw: object) -> list[JsonDict]:
    """Validate an import's ``location_choices``; raise LocationChoiceError.

    Each entry is ``{"cells": ["<lat_grid>,<lng_grid>", ...], "kind": ...}``
    with ``kind`` ``"keyword"`` (``keyword_id``), ``"google"``
    (``place_id`` and its ``name``) or ``"new"`` (``name``, and optionally
    the group's ``latitude``/``longitude`` as its map point). A cell may
    belong to one choice only. The result keeps the request's shape, so a
    job can store it and a retry can send it again.
    """
    if raw is None:
        return []
    if not isinstance(raw, list) or len(raw) > MAX_CHOICES:
        raise LocationChoiceError(
            f"location_choices must be a list of at most {MAX_CHOICES} choices")
    choices, seen = [], set()
    for item in raw:
        if not isinstance(item, dict):
            raise LocationChoiceError("each location choice must be an object")
        cells = item.get("cells")
        if (not isinstance(cells, list) or not cells
                or any(not isinstance(c, str) or not _CELL_RE.match(c) for c in cells)):
            raise LocationChoiceError("each location choice needs its GPS cells")
        if seen.intersection(cells):
            raise LocationChoiceError("a GPS cell can have only one location")
        seen.update(cells)
        if len(seen) > MAX_CHOICE_CELLS:
            raise LocationChoiceError("too many GPS cells in location_choices")
        choices.append({**_parse_choice_target(item), "cells": list(dict.fromkeys(cells))})
    return choices


def _google_details(
    db: Database, choice: JsonDict, api_key: str, language: str | None,
) -> JsonDict | None:
    """Fetch a Google choice's place details unless the catalog has it.

    The network call happens here, before the choice's write transaction,
    so no lookup holds the catalog's write lock.
    """
    if choice["kind"] != "google" or db.locations.find_place_keyword(choice["place_id"]):
        return None
    if not api_key:
        raise LocationChoiceError(
            f'Add a Google Maps API key in Settings to add "{choice["name"]}"')
    details = places.place_details_for_language(choice["place_id"], api_key, language)
    if details is None:
        raise LocationChoiceError(f'Could not look up "{choice["name"]}" on Google Maps')
    return details


def _choice_leaf(db: Database, choice: JsonDict, details: JsonDict | None) -> tuple[int, str]:
    """The location keyword a choice names, created if needed: (id, name)."""
    if choice["kind"] == "keyword":
        row = db.locations.keyword_place_row(choice["keyword_id"])
        if row is None or row["type"] != "location":
            raise LocationChoiceError(
                "a location chosen on the import page no longer exists")
        return row["id"], row["name"]
    if choice["kind"] == "google":
        existing = db.locations.find_place_keyword(choice["place_id"])
        if existing is not None:
            row = db.locations.keyword_place_row(existing["id"])
            return existing["id"], row["name"] if row else choice["name"]
        if details is None:
            # Added between the lookup and this transaction, then removed.
            raise LocationChoiceError(f'Could not look up "{choice["name"]}" on Google Maps')
        return db.upsert_place_chain(details), details.get("name") or choice["name"]
    leaf_id = db.get_or_create_text_location(choice["name"])
    if choice.get("latitude") is not None:
        db.locations.fill_missing_coordinates(leaf_id, choice["latitude"], choice["longitude"])
    return leaf_id, choice["name"]


class _ChoiceCancelled(Exception):
    """Raised inside a choice's transaction so it rolls back whole."""


def _apply_choice(
    db: Database, choice: JsonDict, details: JsonDict | None, pids: Sequence[int],
    cancel_check: Callable[..., bool],
) -> tuple[int, int]:
    """Create the choice's keyword and assign it in one transaction.

    Returns ``(added, kept)``. Every write here, including a new keyword
    and its Google parent chain, commits together or not at all: a
    cancellation raises :class:`_ChoiceCancelled` and rolls back.
    """
    from services.edit_history import change_photo_location

    with db.transaction():
        db.hold_writer_lock()
        leaf_id, name = _choice_leaf(db, choice, details)
        # Re-read under the write lock: another tab may have placed a photo
        # since the plan was made, and imports keep existing locations.
        located = location_keyword_photo_ids(db, pids)
        items = []
        for pid in pids:
            if pid in located:
                continue
            if cancel_check(pause_safe=False):
                raise _ChoiceCancelled
            item = change_photo_location(db, pid, leaf_id)
            if item is not None:
                items.append(item)
        if items:
            # Re-check cancellation after the last assignment: without this,
            # Stop arriving during the final change_photo_location would still
            # record the undo step and commit the transaction.
            if cancel_check(pause_safe=False):
                raise _ChoiceCancelled
            db.record_edit(
                "photo_location",
                f'Added location "{name}" during import to {len(items)} photos',
                str(leaf_id), items, is_batch=True,
            )
    return len(items), len(located)


def _plan_location_choices(
    db: Database, photo_ids: Sequence[int], choices: Sequence[JsonDict],
    summary: JsonDict,
) -> dict[int, list[int]]:
    """Map each choice index to the imported photos in its cells, counting
    the photos no choice applies to on ``summary``."""
    by_cell = {cell: index for index, choice in enumerate(choices) for cell in choice["cells"]}
    photos = db.photos.get_by_ids(list(photo_ids))
    located = location_keyword_photo_ids(db, photo_ids)
    targets: dict[int, list[int]] = {}
    for pid in photo_ids:
        photo = photos.get(pid)
        point = _finite_point(photo["latitude"], photo["longitude"]) if photo else None
        if point is None:
            summary["locations_no_gps"] += 1
            continue
        index = by_cell.get(cell_key(*point))
        if index is None:
            summary["locations_unchosen"] += 1
        elif pid in located:
            summary["locations_kept"] += 1
        else:
            targets.setdefault(index, []).append(pid)
    return targets


def apply_location_choices(
    db: Database, photo_ids: Sequence[int], choices: Sequence[JsonDict], *,
    api_key: str, language: str | None, summary: JsonDict,
    cancel_check: Callable[..., bool],
) -> None:
    """Give each imported photo the location chosen for its GPS cell.

    Photos that already carry a location keep it, and photos in no chosen
    cell are left alone. Each choice is one transaction and one edit-history
    step. Counts land on ``summary``: ``locations_added``,
    ``locations_kept``, ``locations_unchosen``, ``locations_no_gps``,
    ``locations_unresolved``, plus ``errors``; ``cancelled`` when
    ``cancel_check`` stopped it, in which case the choice in progress left
    nothing behind.
    """
    if len(choices) > MAX_CHOICES:
        raise LocationChoiceError(f"at most {MAX_CHOICES} location groups may be applied")
    for counter in ("locations_added", "locations_kept", "locations_unchosen",
                    "locations_no_gps", "locations_unresolved"):
        summary.setdefault(counter, 0)
    targets = _plan_location_choices(db, photo_ids, choices, summary)
    if cancel_check():
        summary["cancelled"] = True
        return
    for index, pids in targets.items():
        if cancel_check():
            summary["cancelled"] = True
            return
        choice = choices[index]
        try:
            details = _google_details(db, choice, api_key, language)
            added, kept = _apply_choice(db, choice, details, pids, cancel_check)
        except _ChoiceCancelled:
            summary["cancelled"] = True
            return
        except (ValueError, sqlite3.Error) as exc:
            # LocationChoiceError is a ValueError; the rest are catalog writes,
            # already rolled back by the choice's transaction.
            db.rollback()
            log.warning("Import location choice failed: %s", exc, exc_info=True)
            summary["errors"].append(f"{exc}; {len(pids)} photos were left untagged")
            summary["locations_unresolved"] += len(pids)
            continue
        summary["locations_added"] += added
        summary["locations_kept"] += kept


# Files per GPS read between progress frames: a few ExifTool batches, so the
# stream reports often and notices a closed page soon after it goes away.
_GPS_READ_CHUNK = 200


def stream_location_suggestions(
    db_path: str, paths: Sequence[str], maps_config: Mapping[str, Any],
) -> Iterator[str]:
    """SSE frames for ``/api/import/location-suggestions``.

    ``{"reading": n, "total": N}`` while the files' GPS is read,
    ``{"naming": n, "total": N}`` per GPS cell while places are named,
    ``{"nearby": n, "total": N}`` per nearby-places lookup, then one
    ``{"done": true, "groups": [...], "file_cells": {...}, "no_gps_paths":
    [...], "location_options": [...]}`` (see :func:`suggest_import_locations`
    and :func:`location_options`), or ``{"done": true, "error": "..."}``.
    Closing the page stops the read at the next frame.
    """
    def frame(payload: JsonDict) -> str:
        return f"data: {json.dumps(payload)}\n\n"

    from metadata import exiftool_available

    unique = list(dict.fromkeys(paths))
    total = len(unique)
    if not exiftool_available():
        # ExifTool reports a missing binary by returning nothing, which
        # would read as "none of these photos have GPS".
        yield frame({"done": True, "error": "ExifTool isn't installed, so Vireo can't read the photos' GPS."})
        return
    yield frame({"reading": 0, "total": total})
    gps: dict[str, tuple[float, float]] = {}
    for start in range(0, total, _GPS_READ_CHUNK):
        gps.update(read_source_gps(unique[start:start + _GPS_READ_CHUNK]))
        yield frame({"reading": min(start + _GPS_READ_CHUNK, total), "total": total})
    # create_app already set up the schema; repeating its DDL here could
    # queue behind an import's write transaction. Same rule as the request DB.
    db = Database(db_path, initialize_schema=(db_path == ":memory:"))
    try:
        suggester = ImportLocationSuggester(
            db, gps,
            api_key=(maps_config.get("google_maps_api_key", "") or "").strip(),
            language=places.result_language(maps_config),
        )
        cells = len(suggester.cell_keys)
        # One frame per cell: naming may call Google, and a frame is where a
        # closed page stops the stream before the next paid lookup.
        for done, key in enumerate(suggester.cell_keys, start=1):
            suggester.name_cell(key)
            yield frame({"naming": done, "total": cells})
        buckets = suggester.nearby_keys()
        for done, bucket in enumerate(buckets, start=1):
            suggester.find_nearby(bucket)
            yield frame({"nearby": done, "total": len(buckets)})
        result = suggester.result()
        options = location_options(db)
    except sqlite3.Error as exc:
        log.warning("Could not build import location suggestions", exc_info=True)
        yield frame({"done": True, "error": f"Could not suggest locations: {exc}"})
        return
    finally:
        db.close()
    no_gps = [path for path in unique if path not in gps]
    yield frame({"done": True, **result, "no_gps_paths": no_gps, "location_options": options})
