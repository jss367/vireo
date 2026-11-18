"""The local time zone at a GPS position on a given date.

``local_zone`` answers what UTC offset a camera at ``(latitude, longitude)``
should have recorded at a given moment: the position's IANA zone (``tzfpy``,
an offline boundary map compiled into the wheel) and that zone's offset then
(``zoneinfo``, so daylight saving follows the date). ``available()`` is False
when either part is missing, for example a Windows install without the
``tzdata`` package; callers then reason without location and say so.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from functools import lru_cache
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

log = logging.getLogger(__name__)

# Positions are rounded to about 1 km before the boundary lookup; a camera
# within a kilometer of a zone border can be placed on either side.
_POSITION_DECIMALS = 2


@lru_cache(maxsize=1)
def available() -> bool:
    """True when positions can be turned into local offsets."""
    try:
        import tzfpy  # noqa: F401
    except ImportError:
        log.info("tzfpy is not installed; camera-clock checks run without location")
        return False
    try:
        ZoneInfo("America/Los_Angeles")
    except ZoneInfoNotFoundError:
        log.info("No time-zone database; camera-clock checks run without location")
        return False
    return True


@lru_cache(maxsize=4096)
def _zone_name(latitude: float, longitude: float) -> str | None:
    from tzfpy import get_tz

    name = get_tz(longitude, latitude)
    return name or None


@lru_cache(maxsize=256)
def _zone(name: str) -> ZoneInfo | None:
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError):
        return None


def local_zone(latitude: float, longitude: float, utc: datetime) -> tuple[str, int] | None:
    """``(zone name, offset minutes)`` at the position at UTC ``utc`` (naive)."""
    if not available():
        return None
    if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
        return None
    if latitude == 0 and longitude == 0:
        return None  # an unset GPS position, not the Gulf of Guinea
    name = _zone_name(round(latitude, _POSITION_DECIMALS), round(longitude, _POSITION_DECIMALS))
    if name is None:
        return None
    zone = _zone(name)
    if zone is None:
        return None
    offset = utc.replace(tzinfo=UTC).astimezone(zone).utcoffset()
    if offset is None:
        return None
    return name, int(offset.total_seconds() // 60)
