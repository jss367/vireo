"""Read-only access to a sidecar: keywords, Vireo's markers, sync-review fields."""

import logging
from pathlib import Path
from xml.etree import ElementTree as ET

from xmp.location_marker import LOCATION_KEYWORDS_MARKER, LOCATION_KEYWORDS_OWNED
from xmp.namespaces import NS_DC, NS_EXIF, NS_LR, NS_RDF, NS_VIREO, NS_XMP, NS_XMPDM
from xmp.photo_scope import (
    _get_plain_property_value,
    _get_property,
    _photo_scoped_bags,
    _register_document_uri,
    _top_descriptions,
)
from xmp.rdf import _li_value

# Every module of the package logs as ``xmp``, the logger the single-module
# version used.
log = logging.getLogger("xmp")


def _parse_xmp(xmp_path):
    """Parse an XMP file, returning (root, tree) or None if missing/corrupt."""
    path = Path(xmp_path)
    if not path.exists():
        return None

    try:
        tree = ET.parse(path)
    except ET.ParseError:
        log.warning("Corrupt XMP file: %s", xmp_path)
        return None

    root = tree.getroot()
    _register_document_uri(root, xmp_path)
    return root, tree


# ── Public API ──────────────────────────────────────────────────────────

def read_keywords(xmp_path):
    """Read dc:subject keywords from an XMP sidecar file.

    Args:
        xmp_path: path to .xmp file

    Returns:
        set of keyword strings (empty if file missing or corrupt)
    """
    result = _parse_xmp(xmp_path)
    if result is None:
        return set()

    root, _tree = result
    keywords = set()
    for bag in _photo_scoped_bags(root, f"{{{NS_DC}}}subject"):
        for li in bag.findall(f"{{{NS_RDF}}}li"):
            value = _li_value(li)
            if value:
                keywords.add(value)
    return keywords


def read_hierarchical_keywords(xmp_path):
    """Read lr:hierarchicalSubject from an XMP sidecar.

    Returns a list of pipe-delimited hierarchy strings, e.g. ['Birds|Raptors|Black kite'].
    """
    result = _parse_xmp(xmp_path)
    if result is None:
        return []

    root, _tree = result
    results = []
    for bag in _photo_scoped_bags(root, f"{{{NS_LR}}}hierarchicalSubject"):
        for li in bag.findall(f"{{{NS_RDF}}}li"):
            value = _li_value(li)
            if value:
                results.append(value)
    return results


def read_vireo_location_keywords(xmp_path):
    """Return the location keyword path Vireo wrote into this sidecar.

    ``None`` when the sidecar is missing, unreadable, or was never written by
    Vireo's location-keyword sync. Import callers use it to tell the entries
    Vireo authored from the ones the user typed in Lightroom.
    """
    result = _parse_xmp(xmp_path)
    if result is None:
        return None
    root, _tree = result
    return _get_property(root, LOCATION_KEYWORDS_MARKER)


def read_vireo_location_keywords_owned(xmp_path):
    """Return the ``vireo:locationKeywordsOwned`` companion value or ``None``.

    ``None`` covers both a missing sidecar and a sidecar that carries the
    marker but no ownership record -- treat that case as "both were ours"
    the way :func:`_parse_location_keywords_owned` does, so import callers
    keep the pre-fix skip behaviour on older sidecars.
    """
    result = _parse_xmp(xmp_path)
    if result is None:
        return None
    root, _tree = result
    return _get_property(root, LOCATION_KEYWORDS_OWNED)


def _parse_gps_coordinate(value):
    """Parse a common XMP GPS coordinate into decimal degrees.

    XMP commonly stores coordinates as ``degrees,minutesN`` but files from
    other tools may use decimal degrees or a three-part DMS value.  Preview
    callers need a friendly value without rejecting an otherwise readable
    sidecar just because its coordinate spelling is unfamiliar.
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None

    hemisphere = text[-1:].upper()
    if hemisphere in {"N", "S", "E", "W"}:
        text = text[:-1].strip()
    else:
        hemisphere = None

    try:
        parts = [float(part.strip()) for part in text.split(",")]
        if len(parts) == 1:
            decimal = parts[0]
        elif len(parts) == 2:
            decimal = abs(parts[0]) + parts[1] / 60.0
            if parts[0] < 0:
                decimal *= -1
        elif len(parts) == 3:
            decimal = abs(parts[0]) + parts[1] / 60.0 + parts[2] / 3600.0
            if parts[0] < 0:
                decimal *= -1
        else:
            return None
    except (TypeError, ValueError):
        return None

    if hemisphere in {"S", "W"}:
        decimal = -abs(decimal)
    elif hemisphere in {"N", "E"}:
        decimal = abs(decimal)
    return decimal


def _sync_preview_gps_pair(root, namespace=NS_EXIF, prefix="", vireo_owns=False):
    """Read one current or backed-up GPS pair for sync-review display.

    When Vireo owns the current coordinate (``vireo:gpsSource'' is
    set on the sidecar), read only Vireo's plain occurrence so the
    preview reflects the value Vireo just wrote -- otherwise a
    higher-scoring qualified sibling ``foo:source="camera"'' would
    still be returned as authoritative while Vireo's marker says
    ``assigned''. When no Vireo marker is present, fall back to the
    default ranking so a genuinely user-authored qualified value
    still wins.
    """
    def read(name):
        if vireo_owns:
            plain = _get_plain_property_value(root, name)
            if plain is not None:
                return plain
        return _get_property(root, name)

    raw_latitude = read(f"{{{namespace}}}{prefix}GPSLatitude")
    raw_longitude = read(f"{{{namespace}}}{prefix}GPSLongitude")
    if raw_latitude is None and raw_longitude is None:
        return None
    return {
        "latitude": _parse_gps_coordinate(raw_latitude),
        "longitude": _parse_gps_coordinate(raw_longitude),
        "raw_latitude": raw_latitude,
        "raw_longitude": raw_longitude,
    }


def read_sync_preview_metadata(xmp_path):
    """Read the sidecar fields shown by the pending-changes review.

    This intentionally parses a sidecar once per photo so a review containing
    several change types does not repeatedly touch the filesystem.  Missing
    and malformed sidecars are distinguished: that difference matters in a
    screen whose purpose is to explain exactly what will be written.
    """
    path = Path(xmp_path)
    empty = {
        "status": "missing",
        "keywords": set(),
        "hierarchical_keywords": set(),
        "rating": None,
        "rating_writable": False,
        "flag": None,
        "location": None,
        "previous_location": None,
        "location_source": None,
        "location_keywords": None,
        "edit_recipe": None,
    }
    try:
        root = ET.parse(path).getroot()
    except FileNotFoundError:
        # Parsing directly avoids a separate stat() before every open.  That
        # distinction is material when a large sync review lives on SMB/NFS:
        # one extra network round trip per photo can add minutes.
        return empty
    except (ET.ParseError, OSError):
        return {**empty, "status": "unreadable"}
    _register_document_uri(root, path)

    keywords = set()
    for bag in _photo_scoped_bags(root, f"{{{NS_DC}}}subject"):
        for li in bag.findall(f"{{{NS_RDF}}}li"):
            value = _li_value(li)
            if value:
                keywords.add(value)

    hierarchical_keywords = set()
    for bag in _photo_scoped_bags(root, f"{{{NS_LR}}}hierarchicalSubject"):
        for li in bag.findall(f"{{{NS_RDF}}}li"):
            value = _li_value(li)
            if value:
                hierarchical_keywords.add(value)

    if not _top_descriptions(root):
        # The sidecar parsed cleanly, so rating writes will land here even
        # though no Description currently belongs to the photo -- either
        # because the packet has ambiguous non-empty subjects, or because it
        # simply carries no top-level Descriptions yet. ``SidecarEditor.set_rating``
        # creates a fresh empty-subject Description in that case, so the
        # rating-only sync preview must report the write as it would happen,
        # not as "unchanged".
        return {
            **empty,
            "status": "ok",
            "keywords": keywords,
            "hierarchical_keywords": hierarchical_keywords,
            "rating_writable": True,
        }

    pick_to_flag = {"1": "flagged", "0": "none", "-1": "rejected"}
    raw_pick = _get_property(root, f"{{{NS_XMPDM}}}pick")
    location_source = _get_property(root, f"{{{NS_VIREO}}}gpsSource")
    return {
        "status": "ok",
        "keywords": keywords,
        "hierarchical_keywords": hierarchical_keywords,
        "rating": _get_property(root, f"{{{NS_XMP}}}Rating"),
        "rating_writable": True,
        "flag": pick_to_flag.get(raw_pick, raw_pick),
        "location": _sync_preview_gps_pair(
            root, vireo_owns=location_source is not None,
        ),
        "previous_location": _sync_preview_gps_pair(
            root, namespace=NS_VIREO, prefix="previous",
        ),
        "location_source": location_source,
        "location_keywords": _get_property(root, LOCATION_KEYWORDS_MARKER),
        "edit_recipe": _get_property(root, f"{{{NS_VIREO}}}editRecipe"),
    }
