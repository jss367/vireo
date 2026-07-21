"""Consolidated XMP sidecar operations.

Provides read/write/merge/remove for XMP keyword and rating metadata.
All XMP namespace constants and helpers live here as the single source of truth.
"""

import logging
import math
from pathlib import Path
from xml.etree import ElementTree as ET

from keyword_normalization import keyword_match_key

log = logging.getLogger(__name__)

# ── Namespace constants (single source of truth) ────────────────────────
NS_X = "adobe:ns:meta/"
NS_RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
NS_DC = "http://purl.org/dc/elements/1.1/"
NS_LR = "http://ns.adobe.com/lightroom/1.0/"
NS_XMP = "http://ns.adobe.com/xap/1.0/"
NS_XMPDM = "http://ns.adobe.com/xmp/1.0/DynamicMedia/"
NS_EXIF = "http://ns.adobe.com/exif/1.0/"
NS_VIREO = "https://vireo.app/ns/1.0/"

# Register namespaces so ET preserves prefixes on output
ET.register_namespace("x", NS_X)
ET.register_namespace("rdf", NS_RDF)
ET.register_namespace("dc", NS_DC)
ET.register_namespace("lr", NS_LR)
ET.register_namespace("xmp", NS_XMP)
ET.register_namespace("xmpDM", NS_XMPDM)
ET.register_namespace("crs", "http://ns.adobe.com/camera-raw-settings/1.0/")
ET.register_namespace("photoshop", "http://ns.adobe.com/photoshop/1.0/")
ET.register_namespace("exif", NS_EXIF)
ET.register_namespace("tiff", "http://ns.adobe.com/tiff/1.0/")
ET.register_namespace("aux", "http://ns.adobe.com/exif/1.0/aux/")
ET.register_namespace("vireo", NS_VIREO)


# ── Private helpers ─────────────────────────────────────────────────────

def _get_or_create_bag(parent, tag_ns, tag_name):
    """Find or create an rdf:Bag under a namespaced element."""
    tag = f"{{{tag_ns}}}{tag_name}"
    elem = parent.find(tag)
    if elem is None:
        elem = ET.SubElement(parent, tag)
    bag = elem.find(f"{{{NS_RDF}}}Bag")
    if bag is None:
        bag = ET.SubElement(elem, f"{{{NS_RDF}}}Bag")
    return bag


def _read_bag_values(bag):
    """Read all rdf:li values from a bag."""
    values = set()
    for li in bag.findall(f"{{{NS_RDF}}}li"):
        if li.text:
            values.add(li.text)
    return values


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

    return tree.getroot(), tree


def _load_or_create_xmp(xmp_path):
    """Load an XMP tree, or create a minimal sidecar tree."""
    path = Path(xmp_path)

    if path.exists():
        try:
            tree = ET.parse(path)
            return tree.getroot(), tree
        except ET.ParseError:
            log.warning("Corrupt XMP file %s — creating new sidecar", path)

    root = ET.Element(f"{{{NS_X}}}xmpmeta")
    tree = ET.ElementTree(root)
    return root, tree


def _get_or_create_description(root):
    """Find or create the first rdf:Description in an XMP tree."""
    rdf = root.find(f"{{{NS_RDF}}}RDF")
    if rdf is None:
        rdf = ET.SubElement(root, f"{{{NS_RDF}}}RDF")

    desc = rdf.find(f"{{{NS_RDF}}}Description")
    if desc is None:
        desc = ET.SubElement(rdf, f"{{{NS_RDF}}}Description")
    return desc


def _format_gps_coordinate(value, positive_ref, negative_ref):
    """Return an XMP GPSCoordinate string such as ``48,51.398N``."""
    ref = positive_ref if value >= 0 else negative_ref
    absolute = abs(float(value))
    degrees = int(absolute)
    minutes = (absolute - degrees) * 60.0
    return f"{degrees},{minutes:.6f}{ref}"


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
    for li in root.findall(f".//{{{NS_DC}}}subject/{{{NS_RDF}}}Bag/{{{NS_RDF}}}li"):
        if li.text:
            keywords.add(li.text)
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
    for li in root.findall(
        f".//{{{NS_LR}}}hierarchicalSubject/{{{NS_RDF}}}Bag/{{{NS_RDF}}}li"
    ):
        if li.text:
            results.append(li.text)
    return results


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


def _sync_preview_gps_pair(desc, namespace=NS_EXIF, prefix=""):
    """Read one current or backed-up GPS pair for sync-review display."""
    lat_attr = f"{{{namespace}}}{prefix}GPSLatitude"
    lon_attr = f"{{{namespace}}}{prefix}GPSLongitude"
    raw_latitude = desc.get(lat_attr)
    raw_longitude = desc.get(lon_attr)
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
        "edit_recipe": None,
    }
    try:
        if not path.exists():
            return empty
    except OSError:
        return {**empty, "status": "unreadable"}

    try:
        root = ET.parse(path).getroot()
    except (ET.ParseError, OSError):
        return {**empty, "status": "unreadable"}

    keywords = set()
    for li in root.findall(
        f".//{{{NS_DC}}}subject/{{{NS_RDF}}}Bag/{{{NS_RDF}}}li"
    ):
        if li.text:
            keywords.add(li.text)

    hierarchical_keywords = set()
    for li in root.findall(
        f".//{{{NS_LR}}}hierarchicalSubject/{{{NS_RDF}}}Bag/{{{NS_RDF}}}li"
    ):
        if li.text:
            hierarchical_keywords.add(li.text)

    desc = root.find(f".//{{{NS_RDF}}}Description")
    if desc is None:
        return {
            **empty,
            "status": "ok",
            "keywords": keywords,
            "hierarchical_keywords": hierarchical_keywords,
        }

    pick_to_flag = {"1": "flagged", "0": "none", "-1": "rejected"}
    raw_pick = desc.get(f"{{{NS_XMPDM}}}pick")
    return {
        "status": "ok",
        "keywords": keywords,
        "hierarchical_keywords": hierarchical_keywords,
        "rating": desc.get(f"{{{NS_XMP}}}Rating"),
        "rating_writable": True,
        "flag": pick_to_flag.get(raw_pick, raw_pick),
        "location": _sync_preview_gps_pair(desc),
        "previous_location": _sync_preview_gps_pair(
            desc, namespace=NS_VIREO, prefix="previous",
        ),
        "location_source": desc.get(f"{{{NS_VIREO}}}gpsSource"),
        "edit_recipe": desc.get(f"{{{NS_VIREO}}}editRecipe"),
    }


def write_sidecar(xmp_path, flat_keywords, hierarchical_keywords):
    """Write or merge keywords into an XMP sidecar file.

    Args:
        xmp_path: Path to the .xmp file (created if missing, merged if exists)
        flat_keywords: set of keyword strings for dc:subject
        hierarchical_keywords: set of pipe-delimited hierarchy strings for lr:hierarchicalSubject
    """
    path = Path(xmp_path)

    if path.exists():
        try:
            tree = ET.parse(path)
            root = tree.getroot()
        except ET.ParseError:
            log.warning("Corrupt XMP file %s — creating new sidecar", path)
            root = ET.Element(f"{{{NS_X}}}xmpmeta")
            tree = ET.ElementTree(root)
    else:
        root = ET.Element(f"{{{NS_X}}}xmpmeta")
        tree = ET.ElementTree(root)

    # Find or create rdf:RDF
    rdf = root.find(f"{{{NS_RDF}}}RDF")
    if rdf is None:
        rdf = ET.SubElement(root, f"{{{NS_RDF}}}RDF")

    # Find or create rdf:Description
    desc = rdf.find(f"{{{NS_RDF}}}Description")
    if desc is None:
        desc = ET.SubElement(rdf, f"{{{NS_RDF}}}Description")

    # Merge flat keywords into dc:subject
    dc_bag = _get_or_create_bag(desc, NS_DC, "subject")
    existing_flat = _read_bag_values(dc_bag)
    for kw in sorted(flat_keywords - existing_flat):
        li = ET.SubElement(dc_bag, f"{{{NS_RDF}}}li")
        li.text = kw

    # Merge hierarchical keywords into lr:hierarchicalSubject
    lr_bag = _get_or_create_bag(desc, NS_LR, "hierarchicalSubject")
    existing_hier = _read_bag_values(lr_bag)
    for kw in sorted(hierarchical_keywords - existing_hier):
        li = ET.SubElement(lr_bag, f"{{{NS_RDF}}}li")
        li.text = kw

    # Write with XML declaration
    ET.indent(tree, space="  ")
    tree.write(xmp_path, xml_declaration=True, encoding="unicode")


def write_rating(xmp_path, rating):
    """Write xmp:Rating attribute to an XMP sidecar.

    No-op if the file does not exist (we don't create an XMP just for a rating).
    """
    path = Path(xmp_path)

    if path.exists():
        try:
            tree = ET.parse(path)
            root = tree.getroot()
        except ET.ParseError:
            return
    else:
        return  # Don't create XMP just for rating

    # Find rdf:Description and set xmp:Rating attribute
    desc = root.find(f".//{{{NS_RDF}}}Description")
    if desc is not None:
        desc.set(f"{{{NS_XMP}}}Rating", str(rating))
        ET.indent(tree, space="  ")
        tree.write(xmp_path, xml_declaration=True, encoding="unicode")


def write_pick_flag(xmp_path, flag):
    """Write Lightroom-compatible pick/reject flag metadata.

    Vireo stores flags as ``flagged`` / ``none`` / ``rejected``. Lightroom
    Classic 13.2+ persists the equivalent pick state in ``xmpDM:pick`` using
    values ``1`` / ``0`` / ``-1``.
    """
    values = {
        "flagged": "1",
        "none": "0",
        "rejected": "-1",
    }
    if flag not in values:
        raise ValueError("flag must be 'none', 'flagged', or 'rejected'")

    root, tree = _load_or_create_xmp(xmp_path)
    desc = _get_or_create_description(root)
    desc.set(f"{{{NS_XMPDM}}}pick", values[flag])
    ET.indent(tree, space="  ")
    tree.write(xmp_path, xml_declaration=True, encoding="unicode")


def write_gps_location(xmp_path, latitude, longitude, source="assigned"):
    """Write Lightroom-compatible GPS coordinates to an XMP sidecar.

    A small Vireo marker records that these GPS fields were written by Vireo,
    so ``remove_vireo_gps_location`` can clear stale assigned-location GPS
    without touching unrelated GPS metadata from another application.
    """
    lat = float(latitude)
    lon = float(longitude)
    if not math.isfinite(lat) or not (-90.0 <= lat <= 90.0):
        raise ValueError("latitude must be between -90 and 90")
    if not math.isfinite(lon) or not (-180.0 <= lon <= 180.0):
        raise ValueError("longitude must be between -180 and 180")

    root, tree = _load_or_create_xmp(xmp_path)
    desc = _get_or_create_description(root)
    marker = f"{{{NS_VIREO}}}gpsSource"
    exif_attrs = {
        "GPSLatitude": f"{{{NS_EXIF}}}GPSLatitude",
        "GPSLongitude": f"{{{NS_EXIF}}}GPSLongitude",
        "GPSMapDatum": f"{{{NS_EXIF}}}GPSMapDatum",
        "GPSVersionID": f"{{{NS_EXIF}}}GPSVersionID",
    }

    # First Vireo write: preserve any GPS another app had already written so
    # clearing the Vireo-assigned location can restore it. Rewrites of an
    # existing Vireo GPS keep the original backup.
    if marker not in desc.attrib:
        for name, attr in exif_attrs.items():
            if attr in desc.attrib:
                desc.set(f"{{{NS_VIREO}}}previous{name}", desc.attrib[attr])

    desc.set(exif_attrs["GPSLatitude"], _format_gps_coordinate(lat, "N", "S"))
    desc.set(exif_attrs["GPSLongitude"], _format_gps_coordinate(lon, "E", "W"))
    desc.set(exif_attrs["GPSMapDatum"], "WGS-84")
    desc.set(exif_attrs["GPSVersionID"], "2.3.0.0")
    desc.set(marker, source or "assigned")
    ET.indent(tree, space="  ")
    tree.write(xmp_path, xml_declaration=True, encoding="unicode")


def remove_vireo_gps_location(xmp_path):
    """Remove GPS fields only when Vireo previously wrote them."""
    result = _parse_xmp(xmp_path)
    if result is None:
        return False

    root, tree = result
    desc = root.find(f".//{{{NS_RDF}}}Description")
    if desc is None:
        return False

    marker = f"{{{NS_VIREO}}}gpsSource"
    if marker not in desc.attrib:
        return False

    removed = False
    for name in ("GPSLatitude", "GPSLongitude", "GPSMapDatum", "GPSVersionID"):
        gps_attr = f"{{{NS_EXIF}}}{name}"
        previous_attr = f"{{{NS_VIREO}}}previous{name}"
        if previous_attr in desc.attrib:
            desc.set(gps_attr, desc.attrib[previous_attr])
            del desc.attrib[previous_attr]
            removed = True
        elif gps_attr in desc.attrib:
            del desc.attrib[gps_attr]
            removed = True

    if marker in desc.attrib:
        del desc.attrib[marker]
        removed = True

    if removed:
        ET.indent(tree, space="  ")
        tree.write(xmp_path, xml_declaration=True, encoding="unicode")
    return removed


def write_edit_recipe(xmp_path, recipe_json):
    """Write or clear Vireo's non-destructive edit recipe marker."""
    recipe_json = recipe_json or ""
    recipe_attr = f"{{{NS_VIREO}}}editRecipe"
    version_attr = f"{{{NS_VIREO}}}editRecipeSchema"

    if recipe_json:
        root, tree = _load_or_create_xmp(xmp_path)
        desc = _get_or_create_description(root)
        desc.set(recipe_attr, recipe_json)
        desc.set(version_attr, "1")
    else:
        result = _parse_xmp(xmp_path)
        if result is None:
            return False
        root, tree = result
        desc = root.find(f".//{{{NS_RDF}}}Description")
        if desc is None:
            return False
        removed = False
        for attr in (recipe_attr, version_attr):
            if attr in desc.attrib:
                del desc.attrib[attr]
                removed = True
        if not removed:
            return False

    ET.indent(tree, space="  ")
    tree.write(xmp_path, xml_declaration=True, encoding="unicode")
    return True


def remove_keywords(xmp_path, keywords_to_remove, *, hierarchical=True):
    """Remove keywords from dc:subject and lr:hierarchicalSubject in an XMP file.

    Args:
        xmp_path: path to the .xmp sidecar
        keywords_to_remove: set of keyword strings to remove
        hierarchical: When True (default), a hierarchical entry is deleted
            if any of its pipe-delimited segments matches — used for real
            keyword removals so ``Animals|Birds|Hawk`` is dropped when the
            user removes ``Hawk``. When False, hierarchies are left
            untouched and only the flat ``dc:subject`` bag is pruned — used
            by the sync path to canonicalize add-equivalent flat variants
            (e.g. strip a legacy ``'apapane`` before writing the clean
            ``apapane``) without deleting an unrelated hierarchy whose
            leaf happens to match the added flat keyword.
    """
    path = Path(xmp_path)
    if not path.exists():
        return

    try:
        tree = ET.parse(path)
    except ET.ParseError:
        log.warning("Corrupt XMP file, cannot remove keywords: %s", xmp_path)
        return

    root = tree.getroot()
    # Compare using the same normalized key add_keyword() uses on insert so a
    # DB removal of `apapane` also clears a sidecar `‘apapane` (or `´apapane`
    # / whitespace/casing variants). A plain `.lower()` comparison would leave
    # the quoted <rdf:li> in place; the next XMP import path would then
    # re-add the keyword the user removed. Drop empty keys so removing a
    # keyword whose name normalizes to `""` doesn't accidentally match empty
    # hierarchical segments (e.g. `"|Birds|"` -> `["", "Birds", ""]`).
    remove_keys = {keyword_match_key(kw) for kw in keywords_to_remove}
    remove_keys.discard("")
    if not remove_keys:
        return
    removed = []

    # Remove from dc:subject bag
    for bag in root.findall(f".//{{{NS_DC}}}subject/{{{NS_RDF}}}Bag"):
        for li in bag.findall(f"{{{NS_RDF}}}li"):
            if li.text and keyword_match_key(li.text) in remove_keys:
                removed.append(li.text)
                bag.remove(li)

    # Remove from lr:hierarchicalSubject bag (matches if any segment matches).
    # Skipped when hierarchical=False: the sync path uses the flat-only mode
    # to canonicalize add-equivalent variants without accidentally deleting
    # unrelated hierarchies that share a segment with the added flat leaf.
    if hierarchical:
        for bag in root.findall(f".//{{{NS_LR}}}hierarchicalSubject/{{{NS_RDF}}}Bag"):
            for li in bag.findall(f"{{{NS_RDF}}}li"):
                if li.text:
                    # Hierarchical keywords use pipe-delimited paths like "Animals|Birds|Hawk"
                    segments = {keyword_match_key(s) for s in li.text.split("|")}
                    segments.discard("")
                    if segments & remove_keys:
                        removed.append(li.text)
                        bag.remove(li)

    if removed:
        ET.indent(tree, space="  ")
        tree.write(xmp_path, xml_declaration=True, encoding="unicode")
        log.info("Removed keywords from %s: %s", xmp_path, removed)
