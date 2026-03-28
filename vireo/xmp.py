"""Consolidated XMP sidecar operations.

Provides read/write/merge/remove for XMP keyword and rating metadata.
All XMP namespace constants and helpers live here as the single source of truth.
"""

import logging
from pathlib import Path
from xml.etree import ElementTree as ET

log = logging.getLogger(__name__)

# ── Namespace constants (single source of truth) ────────────────────────
NS_X = "adobe:ns:meta/"
NS_RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
NS_DC = "http://purl.org/dc/elements/1.1/"
NS_LR = "http://ns.adobe.com/lightroom/1.0/"
NS_XMP = "http://ns.adobe.com/xap/1.0/"

# Register namespaces so ET preserves prefixes on output
ET.register_namespace("x", NS_X)
ET.register_namespace("rdf", NS_RDF)
ET.register_namespace("dc", NS_DC)
ET.register_namespace("lr", NS_LR)
ET.register_namespace("xmp", NS_XMP)
ET.register_namespace("crs", "http://ns.adobe.com/camera-raw-settings/1.0/")
ET.register_namespace("photoshop", "http://ns.adobe.com/photoshop/1.0/")
ET.register_namespace("exif", "http://ns.adobe.com/exif/1.0/")
ET.register_namespace("tiff", "http://ns.adobe.com/tiff/1.0/")
ET.register_namespace("aux", "http://ns.adobe.com/exif/1.0/aux/")


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


def remove_keywords(xmp_path, keywords_to_remove):
    """Remove keywords from dc:subject and lr:hierarchicalSubject in an XMP file.

    Args:
        xmp_path: path to the .xmp sidecar
        keywords_to_remove: set of keyword strings to remove
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
    remove_lower = {kw.lower() for kw in keywords_to_remove}
    removed = []

    # Remove from dc:subject bag
    for bag in root.findall(f".//{{{NS_DC}}}subject/{{{NS_RDF}}}Bag"):
        for li in bag.findall(f"{{{NS_RDF}}}li"):
            if li.text and li.text.lower() in remove_lower:
                removed.append(li.text)
                bag.remove(li)

    # Remove from lr:hierarchicalSubject bag (matches if any segment matches)
    for bag in root.findall(f".//{{{NS_LR}}}hierarchicalSubject/{{{NS_RDF}}}Bag"):
        for li in bag.findall(f"{{{NS_RDF}}}li"):
            if li.text:
                # Hierarchical keywords use pipe-delimited paths like "Animals|Birds|Hawk"
                segments = {s.lower() for s in li.text.split("|")}
                if segments & remove_lower:
                    removed.append(li.text)
                    bag.remove(li)

    if removed:
        ET.indent(tree, space="  ")
        tree.write(xmp_path, xml_declaration=True, encoding="unicode")
        log.info("Removed keywords from %s: %s", xmp_path, removed)
