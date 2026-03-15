"""Write and merge XMP sidecar files with keyword metadata."""

from xml.etree import ElementTree as ET
from pathlib import Path

NS_X = "adobe:ns:meta/"
NS_RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
NS_DC = "http://purl.org/dc/elements/1.1/"
NS_LR = "http://ns.adobe.com/lightroom/1.0/"

# Register namespaces so ET preserves prefixes on output
ET.register_namespace("x", NS_X)
ET.register_namespace("rdf", NS_RDF)
ET.register_namespace("dc", NS_DC)
ET.register_namespace("lr", NS_LR)


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


def write_xmp_sidecar(xmp_path, flat_keywords, hierarchical_keywords):
    """Write or merge keywords into an XMP sidecar file.

    Args:
        xmp_path: Path to the .xmp file (created if missing, merged if exists)
        flat_keywords: set of keyword strings for dc:subject
        hierarchical_keywords: set of pipe-delimited hierarchy strings for lr:hierarchicalSubject
    """
    path = Path(xmp_path)

    if path.exists():
        tree = ET.parse(path)
        root = tree.getroot()
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

    # Ensure namespace declarations on Description
    desc.set(f"xmlns:dc", NS_DC)
    desc.set(f"xmlns:lr", NS_LR)

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
