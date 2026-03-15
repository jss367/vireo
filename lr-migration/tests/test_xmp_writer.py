import os
import sys
import tempfile
from xml.etree import ElementTree as ET

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

NS = {
    "x": "adobe:ns:meta/",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "dc": "http://purl.org/dc/elements/1.1/",
    "lr": "http://ns.adobe.com/lightroom/1.0/",
}


def test_write_new_sidecar():
    """Creates a new XMP sidecar with keywords."""
    from xmp_writer import write_xmp_sidecar

    with tempfile.TemporaryDirectory() as tmpdir:
        xmp_path = os.path.join(tmpdir, "DSC_0001.xmp")

        write_xmp_sidecar(
            xmp_path,
            flat_keywords={"Black kite", "Raptors", "Birds"},
            hierarchical_keywords={"Birds|Raptors|Black kite"},
        )

        assert os.path.exists(xmp_path)

        tree = ET.parse(xmp_path)
        root = tree.getroot()

        # Check dc:subject contains flat keywords
        subjects = root.findall(".//dc:subject/rdf:Bag/rdf:li", NS)
        subject_values = {s.text for s in subjects}
        assert "Black kite" in subject_values
        assert "Raptors" in subject_values
        assert "Birds" in subject_values

        # Check lr:hierarchicalSubject
        hier = root.findall(".//lr:hierarchicalSubject/rdf:Bag/rdf:li", NS)
        hier_values = {h.text for h in hier}
        assert "Birds|Raptors|Black kite" in hier_values


def test_merge_into_existing_sidecar():
    """Merges keywords into an existing XMP sidecar without losing existing data."""
    from xmp_writer import write_xmp_sidecar

    with tempfile.TemporaryDirectory() as tmpdir:
        xmp_path = os.path.join(tmpdir, "DSC_0001.xmp")

        # Write initial sidecar
        existing_xmp = """<x:xmpmeta xmlns:x="adobe:ns:meta/">
  <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
    <rdf:Description
      xmlns:dc="http://purl.org/dc/elements/1.1/"
      xmlns:lr="http://ns.adobe.com/lightroom/1.0/"
      xmlns:crs="http://ns.adobe.com/camera-raw-settings/1.0/"
      crs:Version="15.0">
      <dc:subject>
        <rdf:Bag>
          <rdf:li>Existing tag</rdf:li>
        </rdf:Bag>
      </dc:subject>
    </rdf:Description>
  </rdf:RDF>
</x:xmpmeta>"""
        with open(xmp_path, "w") as f:
            f.write(existing_xmp)

        write_xmp_sidecar(
            xmp_path,
            flat_keywords={"New tag"},
            hierarchical_keywords={"Category|New tag"},
        )

        tree = ET.parse(xmp_path)
        root = tree.getroot()

        # Should have both old and new keywords
        subjects = root.findall(".//dc:subject/rdf:Bag/rdf:li", NS)
        subject_values = {s.text for s in subjects}
        assert "Existing tag" in subject_values
        assert "New tag" in subject_values

        # crs:Version attribute should be preserved
        desc = root.find(".//rdf:Description", NS)
        assert desc.get("{http://ns.adobe.com/camera-raw-settings/1.0/}Version") == "15.0"
