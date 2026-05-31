"""Tests for the consolidated vireo.xmp module."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from xmp import (
    read_hierarchical_keywords,
    read_keywords,
    remove_keywords,
    remove_vireo_gps_location,
    write_gps_location,
    write_pick_flag,
    write_rating,
    write_sidecar,
)

# ── Fixtures ────────────────────────────────────────────────────────────

SAMPLE_XMP = """\
<?xml version='1.0' encoding='utf-8'?>
<x:xmpmeta xmlns:x="adobe:ns:meta/">
  <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
    <rdf:Description crs:Version="15.1" xmlns:crs="http://ns.adobe.com/camera-raw-settings/1.0/">
      <dc:subject xmlns:dc="http://purl.org/dc/elements/1.1/">
        <rdf:Bag>
          <rdf:li>Bird</rdf:li>
          <rdf:li>Raptor</rdf:li>
        </rdf:Bag>
      </dc:subject>
      <lr:hierarchicalSubject xmlns:lr="http://ns.adobe.com/lightroom/1.0/">
        <rdf:Bag>
          <rdf:li>Animals|Birds|Raptor</rdf:li>
          <rdf:li>Location|Forest</rdf:li>
        </rdf:Bag>
      </lr:hierarchicalSubject>
    </rdf:Description>
  </rdf:RDF>
</x:xmpmeta>"""


@pytest.fixture
def sample_xmp(tmp_path):
    """Create a sample XMP file and return its path."""
    p = tmp_path / "photo.xmp"
    p.write_text(SAMPLE_XMP)
    return str(p)


@pytest.fixture
def missing_xmp(tmp_path):
    """Return a path to a non-existent XMP file."""
    return str(tmp_path / "does_not_exist.xmp")


# ── read_keywords ───────────────────────────────────────────────────────

def test_read_keywords_normal(sample_xmp):
    result = read_keywords(sample_xmp)
    assert result == {"Bird", "Raptor"}


def test_read_keywords_missing_file(missing_xmp):
    result = read_keywords(missing_xmp)
    assert result == set()


def test_read_keywords_corrupt_file(tmp_path):
    p = tmp_path / "corrupt.xmp"
    p.write_text("<<<not valid xml>>>")
    result = read_keywords(str(p))
    assert result == set()


# ── read_hierarchical_keywords ──────────────────────────────────────────

def test_read_hierarchical_keywords_normal(sample_xmp):
    result = read_hierarchical_keywords(sample_xmp)
    assert set(result) == {"Animals|Birds|Raptor", "Location|Forest"}


def test_read_hierarchical_keywords_missing_file(missing_xmp):
    result = read_hierarchical_keywords(missing_xmp)
    assert result == []


# ── write_sidecar ───────────────────────────────────────────────────────

def test_write_sidecar_new_file(tmp_path):
    p = str(tmp_path / "new.xmp")
    write_sidecar(p, {"Eagle", "Hawk"}, {"Animals|Birds|Eagle"})

    # Verify written keywords are readable
    assert read_keywords(p) == {"Eagle", "Hawk"}
    assert "Animals|Birds|Eagle" in read_hierarchical_keywords(p)


def test_write_sidecar_merge_existing(sample_xmp):
    # Merge new keywords into existing file
    write_sidecar(sample_xmp, {"Eagle"}, {"Animals|Birds|Eagle"})

    # Original keywords should still be present
    kw = read_keywords(sample_xmp)
    assert "Bird" in kw
    assert "Raptor" in kw
    assert "Eagle" in kw

    hier = read_hierarchical_keywords(sample_xmp)
    assert "Animals|Birds|Raptor" in hier
    assert "Animals|Birds|Eagle" in hier

    # crs:Version attribute should be preserved
    with open(sample_xmp) as f:
        content = f.read()
    assert "crs:Version" in content


# ── write_rating ────────────────────────────────────────────────────────

def test_write_rating_normal(sample_xmp):
    write_rating(sample_xmp, 4)

    with open(sample_xmp) as f:
        content = f.read()
    assert 'xmp:Rating="4"' in content


def test_write_rating_no_file(missing_xmp):
    # Should be a no-op, not raise
    write_rating(missing_xmp, 3)
    assert not os.path.exists(missing_xmp)


# ── write_pick_flag ─────────────────────────────────────────────────────

def test_write_pick_flag_existing_sidecar(sample_xmp):
    write_pick_flag(sample_xmp, "flagged")

    with open(sample_xmp) as f:
        content = f.read()
    assert 'xmpDM:pick="1"' in content


def test_write_pick_flag_rejected_creates_sidecar(missing_xmp):
    write_pick_flag(missing_xmp, "rejected")

    with open(missing_xmp) as f:
        content = f.read()
    assert 'xmpDM:pick="-1"' in content


# ── write_gps_location / remove_vireo_gps_location ──────────────────────

def test_write_gps_location_writes_exif_gps(sample_xmp):
    write_gps_location(sample_xmp, 48.8566, 2.3522)

    with open(sample_xmp) as f:
        content = f.read()
    assert 'exif:GPSLatitude="48,51.396000N"' in content
    assert 'exif:GPSLongitude="2,21.132000E"' in content
    assert 'exif:GPSMapDatum="WGS-84"' in content
    assert 'vireo:gpsSource="assigned"' in content


def test_remove_vireo_gps_location_only_when_marked(sample_xmp):
    write_gps_location(sample_xmp, 48.8566, 2.3522)

    assert remove_vireo_gps_location(sample_xmp) is True

    with open(sample_xmp) as f:
        content = f.read()
    assert "GPSLatitude" not in content
    assert "GPSLongitude" not in content
    assert "vireo:gpsSource" not in content


def test_remove_vireo_gps_location_preserves_unmarked_gps(sample_xmp):
    from xml.etree import ElementTree as ET

    ns_rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    ns_exif = "http://ns.adobe.com/exif/1.0/"
    tree = ET.parse(sample_xmp)
    desc = tree.getroot().find(f".//{{{ns_rdf}}}Description")
    desc.set(f"{{{ns_exif}}}GPSLatitude", "48,51.396000N")
    desc.set(f"{{{ns_exif}}}GPSLongitude", "2,21.132000E")
    tree.write(sample_xmp, xml_declaration=True, encoding="unicode")

    assert remove_vireo_gps_location(sample_xmp) is False

    with open(sample_xmp) as f:
        content = f.read()
    assert 'exif:GPSLatitude="48,51.396000N"' in content
    assert 'exif:GPSLongitude="2,21.132000E"' in content


def test_remove_vireo_gps_location_restores_previous_gps(sample_xmp):
    from xml.etree import ElementTree as ET

    ns_rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    ns_exif = "http://ns.adobe.com/exif/1.0/"
    tree = ET.parse(sample_xmp)
    desc = tree.getroot().find(f".//{{{ns_rdf}}}Description")
    desc.set(f"{{{ns_exif}}}GPSLatitude", "40,46.974000N")
    desc.set(f"{{{ns_exif}}}GPSLongitude", "73,57.924000W")
    tree.write(sample_xmp, xml_declaration=True, encoding="unicode")

    write_gps_location(sample_xmp, 48.8566, 2.3522)
    assert remove_vireo_gps_location(sample_xmp) is True

    with open(sample_xmp) as f:
        content = f.read()
    assert 'exif:GPSLatitude="40,46.974000N"' in content
    assert 'exif:GPSLongitude="73,57.924000W"' in content
    assert "previousGPSLatitude" not in content
    assert "previousGPSLongitude" not in content
    assert "vireo:gpsSource" not in content


def test_write_gps_location_rejects_out_of_range_coords(sample_xmp):
    with pytest.raises(ValueError, match="latitude"):
        write_gps_location(sample_xmp, 91.0, 2.3522)
    with pytest.raises(ValueError, match="longitude"):
        write_gps_location(sample_xmp, 48.8566, 181.0)


# ── remove_keywords ─────────────────────────────────────────────────────

def test_remove_keywords_normal(sample_xmp):
    remove_keywords(sample_xmp, {"bird"})  # case-insensitive

    kw = read_keywords(sample_xmp)
    assert "Bird" not in kw
    assert "Raptor" in kw

    # Hierarchical entry containing "Birds" segment should NOT be removed
    # because we removed "bird", not "birds"
    hier = read_hierarchical_keywords(sample_xmp)
    assert "Animals|Birds|Raptor" in hier


def test_remove_keywords_removes_hierarchical(sample_xmp):
    # "Raptor" appears as a segment in "Animals|Birds|Raptor"
    remove_keywords(sample_xmp, {"Raptor"})

    kw = read_keywords(sample_xmp)
    assert "Raptor" not in kw

    hier = read_hierarchical_keywords(sample_xmp)
    assert "Animals|Birds|Raptor" not in hier
    # "Location|Forest" should remain
    assert "Location|Forest" in hier


def test_remove_keywords_no_file(missing_xmp):
    # Should be a no-op, not raise
    remove_keywords(missing_xmp, {"Bird"})
    assert not os.path.exists(missing_xmp)
