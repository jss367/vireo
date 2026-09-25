"""Sidecar reads and writes across the two XMP layouts in the wild.

Lightroom writes one rdf:Description with properties as attributes. ExifTool
writes one Description per namespace with simple values as child elements.
Every read and write must find a property in either form, or a write adds a
second, conflicting copy that ExifTool and Lightroom resolve differently.
"""

import json
import os
import shutil
import subprocess
import sys
from xml.etree import ElementTree as ET

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from xmp import (
    NS_DC,
    NS_EXIF,
    NS_LR,
    NS_RDF,
    NS_VIREO,
    NS_XMP,
    NS_XMPDM,
    SidecarEditor,
    _property_occurrences,
    read_hierarchical_keywords,
    read_sync_preview_metadata,
    read_vireo_location_keywords,
    remove_vireo_gps_location,
    write_gps_location,
    write_rating,
)

EXIFTOOL_XMP = f"""\
<x:xmpmeta xmlns:x='adobe:ns:meta/' x:xmptk='Image::ExifTool 13.55'>
<rdf:RDF xmlns:rdf='{NS_RDF}'>
 <rdf:Description rdf:about='' xmlns:dc='{NS_DC}'>
  <dc:subject><rdf:Bag><rdf:li>Heron</rdf:li></rdf:Bag></dc:subject>
 </rdf:Description>
 <rdf:Description rdf:about='' xmlns:exif='{NS_EXIF}'>
  <exif:GPSLatitude>10,30.0N</exif:GPSLatitude>
  <exif:GPSLongitude>20,15.0E</exif:GPSLongitude>
 </rdf:Description>
 <rdf:Description rdf:about='' xmlns:lr='{NS_LR}'>
  <lr:hierarchicalSubject><rdf:Bag><rdf:li>Birds|Heron</rdf:li></rdf:Bag></lr:hierarchicalSubject>
 </rdf:Description>
 <rdf:Description rdf:about='' xmlns:xmp='{NS_XMP}'>
  <xmp:Rating>3</xmp:Rating>
 </rdf:Description>
 <rdf:Description rdf:about='' xmlns:xmpDM='{NS_XMPDM}'>
  <xmpDM:pick>1</xmpDM:pick>
 </rdf:Description>
</rdf:RDF>
</x:xmpmeta>"""

ADOBE_XMP = f"""\
<x:xmpmeta xmlns:x="adobe:ns:meta/">
  <rdf:RDF xmlns:rdf="{NS_RDF}">
    <rdf:Description rdf:about=""
        xmlns:dc="{NS_DC}" xmlns:exif="{NS_EXIF}" xmlns:lr="{NS_LR}"
        xmlns:xmp="{NS_XMP}" xmlns:xmpDM="{NS_XMPDM}"
        exif:GPSLatitude="10,30.0N" exif:GPSLongitude="20,15.0E"
        xmp:Rating="3" xmpDM:pick="1">
      <dc:subject><rdf:Bag><rdf:li>Heron</rdf:li></rdf:Bag></dc:subject>
      <lr:hierarchicalSubject><rdf:Bag><rdf:li>Birds|Heron</rdf:li></rdf:Bag></lr:hierarchicalSubject>
    </rdf:Description>
  </rdf:RDF>
</x:xmpmeta>"""

RATING = f"{{{NS_XMP}}}Rating"
PICK = f"{{{NS_XMPDM}}}pick"
GPS_LATITUDE = f"{{{NS_EXIF}}}GPSLatitude"
GPS_LONGITUDE = f"{{{NS_EXIF}}}GPSLongitude"
SUBJECT = f"{{{NS_DC}}}subject"
HIERARCHICAL_SUBJECT = f"{{{NS_LR}}}hierarchicalSubject"


def _copies(path, name):
    """How many places store ``name`` across the sidecar's Descriptions."""
    return len(_property_occurrences(ET.parse(path).getroot(), name))


@pytest.fixture(params=["exiftool", "adobe"])
def layout_xmp(request, tmp_path):
    p = tmp_path / "photo.xmp"
    p.write_text(EXIFTOOL_XMP if request.param == "exiftool" else ADOBE_XMP)
    return str(p)


def test_read_sync_preview_metadata_reads_both_layouts(layout_xmp):
    metadata = read_sync_preview_metadata(layout_xmp)

    assert metadata["rating"] == "3"
    assert metadata["flag"] == "flagged"
    assert metadata["location"]["latitude"] == pytest.approx(10.5)
    assert metadata["location"]["longitude"] == pytest.approx(20.25)
    assert metadata["keywords"] == {"Heron"}
    assert metadata["hierarchical_keywords"] == {"Birds|Heron"}


def test_editor_updates_each_property_in_place_in_both_layouts(layout_xmp):
    editor = SidecarEditor(layout_xmp)
    editor.set_rating(5)
    editor.set_pick_flag("rejected")
    editor.set_gps_location(-33.5, -70.25)
    editor.add_keywords({"Egret"}, {"Birds|Egret"})
    assert editor.commit() is True

    for name in (RATING, PICK, GPS_LATITUDE, GPS_LONGITUDE,
                 SUBJECT, HIERARCHICAL_SUBJECT):
        assert _copies(layout_xmp, name) == 1, name

    metadata = read_sync_preview_metadata(layout_xmp)
    assert metadata["rating"] == "5"
    assert metadata["flag"] == "rejected"
    assert metadata["location"]["latitude"] == pytest.approx(-33.5)
    assert metadata["location"]["longitude"] == pytest.approx(-70.25)
    assert metadata["keywords"] == {"Heron", "Egret"}
    assert metadata["hierarchical_keywords"] == {"Birds|Heron", "Birds|Egret"}


def test_rewriting_the_same_values_is_a_no_op_in_both_layouts(layout_xmp):
    editor = SidecarEditor(layout_xmp)
    editor.set_rating(3)
    editor.set_pick_flag("flagged")
    editor.add_keywords({"Heron"}, {"Birds|Heron"})
    assert editor.commit() is False


def test_gps_backup_and_restore_in_both_layouts(layout_xmp):
    write_gps_location(layout_xmp, -33.5, -70.25)

    metadata = read_sync_preview_metadata(layout_xmp)
    assert metadata["location_source"] == "assigned"
    assert metadata["previous_location"]["raw_latitude"] == "10,30.0N"
    assert metadata["previous_location"]["raw_longitude"] == "20,15.0E"

    assert remove_vireo_gps_location(layout_xmp) is True

    metadata = read_sync_preview_metadata(layout_xmp)
    assert metadata["location"]["raw_latitude"] == "10,30.0N"
    assert metadata["location"]["raw_longitude"] == "20,15.0E"
    assert metadata["previous_location"] is None
    assert metadata["location_source"] is None
    assert _copies(layout_xmp, GPS_LATITUDE) == 1


def test_vireo_markers_stored_as_elements_are_honoured(tmp_path):
    """ExifTool rewriting a Vireo sidecar turns its markers into elements."""
    path = tmp_path / "photo.xmp"
    path.write_text(EXIFTOOL_XMP.replace(
        "</rdf:RDF>",
        f" <rdf:Description rdf:about='' xmlns:vireo='{NS_VIREO}'>\n"
        "  <vireo:gpsSource>assigned</vireo:gpsSource>\n"
        "  <vireo:locationKeywords>Birds|Heron</vireo:locationKeywords>\n"
        "  <vireo:locationKeywordsOwned>flat,hier</vireo:locationKeywordsOwned>\n"
        " </rdf:Description>\n</rdf:RDF>",
    ))
    path = str(path)
    assert read_vireo_location_keywords(path) == "Birds|Heron"

    editor = SidecarEditor(path)
    assert editor.remove_vireo_location_keywords() is True
    assert editor.remove_vireo_gps_location() is True
    editor.commit()

    assert read_vireo_location_keywords(path) is None
    assert read_hierarchical_keywords(path) == []
    metadata = read_sync_preview_metadata(path)
    assert metadata["location"] is None
    assert metadata["location_source"] is None


def test_editor_collapses_copies_left_by_an_earlier_write(tmp_path):
    """Sidecars already carrying two ratings or two bags end up with one."""
    path = tmp_path / "photo.xmp"
    path.write_text(EXIFTOOL_XMP.replace(
        "<rdf:Description rdf:about='' xmlns:dc",
        f"<rdf:Description rdf:about='' xmlns:xmp='{NS_XMP}' xmp:Rating='5'>\n"
        f"  <lr:hierarchicalSubject xmlns:lr='{NS_LR}'>"
        "<rdf:Bag><rdf:li>Birds|Egret</rdf:li></rdf:Bag></lr:hierarchicalSubject>\n"
        " </rdf:Description>\n <rdf:Description rdf:about='' xmlns:dc",
        1,
    ))
    path = str(path)

    editor = SidecarEditor(path)
    editor.set_rating(4)
    editor.add_keywords(set(), {"Birds|Kiwi"})
    editor.commit()

    assert _copies(path, RATING) == 1
    assert _copies(path, HIERARCHICAL_SUBJECT) == 1
    assert read_sync_preview_metadata(path)["rating"] == "4"
    assert set(read_hierarchical_keywords(path)) == {
        "Birds|Egret", "Birds|Heron", "Birds|Kiwi",
    }


def test_nested_struct_descriptions_are_not_treated_as_the_photo(tmp_path):
    """A struct's inner Description must not receive or supply photo values."""
    path = tmp_path / "photo.xmp"
    path.write_text(ADOBE_XMP.replace(
        "<dc:subject>",
        "<xmpMM:History xmlns:xmpMM='http://ns.adobe.com/xap/1.0/mm/'>"
        "<rdf:Seq><rdf:li>"
        f"<rdf:Description xmlns:xmp='{NS_XMP}' xmp:Rating='1'/>"
        "</rdf:li></rdf:Seq></xmpMM:History>\n      <dc:subject>",
    ))
    path = str(path)
    write_rating(path, 5)

    nested = [
        d for d in ET.parse(path).getroot().iter(f"{{{NS_RDF}}}Description")
        if d.get(RATING) == "1"
    ]
    assert len(nested) == 1
    assert read_sync_preview_metadata(path)["rating"] == "5"


def test_auxiliary_rdf_subject_is_ignored(tmp_path):
    """A Description of a different rdf:about is not the photo's."""
    path = tmp_path / "photo.xmp"
    path.write_text(EXIFTOOL_XMP.replace(
        "</rdf:RDF>",
        f" <rdf:Description rdf:about='#aux' xmlns:xmp='{NS_XMP}'"
        f" xmlns:exif='{NS_EXIF}'"
        " xmp:Rating='1' exif:GPSLatitude='40,0.0N' exif:GPSLongitude='40,0.0E'/>"
        "\n</rdf:RDF>",
    ))
    path = str(path)

    metadata = read_sync_preview_metadata(path)
    assert metadata["rating"] == "3"
    assert metadata["location"]["latitude"] == pytest.approx(10.5)
    assert metadata["location"]["longitude"] == pytest.approx(20.25)

    editor = SidecarEditor(path)
    editor.set_rating(5)
    editor.set_gps_location(-33.5, -70.25)
    editor.commit()

    root = ET.parse(path).getroot()
    aux = [
        d for d in root.iter(f"{{{NS_RDF}}}Description")
        if d.get(f"{{{NS_RDF}}}about") == "#aux"
    ]
    assert len(aux) == 1
    assert aux[0].get(RATING) == "1"
    assert aux[0].get(GPS_LATITUDE) == "40,0.0N"
    assert aux[0].get(GPS_LONGITUDE) == "40,0.0E"

    metadata = read_sync_preview_metadata(path)
    assert metadata["rating"] == "5"
    assert metadata["location"]["latitude"] == pytest.approx(-33.5)
    assert metadata["location"]["longitude"] == pytest.approx(-70.25)


def test_new_description_matches_existing_photo_subject(tmp_path):
    """A sidecar with only non-empty rdf:about keeps its subject on new writes."""
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about='uuid:photo'"
        f" xmlns:dc='{NS_DC}'><dc:subject><rdf:Bag>"
        f"<rdf:li>Heron</rdf:li></rdf:Bag></dc:subject>"
        f"</rdf:Description></rdf:RDF></x:xmpmeta>"
    )
    path = str(path)

    write_rating(path, 5)

    root = ET.parse(path).getroot()
    descriptions = list(root.iter(f"{{{NS_RDF}}}Description"))
    assert all(
        d.get(f"{{{NS_RDF}}}about") == "uuid:photo" for d in descriptions
    ), [d.attrib for d in descriptions]
    assert read_sync_preview_metadata(path)["rating"] == "5"


@pytest.mark.skipif(shutil.which("exiftool") is None, reason="exiftool not installed")
def test_exiftool_reads_what_vireo_wrote_in_both_layouts(layout_xmp):
    """ExifTool must see Vireo's values, not a stale copy it wrote itself."""
    editor = SidecarEditor(layout_xmp)
    editor.set_rating(5)
    editor.set_pick_flag("rejected")
    editor.set_gps_location(-33.5, -70.25)
    editor.add_keywords({"Egret"}, {"Birds|Egret"})
    editor.commit()

    out = subprocess.run(
        ["exiftool", "-j", "-n", "-XMP:all", layout_xmp],
        check=True, capture_output=True, text=True,
    )
    tags = json.loads(out.stdout)[0]
    assert tags["Rating"] == 5
    assert tags["Pick"] == -1
    assert tags["GPSLatitude"] == pytest.approx(-33.5)
    assert tags["GPSLongitude"] == pytest.approx(-70.25)
    assert sorted(tags["Subject"]) == ["Egret", "Heron"]
    assert sorted(tags["HierarchicalSubject"]) == ["Birds|Egret", "Birds|Heron"]
