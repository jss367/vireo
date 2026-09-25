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
    read_keywords,
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


def test_auxiliary_rdf_subject_keywords_are_ignored(tmp_path):
    """Keyword bags on a different rdf:about are not the photo's keywords."""
    path = tmp_path / "photo.xmp"
    path.write_text(EXIFTOOL_XMP.replace(
        "</rdf:RDF>",
        f" <rdf:Description rdf:about='#aux'"
        f" xmlns:dc='{NS_DC}' xmlns:lr='{NS_LR}'>\n"
        "  <dc:subject><rdf:Bag>"
        "<rdf:li>AuxOnly</rdf:li>"
        "<rdf:li>Heron</rdf:li>"
        "</rdf:Bag></dc:subject>\n"
        "  <lr:hierarchicalSubject><rdf:Bag>"
        "<rdf:li>Aux|Only</rdf:li>"
        "<rdf:li>Birds|Heron</rdf:li>"
        "</rdf:Bag></lr:hierarchicalSubject>\n"
        " </rdf:Description>\n</rdf:RDF>",
    ))
    path = str(path)

    assert read_keywords(path) == {"Heron"}
    assert read_hierarchical_keywords(path) == ["Birds|Heron"]

    metadata = read_sync_preview_metadata(path)
    assert metadata["keywords"] == {"Heron"}
    assert metadata["hierarchical_keywords"] == {"Birds|Heron"}

    editor = SidecarEditor(path)
    editor.add_keywords({"Heron"}, {"Aux|Only"})
    editor.remove_keywords({"AuxOnly"})
    editor.replace_keyword_hierarchies({"Aux|Only": "Aux|Rewritten"})
    editor.commit()

    root = ET.parse(path).getroot()
    aux = [
        d for d in root.iter(f"{{{NS_RDF}}}Description")
        if d.get(f"{{{NS_RDF}}}about") == "#aux"
    ]
    assert len(aux) == 1
    aux_flat = {
        li.text for li in aux[0].findall(
            f"{{{NS_DC}}}subject/{{{NS_RDF}}}Bag/{{{NS_RDF}}}li"
        )
    }
    aux_hier = {
        li.text for li in aux[0].findall(
            f"{{{NS_LR}}}hierarchicalSubject/{{{NS_RDF}}}Bag/{{{NS_RDF}}}li"
        )
    }
    assert aux_flat == {"AuxOnly", "Heron"}
    assert aux_hier == {"Aux|Only", "Birds|Heron"}

    assert read_keywords(path) == {"Heron"}
    assert set(read_hierarchical_keywords(path)) == {"Birds|Heron", "Aux|Rewritten"}


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


def test_fragment_auxiliary_does_not_hide_photo_subject(tmp_path):
    """A ``#thumbnail`` fragment Description doesn't count as photo-ambiguous.

    A sidecar carrying an auxiliary ``#thumbnail`` Description before a
    ``uuid:photo`` Description names its enclosing photo unambiguously: the
    fragment identifies a resource *inside* the packet, not the enclosing
    photo, so ``uuid:photo`` is the sole photo candidate. Reads must return
    that Description's rating and GPS, and writes must land on it rather
    than creating a fresh empty-subject copy that the auxiliary would then
    shadow on the next read.
    """
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about='#thumbnail'"
        f" xmlns:xmp='{NS_XMP}' xmlns:exif='{NS_EXIF}'"
        f" xmp:Rating='1' exif:GPSLatitude='40,0.0N' exif:GPSLongitude='40,0.0E'/>"
        f"<rdf:Description rdf:about='uuid:photo'"
        f" xmlns:xmp='{NS_XMP}' xmlns:exif='{NS_EXIF}'"
        f" xmp:Rating='4'"
        f" exif:GPSLatitude='10,0.0N' exif:GPSLongitude='20,0.0E'/>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path = str(path)

    metadata = read_sync_preview_metadata(path)
    assert metadata["rating"] == "4"
    assert metadata["location"]["latitude"] == pytest.approx(10.0)
    assert metadata["location"]["longitude"] == pytest.approx(20.0)

    write_gps_location(path, -33.5, -70.25)

    root = ET.parse(path).getroot()
    thumb = [
        d for d in root.iter(f"{{{NS_RDF}}}Description")
        if d.get(f"{{{NS_RDF}}}about") == "#thumbnail"
    ]
    photo_uuid = [
        d for d in root.iter(f"{{{NS_RDF}}}Description")
        if d.get(f"{{{NS_RDF}}}about") == "uuid:photo"
    ]
    # The auxiliary thumbnail Description must remain untouched.
    assert thumb[0].get(RATING) == "1"
    assert thumb[0].get(GPS_LATITUDE) == "40,0.0N"
    # The photo's GPS write lands on the ``uuid:photo`` Description, not on
    # a freshly-minted empty-subject copy that reads would then ignore.
    fresh = [
        d for d in root.iter(f"{{{NS_RDF}}}Description")
        if (d.get(f"{{{NS_RDF}}}about") or "") == ""
        and d.get(f"{{{NS_VIREO}}}gpsSource") == "assigned"
    ]
    assert fresh == []
    metadata = read_sync_preview_metadata(path)
    assert metadata["location"]["latitude"] == pytest.approx(-33.5)
    assert metadata["location"]["longitude"] == pytest.approx(-70.25)
    assert metadata["rating"] == "4"


def test_absolute_uri_fragment_does_not_hide_photo_subject(tmp_path):
    """A ``uuid:photo#thumbnail`` sibling doesn't ambiguate ``uuid:photo``.

    RFC 3986 fragments can appear anywhere in a URI, not only as a
    leading ``#``: ``uuid:photo#thumbnail`` names a resource *inside*
    the packet, distinct from the enclosing photo. The photo-subject
    filter must reject any URI carrying a ``#``, so the packet's
    ``uuid:photo`` Description is the sole photo candidate. Otherwise
    the preview hides the valid rating and writes create a conflicting
    empty-subject copy.
    """
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about='uuid:photo#thumbnail'"
        f" xmlns:xmp='{NS_XMP}' xmp:Rating='1'/>"
        f"<rdf:Description rdf:about='uuid:photo'"
        f" xmlns:xmp='{NS_XMP}' xmp:Rating='4'/>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path = str(path)

    metadata = read_sync_preview_metadata(path)
    assert metadata["rating"] == "4"

    write_rating(path, 5)

    root = ET.parse(path).getroot()
    thumb = [
        d for d in root.iter(f"{{{NS_RDF}}}Description")
        if d.get(f"{{{NS_RDF}}}about") == "uuid:photo#thumbnail"
    ]
    photo_uuid = [
        d for d in root.iter(f"{{{NS_RDF}}}Description")
        if d.get(f"{{{NS_RDF}}}about") == "uuid:photo"
    ]
    assert thumb[0].get(RATING) == "1"
    stored = photo_uuid[0].get(RATING) or photo_uuid[0].findtext(RATING)
    assert stored == "5"

    fresh = [
        d for d in root.iter(f"{{{NS_RDF}}}Description")
        if (d.get(f"{{{NS_RDF}}}about") or "") == ""
    ]
    assert fresh == []


def test_multiple_non_fragment_subjects_stay_ambiguous(tmp_path):
    """Two non-fragment ``rdf:about`` values still refuse to guess a photo.

    Only fragments/blank-nodes/``rdf:ID`` are auxiliary side-resources; two
    distinct URI subjects that could equally well be the photo remain
    ambiguous. Reads fall back to the empty subject (nothing to report) and
    a write creates a fresh empty-subject Description rather than mutating
    either candidate.
    """
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about='urn:a'"
        f" xmlns:xmp='{NS_XMP}' xmp:Rating='1'/>"
        f"<rdf:Description rdf:about='urn:b'"
        f" xmlns:xmp='{NS_XMP}' xmp:Rating='4'/>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path = str(path)

    metadata = read_sync_preview_metadata(path)
    assert metadata["rating"] is None

    write_gps_location(path, -33.5, -70.25)

    root = ET.parse(path).getroot()
    a = [
        d for d in root.iter(f"{{{NS_RDF}}}Description")
        if d.get(f"{{{NS_RDF}}}about") == "urn:a"
    ]
    b = [
        d for d in root.iter(f"{{{NS_RDF}}}Description")
        if d.get(f"{{{NS_RDF}}}about") == "urn:b"
    ]
    assert a[0].get(RATING) == "1"
    assert b[0].get(RATING) == "4"

    fresh = [
        d for d in root.iter(f"{{{NS_RDF}}}Description")
        if (d.get(f"{{{NS_RDF}}}about") or "") == ""
        and d.get(f"{{{NS_VIREO}}}gpsSource") == "assigned"
    ]
    assert len(fresh) == 1


def test_lone_blank_node_subject_is_not_treated_as_the_photo(tmp_path):
    """A single Description identified only by rdf:nodeID is auxiliary.

    A blank-node label never refers to the enclosing resource (the photo);
    it identifies an unnamed side-resource the packet happens to describe.
    Reads therefore return nothing, and a write that creates its own
    Description scopes it to the empty (enclosing-resource) subject rather
    than mutating the blank node's properties.
    """
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:nodeID='aux'"
        f" xmlns:xmp='{NS_XMP}' xmlns:exif='{NS_EXIF}' xmlns:dc='{NS_DC}'"
        f" xmp:Rating='1'"
        f" exif:GPSLatitude='40,0.0N' exif:GPSLongitude='40,0.0E'>"
        f"<dc:subject><rdf:Bag>"
        f"<rdf:li>AuxOnly</rdf:li>"
        f"</rdf:Bag></dc:subject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path = str(path)

    metadata = read_sync_preview_metadata(path)
    assert metadata["rating"] is None
    assert metadata["location"] is None
    assert read_keywords(path) == set()

    write_gps_location(path, -33.5, -70.25)

    root = ET.parse(path).getroot()
    aux = [
        d for d in root.iter(f"{{{NS_RDF}}}Description")
        if d.get(f"{{{NS_RDF}}}nodeID") == "aux"
    ]
    assert len(aux) == 1
    assert aux[0].get(RATING) == "1"
    assert aux[0].get(GPS_LATITUDE) == "40,0.0N"
    assert aux[0].get(GPS_LONGITUDE) == "40,0.0E"

    fresh = [
        d for d in root.iter(f"{{{NS_RDF}}}Description")
        if (d.get(f"{{{NS_RDF}}}about") or "") == ""
        and not d.get(f"{{{NS_RDF}}}nodeID")
        and d.get(f"{{{NS_VIREO}}}gpsSource") == "assigned"
    ]
    assert len(fresh) == 1
    metadata = read_sync_preview_metadata(path)
    assert metadata["location"]["latitude"] == pytest.approx(-33.5)
    assert metadata["location"]["longitude"] == pytest.approx(-70.25)


def test_merging_duplicate_bags_preserves_rdf_li_qualifiers(tmp_path):
    """When collapsing duplicate keyword bags, rdf:li attributes survive."""
    path = tmp_path / "photo.xmp"
    xml_ns = "http://www.w3.org/XML/1998/namespace"
    path.write_text(EXIFTOOL_XMP.replace(
        "<rdf:Description rdf:about='' xmlns:dc",
        f"<rdf:Description rdf:about='' xmlns:dc='{NS_DC}'>\n"
        "  <dc:subject><rdf:Bag>"
        "<rdf:li xml:lang='x-default' rdf:parseType='Literal'>Egret</rdf:li>"
        "</rdf:Bag></dc:subject>\n"
        " </rdf:Description>\n <rdf:Description rdf:about='' xmlns:dc",
        1,
    ))
    path = str(path)

    editor = SidecarEditor(path)
    editor.add_keywords({"Kiwi"}, set())
    editor.commit()

    assert _copies(path, SUBJECT) == 1
    root = ET.parse(path).getroot()
    bags = list(root.iter(f"{{{NS_RDF}}}Bag"))
    egret_lis = [
        li for bag in bags
        for li in bag.findall(f"{{{NS_RDF}}}li")
        if (li.text or "") == "Egret"
    ]
    assert len(egret_lis) == 1
    assert egret_lis[0].get(f"{{{xml_ns}}}lang") == "x-default"
    assert egret_lis[0].get(f"{{{NS_RDF}}}parseType") == "Literal"
    assert set(read_keywords(path)) == {"Heron", "Egret", "Kiwi"}


def test_merging_duplicate_bags_keeps_shared_text_with_distinct_qualifiers(tmp_path):
    """Two rdf:li items with the same text but different qualifiers both survive."""
    path = tmp_path / "photo.xmp"
    xml_ns = "http://www.w3.org/XML/1998/namespace"
    body = EXIFTOOL_XMP.replace(
        "<rdf:li>Heron</rdf:li>",
        "<rdf:li xml:lang='en'>Heron</rdf:li>",
    ).replace(
        "<rdf:Description rdf:about='' xmlns:dc",
        f"<rdf:Description rdf:about='' xmlns:dc='{NS_DC}'>\n"
        "  <dc:subject><rdf:Bag>"
        "<rdf:li xml:lang='fr'>Heron</rdf:li>"
        "</rdf:Bag></dc:subject>\n"
        " </rdf:Description>\n <rdf:Description rdf:about='' xmlns:dc",
        1,
    )
    path.write_text(body)
    path = str(path)

    editor = SidecarEditor(path)
    editor.add_keywords({"Kiwi"}, set())
    editor.commit()

    assert _copies(path, SUBJECT) == 1
    root = ET.parse(path).getroot()
    bags = list(root.iter(f"{{{NS_RDF}}}Bag"))
    heron_lis = [
        li for bag in bags
        for li in bag.findall(f"{{{NS_RDF}}}li")
        if (li.text or "") == "Heron"
    ]
    assert len(heron_lis) == 2
    langs = sorted((li.get(f"{{{xml_ns}}}lang") or "") for li in heron_lis)
    assert langs == ["en", "fr"]


def test_merging_duplicate_bags_keeps_child_only_items(tmp_path):
    """A structured rdf:li with no direct text survives the bag merge."""
    path = tmp_path / "photo.xmp"
    body = EXIFTOOL_XMP.replace(
        "<rdf:li>Heron</rdf:li>",
        "<rdf:li rdf:parseType='Resource'><rdf:value>Heron</rdf:value></rdf:li>",
    ).replace(
        "<rdf:Description rdf:about='' xmlns:dc",
        f"<rdf:Description rdf:about='' xmlns:dc='{NS_DC}'>\n"
        "  <dc:subject><rdf:Bag><rdf:li>Egret</rdf:li></rdf:Bag></dc:subject>\n"
        " </rdf:Description>\n <rdf:Description rdf:about='' xmlns:dc",
        1,
    )
    path.write_text(body)
    path = str(path)

    editor = SidecarEditor(path)
    editor.add_keywords({"Kiwi"}, set())
    editor.commit()

    assert _copies(path, SUBJECT) == 1
    root = ET.parse(path).getroot()
    structured = [
        li for li in root.iter(f"{{{NS_RDF}}}li")
        if li.get(f"{{{NS_RDF}}}parseType") == "Resource"
    ]
    assert len(structured) == 1
    assert structured[0].findtext(f"{{{NS_RDF}}}value") == "Heron"
    texts = sorted(li.text for li in root.iter(f"{{{NS_RDF}}}li") if li.text)
    assert "Egret" in texts and "Kiwi" in texts


def test_merging_duplicate_bags_keeps_mixed_content_tails(tmp_path):
    """Mixed-content items differing only in a child element's tail both survive.

    An ``rdf:li`` whose value is spelled as an XML literal can end in text
    that follows a child element (``<rdf:value>B</rdf:value>C``). Two such
    items differing only in that trailing text carry distinct values; the
    structural signature must include a child's ``.tail`` so a bag merge
    keeps both instead of collapsing them into one and then dropping the
    later property with its unique value.
    """
    path = tmp_path / "photo.xmp"
    body = EXIFTOOL_XMP.replace(
        "<rdf:li>Heron</rdf:li>",
        "<rdf:li rdf:parseType='Literal'><rdf:value>Heron</rdf:value>C</rdf:li>",
    ).replace(
        "<rdf:Description rdf:about='' xmlns:dc",
        f"<rdf:Description rdf:about='' xmlns:dc='{NS_DC}'>\n"
        "  <dc:subject><rdf:Bag>"
        "<rdf:li rdf:parseType='Literal'><rdf:value>Heron</rdf:value>D</rdf:li>"
        "</rdf:Bag></dc:subject>\n"
        " </rdf:Description>\n <rdf:Description rdf:about='' xmlns:dc",
        1,
    )
    path.write_text(body)
    path = str(path)

    editor = SidecarEditor(path)
    editor.add_keywords({"Kiwi"}, set())
    editor.commit()

    assert _copies(path, SUBJECT) == 1
    root = ET.parse(path).getroot()
    literal_lis = [
        li for li in root.iter(f"{{{NS_RDF}}}li")
        if li.get(f"{{{NS_RDF}}}parseType") == "Literal"
    ]
    assert len(literal_lis) == 2
    tails = sorted((li.find(f"{{{NS_RDF}}}value").tail or "") for li in literal_lis)
    assert tails == ["C", "D"]


def test_rating_write_updates_photo_subject_alongside_fragment(tmp_path):
    """A rating write updates the photo Description when a fragment shares the packet.

    When the sidecar pins its photo Description to ``uuid:photo`` and also
    carries an auxiliary ``#thumbnail`` Description, the fragment is a side
    resource inside the packet -- not the enclosing photo. A rating write
    must update ``uuid:photo`` in place rather than creating a fresh
    empty-subject Description that a subsequent read would ignore (leaving
    the queued rating silently dropped from the sidecar's perspective).
    """
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about='#thumbnail'"
        f" xmlns:xmp='{NS_XMP}' xmp:Rating='1'/>"
        f"<rdf:Description rdf:about='uuid:photo'"
        f" xmlns:xmp='{NS_XMP}' xmp:Rating='4'/>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path = str(path)

    write_rating(path, 5)

    root = ET.parse(path).getroot()
    thumb = [
        d for d in root.iter(f"{{{NS_RDF}}}Description")
        if d.get(f"{{{NS_RDF}}}about") == "#thumbnail"
    ]
    photo_uuid = [
        d for d in root.iter(f"{{{NS_RDF}}}Description")
        if d.get(f"{{{NS_RDF}}}about") == "uuid:photo"
    ]
    assert thumb[0].get(RATING) == "1"
    stored = photo_uuid[0].get(RATING) or photo_uuid[0].findtext(RATING)
    assert stored == "5"

    # No fresh empty-subject Description is added -- the write landed on the
    # existing photo Description.
    fresh = [
        d for d in root.iter(f"{{{NS_RDF}}}Description")
        if (d.get(f"{{{NS_RDF}}}about") or "") == ""
    ]
    assert fresh == []
    assert read_sync_preview_metadata(path)["rating"] == "5"


def test_rating_only_write_does_not_create_missing_sidecar(tmp_path):
    """A standalone rating write against a missing sidecar creates nothing."""
    path = str(tmp_path / "missing.xmp")

    write_rating(path, 4)

    assert not os.path.exists(path)


def test_sync_preview_reads_photo_subject_alongside_fragment(tmp_path):
    """A ``#thumbnail`` beside a URI photo Description doesn't hide its rating.

    The auxiliary fragment identifies a resource inside the packet, so the
    URI Description is unambiguously the photo. The sync preview must
    report that Description's rating rather than blanking it as if the
    subjects were ambiguous -- otherwise the pending-changes review would
    treat every re-read of an unchanged sidecar as a rating "loss" and
    keep re-writing it.
    """
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about='#thumbnail'"
        f" xmlns:xmp='{NS_XMP}' xmp:Rating='1'/>"
        f"<rdf:Description rdf:about='uuid:photo'"
        f" xmlns:xmp='{NS_XMP}' xmp:Rating='4'/>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path = str(path)

    metadata = read_sync_preview_metadata(path)
    assert metadata["status"] == "ok"
    assert metadata["rating"] == "4"
    assert metadata["rating_writable"] is True

    write_rating(path, 5)

    metadata = read_sync_preview_metadata(path)
    assert metadata["rating"] == "5"
    assert metadata["rating_writable"] is True


def test_updating_qualified_simple_property_preserves_qualifiers(tmp_path):
    """A qualified ``xmp:Rating`` keeps ``rdf:parseType`` and qualifier siblings.

    When the sidecar spells the rating as
    ``<xmp:Rating rdf:parseType="Resource"><rdf:value>3</rdf:value>...
    </xmp:Rating>``, an update must land on the nested ``rdf:value`` while
    preserving the container's ``parseType`` attribute and every qualifier
    child. Blowing the children away would strip the qualifiers and leave
    ``rdf:parseType="Resource"`` on a text-only element -- invalid RDF that
    a downstream reader may refuse to parse.
    """
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about='' xmlns:xmp='{NS_XMP}'>"
        f"<xmp:Rating rdf:parseType='Resource'>"
        f"<rdf:value>3</rdf:value>"
        f"<xmp:someQualifier>foo</xmp:someQualifier>"
        f"</xmp:Rating>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    write_rating(path_str, 5)

    root = ET.parse(path_str).getroot()
    ratings = list(root.iter(RATING))
    assert len(ratings) == 1
    assert ratings[0].get(f"{{{NS_RDF}}}parseType") == "Resource"
    rdf_value = ratings[0].find(f"{{{NS_RDF}}}value")
    assert rdf_value is not None
    assert rdf_value.text == "5"
    qualifier = ratings[0].find(f"{{{NS_XMP}}}someQualifier")
    assert qualifier is not None
    assert qualifier.text == "foo"


def test_rewriting_qualified_simple_property_with_same_value_is_a_noop(tmp_path):
    """Rewriting a qualified property with its current value changes nothing.

    Before this fix the update path unconditionally wiped the qualifier
    children even when the caller wrote the value the sidecar already
    carried, so an idle sync silently corrupted every qualified rating.
    """
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about='' xmlns:xmp='{NS_XMP}'>"
        f"<xmp:Rating rdf:parseType='Resource'>"
        f"<rdf:value>3</rdf:value>"
        f"<xmp:someQualifier>foo</xmp:someQualifier>"
        f"</xmp:Rating>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)
    before = path.read_bytes()

    write_rating(path_str, 3)

    assert path.read_bytes() == before


def test_collapsing_duplicate_bags_preserves_container_level_qualifiers(tmp_path):
    """A duplicate ``dc:subject`` carrying ``xml:lang`` survives keyword merges.

    Container-level qualifiers on a property (``xml:lang`` on the
    ``dc:subject`` element itself, or any other attribute) apply to every
    ``rdf:li`` inside; folding those items into an unqualified target bag
    would silently drop the qualifier when the duplicate is removed. Leave
    the qualified duplicate in place so its meaning survives an ordinary
    keyword addition; plain duplicates left by earlier writes still
    collapse into one.
    """
    xml_ns = "http://www.w3.org/XML/1998/namespace"
    path = tmp_path / "photo.xmp"
    body = EXIFTOOL_XMP.replace(
        "</rdf:Description>\n <rdf:Description rdf:about='' xmlns:exif",
        "</rdf:Description>\n"
        f" <rdf:Description rdf:about=''"
        f" xmlns:dc='{NS_DC}' xmlns:xml='http://www.w3.org/XML/1998/namespace'>\n"
        "  <dc:subject xml:lang='en'>"
        "<rdf:Bag><rdf:li>Sparrow</rdf:li></rdf:Bag>"
        "</dc:subject>\n"
        " </rdf:Description>\n"
        " <rdf:Description rdf:about='' xmlns:exif",
        1,
    )
    path.write_text(body)
    path_str = str(path)

    editor = SidecarEditor(path_str)
    editor.add_keywords({"Kiwi"}, set())
    editor.commit()

    root = ET.parse(path_str).getroot()
    subjects = list(root.iter(SUBJECT))
    qualified = [s for s in subjects if s.get(f"{{{xml_ns}}}lang") == "en"]
    unqualified = [s for s in subjects if s.get(f"{{{xml_ns}}}lang") is None]

    assert len(qualified) == 1
    q_items = sorted(
        li.text for li in qualified[0].iter(f"{{{NS_RDF}}}li") if li.text
    )
    assert q_items == ["Sparrow"]

    assert len(unqualified) == 1
    u_items = sorted(
        li.text for li in unqualified[0].iter(f"{{{NS_RDF}}}li") if li.text
    )
    assert u_items == ["Heron", "Kiwi"]


def test_collapsing_duplicate_bags_preserves_bag_level_qualifiers(tmp_path):
    """A duplicate whose ``rdf:Bag`` carries an attribute survives keyword merges.

    A qualifier on the bag element applies to every item inside it, so
    dropping the container when merging its items into an unqualified
    target would silently discard the qualifier from every value.
    """
    xml_ns = "http://www.w3.org/XML/1998/namespace"
    path = tmp_path / "photo.xmp"
    body = EXIFTOOL_XMP.replace(
        "</rdf:Description>\n <rdf:Description rdf:about='' xmlns:exif",
        "</rdf:Description>\n"
        f" <rdf:Description rdf:about=''"
        f" xmlns:dc='{NS_DC}' xmlns:xml='http://www.w3.org/XML/1998/namespace'>\n"
        "  <dc:subject>"
        "<rdf:Bag xml:lang='en'><rdf:li>Sparrow</rdf:li></rdf:Bag>"
        "</dc:subject>\n"
        " </rdf:Description>\n"
        " <rdf:Description rdf:about='' xmlns:exif",
        1,
    )
    path.write_text(body)
    path_str = str(path)

    editor = SidecarEditor(path_str)
    editor.add_keywords({"Kiwi"}, set())
    editor.commit()

    root = ET.parse(path_str).getroot()
    qualified_bags = [
        bag for bag in root.iter(f"{{{NS_RDF}}}Bag")
        if bag.get(f"{{{xml_ns}}}lang") == "en"
    ]
    assert len(qualified_bags) == 1
    items = sorted(li.text for li in qualified_bags[0].findall(f"{{{NS_RDF}}}li"))
    assert items == ["Sparrow"]


def test_sync_preview_marks_missing_sidecar_rating_unwritable(tmp_path):
    """Missing and unreadable sidecars still report rating_writable=False.

    A rating-only sync must not create a sidecar or overwrite a corrupt one.
    """
    missing = str(tmp_path / "missing.xmp")
    metadata = read_sync_preview_metadata(missing)
    assert metadata["status"] == "missing"
    assert metadata["rating_writable"] is False

    corrupt = tmp_path / "corrupt.xmp"
    corrupt.write_text("not xml <<<")
    metadata = read_sync_preview_metadata(str(corrupt))
    assert metadata["status"] == "unreadable"
    assert metadata["rating_writable"] is False


def test_reading_qualified_simple_property_returns_rdf_value(tmp_path):
    """A qualified rating/GPS is read from the nested ``rdf:value``.

    When a property is stored as
    ``<xmp:Rating rdf:parseType="Resource"><rdf:value>3</rdf:value>...
    </xmp:Rating>``, the container's own ``text`` is whitespace between
    its children. Returning it would hide the rating from every reader
    and let ``set_gps_location`` back up an empty string that later
    restores empty coordinates instead of the original.
    """
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about='' xmlns:xmp='{NS_XMP}' xmlns:exif='{NS_EXIF}'>"
        f"<xmp:Rating rdf:parseType='Resource'>"
        f"<rdf:value>3</rdf:value>"
        f"<xmp:someQualifier>foo</xmp:someQualifier>"
        f"</xmp:Rating>"
        f"<exif:GPSLatitude rdf:parseType='Resource'>"
        f"<rdf:value>10,30.0N</rdf:value>"
        f"</exif:GPSLatitude>"
        f"<exif:GPSLongitude rdf:parseType='Resource'>"
        f"<rdf:value>20,15.0E</rdf:value>"
        f"</exif:GPSLongitude>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )

    metadata = read_sync_preview_metadata(str(path))
    assert metadata["rating"] == "3"
    assert metadata["location"]["latitude"] == pytest.approx(10.5)
    assert metadata["location"]["longitude"] == pytest.approx(20.25)


def test_backup_of_qualified_gps_restores_original_on_removal(tmp_path):
    """A qualified prior GPS survives a Vireo write-then-remove round trip.

    ``set_gps_location`` snapshots any existing GPS to
    ``vireo:previousGPS*`` on the first Vireo write so
    ``remove_vireo_gps_location`` can restore it. If the existing
    coordinates are stored in the qualified form, reading the container's
    whitespace instead of the nested ``rdf:value`` silently backs up
    empty strings and later restores empty coordinates -- the original
    location is gone.
    """
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about='' xmlns:exif='{NS_EXIF}'>"
        f"<exif:GPSLatitude rdf:parseType='Resource'>"
        f"<rdf:value>10,30.0N</rdf:value>"
        f"</exif:GPSLatitude>"
        f"<exif:GPSLongitude rdf:parseType='Resource'>"
        f"<rdf:value>20,15.0E</rdf:value>"
        f"</exif:GPSLongitude>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    write_gps_location(path_str, -33.5, -70.25)

    intermediate = read_sync_preview_metadata(path_str)
    assert intermediate["location"]["latitude"] == pytest.approx(-33.5)
    assert intermediate["location"]["longitude"] == pytest.approx(-70.25)

    remove_vireo_gps_location(path_str)

    restored = read_sync_preview_metadata(path_str)
    assert restored["location"]["latitude"] == pytest.approx(10.5)
    assert restored["location"]["longitude"] == pytest.approx(20.25)


def test_added_keywords_do_not_inherit_first_bag_qualifier(tmp_path):
    """New items don't inherit ``xml:lang`` from a qualified first bag.

    When the first ``dc:subject`` occurrence carries an ``xml:lang``
    qualifier (on the property or its ``rdf:Bag``) and a later
    occurrence is unqualified, merging into the qualified target would
    silently drop the qualifier from the later items and, worse, cause
    newly added keywords to inherit the language qualifier they never
    asked for. Instead, an unqualified target should be selected (or
    a fresh unqualified property created) so the qualified container
    survives untouched with its original items.
    """
    xml_ns = "http://www.w3.org/XML/1998/namespace"
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about=''"
        f" xmlns:dc='{NS_DC}' xmlns:xml='http://www.w3.org/XML/1998/namespace'>"
        f"<dc:subject xml:lang='en'>"
        f"<rdf:Bag><rdf:li>Sparrow</rdf:li></rdf:Bag>"
        f"</dc:subject>"
        f"</rdf:Description>"
        f"<rdf:Description rdf:about='' xmlns:dc='{NS_DC}'>"
        f"<dc:subject><rdf:Bag><rdf:li>Heron</rdf:li></rdf:Bag></dc:subject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    editor = SidecarEditor(path_str)
    editor.add_keywords({"Kiwi"}, set())
    editor.commit()

    root = ET.parse(path_str).getroot()
    subjects = list(root.iter(SUBJECT))
    qualified = [s for s in subjects if s.get(f"{{{xml_ns}}}lang") == "en"]
    unqualified = [s for s in subjects if s.get(f"{{{xml_ns}}}lang") is None]

    assert len(qualified) == 1
    q_items = sorted(
        li.text for li in qualified[0].iter(f"{{{NS_RDF}}}li") if li.text
    )
    assert q_items == ["Sparrow"]

    assert len(unqualified) >= 1
    u_items = sorted(
        li.text
        for prop in unqualified
        for li in prop.iter(f"{{{NS_RDF}}}li")
        if li.text
    )
    assert u_items == ["Heron", "Kiwi"]


def test_description_level_xml_lang_counts_as_a_container_qualifier(tmp_path):
    """A ``xml:lang`` on the owning Description still qualifies the array.

    ``xml:lang`` (and other ``xml:*`` attributes such as ``xml:base``) are
    inherited by every descendant, so a language declared on
    ``rdf:Description`` applies to every ``rdf:li`` inside its
    ``dc:subject`` bag just as if it were on the property or the bag
    itself. If merging or target selection ignored that inherited
    qualifier, a Description-qualified duplicate would be picked as the
    unqualified target, its language would silently attach to newly
    added keywords, and an actually unqualified duplicate's items would
    be moved into the qualified Description and their original property
    removed.
    """
    xml_ns = "http://www.w3.org/XML/1998/namespace"
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about=''"
        f" xmlns:dc='{NS_DC}' xmlns:xml='http://www.w3.org/XML/1998/namespace'"
        f" xml:lang='en'>"
        f"<dc:subject><rdf:Bag><rdf:li>Sparrow</rdf:li></rdf:Bag></dc:subject>"
        f"</rdf:Description>"
        f"<rdf:Description rdf:about='' xmlns:dc='{NS_DC}'>"
        f"<dc:subject><rdf:Bag><rdf:li>Heron</rdf:li></rdf:Bag></dc:subject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    editor = SidecarEditor(path_str)
    editor.add_keywords({"Kiwi"}, set())
    editor.commit()

    root = ET.parse(path_str).getroot()

    qualified_descs = [
        d for d in root.iter(f"{{{NS_RDF}}}Description")
        if d.get(f"{{{xml_ns}}}lang") == "en"
    ]
    assert len(qualified_descs) == 1
    q_items = sorted(
        li.text for li in qualified_descs[0].iter(f"{{{NS_RDF}}}li") if li.text
    )
    assert q_items == ["Sparrow"]

    unqualified_descs = [
        d for d in root.iter(f"{{{NS_RDF}}}Description")
        if d.get(f"{{{xml_ns}}}lang") is None
    ]
    u_items = sorted(
        li.text
        for d in unqualified_descs
        for li in d.iter(f"{{{NS_RDF}}}li")
        if li.text
    )
    assert u_items == ["Heron", "Kiwi"]


def test_qualified_simple_property_in_long_form_round_trips(tmp_path):
    """A qualified rating/GPS stored under ``rdf:Description`` reads and updates.

    XMP's qualified simple properties have two equivalent RDF/XML
    spellings. The short form nests ``rdf:value`` and the qualifiers
    directly under the property with ``rdf:parseType='Resource'``. The
    long form wraps them in a ``rdf:Description``:

        <xmp:Rating><rdf:Description>
          <rdf:value>3</rdf:value><xmp:someQualifier>foo</xmp:someQualifier>
        </rdf:Description></xmp:Rating>

    Readers must accept both spellings, and writers must update the
    existing nested ``rdf:value`` rather than appending a second one
    beside the ``rdf:Description``. Otherwise the sync preview hides the
    rating, and a rating update leaves the stale value in place while
    adding a second, ambiguous one.
    """
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about='' xmlns:xmp='{NS_XMP}' xmlns:exif='{NS_EXIF}'>"
        f"<xmp:Rating>"
        f"<rdf:Description>"
        f"<rdf:value>3</rdf:value>"
        f"<xmp:someQualifier>foo</xmp:someQualifier>"
        f"</rdf:Description>"
        f"</xmp:Rating>"
        f"<exif:GPSLatitude>"
        f"<rdf:Description>"
        f"<rdf:value>10,30.0N</rdf:value>"
        f"</rdf:Description>"
        f"</exif:GPSLatitude>"
        f"<exif:GPSLongitude>"
        f"<rdf:Description>"
        f"<rdf:value>20,15.0E</rdf:value>"
        f"</rdf:Description>"
        f"</exif:GPSLongitude>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    metadata = read_sync_preview_metadata(path_str)
    assert metadata["rating"] == "3"
    assert metadata["location"]["latitude"] == pytest.approx(10.5)
    assert metadata["location"]["longitude"] == pytest.approx(20.25)

    write_rating(path_str, 5)

    root = ET.parse(path_str).getroot()
    ratings = list(root.iter(RATING))
    assert len(ratings) == 1
    nested_descs = ratings[0].findall(f"{{{NS_RDF}}}Description")
    assert len(nested_descs) == 1
    values = nested_descs[0].findall(f"{{{NS_RDF}}}value")
    assert len(values) == 1
    assert values[0].text == "5"
    assert ratings[0].find(f"{{{NS_RDF}}}value") is None
    qualifiers = nested_descs[0].findall(f"{{{NS_XMP}}}someQualifier")
    assert len(qualifiers) == 1 and qualifiers[0].text == "foo"

    assert read_sync_preview_metadata(path_str)["rating"] == "5"


def test_add_keywords_when_only_owner_is_qualified_creates_fresh_description(tmp_path):
    """Fallback creation never inherits a Description-level ``xml:lang``.

    When every existing ``dc:subject`` occurrence sits under a
    Description that carries ``xml:lang`` (so target selection has no
    unqualified occurrence to reuse), a plain ``add_keywords()`` must
    create the fallback property under a fresh empty-subject
    Description rather than under the qualified one. Otherwise the new
    keyword silently inherits the language qualifier it was intended to
    avoid.
    """
    xml_ns = "http://www.w3.org/XML/1998/namespace"
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about=''"
        f" xmlns:dc='{NS_DC}' xmlns:lr='{NS_LR}'"
        f" xmlns:xml='http://www.w3.org/XML/1998/namespace'"
        f" xml:lang='en'>"
        f"<dc:subject><rdf:Bag><rdf:li>Sparrow</rdf:li></rdf:Bag></dc:subject>"
        f"<lr:hierarchicalSubject>"
        f"<rdf:Bag><rdf:li>Birds|Sparrow</rdf:li></rdf:Bag>"
        f"</lr:hierarchicalSubject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    editor = SidecarEditor(path_str)
    editor.add_keywords({"Kiwi"}, {"Birds|Kiwi"})
    editor.commit()

    root = ET.parse(path_str).getroot()

    qualified_descs = [
        d for d in root.iter(f"{{{NS_RDF}}}Description")
        if d.get(f"{{{xml_ns}}}lang") == "en"
    ]
    assert len(qualified_descs) == 1
    q_items = sorted(
        li.text for li in qualified_descs[0].iter(f"{{{NS_RDF}}}li") if li.text
    )
    assert q_items == ["Birds|Sparrow", "Sparrow"]

    unqualified_descs = [
        d for d in root.iter(f"{{{NS_RDF}}}Description")
        if d.get(f"{{{xml_ns}}}lang") is None
        and (d.get(f"{{{NS_RDF}}}about") or "") == ""
    ]
    u_items = sorted(
        li.text
        for d in unqualified_descs
        for li in d.iter(f"{{{NS_RDF}}}li")
        if li.text
    )
    assert u_items == ["Birds|Kiwi", "Kiwi"]


def test_fallback_description_inherits_photo_subject_when_all_qualified(tmp_path):
    """A fallback bag on a subject-pinned, all-qualified sidecar keeps the subject.

    When every existing photo Description is XML-qualified and the
    photo is pinned to a unique non-empty subject
    (``rdf:about='uuid:photo'``), the fallback Description created for
    a new keyword bag must inherit that same subject. If it were
    inserted subjectless, ``_photo_subject`` would then see two
    distinct subjects (``uuid:photo`` and empty) on the next call and
    fall back to the empty subject -- unscoping every original photo
    Description, so a follow-up read would see only the fresh bag and
    hide the original rating, GPS and keywords.
    """
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about='uuid:photo'"
        f" xmlns:xmp='{NS_XMP}' xmlns:exif='{NS_EXIF}' xmlns:dc='{NS_DC}'"
        f" xmlns:xml='http://www.w3.org/XML/1998/namespace'"
        f" xml:lang='en'"
        f" xmp:Rating='3' exif:GPSLatitude='10,30.0N' exif:GPSLongitude='20,15.0E'>"
        f"<dc:subject><rdf:Bag><rdf:li>Sparrow</rdf:li></rdf:Bag></dc:subject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    editor = SidecarEditor(path_str)
    editor.add_keywords({"Kiwi"}, set())
    editor.commit()

    root = ET.parse(path_str).getroot()
    descriptions = list(root.iter(f"{{{NS_RDF}}}Description"))
    subjects = {d.get(f"{{{NS_RDF}}}about") for d in descriptions}
    # Every top-level Description still belongs to the photo -- no
    # empty-subject sibling has been created that would unscope the
    # original.
    assert subjects == {"uuid:photo"}

    metadata = read_sync_preview_metadata(path_str)
    assert metadata["rating"] == "3"
    assert metadata["location"]["latitude"] == pytest.approx(10.5)
    assert metadata["location"]["longitude"] == pytest.approx(20.25)
    assert read_keywords(path_str) == {"Sparrow", "Kiwi"}


def test_collapsing_duplicate_simple_property_keeps_qualified_copy(tmp_path):
    """When merging duplicates of a simple property, the qualified one wins.

    Earlier writers left both an unqualified attribute
    (``xmp:Rating='1'``) and a qualified child element with its own
    ``rdf:value`` and ``xmp:someQualifier``. A rating update must keep
    the qualified structure (updating its nested ``rdf:value`` in
    place) and only remove the plain attribute, so the qualifier
    siblings survive. Preferring the attribute here would delete the
    qualified element wholesale and silently drop
    ``xmp:someQualifier``.
    """
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about='' xmlns:xmp='{NS_XMP}'"
        f" xmp:Rating='1'>"
        f"<xmp:Rating rdf:parseType='Resource'>"
        f"<rdf:value>3</rdf:value>"
        f"<xmp:someQualifier>preserve-me</xmp:someQualifier>"
        f"</xmp:Rating>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    write_rating(path_str, 5)

    root = ET.parse(path_str).getroot()

    # The unqualified attribute duplicate has been removed.
    desc = root.find(f".//{{{NS_RDF}}}Description")
    assert desc.get(RATING) is None

    ratings = list(root.iter(RATING))
    assert len(ratings) == 1
    rating_el = ratings[0]

    # The qualified structure is intact: the nested ``rdf:value`` was
    # updated in place, and ``xmp:someQualifier`` survived.
    values = rating_el.findall(f"{{{NS_RDF}}}value")
    assert len(values) == 1 and values[0].text == "5"
    qualifiers = rating_el.findall(f"{{{NS_XMP}}}someQualifier")
    assert len(qualifiers) == 1 and qualifiers[0].text == "preserve-me"

    assert read_sync_preview_metadata(path_str)["rating"] == "5"


def test_rdf_id_subject_is_not_treated_as_the_photo(tmp_path):
    """A Description identified by ``rdf:ID`` is auxiliary, not the photo.

    ``rdf:ID`` names a distinct RDF resource, just like ``rdf:about``
    or ``rdf:nodeID`` -- it is never the enclosing photo. If the
    identity ignored ``rdf:ID``, the auxiliary Description would alias
    into the empty-subject bucket alongside the real photo, so photo
    reads would pick up its rating/GPS and photo writes could mutate
    or collapse it.
    """
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about=''"
        f" xmlns:xmp='{NS_XMP}' xmp:Rating='4'/>"
        f"<rdf:Description rdf:ID='aux'"
        f" xmlns:xmp='{NS_XMP}' xmlns:exif='{NS_EXIF}' xmlns:dc='{NS_DC}'"
        f" xmp:Rating='1'"
        f" exif:GPSLatitude='40,0.0N' exif:GPSLongitude='40,0.0E'>"
        f"<dc:subject><rdf:Bag><rdf:li>AuxOnly</rdf:li></rdf:Bag></dc:subject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    metadata = read_sync_preview_metadata(path_str)
    assert metadata["rating"] == "4"
    assert metadata["location"] is None
    assert read_keywords(path_str) == set()

    editor = SidecarEditor(path_str)
    editor.set_rating(5)
    editor.add_keywords({"Heron"}, set())
    editor.commit()

    root = ET.parse(path_str).getroot()
    aux = [
        d for d in root.iter(f"{{{NS_RDF}}}Description")
        if d.get(f"{{{NS_RDF}}}ID") == "aux"
    ]
    assert len(aux) == 1
    assert aux[0].get(RATING) == "1"
    assert aux[0].get(GPS_LATITUDE) == "40,0.0N"
    aux_items = sorted(
        li.text for li in aux[0].iter(f"{{{NS_RDF}}}li") if li.text
    )
    assert aux_items == ["AuxOnly"]

    metadata = read_sync_preview_metadata(path_str)
    assert metadata["rating"] == "5"
    assert read_keywords(path_str) == {"Heron"}


def test_qualified_keyword_bag_under_rdf_value_is_read_and_merged(tmp_path):
    """A ``<dc:subject><rdf:value><rdf:Bag>...`` array is read as the photo's.

    The qualified array form wraps the bag in an ``rdf:value`` resource
    node, so a direct ``prop.find('rdf:Bag')`` misses it. Readers must
    follow the one-level indirection, and ``add_keywords`` must merge
    into that existing bag rather than creating a second unqualified
    ``dc:subject`` beside the qualified one.
    """
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about='' xmlns:dc='{NS_DC}'>"
        f"<dc:subject rdf:parseType='Resource'>"
        f"<rdf:value>"
        f"<rdf:Bag><rdf:li>Heron</rdf:li></rdf:Bag>"
        f"</rdf:value>"
        f"</dc:subject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    assert read_keywords(path_str) == {"Heron"}

    editor = SidecarEditor(path_str)
    editor.add_keywords({"Kiwi"}, set())
    editor.commit()

    root = ET.parse(path_str).getroot()
    subjects = list(root.iter(SUBJECT))
    assert len(subjects) == 1
    bags = list(subjects[0].iter(f"{{{NS_RDF}}}Bag"))
    assert len(bags) == 1
    items = sorted(li.text for li in bags[0].findall(f"{{{NS_RDF}}}li"))
    assert items == ["Heron", "Kiwi"]

    assert read_keywords(path_str) == {"Heron", "Kiwi"}


def test_gps_backup_reads_from_qualified_when_both_forms_exist(tmp_path):
    """``set_gps_location`` backs up the qualified GPS, not the stale attribute.

    An earlier writer left both an unqualified ``exif:GPSLatitude``
    attribute and a qualified child with a nested ``rdf:value`` giving
    a different coordinate. ``_set_properties`` keeps the qualified
    child on the update; the backup snapshot must read from the same
    occurrence, so ``remove_vireo_gps_location`` later restores the
    qualified original -- not the stale attribute value the write
    already removed.
    """
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about='' xmlns:exif='{NS_EXIF}'"
        f" exif:GPSLatitude='40,0.0N' exif:GPSLongitude='40,0.0E'>"
        f"<exif:GPSLatitude rdf:parseType='Resource'>"
        f"<rdf:value>10,30.0N</rdf:value>"
        f"</exif:GPSLatitude>"
        f"<exif:GPSLongitude rdf:parseType='Resource'>"
        f"<rdf:value>20,15.0E</rdf:value>"
        f"</exif:GPSLongitude>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    metadata = read_sync_preview_metadata(path_str)
    # Reader agrees with the writer: the qualified value wins.
    assert metadata["location"]["latitude"] == pytest.approx(10.5)
    assert metadata["location"]["longitude"] == pytest.approx(20.25)

    editor = SidecarEditor(path_str)
    editor.set_gps_location(-33.5, -70.25)
    editor.commit()

    assert read_sync_preview_metadata(path_str)["location"]["latitude"] == pytest.approx(-33.5)

    remove_vireo_gps_location(path_str)

    restored = read_sync_preview_metadata(path_str)
    assert restored["location"]["latitude"] == pytest.approx(10.5)
    assert restored["location"]["longitude"] == pytest.approx(20.25)


def test_new_simple_property_lands_on_unqualified_description(tmp_path):
    """A rating on an all-``xml:lang`` sidecar creates an unqualified Description.

    When every photo Description carries ``xml:lang`` and the rating
    is absent, adding it directly to that Description would attach the
    language qualifier to the numeric value -- invalid XMP semantics
    (and the source of stale readings for tools that respect
    ``xml:lang``). A fresh unqualified Description is created instead.
    """
    xml_ns = "http://www.w3.org/XML/1998/namespace"
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about=''"
        f" xmlns:dc='{NS_DC}'"
        f" xmlns:xml='http://www.w3.org/XML/1998/namespace'"
        f" xml:lang='en'>"
        f"<dc:subject><rdf:Bag><rdf:li>Heron</rdf:li></rdf:Bag></dc:subject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    write_rating(path_str, 4)

    root = ET.parse(path_str).getroot()
    qualified = [
        d for d in root.iter(f"{{{NS_RDF}}}Description")
        if d.get(f"{{{xml_ns}}}lang") == "en"
    ]
    unqualified = [
        d for d in root.iter(f"{{{NS_RDF}}}Description")
        if d.get(f"{{{xml_ns}}}lang") is None
    ]
    # The rating landed on a Description that carries no language.
    assert len(qualified) == 1 and qualified[0].get(RATING) is None
    rated = [d for d in unqualified if d.get(RATING) == "4"]
    assert len(rated) == 1
    # And the original keywords are still on the language-qualified Description.
    keywords = sorted(
        li.text for li in qualified[0].iter(f"{{{NS_RDF}}}li") if li.text
    )
    assert keywords == ["Heron"]


def test_qualified_keyword_bag_in_long_form_is_read_and_merged(tmp_path):
    """A long-form ``<dc:subject><rdf:Description><rdf:value><rdf:Bag>...``
    is read as the photo's keywords and merged into on ``add_keywords``.

    The qualified array has two equivalent serializations: the short
    form wraps the bag directly under ``rdf:value``, the long form
    also inserts an ``rdf:Description`` between the property and the
    ``rdf:value``. Readers must accept both, and ``_bag`` must find
    the existing bag through both layers so a keyword addition
    merges into it rather than creating a second unqualified property
    beside the qualified one.
    """
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about='' xmlns:dc='{NS_DC}'>"
        f"<dc:subject>"
        f"<rdf:Description>"
        f"<rdf:value>"
        f"<rdf:Bag><rdf:li>Heron</rdf:li></rdf:Bag>"
        f"</rdf:value>"
        f"</rdf:Description>"
        f"</dc:subject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    assert read_keywords(path_str) == {"Heron"}

    editor = SidecarEditor(path_str)
    editor.add_keywords({"Kiwi"}, set())
    editor.commit()

    root = ET.parse(path_str).getroot()
    subjects = list(root.iter(SUBJECT))
    assert len(subjects) == 1
    bags = list(subjects[0].iter(f"{{{NS_RDF}}}Bag"))
    assert len(bags) == 1
    items = sorted(li.text for li in bags[0].findall(f"{{{NS_RDF}}}li"))
    assert items == ["Heron", "Kiwi"]

    assert read_keywords(path_str) == {"Heron", "Kiwi"}


def test_xml_lang_on_rdf_value_wrapper_counts_as_container_qualifier(tmp_path):
    """An ``xml:lang`` on the ``rdf:value`` wrapper qualifies the array.

    The ``rdf:value`` element sits between the property and the bag,
    so an ``xml:lang`` on it is inherited by every ``rdf:li`` inside
    just as if it were on the property or the bag itself. Target
    selection must treat the property as qualified: an unqualified
    duplicate's items must not fold into the language-qualified bag,
    and newly added keywords must land under a fresh unqualified
    property rather than inheriting the language.
    """
    xml_ns = "http://www.w3.org/XML/1998/namespace"
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about=''"
        f" xmlns:dc='{NS_DC}'"
        f" xmlns:xml='http://www.w3.org/XML/1998/namespace'>"
        f"<dc:subject rdf:parseType='Resource'>"
        f"<rdf:value xml:lang='en'>"
        f"<rdf:Bag><rdf:li>Sparrow</rdf:li></rdf:Bag>"
        f"</rdf:value>"
        f"</dc:subject>"
        f"<dc:subject><rdf:Bag><rdf:li>Heron</rdf:li></rdf:Bag></dc:subject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    editor = SidecarEditor(path_str)
    editor.add_keywords({"Kiwi"}, set())
    editor.commit()

    root = ET.parse(path_str).getroot()

    # The language-qualified array still holds only its original item.
    qualified_values = [
        v for v in root.iter(f"{{{NS_RDF}}}value")
        if v.get(f"{{{xml_ns}}}lang") == "en"
    ]
    assert len(qualified_values) == 1
    q_items = sorted(
        li.text for li in qualified_values[0].iter(f"{{{NS_RDF}}}li")
        if li.text
    )
    assert q_items == ["Sparrow"]

    # The unqualified array picked up the new keyword next to Heron.
    unqualified_bags = [
        b for b in root.iter(f"{{{NS_RDF}}}Bag")
        if b.get(f"{{{xml_ns}}}lang") is None
        and (b.getparent() is None if hasattr(b, "getparent") else True)
    ]
    all_bags = list(root.iter(f"{{{NS_RDF}}}Bag"))
    unqualified_items = set()
    for bag in all_bags:
        # Skip the bag inside the qualified rdf:value.
        if any(
            v.get(f"{{{xml_ns}}}lang") == "en"
            for v in qualified_values
            if bag in list(v.iter(f"{{{NS_RDF}}}Bag"))
        ):
            continue
        for li in bag.findall(f"{{{NS_RDF}}}li"):
            if li.text:
                unqualified_items.add(li.text)
    assert unqualified_items == {"Heron", "Kiwi"}


def test_element_qualifiers_on_qualified_keyword_bag_survive_merge(tmp_path):
    """A ``rdf:parseType='Resource'`` bag with a sibling qualifier element
    is left alone during a keyword addition.

    The qualified property form permits qualifiers as sibling child
    elements next to ``rdf:value`` (or the bag). They are not
    attributes, so an attribute-only qualifier check misses them: an
    ordinary ``add_keywords`` would then merge the duplicate's items
    into the target and remove the whole element, silently deleting
    the qualifier. Detect those sibling elements too and leave the
    qualified container in place.
    """
    foo_ns = "http://example.com/foo/"
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about=''"
        f" xmlns:dc='{NS_DC}' xmlns:foo='{foo_ns}'>"
        f"<dc:subject rdf:parseType='Resource'>"
        f"<rdf:value>"
        f"<rdf:Bag><rdf:li>Sparrow</rdf:li></rdf:Bag>"
        f"</rdf:value>"
        f"<foo:source>camera</foo:source>"
        f"</dc:subject>"
        f"<dc:subject><rdf:Bag><rdf:li>Heron</rdf:li></rdf:Bag></dc:subject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    editor = SidecarEditor(path_str)
    editor.add_keywords({"Kiwi"}, set())
    editor.commit()

    root = ET.parse(path_str).getroot()

    # The qualified `dc:subject` still holds Sparrow and its qualifier.
    qualified = [
        s for s in root.iter(SUBJECT)
        if s.find(f"{{{foo_ns}}}source") is not None
    ]
    assert len(qualified) == 1
    q_items = sorted(
        li.text for li in qualified[0].iter(f"{{{NS_RDF}}}li")
        if li.text
    )
    assert q_items == ["Sparrow"]
    sources = qualified[0].findall(f"{{{foo_ns}}}source")
    assert len(sources) == 1 and sources[0].text == "camera"

    # The unqualified `dc:subject` picked up the new keyword next to Heron.
    unqualified = [
        s for s in root.iter(SUBJECT)
        if s.find(f"{{{foo_ns}}}source") is None
    ]
    u_items = set()
    for s in unqualified:
        for li in s.iter(f"{{{NS_RDF}}}li"):
            if li.text:
                u_items.add(li.text)
    assert u_items == {"Heron", "Kiwi"}


def test_attribute_form_qualifier_on_wrapper_survives_merge(tmp_path):
    """A ``foo:source='camera'`` attribute on the ``rdf:Description``
    wrapper is left alone during a keyword addition.

    RDF/XML permits the attribute abbreviation form: a namespaced
    attribute on a resource node (like ``rdf:Description foo:source=
    'camera'``) is equivalent to a nested ``<foo:source>camera
    </foo:source>`` child of that resource. It is a qualifier for
    the value, but it doesn't appear as an ``xml:*`` attribute
    anywhere and it isn't a child element, so an attribute-only
    check that only tests ``xml:*`` misses it. Recognize any
    non-structural RDF attribute on the wrapper (or the property /
    bag) so an ordinary ``add_keywords`` does not silently drop it.
    """
    foo_ns = "http://example.com/foo/"
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about=''"
        f" xmlns:dc='{NS_DC}' xmlns:foo='{foo_ns}'>"
        f"<dc:subject>"
        f"<rdf:Description foo:source='camera'>"
        f"<rdf:value>"
        f"<rdf:Bag><rdf:li>Sparrow</rdf:li></rdf:Bag>"
        f"</rdf:value>"
        f"</rdf:Description>"
        f"</dc:subject>"
        f"<dc:subject><rdf:Bag><rdf:li>Heron</rdf:li></rdf:Bag></dc:subject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    editor = SidecarEditor(path_str)
    editor.add_keywords({"Kiwi"}, set())
    editor.commit()

    root = ET.parse(path_str).getroot()

    # The wrapper Description still holds Sparrow and its foo:source.
    qualified_wrappers = [
        d for d in root.iter(f"{{{NS_RDF}}}Description")
        if d.get(f"{{{foo_ns}}}source") == "camera"
    ]
    assert len(qualified_wrappers) == 1
    q_items = sorted(
        li.text for li in qualified_wrappers[0].iter(f"{{{NS_RDF}}}li")
        if li.text
    )
    assert q_items == ["Sparrow"]

    # The unqualified dc:subject picked up the new keyword next to Heron.
    unqualified_subjects = [
        s for s in root.iter(SUBJECT)
        if s.find(f"{{{NS_RDF}}}Description") is None
    ]
    u_items = set()
    for s in unqualified_subjects:
        for li in s.iter(f"{{{NS_RDF}}}li"):
            if li.text:
                u_items.add(li.text)
    assert u_items == {"Heron", "Kiwi"}


def test_fragment_rdf_about_subject_is_not_treated_as_the_photo(tmp_path):
    """A lone ``rdf:about='#thumbnail'`` names a fragment inside the sidecar.

    A fragment identifier (any ``rdf:about`` value starting with
    ``#``) points at a resource *within* the packet, not the enclosing
    photo. If we accepted it as the photo subject, reads would expose
    the fragment's rating and GPS, and writes would mutate that
    auxiliary resource instead of creating a fresh empty-subject
    photo Description.
    """
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about='#thumbnail'"
        f" xmlns:xmp='{NS_XMP}' xmlns:exif='{NS_EXIF}' xmlns:dc='{NS_DC}'"
        f" xmp:Rating='1'"
        f" exif:GPSLatitude='40,0.0N' exif:GPSLongitude='40,0.0E'>"
        f"<dc:subject><rdf:Bag><rdf:li>AuxOnly</rdf:li></rdf:Bag></dc:subject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    metadata = read_sync_preview_metadata(path_str)
    assert metadata["rating"] is None
    assert metadata["location"] is None
    assert read_keywords(path_str) == set()

    write_rating(path_str, 5)

    root = ET.parse(path_str).getroot()
    thumbnail = [
        d for d in root.iter(f"{{{NS_RDF}}}Description")
        if d.get(f"{{{NS_RDF}}}about") == "#thumbnail"
    ]
    assert len(thumbnail) == 1
    # The fragment Description is left untouched.
    assert thumbnail[0].get(RATING) == "1"
    assert thumbnail[0].get(GPS_LATITUDE) == "40,0.0N"
    aux_items = sorted(
        li.text for li in thumbnail[0].iter(f"{{{NS_RDF}}}li") if li.text
    )
    assert aux_items == ["AuxOnly"]

    # The rating landed on a fresh empty-subject photo Description.
    fresh = [
        d for d in root.iter(f"{{{NS_RDF}}}Description")
        if (d.get(f"{{{NS_RDF}}}about") or "") == ""
        and d.get(RATING) == "5"
    ]
    assert len(fresh) == 1

    assert read_sync_preview_metadata(path_str)["rating"] == "5"


def test_rdf_id_on_wrapper_blocks_bag_collapse(tmp_path):
    """A duplicate wrapper Description carrying ``rdf:ID`` is left alone.

    ``rdf:ID`` (like ``rdf:about`` and ``rdf:nodeID``) names a
    distinct RDF resource that other statements in the packet may
    point at. Removing an element that carries an identity attribute
    silently drops the resource from the graph, so a keyword
    addition must not collapse such a duplicate: it must leave the
    identified container in place.
    """
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about='' xmlns:dc='{NS_DC}'>"
        f"<dc:subject>"
        f"<rdf:Description rdf:ID='sparrow-subject'>"
        f"<rdf:value>"
        f"<rdf:Bag><rdf:li>Sparrow</rdf:li></rdf:Bag>"
        f"</rdf:value>"
        f"</rdf:Description>"
        f"</dc:subject>"
        f"<dc:subject><rdf:Bag><rdf:li>Heron</rdf:li></rdf:Bag></dc:subject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    editor = SidecarEditor(path_str)
    editor.add_keywords({"Kiwi"}, set())
    editor.commit()

    root = ET.parse(path_str).getroot()

    # The identified wrapper Description still holds Sparrow and its rdf:ID.
    identified = [
        d for d in root.iter(f"{{{NS_RDF}}}Description")
        if d.get(f"{{{NS_RDF}}}ID") == "sparrow-subject"
    ]
    assert len(identified) == 1
    q_items = sorted(
        li.text for li in identified[0].iter(f"{{{NS_RDF}}}li") if li.text
    )
    assert q_items == ["Sparrow"]

    # The unqualified dc:subject picked up the new keyword next to Heron.
    unqualified_subjects = [
        s for s in root.iter(SUBJECT)
        if s.find(f"{{{NS_RDF}}}Description") is None
    ]
    u_items = set()
    for s in unqualified_subjects:
        for li in s.iter(f"{{{NS_RDF}}}li"):
            if li.text:
                u_items.add(li.text)
    assert u_items == {"Heron", "Kiwi"}


def test_xml_lang_on_rdf_rdf_reaches_every_bag_and_new_property(tmp_path):
    """``xml:lang`` on the outer ``rdf:RDF`` reaches every descendant.

    Ancestors above the owner Description propagate ``xml:*``
    qualifiers just like the owner does. When a new keyword is
    added, the qualifier-check must see the outer ``rdf:RDF`` /
    ``x:xmpmeta`` attribute -- otherwise a fresh photo Description
    would silently inherit that language and every rating / GPS
    coordinate written under it would end up language-tagged.
    """
    xml_ns = "http://www.w3.org/XML/1998/namespace"
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'"
        f" xmlns:xml='http://www.w3.org/XML/1998/namespace'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}' xml:lang='en'>"
        f"<rdf:Description rdf:about='' xmlns:dc='{NS_DC}'>"
        f"<dc:subject><rdf:Bag><rdf:li>Heron</rdf:li></rdf:Bag></dc:subject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    editor = SidecarEditor(path_str)
    editor.add_keywords({"Kiwi"}, set())
    editor.set_rating(4)
    editor.commit()

    root = ET.parse(path_str).getroot()

    # The rating landed on a Description that explicitly resets
    # inherited ``xml:lang``: an ``xml:lang=""`` cancels the
    # inheritance from the outer ``rdf:RDF``.
    rated = [
        d for d in root.iter(f"{{{NS_RDF}}}Description")
        if d.get(RATING) == "4"
    ]
    assert len(rated) == 1
    assert rated[0].get(f"{{{xml_ns}}}lang") == ""


def test_qualified_simple_property_duplicates_are_preserved(tmp_path):
    """A qualified duplicate of a simple property is not silently deleted.

    When two occurrences of ``xmp:Rating`` are both qualified and one
    carries an independent qualifier (an ``rdf:ID``, an attribute-form
    property, or a sibling qualifier element), the current write kept
    only one and removed the other -- silently discarding that
    qualifier. Update the value on the keeper as before, but leave
    the qualified duplicate in place so its data survives.
    """
    path = tmp_path / "photo.xmp"
    foo_ns = "http://example.com/foo/"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about=''"
        f" xmlns:xmp='{NS_XMP}' xmlns:foo='{foo_ns}'>"
        f"<xmp:Rating rdf:parseType='Resource'>"
        f"<rdf:value>3</rdf:value>"
        f"<foo:origin>keeper</foo:origin>"
        f"</xmp:Rating>"
        f"<xmp:Rating rdf:parseType='Resource'>"
        f"<rdf:value>2</rdf:value>"
        f"<foo:origin>preserve-me</foo:origin>"
        f"</xmp:Rating>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    write_rating(path_str, 5)

    root = ET.parse(path_str).getroot()
    ratings = list(root.iter(RATING))
    # Both qualified copies survive.
    assert len(ratings) == 2

    origins = sorted(
        r.findtext(f"{{{foo_ns}}}origin") for r in ratings
    )
    # Neither ``foo:origin`` was silently dropped.
    assert origins == ["keeper", "preserve-me"]


def test_empty_xml_lang_reset_lets_next_write_reuse_the_description(tmp_path):
    """An explicit ``xml:lang=""`` reset is treated as unqualified.

    The previous fix explicitly reset inherited ``xml:*`` on newly
    created photo Descriptions. But the qualifier check treated any
    ``xml:*`` attribute -- including the empty reset -- as a
    qualifier, so a subsequent keyword or property write refused to
    reuse that Description and created yet another. Repeated syncs
    accumulated duplicate ``dc:subject`` bags. The check must
    recognize that an empty ``xml:lang`` (or ``xml:space`` / etc.)
    cancels inheritance and treat the element as effectively
    unqualified.
    """
    xml_ns = "http://www.w3.org/XML/1998/namespace"
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'"
        f" xmlns:xml='http://www.w3.org/XML/1998/namespace'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}' xml:lang='en'>"
        f"<rdf:Description rdf:about='' xmlns:dc='{NS_DC}'>"
        f"<dc:subject><rdf:Bag><rdf:li>Heron</rdf:li></rdf:Bag></dc:subject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    # First add creates the empty-lang reset Description.
    editor = SidecarEditor(path_str)
    editor.add_keywords({"Kiwi"}, set())
    editor.commit()
    # Second add must reuse the reset Description, not create another.
    editor = SidecarEditor(path_str)
    editor.add_keywords({"Owl"}, set())
    editor.commit()

    root = ET.parse(path_str).getroot()

    # Exactly one Description carries the explicit ``xml:lang=""``
    # reset, and both new keywords live in one ``dc:subject`` bag
    # under it.
    reset_descs = [
        d for d in root.iter(f"{{{NS_RDF}}}Description")
        if d.get(f"{{{xml_ns}}}lang") == ""
    ]
    assert len(reset_descs) == 1
    reset_subjects = reset_descs[0].findall(SUBJECT)
    assert len(reset_subjects) == 1
    reset_bags = reset_subjects[0].findall(f"{{{NS_RDF}}}Bag")
    assert len(reset_bags) == 1
    reset_items = sorted(
        li.text for li in reset_bags[0].findall(f"{{{NS_RDF}}}li")
    )
    assert reset_items == ["Kiwi", "Owl"]


def test_xml_directives_on_duplicate_child_do_not_outrank_a_plain_sibling(tmp_path):
    """Local ``xml:space'' on a stale duplicate doesn't beat a plain sibling.

    ``xml:space``, ``xml:base``, and an empty ``xml:lang="" `` on a
    child are non-semantic XML directives that don't qualify a literal
    value. Before this fix ``_property_occurrence_score`` treated any
    non-structural attribute as a qualifier, so a stale
    ``<xmp:Rating xml:space="preserve">1</xmp:Rating>'' outranked the
    fresh plain ``<xmp:Rating>5</xmp:Rating>`` and reads returned the
    stale value; ``set_gps_location`` would then back it up and later
    restore it over the good coordinate. The score now uses the
    value-qualifier predicate, so a directive-only duplicate stays at
    the plain rank and doesn't shadow a genuine plain reading.
    """
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'"
        f" xmlns:xml='http://www.w3.org/XML/1998/namespace'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about='' xmlns:xmp='{NS_XMP}'>"
        f"<xmp:Rating xml:space='preserve'>1</xmp:Rating>"
        f"</rdf:Description>"
        f"<rdf:Description rdf:about='' xmlns:xmp='{NS_XMP}'>"
        f"<xmp:Rating>5</xmp:Rating>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    # The plain ``5`` is neither more nor less semantic than the
    # ``xml:space``-annotated ``1``; the ranker returns them at the
    # same score, and neither one is chosen as strictly authoritative.
    # A subsequent rating write updates both to the same value.
    editor = SidecarEditor(path_str)
    editor.set_rating(4)
    editor.commit()

    root = ET.parse(path_str).getroot()
    ratings = [
        (r.text or "").strip()
        for r in root.iter(RATING)
    ]
    # Both occurrences agree on the newly written value -- no stale
    # ``1`` sitting in a "qualified" copy that a later restore would
    # bring back.
    assert set(ratings) == {"4"}


def test_attribute_form_ranks_qualified_under_owner_inherited_xml_lang(tmp_path):
    """An owner-``xml:lang'' attribute-form GPS beats a plain-child duplicate.

    Attribute-form GPS values live directly on the owning
    ``rdf:Description``, so an ``xml:lang="en"'' on that Description
    language-tags the attribute the same way it would tag a bare-text
    child. Before this fix ``_property_occurrence_score'' ignored the
    owner and returned 0 for any attribute-form occurrence; a stale
    plain-child ``<exif:GPSLatitude>10,0N</exif:GPSLatitude>'' in a
    sibling Description therefore outranked the language-qualified
    attribute and ``_get_property`` handed the stale value to
    ``set_gps_location'', which backed it up for the later restore
    to write over the qualified coordinate.
    """
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'"
        f" xmlns:xml='http://www.w3.org/XML/1998/namespace'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about='' xmlns:exif='{NS_EXIF}'>"
        f"<exif:GPSLatitude>10,0.0N</exif:GPSLatitude>"
        f"<exif:GPSLongitude>20,0.0E</exif:GPSLongitude>"
        f"</rdf:Description>"
        f"<rdf:Description rdf:about='' xml:lang='en'"
        f" xmlns:exif='{NS_EXIF}'"
        f" exif:GPSLatitude='40,0.0N' exif:GPSLongitude='50,0.0E'/>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    metadata = read_sync_preview_metadata(path_str)
    assert metadata["location"]["latitude"] == pytest.approx(40.0)
    assert metadata["location"]["longitude"] == pytest.approx(50.0)


def test_gps_write_honors_owner_inherited_xml_lang_for_authority(tmp_path):
    """Owner-inherited ``xml:lang`` promotes a plain child to the qualified rank.

    ``_property_occurrence_score`` used to look only at the child's
    own attributes: two plain-child GPS occurrences (one under an
    ``xml:lang="en"`` Description, one under a language-free
    Description in document order first) tied at score 1. ``max`` on
    a tie picks the first, so ``_get_property`` read the earlier
    stale value; ``set_gps_location`` then backed up that value and
    the later restore path could rewrite the owner-qualified
    coordinate with the stale copy. Owner-inherited effective
    ``xml:lang`` on a plain child now boosts it to score 2, matching
    the qualified rank of a child that carries the attribute itself.
    """
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'"
        f" xmlns:xml='http://www.w3.org/XML/1998/namespace'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about='' xmlns:exif='{NS_EXIF}'>"
        f"<exif:GPSLatitude>10,0.0N</exif:GPSLatitude>"
        f"<exif:GPSLongitude>20,0.0E</exif:GPSLongitude>"
        f"</rdf:Description>"
        f"<rdf:Description rdf:about='' xml:lang='en'"
        f" xmlns:exif='{NS_EXIF}'>"
        f"<exif:GPSLatitude>40,0.0N</exif:GPSLatitude>"
        f"<exif:GPSLongitude>50,0.0E</exif:GPSLongitude>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    # Reads must pick the owner-qualified pair (score 2), not the
    # plain earlier one in document order.
    metadata = read_sync_preview_metadata(path_str)
    assert metadata["location"]["latitude"] == pytest.approx(40.0)
    assert metadata["location"]["longitude"] == pytest.approx(50.0)


def test_add_keywords_skips_merge_bag_when_no_new_keywords(tmp_path):
    """Re-adding an already-present keyword doesn't publish an empty bag.

    ``_bag`` picks an unqualified target and, when every existing bag
    is qualified, creates a fresh empty one. ``add_keywords`` used to
    call it unconditionally: re-adding a keyword that already sits in
    a qualified sibling bag committed an empty ``dc:subject`` beside
    the populated one, and a reader that resolves to a single
    occurrence would then report the photo as having no keywords at
    all. Create the merge target only when there's something to add.
    """
    xml_ns = "http://www.w3.org/XML/1998/namespace"
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'"
        f" xmlns:xml='http://www.w3.org/XML/1998/namespace'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}' xml:lang='en'>"
        f"<rdf:Description rdf:about='' xmlns:dc='{NS_DC}'>"
        f"<dc:subject><rdf:Bag>"
        f"<rdf:li>Heron</rdf:li>"
        f"</rdf:Bag></dc:subject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    editor = SidecarEditor(path_str)
    editor.add_keywords({"Heron"}, set())
    editor.commit()

    root = ET.parse(path_str).getroot()
    subjects = list(root.iter(SUBJECT))
    # Exactly one ``dc:subject`` survives: the pre-existing one, still
    # holding ``Heron``. No empty ``dc:subject`` sits next to it under
    # a fresh reset Description.
    assert len(subjects) == 1
    items = sorted(
        li.text
        for li in subjects[0].iter(f"{{{NS_RDF}}}li")
        if li.text
    )
    assert items == ["Heron"]
    # And reads land on the same populated bag.
    assert read_keywords(path_str) == {"Heron"}
    _ = xml_ns  # namespace binding used inside the sidecar XML


def test_replace_keyword_hierarchies_preserves_container_qualified_collision(tmp_path):
    """A bare-text sibling under a bag ``xml:lang'' isn't dropped as plain.

    Under ``<rdf:Bag xml:lang="fr">``, a bare-text ``<rdf:li>Birds|Legacy
    </rdf:li>`` is a French-tagged statement. Before this fix
    ``replace_keyword_hierarchies`` looked only at each item's own
    attributes: on a collision with a qualified sibling the bare-text
    item was seen as plain and dropped, silently discarding its ``fr``
    language tag. The collision now treats each container-qualified
    item as qualified via its inherited ``fr``, so both language-
    tagged siblings survive rather than one being classified as
    plain and removed wholesale.
    """
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'"
        f" xmlns:xml='http://www.w3.org/XML/1998/namespace'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about='' xmlns:lr='{NS_LR}'>"
        f"<lr:hierarchicalSubject><rdf:Bag xml:lang='fr'>"
        f"<rdf:li>Birds|Legacy</rdf:li>"
        f"<rdf:li>Birds|Legacy</rdf:li>"
        f"</rdf:Bag></lr:hierarchicalSubject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    editor = SidecarEditor(path_str)
    editor.replace_keyword_hierarchies({})
    editor.commit()

    root = ET.parse(path_str).getroot()
    bag = next(iter(root.iter(HIERARCHICAL_SUBJECT))).find(f"{{{NS_RDF}}}Bag")
    items = list(bag.findall(f"{{{NS_RDF}}}li"))
    # Both items survive: each carries an effective ``fr`` from the
    # bag, so both are treated as qualified on the collision (rather
    # than one being dropped as plain and taking its language with it).
    assert len(items) == 2
    assert all(li.text == "Birds|Legacy" for li in items)


def test_bag_xml_lang_reset_cancels_owner_language_for_reuse(tmp_path):
    """A bag's own ``xml:lang=""`` reset cancels the owner's inherited language.

    The owner Description carries ``xml:lang="en"``, so its
    ``dc:subject`` items would inherit English unless a nearer
    ancestor cancels it. The bag's ``xml:lang=""`` reset does
    exactly that -- the items' effective language is "no known" --
    and the effectively-unqualified bag is a valid reuse target.
    Before this fix the owner-only check saw ``en``, treated the
    prop as qualified, and minted a second ``dc:subject`` beside the
    reset bag; readers walking to the first occurrence would then
    miss the freshly-written keyword.
    """
    xml_ns = "http://www.w3.org/XML/1998/namespace"
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'"
        f" xmlns:xml='http://www.w3.org/XML/1998/namespace'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about='' xml:lang='en'"
        f" xmlns:dc='{NS_DC}'>"
        f"<dc:subject><rdf:Bag xml:lang=''>"
        f"<rdf:li>Heron</rdf:li>"
        f"</rdf:Bag></dc:subject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    editor = SidecarEditor(path_str)
    editor.add_keywords({"Owl"}, set())
    editor.commit()

    root = ET.parse(path_str).getroot()
    subjects = list(root.iter(SUBJECT))
    # Exactly one ``dc:subject`` bag: the pre-existing reset bag now
    # holds both keywords, rather than a fresh empty-language bag
    # sitting under a new Description.
    assert len(subjects) == 1
    bags = subjects[0].findall(f"{{{NS_RDF}}}Bag")
    assert len(bags) == 1
    assert bags[0].get(f"{{{xml_ns}}}lang") == ""
    items = sorted(li.text for li in bags[0].findall(f"{{{NS_RDF}}}li"))
    assert items == ["Heron", "Owl"]


def test_empty_xml_lang_reset_on_bag_lets_add_keyword_reuse_it(tmp_path):
    """An empty ``xml:lang="" `` reset on the bag doesn't force a duplicate bag.

    A bag whose only ``xml:*`` is a cancel-inheritance ``xml:lang=""``
    carries no distinctive language qualifier: the reset just says
    "items here have no known language". The old own-qualifier check
    saw the attribute and refused to reuse the bag, so a subsequent
    keyword add created a second ``dc:subject`` beside the perfectly
    reusable one -- and consumers reading only the first bag would
    then miss the freshly-written keyword. The check must apply
    ``xml:lang`` reset semantics on the bag itself, just as it already
    does at the owning Description level.
    """
    xml_ns = "http://www.w3.org/XML/1998/namespace"
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'"
        f" xmlns:xml='http://www.w3.org/XML/1998/namespace'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about='' xmlns:dc='{NS_DC}'>"
        f"<dc:subject><rdf:Bag xml:lang=''>"
        f"<rdf:li>Heron</rdf:li>"
        f"</rdf:Bag></dc:subject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    editor = SidecarEditor(path_str)
    editor.add_keywords({"Owl"}, set())
    editor.commit()

    root = ET.parse(path_str).getroot()
    subjects = list(root.iter(SUBJECT))
    # Exactly one ``dc:subject`` bag survives: the pre-existing one
    # kept its ``xml:lang=""`` reset and both keywords now live in it,
    # instead of a fresh unqualified ``dc:subject`` bag sitting beside
    # a lone ``Heron`` bag.
    assert len(subjects) == 1
    bags = subjects[0].findall(f"{{{NS_RDF}}}Bag")
    assert len(bags) == 1
    assert bags[0].get(f"{{{xml_ns}}}lang") == ""
    items = sorted(li.text for li in bags[0].findall(f"{{{NS_RDF}}}li"))
    assert items == ["Heron", "Owl"]


def test_rdf_value_attribute_abbreviation_is_read_and_updated(tmp_path):
    """A qualified property whose value lives in ``rdf:value=`` is honored.

    RDF/XML's attribute-abbreviation form lets ``<rdf:Description
    rdf:value='3' foo:source='camera'/>`` stand in for the element
    form. Readers must see the value, and writers must update the
    same attribute in place instead of appending an ``rdf:value``
    child element that leaves the stale attribute alongside a fresh
    conflicting value.
    """
    foo_ns = "http://example.com/foo/"
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about=''"
        f" xmlns:xmp='{NS_XMP}' xmlns:foo='{foo_ns}'>"
        f"<xmp:Rating>"
        f"<rdf:Description rdf:value='3' foo:source='camera'/>"
        f"</xmp:Rating>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    assert read_sync_preview_metadata(path_str)["rating"] == "3"

    write_rating(path_str, 5)

    root = ET.parse(path_str).getroot()
    ratings = list(root.iter(RATING))
    assert len(ratings) == 1
    # The nested Description still holds the value as an attribute --
    # no extra ``rdf:value`` element was appended.
    nested_descs = ratings[0].findall(f"{{{NS_RDF}}}Description")
    assert len(nested_descs) == 1
    assert nested_descs[0].get(f"{{{NS_RDF}}}value") == "5"
    assert nested_descs[0].find(f"{{{NS_RDF}}}value") is None
    # And the qualifier survived intact.
    assert nested_descs[0].get(f"{{{foo_ns}}}source") == "camera"

    assert read_sync_preview_metadata(path_str)["rating"] == "5"


def test_location_keyword_ownership_checked_across_all_bags(tmp_path):
    """A user's keyword in a qualified bag isn't reclassified as Vireo-owned.

    ``set_location_keywords`` used to judge ownership from the merge
    target ``_bag`` returned. When ``_bag`` decided every existing
    bag was qualified and created a fresh empty one, the ownership
    check saw no matching entry and marked the newly added keyword
    as Vireo-owned. ``remove_vireo_location_keywords`` then stripped
    exact matches from every photo-scoped bag on the next removal
    -- including the user's original qualified entry -- an
    irreversible data loss. Check ownership against every
    photo-scoped bag.
    """
    xml_ns = "http://www.w3.org/XML/1998/namespace"
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about=''"
        f" xmlns:dc='{NS_DC}' xmlns:lr='{NS_LR}'"
        f" xmlns:xml='http://www.w3.org/XML/1998/namespace'"
        f" xml:lang='en'>"
        f"<dc:subject><rdf:Bag><rdf:li>Kumeyaay Lake</rdf:li></rdf:Bag></dc:subject>"
        f"<lr:hierarchicalSubject>"
        f"<rdf:Bag><rdf:li>Places|Kumeyaay Lake</rdf:li></rdf:Bag>"
        f"</lr:hierarchicalSubject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    editor = SidecarEditor(path_str)
    editor.set_location_keywords(["Places", "Kumeyaay Lake"])
    editor.commit()

    # Now clear the Vireo-set location. If ownership were miscounted,
    # this would strip the user's original qualified entry too.
    editor = SidecarEditor(path_str)
    editor.remove_vireo_location_keywords()
    editor.commit()

    root = ET.parse(path_str).getroot()
    # The original qualified bag under ``xml:lang='en'`` still has the
    # user's keyword; ``remove_vireo_location_keywords`` left it alone.
    qualified_descs = [
        d for d in root.iter(f"{{{NS_RDF}}}Description")
        if d.get(f"{{{xml_ns}}}lang") == "en"
    ]
    assert len(qualified_descs) == 1
    q_items = sorted(
        li.text for li in qualified_descs[0].iter(f"{{{NS_RDF}}}li") if li.text
    )
    assert "Kumeyaay Lake" in q_items
    assert "Places|Kumeyaay Lake" in q_items


def test_rdf_datatype_blocks_duplicate_simple_property_removal(tmp_path):
    """A ``rdf:datatype`` marker on a duplicate is preserved.

    ``rdf:datatype`` changes the RDF literal's semantics (a typed
    literal vs. a plain literal), so it must count as a qualifier
    -- removing an occurrence carrying one silently drops the typing.

    Here the keeper is the attribute-form
    ``<xmp:Rating rdf:value='3'/>`` (highest score), and the typed
    plain-text occurrence is the duplicate the removal loop would
    otherwise sweep.
    """
    path = tmp_path / "photo.xmp"
    xsd = "http://www.w3.org/2001/XMLSchema"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about='' xmlns:xmp='{NS_XMP}'>"
        f"<xmp:Rating rdf:value='3'/>"
        f"<xmp:Rating rdf:datatype='{xsd}#integer'>3</xmp:Rating>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    write_rating(path_str, 5)

    root = ET.parse(path_str).getroot()
    ratings = list(root.iter(RATING))
    # The typed occurrence survives with its ``rdf:datatype``.
    typed = [
        r for r in ratings
        if r.get(f"{{{NS_RDF}}}datatype") == f"{xsd}#integer"
    ]
    assert len(typed) == 1


def test_owner_xml_lang_blocks_removal_of_plain_child_duplicate(tmp_path):
    """A duplicate under a language-qualified owner Description survives.

    When a plain-text child sits inside a Description that carries
    (or inherits) ``xml:lang``, that text is a language-tagged RDF
    statement in its own right. Removing the duplicate wholesale
    would silently drop the language tag. Preserve such duplicates.
    """
    xml_ns = "http://www.w3.org/XML/1998/namespace"
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about='' xmlns:xmp='{NS_XMP}'>"
        f"<xmp:Rating>3</xmp:Rating>"
        f"</rdf:Description>"
        f"<rdf:Description rdf:about=''"
        f" xmlns:xmp='{NS_XMP}'"
        f" xmlns:xml='http://www.w3.org/XML/1998/namespace'"
        f" xml:lang='en'>"
        f"<xmp:Rating>4</xmp:Rating>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    write_rating(path_str, 5)

    root = ET.parse(path_str).getroot()
    # The language-qualified duplicate still exists with its owner Description.
    qualified_descs = [
        d for d in root.iter(f"{{{NS_RDF}}}Description")
        if d.get(f"{{{xml_ns}}}lang") == "en"
    ]
    assert len(qualified_descs) == 1
    ratings_under_qualified = qualified_descs[0].findall(RATING)
    assert len(ratings_under_qualified) == 1


def test_rdf_value_attribute_occurrence_is_the_keeper(tmp_path):
    """An ``rdf:value`` attribute duplicate is chosen over an unqualified sibling.

    The attribute-abbreviation form is a qualified occurrence, so
    ``_property_occurrence_score`` must rank it above a plain-text
    child. Otherwise a rating update would update the plain-text
    duplicate and leave the qualified attribute with a stale value
    -- external readers might resolve the conflict differently.
    """
    foo_ns = "http://example.com/foo/"
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about=''"
        f" xmlns:xmp='{NS_XMP}' xmlns:foo='{foo_ns}'>"
        f"<xmp:Rating>3</xmp:Rating>"
        f"<xmp:Rating rdf:value='3' foo:source='camera'/>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    write_rating(path_str, 5)

    root = ET.parse(path_str).getroot()
    ratings = list(root.iter(RATING))
    attributed = [r for r in ratings if r.get(f"{{{NS_RDF}}}value") is not None]
    # The attribute-form qualified occurrence is the keeper; its
    # ``rdf:value`` was updated in place and its qualifier survived.
    assert len(attributed) == 1
    assert attributed[0].get(f"{{{NS_RDF}}}value") == "5"
    assert attributed[0].get(f"{{{foo_ns}}}source") == "camera"


def test_set_location_keywords_does_not_duplicate_a_qualified_flat_leaf(tmp_path):
    """Skip the flat insertion when the leaf already lives in a qualified bag.

    ``set_location_keywords`` used to unconditionally add the flat
    leaf regardless of whether it existed. When the user's leaf sat
    only in a qualified bag, ``_bag`` created a fresh empty bag and
    ``add_keywords`` inserted the leaf there. Ownership was False
    (``existed_flat`` correctly detected the user's copy), so a
    later ``remove_vireo_location_keywords`` cleared neither entry
    -- a permanent duplicate. Detecting the exact leaf text across
    every photo-scoped bag lets us skip that insertion.
    """
    xml_ns = "http://www.w3.org/XML/1998/namespace"
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about=''"
        f" xmlns:dc='{NS_DC}' xmlns:lr='{NS_LR}'"
        f" xmlns:xml='http://www.w3.org/XML/1998/namespace'"
        f" xml:lang='en'>"
        f"<dc:subject><rdf:Bag><rdf:li>Kumeyaay Lake</rdf:li></rdf:Bag></dc:subject>"
        f"<lr:hierarchicalSubject>"
        f"<rdf:Bag><rdf:li>Places|Kumeyaay Lake</rdf:li></rdf:Bag>"
        f"</lr:hierarchicalSubject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    editor = SidecarEditor(path_str)
    editor.set_location_keywords(["Places", "Kumeyaay Lake"])
    editor.commit()

    root = ET.parse(path_str).getroot()
    # Every ``dc:subject`` bag: no duplicates.
    all_flat_items = []
    for subj in root.iter(SUBJECT):
        for li in subj.iter(f"{{{NS_RDF}}}li"):
            if li.text:
                all_flat_items.append(li.text)
    assert all_flat_items == ["Kumeyaay Lake"]

    # The qualified bag still holds the user's original entry.
    qualified_descs = [
        d for d in root.iter(f"{{{NS_RDF}}}Description")
        if d.get(f"{{{xml_ns}}}lang") == "en"
    ]
    assert len(qualified_descs) == 1
    q_flat = sorted(
        li.text for subj in qualified_descs[0].findall(SUBJECT)
        for li in subj.iter(f"{{{NS_RDF}}}li") if li.text
    )
    assert q_flat == ["Kumeyaay Lake"]


def test_preserved_qualified_duplicate_gets_its_value_updated(tmp_path):
    """Qualified duplicates receive the new value, keeping their qualifiers.

    Before: the removal loop preserved a qualified duplicate but
    left its ``rdf:value`` untouched, so external readers could
    resolve the conflicting ratings differently from Vireo. The fix
    keeps the qualifier structure while updating the value in
    place.
    """
    foo_ns = "http://example.com/foo/"
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about=''"
        f" xmlns:xmp='{NS_XMP}' xmlns:foo='{foo_ns}'>"
        f"<xmp:Rating rdf:parseType='Resource'>"
        f"<rdf:value>3</rdf:value>"
        f"<foo:origin>keeper</foo:origin>"
        f"</xmp:Rating>"
        f"<xmp:Rating rdf:parseType='Resource'>"
        f"<rdf:value>2</rdf:value>"
        f"<foo:origin>preserve-me</foo:origin>"
        f"</xmp:Rating>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    write_rating(path_str, 5)

    root = ET.parse(path_str).getroot()
    ratings = list(root.iter(RATING))
    assert len(ratings) == 2
    # Every rating carries the updated value...
    for r in ratings:
        assert (r.find(f"{{{NS_RDF}}}value").text or "") == "5"
    # ...and every ``foo:origin`` qualifier is preserved.
    origins = sorted(
        r.findtext(f"{{{foo_ns}}}origin") for r in ratings
    )
    assert origins == ["keeper", "preserve-me"]


def test_qualified_rdf_li_is_read_as_the_keyword_value(tmp_path):
    """A qualified ``rdf:li`` value is read from its nested ``rdf:value``.

    A qualified item's direct ``.text`` is whitespace between
    children, so plain ``li.text`` misses the keyword. Reads must
    follow the nested value, otherwise every keyword-set consumer
    ignores the qualified entry.
    """
    xml_ns = "http://www.w3.org/XML/1998/namespace"
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about=''"
        f" xmlns:dc='{NS_DC}'"
        f" xmlns:xml='http://www.w3.org/XML/1998/namespace'>"
        f"<dc:subject>"
        f"<rdf:Bag>"
        f"<rdf:li rdf:parseType='Resource'>"
        f"<rdf:value>Heron</rdf:value>"
        f"<foo:source xmlns:foo='http://example.com/foo/'>ebird</foo:source>"
        f"</rdf:li>"
        f"</rdf:Bag>"
        f"</dc:subject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    assert "Heron" in read_keywords(path_str)


def test_xml_space_is_not_reset_with_empty_value(tmp_path):
    """A ``xml:space`` inherited from ``rdf:RDF`` is not written as empty.

    ``xml:lang=""`` is XML's way of saying "no language" and is a
    valid cancel. ``xml:space``, in contrast, only permits
    ``default`` or ``preserve``; writing ``xml:space=""`` produces
    invalid XML. The reset should only apply to ``xml:lang``.
    """
    xml_ns = "http://www.w3.org/XML/1998/namespace"
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'"
        f" xmlns:xml='http://www.w3.org/XML/1998/namespace'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}' xml:space='preserve'>"
        f"<rdf:Description rdf:about='' xmlns:dc='{NS_DC}'>"
        f"<dc:subject><rdf:Bag><rdf:li>Heron</rdf:li></rdf:Bag></dc:subject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    editor = SidecarEditor(path_str)
    editor.set_rating(4)
    editor.commit()

    root = ET.parse(path_str).getroot()
    # No Description carries an empty ``xml:space`` reset.
    for d in root.iter(f"{{{NS_RDF}}}Description"):
        if f"{{{xml_ns}}}space" in d.attrib:
            assert d.get(f"{{{xml_ns}}}space") in ("default", "preserve")


def test_qualified_attribute_form_duplicate_is_preserved_and_updated(tmp_path):
    """An attribute-form duplicate on a language-qualified Description survives.

    ``child is None`` used to blindly ``del owner.attrib[name]`` even
    when the owner Description carried (or inherited) ``xml:lang``.
    The attribute was a language-tagged RDF statement, so removing
    it silently dropped that statement. Preserve the attribute
    instead and update its value in place so both keeper and this
    qualified duplicate agree.
    """
    xml_ns = "http://www.w3.org/XML/1998/namespace"
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about='' xmlns:xmp='{NS_XMP}'>"
        f"<xmp:Rating rdf:parseType='Resource'>"
        f"<rdf:value>3</rdf:value>"
        f"</xmp:Rating>"
        f"</rdf:Description>"
        f"<rdf:Description rdf:about=''"
        f" xmlns:xmp='{NS_XMP}'"
        f" xmlns:xml='http://www.w3.org/XML/1998/namespace'"
        f" xml:lang='en'"
        f" xmp:Rating='4'/>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    write_rating(path_str, 5)

    root = ET.parse(path_str).getroot()
    # The language-qualified Description still carries its ``xmp:Rating``
    # attribute -- with the updated value.
    qualified_descs = [
        d for d in root.iter(f"{{{NS_RDF}}}Description")
        if d.get(f"{{{xml_ns}}}lang") == "en"
    ]
    assert len(qualified_descs) == 1
    assert qualified_descs[0].get(RATING) == "5"


def test_ancestor_xml_space_does_not_block_bag_reuse(tmp_path):
    """``xml:space`` on ``rdf:RDF`` does not force fresh Descriptions.

    Only ``xml:lang`` / ``xml:base`` change value semantics.
    ``xml:space`` is a whitespace directive, so a Description that
    inherits ``xml:space='preserve'`` from an ancestor is still a
    perfectly good target -- and reusing it prevents an
    ever-growing pile of duplicate ``dc:subject`` bags across
    repeated syncs.
    """
    xml_ns = "http://www.w3.org/XML/1998/namespace"
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'"
        f" xmlns:xml='http://www.w3.org/XML/1998/namespace'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}' xml:space='preserve'>"
        f"<rdf:Description rdf:about='' xmlns:dc='{NS_DC}'>"
        f"<dc:subject><rdf:Bag><rdf:li>Heron</rdf:li></rdf:Bag></dc:subject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    # Two consecutive syncs must not accumulate bags.
    editor = SidecarEditor(path_str)
    editor.add_keywords({"Kiwi"}, set())
    editor.commit()
    editor = SidecarEditor(path_str)
    editor.add_keywords({"Owl"}, set())
    editor.commit()

    root = ET.parse(path_str).getroot()
    subjects = list(root.iter(SUBJECT))
    assert len(subjects) == 1
    items = sorted(li.text for li in subjects[0].iter(f"{{{NS_RDF}}}li") if li.text)
    assert items == ["Heron", "Kiwi", "Owl"]

    # ``xml:space`` inheritance is left intact -- no empty reset was written.
    for d in root.iter(f"{{{NS_RDF}}}Description"):
        if f"{{{xml_ns}}}space" in d.attrib:
            assert d.get(f"{{{xml_ns}}}space") != ""


def test_qualified_rdf_li_can_be_removed_and_replaced(tmp_path):
    """Removal and hierarchy replacement match on the resolved ``rdf:li`` value.

    A qualified ``rdf:li`` stores its value in a nested ``rdf:value``
    (or attribute). Matching on ``li.text`` misses it, so removal
    and hierarchy replacement silently leave the qualified item in
    place. Use ``_li_value`` for matching and update the existing
    value spelling for replacements.
    """
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about='' xmlns:dc='{NS_DC}' xmlns:lr='{NS_LR}'>"
        f"<dc:subject><rdf:Bag>"
        f"<rdf:li rdf:parseType='Resource'>"
        f"<rdf:value>Heron</rdf:value>"
        f"</rdf:li>"
        f"</rdf:Bag></dc:subject>"
        f"<lr:hierarchicalSubject><rdf:Bag>"
        f"<rdf:li rdf:parseType='Resource'>"
        f"<rdf:value>Birds|Heron</rdf:value>"
        f"</rdf:li>"
        f"</rdf:Bag></lr:hierarchicalSubject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    # Hierarchy replacement: qualified rdf:li's nested value is updated
    # in place; li.text was whitespace so before the fix nothing happened.
    editor = SidecarEditor(path_str)
    editor.replace_keyword_hierarchies({"Birds|Heron": "Waterbirds|Heron"})
    editor.commit()
    assert read_hierarchical_keywords(path_str) == ["Waterbirds|Heron"]

    # Removal: qualified rdf:li is matched via _li_value and removed.
    editor = SidecarEditor(path_str)
    editor.remove_keywords({"Heron"})
    editor.commit()
    assert read_keywords(path_str) == set()
    assert read_hierarchical_keywords(path_str) == []


def test_ancestor_xml_base_does_not_block_bag_reuse(tmp_path):
    """``xml:base`` inheritance no longer forces a fresh photo Description.

    ``xml:base`` affects URI resolution, not the semantics of a
    literal keyword or numeric value. Treating it as a qualifier
    made ``_unqualified_photo_description`` create a fresh
    Description on every sync of an XMP that declared
    ``xml:base`` on ``rdf:RDF``, silently accumulating duplicate
    bags. It's no longer tracked; only ``xml:lang`` blocks reuse.
    """
    xml_ns = "http://www.w3.org/XML/1998/namespace"
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'"
        f" xmlns:xml='http://www.w3.org/XML/1998/namespace'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}' xml:base='https://example.com/'>"
        f"<rdf:Description rdf:about='' xmlns:dc='{NS_DC}'>"
        f"<dc:subject><rdf:Bag><rdf:li>Heron</rdf:li></rdf:Bag></dc:subject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    for keyword in ("Kiwi", "Owl", "Sparrow"):
        editor = SidecarEditor(path_str)
        editor.add_keywords({keyword}, set())
        editor.commit()

    root = ET.parse(path_str).getroot()
    subjects = list(root.iter(SUBJECT))
    assert len(subjects) == 1
    items = sorted(
        li.text for li in subjects[0].iter(f"{{{NS_RDF}}}li") if li.text
    )
    assert items == ["Heron", "Kiwi", "Owl", "Sparrow"]

    # The inherited ``xml:base`` on ``rdf:RDF`` survives; no reset was written.
    xml_base = f"{{{xml_ns}}}base"
    for d in root.iter(f"{{{NS_RDF}}}Description"):
        assert xml_base not in d.attrib


def test_qualified_variant_is_canonicalized_in_place_preserving_qualifiers(tmp_path):
    """A qualified variant of the leaf keeps its qualifiers on canonicalization.

    Before: ``set_location_keywords``'s canonicalization step called
    ``remove_keywords`` on a case variant like ``paris``. After the
    ``_li_value`` fix, a qualified ``<rdf:li rdf:parseType='Resource'>
    <rdf:value>paris</rdf:value><foo:source>user</foo:source></rdf:li>``
    matched — and the wholesale removal silently dropped
    ``foo:source``. The fix now updates such a qualified variant's
    nested value to the canonical spelling in place, so the
    qualifier survives and the follow-up add sees the leaf as
    already present.
    """
    foo_ns = "http://example.com/foo/"
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about=''"
        f" xmlns:dc='{NS_DC}' xmlns:lr='{NS_LR}' xmlns:foo='{foo_ns}'>"
        f"<dc:subject><rdf:Bag>"
        f"<rdf:li rdf:parseType='Resource'>"
        f"<rdf:value>paris</rdf:value>"
        f"<foo:source>user</foo:source>"
        f"</rdf:li>"
        f"</rdf:Bag></dc:subject>"
        f"<lr:hierarchicalSubject><rdf:Bag>"
        f"<rdf:li>Places|Paris</rdf:li>"
        f"</rdf:Bag></lr:hierarchicalSubject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    editor = SidecarEditor(path_str)
    editor.set_location_keywords(["Places", "Paris"])
    editor.commit()

    root = ET.parse(path_str).getroot()
    # Exactly one flat entry across every ``dc:subject`` bag: the
    # canonicalized qualified item, with its ``foo:source`` intact.
    lis = []
    for subj in root.iter(SUBJECT):
        for li in subj.iter(f"{{{NS_RDF}}}li"):
            lis.append(li)
    assert len(lis) == 1
    assert _li_value_local(lis[0]) == "Paris"
    assert lis[0].findtext(f"{{{foo_ns}}}source") == "user"


def _li_value_local(li):
    """Local helper mirroring ``xmp._li_value`` for test assertions."""
    rdf_value = li.find(f"{{{NS_RDF}}}value")
    if rdf_value is not None:
        return (rdf_value.text or "").strip()
    return li.text


def test_hierarchy_replacement_preserves_qualified_collision(tmp_path):
    """A qualified duplicate created by hierarchy replacement survives.

    ``replace_keyword_hierarchies({"Birds|Legacy": "Birds|Canonical"})``
    on a bag that also contains an unqualified ``Birds|Canonical``
    used to remove the qualified ``Birds|Legacy`` outright when its
    resolved value collided with the previously-kept item's — silently
    dropping any ``foo:source`` sibling qualifier. Keep the qualified
    item in place so its metadata survives; the deduplicate-by-text
    contract still holds for plain duplicates.
    """
    foo_ns = "http://example.com/foo/"
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about=''"
        f" xmlns:lr='{NS_LR}' xmlns:foo='{foo_ns}'>"
        f"<lr:hierarchicalSubject><rdf:Bag>"
        f"<rdf:li>Birds|Canonical</rdf:li>"
        f"<rdf:li rdf:parseType='Resource'>"
        f"<rdf:value>Birds|Legacy</rdf:value>"
        f"<foo:source>user</foo:source>"
        f"</rdf:li>"
        f"</rdf:Bag></lr:hierarchicalSubject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    editor = SidecarEditor(path_str)
    editor.replace_keyword_hierarchies({"Birds|Legacy": "Birds|Canonical"})
    editor.commit()

    root = ET.parse(path_str).getroot()
    lis = list(root.iter(f"{{{NS_RDF}}}li"))
    # The qualified item wins the keeper slot -- the plain-text
    # duplicate was removed, its ``rdf:value`` was rewritten to the
    # canonical string, and its ``foo:source`` qualifier survives.
    qualified = [li for li in lis if li.find(f"{{{foo_ns}}}source") is not None]
    assert len(qualified) == 1
    assert (qualified[0].find(f"{{{NS_RDF}}}value").text or "") == "Birds|Canonical"
    assert qualified[0].findtext(f"{{{foo_ns}}}source") == "user"


def test_add_keywords_dedupes_across_all_photo_scoped_bags(tmp_path):
    """``add_keywords`` doesn't duplicate a keyword already in a qualified bag.

    When every existing ``dc:subject`` occurrence is qualified,
    ``_bag`` creates a fresh unqualified target. Reading only that
    target let a keyword already present in a qualified sibling
    land as a plain-text duplicate. Deduplicate against every
    photo-scoped bag.
    """
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about=''"
        f" xmlns:dc='{NS_DC}'"
        f" xmlns:xml='http://www.w3.org/XML/1998/namespace'"
        f" xml:lang='en'>"
        f"<dc:subject><rdf:Bag><rdf:li>Heron</rdf:li></rdf:Bag></dc:subject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    editor = SidecarEditor(path_str)
    editor.add_keywords({"Heron"}, set())
    editor.commit()

    root = ET.parse(path_str).getroot()
    all_texts = []
    for subj in root.iter(SUBJECT):
        for li in subj.iter(f"{{{NS_RDF}}}li"):
            if li.text:
                all_texts.append(li.text)
    # ``Heron`` was already present in the qualified bag; no second copy.
    assert all_texts == ["Heron"]


def test_replace_hierarchy_none_removes_qualified_item(tmp_path):
    """An explicit ``None`` in ``replace_keyword_hierarchies`` deletes the item.

    Merge planning sometimes maps an obsolete hierarchy to ``None``
    to strip it. A qualified item must not survive that -- otherwise
    the sync finishes with the rejected path still visible and a
    later re-import can bring it back.
    """
    foo_ns = "http://example.com/foo/"
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about=''"
        f" xmlns:lr='{NS_LR}' xmlns:foo='{foo_ns}'>"
        f"<lr:hierarchicalSubject><rdf:Bag>"
        f"<rdf:li rdf:parseType='Resource'>"
        f"<rdf:value>Birds|Stale</rdf:value>"
        f"<foo:source>legacy</foo:source>"
        f"</rdf:li>"
        f"</rdf:Bag></lr:hierarchicalSubject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    editor = SidecarEditor(path_str)
    editor.replace_keyword_hierarchies({"Birds|Stale": None})
    editor.commit()

    root = ET.parse(path_str).getroot()
    lis = list(root.iter(f"{{{NS_RDF}}}li"))
    assert lis == []


def test_rdf_datatype_child_ranks_as_qualified_keeper(tmp_path):
    """A ``<xmp:Rating rdf:datatype='...'>3</xmp:Rating>`` wins the keeper slot.

    A plain-text child carrying ``rdf:datatype`` (or another
    non-structural attribute) changes the RDF literal's semantics
    even without children. Rank such an occurrence as qualified so
    reads and writes agree on it, and a stale plain sibling doesn't
    win the keeper score.
    """
    path = tmp_path / "photo.xmp"
    xsd = "http://www.w3.org/2001/XMLSchema"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about='' xmlns:xmp='{NS_XMP}'>"
        f"<xmp:Rating>2</xmp:Rating>"
        f"<xmp:Rating rdf:datatype='{xsd}#integer'>3</xmp:Rating>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    # Read agrees with the qualified occurrence: the typed value.
    assert read_sync_preview_metadata(path_str)["rating"] == "3"

    write_rating(path_str, 5)

    root = ET.parse(path_str).getroot()
    ratings = list(root.iter(RATING))
    # The typed rating is the keeper -- its value was updated in place
    # and its ``rdf:datatype`` survived; the plain sibling was removed.
    typed = [
        r for r in ratings
        if r.get(f"{{{NS_RDF}}}datatype") == f"{xsd}#integer"
    ]
    assert len(typed) == 1
    assert (typed[0].text or "").strip() == "5"


def test_remove_location_prefers_exact_across_bags_over_per_bag_fallback(tmp_path):
    """Cross-bag exact match beats a per-bag variant fallback.

    ``_remove_location_keyword_entries`` used to run its
    exact-then-fallback logic INSIDE each bag. When Vireo's owned
    canonical entry lived in one photo-scoped bag and a user's
    normalized variant lived in another, both were deleted: the
    variant as the first bag's fallback (no exact there) and the
    canonical from the second. Collect exact matches across every
    bag first; only fall back to a normalized match when none
    exists anywhere.

    Set up the state directly with a Vireo ownership marker so this
    test focuses on the ``_remove_location_keyword_entries`` matcher
    without depending on the whole set-then-remove flow.
    """
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about=''"
        f" xmlns:dc='{NS_DC}' xmlns:lr='{NS_LR}' xmlns:vireo='{NS_VIREO}'"
        f" vireo:locationKeywords='Places|Paris'"
        f" vireo:locationKeywordsOwned='flat,hier'>"
        f"<dc:subject><rdf:Bag>"
        f"<rdf:li>paris</rdf:li>"
        f"</rdf:Bag></dc:subject>"
        f"</rdf:Description>"
        f"<rdf:Description rdf:about='' xmlns:dc='{NS_DC}'>"
        f"<dc:subject><rdf:Bag>"
        f"<rdf:li>Paris</rdf:li>"
        f"</rdf:Bag></dc:subject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    editor = SidecarEditor(path_str)
    editor.remove_vireo_location_keywords()
    editor.commit()

    root = ET.parse(path_str).getroot()
    # Only Vireo's exact ``Paris`` is removed. The user's ``paris``
    # variant in the sibling bag survives -- per-bag fallback would
    # have deleted it too.
    all_flat = sorted(
        li.text
        for subj in root.iter(SUBJECT)
        for li in subj.iter(f"{{{NS_RDF}}}li")
        if li.text
    )
    assert all_flat == ["paris"]


def test_remove_location_preserves_qualified_exact_duplicate(tmp_path):
    """A qualified exact-match duplicate is preserved on removal.

    Vireo only ever writes plain ``<rdf:li>Value</rdf:li>``. If a
    user or another tool later adds a qualified ``<rdf:li
    rdf:parseType='Resource'><rdf:value>Paris</rdf:value>
    <foo:source>user</foo:source></rdf:li>`` next to Vireo's own
    plain copy, the qualified duplicate carries metadata we mustn't
    drop. Preserve qualified exact-match duplicates; remove only
    the plain Vireo-authored occurrence.
    """
    foo_ns = "http://example.com/foo/"
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about=''"
        f" xmlns:dc='{NS_DC}' xmlns:vireo='{NS_VIREO}'"
        f" xmlns:foo='{foo_ns}'"
        f" vireo:locationKeywords='Places|Paris'"
        f" vireo:locationKeywordsOwned='flat,hier'>"
        f"<dc:subject><rdf:Bag>"
        f"<rdf:li>Paris</rdf:li>"
        f"<rdf:li rdf:parseType='Resource'>"
        f"<rdf:value>Paris</rdf:value>"
        f"<foo:source>user</foo:source>"
        f"</rdf:li>"
        f"</rdf:Bag></dc:subject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    editor = SidecarEditor(path_str)
    editor.remove_vireo_location_keywords()
    editor.commit()

    root = ET.parse(path_str).getroot()
    # The plain Vireo copy is gone; the qualified user copy survives
    # with its ``foo:source`` intact.
    remaining = list(root.iter(f"{{{NS_RDF}}}li"))
    assert len(remaining) == 1
    assert (remaining[0].find(f"{{{NS_RDF}}}value").text or "") == "Paris"
    assert remaining[0].findtext(f"{{{foo_ns}}}source") == "user"


def test_set_location_keywords_skips_empty_merge_bags_when_present(tmp_path):
    """A no-op re-write of an existing qualified location doesn't add empty bags.

    ``set_location_keywords`` used to call ``_bag`` eagerly to
    "ensure the bags exist" for the follow-up ``add_keywords``. Now
    that ``add_keywords`` creates its merge target lazily, the eager
    call is not just redundant: when the requested leaf and hierarchy
    already live only in a qualified sibling bag, it commits an
    empty unqualified ``dc:subject`` and ``lr:hierarchicalSubject``
    beside the populated ones -- and consumers resolving to a single
    occurrence would then report the photo as having no keywords.
    """
    xml_ns = "http://www.w3.org/XML/1998/namespace"
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'"
        f" xmlns:xml='http://www.w3.org/XML/1998/namespace'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}' xml:lang='en'>"
        f"<rdf:Description rdf:about=''"
        f" xmlns:dc='{NS_DC}' xmlns:lr='{NS_LR}'>"
        f"<dc:subject><rdf:Bag>"
        f"<rdf:li>Paris</rdf:li>"
        f"</rdf:Bag></dc:subject>"
        f"<lr:hierarchicalSubject><rdf:Bag>"
        f"<rdf:li>Places|Paris</rdf:li>"
        f"</rdf:Bag></lr:hierarchicalSubject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    editor = SidecarEditor(path_str)
    editor.set_location_keywords(["Places", "Paris"])
    editor.commit()

    root = ET.parse(path_str).getroot()
    # Exactly one ``dc:subject`` and one ``lr:hierarchicalSubject`` --
    # the pre-existing populated ones -- with no empty duplicates.
    subjects = list(root.iter(SUBJECT))
    hier_subjects = list(root.iter(HIERARCHICAL_SUBJECT))
    assert len(subjects) == 1
    assert len(hier_subjects) == 1
    flat_items = [
        li.text
        for li in subjects[0].iter(f"{{{NS_RDF}}}li")
        if li.text
    ]
    hier_items = [
        li.text
        for li in hier_subjects[0].iter(f"{{{NS_RDF}}}li")
        if li.text
    ]
    assert flat_items == ["Paris"]
    assert hier_items == ["Places|Paris"]
    _ = xml_ns


def test_xml_space_on_bag_does_not_force_a_duplicate_bag(tmp_path):
    """A local ``xml:space`` directive on a bag doesn't disqualify it for reuse.

    ``xml:space`` is a whitespace directive that never changes the
    meaning of a literal keyword or numeric value -- and
    ``_ancestor_carries_xml_qualifier`` already ignores it when it's
    inherited from an ancestor. A bag carrying only ``xml:space="preserve"``
    is just as reusable as one with no ``xml:*`` at all; before this
    fix the local-attribute check saw the directive and refused to
    reuse the bag, so a keyword add committed a second ``dc:subject``
    beside the perfectly-usable one. The same holds for ``xml:base``.
    """
    xml_ns = "http://www.w3.org/XML/1998/namespace"
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'"
        f" xmlns:xml='http://www.w3.org/XML/1998/namespace'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about='' xmlns:dc='{NS_DC}'>"
        f"<dc:subject><rdf:Bag xml:space='preserve'>"
        f"<rdf:li>Heron</rdf:li>"
        f"</rdf:Bag></dc:subject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    editor = SidecarEditor(path_str)
    editor.add_keywords({"Owl"}, set())
    editor.commit()

    root = ET.parse(path_str).getroot()
    subjects = list(root.iter(SUBJECT))
    # One bag, preserving its ``xml:space``, now carries both keywords.
    assert len(subjects) == 1
    bags = subjects[0].findall(f"{{{NS_RDF}}}Bag")
    assert len(bags) == 1
    assert bags[0].get(f"{{{xml_ns}}}space") == "preserve"
    items = sorted(li.text for li in bags[0].findall(f"{{{NS_RDF}}}li"))
    assert items == ["Heron", "Owl"]


def test_set_location_keywords_canonicalizes_container_qualified_leaf_in_place(tmp_path):
    """A container-``xml:lang'' leaf is canonicalized in place, not dropped.

    A user's flat variant may sit as bare-text ``<rdf:li>paris</rdf:li>``
    under a ``<dc:subject xml:lang="fr">``: the language qualifier is
    inherited from the container, not carried on the ``rdf:li``. Before
    this fix, ``set_location_keywords`` treated the item as plain (no
    own qualifiers), so the canonicalization step let the following
    ``remove_keywords`` drop the language-qualified statement entirely
    and re-add an unqualified ``Paris`` on a fresh unqualified bag.
    The check now walks the ancestor chain for effective ``xml:lang``
    on each ``rdf:li`` and canonicalizes qualified spellings in place,
    preserving the container's ``fr`` on the surviving ``rdf:li``.
    """
    xml_ns = "http://www.w3.org/XML/1998/namespace"
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'"
        f" xmlns:xml='http://www.w3.org/XML/1998/namespace'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about='' xmlns:dc='{NS_DC}'>"
        f"<dc:subject xml:lang='fr'><rdf:Bag>"
        f"<rdf:li>paris</rdf:li>"
        f"</rdf:Bag></dc:subject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    editor = SidecarEditor(path_str)
    editor.set_location_keywords(["Places", "Paris"])
    editor.commit()

    root = ET.parse(path_str).getroot()
    subjects = list(root.iter(SUBJECT))
    # The qualified ``<dc:subject xml:lang="fr">`` survives, its
    # ``paris`` was renamed to the canonical ``Paris`` in place, and
    # no unqualified duplicate landed beside it.
    fr_subjects = [s for s in subjects if s.get(f"{{{xml_ns}}}lang") == "fr"]
    assert len(fr_subjects) == 1
    fr_items = [
        li.text
        for li in fr_subjects[0].iter(f"{{{NS_RDF}}}li")
        if li.text
    ]
    assert fr_items == ["Paris"]


def test_remove_location_preserves_container_qualified_exact_duplicate(tmp_path):
    """A container-``xml:lang'' duplicate isn't picked as Vireo's plain removal target.

    When Vireo's owned plain ``<rdf:li>Paris</rdf:li>`` coexists in a
    plain bag with a user-authored ``<rdf:li>Paris</rdf:li>`` under a
    sibling ``<dc:subject xml:lang="fr">``, both matched the previous
    "plain exact" filter because it only inspected the ``rdf:li`` itself.
    ``plain_exact_targets[0]`` could then take the user's language-qualified
    occurrence, leave Vireo's plain copy behind, and still clear the
    ownership marker. The removal filter now walks the item's effective
    inherited ``xml:lang`` and skips any item under a language-qualified
    container.
    """
    xml_ns = "http://www.w3.org/XML/1998/namespace"
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'"
        f" xmlns:xml='http://www.w3.org/XML/1998/namespace'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about=''"
        f" xmlns:dc='{NS_DC}' xmlns:lr='{NS_LR}' xmlns:vireo='{NS_VIREO}'"
        f" vireo:locationKeywords='Places|Paris'"
        f" vireo:locationKeywordsOwned='flat,hier'>"
        f"<dc:subject xml:lang='fr'><rdf:Bag>"
        f"<rdf:li>Paris</rdf:li>"
        f"</rdf:Bag></dc:subject>"
        f"<dc:subject><rdf:Bag>"
        f"<rdf:li>Paris</rdf:li>"
        f"</rdf:Bag></dc:subject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    editor = SidecarEditor(path_str)
    editor.remove_vireo_location_keywords()
    editor.commit()

    root = ET.parse(path_str).getroot()
    # The user's ``fr`` bag still holds ``Paris``. Vireo's own plain
    # ``dc:subject`` bag no longer holds ``Paris`` -- it's the one
    # entry removal was allowed to touch.
    fr_items = [
        li.text
        for subj in root.iter(SUBJECT)
        for li in subj.iter(f"{{{NS_RDF}}}li")
        for a in [subj.get(f"{{{xml_ns}}}lang")]
        if a == "fr" and li.text
    ]
    plain_items = [
        li.text
        for subj in root.iter(SUBJECT)
        for li in subj.iter(f"{{{NS_RDF}}}li")
        for a in [subj.get(f"{{{xml_ns}}}lang")]
        if a is None and li.text
    ]
    assert fr_items == ["Paris"]
    assert plain_items == []


def test_remove_location_falls_back_past_qualified_exact_duplicate(tmp_path):
    """A qualified exact duplicate must not block the normalized-owned fallback.

    Vireo's ownership marker records the exact leaf spelling. If a
    later tool rewrote Vireo's plain entry (say ``Paris`` → ``paris``)
    AND then a user added a qualified exact ``Paris`` next to it, the
    old ``any_exact`` check saw the qualified match, suppressed the
    normalized fallback, and did nothing -- so cleanup cleared the
    ownership markers while Vireo's ``paris`` sat there permanently.
    The fallback now runs whenever no *plain* exact match exists,
    which removes the owned spelling variant and leaves the qualified
    duplicate untouched.
    """
    foo_ns = "http://example.com/foo/"
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about=''"
        f" xmlns:dc='{NS_DC}' xmlns:lr='{NS_LR}' xmlns:vireo='{NS_VIREO}'"
        f" xmlns:foo='{foo_ns}'"
        f" vireo:locationKeywords='Places|Paris'"
        f" vireo:locationKeywordsOwned='flat,hier'>"
        f"<dc:subject><rdf:Bag>"
        f"<rdf:li>paris</rdf:li>"
        f"<rdf:li rdf:parseType='Resource'>"
        f"<rdf:value>Paris</rdf:value>"
        f"<foo:source>user</foo:source>"
        f"</rdf:li>"
        f"</rdf:Bag></dc:subject>"
        f"<lr:hierarchicalSubject><rdf:Bag>"
        f"<rdf:li>Places|paris</rdf:li>"
        f"<rdf:li rdf:parseType='Resource'>"
        f"<rdf:value>Places|Paris</rdf:value>"
        f"<foo:source>user</foo:source>"
        f"</rdf:li>"
        f"</rdf:Bag></lr:hierarchicalSubject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    editor = SidecarEditor(path_str)
    editor.remove_vireo_location_keywords()
    editor.commit()

    root = ET.parse(path_str).getroot()
    # Vireo's owned ``paris`` / ``Places|paris`` are gone; the qualified
    # user duplicates survive with their ``foo:source`` intact.
    flat_bag = next(iter(root.iter(SUBJECT))).find(f"{{{NS_RDF}}}Bag")
    flat_items = list(flat_bag.findall(f"{{{NS_RDF}}}li"))
    assert len(flat_items) == 1
    assert (flat_items[0].find(f"{{{NS_RDF}}}value").text or "") == "Paris"
    assert flat_items[0].findtext(f"{{{foo_ns}}}source") == "user"

    hier_bag = next(iter(root.iter(HIERARCHICAL_SUBJECT))).find(f"{{{NS_RDF}}}Bag")
    hier_items = list(hier_bag.findall(f"{{{NS_RDF}}}li"))
    assert len(hier_items) == 1
    assert (hier_items[0].find(f"{{{NS_RDF}}}value").text or "") == "Places|Paris"
    assert hier_items[0].findtext(f"{{{foo_ns}}}source") == "user"


def test_remove_location_removes_only_one_plain_owned_duplicate(tmp_path):
    """Vireo owns one plain entry, not every plain occurrence.

    When a user (or another tool) adds a second plain ``<rdf:li>Paris
    </rdf:li>`` alongside Vireo's own plain copy, Vireo removed both
    entries on location removal -- silently deleting the user's data.
    Vireo authored exactly one entry, so only one plain occurrence
    should be removed on removal; the sibling plain entry survives.
    """
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about=''"
        f" xmlns:dc='{NS_DC}' xmlns:lr='{NS_LR}' xmlns:vireo='{NS_VIREO}'"
        f" vireo:locationKeywords='Places|Paris'"
        f" vireo:locationKeywordsOwned='flat,hier'>"
        f"<dc:subject><rdf:Bag>"
        f"<rdf:li>Paris</rdf:li>"
        f"<rdf:li>Paris</rdf:li>"
        f"</rdf:Bag></dc:subject>"
        f"<lr:hierarchicalSubject><rdf:Bag>"
        f"<rdf:li>Places|Paris</rdf:li>"
        f"<rdf:li>Places|Paris</rdf:li>"
        f"</rdf:Bag></lr:hierarchicalSubject>"
        f"</rdf:Description>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path_str = str(path)

    editor = SidecarEditor(path_str)
    editor.remove_vireo_location_keywords()
    editor.commit()

    root = ET.parse(path_str).getroot()
    flat = sorted(
        li.text
        for subj in root.iter(SUBJECT)
        for li in subj.iter(f"{{{NS_RDF}}}li")
        if li.text
    )
    assert flat == ["Paris"]
    hier = sorted(
        li.text
        for subj in root.iter(HIERARCHICAL_SUBJECT)
        for li in subj.iter(f"{{{NS_RDF}}}li")
        if li.text
    )
    assert hier == ["Places|Paris"]


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
