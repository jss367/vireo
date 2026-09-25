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


def test_ambiguous_non_empty_subjects_do_not_designate_a_photo(tmp_path):
    """Distinct non-empty rdf:about values never elect a photo by document order.

    A sidecar carrying an auxiliary ``#thumbnail`` Description before a
    ``uuid:photo`` Description must not silently treat the first one as
    the photo. Reads return nothing, and a write that creates its own
    Description scopes it to the empty (enclosing-resource) subject so
    the auxiliary and photo Descriptions are left alone.
    """
    path = tmp_path / "photo.xmp"
    path.write_text(
        f"<x:xmpmeta xmlns:x='adobe:ns:meta/'>"
        f"<rdf:RDF xmlns:rdf='{NS_RDF}'>"
        f"<rdf:Description rdf:about='#thumbnail'"
        f" xmlns:xmp='{NS_XMP}' xmlns:exif='{NS_EXIF}'"
        f" xmp:Rating='1' exif:GPSLatitude='40,0.0N' exif:GPSLongitude='40,0.0E'/>"
        f"<rdf:Description rdf:about='uuid:photo'"
        f" xmlns:xmp='{NS_XMP}' xmp:Rating='4'/>"
        f"</rdf:RDF></x:xmpmeta>"
    )
    path = str(path)

    metadata = read_sync_preview_metadata(path)
    assert metadata["rating"] is None
    assert metadata["location"] is None
    assert read_keywords(path) == set()

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
    assert thumb[0].get(RATING) == "1"
    assert thumb[0].get(GPS_LATITUDE) == "40,0.0N"
    assert photo_uuid[0].get(RATING) == "4"

    fresh = [
        d for d in root.iter(f"{{{NS_RDF}}}Description")
        if (d.get(f"{{{NS_RDF}}}about") or "") == ""
        and d.get(f"{{{NS_VIREO}}}gpsSource") == "assigned"
    ]
    assert len(fresh) == 1
    metadata = read_sync_preview_metadata(path)
    assert metadata["location"]["latitude"] == pytest.approx(-33.5)
    assert metadata["location"]["longitude"] == pytest.approx(-70.25)


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


def test_rating_survives_ambiguous_non_empty_subjects(tmp_path):
    """A rating-only write on a sidecar with ambiguous subjects still lands.

    When every top-level Description carries a distinct non-empty ``rdf:about``
    (an auxiliary ``#thumbnail`` before a ``uuid:photo`` Description, say),
    no existing Description belongs to the photo. A rating-only write must
    still land -- on a fresh empty-subject Description -- instead of silently
    reporting success while dropping the value; a NAS sync would otherwise
    clear the queued rating with nothing written.
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
    assert photo_uuid[0].get(RATING) == "4"

    fresh = [
        d for d in root.iter(f"{{{NS_RDF}}}Description")
        if (d.get(f"{{{NS_RDF}}}about") or "") == ""
    ]
    assert len(fresh) == 1
    stored = fresh[0].get(RATING) or fresh[0].findtext(RATING)
    assert stored == "5"
    assert read_sync_preview_metadata(path)["rating"] == "5"


def test_rating_only_write_does_not_create_missing_sidecar(tmp_path):
    """A standalone rating write against a missing sidecar creates nothing."""
    path = str(tmp_path / "missing.xmp")

    write_rating(path, 4)

    assert not os.path.exists(path)


def test_sync_preview_marks_rating_writable_on_ambiguous_subjects(tmp_path):
    """A readable sidecar with ambiguous subjects reports rating_writable=True.

    ``set_rating`` creates a fresh empty-subject Description in that case,
    so the sync preview must report the rating as a real write rather than
    "unchanged". Otherwise the pending-changes review shows the rating as
    staying only in Vireo while the sync would actually write it, and the
    queued change is cleared with nothing user-visible in the sidecar.
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
    assert metadata["rating"] is None
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
