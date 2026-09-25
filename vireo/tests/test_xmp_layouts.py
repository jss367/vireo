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
