"""A sidecar Vireo rewrites keeps the markup the tool that wrote it chose.

Lightroom, ExifTool and digiKam each write their own prefixes, comments and
``<?xpacket?>`` wrapper. A Vireo edit must leave everything it didn't touch
as that tool wrote it -- same prefixes, same wrapper, same comments -- and a
sidecar Vireo creates must look like a standard XMP writer's.
"""

import os
import sys
from xml.etree import ElementTree as ET

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from xmp import (
    NS_RDF,
    NS_VIREO,
    NS_XMP,
    NS_XMPDM,
    SidecarEditor,
    read_hierarchical_keywords,
    read_keywords,
    read_sync_preview_metadata,
    remove_keywords,
    write_edit_recipe,
    write_rating,
    write_sidecar,
)

NS_CRS = "http://ns.adobe.com/camera-raw-settings/1.0/"
NS_XMPMM = "http://ns.adobe.com/xap/1.0/mm/"
NS_STEVT = "http://ns.adobe.com/xap/1.0/sType/ResourceEvent#"
NS_DIGIKAM = "http://www.digikam.org/ns/1.0/"

# A Lightroom Classic sidecar: xpacket wrapper, one-space indent, every
# namespace declared on the Description, develop settings as ``crs:``
# attributes, an ``xmpMM:History`` of ``stEvt:`` events, plus a comment and
# a digiKam property another tool added.
LIGHTROOM_XMP = """\
<?xpacket begin="﻿" id="W5M0MpCehiHzreSzNTczkc9d"?>
<!-- Lightroom Classic 14.0 -->
<x:xmpmeta xmlns:x="adobe:ns:meta/" x:xmptk="Adobe XMP Core 7.0-c000 1.000000, 0000/00/00-00:00:00        ">
 <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
  <rdf:Description rdf:about=""
    xmlns:xmp="http://ns.adobe.com/xap/1.0/"
    xmlns:xmpMM="http://ns.adobe.com/xap/1.0/mm/"
    xmlns:stEvt="http://ns.adobe.com/xap/1.0/sType/ResourceEvent#"
    xmlns:crs="http://ns.adobe.com/camera-raw-settings/1.0/"
    xmlns:dc="http://purl.org/dc/elements/1.1/"
    xmlns:lr="http://ns.adobe.com/lightroom/1.0/"
    xmlns:digiKam="http://www.digikam.org/ns/1.0/"
   xmp:Rating="2"
   xmpMM:InstanceID="xmp.iid:5f1c"
   crs:Version="17.0"
   crs:Exposure2012="+0.35"
   crs:HasSettings="True">
   <!-- develop history -->
   <xmpMM:History>
    <rdf:Seq>
     <rdf:li
      stEvt:action="saved"
      stEvt:instanceID="xmp.iid:5f1c"
      stEvt:when="2026-09-01T10:00:00-07:00"
      stEvt:softwareAgent="Adobe Photoshop Lightroom Classic 14.0 (Macintosh)"
      stEvt:changed="/metadata"/>
    </rdf:Seq>
   </xmpMM:History>
   <crs:ToneCurvePV2012>
    <rdf:Seq>
     <rdf:li>0, 0</rdf:li>
     <rdf:li>255, 255</rdf:li>
    </rdf:Seq>
   </crs:ToneCurvePV2012>
   <digiKam:TagsList>
    <rdf:Seq>
     <rdf:li>Birds/Heron</rdf:li>
    </rdf:Seq>
   </digiKam:TagsList>
   <dc:subject>
    <rdf:Bag>
     <rdf:li>Heron</rdf:li>
     <!-- added by hand -->
     <rdf:li>Marsh</rdf:li>
    </rdf:Bag>
   </dc:subject>
   <lr:hierarchicalSubject>
    <rdf:Bag>
     <rdf:li>Birds|Heron</rdf:li>
    </rdf:Bag>
   </lr:hierarchicalSubject>
  </rdf:Description>
 </rdf:RDF>
</x:xmpmeta>
<?xpacket end="w"?>"""

LIGHTROOM_PREFIXES = (
    "xmpMM:History", "xmpMM:InstanceID", "stEvt:action", "stEvt:changed",
    "crs:Version", "crs:Exposure2012", "crs:ToneCurvePV2012",
    "digiKam:TagsList", "dc:subject", "lr:hierarchicalSubject",
    'xmlns:stEvt="http://ns.adobe.com/xap/1.0/sType/ResourceEvent#"',
    'xmlns:digiKam="http://www.digikam.org/ns/1.0/"',
)


def _lightroom_sidecar(tmp_path):
    path = tmp_path / "photo.xmp"
    path.write_text(LIGHTROOM_XMP, encoding="utf-8")
    return path


def _untouched_by_rating_and_keywords(root):
    """The elements and attributes a rating and keyword edit must not change."""
    desc = root.find(f"{{{NS_RDF}}}RDF/{{{NS_RDF}}}Description")
    kept = {
        name: value for name, value in desc.attrib.items()
        if name not in (f"{{{NS_XMP}}}Rating", f"{{{NS_XMPDM}}}pick")
    }

    def subtree(tag):
        # Indentation is rewritten; everything else must match.
        return ET.canonicalize(ET.tostring(desc.find(tag)), strip_text=True)

    return (
        kept,
        subtree(f"{{{NS_XMPMM}}}History"),
        subtree(f"{{{NS_CRS}}}ToneCurvePV2012"),
        subtree(f"{{{NS_DIGIKAM}}}TagsList"),
        root.attrib,
    )


def test_lightroom_sidecar_keeps_prefixes_wrapper_and_comments(tmp_path):
    """After a Vireo edit everything else reads as Lightroom wrote it."""
    path = _lightroom_sidecar(tmp_path)
    before = _untouched_by_rating_and_keywords(ET.parse(path).getroot())

    editor = SidecarEditor(str(path))
    editor.set_rating(5)
    editor.add_keywords({"Egret"}, {"Birds|Egret"})
    editor.set_pick_flag("flagged")
    assert editor.commit() is True

    text = path.read_text(encoding="utf-8")
    assert text.startswith(
        '<?xpacket begin="﻿" id="W5M0MpCehiHzreSzNTczkc9d"?>\n'
        "<!-- Lightroom Classic 14.0 -->\n"
        '<x:xmpmeta xmlns:x="adobe:ns:meta/"',
    )
    assert text.endswith('</x:xmpmeta>\n<?xpacket end="w"?>')
    # Lightroom writes no XML declaration; the rewrite adds none.
    assert "<?xml" not in text
    assert "<!-- develop history -->" in text
    assert "<!-- added by hand -->" in text
    for spelling in LIGHTROOM_PREFIXES:
        assert spelling in text
    assert "ns0" not in text and "ns1" not in text
    # The namespaces stay declared on the Description, after rdf:about,
    # where Lightroom put them -- not hoisted to the root.
    assert '<rdf:Description rdf:about="" xmlns:xmp=' in text
    # The one namespace the edit introduced is declared beside them, with
    # Lightroom's prefix for it.
    assert 'xmlns:xmpDM="http://ns.adobe.com/xmp/1.0/DynamicMedia/"' in text
    assert 'xmpDM:pick="1"' in text

    root = ET.parse(path).getroot()
    assert _untouched_by_rating_and_keywords(root) == before
    assert read_keywords(str(path)) == {"Heron", "Marsh", "Egret"}
    assert set(read_hierarchical_keywords(str(path))) == {"Birds|Heron", "Birds|Egret"}
    assert read_sync_preview_metadata(str(path))["rating"] == "5"


def test_comment_survives_removal_of_the_sibling_it_followed(tmp_path):
    """A comment keeps its place relative to the siblings that remain."""
    path = _lightroom_sidecar(tmp_path)

    remove_keywords(str(path), {"Heron"}, hierarchical=False)

    text = path.read_text(encoding="utf-8")
    assert read_keywords(str(path)) == {"Marsh"}
    bag = text[text.index("<dc:subject>"):text.index("</dc:subject>")]
    assert bag.index("<!-- added by hand -->") < bag.index("<rdf:li>Marsh</rdf:li>")
    assert "<rdf:li>Heron</rdf:li>" not in bag


def test_comment_adjacent_to_character_data_keeps_its_place(tmp_path):
    """A comment in a text slot stays where the source had it.

    ``TreeBuilder`` coalesces character data around a comment into one
    ``elem.text`` or child ``tail`` string, so the serializer must remember
    where in that string the comment sat -- not just which element siblings
    preceded it.
    """
    path = tmp_path / "photo.xmp"
    path.write_text(
        '<x:xmpmeta xmlns:x="adobe:ns:meta/">'
        f'<rdf:RDF xmlns:rdf="{NS_RDF}">'
        '<rdf:Description rdf:about="" xmlns:dc="http://purl.org/dc/elements/1.1/">'
        "<dc:subject><rdf:Bag>"
        "<rdf:li><!--before-->Heron</rdf:li>"
        "<rdf:li>Egret<!--after--></rdf:li>"
        "<rdf:li>Hi<!--middle-->There</rdf:li>"
        "</rdf:Bag></dc:subject>"
        "</rdf:Description></rdf:RDF></x:xmpmeta>",
        encoding="utf-8",
    )

    write_rating(str(path), 2)

    text = path.read_text(encoding="utf-8")
    assert "<rdf:li><!--before-->Heron</rdf:li>" in text
    assert "<rdf:li>Egret<!--after--></rdf:li>" in text
    assert "<rdf:li>Hi<!--middle-->There</rdf:li>" in text
    assert read_keywords(str(path)) == {"Heron", "Egret", "HiThere"}


def test_rewrite_is_stable(tmp_path):
    """A second edit changes only what it edits: the markup doesn't drift."""
    path = _lightroom_sidecar(tmp_path)
    write_rating(str(path), 4)
    first = path.read_text(encoding="utf-8")
    write_rating(str(path), 3)
    second = path.read_text(encoding="utf-8")
    assert second == first.replace('xmp:Rating="4"', 'xmp:Rating="3"')


def test_new_namespace_does_not_capture_a_prefix_the_sidecar_uses_elsewhere(tmp_path):
    """A prefix the sidecar binds to another URI is not reused for Vireo's.

    Here ``vireo`` names an unrelated namespace. Declaring Vireo's own
    namespace with the same prefix on the Description would rebind the
    other tool's ``vireo:`` elements beneath it.
    """
    other = "urn:example:not-vireo"
    path = tmp_path / "photo.xmp"
    path.write_text(
        f'<x:xmpmeta xmlns:x="adobe:ns:meta/" xmlns:vireo="{other}">'
        f'<rdf:RDF xmlns:rdf="{NS_RDF}">'
        '<rdf:Description rdf:about="">'
        "<vireo:Note>theirs</vireo:Note>"
        "</rdf:Description></rdf:RDF></x:xmpmeta>",
        encoding="utf-8",
    )

    assert write_edit_recipe(str(path), '{"exposure": 1}') is True

    root = ET.parse(path).getroot()
    desc = root.find(f"{{{NS_RDF}}}RDF/{{{NS_RDF}}}Description")
    assert desc.findtext(f"{{{other}}}Note") == "theirs"
    assert desc.get(f"{{{NS_VIREO}}}editRecipe") == '{"exposure": 1}'
    text = path.read_text(encoding="utf-8")
    assert f'xmlns:vireo="{other}"' in text
    assert "<vireo:Note>theirs</vireo:Note>" in text


def test_default_namespace_sidecar_round_trips(tmp_path):
    """A sidecar using a default namespace stays well-formed and readable."""
    path = tmp_path / "photo.xmp"
    path.write_text(
        '<x:xmpmeta xmlns:x="adobe:ns:meta/">'
        f'<rdf:RDF xmlns:rdf="{NS_RDF}">'
        '<rdf:Description rdf:about="" xmlns="http://purl.org/dc/elements/1.1/">'
        "<subject><rdf:Bag><rdf:li>Heron</rdf:li></rdf:Bag></subject>"
        "</rdf:Description></rdf:RDF></x:xmpmeta>",
        encoding="utf-8",
    )

    write_rating(str(path), 2)

    text = path.read_text(encoding="utf-8")
    assert "<subject>" in text
    assert read_keywords(str(path)) == {"Heron"}
    assert read_sync_preview_metadata(str(path))["rating"] == "2"


def test_declaration_naming_another_encoding_is_replaced(tmp_path):
    """The rewrite is UTF-8, so it must not keep a Latin-1 declaration."""
    path = tmp_path / "photo.xmp"
    path.write_bytes(
        b'<?xml version="1.0" encoding="ISO-8859-1"?>\n'
        b'<x:xmpmeta xmlns:x="adobe:ns:meta/">'
        b'<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">'
        b'<rdf:Description rdf:about="" xmlns:dc="http://purl.org/dc/elements/1.1/">'
        b"<dc:subject><rdf:Bag><rdf:li>Gr\xe8be</rdf:li></rdf:Bag></dc:subject>"
        b"</rdf:Description></rdf:RDF></x:xmpmeta>",
    )

    write_sidecar(str(path), {"Héron"}, set())

    data = path.read_bytes()
    assert data.startswith(b"<?xml version='1.0' encoding='utf-8'?>\n")
    assert read_keywords(str(path)) == {"Grèbe", "Héron"}


def test_declaration_is_kept_verbatim(tmp_path):
    """digiKam's and ExifTool's declarations come back as they were."""
    path = tmp_path / "photo.xmp"
    path.write_text(
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<x:xmpmeta xmlns:x="adobe:ns:meta/">'
        f'<rdf:RDF xmlns:rdf="{NS_RDF}"><rdf:Description rdf:about=""/></rdf:RDF>'
        "</x:xmpmeta>",
        encoding="utf-8",
    )
    write_rating(str(path), 1)
    assert path.read_text(encoding="utf-8").startswith(
        '<?xml version="1.0" encoding="UTF-8"?>\n<x:xmpmeta',
    )


def test_new_sidecar_declares_namespaces_like_a_standard_writer(tmp_path):
    """Adobe's toolkit, ExifTool and Exiv2 declare ``x`` on ``x:xmpmeta``,
    ``rdf`` on ``rdf:RDF`` and each schema on the Description using it."""
    path = tmp_path / "photo.xmp"

    write_sidecar(str(path), {"Heron"}, {"Birds|Heron"})

    lines = path.read_text(encoding="utf-8").splitlines()
    assert lines[0] == "<?xml version='1.0' encoding='utf-8'?>"
    assert lines[1] == '<x:xmpmeta xmlns:x="adobe:ns:meta/">'
    assert lines[2] == f'  <rdf:RDF xmlns:rdf="{NS_RDF}">'
    assert lines[3] == (
        '    <rdf:Description xmlns:dc="http://purl.org/dc/elements/1.1/"'
        ' xmlns:lr="http://ns.adobe.com/lightroom/1.0/">'
    )
    assert read_keywords(str(path)) == {"Heron"}


def test_rewrites_leave_elementtree_prefix_registry_alone(tmp_path):
    """Prefixes are chosen per document, never registered process-wide.

    ``ET.register_namespace`` is global: registering a sidecar's prefixes
    while the sync pool writes several sidecars at once would change the
    prefixes another thread's sidecar is written with.
    """
    # ElementTree keeps its prefix registry in this private module global.
    registry = dict(ET._namespace_map)
    paths = []
    for index in range(4):
        path = tmp_path / f"photo{index}.xmp"
        path.write_text(
            LIGHTROOM_XMP.replace("digiKam", f"tool{index}"), encoding="utf-8",
        )
        paths.append(path)

    for path in paths:
        write_rating(str(path), 1)

    assert dict(ET._namespace_map) == registry
    for index, path in enumerate(paths):
        assert f"tool{index}:TagsList" in path.read_text(encoding="utf-8")


def test_duplicate_bag_merge_keeps_a_locally_declared_prefix_on_the_copy(tmp_path):
    """A local xmlns on an rdf:li survives being deep-copied during a merge.

    When ``_merge_duplicate_bags`` folds two unqualified ``dc:subject``
    bags together, it deep-copies each item it keeps. ``SourceMarkup``
    records per-element namespace declarations by element identity, so
    the copy must inherit the original's recorded binding; otherwise
    the ``<q:value>`` child inside it would fall back to ``ns0`` on
    rewrite, dropping the sidecar's own prefix.

    The marked item has to live in the later (``extra``) bag so
    ``_bag_target_property`` picks the plain-``Egret`` bag as the merge
    target, forcing the marked item through ``_copy_with_source_markup``:
    otherwise reverting that call to ``copy.deepcopy`` would still copy
    only the plain ``Egret`` item and leave this regression unguarded.
    """
    path = tmp_path / "photo.xmp"
    path.write_text(
        '<x:xmpmeta xmlns:x="adobe:ns:meta/">'
        f'<rdf:RDF xmlns:rdf="{NS_RDF}">'
        '<rdf:Description rdf:about=""'
        ' xmlns:dc="http://purl.org/dc/elements/1.1/">'
        "<dc:subject><rdf:Bag><rdf:li>Egret</rdf:li></rdf:Bag></dc:subject>"
        "</rdf:Description>"
        '<rdf:Description rdf:about=""'
        ' xmlns:dc="http://purl.org/dc/elements/1.1/">'
        "<dc:subject><rdf:Bag>"
        '<rdf:li xmlns:q="urn:item" rdf:parseType="Resource">'
        "<q:value>Heron</q:value></rdf:li>"
        "</rdf:Bag></dc:subject>"
        "</rdf:Description>"
        "</rdf:RDF></x:xmpmeta>",
        encoding="utf-8",
    )

    editor = SidecarEditor(str(path))
    editor.add_keywords({"Kiwi"}, set())
    assert editor.commit() is True

    text = path.read_text(encoding="utf-8")
    assert "<q:value>Heron</q:value>" in text
    assert "ns0:" not in text
    assert 'xmlns:q="urn:item"' in text
    # The declaration has to stay on the copied ``rdf:li`` where the
    # source recorded it. A plain ``copy.deepcopy`` loses the recorded
    # binding on the copy, and the serializer then has to declare ``q``
    # on an enclosing scope (the ``rdf:Description``), which the
    # ``xmlns:q`` string check above does not detect.
    for description_tag in text.split("<rdf:Description")[1:]:
        assert "xmlns:q" not in description_tag.split(">", 1)[0]
    carrier_tag_open = text[: text.index("<q:value>")].rsplit("<rdf:li", 1)[1]
    assert 'xmlns:q="urn:item"' in carrier_tag_open


def test_duplicate_bag_merge_carries_inner_comments_onto_the_copy(tmp_path):
    """A comment inside an rdf:li survives being deep-copied during a merge.

    ``SourceMarkup.inner`` records inner comments and PIs by element
    identity, so the merge's deep copy must carry those entries onto
    the new element; otherwise an ordinary keyword edit would silently
    drop the comment when the original property is removed.

    The marked item has to live in the later (``extra``) bag so
    ``_bag_target_property`` picks the plain-``Egret`` bag as the merge
    target, forcing the marked item through ``_copy_with_source_markup``:
    otherwise reverting that call to ``copy.deepcopy`` would still copy
    only the plain ``Egret`` item and leave this regression unguarded.
    """
    path = tmp_path / "photo.xmp"
    path.write_text(
        '<x:xmpmeta xmlns:x="adobe:ns:meta/">'
        f'<rdf:RDF xmlns:rdf="{NS_RDF}">'
        '<rdf:Description rdf:about=""'
        ' xmlns:dc="http://purl.org/dc/elements/1.1/">'
        "<dc:subject><rdf:Bag><rdf:li>Egret</rdf:li></rdf:Bag></dc:subject>"
        "</rdf:Description>"
        '<rdf:Description rdf:about=""'
        ' xmlns:dc="http://purl.org/dc/elements/1.1/">'
        "<dc:subject><rdf:Bag>"
        '<rdf:li rdf:parseType="Resource">'
        "<rdf:value>Heron</rdf:value><!-- source: Lightroom --></rdf:li>"
        "</rdf:Bag></dc:subject>"
        "</rdf:Description>"
        "</rdf:RDF></x:xmpmeta>",
        encoding="utf-8",
    )

    editor = SidecarEditor(str(path))
    editor.add_keywords({"Kiwi"}, set())
    assert editor.commit() is True

    text = path.read_text(encoding="utf-8")
    assert "<!-- source: Lightroom -->" in text
    assert read_keywords(str(path)) == {"Heron", "Egret", "Kiwi"}
