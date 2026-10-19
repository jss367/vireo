"""In-memory XML and selected readers/callers only; never sidecar or catalog I/O."""
import __future__

import ast
import contextlib
import copy
import re
import unicodedata
import urllib.parse
import weakref
from pathlib import Path
from types import SimpleNamespace
from xml.etree import ElementTree as ET

VIREO = Path(__file__).resolve().parents[1]


def _execute(nodes, namespace):
    exec(compile(ast.Module(body=nodes, type_ignores=[]), "<in-memory keyword readers>", "exec", flags=__future__.annotations.compiler_flag), namespace)


def _namespace(*, ownership="hier", failure_at=None, initial_error=None):
    calls = []
    imported = []
    ns = {"contextlib": contextlib, "urllib": urllib, "weakref": weakref,
          "re": re, "unicodedata": unicodedata,
          "log": SimpleNamespace(warning=lambda *args: None)}
    for name in ("xmp/namespaces.py", "keyword_normalization.py", "xmp/rdf.py", "xmp/photo_scope.py", "xmp/location_marker.py"):
        tree = ast.parse((VIREO / name).read_text(encoding="utf-8"))
        # Keep pure constants/definitions, excluding imports and namespace registration.
        nodes = [node for node in tree.body if isinstance(node, (ast.Assign, ast.FunctionDef))]
        _execute(nodes, ns)
    root = ET.fromstring(
        f'<rdf:RDF xmlns:rdf="{ns["NS_RDF"]}" xmlns:dc="{ns["NS_DC"]}"'
        f' xmlns:lr="{ns["NS_LR"]}" xmlns:vireo="{ns["NS_VIREO"]}">'
        '<rdf:Description rdf:about="" vireo:locationKeywords="World|Paris">'
        '<dc:subject><rdf:Bag><rdf:li>Paris</rdf:li></rdf:Bag></dc:subject>'
        '<lr:hierarchicalSubject><rdf:Bag><rdf:li>World|Paris</rdf:li></rdf:Bag></lr:hierarchicalSubject>'
        '</rdf:Description></rdf:RDF>'
    )
    if ownership is not None:
        root[0].set(ns["LOCATION_KEYWORDS_OWNED"], ownership)

    def parse(path):
        assert path == "invented-memory://photo.xmp"
        calls.append(path)
        if initial_error:
            raise initial_error
        if failure_at is not None and len(calls) >= failure_at:
            raise OSError("invented later ownership-read failure")
        return ET.ElementTree(copy.deepcopy(root))

    ns["ET"] = SimpleNamespace(parse=parse, ParseError=ET.ParseError)
    readers = ast.parse((VIREO / "xmp/readers.py").read_text(encoding="utf-8"))
    _execute([node for node in readers.body if isinstance(node, ast.FunctionDef)], ns)
    # A memory tree has no document retrieval URI to register.
    ns["_register_document_uri"] = lambda *args: None
    for name, functions in (
        ("keyword_identity.py", {"drop_stale_vireo_location_keywords"}),
        ("sync.py", {"_read_sidecar_keywords"}),
        ("scanner/keywords.py", {"_import_keywords_for_photo"}),
    ):
        tree = ast.parse((VIREO / name).read_text(encoding="utf-8"))
        _execute([node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in functions], ns)
    ns["filter_removed_import_aliases"] = lambda db, pid, flat, hier, *args: (flat, hier)
    ns["_import_keyword_lists"] = lambda db, pid, flat, hier: imported.append((set(flat), list(hier)))
    db = SimpleNamespace(
        locations=SimpleNamespace(has_pending_change=lambda photo_id: True),
        pending_changes=SimpleNamespace(keyword_removal_keys=lambda *args, **kwargs: set()),
    )
    return ns, db, calls, imported


def test_sync_keyword_boundary_preserves_user_flat_when_later_ownership_read_would_fail():
    ns, db, calls, _ = _namespace(failure_at=3)
    result = ns["_read_sidecar_keywords"](db, 731, "invented-memory://photo.xmp")
    assert result[0] == {"Paris"}
    assert result[1] == {"paris": "Paris"}
    assert result[2] == []
    assert len(calls) == 1


def test_scanner_keyword_boundary_preserves_user_flat_when_later_ownership_read_would_fail():
    ns, db, calls, imported = _namespace(failure_at=4)
    ns["_import_keywords_for_photo"](db, 731, "invented-memory://photo.xmp")
    assert imported == [({"Paris"}, [])]
    assert len(calls) == 1


def test_keyword_snapshot_distinguishes_real_legacy_absence_from_owned_entries():
    for owned, expected_flat, expected_hier in (
        ("hier", {"Paris"}, []), ("flat", set(), ["World|Paris"]),
        ("flat,hier", set(), []), ("", {"Paris"}, ["World|Paris"]),
        (None, set(), []),
    ):
        ns, db, calls, _ = _namespace(ownership=owned)
        result = ns["_read_sidecar_keywords"](db, 731, "invented-memory://photo.xmp")
        assert result[0] == expected_flat and result[2] == expected_hier
        assert len(calls) == 1


def test_initial_read_failure_skips_both_boundaries_and_keeps_reader_empty_results():
    for error in (FileNotFoundError("invented missing"), PermissionError("invented unreadable"), ET.ParseError("invented corrupt")):
        ns, db, _, imported = _namespace(initial_error=error)
        assert ns["read_keywords"]("invented-memory://photo.xmp") == set()
        assert ns["read_hierarchical_keywords"]("invented-memory://photo.xmp") == []
        assert ns["read_keyword_lists"]("invented-memory://photo.xmp") is None
        assert ns["read_keyword_snapshot"]("invented-memory://photo.xmp") is None
        assert ns["read_vireo_location_keywords"]("invented-memory://photo.xmp") is None
        assert ns["read_vireo_location_keywords_owned"]("invented-memory://photo.xmp") is None
        assert ns["_read_sidecar_keywords"](db, 731, "invented-memory://photo.xmp") is None
        ns["_import_keywords_for_photo"](db, 731, "invented-memory://photo.xmp")
        assert imported == []


def test_direct_stale_filter_does_not_confuse_unreadable_ownership_with_legacy_absence():
    ns, db, _, _ = _namespace(initial_error=OSError("invented unreadable"))
    try:
        ns["drop_stale_vireo_location_keywords"](db, 731, "invented-memory://photo.xmp", {"Paris"}, ["World|Paris"])
    except OSError:
        pass
    else:
        raise AssertionError("A failed ownership read is not evidence of legacy ownership")


def test_no_pending_location_change_needs_no_marker_read():
    ns, db, calls, _ = _namespace(initial_error=OSError("must not read"))
    db.locations.has_pending_change = lambda photo_id: False
    result = ns["drop_stale_vireo_location_keywords"](db, 731, "invented-memory://photo.xmp", {"Paris"}, ["World|Paris"])
    assert result == ({"Paris"}, ["World|Paris"])
    assert calls == []


def test_snapshot_keeps_the_existing_two_list_reader_contract():
    ns, _, calls, _ = _namespace()
    snapshot = ns["read_keyword_snapshot"]("invented-memory://photo.xmp")
    assert snapshot == ({"Paris"}, ["World|Paris"], "World|Paris", "hier")
    assert len(calls) == 1
    assert ns["read_keyword_lists"]("invented-memory://photo.xmp") == snapshot[:2]
    assert len(calls) == 2


def test_readable_empty_keywords_are_distinct_from_an_unreadable_sidecar():
    ns, db, _, imported = _namespace()
    empty = ET.fromstring(f'<rdf:RDF xmlns:rdf="{ns["NS_RDF"]}"><rdf:Description rdf:about=""/></rdf:RDF>')
    ns["ET"].parse = lambda path: ET.ElementTree(copy.deepcopy(empty))
    assert ns["read_keyword_snapshot"]("invented-memory://photo.xmp") == (set(), [], None, None)
    assert ns["read_keyword_lists"]("invented-memory://photo.xmp") == (set(), [])
    assert ns["_read_sidecar_keywords"](db, 731, "invented-memory://photo.xmp") == (set(), {}, [], set(), set())
    ns["_import_keywords_for_photo"](db, 731, "invented-memory://photo.xmp")
    assert imported == [(set(), [])]
