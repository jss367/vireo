"""In-memory XML and selected readers/callers only; never sidecar or catalog I/O."""
import __future__

import ast
import contextlib
import copy
import re
import sqlite3
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
    assert ns["_import_keywords_for_photo"](db, 731, "invented-memory://photo.xmp") is True
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
        assert ns["_import_keywords_for_photo"](db, 731, "invented-memory://photo.xmp") is False
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


def _scanner_method(namespace, file, class_name, name):
    tree = ast.parse((VIREO / "scanner" / file).read_text(encoding="utf-8"))
    cls = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == class_name)
    method = next(node for node in cls.body if isinstance(node, ast.FunctionDef) and node.name == name)
    _execute([method], namespace)
    return namespace[name]


def _incremental_boundary(namespace, db):
    existing = {
        "id": 731, "file_mtime": 1, "file_size": 100, "xmp_mtime": 5,
        "extension": ".jpg", "width": 1200, "timestamp": "invented",
        "exif_extracted": True, "summary_needs_extract": False,
        "file_hash": "invented hash", "folder_id": 17, "filename": "photo.jpg",
    }
    commits = []
    db.scanning = SimpleNamespace(set_xmp_mtime=lambda pid, value: existing.update(xmp_mtime=value))
    db.commit_with_retry = lambda: commits.append("commit")
    db.check_and_resolve_duplicates_for_hash = lambda file_hash: None
    owner = SimpleNamespace(db=db, repair_missing_metadata=False,
                            tally=SimpleNamespace(credit_reused_row=lambda *args: None))
    namespace.update(RAW_EXTENSIONS=set(), EMPTY_FILE_SHA256="invented empty hash")
    reuse = _scanner_method(namespace, "triage.py", "_IncrementalTriage", "_reuse_existing_row")
    return existing, commits, lambda mtime=9: reuse(
        owner, existing, "invented image", SimpleNamespace(st_mtime=1, st_size=100),
        "invented-memory://photo.xmp", mtime,
    )


def test_incremental_sidecar_failure_retries_after_same_mtime_recovery():
    ns, db, _, imported = _namespace()
    parse = ns["ET"].parse
    inaccessible = [True]
    attempts = []

    def transient_parse(path):
        attempts.append(path)
        if inaccessible[0]:
            raise OSError("invented transient sidecar failure")
        return parse(path)

    ns["ET"].parse = transient_parse
    row, commits, reuse = _incremental_boundary(ns, db)
    assert reuse() is True
    first_stamp = row["xmp_mtime"]
    inaccessible[0] = False
    assert reuse() is True
    assert reuse() is True
    assert first_stamp == 5
    assert row["xmp_mtime"] == 9 and commits == ["commit"]
    assert len(attempts) == 2 and imported == [({"Paris"}, [])]


def test_readable_empty_sidecar_is_acknowledged_and_not_retried():
    ns, db, _, imported = _namespace()
    attempts = []
    empty = ET.fromstring(f'<rdf:RDF xmlns:rdf="{ns["NS_RDF"]}"><rdf:Description rdf:about=""/></rdf:RDF>')

    def parse(path):
        attempts.append(path)
        return ET.ElementTree(copy.deepcopy(empty))

    ns["ET"].parse = parse
    row, commits, reuse = _incremental_boundary(ns, db)
    assert reuse() is True and reuse() is True
    assert row["xmp_mtime"] == 9 and commits == ["commit"]
    assert len(attempts) == 1 and imported == [(set(), [])]


def test_deleted_sidecar_still_clears_its_previous_acknowledgement():
    ns, db, calls, _ = _namespace()
    row, commits, reuse = _incremental_boundary(ns, db)
    assert reuse(None) is True and reuse(None) is True
    assert row["xmp_mtime"] is None and commits == ["commit"] and calls == []


class _MemorySidecar:
    def exists(self):
        return True

    def __str__(self):
        return "invented-memory://photo.xmp"


def _catalog_boundary(namespace, db, *, existing, file_hash="invented hash", xmp_mtime=9):
    row = {"exists": existing, "xmp_mtime": 5 if existing else None,
           "file_mtime": 5, "file_size": 50}
    events = []

    def add_photo(**values):
        inserted = not row["exists"]
        if inserted:
            row.update(exists=True, xmp_mtime=values["xmp_mtime"],
                       file_mtime=values["file_mtime"], file_size=values["file_size"])
        return 731, inserted

    def set_mtime(photo_id, value):
        events.append("acknowledged")
        row["xmp_mtime"] = value

    def update_photo(photo_id, assignments, values):
        events.append("photo-update")
        bindings = iter(values)
        for assignment in assignments:
            if assignment == "xmp_mtime=NULL":
                row["xmp_mtime"] = None
            else:
                value = next(bindings)
                if assignment in ("xmp_mtime=?", "file_mtime=?", "file_size=?", "file_hash=?"):
                    row[assignment.removesuffix("=?")] = value
        assert list(bindings) == []

    db.add_photo = add_photo
    db._commits_held = contextlib.nullcontext
    db.commit_with_retry = lambda: events.append("commit")
    db.scanning = SimpleNamespace(set_xmp_mtime=set_mtime, update_photo=update_photo)
    db.check_and_resolve_duplicates_for_hash = lambda file_hash: None
    owner = SimpleNamespace(
        db=db, vireo_dir=None, extract_full_metadata=False,
        collections=SimpleNamespace(drop_inherited_collection_membership=lambda photo_id: None),
        tally=SimpleNamespace(credit_indexed=lambda photo_id: None, report_cataloged=lambda *args: None),
        fills=SimpleNamespace(take_pending_companion_fill=lambda *args: None),
        _identity_column_updates=lambda item: (["file_hash=?"], [item.file_hash]) if item.file_hash is not None else ([], []),
        _heal_changed_content=lambda *args: None,
    )
    for name in ("_add_photo", "_write_photo_columns"):
        method = _scanner_method(namespace, "indexing.py", "_FileIndexer", name)
        setattr(owner, name, lambda *args, method=method: method(owner, *args))
    namespace["_import_embedded_keywords_for_photo"] = lambda *args: None
    catalog = _scanner_method(namespace, "indexing.py", "_FileIndexer", "catalog_file")
    item = SimpleNamespace(
        image_path=SimpleNamespace(name="photo.jpg", suffix=".jpg"),
        xmp_path=_MemorySidecar(), folder_id=17, file_mtime=1, file_size=100,
        file_hash=file_hash, xmp_mtime=xmp_mtime, row_already_existed=existing,
        meta=SimpleNamespace(timestamp=None, width=None, height=None, file_meta={}),
    )
    return row, events, lambda: catalog(owner, item)


def test_full_catalog_boundary_only_acknowledges_successful_keyword_reads():
    for existing in (False, True):
        ns, db, _, imported = _namespace()
        parse = ns["ET"].parse
        inaccessible = [True]

        def transient_parse(path, inaccessible=inaccessible, parse=parse):
            if inaccessible[0]:
                raise OSError("invented transient read failure")
            return parse(path)

        ns["ET"].parse = transient_parse
        row, events, catalog = _catalog_boundary(ns, db, existing=existing)
        catalog()
        first_stamp = row["xmp_mtime"]
        assert "acknowledged" not in events
        inaccessible[0] = False
        catalog()
        assert first_stamp == (5 if existing else None)
        assert row["xmp_mtime"] == 9 and events.count("acknowledged") == 1
        assert imported == [({"Paris"}, [])]


def test_keyword_import_errors_and_cancellation_propagate_without_acknowledgement():
    class Cancelled(RuntimeError):
        pass

    for error in (ValueError("invented import error"), Cancelled("invented cancellation"), KeyboardInterrupt()):
        for boundary in ("incremental", "catalog"):
            ns, db, _, _ = _namespace()

            def fail(*args, error=error):
                raise error

            ns["_import_keyword_lists"] = fail
            if boundary == "incremental":
                row, _, invoke = _incremental_boundary(ns, db)
            else:
                row, _, invoke = _catalog_boundary(ns, db, existing=True)
            try:
                invoke()
            except BaseException as caught:
                assert caught is error
            else:
                raise AssertionError("Original import error must propagate")
            assert row["xmp_mtime"] == 5


def test_companion_boundary_does_not_commit_a_failed_read_as_imported():
    ns, db, _, imported = _namespace()
    parse = ns["ET"].parse
    inaccessible = [True]
    commits = []

    def transient_parse(path):
        if inaccessible[0]:
            raise OSError("invented transient read failure")
        return parse(path)

    ns["ET"].parse = transient_parse
    db.commit_with_retry = lambda: commits.append("commit")
    image_path = SimpleNamespace(with_suffix=lambda suffix: _MemorySidecar())
    owner = SimpleNamespace(db=db)
    companion = _scanner_method(ns, "companion_keep.py", "_KnownCompanionKeeper", "import_companion_sidecar")
    companion(owner, image_path, {"owner_id": 731})
    first_commits = list(commits)
    inaccessible[0] = False
    companion(owner, image_path, {"owner_id": 731})
    assert first_commits == [] and commits == ["commit"]
    assert imported == [({"Paris"}, [])]


def test_full_catalog_empty_sidecar_acknowledgement_does_not_require_an_image_hash():
    ns, db, _, imported = _namespace()
    empty = ET.fromstring(f'<rdf:RDF xmlns:rdf="{ns["NS_RDF"]}"><rdf:Description rdf:about=""/></rdf:RDF>')
    ns["ET"].parse = lambda path: ET.ElementTree(copy.deepcopy(empty))
    row, _, catalog = _catalog_boundary(ns, db, existing=True, file_hash=None)
    catalog()
    assert row["xmp_mtime"] == 9 and imported == [(set(), [])]


def test_full_catalog_clears_a_deleted_sidecar_stamp_without_importing():
    ns, db, calls, imported = _namespace()
    row, _, catalog = _catalog_boundary(ns, db, existing=True, xmp_mtime=None)
    catalog()
    assert row["xmp_mtime"] is None and calls == [] and imported == []


def test_no_sidecar_reuses_photo_update_without_an_extra_transaction():
    for prior_stamp in (None, 5):
        ns, db, calls, imported = _namespace()
        row, events, catalog = _catalog_boundary(ns, db, existing=True, xmp_mtime=None)
        row["xmp_mtime"] = prior_stamp
        catalog()
        assert row["xmp_mtime"] is None
        assert events == ["photo-update", "commit"]
        assert calls == [] and imported == []


def test_deleted_sidecar_clears_stamp_when_nonempty_image_hash_is_unavailable():
    ns, db, calls, imported = _namespace()
    row, events, catalog = _catalog_boundary(
        ns, db, existing=True, file_hash=None, xmp_mtime=None,
    )
    catalog()
    assert row["xmp_mtime"] is None
    assert (row["file_mtime"], row["file_size"]) == (5, 50)
    assert events == ["photo-update", "commit"]
    assert calls == [] and imported == []


def test_catalog_read_success_and_failure_keep_ownership_and_acknowledgement_boundaries():
    for error in (None, PermissionError("invented unreadable sidecar"), ET.ParseError("invented malformed sidecar")):
        ns, db, calls, imported = _namespace(initial_error=error)
        row, events, catalog = _catalog_boundary(ns, db, existing=True)
        catalog()
        assert len(calls) == 1
        assert events.count("photo-update") == 1
        assert events.count("commit") == (2 if error is None else 1)
        assert row["xmp_mtime"] == (9 if error is None else 5)
        assert imported == ([({"Paris"}, [])] if error is None else [])
        assert events.count("acknowledged") == (1 if error is None else 0)


def test_failed_catalog_read_defers_duplicate_resolution_until_the_retry():
    ns, db, _, imported = _namespace()
    parse = ns["ET"].parse
    unreadable = [True]
    resolved = []

    def transient_parse(path):
        if unreadable[0]:
            raise PermissionError("invented temporary failure")
        return parse(path)

    ns["ET"].parse = transient_parse
    full_row, _, catalog = _catalog_boundary(ns, db, existing=False)
    db.check_and_resolve_duplicates_for_hash = lambda file_hash: resolved.append(file_hash)
    catalog()
    assert resolved == [] and imported == []
    row, _, reuse = _incremental_boundary(ns, db)
    row["xmp_mtime"] = None
    row["file_hash"] = "invented hash"
    db.check_and_resolve_duplicates_for_hash = lambda file_hash: resolved.append(file_hash)
    unreadable[0] = False
    assert reuse(9) is True
    assert resolved == ["invented hash"] and imported == [({"Paris"}, [])]


def test_sidecarless_catalog_still_runs_duplicate_resolution():
    ns, db, _, _ = _namespace()
    resolved = []
    row, _, catalog = _catalog_boundary(ns, db, existing=False, xmp_mtime=None)
    db.check_and_resolve_duplicates_for_hash = lambda file_hash: resolved.append(file_hash)
    catalog()
    assert resolved == ["invented hash"]


def test_incremental_retry_without_a_prior_null_mtime_does_not_rerun_duplicate_resolution():
    ns, db, _, imported = _namespace()
    resolved = []
    row, _, reuse = _incremental_boundary(ns, db)
    # Simulate a legitimate Lightroom edit: stored mtime is 5, incoming is 9.
    db.check_and_resolve_duplicates_for_hash = lambda file_hash: resolved.append(file_hash)
    assert reuse(9) is True
    assert resolved == [] and imported == [({"Paris"}, [])]


def test_failed_catalog_read_retries_incrementally_without_a_sidecar_mtime_change():
    ns, db, _, imported = _namespace()
    parse = ns["ET"].parse
    unreadable = [True]
    attempts = []

    def transient_parse(path):
        attempts.append(path)
        if unreadable[0]:
            raise PermissionError("invented temporary failure")
        return parse(path)

    ns["ET"].parse = transient_parse
    full_row, _, catalog = _catalog_boundary(ns, db, existing=True, file_hash=None)
    catalog()
    assert full_row["xmp_mtime"] == 5 and not imported
    row, commits, reuse = _incremental_boundary(ns, db)
    row["xmp_mtime"] = full_row["xmp_mtime"]
    unreadable[0] = False
    assert reuse(9) is True and reuse(9) is True
    assert row["xmp_mtime"] == 9 and commits == ["commit"]
    assert len(attempts) == 2 and imported == [({"Paris"}, [])]


def test_literal_null_assignment_and_bound_columns_use_one_invented_sqlite_update():
    tree = ast.parse((VIREO / "repositories" / "scanning.py").read_text(encoding="utf-8"))
    node = next(node for node in ast.walk(tree)
                if isinstance(node, ast.FunctionDef) and node.name == "update_photo")
    namespace = {}
    _execute([node], namespace)
    with contextlib.closing(sqlite3.connect(":memory:")) as conn:
        conn.execute("CREATE TABLE photos(id INTEGER PRIMARY KEY, xmp_mtime REAL, file_mtime REAL)")
        conn.execute("INSERT INTO photos VALUES(731, 5, 5)")
        conn.commit()
        trace = []
        conn.set_trace_callback(trace.append)
        namespace["update_photo"](
            SimpleNamespace(conn=conn), 731, ["file_mtime=?", "xmp_mtime=NULL"], [9],
        )
        assert conn.execute("SELECT xmp_mtime, file_mtime FROM photos").fetchone() == (None, 9)
        assert sum(sql.startswith("UPDATE ") for sql in trace) == 1
        assert "COMMIT" not in trace
