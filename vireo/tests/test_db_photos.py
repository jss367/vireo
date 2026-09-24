"""Behavior pins for the photos domain of ``Database``.

The tests exercise the core photo-row methods only through the public
``Database`` façade, so they hold whether the SQL lives in ``db.py`` or in
``repositories/photos.py``: adding and reading photo rows, the id/path
lookups, the workspace-scoped listing, paging, position, count, calendar
and browse-summary reads, companion counts, deletion (with its commit,
rollback, cache-invalidation and pipeline-prune boundaries), and the
sharpness/quality writers.
"""

import ast
import contextlib
import inspect
import json
import sqlite3
import textwrap

import config as cfg
import pytest
from db import _SQLITE_PARAM_CHUNK_SIZE, Database


@pytest.fixture
def isolated_config(tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))


def _reader(db):
    """A second connection: sees only what the façade has committed."""
    conn = sqlite3.connect(db._db_path)
    conn.row_factory = sqlite3.Row
    return contextlib.closing(conn)


def _photo(db, folder_id, filename, **kw):
    kw.setdefault("extension", "." + filename.rsplit(".", 1)[-1])
    kw.setdefault("file_size", 100)
    kw.setdefault("file_mtime", 1.0)
    return db.add_photo(folder_id=folder_id, filename=filename, **kw)


def _trace(db):
    statements = []
    db.conn.set_trace_callback(statements.append)
    return statements


def _sql(statements, needle):
    return [s.strip() for s in statements if needle in s]


class _RecordingCache:
    def __init__(self):
        self.invalidated = []

    def invalidate_workspaces(self, db_path, workspace_ids):
        self.invalidated.append((db_path, set(workspace_ids)))


@pytest.fixture
def cache(db, monkeypatch):
    recorder = _RecordingCache()
    monkeypatch.setattr(db, "_new_images_cache", recorder)
    return recorder


@pytest.fixture
def lib(db, tmp_path):
    """A workspace with two visible folders and one foreign folder."""
    root = db.add_folder(str(tmp_path / "lib"), name="lib")
    child = db.add_folder(str(tmp_path / "lib" / "c"), name="c", parent_id=root)
    other_ws = db.create_workspace("Other")
    foreign = db.add_folder(
        str(tmp_path / "foreign"), name="foreign", link_to_workspace=False,
    )
    db.add_workspace_folder(other_ws, foreign)
    a = _photo(db, root, "a.jpg", timestamp="2024-01-05T10:00:00")
    b = _photo(db, root, "b.jpg", timestamp="2024-03-07T10:00:00")
    c = _photo(db, child, "c.jpg", timestamp="2023-06-01T10:00:00")
    f = _photo(db, foreign, "f.jpg", timestamp="2022-01-01T10:00:00")
    return {
        "root": root, "child": child, "foreign": foreign, "other_ws": other_ws,
        "a": a, "b": b, "c": c, "f": f,
    }


# -- add_photo ---------------------------------------------------------------


def test_add_photo_commits_and_returns_new_id(db):
    fid = db.add_folder("/p", name="p")
    pid = _photo(db, fid, "x.jpg", timestamp="2024-01-01", width=10,
                 height=20, xmp_mtime=5.0, file_hash="h1")
    assert not db.conn.in_transaction
    with _reader(db) as r:
        row = r.execute(
            "SELECT folder_id, filename, extension, file_size, file_mtime, "
            "xmp_mtime, timestamp, width, height, file_hash FROM photos "
            "WHERE id = ?", (pid,),
        ).fetchone()
    assert tuple(row) == (fid, "x.jpg", ".jpg", 100, 1.0, 5.0,
                          "2024-01-01", 10, 20, "h1")


def test_add_photo_existing_row_returns_existing_id_without_update(db):
    fid = db.add_folder("/p", name="p")
    pid = _photo(db, fid, "x.jpg", file_size=1)
    again = _photo(db, fid, "x.jpg", file_size=999)
    assert again == pid
    assert db.conn.execute(
        "SELECT file_size FROM photos WHERE id = ?", (pid,)
    ).fetchone()[0] == 1


def test_add_photo_runs_duplicate_resolution_only_with_hash(db, monkeypatch):
    calls = []
    monkeypatch.setattr(
        db, "check_and_resolve_duplicates_for_hash", calls.append,
    )
    fid = db.add_folder("/p", name="p")
    _photo(db, fid, "x.jpg")
    assert calls == []
    _photo(db, fid, "y.jpg", file_hash="abc")
    assert calls == ["abc"]
    # A re-add of an existing row still triggers the hook.
    _photo(db, fid, "y.jpg", file_hash="abc")
    assert calls == ["abc", "abc"]


def test_add_photo_duplicate_hash_rejects_loser(db):
    fid = db.add_folder("/p", name="p")
    first = _photo(db, fid, "x.jpg", file_hash="same")
    second = _photo(db, fid, "y.jpg", file_hash="same")
    flags = dict(db.conn.execute(
        "SELECT id, flag FROM photos WHERE id IN (?, ?)", (first, second),
    ).fetchall())
    assert sorted(v or "none" for v in flags.values()) == ["none", "rejected"]


def test_add_photo_uses_retry_helpers(db, monkeypatch):
    import db as db_module

    seen = []
    real_exec = db_module.execute_with_retry
    real_commit = db_module.commit_with_retry

    def exec_spy(conn, sql, params=()):
        seen.append(("exec", sql.split()[0]))
        return real_exec(conn, sql, params)

    def commit_spy(conn, *a, **kw):
        seen.append(("commit",))
        return real_commit(conn, *a, **kw)

    fid = db.add_folder("/p", name="p")
    monkeypatch.setattr(db_module, "execute_with_retry", exec_spy)
    monkeypatch.setattr(db_module, "commit_with_retry", commit_spy)
    _photo(db, fid, "x.jpg")
    assert seen == [("exec", "INSERT"), ("commit",)]


# -- get_photo -----------------------------------------------------------------


def test_get_photo_returns_detail_columns(db, lib):
    row = db.get_photo(lib["a"])
    assert set(row.keys()) == {
        c.strip() for c in Database.PHOTO_DETAIL_COLS.split(",")
    }
    assert row["filename"] == "a.jpg"
    assert db.get_photo(999999) is None


def test_get_photo_sql_is_byte_identical(db, lib):
    statements = _trace(db)
    db.get_photo(lib["a"])
    db.get_photo(lib["a"], verify_workspace=True)
    db.conn.set_trace_callback(None)
    detail = Database.PHOTO_DETAIL_COLS
    assert statements == [
        f"SELECT {detail} FROM photos WHERE id = {lib['a']}",
        f"""SELECT {detail} FROM photos
                    WHERE id = {lib['a']} AND folder_id IN (
                        SELECT folder_id FROM workspace_folders
                        WHERE workspace_id = {db._active_workspace_id})""",
    ]


def test_get_photo_verify_workspace_scopes_to_active_workspace(db, lib):
    assert db.get_photo(lib["f"])["filename"] == "f.jpg"
    assert db.get_photo(lib["f"], verify_workspace=True) is None
    assert db.get_photo(lib["a"], verify_workspace=True)["filename"] == "a.jpg"


def test_get_photo_needs_workspace_only_when_verifying(db, lib):
    db.set_active_workspace(None)
    assert db.get_photo(lib["a"])["filename"] == "a.jpg"
    with pytest.raises(RuntimeError):
        db.get_photo(lib["a"], verify_workspace=True)


# -- id lookups ------------------------------------------------------------------


def test_get_photo_filenames(db, lib):
    assert db.get_photo_filenames([]) == {}
    assert db.get_photo_filenames(None) == {}
    got = db.get_photo_filenames({lib["a"], lib["c"], 999999})
    assert got == {lib["a"]: (lib["root"], "a.jpg"),
                   lib["c"]: (lib["child"], "c.jpg")}


def test_get_photo_filenames_chunks(db, lib):
    ids = [lib["a"]] + list(range(10**6, 10**6 + _SQLITE_PARAM_CHUNK_SIZE))
    statements = _trace(db)
    got = db.get_photo_filenames(ids)
    db.conn.set_trace_callback(None)
    assert got == {lib["a"]: (lib["root"], "a.jpg")}
    assert len(_sql(statements, "SELECT id, folder_id, filename")) == 2


def test_get_photos_by_ids(db, lib):
    assert db.get_photos_by_ids([]) == {}
    got = db.get_photos_by_ids([lib["a"], lib["b"], 999999])
    assert sorted(got) == sorted([lib["a"], lib["b"]])
    cols = {c.strip() for c in Database.PHOTO_COLS.split(",")}
    assert set(got[lib["a"]].keys()) == cols
    with_exif = db.get_photos_by_ids([lib["a"]], include_exif=True)
    assert set(with_exif[lib["a"]].keys()) == cols | {"exif_data"}


def test_get_photos_by_ids_chunks(db, lib):
    ids = [lib["b"]] + list(range(10**6, 10**6 + _SQLITE_PARAM_CHUNK_SIZE))
    statements = _trace(db)
    got = db.get_photos_by_ids(ids)
    db.conn.set_trace_callback(None)
    assert list(got) == [lib["b"]]
    assert len(_sql(statements, "FROM photos WHERE id IN")) == 2


def test_get_photo_folder_statuses(db, lib):
    assert db.get_photo_folder_statuses([]) == {}
    db.conn.execute(
        "UPDATE folders SET status = 'missing' WHERE id = ?", (lib["child"],),
    )
    got = db.get_photo_folder_statuses(
        [lib["a"], lib["c"], lib["a"], 999999]
    )
    assert got == {lib["a"]: "ok", lib["c"]: "missing"}


def test_get_photo_folder_statuses_dedupes_before_chunking(db, lib):
    ids = [lib["a"]] * (_SQLITE_PARAM_CHUNK_SIZE + 5)
    statements = _trace(db)
    assert db.get_photo_folder_statuses(ids) == {lib["a"]: "ok"}
    db.conn.set_trace_callback(None)
    assert len(_sql(statements, "SELECT p.id, f.status")) == 1


# -- counts ------------------------------------------------------------------


def test_count_photos_excludes_missing_folders_and_other_workspaces(db, lib):
    assert db.count_photos() == 3
    assert db.count_photos_in_workspace() == 3
    db.conn.execute(
        "UPDATE folders SET status = 'missing' WHERE id = ?", (lib["child"],),
    )
    assert db.count_photos() == 2
    assert db.count_photos_in_workspace() == 3
    db.conn.execute(
        "UPDATE folders SET status = 'partial' WHERE id = ?", (lib["child"],),
    )
    assert db.count_photos() == 3


def test_counts_require_active_workspace(db, lib):
    db.set_active_workspace(None)
    for fn in (db.count_photos, db.count_photos_in_workspace):
        with pytest.raises(RuntimeError):
            fn()


# -- photos_by_paths --------------------------------------------------------


def test_photos_by_paths(db, lib, tmp_path):
    assert db.photos_by_paths([]) == {}
    a_path = str(tmp_path / "lib" / "a.jpg")
    c_path = str(tmp_path / "lib" / "c" / "c.jpg")
    f_path = str(tmp_path / "foreign" / "f.jpg")
    new_path = str(tmp_path / "lib" / "new.jpg")
    got = db.photos_by_paths([a_path, c_path, f_path, new_path, a_path])
    # Photos are global, so the foreign-workspace photo is found too.
    assert got == {a_path: lib["a"], c_path: lib["c"], f_path: lib["f"]}


def test_photos_by_paths_batches_filenames_per_directory(db, tmp_path):
    fid = db.add_folder(str(tmp_path / "big"), name="big")
    pid = _photo(db, fid, "keep.jpg")
    paths = [str(tmp_path / "big" / "keep.jpg")] + [
        str(tmp_path / "big" / f"n{i}.jpg") for i in range(805)
    ] + [str(tmp_path / "elsewhere" / "keep.jpg")]
    statements = _trace(db)
    got = db.photos_by_paths(paths)
    db.conn.set_trace_callback(None)
    assert got == {str(tmp_path / "big" / "keep.jpg"): pid}
    # 806 names in one directory -> 2 batches of <=800; one more directory.
    assert len(_sql(statements, "SELECT p.id, p.filename")) == 3


# -- filter_out_wildlife_excluded ---------------------------------------------


def test_filter_out_wildlife_excluded(db, lib):
    assert db.filter_out_wildlife_excluded([]) == []
    assert db.filter_out_wildlife_excluded(None) == []
    db.conn.execute(
        "UPDATE photos SET wildlife_excluded = 1 WHERE id = ?", (lib["b"],),
    )
    ids = [lib["c"], lib["b"], lib["a"], lib["c"]]
    assert db.filter_out_wildlife_excluded(ids) == [lib["c"], lib["a"], lib["c"]]
    # Accepts any iterable; never needs an active workspace.
    db.set_active_workspace(None)
    assert db.filter_out_wildlife_excluded(iter([lib["b"], lib["a"]])) == [lib["a"]]


def test_filter_out_wildlife_excluded_reads_instance_chunk_size(db, lib, monkeypatch):
    monkeypatch.setattr(db, "_FILTER_SUBJECT_CHUNK", 2)
    statements = _trace(db)
    db.filter_out_wildlife_excluded([lib["a"], lib["b"], lib["c"]])
    db.conn.set_trace_callback(None)
    assert len(_sql(statements, "wildlife_excluded = 1")) == 2


# -- get_photos / get_photo_ids / count_filtered_photos ------------------------


def test_get_photos_pages_and_scopes(db, lib):
    rows = db.get_photos()
    assert [r["filename"] for r in rows] == ["c.jpg", "a.jpg", "b.jpg"]
    assert set(rows[0].keys()) == {
        c.strip() for c in Database.PHOTO_COLS.split(",")
    }
    assert [r["filename"] for r in db.get_photos(page=2, per_page=2)] == ["b.jpg"]
    # page < 1 is clamped to the first page.
    assert [r["filename"] for r in db.get_photos(page=0, per_page=1)] == ["c.jpg"]
    assert [r["filename"] for r in db.get_photos(sort="name")] == [
        "a.jpg", "b.jpg", "c.jpg",
    ]
    assert [r["filename"] for r in db.get_photos(folder_id=lib["child"])] == ["c.jpg"]
    assert db.get_photo_ids() == [lib["c"], lib["a"], lib["b"]]
    assert db.count_filtered_photos() == 3


def test_listing_filters_agree(db, lib):
    db.update_photo_rating(lib["a"], 5)
    db.update_photo_rating(lib["b"], 2)
    db.update_photo_flag(lib["b"], "flagged")
    db.set_color_label(lib["a"], "red")
    kid = db.add_keyword("Heron")
    db.tag_photo(lib["a"], kid)
    db.tag_photo(lib["b"], kid)
    db.conn.execute(
        "UPDATE photos SET latitude = 1.0, longitude = 2.0 WHERE id = ?",
        (lib["c"],),
    )
    db.conn.commit()
    cases = [
        ({"rating_min": 3}, [lib["a"]]),
        ({"date_from": "2024-01-01"}, [lib["a"], lib["b"]]),
        ({"date_to": "2024-01-05"}, [lib["c"], lib["a"]]),
        ({"flag": "flagged"}, [lib["b"]]),
        ({"flag": "none"}, [lib["c"], lib["a"]]),
        ({"color_label": "red"}, [lib["a"]]),
        ({"keyword": "heron"}, [lib["a"], lib["b"]]),
        ({"keyword": "Heron", "keyword_match_case": True,
          "keyword_whole_word": True}, [lib["a"], lib["b"]]),
        ({"keyword": "   "}, [lib["c"], lib["a"], lib["b"]]),
        ({"location_status": "exif"}, [lib["c"]]),
        ({"folder_id": lib["root"]}, [lib["c"], lib["a"], lib["b"]]),
    ]
    for kwargs, expected in cases:
        assert [r["id"] for r in db.get_photos(**kwargs)] == expected, kwargs
        assert db.get_photo_ids(**kwargs) == expected, kwargs
        assert db.count_filtered_photos(**kwargs) == len(expected), kwargs


def test_listing_keyword_uses_distinct_and_color_join_param_order(db, lib):
    db.set_color_label(lib["a"], "red")
    statements = _trace(db)
    db.get_photos(keyword="x", color_label="red")
    db.get_photo_ids(keyword="x", color_label="red")
    db.get_photos(color_label="red")
    db.conn.set_trace_callback(None)
    selects = [s for s in statements if "FROM photos p" in s]
    assert "SELECT DISTINCT p.id" in selects[0]
    assert "SELECT DISTINCT p.id FROM photos p" in selects[1]
    assert "DISTINCT" not in selects[2].split("FROM")[0]
    ws = db._active_workspace_id
    assert f"pcl.workspace_id = {ws}" in selects[2]


def test_listing_collection_restriction(db, lib):
    cid = db.add_collection(
        "Just A", json.dumps([{"field": "photo_ids", "value": [lib["a"]]}]),
    )
    assert [r["id"] for r in db.get_photos(collection_id=cid)] == [lib["a"]]
    assert db.get_photo_ids(collection_id=cid) == [lib["a"]]
    assert db.count_filtered_photos(collection_id=cid) == 1
    assert db.get_photo_position(lib["a"], collection_id=cid) == 0
    assert db.get_photo_position(lib["b"], collection_id=cid) is None
    summary = db.get_browse_summary(collection_id=cid)
    assert summary["total"] == 3
    assert summary["filtered_total"] == 1
    assert db.get_calendar_data(2024, collection_id=cid)["days"] == {
        "2024-01-05": 1,
    }


@pytest.mark.parametrize("method,args", [
    ("get_photos", ()),
    ("get_photo_ids", ()),
    ("count_filtered_photos", ()),
    ("get_photo_position", (1,)),
    ("get_browse_summary", ()),
    ("get_calendar_data", (2024,)),
])
def test_listing_missing_collection_raises(db, lib, method, args):
    with pytest.raises(ValueError, match="collection not found in active workspace"):
        getattr(db, method)(*args, collection_id=987654)


@pytest.mark.parametrize("method,args", [
    ("get_photos", ()),
    ("get_photo_ids", ()),
    ("count_filtered_photos", ()),
    ("get_photo_position", (1,)),
    ("get_browse_summary", ()),
    ("get_calendar_data", (2024,)),
    ("count_photos_with_companions", ([1],)),
])
def test_listing_requires_active_workspace(db, lib, method, args):
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError):
        getattr(db, method)(*args)


def test_listing_routes_through_facade_helpers(db, lib, monkeypatch):
    """Folder subtrees, collection queries and sort clauses come from the
    façade, so patches of those Database methods reach the listing reads."""
    calls = []
    real_subtree = db.get_folder_subtree_ids
    real_coll = db._build_collection_query
    real_sort = db._photo_sort_clause

    def subtree(fid):
        calls.append(("subtree", fid))
        return real_subtree(fid)

    def coll(cid):
        calls.append(("coll", cid))
        return real_coll(cid)

    def sort(key):
        calls.append(("sort", key))
        return real_sort(key)

    monkeypatch.setattr(db, "get_folder_subtree_ids", subtree)
    monkeypatch.setattr(db, "_build_collection_query", coll)
    monkeypatch.setattr(db, "_photo_sort_clause", sort)
    cid = db.add_collection("All", json.dumps([{"field": "all"}]))
    db.get_photos(folder_id=lib["root"], collection_id=cid, sort="name")
    db.get_photo_ids(folder_id=lib["root"], collection_id=cid)
    db.count_filtered_photos(folder_id=lib["root"], collection_id=cid)
    db.get_photo_position(lib["a"], folder_id=lib["root"], collection_id=cid)
    db.get_browse_summary(folder_id=lib["root"], collection_id=cid)
    db.get_calendar_data(2024, folder_id=lib["root"], collection_id=cid)
    root = lib["root"]
    assert calls == [
        ("subtree", root), ("coll", cid), ("sort", "name"),
        ("subtree", root), ("coll", cid), ("sort", "date"),
        ("subtree", root), ("coll", cid),
        ("subtree", root), ("coll", cid), ("sort", "date"),
        ("subtree", root), ("coll", cid),
        ("coll", cid), ("subtree", root),
    ]


# -- get_photo_position --------------------------------------------------------


def test_get_photo_position(db, lib):
    assert db.get_photo_position(lib["c"]) == 0
    assert db.get_photo_position(lib["b"]) == 2
    assert db.get_photo_position(lib["b"], sort="name") == 1
    assert db.get_photo_position(lib["b"], folder_id=lib["root"]) == 2
    assert db.get_photo_position(lib["f"]) is None
    assert db.get_photo_position(999999) is None


# -- get_calendar_data ---------------------------------------------------------


def test_get_calendar_data(db, lib):
    data = db.get_calendar_data(2024)
    assert data == {
        "year": 2024,
        "days": {"2024-01-05": 1, "2024-03-07": 1},
        "min_year": 2023,
        "max_year": 2024,
    }
    assert db.get_calendar_data(2023, folder_id=lib["child"])["days"] == {
        "2023-06-01": 1,
    }
    db.update_photo_rating(lib["b"], 4)
    assert db.get_calendar_data(
        2024, rules=[{"field": "rating", "op": ">=", "value": 4}],
    )["days"] == {"2024-03-07": 1}


def test_get_calendar_data_year_bounds_default_to_requested_year(db):
    data = db.get_calendar_data(2031)
    assert data == {"year": 2031, "days": {}, "min_year": 2031, "max_year": 2031}


# -- get_browse_summary --------------------------------------------------------


def _detect(db, pid, conf, species, confidence=0.9, fp="fp"):
    det = db.conn.execute(
        "INSERT INTO detections (photo_id, detector_confidence, category) "
        "VALUES (?, ?, 'animal')", (pid, conf),
    ).lastrowid
    pred = db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, "
        "labels_fingerprint, species, confidence) VALUES (?, 'm', ?, ?, ?)",
        (det, fp, species, confidence),
    ).lastrowid
    db.conn.commit()
    return pred


def test_get_browse_summary(db, lib, isolated_config):
    _detect(db, lib["a"], 0.9, "Heron")
    _detect(db, lib["b"], 0.9, "Heron")
    rejected = _detect(db, lib["c"], 0.9, "Egret")
    _detect(db, lib["f"], 0.9, "Crane")
    db.conn.execute(
        "INSERT INTO prediction_review (prediction_id, workspace_id, status) "
        "VALUES (?, ?, 'rejected')", (rejected, db._active_workspace_id),
    )
    db.conn.commit()
    summary = db.get_browse_summary()
    assert summary == {
        "total": 3,
        "filtered_total": 3,
        "classified": 3,
        "unclassified": 0,
        "top_species": [{"species": "Heron", "count": 2}],
        "folder_counts": [
            {"folder_id": lib["root"], "name": "lib", "count": 2},
            {"folder_id": lib["child"], "name": "c", "count": 1},
        ],
    }
    by_folder = db.get_browse_summary(folder_id=lib["child"])
    assert by_folder["filtered_total"] == 1
    assert by_folder["top_species"] == []
    db.update_photo_rating(lib["a"], 5)
    ruled = db.get_browse_summary(
        rules=[{"field": "rating", "op": ">=", "value": 5}],
    )
    assert ruled["total"] == 3
    assert ruled["filtered_total"] == 1
    assert ruled["top_species"] == [{"species": "Heron", "count": 1}]


def test_get_browse_summary_reads_detector_floor_from_effective_config(
    db, lib, isolated_config, monkeypatch,
):
    _detect(db, lib["a"], 0.5, "Heron")
    seen = []
    real = db.get_effective_config

    def effective(config):
        seen.append(True)
        out = dict(real(config))
        out["detector_confidence"] = 0.6
        return out

    monkeypatch.setattr(db, "get_effective_config", effective)
    summary = db.get_browse_summary()
    assert seen == [True]
    assert summary["classified"] == 0
    assert summary["unclassified"] == 3
    assert summary["top_species"] == []


def test_get_browse_summary_rejects_malformed_rules(db, lib, isolated_config):
    with pytest.raises(ValueError):
        db.get_browse_summary(rules=[{"field": "no-such-field", "value": 1}])


# -- count_photos_with_companions ------------------------------------------------


def test_count_photos_with_companions(db, lib):
    db.conn.execute(
        "UPDATE photos SET companion_path = 'a.xmp' WHERE id IN (?, ?)",
        (lib["a"], lib["f"]),
    )
    db.conn.execute(
        "UPDATE photos SET companion_path = '' WHERE id = ?", (lib["b"],),
    )
    ids = [lib["a"], lib["a"], lib["b"], lib["c"], lib["f"]]
    assert db.count_photos_with_companions(ids) == 1
    assert db.count_photos_with_companions([]) == 0
    assert db.count_photos_with_companions(None) == 0


def test_count_photos_with_companions_chunks(db, lib):
    db.conn.execute(
        "UPDATE photos SET companion_path = 'a.xmp' WHERE id = ?", (lib["a"],),
    )
    ids = [lib["a"]] + list(range(10**6, 10**6 + _SQLITE_PARAM_CHUNK_SIZE))
    statements = _trace(db)
    assert db.count_photos_with_companions(ids) == 1
    db.conn.set_trace_callback(None)
    assert len(_sql(statements, "SELECT COUNT(*) AS n")) == 2


# -- resolve_photos_for_delete ---------------------------------------------------


def test_resolve_photos_for_delete(db, lib, tmp_path):
    assert db.resolve_photos_for_delete([]) == {"ids": [], "files": [], "_rows": []}
    assert db.resolve_photos_for_delete([999999]) == {
        "ids": [], "files": [], "_rows": [],
    }
    got = db.resolve_photos_for_delete([lib["a"], lib["a"]])
    assert got["ids"] == [lib["a"]]
    assert got["files"] == [{
        "photo_id": lib["a"],
        "folder_id": lib["root"],
        "folder_path": str(tmp_path / "lib"),
        "filename": "a.jpg",
        "companion_path": None,
    }]
    assert [r["id"] for r in got["_rows"]] == [lib["a"]]
    # Read-only: nothing is deleted and no transaction is left open.
    assert db.get_photo(lib["a"]) is not None
    assert not db.conn.in_transaction


def test_resolve_photos_for_delete_companions(db, lib):
    raw = _photo(db, lib["root"], "a.cr3")
    db.conn.execute(
        "UPDATE photos SET companion_path = 'a.cr3' WHERE id = ?", (lib["a"],),
    )
    db.conn.execute(
        "UPDATE photos SET companion_path = 'gone.cr3' WHERE id = ?", (lib["b"],),
    )
    db.conn.commit()
    without = db.resolve_photos_for_delete([lib["a"]])
    assert without["ids"] == [lib["a"]]
    got = db.resolve_photos_for_delete([lib["a"], lib["b"]], include_companions=True)
    assert sorted(got["ids"]) == sorted([lib["a"], lib["b"], raw])
    assert [f["photo_id"] for f in got["files"]] == [lib["a"], lib["b"], raw]
    # A companion the caller already selected is not added twice.
    both = db.resolve_photos_for_delete([lib["a"], raw], include_companions=True)
    assert [f["photo_id"] for f in both["files"]] == [lib["a"], raw]


# -- delete_photos ---------------------------------------------------------------


def test_delete_photos_removes_rows_and_dependents(db, lib, cache, monkeypatch):
    pruned = []
    monkeypatch.setattr(db, "prune_pipeline_cache_for_ids", pruned.append)
    kid = db.add_keyword("Heron")
    db.tag_photo(lib["a"], kid)
    db.queue_change(lib["a"], "keyword_add", "Heron")
    _detect(db, lib["a"], 0.9, "Heron")
    db.conn.execute(
        "UPDATE folders SET photo_count = 5 WHERE id = ?", (lib["root"],),
    )
    db.conn.commit()

    result = db.delete_photos([lib["a"]])

    assert result["deleted"] == 1
    assert result["ids"] == [lib["a"]]
    assert [f["filename"] for f in result["files"]] == ["a.jpg"]
    assert not db.conn.in_transaction
    with _reader(db) as r:
        for table in ("photo_keywords", "pending_changes", "detections"):
            assert r.execute(
                f"SELECT COUNT(*) FROM {table} WHERE photo_id = ?", (lib["a"],),
            ).fetchone()[0] == 0, table
        assert r.execute("SELECT COUNT(*) FROM predictions").fetchone()[0] == 0
        assert r.execute(
            "SELECT COUNT(*) FROM photos WHERE id = ?", (lib["a"],),
        ).fetchone()[0] == 0
        assert r.execute(
            "SELECT photo_count FROM folders WHERE id = ?", (lib["root"],),
        ).fetchone()[0] == 4
    assert pruned == [[lib["a"]]]
    assert len(cache.invalidated) == 1


def test_delete_photos_nothing_to_delete(db, lib, cache, monkeypatch):
    pruned = []
    monkeypatch.setattr(db, "prune_pipeline_cache_for_ids", pruned.append)
    assert db.delete_photos([]) == {"deleted": 0, "ids": [], "files": []}
    assert db.delete_photos([999999]) == {"deleted": 0, "ids": [], "files": []}
    assert pruned == []
    assert cache.invalidated == []


def test_delete_photos_goes_through_facade_resolver(db, lib, monkeypatch):
    seen = []
    real = db.resolve_photos_for_delete

    def resolver(photo_ids, include_companions=False):
        seen.append((list(photo_ids), include_companions))
        return real(photo_ids, include_companions=include_companions)

    monkeypatch.setattr(db, "resolve_photos_for_delete", resolver)
    db.delete_photos([lib["b"]], include_companions=True)
    assert seen == [([lib["b"]], True)]


def test_delete_photos_cleans_collection_photo_id_rules(db, lib):
    flat = db.add_collection(
        "flat", json.dumps([{"field": "photo_ids", "value": [lib["a"], lib["b"]]}]),
    )
    nested = db.add_collection("nested", json.dumps([
        {"match": "any", "rules": [
            {"field": "photo_ids", "value": [lib["a"]]},
            {"field": "rating", "op": ">=", "value": 3},
        ]},
    ]))
    odd = json.dumps([
        {"field": "photo_ids", "value": "not-a-list"},
        {"field": "photo_ids"},
        "junk",
    ])
    untouched = db.add_collection("odd", odd)
    other = db.add_collection(
        "other", json.dumps([{"field": "photo_ids", "value": [lib["c"]]}]),
    )
    # A collection in another workspace is left alone.
    db.conn.execute(
        "INSERT INTO collections (name, rules, workspace_id) VALUES (?, ?, ?)",
        ("foreign", json.dumps([{"field": "photo_ids", "value": [lib["a"]]}]),
         lib["other_ws"]),
    )
    db.conn.commit()

    statements = _trace(db)
    db.delete_photos([lib["a"]])
    db.conn.set_trace_callback(None)

    rules = dict(db.conn.execute("SELECT name, rules FROM collections").fetchall())
    assert json.loads(rules["flat"]) == [{"field": "photo_ids", "value": [lib["b"]]}]
    assert json.loads(rules["nested"])[0]["rules"][0] == {
        "field": "photo_ids", "value": [],
    }
    assert rules["odd"] == odd
    assert json.loads(rules["other"]) == [{"field": "photo_ids", "value": [lib["c"]]}]
    assert json.loads(rules["foreign"]) == [
        {"field": "photo_ids", "value": [lib["a"]]},
    ]
    updates = _sql(statements, "UPDATE collections SET rules")
    assert [s.rsplit("WHERE id = ", 1)[1] for s in updates] == [
        str(flat), str(nested),
    ]
    assert untouched not in (flat, nested) and other not in (flat, nested)


def test_delete_photos_expires_drained_move_provenance(db, lib, tmp_path):
    lib_path = str(tmp_path / "lib")
    dest = db.add_folder(str(tmp_path / "dest"), name="dest")
    moved_a = _photo(db, dest, "a.cr3")
    moved_b = _photo(db, dest, "b.cr3")
    db.conn.execute(
        "UPDATE photos SET last_move_source_folder_path = ? WHERE id IN (?, ?)",
        (lib_path, moved_a, moved_b),
    )
    # A same-stem sibling that remains in the source keeps b's proof alive.
    _photo(db, lib["root"], "b.cr3")
    db.conn.commit()

    db.delete_photos([lib["a"], lib["b"]])

    got = dict(db.conn.execute(
        "SELECT id, last_move_source_folder_path FROM photos WHERE id IN (?, ?)",
        (moved_a, moved_b),
    ).fetchall())
    assert got == {moved_a: None, moved_b: lib_path}


def test_delete_photos_commit_false_joins_outer_transaction(db, lib, cache, monkeypatch):
    pruned = []
    monkeypatch.setattr(db, "prune_pipeline_cache_for_ids", pruned.append)
    result = db.delete_photos([lib["a"]], commit=False)
    assert result["ids"] == [lib["a"]]
    assert db.conn.in_transaction
    with _reader(db) as r:
        assert r.execute(
            "SELECT COUNT(*) FROM photos WHERE id = ?", (lib["a"],),
        ).fetchone()[0] == 1
    assert pruned == []
    assert len(cache.invalidated) == 1
    db.conn.rollback()
    assert db.get_photo(lib["a"]) is not None


def test_delete_photos_rolls_back_on_error_and_still_invalidates(db, lib, cache, monkeypatch):
    pruned = []
    monkeypatch.setattr(db, "prune_pipeline_cache_for_ids", pruned.append)
    kid = db.add_keyword("Heron")
    db.tag_photo(lib["a"], kid)
    db.conn.execute(
        "CREATE TEMP TRIGGER block_photo_delete BEFORE DELETE ON photos "
        "BEGIN SELECT RAISE(ABORT, 'blocked'); END"
    )
    with pytest.raises(sqlite3.IntegrityError, match="blocked"):
        db.delete_photos([lib["a"]])
    assert not db.conn.in_transaction
    # The earlier keyword delete was rolled back with the rest.
    assert db.conn.execute(
        "SELECT COUNT(*) FROM photo_keywords WHERE photo_id = ?", (lib["a"],),
    ).fetchone()[0] == 1
    assert len(cache.invalidated) == 1
    assert pruned == []


def test_delete_photos_commit_false_error_leaves_rollback_to_caller(db, lib, cache):
    kid = db.add_keyword("Heron")
    db.tag_photo(lib["a"], kid)
    db.conn.execute(
        "CREATE TEMP TRIGGER block_photo_delete BEFORE DELETE ON photos "
        "BEGIN SELECT RAISE(ABORT, 'blocked'); END"
    )
    with pytest.raises(sqlite3.IntegrityError, match="blocked"):
        db.delete_photos([lib["a"]], commit=False)
    assert db.conn.in_transaction
    assert db.conn.execute(
        "SELECT COUNT(*) FROM photo_keywords WHERE photo_id = ?", (lib["a"],),
    ).fetchone()[0] == 0
    db.conn.rollback()
    assert len(cache.invalidated) == 1


def test_delete_photos_resolves_workspace_mid_transaction(db, lib, cache):
    """The active workspace is read after the dependent-row deletes, so a
    missing workspace fails inside the transaction and rolls it back."""
    kid = db.add_keyword("Heron")
    db.tag_photo(lib["a"], kid)
    db.set_active_workspace(None)
    statements = _trace(db)
    with pytest.raises(RuntimeError):
        db.delete_photos([lib["a"]])
    db.conn.set_trace_callback(None)
    stmts = [s.strip() for s in statements]
    first_delete = next(
        i for i, s in enumerate(stmts) if s.startswith("DELETE FROM photo_keywords")
    )
    assert "ROLLBACK" in stmts[first_delete:]
    assert not any("FROM collections" in s for s in stmts)
    assert db.conn.execute(
        "SELECT COUNT(*) FROM photo_keywords WHERE photo_id = ?", (lib["a"],),
    ).fetchone()[0] == 1
    assert len(cache.invalidated) == 1


def test_delete_photos_chunks_every_in_clause(db, lib, monkeypatch):
    monkeypatch.setattr(db, "prune_pipeline_cache_for_ids", lambda ids: None)
    ids = [_photo(db, lib["root"], f"z{i}.jpg")
           for i in range(_SQLITE_PARAM_CHUNK_SIZE + 3)]
    statements = _trace(db)
    result = db.delete_photos(ids)
    db.conn.set_trace_callback(None)
    assert result["deleted"] == len(ids)
    statements = list(dict.fromkeys(s.strip() for s in statements))
    for prefix in ("DELETE FROM photo_keywords", "DELETE FROM pending_changes",
                   "DELETE FROM detections", "DELETE FROM photos"):
        assert len([s for s in statements if s.startswith(prefix)]) == 2, prefix


def test_delete_photos_sql_order(db, lib, monkeypatch, tmp_path):
    monkeypatch.setattr(db, "prune_pipeline_cache_for_ids", lambda ids: None)
    statements = _trace(db)
    db.delete_photos([lib["a"]])
    db.conn.set_trace_callback(None)
    ws = db._active_workspace_id
    a = lib["a"]
    root = lib["root"]
    # FK cascade sub-programs re-report their statement; dedupe keeps order.
    stmts = list(dict.fromkeys(s.strip() for s in statements))
    head = stmts.index(f"DELETE FROM photo_keywords WHERE photo_id IN ({a})")
    assert stmts[head:stmts.index("COMMIT") + 1] == [
        f"DELETE FROM photo_keywords WHERE photo_id IN ({a})",
        f"DELETE FROM pending_changes WHERE photo_id IN ({a})",
        f"DELETE FROM detections WHERE photo_id IN ({a})",
        f"SELECT id, rules FROM collections WHERE workspace_id = {ws}",
        f"DELETE FROM photos WHERE id IN ({a})",
        f"SELECT filename FROM photos WHERE folder_id = {root}",
        "SELECT id, filename, last_move_source_folder_path FROM photos "
        f"WHERE last_move_source_folder_path IN ('{tmp_path / 'lib'}')",
        f"UPDATE folders SET photo_count = photo_count - 1 WHERE id = {root}",
        "COMMIT",
    ]


# -- prune_pipeline_cache_for_ids ----------------------------------------------


def test_prune_pipeline_cache_for_ids(db, monkeypatch):
    import pipeline

    calls = []
    monkeypatch.setattr(pipeline, "prune_results", lambda *a: calls.append(a))
    db.prune_pipeline_cache_for_ids([])
    assert calls == []
    db.prune_pipeline_cache_for_ids([1, 2])
    import os
    assert calls == [(os.path.dirname(db._db_path), db._active_workspace_id, [1, 2])]
    db.set_active_workspace(None)
    db.prune_pipeline_cache_for_ids([3])
    assert len(calls) == 1


def test_prune_pipeline_cache_for_ids_swallows_errors_but_not_interrupts(db, monkeypatch):
    import pipeline

    def boom(*a):
        raise OSError("disk")

    monkeypatch.setattr(pipeline, "prune_results", boom)
    db.prune_pipeline_cache_for_ids([1])

    def interrupt(*a):
        raise KeyboardInterrupt

    monkeypatch.setattr(pipeline, "prune_results", interrupt)
    with pytest.raises(KeyboardInterrupt):
        db.prune_pipeline_cache_for_ids([1])


# -- quality writers -------------------------------------------------------------


def test_update_photo_sharpness_commits(db, lib):
    db.update_photo_sharpness(lib["a"], 12.5)
    assert not db.conn.in_transaction
    with _reader(db) as r:
        assert r.execute(
            "SELECT sharpness FROM photos WHERE id = ?", (lib["a"],),
        ).fetchone()[0] == 12.5


def test_update_photo_quality_commits_and_overwrites_all_fields(db, lib):
    db.update_photo_quality(lib["a"], subject_sharpness=1.0, subject_size=2.0,
                            quality_score=3.0, sharpness=4.0)
    with _reader(db) as r:
        row = r.execute(
            "SELECT subject_sharpness, subject_size, quality_score, sharpness "
            "FROM photos WHERE id = ?", (lib["a"],),
        ).fetchone()
    assert tuple(row) == (1.0, 2.0, 3.0, 4.0)
    db.update_photo_quality(lib["a"], quality_score=9.0)
    row = db.conn.execute(
        "SELECT subject_sharpness, subject_size, quality_score, sharpness "
        "FROM photos WHERE id = ?", (lib["a"],),
    ).fetchone()
    assert tuple(row) == (None, None, 9.0, None)
    assert not db.conn.in_transaction


def test_quality_writers_use_commit_with_retry(db, lib, monkeypatch):
    import db as db_module

    commits = []
    real = db_module.commit_with_retry
    monkeypatch.setattr(
        db_module, "commit_with_retry",
        lambda conn, *a, **kw: (commits.append(1), real(conn, *a, **kw))[1],
    )
    db.update_photo_sharpness(lib["a"], 1.0)
    db.update_photo_quality(lib["a"], sharpness=2.0)
    assert commits == [1, 1]


# -- structure -----------------------------------------------------------------

MOVED = [
    "filter_out_wildlife_excluded", "add_photo", "get_photo",
    "get_photo_filenames", "get_photos_by_ids", "get_photo_folder_statuses",
    "count_photos", "count_photos_in_workspace", "photos_by_paths",
    "get_calendar_data", "get_photos", "get_photo_ids", "get_photo_position",
    "count_filtered_photos", "get_browse_summary",
    "count_photos_with_companions", "resolve_photos_for_delete",
    "delete_photos", "update_photo_sharpness", "update_photo_quality",
]


def _self_attrs(name):
    source = textwrap.dedent(inspect.getsource(getattr(Database, name)))
    fn = ast.parse(source).body[0]
    return {
        node.attr
        for node in ast.walk(fn)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }


@pytest.mark.parametrize("name", MOVED)
def test_photo_method_delegates_to_repository(name):
    attrs = _self_attrs(name)
    assert "conn" not in attrs, (
        f"Database.{name} touches self.conn; move the SQL to PhotoRepository"
    )
    assert "_photos_repository" in attrs, (
        f"Database.{name} no longer delegates to PhotoRepository"
    )


def test_composition_stays_on_the_facade():
    """Cross-domain calls and side effects are made from the wrappers, so
    monkeypatches of these Database methods keep applying."""
    assert {
        "resolve_photos_for_delete", "invalidate_new_images_cache_for_folders",
        "prune_pipeline_cache_for_ids", "_ws_id",
    } <= _self_attrs("delete_photos")
    assert "check_and_resolve_duplicates_for_hash" in _self_attrs("add_photo")
    assert "_FILTER_SUBJECT_CHUNK" in _self_attrs("filter_out_wildlife_excluded")
    assert "get_effective_config" in _self_attrs("get_browse_summary")
    for name in ("get_photos", "get_photo_ids", "count_filtered_photos"):
        assert {
            "get_folder_subtree_ids", "_build_collection_query",
            "_append_location_status_filter",
        } <= _self_attrs(name), name
    for name in ("get_calendar_data", "get_browse_summary"):
        assert "_build_query_from_rules" in _self_attrs(name), name


def test_photo_repository_imports_no_db_code():
    import repositories.photos as module

    tree = ast.parse(inspect.getsource(module))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported |= {alias.name.split(".")[0] for alias in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])
    assert not imported & {"db", "config"}
