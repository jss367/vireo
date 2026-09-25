"""Behavior pins for the locations domain of ``Database``.

The tests exercise place chains, photo locations, map/geo queries and the
reverse-geocode cache only through the public ``Database`` façade (plus a
few private helpers other domains call), so they hold whether the SQL lives
in ``db.py`` or in ``repositories/locations.py``. They pin commit
boundaries, error types and messages, return shapes, the order in which the
active workspace is resolved, chunking, and the façade calls
(``tag_photo``, ``queue_change``, ``get_folder_subtree_ids``,
``_build_query_from_rules``, ``_merge_keyword_into``) that must stay
patchable on ``Database``.
"""

import ast
import contextlib
import inspect
import logging
import sqlite3
import textwrap

import pytest
from db import _SQLITE_PARAM_CHUNK_SIZE, KEYWORD_SOURCE_MANUAL, Database

# -- helpers ------------------------------------------------------------------


def _reader(db):
    """A second connection: sees only what the façade has committed."""
    conn = sqlite3.connect(db._db_path)
    conn.row_factory = sqlite3.Row
    return contextlib.closing(conn)


def _photo(db, fid, name, lat=None, lng=None, timestamp=None):
    pid = db.add_photo(
        folder_id=fid, filename=name, extension=".jpg",
        file_size=10, file_mtime=1.0, timestamp=timestamp,
    )
    if lat is not None or lng is not None:
        db.conn.execute(
            "UPDATE photos SET latitude = ?, longitude = ? WHERE id = ?",
            (lat, lng, pid),
        )
        db.conn.commit()
    return pid


def _kw(db, name, parent_id=None, kw_type="location", lat=None, lng=None,
        place_id=None):
    cur = db.conn.execute(
        "INSERT INTO keywords (name, parent_id, type, latitude, longitude, "
        "place_id) VALUES (?, ?, ?, ?, ?, ?)",
        (name, parent_id, kw_type, lat, lng, place_id),
    )
    db.conn.commit()
    return cur.lastrowid


def _link(db, pid, kid, source="manual"):
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id, source) "
        "VALUES (?, ?, ?)",
        (pid, kid, source),
    )
    db.conn.commit()


def _trace(db):
    statements = []
    db.conn.set_trace_callback(statements.append)
    return statements


def _selects(statements, needle):
    return [s for s in statements if needle in s]


@pytest.fixture
def fid(db):
    return db.add_folder("/photos", name="photos")


# -- location status ------------------------------------------------------------


def test_photo_location_statuses_empty_input_skips_query(db):
    statements = _trace(db)
    assert db.get_photo_location_statuses([]) == {}
    assert statements == []


def test_photo_location_statuses_classify_each_source(db, fid):
    exif = _photo(db, fid, "exif.jpg", 1.0, 2.0)
    half = _photo(db, fid, "half.jpg", 1.0, None)
    assigned = _photo(db, fid, "assigned.jpg")
    bare = _photo(db, fid, "bare.jpg")
    coordless = _photo(db, fid, "coordless.jpg")
    placed = _kw(db, "Park", lat=5.0, lng=6.0)
    text_only = _kw(db, "Somewhere")
    _link(db, half, placed)
    _link(db, assigned, placed)
    _link(db, coordless, text_only)

    assert db.get_photo_location_statuses(
        [exif, half, assigned, bare, coordless, 999_999]
    ) == {
        exif: "exif",
        half: "assigned",
        assigned: "assigned",
        bare: "none",
        coordless: "none",
    }


def test_photo_location_statuses_dedupes_and_chunks(db, fid):
    pid = _photo(db, fid, "a.jpg", 1.0, 2.0)
    ids = [pid, pid] + list(range(10_000, 10_000 + _SQLITE_PARAM_CHUNK_SIZE))
    statements = _trace(db)
    result = db.get_photo_location_statuses(ids)
    assert result == {pid: "exif"}
    queries = _selects(statements, "AS location_status")
    assert len(queries) == 2


def test_append_location_status_filter_rejects_unknown_status():
    conditions = []
    Database._append_location_status_filter(conditions, None)
    assert conditions == []
    with pytest.raises(ValueError, match="location_status must be"):
        Database._append_location_status_filter(conditions, "gps")


# -- map queries ------------------------------------------------------------------


def test_geolocated_photos_shape_fallback_and_order(db, fid):
    late = _photo(db, fid, "late.jpg", 1.0, 2.0, timestamp="2024-06-01T00:00:00")
    early = _photo(db, fid, "early.jpg", timestamp="2024-01-01T00:00:00")
    undated = _photo(db, fid, "undated.jpg", 3.0, 4.0)
    _photo(db, fid, "nowhere.jpg")
    root = _kw(db, "Kenya", lat=0.5, lng=37.0)
    leaf = _kw(db, "Mara", parent_id=root, lat=-1.5, lng=35.1)
    _link(db, early, root)
    _link(db, early, leaf)

    rows = db.get_geolocated_photos()
    assert [r["id"] for r in rows] == [early, late, undated]
    by_id = {r["id"]: dict(r) for r in rows}
    assert by_id[early]["coord_source"] == "keyword"
    assert by_id[early]["keyword_location_name"] == "Mara"
    assert (by_id[early]["latitude"], by_id[early]["longitude"]) == (-1.5, 35.1)
    assert by_id[late]["coord_source"] == "exif"
    assert by_id[late]["keyword_location_name"] is None
    assert set(by_id[late]) == {
        "id", "latitude", "longitude", "coord_source",
        "keyword_location_name", "thumb_path", "filename", "timestamp",
        "rating", "folder_id", "species",
    }


def test_geolocated_photos_resolves_workspace_before_folder_subtree(
    db, fid, monkeypatch,
):
    calls = []
    monkeypatch.setattr(
        db, "get_folder_subtree_ids", lambda f: calls.append(f) or [f],
    )
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError, match="No active workspace"):
        db.get_geolocated_photos(folder_id=fid)
    assert calls == []


def test_geolocated_photos_route_folder_and_rules_through_facade(
    db, fid, monkeypatch,
):
    child = db.add_folder("/photos/sub", name="sub", parent_id=fid)
    in_child = _photo(db, child, "child.jpg", 1.0, 2.0)
    _photo(db, fid, "top.jpg", 1.0, 2.0)

    subtree_calls = []
    real_subtree = db.get_folder_subtree_ids

    def subtree(folder_id):
        subtree_calls.append(folder_id)
        return real_subtree(folder_id)

    rules_calls = []
    real_rules = db._build_query_from_rules

    def rules(rule_tree):
        rules_calls.append(rule_tree)
        return real_rules(rule_tree)

    monkeypatch.setattr(db, "get_folder_subtree_ids", subtree)
    monkeypatch.setattr(db, "_build_query_from_rules", rules)
    rule_tree = [{"field": "rating", "op": ">=", "value": 0}]
    rows = db.get_geolocated_photos(folder_id=child, rules=rule_tree)
    assert [r["id"] for r in rows] == [in_child]
    assert subtree_calls == [child]
    assert rules_calls == [rule_tree]


def test_plottable_photo_ids_folder_subtree_and_keyword_coords(db, fid):
    child = db.add_folder("/photos/sub", name="sub", parent_id=fid)
    other = db.add_folder("/other", name="other")
    exif = _photo(db, child, "exif.jpg", 1.0, 2.0)
    by_kw = _photo(db, fid, "kw.jpg")
    _photo(db, fid, "none.jpg")
    elsewhere = _photo(db, other, "elsewhere.jpg", 1.0, 2.0)
    _link(db, by_kw, _kw(db, "Park", lat=1.0, lng=1.0))

    assert sorted(db.get_plottable_photo_ids()) == sorted([exif, by_kw, elsewhere])
    assert sorted(db.get_plottable_photo_ids(folder_id=fid)) == sorted([exif, by_kw])
    assert db.get_plottable_photo_ids(folder_id=child) == [exif]


def test_plottable_photo_ids_resolves_workspace_before_subtree(db, fid, monkeypatch):
    calls = []
    monkeypatch.setattr(db, "get_folder_subtree_ids", lambda f: calls.append(f) or [f])
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError, match="No active workspace"):
        db.get_plottable_photo_ids(folder_id=fid)
    assert calls == []


def test_count_photos_without_coordinates_skips_missing_folders(db, fid):
    gone = db.add_folder("/gone", name="gone")
    _photo(db, fid, "exif.jpg", 1.0, 2.0)
    half = _photo(db, fid, "half.jpg", None, 2.0)
    _photo(db, fid, "bare.jpg")
    _photo(db, gone, "gone.jpg")
    _link(db, half, _kw(db, "Placed", lat=1.0, lng=1.0))
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (gone,))
    db.conn.commit()

    assert db.count_photos_without_coordinates() == 1
    assert db.count_photos_without_gps() == 1
    db.set_active_workspace(db.create_workspace("Empty"))
    assert db.count_photos_without_coordinates() == 0


def test_count_photos_with_location_is_distinct_and_workspace_scoped(db, fid):
    pid = _photo(db, fid, "a.jpg")
    _photo(db, fid, "b.jpg")
    root = _kw(db, "Kenya")
    _link(db, pid, root)
    _link(db, pid, _kw(db, "Mara", parent_id=root))
    _link(db, pid, _kw(db, "Bird", kw_type="general"))
    assert db.count_photos_with_location() == 1
    db.set_active_workspace(db.create_workspace("Other"))
    assert db.count_photos_with_location() == 0
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError):
        db.count_photos_with_location()


# -- assigned / effective locations ---------------------------------------------


def test_assigned_photo_location_picks_deepest_then_newest(db, fid):
    pid = _photo(db, fid, "a.jpg", 9.0, 9.0)
    root = _kw(db, "Kenya", lat=0.5, lng=37.0, place_id="root-place")
    older = _kw(db, "Mara", parent_id=root, lat=-1.0, lng=35.0, place_id="p-old")
    newer = _kw(db, "Serengeti", parent_id=root, lat=-2.0, lng=34.0)
    _kw(db, "Empty", parent_id=root)  # coordless: never chosen
    for kid in (root, older, newer):
        _link(db, pid, kid)

    assert db.get_assigned_photo_location(pid) == {
        "photo_id": pid,
        "latitude": -2.0,
        "longitude": 34.0,
        "source": "keyword",
        "keyword_location_name": "Serengeti",
        "place_id": None,
    }


def test_assigned_photo_location_none_without_coords(db, fid):
    pid = _photo(db, fid, "a.jpg", 1.0, 2.0)
    _link(db, pid, _kw(db, "Text only"))
    assert db.get_assigned_photo_location(pid) is None
    assert db.get_assigned_photo_location(999_999, verify_workspace=False) is None


def test_assigned_photo_location_workspace_guards(db, fid, monkeypatch):
    pid = _photo(db, fid, "a.jpg")
    _link(db, pid, _kw(db, "Park", lat=1.0, lng=2.0, place_id="pl"))
    other = db.create_workspace("Other")
    db.set_active_workspace(other)
    with pytest.raises(ValueError, match="does not belong to the active workspace"):
        db.get_assigned_photo_location(pid)
    with pytest.raises(ValueError, match="does not belong to the active workspace"):
        db.get_assigned_photo_location(pid, allow_sync_only=True)

    db.conn.execute(
        "INSERT INTO workspace_sync_only_photos (workspace_id, photo_id) "
        "VALUES (?, ?)",
        (other, pid),
    )
    db.conn.commit()
    with pytest.raises(ValueError):
        db.get_assigned_photo_location(pid)
    assert db.get_assigned_photo_location(pid, allow_sync_only=True)["place_id"] == "pl"

    # verify_workspace=False never consults the active workspace.
    db.set_active_workspace(None)
    assert db.get_assigned_photo_location(pid, verify_workspace=False)["latitude"] == 1.0


def test_effective_photo_locations_empty_input_needs_no_workspace(db):
    db.set_active_workspace(None)
    statements = _trace(db)
    assert db.get_effective_photo_locations([]) == {}
    assert statements == []


def test_effective_photo_locations_shapes_and_omissions(db, fid):
    exif = _photo(db, fid, "exif.jpg", 1.0, 2.0)
    half = _photo(db, fid, "half.jpg", 1.0, None)
    bare = _photo(db, fid, "bare.jpg")
    _link(db, half, _kw(db, "Park", lat=5.0, lng=6.0, place_id="pp"))
    _link(db, exif, _kw(db, "Ignored", lat=7.0, lng=8.0))

    result = db.get_effective_photo_locations([exif, half, bare, exif])
    assert result == {
        exif: {
            "photo_id": exif, "latitude": 1.0, "longitude": 2.0,
            "source": "exif", "keyword_location_name": None, "place_id": None,
        },
        half: {
            "photo_id": half, "latitude": 5.0, "longitude": 6.0,
            "source": "keyword", "keyword_location_name": "Park",
            "place_id": "pp",
        },
    }
    assert db.get_effective_photo_location(bare) is None
    assert db.get_effective_photo_location(half)["source"] == "keyword"


def test_effective_photo_locations_reports_first_missing_photo(db, fid):
    inside = _photo(db, fid, "in.jpg", 1.0, 2.0)
    with pytest.raises(
        ValueError, match="Photo 424242 does not belong to the active workspace",
    ):
        db.get_effective_photo_locations([inside, 424242, 525252])


def test_effective_photo_locations_unverified_skips_workspace(db, fid):
    pid = _photo(db, fid, "a.jpg", 1.0, 2.0)
    db.set_active_workspace(None)
    result = db.get_effective_photo_locations([pid, 999], verify_workspace=False)
    assert list(result) == [pid]


def test_effective_photo_locations_chunks_and_resolves_workspace_per_chunk(
    db, fid, monkeypatch,
):
    pid = _photo(db, fid, "a.jpg", 1.0, 2.0)
    ids = [pid] + list(range(50_000, 50_000 + _SQLITE_PARAM_CHUNK_SIZE))
    calls = []
    real_ws = db._ws_id
    monkeypatch.setattr(db, "_ws_id", lambda: calls.append(1) or real_ws())
    statements = _trace(db)
    result = db.get_effective_photo_locations(ids, verify_workspace=False)
    assert list(result) == [pid]
    assert len(_selects(statements, "ranked_locations")) == 2
    assert calls == []
    with pytest.raises(ValueError, match="Photo 50000 does not belong"):
        db.get_effective_photo_locations(ids)
    assert len(calls) == 2


# -- exported location leaves and paths -------------------------------------------


def test_photo_location_keyword_ids_prefers_coordinates_then_depth(db, fid):
    a = _photo(db, fid, "a.jpg")
    b = _photo(db, fid, "b.jpg")
    root = _kw(db, "USA")
    coord_root = _kw(db, "Canada", lat=1.0, lng=1.0)
    child = _kw(db, "Texas", parent_id=root)
    _link(db, a, root)
    _link(db, a, child)
    _link(db, b, child)
    _link(db, b, coord_root)
    _link(db, b, _kw(db, "Bird", kw_type="general"))

    assert db.get_photo_location_keyword_ids([a, b, a, 999]) == {
        a: child, b: coord_root,
    }
    assert db.get_photo_location_keyword_ids([]) == {}


def test_photo_location_leaves_chunk_and_dedupe(db, fid):
    pid = _photo(db, fid, "a.jpg")
    kid = _kw(db, "Park")
    _link(db, pid, kid)
    ids = [pid, pid] + list(range(70_000, 70_000 + _SQLITE_PARAM_CHUNK_SIZE))
    statements = _trace(db)
    leaves = db._get_photo_location_leaves(ids)
    assert list(leaves) == [pid]
    assert tuple(leaves[pid]) == (pid, kid, "Park", None)
    assert len(_selects(statements, "ROW_NUMBER()")) == 2


def test_photo_location_paths_walk_share_and_stop(db, fid):
    a = _photo(db, fid, "a.jpg")
    b = _photo(db, fid, "b.jpg")
    c = _photo(db, fid, "c.jpg")
    general = _kw(db, "Travel", kw_type="general")
    country = _kw(db, "Kenya", parent_id=general)
    park = _kw(db, "Mara", parent_id=country)
    _link(db, a, park)
    _link(db, b, park)
    _photo(db, fid, "d.jpg")

    # A corrupt cycle must terminate rather than loop forever.
    x = _kw(db, "X")
    y = _kw(db, "Y", parent_id=x)
    db.conn.execute("UPDATE keywords SET parent_id = ? WHERE id = ?", (y, x))
    db.conn.commit()
    _link(db, c, y)

    statements = _trace(db)
    paths = db.get_photo_location_paths([a, b, c])
    assert paths == {a: ["Kenya", "Mara"], b: ["Kenya", "Mara"], c: ["X", "Y"]}
    # One parent lookup per ancestor step, shared across photos in the batch.
    walks = _selects(statements, "SELECT name, parent_id, type FROM keywords")
    assert len(walks) == 3
    assert db.get_photo_location_paths([]) == {}


def test_has_pending_location_change_reads_across_workspaces(db, fid):
    pid = _photo(db, fid, "a.jpg")
    assert db.has_pending_location_change(pid) is False
    db.queue_change(pid, "rating", "3")
    assert db.has_pending_location_change(pid) is False
    db.queue_change(pid, "location", "effective")
    db.set_active_workspace(db.create_workspace("Other"))
    assert db.has_pending_location_change(pid) is True
    db.set_active_workspace(None)
    assert db.has_pending_location_change(pid) is True


# -- sync backfill ------------------------------------------------------------------


def test_queue_location_changes_backfill_commits_and_is_idempotent(db, fid):
    a = _photo(db, fid, "a.jpg")
    b = _photo(db, fid, "b.jpg")
    _photo(db, fid, "c.jpg")
    kid = _kw(db, "Park")
    _link(db, a, kid)
    _link(db, b, kid)
    db.queue_change(a, "location", "effective")

    assert db.queue_location_changes_for_tagged_photos() == {
        "photos": 2, "queued": 1, "already_queued": 1,
    }
    assert not db.conn.in_transaction
    with _reader(db) as other:
        rows = other.execute(
            "SELECT photo_id, value, workspace_id FROM pending_changes "
            "WHERE change_type = 'location' ORDER BY photo_id"
        ).fetchall()
    assert [tuple(r) for r in rows] == [
        (a, "effective", db._ws_id()), (b, "effective", db._ws_id()),
    ]
    assert db.queue_location_changes_for_tagged_photos()["queued"] == 0


def test_queue_location_changes_route_through_facade_queue_change(
    db, fid, monkeypatch, caplog,
):
    a = _photo(db, fid, "a.jpg")
    b = _photo(db, fid, "b.jpg")
    kid = _kw(db, "Park")
    _link(db, b, kid)
    _link(db, a, kid)
    calls = []

    def recorder(photo_id, change_type, value, workspace_id=None, _commit=True):
        calls.append((photo_id, change_type, value, workspace_id, _commit))
        return photo_id == a

    monkeypatch.setattr(db, "queue_change", recorder)
    with caplog.at_level(logging.INFO, logger="db"):
        result = db.queue_location_changes_for_tagged_photos()
    ws = db._ws_id()
    assert calls == [
        (a, "location", "effective", ws, False),
        (b, "location", "effective", ws, False),
    ]
    assert result == {"photos": 2, "queued": 1, "already_queued": 1}
    assert "Queued 1 location change(s) for 2 located photo(s)" in caplog.text


def test_queue_location_changes_requires_workspace(db):
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError):
        db.queue_location_changes_for_tagged_photos()


# -- set / clear photo location -----------------------------------------------------


def test_set_photo_location_validates_keyword(db, fid):
    pid = _photo(db, fid, "a.jpg")
    with pytest.raises(ValueError, match="keyword id 424242 does not exist"):
        db.set_photo_location(pid, 424242)
    general = _kw(db, "Bird", kw_type="general")
    with pytest.raises(ValueError, match=r"has type='general', not 'location'"):
        db.set_photo_location(pid, general)


def test_set_photo_location_replaces_and_commits(db, fid):
    pid = _photo(db, fid, "a.jpg")
    old = _kw(db, "Old")
    new = _kw(db, "New")
    bird = _kw(db, "Bird", kw_type="general")
    _link(db, pid, old, source=None)
    _link(db, pid, bird, source=None)

    db.set_photo_location(pid, new)
    assert not db.conn.in_transaction
    with _reader(db) as other:
        rows = other.execute(
            "SELECT keyword_id, source FROM photo_keywords WHERE photo_id = ? "
            "ORDER BY keyword_id",
            (pid,),
        ).fetchall()
    assert [tuple(r) for r in rows] == [(new, "manual"), (bird, None)]


def test_set_photo_location_tags_through_facade_in_one_transaction(
    db, fid, monkeypatch,
):
    pid = _photo(db, fid, "a.jpg")
    old = _kw(db, "Old")
    new = _kw(db, "New")
    _link(db, pid, old)
    calls = []

    def boom(photo_id, keyword_id, source=None, _commit=True, **kwargs):
        calls.append((photo_id, keyword_id, source, _commit))
        assert db.conn.in_transaction
        raise RuntimeError("tag failed")

    monkeypatch.setattr(db, "tag_photo", boom)
    with pytest.raises(RuntimeError, match="tag failed"):
        db.set_photo_location(pid, new)
    assert calls == [(pid, new, KEYWORD_SOURCE_MANUAL, False)]
    # The DELETE rolled back with the failed tag.
    rows = db.conn.execute(
        "SELECT keyword_id FROM photo_keywords WHERE photo_id = ?", (pid,),
    ).fetchall()
    assert [r[0] for r in rows] == [old]
    assert not db.conn.in_transaction


def test_clear_photo_location_commits_and_keeps_other_keywords(db, fid):
    pid = _photo(db, fid, "a.jpg")
    loc = _kw(db, "Park")
    bird = _kw(db, "Bird", kw_type="general")
    _link(db, pid, loc)
    _link(db, pid, bird)
    db.clear_photo_location(pid)
    assert not db.conn.in_transaction
    with _reader(db) as other:
        rows = other.execute(
            "SELECT keyword_id FROM photo_keywords WHERE photo_id = ?", (pid,),
        ).fetchall()
        assert [r[0] for r in rows] == [bird]
        assert other.execute(
            "SELECT 1 FROM keywords WHERE id = ?", (loc,),
        ).fetchone() is not None


# -- place chains ---------------------------------------------------------------------


def test_upsert_place_chain_requires_place_id(db):
    with pytest.raises(ValueError, match=r"requires details\['place_id'\]"):
        db.upsert_place_chain({"name": "Nowhere"})


def test_upsert_place_chain_commits_and_excludes_existing_leaf(db):
    first = db.upsert_place_chain({
        "place_id": "pl-1", "name": "Nairobi", "lat": -1.3, "lng": 36.8,
        "address_components": [
            {"long_name": "Kenya", "types": ["country"]},
        ],
    })
    assert not db.conn.in_transaction
    with _reader(db) as other:
        row = other.execute(
            "SELECT k.name, k.place_id, parent.name AS parent "
            "FROM keywords k JOIN keywords parent ON parent.id = k.parent_id "
            "WHERE k.id = ?",
            (first,),
        ).fetchone()
    assert tuple(row) == ("Nairobi", "pl-1", "Kenya")

    # Renamed on Google's side: an address component now carries the leaf's
    # stored name, but the existing leaf must not become its own parent.
    db.conn.execute(
        "UPDATE keywords SET name = 'Kenya', parent_id = NULL WHERE id = ?",
        (first,),
    )
    db.conn.commit()
    statements = _trace(db)
    again = db.upsert_place_chain({
        "place_id": "pl-1", "name": "Nairobi", "lat": -1.3, "lng": 36.8,
        "address_components": [{"long_name": "Kenya", "types": ["country"]}],
    })
    assert again == first
    statements = [s.strip() for s in statements]
    assert statements[0] == "SELECT id FROM keywords WHERE place_id = 'pl-1'"
    assert statements[-1] == "COMMIT"
    parent = db.conn.execute(
        "SELECT parent_id FROM keywords WHERE id = ?", (first,),
    ).fetchone()[0]
    assert parent != first


def test_upsert_place_chain_rolls_back_on_failure(db, monkeypatch):
    real = db._upsert_one_keyword
    calls = []

    def fail_on_leaf(**kwargs):
        calls.append(kwargs["name"])
        if kwargs.get("place_id"):
            raise RuntimeError("leaf failed")
        return real(**kwargs)

    monkeypatch.setattr(db, "_upsert_one_keyword", fail_on_leaf)
    with pytest.raises(RuntimeError, match="leaf failed"):
        db.upsert_place_chain({
            "place_id": "pl-2", "name": "Mara",
            "address_components": [{"long_name": "Kenya", "types": ["country"]}],
        })
    assert calls == ["Kenya", "Mara"]
    assert not db.conn.in_transaction
    assert db.conn.execute(
        "SELECT COUNT(*) FROM keywords WHERE name = 'Kenya'"
    ).fetchone()[0] == 0


def test_location_parent_components_skip_noise(db):
    parents = db._location_parent_components(
        [
            "not-a-dict",
            {"long_name": "  ", "types": ["country"]},
            {"long_name": "94107", "types": ["postal_code"]},
            {"long_name": "California", "types": ["administrative_area_level_1"]},
            {"long_name": "USA", "types": ["country"]},
            {"name": "usa", "types": ["country"]},
            {"long_name": "Weird", "types": "country"},
        ],
    )
    assert parents == [{"name": "USA"}, {"name": "California"}]
    assert Database._location_component_rank("nope") is None


def test_get_or_create_text_location_rejects_none_and_commits(db):
    with pytest.raises(ValueError, match="must not be empty"):
        db.get_or_create_text_location(None)
    kid = db.get_or_create_text_location("  Back garden ")
    assert not db.conn.in_transaction
    with _reader(db) as other:
        row = other.execute(
            "SELECT name, type, parent_id FROM keywords WHERE id = ?", (kid,),
        ).fetchone()
    assert tuple(row) == ("Back garden", "location", None)
    assert db.get_or_create_text_location("Back garden") == kid


def test_get_or_create_text_location_matches_case_insensitively(db):
    # ``add_keyword`` dedupes location names case-insensitively; the text
    # path must agree, or one place splits into two keyword rows.
    kid = db.get_or_create_text_location("Paris")
    assert db.get_or_create_text_location("paris") == kid
    assert db.add_keyword("PARIS", kw_type="location") == kid
    assert db.conn.execute(
        "SELECT COUNT(*) FROM keywords WHERE name = 'Paris' COLLATE NOCASE"
    ).fetchone()[0] == 1


def test_get_or_create_text_location_prefers_exact_spelling(db):
    # Legacy catalogs can already hold both spellings; the exact one wins.
    upper = db.conn.execute(
        "INSERT INTO keywords (name, type) VALUES ('Paris', 'location')"
    ).lastrowid
    lower = db.conn.execute(
        "INSERT INTO keywords (name, type) VALUES ('paris', 'location')"
    ).lastrowid
    db.conn.commit()
    assert db.get_or_create_text_location("paris") == lower
    assert db.get_or_create_text_location("Paris") == upper


# -- link_keyword_to_place (stays on Database: pinned provenance writer) ---------


def test_link_keyword_to_place_requires_place_id(db):
    kid = _kw(db, "Somewhere")
    with pytest.raises(ValueError, match=r"requires details\['place_id'\]"):
        db.link_keyword_to_place(kid, {"name": "x"})


def test_link_keyword_to_place_disambiguates_under_a_parent(db):
    kid = _kw(db, "Old text")
    country = _kw(db, "Kenya")
    _kw(db, "Mara", parent_id=country)
    result = db.link_keyword_to_place(kid, {
        "place_id": "ChIJ-abcdefgh12345678", "name": "Mara",
        "address_components": [{"long_name": "Kenya", "types": ["country"]}],
    })
    assert result == {"keyword_id": kid, "merged": False}
    row = db.conn.execute(
        "SELECT name, parent_id FROM keywords WHERE id = ?", (kid,),
    ).fetchone()
    assert tuple(row) == ("Mara (12345678)", country)


# -- legacy taxonomy repair -------------------------------------------------------------


def test_restore_misclassified_ancestor_does_not_commit(db):
    country = _kw(db, "USA")
    state = _kw(db, "California", parent_id=country, kw_type="taxonomy")
    db.conn.execute(
        "UPDATE keywords SET is_species = 1 WHERE id = ?", (state,),
    )
    db.conn.commit()
    pure = _kw(db, "Aves", kw_type="taxonomy")

    assert db._restore_misclassified_location_ancestor(pure) is False
    assert db._restore_misclassified_location_ancestor(state) is False
    assert db._restore_misclassified_location_ancestor(state, allow_leaf=True) is True
    assert db.conn.in_transaction
    row = db.conn.execute(
        "SELECT type, is_species, taxon_id FROM keywords WHERE id = ?", (state,),
    ).fetchone()
    assert tuple(row) == ("location", 0, None)
    with _reader(db) as other:
        assert other.execute(
            "SELECT type FROM keywords WHERE id = ?", (state,),
        ).fetchone()[0] == "taxonomy"
    db.conn.rollback()


def test_repair_misclassified_ancestors_noop_leaves_no_transaction(db, caplog):
    _kw(db, "Aves", kw_type="taxonomy")
    with caplog.at_level(logging.INFO, logger="db"):
        assert db.repair_misclassified_location_ancestors() == 0
    assert not db.conn.in_transaction
    assert "repaired" not in caplog.text
    assert "merged" not in caplog.text


def test_repair_misclassified_ancestors_commits_and_logs(db, caplog):
    country = _kw(db, "USA")
    state = _kw(db, "California", parent_id=country, kw_type="taxonomy")
    county = _kw(db, "Marin", parent_id=state, kw_type="taxonomy")
    _kw(db, "Point Reyes", parent_id=county)

    with caplog.at_level(logging.INFO, logger="db"):
        assert db.repair_misclassified_location_ancestors() == 2
    assert not db.conn.in_transaction
    with _reader(db) as other:
        types = [
            r[0] for r in other.execute(
                "SELECT type FROM keywords WHERE id IN (?, ?) ORDER BY id",
                (state, county),
            ).fetchall()
        ]
    assert types == ["location", "location"]
    assert "repaired 2 misclassified location ancestor keyword(s)" in caplog.text


def test_repair_merges_coordless_duplicate_roots_through_facade(
    db, fid, monkeypatch, caplog,
):
    first = _kw(db, "Kenya")
    second = _kw(db, "Kenya")
    third = _kw(db, "Kenya")
    child = _kw(db, "Mara", parent_id=third)
    pid = _photo(db, fid, "a.jpg")
    _link(db, pid, second)

    merges = []
    real_merge = db._merge_keyword_into

    def recorder(src, dst, *args, **kwargs):
        merges.append((src, dst))
        return real_merge(src, dst, *args, **kwargs)

    monkeypatch.setattr(db, "_merge_keyword_into", recorder)
    with caplog.at_level(logging.INFO, logger="db"):
        assert db.repair_misclassified_location_ancestors() == 0
    assert merges == [(second, first), (third, first)]
    assert not db.conn.in_transaction
    assert "merged 2 duplicate location root keyword(s)" in caplog.text
    with _reader(db) as other:
        assert other.execute(
            "SELECT COUNT(*) FROM keywords WHERE name = 'Kenya'"
        ).fetchone()[0] == 1
        assert other.execute(
            "SELECT parent_id FROM keywords WHERE id = ?", (child,),
        ).fetchone()[0] == first


def test_repair_keeps_place_bearing_root_and_single_coordless_anchor(db):
    placed = _kw(db, "Turkey", place_id="pl-turkey", lat=39.0, lng=35.0)
    anchor = _kw(db, "Turkey")
    assert db._merge_duplicate_location_roots() == 0
    extra = _kw(db, "Turkey")
    assert db._merge_duplicate_location_roots() == 1
    ids = [
        r[0] for r in db.conn.execute(
            "SELECT id FROM keywords WHERE name = 'Turkey' ORDER BY id"
        ).fetchall()
    ]
    assert ids == [placed, anchor]
    assert extra not in ids
    db.conn.rollback()


# -- reverse-geocode cache ------------------------------------------------------------


def test_reverse_geocode_cache_put_commits_and_stamps_time(db, monkeypatch):
    import db as db_module

    monkeypatch.setattr(db_module.time, "time", lambda: 1_700_000_000.9)
    db.reverse_geocode_cache_put(37.77449, -122.41939, "pl", '{"a": 1}')
    assert not db.conn.in_transaction
    with _reader(db) as other:
        row = other.execute(
            "SELECT lat_grid, lng_grid, place_id, response, fetched_at "
            "FROM place_reverse_geocode_cache"
        ).fetchone()
    assert tuple(row) == (37774, -122419, "pl", '{"a": 1}', 1_700_000_000)
    assert db.reverse_geocode_cache_get(37.7745, -122.4185) is None
    assert db.reverse_geocode_cache_get(37.7744, -122.4194) == {
        "place_id": "pl", "response": '{"a": 1}',
    }


def test_reverse_geocode_cache_put_upserts_negative_result(db):
    db.reverse_geocode_cache_put(1.0, 2.0, "pl", "{}")
    db.reverse_geocode_cache_put(1.0001, 2.0001, None, "[]")
    assert db.reverse_geocode_cache_get(1.0, 2.0) == {
        "place_id": None, "response": "[]",
    }
    assert db.conn.execute(
        "SELECT COUNT(*) FROM place_reverse_geocode_cache"
    ).fetchone()[0] == 1
    assert Database._reverse_geocode_grid(-0.0004, 0.0006) == (0, 1)


# -- structure: the location SQL lives in the repository -----------------------

# Database methods whose SQL moved to repositories/locations.py. Each stays on
# Database as a thin wrapper so existing call sites keep working; none may
# reach the connection directly again. ``link_keyword_to_place`` is not here:
# test_keyword_provenance_contract pins it to db.py as a photo_keywords writer.
_DELEGATING_LOCATION_METHODS = (
    "get_photo_location_statuses",
    "get_geolocated_photos",
    "get_assigned_photo_location",
    "_get_photo_location_leaves",
    "get_photo_location_paths",
    "has_pending_location_change",
    "count_photos_with_location",
    "queue_location_changes_for_tagged_photos",
    "get_effective_photo_locations",
    "count_photos_without_coordinates",
    "get_plottable_photo_ids",
    "_restore_misclassified_location_ancestor",
    "repair_misclassified_location_ancestors",
    "_merge_duplicate_location_roots",
    "upsert_place_chain",
    "set_photo_location",
    "clear_photo_location",
    "get_or_create_text_location",
    "reverse_geocode_cache_get",
    "reverse_geocode_cache_put",
)


def _self_attrs(name):
    source = textwrap.dedent(inspect.getsource(getattr(Database, name)))
    fn = ast.parse(source).body[0]
    return fn, {
        node.attr
        for node in ast.walk(fn)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }


@pytest.mark.parametrize("name", _DELEGATING_LOCATION_METHODS)
def test_location_method_delegates_to_repository(name):
    _fn, attrs = _self_attrs(name)
    assert "conn" not in attrs, (
        f"Database.{name} touches self.conn; move the SQL to LocationRepository"
    )
    assert "_location_repository" in attrs, (
        f"Database.{name} no longer delegates to LocationRepository"
    )


@pytest.mark.parametrize(
    ("name", "facade_calls"),
    [
        ("get_geolocated_photos", {"get_folder_subtree_ids", "_build_query_from_rules"}),
        ("get_plottable_photo_ids", {"get_folder_subtree_ids"}),
        ("queue_location_changes_for_tagged_photos", {"queue_change"}),
        (
            "repair_misclassified_location_ancestors",
            {"_restore_misclassified_location_ancestor", "_merge_duplicate_location_roots"},
        ),
        ("_merge_duplicate_location_roots", {"_merge_keyword_into"}),
        ("set_photo_location", {"tag_photo"}),
        ("upsert_place_chain", {"_upsert_location_parent_chain", "_upsert_one_keyword"}),
        ("get_or_create_text_location", {"_upsert_one_keyword"}),
    ],
)
def test_cross_domain_calls_stay_on_the_facade(name, facade_calls):
    """Composition is routed through ``Database`` so its patches still apply."""
    _fn, attrs = _self_attrs(name)
    assert facade_calls <= attrs


def test_set_photo_location_tags_as_an_attribute_call():
    """The provenance contract only recognises ``self.tag_photo(...)`` calls."""
    fn, _attrs = _self_attrs("set_photo_location")
    calls = [
        node for node in ast.walk(fn)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "tag_photo"
    ]
    assert len(calls) == 1
    source = {kw.arg: kw.value for kw in calls[0].keywords}["source"]
    assert isinstance(source, ast.Name) and source.id == "KEYWORD_SOURCE_MANUAL"


def test_repository_builds_without_an_active_workspace(db):
    db.set_active_workspace(None)
    repo = db._location_repository()
    assert repo.conn is db.conn
    assert repo.chunk_size == _SQLITE_PARAM_CHUNK_SIZE
    with pytest.raises(RuntimeError):
        repo.workspace_id_fn()
