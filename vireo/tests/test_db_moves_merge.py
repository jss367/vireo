"""Behavior pins for the moves/merge domain of ``Database``.

Covers the saved move rules, the folder/photo move writers
(``batch_update_photo_folder``, ``move_folder_path``,
``check_filename_collisions``, ``query_move_rule_matches``), the sync-only
grants (``_link_survivor_for_sibling_edits`` / ``get_sync_only_photo_paths``)
and the staged-tree -> archive merge with its ``_*_for_merge`` helpers.

Everything goes through the public ``Database`` API on a real temp database,
so the same tests hold before and after the SQL moves behind the façade. Path
rewrites and merges are destructive, so commit boundaries are pinned with a
second connection, and the helpers that run inside the merge's transaction are
pinned as *not* committing.
"""

import contextlib
import logging
import os
import sqlite3

import pytest
from db import Database


@pytest.fixture
def db(tmp_path):
    d = Database(str(tmp_path / "moves.db"))
    yield d
    d.close()


def _other(db):
    """A second connection on the same file: sees only committed writes."""
    conn = sqlite3.connect(db._db_path)
    conn.row_factory = sqlite3.Row
    return contextlib.closing(conn)


def _folder(db, path, parent_id=None, *, root=False, link=True):
    return db.add_folder(
        path, name=path.rstrip("/").rsplit("/", 1)[-1] or path,
        parent_id=parent_id, workspace_root=root, link_to_workspace=link,
    )


def _photo(db, folder_id, filename, **kw):
    return db.add_photo(
        folder_id=folder_id, filename=filename,
        extension="." + filename.rsplit(".", 1)[-1],
        file_size=kw.pop("file_size", 10), file_mtime=kw.pop("file_mtime", 1.0),
        **kw,
    )


def _queue(db, photo_id, change_type, value, created_at, *, ws=None,
           token=None):
    """Insert a pending change with an explicit timestamp; returns its id."""
    if ws is None:
        ws = db._active_workspace_id
    cur = db.conn.execute(
        "INSERT INTO pending_changes "
        "(photo_id, change_type, value, change_token, created_at, workspace_id) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (photo_id, change_type, value, token, created_at, ws),
    )
    db.conn.commit()
    return cur.lastrowid


def _trace(db):
    statements = []
    db.conn.set_trace_callback(statements.append)
    return statements


def _stop_trace(db):
    db.conn.set_trace_callback(None)


def _kw_ids(db, photo_id):
    return sorted(
        r["keyword_id"] for r in db.conn.execute(
            "SELECT keyword_id FROM photo_keywords WHERE photo_id = ?",
            (photo_id,),
        )
    )


def _pending(db, *photo_ids):
    ph = ",".join("?" for _ in photo_ids)
    return [
        dict(r) for r in db.conn.execute(
            f"SELECT id, photo_id, change_type, value, workspace_id "
            f"FROM pending_changes WHERE photo_id IN ({ph}) ORDER BY id",
            photo_ids,
        )
    ]


# -- move rules --------------------------------------------------------------


def test_create_move_rule_commits_and_serializes_criteria(db):
    rid = db.create_move_rule("Keepers", "/dest", {"rating_min": 4, "flag": "flagged"})
    assert isinstance(rid, int)
    assert not db.conn.in_transaction
    with _other(db) as other:
        row = other.execute("SELECT * FROM move_rules WHERE id = ?", (rid,)).fetchone()
    assert row["name"] == "Keepers"
    assert row["destination"] == "/dest"
    assert row["criteria"] == '{"rating_min": 4, "flag": "flagged"}'
    assert row["created_at"] is not None
    assert row["last_run_at"] is None


def test_get_move_rule_returns_row_or_none(db):
    rid = db.create_move_rule("A", "/a", {})
    row = db.get_move_rule(rid)
    assert isinstance(row, sqlite3.Row)
    assert row["id"] == rid and row["criteria"] == "{}"
    assert db.get_move_rule(rid + 999) is None


def test_list_move_rules_orders_by_name(db):
    assert db.list_move_rules() == []
    b = db.create_move_rule("b", "/b", {})
    a = db.create_move_rule("a", "/a", {})
    rows = db.list_move_rules()
    assert [r["id"] for r in rows] == [a, b]
    assert all(isinstance(r, sqlite3.Row) for r in rows)


def test_update_move_rule_sets_only_given_fields_and_commits(db):
    rid = db.create_move_rule("orig", "/orig", {"flag": "flagged"})
    db.update_move_rule(rid, name="renamed")
    assert not db.conn.in_transaction
    with _other(db) as other:
        row = other.execute("SELECT * FROM move_rules WHERE id = ?", (rid,)).fetchone()
    assert (row["name"], row["destination"], row["criteria"]) == (
        "renamed", "/orig", '{"flag": "flagged"}')

    db.update_move_rule(rid, destination="/new", criteria={"rating_min": 2})
    row = db.get_move_rule(rid)
    assert (row["name"], row["destination"], row["criteria"]) == (
        "renamed", "/new", '{"rating_min": 2}')

    # An explicit None is a value, not "not provided".
    db.update_move_rule(rid, criteria=None)
    assert db.get_move_rule(rid)["criteria"] == "null"


def test_update_move_rule_without_fields_runs_no_sql(db):
    rid = db.create_move_rule("orig", "/orig", {})
    statements = _trace(db)
    try:
        assert db.update_move_rule(rid) is None
    finally:
        _stop_trace(db)
    assert statements == []
    assert not db.conn.in_transaction


def test_update_move_rule_statement_shape(db):
    rid = db.create_move_rule("orig", "/orig", {})
    statements = _trace(db)
    try:
        db.update_move_rule(rid, name="n", destination="/d", criteria={})
    finally:
        _stop_trace(db)
    assert [s.strip() for s in statements if s.strip() not in ("BEGIN", "COMMIT")] == [
        f"UPDATE move_rules SET name = 'n', destination = '/d', criteria = '{{}}' "
        f"WHERE id = {rid}",
    ]


def test_delete_move_rule_commits(db):
    rid = db.create_move_rule("gone", "/g", {})
    keep = db.create_move_rule("kept", "/k", {})
    db.delete_move_rule(rid)
    assert not db.conn.in_transaction
    with _other(db) as other:
        ids = [r["id"] for r in other.execute("SELECT id FROM move_rules")]
    assert ids == [keep]
    # Deleting a missing id is a quiet no-op.
    db.delete_move_rule(rid)


def test_touch_move_rule_stamps_last_run_at_and_commits(db):
    rid = db.create_move_rule("r", "/r", {})
    other_id = db.create_move_rule("s", "/s", {})
    db.touch_move_rule(rid)
    assert not db.conn.in_transaction
    with _other(db) as other:
        rows = {r["id"]: r["last_run_at"] for r in other.execute(
            "SELECT id, last_run_at FROM move_rules")}
    assert rows[rid] is not None
    assert rows[other_id] is None


# -- batch_update_photo_folder / check_filename_collisions -------------------


def test_batch_update_photo_folder_empty_runs_no_sql(db):
    statements = _trace(db)
    try:
        assert db.batch_update_photo_folder([], 1) is None
    finally:
        _stop_trace(db)
    assert statements == []


def test_batch_update_photo_folder_moves_only_listed_photos_and_commits(db):
    src = _folder(db, "/src", root=True)
    dst = _folder(db, "/dst", root=True)
    a = _photo(db, src, "a.jpg")
    b = _photo(db, src, "b.jpg")
    c = _photo(db, src, "c.jpg")
    db.batch_update_photo_folder((a, c), dst)
    assert not db.conn.in_transaction
    with _other(db) as other:
        folders = {r["id"]: r["folder_id"] for r in other.execute(
            "SELECT id, folder_id FROM photos")}
    assert folders == {a: dst, b: src, c: dst}


def test_check_filename_collisions(db):
    src = _folder(db, "/src", root=True)
    dst = _folder(db, "/dst", root=True)
    elsewhere = _folder(db, "/else", root=True)
    a = _photo(db, src, "a.jpg")
    b = _photo(db, src, "B.jpg")
    c = _photo(db, src, "c.jpg")
    _photo(db, dst, "a.jpg")
    _photo(db, dst, "b.jpg")  # different case: not a collision
    _photo(db, elsewhere, "c.jpg")  # other folder: not a collision

    statements = _trace(db)
    try:
        assert db.check_filename_collisions([], dst) == []
    finally:
        _stop_trace(db)
    assert statements == []

    result = db.check_filename_collisions([a, b, c], dst)
    assert result == [{"photo_id": a, "filename": "a.jpg"}]
    assert isinstance(result[0], dict)


# -- move_folder_path --------------------------------------------------------


def test_move_folder_path_missing_folder_is_noop(db):
    statements = _trace(db)
    try:
        assert db.move_folder_path(12345, "/new") is None
    finally:
        _stop_trace(db)
    assert [s for s in statements if s.lstrip().upper().startswith("UPDATE")] == []
    assert not db.conn.in_transaction


def test_move_folder_path_rebases_subtree_and_provenance_and_commits(db):
    parent_old = _folder(db, "/old", root=True)
    root = _folder(db, "/old/shoot", parent_old)
    child = _folder(db, "/old/shoot/day1", root)
    grandchild = _folder(db, "/old/shoot/day1/raw", child)
    sibling = _folder(db, "/old/shoot2", parent_old)  # prefix lookalike
    parent_new = _folder(db, "/new", root=True)
    pid = _photo(db, sibling, "x.jpg")
    other = _photo(db, sibling, "y.jpg")
    untouched = _photo(db, sibling, "z.jpg")
    db.conn.execute(
        "UPDATE photos SET last_move_source_folder_path = ? WHERE id = ?",
        ("/old/shoot", pid))
    db.conn.execute(
        "UPDATE photos SET last_move_source_folder_path = ? WHERE id = ?",
        ("/old/shoot/day1", other))
    db.conn.execute(
        "UPDATE photos SET last_move_source_folder_path = ? WHERE id = ?",
        ("/old/shoot2", untouched))
    db.conn.commit()

    db.move_folder_path(root, "/new/renamed", new_name="renamed")

    assert not db.conn.in_transaction
    with _other(db) as conn:
        rows = {r["id"]: dict(r) for r in conn.execute(
            "SELECT id, path, name, parent_id FROM folders")}
        prov = {r["id"]: r["last_move_source_folder_path"] for r in conn.execute(
            "SELECT id, last_move_source_folder_path FROM photos")}
    assert rows[root] == {"id": root, "path": "/new/renamed", "name": "renamed",
                          "parent_id": parent_new}
    assert rows[child]["path"] == "/new/renamed/day1"
    assert rows[child]["name"] == "day1"
    assert rows[child]["parent_id"] == root
    assert rows[grandchild]["path"] == "/new/renamed/day1/raw"
    assert rows[grandchild]["parent_id"] == child
    assert rows[sibling]["path"] == "/old/shoot2"
    assert prov == {pid: "/new/renamed", other: "/new/renamed/day1",
                    untouched: "/old/shoot2"}


def test_move_folder_path_keeps_name_without_new_name(db):
    root = _folder(db, "/a/b", root=True)
    db.move_folder_path(root, "/a/c")
    row = db.conn.execute(
        "SELECT path, name FROM folders WHERE id = ?", (root,)).fetchone()
    assert (row["path"], row["name"]) == ("/a/c", "b")


def test_move_folder_path_to_same_path_skips_provenance_rewrite(db):
    root = _folder(db, "/same", root=True)
    pid = _photo(db, root, "a.jpg")
    db.conn.execute(
        "UPDATE photos SET last_move_source_folder_path = '/same' WHERE id = ?",
        (pid,))
    db.conn.commit()
    statements = _trace(db)
    try:
        db.move_folder_path(root, "/same", new_name="Same")
    finally:
        _stop_trace(db)
    assert not any("last_move_source_folder_path" in s for s in statements)
    row = db.conn.execute("SELECT name FROM folders WHERE id = ?", (root,)).fetchone()
    assert row["name"] == "Same"
    assert not db.conn.in_transaction


def test_move_folder_path_relinks_through_the_facade_after_paths_change(
        db, monkeypatch):
    root = _folder(db, "/p/q", root=True)
    child = _folder(db, "/p/q/r", root)
    seen = []
    real = db._relink_parents_by_path

    def recorder(ids):
        paths = {
            r["id"]: r["path"] for r in db.conn.execute(
                "SELECT id, path FROM folders WHERE id IN (?, ?)", (root, child))
        }
        seen.append((list(ids), paths))
        return real(ids)

    monkeypatch.setattr(db, "_relink_parents_by_path", recorder)
    db.move_folder_path(root, "/z/q")
    assert seen == [([root, child], {root: "/z/q", child: "/z/q/r"})]


def test_move_folder_path_rolls_back_nothing_but_commits_once(db):
    root = _folder(db, "/m", root=True)
    _folder(db, "/m/n", root)
    statements = _trace(db)
    try:
        db.move_folder_path(root, "/k")
    finally:
        _stop_trace(db)
    assert [s.strip() for s in statements].count("COMMIT") == 1


# -- _newest_location_change_key --------------------------------------------


def test_newest_location_change_key(db):
    fid = _folder(db, "/f", root=True)
    pid = _photo(db, fid, "a.jpg")
    assert db._newest_location_change_key(pid) is None
    _queue(db, pid, "rating", "3", "2030-01-01 00:00:00")
    assert db._newest_location_change_key(pid) is None

    older = _queue(db, pid, "location", "", "2026-01-01 00:00:00")
    assert db._newest_location_change_key(pid) == ("2026-01-01 00:00:00", older)
    newer = _queue(db, pid, "location", "", "2026-02-01 00:00:00")
    tie = _queue(db, pid, "location", "", "2026-02-01 00:00:00")
    assert db._newest_location_change_key(pid) == ("2026-02-01 00:00:00", tie)
    assert tie > newer


def test_newest_location_change_key_null_created_at_sorts_as_empty(db):
    fid = _folder(db, "/f", root=True)
    pid = _photo(db, fid, "a.jpg")
    cid = _queue(db, pid, "location", "", None)
    assert db._newest_location_change_key(pid) == ("", cid)


# -- _move_location_state_for_merge -----------------------------------------


@pytest.fixture
def two_photos(db):
    fid = _folder(db, "/f", root=True)
    loser = _photo(db, fid, "loser.jpg")
    survivor = _photo(db, fid, "survivor.jpg")
    return loser, survivor


def test_move_location_state_without_loser_change_keeps_survivor(db, two_photos):
    loser, survivor = two_photos
    here = db.add_keyword("Here", kw_type="location")
    there = db.add_keyword("There", kw_type="location")
    db.tag_photo(loser, here)
    db.tag_photo(survivor, there)
    assert db._move_location_state_for_merge(loser, survivor) is False
    assert _kw_ids(db, survivor) == [there]


def test_move_location_state_newer_survivor_change_wins(db, two_photos):
    loser, survivor = two_photos
    here = db.add_keyword("Here", kw_type="location")
    there = db.add_keyword("There", kw_type="location")
    db.tag_photo(loser, here)
    db.tag_photo(survivor, there)
    _queue(db, loser, "location", "", "2026-01-01 00:00:00")
    _queue(db, survivor, "location", "", "2026-01-02 00:00:00")
    assert db._move_location_state_for_merge(loser, survivor) is False
    assert _kw_ids(db, survivor) == [there]


def test_move_location_state_newer_loser_replaces_location_tags_only(
        db, two_photos):
    loser, survivor = two_photos
    here = db.add_keyword("Here", kw_type="location")
    there = db.add_keyword("There", kw_type="location")
    general = db.add_keyword("Backlit", kw_type="general")
    db.tag_photo(loser, here)
    db.tag_photo(survivor, there)
    db.tag_photo(survivor, general)
    _queue(db, survivor, "location", "", "2026-01-01 00:00:00")
    _queue(db, loser, "location", "", "2026-01-02 00:00:00")

    assert db._move_location_state_for_merge(loser, survivor) is True
    # Runs inside the caller's transaction: nothing committed yet.
    assert db.conn.in_transaction
    assert sorted(_kw_ids(db, survivor)) == sorted([here, general])
    assert db.conn.execute(
        "SELECT source FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?",
        (survivor, here),
    ).fetchone()["source"] == "manual"
    # The loser keeps its own tags; the caller deletes the loser row.
    assert _kw_ids(db, loser) == [here]
    db.conn.rollback()
    assert sorted(_kw_ids(db, survivor)) == sorted([there, general])


def test_move_location_state_untagged_loser_clears_survivor(db, two_photos):
    loser, survivor = two_photos
    there = db.add_keyword("There", kw_type="location")
    db.tag_photo(survivor, there)
    _queue(db, loser, "location", "", "2026-01-02 00:00:00")
    assert db._move_location_state_for_merge(loser, survivor) is True
    assert _kw_ids(db, survivor) == []


def test_move_location_state_tags_through_the_facade(db, two_photos, monkeypatch):
    loser, survivor = two_photos
    here = db.add_keyword("Here", kw_type="location")
    db.tag_photo(loser, here)
    _queue(db, loser, "location", "", "2026-01-02 00:00:00")
    calls = []
    real = db.tag_photo

    def recorder(*args, **kwargs):
        calls.append((args, kwargs))
        return real(*args, **kwargs)

    monkeypatch.setattr(db, "tag_photo", recorder)
    db._move_location_state_for_merge(loser, survivor)
    assert calls == [((survivor, here), {"_commit": False})]


# -- keyword reconciliation --------------------------------------------------


def test_reconcile_keyword_edits_without_rows_returns_zero(db, two_photos,
                                                           monkeypatch):
    loser, survivor = two_photos
    _queue(db, loser, "keyword_remove_flat", "Heron", "2026-01-01 00:00:00")
    calls = []
    monkeypatch.setattr(db, "_carry_keyword_associations_for_merge",
                        lambda *a: calls.append(a))
    assert db._reconcile_conflicting_keyword_edits(loser, survivor) == 0
    assert calls == []


def test_reconcile_keyword_edits_empty_values_still_carry_empty_grouping(
        db, two_photos, monkeypatch):
    loser, survivor = two_photos
    _queue(db, loser, "keyword_add", "", "2026-01-01 00:00:00")
    _queue(db, survivor, "keyword_remove", None, "2026-01-02 00:00:00")
    calls = []
    monkeypatch.setattr(db, "_carry_keyword_associations_for_merge",
                        lambda *a: calls.append(a))
    assert db._reconcile_conflicting_keyword_edits(loser, survivor) == 0
    assert calls == [(loser, survivor, {})]
    assert len(_pending(db, loser, survivor)) == 2


def test_reconcile_keyword_edits_newer_remove_drops_older_add(
        db, two_photos, caplog):
    loser, survivor = two_photos
    heron = db.add_keyword("Heron", kw_type="general")
    db.tag_photo(survivor, heron)
    add = _queue(db, survivor, "keyword_add", "Heron", "2026-01-01 00:00:00")
    remove = _queue(db, loser, "keyword_remove", "heron", "2026-01-02 00:00:00")
    unrelated = _queue(db, loser, "keyword_add", "Egret", "2026-01-03 00:00:00")

    with caplog.at_level(logging.INFO):
        dropped = db._reconcile_conflicting_keyword_edits(loser, survivor)

    assert dropped == 1
    assert db.conn.in_transaction
    assert [r["id"] for r in _pending(db, loser, survivor)] == [remove, unrelated]
    # The loser's winning remove is carried onto the survivor's association.
    assert _kw_ids(db, survivor) == []
    assert any(
        "Merge reconciled opposing 'heron' edits on photos "
        f"{loser}/{survivor}: dropped 1 older queue row(s)" in r.getMessage()
        for r in caplog.records
    )
    db.conn.rollback()
    assert {r["id"] for r in _pending(db, loser, survivor)} == {
        add, remove, unrelated}


def test_reconcile_keyword_edits_same_side_rows_do_not_compete(db, two_photos):
    loser, survivor = two_photos
    _queue(db, loser, "keyword_add", "Heron", "2026-01-01 00:00:00")
    _queue(db, survivor, "keyword_add", "Heron", "2026-01-02 00:00:00")
    assert db._reconcile_conflicting_keyword_edits(loser, survivor) == 0
    assert len(_pending(db, loser, survivor)) == 2


def test_reconcile_keyword_edits_rename_pair_is_one_keep_atom(db, two_photos):
    loser, survivor = two_photos
    # Rename pair on the survivor in one workspace (newest row 01-03) beats an
    # older opposing remove on the loser.
    pair_add = _queue(db, survivor, "keyword_add", "Heron", "2026-01-01 00:00:00")
    pair_rm = _queue(db, survivor, "keyword_remove", "heron", "2026-01-03 00:00:00")
    lone_rm = _queue(db, loser, "keyword_remove", "HERON", "2026-01-02 00:00:00")
    assert db._reconcile_conflicting_keyword_edits(loser, survivor) == 1
    assert [r["id"] for r in _pending(db, loser, survivor)] == [pair_add, pair_rm]
    assert lone_rm not in [r["id"] for r in _pending(db, loser, survivor)]


def test_carry_keyword_associations_add_and_remove(db, two_photos, monkeypatch):
    loser, survivor = two_photos
    heron = db.add_keyword("Heron", kw_type="general")
    egret = db.add_keyword("Egret", kw_type="general")
    db.tag_photo(loser, heron)
    db.tag_photo(survivor, egret)
    _queue(db, loser, "keyword_add", "heron", "2026-01-02 00:00:00")
    _queue(db, loser, "keyword_remove", "egret", "2026-01-02 00:00:00")
    tagged, untagged = [], []
    real_tag, real_untag = db.tag_photo, db.untag_photo
    monkeypatch.setattr(db, "tag_photo", lambda *a, **k: (
        tagged.append((a, k)), real_tag(*a, **k))[1])
    monkeypatch.setattr(db, "untag_photo", lambda *a, **k: (
        untagged.append((a, k)), real_untag(*a, **k))[1])

    assert db._reconcile_conflicting_keyword_edits(loser, survivor) == 0
    assert tagged == [((survivor, heron), {"_commit": False})]
    assert untagged == [((survivor, egret), {"_commit": False})]
    assert _kw_ids(db, survivor) == [heron]


def test_carry_keyword_associations_skips_when_survivor_is_newer(db, two_photos):
    loser, survivor = two_photos
    heron = db.add_keyword("Heron", kw_type="general")
    db.tag_photo(loser, heron)
    _queue(db, loser, "keyword_add", "Heron", "2026-01-01 00:00:00")
    _queue(db, survivor, "keyword_add", "Heron", "2026-01-02 00:00:00")
    db._reconcile_conflicting_keyword_edits(loser, survivor)
    assert _kw_ids(db, survivor) == []


def test_carry_keyword_associations_ignores_rows_already_deleted(db, two_photos):
    loser, survivor = two_photos
    heron = db.add_keyword("Heron", kw_type="general")
    db.tag_photo(loser, heron)
    gone = _queue(db, loser, "keyword_add", "Heron", "2026-01-01 00:00:00")
    by_key = {
        "heron": {
            loser: {db._active_workspace_id: {
                "add": [{"id": gone, "change_type": "keyword_add",
                         "created_at": "2026-01-01 00:00:00"}],
                "remove": []}},
            survivor: {},
        },
    }
    db.conn.execute("DELETE FROM pending_changes WHERE id = ?", (gone,))
    db._carry_keyword_associations_for_merge(loser, survivor, by_key)
    assert _kw_ids(db, survivor) == []


def test_photo_keyword_ids_matching_uses_the_ascii_match_key(db, two_photos):
    loser, _ = two_photos
    upper = db.add_keyword("GREAT HERON", kw_type="general")
    accented = db.add_keyword("Éclair", kw_type="general")
    other = db.add_keyword("Egret", kw_type="general")
    for kid in (upper, accented, other):
        db.tag_photo(loser, kid)
    assert db._photo_keyword_ids_matching(loser, "great heron") == [upper]
    assert db._photo_keyword_ids_matching(loser, "Éclair") == [accented]
    assert db._photo_keyword_ids_matching(loser, "éclair") == []
    assert db._photo_keyword_ids_matching(loser, "nothing") == []


# -- review-state transfer ---------------------------------------------------


def _gps(db, photo_id, fingerprint, reviewed_at):
    db.conn.execute(
        "INSERT INTO location_gps_reviews (photo_id, fingerprint, reviewed_at) "
        "VALUES (?, ?, ?)", (photo_id, fingerprint, reviewed_at))
    db.conn.commit()


def _gps_row(db, photo_id):
    row = db.conn.execute(
        "SELECT fingerprint, reviewed_at FROM location_gps_reviews "
        "WHERE photo_id = ?", (photo_id,)).fetchone()
    return tuple(row) if row else None


def test_transfer_gps_review_takes_newest_and_prefers_survivor_on_tie(
        db, two_photos):
    loser, survivor = two_photos
    _gps(db, loser, "L1", "2026-01-02")
    db._transfer_gps_review_for_merge(loser, survivor)
    assert _gps_row(db, survivor) == ("L1", "2026-01-02")
    assert db.conn.in_transaction
    db.conn.rollback()

    _gps(db, survivor, "S1", "2026-01-02")
    db._transfer_gps_review_for_merge(loser, survivor)
    assert _gps_row(db, survivor) == ("S1", "2026-01-02")

    db.conn.execute(
        "UPDATE location_gps_reviews SET reviewed_at = '2026-01-03' "
        "WHERE photo_id = ?", (loser,))
    db._transfer_gps_review_for_merge(loser, survivor)
    assert _gps_row(db, survivor) == ("L1", "2026-01-03")


def test_transfer_review_state_without_loser_rows_is_zero(db, two_photos):
    loser, survivor = two_photos
    _queue(db, survivor, "rating", "4", "2026-01-01 00:00:00")
    assert db._transfer_review_state_for_merge(loser, survivor) == 0
    assert db.conn.execute(
        "SELECT rating FROM photos WHERE id = ?", (survivor,)).fetchone()[0] == 0


def test_transfer_review_state_newest_rating_and_flag_win(db, two_photos, caplog):
    loser, survivor = two_photos
    old_rating = _queue(db, survivor, "rating", "2", "2026-01-01 00:00:00")
    new_rating = _queue(db, loser, "rating", "5", "2026-01-02 00:00:00")
    new_flag = _queue(db, survivor, "flag", "", "2026-01-04 00:00:00")
    old_flag = _queue(db, loser, "flag", "flagged", "2026-01-03 00:00:00")

    with caplog.at_level(logging.INFO):
        dropped = db._transfer_review_state_for_merge(loser, survivor)

    assert dropped == 2
    assert db.conn.in_transaction
    remaining = {r["id"] for r in _pending(db, loser, survivor)}
    assert remaining == {new_rating, new_flag}
    assert old_rating not in remaining and old_flag not in remaining
    row = db.conn.execute(
        "SELECT rating, flag FROM photos WHERE id = ?", (survivor,)).fetchone()
    assert (row["rating"], row["flag"]) == (5, "none")
    assert any(
        f"Merge reconciled queued review state on photos {loser}/{survivor}: "
        "dropped 2 older queue row(s)" in r.getMessage()
        for r in caplog.records
    )


def test_transfer_review_state_malformed_rating_leaves_column(db, two_photos):
    loser, survivor = two_photos
    db.conn.execute("UPDATE photos SET rating = 3 WHERE id = ?", (survivor,))
    db.conn.commit()
    _queue(db, survivor, "rating", "4", "2026-01-01 00:00:00")
    bad = _queue(db, loser, "rating", "five", "2026-01-02 00:00:00")
    assert db._transfer_review_state_for_merge(loser, survivor) == 1
    assert [r["id"] for r in _pending(db, loser, survivor)] == [bad]
    assert db.conn.execute(
        "SELECT rating FROM photos WHERE id = ?", (survivor,)).fetchone()[0] == 3


def test_transfer_review_state_same_photo_rows_are_not_adjudicated(
        db, two_photos, caplog):
    loser, survivor = two_photos
    _queue(db, loser, "rating", "1", "2026-01-01 00:00:00")
    _queue(db, loser, "rating", "4", "2026-01-02 00:00:00")
    with caplog.at_level(logging.INFO):
        assert db._transfer_review_state_for_merge(loser, survivor) == 0
    assert len(_pending(db, loser)) == 2
    assert db.conn.execute(
        "SELECT rating FROM photos WHERE id = ?", (survivor,)).fetchone()[0] == 4
    assert not any("queued review state" in r.getMessage() for r in caplog.records)


def test_transfer_review_state_composes_through_the_facade(
        db, two_photos, monkeypatch):
    loser, survivor = two_photos
    calls = []
    monkeypatch.setattr(db, "_transfer_gps_review_for_merge",
                        lambda *a: calls.append(("gps",) + a))
    monkeypatch.setattr(db, "_transfer_edit_recipe_for_merge",
                        lambda *a: calls.append(("recipe",) + a) or 3)
    assert db._transfer_review_state_for_merge(loser, survivor) == 3
    assert calls == [("gps", loser, survivor), ("recipe", loser, survivor)]


def _recipe(db, photo_id):
    row = db.conn.execute(
        "SELECT recipe_json FROM photo_edit_recipes WHERE photo_id = ?",
        (photo_id,)).fetchone()
    return row[0] if row else None


def test_transfer_edit_recipe(db, two_photos):
    loser, survivor = two_photos
    assert db._transfer_edit_recipe_for_merge(loser, survivor) == 0

    db.conn.execute(
        "INSERT INTO photo_edit_recipes (photo_id, recipe_json, updated_at) "
        "VALUES (?, '{\"old\": 1}', '2020-01-01')", (survivor,))
    db.conn.commit()
    older = _queue(db, survivor, "edit_recipe", '{"s": 1}', "2026-01-01 00:00:00")
    newer = _queue(db, loser, "edit_recipe", '{"l": 2}', "2026-01-02 00:00:00")
    assert db._transfer_edit_recipe_for_merge(loser, survivor) == 1
    assert db.conn.in_transaction
    assert [r["id"] for r in _pending(db, loser, survivor)] == [newer]
    assert older not in [r["id"] for r in _pending(db, loser, survivor)]
    assert _recipe(db, survivor) == '{"l": 2}'
    assert db.conn.execute(
        "SELECT updated_at FROM photo_edit_recipes WHERE photo_id = ?",
        (survivor,)).fetchone()[0] != "2020-01-01"

    # A newest empty value is the "recipe cleared" edit.
    _queue(db, loser, "edit_recipe", "", "2026-01-03 00:00:00")
    assert db._transfer_edit_recipe_for_merge(loser, survivor) == 0
    assert _recipe(db, survivor) is None


def test_transfer_edit_recipe_inserts_missing_survivor_row(db, two_photos):
    loser, survivor = two_photos
    _queue(db, loser, "edit_recipe", '{"x": 1}', "2026-01-02 00:00:00")
    assert db._transfer_edit_recipe_for_merge(loser, survivor) == 0
    assert _recipe(db, survivor) == '{"x": 1}'


# -- sync-only grants --------------------------------------------------------


def test_link_survivor_for_sibling_edits(db, caplog):
    active = db._active_workspace_id
    sibling = db.create_workspace("Sibling")
    fid = _folder(db, "/arch", root=True)
    pid = _photo(db, fid, "a.jpg")

    assert db._link_survivor_for_sibling_edits(sibling, pid + 999) is False
    # The active workspace already sees the folder: no grant needed.
    assert db._link_survivor_for_sibling_edits(active, pid) is False

    with caplog.at_level(logging.INFO):
        assert db._link_survivor_for_sibling_edits(sibling, pid) is True
    assert db.conn.in_transaction  # caller owns the commit
    assert [tuple(r) for r in db.conn.execute(
        "SELECT workspace_id, photo_id FROM workspace_sync_only_photos")] == [
        (sibling, pid)]
    assert db.conn.execute(
        "SELECT COUNT(*) FROM workspace_folders WHERE workspace_id = ?",
        (sibling,)).fetchone()[0] == 0
    assert any(
        f"Granted workspace {sibling} sync-only access to photo {pid}"
        in r.getMessage() for r in caplog.records)
    assert db._link_survivor_for_sibling_edits(sibling, pid) is False


def test_get_sync_only_photo_paths(db):
    active = db._active_workspace_id
    sibling = db.create_workspace("Sibling")
    ok = _folder(db, "/ok", root=True)
    gone = _folder(db, "/gone", root=True)
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (gone,))
    a = _photo(db, ok, "a.jpg")
    b = _photo(db, gone, "b.jpg")
    c = _photo(db, ok, "c.jpg")
    db.conn.executemany(
        "INSERT INTO workspace_sync_only_photos (workspace_id, photo_id) "
        "VALUES (?, ?)", [(active, a), (active, b), (sibling, c)])
    db.conn.commit()

    assert db.get_sync_only_photo_paths() == {a: "/ok"}
    assert db.get_sync_only_photo_paths(sibling) == {c: "/ok"}
    db.set_active_workspace(None)
    assert db.get_sync_only_photo_paths(sibling) == {c: "/ok"}
    with pytest.raises(RuntimeError, match="No active workspace set"):
        db.get_sync_only_photo_paths()


def test_get_sync_only_photo_paths_legacy_folder_grants(db):
    sibling = db.create_workspace("Sibling")
    granted = _folder(db, "/granted", root=True)
    moved_to = _folder(db, "/moved", root=True)
    direct = _photo(db, granted, "direct.jpg")
    moved = _photo(db, moved_to, "moved.jpg")
    stray = _photo(db, moved_to, "stray.jpg")  # no pending edge: not authorized
    explicit = _photo(db, moved_to, "explicit.jpg")
    db.conn.execute(
        "UPDATE photos SET last_move_source_folder_path = '/granted' "
        "WHERE id IN (?, ?)", (moved, stray))
    db.conn.execute(
        "CREATE TABLE workspace_sync_only_folders "
        "(workspace_id INTEGER, folder_id INTEGER)")
    db.conn.execute(
        "INSERT INTO workspace_sync_only_folders VALUES (?, ?)",
        (sibling, granted))
    db.conn.execute(
        "INSERT INTO workspace_sync_only_photos (workspace_id, photo_id) "
        "VALUES (?, ?)", (sibling, explicit))
    db.conn.commit()
    for pid in (direct, moved, explicit):
        _queue(db, pid, "rating", "3", "2026-01-01 00:00:00", ws=sibling)

    assert db.get_sync_only_photo_paths(sibling) == {
        direct: "/granted", moved: "/moved", explicit: "/moved"}


# -- query_move_rule_matches -------------------------------------------------


@pytest.fixture
def rule_photos(db):
    fid = _folder(db, "/lib", root=True)
    other = _folder(db, "/lib2", root=True)
    p = {
        "hi": _photo(db, fid, "hi.jpg", timestamp="2026-01-01T00:00:00"),
        "lo": _photo(db, fid, "lo.jpg", timestamp="2026-03-01T00:00:00"),
        "other": _photo(db, other, "o.jpg", timestamp="2025-01-01T00:00:00"),
    }
    db.conn.execute("UPDATE photos SET rating = 5, flag = 'flagged' WHERE id = ?",
                    (p["hi"],))
    db.conn.execute("UPDATE photos SET rating = 1 WHERE id = ?", (p["lo"],))
    db.conn.commit()
    p["fid"], p["other_fid"] = fid, other
    return p


def test_query_move_rule_matches_criteria(db, rule_photos):
    p = rule_photos
    assert sorted(db.query_move_rule_matches({})) == sorted(
        [p["hi"], p["lo"], p["other"]])
    assert db.query_move_rule_matches({"rating_min": 3}) == [p["hi"]]
    assert db.query_move_rule_matches({"flag": "flagged"}) == [p["hi"]]
    assert sorted(db.query_move_rule_matches({"folder_ids": [p["fid"]]})) == sorted(
        [p["hi"], p["lo"]])
    assert sorted(db.query_move_rule_matches({"folder_ids": []})) == sorted(
        [p["hi"], p["lo"], p["other"]])
    assert sorted(db.query_move_rule_matches(
        {"imported_before": "2026-02-01"})) == sorted([p["hi"], p["other"]])
    assert db.query_move_rule_matches(
        {"rating_min": 1, "imported_before": "2026-02-01",
         "folder_ids": [p["fid"]]}) == [p["hi"]]


def test_query_move_rule_matches_species(db, rule_photos):
    p = rule_photos
    robin = db.add_keyword("Robin", is_species=True, kw_type="taxonomy")
    wren = db.add_keyword("Wren", is_species=True, kw_type="taxonomy")
    plain = db.add_keyword("Robin Nest", kw_type="general")
    db.conn.execute("UPDATE keywords SET is_species = 1 WHERE id IN (?, ?)",
                    (robin, wren))
    db.conn.commit()
    db.tag_photo(p["hi"], robin)
    db.tag_photo(p["hi"], wren)
    db.tag_photo(p["lo"], plain)
    assert db.query_move_rule_matches({"species": ["Robin", "Wren"]}) == [p["hi"]]
    assert db.query_move_rule_matches({"species": ["Robin Nest"]}) == []
    assert len(db.query_move_rule_matches({"species": []})) == 3


def test_query_move_rule_matches_scopes_to_active_workspace_and_visible_folders(
        db, rule_photos):
    p = rule_photos
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?",
                    (p["other_fid"],))
    db.conn.commit()
    assert p["other"] not in db.query_move_rule_matches({})
    empty_ws = db.create_workspace("Empty")
    db.set_active_workspace(empty_ws)
    assert db.query_move_rule_matches({}) == []
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError, match="No active workspace set"):
        db.query_move_rule_matches({})


def test_query_move_rule_matches_reads_config_only_for_has_predictions(
        db, rule_photos, monkeypatch):
    p = rule_photos
    calls = []
    real = db.get_effective_config

    def recorder(cfg):
        calls.append(cfg)
        return real(cfg)

    monkeypatch.setattr(db, "get_effective_config", recorder)
    db.query_move_rule_matches({"rating_min": 1})
    assert calls == []

    det = db.save_detections(p["hi"], [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.5,
         "category": "animal"}], detector_model="MDV6")[0]
    db.add_prediction(det, species="Robin", confidence=0.9, model="bioclip")
    assert db.query_move_rule_matches({"has_predictions": True}) == [p["hi"]]
    assert len(calls) == 1
    # A workspace override above the detection's confidence hides it.
    db.update_workspace(db._active_workspace_id,
                        config_overrides={"detector_confidence": 0.6})
    assert db.query_move_rule_matches({"has_predictions": True}) == []
    assert p["hi"] in db.query_move_rule_matches({"has_predictions": False})


# -- merge_staged_tree_into_archive -----------------------------------------


EMPTY_COUNTS = {"new_photos": 0, "new_folders": 0, "merged_folders": 0,
                "already_present": 0, "dropped_photo_ids": [],
                "preserved_edit_count": 0,
                "preserved_off_staging_identities": []}


def test_merge_missing_staged_root_returns_empty_counts_before_workspace(
        db, monkeypatch):
    calls = []
    monkeypatch.setattr(db, "update_folder_counts", lambda: calls.append(1))
    db.set_active_workspace(None)
    assert db.merge_staged_tree_into_archive(99999, "/arch") == EMPTY_COUNTS
    assert calls == []


def test_merge_requires_active_workspace_when_staged_root_exists(db):
    stage = _folder(db, "/stage", root=True)
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError, match="No active workspace set"):
        db.merge_staged_tree_into_archive(stage, "/arch")


class _CacheRecorder:
    def __init__(self):
        self.calls = []

    def invalidate_workspaces(self, db_path, workspace_ids):
        self.calls.append((db_path, list(workspace_ids)))

    def __getattr__(self, name):  # other cache hooks the setup may hit
        return lambda *a, **k: None


def _collision_tree(db, tmp_path, *, same_hash=True):
    """Archive folder holding ``dup.raf`` + a staged tree colliding with it."""
    arch = tmp_path / "arch"
    day = arch / "day"
    day.mkdir(parents=True)
    (day / "dup.raf").write_bytes(b"bytes")
    base = _folder(db, str(arch), root=True)
    day_id = _folder(db, str(day), base)
    survivor = _photo(db, day_id, "dup.raf", file_hash="H")
    stage = tmp_path / "stage"
    stage_root = _folder(db, str(stage), link=False)
    stage_day = _folder(db, str(stage / "day"), stage_root, link=False)
    staged = _photo(db, stage_day, "dup.raf",
                    file_hash="H" if same_hash else "OTHER")
    fresh = _photo(db, stage_day, "fresh.raf")
    new_leaf = _folder(db, str(stage / "day2"), stage_root, link=False)
    newer = _photo(db, new_leaf, "n.raf")
    return {"arch": str(arch), "base": base, "day": day_id,
            "survivor": survivor, "stage_root": stage_root,
            "stage_day": stage_day, "staged": staged, "fresh": fresh,
            "new_leaf": new_leaf, "newer": newer}


def test_merge_commits_counts_and_invalidates_after_reparent(
        db, tmp_path, monkeypatch):
    t = _collision_tree(db, tmp_path)
    ws = db._active_workspace_id
    _queue(db, t["staged"], "rating", "4", "2026-01-01 00:00:00", token="tok")
    recorder = _CacheRecorder()
    order = []
    real_counts = db.update_folder_counts

    def counts_recorder():
        order.append(("counts", db.conn.in_transaction))
        return real_counts()

    monkeypatch.setattr(db, "_new_images_cache", recorder)
    monkeypatch.setattr(db, "update_folder_counts", counts_recorder)

    counts = db.merge_staged_tree_into_archive(t["stage_root"], t["arch"])

    assert counts == {
        "new_photos": 2, "new_folders": 1, "merged_folders": 2,
        "already_present": 1, "dropped_photo_ids": [t["staged"]],
        "preserved_edit_count": 1,
        "preserved_off_staging_identities": ["tok"],
    }
    assert (db._db_path, [ws]) in recorder.calls
    assert order == [("counts", False)]
    with _other(db) as other:
        folders = {r["path"]: r["id"] for r in other.execute(
            "SELECT id, path FROM folders")}
        photos = {r["id"]: r["folder_id"] for r in other.execute(
            "SELECT id, folder_id FROM photos")}
        pending = [tuple(r) for r in other.execute(
            "SELECT photo_id, change_token FROM pending_changes")]
    assert not any(p.startswith(str(tmp_path / "stage")) for p in folders)
    # The merge keeps the archive root's separator (``\`` on Windows).
    assert folders[os.path.join(t["arch"], "day2")] == t["new_leaf"]
    assert photos == {t["survivor"]: t["day"], t["fresh"]: t["day"],
                      t["newer"]: t["new_leaf"]}
    assert pending == [(t["survivor"], "tok")]


def test_merge_rolls_back_every_reparent_on_error(db, tmp_path, monkeypatch):
    t = _collision_tree(db, tmp_path)
    recorder = _CacheRecorder()
    counts_calls = []
    monkeypatch.setattr(db, "_new_images_cache", recorder)

    def boom(losing_id, surviving_id):
        raise RuntimeError("transfer exploded")

    monkeypatch.setattr(db, "_transfer_review_state_for_merge", boom)
    monkeypatch.setattr(db, "update_folder_counts",
                        lambda: counts_calls.append(1))
    before_folders = [tuple(r) for r in db.conn.execute(
        "SELECT id, path, parent_id FROM folders ORDER BY id")]
    before_photos = [tuple(r) for r in db.conn.execute(
        "SELECT id, folder_id FROM photos ORDER BY id")]

    with pytest.raises(RuntimeError, match="transfer exploded"):
        db.merge_staged_tree_into_archive(t["stage_root"], t["arch"])

    assert not db.conn.in_transaction
    assert [tuple(r) for r in db.conn.execute(
        "SELECT id, path, parent_id FROM folders ORDER BY id")] == before_folders
    assert [tuple(r) for r in db.conn.execute(
        "SELECT id, folder_id FROM photos ORDER BY id")] == before_photos
    assert counts_calls == []
    assert recorder.calls == []


def test_merge_routes_real_collision_helpers_through_the_facade(
        db, tmp_path, monkeypatch):
    t = _collision_tree(db, tmp_path)
    calls = []
    for name in ("_move_location_state_for_merge",
                 "_reconcile_conflicting_keyword_edits",
                 "_transfer_review_state_for_merge"):
        real = getattr(db, name)

        def recorder(losing, surviving, _name=name, _real=real):
            calls.append((_name, losing, surviving))
            return _real(losing, surviving)

        monkeypatch.setattr(db, name, recorder)
    db.merge_staged_tree_into_archive(t["stage_root"], t["arch"])
    assert calls == [
        ("_move_location_state_for_merge", t["staged"], t["survivor"]),
        ("_reconcile_conflicting_keyword_edits", t["staged"], t["survivor"]),
        ("_transfer_review_state_for_merge", t["staged"], t["survivor"]),
    ]


def test_merge_routes_phantom_helpers_through_the_facade(
        db, tmp_path, monkeypatch):
    t = _collision_tree(db, tmp_path, same_hash=False)
    calls = []
    for name in ("_move_location_state_for_merge",
                 "_reconcile_conflicting_keyword_edits",
                 "_transfer_review_state_for_merge"):
        real = getattr(db, name)

        def recorder(losing, surviving, _name=name, _real=real):
            calls.append((_name, losing, surviving))
            return _real(losing, surviving)

        monkeypatch.setattr(db, name, recorder)
    counts = db.merge_staged_tree_into_archive(t["stage_root"], t["arch"])
    assert calls == [
        ("_move_location_state_for_merge", t["survivor"], t["staged"]),
        ("_reconcile_conflicting_keyword_edits", t["survivor"], t["staged"]),
        ("_transfer_review_state_for_merge", t["survivor"], t["staged"]),
    ]
    assert counts["dropped_photo_ids"] == [t["survivor"]]
    assert db.conn.execute(
        "SELECT folder_id FROM photos WHERE id = ?", (t["staged"],)
    ).fetchone()[0] == t["day"]


def test_merge_links_sibling_survivors_through_the_facade(
        db, tmp_path, monkeypatch):
    t = _collision_tree(db, tmp_path)
    sibling = db.create_workspace("Sibling")
    _queue(db, t["staged"], "flag", "flagged", "2026-01-01 00:00:00", ws=sibling)
    calls = []
    real = db._link_survivor_for_sibling_edits

    def recorder(ws_id, photo_id):
        calls.append((ws_id, photo_id, db.conn.execute(
            "SELECT folder_id FROM photos WHERE id = ?", (photo_id,)
        ).fetchone()[0]))
        return real(ws_id, photo_id)

    monkeypatch.setattr(db, "_link_survivor_for_sibling_edits", recorder)
    counts = db.merge_staged_tree_into_archive(t["stage_root"], t["arch"])
    assert calls == [(sibling, t["survivor"], t["day"])]
    # Sibling rows are preserved but not reported as active-ws identities.
    assert counts["preserved_edit_count"] == 1
    assert counts["preserved_off_staging_identities"] == []
    assert db.get_sync_only_photo_paths(sibling) == {
        t["survivor"]: str(tmp_path / "arch" / "day")}


def test_merge_links_archive_base_through_the_facade(db, monkeypatch):
    ws = db._active_workspace_id
    base = _folder(db, "/arch", link=False)
    stage = _folder(db, "/stage", link=False)
    _photo(db, stage, "a.raf")
    calls = []
    real = db.add_workspace_folder

    def recorder(workspace_id, folder_id, *, is_root=True, **kw):
        calls.append((workspace_id, folder_id, is_root))
        return real(workspace_id, folder_id, is_root=is_root, **kw)

    monkeypatch.setattr(db, "add_workspace_folder", recorder)
    counts = db.merge_staged_tree_into_archive(stage, "/arch")
    assert calls == [(ws, base, True)]
    assert counts["merged_folders"] == 1 and counts["new_photos"] == 1


def test_merge_missing_target_parent_logs_and_leaves_parent_null(db, caplog):
    _folder(db, "/arch", root=True)
    stage = _folder(db, "/stage", link=False)
    # A gap in the staged tree: /stage/a has no folder row.
    deep = _folder(db, "/stage/a/b", stage, link=False)
    _photo(db, deep, "x.raf")
    with caplog.at_level(logging.WARNING):
        db.merge_staged_tree_into_archive(stage, "/arch")
    row = db.conn.execute(
        "SELECT path, parent_id FROM folders WHERE id = ?", (deep,)).fetchone()
    assert (row["path"], row["parent_id"]) == ("/arch/a/b", None)
    assert any(
        "merge_staged_tree_into_archive: no folder row for target parent "
        "'/arch/a' of '/arch/a/b'; leaving parent_id NULL" in r.getMessage()
        for r in caplog.records)


def test_merge_missing_target_parent_falls_back_to_processed_parent(db, caplog):
    _folder(db, "/arch", root=True)
    stage = _folder(db, "/stage", link=False)
    mid = _folder(db, "/stage/a", stage, link=False)
    deep = _folder(db, "/stage/a/b", mid, link=False)
    _photo(db, deep, "x.raf")
    # Make the repointed /arch/a row unfindable by path afterwards, so the
    # child has to fall back to the id recorded for its processed parent.
    db.conn.execute(
        "CREATE TEMP TRIGGER hide_mid AFTER UPDATE OF path ON folders "
        "WHEN NEW.path = '/arch/a' BEGIN "
        "UPDATE folders SET path = '/arch/a-hidden' WHERE id = NEW.id; END")
    with caplog.at_level(logging.WARNING):
        db.merge_staged_tree_into_archive(stage, "/arch")
    row = db.conn.execute(
        "SELECT path, parent_id FROM folders WHERE id = ?", (deep,)).fetchone()
    assert (row["path"], row["parent_id"]) == ("/arch/a/b", mid)
    assert any(
        "merge_staged_tree_into_archive: no folder row for target parent "
        f"'/arch/a' of '/arch/a/b'; falling back to id {mid}" in r.getMessage()
        for r in caplog.records)


def _ws_links(db, ws):
    return {
        r["path"]: r["is_root"] for r in db.conn.execute(
            "SELECT f.path, wf.is_root FROM workspace_folders wf "
            "JOIN folders f ON f.id = wf.folder_id WHERE wf.workspace_id = ?",
            (ws,),
        )
    }


def test_merge_materializes_missing_intermediates_top_down(db):
    ws = db._active_workspace_id
    base = _folder(db, "/arch", root=True)
    stage = _folder(db, "/stage", link=False)
    _photo(db, stage, "a.raf")

    counts = db.merge_staged_tree_into_archive(stage, "/arch/2026/05/shoot")

    assert counts["new_folders"] == 1 and counts["new_photos"] == 1
    with _other(db) as other:
        rows = {r["path"]: dict(r) for r in other.execute(
            "SELECT id, path, name, parent_id FROM folders")}
    y, m = rows["/arch/2026"], rows["/arch/2026/05"]
    assert (y["name"], y["parent_id"]) == ("2026", base)
    assert (m["name"], m["parent_id"]) == ("05", y["id"])
    assert rows["/arch/2026/05/shoot"]["id"] == stage
    assert rows["/arch/2026/05/shoot"]["parent_id"] == m["id"]
    links = _ws_links(db, ws)
    assert links["/arch"] == 1
    assert links["/arch/2026"] == 0 and links["/arch/2026/05"] == 0
    assert links["/arch/2026/05/shoot"] == 0


def test_merge_flips_missing_archive_base_and_target_to_ok(db):
    base = _folder(db, "/arch", root=True)
    day = _folder(db, "/arch/day", base)
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id IN (?, ?)",
                    (base, day))
    db.conn.commit()
    stage = _folder(db, "/stage", link=False)
    stage_day = _folder(db, "/stage/day", stage, link=False)
    _photo(db, stage_day, "a.raf")
    db.merge_staged_tree_into_archive(stage, "/arch")
    with _other(db) as other:
        statuses = {r["id"]: r["status"] for r in other.execute(
            "SELECT id, status FROM folders WHERE id IN (?, ?)", (base, day))}
    assert statuses == {base: "ok", day: "ok"}


def test_merge_scoped_workspace_prunes_instead_of_linking_broad_base(db):
    ws = db._active_workspace_id
    base = _folder(db, "/arch", link=False)
    usa = _folder(db, "/arch/USA", base, link=False)
    _folder(db, "/arch/USA/2026", usa, root=True)
    sibling_year = _folder(db, "/arch/USA/2027", usa, link=False)
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id, is_root) "
        "VALUES (?, ?, 0)", (ws, sibling_year))
    db.conn.commit()
    stage = _folder(db, "/stage", link=False)
    _photo(db, stage, "a.raf")

    db.merge_staged_tree_into_archive(stage, "/arch/USA")

    links = _ws_links(db, ws)
    assert "/arch/USA" not in links and "/arch" not in links
    assert "/arch/USA/2027" not in links
    assert links["/arch/USA/2026"] == 1


def test_merge_new_path_roots_deepest_tracked_ancestor(db):
    ws = db._active_workspace_id
    other_ws = db.create_workspace("Other")
    db.set_active_workspace(other_ws)
    _folder(db, "/photos", root=True)
    db.set_active_workspace(ws)
    stage = _folder(db, "/stage", link=False)
    _photo(db, stage, "a.raf")
    db.merge_staged_tree_into_archive(stage, "/photos/sub/new")
    links = _ws_links(db, ws)
    assert links["/photos"] == 1
    assert links["/photos/sub"] == 0
    assert links["/photos/sub/new"] == 0


def test_merge_new_path_under_descendant_root_prunes_ancestor(db):
    ws = db._active_workspace_id
    top = _folder(db, "/photos", link=False)
    _folder(db, "/photos/2026", top, root=True)
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id, is_root) "
        "VALUES (?, ?, 0)", (ws, top))
    db.conn.commit()
    stage = _folder(db, "/stage", root=True)
    _photo(db, stage, "a.raf")
    db.merge_staged_tree_into_archive(stage, "/photos/2027")
    links = _ws_links(db, ws)
    assert "/photos" not in links
    # Not covered by any workspace root: the staged link is dropped.
    assert "/photos/2027" not in links
    assert links["/photos/2026"] == 1


def test_merge_intra_staged_case_alias_drops_later_row(db, monkeypatch):
    import move

    monkeypatch.setattr(move, "_case_insensitive_root", lambda p: "/")
    base = _folder(db, "/arch", root=True)
    _folder(db, "/arch/day", base)
    stage = _folder(db, "/stage", link=False)
    stage_day = _folder(db, "/stage/day", stage, link=False)
    first = _photo(db, stage_day, "IMG.raf")
    second = _photo(db, stage_day, "img.RAF")
    _queue(db, second, "rating", "2", "2026-01-01 00:00:00", token="t2")
    counts = db.merge_staged_tree_into_archive(stage, "/arch")
    assert counts["already_present"] == 1
    assert counts["dropped_photo_ids"] == [second]
    assert counts["preserved_edit_count"] == 1
    assert counts["preserved_off_staging_identities"] == []
    assert [tuple(r) for r in db.conn.execute(
        "SELECT photo_id, change_token FROM pending_changes")] == [(first, "t2")]


def test_merge_phantom_sibling_edits_link_staged_survivor(db, tmp_path):
    t = _collision_tree(db, tmp_path, same_hash=False)
    sibling = db.create_workspace("Sibling")
    _queue(db, t["survivor"], "rating", "3", "2026-01-01 00:00:00", ws=sibling)
    counts = db.merge_staged_tree_into_archive(t["stage_root"], t["arch"])
    assert counts["preserved_edit_count"] == 1
    assert db.get_sync_only_photo_paths(sibling) == {
        t["staged"]: str(tmp_path / "arch" / "day")}


# -- structure ---------------------------------------------------------------

MOVED = [
    "create_move_rule", "get_move_rule", "list_move_rules", "update_move_rule",
    "delete_move_rule", "touch_move_rule", "batch_update_photo_folder",
    "move_folder_path", "_newest_location_change_key",
    "_move_location_state_for_merge", "_reconcile_conflicting_keyword_edits",
    "_carry_keyword_associations_for_merge", "_photo_keyword_ids_matching",
    "_transfer_gps_review_for_merge", "_transfer_review_state_for_merge",
    "_transfer_edit_recipe_for_merge", "_link_survivor_for_sibling_edits",
    "get_sync_only_photo_paths", "merge_staged_tree_into_archive",
    "check_filename_collisions", "query_move_rule_matches",
]


def _method_ast(name):
    import ast
    import inspect
    import textwrap

    source = textwrap.dedent(inspect.getsource(getattr(Database, name)))
    return ast.parse(source).body[0]


def _self_attrs(fn):
    import ast

    return {
        node.attr for node in ast.walk(fn)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name) and node.value.id == "self"
    }


@pytest.mark.parametrize("name", MOVED)
def test_moves_merge_method_delegates_to_repository(name):
    attrs = _self_attrs(_method_ast(name))
    assert "conn" not in attrs, (
        f"Database.{name} touches self.conn; move the SQL to MovesMergeRepository")
    assert "_moves_merge_repository" in attrs, (
        f"Database.{name} no longer delegates to MovesMergeRepository")


def test_keyword_retags_stay_on_the_facade():
    """``test_keyword_provenance_contract`` only sees ``self.tag_photo(...)``."""
    import ast

    import repositories.moves_merge as repo_module

    for name, expected in (
        ("_move_location_state_for_merge", {"tag_photo"}),
        ("_carry_keyword_associations_for_merge", {"tag_photo", "untag_photo"}),
    ):
        called = {
            node.func.attr for node in ast.walk(_method_ast(name))
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "self"
        }
        assert expected <= called, name
    tree = ast.parse(open(repo_module.__file__, encoding="utf-8").read())
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            callee = func.attr if isinstance(func, ast.Attribute) else getattr(
                func, "id", None)
            assert callee not in {"tag_photo", "untag_photo"}, (
                "keyword re-tagging must stay on Database so the provenance "
                "contract sees it")


def test_merge_wires_every_callback_to_the_facade():
    import ast

    expected = {
        "workspace_id_fn": "_ws_id",
        "root_ancestor_exists": "_active_ws_root_ancestor_exists",
        "root_descendant_exists": "_active_ws_root_descendant_exists",
        "prune_nonroot_links_outside_roots": "_prune_ws_nonroot_links_outside_roots",
        "materialize_workspace_descendants": "_materialize_workspace_descendants",
        "add_workspace_folder": "add_workspace_folder",
        "add_workspace_folder_no_commit": "_add_workspace_folder_no_commit",
        "move_location_state": "_move_location_state_for_merge",
        "reconcile_keyword_edits": "_reconcile_conflicting_keyword_edits",
        "transfer_review_state": "_transfer_review_state_for_merge",
        "link_survivor_for_sibling_edits": "_link_survivor_for_sibling_edits",
        "update_folder_counts": "update_folder_counts",
    }
    call = next(
        node for node in ast.walk(_method_ast("merge_staged_tree_into_archive"))
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        and node.func.attr == "merge_staged_tree_into_archive"
    )
    wired = {
        kw.arg: kw.value.attr for kw in call.keywords
        if isinstance(kw.value, ast.Attribute)
        and isinstance(kw.value.value, ast.Name) and kw.value.value.id == "self"
    }
    assert wired == expected
    others = {kw.arg for kw in call.keywords} - set(expected)
    assert others == {"case_insensitive_root", "invalidate_new_images"}


@pytest.mark.parametrize("name,callback,target", [
    ("move_folder_path", "relink_parents_by_path", "_relink_parents_by_path"),
    ("_reconcile_conflicting_keyword_edits", "carry_keyword_associations",
     "_carry_keyword_associations_for_merge"),
    ("_transfer_review_state_for_merge", "transfer_gps_review",
     "_transfer_gps_review_for_merge"),
    ("_transfer_review_state_for_merge", "transfer_edit_recipe",
     "_transfer_edit_recipe_for_merge"),
])
def test_helper_callbacks_route_through_the_facade(name, callback, target):
    import ast

    wired = {
        kw.arg: ast.unparse(kw.value)
        for node in ast.walk(_method_ast(name)) if isinstance(node, ast.Call)
        for kw in node.keywords
    }
    assert wired[callback] == f"self.{target}"


def test_facade_signatures_are_unchanged():
    import inspect

    from repositories import UNSET

    expected = {
        "create_move_rule": "(self, name, destination, criteria)",
        "get_move_rule": "(self, rule_id)",
        "list_move_rules": "(self)",
        "delete_move_rule": "(self, rule_id)",
        "touch_move_rule": "(self, rule_id)",
        "batch_update_photo_folder": "(self, photo_ids, target_folder_id)",
        "move_folder_path": "(self, folder_id, new_path, new_name=None)",
        "_newest_location_change_key": "(self, photo_id)",
        "_move_location_state_for_merge": "(self, losing_id, surviving_id)",
        "_reconcile_conflicting_keyword_edits": "(self, losing_id, surviving_id)",
        "_carry_keyword_associations_for_merge": "(self, losing_id, surviving_id, by_key)",
        "_photo_keyword_ids_matching": "(self, photo_id, match_key)",
        "_transfer_gps_review_for_merge": "(self, losing_id, surviving_id)",
        "_transfer_review_state_for_merge": "(self, losing_id, surviving_id)",
        "_transfer_edit_recipe_for_merge": "(self, losing_id, surviving_id)",
        "_link_survivor_for_sibling_edits": "(self, workspace_id, photo_id)",
        "get_sync_only_photo_paths": "(self, workspace_id=None)",
        "merge_staged_tree_into_archive": "(self, staged_root_id, archive_path)",
        "check_filename_collisions": "(self, photo_ids, target_folder_id)",
        "query_move_rule_matches": "(self, criteria)",
    }
    for name, sig in expected.items():
        assert str(inspect.signature(getattr(Database, name))) == sig, name
    params = inspect.signature(Database.update_move_rule).parameters
    assert list(params) == ["self", "rule_id", "name", "destination", "criteria"]
    assert all(params[p].default is UNSET for p in ("name", "destination", "criteria"))
