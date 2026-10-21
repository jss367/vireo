"""Behavior pins for the moves/merge domain of ``Database``.

Covers the saved move rules, the folder/photo move writers
(``moves_merge.batch_update_photo_folder``, ``moves_merge.move_folder_path``,
``moves_merge.filename_collisions``, ``query_move_rule_matches``), the sync-only
grants (``moves_merge.link_survivor_for_sibling_edits`` / ``get_sync_only_photo_paths``)
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
from repositories.folders import FolderRepository
from repositories.moves_merge import MovesMergeRepository
from repositories.workspace_folders import WorkspaceFolderRepository
from services import photo_ingest


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
    return photo_ingest.add_photo(
        db, folder_id=folder_id, filename=filename,
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
    rid = db.moves_merge.create_rule("Keepers", "/dest", {"rating_min": 4, "flag": "flagged"})
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
    rid = db.moves_merge.create_rule("A", "/a", {})
    row = db.moves_merge.get_rule(rid)
    assert isinstance(row, sqlite3.Row)
    assert row["id"] == rid and row["criteria"] == "{}"
    assert db.moves_merge.get_rule(rid + 999) is None


def test_list_move_rules_orders_by_name(db):
    assert db.moves_merge.list_rules() == []
    b = db.moves_merge.create_rule("b", "/b", {})
    a = db.moves_merge.create_rule("a", "/a", {})
    rows = db.moves_merge.list_rules()
    assert [r["id"] for r in rows] == [a, b]
    assert all(isinstance(r, sqlite3.Row) for r in rows)


def test_update_move_rule_sets_only_given_fields_and_commits(db):
    rid = db.moves_merge.create_rule("orig", "/orig", {"flag": "flagged"})
    db.moves_merge.update_rule(rid, name="renamed")
    assert not db.conn.in_transaction
    with _other(db) as other:
        row = other.execute("SELECT * FROM move_rules WHERE id = ?", (rid,)).fetchone()
    assert (row["name"], row["destination"], row["criteria"]) == (
        "renamed", "/orig", '{"flag": "flagged"}')

    db.moves_merge.update_rule(rid, destination="/new", criteria={"rating_min": 2})
    row = db.moves_merge.get_rule(rid)
    assert (row["name"], row["destination"], row["criteria"]) == (
        "renamed", "/new", '{"rating_min": 2}')

    # An explicit None is a value, not "not provided".
    db.moves_merge.update_rule(rid, criteria=None)
    assert db.moves_merge.get_rule(rid)["criteria"] == "null"


def test_update_move_rule_without_fields_runs_no_sql(db):
    rid = db.moves_merge.create_rule("orig", "/orig", {})
    statements = _trace(db)
    try:
        assert db.moves_merge.update_rule(rid) is None
    finally:
        _stop_trace(db)
    assert statements == []
    assert not db.conn.in_transaction


def test_update_move_rule_statement_shape(db):
    rid = db.moves_merge.create_rule("orig", "/orig", {})
    statements = _trace(db)
    try:
        db.moves_merge.update_rule(rid, name="n", destination="/d", criteria={})
    finally:
        _stop_trace(db)
    assert [s.strip() for s in statements if s.strip() not in ("BEGIN", "COMMIT")] == [
        f"UPDATE move_rules SET name = 'n', destination = '/d', criteria = '{{}}' "
        f"WHERE id = {rid}",
    ]


def test_delete_move_rule_commits(db):
    rid = db.moves_merge.create_rule("gone", "/g", {})
    keep = db.moves_merge.create_rule("kept", "/k", {})
    db.moves_merge.delete_rule(rid)
    assert not db.conn.in_transaction
    with _other(db) as other:
        ids = [r["id"] for r in other.execute("SELECT id FROM move_rules")]
    assert ids == [keep]
    # Deleting a missing id is a quiet no-op.
    db.moves_merge.delete_rule(rid)


def test_touch_move_rule_stamps_last_run_at_and_commits(db):
    rid = db.moves_merge.create_rule("r", "/r", {})
    other_id = db.moves_merge.create_rule("s", "/s", {})
    db.moves_merge.touch_rule(rid)
    assert not db.conn.in_transaction
    with _other(db) as other:
        rows = {r["id"]: r["last_run_at"] for r in other.execute(
            "SELECT id, last_run_at FROM move_rules")}
    assert rows[rid] is not None
    assert rows[other_id] is None


# -- moves_merge.batch_update_photo_folder / moves_merge.filename_collisions -------------------


def test_batch_update_photo_folder_empty_runs_no_sql(db):
    statements = _trace(db)
    try:
        assert db.moves_merge.batch_update_photo_folder([], 1) is None
    finally:
        _stop_trace(db)
    assert statements == []


def test_batch_update_photo_folder_moves_only_listed_photos_and_commits(db):
    src = _folder(db, "/src", root=True)
    dst = _folder(db, "/dst", root=True)
    a = _photo(db, src, "a.jpg")
    b = _photo(db, src, "b.jpg")
    c = _photo(db, src, "c.jpg")
    db.moves_merge.batch_update_photo_folder((a, c), dst)
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
        assert db.moves_merge.filename_collisions([], dst) == []
    finally:
        _stop_trace(db)
    assert statements == []

    result = db.moves_merge.filename_collisions([a, b, c], dst)
    assert result == [{"photo_id": a, "filename": "a.jpg"}]
    assert isinstance(result[0], dict)


# -- moves_merge.move_folder_path --------------------------------------------------------


def test_move_folder_path_missing_folder_is_noop(db):
    statements = _trace(db)
    try:
        assert db.moves_merge.move_folder_path(12345, "/new") is None
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

    db.moves_merge.move_folder_path(root, "/new/renamed", new_name="renamed")

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
    db.moves_merge.move_folder_path(root, "/a/c")
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
        db.moves_merge.move_folder_path(root, "/same", new_name="Same")
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
    real = FolderRepository.relink_parents_by_path

    def recorder(self, ids):
        paths = {
            r["id"]: r["path"] for r in db.conn.execute(
                "SELECT id, path FROM folders WHERE id IN (?, ?)", (root, child))
        }
        seen.append((list(ids), paths))
        return real(self, ids)

    monkeypatch.setattr(FolderRepository, "relink_parents_by_path", recorder)
    db.moves_merge.move_folder_path(root, "/z/q")
    assert seen == [([root, child], {root: "/z/q", child: "/z/q/r"})]


def test_move_folder_path_rolls_back_nothing_but_commits_once(db):
    root = _folder(db, "/m", root=True)
    _folder(db, "/m/n", root)
    statements = _trace(db)
    try:
        db.moves_merge.move_folder_path(root, "/k")
    finally:
        _stop_trace(db)
    assert [s.strip() for s in statements].count("COMMIT") == 1


# -- moves_merge.newest_location_change_key --------------------------------------------


def test_newest_location_change_key(db):
    fid = _folder(db, "/f", root=True)
    pid = _photo(db, fid, "a.jpg")
    assert db.moves_merge.newest_location_change_key(pid) is None
    _queue(db, pid, "rating", "3", "2030-01-01 00:00:00")
    assert db.moves_merge.newest_location_change_key(pid) is None

    older = _queue(db, pid, "location", "", "2026-01-01 00:00:00")
    assert db.moves_merge.newest_location_change_key(pid) == ("2026-01-01 00:00:00", older)
    newer = _queue(db, pid, "location", "", "2026-02-01 00:00:00")
    tie = _queue(db, pid, "location", "", "2026-02-01 00:00:00")
    assert db.moves_merge.newest_location_change_key(pid) == ("2026-02-01 00:00:00", tie)
    assert tie > newer


def test_newest_location_change_key_null_created_at_sorts_as_empty(db):
    fid = _folder(db, "/f", root=True)
    pid = _photo(db, fid, "a.jpg")
    cid = _queue(db, pid, "location", "", None)
    assert db.moves_merge.newest_location_change_key(pid) == ("", cid)


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
    assert db.moves_merge.reconcile_conflicting_keyword_edits(loser, survivor) == 0
    assert calls == []


def test_reconcile_keyword_edits_empty_values_still_carry_empty_grouping(
        db, two_photos, monkeypatch):
    loser, survivor = two_photos
    _queue(db, loser, "keyword_add", "", "2026-01-01 00:00:00")
    _queue(db, survivor, "keyword_remove", None, "2026-01-02 00:00:00")
    calls = []
    monkeypatch.setattr(db, "_carry_keyword_associations_for_merge",
                        lambda *a: calls.append(a))
    assert db.moves_merge.reconcile_conflicting_keyword_edits(loser, survivor) == 0
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
        dropped = db.moves_merge.reconcile_conflicting_keyword_edits(loser, survivor)

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
    assert db.moves_merge.reconcile_conflicting_keyword_edits(loser, survivor) == 0
    assert len(_pending(db, loser, survivor)) == 2


def test_reconcile_keyword_edits_rename_pair_is_one_keep_atom(db, two_photos):
    loser, survivor = two_photos
    # Rename pair on the survivor in one workspace (newest row 01-03) beats an
    # older opposing remove on the loser.
    pair_add = _queue(db, survivor, "keyword_add", "Heron", "2026-01-01 00:00:00")
    pair_rm = _queue(db, survivor, "keyword_remove", "heron", "2026-01-03 00:00:00")
    lone_rm = _queue(db, loser, "keyword_remove", "HERON", "2026-01-02 00:00:00")
    assert db.moves_merge.reconcile_conflicting_keyword_edits(loser, survivor) == 1
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

    assert db.moves_merge.reconcile_conflicting_keyword_edits(loser, survivor) == 0
    assert tagged == [((survivor, heron), {"_commit": False})]
    assert untagged == [((survivor, egret), {"_commit": False})]
    assert _kw_ids(db, survivor) == [heron]


def test_carry_keyword_associations_skips_when_survivor_is_newer(db, two_photos):
    loser, survivor = two_photos
    heron = db.add_keyword("Heron", kw_type="general")
    db.tag_photo(loser, heron)
    _queue(db, loser, "keyword_add", "Heron", "2026-01-01 00:00:00")
    _queue(db, survivor, "keyword_add", "Heron", "2026-01-02 00:00:00")
    db.moves_merge.reconcile_conflicting_keyword_edits(loser, survivor)
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
    assert db.moves_merge.photo_keyword_ids_matching(loser, "great heron") == [upper]
    assert db.moves_merge.photo_keyword_ids_matching(loser, "Éclair") == [accented]
    assert db.moves_merge.photo_keyword_ids_matching(loser, "éclair") == []
    assert db.moves_merge.photo_keyword_ids_matching(loser, "nothing") == []


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
    db.moves_merge.transfer_gps_review(loser, survivor)
    assert _gps_row(db, survivor) == ("L1", "2026-01-02")
    assert db.conn.in_transaction
    db.conn.rollback()

    _gps(db, survivor, "S1", "2026-01-02")
    db.moves_merge.transfer_gps_review(loser, survivor)
    assert _gps_row(db, survivor) == ("S1", "2026-01-02")

    db.conn.execute(
        "UPDATE location_gps_reviews SET reviewed_at = '2026-01-03' "
        "WHERE photo_id = ?", (loser,))
    db.moves_merge.transfer_gps_review(loser, survivor)
    assert _gps_row(db, survivor) == ("L1", "2026-01-03")


def test_transfer_review_state_without_loser_rows_is_zero(db, two_photos):
    loser, survivor = two_photos
    _queue(db, survivor, "rating", "4", "2026-01-01 00:00:00")
    assert db.moves_merge.transfer_review_state(loser, survivor) == 0
    assert db.conn.execute(
        "SELECT rating FROM photos WHERE id = ?", (survivor,)).fetchone()[0] == 0


def test_transfer_review_state_newest_rating_and_flag_win(db, two_photos, caplog):
    loser, survivor = two_photos
    old_rating = _queue(db, survivor, "rating", "2", "2026-01-01 00:00:00")
    new_rating = _queue(db, loser, "rating", "5", "2026-01-02 00:00:00")
    new_flag = _queue(db, survivor, "flag", "", "2026-01-04 00:00:00")
    old_flag = _queue(db, loser, "flag", "flagged", "2026-01-03 00:00:00")

    with caplog.at_level(logging.INFO):
        dropped = db.moves_merge.transfer_review_state(loser, survivor)

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
    assert db.moves_merge.transfer_review_state(loser, survivor) == 1
    assert [r["id"] for r in _pending(db, loser, survivor)] == [bad]
    assert db.conn.execute(
        "SELECT rating FROM photos WHERE id = ?", (survivor,)).fetchone()[0] == 3


def test_transfer_review_state_same_photo_rows_are_not_adjudicated(
        db, two_photos, caplog):
    loser, survivor = two_photos
    _queue(db, loser, "rating", "1", "2026-01-01 00:00:00")
    _queue(db, loser, "rating", "4", "2026-01-02 00:00:00")
    with caplog.at_level(logging.INFO):
        assert db.moves_merge.transfer_review_state(loser, survivor) == 0
    assert len(_pending(db, loser)) == 2
    assert db.conn.execute(
        "SELECT rating FROM photos WHERE id = ?", (survivor,)).fetchone()[0] == 4
    assert not any("queued review state" in r.getMessage() for r in caplog.records)


def test_transfer_review_state_composes_through_the_facade(
        db, two_photos, monkeypatch):
    loser, survivor = two_photos
    calls = []
    monkeypatch.setattr(MovesMergeRepository, "transfer_gps_review",
                        lambda self, *a: calls.append(("gps",) + a))
    monkeypatch.setattr(MovesMergeRepository, "transfer_edit_recipe",
                        lambda self, *a: calls.append(("recipe",) + a) or 3)
    assert db.moves_merge.transfer_review_state(loser, survivor) == 3
    assert calls == [("gps", loser, survivor), ("recipe", loser, survivor)]


def _recipe(db, photo_id):
    row = db.conn.execute(
        "SELECT recipe_json FROM photo_edit_recipes WHERE photo_id = ?",
        (photo_id,)).fetchone()
    return row[0] if row else None


def test_transfer_edit_recipe(db, two_photos):
    loser, survivor = two_photos
    assert db.moves_merge.transfer_edit_recipe(loser, survivor) == 0

    db.conn.execute(
        "INSERT INTO photo_edit_recipes (photo_id, recipe_json, updated_at) "
        "VALUES (?, '{\"old\": 1}', '2020-01-01')", (survivor,))
    db.conn.commit()
    older = _queue(db, survivor, "edit_recipe", '{"s": 1}', "2026-01-01 00:00:00")
    newer = _queue(db, loser, "edit_recipe", '{"l": 2}', "2026-01-02 00:00:00")
    assert db.moves_merge.transfer_edit_recipe(loser, survivor) == 1
    assert db.conn.in_transaction
    assert [r["id"] for r in _pending(db, loser, survivor)] == [newer]
    assert older not in [r["id"] for r in _pending(db, loser, survivor)]
    assert _recipe(db, survivor) == '{"l": 2}'
    assert db.conn.execute(
        "SELECT updated_at FROM photo_edit_recipes WHERE photo_id = ?",
        (survivor,)).fetchone()[0] != "2020-01-01"

    # A newest empty value is the "recipe cleared" edit.
    _queue(db, loser, "edit_recipe", "", "2026-01-03 00:00:00")
    assert db.moves_merge.transfer_edit_recipe(loser, survivor) == 0
    assert _recipe(db, survivor) is None


def test_transfer_edit_recipe_inserts_missing_survivor_row(db, two_photos):
    loser, survivor = two_photos
    _queue(db, loser, "edit_recipe", '{"x": 1}', "2026-01-02 00:00:00")
    assert db.moves_merge.transfer_edit_recipe(loser, survivor) == 0
    assert _recipe(db, survivor) == '{"x": 1}'


# -- sync-only grants --------------------------------------------------------


def test_link_survivor_for_sibling_edits(db, caplog):
    active = db._active_workspace_id
    sibling = db.create_workspace("Sibling")
    fid = _folder(db, "/arch", root=True)
    pid = _photo(db, fid, "a.jpg")

    assert db.moves_merge.link_survivor_for_sibling_edits(sibling, pid + 999) is False
    # The active workspace already sees the folder: no grant needed.
    assert db.moves_merge.link_survivor_for_sibling_edits(active, pid) is False

    with caplog.at_level(logging.INFO):
        assert db.moves_merge.link_survivor_for_sibling_edits(sibling, pid) is True
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
    assert db.moves_merge.link_survivor_for_sibling_edits(sibling, pid) is False


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

    det = db.detections.save(p["hi"], [
        {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": 0.5,
         "category": "animal"}], detector_model="MDV6")[0]
    db.predictions.add(det, species="Robin", confidence=0.9, model="bioclip")
    assert db.query_move_rule_matches({"has_predictions": True}) == [p["hi"]]
    assert len(calls) == 1
    # A workspace override above the detection's confidence hides it.
    db.workspaces.update(db._active_workspace_id,
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
    monkeypatch.setattr(FolderRepository, "update_counts", lambda self: calls.append(1))
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



@pytest.mark.parametrize('same_hash', [True, False])
@pytest.mark.parametrize('status', ['unresolved', 'resolved'])
def test_duplicate_merge_preserves_global_identity_and_retargets_audit(db, tmp_path, same_hash, status):
    from services.identity_alternatives import resolve_alternatives, save_alternatives
    t = _collision_tree(db, tmp_path, same_hash=same_hash)
    losing, survivor = (t['staged'], t['survivor']) if same_hash else (t['survivor'], t['staged'])
    first = db.require_workspace_id()
    second = db.create_workspace('Shared review')
    db.photo_visibility.grant(first, [losing, survivor])
    db.photo_visibility.grant(second, [losing])
    keyword = db.add_keyword('Tropical Kingbird', is_species=True)
    db.tag_photo(losing, keyword)
    db.tag_photo(survivor, keyword)
    _queue(db, survivor, 'keyword_add', 'Tropical Kingbird', '2020-01-01', ws=first)
    _queue(db, losing, 'keyword_add', 'Tropical Kingbird', '2020-01-01', ws=second)
    unrelated = db.add_keyword('American Robin', is_species=True)
    db.tag_photo(survivor, unrelated)
    db.photo_review.set_flag(survivor, 'rejected')
    with db._commits_held():
        edit_id = save_alternatives(db, [losing], ['Tropical Kingbird', "Couch's Kingbird"], 'Uncertain call', 'Audio')
        if status == 'resolved':
            edit_id = resolve_alternatives(db, [losing], 'Tropical Kingbird')
    state = db.identity_alternatives.for_photos([losing], include_resolved=True)[losing]
    counts = db.merge_staged_tree_into_archive(t['stage_root'], t['arch'])
    assert losing in counts['dropped_photo_ids']
    assert db.identity_alternatives.for_photos([survivor], include_resolved=True)[survivor] == state
    assert db.conn.execute('SELECT photo_id FROM edit_history_items WHERE edit_id=?', (edit_id,)).fetchone()[0] == survivor
    assert not db.conn.execute('SELECT 1 FROM pending_changes WHERE photo_id=?', (losing,)).fetchone()
    assert db.conn.execute('SELECT 1 FROM pending_changes WHERE photo_id=?', (survivor,)).fetchone()
    assert unrelated in _kw_ids(db, survivor)
    assert db.photos.get(survivor)['flag'] == 'rejected'
    assert db.undo_last_edit()['id'] == edit_id
    assert db.redo_last_undo()['id'] == edit_id
    assert db.identity_alternatives.for_photos([survivor], include_resolved=True)[survivor] == state
    assert unrelated in _kw_ids(db, survivor)
    assert db.photos.get(survivor)['flag'] == 'rejected'
    db.set_active_workspace(second)
    assert db.identity_alternatives.for_photos([survivor], include_resolved=True)[survivor] == state
    assert set(db.identity_alternatives.for_photos()) == ({survivor} if status == 'unresolved' else set())


@pytest.mark.parametrize('same_hash', [True, False])
@pytest.mark.parametrize('difference', ['reason', 'revision', 'status'])
def test_duplicate_merge_rejects_distinct_identity_decisions_atomically(db, tmp_path, same_hash, difference):
    from repositories.identity_alternatives import IdentityMergeConflict
    t = _collision_tree(db, tmp_path, same_hash=same_hash)
    state = {'group_id': 'shared', 'candidates': ['Tropical Kingbird', "Couch's Kingbird"],
             'reason': 'Original reason', 'missing_evidence': 'Audio', 'status': 'unresolved'}
    db.identity_alternatives.set_state(t['staged'], state)
    db.identity_alternatives.set_state(t['survivor'], {**state, difference if difference != 'revision' else 'group_id':
                                                      {'reason': 'New reason', 'revision': 'other', 'status': 'resolved'}[difference]})
    _queue(db, t['staged'], 'rating', '5', '2026-01-01', token='preserve')
    tables = ['photos', 'folders', 'photo_identity_alternatives', 'pending_changes', 'photo_keywords']
    before = {table: [tuple(row) for row in db.conn.execute(f'SELECT * FROM {table} ORDER BY id' if table not in ['photo_identity_alternatives', 'photo_keywords'] else f'SELECT * FROM {table} ORDER BY photo_id')]
              for table in tables}
    with pytest.raises(IdentityMergeConflict, match='Reconcile their alternatives and evidence in Review'):
        db.merge_staged_tree_into_archive(t['stage_root'], t['arch'])
    assert not db.conn.in_transaction
    for table in tables:
        after = [tuple(row) for row in db.conn.execute(f'SELECT * FROM {table} ORDER BY id' if table not in ['photo_identity_alternatives', 'photo_keywords'] else f'SELECT * FROM {table} ORDER BY photo_id')]
        assert after == before[table]


@pytest.mark.parametrize('owner', ['survivor', 'both'])
def test_duplicate_merge_keeps_existing_equal_identity_decision(db, tmp_path, owner):
    t = _collision_tree(db, tmp_path)
    state = {'group_id': 'shared', 'candidates': ['Tropical Kingbird', "Couch's Kingbird"],
             'reason': 'Original reason', 'missing_evidence': 'Audio', 'status': 'unresolved'}
    db.identity_alternatives.set_state(t['survivor'], state)
    if owner == 'both':
        db.identity_alternatives.set_state(t['staged'], state)
    db.commit()
    db.merge_staged_tree_into_archive(t['stage_root'], t['arch'])
    assert db.identity_alternatives.for_photos()[t['survivor']] == state


@pytest.mark.parametrize('same_hash', [True, False])
@pytest.mark.parametrize('alias', ['common', 'scientific', 'alternate_common', 'source_only'])
def test_duplicate_merge_rejects_synced_candidate_confirmation_aliases(db, tmp_path, same_hash, alias):
    from repositories.identity_alternatives import IdentityMergeConflict
    t = _collision_tree(db, tmp_path, same_hash=same_hash)
    losing, survivor = (t['staged'], t['survivor']) if same_hash else (t['survivor'], t['staged'])
    candidate = 'Tropical Kingbird'
    if alias == 'source_only':
        parent = db.add_keyword('First hierarchy')
        other = db.add_keyword('Second hierarchy')
        common = db.add_keyword(candidate, parent_id=parent, is_species=True, source_taxon_id=999999)
        keyword = db.add_keyword('Tyrannus melancholicus', parent_id=other, is_species=True, source_taxon_id=999999)
        candidate = db.keywords.name_of(common)
    else:
        db.conn.execute("INSERT INTO taxa (id, inat_id, name, common_name, rank) VALUES (91001, 999999, 'Tyrannus melancholicus', 'Tropical Kingbird', 'species')")
        db.conn.execute("INSERT INTO taxa_common_names(taxon_id,name) VALUES (91001,'Yellow Kingbird')")
        keyword = db.add_keyword('Tyrannus melancholicus' if alias != 'common' else candidate, is_species=True)
        if alias == 'alternate_common':
            candidate = 'yellow kingbird'
    db.tag_photo(survivor, keyword)
    state = {'group_id': 'saved', 'candidates': [candidate, "Couch's Kingbird"],
             'reason': 'Need call evidence', 'missing_evidence': 'Audio', 'status': 'unresolved'}
    db.identity_alternatives.set_state(losing, state)
    db.commit()
    assert not db.conn.execute('SELECT 1 FROM pending_changes').fetchone()
    with pytest.raises(IdentityMergeConflict, match='Reconcile the species confirmation in Review'):
        db.merge_staged_tree_into_archive(t['stage_root'], t['arch'])
    assert db.photos.get(losing) is not None
    assert db.photos.get(survivor) is not None
    assert keyword in _kw_ids(db, survivor)
    assert db.conn.execute('SELECT reason FROM photo_identity_alternatives WHERE photo_id=?', (losing,)).fetchone()[0] == state['reason']
    assert not db.conn.execute('SELECT 1 FROM pending_changes').fetchone()

def test_merge_commits_counts_and_invalidates_after_reparent(
        db, tmp_path, monkeypatch):
    t = _collision_tree(db, tmp_path)
    ws = db._active_workspace_id
    _queue(db, t["staged"], "rating", "4", "2026-01-01 00:00:00", token="tok")
    recorder = _CacheRecorder()
    order = []
    real_counts = FolderRepository.update_counts

    def counts_recorder(self):
        order.append(("counts", db.conn.in_transaction))
        return real_counts(self)

    monkeypatch.setattr(db, "_new_images_cache", recorder)
    monkeypatch.setattr(FolderRepository, "update_counts", counts_recorder)

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

    def boom(self, losing_id, surviving_id):
        raise RuntimeError("transfer exploded")

    monkeypatch.setattr(MovesMergeRepository, "transfer_review_state", boom)
    monkeypatch.setattr(FolderRepository, "update_counts",
                        lambda self: counts_calls.append(1))
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
    real_move = db._move_location_state_for_merge

    def move_recorder(losing, surviving):
        calls.append(("_move_location_state_for_merge", losing, surviving))
        return real_move(losing, surviving)

    monkeypatch.setattr(db, "_move_location_state_for_merge", move_recorder)
    for name in ("reconcile_conflicting_keyword_edits", "transfer_review_state"):
        real = getattr(MovesMergeRepository, name)

        def recorder(self, losing, surviving, _name=name, _real=real):
            calls.append((_name, losing, surviving))
            return _real(self, losing, surviving)

        monkeypatch.setattr(MovesMergeRepository, name, recorder)
    db.merge_staged_tree_into_archive(t["stage_root"], t["arch"])
    assert calls == [
        ("_move_location_state_for_merge", t["staged"], t["survivor"]),
        ("reconcile_conflicting_keyword_edits", t["staged"], t["survivor"]),
        ("transfer_review_state", t["staged"], t["survivor"]),
    ]


def test_merge_routes_phantom_helpers_through_the_facade(
        db, tmp_path, monkeypatch):
    t = _collision_tree(db, tmp_path, same_hash=False)
    calls = []
    real_move = db._move_location_state_for_merge

    def move_recorder(losing, surviving):
        calls.append(("_move_location_state_for_merge", losing, surviving))
        return real_move(losing, surviving)

    monkeypatch.setattr(db, "_move_location_state_for_merge", move_recorder)
    for name in ("reconcile_conflicting_keyword_edits", "transfer_review_state"):
        real = getattr(MovesMergeRepository, name)

        def recorder(self, losing, surviving, _name=name, _real=real):
            calls.append((_name, losing, surviving))
            return _real(self, losing, surviving)

        monkeypatch.setattr(MovesMergeRepository, name, recorder)
    counts = db.merge_staged_tree_into_archive(t["stage_root"], t["arch"])
    assert calls == [
        ("_move_location_state_for_merge", t["survivor"], t["staged"]),
        ("reconcile_conflicting_keyword_edits", t["survivor"], t["staged"]),
        ("transfer_review_state", t["survivor"], t["staged"]),
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
    real = MovesMergeRepository.link_survivor_for_sibling_edits

    def recorder(self, ws_id, photo_id):
        calls.append((ws_id, photo_id, db.conn.execute(
            "SELECT folder_id FROM photos WHERE id = ?", (photo_id,)
        ).fetchone()[0]))
        return real(self, ws_id, photo_id)

    monkeypatch.setattr(MovesMergeRepository, "link_survivor_for_sibling_edits", recorder)
    counts = db.merge_staged_tree_into_archive(t["stage_root"], t["arch"])
    assert calls == [(sibling, t["survivor"], t["day"])]
    # Sibling rows are preserved but not reported as active-ws identities.
    assert counts["preserved_edit_count"] == 1
    assert counts["preserved_off_staging_identities"] == []
    assert db.get_sync_only_photo_paths(sibling) == {
        t["survivor"]: str(tmp_path / "arch" / "day")}


def test_merge_moves_embedded_keyword_offered_onto_the_survivor(db, tmp_path):
    """A staged row with embedded suppression folds in without an FK abort.

    ``photo_embedded_keyword_offered.photo_id`` is a non-cascading FK, so a
    leftover row on the losing photo aborts the ``DELETE FROM photos`` with
    ``FOREIGN KEY constraint failed``. The suppression must also follow the
    surviving row so a later rescan of the embedded value does not re-tag
    the archive-side photo with a keyword the user had already removed.
    """
    t = _collision_tree(db, tmp_path)
    db.photo_keywords.record_embedded_offered_keys(t["staged"], ["robin"])

    counts = db.merge_staged_tree_into_archive(t["stage_root"], t["arch"])

    assert counts["dropped_photo_ids"] == [t["staged"]]
    with _other(db) as other:
        offered_rows = other.execute(
            "SELECT photo_id, keyword_key "
            "FROM photo_embedded_keyword_offered",
        ).fetchall()
    assert [tuple(r) for r in offered_rows] == [(t["survivor"], "robin")]


def test_merge_moves_embedded_keyword_offered_from_phantom_target(
        db, tmp_path):
    """A phantom-target row with embedded suppression merges without FK abort.

    Same contract as above but on the replacement path: the phantom target
    is dropped, so its ``photo_embedded_keyword_offered`` row has to move
    onto the staged survivor (``pid``) before the delete, or the merge
    aborts on the FK.
    """
    t = _collision_tree(db, tmp_path, same_hash=False)
    db.photo_keywords.record_embedded_offered_keys(t["survivor"], ["robin"])

    counts = db.merge_staged_tree_into_archive(t["stage_root"], t["arch"])

    assert counts["dropped_photo_ids"] == [t["survivor"]]
    with _other(db) as other:
        offered_rows = other.execute(
            "SELECT photo_id, keyword_key "
            "FROM photo_embedded_keyword_offered",
        ).fetchall()
    assert [tuple(r) for r in offered_rows] == [(t["staged"], "robin")]


@pytest.mark.parametrize("same_hash", [True, False])
@pytest.mark.parametrize("hierarchical", [False, True])
@pytest.mark.parametrize("removed_on_survivor", [False, True])
def test_merge_preserves_embedded_tags_and_survivor_removals(
        db, tmp_path, same_hash, hierarchical, removed_on_survivor):
    from scanner.keywords import _import_embedded_keywords_for_photo

    t = _collision_tree(db, tmp_path, same_hash=same_hash)
    losing, survivor = ((t["staged"], t["survivor"]) if same_hash
                        else (t["survivor"], t["staged"]))
    metadata = {"XMP": ({"HierarchicalSubject": ["Birds|Robin"]} if hierarchical
                        else {"Subject": ["Robin"]})}
    assert _import_embedded_keywords_for_photo(db, losing, metadata)
    keyword = db.photo_keywords.get_for_photo(losing)[0]
    source = db.conn.execute(
        "SELECT source FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?",
        (losing, keyword["id"]),
    ).fetchone()[0]
    if removed_on_survivor:
        assert _import_embedded_keywords_for_photo(db, survivor, metadata)
        db.untag_photo(survivor, keyword["id"])

    db.merge_staged_tree_into_archive(t["stage_root"], t["arch"])

    assert {k["name"] for k in db.photo_keywords.get_for_photo(survivor)} == (
        set() if removed_on_survivor else {"Robin"})
    assert not _import_embedded_keywords_for_photo(db, survivor, metadata)
    assert {k["name"] for k in db.photo_keywords.get_for_photo(survivor)} == (
        set() if removed_on_survivor else {"Robin"})
    if not removed_on_survivor:
        assert db.conn.execute(
            "SELECT source FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?",
            (survivor, keyword["id"]),
        ).fetchone()[0] == source


@pytest.mark.parametrize("same_hash", [True, False])
def test_embedded_merge_keeps_survivor_manual_provenance(db, tmp_path, same_hash):
    from scanner.keywords import _import_embedded_keywords_for_photo

    t = _collision_tree(db, tmp_path, same_hash=same_hash)
    losing, survivor = ((t["staged"], t["survivor"]) if same_hash
                        else (t["survivor"], t["staged"]))
    _import_embedded_keywords_for_photo(db, losing, {"XMP": {"Subject": ["Robin"]}})
    keyword = db.photo_keywords.get_for_photo(losing)[0]
    db.tag_photo(survivor, keyword["id"])

    db.merge_staged_tree_into_archive(t["stage_root"], t["arch"])

    assert db.conn.execute(
        "SELECT source FROM photo_keywords WHERE photo_id = ? AND keyword_id = ?",
        (survivor, keyword["id"]),
    ).fetchone()[0] == "manual"


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


def _visible_to(db, ws):
    return {
        r["filename"] for r in db.conn.execute(
            "SELECT p.filename FROM photos p JOIN photo_workspace_visibility v "
            "ON v.photo_id = p.id WHERE v.workspace_id = ?", (ws,))
    }


def test_merge_into_existing_folder_keeps_photo_in_sharing_workspace(db):
    """A staged photo folded into an existing archive folder stays visible to
    a workspace that shared the staged folder, without exposing that
    workspace to the archive folder's other photos."""
    ws = db._active_workspace_id
    sharing = db.create_workspace("Sharing")
    linked = db.create_workspace("Linked")
    db.set_active_workspace(ws)
    arch = _folder(db, "/arch", root=True)
    _photo(db, arch, "old.jpg")
    db.add_workspace_folder(linked, arch)
    stage = _folder(db, "/stage", root=True)
    _photo(db, stage, "new.jpg")
    db.add_workspace_folder(sharing, stage)
    db.add_workspace_folder(linked, stage)

    counts = db.merge_staged_tree_into_archive(stage, "/arch")

    assert counts["new_photos"] == 1 and counts["merged_folders"] == 1
    assert _visible_to(db, ws) == {"old.jpg", "new.jpg"}
    assert _visible_to(db, sharing) == {"new.jpg"}
    assert _visible_to(db, linked) == {"old.jpg", "new.jpg"}
    # Only the workspace without a link to the archive folder needs a grant.
    assert [tuple(r) for r in db.conn.execute(
        "SELECT workspace_id FROM workspace_photos")] == [(sharing,)]


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


# -- _photo_syncable_in_workspace ---------------------------------------------


def test_photo_syncable_in_workspace(db):
    sibling = db.create_workspace("Sibling")
    lib = _folder(db, "/lib", root=True)
    other = _folder(db, "/other", link=False)
    member = _photo(db, lib, "member.jpg")
    granted = _photo(db, other, "granted.jpg")
    stranger = _photo(db, other, "stranger.jpg")
    db.conn.execute(
        "INSERT INTO workspace_sync_only_photos (workspace_id, photo_id) "
        "VALUES (?, ?)", (db._active_workspace_id, granted))
    db.conn.commit()

    assert db._photo_syncable_in_workspace(member) is True
    assert db._photo_syncable_in_workspace(granted) is True
    assert db._photo_syncable_in_workspace(stranger) is False
    assert db._photo_syncable_in_workspace(stranger + 999) is False
    db._verify_photo_syncable_in_workspace(granted)
    with pytest.raises(ValueError):
        db._verify_photo_syncable_in_workspace(stranger)
    db.set_active_workspace(sibling)
    assert db._photo_syncable_in_workspace(member) is False
    assert db._photo_syncable_in_workspace(granted) is False
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError, match="No active workspace set"):
        db._photo_syncable_in_workspace(member)


def test_photo_syncable_in_workspace_checks_membership_through_the_facade(
        db, monkeypatch):
    other = _folder(db, "/other", link=False)
    pid = _photo(db, other, "a.jpg")
    assert db._photo_syncable_in_workspace(pid) is False
    monkeypatch.setattr(WorkspaceFolderRepository, "photo_in_workspace",
                        lambda self, photo_id, workspace_id: True)
    statements = _trace(db)
    try:
        assert db._photo_syncable_in_workspace(pid) is True
    finally:
        _stop_trace(db)
    assert statements == []


# -- The move package's file-move job SQL (``db.moves_merge``) ---------------


def _committed(db, sql, params=()):
    with _other(db) as conn:
        return [tuple(r) for r in conn.execute(sql, params).fetchall()]


def test_folder_lookups_for_file_moves(db):
    a = _folder(db, "/lib/a", root=True)
    b = _folder(db, "/lib/a/b", a)
    c = _folder(db, "/lib/a_c", root=True)
    moves = db.moves_merge

    assert sorted(tuple(r) for r in moves.folders_except(a)) == [
        (b, "/lib/a/b"), (c, "/lib/a_c")]
    assert moves.folder_path_row(b)["path"] == "/lib/a/b"
    assert moves.folder_path_row(b + 999) is None
    assert tuple(moves.folder_source_row(b)) == (b, "/lib/a/b", "b")
    assert moves.folder_source_row(b + 999) is None
    assert moves.folder_id_row("/lib/a/b")["id"] == b
    assert moves.folder_id_row("/lib/a/missing") is None
    # A literal prefix: ``_`` in the base is not a wildcard.
    assert moves.folder_paths_below("/lib/a") == [("/lib/a/b", "b")]
    assert moves.folder_paths_below("/lib/a_") == []


def test_folder_paths_below_matches_windows_separators(db):
    """Windows stores ``\\``-separated paths; the subtree must still match."""
    a = _folder(db, "C:\\lib\\a", root=True)
    b = _folder(db, "C:\\lib\\a\\b", a)
    _folder(db, "C:\\lib\\a\\b\\c", b)
    _folder(db, "C:\\lib\\ab", root=True)
    assert sorted(db.moves_merge.folder_paths_below("C:\\lib\\a")) == [
        ("C:\\lib\\a\\b", "b"),
        ("C:\\lib\\a\\b\\c", "b/c"),
    ]


def test_folder_paths_below_does_not_fold_posix_backslashes(db):
    """``\\`` is a legal POSIX filename character, so a base like
    ``/lib/a\\b`` must not match the unrelated subtree ``/lib/a/b/...``.
    """
    _folder(db, "/lib/a\\b", root=True)
    a = _folder(db, "/lib/a", root=True)
    b = _folder(db, "/lib/a/b", a)
    _folder(db, "/lib/a/b/c", b)
    assert db.moves_merge.folder_paths_below("/lib/a\\b") == []
    assert sorted(db.moves_merge.folder_paths_below("/lib/a")) == [
        ("/lib/a/b", "b"),
        ("/lib/a/b/c", "b/c"),
    ]


def test_folders_except_is_an_unfetched_cursor(db):
    _folder(db, "/x", root=True)
    rows = db.moves_merge.folders_except(-1)
    assert isinstance(rows, sqlite3.Cursor)


def test_insert_folder_if_absent_does_not_commit_or_link(db):
    parent = _folder(db, "/lib", root=True)
    moves = db.moves_merge
    cur = moves.insert_folder_if_absent("/lib/new", "new", parent)
    assert cur.rowcount == 1
    new_id = cur.lastrowid
    assert db.in_transaction
    assert _committed(db, "SELECT id FROM folders WHERE path = ?", ("/lib/new",)) == []
    db.commit()
    assert _committed(
        db, "SELECT id, name, parent_id FROM folders WHERE path = ?",
        ("/lib/new",)) == [(new_id, "new", parent)]
    assert _committed(
        db, "SELECT 1 FROM workspace_folders WHERE folder_id = ?", (new_id,)) == []
    again = moves.insert_folder_if_absent("/lib/new", "other", None)
    assert again.rowcount == 0


def test_date_move_subtree_photos_registers_functions_first(db):
    root = _folder(db, "/lib", root=True)
    child = _folder(db, "/lib/kid", root)
    other = _folder(db, "/elsewhere", root=True)
    p_root = _photo(db, root, "r.jpg", timestamp="2026-01-02T03:04:05")
    p_child = _photo(db, child, "k.jpg")
    _photo(db, other, "o.jpg")
    seen = []

    def descends(path):
        seen.append(path)
        return int(path.startswith("/lib/"))

    rows = db.moves_merge.date_move_subtree_photos(
        root, "VIREO_TEST_DESCENDS(f.path) = ?", (1,),
        [("VIREO_TEST_DESCENDS", 1, descends)],
    )
    assert [r["id"] for r in rows] == [p_root, p_child]
    assert rows[0].keys() == ["id", "filename", "exif_data", "timestamp", "file_mtime"]
    assert rows[0]["timestamp"] == "2026-01-02T03:04:05"
    assert "/elsewhere" in seen
    assert [r["id"] for r in db.moves_merge.date_move_subtree_photos(
        child, "0", ())] == [p_child]


def test_subtree_and_under_path_photo_reads(db):
    root = _folder(db, "/lib/shoot", root=True)
    child = _folder(db, "/lib/shoot/day", root)
    sibling = _folder(db, "/lib/shoot2", root=True)
    p_root = _photo(db, root, "a.jpg", file_size=5, file_mtime=2.0)
    p_child = _photo(db, child, "b.jpg")
    _photo(db, sibling, "c.jpg")
    moves = db.moves_merge

    rows = moves.subtree_photo_rows("/lib/shoot", "/lib/shoot/")
    assert sorted((r["id"], r["folder_path"]) for r in rows) == [
        (p_root, "/lib/shoot"), (p_child, "/lib/shoot/day")]
    first = next(r for r in rows if r["id"] == p_root)
    assert (first["filename"], first["file_mtime"], first["file_size"]) == (
        "a.jpg", 2.0, 5)
    assert sorted(r["id"] for r in moves.photo_ids_under_path("/lib/shoot")) == [
        p_root, p_child]


def test_move_provenance_reads_and_clear_without_commit(db):
    dest = _folder(db, "/dest", root=True)
    src = _folder(db, "/src", root=True)
    p1 = _photo(db, dest, "IMG_1.jpg")
    p2 = _photo(db, dest, "IMG_1.cr3")
    _photo(db, src, "IMG_2.jpg")
    db.conn.execute(
        "UPDATE photos SET last_move_source_folder_path = '/src' "
        "WHERE id IN (?, ?)", (p1, p2))
    db.commit()
    moves = db.moves_merge

    assert sorted(tuple(r) for r in moves.move_provenance_in_folder(dest)) == [
        ("IMG_1.cr3", "/src"), ("IMG_1.jpg", "/src")]
    assert [r["filename"] for r in moves.filenames_in_folder(src)] == ["IMG_2.jpg"]
    assert sorted(tuple(r) for r in moves.photos_moved_from("/src")) == [
        (p1, "IMG_1.jpg"), (p2, "IMG_1.cr3")]

    moves.clear_move_source([p1])
    assert db.in_transaction
    assert sorted(_committed(
        db, "SELECT id FROM photos WHERE last_move_source_folder_path = '/src'"
    )) == sorted([(p1,), (p2,)])
    db.commit()
    assert _committed(
        db, "SELECT id FROM photos WHERE last_move_source_folder_path = '/src'"
    ) == [(p2,)]


def test_photo_move_savepoint_commits_the_repoint(db):
    src = _folder(db, "/src", root=True)
    dest = _folder(db, "/dest", root=True)
    pid = _photo(db, src, "a.jpg")
    moves = db.moves_merge
    with moves.photo_move_savepoint():
        moves.repoint_moved_photo(pid, dest, "/src")
    assert not db.in_transaction
    assert _committed(
        db, "SELECT folder_id, last_move_source_folder_path FROM photos "
        "WHERE id = ?", (pid,)) == [(dest, "/src")]


def test_photo_move_savepoint_rolls_back_to_itself_and_reraises(db):
    src = _folder(db, "/src", root=True)
    dest = _folder(db, "/dest", root=True)
    pid = _photo(db, src, "a.jpg")
    # Work already pending on the connection stays pending: only the
    # savepoint's own writes are undone.
    db.conn.execute("UPDATE photos SET rating = 4 WHERE id = ?", (pid,))
    moves = db.moves_merge
    with pytest.raises(KeyboardInterrupt):
        with moves.photo_move_savepoint():
            moves.repoint_moved_photo(pid, dest, "/src")
            raise KeyboardInterrupt
    assert db.in_transaction
    row = db.conn.execute(
        "SELECT folder_id, last_move_source_folder_path, rating FROM photos "
        "WHERE id = ?", (pid,)).fetchone()
    assert tuple(row) == (src, None, 4)
    db.rollback()


def test_restamp_mtimes_moves_only_pinned_markers_without_commit(db):
    folder = _folder(db, "/f", root=True)
    pinned = _photo(db, folder, "a.jpg", file_size=10, file_mtime=1.0)
    raced = _photo(db, folder, "b.jpg", file_size=10, file_mtime=7.0)
    db.conn.execute(
        "UPDATE photos SET working_copy_evicted_mtime = 1.0, "
        "working_copy_failed_mtime = -1 WHERE id = ?", (pinned,))
    for pid, size in ((pinned, 10), (raced, 99)):
        db.conn.execute(
            "INSERT INTO offline_originals (photo_id, source_size, source_mtime, "
            "cached_at, status) VALUES (?, ?, 1.0, 0, 'ok')", (pid, size))
    db.commit()
    updates = [(5.0, pinned, 1.0, 10), (5.0, raced, 1.0, 10)]
    moves = db.moves_merge

    assert moves.restamp_photo_mtimes(updates) == 1
    moves.restamp_offline_original_mtimes(updates)
    assert db.in_transaction
    assert _committed(db, "SELECT file_mtime FROM photos WHERE id = ?", (pinned,)) == [(1.0,)]
    db.commit()
    assert _committed(
        db, "SELECT file_mtime, working_copy_evicted_mtime, "
        "working_copy_failed_mtime FROM photos WHERE id = ?", (pinned,)
    ) == [(5.0, 5.0, -1)]
    assert _committed(db, "SELECT file_mtime FROM photos WHERE id = ?", (raced,)) == [(7.0,)]
    assert sorted(_committed(
        db, "SELECT photo_id, source_mtime FROM offline_originals")) == sorted(
        [(pinned, 5.0), (raced, 1.0)])


def test_thumb_paths_for_ids_skips_photos_without_a_thumbnail(db):
    folder = _folder(db, "/f", root=True)
    with_thumb = _photo(db, folder, "a.jpg")
    without = _photo(db, folder, "b.jpg")
    db.conn.execute("UPDATE photos SET thumb_path = 't/a.jpg' WHERE id = ?", (with_thumb,))
    db.commit()
    rows = db.moves_merge.thumb_paths_for_ids([with_thumb, without, without + 999])
    assert [tuple(r) for r in rows] == [(with_thumb, "t/a.jpg")]


# -- structure ---------------------------------------------------------------

# Still on ``Database``: the keyword re-tagging (which the provenance contract
# finds through ``self.tag_photo`` / ``untag_photo``), the staged-tree merge
# (which wires in the workspace-link, new-images and folder-count hooks), and
# the reads that resolve the active workspace or its detector floor.
MOVED = [
    "_move_location_state_for_merge",
    "_carry_keyword_associations_for_merge",
    "get_sync_only_photo_paths", "merge_staged_tree_into_archive",
    "query_move_rule_matches",
    "_photo_syncable_in_workspace",
]

# The forwarding wrappers this domain used to have on ``Database``; callers
# reach the same methods through ``db.moves_merge`` now.
REMOVED_MOVES_MERGE_WRAPPERS = [
    "create_move_rule",
    "get_move_rule",
    "list_move_rules",
    "update_move_rule",
    "delete_move_rule",
    "touch_move_rule",
    "batch_update_photo_folder",
    "move_folder_path",
    "_newest_location_change_key",
    "_reconcile_conflicting_keyword_edits",
    "_photo_keyword_ids_matching",
    "_transfer_gps_review_for_merge",
    "_transfer_review_state_for_merge",
    "_transfer_edit_recipe_for_merge",
    "_link_survivor_for_sibling_edits",
    "check_filename_collisions",
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


def test_moves_merge_has_no_forwarding_wrappers_on_database():
    """The domain is reached through ``db.moves_merge``; Database keeps no aliases."""
    for name in REMOVED_MOVES_MERGE_WRAPPERS:
        assert not hasattr(Database, name), f"Database.{name} came back; call db.moves_merge"


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
        "prune_nonroot_links_outside_roots": "_prune_ws_nonroot_links_outside_roots",
        "materialize_workspace_descendants": "_materialize_workspace_descendants",
        "add_workspace_folder": "add_workspace_folder",
        "add_workspace_folder_no_commit": "_add_workspace_folder_no_commit",
        "move_location_state": "_move_location_state_for_merge",
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
    assert others == {"case_insensitive_root", "invalidate_new_images", "KEYWORD_SOURCE_CONFLICT_SQL",
                      "update_folder_counts", "root_ancestor_exists", "root_descendant_exists"}
    through_accessors = {
        kw.arg: ast.unparse(kw.value) for kw in call.keywords
        if kw.arg in {"update_folder_counts", "root_ancestor_exists", "root_descendant_exists"}
    }
    assert through_accessors == {
        "update_folder_counts": "self.folders.update_counts",
        "root_ancestor_exists": "self.workspace_folders.root_ancestor_exists",
        "root_descendant_exists": "self.workspace_folders.root_descendant_exists",
    }
    fold = next(kw.value for kw in call.keywords if kw.arg == "KEYWORD_SOURCE_CONFLICT_SQL")
    assert isinstance(fold, ast.Name) and fold.id == "KEYWORD_SOURCE_CONFLICT_SQL"


def test_factory_binds_the_cross_domain_callbacks():
    """The keyword carry re-tags through the façade; the relink is late-bound."""
    import ast

    wired = {
        kw.arg: ast.unparse(kw.value)
        for node in ast.walk(_method_ast("_moves_merge_repository"))
        if isinstance(node, ast.Call)
        for kw in node.keywords
    }
    assert wired["carry_keyword_associations"] == "self._carry_keyword_associations_for_merge"
    assert wired["relink_parents_by_path"] == (
        "lambda folder_ids: self.folders.relink_parents_by_path(folder_ids)")


def test_relink_reaches_a_patched_folder_repository(db, monkeypatch):
    root = _folder(db, "/p/q", root=True)
    seen = []
    monkeypatch.setattr(FolderRepository, "relink_parents_by_path",
                        lambda self, ids: seen.append(list(ids)))
    db.moves_merge.move_folder_path(root, "/z/q")
    assert seen == [[root]]


def test_facade_signatures_are_unchanged():
    import inspect

    from repositories import UNSET

    expected = {
        "_move_location_state_for_merge": "(self, losing_id, surviving_id)",
        "_carry_keyword_associations_for_merge": "(self, losing_id, surviving_id, by_key)",
        "get_sync_only_photo_paths": "(self, workspace_id=None)",
        "merge_staged_tree_into_archive": "(self, staged_root_id, archive_path)",
        "query_move_rule_matches": "(self, criteria)",
    }
    for name, sig in expected.items():
        assert str(inspect.signature(getattr(Database, name))) == sig, name
    repo_expected = {
        "create_rule": "(self, name, destination, criteria)",
        "get_rule": "(self, rule_id)",
        "list_rules": "(self)",
        "delete_rule": "(self, rule_id)",
        "touch_rule": "(self, rule_id)",
        "batch_update_photo_folder": "(self, photo_ids, target_folder_id)",
        "move_folder_path": "(self, folder_id, new_path, new_name=None)",
        "newest_location_change_key": "(self, photo_id)",
        "reconcile_conflicting_keyword_edits": "(self, losing_id, surviving_id)",
        "photo_keyword_ids_matching": "(self, photo_id, match_key)",
        "transfer_gps_review": "(self, losing_id, surviving_id)",
        "transfer_review_state": "(self, losing_id, surviving_id)",
        "transfer_edit_recipe": "(self, losing_id, surviving_id)",
        "link_survivor_for_sibling_edits": "(self, workspace_id, photo_id)",
        "filename_collisions": "(self, photo_ids, target_folder_id)",
    }
    for name, sig in repo_expected.items():
        assert str(inspect.signature(getattr(MovesMergeRepository, name))) == sig, name
    params = inspect.signature(MovesMergeRepository.update_rule).parameters
    assert list(params) == ["self", "rule_id", "name", "destination", "criteria"]
    assert all(params[p].default is UNSET for p in ("name", "destination", "criteria"))


def test_moves_merge_is_a_fresh_repository_on_the_connection_per_access(db):
    """``db.moves_merge`` builds a new repository each time, never a cached one."""
    from repositories.moves_merge import MovesMergeRepository

    first, second = db.moves_merge, db.moves_merge
    assert isinstance(first, MovesMergeRepository)
    assert first is not second
    assert first.conn is db.conn


def test_moves_merge_accessor_builds_through_the_factory():
    import ast
    import inspect
    import textwrap

    accessor = Database.__dict__["moves_merge"]
    assert isinstance(accessor, property)
    source = textwrap.dedent(inspect.getsource(accessor.fget))
    attrs = {
        node.attr
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }
    assert "_moves_merge_repository" in attrs
    assert "conn" not in attrs


def test_move_module_reaches_sql_only_through_the_accessor():
    """The ``move`` package moves originals; its catalog SQL lives in the
    repository."""
    import ast
    from pathlib import Path

    import move

    modules = sorted(Path(move.__file__).parent.glob("*.py"))
    assert len(modules) > 1
    assert not [
        f"{module.name}:{node.lineno}"
        for module in modules
        for node in ast.walk(ast.parse(module.read_text()))
        if isinstance(node, ast.Attribute) and node.attr == "conn"
    ]
