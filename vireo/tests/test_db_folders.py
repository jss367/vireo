"""Behavior pins for the folder domain of ``Database``.

The behavior tests exercise the folder methods only through the public
``Database`` façade, so they hold regardless of whether the SQL lives in
``db.py`` or in ``repositories/folders.py``. They cover folder rows and
trees, parent repair, folder health and missing originals, relocation and
merge into an existing folder, ``delete_folder``, counts, and the
quality-data folder list. Where a method composes other ``Database``
methods (``add_workspace_folder``, ``delete_photos``,
``_merge_into_existing``, ``_transfer_gps_review_for_merge``,
``nearest_ancestor_folder_id``), the tests patch the façade method to pin
that the call still routes through it.
"""

import ast
import contextlib
import inspect
import logging
import os
import sqlite3
import textwrap
import unicodedata

import db as db_module
import pytest
from db import Database, MissingPhotosCancelled


class _RecordingCache:
    """Stand-in for the shared new-images cache that records invalidations."""

    def __init__(self):
        self.invalidated = []

    def invalidate_workspaces(self, db_path, workspace_ids):
        self.invalidated.append((db_path, set(workspace_ids)))


@pytest.fixture
def cache(db, monkeypatch):
    recorder = _RecordingCache()
    monkeypatch.setattr(db, "_new_images_cache", recorder)
    return recorder


def _reader(db):
    """A second connection: sees only what the façade has committed."""
    conn = sqlite3.connect(db._db_path)
    conn.row_factory = sqlite3.Row
    return contextlib.closing(conn)


def _raw_folder(db, path, parent_id=None, *, status="ok", name=None):
    """Insert a folder row without linking it to any workspace."""
    cur = db.conn.execute(
        "INSERT INTO folders (path, name, parent_id, status) VALUES (?, ?, ?, ?)",
        (path, name, parent_id, status),
    )
    db.conn.commit()
    return cur.lastrowid


def _link(db, workspace_id, folder_id, is_root=1):
    db.conn.execute(
        "INSERT OR REPLACE INTO workspace_folders (workspace_id, folder_id, is_root) "
        "VALUES (?, ?, ?)",
        (workspace_id, folder_id, is_root),
    )
    db.conn.commit()


def _links(db, folder_id):
    return {
        (r["workspace_id"], r["is_root"])
        for r in db.conn.execute(
            "SELECT workspace_id, is_root FROM workspace_folders WHERE folder_id = ?",
            (folder_id,),
        )
    }


def _photo(db, folder_id, filename, **kw):
    return db.add_photo(
        folder_id=folder_id, filename=filename, extension=os.path.splitext(filename)[1],
        file_size=kw.pop("file_size", 10), file_mtime=kw.pop("file_mtime", 1.0), **kw,
    )


def _folder_row(db, folder_id):
    return db.conn.execute(
        "SELECT id, path, parent_id, status, photo_count FROM folders WHERE id = ?",
        (folder_id,),
    ).fetchone()


def _set_provenance(db, photo_id, path):
    db.conn.execute(
        "UPDATE photos SET last_move_source_folder_path = ? WHERE id = ?",
        (path, photo_id),
    )
    db.conn.commit()


def _map_local(db, folder_id, source_path, local_path):
    db.conn.execute(
        "INSERT OR IGNORE INTO local_folders (root_folder_id, state) VALUES (?, 'active')",
        (folder_id,),
    )
    db.conn.execute(
        "INSERT INTO local_folder_mappings (root_folder_id, folder_id, source_path, local_path) "
        "VALUES (?, ?, ?, ?)",
        (folder_id, folder_id, source_path, local_path),
    )
    db.conn.commit()


# -- add_folder ---------------------------------------------------------------


def test_add_folder_inserts_commits_and_links_as_workspace_root(db):
    ws = db._ws_id()
    fid = db.add_folder("/p/a", name="a")
    assert isinstance(fid, int)
    with _reader(db) as r:
        row = r.execute("SELECT path, name, parent_id FROM folders WHERE id = ?", (fid,)).fetchone()
        assert tuple(row) == ("/p/a", "a", None)
        link = r.execute(
            "SELECT is_root FROM workspace_folders WHERE workspace_id = ? AND folder_id = ?",
            (ws, fid),
        ).fetchone()
    assert link["is_root"] == 1


def test_add_folder_non_root_and_unlinked_variants(db):
    ws = db._ws_id()
    parent = db.add_folder("/p", name="p")
    child = db.add_folder("/p/c", name="c", parent_id=parent, workspace_root=False)
    assert _links(db, child) == {(ws, 0)}
    detached = db.add_folder("/q", name="q", link_to_workspace=False)
    assert _links(db, detached) == set()
    db.set_active_workspace(None)
    no_ws = db.add_folder("/r", name="r")
    assert _links(db, no_ws) == set()


def test_add_folder_routes_workspace_link_through_facade(db, monkeypatch):
    calls = []
    monkeypatch.setattr(
        Database, "add_workspace_folder",
        lambda self, *a, **kw: calls.append((a, kw)),
    )
    ws = db._ws_id()
    fid = db.add_folder("/p/a", workspace_root=False)
    assert calls == [((ws, fid), {"is_root": False, "restore_removed": False})]


def test_add_folder_existing_path_returns_id_and_backfills_missing_parent(db, monkeypatch):
    parent = db.add_folder("/p")
    child = db.add_folder("/p/c")
    assert _folder_row(db, child)["parent_id"] is None

    commits = []
    real = db_module.commit_with_retry

    def recording(conn, *a, **kw):
        commits.append(conn)
        return real(conn, *a, **kw)

    monkeypatch.setattr(db_module, "commit_with_retry", recording)
    assert db.add_folder("/p/c", parent_id=parent) == child
    assert len(commits) == 2  # INSERT OR IGNORE, then the parent backfill
    with _reader(db) as r:
        assert r.execute("SELECT parent_id FROM folders WHERE id = ?", (child,)).fetchone()[0] == parent

    # An existing parent is never overwritten, and a self-parent is refused.
    other = db.add_folder("/o")
    commits.clear()
    assert db.add_folder("/p/c", parent_id=other) == child
    assert _folder_row(db, child)["parent_id"] == parent
    assert db.add_folder("/o", parent_id=other) == other
    assert _folder_row(db, other)["parent_id"] is None
    assert len(commits) == 2  # only the two INSERT OR IGNORE commits


def test_add_folder_new_row_commits_once(db, monkeypatch):
    commits = []
    real = db_module.commit_with_retry
    monkeypatch.setattr(
        db_module, "commit_with_retry",
        lambda conn, *a, **kw: (commits.append(1), real(conn, *a, **kw))[1],
    )
    db.add_folder("/new", link_to_workspace=False)
    assert commits == [1]


# -- get_folder / get_folder_tree / get_folder_subtree_ids --------------------


def test_get_folder_returns_row_or_none(db):
    fid = db.add_folder("/p/a", name="a")
    row = db.get_folder(fid)
    assert set(row.keys()) == {"id", "path", "name", "parent_id", "status", "photo_count"}
    assert (row["id"], row["path"], row["name"], row["status"]) == (fid, "/p/a", "a", "ok")
    assert db.get_folder(fid + 999) is None
    db.set_active_workspace(None)
    assert db.get_folder(fid)["id"] == fid  # catalog-wide


def test_get_folder_tree_filters_status_and_rewrites_parents(db):
    ws = db._ws_id()
    root = db.add_folder("/t", name="t")
    hidden = _raw_folder(db, "/t/hidden", root)  # not linked to the workspace
    grand = _raw_folder(db, "/t/hidden/g", hidden)
    _link(db, ws, grand, 0)
    partial = db.add_folder("/t/partial", parent_id=root, workspace_root=False)
    db.conn.execute("UPDATE folders SET status = 'partial' WHERE id = ?", (partial,))
    missing = db.add_folder("/t/missing", parent_id=root, workspace_root=False)
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (missing,))
    under_missing = db.add_folder("/t/missing/u", parent_id=missing, workspace_root=False)
    orphan_parent = _raw_folder(db, "/x")
    orphan = _raw_folder(db, "/x/o", orphan_parent)
    _link(db, ws, orphan, 1)
    db.conn.commit()

    rows = db.get_folder_tree()
    assert [r["path"] for r in rows] == ["/t", "/t/hidden/g", "/t/missing/u", "/t/partial", "/x/o"]
    by_id = {r["id"]: r for r in rows}
    assert set(rows[0].keys()) == {
        "id", "path", "name", "parent_id", "photo_count", "status", "is_workspace_root",
    }
    assert by_id[root]["parent_id"] is None
    assert by_id[grand]["parent_id"] == root  # skips the unlinked /t/hidden
    assert by_id[under_missing]["parent_id"] == root  # skips the missing folder
    assert by_id[partial]["status"] == "partial"
    assert by_id[orphan]["parent_id"] is None
    assert (by_id[root]["is_workspace_root"], by_id[grand]["is_workspace_root"]) == (1, 0)

    db.set_active_workspace(None)
    with pytest.raises(RuntimeError, match="No active workspace set"):
        db.get_folder_tree()


def test_get_folder_subtree_ids_walks_only_linked_nodes(db):
    ws = db._ws_id()
    root = db.add_folder("/s")
    a = db.add_folder("/s/a", parent_id=root, workspace_root=False)
    a1 = db.add_folder("/s/a/1", parent_id=a, workspace_root=False)
    gap = _raw_folder(db, "/s/gap", root)
    below_gap = _raw_folder(db, "/s/gap/b", gap)
    _link(db, ws, below_gap, 0)
    assert sorted(db.get_folder_subtree_ids(root)) == sorted([root, a, a1])
    assert db.get_folder_subtree_ids(root)[0] == root
    # An unlinked root is returned as-is but never expands.
    assert db.get_folder_subtree_ids(gap) == [gap]
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError):
        db.get_folder_subtree_ids(root)


# -- path-based subtree helpers -----------------------------------------------


def test_folder_subtree_ids_by_path_uses_paths_and_local_mappings(db):
    root = _raw_folder(db, "/lib/root")
    legacy = _raw_folder(db, "/lib/root/legacy")  # parent_id NULL
    win = _raw_folder(db, "\\lib\\root\\win")  # backslash-separated descendant
    sibling = _raw_folder(db, "/lib/rootish")
    staged = _raw_folder(db, "/local-folders/1/staged")
    _map_local(db, staged, "/lib/root/staged", "/local-folders/1/staged")
    ids = db._folder_subtree_ids_by_path(root)
    assert sorted(ids) == sorted([root, legacy, win, staged])
    assert sibling not in ids
    assert db._folder_subtree_ids_by_path(99999) == [99999]
    empty = _raw_folder(db, "")
    assert db._folder_subtree_ids_by_path(empty) == [empty]


def test_folder_subtree_ids_by_path_routes_local_lookup_through_facade(db, monkeypatch):
    root = _raw_folder(db, "/lib/root")
    seen = []
    monkeypatch.setattr(
        Database, "_local_source_descendant_ids",
        lambda self, path: (seen.append(path), [4242])[1],
    )
    assert sorted(db._folder_subtree_ids_by_path(root)) == sorted([root, 4242])
    assert seen == ["/lib/root"]


def test_local_source_descendant_ids(db):
    a = _raw_folder(db, "/local/a")
    b = _raw_folder(db, "/local/b")
    c = _raw_folder(db, "/local/c")
    _map_local(db, a, "/src/root", "/local/a")
    _map_local(db, b, "C:\\src\\root\\b", "/local/b")
    _map_local(db, c, "/src/rootish", "/local/c")
    assert db._local_source_descendant_ids("") == []
    assert db._local_source_descendant_ids(None) == []
    got = db._local_source_descendant_ids("/src/root")
    assert got == [a]
    assert all(isinstance(x, int) for x in got)
    assert db._local_source_descendant_ids("C:/src/root") == [b]


# -- nearest ancestor / parent repair -----------------------------------------


def test_nearest_ancestor_folder_id(db):
    top = _raw_folder(db, "/n")
    mid = _raw_folder(db, "/n/m/")
    _raw_folder(db, "/nm")
    assert db.nearest_ancestor_folder_id("/n/m/x") == mid
    assert db.nearest_ancestor_folder_id("\\n\\m\\x") == mid
    assert db.nearest_ancestor_folder_id("/n/m/x", exclude_id=mid) == top
    assert db.nearest_ancestor_folder_id("/n") is None  # equal is not an ancestor
    assert db.nearest_ancestor_folder_id("/elsewhere/x") is None


def test_relink_parents_by_path_does_not_commit_and_skips_unknown(db, monkeypatch):
    top = _raw_folder(db, "/r")
    child = _raw_folder(db, "/r/c")
    calls = []
    real = Database.nearest_ancestor_folder_id

    def recording(self, path, exclude_id=None):
        calls.append((path, exclude_id))
        return real(self, path, exclude_id=exclude_id)

    monkeypatch.setattr(Database, "nearest_ancestor_folder_id", recording)
    db._relink_parents_by_path([child, 99999])
    assert calls == [("/r/c", child)]
    assert db.conn.in_transaction
    assert _folder_row(db, child)["parent_id"] == top
    db.conn.rollback()
    assert _folder_row(db, child)["parent_id"] is None


def test_repair_missing_folder_parents(db):
    top = _raw_folder(db, "/m")
    child = _raw_folder(db, "/m/c")
    orphan = _raw_folder(db, "/zz/c")
    root = _raw_folder(db, "/")
    db.repair_missing_folder_parents()
    with _reader(db) as r:
        rows = dict(r.execute("SELECT id, parent_id FROM folders").fetchall())
    assert rows[child] == top
    assert rows[top] == root
    assert rows[orphan] is None
    assert rows[root] is None  # "/" has no parent path; never self-parented
    # No-op: nothing to update, no transaction left open.
    db.repair_missing_folder_parents()
    assert not db.conn.in_transaction


def test_repair_stale_folder_parents(db, monkeypatch, caplog):
    top = _raw_folder(db, "/a")
    other = _raw_folder(db, "/b")
    good = _raw_folder(db, "/a/good", top)
    stale = _raw_folder(db, "/a/stale", other)
    managed = _raw_folder(db, "/a/managed", other)
    _map_local(db, managed, "/a/managed", "/local/managed")
    under_managed = _raw_folder(db, "/a/managed/u", top)
    ws = db._ws_id()
    lwf = _raw_folder(db, "/a/lwf", other)
    db.conn.execute(
        "INSERT INTO local_workspace_folders (workspace_id, folder_id, source_path, local_path) "
        "VALUES (?, ?, '/a/lwf', '/local/lwf')",
        (ws, lwf),
    )
    db.conn.commit()

    calls = []
    real = Database.nearest_ancestor_folder_id
    monkeypatch.setattr(
        Database, "nearest_ancestor_folder_id",
        lambda self, path, exclude_id=None: (
            calls.append((path, exclude_id)), real(self, path, exclude_id=exclude_id)
        )[1],
    )
    with caplog.at_level(logging.INFO, logger=db_module.log.name):
        assert db.repair_stale_folder_parents() == 1
    assert calls == [("/a/stale", stale)]
    assert "Repaired 1 stale folder parent links" in caplog.text
    with _reader(db) as r:
        rows = dict(r.execute("SELECT id, parent_id FROM folders").fetchall())
    assert rows[stale] == top
    assert rows[good] == top
    assert rows[managed] == other
    assert rows[lwf] == other
    assert rows[under_managed] == top  # parent is managed: deferred, left alone
    caplog.clear()
    with caplog.at_level(logging.INFO, logger=db_module.log.name):
        assert db.repair_stale_folder_parents() == 0
    assert "Repaired" not in caplog.text
    assert not db.conn.in_transaction


# -- health / missing ---------------------------------------------------------


def test_check_folder_health_updates_status_and_clears_provenance(db, tmp_path):
    present = tmp_path / "present"
    present.mkdir()
    ok_gone = db.add_folder(str(tmp_path / "gone"))
    partial = db.add_folder(str(present))
    db.conn.execute("UPDATE folders SET status = 'partial' WHERE id = ?", (partial,))
    back = db.add_folder(str(tmp_path))
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (back,))
    partial_gone = db.add_folder(str(tmp_path / "pg"))
    db.conn.execute("UPDATE folders SET status = 'partial' WHERE id = ?", (partial_gone,))
    empty = _raw_folder(db, "")
    pid = _photo(db, partial, "x.jpg")
    other = _photo(db, partial, "y.jpg")
    db.conn.execute(
        "UPDATE photos SET last_move_source_folder_path = ? WHERE id = ?",
        (str(tmp_path / "gone"), pid),
    )
    db.conn.execute(
        "UPDATE photos SET last_move_source_folder_path = ? WHERE id = ?",
        (str(present), other),
    )
    db.conn.commit()

    assert db.check_folder_health() == 4
    with _reader(db) as r:
        status = dict(r.execute("SELECT id, status FROM folders").fetchall())
        prov = dict(r.execute("SELECT id, last_move_source_folder_path FROM photos").fetchall())
    assert status == {
        ok_gone: "missing", partial: "partial", back: "ok",
        partial_gone: "missing", empty: "missing",
    }
    assert prov == {pid: None, other: str(present)}
    # Second pass: nothing changes, nothing is left uncommitted.
    assert db.check_folder_health() == 0
    assert not db.conn.in_transaction


def test_check_folder_health_chunks_provenance_clear(db, tmp_path):
    n = db_module._SQLITE_PARAM_CHUNK_SIZE + 3
    db.conn.executemany(
        "INSERT INTO folders (path, status) VALUES (?, 'ok')",
        [(str(tmp_path / f"gone{i}"),) for i in range(n)],
    )
    db.conn.commit()
    statements = []
    db.conn.set_trace_callback(statements.append)
    assert db.check_folder_health() == n
    db.conn.set_trace_callback(None)
    clears = [s for s in statements if s.startswith("UPDATE photos SET last_move_source_folder_path = NULL")]
    assert len(clears) == 2


def test_get_missing_folders_scoped_with_counts(db):
    b = db.add_folder("/mf/b")
    a = db.add_folder("/mf/a")
    ok = db.add_folder("/mf/ok")
    foreign = _raw_folder(db, "/mf/foreign", status="missing")
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id IN (?, ?)", (a, b))
    db.conn.commit()
    _photo(db, a, "1.jpg")
    _photo(db, a, "2.jpg")
    rows = db.get_missing_folders()
    assert [(r["id"], r["photo_count"]) for r in rows] == [(a, 2), (b, 0)]
    assert set(rows[0].keys()) == {"id", "path", "name", "parent_id", "photo_count"}
    assert ok not in {r["id"] for r in rows} and foreign not in {r["id"] for r in rows}
    other = db.create_workspace("Other")
    db.set_active_workspace(other)
    assert db.get_missing_folders() == []
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError):
        db.get_missing_folders()


def test_get_folder_health_version(db):
    assert db.get_folder_health_version() == 0
    db.set_meta("folder_health_version", "7")
    assert db.get_folder_health_version() == 7


def _ghost_setup(db, tmp_path):
    root = tmp_path / "root"
    sub = root / "sub"
    sub.mkdir(parents=True)
    froot = db.add_folder(str(root))
    fsub = db.add_folder(str(sub), parent_id=froot, workspace_root=False)
    (root / "here.jpg").write_bytes(b"x")
    here = _photo(db, froot, "here.jpg")
    gone = _photo(db, froot, "gone.jpg")
    (sub / "s.jpg").write_bytes(b"x")
    sub_here = _photo(db, fsub, "s.jpg")
    sub_gone = _photo(db, fsub, "t.jpg")
    return froot, fsub, {"here": here, "gone": gone, "sub_here": sub_here, "sub_gone": sub_gone}


def test_get_missing_photos_whole_workspace_and_subtree(db, tmp_path):
    froot, fsub, ids = _ghost_setup(db, tmp_path)
    rows = db.get_missing_photos()
    assert [r["id"] for r in rows] == [ids["gone"], ids["sub_gone"]]
    assert set(rows[0].keys()) == {
        "id", "filename", "extension", "file_size", "timestamp",
        "working_copy_path", "folder_id", "folder_path",
    }
    assert [r["id"] for r in db.get_missing_photos(folder_id=fsub)] == [ids["sub_gone"]]
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError):
        db.get_missing_photos()


def test_get_missing_photos_skips_missing_and_offline_folders(db, tmp_path):
    froot, fsub, ids = _ghost_setup(db, tmp_path)
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (fsub,))
    offline = db.add_folder(str(tmp_path / "offline"))
    _photo(db, offline, "o.jpg")
    db.conn.commit()
    assert [r["id"] for r in db.get_missing_photos()] == [ids["gone"]]


def test_get_missing_photos_unreadable_folder_is_treated_as_offline(db, tmp_path, monkeypatch):
    froot, fsub, ids = _ghost_setup(db, tmp_path)
    real_scandir = os.scandir
    sub_path = db.get_folder(fsub)["path"]

    def scandir(path):
        if path == sub_path:
            raise PermissionError("nope")
        return real_scandir(path)

    monkeypatch.setattr(os, "scandir", scandir)
    progress = []
    rows = db.get_missing_photos(progress_callback=progress.append)
    assert [r["id"] for r in rows] == [ids["gone"]]
    assert progress[-1] == {
        "folders_checked": 2, "photos_considered": 4, "missing_found": 1,
        "total_photos": 4, "current_folder": "",
    }


def test_get_missing_photos_nfc_symlink_and_case_fallback(db, tmp_path):
    folder = tmp_path / "nfc"
    folder.mkdir()
    fid = db.add_folder(str(folder))
    nfd = unicodedata.normalize("NFD", "café.jpg")
    (folder / nfd).write_bytes(b"x")
    nfc_photo = _photo(db, fid, unicodedata.normalize("NFC", "café.jpg"))
    (folder / "target.jpg").write_bytes(b"x")
    os.symlink(folder / "target.jpg", folder / "link.jpg")
    os.symlink(folder / "nowhere.jpg", folder / "broken.jpg")
    link = _photo(db, fid, "link.jpg")
    broken = _photo(db, fid, "broken.jpg")
    missing = {r["id"] for r in db.get_missing_photos()}
    assert broken in missing
    assert nfc_photo not in missing and link not in missing


def test_get_missing_photos_progress_and_callback_failure(db, tmp_path, monkeypatch, caplog):
    froot, fsub, ids = _ghost_setup(db, tmp_path)
    monkeypatch.setattr(db_module, "_MISSING_PHOTOS_PROGRESS_INTERVAL", 1)
    seen = []

    def boom(payload):
        seen.append(payload)
        raise RuntimeError("callback broke")

    with caplog.at_level(logging.ERROR, logger=db_module.log.name):
        rows = db.get_missing_photos(progress_callback=boom)
    assert [r["id"] for r in rows] == [ids["gone"], ids["sub_gone"]]
    assert len(seen) == 1  # disabled after the first failure
    assert "Missing photos progress callback failed" in caplog.text

    progress = []
    db.get_missing_photos(progress_callback=progress.append)
    assert progress[0]["folders_checked"] == 1
    assert progress[-1]["current_folder"] == ""
    assert progress[-1]["missing_found"] == 2


def test_get_missing_photos_cancel(db, tmp_path):
    _ghost_setup(db, tmp_path)
    with pytest.raises(MissingPhotosCancelled):
        db.get_missing_photos(cancel_callback=lambda: True)
    polls = []

    def cancel_later():
        polls.append(1)
        return len(polls) > 3

    with pytest.raises(MissingPhotosCancelled):
        db.get_missing_photos(cancel_callback=cancel_later)


def test_get_missing_photos_subtree_goes_through_facade(db, tmp_path, monkeypatch):
    froot, fsub, ids = _ghost_setup(db, tmp_path)
    monkeypatch.setattr(Database, "_folder_subtree_ids_by_path", lambda self, fid: [])
    assert db.get_missing_photos(folder_id=froot) == []


def test_get_missing_photos_large_subtree_uses_temp_table(db, tmp_path, monkeypatch):
    froot, fsub, ids = _ghost_setup(db, tmp_path)
    monkeypatch.setattr(db_module, "_SQLITE_PARAM_CHUNK_SIZE", 1)
    staged = []
    real = Database._stage_scope_ids
    monkeypatch.setattr(
        Database, "_stage_scope_ids",
        lambda self, name, values: (staged.append((name, sorted(values))), real(self, name, values))[1],
    )
    got = [r["id"] for r in db.get_missing_photos(folder_id=froot)]
    assert got == [ids["gone"], ids["sub_gone"]]
    assert staged == [("missing_subtree_ids", sorted([froot, fsub]))]


# -- relocate / merge ---------------------------------------------------------


def test_relocate_folder_rejects_conflict_with_ok_source(db):
    a = db.add_folder("/rel/a")
    b = db.add_folder("/rel/b")
    with pytest.raises(ValueError, match=f"Path is already tracked as folder {b}"):
        db.relocate_folder(a, "/rel/b")


def test_relocate_folder_conflict_revalidates_missing_source_that_came_back(db, tmp_path):
    back = tmp_path / "back"
    back.mkdir()
    a = db.add_folder(str(back))
    b = db.add_folder("/rel/b")
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (a,))
    db.conn.commit()
    with pytest.raises(ValueError, match=f"Path is already tracked as folder {b}"):
        db.relocate_folder(a, "/rel/b")
    with _reader(db) as r:
        assert r.execute("SELECT status FROM folders WHERE id = ?", (a,)).fetchone()[0] == "ok"


def test_relocate_folder_conflict_merges_missing_source_via_facade(db, monkeypatch):
    a = db.add_folder("/rel/gone")
    b = db.add_folder("/rel/b")
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (a,))
    db.conn.commit()
    calls = []
    monkeypatch.setattr(
        Database, "_merge_into_existing",
        lambda self, *args, **kw: (calls.append((args, kw)), ["merged"])[1],
    )
    assert db.relocate_folder(a, "/rel/b") == ["merged"]
    assert calls == [((a, b, "/rel/b"), {})]


def test_relocate_folder_moves_row_cascades_children_and_rebases(db, tmp_path, monkeypatch):
    new_root = tmp_path / "new"
    (new_root / "kid" / "deep").mkdir(parents=True)
    (new_root / "clash").mkdir()
    (new_root / "clash" / "below").mkdir()
    holder = db.add_folder(str(tmp_path))
    old = db.add_folder("/old", parent_id=None)
    kid = db.add_folder("/old/kid", parent_id=old, workspace_root=False)
    deep = db.add_folder("/old/kid/deep", parent_id=kid, workspace_root=False)
    clash = db.add_folder("/old/clash", parent_id=old, workspace_root=False)
    below = db.add_folder("/old/clash/below", parent_id=clash, workspace_root=False)
    absent = db.add_folder("/old/absent", parent_id=old, workspace_root=False)
    ok_child = db.add_folder("/old/fine", parent_id=old, workspace_root=False)
    taken = db.add_folder(str(new_root / "clash"))
    db.conn.execute(
        "UPDATE folders SET status = 'missing' WHERE id IN (?, ?, ?, ?, ?, ?)",
        (old, kid, deep, clash, below, absent),
    )
    db.conn.commit()
    p1 = _photo(db, holder, "p1.jpg")
    p2 = _photo(db, holder, "p2.jpg")
    p3 = _photo(db, holder, "p3.jpg")
    _set_provenance(db, p1, "/old")
    _set_provenance(db, p2, "/old/kid")
    _set_provenance(db, p3, "/old/absent")

    relinked = []
    real = Database._relink_parents_by_path
    monkeypatch.setattr(
        Database, "_relink_parents_by_path",
        lambda self, ids: (relinked.append(list(ids)), real(self, ids))[1],
    )
    cascaded = db.relocate_folder(old, str(new_root))
    assert cascaded == [
        {"id": kid, "old_path": "/old/kid", "new_path": str(new_root / "kid")},
        {"id": deep, "old_path": "/old/kid/deep", "new_path": str(new_root / "kid" / "deep")},
    ]
    assert relinked == [[old, kid, deep]]
    with _reader(db) as r:
        rows = {row["id"]: row for row in r.execute("SELECT id, path, status, parent_id FROM folders")}
        prov = dict(r.execute("SELECT id, last_move_source_folder_path FROM photos").fetchall())
    assert (rows[old]["path"], rows[old]["status"]) == (str(new_root), "ok")
    assert rows[old]["parent_id"] == holder
    assert rows[kid]["parent_id"] == old
    assert rows[clash]["status"] == "missing" and rows[below]["status"] == "missing"
    assert rows[below]["path"] == "/old/clash/below"  # descendant of a conflict: skipped
    assert rows[absent]["status"] == "missing"
    assert rows[ok_child]["path"] == "/old/fine"  # only missing children cascade
    assert rows[taken]["path"] == str(new_root / "clash")
    assert prov == {p1: str(new_root), p2: str(new_root / "kid"), p3: "/old/absent"}
    assert not db.conn.in_transaction


def test_relocate_folder_unknown_id_and_same_path(db):
    a = db.add_folder("/same")
    assert db.relocate_folder(a, "/same") == []
    assert db.relocate_folder(99999, "/nowhere") == []
    assert _folder_row(db, a)["status"] == "ok"


def test_merge_into_existing_moves_drops_and_transfers(db, tmp_path, monkeypatch, cache):
    ws = db._ws_id()
    other_ws = db.create_workspace("Other")
    target_dir = tmp_path / "target"
    (target_dir / "kid").mkdir(parents=True)
    (target_dir / "clash" / "below").mkdir(parents=True)
    (target_dir / "moved.jpg").write_bytes(b"x")
    target = db.add_folder(str(target_dir), workspace_root=False)
    source = db.add_folder("/src")
    _link(db, other_ws, source, 1)
    kid = db.add_folder("/src/kid", parent_id=source, workspace_root=False)
    clash = db.add_folder("/src/clash", parent_id=source, workspace_root=False)
    below = db.add_folder("/src/clash/below", parent_id=clash, workspace_root=False)
    taken = db.add_folder(str(target_dir / "clash"))
    db.conn.execute(
        "UPDATE folders SET status = 'missing' WHERE id IN (?, ?, ?, ?)",
        (source, kid, clash, below),
    )
    db.conn.commit()
    survivor = _photo(db, target, "dup.jpg")
    dup = _photo(db, source, "dup.jpg")
    moved = _photo(db, source, "moved.jpg")
    phantom = _photo(db, source, "phantom.jpg")
    kw = db.add_keyword("K")
    db.tag_photo(dup, kw)
    witness = _photo(db, target, "w.jpg")
    _set_provenance(db, witness, "/src")
    kid_witness = _photo(db, target, "kw.jpg")
    _set_provenance(db, kid_witness, "/src/kid")

    transfers = []
    monkeypatch.setattr(
        Database, "_transfer_gps_review_for_merge",
        lambda self, losing, surviving: transfers.append((losing, surviving)),
    )
    relinked = []
    real = Database._relink_parents_by_path
    monkeypatch.setattr(
        Database, "_relink_parents_by_path",
        lambda self, ids: (relinked.append(list(ids)), real(self, ids))[1],
    )
    cascaded = db._merge_into_existing(source, target, str(target_dir))
    assert cascaded == [{"id": kid, "old_path": "/src/kid", "new_path": str(target_dir / "kid")}]
    assert transfers == [(dup, survivor)]
    assert relinked == [[kid]]
    with _reader(db) as r:
        photos = dict(r.execute("SELECT id, folder_id FROM photos").fetchall())
        folders = {row["id"]: row for row in r.execute("SELECT * FROM folders")}
        prov = dict(r.execute("SELECT id, last_move_source_folder_path FROM photos").fetchall())
        pk = r.execute("SELECT COUNT(*) FROM photo_keywords WHERE photo_id = ?", (dup,)).fetchone()[0]
        links = set(r.execute(
            "SELECT workspace_id, folder_id, is_root FROM workspace_folders "
            "WHERE folder_id IN (?, ?)", (source, target),
        ).fetchall())
    assert dup not in photos and phantom not in photos and pk == 0
    assert photos[moved] == target
    assert source not in folders
    assert (folders[target]["status"], folders[target]["photo_count"]) == ("ok", 4)
    assert folders[clash]["parent_id"] == target  # reparented from the source
    assert folders[below]["path"] == "/src/clash/below"
    assert folders[kid]["status"] == "ok"
    assert {tuple(x) for x in links} == {(ws, target, 1), (other_ws, target, 1)}
    assert prov[witness] is None
    assert prov[kid_witness] == str(target_dir / "kid")
    assert taken in folders


def test_merge_into_existing_without_commit_leaves_transaction_open(db, tmp_path):
    target = db.add_folder(str(tmp_path))
    source = db.add_folder("/gone-src")
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (source,))
    db.conn.commit()
    assert db._merge_into_existing(source, target, str(tmp_path), commit=False) == []
    assert db.conn.in_transaction
    db.conn.rollback()
    assert _folder_row(db, source) is not None


def test_merge_into_existing_preserves_non_root_link(db, tmp_path):
    ws = db._ws_id()
    target = db.add_folder(str(tmp_path), link_to_workspace=False)
    source = db.add_folder("/nr-src", workspace_root=False)
    db._merge_into_existing(source, target, str(tmp_path))
    assert _links(db, target) == {(ws, 0)}


# -- delete_folder ------------------------------------------------------------


def test_folders_linked_in_other_workspace(db):
    ws = db._ws_id()
    other = db.create_workspace("Other")
    a = db.add_folder("/l/a")
    b = db.add_folder("/l/b")
    _link(db, other, b, 0)
    assert db._folders_linked_in_other_workspace([a, b], ws) == {b}
    assert db._folders_linked_in_other_workspace([a, b], None) == {a, b}
    assert db._folders_linked_in_other_workspace([], ws) == set()


def test_folders_linked_in_other_workspace_chunks(db):
    other = db.create_workspace("Other")
    b = db.add_folder("/l/b")
    _link(db, other, b, 1)
    ids = list(range(100000, 100000 + db_module._SQLITE_PARAM_CHUNK_SIZE + 5)) + [b]
    statements = []
    db.conn.set_trace_callback(statements.append)
    assert db._folders_linked_in_other_workspace(ids, db._ws_id()) == {b}
    db.conn.set_trace_callback(None)
    assert len([s for s in statements if "FROM workspace_folders" in s]) == 2


def test_delete_folder_removes_subtree_photos_and_provenance(db, cache, monkeypatch):
    ws = db._ws_id()
    root = db.add_folder("/d")
    child = db.add_folder("/d/c", parent_id=root, workspace_root=False)
    legacy = db.add_folder("/d/legacy", workspace_root=False)  # NULL parent_id
    keep = db.add_folder("/keep")
    p1 = _photo(db, root, "1.jpg")
    p2 = _photo(db, child, "2.jpg")
    survivor = _photo(db, keep, "s.jpg")
    _set_provenance(db, survivor, "/d/c")

    delete_calls = []
    real_delete = Database.delete_photos

    def recording_delete(self, ids, *a, **kw):
        delete_calls.append((sorted(ids), a, kw))
        return real_delete(self, ids, *a, **kw)

    monkeypatch.setattr(Database, "delete_photos", recording_delete)
    pruned = []
    monkeypatch.setattr(Database, "prune_pipeline_cache_for_ids", lambda self, ids: pruned.append(sorted(ids)))

    result = db.delete_folder(root)
    assert result["deleted_photos"] == 2
    assert sorted(f["photo_id"] for f in result["files"]) == sorted([p1, p2])
    assert delete_calls == [(sorted([p1, p2]), (), {"commit": False})]
    assert pruned == [sorted([p1, p2])]
    assert (db._db_path, {ws}) in cache.invalidated
    with _reader(db) as r:
        left = {row[0] for row in r.execute("SELECT id FROM folders")}
        assert r.execute("SELECT COUNT(*) FROM workspace_folders WHERE folder_id IN (?, ?, ?)",
                         (root, child, legacy)).fetchone()[0] == 0
        assert r.execute(
            "SELECT last_move_source_folder_path FROM photos WHERE id = ?", (survivor,)
        ).fetchone()[0] is None
    assert left == {keep}


def test_delete_folder_protected_target_is_only_unlinked(db, cache):
    ws = db._ws_id()
    other = db.create_workspace("Other")
    root = db.add_folder("/pt")
    child = db.add_folder("/pt/c", parent_id=root, workspace_root=False)
    _link(db, other, root, 1)
    pid = _photo(db, child, "x.jpg")
    assert db.delete_folder(root) == {"deleted_photos": 0, "files": []}
    assert _links(db, root) == {(other, 1)}
    assert _links(db, child) == set()
    assert _folder_row(db, child) is not None
    assert db.conn.execute("SELECT 1 FROM photos WHERE id = ?", (pid,)).fetchone()
    removals = {
        r[0] for r in db.conn.execute(
            "SELECT folder_id FROM workspace_folder_removals WHERE workspace_id = ?", (ws,)
        )
    }
    assert {root, child} <= removals
    assert (db._db_path, {ws}) in cache.invalidated


def test_delete_folder_protected_descendant_is_reparented(db, cache):
    other = db.create_workspace("Other")
    root = db.add_folder("/pd")
    kept = db.add_folder("/pd/kept", parent_id=root, workspace_root=False)
    kept_kid = db.add_folder("/pd/kept/k", parent_id=kept, workspace_root=False)
    gone = db.add_folder("/pd/gone", parent_id=root, workspace_root=False)
    _link(db, other, kept, 0)
    result = db.delete_folder(root)
    assert result == {"deleted_photos": 0, "files": []}
    assert _folder_row(db, root) is None and _folder_row(db, gone) is None
    assert _folder_row(db, kept)["parent_id"] is None
    assert _folder_row(db, kept_kid)["parent_id"] == kept
    assert _links(db, kept) == {(other, 0)}


def test_delete_folder_rolls_back_on_failure(db, cache, monkeypatch):
    root = db.add_folder("/rb")
    child = db.add_folder("/rb/c", parent_id=root, workspace_root=False)
    _photo(db, child, "x.jpg")
    pruned = []
    monkeypatch.setattr(Database, "prune_pipeline_cache_for_ids", lambda self, ids: pruned.append(ids))
    db.conn.execute(
        "CREATE TEMP TRIGGER no_folder_delete BEFORE DELETE ON folders "
        "BEGIN SELECT RAISE(ABORT, 'blocked'); END"
    )
    with pytest.raises(sqlite3.IntegrityError, match="blocked"):
        db.delete_folder(root)
    assert not db.conn.in_transaction
    assert _folder_row(db, root) is not None
    assert db.conn.execute("SELECT COUNT(*) FROM photos WHERE folder_id = ?", (child,)).fetchone()[0] == 1
    assert pruned == []  # the pipeline-cache prune only runs after a commit


def test_delete_folder_without_workspace_raises(db):
    fid = db.add_folder("/nows")
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError, match="No active workspace set"):
        db.delete_folder(fid)
    assert _folder_row(db, fid) is not None


def test_delete_folder_chunks_wide_subtrees(db, cache):
    n = db_module._SQLITE_PARAM_CHUNK_SIZE + 5
    root = db.add_folder("/wide")
    db.conn.executemany(
        "INSERT INTO folders (path, parent_id) VALUES (?, ?)",
        [(f"/wide/s{i}", root) for i in range(n)],
    )
    db.conn.commit()
    assert db.delete_folder(root) == {"deleted_photos": 0, "files": []}
    assert db.conn.execute("SELECT COUNT(*) FROM folders").fetchone()[0] == 0


# -- counts / quality ---------------------------------------------------------


def test_count_folders(db):
    db.add_folder("/c/a")
    b = db.add_folder("/c/b")
    c = db.add_folder("/c/c")
    _raw_folder(db, "/c/unlinked")
    db.conn.execute("UPDATE folders SET status = 'partial' WHERE id = ?", (b,))
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (c,))
    db.conn.commit()
    assert db.count_folders() == 2
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError):
        db.count_folders()


def test_get_folders_with_quality_data(db):
    ws = db._ws_id()
    root = db.add_folder("/q")
    sub = db.add_folder("/q/sub", parent_id=root, workspace_root=False)
    gap = _raw_folder(db, "/q/gap", root)
    below_gap = _raw_folder(db, "/q/gap/b", gap)
    _link(db, ws, below_gap, 0)
    empty = db.add_folder("/q/empty", parent_id=root, workspace_root=False)
    missing = db.add_folder("/q/missing", parent_id=root, workspace_root=False)
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (missing,))
    db.conn.commit()
    scored = [
        (root, "a.jpg", "2024-01-01T00:00:00"),
        (sub, "b.jpg", "2024-06-01T00:00:00"),
        (below_gap, "c.jpg", "2025-01-01T00:00:00"),
        (missing, "d.jpg", "2026-01-01T00:00:00"),
    ]
    for fid, name, ts in scored:
        pid = _photo(db, fid, name, timestamp=ts)
        db.conn.execute("UPDATE photos SET quality_score = 0.5 WHERE id = ?", (pid,))
    _photo(db, empty, "unscored.jpg", timestamp="2027-01-01T00:00:00")
    db.conn.commit()
    rows = db.get_folders_with_quality_data()
    assert [(r["id"], r["photo_count"], r["latest_photo"]) for r in rows] == [
        (below_gap, 1, "2025-01-01T00:00:00"),
        (sub, 1, "2024-06-01T00:00:00"),
        (root, 2, "2024-06-01T00:00:00"),
    ]
    assert set(rows[0].keys()) == {"id", "path", "name", "photo_count", "latest_photo"}
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError):
        db.get_folders_with_quality_data()


def test_update_folder_counts_recomputes_and_commits(db):
    a = db.add_folder("/u/a")
    b = db.add_folder("/u/b")
    _photo(db, a, "1.jpg")
    _photo(db, a, "2.jpg")
    db.conn.execute("UPDATE folders SET photo_count = 99")
    db.conn.commit()
    db.set_active_workspace(None)
    db.update_folder_counts()
    with _reader(db) as r:
        counts = dict(r.execute("SELECT id, photo_count FROM folders").fetchall())
    assert counts == {a: 2, b: 0}


# -- structure ----------------------------------------------------------------

_DELEGATING_FOLDER_METHODS = (
    "repair_missing_folder_parents",
    "repair_stale_folder_parents",
    "_folder_subtree_ids_by_path",
    "_local_source_descendant_ids",
    "add_folder",
    "get_folder_tree",
    "get_folder_subtree_ids",
    "get_folder",
    "check_folder_health",
    "get_missing_folders",
    "get_missing_photos",
    "nearest_ancestor_folder_id",
    "_relink_parents_by_path",
    "relocate_folder",
    "_merge_into_existing",
    "_folders_linked_in_other_workspace",
    "delete_folder",
    "count_folders",
    "get_folders_with_quality_data",
    "update_folder_counts",
)


def _method_ast(name):
    source = textwrap.dedent(inspect.getsource(getattr(Database, name)))
    return ast.parse(source).body[0]


@pytest.mark.parametrize("name", _DELEGATING_FOLDER_METHODS)
def test_folder_method_delegates_to_repository(name):
    fn = _method_ast(name)
    attrs = {
        node.attr
        for node in ast.walk(fn)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }
    assert "conn" not in attrs, (
        f"Database.{name} touches self.conn; move the SQL to FolderRepository"
    )
    assert "_folder_repository" in attrs, (
        f"Database.{name} no longer delegates to FolderRepository"
    )


# Façade methods a folder wrapper hands to the repository, so the call still
# routes through ``Database`` (and its monkeypatches) at the same point in the
# SQL.
_FACADE_CALLBACKS = {
    "repair_stale_folder_parents": {"nearest_ancestor_id": "nearest_ancestor_folder_id"},
    "_folder_subtree_ids_by_path": {
        "local_source_descendant_ids": "_local_source_descendant_ids",
    },
    "_relink_parents_by_path": {"nearest_ancestor_id": "nearest_ancestor_folder_id"},
    "relocate_folder": {
        "merge_into_existing": "_merge_into_existing",
        "relink_parents_by_path": "_relink_parents_by_path",
    },
    "_merge_into_existing": {
        "transfer_gps_review": "_transfer_gps_review_for_merge",
        "relink_parents_by_path": "_relink_parents_by_path",
    },
    "delete_folder": {
        "subtree_ids_by_path": "_folder_subtree_ids_by_path",
        "linked_in_other_workspace": "_folders_linked_in_other_workspace",
        "delete_photos": "delete_photos",
        "remember_removals": "_remember_workspace_folder_removals",
    },
}


@pytest.mark.parametrize("name", sorted(_FACADE_CALLBACKS))
def test_folder_wrapper_passes_facade_methods_as_callbacks(name):
    passed = {
        kw.arg: kw.value.attr
        for node in ast.walk(_method_ast(name))
        if isinstance(node, ast.Call)
        for kw in node.keywords
        if isinstance(kw.value, ast.Attribute)
        and isinstance(kw.value.value, ast.Name)
        and kw.value.value.id == "self"
    }
    for kwarg, facade in _FACADE_CALLBACKS[name].items():
        assert passed.get(kwarg) == facade, (name, kwarg)


def test_add_folder_keeps_the_workspace_link_on_the_facade():
    calls = {
        node.func.attr
        for node in ast.walk(_method_ast("add_folder"))
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }
    assert "add_workspace_folder" in calls


def test_folder_repository_imports_no_db_code():
    import repositories.folders as folders_module

    tree = ast.parse(inspect.getsource(folders_module))
    imported = {
        alias.name.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        node.module.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module
    }
    assert "db" not in imported


def test_folder_repository_chunks_by_its_chunk_size(db):
    from repositories.folders import FolderRepository

    repo = FolderRepository(
        db.conn, None, commit_with_retry=None, path_for_subtree_match=None,
        stored_parent_path=None, subtree_prefix=None, subtree_relative=None,
        join_subtree_path=None, chunk_size=2,
    )
    assert list(repo._chunks(range(5))) == [[0, 1], [2, 3], [4]]
    assert list(repo._chunks([])) == []
