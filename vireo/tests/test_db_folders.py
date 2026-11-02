"""Behavior pins for the folder domain of ``Database``.

The behavior tests exercise the folder methods only through the public
``Database`` façade, so they hold regardless of whether the SQL lives in
``db.py`` or in ``repositories/folders.py``. They cover folder rows and
trees, parent repair, folder health and missing originals, relocation and
merge into an existing folder, ``delete_folder``, counts, and the
quality-data folder list. Where a method composes other ``Database``
methods (``add_workspace_folder``, ``delete_photos``,
``folders.merge_into_existing``, ``moves_merge.transfer_gps_review``,
``folders.nearest_ancestor_id``), the tests patch the façade method to pin
that the call still routes through it.
"""

import ast
import contextlib
import inspect
import json
import logging
import os
import sqlite3
import textwrap
import unicodedata

import db as db_module
import pytest
from db import Database, MissingPhotosCancelled
from repositories.folders import FolderRepository
from repositories.moves_merge import MovesMergeRepository
from repositories.stats import StatsRepository
from services import catalog_deletion, missing_originals, photo_ingest, workspace_folders


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
    return photo_ingest.add_photo(
        db, folder_id=folder_id, filename=filename, extension=os.path.splitext(filename)[1],
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
    fid = workspace_folders.add_folder(db, "/p/a", name="a")
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
    parent = workspace_folders.add_folder(db, "/p", name="p")
    child = workspace_folders.add_folder(db, "/p/c", name="c", parent_id=parent, workspace_root=False)
    assert _links(db, child) == {(ws, 0)}
    detached = workspace_folders.add_folder(db, "/q", name="q", link_to_workspace=False)
    assert _links(db, detached) == set()
    db.set_active_workspace(None)
    no_ws = workspace_folders.add_folder(db, "/r", name="r")
    assert _links(db, no_ws) == set()


def test_add_folder_routes_workspace_link_through_facade(db, monkeypatch):
    calls = []
    monkeypatch.setattr(
        workspace_folders, "add_workspace_folder",
        lambda self, *a, **kw: calls.append((a, kw)),
    )
    ws = db._ws_id()
    fid = workspace_folders.add_folder(db, "/p/a", workspace_root=False)
    assert calls == [((ws, fid), {"is_root": False, "restore_removed": False})]


def test_add_folder_existing_path_returns_id_and_backfills_missing_parent(db, monkeypatch):
    parent = workspace_folders.add_folder(db, "/p")
    child = workspace_folders.add_folder(db, "/p/c")
    assert _folder_row(db, child)["parent_id"] is None

    commits = _unit_commit_recorder(monkeypatch)
    assert workspace_folders.add_folder(db, "/p/c", parent_id=parent) == child
    # The INSERT OR IGNORE and the parent backfill land in add_folder's one unit.
    assert len(commits) == 1
    with _reader(db) as r:
        assert r.execute("SELECT parent_id FROM folders WHERE id = ?", (child,)).fetchone()[0] == parent

    # An existing parent is never overwritten, and a self-parent is refused.
    other = workspace_folders.add_folder(db, "/o")
    commits.clear()
    assert workspace_folders.add_folder(db, "/p/c", parent_id=other) == child
    assert _folder_row(db, child)["parent_id"] == parent
    assert workspace_folders.add_folder(db, "/o", parent_id=other) == other
    assert _folder_row(db, other)["parent_id"] is None
    assert len(commits) == 2  # one unit per add_folder call


def _unit_commit_recorder(monkeypatch):
    """Record each unit-of-work commit (the data layer itself never commits)."""
    import unit_of_work

    commits = []
    real = unit_of_work._commit
    monkeypatch.setattr(
        unit_of_work, "_commit", lambda conn: (commits.append(conn), real(conn))[1],
    )
    return commits


def test_add_folder_keeps_the_row_and_its_workspace_link_together(db, monkeypatch):
    """The folder row and its workspace link are one unit of work.

    The row used to commit before the link was written, so a failure while
    linking left a catalog folder no workspace could see.
    """
    from repositories.workspace_folders import WorkspaceFolderRepository

    def fail(*args, **kwargs):
        raise sqlite3.OperationalError("disk I/O error")

    monkeypatch.setattr(WorkspaceFolderRepository, "add_subtree", fail)
    with pytest.raises(sqlite3.OperationalError):
        workspace_folders.add_folder(db, "/linked")
    assert not db.conn.in_transaction
    with _reader(db) as r:
        assert r.execute("SELECT COUNT(*) FROM folders WHERE path = '/linked'").fetchone()[0] == 0


def test_add_folder_new_row_commits_once(db, monkeypatch):
    commits = _unit_commit_recorder(monkeypatch)
    workspace_folders.add_folder(db, "/new", link_to_workspace=False)
    assert len(commits) == 1


# -- folders.get / folders.tree / get_folder_subtree_ids --------------------


def test_get_folder_returns_row_or_none(db):
    fid = workspace_folders.add_folder(db, "/p/a", name="a")
    row = db.folders.get(fid)
    assert set(row.keys()) == {"id", "path", "name", "parent_id", "status", "photo_count"}
    assert (row["id"], row["path"], row["name"], row["status"]) == (fid, "/p/a", "a", "ok")
    assert db.folders.get(fid + 999) is None
    db.set_active_workspace(None)
    assert db.folders.get(fid)["id"] == fid  # catalog-wide


def test_get_folder_paths_is_catalog_wide(db, monkeypatch):
    assert db.folders.paths_by_id([]) == {}
    a = workspace_folders.add_folder(db, "/p/a", name="a")
    b = _raw_folder(db, "/p/b")  # linked to no workspace
    db.set_active_workspace(None)
    assert db.folders.paths_by_id([a, b, a + b + 999]) == {a: "/p/a", b: "/p/b"}
    monkeypatch.setattr(db_module, "_SQLITE_PARAM_CHUNK_SIZE", 1)
    statements = []
    db.conn.set_trace_callback(statements.append)
    assert db.folders.paths_by_id([a, b]) == {a: "/p/a", b: "/p/b"}
    db.conn.set_trace_callback(None)
    assert len([s for s in statements if "SELECT id, path FROM folders" in s]) == 2


def test_get_folder_tree_filters_status_and_rewrites_parents(db):
    ws = db._ws_id()
    root = workspace_folders.add_folder(db, "/t", name="t")
    hidden = _raw_folder(db, "/t/hidden", root)  # not linked to the workspace
    grand = _raw_folder(db, "/t/hidden/g", hidden)
    _link(db, ws, grand, 0)
    partial = workspace_folders.add_folder(db, "/t/partial", parent_id=root, workspace_root=False)
    db.conn.execute("UPDATE folders SET status = 'partial' WHERE id = ?", (partial,))
    missing = workspace_folders.add_folder(db, "/t/missing", parent_id=root, workspace_root=False)
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (missing,))
    under_missing = workspace_folders.add_folder(db, "/t/missing/u", parent_id=missing, workspace_root=False)
    orphan_parent = _raw_folder(db, "/x")
    orphan = _raw_folder(db, "/x/o", orphan_parent)
    _link(db, ws, orphan, 1)
    db.conn.commit()

    rows = db.folders.tree()
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
        db.folders.tree()


def test_get_folder_subtree_ids_walks_hidden_nodes_but_returns_only_visible_descendants(db):
    ws = db._ws_id()
    root = workspace_folders.add_folder(db, "/s")
    a = workspace_folders.add_folder(db, "/s/a", parent_id=root, workspace_root=False)
    a1 = workspace_folders.add_folder(db, "/s/a/1", parent_id=a, workspace_root=False)
    gap = _raw_folder(db, "/s/gap", root)
    below_gap = _raw_folder(db, "/s/gap/b", gap)
    _link(db, ws, below_gap, 0)
    assert sorted(db.get_folder_subtree_ids(root)) == sorted([root, a, a1, below_gap])
    assert db.get_folder_subtree_ids(root)[0] == root
    # A stale unlinked seed still cannot expand into visible descendants.
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
    ids = db.folders.subtree_ids_by_path(root)
    assert sorted(ids) == sorted([root, legacy, win, staged])
    assert sibling not in ids
    assert db.folders.subtree_ids_by_path(99999) == [99999]
    empty = _raw_folder(db, "")
    assert db.folders.subtree_ids_by_path(empty) == [empty]


def test_folder_subtree_ids_by_path_routes_local_lookup_through_facade(db, monkeypatch):
    root = _raw_folder(db, "/lib/root")
    seen = []
    monkeypatch.setattr(
        FolderRepository, "local_source_descendant_ids",
        lambda self, path: (seen.append(path), [4242])[1],
    )
    assert sorted(db.folders.subtree_ids_by_path(root)) == sorted([root, 4242])
    assert seen == ["/lib/root"]


def test_local_source_descendant_ids(db):
    a = _raw_folder(db, "/local/a")
    b = _raw_folder(db, "/local/b")
    c = _raw_folder(db, "/local/c")
    _map_local(db, a, "/src/root", "/local/a")
    _map_local(db, b, "C:\\src\\root\\b", "/local/b")
    _map_local(db, c, "/src/rootish", "/local/c")
    assert db.folders.local_source_descendant_ids("") == []
    assert db.folders.local_source_descendant_ids(None) == []
    got = db.folders.local_source_descendant_ids("/src/root")
    assert got == [a]
    assert all(isinstance(x, int) for x in got)
    assert db.folders.local_source_descendant_ids("C:/src/root") == [b]


# -- nearest ancestor / parent repair -----------------------------------------


def test_nearest_ancestor_folder_id(db):
    top = _raw_folder(db, "/n")
    mid = _raw_folder(db, "/n/m/")
    _raw_folder(db, "/nm")
    assert db.folders.nearest_ancestor_id("/n/m/x") == mid
    assert db.folders.nearest_ancestor_id("\\n\\m\\x") == mid
    assert db.folders.nearest_ancestor_id("/n/m/x", exclude_id=mid) == top
    assert db.folders.nearest_ancestor_id("/n") is None  # equal is not an ancestor
    assert db.folders.nearest_ancestor_id("/elsewhere/x") is None


def test_relink_parents_by_path_does_not_commit_and_skips_unknown(db, monkeypatch):
    top = _raw_folder(db, "/r")
    child = _raw_folder(db, "/r/c")
    calls = []
    real = FolderRepository.nearest_ancestor_id

    def recording(self, path, exclude_id=None):
        calls.append((path, exclude_id))
        return real(self, path, exclude_id=exclude_id)

    monkeypatch.setattr(FolderRepository, "nearest_ancestor_id", recording)
    db.folders.relink_parents_by_path([child, 99999])
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
    db.folders.repair_missing_parents()
    db.commit()
    with _reader(db) as r:
        rows = dict(r.execute("SELECT id, parent_id FROM folders").fetchall())
    assert rows[child] == top
    assert rows[top] == root
    assert rows[orphan] is None
    assert rows[root] is None  # "/" has no parent path; never self-parented
    # No-op: nothing to update, no transaction left open.
    db.folders.repair_missing_parents()
    db.commit()
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
    real = FolderRepository.nearest_ancestor_id
    monkeypatch.setattr(
        FolderRepository, "nearest_ancestor_id",
        lambda self, path, exclude_id=None: (
            calls.append((path, exclude_id)), real(self, path, exclude_id=exclude_id)
        )[1],
    )
    with caplog.at_level(logging.INFO, logger=db_module.log.name), db.transaction(immediate=True):
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
    with caplog.at_level(logging.INFO, logger=db_module.log.name), db.transaction(immediate=True):
        assert db.repair_stale_folder_parents() == 0
    assert "Repaired" not in caplog.text
    assert not db.conn.in_transaction


# -- health / missing ---------------------------------------------------------


def test_check_folder_health_updates_status_and_clears_provenance(db, tmp_path):
    present = tmp_path / "present"
    present.mkdir()
    ok_gone = workspace_folders.add_folder(db, str(tmp_path / "gone"))
    partial = workspace_folders.add_folder(db, str(present))
    db.conn.execute("UPDATE folders SET status = 'partial' WHERE id = ?", (partial,))
    back = workspace_folders.add_folder(db, str(tmp_path))
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (back,))
    partial_gone = workspace_folders.add_folder(db, str(tmp_path / "pg"))
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

    assert workspace_folders.check_folder_health(db) == 4
    with _reader(db) as r:
        status = dict(r.execute("SELECT id, status FROM folders").fetchall())
        prov = dict(r.execute("SELECT id, last_move_source_folder_path FROM photos").fetchall())
    assert status == {
        ok_gone: "missing", partial: "partial", back: "ok",
        partial_gone: "missing", empty: "missing",
    }
    assert prov == {pid: None, other: str(present)}
    # Second pass: nothing changes, nothing is left uncommitted.
    assert workspace_folders.check_folder_health(db) == 0
    assert not db.conn.in_transaction


def test_folder_health_interrupted_during_publish_rolls_back_status_and_provenance(db, tmp_path):
    gone = str(tmp_path / "gone")
    folder = workspace_folders.add_folder(db, gone)
    photo = _photo(db, folder, "bird.jpg")
    db.conn.execute("UPDATE photos SET last_move_source_folder_path=? WHERE id=?", (gone, photo))
    db.commit()

    class TransferStarted(Exception):
        pass

    def checkpoint():
        if db.conn.in_transaction:
            # A transfer starts after folder updates, before provenance and
            # commit. The pass must undo both changes instead of publishing.
            raise TransferStarted()

    with pytest.raises(TransferStarted):
        workspace_folders.check_folder_health(db, checkpoint=checkpoint)
    assert not db.conn.in_transaction
    assert db.folders.get(folder)["status"] == "ok"
    assert db.conn.execute(
        "SELECT last_move_source_folder_path FROM photos WHERE id=?", (photo,),
    ).fetchone()[0] == gone
    with _reader(db) as conn:
        assert conn.execute("SELECT status FROM folders WHERE id=?", (folder,)).fetchone()[0] == "ok"


def test_folder_health_does_not_overwrite_a_concurrently_relocated_folder(db, tmp_path, monkeypatch):
    gone = str(tmp_path / "gone")
    moved = str(tmp_path / "moved")
    folder = workspace_folders.add_folder(db, gone)
    photo = _photo(db, folder, "bird.jpg")
    db.conn.execute("UPDATE photos SET last_move_source_folder_path=? WHERE id=?", (gone, photo))
    db.commit()
    real_exists = os.path.exists

    def exists(path):
        if path == gone:
            with _reader(db) as conn:
                conn.execute("UPDATE folders SET path=? WHERE id=?", (moved, folder))
                conn.commit()
        return real_exists(path)

    monkeypatch.setattr(os.path, "exists", exists)
    assert db.folders.check_health() == 0
    assert db.folders.get(folder)["path"] == moved
    assert db.folders.get(folder)["status"] == "ok"
    assert db.conn.execute(
        "SELECT last_move_source_folder_path FROM photos WHERE id=?", (photo,),
    ).fetchone()[0] == gone


def test_check_folder_health_chunks_provenance_clear(db, tmp_path):
    n = db_module._SQLITE_PARAM_CHUNK_SIZE + 3
    db.conn.executemany(
        "INSERT INTO folders (path, status) VALUES (?, 'ok')",
        [(str(tmp_path / f"gone{i}"),) for i in range(n)],
    )
    db.conn.commit()
    statements = []
    db.conn.set_trace_callback(statements.append)
    assert db.folders.check_health() == n
    db.conn.set_trace_callback(None)
    clears = [s for s in statements if s.startswith("UPDATE photos SET last_move_source_folder_path = NULL")]
    assert len(clears) == 2


def test_get_missing_folders_scoped_with_counts(db):
    b = workspace_folders.add_folder(db, "/mf/b")
    a = workspace_folders.add_folder(db, "/mf/a")
    ok = workspace_folders.add_folder(db, "/mf/ok")
    foreign = _raw_folder(db, "/mf/foreign", status="missing")
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id IN (?, ?)", (a, b))
    db.conn.commit()
    _photo(db, a, "1.jpg")
    _photo(db, a, "2.jpg")
    rows = db.folders.missing()
    assert [(r["id"], r["photo_count"]) for r in rows] == [(a, 2), (b, 0)]
    assert set(rows[0].keys()) == {"id", "path", "name", "parent_id", "photo_count"}
    assert ok not in {r["id"] for r in rows} and foreign not in {r["id"] for r in rows}
    other = db.create_workspace("Other")
    db.commit()
    db.set_active_workspace(other)
    assert db.folders.missing() == []
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError):
        db.folders.missing()


def test_missing_photos_scan_lives_in_the_service():
    """The scan and the health-version read moved to ``services/missing_originals.py``."""
    for name in ("get_missing_photos", "get_folder_health_version", "_missing_photos_subtree_clause"):
        assert not hasattr(Database, name), f"Database.{name} came back"
    source = inspect.getsource(missing_originals.get_missing_photos)
    assert "db.folders.photos_in_present_folders" in source
    assert ".conn" not in source


def test_get_folder_health_version(db):
    assert missing_originals.get_folder_health_version(db) == 0
    db.meta.set("folder_health_version", "7")
    assert missing_originals.get_folder_health_version(db) == 7


def _ghost_setup(db, tmp_path):
    root = tmp_path / "root"
    sub = root / "sub"
    sub.mkdir(parents=True)
    froot = workspace_folders.add_folder(db, str(root))
    fsub = workspace_folders.add_folder(db, str(sub), parent_id=froot, workspace_root=False)
    (root / "here.jpg").write_bytes(b"x")
    here = _photo(db, froot, "here.jpg")
    gone = _photo(db, froot, "gone.jpg")
    (sub / "s.jpg").write_bytes(b"x")
    sub_here = _photo(db, fsub, "s.jpg")
    sub_gone = _photo(db, fsub, "t.jpg")
    return froot, fsub, {"here": here, "gone": gone, "sub_here": sub_here, "sub_gone": sub_gone}


def test_get_missing_photos_whole_workspace_and_subtree(db, tmp_path):
    froot, fsub, ids = _ghost_setup(db, tmp_path)
    rows = missing_originals.get_missing_photos(db)
    assert [r["id"] for r in rows] == [ids["gone"], ids["sub_gone"]]
    assert set(rows[0].keys()) == {
        "id", "filename", "extension", "file_size", "timestamp",
        "working_copy_path", "folder_id", "folder_path",
    }
    assert [r["id"] for r in missing_originals.get_missing_photos(db, folder_id=fsub)] == [ids["sub_gone"]]
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError):
        missing_originals.get_missing_photos(db)


def test_get_missing_photos_skips_missing_and_offline_folders(db, tmp_path):
    froot, fsub, ids = _ghost_setup(db, tmp_path)
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (fsub,))
    offline = workspace_folders.add_folder(db, str(tmp_path / "offline"))
    _photo(db, offline, "o.jpg")
    db.conn.commit()
    assert [r["id"] for r in missing_originals.get_missing_photos(db)] == [ids["gone"]]


def test_get_missing_photos_unreadable_folder_is_treated_as_offline(db, tmp_path, monkeypatch):
    froot, fsub, ids = _ghost_setup(db, tmp_path)
    real_scandir = os.scandir
    sub_path = db.folders.get(fsub)["path"]

    def scandir(path):
        if path == sub_path:
            raise PermissionError("nope")
        return real_scandir(path)

    monkeypatch.setattr(os, "scandir", scandir)
    progress = []
    rows = missing_originals.get_missing_photos(db, progress_callback=progress.append)
    assert [r["id"] for r in rows] == [ids["gone"]]
    assert progress[-1] == {
        "folders_checked": 2, "photos_considered": 4, "missing_found": 1,
        "total_photos": 4, "current_folder": "",
    }


def test_get_missing_photos_nfc_symlink_and_case_fallback(db, tmp_path):
    folder = tmp_path / "nfc"
    folder.mkdir()
    fid = workspace_folders.add_folder(db, str(folder))
    nfd = unicodedata.normalize("NFD", "café.jpg")
    (folder / nfd).write_bytes(b"x")
    nfc_photo = _photo(db, fid, unicodedata.normalize("NFC", "café.jpg"))
    (folder / "target.jpg").write_bytes(b"x")
    os.symlink(folder / "target.jpg", folder / "link.jpg")
    os.symlink(folder / "nowhere.jpg", folder / "broken.jpg")
    link = _photo(db, fid, "link.jpg")
    broken = _photo(db, fid, "broken.jpg")
    missing = {r["id"] for r in missing_originals.get_missing_photos(db)}
    assert broken in missing
    assert nfc_photo not in missing and link not in missing


def test_get_missing_photos_progress_and_callback_failure(db, tmp_path, monkeypatch, caplog):
    froot, fsub, ids = _ghost_setup(db, tmp_path)
    monkeypatch.setattr(missing_originals, "_MISSING_PHOTOS_PROGRESS_INTERVAL", 1)
    seen = []

    def boom(payload):
        seen.append(payload)
        raise RuntimeError("callback broke")

    with caplog.at_level(logging.ERROR, logger=missing_originals.log.name):
        rows = missing_originals.get_missing_photos(db, progress_callback=boom)
    assert [r["id"] for r in rows] == [ids["gone"], ids["sub_gone"]]
    assert len(seen) == 1  # disabled after the first failure
    assert "Missing photos progress callback failed" in caplog.text

    progress = []
    missing_originals.get_missing_photos(db, progress_callback=progress.append)
    assert progress[0]["folders_checked"] == 1
    assert progress[-1]["current_folder"] == ""
    assert progress[-1]["missing_found"] == 2


def test_get_missing_photos_cancel(db, tmp_path):
    _ghost_setup(db, tmp_path)
    with pytest.raises(MissingPhotosCancelled):
        missing_originals.get_missing_photos(db, cancel_callback=lambda: True)
    polls = []

    def cancel_later():
        polls.append(1)
        return len(polls) > 3

    with pytest.raises(MissingPhotosCancelled):
        missing_originals.get_missing_photos(db, cancel_callback=cancel_later)


def test_get_missing_photos_subtree_goes_through_facade(db, tmp_path, monkeypatch):
    froot, fsub, ids = _ghost_setup(db, tmp_path)
    monkeypatch.setattr(FolderRepository, "subtree_ids_by_path", lambda self, fid: [])
    assert missing_originals.get_missing_photos(db, folder_id=froot) == []


def test_get_missing_photos_large_subtree_uses_temp_table(db, tmp_path, monkeypatch):
    froot, fsub, ids = _ghost_setup(db, tmp_path)
    monkeypatch.setattr(missing_originals, "SQLITE_PARAM_CHUNK_SIZE", 1)
    staged = []
    real = StatsRepository.stage_scope_ids
    monkeypatch.setattr(
        StatsRepository, "stage_scope_ids",
        lambda self, name, values: (staged.append((name, sorted(values))), real(self, name, values))[1],
    )
    got = [r["id"] for r in missing_originals.get_missing_photos(db, folder_id=froot)]
    assert got == [ids["gone"], ids["sub_gone"]]
    assert staged == [("missing_subtree_ids", sorted([froot, fsub]))]


# -- relocate / merge ---------------------------------------------------------


def test_relocate_folder_rejects_conflict_with_ok_source(db):
    a = workspace_folders.add_folder(db, "/rel/a")
    b = workspace_folders.add_folder(db, "/rel/b")
    with pytest.raises(ValueError, match=f"Path is already tracked as folder {b}"):
        db.folders.relocate(a, "/rel/b")
        db.commit()


def test_relocate_folder_conflict_revalidates_missing_source_that_came_back(db, tmp_path):
    back = tmp_path / "back"
    back.mkdir()
    a = workspace_folders.add_folder(db, str(back))
    b = workspace_folders.add_folder(db, "/rel/b")
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (a,))
    db.conn.commit()
    from repositories.folders import OriginalPathReturned
    from services.folder_relocation import relocate_folder

    # The repository refuses without writing; the relocate service refreshes
    # the status in the unit that reports the refusal.
    cascaded, refusal = relocate_folder(db, a, "/rel/b")
    assert cascaded == []
    assert isinstance(refusal, OriginalPathReturned)
    assert str(refusal) == f"Path is already tracked as folder {b}"
    with _reader(db) as r:
        assert r.execute("SELECT status FROM folders WHERE id = ?", (a,)).fetchone()[0] == "ok"


def test_relocate_folder_conflict_merges_missing_source_via_facade(db, monkeypatch):
    a = workspace_folders.add_folder(db, "/rel/gone")
    b = workspace_folders.add_folder(db, "/rel/b")
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (a,))
    db.conn.commit()
    calls = []
    monkeypatch.setattr(
        FolderRepository, "merge_into_existing",
        lambda self, *args, **kw: (calls.append((args, kw)), ["merged"])[1],
    )
    assert db.folders.relocate(a, "/rel/b") == ["merged"]
    assert calls == [((a, b, "/rel/b"), {})]


def test_relocate_folder_moves_row_cascades_children_and_rebases(db, tmp_path, monkeypatch):
    new_root = tmp_path / "new"
    (new_root / "kid" / "deep").mkdir(parents=True)
    (new_root / "clash").mkdir()
    (new_root / "clash" / "below").mkdir()
    holder = workspace_folders.add_folder(db, str(tmp_path))
    old = workspace_folders.add_folder(db, "/old", parent_id=None)
    kid = workspace_folders.add_folder(db, "/old/kid", parent_id=old, workspace_root=False)
    deep = workspace_folders.add_folder(db, "/old/kid/deep", parent_id=kid, workspace_root=False)
    clash = workspace_folders.add_folder(db, "/old/clash", parent_id=old, workspace_root=False)
    below = workspace_folders.add_folder(db, "/old/clash/below", parent_id=clash, workspace_root=False)
    absent = workspace_folders.add_folder(db, "/old/absent", parent_id=old, workspace_root=False)
    ok_child = workspace_folders.add_folder(db, "/old/fine", parent_id=old, workspace_root=False)
    taken = workspace_folders.add_folder(db, str(new_root / "clash"))
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
    real = FolderRepository.relink_parents_by_path
    monkeypatch.setattr(
        FolderRepository, "relink_parents_by_path",
        lambda self, ids: (relinked.append(list(ids)), real(self, ids))[1],
    )
    cascaded = db.folders.relocate(old, str(new_root))
    db.commit()
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
    a = workspace_folders.add_folder(db, "/same")
    assert db.folders.relocate(a, "/same") == []
    assert db.folders.relocate(99999, "/nowhere") == []
    assert _folder_row(db, a)["status"] == "ok"


def test_merge_into_existing_moves_drops_and_transfers(db, tmp_path, monkeypatch, cache):
    ws = db._ws_id()
    other_ws = db.create_workspace("Other")
    db.commit()
    target_dir = tmp_path / "target"
    (target_dir / "kid").mkdir(parents=True)
    (target_dir / "clash" / "below").mkdir(parents=True)
    (target_dir / "moved.jpg").write_bytes(b"x")
    target = workspace_folders.add_folder(db, str(target_dir), workspace_root=False)
    source = workspace_folders.add_folder(db, "/src")
    _link(db, other_ws, source, 1)
    kid = workspace_folders.add_folder(db, "/src/kid", parent_id=source, workspace_root=False)
    clash = workspace_folders.add_folder(db, "/src/clash", parent_id=source, workspace_root=False)
    below = workspace_folders.add_folder(db, "/src/clash/below", parent_id=clash, workspace_root=False)
    taken = workspace_folders.add_folder(db, str(target_dir / "clash"))
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
    db.commit()
    witness = _photo(db, target, "w.jpg")
    _set_provenance(db, witness, "/src")
    kid_witness = _photo(db, target, "kw.jpg")
    _set_provenance(db, kid_witness, "/src/kid")

    transfers = []
    monkeypatch.setattr(
        MovesMergeRepository, "transfer_gps_review",
        lambda self, losing, surviving: transfers.append((losing, surviving)),
    )
    relinked = []
    real = FolderRepository.relink_parents_by_path
    monkeypatch.setattr(
        FolderRepository, "relink_parents_by_path",
        lambda self, ids: (relinked.append(list(ids)), real(self, ids))[1],
    )
    cascaded = db.folders.merge_into_existing(source, target, str(target_dir))
    db.commit()
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


def test_merge_into_existing_repoints_collection_photo_ids(db, tmp_path):
    other_ws = db.create_workspace("Other")
    db.commit()
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    target = workspace_folders.add_folder(db, str(target_dir), workspace_root=False)
    source = workspace_folders.add_folder(db, "/coll-src")
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (source,))
    db.conn.commit()
    survivor = _photo(db, target, "dup.jpg")
    dup = _photo(db, source, "dup.jpg")
    phantom = _photo(db, source, "phantom.jpg")
    coll = db.conn.execute(
        "INSERT INTO collections (name, rules, workspace_id) VALUES (?, ?, ?)",
        ("static", json.dumps([{"field": "photo_ids", "value": [dup, survivor, phantom]}]),
         other_ws),
    ).lastrowid
    db.conn.commit()

    db.folders.merge_into_existing(source, target, str(target_dir))
    db.commit()

    rules = json.loads(db.conn.execute(
        "SELECT rules FROM collections WHERE id = ?", (coll,),
    ).fetchone()[0])
    # The duplicate folds into its survivor (listed once); the phantom goes.
    assert rules == [{"field": "photo_ids", "value": [survivor]}]


def test_merge_into_existing_leaves_commit_to_caller(db, tmp_path):
    target = workspace_folders.add_folder(db, str(tmp_path))
    source = workspace_folders.add_folder(db, "/gone-src")
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (source,))
    db.conn.commit()
    assert db.folders.merge_into_existing(source, target, str(tmp_path)) == []
    assert db.conn.in_transaction
    db.conn.rollback()
    assert _folder_row(db, source) is not None


def test_merge_into_existing_preserves_non_root_link(db, tmp_path):
    ws = db._ws_id()
    target = workspace_folders.add_folder(db, str(tmp_path), link_to_workspace=False)
    source = workspace_folders.add_folder(db, "/nr-src", workspace_root=False)
    db.folders.merge_into_existing(source, target, str(tmp_path))
    db.commit()
    assert _links(db, target) == {(ws, 0)}


def test_merge_into_existing_moves_embedded_keyword_offered_onto_survivor(
        db, tmp_path):
    """A same-filename duplicate's embedded-offered suppression follows the survivor.

    Without the transfer, the ``DELETE FROM photos`` on the losing row
    aborts on the non-cascading FK, and a later rescan of the surviving
    row's embedded metadata would re-tag a keyword the user had already
    removed on the merged-in source.
    """
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    target = workspace_folders.add_folder(db, str(target_dir), workspace_root=False)
    source = workspace_folders.add_folder(db, "/src")
    db.conn.execute(
        "UPDATE folders SET status = 'missing' WHERE id = ?", (source,))
    db.conn.commit()
    survivor = _photo(db, target, "bird.jpg")
    dup = _photo(db, source, "bird.jpg")
    db.photo_keywords.record_embedded_offered_keys(dup, ["robin"])
    db.commit()
    assert db.photo_keywords.embedded_offered_keys(dup) == {"robin"}
    assert db.photo_keywords.embedded_offered_keys(survivor) == set()

    db.folders.merge_into_existing(source, target, str(target_dir))
    db.commit()

    with _reader(db) as r:
        offered_rows = r.execute(
            "SELECT photo_id, keyword_key "
            "FROM photo_embedded_keyword_offered",
        ).fetchall()
    assert [tuple(row) for row in offered_rows] == [(survivor, "robin")]


def test_merge_into_existing_drops_phantom_embedded_keyword_offered(
        db, tmp_path):
    """A phantom source row's suppression is cleared instead of transferred.

    When the source photo has no matching filename on disk at the target,
    the row is a phantom with no survivor to inherit anything, so the
    offered record is dropped. The DELETE must still run so the FK on
    ``photo_embedded_keyword_offered.photo_id`` does not abort the
    ``DELETE FROM photos``.
    """
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    target = workspace_folders.add_folder(db, str(target_dir), workspace_root=False)
    source = workspace_folders.add_folder(db, "/src")
    db.conn.execute(
        "UPDATE folders SET status = 'missing' WHERE id = ?", (source,))
    db.conn.commit()
    phantom = _photo(db, source, "gone.jpg")
    db.photo_keywords.record_embedded_offered_keys(phantom, ["robin"])
    db.commit()

    db.folders.merge_into_existing(source, target, str(target_dir))
    db.commit()

    with _reader(db) as r:
        offered_rows = r.execute(
            "SELECT photo_id, keyword_key "
            "FROM photo_embedded_keyword_offered",
        ).fetchall()
    assert offered_rows == []


def test_merge_into_existing_carries_source_keywords_onto_survivor(
        db, tmp_path):
    """A duplicate source row's attached keywords move to the survivor.

    Dropping source's keyword associations while transferring embedded
    suppression keys would leave the survivor marked as "already offered"
    for a key it doesn't actually carry, and every later embedded rescan
    would silently filter out the tag -- permanently losing a keyword
    that only the source row carried.
    """
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    target = workspace_folders.add_folder(db, str(target_dir), workspace_root=False)
    source = workspace_folders.add_folder(db, "/src")
    db.conn.execute(
        "UPDATE folders SET status = 'missing' WHERE id = ?", (source,))
    db.conn.commit()
    survivor = _photo(db, target, "bird.jpg")
    dup = _photo(db, source, "bird.jpg")
    # Source carries an embedded-imported Robin tag; the survivor has no
    # keyword yet. The suppression key is what a prior embedded scan of
    # ``bird.jpg`` on the source row recorded.
    robin_id = db.add_keyword("Robin")
    db.commit()
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id, source) "
        "VALUES (?, ?, NULL)",
        (dup, robin_id),
    )
    db.photo_keywords.record_embedded_offered_keys(dup, ["robin"])
    db.commit()
    db.conn.commit()

    db.folders.merge_into_existing(source, target, str(target_dir))
    db.commit()

    with _reader(db) as r:
        tag_names = {
            row["name"] for row in r.execute(
                "SELECT k.name FROM photo_keywords pk "
                "JOIN keywords k ON k.id = pk.keyword_id "
                "WHERE pk.photo_id = ?",
                (survivor,),
            )
        }
        offered_keys = {
            row["keyword_key"] for row in r.execute(
                "SELECT keyword_key FROM photo_embedded_keyword_offered "
                "WHERE photo_id = ?",
                (survivor,),
            )
        }
    # The attached Robin followed the suppression onto the survivor so a
    # later rescan sees a tag matching the offered key and skips re-import
    # (the correct behavior) instead of filtering a missing tag out
    # permanently.
    assert tag_names == {"Robin"}
    assert offered_keys == {"robin"}


@pytest.mark.parametrize("removed_on_survivor", [False, True])
@pytest.mark.parametrize("hierarchical", [False, True])
def test_folder_merge_respects_embedded_keyword_removals(
        db, tmp_path, removed_on_survivor, hierarchical):
    from scanner.keywords import _import_embedded_keywords_for_photo

    target_dir = tmp_path / "target"
    target_dir.mkdir()
    target = workspace_folders.add_folder(db, str(target_dir), workspace_root=False)
    source = workspace_folders.add_folder(db, str(tmp_path / "missing"))
    survivor = _photo(db, target, "bird.jpg")
    duplicate = _photo(db, source, "bird.jpg")
    metadata = {"XMP": ({"HierarchicalSubject": ["Birds|Robin"]} if hierarchical
                        else {"Subject": ["Robin"]})}
    _import_embedded_keywords_for_photo(db, duplicate, metadata)
    if removed_on_survivor:
        _import_embedded_keywords_for_photo(db, survivor, metadata)
        for keyword in db.photo_keywords.get_for_photo(survivor):
            db.untag_photo(survivor, keyword["id"])
            db.commit()

    db.folders.merge_into_existing(source, target, str(target_dir))
    db.commit()

    assert {k["name"] for k in db.photo_keywords.get_for_photo(survivor)} == (
        set() if removed_on_survivor else {"Robin"})
    assert not _import_embedded_keywords_for_photo(db, survivor, metadata)


# -- delete_folder ------------------------------------------------------------


def test_folders_linked_in_other_workspace(db):
    ws = db._ws_id()
    other = db.create_workspace("Other")
    db.commit()
    a = workspace_folders.add_folder(db, "/l/a")
    b = workspace_folders.add_folder(db, "/l/b")
    _link(db, other, b, 0)
    assert db.folders.linked_in_other_workspace([a, b], ws) == {b}
    assert db.folders.linked_in_other_workspace([a, b], None) == {a, b}
    assert db.folders.linked_in_other_workspace([], ws) == set()


def test_folders_linked_in_other_workspace_chunks(db):
    other = db.create_workspace("Other")
    db.commit()
    b = workspace_folders.add_folder(db, "/l/b")
    _link(db, other, b, 1)
    ids = list(range(100000, 100000 + db_module._SQLITE_PARAM_CHUNK_SIZE + 5)) + [b]
    statements = []
    db.conn.set_trace_callback(statements.append)
    assert db.folders.linked_in_other_workspace(ids, db._ws_id()) == {b}
    db.conn.set_trace_callback(None)
    assert len([s for s in statements if "FROM workspace_visible_folders" in s]) == 2


def test_delete_folder_removes_subtree_photos_and_provenance(db, cache, monkeypatch):
    ws = db._ws_id()
    root = workspace_folders.add_folder(db, "/d")
    child = workspace_folders.add_folder(db, "/d/c", parent_id=root, workspace_root=False)
    legacy = workspace_folders.add_folder(db, "/d/legacy", workspace_root=False)  # NULL parent_id
    keep = workspace_folders.add_folder(db, "/keep")
    p1 = _photo(db, root, "1.jpg")
    p2 = _photo(db, child, "2.jpg")
    survivor = _photo(db, keep, "s.jpg")
    _set_provenance(db, survivor, "/d/c")

    delete_calls = []
    real_delete = catalog_deletion.delete_photos

    def recording_delete(self, ids, *a, **kw):
        delete_calls.append((sorted(ids), a, kw))
        return real_delete(self, ids, *a, **kw)

    monkeypatch.setattr(catalog_deletion, "delete_photos", recording_delete)
    pruned = []
    monkeypatch.setattr(catalog_deletion, "prune_pipeline_cache_for_ids", lambda self, ids: pruned.append(sorted(ids)))

    result = catalog_deletion.delete_folder(db, root)
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
    db.commit()
    root = workspace_folders.add_folder(db, "/pt")
    child = workspace_folders.add_folder(db, "/pt/c", parent_id=root, workspace_root=False)
    _link(db, other, root, 1)
    pid = _photo(db, child, "x.jpg")
    assert catalog_deletion.delete_folder(db, root) == {"deleted_photos": 0, "files": []}
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
    db.commit()
    root = workspace_folders.add_folder(db, "/pd")
    kept = workspace_folders.add_folder(db, "/pd/kept", parent_id=root, workspace_root=False)
    kept_kid = workspace_folders.add_folder(db, "/pd/kept/k", parent_id=kept, workspace_root=False)
    gone = workspace_folders.add_folder(db, "/pd/gone", parent_id=root, workspace_root=False)
    _link(db, other, kept, 0)
    result = catalog_deletion.delete_folder(db, root)
    assert result == {"deleted_photos": 0, "files": []}
    assert _folder_row(db, root) is None and _folder_row(db, gone) is None
    assert _folder_row(db, kept)["parent_id"] is None
    assert _folder_row(db, kept_kid)["parent_id"] == kept
    assert _links(db, kept) == {(other, 0)}


def test_delete_folder_rolls_back_on_failure(db, cache, monkeypatch):
    root = workspace_folders.add_folder(db, "/rb")
    child = workspace_folders.add_folder(db, "/rb/c", parent_id=root, workspace_root=False)
    _photo(db, child, "x.jpg")
    pruned = []
    monkeypatch.setattr(catalog_deletion, "prune_pipeline_cache_for_ids", lambda self, ids: pruned.append(ids))
    db.conn.execute(
        "CREATE TEMP TRIGGER no_folder_delete BEFORE DELETE ON folders "
        "BEGIN SELECT RAISE(ABORT, 'blocked'); END"
    )
    with pytest.raises(sqlite3.IntegrityError, match="blocked"):
        catalog_deletion.delete_folder(db, root)
    assert not db.conn.in_transaction
    assert _folder_row(db, root) is not None
    assert db.conn.execute("SELECT COUNT(*) FROM photos WHERE folder_id = ?", (child,)).fetchone()[0] == 1
    assert pruned == []  # the pipeline-cache prune only runs after a commit


def test_delete_folder_without_workspace_raises(db):
    fid = workspace_folders.add_folder(db, "/nows")
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError, match="No active workspace set"):
        catalog_deletion.delete_folder(db, fid)
    assert _folder_row(db, fid) is not None


def test_delete_folder_chunks_wide_subtrees(db, cache):
    n = db_module._SQLITE_PARAM_CHUNK_SIZE + 5
    root = workspace_folders.add_folder(db, "/wide")
    db.conn.executemany(
        "INSERT INTO folders (path, parent_id) VALUES (?, ?)",
        [(f"/wide/s{i}", root) for i in range(n)],
    )
    db.conn.commit()
    assert catalog_deletion.delete_folder(db, root) == {"deleted_photos": 0, "files": []}
    assert db.conn.execute("SELECT COUNT(*) FROM folders").fetchone()[0] == 0


# -- counts / quality ---------------------------------------------------------


def test_count_folders(db):
    workspace_folders.add_folder(db, "/c/a")
    b = workspace_folders.add_folder(db, "/c/b")
    c = workspace_folders.add_folder(db, "/c/c")
    _raw_folder(db, "/c/unlinked")
    db.conn.execute("UPDATE folders SET status = 'partial' WHERE id = ?", (b,))
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (c,))
    db.conn.commit()
    assert db.folders.count() == 2
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError):
        db.folders.count()


def test_count_all_folders_is_catalog_wide(db):
    before = db.folders.count_all()
    workspace_folders.add_folder(db, "/every/a")
    _raw_folder(db, "/every/unlinked")
    _raw_folder(db, "/every/gone", status="missing")
    assert db.folders.count_all() == before + 3
    # Needs no active workspace.
    db.set_active_workspace(None)
    assert db.folders.count_all() == before + 3


def test_get_all_folders_is_catalog_wide(db):
    a = workspace_folders.add_folder(db, "/all/a", name="a")
    gone = _raw_folder(db, "/all/gone", status="missing", name="gone")
    rows = {r["id"]: dict(r) for r in db.folders.all_rows()}
    # Not scoped to a workspace or a status, and needs no active workspace.
    assert rows[a] == {"id": a, "path": "/all/a", "name": "a"}
    assert rows[gone] == {"id": gone, "path": "/all/gone", "name": "gone"}
    db.set_active_workspace(None)
    assert {r["id"] for r in db.folders.all_rows()} == set(rows)


def test_count_present_photos_under_path(db):
    root = workspace_folders.add_folder(db, "/arc", name="arc")
    child = workspace_folders.add_folder(db, "/arc/2024", name="2024", parent_id=root)
    partial = workspace_folders.add_folder(db, "/arc/partial", name="partial", parent_id=root)
    missing = workspace_folders.add_folder(db, "/arc/missing", name="missing", parent_id=root)
    unlinked = _raw_folder(db, "/arc/unlinked")
    windows = _raw_folder(db, "\\arc\\win")
    sibling = workspace_folders.add_folder(db, "/arcade", name="arcade")
    for fid, name in [(root, "r.jpg"), (child, "c.jpg"), (partial, "p.jpg"),
                      (missing, "m.jpg"), (unlinked, "u.jpg"),
                      (windows, "w.jpg"), (sibling, "s.jpg")]:
        _photo(db, fid, name)
    db.conn.execute("UPDATE folders SET status = 'partial' WHERE id = ?", (partial,))
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (missing,))
    db.conn.commit()
    # ok + partial folders anywhere in the catalog; the missing folder and the
    # prefix-sharing sibling are left out, and a backslash path is folded.
    assert db.folders.present_photo_count_under("/arc") == 5
    assert db.folders.present_photo_count_under("/arc/2024") == 1
    assert db.folders.present_photo_count_under("/nowhere") == 0
    db.set_active_workspace(None)
    assert db.folders.present_photo_count_under("/arc") == 5


def test_get_photo_ids_under_path(db):
    # Paths are spelled out rather than built with os.path, so the
    # separators under test are the same on every platform.
    root = workspace_folders.add_folder(db, "/photos/lib", name="lib")
    child = workspace_folders.add_folder(db, "/photos/lib/c", name="c", parent_id=root)
    missing = workspace_folders.add_folder(db, "/photos/lib/gone", name="gone", parent_id=root)
    unlinked = _raw_folder(db, "/photos/lib/unlinked")
    a = _photo(db, root, "a.jpg")
    b = _photo(db, root, "b.jpg")
    c = _photo(db, child, "c.jpg")
    m = _photo(db, missing, "m.jpg")
    u = _photo(db, unlinked, "u.jpg")
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (missing,))
    db.conn.commit()
    everything = sorted([a, b, c, m, u])
    # Any status, any workspace.
    assert sorted(db.folders.photo_ids_under("/photos/lib")) == everything
    # The root itself matches by exact stored path, so a trailing slash
    # finds only the descendants.
    assert sorted(db.folders.photo_ids_under("/photos/lib/")) == sorted([c, m, u])
    assert db.folders.photo_ids_under("/photos/lib/c") == [c]
    # A sibling sharing the prefix is not a descendant.
    sib = workspace_folders.add_folder(db, "/photos/library", name="library")
    _photo(db, sib, "s.jpg")
    assert sorted(db.folders.photo_ids_under("/photos/lib")) == everything
    assert db.folders.photo_ids_under("/photos/nowhere") == []
    db.set_active_workspace(None)
    assert sorted(db.folders.photo_ids_under("/photos/lib")) == everything


@pytest.mark.parametrize("sibling", ["/a/myXtrip", "/a/my-trip"])
def test_photo_ids_under_path_treats_underscore_literally(db, sibling):
    """``_`` in the root is a character, not LIKE's any-one-character."""
    trip = workspace_folders.add_folder(db, "/a/my_trip", name="my_trip")
    day = workspace_folders.add_folder(db, "/a/my_trip/day1", name="day1", parent_id=trip)
    other = workspace_folders.add_folder(db, sibling, name=sibling.rsplit("/", 1)[1])
    other_day = workspace_folders.add_folder(db, sibling + "/day1", name="day1", parent_id=other)
    mine = [_photo(db, trip, "t.jpg"), _photo(db, day, "d.jpg")]
    _photo(db, other, "x.jpg")
    _photo(db, other_day, "y.jpg")
    assert sorted(db.folders.photo_ids_under("/a/my_trip")) == sorted(mine)


def test_photo_ids_under_path_treats_percent_literally(db):
    """``%`` in the root is a character, not LIKE's any-run-of-characters."""
    pct = workspace_folders.add_folder(db, "/a/100%", name="100%")
    pct_day = workspace_folders.add_folder(db, "/a/100%/day1", name="day1", parent_id=pct)
    sibling = workspace_folders.add_folder(db, "/a/100 birds", name="100 birds")
    sibling_day = workspace_folders.add_folder(db, "/a/100 birds/day1", name="day1", parent_id=sibling)
    mine = [_photo(db, pct, "p.jpg"), _photo(db, pct_day, "q.jpg")]
    _photo(db, sibling, "s.jpg")
    _photo(db, sibling_day, "t.jpg")
    assert sorted(db.folders.photo_ids_under("/a/100%")) == sorted(mine)
    # Nor does a ``%`` folder swallow everything below its parent's prefix.
    assert db.folders.photo_ids_under("/a/100%/day1") == [mine[1]]


def test_photo_ids_under_path_folds_backslash_separators(db):
    """Windows paths are stored with ``\\``; descendants are found as they
    are by the rest of the folder domain."""
    root = workspace_folders.add_folder(db, "C:\\Pictures\\Trip", name="Trip")
    day = workspace_folders.add_folder(db, "C:\\Pictures\\Trip\\day1", name="day1", parent_id=root)
    deep = workspace_folders.add_folder(db, "C:\\Pictures\\Trip\\day1\\burst", name="burst", parent_id=day)
    sibling = workspace_folders.add_folder(db, "C:\\Pictures\\Trip2", name="Trip2")
    mine = [_photo(db, root, "r.jpg"), _photo(db, day, "d.jpg"),
            _photo(db, deep, "b.jpg")]
    _photo(db, sibling, "s.jpg")
    assert sorted(db.folders.photo_ids_under("C:\\Pictures\\Trip")) == sorted(mine)
    assert sorted(db.folders.photo_ids_under("C:\\Pictures\\Trip\\day1")) == sorted(mine[1:])


def test_photo_ids_under_path_is_case_sensitive(db):
    """On a case-sensitive filesystem ``/Photos/Trip`` and ``/photos/trip``
    are different folders; neither collects the other's photos. Case
    aliases of one folder are resolved before the query, by
    ``file_identity.catalog_scan_root``."""
    upper = workspace_folders.add_folder(db, "/Photos/Trip", name="Trip")
    upper_day = workspace_folders.add_folder(db, "/Photos/Trip/day1", name="day1", parent_id=upper)
    lower = workspace_folders.add_folder(db, "/photos/trip", name="trip")
    lower_day = workspace_folders.add_folder(db, "/photos/trip/day1", name="day1", parent_id=lower)
    mine = [_photo(db, upper, "u.jpg"), _photo(db, upper_day, "v.jpg")]
    theirs = [_photo(db, lower, "l.jpg"), _photo(db, lower_day, "m.jpg")]
    assert sorted(db.folders.photo_ids_under("/Photos/Trip")) == sorted(mine)
    assert sorted(db.folders.photo_ids_under("/photos/trip")) == sorted(theirs)


def test_get_folders_with_quality_data(db):
    ws = db._ws_id()
    root = workspace_folders.add_folder(db, "/q")
    sub = workspace_folders.add_folder(db, "/q/sub", parent_id=root, workspace_root=False)
    gap = _raw_folder(db, "/q/gap", root)
    below_gap = _raw_folder(db, "/q/gap/b", gap)
    _link(db, ws, below_gap, 0)
    empty = workspace_folders.add_folder(db, "/q/empty", parent_id=root, workspace_root=False)
    missing = workspace_folders.add_folder(db, "/q/missing", parent_id=root, workspace_root=False)
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
    rows = db.folders.with_quality_data()
    assert {r["id"]: (r["photo_count"], r["latest_photo"]) for r in rows} == {
        below_gap: (1, "2025-01-01T00:00:00"),
        sub: (1, "2024-06-01T00:00:00"),
        root: (3, "2025-01-01T00:00:00"),
    }
    assert set(rows[0].keys()) == {"id", "path", "name", "photo_count", "latest_photo"}
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError):
        db.folders.with_quality_data()


def test_update_folder_counts_recomputes_and_commits(db):
    a = workspace_folders.add_folder(db, "/u/a")
    b = workspace_folders.add_folder(db, "/u/b")
    _photo(db, a, "1.jpg")
    _photo(db, a, "2.jpg")
    db.conn.execute("UPDATE folders SET photo_count = 99")
    db.conn.commit()
    db.set_active_workspace(None)
    db.folders.update_counts()
    db.commit()
    with _reader(db) as r:
        counts = dict(r.execute("SELECT id, photo_count FROM folders").fetchall())
    assert counts == {a: 2, b: 0}


# -- folders.id_by_path / folders.source_path --------------------------


def test_get_folder_id_by_path_matches_the_stored_path_exactly(db):
    a = _raw_folder(db, "/u/by-path")
    db.conn.commit()
    db.set_active_workspace(None)
    assert db.folders.id_by_path("/u/by-path") == a
    assert db.folders.id_by_path("/u/by-path/") is None
    assert db.folders.id_by_path("/U/BY-PATH") is None
    assert db.folders.id_by_path("/u/nowhere") is None


def _stage_root(db, root_id, source_path, *, mapped_folder_id=None, is_root=1):
    db.conn.execute(
        "INSERT OR IGNORE INTO local_folders (root_folder_id, state) VALUES (?, 'active')",
        (root_id,),
    )
    db.conn.execute(
        "INSERT INTO local_folder_mappings "
        "(root_folder_id, folder_id, source_path, local_path, is_root) "
        "VALUES (?, ?, ?, ?, ?)",
        (root_id, mapped_folder_id or root_id, source_path, "/local/x", is_root),
    )
    db.conn.commit()


def test_get_folder_source_path_prefers_the_root_mapping(db):
    root = _raw_folder(db, "/local-folders/1/birds")
    child = _raw_folder(db, "/local-folders/1/birds/day1", root)
    plain = _raw_folder(db, "/u/plain")
    _stage_root(db, root, "/archive/birds")
    # A non-root mapping is not consulted: the folder answers its own path.
    _stage_root(db, root, "/archive/birds/day1", mapped_folder_id=child, is_root=0)
    db.set_active_workspace(None)
    assert db.folders.source_path(root) == "/archive/birds"
    assert db.folders.source_path(child) == "/local-folders/1/birds/day1"
    assert db.folders.source_path(plain) == "/u/plain"
    assert db.folders.source_path(999999) is None


# -- structure ----------------------------------------------------------------

# The folder methods that stay on ``Database``: the startup parent repair
# ``__init__`` runs, and ``get_folder_subtree_ids``, which ``PhotoRepository``
# binds from the façade by name (``FACADE_METHODS``) and the species-curation
# and location factories hand over bound. ``add_folder`` lives in
# ``services/workspace_folders.py`` and ``delete_folder`` in
# ``services/catalog_deletion.py``.
_DELEGATING_FOLDER_METHODS = (
    "repair_stale_folder_parents",
    "get_folder_subtree_ids",
)

# The forwarding wrappers this domain used to have on ``Database``; callers
# reach the same methods through ``db.folders`` now.
_REMOVED_FOLDER_WRAPPERS = (
    "repair_missing_folder_parents",
    "_folder_subtree_ids_by_path",
    "_local_source_descendant_ids",
    "get_folder_tree",
    "get_folder",
    "get_folder_paths",
    "check_folder_health",
    "get_missing_folders",
    "nearest_ancestor_folder_id",
    "_relink_parents_by_path",
    "relocate_folder",
    "_merge_into_existing",
    "_folders_linked_in_other_workspace",
    "count_folders",
    "count_all_folders",
    "get_all_folders",
    "get_folder_id_by_path",
    "get_folder_source_path",
    "count_present_photos_under_path",
    "get_photo_ids_under_path",
    "get_folders_with_quality_data",
    "update_folder_counts",
)


def _method_ast(name):
    source = textwrap.dedent(inspect.getsource(getattr(Database, name)))
    return ast.parse(source).body[0]


def _self_attrs(fn):
    return {
        node.attr
        for node in ast.walk(fn)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }


@pytest.mark.parametrize("name", _DELEGATING_FOLDER_METHODS)
def test_folder_method_delegates_to_repository(name):
    attrs = _self_attrs(_method_ast(name))
    assert "conn" not in attrs, (
        f"Database.{name} touches self.conn; move the SQL to FolderRepository"
    )
    assert "_folder_repository" in attrs, (
        f"Database.{name} no longer delegates to FolderRepository"
    )


def test_folders_has_no_other_forwarding_wrappers_on_database():
    """The domain is reached through ``db.folders``; Database keeps no aliases."""
    for name in _REMOVED_FOLDER_WRAPPERS:
        assert not hasattr(Database, name), f"Database.{name} came back; call db.folders"
    accessor = Database.__dict__["folders"]
    assert isinstance(accessor, property)
    source = textwrap.dedent(inspect.getsource(accessor.fget))
    attrs = _self_attrs(ast.parse(source).body[0])
    assert "_folder_repository" in attrs
    assert "conn" not in attrs


def test_folders_accessor_builds_a_fresh_repository_and_resolves_the_workspace_lazily(db):
    first = db.folders
    assert db.folders is not first
    assert first.conn is db.conn
    db.set_active_workspace(None)
    repo = db.folders
    # Catalog-wide methods need no workspace; scoped reads raise.
    assert repo.count_all() >= 0
    with pytest.raises(RuntimeError):
        repo.tree()


# Façade methods a folder wrapper hands to the repository, so the call still
# routes through ``Database`` (and its monkeypatches) at the same point in the
# SQL.
_FACADE_CALLBACKS = {
    "_folder_repository": {
        "transfer_gps_review": "moves_merge.transfer_gps_review",
        "transfer_embedded_offered": "photo_keywords.transfer_embedded_offered_keys",
    },
}


@pytest.mark.parametrize("name", sorted(_FACADE_CALLBACKS))
def test_folder_wrapper_passes_facade_methods_as_callbacks(name):
    # ``self.<method>``, or ``self.<accessor>.<method>`` for a domain accessor.
    passed = {
        kw.arg: ast.unparse(kw.value).removeprefix("self.")
        for node in ast.walk(_method_ast(name))
        if isinstance(node, ast.Call)
        for kw in node.keywords
        if isinstance(kw.value, ast.Attribute)
        and ast.unparse(kw.value).startswith("self.")
    }
    for kwarg, facade in _FACADE_CALLBACKS[name].items():
        assert passed.get(kwarg) == facade, (name, kwarg)


def test_delete_folder_passes_the_photo_delete_and_removal_record_as_callbacks():
    """``catalog_deletion.delete_folder`` hands the repository its callbacks.

    The photo delete goes through the service's ``delete_photos`` (looked up
    when each chunk runs, so a patch of it applies) and the removal record
    through ``workspace_folders``.
    """
    assert not hasattr(Database, "delete_folder")
    tree = ast.parse(textwrap.dedent(inspect.getsource(catalog_deletion.delete_folder)))
    passed = {
        kw.arg: ast.unparse(kw.value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and ast.unparse(node.func) == "db.folders.delete"
        for kw in node.keywords
    }
    assert passed == {
        "delete_photos": "delete_chunk",
        "remember_removals": "partial(workspace_folders.remember_workspace_folder_removals, db)",
    }
    chunk = next(
        node for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "delete_chunk"
    )
    assert "delete_photos(db, chunk" in ast.unparse(chunk)


def test_add_folder_links_through_the_workspace_folder_service():
    assert not hasattr(Database, "add_folder")
    tree = ast.parse(textwrap.dedent(inspect.getsource(workspace_folders.add_folder)))
    calls = {
        ast.unparse(node.func)
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
    }
    assert {"db.folders.add", "add_workspace_folder"} <= calls


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
    from repositories.folders import FolderMergeHooks, FolderRepository

    repo = FolderRepository(
        db.conn, None, path_for_subtree_match=None,
        stored_parent_path=None, subtree_prefix=None, subtree_relative=None,
        join_subtree_path=None, chunk_size=2, merge_hooks=FolderMergeHooks(None, None),
        keyword_source_conflict_sql="",
    )
    assert list(repo._chunks(range(5))) == [[0, 1], [2, 3], [4]]
    assert list(repo._chunks([])) == []
