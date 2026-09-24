"""Behavior pins for the workspace-folder membership domain of ``Database``.

Every test goes through the ``Database`` façade (public methods and the
private helpers other domains call), so the pins hold whether the SQL lives
in ``db.py`` or in ``repositories/workspace_folders.py``. They cover linking
and unlinking folders (single, exact, subtree), removal records, descendant
materialization, root marking and the root/extension queries, moving folders
between workspaces, the merge helpers that inspect or prune root links, and
the import-plan unlinked-folder count. The structural tests at the end keep
the SQL in ``WorkspaceFolderRepository``.
"""

import ast
import inspect
import sqlite3
import textwrap

import pytest
from db import Database


class _RecordingCache:
    """Stand-in for the shared new-images cache that records invalidations."""

    def __init__(self):
        self.invalidated = []

    def invalidate_workspaces(self, db_path, workspace_ids):
        self.invalidated.append((db_path, sorted(workspace_ids)))


@pytest.fixture
def cache(db, monkeypatch):
    recorder = _RecordingCache()
    monkeypatch.setattr(db, "_new_images_cache", recorder)
    return recorder


def _reader(db):
    """A second connection: sees only what the façade has committed."""
    conn = sqlite3.connect(db._db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _folder(db, path, parent_id=None, *, status=None):
    fid = db.add_folder(path, parent_id=parent_id, link_to_workspace=False)
    if status is not None:
        db.conn.execute("UPDATE folders SET status = ? WHERE id = ?", (status, fid))
        db.conn.commit()
    return fid


def _links(db, workspace_id, conn=None):
    conn = conn or db.conn
    return {
        r["folder_id"]: r["is_root"]
        for r in conn.execute(
            "SELECT folder_id, is_root FROM workspace_folders WHERE workspace_id = ?",
            (workspace_id,),
        )
    }


def _removals(db, workspace_id):
    return {
        r["folder_id"]: r["recursive"]
        for r in db.conn.execute(
            "SELECT folder_id, recursive FROM workspace_folder_removals WHERE workspace_id = ?",
            (workspace_id,),
        )
    }


def _link_raw(db, workspace_id, folder_id, is_root):
    db.conn.execute(
        "INSERT INTO workspace_folders (workspace_id, folder_id, is_root) VALUES (?, ?, ?)",
        (workspace_id, folder_id, is_root),
    )
    db.conn.commit()


def _photo(db, folder_id, name, extension=".jpg"):
    return db.add_photo(
        folder_id=folder_id, filename=name, extension=extension,
        file_size=1, file_mtime=1.0,
    )


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


@pytest.fixture
def tree(db):
    """/p, /p/a, /p/a/b and an unrelated /q, none linked yet."""
    ws = db._ws_id()
    p = _folder(db, "/p")
    a = _folder(db, "/p/a", p)
    b = _folder(db, "/p/a/b", a)
    q = _folder(db, "/q")
    return ws, p, a, b, q


# -- add_workspace_folder / _add_workspace_folder_no_commit ------------------


def test_add_workspace_folder_links_subtree_with_only_target_as_root(db, tree, cache):
    ws, p, a, b, q = tree
    assert db.add_workspace_folder(ws, p) is None
    assert _links(db, ws) == {p: 1, a: 0, b: 0}
    with _reader(db) as conn:
        assert _links(db, ws, conn) == {p: 1, a: 0, b: 0}
    assert cache.invalidated == [(db._db_path, [ws])]


def test_add_workspace_folder_is_root_demotes_previous_roots_in_subtree(db, tree, cache):
    ws, p, a, b, q = tree
    db.add_workspace_folder(ws, a)
    assert _links(db, ws) == {a: 1, b: 0}
    db.add_workspace_folder(ws, p)
    assert _links(db, ws) == {p: 1, a: 0, b: 0}


def test_add_workspace_folder_not_root_leaves_flags_alone(db, tree, cache):
    ws, p, a, b, q = tree
    db.add_workspace_folder(ws, a)
    db.add_workspace_folder(ws, p, is_root=False)
    # New rows are non-root; the existing root keeps its flag.
    assert _links(db, ws) == {p: 0, a: 1, b: 0}
    assert cache.invalidated == [(db._db_path, [ws])] * 2


def test_add_workspace_folder_restores_removed_descendants_by_default(db, tree):
    ws, p, a, b, q = tree
    db.add_workspace_folder(ws, p)
    db.remove_workspace_folder_tree(ws, a)
    assert _links(db, ws) == {p: 1}
    db.add_workspace_folder(ws, p)
    assert _links(db, ws) == {p: 1, a: 0, b: 0}
    assert _removals(db, ws) == {}


def test_add_workspace_folder_without_restore_keeps_removed_descendants_out(db, tree):
    ws, p, a, b, q = tree
    db.add_workspace_folder(ws, p)
    db.remove_workspace_folder_tree(ws, a)
    db.add_workspace_folder(ws, p, restore_removed=False)
    assert _links(db, ws) == {p: 1}
    # The directly targeted folder is always restored, even when removed.
    db.add_workspace_folder(ws, a, restore_removed=False)
    assert _links(db, ws) == {p: 1, a: 1}


def test_add_workspace_folder_no_commit_leaves_transaction_open(db, tree, cache):
    ws, p, a, b, q = tree
    db._add_workspace_folder_no_commit(ws, p)
    assert db.conn.in_transaction
    assert _links(db, ws) == {p: 1, a: 0, b: 0}
    with _reader(db) as conn:
        assert _links(db, ws, conn) == {}
    assert cache.invalidated == []
    db.conn.rollback()
    assert _links(db, ws) == {}


def test_add_workspace_folder_no_commit_skips_removed_unless_restoring(db, tree):
    ws, p, a, b, q = tree
    db.add_workspace_folder(ws, p)
    db.remove_workspace_folder_tree(ws, a)
    db._add_workspace_folder_no_commit(ws, p)
    assert _links(db, ws) == {p: 1}
    db._add_workspace_folder_no_commit(ws, p, restore_removed=True, is_root=False)
    assert _links(db, ws) == {p: 1, a: 0, b: 0}
    db.conn.commit()


def test_add_workspace_folder_chunks_root_update_for_wide_subtrees(db, cache):
    ws = db._ws_id()
    root = _folder(db, "/wide")
    db.conn.executemany(
        "INSERT INTO folders (path, name, parent_id) VALUES (?, ?, ?)",
        [(f"/wide/s{i}", f"s{i}", root) for i in range(805)],
    )
    db.conn.commit()
    statements = []
    db.conn.set_trace_callback(statements.append)
    try:
        db.add_workspace_folder(ws, root)
    finally:
        db.conn.set_trace_callback(None)
    updates = {s for s in statements if s.strip().startswith("UPDATE workspace_folders")}
    assert len(updates) == 2
    links = _links(db, ws)
    assert len(links) == 806
    assert sum(links.values()) == 1 and links[root] == 1


# -- add_workspace_folder_exact ------------------------------------------------


def test_add_workspace_folder_exact_links_only_that_folder(db, tree, cache):
    ws, p, a, b, q = tree
    assert db.add_workspace_folder_exact(ws, a) is None
    assert _links(db, ws) == {a: 0}
    with _reader(db) as conn:
        assert _links(db, ws, conn) == {a: 0}
    assert cache.invalidated == [(db._db_path, [ws])]


def test_add_workspace_folder_exact_root_promotes_existing_link(db, tree, cache):
    ws, p, a, b, q = tree
    db.add_workspace_folder_exact(ws, a)
    db.add_workspace_folder_exact(ws, a)  # idempotent, stays non-root
    assert _links(db, ws) == {a: 0}
    db.add_workspace_folder_exact(ws, a, is_root=True)
    assert _links(db, ws) == {a: 1}
    db.add_workspace_folder_exact(ws, b, is_root=True)
    assert _links(db, ws) == {a: 1, b: 1}
    assert len(cache.invalidated) == 4


def test_add_workspace_folder_exact_clears_removal_via_trigger(db, tree):
    ws, p, a, b, q = tree
    db.add_workspace_folder(ws, p)
    db.remove_workspace_folder(ws, a)
    assert _removals(db, ws) == {a: 0}
    db.add_workspace_folder_exact(ws, a)
    assert _removals(db, ws) == {}


# -- removal records ------------------------------------------------------------


def test_removed_workspace_folder_ids_returns_a_set_per_workspace(db, tree):
    ws, p, a, b, q = tree
    other = db.create_workspace("Other")
    assert db._removed_workspace_folder_ids(ws) == set()
    db.add_workspace_folder(ws, p)
    db.add_workspace_folder(other, q)
    db.remove_workspace_folder_tree(ws, a)
    db.remove_workspace_folder(other, q)
    removed = db._removed_workspace_folder_ids(ws)
    assert isinstance(removed, set)
    assert removed == {a, b}
    assert db._removed_workspace_folder_ids(other) == {q}


def test_folder_removal_root_ids_keeps_topmost_paths(db, tree):
    ws, p, a, b, q = tree
    sibling = _folder(db, "/pa")  # shares a string prefix, not a path prefix
    assert db._folder_removal_root_ids([b, a, q, sibling]) == {a, q, sibling}
    assert db._folder_removal_root_ids([a, p, b]) == {p}
    assert db._folder_removal_root_ids([]) == set()
    assert db._folder_removal_root_ids([999999]) == set()


def test_folder_removal_root_ids_uses_local_source_paths(db, tree):
    ws, p, a, b, q = tree
    staged = _folder(db, "/local-folders/1/b2")
    _map_local(db, staged, "/p/a/b2", "/local-folders/1/b2")
    windows = _folder(db, "C:\\photos\\x\\")
    assert db._folder_removal_root_ids([staged, a]) == {a}
    assert db._folder_removal_root_ids([staged]) == {staged}
    assert db._folder_removal_root_ids([windows]) == {windows}


def test_folder_removal_root_ids_chunks_large_id_lists(db):
    root = _folder(db, "/big")
    db.conn.executemany(
        "INSERT INTO folders (path, name, parent_id) VALUES (?, ?, ?)",
        [(f"/big/c{i}", f"c{i}", root) for i in range(805)],
    )
    db.conn.commit()
    ids = [r["id"] for r in db.conn.execute("SELECT id FROM folders WHERE path LIKE '/big%'")]
    statements = []
    db.conn.set_trace_callback(statements.append)
    try:
        roots = db._folder_removal_root_ids(ids)
    finally:
        db.conn.set_trace_callback(None)
    assert roots == {root}
    assert len({s for s in statements if "FROM folders f" in s}) == 2


def test_remember_removals_exact_and_recursive_records(db, tree):
    ws, p, a, b, q = tree
    db._remember_workspace_folder_removals(ws, [a, b])
    assert db.conn.in_transaction
    assert _removals(db, ws) == {a: 0, b: 0}
    db.conn.commit()
    # Recursive: only the topmost surviving folder carries recursive=1.
    db._remember_workspace_folder_removals(ws, iter([a, b]), recursive=True)
    assert _removals(db, ws) == {a: 1, b: 0}
    # Non-recursive re-record keeps the stronger (recursive) flag.
    db._remember_workspace_folder_removals(ws, [a])
    assert _removals(db, ws) == {a: 1, b: 0}
    # A recursive pass where ``a`` is no longer topmost overwrites it to 0.
    db._remember_workspace_folder_removals(ws, [p, a], recursive=True)
    assert _removals(db, ws) == {p: 1, a: 0, b: 0}
    # Unknown folder ids are ignored (the insert selects from ``folders``).
    db._remember_workspace_folder_removals(ws, [999999])
    assert 999999 not in _removals(db, ws)
    db.conn.commit()


# -- remove_workspace_folder / remove_workspace_folder_tree --------------------


def test_remove_workspace_folder_unlinks_one_and_records_it(db, tree, cache):
    ws, p, a, b, q = tree
    db.add_workspace_folder(ws, p)
    cache.invalidated.clear()
    assert db.remove_workspace_folder(ws, a) is None
    assert _links(db, ws) == {p: 1, b: 0}
    with _reader(db) as conn:
        assert _links(db, ws, conn) == {p: 1, b: 0}
        assert [tuple(r) for r in conn.execute(
            "SELECT folder_id, recursive FROM workspace_folder_removals"
        ).fetchall()] == [(a, 0)]
    assert cache.invalidated == [(db._db_path, [ws])]


def test_remove_workspace_folder_tree_unlinks_subtree(db, tree, cache):
    ws, p, a, b, q = tree
    db.add_workspace_folder(ws, p)
    db.add_workspace_folder(ws, q)
    cache.invalidated.clear()
    assert db.remove_workspace_folder_tree(ws, a) is None
    assert _links(db, ws) == {p: 1, q: 1}
    assert _removals(db, ws) == {a: 1, b: 0}
    with _reader(db) as conn:
        assert _links(db, ws, conn) == {p: 1, q: 1}
    assert cache.invalidated == [(db._db_path, [ws])]


def test_remove_workspace_folder_tree_chunks_wide_subtrees(db, cache):
    ws = db._ws_id()
    root = _folder(db, "/wide")
    db.conn.executemany(
        "INSERT INTO folders (path, name, parent_id) VALUES (?, ?, ?)",
        [(f"/wide/s{i}", f"s{i}", root) for i in range(805)],
    )
    db.conn.commit()
    db.add_workspace_folder(ws, root)
    statements = []
    db.conn.set_trace_callback(statements.append)
    try:
        db.remove_workspace_folder_tree(ws, root)
    finally:
        db.conn.set_trace_callback(None)
    deletes = {s for s in statements if s.strip().startswith("DELETE FROM workspace_folders")}
    assert len(deletes) == 2
    assert _links(db, ws) == {}


# -- _materialize_workspace_descendants ------------------------------------------


def test_materialize_links_new_descendants_as_non_roots(db, tree, cache):
    ws, p, a, b, q = tree
    db.add_workspace_folder(ws, p)
    late = _folder(db, "/p/a/late", a)
    cache.invalidated.clear()
    assert db._materialize_workspace_descendants(ws) is None
    assert _links(db, ws) == {p: 1, a: 0, b: 0, late: 0}
    with _reader(db) as conn:
        assert _links(db, ws, conn)[late] == 0
    assert cache.invalidated == [(db._db_path, [ws])]


def test_materialize_is_a_quiet_noop_when_nothing_is_missing(db, tree, cache):
    ws, p, a, b, q = tree
    db.add_workspace_folder(ws, p)
    cache.invalidated.clear()
    db._materialize_workspace_descendants(ws)
    assert not db.conn.in_transaction
    assert cache.invalidated == []


def test_materialize_skips_removed_descendants(db, tree, cache):
    ws, p, a, b, q = tree
    db.add_workspace_folder(ws, p)
    db.remove_workspace_folder(ws, b)
    cache.invalidated.clear()
    db._materialize_workspace_descendants(ws)
    assert _links(db, ws) == {p: 1, a: 0}
    assert cache.invalidated == []


def test_materialize_matches_backslash_paths_and_local_source_paths(db, cache):
    ws = db._ws_id()
    root = _folder(db, "C:\\pics\\")
    child = _folder(db, "C:\\pics\\2024")
    staged = _folder(db, "/local-folders/7/2025")
    _map_local(db, staged, "C:\\pics\\2025", "/local-folders/7/2025")
    _link_raw(db, ws, root, 1)
    db._materialize_workspace_descendants(ws)
    assert _links(db, ws) == {root: 1, child: 0, staged: 0}


def test_materialize_ignores_other_workspaces(db, tree):
    ws, p, a, b, q = tree
    other = db.create_workspace("Other")
    db.add_workspace_folder(other, p)
    _folder(db, "/p/new", p)
    db._materialize_workspace_descendants(ws)
    assert _links(db, ws) == {}


# -- mark_workspace_folder_roots -------------------------------------------------


def test_mark_workspace_folder_roots_promotes_linked_folders(db, tree):
    ws, p, a, b, q = tree
    db.add_workspace_folder(ws, p)
    assert db.mark_workspace_folder_roots(ws, [a, b, q]) is None
    assert _links(db, ws) == {p: 1, a: 1, b: 1}
    with _reader(db) as conn:
        assert _links(db, ws, conn) == {p: 1, a: 1, b: 1}


def test_mark_workspace_folder_roots_empty_is_a_noop(db, tree):
    ws, p, a, b, q = tree
    statements = []
    db.conn.set_trace_callback(statements.append)
    try:
        db.mark_workspace_folder_roots(ws, [])
        db.mark_workspace_folder_roots(ws, None)
    finally:
        db.conn.set_trace_callback(None)
    assert statements == []


def test_mark_workspace_folder_roots_chunks(db, cache):
    ws = db._ws_id()
    root = _folder(db, "/wide")
    db.conn.executemany(
        "INSERT INTO folders (path, name, parent_id) VALUES (?, ?, ?)",
        [(f"/wide/s{i}", f"s{i}", root) for i in range(805)],
    )
    db.conn.commit()
    db.add_workspace_folder(ws, root)
    ids = [fid for fid in _links(db, ws) if fid != root]
    statements = []
    db.conn.set_trace_callback(statements.append)
    try:
        db.mark_workspace_folder_roots(ws, ids)
    finally:
        db.conn.set_trace_callback(None)
    assert len({s for s in statements if s.strip().startswith("UPDATE")}) == 2
    assert set(_links(db, ws).values()) == {1}


# -- get_workspace_folders / get_folder_workspaces ------------------------------


def test_get_workspace_folders_materializes_and_orders_by_path(db, tree):
    ws, p, a, b, q = tree
    db.add_workspace_folder(ws, q)
    db.add_workspace_folder_exact(ws, b)
    db.add_workspace_folder(ws, p, is_root=False)
    late = _folder(db, "/p/0late", p)
    rows = db.get_workspace_folders(ws)
    assert [r["path"] for r in rows] == ["/p", "/p/0late", "/p/a", "/p/a/b", "/q"]
    assert isinstance(rows[0], sqlite3.Row)
    assert set(rows[0].keys()) >= {"id", "path", "parent_id", "name", "photo_count", "status"}
    assert late in _links(db, ws)
    assert db.get_workspace_folders(db.create_workspace("Empty")) == []


def test_get_folder_workspaces_direct_inherited_and_removed(db, tree):
    ws, p, a, b, q = tree
    zed = db.create_workspace("zed")
    alpha = db.create_workspace("Alpha")
    pinned = db.create_workspace("pinned")
    db.update_workspace(pinned, pinned_at="2026-01-01 00:00:00")
    db.add_workspace_folder(ws, p)            # root covering b
    db.add_workspace_folder_exact(zed, b)     # direct non-root
    db.add_workspace_folder_exact(alpha, b, is_root=True)
    _link_raw(db, pinned, p, 1)               # root without materialized b
    rows = db.get_folder_workspaces(b)
    assert [(r["id"], r["is_root"]) for r in rows] == [
        (pinned, 0), (alpha, 1), (ws, 0), (zed, 0),
    ]
    assert set(rows[0].keys()) == {"id", "name", "is_root"}
    # Inspecting memberships does not materialize the inherited link.
    assert b not in _links(db, pinned)
    db.remove_workspace_folder(pinned, b)
    assert pinned not in {r["id"] for r in db.get_folder_workspaces(b)}


def test_get_folder_workspaces_non_root_links_do_not_cover_descendants(db, tree):
    ws, p, a, b, q = tree
    other = db.create_workspace("Other")
    _link_raw(db, other, p, 0)
    assert db.get_folder_workspaces(b) == []
    assert [r["id"] for r in db.get_folder_workspaces(p)] == [other]
    assert db.get_folder_workspaces(999999) == []


def test_get_folder_workspaces_matches_local_source_and_backslash_roots(db):
    ws = db._ws_id()
    root = _folder(db, "D:\\shoot")
    staged = _folder(db, "/local-folders/3/day1")
    _map_local(db, staged, "D:\\shoot\\day1", "/local-folders/3/day1")
    exact = _folder(db, "/local-folders/3/root")
    _map_local(db, exact, "D:\\shoot", "/local-folders/3/root")
    _link_raw(db, ws, root, 1)
    assert [(r["id"], r["is_root"]) for r in db.get_folder_workspaces(staged)] == [(ws, 0)]
    assert [r["id"] for r in db.get_folder_workspaces(exact)] == [ws]
    assert [(r["id"], r["is_root"]) for r in db.get_folder_workspaces(root)] == [(ws, 1)]


# -- root queries ------------------------------------------------------------------


def test_get_workspace_root_folder_ids_defaults_to_active_workspace(db, tree):
    ws, p, a, b, q = tree
    db.add_workspace_folder(ws, q)
    db.add_workspace_folder(ws, p)
    db.mark_workspace_folder_roots(ws, [b])
    assert db.get_workspace_root_folder_ids() == [p, b, q]
    assert all(isinstance(i, int) for i in db.get_workspace_root_folder_ids())
    assert db.get_workspace_root_folder_ids(db.create_workspace("Empty")) == []


def test_get_workspace_root_folder_ids_requires_workspace_only_by_default(db, tree):
    ws, p, a, b, q = tree
    db.add_workspace_folder(ws, p)
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError, match="No active workspace set"):
        db.get_workspace_root_folder_ids()
    assert db.get_workspace_root_folder_ids(ws) == [p]


def test_get_workspace_root_folder_ids_materializes_descendants(db, tree):
    ws, p, a, b, q = tree
    db.add_workspace_folder(ws, p)
    late = _folder(db, "/p/late", p)
    db.get_workspace_root_folder_ids(ws)
    assert _links(db, ws)[late] == 0


def test_get_workspace_folder_roots_counts_linked_subtree_photos(db, tree):
    ws, p, a, b, q = tree
    other = db.create_workspace("Other")
    db.add_workspace_folder(ws, p)
    db.add_workspace_folder(ws, q)
    db.add_workspace_folder(other, a)
    _photo(db, p, "p1.jpg")
    _photo(db, a, "a1.jpg")
    _photo(db, b, "b1.jpg")
    _photo(db, b, "b2.jpg")
    db.remove_workspace_folder(ws, b)  # detached descendant: not counted
    staged = _folder(db, "/local-folders/9/c")
    _map_local(db, staged, "/p/c", "/local-folders/9/c")
    _photo(db, staged, "c1.jpg")
    rows = db.get_workspace_folder_roots(ws)
    assert [(r["path"], r["workspace_photo_count"]) for r in rows] == [("/p", 3), ("/q", 0)]
    assert {"status", "workspace_photo_count"} <= set(rows[0].keys())
    assert staged in _links(db, ws)  # materialized through the source path
    other_rows = db.get_workspace_folder_roots(other)
    assert [(r["id"], r["workspace_photo_count"]) for r in other_rows] == [(a, 3)]


def test_get_workspace_folder_roots_hides_non_root_links(db, tree):
    ws, p, a, b, q = tree
    db.add_workspace_folder(ws, p, is_root=False)
    assert db.get_workspace_folder_roots(ws) == []


# -- get_workspace_extensions ------------------------------------------------------


def test_get_workspace_extensions_distinct_lowercased_and_filtered(db, tree):
    ws, p, a, b, q = tree
    other = db.create_workspace("Other")
    gone = _folder(db, "/gone", status="missing")
    partial = _folder(db, "/partial", status="partial")
    for fid in (p, gone, partial):
        db.add_workspace_folder(ws, fid)
    db.add_workspace_folder(other, q)
    _photo(db, p, "a.JPG", ".JPG")
    _photo(db, p, "b.jpg", ".jpg")
    _photo(db, p, "c.NEF", ".NEF")
    _photo(db, p, "d", "")
    _photo(db, p, "e", None)
    _photo(db, partial, "f.tif", ".tif")
    _photo(db, gone, "g.cr3", ".cr3")
    _photo(db, q, "h.png", ".png")
    assert db.get_workspace_extensions() == [".jpg", ".nef", ".tif"]
    db.set_active_workspace(other)
    assert db.get_workspace_extensions() == [".png"]


def test_get_workspace_extensions_requires_active_workspace(db):
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError, match="No active workspace set"):
        db.get_workspace_extensions()


# -- move_folders_to_workspace ---------------------------------------------------


@pytest.fixture
def move_setup(db, tree):
    ws, p, a, b, q = tree
    target = db.create_workspace("Target")
    db.add_workspace_folder(ws, p)
    db.add_workspace_folder(ws, q)
    return ws, target, p, a, b, q


def test_move_validation_errors(db, move_setup):
    src, target, p, a, b, q = move_setup
    with pytest.raises(ValueError, match="Source workspace 999 not found"):
        db.move_folders_to_workspace(999, target, [p])
    with pytest.raises(ValueError, match="Target workspace 998 not found"):
        db.move_folders_to_workspace(src, 998, [p])
    with pytest.raises(ValueError, match="Source and target workspace are the same"):
        db.move_folders_to_workspace(src, src, [p])
    with pytest.raises(ValueError, match="Folder 424242 does not belong to source workspace"):
        db.move_folders_to_workspace(src, target, [424242])
    with pytest.raises(ValueError, match="covered by another source workspace folder"):
        db.move_folders_to_workspace(src, target, [a])


def test_move_empty_folder_list_returns_zero_counts(db, move_setup, cache):
    src, target, p, a, b, q = move_setup
    cache.invalidated.clear()
    assert db.move_folders_to_workspace(src, target, []) == {
        "folders_moved": 0,
        "pending_changes_moved": 0,
        "photo_preferences_moved": 0,
        "species_highlights_moved": 0,
    }
    assert cache.invalidated == []
    assert _links(db, target) == {}


def _prediction(db, photo_id, species):
    det = db.conn.execute(
        "INSERT INTO detections (photo_id, category) VALUES (?, 'animal')", (photo_id,),
    ).lastrowid
    return db.conn.execute(
        "INSERT INTO predictions (detection_id, classifier_model, species) VALUES (?, 'm', ?)",
        (det, species),
    ).lastrowid


def test_move_folders_carries_workspace_scoped_rows(db, move_setup, cache):
    src, target, p, a, b, q = move_setup
    pb = _photo(db, b, "b.jpg")
    pp = _photo(db, p, "p.jpg")
    pq = _photo(db, q, "q.jpg")
    for photo in (pb, pp, pq):
        db.conn.execute(
            "INSERT INTO pending_changes (photo_id, change_type, value, workspace_id) "
            "VALUES (?, 'rating', '3', ?)", (photo, src),
        )
    kept = _prediction(db, pb, "Robin")
    fresh = _prediction(db, pp, "Wren")
    stays = _prediction(db, pq, "Crow")
    db.conn.executemany(
        "INSERT INTO prediction_review (prediction_id, workspace_id, status, group_id, vote_count) "
        "VALUES (?, ?, ?, ?, ?)",
        [(kept, src, "accepted", "g1", 2), (kept, target, "rejected", None, None),
         (fresh, src, "accepted", "g2", 5), (stays, src, "accepted", None, None)],
    )
    db.conn.executemany(
        "INSERT INTO photo_preferences (workspace_id, purpose, species, photo_id) VALUES (?, ?, ?, ?)",
        [(src, "life_list", "Robin", pb), (src, "highlights", "Wren", pp),
         (target, "highlights", "Wren", pq), (src, "life_list", "Crow", pq)],
    )
    db.conn.executemany(
        "INSERT INTO species_highlights (workspace_id, species, photo_id, rank, created_at) "
        "VALUES (?, ?, ?, ?, ?)",
        [(target, "Robin", pq, 4, "t0"), (target, "Robin", pp, 7, "t0"),
         (src, "Robin", pb, 1, "t1"), (src, "Robin", pp, 2, "t2"),
         (src, "Wren", pp, 9, "t3"), (src, "Crow", pq, 1, "t4")],
    )
    db.conn.commit()
    cache.invalidated.clear()

    result = db.move_folders_to_workspace(src, target, [p])

    assert result == {
        "folders_moved": 1,
        "pending_changes_moved": 2,
        "photo_preferences_moved": 1,
        "species_highlights_moved": 2,
    }
    assert _links(db, src) == {q: 1}
    assert _links(db, target) == {p: 1, a: 0, b: 0}
    pending = {
        (r["photo_id"], r["workspace_id"])
        for r in db.conn.execute("SELECT photo_id, workspace_id FROM pending_changes")
    }
    assert pending == {(pb, target), (pp, target), (pq, src)}
    reviews = {
        (r["prediction_id"], r["workspace_id"]): (r["status"], r["group_id"], r["vote_count"])
        for r in db.conn.execute("SELECT * FROM prediction_review")
    }
    assert reviews == {
        (kept, target): ("rejected", None, None),   # target's existing row wins
        (fresh, target): ("accepted", "g2", 5),
        (stays, src): ("accepted", None, None),
    }
    prefs = {
        (r["workspace_id"], r["purpose"], r["species"]): r["photo_id"]
        for r in db.conn.execute("SELECT * FROM photo_preferences")
    }
    assert prefs == {
        (target, "life_list", "Robin"): pb,
        (target, "highlights", "Wren"): pq,   # target's existing row wins
        (src, "life_list", "Crow"): pq,
    }
    highlights = [
        (r["workspace_id"], r["species"], r["photo_id"], r["rank"], r["created_at"])
        for r in db.conn.execute("SELECT * FROM species_highlights")
    ]
    assert sorted(highlights) == sorted([
        (target, "Robin", pq, 4, "t0"),
        (target, "Robin", pp, 7, "t0"),       # already present: IGNOREd, no rank bump
        (target, "Robin", pb, 8, "t1"),
        (target, "Wren", pp, 1, "t3"),
        (src, "Crow", pq, 1, "t4"),
    ])
    with _reader(db) as conn:
        assert _links(db, target, conn) == {p: 1, a: 0, b: 0}
    assert cache.invalidated == [(db._db_path, sorted([src, target]))]


def test_move_folders_marks_only_selected_folders_as_roots(db, cache):
    src = db._ws_id()
    target = db.create_workspace("Target")
    x = _folder(db, "/x")
    y = _folder(db, "/y")
    y1 = _folder(db, "/y/1", y)
    db.add_workspace_folder(src, x)
    db.add_workspace_folder(src, y)
    _link_raw(db, target, y1, 1)
    result = db.move_folders_to_workspace(src, target, [y, x])
    assert result["folders_moved"] == 2
    assert _links(db, src) == {}
    # y/1 was already linked to the target as a root; INSERT OR IGNORE keeps it.
    assert _links(db, target) == {x: 1, y: 1, y1: 1}


def test_move_folders_rolls_back_on_failure(db, move_setup, cache):
    src, target, p, a, b, q = move_setup
    pb = _photo(db, b, "b.jpg")
    db.conn.execute(
        "INSERT INTO pending_changes (photo_id, change_type, value, workspace_id) "
        "VALUES (?, 'rating', '3', ?)", (pb, src),
    )
    db.conn.execute(
        "CREATE TEMP TRIGGER block_move BEFORE INSERT ON workspace_folders "
        "BEGIN SELECT RAISE(ABORT, 'blocked move'); END"
    )
    db.conn.commit()
    cache.invalidated.clear()
    with pytest.raises(sqlite3.IntegrityError, match="blocked move"):
        db.move_folders_to_workspace(src, target, [p])
    assert not db.conn.in_transaction
    assert _links(db, src) == {p: 1, a: 0, b: 0, q: 1}
    assert _links(db, target) == {}
    assert db.conn.execute(
        "SELECT workspace_id FROM pending_changes WHERE photo_id = ?", (pb,)
    ).fetchone()[0] == src
    assert cache.invalidated == []


def test_move_folders_chunks_wide_subtrees(db, cache):
    src = db._ws_id()
    target = db.create_workspace("Target")
    root = _folder(db, "/wide")
    db.conn.executemany(
        "INSERT INTO folders (path, name, parent_id) VALUES (?, ?, ?)",
        [(f"/wide/s{i}", f"s{i}", root) for i in range(805)],
    )
    db.conn.commit()
    db.add_workspace_folder(src, root)
    statements = []
    db.conn.set_trace_callback(statements.append)
    try:
        result = db.move_folders_to_workspace(src, target, [root])
    finally:
        db.conn.set_trace_callback(None)
    assert result["folders_moved"] == 1
    assert len({s for s in statements if s.strip().startswith("UPDATE pending_changes")}) == 2
    assert len({s for s in statements if s.strip().startswith("DELETE FROM workspace_folders")}) == 2
    links = _links(db, target)
    assert len(links) == 806 and links[root] == 1 and sum(links.values()) == 1


# -- merge helpers: root ancestry and pruning ---------------------------------


def test_root_ancestor_and_descendant_checks(db, tree):
    ws, p, a, b, q = tree
    win = _folder(db, "E:\\arch\\")
    db.add_workspace_folder(ws, a)
    db.add_workspace_folder(ws, win)
    _link_raw(db, ws, q, 0)  # non-root links never count
    assert db._active_ws_root_ancestor_exists(ws, "/p/a") is True
    assert db._active_ws_root_ancestor_exists(ws, "/p/a/b/c/") is True
    assert db._active_ws_root_ancestor_exists(ws, "E:/arch/2026") is True
    assert db._active_ws_root_ancestor_exists(ws, "/p") is False
    assert db._active_ws_root_ancestor_exists(ws, "/p/ab") is False
    assert db._active_ws_root_ancestor_exists(ws, "/q") is False
    assert db._active_ws_root_descendant_exists(ws, "/p") is True
    assert db._active_ws_root_descendant_exists(ws, "\\p\\") is True
    assert db._active_ws_root_descendant_exists(ws, "E:\\") is True
    assert db._active_ws_root_descendant_exists(ws, "/p/a") is False
    assert db._active_ws_root_descendant_exists(ws, "/") is True  # "" + "/" prefix
    assert db._active_ws_root_descendant_exists(ws, "/q") is False
    empty = db.create_workspace("Empty")
    assert db._active_ws_root_ancestor_exists(empty, "/p") is False
    assert db._active_ws_root_descendant_exists(empty, "/") is False


def test_prune_nonroot_links_outside_roots(db, cache):
    ws = db._ws_id()
    archive = _folder(db, "/archive")
    usa = _folder(db, "/archive/USA", archive)
    y26 = _folder(db, "/archive/USA/2026", usa)
    y26d = _folder(db, "/archive/USA/2026/d1", y26)
    y27 = _folder(db, "/archive/USA/2027", usa)
    other = _folder(db, "/other")
    covered_root = _folder(db, "/kept")
    covered = _folder(db, "/kept/USA2026", covered_root)
    for fid in (archive, usa, y26d, y27, other):
        _link_raw(db, ws, fid, 0)
    _link_raw(db, ws, covered_root, 1)
    _link_raw(db, ws, covered, 0)
    cache.invalidated.clear()

    db._prune_ws_nonroot_links_outside_roots(ws, "/archive/USA/")

    # Ancestors (/archive), self (/archive/USA) and descendants go; the
    # unrelated /other survives.
    assert _links(db, ws) == {other: 0, covered_root: 1, covered: 0}
    with _reader(db) as conn:
        assert _links(db, ws, conn) == {other: 0, covered_root: 1, covered: 0}
    assert cache.invalidated == [(db._db_path, [ws])]

    # Links inside a root are kept even when they sit on the path.
    cache.invalidated.clear()
    db._prune_ws_nonroot_links_outside_roots(ws, "/kept/USA2026/x")
    assert _links(db, ws) == {other: 0, covered_root: 1, covered: 0}
    assert not db.conn.in_transaction
    assert cache.invalidated == []


def test_prune_nonroot_links_chunks(db, cache):
    ws = db._ws_id()
    root = _folder(db, "/wide")
    db.conn.executemany(
        "INSERT INTO folders (path, name, parent_id) VALUES (?, ?, ?)",
        [(f"/wide/s{i}", f"s{i}", root) for i in range(805)],
    )
    db.conn.commit()
    db.add_workspace_folder(ws, root, is_root=False)
    statements = []
    db.conn.set_trace_callback(statements.append)
    try:
        db._prune_ws_nonroot_links_outside_roots(ws, "/wide")
    finally:
        db.conn.set_trace_callback(None)
    assert len({s for s in statements if s.strip().startswith("DELETE FROM workspace_folders")}) == 2
    assert _links(db, ws) == {}


# -- workspace_unlinked_folder_count ---------------------------------------------


def test_workspace_unlinked_folder_count(db, tree):
    ws, p, a, b, q = tree
    db.add_workspace_folder(ws, a)
    assert db.workspace_unlinked_folder_count(
        ["/p", "/p/a", "/p/a/b", "/p/a", "/never", "", None]
    ) == 2
    other = db.create_workspace("Other")
    db.set_active_workspace(other)
    assert db.workspace_unlinked_folder_count(["/p/a", "/q"]) == 2


def test_workspace_unlinked_folder_count_empty_input_is_lazy(db):
    db.set_active_workspace(None)
    assert db.workspace_unlinked_folder_count([]) == 0
    assert db.workspace_unlinked_folder_count(None) == 0
    # Non-empty input reads the active workspace before filtering blanks.
    with pytest.raises(RuntimeError, match="No active workspace set"):
        db.workspace_unlinked_folder_count(["", None])


def test_workspace_unlinked_folder_count_all_blank_paths(db):
    assert db.workspace_unlinked_folder_count(["", None]) == 0


def test_workspace_unlinked_folder_count_batches(db):
    ws = db._ws_id()
    root = _folder(db, "/wide")
    db.conn.executemany(
        "INSERT INTO folders (path, name, parent_id) VALUES (?, ?, ?)",
        [(f"/wide/s{i}", f"s{i}", root) for i in range(805)],
    )
    db.conn.commit()
    db.add_workspace_folder(ws, root)
    paths = [f"/wide/s{i}" for i in range(805)] + ["/nope1", "/nope2"]
    statements = []
    db.conn.set_trace_callback(statements.append)
    try:
        assert db.workspace_unlinked_folder_count(paths) == 2
    finally:
        db.conn.set_trace_callback(None)
    assert len({s for s in statements if "FROM folders f" in s}) == 2


# -- structure -------------------------------------------------------------------

_MOVED_METHODS = [
    "_add_workspace_folder_no_commit",
    "add_workspace_folder",
    "add_workspace_folder_exact",
    "_removed_workspace_folder_ids",
    "_folder_removal_root_ids",
    "_remember_workspace_folder_removals",
    "remove_workspace_folder",
    "remove_workspace_folder_tree",
    "_materialize_workspace_descendants",
    "mark_workspace_folder_roots",
    "get_workspace_folders",
    "get_folder_workspaces",
    "get_workspace_root_folder_ids",
    "get_workspace_folder_roots",
    "get_workspace_extensions",
    "move_folders_to_workspace",
    "_active_ws_root_ancestor_exists",
    "_active_ws_root_descendant_exists",
    "_prune_ws_nonroot_links_outside_roots",
    "workspace_unlinked_folder_count",
]


@pytest.mark.parametrize("name", _MOVED_METHODS)
def test_workspace_folder_method_delegates_to_repository(name):
    source = textwrap.dedent(inspect.getsource(getattr(Database, name)))
    fn = ast.parse(source).body[0]
    attrs = {
        node.attr
        for node in ast.walk(fn)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }
    assert "conn" not in attrs, (
        f"Database.{name} touches self.conn; move the SQL to WorkspaceFolderRepository"
    )
    assert "_workspace_folder_repository" in attrs, (
        f"Database.{name} no longer delegates to WorkspaceFolderRepository"
    )


def test_wrappers_keep_composition_on_the_facade(db, tree, monkeypatch):
    """Sibling calls stay on ``Database`` so monkeypatches of them apply."""
    ws, p, a, b, q = tree
    calls = []
    original = Database._removed_workspace_folder_ids

    def spy(self, workspace_id):
        calls.append(workspace_id)
        return original(self, workspace_id)

    monkeypatch.setattr(Database, "_removed_workspace_folder_ids", spy)
    db.add_workspace_folder(ws, p, restore_removed=False)
    _folder(db, "/p/late", p)
    db.get_workspace_folders(ws)
    assert calls == [ws, ws]
