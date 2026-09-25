"""Behavior pins for the sync domain of ``Database`` (the pending XMP queue).

The tests exercise the pending-change methods only through the ``Database``
façade, so they hold whether the SQL lives in ``db.py`` or in
``repositories/sync.py``. They pin queue dedupe rules, keyword-name
normalization, commit boundaries and the ``_commit=False`` nested-transaction
seams, workspace scoping, lazy active-workspace resolution, chunking, the
cancel-captured-keyword inverse, token-vs-id clearing, flat-removal
equivalence across workspaces, the staged sync scope counts, and that
composition (``queue_change``, ``_pending_keyword_sidecar_alias``,
``clear_equivalent_flat_removals``, ``get_effective_config``) still routes
through the façade so monkeypatches take effect. The structural test at
the end keeps the SQL in ``SyncRepository``.
"""

import ast
import contextlib
import inspect
import os
import sqlite3
import textwrap
import uuid

import pytest
from db import Database


def _visible_rows(db):
    """Pending rows as a second connection sees them (i.e. committed)."""
    with sqlite3.connect(db._db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT photo_id, change_type, value, workspace_id, sync_started "
            "FROM pending_changes ORDER BY id"
        ).fetchall()
    conn.close()
    return [tuple(r) for r in rows]


def _rows(db):
    return [
        tuple(r)
        for r in db.conn.execute(
            "SELECT photo_id, change_type, value, workspace_id, sync_started "
            "FROM pending_changes ORDER BY id"
        ).fetchall()
    ]


def _insert(db, photo_id, change_type, value, ws, token=None, sync_started=0):
    cur = db.conn.execute(
        "INSERT INTO pending_changes "
        "(photo_id, change_type, value, change_token, workspace_id, sync_started) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (photo_id, change_type, value, token, ws, sync_started),
    )
    db.conn.commit()
    return cur.lastrowid


def _trace(db):
    """Record executed SQL. SQLite reports a statement again for each FK
    cascade it fires, so count distinct texts, not entries."""
    statements = []
    db.conn.set_trace_callback(statements.append)
    return statements


@pytest.fixture
def lib(db):
    """Active workspace with two folders, a sibling workspace, and photos."""
    ws = db._active_workspace_id
    other = db.create_workspace("Other")
    f1 = db.add_folder("/lib/one", name="one")
    f2 = db.add_folder("/lib/two", name="two")
    db.add_workspace_folder(other, f1)
    db.add_workspace_folder(other, f2)
    p = {
        "a": db.add_photo(f1, "a.jpg", ".jpg", 1, 1.0),
        "a_raw": db.add_photo(f1, "a.cr2", ".cr2", 1, 1.0),
        "b": db.add_photo(f1, "b.jpg", ".jpg", 1, 1.0),
        "c": db.add_photo(f2, "c.jpg", ".jpg", 1, 1.0),
    }
    db.conn.commit()
    return {"db": db, "ws": ws, "other": other, "f1": f1, "f2": f2, **p}


# -- count / get --------------------------------------------------------------


def test_count_and_get_pending_changes_scoped_and_ordered(lib):
    db, ws, other = lib["db"], lib["ws"], lib["other"]
    _insert(db, lib["b"], "rating", "3", ws)
    _insert(db, lib["a"], "rating", "2", other)
    _insert(db, lib["a"], "flag", "flagged", ws)
    db.conn.execute(
        "UPDATE pending_changes SET created_at = '2000-01-01 00:00:00' "
        "WHERE change_type = 'flag'"
    )
    db.conn.commit()
    assert db.count_pending_changes() == 2
    rows = db.get_pending_changes()
    assert isinstance(rows, list)
    assert isinstance(rows[0], sqlite3.Row)
    assert [r["change_type"] for r in rows] == ["flag", "rating"]
    assert {r["workspace_id"] for r in rows} == {ws}
    assert set(rows[0].keys()) >= {
        "id", "photo_id", "change_type", "value", "change_token",
        "sync_started", "created_at", "workspace_id",
    }


def test_count_and_get_require_active_workspace(lib):
    db = lib["db"]
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError, match="No active workspace"):
        db.count_pending_changes()
    with pytest.raises(RuntimeError, match="No active workspace"):
        db.get_pending_changes()


# -- queue_change ---------------------------------------------------------------


def test_queue_change_returns_token_and_commits(lib):
    db, ws = lib["db"], lib["ws"]
    token = db.queue_change(lib["a"], "flag", "flagged")
    assert str(uuid.UUID(token)) == token
    assert not db.conn.in_transaction
    assert _visible_rows(db) == [(lib["a"], "flag", "flagged", ws, 0)]
    row = db.conn.execute("SELECT change_token FROM pending_changes").fetchone()
    assert row["change_token"] == token


def test_queue_change_no_commit_leaves_transaction_open(lib):
    db, ws = lib["db"], lib["ws"]
    token = db.queue_change(lib["a"], "flag", "flagged", _commit=False)
    assert token is not None
    assert db.conn.in_transaction
    assert _visible_rows(db) == []
    assert _rows(db) == [(lib["a"], "flag", "flagged", ws, 0)]
    db.conn.commit()


def test_queue_change_explicit_workspace(lib):
    db, other = lib["db"], lib["other"]
    db.queue_change(lib["a"], "flag", "flagged", workspace_id=other)
    assert _rows(db) == [(lib["a"], "flag", "flagged", other, 0)]
    # The active workspace is not consulted when one is supplied.
    db.set_active_workspace(None)
    assert db.queue_change(lib["b"], "flag", "rejected", workspace_id=other)


def test_queue_change_requires_active_workspace_when_none_given(lib):
    db = lib["db"]
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError, match="No active workspace"):
        db.queue_change(lib["a"], "flag", "flagged")


def test_queue_change_normalizes_keyword_values(lib):
    db = lib["db"]
    for kind in ("keyword_add", "keyword_remove", "keyword_remove_flat"):
        db.queue_change(lib["a"], kind, '"Robin"')
    assert [r[1:3] for r in _rows(db)] == [
        ("keyword_add", "Robin"),
        ("keyword_remove", "Robin"),
        ("keyword_remove_flat", "Robin"),
    ]
    # Non-keyword values are stored verbatim.
    db.queue_change(lib["a"], "title", '"Robin"')
    assert _rows(db)[-1][2] == '"Robin"'


def test_queue_change_blank_keyword_returns_none_before_workspace(lib):
    db = lib["db"]
    db.set_active_workspace(None)
    assert db.queue_change(lib["a"], "keyword_add", '""') is None
    assert db.queue_change(lib["a"], "keyword_remove", "   ") is None
    assert _rows(db) == []
    assert not db.conn.in_transaction


def test_queue_change_generic_dedupe(lib):
    db, ws, other = lib["db"], lib["ws"], lib["other"]
    assert db.queue_change(lib["a"], "flag", "flagged")
    statements = _trace(db)
    assert db.queue_change(lib["a"], "flag", "flagged") is None
    db.conn.set_trace_callback(None)
    assert not any(s.startswith("INSERT") for s in statements)
    assert not db.conn.in_transaction
    # A different value, photo or workspace is a new intent.
    assert db.queue_change(lib["a"], "flag", "rejected")
    assert db.queue_change(lib["b"], "flag", "flagged")
    assert db.queue_change(lib["a"], "flag", "flagged", workspace_id=other)
    assert len(_rows(db)) == 4
    assert {r[3] for r in _rows(db)} == {ws, other}


def test_queue_change_rating_dedupes_only_against_latest(lib):
    db = lib["db"]
    assert db.queue_change(lib["a"], "rating", "1")
    assert db.queue_change(lib["a"], "rating", "1") is None
    assert db.queue_change(lib["a"], "rating", "2")
    # 1 -> 2 -> 1 keeps the last 1.
    assert db.queue_change(lib["a"], "rating", "1")
    assert [r[2] for r in _rows(db)] == ["1", "2", "1"]


def test_queue_change_rating_requeues_after_other_photo_rating(lib):
    db = lib["db"]
    assert db.queue_change(lib["a"], "rating", "3")
    assert db.queue_change(lib["b"], "rating", "4")
    # Same latest value for photo a, but a later rating on another photo
    # (possibly sharing the sidecar) intervened.
    assert db.queue_change(lib["a"], "rating", "3")
    assert len(_rows(db)) == 3
    # Another workspace's rating does not count as intervening.
    db.queue_change(lib["c"], "rating", "5", workspace_id=lib["other"])
    assert db.queue_change(lib["a"], "rating", "3") is None


def test_queue_change_keyword_dedupe(lib):
    db = lib["db"]
    assert db.queue_change(lib["a"], "keyword_add", "Robin")
    assert db.queue_change(lib["a"], "keyword_add", "Robin") is None
    # Case-variant spelling is a different latest value -> queued.
    assert db.queue_change(lib["a"], "keyword_add", "robin")
    # An opposite intent is always queued.
    assert db.queue_change(lib["a"], "keyword_remove", "robin")
    assert db.queue_change(lib["a"], "keyword_remove", "ROBIN")
    assert db.queue_change(lib["a"], "keyword_remove", "ROBIN") is None
    assert [r[1:3] for r in _rows(db)] == [
        ("keyword_add", "Robin"),
        ("keyword_add", "robin"),
        ("keyword_remove", "robin"),
        ("keyword_remove", "ROBIN"),
    ]


def test_queue_change_keyword_requeues_after_other_photo_edit(lib):
    db = lib["db"]
    assert db.queue_change(lib["a"], "keyword_add", "Robin")
    # Unrelated keyword on another photo does not intervene.
    db.queue_change(lib["b"], "keyword_add", "Wren")
    assert db.queue_change(lib["a"], "keyword_add", "Robin") is None
    # Same keyword (any case, any keyword type) on another photo does.
    db.queue_change(lib["b"], "keyword_remove_flat", "ROBIN")
    assert db.queue_change(lib["a"], "keyword_add", "Robin")
    assert len(_rows(db)) == 4


def test_queue_change_keyword_latest_by_created_at(lib):
    db, ws = lib["db"], lib["ws"]
    newer = _insert(db, lib["a"], "keyword_remove", "Robin", ws)
    older = _insert(db, lib["a"], "keyword_add", "Robin", ws)
    db.conn.execute(
        "UPDATE pending_changes SET created_at = '2000-01-01' WHERE id = ?",
        (older,),
    )
    db.conn.execute(
        "UPDATE pending_changes SET created_at = '2001-01-01' WHERE id = ?",
        (newer,),
    )
    db.conn.commit()
    # Latest by created_at is the remove, so a remove is redundant.
    assert db.queue_change(lib["a"], "keyword_remove", "Robin") is None
    assert db.queue_change(lib["a"], "keyword_add", "Robin")


# -- claim_pending_changes_for_sync ------------------------------------------------


def test_claim_empty_returns_list_without_workspace(lib):
    db = lib["db"]
    db.set_active_workspace(None)
    assert db.claim_pending_changes_for_sync([]) == []


def test_claim_marks_rows_and_returns_in_caller_order(lib):
    db, ws, other = lib["db"], lib["ws"], lib["other"]
    db.queue_change(lib["a"], "keyword_add", "Robin")
    db.queue_change(lib["b"], "flag", "flagged")
    legacy = _insert(db, lib["c"], "rating", "2", ws, token=None)
    foreign = _insert(db, lib["a"], "rating", "5", other, token="t-foreign")
    rows = db.get_pending_changes()
    changes = list(reversed(rows)) + [
        {"id": foreign, "change_token": "t-foreign"},
    ]
    claimed = db.claim_pending_changes_for_sync(changes)
    assert [r["id"] for r in claimed] == [r["id"] for r in reversed(rows)]
    assert all(r["sync_started"] == 1 for r in claimed)
    assert legacy in [r["id"] for r in claimed]
    assert not db.conn.in_transaction
    visible = _visible_rows(db)
    assert [r[4] for r in visible] == [1, 1, 1, 0]


def test_claim_skips_replaced_or_cancelled_rows(lib):
    db = lib["db"]
    db.queue_change(lib["a"], "flag", "flagged")
    row = db.get_pending_changes()[0]
    stale = {"id": row["id"], "change_token": "not-the-token"}
    gone = {"id": row["id"] + 100, "change_token": None}
    assert db.claim_pending_changes_for_sync([stale, gone]) == []
    assert db.get_pending_changes()[0]["sync_started"] == 0


def test_claim_chunks_by_400(lib):
    db, ws = lib["db"], lib["ws"]
    for i in range(401):
        _insert(db, lib["a"], "title", str(i), ws, token=f"t{i}")
    rows = db.get_pending_changes()
    statements = _trace(db)
    claimed = db.claim_pending_changes_for_sync(rows)
    db.conn.set_trace_callback(None)
    assert len(claimed) == 401
    assert len({s for s in statements if "UPDATE pending_changes" in s}) == 2


def test_claim_requires_active_workspace(lib):
    db = lib["db"]
    db.queue_change(lib["a"], "flag", "flagged")
    rows = db.get_pending_changes()
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError, match="No active workspace"):
        db.claim_pending_changes_for_sync(rows)


# -- get_pending_keyword_removal_keys ---------------------------------------------


def test_pending_keyword_removal_keys(lib):
    db, ws, other = lib["db"], lib["ws"], lib["other"]
    _insert(db, lib["a"], "keyword_remove", "Robin", ws)
    _insert(db, lib["a"], "keyword_remove_flat", "WREN", other)
    _insert(db, lib["a"], "keyword_add", "Jay", ws)
    _insert(db, lib["a"], "keyword_remove", '""', ws)
    _insert(db, lib["b"], "keyword_remove", "Finch", ws)
    assert db.get_pending_keyword_removal_keys(lib["a"]) == {"robin", "wren"}
    assert db.get_pending_keyword_removal_keys(
        lib["a"], hierarchical=True
    ) == {"robin"}
    assert db.get_pending_keyword_removal_keys(lib["c"]) == set()
    # Reads across workspaces, so no active workspace is needed.
    db.set_active_workspace(None)
    assert db.get_pending_keyword_removal_keys(lib["a"]) == {"robin", "wren"}


# -- _pending_keyword_sidecar_alias -------------------------------------------------


def test_sidecar_alias_same_stem_shares_sidecar(lib):
    db, ws = lib["db"], lib["ws"]
    assert db._pending_keyword_sidecar_alias(lib["a"], ws, "Robin") is False
    _insert(db, lib["b"], "keyword_add", "Robin", ws)
    # Different stem -> different sidecar.
    assert db._pending_keyword_sidecar_alias(lib["a"], ws, "Robin") is False
    _insert(db, lib["a_raw"], "keyword_add", "ROBIN", ws)
    # a.jpg and a.cr2 share a.xmp; value matches case-insensitively.
    assert db._pending_keyword_sidecar_alias(lib["a"], ws, "Robin") is True
    # Other workspace or non-keyword rows are ignored.
    assert db._pending_keyword_sidecar_alias(lib["a"], lib["other"], "Robin") is False
    _insert(db, lib["a_raw"], "title", "Jay", ws)
    assert db._pending_keyword_sidecar_alias(lib["a"], ws, "Jay") is False


def test_sidecar_alias_missing_own_photo(lib):
    db, ws = lib["db"], lib["ws"]
    _insert(db, lib["b"], "keyword_add", "Robin", ws)
    assert db._pending_keyword_sidecar_alias(999999, ws, "Robin") is False


def test_sidecar_alias_case_variant_requires_samefile(db, tmp_path):
    ws = db._active_workspace_id
    upper = tmp_path / "Dir"
    lower = tmp_path / "dir"
    upper.mkdir()
    # On a case-insensitive filesystem "dir" is the same directory.
    with contextlib.suppress(FileExistsError):
        lower.mkdir()
    fu = db.add_folder(str(upper), name="Dir")
    fl = db.add_folder(str(lower), name="dir")
    pu = db.add_photo(fu, "a.jpg", ".jpg", 1, 1.0)
    pl = db.add_photo(fl, "a.png", ".png", 1, 1.0)
    _insert(db, pl, "keyword_add", "Robin", ws)
    # Raw DB paths differ ("Dir" vs "dir"): production must confirm with
    # samefile before treating them as aliases. Neither sidecar exists yet,
    # so the parent folders decide: one case-insensitive directory (macOS,
    # Windows) means the same future sidecar, two case-sensitive
    # directories (Linux) mean distinct ones.
    same_dir = os.path.samefile(upper, lower)
    assert db._pending_keyword_sidecar_alias(pu, ws, "Robin") is same_dir
    (upper / "a.xmp").write_text("x")
    if not (lower / "a.xmp").exists():
        os.link(upper / "a.xmp", lower / "a.xmp")
    assert db._pending_keyword_sidecar_alias(pu, ws, "Robin") is True


def test_sidecar_alias_case_variant_distinct_files(db, tmp_path):
    ws = db._active_workspace_id
    fu = db.add_folder(str(tmp_path / "Nope"), name="Nope")
    fl = db.add_folder(str(tmp_path / "nope"), name="nope")
    fx = db.add_folder(str(tmp_path / "elsewhere"), name="elsewhere")
    pu = db.add_photo(fu, "a.jpg", ".jpg", 1, 1.0)
    pl = db.add_photo(fl, "a.png", ".png", 1, 1.0)
    px = db.add_photo(fx, "z.jpg", ".jpg", 1, 1.0)
    _insert(db, pl, "keyword_add", "Robin", ws)
    _insert(db, px, "keyword_add", "Robin", ws)
    # No sidecars on disk: production cannot confirm any aliasing with
    # samefile (raises OSError) and must return False on every platform,
    # so a case-fold collision on Windows does not queue a destructive
    # inverse keyword removal against an unrelated file.
    assert db._pending_keyword_sidecar_alias(pu, ws, "Robin") is False


def test_sidecar_alias_missing_sidecars_on_case_insensitive_volume(
    db, tmp_path, monkeypatch
):
    """Missing case-fold-aliased sidecars queue inverse on case-insensitive fs.

    Mirrors ``_sidecar_target_identities`` in ``vireo/sync.py``, which
    groups missing case-fold aliases as one write target. Simulates a
    case-insensitive volume (e.g. a normal Windows drive) by folding
    ``normcase`` to lowercase and making ``samefile`` treat case-different
    parent directory spellings as the same inode.
    """
    upper = tmp_path / "Dir"
    lower = tmp_path / "dir"
    upper.mkdir()
    with contextlib.suppress(FileExistsError):
        lower.mkdir()
    # A case-insensitive volume folds names inside the directory too, so
    # a case-swap probe on any existing entry resolves back. Give the
    # probe something to probe (the sidecars themselves are still missing).
    (upper / "keep.txt").write_bytes(b"x")
    ws = db._active_workspace_id
    fu = db.add_folder(str(upper), name="Dir")
    fl = db.add_folder(str(lower), name="dir")
    pu = db.add_photo(fu, "a.jpg", ".jpg", 1, 1.0)
    pl = db.add_photo(fl, "a.png", ".png", 1, 1.0)
    _insert(db, pl, "keyword_add", "Robin", ws)

    real_samefile = os.path.samefile
    upper_str = str(upper)
    lower_str = str(lower)
    keep = str(upper / "keep.txt")
    keep_swapped = str(upper / "KEEP.TXT")

    def fake_samefile(a, b):
        pair = {str(a), str(b)}
        if pair == {upper_str, lower_str}:
            return True  # grandparent folds case on directory lookup
        if pair == {keep, keep_swapped}:
            return True  # inside dir also folds case (case-insensitive volume)
        return real_samefile(a, b)

    monkeypatch.setattr("vireo.repositories.sync.os.path.samefile", fake_samefile)
    monkeypatch.setattr(
        "vireo.repositories.sync.os.path.normcase", lambda p: p.lower()
    )
    # Sidecars do not exist yet, so path-level samefile raises. The parent
    # samefile shim shows the fs aliases the directories, and the child
    # probe shows the fs folds case for entries inside them: the missing
    # sidecars will alias, so a cancellation queues its inverse and the
    # sibling's write cannot leave the cancelled keyword on the shared
    # sidecar.
    assert db._pending_keyword_sidecar_alias(pu, ws, "Robin") is True


def test_sidecar_alias_missing_sidecars_on_case_sensitive_windows(
    db, tmp_path, monkeypatch
):
    """Case-sensitive Windows: missing case-fold sidecars stay distinct.

    Simulates per-directory case-sensitive Windows (opt-in via fsutil):
    ``normcase`` still folds case but the two directories are kept as
    distinct inodes on disk. The alias check must not queue a destructive
    inverse removal against the unrelated sidecar.
    """
    upper = tmp_path / "Dir"
    lower = tmp_path / "dir"
    upper.mkdir()
    if lower.exists():
        pytest.skip("tmp_path is on a case-insensitive filesystem")
    lower.mkdir()
    ws = db._active_workspace_id
    fu = db.add_folder(str(upper), name="Dir")
    fl = db.add_folder(str(lower), name="dir")
    pu = db.add_photo(fu, "a.jpg", ".jpg", 1, 1.0)
    pl = db.add_photo(fl, "a.png", ".png", 1, 1.0)
    _insert(db, pl, "keyword_add", "Robin", ws)
    monkeypatch.setattr(
        "vireo.repositories.sync.os.path.normcase", lambda p: p.lower()
    )
    # Real samefile keeps the two directories distinct because the fs is
    # case-sensitive. Sidecars are missing, path-level samefile raises,
    # and the parent-samefile fallback returns False -- no destructive
    # inverse queued.
    assert db._pending_keyword_sidecar_alias(pu, ws, "Robin") is False


def _same_dir_case_variant_stems(db, tmp_path):
    """Two photos in one folder whose stems differ only by case, on disk."""
    parent = tmp_path / "photos"
    parent.mkdir()
    (parent / "A.raw").write_bytes(b"raw")
    ws = db._active_workspace_id
    folder = db.add_folder(str(parent), name="photos")
    pu = db.add_photo(folder, "A.raw", ".raw", 1, 1.0)
    pl = db.add_photo(folder, "a.jpg", ".jpg", 1, 1.0)
    _insert(db, pl, "keyword_add", "Robin", ws)
    return parent, pu, ws


def test_sidecar_alias_same_dir_case_variant_stems_follow_the_folder(db, tmp_path):
    """``A.xmp`` and ``a.xmp`` in one folder are one sidecar exactly when that
    folder folds case, so a cancellation queues its inverse only then."""
    parent, pu, ws = _same_dir_case_variant_stems(db, tmp_path)
    folds = (parent / "a.RAW").exists()
    assert db._pending_keyword_sidecar_alias(pu, ws, "Robin") is folds


def test_sidecar_alias_same_dir_case_variant_stems_on_case_insensitive_folder(
    db, tmp_path, monkeypatch
):
    """Simulated case-insensitive folder: the entry's swapped spelling is
    the same file, so the stems alias and the inverse is queued."""
    parent, pu, ws = _same_dir_case_variant_stems(db, tmp_path)
    entry, swapped = str(parent / "A.raw"), str(parent / "a.RAW")
    real_samefile = os.path.samefile

    def folding_samefile(a, b):
        if {str(a), str(b)} == {entry, swapped}:
            return True
        return real_samefile(a, b)

    monkeypatch.setattr(os.path, "samefile", folding_samefile)
    assert db._pending_keyword_sidecar_alias(pu, ws, "Robin") is True


def test_sidecar_alias_case_sensitive_folder_inside_case_insensitive_parent(
    db, tmp_path, monkeypatch
):
    """Windows per-directory case sensitivity: the folder's own name folds
    in its parent, but names inside it do not. The probe must ask the
    folder, not its parent, or it would queue a destructive inverse
    against a distinct sidecar."""
    parent, pu, ws = _same_dir_case_variant_stems(db, tmp_path)
    folder_spellings = {str(parent), str(tmp_path / "PHOTOS")}
    entry, swapped = str(parent / "A.raw"), str(parent / "a.RAW")
    real_samefile = os.path.samefile

    def per_directory_samefile(a, b):
        pair = {str(a), str(b)}
        if pair == folder_spellings:
            return True
        if pair == {entry, swapped}:
            raise FileNotFoundError(swapped)
        return real_samefile(a, b)

    monkeypatch.setattr(os.path, "samefile", per_directory_samefile)
    assert db._pending_keyword_sidecar_alias(pu, ws, "Robin") is False


def test_sidecar_alias_case_sensitive_child_reached_via_case_variant_parents(
    db, tmp_path, monkeypatch
):
    """Two photos in case-variant parent folders that ``samefile`` to the
    same case-sensitive child directory: names inside the resolved folder
    are still distinct, so the missing sidecars must not alias."""
    parent = tmp_path / "photos"
    parent.mkdir()
    (parent / "A.raw").write_bytes(b"raw")
    alias_parent = tmp_path / "PHOTOS"
    ws = db._active_workspace_id
    fu = db.add_folder(str(parent), name="photos")
    fl = db.add_folder(str(alias_parent), name="PHOTOS")
    pu = db.add_photo(fu, "A.raw", ".raw", 1, 1.0)
    pl = db.add_photo(fl, "a.jpg", ".jpg", 1, 1.0)
    _insert(db, pl, "keyword_add", "Robin", ws)
    parent_pair = {str(parent), str(alias_parent)}
    entry_pair = {str(parent / "A.raw"), str(parent / "a.RAW")}
    real_samefile = os.path.samefile

    def per_directory_samefile(a, b):
        pair = {str(a), str(b)}
        if pair == parent_pair:
            return True  # grandparent folds case on the folder name lookup
        if pair == entry_pair:
            raise FileNotFoundError(str(parent / "a.RAW"))  # child is case-sensitive
        return real_samefile(a, b)

    monkeypatch.setattr(os.path, "samefile", per_directory_samefile)
    assert db._pending_keyword_sidecar_alias(pu, ws, "Robin") is False


def test_sidecar_alias_ignores_case_swapped_hard_link_pair(
    db, tmp_path, monkeypatch
):
    """A hard link with a case-swapped name on a case-sensitive directory
    aliases via ``samefile`` even though the directory itself does not
    fold case. The probe must ignore that pair or it would queue a
    destructive inverse against an unrelated sidecar."""
    parent = tmp_path / "photos"
    parent.mkdir()
    (parent / "A.raw").write_bytes(b"raw")
    # Simulate a case-swapped hard link entry regardless of the host fs.
    hard_link_names = ["A.raw", "a.RAW"]
    real_scandir = os.scandir

    class _FakeEntry:
        def __init__(self, name, dir_path):
            self.name = name
            self.path = os.path.join(dir_path, name)

    class _FakeScandir:
        def __init__(self, dir_path):
            self._dir_path = dir_path
            self._real = None
            if os.fspath(dir_path) != str(parent):
                self._real = real_scandir(dir_path)

        def __enter__(self):
            if self._real is not None:
                return iter(self._real.__enter__())
            return iter(
                _FakeEntry(name, str(parent)) for name in hard_link_names
            )

        def __exit__(self, exc_type, exc, tb):
            if self._real is not None:
                return self._real.__exit__(exc_type, exc, tb)
            return False

    monkeypatch.setattr("vireo.repositories.sync.os.scandir", _FakeScandir)

    real_samefile = os.path.samefile
    entry_pair = {str(parent / "A.raw"), str(parent / "a.RAW")}

    def hardlinked_samefile(a, b):
        # The two spellings point to the same inode (hard link) even
        # though the directory is case-sensitive.
        if {str(a), str(b)} == entry_pair:
            return True
        return real_samefile(a, b)

    monkeypatch.setattr(os.path, "samefile", hardlinked_samefile)
    ws = db._active_workspace_id
    folder = db.add_folder(str(parent), name="photos")
    pu = db.add_photo(folder, "A.raw", ".raw", 1, 1.0)
    pl = db.add_photo(folder, "a.jpg", ".jpg", 1, 1.0)
    _insert(db, pl, "keyword_add", "Robin", ws)
    # Hard-linked entry pair is ignored; no other entry to probe, so the
    # answer stays conservatively False and no inverse is queued.
    assert db._pending_keyword_sidecar_alias(pu, ws, "Robin") is False


# -- remove_pending_changes ------------------------------------------------------------


def test_remove_pending_changes_filters_and_commits(lib):
    db, ws, other = lib["db"], lib["ws"], lib["other"]
    _insert(db, lib["a"], "flag", "flagged", ws)
    _insert(db, lib["a"], "rating", "3", ws)
    _insert(db, lib["a"], "rating", "4", ws)
    _insert(db, lib["a"], "rating", "3", other)
    _insert(db, lib["b"], "rating", "3", ws)
    assert db.remove_pending_changes(lib["a"], "rating", "3") == 1
    assert not db.conn.in_transaction
    assert (lib["a"], "rating", "3", ws, 0) not in _visible_rows(db)
    assert db.remove_pending_changes(lib["a"], "rating") == 1
    assert db.remove_pending_changes(lib["a"], workspace_id=other) == 1
    assert db.remove_pending_changes(lib["a"]) == 1
    assert db.remove_pending_changes(lib["a"]) == 0
    assert _visible_rows(db) == [(lib["b"], "rating", "3", ws, 0)]


def test_remove_pending_changes_no_commit(lib):
    db, ws = lib["db"], lib["ws"]
    _insert(db, lib["a"], "flag", "flagged", ws)
    assert db.remove_pending_changes(lib["a"], _commit=False) == 1
    assert db.conn.in_transaction
    assert len(_visible_rows(db)) == 1
    db.conn.rollback()


def test_remove_pending_changes_requires_workspace(lib):
    db = lib["db"]
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError, match="No active workspace"):
        db.remove_pending_changes(lib["a"])


def test_remove_uncaptured_keyword_has_no_inverse(lib):
    db, ws = lib["db"], lib["ws"]
    _insert(db, lib["a"], "keyword_add", "Robin", ws)
    _insert(db, lib["a"], "keyword_remove_flat", "Jay", ws, sync_started=1)
    _insert(db, lib["a"], "flag", "flagged", ws, sync_started=1)
    assert db.remove_pending_changes(lib["a"]) == 3
    assert _visible_rows(db) == []


def test_remove_captured_keyword_queues_inverse(lib):
    db, ws = lib["db"], lib["ws"]
    _insert(db, lib["a"], "keyword_add", "Robin", ws, sync_started=1)
    _insert(db, lib["a"], "keyword_remove", "Wren", ws, sync_started=1)
    assert db.remove_pending_changes(lib["a"], _commit=False) == 2
    assert db.conn.in_transaction
    db.conn.commit()
    assert _visible_rows(db) == [
        (lib["a"], "keyword_remove", "Robin", ws, 1),
        (lib["a"], "keyword_add", "Wren", ws, 1),
    ]


def test_remove_aliased_keyword_queues_inverse(lib):
    db, ws = lib["db"], lib["ws"]
    _insert(db, lib["a"], "keyword_add", "Robin", ws)
    _insert(db, lib["a_raw"], "keyword_add", "Robin", ws)
    assert db.remove_pending_changes(lib["a"], "keyword_add") == 1
    assert _visible_rows(db) == [
        (lib["a_raw"], "keyword_add", "Robin", ws, 0),
        (lib["a"], "keyword_remove", "Robin", ws, 1),
    ]


def test_remove_routes_composition_through_facade(lib, monkeypatch):
    db, ws, other = lib["db"], lib["ws"], lib["other"]
    _insert(db, lib["a"], "keyword_add", "Robin", other)
    queued, alias_calls = [], []
    real_queue = db.queue_change

    def alias(photo_id, workspace_id, value):
        alias_calls.append((photo_id, workspace_id, value))
        return True

    def recorder(*args, **kwargs):
        queued.append((args, kwargs))
        return real_queue(*args, **kwargs)

    monkeypatch.setattr(db, "_pending_keyword_sidecar_alias", alias)
    monkeypatch.setattr(db, "queue_change", recorder)
    assert db.remove_pending_changes(lib["a"], workspace_id=other) == 1
    assert alias_calls == [(lib["a"], other, "Robin")]
    assert queued == [
        ((lib["a"], "keyword_remove", "Robin"),
         {"workspace_id": other, "_commit": False}),
    ]
    assert _visible_rows(db) == [(lib["a"], "keyword_remove", "Robin", other, 1)]
    assert ws != other


# -- remove_pending_change_token ---------------------------------------------------


def test_remove_pending_change_token(lib):
    db, ws, other = lib["db"], lib["ws"], lib["other"]
    db.set_active_workspace(None)
    assert db.remove_pending_change_token(None) == 0
    assert db.remove_pending_change_token("") == 0
    db.set_active_workspace(ws)
    _insert(db, lib["a"], "flag", "flagged", ws, token="tok")
    _insert(db, lib["b"], "flag", "flagged", other, token="tok")
    assert db.remove_pending_change_token("missing") == 0
    assert db.remove_pending_change_token("tok") == 1
    assert not db.conn.in_transaction
    assert _visible_rows(db) == [(lib["b"], "flag", "flagged", other, 0)]
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError, match="No active workspace"):
        db.remove_pending_change_token("tok")


# -- clear_pending -----------------------------------------------------------------


def test_clear_pending_empty_is_noop_without_workspace(lib):
    db = lib["db"]
    db.set_active_workspace(None)
    assert db.clear_pending([]) is None
    assert db.clear_pending([], expected_tokens=["x"]) is None


def test_clear_pending_by_ids_scoped_and_committed(lib):
    db, ws, other = lib["db"], lib["ws"], lib["other"]
    mine = _insert(db, lib["a"], "flag", "flagged", ws)
    theirs = _insert(db, lib["a"], "flag", "flagged", other)
    keep = _insert(db, lib["b"], "flag", "flagged", ws)
    assert db.clear_pending([mine, theirs]) is None
    assert not db.conn.in_transaction
    assert _visible_rows(db) == [
        (lib["a"], "flag", "flagged", other, 0),
        (lib["b"], "flag", "flagged", ws, 0),
    ]
    assert keep


def test_clear_pending_chunks_ids(lib):
    db, ws = lib["db"], lib["ws"]
    ids = [_insert(db, lib["a"], "title", str(i), ws) for i in range(3)]
    statements = _trace(db)
    db.clear_pending(ids + list(range(10_000, 10_800)))
    db.conn.set_trace_callback(None)
    assert len({s for s in statements if s.startswith("DELETE")}) == 2
    assert _rows(db) == []


def test_clear_pending_requires_workspace(lib):
    db = lib["db"]
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError, match="No active workspace"):
        db.clear_pending([1])
    # The workspace is resolved before the token-length check.
    with pytest.raises(RuntimeError, match="No active workspace"):
        db.clear_pending([1], expected_tokens=[])


def test_clear_pending_expected_tokens_length_mismatch(lib):
    db, ws = lib["db"], lib["ws"]
    rid = _insert(db, lib["a"], "flag", "flagged", ws, token="t")
    with pytest.raises(
        ValueError, match="expected_tokens must be the same length as change_ids"
    ):
        db.clear_pending([rid], expected_tokens=["t", "u"])
    assert len(_rows(db)) == 1


def test_clear_pending_expected_tokens(lib):
    db, ws, other = lib["db"], lib["ws"], lib["other"]
    tokened = _insert(db, lib["a"], "flag", "flagged", ws, token="t1")
    replaced = _insert(db, lib["b"], "flag", "rejected", ws, token="new")
    legacy = _insert(db, lib["c"], "flag", "flagged", ws, token=None)
    legacy_now_tokened = _insert(db, lib["a"], "title", "x", ws, token="late")
    foreign = _insert(db, lib["b"], "title", "y", other, token="t1")
    db.clear_pending(
        [tokened, replaced, legacy, legacy_now_tokened],
        expected_tokens=["t1", "old", None, None],
    )
    assert not db.conn.in_transaction
    remaining = {
        r["id"] for r in db.conn.execute("SELECT id FROM pending_changes")
    }
    assert remaining == {replaced, legacy_now_tokened, foreign}


def test_clear_pending_equivalent_flat_removals_by_id(lib, monkeypatch):
    db, ws, other = lib["db"], lib["ws"], lib["other"]
    flat = _insert(db, lib["a"], "keyword_remove_flat", "Robin", ws)
    add = _insert(db, lib["a"], "keyword_add", "Jay", ws)
    _insert(db, lib["a"], "keyword_remove_flat", "ROBIN", other)
    _insert(db, lib["a"], "keyword_remove", "Robin", other)
    _insert(db, lib["b"], "keyword_remove_flat", "Robin", other)
    calls = []
    real = db.clear_equivalent_flat_removals

    def recorder(changes, _commit=True):
        calls.append(([tuple(c) for c in changes], _commit))
        return real(changes, _commit=_commit)

    monkeypatch.setattr(db, "clear_equivalent_flat_removals", recorder)
    db.clear_pending([flat, add], clear_equivalent_flat_removals=True)
    assert calls == [([(lib["a"], "keyword_remove_flat", "Robin")], False)]
    assert not db.conn.in_transaction
    assert _visible_rows(db) == [
        (lib["a"], "keyword_remove", "Robin", other, 0),
        (lib["b"], "keyword_remove_flat", "Robin", other, 0),
    ]


def test_clear_pending_flat_flag_without_flat_rows_skips_helper(lib, monkeypatch):
    db, ws = lib["db"], lib["ws"]
    add = _insert(db, lib["a"], "keyword_add", "Jay", ws)
    monkeypatch.setattr(
        db, "clear_equivalent_flat_removals",
        lambda *a, **k: pytest.fail("helper should not run"),
    )
    db.clear_pending([add], clear_equivalent_flat_removals=True)
    db.clear_pending([add], expected_tokens=[None],
                     clear_equivalent_flat_removals=True)
    assert _visible_rows(db) == []


def test_clear_pending_equivalent_flat_removals_with_tokens(lib):
    db, ws, other = lib["db"], lib["ws"], lib["other"]
    tok = _insert(db, lib["a"], "keyword_remove_flat", "Robin", ws, token="t")
    leg = _insert(db, lib["b"], "keyword_remove_flat", "Wren", ws, token=None)
    _insert(db, lib["a"], "keyword_remove_flat", "robin", other, token="o1")
    _insert(db, lib["b"], "keyword_remove_flat", "WREN", other, token="o2")
    _insert(db, lib["c"], "keyword_remove_flat", "Robin", other, token="o3")
    db.clear_pending(
        [tok, leg], expected_tokens=["t", None],
        clear_equivalent_flat_removals=True,
    )
    assert not db.conn.in_transaction
    assert _visible_rows(db) == [
        (lib["c"], "keyword_remove_flat", "Robin", other, 0),
    ]


def test_clear_pending_expected_tokens_chunks(lib):
    db, ws = lib["db"], lib["ws"]
    ids = [_insert(db, lib["a"], "title", str(i), ws, token=f"t{i}")
           for i in range(801)]
    legacy = [_insert(db, lib["b"], "title", str(i), ws) for i in range(801)]
    statements = _trace(db)
    db.clear_pending(
        ids + legacy,
        expected_tokens=[f"t{i}" for i in range(801)] + [None] * 801,
        clear_equivalent_flat_removals=True,
    )
    db.conn.set_trace_callback(None)
    assert len({s for s in statements if s.startswith("DELETE")}) == 4
    assert len({s for s in statements if s.lstrip().startswith("SELECT")}) == 4
    assert _rows(db) == []


# -- clear_pending_by_token --------------------------------------------------------------


def test_clear_pending_by_token(lib):
    db, ws, other = lib["db"], lib["ws"], lib["other"]
    db.set_active_workspace(None)
    assert db.clear_pending_by_token([]) is None
    db.set_active_workspace(ws)
    _insert(db, lib["a"], "flag", "flagged", ws, token="t1")
    _insert(db, lib["b"], "flag", "flagged", ws, token="t2")
    _insert(db, lib["c"], "flag", "flagged", other, token="t1")
    assert db.clear_pending_by_token(["t1", "nope"]) is None
    assert not db.conn.in_transaction
    assert _visible_rows(db) == [
        (lib["b"], "flag", "flagged", ws, 0),
        (lib["c"], "flag", "flagged", other, 0),
    ]
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError, match="No active workspace"):
        db.clear_pending_by_token(["t2"])


def test_clear_pending_by_token_flat_removals_and_chunks(lib, monkeypatch):
    db, ws, other = lib["db"], lib["ws"], lib["other"]
    _insert(db, lib["a"], "keyword_remove_flat", "Robin", ws, token="t0")
    for i in range(1, 801):
        _insert(db, lib["b"], "title", str(i), ws, token=f"t{i}")
    _insert(db, lib["a"], "keyword_remove_flat", "ROBIN", other, token="o")
    calls = []
    real = db.clear_equivalent_flat_removals

    def recorder(changes, _commit=True):
        calls.append(([tuple(c) for c in changes], _commit))
        return real(changes, _commit=_commit)

    monkeypatch.setattr(db, "clear_equivalent_flat_removals", recorder)
    statements = _trace(db)
    db.clear_pending_by_token(
        [f"t{i}" for i in range(801)], clear_equivalent_flat_removals=True,
    )
    db.conn.set_trace_callback(None)
    assert calls == [([(lib["a"], "keyword_remove_flat", "Robin")], False)]
    assert len({
        s for s in statements
        if s.startswith("DELETE FROM pending_changes WHERE change_token")
    }) == 2
    assert _visible_rows(db) == []


# -- clear_equivalent_flat_removals ------------------------------------------------------


def test_clear_equivalent_flat_removals(lib):
    db, ws, other = lib["db"], lib["ws"], lib["other"]
    _insert(db, lib["a"], "keyword_remove_flat", "robin", other)
    _insert(db, lib["a"], "keyword_remove", "Robin", other)
    _insert(db, lib["b"], "keyword_remove_flat", "Robin", other)
    changes = [
        {"photo_id": lib["a"], "change_type": "keyword_remove_flat", "value": "Robin"},
        {"photo_id": lib["b"], "change_type": "keyword_add", "value": "Robin"},
    ]
    db.clear_equivalent_flat_removals(changes, _commit=False)
    assert db.conn.in_transaction
    assert len(_visible_rows(db)) == 3
    db.conn.commit()
    db.clear_equivalent_flat_removals(changes)
    assert not db.conn.in_transaction
    assert _visible_rows(db) == [
        (lib["a"], "keyword_remove", "Robin", other, 0),
        (lib["b"], "keyword_remove_flat", "Robin", other, 0),
    ]
    assert ws


def test_clear_equivalent_flat_removals_no_flat_changes(lib):
    db = lib["db"]
    db.set_active_workspace(None)
    statements = _trace(db)
    db.clear_equivalent_flat_removals([
        {"photo_id": lib["a"], "change_type": "keyword_add", "value": "x"},
    ])
    db.clear_equivalent_flat_removals([], _commit=False)
    db.conn.set_trace_callback(None)
    assert not any("DELETE" in s for s in statements)


# -- queue_flag_change_if_enabled ------------------------------------------------------


@pytest.fixture
def flag_config(tmp_path, monkeypatch):
    import config as cfg

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    def set_enabled(value):
        conf = cfg.load()
        conf["sync_flags_to_xmp"] = value
        cfg.save(conf)

    return set_enabled


def test_queue_flag_change_enabled(lib, flag_config):
    db, ws = lib["db"], lib["ws"]
    flag_config(True)
    first = db.queue_flag_change_if_enabled(lib["a"], "flagged")
    assert first is not None
    assert not db.conn.in_transaction
    second = db.queue_flag_change_if_enabled(lib["a"], "rejected")
    assert second not in (None, first)
    assert db.queue_flag_change_if_enabled(lib["b"], None) is not None
    assert db.queue_flag_change_if_enabled(lib["c"], "") is not None
    assert _visible_rows(db) == [
        (lib["a"], "flag", "rejected", ws, 0),
        (lib["b"], "flag", "none", ws, 0),
        (lib["c"], "flag", "none", ws, 0),
    ]


def test_queue_flag_change_enabled_no_commit(lib, flag_config):
    db, other = lib["db"], lib["other"]
    flag_config(True)
    token = db.queue_flag_change_if_enabled(
        lib["a"], "flagged", workspace_id=other, _commit=False,
    )
    assert token is not None
    assert db.conn.in_transaction
    assert _visible_rows(db) == []
    db.conn.commit()
    assert _visible_rows(db) == [(lib["a"], "flag", "flagged", other, 0)]


def test_queue_flag_change_disabled_removes_existing(lib, flag_config):
    db, ws = lib["db"], lib["ws"]
    flag_config(False)
    _insert(db, lib["a"], "flag", "flagged", ws)
    _insert(db, lib["a"], "rating", "2", ws)
    assert db.queue_flag_change_if_enabled(lib["a"], "rejected") is None
    assert not db.conn.in_transaction
    assert _visible_rows(db) == [(lib["a"], "rating", "2", ws, 0)]
    _insert(db, lib["a"], "flag", "flagged", ws)
    assert db.queue_flag_change_if_enabled(
        lib["a"], "rejected", _commit=False
    ) is None
    assert db.conn.in_transaction
    db.conn.rollback()


def test_queue_flag_change_invalid_flag(lib, flag_config, caplog):
    db, ws = lib["db"], lib["ws"]
    flag_config(True)
    _insert(db, lib["a"], "flag", "flagged", ws)
    with caplog.at_level("WARNING"):
        assert db.queue_flag_change_if_enabled(lib["a"], "bogus") is None
    assert "Not queueing invalid XMP flag value" in caplog.text
    assert not db.conn.in_transaction
    assert _visible_rows(db) == []
    _insert(db, lib["a"], "flag", "flagged", ws)
    assert db.queue_flag_change_if_enabled(
        lib["a"], "bogus", _commit=False
    ) is None
    assert db.conn.in_transaction
    db.conn.rollback()


def test_queue_flag_change_config_error_is_disabled(lib, flag_config, monkeypatch, caplog):
    db, ws = lib["db"], lib["ws"]
    flag_config(True)

    def boom(*a, **k):
        raise ValueError("bad config")

    monkeypatch.setattr(db, "get_effective_config", boom)
    _insert(db, lib["a"], "flag", "flagged", ws)
    with caplog.at_level("WARNING"):
        assert db.queue_flag_change_if_enabled(lib["a"], "rejected") is None
    assert "Failed to read sync_flags_to_xmp config" in caplog.text
    assert not db.conn.in_transaction
    assert _visible_rows(db) == []


def test_queue_flag_change_routes_through_facade(lib, flag_config, monkeypatch):
    db, ws = lib["db"], lib["ws"]
    flag_config(False)
    monkeypatch.setattr(
        db, "get_effective_config", lambda conf: {"sync_flags_to_xmp": True},
    )
    removed, queued = [], []
    real_remove, real_queue = db.remove_pending_changes, db.queue_change

    def rec_remove(*a, **k):
        removed.append((a, k))
        return real_remove(*a, **k)

    def rec_queue(*a, **k):
        queued.append((a, k))
        return real_queue(*a, **k)

    monkeypatch.setattr(db, "remove_pending_changes", rec_remove)
    monkeypatch.setattr(db, "queue_change", rec_queue)
    assert db.queue_flag_change_if_enabled(lib["a"], "flagged")
    assert removed == [((lib["a"], "flag"), {"workspace_id": ws, "_commit": False})]
    assert queued == [
        ((lib["a"], "flag", "flagged"), {"workspace_id": ws, "_commit": False}),
    ]


def test_queue_flag_change_requires_workspace(lib, flag_config):
    db = lib["db"]
    flag_config(True)
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError, match="No active workspace"):
        db.queue_flag_change_if_enabled(lib["a"], "flagged")


# -- staged_sync_scope / staged_sync_scope_by_photos -------------------------------------


@pytest.fixture
def staged(lib):
    db, ws, other = lib["db"], lib["ws"], lib["other"]
    ids = {
        "a_here": _insert(db, lib["a"], "flag", "flagged", ws, token="ta"),
        "a_other": _insert(db, lib["a"], "flag", "flagged", other, token="to"),
        "b_other": _insert(db, lib["b"], "rating", "2", other, token="tb"),
        "c_here": _insert(db, lib["c"], "rating", "3", ws, token=None),
    }
    return {**lib, **ids}


def test_staged_sync_scope_by_folder(staged):
    db, s = staged["db"], staged
    changes, here, elsewhere, overlap = db.staged_sync_scope([s["f1"], s["f2"]])
    assert sorted(changes, key=lambda c: c[1]) == [
        ("ta", s["a_here"], s["a"]),
        (("id", s["c_here"]), s["c_here"], s["c"]),
    ]
    assert (here, elsewhere, overlap) == (2, 1, 1)
    assert db.staged_sync_scope([s["f2"]]) == (
        [(("id", s["c_here"]), s["c_here"], s["c"])], 1, 0, 0,
    )
    assert db.staged_sync_scope([]) == ([], 0, 0, 0)


def test_staged_sync_scope_by_photos(staged):
    db, s = staged["db"], staged
    changes, here, elsewhere, overlap = db.staged_sync_scope_by_photos(
        [s["a"], s["b"], s["c"]]
    )
    assert sorted(changes, key=lambda c: c[1]) == [
        ("ta", s["a_here"], s["a"]),
        (("id", s["c_here"]), s["c_here"], s["c"]),
    ]
    assert (here, elsewhere, overlap) == (2, 1, 1)
    assert db.staged_sync_scope_by_photos([s["b"]]) == ([], 0, 1, 0)
    assert db.staged_sync_scope_by_photos([]) == ([], 0, 0, 0)


def test_staged_sync_scope_resolves_workspace_lazily(staged):
    db, s = staged["db"], staged
    db.set_active_workspace(None)
    # No rows matched -> the active workspace is never consulted.
    assert db.staged_sync_scope([999999]) == ([], 0, 0, 0)
    assert db.staged_sync_scope_by_photos([999999]) == ([], 0, 0, 0)
    assert db.staged_sync_scope_by_photos([]) == ([], 0, 0, 0)
    with pytest.raises(RuntimeError, match="No active workspace"):
        db.staged_sync_scope([s["f1"]])
    with pytest.raises(RuntimeError, match="No active workspace"):
        db.staged_sync_scope_by_photos([s["a"]])


def test_staged_sync_scope_chunks(staged):
    db, s = staged["db"], staged
    statements = _trace(db)
    folder_ids = [s["f1"]] + list(range(100_000, 100_800))
    photo_ids = [s["a"]] + list(range(100_000, 100_800))
    by_folder = db.staged_sync_scope(folder_ids)
    by_photo = db.staged_sync_scope_by_photos(photo_ids)
    db.conn.set_trace_callback(None)
    assert sum(1 for s_ in statements if "FROM pending_changes" in s_) == 4
    assert by_folder[1:] == (1, 1, 1)
    assert by_photo[1:] == (1, 0, 1)


# -- structure ----------------------------------------------------------------------------


_DELEGATING_SYNC_METHODS = (
    "count_pending_changes",
    "staged_sync_scope_by_photos",
    "staged_sync_scope",
    "queue_change",
    "get_pending_changes",
    "claim_pending_changes_for_sync",
    "get_pending_keyword_removal_keys",
    "_pending_keyword_sidecar_alias",
    "remove_pending_changes",
    "remove_pending_change_token",
    "clear_pending",
    "clear_pending_by_token",
    "clear_equivalent_flat_removals",
    "queue_flag_change_if_enabled",
)


@pytest.mark.parametrize("name", _DELEGATING_SYNC_METHODS)
def test_sync_method_delegates_to_repository(name):
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
        f"Database.{name} touches self.conn; move the SQL to SyncRepository"
    )
    assert "_sync_repository" in attrs, (
        f"Database.{name} no longer delegates to SyncRepository"
    )


def test_sync_facade_signatures_unchanged():
    sig = {n: str(inspect.signature(getattr(Database, n)))
           for n in _DELEGATING_SYNC_METHODS}
    assert sig["queue_change"] == (
        "(self, photo_id, change_type, value, workspace_id=None, _commit=True)"
    )
    assert sig["remove_pending_changes"] == (
        "(self, photo_id, change_type=None, value=None, workspace_id=None, "
        "_commit=True)"
    )
    assert sig["clear_pending"] == (
        "(self, change_ids, *, clear_equivalent_flat_removals=False, "
        "expected_tokens=None)"
    )
    assert sig["clear_pending_by_token"] == (
        "(self, change_tokens, *, clear_equivalent_flat_removals=False)"
    )
    assert sig["clear_equivalent_flat_removals"] == "(self, changes, _commit=True)"
    assert sig["queue_flag_change_if_enabled"] == (
        "(self, photo_id, flag, workspace_id=None, _commit=True)"
    )
    assert sig["get_pending_keyword_removal_keys"] == (
        "(self, photo_id, hierarchical=False)"
    )
