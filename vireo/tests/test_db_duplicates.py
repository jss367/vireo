"""Behavior pins for the exact-duplicate domain of ``Database``.

The behavior tests exercise the duplicate methods only through the public
``Database`` façade, so they hold whether the SQL lives in ``db.py`` or in
``repositories/duplicates.py``; the structural tests at the end keep it in
the repository. They cover the add-photo auto-resolve hook, group listing,
resolver-driven and folder-driven resolution, the winner/loser merge, and
reopening a resolved group.

The pure resolver (``vireo/duplicates.py``) has its own tests; these only pin
how the database feeds it and applies its verdict.
"""

import ast
import inspect
import logging
import os
import sqlite3
import textwrap

import pytest
from db import Database


def _photo(db, folder_id, filename, file_hash=None, *, file_mtime=100.0,
           rating=0, flag="none"):
    """Insert a photo row directly, bypassing add_photo's auto-resolve hook."""
    cur = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_mtime, file_hash, rating, flag) VALUES (?, ?, ?, 1, ?, ?, ?, ?)",
        (folder_id, filename, os.path.splitext(filename)[1] or ".jpg",
         file_mtime, file_hash, rating, flag),
    )
    db.conn.commit()
    return cur.lastrowid


def _touch(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as fh:
        fh.write(b"x")
    return path


def _reader(db):
    """A second connection: sees only what the façade has committed."""
    conn = sqlite3.connect(db._db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _flags(db, ids):
    reader = _reader(db)
    try:
        placeholders = ",".join("?" * len(ids))
        return {
            r["id"]: r["flag"]
            for r in reader.execute(
                f"SELECT id, flag FROM photos WHERE id IN ({placeholders})",
                list(ids),
            )
        }
    finally:
        reader.close()


def _committed_rating(db, photo_id):
    reader = _reader(db)
    try:
        return reader.execute(
            "SELECT rating FROM photos WHERE id = ?", (photo_id,)
        ).fetchone()["rating"]
    finally:
        reader.close()


def _keyword_sources(db, photo_id):
    return {
        r["keyword_id"]: r["source"]
        for r in db.conn.execute(
            "SELECT keyword_id, source FROM photo_keywords WHERE photo_id = ?",
            (photo_id,),
        )
    }


class _SqlRecorder:
    """Record every statement the connection runs (via set_trace_callback)."""

    def __init__(self, db):
        self.statements = []
        self._db = db

    def __enter__(self):
        self._db.conn.set_trace_callback(self.statements.append)
        return self

    def __exit__(self, *exc):
        self._db.conn.set_trace_callback(None)

    def matching(self, needle):
        return [s for s in self.statements if needle in s]


@pytest.fixture
def folder(db, tmp_path):
    path = str(tmp_path / "photos")
    os.makedirs(path, exist_ok=True)
    return db.add_folder(path), path


# -- check_and_resolve_duplicates_for_hash ------------------------------------


@pytest.mark.parametrize("empty", [None, ""])
def test_check_and_resolve_ignores_empty_hash(db, monkeypatch, empty):
    calls = []
    monkeypatch.setattr(db, "apply_duplicate_resolution", calls.append)
    # With no active workspace nothing workspace-scoped may be touched either.
    db.set_active_workspace(None)
    with _SqlRecorder(db) as rec:
        assert db.check_and_resolve_duplicates_for_hash(empty) is None
    assert rec.statements == []
    assert calls == []


def test_check_and_resolve_returns_none_below_two_live_rows(db, folder, monkeypatch):
    fid, _ = folder
    _photo(db, fid, "a.jpg", "H")
    _photo(db, fid, "b.jpg", "H", flag="rejected")
    calls = []
    monkeypatch.setattr(db, "apply_duplicate_resolution", calls.append)

    assert db.check_and_resolve_duplicates_for_hash("H") is None
    assert db.check_and_resolve_duplicates_for_hash("unknown") is None
    assert calls == []


def test_check_and_resolve_routes_live_ids_through_the_facade(db, folder, monkeypatch):
    fid, _ = folder
    a = _photo(db, fid, "a.jpg", "H")
    _photo(db, fid, "r.jpg", "H", flag="rejected")
    b = _photo(db, fid, "b.jpg", "H")
    c = _photo(db, fid, "c.jpg", "H", flag=None)  # NULL flag is live
    _photo(db, fid, "other.jpg", "OTHER")
    seen = []

    def _resolve(ids):
        seen.append(ids)
        return {"sentinel": True}

    monkeypatch.setattr(db, "apply_duplicate_resolution", _resolve)

    assert db.check_and_resolve_duplicates_for_hash("H") == {"sentinel": True}
    assert len(seen) == 1
    assert isinstance(seen[0], list)
    assert sorted(seen[0]) == sorted([a, b, c])


def test_check_and_resolve_runs_the_real_resolution(db, folder):
    fid, path = folder
    a = _photo(db, fid, "owl.jpg", "H")
    b = _photo(db, fid, "owl (2).jpg", "H")
    _touch(os.path.join(path, "owl.jpg"))
    _touch(os.path.join(path, "owl (2).jpg"))

    result = db.check_and_resolve_duplicates_for_hash("H")

    assert result == {"winner_id": a, "loser_ids": [b], "rejected": 1}
    assert _flags(db, [a, b]) == {a: "none", b: "rejected"}


def test_check_and_resolve_swallows_a_failing_lookup(db, folder, caplog):
    fid, _ = folder
    _photo(db, fid, "a.jpg", "H")
    _photo(db, fid, "b.jpg", "H")

    def _deny_photos(action, table, *_):
        if action == sqlite3.SQLITE_READ and table == "photos":
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    db.conn.set_authorizer(_deny_photos)
    try:
        with caplog.at_level(logging.WARNING, logger="db"):
            assert db.check_and_resolve_duplicates_for_hash("H") is None
    finally:
        db.conn.set_authorizer(None)

    assert "Duplicate auto-resolve failed for hash H" in caplog.text


def test_check_and_resolve_swallows_a_failing_resolution(db, folder, monkeypatch, caplog):
    fid, _ = folder
    _photo(db, fid, "a.jpg", "H")
    _photo(db, fid, "b.jpg", "H")

    def _boom(ids):
        raise sqlite3.OperationalError("synthetic")

    monkeypatch.setattr(db, "apply_duplicate_resolution", _boom)
    with caplog.at_level(logging.WARNING, logger="db"):
        assert db.check_and_resolve_duplicates_for_hash("H") is None
    assert "Duplicate auto-resolve failed for hash H: synthetic" in caplog.text


def test_check_and_resolve_propagates_non_sqlite_errors(db, folder, monkeypatch):
    fid, _ = folder
    _photo(db, fid, "a.jpg", "H")
    _photo(db, fid, "b.jpg", "H")

    def _boom(ids):
        raise ValueError("not a sqlite error")

    monkeypatch.setattr(db, "apply_duplicate_resolution", _boom)
    with pytest.raises(ValueError, match="not a sqlite error"):
        db.check_and_resolve_duplicates_for_hash("H")


def test_add_photo_with_hash_runs_the_hook(db, folder, monkeypatch):
    fid, _ = folder
    seen = []
    monkeypatch.setattr(
        db, "check_and_resolve_duplicates_for_hash", seen.append,
    )
    db.add_photo(folder_id=fid, filename="a.jpg", extension=".jpg",
                 file_size=1, file_mtime=1.0, file_hash="HOOK")
    db.add_photo(folder_id=fid, filename="b.jpg", extension=".jpg",
                 file_size=1, file_mtime=1.0)
    assert seen == ["HOOK"]


# -- find_duplicate_groups ------------------------------------------------------


def _normalized(groups):
    return [
        {**g, "photo_ids": sorted(g["photo_ids"])} for g in groups
    ]


def test_find_duplicate_groups_returns_unresolved_only_by_default(db, folder):
    fid, _ = folder
    u1 = _photo(db, fid, "u1.jpg", "UNRES")
    u2 = _photo(db, fid, "u2.jpg", "UNRES", flag=None)
    u3 = _photo(db, fid, "u3.jpg", "UNRES", flag="rejected")
    _photo(db, fid, "k.jpg", "RES")
    _photo(db, fid, "l.jpg", "RES", flag="rejected")
    _photo(db, fid, "solo.jpg", "SOLO")
    _photo(db, fid, "n1.jpg", None)
    _photo(db, fid, "n2.jpg", None)

    groups = db.find_duplicate_groups()

    assert _normalized(groups) == [
        {"file_hash": "UNRES", "photo_ids": sorted([u1, u2]), "status": "unresolved"},
    ]
    assert u3 not in groups[0]["photo_ids"]
    assert all(isinstance(i, int) for i in groups[0]["photo_ids"])
    assert db.find_duplicate_groups(include_resolved=False) == groups


def test_find_duplicate_groups_include_resolved(db, folder):
    fid, _ = folder
    b1 = _photo(db, fid, "b1.jpg", "B")
    b2 = _photo(db, fid, "b2.jpg", "B")
    k = _photo(db, fid, "k.jpg", "A_RES")
    l1 = _photo(db, fid, "l1.jpg", "A_RES", flag="rejected")
    l2 = _photo(db, fid, "l2.jpg", "A_RES", flag="rejected")
    # Only-rejected hash: no kept anchor, so not a resolved group.
    _photo(db, fid, "r1.jpg", "ALLREJ", flag="rejected")
    _photo(db, fid, "r2.jpg", "ALLREJ", flag="rejected")
    # Two kept + one rejected is unresolved, not resolved.
    m1 = _photo(db, fid, "m1.jpg", "MIXED")
    m2 = _photo(db, fid, "m2.jpg", "MIXED")
    _photo(db, fid, "m3.jpg", "MIXED", flag="rejected")
    # NULL-hash rows never group, even with a rejected NULL sibling.
    _photo(db, fid, "n1.jpg", None)
    _photo(db, fid, "n2.jpg", None, flag="rejected")

    groups = db.find_duplicate_groups(include_resolved=True)

    unresolved = [g for g in groups if g["status"] == "unresolved"]
    resolved = [g for g in groups if g["status"] == "resolved"]
    # Unresolved groups come first, then resolved ones.
    assert groups == unresolved + resolved
    assert sorted(_normalized(unresolved), key=lambda g: g["file_hash"]) == [
        {"file_hash": "B", "photo_ids": sorted([b1, b2]), "status": "unresolved"},
        {"file_hash": "MIXED", "photo_ids": sorted([m1, m2]), "status": "unresolved"},
    ]
    assert _normalized(resolved) == [
        {"file_hash": "A_RES", "photo_ids": sorted([k, l1, l2]), "status": "resolved"},
    ]


def test_find_duplicate_groups_empty_catalog(db):
    assert db.find_duplicate_groups() == []
    assert db.find_duplicate_groups(include_resolved=True) == []


# -- apply_duplicate_resolution -------------------------------------------------


_NOOP = {"winner_id": None, "loser_ids": [], "rejected": 0}


@pytest.mark.parametrize("ids", [None, [], [1]])
def test_apply_resolution_short_input_is_a_noop_without_sql(db, ids):
    with _SqlRecorder(db) as rec:
        assert db.apply_duplicate_resolution(ids) == _NOOP
    assert rec.statements == []


def test_apply_resolution_dedupes_ids_before_counting(db, folder, monkeypatch):
    fid, _ = folder
    a = _photo(db, fid, "a.jpg", "H")
    merges = []
    monkeypatch.setattr(
        db, "_apply_winner_loser_merge", lambda w, l: merges.append((w, l)),
    )
    assert db.apply_duplicate_resolution([a, a]) == _NOOP
    assert merges == []


def test_apply_resolution_ignores_rejected_and_unknown_ids(db, folder, monkeypatch):
    fid, _ = folder
    a = _photo(db, fid, "a.jpg", "H")
    r = _photo(db, fid, "r.jpg", "H", flag="rejected")
    merges = []
    monkeypatch.setattr(
        db, "_apply_winner_loser_merge", lambda w, l: merges.append((w, l)),
    )
    assert db.apply_duplicate_resolution([a, r, 999999]) == _NOOP
    assert merges == []
    assert _flags(db, [r]) == {r: "rejected"}


def test_apply_resolution_prefers_the_file_that_exists(db, folder):
    fid, path = folder
    # Shorter, cleaner name would win on every other rule, but it's missing.
    missing = _photo(db, fid, "a.jpg", "H", file_mtime=1.0)
    present = _photo(db, fid, "a_longer_name (2).jpg", "H", file_mtime=9.0)
    _touch(os.path.join(path, "a_longer_name (2).jpg"))

    result = db.apply_duplicate_resolution([missing, present])

    assert result == {"winner_id": present, "loser_ids": [missing], "rejected": 1}
    assert _flags(db, [missing, present]) == {missing: "rejected", present: "none"}


def test_apply_resolution_handles_rows_without_folder_or_mtime(db, folder):
    fid, _ = folder
    a = _photo(db, None, "a.jpg", "H")
    b = _photo(db, fid, "b.jpg", "H")
    db.conn.execute("UPDATE photos SET file_mtime = NULL WHERE id IN (?, ?)", (a, b))
    db.conn.commit()

    result = db.apply_duplicate_resolution([a, b])

    # Both missing on disk; "a.jpg" (no folder) is the shorter path.
    assert result == {"winner_id": a, "loser_ids": [b], "rejected": 1}


def test_apply_resolution_routes_the_merge_through_the_facade(db, folder, monkeypatch):
    fid, path = folder
    a = _photo(db, fid, "owl.jpg", "H")
    b = _photo(db, fid, "owl (2).jpg", "H")
    c = _photo(db, fid, "owl (3).jpg", "H")
    merges = []
    monkeypatch.setattr(
        db, "_apply_winner_loser_merge", lambda w, l: merges.append((w, list(l))),
    )

    result = db.apply_duplicate_resolution([c, b, a])

    assert result["winner_id"] == a
    assert sorted(result["loser_ids"]) == sorted([b, c])
    assert result["rejected"] == 2
    assert merges == [(a, result["loser_ids"])]
    # The merge was stubbed, so nothing was rejected.
    assert set(_flags(db, [a, b, c]).values()) == {"none"}


def test_apply_resolution_chunks_its_candidate_lookup(db, folder, monkeypatch):
    fid, _ = folder
    ids = []
    for i in range(801):
        cur = db.conn.execute(
            "INSERT INTO photos (folder_id, filename, extension, file_size,"
            " file_mtime, file_hash) VALUES (?, ?, '.jpg', 1, 1.0, 'BIG')",
            (fid, f"p{i:04d}.jpg"),
        )
        ids.append(cur.lastrowid)
    db.conn.commit()
    merges = []
    monkeypatch.setattr(
        db, "_apply_winner_loser_merge", lambda w, l: merges.append((w, list(l))),
    )

    with _SqlRecorder(db) as rec:
        result = db.apply_duplicate_resolution(ids)

    lookups = rec.matching("LEFT JOIN folders f ON f.id = p.folder_id")
    assert len(lookups) == 2  # 800 + 1
    assert result["winner_id"] == ids[0]
    assert sorted(result["loser_ids"]) == ids[1:]
    assert merges == [(ids[0], result["loser_ids"])]


# -- _apply_winner_loser_merge --------------------------------------------------


def test_merge_raises_rating_rejects_losers_and_commits(db, folder):
    fid, _ = folder
    w = _photo(db, fid, "w.jpg", "H", rating=2)
    l1 = _photo(db, fid, "l1.jpg", "H", rating=5)
    l2 = _photo(db, fid, "l2.jpg", "H", rating=None)

    db._apply_winner_loser_merge(w, [l1, l2])

    assert not db.conn.in_transaction
    assert _committed_rating(db, w) == 5
    assert _flags(db, [w, l1, l2]) == {w: "none", l1: "rejected", l2: "rejected"}


def test_merge_leaves_a_higher_winner_rating_alone(db, folder):
    fid, _ = folder
    w = _photo(db, fid, "w.jpg", "H", rating=4)
    lo = _photo(db, fid, "l.jpg", "H", rating=1)

    with _SqlRecorder(db) as rec:
        db._apply_winner_loser_merge(w, [lo])

    assert rec.matching("UPDATE photos SET rating") == []
    assert _committed_rating(db, w) == 4
    assert _flags(db, [lo]) == {lo: "rejected"}


def test_merge_treats_null_winner_rating_as_zero(db, folder):
    fid, _ = folder
    w = _photo(db, fid, "w.jpg", "H", rating=None)
    lo = _photo(db, fid, "l.jpg", "H", rating=0)

    with _SqlRecorder(db) as rec:
        db._apply_winner_loser_merge(w, [lo])

    # new_rating (0) equals the winner's coerced rating (0): no UPDATE,
    # so the NULL survives.
    assert rec.matching("UPDATE photos SET rating") == []
    assert _committed_rating(db, w) is None


def test_merge_carries_the_strongest_loser_source_per_keyword(db, folder):
    fid, _ = folder
    w = _photo(db, fid, "w.jpg", "H")
    l1 = _photo(db, fid, "l1.jpg", "H")
    l2 = _photo(db, fid, "l2.jpg", "H")
    k_manual = db.add_keyword("Manual Tag")
    k_accept = db.add_keyword("Accepted Tag")
    k_unknown = db.add_keyword("Unknown Tag")
    k_shared = db.add_keyword("Winner Tag")
    db.tag_photo(l1, k_manual, source=None)
    db.tag_photo(l2, k_manual, source="manual")
    db.tag_photo(l1, k_accept, source="accept")
    db.tag_photo(l2, k_unknown, source=None)
    db.tag_photo(w, k_shared, source=None)
    db.tag_photo(l1, k_shared, source="manual")

    db._apply_winner_loser_merge(w, [l1, l2])

    assert _keyword_sources(db, w) == {
        k_manual: "manual",
        k_accept: "accept",
        k_unknown: None,
        # A manual loser upgrades the winner's weaker existing association.
        k_shared: "manual",
    }


def test_merge_tags_through_the_facade_inside_its_transaction(db, folder, monkeypatch):
    fid, _ = folder
    # The rating bump opens the merge transaction before tagging starts.
    w = _photo(db, fid, "w.jpg", "H", rating=1)
    lo = _photo(db, fid, "l.jpg", "H", rating=3)
    kid = db.add_keyword("Heron")
    db.tag_photo(lo, kid, source="accept")
    calls = []

    def _record(photo_id, keyword_id, source="manual", _commit=True):
        calls.append((photo_id, keyword_id, source, _commit, db.conn.in_transaction))

    monkeypatch.setattr(db, "tag_photo", _record)

    db._apply_winner_loser_merge(w, [lo])

    assert calls == [(w, kid, "accept", False, True)]
    assert not db.conn.in_transaction
    assert _committed_rating(db, w) == 3


def test_merge_rolls_back_everything_when_tagging_fails(db, folder, monkeypatch):
    fid, _ = folder
    w = _photo(db, fid, "w.jpg", "H", rating=1)
    lo = _photo(db, fid, "l.jpg", "H", rating=5)
    kid = db.add_keyword("Egret")
    db.tag_photo(lo, kid)

    def _boom(*args, **kwargs):
        raise RuntimeError("tag failed")

    monkeypatch.setattr(db, "tag_photo", _boom)

    with pytest.raises(RuntimeError, match="tag failed"):
        db._apply_winner_loser_merge(w, [lo])

    assert not db.conn.in_transaction
    assert _committed_rating(db, w) == 1
    assert _flags(db, [lo]) == {lo: "none"}


def test_merge_logs_the_verdict(db, folder, caplog):
    fid, _ = folder
    w = _photo(db, fid, "w.jpg", "H")
    lo = _photo(db, fid, "l.jpg", "H")

    with caplog.at_level(logging.INFO, logger="db"):
        db._apply_winner_loser_merge(w, [lo])

    assert f"Duplicate resolved: kept id={w}, rejected id(s)=[{lo}]" in caplog.text


def test_merge_reads_pending_changes_without_copying_them(db, folder):
    fid, _ = folder
    w = _photo(db, fid, "w.jpg", "H")
    lo = _photo(db, fid, "l.jpg", "H")
    db.conn.execute(
        "INSERT INTO pending_changes (photo_id, change_type, value, workspace_id)"
        " VALUES (?, 'rating', '3', ?)",
        (lo, db._ws_id()),
    )
    db.conn.commit()

    db._apply_winner_loser_merge(w, [lo])

    rows = db.conn.execute(
        "SELECT photo_id FROM pending_changes ORDER BY id"
    ).fetchall()
    assert [r["photo_id"] for r in rows] == [lo]
    assert _flags(db, [lo]) == {lo: "rejected"}


def test_merge_with_no_losers_only_logs(db, folder):
    fid, _ = folder
    w = _photo(db, fid, "w.jpg", "H", rating=3)

    db._apply_winner_loser_merge(w, [])

    assert _committed_rating(db, w) == 3
    assert _flags(db, [w]) == {w: "none"}


def test_merge_chunks_loser_reads_and_rejections(db, folder):
    fid, _ = folder
    w = _photo(db, fid, "w.jpg", "BIG")
    losers = []
    for i in range(801):
        cur = db.conn.execute(
            "INSERT INTO photos (folder_id, filename, extension, file_size,"
            " file_mtime, file_hash) VALUES (?, ?, '.jpg', 1, 1.0, 'BIG')",
            (fid, f"l{i:04d}.jpg"),
        )
        losers.append(cur.lastrowid)
    db.conn.commit()
    kid = db.add_keyword("Last Loser Tag")
    db.tag_photo(losers[-1], kid, source="accept")

    with _SqlRecorder(db) as rec:
        db._apply_winner_loser_merge(w, losers)

    assert len(rec.matching("WHERE photo_id IN (")) == 2
    assert len(rec.matching("UPDATE photos SET flag = 'rejected' WHERE id IN (")) == 2
    rejected = db.conn.execute(
        "SELECT COUNT(*) FROM photos WHERE flag = 'rejected'"
    ).fetchone()[0]
    assert rejected == 801
    assert _keyword_sources(db, w) == {kid: "accept"}


# -- bulk_resolve_by_folder -----------------------------------------------------


def test_bulk_resolve_reports_every_skip_reason_in_order(db, tmp_path):
    keep = str(tmp_path / "keep")
    other = str(tmp_path / "other")
    fk = db.add_folder(keep)
    fo = db.add_folder(other)
    # "fewer than 2 candidates": one live row plus a rejected twin.
    _photo(db, fk, "solo.jpg", "SOLO")
    _photo(db, fo, "solo.jpg", "SOLO", flag="rejected")
    # "no candidate in keep_folder"
    _photo(db, fo, "x.jpg", "ELSEWHERE")
    _photo(db, fo, "x (2).jpg", "ELSEWHERE")
    # "keep_folder candidate missing on disk"
    _photo(db, fk, "gone.jpg", "GONE")
    _photo(db, fo, "gone.jpg", "GONE")
    _touch(os.path.join(other, "gone.jpg"))

    result = db.bulk_resolve_by_folder(
        ["NOPE", "SOLO", "ELSEWHERE", "GONE"], keep,
    )

    assert result == {
        "resolved": [],
        "skipped": [
            {"file_hash": "NOPE", "reason": "no candidates"},
            {"file_hash": "SOLO", "reason": "fewer than 2 candidates"},
            {"file_hash": "ELSEWHERE", "reason": "no candidate in keep_folder"},
            {"file_hash": "GONE", "reason": "keep_folder candidate missing on disk"},
        ],
    }
    assert db.conn.execute(
        "SELECT COUNT(*) FROM photos WHERE flag = 'rejected'"
    ).fetchone()[0] == 1


def test_bulk_resolve_keeps_the_folder_copy_and_merges(db, tmp_path):
    keep = str(tmp_path / "keep")
    other = str(tmp_path / "other")
    fk = db.add_folder(keep)
    fo = db.add_folder(other)
    # The resolver alone would pick the shorter "other" path; keep_folder wins.
    w = _photo(db, fk, "long_keep_name.jpg", "H1", rating=1)
    lo = _photo(db, fo, "a.jpg", "H1", rating=4)
    _touch(os.path.join(keep, "long_keep_name.jpg"))
    kid = db.add_keyword("Plover")
    db.tag_photo(lo, kid, source="manual")
    w2 = _photo(db, fk, "b.jpg", "H2")
    l2 = _photo(db, fo, "b.jpg", "H2")
    _touch(os.path.join(keep, "b.jpg"))

    # Trailing separator on the choice still matches.
    result = db.bulk_resolve_by_folder(["H1", "H2"], keep + os.sep)

    assert result == {
        "resolved": [
            {"file_hash": "H1", "winner_id": w, "loser_ids": [lo]},
            {"file_hash": "H2", "winner_id": w2, "loser_ids": [l2]},
        ],
        "skipped": [],
    }
    assert not db.conn.in_transaction
    assert _flags(db, [w, lo, w2, l2]) == {
        w: "none", lo: "rejected", w2: "none", l2: "rejected",
    }
    assert _committed_rating(db, w) == 4
    assert _keyword_sources(db, w) == {kid: "manual"}


def test_bulk_resolve_matches_a_trailing_slash_folder_row(db, tmp_path):
    keep = str(tmp_path / "keep")
    fk = db.add_folder(keep)
    db.conn.execute("UPDATE folders SET path = ? WHERE id = ?", (keep + "/", fk))
    db.conn.commit()
    fo = db.add_folder(str(tmp_path / "other"))
    w = _photo(db, fk, "a.jpg", "H")
    lo = _photo(db, fo, "a.jpg", "H")
    _touch(os.path.join(keep, "a.jpg"))

    result = db.bulk_resolve_by_folder(["H"], keep)

    assert result["resolved"] == [{"file_hash": "H", "winner_id": w, "loser_ids": [lo]}]


def test_bulk_resolve_uses_the_resolver_among_same_folder_copies(db, tmp_path):
    keep = str(tmp_path / "keep")
    fk = db.add_folder(keep)
    fo = db.add_folder(str(tmp_path / "other"))
    dirty = _photo(db, fk, "owl (2).jpg", "H")
    clean = _photo(db, fk, "owl.jpg", "H")
    missing = _photo(db, fk, "o.jpg", "H")  # shortest, but not on disk
    elsewhere = _photo(db, fo, "owl.jpg", "H")
    _touch(os.path.join(keep, "owl (2).jpg"))
    _touch(os.path.join(keep, "owl.jpg"))

    result = db.bulk_resolve_by_folder(["H"], keep)

    assert result["resolved"] == [{
        "file_hash": "H",
        "winner_id": clean,
        "loser_ids": [r for r in (dirty, missing, elsewhere)],
    }]
    assert _flags(db, [dirty, clean, missing, elsewhere]) == {
        dirty: "rejected", clean: "none", missing: "rejected", elsewhere: "rejected",
    }


@pytest.mark.parametrize("keep_folder", [None, ""])
def test_bulk_resolve_empty_keep_folder_matches_nothing(db, tmp_path, keep_folder):
    # A folderless row normalizes to "." and never equals the empty choice.
    _photo(db, None, "a.jpg", "H")
    _photo(db, None, "b.jpg", "H")

    result = db.bulk_resolve_by_folder(["H"], keep_folder)

    assert result == {
        "resolved": [],
        "skipped": [{"file_hash": "H", "reason": "no candidate in keep_folder"}],
    }


def test_bulk_resolve_routes_each_merge_through_the_facade(db, tmp_path, monkeypatch):
    keep = str(tmp_path / "keep")
    fk = db.add_folder(keep)
    fo = db.add_folder(str(tmp_path / "other"))
    w1 = _photo(db, fk, "a.jpg", "H1")
    l1 = _photo(db, fo, "a.jpg", "H1")
    w2 = _photo(db, fk, "b.jpg", "H2")
    l2 = _photo(db, fo, "b.jpg", "H2")
    _touch(os.path.join(keep, "a.jpg"))
    _touch(os.path.join(keep, "b.jpg"))
    merges = []
    monkeypatch.setattr(
        db, "_apply_winner_loser_merge", lambda w, l: merges.append((w, l)),
    )

    result = db.bulk_resolve_by_folder(["H1", "H2"], keep)

    assert merges == [(w1, [l1]), (w2, [l2])]
    assert [r["winner_id"] for r in result["resolved"]] == [w1, w2]


def test_bulk_resolve_empty_batch(db):
    assert db.bulk_resolve_by_folder([], "/anywhere") == {"resolved": [], "skipped": []}


# -- reopen_duplicate_group -----------------------------------------------------


def test_reopen_unrejects_only_that_hash_and_commits(db, folder):
    fid, _ = folder
    k = _photo(db, fid, "k.jpg", "H")
    r1 = _photo(db, fid, "r1.jpg", "H", flag="rejected")
    r2 = _photo(db, fid, "r2.jpg", "H", flag="rejected")
    p = _photo(db, fid, "p.jpg", "H", flag="pick")
    other = _photo(db, fid, "o.jpg", "OTHER", flag="rejected")

    assert db.reopen_duplicate_group("H") == 2

    assert not db.conn.in_transaction
    assert _flags(db, [k, r1, r2, p, other]) == {
        k: "none", r1: "none", r2: "none", p: "pick", other: "rejected",
    }


def test_reopen_returns_zero_when_nothing_is_rejected(db, folder):
    fid, _ = folder
    _photo(db, fid, "k.jpg", "H")
    assert db.reopen_duplicate_group("H") == 0
    assert db.reopen_duplicate_group("UNKNOWN") == 0
    assert not db.conn.in_transaction


# -- structure: the SQL lives in DuplicatesRepository ---------------------------
#
# ``_apply_winner_loser_merge`` keeps its provenance fold (keyword_source_max)
# and its ``self.tag_photo`` calls on the façade: test_keyword_provenance_contract
# pins that writer to ("db.py", "_apply_winner_loser_merge"), and patches of
# ``Database.tag_photo`` must still reach the merge.


DUPLICATE_METHODS = [
    "check_and_resolve_duplicates_for_hash",
    "find_duplicate_groups",
    "apply_duplicate_resolution",
    "_apply_winner_loser_merge",
    "bulk_resolve_by_folder",
    "reopen_duplicate_group",
]


@pytest.mark.parametrize("name", DUPLICATE_METHODS)
def test_duplicate_method_delegates_to_repository(name):
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
        f"Database.{name} touches self.conn; move the SQL to DuplicatesRepository"
    )
    assert "_duplicates_repository" in attrs, (
        f"Database.{name} no longer delegates to DuplicatesRepository"
    )


def test_facade_signatures_are_unchanged():
    expected = {
        "check_and_resolve_duplicates_for_hash": "(self, file_hash: str) -> dict | None",
        "find_duplicate_groups": "(self, include_resolved=False)",
        "apply_duplicate_resolution": "(self, photo_ids)",
        "_apply_winner_loser_merge": "(self, winner_id, loser_ids)",
        "bulk_resolve_by_folder": "(self, file_hashes, keep_folder)",
        "reopen_duplicate_group": "(self, file_hash)",
    }
    for name, sig in expected.items():
        assert str(inspect.signature(getattr(Database, name))) == sig, name
