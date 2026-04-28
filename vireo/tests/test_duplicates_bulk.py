"""Tests for ``Database.bulk_resolve_by_folder``.

Backs the bulk-decide UI: when the user looks at a bucket of N duplicate
groups all sharing the same {folderA, folderB} parent-dir set, clicking
"Keep folderA for all N" resolves every group at once with the photo in
folderA forced as the winner.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from db import Database


def _add(db, folder_id, filename, file_hash=None, file_mtime=100.0, rating=0):
    pid = db.add_photo(
        folder_id=folder_id,
        filename=filename,
        extension=os.path.splitext(filename)[1] or ".jpg",
        file_size=1000,
        file_mtime=file_mtime,
        file_hash=file_hash,
    )
    if rating:
        db.conn.execute("UPDATE photos SET rating = ? WHERE id = ?", (rating, pid))
        db.conn.commit()
    return pid


def _touch(folder, filename):
    """Materialize ``filename`` inside ``folder`` so the existence check in
    ``bulk_resolve_by_folder`` doesn't trip. Tests that exercise the
    'keep_folder candidate missing on disk' branch deliberately skip this.
    """
    p = folder / filename
    p.write_bytes(b"x")
    return p


def _reset_flags(db, file_hash):
    db.conn.execute(
        "UPDATE photos SET flag = 'none' WHERE file_hash = ?", (file_hash,)
    )
    db.conn.commit()


def _flags(db, ids):
    return {
        r["id"]: r["flag"]
        for r in db.conn.execute(
            f"SELECT id, flag FROM photos WHERE id IN ({','.join('?' * len(ids))})",
            list(ids),
        ).fetchall()
    }


def test_bulk_resolve_single_hash_picks_photo_in_keep_folder(tmp_path):
    """For one hash with two candidates, the photo in keep_folder becomes
    the winner; the other gets flagged rejected."""
    db = Database(str(tmp_path / "t.db"))
    a_dir = tmp_path / "a"
    b_dir = tmp_path / "b"
    a_dir.mkdir()
    b_dir.mkdir()
    a_fid = db.add_folder(str(a_dir))
    b_fid = db.add_folder(str(b_dir))
    p_a = _add(db, a_fid, "owl.jpg", file_hash="HBR1")
    p_b = _add(db, b_fid, "owl.jpg", file_hash="HBR1")
    _touch(a_dir, "owl.jpg")
    _touch(b_dir, "owl.jpg")
    _reset_flags(db, "HBR1")  # make group unresolved

    result = db.bulk_resolve_by_folder(["HBR1"], str(b_dir))

    assert result["resolved"] == [
        {"file_hash": "HBR1", "winner_id": p_b, "loser_ids": [p_a]}
    ]
    assert result["skipped"] == []
    assert _flags(db, [p_a, p_b]) == {p_a: "rejected", p_b: "none"}


def test_bulk_resolve_multiple_hashes_share_keep_folder(tmp_path):
    """The whole point of bulk: 3 hashes, one call, all resolved by keeping
    the candidates in one folder."""
    db = Database(str(tmp_path / "t.db"))
    a_dir = tmp_path / "a"
    b_dir = tmp_path / "b"
    a_dir.mkdir()
    b_dir.mkdir()
    a_fid = db.add_folder(str(a_dir))
    b_fid = db.add_folder(str(b_dir))

    pairs = []
    for h, name in [("H1", "owl.jpg"), ("H2", "hawk.jpg"), ("H3", "finch.jpg")]:
        p_a = _add(db, a_fid, name, file_hash=h)
        p_b = _add(db, b_fid, name, file_hash=h)
        _touch(a_dir, name)
        _touch(b_dir, name)
        _reset_flags(db, h)
        pairs.append((h, p_a, p_b))

    result = db.bulk_resolve_by_folder(["H1", "H2", "H3"], str(a_dir))

    assert len(result["resolved"]) == 3
    assert result["skipped"] == []
    # Every /a photo wins; every /b photo loses.
    for h, p_a, p_b in pairs:
        flags = _flags(db, [p_a, p_b])
        assert flags == {p_a: "none", p_b: "rejected"}, f"hash {h}"


def test_bulk_resolve_skips_hash_with_no_photo_in_keep_folder(tmp_path):
    """A hash whose candidates all live outside keep_folder is skipped
    cleanly — the rest of the batch still resolves."""
    db = Database(str(tmp_path / "t.db"))
    a_dir = tmp_path / "a"
    b_dir = tmp_path / "b"
    c_dir = tmp_path / "c"
    a_dir.mkdir()
    b_dir.mkdir()
    c_dir.mkdir()
    a_fid = db.add_folder(str(a_dir))
    b_fid = db.add_folder(str(b_dir))
    c_fid = db.add_folder(str(c_dir))

    # H1 has files in /a and /b; H2 has files in /b and /c. Picking /a as
    # keep_folder resolves H1 but skips H2 (no candidate in /a).
    h1_a = _add(db, a_fid, "owl.jpg", file_hash="H1")
    h1_b = _add(db, b_fid, "owl.jpg", file_hash="H1")
    h2_b = _add(db, b_fid, "hawk.jpg", file_hash="H2")
    h2_c = _add(db, c_fid, "hawk.jpg", file_hash="H2")
    _touch(a_dir, "owl.jpg")
    _touch(b_dir, "owl.jpg")
    _touch(b_dir, "hawk.jpg")
    _touch(c_dir, "hawk.jpg")
    _reset_flags(db, "H1")
    _reset_flags(db, "H2")

    result = db.bulk_resolve_by_folder(["H1", "H2"], str(a_dir))

    assert len(result["resolved"]) == 1
    assert result["resolved"][0]["file_hash"] == "H1"
    assert result["resolved"][0]["winner_id"] == h1_a
    assert result["skipped"] == [
        {"file_hash": "H2", "reason": "no candidate in keep_folder"}
    ]
    # H1 resolved, H2 untouched.
    assert _flags(db, [h1_a, h1_b]) == {h1_a: "none", h1_b: "rejected"}
    assert _flags(db, [h2_b, h2_c]) == {h2_b: "none", h2_c: "none"}


def test_bulk_resolve_unknown_hash_skipped(tmp_path):
    """A hash with no DB rows at all is reported as skipped, not as a fatal
    error — the user might be acting on stale scan results."""
    db = Database(str(tmp_path / "t.db"))
    a_dir = tmp_path / "a"
    a_dir.mkdir()
    db.add_folder(str(a_dir))

    result = db.bulk_resolve_by_folder(["DOES_NOT_EXIST"], str(a_dir))

    assert result["resolved"] == []
    assert result["skipped"] == [
        {"file_hash": "DOES_NOT_EXIST", "reason": "no candidates"}
    ]


def test_bulk_resolve_singleton_hash_skipped(tmp_path):
    """A hash with only one non-rejected candidate (others already rejected
    by an earlier resolution) has nothing to resolve — skip."""
    db = Database(str(tmp_path / "t.db"))
    a_dir = tmp_path / "a"
    a_dir.mkdir()
    a_fid = db.add_folder(str(a_dir))
    # Single candidate.
    _add(db, a_fid, "owl.jpg", file_hash="HSOLO")
    _reset_flags(db, "HSOLO")

    result = db.bulk_resolve_by_folder(["HSOLO"], str(a_dir))

    assert result["resolved"] == []
    assert result["skipped"] == [
        {"file_hash": "HSOLO", "reason": "fewer than 2 candidates"}
    ]


def test_bulk_resolve_multiple_in_keep_folder_resolves_among_them(tmp_path):
    """Edge case: two candidates share a hash AND both are in keep_folder
    (e.g., owl.jpg and owl-2.jpg). The resolver picks one of them as
    winner; the other inside-folder copy AND the outside-folder copies
    all become losers."""
    db = Database(str(tmp_path / "t.db"))
    a_dir = tmp_path / "a"
    b_dir = tmp_path / "b"
    a_dir.mkdir()
    b_dir.mkdir()
    a_fid = db.add_folder(str(a_dir))
    b_fid = db.add_folder(str(b_dir))

    # Need files on disk so resolve_duplicates Rule 0 doesn't bias picks.
    (a_dir / "owl.jpg").write_bytes(b"x")
    (a_dir / "owl-2.jpg").write_bytes(b"x")
    (b_dir / "owl.jpg").write_bytes(b"x")
    p_clean = _add(db, a_fid, "owl.jpg", file_hash="HMULTI")
    p_dirty = _add(db, a_fid, "owl-2.jpg", file_hash="HMULTI")
    p_b = _add(db, b_fid, "owl.jpg", file_hash="HMULTI")
    _reset_flags(db, "HMULTI")

    result = db.bulk_resolve_by_folder(["HMULTI"], str(a_dir))

    assert len(result["resolved"]) == 1
    res = result["resolved"][0]
    # Rule 1 (clean filename) picks p_clean over p_dirty within /a.
    assert res["winner_id"] == p_clean
    assert sorted(res["loser_ids"]) == sorted([p_dirty, p_b])
    flags = _flags(db, [p_clean, p_dirty, p_b])
    assert flags == {p_clean: "none", p_dirty: "rejected", p_b: "rejected"}


def test_bulk_resolve_merges_metadata_onto_forced_winner(tmp_path):
    """Forced winner inherits the max rating from losers, same as the normal
    apply_duplicate_resolution path."""
    db = Database(str(tmp_path / "t.db"))
    a_dir = tmp_path / "a"
    b_dir = tmp_path / "b"
    a_dir.mkdir()
    b_dir.mkdir()
    a_fid = db.add_folder(str(a_dir))
    b_fid = db.add_folder(str(b_dir))
    # Winner has rating=2; loser has rating=5. Bulk-resolve should merge to 5.
    p_a = _add(db, a_fid, "owl.jpg", file_hash="HMERGE", rating=2)
    p_b = _add(db, b_fid, "owl.jpg", file_hash="HMERGE", rating=5)
    _touch(a_dir, "owl.jpg")
    _touch(b_dir, "owl.jpg")
    _reset_flags(db, "HMERGE")

    db.bulk_resolve_by_folder(["HMERGE"], str(a_dir))

    row = db.conn.execute(
        "SELECT rating FROM photos WHERE id = ?", (p_a,)
    ).fetchone()
    assert row["rating"] == 5


def test_bulk_resolve_skips_when_keep_folder_candidate_missing_on_disk(tmp_path):
    """If the keep_folder row points at a file that's been deleted externally,
    skip the hash rather than promoting a missing winner. Force-rejecting the
    surviving sibling in another folder, followed by the chained
    delete-loser-files, would otherwise trash the only remaining copy."""
    db = Database(str(tmp_path / "t.db"))
    a_dir = tmp_path / "a"
    b_dir = tmp_path / "b"
    a_dir.mkdir()
    b_dir.mkdir()
    a_fid = db.add_folder(str(a_dir))
    b_fid = db.add_folder(str(b_dir))
    p_a = _add(db, a_fid, "owl.jpg", file_hash="HGONE")
    p_b = _add(db, b_fid, "owl.jpg", file_hash="HGONE")
    # Only the /b copy exists on disk; the /a row is stale.
    _touch(b_dir, "owl.jpg")
    _reset_flags(db, "HGONE")

    result = db.bulk_resolve_by_folder(["HGONE"], str(a_dir))

    assert result["resolved"] == []
    assert result["skipped"] == [
        {"file_hash": "HGONE", "reason": "keep_folder candidate missing on disk"}
    ]
    # Both rows untouched — the surviving /b file MUST remain selectable.
    assert _flags(db, [p_a, p_b]) == {p_a: "none", p_b: "none"}


def test_bulk_resolve_normalizes_keep_folder_trailing_slash(tmp_path):
    """The bucket UI passes folder paths derived from ``os.path.dirname(...)``
    — never trailing-slashed. But ``folders.path`` rows in the DB can carry
    a trailing separator (manual relocation, legacy imports). A naive
    ``folder_path == keep_folder`` comparison would silently no-op the
    bulk action for affected users; the lookup must normalize."""
    db = Database(str(tmp_path / "t.db"))
    a_dir = tmp_path / "a"
    b_dir = tmp_path / "b"
    a_dir.mkdir()
    b_dir.mkdir()
    # Folder stored WITH a trailing slash (simulates legacy/relocated rows).
    a_fid = db.add_folder(str(a_dir) + "/")
    b_fid = db.add_folder(str(b_dir))
    p_a = _add(db, a_fid, "owl.jpg", file_hash="HSLASH")
    p_b = _add(db, b_fid, "owl.jpg", file_hash="HSLASH")
    _touch(a_dir, "owl.jpg")
    _touch(b_dir, "owl.jpg")
    _reset_flags(db, "HSLASH")

    # Caller passes the un-slashed form — the form the bucket UI derives.
    result = db.bulk_resolve_by_folder(["HSLASH"], str(a_dir))

    assert result["skipped"] == []
    assert result["resolved"] == [
        {"file_hash": "HSLASH", "winner_id": p_a, "loser_ids": [p_b]}
    ]
    assert _flags(db, [p_a, p_b]) == {p_a: "none", p_b: "rejected"}


def test_bulk_resolve_skips_when_all_keep_folder_candidates_missing(tmp_path):
    """Same protection when there are multiple stale rows in keep_folder:
    don't fall through to the resolver and pick a missing winner."""
    db = Database(str(tmp_path / "t.db"))
    a_dir = tmp_path / "a"
    b_dir = tmp_path / "b"
    a_dir.mkdir()
    b_dir.mkdir()
    a_fid = db.add_folder(str(a_dir))
    b_fid = db.add_folder(str(b_dir))
    p_a1 = _add(db, a_fid, "owl.jpg", file_hash="HALLGONE")
    p_a2 = _add(db, a_fid, "owl-2.jpg", file_hash="HALLGONE")
    p_b = _add(db, b_fid, "owl.jpg", file_hash="HALLGONE")
    # Neither /a copy on disk; only /b survives.
    _touch(b_dir, "owl.jpg")
    _reset_flags(db, "HALLGONE")

    result = db.bulk_resolve_by_folder(["HALLGONE"], str(a_dir))

    assert result["resolved"] == []
    assert result["skipped"] == [
        {"file_hash": "HALLGONE", "reason": "keep_folder candidate missing on disk"}
    ]
    assert _flags(db, [p_a1, p_a2, p_b]) == {
        p_a1: "none", p_a2: "none", p_b: "none",
    }
