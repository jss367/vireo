"""DB integration tests for duplicate resolution (Tasks 7 & 8)."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from db import Database


def _add(db, folder_id, filename, file_hash=None, file_mtime=100.0, rating=0):
    """Helper: add_photo + optionally set file_hash + rating in-place."""
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


def _reset_flags(db, file_hash):
    """Clear auto-rejection so tests can exercise apply_duplicate_resolution directly."""
    db.conn.execute(
        "UPDATE photos SET flag = 'none' WHERE file_hash = ?", (file_hash,)
    )
    db.conn.commit()


# -----------------------------------------------------------------------------
# Task 7: apply_duplicate_resolution
# -----------------------------------------------------------------------------

def test_apply_resolution_rejects_loser_and_merges_rating(tmp_path):
    """The resolver picks a winner, merges max rating, rejects losers."""
    db = Database(str(tmp_path / "t.db"))
    fid = db.add_folder(str(tmp_path))
    p1 = _add(db, fid, "owl.jpg", file_hash="HASH1", rating=0)
    p2 = _add(db, fid, "owl (2).jpg", file_hash="HASH1")
    # The add_photo hook auto-rejected one already; reset so we can test
    # apply_duplicate_resolution directly. Also set p2's rating via raw SQL.
    _reset_flags(db, "HASH1")
    db.conn.execute("UPDATE photos SET rating = ? WHERE id = ?", (5, p2))
    db.conn.commit()

    result = db.apply_duplicate_resolution([p1, p2])
    assert result["winner_id"] == p1
    assert result["loser_ids"] == [p2]
    assert result["rejected"] == 1

    row = db.conn.execute(
        "SELECT rating, flag FROM photos WHERE id = ?", (p1,)
    ).fetchone()
    assert row["rating"] == 5
    assert row["flag"] != "rejected"
    row = db.conn.execute(
        "SELECT flag FROM photos WHERE id = ?", (p2,)
    ).fetchone()
    assert row["flag"] == "rejected"


def test_apply_resolution_skips_already_rejected(tmp_path):
    """If fewer than 2 non-rejected candidates remain, resolver is a no-op.

    After two add_photo calls with a shared hash, the hook has already
    rejected one of them. Calling apply_duplicate_resolution on the same
    pair should be a no-op.
    """
    db = Database(str(tmp_path / "t.db"))
    fid = db.add_folder(str(tmp_path))
    p1 = _add(db, fid, "a.jpg", file_hash="H")
    p2 = _add(db, fid, "b.jpg", file_hash="H")

    # Exactly one of p1/p2 is rejected by the hook; identify the survivor.
    flags = {
        r["id"]: r["flag"]
        for r in db.conn.execute(
            "SELECT id, flag FROM photos WHERE id IN (?, ?)", (p1, p2)
        ).fetchall()
    }
    rejected = [pid for pid, f in flags.items() if f == "rejected"]
    assert len(rejected) == 1

    result = db.apply_duplicate_resolution([p1, p2])
    assert result["winner_id"] is None
    assert result["loser_ids"] == []
    assert result["rejected"] == 0
    # Flags unchanged.
    flags_after = {
        r["id"]: r["flag"]
        for r in db.conn.execute(
            "SELECT id, flag FROM photos WHERE id IN (?, ?)", (p1, p2)
        ).fetchall()
    }
    assert flags_after == flags


def test_apply_resolution_merges_keywords(tmp_path):
    """Winner gains loser's keywords (union)."""
    db = Database(str(tmp_path / "t.db"))
    fid = db.add_folder(str(tmp_path))
    p1 = _add(db, fid, "owl.jpg", file_hash="H")
    p2 = _add(db, fid, "owl (2).jpg", file_hash="H")
    # Reset hook's auto-rejection so apply_duplicate_resolution sees both.
    _reset_flags(db, "H")
    kw_id = db.add_keyword("bird")
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (p2, kw_id),
    )
    db.conn.commit()

    db.apply_duplicate_resolution([p1, p2])
    rows = db.conn.execute(
        "SELECT keyword_id FROM photo_keywords WHERE photo_id = ?", (p1,)
    ).fetchall()
    assert any(r["keyword_id"] == kw_id for r in rows)


def test_apply_resolution_single_candidate_noop(tmp_path):
    """With <2 candidates we return an empty result with no changes."""
    db = Database(str(tmp_path / "t.db"))
    fid = db.add_folder(str(tmp_path))
    p1 = _add(db, fid, "a.jpg", file_hash="H")
    result = db.apply_duplicate_resolution([p1])
    assert result["winner_id"] is None
    assert result["rejected"] == 0


# -----------------------------------------------------------------------------
# Task 8: Auto-resolve hook in add_photo
# -----------------------------------------------------------------------------

def test_add_photo_auto_rejects_duplicate(tmp_path):
    """Adding a second photo with a duplicate file_hash auto-rejects the loser."""
    db = Database(str(tmp_path / "t.db"))
    fid = db.add_folder(str(tmp_path))
    p1 = _add(db, fid, "owl.jpg", file_hash="HDUP")
    p2 = _add(db, fid, "owl (2).jpg", file_hash="HDUP")

    # Dirty-suffix filename loses to clean filename (rule 1).
    row2 = db.conn.execute("SELECT flag FROM photos WHERE id = ?", (p2,)).fetchone()
    assert row2["flag"] == "rejected"
    row1 = db.conn.execute("SELECT flag FROM photos WHERE id = ?", (p1,)).fetchone()
    assert row1["flag"] != "rejected"


def test_add_photo_no_hash_no_hook(tmp_path):
    """add_photo without file_hash does not trigger the hook."""
    db = Database(str(tmp_path / "t.db"))
    fid = db.add_folder(str(tmp_path))
    p1 = _add(db, fid, "a.jpg")
    p2 = _add(db, fid, "b.jpg")
    for pid in (p1, p2):
        row = db.conn.execute("SELECT flag FROM photos WHERE id = ?", (pid,)).fetchone()
        assert row["flag"] == "none"


def test_add_photo_hook_swallows_sqlite_error(tmp_path, monkeypatch):
    """sqlite3.Error raised inside the hook is logged and swallowed."""
    import sqlite3

    db = Database(str(tmp_path / "t.db"))
    fid = db.add_folder(str(tmp_path))
    p1 = _add(db, fid, "owl.jpg", file_hash="HBOOM")

    # Patch apply_duplicate_resolution on the instance so the hook's
    # execute() succeeds but the resolver call raises sqlite3.Error.
    def _boom(ids):
        raise sqlite3.OperationalError("synthetic")
    monkeypatch.setattr(db, "apply_duplicate_resolution", _boom)

    p2 = db.add_photo(
        folder_id=fid,
        filename="owl (2).jpg",
        extension=".jpg",
        file_size=1000,
        file_mtime=100.0,
        file_hash="HBOOM",
    )
    assert p2 is not None
    # Neither photo is rejected because the hook failed gracefully.
    r2 = db.conn.execute("SELECT flag FROM photos WHERE id = ?", (p2,)).fetchone()
    assert r2["flag"] != "rejected"


# -----------------------------------------------------------------------------
# Scanner-style flow: add_photo (no hash) + UPDATE hash + explicit hook call.
# -----------------------------------------------------------------------------

def test_check_and_resolve_for_hash_covers_scanner_flow(tmp_path):
    """Mimic scanner.py: insert without file_hash, UPDATE it, then call the hook."""
    db = Database(str(tmp_path / "t.db"))
    fid = db.add_folder(str(tmp_path))

    # Scanner path: add_photo with no hash, then UPDATE photos SET file_hash=?
    p1 = db.add_photo(
        folder_id=fid, filename="owl.jpg", extension=".jpg",
        file_size=1000, file_mtime=100.0,
    )
    p2 = db.add_photo(
        folder_id=fid, filename="owl (2).jpg", extension=".jpg",
        file_size=1000, file_mtime=100.0,
    )
    db.conn.execute("UPDATE photos SET file_hash = ? WHERE id = ?", ("SCANHASH", p1))
    db.conn.commit()
    # First call: only one row has the hash — no-op.
    result = db.check_and_resolve_duplicates_for_hash("SCANHASH")
    assert result is None

    db.conn.execute("UPDATE photos SET file_hash = ? WHERE id = ?", ("SCANHASH", p2))
    db.conn.commit()
    # Second call: two rows share the hash — resolve.
    result = db.check_and_resolve_duplicates_for_hash("SCANHASH")
    assert result is not None
    assert result["winner_id"] == p1
    assert result["loser_ids"] == [p2]

    r1 = db.conn.execute("SELECT flag FROM photos WHERE id = ?", (p1,)).fetchone()
    r2 = db.conn.execute("SELECT flag FROM photos WHERE id = ?", (p2,)).fetchone()
    assert r1["flag"] != "rejected"
    assert r2["flag"] == "rejected"
