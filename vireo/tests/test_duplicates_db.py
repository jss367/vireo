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


# -----------------------------------------------------------------------------
# Task 7: apply_duplicate_resolution
# -----------------------------------------------------------------------------

def test_apply_resolution_rejects_loser_and_merges_rating(tmp_path, monkeypatch):
    """The resolver picks a winner, merges max rating, rejects losers."""
    # Disable the auto-resolve hook so we can test apply_duplicate_resolution directly.
    monkeypatch.setenv("VIREO_DISABLE_AUTO_DUP_RESOLVE", "1")
    db = Database(str(tmp_path / "t.db"))
    fid = db.add_folder(str(tmp_path))
    p1 = _add(db, fid, "owl.jpg", file_hash="HASH1", rating=0)
    p2 = _add(db, fid, "owl (2).jpg", file_hash="HASH1", rating=5)

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


def test_apply_resolution_skips_already_rejected(tmp_path, monkeypatch):
    """If fewer than 2 non-rejected candidates remain, resolver is a no-op."""
    monkeypatch.setenv("VIREO_DISABLE_AUTO_DUP_RESOLVE", "1")
    db = Database(str(tmp_path / "t.db"))
    fid = db.add_folder(str(tmp_path))
    p1 = _add(db, fid, "a.jpg", file_hash="H")
    p2 = _add(db, fid, "b.jpg", file_hash="H")
    db.conn.execute("UPDATE photos SET flag = 'rejected' WHERE id = ?", (p2,))
    db.conn.commit()

    result = db.apply_duplicate_resolution([p1, p2])
    assert result["winner_id"] is None
    assert result["loser_ids"] == []
    assert result["rejected"] == 0
    # p2 stays rejected, p1 stays unflagged
    r1 = db.conn.execute("SELECT flag FROM photos WHERE id = ?", (p1,)).fetchone()
    assert r1["flag"] != "rejected"


def test_apply_resolution_merges_keywords(tmp_path, monkeypatch):
    """Winner gains loser's keywords (union)."""
    monkeypatch.setenv("VIREO_DISABLE_AUTO_DUP_RESOLVE", "1")
    db = Database(str(tmp_path / "t.db"))
    fid = db.add_folder(str(tmp_path))
    p1 = _add(db, fid, "owl.jpg", file_hash="H")
    p2 = _add(db, fid, "owl (2).jpg", file_hash="H")
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


def test_apply_resolution_single_candidate_noop(tmp_path, monkeypatch):
    """With <2 candidates we return an empty result with no changes."""
    monkeypatch.setenv("VIREO_DISABLE_AUTO_DUP_RESOLVE", "1")
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


def test_add_photo_hook_does_not_break_on_resolver_error(tmp_path, monkeypatch):
    """If the resolver raises, the insert still succeeds and returns the id."""
    db = Database(str(tmp_path / "t.db"))
    fid = db.add_folder(str(tmp_path))
    p1 = _add(db, fid, "owl.jpg", file_hash="HBOOM")

    # Monkeypatch the resolver to raise — the hook should swallow and log.
    import duplicates as dup_mod
    def _boom(*a, **kw):
        raise RuntimeError("synthetic resolver failure")
    monkeypatch.setattr(dup_mod, "resolve_duplicates", _boom)

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
