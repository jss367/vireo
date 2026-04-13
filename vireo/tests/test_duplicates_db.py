"""DB integration tests for duplicate resolution (Task 7)."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from db import Database


def _add(db, folder_id, filename, file_mtime=100.0, rating=0, file_hash=None):
    """Helper: add_photo + optionally set file_hash + rating in-place.

    file_hash is applied via UPDATE because the Task-7 add_photo signature
    does not accept it yet — Task 8 will add the kwarg.
    """
    pid = db.add_photo(
        folder_id=folder_id,
        filename=filename,
        extension=os.path.splitext(filename)[1] or ".jpg",
        file_size=1000,
        file_mtime=file_mtime,
    )
    if file_hash is not None:
        db.conn.execute(
            "UPDATE photos SET file_hash = ? WHERE id = ?", (file_hash, pid)
        )
    if rating:
        db.conn.execute("UPDATE photos SET rating = ? WHERE id = ?", (rating, pid))
    db.conn.commit()
    return pid


def test_apply_resolution_rejects_loser_and_merges_rating(tmp_path):
    """The resolver picks a winner, merges max rating, rejects losers."""
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


def test_apply_resolution_skips_already_rejected(tmp_path):
    """If fewer than 2 non-rejected candidates remain, resolver is a no-op."""
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
    r1 = db.conn.execute("SELECT flag FROM photos WHERE id = ?", (p1,)).fetchone()
    assert r1["flag"] != "rejected"


def test_apply_resolution_merges_keywords(tmp_path):
    """Winner gains loser's keywords (union)."""
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


def test_apply_resolution_single_candidate_noop(tmp_path):
    """With <2 candidates we return an empty result with no changes."""
    db = Database(str(tmp_path / "t.db"))
    fid = db.add_folder(str(tmp_path))
    p1 = _add(db, fid, "a.jpg", file_hash="H")
    result = db.apply_duplicate_resolution([p1])
    assert result["winner_id"] is None
    assert result["rejected"] == 0
