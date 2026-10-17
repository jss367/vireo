"""Tests for ``Database.keep_duplicate_copy``.

Backs "Keep this copy instead" on the Duplicates page: the user overrides
which copy of a resolved group is kept, so a rejected copy is promoted and
the copy it replaces is rejected, with metadata following the kept copy.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import repositories.duplicates as dup_repo
from db import Database


def _resolved_pair(tmp_path, file_hash="HKEEP", touch=(True, True)):
    """Two copies of one file in different folders; the hook rejects one.

    Returns ``(db, kept_id, rejected_id)``.
    """
    db = Database(str(tmp_path / "t.db"))
    ids = []
    for name, make in zip(("a", "longer_b"), touch, strict=True):
        folder = tmp_path / name
        folder.mkdir()
        if make:
            (folder / "owl.jpg").write_bytes(b"x")
        fid = db.add_folder(str(folder))
        ids.append(db.add_photo(
            folder_id=fid, filename="owl.jpg", extension=".jpg",
            file_size=1, file_mtime=100.0, file_hash=file_hash,
        ))
    flags = _flags(db, ids)
    kept = [pid for pid in ids if flags[pid] != "rejected"]
    rejected = [pid for pid in ids if flags[pid] == "rejected"]
    assert len(kept) == 1 and len(rejected) == 1
    return db, kept[0], rejected[0]


def _flags(db, ids):
    return {
        r["id"]: r["flag"]
        for r in db.conn.execute(
            f"SELECT id, flag FROM photos WHERE id IN ({','.join('?' * len(ids))})",
            list(ids),
        ).fetchall()
    }


def _duplicate_rejections(db):
    return {r["photo_id"] for r in db.conn.execute(
        "SELECT photo_id FROM duplicate_rejections"
    ).fetchall()}


def test_keep_promotes_rejected_copy_and_rejects_the_kept_one(tmp_path):
    db, kept, rejected = _resolved_pair(tmp_path)

    result = db.keep_duplicate_copy(rejected)

    assert result == {
        "file_hash": "HKEEP", "winner_id": rejected, "loser_ids": [kept],
        "rating": 0, "flag": "none",
    }
    assert _flags(db, [kept, rejected]) == {kept: "rejected", rejected: "none"}
    # The replaced copy is now the resolver-style rejection the trash and
    # auto-reopen paths look for; the promoted copy no longer is.
    assert _duplicate_rejections(db) == {kept}
    groups = db.duplicates.find_groups(include_resolved=True)
    assert [g["winner_id"] for g in groups] == [rejected]


def test_keep_moves_rating_and_keywords_to_the_chosen_copy(tmp_path):
    db, kept, rejected = _resolved_pair(tmp_path)
    kw_id = db.add_keyword("Red Kangaroo")
    db.tag_photo(kept, kw_id)
    db.conn.execute("UPDATE photos SET rating = 4 WHERE id = ?", (kept,))
    db.conn.commit()

    result = db.keep_duplicate_copy(rejected)

    assert result["rating"] == 4
    row = db.conn.execute(
        "SELECT rating FROM photos WHERE id = ?", (rejected,),
    ).fetchone()
    assert row["rating"] == 4
    kws = {r["keyword_id"] for r in db.conn.execute(
        "SELECT keyword_id FROM photo_keywords WHERE photo_id = ?", (rejected,),
    ).fetchall()}
    assert kw_id in kws


def test_keep_carries_the_pick_flag_to_the_chosen_copy(tmp_path):
    db, kept, rejected = _resolved_pair(tmp_path)
    db.conn.execute("UPDATE photos SET flag = 'flagged' WHERE id = ?", (kept,))
    db.conn.commit()

    result = db.keep_duplicate_copy(rejected)

    assert result["flag"] == "flagged"
    assert _flags(db, [kept, rejected]) == {kept: "rejected", rejected: "flagged"}


def test_keep_switches_back(tmp_path):
    db, kept, rejected = _resolved_pair(tmp_path)

    db.keep_duplicate_copy(rejected)
    result = db.keep_duplicate_copy(kept)

    assert result["loser_ids"] == [rejected]
    assert _flags(db, [kept, rejected]) == {kept: "none", rejected: "rejected"}


def test_keep_already_kept_copy_is_a_noop(tmp_path):
    db, kept, rejected = _resolved_pair(tmp_path)

    result = db.keep_duplicate_copy(kept)

    assert result["loser_ids"] == []
    assert _flags(db, [kept, rejected]) == {kept: "none", rejected: "rejected"}


def test_keep_rejects_every_other_kept_copy(tmp_path):
    """A group with two kept copies ends with only the chosen one kept;
    a copy rejected before stays rejected."""
    db, kept, rejected = _resolved_pair(tmp_path)
    folder = tmp_path / "c"
    folder.mkdir()
    fid = db.add_folder(str(folder))
    third = db.add_photo(
        folder_id=fid, filename="owl.jpg", extension=".jpg",
        file_size=1, file_mtime=100.0, file_hash="HKEEP",
    )
    db.conn.execute("UPDATE photos SET flag = 'none' WHERE id = ?", (third,))
    db.conn.commit()

    result = db.keep_duplicate_copy(rejected)

    assert sorted(result["loser_ids"]) == sorted([kept, third])
    assert _flags(db, [kept, rejected, third]) == {
        kept: "rejected", rejected: "none", third: "rejected",
    }


def test_keep_refuses_copy_missing_on_disk(tmp_path):
    """Promoting a copy whose file is gone would reject the one that exists."""
    db, kept, rejected = _resolved_pair(tmp_path, touch=(True, False))

    assert db.keep_duplicate_copy(rejected) == {"error": "missing on disk"}
    assert _flags(db, [kept, rejected]) == {kept: "none", rejected: "rejected"}


def test_keep_refuses_copy_on_offline_volume(tmp_path, monkeypatch):
    db, kept, rejected = _resolved_pair(tmp_path)
    monkeypatch.setattr(dup_repo, "_volume_offline", lambda path: True)

    assert db.keep_duplicate_copy(rejected) == {"error": "volume offline"}
    assert _flags(db, [kept, rejected]) == {kept: "none", rejected: "rejected"}


def test_keep_refuses_photo_without_duplicates(tmp_path):
    db = Database(str(tmp_path / "t.db"))
    (tmp_path / "solo.jpg").write_bytes(b"x")
    fid = db.add_folder(str(tmp_path))
    pid = db.add_photo(
        folder_id=fid, filename="solo.jpg", extension=".jpg",
        file_size=1, file_mtime=100.0, file_hash="HSOLO",
    )

    assert db.keep_duplicate_copy(pid) == {"error": "not a duplicate"}
    assert db.keep_duplicate_copy(pid + 999) == {"error": "not found"}
