"""Reviewed coordinates survive photo collapse without silently choosing evidence."""

import pytest
from db import Database
from repositories.photo_row_deletion import photo_row_deletion
from services import folder_merges, moves_merge
from test_db_moves_merge import _collision_tree
from test_identity_collapse_paths import _fixture

CORRECTION = (.1, .2, "Synthetic review")


@pytest.fixture
def db(tmp_path):
    with Database(str(tmp_path / "gps-merge.db")) as value:
        yield value


@pytest.fixture(params=["direct", "folder", "companion", "staged_same", "staged_different"])
def merge_case(db, tmp_path, request):
    path = request.param
    if path.startswith("staged"):
        same_hash = path == "staged_same"
        tree = _collision_tree(db, tmp_path, same_hash=same_hash)
        losing, survivor = ((tree["staged"], tree["survivor"]) if same_hash
                            else (tree["survivor"], tree["staged"]))
        db.photo_visibility.grant(db.require_workspace_id(), [losing, survivor])
    else:
        source, target, survivor, losing, target_path = _fixture(
            db, tmp_path, "companion" if path == "companion" else "folder",
        )
    db.commit()

    def merge():
        if path.startswith("staged"):
            moves_merge.merge_staged_tree_into_archive(db, tree["stage_root"], tree["arch"])
        elif path == "folder":
            folder_merges.merge_into_existing(db, source, target, str(target_path))
        elif path == "companion":
            from scanner.companions import _pair_raw_jpeg_companions
            return _pair_raw_jpeg_companions(db)
        else:
            with photo_row_deletion(db.conn) as deletion:
                deletion.delete({losing: survivor})
        return None

    return path, losing, survivor, merge


def _review(db, pid, correction=CORRECTION):
    lat, lng, evidence = correction
    db.audit.apply_gps_review([pid], evidence, lat, lng)


def _rows(db):
    return [tuple(row) for row in db.conn.execute("SELECT * FROM reviewed_gps ORDER BY photo_id")]


@pytest.mark.parametrize("state", ["loser_only", "survivor_only", "identical", "none"])
def test_reviewed_gps_survives_every_merge_path(db, merge_case, state):
    _path, losing, survivor, merge = merge_case
    if state in {"loser_only", "identical"}:
        _review(db, losing)
    if state in {"survivor_only", "identical"}:
        _review(db, survivor)
    db.commit()

    with db.transaction():
        merge()

    assert db.photos.get(losing) is None
    assert _rows(db) == ([] if state == "none" else [(survivor, *CORRECTION)])
    assert not list(db.conn.execute("PRAGMA foreign_key_check"))
    if state != "none":
        effective = db.locations.get_effective([survivor])[survivor]
        assert (effective["latitude"], effective["longitude"], effective["source"]) == (.1, .2, "reviewed_gps")


@pytest.mark.parametrize("other", [(.3, .4, "Synthetic review"), (.1, .2, "Distinct evidence")])
def test_conflicting_reviews_refuse_merge_and_preserve_both(db, merge_case, other):
    path, losing, survivor, merge = merge_case
    _review(db, losing)
    _review(db, survivor, other)
    db.commit()
    before = list(db.conn.iterdump())

    if path == "companion":
        assert merge() == {}
    else:
        with pytest.raises(ValueError, match="GPS"), db.transaction():
            merge()

    assert list(db.conn.iterdump()) == before
    assert db.photos.get(losing) is not None and db.photos.get(survivor) is not None
    assert not list(db.conn.execute("PRAGMA foreign_key_check"))


def test_ordinary_deletion_cascades_only_the_deleted_review(db, tmp_path):
    _source, _target, survivor, losing, _target_path = _fixture(db, tmp_path, "folder")
    _review(db, losing)
    _review(db, survivor)
    db.commit()

    with db.transaction(), photo_row_deletion(db.conn) as deletion:
        deletion.delete({losing: None})

    assert _rows(db) == [(survivor, *CORRECTION)]
    assert not list(db.conn.execute("PRAGMA foreign_key_check"))


def test_conflicting_batch_rolls_back_earlier_review_transfer(db, tmp_path):
    from services import photo_ingest

    source, _target, survivor, losing, _target_path = _fixture(db, tmp_path, "folder")
    other = photo_ingest.add_photo(db, source, "other.jpg", ".jpg", 5, 1)
    _review(db, losing)
    _review(db, other, (.3, .4, "Other synthetic review"))
    db.commit()
    before = list(db.conn.iterdump())

    with pytest.raises(ValueError, match="GPS"), db.transaction():
        with photo_row_deletion(db.conn) as deletion:
            deletion.delete({losing: survivor, other: survivor})

    assert list(db.conn.iterdump()) == before
    assert not list(db.conn.execute("PRAGMA foreign_key_check"))
