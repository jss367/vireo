"""Behavior pins for the review domain of ``Database`` (ratings + flags).

The behavior tests go through the ``db.photo_review`` accessor; the
structural tests at the end pin the accessor's shape and keep the old
forwarding wrappers gone. They cover per-photo and batch writes and
the workspace-membership check that ``verify_workspace=True`` runs. The
``_commit=False`` handoff is a signature contract enforced by the signature
test.
"""

import ast
import inspect
import textwrap

import pytest
from db import Database
from repositories.photo_review import PhotoReviewRepository


def _photo(db, name="bird.jpg"):
    fid = db.add_folder(f"/photos/{name}", name=name)
    return db.add_photo(
        folder_id=fid, filename=name, extension=".jpg",
        file_size=1000, file_mtime=1.0,
    )


def _rating(db, photo_id):
    return db.conn.execute(
        "SELECT rating FROM photos WHERE id = ?", (photo_id,)
    ).fetchone()["rating"]


def _flag(db, photo_id):
    return db.conn.execute(
        "SELECT flag FROM photos WHERE id = ?", (photo_id,)
    ).fetchone()["flag"]


# -- update_photo_rating ------------------------------------------------------


def test_update_rating_writes_the_value(db):
    pid = _photo(db)
    db.photo_review.set_rating(pid, 4)
    assert _rating(db, pid) == 4


def test_update_rating_rejects_photo_outside_workspace(db):
    pid = _photo(db)
    other = db.create_workspace("Other")
    db.set_active_workspace(other)
    with pytest.raises(ValueError, match="does not belong"):
        db.photo_review.set_rating(pid, 2)


def test_update_rating_skips_workspace_check_when_disabled(db):
    pid = _photo(db)
    other = db.create_workspace("Other")
    db.set_active_workspace(other)
    db.photo_review.set_rating(pid, 2, verify_workspace=False)
    assert _rating(db, pid) == 2


# -- update_photo_flag --------------------------------------------------------


def test_update_flag_writes_the_value(db):
    pid = _photo(db)
    db.photo_review.set_flag(pid, "flagged")
    assert _flag(db, pid) == "flagged"


def test_update_flag_rejects_photo_outside_workspace(db):
    pid = _photo(db)
    other = db.create_workspace("Other")
    db.set_active_workspace(other)
    with pytest.raises(ValueError, match="does not belong"):
        db.photo_review.set_flag(pid, "flagged")


# -- batch --------------------------------------------------------------------


def test_batch_update_rating_writes_all(db):
    a, b = _photo(db, "a.jpg"), _photo(db, "b.jpg")
    db.photo_review.set_ratings([a, b], 5)
    assert _rating(db, a) == 5
    assert _rating(db, b) == 5


def test_batch_update_flag_writes_all(db):
    a, b = _photo(db, "a.jpg"), _photo(db, "b.jpg")
    db.photo_review.set_flags([a, b], "rejected")
    assert _flag(db, a) == "rejected"
    assert _flag(db, b) == "rejected"


def test_batch_update_empty_input_is_a_no_op(db):
    db.photo_review.set_ratings([], 4)
    db.photo_review.set_flags([], "flagged")


def test_batch_update_rating_rejects_photo_outside_workspace(db):
    a = _photo(db, "a.jpg")
    other = db.create_workspace("Other")
    db.set_active_workspace(other)
    with pytest.raises(ValueError, match="does not belong"):
        db.photo_review.set_ratings([a], 2)


# -- structure ------------------------------------------------------------------

_REMOVED_PHOTO_REVIEW_WRAPPERS = (
    "update_photo_rating",
    "batch_update_photo_rating",
    "update_photo_flag",
    "batch_update_photo_flag",
    "get_wildlife_excluded_states",
)


def test_photo_review_signatures_unchanged():
    """``verify_workspace`` and the ``_commit=False`` handoff keep their defaults."""
    empty = inspect.Parameter.empty
    kw = inspect.Parameter.KEYWORD_ONLY

    def params(name):
        return [
            (p.name, p.default, p.kind == kw)
            for p in inspect.signature(getattr(PhotoReviewRepository, name)).parameters.values()
        ]

    assert params("set_rating") == [
        ("self", empty, False), ("photo_id", empty, False), ("rating", empty, False),
        ("verify_workspace", True, True),
    ]
    assert params("set_ratings") == [
        ("self", empty, False), ("photo_ids", empty, False), ("rating", empty, False),
        ("verify_workspace", True, True),
    ]
    assert params("set_flag") == [
        ("self", empty, False), ("photo_id", empty, False), ("flag", empty, False),
        ("verify_workspace", True, True), ("_commit", True, True),
    ]
    assert params("set_flags") == [
        ("self", empty, False), ("photo_ids", empty, False), ("flag", empty, False),
        ("verify_workspace", True, True),
    ]


def test_photo_review_repository_builds_without_an_active_workspace(db):
    db.set_active_workspace(None)
    repo = db.photo_review
    assert repo.conn is db.conn
    assert repo.workspace_id is None


def test_photo_review_is_a_fresh_repository_on_the_connection_per_access(db):
    """``db.photo_review`` builds a new repository each time, never a cached one."""
    first, second = db.photo_review, db.photo_review
    assert isinstance(first, PhotoReviewRepository)
    assert first is not second
    assert first.conn is db.conn
    assert first.workspace_id == db.active_workspace_id


def test_photo_review_follows_a_workspace_switch_between_accesses(db):
    """Each access carries the workspace active at that moment."""
    a = _photo(db, "a.jpg")
    home = db.require_workspace_id()
    other = db.create_workspace("Other")
    db.set_active_workspace(other)
    assert db.photo_review.workspace_id == other
    with pytest.raises(ValueError, match="does not belong"):
        db.photo_review.set_rating(a, 2)
    db.set_active_workspace(home)
    db.photo_review.set_rating(a, 2)
    assert db.get_photo(a)["rating"] == 2


def test_photo_review_has_no_forwarding_wrappers_on_database():
    """The domain is reached through ``db.photo_review``; Database keeps no aliases."""
    for name in _REMOVED_PHOTO_REVIEW_WRAPPERS:
        assert not hasattr(Database, name), f"Database.{name} came back; call db.photo_review"
    accessor = Database.__dict__["photo_review"]
    assert isinstance(accessor, property)
    source = textwrap.dedent(inspect.getsource(accessor.fget))
    attrs = {
        node.attr
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }
    assert "_photo_review_repository" in attrs
    assert "conn" not in attrs
