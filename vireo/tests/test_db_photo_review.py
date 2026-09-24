"""Behavior pins for the review domain of ``Database`` (ratings + flags).

The behavior tests exercise the review methods only through the public
``Database`` façade, so they hold regardless of whether the SQL lives in
``db.py`` or in ``repositories/photo_review.py``; the structural test at the
end keeps it in the repository. They cover per-photo and batch writes and
the workspace-membership check that ``verify_workspace=True`` runs. The
``_commit=False`` handoff is a signature contract enforced by the signature
test.
"""

import ast
import inspect
import textwrap

import pytest
from db import Database


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
    db.update_photo_rating(pid, 4)
    assert _rating(db, pid) == 4


def test_update_rating_rejects_photo_outside_workspace(db):
    pid = _photo(db)
    other = db.create_workspace("Other")
    db.set_active_workspace(other)
    with pytest.raises(ValueError, match="does not belong"):
        db.update_photo_rating(pid, 2)


def test_update_rating_skips_workspace_check_when_disabled(db):
    pid = _photo(db)
    other = db.create_workspace("Other")
    db.set_active_workspace(other)
    db.update_photo_rating(pid, 2, verify_workspace=False)
    assert _rating(db, pid) == 2


# -- update_photo_flag --------------------------------------------------------


def test_update_flag_writes_the_value(db):
    pid = _photo(db)
    db.update_photo_flag(pid, "flagged")
    assert _flag(db, pid) == "flagged"


def test_update_flag_rejects_photo_outside_workspace(db):
    pid = _photo(db)
    other = db.create_workspace("Other")
    db.set_active_workspace(other)
    with pytest.raises(ValueError, match="does not belong"):
        db.update_photo_flag(pid, "flagged")


# -- batch --------------------------------------------------------------------


def test_batch_update_rating_writes_all(db):
    a, b = _photo(db, "a.jpg"), _photo(db, "b.jpg")
    db.batch_update_photo_rating([a, b], 5)
    assert _rating(db, a) == 5
    assert _rating(db, b) == 5


def test_batch_update_flag_writes_all(db):
    a, b = _photo(db, "a.jpg"), _photo(db, "b.jpg")
    db.batch_update_photo_flag([a, b], "rejected")
    assert _flag(db, a) == "rejected"
    assert _flag(db, b) == "rejected"


def test_batch_update_empty_input_is_a_no_op(db):
    db.batch_update_photo_rating([], 4)
    db.batch_update_photo_flag([], "flagged")


def test_batch_update_rating_rejects_photo_outside_workspace(db):
    a = _photo(db, "a.jpg")
    other = db.create_workspace("Other")
    db.set_active_workspace(other)
    with pytest.raises(ValueError, match="does not belong"):
        db.batch_update_photo_rating([a], 2)


# -- structure: the SQL lives in repositories/photo_review.py -----------------

_DELEGATING_PHOTO_REVIEW_METHODS = (
    "update_photo_rating",
    "batch_update_photo_rating",
    "update_photo_flag",
    "batch_update_photo_flag",
)


@pytest.mark.parametrize("name", _DELEGATING_PHOTO_REVIEW_METHODS)
def test_photo_review_method_delegates_to_repository(name):
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
        f"Database.{name} touches self.conn; move the SQL to PhotoReviewRepository"
    )
    assert "_photo_review_repository" in attrs, (
        f"Database.{name} no longer delegates to PhotoReviewRepository"
    )


def test_photo_review_signatures_unchanged():
    empty = inspect.Parameter.empty

    def params(name):
        return [
            (p.name, p.default)
            for p in inspect.signature(getattr(Database, name)).parameters.values()
        ]

    assert params("update_photo_rating") == [
        ("self", empty), ("photo_id", empty), ("rating", empty),
        ("verify_workspace", True),
    ]
    assert params("batch_update_photo_rating") == [
        ("self", empty), ("photo_ids", empty), ("rating", empty),
        ("verify_workspace", True),
    ]
    assert params("update_photo_flag") == [
        ("self", empty), ("photo_id", empty), ("flag", empty),
        ("verify_workspace", True), ("_commit", True),
    ]
    assert params("batch_update_photo_flag") == [
        ("self", empty), ("photo_ids", empty), ("flag", empty),
        ("verify_workspace", True),
    ]


def test_photo_review_repository_builds_without_an_active_workspace(db):
    db.set_active_workspace(None)
    repo = db._photo_review_repository()
    assert repo.conn is db.conn
    assert repo.workspace_id is None
