"""Behavior pins for the color-label domain of ``Database``.

The behavior tests go through the ``db.photo_labels`` accessor; the
structural tests at the end pin the accessor's shape and keep the old
forwarding wrappers gone. They cover set / remove / get, the batch setter,
where the active workspace is resolved, and the description read/write that
rides on the workspace's ``config_overrides`` JSON blob.
"""

import ast
import inspect
import textwrap

import pytest
from db import Database
from repositories.photo_labels import (
    MAX_COLOR_LABEL_DESCRIPTION_LENGTH,
    VALID_COLOR_LABELS,
    PhotoLabelRepository,
)


def _photo(db, name="bird.jpg"):
    fid = db.add_folder(f"/photos/{name}", name=name)
    return db.add_photo(
        folder_id=fid, filename=name, extension=".jpg",
        file_size=1000, file_mtime=1.0,
    )


# -- set / remove / get -------------------------------------------------------


def test_set_then_get_returns_the_color(db):
    pid = _photo(db)
    db.photo_labels.set(pid, "red")
    assert db.photo_labels.get(pid) == "red"


def test_get_without_a_label_returns_none(db):
    pid = _photo(db)
    assert db.photo_labels.get(pid) is None


def test_set_replaces_previous_color(db):
    pid = _photo(db)
    db.photo_labels.set(pid, "red")
    db.photo_labels.set(pid, "green")
    assert db.photo_labels.get(pid) == "green"


def test_remove_clears_the_color(db):
    pid = _photo(db)
    db.photo_labels.set(pid, "blue")
    db.photo_labels.remove(pid)
    assert db.photo_labels.get(pid) is None


def test_remove_without_a_label_is_a_no_op(db):
    pid = _photo(db)
    db.photo_labels.remove(pid)
    assert db.photo_labels.get(pid) is None


def test_set_rejects_unknown_color(db):
    pid = _photo(db)
    with pytest.raises(ValueError, match="Invalid color label"):
        db.photo_labels.set(pid, "cyan")


# -- workspace scoping --------------------------------------------------------


def test_labels_are_scoped_to_the_active_workspace(db):
    pid = _photo(db)
    default_ws = db._ws_id()
    other = db.create_workspace("Other")
    db.set_active_workspace(default_ws)
    db.photo_labels.set(pid, "red")
    db.set_active_workspace(other)
    assert db.photo_labels.get(pid) is None
    db.set_active_workspace(default_ws)
    assert db.photo_labels.get(pid) == "red"


# -- batch --------------------------------------------------------------------


def test_get_color_labels_for_photos_returns_a_dict(db):
    a, b, c = _photo(db, "a.jpg"), _photo(db, "b.jpg"), _photo(db, "c.jpg")
    db.photo_labels.set(a, "red")
    db.photo_labels.set(c, "yellow")
    result = db.photo_labels.get_for_photos([a, b, c])
    assert result == {a: "red", c: "yellow"}


def test_get_color_labels_for_photos_empty_input(db):
    assert db.photo_labels.get_for_photos([]) == {}


def test_batch_set_writes_all_photos(db):
    a, b = _photo(db, "a.jpg"), _photo(db, "b.jpg")
    db.photo_labels.set_many([a, b], "purple")
    assert db.photo_labels.get_for_photos([a, b]) == {a: "purple", b: "purple"}


def test_batch_set_with_none_clears_all(db):
    a, b = _photo(db, "a.jpg"), _photo(db, "b.jpg")
    db.photo_labels.set_many([a, b], "red")
    db.photo_labels.set_many([a, b], None)
    assert db.photo_labels.get_for_photos([a, b]) == {}


def test_batch_set_rejects_unknown_color(db):
    pid = _photo(db)
    with pytest.raises(ValueError, match="Invalid color label"):
        db.photo_labels.set_many([pid], "chartreuse")


# -- descriptions -------------------------------------------------------------


def test_descriptions_default_empty(db):
    assert db.photo_labels.get_descriptions() == {}


def test_set_and_read_a_description(db):
    db.photo_labels.set_description("red", "keeper")
    assert db.photo_labels.get_descriptions() == {"red": "keeper"}


def test_setting_empty_string_clears_that_color(db):
    db.photo_labels.set_description("red", "keeper")
    db.photo_labels.set_description("red", "")
    assert db.photo_labels.get_descriptions() == {}


def test_description_normalizes_internal_whitespace(db):
    db.photo_labels.set_description("red", "keep\tit\n\n one  copy")
    assert db.photo_labels.get_descriptions() == {"red": "keep it one copy"}


def test_description_rejects_unknown_color(db):
    with pytest.raises(ValueError, match="Invalid color label"):
        db.photo_labels.set_description("cyan", "keeper")


def test_description_rejects_non_string(db):
    with pytest.raises(ValueError, match="must be a string"):
        db.photo_labels.set_description("red", None)


def test_description_length_cap(db):
    over = "x" * (MAX_COLOR_LABEL_DESCRIPTION_LENGTH + 1)
    with pytest.raises(ValueError, match="characters or fewer"):
        db.photo_labels.set_description("red", over)


def test_descriptions_are_scoped_to_the_active_workspace(db):
    default_ws = db._ws_id()
    other = db.create_workspace("Other")
    db.set_active_workspace(default_ws)
    db.photo_labels.set_description("red", "keeper")
    db.set_active_workspace(other)
    assert db.photo_labels.get_descriptions() == {}
    db.set_active_workspace(default_ws)
    assert db.photo_labels.get_descriptions() == {"red": "keeper"}


# -- workspace resolution -------------------------------------------------------


def test_every_method_raises_before_any_sql_without_a_workspace(db):
    """Each call needs a workspace, exactly where the old eager factory did.

    The workspace is resolved lazily, so reaching ``db.photo_labels`` with none
    active is fine; every method raises ``RuntimeError`` before validating
    its arguments or touching a table, as the factory's ``_ws_id()`` did.
    """
    pid = _photo(db)
    db.photo_labels.set(pid, "red")
    db.set_active_workspace(None)
    repo = db.photo_labels
    statements = []
    db.conn.set_trace_callback(statements.append)
    try:
        for call in (
            lambda: repo.set(pid, "chartreuse"),
            lambda: repo.set_many([], "chartreuse"),
            lambda: repo.get_for_photos([]),
            lambda: repo.remove(pid),
            lambda: repo.set_description("cyan", None),
        ):
            with pytest.raises(RuntimeError, match="No active workspace set"):
                call()
    finally:
        db.conn.set_trace_callback(None)
    assert statements == []


def test_each_call_reads_the_workspace_active_when_it_runs(db):
    """A repository built in one workspace still follows a later switch."""
    pid = _photo(db)
    home = db.require_workspace_id()
    other = db.create_workspace("Other")
    db.set_active_workspace(home)
    db.photo_labels.set(pid, "red")
    db.set_active_workspace(other)
    assert db.photo_labels.get(pid) is None
    db.set_active_workspace(home)
    assert db.photo_labels.get(pid) == "red"


# -- structure ------------------------------------------------------------------

_REMOVED_PHOTO_LABEL_WRAPPERS = (
    "set_color_label",
    "remove_color_label",
    "get_color_label",
    "get_color_labels_for_photos",
    "filter_photo_ids_in_workspace",
    "batch_set_color_label",
    "get_color_label_descriptions",
    "set_color_label_description",
)


def test_photo_labels_is_a_fresh_repository_on_the_connection_per_access(db):
    """``db.photo_labels`` builds a new repository each time, never a cached one.

    The workspace is passed as ``Database._ws_id`` itself, uncalled, so building
    the repository resolves nothing.
    """
    first, second = db.photo_labels, db.photo_labels
    assert isinstance(first, PhotoLabelRepository)
    assert first is not second
    assert first.conn is db.conn
    assert first.workspace_id_fn == db._ws_id


def test_photo_labels_has_no_forwarding_wrappers_on_database():
    """The domain is reached through ``db.photo_labels``; Database keeps no aliases."""
    for name in _REMOVED_PHOTO_LABEL_WRAPPERS:
        assert not hasattr(Database, name), f"Database.{name} came back; call db.photo_labels"
    assert not hasattr(PhotoLabelRepository, "visible_photo_ids"), (
        "the visibility filter lives on db.photo_visibility"
    )
    accessor = Database.__dict__["photo_labels"]
    assert isinstance(accessor, property)
    source = textwrap.dedent(inspect.getsource(accessor.fget))
    attrs = {
        node.attr
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }
    assert "_photo_label_repository" in attrs
    assert "conn" not in attrs


def test_photo_label_signatures_unchanged():
    def params(name):
        return list(inspect.signature(getattr(PhotoLabelRepository, name)).parameters)

    assert params("set") == ["self", "photo_id", "color"]
    assert params("remove") == ["self", "photo_id"]
    assert params("get") == ["self", "photo_id"]
    assert params("get_for_photos") == ["self", "photo_ids"]
    assert params("set_many") == ["self", "photo_ids", "color"]
    assert params("get_descriptions") == ["self"]
    assert params("set_description") == ["self", "color", "description"]


def test_valid_color_labels_re_exported_from_repository():
    assert Database.VALID_COLOR_LABELS is VALID_COLOR_LABELS
