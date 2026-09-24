"""Behavior pins for the color-label domain of ``Database``.

The behavior tests exercise the color-label methods only through the public
``Database`` façade, so they hold regardless of whether the SQL lives in
``db.py`` or in ``repositories/photo_labels.py``; the structural test at the
end keeps it in the repository. They cover set / remove / get, the batch
setter, workspace-visible filtering, and the description read/write that
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
    db.set_color_label(pid, "red")
    assert db.get_color_label(pid) == "red"


def test_get_without_a_label_returns_none(db):
    pid = _photo(db)
    assert db.get_color_label(pid) is None


def test_set_replaces_previous_color(db):
    pid = _photo(db)
    db.set_color_label(pid, "red")
    db.set_color_label(pid, "green")
    assert db.get_color_label(pid) == "green"


def test_remove_clears_the_color(db):
    pid = _photo(db)
    db.set_color_label(pid, "blue")
    db.remove_color_label(pid)
    assert db.get_color_label(pid) is None


def test_remove_without_a_label_is_a_no_op(db):
    pid = _photo(db)
    db.remove_color_label(pid)
    assert db.get_color_label(pid) is None


def test_set_rejects_unknown_color(db):
    pid = _photo(db)
    with pytest.raises(ValueError, match="Invalid color label"):
        db.set_color_label(pid, "cyan")


# -- workspace scoping --------------------------------------------------------


def test_labels_are_scoped_to_the_active_workspace(db):
    pid = _photo(db)
    default_ws = db._ws_id()
    other = db.create_workspace("Other")
    db.set_active_workspace(default_ws)
    db.set_color_label(pid, "red")
    db.set_active_workspace(other)
    assert db.get_color_label(pid) is None
    db.set_active_workspace(default_ws)
    assert db.get_color_label(pid) == "red"


# -- batch --------------------------------------------------------------------


def test_get_color_labels_for_photos_returns_a_dict(db):
    a, b, c = _photo(db, "a.jpg"), _photo(db, "b.jpg"), _photo(db, "c.jpg")
    db.set_color_label(a, "red")
    db.set_color_label(c, "yellow")
    result = db.get_color_labels_for_photos([a, b, c])
    assert result == {a: "red", c: "yellow"}


def test_get_color_labels_for_photos_empty_input(db):
    assert db.get_color_labels_for_photos([]) == {}


def test_batch_set_writes_all_photos(db):
    a, b = _photo(db, "a.jpg"), _photo(db, "b.jpg")
    db.batch_set_color_label([a, b], "purple")
    assert db.get_color_labels_for_photos([a, b]) == {a: "purple", b: "purple"}


def test_batch_set_with_none_clears_all(db):
    a, b = _photo(db, "a.jpg"), _photo(db, "b.jpg")
    db.batch_set_color_label([a, b], "red")
    db.batch_set_color_label([a, b], None)
    assert db.get_color_labels_for_photos([a, b]) == {}


def test_batch_set_rejects_unknown_color(db):
    pid = _photo(db)
    with pytest.raises(ValueError, match="Invalid color label"):
        db.batch_set_color_label([pid], "chartreuse")


def test_filter_photo_ids_in_workspace_keeps_only_visible(db):
    pid = _photo(db)
    result = db.filter_photo_ids_in_workspace([pid, 999_999_999])
    assert result == [pid]


# -- descriptions -------------------------------------------------------------


def test_descriptions_default_empty(db):
    assert db.get_color_label_descriptions() == {}


def test_set_and_read_a_description(db):
    db.set_color_label_description("red", "keeper")
    assert db.get_color_label_descriptions() == {"red": "keeper"}


def test_setting_empty_string_clears_that_color(db):
    db.set_color_label_description("red", "keeper")
    db.set_color_label_description("red", "")
    assert db.get_color_label_descriptions() == {}


def test_description_normalizes_internal_whitespace(db):
    db.set_color_label_description("red", "keep\tit\n\n one  copy")
    assert db.get_color_label_descriptions() == {"red": "keep it one copy"}


def test_description_rejects_unknown_color(db):
    with pytest.raises(ValueError, match="Invalid color label"):
        db.set_color_label_description("cyan", "keeper")


def test_description_rejects_non_string(db):
    with pytest.raises(ValueError, match="must be a string"):
        db.set_color_label_description("red", None)


def test_description_length_cap(db):
    over = "x" * (MAX_COLOR_LABEL_DESCRIPTION_LENGTH + 1)
    with pytest.raises(ValueError, match="characters or fewer"):
        db.set_color_label_description("red", over)


def test_descriptions_are_scoped_to_the_active_workspace(db):
    default_ws = db._ws_id()
    other = db.create_workspace("Other")
    db.set_active_workspace(default_ws)
    db.set_color_label_description("red", "keeper")
    db.set_active_workspace(other)
    assert db.get_color_label_descriptions() == {}
    db.set_active_workspace(default_ws)
    assert db.get_color_label_descriptions() == {"red": "keeper"}


# -- structure: the SQL lives in repositories/photo_labels.py -----------------

_DELEGATING_PHOTO_LABEL_METHODS = (
    "set_color_label",
    "remove_color_label",
    "get_color_label",
    "get_color_labels_for_photos",
    "filter_photo_ids_in_workspace",
    "batch_set_color_label",
    "get_color_label_descriptions",
    "set_color_label_description",
)


@pytest.mark.parametrize("name", _DELEGATING_PHOTO_LABEL_METHODS)
def test_photo_label_method_delegates_to_repository(name):
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
        f"Database.{name} touches self.conn; move the SQL to PhotoLabelRepository"
    )
    assert "_photo_label_repository" in attrs, (
        f"Database.{name} no longer delegates to PhotoLabelRepository"
    )


def test_photo_label_signatures_unchanged():
    def params(name):
        return list(inspect.signature(getattr(Database, name)).parameters)

    assert params("set_color_label") == ["self", "photo_id", "color"]
    assert params("remove_color_label") == ["self", "photo_id"]
    assert params("get_color_label") == ["self", "photo_id"]
    assert params("get_color_labels_for_photos") == ["self", "photo_ids"]
    assert params("filter_photo_ids_in_workspace") == ["self", "photo_ids"]
    assert params("batch_set_color_label") == ["self", "photo_ids", "color"]
    assert params("get_color_label_descriptions") == ["self"]
    assert params("set_color_label_description") == ["self", "color", "description"]


def test_valid_color_labels_re_exported_from_repository():
    assert Database.VALID_COLOR_LABELS is VALID_COLOR_LABELS
