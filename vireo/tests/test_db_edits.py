"""Behavior pins for the edits domain of ``Database``.

The behavior tests exercise per-photo edit recipes and global edit presets
only through the public ``Database`` façade, so they hold regardless of
whether the SQL lives in ``db.py`` or in ``repositories/edits.py``; the
structural tests at the end keep it in the repository.
"""

import ast
import contextlib
import inspect
import json
import sqlite3
import textwrap

import pytest
from db import Database
from image_edits import RecipeError


@contextlib.contextmanager
def _reader(db):
    """A second connection: sees only what the façade has committed."""
    conn = sqlite3.connect(db._db_path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def _photo(db, folder_id, name):
    return db.add_photo(folder_id, name, ".jpg", 100, 1.0)


@pytest.fixture
def photos(db, tmp_path):
    """One photo in the active workspace, one in an unlinked folder."""
    inside_folder = db.add_folder(str(tmp_path / "inside"))
    outside_folder = db.add_folder(
        str(tmp_path / "outside"), link_to_workspace=False
    )
    return _photo(db, inside_folder, "in.jpg"), _photo(db, outside_folder, "out.jpg")


def _raw_recipe(db, photo_id, recipe_json):
    db.conn.execute(
        "INSERT INTO photo_edit_recipes (photo_id, recipe_json) VALUES (?, ?)",
        (photo_id, recipe_json),
    )
    db.conn.commit()


def _stored_recipe(db, photo_id):
    with _reader(db) as conn:
        row = conn.execute(
            "SELECT recipe_json FROM photo_edit_recipes WHERE photo_id = ?",
            (photo_id,),
        ).fetchone()
    return None if row is None else row["recipe_json"]


EXPOSURE = {"adjustments": {"exposure": 0.5}}
NORMALIZED_EXPOSURE = {"version": 1, "adjustments": {"exposure": 0.5}}


# -- per-photo edit recipes ---------------------------------------------------


def test_get_recipe_returns_none_without_a_row(db, photos):
    inside, _ = photos
    assert db.get_photo_edit_recipe(inside) is None


def test_set_recipe_commits_and_returns_normalized_copy(db, photos):
    inside, _ = photos
    result = db.set_photo_edit_recipe(inside, EXPOSURE)
    assert result == NORMALIZED_EXPOSURE
    assert not db.conn.in_transaction
    assert json.loads(_stored_recipe(db, inside)) == NORMALIZED_EXPOSURE
    got = db.get_photo_edit_recipe(inside)
    assert got == NORMALIZED_EXPOSURE
    got["adjustments"]["exposure"] = 9
    assert db.get_photo_edit_recipe(inside) == NORMALIZED_EXPOSURE


def test_set_recipe_upserts_existing_row(db, photos):
    inside, _ = photos
    db.set_photo_edit_recipe(inside, EXPOSURE)
    db.set_photo_edit_recipe(inside, {"adjustments": {"contrast": 10}})
    with _reader(db) as conn:
        rows = conn.execute(
            "SELECT recipe_json FROM photo_edit_recipes WHERE photo_id = ?",
            (inside,),
        ).fetchall()
    assert len(rows) == 1
    assert json.loads(rows[0]["recipe_json"])["adjustments"] == {"contrast": 10.0}


def test_set_recipe_accepts_json_string(db, photos):
    inside, _ = photos
    assert db.set_photo_edit_recipe(inside, json.dumps(EXPOSURE)) == NORMALIZED_EXPOSURE


@pytest.mark.parametrize("noop", [None, {}, ""])
def test_set_noop_recipe_deletes_row_and_commits(db, photos, noop):
    inside, _ = photos
    db.set_photo_edit_recipe(inside, EXPOSURE)
    assert db.set_photo_edit_recipe(inside, noop) is None
    assert not db.conn.in_transaction
    assert _stored_recipe(db, inside) is None
    assert db.get_photo_edit_recipe(inside) is None


def test_set_recipe_without_commit_joins_callers_transaction(db, photos):
    inside, _ = photos
    assert db.set_photo_edit_recipe(inside, EXPOSURE, _commit=False) == (
        NORMALIZED_EXPOSURE
    )
    assert db.conn.in_transaction
    assert _stored_recipe(db, inside) is None
    db.conn.commit()
    assert json.loads(_stored_recipe(db, inside)) == NORMALIZED_EXPOSURE


def test_set_noop_recipe_without_commit_joins_callers_transaction(db, photos):
    inside, _ = photos
    db.set_photo_edit_recipe(inside, EXPOSURE)
    assert db.set_photo_edit_recipe(inside, None, _commit=False) is None
    assert db.conn.in_transaction
    assert _stored_recipe(db, inside) is not None
    db.conn.rollback()
    assert db.get_photo_edit_recipe(inside) == NORMALIZED_EXPOSURE


def test_set_invalid_recipe_raises_and_writes_nothing(db, photos):
    inside, _ = photos
    with pytest.raises(RecipeError, match="recipe must be an object"):
        db.set_photo_edit_recipe(inside, [1])
    assert _stored_recipe(db, inside) is None


def test_set_recipe_verifies_workspace_by_default(db, photos):
    _, outside = photos
    with pytest.raises(ValueError, match="does not belong to the active workspace"):
        db.set_photo_edit_recipe(outside, EXPOSURE)
    assert _stored_recipe(db, outside) is None
    assert db.set_photo_edit_recipe(
        outside, EXPOSURE, verify_workspace=False
    ) == NORMALIZED_EXPOSURE


def test_set_recipe_without_verify_needs_no_active_workspace(db, photos):
    inside, _ = photos
    db.set_active_workspace(None)
    assert db.set_photo_edit_recipe(
        inside, EXPOSURE, verify_workspace=False
    ) == NORMALIZED_EXPOSURE
    with pytest.raises(RuntimeError, match="No active workspace set"):
        db.set_photo_edit_recipe(inside, EXPOSURE)


def test_get_recipe_skips_workspace_check_by_default(db, photos):
    _, outside = photos
    _raw_recipe(db, outside, json.dumps(EXPOSURE))
    db.set_active_workspace(None)
    assert db.get_photo_edit_recipe(outside) == NORMALIZED_EXPOSURE


def test_get_recipe_with_verify_checks_workspace(db, photos):
    inside, outside = photos
    _raw_recipe(db, inside, json.dumps(EXPOSURE))
    _raw_recipe(db, outside, json.dumps(EXPOSURE))
    assert db.get_photo_edit_recipe(inside, verify_workspace=True) == (
        NORMALIZED_EXPOSURE
    )
    with pytest.raises(ValueError, match="does not belong to the active workspace"):
        db.get_photo_edit_recipe(outside, verify_workspace=True)


@pytest.mark.parametrize("raw", ["{", "[1]", '{"rotation": 45}'])
def test_get_recipe_returns_none_for_malformed_row(db, photos, caplog, raw):
    inside, _ = photos
    _raw_recipe(db, inside, raw)
    with caplog.at_level("WARNING"):
        assert db.get_photo_edit_recipe(inside) is None
    assert f"Invalid stored edit recipe for photo {inside}" in caplog.text


def test_get_recipe_returns_none_for_empty_stored_recipe(db, photos):
    inside, _ = photos
    _raw_recipe(db, inside, "{}")
    assert db.get_photo_edit_recipe(inside) is None


def test_get_recipes_empty_input_needs_no_workspace(db):
    db.set_active_workspace(None)
    assert db.get_photo_edit_recipes([]) == {}
    assert db.get_photo_edit_recipes(()) == {}


def test_get_recipes_maps_ids_and_skips_bad_empty_and_missing(db, photos, caplog):
    inside, outside = photos
    folder = db.add_folder("/tmp/more")
    bad = _photo(db, folder, "bad.jpg")
    empty = _photo(db, folder, "empty.jpg")
    missing = _photo(db, folder, "missing.jpg")
    _raw_recipe(db, inside, json.dumps(EXPOSURE))
    _raw_recipe(db, outside, json.dumps({"rotation": 90}))
    _raw_recipe(db, bad, "not json")
    _raw_recipe(db, empty, "{}")
    db.set_active_workspace(None)
    with caplog.at_level("WARNING"):
        out = db.get_photo_edit_recipes([inside, outside, bad, empty, missing])
    assert out == {
        inside: NORMALIZED_EXPOSURE,
        outside: {"version": 1, "rotation": 90},
    }
    assert f"Invalid stored edit recipe for photo {bad}" in caplog.text


def test_get_recipes_accepts_any_iterable(db, photos):
    inside, _ = photos
    _raw_recipe(db, inside, json.dumps(EXPOSURE))
    assert db.get_photo_edit_recipes(iter([inside])) == {inside: NORMALIZED_EXPOSURE}


def test_get_recipes_chunks_past_the_sqlite_parameter_limit(db, tmp_path):
    folder = db.add_folder(str(tmp_path / "many"))
    db.conn.executemany(
        "INSERT INTO photos (folder_id, filename, extension, file_size, file_mtime) "
        "VALUES (?, ?, '.jpg', 1, 1.0)",
        [(folder, f"p{i}.jpg") for i in range(1700)],
    )
    ids = [r[0] for r in db.conn.execute("SELECT id FROM photos ORDER BY id")]
    recipe_json = json.dumps(EXPOSURE)
    db.conn.executemany(
        "INSERT INTO photo_edit_recipes (photo_id, recipe_json) VALUES (?, ?)",
        [(pid, recipe_json) for pid in ids],
    )
    db.conn.commit()
    out = db.get_photo_edit_recipes(ids)
    assert len(out) == 1700
    assert out[ids[-1]] == NORMALIZED_EXPOSURE


def test_clear_recipe_reports_whether_a_row_was_removed(db, photos):
    inside, _ = photos
    db.set_photo_edit_recipe(inside, EXPOSURE)
    assert db.clear_photo_edit_recipe(inside) is True
    assert not db.conn.in_transaction
    assert _stored_recipe(db, inside) is None
    assert db.clear_photo_edit_recipe(inside) is False
    assert not db.conn.in_transaction


def test_clear_recipe_verifies_workspace_by_default(db, photos):
    _, outside = photos
    _raw_recipe(db, outside, json.dumps(EXPOSURE))
    with pytest.raises(ValueError, match="does not belong to the active workspace"):
        db.clear_photo_edit_recipe(outside)
    assert _stored_recipe(db, outside) is not None
    db.set_active_workspace(None)
    assert db.clear_photo_edit_recipe(outside, verify_workspace=False) is True
    assert _stored_recipe(db, outside) is None


def test_edit_history_undo_restores_recipe_through_facade(db, photos, monkeypatch):
    inside, _ = photos
    calls = []
    real = db.set_photo_edit_recipe

    def spy(*args, **kwargs):
        calls.append((args, kwargs))
        return real(*args, **kwargs)

    monkeypatch.setattr(db, "set_photo_edit_recipe", spy)
    db._edit_set_edit_recipe(inside, json.dumps(EXPOSURE))
    db._edit_set_edit_recipe(inside, "")
    assert calls == [
        ((inside, json.dumps(EXPOSURE)), {"verify_workspace": False}),
        ((inside, None), {"verify_workspace": False}),
    ]
    assert db.get_photo_edit_recipe(inside) is None


# -- edit presets --------------------------------------------------------------


def _raw_preset(db, name, recipe_json):
    db.conn.execute(
        "INSERT INTO edit_presets (name, recipe_json) VALUES (?, ?)",
        (name, recipe_json),
    )
    db.conn.commit()


def test_presets_are_global_and_need_no_workspace(db):
    db.set_active_workspace(None)
    saved = db.save_edit_preset("Global", EXPOSURE)
    assert db.list_edit_presets() == [saved]
    assert db.delete_edit_preset(saved["id"]) is True


def test_save_preset_returns_stored_shape_and_commits(db):
    saved = db.save_edit_preset("  Dawn ", EXPOSURE)
    assert list(saved) == ["id", "name", "recipe", "updated_at"]
    assert saved["name"] == "Dawn"
    assert saved["recipe"] == NORMALIZED_EXPOSURE
    assert isinstance(saved["id"], int)
    assert saved["updated_at"]
    assert not db.conn.in_transaction
    with _reader(db) as conn:
        row = conn.execute(
            "SELECT id, recipe_json FROM edit_presets WHERE name = 'Dawn'"
        ).fetchone()
    assert row["id"] == saved["id"]
    assert json.loads(row["recipe_json"]) == NORMALIZED_EXPOSURE


def test_save_preset_with_fields_keeps_neutral_values(db):
    saved = db.save_edit_preset(
        "Reset exposure",
        {"adjustments": {"exposure": 0, "contrast": 20}},
        fields=["adjustments.exposure"],
    )
    assert list(saved) == ["id", "name", "recipe", "fields", "updated_at"]
    assert saved["fields"] == ["adjustments.exposure"]
    assert "contrast" not in saved["recipe"].get("adjustments", {})
    [listed] = db.list_edit_presets()
    assert listed == saved
    with _reader(db) as conn:
        stored = json.loads(conn.execute(
            "SELECT recipe_json FROM edit_presets"
        ).fetchone()["recipe_json"])
    assert stored["fields"] == ["adjustments.exposure"]


def test_save_preset_rejects_bad_fields(db):
    with pytest.raises(RecipeError, match="Select at least one setting"):
        db.save_edit_preset("Empty fields", EXPOSURE, fields=[])
    with pytest.raises(RecipeError, match="Unsupported development setting"):
        db.save_edit_preset("Bad field", EXPOSURE, fields=["nope"])
    assert db.list_edit_presets() == []


@pytest.mark.parametrize("name", [None, 7, "", "   "])
def test_save_preset_rejects_blank_or_non_string_name(db, name):
    with pytest.raises(ValueError, match="preset name must not be blank"):
        db.save_edit_preset(name, EXPOSURE)


def test_save_preset_name_length_limit(db):
    assert db.EDIT_PRESET_NAME_MAX == 80
    assert db.save_edit_preset("x" * 80, EXPOSURE)["name"] == "x" * 80
    assert db.save_edit_preset(" " + "y" * 80 + " ", EXPOSURE)["name"] == "y" * 80
    with pytest.raises(ValueError, match="preset name must be 80 characters or fewer"):
        db.save_edit_preset("z" * 81, EXPOSURE)


def test_save_preset_name_limit_reads_instance_attribute(db, monkeypatch):
    monkeypatch.setattr(db, "EDIT_PRESET_NAME_MAX", 5)
    with pytest.raises(ValueError, match="preset name must be 5 characters or fewer"):
        db.save_edit_preset("sixsix", EXPOSURE)
    assert db.save_edit_preset("five5", EXPOSURE)["name"] == "five5"


def test_save_preset_accepts_json_string_recipe(db):
    saved = db.save_edit_preset(
        "From JSON", json.dumps({"rotation": 90, "adjustments": {"exposure": 0.5}})
    )
    assert saved["recipe"] == NORMALIZED_EXPOSURE


def test_save_preset_empty_string_recipe_needs_an_adjustment(db):
    with pytest.raises(ValueError, match="preset must include at least one adjustment"):
        db.save_edit_preset("Blank", "")


def test_save_preset_malformed_string_recipe_raises(db):
    with pytest.raises(RecipeError, match="recipe must be valid JSON"):
        db.save_edit_preset("Broken", "{")


@pytest.mark.parametrize("recipe", [[1], 3, None])
def test_save_preset_non_object_recipe_raises(db, recipe):
    with pytest.raises(RecipeError, match="recipe must be an object"):
        db.save_edit_preset("Not an object", recipe)


def test_save_preset_upsert_keeps_id_and_bumps_recipe(db):
    first = db.save_edit_preset("Same", EXPOSURE)
    second = db.save_edit_preset("Same", {"adjustments": {"shadows": 30}})
    assert second["id"] == first["id"]
    assert second["recipe"]["adjustments"] == {"shadows": 30.0}
    assert len(db.list_edit_presets()) == 1


def test_list_presets_skips_malformed_rows(db, caplog):
    _raw_preset(db, "Broken", "{")
    _raw_preset(db, "Bad fields", json.dumps({"recipe": {}, "fields": ["nope"]}))
    good = db.save_edit_preset("good", EXPOSURE)
    with caplog.at_level("WARNING"):
        assert db.list_edit_presets() == [good]
    assert "Invalid stored edit preset" in caplog.text
    assert "'Broken'" in caplog.text
    assert "'Bad fields'" in caplog.text


def test_list_presets_sorts_casefolded_and_omits_fields_for_legacy(db):
    for name in ("beta", "Alpha", "gamma", "ALPHA2"):
        db.save_edit_preset(name, EXPOSURE)
    listed = db.list_edit_presets()
    assert [p["name"] for p in listed] == ["Alpha", "ALPHA2", "beta", "gamma"]
    assert all(list(p) == ["id", "name", "recipe", "updated_at"] for p in listed)


def test_delete_preset_commits_and_reports_removal(db):
    saved = db.save_edit_preset("Doomed", EXPOSURE)
    assert db.delete_edit_preset(saved["id"]) is True
    assert not db.conn.in_transaction
    with _reader(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM edit_presets").fetchone()[0] == 0
    assert db.delete_edit_preset(saved["id"]) is False
    assert db.delete_edit_preset(12345) is False


# -- structure: the SQL lives in repositories/edits.py -------------------------

_DELEGATED = [
    "get_photo_edit_recipe",
    "get_photo_edit_recipes",
    "set_photo_edit_recipe",
    "clear_photo_edit_recipe",
    "list_edit_presets",
    "save_edit_preset",
    "delete_edit_preset",
]


@pytest.mark.parametrize("name", _DELEGATED)
def test_edits_method_delegates_to_repository(name):
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
        f"Database.{name} touches self.conn; move the SQL to EditsRepository"
    )
    assert "_edits_repository" in attrs, (
        f"Database.{name} no longer delegates to EditsRepository"
    )


def test_edits_facade_signatures_are_unchanged():
    def params(name):
        return [
            (p.name, p.default)
            for p in inspect.signature(getattr(Database, name)).parameters.values()
        ]

    empty = inspect.Parameter.empty
    assert params("get_photo_edit_recipe") == [
        ("self", empty), ("photo_id", empty), ("verify_workspace", False),
    ]
    assert params("set_photo_edit_recipe") == [
        ("self", empty), ("photo_id", empty), ("recipe", empty),
        ("verify_workspace", True), ("_commit", True),
    ]
    assert params("clear_photo_edit_recipe") == [
        ("self", empty), ("photo_id", empty), ("verify_workspace", True),
    ]
    assert params("save_edit_preset") == [
        ("self", empty), ("name", empty), ("recipe", empty), ("fields", None),
    ]


def test_edits_repository_builds_without_an_active_workspace(db):
    db.set_active_workspace(None)
    repo = db._edits_repository()
    assert repo.conn is db.conn
    assert repo.preset_name_max == Database.EDIT_PRESET_NAME_MAX
