"""Extended behavior pins for the photo-review domain of ``Database``.

Complements ``test_db_photo_review.py`` (ratings/flags basics) with the
wildlife-excluded bit, color labels and their descriptions, commit
visibility, and a structural check on the kept wildlife-exclusion write.

The behavior tests go through the ``db.photo_review`` and ``db.photo_labels``
accessors (and the visibility filter through ``db.photo_visibility``), plus
``Database.update_photo_wildlife_excluded``, the one coordinated write kept
on the façade. The accessor-shape tests live in ``test_db_photo_review.py``
and ``test_db_photo_labels.py``.
"""

import ast
import inspect
import json
import sqlite3
import textwrap

import db as db_module
import pytest
from db import Database


def _reader(db):
    """A second connection: sees only what the façade has committed."""
    conn = sqlite3.connect(db._db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _column(db, photo_id, column):
    with _reader(db) as other:
        return other.execute(
            f"SELECT {column} FROM photos WHERE id = ?", (photo_id,)
        ).fetchone()[0]


def _labels_table(db):
    with _reader(db) as other:
        return sorted(
            tuple(row)
            for row in other.execute(
                "SELECT photo_id, workspace_id, color FROM photo_color_labels"
            )
        )


def _raw_overrides(db, workspace_id, value):
    db.conn.execute(
        "UPDATE workspaces SET config_overrides = ? WHERE id = ?",
        (value, workspace_id),
    )
    db.conn.commit()


def _stored_overrides(db, workspace_id):
    with _reader(db) as other:
        return other.execute(
            "SELECT config_overrides FROM workspaces WHERE id = ?",
            (workspace_id,),
        ).fetchone()[0]


def _add_photos(db, folder_id, count, prefix="p"):
    return [
        db.add_photo(
            folder_id=folder_id,
            filename=f"{prefix}{i}.jpg",
            extension=".jpg",
            file_size=100,
            file_mtime=1.0,
        )
        for i in range(count)
    ]


@pytest.fixture
def photos(db):
    """Three photos visible in the active workspace."""
    folder_id = db.add_folder("/photos", name="photos")
    return _add_photos(db, folder_id, 3)


@pytest.fixture
def outsider(db):
    """A photo in a folder the active workspace does not see."""
    active = db._ws_id()
    other_ws = db.create_workspace("Other")
    db.set_active_workspace(other_ws)
    folder_id = db.add_folder("/elsewhere", name="elsewhere")
    (photo_id,) = _add_photos(db, folder_id, 1, prefix="out")
    db.set_active_workspace(active)
    return photo_id


# -- rating -------------------------------------------------------------------


def test_update_photo_rating_writes_and_commits(db, photos):
    db.photo_review.set_rating(photos[0], 4)
    assert not db.conn.in_transaction
    assert _column(db, photos[0], "rating") == 4
    assert _column(db, photos[1], "rating") == 0


def test_update_photo_rating_rejects_photo_outside_workspace(db, outsider):
    with pytest.raises(
        ValueError,
        match=f"Photo {outsider} does not belong to the active workspace",
    ):
        db.photo_review.set_rating(outsider, 5)
    assert _column(db, outsider, "rating") == 0


def test_update_photo_rating_skips_check_when_asked(db, outsider):
    db.photo_review.set_rating(outsider, 3, verify_workspace=False)
    assert _column(db, outsider, "rating") == 3


def test_update_photo_rating_without_workspace(db, photos):
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError, match="No active workspace set"):
        db.photo_review.set_rating(photos[0], 2)
    # The unverified path never needs the active workspace.
    db.photo_review.set_rating(photos[0], 2, verify_workspace=False)
    assert _column(db, photos[0], "rating") == 2


def test_batch_update_photo_rating_writes_all_and_commits(db, photos):
    db.photo_review.set_ratings(photos[:2], 5)
    assert not db.conn.in_transaction
    assert [_column(db, pid, "rating") for pid in photos] == [5, 5, 0]


def test_batch_update_photo_rating_empty_is_a_noop(db, photos):
    db.set_active_workspace(None)
    db.photo_review.set_ratings([], 5)
    assert not db.conn.in_transaction
    assert [_column(db, pid, "rating") for pid in photos] == [0, 0, 0]


def test_batch_update_photo_rating_verifies_before_writing(db, photos, outsider):
    with pytest.raises(ValueError, match="does not belong to the active workspace"):
        db.photo_review.set_ratings([photos[0], outsider], 4)
    assert _column(db, photos[0], "rating") == 0
    db.photo_review.set_ratings([photos[0], outsider], 4, verify_workspace=False)
    assert _column(db, photos[0], "rating") == 4
    assert _column(db, outsider, "rating") == 4


def test_batch_update_photo_rating_chunks(db, monkeypatch):
    folder_id = db.add_folder("/many", name="many")
    ids = _add_photos(db, folder_id, 5)
    monkeypatch.setattr(db_module, "_SQLITE_PARAM_CHUNK_SIZE", 2)
    db.photo_review.set_ratings(ids, 1)
    assert [_column(db, pid, "rating") for pid in ids] == [1] * 5


def test_batch_update_photo_rating_rolls_back_partial_chunks(db, monkeypatch):
    folder_id = db.add_folder("/many", name="many")
    ids = _add_photos(db, folder_id, 3)
    monkeypatch.setattr(db_module, "_SQLITE_PARAM_CHUNK_SIZE", 1)
    db.conn.execute(
        "CREATE TEMP TRIGGER no_rating BEFORE UPDATE OF rating ON photos "
        f"WHEN NEW.id = {ids[2]} BEGIN SELECT RAISE(ABORT, 'blocked'); END"
    )
    with pytest.raises(sqlite3.IntegrityError, match="blocked"):
        db.photo_review.set_ratings(ids, 3)
    assert not db.conn.in_transaction
    # The first two chunks were written, then rolled back.
    assert [_column(db, pid, "rating") for pid in ids] == [0, 0, 0]


# -- flag ---------------------------------------------------------------------


def test_update_photo_flag_writes_and_commits(db, photos):
    db.photo_review.set_flag(photos[0], "flagged")
    assert not db.conn.in_transaction
    assert _column(db, photos[0], "flag") == "flagged"


def test_update_photo_flag_can_leave_commit_to_caller(db, photos):
    db.photo_review.set_flag(photos[0], "rejected", _commit=False)
    assert db.conn.in_transaction
    assert _column(db, photos[0], "flag") == "none"
    db.conn.commit()
    assert _column(db, photos[0], "flag") == "rejected"


def test_update_photo_flag_rejects_photo_outside_workspace(db, outsider):
    with pytest.raises(ValueError, match="does not belong to the active workspace"):
        db.photo_review.set_flag(outsider, "flagged")
    assert _column(db, outsider, "flag") == "none"
    db.photo_review.set_flag(outsider, "flagged", verify_workspace=False)
    assert _column(db, outsider, "flag") == "flagged"


def test_batch_update_photo_flag_writes_all_and_commits(db, photos):
    db.photo_review.set_flags(photos[1:], "rejected")
    assert not db.conn.in_transaction
    assert [_column(db, pid, "flag") for pid in photos] == [
        "none", "rejected", "rejected",
    ]


def test_batch_update_photo_flag_verifies_before_writing(db, photos, outsider):
    with pytest.raises(ValueError, match="does not belong to the active workspace"):
        db.photo_review.set_flags([photos[0], outsider], "flagged")
    assert _column(db, photos[0], "flag") == "none"
    db.photo_review.set_flags([outsider], "flagged", verify_workspace=False)
    assert _column(db, outsider, "flag") == "flagged"


def test_batch_update_photo_flag_empty_is_a_noop(db, photos):
    db.photo_review.set_flags([], "flagged")
    assert not db.conn.in_transaction
    assert [_column(db, pid, "flag") for pid in photos] == ["none"] * 3


# -- wildlife excluded --------------------------------------------------------


@pytest.mark.parametrize(
    "value, stored",
    [(True, 1), (False, 0), ("yes", 1), (0, 0), (None, 0), (2, 1)],
)
def test_update_photo_wildlife_excluded_stores_a_bool(db, photos, value, stored):
    db.conn.execute("UPDATE photos SET wildlife_excluded = 7 WHERE id = ?", (photos[0],))
    db.conn.commit()
    db.update_photo_wildlife_excluded(photos[0], value)
    assert not db.conn.in_transaction
    assert _column(db, photos[0], "wildlife_excluded") == stored
    assert _column(db, photos[1], "wildlife_excluded") == 0


def test_update_photo_wildlife_excluded_rejects_photo_outside_workspace(db, outsider):
    with pytest.raises(
        ValueError,
        match=f"Photo {outsider} does not belong to the active workspace",
    ):
        db.update_photo_wildlife_excluded(outsider, True)
    assert _column(db, outsider, "wildlife_excluded") == 0
    db.update_photo_wildlife_excluded(outsider, True, verify_workspace=False)
    assert _column(db, outsider, "wildlife_excluded") == 1


def test_update_photo_wildlife_excluded_without_workspace(db, photos):
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError, match="No active workspace set"):
        db.update_photo_wildlife_excluded(photos[0], True)
    assert _column(db, photos[0], "wildlife_excluded") == 0
    db.update_photo_wildlife_excluded(photos[0], True, verify_workspace=False)
    assert _column(db, photos[0], "wildlife_excluded") == 1


def test_update_photo_wildlife_excluded_verifies_through_the_scope_guard(
    db, photos, monkeypatch
):
    seen = []
    monkeypatch.setattr(db, "_verify_photo_in_workspace", seen.append)
    db.update_photo_wildlife_excluded(photos[0], True)
    assert seen == [photos[0]]
    db.update_photo_wildlife_excluded(photos[1], True, verify_workspace=False)
    assert seen == [photos[0]]


def test_update_photo_wildlife_excluded_can_leave_the_write_uncommitted(db, photos):
    db.update_photo_wildlife_excluded(
        photos[0], True, verify_workspace=False, _commit=False,
    )
    assert db.conn.in_transaction
    assert _column(db, photos[0], "wildlife_excluded") == 0
    db.conn.commit()
    assert _column(db, photos[0], "wildlife_excluded") == 1


def test_get_wildlife_excluded_states_reads_visible_photos(db, photos, outsider):
    db.update_photo_wildlife_excluded(photos[1], True)
    assert db.photo_review.wildlife_excluded_states(
        [photos[0], photos[1], outsider, 987_654]
    ) == {photos[0]: 0, photos[1]: 1}
    assert db.photo_review.wildlife_excluded_states([]) == {}


def test_get_wildlife_excluded_states_chunks(db, photos, monkeypatch):
    monkeypatch.setattr(db_module, "_SQLITE_PARAM_CHUNK_SIZE", 2)
    statements = []
    db.conn.set_trace_callback(statements.append)
    states = db.photo_review.wildlife_excluded_states(photos)
    db.conn.set_trace_callback(None)
    assert states == {pid: 0 for pid in photos}
    assert len([s for s in statements if "wildlife_excluded, 0)" in s]) == 2


def test_get_wildlife_excluded_states_without_workspace(db, photos):
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError, match="No active workspace set"):
        db.photo_review.wildlife_excluded_states(photos)


# -- color labels -------------------------------------------------------------


def test_color_label_set_get_remove_commits(db, photos):
    ws = db._ws_id()
    assert db.photo_labels.get(photos[0]) is None
    db.photo_labels.set(photos[0], "red")
    assert not db.conn.in_transaction
    assert _labels_table(db) == [(photos[0], ws, "red")]
    db.photo_labels.set(photos[0], "blue")
    assert db.photo_labels.get(photos[0]) == "blue"
    assert _labels_table(db) == [(photos[0], ws, "blue")]
    db.photo_labels.remove(photos[0])
    assert not db.conn.in_transaction
    assert db.photo_labels.get(photos[0]) is None
    assert _labels_table(db) == []


def test_set_color_label_rejects_unknown_color(db, photos):
    with pytest.raises(ValueError, match=r"Invalid color label: pink\. Must be one of"):
        db.photo_labels.set(photos[0], "pink")
    assert _labels_table(db) == []


def test_color_labels_are_workspace_scoped(db, photos):
    first = db._ws_id()
    db.photo_labels.set(photos[0], "green")
    second = db.create_workspace("Second")
    db.set_active_workspace(second)
    assert db.photo_labels.get(photos[0]) is None
    assert db.photo_labels.get_for_photos(photos) == {}
    db.photo_labels.set(photos[0], "purple")
    db.photo_labels.remove(photos[0])
    db.set_active_workspace(first)
    assert db.photo_labels.get(photos[0]) == "green"


def test_color_label_methods_require_active_workspace(db, photos):
    db.set_active_workspace(None)
    calls = [
        lambda: db.photo_labels.set(photos[0], "red"),
        lambda: db.photo_labels.remove(photos[0]),
        lambda: db.photo_labels.get(photos[0]),
        lambda: db.photo_labels.get_for_photos([]),
        lambda: db.photo_visibility.visible_photo_ids([]),
        lambda: db.photo_labels.set_many([], "red"),
        lambda: db.photo_labels.get_descriptions(),
        lambda: db.photo_labels.set_description("red", "x"),
    ]
    for call in calls:
        with pytest.raises(RuntimeError, match="No active workspace set"):
            call()


def test_get_color_labels_for_photos(db, photos, monkeypatch):
    assert db.photo_labels.get_for_photos([]) == {}
    db.photo_labels.set(photos[0], "red")
    db.photo_labels.set(photos[2], "yellow")
    monkeypatch.setattr(db_module, "_SQLITE_PARAM_CHUNK_SIZE", 1)
    labels = db.photo_labels.get_for_photos(photos + [999_999])
    assert labels == {photos[0]: "red", photos[2]: "yellow"}
    assert isinstance(labels, dict)


def test_filter_photo_ids_in_workspace_keeps_order_and_dedupes(
    db, photos, outsider, monkeypatch
):
    monkeypatch.setattr(db_module, "_SQLITE_PARAM_CHUNK_SIZE", 2)
    requested = [photos[2], outsider, photos[0], 999_999, photos[2], photos[1]]
    assert db.photo_visibility.visible_photo_ids(requested) == [
        photos[2], photos[0], photos[1],
    ]
    assert db.photo_visibility.visible_photo_ids([]) == []
    assert db.photo_visibility.visible_photo_ids(iter([photos[1]])) == [photos[1]]


def test_batch_set_color_label_sets_and_clears(db, photos, monkeypatch):
    ws = db._ws_id()
    monkeypatch.setattr(db_module, "_SQLITE_PARAM_CHUNK_SIZE", 2)
    db.photo_labels.set_many(photos, "green")
    assert not db.conn.in_transaction
    assert _labels_table(db) == [(pid, ws, "green") for pid in sorted(photos)]
    db.photo_labels.set_many(photos[:1], "red")
    assert db.photo_labels.get_for_photos(photos) == {
        photos[0]: "red", photos[1]: "green", photos[2]: "green",
    }
    db.photo_labels.set_many(photos, None)
    assert not db.conn.in_transaction
    assert _labels_table(db) == []


def test_batch_set_color_label_clear_leaves_other_workspaces(db, photos):
    first = db._ws_id()
    db.photo_labels.set_many(photos, "blue")
    second = db.create_workspace("Second")
    db.set_active_workspace(second)
    db.photo_labels.set_many(photos, None)
    db.set_active_workspace(first)
    assert db.photo_labels.get_for_photos(photos) == dict.fromkeys(photos, "blue")


def test_batch_set_color_label_empty_and_invalid(db, photos):
    db.photo_labels.set_many([], "not-a-color")
    assert not db.conn.in_transaction
    with pytest.raises(ValueError, match=r"Invalid color label: pink\. Must be one of"):
        db.photo_labels.set_many(photos, "pink")
    assert _labels_table(db) == []


# -- color label descriptions -------------------------------------------------


@pytest.mark.parametrize(
    "raw",
    [
        None,
        "",
        "{",
        "[1]",
        '"text"',
        json.dumps({"other": 1}),
        json.dumps({"color_label_descriptions": ["red"]}),
        json.dumps({"color_label_descriptions": "red"}),
    ],
)
def test_get_color_label_descriptions_falls_back_to_empty(db, raw):
    _raw_overrides(db, db._ws_id(), raw)
    assert db.photo_labels.get_descriptions() == {}


def test_get_color_label_descriptions_filters_and_strips(db):
    _raw_overrides(
        db,
        db._ws_id(),
        json.dumps({
            "color_label_descriptions": {
                "red": "  Keepers ",
                "blue": "   ",
                "pink": "not a label",
                "green": 5,
                "yellow": "Maybe",
            }
        }),
    )
    assert db.photo_labels.get_descriptions() == {
        "red": "Keepers",
        "yellow": "Maybe",
    }


def test_get_color_label_descriptions_missing_workspace_row(db):
    ghost = db.create_workspace("Ghost")
    db.set_active_workspace(ghost)
    db.conn.execute("DELETE FROM workspaces WHERE id = ?", (ghost,))
    db.conn.commit()
    assert db.photo_labels.get_descriptions() == {}


def test_set_color_label_description_normalizes_and_commits(db):
    ws = db._ws_id()
    _raw_overrides(db, ws, json.dumps({"keep": True}))
    result = db.photo_labels.set_description("red", "  Best \n  of   day ")
    assert result == "Best of day"
    assert not db.conn.in_transaction
    assert json.loads(_stored_overrides(db, ws)) == {
        "keep": True,
        "color_label_descriptions": {"red": "Best of day"},
    }
    assert db.photo_labels.get_descriptions() == {"red": "Best of day"}


def test_set_color_label_description_clearing_drops_key_and_nulls_column(db):
    ws = db._ws_id()
    db.photo_labels.set_description("red", "Keepers")
    db.photo_labels.set_description("blue", "Edit")
    assert db.photo_labels.set_description("red", "   ") == ""
    assert json.loads(_stored_overrides(db, ws)) == {
        "color_label_descriptions": {"blue": "Edit"},
    }
    assert db.photo_labels.set_description("blue", "") == ""
    assert _stored_overrides(db, ws) is None
    # Clearing a color that was never set is harmless.
    assert db.photo_labels.set_description("green", "") == ""
    assert _stored_overrides(db, ws) is None


def test_set_color_label_description_keeps_other_overrides_when_clearing(db):
    ws = db._ws_id()
    _raw_overrides(
        db, ws, json.dumps({"x": 1, "color_label_descriptions": {"red": "a"}})
    )
    db.photo_labels.set_description("red", "")
    assert json.loads(_stored_overrides(db, ws)) == {"x": 1}


@pytest.mark.parametrize(
    "raw",
    ["{", "[1]", '"text"', json.dumps({"color_label_descriptions": ["x"]})],
)
def test_set_color_label_description_replaces_malformed_overrides(db, raw):
    ws = db._ws_id()
    _raw_overrides(db, ws, raw)
    db.photo_labels.set_description("purple", "Share")
    stored = json.loads(_stored_overrides(db, ws))
    assert stored["color_label_descriptions"] == {"purple": "Share"}


def test_set_color_label_description_validation(db):
    ws = db._ws_id()
    with pytest.raises(ValueError, match=r"Invalid color label: pink\. Must be one of"):
        db.photo_labels.set_description("pink", "x")
    with pytest.raises(ValueError, match="description must be a string"):
        db.photo_labels.set_description("red", None)
    assert db.photo_labels.set_description("red", "x" * 120) == "x" * 120
    with pytest.raises(ValueError, match="description must be 120 characters or fewer"):
        db.photo_labels.set_description("red", "y" * 121)
    assert db.photo_labels.get_descriptions() == {"red": "x" * 120}
    assert json.loads(_stored_overrides(db, ws)) == {
        "color_label_descriptions": {"red": "x" * 120},
    }


def test_valid_color_labels_constant_on_database():
    assert Database.VALID_COLOR_LABELS == (
        "red", "yellow", "green", "blue", "purple",
    )


# -- structure ------------------------------------------------------------------

def test_wildlife_excluded_write_stays_a_coordinated_database_method():
    """``update_photo_wildlife_excluded`` runs the façade's workspace check first.

    It is the one write in these domains that stays on ``Database``: it checks
    ``_verify_photo_in_workspace`` and then hands the SQL to the review
    repository, never to the connection.
    """
    source = textwrap.dedent(
        inspect.getsource(Database.update_photo_wildlife_excluded)
    )
    attrs = {
        node.attr
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }
    assert "conn" not in attrs
    assert {"_verify_photo_in_workspace", "_photo_review_repository"} <= attrs


def test_production_code_uses_facade_for_the_wildlife_exclusion_write():
    """Nothing outside the data layer skips ``update_photo_wildlife_excluded``'s check."""
    import re
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    pattern = re.compile(r"\.photo_review\.set_wildlife_excluded\(")
    offenders = []
    for path in sorted(root.rglob("*.py")):
        rel = path.relative_to(root)
        if rel.parts[0] in ("tests", "repositories") or rel.name == "db.py":
            continue
        text = path.read_text(encoding="utf-8")
        offenders += [f"{rel}: {m.group(0)}" for m in pattern.finditer(text)]
    assert offenders == [], (
        f"Call db.update_photo_wildlife_excluded instead: {offenders}"
    )
