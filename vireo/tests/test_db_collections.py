"""Behavior pins for the collections domain of ``Database``.

The behavior tests exercise collection CRUD, the smart-collection rules
engine (``_build_query_from_rules``), the rule-driven photo queries, the
stacked Browse projection, filter-value suggestions, and the
default-collection migrations only through the ``Database`` façade, so they
hold whether the SQL lives in ``db.py`` or in ``repositories/collections.py``;
the structural tests at the end keep it in the repository.
"""

import ast
import contextlib
import inspect
import json
import sqlite3
import textwrap

import config as cfg
import pytest
from db import (
    GPS_WITHOUT_LOCATION_KEYWORD_RULES,
    NEEDS_IDENTIFICATION_RULES,
    Database,
)
from services import browse_queries, photo_ingest, workspace_folders


@pytest.fixture(autouse=True)
def isolated_config(tmp_path, monkeypatch):
    # Prediction predicates read the workspace-effective detector floor
    # through ``cfg.load()``; keep that off the real ~/.vireo config.
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))


def _reader(db):
    """A second connection: sees only what the façade has committed."""
    conn = sqlite3.connect(db._db_path)
    conn.row_factory = sqlite3.Row
    return contextlib.closing(conn)


@pytest.fixture
def folder(db, tmp_path):
    return workspace_folders.add_folder(db, str(tmp_path / "photos"), name="photos")


def _photo(db, folder_id, filename, **cols):
    pid = photo_ingest.add_photo(
        db, folder_id=folder_id, filename=filename, extension=".jpg",
        file_size=100, file_mtime=1.0,
    )
    if cols:
        assignments = ", ".join(f"{k} = ?" for k in cols)
        db.conn.execute(
            f"UPDATE photos SET {assignments} WHERE id = ?", [*cols.values(), pid]
        )
        db.conn.commit()
    return pid


def _ids(db, rules, **kwargs):
    return db.collections.query_photo_ids(rules, sort="name", **kwargs)


def _prediction(db, photo_id, *, confidence=0.9, taxonomy=None, status="pending"):
    det_ids = db.detections.save(
        photo_id,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.8, "h": 0.8}, "confidence": 0.95}],
        detector_model="MegaDetector",
    )
    db.predictions.add(
        det_ids[0], species="Vulpes vulpes", confidence=confidence,
        model="bioclip-2.5", category="match", taxonomy=taxonomy,
        status=status,
    )


# -- CRUD ------------------------------------------------------------------


def test_add_collection_commits_and_returns_id(db):
    cid = db.collections.add("Best", json.dumps([{"field": "all"}]), '{"prompt": "x"}')
    db.commit()
    with _reader(db) as other:
        row = other.execute(
            "SELECT name, rules, workspace_id, visual_json FROM collections WHERE id = ?",
            (cid,),
        ).fetchone()
    assert dict(row) == {
        "name": "Best",
        "rules": '[{"field": "all"}]',
        "workspace_id": db._active_workspace_id,
        "visual_json": '{"prompt": "x"}',
    }


def test_get_collections_is_workspace_scoped_and_name_ordered(db):
    db.collections.add("b", "[]")
    db.commit()
    db.collections.add("a", "[]", "{}")
    db.commit()
    other_ws = db.create_workspace("other")
    db.set_active_workspace(other_ws)
    db.collections.add("elsewhere", "[]")
    db.commit()
    db.set_active_workspace(1)
    rows = db.collections.list_all()
    assert [tuple(r.keys()) for r in rows] == [("id", "name", "rules", "visual_json")] * 2
    assert [(r["name"], r["visual_json"]) for r in rows] == [("a", "{}"), ("b", None)]


def test_delete_collection_commits_and_ignores_other_workspace(db):
    keep = db.collections.add("keep", "[]")
    db.commit()
    other_ws = db.create_workspace("other")
    db.set_active_workspace(other_ws)
    foreign = db.collections.add("foreign", "[]")
    db.commit()
    db.set_active_workspace(1)
    db.collections.delete(keep)
    db.commit()
    db.collections.delete(foreign)
    db.commit()
    with _reader(db) as other:
        names = [r["name"] for r in other.execute("SELECT name FROM collections")]
    assert names == ["foreign"]


def test_rename_collection_commits(db):
    cid = db.collections.add("old", "[]")
    db.commit()
    db.collections.rename(cid, "new")
    db.commit()
    assert not db.conn.in_transaction
    with _reader(db) as other:
        assert other.execute(
            "SELECT name FROM collections WHERE id = ?", (cid,)
        ).fetchone()["name"] == "new"


def test_rename_collection_missing_or_foreign_raises(db):
    other_ws = db.create_workspace("other")
    db.set_active_workspace(other_ws)
    foreign = db.collections.add("foreign", "[]")
    db.commit()
    db.set_active_workspace(1)
    with pytest.raises(ValueError, match="^collection not found$"):
        db.collections.rename(foreign, "stolen")
        db.commit()
    with pytest.raises(ValueError, match="^collection not found$"):
        db.collections.rename(9999, "x")
        db.commit()
    db.conn.rollback()
    assert db.conn.execute(
        "SELECT name FROM collections WHERE id = ?", (foreign,)
    ).fetchone()["name"] == "foreign"


def test_rename_collection_needs_active_workspace(db):
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError):
        db.collections.rename(1, "x")
        db.commit()


def test_duplicate_collection_names_copies_incrementally(db):
    cid = db.collections.add("Birds", '[{"field": "all"}]', '{"prompt": "owl"}')
    db.commit()
    first = db.collections.duplicate(cid)
    db.commit()
    second = db.collections.duplicate(cid)
    db.commit()
    third = db.collections.duplicate(cid)
    db.commit()
    with _reader(db) as other:
        rows = {
            r["id"]: dict(r) for r in other.execute(
                "SELECT id, name, rules, visual_json FROM collections"
            )
        }
    assert [rows[i]["name"] for i in (first, second, third)] == [
        "Birds (copy)", "Birds (copy 2)", "Birds (copy 3)",
    ]
    assert {rows[i]["rules"] for i in (first, second, third)} == {'[{"field": "all"}]'}
    assert {rows[i]["visual_json"] for i in (first, second, third)} == {'{"prompt": "owl"}'}


def test_duplicate_collection_missing_raises(db):
    with pytest.raises(ValueError, match="^collection not found$"):
        db.collections.duplicate(9999)
        db.commit()


def test_get_collections_for_picker_orders_case_insensitively_then_by_id(db):
    b = db.collections.add("b", "[]")
    db.commit()
    upper_a = db.collections.add("A", "[]", "{}")
    db.commit()
    lower_a = db.collections.add("a", "[]")
    db.commit()
    other_ws = db.create_workspace("other")
    db.set_active_workspace(other_ws)
    db.collections.add("elsewhere", "[]")
    db.commit()
    db.set_active_workspace(1)
    rows = db.collections.list_for_picker()
    assert [tuple(r.keys()) for r in rows] == [("id", "name", "rules", "visual_json")] * 3
    # get_collections() would put "A" before "a" before "b" by binary order;
    # here "A"/"a" tie on NOCASE and fall back to id.
    assert [r["id"] for r in rows] == [upper_a, lower_a, b]
    assert rows[0]["visual_json"] == "{}"


def test_get_collection_is_workspace_scoped(db):
    cid = db.collections.add("Mine", '[{"field": "all"}]', '{"prompt": "owl"}')
    db.commit()
    other_ws = db.create_workspace("other")
    db.set_active_workspace(other_ws)
    foreign = db.collections.add("foreign", "[]")
    db.commit()
    db.set_active_workspace(1)
    assert dict(db.collections.get(cid)) == {
        "id": cid, "name": "Mine", "rules": '[{"field": "all"}]',
        "visual_json": '{"prompt": "owl"}',
    }
    assert db.collections.get(foreign) is None
    assert db.collections.get(9999) is None
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError):
        db.collections.get(cid)


def test_update_collection_writes_only_the_given_columns_and_commits(db):
    cid = db.collections.add("old", '[{"field": "all"}]', '{"prompt": "owl"}')
    db.commit()

    def stored():
        with _reader(db) as other:
            return dict(other.execute(
                "SELECT name, rules, visual_json FROM collections WHERE id = ?",
                (cid,),
            ).fetchone())

    db.collections.update(cid, name="new")
    db.commit()
    assert not db.in_transaction
    assert stored() == {
        "name": "new", "rules": '[{"field": "all"}]', "visual_json": '{"prompt": "owl"}',
    }
    db.collections.update(cid, rules_json="[]")
    db.commit()
    assert stored()["rules"] == "[]"
    # An explicit None clears the visual clause; omitting it leaves it alone.
    db.collections.update(cid, name="newer", rules_json='[{"field": "flag"}]', visual_json=None)
    db.commit()
    assert stored() == {"name": "newer", "rules": '[{"field": "flag"}]', "visual_json": None}


def test_update_collection_with_nothing_runs_no_statement(db):
    cid = db.collections.add("old", "[]")
    db.commit()
    statements = []
    db.conn.set_trace_callback(statements.append)
    try:
        db.collections.update(cid)
        db.commit()
    finally:
        db.conn.set_trace_callback(None)
    assert statements == []


def test_update_collection_ignores_other_workspace(db):
    other_ws = db.create_workspace("other")
    db.set_active_workspace(other_ws)
    foreign = db.collections.add("foreign", "[]")
    db.commit()
    db.set_active_workspace(1)
    db.collections.update(foreign, name="stolen", rules_json='[{"field": "all"}]')
    db.commit()
    with _reader(db) as other:
        row = other.execute(
            "SELECT name, rules FROM collections WHERE id = ?", (foreign,)
        ).fetchone()
    assert dict(row) == {"name": "foreign", "rules": "[]"}
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError):
        db.collections.update(foreign, name="x")
        db.commit()


# -- Rules engine ------------------------------------------------------------


def test_rules_engine_rejects_malformed_trees(db):
    bad = [
        ({"mode": "some", "rules": []}, "rule group mode must be all, any, or none"),
        ({"field": "keyword_identity", "op": "contains", "value": "x"}, "keyword_identity requires"),
        ({"field": "keyword_identity", "op": "equals", "value": ""}, "keyword_identity requires"),
        ({"field": "filename", "op": "matches", "value": "x"}, "unsupported collection rule field/op: filename/matches"),
    ]
    for rule, message in bad:
        rules = rule if "mode" in rule else [rule]
        assert browse_queries.rules_resolvable(db, rules) is False
        with pytest.raises(ValueError, match=message):
            db.collections.count_photos_for_rules(rules)


@pytest.mark.parametrize("value, op", [
    ('{"name": "Verdin", "taxon_id": null}', "contains"),
    ("{not json", "equals"),
    ('["name", "taxon_id"]', "equals"),
    ('{"name": "Verdin"}', "equals"),
    ('{"name": "", "taxon_id": null}', "equals"),
    ('{"name": 3, "taxon_id": null}', "equals"),
    ('{"name": "Verdin", "taxon_id": true}', "equals"),
    ('{"name": "Verdin", "taxon_id": "7"}', "equals"),
])
def test_life_list_uncounted_rejects_invalid_tokens(db, value, op):
    rules = [{"field": "life_list_uncounted", "op": op, "value": value}]
    with pytest.raises(ValueError, match="^invalid Life List identification filter$"):
        db.collections.count_photos_for_rules(rules)


def test_life_list_uncounted_wraps_json_error(db):
    rules = [{"field": "life_list_uncounted", "op": "equals", "value": "{bad"}]
    with pytest.raises(ValueError) as excinfo:
        db.collections.count_photos_for_rules(rules)
    assert isinstance(excinfo.value.__cause__, json.JSONDecodeError)


def test_boolean_is_not_flips_the_predicate(db, folder):
    with_gps = _photo(db, folder, "a.jpg", latitude=1.0, longitude=2.0)
    without = _photo(db, folder, "b.jpg")
    assert _ids(db, [{"field": "has_gps", "op": "is not", "value": True}]) == [without]
    assert _ids(db, [{"field": "has_gps", "op": "is not", "value": False}]) == [with_gps]


def test_case_sensitive_text_rules(db, folder):
    upper = _photo(db, folder, "Heron.JPG")
    lower = _photo(db, folder, "heron.jpg")
    both = [upper, lower]

    def run(op, value):
        return _ids(db, [{"field": "filename", "op": op, "value": value, "case": True}])

    assert run("starts_with", "Her") == [upper]
    assert run("ends_with", ".JPG") == [upper]
    assert run("starts_with", "") == both
    assert run("is not", "Heron.JPG") == [lower]
    assert run("equals", "heron.jpg") == [lower]
    assert run("contains", "ron.J") == [upper]


def test_photo_ids_rule_binds_non_integer_ids(db, folder):
    a = _photo(db, folder, "a.jpg")
    b = _photo(db, folder, "b.jpg")
    _photo(db, folder, "c.jpg")
    assert _ids(db, [{"field": "photo_ids", "value": [str(b)]}]) == [b]
    assert _ids(db, [{"field": "photo_ids", "value": [a, str(b)]}]) == [a, b]
    sql = db._build_query_from_rules([{"field": "photo_ids", "value": [a, str(b)]}])
    assert sql[2] == f"WHERE ((p.id IN ({a}) OR p.id IN (?)))"
    assert sql[3] == [db._active_workspace_id, str(b)]


def test_empty_in_lists_become_constants(db, folder):
    a = _photo(db, folder, "a.jpg")
    for field in ("color_label", "extension", "prediction_status"):
        assert _ids(db, [{"field": field, "op": "in", "value": []}]) == []
        assert _ids(db, [{"field": field, "op": "not_in", "value": []}]) == [a]


def test_wildlife_excluded_is_not(db, folder):
    excluded = _photo(db, folder, "a.jpg", wildlife_excluded=1)
    kept = _photo(db, folder, "b.jpg")
    assert _ids(db, [{"field": "wildlife_excluded", "op": "is not", "value": True}]) == [kept]
    assert _ids(db, [{"field": "wildlife_excluded", "op": "is not", "value": False}]) == [excluded]


def test_taxonomy_rules_is_not_and_contains(db, folder):
    fox = _photo(db, folder, "fox.jpg")
    bare = _photo(db, folder, "bare.jpg")
    _prediction(db, fox, taxonomy={"family": "Canidae", "class": "Mammalia"})
    assert _ids(db, [{"field": "taxonomy_family", "op": "contains", "value": "nida"}]) == [fox]
    assert _ids(db, [{"field": "taxonomy_family", "op": "is not", "value": "Canidae"}]) == [bare]


def test_taxonomy_contains_treats_like_wildcards_literally(db, folder):
    fox = _photo(db, folder, "fox.jpg")
    odd = _photo(db, folder, "odd.jpg")
    _prediction(db, fox, taxonomy={"family": "Canidae", "genus": "axb"})
    _prediction(db, odd, taxonomy={"family": "100%_wild", "genus": "a_b"})
    assert _ids(db, [{"field": "taxonomy_family", "op": "contains", "value": "%"}]) == [odd]
    assert _ids(db, [{"field": "taxonomy_family", "op": "contains", "value": "_"}]) == [odd]
    assert _ids(db, [{"field": "taxonomy_genus", "op": "contains", "value": "a_b"}]) == [odd]


def test_taxonomy_contains_preserves_falsey_scalars(db, folder):
    """A falsey scalar (``0``, ``False``) must not collapse to ``LIKE '%%'``.

    ``value or ''`` would turn the operand into an empty string and match
    every classified photo — the exact unbounded match the LIKE escape is
    meant to block. Preserve the scalar's string form instead.
    """
    numeric = _photo(db, folder, "num.jpg")
    named = _photo(db, folder, "named.jpg")
    bare = _photo(db, folder, "bare.jpg")
    _prediction(db, numeric, taxonomy={"family": "Canidae0", "genus": "gen"})
    _prediction(db, named, taxonomy={"family": "Falseidae", "genus": "gen"})
    # ``0`` matches only the family literally containing ``0``.
    assert _ids(db, [{"field": "taxonomy_family", "op": "contains", "value": 0}]) == [numeric]
    # ``False`` matches only the family literally containing ``False``, not
    # every classified photo, and bare (no taxonomy at all) is excluded.
    matched = _ids(db, [{"field": "taxonomy_family", "op": "contains", "value": False}])
    assert matched == [named]
    assert bare not in matched


def test_active_mask_variant_contains_preserves_falsey_scalars(db, folder):
    """Same falsey-scalar guard for the active_mask_variant contains branch."""
    variant_zero = _photo(db, folder, "z.jpg", active_mask_variant="sam2-0-small")
    variant_named = _photo(db, folder, "n.jpg", active_mask_variant="Falsebranch")
    variant_plain = _photo(db, folder, "p.jpg", active_mask_variant="sam2-large")
    _photo(db, folder, "none.jpg")
    assert _ids(db, [{"field": "active_mask_variant", "op": "contains", "value": 0}]) == [variant_zero]
    matched = _ids(db, [{"field": "active_mask_variant", "op": "contains", "value": False}])
    assert matched == [variant_named]
    assert variant_plain not in matched


def test_remap_collection_photo_ids_follows_chains_and_folds_duplicates(db):
    from repositories.collections import remap_collection_photo_ids

    other_ws = db.create_workspace("Other")
    rows = {
        "chain": [{"field": "photo_ids", "value": [1, "2", 9]}],
        "dupes": [{"match": "any", "rules": [
            {"field": "photo_ids", "value": [3, 3, 1, 4]},
        ]}],
        "plain": [{"field": "rating", "op": ">=", "value": 3}],
    }
    ids = {}
    for name, rules in rows.items():
        ids[name] = db.conn.execute(
            "INSERT INTO collections (name, rules, workspace_id) VALUES (?, ?, ?)",
            (name, json.dumps(rules), other_ws),
        ).lastrowid
    # 1 -> 2 -> 4, and 2 itself is gone into 4; 9 is deleted outright.
    assert remap_collection_photo_ids(db.conn, {1: 2, 2: 4, 9: None}) == 2
    stored = {
        name: json.loads(db.conn.execute(
            "SELECT rules FROM collections WHERE id = ?", (cid,),
        ).fetchone()[0])
        for name, cid in ids.items()
    }
    assert stored["chain"] == [{"field": "photo_ids", "value": [4]}]
    # The pre-existing 3, 3 is left as saved; only the remap's 4 is folded.
    assert stored["dupes"][0]["rules"][0]["value"] == [3, 3, 4]
    assert stored["plain"] == rows["plain"]
    assert remap_collection_photo_ids(db.conn, {}) == 0


def test_remap_collection_photo_ids_normalizes_boolean_ids(db):
    """A rule value of ``True``/``False`` binds as 1/0 and must remap too.

    ``_is_scalar`` accepts booleans, and the ``photo_ids`` engine binds
    non-int values (booleans are excluded from the inline int list) so
    SQLite treats a bound ``True`` as ``p.id = 1``. Missing that spelling
    leaves a stale entry that silently rejoins the next photo to reuse id 1.
    """
    from repositories.collections import remap_collection_photo_ids

    other_ws = db.create_workspace("Other")
    rules = [{"field": "photo_ids", "value": [True, 1, False, 0, 2]}]
    cid = db.conn.execute(
        "INSERT INTO collections (name, rules, workspace_id) VALUES (?, ?, ?)",
        ("bools", json.dumps(rules), other_ws),
    ).lastrowid
    assert remap_collection_photo_ids(db.conn, {1: 7, 0: None}) == 1
    stored = json.loads(db.conn.execute(
        "SELECT rules FROM collections WHERE id = ?", (cid,),
    ).fetchone()[0])
    # Both spellings of id 1 (``True`` and ``1``) fold to 7; both spellings
    # of id 0 (``False`` and ``0``) drop; id 2 is untouched.
    assert stored == [{"field": "photo_ids", "value": [7, 2]}]


def test_remap_collection_photo_ids_normalizes_float_and_stringified_ids(db):
    """Non-int spellings SQLite's integer affinity still matches must remap.

    The rule validator permits any scalar (``_is_scalar`` accepts int, float,
    str, bool, None), and the ``photo_ids`` engine binds anything that isn't
    an ``int`` — so ``1.0``, ``"1"`` and ``"1.0"`` all match ``p.id = 1`` at
    query time. Miss any spelling here and the stale entry silently rejoins
    the next photo that reuses id 1.
    """
    from repositories.collections import remap_collection_photo_ids

    other_ws = db.create_workspace("Other")
    rules = [
        {"field": "photo_ids", "value": [1, 1.0, "1", "1.0", "1e0", "2"]},
    ]
    cid = db.conn.execute(
        "INSERT INTO collections (name, rules, workspace_id) VALUES (?, ?, ?)",
        ("mixed", json.dumps(rules), other_ws),
    ).lastrowid
    assert remap_collection_photo_ids(db.conn, {1: 7, 2: None}) == 1
    stored = json.loads(db.conn.execute(
        "SELECT rules FROM collections WHERE id = ?", (cid,),
    ).fetchone()[0])
    # Every spelling of 1 is remapped to 7 and folded to a single entry; 2 is
    # dropped outright. Order preserved from the first surviving occurrence.
    assert stored == [{"field": "photo_ids", "value": [7]}]


def test_remap_collection_photo_ids_leaves_unicode_whitespace_wrapped_ids_alone(db):
    """SQLite's numeric affinity only skips ASCII whitespace before parsing.

    A rule value like ``"\\xa01\\xa0"`` (NBSP-wrapped) never matches photo 1
    through the rules engine because SQLite leaves the NBSP bytes in place,
    the value stays TEXT, and TEXT never coerces to an integer id. Remapping
    id 1 must therefore leave that spelling alone — rewriting it would remove
    an entry the deleted photo never actually owned.
    """
    from repositories.collections import remap_collection_photo_ids

    other_ws = db.create_workspace("Other")
    rules = [
        {"field": "photo_ids", "value": [" 1 ", " 1 ", "1"]},
    ]
    cid = db.conn.execute(
        "INSERT INTO collections (name, rules, workspace_id) VALUES (?, ?, ?)",
        ("mixed-ws", json.dumps(rules), other_ws),
    ).lastrowid
    assert remap_collection_photo_ids(db.conn, {1: 7}) == 1
    stored = json.loads(db.conn.execute(
        "SELECT rules FROM collections WHERE id = ?", (cid,),
    ).fetchone()[0])
    # Only the plain ``"1"`` (which SQLite does match to id 1) is rewritten;
    # the NBSP- and U+2000-wrapped spellings stay verbatim so the collection
    # still says what the user saved.
    assert stored == [{"field": "photo_ids", "value": [" 1 ", " 1 ", 7]}]


def test_photo_id_key_normalizes_the_spellings_sqlite_matches():
    from repositories.collections import _photo_id_key

    assert _photo_id_key(1) == 1
    assert _photo_id_key(-3) == -3
    assert _photo_id_key(1.0) == 1
    assert _photo_id_key(-1.0) == -1
    assert _photo_id_key("1") == 1
    assert _photo_id_key(" 1 ") == 1
    assert _photo_id_key("-1") == -1
    assert _photo_id_key("1.0") == 1
    assert _photo_id_key("-1.0") == -1
    assert _photo_id_key("1e3") == 1000
    # SQLite treats True/False as integers 1/0 (integer affinity again), so
    # a rule value of ``True`` names id 1 and must remap alongside int 1.
    assert _photo_id_key(True) == 1
    assert _photo_id_key(False) == 0
    # Non-integral or non-numeric spellings never name an integer id.
    assert _photo_id_key(1.5) is None
    assert _photo_id_key("1.5") is None
    assert _photo_id_key(float("nan")) is None
    assert _photo_id_key(float("inf")) is None
    assert _photo_id_key(None) is None
    assert _photo_id_key("") is None
    assert _photo_id_key("   ") is None
    assert _photo_id_key("abc") is None
    # Spellings Python parses but SQLite does not: PEP 515 underscores and
    # Unicode digits never reach an integer id through SQLite's numeric
    # affinity, so accepting them would rewrite the wrong photo.
    assert _photo_id_key("1_0") is None
    assert _photo_id_key("1_000") is None
    assert _photo_id_key("٢") is None
    assert _photo_id_key("١٢٣") is None
    assert _photo_id_key("०") is None
    # SQLite's numeric affinity skips only ASCII space/tab/newline/vtab/
    # form feed/CR before conversion. ``str.strip()`` without arguments
    # additionally removes Unicode whitespace like NBSP and the U+2000
    # range, but SQLite leaves those bytes in place so the value stays
    # TEXT and never matches an integer id; accepting them here would
    # rewrite an unrelated collection entry.
    assert _photo_id_key(" 1 ") is None
    assert _photo_id_key(" 1 ") is None
    assert _photo_id_key("　1　") is None
    assert _photo_id_key(" 1") is None
    # ASCII whitespace SQLite does skip must still parse.
    assert _photo_id_key("\t1\t") == 1
    assert _photo_id_key("\n1\n") == 1
    assert _photo_id_key("\v1\v") == 1
    assert _photo_id_key("\f1\f") == 1
    assert _photo_id_key("\r1\r") == 1
    # Digit-only spellings must not lose precision above 2^53: Python's
    # ``float`` rounds them, but SQLite stores TEXT with integer affinity
    # as a 64-bit INTEGER exactly, so we mirror SQLite by parsing pure
    # integer forms through ``int``.
    assert _photo_id_key("9007199254740993") == 9007199254740993
    assert _photo_id_key("-9007199254740993") == -9007199254740993
    assert _photo_id_key(" 9007199254740993 ") == 9007199254740993
    assert _photo_id_key("+9007199254740993") == 9007199254740993
    # Decimal and exponent spellings still fall back to ``float`` because
    # SQLite's own REAL conversion loses the same precision there.
    assert _photo_id_key("9007199254740993.0") == 9007199254740992
    assert _photo_id_key("9007199254740993e0") == 9007199254740992


def test_remap_collection_photo_ids_preserves_large_integer_string_ids(db):
    """A digit-only string above 2^53 names a photo id exactly.

    SQLite parses ``"9007199254740993"`` bound against ``p.id`` as a 64-bit
    INTEGER without rounding, so the rules engine's ``photo_ids`` match hits
    that photo. If ``_photo_id_key`` collapsed that spelling through
    ``float()`` it would remap the neighbouring id instead, leaving a stale
    entry that silently rejoins the next reused id.
    """
    from repositories.collections import remap_collection_photo_ids

    other_ws = db.create_workspace("Other")
    big = 9007199254740993
    rules = [{"field": "photo_ids", "value": [str(big), big + 2]}]
    cid = db.conn.execute(
        "INSERT INTO collections (name, rules, workspace_id) VALUES (?, ?, ?)",
        ("big", json.dumps(rules), other_ws),
    ).lastrowid
    # Remap the exact id; the neighbouring id must be left alone.
    assert remap_collection_photo_ids(db.conn, {big: 7, big + 1: None}) == 1
    stored = json.loads(db.conn.execute(
        "SELECT rules FROM collections WHERE id = ?", (cid,),
    ).fetchone()[0])
    assert stored == [{"field": "photo_ids", "value": [7, big + 2]}]


def test_remap_collection_photo_ids_handles_json_escaped_field_names(db):
    """Valid JSON may spell ``photo_ids`` with escapes like ``photo\\u005fids``.

    ``add_collection`` stores callers' JSON text verbatim, so the raw row
    can encode the field name with unicode escapes even though the rules
    engine still parses it as ``photo_ids``. A raw-text ``LIKE`` filter on
    ``%photo_ids%`` would skip that row, leaving the stale id behind for
    the next photo that reuses it.
    """
    from repositories.collections import remap_collection_photo_ids

    other_ws = db.create_workspace("Other")
    raw = '[{"field":"photo\\u005fids","value":[1]}]'
    assert "photo_ids" not in raw
    cid = db.conn.execute(
        "INSERT INTO collections (name, rules, workspace_id) VALUES (?, ?, ?)",
        ("escaped", raw, other_ws),
    ).lastrowid
    assert remap_collection_photo_ids(db.conn, {1: 7}) == 1
    stored = json.loads(db.conn.execute(
        "SELECT rules FROM collections WHERE id = ?", (cid,),
    ).fetchone()[0])
    assert stored == [{"field": "photo_ids", "value": [7]}]


def test_needs_review_rule(db, folder):
    pending = _photo(db, folder, "a.jpg")
    accepted = _photo(db, folder, "b.jpg")
    nothing = _photo(db, folder, "c.jpg")
    _prediction(db, pending)
    _prediction(db, accepted, status="accepted")
    assert _ids(db, [{"field": "needs_review", "op": "is", "value": True}]) == [pending]
    assert _ids(db, [{"field": "needs_review", "op": "is", "value": False}]) == [accepted, nothing]
    row_scoped = db._build_query_from_rules(
        [{"field": "needs_review", "op": "is", "value": False}], row_scoped=True,
    )
    assert "COALESCE(prv.status, 'pending') != 'pending'" in row_scoped[2]
    assert "NOT EXISTS" not in row_scoped[2]


def test_active_mask_variant_is_not_and_contains(db, folder):
    large = _photo(db, folder, "a.jpg", active_mask_variant="sam2-large")
    small = _photo(db, folder, "b.jpg", active_mask_variant="sam2-small")
    none = _photo(db, folder, "c.jpg")
    assert _ids(db, [{"field": "active_mask_variant", "op": "is not", "value": "sam2-large"}]) == [small, none]
    assert _ids(db, [{"field": "active_mask_variant", "op": "contains", "value": "small"}]) == [small]
    assert large not in _ids(db, [{"field": "active_mask_variant", "op": "contains", "value": "small"}])
    # ``_`` and ``%`` are literal characters, not LIKE wildcards.
    assert _ids(db, [{"field": "active_mask_variant", "op": "contains", "value": "sam2_"}]) == []
    assert _ids(db, [{"field": "active_mask_variant", "op": "contains", "value": "%"}]) == []


def test_rules_engine_reads_workspace_lazily(db):
    db.set_active_workspace(None)
    # Validation errors surface before the workspace is resolved...
    with pytest.raises(ValueError, match="rules must be a list or group object"):
        db._build_query_from_rules("nope")
    # ...and a valid tree needs the active workspace for the folder join.
    with pytest.raises(RuntimeError):
        db._build_query_from_rules([{"field": "all"}])


def test_rules_engine_reads_effective_detector_floor_once(db, monkeypatch):
    calls = []
    real = db.get_effective_config

    def recording(config):
        calls.append(config)
        return real(config)

    monkeypatch.setattr(db, "get_effective_config", recording)
    db._build_query_from_rules([
        {"field": "prediction_confidence", "op": ">=", "value": 0.5},
        {"field": "classifier_model", "op": "equals", "value": "m"},
    ])
    assert len(calls) == 1


def test_has_subject_honors_patched_subject_types(db, monkeypatch):
    monkeypatch.setattr(db, "get_subject_types", lambda: set())
    folder_join, join, where, params = db._build_query_from_rules(
        [{"field": "has_subject", "op": "is", "value": True}]
    )
    assert where == "WHERE (0)"
    assert params == [db._active_workspace_id]


def test_rules_engine_rejects_malformed_groups_and_metadata(db):
    with pytest.raises(ValueError, match="^rule group rules must be a list$"):
        db._build_query_from_rules({"mode": "all", "rules": [{"rules": "x"}]})
    with pytest.raises(ValueError, match="metadata search requires contains/not_contains"):
        db.collections.count_photos_for_rules([{"field": "metadata", "op": "equals", "value": "x"}])
    with pytest.raises(ValueError, match="metadata search is limited to 4,096 characters"):
        db.collections.count_photos_for_rules(
            [{"field": "metadata", "op": "contains", "value": "x" * 4097}]
        )


def test_keyword_identity_rule(db, folder):
    from repositories.keyword_identity_sql import identity_sql

    a = _photo(db, folder, "a.jpg")
    _photo(db, folder, "b.jpg")
    kid = db.add_keyword("Heron")
    db.tag_photo(a, kid)
    db.commit()
    identity = db.conn.execute(
        f"SELECT {identity_sql('k')} FROM keywords k WHERE k.id = ?", (kid,)
    ).fetchone()[0]
    assert _ids(db, [{"field": "keyword_identity", "op": "equals", "value": identity}]) == [a]


def test_prediction_rules_split_by_row_scope(db, folder):
    fox = _photo(db, folder, "fox.jpg")
    bare = _photo(db, folder, "bare.jpg")
    _prediction(db, fox, taxonomy={"genus": "Vulpes"})
    assert _ids(db, [{"field": "taxonomy_genus", "op": "equals", "value": "Vulpes"}]) == [fox]
    assert _ids(db, [{"field": "prediction_status", "op": "in", "value": ["pending"]}]) == [fox]
    assert _ids(db, [{"field": "prediction_status", "op": "not_in", "value": ["pending"]}]) == [bare]
    assert _ids(db, [{"field": "classifier_model", "op": "is not", "value": "bioclip-2.5"}]) == [bare]
    for rule, positive in (
        ({"field": "classifier_model", "op": "is not", "value": "m"},
         "pred.classifier_model != ?"),
        ({"field": "prediction_status", "op": "is not", "value": "accepted"},
         "COALESCE(prv.status, 'pending') != ?"),
        ({"field": "prediction_status", "op": "not_in", "value": ["accepted"]},
         "COALESCE(prv.status, 'pending') NOT IN (?)"),
    ):
        where = db._build_query_from_rules([rule], row_scoped=True)[2]
        assert positive in where
        assert not where.startswith("WHERE (NOT ")


def test_rule_queries_and_restrictions_with_a_where_clause(db, folder, tmp_path):
    sub = workspace_folders.add_folder(db, str(tmp_path / "photos" / "sub"), name="sub", parent_id=folder)
    top = _photo(db, folder, "a.jpg", rating=5)
    nested = _photo(db, sub, "b.jpg", rating=5)
    _photo(db, sub, "c.jpg", rating=1)
    rated = [{"field": "rating", "op": ">=", "value": 4}]
    cid = db.collections.add("Rated", json.dumps(rated))
    db.commit()
    rows = db.collections.get_photos(cid, photo_ids=[nested, top])
    assert sorted(r["id"] for r in rows) == [top, nested]
    assert db.collections.count_photo_availability(cid) == {
        "total": 2, "available": 2, "offline": 0,
    }
    has_name = [{"field": "filename", "op": "contains", "value": "."}]
    assert db.collections.count_photos_for_rules(has_name, collection_id=cid, folder_id=sub) == 1
    assert db.collections.query_photo_ids(has_name, collection_id=cid) == [top, nested]
    rows = db.collections.query_photos(rated, folder_id=sub, include_offline_folders=True)
    assert [(r["id"], r["folder_status"]) for r in rows] == [(nested, "ok")]


def test_offline_photos_stay_single_in_stacks(db, folder, tmp_path):
    offline_folder = workspace_folders.add_folder(db, str(tmp_path / "gone"), name="gone")
    a = _photo(db, folder, "a.jpg", file_hash="h")
    b = _photo(db, offline_folder, "b.jpg", file_hash="h")
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (offline_folder,))
    db.conn.commit()
    rows = db.collections.query_browse_stacks([], sort="name", include_offline_folders=True)
    assert [(r["id"], r["_browse_stack_kind"], r["folder_status"]) for r in rows] == [
        (a, None, "ok"), (b, None, "missing"),
    ]
    assert db.collections.browse_stack_totals([], include_offline_folders=True) == {
        "total": 2, "stack_count": 0,
    }


def test_private_helpers_stay_callable_on_the_facade(db, folder):
    a = _photo(db, folder, "a.jpg", timestamp="2024-01-01T00:00:00")
    assert browse_queries.rules_resolvable(db, [{"field": "all"}]) is True
    assert db.collections._stack_sort_spec("bogus") == db.collections._stack_sort_spec("date")
    assert db.collections._stack_sort_clause("name") == "_stack_lead_filename ASC, _stack_lead_id ASC"
    assert db.collections._append_folder_restriction(None, "WHERE x", [1]) == ("WHERE x", [1])
    assert db.collections._append_collection_restriction(None, "", []) == ("", [])
    where, params = db.collections._append_folder_restriction(folder, "WHERE (a) OR (b)", [7])
    assert where == "WHERE ((a) OR (b)) AND p.folder_id IN (?)"
    assert params == [7, folder]
    ctes, params = db.collections._browse_stack_query_parts([])
    assert "keyed AS" in ctes
    assert params[0] == db._active_workspace_id
    ranked, ranked_params = db.collections._ranked_stack_query([], sort="name")
    assert ranked.startswith(ctes)
    assert ranked_params == params
    assert db.collections._burst_keys_for_ids([a]) == {}
    folder_join, join, where, params = db._build_query_from_rules([])
    assert db.collections._folder_filter_values(folder_join, join, where, params, q=None, limit=5) == [
        {"value": db.folders.get(folder)["path"], "count": 1},
    ]


# -- Collection-backed queries ------------------------------------------------


def test_missing_collection_short_circuits(db):
    assert db.collections.get_photos(9999) == []
    assert db.collections.get_photo_ids(9999) == []
    assert db.collections.count_photos(9999) == 0
    assert db.collections.count_photo_availability(9999) == {
        "total": 0, "available": 0, "offline": 0,
    }
    assert db.collections.photo_ids(9999) == set()
    with pytest.raises(ValueError, match="^collection not found in active workspace$"):
        db.collections.count_photos_for_rules([], collection_id=9999)


def test_get_collection_photos_narrowed_by_photo_ids(db, folder):
    a = _photo(db, folder, "a.jpg")
    b = _photo(db, folder, "b.jpg")
    cid = db.collections.add("All", json.dumps([{"field": "all"}]))
    db.commit()
    assert db.collections.get_photos(cid, photo_ids=[]) == []
    rows = db.collections.get_photos(cid, photo_ids=[b, b])
    assert [r["id"] for r in rows] == [b]
    assert sorted(db.collections.photo_ids(cid)) == [a, b]


def test_collection_queries_sql_shape(db, folder):
    _photo(db, folder, "a.jpg")
    cid = db.collections.add("All", json.dumps([{"field": "all"}]))
    db.commit()
    statements = []
    db.conn.set_trace_callback(statements.append)
    try:
        db.collections.count_photos(cid)
    finally:
        db.conn.set_trace_callback(None)
    ws = db._active_workspace_id
    assert [" ".join(s.split()) for s in statements] == [
        f"SELECT rules FROM collections WHERE id = {cid} AND workspace_id = {ws}",
        "SELECT COUNT(DISTINCT p.id) FROM photos p JOIN folders f ON f.id = p.folder_id "
        "AND f.status IN ('ok', 'partial') JOIN photo_workspace_visibility wf ON "
        f"wf.photo_id = p.id AND wf.workspace_id = {ws}",
    ]


# -- Stacks and positions ------------------------------------------------------


def test_stack_position_first_none_when_no_candidate_matches(db, folder):
    a = _photo(db, folder, "a.jpg")
    assert db.collections.query_browse_stack_position_first(
        [{"field": "photo_ids", "value": [a]}], [a + 100]
    ) is None
    assert db.collections.query_browse_stack_position_first([], []) is None


def test_photo_position_first_empty_ids(db):
    assert db.collections.query_photo_position_first([], []) is None


def test_collapse_stack_ids_empty_and_single(db, folder):
    a = _photo(db, folder, "a.jpg", timestamp="2024-01-01T00:00:00")
    assert db.collections.collapse_browse_stack_photo_ids([]) == []
    assert db.collections.collapse_browse_stack_photo_ids([a]) == [
        {"cover_id": a, "kind": None, "member_ids": [a]},
    ]


def test_collapse_stack_ids_duplicates_and_standalone(db, folder):
    dup_a = _photo(db, folder, "a.jpg", file_hash="h1", quality_score=0.2)
    dup_b = _photo(db, folder, "b.jpg", file_hash="h1", quality_score=0.9)
    offline = _photo(db, folder, "c.jpg", file_hash="h1", quality_score=1.0)
    solo = _photo(db, folder, "d.jpg")
    items = db.collections.collapse_browse_stack_photo_ids(
        [solo, dup_a, offline, dup_b, 9999], standalone_ids=[offline],
    )
    assert items == [
        {"cover_id": solo, "kind": None, "member_ids": [solo]},
        {"cover_id": dup_b, "kind": "duplicate", "member_ids": [dup_a, dup_b]},
        {"cover_id": offline, "kind": None, "member_ids": [offline]},
    ]


def test_burst_keys_drop_the_temp_scope(db, folder):
    a = _photo(db, folder, "a.jpg", timestamp="2024-01-01T00:00:00")
    b = _photo(db, folder, "b.jpg", timestamp="2024-01-01T00:00:01")
    items = db.collections.collapse_browse_stack_photo_ids([a, b])
    assert [(i["kind"], i["member_ids"]) for i in items] == [("burst", [a, b])]
    assert db.conn.execute(
        "SELECT name FROM temp.sqlite_master WHERE name = '_burst_scope'"
    ).fetchone() is None


# -- Filter-value suggestions ----------------------------------------------------


def test_keyword_filter_values(db, folder):
    a = _photo(db, folder, "a.jpg")
    b = _photo(db, folder, "b.jpg")
    kid = db.add_keyword("Sunset")
    other = db.add_keyword("Sunrise")
    db.tag_photo(a, kid)
    db.tag_photo(b, kid)
    db.tag_photo(b, other)
    db.commit()
    assert db.collections.get_filter_field_values("keyword", q="sun") == [
        {"value": "Sunset", "count": 2},
        {"value": "Sunrise", "count": 1},
    ]
    with pytest.raises(ValueError, match="does not support value suggestions"):
        db.collections.get_filter_field_values("rating")


# -- Default collections and migrations ------------------------------------------


def test_create_default_collections_commits(db):
    db.collections.create_defaults()
    db.commit()
    with _reader(db) as other:
        rows = {
            r["name"]: json.loads(r["rules"]) for r in other.execute(
                "SELECT name, rules FROM collections WHERE workspace_id = ?",
                (db._active_workspace_id,),
            )
        }
    assert rows["Needs Identification"] == NEEDS_IDENTIFICATION_RULES
    assert rows["GPS Without Location Keyword"] == GPS_WITHOUT_LOCATION_KEYWORD_RULES
    assert len(rows) == 6


def test_create_default_collections_for_explicit_workspace_skips_active(db):
    other_ws = db.create_workspace("other")
    db.set_active_workspace(None)
    db.collections.create_defaults(workspace_id=other_ws)
    db.commit()
    count = db.conn.execute(
        "SELECT COUNT(*) FROM collections WHERE workspace_id = ?", (other_ws,)
    ).fetchone()[0]
    assert count == 6


# -- structure ----------------------------------------------------------------


# The forwarding wrappers this domain used to have on ``Database``; callers
# reach the same methods through ``db.collections`` now.
_REMOVED_COLLECTION_WRAPPERS = (
    "add_collection",
    "get_collections_for_picker",
    "get_collection",
    "update_collection",
    "get_collections",
    "delete_collection",
    "rename_collection",
    "duplicate_collection",
    "_build_collection_query",
    "_photo_sort_clause",
    "get_collection_photos",
    "get_collection_photo_ids",
    "count_collection_photos",
    "count_collection_photo_availability",
    "count_photos_for_rules",
    "_append_folder_restriction",
    "_append_collection_restriction",
    "query_photos",
    "_burst_run_ctes",
    "_browse_stack_query_parts",
    "_stack_sort_spec",
    "_stack_sort_clause",
    "_ranked_stack_query",
    "query_browse_stacks",
    "browse_stack_totals",
    "query_browse_stack_position_first",
    "_stacked_photo_ids",
    "_burst_keys_for_ids",
    "collapse_browse_stack_photo_ids",
    "query_photo_ids",
    "query_photo_position_first",
    "get_filter_field_values",
    "_folder_filter_values",
    "collection_photo_ids",
    "create_default_collections",
)

# Still forwarders on ``Database``: ``PredictionRepository`` binds them from
# the façade under these names (``repositories.predictions.FACADE_METHODS``),
# so monkeypatches of them keep reaching the prediction SQL.
_DELEGATING_COLLECTION_METHODS = (
    "_build_query_from_rules",
    "_top_prediction_confidence_params",
)

# No SQL of their own: services compose collection reads, so class-level
# patches of the repository methods (``query_photo_position_first``, ...) still
# apply. ``rules_resolvable`` compiles through the façade forwarder.
_COMPOSING_COLLECTION_FUNCTIONS = {
    ("browse_queries", "rules_resolvable"): "_build_query_from_rules",
    ("browse_queries", "query_photo_position"): "query_photo_position_first",
    ("browse_queries", "query_browse_stack_position"): "query_browse_stack_position_first",
    ("browse_queries", "count_browse_stacks"): "browse_stack_totals",
    ("browse_queries", "get_collection_photo_ids_stacked"): "_stacked_photo_ids",
    ("browse_queries", "query_photo_ids_stacked"): "_stacked_photo_ids",
    ("workspace_config", "create_default_collections_for_all_workspaces"): "create_defaults",
}


def _self_attrs(name):
    source = textwrap.dedent(inspect.getsource(getattr(Database, name)))
    fn = ast.parse(source).body[0]
    return {
        node.attr
        for node in ast.walk(fn)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }


def _accessor_calls(name):
    """Methods ``Database.<name>`` calls as ``self.collections.<method>``."""
    source = textwrap.dedent(inspect.getsource(getattr(Database, name)))
    return {
        node.attr
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Attribute)
        and node.value.attr == "collections"
        and isinstance(node.value.value, ast.Name)
        and node.value.value.id == "self"
    }


@pytest.mark.parametrize("name", _DELEGATING_COLLECTION_METHODS)
def test_collection_method_delegates_to_repository(name):
    attrs = _self_attrs(name)
    assert "conn" not in attrs, (
        f"Database.{name} touches self.conn; move the SQL to CollectionRepository"
    )
    assert "_collection_repository" in attrs, (
        f"Database.{name} no longer delegates to CollectionRepository"
    )


def test_collections_has_no_forwarding_wrappers_on_database():
    """The domain is reached through ``db.collections``; Database keeps no aliases."""
    from repositories.collections import CollectionRepository

    for name in _REMOVED_COLLECTION_WRAPPERS:
        assert not hasattr(Database, name), (
            f"Database.{name} came back; call db.collections"
        )
    accessor = Database.__dict__["collections"]
    assert isinstance(accessor, property)
    source = textwrap.dedent(inspect.getsource(accessor.fget))
    attrs = {
        node.attr
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }
    assert "_collection_repository" in attrs
    assert "conn" not in attrs
    for name in _COMPOSING_COLLECTION_FUNCTIONS.values():
        assert callable(getattr(CollectionRepository, name)), name


def test_collections_is_a_fresh_repository_per_access(db):
    from repositories.collections import CollectionRepository

    first, second = db.collections, db.collections
    assert isinstance(first, CollectionRepository)
    assert first is not second
    assert first.conn is db.conn


def test_candidate_preference_case_lives_on_the_repository():
    from repositories.collections import CollectionRepository

    assert not hasattr(Database, "_candidate_preference_case")
    assert CollectionRepository._candidate_preference_case("id", [5, 7]) == (
        "CASE id WHEN ? THEN 0 WHEN ? THEN 1 ELSE 2 END"
    )


def _service_calls(fn):
    """Attributes a service function reads off ``db`` or ``db.collections``."""
    source = textwrap.dedent(inspect.getsource(fn))
    tree = ast.parse(source)
    direct = {
        node.attr for node in ast.walk(tree)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name) and node.value.id == "db"
    }
    via_accessor = {
        node.attr for node in ast.walk(tree)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Attribute)
        and node.value.attr == "collections"
        and isinstance(node.value.value, ast.Name)
        and node.value.value.id == "db"
    }
    return direct, via_accessor


@pytest.mark.parametrize(
    "where, callee", sorted(_COMPOSING_COLLECTION_FUNCTIONS.items()),
    ids=[name for (_module, name) in sorted(_COMPOSING_COLLECTION_FUNCTIONS)],
)
def test_collection_composition_lives_in_services(where, callee):
    import importlib

    module_name, name = where
    fn = getattr(importlib.import_module(f"services.{module_name}"), name)
    direct, via_accessor = _service_calls(fn)
    assert "conn" not in direct
    assert "_collection_repository" not in direct
    assert callee in direct | via_accessor
    assert not hasattr(Database, name), f"Database.{name} came back"


def test_collection_repository_wiring(db, monkeypatch):
    import db as db_module

    db.set_active_workspace(None)
    repo = db._collection_repository()  # resolves the workspace lazily
    with pytest.raises(RuntimeError):
        _ = repo.workspace_id
    db.set_active_workspace(1)
    assert repo.workspace_id == 1
    assert repo.conn is db.conn
    assert repo.effective_config == db.effective_config
    assert repo.get_subject_types == db.get_subject_types
    assert repo.get_folder_subtree_ids == db.get_folder_subtree_ids
    assert repo._chunks is db_module._chunks
    assert repo._escape_like is db_module._escape_like
    assert repo.PHOTO_COLS is Database.PHOTO_COLS
    assert repo._STACK_SORT_SPECS is Database._STACK_SORT_SPECS
    assert repo.NEEDS_IDENTIFICATION_RULES is NEEDS_IDENTIFICATION_RULES
    # Module constants are read when the repository is built, so a patch of
    # the ``db`` module still reaches the moved SQL.
    monkeypatch.setattr(db_module, "BURST_GAP_TOLERANCE_SECONDS", 99)
    assert db.collections._burst_run_ctes({"split_mode": "break", "time_gap": 1})[1] == [100]


def test_collection_repository_imports_no_db_code():
    import repositories.collections as module

    tree = ast.parse(inspect.getsource(module))
    imported = {
        alias.name.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    } | {
        node.module.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module
    }
    assert "db" not in imported


def test_collection_signatures_unchanged():
    from repositories.collections import CollectionRepository

    facade = {n: str(inspect.signature(getattr(Database, n)))
              for n in _DELEGATING_COLLECTION_METHODS}
    assert facade["_build_query_from_rules"] == (
        "(self, rules, include_offline_folders=False, row_scoped=False)"
    )

    def sig(name):
        return str(inspect.signature(getattr(CollectionRepository, name)))

    assert sig("add") == "(self, name, rules_json, visual_json=None)"
    assert sig("get_photos") == (
        "(self, collection_id, page=1, per_page=50, photo_ids=None, sort='date', "
        "include_offline_folders=False)"
    )
    assert sig("query_browse_stacks") == (
        "(self, rules, sort='date', page=1, per_page=50, collection_id=None, "
        "folder_id=None, include_offline_folders=False, stack_config=None)"
    )
    assert sig("get_filter_field_values") == (
        "(self, field, rules=None, q=None, limit=20, folder_id=None, collection_id=None)"
    )
    assert sig("collapse_browse_stack_photo_ids") == (
        "(self, photo_ids, standalone_ids=None, stack_config=None)"
    )
    assert sig("create_defaults") == "(self, workspace_id=None)"


def test_top_predicted_species_matches_summary(db, folder):
    """Clicks match current winning predictions, not keywords or runners-up."""
    robin = _photo(db, folder, "robin.jpg")
    hawk = _photo(db, folder, "hawk.jpg")
    hidden = _photo(db, folder, "hidden.jpg")
    for pid, floor in [(robin, .95), (hawk, .95), (hidden, .01)]:
        det = db.detections.save(pid, [
            {"box": {"x": 0, "y": 0, "w": 1, "h": 1}, "confidence": floor}
        ], detector_model="test")[0]
        for species, confidence, fingerprint, created in [
            ("Stale", .99, "old", "2026-01-01"),
            ("Runner-up", .5, "current", "2026-02-01"),
            ("Robin" if pid == robin else "Hawk", .9, "current", "2026-02-01"),
        ]:
            db.conn.execute(
                "INSERT INTO predictions (detection_id, classifier_model, labels_fingerprint, "
                "species, confidence, created_at) VALUES (?, 'test', ?, ?, ?, ?)",
                (det, fingerprint, species, confidence, created),
            )
    db.conn.commit()
    # Conflicting saved metadata must not change a prediction drill-down.
    db.tag_photo(hawk, db.add_keyword("Robin", is_species=True))
    db.commit()
    def rule(species):
        return [{"field": "top_predicted_species", "op": "is", "value": species}]

    assert _ids(db, rule("Robin")) == [robin]
    assert _ids(db, rule("Hawk")) == [hawk]
    assert _ids(db, rule("Runner-up")) == []
    assert _ids(db, rule("Stale")) == []
    for row in browse_queries.get_browse_summary(db, folder_id=folder)["top_species"]:
        assert len(_ids(db, rule(row["species"]), folder_id=folder)) == row["count"]
    pred_id = db.conn.execute("SELECT id FROM predictions WHERE species = 'Robin'").fetchone()[0]
    db.update_prediction_status(pred_id, "rejected")
    assert _ids(db, rule("Robin")) == []
    assert _ids(db, rule("Runner-up")) == [robin]
