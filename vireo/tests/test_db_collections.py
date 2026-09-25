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
    NO_LOCATION_INFORMATION_RULES,
    Database,
)


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
    return db.add_folder(str(tmp_path / "photos"), name="photos")


def _photo(db, folder_id, filename, **cols):
    pid = db.add_photo(
        folder_id=folder_id, filename=filename, extension=".jpg",
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
    return db.query_photo_ids(rules, sort="name", **kwargs)


def _prediction(db, photo_id, *, confidence=0.9, taxonomy=None, status="pending"):
    det_ids = db.save_detections(
        photo_id,
        [{"box": {"x": 0.1, "y": 0.1, "w": 0.8, "h": 0.8}, "confidence": 0.95}],
        detector_model="MegaDetector",
    )
    db.add_prediction(
        det_ids[0], species="Vulpes vulpes", confidence=confidence,
        model="bioclip-2.5", category="match", taxonomy=taxonomy,
        status=status,
    )


# -- CRUD ------------------------------------------------------------------


def test_add_collection_commits_and_returns_id(db):
    cid = db.add_collection("Best", json.dumps([{"field": "all"}]), '{"prompt": "x"}')
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
    db.add_collection("b", "[]")
    db.add_collection("a", "[]", "{}")
    other_ws = db.create_workspace("other")
    db.set_active_workspace(other_ws)
    db.add_collection("elsewhere", "[]")
    db.set_active_workspace(1)
    rows = db.get_collections()
    assert [tuple(r.keys()) for r in rows] == [("id", "name", "rules", "visual_json")] * 2
    assert [(r["name"], r["visual_json"]) for r in rows] == [("a", "{}"), ("b", None)]


def test_delete_collection_commits_and_ignores_other_workspace(db):
    keep = db.add_collection("keep", "[]")
    other_ws = db.create_workspace("other")
    db.set_active_workspace(other_ws)
    foreign = db.add_collection("foreign", "[]")
    db.set_active_workspace(1)
    db.delete_collection(keep)
    db.delete_collection(foreign)
    with _reader(db) as other:
        names = [r["name"] for r in other.execute("SELECT name FROM collections")]
    assert names == ["foreign"]


def test_rename_collection_commits(db):
    cid = db.add_collection("old", "[]")
    db.rename_collection(cid, "new")
    assert not db.conn.in_transaction
    with _reader(db) as other:
        assert other.execute(
            "SELECT name FROM collections WHERE id = ?", (cid,)
        ).fetchone()["name"] == "new"


def test_rename_collection_missing_or_foreign_raises(db):
    other_ws = db.create_workspace("other")
    db.set_active_workspace(other_ws)
    foreign = db.add_collection("foreign", "[]")
    db.set_active_workspace(1)
    with pytest.raises(ValueError, match="^collection not found$"):
        db.rename_collection(foreign, "stolen")
    with pytest.raises(ValueError, match="^collection not found$"):
        db.rename_collection(9999, "x")
    db.conn.rollback()
    assert db.conn.execute(
        "SELECT name FROM collections WHERE id = ?", (foreign,)
    ).fetchone()["name"] == "foreign"


def test_rename_collection_needs_active_workspace(db):
    db.set_active_workspace(None)
    with pytest.raises(RuntimeError):
        db.rename_collection(1, "x")


def test_duplicate_collection_names_copies_incrementally(db):
    cid = db.add_collection("Birds", '[{"field": "all"}]', '{"prompt": "owl"}')
    first = db.duplicate_collection(cid)
    second = db.duplicate_collection(cid)
    third = db.duplicate_collection(cid)
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
        db.duplicate_collection(9999)


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
        assert db.rules_resolvable(rules) is False
        with pytest.raises(ValueError, match=message):
            db.count_photos_for_rules(rules)


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
        db.count_photos_for_rules(rules)


def test_life_list_uncounted_wraps_json_error(db):
    rules = [{"field": "life_list_uncounted", "op": "equals", "value": "{bad"}]
    with pytest.raises(ValueError) as excinfo:
        db.count_photos_for_rules(rules)
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
        db.count_photos_for_rules([{"field": "metadata", "op": "equals", "value": "x"}])
    with pytest.raises(ValueError, match="metadata search is limited to 4,096 characters"):
        db.count_photos_for_rules(
            [{"field": "metadata", "op": "contains", "value": "x" * 4097}]
        )


def test_keyword_identity_rule(db, folder):
    from keyword_identity import identity_sql

    a = _photo(db, folder, "a.jpg")
    _photo(db, folder, "b.jpg")
    kid = db.add_keyword("Heron")
    db.tag_photo(a, kid)
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
    sub = db.add_folder(str(tmp_path / "photos" / "sub"), name="sub", parent_id=folder)
    top = _photo(db, folder, "a.jpg", rating=5)
    nested = _photo(db, sub, "b.jpg", rating=5)
    _photo(db, sub, "c.jpg", rating=1)
    rated = [{"field": "rating", "op": ">=", "value": 4}]
    cid = db.add_collection("Rated", json.dumps(rated))
    rows = db.get_collection_photos(cid, photo_ids=[nested, top])
    assert sorted(r["id"] for r in rows) == [top, nested]
    assert db.count_collection_photo_availability(cid) == {
        "total": 2, "available": 2, "offline": 0,
    }
    has_name = [{"field": "filename", "op": "contains", "value": "."}]
    assert db.count_photos_for_rules(has_name, collection_id=cid, folder_id=sub) == 1
    assert db.query_photo_ids(has_name, collection_id=cid) == [top, nested]
    rows = db.query_photos(rated, folder_id=sub, include_offline_folders=True)
    assert [(r["id"], r["folder_status"]) for r in rows] == [(nested, "ok")]


def test_offline_photos_stay_single_in_stacks(db, folder, tmp_path):
    offline_folder = db.add_folder(str(tmp_path / "gone"), name="gone")
    a = _photo(db, folder, "a.jpg", file_hash="h")
    b = _photo(db, offline_folder, "b.jpg", file_hash="h")
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (offline_folder,))
    db.conn.commit()
    rows = db.query_browse_stacks([], sort="name", include_offline_folders=True)
    assert [(r["id"], r["_browse_stack_kind"], r["folder_status"]) for r in rows] == [
        (a, None, "ok"), (b, None, "missing"),
    ]
    assert db.browse_stack_totals([], include_offline_folders=True) == {
        "total": 2, "stack_count": 0,
    }


def test_private_helpers_stay_callable_on_the_facade(db, folder):
    a = _photo(db, folder, "a.jpg", timestamp="2024-01-01T00:00:00")
    assert db.rules_resolvable([{"field": "all"}]) is True
    assert db._stack_sort_spec("bogus") == db._stack_sort_spec("date")
    assert db._stack_sort_clause("name") == "_stack_lead_filename ASC, _stack_lead_id ASC"
    assert db._append_folder_restriction(None, "WHERE x", [1]) == ("WHERE x", [1])
    assert db._append_collection_restriction(None, "", []) == ("", [])
    where, params = db._append_folder_restriction(folder, "WHERE (a) OR (b)", [7])
    assert where == "WHERE ((a) OR (b)) AND p.folder_id IN (?)"
    assert params == [7, folder]
    ctes, params = db._browse_stack_query_parts([])
    assert "keyed AS" in ctes
    assert params[0] == db._active_workspace_id
    ranked, ranked_params = db._ranked_stack_query([], sort="name")
    assert ranked.startswith(ctes)
    assert ranked_params == params
    assert db._burst_keys_for_ids([a]) == {}
    folder_join, join, where, params = db._build_query_from_rules([])
    assert db._folder_filter_values(folder_join, join, where, params, q=None, limit=5) == [
        {"value": db.get_folder(folder)["path"], "count": 1},
    ]


# -- Collection-backed queries ------------------------------------------------


def test_missing_collection_short_circuits(db):
    assert db.get_collection_photos(9999) == []
    assert db.get_collection_photo_ids(9999) == []
    assert db.count_collection_photos(9999) == 0
    assert db.count_collection_photo_availability(9999) == {
        "total": 0, "available": 0, "offline": 0,
    }
    assert db.collection_photo_ids(9999) == set()
    with pytest.raises(ValueError, match="^collection not found in active workspace$"):
        db.count_photos_for_rules([], collection_id=9999)


def test_get_collection_photos_narrowed_by_photo_ids(db, folder):
    a = _photo(db, folder, "a.jpg")
    b = _photo(db, folder, "b.jpg")
    cid = db.add_collection("All", json.dumps([{"field": "all"}]))
    assert db.get_collection_photos(cid, photo_ids=[]) == []
    rows = db.get_collection_photos(cid, photo_ids=[b, b])
    assert [r["id"] for r in rows] == [b]
    assert sorted(db.collection_photo_ids(cid)) == [a, b]


def test_collection_queries_sql_shape(db, folder):
    _photo(db, folder, "a.jpg")
    cid = db.add_collection("All", json.dumps([{"field": "all"}]))
    statements = []
    db.conn.set_trace_callback(statements.append)
    try:
        db.count_collection_photos(cid)
    finally:
        db.conn.set_trace_callback(None)
    ws = db._active_workspace_id
    assert [" ".join(s.split()) for s in statements] == [
        f"SELECT rules FROM collections WHERE id = {cid} AND workspace_id = {ws}",
        "SELECT COUNT(DISTINCT p.id) FROM photos p JOIN folders f ON f.id = p.folder_id "
        "AND f.status IN ('ok', 'partial') JOIN workspace_folders wf ON "
        f"wf.folder_id = f.id AND wf.workspace_id = {ws}",
    ]


# -- Stacks and positions ------------------------------------------------------


def test_stack_position_first_none_when_no_candidate_matches(db, folder):
    a = _photo(db, folder, "a.jpg")
    assert db.query_browse_stack_position_first(
        [{"field": "photo_ids", "value": [a]}], [a + 100]
    ) is None
    assert db.query_browse_stack_position_first([], []) is None


def test_photo_position_first_empty_ids(db):
    assert db.query_photo_position_first([], []) is None


def test_collapse_stack_ids_empty_and_single(db, folder):
    a = _photo(db, folder, "a.jpg", timestamp="2024-01-01T00:00:00")
    assert db.collapse_browse_stack_photo_ids([]) == []
    assert db.collapse_browse_stack_photo_ids([a]) == [
        {"cover_id": a, "kind": None, "member_ids": [a]},
    ]


def test_collapse_stack_ids_duplicates_and_standalone(db, folder):
    dup_a = _photo(db, folder, "a.jpg", file_hash="h1", quality_score=0.2)
    dup_b = _photo(db, folder, "b.jpg", file_hash="h1", quality_score=0.9)
    offline = _photo(db, folder, "c.jpg", file_hash="h1", quality_score=1.0)
    solo = _photo(db, folder, "d.jpg")
    items = db.collapse_browse_stack_photo_ids(
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
    items = db.collapse_browse_stack_photo_ids([a, b])
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
    assert db.get_filter_field_values("keyword", q="sun") == [
        {"value": "Sunset", "count": 2},
        {"value": "Sunrise", "count": 1},
    ]
    with pytest.raises(ValueError, match="does not support value suggestions"):
        db.get_filter_field_values("rating")


# -- Default collections and migrations ------------------------------------------


def test_create_default_collections_commits(db):
    db.create_default_collections()
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
    db.create_default_collections(workspace_id=other_ws)
    count = db.conn.execute(
        "SELECT COUNT(*) FROM collections WHERE workspace_id = ?", (other_ws,)
    ).fetchone()[0]
    assert count == 6


def _raw_collection(db, name, rules, workspace_id=1):
    db.conn.execute(
        "INSERT INTO collections (name, rules, workspace_id) VALUES (?, ?, ?)",
        (name, rules, workspace_id),
    )
    db.conn.commit()


def test_migrate_location_collections_skips_malformed_rules(db):
    _raw_collection(db, "Needs Location", "{not json")
    assert db.migrate_default_location_collections() == 0
    assert not db.conn.in_transaction


def test_migrate_location_collections_fixes_no_location(db):
    _raw_collection(
        db, "No Location",
        json.dumps([{"field": "location_keyword_missing", "op": "equals", "value": 0}]),
    )
    assert db.migrate_default_location_collections() == 1
    with _reader(db) as other:
        row = other.execute("SELECT name, rules FROM collections").fetchone()
    assert row["name"] == "No Location Information"
    assert json.loads(row["rules"]) == NO_LOCATION_INFORMATION_RULES


def test_migrate_subject_collection_skips_malformed_and_existing(db):
    legacy = json.dumps([{"field": "has_species", "op": "equals", "value": 0}])
    _raw_collection(db, "Needs Classification", "{not json")
    _raw_collection(db, "Needs Classification", legacy)
    _raw_collection(db, "Needs Identification", "[]")
    db.migrate_default_subject_collection()
    names = sorted(
        r["name"] for r in db.conn.execute("SELECT name FROM collections")
    )
    assert names == ["Needs Classification", "Needs Classification", "Needs Identification"]
    assert not db.conn.in_transaction


def test_migrate_needs_identification_skips_malformed(db):
    _raw_collection(db, "Needs Identification", None)
    _raw_collection(
        db, "Needs Identification",
        json.dumps([{"field": "has_subject", "op": "equals", "value": 0}]),
    )
    assert db.migrate_default_needs_identification_collection() == 1
    with _reader(db) as other:
        rules = [r["rules"] for r in other.execute("SELECT rules FROM collections ORDER BY id")]
    assert rules == [None, json.dumps(NEEDS_IDENTIFICATION_RULES)]
    assert db.migrate_default_needs_identification_collection() == 0
    assert not db.conn.in_transaction


# -- structure ----------------------------------------------------------------


_DELEGATING_COLLECTION_METHODS = (
    "add_collection",
    "get_collections",
    "delete_collection",
    "rename_collection",
    "duplicate_collection",
    "_build_collection_query",
    "_build_query_from_rules",
    "_top_prediction_confidence_params",
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
    "migrate_default_location_collections",
    "migrate_default_subject_collection",
    "migrate_default_needs_identification_collection",
)

# No SQL of their own: they compose other façade methods, so monkeypatches of
# those (``query_photo_position_first``, ``get_workspaces``, ...) still apply.
_COMPOSING_COLLECTION_METHODS = {
    "rules_resolvable": "_build_query_from_rules",
    "query_photo_position": "query_photo_position_first",
    "query_browse_stack_position": "query_browse_stack_position_first",
    "count_browse_stacks": "browse_stack_totals",
    "get_collection_photo_ids_stacked": "_stacked_photo_ids",
    "query_photo_ids_stacked": "_stacked_photo_ids",
    "create_default_collections_for_all_workspaces": "create_default_collections",
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


@pytest.mark.parametrize("name", _DELEGATING_COLLECTION_METHODS)
def test_collection_method_delegates_to_repository(name):
    attrs = _self_attrs(name)
    assert "conn" not in attrs, (
        f"Database.{name} touches self.conn; move the SQL to CollectionRepository"
    )
    assert "_collection_repository" in attrs, (
        f"Database.{name} no longer delegates to CollectionRepository"
    )


def test_candidate_preference_case_delegates_to_repository():
    from repositories.collections import CollectionRepository

    source = textwrap.dedent(inspect.getsource(Database._candidate_preference_case))
    assert "CollectionRepository._candidate_preference_case" in source
    assert Database._candidate_preference_case("id", [5, 7]) == (
        CollectionRepository._candidate_preference_case("id", [5, 7])
    ) == "CASE id WHEN ? THEN 0 WHEN ? THEN 1 ELSE 2 END"


@pytest.mark.parametrize("name, callee", sorted(_COMPOSING_COLLECTION_METHODS.items()))
def test_collection_composition_stays_on_facade(name, callee):
    attrs = _self_attrs(name)
    assert "conn" not in attrs
    assert "_collection_repository" not in attrs
    assert callee in attrs


def test_collection_repository_wiring(db, monkeypatch):
    import db as db_module

    db.set_active_workspace(None)
    repo = db._collection_repository()  # resolves the workspace lazily
    with pytest.raises(RuntimeError):
        _ = repo.workspace_id
    db.set_active_workspace(1)
    assert repo.workspace_id == 1
    assert repo.conn is db.conn
    assert repo.get_effective_config == db.get_effective_config
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
    assert db._burst_run_ctes({"split_mode": "break", "time_gap": 1})[1] == [100]


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


def test_collection_facade_signatures_unchanged():
    sig = {n: str(inspect.signature(getattr(Database, n)))
           for n in (*_DELEGATING_COLLECTION_METHODS, *_COMPOSING_COLLECTION_METHODS)}
    assert sig["add_collection"] == "(self, name, rules_json, visual_json=None)"
    assert sig["_build_query_from_rules"] == (
        "(self, rules, include_offline_folders=False, row_scoped=False)"
    )
    assert sig["get_collection_photos"] == (
        "(self, collection_id, page=1, per_page=50, photo_ids=None, sort='date', "
        "include_offline_folders=False)"
    )
    assert sig["query_browse_stacks"] == (
        "(self, rules, sort='date', page=1, per_page=50, collection_id=None, "
        "folder_id=None, include_offline_folders=False, stack_config=None)"
    )
    assert sig["get_filter_field_values"] == (
        "(self, field, rules=None, q=None, limit=20, folder_id=None, collection_id=None)"
    )
    assert sig["collapse_browse_stack_photo_ids"] == (
        "(self, photo_ids, standalone_ids=None, stack_config=None)"
    )
    assert sig["create_default_collections"] == "(self, workspace_id=None)"
