"""Behavior pins for the canonical schema that ``Database`` creates on open.

The tests open databases only through the public ``Database`` constructor,
so they hold regardless of where the ``CREATE TABLE`` script and its inline
migrations live. ``fixtures/canonical_schema_snapshot.json`` is the
``sqlite_master`` and ``db_meta`` content of a freshly created catalog; any
change to the canonical schema shows up here as a readable diff. The
structural tests at the end keep the schema SQL in ``canonical_schema.py``.

Regenerate the snapshot after a deliberate schema change with::

    python vireo/tests/test_db_canonical_schema.py --update
"""

import ast
import inspect
import json
import os
import sqlite3
import sys
import textwrap
from contextlib import closing

if __name__ == "__main__":  # pragma: no cover - snapshot regeneration entry point
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import db as db_module
from db import Database

SNAPSHOT_PATH = os.path.join(
    os.path.dirname(__file__), "fixtures", "canonical_schema_snapshot.json"
)

# The species-identity repair marker embeds a digest of the local taxonomy
# resolution, so its key differs between machines. It is written by
# ``species_identity_repair`` after the schema is created, not by the schema.
_ENVIRONMENT_META_PREFIXES = ("species_identity_repair:",)


def _sqlite_master(path):
    """Every schema object in creation (rowid) order."""
    with closing(sqlite3.connect(path)) as conn:
        return [
            list(row)
            for row in conn.execute(
                "SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY rowid"
            )
        ]


def _db_meta(path):
    with closing(sqlite3.connect(path)) as conn:
        return dict(conn.execute("SELECT key, value FROM db_meta ORDER BY key"))


def _fresh_snapshot(path):
    Database(path).close()
    meta = {
        key: value
        for key, value in _db_meta(path).items()
        if not key.startswith(_ENVIRONMENT_META_PREFIXES)
    }
    return {"sqlite_master": _sqlite_master(path), "db_meta": meta}


def _load_snapshot():
    with open(SNAPSHOT_PATH, encoding="utf-8") as f:
        return json.load(f)


def test_fresh_database_schema_matches_snapshot(tmp_path):
    actual = _fresh_snapshot(str(tmp_path / "fresh.db"))
    expected = _load_snapshot()

    actual_names = [(row[0], row[1]) for row in actual["sqlite_master"]]
    expected_names = [(row[0], row[1]) for row in expected["sqlite_master"]]
    assert actual_names == expected_names, (
        "schema objects or their creation order changed; regenerate with "
        "`python vireo/tests/test_db_canonical_schema.py --update` if deliberate"
    )
    for got, want in zip(actual["sqlite_master"], expected["sqlite_master"], strict=True):
        assert got == want, f"{want[0]} {want[1]} changed"
    assert actual["db_meta"] == expected["db_meta"]


def test_fresh_snapshot_covers_every_object_kind():
    """Guard against a truncated or hand-edited snapshot file."""
    kinds = {row[0] for row in _load_snapshot()["sqlite_master"]}
    assert {"table", "index", "trigger", "view"} <= kinds


def test_reopening_existing_database_changes_nothing(tmp_path):
    path = str(tmp_path / "reopen.db")
    Database(path).close()
    first_master = _sqlite_master(path)
    first_meta = _db_meta(path)

    Database(path).close()
    assert _sqlite_master(path) == first_master
    assert _db_meta(path) == first_meta

    Database(path).close()
    assert _sqlite_master(path) == first_master
    assert _db_meta(path) == first_meta


def test_reopen_keeps_seeded_processes_and_default_workspace(tmp_path):
    """The one-shot seeds in schema setup run once, not on every open."""
    path = str(tmp_path / "seeds.db")
    Database(path).close()
    with closing(sqlite3.connect(path)) as conn:
        processes = conn.execute(
            "SELECT id, name, is_seed, sort_order FROM saved_processes ORDER BY id"
        ).fetchall()
        workspaces = conn.execute(
            "SELECT id, name, tabs FROM workspaces ORDER BY id"
        ).fetchall()
    assert processes
    assert workspaces

    Database(path).close()
    with closing(sqlite3.connect(path)) as conn:
        assert conn.execute(
            "SELECT id, name, is_seed, sort_order FROM saved_processes ORDER BY id"
        ).fetchall() == processes
        assert conn.execute(
            "SELECT id, name, tabs FROM workspaces ORDER BY id"
        ).fetchall() == workspaces


# -- structure: the canonical schema lives in canonical_schema.py -------------


def _self_attrs(fn):
    return {
        node.attr
        for node in ast.walk(fn)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }


def test_create_tables_delegates_to_canonical_schema():
    source = textwrap.dedent(inspect.getsource(Database._create_tables))
    fn = ast.parse(source).body[0]
    attrs = _self_attrs(fn)
    assert "conn" not in attrs, (
        "Database._create_tables touches self.conn; schema SQL belongs in "
        "canonical_schema.py"
    )
    assert "_canonical_schema" in attrs, (
        "Database._create_tables no longer delegates to CanonicalSchema"
    )
    assert list(inspect.signature(Database._create_tables).parameters) == ["self"]


def test_canonical_schema_receives_bound_facade_method(tmp_path):
    """The removal-scope upgrade reaches Database through the bound method."""
    with Database(str(tmp_path / "bound.db")) as db:
        schema_obj = db._canonical_schema()
        assert schema_obj.conn is db.conn
        assert schema_obj._folder_removal_root_ids == db._folder_removal_root_ids
        assert schema_obj.default_tabs is db_module.DEFAULT_TABS


def test_canonical_schema_imports_no_db_code():
    import canonical_schema

    tree = ast.parse(inspect.getsource(canonical_schema))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.add(node.module)
    assert "db" not in imported
    assert "schema" not in imported


if __name__ == "__main__":  # pragma: no cover
    import tempfile

    if sys.argv[1:] != ["--update"]:
        sys.exit("usage: python vireo/tests/test_db_canonical_schema.py --update")
    with tempfile.TemporaryDirectory() as tmp:
        snapshot = _fresh_snapshot(os.path.join(tmp, "fresh.db"))
    with open(SNAPSHOT_PATH, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=1, ensure_ascii=False)
        f.write("\n")
    print(f"wrote {SNAPSHOT_PATH}")
