"""Behavior pins for the import job's catalog SQL (``db.imports``).

``import_job`` reads the catalog around each batch it lands (the hash-stamp
cross-check, cache invalidation, a resume's recovery of its parent's
landings, the duplicate gate's twins) and promotes folders it proved present
out of ``'missing'``. These tests pin what each ``db.imports`` method returns
and that its writes stay in the caller's transaction. The structural tests at
the end keep the SQL in ``repositories/imports.py`` and the import job off the
connection.
"""

import ast
import contextlib
import inspect
import sqlite3
import textwrap
from pathlib import Path

import pytest
from db import Database
from repositories.imports import ImportRepository

VIREO = Path(__file__).resolve().parents[1]


@pytest.fixture
def catalog(db):
    """Two folders with a RAW+JPEG pair, a plain JPEG and a working copy."""
    archive = db.add_folder("/archive/2026", name="2026")
    other = db.add_folder("/other", name="other")
    raw = db.add_photo(archive, "bird.cr3", ".cr3", 300, 1.0,
                       timestamp="2026-01-01T10:00:00", file_hash="h-raw")
    jpg = db.add_photo(archive, "fox.jpg", ".jpg", 100, 1.0,
                       timestamp="2026-01-01T11:00:00", file_hash="h-fox")
    twin = db.add_photo(other, "fox-copy.jpg", ".jpg", 100, 1.0,
                        timestamp="2026-01-02T11:00:00", file_hash="h-twin")
    db.conn.execute(
        "UPDATE photos SET companion_path = 'bird.jpg' WHERE id = ?", (raw,),
    )
    db.conn.execute(
        "UPDATE photos SET working_copy_path = '/wc/fox.jpg' WHERE id = ?", (jpg,),
    )
    db.conn.commit()
    return {"archive": archive, "other": other, "raw": raw, "jpg": jpg, "twin": twin}


def _committed_status(db, folder_id):
    with contextlib.closing(sqlite3.connect(db._db_path)) as other:
        return other.execute(
            "SELECT status FROM folders WHERE id = ?", (folder_id,),
        ).fetchone()[0]


def test_landed_fingerprint_rows_reads_identity_of_existing_ids(db, catalog):
    rows = db.imports.landed_fingerprint_rows([catalog["raw"], catalog["jpg"], 9999])
    by_id = {row["id"]: dict(row) for row in rows}
    assert set(by_id) == {catalog["raw"], catalog["jpg"]}
    assert by_id[catalog["raw"]] == {
        "id": catalog["raw"], "folder_path": "/archive/2026",
        "filename": "bird.cr3", "file_size": 300, "file_hash": "h-raw",
    }


def test_photo_hash_row_is_exact_on_folder_and_filename(db, catalog):
    row = db.imports.photo_hash_row("/archive/2026", "fox.jpg")
    assert tuple(row) == (catalog["jpg"], "h-fox")
    assert db.imports.photo_hash_row("/archive/2026", "bird.jpg") is None
    assert db.imports.photo_hash_row("/other", "fox.jpg") is None


def test_photo_with_companion_finds_the_raw_holding_a_merged_jpeg(db, catalog):
    assert db.imports.photo_with_companion("/archive/2026", "bird.jpg")["id"] == catalog["raw"]
    assert db.imports.photo_with_companion("/other", "bird.jpg") is None
    assert db.imports.photo_with_companion("/archive/2026", "fox.jpg") is None


def test_folder_photo_identities_lists_one_folder(db, catalog):
    rows = db.imports.folder_photo_identities("/archive/2026")
    assert sorted(tuple(row) for row in rows) == sorted([
        (catalog["raw"], "bird.cr3", "bird.jpg", "h-raw"),
        (catalog["jpg"], "fox.jpg", None, "h-fox"),
    ])
    assert db.imports.folder_photo_identities("/nowhere") == []


def test_folders_lacking_working_copy_skips_photos_with_one(db, catalog):
    rows = db.imports.folders_lacking_working_copy(
        [catalog["raw"], catalog["jpg"], catalog["twin"]],
    )
    assert sorted(row["path"] for row in rows) == ["/archive/2026", "/other"]
    assert db.imports.folders_lacking_working_copy([catalog["jpg"]]) == []


def test_twin_rows_by_size_and_hash_carry_folder_path_and_status(db, catalog):
    rows = db.imports.twin_rows_by_size(100)
    assert sorted(row["id"] for row in rows) == sorted([catalog["jpg"], catalog["twin"]])
    assert set(rows[0].keys()) == {
        "id", "filename", "file_size", "timestamp", "file_hash",
        "folder_path", "folder_status",
    }
    rows = db.imports.twin_rows_by_hash("h-twin")
    assert [tuple(row) for row in rows] == [
        (catalog["twin"], "fox-copy.jpg", 100, "/other", "ok"),
    ]
    assert db.imports.twin_rows_by_hash("absent") == []


@pytest.mark.parametrize("by_path", [False, True])
def test_promote_missing_folder_only_clears_missing_and_never_commits(db, catalog, by_path):
    archive, other = catalog["archive"], catalog["other"]
    db.conn.execute("UPDATE folders SET status = 'missing' WHERE id = ?", (archive,))
    db.conn.execute("UPDATE folders SET status = 'partial' WHERE id = ?", (other,))
    db.conn.commit()
    if by_path:
        db.imports.promote_missing_folder_at("/archive/2026")
        db.imports.promote_missing_folder_at("/other")
    else:
        db.imports.promote_missing_folder(archive)
        db.imports.promote_missing_folder(other)
    assert db.imports.folder_status_row("/archive/2026")["status"] == "ok"
    assert db.imports.folder_status_row("/other")["status"] == "partial"
    # The write runs in the caller's transaction: nothing is committed yet.
    assert db.in_transaction
    assert _committed_status(db, archive) == "missing"
    db.commit()
    assert _committed_status(db, archive) == "ok"


def test_folder_status_row(db, catalog):
    row = db.imports.folder_status_row("/other")
    assert tuple(row) == (catalog["other"], "ok")
    assert db.imports.folder_status_row("/nowhere") is None


def test_imports_is_catalog_wide(db, catalog):
    db.set_active_workspace(None)
    assert db.imports.photo_hash_row("/archive/2026", "fox.jpg") is not None
    assert len(db.imports.twin_rows_by_size(100)) == 2


# -- structure ------------------------------------------------------------------


def test_imports_is_a_fresh_repository_on_the_connection_per_access(db):
    """``db.imports`` builds a new repository each time, never a cached one."""
    first, second = db.imports, db.imports
    assert isinstance(first, ImportRepository)
    assert first is not second
    assert first.conn is db.conn


def test_imports_accessor_only_builds_the_repository():
    accessor = Database.__dict__["imports"]
    assert isinstance(accessor, property)
    source = textwrap.dedent(inspect.getsource(accessor.fget))
    attrs = {
        node.attr
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id == "self"
    }
    assert "_imports_repository" in attrs
    assert "conn" not in attrs


def _import_job_sources():
    package = VIREO / "import_job"
    if package.is_dir():
        return sorted(package.glob("*.py"))
    return [VIREO / "import_job.py"]


def test_import_job_never_touches_the_connection():
    """The import job's SQL stays in ``repositories/imports.py``.

    Transaction control goes through ``db.commit()`` / ``db.rollback()``, so
    no ``<expr>.conn`` may reappear anywhere in the import job.
    """
    offenders = {}
    for path in _import_job_sources():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        lines = [
            node.lineno for node in ast.walk(tree)
            if isinstance(node, ast.Attribute) and node.attr == "conn"
        ]
        if lines:
            offenders[path.name] = lines
    assert not offenders, (
        f"import_job reaches the catalog connection again (file: lines): {offenders}. "
        "Add a method to repositories/imports.py and call it through db.imports."
    )
