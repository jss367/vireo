"""Catalog alias order uses the original path-only query; no filesystem reads."""

import os
import posixpath
import sqlite3
import sys
from types import SimpleNamespace

import pytest


def test_alias_selection_preserves_path_only_catalog_order(monkeypatch):
    import file_identity
    from repositories.local_folders import LocalFolderRepository

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("CREATE TABLE folders(id INTEGER PRIMARY KEY, path TEXT UNIQUE, name TEXT)")
        conn.executemany("INSERT INTO folders VALUES(?,?,?)", [
            (1, "/invented/alias", "lower"), (2, "/invented/ALIAS", "upper"),
        ])
        conn.commit()
        original = [row["path"] for row in conn.execute("SELECT path FROM folders")]
        db = SimpleNamespace(
            conn=conn,
            local_folders=LocalFolderRepository(conn),
            folders=SimpleNamespace(all_rows=lambda: conn.execute("SELECT id, path, name FROM folders").fetchall(), id_by_path=lambda path: None),
        )
        fake_path = SimpleNamespace(
            normpath=posixpath.normpath, abspath=lambda path: path,
            realpath=lambda path: "/invented/Alias", samefile=lambda left, right: True,
        )
        monkeypatch.setattr(file_identity, "os", SimpleNamespace(fspath=str, path=fake_path))
        aliases = file_identity.catalog_folder_aliases(db)
        assert aliases["/invented/alias"] == original
        assert file_identity.catalog_folder_path(db, "/invented/request") == original[0]
        for spelling in original:
            assert file_identity.catalog_folder_path(db, spelling, aliases=aliases) == spelling
    finally:
        conn.close()


def test_catalog_scan_roots_rejects_excluded_bundle_before_realpath(
    tmp_path, monkeypatch,
):
    """A symlink retargeted to a macOS other-app bundle between the
    request-boundary ``is_excluded_scan_path`` guard and the deferred
    ``catalog_scan_roots`` mapping (deferred working-copy extraction and
    ``invalidate_new_images_after_scan``) must not trigger a ``realpath``
    stat of the bundle. The in-function textual exclusion check skips the
    alias canonicalization path so no filesystem walk reaches the bundle
    target.
    """
    if sys.platform == "win32":
        pytest.skip("POSIX symlinks required")
    import file_identity

    bundle = tmp_path / "Photos Library.photoslibrary"
    (bundle / "originals").mkdir(parents=True)
    link = tmp_path / "mylib"
    os.symlink(str(bundle), str(link))

    db = SimpleNamespace(
        folders=SimpleNamespace(id_by_path=lambda path: None),
        local_folders=SimpleNamespace(catalog_paths=lambda: []),
    )

    def fail_realpath(_path):
        raise AssertionError(
            "os.path.realpath must not be called on a retargeted symlink "
            "to a macOS protected bundle"
        )

    monkeypatch.setattr(file_identity.os.path, "realpath", fail_realpath)
    assert file_identity.catalog_scan_roots(db, [str(link)]) == [str(link)]


def test_catalog_scan_roots_resolves_a_non_excluded_symlink(tmp_path):
    """The exclusion short-circuit must not break the ordinary path:
    a symlink to a normal folder still resolves through ``realpath``."""
    if sys.platform == "win32":
        pytest.skip("POSIX symlinks required")
    import file_identity

    target = tmp_path / "card"
    target.mkdir()
    link = tmp_path / "linked"
    os.symlink(str(target), str(link))

    db = SimpleNamespace(
        folders=SimpleNamespace(id_by_path=lambda path: None),
        local_folders=SimpleNamespace(catalog_paths=lambda: []),
    )
    assert file_identity.catalog_scan_roots(db, [str(link)]) == [
        os.path.realpath(str(link))
    ]
