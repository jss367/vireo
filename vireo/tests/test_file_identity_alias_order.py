"""Catalog alias order uses the original path-only query; no filesystem reads."""

import posixpath
import sqlite3
from types import SimpleNamespace


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
            get_all_folders=lambda: conn.execute("SELECT id, path, name FROM folders").fetchall(),
            get_folder_id_by_path=lambda path: None,
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
