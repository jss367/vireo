"""Routes that act on global photos/folders refuse ids the active workspace
cannot see, and scans refuse trees that a folder-level local copy has
rebased.

Photos and folders are global, but every write below either queues work
under the active workspace or rewrites state every workspace shares, so an
id from another workspace (or an unvalidated path) must be refused rather
than silently acted on.
"""

import os
import time

import pytest


@pytest.fixture
def scoped(app_and_db, tmp_path):
    """The fixture app plus a photo and folder visible only to "Other"."""
    app, db = app_and_db
    default_ws = db._active_workspace_id
    visible_pid = db.conn.execute(
        "SELECT id FROM photos ORDER BY id LIMIT 1"
    ).fetchone()[0]

    other_ws = db.create_workspace("Other")
    db.set_active_workspace(other_ws)
    other_dir = tmp_path / "other"
    other_dir.mkdir()
    other_fid = db.add_folder(str(other_dir), name="other")
    foreign_pid = db.add_photo(
        folder_id=other_fid, filename="foreign.jpg", extension=".jpg",
        file_size=10, file_mtime=1.0,
    )
    # Hand the "last opened" workspace back to Default so the per-request
    # Database restores it.
    time.sleep(0.01)
    db.set_active_workspace(default_ws)
    assert db.filter_photo_ids_in_workspace([foreign_pid]) == []

    return {
        "app": app,
        "db": db,
        "client": app.test_client(),
        "visible_pid": visible_pid,
        "foreign_pid": foreign_pid,
        "other_fid": other_fid,
        "other_dir": str(other_dir),
    }


# -- /masks/<filename> -------------------------------------------------------


def _write_mask(db, tmp_path, pid):
    masks_dir = tmp_path / "masks"
    masks_dir.mkdir(exist_ok=True)
    mask = masks_dir / f"{pid}.png"
    mask.write_bytes(b"\x89PNG\r\n\x1a\nmask")
    db.conn.execute(
        "UPDATE photos SET mask_path = ? WHERE id = ?", (str(mask), pid)
    )
    db.conn.commit()


def test_mask_served_only_for_active_workspace_photo(scoped, tmp_path):
    db, client = scoped["db"], scoped["client"]
    _write_mask(db, tmp_path, scoped["visible_pid"])
    _write_mask(db, tmp_path, scoped["foreign_pid"])

    assert client.get(f"/masks/{scoped['visible_pid']}.png").status_code == 200
    assert client.get(f"/masks/{scoped['foreign_pid']}.png").status_code == 404


def test_mask_without_photo_id_prefix_is_not_served(scoped, tmp_path):
    masks_dir = tmp_path / "masks"
    masks_dir.mkdir(exist_ok=True)
    (masks_dir / "legacy.png").write_bytes(b"\x89PNG\r\n\x1a\nmask")

    assert scoped["client"].get("/masks/legacy.png").status_code == 404


def test_masks_behind_browser_session_guard(scoped, tmp_path):
    app, db = scoped["app"], scoped["db"]
    _write_mask(db, tmp_path, scoped["visible_pid"])
    app.config["BROWSER_AUTH_ENABLED"] = True

    resp = app.test_client().get(
        f"/masks/{scoped['visible_pid']}.png",
        headers={"Origin": "https://evil.example", "Sec-Fetch-Site": "cross-site"},
    )

    assert resp.status_code in (401, 403)


# -- /api/audit/resolve and /api/audit/import-untracked ----------------------


def test_audit_resolve_refuses_foreign_photo(scoped):
    db = scoped["db"]
    resp = scoped["client"].post(
        "/api/audit/resolve",
        json={"photo_id": scoped["foreign_pid"], "direction": "use_db"},
    )
    assert resp.status_code == 404
    assert db.conn.execute(
        "SELECT COUNT(*) FROM pending_changes WHERE photo_id = ?",
        (scoped["foreign_pid"],),
    ).fetchone()[0] == 0


@pytest.mark.parametrize("path_kind", ["outside", "relative", "other_workspace"])
def test_audit_import_untracked_refuses_paths_outside_workspace(
    scoped, tmp_path, path_kind,
):
    db = scoped["db"]
    if path_kind == "outside":
        stray = tmp_path / "anywhere"
        stray.mkdir()
        (stray / "x.jpg").write_bytes(b"jpg")
        path = str(stray / "x.jpg")
    elif path_kind == "relative":
        path = "anywhere/x.jpg"
    else:
        path = os.path.join(scoped["other_dir"], "new.jpg")
        with open(path, "wb") as f:
            f.write(b"jpg")
    folders_before = db.conn.execute("SELECT COUNT(*) FROM folders").fetchone()[0]

    resp = scoped["client"].post(
        "/api/audit/import-untracked", json={"paths": [path]},
    )

    assert resp.status_code == 400
    assert db.conn.execute(
        "SELECT COUNT(*) FROM folders"
    ).fetchone()[0] == folders_before


def test_audit_import_untracked_rejects_non_list_paths(scoped):
    resp = scoped["client"].post(
        "/api/audit/import-untracked", json={"paths": "/photos/2024/x.jpg"},
    )
    assert resp.status_code == 400


def test_audit_import_untracked_rejects_symlink_escape(scoped, tmp_path):
    """A directory symlink inside a workspace root that points outside must
    not smuggle an out-of-tree path past the containment check: the scanner
    later canonicalizes the parent through the link and would otherwise
    catalog files under a directory the workspace does not actually cover.
    """
    db = scoped["db"]
    root_dir = tmp_path / "shoot"
    root_dir.mkdir()
    db.add_folder(str(root_dir), name="shoot")
    outside_dir = tmp_path / "elsewhere"
    outside_dir.mkdir()
    (outside_dir / "x.jpg").write_bytes(b"jpg")
    os.symlink(str(outside_dir), str(root_dir / "link"))
    path = str(root_dir / "link" / "x.jpg")
    folders_before = db.conn.execute("SELECT COUNT(*) FROM folders").fetchone()[0]

    resp = scoped["client"].post(
        "/api/audit/import-untracked", json={"paths": [path]},
    )

    assert resp.status_code == 400
    assert db.conn.execute(
        "SELECT COUNT(*) FROM folders"
    ).fetchone()[0] == folders_before


# -- collections and highlights ----------------------------------------------


def _static_collection(db):
    return db.add_collection(
        "Static", '[{"field": "photo_ids", "value": []}]',
    )


def test_collection_add_photos_refuses_foreign_and_unknown_ids(scoped):
    db = scoped["db"]
    cid = _static_collection(db)
    client = scoped["client"]

    for bad in (scoped["foreign_pid"], 999999):
        resp = client.post(
            f"/api/collections/{cid}/add-photos",
            json={"photo_ids": [scoped["visible_pid"], bad]},
        )
        assert resp.status_code == 403, bad

    rules = db.conn.execute(
        "SELECT rules FROM collections WHERE id = ?", (cid,)
    ).fetchone()[0]
    assert str(scoped["foreign_pid"]) not in rules
    assert "999999" not in rules


def test_collection_photos_404_for_unknown_or_foreign_collection(scoped):
    db, client = scoped["db"], scoped["client"]
    default_ws = db._active_workspace_id
    other_ws = db.conn.execute(
        "SELECT id FROM workspaces WHERE name = 'Other'"
    ).fetchone()[0]
    db.set_active_workspace(other_ws)
    foreign_cid = _static_collection(db)
    time.sleep(0.01)
    db.set_active_workspace(default_ws)
    own_cid = _static_collection(db)

    assert client.get(f"/api/collections/{own_cid}/photos").status_code == 200
    for cid in (foreign_cid, 999999):
        assert client.get(f"/api/collections/{cid}/photos").status_code == 404
        v1 = client.get(
            f"/api/v1/collections/{cid}/photos",
            headers={"X-Vireo-Token": "test-token-123"},
        )
        assert v1.status_code == 404


@pytest.mark.parametrize("photo_ids", [[[1]], ["1"], [True], "1"])
def test_highlights_save_rejects_malformed_photo_ids(scoped, photo_ids):
    db = scoped["db"]
    before = db.conn.execute("SELECT COUNT(*) FROM collections").fetchone()[0]

    resp = scoped["client"].post(
        "/api/highlights/save", json={"photo_ids": photo_ids, "name": "Best"},
    )

    assert resp.status_code == 400
    assert db.conn.execute(
        "SELECT COUNT(*) FROM collections"
    ).fetchone()[0] == before


def test_highlights_save_refuses_foreign_photo(scoped):
    db = scoped["db"]
    before = db.conn.execute("SELECT COUNT(*) FROM collections").fetchone()[0]

    resp = scoped["client"].post(
        "/api/highlights/save",
        json={"photo_ids": [scoped["visible_pid"], scoped["foreign_pid"]],
              "name": "Best"},
    )

    assert resp.status_code == 403
    assert db.conn.execute(
        "SELECT COUNT(*) FROM collections"
    ).fetchone()[0] == before


# -- /api/encounters/species -------------------------------------------------


def test_encounter_species_refuses_foreign_photo(scoped):
    db = scoped["db"]
    resp = scoped["client"].post(
        "/api/encounters/species",
        json={"species": "Great Egret", "photo_ids": [scoped["foreign_pid"]]},
    )

    assert resp.status_code == 403
    assert db.conn.execute(
        "SELECT COUNT(*) FROM photo_keywords WHERE photo_id = ?",
        (scoped["foreign_pid"],),
    ).fetchone()[0] == 0


# -- /api/folders/<id>/relocate ----------------------------------------------


def test_relocate_refuses_folder_outside_active_workspace(scoped, tmp_path):
    db = scoped["db"]
    target = tmp_path / "moved"
    target.mkdir()

    resp = scoped["client"].post(
        f"/api/folders/{scoped['other_fid']}/relocate",
        json={"path": str(target)},
    )

    assert resp.status_code == 404
    assert db.conn.execute(
        "SELECT path FROM folders WHERE id = ?", (scoped["other_fid"],)
    ).fetchone()[0] == scoped["other_dir"]


def test_relocate_rejects_relative_path(scoped, tmp_path, monkeypatch):
    db = scoped["db"]
    fid = db.conn.execute(
        "SELECT id FROM folders WHERE path = '/photos/2024'"
    ).fetchone()[0]
    (tmp_path / "rel").mkdir()
    monkeypatch.chdir(tmp_path)

    resp = scoped["client"].post(
        f"/api/folders/{fid}/relocate", json={"path": "rel"},
    )

    assert resp.status_code == 400
    assert db.conn.execute(
        "SELECT path FROM folders WHERE id = ?", (fid,)
    ).fetchone()[0] == "/photos/2024"


# -- /api/jobs/move-photos ---------------------------------------------------


def test_move_photos_refuses_foreign_photo(scoped, tmp_path):
    db = scoped["db"]
    dest = tmp_path / "dest"
    dest.mkdir()

    resp = scoped["client"].post(
        "/api/jobs/move-photos",
        json={"photo_ids": [scoped["foreign_pid"]], "destination": str(dest)},
    )

    assert resp.status_code == 403
    assert db.conn.execute(
        "SELECT folder_id FROM photos WHERE id = ?", (scoped["foreign_pid"],)
    ).fetchone()[0] == scoped["other_fid"]


@pytest.mark.parametrize("photo_ids", [None, [], ["1"], 5])
def test_move_photos_rejects_malformed_photo_ids(scoped, tmp_path, photo_ids):
    dest = tmp_path / "dest"
    dest.mkdir()
    resp = scoped["client"].post(
        "/api/jobs/move-photos",
        json={"photo_ids": photo_ids, "destination": str(dest)},
    )
    assert resp.status_code == 400


# -- scans over a folder-level local copy's original tree --------------------


@pytest.fixture
def staged(scoped, tmp_path):
    """A folder whose catalog rows were rebased onto a local copy."""
    db = scoped["db"]
    archive = tmp_path / "archive"
    source = archive / "2024-05-01"
    source.mkdir(parents=True)
    (source / "a.jpg").write_bytes(b"jpg")
    local = tmp_path / "local-folders" / "2024-05-01"
    local.mkdir(parents=True)
    fid = db.add_folder(str(local), name="2024-05-01")
    db.conn.execute(
        "INSERT INTO local_folders (root_folder_id, state, created_at) "
        "VALUES (?, 'active', ?)",
        (fid, time.time()),
    )
    db.conn.execute(
        "INSERT INTO local_folder_mappings "
        "(root_folder_id, folder_id, source_path, local_path, is_root) "
        "VALUES (?, ?, ?, ?, 1)",
        (fid, fid, str(source), str(local)),
    )
    db.conn.commit()
    return {**scoped, "archive": str(archive), "source": str(source)}


@pytest.mark.parametrize("which", ["source", "inside", "ancestor"])
def test_scan_refuses_tree_overlapping_local_copy(staged, which):
    db = staged["db"]
    if which == "source":
        root = staged["source"]
    elif which == "inside":
        root = os.path.join(staged["source"], "sub")
        os.mkdir(root)
    else:
        root = staged["archive"]
    photos_before = db.conn.execute("SELECT COUNT(*) FROM photos").fetchone()[0]

    resp = staged["client"].post("/api/jobs/scan", json={"root": root})

    assert resp.status_code == 409
    assert "local copy" in resp.get_json()["error"]
    assert db.conn.execute(
        "SELECT COUNT(*) FROM photos"
    ).fetchone()[0] == photos_before


def test_scan_of_unrelated_tree_is_not_blocked(staged, tmp_path):
    from services.local_folder import local_copy_scan_conflict

    unrelated = tmp_path / "elsewhere"
    unrelated.mkdir()
    # A sibling whose name shares the staged folder's prefix is not inside it.
    sibling = staged["source"] + "-b"
    os.mkdir(sibling)

    assert local_copy_scan_conflict(staged["db"], [str(unrelated)]) is None
    assert local_copy_scan_conflict(staged["db"], [sibling]) is None


def test_copy_import_blocks_ancestor_and_inside_of_staged_folder(staged):
    """The two copy-import routes call ``local_copy_scan_conflict`` without
    ``include_descendants=False`` so that a destination that merely contains
    a staged day folder is refused too: ``folder_template`` can render a
    dated folder that coincides with the staged source (``%Y-%m-%d`` when
    ``/archive/2024-05-01`` is staged and a photo was taken that day), and
    then the import would copy into the original source and scan it.
    """
    from services.local_folder import local_copy_scan_conflict

    db = staged["db"]
    assert local_copy_scan_conflict(db, [staged["archive"]]) is not None
    assert local_copy_scan_conflict(db, [staged["source"]]) is not None


def test_scan_over_symlink_alias_of_staged_source_is_refused(staged, tmp_path):
    """A symlink alias of a staged source must not slip past the lexical guard.

    Without a physical (``realpath``) comparison the scanner would resolve
    catalog folder paths through the alias, reach the staged originals, and
    catalog them a second time despite the local-copy mapping.
    """
    from services.local_folder import local_copy_scan_conflict

    alias_dir = tmp_path / "alias-dir"
    alias_dir.mkdir()
    alias = alias_dir / "alias-day"
    os.symlink(staged["source"], alias)
    inside_alias = str(alias / "sub")

    # Scanning through the symlink to the staged source itself is refused.
    assert local_copy_scan_conflict(staged["db"], [str(alias)]) is not None
    # And so is scanning through a subpath of that alias.
    assert local_copy_scan_conflict(staged["db"], [inside_alias]) is not None


def test_folder_rescan_refuses_folder_containing_local_copy(staged):
    db = staged["db"]
    archive_fid = db.add_folder(staged["archive"], name="archive")

    resp = staged["client"].post(f"/api/folders/{archive_fid}/rescan", json={})

    assert resp.status_code == 409


def test_snapshot_import_refuses_paths_inside_staged_source(staged):
    """``import-in-place`` with a ``source_snapshot_id`` restricts the scan to
    the frozen file paths, but the scanner canonicalizes their parent folders.
    A snapshot captured before a descendant was staged would still catalog
    those originals a second time. The route must refuse when any snapshot
    path falls within a staged source, mirroring the explicit-``sources``
    guard.
    """
    db = staged["db"]
    # Register the archive so the snapshot's frozen path resolves to a
    # workspace root, exactly like the "captured before staging" case.
    db.add_folder(staged["archive"], name="archive")
    frozen = os.path.join(staged["source"], "a.jpg")
    snap_id = db.create_new_images_snapshot([frozen])

    resp = staged["client"].post(
        "/api/jobs/import-in-place",
        json={"source_snapshot_id": snap_id, "after_import": None},
    )

    assert resp.status_code == 409
    assert "local copy" in resp.get_json()["error"]
