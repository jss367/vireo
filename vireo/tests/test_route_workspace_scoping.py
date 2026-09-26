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


@pytest.mark.parametrize("nested", [False, True])
def test_audit_import_untracked_accepts_file_symlink(scoped, tmp_path, nested):
    """An image offered by Audit must import under its in-workspace parent,
    even when the image itself links to a file outside the workspace.
    """
    from PIL import Image

    db, client = scoped["db"], scoped["client"]
    root_dir = tmp_path / "shoot"
    root_dir.mkdir()
    db.add_folder(str(root_dir), name="shoot")
    parent = root_dir / "day" if nested else root_dir
    parent.mkdir(exist_ok=True)
    outside_dir = tmp_path / "elsewhere"
    outside_dir.mkdir()
    target = outside_dir / "original.jpg"
    Image.new("RGB", (16, 16), "red").save(target)
    path = parent / "linked.jpg"
    os.symlink(target, path)

    untracked = client.get("/api/audit/untracked")
    assert untracked.status_code == 200
    assert str(path) in {item["path"] for item in untracked.get_json()}

    response = client.post(
        "/api/audit/import-untracked", json={"paths": [str(path)]},
    )

    assert response.status_code == 200
    assert response.get_json()["imported"] == 1
    row = db.conn.execute(
        "SELECT f.path FROM photos p JOIN folders f ON f.id = p.folder_id "
        "WHERE p.filename = 'linked.jpg'"
    ).fetchone()
    assert row["path"] == os.path.realpath(parent)
    assert db.conn.execute(
        "SELECT COUNT(*) FROM folders WHERE path = ?",
        (os.path.realpath(outside_dir),),
    ).fetchone()[0] == 0
    assert str(path) not in {
        item["path"] for item in client.get("/api/audit/untracked").get_json()
    }


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


def _stub_final_check_conflict(monkeypatch, marker):
    """Make the final atomic ``local_copy_scan_conflict`` fail while the
    pre-flight passes.

    ``import-in-place`` and ``import-photos`` run a pre-flight check
    BEFORE ``_prepare_import_workspace`` (which is why a plain-overlap
    request never reaches the atomic block and the rollback path). The
    finding is about the RACE: a folder-stage request slips in between
    the pre-flight release and the atomic re-check. Simulate the race by
    stubbing ``local_copy_scan_conflict`` so only the final call (the one
    after workspace creation) reports a conflict.
    """
    from web import imports as imports_module

    calls = {"count": 0}

    def flaky_conflict(*args, **kwargs):
        calls["count"] += 1
        if calls["count"] < marker:
            return None
        return "simulated race: overlapping stage published between checks"

    monkeypatch.setattr(
        imports_module, "local_copy_scan_conflict", flaky_conflict,
    )
    return calls


def test_import_in_place_conflict_rolls_back_new_workspace(
    staged, monkeypatch, tmp_path,
):
    """``import-in-place`` with ``new_workspace_name`` creates the workspace
    and switches active to it BEFORE the final atomic conflict check inside
    ``stage_boundary_lock``. If that check rejects the request (a race with
    a folder-stage published after the pre-flight released the lock), the
    workspace and the active-workspace change must be undone — otherwise the
    409 leaves an orphan workspace and silently changes the user's active
    workspace even though no import was queued.
    """
    db = staged["db"]
    active_before = db._active_workspace_id
    workspaces_before = {int(ws["id"]) for ws in db.get_workspaces()}
    # A source outside the staged tree so the pre-flight passes; the
    # simulated race makes the atomic final check reject it anyway.
    fresh_source = tmp_path / "unrelated"
    fresh_source.mkdir()
    (fresh_source / "b.jpg").write_bytes(b"jpg")
    # Two calls fire inside the route (explicit-sources branch): the
    # pre-flight at request entry and the atomic re-check before
    # ``runner.start``. Trip only the second one.
    _stub_final_check_conflict(monkeypatch, marker=2)

    call_log = _stub_final_check_conflict(monkeypatch, marker=2)

    resp = staged["client"].post(
        "/api/jobs/import-in-place",
        json={
            "sources": [str(fresh_source)],
            "new_workspace_name": "Orphan In-Place",
            "after_import": None,
        },
    )

    assert call_log["count"] >= 2, (
        f"only saw {call_log['count']} conflict calls; "
        f"status={resp.status_code} body={resp.get_json()}"
    )
    assert resp.status_code == 409
    assert "simulated race" in resp.get_json()["error"]
    workspaces_after = {int(ws["id"]) for ws in db.get_workspaces()}
    assert workspaces_after == workspaces_before
    assert not any(
        ws["name"] == "Orphan In-Place" for ws in db.get_workspaces()
    )
    # A per-request Database is instantiated by the app, so re-check active
    # workspace through the API rather than the fixture db.
    active = staged["client"].get("/api/workspaces/active").get_json()
    assert int(active["id"]) == int(active_before)


def test_import_photos_conflict_rolls_back_new_workspace(
    staged, monkeypatch, tmp_path,
):
    """Same guarantee for ``import-photos``. A ``new_workspace_name`` request
    whose destination is claimed by a folder-stage after the pre-flight
    releases the boundary lock must not leave an orphan workspace or a
    silent active-workspace change behind.
    """
    db = staged["db"]
    active_before = db._active_workspace_id
    workspaces_before = {int(ws["id"]) for ws in db.get_workspaces()}
    card = tmp_path / "card"
    card.mkdir()
    (card / "DSC_0001.jpg").write_bytes(b"jpg")
    dest = tmp_path / "archive-photos"
    dest.mkdir()
    # Two calls fire inside the route: the pre-flight at request entry
    # and the atomic re-check before ``runner.start``. Trip only the second.
    _stub_final_check_conflict(monkeypatch, marker=2)

    resp = staged["client"].post(
        "/api/jobs/import-photos",
        json={
            "sources": [str(card)],
            "destination": str(dest),
            "after_import": None,
            "new_workspace_name": "Orphan Photos",
        },
    )

    assert resp.status_code == 409
    assert "simulated race" in resp.get_json()["error"]
    workspaces_after = {int(ws["id"]) for ws in db.get_workspaces()}
    assert workspaces_after == workspaces_before
    assert not any(
        ws["name"] == "Orphan Photos" for ws in db.get_workspaces()
    )
    active = staged["client"].get("/api/workspaces/active").get_json()
    assert int(active["id"]) == int(active_before)


def test_audit_import_untracked_refuses_paths_in_staged_source(staged):
    """``/api/audit/untracked`` reports the originals under a staged source
    because staging rebased the catalog rows to the local copy. Those
    originals then pass the workspace-containment check on
    ``/api/audit/import-untracked``: without a local-copy guard the audit
    action would scan the staged source's parent and recreate the
    original-path photo rows.
    """
    db = staged["db"]
    # Register the archive as a workspace root so the source's file passes
    # the containment check and would otherwise be importable.
    db.add_folder(staged["archive"], name="archive")
    original = os.path.join(staged["source"], "a.jpg")
    photos_before = db.conn.execute("SELECT COUNT(*) FROM photos").fetchone()[0]

    resp = staged["client"].post(
        "/api/audit/import-untracked", json={"paths": [original]},
    )

    assert resp.status_code == 409
    assert "local copy" in resp.get_json()["error"]
    assert db.conn.execute(
        "SELECT COUNT(*) FROM photos"
    ).fetchone()[0] == photos_before


def test_repair_metadata_refuses_root_containing_staged_source(
    staged, monkeypatch,
):
    """``/api/jobs/repair-metadata`` walks every reachable workspace root the
    same way ``/api/jobs/scan-workspace`` does. When a root contains another
    workspace's staged descendant, that walk would recreate the original-path
    rows the local copy replaced; the route must refuse before starting the
    job.
    """
    import metadata

    monkeypatch.setattr(metadata, "exiftool_status", lambda: {
        "available": True,
        "path": "/bundled/exiftool",
        "version": "13.59",
        "error": None,
        "hint": "",
    })
    db = staged["db"]
    # Register the archive as a workspace root so the metadata-repair job
    # would walk it (and its staged descendant) if not guarded.
    db.add_folder(staged["archive"], name="archive")

    resp = staged["client"].post("/api/jobs/repair-metadata")

    assert resp.status_code == 409
    assert "local copy" in resp.get_json()["error"]


def test_scan_conflict_loads_mappings_once_per_call(staged, monkeypatch):
    """``local_copy_scan_conflict`` scans ``local_folder_mappings`` once per
    call and reuses the loaded rows for every path. A snapshot import can
    pass thousands of frozen file paths; the pre-optimization loop repeated
    both the DB query and the ``realpath`` resolution for each path, so the
    O(N*M) cost dominated even when nothing overlapped.
    """
    from services import local_folder as sf

    db = staged["db"]
    calls = {"count": 0}
    real_load = sf._load_staged_source_index

    def counting_load(target_db):
        calls["count"] += 1
        return real_load(target_db)

    monkeypatch.setattr(sf, "_load_staged_source_index", counting_load)
    paths = [os.path.join(staged["archive"], f"day-{i:04d}") for i in range(50)]
    sf.local_copy_scan_conflict(db, paths)
    assert calls["count"] == 1


def test_scan_conflict_names_source_when_visible_to_workspace(staged):
    """When the mapping IS visible to the caller's workspace, the 409
    message names ``source_path`` so the user knows which local copy to
    sync or discard. The ``staged`` fixture's mapping is linked to the
    Default workspace (which is the active workspace here), so the
    caller can already see the source path.
    """
    from services.local_folder import local_copy_scan_conflict

    db = staged["db"]
    conflict = local_copy_scan_conflict(
        db, [staged["source"]],
        active_workspace_id=db._active_workspace_id,
    )
    assert conflict is not None
    assert staged["source"] in conflict


def test_scan_conflict_hides_source_path_from_foreign_workspace(staged):
    """The 409 message must not disclose the source path of a local copy
    the caller's workspace cannot otherwise see. Local mappings are keyed
    on source path, so a caller who never registered the archive could
    otherwise learn its absolute spelling by scanning any path near it.
    """
    from services.local_folder import local_copy_scan_conflict

    db = staged["db"]
    # Unlink the mapping's folder from every workspace so the caller's
    # active workspace cannot see it. In practice this reproduces the case
    # of a mapping that lives entirely in another workspace: the caller's
    # scan path happens to overlap its source, but that source path was
    # never surfaced to them.
    fid = db.conn.execute(
        "SELECT root_folder_id FROM local_folder_mappings WHERE is_root=1"
    ).fetchone()[0]
    db.conn.execute(
        "DELETE FROM workspace_folders WHERE folder_id=?",
        (fid,),
    )
    db.conn.commit()

    # Use ``archive`` (an ancestor of ``source``) as the caller's scan path
    # so we can distinguish the caller's own path (which the message may
    # echo) from the mapping's ``source_path`` (which it may not).
    conflict = local_copy_scan_conflict(
        db, [staged["archive"]],
        active_workspace_id=db._active_workspace_id,
    )
    assert conflict is not None
    assert staged["source"] not in conflict
    assert "another workspace" in conflict


def test_scan_conflict_sees_queued_folder_stage_job(staged, tmp_path):
    """A folder-stage job that has been queued but whose worker has not yet
    entered ``stage_folder`` has no ``local_folder_mappings`` row. Scan and
    import admissions that only read the mapping table would accept a path
    the queued stage is about to rebase, and the scan would then walk the
    staged originals once the mapping publishes.
    """
    from services.local_folder import (
        local_copy_scan_conflict,
        stage_pending_source_paths,
    )

    db = staged["db"]
    # Register a fresh folder that is NOT yet staged: no mapping row exists.
    pending_source = tmp_path / "queued-source"
    pending_source.mkdir()
    fid = db.add_folder(str(pending_source), name="queued")

    def list_jobs():
        return [
            {
                "id": "job-1",
                "type": "work-locally-folder-stage",
                "status": "queued",
                "workspace_id": 99,
                "config": {"root_folder_ids": [fid]},
            }
        ]

    pending = stage_pending_source_paths(list_jobs, db)
    assert str(pending_source) in pending

    conflict = local_copy_scan_conflict(
        db, [str(pending_source)],
        pending_stage_sources=pending,
    )
    assert conflict is not None
    assert "stage" in conflict.lower()


def test_scan_conflict_ignores_stage_job_in_terminal_state(staged, tmp_path):
    """A stage job that has completed, failed, or been cancelled no longer
    holds a claim on its source. ``stage_pending_source_paths`` filters by
    the queued/running/pausing/paused statuses so a stale entry in job
    history does not block every future scan.
    """
    from services.local_folder import stage_pending_source_paths

    db = staged["db"]
    pending_source = tmp_path / "done-source"
    pending_source.mkdir()
    fid = db.add_folder(str(pending_source), name="done")

    def list_jobs():
        return [
            {
                "id": "job-1",
                "type": "work-locally-folder-stage",
                "status": "completed",
                "workspace_id": 99,
                "config": {"root_folder_ids": [fid]},
            }
        ]

    assert stage_pending_source_paths(list_jobs, db) == []


@pytest.mark.parametrize("job_type,path_key", [
    ("scan", "roots"),
    ("scan", "root"),
    ("metadata-repair", "roots"),
    ("import-full", "source"),
    ("import-full", "destination"),
    ("import-in-place", "sources"),
    ("import", "sources"),
    ("import", "destination"),
])
def test_stage_admission_refuses_when_scan_registered_first(
    staged, tmp_path, monkeypatch, job_type, path_key,
):
    """``_busy_job`` includes queued/running scan and import jobs whose
    ``config`` paths overlap the stage source, even when those jobs live
    in a workspace that shares no folder with the mapping. Local mappings
    are keyed on source path, so cross-workspace overlap is what actually
    determines whether a scan will race a stage's catalog rebase.
    """
    db = staged["db"]
    # A fresh folder to stage: no mapping yet.
    pending_source = tmp_path / "to-stage"
    pending_source.mkdir()
    (pending_source / "b.jpg").write_bytes(b"jpg")
    stage_fid = db.add_folder(str(pending_source), name="to-stage")

    real_runner = staged["app"]._job_runner
    fake_scan_job = {
        "id": "scan-1",
        "type": job_type,
        "status": "running",
        "workspace_id": 99,
        "config": {
            path_key: [str(pending_source)]
            if path_key in {"roots", "sources"} else str(pending_source),
        },
        "blocks_local_transitions": True,
    }
    monkeypatch.setattr(
        real_runner, "list_jobs", lambda: [fake_scan_job],
    )

    resp = staged["client"].post(
        "/api/workspaces/active/local-folders/stage",
        json={"root_folder_ids": [stage_fid]},
    )
    assert resp.status_code == 409
    assert job_type in resp.get_json()["error"].lower()


def test_stage_admission_refuses_when_pipeline_registered_first(
    staged, tmp_path, monkeypatch,
):
    """A pipeline job in another workspace whose ``config`` records a
    ``source``/``sources``/``destination`` overlapping this stage source
    must be caught by ``_busy_job``. ``scanner_stage`` (broken metadata
    repair, snapshot / local_processing runs that ever land in the
    config) walks those paths, so a stage that rebases them mid-run would
    have its originals re-cataloged. Pipeline is in
    ``_PATH_CONFIG_JOB_TYPES`` so cross-workspace pipeline configs are
    consulted the same way import and scan configs already are.
    """
    db = staged["db"]
    pending_source = tmp_path / "to-stage-pipeline"
    pending_source.mkdir()
    (pending_source / "b.jpg").write_bytes(b"jpg")
    stage_fid = db.add_folder(str(pending_source), name="to-stage-pipeline")

    real_runner = staged["app"]._job_runner
    fake_pipeline_job = {
        "id": "pipeline-1",
        "type": "pipeline",
        "status": "running",
        "workspace_id": 99,
        "config": {"sources": [str(pending_source)]},
        "blocks_local_transitions": True,
    }
    monkeypatch.setattr(
        real_runner, "list_jobs", lambda: [fake_pipeline_job],
    )

    resp = staged["client"].post(
        "/api/workspaces/active/local-folders/stage",
        json={"root_folder_ids": [stage_fid]},
    )
    assert resp.status_code == 409
    assert "pipeline" in resp.get_json()["error"].lower()


def test_stage_admission_refuses_when_scan_overlaps_destination(
    staged, tmp_path, monkeypatch,
):
    """The stage-side overlap check must include the caller's chosen
    local destination, not just the folder's source path. If a scan or
    import in another workspace is already cataloging the tree the stage
    is about to copy files into, the stage worker would race that scan
    and either duplicate rows or fail on an already-created directory.
    ``_busy_job`` receives the computed final destinations via
    ``extra_stage_paths`` so cross-workspace conflicts on the
    destination are caught the same way overlaps on the source are.
    """
    from services.local_folder import local_path_for_base

    db = staged["db"]
    pending_source = tmp_path / "to-stage-destination"
    pending_source.mkdir()
    (pending_source / "b.jpg").write_bytes(b"jpg")
    stage_fid = db.add_folder(str(pending_source), name="to-stage-destination")
    destination_base = tmp_path / "custom-destination"
    destination_base.mkdir()
    final_destination = str(local_path_for_base(
        str(destination_base), stage_fid, str(pending_source),
    ))

    real_runner = staged["app"]._job_runner
    fake_scan_job = {
        "id": "scan-1",
        "type": "scan",
        "status": "running",
        "workspace_id": 99,
        # The scan's root is the parent of our destination -- it will
        # walk into the destination while the stage worker is writing to
        # it, so admission must refuse the stage.
        "config": {"roots": [str(destination_base)]},
        "blocks_local_transitions": True,
    }
    monkeypatch.setattr(
        real_runner, "list_jobs", lambda: [fake_scan_job],
    )

    resp = staged["client"].post(
        "/api/workspaces/active/local-folders/stage",
        json={
            "root_folder_ids": [stage_fid],
            "destination_bases": {str(stage_fid): str(destination_base)},
        },
    )
    assert resp.status_code == 409
    assert "scan" in resp.get_json()["error"].lower()
    # Sanity: nothing should have been queued or written to the destination.
    assert not (
        tmp_path / "custom-destination" / os.path.basename(final_destination)
    ).exists() or list(
        (tmp_path / "custom-destination" / os.path.basename(final_destination)).iterdir()
    ) == []


def test_scan_conflict_sees_queued_folder_stage_destination(staged, tmp_path):
    """A queued folder-stage job records its chosen local destination in
    ``config.destination_paths`` so another workspace's scan or import
    admission can reserve it even before the mapping row publishes.
    ``stage_pending_source_paths`` returns both the source and the
    destination for the same reason ``local_copy_scan_conflict`` blocks
    the pending source: either path becomes catalog-unsafe once the
    stage worker begins writing.
    """
    from services.local_folder import (
        local_copy_scan_conflict,
        stage_pending_source_paths,
    )

    db = staged["db"]
    pending_dest = tmp_path / "queued-dest" / "photos"
    pending_dest.parent.mkdir()

    def list_jobs():
        return [
            {
                "id": "job-1",
                "type": "work-locally-folder-stage",
                "status": "queued",
                "workspace_id": 99,
                "config": {
                    "root_folder_ids": [],
                    "destination_paths": [str(pending_dest)],
                },
            }
        ]

    pending = stage_pending_source_paths(list_jobs, db)
    assert str(pending_dest) in pending

    conflict = local_copy_scan_conflict(
        db, [str(pending_dest)],
        pending_stage_sources=pending,
    )
    assert conflict is not None
    assert "stage" in conflict.lower()

    # An unrelated path is still allowed.
    unrelated = tmp_path / "unrelated"
    unrelated.mkdir()
    assert (
        local_copy_scan_conflict(
            db, [str(unrelated)], pending_stage_sources=pending,
        )
        is None
    )
