"""Tests for POST /api/folders/reveal — bulk reveal of folder paths in
the OS file manager. Backs the bulk-decide UI's "Reveal in Finder" button.
"""

from unittest.mock import MagicMock, patch


def _seed_folder(db, path):
    """Add a folder by path so the reveal endpoint's path-validation
    accepts it. Returns the folder id."""
    return db.add_folder(path)


def test_folders_reveal_single_path_macos(app_and_db):
    """A single known folder path triggers ``open -R -- <path>`` on macOS."""
    app, db = app_and_db
    _seed_folder(db, "/tmp/dupreveal_one")

    with app.test_client() as c, \
         patch("vireo.app.sys.platform", "darwin"), \
         patch("vireo.app.subprocess.run") as run:
        run.return_value = MagicMock(returncode=0)
        resp = c.post("/api/folders/reveal",
                      json={"paths": ["/tmp/dupreveal_one"]})
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["ok"] is True
        assert body["revealed"] == ["/tmp/dupreveal_one"]
        assert body["skipped"] == []
        assert body["failed"] == []
        # One subprocess call, with the canonical macOS argv.
        assert run.call_count == 1
        argv = run.call_args[0][0]
        assert argv == ["open", "-R", "--", "/tmp/dupreveal_one"]


def test_folders_reveal_multiple_paths_calls_each(app_and_db):
    """Two paths in one request → two subprocess calls, both reported as
    revealed. Lets the bulk-decide button open every folder in a bucket
    with one click."""
    app, db = app_and_db
    _seed_folder(db, "/tmp/dupreveal_a")
    _seed_folder(db, "/tmp/dupreveal_b")

    with app.test_client() as c, \
         patch("vireo.app.sys.platform", "darwin"), \
         patch("vireo.app.subprocess.run") as run:
        run.return_value = MagicMock(returncode=0)
        resp = c.post("/api/folders/reveal", json={
            "paths": ["/tmp/dupreveal_a", "/tmp/dupreveal_b"],
        })
        assert resp.status_code == 200
        body = resp.get_json()
        assert sorted(body["revealed"]) == ["/tmp/dupreveal_a", "/tmp/dupreveal_b"]
        assert run.call_count == 2


def test_folders_reveal_unknown_path_skipped_not_failed(app_and_db):
    """Refusing to reveal arbitrary filesystem paths is the security
    boundary: only paths that exist in the folders table get revealed.
    Unknowns are reported in ``skipped`` so the UI can surface a partial-
    success summary instead of a hard failure."""
    app, db = app_and_db
    _seed_folder(db, "/tmp/dupreveal_known")

    with app.test_client() as c, \
         patch("vireo.app.sys.platform", "darwin"), \
         patch("vireo.app.subprocess.run") as run:
        run.return_value = MagicMock(returncode=0)
        resp = c.post("/api/folders/reveal", json={
            "paths": ["/tmp/dupreveal_known", "/etc/passwd"],
        })
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["revealed"] == ["/tmp/dupreveal_known"]
        assert body["skipped"] == [
            {"path": "/etc/passwd", "reason": "not a known folder"}
        ]
        # Only the known path triggers a subprocess call.
        assert run.call_count == 1


def test_folders_reveal_shell_failure_reports_per_path(app_and_db):
    """If the subprocess raises for one path, the rest of the batch is
    still attempted; the failure is surfaced per-path so the user can
    see exactly which folder couldn't be opened."""
    app, db = app_and_db
    _seed_folder(db, "/tmp/dupreveal_ok")
    _seed_folder(db, "/tmp/dupreveal_fail")

    call_count = {"n": 0}

    def fake_run(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 2:
            raise FileNotFoundError("no 'open'")
        return MagicMock(returncode=0)

    with app.test_client() as c, \
         patch("vireo.app.sys.platform", "darwin"), \
         patch("vireo.app.subprocess.run", side_effect=fake_run):
        resp = c.post("/api/folders/reveal", json={
            "paths": ["/tmp/dupreveal_ok", "/tmp/dupreveal_fail"],
        })
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["revealed"] == ["/tmp/dupreveal_ok"]
        assert len(body["failed"]) == 1
        assert body["failed"][0]["path"] == "/tmp/dupreveal_fail"
        assert "reason" in body["failed"][0]


def test_folders_reveal_allows_cross_workspace_paths_known_to_library(app_and_db):
    """Duplicate scans are library-wide (``file_hash`` is global across
    workspaces), so a bucket can legitimately surface folders linked
    only to another workspace. Reveal must still succeed for those —
    otherwise bulk-decide buckets that span workspaces become
    un-revealable. The "must exist in folders table" check is enough
    to keep arbitrary filesystem-path probes off this endpoint, and
    Vireo is single-user so workspaces are organizational, not access
    boundaries."""
    app, db = app_and_db
    default_ws = db._active_workspace_id
    other_ws = db.create_workspace("Other")
    db.set_active_workspace(other_ws)
    db.add_folder("/tmp/cross-ws-bucket")
    db.set_active_workspace(default_ws)

    with app.test_client() as c, \
         patch("vireo.app.sys.platform", "darwin"), \
         patch("vireo.app.subprocess.run") as run:
        run.return_value = MagicMock(returncode=0)
        resp = c.post("/api/folders/reveal",
                      json={"paths": ["/tmp/cross-ws-bucket"]})
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["revealed"] == ["/tmp/cross-ws-bucket"]
        assert body["skipped"] == []


def test_folders_reveal_non_zero_exit_reports_failure(app_and_db):
    """``subprocess.run(..., check=False)`` returns a CompletedProcess for
    every exit code, success or not. The endpoint must inspect
    ``returncode`` so a path that fails to open (e.g. unmounted volume,
    disappeared on disk) lands in ``failed`` instead of ``revealed`` —
    otherwise the UI shows nothing happened but reports success."""
    app, db = app_and_db
    _seed_folder(db, "/tmp/dupreveal_nonzero")

    with app.test_client() as c, \
         patch("vireo.app.sys.platform", "darwin"), \
         patch("vireo.app.subprocess.run") as run:
        run.return_value = MagicMock(returncode=1)
        resp = c.post("/api/folders/reveal",
                      json={"paths": ["/tmp/dupreveal_nonzero"]})
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["revealed"] == []
        assert len(body["failed"]) == 1
        assert body["failed"][0]["path"] == "/tmp/dupreveal_nonzero"
        assert "reason" in body["failed"][0]


def test_folders_reveal_normalizes_path_trailing_slash(app_and_db):
    """The bucket UI passes folders derived from ``os.path.dirname(...)`` —
    never trailing-slashed. ``folders.path`` rows can carry a trailing
    separator from manual relocation or legacy imports. A naive string
    compare silently treats those as unknown and Reveal in Finder
    becomes a no-op for affected users — same trap that
    bulk_resolve_by_folder fell into."""
    app, db = app_and_db
    # Folder stored WITH trailing slash.
    _seed_folder(db, "/tmp/dupreveal_slash/")

    with app.test_client() as c, \
         patch("vireo.app.sys.platform", "darwin"), \
         patch("vireo.app.subprocess.run") as run:
        run.return_value = MagicMock(returncode=0)
        # Caller passes the un-slashed form (the form the bucket UI derives).
        resp = c.post("/api/folders/reveal",
                      json={"paths": ["/tmp/dupreveal_slash"]})
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["revealed"] == ["/tmp/dupreveal_slash"]
        assert body["skipped"] == []


def test_folders_reveal_validates_inputs(app_and_db):
    """Missing body / empty list / non-string entries → 400."""
    app, _ = app_and_db
    with app.test_client() as c:
        # No body
        assert c.post("/api/folders/reveal").status_code == 400
        # Missing paths
        assert c.post("/api/folders/reveal", json={}).status_code == 400
        # Empty list
        assert c.post("/api/folders/reveal",
                      json={"paths": []}).status_code == 400
        # Non-string entry
        assert c.post("/api/folders/reveal",
                      json={"paths": [123]}).status_code == 400
        # Empty string entry
        assert c.post("/api/folders/reveal",
                      json={"paths": [""]}).status_code == 400
