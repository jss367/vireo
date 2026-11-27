"""Pre-upgrade catalog snapshots: how many are kept and where they go."""

import os

import catalog_backups
import pytest
from wait import wait_for_job_via_runner


def _write(path, text="snapshot"):
    with open(path, "w") as f:
        f.write(text)
    return path


@pytest.fixture
def catalog(tmp_path):
    catalog_dir = tmp_path / "data"
    catalog_dir.mkdir()
    return str(catalog_dir / "vireo.db")


@pytest.fixture
def backup_dir(tmp_path):
    folder = tmp_path / "nas" / "Vireo backups"
    folder.mkdir(parents=True)
    return str(folder)


def test_prune_counts_both_folders_toward_one_limit(catalog, backup_dir):
    """The snapshot still waiting beside the catalog and the older ones in
    the backup folder share the three-version limit."""
    catalog_dir = os.path.dirname(catalog)
    _write(f"{catalog}.pre-v10.bak")
    for version in (9, 8, 7):
        _write(os.path.join(backup_dir, f"vireo.db.pre-v{version}.bak"))

    removed = catalog_backups.prune([catalog_dir, backup_dir], "vireo.db", 10)

    assert removed == [os.path.join(backup_dir, "vireo.db.pre-v7.bak")]
    assert sorted(b.version for b in catalog_backups.list_backups(backup_dir, "vireo.db")) == [8, 9]


def test_prune_ignores_other_catalogs_and_newer_versions(catalog, backup_dir):
    catalog_dir = os.path.dirname(catalog)
    for version in (12, 6, 5, 4, 3):
        _write(f"{catalog}.pre-v{version}.bak")
    other = _write(os.path.join(catalog_dir, "other.db.pre-v1.bak"))

    catalog_backups.prune([catalog_dir], "vireo.db", 10)

    assert [b.version for b in catalog_backups.list_backups(catalog_dir, "vireo.db")] == [12, 6, 5, 4]
    assert os.path.exists(other)


def test_offload_moves_snapshots_and_verifies_the_copy(catalog, backup_dir):
    local = _write(f"{catalog}.pre-v10.bak", "catalog bytes")
    calls = []

    result = catalog_backups.offload(catalog, backup_dir, lambda *args: calls.append(args))

    dest = os.path.join(backup_dir, "vireo.db.pre-v10.bak")
    assert result.moved == [dest]
    assert result.errors == []
    assert result.bytes_copied == len("catalog bytes")
    assert not os.path.exists(local)
    with open(dest) as f:
        assert f.read() == "catalog bytes"
    assert not os.path.exists(dest + ".tmp")
    assert calls[-1] == ("vireo.db.pre-v10.bak", 13, 13)


def test_offload_never_creates_a_missing_backup_folder(catalog, tmp_path):
    """An unplugged drive's folder is gone; creating it would put the
    backups on the startup disk while they look safely offloaded."""
    local = _write(f"{catalog}.pre-v10.bak")
    missing = str(tmp_path / "Volumes" / "Photography" / "Backups")

    result = catalog_backups.offload(catalog, missing)

    assert result.moved == []
    assert "not available" in result.errors[0]
    assert "1 catalog backup(s) stay next to the catalog" in result.errors[0]
    assert os.path.exists(local)
    assert not os.path.exists(missing)


def test_offload_with_nothing_to_move_reports_nothing(catalog, tmp_path):
    result = catalog_backups.offload(catalog, str(tmp_path / "missing"))
    assert (result.moved, result.errors) == ([], [])


def test_offload_keeps_a_local_copy_that_differs_from_the_backup_folder(catalog, backup_dir):
    local = _write(f"{catalog}.pre-v10.bak", "this catalog")
    existing = _write(os.path.join(backup_dir, "vireo.db.pre-v10.bak"), "another catalog")

    result = catalog_backups.offload(catalog, backup_dir)

    assert result.moved == []
    assert "different contents" in result.errors[0]
    assert os.path.exists(local)
    with open(existing) as f:
        assert f.read() == "another catalog"


def test_offload_finishes_a_move_whose_local_delete_failed(catalog, backup_dir):
    local = _write(f"{catalog}.pre-v10.bak", "same bytes")
    _write(os.path.join(backup_dir, "vireo.db.pre-v10.bak"), "same bytes")

    result = catalog_backups.offload(catalog, backup_dir)

    assert result.errors == []
    assert not os.path.exists(local)


def test_offload_removes_the_partial_copy_when_cancelled(catalog, backup_dir):
    local = _write(f"{catalog}.pre-v10.bak")

    def cancel(*_args):
        raise RuntimeError("cancelled")

    with pytest.raises(RuntimeError):
        catalog_backups.offload(catalog, backup_dir, cancel)

    assert os.path.exists(local)
    assert os.listdir(backup_dir) == []


def test_offload_observes_cancellation_during_verification_readback(catalog, backup_dir):
    """A slow readback on an external drive or NAS must still observe
    cancellation; otherwise shutdown waits for the full second I/O pass."""
    local = _write(f"{catalog}.pre-v10.bak", "catalog bytes")
    calls = 0

    def cancel(*_args):
        nonlocal calls
        calls += 1
        if calls > 1:
            raise RuntimeError("cancelled during verification")

    with pytest.raises(RuntimeError, match="verification"):
        catalog_backups.offload(catalog, backup_dir, cancel)

    dest = os.path.join(backup_dir, "vireo.db.pre-v10.bak")
    assert not os.path.exists(dest)
    assert not os.path.exists(dest + ".tmp")
    assert os.path.exists(local)


def test_offload_observes_cancellation_while_hashing_an_existing_destination(catalog, backup_dir):
    """A collision on a slow external drive or NAS must observe cancellation
    while hashing both files, not block for two full-file reads."""
    local = _write(f"{catalog}.pre-v10.bak", "this catalog")
    existing = _write(os.path.join(backup_dir, "vireo.db.pre-v10.bak"), "another catalog")
    calls = 0

    def cancel(*_args):
        nonlocal calls
        calls += 1
        if calls > 1:
            raise RuntimeError("cancelled during hashing")

    with pytest.raises(RuntimeError, match="hashing"):
        catalog_backups.offload(catalog, backup_dir, cancel)

    assert os.path.exists(local)
    with open(existing) as f:
        assert f.read() == "another catalog"


def test_startup_job_moves_backups_and_keeps_the_newest_three(app_and_db, tmp_path, backup_dir):
    import config as cfg
    from canonical_schema import SCHEMA_VERSION

    app, _db = app_and_db
    db_path = app.config["DB_PATH"]
    latest = SCHEMA_VERSION
    _write(f"{db_path}.pre-v{latest}.bak", "newest")
    for version in (latest - 1, latest - 2, latest - 3):
        _write(os.path.join(backup_dir, f"test.db.pre-v{version}.bak"))
    cfg.save({**cfg.load(), "catalog_backup_dir": backup_dir})

    job = wait_for_job_via_runner(
        app._job_runner, app._kickoff_catalog_backup_offload(), wait_for_history=True,
    )

    assert job["status"] == "completed", job
    assert job["origin"] == "system"
    assert not os.path.exists(f"{db_path}.pre-v{latest}.bak")
    assert sorted(b.version for b in catalog_backups.list_backups(backup_dir, "test.db")) == [
        latest - 2, latest - 1, latest,
    ]
    assert "Moved 1 catalog backup(s)" in job["result"]["summary"]
    assert "removed 1 older backup(s)" in job["result"]["summary"]


def test_startup_job_fails_visibly_while_the_backup_folder_is_disconnected(app_and_db, tmp_path):
    import config as cfg

    app, _db = app_and_db
    db_path = app.config["DB_PATH"]
    _write(f"{db_path}.pre-v99.bak")
    cfg.save({**cfg.load(), "catalog_backup_dir": str(tmp_path / "unplugged")})

    job = wait_for_job_via_runner(
        app._job_runner, app._kickoff_catalog_backup_offload(), wait_for_history=True,
    )

    assert job["status"] == "failed", job
    assert "not available" in job["result"]["summary"]
    assert os.path.exists(f"{db_path}.pre-v99.bak")


def test_no_backup_folder_starts_no_job(app_and_db):
    app, _db = app_and_db
    assert app._kickoff_catalog_backup_offload() is None


def test_saving_a_backup_folder_starts_the_move(app_and_db, backup_dir):
    app, _db = app_and_db
    db_path = app.config["DB_PATH"]
    _write(f"{db_path}.pre-v5.bak")

    resp = app.test_client().patch(
        "/api/settings/global", json={"key": "catalog_backup_dir", "value": backup_dir},
    )

    assert resp.status_code == 200
    jobs = [j for j in app._job_runner.list_jobs() if j["type"] == "catalog-backups"]
    assert len(jobs) == 1
    wait_for_job_via_runner(app._job_runner, jobs[0]["id"], wait_for_history=True)
    assert os.path.exists(os.path.join(backup_dir, "test.db.pre-v5.bak"))


def test_saving_a_disconnected_backup_folder_warns(app_and_db, tmp_path):
    app, _db = app_and_db

    resp = app.test_client().patch(
        "/api/settings/global",
        json={"key": "catalog_backup_dir", "value": str(tmp_path / "unplugged")},
    )

    assert resp.status_code == 200
    assert "not available" in resp.get_json()["warning"]
    assert not [j for j in app._job_runner.list_jobs() if j["type"] == "catalog-backups"]
