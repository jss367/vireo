"""Import job: copy card -> archive with hash verification."""
import os
import sys
from datetime import datetime
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from db import Database
from PIL import Image


class FakeRunner:
    """Minimal JobRunner stand-in (mirrors test_pipeline_job.FakeRunner)."""

    def __init__(self):
        self.events = []
        self.step_updates = []
        self.cancelled_ids = set()

    def push_event(self, job_id, event_type, data):
        self.events.append((job_id, event_type, data))

    def set_steps(self, job_id, steps):
        self.steps_defined = list(steps)

    def update_step(self, job_id, step_id, **kwargs):
        self.step_updates.append((job_id, step_id, kwargs))

    def is_cancelled(self, job_id):
        return job_id in self.cancelled_ids

    def cancellation_requested(self, job_id):
        return job_id in self.cancelled_ids


def _make_job(job_id="import-test-1"):
    return {
        "id": job_id,
        "type": "import",
        "status": "running",
        "progress": {"current": 0, "total": 0, "current_file": ""},
        "result": None,
        "errors": [],
        "config": {},
        "workspace_id": 1,
    }


def _make_card(tmp_path, specs, card_name="card"):
    """A fake card with tiny JPEGs. ``specs`` is a list of
    ``(filename, mtime_datetime)`` (or ``(filename, mtime, color)``)
    tuples. Distinct mtimes drive folder planning:
    ingest._source_file_timestamps falls back to file mtime when EXIF is
    absent before build_destination_path formats the destination folder.
    """
    card = tmp_path / card_name
    card.mkdir(exist_ok=True)
    for spec in specs:
        name, mtime, color = spec if len(spec) == 3 else (*spec, "red")
        path = card / name
        Image.new("RGB", (16, 16), color).save(str(path))
        ts = mtime.timestamp()
        os.utime(str(path), (ts, ts))
    return card


def _run_import(tmp_path, params, runner=None, job=None):
    from import_job import run_import_job

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    runner = runner or FakeRunner()
    job = job or _make_job()
    result = run_import_job(job, runner, db_path, ws_id, params)
    return db, ws_id, result


def _photo_rows(db):
    return db.conn.execute(
        """SELECT p.id, p.filename, p.file_hash, p.hash_status,
                  p.hash_checked_at, f.path AS folder_path
           FROM photos p JOIN folders f ON f.id = p.folder_id"""
    ).fetchall()


def _unsafe_paths(result):
    return {u["path"] for u in result["unsafe_files"]}


def _unsafe_reason(result, path):
    return next(u["reason"] for u in result["unsafe_files"]
                if u["path"] == path)


def test_import_eta_waits_for_a_completed_transfer_batch():
    """Fast duplicate-only work must not seed the transfer ETA."""
    from import_job import _ImportEtaEstimator

    now = [0.0]
    eta = _ImportEtaEstimator(clock=lambda: now[0])

    # A quick duplicate-only batch settles 200 files, but says nothing
    # about how long the remaining card-to-archive transfers will take.
    eta.note_importing(copied=0)
    now[0] = 2.0
    eta.note_batch_complete(current=200, copied=0)
    assert eta.fields(1000) == {
        "eta_state": "estimating",
        "eta_settled": 200,
    }

    # The first real transfer+catalog batch supplies the first honest rate.
    eta.note_importing(copied=0)
    now[0] = 22.0
    eta.note_batch_complete(current=400, copied=200)
    assert eta.fields(1000) == {
        "eta_state": "ready",
        "eta_settled": 400,
        "eta_seconds": 60.0,
        "eta_rate_per_min": 600.0,
    }


def test_import_eta_does_not_count_an_active_prepared_batch_as_settled():
    """Queuing the next batch must not make its ETA disappear early."""
    from import_job import _ImportEtaEstimator

    now = [0.0]
    eta = _ImportEtaEstimator(clock=lambda: now[0])
    eta.note_importing(copied=0)
    now[0] = 20.0
    eta.note_batch_complete(current=200, copied=200)

    # The UI counter may already say 400 while rsync is still transferring
    # that second batch. The estimator intentionally remains at 200 settled.
    eta.note_importing(copied=200)
    assert eta.fields(1000)["eta_settled"] == 200
    assert eta.fields(1000)["eta_seconds"] == 80.0


def test_import_eta_uses_preview_new_count_for_a_mixed_boundary_batch():
    """A mostly-duplicate batch must not look like 200 fast transfers."""
    from import_job import _ImportEtaEstimator

    now = [0.0]
    eta = _ImportEtaEstimator(clock=lambda: now[0], expected_new=100)

    # Learn the duplicate-check cost separately.
    eta.note_importing(copied=0)
    now[0] = 2.0
    eta.note_batch_complete(current=200, copied=0)

    # The next 200-file batch crosses the old/new boundary: 180 duplicates,
    # only 20 actual copies. Its 22 seconds therefore imply about 1.01s per
    # new file after subtracting the learned duplicate-check time.
    eta.note_importing(copied=0)
    now[0] = 24.0
    eta.note_batch_complete(current=400, copied=20)
    fields = eta.fields(1000)

    # 80 new * 1.01s + 520 duplicates * 0.01s = 86 seconds.
    assert fields["eta_state"] == "ready"
    assert fields["eta_settled"] == 400
    assert fields["eta_seconds"] == 86.0


def test_import_eta_no_preview_measures_only_copied_files_in_mixed_batch():
    """A no-preview mixed batch must not dilute one transfer over 200 files."""
    from import_job import _ImportEtaEstimator

    now = [0.0]
    eta = _ImportEtaEstimator(clock=lambda: now[0])

    # Learn a 0.01-second duplicate check from a pure duplicate batch.
    eta.note_importing(copied=0)
    now[0] = 2.0
    eta.note_batch_complete(current=200, copied=0)

    # The next batch takes 12 seconds but contains only one real copy. After
    # subtracting 1.99 seconds for its 199 duplicates, the expensive rate is
    # 10.01 seconds per copied file, not 0.06 seconds per settled source.
    eta.note_importing(copied=0)
    now[0] = 14.0
    eta.note_batch_complete(current=400, copied=1)
    fields = eta.fields(600)

    assert fields["eta_state"] == "ready"
    assert fields["eta_seconds"] == 2002.0


def _ws_linked_folder_paths(db, ws_id):
    return {
        row["path"]
        for row in db.conn.execute(
            """SELECT f.path FROM folders f
               JOIN workspace_folders wf ON wf.folder_id = f.id
               WHERE wf.workspace_id = ?""",
            (ws_id,),
        )
    }


def test_run_import_job_copies_verifies_and_catalogs(tmp_path):
    from import_dedup import compute_file_hash
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
        ("DSC_0003.jpg", datetime(2026, 7, 4, 9, 0, 0), "blue"),
        ("DSC_0004.jpg", datetime(2026, 7, 4, 9, 5, 0), "white"),
    ])
    archive = tmp_path / "archive"

    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(archive),
    ))

    # Every discovered file landed at its template path.
    expected = {
        str(archive / "2026" / "2026-07-03" / "DSC_0001.jpg"),
        str(archive / "2026" / "2026-07-03" / "DSC_0002.jpg"),
        str(archive / "2026" / "2026-07-04" / "DSC_0003.jpg"),
        str(archive / "2026" / "2026-07-04" / "DSC_0004.jpg"),
    }
    for path in expected:
        assert os.path.isfile(path), f"missing archive file: {path}"

    # A photo row exists for each copied file at its final path, with the
    # verified hash stamped in the integrity-audit vocabulary.
    rows = _photo_rows(db)
    row_paths = {
        os.path.join(r["folder_path"], r["filename"]) for r in rows
    }
    assert row_paths == expected
    for r in rows:
        full = os.path.join(r["folder_path"], r["filename"])
        assert r["file_hash"] == compute_file_hash(full)
        assert r["hash_status"] == "ok"
        assert r["hash_checked_at"] is not None

    # Result counts are consistent.
    assert result["discovered"] == 4
    assert result["copied"] == 4
    assert result["verified"] == 4
    assert result["skipped_duplicate"] == 0
    assert result["failed"] == 0

    # The date folders are linked to the active workspace.
    linked = _ws_linked_folder_paths(db, ws_id)
    assert str(archive / "2026" / "2026-07-03") in linked
    assert str(archive / "2026" / "2026-07-04") in linked


def test_duplicate_only_import_links_matched_folders(tmp_path):
    """Importing a card of only already-cataloged duplicates must still
    link the matched destination folders to the active workspace without
    scanning them or copying fresh files."""
    from import_dedup import compute_file_hash
    from import_job import ImportParams

    # Pre-catalog a photo at the archive destination WITHOUT linking its
    # folder to the active workspace (raw SQL, no workspace_folders rows).
    archive = tmp_path / "archive"
    dest_dir = archive / "2026" / "2026-07-03"
    dest_dir.mkdir(parents=True)
    dest_file = dest_dir / "IMG_0100.jpg"
    Image.new("RGB", (16, 16), "red").save(str(dest_file))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (str(dest_dir), dest_dir.name),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_hash) VALUES (?, ?, '.jpg', ?, ?)",
        (
            fid,
            "IMG_0100.jpg",
            os.path.getsize(str(dest_file)),
            compute_file_hash(str(dest_file)),
        ),
    )
    db.conn.commit()
    assert str(dest_dir) not in _ws_linked_folder_paths(db, ws_id)

    # Card holds a byte-identical copy of the cataloged photo.
    card = tmp_path / "card"
    card.mkdir()
    import shutil
    shutil.copy2(str(dest_file), str(card / "IMG_0100.jpg"))

    runner = FakeRunner()
    job = _make_job()
    from import_job import run_import_job
    result = run_import_job(job, runner, db_path, ws_id, ImportParams(
        sources=[str(card)], destination=str(archive),
    ))

    assert result["copied"] == 0
    assert result["skipped_duplicate"] == 1
    assert result["failed"] == 0
    # The matched folder was linked despite zero fresh copies.
    assert str(dest_dir) in _ws_linked_folder_paths(db, ws_id)
    # Still exactly one photo row — no re-import of known bytes.
    assert len(_photo_rows(db)) == 1


def _mark_exif_extracted(db):
    """Stand in for a successful ExifTool pass over the cataloged rows.

    ExifTool isn't installed in CI, so rows scanned there keep
    ``exif_data`` NULL — which scanner reads as "metadata was never
    extracted" and re-processes on the next incremental scan whatever
    the mtime says. That retry path is deliberate and tested in
    test_scanner.py; the tests below are about avoiding archive reads, so
    give the rows the marker a real extraction would have written.
    """
    db.conn.execute(
        "UPDATE photos SET exif_data = '{}' WHERE exif_data IS NULL",
    )
    db.conn.commit()


def _count_feature_computations(monkeypatch):
    """Record every file scanner re-reads to derive phash/file_hash.

    ``_compute_file_features`` opens the image and hashes every byte, so
    it is the direct proxy for "did the scan actually read this file off
    the archive volume".
    """
    import scanner

    read_paths = []
    real = scanner._compute_file_features

    def _counting(path_str):
        read_paths.append(path_str)
        return real(path_str)

    monkeypatch.setattr(scanner, "_compute_file_features", _counting)
    return read_paths


def test_duplicate_only_import_does_not_reread_unchanged_twins(
    tmp_path, monkeypatch,
):
    """Direct duplicate-folder linking must not read cataloged twin bytes."""
    import shutil

    import scanner
    from import_job import ImportParams, run_import_job

    archive = tmp_path / "archive"
    twin_dir = archive / "2026" / "2026-07-03"
    twin_dir.mkdir(parents=True)
    names = ["IMG_0400.jpg", "IMG_0401.jpg", "IMG_0402.jpg"]
    for i, name in enumerate(names):
        Image.new("RGB", (16, 16), ("red", "green", "blue")[i]).save(
            str(twin_dir / name),
        )

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    # Catalog the archive exactly as a normal scan would, so the rows
    # carry the file_mtime/metadata an incremental pass needs to skip on.
    scanner.scan(str(archive), db)
    _mark_exif_extracted(db)
    assert len(_photo_rows(db)) == 3

    # The card holds byte-identical copies of all three cataloged files.
    card = tmp_path / "card"
    card.mkdir()
    for name in names:
        shutil.copy2(str(twin_dir / name), str(card / name))

    read_paths = _count_feature_computations(monkeypatch)
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(sources=[str(card)], destination=str(archive)),
    )

    assert result["copied"] == 0
    assert result["skipped_duplicate"] == 3
    assert result["failed"] == 0
    # The folder is still linked without archive reads.
    assert str(twin_dir) in _ws_linked_folder_paths(db, ws_id)
    assert read_paths == [], (
        "duplicate-only import re-read already-cataloged, unchanged twins: "
        f"{read_paths}"
    )


def test_duplicate_only_import_does_not_scan_matched_folder(
        tmp_path, monkeypatch):
    """A duplicate-only import links the cataloged folder without asking
    scanner to enumerate it. This is the SMB/NAS regression: even an
    incremental scan performs metadata calls for every directory entry."""
    import shutil

    import scanner
    from import_job import ImportParams, run_import_job

    archive = tmp_path / "archive"
    twin_dir = archive / "2026" / "2026-07-03"
    twin_dir.mkdir(parents=True)
    twin_file = twin_dir / "IMG_0600.jpg"
    Image.new("RGB", (16, 16), "red").save(str(twin_file))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    scanner.scan(str(archive), db)

    card = tmp_path / "card"
    card.mkdir()
    shutil.copy2(str(twin_file), str(card / "IMG_0600.jpg"))

    def unexpected_scan(*args, **kwargs):
        raise AssertionError("duplicate-only import must not scan the archive")

    monkeypatch.setattr(scanner, "scan", unexpected_scan)

    runner = FakeRunner()
    result = run_import_job(
        _make_job(), runner, db_path, ws_id,
        ImportParams(sources=[str(card)], destination=str(archive)),
    )
    assert result["skipped_duplicate"] == 1
    assert result["failed"] == 0
    assert str(twin_dir) in _ws_linked_folder_paths(db, ws_id)


def test_duplicate_only_import_leaves_stray_for_explicit_rescan(
    tmp_path, monkeypatch,
):
    """Import must not hide folder health or repair unrelated archive files.

    A partial matched folder stays partial and its uncataloged stray remains
    untouched until an explicit rescan repairs both conditions.
    """
    import shutil

    import scanner
    from import_job import ImportParams, run_import_job

    archive = tmp_path / "archive"
    twin_dir = archive / "2026" / "2026-07-03"
    twin_dir.mkdir(parents=True)
    twin_file = twin_dir / "IMG_0500.jpg"
    Image.new("RGB", (16, 16), "red").save(str(twin_file))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    scanner.scan(str(archive), db)
    _mark_exif_extracted(db)
    db.conn.execute(
        "UPDATE folders SET status = 'partial' WHERE path = ?",
        (str(twin_dir),),
    )
    db.conn.commit()

    # A stray lands in the twin folder AFTER the catalog was built.
    stray = twin_dir / "IMG_0501.jpg"
    Image.new("RGB", (16, 16), "green").save(str(stray))

    card = tmp_path / "card"
    card.mkdir()
    shutil.copy2(str(twin_file), str(card / "IMG_0500.jpg"))

    read_paths = _count_feature_computations(monkeypatch)
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(sources=[str(card)], destination=str(archive)),
    )

    assert result["skipped_duplicate"] == 1
    row_paths = {
        os.path.join(r["folder_path"], r["filename"]) for r in _photo_rows(db)
    }
    assert str(stray) not in row_paths
    assert read_paths == []
    status = db.conn.execute(
        "SELECT status FROM folders WHERE path = ?", (str(twin_dir),),
    ).fetchone()["status"]
    assert status == "partial"

    # The existing explicit rescan workflow still performs the repair.
    scanner.scan(str(archive), db, incremental=True)
    row_paths = {
        os.path.join(r["folder_path"], r["filename"]) for r in _photo_rows(db)
    }
    assert str(stray) in row_paths
    assert read_paths == [str(stray)]
    status = db.conn.execute(
        "SELECT status FROM folders WHERE path = ?", (str(twin_dir),),
    ).fetchone()["status"]
    assert status == "ok"


def test_duplicate_only_import_skips_twins_cataloged_in_other_workspace(
    tmp_path, monkeypatch,
):
    """A folder cataloged in another workspace is directly linked into the
    active one without reading its already-known photos."""
    import shutil

    import scanner
    from import_job import ImportParams, run_import_job

    archive = tmp_path / "archive"
    twin_dir = archive / "2026" / "2026-07-03"
    twin_dir.mkdir(parents=True)
    names = ["IMG_0700.jpg", "IMG_0701.jpg", "IMG_0702.jpg"]
    for i, name in enumerate(names):
        Image.new("RGB", (16, 16), ("red", "green", "blue")[i]).save(
            str(twin_dir / name),
        )

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    seed_ws = db._active_workspace_id
    # Seed the catalog by scanning under the default workspace so rows
    # carry the file_mtime/metadata an incremental pass needs to skip on.
    scanner.scan(str(archive), db)
    _mark_exif_extracted(db)
    assert len(_photo_rows(db)) == 3
    assert str(twin_dir) in _ws_linked_folder_paths(db, seed_ws)

    # A fresh workspace has never seen this folder.
    fresh_ws = db.create_workspace("Fresh")
    assert str(twin_dir) not in _ws_linked_folder_paths(db, fresh_ws)

    card = tmp_path / "card"
    card.mkdir()
    for name in names:
        shutil.copy2(str(twin_dir / name), str(card / name))

    read_paths = _count_feature_computations(monkeypatch)
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, fresh_ws,
        ImportParams(sources=[str(card)], destination=str(archive)),
    )

    assert result["copied"] == 0
    assert result["skipped_duplicate"] == 3, result
    assert result["failed"] == 0, result
    # The folder is now linked to the fresh workspace without archive reads.
    assert str(twin_dir) in _ws_linked_folder_paths(db, fresh_ws)
    assert read_paths == [], (
        "duplicate-only import re-read twins whose folder was cataloged "
        "but not yet linked to the active workspace: "
        f"{read_paths}"
    )


def test_duplicate_only_import_links_alias_spelled_twin(tmp_path):
    """When a twin folder is cataloged through a symlink alias but the
    import ``destination`` resolves to a different (real) spelling, the
    duplicate-link pass must still workspace-link the twin. Passing an
    alias-spelled ``restrict_dir`` to ``scan(root=destination)`` would
    infinite-recurse in ``_ensure_folder`` (it walks parents lexically
    without ever matching the realpath'd root); the import job routes
    the alias case straight to ``workspace_folders`` instead.
    """
    import sys as _sys

    if _sys.platform == "win32":
        # os.symlink usually requires elevation on Windows; skip.
        import pytest
        pytest.skip("symlinks not routinely available on Windows")

    from import_dedup import compute_file_hash
    from import_job import ImportParams, run_import_job

    # Real archive dir + symlink alias to it.
    real_archive = tmp_path / "real" / "archive"
    real_archive.mkdir(parents=True)
    dest_dir_real = real_archive / "2026" / "2026-07-03"
    dest_dir_real.mkdir(parents=True)
    dest_file = dest_dir_real / "IMG_9000.jpg"
    Image.new("RGB", (16, 16), "red").save(str(dest_file))

    alias_archive = tmp_path / "alias"
    os.symlink(str(real_archive), str(alias_archive))
    alias_dest_dir = alias_archive / "2026" / "2026-07-03"
    # Sanity: the alias resolves to the same folder.
    assert os.path.realpath(str(alias_dest_dir)) == str(dest_dir_real)

    # Catalog the twin under the ALIAS spelling — simulating a prior
    # scan that used ``/alias/…`` as its root — and don't link its
    # folder to the workspace.
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (str(alias_dest_dir), alias_dest_dir.name),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_hash) VALUES (?, ?, '.jpg', ?, ?)",
        (
            fid,
            "IMG_9000.jpg",
            os.path.getsize(str(dest_file)),
            compute_file_hash(str(dest_file)),
        ),
    )
    db.conn.commit()
    assert str(alias_dest_dir) not in _ws_linked_folder_paths(db, ws_id)

    # Card has a byte-identical copy. Import to the REAL destination.
    card = tmp_path / "card"
    card.mkdir()
    import shutil
    shutil.copy2(str(dest_file), str(card / "IMG_9000.jpg"))

    runner = FakeRunner()
    job = _make_job()
    result = run_import_job(job, runner, db_path, ws_id, ImportParams(
        sources=[str(card)], destination=str(real_archive),
    ))

    assert result["copied"] == 0
    assert result["skipped_duplicate"] == 1
    assert result["failed"] == 0
    # The alias-spelled twin folder is now workspace-linked (via the
    # direct-link path, bypassing scan which would have infinite-
    # recursed in _ensure_folder).
    assert str(alias_dest_dir) in _ws_linked_folder_paths(db, ws_id)
    # Still exactly one photo row — no double-catalog of the twin.
    assert len(_photo_rows(db)) == 1
    # And the run is safe to format the card.
    assert result["safe_to_format"] is True


def test_catalog_never_references_missing_files(tmp_path, monkeypatch):
    """Invariant: catalog is a subset of verified on-disk files, even when
    some copies fail."""
    import shutil as shutil_mod

    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0010.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0011.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])
    archive = tmp_path / "archive"

    # Sabotage the second copy: corrupt destination bytes for DSC_0011.
    real_copy2 = shutil_mod.copy2

    def flaky_copy2(s, d):
        real_copy2(s, d)
        if "DSC_0011" in str(d):
            with open(d, "r+b") as f:
                f.write(b"CORRUPT")

    monkeypatch.setattr("import_job.shutil.copy2", flaky_copy2)
    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(archive),
    ))

    assert result["copied"] == 1
    assert result["failed"] == 1
    rows = _photo_rows(db)
    # Only the verified file is cataloged, and it exists on disk.
    assert len(rows) == 1
    for r in rows:
        assert os.path.isfile(os.path.join(r["folder_path"], r["filename"]))
    # A failed copy means the card is NOT safe to format, with the
    # failure named.
    assert result["safe_to_format"] is False
    assert len(result["unsafe_files"]) == 1
    assert "DSC_0011" in result["unsafe_files"][0]["path"]
    assert result["unsafe_files"][0]["reason"]


# --- safe-to-format ledger ----------------------------------------------

def test_fresh_import_is_safe_to_format(tmp_path):
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0020.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0021.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])
    _, _, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(tmp_path / "archive"),
    ))
    assert result["safe_to_format"] is True
    assert result["unsafe_files"] == []


def test_hash_backed_duplicate_is_safe_to_format(tmp_path):
    """A byte-identical twin already cataloged means the card file's bytes
    verifiably exist — safe."""
    from import_dedup import compute_file_hash
    from import_job import ImportParams

    archive = tmp_path / "archive"
    dest_dir = archive / "2026" / "2026-07-03"
    dest_dir.mkdir(parents=True)
    dest_file = dest_dir / "IMG_0300.jpg"
    Image.new("RGB", (16, 16), "red").save(str(dest_file))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (str(dest_dir), dest_dir.name),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_hash) VALUES (?, ?, '.jpg', ?, ?)",
        (fid, "IMG_0300.jpg", os.path.getsize(str(dest_file)),
         compute_file_hash(str(dest_file))),
    )
    db.conn.commit()

    card = tmp_path / "card"
    card.mkdir()
    import shutil
    shutil.copy2(str(dest_file), str(card / "IMG_0300.jpg"))

    from import_job import run_import_job
    result = run_import_job(_make_job(), FakeRunner(), db_path, ws_id,
                            ImportParams(sources=[str(card)],
                                         destination=str(archive)))
    assert result["skipped_duplicate"] == 1
    assert result["safe_to_format"] is True


def test_stale_hash_row_without_on_disk_twin_imports_as_fresh(tmp_path):
    """A cataloged ``photos.file_hash`` row whose archive file has been
    deleted since scan must NOT let the card be counted as skipped. The
    hash token matches (card bytes hash to the cataloged value) but no
    on-disk twin backs it — safe_to_format would go green while the card
    is the only remaining copy of the bytes. The card must import as a
    fresh photo instead."""
    from import_dedup import compute_file_hash
    from import_job import ImportParams, run_import_job

    # Card file whose bytes hash to a specific value.
    card = tmp_path / "card"
    card.mkdir()
    card_file = card / "IMG_0500.jpg"
    Image.new("RGB", (16, 16), "red").save(str(card_file))
    ts = datetime(2026, 6, 1, 9, 0, 0).timestamp()
    os.utime(str(card_file), (ts, ts))
    card_hash = compute_file_hash(str(card_file))
    card_size = os.path.getsize(str(card_file))

    # Seeded catalog row: a folder that once held a byte-identical twin,
    # but the archive file is GONE. The folder path exists on disk
    # (folder_status still 'ok') to isolate the missing-file case.
    archive = tmp_path / "archive"
    library = archive / "old-library"
    library.mkdir(parents=True)
    ghost_path = library / "IMG_0500.jpg"
    assert not ghost_path.exists()

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (str(library), "old-library"),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_hash) VALUES (?, ?, '.jpg', ?, ?)",
        (fid, "IMG_0500.jpg", card_size, card_hash),
    )
    db.conn.commit()

    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(sources=[str(card)], destination=str(archive),
                     verify_by_hash=True),
    )

    # Card must NOT be counted as a skipped duplicate — no twin backs it.
    assert result["skipped_duplicate"] == 0
    assert result["copied"] == 1
    # Bytes now live at the fresh archive path.
    dest = archive / "2026" / "2026-06-01" / "IMG_0500.jpg"
    assert dest.exists()
    assert compute_file_hash(str(dest)) == card_hash
    # safe_to_format is true because the card's bytes verifiably exist
    # at the fresh archive path — but via copy, not via a stale row.
    assert result["safe_to_format"] is True


def test_stale_hash_row_with_modified_bytes_imports_as_fresh(tmp_path):
    """The archive file at the cataloged path exists but was modified
    since scan (bytes no longer match ``photos.file_hash``). The card's
    hash still matches the stale row; the twin's re-hash does not. The
    card must import as fresh — the stale hash row is not proof that the
    bytes verifiably exist on disk."""
    from import_dedup import compute_file_hash
    from import_job import ImportParams, run_import_job

    card = tmp_path / "card"
    card.mkdir()
    card_file = card / "IMG_0600.jpg"
    Image.new("RGB", (16, 16), "green").save(str(card_file))
    ts = datetime(2026, 6, 2, 9, 0, 0).timestamp()
    os.utime(str(card_file), (ts, ts))
    card_hash = compute_file_hash(str(card_file))

    # Seed a "twin" archive file whose CURRENT bytes differ from the
    # cataloged file_hash (a stale row: the file was modified after the
    # last scan).
    archive = tmp_path / "archive"
    library = archive / "old-library"
    library.mkdir(parents=True)
    twin_path = library / "IMG_0600.jpg"
    Image.new("RGB", (16, 16), "blue").save(str(twin_path))
    twin_current_hash = compute_file_hash(str(twin_path))
    assert twin_current_hash != card_hash

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (str(library), "old-library"),
    ).lastrowid
    # Stale row: file_hash claims card_hash but on-disk bytes hash to
    # twin_current_hash.
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_hash) VALUES (?, ?, '.jpg', ?, ?)",
        (fid, "IMG_0600.jpg", os.path.getsize(str(card_file)), card_hash),
    )
    db.conn.commit()

    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(sources=[str(card)], destination=str(archive),
                     verify_by_hash=True),
    )

    assert result["skipped_duplicate"] == 0
    assert result["copied"] == 1
    dest = archive / "2026" / "2026-06-02" / "IMG_0600.jpg"
    assert compute_file_hash(str(dest)) == card_hash
    assert result["safe_to_format"] is True


def test_catalog_twin_under_source_root_does_not_prove_duplicate(tmp_path):
    """A cataloged twin whose folder_path IS (or is under) an import
    source is the card file being imported — the user previously scanned
    the mounted card, so ``photos.file_hash`` matches the card's own
    bytes. Re-hashing that "twin" just re-reads the source and proves
    nothing about any archive copy; accepting it as duplicate proof would
    flip ``safe_to_format`` green over a card whose bytes never made it
    to the archive. The card must import fresh instead."""
    from import_dedup import compute_file_hash
    from import_job import ImportParams, run_import_job

    # Card file whose bytes hash to a known value.
    card = tmp_path / "card"
    card.mkdir()
    card_file = card / "IMG_0800.jpg"
    Image.new("RGB", (16, 16), "purple").save(str(card_file))
    ts = datetime(2026, 6, 3, 14, 0, 0).timestamp()
    os.utime(str(card_file), (ts, ts))
    card_hash = compute_file_hash(str(card_file))
    card_size = os.path.getsize(str(card_file))

    # Seed a stale catalog row whose folder_path IS the mounted card
    # (a prior scan of the card left this behind). file_hash matches the
    # card because it WAS computed by hashing the card file.
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (str(card), card.name),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_hash) VALUES (?, ?, '.jpg', ?, ?)",
        (fid, "IMG_0800.jpg", card_size, card_hash),
    )
    db.conn.commit()

    archive = tmp_path / "archive"
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(sources=[str(card)], destination=str(archive),
                     verify_by_hash=True),
    )

    # Not skipped: the only "twin" is the card itself, and the card
    # can't prove its own bytes safe.
    assert result["skipped_duplicate"] == 0
    assert result["copied"] == 1
    dest = archive / "2026" / "2026-06-03" / "IMG_0800.jpg"
    assert dest.exists()
    assert compute_file_hash(str(dest)) == card_hash
    # safe_to_format is true because the card's bytes verifiably landed
    # at the archive — via a real copy, not via a stale card-side row.
    assert result["safe_to_format"] is True


def test_key_match_with_different_bytes_imports_as_distinct(tmp_path):
    """A metadata-only ("key") match against a cataloged twin whose bytes
    differ must NOT be skipped: the card's bytes were never verified
    anywhere, so skipping would let the safe-to-format pill go green while
    the card holds the only copy. The file imports as a distinct photo."""
    from import_job import ImportParams
    from PIL.ExifTags import Base as ExifBase

    dt = datetime(2026, 5, 1, 10, 15, 30)

    # Card file with a trustworthy EXIF capture time.
    card = tmp_path / "card"
    card.mkdir()
    card_file = card / "IMG_0400.jpg"
    img = Image.new("RGB", (16, 16), "red")
    exif = img.getexif()
    exif[ExifBase.DateTimeOriginal] = dt.strftime("%Y:%m:%d %H:%M:%S")
    img.save(str(card_file), exif=exif)
    card_bytes = card_file.read_bytes()

    # Cataloged twin: same name, same size, same trusted capture time —
    # different bytes (last byte flipped; only ever hashed, never decoded).
    library = tmp_path / "library"
    library.mkdir()
    twin_file = library / "IMG_0400.jpg"
    twin_bytes = card_bytes[:-1] + bytes([card_bytes[-1] ^ 0xFF])
    twin_file.write_bytes(twin_bytes)
    assert len(twin_bytes) == len(card_bytes)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (str(library), "library"),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " timestamp) VALUES (?, ?, '.jpg', ?, ?)",
        (fid, "IMG_0400.jpg", len(twin_bytes), "2026-05-01T10:15:30"),
    )
    db.conn.commit()

    archive = tmp_path / "archive"
    from import_job import run_import_job
    result = run_import_job(_make_job(), FakeRunner(), db_path, ws_id,
                            ImportParams(sources=[str(card)],
                                         destination=str(archive)))

    # Imported as a fresh distinct photo, not skipped.
    assert result["copied"] == 1
    assert result["skipped_duplicate"] == 0
    dest = archive / "2026" / "2026-05-01" / "IMG_0400.jpg"
    assert dest.read_bytes() == card_bytes
    # Two catalog rows now: the seeded twin and the new import.
    assert len(_photo_rows(db)) == 2
    # The copy verified, so the card is safe.
    assert result["safe_to_format"] is True


def test_trust_likely_duplicates_skips_metadata_match_without_byte_check(
    tmp_path,
):
    """Fast mode deliberately trusts filename + size + capture second.

    A same-metadata, different-bytes twin is skipped, reported separately
    as unverified, and must keep the safe-to-format result false.
    """
    from import_job import ImportParams, run_import_job
    from PIL.ExifTags import Base as ExifBase

    dt = datetime(2026, 5, 1, 10, 15, 30)
    card = tmp_path / "card"
    card.mkdir()
    card_file = card / "IMG_0400.jpg"
    img = Image.new("RGB", (16, 16), "red")
    exif = img.getexif()
    exif[ExifBase.DateTimeOriginal] = dt.strftime("%Y:%m:%d %H:%M:%S")
    img.save(str(card_file), exif=exif)
    card_bytes = card_file.read_bytes()

    library = tmp_path / "library"
    library.mkdir()
    twin_file = library / "IMG_0400.jpg"
    twin_file.write_bytes(
        card_bytes[:-1] + bytes([card_bytes[-1] ^ 0xFF])
    )
    assert twin_file.stat().st_size == card_file.stat().st_size

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (str(library), "library"),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " timestamp) VALUES (?, ?, '.jpg', ?, ?)",
        (fid, "IMG_0400.jpg", len(card_bytes), "2026-05-01T10:15:30"),
    )
    db.conn.commit()

    archive = tmp_path / "archive"
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)],
            destination=str(archive),
            trust_likely_duplicates=True,
        ),
    )

    assert result["copied"] == 0
    assert result["skipped_duplicate"] == 1
    assert result["unverified_duplicate"] == 1
    assert result["unverified_duplicates_only"] is True
    assert result["safe_to_format"] is False
    assert not list(archive.rglob("IMG_0400.jpg"))


def test_intra_run_key_collision_across_cards_imports_second_as_fresh(tmp_path):
    """Two cards can hold different bytes at the same filename+size+
    capture-second (say, an IMG_XXXX rollover after a firmware reset).
    A metadata-key match against the first card's just-copied file must
    NOT let the second card's file be counted as skipped without a byte
    check: the two files' bytes were never compared, so skipping would
    let safe_to_format go green while the second card is the only copy
    of its bytes."""
    from import_dedup import compute_file_hash
    from import_job import ImportParams, run_import_job
    from PIL.ExifTags import Base as ExifBase

    dt = datetime(2026, 5, 2, 11, 20, 45)

    card1 = tmp_path / "card1"
    card1.mkdir()
    card1_file = card1 / "IMG_0700.jpg"
    img = Image.new("RGB", (16, 16), "red")
    exif = img.getexif()
    exif[ExifBase.DateTimeOriginal] = dt.strftime("%Y:%m:%d %H:%M:%S")
    img.save(str(card1_file), exif=exif)
    card1_bytes = card1_file.read_bytes()

    # Card 2: SAME filename, SAME size, SAME trusted capture time,
    # different bytes (last byte flipped; EXIF header untouched).
    card2 = tmp_path / "card2"
    card2.mkdir()
    card2_file = card2 / "IMG_0700.jpg"
    card2_bytes = card1_bytes[:-1] + bytes([card1_bytes[-1] ^ 0xFF])
    assert len(card2_bytes) == len(card1_bytes)
    assert card2_bytes != card1_bytes
    card2_file.write_bytes(card2_bytes)

    archive = tmp_path / "archive"
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id, ImportParams(
            sources=[str(card1), str(card2)], destination=str(archive),
        ),
    )

    # Second card must NOT be counted as skipped_duplicate.
    assert result["copied"] == 2
    assert result["skipped_duplicate"] == 0
    assert result["failed"] == 0
    # safe_to_format is true because both cards' bytes verifiably landed.
    assert result["safe_to_format"] is True

    dest_dir = archive / "2026" / "2026-05-02"
    landed = sorted(p for p in dest_dir.iterdir() if p.is_file())
    assert len(landed) == 2
    on_disk = {p.read_bytes() for p in landed}
    assert card1_bytes in on_disk
    assert card2_bytes in on_disk
    hashes_on_disk = {compute_file_hash(str(p)) for p in landed}
    assert compute_file_hash(str(card1_file)) in hashes_on_disk
    assert compute_file_hash(str(card2_file)) in hashes_on_disk


def test_key_candidate_source_read_error_fails_only_that_file(
        tmp_path, monkeypatch):
    """When the current-source hash read for a metadata-key duplicate
    candidate raises OSError (removable media pulled mid-check, I/O
    error), that source alone is bucketed as failed. The failure must
    not escape and kill the whole background job — siblings still import
    normally, and the safe-to-format ledger records the failure."""
    import import_dedup
    from import_job import ImportParams, run_import_job
    from PIL.ExifTags import Base as ExifBase

    dt_bad = datetime(2026, 5, 3, 12, 0, 0)
    dt_good = datetime(2026, 5, 3, 12, 5, 0)

    card = tmp_path / "card"
    card.mkdir()
    bad_file = card / "IMG_0800.jpg"
    img = Image.new("RGB", (16, 16), "red")
    exif_bad = img.getexif()
    exif_bad[ExifBase.DateTimeOriginal] = (
        dt_bad.strftime("%Y:%m:%d %H:%M:%S")
    )
    img.save(str(bad_file), exif=exif_bad)

    good_file = card / "IMG_0801.jpg"
    img2 = Image.new("RGB", (16, 16), "green")
    exif_good = img2.getexif()
    exif_good[ExifBase.DateTimeOriginal] = (
        dt_good.strftime("%Y:%m:%d %H:%M:%S")
    )
    img2.save(str(good_file), exif=exif_good)

    # Seed a cataloged twin whose (filename, size, capture-second) matches
    # the bad file — checker.match() will return ('key', …) without needing
    # to hash, so the next read (the current-source hash for byte
    # verification) is the one we make fail.
    library = tmp_path / "library"
    library.mkdir()
    twin_file = library / "IMG_0800.jpg"
    twin_bytes = bad_file.read_bytes()[:-1] + b"\x00"
    twin_file.write_bytes(twin_bytes)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (str(library), "library"),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " timestamp) VALUES (?, ?, '.jpg', ?, ?)",
        (fid, "IMG_0800.jpg", os.path.getsize(str(bad_file)),
         dt_bad.strftime("%Y-%m-%dT%H:%M:%S")),
    )
    db.conn.commit()

    real_hash = import_dedup.compute_file_hash
    bad_path_str = str(bad_file)

    def flaky_hash(path):
        if str(path) == bad_path_str:
            raise OSError("card yanked mid-check")
        return real_hash(path)

    monkeypatch.setattr(import_dedup, "compute_file_hash", flaky_hash)

    archive = tmp_path / "archive"
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(sources=[str(card)], destination=str(archive)),
    )

    assert result["failed"] == 1
    assert result["copied"] == 1
    assert result["skipped_duplicate"] == 0
    assert result["safe_to_format"] is False
    assert len(result["unsafe_files"]) == 1
    assert result["unsafe_files"][0]["path"] == bad_path_str
    assert "duplicate check failed" in result["unsafe_files"][0]["reason"]

    dest_good = archive / "2026" / "2026-05-03" / "IMG_0801.jpg"
    assert dest_good.exists()
    assert dest_good.read_bytes() == good_file.read_bytes()


def test_copy_and_hash_verify_roundtrip(tmp_path):
    from import_dedup import compute_file_hash
    from import_job import copy_and_hash_verify

    src = tmp_path / "card" / "DSC_0001.jpg"
    src.parent.mkdir()
    src.write_bytes(b"pixels" * 1000)
    dst = tmp_path / "archive" / "2026" / "DSC_0001.jpg"

    ok, file_hash = copy_and_hash_verify(str(src), str(dst))
    assert ok is True
    assert file_hash == compute_file_hash(str(src))
    assert dst.read_bytes() == src.read_bytes()


def test_copy_and_hash_verify_detects_corruption(tmp_path, monkeypatch):
    """A copy whose destination bytes differ must fail without deleting any
    previously verified archive file at the destination path."""
    import shutil

    from import_job import copy_and_hash_verify

    src = tmp_path / "card" / "DSC_0002.jpg"
    src.parent.mkdir()
    src.write_bytes(b"good bytes")
    dst = tmp_path / "archive" / "DSC_0002.jpg"
    dst.parent.mkdir()
    dst.write_bytes(b"existing verified archive bytes")

    real_copy2 = shutil.copy2

    def corrupting_copy2(s, d):
        real_copy2(s, d)
        with open(d, "r+b") as f:
            f.write(b"BAD")

    monkeypatch.setattr("import_job.shutil.copy2", corrupting_copy2)
    ok, file_hash = copy_and_hash_verify(str(src), str(dst))
    assert ok is False
    assert file_hash is None
    assert dst.read_bytes() == b"existing verified archive bytes"
    assert not list(dst.parent.glob(".DSC_0002.jpg.*.tmp"))


# --- working copies from the card (Task 2.5) -----------------------------

def _stub_extractor(monkeypatch, outcome):
    """Replace scanner.extract_working_copy, recording calls.

    ``outcome(source_path)`` decides the stubbed return value.
    """
    import scanner
    calls = []

    def fake_extract(source_path, output_path, max_size=4096, quality=92):
        calls.append((str(source_path), str(output_path)))
        return outcome(str(source_path))

    monkeypatch.setattr(scanner, "extract_working_copy", fake_extract)
    return calls


def test_working_copy_extracted_from_card_path(tmp_path, monkeypatch):
    """The working copy reads the CARD copy of a RAW file, not the archive
    copy — after import, no processing stage re-reads originals from the
    (possibly slow) archive volume."""
    from import_job import ImportParams

    calls = _stub_extractor(monkeypatch, lambda src: True)

    card = tmp_path / "card"
    card.mkdir()
    # JPEG bytes under a RAW extension: extraction is stubbed, so only the
    # extension-based RAW candidacy matters.
    Image.new("RGB", (16, 16), "red").save(str(card / "DSC_0500.jpg"))
    os.rename(str(card / "DSC_0500.jpg"), str(card / "DSC_0500.NEF"))

    archive = tmp_path / "archive"
    vireo_dir = tmp_path / "vireo"
    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(archive),
        vireo_dir=str(vireo_dir),
    ))

    assert result["copied"] == 1
    assert result["safe_to_format"] is True
    assert len(calls) == 1
    src, out = calls[0]
    assert src == str(card / "DSC_0500.NEF"), (
        f"extraction read {src}, expected the card path"
    )
    rows = _photo_rows(db)
    assert len(rows) == 1
    pid = rows[0]["id"]
    assert out == os.path.join(str(vireo_dir), "working", f"{pid}.jpg")
    wc = db.conn.execute(
        "SELECT working_copy_path FROM photos WHERE id = ?", (pid,),
    ).fetchone()["working_copy_path"]
    assert wc == f"working/{pid}.jpg"


def test_working_copy_companion_fallback_reads_card_jpeg(
        tmp_path, monkeypatch):
    """RAW+JPEG pair: when the RAW decode fails, the companion fallback
    must read the CARD's JPEG. Also pins that the companion file (whose
    photo row is deliberately merged away by pairing) is not bucketed as
    a failure by the hash-stamping pass."""
    from import_job import ImportParams

    calls = _stub_extractor(
        monkeypatch, lambda src: not src.lower().endswith(".nef"),
    )

    card = tmp_path / "card"
    card.mkdir()
    Image.new("RGB", (16, 16), "red").save(str(card / "DSC_0501.jpg"))
    # The RAW must be distinct bytes (a real pair always is) or the
    # duplicate gate would skip it as an intra-run twin of the JPEG.
    raw_bytes = (card / "DSC_0501.jpg").read_bytes() + b"RAW-SENSOR-DATA"
    (card / "DSC_0501.NEF").write_bytes(raw_bytes)

    archive = tmp_path / "archive"
    vireo_dir = tmp_path / "vireo"
    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(archive),
        vireo_dir=str(vireo_dir),
    ))

    assert result["copied"] == 2
    assert result["failed"] == 0
    assert result["safe_to_format"] is True

    # RAW attempt from the card, then companion fallback from the card.
    sources_tried = [src for src, _ in calls]
    assert str(card / "DSC_0501.NEF") in sources_tried
    assert str(card / "DSC_0501.jpg") in sources_tried

    # Pairing merged the JPEG row into the RAW primary.
    rows = _photo_rows(db)
    assert len(rows) == 1
    assert rows[0]["filename"] == "DSC_0501.NEF"
    wc = db.conn.execute(
        "SELECT working_copy_path, companion_path FROM photos WHERE id = ?",
        (rows[0]["id"],),
    ).fetchone()
    assert wc["working_copy_path"] == f"working/{rows[0]['id']}.jpg"
    assert wc["companion_path"] == "DSC_0501.jpg"


def test_failed_extraction_leaves_working_copy_null(tmp_path, monkeypatch):
    from import_job import ImportParams

    _stub_extractor(monkeypatch, lambda src: False)

    card = tmp_path / "card"
    card.mkdir()
    Image.new("RGB", (16, 16), "red").save(str(card / "DSC_0502.jpg"))
    os.rename(str(card / "DSC_0502.jpg"), str(card / "DSC_0502.NEF"))

    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(tmp_path / "archive"),
        vireo_dir=str(tmp_path / "vireo"),
    ))

    # Import itself still succeeds; the backfill retries extraction later.
    assert result["copied"] == 1
    assert result["safe_to_format"] is True
    rows = _photo_rows(db)
    wc = db.conn.execute(
        "SELECT working_copy_path FROM photos WHERE id = ?",
        (rows[0]["id"],),
    ).fetchone()["working_copy_path"]
    assert wc is None


# --- interruption + resume contract (Task 2.6) ---------------------------
# These tests prove _deindex_staging has no equivalent here: every stopping
# point leaves a valid catalog, and a retry resumes instead of redoing.

class CancelAfterFirstBatchRunner(FakeRunner):
    """Flips to cancelled once progress reports a file in the second
    destination folder (i.e. the second batch has started)."""

    def __init__(self, trigger_fragment):
        super().__init__()
        self.trigger_fragment = trigger_fragment

    def push_event(self, job_id, event_type, data):
        super().push_event(job_id, event_type, data)
        if (
            event_type == "progress"
            and self.trigger_fragment in (data.get("phase") or "")
        ):
            self.cancelled_ids.add(job_id)


def test_cancel_leaves_valid_partial_catalog(tmp_path):
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0030.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0031.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
        ("DSC_0032.jpg", datetime(2026, 7, 4, 9, 0, 0), "blue"),
        ("DSC_0033.jpg", datetime(2026, 7, 4, 9, 5, 0), "white"),
    ])
    archive = tmp_path / "archive"
    runner = CancelAfterFirstBatchRunner("2026/2026-07-04")

    db, ws_id, result = _run_import(
        tmp_path,
        ImportParams(sources=[str(card)], destination=str(archive)),
        runner=runner,
    )

    assert result["cancelled"] is True
    assert result["safe_to_format"] is False
    # Partial progress: all of batch 1, some of batch 2 — never zero,
    # never everything.
    assert 0 < result["copied"] < 4
    assert result["failed"] == 0

    # The catalog is valid: every row's file exists on disk, verified.
    rows = _photo_rows(db)
    assert len(rows) == result["copied"]
    for r in rows:
        full = os.path.join(r["folder_path"], r["filename"])
        assert os.path.isfile(full)
        assert r["hash_status"] == "ok"


def test_rerun_resumes_and_completes(tmp_path):
    """Re-running the same import after a cancel skips exactly what landed
    and copies the rest — no unwinding, no redo."""
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0030.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0031.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
        ("DSC_0032.jpg", datetime(2026, 7, 4, 9, 0, 0), "blue"),
        ("DSC_0033.jpg", datetime(2026, 7, 4, 9, 5, 0), "white"),
    ])
    archive = tmp_path / "archive"
    params = ImportParams(sources=[str(card)], destination=str(archive))

    runner = CancelAfterFirstBatchRunner("2026/2026-07-04")
    db, ws_id, first = _run_import(tmp_path, params, runner=runner)
    landed_first = first["copied"]
    assert 0 < landed_first < 4

    # Second run: same card, same params, fresh runner/job.
    from import_job import run_import_job
    second = run_import_job(
        _make_job("import-test-2"), FakeRunner(),
        str(tmp_path / "test.db"), ws_id, params,
    )

    assert second["cancelled"] is False
    assert second["failed"] == 0
    # Everything already landed is skipped; only the remainder copies.
    assert second["copied"] == 4 - landed_first
    assert second["copied"] + second["skipped_duplicate"] == 4
    assert second["safe_to_format"] is True

    # Combined catalog: exactly one row per card file, all verified.
    rows = _photo_rows(db)
    assert len(rows) == 4
    names = sorted(r["filename"] for r in rows)
    assert names == [
        "DSC_0030.jpg", "DSC_0031.jpg", "DSC_0032.jpg", "DSC_0033.jpg",
    ]
    for r in rows:
        assert os.path.isfile(os.path.join(r["folder_path"], r["filename"]))


def test_crash_shaped_copies_are_adopted_not_suffixed(tmp_path):
    """Files that landed on disk but died before their batch's scan (no
    catalog rows) must be adopted as already-present on re-run — cataloged
    without creating numeric-suffix duplicates. This is the 'rescan
    self-heals' story from the design doc."""
    import shutil

    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0040.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0041.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])
    archive = tmp_path / "archive"

    # Simulate the crash: both files already at the destination,
    # byte-identical, with NO catalog rows.
    dest_dir = archive / "2026" / "2026-07-03"
    dest_dir.mkdir(parents=True)
    for name in ("DSC_0040.jpg", "DSC_0041.jpg"):
        shutil.copy2(str(card / name), str(dest_dir / name))

    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(archive),
    ))

    # Adopted as already-present, cataloged, no re-copy, no suffixes.
    assert result["copied"] == 0
    assert result["skipped_duplicate"] == 2
    assert result["failed"] == 0
    assert result["safe_to_format"] is True
    assert sorted(os.listdir(str(dest_dir))) == [
        "DSC_0040.jpg", "DSC_0041.jpg",
    ]
    rows = _photo_rows(db)
    assert len(rows) == 2
    for r in rows:
        assert r["hash_status"] == "ok"
        assert os.path.isfile(os.path.join(r["folder_path"], r["filename"]))


# --- Codex review 2026-07-05 regressions ---------------------------------


def test_copy_and_hash_verify_refuses_to_overwrite_existing_destination(
        tmp_path):
    """Concurrent imports targeting the same destination/filename cannot
    both pass the pre-copy collision check and then both promote their
    temp file; ``copy_and_hash_verify`` must fail the second promote
    (leaving the first job's verified archive bytes untouched) rather
    than silently overwriting with ``os.replace``.
    """
    from import_job import copy_and_hash_verify

    src = tmp_path / "card" / "DSC_9001.jpg"
    src.parent.mkdir()
    src.write_bytes(b"card bytes")

    dst = tmp_path / "archive" / "DSC_9001.jpg"
    dst.parent.mkdir()
    dst.write_bytes(b"already-verified archive bytes")

    ok, file_hash = copy_and_hash_verify(str(src), str(dst))
    assert ok is False
    assert file_hash is None
    # The pre-existing verified copy must survive the race.
    assert dst.read_bytes() == b"already-verified archive bytes"
    # And the temp file must be cleaned up.
    assert not list(dst.parent.glob(".DSC_9001.jpg.*.tmp"))


def test_import_destination_with_dot_segments_normalizes_scan_root(tmp_path):
    """Absolute destinations containing ``..`` segments must still catalog
    successfully. The scanner stops folder-chain recursion when a parent
    equals the scan root string; if the copy layout normalizes the path
    but the scan root does not, the recursion never reaches root and the
    copied files bucket as catalog failures.
    """
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0090.jpg", datetime(2026, 7, 4, 10, 0, 0), "red"),
    ])

    real_archive = tmp_path / "archive"
    real_archive.mkdir()
    (tmp_path / "junk").mkdir()

    # Absolute path with a dot segment resolving to real_archive.
    unnormalized = str(tmp_path / "junk" / ".." / "archive")

    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=unnormalized,
    ))

    assert result["copied"] == 1
    assert result["failed"] == 0
    assert result["safe_to_format"] is True

    rows = _photo_rows(db)
    assert len(rows) == 1
    assert os.path.isfile(
        os.path.join(rows[0]["folder_path"], rows[0]["filename"])
    )
    # And the file actually lives under the normalized archive root, not
    # a duplicated dot-segment path.
    assert os.path.realpath(rows[0]["folder_path"]).startswith(
        os.path.realpath(str(real_archive))
    )


def test_unreadable_source_subtree_flips_safe_to_format_off(
        tmp_path, monkeypatch):
    """If ``discover_source_files`` cannot enter a source subtree
    (permission denied, TCC block, unreadable removable-media dir), the
    files under that subtree are unseen — ``discovered`` shrinks silently
    and ``safe_to_format`` used to still flip green. Enumeration errors
    must now bubble into the ledger.
    """
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0100.jpg", datetime(2026, 7, 4, 10, 0, 0), "red"),
    ])

    # Force safe_scan_walk to report an error on the first walk step.
    import image_loader
    real_walk = image_loader.safe_scan_walk

    def broken_walk(top, onerror=None):
        # Simulate a PermissionError bubbling up from os.scandir on the
        # source root; safe_scan_walk's OSError branch would forward it
        # via onerror and yield nothing further.
        if onerror is not None:
            onerror(PermissionError(13, "Operation not permitted", str(top)))
        # Still yield everything real_walk would have produced so we can
        # verify the ledger is unsafe even when copies still landed.
        yield from real_walk(top, onerror=onerror)

    monkeypatch.setattr("ingest.safe_scan_walk", broken_walk)

    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(tmp_path / "archive"),
    ))

    # The one visible file still lands in the catalog...
    assert result["copied"] == 1
    # ...but safe_to_format is False because we couldn't prove every
    # source subtree was walked cleanly.
    assert result["safe_to_format"] is False
    assert result["ok"] is False
    assert result["discovery_errors"] == 1
    assert any(
        "source enumeration failed" in e for e in result["errors"]
    )


def test_nonexistent_source_root_flips_safe_to_format_off(tmp_path):
    """If a requested source root cannot be positively walked at all
    (unmounted removable media, permission denied on the root itself, or
    a path that no longer exists between enqueue and worker start),
    ``discover_source_files`` used to return ``[]`` at its pre-walk
    ``is_dir()`` guard WITHOUT invoking the ``onerror`` collector. That
    left ``discovered == 0`` and ``discovery_errors`` empty, so the
    predicate reported ``safe_to_format: true`` even though no card
    contents were ever enumerated — the UI would tell the user it's safe
    to format a card whose contents were never verified. See PR #1107
    review (P1 line 927).
    """
    from import_job import ImportParams

    missing_card = tmp_path / "unmounted-card"
    # Deliberately do NOT create the directory. This mirrors a card that
    # was ejected between enqueue and the import worker starting.

    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(missing_card)],
        destination=str(tmp_path / "archive"),
    ))

    assert result["discovered"] == 0
    assert result["safe_to_format"] is False
    assert result["ok"] is False
    assert result["discovery_errors"] == 1
    assert any(
        "source enumeration failed" in e for e in result["errors"]
    )
    assert any(str(missing_card) in e for e in result["errors"])


def test_excluded_bundle_source_root_flips_safe_to_format_off(tmp_path):
    """Same guarantee for the excluded-bundle branch of the pre-walk
    guard: if the caller pointed the import at a Photos-library-style
    data bundle directly (or the root is otherwise refused), the run
    must not silently report zero-files-imported-safe-to-format.
    """
    from import_job import ImportParams

    bundle = tmp_path / "Photos Library.photoslibrary"
    bundle.mkdir()
    # A single file inside the bundle so a naive walk would find it —
    # the guard must fire on the ROOT and refuse to enumerate it,
    # bubbling that refusal as a discovery error.
    (bundle / "originals").mkdir()
    (bundle / "originals" / "managed.jpg").write_bytes(b"jpeg")

    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(bundle)],
        destination=str(tmp_path / "archive"),
    ))

    assert result["discovered"] == 0
    assert result["safe_to_format"] is False
    assert result["ok"] is False
    assert result["discovery_errors"] == 1


def test_filtered_import_is_never_safe_to_format(tmp_path):
    """A narrowed ``file_types`` (``"raw"``, ``"jpeg"``, or a custom list)
    only enumerates the requested subset, so ``discovered`` covers less
    than the card's actual supported-file footprint. The naive
    ``copied + skipped_duplicate == discovered`` check would still pass
    even though other supported photos on the card were never imported —
    and the pill would then tell the user it's safe to format a card that
    still holds files. See PR #1107 review (P1 line 420).
    """
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])
    archive = tmp_path / "archive"

    # ``file_types="jpeg"`` still copies every file this card actually
    # holds (they're all JPEGs), so copied == discovered would otherwise
    # flip safe_to_format green.
    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)],
        destination=str(archive),
        file_types="jpeg",
    ))

    assert result["copied"] == 2
    assert result["failed"] == 0
    assert result["discovered"] == 2
    # But the run only asked for JPEGs — a RAW sitting on the same card
    # would have been silently skipped. The pill has no way to prove
    # otherwise without re-walking the card, so it stays false.
    assert result["safe_to_format"] is False


def test_custom_file_types_list_is_never_safe_to_format(tmp_path):
    """Same guarantee for the explicit-extension-list form of
    ``file_types``: any narrowing counts as filtered."""
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])
    archive = tmp_path / "archive"

    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)],
        destination=str(archive),
        file_types=[".jpg"],
    ))

    assert result["copied"] == 1
    assert result["safe_to_format"] is False


def test_non_recursive_import_is_never_safe_to_format(tmp_path):
    """``recursive=False`` only enumerates top-level files, so any card
    with photos in subdirectories has files ``discovered`` never saw.
    ``copied + skipped_duplicate == discovered`` would still pass and the
    pill would tell the user it's safe to format a card that still holds
    unimported photos in subfolders. See PR #1107 Codex review on commit
    7dc0cce (import_job.py:1350).
    """
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])
    # A photo tucked in a subdirectory the non-recursive walk cannot see.
    subdir = card / "subfolder"
    subdir.mkdir()
    Image.new("RGB", (16, 16), "blue").save(str(subdir / "DSC_0002.jpg"))
    ts = datetime(2026, 7, 3, 11, 0, 0).timestamp()
    os.utime(str(subdir / "DSC_0002.jpg"), (ts, ts))
    archive = tmp_path / "archive"

    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)],
        destination=str(archive),
        recursive=False,
    ))

    # The top-level file copied cleanly, so the naive equality check
    # (copied + skipped == discovered) would otherwise flip green.
    assert result["copied"] == 1
    assert result["failed"] == 0
    assert result["discovered"] == 1
    # But the non-recursive walk never saw ``subfolder/DSC_0002.jpg``;
    # formatting the card would delete an unimported photo.
    assert result["safe_to_format"] is False


def test_deferred_extraction_skipped_when_already_cancelled(
    tmp_path, monkeypatch,
):
    """If the run was cancelled at a batch boundary before the deferred
    working-copy pass, don't spend minutes decoding RAWs the user has
    already asked us to abort. The extractor must not be called at all,
    and the returned status must remain ``cancelled``. See PR #1107
    Codex review on commit 7dc0cce (import_job.py:1296).
    """
    import scanner
    from import_job import ImportParams

    calls = []

    def spy_extract(*args, **kwargs):
        calls.append((args, kwargs))

    monkeypatch.setattr(scanner, "_extract_working_copies", spy_extract)

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])
    archive = tmp_path / "archive"

    runner = FakeRunner()
    job = _make_job()
    # Cancel before the run starts. The first batch-boundary check flips
    # ``cancelled`` on, and the deferred pass must then be skipped.
    runner.cancelled_ids.add(job["id"])

    db, ws_id, result = _run_import(
        tmp_path,
        ImportParams(
            sources=[str(card)],
            destination=str(archive),
            vireo_dir=str(tmp_path / "vireo_dir"),
        ),
        runner=runner,
        job=job,
    )

    assert result["cancelled"] is True
    assert result["safe_to_format"] is False
    assert calls == [], (
        "deferred _extract_working_copies must be skipped when cancelled"
    )


def test_deferred_extraction_threads_cancel_check(tmp_path, monkeypatch):
    """When the deferred working-copy pass does run, it must receive a
    ``cancel_check`` callable so a cancel issued mid-pass aborts the
    per-row loop instead of decoding every RAW to completion. See PR
    #1107 Codex review on commit 7dc0cce (import_job.py:1296).
    """
    import scanner
    from import_job import ImportParams

    captured = {}

    def spy_extract(*args, **kwargs):
        captured["cancel_check"] = kwargs.get("cancel_check")
        captured["source_paths"] = kwargs.get("source_paths")

    monkeypatch.setattr(scanner, "_extract_working_copies", spy_extract)

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])
    archive = tmp_path / "archive"

    runner = FakeRunner()
    job = _make_job()

    _run_import(
        tmp_path,
        ImportParams(
            sources=[str(card)],
            destination=str(archive),
            vireo_dir=str(tmp_path / "vireo_dir"),
        ),
        runner=runner,
        job=job,
    )

    cancel_check = captured.get("cancel_check")
    assert callable(cancel_check), (
        "deferred pass must receive a cancel_check callable"
    )
    # Not cancelled yet → callable returns falsy.
    assert not cancel_check()
    # Once the runner records a cancel, the callable flips to truthy so
    # ``_extract_working_copies`` bails out on the next row check.
    runner.cancelled_ids.add(job["id"])
    assert cancel_check()


def test_wc_extraction_deferred_to_after_last_batch(tmp_path, monkeypatch):
    """A RAW+JPEG companion pair that straddles a batch boundary must not
    trigger per-batch working-copy extraction while the JPEG's row is
    still uncataloged (pairing has not run yet). Deferring extraction to
    end-of-run guarantees ``_pair_raw_jpeg_companions`` has seen every
    JPEG in every batch before the extractor decides which source to
    read.
    """
    import import_job
    import scanner

    monkeypatch.setattr(import_job, "IMPORT_BATCH_SIZE", 1)

    # Track extraction call order. ``extract_working_copy`` is invoked
    # once per candidate row; we care that when the RAW is processed the
    # JPEG's row has already been merged in (pairing has run).
    calls = []

    def fake_extract(source_path, output_path, max_size=4096, quality=92):
        calls.append(str(source_path))
        # Simulate a RAW decode failure so the companion fallback path
        # is exercised — the reason deferral matters at all.
        return not str(source_path).lower().endswith(".nef")

    monkeypatch.setattr(scanner, "extract_working_copy", fake_extract)

    card = tmp_path / "card"
    card.mkdir()
    Image.new("RGB", (16, 16), "red").save(str(card / "DSC_0700.jpg"))
    raw_bytes = (card / "DSC_0700.jpg").read_bytes() + b"RAW-SENSOR-DATA"
    (card / "DSC_0700.NEF").write_bytes(raw_bytes)
    # Same mtime so both files plan into the same destination folder.
    ts = datetime(2026, 7, 4, 10, 0, 0).timestamp()
    for name in ("DSC_0700.jpg", "DSC_0700.NEF"):
        os.utime(str(card / name), (ts, ts))

    vireo_dir = tmp_path / "vireo"
    db, ws_id, result = _run_import(tmp_path, import_job.ImportParams(
        sources=[str(card)], destination=str(tmp_path / "archive"),
        vireo_dir=str(vireo_dir),
    ))

    assert result["copied"] == 2
    assert result["failed"] == 0
    assert result["safe_to_format"] is True

    # After deferred extraction, pairing has merged the JPEG row into
    # the RAW primary and the extractor read the companion (from the
    # card) after the RAW decode was stubbed as failed.
    rows = _photo_rows(db)
    assert len(rows) == 1
    assert rows[0]["filename"] == "DSC_0700.NEF"
    wc = db.conn.execute(
        "SELECT working_copy_path, companion_path FROM photos WHERE id = ?",
        (rows[0]["id"],),
    ).fetchone()
    assert wc["companion_path"] == "DSC_0700.jpg"
    assert wc["working_copy_path"] == f"working/{rows[0]['id']}.jpg"

    # The JPEG source must have been read from the card, not the archive.
    jpeg_reads = [c for c in calls if c.lower().endswith(".jpg")]
    assert jpeg_reads, "companion fallback should have been attempted"
    assert all(str(card) in c for c in jpeg_reads), (
        f"companion extraction should read from the card, got {jpeg_reads}"
    )


def test_remote_import_wc_identity_captured_before_transfer(
        tmp_path, monkeypatch):
    """Spec decision 7: the working-copy identity tuple ``(size,
    mtime_ns)`` must attest the SOURCE at decision time. The remote path
    historically stat'd the source AFTER the transfer, so a source that
    changed mid-transfer (card glitch, live folder) still looked clean
    to the working-copy identity check. Mirrors the local path, which
    stats before the copy."""
    import move as _move
    import scanner as _scanner

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)
    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])
    src = card / "DSC_0001.jpg"
    pre_size = src.stat().st_size
    pre_mtime_ns = src.stat().st_mtime_ns

    base_fake = _move._run_rsync_streamed  # the harness fake

    def mutating_rsync(*args, **kw):
        rc = base_fake(*args, **kw)
        # The source changes while/just after the batch is on the wire:
        # append a byte and bump mtime. Decision-time capture must not
        # see this.
        with open(src, "ab") as fh:
            fh.write(b"x")
        os.utime(src, ns=(pre_mtime_ns + 5_000_000_000,
                          pre_mtime_ns + 5_000_000_000))
        return rc

    monkeypatch.setattr(_move, "_run_rsync_streamed", mutating_rsync)

    captured = {}

    def spy_extract(*args, **kwargs):
        captured["source_paths"] = dict(kwargs.get("source_paths") or {})
        return None

    monkeypatch.setattr(_scanner, "_extract_working_copies", spy_extract)

    from import_job import ImportParams, run_import_job
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, db._active_workspace_id,
        ImportParams(sources=[str(card)], destination=ra["mount_base"],
                     remote_target=ra, verify_by_hash=True,
                     vireo_dir=str(tmp_path / "vdir")))

    assert result["copied"] == 1, result
    assert captured, "working-copy extraction never ran"
    [(dest_path, (sf, sz, mt))] = captured["source_paths"].items()
    # ``wc_source_paths`` is keyed by the mount-side destination path.
    assert dest_path.endswith("DSC_0001.jpg")
    assert sf == str(src)
    # Identity attests the source BEFORE the mid-transfer mutation.
    assert (sz, mt) == (pre_size, pre_mtime_ns), (sz, mt)


# --- Codex 2026-07-05 followups: two findings not addressed by 9e0834af ----

def test_checker_record_oserror_does_not_kill_job(tmp_path, monkeypatch):
    """If ``DuplicateChecker.record`` re-``os.stat``s the source after a
    verified ``copy_and_hash_verify`` succeeded and the card has since
    been pulled, the OSError must not escape and kill the background
    job — the file is already verified at the archive, so the ledger
    keeps the copy and the run continues to catalog what landed."""
    import import_dedup
    from import_job import ImportParams, run_import_job

    card = _make_card(tmp_path, [
        ("DSC_0A60.jpg", datetime(2026, 8, 4, 10, 0, 0), "red"),
        ("DSC_0A61.jpg", datetime(2026, 8, 4, 10, 5, 0), "green"),
    ])

    real_record = import_dedup.DuplicateChecker.record
    calls = {"n": 0}

    def flaky_record(self, source_file):
        calls["n"] += 1
        if calls["n"] == 1:
            # First file lands, then card "goes away" — record's re-stat
            # raises. The file itself is on the archive already.
            raise OSError("card yanked after copy")
        return real_record(self, source_file)

    monkeypatch.setattr(
        import_dedup.DuplicateChecker, "record", flaky_record,
    )

    archive = tmp_path / "archive"
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(sources=[str(card)], destination=str(archive)),
    )

    # Both files landed and were cataloged (the record OSError was a
    # bookkeeping optimization; the archive is the source of truth).
    assert result["copied"] == 2
    assert result["failed"] == 0
    assert result["safe_to_format"] is True
    rows = _photo_rows(db)
    assert {r["filename"] for r in rows} == {
        "DSC_0A60.jpg", "DSC_0A61.jpg",
    }
    for r in rows:
        assert os.path.isfile(os.path.join(r["folder_path"], r["filename"]))
        assert r["hash_status"] == "ok"


def test_dup_workspace_link_failure_marks_unsafe(tmp_path, monkeypatch):
    """A direct workspace-link failure must keep safe_to_format false and
    identify the folder whose existing catalog rows could not be exposed."""
    from import_dedup import compute_file_hash
    from import_job import ImportParams, run_import_job

    # Pre-catalog a photo at the archive destination WITHOUT linking its
    # folder to the active workspace (raw SQL, no workspace_folders rows).
    archive = tmp_path / "archive"
    dest_dir = archive / "2026" / "2026-08-05"
    dest_dir.mkdir(parents=True)
    dest_file = dest_dir / "IMG_0A80.jpg"
    Image.new("RGB", (16, 16), "red").save(str(dest_file))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (str(dest_dir), dest_dir.name),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_hash) VALUES (?, ?, '.jpg', ?, ?)",
        (
            fid,
            "IMG_0A80.jpg",
            os.path.getsize(str(dest_file)),
            compute_file_hash(str(dest_file)),
        ),
    )
    db.conn.commit()

    # Card holds a byte-identical copy → duplicate-only batch.
    card = tmp_path / "card"
    card.mkdir()
    import shutil
    shutil.copy2(str(dest_file), str(card / "IMG_0A80.jpg"))

    real_link = Database.add_workspace_folder

    def flaky_link(self, workspace_id, folder_id, *, is_root=True):
        if folder_id == fid:
            raise OSError("simulated workspace-link failure")
        return real_link(self, workspace_id, folder_id, is_root=is_root)

    monkeypatch.setattr(Database, "add_workspace_folder", flaky_link)

    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(sources=[str(card)], destination=str(archive)),
    )

    assert result["skipped_duplicate"] == 1
    assert result["safe_to_format"] is False
    assert result["ok"] is False
    assert any(
        str(dest_dir) in u["path"] for u in result["unsafe_files"]
    ), (
        "expected the failing dup-link folder in unsafe_files; got "
        f"{result['unsafe_files']!r}"
    )
    # The seeded folder still isn't linked (that was the whole point).
    assert str(dest_dir) not in _ws_linked_folder_paths(db, ws_id)


def test_dup_workspace_link_runtime_error_marks_unsafe(
        tmp_path, monkeypatch):
    """A RuntimeError from the direct link is a real import failure, not
    scanner cancellation, and must leave the matched folder unlinked."""
    from import_dedup import compute_file_hash
    from import_job import ImportParams, run_import_job

    archive = tmp_path / "archive"
    dest_dir = archive / "2026" / "2026-08-06"
    dest_dir.mkdir(parents=True)
    dest_file = dest_dir / "IMG_0B81.jpg"
    Image.new("RGB", (16, 16), "green").save(str(dest_file))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (str(dest_dir), dest_dir.name),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_hash) VALUES (?, ?, '.jpg', ?, ?)",
        (
            fid,
            "IMG_0B81.jpg",
            os.path.getsize(str(dest_file)),
            compute_file_hash(str(dest_file)),
        ),
    )
    db.conn.commit()

    card = tmp_path / "card"
    card.mkdir()
    import shutil
    shutil.copy2(str(dest_file), str(card / "IMG_0B81.jpg"))

    real_link = Database.add_workspace_folder

    def flaky_link(self, workspace_id, folder_id, *, is_root=True):
        if folder_id == fid:
            raise RuntimeError("database link exploded")
        return real_link(self, workspace_id, folder_id, is_root=is_root)

    monkeypatch.setattr(Database, "add_workspace_folder", flaky_link)

    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(sources=[str(card)], destination=str(archive)),
    )

    assert result["skipped_duplicate"] == 1
    # Direct-link errors are not cancellation sentinels.
    assert result["cancelled"] is False
    assert result["safe_to_format"] is False
    assert result["ok"] is False
    assert any(
        str(dest_dir) in u["path"] for u in result["unsafe_files"]
    ), (
        "expected the failing dup-link folder in unsafe_files; got "
        f"{result['unsafe_files']!r}"
    )
    assert str(dest_dir) not in _ws_linked_folder_paths(db, ws_id)


def test_wc_extraction_falls_back_to_archive_when_card_vanishes(
        tmp_path, monkeypatch):
    """When the deferred working-copy pass runs after copying and the
    card has been unmounted, ``source_paths`` still points at the card's
    dead path; the extractor must NOT read from that missing path and
    record a failure marker. It must fall back to the verified archive
    copy, so the extraction reads a live file.
    """
    import import_job
    import scanner

    # A RAW file (fake .NEF) is what makes the row a working-copy
    # extraction candidate — a bare small JPEG is skipped by the
    # candidate predicate. Same trick as
    # ``test_wc_extraction_deferred_to_after_last_batch``: bytes that
    # scanner's metadata reader accepts as an image, extra bytes past
    # the end to distinguish RAW from JPEG.
    card = tmp_path / "card"
    card.mkdir()
    Image.new("RGB", (16, 16), "red").save(str(card / "DSC_1001.jpg"))
    raw_bytes = (card / "DSC_1001.jpg").read_bytes() + b"RAW-SENSOR-DATA"
    (card / "DSC_1001.NEF").write_bytes(raw_bytes)
    (card / "DSC_1001.jpg").unlink()  # RAW-only, no companion
    ts = datetime(2026, 7, 4, 10, 0, 0).timestamp()
    os.utime(str(card / "DSC_1001.NEF"), (ts, ts))

    # Track every extract_working_copy call's source path.
    calls = []

    def fake_extract(source_path, output_path, max_size=4096, quality=92):
        calls.append(str(source_path))
        if not os.path.isfile(source_path):
            return False
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "wb") as f:
            f.write(b"jpeg-bytes")
        return True

    monkeypatch.setattr(scanner, "extract_working_copy", fake_extract)

    vireo_dir = tmp_path / "vireo"
    (vireo_dir / "working").mkdir(parents=True)
    dest = str(tmp_path / "archive")

    # Unmount the card BETWEEN copy and end-of-run extraction: patch
    # ``scanner._extract_working_copies`` to unlink card files first,
    # then delegate to the real extractor.
    real_extract_wc = scanner._extract_working_copies

    def unmount_then_extract(*a, **kw):
        for card_file in list(card.iterdir()):
            card_file.unlink()
        card.rmdir()
        return real_extract_wc(*a, **kw)

    monkeypatch.setattr(
        "scanner._extract_working_copies", unmount_then_extract,
    )

    _db, _ws_id, result = _run_import(tmp_path, import_job.ImportParams(
        sources=[str(card)], destination=dest,
        vireo_dir=str(vireo_dir),
    ))

    assert result["copied"] == 1
    assert result["safe_to_format"] is True

    # The extractor was asked to read the archive path (which exists),
    # not the vanished card path. Without the fallback the extractor
    # would read only the dead card path and return False.
    assert calls, "extractor should have run"
    live_reads = [c for c in calls if os.path.isfile(c)]
    assert live_reads, (
        f"extractor should have read a live path (archive fallback), "
        f"got calls={calls}"
    )
    dead_reads = [c for c in calls if str(card) in c]
    assert not dead_reads, (
        f"extractor should not have read the vanished card path, "
        f"got dead reads={dead_reads}"
    )


def test_wc_extraction_ignores_card_override_when_size_no_longer_matches(
        tmp_path, monkeypatch):
    """The override existence check is not enough: if the card was reused
    (mount point holds a different card, or the same file rewritten with
    different content), reading it caches a working copy for the WRONG
    bytes — and because ``working_copy_path`` gets set, normal backfill
    won't regenerate from the archive. The extractor must compare the
    override's on-disk size against the row's file_size and fall back to
    the verified archive copy on mismatch.
    """
    import import_job
    import scanner

    # Card holds a RAW file with a distinctive size.
    card = tmp_path / "card"
    card.mkdir()
    Image.new("RGB", (16, 16), "red").save(str(card / "DSC_2001.jpg"))
    raw_bytes = (card / "DSC_2001.jpg").read_bytes() + b"RAW-SENSOR-DATA"
    (card / "DSC_2001.NEF").write_bytes(raw_bytes)
    (card / "DSC_2001.jpg").unlink()
    ts = datetime(2026, 7, 5, 10, 0, 0).timestamp()
    os.utime(str(card / "DSC_2001.NEF"), (ts, ts))
    original_size = os.path.getsize(str(card / "DSC_2001.NEF"))

    calls = []

    def fake_extract(source_path, output_path, max_size=4096, quality=92):
        calls.append(str(source_path))
        if not os.path.isfile(source_path):
            return False
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "wb") as f:
            f.write(b"jpeg-bytes")
        return True

    monkeypatch.setattr(scanner, "extract_working_copy", fake_extract)

    vireo_dir = tmp_path / "vireo"
    (vireo_dir / "working").mkdir(parents=True)
    dest = str(tmp_path / "archive")

    # Between copy+catalog and the end-of-run extraction, rewrite the
    # card file with DIFFERENT bytes (and a different size). This mimics
    # the card being reused for a different shoot, or the same file being
    # rewritten by the camera. os.path.isfile still returns True — only
    # a size compare catches it.
    real_extract_wc = scanner._extract_working_copies

    def rewrite_then_extract(*a, **kw):
        card_raw = card / "DSC_2001.NEF"
        card_raw.write_bytes(b"COMPLETELY DIFFERENT CONTENT")
        assert os.path.getsize(str(card_raw)) != original_size
        return real_extract_wc(*a, **kw)

    monkeypatch.setattr(
        "scanner._extract_working_copies", rewrite_then_extract,
    )

    _db, _ws_id, result = _run_import(tmp_path, import_job.ImportParams(
        sources=[str(card)], destination=dest,
        vireo_dir=str(vireo_dir),
    ))

    assert result["copied"] == 1

    # The extractor must have read the ARCHIVE path (verified bytes),
    # never the rewritten card path. Without the size check the extractor
    # would happily read the card's new bytes and cache a wrong working
    # copy — indistinguishable in the WC file from a real success.
    assert calls, "extractor should have run"
    archive_reads = [c for c in calls if str(card) not in c]
    card_reads = [c for c in calls if str(card) in c]
    assert archive_reads, (
        f"extractor should have read from the archive (size mismatch → "
        f"fall back to catalog primary); got calls={calls}"
    )
    assert not card_reads, (
        f"extractor should not have read the rewritten card path (size "
        f"no longer matches file_size); got card reads={card_reads}"
    )


def test_wc_extraction_ignores_card_override_when_mtime_no_longer_matches(
        tmp_path, monkeypatch):
    """Size alone is not enough: a rewritten card file (same byte count,
    different content) OR a reused card mount holding a coincidentally
    same-sized file would pass a size-only guard and cache a working
    copy for the WRONG bytes. mtime narrows trust from "any same-sized
    file at this path" to "the exact file we just copied" — a rewrite
    or a remount presents a different mtime. This test rewrites the
    card RAW between copy and the deferred extraction with EXACTLY the
    same size (identical original bytes shuffled) but a fresh mtime;
    the extractor must fall back to the verified archive copy.
    """
    import import_job
    import scanner

    card = tmp_path / "card"
    card.mkdir()
    Image.new("RGB", (16, 16), "red").save(str(card / "DSC_4001.jpg"))
    raw_bytes = (card / "DSC_4001.jpg").read_bytes() + b"RAW-SENSOR-DATA"
    (card / "DSC_4001.NEF").write_bytes(raw_bytes)
    (card / "DSC_4001.jpg").unlink()
    original_mtime = datetime(2026, 7, 5, 12, 0, 0).timestamp()
    os.utime(str(card / "DSC_4001.NEF"), (original_mtime, original_mtime))
    original_size = os.path.getsize(str(card / "DSC_4001.NEF"))

    calls = []

    def fake_extract(source_path, output_path, max_size=4096, quality=92):
        calls.append(str(source_path))
        if not os.path.isfile(source_path):
            return False
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "wb") as f:
            f.write(b"jpeg-bytes")
        return True

    monkeypatch.setattr(scanner, "extract_working_copy", fake_extract)

    vireo_dir = tmp_path / "vireo"
    (vireo_dir / "working").mkdir(parents=True)
    dest = str(tmp_path / "archive")

    # Between copy+catalog and the end-of-run extraction, replace the
    # card file with DIFFERENT bytes of the SAME size and set a
    # DIFFERENT mtime. Size-only guards would accept this override; the
    # tightened identity check rejects it because mtime moved.
    real_extract_wc = scanner._extract_working_copies

    def rewrite_same_size_diff_mtime(*a, **kw):
        card_raw = card / "DSC_4001.NEF"
        different_bytes = bytes(reversed(card_raw.read_bytes()))
        card_raw.write_bytes(different_bytes)
        assert os.path.getsize(str(card_raw)) == original_size, (
            "replacement must keep byte count identical"
        )
        new_mtime = datetime(2026, 7, 6, 9, 0, 0).timestamp()
        os.utime(str(card_raw), (new_mtime, new_mtime))
        return real_extract_wc(*a, **kw)

    monkeypatch.setattr(
        "scanner._extract_working_copies", rewrite_same_size_diff_mtime,
    )

    _db, _ws_id, result = _run_import(tmp_path, import_job.ImportParams(
        sources=[str(card)], destination=dest,
        vireo_dir=str(vireo_dir),
    ))

    assert result["copied"] == 1
    assert calls, "extractor should have run"
    archive_reads = [c for c in calls if str(card) not in c]
    card_reads = [c for c in calls if str(card) in c]
    assert archive_reads, (
        f"extractor should have read from the archive (mtime mismatch → "
        f"fall back to catalog primary); got calls={calls}"
    )
    assert not card_reads, (
        f"extractor should not have read the rewritten card path (mtime "
        f"no longer matches captured identity); got card reads={card_reads}"
    )


def test_wc_extraction_ignores_companion_override_when_mtime_changes(
        tmp_path, monkeypatch):
    """RAW+JPEG pair: after copy the RAW's row carries companion_path
    pointing at the JPEG's archive path. The extractor's companion
    fallback used to accept any existing card-side JPEG at the override
    location without identity verification — a rewritten card-side JPEG
    (or a remounted card holding a same-sized JPEG) would then poison
    the RAW's working copy through the RAW-fails-fall-back-to-companion
    path. Identity-checking the companion override against (size, mtime)
    captured at import time makes the extractor read the verified
    archive companion on any mismatch.
    """
    import import_job
    import scanner

    card = tmp_path / "card"
    card.mkdir()
    Image.new("RGB", (16, 16), "blue").save(str(card / "DSC_5001.jpg"))
    raw_bytes = (card / "DSC_5001.jpg").read_bytes() + b"RAW-SENSOR-DATA"
    (card / "DSC_5001.NEF").write_bytes(raw_bytes)
    original_jpeg_bytes = (card / "DSC_5001.jpg").read_bytes()
    original_jpeg_size = len(original_jpeg_bytes)
    original_mtime = datetime(2026, 7, 5, 13, 0, 0).timestamp()
    os.utime(str(card / "DSC_5001.jpg"), (original_mtime, original_mtime))
    os.utime(str(card / "DSC_5001.NEF"), (original_mtime, original_mtime))

    calls = []

    def fake_extract(source_path, output_path, max_size=4096, quality=92):
        calls.append(str(source_path))
        # Force the RAW primary to fail so the extractor falls into the
        # companion-fallback branch — that's the code path where the
        # companion override identity check lives. .NEF is the RAW
        # extension used above.
        if str(source_path).lower().endswith(".nef"):
            return False
        if not os.path.isfile(source_path):
            return False
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "wb") as f:
            f.write(b"jpeg-bytes")
        return True

    monkeypatch.setattr(scanner, "extract_working_copy", fake_extract)

    vireo_dir = tmp_path / "vireo"
    (vireo_dir / "working").mkdir(parents=True)
    dest = str(tmp_path / "archive")

    # Between copy+catalog and the end-of-run extraction, rewrite the
    # card-side JPEG companion with DIFFERENT bytes of the SAME size and
    # a fresh mtime. Without the identity check the companion-fallback
    # branch would read this poisoned override.
    real_extract_wc = scanner._extract_working_copies

    def rewrite_companion_same_size(*a, **kw):
        card_jpeg = card / "DSC_5001.jpg"
        card_jpeg.write_bytes(bytes(reversed(original_jpeg_bytes)))
        assert os.path.getsize(str(card_jpeg)) == original_jpeg_size, (
            "replacement must keep byte count identical"
        )
        new_mtime = datetime(2026, 7, 6, 10, 0, 0).timestamp()
        os.utime(str(card_jpeg), (new_mtime, new_mtime))
        return real_extract_wc(*a, **kw)

    monkeypatch.setattr(
        "scanner._extract_working_copies", rewrite_companion_same_size,
    )

    _db, _ws_id, result = _run_import(tmp_path, import_job.ImportParams(
        sources=[str(card)], destination=dest,
        vireo_dir=str(vireo_dir),
    ))

    # Both the RAW and its JPEG landed in the archive.
    assert result["copied"] == 2, (
        f"expected RAW + JPEG both landed, got {result!r}"
    )
    assert calls, "extractor should have run"

    # The RAW extraction attempts (both card and archive) fail; the
    # extractor then falls back to the companion. That companion read
    # must be against the ARCHIVE JPEG, not the rewritten card JPEG —
    # the mtime mismatch on the card override forces archive fallback.
    jpeg_calls = [c for c in calls if c.lower().endswith(".jpg")]
    assert jpeg_calls, (
        f"expected companion fallback to run after RAW failure; "
        f"got calls={calls}"
    )
    card_jpeg_reads = [c for c in jpeg_calls if str(card) in c]
    archive_jpeg_reads = [c for c in jpeg_calls if str(card) not in c]
    assert archive_jpeg_reads, (
        f"extractor should have read the archive JPEG (companion mtime "
        f"mismatch → archive companion); got jpeg_calls={jpeg_calls}"
    )
    assert not card_jpeg_reads, (
        f"extractor should not have read the rewritten card JPEG "
        f"companion; got card jpeg reads={card_jpeg_reads}"
    )


def test_wc_extraction_retries_from_archive_when_card_read_fails(
        tmp_path, monkeypatch):
    """The size check can pass (card intact when we peek), then reading
    the file can still fail (transient I/O error, card unmounted right
    after the stat, permission blip). In that case the row would be
    marked ``working_copy_failed_at`` even though the archive copy is
    hash-verified and available; the extractor must retry from the
    catalog primary before giving up.
    """
    import import_job
    import scanner

    card = tmp_path / "card"
    card.mkdir()
    Image.new("RGB", (16, 16), "red").save(str(card / "DSC_3001.jpg"))
    raw_bytes = (card / "DSC_3001.jpg").read_bytes() + b"RAW-SENSOR-DATA"
    (card / "DSC_3001.NEF").write_bytes(raw_bytes)
    (card / "DSC_3001.jpg").unlink()
    ts = datetime(2026, 7, 5, 11, 0, 0).timestamp()
    os.utime(str(card / "DSC_3001.NEF"), (ts, ts))

    card_raw_path = str(card / "DSC_3001.NEF")
    calls = []

    def fake_extract(source_path, output_path, max_size=4096, quality=92):
        calls.append(str(source_path))
        # Card-side read always fails (simulate an unreadable RAW /
        # transient card I/O error); archive-side read succeeds.
        if str(source_path) == card_raw_path:
            return False
        if not os.path.isfile(source_path):
            return False
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "wb") as f:
            f.write(b"jpeg-bytes")
        return True

    monkeypatch.setattr(scanner, "extract_working_copy", fake_extract)

    vireo_dir = tmp_path / "vireo"
    (vireo_dir / "working").mkdir(parents=True)
    dest = str(tmp_path / "archive")

    db, _ws_id, result = _run_import(tmp_path, import_job.ImportParams(
        sources=[str(card)], destination=dest,
        vireo_dir=str(vireo_dir),
    ))

    assert result["copied"] == 1

    # The extractor tried the card (size matched), failed, then retried
    # from the archive and succeeded. Both reads visible in the call log.
    assert card_raw_path in calls, (
        f"expected the card override to be tried first; got calls={calls}"
    )
    archive_reads = [c for c in calls if c != card_raw_path]
    assert archive_reads, (
        f"expected retry from archive after card read failed; "
        f"got calls={calls}"
    )

    # The photo row must show a successful working_copy_path — the retry
    # from archive worked, so no failure marker.
    row = db.conn.execute(
        "SELECT working_copy_path, working_copy_failed_at FROM photos"
    ).fetchone()
    assert row["working_copy_path"] is not None, (
        "expected working_copy_path set after archive-retry success"
    )
    assert row["working_copy_failed_at"] is None, (
        "expected no failure marker after archive-retry success"
    )


def test_reclassified_landed_entry_skips_card_source_override(
        tmp_path, monkeypatch):
    """A landed entry reclassified as failed by the hash-stamping loop
    (because the archive file was mutated between ``copy_and_hash_verify``
    and the restricted scan) must not contribute a card-side override to
    the deferred ``_extract_working_copies`` pass. Without the filter the
    extractor would cache a working copy from the still-clean card bytes
    onto a photo whose catalog ``file_hash`` describes the mutated archive
    bytes instead — leaving preview/edit renders that don't match the
    archive contents even though the import was reported unsafe.
    """
    import scanner
    from import_job import ImportParams

    # RAW files (.NEF) are WC-extraction candidates regardless of size;
    # tiny JPEGs would be skipped by the working-copy candidate filter,
    # so RAWs are the smallest fixture that actually exercises the
    # extractor. Seed the RAW body with real JPEG bytes plus a trailing
    # tag so the two "RAW"s have distinct content.
    card = tmp_path / "card"
    card.mkdir()
    seed = card / "_seed.jpg"
    Image.new("RGB", (16, 16), "red").save(str(seed))
    seed_bytes = seed.read_bytes()
    seed.unlink()
    for name, mtime in (
        ("DSC_9100.NEF", datetime(2026, 7, 3, 10, 0, 0)),
        ("DSC_9101.NEF", datetime(2026, 7, 3, 11, 0, 0)),
    ):
        (card / name).write_bytes(seed_bytes + name.encode())
        ts = mtime.timestamp()
        os.utime(str(card / name), (ts, ts))

    archive = tmp_path / "archive"
    calls = []

    def fake_extract(source_path, output_path, max_size=4096, quality=92):
        calls.append(str(source_path))
        if not os.path.isfile(source_path):
            return False
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "wb") as f:
            f.write(b"jpeg-bytes")
        return True

    monkeypatch.setattr(scanner, "extract_working_copy", fake_extract)

    # Wrap ``scanner.scan`` so DSC_9101's archive bytes are mutated
    # after ``copy_and_hash_verify`` succeeded but BEFORE scan reads
    # them. scan() then hashes and records the mutated bytes; the
    # hash-stamping loop's mismatch branch reclassifies DSC_9101 from
    # ``copied`` to ``failed``. DSC_9100 stays untouched and lands
    # cleanly. ``run_import_job`` does ``from scanner import scan``
    # inside the function, so patching on the scanner module is picked
    # up on each invocation (mirrors ``test_dup_link_scan_failure_...``
    # above).
    real_scan = scanner.scan

    def scan_after_mutating(*args, **kwargs):
        for root, _dirs, files in os.walk(str(archive)):
            for name in files:
                if name == "DSC_9101.NEF":
                    with open(os.path.join(root, name), "r+b") as fh:
                        fh.write(b"MUTATED-ARCHIVE-BYTES")
        return real_scan(*args, **kwargs)

    monkeypatch.setattr(scanner, "scan", scan_after_mutating)

    vireo_dir = tmp_path / "vireo"
    (vireo_dir / "working").mkdir(parents=True)

    _db, _ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(archive),
        vireo_dir=str(vireo_dir),
    ))

    assert result["copied"] == 1, result
    assert result["failed"] == 1, result
    assert result["safe_to_format"] is False
    assert any("DSC_9101" in u["path"] for u in result["unsafe_files"]), (
        f"expected DSC_9101 in unsafe_files: {result['unsafe_files']!r}"
    )

    # DSC_9100 is a successful landing — it should still use its
    # card-side override (the whole point of source_paths). This anchors
    # the negative assertion below: the filter narrows to reclassified
    # entries and does not blanket-drop the override for the batch.
    card_reads_9100 = [
        c for c in calls if "DSC_9100" in c and str(card) in c
    ]
    assert card_reads_9100, (
        f"successful entry should have used card-side override; "
        f"got calls={calls}"
    )

    # DSC_9101 was reclassified: the extractor must NEVER be told to
    # read the still-clean card bytes. It may still read the mutated
    # archive path (that's fine — it matches whatever the catalog now
    # holds), but the card path is off-limits for this row.
    card_reads_9101 = [
        c for c in calls if "DSC_9101" in c and str(card) in c
    ]
    assert not card_reads_9101, (
        f"reclassified entry must not contribute a card override; "
        f"got calls={calls}"
    )


def test_copy_and_hash_verify_falls_back_when_hardlinks_unsupported(
        tmp_path, monkeypatch):
    """Destinations on FAT/exFAT and some SMB/NFS mounts reject os.link
    with EPERM/ENOTSUP. Without a fallback promotion path every file on
    those archives buckets as a copy failure; with the fallback the
    verified temp file lands via an atomic O_EXCL + os.replace and the
    copy succeeds.
    """
    import errno as errno_mod

    from import_dedup import compute_file_hash
    from import_job import copy_and_hash_verify

    src = tmp_path / "card" / "DSC_9500.jpg"
    src.parent.mkdir()
    src.write_bytes(b"card-file-bytes" * 100)
    dst = tmp_path / "archive" / "DSC_9500.jpg"

    def unsupported_link(a, b):
        raise OSError(errno_mod.EPERM, "operation not permitted")

    monkeypatch.setattr("import_job.os.link", unsupported_link)

    ok, file_hash = copy_and_hash_verify(str(src), str(dst))
    assert ok is True
    assert file_hash == compute_file_hash(str(src))
    assert dst.read_bytes() == src.read_bytes()
    # No stray .tmp / empty-placeholder residue.
    assert not list(dst.parent.glob(".DSC_9500.jpg.*.tmp"))


def test_copy_and_hash_verify_fallback_still_refuses_to_overwrite(
        tmp_path, monkeypatch):
    """The O_EXCL fallback must preserve no-overwrite race protection: an
    existing verified archive file must survive when os.link is not
    available, mirroring the primary os.link path's FileExistsError."""
    import errno as errno_mod

    from import_job import copy_and_hash_verify

    src = tmp_path / "card" / "DSC_9501.jpg"
    src.parent.mkdir()
    src.write_bytes(b"card bytes")

    dst = tmp_path / "archive" / "DSC_9501.jpg"
    dst.parent.mkdir()
    dst.write_bytes(b"already-verified archive bytes")

    def unsupported_link(a, b):
        raise OSError(errno_mod.EPERM, "operation not permitted")

    monkeypatch.setattr("import_job.os.link", unsupported_link)

    ok, file_hash = copy_and_hash_verify(str(src), str(dst))
    assert ok is False
    assert file_hash is None
    # The pre-existing verified copy must survive both the os.link race
    # AND the fallback path — the existence check fires and we never
    # touch dst.
    assert dst.read_bytes() == b"already-verified archive bytes"
    # And the temp file must be cleaned up.
    assert not list(dst.parent.glob(".DSC_9501.jpg.*.tmp"))


def test_copy_and_hash_verify_fallback_leaves_no_placeholder_at_dst_on_promote_failure(
        tmp_path, monkeypatch):
    """Crash-safety on hardlinkless destinations: if the promote step
    fails after the temp file has verified, the fallback path must NOT
    leave a zero-byte file at ``dst``. Otherwise the intended archive
    name is occupied by a stray empty file, retry treats it as an
    existing archive, suffixes the real photo to ``name_1.ext``, and
    the invariant that a dead run leaves only valid archive copies or
    hidden temps breaks. See PR #1107 review.
    """
    import errno as errno_mod

    from import_job import copy_and_hash_verify

    src = tmp_path / "card" / "DSC_9502.jpg"
    src.parent.mkdir()
    src.write_bytes(b"card-file-bytes" * 100)
    dst = tmp_path / "archive" / "DSC_9502.jpg"

    def unsupported_link(a, b):
        raise OSError(errno_mod.EPERM, "operation not permitted")

    def failing_rename(a, b):
        raise OSError(errno_mod.EIO, "simulated FS I/O error during promote")

    monkeypatch.setattr("import_job.os.link", unsupported_link)
    monkeypatch.setattr("import_job.os.rename", failing_rename)

    ok, file_hash = copy_and_hash_verify(str(src), str(dst))
    assert ok is False
    assert file_hash is None
    # No zero-byte placeholder at final dst — that was the specific
    # crash-recovery hole this fix closes.
    assert not dst.exists(), (
        f"fallback promote failure must not leave any file at {dst}; "
        f"a zero-byte stray would trip crash-recovery retries"
    )
    # And the hidden temp must be cleaned up too.
    assert not list(dst.parent.glob(".DSC_9502.jpg.*.tmp"))


def test_copy_and_hash_verify_fallback_serializes_via_directory_flock(
        tmp_path, monkeypatch):
    """The hardlinkless-FS fallback wraps its exists-check + rename in a
    ``fcntl.flock`` on the destination directory. Without this, two
    concurrent imports targeting the same date folder could both pass
    exists() before either rename(), and the later rename would silently
    overwrite the first job's already-verified archive copy — its
    ``safe_to_format`` would still report green after its bytes are
    gone. See PR #1107 review.
    """
    import errno as errno_mod

    from import_job import copy_and_hash_verify

    src = tmp_path / "card" / "DSC_9503.jpg"
    src.parent.mkdir()
    src.write_bytes(b"card bytes for lock test" * 50)
    dst = tmp_path / "archive" / "DSC_9503.jpg"

    def unsupported_link(a, b):
        raise OSError(errno_mod.EPERM, "operation not permitted")

    monkeypatch.setattr("import_job.os.link", unsupported_link)

    flock_calls = []

    real_flock = None
    try:
        import fcntl as fcntl_mod
        real_flock = fcntl_mod.flock

        def spy_flock(fd, op):
            flock_calls.append((fd, op))
            return real_flock(fd, op)

        monkeypatch.setattr("import_job.fcntl.flock", spy_flock)
    except ImportError:  # pragma: no cover - Windows
        pass

    ok, _ = copy_and_hash_verify(str(src), str(dst))
    assert ok is True
    # LOCK_EX must have been requested exactly once during the fallback
    # promote critical section — the exists+rename window is serialized.
    if real_flock is not None:
        assert len(flock_calls) == 1, (
            f"expected one flock(LOCK_EX) on fallback path; got {flock_calls}"
        )
        _, op = flock_calls[0]
        assert op == fcntl_mod.LOCK_EX


def test_landed_file_failed_after_scan_is_not_double_counted(
        tmp_path, monkeypatch):
    """A file that lands (copy verifies), then hits a scan/lookup failure
    must move out of copied/skipped_duplicate into failed — otherwise
    ``copied + skipped_duplicate + failed`` exceeds ``discovered`` and the
    exactly-one-terminal-bucket invariant breaks. Simulate a per-batch
    scan failure and check the counts sum to ``discovered``.
    """
    import scanner as scanner_mod
    from import_job import ImportParams, run_import_job

    card = _make_card(tmp_path, [
        ("DSC_1200.jpg", datetime(2026, 7, 6, 10, 0, 0), "red"),
        ("DSC_1201.jpg", datetime(2026, 7, 6, 11, 0, 0), "green"),
    ])
    archive = tmp_path / "archive"

    real_scan = scanner_mod.scan

    def failing_scan(root, db_arg, **kwargs):
        # Fail the restricted (per-batch) scan — the one with
        # restrict_files. The dup-link path (no restrict_files) isn't
        # exercised here; there are no duplicates.
        if kwargs.get("restrict_files"):
            raise OSError("simulated per-batch scan failure")
            # (Not RuntimeError — that's cancellation.)
        return real_scan(root, db_arg, **kwargs)

    monkeypatch.setattr(scanner_mod, "scan", failing_scan)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(sources=[str(card)], destination=str(archive)),
    )

    # The invariant: every discovered file ends in exactly one terminal
    # bucket. Before the fix, copied stayed at 2 and failed also went to
    # 2, giving a sum of 4 > 2 discovered.
    assert result["discovered"] == 2
    assert (
        result["copied"]
        + result["skipped_duplicate"]
        + result["failed"]
    ) == result["discovered"], (
        f"exactly-one-terminal-bucket violated: {result!r}"
    )
    assert result["failed"] == 2
    assert result["copied"] == 0
    assert result["safe_to_format"] is False

    # Folder counts must also be internally consistent.
    for _rel, counts in result["folders"].items():
        assert counts["copied"] >= 0
        assert counts["skipped_duplicate"] >= 0
        assert counts["failed"] >= 0
        assert (
            counts["copied"]
            + counts["skipped_duplicate"]
            + counts["failed"]
        ) == 2, f"folder count sum mismatch: {counts!r}"


def test_import_photos_rejects_case_variant_source_nested_destination(
        tmp_path, monkeypatch):
    """On case-insensitive filesystems (macOS APFS/HFS+, Windows NTFS)
    ``/Volumes/Card`` and ``/volumes/card`` refer to the same directory.
    The API guard against source-contained destinations must compare
    case-folded on those platforms; a naive prefix check would let a
    differently cased spelling slip past and hit the safe-to-format
    data-loss trap.
    """
    import sys

    from db import Database

    # Config isolation — same pattern as vireo/tests/test_app.py.
    monkeypatch.setenv("HOME", str(tmp_path))
    import config as cfg
    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))

    # Force the case-insensitive code path regardless of the test host.
    monkeypatch.setattr(sys, "platform", "darwin")

    db_path = str(tmp_path / "test.db")
    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir)
    d = Database(db_path)
    d.ensure_default_workspace()
    d.close()

    # Set up a plausibly cased source (real dir) and a differently cased
    # destination that resolves under it. We use case-preserving names on
    # the underlying filesystem; ``realpath`` won't rewrite the case on
    # Linux, so the case-fold comparison is what catches the containment.
    source = tmp_path / "Card"
    source.mkdir()
    dest_inside = str(source).replace("Card", "card") + "/archive"

    from app import create_app

    app = create_app(db_path=db_path, thumb_cache_dir=thumb_dir)
    with app.test_client() as client:
        resp = client.post(
            "/api/jobs/import-photos",
            json={
                "sources": [str(source)],
                "destination": dest_inside,
            },
        )

    assert resp.status_code == 400, resp.get_data(as_text=True)
    payload = resp.get_json()
    assert "inside a source" in (payload.get("error") or "")


def test_crash_recovered_suffix_is_adopted_not_re_copied(tmp_path):
    """When a prior run copied a different-content collision to
    ``DSC_XXX.jpg`` and this source's bytes to the suffixed ``DSC_XXX_1.jpg``
    then died before scan, a retry must hash-match every existing suffix
    candidate and adopt on a match — not advance past it and re-copy to
    ``DSC_XXX_2.jpg``. Without the hash-match, the archive would carry two
    byte-identical copies of the same source photo.
    """
    import shutil

    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0060.jpg", datetime(2026, 7, 3, 10, 0, 0), "yellow"),
    ])
    archive = tmp_path / "archive"
    dest_dir = archive / "2026" / "2026-07-03"
    dest_dir.mkdir(parents=True)

    # A different-content name-collision from an earlier run (some other
    # source photo happened to share the filename+date). Not this card's
    # bytes.
    Image.new("RGB", (16, 16), "blue").save(str(dest_dir / "DSC_0060.jpg"))
    # THIS card's bytes, landed at the suffixed name by a prior run that
    # died before its restricted scan.
    shutil.copy2(str(card / "DSC_0060.jpg"), str(dest_dir / "DSC_0060_1.jpg"))

    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(archive),
    ))

    # Adopted the crash-recovered suffix — no re-copy, no double.
    assert result["copied"] == 0
    assert result["skipped_duplicate"] == 1
    assert result["failed"] == 0
    assert result["safe_to_format"] is True

    # Only the two files that existed before + no DSC_0060_2.jpg.
    files_on_disk = sorted(os.listdir(str(dest_dir)))
    assert files_on_disk == ["DSC_0060.jpg", "DSC_0060_1.jpg"], files_on_disk

    # The adopted suffix is cataloged with hash_status=ok. (The pre-existing
    # ``DSC_0060.jpg`` with different bytes is a stray outside this import;
    # a future full scan would catalog it — out of scope for this run.)
    rows = _photo_rows(db)
    adopted = [r for r in rows if r["filename"] == "DSC_0060_1.jpg"]
    assert len(adopted) == 1
    assert adopted[0]["hash_status"] == "ok"


def test_paired_companion_archive_mutation_after_scan_reclassifies(
        tmp_path, monkeypatch):
    """The RAW+JPEG pair-merge in ``scanner.scan()`` deletes the JPEG's own
    photo row. Before this fix, the import job's hash-stamping loop
    accepted that as success without re-reading the archive JPEG. If the
    archive JPEG gets rewritten or corrupted between promote and the
    stamping check, ``safe_to_format`` could still go green over bytes we
    never verified. Simulate archive-side mutation of the paired JPEG and
    require the import to reclassify it to failed.
    """
    import scanner as scanner_mod
    from import_job import ImportParams, run_import_job

    # Card carries a RAW+JPEG pair sharing the base name. Same shape as
    # ``test_working_copy_companion_fallback_reads_card_jpeg``: the "RAW"
    # is opaque bytes with a .NEF extension (scanner sniffs by extension,
    # not content), so scan()'s pair-merge deletes the JPEG's photo row
    # and sets companion_path on the RAW primary.
    card = tmp_path / "card"
    card.mkdir()
    Image.new("RGB", (16, 16), "red").save(str(card / "DSC_2000.jpg"))
    raw_bytes = (card / "DSC_2000.jpg").read_bytes() + b"RAW-SENSOR-DATA"
    (card / "DSC_2000.NEF").write_bytes(raw_bytes)
    ts = datetime(2026, 7, 3, 10, 0, 0).timestamp()
    for name in ("DSC_2000.jpg", "DSC_2000.NEF"):
        os.utime(str(card / name), (ts, ts))
    archive = tmp_path / "archive"

    real_scan = scanner_mod.scan

    def mutating_scan(root, db_arg, **kwargs):
        result = real_scan(root, db_arg, **kwargs)
        # AFTER cataloging + pairing but before import_job's hash-stamping
        # loop runs, mutate the archive JPEG. If the fix works this
        # forces the JPEG entry into ``failed``; without it, the JPEG's
        # deleted-by-pair row makes the check silently pass.
        for f in kwargs.get("restrict_files") or set():
            if str(f).lower().endswith("dsc_2000.jpg") and os.path.exists(f):
                with open(f, "r+b") as fh:
                    fh.write(b"CORRUPT-MUTATION-POST-COPY")
        return result

    monkeypatch.setattr(scanner_mod, "scan", mutating_scan)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(sources=[str(card)], destination=str(archive)),
    )

    # The JPEG bytes on disk no longer match copy_and_hash_verify's
    # verified hash — the import must NOT report safe.
    assert result["safe_to_format"] is False
    assert result["failed"] >= 1
    # Terminal-bucket invariant still holds.
    assert (
        result["copied"]
        + result["skipped_duplicate"]
        + result["failed"]
    ) == result["discovered"]
    # The specific failure names the JPEG.
    unsafe_paths = [u["path"] for u in result["unsafe_files"]]
    assert any("DSC_2000.jpg" in p for p in unsafe_paths), unsafe_paths


def test_non_empty_null_scan_hash_reclassifies_when_rehash_disagrees(
        tmp_path, monkeypatch):
    """When ``scanner.scan()`` writes a photo row for a non-empty file but
    leaves ``file_hash`` NULL (its own hash read failed between promote
    and scan), the import job must NOT stamp the copy-time hash and call
    it verified. Re-hashing the archive path is the last check — if it
    also disagrees (file mutated or unreadable), the entry must be
    reclassified to failed. Simulate that shape and require the ledger
    to bucket the file as failed rather than reporting safe.
    """
    import scanner as scanner_mod
    from import_job import ImportParams, run_import_job

    card = _make_card(tmp_path, [
        ("DSC_3000.jpg", datetime(2026, 7, 3, 10, 0, 0), "purple"),
    ])
    archive = tmp_path / "archive"

    real_scan = scanner_mod.scan

    def sabotaging_scan(root, db_arg, **kwargs):
        result = real_scan(root, db_arg, **kwargs)
        # Wipe file_hash for freshly cataloged photos (simulating a
        # scan-side hash-read failure that landed a NULL) AND mutate the
        # archive so a re-hash also disagrees.
        if kwargs.get("restrict_files"):
            for f in kwargs["restrict_files"]:
                if not os.path.exists(f):
                    continue
                with open(f, "r+b") as fh:
                    fh.write(b"POST-SCAN-MUTATION-NULL-HASH")
                db_arg.conn.execute(
                    """UPDATE photos SET file_hash = NULL
                       WHERE filename = ?""",
                    (os.path.basename(f),),
                )
                db_arg.conn.commit()
        return result

    monkeypatch.setattr(scanner_mod, "scan", sabotaging_scan)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(sources=[str(card)], destination=str(archive)),
    )

    assert result["safe_to_format"] is False
    assert result["failed"] == 1
    assert result["copied"] == 0
    assert (
        result["copied"]
        + result["skipped_duplicate"]
        + result["failed"]
    ) == result["discovered"]
    unsafe_paths = [u["path"] for u in result["unsafe_files"]]
    assert any("DSC_3000.jpg" in p for p in unsafe_paths), unsafe_paths


def test_dest_file_nested_under_source_is_rejected(tmp_path):
    """When the destination is a legal ancestor of a source but the folder
    template maps the source right back INTO the source tree, ``dest_file``
    is a different path than the source file (samefile is False) but still
    lives under the card. Copying there is counted as ``copied``,
    ``safe_to_format`` can go green, and formatting the card also erases
    the "archive" copy. The import job must reject that overlap even
    though the two paths are not the same file. See PR #1107 review.
    """
    from import_job import ImportParams

    # Source is /volumes/Card/DCIM (with photos directly in it); the
    # destination is /volumes/Card and the folder template ``DCIM/Archive/
    # %Y`` maps the source back into itself: dest_file lives at
    # /volumes/Card/DCIM/Archive/2026/<name>, which is under the source
    # but is not the source file.
    card = tmp_path / "volumes" / "Card"
    dcim = card / "DCIM"
    dcim.mkdir(parents=True)
    src_file = dcim / "DSC_5000.jpg"
    Image.new("RGB", (16, 16), "goldenrod").save(str(src_file))
    ts = datetime(2026, 7, 5, 10, 0, 0).timestamp()
    os.utime(str(src_file), (ts, ts))
    original_bytes = src_file.read_bytes()

    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(dcim)],
        destination=str(card),
        folder_template="DCIM/Archive/%Y",
    ))

    # The source bytes MUST still be on disk.
    assert src_file.exists()
    assert src_file.read_bytes() == original_bytes
    # The nested "archive" copy MUST NOT have been created.
    assert not (dcim / "Archive" / "2026" / "DSC_5000.jpg").exists()
    # It must NOT be counted as copied/skipped; it must be failed.
    assert result["copied"] == 0
    assert result["skipped_duplicate"] == 0
    assert result["failed"] == 1
    # Safe-to-format MUST NOT go green when the archive would live on
    # the card being imported.
    assert result["safe_to_format"] is False
    assert (
        result["copied"]
        + result["skipped_duplicate"]
        + result["failed"]
    ) == result["discovered"]
    unsafe_paths = [u["path"] for u in result["unsafe_files"]]
    assert any("DSC_5000.jpg" in p for p in unsafe_paths), unsafe_paths


def test_batch_destination_under_source_creates_no_directories(tmp_path):
    """When ``dest_folder`` for a batch resolves under a source root, the
    per-file loop rejects each ``dest_file`` — but that check only runs
    AFTER ``os.makedirs(dest_folder)``, which would still materialize the
    archive directory tree on the card (and raise on read-only removable
    media, killing the background job with an uncaught OSError instead of
    returning a controlled unsafe result). The import job must short-circuit
    at the batch boundary: nothing under any source ever gets created, and
    the run returns the same controlled ``failed``/``safe_to_format=False``
    ledger it would for a writable destination inside the card. See PR
    #1107 review.
    """
    from import_job import ImportParams

    # Source ``/volumes/Card/DCIM``; destination ``/volumes/Card`` +
    # template ``DCIM/Archive/%Y`` puts the batch's dest_folder at
    # ``/volumes/Card/DCIM/Archive/2026`` — under the source root.
    card = tmp_path / "volumes" / "Card"
    dcim = card / "DCIM"
    dcim.mkdir(parents=True)
    src_file = dcim / "DSC_6100.jpg"
    Image.new("RGB", (16, 16), "olive").save(str(src_file))
    ts = datetime(2026, 7, 5, 10, 0, 0).timestamp()
    os.utime(str(src_file), (ts, ts))

    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(dcim)],
        destination=str(card),
        folder_template="DCIM/Archive/%Y",
    ))

    # No batch directory was created on the card.
    assert not (dcim / "Archive").exists()
    # Same controlled unsafe result as the per-file case.
    assert result["failed"] == 1
    assert result["copied"] == 0
    assert result["skipped_duplicate"] == 0
    assert result["safe_to_format"] is False
    assert result["ok"] is False
    unsafe_paths = [u["path"] for u in result["unsafe_files"]]
    assert any("DSC_6100.jpg" in p for p in unsafe_paths), unsafe_paths


def test_full_coverage_file_types_list_is_treated_as_unfiltered(tmp_path):
    """The pipeline UI's ``getIngestFileTypes()`` returns a list of every
    supported extension when the user checks every box. Semantically
    that is the same as ``file_types="both"``: ``discover_source_files``
    walks it identically. Flagging any list as ``partial_scope``
    therefore leaves ``safe_to_format`` permanently false over an
    unfiltered import even though every card file was verified — the
    pill would deceive the user, ``COPY_PHILOSOPHY.md``'s "show the
    user what's happening" contract. Normalize full-coverage lists to
    the same status as ``"both"``. See PR #1107 review.
    """
    from image_loader import SUPPORTED_EXTENSIONS
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0500.jpg", datetime(2026, 7, 3, 10, 0, 0), "coral"),
    ])
    archive = tmp_path / "archive"

    # A list covering every supported extension — what the UI sends when
    # the user checks every filetype box, including some casing variance
    # to guarantee the normalization path is exercised.
    full_list = sorted(SUPPORTED_EXTENSIONS)
    full_list[0] = full_list[0].upper()  # e.g. ".ARW"
    full_list.append("jpg")              # no-leading-dot alias for coverage

    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)],
        destination=str(archive),
        file_types=full_list,
    ))

    assert result["copied"] == 1
    assert result["failed"] == 0
    # Full-coverage list did NOT actually narrow the walk, so
    # safe_to_format may go green just like ``"both"``.
    assert result["safe_to_format"] is True


def test_source_equals_dest_file_is_rejected(tmp_path):
    """When a source lives under the destination and the folder template
    maps it back to the same directory, dest_file resolves to the source
    file itself. The adopt branch would hash the file against itself and
    count it as ``skipped_duplicate`` with safe_to_format=True — then
    formatting/erasing the source erases the only copy. The import job
    must reject that overlap at the worker level (the API rejects
    ``destination inside source`` but not the reverse). See PR #1107 review.
    """
    from import_job import ImportParams

    # Source is /archive/2026/2026-07-05; destination is /archive with the
    # default %Y/%Y-%m-%d template → dest_folder becomes 2026/2026-07-05,
    # so dest_file IS the source file.
    archive = tmp_path / "archive"
    day = archive / "2026" / "2026-07-05"
    day.mkdir(parents=True)
    from PIL import Image
    src_file = day / "DSC_4000.jpg"
    Image.new("RGB", (16, 16), "coral").save(str(src_file))
    ts = datetime(2026, 7, 5, 10, 0, 0).timestamp()
    os.utime(str(src_file), (ts, ts))
    original_bytes = src_file.read_bytes()

    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(day)], destination=str(archive),
    ))

    # The source bytes MUST still be on disk (nothing was moved/deleted).
    assert src_file.exists()
    assert src_file.read_bytes() == original_bytes
    # It must NOT be counted as skipped_duplicate; it must be failed.
    assert result["skipped_duplicate"] == 0
    assert result["failed"] == 1
    assert result["copied"] == 0
    # Safe-to-format must NOT go green when the source == dest.
    assert result["safe_to_format"] is False
    assert (
        result["copied"]
        + result["skipped_duplicate"]
        + result["failed"]
    ) == result["discovered"]
    # The failure specifically names the file.
    unsafe_paths = [u["path"] for u in result["unsafe_files"]]
    assert any("DSC_4000.jpg" in p for p in unsafe_paths), unsafe_paths


def test_import_invalidates_new_images_cache(tmp_path):
    """Per-batch scans and duplicate-folder links must invalidate the
    /new-images cache for the touched destination folders. Otherwise
    a workspace whose cache was warm before the import keeps reporting
    the just-imported files as new until TTL expires or another full
    scan runs. Mirrors the try/finally in api_job_scan / api_job_import_full
    / pipeline_job. See PR #1107 review.
    """
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_5000.jpg", datetime(2026, 7, 3, 10, 0, 0), "teal"),
    ])
    archive = tmp_path / "archive"

    # Prime the cache with a sentinel value for the active workspace so
    # we can observe invalidation without racing an actual /new-images
    # walk. If run_import_job invalidates correctly, the sentinel is
    # gone by the time the job returns.
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    db._new_images_cache.set(
        db_path, ws_id, {"new_count": 999, "sample": []},
    )
    assert db._new_images_cache.get(db_path, ws_id) is not None

    from import_job import run_import_job
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(sources=[str(card)], destination=str(archive)),
    )
    assert result["copied"] == 1  # sanity: the import actually ran

    # The restricted scan invalidation must have cleared the sentinel for
    # the workspace linked to the dest_folder.
    assert db._new_images_cache.get(db_path, ws_id) is None, (
        "run_import_job must invalidate the new-images cache for the "
        "workspace linked to the destination folder after its restricted "
        "scans (mirrors pipeline_job / api_job_scan / api_job_import_full)"
    )


def test_zero_byte_destination_collision_is_adopted_and_cataloged(tmp_path):
    """A crash-recovery zero-byte destination has no hash-index identity.

    It must enter the exact-file landed scan instead of the cataloged-twin
    link path, because the previous run may have died before creating even
    the destination folder row.
    """
    from import_job import ImportParams, run_import_job

    card = tmp_path / "card"
    card.mkdir()
    source_file = card / "EMPTY.jpg"
    source_file.touch()
    timestamp = datetime(2026, 7, 3, 10, 0, 0).timestamp()
    os.utime(str(source_file), (timestamp, timestamp))

    archive = tmp_path / "archive"
    dest_dir = archive / "2026" / "2026-07-03"
    dest_dir.mkdir(parents=True)
    dest_file = dest_dir / source_file.name
    dest_file.touch()
    os.utime(str(dest_file), (timestamp, timestamp))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    assert db.conn.execute(
        "SELECT id FROM folders WHERE path = ?", (str(dest_dir),),
    ).fetchone() is None

    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(sources=[str(card)], destination=str(archive)),
    )

    assert result["copied"] == 0, result
    assert result["skipped_duplicate"] == 1, result
    assert result["failed"] == 0, result
    assert result["safe_to_format"] is True, result
    row = db.conn.execute(
        """SELECT p.id, p.hash_status FROM photos p
           JOIN folders f ON f.id = p.folder_id
           WHERE f.path = ? AND p.filename = ?""",
        (str(dest_dir), source_file.name),
    ).fetchone()
    assert row is not None
    assert row["hash_status"] == "ok"
    assert str(dest_dir) in _ws_linked_folder_paths(db, ws_id)


def test_import_promotes_missing_destination_folder_to_ok(tmp_path):
    """A pre-existing ``folders`` row marked ``'missing'`` for the import's
    destination path must transition back to ``'ok'`` before the batch
    scan runs — otherwise workspace queries filter the folder out and the
    just-imported files never appear in the workspace, even though
    safe_to_format goes green.

    Standalone scans run ``check_folder_health()`` as their preflight,
    which handles this globally. The import path calls ``scanner.scan()``
    directly, and scan's success stamp only clears ``'partial'``. So a
    reattached archive drive whose folder rows still say ``'missing'``
    stays invisible after a successful import unless the import job
    itself promotes the row. See PR #1107 review.
    """
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0500.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])
    archive = tmp_path / "archive"
    dest_dir = archive / "2026" / "2026-07-03"
    dest_dir.mkdir(parents=True)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    # Pre-existing missing row for the same path that this import will
    # populate (simulates: archive drive was disconnected during a health
    # check and got reattached before this import).
    db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'missing')",
        (str(dest_dir), dest_dir.name),
    )
    db.conn.commit()

    from import_job import run_import_job
    result = run_import_job(_make_job(), FakeRunner(), db_path, ws_id,
                            ImportParams(sources=[str(card)],
                                         destination=str(archive)))
    assert result["copied"] == 1
    assert result["safe_to_format"] is True

    status = db.conn.execute(
        "SELECT status FROM folders WHERE path = ?", (str(dest_dir),),
    ).fetchone()["status"]
    assert status == "ok", (
        "import must promote pre-existing missing folder row to 'ok' so "
        "the imported files are visible in the workspace"
    )


def test_import_missing_promotion_narrow_status_guard(tmp_path):
    """The missing→ok promotion must be gated on ``status='missing'`` so it
    can't clobber other statuses. (``'partial'`` gets cleared by
    scanner's success stamp anyway, but the pre-scan targeted UPDATE
    must not overreach — that's what the ``AND status = 'missing'``
    guard is for.)"""
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0600.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])
    archive = tmp_path / "archive"
    dest_dir = archive / "2026" / "2026-07-03"
    dest_dir.mkdir(parents=True)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # A sibling folder marked 'missing' whose path is NOT this import's
    # destination must NOT be promoted — the pre-scan UPDATE is targeted
    # by path, so unrelated rows stay untouched.
    other_dir = archive / "2025" / "2025-01-01"
    db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'missing')",
        (str(other_dir), other_dir.name),
    )
    db.conn.commit()

    from import_job import run_import_job
    run_import_job(_make_job(), FakeRunner(), db_path, ws_id,
                   ImportParams(sources=[str(card)], destination=str(archive)))

    status = db.conn.execute(
        "SELECT status FROM folders WHERE path = ?", (str(other_dir),),
    ).fetchone()["status"]
    assert status == "missing", (
        "the missing→ok promotion must be scoped to this batch's dest_folder"
    )


def test_duplicate_only_import_promotes_missing_twin_folder(tmp_path):
    """A duplicate-only import that matches a cataloged twin whose folder
    row is stale-marked ``'missing'`` (but the folder is still on disk
    under the import destination) must promote that folder to ``'ok'``
    as part of its direct link — otherwise the archive stays filtered
    out of workspace queries even though safe_to_format goes green.
    See PR #1107 review.
    """
    from import_dedup import compute_file_hash
    from import_job import ImportParams

    archive = tmp_path / "archive"
    twin_dir = archive / "2026" / "2026-07-03"
    twin_dir.mkdir(parents=True)
    twin_file = twin_dir / "IMG_0300.jpg"
    Image.new("RGB", (16, 16), "red").save(str(twin_file))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    # Pre-catalog the twin at MISSING status (simulates a health check
    # that flipped the folder to missing right before a reattach).
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'missing')",
        (str(twin_dir), twin_dir.name),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_hash) VALUES (?, ?, '.jpg', ?, ?)",
        (
            fid,
            "IMG_0300.jpg",
            os.path.getsize(str(twin_file)),
            compute_file_hash(str(twin_file)),
        ),
    )
    db.conn.commit()

    card = tmp_path / "card"
    card.mkdir()
    import shutil
    shutil.copy2(str(twin_file), str(card / "IMG_0300.jpg"))

    from import_job import run_import_job
    result = run_import_job(_make_job(), FakeRunner(), db_path, ws_id,
                            ImportParams(sources=[str(card)],
                                         destination=str(archive)))
    assert result["copied"] == 0
    assert result["skipped_duplicate"] == 1
    assert result["failed"] == 0
    assert result["safe_to_format"] is True

    # Folder status was promoted so workspace queries can see it.
    status = db.conn.execute(
        "SELECT status FROM folders WHERE path = ?", (str(twin_dir),),
    ).fetchone()["status"]
    assert status == "ok", (
        "duplicate-only import must promote a matched 'missing'-marked "
        "twin folder to 'ok' as part of its direct workspace link"
    )
    # And the folder is linked to the active workspace.
    assert str(twin_dir) in _ws_linked_folder_paths(db, ws_id)


def test_duplicate_only_import_links_twin_folder_when_destination_is_symlink(tmp_path):
    """A duplicate-only import whose ``destination`` is a symlink to the
    twin's on-disk archive root must still resolve containment through
    the link and link the twin folder. A lexical prefix check would drop
    the twin from ``dup_dirs``, the direct workspace link would never run,
    and the imported duplicate would stay filtered out of the active
    workspace even though safe_to_format flipped green. See PR #1107
    review.
    """
    import pytest
    from import_dedup import compute_file_hash
    from import_job import ImportParams, run_import_job

    real_archive = tmp_path / "real_archive"
    twin_dir = real_archive / "2026" / "2026-07-03"
    twin_dir.mkdir(parents=True)
    twin_file = twin_dir / "IMG_0400.jpg"
    Image.new("RGB", (16, 16), "red").save(str(twin_file))

    # Destination the user hands to the import is a symlink to the real
    # archive root. The twin folder was cataloged under its real path.
    alias_archive = tmp_path / "archive-alias"
    try:
        os.symlink(str(real_archive), str(alias_archive), target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("symlink creation not supported on this platform")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (str(twin_dir), twin_dir.name),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_hash) VALUES (?, ?, '.jpg', ?, ?)",
        (
            fid,
            "IMG_0400.jpg",
            os.path.getsize(str(twin_file)),
            compute_file_hash(str(twin_file)),
        ),
    )
    db.conn.commit()
    # Nothing linked before the run: proves the run must do the linking.
    assert str(twin_dir) not in _ws_linked_folder_paths(db, ws_id)

    card = tmp_path / "card"
    card.mkdir()
    import shutil
    shutil.copy2(str(twin_file), str(card / "IMG_0400.jpg"))

    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(sources=[str(card)], destination=str(alias_archive)),
    )

    assert result["copied"] == 0
    assert result["skipped_duplicate"] == 1
    assert result["failed"] == 0
    assert result["safe_to_format"] is True
    # The twin folder was scanned + linked despite the destination being
    # a symlink to (not literally equal to) the twin's cataloged root.
    assert str(twin_dir) in _ws_linked_folder_paths(db, ws_id)


def test_import_invalidates_derived_caches_on_content_change(tmp_path):
    """When a landed file replaces bytes at a path whose catalog row already
    has ``working_copy_path`` set (from a prior scan of an older archive
    file at the same path), the import must invalidate that WC — the
    deferred end-of-run ``_extract_working_copies`` skips rows with
    ``working_copy_path IS NOT NULL``, so without invalidation the WC
    persists pointing at bytes the archive no longer holds. See PR #1107
    review.
    """
    from import_dedup import compute_file_hash
    from import_job import ImportParams

    archive = tmp_path / "archive"
    dest_dir = archive / "2026" / "2026-07-03"
    dest_dir.mkdir(parents=True)
    # A stale archive file present before the import; its catalog row
    # captures its OLD hash + a fake WC path (as if a prior scan
    # extracted a WC for it).
    stale_archive = dest_dir / "DSC_0700.jpg"
    Image.new("RGB", (16, 16), "blue").save(str(stale_archive))
    stale_hash = compute_file_hash(str(stale_archive))

    vireo_dir = tmp_path / "vireo_data"
    (vireo_dir / "working").mkdir(parents=True)
    fake_wc = vireo_dir / "working" / "1.jpg"
    Image.new("RGB", (8, 8), "yellow").save(str(fake_wc))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (str(dest_dir), dest_dir.name),
    ).lastrowid
    photo_id = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_hash, working_copy_path) VALUES (?, ?, '.jpg', ?, ?, ?)",
        (
            fid,
            "DSC_0700.jpg",
            os.path.getsize(str(stale_archive)),
            stale_hash,
            str(fake_wc),
        ),
    ).lastrowid
    db.conn.commit()

    # Orphan preview file: previews/{pid}_{size}.jpg with NO
    # preview_cache row (legacy code path / interrupted insert).
    # Row-driven ``_invalidate_derived_caches`` can't see it; an
    # untracked-preview sweep removes it before it can be lazy-adopted
    # and served as stale pre-change bytes. In this content-change
    # geometry EITHER sweep suffices — scan()'s own internal sweep also
    # covers it — so this asserts "some sweep ran"; the pairing tests
    # isolate the import job's sweep call with a geometry scan()'s
    # sweep set never contains.
    previews_dir = vireo_dir / "previews"
    previews_dir.mkdir()
    orphan_preview = previews_dir / f"{photo_id}_512.jpg"
    orphan_preview.write_bytes(b"stale-orphan-preview-bytes")

    # Overwrite the archive file with DIFFERENT bytes (simulates: the
    # archive file was deleted/replaced between the prior scan and this
    # import, and the import restores the same filename with new bytes).
    stale_archive.unlink()

    # Card holds the NEW bytes at the same filename/date, which will land
    # at the same dest_path.
    card = _make_card(tmp_path, [
        ("DSC_0700.jpg", datetime(2026, 7, 3, 10, 0, 0), "green"),
    ])
    # Force skip_duplicates False so the card's new bytes actually get
    # copied over even though the stale row's hash still exists.
    from import_job import run_import_job
    result = run_import_job(_make_job(), FakeRunner(), db_path, ws_id,
                            ImportParams(sources=[str(card)],
                                         destination=str(archive),
                                         skip_duplicates=False,
                                         vireo_dir=str(vireo_dir)))
    assert result["copied"] == 1
    assert result["failed"] == 0

    # The row's WC path was cleared so the deferred extractor / later
    # backfill can rebuild against the new archive bytes.
    row = db.conn.execute(
        "SELECT working_copy_path FROM photos WHERE id = ?", (photo_id,),
    ).fetchone()
    assert row["working_copy_path"] is None, (
        "content change on a landed row must clear working_copy_path so "
        "the deferred WC pass rebuilds it against the new archive bytes"
    )
    # The stale WC file was also unlinked from disk.
    assert not fake_wc.exists(), (
        "content change on a landed row must delete the stale WC file"
    )
    # The orphan preview (no preview_cache row) was removed by the
    # untracked-preview sweep.
    assert not orphan_preview.exists(), (
        "untracked-preview sweep must remove orphan preview files for "
        "invalidated photos"
    )


def test_import_invalidates_derived_caches_when_pre_row_had_null_hash(tmp_path):
    """Legacy row invariant: a pre-scan row with ``file_hash IS NULL`` can
    still carry ``working_copy_path``/thumb/preview caches from earlier
    processing (e.g. a prior scan that couldn't read the file cleared the
    hash but left derived rows). Scanner's own content-change path treats
    ``NULL -> concrete hash`` as an invalidating transition; the import
    per-batch invalidation loop must mirror that, or restoring a deleted
    archive file at that path leaves stale WC/thumb bytes cached against
    the fresh hash. See PR #1107 review.
    """
    from import_job import ImportParams, run_import_job

    archive = tmp_path / "archive"
    dest_dir = archive / "2026" / "2026-07-03"
    dest_dir.mkdir(parents=True)

    vireo_dir = tmp_path / "vireo_data"
    (vireo_dir / "working").mkdir(parents=True)
    fake_wc = vireo_dir / "working" / "1.jpg"
    Image.new("RGB", (8, 8), "yellow").save(str(fake_wc))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (str(dest_dir), dest_dir.name),
    ).lastrowid
    # Legacy-shaped row: no file on disk, ``file_hash IS NULL``, but a
    # stale ``working_copy_path`` from an earlier processing pass.
    photo_id = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_hash, working_copy_path) VALUES (?, ?, '.jpg', ?, NULL, ?)",
        (fid, "DSC_0701.jpg", 12345, str(fake_wc)),
    ).lastrowid
    db.conn.commit()

    # Card holds the NEW bytes at the same filename/date — the import
    # will land them at the archive path whose row currently has
    # ``file_hash IS NULL`` + a stale WC.
    card = _make_card(tmp_path, [
        ("DSC_0701.jpg", datetime(2026, 7, 3, 10, 0, 0), "green"),
    ])
    result = run_import_job(_make_job(), FakeRunner(), db_path, ws_id,
                            ImportParams(sources=[str(card)],
                                         destination=str(archive),
                                         skip_duplicates=False,
                                         vireo_dir=str(vireo_dir)))
    assert result["copied"] == 1
    assert result["failed"] == 0

    # The row's WC path was cleared: NULL -> concrete hash is a real
    # content change, and the deferred extractor / later backfill must be
    # allowed to rebuild the WC against the just-imported archive bytes.
    row = db.conn.execute(
        "SELECT working_copy_path FROM photos WHERE id = ?", (photo_id,),
    ).fetchone()
    assert row["working_copy_path"] is None, (
        "NULL-hash pre-scan row must invalidate its stale derived caches "
        "when the import stamps a concrete hash (mirrors scanner.scan()'s "
        "content-change path)"
    )
    assert not fake_wc.exists(), (
        "NULL-hash pre-scan row must have its stale WC file unlinked"
    )


def test_import_invalidates_raw_caches_when_new_jpeg_pairs(tmp_path):
    """RAW+JPEG companion restore: when a freshly copied JPEG lands as
    companion to an existing RAW row (pair-merge deletes the JPEG's own
    row), the RAW row's derived caches may reflect the pre-pair state
    (RAW-only preview, or a deleted/replaced prior companion). The
    hash-stamping loop treats ``row is None`` as a fresh insert with no
    diff to invalidate, so without an explicit companion-invalidation
    pass the deferred WC pass skips the RAW (working_copy_path is set)
    and the UI keeps serving stale derived files. See PR #1107 review.
    """
    from import_job import ImportParams, run_import_job

    archive = tmp_path / "archive"
    dest_dir = archive / "2026" / "2026-07-03"
    dest_dir.mkdir(parents=True)

    vireo_dir = tmp_path / "vireo_data"
    (vireo_dir / "working").mkdir(parents=True)

    # Pre-existing RAW file at the archive path, cataloged standalone
    # with a stale working_copy_path from a prior RAW-only extraction.
    raw_archive = dest_dir / "DSC_0800.NEF"
    Image.new("RGB", (16, 16), "red").save(str(dest_dir / "_seed.jpg"))
    raw_bytes = (dest_dir / "_seed.jpg").read_bytes() + b"RAW-SENSOR-DATA"
    raw_archive.write_bytes(raw_bytes)
    (dest_dir / "_seed.jpg").unlink()

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (str(dest_dir), dest_dir.name),
    ).lastrowid
    # WC file must live at working/{photo_id}.jpg — that's the layout
    # _invalidate_derived_caches unlinks.
    raw_photo_id = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_hash, working_copy_path) VALUES (?, ?, '.nef', ?, ?, 'placeholder')",
        (fid, "DSC_0800.NEF", len(raw_bytes),
         "deadbeef" * 8),
    ).lastrowid
    fake_wc = vireo_dir / "working" / f"{raw_photo_id}.jpg"
    Image.new("RGB", (8, 8), "orange").save(str(fake_wc))
    stale_wc_bytes = fake_wc.read_bytes()
    db.conn.execute(
        "UPDATE photos SET working_copy_path = ? WHERE id = ?",
        (str(fake_wc), raw_photo_id),
    )
    db.conn.commit()

    # Orphan preview file for the RAW: previews/{raw_photo_id}_512.jpg
    # with NO preview_cache row. In this companion-pair geometry the
    # RAW's id never enters scan()'s own internal sweep set (pairing is
    # not a content change), so ONLY the import job's post-batch
    # ``_sweep_untracked_previews_for_photos`` call can remove it —
    # this seed isolates that call site.
    previews_dir = vireo_dir / "previews"
    previews_dir.mkdir()
    orphan_preview = previews_dir / f"{raw_photo_id}_512.jpg"
    orphan_preview.write_bytes(b"stale-orphan-preview-bytes")

    # Card holds a NEW JPEG that will land at DSC_0800.jpg and pair with
    # the existing RAW during the batch scan.
    card = _make_card(tmp_path, [
        ("DSC_0800.jpg", datetime(2026, 7, 3, 10, 0, 0), "green"),
    ])

    result = run_import_job(_make_job(), FakeRunner(), db_path, ws_id,
                            ImportParams(sources=[str(card)],
                                         destination=str(archive),
                                         skip_duplicates=False,
                                         vireo_dir=str(vireo_dir)))
    assert result["copied"] == 1
    assert result["failed"] == 0

    # The RAW row's stale WC path was cleared (invalidation ran) and
    # the on-disk stale WC file was unlinked. The deferred end-of-run
    # ``_extract_working_copies`` then either succeeds with a fresh WC
    # (path differs from the stale one) or leaves working_copy_path
    # NULL for the scanner's later backfill; either way the row no
    # longer points at the pre-pair bytes.
    row = db.conn.execute(
        "SELECT working_copy_path, companion_path FROM photos WHERE id = ?",
        (raw_photo_id,),
    ).fetchone()
    assert row["companion_path"] == "DSC_0800.jpg", (
        "pair-merge must record the newly landed JPEG as the RAW's "
        "companion_path"
    )
    # If invalidation didn't run the row would still point at the
    # pre-pair WC path (which the extractor's candidate predicate would
    # then skip, since working_copy_path is set). Invalidation resets
    # the path, and the deferred WC pass rebuilds fresh: even when the
    # extractor happens to reuse the same on-disk slot
    # (``working/{id}.jpg``), the bytes at that path must differ from
    # the stale orange placeholder we seeded, because the WC now comes
    # from the just-verified companion JPEG.
    if fake_wc.exists():
        assert fake_wc.read_bytes() != stale_wc_bytes, (
            "RAW's stale WC bytes must not survive the import — either "
            "the file is unlinked or overwritten with a fresh WC from "
            "the verified companion JPEG"
        )
    # The RAW's orphan preview (no preview_cache row) is gone. Scan()'s
    # internal sweep never sees companion-paired RAW ids, so only the
    # import job's own sweep call can have removed it.
    assert not orphan_preview.exists(), (
        "the import-path untracked-preview sweep must remove the paired "
        "RAW's orphan preview files"
    )


def test_key_duplicate_links_only_byte_verified_twin_folder(tmp_path):
    """Metadata-only ('key') duplicate: ``_key_twin_rows`` returns every
    catalog row sharing filename+size+capture-second, but only ONE of
    them may hold the card's actual bytes. The others are key-collisions
    with unrelated content (say, a burst frame with the same DateTime).

    Only the twin whose bytes we hashed and matched is a proven
    duplicate; linking the other key-collision folders would pull
    unrelated archive photos into the active workspace on a
    duplicate-only import. See PR #1107 review.
    """
    from import_job import ImportParams, run_import_job
    from PIL.ExifTags import Base as ExifBase

    dt = datetime(2026, 6, 15, 9, 45, 30)

    # Card file: red bytes, EXIF-timed so the checker generates a
    # trustworthy metadata key.
    card = tmp_path / "card"
    card.mkdir()
    card_file = card / "IMG_1200.jpg"
    img = Image.new("RGB", (16, 16), "red")
    exif = img.getexif()
    exif[ExifBase.DateTimeOriginal] = dt.strftime("%Y:%m:%d %H:%M:%S")
    img.save(str(card_file), exif=exif)
    card_bytes = card_file.read_bytes()

    # Two archive folders both containing IMG_1200.jpg — same filename,
    # same size, same trusted capture time — so both rows produce the
    # same metadata key and both appear in ``_key_twin_rows``.
    archive = tmp_path / "archive"
    archive.mkdir()
    verified_dir = archive / "verified-twin"
    verified_dir.mkdir()
    verified_file = verified_dir / "IMG_1200.jpg"
    # Twin A: SAME bytes as card — the real duplicate.
    verified_file.write_bytes(card_bytes)

    collision_dir = archive / "key-collision"
    collision_dir.mkdir()
    collision_file = collision_dir / "IMG_1200.jpg"
    # Twin B: same size, same key — DIFFERENT bytes.
    collision_bytes = card_bytes[:-1] + bytes([card_bytes[-1] ^ 0xFF])
    assert len(collision_bytes) == len(card_bytes)
    assert collision_bytes != card_bytes
    collision_file.write_bytes(collision_bytes)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    # Catalog both twins with file_hash=NULL — this forces
    # DuplicateChecker to return a ('key', …) token (not 'hash') so
    # we exercise the metadata-only branch. The timestamp is
    # trustworthy so both rows produce a matching key.
    for folder_dir, file_path in (
        (verified_dir, verified_file),
        (collision_dir, collision_file),
    ):
        fid = db.conn.execute(
            "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
            (str(folder_dir), folder_dir.name),
        ).lastrowid
        db.conn.execute(
            "INSERT INTO photos (folder_id, filename, extension, file_size,"
            " timestamp) VALUES (?, ?, '.jpg', ?, ?)",
            (
                fid, "IMG_1200.jpg",
                os.path.getsize(str(file_path)),
                dt.strftime("%Y-%m-%dT%H:%M:%S"),
            ),
        )
    db.conn.commit()

    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id, ImportParams(
            sources=[str(card)], destination=str(archive),
        ),
    )

    # The card was byte-identical to the verified twin, so it's a
    # legitimate skip.
    assert result["copied"] == 0
    assert result["skipped_duplicate"] == 1
    assert result["failed"] == 0
    assert result["safe_to_format"] is True

    ws_folder_is_root = {
        row["path"]: row["is_root"]
        for row in db.conn.execute(
            "SELECT f.path, wf.is_root FROM workspace_folders wf "
            "JOIN folders f ON f.id = wf.folder_id "
            "WHERE wf.workspace_id = ?",
            (ws_id,),
        )
    }
    # The byte-verified twin's folder is a user-facing workspace root
    # (``is_root=1``) — the import proved the archive holds the card's
    # bytes and links its folder into the workspace UI.
    assert ws_folder_is_root.get(str(verified_dir)) == 1
    # The key-collision folder was NEVER byte-verified against the
    # card. Before the verified-row filter, it could be passed to folder
    # linking and workspace-linked as its own top-level root
    # (``is_root=1``), pulling an unrelated archive folder into the
    # workspace UI. It must NOT surface as a workspace root here, and
    # after the restricted-scan cascade fix (PR #1107, line 1186) it
    # must not appear in ``workspace_folders`` at all.
    assert str(collision_dir) not in ws_folder_is_root

    # Neither folder was scanned. The verified folder is linked because the
    # duplicate gate compared its bytes directly for this run; that proof
    # does not turn archive repair into an import side effect. The unrelated
    # collision remains both unlinked and untouched.
    photo_hashes = {
        row["folder_path"]: row["file_hash"]
        for row in db.conn.execute(
            "SELECT p.file_hash, f.path AS folder_path "
            "FROM photos p JOIN folders f ON f.id = p.folder_id"
        )
    }
    assert photo_hashes.get(str(verified_dir)) is None
    assert photo_hashes.get(str(collision_dir)) is None, (
        "key-collision folder must NOT be scanned by a duplicate-"
        "only import: its bytes were never proven to match the card"
    )


def test_hash_duplicate_links_only_byte_verified_twin_folder(tmp_path):
    """Hash-token duplicate: ``_hash_twin_rows`` returns every catalog
    row whose stored ``photos.file_hash`` matches the card. The stored
    hash column reflects the LAST scan, so a stale row can name a
    folder whose archive file has since been overwritten with unrelated
    bytes. Only the twin(s) whose CURRENT on-disk bytes we re-hashed
    and matched are proven duplicates; linking the other rows'
    folders would pull unrelated/missing archive folders into the
    active workspace on a duplicate-only import. See PR #1107 review.
    """
    from import_dedup import compute_file_hash
    from import_job import ImportParams, run_import_job

    # Card file whose bytes hash to a known value.
    card = tmp_path / "card"
    card.mkdir()
    card_file = card / "IMG_1300.jpg"
    Image.new("RGB", (16, 16), "cyan").save(str(card_file))
    ts = datetime(2026, 6, 20, 11, 30, 0).timestamp()
    os.utime(str(card_file), (ts, ts))
    card_hash = compute_file_hash(str(card_file))
    card_size = os.path.getsize(str(card_file))

    # Two archive folders both cataloged as holding a photo with
    # file_hash == card_hash. Twin A really does; Twin B was modified
    # after its scan and now holds different bytes (stale hash row).
    archive = tmp_path / "archive"
    archive.mkdir()
    verified_dir = archive / "verified-hash-twin"
    verified_dir.mkdir()
    verified_file = verified_dir / "IMG_1300.jpg"
    # Real duplicate: same bytes as the card.
    with open(str(card_file), "rb") as src, open(str(verified_file), "wb") as dst:
        dst.write(src.read())

    stale_dir = archive / "stale-hash-twin"
    stale_dir.mkdir()
    stale_file = stale_dir / "IMG_1300.jpg"
    # Stale twin: on-disk bytes NO LONGER match card_hash. Same size
    # (so the size sanity check doesn't reject it) but a byte flipped.
    stale_bytes = bytearray(card_file.read_bytes())
    stale_bytes[-1] ^= 0xFF
    stale_file.write_bytes(bytes(stale_bytes))
    assert compute_file_hash(str(stale_file)) != card_hash

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    # Catalog both twins with file_hash == card_hash. Both rows appear
    # in ``_hash_twin_rows`` — but only ``verified_dir`` currently
    # holds those bytes.
    for folder_dir in (verified_dir, stale_dir):
        fid = db.conn.execute(
            "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
            (str(folder_dir), folder_dir.name),
        ).lastrowid
        db.conn.execute(
            "INSERT INTO photos (folder_id, filename, extension, file_size,"
            " file_hash) VALUES (?, ?, '.jpg', ?, ?)",
            (fid, "IMG_1300.jpg", card_size, card_hash),
        )
    db.conn.commit()

    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id, ImportParams(
            sources=[str(card)], destination=str(archive),
            verify_by_hash=True,
        ),
    )

    # The verified twin proves the archive still holds the card's
    # bytes — a legitimate skip.
    assert result["copied"] == 0
    assert result["skipped_duplicate"] == 1
    assert result["failed"] == 0
    assert result["safe_to_format"] is True

    ws_folder_is_root = {
        row["path"]: row["is_root"]
        for row in db.conn.execute(
            "SELECT f.path, wf.is_root FROM workspace_folders wf "
            "JOIN folders f ON f.id = wf.folder_id "
            "WHERE wf.workspace_id = ?",
            (ws_id,),
        )
    }
    # The byte-verified twin's folder is a user-facing workspace root
    # (``is_root=1``) — the import proved the archive holds the card's
    # bytes and links its folder into the workspace UI.
    assert ws_folder_is_root.get(str(verified_dir)) == 1
    # The stale-hash-twin folder was NEVER byte-verified against the
    # card this run. Before the fix, the hash path passed the whole
    # ``twin_rows`` set to ``_linkable_twin_dirs`` on the assumption
    # that ``photos.file_hash`` is authoritative, so this folder was
    # workspace-linked as its own top-level root, pulling an unrelated
    # archive folder into the workspace UI. It must NOT surface as a
    # workspace root here, and after the restricted-scan cascade fix
    # (PR #1107, line 1186) it must not appear in ``workspace_folders``
    # at all.
    assert str(stale_dir) not in ws_folder_is_root


def test_restricted_scan_does_not_link_unrelated_archive_subtrees(tmp_path):
    """The per-batch restricted ``scan()`` call in ``run_import_job``
    passes the broad archive ``destination`` as ``root`` and the templated
    ``dest_folder`` as the only ``restrict_dir``. Before the fix,
    scanner's eager ``_ensure_folder(root_path)`` (and every parent-chain
    step between the two) called ``db.add_folder(..., workspace_root=
    False)``, which still fires ``add_workspace_folder`` — and its
    path-prefix subtree cascade in ``_add_workspace_folder_no_commit``
    would link every pre-existing cataloged descendant of ``destination``
    into the active workspace. A one-folder import would therefore make
    unrelated archive subtrees (e.g. shoots from a different card or a
    different workspace) suddenly visible in the current workspace UI.

    See PR #1107 review at line 1186:
    "Avoid linking the whole archive during restricted scans."
    """
    from import_job import ImportParams

    # Card: one file. Templates to <archive>/2026/2026-07-05/.
    card = _make_card(tmp_path, [
        ("DSC_9001.jpg", datetime(2026, 7, 5, 12, 0, 0), "orange"),
    ])
    archive = tmp_path / "archive"
    archive.mkdir()

    # Pre-existing archive tree: two unrelated folders already cataloged
    # in ``folders`` (as if scanned by a prior workspace or a previous
    # session on this workspace), NOT currently linked to the active
    # workspace. We insert them via raw SQL to bypass ``add_folder``'s
    # auto-link so the "unlinked descendants of destination" precondition
    # holds cleanly at the start of the run.
    unrelated_a = archive / "2024" / "2024-01-15-kenya-trip"
    unrelated_a.mkdir(parents=True)
    unrelated_b = archive / "2025" / "2025-09-02-yosemite"
    unrelated_b.mkdir(parents=True)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    for folder_dir in (unrelated_a, unrelated_b):
        db.conn.execute(
            "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
            (str(folder_dir), folder_dir.name),
        )
    db.conn.commit()
    unrelated_paths = {str(unrelated_a), str(unrelated_b)}
    # Precondition: neither pre-existing folder is workspace-linked yet.
    assert unrelated_paths.isdisjoint(_ws_linked_folder_paths(db, ws_id))

    # Run the import into ONE new templated dest_folder.
    from import_job import run_import_job
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(sources=[str(card)], destination=str(archive)),
    )
    assert result["copied"] == 1
    assert result["failed"] == 0

    linked = _ws_linked_folder_paths(db, ws_id)
    # The newly-imported dest_folder must be linked (that's the whole
    # point of the import).
    dest_folder = archive / "2026" / "2026-07-05"
    assert str(dest_folder) in linked, (
        "the imported dest_folder must be visible in the active workspace"
    )
    # Neither pre-existing unrelated archive subtree should have been
    # dragged into the active workspace by the restricted scan.
    for path in unrelated_paths:
        assert path not in linked, (
            f"unrelated pre-existing archive folder {path} was linked "
            f"into the active workspace by the restricted scan — the "
            f"cascade in ``_add_workspace_folder_no_commit`` fired for "
            f"``destination`` even though it was not the user's target"
        )


# --------------------------------------------------------------------------
# Task 2.7: Remote (SSH) archive destination.
#
# The card is rsynced to ``remote_path/subpath`` over SSH and cataloged at
# ``mount_path/subpath`` (resolve_remote_archive's mapping). The SSH/rsync
# transport seams are monkeypatched — the fake rsync writes the batch's
# files into the local mount (standing in for the NAS as seen through the
# SMB mount), so ``scan()`` catalogs them at the mount path exactly as it
# would against a real mounted NAS. No network is touched.
# --------------------------------------------------------------------------

def _remote_archive_for(tmp_path, subpath="2026/2026-07-03"):
    """Build a resolved remote-archive dict (resolve_remote_archive shape)
    plus the local mount base. The mount is a real tmp_path dir so scan()
    can walk the cataloged files after the fake rsync lands them there."""
    from move import build_remote_move_spec

    mount_base = tmp_path / "mount"
    mount_base.mkdir(exist_ok=True)
    target = {
        "id": "nas1", "name": "NAS", "host": "nas", "user": "me",
        "port": 22, "ssh_key": "", "bwlimit_kbps": 0,
        "remote_path": "/volume1/Photography",
        "mount_path": str(mount_base),
    }
    spec = build_remote_move_spec(target, "", "/usr/bin/rsync")
    return {
        "target": target,
        "rsync_bin": "/usr/bin/rsync",
        "remote": spec,  # host/user/port/ssh_key/bwlimit/ssh_dest_base/...
        "ssh_base": target["remote_path"],
        "mount_base": str(mount_base),
    }


def _install_fake_remote_rsync(monkeypatch, calls, *, verify=None):
    """Monkeypatch move's transport seams so a remote import never touches
    the network. The fake rsync copies the explicit source files into the
    local mount dir derived from the SSH dest path, recording every
    invocation in ``calls`` (list of dicts). ``verify`` overrides
    _remote_verify_complete's return (None == fully verified)."""
    import os as _os
    import shutil as _shutil

    import move as _move

    def fake_rsync(src_path, dest_spec, rsync_flags, total_files,
                   progress_cb, rsync_bin="rsync", extra_args=None,
                   src_specs=None, src_specs_dest_is_dir=True, **kw):
        # dest_spec is ``user@host:/volume1/Photography/2026/2026-07-03``
        # (a NAS dir) or, for a collision-renamed single file,
        # ``user@host:/.../DSC_0001_1.jpg`` (a NAS FILE). Map the NAS path
        # back to the local mount by swapping the SSH base prefix.
        ssh_path = dest_spec.split(":", 1)[1]
        if src_specs_dest_is_dir:
            rel = _os.path.relpath(ssh_path, calls["_ssh_base"])
            mount_dir = _os.path.join(calls["_mount_base"], rel)
            _os.makedirs(mount_dir, exist_ok=True)
            for s in src_specs:
                _shutil.copy2(
                    s, _os.path.join(mount_dir, _os.path.basename(s)))
        else:
            # File dest: exactly one source, landing under the chosen name.
            rel = _os.path.relpath(ssh_path, calls["_ssh_base"])
            mount_file = _os.path.join(calls["_mount_base"], rel)
            _os.makedirs(_os.path.dirname(mount_file), exist_ok=True)
            _shutil.copy2(src_specs[0], mount_file)
        calls["rsync"].append({
            "src_specs": list(src_specs), "dest_spec": dest_spec,
            "rsync_bin": rsync_bin, "extra_args": list(extra_args or []),
            "flags": list(rsync_flags or []),
            "dest_is_dir": src_specs_dest_is_dir,
        })
        return (0, "", False)

    monkeypatch.setattr(_move, "_run_rsync_streamed", fake_rsync)
    monkeypatch.setattr(_move, "_remote_mkdir_p", lambda r, p: (True, ""))

    # Card -> NAS verification seam (Task 2.7 FIX 1). ``verify`` may be a
    # plain return value, or a callable ``fn(src_specs) -> return`` so a
    # test can fail one specific card file by basename.
    def fake_verify_files(rsync_bin, src_specs, rsync_target, remote,
                          **kw):
        calls["verify"] += 1
        calls["verify_src_specs"].append(list(src_specs))
        if callable(verify):
            return verify(src_specs)
        return verify

    monkeypatch.setattr(_move, "remote_verify_files", fake_verify_files)


def _remote_calls(remote_archive):
    return {
        "rsync": [], "verify": 0, "verify_src_specs": [],
        "_ssh_base": remote_archive["ssh_base"],
        "_mount_base": remote_archive["mount_base"],
    }


def _run_remote_import(root, monkeypatch, params_kwargs, *, runner=None):
    """Remote counterpart to ``_run_import``, for tests that only need the
    happy-path transport seams (fake rsync, verification always OK).

    Builds a fake NAS target under ``root`` (mount at ``root/mount``),
    installs the transport seams, and runs the job against a fresh db at
    ``root/test.db``. ``destination``, ``remote_target`` and
    ``verify_by_hash`` are OWNED by this helper and must not be passed in
    ``params_kwargs`` (each would collide); ``params_kwargs`` supplies
    ``sources`` and everything else.

    ``verify_by_hash`` is forced True because with it off
    ``remote_unverified`` makes both card-safety verdicts False for free, and
    every safety assertion built on this helper would pass vacuously.

    RETURN SHAPE DIFFERS FROM ``_run_import``, deliberately and visibly:
    this returns ``(result, calls)``; ``_run_import`` returns
    ``(db, ws_id, result)``. Do not copy a destructuring from one to the
    other. A test that needs the ``Database`` or the ``FakeRunner`` should
    construct them itself — pass the runner in via ``runner=`` and keep its
    own reference — or call ``run_import_job`` directly, as the tests that
    need a hand-built remote-archive dict do.
    """
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(root)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)
    db_path = str(root / "test.db")
    db = Database(db_path)
    result = run_import_job(
        _make_job(), runner or FakeRunner(), db_path,
        db._active_workspace_id,
        ImportParams(
            destination=ra["mount_base"], remote_target=ra,
            verify_by_hash=True, **params_kwargs,
        ),
    )
    return result, calls


def _summaries(runner):
    """Every step summary the job pushed, in order."""
    return [kw.get("summary", "") for _, _, kw in runner.step_updates]


def test_remote_import_rsyncs_to_remote_and_catalogs_at_mount(
        tmp_path, monkeypatch):
    """A remote destination rsyncs the card into ``remote_path/subpath`` and
    catalogs the resulting rows at ``mount_path/subpath`` — the catalog
    paths are the local mount, never the NAS path."""
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra,
        ),
    )

    assert result["failed"] == 0
    assert result["copied"] == 2
    # rsync addressed the SSH target with the NAS-side path.
    assert calls["rsync"], "no rsync invocation captured"
    dests = {c["dest_spec"] for c in calls["rsync"]}
    assert dests == {"me@nas:/volume1/Photography/2026/2026-07-03"}
    for c in calls["rsync"]:
        assert c["rsync_bin"] == "/usr/bin/rsync"

    # Catalog rows point at the LOCAL MOUNT path, not the NAS path.
    rows = _photo_rows(db)
    row_paths = {os.path.join(r["folder_path"], r["filename"]) for r in rows}
    mount_dir = os.path.join(ra["mount_base"], "2026", "2026-07-03")
    assert row_paths == {
        os.path.join(mount_dir, "DSC_0001.jpg"),
        os.path.join(mount_dir, "DSC_0002.jpg"),
    }
    for r in row_paths:
        assert "/volume1/Photography" not in r
    # The mount folder is linked to the active workspace.
    assert mount_dir in _ws_linked_folder_paths(db, ws_id)


def test_remote_import_per_batch_rsync_invocation(tmp_path, monkeypatch):
    """Each destination-folder batch issues its own rsync invocation (the
    per-batch commit unit), captured by the fake harness."""
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    # Two distinct template folders -> two batches -> two rsync calls.
    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 4, 9, 0, 0), "blue"),
    ])

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra,
        ),
    )

    dests = sorted(c["dest_spec"] for c in calls["rsync"])
    assert dests == [
        "me@nas:/volume1/Photography/2026/2026-07-03",
        "me@nas:/volume1/Photography/2026/2026-07-04",
    ]


def test_remote_import_without_verify_leaves_hashes_null_and_unsafe(
        tmp_path, monkeypatch):
    """Without verify_by_hash the transfer relies on rsync's own integrity
    checking only: catalog rows keep NULL hash_status/hash_checked_at (no
    invented status values) and the run honestly reports safe_to_format
    False with the exact remote-verification reason."""
    from import_job import ImportParams, run_import_job

    # verify_by_hash=False below is also the DEFAULT: an operator who never
    # touches the toggle gets this honesty gate, so the default must stay
    # False for the pin to cover default runs.
    assert ImportParams(sources=[], destination="x").verify_by_hash is False

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra, verify_by_hash=False,
        ),
    )

    assert result["failed"] == 0
    assert result["copied"] == 1
    # No checksum verification ran.
    assert calls["verify"] == 0
    # Rows have NULL hash_status / hash_checked_at (not a fabricated value).
    rows = _photo_rows(db)
    assert rows
    for r in rows:
        assert r["hash_status"] is None
        assert r["hash_checked_at"] is None
    # Honest pill: not safe to format, exact reason string.
    assert result["safe_to_format"] is False
    assert any(
        u["reason"] == "enable verify_by_hash for remote verification"
        for u in result["unsafe_files"]
    ), result["unsafe_files"]


def test_remote_import_with_verify_stamps_ok_and_can_be_safe(
        tmp_path, monkeypatch):
    """With verify_by_hash a card->NAS --checksum dry-run runs; when it
    confirms every file, rows get hash_status='ok' and the run may report
    safe_to_format True."""
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra, verify_by_hash=True,
        ),
    )

    assert result["failed"] == 0
    assert result["copied"] == 2
    # The checksum dry-run ran (at least once).
    assert calls["verify"] >= 1
    rows = _photo_rows(db)
    assert rows
    for r in rows:
        assert r["hash_status"] == "ok"
        assert r["hash_checked_at"] is not None
        assert r["file_hash"] is not None
    assert result["safe_to_format"] is True
    assert result["unsafe_files"] == []


def test_remote_verify_checks_card_files_not_mount(tmp_path, monkeypatch):
    """FIX 1: verification must be genuinely card -> NAS. The verify seam's
    source args must be the CARD file paths, never the local mount folder
    (which is the same physical storage as the NAS, making that comparison
    tautological)."""
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra, verify_by_hash=True,
        ),
    )

    assert calls["verify_src_specs"], "verify seam was never called"
    verified_sources = {
        os.path.realpath(s)
        for batch in calls["verify_src_specs"] for s in batch
    }
    expected_card = {
        os.path.realpath(str(card / "DSC_0001.jpg")),
        os.path.realpath(str(card / "DSC_0002.jpg")),
    }
    assert verified_sources == expected_card, verified_sources
    # And NOT the mount folder / mount files.
    for s in verified_sources:
        assert ra["mount_base"] not in s


def test_remote_import_verify_failure_fails_specific_file(
        tmp_path, monkeypatch):
    """FIX 1: when the card->NAS verify reports a specific card file
    missing/different at the NAS, only THAT file fails (no hash_status='ok'
    row for it) and safe_to_format is False; a sibling that verified is
    stamped ok."""
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)

    def verify_fn(src_specs):
        # Report DSC_0001.jpg as still differing/absent at the NAS.
        for s in src_specs:
            if os.path.basename(s) == "DSC_0001.jpg":
                return ("DSC_0001.jpg", None)
        return None

    _install_fake_remote_rsync(monkeypatch, calls, verify=verify_fn)

    # Both files land in the SAME date folder -> one batch, so the batch's
    # verify sees both and reports one bad.
    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 10, 5, 0), "green"),
    ])

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra, verify_by_hash=True,
        ),
    )

    assert result["failed"] == 1
    # The sibling that verified still lands and counts as copied.
    assert result["copied"] == 1, result
    assert result["safe_to_format"] is False
    assert any(
        "DSC_0001.jpg" in u["path"] or "DSC_0001.jpg" in u["reason"]
        for u in result["unsafe_files"]
    ), result["unsafe_files"]

    # The failed file must NOT carry an 'ok' hash verdict; the sibling that
    # verified must.
    rows = {r["filename"]: r for r in _photo_rows(db)}
    if "DSC_0001.jpg" in rows:
        assert rows["DSC_0001.jpg"]["hash_status"] != "ok"
    assert rows["DSC_0002.jpg"]["hash_status"] == "ok"


def test_remote_import_same_basename_collision_parity(tmp_path, monkeypatch):
    """FIX 2: two DIFFERENT card files with the same basename destined for
    one date folder must both land under distinct names on the NAS and be
    cataloged as distinct photos — never silently clobber."""
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    # Same basename, different content, in two card subdirs, same mtime
    # (same date folder). Distinct colors -> distinct bytes.
    card = tmp_path / "card"
    (card / "DCIM" / "100").mkdir(parents=True)
    (card / "DCIM" / "101").mkdir(parents=True)
    from PIL import Image as _Image
    p1 = card / "DCIM" / "100" / "DSC_0001.jpg"
    p2 = card / "DCIM" / "101" / "DSC_0001.jpg"
    _Image.new("RGB", (16, 16), "red").save(str(p1))
    _Image.new("RGB", (16, 16), "blue").save(str(p2))
    ts = datetime(2026, 7, 3, 10, 0, 0).timestamp()
    for p in (p1, p2):
        os.utime(str(p), (ts, ts))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra,
        ),
    )

    assert result["failed"] == 0, result["unsafe_files"]
    # Both cataloged as distinct photos.
    rows = _photo_rows(db)
    assert len(rows) == 2, [dict(r) for r in rows]
    filenames = sorted(r["filename"] for r in rows)
    # Distinct on-disk names: the second landed under a numeric suffix.
    assert filenames == ["DSC_0001.jpg", "DSC_0001_1.jpg"], filenames
    # Both files physically exist at the mount, distinct content.
    mount_dir = os.path.join(ra["mount_base"], "2026", "2026-07-03")
    b0 = open(os.path.join(mount_dir, "DSC_0001.jpg"), "rb").read()
    b1 = open(os.path.join(mount_dir, "DSC_0001_1.jpg"), "rb").read()
    assert b0 != b1


# --------------------------------------------------------------------------
# PR #1113 review regressions.
# --------------------------------------------------------------------------

def test_remote_import_source_card_twin_does_not_back_duplicate_skip(
        tmp_path, monkeypatch):
    """A stale ``photos`` row whose folder path IS the card being imported
    must not back a remote duplicate skip. Re-hashing that twin just
    re-reads the card, proving nothing about an off-card copy; accepting
    it would count the file as ``skipped_duplicate`` and, with
    ``verify_by_hash`` on, let ``safe_to_format`` flip green over a card
    whose bytes never crossed the network. Mirrors the local path's
    source-root filter."""
    from import_job import ImportParams, run_import_job
    from scanner import scan

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # Seed the catalog with a row whose folder path IS the card — as if the
    # mounted card had been scanned into the DB in a previous session.
    scan(str(card), db)
    seeded = _photo_rows(db)
    assert seeded, "expected pre-seeded catalog row for the card file"
    assert any(r["folder_path"] == str(card) for r in seeded), \
        [dict(r) for r in seeded]

    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra, verify_by_hash=True,
        ),
    )

    # The card file must have been copied off — not skipped as a duplicate.
    assert result["copied"] == 1, result
    assert result["skipped_duplicate"] == 0, result
    # rsync actually ran with the card file.
    assert calls["rsync"], "no rsync invocation captured"
    src_specs = {
        os.path.basename(s)
        for c in calls["rsync"] for s in c["src_specs"]
    }
    assert "DSC_0001.jpg" in src_specs, src_specs
    # A row now exists under the mount path (the new archive copy), not
    # just the card.
    rows = _photo_rows(db)
    mount_dir = os.path.join(ra["mount_base"], "2026", "2026-07-03")
    row_paths = {os.path.join(r["folder_path"], r["filename"]) for r in rows}
    assert os.path.join(mount_dir, "DSC_0001.jpg") in row_paths, row_paths


def test_remote_import_no_verify_fails_uncataloged_landings(
        tmp_path, monkeypatch):
    """Without ``verify_by_hash`` the row-presence check still runs — a
    remote rsync that returns success without actually populating the local
    mount (unmounted/misconfigured mount base) must not report
    ``copied``/NULL for a file with no catalog row. It fails and
    ``safe_to_format`` cannot flip green off a ghost success."""
    import move as _move
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)

    # Fake rsync that pretends to succeed but writes nothing to the local
    # mount — simulates a real rsync succeeding at the NAS while the local
    # mount base points at an unmounted / wrong path so scan() sees no
    # landed files.
    def fake_rsync_no_write(src_path, dest_spec, rsync_flags, total_files,
                            progress_cb, rsync_bin="rsync", extra_args=None,
                            src_specs=None, src_specs_dest_is_dir=True, **kw):
        calls["rsync"].append({
            "src_specs": list(src_specs or []),
            "dest_spec": dest_spec,
        })
        return (0, "", False)

    monkeypatch.setattr(_move, "_run_rsync_streamed", fake_rsync_no_write)
    monkeypatch.setattr(_move, "_remote_mkdir_p", lambda r, p: (True, ""))

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra, verify_by_hash=False,
        ),
    )

    # The file "landed" per rsync's return but scan() found nothing on the
    # mount, so it must be failed instead of counted as copied.
    assert result["copied"] == 0, result
    assert result["failed"] == 1, result
    assert any(
        "not cataloged" in u["reason"]
        for u in result["unsafe_files"]
    ), result["unsafe_files"]
    assert result["safe_to_format"] is False


def test_remote_import_verify_fails_uncataloged_landings(
        tmp_path, monkeypatch):
    """Parity check: the ``verify_by_hash=True`` path also fails a landed
    file with no catalog row (it did before this fix; the refactor must
    preserve that)."""
    import move as _move
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)

    def fake_rsync_no_write(src_path, dest_spec, rsync_flags, total_files,
                            progress_cb, rsync_bin="rsync", extra_args=None,
                            src_specs=None, src_specs_dest_is_dir=True, **kw):
        calls["rsync"].append({"src_specs": list(src_specs or [])})
        return (0, "", False)

    monkeypatch.setattr(_move, "_run_rsync_streamed", fake_rsync_no_write)
    monkeypatch.setattr(_move, "_remote_mkdir_p", lambda r, p: (True, ""))
    monkeypatch.setattr(
        _move, "remote_verify_files",
        lambda *a, **kw: None,
    )

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra, verify_by_hash=True,
        ),
    )

    assert result["copied"] == 0, result
    assert result["failed"] == 1, result
    assert any(
        "not cataloged" in u["reason"]
        for u in result["unsafe_files"]
    ), result["unsafe_files"]


def test_remote_import_rsync_uses_ignore_existing_to_survive_basename_races(
        tmp_path, monkeypatch):
    """Two remote import jobs (or a job racing another writer) that plan to
    land different bytes under the same basename would both pass the earlier
    mount-side ``os.path.exists`` collision check. Plain ``rsync -a`` would
    then let the second writer clobber the first's already-verified NAS
    bytes; ``--ignore-existing`` on the transfer flag list stops rsync
    from overwriting the receiver-side file, and the verify step still
    catches the mismatch. Regression: the transport flags must include
    ``--ignore-existing`` on every rsync call the remote import issues."""
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 10, 5, 0), "green"),
    ])

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra,
        ),
    )

    assert calls["rsync"], "no rsync invocation captured"
    for c in calls["rsync"]:
        assert "--ignore-existing" in c["extra_args"], (
            "rsync invocation missing --ignore-existing "
            f"race-guard: {c['extra_args']}"
        )


def test_remote_import_rsync_dereferences_symlinked_source_files(
        tmp_path, monkeypatch):
    """A curated card folder that symlinks to the real image files (or a
    source root that's itself a symlink into ``/Volumes/Card/DCIM``) must
    land the referenced BYTES on the NAS, not the symlink itself. The base
    rsync command is ``rsync -a``, which preserves symlinks: without
    ``--copy-links`` the NAS would receive a symlink pointing back at the
    card path, and formatting/unmounting the card would break the archive.
    With ``verify_by_hash`` the mount-side scan follows the symlink through
    the mount, so ``safe_to_format`` could still go green over an archive
    that only contains symlinks. Regression: every import rsync must carry
    ``--copy-links``."""
    import pytest
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    if not hasattr(os, "symlink"):
        pytest.skip("symlinks not available on this platform")

    # Real files live under ``real_card``; the ``curated`` folder we hand to
    # the import contains symlinks to them. Each symlink is what
    # ``run_import_job`` walks and hands to rsync — a plain ``rsync -a``
    # would preserve the link.
    real_card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ], card_name="real_card")
    curated = tmp_path / "curated"
    curated.mkdir()
    for name in ("DSC_0001.jpg", "DSC_0002.jpg"):
        try:
            os.symlink(str(real_card / name), str(curated / name))
        except (OSError, NotImplementedError):
            pytest.skip("symlink creation not supported on this platform")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(curated)], destination=ra["mount_base"],
            remote_target=ra,
        ),
    )

    assert calls["rsync"], "no rsync invocation captured"
    for c in calls["rsync"]:
        assert "--copy-links" in c["extra_args"], (
            "rsync invocation missing --copy-links symlink-dereference "
            f"guard: {c['extra_args']}"
        )


def test_remote_import_fails_when_mount_row_hash_disagrees_with_verified_card(
        tmp_path, monkeypatch):
    """``remote_verify_files`` runs card -> NAS (``ssh_base``); ``scan()``
    reads under the local mount. If the mount base is stale/misconfigured
    but happens to already contain the same ``<folder>/<filename>`` we
    transferred, scan populates the row from unrelated bytes. Stamping
    ``hash_status='ok'`` on that row would flip ``safe_to_format`` green
    over storage we never touched. This exercises the cross-check: the
    landed file is failed rather than stamped ok, and safe_to_format
    stays False."""
    import move as _move
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])

    # Fake rsync that "lands" the file under the destination BUT with
    # wrong bytes — the scenario Codex flagged: the NAS is fine (verify
    # step below reports OK), but the mount base points at different
    # storage whose bytes at ``<folder>/<filename>`` don't match the
    # card. scan() will pick up the wrong-storage hash; the cross-check
    # must fail this file rather than stamp ``hash_status='ok'``.
    def fake_rsync_writes_wrong_bytes(
            src_path, dest_spec, rsync_flags, total_files,
            progress_cb, rsync_bin="rsync", extra_args=None,
            src_specs=None, src_specs_dest_is_dir=True, **kw):
        calls["rsync"].append({
            "src_specs": list(src_specs or []),
            "dest_spec": dest_spec,
            "extra_args": list(extra_args or []),
        })
        # Map the SSH dest_spec back to the mount side and write a file
        # with DIFFERENT bytes than the card — the wrong-storage stand-in.
        ssh_path = dest_spec.split(":", 1)[1]
        if src_specs_dest_is_dir:
            rel = os.path.relpath(ssh_path, ra["ssh_base"])
            mount_dir = os.path.join(ra["mount_base"], rel)
            os.makedirs(mount_dir, exist_ok=True)
            from PIL import Image as _Image
            for s in src_specs:
                _Image.new("RGB", (16, 16), "green").save(
                    os.path.join(mount_dir, os.path.basename(s)))
        else:
            rel = os.path.relpath(ssh_path, ra["ssh_base"])
            mount_file = os.path.join(ra["mount_base"], rel)
            os.makedirs(os.path.dirname(mount_file), exist_ok=True)
            from PIL import Image as _Image
            _Image.new("RGB", (16, 16), "green").save(mount_file)
        return (0, "", False)

    monkeypatch.setattr(_move, "_run_rsync_streamed",
                        fake_rsync_writes_wrong_bytes)
    monkeypatch.setattr(_move, "_remote_mkdir_p", lambda r, p: (True, ""))
    # NAS-side card verification succeeds (the card bytes DID make it to
    # the NAS in this scenario). The mount just doesn't show them.
    monkeypatch.setattr(_move, "remote_verify_files",
                        lambda *a, **kw: None)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra, verify_by_hash=True,
        ),
    )

    assert result["copied"] == 0, result
    assert result["failed"] == 1, result
    assert result["safe_to_format"] is False, result
    assert any(
        "scanned mount row hash" in u["reason"]
        for u in result["unsafe_files"]
    ), result["unsafe_files"]
    rows = {r["filename"]: r for r in _photo_rows(db)}
    # The row must NOT carry an 'ok' verdict.
    if "DSC_0001.jpg" in rows:
        assert rows["DSC_0001.jpg"]["hash_status"] != "ok", dict(
            rows["DSC_0001.jpg"])


def test_remote_import_promotes_missing_destination_folder_to_ok(
        tmp_path, monkeypatch):
    """A remote import that lands into a folder whose ``folders`` row is
    currently ``status='missing'`` (e.g. a previous health check ran while
    the NAS mount was absent) must promote the row to ``'ok'`` after
    makedirs — mirroring the local path. ``scanner.scan()`` only clears
    ``'partial'`` on success, so a row still labelled ``'missing'`` would
    keep the imported photos hidden from workspace queries while
    ``safe_to_format`` could still flip green. See PR #1113 review."""
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # Pre-seed the destination folder row as missing (as if a prior health
    # check ran while the NAS mount was offline).
    mount_dir = os.path.join(ra["mount_base"], "2026", "2026-07-03")
    os.makedirs(mount_dir, exist_ok=True)
    db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'missing')",
        (mount_dir, os.path.basename(mount_dir)),
    )
    db.conn.commit()

    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra, verify_by_hash=True,
        ),
    )

    assert result["copied"] == 1, result
    assert result["failed"] == 0, result

    # The folder row must have flipped out of 'missing'. Otherwise
    # workspace queries would filter the just-imported photo out.
    row = db.conn.execute(
        "SELECT status FROM folders WHERE path = ?", (mount_dir,),
    ).fetchone()
    assert row is not None, "destination folder row disappeared"
    assert row["status"] == "ok", (
        f"expected status='ok' after remote import into a previously "
        f"missing folder; got {row['status']!r}"
    )

    # And the imported photo is visible in the active workspace.
    linked = _ws_linked_folder_paths(db, ws_id)
    assert mount_dir in linked, linked


def test_remote_import_promotes_missing_folder_preserves_partial(
        tmp_path, monkeypatch):
    """Guard-rail: the missing→ok promote must NOT stomp a row currently
    labelled ``'partial'`` (a real prior-scan needs-rescan signal). The
    local path preserves ``'partial'``; the remote path must too."""
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    card = _make_card(tmp_path, [
        ("DSC_0002.jpg", datetime(2026, 7, 3, 10, 0, 0), "blue"),
    ])

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    mount_dir = os.path.join(ra["mount_base"], "2026", "2026-07-03")
    os.makedirs(mount_dir, exist_ok=True)
    db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'partial')",
        (mount_dir, os.path.basename(mount_dir)),
    )
    db.conn.commit()

    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra, verify_by_hash=True,
        ),
    )
    assert result["failed"] == 0, result

    row = db.conn.execute(
        "SELECT status FROM folders WHERE path = ?", (mount_dir,),
    ).fetchone()
    # scan() may clear partial→ok on its own success stamp; the point of
    # this test is that our missing→ok UPDATE did NOT stomp partial before
    # scan ran. Either 'ok' (scan cleared it) or 'partial' (scan
    # preserved it) is acceptable; anything else means our UPDATE
    # over-reached.
    assert row["status"] in ("ok", "partial"), (
        f"missing→ok UPDATE must preserve 'partial'; got {row['status']!r}"
    )


def test_remote_import_accepts_paired_jpeg_companion_row(
        tmp_path, monkeypatch):
    """When a remote batch lands a JPEG whose sibling RAW is already
    cataloged in the same destination folder, ``scanner.scan()`` runs
    ``_pair_raw_jpeg_companions`` which merges the JPEG into the RAW row
    (``companion_path``) and DELETES the JPEG's own row. The catalog-row
    presence check must recognize this legitimate ``row is None`` case
    via the companion lookup instead of failing the JPEG as
    ``not cataloged after scan``. Mirrors the local path. See PR #1113
    review."""
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    # Seed an existing RAW file at the destination + its catalog row.
    mount_dir = os.path.join(ra["mount_base"], "2026", "2026-07-03")
    os.makedirs(mount_dir, exist_ok=True)
    raw_seed = os.path.join(mount_dir, "_seed.jpg")
    Image.new("RGB", (16, 16), "red").save(raw_seed)
    raw_bytes = open(raw_seed, "rb").read() + b"RAW-SENSOR-DATA"
    os.unlink(raw_seed)
    raw_path = os.path.join(mount_dir, "DSC_0800.NEF")
    with open(raw_path, "wb") as f:
        f.write(raw_bytes)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (mount_dir, os.path.basename(mount_dir)),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_hash) VALUES (?, ?, '.nef', ?, ?)",
        (fid, "DSC_0800.NEF", len(raw_bytes), "deadbeef" * 8),
    )
    db.conn.commit()

    # Card holds a new JPEG that will land as DSC_0800.jpg and pair with
    # the pre-existing RAW during the batch's restricted scan.
    card = _make_card(tmp_path, [
        ("DSC_0800.jpg", datetime(2026, 7, 3, 10, 0, 0), "green"),
    ])

    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra, verify_by_hash=True,
        ),
    )

    # The paired JPEG must NOT be reported as "not cataloged after scan"
    # — its bytes are represented by the RAW row's companion_path.
    assert result["copied"] == 1, result
    assert result["failed"] == 0, result
    assert not any(
        "not cataloged" in u["reason"]
        for u in result["unsafe_files"]
    ), result["unsafe_files"]

    # The RAW row now records the JPEG as its companion.
    raw_row = db.conn.execute(
        "SELECT companion_path FROM photos WHERE filename = 'DSC_0800.NEF'",
    ).fetchone()
    assert raw_row["companion_path"] == "DSC_0800.jpg", (
        f"pair-merge must record JPEG on the RAW; got {dict(raw_row)}"
    )
    # And the JPEG has no standalone row of its own.
    jpeg_row = db.conn.execute(
        "SELECT id FROM photos WHERE filename = 'DSC_0800.jpg'",
    ).fetchone()
    assert jpeg_row is None, (
        "paired JPEG must not keep a standalone row after pair-merge"
    )


def test_remote_import_invalidates_derived_caches_on_content_change(
        tmp_path, monkeypatch):
    """Remote mirror of
    ``test_import_invalidates_derived_caches_on_content_change``: when a
    rsynced file replaces bytes at a mount path whose catalog row already
    has ``working_copy_path`` set (from a prior scan of an older mount
    file at the same path), the import must invalidate that WC — the
    deferred end-of-run ``_extract_working_copies`` skips rows with
    ``working_copy_path IS NOT NULL``, so without invalidation the WC
    persists pointing at bytes the mount no longer holds. Spec decision 6.
    """
    from import_dedup import compute_file_hash
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    mount_dir = os.path.join(ra["mount_base"], "2026", "2026-07-03")
    os.makedirs(mount_dir, exist_ok=True)
    # A stale mount file present before the import; its catalog row
    # captures its OLD hash + a fake WC path (as if a prior scan
    # extracted a WC for it).
    stale_mount = os.path.join(mount_dir, "DSC_0700.jpg")
    Image.new("RGB", (16, 16), "blue").save(stale_mount)
    stale_hash = compute_file_hash(stale_mount)

    vireo_dir = tmp_path / "vireo_data"
    (vireo_dir / "working").mkdir(parents=True)
    fake_wc = vireo_dir / "working" / "1.jpg"
    Image.new("RGB", (8, 8), "yellow").save(str(fake_wc))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (mount_dir, os.path.basename(mount_dir)),
    ).lastrowid
    photo_id = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_hash, working_copy_path) VALUES (?, ?, '.jpg', ?, ?, ?)",
        (
            fid,
            "DSC_0700.jpg",
            os.path.getsize(stale_mount),
            stale_hash,
            str(fake_wc),
        ),
    ).lastrowid
    db.conn.commit()

    # Orphan preview file: previews/{pid}_{size}.jpg with NO
    # preview_cache row (legacy code path / interrupted insert).
    # Row-driven ``_invalidate_derived_caches`` can't see it; an
    # untracked-preview sweep removes it before it can be lazy-adopted
    # and served as stale pre-change bytes. In this content-change
    # geometry EITHER sweep suffices — scan()'s own internal sweep also
    # covers it — so this asserts "some sweep ran"; the pairing tests
    # isolate the import job's sweep call with a geometry scan()'s
    # sweep set never contains.
    previews_dir = vireo_dir / "previews"
    previews_dir.mkdir()
    orphan_preview = previews_dir / f"{photo_id}_512.jpg"
    orphan_preview.write_bytes(b"stale-orphan-preview-bytes")

    # Remove the mount file (simulates: the mount file was deleted/
    # replaced between the prior scan and this import, and the import
    # restores the same filename with new bytes).
    os.unlink(stale_mount)

    # Card holds the NEW bytes at the same filename/date, which will land
    # at the same mount path.
    card = _make_card(tmp_path, [
        ("DSC_0700.jpg", datetime(2026, 7, 3, 10, 0, 0), "green"),
    ])
    # Force skip_duplicates False so the card's new bytes actually get
    # copied over even though the stale row's hash still exists.
    result = run_import_job(_make_job(), FakeRunner(), db_path, ws_id,
                            ImportParams(sources=[str(card)],
                                         destination=ra["mount_base"],
                                         remote_target=ra,
                                         verify_by_hash=True,
                                         skip_duplicates=False,
                                         vireo_dir=str(vireo_dir)))
    assert result["copied"] == 1
    assert result["failed"] == 0

    # The row's WC path was cleared so the deferred extractor / later
    # backfill can rebuild against the new mount bytes.
    row = db.conn.execute(
        "SELECT working_copy_path FROM photos WHERE id = ?", (photo_id,),
    ).fetchone()
    assert row["working_copy_path"] is None, (
        "content change on a landed row must clear working_copy_path so "
        "the deferred WC pass rebuilds it against the new mount bytes"
    )
    # The stale WC file was also unlinked from disk.
    assert not fake_wc.exists(), (
        "content change on a landed row must delete the stale WC file"
    )
    # The orphan preview (no preview_cache row) was removed by the
    # untracked-preview sweep.
    assert not orphan_preview.exists(), (
        "untracked-preview sweep must remove orphan preview files for "
        "invalidated photos"
    )


def test_remote_import_invalidates_derived_caches_when_pre_row_had_null_hash(
        tmp_path, monkeypatch):
    """Remote mirror of
    ``test_import_invalidates_derived_caches_when_pre_row_had_null_hash``:
    a pre-scan row with ``file_hash IS NULL`` can still carry
    ``working_copy_path``/thumb/preview caches from earlier processing.
    Scanner's own content-change path treats ``NULL -> concrete hash`` as
    an invalidating transition; the remote per-batch invalidation must
    mirror that, or restoring a deleted mount file at that path leaves
    stale WC/thumb bytes cached against the fresh hash. Spec decision 6.
    """
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    mount_dir = os.path.join(ra["mount_base"], "2026", "2026-07-03")
    os.makedirs(mount_dir, exist_ok=True)

    vireo_dir = tmp_path / "vireo_data"
    (vireo_dir / "working").mkdir(parents=True)
    fake_wc = vireo_dir / "working" / "1.jpg"
    Image.new("RGB", (8, 8), "yellow").save(str(fake_wc))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (mount_dir, os.path.basename(mount_dir)),
    ).lastrowid
    # Legacy-shaped row: no file on the mount, ``file_hash IS NULL``, but
    # a stale ``working_copy_path`` from an earlier processing pass.
    photo_id = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_hash, working_copy_path) VALUES (?, ?, '.jpg', ?, NULL, ?)",
        (fid, "DSC_0701.jpg", 12345, str(fake_wc)),
    ).lastrowid
    db.conn.commit()

    # Card holds the NEW bytes at the same filename/date — the import
    # will land them at the mount path whose row currently has
    # ``file_hash IS NULL`` + a stale WC.
    card = _make_card(tmp_path, [
        ("DSC_0701.jpg", datetime(2026, 7, 3, 10, 0, 0), "green"),
    ])
    result = run_import_job(_make_job(), FakeRunner(), db_path, ws_id,
                            ImportParams(sources=[str(card)],
                                         destination=ra["mount_base"],
                                         remote_target=ra,
                                         verify_by_hash=True,
                                         skip_duplicates=False,
                                         vireo_dir=str(vireo_dir)))
    assert result["copied"] == 1
    assert result["failed"] == 0

    # The row's WC path was cleared: NULL -> concrete hash is a real
    # content change, and the deferred extractor / later backfill must be
    # allowed to rebuild the WC against the just-imported mount bytes.
    row = db.conn.execute(
        "SELECT working_copy_path FROM photos WHERE id = ?", (photo_id,),
    ).fetchone()
    assert row["working_copy_path"] is None, (
        "NULL-hash pre-scan row must invalidate its stale derived caches "
        "when the import stamps a concrete hash (mirrors scanner.scan()'s "
        "content-change path)"
    )
    assert not fake_wc.exists(), (
        "NULL-hash pre-scan row must have its stale WC file unlinked"
    )


def test_remote_import_invalidates_raw_caches_when_new_jpeg_pairs(
        tmp_path, monkeypatch):
    """RAW+JPEG companion restore, remote mirror of
    ``test_import_invalidates_raw_caches_when_new_jpeg_pairs``: when a
    freshly rsynced JPEG lands as companion to an existing RAW row
    (pair-merge deletes the JPEG's own row), the RAW row's derived
    caches may reflect the pre-pair state (RAW-only preview, or a
    deleted/replaced prior companion). The hash-stamping loop treats
    ``row is None`` as a fresh insert with no diff to invalidate, so
    without an explicit companion-invalidation pass the deferred WC
    pass skips the RAW (working_copy_path is set) and the UI keeps
    serving stale derived files. Spec decision 6.
    """
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    vireo_dir = tmp_path / "vireo_data"
    (vireo_dir / "working").mkdir(parents=True)

    # Pre-existing RAW file at the MOUNT path, cataloged standalone
    # with a stale working_copy_path from a prior RAW-only extraction.
    mount_dir = os.path.join(ra["mount_base"], "2026", "2026-07-03")
    os.makedirs(mount_dir, exist_ok=True)
    raw_seed = os.path.join(mount_dir, "_seed.jpg")
    Image.new("RGB", (16, 16), "red").save(raw_seed)
    raw_bytes = Path(raw_seed).read_bytes() + b"RAW-SENSOR-DATA"
    os.unlink(raw_seed)
    raw_archive = os.path.join(mount_dir, "DSC_0800.NEF")
    with open(raw_archive, "wb") as f:
        f.write(raw_bytes)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (mount_dir, os.path.basename(mount_dir)),
    ).lastrowid
    # WC file must live at working/{photo_id}.jpg — that's the layout
    # _invalidate_derived_caches unlinks.
    raw_photo_id = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_hash, working_copy_path) VALUES (?, ?, '.nef', ?, ?, 'placeholder')",
        (fid, "DSC_0800.NEF", len(raw_bytes),
         "deadbeef" * 8),
    ).lastrowid
    fake_wc = vireo_dir / "working" / f"{raw_photo_id}.jpg"
    Image.new("RGB", (8, 8), "orange").save(str(fake_wc))
    stale_wc_bytes = fake_wc.read_bytes()
    db.conn.execute(
        "UPDATE photos SET working_copy_path = ? WHERE id = ?",
        (str(fake_wc), raw_photo_id),
    )
    db.conn.commit()

    # Orphan preview file for the RAW: previews/{raw_photo_id}_512.jpg
    # with NO preview_cache row. In this companion-pair geometry the
    # RAW's id never enters scan()'s own internal sweep set (pairing is
    # not a content change), so ONLY the import job's post-batch
    # ``_sweep_untracked_previews_for_photos`` call can remove it —
    # this seed isolates that call site.
    previews_dir = vireo_dir / "previews"
    previews_dir.mkdir()
    orphan_preview = previews_dir / f"{raw_photo_id}_512.jpg"
    orphan_preview.write_bytes(b"stale-orphan-preview-bytes")

    # Card holds a NEW JPEG that will land at DSC_0800.jpg and pair with
    # the existing RAW during the batch scan.
    card = _make_card(tmp_path, [
        ("DSC_0800.jpg", datetime(2026, 7, 3, 10, 0, 0), "green"),
    ])

    result = run_import_job(_make_job(), FakeRunner(), db_path, ws_id,
                            ImportParams(sources=[str(card)],
                                         destination=ra["mount_base"],
                                         remote_target=ra,
                                         verify_by_hash=True,
                                         skip_duplicates=False,
                                         vireo_dir=str(vireo_dir)))
    assert result["copied"] == 1
    assert result["failed"] == 0

    # The RAW row's stale WC path was cleared (invalidation ran) and
    # the on-disk stale WC file was unlinked. The deferred end-of-run
    # ``_extract_working_copies`` then either succeeds with a fresh WC
    # (path differs from the stale one) or leaves working_copy_path
    # NULL for the scanner's later backfill; either way the row no
    # longer points at the pre-pair bytes.
    row = db.conn.execute(
        "SELECT working_copy_path, companion_path FROM photos WHERE id = ?",
        (raw_photo_id,),
    ).fetchone()
    assert row["companion_path"] == "DSC_0800.jpg", (
        "pair-merge must record the newly landed JPEG as the RAW's "
        "companion_path"
    )
    # If invalidation didn't run the row would still point at the
    # pre-pair WC path (which the extractor's candidate predicate would
    # then skip, since working_copy_path is set). Invalidation resets
    # the path, and the deferred WC pass rebuilds fresh: even when the
    # extractor happens to reuse the same on-disk slot
    # (``working/{id}.jpg``), the bytes at that path must differ from
    # the stale orange placeholder we seeded, because the WC now comes
    # from the just-verified companion JPEG.
    if fake_wc.exists():
        assert fake_wc.read_bytes() != stale_wc_bytes, (
            "RAW's stale WC bytes must not survive the import — either "
            "the file is unlinked or overwritten with a fresh WC from "
            "the verified companion JPEG"
        )
    # The RAW's orphan preview (no preview_cache row) is gone. Scan()'s
    # internal sweep never sees companion-paired RAW ids, so only the
    # import job's own sweep call can have removed it.
    assert not orphan_preview.exists(), (
        "the import-path untracked-preview sweep must remove the paired "
        "RAW's orphan preview files"
    )


def test_remote_import_invalidates_raw_caches_when_adopted_jpeg_pairs(
        tmp_path, monkeypatch):
    """Adopted-branch variant of
    ``test_remote_import_invalidates_raw_caches_when_new_jpeg_pairs``:
    the card's JPEG is ALREADY on the mount byte-identical (crash-
    recovery/resume geometry), so the collision loop adopts it
    (``skipped_duplicate``, no rsync) instead of transferring. Adoption
    only proves the JPEG bytes pre-existed on the mount — NOT that the
    RAW row already carried ``companion_path`` or that its derived
    caches reflect the paired state — so the adopted companion accept
    branch must invalidate the RAW's stale caches just like the
    transferred branch. Spec decision 6.
    """
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    vireo_dir = tmp_path / "vireo_data"
    (vireo_dir / "working").mkdir(parents=True)

    # Pre-existing RAW file at the MOUNT path, cataloged standalone
    # with a stale working_copy_path from a prior RAW-only extraction.
    mount_dir = os.path.join(ra["mount_base"], "2026", "2026-07-03")
    os.makedirs(mount_dir, exist_ok=True)
    raw_seed = os.path.join(mount_dir, "_seed.jpg")
    Image.new("RGB", (16, 16), "red").save(raw_seed)
    raw_bytes = Path(raw_seed).read_bytes() + b"RAW-SENSOR-DATA"
    os.unlink(raw_seed)
    raw_archive = os.path.join(mount_dir, "DSC_0800.NEF")
    with open(raw_archive, "wb") as f:
        f.write(raw_bytes)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (mount_dir, os.path.basename(mount_dir)),
    ).lastrowid
    # WC file must live at working/{photo_id}.jpg — that's the layout
    # _invalidate_derived_caches unlinks.
    raw_photo_id = db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_hash, working_copy_path) VALUES (?, ?, '.nef', ?, ?, 'placeholder')",
        (fid, "DSC_0800.NEF", len(raw_bytes),
         "deadbeef" * 8),
    ).lastrowid
    fake_wc = vireo_dir / "working" / f"{raw_photo_id}.jpg"
    Image.new("RGB", (8, 8), "orange").save(str(fake_wc))
    stale_wc_bytes = fake_wc.read_bytes()
    db.conn.execute(
        "UPDATE photos SET working_copy_path = ? WHERE id = ?",
        (str(fake_wc), raw_photo_id),
    )
    db.conn.commit()

    # Card holds the JPEG — and the SAME bytes are already at the
    # template mount path, uncataloged, so the collision loop adopts
    # rather than rsyncs.
    card = _make_card(tmp_path, [
        ("DSC_0800.jpg", datetime(2026, 7, 3, 10, 0, 0), "green"),
    ])
    import shutil
    shutil.copy2(str(card / "DSC_0800.jpg"),
                 os.path.join(mount_dir, "DSC_0800.jpg"))

    result = run_import_job(_make_job(), FakeRunner(), db_path, ws_id,
                            ImportParams(sources=[str(card)],
                                         destination=ra["mount_base"],
                                         remote_target=ra,
                                         verify_by_hash=True,
                                         skip_duplicates=False,
                                         vireo_dir=str(vireo_dir)))
    # The JPEG was adopted on-disk, not transferred.
    assert result["copied"] == 0
    assert result["failed"] == 0
    assert result["skipped_duplicate"] == 1
    assert calls["rsync"] == [], (
        "byte-identical mount file must be adopted without an rsync"
    )

    # Pairing still happened via the batch scan of the adopted path,
    # and the adopted-companion accept branch invalidated the RAW's
    # stale derived caches.
    row = db.conn.execute(
        "SELECT working_copy_path, companion_path FROM photos WHERE id = ?",
        (raw_photo_id,),
    ).fetchone()
    assert row["companion_path"] == "DSC_0800.jpg", (
        "pair-merge must record the adopted JPEG as the RAW's "
        "companion_path"
    )
    if fake_wc.exists():
        assert fake_wc.read_bytes() != stale_wc_bytes, (
            "RAW's stale WC bytes must not survive the import — either "
            "the file is unlinked or overwritten with a fresh WC from "
            "the adopted companion JPEG"
        )


def test_remote_import_paired_jpeg_verify_fails_on_mount_hash_mismatch(
        tmp_path, monkeypatch):
    """Cross-check parity: when the paired JPEG's mount bytes disagree
    with the source hash confirmed on the NAS (stale/misconfigured
    mount), the ``verify_by_hash`` branch must fail the JPEG instead of
    silently accepting it via the companion row. Same guard the non-
    companion branch runs; the pair-merged JPEG can't be exempt from
    it."""
    import move as _move
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)

    # Seed the existing RAW at the mount.
    mount_dir = os.path.join(ra["mount_base"], "2026", "2026-07-03")
    os.makedirs(mount_dir, exist_ok=True)
    raw_seed = os.path.join(mount_dir, "_seed.jpg")
    Image.new("RGB", (16, 16), "red").save(raw_seed)
    raw_bytes = open(raw_seed, "rb").read() + b"RAW-SENSOR-DATA"
    os.unlink(raw_seed)
    raw_path = os.path.join(mount_dir, "DSC_0800.NEF")
    with open(raw_path, "wb") as f:
        f.write(raw_bytes)

    # Fake rsync that "lands" the JPEG at the mount but with WRONG bytes
    # (the wrong-storage stand-in). NAS-side card verify still succeeds.
    def fake_rsync_wrong_bytes(
            src_path, dest_spec, rsync_flags, total_files,
            progress_cb, rsync_bin="rsync", extra_args=None,
            src_specs=None, src_specs_dest_is_dir=True, **kw):
        calls["rsync"].append({
            "src_specs": list(src_specs or []),
            "extra_args": list(extra_args or []),
        })
        ssh_path = dest_spec.split(":", 1)[1]
        rel = os.path.relpath(ssh_path, ra["ssh_base"])
        if src_specs_dest_is_dir:
            mount_dst = os.path.join(ra["mount_base"], rel)
            os.makedirs(mount_dst, exist_ok=True)
            for s in src_specs:
                Image.new("RGB", (16, 16), "yellow").save(
                    os.path.join(mount_dst, os.path.basename(s)))
        else:
            mount_file = os.path.join(ra["mount_base"], rel)
            os.makedirs(os.path.dirname(mount_file), exist_ok=True)
            Image.new("RGB", (16, 16), "yellow").save(mount_file)
        return (0, "", False)

    monkeypatch.setattr(_move, "_run_rsync_streamed", fake_rsync_wrong_bytes)
    monkeypatch.setattr(_move, "_remote_mkdir_p", lambda r, p: (True, ""))
    monkeypatch.setattr(_move, "remote_verify_files",
                        lambda *a, **kw: None)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (mount_dir, os.path.basename(mount_dir)),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_hash) VALUES (?, ?, '.nef', ?, ?)",
        (fid, "DSC_0800.NEF", len(raw_bytes), "deadbeef" * 8),
    )
    db.conn.commit()

    card = _make_card(tmp_path, [
        ("DSC_0800.jpg", datetime(2026, 7, 3, 10, 0, 0), "green"),
    ])

    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra, verify_by_hash=True,
        ),
    )

    assert result["copied"] == 0, result
    assert result["failed"] == 1, result
    assert result["safe_to_format"] is False, result
    assert any(
        "paired companion mount bytes" in u["reason"]
        for u in result["unsafe_files"]
    ), result["unsafe_files"]


def test_remote_import_rejects_dest_folder_under_source(
        tmp_path, monkeypatch):
    """When the mount base is an ancestor of a selected source and the
    folder template maps back into that source folder, ``dest_folder``
    (and every ``cand_mount`` under it) resolves inside a source root.
    The per-file collision loop would otherwise hash those source-backed
    ``cand_mount`` files, byte-match them against the card, and count
    them as ``skipped_duplicate`` — with ``verify_by_hash=True`` that
    would let ``safe_to_format`` flip green over a card whose bytes
    never crossed the network. The batch-level guard (mirroring the
    local path at ``_path_under_any_source(dest_folder)``) must reject
    the whole batch before makedirs and before any duplicate-adopt
    hashing runs. See PR #1113 review."""
    import move as _move
    from import_job import ImportParams, run_import_job

    # Set up a card that ALSO serves as the mount base — the folder
    # template ``2026/2026-07-03`` under the card is the dest_folder.
    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])

    # Build a remote-archive dict whose mount_base points at the card
    # itself. rsync should never be invoked in this scenario because the
    # batch is rejected before makedirs.
    from move import build_remote_move_spec
    target = {
        "id": "nas1", "name": "NAS", "host": "nas", "user": "me",
        "port": 22, "ssh_key": "", "bwlimit_kbps": 0,
        "remote_path": "/volume1/Photography",
        "mount_path": str(card),
    }
    spec = build_remote_move_spec(target, "", "/usr/bin/rsync")
    ra = {
        "target": target,
        "rsync_bin": "/usr/bin/rsync",
        "remote": spec,
        "ssh_base": target["remote_path"],
        "mount_base": str(card),
    }
    calls = {
        "rsync": [], "verify": 0, "verify_src_specs": [],
        "_ssh_base": ra["ssh_base"], "_mount_base": ra["mount_base"],
    }
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)
    # A stat-refusing OSError never needed: the guard runs before any
    # duplicate-adopt hashing that would touch cand_mount.

    # Also monkeypatch compute_file_hash to blow up if reached — the
    # guard's job is to prevent us ever hashing a source-backed
    # cand_mount as "already at destination".
    import import_job as _ij

    def _refuse_hash(path):
        raise AssertionError(
            f"unexpected hash of {path!r} — dest-under-source guard "
            "should have rejected the batch before this call"
        )

    orig_hash = _ij.compute_file_hash
    monkeypatch.setattr(_ij, "compute_file_hash", _refuse_hash)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra, verify_by_hash=True,
        ),
    )
    # Restore hash for any later use in this test.
    monkeypatch.setattr(_ij, "compute_file_hash", orig_hash)
    monkeypatch.setattr(_move, "_run_rsync_streamed", lambda *a, **kw: (
        (_ for _ in ()).throw(AssertionError("rsync should not run"))
    ))

    # No rsync invocation happened — the batch was rejected up front.
    assert calls["rsync"] == [], calls["rsync"]
    # Every file lands in ``failed`` with the dest-under-source reason.
    assert result["failed"] == 1, result
    assert result["copied"] == 0, result
    assert result["skipped_duplicate"] == 0, result
    assert result["safe_to_format"] is False, result
    assert any(
        "destination folder resolves inside a source directory"
        in u["reason"]
        for u in result["unsafe_files"]
    ), result["unsafe_files"]


def test_local_import_dest_under_source_refusal_reports_progress(
        tmp_path, monkeypatch):
    """Spec decision 2: a batch refused because dest_folder resolves
    inside a source directory must advance ``emitted`` and emit the
    batch-summary phase, exactly like the remote guard. Historically the
    local guard did neither, freezing the progress bar at the last
    pre-refusal value while the whole batch quietly failed."""
    from import_job import ImportParams, run_import_job

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 10, 5, 0), "green"),
    ])
    runner = FakeRunner()
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    # Destination = the card itself: the %Y/%Y-%m-%d dest_folder resolves
    # under the source root, tripping the batch-level guard (the
    # /api/jobs/import-photos route refuses this shape up front, but the
    # job-level guard is the backstop this test pins).
    result = run_import_job(
        _make_job(), runner, db_path, db._active_workspace_id,
        ImportParams(sources=[str(card)], destination=str(card),
                     verify_by_hash=True))

    assert result["failed"] == 2, result
    assert result["safe_to_format"] is False, result
    events = [d for _, kind, d in runner.events if kind == "progress"]
    # The refusal advances the bar over the whole rejected batch...
    assert any(d["current"] == 2 and d["total"] == 2 for d in events), events
    # ...with the same batch-summary phase string the remote path emits.
    assert any(d["phase"].endswith("0 copied · 0 already present")
               for d in events), [d["phase"] for d in events]


def test_remote_import_no_verify_fails_on_mount_hash_mismatch(
        tmp_path, monkeypatch):
    """Catalog-integrity guard on the no-verify path: when
    ``verify_by_hash=False`` and the mount base is stale/misconfigured
    (or an ``--ignore-existing``-blocked race left a different file at
    the same mount path), the row-presence check alone would let
    ``run_import_job`` return ``copied`` while the catalog row points
    at the wrong bytes. The scanned row's ``file_hash`` must be
    cross-checked against ``src_hash`` regardless of ``verify_by_hash``,
    with only the ``hash_status='ok'`` stamp still gated on the
    checksum-verification path. See PR #1113 review."""
    import move as _move
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)

    def fake_rsync_writes_wrong_bytes(
            src_path, dest_spec, rsync_flags, total_files,
            progress_cb, rsync_bin="rsync", extra_args=None,
            src_specs=None, src_specs_dest_is_dir=True, **kw):
        calls["rsync"].append({
            "src_specs": list(src_specs or []),
            "extra_args": list(extra_args or []),
        })
        ssh_path = dest_spec.split(":", 1)[1]
        rel = os.path.relpath(ssh_path, ra["ssh_base"])
        if src_specs_dest_is_dir:
            mount_dst = os.path.join(ra["mount_base"], rel)
            os.makedirs(mount_dst, exist_ok=True)
            for s in src_specs:
                Image.new("RGB", (16, 16), "yellow").save(
                    os.path.join(mount_dst, os.path.basename(s)))
        else:
            mount_file = os.path.join(ra["mount_base"], rel)
            os.makedirs(os.path.dirname(mount_file), exist_ok=True)
            Image.new("RGB", (16, 16), "yellow").save(mount_file)
        return (0, "", False)

    monkeypatch.setattr(_move, "_run_rsync_streamed",
                        fake_rsync_writes_wrong_bytes)
    monkeypatch.setattr(_move, "_remote_mkdir_p", lambda r, p: (True, ""))

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra, verify_by_hash=False,
        ),
    )

    # The mismatch must be detected even without verify_by_hash — the
    # catalog cannot silently point at unrelated bytes.
    assert result["copied"] == 0, result
    assert result["failed"] == 1, result
    assert result["safe_to_format"] is False, result
    assert any(
        "scanned mount row hash" in u["reason"]
        for u in result["unsafe_files"]
    ), result["unsafe_files"]
    # No hash_status='ok' stamp on the no-verify path even if the row
    # survived — that stamp remains gated on the checksum path.
    rows = {r["filename"]: r for r in _photo_rows(db)}
    if "DSC_0001.jpg" in rows:
        assert rows["DSC_0001.jpg"]["hash_status"] != "ok", dict(
            rows["DSC_0001.jpg"])


def test_remote_import_dup_only_batch_does_not_scan_mount(
        tmp_path, monkeypatch):
    """A remote duplicate-only batch neither transfers nor scans files.

    Its cataloged twin folder is linked directly, so a mounted SMB archive
    cannot turn the no-op batch into a whole-directory metadata walk.
    """
    import scanner as _scanner
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    # Card holds one file whose bytes already exist on the archive at a
    # pre-seeded twin folder — the duplicate gate will match and skip.
    from import_dedup import compute_file_hash as _hash
    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])
    card_file = str(card / "DSC_0001.jpg")
    src_hash = _hash(card_file)

    # Seed a byte-identical twin beneath the mounted archive destination.
    archive_twin_dir = Path(ra["mount_base"]) / "unsorted"
    archive_twin_dir.mkdir(parents=True)
    twin_path = archive_twin_dir / "DSC_0001.jpg"
    import shutil as _shutil
    _shutil.copy2(card_file, str(twin_path))
    # scan the twin folder into the DB so DuplicateChecker sees a hash
    # match token.
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    _scanner.scan(str(archive_twin_dir), db)
    # Run from a fresh workspace so the import must perform the link.
    ws_id = db.create_workspace("Fresh")
    assert str(archive_twin_dir) not in _ws_linked_folder_paths(db, ws_id)
    # Sanity: the twin row is present with the right hash.
    twin_rows = db.conn.execute(
        "SELECT p.file_hash, f.path FROM photos p "
        "JOIN folders f ON f.id = p.folder_id "
        "WHERE p.filename = 'DSC_0001.jpg'",
    ).fetchall()
    assert twin_rows and twin_rows[0]["file_hash"] == src_hash, [
        dict(r) for r in twin_rows]

    # Any import-time scanner call is the regression.
    def failing_scan(*args, **kwargs):
        raise AssertionError("duplicate-only remote import scanned the mount")

    monkeypatch.setattr(_scanner, "scan", failing_scan)
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra, verify_by_hash=True,
        ),
    )

    # rsync never ran — the file was accepted as a duplicate against the
    # off-card twin, and the folder was linked without scanning.
    assert calls["rsync"] == [], calls["rsync"]
    assert result["failed"] == 0, result
    assert result["safe_to_format"] is True, result
    assert result["skipped_duplicate"] == 1, result
    assert str(archive_twin_dir) in _ws_linked_folder_paths(db, ws_id)


def test_remote_import_links_verified_twin_folder_in_other_layout(
        tmp_path, monkeypatch):
    """A verified duplicate skip's twin folder may live under the mount
    destination in a DIFFERENT sub-folder than this run's template output
    (e.g. an older ``unsorted`` or ``%Y-%m-%d`` layout). Link that cataloged
    folder directly so it becomes visible without an archive scan. See
    PR #1113 review."""
    from import_dedup import compute_file_hash as _hash
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    # Card holds one photo whose bytes ALREADY exist under the mount base
    # in an "unsorted" sub-folder — an older layout the run's %Y/%Y-%m-%d
    # template does not target.
    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])
    card_file = str(card / "DSC_0001.jpg")
    src_hash = _hash(card_file)

    twin_folder = os.path.join(ra["mount_base"], "unsorted")
    os.makedirs(twin_folder, exist_ok=True)
    twin_path = os.path.join(twin_folder, "DSC_0001.jpg")
    import shutil as _shutil
    _shutil.copy2(card_file, twin_path)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    # Catalog the twin via raw SQL so its folder row is NOT linked into
    # the active workspace (running scanner.scan() here would link it via
    # the cascade in ``_add_workspace_folder_no_commit``, breaking the
    # test's "before" state). The duplicate-skip case is exactly "a byte-
    # identical twin is cataloged somewhere in the archive but the
    # user's active workspace does not yet see it yet".
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (twin_folder, os.path.basename(twin_folder)),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_hash) VALUES (?, ?, '.jpg', ?, ?)",
        (fid, "DSC_0001.jpg", os.path.getsize(twin_path), src_hash),
    )
    db.conn.commit()
    linked_before = _ws_linked_folder_paths(db, ws_id)
    assert twin_folder not in linked_before, linked_before

    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra, verify_by_hash=True,
        ),
    )

    # rsync stayed silent — the card was accepted as a duplicate.
    assert calls["rsync"] == [], calls["rsync"]
    assert result["skipped_duplicate"] == 1, result
    assert result["failed"] == 0, result
    assert result["safe_to_format"] is True, result
    # The verified twin folder is now visible in the active workspace,
    # even though it lives in a different sub-folder than the run's own
    # dest_folder (%Y/%Y-%m-%d template would have produced
    # ``2026/2026-07-03``, not ``unsorted``).
    linked_after = _ws_linked_folder_paths(db, ws_id)
    assert twin_folder in linked_after, {
        "before": sorted(linked_before), "after": sorted(linked_after),
        "twin_folder": twin_folder,
    }


def test_remote_import_direct_dup_link_does_not_read_unchanged_twins(
        tmp_path, monkeypatch):
    """Remote duplicate links must not read cataloged twin bytes."""
    import scanner
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])

    twin_folder = os.path.join(ra["mount_base"], "unsorted")
    os.makedirs(twin_folder, exist_ok=True)
    import shutil as _shutil
    for name in ("DSC_0001.jpg", "DSC_0002.jpg"):
        _shutil.copy2(str(card / name), os.path.join(twin_folder, name))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    # Catalog the twins the way a real scan would, so the rows carry the
    # file_mtime/metadata an incremental pass needs to skip on.
    scanner.scan(ra["mount_base"], db)
    _mark_exif_extracted(db)
    assert len(_photo_rows(db)) == 2

    read_paths = _count_feature_computations(monkeypatch)
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra, verify_by_hash=True,
        ),
    )

    assert calls["rsync"] == [], calls["rsync"]
    assert result["skipped_duplicate"] == 2, result
    assert result["failed"] == 0, result
    assert twin_folder in _ws_linked_folder_paths(db, ws_id)
    assert read_paths == [], (
        "remote duplicate-only import re-read already-cataloged, unchanged "
        f"twins: {read_paths}"
    )


def test_remote_import_dup_only_batch_does_not_read_dest_folder_twins(
        tmp_path, monkeypatch):
    """A duplicate-only remote batch does not read twins in its own date
    folder; it skips the batch scan and links their cataloged folder."""
    import scanner
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])

    # The twins live in the very folder this card's files map to, which is
    # what re-importing an already-imported card looks like.
    dest_folder = os.path.join(ra["mount_base"], "2026", "2026-07-03")
    os.makedirs(dest_folder, exist_ok=True)
    import shutil as _shutil
    for name in ("DSC_0001.jpg", "DSC_0002.jpg"):
        _shutil.copy2(str(card / name), os.path.join(dest_folder, name))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    scanner.scan(ra["mount_base"], db)
    _mark_exif_extracted(db)
    assert len(_photo_rows(db)) == 2

    read_paths = _count_feature_computations(monkeypatch)
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra, verify_by_hash=True,
        ),
    )

    assert calls["rsync"] == [], calls["rsync"]
    assert result["skipped_duplicate"] == 2, result
    assert result["copied"] == 0, result
    assert result["failed"] == 0, result
    assert dest_folder in _ws_linked_folder_paths(db, ws_id)
    assert read_paths == [], (
        "remote duplicate-only import re-read already-cataloged, unchanged "
        f"twins in the destination folder: {read_paths}"
    )


def test_remote_import_landing_refreshes_stale_row_with_matching_mtime(
        tmp_path, monkeypatch):
    """A fresh landing at a destination path that carries a stale catalog
    row must overwrite the row's ``file_hash`` even when the newly
    transferred bytes inherit the same ``file_mtime`` the stale row
    remembers.

    ``scanner.scan()``'s incremental fast path treats a path as unchanged
    when the on-disk ``file_mtime`` matches the catalog row's
    ``file_mtime``. rsync ``-a`` (and ``shutil.copy2`` in the fake
    harness) preserves the source's mtime on the destination, so a source
    card whose file happens to share an mtime with a stale row — an
    orphan left after the actual file was deleted, then re-landed by a
    fresh import — would otherwise slip through incremental with its
    stale ``file_hash`` intact. The subsequent
    ``scan_h`` vs ``src_h_norm`` cross-check would then reject the
    landing as a mount-hash mismatch and no retry could fix it (mtime has
    not moved). The batch scan must therefore run non-incrementally
    whenever it targets landed/adopted paths, matching the local path.
    """
    import scanner
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    # Stale row: seed the destination path with red bytes and catalog
    # them, then delete the file so the fresh landing goes through the
    # "no collision" branch.
    dest_folder = os.path.join(ra["mount_base"], "2026", "2026-07-03")
    os.makedirs(dest_folder, exist_ok=True)
    stale_target = os.path.join(dest_folder, "DSC_0001.jpg")
    Image.new("RGB", (16, 16), "red").save(stale_target)
    pinned_mtime = datetime(2026, 7, 3, 10, 0, 0).timestamp()
    os.utime(stale_target, (pinned_mtime, pinned_mtime))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    scanner.scan(ra["mount_base"], db)
    _mark_exif_extracted(db)
    stale_row = db.conn.execute(
        "SELECT id, file_hash FROM photos WHERE filename = ?",
        ("DSC_0001.jpg",),
    ).fetchone()
    assert stale_row is not None
    stale_hash = stale_row["file_hash"]

    os.remove(stale_target)

    # Card file with byte-different content but the SAME mtime the stale
    # row remembers, so shutil.copy2 in the fake rsync lands bytes that
    # match the incremental fast path's "unchanged" trigger.
    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "green"),
    ])

    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra, verify_by_hash=True,
        ),
    )

    assert result["failed"] == 0, result
    assert result["copied"] == 1, result
    assert result["safe_to_format"] is True, result
    updated_row = db.conn.execute(
        "SELECT file_hash FROM photos WHERE filename = ?",
        ("DSC_0001.jpg",),
    ).fetchone()
    assert updated_row["file_hash"] != stale_hash, (
        "stale catalog row was not refreshed after a fresh landing at the "
        "same path with a matching mtime; the batch scan's incremental "
        "fast path skipped the freshly transferred file"
    )


def test_remote_import_scans_adopted_duplicate_in_mixed_batch(
        tmp_path, monkeypatch):
    """A retry / crash-recovery batch that (a) copies one fresh file AND
    (b) finds a byte-identical file already on the mount for a different
    card file must catalog BOTH: the fresh copy AND the adopted duplicate.

    Without adding adopted mount paths to the restricted scan's
    ``restrict_files`` set, ``landed_paths`` alone scopes the scan so
    tightly that the adopted-but-uncataloged mount file is left without
    a photo row, while ``copied + skipped_duplicate == discovered`` still
    lets a verified remote run report ``safe_to_format=True`` for an
    invisible file. See PR #1113 review."""
    import shutil as _shutil

    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    # Both files belong to the same destination batch (same date), so
    # they share a single fresh scan call.
    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])

    # Pre-seed the mount destination with DSC_0001 (crash-recovery: a
    # prior run wrote it but died before catalog). Duplicates-index
    # (skip_duplicates=True default) sees no cataloged twin for it, so
    # the collision loop's on-disk hash-match branch fires and adopts
    # the mount file. DSC_0002 is a fresh copy this run.
    dest_folder = os.path.join(
        ra["mount_base"], "2026", "2026-07-03",
    )
    os.makedirs(dest_folder, exist_ok=True)
    seeded = os.path.join(dest_folder, "DSC_0001.jpg")
    _shutil.copy2(str(card / "DSC_0001.jpg"), seeded)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra, verify_by_hash=True,
        ),
    )

    assert result["failed"] == 0, result
    assert result["copied"] == 1, result       # DSC_0002 fresh
    assert result["skipped_duplicate"] == 1, result  # DSC_0001 adopted

    # BOTH files are cataloged at the mount path (the adopted duplicate
    # is not orphaned).
    rows = _photo_rows(db)
    row_paths = {os.path.join(r["folder_path"], r["filename"]) for r in rows}
    assert seeded in row_paths, row_paths
    assert os.path.join(dest_folder, "DSC_0002.jpg") in row_paths, row_paths

    # The pre-existing-but-now-adopted file is included in the run's
    # photo_ids so the after-import chaining hook processes it.
    adopted_row = next(
        r for r in rows if os.path.join(r["folder_path"], r["filename"]) == seeded
    )
    assert adopted_row["id"] in result["photo_ids"], result["photo_ids"]


def test_remote_adopted_only_scan_failure_counts_each_file_once(
        tmp_path, monkeypatch):
    """A failed adopted-only scan reports the source file exactly once.

    The validation pass below the scan already converts every adopted path
    without a catalog row from ``skipped_duplicate`` to ``failed``. A second
    folder-level failure would inflate the terminal ledger to N+1 entries.
    """
    import shutil as _shutil

    import scanner as scanner_module
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])
    dest_folder = os.path.join(
        ra["mount_base"], "2026", "2026-07-03",
    )
    os.makedirs(dest_folder, exist_ok=True)
    _shutil.copy2(
        str(card / "DSC_0001.jpg"),
        os.path.join(dest_folder, "DSC_0001.jpg"),
    )

    def failing_scan(*args, **kwargs):
        raise OSError("simulated adopted-only catalog scan failure")

    monkeypatch.setattr(scanner_module, "scan", failing_scan)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra, verify_by_hash=True,
        ),
    )

    assert result["discovered"] == 1, result
    assert result["copied"] == 0, result
    assert result["skipped_duplicate"] == 0, result
    assert result["failed"] == 1, result
    assert len(result["unsafe_files"]) == 1, result
    assert result["safe_to_format"] is False, result


def test_remote_import_result_carries_imported_photo_ids(
        tmp_path, monkeypatch):
    """The after-import chaining hook builds its process-job collection
    from ``result['photo_ids']``. The remote path must populate this the
    same way the local path does: freshly cataloged mount rows go in;
    duplicate-only imports return an empty list so the hook falls into
    "no new photos" instead of enqueueing an empty process run. See PR
    #1113 review."""
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # First run: two fresh copies -> two photo_ids in the result.
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra, verify_by_hash=True,
        ),
    )
    assert result["failed"] == 0, result
    assert result["copied"] == 2, result
    all_ids = sorted(r["id"] for r in _photo_rows(db))
    assert sorted(result["photo_ids"]) == all_ids
    assert len(all_ids) == 2

    # Duplicates-only rerun: present but empty — chaining hook must skip
    # into "no new photos" rather than enqueue an empty process run.
    rerun_result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra, verify_by_hash=True,
        ),
    )
    assert rerun_result["photo_ids"] == []
    assert rerun_result["skipped_duplicate"] == 2


def test_remote_import_null_scan_hash_but_mount_matches_still_ok(
        tmp_path, monkeypatch):
    """When scan() creates the photo row but leaves ``file_hash`` NULL —
    e.g. scanner's hash step was skipped or the row survives from a prior
    partial scan — the remote path must fall back to re-hashing the
    mount file (mirroring the local path's ``_rehash_dest_or_none``)
    instead of trusting the stamp behind ``verify_by_hash`` on a row
    whose bytes we haven't confirmed. When the re-hash matches the
    source, accept the landing and stamp ``ok``. Guards against
    regression on the fresh path where the mount is fine. See PR #1113
    review."""
    import scanner as _scanner
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    # Fake rsync copies card -> mount with matching bytes.
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])

    db_path = str(tmp_path / "test.db")

    # Wrap scan() to null out the file_hash the scanner wrote, simulating
    # the "row exists but hash unknown" case Codex flagged.
    orig_scan = _scanner.scan

    def scan_then_null_hash(destination, db_arg, **kw):
        rv = orig_scan(destination, db_arg, **kw)
        db_arg.conn.execute(
            "UPDATE photos SET file_hash = NULL "
            "WHERE filename = 'DSC_0001.jpg'"
        )
        db_arg.conn.commit()
        return rv

    monkeypatch.setattr(_scanner, "scan", scan_then_null_hash)

    db = Database(db_path)
    ws_id = db._active_workspace_id
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra, verify_by_hash=True,
        ),
    )

    # Rehash matched the source hash, so the landing is accepted and
    # stamped ok. copied=1, failed=0.
    assert result["failed"] == 0, result
    assert result["copied"] == 1, result
    assert result["safe_to_format"] is True, result
    rows = {r["filename"]: r for r in _photo_rows(db)}
    assert rows["DSC_0001.jpg"]["hash_status"] == "ok", dict(
        rows["DSC_0001.jpg"])


def test_remote_import_null_scan_hash_with_stale_mount_fails(
        tmp_path, monkeypatch):
    """The key Codex case: scan() leaves ``file_hash`` NULL AND the
    mount file the row points at is unreadable/gone/different by the
    time we try to confirm it. Without a re-hash fallback the code would
    fall through to stamp ``hash_status='ok'`` under ``verify_by_hash``
    on a row whose bytes we never confirmed (the NAS checksum only
    proves the card bytes reached the SSH target, not that the mount
    path holds those bytes). Must fail the landing and keep
    safe_to_format False. See PR #1113 review."""
    import scanner as _scanner
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])

    db_path = str(tmp_path / "test.db")

    # Wrap scan() to null the file_hash AND delete the mount file so the
    # rehash fallback returns None.
    orig_scan = _scanner.scan
    mount_file = os.path.join(
        ra["mount_base"], "2026", "2026-07-03", "DSC_0001.jpg",
    )

    def scan_then_null_and_wipe(destination, db_arg, **kw):
        rv = orig_scan(destination, db_arg, **kw)
        db_arg.conn.execute(
            "UPDATE photos SET file_hash = NULL "
            "WHERE filename = 'DSC_0001.jpg'"
        )
        db_arg.conn.commit()
        if os.path.exists(mount_file):
            os.unlink(mount_file)
        return rv

    monkeypatch.setattr(_scanner, "scan", scan_then_null_and_wipe)

    db = Database(db_path)
    ws_id = db._active_workspace_id
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra, verify_by_hash=True,
        ),
    )

    assert result["copied"] == 0, result
    assert result["failed"] == 1, result
    assert result["safe_to_format"] is False, result
    assert any(
        "scan wrote no mount row hash" in u["reason"]
        for u in result["unsafe_files"]
    ), result["unsafe_files"]
    # No hash_status='ok' stamp — the guard must have short-circuited
    # before update_photo_hash_check ran.
    rows = {r["filename"]: r for r in _photo_rows(db)}
    if "DSC_0001.jpg" in rows:
        assert rows["DSC_0001.jpg"]["hash_status"] != "ok", dict(
            rows["DSC_0001.jpg"])


def test_remote_import_paired_jpeg_no_verify_fails_on_mount_mismatch(
        tmp_path, monkeypatch):
    """Companion parity with the non-companion branch: without
    ``verify_by_hash`` the non-companion path still cross-checks the
    scanned row's ``file_hash`` against ``src_hash`` as a stale-mount
    catalog-integrity guard. Paired JPEGs (whose own row is deleted by
    pair-merge) must get the same protection — otherwise a stale/
    misconfigured mount with same-named but different JPEG bytes would
    be enqueued for after-import processing against the wrong companion
    even in no-verify mode. See PR #1113 review."""
    import move as _move
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)

    # Seed an existing RAW at the mount + catalog row so scan()'s
    # pair-merge will fold the just-landed JPEG into the RAW's
    # companion_path.
    mount_dir = os.path.join(ra["mount_base"], "2026", "2026-07-03")
    os.makedirs(mount_dir, exist_ok=True)
    raw_seed = os.path.join(mount_dir, "_seed.jpg")
    Image.new("RGB", (16, 16), "red").save(raw_seed)
    raw_bytes = open(raw_seed, "rb").read() + b"RAW-SENSOR-DATA"
    os.unlink(raw_seed)
    raw_path = os.path.join(mount_dir, "DSC_0800.NEF")
    with open(raw_path, "wb") as f:
        f.write(raw_bytes)

    # Fake rsync writes WRONG bytes for the JPEG at the mount (the
    # stale/misconfigured-mount stand-in Codex called out).
    def fake_rsync_wrong_bytes(
            src_path, dest_spec, rsync_flags, total_files,
            progress_cb, rsync_bin="rsync", extra_args=None,
            src_specs=None, src_specs_dest_is_dir=True, **kw):
        calls["rsync"].append({
            "src_specs": list(src_specs or []),
            "extra_args": list(extra_args or []),
        })
        ssh_path = dest_spec.split(":", 1)[1]
        rel = os.path.relpath(ssh_path, ra["ssh_base"])
        if src_specs_dest_is_dir:
            mount_dst = os.path.join(ra["mount_base"], rel)
            os.makedirs(mount_dst, exist_ok=True)
            for s in src_specs:
                Image.new("RGB", (16, 16), "yellow").save(
                    os.path.join(mount_dst, os.path.basename(s)))
        else:
            mount_file = os.path.join(ra["mount_base"], rel)
            os.makedirs(os.path.dirname(mount_file), exist_ok=True)
            Image.new("RGB", (16, 16), "yellow").save(mount_file)
        return (0, "", False)

    monkeypatch.setattr(_move, "_run_rsync_streamed", fake_rsync_wrong_bytes)
    monkeypatch.setattr(_move, "_remote_mkdir_p", lambda r, p: (True, ""))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (mount_dir, os.path.basename(mount_dir)),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_hash) VALUES (?, ?, '.nef', ?, ?)",
        (fid, "DSC_0800.NEF", len(raw_bytes), "deadbeef" * 8),
    )
    db.conn.commit()

    card = _make_card(tmp_path, [
        ("DSC_0800.jpg", datetime(2026, 7, 3, 10, 0, 0), "green"),
    ])

    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra, verify_by_hash=False,
        ),
    )

    # Even without verify_by_hash, the companion-branch mount-hash
    # cross-check must fire — same guard the non-companion branch runs.
    assert result["copied"] == 0, result
    assert result["failed"] == 1, result
    assert result["safe_to_format"] is False, result
    assert any(
        "paired companion mount bytes" in u["reason"]
        and "source hash" in u["reason"]
        for u in result["unsafe_files"]
    ), result["unsafe_files"]


def test_result_carries_imported_photo_ids(tmp_path):
    """The after-import chaining hook builds the process job's collection
    from the freshly imported rows; the result must name them."""
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 4, 9, 0, 0), "green"),
    ])
    archive = tmp_path / "archive"

    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(archive),
    ))

    all_ids = sorted(r["id"] for r in _photo_rows(db))
    assert sorted(result["photo_ids"]) == all_ids
    assert len(all_ids) == 2

    # Duplicates-only rerun: present but empty — the chaining hook skips
    # with "no new photos" instead of enqueueing an empty process run.
    from import_job import run_import_job

    rerun_result = run_import_job(
        _make_job(), FakeRunner(), str(tmp_path / "test.db"), ws_id,
        ImportParams(sources=[str(card)], destination=str(archive)),
    )
    assert rerun_result["photo_ids"] == []
    assert rerun_result["skipped_duplicate"] == 2


def test_progress_events_carry_live_per_folder_counts(tmp_path):
    """The Import page renders per-folder progress from the SSE stream;
    an in-flight event mid-run must already show nonzero counts for the
    folder being copied — not just at completion (transparency rule:
    never fake per-folder progress from stale counters)."""
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
        ("DSC_0003.jpg", datetime(2026, 7, 4, 9, 0, 0), "blue"),
        ("DSC_0004.jpg", datetime(2026, 7, 4, 9, 5, 0), "white"),
    ])
    runner = FakeRunner()
    job = _make_job()
    include_paths = {str(path) for path in card.iterdir()}
    _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(tmp_path / "archive"),
        include_paths=include_paths, previewed_count=4, checked_count=4,
    ), runner=runner, job=job)

    progress_folder_totals = []
    eta_events = []
    for (_, evt, data) in runner.events:
        if evt != "progress" or "folders" not in data:
            continue
        if "eta_state" in data:
            eta_events.append(data)
        total_copied = sum(
            c.get("copied", 0) for c in data["folders"].values()
        )
        progress_folder_totals.append(total_copied)

    assert progress_folder_totals, "no progress event carried folders"
    # Some event fired strictly mid-run: after the first copy landed but
    # before the last one did.
    assert any(0 < t < 4 for t in progress_folder_totals), (
        progress_folder_totals
    )
    assert any(e["eta_state"] == "estimating" for e in eta_events)
    assert eta_events[-1]["eta_state"] == "ready"
    assert eta_events[-1]["eta_settled"] == 4
    assert eta_events[-1]["eta_seconds"] == 0.0
    assert "eta_rate_per_min" not in job["progress"]


def test_remote_import_links_alias_spelled_twin_folder(tmp_path, monkeypatch):
    """A verified duplicate skip whose twin was cataloged through a symlink
    alias of the mount base is accepted via ``_path_under_destination``'s
    realpath check. The remote path links that existing alias-spelled
    catalog row directly. See PR #1113 review."""
    if not hasattr(os, "symlink"):
        return
    from import_dedup import compute_file_hash as _hash
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    # Card holds one photo whose bytes ALREADY exist under the real mount
    # base in an "unsorted" folder.
    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])
    card_file = str(card / "DSC_0001.jpg")
    src_hash = _hash(card_file)

    real_mount_base = ra["mount_base"]
    twin_folder_real = os.path.join(real_mount_base, "unsorted")
    os.makedirs(twin_folder_real, exist_ok=True)
    twin_path_real = os.path.join(twin_folder_real, "DSC_0001.jpg")
    import shutil as _shutil
    _shutil.copy2(card_file, twin_path_real)

    # Alias root points at the same physical directory via symlink. The
    # twin's cataloged folder path is spelled through the alias — NOT
    # lexically under the mount base. Direct linking is independent of that
    # spelling and does not call scan() on the alias path.
    try:
        alias_root = str(tmp_path / "mount_alias")
        os.symlink(real_mount_base, alias_root)
    except OSError:
        # No symlink support / permission — skip: the failure mode this
        # test proves needs a real alias on disk.
        return
    alias_twin_folder = os.path.join(alias_root, "unsorted")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (alias_twin_folder, os.path.basename(alias_twin_folder)),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_hash) VALUES (?, ?, '.jpg', ?, ?)",
        (fid, "DSC_0001.jpg", os.path.getsize(twin_path_real), src_hash),
    )
    db.conn.commit()
    assert alias_twin_folder not in _ws_linked_folder_paths(db, ws_id)

    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=real_mount_base,
            remote_target=ra, verify_by_hash=True,
        ),
    )

    # rsync stayed silent — card accepted as a duplicate.
    assert calls["rsync"] == [], calls["rsync"]
    assert result["skipped_duplicate"] == 1, result
    # The alias-spelled twin folder MUST be linked into the active
    # workspace through its existing catalog row.
    assert result["failed"] == 0, result
    assert result["safe_to_format"] is True, result
    linked_after = _ws_linked_folder_paths(db, ws_id)
    assert alias_twin_folder in linked_after, sorted(linked_after)


def test_remote_import_links_case_only_twin_folder(tmp_path, monkeypatch):
    """On a case-insensitive destination (macOS APFS/HFS+, SMB, FAT), a
    cataloged twin folder whose stored path differs from ``destination``
    only by case is accepted via ``_path_under_destination``'s casefold
    check. But scanner's ``_ensure_folder`` walks ``Path`` parents until
    they *lexically* (case-sensitive string equality, independent of the
    underlying filesystem) equal the scan root — a restrict_dir like
    ``/volumes/nas/…`` under root ``/Volumes/NAS/…`` recurses toward
    ``/`` and marks the verified duplicate-only remote import failed/
    unsafe before the workspace link is created. The dup-link split must
    route case-only aliases through the direct-link path (the same code
    path as symlink aliases), not scan them. See PR #1113 review.
    """
    import import_job as _ij
    from import_dedup import compute_file_hash as _hash
    from import_job import ImportParams, run_import_job

    # Force the case-insensitive destination code path regardless of the
    # test host filesystem (Linux ext4 is case-sensitive). This is the
    # module-level constant _dest_ci consults; sys.platform monkeypatch
    # wouldn't work because the constant is bound at import time.
    monkeypatch.setattr(_ij, "_CASE_INSENSITIVE_PLATFORM", True)

    # Build the remote archive rooted under a case-neutral parent so we can
    # spell the destination and the twin's DB path with a differently-cased
    # ancestor. The bug only triggers when the CASE MISMATCH lies in the
    # prefix common to destination and the twin (a differently-cased leaf
    # still matches the literal prefix check inside ``lex_dup_dirs``).
    lower_root = tmp_path / "archive_root"
    lower_root.mkdir(exist_ok=True)
    upper_root = tmp_path / "ARCHIVE_ROOT"
    # On a case-insensitive host FS (macOS APFS/HFS+, Windows NTFS in the
    # default configuration) the two spellings resolve to a single physical
    # directory, so the second mkdir would raise FileExistsError; the
    # existing dir already serves both cases. On case-sensitive Linux the
    # dir does not yet exist and this creates the second physical dir.
    upper_root.mkdir(exist_ok=True)
    ra = _remote_archive_for(lower_root)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])
    card_file = str(card / "DSC_0001.jpg")
    src_hash = _hash(card_file)

    real_mount_base = ra["mount_base"]  # lower_root/mount
    # Twin folder cataloged with an ANCESTOR case-mismatch: the destination
    # is ``<tmp>/archive_root/mount`` (lowercase), and the twin's DB path
    # lives under ``<tmp>/ARCHIVE_ROOT/mount/unsorted``. On a real
    # case-insensitive volume these are the same physical directory; on
    # the case-sensitive Linux test filesystem we materialize both real
    # directories so ``os.path.isdir`` succeeds. The bug fires purely in
    # the string comparison inside the dup-link classification: the twin's
    # DB path casefold-matches ``destination`` (routing it to ``lex_dup_
    # dirs``), but its literal prefix doesn't match — passing that path to
    # ``scan(destination, restrict_dirs=[…])`` would blow scanner's
    # parent-walk.
    lex_mount_case = str(upper_root / "mount")
    os.makedirs(lex_mount_case, exist_ok=True)
    twin_folder_case = os.path.join(lex_mount_case, "unsorted")
    os.makedirs(twin_folder_case, exist_ok=True)
    twin_path_case = os.path.join(twin_folder_case, "DSC_0001.jpg")
    import shutil as _shutil
    _shutil.copy2(card_file, twin_path_case)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (twin_folder_case, os.path.basename(twin_folder_case)),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_hash) VALUES (?, ?, '.jpg', ?, ?)",
        (fid, "DSC_0001.jpg", os.path.getsize(twin_path_case), src_hash),
    )
    db.conn.commit()
    assert twin_folder_case not in _ws_linked_folder_paths(db, ws_id)

    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=real_mount_base,
            remote_target=ra, verify_by_hash=True,
        ),
    )

    # rsync stayed silent — card accepted as a duplicate.
    assert calls["rsync"] == [], calls["rsync"]
    assert result["skipped_duplicate"] == 1, result
    # The case-only-alias twin folder MUST be linked into the active
    # workspace via the direct-link path, independent of path case.
    assert result["failed"] == 0, result
    assert result["safe_to_format"] is True, result
    linked_after = _ws_linked_folder_paths(db, ws_id)
    assert twin_folder_case in linked_after, sorted(linked_after)


def test_remote_import_records_landings_in_intra_run_checker(
        tmp_path, monkeypatch):
    """With ``skip_duplicates=True`` (the default) and ``verify_by_hash``
    true, a remote batch that contains two byte-identical files with
    different basenames must record the first landing with the intra-run
    duplicate checker so the second file is recognized as an intra-run
    duplicate — otherwise the checker only ever sees the pre-run catalog,
    and both files get rsynced/cataloged separately.

    Mirrors the local path's ``_record_checker`` after every accepted
    landing. See PR #1113 review."""
    import shutil as _shutil

    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    # Two files, same bytes (same color -> same PIL-encoded JPEG), same
    # date (same batch), different basenames.
    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])
    _shutil.copy2(str(card / "DSC_0001.jpg"), str(card / "DSC_0002.jpg"))
    ts = datetime(2026, 7, 3, 10, 0, 0).timestamp()
    os.utime(str(card / "DSC_0002.jpg"), (ts, ts))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra, verify_by_hash=True,
        ),
    )

    # Exactly one file lands on the NAS; the second is an intra-run
    # duplicate — without the record() call, both would be rsynced.
    assert result["failed"] == 0, result
    assert result["copied"] == 1, result
    assert result["skipped_duplicate"] == 1, result

    # The rsync fake records each source it moved. Sum across all calls
    # must be exactly one card file (the flat batch may or may not
    # coalesce depending on collision handling, but the DEDUPED count is
    # what the checker enforces).
    all_transferred = [
        s for c in calls["rsync"] for s in c["src_specs"]
    ]
    assert len(all_transferred) == 1, all_transferred


def test_remote_import_queues_both_when_skip_duplicates_disabled(
        tmp_path, monkeypatch):
    """With ``skip_duplicates=False`` and ``verify_by_hash`` true, a remote
    batch that contains two byte-identical files with different basenames
    must rsync/catalog BOTH files instead of counting the second as
    ``skipped_duplicate``.

    The remote intra-batch content-dedup shortcut hashes ``src_hash``
    regardless of whether ``DuplicateChecker`` is active, so before the
    fix the second file was silently skipped and — because
    ``copied + skipped_duplicate == discovered`` — a verified run could
    still report ``safe_to_format=True`` without an off-card row for the
    second file. The local path only dedupes intra-batch same-content
    twins through ``DuplicateChecker``, so disabling duplicate skipping
    must queue both files here too. See PR #1113 review."""
    import shutil as _shutil

    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    # Two files, same bytes, same date (same batch), different basenames.
    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])
    _shutil.copy2(str(card / "DSC_0001.jpg"), str(card / "DSC_0002.jpg"))
    ts = datetime(2026, 7, 3, 10, 0, 0).timestamp()
    os.utime(str(card / "DSC_0002.jpg"), (ts, ts))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra, verify_by_hash=True,
            skip_duplicates=False,
        ),
    )

    # Both files must land as ``copied``; nothing may be counted as
    # ``skipped_duplicate`` because the user opted out of dedup.
    assert result["failed"] == 0, result
    assert result["copied"] == 2, result
    assert result["skipped_duplicate"] == 0, result

    # And both card files must actually reach the rsync transport (not
    # just the counters), otherwise the off-card row for the second file
    # is fictional.
    all_transferred = sorted({
        os.path.basename(s)
        for c in calls["rsync"] for s in c["src_specs"]
    })
    assert all_transferred == ["DSC_0001.jpg", "DSC_0002.jpg"], (
        all_transferred, calls["rsync"])

    # Both source paths must end up cataloged as distinct photo rows on
    # the mount so ``safe_to_format=True`` isn't a lie.
    rows = db.conn.execute(
        "SELECT filename FROM photos ORDER BY filename",
    ).fetchall()
    filenames = sorted(r["filename"] for r in rows)
    assert len(filenames) == 2, filenames
    # Basenames may include a numeric suffix if rsync flat-lands both to
    # the same mount folder — what matters is that there are two distinct
    # rows and both card files were transferred.
    assert result["safe_to_format"] is True, result


def test_remote_import_refuses_when_mount_root_absent(tmp_path, monkeypatch):
    """When a saved remote target's local mount root (``/Volumes/NAS``,
    ``/mnt/NAS``, ``/media/user/NAS``) is not currently mounted, the
    remote import must fail before ``os.makedirs(dest_folder)`` creates a
    local shadow of the mount tree on the internal disk. The SSH rsync
    could still push to the NAS, but the batch scan then reads the empty
    shadow and leaves the import uncataloged; on macOS/Linux the shadow
    root can also prevent the real share from remounting. Mirrors the
    pipeline path's ``_missing_archive_mount_root`` preflight. See PR
    #1113 review."""
    import move as _move
    import pipeline_job as _pj
    from import_job import ImportParams, run_import_job

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])

    # Build a resolved-archive dict whose mount base sits under a
    # (simulated) unmounted volume. The pipeline helper only fires for
    # the specific ``/Volumes/*`` / ``/mnt/*`` / ``/media/*/*`` shapes,
    # so monkeypatch it to report our fake mount base as missing —
    # analogous to how test_pipeline_api's
    # ``test_pipeline_local_processing_rejects_missing_archive_mount_root``
    # stubs the helper.
    fake_mount_base = str(tmp_path / "Volumes_NAS_Photos")
    target = {
        "id": "nas1", "name": "NAS", "host": "nas", "user": "me",
        "port": 22, "ssh_key": "", "bwlimit_kbps": 0,
        "remote_path": "/volume1/Photography",
        "mount_path": fake_mount_base,
    }
    from move import build_remote_move_spec
    spec = build_remote_move_spec(target, "", "/usr/bin/rsync")
    ra = {
        "target": target,
        "rsync_bin": "/usr/bin/rsync",
        "remote": spec,
        "ssh_base": target["remote_path"],
        "mount_base": fake_mount_base,
    }

    monkeypatch.setattr(
        _pj, "_missing_archive_mount_root",
        lambda path: (
            "/Volumes/NAS"
            if path.startswith(fake_mount_base) else None
        ),
    )

    # rsync must NEVER run — the batch is rejected before makedirs and
    # before any transport call.
    def _refuse_rsync(*a, **kw):
        raise AssertionError(
            "rsync should not run when the mount root is absent"
        )

    monkeypatch.setattr(_move, "_run_rsync_streamed", _refuse_rsync)
    monkeypatch.setattr(_move, "_remote_mkdir_p", lambda r, p: (True, ""))
    monkeypatch.setattr(
        _move, "remote_verify_files",
        lambda *a, **kw: (
            (_ for _ in ()).throw(
                AssertionError("verify should not run either")
            )
        ),
    )

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra, verify_by_hash=True,
        ),
    )

    # Every discovered card file lands in ``failed`` with the
    # mount-root reason; the guarded ``os.makedirs`` never ran, so no
    # shadow tree exists under the fake mount base.
    assert result["failed"] == 2, result
    assert result["copied"] == 0, result
    assert result["skipped_duplicate"] == 0, result
    assert result["safe_to_format"] is False, result
    # Filter out the ``<remote>`` honesty-gate marker so a regression that
    # drops the per-file mount-root reason (or funnels everything into a
    # ``<remote>`` entry) does not make the ``all()`` below vacuously true
    # over an empty generator; assert the filtered subset is non-empty first.
    non_remote = [
        u for u in result["unsafe_files"] if u["path"] != "<remote>"
    ]
    assert non_remote, result["unsafe_files"]
    assert all(
        "/Volumes/NAS" in u["reason"] and "not available" in u["reason"]
        for u in non_remote
    ), result["unsafe_files"]
    assert not os.path.exists(fake_mount_base), (
        f"shadow directory was created at {fake_mount_base}: "
        "the preflight failed to prevent os.makedirs"
    )


def test_remote_import_missing_mount_root_emits_archive_unavailable(
        tmp_path, monkeypatch):
    """Spec decision 3: the missing-mount-root batch refusal must emit
    the specific ``"{rel}: archive unavailable"`` phase (the local
    path's honest signal) instead of the generic copied/present summary
    the remote path historically reused for this failure."""
    import pipeline_job as _pj
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)
    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])
    # Same stub shape as ``test_remote_import_refuses_when_mount_root_absent``:
    # report the mount root missing only for paths under this run's mount
    # base (the import is a run-time function-level import in the remote
    # body, so patching the pipeline_job module attribute intercepts it).
    monkeypatch.setattr(
        _pj, "_missing_archive_mount_root",
        lambda path: (
            "/Volumes/GoneShare"
            if path.startswith(ra["mount_base"]) else None
        ),
    )

    runner = FakeRunner()
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    result = run_import_job(
        _make_job(), runner, db_path, db._active_workspace_id,
        ImportParams(sources=[str(card)], destination=ra["mount_base"],
                     remote_target=ra, verify_by_hash=True))

    assert result["failed"] == 1, result
    assert "is not available" in result["unsafe_files"][0]["reason"]
    phases = [d["phase"] for _, kind, d in runner.events
              if kind == "progress"]
    assert any(p.endswith(": archive unavailable") for p in phases), phases
    # Guard against a refactor emitting both the honest refusal phase
    # and the generic copied/present summary for the same batch.
    assert not any("already present" in p for p in phases), phases
    assert calls["rsync"] == []


def test_remote_import_case_only_basename_collision_on_ci_destination(
        tmp_path, monkeypatch):
    """On a case-insensitive destination (macOS APFS/HFS+, SMB, FAT/exFAT),
    two DIFFERENT card files whose basenames differ only by case (e.g.
    ``IMG_0001.JPG`` and ``img_0001.jpg``) collapse onto the same effective
    on-disk file. The intra-batch collision map ``claimed_basenames`` must
    key case-foldedly on such destinations so the second file is detected
    as colliding and advanced to a numeric-suffixed name, otherwise both
    are queued as distinct entries into the same flat rsync — where
    ``--ignore-existing`` drops the second and the later catalog/hash
    validation fails instead of the local path's rename-to-suffix
    behaviour. See PR #1113 review."""
    import import_job as _ij
    from import_job import ImportParams, run_import_job

    # Skip on a case-insensitive host filesystem (macOS APFS/HFS+, Windows
    # NTFS in the default configuration): the two case-only-different card
    # files can't both exist on such a filesystem — writing ``img_0001.jpg``
    # after ``IMG_0001.JPG`` overwrites the first, so discovery only sees
    # ONE file and the destination-side collision loop we're trying to
    # exercise has nothing to collide against. The production behaviour
    # itself is unaffected (the collision loop keys ``claimed_basenames``
    # case-foldedly whenever ``_dest_ci`` is true regardless of the card
    # filesystem); this is a test-fixture limitation, not a code bug.
    _probe_dir = tmp_path / "_case_probe"
    _probe_dir.mkdir()
    (_probe_dir / "CaseProbe.txt").write_text("")
    if (_probe_dir / "caseprobe.txt").exists():
        import pytest
        pytest.skip(
            "case-insensitive host filesystem cannot hold two card files "
            "whose basenames differ only by case",
        )

    # Force the case-insensitive destination code path regardless of the
    # test host filesystem (Linux ext4 is case-sensitive). The module-
    # level constant _dest_ci consults; sys.platform monkeypatch wouldn't
    # work because the constant is bound at import time.
    monkeypatch.setattr(_ij, "_CASE_INSENSITIVE_PLATFORM", True)

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    # Two DIFFERENT-content files whose basenames differ only by case,
    # same date (same batch). Distinct colors -> distinct bytes so the
    # intra-batch same-content dedup shortcut can't hide the collision.
    card = _make_card(tmp_path, [
        ("IMG_0001.JPG", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("img_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "blue"),
    ])

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra, verify_by_hash=True,
        ),
    )

    # Both must land as ``copied``, neither as ``skipped_duplicate`` (the
    # bytes are different so intra-batch content dedup doesn't apply).
    assert result["failed"] == 0, result
    assert result["copied"] == 2, result
    assert result["skipped_duplicate"] == 0, result

    # Both card files must actually reach the transport under DISTINCT
    # dest basenames whose case-folded forms are also distinct (the whole
    # point of the fix: without it both share the effective receiver path
    # on a CI destination).
    all_transferred_dest = []
    for c in calls["rsync"]:
        if c["dest_is_dir"]:
            for s in c["src_specs"]:
                all_transferred_dest.append(os.path.basename(s))
        else:
            # File dest: name is the last path segment of dest_spec.
            all_transferred_dest.append(
                c["dest_spec"].rsplit("/", 1)[-1])
    folded = [n.casefold() for n in all_transferred_dest]
    assert len(folded) == 2, (all_transferred_dest, calls["rsync"])
    assert len(set(folded)) == 2, (
        "case-folded dest basenames must be distinct after the "
        "collision loop; both files landed under the same effective "
        "receiver name",
        all_transferred_dest,
    )

    # And a collision-renamed rsync call MUST have fired (dest_is_dir=
    # False) — that's the rename-to-suffix path the local import uses.
    # Without the case-fold fix both files would go into a single flat
    # rsync (dest_is_dir=True) and be silently coalesced by the CI
    # receiver.
    renamed_calls = [c for c in calls["rsync"] if not c["dest_is_dir"]]
    assert renamed_calls, (
        "expected a collision-renamed single-file rsync call for the "
        "case-only basename collision",
        calls["rsync"],
    )

    # Two distinct rows on the mount whose (case-folded) filenames are
    # also distinct: the catalog reflects two off-card files, not one.
    rows = _photo_rows(db)
    assert len(rows) == 2, [dict(r) for r in rows]
    folded_rows = {r["filename"].casefold() for r in rows}
    assert len(folded_rows) == 2, [dict(r) for r in rows]
    assert result["safe_to_format"] is True, result


def test_remote_import_cancel_mid_batch_does_not_start_rsync(
        tmp_path, monkeypatch):
    """When Stop is requested inside the per-file queue-building loop of a
    remote import — after one or more files have been appended to
    ``to_transfer`` but before the per-batch rsync fires — the guard on
    ``if to_transfer:`` must keep the network transfer from starting. The
    remote path decouples "decide to copy" (the queue-building loop) from
    "actually copied" (the post-loop rsync), so the mid-batch cancel-break
    on its own leaves queued files that would still be sent by the block
    below. Queued files that never rsync stay on the card and get picked
    up by the next run. See PR #1113 review."""
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    # Two files in the SAME batch (same date -> same rel folder) so the
    # per-file queue loop iterates twice within one batch: file 1 gets
    # queued into ``to_transfer``, then the runner flips ``cancelled`` on
    # file 1's ``importing`` progress event, and file 2's iteration sees
    # the cancel and breaks. Without the guard, the rsync block below
    # would still copy file 1 after Stop was requested.
    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 10, 5, 0), "green"),
    ])

    # ``CancelAfterFirstBatchRunner`` matches its trigger fragment against
    # the progress event's ``phase`` and flips cancelled inside
    # ``push_event`` — so file 1's ``_emit(f"{rel}: importing", …)`` at
    # the top of its loop body cancels the runner before file 2's
    # iteration, which is exactly the "queued but not yet sent" race the
    # guard exists to close.
    runner = CancelAfterFirstBatchRunner("2026/2026-07-03: importing")

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    result = run_import_job(
        _make_job(), runner, db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra, verify_by_hash=True,
        ),
    )

    assert result["cancelled"] is True, result
    # No rsync call — neither the flat batch transfer nor a collision-
    # renamed single-file transfer may fire once ``cancelled`` is set.
    assert calls["rsync"] == [], calls["rsync"]
    # And no card->NAS verification either (verification only runs for
    # files that were actually transferred).
    assert calls["verify"] == 0, calls
    assert result["copied"] == 0, result
    assert result["failed"] == 0, result
    # A cancelled run must not report ``safe_to_format=True`` — the
    # honesty gate below already covers this, but assert it directly so
    # a regression that also drops the cancel guard is caught here.
    assert result["safe_to_format"] is False, result

    # Nothing landed on the mount either. The fake rsync copies each
    # src file into ``mount_base/rel`` — after cancel there must be no
    # such file under the batch's dest folder.
    dest_dir = os.path.join(ra["mount_base"], "2026", "2026-07-03")
    landed = (
        os.listdir(dest_dir) if os.path.isdir(dest_dir) else []
    )
    assert landed == [], (
        f"no card files should have landed on the mount after "
        f"cancellation, but found: {landed}"
    )


def test_remote_import_stop_kills_in_flight_rsync_batch(
        tmp_path, monkeypatch):
    """Stop must reach a running per-batch rsync. The job passes the
    runner's cancel signal into ``_run_rsync_streamed`` as ``cancel_check``
    (so the watchdog can kill the subprocess), and when the killed rsync
    returns nonzero the batch is treated as cancelled work — its files are
    NOT reported as per-file failures. Queued files stay on the card for
    the next run; whatever rsync landed before the kill is adopted by
    crash-recovery, exactly like a mid-batch crash."""
    import shutil

    import move as _move
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 10, 5, 0), "green"),
    ])

    runner = FakeRunner()
    job = _make_job()
    seen = {"cancel_check": None, "cancel_check_result": None}

    def killed_rsync(src_path, dest_spec, rsync_flags, total_files,
                     progress_cb, rsync_bin="rsync", extra_args=None,
                     src_specs=None, src_specs_dest_is_dir=True, **kw):
        seen["cancel_check"] = kw.get("cancel_check")
        # Stop arrives mid-transfer: flip the runner's cancel flag, land
        # only the first file (the kill interrupted the rest), and return
        # the killed subprocess's nonzero exit with timed_out=False.
        runner.cancelled_ids.add(job["id"])
        if seen["cancel_check"] is not None:
            seen["cancel_check_result"] = seen["cancel_check"]()
        ssh_path = dest_spec.split(":", 1)[1]
        rel = os.path.relpath(ssh_path, calls["_ssh_base"])
        mount_dir = os.path.join(calls["_mount_base"], rel)
        os.makedirs(mount_dir, exist_ok=True)
        shutil.copy2(
            src_specs[0],
            os.path.join(mount_dir, os.path.basename(src_specs[0])))
        return (-9, "", False)

    monkeypatch.setattr(_move, "_run_rsync_streamed", killed_rsync)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    result = run_import_job(
        job, runner, db_path, db._active_workspace_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra, verify_by_hash=True,
        ),
    )

    # The transfer received a live cancel_check wired to the runner.
    assert seen["cancel_check"] is not None, (
        "the per-batch rsync was started without a cancel_check — Stop "
        "cannot reach the subprocess")
    assert seen["cancel_check_result"] is True

    assert result["cancelled"] is True, result
    # A killed batch is cancelled work, not a pile of per-file failures.
    assert result["failed"] == 0, result
    assert not any(
        "rsync" in u["reason"] for u in result["unsafe_files"]
    ), result["unsafe_files"]
    # Nothing was verified/cataloged from the interrupted batch, and a
    # cancelled run never claims the card is safe to erase.
    assert result["copied"] == 0, result
    assert result["safe_to_format"] is False, result
    # And no catalog row exists for the interrupted batch — the file that
    # rsync landed before the kill is left for the NEXT run's crash-recovery
    # to adopt, not cataloged by this cancelled run.
    assert _photo_rows(db) == [], [dict(r) for r in _photo_rows(db)]


def test_remote_import_stop_between_renamed_transfers_keeps_completed_files(
        tmp_path, monkeypatch):
    """CHARACTERIZATION (spec: outcome-completeness invariant). Renamed
    files transfer one rsync each. A Stop after the first file's rsync
    returned success must keep that file's outcome (verified + cataloged)
    while the not-yet-transferred file produces neither a failure nor a
    landing — it stays on the card for the next run."""
    import move as _move
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    # Same capture date -> same batch; distinct colors -> distinct bytes.
    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 10, 5, 0), "green"),
    ])
    # Force collisions: different-byte files already at the template path.
    mount_day = Path(ra["mount_base"]) / "2026" / "2026-07-03"
    mount_day.mkdir(parents=True)
    for name in ("DSC_0001.jpg", "DSC_0002.jpg"):
        Image.new("RGB", (16, 16), "blue").save(str(mount_day / name))

    runner = FakeRunner()
    job = _make_job()
    base_fake = _move._run_rsync_streamed  # the harness fake installed above

    state = {"renamed_calls": 0}

    def stop_after_first_renamed(src_path, dest_spec, rsync_flags,
                                 total_files, progress_cb,
                                 rsync_bin="rsync", extra_args=None,
                                 src_specs=None,
                                 src_specs_dest_is_dir=True, **kw):
        assert not src_specs_dest_is_dir, (
            "expected only renamed (file-dest) transfers in this geometry")
        state["renamed_calls"] += 1
        rc = base_fake(src_path, dest_spec, rsync_flags, total_files,
                       progress_cb, rsync_bin=rsync_bin,
                       extra_args=extra_args, src_specs=src_specs,
                       src_specs_dest_is_dir=src_specs_dest_is_dir, **kw)
        # Stop arrives after this file completed.
        runner.cancelled_ids.add(job["id"])
        return rc

    monkeypatch.setattr(_move, "_run_rsync_streamed",
                        stop_after_first_renamed)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    result = run_import_job(
        job, runner, db_path, db._active_workspace_id,
        ImportParams(sources=[str(card)], destination=ra["mount_base"],
                     remote_target=ra, verify_by_hash=True))

    # Only the first renamed rsync ran; the loop observed Stop before the
    # second (import_job.py:2361-2364).
    assert state["renamed_calls"] == 1, state
    assert result["cancelled"] is True
    # The completed file keeps its outcome: verified and cataloged.
    assert result["copied"] == 1, result
    assert result["verified"] == 1, result
    suffixed = [(fn, hs) for _rel, fn, _fh, hs in
                _dest_photo_facts(db, ra["mount_base"])
                if fn.startswith("DSC_0001")]
    assert ("DSC_0001_1.jpg", "ok") in suffixed, suffixed
    # ...and the un-transferred file is neither failed nor landed (any
    # DSC_0002 catalog row — suffixed or not — would be a regression;
    # the pre-seeded mount files are never cataloged).
    assert result["failed"] == 0, result
    assert not any(fn.startswith("DSC_0002") for _rel, fn, _fh, _hs in
                   _dest_photo_facts(db, ra["mount_base"]))
    assert result["safe_to_format"] is False, result


def test_remote_import_stop_after_flat_batch_keeps_flat_outcomes(
        tmp_path, monkeypatch):
    """CHARACTERIZATION (spec: outcome-completeness invariant, mixed
    geometry). A batch that mixes non-colliding (flat) and colliding
    (renamed) files runs the flat rsync first (import_job.py:2341-2358),
    then loops the renamed files one rsync each. A Stop observed at the
    renamed-loop guard (import_job.py:2361-2364) — AFTER the flat rsync
    returned success — must keep the flat files' outcomes (verified and
    cataloged) rather than discarding them as cancelled work. The
    companion killed-flat test asserts ``copied==0`` when the flat
    rsync itself is the one killed; this test pins the OTHER direction
    so a unified ``flush_batch`` that treats ``cancelled=True`` as
    "throw away every outcome" cannot pass both."""
    import move as _move
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    # Same capture date -> same batch. DSC_0001 (no collision) goes down
    # the flat path; DSC_0002 collides and goes down the renamed path.
    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 10, 5, 0), "green"),
    ])
    mount_day = Path(ra["mount_base"]) / "2026" / "2026-07-03"
    mount_day.mkdir(parents=True)
    # Only DSC_0002 pre-exists at the destination with different bytes,
    # so ONLY that one gets suffixed to DSC_0002_1.jpg and takes the
    # renamed path. DSC_0001 stays flat.
    Image.new("RGB", (16, 16), "blue").save(
        str(mount_day / "DSC_0002.jpg"))

    runner = FakeRunner()
    job = _make_job()
    base_fake = _move._run_rsync_streamed  # harness fake installed above

    state = {"flat_calls": 0, "renamed_calls": 0}

    def stop_after_flat_batch(src_path, dest_spec, rsync_flags,
                              total_files, progress_cb,
                              rsync_bin="rsync", extra_args=None,
                              src_specs=None,
                              src_specs_dest_is_dir=True, **kw):
        rc = base_fake(src_path, dest_spec, rsync_flags, total_files,
                       progress_cb, rsync_bin=rsync_bin,
                       extra_args=extra_args, src_specs=src_specs,
                       src_specs_dest_is_dir=src_specs_dest_is_dir, **kw)
        if src_specs_dest_is_dir:
            # The flat batch just completed successfully. Stop arrives
            # between the flat batch and the first renamed rsync — the
            # exact race the renamed-loop guard is meant to close.
            state["flat_calls"] += 1
            runner.cancelled_ids.add(job["id"])
        else:
            state["renamed_calls"] += 1
        return rc

    monkeypatch.setattr(_move, "_run_rsync_streamed",
                        stop_after_flat_batch)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    result = run_import_job(
        job, runner, db_path, db._active_workspace_id,
        ImportParams(sources=[str(card)], destination=ra["mount_base"],
                     remote_target=ra, verify_by_hash=True))

    # The flat batch ran; the renamed loop observed Stop at its guard
    # (import_job.py:2361-2364) before firing its rsync.
    assert state["flat_calls"] == 1, state
    assert state["renamed_calls"] == 0, state
    assert result["cancelled"] is True, result

    # The completed flat file keeps its outcome: verified and cataloged.
    # This is the outcome a unified ``flush_batch`` implementation that
    # discards successful flat outcomes on ``cancelled=True`` would
    # silently drop.
    assert result["copied"] == 1, result
    assert result["verified"] == 1, result
    flat_rows = [(fn, hs) for _rel, fn, _fh, hs in
                 _dest_photo_facts(db, ra["mount_base"])
                 if fn.startswith("DSC_0001")]
    assert ("DSC_0001.jpg", "ok") in flat_rows, flat_rows

    # ...and the un-transferred renamed file is neither failed nor
    # landed (any DSC_0002 catalog row — suffixed or not — would be a
    # regression; the pre-seeded mount file is never cataloged).
    assert result["failed"] == 0, result
    assert not any(fn.startswith("DSC_0002") for _rel, fn, _fh, _hs in
                   _dest_photo_facts(db, ra["mount_base"]))
    # Cancelled runs never claim the card is safe to erase.
    assert result["safe_to_format"] is False, result


def test_remote_import_rsync_watchdog_does_not_block_on_pause(
        tmp_path, monkeypatch):
    """The rsync watchdog thread's cancel_check must be a non-blocking
    probe. Import jobs run pausable, and ``runner.is_cancelled()`` parks
    the caller inside ``wait_if_paused`` when a Pause is pending. Wiring
    the watchdog to that pause-aware method would silently disable both
    the stall watchdog and Stop while paused — rsync would keep copying
    at 50 KB/s until Resume/Cancel. Use ``cancellation_requested()``
    instead; pause is observed at the existing batch-boundary check."""
    import move as _move
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])

    class PauseAwareRunner(FakeRunner):
        """Mirror JobRunner's pause behaviour: is_cancelled blocks while
        paused; cancellation_requested does not."""

        def __init__(self):
            super().__init__()
            self.paused_ids = set()

        def is_cancelled(self, job_id):
            if job_id in self.paused_ids and job_id not in self.cancelled_ids:
                raise AssertionError(
                    "watchdog called the pause-aware is_cancelled and would "
                    "have blocked the stall/cancel watchdog thread during "
                    "Pause")
            return job_id in self.cancelled_ids

        def cancellation_requested(self, job_id):
            return job_id in self.cancelled_ids

    runner = PauseAwareRunner()
    job = _make_job()
    seen = {"probe_during_pause": None}

    def paused_rsync(src_path, dest_spec, rsync_flags, total_files,
                    progress_cb, rsync_bin="rsync", extra_args=None,
                    src_specs=None, src_specs_dest_is_dir=True, **kw):
        # A Pause arrives mid-transfer. The watchdog's cancel_check must
        # still be safe to call in that state — a blocking call here is
        # the exact defect the fix guards against.
        runner.paused_ids.add(job["id"])
        cancel_check = kw.get("cancel_check")
        if cancel_check is not None:
            seen["probe_during_pause"] = cancel_check()
        # Simulate a healthy (uncancelled) transfer completing normally.
        import shutil
        ssh_path = dest_spec.split(":", 1)[1]
        rel = os.path.relpath(ssh_path, calls["_ssh_base"])
        mount_dir = os.path.join(calls["_mount_base"], rel)
        os.makedirs(mount_dir, exist_ok=True)
        shutil.copy2(
            src_specs[0],
            os.path.join(mount_dir, os.path.basename(src_specs[0])))
        return (0, "", False)

    monkeypatch.setattr(_move, "_run_rsync_streamed", paused_rsync)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    # No exception ⇒ watchdog probed cancellation without blocking on the
    # pause request. The transfer completes on its own since nothing was
    # cancelled.
    result = run_import_job(
        job, runner, db_path, db._active_workspace_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra, verify_by_hash=False,
        ),
    )

    assert seen["probe_during_pause"] is False, (
        "watchdog's cancel_check returned something other than a "
        "non-blocking False during Pause")
    assert result["copied"] == 1, result


def test_remote_import_reports_per_file_transfer_progress(
        tmp_path, monkeypatch):
    """The per-batch rsync streams each transferred file; the job must
    surface that as sub-phase progress (``phase_current``/``phase_total``/
    ``phase_label`` — the same keys the scanner's metadata phase uses, so
    the bottom panel renders them as-is) instead of discarding the
    callback. The prepared-files counter (``current``/``total``) alone
    reads as "files completed" while the batch is still crossing the
    network — the UI transparency rule requires the real transfer count
    alongside it. The prep counter itself must NOT move during the
    transfer, and the transfer fields must not outlive the batch in
    ``job["progress"]``."""
    import move as _move
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    # Two sources carrying the SAME basename with different capture times
    # in the same date folder: the second queues as a collision-renamed
    # single-file rsync (DSC_0001_1.jpg), so the transfer counter must
    # span the flat batch AND the renamed transfer.
    card1 = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ], card_name="card1")
    card2 = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 5, 0), "green"),
    ], card_name="card2")

    real_fake = _move._run_rsync_streamed  # landing behavior from the seam

    def streaming_rsync(src_path, dest_spec, rsync_flags, total_files,
                        progress_cb, rsync_bin="rsync", extra_args=None,
                        src_specs=None, src_specs_dest_is_dir=True, **kw):
        rc, stderr, timed_out = real_fake(
            src_path, dest_spec, rsync_flags, total_files, None,
            rsync_bin=rsync_bin, extra_args=extra_args, src_specs=src_specs,
            src_specs_dest_is_dir=src_specs_dest_is_dir)
        if progress_cb is not None:
            for i, s in enumerate(src_specs, 1):
                progress_cb(i, total_files, os.path.basename(s),
                            "Copying files")
        return rc, stderr, timed_out

    monkeypatch.setattr(_move, "_run_rsync_streamed", streaming_rsync)

    runner = FakeRunner()
    job = _make_job()
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    result = run_import_job(
        job, runner, db_path, db._active_workspace_id,
        ImportParams(
            sources=[str(card1), str(card2)], destination=ra["mount_base"],
            remote_target=ra, verify_by_hash=True,
        ),
    )
    assert result["failed"] == 0, result
    assert result["copied"] == 2, result

    transfer_events = [
        data for _jid, etype, data in runner.events
        if etype == "progress" and "phase_current" in data
    ]
    assert transfer_events, (
        "no transfer progress events — the batch rsync's per-file stream "
        "is being discarded")
    for ev in transfer_events:
        assert ev["phase"].endswith(": transferring"), ev
        assert ev["phase_label"] == "Transferring batch", ev
        assert ev["phase_total"] == 2, ev
        # The prepared-files counter must not be inflated or reset by
        # transfer reporting.
        assert ev["current"] == 2 and ev["total"] == 2, ev
        # Spec decision 1: transfer sub-progress events must also carry
        # the folders snapshot, or the Import page's folder table blanks
        # for the duration of every batch transfer.
        assert "folders" in ev, ev
    # Both the flat batch file and the collision-renamed file reported.
    assert max(ev["phase_current"] for ev in transfer_events) == 2
    # Transfer fields are batch-scoped: cleared once the batch settles.
    assert "phase_current" not in job["progress"], job["progress"]
    assert "phase_total" not in job["progress"], job["progress"]
    assert "phase_label" not in job["progress"], job["progress"]


def test_include_paths_imports_only_selected_files(tmp_path):
    """include_paths restricts the copy set; discovered still counts the card."""
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
        ("DSC_0003.jpg", datetime(2026, 7, 3, 12, 0, 0), "blue"),
    ])
    keep = {str(card / "DSC_0001.jpg"), str(card / "DSC_0003.jpg")}
    archive = tmp_path / "archive"

    db, _, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(archive),
        include_paths=keep, previewed_count=3, checked_count=2,
    ))

    assert result["copied"] == 2
    # No copy failed — otherwise "the deselected file is absent" below would
    # also hold for a no-op filter whose third copy merely broke.
    assert result["failed"] == 0
    # discovered stays the full card — it backs the card-safety verdict.
    assert result["discovered"] == 3

    # The selected files landed in the archive and the deselected one did not.
    expected = {
        str(archive / "2026" / "2026-07-03" / "DSC_0001.jpg"),
        str(archive / "2026" / "2026-07-03" / "DSC_0003.jpg"),
    }
    for path in expected:
        assert os.path.isfile(path), f"missing archive file: {path}"
    deselected = str(archive / "2026" / "2026-07-03" / "DSC_0002.jpg")
    assert not os.path.exists(deselected), (
        f"deselected file was copied to the archive: {deselected}"
    )

    rows = _photo_rows(db)
    assert {
        os.path.join(r["folder_path"], r["filename"]) for r in rows
    } == expected


def test_include_paths_absent_imports_everything(tmp_path):
    """No selection means no opinion — current behavior is unchanged."""
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])
    _, _, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(tmp_path / "archive"),
    ))
    assert result["copied"] == 2
    assert result["discovered"] == 2


def test_include_paths_empty_set_imports_nothing(tmp_path):
    """An empty selection is 'nothing chosen', not 'no opinion'.

    Truthiness here instead of ``is not None`` would import the whole card.
    """
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])
    _, _, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(tmp_path / "archive"),
        include_paths=set(), previewed_count=2, checked_count=0,
    ))
    assert result["copied"] == 0
    assert result["discovered"] == 2


def test_deselection_makes_card_unsafe_to_format(tmp_path):
    """End-to-end assertion, but NOT the guard for this commit's condition:
    it also passes with the feature removed, because the ledger equality
    already fails here (2 discovered, 1 copied). The real guard is
    ``test_deselected_then_vanished_file_makes_card_unsafe``, where the
    equality balances — delete that one and the protection is gone.
    """
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])
    _, _, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(tmp_path / "archive"),
        include_paths={str(card / "DSC_0001.jpg")},
        previewed_count=2, checked_count=1,
    ))
    assert result["safe_to_format"] is False
    assert result["unverified_duplicates_only"] is False


def test_full_selection_of_card_with_duplicates_is_safe_to_format(tmp_path):
    """THE duplicate-accounting regression guard.

    Duplicates stay in include_paths, so the checker counts them as
    skipped_duplicate and the ledger balances. If someone "fixes"
    include_paths to mean the checked boxes, this goes false and Vireo
    tells the user not to format a card that is fully archived.
    """
    from import_job import ImportParams

    archive = tmp_path / "archive"
    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])
    # First import puts both in the archive.
    _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(archive),
    ))
    # Second import of the same card: everything is a duplicate.
    all_paths = {str(card / "DSC_0001.jpg"), str(card / "DSC_0002.jpg")}
    _, _, result = _run_import(
        tmp_path, ImportParams(
            sources=[str(card)], destination=str(archive),
            include_paths=all_paths, previewed_count=2, checked_count=0,
        ),
    )
    assert result["copied"] == 0
    assert result["skipped_duplicate"] == 2
    assert result["safe_to_format"] is True


def test_vanished_in_scope_file_makes_card_unsafe(tmp_path):
    """The ledger equality still balances here — 1 processed of 1 discovered —
    so this needs its own condition."""
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])
    gone = str(card / "DSC_0002.jpg")  # previewed, then deleted
    _, _, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(tmp_path / "archive"),
        include_paths={str(card / "DSC_0001.jpg"), gone},
        previewed_count=2, checked_count=2,
    ))
    assert result["copied"] == 1
    assert result["discovered"] == 1
    assert result["safe_to_format"] is False
    assert result["unverified_duplicates_only"] is False


def test_deselected_then_vanished_file_makes_card_unsafe(tmp_path):
    """Deselect X, then X disappears before the job.

    discovered=1, queued=1, copied=1 → the equality balances. vanished_paths
    is empty because X was never in include_paths. Only the explicit
    ``deselected == 0`` condition catches this.
    """
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])
    _, _, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(tmp_path / "archive"),
        include_paths={str(card / "DSC_0001.jpg")},
        previewed_count=2, checked_count=1,   # DSC_0002 previewed, deselected, gone
    ))
    assert result["copied"] == 1
    assert result["discovered"] == 1
    assert result["safe_to_format"] is False
    assert result["unverified_duplicates_only"] is False


def _seed_likely_twin(tmp_path, db, name="IMG_0400.jpg", card_name="card"):
    """Card file + a catalog row matching name/size/capture-time with
    DIFFERENT bytes. With trust_likely_duplicates=True this drives
    unverified_duplicate > 0 — the precondition that makes
    unverified_duplicates_only reachable at all. Lifted from
    test_trust_likely_duplicates_skips_metadata_match_without_byte_check.

    ``card_name`` is parameterized (and the catalog folder derived from it)
    so a caller can seed two independent cards under one ``tmp_path``
    without silently merging them into ``_make_card``'s default ``card``.
    """
    from PIL.ExifTags import Base as ExifBase

    dt = datetime(2026, 5, 1, 10, 15, 30)
    card = tmp_path / card_name
    card.mkdir(exist_ok=True)
    card_file = card / name
    img = Image.new("RGB", (16, 16), "red")
    exif = img.getexif()
    exif[ExifBase.DateTimeOriginal] = dt.strftime("%Y:%m:%d %H:%M:%S")
    img.save(str(card_file), exif=exif)
    card_bytes = card_file.read_bytes()

    library = tmp_path / f"{card_name}_library"
    library.mkdir(exist_ok=True)
    (library / name).write_bytes(
        card_bytes[:-1] + bytes([card_bytes[-1] ^ 0xFF]))

    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (str(library), library.name),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " timestamp) VALUES (?, ?, '.jpg', ?, ?)",
        (fid, name, len(card_bytes), "2026-05-01T10:15:30"),
    )
    db.conn.commit()
    return card, card_file


def test_deselected_then_vanished_also_blocks_the_amber_verdict(tmp_path):
    """NON-VACUOUS guard for the second verdict block.

    unverified_duplicates_only's FIRST condition is unverified_duplicate > 0,
    so any test without a likely-duplicate asserts nothing — the verdict is
    already False and a patch to safe_to_format alone would pass. This setup
    makes it True on the baseline, and the ledger equality HOLDS (0 copied +
    1 skipped == 1 discovered), so only the explicit deselected == 0
    condition can flip it.
    """
    from import_job import ImportParams, run_import_job

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    card, card_file = _seed_likely_twin(tmp_path, db)

    result = run_import_job(
        _make_job(), FakeRunner(), db_path, db._active_workspace_id,
        ImportParams(
            sources=[str(card)], destination=str(tmp_path / "archive"),
            trust_likely_duplicates=True,
            include_paths={str(card_file)},
            previewed_count=2, checked_count=1,  # a 2nd file was deselected, then vanished
        ),
    )
    # Preconditions: without these the assertions below are vacuous.
    assert result["unverified_duplicate"] == 1
    assert result["copied"] + result["skipped_duplicate"] == result["discovered"]

    assert result["safe_to_format"] is False
    assert result["unverified_duplicates_only"] is False


def test_vanished_file_also_blocks_the_amber_verdict(tmp_path):
    """Same non-vacuous shape, for the vanished_paths condition."""
    from import_job import ImportParams, run_import_job

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    card, card_file = _seed_likely_twin(tmp_path, db)

    result = run_import_job(
        _make_job(), FakeRunner(), db_path, db._active_workspace_id,
        ImportParams(
            sources=[str(card)], destination=str(tmp_path / "archive"),
            trust_likely_duplicates=True,
            include_paths={str(card_file), str(card / "GONE.jpg")},
            previewed_count=2, checked_count=2,
        ),
    )
    assert result["unverified_duplicate"] == 1
    assert result["copied"] + result["skipped_duplicate"] == result["discovered"]

    assert result["safe_to_format"] is False
    assert result["unverified_duplicates_only"] is False


def test_vanished_file_without_previewed_count_makes_card_unsafe(tmp_path):
    """``vanished_paths`` must not be gated on ``previewed_count``.

    Both fields are independently optional on ``ImportParams``, so a caller
    can send a selection without a preview size. The ledger equality cannot
    catch the vanished file (discovered=1, copied=1, balanced), so gating
    the vanished-path check on ``previewed_count`` fails OPEN on exactly the
    case this condition exists to close.
    """
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])
    _, _, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(tmp_path / "archive"),
        include_paths={str(card / "DSC_0001.jpg"), str(card / "GONE.jpg")},
        previewed_count=None, checked_count=None,
    ))
    assert result["copied"] == 1
    assert result["discovered"] == 1
    assert result["safe_to_format"] is False
    assert result["unverified_duplicates_only"] is False


def test_include_paths_accepts_a_list(tmp_path):
    """A JSON payload deserializes ``include_paths`` to a list.

    The drift math does set arithmetic on it, so without coercing once above
    the filter this raises TypeError before a single file is copied. That
    crash is all this test guards: its ``safe_to_format`` assertion still
    passes under a raw ``len(params.include_paths)``, because the ledger
    equality already fails here (1 copied, 2 discovered). The guard for the
    dedupe is ``test_repeated_include_path_does_not_mask_a_deselection``,
    where the equality balances.
    """
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])
    kept = str(card / "DSC_0001.jpg")
    _, _, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(tmp_path / "archive"),
        include_paths=[kept, kept],   # list, with a duplicate entry
        previewed_count=2, checked_count=1,
    ))
    assert result["copied"] == 1
    assert result["safe_to_format"] is False


def test_negative_deselected_count_makes_card_unsafe(tmp_path):
    """A payload previewing fewer files than it selected is self-inconsistent.

    ``deselected`` goes negative, and ``> 0`` would read that as "nothing
    deselected" and let the card go green. This module fails closed on
    inconsistent input.
    """
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])
    _, _, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(tmp_path / "archive"),
        include_paths={str(card / "DSC_0001.jpg")},
        previewed_count=0, checked_count=1,
    ))
    assert result["copied"] == 1
    assert result["discovered"] == 1
    assert result["safe_to_format"] is False
    assert result["unverified_duplicates_only"] is False


def test_repeated_include_path_does_not_mask_a_deselection(tmp_path):
    """THE dedupe guard: a repeat must not inflate the selected count.

    Three files previewed; the payload carries A twice and B once, so a raw
    ``len(params.include_paths)`` reads 3 and computes ``deselected == 0``
    even though only two distinct files were selected. The third previewed
    file is not on the card any more, so ``vanished_paths`` is empty too,
    and the ledger balances (2 copied of 2 discovered) — every other guard
    is silent. Only counting the deduped set keeps the card unsafe.
    """
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])
    a, b = str(card / "DSC_0001.jpg"), str(card / "DSC_0002.jpg")
    _, _, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(tmp_path / "archive"),
        include_paths=[a, a, b], previewed_count=3, checked_count=3,
    ))
    # Preconditions: without these the assertion below is vacuous.
    assert result["copied"] + result["skipped_duplicate"] == result["discovered"]
    assert result["copied"] == 2

    assert result["safe_to_format"] is False
    assert result["unverified_duplicates_only"] is False


def test_deselection_explains_itself_on_the_result_card(tmp_path):
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])
    _, _, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(tmp_path / "archive"),
        include_paths={str(card / "DSC_0001.jpg")},
        previewed_count=2, checked_count=1,
    ))
    assert "Deselected files" in _unsafe_paths(result)
    reason = _unsafe_reason(result, "Deselected files")
    assert "1 file you deselected was not copied" in reason
    # Must NOT claim the card holds the only copies — false when the
    # deselected file is byte-identical to a selected one.
    assert "only copies" not in reason


def test_vanished_file_explains_itself_on_the_result_card(tmp_path):
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])
    _, _, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(tmp_path / "archive"),
        include_paths={str(card / "DSC_0001.jpg"), str(card / "GONE.jpg")},
        previewed_count=2, checked_count=2,
    ))
    assert "Files missing at import time" in _unsafe_paths(result)
    # The count must be the number that VANISHED (1), not the size of the
    # selection (2). Asserting only the path key lets a wrong denominator
    # through: ``len(include_paths)`` here renders "2 files ... disappeared"
    # while exactly one did, in user-facing card-safety copy.
    assert _unsafe_reason(result, "Files missing at import time") == (
        "1 file was in scope but had disappeared from the source "
        "when the import ran"
    )


def test_files_appearing_after_preview_explain_themselves(tmp_path):
    """A file that arrived after the preview is not in ``include_paths``.

    It is therefore never copied, and the card still holds the only copy.
    ``deselected`` is 0 here (two previewed, two selected) and nothing
    vanished, so the appeared count is the only signal that can name it.
    """
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
        ("DSC_0003.jpg", datetime(2026, 7, 3, 12, 0, 0), "blue"),
    ])
    _, _, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(tmp_path / "archive"),
        include_paths={
            str(card / "DSC_0001.jpg"), str(card / "DSC_0002.jpg"),
        },
        previewed_count=2, checked_count=2,
    ))
    assert "Files added after preview" in _unsafe_paths(result)
    assert _unsafe_reason(result, "Files added after preview") == (
        "at least 1 file arrived after your preview and was not imported "
        "— re-preview to include it"
    )


def test_inconsistent_selection_count_explains_itself(tmp_path):
    """A negative ``deselected`` blocks the card, so it must also speak.

    ``_selection_blocks_format`` fails closed on ``deselected != 0``, so a
    payload that previewed fewer files than it selected turns the pill red.
    Appending only under ``deselected > 0`` would leave that red pill bare —
    a warning with no stated reason. The count itself is untrustworthy here,
    so the entry says the payload is inconsistent rather than quoting a
    nonsense number.
    """
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])
    _, _, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(tmp_path / "archive"),
        include_paths={str(card / "DSC_0001.jpg")},
        previewed_count=0, checked_count=1,
    ))
    # Selection is the ONLY blocker: the ledger balances and nothing failed.
    assert result["copied"] == 1
    assert result["copied"] + result["skipped_duplicate"] == result["discovered"]
    assert result["failed"] == 0
    assert result["safe_to_format"] is False

    assert "Selection count mismatch" in _unsafe_paths(result)
    # No nonsense number: the count is what is untrustworthy.
    assert "-1" not in _unsafe_reason(result, "Selection count mismatch")


def test_selection_entry_grammar_matches_the_count(tmp_path):
    """Plural counterpart to the singular assertions above.

    1 is the most likely real case (deselect one frame, one file vanishes),
    so the singular forms are pinned per-branch; this pins the plural forms
    so the pluralization can't be dropped in either direction.
    """
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
        ("DSC_0003.jpg", datetime(2026, 7, 3, 12, 0, 0), "blue"),
    ])
    _, _, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(tmp_path / "archive"),
        include_paths={
            str(card / "DSC_0001.jpg"),
            str(card / "GONE_A.jpg"), str(card / "GONE_B.jpg"),
        },
        previewed_count=5, checked_count=3,
    ))
    assert _unsafe_reason(result, "Deselected files") == (
        "2 files you deselected were not copied"
    )
    assert _unsafe_reason(result, "Files missing at import time") == (
        "2 files were in scope but had disappeared from the source "
        "when the import ran"
    )

    # ``appeared`` has singular coverage above; pin its plural forms here so
    # all three of its conditionals (``_plural``, the verb, the pronoun) are
    # held from both sides. A fresh root is required: reusing ``tmp_path``
    # would reuse this run's db/archive and turn the copy into a duplicate
    # skip.
    plural_root = tmp_path / "appeared_plural"
    plural_root.mkdir()
    card2 = _make_card(plural_root, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
        ("DSC_0003.jpg", datetime(2026, 7, 3, 12, 0, 0), "blue"),
    ])
    _, _, result2 = _run_import(plural_root, ImportParams(
        sources=[str(card2)], destination=str(plural_root / "archive"),
        include_paths={str(card2 / "DSC_0001.jpg")},
        previewed_count=1, checked_count=1,
    ))
    assert _unsafe_reason(result2, "Files added after preview") == (
        "at least 2 files arrived after your preview and were not "
        "imported — re-preview to include them"
    )
    # Pin the KEY to a nonzero value, not just the sentence: every other
    # ``files_appeared`` assertion in this file expects 0, so hard-coding the
    # key to 0 would pass them all while the frontend readout that consumes
    # it silently stopped reporting drift.
    assert result2["files_appeared"] == 2


def test_no_selection_blocked_result_leaves_the_pill_bare(tmp_path):
    """The invariant: a selection-blocked card always states a reason.

    ``renderResult`` hides the unsafe list entirely when it is empty, so any
    path that flips the pill red without appending an entry produces a scary
    warning with no stated reason. Each scenario asserts the SPECIFIC entry
    it should produce — asserting only "non-empty" would pass on an
    unrelated pre-existing entry (a failed copy, a likely duplicate).
    """
    from import_job import ImportParams

    scenarios = [
        # (name, card specs, include filenames, extra include, previewed,
        #  expected entry path)
        ("deselect",
         ["DSC_0001.jpg", "DSC_0002.jpg"], ["DSC_0001.jpg"], None, 2,
         "Deselected files"),
        ("vanished",
         ["DSC_0001.jpg"], ["DSC_0001.jpg"], "GONE.jpg", 2,
         "Files missing at import time"),
        ("appeared",
         ["DSC_0001.jpg", "DSC_0002.jpg", "DSC_0003.jpg"],
         ["DSC_0001.jpg", "DSC_0002.jpg"], None, 2,
         "Files added after preview"),
        # A negative ``deselected`` cannot occur alone: ``include_paths``
        # larger than ``previewed_count`` means either a selected path is not
        # on the card (vanished) or the card holds more than was previewed
        # (appeared). The assertion is on the specific entry, so the
        # co-occurring one cannot satisfy it.
        ("negative",
         ["DSC_0001.jpg"], ["DSC_0001.jpg"], None, 0,
         "Selection count mismatch"),
    ]
    for name, specs, included, extra, previewed, expected in scenarios:
        root = tmp_path / name
        root.mkdir()
        # Distinct colors: identical bytes would make the second file an
        # in-batch duplicate (copied=1, skipped_duplicate=1), so a scenario
        # that reads as two plain copies would quietly be exercising the
        # duplicate path instead.
        colors = ["red", "green", "blue", "white"]
        card = _make_card(root, [
            (fn, datetime(2026, 7, 3, 10 + i, 0, 0), colors[i])
            for i, fn in enumerate(specs)
        ])
        include_paths = {str(card / fn) for fn in included}
        if extra:
            include_paths.add(str(card / extra))
        _, _, result = _run_import(root, ImportParams(
            sources=[str(card)], destination=str(root / "archive"),
            include_paths=include_paths,
            previewed_count=previewed, checked_count=len(include_paths),
        ))
        assert result["safe_to_format"] is False, name
        assert result["unsafe_files"], name
        assert expected in _unsafe_paths(result), name
        # Entries are mirrored into ``errors``; both surfaces must speak.
        assert any(e.startswith(expected + ": ") for e in result["errors"]), name


def test_progress_total_is_the_queued_work_not_the_card(tmp_path):
    """One of three files selected: the bar is sized 1, not 3.

    Progress must run on the work actually enqueued. Against the full card a
    half-deselected import runs to completion with the bar stalled part-way
    — a finished job that looks hung.

    The copy-loop emits are asserted separately from the discovery emit, and
    with no truthiness filter on the way in. Three weaker shapes were tried
    and each let a real mutation through: ``3 not in totals`` accepts any
    other wrong number; ``if d.get("total")`` discards a zeroed total before
    it can be seen; and set equality over ALL emits accepts a zeroed
    copy-loop total by hiding it behind the discovery emit's legitimate 0.
    """
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
        ("DSC_0003.jpg", datetime(2026, 7, 3, 12, 0, 0), "blue"),
    ])
    runner = FakeRunner()
    _run_import(
        tmp_path,
        ImportParams(
            sources=[str(card)], destination=str(tmp_path / "archive"),
            include_paths={str(card / "DSC_0001.jpg")},
            previewed_count=3, checked_count=1,
        ),
        runner=runner,
    )
    events = [d for _, kind, d in runner.events if kind == "progress"]
    # The discovery emit is the only one entitled to a 0 total — the count
    # isn't known yet when it fires.
    assert [d["total"] for d in events
            if d["phase"] == "Discovering files"] == [0]
    # Every remaining emit is a copy-loop emit (one per file, one per batch)
    # and every one of them must be sized by the queued work.
    copy_totals = [d["total"] for d in events
                   if d["phase"] != "Discovering files"]
    assert copy_totals, "no copy-loop progress was emitted at all"
    assert set(copy_totals) == {1}


def test_ordinary_deselection_reports_no_files_appeared(tmp_path):
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])
    _, _, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(tmp_path / "archive"),
        include_paths={str(card / "DSC_0001.jpg")},
        previewed_count=2, checked_count=1,
    ))
    assert result["files_appeared"] == 0
    assert result["files_vanished"] == 0


def test_mixed_appear_and_vanish_never_reports_a_negative_count(tmp_path):
    """files_appeared is a net delta clamped at zero. Without the clamp, more
    vanishing than arriving renders "-3 files were added"."""
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])
    _, _, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=str(tmp_path / "archive"),
        include_paths={str(card / "DSC_0001.jpg"),
                       str(card / "GONE_A.jpg"), str(card / "GONE_B.jpg")},
        previewed_count=3, checked_count=3,
    ))
    assert result["files_appeared"] == 0
    assert result["files_vanished"] == 2


def test_step_summary_selected_figure_comes_from_checked_count(tmp_path):
    """Not len(include_paths) — that set retains unchecked duplicates and
    would overstate what the user chose."""
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])
    runner = FakeRunner()
    _run_import(
        tmp_path,
        ImportParams(
            sources=[str(card)], destination=str(tmp_path / "archive"),
            include_paths={str(card / "DSC_0001.jpg"),
                           str(card / "DSC_0002.jpg")},
            previewed_count=2, checked_count=1,
        ),
        runner=runner,
    )
    summaries = _summaries(runner)
    # Full equality, not a substring: the discovered total has to survive in
    # the selection form too. A ``"1 selected"`` substring check would pass
    # with ``of 2 discovered`` quietly dropped, leaving the user a selected
    # figure with nothing to read it against.
    assert "1 selected of 2 discovered, 2 copied, 0 already present, 0 failed" \
        in summaries


def test_step_summary_without_selection_is_unchanged(tmp_path):
    """The no-selection wording is the one every user sees today.

    Pinned as full equality against the string ``main`` emits (verified by
    running this assertion on ``main``), because the selection prefix was
    added by composing around this tail — a substring assertion would pass
    if the ``of N discovered`` tail were dropped to make room for it. The
    discovered total appears exactly once in each of the two forms: in the
    tail here, in the prefix when a selection is present.
    """
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])
    runner = FakeRunner()
    _run_import(
        tmp_path,
        ImportParams(
            sources=[str(card)], destination=str(tmp_path / "archive"),
        ),
        runner=runner,
    )
    summaries = _summaries(runner)
    assert "2 copied, 0 already present, 0 failed of 2 discovered" in summaries
    # No selection means no selection prefix.
    assert not any("selected" in s for s in summaries)


def test_step_summary_claims_a_selection_only_when_one_was_applied(tmp_path):
    """``include_paths`` and ``checked_count`` are independently optional.

    It is ``include_paths`` alone that filters the copy set, so the summary's
    selection form has to be gated on BOTH fields. Keyed on ``checked_count``
    by itself, a payload carrying a count but no paths copies the whole card
    while reporting "1 selected of 2 discovered" — a selection claimed for a
    run where none was applied. Both divergent combinations are asserted:
    each one alone kills a different half of the conjunction.
    """
    from import_job import ImportParams

    # checked_count without include_paths: nothing was filtered, so nothing
    # may be claimed. Both files are copied.
    count_only = tmp_path / "count_only"
    count_only.mkdir()
    card = _make_card(count_only, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])
    runner = FakeRunner()
    _, _, result = _run_import(
        count_only,
        ImportParams(
            sources=[str(card)], destination=str(count_only / "archive"),
            checked_count=1,
        ),
        runner=runner,
    )
    assert result["copied"] == 2, "no include_paths means no filtering"
    summaries = _summaries(runner)
    assert "2 copied, 0 already present, 0 failed of 2 discovered" in summaries
    assert not any("selected" in s for s in summaries)

    # include_paths without checked_count: the copy set IS filtered, but
    # there is no trustworthy figure to quote, so it degrades to the plain
    # form rather than printing "None selected".
    paths_only = tmp_path / "paths_only"
    paths_only.mkdir()
    card2 = _make_card(paths_only, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])
    runner2 = FakeRunner()
    _, _, result2 = _run_import(
        paths_only,
        ImportParams(
            sources=[str(card2)], destination=str(paths_only / "archive"),
            include_paths={str(card2 / "DSC_0001.jpg")},
        ),
        runner=runner2,
    )
    assert result2["copied"] == 1, "include_paths must still filter"
    summaries2 = _summaries(runner2)
    assert "1 copied, 0 already present, 0 failed of 2 discovered" in summaries2
    assert not any("selected" in s for s in summaries2)


# --- Selection on the REMOTE copy path --------------------------------------
#
# ``run_import_job`` delegates the whole run to ``_run_remote_import_job``
# whenever ``params.remote_target`` is set (the user picked a saved NAS
# target), so every selection behaviour asserted above for the local path has
# to be asserted again here — the two functions share no code.
#
# EVERY test below passes ``verify_by_hash=True``. Without it
# ``remote_unverified`` is True, both verdicts are already False, and the
# card-safety assertions would pass on a remote path with no selection
# handling at all.


def test_remote_import_honors_include_paths_and_card_safety(
        tmp_path, monkeypatch):
    """Every assertion from Tasks 1-3, against the remote path."""
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, db._active_workspace_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra, verify_by_hash=True,
            include_paths={str(card / "DSC_0001.jpg")},
            previewed_count=2, checked_count=1,
        ),
    )

    assert result["copied"] == 1
    assert result["discovered"] == 2
    assert "Deselected files" in {u["path"] for u in result["unsafe_files"]}
    # verify_by_hash=True so remote_unverified is False and this assertion
    # actually depends on the new condition rather than passing for free.
    assert result["safe_to_format"] is False

    # The deselected file never crossed the network, and never landed on the
    # mount. Asserting only the counters would pass on a filter that dropped
    # the file from the ledger while still rsyncing it.
    transferred = {
        os.path.basename(s) for c in calls["rsync"] for s in c["src_specs"]
    }
    assert transferred == {"DSC_0001.jpg"}
    mount_dir = os.path.join(ra["mount_base"], "2026", "2026-07-03")
    assert not os.path.exists(os.path.join(mount_dir, "DSC_0002.jpg"))
    rows = _photo_rows(db)
    assert {
        os.path.join(r["folder_path"], r["filename"]) for r in rows
    } == {os.path.join(mount_dir, "DSC_0001.jpg")}


def test_remote_deselected_then_vanished_is_unsafe(tmp_path, monkeypatch):
    """The equality balances (1 copied of 1 discovered); only the explicit
    deselected condition catches it."""
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    # verify=None is the "verified OK" sentinel. Any non-None value is treated
    # as a (name, detail) failure tuple and unpacked, so verify=True raises
    # TypeError instead of failing an assertion.
    _install_fake_remote_rsync(monkeypatch, _remote_calls(ra), verify=None)

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, db._active_workspace_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra, verify_by_hash=True,
            include_paths={str(card / "DSC_0001.jpg")},
            previewed_count=2, checked_count=1,
        ),
    )
    assert result["copied"] == 1
    assert result["discovered"] == 1
    assert result["safe_to_format"] is False


def test_remote_include_paths_absent_imports_everything(tmp_path, monkeypatch):
    """No selection means no opinion — remote behaviour is unchanged."""
    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])
    result, _ = _run_remote_import(
        tmp_path, monkeypatch, {"sources": [str(card)]})
    assert result["copied"] == 2
    assert result["discovered"] == 2
    assert result["safe_to_format"] is True


def test_remote_include_paths_empty_set_imports_nothing(tmp_path, monkeypatch):
    """An empty selection is 'nothing chosen', not 'no opinion'.

    Truthiness instead of ``is not None`` would rsync the whole card.
    """
    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])
    result, calls = _run_remote_import(tmp_path, monkeypatch, {
        "sources": [str(card)],
        "include_paths": set(), "previewed_count": 2, "checked_count": 0,
    })
    assert result["copied"] == 0
    assert result["discovered"] == 2
    assert calls["rsync"] == []


def test_remote_full_selection_of_duplicates_is_safe_to_format(
        tmp_path, monkeypatch):
    """THE duplicate-accounting regression guard, remote edition.

    Duplicates stay in ``include_paths``, so the checker counts them as
    ``skipped_duplicate`` and the ledger balances. If someone "fixes"
    ``include_paths`` to mean the checked boxes, this goes false and Vireo
    tells the user not to format a card that is fully archived.
    """
    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])
    # First import puts both on the NAS (and in the catalog).
    first, _ = _run_remote_import(
        tmp_path, monkeypatch, {"sources": [str(card)]})
    assert first["copied"] == 2

    # Second import of the same card: everything is a duplicate. Same db and
    # same mount, so the twins are found.
    result, _ = _run_remote_import(tmp_path, monkeypatch, {
        "sources": [str(card)],
        "include_paths": {str(card / "DSC_0001.jpg"),
                          str(card / "DSC_0002.jpg")},
        "previewed_count": 2, "checked_count": 0,
    })
    assert result["copied"] == 0
    assert result["skipped_duplicate"] == 2
    assert result["safe_to_format"] is True


def test_remote_vanished_in_scope_file_makes_card_unsafe(
        tmp_path, monkeypatch):
    """The ledger equality balances here — 1 processed of 1 discovered — so
    this needs its own condition."""
    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])
    result, _ = _run_remote_import(tmp_path, monkeypatch, {
        "sources": [str(card)],
        "include_paths": {str(card / "DSC_0001.jpg"),
                          str(card / "DSC_0002.jpg")},   # previewed, deleted
        "previewed_count": 2, "checked_count": 2,
    })
    assert result["copied"] == 1
    assert result["discovered"] == 1
    assert result["safe_to_format"] is False


def test_remote_vanished_file_without_previewed_count_is_unsafe(
        tmp_path, monkeypatch):
    """``vanished_paths`` must not be gated on ``previewed_count``.

    Both fields are independently optional, the ledger equality cannot see
    the vanished file (1 copied of 1 discovered), and gating this on
    ``previewed_count`` fails OPEN on exactly the case it exists to close.
    """
    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])
    result, _ = _run_remote_import(tmp_path, monkeypatch, {
        "sources": [str(card)],
        "include_paths": {str(card / "DSC_0001.jpg"),
                          str(card / "GONE.jpg")},
        "previewed_count": None, "checked_count": None,
    })
    assert result["copied"] == 1
    assert result["discovered"] == 1
    assert result["safe_to_format"] is False
    assert "Files missing at import time" in _unsafe_paths(result)


def test_remote_negative_deselected_count_makes_card_unsafe(
        tmp_path, monkeypatch):
    """A payload previewing fewer files than it selected is self-inconsistent.

    ``deselected`` goes negative, and ``> 0`` would read that as "nothing
    deselected" and let the card go green.
    """
    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])
    result, _ = _run_remote_import(tmp_path, monkeypatch, {
        "sources": [str(card)],
        "include_paths": {str(card / "DSC_0001.jpg")},
        "previewed_count": 0, "checked_count": 1,
    })
    # Selection is the ONLY blocker: the ledger balances and nothing failed.
    assert result["copied"] == 1
    assert result["copied"] + result["skipped_duplicate"] == result["discovered"]
    assert result["failed"] == 0
    assert result["safe_to_format"] is False
    assert "Selection count mismatch" in _unsafe_paths(result)
    # No nonsense number: the count is what is untrustworthy.
    assert "-1" not in _unsafe_reason(result, "Selection count mismatch")


def test_remote_include_paths_accepts_a_list(tmp_path, monkeypatch):
    """A JSON payload deserializes ``include_paths`` to a list.

    The drift math does set arithmetic on it, so without coercing once above
    the filter this raises TypeError before a single file is rsynced.
    """
    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])
    kept = str(card / "DSC_0001.jpg")
    result, _ = _run_remote_import(tmp_path, monkeypatch, {
        "sources": [str(card)],
        "include_paths": [kept, kept],   # list, with a duplicate entry
        "previewed_count": 2, "checked_count": 1,
    })
    assert result["copied"] == 1
    assert result["safe_to_format"] is False


def test_remote_repeated_include_path_does_not_mask_a_deselection(
        tmp_path, monkeypatch):
    """THE dedupe guard: a repeat must not inflate the selected count.

    Three files previewed; the payload carries A twice and B once, so a raw
    ``len(params.include_paths)`` reads 3 and computes ``deselected == 0``.
    The third previewed file is gone, so ``vanished_paths`` is empty too, and
    the ledger balances — every other guard is silent.
    """
    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])
    a, b = str(card / "DSC_0001.jpg"), str(card / "DSC_0002.jpg")
    result, _ = _run_remote_import(tmp_path, monkeypatch, {
        "sources": [str(card)],
        "include_paths": [a, a, b],
        "previewed_count": 3, "checked_count": 3,
    })
    # Preconditions: without these the assertion below is vacuous.
    assert result["copied"] + result["skipped_duplicate"] == result["discovered"]
    assert result["copied"] == 2
    assert result["safe_to_format"] is False


def test_remote_selection_entries_explain_themselves(tmp_path, monkeypatch):
    """All four selection entries, with their exact user-facing wording.

    ``renderResult`` HIDES the unsafe list when it is empty, so every
    selection signal that flips the pill red must also append a line. Each
    scenario asserts its SPECIFIC entry and full reason string — asserting
    only "non-empty" would pass on an unrelated pre-existing entry, and
    asserting only the path key would let a wrong count through in
    user-facing card-safety copy.
    """
    scenarios = [
        # (name, card filenames, included filenames, extra include,
        #  previewed, expected path, expected reason)
        ("deselect",
         ["DSC_0001.jpg", "DSC_0002.jpg"], ["DSC_0001.jpg"], None, 2,
         "Deselected files",
         "1 file you deselected was not copied"),
        ("deselect_plural",
         ["DSC_0001.jpg", "DSC_0002.jpg", "DSC_0003.jpg"], ["DSC_0001.jpg"],
         None, 3,
         "Deselected files",
         "2 files you deselected were not copied"),
        ("vanished",
         ["DSC_0001.jpg"], ["DSC_0001.jpg"], "GONE.jpg", 2,
         "Files missing at import time",
         "1 file was in scope but had disappeared from the source "
         "when the import ran"),
        ("appeared",
         ["DSC_0001.jpg", "DSC_0002.jpg", "DSC_0003.jpg"],
         ["DSC_0001.jpg", "DSC_0002.jpg"], None, 2,
         "Files added after preview",
         "at least 1 file arrived after your preview and was not imported "
         "— re-preview to include it"),
        ("appeared_plural",
         ["DSC_0001.jpg", "DSC_0002.jpg", "DSC_0003.jpg"], ["DSC_0001.jpg"],
         None, 1,
         "Files added after preview",
         "at least 2 files arrived after your preview and were not "
         "imported — re-preview to include them"),
    ]
    for name, specs, included, extra, previewed, path, reason in scenarios:
        root = tmp_path / name
        root.mkdir()
        # Distinct colors: identical bytes would make the second file an
        # in-batch duplicate, so a scenario meant to read as two plain
        # copies would quietly exercise the duplicate path instead.
        colors = ["red", "green", "blue", "white"]
        card = _make_card(root, [
            (fn, datetime(2026, 7, 3, 10 + i, 0, 0), colors[i])
            for i, fn in enumerate(specs)
        ])
        include_paths = {str(card / fn) for fn in included}
        if extra:
            include_paths.add(str(card / extra))
        result, _ = _run_remote_import(root, monkeypatch, {
            "sources": [str(card)], "include_paths": include_paths,
            "previewed_count": previewed,
            "checked_count": len(include_paths),
        })
        assert result["safe_to_format"] is False, name
        assert path in _unsafe_paths(result), name
        assert _unsafe_reason(result, path) == reason, name
        # Entries are mirrored into ``errors``; both surfaces must speak.
        assert any(e.startswith(path + ": ") for e in result["errors"]), name

    # The plural vanished wording, alongside a plural deselection (previewed
    # 5, selected 3, of which 2 are no longer on the card).
    plural_root = tmp_path / "vanished_plural"
    plural_root.mkdir()
    card = _make_card(plural_root, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
        ("DSC_0003.jpg", datetime(2026, 7, 3, 12, 0, 0), "blue"),
    ])
    result, _ = _run_remote_import(plural_root, monkeypatch, {
        "sources": [str(card)],
        "include_paths": {str(card / "DSC_0001.jpg"),
                          str(card / "GONE_A.jpg"),
                          str(card / "GONE_B.jpg")},
        "previewed_count": 5, "checked_count": 3,
    })
    assert _unsafe_reason(result, "Deselected files") == (
        "2 files you deselected were not copied"
    )
    assert _unsafe_reason(result, "Files missing at import time") == (
        "2 files were in scope but had disappeared from the source "
        "when the import ran"
    )


def test_remote_selection_drift_counts_are_reported(tmp_path, monkeypatch):
    """``files_appeared`` / ``files_vanished`` are pinned to NONZERO values.

    Every other drift assertion in this file expects 0, so hard-coding either
    key to 0 would pass them all while the frontend readout that consumes it
    silently stopped reporting drift.
    """
    appeared_root = tmp_path / "appeared"
    appeared_root.mkdir()
    card = _make_card(appeared_root, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
        ("DSC_0003.jpg", datetime(2026, 7, 3, 12, 0, 0), "blue"),
    ])
    result, _ = _run_remote_import(appeared_root, monkeypatch, {
        "sources": [str(card)],
        "include_paths": {str(card / "DSC_0001.jpg")},
        "previewed_count": 1, "checked_count": 1,
    })
    assert result["files_appeared"] == 2
    assert result["files_vanished"] == 0

    # ``files_appeared`` is a net delta clamped at zero: more vanishing than
    # arriving must read 0, never a negative.
    vanished_root = tmp_path / "vanished"
    vanished_root.mkdir()
    card2 = _make_card(vanished_root, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])
    result2, _ = _run_remote_import(vanished_root, monkeypatch, {
        "sources": [str(card2)],
        "include_paths": {str(card2 / "DSC_0001.jpg"),
                          str(card2 / "GONE_A.jpg"),
                          str(card2 / "GONE_B.jpg")},
        "previewed_count": 3, "checked_count": 3,
    })
    assert result2["files_vanished"] == 2
    assert result2["files_appeared"] == 0


def test_remote_ordinary_deselection_reports_no_drift(tmp_path, monkeypatch):
    """Both keys are unconditionally present, and read 0 on a plain
    deselection."""
    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])
    result, _ = _run_remote_import(tmp_path, monkeypatch, {
        "sources": [str(card)],
        "include_paths": {str(card / "DSC_0001.jpg")},
        "previewed_count": 2, "checked_count": 1,
    })
    assert result["files_appeared"] == 0
    assert result["files_vanished"] == 0


def test_remote_drift_keys_present_without_any_selection(
        tmp_path, monkeypatch):
    """The keys are unconditional — a caller reading them must not KeyError
    on the overwhelmingly common no-selection run."""
    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])
    result, _ = _run_remote_import(
        tmp_path, monkeypatch, {"sources": [str(card)]})
    assert result["files_appeared"] == 0
    assert result["files_vanished"] == 0


def test_remote_progress_total_is_the_queued_work_not_the_card(
        tmp_path, monkeypatch):
    """One of three files selected: the bar is sized 1, not 3.

    The copy-loop emits are asserted separately from the discovery emit, and
    with no truthiness filter on the way in — a ``3 not in totals`` check
    accepts any other wrong number, and set equality over ALL emits accepts a
    zeroed copy-loop total by hiding it behind the discovery emit's
    legitimate 0.
    """
    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
        ("DSC_0003.jpg", datetime(2026, 7, 3, 12, 0, 0), "blue"),
    ])
    runner = FakeRunner()
    _run_remote_import(tmp_path, monkeypatch, {
        "sources": [str(card)],
        "include_paths": {str(card / "DSC_0001.jpg")},
        "previewed_count": 3, "checked_count": 1,
    }, runner=runner)
    events = [d for _, kind, d in runner.events if kind == "progress"]
    # The discovery emit is the only one entitled to a 0 total — the count
    # isn't known yet when it fires.
    assert [d["total"] for d in events
            if d["phase"] == "Discovering files"] == [0]
    copy_totals = [d["total"] for d in events
                   if d["phase"] != "Discovering files"]
    assert copy_totals, "no copy-loop progress was emitted at all"
    assert set(copy_totals) == {1}


def test_remote_progress_total_covers_the_batch_guard_emits(
        tmp_path, monkeypatch):
    """The dest-under-source batch guard emits too, and it is also sized by
    the queued work.

    ``destination`` is the card itself here, so every batch fails the
    dest-under-source guard and takes the early-``continue`` emit — the only
    way to reach that site.
    """
    from import_job import ImportParams, run_import_job

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
        ("DSC_0003.jpg", datetime(2026, 7, 3, 12, 0, 0), "blue"),
    ])
    ra = _remote_archive_for(tmp_path)
    ra["mount_base"] = str(card)          # destination inside the source
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    runner = FakeRunner()
    result = run_import_job(
        _make_job(), runner, db_path, db._active_workspace_id,
        ImportParams(
            sources=[str(card)], destination=str(card), remote_target=ra,
            verify_by_hash=True,
            include_paths={str(card / "DSC_0001.jpg"),
                           str(card / "DSC_0002.jpg")},
            previewed_count=3, checked_count=2,
        ),
    )
    assert result["failed"] == 2, "the guard did not fire"
    events = [d for _, kind, d in runner.events if kind == "progress"]
    copy_totals = [d["total"] for d in events
                   if d["phase"] != "Discovering files"]
    assert copy_totals, "no copy-loop progress was emitted at all"
    assert set(copy_totals) == {2}


def test_remote_progress_total_covers_the_mount_root_guard_emit(
        tmp_path, monkeypatch):
    """The mount-root-absent batch guard has its own ``_emit``, and it is the
    fourth and last copy-loop site.

    Nothing else reaches it — the guard fires before makedirs and before any
    transport call — so without this test that one denominator can stay sized
    against the whole card while the other three are fixed. Setup lifted from
    ``test_remote_import_refuses_when_mount_root_absent``.
    """
    import move as _move
    import pipeline_job as _pj
    from import_job import ImportParams, run_import_job
    from move import build_remote_move_spec

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
        ("DSC_0003.jpg", datetime(2026, 7, 3, 12, 0, 0), "blue"),
    ])
    fake_mount_base = str(tmp_path / "Volumes_NAS_Photos")
    target = {
        "id": "nas1", "name": "NAS", "host": "nas", "user": "me",
        "port": 22, "ssh_key": "", "bwlimit_kbps": 0,
        "remote_path": "/volume1/Photography",
        "mount_path": fake_mount_base,
    }
    ra = {
        "target": target, "rsync_bin": "/usr/bin/rsync",
        "remote": build_remote_move_spec(target, "", "/usr/bin/rsync"),
        "ssh_base": target["remote_path"], "mount_base": fake_mount_base,
    }
    monkeypatch.setattr(
        _pj, "_missing_archive_mount_root",
        lambda path: (
            "/Volumes/NAS" if path.startswith(fake_mount_base) else None
        ),
    )
    monkeypatch.setattr(_move, "_remote_mkdir_p", lambda r, p: (True, ""))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    runner = FakeRunner()
    result = run_import_job(
        _make_job(), runner, db_path, db._active_workspace_id,
        ImportParams(
            sources=[str(card)], destination=fake_mount_base,
            remote_target=ra, verify_by_hash=True,
            include_paths={str(card / "DSC_0001.jpg"),
                           str(card / "DSC_0002.jpg")},
            previewed_count=3, checked_count=2,
        ),
    )
    # Only the two SELECTED files reach the guard — the deselected one was
    # never queued.
    assert result["failed"] == 2, result
    events = [d for _, kind, d in runner.events if kind == "progress"]
    copy_totals = [d["total"] for d in events
                   if d["phase"] != "Discovering files"]
    assert copy_totals, "no copy-loop progress was emitted at all"
    assert set(copy_totals) == {2}


def test_remote_step_summary_selected_figure_comes_from_checked_count(
        tmp_path, monkeypatch):
    """Not ``len(include_paths)`` — that set retains unchecked duplicates and
    would overstate what the user chose."""
    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])
    runner = FakeRunner()
    _run_remote_import(tmp_path, monkeypatch, {
        "sources": [str(card)],
        "include_paths": {str(card / "DSC_0001.jpg"),
                          str(card / "DSC_0002.jpg")},
        "previewed_count": 2, "checked_count": 1,
    }, runner=runner)
    # Full equality, not a substring: the discovered total has to survive in
    # the selection form too.
    assert "1 selected of 2 discovered, 2 copied, 0 already present, 0 failed" \
        in _summaries(runner)


def test_remote_step_summary_without_selection_is_unchanged(
        tmp_path, monkeypatch):
    """The no-selection wording is the one every user sees today, and it is
    byte-identical to what the remote path emitted before selection existed.
    """
    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])
    runner = FakeRunner()
    _run_remote_import(
        tmp_path, monkeypatch, {"sources": [str(card)]}, runner=runner)
    summaries = _summaries(runner)
    assert "2 copied, 0 already present, 0 failed of 2 discovered" in summaries
    assert not any("selected" in s for s in summaries)


def test_remote_step_summary_claims_a_selection_only_when_applied(
        tmp_path, monkeypatch):
    """``include_paths`` and ``checked_count`` are independently optional.

    It is ``include_paths`` alone that filters the copy set, so the selection
    form has to be gated on BOTH. Keyed on ``checked_count`` alone, a payload
    carrying a count but no paths copies the whole card while reporting
    "1 selected of 2 discovered" — a selection claimed for a run where none
    was applied. Both divergent combinations are asserted: each one alone
    kills a different half of the conjunction.
    """
    # checked_count without include_paths: nothing was filtered, so nothing
    # may be claimed.
    count_only = tmp_path / "count_only"
    count_only.mkdir()
    card = _make_card(count_only, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])
    runner = FakeRunner()
    result, _ = _run_remote_import(count_only, monkeypatch, {
        "sources": [str(card)], "checked_count": 1,
    }, runner=runner)
    assert result["copied"] == 2, "no include_paths means no filtering"
    summaries = _summaries(runner)
    assert "2 copied, 0 already present, 0 failed of 2 discovered" in summaries
    assert not any("selected" in s for s in summaries)

    # include_paths without checked_count: the copy set IS filtered, but there
    # is no trustworthy figure to quote, so it degrades to the plain form
    # rather than printing "None selected".
    paths_only = tmp_path / "paths_only"
    paths_only.mkdir()
    card2 = _make_card(paths_only, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])
    runner2 = FakeRunner()
    result2, _ = _run_remote_import(paths_only, monkeypatch, {
        "sources": [str(card2)],
        "include_paths": {str(card2 / "DSC_0001.jpg")},
    }, runner=runner2)
    assert result2["copied"] == 1, "include_paths must still filter"
    summaries2 = _summaries(runner2)
    assert "1 copied, 0 already present, 0 failed of 2 discovered" in summaries2
    assert not any("selected" in s for s in summaries2)


# --- Local/remote selection parity -------------------------------------
# The two copy paths (``run_import_job`` and ``_run_remote_import_job``)
# carry the same selection logic. These tests are the safety net for that:
# they drive IDENTICAL selection payloads through BOTH entry points and
# assert the observable selection results agree, so a change applied to one
# path and not the other fails here instead of shipping a wrong
# format-the-card verdict on whichever path the author wasn't looking at.
# The block also holds the BEHAVIORAL parity net: seeded-destination
# scenarios (prior imports, uncataloged file drops) compared at the
# DB-observable level (photo rows, workspace-linked folders) on both paths.


# The four ``unsafe_files`` entries the selection logic owns. Order within
# this subset is user-visible — ``renderResult`` lists entries as it finds
# them — so parity compares it. The filter matters: comparing the raw
# ``unsafe_files`` order would drag in copy-failure and unverified-duplicate
# entries, whose relative ordering legitimately differs between the two
# paths, and the resulting noise would force the check to be dropped.
_SELECTION_UNSAFE_PATHS = {
    "Deselected files",
    "Selection count mismatch",
    "Files missing at import time",
    "Files added after preview",
}


def _selection_observables(result, runner):
    """The selection-visible surface of an import run, normalized so the
    local and remote paths are directly comparable.

    Deliberately excludes keys the two paths differ on for reasons that
    have nothing to do with selection (``photo_ids``, ``verified``,
    ``folders``, ``errors`` ordering).
    """
    import_summaries = [
        kw.get("summary")
        for _, step_id, kw in runner.step_updates
        if step_id == "import" and kw.get("summary") is not None
    ]
    events = [d for _, kind, d in runner.events if kind == "progress"]
    return {
        "discovered": result["discovered"],
        "copied": result["copied"],
        "skipped_duplicate": result["skipped_duplicate"],
        "failed": result["failed"],
        "safe_to_format": result["safe_to_format"],
        "unverified_duplicates_only": result["unverified_duplicates_only"],
        "files_appeared": result["files_appeared"],
        "files_vanished": result["files_vanished"],
        "unsafe": {(u["path"], u["reason"]) for u in result["unsafe_files"]},
        # A set erases render order, and render order is user-visible. Without
        # this, emitting the drift lines in a different order on one path is a
        # real one-sided divergence that passes every test in this file.
        "unsafe_order": [u["path"] for u in result["unsafe_files"]
                         if u["path"] in _SELECTION_UNSAFE_PATHS],
        "summary": import_summaries[-1] if import_summaries else None,
        # The discovery emit legitimately fires before the count is known,
        # so it is excluded; every other emit is sized by the queued work.
        "copy_totals": {d["total"] for d in events
                        if d["phase"] != "Discovering files"},
    }


def _dest_photo_facts(db, dest_root):
    """DB-level import outcome, normalized for local/remote comparison:
    {(folder relpath under dest_root, filename, file_hash, hash_status)}.
    The persisted hash VALUE is part of the tuple because
    ``safe_to_format`` only reports each path's own internal verification;
    two paths can both return True and stamp ``hash_status='ok'`` while
    persisting different or stale ``file_hash`` values, which affects
    deduplication and cache identity. Including it here makes one-sided
    catalog-hash regressions visible in the parity net."""
    facts = set()
    for row in _photo_rows(db):
        rel = os.path.relpath(row["folder_path"], str(dest_root))
        # Normalize to POSIX separators so parity comparisons and scenario
        # assertions read identically on Windows and POSIX runners.
        rel = rel.replace(os.sep, "/")
        facts.add((rel, row["filename"], row["file_hash"],
                   row["hash_status"]))
    return facts


def _linked_folder_rels(db, dest_root):
    """Folder paths visible in the active workspace, relative to the
    destination root. Twin-folder linking is workspace-scoped, so this is
    where a one-sided _link_duplicate_twin_dirs regression shows up."""
    rows = db.conn.execute(
        """SELECT f.path FROM folders f
           JOIN workspace_folders wf ON wf.folder_id = f.id
           WHERE wf.workspace_id = ?""",
        (db._active_workspace_id,),
    ).fetchall()
    return {os.path.relpath(r["path"], str(dest_root)).replace(os.sep, "/")
            for r in rows}


def _behavior_observables(result, runner, db, dest_root):
    """Superset of _selection_observables: adds DB-level facts. Excludes
    the same legitimately-divergent keys (photo_ids, folders, errors
    ordering) plus eta fields.

    Note: the inherited "excludes folders" rationale no longer fully
    applies — ``folders_final`` (the per-folder snapshot on the last
    progress event) IS compared cross-path now: rel keys come from
    ``build_destination_path`` on both paths, so equality holds."""
    obs = _selection_observables(result, runner)
    obs["verified"] = result["verified"]
    obs["cancelled"] = result["cancelled"]
    obs["db_photos"] = _dest_photo_facts(db, dest_root)
    obs["db_linked_folders"] = _linked_folder_rels(db, dest_root)
    # Decision 1 (spec): every progress event must carry the per-folder
    # snapshot the Import page renders. Count the events that don't, and
    # capture the final snapshot for cross-path comparison.
    events = [d for _, kind, d in runner.events if kind == "progress"]
    obs["events_missing_folders"] = sum(
        1 for d in events if "folders" not in d)
    obs["folders_final"] = (
        {rel: dict(c) for rel, c in events[-1]["folders"].items()}
        if events and "folders" in events[-1] else None)
    return obs


def _run_local_behavior_case(root, monkeypatch, specs, *, seed=None,
                             params_kwargs=None, runner=None,
                             verify_by_hash=True):
    """Local-path runner for behavioral parity scenarios.

    ``seed(dest_root, db_path)`` runs BEFORE the measured import to
    pre-populate/pre-catalog the destination (e.g. by running a prior
    import). ``verify_by_hash=True`` for the same anti-vacuity reason as
    _run_local_selection_case.
    """
    from import_job import ImportParams, run_import_job

    card = _make_card(root, specs)
    dest_root = root / "archive"
    db_path = str(root / "test.db")
    db = Database(db_path)
    if seed is not None:
        seed(dest_root, db_path)
    runner = runner or FakeRunner()
    result = run_import_job(
        _make_job(), runner, db_path, db._active_workspace_id,
        ImportParams(
            sources=[str(card)], destination=str(dest_root),
            verify_by_hash=verify_by_hash, **(params_kwargs or {}),
        ),
    )
    return _behavior_observables(result, runner, db, dest_root)


def _run_remote_behavior_case(root, monkeypatch, specs, *, seed=None,
                              params_kwargs=None, runner=None,
                              verify_by_hash=True):
    """Remote-path runner. Mirrors _run_local_behavior_case's geometry:
    the mount base plays the destination root, and ``seed`` receives it.
    Builds the transport seams itself (rather than _run_remote_import) so
    it can hand ``seed`` the db_path before the measured run."""
    from import_job import ImportParams, run_import_job

    card = _make_card(root, specs)
    ra = _remote_archive_for(root)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)
    db_path = str(root / "test.db")
    db = Database(db_path)
    if seed is not None:
        seed(Path(ra["mount_base"]), db_path)
    runner = runner or FakeRunner()
    result = run_import_job(
        _make_job(), runner, db_path, db._active_workspace_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra, verify_by_hash=verify_by_hash,
            **(params_kwargs or {}),
        ),
    )
    return _behavior_observables(result, runner, db, ra["mount_base"])


def _run_local_selection_case(root, monkeypatch, specs, selection):
    """Run one selection scenario through the LOCAL copy path.

    ``verify_by_hash=True`` matches what ``_run_remote_import`` forces: with
    it off the remote path's ``remote_unverified`` makes both card-safety
    verdicts False for free and every parity assertion passes vacuously.
    """
    from import_job import ImportParams, run_import_job

    card = _make_card(root, specs)
    runner = FakeRunner()
    db_path = str(root / "test.db")
    db = Database(db_path)
    result = run_import_job(
        _make_job(), runner, db_path, db._active_workspace_id,
        ImportParams(
            sources=[str(card)], destination=str(root / "archive"),
            verify_by_hash=True, **selection(card),
        ),
    )
    return _selection_observables(result, runner)


def _run_remote_selection_case(root, monkeypatch, specs, selection):
    """Run one selection scenario through the REMOTE copy path."""
    card = _make_card(root, specs)
    runner = FakeRunner()
    result, _ = _run_remote_import(root, monkeypatch, {
        "sources": [str(card)], **selection(card),
    }, runner=runner)
    return _selection_observables(result, runner)


# Three distinct-colored frames: identical bytes would make the later files
# in-batch duplicates, so a scenario meant to read as plain copies would
# quietly exercise the duplicate path instead.
_PARITY_CARD = [
    ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ("DSC_0003.jpg", datetime(2026, 7, 3, 12, 0, 0), "blue"),
]


def test_local_and_remote_agree_on_plain_import(tmp_path, monkeypatch):
    """Behavioral-parity baseline: a plain three-file import (no seeding,
    no selection, no failures) must produce identical behavior observables
    — result counters, safety verdicts, summaries, and DB-level facts
    (photo rows and workspace-linked folders, dest-relative) — through the
    local and remote copy paths. Every seeded parity scenario builds on
    this; if the baseline diverges, nothing built on it is trustworthy."""
    lroot = tmp_path / "l"
    lroot.mkdir()
    rroot = tmp_path / "r"
    rroot.mkdir()
    local = _run_local_behavior_case(lroot, monkeypatch, _PARITY_CARD)
    remote = _run_remote_behavior_case(rroot, monkeypatch, _PARITY_CARD)
    assert local == remote
    # Positive anchor: parity of two broken runs (0 copied on both paths)
    # must not read as a pass.
    assert local["copied"] == 3


def test_remote_import_progress_events_carry_folder_snapshots(
        tmp_path, monkeypatch):
    """Spec decision 1: the remote path historically never sent the
    ``folders={...}`` snapshot, so the Import page's live folder table
    stayed empty for remote imports. Every progress event must now carry
    it, and the final snapshot matches the known terminal per-folder
    result."""
    runner = FakeRunner()
    obs = _run_remote_behavior_case(
        tmp_path, monkeypatch, _PARITY_CARD, runner=runner)
    assert obs["events_missing_folders"] == 0, obs["events_missing_folders"]
    assert obs["folders_final"] == {
        "2026/2026-07-03": {"copied": 3, "skipped_duplicate": 0,
                            "failed": 0}}


def _seed_prior_import(specs):
    """Seed by running a full prior import of ``specs`` through the SAME
    path as the measured run — the seed card lives in a sibling dir."""
    def seed(dest_root, db_path):
        from import_job import ImportParams, run_import_job
        seed_root = dest_root.parent / "seedcard"
        seed_root.mkdir(exist_ok=True)
        card = _make_card(seed_root, specs, card_name="prior")
        db = Database(db_path)
        run_import_job(
            _make_job("seed-import"), FakeRunner(), db_path,
            db._active_workspace_id,
            ImportParams(sources=[str(card)], destination=str(dest_root),
                         verify_by_hash=True))
    seed.seed_specs = specs
    return seed


def _seed_file_drop(specs):
    """Seed by writing files at their template destination WITHOUT
    cataloging them (simulates a prior crashed run). Files are built
    exactly like _make_card's (same PIL call, same mtime) so they are
    byte-identical to the measured card's twins."""
    def seed(dest_root, db_path):
        for spec in specs:
            # Same unpack idiom and same PIL call as _make_card, so the
            # byte-identity contract is upheld by construction.
            name, mtime, color = spec if len(spec) == 3 else (*spec, "red")
            folder = dest_root / mtime.strftime("%Y/%Y-%m-%d")
            folder.mkdir(parents=True, exist_ok=True)
            Image.new("RGB", (16, 16), color).save(str(folder / name))
            ts = mtime.timestamp()
            os.utime(str(folder / name), (ts, ts))
    seed.seed_specs = specs
    return seed


_TWIN = ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red")


def _renamed_twin_case_specs():
    twin = ("X.jpg", datetime(2026, 7, 3, 10, 0, 0), "red")
    card = [twin, ("Y.jpg", datetime(2026, 7, 3, 10, 0, 0), "red")]
    return twin, card


# Import-time snapshot for the scenario list; the per-path tests call the
# helper directly.
_RT_TWIN, _RT_CARD = _renamed_twin_case_specs()

# (id, card specs, seeder, params_kwargs). Each seeds the destination with
# prior state, then measures an import over it on BOTH copy paths. For the
# remote runner the seeders receive the MOUNT base as dest_root, so
# _seed_prior_import's seed import runs locally into the mount — which is
# exactly what "the NAS already holds cataloged photos" looks like from
# this machine.
_BEHAVIOR_PARITY_SCENARIOS = [
    # Duplicate skip against a cataloged twin: same file re-imported.
    ("duplicate_skip", [_TWIN], _seed_prior_import([_TWIN]), {}),
    # Basename collision, different bytes: seed cataloged blue DSC_0001,
    # import red DSC_0001 -> suffix copy DSC_0001_1.jpg.
    ("collision_different_bytes", [_TWIN],
     _seed_prior_import([("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0),
                          "blue")]), {}),
    # Crash-recovery adoption: identical bytes already AT the template
    # path but NOT cataloged (plain file drop, no prior import). Diverged
    # on the adopted row's hash_status until PR 5a folded remote
    # adoptions into ``landed`` (spec divergence 10); the positive
    # control lives in
    # test_local_adoption_uncataloged_dest_twin_current_behavior.
    ("adoption_uncataloged_dest_twin", [_TWIN], _seed_file_drop([_TWIN]),
     {}),
    # Mixed batch: one fresh copy + one duplicate of a cataloged twin.
    ("mixed_fresh_and_duplicate",
     [_TWIN, ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green")],
     _seed_prior_import([_TWIN]), {}),
    # Renamed twin of a cataloged duplicate: card carries X plus a renamed
    # byte-identical Y against a seeded cataloged X — both skip via
    # CatalogIndex.known_hashes on both paths. The decision-5 mechanism
    # notes (why the remote accept-branch _record_checker(source_file)
    # call, removed in PR 3, was behaviorally dead here) live in the
    # per-path characterization pair
    # test_local/_remote_renamed_twin_of_accepted_duplicate_current_behavior.
    ("renamed_twin_skip", _RT_CARD, _seed_prior_import([_RT_TWIN]), {}),
]

def test_behavior_parity_scenarios_are_distinct():
    scenarios = _BEHAVIOR_PARITY_SCENARIOS
    names = [n for n, _s, _seed, _p in scenarios]
    assert len(names) == len(set(names))
    # seed_specs joins the key because two scenarios legitimately share
    # measured specs, seeder family AND params, differing only in what the
    # seeder plants (duplicate_skip vs collision_different_bytes).
    keys = {(repr(s), seed.__qualname__.split(".")[0],
             repr(seed.seed_specs), repr(p))
            for _n, s, seed, p in scenarios}
    assert len(keys) == len(scenarios)


def test_local_and_remote_behavior_results_agree(tmp_path, monkeypatch):
    """CHARACTERIZATION: seeded-destination scenarios must produce the
    same outcome, DB rows included, on both copy paths."""
    mismatches = []
    for name, specs, seed, pkw in _BEHAVIOR_PARITY_SCENARIOS:
        lroot = tmp_path / f"local_{name}"
        lroot.mkdir()
        rroot = tmp_path / f"remote_{name}"
        rroot.mkdir()
        local = _run_local_behavior_case(
            lroot, monkeypatch, specs, seed=seed, params_kwargs=pkw)
        remote = _run_remote_behavior_case(
            rroot, monkeypatch, specs, seed=seed, params_kwargs=pkw)
        if local != remote:
            mismatches.append((name, local, remote))
    assert not mismatches, "\n".join(
        f"{n}:\n  local ={l}\n  remote={r}" for n, l, r in mismatches)


def test_behavior_parity_scenarios_exercise_their_branches(
        tmp_path, monkeypatch):
    """Positive control: pin each scenario's expected local outcome so a
    branch that stops firing fails here, not silently in parity."""
    seen = {}
    for name, specs, seed, pkw in _BEHAVIOR_PARITY_SCENARIOS:
        root = tmp_path / name
        root.mkdir()
        seen[name] = _run_local_behavior_case(
            root, monkeypatch, specs, seed=seed, params_kwargs=pkw)

    assert seen["duplicate_skip"]["skipped_duplicate"] == 1
    assert seen["duplicate_skip"]["copied"] == 0
    assert seen["duplicate_skip"]["safe_to_format"] is True

    assert seen["collision_different_bytes"]["copied"] == 1
    assert any(fn == "DSC_0001_1.jpg"
               for _rel, fn, _fh, _hs in
               seen["collision_different_bytes"]["db_photos"])

    assert seen["mixed_fresh_and_duplicate"]["copied"] == 1
    assert seen["mixed_fresh_and_duplicate"]["skipped_duplicate"] == 1
    assert seen["mixed_fresh_and_duplicate"]["safe_to_format"] is True

    assert seen["renamed_twin_skip"]["skipped_duplicate"] == 2
    assert seen["renamed_twin_skip"]["copied"] == 0
    assert seen["renamed_twin_skip"]["safe_to_format"] is True
    # The adoption scenario's positive control lives in
    # test_local_adoption_uncataloged_dest_twin_current_behavior below.


def test_local_adoption_uncataloged_dest_twin_current_behavior(
        tmp_path, monkeypatch):
    """Positive control for the adoption parity scenario: local
    crash-recovery adoption folds the adopted file into ``landed`` with
    its verified hash, so the verify_by_hash catalog stamp marks the
    adopted row ``hash_status='ok'``.

    Since PR 5a folded remote adoptions into ``landed`` too (spec
    divergence 10), both paths share this shape; cross-path equality is
    the parity suite's job, and this test pins the absolute values the
    parity comparison alone could not (two identically-wrong paths
    still agree)."""
    from import_dedup import compute_file_hash
    _name, specs, seed, pkw = next(
        s for s in _BEHAVIOR_PARITY_SCENARIOS
        if s[0] == "adoption_uncataloged_dest_twin")
    lroot = tmp_path / "l"
    lroot.mkdir()
    rroot = tmp_path / "r"
    rroot.mkdir()
    obs = _run_local_behavior_case(
        lroot, monkeypatch, specs, seed=seed, params_kwargs=pkw)
    remote = _run_remote_behavior_case(
        rroot, monkeypatch, specs, seed=seed, params_kwargs=pkw)
    # Full-dict equality (db_photos included, since PR 5a's fold) — a
    # belt over the parity suite's own comparison of this scenario.
    assert obs == remote
    assert obs["skipped_duplicate"] == 1
    assert obs["copied"] == 0
    assert obs["safe_to_format"] is True
    # The adopted file gained a photo row, stamped verified. Hash value
    # ties the row to the exact bytes on disk so a stale/wrong hash
    # persisted with hash_status='ok' would still fail this assertion.
    adopted = lroot / "archive" / "2026" / "2026-07-03" / "DSC_0001.jpg"
    expected_hash = compute_file_hash(str(adopted))
    assert obs["db_photos"] == {
        ("2026/2026-07-03", "DSC_0001.jpg", expected_hash, "ok")}
    assert obs["db_linked_folders"] == {"2026/2026-07-03"}


def test_remote_adoption_gets_card_side_wc_override(tmp_path, monkeypatch):
    """Flip 2 of the PR 5a fold: adopted mount files now carry a
    card-side working-copy source override, exactly like local
    adoptions always did (they live in ``landed``, whose entries feed
    ``wc_source_paths``). Pre-fold, remote adoptions lived outside
    ``landed`` and WC extraction read the mount copy instead."""
    import scanner as _scanner

    captured = {}

    def spy_extract(*args, **kwargs):
        captured["source_paths"] = dict(kwargs.get("source_paths") or {})
        return None

    monkeypatch.setattr(_scanner, "_extract_working_copies", spy_extract)

    _name, specs, seed, pkw = next(
        s for s in _BEHAVIOR_PARITY_SCENARIOS
        if s[0] == "adoption_uncataloged_dest_twin")
    obs = _run_remote_behavior_case(
        tmp_path, monkeypatch, specs, seed=seed,
        params_kwargs={**pkw, "vireo_dir": str(tmp_path / "vdir")})

    assert obs["skipped_duplicate"] == 1, obs
    assert captured, "working-copy extraction never ran"
    adopted_dest = str(
        tmp_path / "mount" / "2026" / "2026-07-03" / "DSC_0001.jpg")
    card_src = str(tmp_path / "card" / "DSC_0001.jpg")
    assert adopted_dest in captured["source_paths"], captured["source_paths"]
    src_path, src_size, src_mtime_ns = captured["source_paths"][adopted_dest]
    assert src_path == card_src
    st = os.stat(card_src)
    assert (src_size, src_mtime_ns) == (st.st_size, st.st_mtime_ns)


def test_remote_adopted_file_scan_mismatch_fails_with_mount_subject(
        tmp_path, monkeypatch):
    """Flip 3 of the PR 5a fold: an adopted file whose mount bytes no
    longer match at catalog-scan time is failed with the MOUNT dest
    path as the ``unsafe_files`` subject and the landed stamping loop's
    wording — pre-fold, the separate ``adopted_paths`` validation pass
    reported the CARD source path with adoption-specific wording."""
    import scanner as _scanner

    real_scan = _scanner.scan
    mutated = {}

    def mutating_scan(*args, **kwargs):
        # The mount file is swapped between adoption's hash check and
        # the catalog scan (stale/misbehaving share).
        adopted = (tmp_path / "mount" / "2026" / "2026-07-03"
                   / "DSC_0001.jpg")
        if not mutated and adopted.exists():
            Image.new("RGB", (16, 16), "blue").save(str(adopted))
            mutated["done"] = True
        return real_scan(*args, **kwargs)

    monkeypatch.setattr(_scanner, "scan", mutating_scan)

    _name, specs, seed, pkw = next(
        s for s in _BEHAVIOR_PARITY_SCENARIOS
        if s[0] == "adoption_uncataloged_dest_twin")
    obs = _run_remote_behavior_case(
        tmp_path, monkeypatch, specs, seed=seed, params_kwargs=pkw)

    assert mutated, "the scan wrapper never saw the adopted file"
    # Rolled back exactly once (0, never -1) and reported as failed.
    assert obs["skipped_duplicate"] == 0, obs
    assert obs["failed"] == 1, obs
    assert obs["safe_to_format"] is False, obs
    adopted_dest = str(
        tmp_path / "mount" / "2026" / "2026-07-03" / "DSC_0001.jpg")
    reasons = {p: r for p, r in obs["unsafe"]}
    assert adopted_dest in reasons, obs["unsafe"]
    assert "scanned mount row hash does not match" in reasons[adopted_dest]


def test_local_renamed_twin_of_accepted_duplicate_current_behavior(
        tmp_path, monkeypatch):
    """CHARACTERIZATION for spec decision 5 (local half). The local path
    does NOT register accepted duplicates with the checker — its accept
    branch never had a counterpart to the remote path's source-only
    ``_record_checker(source_file)`` call (removed in PR 3).

    ACTUAL: both files skip anyway. The seed import's post-import scan
    catalogs the twin WITH its file_hash, so ``CatalogIndex.from_db``
    puts the shared hash in ``known_hashes`` and renamed Y matches in
    ``DuplicateChecker.match`` (import_dedup.py:369-376) straight from
    the catalog — the checker's per-run ``_seen_hashes`` is never needed.
    The skip is then byte-backed by re-hashing the on-disk twin
    (``_hash_twin_rows`` + ``_hash_dest_file``), same as the plain
    duplicate_skip scenario.

    A ``verify_by_hash=False`` variant was probed and dropped: it lands
    in the SAME world on both paths (2 skipped, 0 copied). The metadata
    key — the one place ``_seen_keys`` could matter — never forms,
    because these harness JPEGs carry no EXIF and capture time is
    EXIF-only with no mtime fallback
    (``import_dedup.source_capture_timestamps``), so ``match`` falls
    through to the fallback content check and again hits
    ``known_hashes``. And even with EXIF, ``record``-ing X would add
    X's key, which renamed Y (different filename) can never match. The
    remote accept-branch call was therefore behaviorally unobservable in
    this cataloged-twin geometry in both verify modes, which is why PR 3
    removed it as a no-op (decision 5,
    docs/superpowers/specs/2026-08-06-import-path-unification-design.md).
    """
    from import_dedup import compute_file_hash
    twin, card = _renamed_twin_case_specs()
    obs = _run_local_behavior_case(
        tmp_path, monkeypatch, card, seed=_seed_prior_import([twin]))
    assert obs["skipped_duplicate"] == 2, obs
    assert obs["copied"] == 0, obs
    assert obs["safe_to_format"] is True, obs
    # Only the seeded twin is cataloged — renamed Y left no photo row.
    seeded = tmp_path / "archive" / "2026" / "2026-07-03" / "X.jpg"
    expected_hash = compute_file_hash(str(seeded))
    assert obs["db_photos"] == {
        ("2026/2026-07-03", "X.jpg", expected_hash, "ok")}, obs
    # The seed import linked the day folder; the all-skip run adds none.
    assert obs["db_linked_folders"] == {"2026/2026-07-03"}, obs


def test_remote_renamed_twin_of_accepted_duplicate_current_behavior(
        tmp_path, monkeypatch):
    """CHARACTERIZATION for spec decision 5 (remote half). The remote path
    used to register accepted duplicates via a source-only
    ``_record_checker(source_file)`` call in its duplicate-accept branch;
    PR 3 removed that call, and this test was the tripwire that pinned
    the removal as a no-op — it passed unchanged before and after,
    because the call was behaviorally dead in this geometry.

    ACTUAL: identical to the local half (2 skipped, 0 copied). Renamed Y
    matches via ``CatalogIndex.known_hashes`` (the seed import's scan
    cataloged the twin's hash), not via the ``_seen_hashes`` entry the
    removed call added — ``match`` checks ``known_hashes`` first and
    either membership yields the same ``('hash', …)`` token
    (import_dedup.py:369-376). The call also never populated the
    ``run_dest_folders`` intra-run fast path (it passed no dest_folder),
    so acceptance still goes through the on-disk twin re-hash on both
    paths. A ``verify_by_hash=False`` probe landed in the same world for
    the same reason — see the local twin test's docstring for the traced
    no-EXIF mechanism. Decision 5's removal was a proven no-op here.
    """
    from import_dedup import compute_file_hash
    twin, card = _renamed_twin_case_specs()
    obs = _run_remote_behavior_case(
        tmp_path, monkeypatch, card, seed=_seed_prior_import([twin]))
    assert obs["skipped_duplicate"] == 2, obs
    assert obs["copied"] == 0, obs
    assert obs["safe_to_format"] is True, obs
    seeded = tmp_path / "mount" / "2026" / "2026-07-03" / "X.jpg"
    expected_hash = compute_file_hash(str(seeded))
    assert obs["db_photos"] == {
        ("2026/2026-07-03", "X.jpg", expected_hash, "ok")}, obs
    # The seed import linked the day folder; the all-skip run adds none.
    assert obs["db_linked_folders"] == {"2026/2026-07-03"}, obs


def _sel(names, previewed, checked, extra=()):
    """Build the selection kwargs for a card, by basename."""
    def build(card):
        paths = {str(card / n) for n in names}
        paths |= {str(card / n) for n in extra}
        return {
            "include_paths": paths,
            "previewed_count": previewed,
            "checked_count": checked,
        }
    return build


# (id, card specs, selection builder). Each exercises one branch of the
# shared selection logic; between them they cover both ``deselected``
# branches, both drift counters, the two summary forms, and the
# no-selection / empty-selection boundary.
_SELECTION_PARITY_SCENARIOS = [
    ("no_selection", _PARITY_CARD, lambda card: {}),
    ("full_selection", _PARITY_CARD,
     _sel(["DSC_0001.jpg", "DSC_0002.jpg", "DSC_0003.jpg"], 3, 3)),
    ("empty_selection", _PARITY_CARD,
     lambda card: {"include_paths": set(), "previewed_count": 3,
                   "checked_count": 0}),
    ("deselect_one", _PARITY_CARD,
     _sel(["DSC_0001.jpg", "DSC_0002.jpg"], 3, 2)),
    ("deselect_plural", _PARITY_CARD, _sel(["DSC_0001.jpg"], 3, 1)),
    ("vanished", _PARITY_CARD,
     _sel(["DSC_0001.jpg", "DSC_0002.jpg", "DSC_0003.jpg"], 4, 4,
          extra=["GONE.jpg"])),
    ("vanished_without_previewed_count", _PARITY_CARD,
     lambda card: {
         "include_paths": {str(card / "DSC_0001.jpg"), str(card / "GONE.jpg")},
         "previewed_count": None, "checked_count": None,
     }),
    ("appeared", _PARITY_CARD, _sel(["DSC_0001.jpg"], 1, 1)),
    ("negative_deselected", _PARITY_CARD,
     _sel(["DSC_0001.jpg", "DSC_0002.jpg", "DSC_0003.jpg"], 2, 3)),
    ("mixed_appear_and_vanish", _PARITY_CARD,
     _sel(["DSC_0001.jpg"], 2, 2, extra=["GONE.jpg"])),
    # Deselect AND vanish: the compound red pill, and the only scenario that
    # emits two selection entries, so it is what pins their render order.
    ("deselected_and_vanished", _PARITY_CARD,
     _sel(["DSC_0001.jpg"], 3, 2, extra=["GONE.jpg"])),
    ("include_paths_without_checked_count", _PARITY_CARD,
     lambda card: {"include_paths": {str(card / "DSC_0001.jpg")},
                   "previewed_count": 3, "checked_count": None}),
    ("checked_count_without_include_paths", _PARITY_CARD,
     lambda card: {"checked_count": 1}),
]


def test_selection_parity_scenarios_are_distinct():
    """Guard for the parity test below: two scenarios that collapse onto the
    same payload still both run, but the list covers one fewer branch than
    its length suggests — a coverage gap that reads as coverage."""
    seen = set()
    for name, _specs, builder in _SELECTION_PARITY_SCENARIOS:
        class _C:
            def __truediv__(self, other):
                return "/card/" + other
        key = repr(sorted(
            (k, sorted(v) if isinstance(v, set) else v)
            for k, v in builder(_C()).items()
        ))
        assert key not in seen, f"{name} duplicates an earlier scenario"
        seen.add(key)
    assert len(seen) == len(_SELECTION_PARITY_SCENARIOS)


def test_local_and_remote_selection_results_agree(tmp_path, monkeypatch):
    """CHARACTERIZATION: the same selection payload must produce the same
    selection outcome on both copy paths.

    Runs every scenario through both entry points in its own tmp dir and
    compares discovered/copied/verdict/drift counts, the (path, reason) set
    of ``unsafe_files``, the import step summary, and the progress totals.

    This is asserted as a whole-dict equality on purpose: a per-key check
    added one at a time is how a divergence in the key nobody thought to
    list survives.
    """
    mismatches = []
    for name, specs, builder in _SELECTION_PARITY_SCENARIOS:
        local_root = tmp_path / f"local_{name}"
        local_root.mkdir()
        remote_root = tmp_path / f"remote_{name}"
        remote_root.mkdir()
        local = _run_local_selection_case(
            local_root, monkeypatch, specs, builder)
        remote = _run_remote_selection_case(
            remote_root, monkeypatch, specs, builder)
        if local != remote:
            mismatches.append((name, local, remote))
    assert not mismatches, "\n".join(
        f"{n}:\n  local ={l}\n  remote={r}" for n, l, r in mismatches
    )


def test_selection_parity_scenarios_actually_exercise_the_branches(
        tmp_path, monkeypatch):
    """Positive control for the parity test.

    Equality between two paths is satisfiable by both being wrong in the
    same way — or by every scenario producing an identical, boring result.
    Pin the outcomes the scenarios are supposed to produce so a selection
    branch that stops firing is caught here rather than passing parity.
    """
    seen = {}
    for name, specs, builder in _SELECTION_PARITY_SCENARIOS:
        root = tmp_path / name
        root.mkdir()
        seen[name] = _run_local_selection_case(
            root, monkeypatch, specs, builder)

    # A whole-card import with nothing selected is the green baseline.
    assert seen["no_selection"]["safe_to_format"] is True
    assert seen["no_selection"]["copied"] == 3
    assert seen["no_selection"]["summary"] == (
        "3 copied, 0 already present, 0 failed of 3 discovered")

    # Selecting everything is still green, and switches summary form.
    assert seen["full_selection"]["safe_to_format"] is True
    assert seen["full_selection"]["copied"] == 3
    assert seen["full_selection"]["summary"] == (
        "3 selected of 3 discovered, 3 copied, 0 already present, 0 failed")

    # Every drift scenario is red, and names its own reason.
    reds = {
        "empty_selection": "Deselected files",
        "deselect_one": "Deselected files",
        "deselect_plural": "Deselected files",
        "vanished": "Files missing at import time",
        "vanished_without_previewed_count": "Files missing at import time",
        "appeared": "Files added after preview",
        "negative_deselected": "Selection count mismatch",
        "mixed_appear_and_vanish": "Files missing at import time",
        "deselected_and_vanished": "Files missing at import time",
    }
    for name, path in reds.items():
        assert seen[name]["safe_to_format"] is False, name
        assert path in {p for p, _ in seen[name]["unsafe"]}, name

    # The compound case emits BOTH lines, in the order the helper appends
    # them — this is the render order parity now compares.
    assert seen["deselected_and_vanished"]["unsafe_order"] == [
        "Deselected files", "Files missing at import time",
    ]

    # Drift counters are nonzero where the scenario says they should be.
    assert seen["appeared"]["files_appeared"] == 2
    assert seen["appeared"]["files_vanished"] == 0
    assert seen["vanished"]["files_vanished"] == 1
    assert seen["mixed_appear_and_vanish"]["files_vanished"] == 1
    assert seen["mixed_appear_and_vanish"]["files_appeared"] == 1

    # Progress totals are the queued work, not the card.
    assert seen["deselect_plural"]["copy_totals"] == {1}
    assert seen["no_selection"]["copy_totals"] == {3}

    # Both degraded summary forms fall back to the plain wording.
    assert "selected" not in seen["include_paths_without_checked_count"][
        "summary"]
    assert "selected" not in seen["checked_count_without_include_paths"][
        "summary"]
    # ...and a bare ``checked_count`` must not filter the copy set.
    assert seen["checked_count_without_include_paths"]["copied"] == 3


def _symlinked_card(tmp_path, specs):
    """A card reached through a symlinked directory.

    ``pytest``'s ``tmp_path`` is already fully resolved, so an ordinary card
    cannot tell ``str(f)`` apart from ``os.path.realpath(str(f))``. This one
    can: the enumerated paths run through the symlink, their realpaths do
    not, and the selection filter matches on the former.
    """
    import pytest

    real = _make_card(tmp_path, specs, card_name="real_card")
    link = tmp_path / "link_card"
    try:
        os.symlink(str(real), str(link), target_is_directory=True)
    except (OSError, NotImplementedError, AttributeError):
        pytest.skip("symlink creation not supported on this platform")
    assert os.path.realpath(str(link)) != str(link), (
        "the symlink resolved to itself; this test cannot distinguish "
        "realpath from the literal path"
    )
    return link


def test_selection_filter_matches_unresolved_paths_local(tmp_path):
    """The selection filter must NOT resolve symlinks.

    The caller's paths come from ``discover_source_files`` over the raw
    source string, so they run through the symlink. Realpath-ing inside the
    filter empties it and copies nothing — and every other test in this file
    passes under that mutation, because ``tmp_path`` is already resolved.
    """
    from import_job import ImportParams, run_import_job

    link = _symlinked_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, db._active_workspace_id,
        ImportParams(
            sources=[str(link)], destination=str(tmp_path / "archive"),
            verify_by_hash=True,
            include_paths={str(link / "DSC_0001.jpg"),
                           str(link / "DSC_0002.jpg")},
            previewed_count=2, checked_count=2,
        ),
    )
    assert result["copied"] == 2, "realpath in the filter would copy nothing"
    assert result["files_vanished"] == 0
    assert result["safe_to_format"] is True


def test_selection_filter_matches_unresolved_paths_remote(
        tmp_path, monkeypatch):
    """Remote edition of the symlink guard — same mutation, same path."""
    link = _symlinked_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])
    result, calls = _run_remote_import(tmp_path, monkeypatch, {
        "sources": [str(link)],
        "include_paths": {str(link / "DSC_0001.jpg"),
                          str(link / "DSC_0002.jpg")},
        "previewed_count": 2, "checked_count": 2,
    })
    assert result["copied"] == 2, "realpath in the filter would rsync nothing"
    assert calls["rsync"], "nothing was rsynced at all"
    assert result["files_vanished"] == 0
    assert result["safe_to_format"] is True



def test_local_import_refuses_when_mount_root_absent(tmp_path, monkeypatch):
    """The LOCAL copy path needs the same mount-root guard as the remote one.

    ``_run_remote_import_job`` refuses to ``os.makedirs`` into an absent
    mount root (PR #1113), but ``run_import_job``'s own batch loop had no
    such check — it called ``os.makedirs(dest_folder)`` unconditionally.
    On a platform where the mount point survives unmount (Linux
    ``/mnt/<name>``) that silently builds a shadow tree on the internal
    disk and copies the card into it, where the photos look imported but
    vanish the moment the real share remounts.
    """
    import pipeline_job as _pj
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])
    dest = str(tmp_path / "Volumes_NAS_Photos")

    # The real helper only fires for /Volumes/* | /mnt/* | /media/*/*
    # shapes, so stub it to call our tmp destination's root missing —
    # same approach as test_remote_import_refuses_when_mount_root_absent.
    monkeypatch.setattr(
        _pj, "_missing_archive_mount_root", lambda path: "/Volumes/NAS",
    )

    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=dest,
    ))

    assert result["copied"] == 0, result
    assert result["failed"] == 2, result
    assert result["safe_to_format"] is False, result
    assert result["unsafe_files"], result
    assert all(
        "/Volumes/NAS" in u["reason"] and "not available" in u["reason"]
        for u in result["unsafe_files"]
    ), result["unsafe_files"]
    assert not os.path.exists(dest), (
        f"shadow directory was created at {dest}: the guard failed to "
        "stop os.makedirs"
    )


def test_local_import_stops_when_mount_disappears_mid_run(
        tmp_path, monkeypatch):
    """A mount that drops *during* a long import must stop the next batch.

    The guard was a start-of-job preflight computed once outside the
    batch loop, so an archive that unmounted two hours into a run (the
    2026-07-30 SMB outage) sailed straight into ``os.makedirs``. Re-check
    per batch so the outage is caught when it happens.
    """
    import pipeline_job as _pj
    from import_job import ImportParams

    # Two dates -> two batches, so the mount can drop between them.
    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 4, 9, 0, 0), "blue"),
    ])
    dest = str(tmp_path / "archive")
    os.makedirs(dest, exist_ok=True)

    calls = {"n": 0}

    def flaky_mount(path):
        calls["n"] += 1
        # Mounted for the first batch, gone for every batch after it.
        return None if calls["n"] == 1 else "/Volumes/NAS"

    monkeypatch.setattr(_pj, "_missing_archive_mount_root", flaky_mount)

    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=dest,
    ))

    assert calls["n"] >= 2, (
        "the mount root was checked once for the whole job, so a mid-run "
        "unmount can never be detected; it must be re-checked per batch"
    )
    assert result["copied"] == 1, result
    assert result["failed"] == 1, result
    assert result["safe_to_format"] is False, result
    assert any(
        "/Volumes/NAS" in u["reason"] and "not available" in u["reason"]
        for u in result["unsafe_files"]
    ), result["unsafe_files"]


def test_remote_import_stops_when_mount_disappears_mid_run(
        tmp_path, monkeypatch):
    """Same mid-run re-check for the remote path, whose guard was also
    computed once outside the batch loop."""
    import pipeline_job as _pj
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 4, 9, 0, 0), "blue"),
    ])

    probes = {"n": 0}

    def flaky_mount(path):
        probes["n"] += 1
        return None if probes["n"] == 1 else "/Volumes/NAS"

    monkeypatch.setattr(_pj, "_missing_archive_mount_root", flaky_mount)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra,
        ),
    )

    assert probes["n"] >= 2, (
        "remote mount root was checked once for the whole job; a mid-run "
        "unmount can never be detected"
    )
    assert result["copied"] == 1, result
    assert result["failed"] == 1, result
    assert result["safe_to_format"] is False, result


def test_local_import_mount_loss_still_advances_progress(tmp_path, monkeypatch):
    """A batch rejected by the mount guard must still count its files as
    emitted.

    ``emitted`` only advances inside the per-file copy loop, which the
    guard skips. Without bumping it there, an import whose share drops
    after the first batch leaves the progress bar frozen at the last
    copied file while the job silently fails hundreds more — a stalled
    bar reads as "still working", which is the opposite of what happened.
    """
    import pipeline_job as _pj
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 4, 9, 0, 0), "blue"),
        ("DSC_0003.jpg", datetime(2026, 7, 5, 9, 0, 0), "green"),
    ])
    dest = str(tmp_path / "archive")
    os.makedirs(dest, exist_ok=True)

    calls = {"n": 0}

    def flaky_mount(path):
        calls["n"] += 1
        return None if calls["n"] == 1 else "/Volumes/NAS"

    monkeypatch.setattr(_pj, "_missing_archive_mount_root", flaky_mount)

    runner = FakeRunner()
    db, ws_id, result = _run_import(
        tmp_path,
        ImportParams(sources=[str(card)], destination=dest),
        runner=runner,
    )

    assert result["discovered"] == 3, result
    progress = [
        data for _jid, etype, data in runner.events if etype == "progress"
    ]
    assert progress, "no progress events emitted"
    assert progress[-1]["current"] == 3, (
        f"progress stalled at {progress[-1]['current']} of 3 after the "
        "mount dropped; every discovered file must be accounted for"
    )


def test_local_import_survives_makedirs_failure(tmp_path):
    """``os.makedirs`` on the destination must never kill the job.

    The mount-root guard only recognizes a vacated mount point. It can't
    see a stale-but-present mount (Linux ``/mnt/<name>``), a read-only
    parent, or a permission change — and the 2026-07-30 incident was
    precisely an uncaught ``PermissionError`` out of this call, which
    tore down the whole background job after two hours of work. Whatever
    the cause, it belongs in the per-file failure bucket with the card
    still marked unsafe to format.
    """
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])
    dest = str(tmp_path / "archive")
    # A regular FILE where the batch's destination folder must go, so the
    # real os.makedirs raises FileExistsError on every platform.
    os.makedirs(os.path.join(dest, "2026"), exist_ok=True)
    with open(os.path.join(dest, "2026", "2026-07-03"), "w") as fh:
        fh.write("not a directory")

    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=dest,
    ))

    assert result["copied"] == 0, result
    assert result["failed"] == 1, result
    assert result["safe_to_format"] is False, result
    assert any(
        "2026-07-03" in u["reason"] for u in result["unsafe_files"]
    ), result["unsafe_files"]


def test_local_import_stops_when_mount_point_persists_after_unmount(
        tmp_path, monkeypatch):
    """A Linux-shaped unmount must stop the import, not shadow-write.

    ``_missing_archive_mount_root`` only sees a mount point that vanished
    (macOS ejects remove ``/Volumes/<share>``). Linux keeps ``/mnt/<name>``
    as an empty directory, so the destination still "exists", os.makedirs
    succeeds, and the remaining card files land on the system disk under
    a stale mount point — where safe_to_format could go green over photos
    that disappear the moment the real archive remounts.
    See PR #1394 review (Codex P1 r3687190865).
    """
    import pipeline_job as _pj
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 4, 9, 0, 0), "blue"),
    ])
    dest = str(tmp_path / "mnt_NAS")
    os.makedirs(dest, exist_ok=True)

    # The destination is a real mount at job start, then the share drops
    # while leaving the directory in place.
    monkeypatch.setattr(
        _pj, "_archive_mount_root_candidates",
        lambda path: [dest] if str(path).startswith(dest) else [],
    )
    real_ismount = os.path.ismount
    second_batch_dir = os.path.join(dest, "2026", "2026-07-04")

    def fake_ismount(p):
        if str(p) != dest:
            return real_ismount(p)
        # Detach cleanly BETWEEN batches: still mounted while batch one
        # copies and catalogs, gone by the time batch two starts writing.
        # Keyed on observable state (has batch two's folder been created)
        # rather than a probe count, so adding or removing a probe cannot
        # silently retime the test — and deliberately later than "batch
        # one's file landed", which would instead exercise a mid-batch
        # detach and correctly fail batch one too.
        return not os.path.exists(second_batch_dir)

    monkeypatch.setattr(os.path, "ismount", fake_ismount)

    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=dest,
    ))

    assert result["copied"] == 1, result
    assert result["failed"] == 1, result
    assert result["safe_to_format"] is False, result
    # "detached" is common to the batch-level and per-file guard messages.
    # Which one fires depends on exactly when the share drops relative to
    # the batch boundary, and this test cares that the detach is reported
    # at all, not which probe noticed it first.
    assert any(
        "detached" in u["reason"] for u in result["unsafe_files"]
    ), result["unsafe_files"]


def test_local_import_allows_a_plain_directory_that_looks_mount_shaped(
        tmp_path, monkeypatch):
    """An ordinary directory under /mnt must still be a valid destination.

    The guard keys on a mounted → unmounted transition precisely so a
    hand-made local ``/mnt/photos`` (never a mount, so ismount is False
    throughout) is not refused. A bare "is it mounted?" check would break
    this setup.
    """
    import pipeline_job as _pj
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 4, 9, 0, 0), "blue"),
    ])
    dest = str(tmp_path / "mnt_photos")
    os.makedirs(dest, exist_ok=True)

    # Mount-shaped path, but ismount is False the whole way through.
    monkeypatch.setattr(
        _pj, "_archive_mount_root_candidates",
        lambda path: [dest] if str(path).startswith(dest) else [],
    )

    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=dest,
    ))

    assert result["copied"] == 2, result
    assert result["failed"] == 0, result


def test_remote_import_stops_when_mount_point_persists_after_unmount(
        tmp_path, monkeypatch):
    """Same persistent-unmount guard on the remote path.

    Worse here than locally: rsync keeps succeeding against the NAS while
    the per-batch scan reads the empty local shadow, so the bytes land
    remotely and the catalog records nothing.
    """
    import pipeline_job as _pj
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 4, 9, 0, 0), "blue"),
    ])
    mount_base = ra["mount_base"]

    monkeypatch.setattr(
        _pj, "_archive_mount_root_candidates",
        lambda path: [mount_base] if str(path).startswith(mount_base) else [],
    )
    real_ismount = os.path.ismount
    first_landed = os.path.join(
        mount_base, "2026", "2026-07-03", "DSC_0001.jpg")

    def fake_ismount(p):
        if str(p) != mount_base:
            return real_ismount(p)
        # Detaches once the first batch has landed. Keyed on observable
        # state rather than a probe count so the test can't be retimed by
        # a probe being added elsewhere.
        return not os.path.exists(first_landed)

    monkeypatch.setattr(os.path, "ismount", fake_ismount)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=mount_base, remote_target=ra,
        ),
    )

    assert result["copied"] == 1, result
    assert result["failed"] == 1, result
    assert result["safe_to_format"] is False, result
    assert any(
        "no longer mounted" in u["reason"] for u in result["unsafe_files"]
    ), result["unsafe_files"]


def test_local_import_takes_mount_baseline_before_discovery(
        tmp_path, monkeypatch):
    """The baseline must predate discovery, not follow it.

    Discovery, catalog-index construction and timestamp extraction all run
    before the copy loop and are slow against a network archive — the
    2026-07-30 incident spent eight minutes just enumerating the
    destination. A share that detaches during that window would be
    recorded as ``False`` by a late baseline, so the mounted -> unmounted
    transition never fires and the guard is silently disarmed for the
    whole run. See PR #1396 review (Codex P1 r3687336684).
    """
    import import_job as _ij
    import pipeline_job as _pj
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 4, 9, 0, 0), "blue"),
    ])
    dest = str(tmp_path / "mnt_NAS")
    os.makedirs(dest, exist_ok=True)

    monkeypatch.setattr(
        _pj, "_archive_mount_root_candidates",
        lambda path: [dest] if str(path).startswith(dest) else [],
    )

    state = {"mounted": True}
    real_ismount = os.path.ismount
    monkeypatch.setattr(
        os.path, "ismount",
        lambda p: state["mounted"] if str(p) == dest else real_ismount(p),
    )

    # The share drops WHILE discovery is running.
    real_discover = _ij.discover_source_files

    def discover_then_detach(*a, **kw):
        result = list(real_discover(*a, **kw))
        state["mounted"] = False
        return result

    monkeypatch.setattr(_ij, "discover_source_files", discover_then_detach)

    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=dest,
    ))

    assert result["copied"] == 0, result
    assert result["failed"] == 2, result
    assert result["safe_to_format"] is False, result
    assert all(
        "no longer mounted" in u["reason"] for u in result["unsafe_files"]
    ), result["unsafe_files"]


def test_remote_import_takes_mount_baseline_before_discovery(
        tmp_path, monkeypatch):
    """Remote path takes its baseline before discovery too.

    Same disarming window as the local path — Codex flagged both in
    PR #1396 review (P1 r3687336684).
    """
    import import_job as _ij
    import pipeline_job as _pj
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 4, 9, 0, 0), "blue"),
    ])
    mount_base = ra["mount_base"]

    monkeypatch.setattr(
        _pj, "_archive_mount_root_candidates",
        lambda path: [mount_base] if str(path).startswith(mount_base) else [],
    )
    state = {"mounted": True}
    real_ismount = os.path.ismount
    monkeypatch.setattr(
        os.path, "ismount",
        lambda p: state["mounted"] if str(p) == mount_base else real_ismount(p),
    )

    real_discover = _ij.discover_source_files

    def discover_then_detach(*a, **kw):
        result = list(real_discover(*a, **kw))
        state["mounted"] = False
        return result

    monkeypatch.setattr(_ij, "discover_source_files", discover_then_detach)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=mount_base, remote_target=ra,
        ),
    )

    assert result["copied"] == 0, result
    assert result["failed"] == 2, result
    assert result["safe_to_format"] is False, result
    # Drop the ``<remote>`` honesty-gate marker (verify_by_hash advice) so
    # the mount reason is asserted over a non-empty set rather than
    # vacuously — same filtering as
    # ``test_remote_import_refuses_when_mount_root_absent``.
    non_remote = [u for u in result["unsafe_files"] if u["path"] != "<remote>"]
    assert non_remote, result["unsafe_files"]
    assert all(
        "no longer mounted" in u["reason"] for u in non_remote
    ), result["unsafe_files"]


def test_local_import_detects_mount_loss_inside_a_single_batch(
        tmp_path, monkeypatch):
    """A detach mid-batch must not ride out to the end of the batch.

    Batch-boundary probing alone leaves a whole folder unguarded, and when
    there is only ONE batch there is no later boundary at all — every
    remaining file is copied, hash-verified and cataloged into the local
    shadow, and safe_to_format can still go green over a card that is the
    only real copy. See PR #1396 review (Codex P1 r3687401641).
    """
    import pipeline_job as _pj
    from import_job import ImportParams

    # One batch: four files, same capture date. Distinct colours so each
    # is genuinely copied — identical bytes would route files 2-4 through
    # the intra-run duplicate gate and never exercise the copy path.
    card = _make_card(tmp_path, [
        (f"DSC_000{i}.jpg", datetime(2026, 7, 3, 10, i, 0), colour)
        for i, colour in enumerate(
            ("red", "green", "blue", "yellow"), start=1)
    ])
    dest = str(tmp_path / "mnt_NAS")
    os.makedirs(dest, exist_ok=True)

    monkeypatch.setattr(
        _pj, "_archive_mount_root_candidates",
        lambda path: [dest] if str(path).startswith(dest) else [],
    )

    state = {"mounted": True, "probes": 0}
    real_ismount = os.path.ismount

    def fake_ismount(p):
        if str(p) != dest:
            return real_ismount(p)
        state["probes"] += 1
        # Baseline + batch-level check + first two files stay mounted;
        # the share drops partway through the single batch.
        if state["probes"] > 4:
            state["mounted"] = False
        return state["mounted"]

    monkeypatch.setattr(os.path, "ismount", fake_ismount)

    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=dest,
    ))

    assert result["discovered"] == 4, result
    # Whatever the split, nothing may be left claiming a successful
    # archive copy, and the card must not be declared safe to format.
    assert result["copied"] == 0, result
    assert result["failed"] == 4, result
    assert result["safe_to_format"] is False, result
    assert any(
        "detached" in u["reason"] for u in result["unsafe_files"]
    ), result["unsafe_files"]

    # Nothing from this run may be cataloged under the shadow directory.
    rows = _photo_rows(db)
    assert rows == [], rows


def test_local_import_detects_mount_loss_during_the_final_file(
        tmp_path, monkeypatch):
    """A detach during the LAST file's copy must still be caught.

    The pre-file probe runs before each copy, so with a single-file batch
    (or on the last file of any batch) there is no next iteration to trip
    it. Without a post-loop probe the copy, hash verification and catalog
    scan all succeed against the local shadow and safe_to_format can go
    true. See PR #1396 review (Codex P1 r3687456172).
    """
    import pipeline_job as _pj
    from import_job import ImportParams

    # A single file -> a single batch -> exactly one loop iteration.
    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
    ])
    dest = str(tmp_path / "mnt_NAS")
    os.makedirs(dest, exist_ok=True)

    monkeypatch.setattr(
        _pj, "_archive_mount_root_candidates",
        lambda path: [dest] if str(path).startswith(dest) else [],
    )
    real_ismount = os.path.ismount
    landed_path = os.path.join(dest, "2026", "2026-07-03", "DSC_0001.jpg")

    def fake_ismount(p):
        if str(p) != dest:
            return real_ismount(p)
        # Mounted right up until the file lands — i.e. the share drops
        # while that final copy is in flight.
        return not os.path.exists(landed_path)

    monkeypatch.setattr(os.path, "ismount", fake_ismount)

    db, ws_id, result = _run_import(tmp_path, ImportParams(
        sources=[str(card)], destination=dest,
    ))

    assert result["copied"] == 0, result
    assert result["failed"] == 1, result
    assert result["safe_to_format"] is False, result
    # The shadow copy must not be cataloged as an archive photo.
    assert _photo_rows(db) == [], _photo_rows(db)


def test_local_import_refuses_when_share_detached_before_run_starts(
        tmp_path, monkeypatch):
    """A share detached BEFORE baseline capture must still be refused.

    Every check in this file so far assumes ``ismount == True`` at least
    for baseline capture: the transition-only guard fires only on
    True → False. A share that was already down when the run started
    baselines False, no transition can fire against a False baseline,
    and the persistent ``/mnt/<name>`` stub still passes the per-batch
    check — the same silent-shadow-write shape this PR opened, just
    with the timing shifted earlier so no within-run probe can see it.

    Cross-run history (mount roots ever observed live, persisted in
    ``db_meta``) is what closes that hole: a second run to the same
    destination baselines True by seeding, and the still-False current
    state fires the transition just as it would mid-run. A hand-made
    local ``/mnt/photos`` never enters the known-set (no run ever
    observed it as a live mount), so its baseline stays False and it
    stays a valid destination. See PR #1396 review
    (Codex P1 r3687401636).
    """
    import pipeline_job as _pj
    from import_job import ImportParams, run_import_job

    card = _make_card(tmp_path, [
        ("DSC_0001.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0002.jpg", datetime(2026, 7, 4, 9, 0, 0), "blue"),
    ])
    dest = str(tmp_path / "mnt_NAS")
    os.makedirs(dest, exist_ok=True)

    monkeypatch.setattr(
        _pj, "_archive_mount_root_candidates",
        lambda path: [dest] if str(path).startswith(dest) else [],
    )
    state = {"mounted": True}
    real_ismount = os.path.ismount
    monkeypatch.setattr(
        os.path, "ismount",
        lambda p: state["mounted"] if str(p) == dest else real_ismount(p),
    )

    # First run: share is live, files copy, persistent record captures
    # that ``dest`` was seen mounted.
    db_path = str(tmp_path / "test.db")
    db1 = Database(db_path)
    ws_id = db1._active_workspace_id
    first = run_import_job(
        _make_job("import-first"), FakeRunner(), db_path, ws_id,
        ImportParams(sources=[str(card)], destination=dest),
    )
    assert first["copied"] == 2, first
    assert first["failed"] == 0, first
    # Sanity: the DB now knows this destination as a mount root.
    assert dest in _pj._load_known_mount_roots(db1)

    # Second run: same destination, share detached BEFORE the run starts.
    # Without the persisted history the baseline would be False and the
    # guard would silently disarm.
    state["mounted"] = False
    card2 = _make_card(tmp_path, [
        ("DSC_0003.jpg", datetime(2026, 7, 5, 8, 0, 0), "green"),
        ("DSC_0004.jpg", datetime(2026, 7, 6, 7, 0, 0), "yellow"),
    ], card_name="card2")
    result = run_import_job(
        _make_job("import-second"), FakeRunner(), db_path, ws_id,
        ImportParams(sources=[str(card2)], destination=dest),
    )

    assert result["copied"] == 0, result
    assert result["failed"] == 2, result
    assert result["safe_to_format"] is False, result
    # Either the batch-level or per-file guard may notice first; the
    # substring common to both messages is "detached" / "no longer
    # mounted", so key the assertion on either.
    assert any(
        "no longer mounted" in u["reason"] or "detached" in u["reason"]
        for u in result["unsafe_files"]
    ), result["unsafe_files"]


def test_local_import_invalidates_duplicate_skips_when_mount_detaches(
        tmp_path, monkeypatch):
    """A duplicate-only batch must not vouch for a detached archive.

    An accepted duplicate never enters ``landed``, so reclassifying only
    landed entries leaves the skip standing. A duplicate-only batch can
    then satisfy ``copied + skipped_duplicate == discovered`` and report
    safe_to_format=True while the real archive holds none of the bytes —
    the card gets erased and the shadow disappears on remount.
    See PR #1396 review (Codex P1 r3687506040).
    """
    import shutil

    import import_job as _ij
    import pipeline_job as _pj
    from import_job import ImportParams, run_import_job
    from scanner import compute_file_hash

    dest = tmp_path / "mnt_NAS"
    dest_dir = dest / "2026" / "2026-07-03"
    dest_dir.mkdir(parents=True)
    dest_file = dest_dir / "IMG_0100.jpg"
    Image.new("RGB", (16, 16), "red").save(str(dest_file))
    ts = datetime(2026, 7, 3, 10, 0, 0).timestamp()
    os.utime(str(dest_file), (ts, ts))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (str(dest_dir), dest_dir.name),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_hash) VALUES (?, ?, '.jpg', ?, ?)",
        (fid, "IMG_0100.jpg", os.path.getsize(str(dest_file)),
         compute_file_hash(str(dest_file))),
    )
    db.conn.commit()

    card = tmp_path / "card"
    card.mkdir()
    shutil.copy2(str(dest_file), str(card / "IMG_0100.jpg"))

    monkeypatch.setattr(
        _pj, "_archive_mount_root_candidates",
        lambda path: [str(dest)] if str(path).startswith(str(dest)) else [],
    )
    state = {"mounted": True}
    real_ismount = os.path.ismount
    monkeypatch.setattr(
        os.path, "ismount",
        lambda p: (
            state["mounted"] if str(p) == str(dest) else real_ismount(p)
        ),
    )

    # The share drops while the duplicate gate is consulting the twin —
    # after the per-file probe has already passed. Both lookup helpers are
    # wrapped because which one runs depends on whether the checker
    # produced a hash token or a metadata-key token.
    def _detach_after(fn):
        def wrapper(*a, **kw):
            rows = fn(*a, **kw)
            state["mounted"] = False
            return rows
        return wrapper

    monkeypatch.setattr(
        _ij, "_key_twin_rows", _detach_after(_ij._key_twin_rows))
    monkeypatch.setattr(
        _ij, "_hash_twin_rows", _detach_after(_ij._hash_twin_rows))

    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id, ImportParams(
            sources=[str(card)], destination=str(dest),
        ),
    )

    assert result["copied"] == 0, result
    assert result["skipped_duplicate"] == 0, (
        "a duplicate accepted against a detached archive still counted as "
        f"safely already-present: {result}"
    )
    assert result["failed"] == 1, result
    assert result["safe_to_format"] is False, result


def test_local_import_mount_loss_is_sticky_across_batches(
        tmp_path, monkeypatch):
    """Same rationale as the remote-path sticky test: a batch's mount-
    detach rollback undoes ``dup_skips`` / ``landed`` but not the
    identities the same batch installed in the job-wide checker (and
    ``run_dest_folders`` / ``run_verified_hashes``) via
    ``_record_checker`` — ``DuplicateChecker`` exposes no removal API. If
    the share remounts before a later batch, a byte-identical card file
    would hit the intra-run fast path and be counted as a duplicate of a
    rolled-back landing whose archive bytes never made it. See PR #1400
    review (Codex P2 r3688614624).
    """
    import shutil

    import pipeline_job as _pj
    from import_job import ImportParams, run_import_job

    dest = tmp_path / "mnt_NAS"
    dest.mkdir()
    day1_dir = dest / "2026" / "2026-07-03"
    day1_dir.mkdir(parents=True)
    # A twin file already at the batch 1 destination — the adopt branch
    # matches it and calls ``_record_checker`` with dest_folder + hash,
    # populating the job-wide checker's ``_seen_hashes`` and the intra-
    # run cache. Those are what leak past the rollback.
    twin_path = day1_dir / "IMG_0100.jpg"
    Image.new("RGB", (16, 16), "red").save(str(twin_path))
    ts1 = datetime(2026, 7, 3, 10, 0, 0).timestamp()
    os.utime(str(twin_path), (ts1, ts1))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    card = tmp_path / "card"
    card.mkdir()
    # Batch 1 (2026-07-03): matches the adopted twin.
    file_a = card / "IMG_0100.jpg"
    shutil.copy2(str(twin_path), str(file_a))
    os.utime(str(file_a), (ts1, ts1))
    # Batch 2 (2026-07-04): byte-identical to A. Without the sticky
    # fix, this hits the intra-run fast path against A's stale entry.
    file_b = card / "IMG_0200.jpg"
    shutil.copy2(str(twin_path), str(file_b))
    ts2 = datetime(2026, 7, 4, 10, 0, 0).timestamp()
    os.utime(str(file_b), (ts2, ts2))

    monkeypatch.setattr(
        _pj, "_archive_mount_root_candidates",
        lambda path: [str(dest)] if str(path).startswith(str(dest)) else [],
    )
    real_ismount = os.path.ismount
    monkeypatch.setattr(
        os.path, "ismount",
        lambda p: True if str(p) == str(dest) else real_ismount(p),
    )

    # Sequence the mount-probe returns so batch 1 detaches AFTER its
    # per-file loop and batch 2 would see a REMOUNTED share at its
    # batch-boundary probe. Local path call sites (per batch):
    #   batch-boundary, per-file (1x), post-loop.
    seq = iter([None, None, str(dest), None, None, None])

    def fake_unmounted(baseline):
        try:
            return next(seq)
        except StopIteration:
            return None

    monkeypatch.setattr(_pj, "_unmounted_since_baseline", fake_unmounted)

    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id, ImportParams(
            sources=[str(card)], destination=str(dest),
        ),
    )

    assert result["copied"] == 0, result
    assert result["skipped_duplicate"] == 0, (
        "file B in batch 2 was counted as a duplicate of A's rolled-back "
        f"adopt via the stale intra-run cache: {result}"
    )
    assert result["failed"] == 2, result
    assert result["safe_to_format"] is False, result


def test_remote_import_invalidates_duplicate_skips_when_mount_detaches(
        tmp_path, monkeypatch):
    """The remote path needs the local path's duplicate-skip rollback.

    A twin-verified skip increments ``skipped_duplicate``, never enters
    ``landed``, and can be satisfied by a shadow file
    left on the persistent mount stub by an earlier failed import — so
    rsync is skipped entirely and ``copied + skipped_duplicate ==
    discovered`` holds against bytes that are not on the NAS. With
    ``verify_by_hash`` on, the ``<remote>`` honesty gate stops masking it
    and safe_to_format can go true over a card that is the only real
    copy. See PR #1396 review (Codex P1 r3688498501 / r3688501706).
    """
    import shutil

    import import_job as _ij
    import pipeline_job as _pj
    from import_job import ImportParams, run_import_job
    from scanner import compute_file_hash

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)
    mount_base = ra["mount_base"]

    # A shadow twin sitting on the mount stub, already cataloged — what an
    # earlier failed import would have left behind.
    twin_dir = os.path.join(mount_base, "2026", "2026-07-03")
    os.makedirs(twin_dir, exist_ok=True)
    twin_file = os.path.join(twin_dir, "IMG_0100.jpg")
    Image.new("RGB", (16, 16), "red").save(twin_file)
    ts = datetime(2026, 7, 3, 10, 0, 0).timestamp()
    os.utime(twin_file, (ts, ts))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (twin_dir, "2026-07-03"),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_hash) VALUES (?, ?, '.jpg', ?, ?)",
        (fid, "IMG_0100.jpg", os.path.getsize(twin_file),
         compute_file_hash(twin_file)),
    )
    db.conn.commit()

    card = tmp_path / "card"
    card.mkdir()
    shutil.copy2(twin_file, str(card / "IMG_0100.jpg"))

    monkeypatch.setattr(
        _pj, "_archive_mount_root_candidates",
        lambda path: [mount_base] if str(path).startswith(mount_base) else [],
    )
    state = {"mounted": True}
    real_ismount = os.path.ismount
    monkeypatch.setattr(
        os.path, "ismount",
        lambda p: (
            state["mounted"] if str(p) == mount_base else real_ismount(p)
        ),
    )

    # Detach while the duplicate gate consults the twin — i.e. after the
    # batch-boundary probe has already passed.
    def _detach_after(fn):
        def wrapper(*a, **kw):
            rows = fn(*a, **kw)
            state["mounted"] = False
            return rows
        return wrapper

    monkeypatch.setattr(
        _ij, "_key_twin_rows", _detach_after(_ij._key_twin_rows))
    monkeypatch.setattr(
        _ij, "_hash_twin_rows", _detach_after(_ij._hash_twin_rows))

    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id, ImportParams(
            sources=[str(card)], destination=mount_base,
            remote_target=ra, verify_by_hash=True,
        ),
    )

    assert result["copied"] == 0, result
    assert result["skipped_duplicate"] == 0, (
        "a duplicate accepted against a detached archive still counted as "
        f"safely already-present: {result}"
    )
    assert result["failed"] == 1, result
    assert result["safe_to_format"] is False, result


def test_remote_import_mount_loss_is_sticky_across_batches(
        tmp_path, monkeypatch):
    """Mount loss must be sticky for the rest of the run.

    A batch's mount-detach rollback undoes ``dup_skips`` / ``to_transfer``
    / the ``landed`` ledger (fresh and adopted entries) but not the identities the same batch already
    installed in the job-wide checker (and in ``run_dest_folders`` /
    ``run_verified_hashes``) via ``_record_checker`` — and
    ``DuplicateChecker`` exposes no removal API, so those entries cannot
    be surgically undone. If the share remounts before a later batch, a
    byte-identical card file in that later batch would hit the intra-run
    fast path at line 1237 and be counted as a duplicate of an adopted
    or queued file whose archive claim was rolled back, with no backing
    NAS copy. Refusing every remaining batch keeps the stale cache from
    being consulted. See PR #1400 review (Codex P2 r3688614624).
    """
    import shutil

    import pipeline_job as _pj
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)
    mount_base = ra["mount_base"]

    # Adopted twin already at the batch 1 destination (crash-recovery
    # shape). The adopt branch matches its bytes and calls
    # ``_record_checker`` with dest_folder + hash — populating both the
    # job-wide checker's ``_seen_hashes`` and the intra-run
    # ``run_dest_folders`` / ``run_verified_hashes``. Those are the
    # entries the fix has to keep unreachable after the rollback.
    day1_dir = os.path.join(mount_base, "2026", "2026-07-03")
    os.makedirs(day1_dir)
    twin_path = os.path.join(day1_dir, "IMG_0100.jpg")
    Image.new("RGB", (16, 16), "red").save(twin_path)
    ts1 = datetime(2026, 7, 3, 10, 0, 0).timestamp()
    os.utime(twin_path, (ts1, ts1))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    card = tmp_path / "card"
    card.mkdir()
    # Batch 1 (2026-07-03): matches the adopted twin.
    file_a = card / "IMG_0100.jpg"
    shutil.copy2(twin_path, str(file_a))
    os.utime(str(file_a), (ts1, ts1))
    # Batch 2 (2026-07-04): BYTE-IDENTICAL to A. Without the sticky fix
    # this hits the intra-run fast path against A's stale entry in
    # ``run_dest_folders`` and is counted as ``skipped_duplicate`` even
    # though A's landing was rolled back and the NAS holds nothing for
    # it. With the fix, batch 2 is refused at the top of the loop.
    file_b = card / "IMG_0200.jpg"
    shutil.copy2(twin_path, str(file_b))
    ts2 = datetime(2026, 7, 4, 10, 0, 0).timestamp()
    os.utime(str(file_b), (ts2, ts2))

    monkeypatch.setattr(
        _pj, "_archive_mount_root_candidates",
        lambda path: [mount_base] if str(path).startswith(mount_base) else [],
    )

    # Baseline captured at job start uses ismount — leave the mount root
    # reporting live so the baseline is True. The per-batch detach/remount
    # timing is driven through ``_unmounted_since_baseline`` below.
    real_ismount = os.path.ismount
    monkeypatch.setattr(
        os.path, "ismount",
        lambda p: True if str(p) == mount_base else real_ismount(p),
    )

    # Sequence the mount-probe returns so batch 1 detaches AFTER the
    # per-file loop (post the adopt-branch _record_checker) and batch 2
    # would see a REMOUNTED share at its batch-boundary probe. Without
    # the sticky fix, batch 2 then reaches the dup gate and consults the
    # stale intra-run cache; with it, ``mount_ever_lost`` already fired
    # in batch 1 and batch 2's per-file fail loop runs before any probe.
    #
    # Call sites (remote path):
    #   batch 1: batch-boundary, per-file (1x), post-loop
    #   batch 2 without the fix: batch-boundary, per-file (1x), post-loop
    seq = iter([None, None, mount_base, None, None, None])

    def fake_unmounted(baseline):
        try:
            return next(seq)
        except StopIteration:
            return None

    monkeypatch.setattr(_pj, "_unmounted_since_baseline", fake_unmounted)

    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id, ImportParams(
            sources=[str(card)], destination=mount_base,
            remote_target=ra, verify_by_hash=True,
        ),
    )

    # File A was adopted in batch 1 then rolled back on detach; file B
    # in batch 2 must NOT be counted as skipped_duplicate against A's
    # stale intra-run cache entry. Both end up failed.
    assert result["copied"] == 0, result
    assert result["skipped_duplicate"] == 0, (
        "file B in batch 2 was counted as a duplicate of A's rolled-back "
        f"adopt via the stale intra-run cache: {result}"
    )
    assert result["failed"] == 2, result
    assert result["safe_to_format"] is False, result

    # No rsync happened either — both batches short-circuited before the
    # transfer step.
    assert calls["rsync"] == [], calls["rsync"]


def test_remote_import_mount_detach_after_adoption_rolls_back_once(
        tmp_path, monkeypatch):
    """An adoption's mount-detach rollback must decrement exactly once.

    Pre-fold, a remote adoption was booked in ``dup_skips`` (plus the
    since-deleted ``adopted_paths`` dict); the PR 5a fold moved it into
    ``landed`` with origin ``"skipped_duplicate"`` and added a
    mount-lost ``landed`` rollback. Had the fold kept the adoption
    branch's ``dup_skips.append``, the same file would be rolled back
    by BOTH blocks and ``skipped_duplicate`` would read -1 on a detach
    after an adoption. This test pinned the single decrement across
    that refactor: exactly 0, never negative.

    Geometry: one 2-file batch (same capture date). File A has a
    byte-identical, uncataloged twin pre-seeded at its mount destination
    (crash-recovery shape -> the collision walk adopts it); file B is
    fresh and gets queued for rsync. The probe spy stays healthy through
    the batch-boundary probe and both per-file probes, then reports a
    detach from the POST-LOOP probe onward — after both files were
    decided, before transfer/catalog.
    """
    import shutil

    import pipeline_job as _pj
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)
    mount_base = ra["mount_base"]

    card = _make_card(tmp_path, [
        ("DSC_0100.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0101.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])

    # Byte-identical twin already at file A's mount destination,
    # uncataloged (no DB rows) — the duplicate gate finds nothing, the
    # collision walk hashes the on-disk twin and adopts it.
    day_dir = os.path.join(mount_base, "2026", "2026-07-03")
    os.makedirs(day_dir)
    shutil.copy2(str(card / "DSC_0100.jpg"),
                 os.path.join(day_dir, "DSC_0100.jpg"))

    probe_calls = {"count": 0}

    def spy_unmounted(baseline):
        probe_calls["count"] += 1
        # Healthy for batch-boundary (1) and the per-file probes for A
        # (2) and B (3); detached from the post-loop probe (4) onward.
        if probe_calls["count"] >= 4:
            return "/Volumes/NAS"
        return None

    monkeypatch.setattr(_pj, "_unmounted_since_baseline", spy_unmounted)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    result = run_import_job(
        _make_job(), FakeRunner(), db_path, ws_id, ImportParams(
            sources=[str(card)], destination=mount_base,
            remote_target=ra, verify_by_hash=True,
        ),
    )

    # The per-file loop completed (both files decided) and the detach
    # was first seen at the post-loop probe — not earlier, not at a
    # next-batch boundary (there is no next batch).
    assert probe_calls["count"] == 4, (
        f"expected 4 mount probes (batch-boundary + 2x per-file + "
        f"post-loop) but got {probe_calls['count']}"
    )
    # THE pin: adopted file A's skipped_duplicate is rolled back exactly
    # once. A double rollback (dup_skips AND landed, post-fold) yields -1.
    assert result["skipped_duplicate"] == 0, result
    assert result["copied"] == 0, result
    assert result["failed"] == 2, result
    assert result["safe_to_format"] is False, result
    # Detach was observed before the transfer step ever ran.
    assert calls["rsync"] == [], calls["rsync"]
    # Reason wording proves WHICH block failed each file: the post-loop
    # rollback blocks, not the per-file probe (whose wording is
    # "detached while this batch was being prepared").
    # Flip 3 (PR 5a fold): adopted A now rolls back via the mount-lost
    # ``landed`` block — MOUNT dest path subject + the local path's
    # "local shadow" wording (pre-fold it went through ``dup_skips``
    # with the card path and "cannot be confirmed"). Fresh B still rolls
    # back via ``to_transfer`` with the card path, unchanged.
    reason_a = _unsafe_reason(
        result, os.path.join(day_dir, "DSC_0100.jpg"))
    assert "local shadow" in reason_a, reason_a
    reason_b = _unsafe_reason(result, str(card / "DSC_0101.jpg"))
    assert "detached before this file was transferred" in reason_b, reason_b


def test_local_import_mount_detach_after_adoption_rolls_back_once(
        tmp_path, monkeypatch):
    """Local mirror of the remote adoption single-rollback pin.

    Local adoptions are booked straight into ``landed`` with origin
    ``"skipped_duplicate"`` (never ``dup_skips``) and rolled back on
    mount detach via ``_reclassify_landed_failed`` — the exact shape the
    PR 5a fold gives the remote path. Pinning the local counter at
    exactly 0 keeps the two paths' single-decrement behavior verifiably
    identical while the remote bookkeeping moves.

    Geometry mirrors the remote test: one 2-file batch, file A's
    byte-identical uncataloged twin pre-seeded at the archive
    destination (adopted), file B fresh (copied), detach first visible
    at the post-loop probe.
    """
    import shutil

    import pipeline_job as _pj
    from import_job import ImportParams

    card = _make_card(tmp_path, [
        ("DSC_0100.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0101.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])
    archive = tmp_path / "archive"
    day_dir = archive / "2026" / "2026-07-03"
    day_dir.mkdir(parents=True)
    shutil.copy2(str(card / "DSC_0100.jpg"), str(day_dir / "DSC_0100.jpg"))

    probe_calls = {"count": 0}

    def spy_unmounted(baseline):
        probe_calls["count"] += 1
        # Healthy for batch-boundary (1) and the per-file probes for A
        # (2) and B (3); detached from the post-loop probe (4) onward.
        if probe_calls["count"] >= 4:
            return "/Volumes/NAS"
        return None

    monkeypatch.setattr(_pj, "_unmounted_since_baseline", spy_unmounted)

    db, ws_id, result = _run_import(
        tmp_path,
        ImportParams(sources=[str(card)], destination=str(archive)),
    )

    # Loop completed; detach first seen at the post-loop probe (single
    # batch — a next-batch boundary probe does not exist here).
    assert probe_calls["count"] == 4, (
        f"expected 4 mount probes (batch-boundary + 2x per-file + "
        f"post-loop) but got {probe_calls['count']}"
    )
    # THE pin: adoption rolled back exactly once (0, never -1).
    assert result["skipped_duplicate"] == 0, result
    assert result["copied"] == 0, result
    assert result["failed"] == 2, result
    assert result["safe_to_format"] is False, result
    # Both files were rolled back by the ``landed`` block (dest-side
    # paths + "local shadow" wording), proving the detach was handled at
    # the post-loop probe, not by the per-file probe (card-side paths,
    # "detached while this batch was copying" wording).
    assert _unsafe_paths(result) == {
        str(day_dir / "DSC_0100.jpg"), str(day_dir / "DSC_0101.jpg"),
    }, result["unsafe_files"]
    for u in result["unsafe_files"]:
        assert "local shadow" in u["reason"], u


# --------------------------------------------------------------------------
# Destination-side hashing must not hold cancellation hostage. On a stale
# SMB mount a single blocking read can pin the worker for tens of minutes
# while Stop goes unobserved (cancellation is only polled between files).
# ``_hash_dest_file`` bounds every mount-side hash read with the job's Stop
# signal and a stall watchdog. The FIFOs below stand in for a dead network
# mount — opening one for reading blocks until a writer appears, exactly
# like a read against a wedged SMB session.
# --------------------------------------------------------------------------

def _release_fifo(fifo):
    """Unblock any reader stuck on the FIFO (open the writer side, close →
    the reader sees EOF). Best-effort cleanup so an abandoned hash worker
    doesn't outlive its test."""
    import contextlib
    with contextlib.suppress(OSError):
        fd = os.open(str(fifo), os.O_WRONLY | os.O_NONBLOCK)
        os.close(fd)


class CancelOnImportingRunner(FakeRunner):
    """Flips to cancelled the moment the per-file "importing" progress
    emit lands — i.e. after the loop-top cancellation check has already
    passed for that file, right before the duplicate gate's destination
    reads."""

    def push_event(self, job_id, event_type, data):
        super().push_event(job_id, event_type, data)
        if (
            event_type == "progress"
            and ": importing" in (data.get("phase") or "")
        ):
            self.cancelled_ids.add(job_id)


def test_hash_dest_file_matches_compute_file_hash(tmp_path):
    from import_dedup import compute_file_hash
    from import_job import _hash_dest_file

    f = tmp_path / "a.jpg"
    f.write_bytes(b"some destination bytes" * 1000)
    assert _hash_dest_file(str(f), lambda: False) == \
        compute_file_hash(str(f))


def test_dest_read_cancelled_is_an_oserror():
    """Call sites without explicit cancel handling catch OSError and treat
    the file as unreadable — safe-by-default for any unwired site."""
    from import_job import DestReadCancelled

    assert issubclass(DestReadCancelled, OSError)


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="os.mkfifo is POSIX-only; FIFO stands in for a dead SMB mount.",
)
def test_hash_dest_file_cancel_interrupts_blocked_read(tmp_path):
    import threading
    import time

    from import_job import DestReadCancelled, _hash_dest_file

    fifo = tmp_path / "stuck.NEF"
    os.mkfifo(str(fifo))
    cancel = threading.Event()
    timer = threading.Timer(0.3, cancel.set)
    timer.start()
    start = time.monotonic()
    try:
        with pytest.raises(DestReadCancelled):
            _hash_dest_file(str(fifo), cancel.is_set)
        assert time.monotonic() - start < 5.0, (
            "cancellation took too long to interrupt the blocked read"
        )
    finally:
        timer.cancel()
        _release_fifo(fifo)


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="os.mkfifo is POSIX-only; FIFO stands in for a dead SMB mount.",
)
def test_hash_dest_file_cancel_pending_never_opens(tmp_path):
    """Stop already requested → raise before touching the file at all: on
    a dead mount the ``open()`` itself blocks, so returning at all proves
    the open was skipped (a FIFO reader with no writer never returns)."""
    from import_job import DestReadCancelled, _hash_dest_file

    fifo = tmp_path / "stuck.NEF"
    os.mkfifo(str(fifo))
    with pytest.raises(DestReadCancelled):
        _hash_dest_file(str(fifo), lambda: True)


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="os.mkfifo is POSIX-only; FIFO stands in for a dead SMB mount.",
)
def test_hash_dest_file_stall_raises_plain_oserror(tmp_path):
    """No Stop in flight — a read that produces no data for stall_timeout
    is an unreadable file, the same OSError shape every call site already
    handles (NOT a cancellation)."""
    from import_job import DestReadCancelled, _hash_dest_file

    fifo = tmp_path / "stuck.NEF"
    os.mkfifo(str(fifo))
    try:
        with pytest.raises(OSError) as exc_info:
            _hash_dest_file(str(fifo), lambda: False, stall_timeout=0.5)
        assert not isinstance(exc_info.value, DestReadCancelled)
    finally:
        _release_fifo(fifo)


def _catalog_twin_row(db, dest_dir, filename, size, file_hash):
    fid = db.conn.execute(
        "INSERT INTO folders (path, name, status) VALUES (?, ?, 'ok')",
        (str(dest_dir), os.path.basename(str(dest_dir))),
    ).lastrowid
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size,"
        " file_hash) VALUES (?, ?, ?, ?, ?)",
        (fid, filename, os.path.splitext(filename)[1], size, file_hash),
    )
    db.conn.commit()


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="os.mkfifo is POSIX-only; FIFO stands in for a dead SMB mount.",
)
def test_local_import_cancel_interrupts_stuck_twin_hash(tmp_path):
    """Stop must not wait out a duplicate-gate hash read blocked on a dead
    mount. The cataloged twin here is a FIFO: hashing it blocks forever
    until released, like a stale SMB session. The job must notice the
    cancel and exit — with the interrupted file neither copied nor
    counted as failed (it stays on the card for the next run)."""
    import threading

    from import_dedup import compute_file_hash
    from import_job import ImportParams, run_import_job

    card = tmp_path / "card"
    card.mkdir()
    card_file = card / "IMG_0300.jpg"
    Image.new("RGB", (16, 16), "red").save(str(card_file))

    archive = tmp_path / "archive"
    dest_dir = archive / "old"
    dest_dir.mkdir(parents=True)
    twin = dest_dir / "IMG_0300.jpg"
    os.mkfifo(str(twin))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    _catalog_twin_row(
        db, dest_dir, "IMG_0300.jpg",
        os.path.getsize(str(card_file)), compute_file_hash(str(card_file)),
    )

    runner = CancelOnImportingRunner()
    job = _make_job()
    result_box = {}

    def _run():
        result_box["result"] = run_import_job(
            job, runner, db_path, ws_id,
            ImportParams(sources=[str(card)], destination=str(archive)),
        )

    worker = threading.Thread(target=_run, daemon=True)
    worker.start()
    try:
        worker.join(timeout=15.0)
        assert not worker.is_alive(), (
            "Stop did not interrupt the destination hash read blocked on "
            "the dead-mount twin"
        )
    finally:
        _release_fifo(twin)
        worker.join(timeout=5.0)

    result = result_box["result"]
    assert result["cancelled"] is True
    assert result["failed"] == 0, result
    assert result["copied"] == 0, result


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="os.mkfifo is POSIX-only; FIFO stands in for a dead SMB mount.",
)
def test_remote_import_cancel_interrupts_stuck_twin_hash(
        tmp_path, monkeypatch):
    """Remote-path mirror of the local stuck-twin-hash cancel test — the
    remote duplicate gate hashes cataloged twins through the SMB mount
    too, and must observe Stop the same way. Nothing may reach rsync.

    Geometry matches the local mirror: the twin lives OFF the template
    path so only the duplicate-gate twin re-hash can reach it — a
    template-shaped twin would let the collision/adopt walk satisfy this
    test with the gate broken."""
    import threading

    from import_dedup import compute_file_hash
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    card = tmp_path / "card"
    card.mkdir()
    card_file = card / "IMG_0300.jpg"
    Image.new("RGB", (16, 16), "red").save(str(card_file))

    dest_dir = Path(ra["mount_base"]) / "old"
    dest_dir.mkdir(parents=True)
    twin = dest_dir / "IMG_0300.jpg"
    os.mkfifo(str(twin))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    _catalog_twin_row(
        db, dest_dir, "IMG_0300.jpg",
        os.path.getsize(str(card_file)), compute_file_hash(str(card_file)),
    )

    runner = CancelOnImportingRunner()
    job = _make_job()
    result_box = {}

    def _run():
        result_box["result"] = run_import_job(
            job, runner, db_path, ws_id,
            ImportParams(
                sources=[str(card)], destination=ra["mount_base"],
                remote_target=ra, verify_by_hash=True,
            ),
        )

    worker = threading.Thread(target=_run, daemon=True)
    worker.start()
    try:
        worker.join(timeout=15.0)
        assert not worker.is_alive(), (
            "Stop did not interrupt the destination hash read blocked on "
            "the dead-mount twin"
        )
    finally:
        _release_fifo(twin)
        worker.join(timeout=5.0)

    result = result_box["result"]
    assert result["cancelled"] is True
    assert result["failed"] == 0, result
    assert result["copied"] == 0, result
    assert calls["rsync"] == [], calls["rsync"]


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="os.mkfifo is POSIX-only; FIFO stands in for a dead SMB mount.",
)
def test_remote_import_cancel_interrupts_stuck_collision_hash(
        tmp_path, monkeypatch):
    """Remote-path mirror of the twin-hash cancel, but for the OTHER
    destination-side hash on this path: the collision/adopt loop.

    Scenario: a file already sits at the collision candidate path on the
    mount (crash-recovery shape — landed by an earlier run before catalog)
    but is NOT cataloged, so the twin-hash branch above finds nothing and
    execution reaches the ``os.path.exists(cand_mount)`` -> ``_hash_dest_file``
    branch. That mount is wedged (FIFO). Before the fix, ``DestReadCancelled``
    was swallowed as ``on_disk = None`` and the ``while True`` loop advanced
    to the next suffix, calling ``os.path.exists`` / ``_hash_dest_file`` on
    the same dead mount again. Stop must now leave both the candidate loop
    and the source-file loop the moment cancellation is observed, so nothing
    reaches rsync and no further mount work happens."""
    import threading
    from datetime import datetime

    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    card = tmp_path / "card"
    card.mkdir()
    card_file = card / "IMG_0300.jpg"
    Image.new("RGB", (16, 16), "red").save(str(card_file))
    # Pin the card file's mtime so the folder-template resolves to a
    # deterministic dest_folder. Fixed local-time date so the collision
    # FIFO below sits at the exact ``cand_mount`` path the import job
    # computes for this file.
    fixed_dt = datetime(2026, 1, 15, 12, 0, 0)
    ts = fixed_dt.timestamp()
    os.utime(str(card_file), (ts, ts))

    dest_dir = Path(ra["mount_base"]) / fixed_dt.strftime("%Y") / \
        fixed_dt.strftime("%Y-%m-%d")
    dest_dir.mkdir(parents=True)
    # Two NOT-cataloged mount files at consecutive suffixes — the primary
    # candidate blocks the collision hash; the second exists so that
    # advancing past the primary would re-enter ``os.path.exists`` /
    # ``_hash_dest_file`` on the same wedged mount. This is the "next
    # iteration" behavior Codex flagged — leaving the candidate loop on
    # cancel means the second suffix is never touched.
    collision = dest_dir / "IMG_0300.jpg"
    os.mkfifo(str(collision))
    next_collision = dest_dir / "IMG_0300_1.jpg"
    os.mkfifo(str(next_collision))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id

    # Spy on the collision-candidate probe. ``os.path.exists`` gets called
    # once per candidate iteration, so without the inner break Codex asked
    # for, cancelling on the primary FIFO's hash would fall through to the
    # counter+=1/continue path and probe (then hash) the second FIFO on
    # the same dead mount before the outer ``if cancelled: break`` check
    # fires.
    import import_job as _ij
    real_hash_dest_file = _ij._hash_dest_file
    hashed_paths = []

    def spy_hash_dest_file(path, cancel_check, **kw):
        hashed_paths.append(path)
        return real_hash_dest_file(path, cancel_check, **kw)

    monkeypatch.setattr(_ij, "_hash_dest_file", spy_hash_dest_file)

    runner = CancelOnImportingRunner()
    job = _make_job()
    result_box = {}

    def _run():
        result_box["result"] = run_import_job(
            job, runner, db_path, ws_id,
            ImportParams(
                sources=[str(card)], destination=ra["mount_base"],
                remote_target=ra, verify_by_hash=True,
            ),
        )

    worker = threading.Thread(target=_run, daemon=True)
    worker.start()
    try:
        worker.join(timeout=15.0)
        assert not worker.is_alive(), (
            "Stop did not interrupt the collision hash read blocked on "
            "the dead-mount candidate"
        )
    finally:
        _release_fifo(collision)
        _release_fifo(next_collision)
        worker.join(timeout=5.0)

    result = result_box["result"]
    assert result["cancelled"] is True
    assert result["failed"] == 0, result
    assert result["copied"] == 0, result
    assert calls["rsync"] == [], calls["rsync"]
    # After the primary candidate's hash raised DestReadCancelled, the
    # inner break must exit the candidate loop — no second dead-mount
    # candidate should have been hashed. Without the fix, the code would
    # advance to ``IMG_0300_1.jpg`` and hash it too before observing
    # cancellation at the outer batch boundary.
    dest_hashes = [p for p in hashed_paths if str(dest_dir) in str(p)]
    assert len(dest_hashes) <= 1, (
        f"collision loop hashed multiple dead-mount candidates on cancel: "
        f"{dest_hashes}"
    )


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="os.mkfifo is POSIX-only; FIFO stands in for a dead SMB mount.",
)
def test_remote_import_cancel_skips_post_loop_mount_probe(
        tmp_path, monkeypatch):
    """After Stop interrupts a destination-side hash on the remote path,
    the post-loop ``_unmounted_since_baseline`` probe MUST NOT fire. On a
    real wedged mount that probe would block for the mount's own timeout,
    pinning the job in "cancelling" — exactly the behavior this PR set out
    to eliminate.

    Spy on the probe: it may run once (the per-file check that ran before
    the twin-hash cancel), but the second call — post-loop — must be
    suppressed by the ``not cancelled`` gate.

    Geometry matches the local mirror: the twin lives OFF the template
    path so only the duplicate-gate twin re-hash can reach it — a
    template-shaped twin would let the collision/adopt walk satisfy this
    test with the gate broken."""
    import threading

    import pipeline_job as _pj
    from import_dedup import compute_file_hash
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    card = tmp_path / "card"
    card.mkdir()
    card_file = card / "IMG_0300.jpg"
    Image.new("RGB", (16, 16), "red").save(str(card_file))

    dest_dir = Path(ra["mount_base"]) / "old"
    dest_dir.mkdir(parents=True)
    twin = dest_dir / "IMG_0300.jpg"
    os.mkfifo(str(twin))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    _catalog_twin_row(
        db, dest_dir, "IMG_0300.jpg",
        os.path.getsize(str(card_file)), compute_file_hash(str(card_file)),
    )

    probe_calls = {"count": 0}

    def spy_unmounted(baseline):
        probe_calls["count"] += 1
        return None

    monkeypatch.setattr(_pj, "_unmounted_since_baseline", spy_unmounted)

    runner = CancelOnImportingRunner()
    job = _make_job()
    result_box = {}

    def _run():
        result_box["result"] = run_import_job(
            job, runner, db_path, ws_id,
            ImportParams(
                sources=[str(card)], destination=ra["mount_base"],
                remote_target=ra, verify_by_hash=True,
            ),
        )

    worker = threading.Thread(target=_run, daemon=True)
    worker.start()
    try:
        worker.join(timeout=15.0)
        assert not worker.is_alive()
    finally:
        _release_fifo(twin)
        worker.join(timeout=5.0)

    result = result_box["result"]
    assert result["cancelled"] is True
    # Exactly two calls are expected on this single-file, single-batch
    # cancel path: pre-batch (once per batch, before the for-source_file
    # loop) + per-file (once at the top of the loop, before the twin-hash
    # break). ``== 2`` catches BOTH regressions: a third call would mean
    # the post-loop probe fired (unconditional before this PR's fix), and
    # a count below 2 would mean the per-file or pre-batch probe was
    # accidentally removed.
    assert probe_calls["count"] == 2, (
        f"expected exactly 2 mount probes (pre-batch + per-file) but got "
        f"{probe_calls['count']}"
    )


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="os.mkfifo is POSIX-only; FIFO stands in for a dead SMB mount.",
)
def test_local_import_cancel_skips_post_loop_mount_probe(
        tmp_path, monkeypatch):
    """Local-path mirror of the remote skip-post-loop-mount-probe test.
    The local path has the same unconditional post-loop probe and the same
    dead-mount hang risk on a wedged twin hash."""
    import threading

    import pipeline_job as _pj
    from import_dedup import compute_file_hash
    from import_job import ImportParams, run_import_job

    card = tmp_path / "card"
    card.mkdir()
    card_file = card / "IMG_0300.jpg"
    Image.new("RGB", (16, 16), "red").save(str(card_file))

    archive = tmp_path / "archive"
    dest_dir = archive / "old"
    dest_dir.mkdir(parents=True)
    twin = dest_dir / "IMG_0300.jpg"
    os.mkfifo(str(twin))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    _catalog_twin_row(
        db, dest_dir, "IMG_0300.jpg",
        os.path.getsize(str(card_file)), compute_file_hash(str(card_file)),
    )

    probe_calls = {"count": 0}

    def spy_unmounted(baseline):
        probe_calls["count"] += 1
        return None

    monkeypatch.setattr(_pj, "_unmounted_since_baseline", spy_unmounted)

    runner = CancelOnImportingRunner()
    job = _make_job()
    result_box = {}

    def _run():
        result_box["result"] = run_import_job(
            job, runner, db_path, ws_id,
            ImportParams(sources=[str(card)], destination=str(archive)),
        )

    worker = threading.Thread(target=_run, daemon=True)
    worker.start()
    try:
        worker.join(timeout=15.0)
        assert not worker.is_alive()
    finally:
        _release_fifo(twin)
        worker.join(timeout=5.0)

    result = result_box["result"]
    assert result["cancelled"] is True
    # As on the remote path: exactly two calls are expected (pre-batch +
    # per-file, before the twin-hash cancel). ``== 2`` catches BOTH a
    # third call (post-loop probe not suppressed) and a missing probe.
    assert probe_calls["count"] == 2, (
        f"expected exactly 2 mount probes (pre-batch + per-file) but got "
        f"{probe_calls['count']}"
    )


# --------------------------------------------------------------------------
# On a PLAIN Stop (user-hit cancel observed by ``runner.is_cancelled`` at the
# top of the source-file loop) the mount is not necessarily wedged; an
# earlier file in the same batch may have been copied or adopted while the
# archive was still healthy, and the share could then have detached in the
# gap between that operation and the Stop. The post-loop mount probe is the
# last chance to catch that detach — without it ``landed`` / ``dup_skips``
# stay accepted and the catalog block trusts a local shadow. See PR #1423
# review (Codex P2 r3716581282 / r3716581283).
# --------------------------------------------------------------------------


def test_local_import_plain_stop_still_runs_post_loop_mount_probe(
        tmp_path, monkeypatch):
    """A plain Stop (not a wedged-mount ``DestReadCancelled``) MUST NOT
    suppress the post-loop mount probe. If the archive detaches after an
    earlier file in the batch landed, only this probe can roll it back
    before the catalog block scans a local shadow."""
    import pipeline_job as _pj
    from import_job import ImportParams

    # Two files, one batch (same date -> same destination folder). File
    # one gets to land; file two's loop-top cancel check breaks.
    card = _make_card(tmp_path, [
        ("DSC_0100.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0101.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])
    archive = tmp_path / "archive"

    probe_calls = {"count": 0}

    def spy_unmounted(baseline):
        probe_calls["count"] += 1
        # Detach only appears on the post-loop probe: pre-batch (call 1)
        # and per-file file-one (call 2) must return healthy so file one
        # successfully lands. On the third call — post-loop — return a
        # truthy mount root so the ``if mount_lost and landed`` rollback
        # is exercised.
        if probe_calls["count"] >= 3:
            return "/Volumes/NAS"
        return None

    monkeypatch.setattr(_pj, "_unmounted_since_baseline", spy_unmounted)

    runner = CancelOnImportingRunner()
    db, ws_id, result = _run_import(
        tmp_path,
        ImportParams(sources=[str(card)], destination=str(archive)),
        runner=runner,
    )

    assert result["cancelled"] is True
    # Post-loop probe MUST fire on plain Stop: pre-batch (1) + per-file
    # for the one file that entered the loop body (2) + post-loop (3).
    # If the gate ever regresses to ``not cancelled``, the third call
    # disappears and the rollback below silently stops running.
    assert probe_calls["count"] == 3, (
        f"expected 3 mount probes (pre-batch + per-file + post-loop) "
        f"but got {probe_calls['count']}"
    )
    # File one was copied but must be rolled back to failed once the
    # post-loop probe sees the detach — the archive holds a local shadow
    # of that file, not the real bytes, so booking it as copied would
    # let ``safe_to_format`` go green over a card that still holds the
    # only copy.
    assert result["copied"] == 0, result
    assert result["failed"] == 1, result
    assert result["safe_to_format"] is False, result
    assert any(
        "detached" in u["reason"] and "local shadow" in u["reason"]
        for u in result["unsafe_files"]
    ), result["unsafe_files"]


def test_remote_import_plain_stop_still_runs_post_loop_mount_probe(
        tmp_path, monkeypatch):
    """Remote-path mirror of the local plain-Stop probe-firing test.
    An earlier file in the same batch was queued for rsync as a fresh
    copy; the share then detaches; a plain Stop breaks the loop. The
    post-loop probe MUST fire so ``to_transfer`` is rolled back rather
    than trusted."""
    import pipeline_job as _pj
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    card = _make_card(tmp_path, [
        ("DSC_0100.jpg", datetime(2026, 7, 3, 10, 0, 0), "red"),
        ("DSC_0101.jpg", datetime(2026, 7, 3, 11, 0, 0), "green"),
    ])

    probe_calls = {"count": 0}

    def spy_unmounted(baseline):
        probe_calls["count"] += 1
        if probe_calls["count"] >= 3:
            return "/Volumes/NAS"
        return None

    monkeypatch.setattr(_pj, "_unmounted_since_baseline", spy_unmounted)

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    result = run_import_job(
        _make_job(), CancelOnImportingRunner(), db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra,
        ),
    )

    assert result["cancelled"] is True
    assert probe_calls["count"] == 3, (
        f"expected 3 mount probes (pre-batch + per-file + post-loop) "
        f"but got {probe_calls['count']}"
    )
    # File one was queued for rsync; the post-loop probe must roll it
    # back to failed. rsync itself is gated on ``not cancelled`` and so
    # never runs (``copied == 0`` alone doesn't prove the probe fired;
    # the failed-with-detach reason does).
    assert result["copied"] == 0, result
    assert result["failed"] == 1, result
    assert result["safe_to_format"] is False, result
    assert any(
        "detached" in u["reason"] for u in result["unsafe_files"]
    ), result["unsafe_files"]


# --------------------------------------------------------------------------
# The ``_stop_requested`` closures threaded into ``_hash_dest_file`` supervise
# the watchdog thread that bounds every mount-side hash read. Import jobs are
# pausable, so ``runner.is_cancelled()`` parks inside ``wait_if_paused`` when
# a Pause is pending — wiring the watchdog to that pause-aware method would
# freeze the watchdog loop itself, stopping the 120s stall timer from running
# while the daemon reader can keep touching the archive at kernel-read speed.
# Mirrors the rsync watchdog's use of ``cancellation_requested`` for the same
# reason (see ``test_remote_import_rsync_watchdog_does_not_block_on_pause``).
# --------------------------------------------------------------------------


class _PauseWhileHashingRunner(FakeRunner):
    """FakeRunner with real JobRunner-shaped pause semantics: is_cancelled
    blocks during a pending Pause; cancellation_requested does not.

    Raises on the blocking call so a broken closure fails the test loudly
    instead of hanging the worker thread past its join timeout.
    """

    def __init__(self):
        super().__init__()
        self.paused_ids = set()

    def is_cancelled(self, job_id):
        if job_id in self.paused_ids and job_id not in self.cancelled_ids:
            raise AssertionError(
                "hash watchdog called the pause-aware is_cancelled and "
                "would have blocked the stall/cancel watchdog thread "
                "during Pause"
            )
        return job_id in self.cancelled_ids

    def cancellation_requested(self, job_id):
        return job_id in self.cancelled_ids


def test_local_import_hash_watchdog_does_not_block_on_pause(
        tmp_path, monkeypatch):
    """Local-path duplicate-gate: the ``_stop_requested`` closure threaded
    into ``_hash_dest_file`` must probe cancellation without parking in
    ``wait_if_paused``. Without the fix, a Pause during a twin hash would
    freeze the watchdog loop itself — stall timer disabled, Stop unobserved
    — and the daemon reader keeps hitting the archive."""
    import import_job as _ij
    from import_dedup import compute_file_hash
    from import_job import ImportParams, run_import_job

    card = tmp_path / "card"
    card.mkdir()
    card_file = card / "IMG_0300.jpg"
    Image.new("RGB", (16, 16), "red").save(str(card_file))

    archive = tmp_path / "archive"
    dest_dir = archive / "twins"
    dest_dir.mkdir(parents=True)
    # Real byte-identical twin (not a FIFO) — the duplicate gate hashes
    # it through ``_hash_dest_file`` to confirm the on-disk bytes match.
    twin_file = dest_dir / "IMG_0300.jpg"
    Image.new("RGB", (16, 16), "red").save(str(twin_file))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    _catalog_twin_row(
        db, dest_dir, "IMG_0300.jpg",
        os.path.getsize(str(twin_file)),
        compute_file_hash(str(twin_file)),
    )

    runner = _PauseWhileHashingRunner()
    job = _make_job()

    # Mark the job paused for exactly the window in which the watchdog's
    # ``cancel_check`` runs. Covers both the pre-open probe (line 788) and
    # every mid-read chunk boundary (line 817).
    real_hash_dest_file = _ij._hash_dest_file

    def spy_hash_dest_file(path, cancel_check, **kw):
        runner.paused_ids.add(job["id"])
        try:
            return real_hash_dest_file(path, cancel_check, **kw)
        finally:
            runner.paused_ids.discard(job["id"])

    monkeypatch.setattr(_ij, "_hash_dest_file", spy_hash_dest_file)

    # No AssertionError ⇒ the closure probed with the nonblocking method.
    # The twin's real bytes hash cleanly so the import lands the file as
    # ``skipped_duplicate``.
    result = run_import_job(
        job, runner, db_path, ws_id,
        ImportParams(sources=[str(card)], destination=str(archive)),
    )
    assert result["skipped_duplicate"] == 1, result
    assert result["failed"] == 0, result


def test_remote_import_hash_watchdog_does_not_block_on_pause(
        tmp_path, monkeypatch):
    """Remote-path mirror of the local hash-watchdog pause test — the
    remote ``_stop_requested`` closure has to use ``cancellation_requested``
    too. Each fix lands twice (see PR description)."""
    import import_job as _ij
    from import_dedup import compute_file_hash
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    card = tmp_path / "card"
    card.mkdir()
    card_file = card / "IMG_0300.jpg"
    Image.new("RGB", (16, 16), "red").save(str(card_file))

    dest_dir = Path(ra["mount_base"]) / "2026" / "2026-01-01"
    dest_dir.mkdir(parents=True)
    twin_file = dest_dir / "IMG_0300.jpg"
    Image.new("RGB", (16, 16), "red").save(str(twin_file))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    _catalog_twin_row(
        db, dest_dir, "IMG_0300.jpg",
        os.path.getsize(str(twin_file)),
        compute_file_hash(str(twin_file)),
    )

    runner = _PauseWhileHashingRunner()
    job = _make_job()

    real_hash_dest_file = _ij._hash_dest_file

    def spy_hash_dest_file(path, cancel_check, **kw):
        runner.paused_ids.add(job["id"])
        try:
            return real_hash_dest_file(path, cancel_check, **kw)
        finally:
            runner.paused_ids.discard(job["id"])

    monkeypatch.setattr(_ij, "_hash_dest_file", spy_hash_dest_file)

    result = run_import_job(
        job, runner, db_path, ws_id,
        ImportParams(
            sources=[str(card)], destination=ra["mount_base"],
            remote_target=ra, verify_by_hash=True,
        ),
    )
    assert result["skipped_duplicate"] == 1, result
    assert result["failed"] == 0, result


# --------------------------------------------------------------------------
# When a destination-side hash cancels mid-read (``DestReadCancelled``), the
# mount is misbehaving. The post-loop catalog block MUST NOT run: ``scan()``
# and its ``_hash_dest_file`` re-checks would touch the same wedged mount
# and pin the job in "cancelling" for the mount's own timeout — the exact
# failure mode this PR set out to eliminate. Already-landed bytes are picked
# up by the next run's crash-recovery adoption. A plain user Stop on a
# healthy mount leaves ``dest_read_cancelled`` False so partially-landed
# batches keep cataloging like before.
# --------------------------------------------------------------------------


class CancelOnFileImportingRunner(FakeRunner):
    """Flips to cancelled the moment the "importing" progress emit lands
    for a specific card filename — so an earlier file in the same batch can
    complete normally (populating ``landed``) before Stop
    trips on the target file's destination-side hash."""

    def __init__(self, filename):
        super().__init__()
        self.filename = filename

    def push_event(self, job_id, event_type, data):
        super().push_event(job_id, event_type, data)
        if (
            event_type == "progress"
            and ": importing" in (data.get("phase") or "")
            and data.get("current_file") == self.filename
        ):
            self.cancelled_ids.add(job_id)


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="os.mkfifo is POSIX-only; FIFO stands in for a dead SMB mount.",
)
def test_local_import_dest_read_cancel_skips_catalog_scan(
        tmp_path, monkeypatch):
    """Local path: an earlier file in the batch fresh-copies and lands.
    The next file's twin is a FIFO — twin-hash blocks, cancel fires, and
    ``DestReadCancelled`` trips ``dest_read_cancelled``. The post-loop
    ``if landed:`` catalog block MUST be skipped: ``scan()`` and the
    ``_rehash_dest_or_none`` re-checks below would touch the same wedged
    mount and pin the job in "cancelling"."""
    import threading

    import import_job as _ij
    from import_dedup import compute_file_hash
    from import_job import ImportParams, run_import_job

    card = tmp_path / "card"
    card.mkdir()
    a_file = card / "A.jpg"
    b_file = card / "B.jpg"
    Image.new("RGB", (16, 16), "red").save(str(a_file))
    Image.new("RGB", (16, 16), "blue").save(str(b_file))

    archive = tmp_path / "archive"
    twin_folder = archive / "old"
    twin_folder.mkdir(parents=True)
    # Only B has a cataloged twin — A fresh-copies. The twin file is a
    # FIFO so its hash read blocks forever until released (stands in for
    # a wedged SMB session).
    fifo = twin_folder / "B.jpg"
    os.mkfifo(str(fifo))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    _catalog_twin_row(
        db, twin_folder, "B.jpg",
        os.path.getsize(str(b_file)), compute_file_hash(str(b_file)),
    )

    scan_calls = []
    real_scan = _ij.scan if hasattr(_ij, "scan") else None

    def spy_scan(*args, **kwargs):
        scan_calls.append((args, kwargs))
        if real_scan is not None:
            return real_scan(*args, **kwargs)
        return None

    # Both local and remote paths do ``from scanner import scan`` inside
    # ``run_import_job``; patch at the scanner module so the fresh import
    # binds the spy.
    import scanner
    monkeypatch.setattr(scanner, "scan", spy_scan)

    runner = CancelOnFileImportingRunner("B.jpg")
    job = _make_job()
    result_box = {}

    def _run():
        result_box["result"] = run_import_job(
            job, runner, db_path, ws_id,
            ImportParams(sources=[str(card)], destination=str(archive)),
        )

    worker = threading.Thread(target=_run, daemon=True)
    worker.start()
    try:
        worker.join(timeout=15.0)
        assert not worker.is_alive(), (
            "Stop did not interrupt the destination hash read blocked on "
            "the dead-mount twin"
        )
    finally:
        _release_fifo(fifo)
        worker.join(timeout=5.0)

    result = result_box["result"]
    assert result["cancelled"] is True
    assert result["failed"] == 0, result
    # A landed on disk (fresh copy completed before B's cancel), so the
    # unfixed code would call ``scan()`` here — hitting the same wedged
    # mount and hanging. The gate on ``dest_read_cancelled`` must suppress
    # every catalog-block scan.
    assert scan_calls == [], (
        f"catalog scan() fired on a cancelled batch after "
        f"DestReadCancelled — would hang on the wedged mount: "
        f"{len(scan_calls)} calls"
    )
    # A's bytes are still on disk somewhere under the archive, ready for
    # the next run's crash-recovery adoption. Anywhere under the archive
    # is fine — the folder-template date depends on system tz, so don't
    # pin the exact subfolder.
    assert list(archive.rglob("A.jpg")), (
        "A's fresh copy should be on disk for the next run to adopt"
    )


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="os.mkfifo is POSIX-only; FIFO stands in for a dead SMB mount.",
)
def test_remote_import_dest_read_cancel_skips_catalog_scan(
        tmp_path, monkeypatch):
    """Remote path mirror. An earlier file in the batch is adopted from
    an already-on-disk (crash-recovery) mount copy, entering ``landed``
    with origin ``skipped_duplicate`` (since the PR 5a fold). The next
    file's cataloged twin is a FIFO — twin-hash blocks, cancel fires,
    ``DestReadCancelled`` trips ``dest_read_cancelled``. The post-loop
    ``if landed and not dest_read_cancelled:`` catalog block MUST be
    skipped."""
    import threading
    from datetime import datetime

    import import_job as _ij
    from import_dedup import compute_file_hash
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    card = tmp_path / "card"
    card.mkdir()
    a_file = card / "A.jpg"
    b_file = card / "B.jpg"
    Image.new("RGB", (16, 16), "red").save(str(a_file))
    Image.new("RGB", (16, 16), "blue").save(str(b_file))
    # Pin both card files' mtimes to the same date so the folder-template
    # resolves to a single deterministic dest_folder — the adopted-on-disk
    # A candidate and the FIFO twin for B must live in that same folder
    # for the intra-batch cancel-after-adopt scenario to fire.
    fixed_dt = datetime(2026, 1, 15, 12, 0, 0)
    ts = fixed_dt.timestamp()
    os.utime(str(a_file), (ts, ts))
    os.utime(str(b_file), (ts, ts))

    dest_dir = Path(ra["mount_base"]) / fixed_dt.strftime("%Y") / \
        fixed_dt.strftime("%Y-%m-%d")
    dest_dir.mkdir(parents=True)
    # A is already on the mount as byte-identical (crash-recovery shape):
    # the collision loop hashes it, matches, and adopts it into
    # ``landed`` (origin ``skipped_duplicate``) without going through
    # rsync.
    (dest_dir / "A.jpg").write_bytes(a_file.read_bytes())

    # B has a cataloged twin (different folder from ``dest_dir``) whose
    # on-disk file is a FIFO — twin-hash blocks and Stop trips
    # DestReadCancelled.
    twin_folder = Path(ra["mount_base"]) / "old"
    twin_folder.mkdir()
    fifo = twin_folder / "B.jpg"
    os.mkfifo(str(fifo))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    _catalog_twin_row(
        db, twin_folder, "B.jpg",
        os.path.getsize(str(b_file)), compute_file_hash(str(b_file)),
    )

    scan_calls = []
    real_scan = _ij.scan if hasattr(_ij, "scan") else None

    def spy_scan(*args, **kwargs):
        scan_calls.append((args, kwargs))
        if real_scan is not None:
            return real_scan(*args, **kwargs)
        return None

    import scanner
    monkeypatch.setattr(scanner, "scan", spy_scan)

    runner = CancelOnFileImportingRunner("B.jpg")
    job = _make_job()
    result_box = {}

    def _run():
        result_box["result"] = run_import_job(
            job, runner, db_path, ws_id,
            ImportParams(
                sources=[str(card)], destination=ra["mount_base"],
                remote_target=ra, verify_by_hash=True,
            ),
        )

    worker = threading.Thread(target=_run, daemon=True)
    worker.start()
    try:
        worker.join(timeout=15.0)
        assert not worker.is_alive(), (
            "Stop did not interrupt the destination hash read blocked on "
            "the dead-mount twin"
        )
    finally:
        _release_fifo(fifo)
        worker.join(timeout=5.0)

    result = result_box["result"]
    assert result["cancelled"] is True
    assert result["failed"] == 0, result
    # A was adopted from the mount before B's cancel, so ``landed``
    # holds its adoption entry. The unfixed code would call
    # ``scan()`` here — hitting the wedged mount and hanging. The gate on
    # ``dest_read_cancelled`` must suppress every catalog-block scan.
    assert scan_calls == [], (
        f"catalog scan() fired on a cancelled remote batch after "
        f"DestReadCancelled — would hang on the wedged mount: "
        f"{len(scan_calls)} calls"
    )
    # Nothing rsync'd either — Stop must reach the transfer gate too.
    assert calls["rsync"] == [], calls["rsync"]


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="os.mkfifo is POSIX-only; FIFO stands in for a dead SMB mount.",
)
def test_local_import_dest_read_cancel_skips_catalog_scan_adoption_path(
        tmp_path, monkeypatch):
    """Local path, ADOPTION geometry — mirror of
    ``test_remote_import_dest_read_cancel_skips_catalog_scan``. An
    earlier file in the batch (A) finds a byte-identical, uncataloged
    copy already at its template destination path and is ADOPTED via
    the collision loop. Adoption appends straight into ``landed`` (with
    origin ``"skipped_duplicate"``) — since the PR 5a fold this shape
    is identical on both paths, and both guards read ``if landed and
    not dest_read_cancelled:`` — so this geometry gates the ``landed``
    term through its adoption feeder. The next file's (B) cataloged twin is a
    FIFO: twin-hash blocks, cancel fires, ``DestReadCancelled`` trips
    ``dest_read_cancelled``, and the catalog block MUST be skipped.

    Characterization of the adoption "rollback" (verified against
    import_job.py): there is NONE on ``dest_read_cancelled``. The
    adopted file was counted ``skipped_duplicate`` at adopt time and
    only a ``mount_lost`` probe rolls ``landed`` back — so the run
    reports ``skipped_duplicate == 1`` even though the scan was
    suppressed and the adopted file was never cataloged. The pre-
    existing destination copy stays on disk for the next run's
    crash-recovery adoption to catalog."""
    import threading
    from datetime import datetime

    import import_job as _ij
    from import_dedup import compute_file_hash
    from import_job import ImportParams, run_import_job

    card = tmp_path / "card"
    card.mkdir()
    a_file = card / "A.jpg"
    b_file = card / "B.jpg"
    Image.new("RGB", (16, 16), "red").save(str(a_file))
    Image.new("RGB", (16, 16), "blue").save(str(b_file))
    # Pin mtimes so the default ``%Y/%Y-%m-%d`` folder template resolves
    # to one deterministic dest folder — the pre-written adoption
    # candidate for A must sit exactly where the import wants to land A.
    # Mirrors the remote adoption test's mtime pinning.
    fixed_dt = datetime(2026, 1, 15, 12, 0, 0)
    ts = fixed_dt.timestamp()
    os.utime(str(a_file), (ts, ts))
    os.utime(str(b_file), (ts, ts))

    archive = tmp_path / "archive"
    dest_dir = archive / fixed_dt.strftime("%Y") / \
        fixed_dt.strftime("%Y-%m-%d")
    dest_dir.mkdir(parents=True)
    # A is already at the destination as byte-identical and UNCATALOGED
    # (crash-recovery shape): the collision loop hashes it, matches, and
    # adopts it into ``landed`` without copying. The adopt-time
    # ``_hash_dest_file`` on this file runs BEFORE B's cancel fires
    # (CancelOnFileImportingRunner trips on B's "importing" emit), so —
    # exactly like the remote adoption mirror — the cancel fires at B's
    # cataloged-twin hash, not at A's adoption hash.
    (dest_dir / "A.jpg").write_bytes(a_file.read_bytes())

    # B's cataloged twin is a FIFO in a different folder — its hash read
    # blocks forever until released (stands in for a wedged SMB session).
    twin_folder = archive / "old"
    twin_folder.mkdir()
    fifo = twin_folder / "B.jpg"
    os.mkfifo(str(fifo))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    _catalog_twin_row(
        db, twin_folder, "B.jpg",
        os.path.getsize(str(b_file)), compute_file_hash(str(b_file)),
    )

    scan_calls = []
    real_scan = _ij.scan if hasattr(_ij, "scan") else None

    def spy_scan(*args, **kwargs):
        scan_calls.append((args, kwargs))
        if real_scan is not None:
            return real_scan(*args, **kwargs)
        return None

    import scanner
    monkeypatch.setattr(scanner, "scan", spy_scan)

    runner = CancelOnFileImportingRunner("B.jpg")
    job = _make_job()
    result_box = {}

    def _run():
        result_box["result"] = run_import_job(
            job, runner, db_path, ws_id,
            ImportParams(sources=[str(card)], destination=str(archive)),
        )

    worker = threading.Thread(target=_run, daemon=True)
    worker.start()
    try:
        worker.join(timeout=15.0)
        assert not worker.is_alive(), (
            "Stop did not interrupt the destination hash read blocked on "
            "the dead-mount twin"
        )
    finally:
        _release_fifo(fifo)
        worker.join(timeout=5.0)

    result = result_box["result"]
    assert result["cancelled"] is True
    assert result["failed"] == 0, result
    # A was adopted into ``landed`` before B's cancel, so the unfixed
    # code would call ``scan()`` here — hitting the same wedged mount
    # and hanging. The gate on ``dest_read_cancelled`` must suppress
    # every catalog-block scan.
    assert scan_calls == [], (
        f"catalog scan() fired on a cancelled batch after "
        f"DestReadCancelled — would hang on the wedged mount: "
        f"{len(scan_calls)} calls"
    )
    # Characterization: no adoption rollback on dest_read_cancelled.
    # The adopt-time counter stands even though the scan was skipped and
    # nothing was cataloged.
    assert result["copied"] == 0, result
    assert result["skipped_duplicate"] == 1, result
    assert db.conn.execute(
        "SELECT COUNT(*) FROM photos p JOIN folders f "
        "ON f.id = p.folder_id WHERE f.path = ?", (str(dest_dir),),
    ).fetchone()[0] == 0, "adopted file must NOT have been cataloged"
    # The adopted on-disk copy stays put for the next run to re-adopt.
    assert (dest_dir / "A.jpg").exists()


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="os.mkfifo is POSIX-only; FIFO stands in for a dead SMB mount.",
)
def test_remote_import_dest_read_cancel_skips_catalog_scan_fresh_transfer(
        tmp_path, monkeypatch):
    """Remote path, FRESH-TRANSFER geometry — the asymmetric corner of
    the 2x2 dest-read-cancel matrix. Mirror of
    ``test_local_import_dest_read_cancel_skips_catalog_scan`` (which
    gates the local guard's ``landed`` term via a fresh copy). No file
    is pre-written on the mount, so nothing can be adopted.

    Characterization surprise (verified against import_job.py): on the
    remote path ``landed`` can never actually be non-empty alongside
    ``dest_read_cancelled`` — transfers are queued per batch and only
    flushed after the per-file loop, behind ``if to_transfer and not
    cancelled:``, and every ``dest_read_cancelled = True`` site also
    sets ``cancelled = True`` and breaks. So here A is queued but never
    rsync'd (it stays on the card for the next run), and ``landed``
    stays empty.

    IMPORTANT: this test does NOT independently exercise the
    ``not dest_read_cancelled`` term of the guard ``if landed and not
    dest_read_cancelled:``. Because the guard's
    subject ``landed`` is structurally empty in this
    geometry, ``scan()`` is doubly protected — removing the
    ``dest_read_cancelled`` gate would still leave the empty subject
    suppressing ``scan()``. The three sibling tests in the matrix
    (local fresh, local adoption, remote adoption) DO exercise the gate
    with a nonempty subject; this test's role in the matrix is to pin
    the asymmetric fourth corner — that on remote+fresh cancellation,
    nothing ever crosses the network and the catalog block is
    unreachable via its subject rather than via the gate."""
    import threading

    import import_job as _ij
    from import_dedup import compute_file_hash
    from import_job import ImportParams, run_import_job

    ra = _remote_archive_for(tmp_path)
    calls = _remote_calls(ra)
    _install_fake_remote_rsync(monkeypatch, calls, verify=None)

    card = tmp_path / "card"
    card.mkdir()
    a_file = card / "A.jpg"
    b_file = card / "B.jpg"
    Image.new("RGB", (16, 16), "red").save(str(a_file))
    Image.new("RGB", (16, 16), "blue").save(str(b_file))
    # NO pre-written mount copy of A — pure fresh-transfer geometry. A
    # is queued for rsync; B's cancel then breaks the batch before the
    # transfer flush.

    # B has a cataloged twin whose on-disk file is a FIFO — twin-hash
    # blocks and Stop trips DestReadCancelled, same firing point as the
    # remote adoption test.
    twin_folder = Path(ra["mount_base"]) / "old"
    twin_folder.mkdir()
    fifo = twin_folder / "B.jpg"
    os.mkfifo(str(fifo))

    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    ws_id = db._active_workspace_id
    _catalog_twin_row(
        db, twin_folder, "B.jpg",
        os.path.getsize(str(b_file)), compute_file_hash(str(b_file)),
    )

    scan_calls = []
    real_scan = _ij.scan if hasattr(_ij, "scan") else None

    def spy_scan(*args, **kwargs):
        scan_calls.append((args, kwargs))
        if real_scan is not None:
            return real_scan(*args, **kwargs)
        return None

    import scanner
    monkeypatch.setattr(scanner, "scan", spy_scan)

    runner = CancelOnFileImportingRunner("B.jpg")
    job = _make_job()
    result_box = {}

    def _run():
        result_box["result"] = run_import_job(
            job, runner, db_path, ws_id,
            ImportParams(
                sources=[str(card)], destination=ra["mount_base"],
                remote_target=ra, verify_by_hash=True,
            ),
        )

    worker = threading.Thread(target=_run, daemon=True)
    worker.start()
    try:
        worker.join(timeout=15.0)
        assert not worker.is_alive(), (
            "Stop did not interrupt the destination hash read blocked on "
            "the dead-mount twin"
        )
    finally:
        _release_fifo(fifo)
        worker.join(timeout=5.0)

    result = result_box["result"]
    assert result["cancelled"] is True
    assert result["failed"] == 0, result
    # Pin the doubled-protection observables FIRST — this is what makes
    # this matrix corner asymmetric: on remote+fresh geometry the cancel
    # reaches the transfer gate before flush, so ``landed`` is
    # structurally empty at the catalog block. Nothing rsync'd (proxy
    # for no fresh entries); nothing adopted (no pre-written mount
    # copy).
    assert calls["rsync"] == [], calls["rsync"]
    assert result["copied"] == 0, result
    # ``scan_calls == []`` here is doubly protected — by the empty guard
    # subject AND by the ``dest_read_cancelled`` gate. This test does NOT
    # discriminate the ``not dest_read_cancelled`` term on its own; that
    # term is exercised by the three sibling tests in the 2x2 matrix
    # (local fresh, local adoption, remote adoption), each of which has
    # a nonempty ``landed`` at the cancel point. The
    # role of this test in the matrix is to pin the fourth (asymmetric)
    # corner: scan is skipped on remote+fresh cancellation because
    # nothing ever reached the guard's subject to begin with.
    assert scan_calls == [], (
        f"catalog scan() fired on the remote fresh-transfer cancel "
        f"corner — nothing was landed or adopted, so scan() must not "
        f"run: {len(scan_calls)} calls"
    )
