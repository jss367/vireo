"""An import whose working copies could not be made says so.

The import's deferred working-copy pass logged per-photo failures and swallowed
a crash of the whole pass, so the import page and the Jobs page reported a
clean import while photos were left without their JPEG working copy. The
originals are imported and verified either way, so this is a notice on the
result, not an import failure: ``ok`` stays true, the card-safety verdict is
unchanged, and processing still starts.
"""

import os

import pytest
from PIL import Image
from test_import_job import FakeRunner, _run_import, _stub_extractor


def _raw_card(tmp_path, names):
    """A card holding JPEG bytes under RAW names (extraction is stubbed)."""
    card = tmp_path / "card"
    card.mkdir()
    for i, name in enumerate(names):
        jpg = card / (name + ".jpg")
        Image.new("RGB", (16, 16), (40 * i, 80, 120)).save(str(jpg))
        os.rename(str(jpg), str(card / name))
    return card


def _params(tmp_path, card):
    from import_job import ImportParams

    return ImportParams(
        sources=[str(card)], destination=str(tmp_path / "archive"),
        vireo_dir=str(tmp_path / "vireo"),
    )


def test_per_photo_failures_are_named_and_explained(tmp_path, monkeypatch):
    _stub_extractor(monkeypatch, lambda src: not src.endswith("DSC_0002.NEF"))
    card = _raw_card(tmp_path, ["DSC_0001.NEF", "DSC_0002.NEF"])

    runner = FakeRunner()
    db, _ws, result = _run_import(tmp_path, _params(tmp_path, card), runner=runner)

    # The Jobs page lists the import under its step summary.
    import_summaries = [
        kw["summary"] for _job, step, kw in runner.step_updates
        if step == "import" and "summary" in kw
    ]
    assert import_summaries[-1] == (
        "2 copied, 0 already present, 0 failed of 2 discovered, "
        "1 photo without a working copy"
    )
    assert result["copied"] == 2
    assert result["ok"] is True
    assert result["safe_to_format"] is True
    [failed] = result["working_copy_failures"]
    assert failed.endswith(os.path.join("", "DSC_0002.NEF"))
    assert os.path.isabs(failed) and str(tmp_path / "archive") in failed
    assert result["working_copy_notice"] == (
        "Vireo could not create a working copy for 1 photo. The original "
        "was imported; only its working copy is missing. Vireo tries again "
        "the next time it scans the folder, once 24 hours have passed or "
        "sooner if the file changes."
    )
    # Not an import failure: the card-safety list and error tally are untouched.
    assert result["errors"] == [] and result["failed"] == 0


def test_a_crashed_pass_is_reported(tmp_path, monkeypatch):
    import scanner

    def crash(*_args, **_kwargs):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(scanner, "_extract_working_copies", crash)
    card = _raw_card(tmp_path, ["DSC_0001.NEF"])

    _db, _ws, result = _run_import(tmp_path, _params(tmp_path, card))

    assert result["ok"] is True and result["copied"] == 1
    assert "working_copy_failures" not in result
    assert result["working_copy_notice"] == (
        "Vireo stopped creating working copies after an error ([Errno 28] No "
        "space left on device), so some photos from this import may not have "
        "one yet. The originals were imported. Vireo creates the missing "
        "working copies the next time it scans their folders."
    )


def test_a_clean_pass_adds_nothing(tmp_path, monkeypatch):
    _stub_extractor(monkeypatch, lambda src: True)
    card = _raw_card(tmp_path, ["DSC_0001.NEF"])

    _db, _ws, result = _run_import(tmp_path, _params(tmp_path, card))

    assert "working_copy_notice" not in result
    assert "working_copy_failures" not in result


@pytest.mark.parametrize("count, expected", [
    (2, "Vireo could not create a working copy for 2 photos. The originals "
        "were imported; only the working copies are missing. Vireo tries "
        "again the next time it scans their folders, once 24 hours have "
        "passed or sooner if a file changes."),
])
def test_notice_wording_for_several_photos(count, expected):
    from scanner import import_working_copy_notice

    assert import_working_copy_notice(["/a/1.NEF", "/a/2.NEF"][:count], None) == expected


def test_the_extraction_pass_returns_the_photos_it_failed(tmp_path, monkeypatch):
    from db import Database
    from scanner import _extract_working_copies
    from services import photo_ingest

    _stub_extractor(monkeypatch, lambda src: False)
    folder = tmp_path / "photos"
    folder.mkdir()
    (folder / "bird.NEF").write_bytes(b"raw")
    db = Database(str(tmp_path / "t.db"))
    fid = db.add_folder(str(folder), name="photos")
    photo_ingest.add_photo(db, folder_id=fid, filename="bird.NEF", extension=".nef",
                           file_size=3, file_mtime=1.0)

    failures = _extract_working_copies(db, str(tmp_path / "vireo"), scope=[str(folder)])

    assert failures == [str(folder / "bird.NEF")]


def test_jobs_page_details_carry_the_notice_and_the_photos():
    from job_summaries import describe_result

    notice = "Vireo could not create a working copy for 1 photo. (...)"
    out = describe_result("import", {
        "discovered": 1, "copied": 1, "failed": 0, "ok": True, "errors": [],
        "working_copy_notice": notice,
        "working_copy_failures": ["/archive/2026/DSC_0002.NEF"],
    })
    assert out["details"][:3] == [notice, "1 working copy failure:", "/archive/2026/DSC_0002.NEF"]
    assert "Working copy notice" not in out["summary"]


def test_in_place_import_names_photos_without_a_working_copy(app_and_db, tmp_path, monkeypatch):
    from wait import wait_for_job_via_client

    _stub_extractor(monkeypatch, lambda src: False)
    app, _db = app_and_db
    card = _raw_card(tmp_path, ["DSC_0001.NEF"])
    client = app.test_client()

    resp = client.post("/api/jobs/import-in-place", json={
        "sources": [str(card)], "after_import": None,
    })
    assert resp.status_code == 200, resp.get_json()
    job = wait_for_job_via_client(client, resp.get_json()["job_id"])

    assert job["status"] == "completed", job
    result = job["result"]
    assert result["ok"] is True and result["errors"] == []
    assert result["working_copy_failures"] == [str(card / "DSC_0001.NEF")]
    assert result["working_copy_notice"].startswith(
        "Vireo could not create a working copy for 1 photo."
    )
    scan_step = next(step for step in job["steps"] if step["id"] == "scan")
    assert scan_step["summary"] == "1 photos, 1 without a working copy"


def test_a_mid_pass_crash_still_names_the_photos_already_marked(tmp_path, monkeypatch):
    """A pass that commits per-photo failures then raises keeps those paths.

    Without this, the ``working_copy_notice`` says "stopped after an error"
    while the per-photo failures the pass had already committed to the
    catalog (and that will stay skipped for 24h) are silently dropped from
    ``working_copy_failures`` and the step summary.
    """
    import scanner

    def crashing_extract(*args, **kwargs):
        # Append to the sink the batch would normally write to, then raise
        # as if the row that followed hit an unrecoverable error.
        sink = kwargs.get("failed_paths_sink")
        folder = tmp_path / "archive" / "card" / "sub"
        folder.mkdir(parents=True, exist_ok=True)
        marker = folder / "DSC_0002.NEF"
        marker.write_bytes(b"placeholder")
        if sink is not None:
            sink.append(str(marker))
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(scanner, "_extract_working_copies", crashing_extract)
    _stub_extractor(monkeypatch, lambda src: True)
    card = _raw_card(tmp_path, ["DSC_0001.NEF"])

    _db, _ws, result = _run_import(tmp_path, _params(tmp_path, card))

    assert result["ok"] is True
    assert result["working_copy_failures"], (
        "committed per-photo failures must be preserved across a pass crash"
    )
    assert any("DSC_0002.NEF" in path for path in result["working_copy_failures"])
    # Both the per-photo wording and the pass-level wording appear.
    notice = result["working_copy_notice"]
    assert "could not create a working copy for 1 photo" in notice
    assert "stopped creating working copies after an error" in notice


def test_a_concurrent_publisher_does_not_turn_into_a_false_failure(tmp_path, monkeypatch):
    """A guarded ``mark_failed`` that matches no rows must not report the photo.

    Simulates a concurrent on-demand publisher that gave the row a working
    copy while this batch's extraction was running: ``mark_failed`` returns
    0 (``working_copy_path IS NULL`` fails), so the batch leaves the photo
    off ``failed_paths`` and the import notice does not falsely claim it is
    without a working copy.
    """
    from db import Database
    from scanner import _extract_working_copies
    from services import photo_ingest

    _stub_extractor(monkeypatch, lambda src: False)
    folder = tmp_path / "photos"
    folder.mkdir()
    (folder / "bird.NEF").write_bytes(b"raw")
    db = Database(str(tmp_path / "t.db"))
    fid = db.add_folder(str(folder), name="photos")
    photo_ingest.add_photo(
        db, folder_id=fid, filename="bird.NEF", extension=".nef",
        file_size=3, file_mtime=1.0,
    )

    # Stand in for a concurrent publisher: `mark_failed`'s guard includes
    # ``working_copy_path IS NULL``, so pre-setting the path makes the
    # UPDATE match zero rows exactly as a late on-demand publication would.
    import repositories.working_copies as wc_repo
    real_mark_failed = wc_repo.WorkingCopyRepository.mark_failed

    def mark_failed_after_publisher(self, row, failure_source):
        self.conn.execute(
            "UPDATE photos SET working_copy_path=? WHERE id=?",
            ("working/999.jpg", row["id"]),
        )
        return real_mark_failed(self, row, failure_source)

    monkeypatch.setattr(
        wc_repo.WorkingCopyRepository, "mark_failed", mark_failed_after_publisher,
    )

    failures = _extract_working_copies(db, str(tmp_path / "vireo"), scope=[str(folder)])

    assert failures == [], (
        "mark_failed matched no rows (another writer installed a working "
        "copy), so the photo must not be reported as without one"
    )
