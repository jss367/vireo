"""Human-readable job result text (job_summaries.describe_result)."""

import json

from job_summaries import describe_result


def test_batch_delete_trash_mode_reads_as_prose():
    result = {"deleted": 28, "failed_photo_ids": [], "ok": True,
              "trash_failed": [], "trashed": 28}
    out = describe_result("batch-delete", result, {"mode": "disk"})
    assert out["summary"] == "28 photos removed from Vireo, 28 files moved to Trash"
    assert out["details"] == []
    assert out["error"] is None
    assert "{" not in out["summary"]
    assert "ok" not in out["summary"]


def test_batch_delete_vireo_only_mode():
    out = describe_result(
        "batch-delete",
        {"ok": True, "deleted": 1, "trashed": 0, "trash_failed": [], "failed_photo_ids": []},
        {"mode": "vireo"},
    )
    assert out["summary"] == "1 photo removed from Vireo, files kept on disk"


def test_batch_delete_lists_trash_failures_and_permanent_wording():
    result = {
        "ok": True, "deleted": 0, "trashed": 0,
        "trash_failed": [
            {"path": "/Volumes/P/DSC_6827.NEF", "error": "Finder Trash timed out after 30s", "photo_id": 1},
            {"path": "/Volumes/P/DSC_6828.NEF"},
        ],
        "failed_photo_ids": [1, 2],
    }
    out = describe_result("batch-delete", result, {"mode": "disk"})
    assert out["summary"] == "Nothing deleted, 2 photos failed"
    assert out["details"][0] == "2 files could not be moved to Trash:"
    assert out["details"][1] == "/Volumes/P/DSC_6827.NEF: Finder Trash timed out after 30s"
    assert out["details"][2] == "/Volumes/P/DSC_6828.NEF"

    permanent = describe_result(
        "batch-delete", {"ok": True, "deleted": 0, "trashed": 3, "trash_failed": []},
        {"mode": "disk_permanent"},
    )
    assert permanent["summary"] == "3 files deleted permanently"


def test_detail_lists_are_capped():
    errors = [f"file{i}.NEF: source file missing" for i in range(25)]
    out = describe_result("export", {"exported": 5, "errors": errors, "destination": "/out"})
    assert out["summary"] == "5 photos exported to /out, 25 errors"
    assert out["details"][0] == "25 errors:"
    assert len(out["details"]) == 12  # heading + 10 items + "and 15 more"
    assert out["details"][-1] == "and 15 more"


def test_known_shapes_from_history():
    cases = {
        ("thumbnails", '{"generated": 644, "skipped": 930, "failed": 0}'):
            "644 thumbnails generated, 930 already up to date",
        ("previews", '{"generated": 0, "skipped": 1574, "total": 1574}'):
            "0 previews generated, 1,574 already up to date",
        ("classify", '{"classified": 7, "groups": 1, "failed": 0, "total": 7}'):
            "7 of 7 photos classified in 1 group",
        ("cull", '{"total_photos": 1574, "suggested_keepers": 852, "suggested_rejects": 722, "species_count": 151}'):
            "1,574 photos reviewed: 852 suggested keepers, 722 suggested rejects across 151 species",
        ("prepare-full-resolution", '{"ok": true, "ready": 151, "copied": 151, "reused": 0, "failed": 0, "total": 151, "bytes": 2995224419, "errors": []}'):
            "151 of 151 full-resolution files ready (151 copied, 2.8 GB)",
        ("verify-models", '{"verified": 0, "failed": [], "ok": []}'):
            "0 models verified",
        ("verify-models", '{"verified": 5, "failed": ["a", "b"], "ok": ["c", "d", "e"]}'):
            "3 of 5 models verified, 2 failed",
        ("fetch-labels", '{"species_count": 10440, "labels_file": "/tmp/labels.txt"}'):
            "10,440 species labels fetched",
        ("regroup", '{"total_photos": 930, "encounter_count": 29, "burst_count": 368, "keep_count": 108, "review_count": 518, "reject_count": 304, "rarity_protected": 31}'):
            "930 photos grouped into 29 encounters and 368 bursts",
        ("sharpness", '{"scored_count": 537, "group_count": 48, "auto_flagged": 48}'):
            "537 photos scored for sharpness in 48 groups, 48 auto-flagged",
        ("download-vit-s14", '{"status": "downloaded", "model_id": "vit-s14"}'):
            "Model vit-s14 downloaded",
        ("ingest", '{"copied": 1406, "skipped_duplicate": 0, "failed": 0, "total": 1406}'):
            "1,406 photos imported",
        ("capture-time", '{"updated": 1, "failed": 0, "failures": [], "shift_minutes": -180, "shifts_vary": false}'):
            "1 photo updated, capture time shifted -3 hours",
        ("develop", '{"developed": 0, "errors": 1, "total": 1}'):
            "0 of 1 photo developed, 1 error",
        ("duplicate-scan", '{"proposals": [{"status": "resolved"}, {"status": "pending"}]}'):
            "2 duplicate groups found, 1 resolved",
        ("publish-site", '{"destination": "/Volumes/Lexar/Pictures", "data_files": ["a"], "exported_images": 5656, "errors": ["x: missing"]}'):
            "5,656 images published to /Volumes/Lexar/Pictures, 1 error",
    }
    for (job_type, raw), expected in cases.items():
        out = describe_result(job_type, json.loads(raw))
        assert out["summary"] == expected, (job_type, out["summary"])


def test_regroup_and_fetch_labels_details():
    out = describe_result("regroup", {"total_photos": 10, "encounter_count": 2, "burst_count": 3,
                                      "keep_count": 4, "review_count": 5, "reject_count": 1,
                                      "rarity_protected": 2})
    assert out["details"] == ["4 keep, 5 review, 1 reject", "2 photos protected as rare species"]
    out = describe_result("fetch-labels", {"species_count": 3, "labels_file": "/x/y.txt"})
    assert out["details"] == ["Saved to /x/y.txt"]
    out = describe_result("fetch-labels", {"species_count": 3, "labels_file": "/x/y.txt",
                                           "disambiguated": 2})
    assert out["details"][0] == (
        "2 labels whose common name is used by more than one species — "
        "saved as “Common Name (Scientific name)”"
    )


def test_explicit_summary_wins():
    out = describe_result("work-locally-folder-sync",
                          {"summary": "10 published, 0 deleted", "folders": [{"ok": True}]})
    assert out["summary"] == "10 published, 0 deleted"
    assert out["details"] == []

    # The authored sentence stays, but details still come from the payload.
    out = describe_result("move-folder", {
        "ok": True, "moved": 8, "errors": ["DSC_1.NEF: stem exists", "DSC_2.NEF: stem exists"],
        "summary": "Moved 8 photos, 2 error(s)",
    })
    assert out["summary"] == "Moved 8 photos, 2 error(s)"
    assert out["details"] == ["2 errors:", "DSC_1.NEF: stem exists", "DSC_2.NEF: stem exists"]


def test_error_only_result_surfaces_error():
    out = describe_result("sync", {"error": "No module named 'xmp_writer'"})
    assert out["error"] == "No module named 'xmp_writer'"
    assert out["summary"] == "No module named 'xmp_writer'"
    assert out["details"] == []

    # A failed download must not invent "Download complete" next to the error.
    out = describe_result("download-vit-b14", {"error": "Connection reset by peer"})
    assert out["summary"] == "Connection reset by peer"
    assert out["details"] == []

    # Job-authored summaries survive alongside the error.
    out = describe_result("move-folder", {"moved": 0, "errors": ["rsync stalled"],
                                          "summary": "Move failed — rsync stalled",
                                          "error": "rsync stalled"})
    assert out["summary"] == "Move failed — rsync stalled"
    assert out["error"] == "rsync stalled"


def test_non_model_downloads_keep_their_details():
    out = describe_result("download-megadetector",
                          {"status": "downloaded", "size": "233.4 MB", "path": "/m/md_v6.onnx"})
    assert out["summary"] == "MegaDetector downloaded (233.4 MB)"
    assert out["details"] == ["Saved to /m/md_v6.onnx"]

    out = describe_result("download-darktable", {
        "version": "5.2.1", "downloaded_to": "/tmp/darktable.dmg",
        "verified": "SHA-256 matches the published checksum", "action": "installed",
        "bin_path": "/Applications/darktable.app/Contents/MacOS/darktable-cli",
        "config_written": True, "quarantined": False,
    })
    assert out["summary"] == "darktable 5.2.1 installed"
    assert out["details"] == [
        "Vireo will use /Applications/darktable.app/Contents/MacOS/darktable-cli",
        "Saved as the darktable path in Settings",
        "SHA-256 matches the published checksum",
    ]

    out = describe_result("download-darktable", {
        "version": "5.2.1", "downloaded_to": "/Downloads/darktable.exe",
        "verified": "ok", "action": "opened-installer", "bin_path": None,
        "config_written": False, "quarantined": False,
    })
    assert out["summary"] == "darktable 5.2.1 downloaded, installer opened"
    assert out["details"][0] == "Saved to /Downloads/darktable.exe"

    # Unknown download-* types without a model id keep their fields as details.
    out = describe_result("download-something",
                          {"status": "downloaded", "size": "12 MB", "path": "/x/y"})
    assert out["summary"] == "Download complete"
    assert out["details"] == ["Size: 12 MB", "Path: /x/y"]

    # Model downloads still read as before, including via config.model_id.
    assert describe_result("download-vit-s14", {"status": "downloaded"},
                           {"model_id": "vit-s14"})["summary"] == "Model vit-s14 downloaded"


def test_capture_time_reports_skipped():
    out = describe_result("capture-time", {"updated": 0, "skipped": 12, "failed": 0,
                                           "failures": [], "shift_minutes": 0})
    assert out["summary"] == "0 photos updated, 12 already correct"
    out = describe_result("capture-time", {"updated": 3, "skipped": 2, "failed": 1,
                                           "failures": ["a.NEF: locked"], "shift_minutes": 60})
    assert out["summary"] == "3 photos updated, 2 already correct, capture time shifted +1 hour, 1 failed"
    assert out["details"] == ["Failed:", "a.NEF: locked"]


def test_site_export_summary_reports_partial_results_and_destination():
    out = describe_result('export-site', {
        'exported_images': 8, 'photo_count': 10, 'album_count': 2,
        'destination': '/exports/site-export-example',
        'errors': ['Photo 1: missing', 'Photo 2: unreadable'],
    })
    assert '8 of 10 photos exported' in out['summary']
    assert 'incomplete: 2 errors' in out['summary']
    assert 'Export folder: /exports/site-export-example' in out['details']
    assert 'Photo 1: missing' in out['details']


def test_verify_hashes_reports_every_problem_count():
    out = describe_result("verify-hashes", {
        "checked": 100, "ok": 95, "baselined": 3, "modified": 0, "corrupt": 0,
        "unreadable": 1, "missing": 1, "cancelled": False,
    })
    assert out["summary"] == "100 files checked: 95 unchanged, 3 newly baselined, 1 unreadable, 1 missing"
    out = describe_result("verify-hashes", {
        "checked": 10, "ok": 10, "baselined": 0, "modified": 0, "corrupt": 0,
        "unreadable": 0, "missing": 0, "cancelled": True,
    })
    assert out["summary"] == "10 files checked before cancel: 10 unchanged"
    out = describe_result("verify-hashes", {"checked": 0, "ok": 0, "cancelled": False})
    assert out["summary"] == "0 files checked"


def test_generic_orders_nonzero_counts_before_zero_ones():
    out = describe_result("some-audit", {
        "checked": 100, "ok": 95, "baselined": 0, "modified": 0, "corrupt": 0,
        "unreadable": 1, "missing": 1,
    })
    assert out["summary"] == "Checked: 100, Ok: 95, Unreadable: 1, Missing: 1"
    # A boolean ok is still treated as bookkeeping.
    assert describe_result("x", {"ok": True, "count": 2})["summary"] == "Count: 2"


def test_failure_records_keep_photo_ids_and_prefer_reasons():
    out = describe_result("sync", {"synced": 1, "failed": 2, "failures": [
        {"photo_id": 41, "error": "PermissionError: [Errno 13]", "reason": "File is read-only"},
        {"photo_id": 42, "change_id": 7, "error": "unsupported change type: rating"},
    ]})
    assert out["summary"] == "1 photo synced, 2 failed"
    assert out["details"] == ["Failed:", "Photo 41: File is read-only",
                              "Photo 42: unsupported change type: rating"]


def test_previews_report_failures_and_cull_reports_missing_phash():
    out = describe_result("previews", {"generated": 0, "skipped": 0, "failed": 7, "total": 7})
    assert out["summary"] == "0 previews generated, 7 failed"
    out = describe_result("cull", {"total_photos": 10, "suggested_keepers": 6,
                                   "suggested_rejects": 4, "species_count": 2,
                                   "photos_missing_phash": 3})
    assert out["details"] == [
        "3 photos could not be fingerprinted for scene grouping and were grouped on their own"
    ]
    assert describe_result("cull", {"total_photos": 1, "photos_missing_phash": 0})["details"] == []


def test_card_cleanup_scan_surfaces_nested_totals():
    out = describe_result("card-cleanup-scan", {
        "cancelled": False,
        "totals": {"deletable": {"count": 120, "bytes": 3_000_000_000},
                   "kept": {"count": 4, "bytes": 90_000_000},
                   "ignored": {"count": 2, "bytes": 0}},
        "source_root": "/Volumes/SD_CARD/DCIM", "manifest_path": "/x/manifest.json",
        "walk_errors": 1,
    })
    assert out["summary"] == "120 card files safe to delete (2.8 GB), 4 to keep (85.8 MB), 2 ignored"
    assert out["details"] == ["Card: /Volumes/SD_CARD/DCIM",
                              "1 folder could not be read during the walk"]
    assert describe_result("card-cleanup-scan", {"cancelled": True})["summary"] == "Scan cancelled"


def test_staging_verify_leads_with_the_verdict():
    base = {"path": "/v/staging/abc", "source_root": "/v/staging/abc/files", "name": "abc",
            "file_count": 40, "bytes": 2_000_000_000, "verified": 40, "unaccounted": 0,
            "unreachable": 0, "details": [], "can_delete": True, "status": "safe_to_delete",
            "inferred_destination": "/Volumes/Photography/2026-08-01"}
    out = describe_result("staging-verify", base)
    assert out["summary"] == "abc: all 40 files verified in the archive, safe to delete"
    assert out["details"] == ["Staging folder: /v/staging/abc/files",
                              "Archive destination: /Volumes/Photography/2026-08-01",
                              "Size: 1.9 GB"]

    needs = dict(base, verified=38, unaccounted=2, can_delete=False, status="needs_import",
                 details=[{"path": "/v/staging/abc/files/a.NEF", "rel": "a.NEF", "status": "unaccounted"},
                          {"path": "/v/staging/abc/files/b.NEF", "rel": "b.NEF", "status": "verified"}])
    out = describe_result("staging-verify", needs)
    assert out["summary"] == "abc: 2 files not found in the archive, still needs importing (38 of 40 verified)"
    assert out["details"][-2:] == ["Not verified:", "a.NEF: unaccounted"]

    unreachable = dict(base, verified=0, unreachable=40, can_delete=False, status="unreachable")
    out = describe_result("staging-verify", unreachable)
    assert out["summary"].startswith("abc: 40 files could not be checked because the archive is unreachable")


def test_card_cleanup_verify_names_every_problem():
    out = describe_result("card-cleanup-verify", {
        "hashes_total": 120, "hashes_processed": 120, "archive_files_read": 118,
        "verified": 115, "modified": 2, "corrupt": 3, "unreadable": 0,
        "cancelled": False, "remaining": 0, "unblocked_files": 400, "unblocked_bytes": 5_000_000_000,
    })
    assert out["summary"] == "115 of 120 archive copies verified, 2 modified, 3 corrupt"
    assert out["details"] == ["400 card files now cleared for deletion (4.7 GB)"]
    out = describe_result("card-cleanup-verify", {
        "hashes_total": 120, "hashes_processed": 40, "verified": 40, "modified": 0,
        "corrupt": 0, "unreadable": 0, "cancelled": True, "remaining": 80,
    })
    assert out["summary"] == "Cancelled after 40 of 120 archive copies checked"
    assert out["details"] == ["80 archive copies not yet checked"]


def test_card_cleanup_delete_uses_exact_totals_for_sampled_lists():
    skipped = [{"path": f"/card/{i}.NEF", "reason": "changed since scan"} for i in range(500)]
    out = describe_result("card-cleanup-delete", {
        "deleted": 1200, "deleted_bytes": 30_000_000_000,
        "skipped": skipped, "failed": [{"path": "/card/x.NEF", "error": "EACCES"}],
        "skipped_total": 990, "failed_total": 1,
    })
    assert out["summary"] == "1,200 card files deleted (27.9 GB freed), 990 skipped, 1 failed"
    assert out["details"][0] == "Failed:"
    assert out["details"][1] == "/card/x.NEF: EACCES"
    assert out["details"][2] == "Skipped (showing 500 of 990):"
    assert out["details"][-1] == "and 490 more"

    # Unknown job types get the same exact-total treatment from the fallback.
    out = describe_result("some-bulk-job", {"done": 3, "skipped": skipped[:2], "skipped_total": 900})
    assert out["summary"] == "Done: 3, 900 skipped"
    assert out["details"][0] == "900 skipped (showing 2):"


def test_generic_hides_ids():
    out = describe_result("import-in-place", {"discovered": 3, "indexed": 3,
                                              "process_job_id": "pipeline-1"})
    assert out["summary"] == "Discovered: 3, Indexed: 3"


def test_generic_fallback_humanizes_unknown_shapes():
    out = describe_result("some-new-job", {
        "ok": True, "photos_indexed": 200, "skipped_files": ["a.jpg", "b.jpg"],
        "nested": {"x": 1}, "label": "", "ratio": 0.5,
    })
    assert out["summary"] == "Photos indexed: 200, 2 skipped files, Ratio: 0.5"
    assert out["details"] == ["2 skipped files:", "a.jpg", "b.jpg"]
    assert "ok" not in out["summary"].lower().split(",")[0]

    # Opaque records get counted, never dumped field by field.
    out = describe_result("work-locally-folder-stage",
                          {"folders": [{"ok": True, "root_folder_id": 144, "files": 3275}]})
    assert out["summary"] == "1 folder"
    assert out["details"] == []

    assert describe_result("download-taxonomy", {"ok": True})["summary"] == "Taxonomy downloaded"
    assert describe_result("download-model", {"model_id": "bioclip-2", "weights_path": "hf-hub:x"})["summary"] == "Model bioclip-2 downloaded"
    assert describe_result("scan", {"photos_indexed": 1639})["summary"] == "1,639 photos indexed"
    assert describe_result("sync", {"synced": 101, "failed": 0, "failures": []})["summary"] == "101 photos synced"
    assert describe_result("precompute-embeddings", {"labels": 532, "model": "BioCLIP-2.5"})["summary"] == "532 label embeddings precomputed with BioCLIP-2.5"


def test_failed_job_leads_with_error_and_keeps_partial_progress():
    out = describe_result("classify", {"classified": 344, "total": 548, "failed": 0,
                                       "error": "name 'Image' is not defined"})
    assert out["summary"] == "name 'Image' is not defined"
    assert out["error"] == "name 'Image' is not defined"
    assert out["details"][0] == "344 of 548 photos classified"

    out = describe_result("pipeline", {"stages": {}, "duration": 89.2, "collection_id": 31,
                                       "errors": ["[model_loader] Fatal: incomplete"],
                                       "error": "[model_loader] Fatal: incomplete"})
    assert out["summary"] == "[model_loader] Fatal: incomplete"
    assert "Duration" not in " ".join(out["details"])
    assert out["details"] == ["1 error", "1 error:", "[model_loader] Fatal: incomplete"]


def test_non_dict_results():
    assert describe_result("x", None) == {"summary": "", "details": [], "error": None}
    assert describe_result("x", "done")["summary"] == "done"
    assert describe_result("x", 5)["summary"] == ""


def test_legacy_history_summaries_are_recomputed(tmp_path):
    """Rows written before the prose describer carry ``ok: True, deleted:
    28`` summaries; reading them back must yield prose. Step-derived and
    job-authored summaries are left alone."""
    from db import Database
    from jobs import JobRunner

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    runner = JobRunner(db=db)

    rows = [
        ("legacy", "batch-delete", '{"ok": true, "deleted": 28, "trashed": 28, "trash_failed": [], "failed_photo_ids": []}',
         '[]', "ok: True, deleted: 28, trashed: 28", '{"mode": "disk"}'),
        ("steps", "scan", '{"photos_indexed": 5}',
         '[{"id": "scan", "label": "Scan", "status": "completed", "summary": "142 folders"}]',
         "142 folders", "{}"),
        ("authored", "move-folder", '{"moved": 0, "summary": "Move failed — rsync timed out"}',
         '[]', "Move failed — rsync timed out", "{}"),
        ("interrupted", "scan", '{"error": "Interrupted by Vireo restart", "interrupted": true, "last_progress_at": "2026-01-01T00:03:00"}',
         '[]', "Interrupted by Vireo restart at 1,234 of 5,000", "{}"),
    ]
    for i, (jid, jtype, result, tree, summary, config) in enumerate(rows):
        db.conn.execute(
            """INSERT INTO job_history
               (id, type, status, started_at, finished_at, duration,
                result, error_count, config, workspace_id, tree, summary)
               VALUES (?, ?, 'completed', ?, ?, 1.0, ?, 0, ?, ?, ?, ?)""",
            (jid, jtype, f"2026-01-01T00:0{i}:00", f"2026-01-01T00:0{i}:01",
             result, config, ws_id, tree, summary),
        )
    db.conn.commit()

    by_id = {r["id"]: r for r in runner.get_history(db, limit=10)}
    assert by_id["legacy"]["summary"] == "28 photos removed from Vireo, 28 files moved to Trash"
    assert by_id["steps"]["summary"] == "142 folders"
    assert by_id["authored"]["summary"] == "Move failed — rsync timed out"
    assert by_id["interrupted"]["summary"] == "Interrupted by Vireo restart at 1,234 of 5,000"
    assert by_id["interrupted"]["result_details"] == []


def test_failed_snapshot_with_partial_result_carries_fatal_error(tmp_path):
    """A worker that stores a partial result and then raises (offline-cache
    does this) must describe the failure in the live snapshot, not only in
    the persisted history row."""
    from db import Database
    from jobs import JobRunner
    from tests.test_jobs import wait_for_job_via_runner

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    runner = JobRunner(db=db)

    def work(job):
        job["result"] = {"cached": 3, "skipped": 0, "failed": 2, "total": 5, "bytes": 10}
        job["_fatal_error"] = "2/5 photos could not be cached: disk full"
        raise RuntimeError("disk full")

    job_id = runner.start("offline-cache", work, workspace_id=ws_id)
    wait_for_job_via_runner(runner, job_id, wait_for_history=True)

    snap = runner.get(job_id)
    assert snap["status"] == "failed"
    assert snap["error"] == "2/5 photos could not be cached: disk full"
    assert snap["summary"] == "2/5 photos could not be cached: disk full"
    assert any("Cached: 3" in line for line in snap["result_details"])

    row = runner.get_history(db, limit=1)[0]
    assert row["summary"] == snap["summary"]
    assert row["error"] == snap["error"]


def test_runner_history_and_snapshot_carry_prose(tmp_path):
    """Both the history row and the in-memory snapshot expose summary and
    result_details, so the Jobs page never has to render the raw dict."""
    from db import Database
    from jobs import JobRunner
    from tests.test_jobs import wait_for_job_via_runner

    db = Database(str(tmp_path / "test.db"))
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    runner = JobRunner(db=db)

    def work(job):
        return {"ok": True, "deleted": 2, "trashed": 2,
                "trash_failed": [{"path": "/p/a.NEF", "error": "busy"}],
                "failed_photo_ids": [9]}

    job_id = runner.start("batch-delete", work, workspace_id=ws_id,
                          config={"mode": "disk"})
    wait_for_job_via_runner(runner, job_id, wait_for_history=True)

    snap = runner.get(job_id)
    assert snap["summary"] == "2 photos removed from Vireo, 2 files moved to Trash, 1 photo failed"
    assert snap["result_details"] == ["1 file could not be moved to Trash:", "/p/a.NEF: busy"]

    row = runner.get_history(db, limit=1)[0]
    assert row["summary"] == snap["summary"]
    assert row["result_details"] == snap["result_details"]
    assert "{" not in row["summary"]
