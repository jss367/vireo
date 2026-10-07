"""Card cleanup: verify removable-media files against the archive, then delete.

Scan writes a manifest keyed by the scan job id; verify and delete read it
back. Verify/delete registration happens under one lock so two POSTs for
the same scan cannot both pass the conflict check and start workers.
"""

from __future__ import annotations

import os
import threading

import card_cleanup
import path_guard
from db import Database
from flask import Blueprint, jsonify, request
from web.background_jobs import make_background_job


def create_card_cleanup_blueprint(get_db, json_error, get_runner, db_path, config):
    """Build the card-cleanup blueprint.

    ``config`` is the Flask app's config mapping (``CARD_CLEANUP_DIR``).
    """
    blueprint = Blueprint("card_cleanup", __name__)
    background_job = make_background_job(get_runner, get_db, db_path, Database)
    _CARD_CLEANUP_JOB_LOCK = threading.Lock()

    def _resolve_completed_card_cleanup_scan(db, runner, scan_job_id):
        """Look up a card-cleanup-scan job, live runner first then
        job_history, and return (scan_job, error_response). Either the
        job is completed and the caller may proceed, or ``error_response``
        is a ready-to-return :func:`json_error` for the bad states.

        Shared by the verify and delete endpoints so their
        scan-resolution rules can't drift.
        """
        scan_job = runner.get(scan_job_id)
        if scan_job is None:
            row = db.get_job_history_row(scan_job_id)
            scan_job = dict(row) if row is not None else None
        if scan_job is None or scan_job.get("type") != "card-cleanup-scan":
            return None, json_error("unknown scan job", status=404)
        if scan_job.get("status") in ("queued", "running", "pausing", "paused"):
            # Mid-flight, not a dead end: telling the user to re-scan
            # here would throw away a scan that is about to succeed.
            return None, json_error(
                "scan still running — wait for it to finish")
        if scan_job.get("status") != "completed":
            return None, json_error(
                "scan did not complete — re-scan the card")
        return scan_job, None

    def _card_cleanup_job_conflict_response(runner, scan_job_id):
        """Return a 409 :func:`json_error` when a verify or delete for
        this scan is already queued or running, else ``None``. Callers
        MUST hold ``_CARD_CLEANUP_JOB_LOCK`` across both this call and
        the following ``runner.start()`` so a second POST cannot slip in
        during the gap and register its own worker.
        """
        for j in runner.list_jobs():
            if (j.get("type") in
                    ("card-cleanup-verify", "card-cleanup-delete")
                    and j.get("status") in ("queued", "running", "pausing", "paused")
                    and (j.get("config") or {}).get("scan_job_id")
                    == scan_job_id):
                return json_error(
                    "verification or deletion for this scan is already "
                    "running", status=409)
        return None

    @blueprint.route("/api/card-cleanup/scan", methods=["POST"])
    @background_job
    def api_card_cleanup_scan(ctx):
        """Scan a removable-media source and write a manifest of files
        that are safely deletable (verified elsewhere in the archive).

        Background job: the manifest is written to disk by
        card_cleanup.scan_card and read back by the manifest/delete
        endpoints below, keyed by this job's id.
        """
        body = request.get_json(silent=True) or {}
        if not isinstance(body, dict):
            # get_json happily returns 5 or "x" for valid non-object
            # JSON; body.get would then 500.
            return json_error("request body must be a JSON object")
        source = body.get("source")
        recursive = body.get("recursive", True)
        if not isinstance(recursive, bool):
            # bool("false") is True — a stringly-typed client would get
            # the opposite of what it asked for. JSON booleans only.
            return json_error("recursive must be a boolean")
        if not source or not isinstance(source, str):
            return json_error("source required")
        if not os.path.isabs(source):
            return json_error("source must be an absolute path")
        if not os.path.isdir(source):
            return json_error("source is not an accessible directory")
        db = get_db()
        # Overlap fail-fast, across all workspaces (folders is global):
        # this tool is for removable media, not the archive. The per-file
        # guard in card_cleanup.qualify_rows is the real invariant; this
        # is the clear early error.
        #
        # Cost: O(folders) realpath calls at request time, plus (on Linux,
        # where contains_resolved probes case sensitivity) a listdir of
        # each root. Acceptable at the thousands-of-rows scale of a local
        # folders table with local paths. Revisit — cache the resolved
        # roots, or narrow the candidate set by prefix first — if folder
        # counts grow much larger or roots live on network mounts where
        # each realpath/listdir is a round trip.
        source_real = os.path.realpath(source)
        for row in db.get_all_folders():
            froot = row["path"]
            if not froot:
                continue
            froot_real = os.path.realpath(froot)
            if (path_guard.contains_resolved(source_real, froot_real)
                    or path_guard.contains_resolved(froot_real, source_real)):
                return json_error(
                    "the selected source overlaps the cataloged archive "
                    f"folder {froot!r}; this tool is for removable media "
                    "like memory cards, not archive folders")
        card_cleanup_dir = config["CARD_CLEANUP_DIR"]
        card_cleanup.prune_manifests(card_cleanup_dir)

        def work(job):
            thread_db = ctx.thread_db()
            try:

                def progress_cb(current, total, filename):
                    # Throttle SSE events; hashing is per-file fast on
                    # small files and the stream doesn't need every one.
                    if current % 10 != 0 and current not in (1, total):
                        return
                    ctx.runner.push_event(job["id"], "progress", {
                        "current": current, "total": total,
                        "current_file": filename,
                        "phase": "Verifying card files against the archive",
                    })

                manifest = card_cleanup.scan_card(
                    thread_db, source, recursive, card_cleanup_dir,
                    job["id"],
                    progress_cb=progress_cb,
                    should_cancel=lambda: ctx.runner.is_cancelled(job["id"]),
                )
                if manifest.get("cancelled"):
                    return {"cancelled": True}
                # Trimmed on purpose: entries can be a multi-MB blob (one
                # per card file) and nothing consumes the job result for
                # them — the UI fetches the manifest endpoint instead.
                # json-dumping the full manifest into job_history.result,
                # the SSE complete event, and every /api/jobs/<id> poll
                # would ship that blob for no reader.
                return {
                    "cancelled": False,
                    "totals": manifest["totals"],
                    "source_root": manifest["source_root"],
                    "manifest_path": card_cleanup.manifest_path(
                        card_cleanup_dir, job["id"]),
                    "walk_errors": len(manifest["walk_errors"]),
                }
            finally:
                thread_db.close()

        return ctx.start(
            "card-cleanup-scan", work, pausable=True,
            config={"source": source, "recursive": recursive})

    @blueprint.route("/api/card-cleanup/<scan_job_id>/manifest")
    def api_card_cleanup_manifest(scan_job_id):
        """Read back the manifest a completed scan job wrote, so the UI
        can preview what a delete would remove."""
        card_cleanup_dir = config["CARD_CLEANUP_DIR"]
        try:
            manifest = card_cleanup.load_manifest(
                card_cleanup_dir, scan_job_id)
        except card_cleanup.ManifestError as e:
            return json_error(str(e), status=e.http_status)
        return jsonify(manifest)

    @blueprint.route("/api/card-cleanup/verify", methods=["POST"])
    @background_job
    def api_card_cleanup_verify(ctx):
        """Verify only archive copies needed by a cleanup preview.

        Unlike the workspace-wide integrity audit, this reads at most one
        viable archive copy per distinct pending card hash and refreshes the
        existing manifest when it finishes.
        """
        body = request.get_json(silent=True) or {}
        if not isinstance(body, dict):
            return json_error("request body must be a JSON object")
        scan_job_id = body.get("scan_job_id")
        if not scan_job_id or not isinstance(scan_job_id, str):
            return json_error("scan_job_id required")

        db = get_db()
        _scan_job, err = _resolve_completed_card_cleanup_scan(
            db, ctx.runner, scan_job_id)
        if err is not None:
            return err

        card_cleanup_dir = config["CARD_CLEANUP_DIR"]

        # Codex P1: load the manifest INSIDE the lock, after the conflict
        # check passes. Loading it before we hold the lock captures a
        # snapshot that a concurrent verify can outdate before our worker
        # starts: if request A loads revision N, releases, then request B
        # runs to completion and writes revision N+1, A can still enter
        # the lock (B is no longer registered), pass the conflict check,
        # and hand its stale revision-N manifest to verify_manifest_
        # archives — which bumps to N+1 and writes it, clobbering B's
        # promotions with entries the user never saw promoted.
        with _CARD_CLEANUP_JOB_LOCK:
            conflict = _card_cleanup_job_conflict_response(
                ctx.runner, scan_job_id)
            if conflict is not None:
                return conflict
            try:
                manifest = card_cleanup.load_manifest(
                    card_cleanup_dir, scan_job_id)
            except card_cleanup.ManifestError as e:
                return json_error(str(e), status=e.http_status)
            if not any(
                entry.get("bucket") == "kept"
                and entry.get("reason") == card_cleanup.KEEP_NOT_VERIFIED
                and entry.get("hash")
                for entry in manifest["entries"]
            ):
                return json_error("nothing needs archive verification")

            def work(job):
                thread_db = ctx.thread_db()
                try:

                    def progress_cb(current, total, filename):
                        ctx.runner.push_event(job["id"], "progress", {
                            "current": current, "total": total,
                            "current_file": filename,
                            "phase": "Verifying matching archive copies",
                        })

                    return card_cleanup.verify_manifest_archives(
                        thread_db, manifest, card_cleanup_dir,
                        progress_cb=progress_cb,
                        should_cancel=lambda: ctx.runner.is_cancelled(job["id"]),
                    )
                finally:
                    thread_db.close()

            job_id = ctx.start_job(
                "card-cleanup-verify", work, pausable=True,
                config={"scan_job_id": scan_job_id})
        return jsonify({"job_id": job_id})

    @blueprint.route("/api/card-cleanup/delete", methods=["POST"])
    @background_job
    def api_card_cleanup_delete(ctx):
        """Delete the deletable bucket of a scan's manifest.

        Background job: re-verifies every file against the card and the
        archive immediately before each unlink (see card_cleanup.delete_
        verified). Validates the referencing scan job via the live ctx.runner
        first, falling back to job_history so a delete can still be
        requested for a scan whose job fell out of the ctx.runner's in-memory
        table (e.g. after a process restart) as long as its manifest is
        still on disk.
        """
        body = request.get_json(silent=True) or {}
        if not isinstance(body, dict):
            return json_error("request body must be a JSON object")
        scan_job_id = body.get("scan_job_id")
        if not scan_job_id or not isinstance(scan_job_id, str):
            return json_error("scan_job_id required")
        db = get_db()

        _scan_job, err = _resolve_completed_card_cleanup_scan(
            db, ctx.runner, scan_job_id)
        if err is not None:
            return err
        # Codex P1: the client must send the manifest revision it
        # confirmed. If a verify has run since that confirmation, the
        # on-disk manifest carries a newer revision and its deletable
        # set may include files the user never saw — reject rather than
        # sweep them in silently. Checked AFTER the scan-job resolution
        # so an unknown scan still surfaces as 404 rather than a body
        # complaint.
        manifest_revision = body.get("manifest_revision")
        if (not isinstance(manifest_revision, int)
                or isinstance(manifest_revision, bool)):
            return json_error("manifest_revision required")
        card_cleanup_dir = config["CARD_CLEANUP_DIR"]
        try:
            manifest = card_cleanup.load_manifest(
                card_cleanup_dir, scan_job_id)
        except card_cleanup.ManifestError as e:
            return json_error(str(e), status=e.http_status)
        loaded_revision = int(manifest.get(
            "revision", card_cleanup.INITIAL_MANIFEST_REVISION))
        if loaded_revision != manifest_revision:
            # Explicit code so the UI can distinguish this from generic
            # 409s and drop the user back into a "reload the preview
            # before deleting" state.
            return json_error(
                "the preview has changed since you confirmed it — "
                "reload the page to review the updated totals before "
                "deleting.",
                status=409,
                code="manifest_revision_mismatch",
            )
        if not any(e.get("bucket") == "deletable"
                   for e in manifest["entries"]):
            return json_error("nothing to delete — no verified files")

        def work(job):
            thread_db = ctx.thread_db()
            try:

                def progress_cb(current, total, filename):
                    # Not throttled — deletions are the events the user
                    # watches, unlike the scan's per-file hashing.
                    ctx.runner.push_event(job["id"], "progress", {
                        "current": current, "total": total,
                        "current_file": filename,
                        "phase": "Deleting verified files from the card",
                    })

                summary = card_cleanup.delete_verified(
                    thread_db, manifest,
                    progress_cb=progress_cb,
                    should_cancel=lambda: ctx.runner.is_cancelled(job["id"]),
                )
                # Bound the persisted result: skipped/failed can run to
                # thousands of per-file entries on a wholesale-drifted
                # card, and this dict lands in job_history.result and
                # every /api/jobs/<id> poll. Totals stay exact; the
                # lists carry a generous sample and the UI says when it
                # is showing a sample.
                for key in ("skipped", "failed"):
                    summary[f"{key}_total"] = len(summary[key])
                    summary[key] = summary[key][:500]
                return summary
            finally:
                thread_db.close()

        # Atomic conflict-check + start (Codex P1): pair with the same
        # lock the verify endpoint uses so a verify POST can't slip in
        # between our check and our registration and start writing a
        # new manifest under this delete.
        with _CARD_CLEANUP_JOB_LOCK:
            conflict = _card_cleanup_job_conflict_response(
                ctx.runner, scan_job_id)
            if conflict is not None:
                return conflict
            job_id = ctx.start_job(
                "card-cleanup-delete", work, pausable=True,
                config={"scan_job_id": scan_job_id})
        return jsonify({"job_id": job_id})

    return blueprint
