"""The background work function behind every scan job.

``build_scan_work`` is shared by ``POST /api/jobs/scan``,
``POST /api/jobs/scan-workspace``, the repair-metadata launcher and the
per-folder rescan route. It never touches ``request`` or the request
database: the worker opens its own ``Database`` and binds the job's
workspace before scanning.
"""

from __future__ import annotations

import logging
import os
import time

from db import Database

# The canonical implementation lives in ``new_images.py``; bound here as a
# module global so tests can swap it for the scan job alone.
from new_images import invalidate_new_images_after_scan as _invalidate_new_images_after_scan

log = logging.getLogger(__name__)


def _scan_metadata_warning():
    """Thin wrapper over ``metadata.scan_metadata_warning`` for the scan paths.

    The implementation lives in ``metadata`` so the pipeline-job module can
    share it.
    """
    from metadata import scan_metadata_warning
    return scan_metadata_warning()


class _ScanJobRun:
    """One scan job's run-wide state: cumulative progress, per-root failures.

    Owns the callbacks handed to ``scanner.scan`` and the step updates and
    progress events the job publishes around each phase.
    """

    def __init__(
        self, job, *, runner, thread_db, roots_list, incremental,
        repair_missing_metadata, config, invalidate_missing_originals,
        do_scan, scan_cancelled,
    ):
        self.job = job
        self.runner = runner
        self.thread_db = thread_db
        self.roots_list = roots_list
        self.incremental = incremental
        self.repair_missing_metadata = repair_missing_metadata
        self.config = config
        self.invalidate_missing_originals = invalidate_missing_originals
        self.do_scan = do_scan
        self.scan_cancelled = scan_cancelled

        # Accumulator so multi-root progress doesn't rewind at each
        # root boundary. scanner.scan() reports (current, total) local
        # to its invocation; we fold those into cumulative counters
        # that the SSE/status stream reads.
        # Track both the last reported *processed* count and the
        # last reported *total* for the current root. On root
        # boundary we advance the cumulative baseline by the
        # processed count (not the planned total) so a root that
        # fails mid-scan doesn't inflate the baseline with phantom
        # files the next root would start above.
        self.scan_acc = {"prior": 0, "last_current": 0, "last_total": 0}
        # Photos that actually reached the catalog, summed across roots
        # from each scan()'s counts sink. Kept separate from the
        # progress accumulator above: progress counts every file the
        # scan disposes of (including ones skipped because they
        # vanished under a dropped mount), which is what a progress bar
        # needs but overstates the result line.
        self.indexed = 0

        # Per-root failures are caught and recorded rather than
        # re-raised so a failure on root A doesn't prevent root B
        # from scanning. Any failure flips the job to "failed" at
        # the end (mixed-outcome rollup).
        #
        # Track roots by failure class so the rollup below can
        # distinguish "this root's scan raised" (no photos indexed
        # — thumbnails can skip) from "this root's scan succeeded
        # but cache invalidation raised" (photos DID get indexed —
        # thumbnails must still run). Using len(root_errors) alone
        # double-counts roots that hit both failure classes and
        # misclassifies cache-only failures as scan failures.
        self.root_errors = []
        self.scan_failed_roots = set()
        self.cache_failed_roots = set()

    def progress_cb(self, current, total):
        job = self.job
        scan_acc = self.scan_acc
        scan_acc["last_current"] = current
        scan_acc["last_total"] = total
        cum_current = scan_acc["prior"] + current
        cum_total = scan_acc["prior"] + total
        job["progress"]["current"] = cum_current
        job["progress"]["total"] = cum_total
        self.runner.update_step(
            job["id"], "scan",
            progress={"current": cum_current, "total": cum_total},
        )
        self.runner.push_event(
            job["id"],
            "progress",
            {
                "current": cum_current,
                "total": cum_total,
                "current_file": job["progress"].get("current_file", ""),
                "rate": round(
                    cum_current / max(time.time() - job["_start_time"], 0.01), 1
                ),
                "phase": "Scanning photos",
            },
        )

    def advance_scan_acc(self):
        # Use processed count, not planned total — a root that
        # raised mid-scan will have last_current < last_total,
        # and starting the next root above the actual processed
        # count would overreport photos indexed.
        self.scan_acc["prior"] += self.scan_acc["last_current"]
        self.scan_acc["last_current"] = 0
        self.scan_acc["last_total"] = 0

    def status_cb(self, message, phase_current=None, phase_total=None, phase_label=None):
        job = self.job
        progress_payload = {
            "phase": phase_label or message,
            "current": job["progress"].get("current", 0),
            "total": job["progress"].get("total", 0),
            "current_file": message,
            "rate": 0,
            "phase_current": phase_current,
            "phase_total": phase_total,
            "phase_label": phase_label,
        }
        self.runner.update_step(job["id"], "scan", current_file=message)
        self.runner.push_event(job["id"], "progress", progress_payload)

    def cancel_check(self):
        return self.runner.is_cancelled(self.job["id"])

    def pause_check(self):
        return self.runner.pause_requested(self.job["id"])

    def cancel_only_check(self):
        return self.runner.cancellation_requested(self.job["id"])

    def begin(self, cfg):
        """Declare the job's steps and read the scan settings."""
        job = self.job
        job["_start_time"] = time.time()
        self.runner.set_steps(job["id"], [
            {"id": "scan", "label": "Scan photos"},
            {"id": "thumbnails", "label": "Generate thumbnails"},
        ])
        self.runner.update_step(job["id"], "scan", status="running")
        effective_cfg = self.thread_db.get_effective_config(cfg.load())
        self.pipeline_cfg = effective_cfg.get("pipeline", {})
        self.vireo_dir = os.path.dirname(self.config["THUMB_CACHE_DIR"])

    def announce_root(self, idx, root):
        job = self.job
        phase = (
            f"Scanning root {idx} of {len(self.roots_list)}: {root}"
            if len(self.roots_list) > 1
            else "Scanning photos"
        )
        self.runner.push_event(job["id"], "progress", {
            "phase": phase,
            "current": job["progress"].get("current", 0),
            "total": job["progress"].get("total", 0),
            "current_file": phase,
            "rate": 0,
        })

    def scan_root(self, root):
        """Scan one root, recording any failure; return True when cancelled."""
        job = self.job
        # Counts sink rather than the return value: scan() commits
        # rows as it goes and can raise (or be cancelled) after
        # thousands have landed. Reading the sink in `finally`
        # credits that work on every exit path — a root that dies
        # late would otherwise report zero photos indexed.
        root_counts = {}
        try:
            self.do_scan(
                root, self.thread_db,
                progress_callback=self.progress_cb,
                incremental=self.incremental,
                extract_full_metadata=self.pipeline_cfg.get("extract_full_metadata", True),
                status_callback=self.status_cb,
                vireo_dir=self.vireo_dir,
                thumb_cache_dir=self.config["THUMB_CACHE_DIR"],
                cancel_check=self.cancel_check,
                pause_check=self.pause_check,
                cancel_only_check=self.cancel_only_check,
                repair_missing_metadata=self.repair_missing_metadata,
                counts=root_counts,
            )
        except Exception as exc:
            if isinstance(exc, self.scan_cancelled) and self.cancel_check():
                log.info("Scan job %s cancelled during root %s", job["id"], root)
                return True
            log.exception("Scan failed for root %s", root)
            self.scan_failed_roots.add(root)
            msg = f"[{root}] {exc}"
            self.root_errors.append(msg)
            if msg not in job["errors"]:
                job["errors"].append(msg)
        finally:
            self._finish_root(root, root_counts)
        return False

    def _finish_root(self, root, root_counts):
        """Credit the root's photos and drop the caches its scan made stale."""
        job = self.job
        # Credit whatever this root indexed regardless of how
        # it exited — completed, raised, or cancelled. The
        # rows are committed either way, so the summary must
        # count them either way.
        self.indexed += root_counts.get("indexed", 0)
        # scanner.scan commits photo rows incrementally, so
        # even a mid-scan failure can leave DB state that
        # invalidates cached new-image counts. A failure
        # here must surface: the shared cache has a 5-min
        # TTL, so users would see stale "new images" counts
        # with no job-level failure signal if we swallowed
        # these errors. Keep the try/except so we still
        # advance scan_acc and try the remaining roots,
        # but record the failure into root_errors so the
        # job is flagged failed at the rollup below.
        try:
            _invalidate_new_images_after_scan(self.thread_db, root)
        except Exception as cache_exc:
            log.exception(
                "Failed to invalidate new-image cache for %s", root,
            )
            self.cache_failed_roots.add(root)
            cache_msg = (
                f"[{root}] cache invalidation failed "
                f"after scan: {cache_exc}"
            )
            self.root_errors.append(cache_msg)
            if cache_msg not in job["errors"]:
                job["errors"].append(cache_msg)
        # scanner.scan touches disk and may add or remove
        # photo rows; a ready Missing Originals payload
        # computed before the scan can now be stale (e.g.
        # user restored an original before running "Rescan
        # this Folder"). The pre-scan health-check
        # invalidation only fires when a folder flips
        # missing/ok, so also drop the cache once the scan
        # itself has run — even on partial failure, since
        # rows are committed incrementally.
        try:
            self.invalidate_missing_originals()
        except Exception:
            log.exception(
                "Failed to invalidate missing-originals cache for %s",
                root,
            )
        self.advance_scan_acc()

    def finish_cancelled(self):
        # Same indexed count as the completed path, not the
        # progress counter. Otherwise "N photos" would mean two
        # different things depending on which branch produced it,
        # and a cancelled scan of a dropped share would report a
        # full house of photos it never cataloged.
        photo_count = self.indexed
        self.runner.update_step(
            self.job["id"], "scan", status="cancelled",
            summary=f"{photo_count} photos (cancelled)",
        )
        self.runner.update_step(
            self.job["id"], "thumbnails", status="skipped",
            summary="skipped (cancelled)",
        )
        return {"photos_indexed": photo_count, "cancelled": True}

    def record_scan_step(self, photo_count):
        """Close the scan step as completed or failed, with its summary."""
        # Unique roots that hit any failure class. Counting unique
        # roots (not error entries) avoids inflating the "N of M"
        # summary when a single root raises in both scan and cache
        # invalidation.
        failed_root_count = len(self.scan_failed_roots | self.cache_failed_roots)
        metadata_warning = _scan_metadata_warning()
        if self.root_errors:
            scan_summary = (
                f"{photo_count} photos ({failed_root_count} of "
                f"{len(self.roots_list)} root"
                f"{'s' if len(self.roots_list) != 1 else ''} failed)"
            )
            if metadata_warning:
                scan_summary += f" — {metadata_warning}"
            self.runner.update_step(
                self.job["id"], "scan", status="failed", summary=scan_summary,
                error=self.root_errors[0], error_count=len(self.root_errors),
            )
        else:
            scan_summary = f"{photo_count} photos"
            if metadata_warning:
                scan_summary += f" — {metadata_warning}"
            self.runner.update_step(
                self.job["id"], "scan", status="completed",
                summary=scan_summary,
            )

    def skip_thumbnails(self):
        log.info(
            "All %d scan root(s) failed; skipping thumbnail phase",
            len(self.roots_list),
        )
        self.runner.update_step(
            self.job["id"], "thumbnails", status="skipped",
            summary="skipped (all scan roots failed)",
        )

    def generate_thumbnails(self):
        job = self.job
        self.runner.update_step(job["id"], "thumbnails", status="running")

        # Auto-generate thumbnails for new photos only
        from thumbnails import generate_all

        log.info("Generating thumbnails...")
        self.runner.push_event(
            job["id"],
            "progress",
            {
                "current": 0,
                "total": 0,
                "current_file": "Checking for new thumbnails...",
                "rate": 0,
                "phase": "Generating thumbnails",
            },
        )

        thumb_result = generate_all(
            self.thread_db, self.config["THUMB_CACHE_DIR"],
            progress_callback=self.thumb_cb, vireo_dir=self.vireo_dir,
        )
        from thumbnails import format_summary as thumb_summary
        self.runner.update_step(job["id"], "thumbnails", status="completed",
                                summary=thumb_summary(thumb_result))
        return thumb_result

    def thumb_cb(self, current, total):
        job = self.job
        job["progress"]["current"] = current
        job["progress"]["total"] = total
        self.runner.push_event(
            job["id"],
            "progress",
            {
                "current": current,
                "total": total,
                "current_file": "",
                "rate": round(
                    current / max(time.time() - job["_start_time"], 0.01), 1
                ),
                "phase": "Generating thumbnails",
            },
        )


def build_scan_work(
    roots, incremental, active_ws, repair_missing_metadata=False, *,
    get_runner, db_path, config, invalidate_missing_originals,
):
    """Build the background work function for a scan job.

    Shared by ``POST /api/jobs/scan`` and
    ``POST /api/folders/<id>/rescan`` so per-folder rescans reuse the
    same scan + thumbnail pipeline as a full scan.

    ``roots`` may be a single path string (back-compat, one root) or a
    list of paths. When multiple roots are given they are scanned
    **serially** inside this single job -- that's the whole point of
    this wrapper: parallel scan jobs used to fight for the SQLite
    writer lock, so we now process roots one after another. A failure
    on one root does not abort the others; the error is recorded and
    the job ends in ``"failed"`` (mixed-outcome rollup convention).

    ``get_runner`` is called once, when the work function is built;
    ``config`` is the Flask app's config mapping, read when the job runs;
    ``invalidate_missing_originals`` drops the app's Missing Originals
    cache (owned by ``create_app``).
    """
    import config as cfg

    runner = get_runner()

    # Back-compat: accept a bare string in addition to a list.
    if isinstance(roots, str):
        roots_list = [roots]
    else:
        roots_list = list(roots)

    def work(job):
        from scanner import ScanCancelled
        from scanner import scan as do_scan

        thread_db = Database(db_path)
        thread_db.set_active_workspace(active_ws)
        # Check folder health before scanning to prevent duplicate imports
        if thread_db.check_folder_health():
            invalidate_missing_originals()

        run = _ScanJobRun(
            job, runner=runner, thread_db=thread_db, roots_list=roots_list,
            incremental=incremental,
            repair_missing_metadata=repair_missing_metadata, config=config,
            invalidate_missing_originals=invalidate_missing_originals,
            do_scan=do_scan, scan_cancelled=ScanCancelled,
        )
        run.begin(cfg)

        cancelled = False
        for idx, root in enumerate(roots_list, 1):
            if run.cancel_check():
                cancelled = True
                break
            run.announce_root(idx, root)
            if run.scan_root(root):
                cancelled = True
                break

        if cancelled or run.cancel_check():
            return run.finish_cancelled()

        # Count what the scans reported as indexed, not the progress
        # counter. On a clean run they agree; they diverge exactly when
        # the run went wrong — progress advances for files that were
        # discovered and then skipped (vanished under a mount that
        # dropped mid-scan), so "N photos" would read as a success
        # line for a scan that cataloged nothing.
        photo_count = run.indexed
        run.record_scan_step(photo_count)
        # Skip the thumbnail phase when EVERY requested root's scan
        # raised. generate_all() walks the whole library looking
        # for missing thumbnails — running it after a total scan
        # failure does a long, unrelated pass and delays the
        # failure feedback the user actually needs. When at least
        # one root's scan succeeded we still run thumbs so those
        # newly-indexed photos get covered. Cache-invalidation
        # failures do NOT gate this decision: the scan for that
        # root did produce indexed photos that need thumbnails.
        all_roots_failed = (
            bool(roots_list) and len(run.scan_failed_roots) == len(roots_list)
        )

        if all_roots_failed:
            run.skip_thumbnails()
            thumb_result = None
        else:
            thumb_result = run.generate_thumbnails()

        # Mixed-outcome rollup: any failed root => job is "failed".
        # JobRunner._run_job dedupes job["errors"] by exact string
        # match. Raise the first per-root message (already recorded
        # above) so no extra aggregate entry is appended — that
        # would inflate error_count in job/history output. The
        # "N of M roots failed" context is already visible via the
        # scan step's summary and error_count set above.
        if run.root_errors:
            raise RuntimeError(run.root_errors[0])

        return {"photos_indexed": photo_count, "thumbnails": thumb_result}

    return work
