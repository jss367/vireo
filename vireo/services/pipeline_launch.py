"""Launching pipeline runs: request-time helpers and the after-import chain.

``apply_no_model_auto_skip`` and ``resolve_remote_archive_target`` are the
checks the pipeline and import routes share so a chained run degrades the
same way a manually-started one does. ``PipelineChain`` is the job-thread
side: it enqueues the process run an import asked for and, when that run
ends, the NAS moves chained off it. It never touches ``request`` or the
request database — every dependency is passed in explicitly.
"""

from __future__ import annotations

import logging
import os
import threading
from typing import TYPE_CHECKING, Protocol

from db import Database
from runtime_warnings import build_cpu_runtime_warning, runtime_warning_work_units
from services import saved_processes

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from jobs import JobRunner
    from pipeline_stages.context import PipelineParams
    from services.service_types import JobDict, JobResult, JsonDict

log = logging.getLogger(__name__)


class ErrorBuilder[E](Protocol):
    """Builds the caller's error answer from a message and a status.

    The routes pass ``web.responses.json_error``; the import service passes
    its ``ImportFailure`` dataclass, so the answer's type follows the caller.
    """

    def __call__(self, message: str, /, status: int = ...) -> E: ...


def apply_no_model_auto_skip(params: PipelineParams) -> str | None:
    """Auto-skip classify stages when no model is available.

    Mutates ``params`` (skip_classify/skip_extract_masks/skip_regroup)
    and returns the user-facing warning string, or None when a model
    resolved. One implementation shared by ``api_job_pipeline`` and the
    after-import chaining hook, so chained runs degrade identically to
    manually-started ones.
    """
    if params.skip_classify:
        return None
    from models import get_active_model, get_models

    # Resolve the set of requested models. Prefer the explicit list,
    # fall back to the legacy single id, and finally to whatever is
    # marked active. Any requested id that isn't downloaded fails the
    # check, so the user sees the "no model available" warning instead
    # of a mid-run model_loader crash.
    requested_ids = list(params.model_ids or [])
    if not requested_ids and params.model_id:
        requested_ids = [params.model_id]

    if requested_ids:
        by_id = {m["id"]: m for m in get_models()}
        resolved_any = all(
            by_id.get(mid, {}).get("downloaded") for mid in requested_ids
        )
    else:
        resolved_any = get_active_model() is not None

    if resolved_any:
        return None
    params.skip_classify = True
    params.skip_extract_masks = True
    params.skip_regroup = True
    return (
        "No model available — classification was skipped. "
        "Download a model in Settings to enable species identification."
    )

def resolve_remote_archive_target[E](
    db: Database, remote_target_id: str, remote_subpath: str, *, json_error: ErrorBuilder[E],
) -> tuple[JsonDict, str, None] | tuple[None, None, E]:
    """Shared remote-archive-target resolution for the import-photos
    and pipeline routes.

    Returns ``(remote_archive_config, rsync_bin, None)`` on success,
    or ``(None, None, error_response)`` when the request can't be
    served (unknown target, invalid subpath, GNU rsync unavailable).
    Callers still own their own mutual-exclusivity /
    local_processing checks — this helper handles only the shared
    target-lookup + resolve_remote_archive + rsync-availability
    block that was duplicated across the two endpoints. See PR
    #1113 review.
    """
    import config as cfg
    import move as move_mod
    from pipeline_job import resolve_remote_archive

    target = cfg.get_remote_target(remote_target_id)
    if not target:
        return None, None, json_error("Remote target not found", status=404)
    try:
        remote_archive_config = resolve_remote_archive(
            target, remote_subpath,
        )
    except ValueError as exc:
        return None, None, json_error(str(exc))
    effective_cfg = db.get_effective_config(cfg.load())
    rsync_bin = move_mod.resolve_rsync_bin(
        effective_cfg.get("rsync_bin", "") or "")
    # An explicit rsync_bin path in Settings → Paths bypasses the
    # macOS auto-select guard in resolve_rsync_bin (it just checks
    # executability), so Apple's /usr/bin/rsync can still land here
    # even though it can't drive rsync-over-SSH. Fail fast before
    # enqueue instead of starting a job that dies mid-transfer, the
    # same way the remote-target list/test routes do.
    if rsync_bin and not move_mod.is_gnu_rsync(rsync_bin):
        rsync_bin = None
    if not rsync_bin:
        return None, None, json_error(
            "No usable GNU rsync was found for remote archiving. Install "
            "GNU rsync for your platform or set its executable under "
            "Settings → Paths."
        )
    ssh_bin = move_mod.resolve_ssh_bin(effective_cfg.get("ssh_bin", "") or "")
    if not ssh_bin:
        return None, None, json_error(
            "OpenSSH Client was not found. On Windows, install the OpenSSH "
            "Client optional feature or set ssh.exe under Settings → Paths."
        )
    remote_archive_config["target"] = dict(remote_archive_config["target"])
    remote_archive_config["target"]["ssh_bin"] = ssh_bin
    return remote_archive_config, rsync_bin, None


def _record_after_move_skip(runner: JobRunner, job: JobDict, result: JobResult, skip: str) -> None:
    """Record why a chained process run's NAS moves were not enqueued."""
    if isinstance(result, dict):
        result["after_move_skipped"] = skip
    if skip != "process cancelled":
        # Stepped pipeline jobs render ONLY the step tree, so a
        # skip recorded just on the result would be invisible —
        # and a skip_note means photos the user expected on the
        # NAS stay local. Surface it as a warning step.
        runner.append_step(
            job["id"], "after-move", "Move to NAS",
            summary="Skipped",
            error=skip, error_count=1,
        )
    log.info("after-process move skipped: %s", skip)


def _record_after_move_outcome(
    runner: JobRunner, job: JobDict, result: JobResult, after_move: JsonDict,
    move_ids: list[str], failures: list[str],
) -> None:
    """Record the chained moves' handoff on the result, job and step tree."""
    if isinstance(result, dict):
        if move_ids:
            result["move_job_ids"] = move_ids
        if after_move.get("skip_note"):
            # Partial skip: some folders moved but others (e.g.
            # photos cataloged on the archive root) stayed local.
            result["after_move_note"] = after_move["skip_note"]
        if failures:
            result["after_move_errors"] = failures
    elif failures:
        # run_pipeline_job raised, so there is no result dict to
        # carry after_move_errors — fold the failures into the
        # job's own error tally so they reach the persisted
        # history and jobs panel instead of dying in the log.
        # Safe: same thread, before _run_job's except handler
        # takes the lock, and that handler dedups before
        # appending.
        job["errors"].extend(failures)
    # Stepped pipeline jobs render ONLY the step tree, so the
    # handoff outcome must live there too: a completed process job
    # whose chained move never started (missing rsync/ssh, folder
    # guard, bad mount) would otherwise look clean while the photos
    # silently stay in the local archive.
    target_name = (after_move.get("target") or {}).get("name") or "NAS"
    if failures and not move_ids:
        runner.append_step(
            job["id"], "after-move", "Move to NAS",
            status="failed",
            summary="Failed to start the chained move",
            error="; ".join(failures), error_count=len(failures),
        )
    else:
        n = len(move_ids)
        warn_bits = list(failures)
        if after_move.get("skip_note"):
            warn_bits.append(after_move["skip_note"])
        runner.append_step(
            job["id"], "after-move", "Move to NAS",
            summary=(
                f"{n} move job{'s' if n != 1 else ''} started → "
                f"{target_name}"
            ),
            error="; ".join(warn_bits) or None,
            error_count=len(warn_bits),
        )


def _process_pipeline_params(collection_id: int, expanded: JsonDict) -> PipelineParams:
    """Build the collection-scoped ``PipelineParams`` for a saved process."""
    from pipeline_job import PipelineParams

    return PipelineParams(
        collection_id=collection_id,
        skip_classify=expanded["skip_classify"],
        skip_extract_masks=expanded["skip_extract_masks"],
        skip_eye_keypoints=expanded["skip_eye_keypoints"],
        skip_regroup=expanded["skip_regroup"],
        miss_enabled=expanded["miss_enabled"],
        review_mode=expanded["review_mode"],
        # A saved process's Eye Keypoints toggle is an explicit opt-in
        # (the Process-page checkbox sends eye_detect_override alongside
        # it). When the process runs the eye stage, opt into eye scoring
        # too so a chained/by-id run matches running the same toggles on
        # the page, instead of silently deferring to the workspace's
        # eye_detect_enabled default and skipping eye detection.
        eye_detect_override=(
            None if expanded["skip_eye_keypoints"] else True
        ),
    )


def _missing_labels_blocker(thread_db: Database, params: PipelineParams, process_id: int) -> str | None:
    """Return the pause message when Classify has no species list, else None."""
    if not params.skip_classify:
        from classify_job import _load_labels
        from models import get_active_model

        model = get_active_model()
        if model is not None:
            try:
                _load_labels(
                    model.get("model_type", "bioclip"),
                    model.get("model_str", ""),
                    None,
                    None,
                    db=thread_db,
                    model_dir=model.get("weights_path"),
                )
            except RuntimeError:
                saved_process = thread_db.processes.get(process_id)
                process_name = (
                    saved_process["name"] if saved_process
                    else "the selected process"
                )
                return (
                    "paused — Classify needs a species list. Download one "
                    "in Settings › Labels, then run "
                    f"{process_name} on this import collection."
                )
    return None


def _process_job_config(
    thread_db: Database, collection_id: int, process_id: int, params: PipelineParams,
    chained_from: str | None,
) -> JsonDict:
    """Build the jobs-panel config for a chained/by-id process run."""
    job_config = {
        "source": None,
        "sources": None,
        "collection_id": collection_id,
        "collection_name": next(
            (
                c["name"]
                for c in thread_db.collections.list_all()
                if c["id"] == collection_id
            ),
            None,
        ),
        "destination": None,
        "local_processing": False,
        "process_id": process_id,
        "skip_classify": params.skip_classify,
        "skip_extract_masks": params.skip_extract_masks,
        "skip_regroup": params.skip_regroup,
        "review_mode": params.review_mode,
        "miss_enabled": params.miss_enabled,
        "eye_detect_override": params.eye_detect_override,
    }
    if chained_from:
        # Provenance for the jobs panel: this run was started by an
        # import's after-import choice, not by hand.
        job_config["chained_from"] = chained_from
    return job_config


class PipelineChain:
    """After-import processing and after-process NAS moves.

    ``get_runner`` is called lazily (tests swap ``app._job_runner``);
    ``config`` is the Flask app's config mapping, read when a job runs;
    ``invalidate_missing_originals`` and ``enqueue_move_folder_job`` are
    the ``create_app`` closures this service still hands off to.
    """

    def __init__(
        self, *, get_runner: Callable[[], JobRunner], db_path: str, config: Mapping[str, object],
        invalidate_missing_originals: Callable[[], None],
        # ``FolderMoves.enqueue_job``, which returns the move job's id. Its
        # signature is typed in ``services.folder_moves``; ``...`` here keeps
        # this hook independent of that module's keyword arguments.
        enqueue_move_folder_job: Callable[..., str],
    ) -> None:
        self._get_runner = get_runner
        self._db_path = db_path
        self._config = config
        self._invalidate_missing_originals = invalidate_missing_originals
        self._enqueue_move_folder_job = enqueue_move_folder_job

    def chain_after_move(self, job: JobDict, result: JobResult, after_move: JsonDict, workspace_id: int) -> None:
        """Enqueue the chained NAS moves when a chained process run ends.

        Decision table (spec §4): fires on success AND failure; skips only on
        an explicit cancel. ``result`` is None when run_pipeline_job raised.
        """
        try:
            runner = self._get_runner()
            skip = None
            # Best-effort by design: a cancel landing in the microsecond
            # window between this check and the runner's locked terminal-
            # status decision still lets the moves enqueue.
            if runner.is_cancelled(job["id"]) or (
                    isinstance(result, dict) and result.get("cancelled")):
                skip = "process cancelled"
            elif not after_move.get("folders"):
                # skip_note carries the real reason when the import found
                # folders but every one was unmovable (photos cataloged on
                # the archive root itself) — "no folders to move" alone
                # would hide that those photos silently stay local.
                skip = after_move.get("skip_note") or "no folders to move"
            if skip:
                _record_after_move_skip(runner, job, result, skip)
                return
            thread_db = Database(self._db_path)
            thread_db.set_active_workspace(workspace_id)
            move_ids, failures = self._enqueue_after_moves(
                thread_db, runner, job, after_move, workspace_id,
            )
            _record_after_move_outcome(
                runner, job, result, after_move, move_ids, failures,
            )
        except Exception as e:
            # This hook runs in the process job's ``finally`` — raising
            # here would mask a run_pipeline_job failure with a chaining
            # bug, so log and swallow.
            log.exception("after-process move chaining failed")
            self._record_chaining_failure(job, result, e)

    def _enqueue_after_moves(
        self, thread_db: Database, runner: JobRunner, job: JobDict, after_move: JsonDict, workspace_id: int,
    ) -> tuple[list[str], list[str]]:
        """Enqueue one move-folder job per chained folder.

        Returns ``(move_ids, failures)``; a folder whose enqueue raises is
        logged and reported in ``failures`` instead of stopping the rest.
        """
        # One shared lock per batch: the move jobs all enqueue now (so
        # move_job_ids is complete immediately) but their transfers run
        # one at a time — see the why-comment in _start_move_folder_job.
        serialize_lock = threading.Lock()
        move_ids: list[str] = []
        failures: list[str] = []
        for entry in after_move["folders"]:
            try:
                move_ids.append(self._enqueue_move_folder_job(
                    thread_db, runner, workspace_id,
                    folder_id=entry["folder_id"],
                    subpath=entry["subpath"],
                    target=after_move["target"],
                    chained_from=job["id"],
                    serialize_lock=serialize_lock,
                ))
            except Exception as e:
                log.exception(
                    "after-process move enqueue failed for folder %s",
                    entry["folder_id"])
                failures.append(f"{entry['subpath']}: {e}")
        return move_ids, failures

    def _record_chaining_failure(self, job: JobDict, result: JobResult, e: Exception) -> None:
        """Surface a failure of the chaining machinery itself."""
        msg = f"after-process move chaining failed: {e}"
        # Same visibility rule as above: the step tree is the only
        # surface a stepped job shows, so record the machinery
        # failure there too. Best-effort — never let step plumbing
        # mask the original failure being handled here.
        try:
            self._get_runner().append_step(
                job["id"], "after-move", "Move to NAS",
                status="failed", summary="Chaining failed",
                error=msg, error_count=1,
            )
        except Exception:
            log.warning("Could not record the chaining failure step", exc_info=True)
        if isinstance(result, dict):
            # The pipeline succeeded but the chain machinery itself
            # failed (e.g. creating the thread Database) — surface it
            # on the result so the planned move doesn't just silently
            # never happen.
            result.setdefault("after_move_errors", []).append(msg)
        elif msg not in job["errors"]:
            # Raise path: no result dict to surface the failure on, so
            # record it on the job itself (see the dedup note above).
            job["errors"].append(msg)

    def enqueue_process_job(
        self, thread_db: Database, runner: JobRunner, workspace_id: int, *,
        collection_id: int, process_id: int,
        chained_from: str | None = None, expanded: JsonDict | None = None,
        after_move: JsonDict | None = None,
    ) -> tuple[str | None, str | None, str | None]:
        """Enqueue a collection-scoped process run for a saved-process id.

        The after-import chaining hook's path into the pipeline. Runs on a
        job thread — no request context: takes an explicit thread-local
        ``Database`` and never touches ``request``/``_get_db``. Applies the
        same process expansion and no-model auto-skip as ``api_job_pipeline``
        so a chained run degrades identically to a manual one. Returns
        ``(job_id, model_warning, process_blocker)``. A non-null blocker
        means prerequisites are missing and no doomed job was enqueued.

        ``expanded`` is the pre-resolved flag snapshot the import endpoint
        captured at enqueue-request time. Pass it through so a saved-process
        edit or delete while the import copies photos (a card copy can take
        many minutes) can't silently swap in different toggles — or fail
        the chain outright — after the user has already accepted the choice.
        Falls back to a live resolve for callers that don't pre-capture.

        ``after_move`` — ``{"target": dict, "folders": [{"folder_id",
        "subpath"}], "skip_note": str (optional)}`` — chains NAS moves off
        this run's completion via ``_chain_after_move``. The target dict is
        the import-enqueue-time snapshot (never re-resolved from Settings);
        ``skip_note`` explains folders the import cataloged but the move
        must leave local (photos on the archive root itself).
        """
        from pipeline_job import run_pipeline_job

        if expanded is None:
            expanded = saved_processes.resolve_process(thread_db, process_id)
        params = _process_pipeline_params(collection_id, expanded)
        model_warning = apply_no_model_auto_skip(params)

        # Chained imports bypass the Process page's missing-label Start gate.
        # Resolve the active label mode with the pipeline's own loader before
        # reserving a queue slot, so missing regional labels pause cleanly
        # instead of producing a guaranteed model_loader failure.
        blocker = _missing_labels_blocker(thread_db, params, process_id)
        if blocker is not None:
            return None, model_warning, blocker

        work_units = runtime_warning_work_units(
            "pipeline collection",
            lambda: thread_db.collections.count_photos(collection_id),
        )
        runtime_warning = None
        if not (params.skip_classify and params.skip_extract_masks):
            runtime_warning = build_cpu_runtime_warning(
                "pipeline",
                work_units=work_units,
                reason="large_pipeline_ml_job_cpu_only",
            )

        def work(job: JobDict) -> JobResult:
            result = None
            try:
                result = run_pipeline_job(
                    job, runner, self._db_path, workspace_id, params,
                    thumb_cache_dir=self._config["THUMB_CACHE_DIR"],
                    missing_originals_invalidator=self._invalidate_missing_originals,
                    computation_cache_dir=self._config["COMPUTATION_CACHE_DIR"],
                )
                return result
            finally:
                # The move must fire even when processing FAILS (a processing
                # failure must not strand photos off the NAS) — hence finally,
                # not a post-return call. Explicit cancel is the one thing
                # that stops the chain; _chain_after_move checks it.
                if after_move:
                    self.chain_after_move(job, result, after_move, workspace_id)

        job_config = _process_job_config(
            thread_db, collection_id, process_id, params, chained_from,
        )
        # The folders of every photo the run processes: an import admitted
        # beside a NAS transfer can process its own photos while the
        # transfer moves other folders.
        workspace_paths = [
            os.path.realpath(path)
            for path in thread_db.collections.folder_paths(collection_id)
        ]
        job_id = runner.enqueue_pipeline(
            work,
            config=job_config,
            workspace_id=workspace_id,
            runtime_warning=runtime_warning,
            workspace_paths=workspace_paths,
        )
        return job_id, model_warning, None
