"""Job dicts: a fresh registration, and the copies callers get outside the lock."""

import time
from datetime import datetime

from job_runner.outcome import attach_result_text
from job_runner.pause_policy import resolve_pause_policy
from resource_ledger import get_resource_ledger


def new_job_dict(job_id, job_type, *, config, workspace_id,
                 ephemeral, counts_for_badge, pausable,
                 blocks_local_transitions, runtime_warning,
                 singleton_key=None, workspace_paths=None, lineage=None):
    """A fresh running job; ``lineage`` is ``resolve_lineage``'s fields."""
    lineage = lineage or {
        "origin": "user", "parent_job_id": None, "reason": None, "task_scope": None,
    }
    pausable, pause_unavailable_reason = resolve_pause_policy(job_type, pausable)
    return {
        "id": job_id,
        "type": job_type,
        "status": "running",
        "started_at": datetime.now().isoformat(),
        "finished_at": None,
        "progress": {"current": 0, "total": 0, "current_file": ""},
        "result": None,
        "errors": [],
        "config": config or {},
        "workspace_id": workspace_id,
        "steps": [],
        "ephemeral": ephemeral,
        "counts_for_badge": counts_for_badge,
        "pausable": bool(pausable),
        "pause_unavailable_reason": pause_unavailable_reason,
        "cancellable": True,
        "control_unavailable_reason": None,
        "blocks_local_transitions": bool(blocks_local_transitions),
        "exclusive_workspace": False,
        "workspace_transfer_batch": None,
        # The folder trees the job reads or writes, realpath'd, when it
        # declared them; None means "anywhere in the workspace". A transfer's
        # are the trees it moves; see job_runner.reservations.
        "workspace_paths": list(workspace_paths) if workspace_paths else None,
        "runtime_warning": runtime_warning,
        "singleton_key": singleton_key,
        "origin": lineage["origin"],
        "parent_job_id": lineage["parent_job_id"],
        "reason": lineage["reason"],
        "task_scope": lineage["task_scope"],
        "resource_wait_seconds": 0.0,
        "resource_wait_count": 0,
        # Pre-seeded so later writes from worker threads update an
        # existing key instead of inserting a new one: key insertion
        # while a request handler iterates the same dict (jsonify of
        # /api/jobs) raises "dictionary changed size during iteration";
        # same-key updates don't resize the dict.
        "_start_time": time.time(),
        "_ended_at": None,
        "_persisted": False,
        "_fatal_error": None,
    }


def synthesize_queued_view(job_id, ctx):
    """Render a queued pipeline's in-memory context as a job-shaped dict.

    Queued pipelines aren't in the runner's ``_jobs`` yet — they live in
    the pipeline queue until the scheduler promotes them.
    ``get()`` and ``list_jobs()`` both need to surface them in the
    same shape as a live job so callers (UI, SSE, the navbar's
    active-jobs polling) can render and cancel them uniformly.
    """
    return {
        "id": job_id,
        "type": "pipeline",
        "status": "queued",
        "started_at": ctx["started_at"],
        "finished_at": None,
        "progress": {"current": 0, "total": 0, "current_file": ""},
        "result": None,
        "errors": [],
        "config": dict(ctx["config"]),
        "workspace_id": ctx["workspace_id"],
        "steps": [],
        "ephemeral": False,
        "counts_for_badge": True,
        "pausable": False,
        "runtime_warning": ctx.get("runtime_warning"),
        **queued_lineage(ctx),
        "resource_wait_seconds": 0.0,
        "resource_wait_count": 0,
    }


def queued_lineage(ctx):
    """A queued pipeline context's lineage fields (absent: the user's own run)."""
    return {
        "origin": ctx.get("origin") or "user",
        "parent_job_id": ctx.get("parent_job_id"),
        "reason": ctx.get("reason"),
        "task_scope": ctx.get("task_scope"),
    }


def snapshot_job(job):
    """Copy a job dict for callers outside the lock.

    The top-level copy alone isn't enough: handlers jsonify the nested
    progress/steps containers while worker threads mutate them, and a
    key insertion during that iteration raises RuntimeError. Snapshot
    the nested mutable containers under the lock too.
    """
    snap = dict(job)
    # Checkpoint-only state: can be large, and callers read ``result``.
    snap.pop("partial_result", None)
    snap.pop("_checkpointed_partial", None)
    snap["progress"] = dict(job.get("progress") or {})
    snap["steps"] = [dict(s) for s in (job.get("steps") or [])]
    snap["errors"] = list(job.get("errors") or [])
    # Freeze the top-level ``result`` mapping too. Some workers mutate
    # ``job["result"]`` in place (e.g. the release-local-originals loop
    # bumps ``failed`` / ``errors_total`` without holding ``_lock``), so
    # a checkpoint that leaves ``snap["result"]`` as the live dict can
    # dump the result at one state in ``write_checkpoints`` and then
    # read ``error_total`` at a later state — the row's ``error_count``
    # column then disagrees with the ``errors_total`` the serialized
    # result reports, and the startup-sweep +1 compounds the mismatch.
    # Also freeze the nested per-photo sample lists (``errors`` and
    # ``kept_reasons``): workers that append to them during the first 50
    # failures would otherwise let ``json.dumps(snap["result"])``
    # iterate a live, mid-append list, and the serialized sample size
    # could race ahead of the top-level ``snap["errors"]`` copy taken a
    # few lines up — the two counts the checkpoint row cross-references
    # must come from one immutable snapshot.
    if isinstance(snap.get("result"), dict):
        snap["result"] = dict(snap["result"])
        for key in ("errors", "kept_reasons"):
            value = snap["result"].get(key)
            if isinstance(value, list):
                snap["result"][key] = list(value)
    if job.get("status") in ("running", "pausing", "paused"):
        timing = get_resource_ledger().owner_timing(job["id"])
        snap["resource_wait_seconds"] = timing["wait_seconds"]
        snap["resource_wait_count"] = timing["wait_count"]
    elif job.get("status") in ("completed", "failed", "cancelled"):
        attach_result_text(snap)
    return snap
