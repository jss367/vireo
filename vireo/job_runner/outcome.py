"""What a finished job reports: its final status, error count and summary.

Pure functions over a job dict. ``apply_result_verdict`` is the rollup rule
for a work function that returned normally; the others derive the error
count and the one-line summary that the history row and the Jobs page show.
"""

import json

from job_summaries import describe_result, error_text


def apply_result_verdict(job, result):
    """Settle the status of a job whose work function returned normally.

    Must be called with the runner's ``_pause_condition`` held.
    """
    job["status"] = "completed"
    # A work function can return normally yet still have
    # failed (e.g. move-folder returns {"moved": 0, "errors":
    # [...]} when rsync times out). When the result opts into
    # the convention by carrying an "ok" key, honor it: fold
    # its errors into the job's tally so error_count is
    # accurate, and demote a falsy "ok" to "failed" so the
    # history doesn't read "completed, 0 errors" for a run
    # that accomplished nothing. Only a boolean is a verdict:
    # verify-models uses "ok" for the list of models that
    # passed.
    has_verdict = isinstance(result, dict) and isinstance(result.get("ok"), bool)
    if has_verdict:
        for err in (result.get("errors") or []):
            # Structured errors ({"photo_id", "error"})
            # read as "<file>: <reason>", not a dict repr.
            err_str = error_text(err)
            if err_str not in job["errors"]:
                job["errors"].append(err_str)
        if result["ok"] is False:
            job["status"] = "failed"
    # Rollup rule: a run that recorded any error failed,
    # even though its loop finished. The result keeps the
    # per-item counts, so "412 imported, 3 failed" is
    # still shown; only the status stops reading as a
    # clean success. An explicit "ok" is the job's own
    # verdict and wins: an import lists card-safety
    # notices ("1 file you deselected was not copied") in
    # its errors without anything having failed.
    if job["errors"] and job["status"] == "completed" and not has_verdict:
        job["status"] = "failed"
    # It ran to the end, so its summary leads with what it
    # did; only a crash leads with the error.
    if job["status"] == "failed":
        job["_partial_failure"] = True


def error_total(job):
    """How many errors a finished job recorded.

    A result dict may report ``errors_total`` when its ``errors`` list
    is capped (e.g. the importer caps its per-file failures at 50). The
    capped list is what the runner folded into ``job["errors"]``, so
    ``len(job["errors"])`` under-reports the real failure count; prefer
    ``errors_total`` when it is larger so the persisted ``error_count``
    and the Jobs-page badge reflect the true total.
    """
    errors = job.get("errors") or []
    result = job.get("result")
    if job.get("type") == "pipeline" and isinstance(result, dict):
        notes = result.get("notes") or []
        errors = [error for error in errors if error not in notes]
    count = len(errors)
    if isinstance(job.get("result"), dict):
        try:
            total = int(job["result"].get("errors_total") or 0)
        except (TypeError, ValueError):
            total = 0
        count = max(count, total)
    return count


def build_summary(job, result=None):
    """Build a one-line summary from job steps or result.

    ``result`` is the payload being persisted (which for a failed job
    already carries the fatal ``error``); it defaults to the job's own
    result so failed runs summarize with their error instead of a bare
    "<Type> failed".
    """
    steps = job.get("steps", [])
    if steps:
        parts = []
        for s in steps:
            if s.get("summary"):
                parts.append(s["summary"])
        if parts:
            return ", ".join(parts)

    if result is None:
        result = job.get("result")
    if result and isinstance(result, dict):
        described = describe_result(job["type"], result, job.get("config"))
        if described["summary"]:
            return described["summary"]

    # Final fallback: title-case the job type (e.g. "duplicate-scan" →
    # "Duplicate Scan") so the summary line is presentable to the user.
    pretty_type = " ".join(
        "NAS" if w.lower() == "nas" else w.capitalize()
        for w in job["type"].replace("_", " ").replace("-", " ").split()
    )
    return f"{pretty_type} {job['status']}"


def attach_result_text(job):
    """Add ``summary`` / ``result_details`` prose derived from ``result``.

    The Jobs page shows these instead of the raw result dict, so a
    finished job reads as "28 photos removed from Vireo, 28 files
    moved to Trash" rather than ``{"deleted": 28, "trashed": 28, ...}``.

    A stored ``summary`` is kept only when it came from step summaries
    or from the job itself (``result["summary"]``). Anything else was
    generated from the result dict, and history rows written before
    the prose describer exist still carry the old ``ok: True,
    deleted: 28`` form, so those are recomputed from the result here
    instead of requiring a backfill.
    """
    result = job.get("result")
    if isinstance(result, str):
        try:
            result = json.loads(result)
        except (json.JSONDecodeError, TypeError):
            result = None
    # A worker that stored a partial result and then raised leaves the
    # in-memory job with the fatal error in _fatal_error/errors, not in
    # result (only the persisted history row gets it merged in). Fold
    # it in here so live snapshots of failed jobs describe the failure
    # too, not just the partial counts.
    if job.get("status") == "failed" and not (
        isinstance(result, dict) and result.get("error")
    ):
        fatal = job.get("_fatal_error") or (job.get("errors") or [None])[0]
        if fatal:
            if isinstance(result, dict):
                result = {**result, "error": fatal}
                if job.get("_partial_failure"):
                    result["partial_failure"] = error_total(job)
            else:
                result = {"error": fatal}
    described = describe_result(job.get("type") or "", result, job.get("config"))
    steps = job.get("steps") or job.get("tree") or []
    if isinstance(steps, str):
        try:
            steps = json.loads(steps)
        except (json.JSONDecodeError, TypeError):
            steps = []
    from_steps = any(
        isinstance(s, dict) and s.get("summary") for s in steps
    )
    # Interrupted rows get a summary from the startup sweep that says
    # how far the job got; that beats the plain error text the
    # describer would produce, so treat it as authored too.
    authored = isinstance(result, dict) and bool(
        result.get("summary") or result.get("interrupted")
    )
    if described["summary"] and not (from_steps or authored) or not job.get("summary"):
        job["summary"] = described["summary"]
    job["result_details"] = described["details"]
    if described["error"] and not job.get("error"):
        job["error"] = described["error"]
    return job
