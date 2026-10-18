"""A job's step tree: the plan, appended steps, and per-step updates."""

from datetime import datetime


class StepTree:
    """Writes ``job["steps"]`` on the runner's live jobs.

    Owns no state of its own: the steps live on each job dict, and every
    write happens under the runner lock it is given, so it cannot race
    ``snapshot_job``'s copy.
    """

    def __init__(self, lock, jobs):
        self._lock = lock
        self._jobs = jobs

    def set_steps(self, job_id, steps):
        """Define the execution plan for a job.

        Args:
            job_id: job identifier
            steps: list of dicts with at least 'id' and 'label' keys
        """
        full_steps = []
        for s in steps:
            full_steps.append({
                "id": s["id"],
                "label": s["label"],
                "status": "pending",
                "progress": {"current": 0, "total": 0},
                "started_at": None,
                "finished_at": None,
                "duration": None,
                "summary": None,
                "error": None,
                "error_count": 0,
            })
        with self._lock:
            job = self._jobs.get(job_id)
            if job:
                job["steps"] = full_steps

    def append_step(self, job_id, step_id, label, *, status="completed",
                    summary=None, error=None, error_count=0):
        """Append an already-terminal step to a job's plan after the fact.

        For completion hooks that learn of extra work — or its failure —
        only when the job ends (the after-process NAS-move handoff). A
        stepped job's panel view renders ONLY the step tree: per-step
        summary/error/error_count are its sole error surface (global
        ``job.errors`` shows only for failed jobs), so a hook outcome that
        lives just in ``job.result`` would be invisible to the user.

        Appends under the runner lock — worker-thread mutation of
        ``job["steps"]`` outside it races ``snapshot_job``'s copy.
        """
        now = datetime.now().isoformat()
        step = {
            "id": step_id,
            "label": label,
            "status": status,
            "progress": {"current": 0, "total": 0},
            "started_at": now,
            "finished_at": now,
            "duration": 0.0,
            "summary": summary,
            "error": error,
            "error_count": error_count,
        }
        with self._lock:
            job = self._jobs.get(job_id)
            if job is not None:
                job.setdefault("steps", []).append(step)

    def update_step(self, job_id, step_id, **kwargs):
        """Update a step's fields (status, progress, summary, error).

        Automatically sets started_at/finished_at/duration timestamps.
        """
        with self._lock:
            job = self._jobs.get(job_id)
            if not job or "steps" not in job:
                return
            for step in job["steps"]:
                if step["id"] == step_id:
                    new_status = kwargs.get("status")
                    if new_status == "running" and step["status"] == "pending":
                        step["started_at"] = datetime.now().isoformat()
                    # "cancelled" is terminal too — classify/pipeline steps
                    # report it on user cancel; without it here those steps
                    # persist with no finished_at/duration.
                    if new_status in ("completed", "failed", "cancelled") and step["started_at"]:
                        step["finished_at"] = datetime.now().isoformat()
                        start = datetime.fromisoformat(step["started_at"])
                        end = datetime.fromisoformat(step["finished_at"])
                        step["duration"] = round((end - start).total_seconds(), 1)
                    for key in (
                        "status", "summary", "error", "error_count",
                        "progress", "current_file", "source_index",
                        "label_source",
                    ):
                        if key in kwargs:
                            step[key] = kwargs[key]
                    break
