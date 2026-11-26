"""Whether restarting Vireo to install an update would stop the user's work.

The desktop shell polls ``/api/v1/update-readiness`` while it waits to
restart for an update. A job blocks the restart when the user started it
(``origin`` is not ``system``) and it is queued, running or pausing.

System jobs (hourly maintenance, the missing-originals and new-images checks)
never block: they start often enough that a moment with none running might
never come, and they are safe to interrupt. A paused job never blocks either:
it is not about to finish, so waiting for it could hold the update forever.
Paused jobs are still listed so the prompt can say they will stop.

A queued job blocks, so a follow-up that is waiting to start (the Process run
an import queues) holds the restart like a running one.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from services.service_types import JobDict, JsonDict

BLOCKING_STATUSES = frozenset({"queued", "running", "pausing"})

# Names for the job types whose generic label would mislead: the pipeline job
# is "Process" everywhere in the UI, and the import variants are one feature.
_JOB_NAMES = {
    "pipeline": "Process run",
    "import": "Import",
    "import-full": "Import",
    "import-in-place": "Import",
    "import-photos": "Import",
}


def job_name(job_type: str) -> str:
    """The name the update prompt uses for a job of ``job_type``."""
    if job_type in _JOB_NAMES:
        return _JOB_NAMES[job_type]
    words = [w for w in job_type.replace("_", "-").split("-") if w]
    if not words:
        return "Job"
    words = ["NAS" if w.lower() == "nas" else w.lower() for w in words]
    words[0] = words[0][:1].upper() + words[0][1:]
    return " ".join(words)


def _entry(job: JobDict) -> JsonDict:
    job_type = str(job.get("type") or "")
    return {
        "id": job.get("id"),
        "type": job_type,
        "name": job_name(job_type),
        "status": job.get("status"),
    }


def restart_readiness(jobs: Iterable[JobDict]) -> JsonDict:
    """Say whether a restart now would stop work the user started.

    ``jobs`` is ``JobRunner.list_jobs()``: live jobs plus queued pipelines.
    ``idle`` is true when no user job is queued, running or pausing;
    ``blocking`` lists those jobs, and ``paused`` lists the user's paused
    jobs, which a restart stops but which do not hold it back.
    """
    blocking: list[JsonDict] = []
    paused: list[JsonDict] = []
    for job in jobs:
        if job.get("origin") == "system":
            continue
        status = job.get("status")
        if status in BLOCKING_STATUSES:
            blocking.append(_entry(job))
        elif status == "paused":
            paused.append(_entry(job))
    return {"idle": not blocking, "blocking": blocking, "paused": paused}
