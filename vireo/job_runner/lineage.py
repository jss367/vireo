"""Who caused a job, which job it follows, and why it started.

Three separate facts, recorded on the job dict and its ``job_history`` row:

- ``origin``: ``"user"`` when the user asked for the work (a click, a request
  the UI made on their behalf), ``"system"`` when Vireo started it on its own
  (startup backfills, hourly maintenance, the recurring discovery checks).
- ``parent_job_id``: the job whose completion started this one (the
  processing run an import chains, the NAS move a processing run chains). A
  follow-up always inherits its parent's origin, so a whole operation reads as
  the user's or as Vireo's, never a mix.
- ``reason``: one sentence saying why a system job started, shown with it.

Callers pass a ``JobLineage`` to ``JobRunner.start`` / ``start_singleton`` /
``enqueue_pipeline``; the runner resolves inheritance and stamps the three
keys on the job. Nothing here decides retention or badge counting; history
reads ``origin`` for its own rules (see ``job_runner.history``).
"""

from __future__ import annotations

from dataclasses import dataclass

ORIGINS = ("user", "system")


@dataclass(frozen=True)
class JobLineage:
    """A job's requested provenance; ``origin=None`` means inherit or ``user``."""

    origin: str | None = None
    parent_job_id: str | None = None
    reason: str | None = None

    def __post_init__(self) -> None:
        if self.origin is not None and self.origin not in ORIGINS:
            raise ValueError(f"unknown job origin {self.origin!r}")

    @classmethod
    def system(cls, reason: str) -> JobLineage:
        """Work Vireo starts on its own, with the sentence saying why."""
        return cls(origin="system", reason=reason)

    @classmethod
    def following(cls, parent_job_id: str | None) -> JobLineage:
        """A follow-up of ``parent_job_id`` (no parent: the user's own job)."""
        return cls(parent_job_id=parent_job_id or None)


def resolve_lineage(lineage: JobLineage | None, parent_origin: str | None) -> dict:
    """The job-dict fields for ``lineage``, given the parent's origin if known.

    A known parent's origin wins over the requested one: the follow-up of a
    system job is system work even if its launcher is also used for the
    user's own requests.
    """
    lineage = lineage or JobLineage()
    origin = parent_origin if parent_origin in ORIGINS else (lineage.origin or "user")
    return {
        "origin": origin,
        "parent_job_id": lineage.parent_job_id,
        "reason": lineage.reason,
    }
