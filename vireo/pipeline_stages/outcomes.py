"""Bounded diagnostics persisted inside the existing job-history result.

Counts cover every observed outcome; samples are explicitly incomplete above
the cap. An absent outcome is never evidence that a stage ran successfully.
"""

from typing import Any


class StageOutcomes:
    SAMPLE_LIMIT = 20

    def __init__(self) -> None:
        self.counts: dict[str, int] = {}
        self.samples: dict[str, list[dict[str, Any]]] = {}
        self.settings: dict[str, Any] = {}
        self.stage_error: str | None = None

    def record(self, photo_id: int, reason: str, error: Exception | None = None) -> None:
        self.counts[reason] = self.counts.get(reason, 0) + 1
        samples = self.samples.setdefault(reason, [])
        if len(samples) < self.SAMPLE_LIMIT:
            sample: dict[str, Any] = {"photo_id": photo_id}
            if error is not None:
                sample["error"] = f"{type(error).__name__}: {error}"[:1000]
            samples.append(sample)

    def as_dict(self) -> dict[str, Any]:
        return {
            "counts": self.counts,
            "samples": self.samples,
            "sample_limit_per_outcome": self.SAMPLE_LIMIT,
            "samples_truncated": any(
                count > self.SAMPLE_LIMIT for count in self.counts.values()
            ),
            "settings": self.settings,
            "stage_error": self.stage_error,
        }
