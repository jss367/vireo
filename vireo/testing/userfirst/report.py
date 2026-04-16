"""Findings and report formatting for user-first testing runs."""
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

FINDING_KINDS = ("BUG", "SUSPECT", "PERF", "WARN")


@dataclass
class Finding:
    kind: str
    message: str
    context: dict = field(default_factory=dict)

    @classmethod
    def bug(cls, message, **ctx):
        return cls("BUG", message, ctx)

    @classmethod
    def suspect(cls, message, **ctx):
        return cls("SUSPECT", message, ctx)

    @classmethod
    def perf(cls, message, **ctx):
        return cls("PERF", message, ctx)

    @classmethod
    def warn(cls, message, **ctx):
        return cls("WARN", message, ctx)

    def to_dict(self):
        return {"kind": self.kind, "message": self.message, "context": self.context}


class Report:
    def __init__(self, name):
        self.name = name
        self.findings: list[Finding] = []
        self.steps: list[dict[str, Any]] = []
        self.screenshots: list[Path] = []
        self.duration_s: float = 0.0

    def add(self, finding):
        self.findings.append(finding)

    def record_step(self, action, status=None, elapsed_ms=None, **extra):
        step = {"action": action, "status": status, "elapsed_ms": elapsed_ms}
        step.update(extra)
        self.steps.append(step)

    def add_screenshot(self, path):
        self.screenshots.append(Path(path))

    def counts(self):
        c = {k: 0 for k in FINDING_KINDS}
        for f in self.findings:
            c[f.kind] = c.get(f.kind, 0) + 1
        return c

    def has_bugs(self):
        return any(f.kind == "BUG" for f in self.findings)

    def to_markdown(self):
        counts = self.counts()
        header = (
            f"## User-first run — {self.name}\n"
            f"**Result:** "
            + ", ".join(f"{counts[k]} {k}" for k in FINDING_KINDS)
            + f"\n**Duration:** {self.duration_s:.1f}s\n"
        )

        lines = [header]

        if self.findings:
            lines.append("### Findings\n")
            for f in self.findings:
                ctx = (
                    " (" + ", ".join(f"{k}={v}" for k, v in f.context.items()) + ")"
                    if f.context
                    else ""
                )
                lines.append(f"- [{f.kind}] {f.message}{ctx}")
            lines.append("")

        if self.steps:
            lines.append("### Steps")
            for i, s in enumerate(self.steps, 1):
                status = f" → {s['status']}" if s.get("status") is not None else ""
                elapsed = (
                    f" ({s['elapsed_ms']}ms)" if s.get("elapsed_ms") is not None else ""
                )
                lines.append(f"{i}. {s['action']}{status}{elapsed}")
            lines.append("")

        if self.screenshots:
            lines.append("### Screenshots")
            for s in self.screenshots:
                lines.append(f"- {s.name}")
            lines.append("")

        return "\n".join(lines)

    def to_dict(self):
        return {
            "name": self.name,
            "duration_s": self.duration_s,
            "counts": self.counts(),
            "findings": [f.to_dict() for f in self.findings],
            "steps": self.steps,
            "screenshots": [str(s) for s in self.screenshots],
        }

    def write_json(self, path):
        Path(path).write_text(json.dumps(self.to_dict(), indent=2, default=str))

    def write_markdown(self, path):
        Path(path).write_text(self.to_markdown())
