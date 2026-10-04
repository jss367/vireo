"""Deterministic serialization, repository identity, and algorithm contracts."""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np


def json_default(value):
    if isinstance(value, np.ndarray):
        return value.tolist()
    raise TypeError(type(value).__name__)


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False, default=json_default)


def digest(value):
    return hashlib.sha256(encode(value).encode()).hexdigest()


def write_json(path, value):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(encode(value) + "\n", encoding="utf-8")
    temporary.replace(path)


def configure_repo(path=None):
    repo = Path(path).resolve() if path else Path(__file__).resolve().parents[4]
    if not (repo / "vireo" / "encounters.py").is_file():
        raise ValueError("Run from a Vireo checkout or pass --repo /path/to/vireo")
    sys.path.insert(0, str(repo / "vireo"))
    return repo


def code_identity(repo):
    # Hash actual source contents, including untracked experiments, not just HEAD.
    roots = [repo / "vireo", repo / "tools" / "encounter-evaluation" / "src"]
    files = {}
    for root in roots:
        for path in sorted(root.rglob("*.py")):
            if "__pycache__" not in path.parts:
                files[str(path.relative_to(repo))] = hashlib.sha256(path.read_bytes()).hexdigest()
    # Report generation also consumes HTML templates alongside the Python
    # modules; a template-only change still produces a materially different
    # review artifact, so the frozen source digest has to notice it.
    for path in sorted((repo / "tools" / "encounter-evaluation" / "src").rglob("*.html")):
        files[str(path.relative_to(repo))] = hashlib.sha256(path.read_bytes()).hexdigest()
    try:
        revision = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=repo, text=True, stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        revision = "unavailable"
    return {"revision": revision, "source_digest": digest(files), "python": sys.version,
            "numpy": np.__version__}


@dataclass(frozen=True)
class Group:
    photo_ids: tuple[int, ...]
    # None means unresolved, never a verified empty set.
    roster: tuple[str, ...] | None
    reason: str


def validate_groups(photos, groups):
    expected = [p["id"] for p in photos]
    actual = [pid for group in groups for pid in group.photo_ids]
    if actual != expected or any(not g.photo_ids for g in groups):
        raise ValueError("Algorithm must preserve ordered, contiguous, exactly-once photo membership")


def restore_features(photos):
    for photo in photos:
        for key in ("dino_subject_embedding", "dino_global_embedding"):
            if photo.get(key) is not None:
                photo[key] = np.asarray(photo[key], dtype=np.float32)
    return photos
