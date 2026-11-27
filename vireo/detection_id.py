# vireo/detection_id.py
"""Content-addressed identifiers for detection rows.

Detection IDs are computed from the detection's natural key so that two
pipelines concurrently writing the same (photo, model) produce identical
ids — converting the DELETE+INSERT in ``write_detection_batch`` into an
idempotent UPSERT.

The hash is truncated to 52 bits so every id is exactly representable as
a JavaScript Number (``Number.MAX_SAFE_INTEGER == 2**53 - 1``), avoiding
silent precision loss when ids ride through JSON to the frontend.
"""
import hashlib
from collections.abc import Mapping
from typing import Any

ALIGNED_REVIEWER_RUNTIME = "reviewer-aligned-v1"
REVIEWER_FRAME_PREFIX = "reviewer-frame-v2:"
COMPANION_FRAME_RUNTIME = "unverified-companion-frame"

DETECTOR_FRAME_PREFIX = "detector-frame-v1:"


def bind_detector_frame(runtime: str, frame_id: object) -> str:
    if isinstance(frame_id, str) and len(frame_id) == 64:
        return DETECTOR_FRAME_PREFIX + frame_id + ":" + runtime
    return runtime


def detection_prompt_runtime(detection: Mapping[str, Any]) -> str:
    row = dict(detection)
    runtime = row.get("runtime_fingerprint") or "legacy"
    if runtime == COMPANION_FRAME_RUNTIME:
        return runtime
    return bind_detector_frame(runtime, row.get("frame_id"))


def prompt_frame_id(runtime: str | None) -> str | None:
    if isinstance(runtime, str):
        if runtime.startswith(REVIEWER_FRAME_PREFIX):
            return runtime[len(REVIEWER_FRAME_PREFIX):]
        if runtime.startswith(DETECTOR_FRAME_PREFIX):
            return runtime[len(DETECTOR_FRAME_PREFIX):].split(":", 1)[0]
    return None


class PromptFrameError(ValueError):
    """The selected box has no proven mapping to the analysis frame."""


class UnverifiedReviewerBox(PromptFrameError):
    """A saved reviewer box still needs confirmation on the original."""


def require_original_box(runtime):
    if runtime == COMPANION_FRAME_RUNTIME:
        raise PromptFrameError(
            "The box may come from a differently cropped JPEG companion. "
            "Re-classify the RAW or draw the subject on the original."
        )


def positive_int_hash(*parts: str) -> int:
    """Return a 52-bit non-negative int derived from SHA-256 of the parts.

    Uses length-prefix encoding (``len:value`` per part) so the boundary
    between parts is unambiguous even if a part itself contains the
    separator character. Today's callers only feed alphanumeric/dotted
    strings, but pinning the encoding now means future callers don't
    have to audit for delimiter collisions.
    """
    payload = "".join(f"{len(p)}:{p}" for p in parts).encode("utf-8")
    digest = hashlib.sha256(payload).hexdigest()
    return int(digest[:13], 16)  # 13 hex chars == 52 bits


def detection_id(
    photo_id: int,
    detector_model: str,
    box: tuple[float, float, float, float],
    category: str,
) -> int:
    """Compute the content-addressed id for a detection row.

    ``box`` is ``(x, y, w, h)`` normalized to [0, 1]. Coords are quantized
    to 4 decimals (~0.4 px on a 4K image) so ONNX float drift between
    inference providers does not produce divergent ids for the same
    logical detection.
    """
    x, y, w, h = box
    qbox = (f"{round(x, 4):.4f}", f"{round(y, 4):.4f}",
            f"{round(w, 4):.4f}", f"{round(h, 4):.4f}")
    return positive_int_hash(
        str(photo_id), detector_model, *qbox, category,
    )
