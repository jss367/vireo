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
