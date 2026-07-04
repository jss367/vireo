"""Edit-mask snapshots for local (mask-weighted) adjustments.

A recipe's ``local.mask.ref`` points at a content-addressed copy of the
photo's active SAM mask, frozen at the moment the local adjustment was
added, so renders stay a deterministic function of (source pixels, recipe)
— the live mask can regenerate without silently changing committed edits.
Staleness against the live mask is detected from ``source_digest`` (a hash
over the source mask file plus the prompt/detector metadata that produced
it), never by comparing snapshot pixels. See
docs/plans/2026-07-03-local-adjustments-design.md; this module implements
the trimmed v1 scope: one snapshot per recipe (copied from the active
photo_masks file for every photo type), aspect-checked at creation, with a
simple grace-window GC instead of a publish/GC lock protocol.
"""

from __future__ import annotations

import contextlib
import hashlib
import io
import json
import logging
import os
import re
import tempfile
import time

from PIL import Image

log = logging.getLogger(__name__)

EDIT_MASKS_SUBDIR = "edit-masks"

# Relative width/height-ratio disagreement beyond which a mask cannot be
# uniform-scaled onto the photo (e.g. a 16:9 embedded-preview mask against a
# 3:2 sensor). Local adjustments are refused rather than misaligned.
ASPECT_TOLERANCE = 0.01

# Unreferenced snapshot files younger than this are never deleted, so GC can
# run concurrently with snapshot creation / recipe saves without a lock.
GC_GRACE_SECONDS = 24 * 3600

_REF_RE = re.compile(r"^[0-9a-f]{12}$")
_SNAPSHOT_FILE_RE = re.compile(r"^(\d+)\.([0-9a-f]{12})\.png$")


def edit_masks_dir(vireo_dir):
    return os.path.join(vireo_dir, EDIT_MASKS_SUBDIR)


def snapshot_path(vireo_dir, photo_id, ref):
    return os.path.join(edit_masks_dir(vireo_dir), f"{photo_id}.{ref}.png")


def _mix_source_meta(h, mask_row):
    meta = json.dumps(
        {
            "variant": mask_row.get("variant"),
            "detector_model": mask_row.get("detector_model"),
            "prompt": [
                mask_row.get("prompt_x"), mask_row.get("prompt_y"),
                mask_row.get("prompt_w"), mask_row.get("prompt_h"),
            ],
        },
        sort_keys=True,
    )
    h.update(meta.encode("utf-8"))


def _source_digest_from_bytes(mask_bytes, mask_row):
    """Digest from an already-read mask buffer + prompt/detector metadata.

    Used by ``create_snapshot`` so the ``ref`` (hash of the snapshotted
    bytes) and ``source_digest`` (staleness signal) describe the SAME
    bytes, even if a mask-extraction job rewrites the live mask file
    between the initial read and the digest — otherwise the recipe would
    record ``source_digest`` over new bytes while the snapshot file holds
    the old ones, and ``is_stale()`` would silently report "current" for
    a stale render.
    """
    h = hashlib.sha1(mask_bytes)
    _mix_source_meta(h, mask_row)
    return f"sha1:{h.hexdigest()}"


def source_digest(mask_row):
    """Digest over the snapshot's source inputs: mask file bytes + the
    prompt/detector metadata that produced it. This is the staleness signal —
    the snapshot's own pixels are never compared."""
    h = hashlib.sha1()
    with open(mask_row["path"], "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    _mix_source_meta(h, mask_row)
    return f"sha1:{h.hexdigest()}"


def create_snapshot(*, photo_id, mask_row, vireo_dir, native_size=None):
    """Freeze the active mask into a content-addressed snapshot.

    Returns the recipe ``local.mask`` fields ``{"ref", "source_digest"}``.
    Raises ValueError when there is no usable mask or when the mask's aspect
    cannot be uniform-scaled onto the photo (``native_size`` is the
    orientation-corrected native (width, height); see
    render_source.recipe_source_dimensions).
    """
    if not mask_row or not mask_row.get("path"):
        raise ValueError("photo has no active subject mask")
    src_path = mask_row["path"]
    if not os.path.exists(src_path):
        raise ValueError("active subject mask file is missing")

    with open(src_path, "rb") as f:
        data = f.read()
    # Open the image from the copied buffer, not the live path — a
    # mask-extraction job rewriting src_path between the read above and
    # the size check would otherwise let us aspect-check different bytes
    # than we snapshot, and could pass a validation that the snapshotted
    # bytes should have failed.
    try:
        with Image.open(io.BytesIO(data)) as img:
            mask_w, mask_h = img.size
    except (OSError, ValueError) as e:
        # Truncated/non-image bytes → UnidentifiedImageError (subclass of
        # OSError) or a decode OSError. Surface as ValueError so the
        # snapshot endpoint returns a recoverable 400 rather than a 500.
        raise ValueError(
            f"active subject mask is not a readable image: {e}"
        ) from e
    if native_size:
        native_w, native_h = native_size
        if native_w and native_h and mask_w and mask_h:
            mask_ar = mask_w / mask_h
            native_ar = native_w / native_h
            if abs(mask_ar - native_ar) / native_ar > ASPECT_TOLERANCE:
                raise ValueError(
                    "subject mask aspect does not match the photo "
                    f"({mask_w}x{mask_h} vs {native_w}x{native_h}); "
                    "local adjustments need a mask regenerated for this photo"
                )

    ref = hashlib.sha1(data).hexdigest()[:12]
    dest = snapshot_path(vireo_dir, photo_id, ref)
    if not os.path.exists(dest):
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        # Per-call tempfile so concurrent POSTs for the same
        # (photo, ref) don't race on a shared ``dest + ".tmp"``: with a
        # deterministic name, one writer's ``os.replace()`` can steal the
        # other's tmp path out from under it, causing the loser to raise
        # FileNotFoundError. mkstemp gives each writer a unique name.
        fd, tmp = tempfile.mkstemp(
            prefix=f".{photo_id}.{ref}.", suffix=".png.tmp",
            dir=os.path.dirname(dest),
        )
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
            os.replace(tmp, dest)
        except OSError:
            with contextlib.suppress(OSError):
                os.unlink(tmp)
            raise
    else:
        # Refresh mtime so the GC grace window is measured from *this*
        # snapshot request, not from whenever the file was first written.
        # Otherwise an aged, currently-unreferenced snapshot (e.g. one
        # created hours ago for a recipe that was never saved) can be
        # swept between this call and the recipe save that re-references
        # it, breaking the just-returned ref.
        with contextlib.suppress(OSError):
            os.utime(dest, None)
    # Digest the same bytes we snapshotted — never re-open src_path —
    # so a mask-extraction job rewriting the live file mid-snapshot
    # can't cause us to record source_digest over NEW bytes while the
    # returned ref points at OLD ones.
    return {
        "ref": ref,
        "source_digest": _source_digest_from_bytes(data, mask_row),
    }


def load_snapshot(vireo_dir, photo_id, recipe):
    """Return the recipe's snapshot as a PIL 'L' image, or None.

    None means "no local pass": the recipe has no local section, or the
    snapshot file is missing/unreadable — the renderer disables every local
    region (never inverts a missing mask into a background weight of 1).
    """
    local = (recipe or {}).get("local") if isinstance(recipe, dict) else None
    if not local:
        return None
    ref = ((local.get("mask") or {}).get("ref")) or ""
    if not _REF_RE.match(ref):
        return None
    path = snapshot_path(vireo_dir, photo_id, ref)
    try:
        with Image.open(path) as img:
            return img.convert("L").copy()
    except (OSError, ValueError):
        log.warning(
            "Edit-mask snapshot missing/unreadable for photo %s ref %s; "
            "local adjustments disabled for this render",
            photo_id, ref,
        )
        return None


def is_stale(recipe, active_mask_row):
    """True when the live active mask no longer matches the recipe snapshot's
    recorded source (prompt moved, detector changed, mask file rewritten, or
    the mask went away). Recipes without a local section are never stale."""
    local = (recipe or {}).get("local") if isinstance(recipe, dict) else None
    if not local:
        return False
    recorded = (local.get("mask") or {}).get("source_digest")
    if not recorded:
        return True
    if not active_mask_row or not active_mask_row.get("path"):
        return True
    try:
        return source_digest(active_mask_row) != recorded
    except OSError:
        return True


def _referenced_refs(db):
    """Every local.mask.ref reachable from current recipes or edit history."""
    refs = set()
    pattern = re.compile(r'"ref":\s*"([0-9a-f]{12})"')
    queries = (
        "SELECT recipe_json AS v FROM photo_edit_recipes "
        "WHERE recipe_json LIKE '%\"local\"%'",
        "SELECT old_value AS v FROM edit_history_items "
        "WHERE old_value LIKE '%\"local\"%'",
        "SELECT new_value AS v FROM edit_history_items "
        "WHERE new_value LIKE '%\"local\"%'",
    )
    for query in queries:
        for row in db.conn.execute(query).fetchall():
            refs.update(pattern.findall(row["v"] or ""))
    return refs


def gc_edit_masks(db, vireo_dir, grace_seconds=GC_GRACE_SECONDS):
    """Delete snapshot files no recipe (current or history) references.

    Files younger than ``grace_seconds`` are kept regardless, which makes
    the sweep safe against snapshots created moments before their recipe
    row commits — no locking needed for a single-process app.
    """
    directory = edit_masks_dir(vireo_dir)
    if not os.path.isdir(directory):
        return {"deleted": 0, "kept": 0}
    refs = _referenced_refs(db)
    cutoff = time.time() - grace_seconds
    deleted = kept = 0
    for name in os.listdir(directory):
        path = os.path.join(directory, name)
        if name.endswith(".tmp"):
            match = None
        else:
            match = _SNAPSHOT_FILE_RE.match(name)
        try:
            if match and match.group(2) in refs:
                kept += 1
                continue
            if os.path.getmtime(path) > cutoff:
                kept += 1
                continue
            os.remove(path)
            deleted += 1
        except OSError:
            log.warning("Could not GC edit-mask file %s", path, exc_info=True)
    return {"deleted": deleted, "kept": kept}
