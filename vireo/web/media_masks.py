"""Workspace-scoped mask serving, variant listing and immutable mask readers."""

from __future__ import annotations

import logging
import os
import re
from typing import TYPE_CHECKING

from flask import (
    Response,
    jsonify,
    request,
)
from web.media_common import _comparison_media_subject
from web.responses import photo_not_found_error

if TYPE_CHECKING:

    from db import Database
    from flask.typing import ResponseReturnValue
    from PIL import Image
    from services.service_types import JsonError
    from web.web_types import GetDb


log = logging.getLogger(__name__)


_MASK_VARIANT_RE = re.compile(r"^[A-Za-z0-9_-]+$")


def _load_active_mask(db: Database, photo_id: int) -> Image.Image | None:
    """The photo's active SAM mask as a PIL 'L' image, or None."""
    import io

    from PIL import Image

    for _attempt in range(3):
        variant = db.masks_features.active_variant(photo_id)
        mask_row = db.masks_features.get_mask(photo_id, variant) if variant else None
        if not mask_row or not mask_row.get("path"):
            return None
        try:
            with open(mask_row["path"], "rb") as handle:
                mask_bytes = handle.read()
            with Image.open(io.BytesIO(mask_bytes)) as img:
                return img.convert("L").copy()
        except (OSError, ValueError):
            continue
    return None


def _mask_file_is_db_backed(get_db: GetDb, filename: str, mask_path: str) -> bool:
    """Whether a mask file on disk is actually this photo's.

    ``masks/<id>.png`` and ``masks/<id>.<variant>.png`` are keyed by
    bare photo id, and ``photos.id`` recycles freed rowids, so file
    existence alone does not mean the file belongs to the photo now
    holding that id. ``photo_masks`` rows cascade away with the photo,
    so requiring one is what distinguishes "this photo's mask" from
    "a mask the previous owner of this rowid left behind" — and unlike
    a timestamp check it can't be fooled by a stale file that happens
    to be newer than the new photo's source.

    Callers only reach this with an id-keyed filename; ``serve_mask``
    refuses the rest because it cannot scope them to a workspace.
    """
    m = re.match(r"^(\d+)[._]", filename)
    if not m:
        return True
    pid = int(m.group(1))
    db = get_db()
    real = os.path.realpath(mask_path)
    for mask in db.masks_features.list_masks_for_photo(pid):
        if mask["path"] and os.path.realpath(mask["path"]) == real:
            return True
    active_mask_path = db.masks_features.photo_mask_path(pid)
    return bool(active_mask_path and os.path.realpath(active_mask_path) == real)


def _read_mask_bytes(mask_path: str) -> bytes | None:
    """Read an immutable mask generation, or lose a cleanup race.

    POSIX keeps an already-open unlinked file readable, while Windows
    refuses to unlink an open handle. The only race to recover from is
    cleanup winning before ``open``; callers then re-resolve the current
    database path and retry without waiting for a long-running writer.
    """
    try:
        with open(mask_path, "rb") as handle:
            return handle.read()
    except OSError:
        return None


class MaskRoutes:
    """Per-app dependencies for this group of media views."""

    def __init__(self, get_db: GetDb, db_path: str, json_error: JsonError) -> None:
        self._get_db = get_db
        self._db_path = db_path
        self._json_error = json_error

    def serve_mask(self, filename: str) -> ResponseReturnValue:
        """Serve mask PNG files.

        Legacy callers (e.g. ``openInspect`` in pipeline_review.html) still
        request ``/masks/{photo_id}.png``. Variant-aware extraction now
        writes ``{photo_id}.{variant}.png``, so when the literal filename
        isn't on disk and the request looks like ``{photo_id}.png``, fall
        back to the photo's active mask via ``photo_masks`` so the inspect
        panel keeps working without a round-trip API call.
        """
        masks_dir = os.path.join(os.path.dirname(self._db_path), "masks")
        mask_path = os.path.join(masks_dir, filename)
        id_match = re.match(r"^(\d+)[._]", filename)
        if not id_match:
            # Every mask writer names files ``<photo_id>[.<variant>].png``.
            # A file without that prefix can't be tied to a photo, so it
            # can't be scoped to the active workspace -- refuse it.
            return "", 404

        pid = int(id_match.group(1))
        # Same boundary as /api/masks/<pid>/<variant>.png: a mask is only
        # served for a photo the active workspace can see.
        if self._get_db().photos.get(pid, verify_workspace=True) is None:
            return "", 404
        if os.path.exists(mask_path) and _mask_file_is_db_backed(
            self._get_db, filename, mask_path,
        ):
            mask_bytes = _read_mask_bytes(mask_path)
            if mask_bytes is not None:
                return Response(mask_bytes, mimetype="image/png")

        if re.match(r"^(\d+)\.png$", filename):
            db = self._get_db()
            # A committed immutable generation can be unlinked after the DB
            # lookup but before open. Re-resolve after that narrow race; do
            # not block this HTTP request on full model regeneration.
            for _attempt in range(3):
                active = db.masks_features.active_variant(pid)
                if active:
                    mask = db.masks_features.get_mask(pid, active)
                    if mask and mask.get("path"):
                        masks_dir_real = os.path.realpath(masks_dir)
                        abs_path = os.path.realpath(mask["path"])
                        if (
                            abs_path == masks_dir_real
                            or abs_path.startswith(masks_dir_real + os.sep)
                        ):
                            mask_bytes = _read_mask_bytes(abs_path)
                            if mask_bytes is not None:
                                return Response(
                                    mask_bytes, mimetype="image/png",
                                )
        return "", 404

    def api_serve_mask(self, pid: int, variant: str) -> ResponseReturnValue:
        """Serve the mask file recorded on the ``photo_masks`` row.

        Reads the stored ``photo_masks.path`` instead of reconstructing
        ``{pid}.{variant}.png`` so migrated legacy masks (backfilled as
        ``variant='unknown'`` pointing at the old ``{pid}.png`` filename)
        remain viewable in the lightbox.

        Defense in depth: the variant must (1) match the conservative
        whitelist regex, (2) correspond to an actual ``photo_masks`` row,
        and (3) the stored path must resolve inside the masks directory
        (so an attacker-controlled or corrupted DB row can't escape).
        """
        if not _MASK_VARIANT_RE.match(variant):
            return "", 404
        if request.args.get("detection_id") is not None:
            import config as cfg
            db = self._get_db()
            photo = db.photos.get(pid, verify_workspace=True)
            if photo is None:
                return "", 404
            try:
                _, analysis = _comparison_media_subject(db, photo, request.args, db.get_effective_config(cfg.load()))
            except ValueError as exc:
                return self._json_error(str(exc), 404)
            response = Response(analysis["mask_png"], mimetype="image/png")
            response.cache_control.private = True
            response.cache_control.max_age = 0
            return response
        for _attempt in range(3):
            db = self._get_db()
            if db.photos.get(pid, verify_workspace=True) is None:
                return "", 404
            mask = db.masks_features.get_mask(pid, variant)
            if mask is None or not mask.get("path"):
                return "", 404
            masks_dir = os.path.realpath(
                os.path.join(os.path.dirname(self._db_path), "masks")
            )
            abs_path = os.path.realpath(mask["path"])
            if not (abs_path == masks_dir
                    or abs_path.startswith(masks_dir + os.sep)):
                return "", 404
            mask_bytes = _read_mask_bytes(abs_path)
            if mask_bytes is not None:
                # Response owns the bytes, so cleanup after open cannot turn
                # this request into a transient 404 or truncated stream.
                return Response(mask_bytes, mimetype="image/png")
        return "", 404

    def api_photo_masks(self, pid: int) -> ResponseReturnValue:
        """List a photo's available SAM mask variants and the active one.

        Powers the lightbox variant-toggle dropdown: the UI fetches this
        when opening / navigating to a photo and builds one option per
        returned variant plus a default "active" option.
        """
        db = self._get_db()
        if db.photos.get(pid, verify_workspace=True) is None:
            return photo_not_found_error()
        masks = db.masks_features.list_masks_for_photo(pid)
        active = db.masks_features.active_variant(pid)
        return jsonify({
            "photo_id": pid,
            "active": active,
            "variants": [
                {
                    "variant": m["variant"],
                    "url": f"/api/masks/{pid}/{m['variant']}.png",
                    "created_at": m["created_at"],
                }
                for m in masks
            ],
        })
