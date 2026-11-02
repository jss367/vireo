"""Detection and focus crops, native-source inspection and evidence validation."""

from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from typing import TYPE_CHECKING, cast

from app_paths import media_root
from config import config_value
from flask import (
    Response,
    request,
)
from services import detection_store
from web.media_common import _comparison_media_subject, _focus_source_image
from web.media_masks import _read_mask_bytes
from working_copy_cache import (
    touch_working_copy_access,
    working_copy_publication_guard,
)

if TYPE_CHECKING:
    from collections.abc import Sequence
    from sqlite3 import Row

    from db import Database
    from flask.typing import ResponseReturnValue
    from PIL import Image
    from services.service_types import JsonDict, JsonError
    from web.web_types import AppConfig, GetDb


log = logging.getLogger(__name__)


_CROP_FOLDER_MISSING = object()


def _load_crop_source(config: AppConfig, db: Database, photo: Row) -> object | Image.Image | None:
    """Decode the crop source: the working copy first, then the original.

    Returns ``_CROP_FOLDER_MISSING`` when there is no working copy and
    the photo's folder row is gone, and ``None`` when nothing decoded.
    """
    from image_loader import load_image

    # Try working copy first, fall back to original
    vireo_dir = media_root(config["THUMB_CACHE_DIR"])
    image_path = None
    using_working_copy = False
    if photo["working_copy_path"]:
        wc = os.path.join(vireo_dir, photo["working_copy_path"])
        if os.path.exists(wc):
            image_path = wc
            using_working_copy = True
    if image_path is None:
        folder = db.folders.get(photo["folder_id"])
        if not folder:
            return _CROP_FOLDER_MISSING
        image_path = os.path.join(folder["path"], photo["filename"])

    if using_working_copy:
        # Stamp access recency — same pattern as
        # ``/photos/<id>/original`` and ``/photos/<id>/edit-preview``.
        # Without this, repeated interactive crop previews leave the
        # working copy's atime unchanged and it stays first in the
        # quota eviction queue even while the user is actively
        # working on the photo.
        #
        # Only the touch runs under the guard, and deliberately so.
        # ``working_copy_publication_guard`` is a process-wide lock
        # that the quota pass holds across a full scandir of the
        # cache; holding it across a decode as well would serialize
        # every interactive image read in the app against every
        # other one and against eviction. A `utime` under the lock
        # costs microseconds. If eviction unlinks the file between
        # the touch and the open, the decode returns None and the
        # original-source fallback below handles it — that path
        # already exists for precisely this race.
        with working_copy_publication_guard():
            touch_working_copy_access(image_path)
    preview_size = 1024 if request.args.get("detection_id") is not None else None
    img = load_image(image_path, max_size=preview_size)
    if img is None and using_working_copy:
        # Quota enforcement can unlink the working copy after the
        # existence check above but before Pillow opens it. Re-resolve
        # the primary source once so an otherwise healthy crop request
        # does not become a transient 500 during eviction.
        folder = db.folders.get(photo["folder_id"])
        original_path = (
            os.path.join(folder["path"], photo["filename"])
            if folder else None
        )
        if original_path and os.path.isfile(original_path):
            img = load_image(original_path, max_size=preview_size)
    return img


def _crop_to_detection(img: Image.Image, dets: list[Row], requested_detection: int | None) -> Image.Image:
    """Crop to the primary (or requested) subject box with its padding."""
    import config as cfg

    det_box = None
    if dets:
        det_row = dets[0]
        det_box = {
            "x": det_row["box_x"], "y": det_row["box_y"],
            "w": det_row["box_w"], "h": det_row["box_h"],
        }
    if requested_detection is not None:
        from subjects import suggested_crop
        det_box = suggested_crop(det_row)
    if det_box:
        iw, ih = img.size
        padding = 0 if requested_detection is not None else config_value(cfg.load(), "detection_padding")
        pad_w = det_box["w"] * padding
        pad_h = det_box["h"] * padding
        x1 = max(0, int((det_box["x"] - pad_w) * iw))
        y1 = max(0, int((det_box["y"] - pad_h) * ih))
        x2 = min(iw, int((det_box["x"] + det_box["w"] + pad_w) * iw))
        y2 = min(ih, int((det_box["y"] + det_box["h"] + pad_h) * ih))
        crop = img.crop((x1, y1, x2, y2))
        if requested_detection is not None or (crop.size[0] >= 50 and crop.size[1] >= 50):
            img = crop
    return img


def _focus_mask_bytes(db_path: str, db: Database, photo_id: int) -> bytes | None:
    """Only read the recorded mask inside this catalog's mask directory."""
    mask_path = db.masks_features.photo_mask_path(photo_id)
    if not mask_path:
        return None
    masks_dir = os.path.realpath(os.path.join(os.path.dirname(db_path), "masks"))
    abs_mask = os.path.realpath(mask_path)
    inside = abs_mask == masks_dir or abs_mask.startswith(masks_dir + os.sep)
    return _read_mask_bytes(abs_mask) if inside else None


def _validated_focus_source(
    config: AppConfig, db: Database, photo: JsonDict, args: Mapping[str, str], effective: JsonDict,
    source_choice: str, boxes: Sequence[Row | JsonDict] = (), basis: str = "auto",
) -> tuple[Image.Image | None, str | None]:
    """Decoding may outlive a source replacement; revalidate pinned evidence."""
    img, source = _focus_source_image(config, photo, db.folders.get(photo["folder_id"]), source_choice)
    if img is not None and args.get("detection_id") is not None:
        try:
            _comparison_media_subject(db, photo, args, effective)
        except ValueError:
            img.close()
            raise
    if img is not None and args.get("detection_id") is None and args.get("center") is None and basis != "frame":
        from focus_crop import focus_subject_detections
        try:
            current = focus_subject_detections(db, photo["id"])
            if [dict(box) for box in current] != [dict(box) for box in boxes]:
                raise ValueError("Subject association changed while decoding; refresh the inspection")
        except ValueError:
            img.close()
            raise
    return img, source


def _focus_geometry_error(img: Image.Image, photo: JsonDict | Row, used: str, args: Mapping[str, str]) -> str | None:
    from focus_crop import validate_raster_geometry
    if used in {"eye", "subject"} or args.get("mask") == "1":
        try:
            validate_raster_geometry(img.size, photo)
        except ValueError as exc:
            img.close()
            return str(exc)
    return None


class CropRoutes:
    """Per-app dependencies for this group of media views."""

    def __init__(self, get_db: GetDb, config: AppConfig, db_path: str, json_error: JsonError) -> None:
        self._get_db = get_db
        self._config = config
        self._db_path = db_path
        self._json_error = json_error

    def serve_crop_preview(self, photo_id: int) -> ResponseReturnValue:
        """Serve the cropped region that would be sent to BioCLIP."""
        import config as cfg
        from PIL import Image

        db = self._get_db()
        # verify_workspace: don't serve image bytes for photos hidden from
        # the active workspace (mirrors serve_thumbnail).
        photo = db.photos.get(photo_id, verify_workspace=True)
        if not photo:
            return "Not found", 404

        # Get primary detection box — global detections table, threshold
        # resolved from workspace-effective config in get_detections.
        requested_detection = request.args.get("detection_id")
        # An explicit detection_id may name a region the user rejected as
        # background: the subject panel shows its crop next to Restore.
        dets = detection_store.get_detections(
            db, photo_id, min_conf=0 if requested_detection is not None else None,
            include_rejected=requested_detection is not None,
        )
        if requested_detection is not None:
            from focus_crop import valid_subject_box
            try:
                requested_detection = int(requested_detection)
            except ValueError:
                return self._json_error("Invalid detection_id")
            dets = [d for d in dets if d["id"] == requested_detection
                    and d["category"] == "animal" and d["detector_model"] != "full-image" and valid_subject_box(d)]
            if not dets:
                return self._json_error("Subject not found", 404)

        loaded = _load_crop_source(self._config, db, photo)
        if loaded is _CROP_FOLDER_MISSING:
            return "Not found", 404
        if loaded is None:
            return "Could not load image", 500
        # The sentinel and None were answered above.
        img = cast("Image.Image", loaded)

        if requested_detection is not None:
            geometry_error = _focus_geometry_error(img, photo, "subject", request.args)
            if geometry_error:
                return self._json_error(geometry_error, 404)
        img = _crop_to_detection(img, dets, requested_detection)

        if requested_detection is not None and request.args.get("suggested") == "1":
            exposure_ev = db.detections.subject_exposure_ev(requested_detection)
            if exposure_ev is not None:
                from image_edits import apply_recipe
                img = apply_recipe(img, {"adjustments": {"exposure": exposure_ev}})
        img.thumbnail((800, 800), Image.Resampling.LANCZOS)
        import io

        preview_quality = config_value(cfg.load(), "preview_quality")
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=preview_quality)
        buf.seek(0)
        response = Response(buf.read(), mimetype="image/jpeg")
        if requested_detection is not None:
            # URLs include the analysis source fingerprint. Reusing a crop in
            # the subject strip or toggling correction can use browser cache.
            response.cache_control.private = True
            response.cache_control.max_age = 3600 if request.args.get("v") else 0
            response.add_etag()
            response.make_conditional(request)
        return response

    def serve_focus_crop(self, photo_id: int) -> ResponseReturnValue:
        """Native upright-source window; eyes require detection/generation binding."""
        import io

        import config as cfg
        from burst_evidence import (
            DEFAULT_FOCUS_CROP_SIZE,
            MAX_FOCUS_CROP_SIZE,
            MIN_FOCUS_CROP_SIZE,
            numeric_query_arg,
        )
        from focus_crop import (
            focus_evidence_headers,
            focus_headers,
            focus_point,
            focus_subject_detections,
            focus_window,
            original_scale,
            parse_center,
            shade_focus_mask,
        )
        from PIL import Image
        from scoring import _normalize_scoring_config

        db = self._get_db()
        photo = db.photos.get(photo_id, verify_workspace=True)
        if not photo:
            return "Not found", 404
        try:
            size = numeric_query_arg(request.args, "size", DEFAULT_FOCUS_CROP_SIZE,
                                     int, MIN_FOCUS_CROP_SIZE, MAX_FOCUS_CROP_SIZE)
        except ValueError as exc:
            return self._json_error(str(exc))
        basis = (request.args.get("basis") or "auto").strip().lower()
        if basis not in {"auto", "eye", "subject", "frame"}:
            return self._json_error("basis must be auto, eye, subject or frame")
        source_choice = (request.args.get("source") or "auto").strip().lower()
        if source_choice not in {"auto", "working", "original"}:
            return self._json_error("source must be auto, working or original")
        point = None
        if request.args.get("center") is not None:
            try:
                point = parse_center(request.args.get("center"))
            except ValueError as exc:
                return self._json_error(str(exc))
        effective = db.get_effective_config(cfg.load())
        eye_enabled = bool(_normalize_scoring_config(effective).get("eye_detect_enabled", False))
        boxes = []
        if point is None and basis != "frame" and request.args.get("detection_id") is None:
            try:
                boxes = focus_subject_detections(db, photo_id)
            except ValueError as exc:
                return self._json_error(str(exc), 404)
        selected_analysis = None
        if request.args.get("detection_id") is not None:
            try:
                detection, selected_analysis = _comparison_media_subject(db, photo, request.args, effective)
            except ValueError as exc:
                return self._json_error(str(exc), 404)
            photo = {**dict(photo), **selected_analysis["features"]}
            boxes = [detection]
        if selected_analysis is None:
            if basis == "eye" and point is None:
                return self._json_error("Eye evidence has no subject/generation binding; analyze the selected subject", 404)
            # Legacy eyes have no detection/mask-generation binding, even when boxes overlap.
            photo = {**dict(photo), "eye_x": None, "eye_y": None, "eye_tenengrad": None}
        try:
            center, used = focus_point(photo, boxes, basis, eye_enabled=eye_enabled, point=point)
            img, source = _validated_focus_source(self._config, db, photo, request.args, effective, source_choice, boxes, basis)
        except ValueError as exc:
            return self._json_error(str(exc), 404)
        if img is None:
            if source_choice in {"working", "original"}:
                return self._json_error(f"The {source_choice} image is not available", 404)
            return "Could not load image", 500
        geometry_error = _focus_geometry_error(img, photo, used, request.args)
        if geometry_error:
            return self._json_error(geometry_error, 404)
        window, center = focus_window(img.size, center, size)
        crop = img.crop(window).convert("RGB")
        mask_bytes = None
        if request.args.get("mask") == "1" and (selected_analysis or (point is None and boxes)):
            mask_bytes = selected_analysis["mask_png"] if selected_analysis else _focus_mask_bytes(self._db_path, db, photo_id)
        if mask_bytes is not None:
            crop = shade_focus_mask(crop, mask_bytes, img.size, window, Image)
        buf = io.BytesIO()
        crop.save(buf, format="JPEG", quality=config_value(effective, "preview_quality"))
        response = Response(buf.getvalue(), mimetype="image/jpeg")
        response.headers.update(focus_headers(
            used, center, window, img.size, source, mask_bytes is not None,
            scale=original_scale(img.size, photo),
        ))
        response.headers.update(focus_evidence_headers(photo, boxes, bound=selected_analysis is not None))
        if selected_analysis:
            response.headers["X-Vireo-Detection-Id"] = str(detection["id"])
            response.headers["X-Vireo-Analysis-Key"] = request.args["analysis_key"]
        response.cache_control.private = True
        response.cache_control.max_age = 0
        return response
