"""Editor image and mask previews, cancellation, auto tone, and the preview worker entry point."""

from __future__ import annotations

import contextlib
import json
import logging
import math
import os
from collections.abc import Callable, Iterator
from typing import TYPE_CHECKING, Protocol, cast

from app_paths import media_root
from config import config_value
from flask import (
    Response,
    jsonify,
    request,
)
from render_source import (
    companion_image_can_replace_raw_result as _companion_image_can_replace_raw_result,
)
from render_source import (
    has_current_working_copy_failure as _has_current_working_copy_failure,
)
from render_source import (
    image_is_smaller_than_expected as _image_is_smaller_than_expected,
)
from render_source import (
    image_size_after_exif_orientation as _image_size_after_exif_orientation,
)
from render_source import (
    recipe_render_source as _recipe_render_source,
)
from render_source import (
    recipe_source_dimensions as _recipe_source_dimensions,
)
from render_source import (
    record_working_copy_failure as _record_working_copy_failure,
)
from render_source import (
    scaled_recipe_source_dimensions as _scaled_recipe_source_dimensions,
)
from services import detection_store
from web.media_masks import _load_active_mask
from working_copy_cache import (
    touch_working_copy_access,
    working_copy_publication_guard,
)

if TYPE_CHECKING:
    import threading
    from sqlite3 import Row

    import numpy as np
    from db import Database
    from flask.typing import ResponseReturnValue
    from float_image import FloatImage
    from PIL import Image
    from services.preview_workers import PreviewGuard, PreviewResult
    from services.service_types import JsonDict, JsonError
    from web.web_types import AppConfig, GetDb


log = logging.getLogger(__name__)


def _requested_edit_recipe(db: Database, photo_id: int) -> JsonDict:
    """Explicit previews and saved versions never fall back to a different edit."""
    raw = request.args.get("recipe")
    if raw:
        recipe = json.loads(raw)
    elif request.args.get("version_id"):
        version = db.photo_versions.get(photo_id, request.args["version_id"])
        if version is None:
            raise LookupError("Version not found")
        recipe = version["recipe"] or {}
    else:
        recipe = db.get_photo_edit_recipe(photo_id) or {}
    if not isinstance(recipe, dict):
        raise ValueError("Invalid recipe")
    return recipe


class _RenderEditPreview(Protocol):
    """``PreviewWorkers.render``: ``(status, body, source_kind)``."""

    def __call__(
        self, payload: JsonDict, session: str, sequence: int, *, guard: PreviewGuard | None = None,
    ) -> PreviewResult: ...


class _EditPreviewRequest:
    """One ``/photos/<id>/edit-preview`` request after its recipe is parsed.

    ``recipe`` is the normalized edit recipe and ``display_recipe`` the copy
    actually rendered (without the crop unless ``apply_crop``). The selected
    source and its decode bound are filled in by ``load_source``; the
    recovery steps move ``canonical`` when they swap in another file.
    """

    def __init__(
        self,
        db: Database,
        photo_id: int,
        photo: Row,
        folder_row: Row,
        vireo_dir: str,
        recipe: JsonDict,
        display_recipe: JsonDict,
        size: int,
        apply_crop: bool,
        analysis: bool = False,
    ) -> None:
        self.db = db
        self.photo_id = photo_id
        self.photo = photo
        self.folder_row = folder_row
        self.vireo_dir = vireo_dir
        self.recipe = recipe
        self.display_recipe = display_recipe
        self.size = size
        self.apply_crop = apply_crop
        self.analysis = analysis
        self.original_abs = os.path.join(
            folder_row["path"], photo["filename"],
        )
        # Late-bound: _select_and_load_source sets the selected source and
        # its decode bound before anything reads them.
        self.canonical = cast(str, None)
        self.using_working_copy = False
        self.selected_ext = cast(str, None)
        self.load_max_size: int | None = None
        self.native_dims = cast("tuple[int, int]", None)
        self.source_failure_current = False

    def load(self) -> Image.Image | FloatImage | None:
        """Decode the edit source with every recovery path; ``None`` on failure.

        Retries the original when an evicted working copy vanished, swaps an
        undersized RAW decode for a larger companion JPEG, and falls back to
        the companion when the RAW will not decode. A failure the RAW already
        recorded for this source mtime is not retried or re-recorded.
        """
        from image_loader import RAW_EXTENSIONS

        photo = self.photo
        img = self.load_source()
        if self.source_failure_current:
            log.info(
                "Skipping edit-preview generation for photo %s; RAW "
                "working-copy extraction already failed for current source mtime",
                self.photo_id,
            )
            return None
        if img is None and self.using_working_copy:
            img = self.retry_original_source()
        if (
            img is not None
            and self.selected_ext in RAW_EXTENSIONS
            and photo["width"]
            and photo["height"]
        ):
            img = self.replace_undersized_raw_decode(img)
        if img is None and self.selected_ext in RAW_EXTENSIONS:
            img = self.load_companion_after_raw_failure()
        if img is None:
            _record_working_copy_failure(self.db, photo, self.canonical)
        return img

    def load_source(self) -> Image.Image | FloatImage | None:
        """Select the edit source and decode it; ``None`` when it failed.

        If the original volume is offline, the working copy is the only
        local edit source. Hold the publication/eviction guard from source
        selection through Pillow decode so quota enforcement cannot unlink it
        in the exists/open window. Once load_image returns, the decoded PIL
        image no longer depends on the path.
        """
        source_guard = contextlib.nullcontext()
        if self.photo["working_copy_path"] and not os.path.exists(self.original_abs):
            source_guard = working_copy_publication_guard()
        with source_guard:
            return self._select_and_load_source()

    def _source_recipe(self) -> JsonDict:
        """The recipe ``_recipe_render_source`` selects the source with.

        Resolve the source with the actual recipe (not None) so
        _recipe_render_source applies its RAW-primary gating: a RAW photo
        with a legacy JPEG working copy must use the RAW source here too, so
        the in-progress edit preview matches the saved preview/export bytes
        from the highlight-preserving decode. Auto Tone strips tonal
        adjustments to sample neutral pixels; for a RAW primary that can
        leave recipe empty, which would normally short-circuit
        _recipe_render_source to the canonical working copy. Pass a sentinel
        non-empty recipe in that case so the same RAW-primary gating applies
        and analysis reads the highlight-preserved RAW decode rather than
        clipped legacy working-copy bytes. The same sentinel applies when the
        request asks for more pixels than the (size-capped) working copy
        holds — the editor's 100% zoom requests a native-resolution render,
        and short-circuiting to a 4096-capped working copy would silently
        serve half-resolution pixels as "1:1".
        """
        from edit_recipe import SCHEMA_VERSION
        from image_loader import RAW_EXTENSIONS

        photo = self.photo
        recipe = self.recipe
        source_recipe = recipe
        if not recipe:
            from render_source import working_copy_satisfies_recipe_render

            undersized_wc = photo["working_copy_path"] and (
                not working_copy_satisfies_recipe_render(
                    photo, recipe, self.size, self.vireo_dir,
                )
            )
            if (
                self.analysis or undersized_wc
                or os.path.splitext(photo["filename"])[1].lower() in RAW_EXTENSIONS
            ):
                source_recipe = {"version": SCHEMA_VERSION}
        return source_recipe

    def _select_and_load_source(self) -> Image.Image | FloatImage | None:
        from image_loader import (
            RAW_DECODE_LINEAR,
            RAW_EXTENSIONS,
            load_image,
        )

        photo = self.photo
        folder_row = self.folder_row
        source_recipe = self._source_recipe()
        canonical, using_working_copy = _recipe_render_source(
            photo, source_recipe, self.size, self.vireo_dir,
            {folder_row["id"]: folder_row["path"]},
        )
        selected_ext = os.path.splitext(canonical)[1].lower()
        self.canonical = canonical
        self.using_working_copy = using_working_copy
        self.selected_ext = selected_ext
        self.source_failure_current = (
            not using_working_copy
            and selected_ext in RAW_EXTENSIONS
            and _has_current_working_copy_failure(
                photo,
                self.vireo_dir,
                trust_existing_working_copy=False,
                live_source_path=canonical,
                folder_path=folder_row["path"],
            )
        )
        raw_decode = (
            RAW_DECODE_LINEAR
            if selected_ext in RAW_EXTENSIONS
            else None
        )
        load_kwargs = {"raw_decode": raw_decode} if raw_decode else {}
        self.native_dims = _recipe_source_dimensions(photo)
        self.load_max_size = self._source_load_max_size()
        img = None
        if not self.source_failure_current:
            if using_working_copy:
                # Record access on the working copy whenever we
                # read it as an edit-preview source. Without the
                # touch the interactive editor's repeated
                # ``/edit-preview`` requests never advance the
                # WC's recency and an actively edited photo
                # stays the oldest eviction candidate. The touch
                # runs under the guard because it races
                # ``_evict_once``'s identity check, which would
                # otherwise skip this file as "replaced" and
                # silently overshoot a lowered quota.
                #
                # The decode stays OUTSIDE the guard: it is a
                # process-wide lock and the editor fires these
                # on every slider move, so holding it across
                # Pillow would serialize the whole app's image
                # reads behind one preview. If eviction unlinks
                # the copy in the exists/open window, ``img`` is
                # None and the original-source retry below
                # recovers. The offline-only ``source_guard``
                # below still wraps the decode for the case
                # where the WC is the only local source; the
                # guard is an RLock, so the touch reentering it
                # from there is safe.
                with working_copy_publication_guard():
                    touch_working_copy_access(canonical)
            img = load_image(
                canonical, max_size=self.load_max_size, **load_kwargs,
            )
        return img

    def _source_load_max_size(self) -> int | None:
        """The long edge to decode the selected source at.

        A cropped output needs more source pixels than its requested long
        edge. Scale the source request by the crop ratio, then let the recipe
        renderer crop and cap the result to ``size``. At 100% this naturally
        reaches the full source and stays truly 1:1 without forcing a native
        decode for every fitted slider update.
        """
        from image_loader import RAW_EXTENSIONS
        from render_source import rendered_recipe_long_edge

        size = self.size
        recipe = self.recipe
        canonical = self.canonical
        load_max_size = size
        if self.apply_crop and recipe.get("crop"):
            selected_dims = self.native_dims
            try:
                from PIL import Image as _PILImage

                if self.selected_ext not in RAW_EXTENSIONS:
                    with _PILImage.open(canonical) as selected_image:
                        candidate_dims = _image_size_after_exif_orientation(
                            selected_image,
                        )
                    if all(candidate_dims):
                        selected_dims = candidate_dims
            except Exception:
                # broad: some RAW formats cannot be opened by Pillow. Their
                # stored native dimensions remain the best available
                # decode bound.
                log.debug("Could not read dimensions of %s", canonical, exc_info=True)
            if all(selected_dims):
                selected_source_long = max(selected_dims)
                rendered_long = rendered_recipe_long_edge(
                    selected_dims[0], selected_dims[1], recipe,
                )
                if rendered_long > 0:
                    load_max_size = min(
                        selected_source_long,
                        int(math.ceil(
                            size * selected_source_long / rendered_long
                        )),
                    )
            else:
                load_max_size = None
        return load_max_size

    def retry_original_source(self) -> Image.Image | FloatImage | None:
        """Decode the original after the working copy vanished; may be None.

        Quota enforcement can unlink the selected working copy after
        _recipe_render_source returned but before load_image opens it. Retry
        the original source once so an otherwise healthy edit preview does
        not become a transient 500 during eviction; mirrors the crop and
        preview materializer recovery paths.
        """
        from image_loader import (
            RAW_DECODE_LINEAR,
            RAW_EXTENSIONS,
            load_image,
        )

        original_abs = self.original_abs
        img = None
        original_ext = os.path.splitext(original_abs)[1].lower()
        original_is_raw = original_ext in RAW_EXTENSIONS
        original_failure_current = (
            original_is_raw
            and _has_current_working_copy_failure(
                self.photo,
                self.vireo_dir,
                trust_existing_working_copy=False,
                live_source_path=original_abs,
                folder_path=self.folder_row["path"],
            )
        )
        if (
            os.path.abspath(original_abs)
            != os.path.abspath(self.canonical)
            and os.path.isfile(original_abs)
            and not original_failure_current
        ):
            fallback_raw_decode = (
                RAW_DECODE_LINEAR
                if original_is_raw else None
            )
            fallback_kwargs = (
                {"raw_decode": fallback_raw_decode}
                if fallback_raw_decode else {}
            )
            img = load_image(
                original_abs,
                max_size=self.load_max_size,
                **fallback_kwargs,
            )
            if img is not None:
                self.canonical = original_abs
                self.selected_ext = original_ext
                self.using_working_copy = False
        return img

    def replace_undersized_raw_decode(self, img: Image.Image | FloatImage) -> Image.Image | FloatImage | None:
        """Swap an undersized RAW decode for the companion JPEG if larger.

        _load_raw falls back to an embedded camera JPEG when libraw can't
        demosaic; that preview is often a fraction of sensor resolution, so
        non-None can still be undersized. Mirror serve_preview's cache-miss
        undersized check so the in-progress editor doesn't render against a
        clipped downscaled preview when a full-size companion JPEG is sitting
        next to the RAW.
        """
        from image_loader import load_image

        photo = self.photo
        expected_w, expected_h = _scaled_recipe_source_dimensions(
            photo, self.load_max_size,
        )
        if _image_is_smaller_than_expected(img, expected_w, expected_h):
            companion_rel = photo["companion_path"]
            if companion_rel:
                companion_abs = os.path.join(
                    self.folder_row["path"], companion_rel,
                )
                if (
                    os.path.exists(companion_abs)
                    and companion_abs != self.canonical
                ):
                    companion_img = load_image(
                        companion_abs, max_size=self.load_max_size,
                    )
                    if companion_img is not None and _companion_image_can_replace_raw_result(
                        companion_img, img, expected_w, expected_h,
                    ):
                        log.info(
                            "RAW decode for photo %s edit-preview "
                            "returned undersized embedded preview "
                            "(%dx%d, expected %dx%d); falling "
                            "back to companion JPEG",
                            self.photo_id, img.size[0], img.size[1],
                            expected_w, expected_h,
                        )
                        img.close()
                        img = companion_img
                        self.canonical = companion_abs
                    elif companion_img is not None:
                        companion_img.close()
        return img

    def load_companion_after_raw_failure(self) -> Image.Image | None:
        """Decode the companion JPEG after the RAW failed; may be None.

        Same companion fallback as serve_preview's cache-miss path: an
        unsupported RAW shouldn't kill the in-progress edit preview when a
        usable companion JPEG sits next to it.
        """
        from image_loader import load_image

        img = None
        companion_rel = self.photo["companion_path"]
        if companion_rel:
            companion_abs = os.path.join(self.folder_row["path"], companion_rel)
            if (
                os.path.exists(companion_abs)
                and companion_abs != self.canonical
            ):
                log.info(
                    "RAW decode failed for photo %s edit-preview; "
                    "falling back to companion JPEG", self.photo_id,
                )
                _record_working_copy_failure(self.db, self.photo, self.canonical)
                img = load_image(companion_abs, max_size=self.load_max_size)
                if img is not None:
                    self.canonical = companion_abs
        return img

    def render(self, img: Image.Image | FloatImage) -> Image.Image | FloatImage:
        """Apply the display recipe to the decoded source."""
        from image_edits import apply_recipe_to_loaded_image, detail_render_scale
        from render_source import rendered_recipe_long_edge

        native_dims = self.native_dims
        recipe = self.recipe
        # Detail scale must reflect the saved cropped render. This matters
        # especially in crop-edit mode, where the preview itself is still
        # uncropped: a tighter crop must not make the saved output's
        # sharpen/NR stronger than the preview showed.
        preview_detail_scale = None
        if native_dims and native_dims[0] and native_dims[1]:
            saved_native_long = float(rendered_recipe_long_edge(
                native_dims[0], native_dims[1], recipe,
            ))
            if saved_native_long > 0:
                saved_rendered_long = min(float(self.size), saved_native_long)
                preview_detail_scale = detail_render_scale(
                    (saved_rendered_long, saved_rendered_long),
                    native_dims,
                    recipe,
                )
        import local_masks
        return apply_recipe_to_loaded_image(
            img, self.display_recipe, max_size=self.size,
            camera_metadata=self.photo,
            native_size=native_dims,
            detail_scale=preview_detail_scale,
            local_mask=local_masks.load_snapshot(
                self.vireo_dir, self.photo_id, self.display_recipe,
            ),
        )


def render_edit_preview_job(payload: JsonDict, output_path: str) -> tuple[int, str, str]:
    """Child-process entry: all decode/render work, without constructing Flask."""
    import config as cfg
    import cv2
    from db import Database
    from detail_backend import detail_thread_budget
    from edit_recipe import RecipeError
    from float_image import FloatImage

    cfg.CONFIG_PATH = payload['config_path']
    cv2.setNumThreads(payload.get('threads', 2))
    if payload.get('clear_source_cache'):
        import image_loader
        with image_loader._linear_cache_lock:
            image_loader._linear_cache.clear()
    with Database(payload['db_path'], initialize_schema=False) as db:
        edit = _EditPreviewRequest(
            db, payload['photo_id'], payload['photo'], payload['folder'],
            payload['vireo_dir'], payload['recipe'], payload['display_recipe'],
            payload['size'], payload['apply_crop'],
        )
        source = edit.load()
        if source is None:
            return 500, 'Could not load image', ''
        source_kind = 'linear' if isinstance(source, FloatImage) and source.encoding == 'linear' else 'srgb'
        rendered = None
        try:
            with detail_thread_budget(payload.get("threads", 2)):
                rendered = edit.render(source)
            rendered.save(output_path, format='JPEG', quality=payload['quality'])
        except RecipeError as error:
            return 400, str(error), ''
        finally:
            if rendered is not None and rendered is not source:
                rendered.close()
            source.close()
    return 200, '', source_kind


@contextlib.contextmanager
def _edit_preview_source_guard(photo: Row, folder: Row, cancelled: threading.Event) -> Iterator[None]:
    # The eviction guard is process-local. Keep ownership in the parent when
    # the working copy is the only source; the child must not race its eviction.
    working_copy = photo['working_copy_path']
    if working_copy:
        with working_copy_publication_guard(cancelled=cancelled.is_set):
            if not os.path.exists(os.path.join(folder['path'], photo['filename'])):
                yield
                return
    yield


def _edit_mask_preview_geometry(
    photo: Row, normalized: JsonDict, native_dims: tuple[int, int], size: int, apply_crop: bool,
    snapshot: Image.Image,
) -> tuple[tuple[int, int], float | None]:
    """Source dimensions and feather scale for ``/photos/<id>/edit-mask-preview``.

    Returns ``(source_dims, preview_detail_scale)``: the size the local
    weight map is rendered at (falling back to the snapshot's own size when
    the photo's dimensions are unknown) and the detail scale of the saved
    render, or ``None`` when it cannot be derived.
    """
    from image_edits import detail_render_scale
    from render_source import rendered_recipe_long_edge

    source_size = size
    if apply_crop and normalized.get("crop") and all(native_dims):
        native_source_long = max(native_dims)
        rendered_long = rendered_recipe_long_edge(
            native_dims[0], native_dims[1], normalized,
        )
        if rendered_long > 0:
            source_size = min(
                native_source_long,
                int(math.ceil(size * native_source_long / rendered_long)),
            )
    source_dims = _scaled_recipe_source_dimensions(photo, source_size)
    if not source_dims[0] or not source_dims[1]:
        source_dims = snapshot.size
    # Feather scale must reflect the saved cropped render in both modes.
    # In crop-edit mode the overlay itself is aligned with the uncropped
    # preview, but its feather still needs to match the pixels the saved
    # render will actually weight.
    preview_detail_scale = None
    if native_dims and native_dims[0] and native_dims[1]:
        saved_native_long = float(rendered_recipe_long_edge(
            native_dims[0], native_dims[1], normalized,
        ))
        if saved_native_long > 0:
            saved_rendered_long = min(float(size), saved_native_long)
            preview_detail_scale = detail_render_scale(
                (saved_rendered_long, saved_rendered_long),
                native_dims,
                normalized,
            )
    return source_dims, preview_detail_scale


def _tinted_mask_png(weight: np.ndarray) -> bytes:
    """Encode a local weight map as the editor's tinted RGBA PNG overlay."""
    import io

    import numpy as np
    from PIL import Image

    height, width = weight.shape
    rgba = np.zeros((height, width, 4), dtype=np.uint8)
    # Accent teal at up to ~60% opacity where the subject weight is 1.
    rgba[..., 0] = 112
    rgba[..., 1] = 199
    rgba[..., 2] = 186
    rgba[..., 3] = np.clip(weight * 150.0 + 0.5, 0, 255).astype(np.uint8)
    buf = io.BytesIO()
    Image.fromarray(rgba, "RGBA").save(buf, format="PNG")
    return buf.getvalue()


class EditingRoutes:
    """Per-app dependencies for this group of media views."""

    def __init__(self, get_db: GetDb, db_path: str, config: AppConfig, json_error: JsonError, render_edit_preview: _RenderEditPreview, cancel_edit_preview: Callable[[str, int], None]) -> None:
        self._get_db = get_db
        self._db_path = db_path
        self._config = config
        self._json_error = json_error
        self._render_edit_preview = render_edit_preview
        self._cancel_edit_preview = cancel_edit_preview

    def serve_edit_mask_preview(self, photo_id: int) -> ResponseReturnValue:
        """Serve a recipe's transformed, feathered local-weight map as a
        tinted RGBA PNG aligned with the editor preview.

        The existing lightbox mask endpoint serves the live photo_masks file
        untouched; this one shows the pixels the renderer actually weights —
        the recipe's snapshot after geometry and feathering — so the editor
        overlay can never disagree with the saved render.

        A recipe with no local section previews the photo's active mask
        instead (the one the first local slider would freeze), feathered by
        the ``feather`` arg, so Show Mask works before any local edit.
        """
        db = self._get_db()
        photo = db.photos.get(photo_id, verify_workspace=True)
        if not photo:
            return "Not found", 404
        try:
            size = int(request.args.get("size", "1920"))
        except ValueError:
            return "Invalid size", 400
        # The overlay is stretched over the displayed editor image, which the
        # client caps at the overlay-preview size, so allocating a native-res
        # RGBA buffer here would be hundreds of MB to over 1 GB with no
        # visible benefit. Keep the mask endpoint at the overlay cap; only
        # /edit-preview needs the raised limit for true 1:1 zoom.
        size = max(256, min(3840, size))

        raw_recipe = request.args.get("recipe")
        if raw_recipe:
            try:
                recipe = json.loads(raw_recipe)
            except (TypeError, ValueError):
                return "Invalid recipe", 400
        elif request.args.get("version_id"):
            version = db.photo_versions.get(photo_id, request.args["version_id"])
            if version is None:
                return "Version not found", 404
            recipe = version["recipe"] or {}
        else:
            recipe = db.get_photo_edit_recipe(photo_id) or {}

        import local_masks
        from edit_recipe import LOCAL_FEATHER_RANGE, RecipeError, normalize_recipe
        from image_edits import local_weight_map
        try:
            normalized = normalize_recipe(recipe) or {}
        except RecipeError as e:
            return str(e), 400
        preview_feather = None
        if not normalized.get("local"):
            try:
                preview_feather = float(request.args.get("feather", "0"))
            except ValueError:
                return "Invalid feather", 400
            lo, hi = LOCAL_FEATHER_RANGE
            if not math.isfinite(preview_feather) or not (
                lo <= preview_feather <= hi
            ):
                return "Invalid feather", 400
        apply_crop = request.args.get("apply_crop") == "1"
        # Crop editing uses the whole source; after the crop is committed the
        # editor displays the real cropped render and the overlay must follow.
        display_recipe = dict(normalized)
        if not apply_crop:
            display_recipe.pop("crop", None)

        vireo_dir = media_root(self._config["THUMB_CACHE_DIR"])
        if preview_feather is None:
            snapshot = local_masks.load_snapshot(vireo_dir, photo_id, normalized)
            if snapshot is None:
                return "No usable edit-mask snapshot", 404
        else:
            snapshot = _load_active_mask(db, photo_id)
            if snapshot is None:
                return "Photo has no active subject mask", 404
        native_dims = _recipe_source_dimensions(photo)
        source_dims, preview_detail_scale = _edit_mask_preview_geometry(
            photo, normalized, native_dims, size, apply_crop, snapshot,
        )
        weight = local_weight_map(
            snapshot, source_dims, display_recipe,
            native_size=native_dims,
            detail_scale=preview_detail_scale,
            feather=preview_feather,
        )
        if weight is None:
            return "Mask does not fit this photo", 404

        return Response(_tinted_mask_png(weight), mimetype="image/png")

    def api_cancel_edit_preview(self) -> ResponseReturnValue:
        from services.preview_workers import parse_request

        body = request.get_json(silent=True) or {}
        if not isinstance(body, dict):
            return self._json_error('Expected a preview session and sequence')
        try:
            session, sequence = parse_request(body.get('session'), str(body.get('sequence', '')))
        except ValueError as error:
            return self._json_error(str(error))
        self._cancel_edit_preview(session, sequence)
        return '', 204

    def serve_photo_edit_preview(self, photo_id: int) -> ResponseReturnValue:
        """Serve a preview for an in-progress or committed edit recipe.

        Crop editing intentionally uses the whole transformed source. Once a
        crop is committed, ``apply_crop=1`` returns the actual cropped pixels
        so the editor can fit the finished frame in the stage.
        """
        import io

        import config as cfg

        db = self._get_db()
        photo = db.photos.get(photo_id, verify_workspace=True)
        if not photo:
            return "Not found", 404
        try:
            size = int(request.args.get("size", "1920"))
        except ValueError:
            return "Invalid size", 400
        # 16384 (not 3840) so the editor's 100% zoom can request a
        # native-resolution render and be a true 1:1 view.
        size = max(256, min(16384, size))

        try:
            recipe = _requested_edit_recipe(db, photo_id)
        except LookupError:
            return "Version not found", 404
        except ValueError:
            return "Invalid recipe", 400

        try:
            from edit_recipe import RecipeError, normalize_recipe
            recipe = normalize_recipe(recipe) or {}
            apply_crop = request.args.get("apply_crop") == "1"
            display_recipe = dict(recipe)
            if not apply_crop:
                display_recipe.pop("crop", None)
            vireo_dir = media_root(self._config["THUMB_CACHE_DIR"])
            folder_row = db.folders.get(photo["folder_id"])
            if not folder_row:
                return "Not found", 404
            if not self._config.get('EDIT_PREVIEW_IN_PROCESS', False):
                import uuid

                from services.preview_workers import parse_request

                try:
                    session, sequence = parse_request(
                        request.args.get('preview_session', uuid.uuid4().hex),
                        request.args.get('preview_seq', '0'),
                    )
                except ValueError as error:
                    return str(error), 400
                payload = {
                    'db_path': self._db_path, 'config_path': str(cfg.CONFIG_PATH),
                    'photo_id': photo_id, 'photo': dict(photo), 'folder': dict(folder_row),
                    'vireo_dir': vireo_dir, 'recipe': recipe, 'display_recipe': display_recipe,
                    'size': size, 'apply_crop': apply_crop,
                    'quality': config_value(cfg.load(), "preview_quality"),
                    'clear_source_cache': self._config.get('EDIT_PREVIEW_CLEAR_SOURCE_CACHE', False),
                    'threads': self._config.get('EDIT_PREVIEW_THREADS', 2),
                }
                status, body, source_kind = self._render_edit_preview(
                    payload, session, sequence,
                    guard=lambda cancelled: _edit_preview_source_guard(photo, folder_row, cancelled),
                )
                response = Response(body, status=status, mimetype='image/jpeg' if status == 200 else 'text/plain')
                response.headers['Cache-Control'] = 'no-store'
                if source_kind:
                    response.headers['X-Vireo-Preview-Source'] = source_kind
                return response
            # Explicit in-process test seam for source/fallback monkeypatches.
            edit = _EditPreviewRequest(
                db,
                photo_id,
                photo,
                folder_row,
                vireo_dir,
                recipe,
                display_recipe,
                size,
                apply_crop,
            )
            img = edit.load()
            if img is None:
                return "Could not load image", 500
            img = edit.render(img)
        except RecipeError as e:
            return str(e), 400

        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=config_value(cfg.load(), "preview_quality"))
        img.close()
        return Response(buf.getvalue(), mimetype="image/jpeg")

    def api_auto_tone(self, photo_id: int) -> ResponseReturnValue:
        """Fit Auto Tone to the photo as framed by ``recipe``, in ``style``.

        Returns the fitted exposure, highlights, shadows, contrast, whites,
        blacks, vibrance and saturation, whether metering weighted the
        subject (and from its mask or detection box), and plain-language
        notes on what changed. Reads the same highlight-preserving source as
        the edit preview; the recipe's tonal values do not affect the fit.
        """
        import auto_tone

        db = self._get_db()
        photo = db.photos.get(photo_id, verify_workspace=True)
        if not photo:
            return self._json_error("Photo not found", 404)
        try:
            recipe = json.loads(request.args.get("recipe") or "{}")
        except (TypeError, ValueError):
            return self._json_error("Invalid recipe")
        if not isinstance(recipe, dict):
            return self._json_error("Invalid recipe")
        style = request.args.get("style") or "balanced"
        if style not in auto_tone.STYLES:
            return self._json_error("Unknown Auto Tone style")

        from edit_recipe import RecipeError, normalize_recipe
        try:
            recipe = normalize_recipe(recipe) or {}
        except RecipeError as e:
            return self._json_error(str(e))
        # Source selection and decode size depend only on the frame.
        frame_recipe = {
            key: recipe[key]
            for key in ("version", "rotation", "flip", "straighten", "crop")
            if key in recipe
        }
        folder_row = db.folders.get(photo["folder_id"])
        if not folder_row:
            return self._json_error("Photo not found", 404)
        edit = _EditPreviewRequest(
            db,
            photo_id,
            photo,
            folder_row,
            media_root(self._config["THUMB_CACHE_DIR"]),
            frame_recipe,
            frame_recipe,
            auto_tone.SOURCE_LONG_EDGE,
            True,
            analysis=True,
        )
        img = edit.load()
        if img is None:
            return self._json_error("Could not load image", 500)
        box = None
        mask = _load_active_mask(db, photo_id)
        detections = [
            detection for detection in detection_store.get_detections(db, photo_id)
            if detection["category"] == "animal" and detection["detector_model"] != "full-image"
        ]
        if detections:
            primary = detections[0]
            box = {
                "x": primary["box_x"], "y": primary["box_y"],
                "w": primary["box_w"], "h": primary["box_h"],
            }
        try:
            result = auto_tone.fit_loaded_image(
                img, recipe, native_size=edit.native_dims, mask=mask, box=box,
                style=style,
            )
        finally:
            img.close()
            if mask is not None:
                mask.close()
        return jsonify(result)
