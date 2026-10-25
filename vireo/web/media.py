"""Compose the media blueprint from focused image-serving owners.

URLs and media.<view> endpoint names remain stable. The original and preview
views are also called directly by the prepare-full-resolution job. Thumbnail
repair, masks, crops, previews, editor rendering and originals live in their
respective media_* modules; tests patch collaborators in the module using them.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from flask import Blueprint
from web.media_crops import CropRoutes
from web.media_editing import EditingRoutes
from web.media_editing import render_edit_preview_job as render_edit_preview_job
from web.media_masks import MaskRoutes
from web.media_originals import OriginalRoutes
from web.media_previews import _PreviewServer
from web.media_thumbnails import ThumbnailRoutes

if TYPE_CHECKING:
    from services.service_types import JsonError
    from web.media_editing import _RenderEditPreview
    from web.web_types import AppConfig, ClearPreviewCacheInvalid, GetDb, GetRunner


def create_media_blueprint(
    get_db: GetDb,
    json_error: JsonError,
    db_path: str,
    config: AppConfig,
    *,
    get_runner: GetRunner,
    invalid_preview_cache_paths: set[str],
    clear_preview_cache_invalid: ClearPreviewCacheInvalid,
    render_edit_preview: _RenderEditPreview,
    cancel_edit_preview: Callable[[str, int], None],
) -> Blueprint:
    """Compose media views while preserving the public URLs and endpoint names."""
    blueprint = Blueprint("media", __name__)
    thumbnails = ThumbnailRoutes(get_db, config, get_runner)
    masks = MaskRoutes(get_db, db_path, json_error)
    crops = CropRoutes(get_db, config, db_path, json_error)
    previews = _PreviewServer(
        get_db, config, invalid_preview_cache_paths=invalid_preview_cache_paths,
        clear_preview_cache_invalid=clear_preview_cache_invalid,
    )
    editing = EditingRoutes(get_db, db_path, config, json_error, render_edit_preview, cancel_edit_preview)
    originals = OriginalRoutes(get_db, config)
    for rule, view in (
        ('/thumbnails/<filename>', thumbnails.serve_thumbnail),
        ('/thumbnails/duplicate/<filename>', thumbnails.serve_duplicate_thumbnail),
        ('/masks/<filename>', masks.serve_mask),
        ('/api/masks/<int:pid>/<variant>.png', masks.api_serve_mask),
        ('/api/photos/<int:pid>/masks', masks.api_photo_masks),
        ('/photos/<int:photo_id>/crop', crops.serve_crop_preview),
        ('/photos/<int:photo_id>/focus-crop', crops.serve_focus_crop),
        ('/photos/<int:photo_id>/full', previews.serve_full_photo),
        ('/photos/<int:photo_id>/preview', previews.serve_photo_preview),
        ('/photos/<int:photo_id>/edit-mask-preview', editing.serve_edit_mask_preview),
        ('/photos/<int:photo_id>/edit-preview', editing.serve_photo_edit_preview),
        ('/api/photos/<int:photo_id>/auto-tone', editing.api_auto_tone),
        ('/photos/<int:photo_id>/original', originals.serve_original_photo),
    ):
        blueprint.add_url_rule(rule, endpoint=view.__name__, view_func=view)
    blueprint.add_url_rule(
        "/api/edit-preview/cancel", endpoint="api_cancel_edit_preview",
        view_func=editing.api_cancel_edit_preview, methods=["POST"],
    )
    return blueprint
