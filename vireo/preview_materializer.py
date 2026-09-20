"""Shared preview rendering and durable cache materialization.

The interactive photo routes, the standalone preview job, and the pipeline
preview stage all use this module.  Keeping source selection and publication
in one place prevents a background warmer from racing an interactive request
or publishing visually different bytes for the same ``{photo_id}_{size}``
artifact.
"""

from __future__ import annotations

import contextlib
import io
import logging
import os
from dataclasses import dataclass

from artifact_flight import atomic_write_bytes, preview_artifact_flights
from camera_denoise import cache_matches, cache_save_options
from render_source import (
    companion_image_can_replace_raw_result,
    has_current_working_copy_failure,
    image_is_smaller_than_expected,
    recipe_render_source,
    recipe_source_dimensions,
    record_working_copy_failure,
    scaled_recipe_source_dimensions,
    working_copy_path_if_satisfies,
)
from working_copy_cache import working_copy_publication_guard

log = logging.getLogger(__name__)


class PreviewMaterializationError(RuntimeError):
    """A preview producer finished without publishing a usable artifact."""


class PreviewSourceUnavailable(PreviewMaterializationError):
    """The current RAW source is already known to be unreadable."""


@dataclass(frozen=True)
class PreviewMaterialization:
    data: bytes | None
    generated: bool
    published: bool


def render_preview_bytes(
    db,
    photo,
    folder_path,
    *,
    size,
    vireo_dir,
    preview_quality,
    recipe=None,
    pair_source=None,
    pair_source_path=None,
):
    """Render one preview using the same RAW/edit fallback rules everywhere."""
    from image_edits import apply_recipe_to_loaded_image
    from image_loader import (
        RAW_DECODE_PRESERVE_HIGHLIGHTS,
        RAW_EXTENSIONS,
        load_image,
    )

    photo_id = photo["id"]
    if not pair_source_path and not folder_path:
        raise PreviewSourceUnavailable(
            f"source folder for photo {photo_id} is unavailable"
        )
    folders = {photo["folder_id"]: folder_path}

    original_abs = (
        os.path.join(folder_path, photo["filename"]) if folder_path else ""
    )

    def _select_and_load_source():
        if pair_source_path:
            canonical = pair_source_path
            using_working_copy = False
        elif (
            not recipe
            and os.path.splitext(photo["filename"])[1].lower()
            in RAW_EXTENSIONS
        ):
            source_path = original_abs
            if (
                os.path.exists(source_path)
                and not has_current_working_copy_failure(
                    photo,
                    vireo_dir,
                    trust_existing_working_copy=False,
                    live_source_path=source_path,
                    folder_path=folder_path,
                )
            ):
                canonical = source_path
                using_working_copy = False
            else:
                canonical, using_working_copy = recipe_render_source(
                    photo, recipe, size, vireo_dir, folders,
                )
        else:
            canonical, using_working_copy = recipe_render_source(
                photo, recipe, size, vireo_dir, folders,
            )

        selected_ext = os.path.splitext(canonical)[1].lower()
        if (
            not using_working_copy
            and selected_ext in RAW_EXTENSIONS
            and has_current_working_copy_failure(
                photo,
                vireo_dir,
                trust_existing_working_copy=False,
                live_source_path=canonical,
                folder_path=folder_path,
            )
        ):
            raise PreviewSourceUnavailable(
                f"RAW source for photo {photo_id} already failed at its current mtime"
            )

        load_max_size = None if recipe and recipe.get("crop") else size
        raw_decode = (
            RAW_DECODE_PRESERVE_HIGHLIGHTS
            if selected_ext in RAW_EXTENSIONS and (recipe or pair_source == "raw")
            else None
        )
        load_kwargs = {"raw_decode": raw_decode} if raw_decode else {}
        img = load_image(canonical, max_size=load_max_size, **load_kwargs)
        return canonical, using_working_copy, selected_ext, load_max_size, img

    # With the original volume offline, the working copy is the only usable
    # preview source and retry-to-original cannot recover an eviction race.
    # Pin it from source selection through decode; the returned PIL image no
    # longer depends on the pathname after load_image completes.
    source_guard = contextlib.nullcontext()
    if (
        not pair_source_path
        and photo["working_copy_path"]
        and not os.path.isfile(original_abs)
    ):
        source_guard = working_copy_publication_guard()
    with source_guard:
        (
            canonical, using_working_copy, selected_ext, load_max_size, img,
        ) = _select_and_load_source()

    if img is None and using_working_copy and not pair_source_path:
        # The working copy may be evicted after source selection but before
        # ``load_image`` opens it. Retry the primary source once when it is
        # usable, mirroring export/crop recovery instead of surfacing a
        # PreviewMaterializationError for a benign quota race.
        original_ext = os.path.splitext(original_abs)[1].lower()
        original_is_raw = original_ext in RAW_EXTENSIONS
        original_failure_current = original_is_raw and (
            has_current_working_copy_failure(
                photo,
                vireo_dir,
                trust_existing_working_copy=False,
                live_source_path=original_abs,
                folder_path=folder_path,
            )
        )
        if (
            os.path.abspath(original_abs) != os.path.abspath(canonical)
            and os.path.isfile(original_abs)
            and not original_failure_current
        ):
            fallback_raw_decode = (
                RAW_DECODE_PRESERVE_HIGHLIGHTS
                if original_is_raw and (recipe or pair_source == "raw")
                else None
            )
            fallback_kwargs = (
                {"raw_decode": fallback_raw_decode}
                if fallback_raw_decode else {}
            )
            img = load_image(
                original_abs, max_size=load_max_size, **fallback_kwargs,
            )
            if img is not None:
                canonical = original_abs
                selected_ext = original_ext
                using_working_copy = False

    if (
        img is not None
        and selected_ext in RAW_EXTENSIONS
        and photo["width"]
        and photo["height"]
        and pair_source != "raw"
    ):
        expected_w, expected_h = scaled_recipe_source_dimensions(
            photo, load_max_size,
        )
        if image_is_smaller_than_expected(img, expected_w, expected_h):
            companion_rel = photo["companion_path"]
            if companion_rel:
                companion_abs = os.path.join(folder_path, companion_rel)
                if os.path.exists(companion_abs) and companion_abs != canonical:
                    companion_img = load_image(
                        companion_abs, max_size=load_max_size,
                    )
                    if companion_image_can_replace_raw_result(
                        companion_img, img, expected_w, expected_h,
                    ):
                        log.info(
                            "RAW decode for photo %s preview at size=%s "
                            "returned undersized embedded preview (%dx%d, "
                            "expected %dx%d); falling back to companion JPEG",
                            photo_id, size, img.size[0], img.size[1],
                            expected_w, expected_h,
                        )
                        img.close()
                        img = companion_img
                        canonical = companion_abs
                    elif companion_img is not None:
                        companion_img.close()

    if img is None and selected_ext in RAW_EXTENSIONS and pair_source != "raw":
        companion_rel = photo["companion_path"]
        if companion_rel:
            companion_abs = os.path.join(folder_path, companion_rel)
            if os.path.exists(companion_abs) and companion_abs != canonical:
                log.info(
                    "RAW decode failed for photo %s preview at size=%s; "
                    "falling back to companion JPEG",
                    photo_id, size,
                )
                record_working_copy_failure(db, photo, canonical)
                img = load_image(companion_abs, max_size=load_max_size)
                if img is not None:
                    canonical = companion_abs

        if img is None:
            working_copy = working_copy_path_if_satisfies(
                photo, recipe, size, vireo_dir, rel_slack=0.01,
            )
            if (
                working_copy
                and os.path.abspath(working_copy) != os.path.abspath(canonical)
            ):
                log.info(
                    "RAW decode failed for photo %s preview at size=%s; "
                    "falling back to JPEG working copy",
                    photo_id, size,
                )
                record_working_copy_failure(db, photo, canonical)
                img = load_image(working_copy, max_size=load_max_size)
                if img is not None:
                    canonical = working_copy

    if img is None:
        record_working_copy_failure(db, photo, canonical)
        raise PreviewMaterializationError(
            f"could not load a preview source for photo {photo_id}"
        )

    try:
        if recipe:
            import local_masks

            rendered = apply_recipe_to_loaded_image(
                img,
                recipe,
                max_size=size,
                camera_metadata=photo,
                native_size=recipe_source_dimensions(photo),
                local_mask=local_masks.load_snapshot(
                    vireo_dir, photo_id, recipe,
                ),
            )
            if rendered is not img:
                img.close()
            img = rendered

        encoded = io.BytesIO()
        img.save(encoded, format="JPEG", quality=preview_quality, **cache_save_options(photo, recipe))
        return encoded.getvalue()
    finally:
        with contextlib.suppress(Exception):
            img.close()


def materialize_preview(
    db,
    photo,
    folder_path,
    *,
    size,
    vireo_dir,
    preview_quality,
    recipe=None,
    cache_path=None,
    pair_source=None,
    pair_source_path=None,
    coordinate=True,
    publish_best_effort=False,
):
    """Render and atomically publish one preview, joining equal-key work.

    ``cache_path=None`` is used by explicit paired-source views, whose bytes
    must never enter the ordinary ``(photo_id, size)`` cache.
    """
    photo_id = photo["id"]
    if coordinate and publish_best_effort:
        raise ValueError(
            "coordinated previews require reliable artifact publication"
        )

    def consume_published():
        if (cache_path and os.path.exists(cache_path) and os.path.getsize(cache_path)
                and cache_matches(cache_path, photo, recipe)):
            return PreviewMaterialization(
                data=None, generated=False, published=True,
            )
        raise PreviewMaterializationError(
            f"preview producer did not publish {cache_path!r}"
        )

    def produce():
        if (
            coordinate
            and cache_path
            and os.path.exists(cache_path)
            and os.path.getsize(cache_path)
            and cache_matches(cache_path, photo, recipe)
        ):
            return PreviewMaterialization(
                data=None, generated=False, published=True,
            )
        data = render_preview_bytes(
            db,
            photo,
            folder_path,
            size=size,
            vireo_dir=vireo_dir,
            preview_quality=preview_quality,
            recipe=recipe,
            pair_source=pair_source,
            pair_source_path=pair_source_path,
        )
        published = False
        if cache_path:
            try:
                atomic_write_bytes(data, cache_path)
            except Exception:
                if not publish_best_effort:
                    raise
                log.warning(
                    "Failed to persist preview cache %s", cache_path,
                    exc_info=True,
                )
            else:
                published = True
                with contextlib.suppress(Exception):
                    db.preview_cache_insert(photo_id, size, len(data))
        return PreviewMaterialization(
            data=data, generated=True, published=published,
        )

    if cache_path and coordinate:
        result = preview_artifact_flights.run(
            os.path.abspath(cache_path), produce, consume_published,
        )
        return result.value
    return produce()
