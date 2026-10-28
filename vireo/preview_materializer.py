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
import sqlite3
from dataclasses import dataclass

from artifact_flight import atomic_write_bytes, preview_artifact_flights
from render_fingerprint import cache_matches, cache_save_options
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


def _select_and_load_preview_source(
    photo, folder_path, original_abs, folders, *, size, vireo_dir, recipe,
    pair_source, pair_source_path, load_image,
):
    """Pick the preview source and decode it.

    Returns ``(canonical, using_working_copy, selected_ext, load_max_size,
    img)``; ``img`` is ``None`` when the decode failed. Raises
    ``PreviewSourceUnavailable`` when the RAW already failed at its current
    mtime.
    """
    from image_loader import RAW_DECODE_LINEAR, RAW_EXTENSIONS

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
            f"RAW source for photo {photo['id']} already failed at its current mtime"
        )

    load_max_size = None if recipe and recipe.get("crop") else size
    raw_decode = (
        RAW_DECODE_LINEAR
        if selected_ext in RAW_EXTENSIONS and (recipe or pair_source == "raw")
        else None
    )
    load_kwargs = {"raw_decode": raw_decode} if raw_decode else {}
    img = load_image(canonical, max_size=load_max_size, **load_kwargs)
    return canonical, using_working_copy, selected_ext, load_max_size, img


def _retry_original_after_eviction(
    photo, folder_path, original_abs, canonical, *, vireo_dir, recipe,
    pair_source, load_max_size, load_image,
):
    """Retry the primary source when the working copy vanished before decode.

    Returns ``(img, canonical, selected_ext)`` for the original, or ``None``
    when it is unusable or did not decode either.
    """
    from image_loader import RAW_DECODE_LINEAR, RAW_EXTENSIONS

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
            RAW_DECODE_LINEAR
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
            return img, original_abs, original_ext
    return None


def _replace_undersized_raw_preview(
    img, photo, folder_path, canonical, *, size, selected_ext, pair_source,
    load_max_size, load_image,
):
    """Swap an undersized embedded RAW preview for a larger companion JPEG.

    Returns ``(img, canonical)``, unchanged when no companion qualifies.
    """
    from image_loader import RAW_EXTENSIONS

    photo_id = photo["id"]
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
    return img, canonical


def _load_raw_failure_fallback(
    db, photo, folder_path, canonical, *, size, vireo_dir, recipe,
    load_max_size, load_image,
):
    """After a RAW decode failure, try the companion JPEG then the working copy.

    Returns ``(img, canonical)``; ``img`` stays ``None`` when neither loads.
    """
    photo_id = photo["id"]
    img = None
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
    return img, canonical


def _encode_preview(
    img, photo, *, size, vireo_dir, recipe, preview_quality,
    apply_recipe_to_loaded_image, source_path=None,
):
    """Apply the recipe (if any) and JPEG-encode; always closes ``img``."""
    photo_id = photo["id"]
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
        img.save(encoded, format="JPEG", quality=preview_quality, **cache_save_options(photo, recipe, source_path))
        return encoded.getvalue()
    finally:
        # broad: cleanup must not replace the render's own exception (or its bytes)
        with contextlib.suppress(Exception):
            img.close()


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
        ) = _select_and_load_preview_source(
            photo, folder_path, original_abs, folders, size=size,
            vireo_dir=vireo_dir, recipe=recipe, pair_source=pair_source,
            pair_source_path=pair_source_path, load_image=load_image,
        )

    if img is None and using_working_copy and not pair_source_path:
        retried = _retry_original_after_eviction(
            photo, folder_path, original_abs, canonical, vireo_dir=vireo_dir,
            recipe=recipe, pair_source=pair_source,
            load_max_size=load_max_size, load_image=load_image,
        )
        if retried is not None:
            img, canonical, selected_ext = retried
            using_working_copy = False

    img, canonical = _replace_undersized_raw_preview(
        img, photo, folder_path, canonical, size=size,
        selected_ext=selected_ext, pair_source=pair_source,
        load_max_size=load_max_size, load_image=load_image,
    )

    if img is None and selected_ext in RAW_EXTENSIONS and pair_source != "raw":
        img, canonical = _load_raw_failure_fallback(
            db, photo, folder_path, canonical, size=size, vireo_dir=vireo_dir,
            recipe=recipe, load_max_size=load_max_size, load_image=load_image,
        )

    if img is None:
        record_working_copy_failure(db, photo, canonical)
        raise PreviewMaterializationError(
            f"could not load a preview source for photo {photo_id}"
        )

    return _encode_preview(
        img, photo, size=size, vireo_dir=vireo_dir, recipe=recipe,
        preview_quality=preview_quality,
        apply_recipe_to_loaded_image=apply_recipe_to_loaded_image,
        source_path=canonical,
    )


def _publish_preview(
    db, photo_id, size, data, cache_path, *, publication_guard,
    publish_best_effort,
):
    """Atomically write rendered bytes to ``cache_path`` and track them.

    Returns whether the file was published. A write failure is raised unless
    ``publish_best_effort``; a tracking failure only logs.
    """
    published = False
    if cache_path:
        guard = (
            publication_guard() if publication_guard
            else contextlib.nullcontext()
        )
        with guard:
            try:
                atomic_write_bytes(data, cache_path)
            except Exception:  # broad: best-effort publication is the caller's choice; otherwise re-raises
                if not publish_best_effort:
                    raise
                log.warning(
                    "Failed to persist preview cache %s", cache_path,
                    exc_info=True,
                )
            else:
                published = True
        if published:
            try:
                # Standalone renders (no catalog) pass db=None.
                if db is not None:
                    db.caches.preview_insert(photo_id, size, len(data))
            except sqlite3.Error:
                # The file is published; an untracked preview is adopted
                # on its next hit. Usually the photo was deleted (FK).
                log.debug("Could not track preview for photo %s", photo_id, exc_info=True)
    return published


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
    publication_guard=None,
):
    """Render and atomically publish one preview, joining equal-key work.

    ``cache_path=None`` is used by explicit paired-source views, whose bytes
    must never enter the ordinary ``(photo_id, size)`` cache.
    ``publication_guard`` returns a context manager held around the cache
    write; an exception it raises is the caller's, never swallowed as a
    best-effort publish failure.
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
        published = _publish_preview(
            db, photo_id, size, data, cache_path,
            publication_guard=publication_guard,
            publish_best_effort=publish_best_effort,
        )
        return PreviewMaterialization(
            data=data, generated=True, published=published,
        )

    if cache_path and coordinate:
        result = preview_artifact_flights.run(
            os.path.abspath(cache_path), produce, consume_published,
        )
        materialized = result.value
        if materialized is None:
            # Only ``join=False`` flights come back skipped without a
            # value; this one joins, so a missing value is a producer bug.
            raise PreviewMaterializationError(
                f"preview flight for {cache_path!r} finished without a result"
            )
        return materialized
    return produce()
