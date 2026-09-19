"""Stitch edited photos into a separate panorama without changing sources."""

import contextlib
import os

import numpy as np
from export import _claim_export_path, _get_photo_exif_data, load_export_image
from PIL import Image

MAX_PHOTOS = 12
INPUT_SIZES = (2048, 4096)


def validate_options(body):
    """Validate the entire selection: silently dropping a frame is unsafe."""
    if not isinstance(body, dict):
        raise ValueError("Request body must be a JSON object")
    ids = body.get("photo_ids")
    if not isinstance(ids, list) or not 2 <= len(ids) <= MAX_PHOTOS:
        raise ValueError(f"Select between 2 and {MAX_PHOTOS} overlapping photos")
    if any(type(pid) is not int or pid <= 0 for pid in ids):
        raise ValueError("photo_ids must contain positive integers")
    if len(set(ids)) != len(ids):
        raise ValueError("Select distinct photos")
    destination = body.get("destination", "")
    if not isinstance(destination, str):
        raise ValueError("Destination must be a folder path")
    destination = destination.strip()
    if destination and not os.path.isabs(destination):
        raise ValueError("Destination must be an absolute folder path")
    output_format = body.get("format", "jpg")
    if output_format not in ("jpg", "png"):
        raise ValueError("Format must be jpg or png")
    size = body.get("input_size", 2048)
    if type(size) is not int or size not in INPUT_SIZES:
        raise ValueError("Input size must be 2048 or 4096")
    reveal = body.get("reveal", True)
    if not isinstance(reveal, bool):
        raise ValueError("reveal must be a boolean")
    return dict(photo_ids=ids, destination=destination, output_format=output_format, input_size=size, reveal=reveal)


def _check_status(status):
    if status == 0:
        return
    messages = {
        1: "Not enough matching detail. Select photos with more overlap (about 30–50%).",
        2: "Could not align these photos. Use overlapping views taken from the same position.",
        3: "Could not align the camera views. Try a smaller set with more overlap.",
    }
    raise ValueError(messages.get(status, "Panorama stitching failed"))


def create_panorama(db, vireo_dir, *, photo_ids, destination, output_format, input_size, config, checkpoint, progress):
    """Render, align, blend and save; checkpoints surround native stitching calls.

    OpenCV can discard disconnected frames even on success. Require the whole
    selection before composing so the saved image never silently omits a photo.
    """
    import cv2

    photos = db.get_photos_by_ids(photo_ids)
    folders = {f["id"]: f["path"] for f in db.get_folder_tree()}
    if any(pid not in photos or photos[pid]["folder_id"] not in folders for pid in photo_ids):
        raise ValueError("A selected photo is no longer available in this workspace")
    first = photos[photo_ids[0]]
    destination = destination or folders[first["folder_id"]]
    # Do not recreate a disconnected volume's path on the local filesystem.
    if not os.path.isdir(destination):
        raise ValueError("The destination folder is unavailable. Choose an existing folder.")

    recipes = db.get_photo_edit_recipes(photo_ids)
    exif = _get_photo_exif_data(db, photo_ids)
    images = []
    total = len(photo_ids) + 3
    for i, pid in enumerate(photo_ids):
        checkpoint()
        photo = photos[pid]
        progress(i, total, "Loading " + photo["filename"])
        try:
            with load_export_image(
                photo,
                vireo_dir,
                folders,
                recipe=recipes.get(pid),
                exif_data=exif.get(pid),
                max_size=input_size,
                wc_max=config.get("working_copy_max_size", 4096),
                developed_dir=config.get("darktable_output_dir", "") or "",
            ) as img:
                # The renderer may return a full-size developed image.
                img.thumbnail((input_size, input_size), Image.Resampling.LANCZOS)
                images.append(cv2.cvtColor(np.asarray(img.convert("RGB")), cv2.COLOR_RGB2BGR))
        except (OSError, ValueError) as exc:
            raise ValueError(f"{photo['filename']}: {exc}") from exc

    checkpoint()
    stitcher = cv2.Stitcher_create(cv2.Stitcher_PANORAMA)
    try:
        progress(len(images), total, "Aligning photos")
        _check_status(stitcher.estimateTransform(images))
        included = set(stitcher.component())
        if included != set(range(len(images))):
            missing = [photos[pid]["filename"] for i, pid in enumerate(photo_ids) if i not in included]
            raise ValueError(
                "Could not match all selected photos: "
                + ", ".join(missing)
                + ". Select a connected set of overlapping views."
            )
        checkpoint()
        progress(len(images) + 1, total, "Blending panorama")
        status, pixels = stitcher.composePanorama()
        _check_status(status)
        checkpoint()
        if pixels is None or not pixels.size:
            raise ValueError("Stitching produced an empty panorama")
        rgb = cv2.cvtColor(pixels, cv2.COLOR_BGR2RGB)
        output = Image.fromarray(rgb)
    except cv2.error as exc:
        raise ValueError("Could not stitch these photos. Try fewer photos with more overlap.") from exc

    out_path = None
    try:
        with output:
            checkpoint()
            progress(total - 1, total, "Saving panorama")
            # Exclusive creation handles concurrent jobs and existing outputs.
            stem = os.path.splitext(os.path.basename(first["filename"]))[0]
            out_path, stream = _claim_export_path(os.path.join(destination, f"{stem}_panorama.{output_format}"))
            with stream:
                if output_format == "jpg":
                    output.save(stream, format="JPEG", quality=95, subsampling=0)
                else:
                    output.save(stream, format="PNG")
            checkpoint()
            width, height = output.size
    except BaseException:
        if out_path:
            with contextlib.suppress(OSError):
                os.unlink(out_path)
        raise
    progress(total, total, "Panorama saved")
    return {"path": out_path, "width": width, "height": height, "photo_count": len(photo_ids)}
