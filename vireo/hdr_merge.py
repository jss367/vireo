"""Merge stationary shutter-speed brackets into a relative radiance image."""

from contextlib import ExitStack
from pathlib import Path

import numpy as np
from export import _claim_export_path
from image_loader import RAW_EXTENSIONS, load_image
from offline_cache import resolve_original_path
from PIL import Image


def _merge_frames(frames, input_size, checkpoint, progress):
    # Normalize radiance to the longest exposure. Prefer midrange samples;
    # a small weight at either endpoint keeps fully black/white areas defined.
    reference = max(photo["shutter_speed"] for photo, _ in frames)
    levels = np.arange(256, dtype=np.float32) / 255
    linear_lut = np.where(levels <= 0.04045, levels / 12.92, ((levels + 0.055) / 1.055) ** 2.4)
    radiance = weights = None
    for index, (photo, path) in enumerate(frames):
        checkpoint()
        progress(index, len(frames) + 1, "Loading " + photo["filename"])
        image = load_image(str(path), max_size=input_size)
        if image is None:
            raise ValueError(f"Could not read original {path.name}")
        with ExitStack() as stack:
            stack.enter_context(image)
            if not isinstance(image, Image.Image):
                raise ValueError("RAW HDR merging is not available yet. Use JPEG or PNG bracketed exposures.")
            pixels = np.asarray(stack.enter_context(image.convert("RGB")))
            if radiance is None:
                radiance = np.zeros(pixels.shape, dtype=np.float32)
                weights = np.zeros(pixels.shape, dtype=np.float32)
            if pixels.shape != radiance.shape:
                raise ValueError("The bracketed images must have matching dimensions")
            # Weight each channel separately so a clipped red channel can
            # recover detail even when green and blue remain well exposed.
            for top in range(0, pixels.shape[0], 128):
                rows = slice(top, top + 128)
                samples = pixels[rows]
                linear = linear_lut[samples]
                weight = np.clip(np.minimum(linear, 1 - linear), 1e-8, 1)
                radiance[rows] += linear * weight * (reference / photo["shutter_speed"])
                weights[rows] += weight
    radiance /= weights
    return radiance


def create_hdr(db, vireo_dir, *, photo_ids, destination, output_format, input_size, checkpoint, progress):
    import cv2

    photos = db.photos.get_by_ids(photo_ids)
    if set(photos) != set(photo_ids) or set(db.photo_visibility.visible_photo_ids(photo_ids)) != set(photo_ids):
        raise ValueError("Every selected photo must be available in the current workspace")
    if any(Path(p["filename"]).suffix.lower() in RAW_EXTENSIONS or p["extension"].lower() in RAW_EXTENSIONS
           for p in photos.values()):
        raise ValueError("RAW HDR merging is not available yet. Use JPEG or PNG bracketed exposures.")
    camera = db.photo_metadata.camera_data_by_ids(photo_ids)
    folders = {f["id"]: f["path"] for f in db.folders.tree()}
    first = photos[photo_ids[0]]
    destination = destination or folders.get(first["folder_id"], "")
    if not destination or not Path(destination).is_dir():
        raise ValueError("Choose an existing destination folder; the source folder may be offline.")
    folder = Path(destination)
    frames = []
    for pid in photo_ids:
        photo = {**photos[pid], **camera[pid]}
        shutter = photo["shutter_speed"]
        if not isinstance(shutter, (float, int)) or not np.isfinite(shutter) or shutter <= 0:
            raise ValueError(f"Missing or invalid shutter speed for {photo['filename']}")
        original, _ = resolve_original_path(db, photo, vireo_dir, folders)
        if not original or not Path(original).is_file():
            raise ValueError(f"Original unavailable for {photo['filename']}")
        frames.append((photo, Path(original)))
    if len({p["shutter_speed"] for p, _ in frames}) < 2:
        raise ValueError("Choose frames taken at different shutter speeds")
    for field in ("iso", "aperture"):
        if len({p[field] for p, _ in frames if p[field] is not None}) > 1:
            raise ValueError("Use a bracket captured at a fixed aperture and ISO")
    pixels = _merge_frames(frames, input_size, checkpoint, progress)
    total = len(frames) + 1
    checkpoint()
    progress(len(frames), total, "Encoding HDR merge")
    if output_format == "jpg":
        # Reinhard luminance compression followed by the sRGB transfer curve.
        luminance = pixels @ np.array([0.2126, 0.7152, 0.0722], dtype=np.float32)
        scale = 0.18 / np.exp(np.log(luminance + 1e-6).mean())
        pixels *= (scale / (1 + scale * luminance))[:, :, None]
        pixels = np.clip(pixels, 0, 1)
        pixels = np.where(pixels <= 0.0031308, 12.92 * pixels, 1.055 * pixels ** (1 / 2.4) - 0.055)
        pixels = np.rint(pixels * 255).astype(np.uint8)
    ok, encoded = cv2.imencode("." + output_format, pixels[:, :, ::-1])
    if not ok:
        raise ValueError("Could not encode HDR merge")
    checkpoint()
    path, stream = _claim_export_path(str(folder / f"{Path(first['filename']).stem}_hdr.{output_format}"))
    complete = False
    try:
        with stream:
            stream.write(encoded)
        checkpoint()
        complete = True
    finally:
        if not complete:
            Path(path).unlink(missing_ok=True)
    progress(total, total, "HDR merge saved")
    return {"path": path, "width": pixels.shape[1], "height": pixels.shape[0], "photo_count": len(frames)}
