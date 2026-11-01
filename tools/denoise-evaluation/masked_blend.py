"""Offline subject-preserving blend of the exact previously reviewed PNGs.

Blend in display-referred sRGB, matching the placement of Vireo's local-detail
blend, but using saved 8-bit endpoints. This is a compositing feasibility test,
not a new RAW render or production integration. Originals and catalog are untouched.
"""

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
from PIL import Image
from scipy.ndimage import binary_erosion, binary_fill_holes, distance_transform_edt


def digest(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for b in iter(lambda: f.read(1024 * 1024), b""):
            h.update(b)
    return h.hexdigest()


def protected_weight(mask, guard=8, transition=16):
    if guard < 0 or transition <= 0:
        raise ValueError("Invalid blend margin")
    core = binary_fill_holes(mask > 127)
    if not core.any() or core.all():
        raise ValueError("Mask must contain both subject and background")
    distance = distance_transform_edt(~core).astype(np.float32)
    t = np.clip((distance - guard) / transition, 0, 1)
    weight = 1 - t * t * (3 - 2 * t)
    return weight, core


def composite(subject, background, weight):
    if subject.shape != background.shape or subject.shape[:2] != weight.shape:
        raise ValueError("Blend geometry mismatch")
    if not np.isfinite(weight).all() or weight.min() < 0 or weight.max() > 1:
        raise ValueError("Invalid mask weights")
    out = np.empty_like(subject)
    for y in range(0, len(subject), 128):
        a = subject[y : y + 128].astype(np.float32)
        b = background[y : y + 128].astype(np.float32)
        w = weight[y : y + 128, :, None]
        out[y : y + 128] = np.clip(np.rint(a * w + b * (1 - w)), 0, 255).astype(np.uint8)
    return out


def crop_box(center, size):
    w, h = size
    x = max(0, min(w - 1024, int(center[0] * w - 512)))
    y = max(0, min(h - 1024, int(center[1] * h - 512)))
    return (x, y, x + 1024, y + 1024)


def render_photo(photo, source, mask_path, output):
    pid = photo["id"]
    folder = output / str(pid)
    folder.mkdir(parents=True, exist_ok=False)
    first = source / str(pid) / "camera70.png"
    second = source / str(pid) / "neural50.png"
    before = {str(p): digest(p) for p in [first, second, mask_path]}
    with Image.open(first) as im:
        size = im.size
        profile = im.info["icc_profile"]
        subject = np.array(im.convert("RGB"))
    with Image.open(second) as im:
        other_profile = im.info["icc_profile"]
        # ICC header bytes 24:36 are creation time, not the color transform.
        if im.size != size or other_profile[:24] + other_profile[36:] != profile[:24] + profile[36:]:
            raise ValueError("Endpoint geometry or color profile mismatch")
        background = np.array(im.convert("RGB"))
    with Image.open(mask_path) as im:
        if abs(im.width / im.height - size[0] / size[1]) / (size[0] / size[1]) > 0.01:
            raise ValueError("Mask aspect mismatch")
        mask = np.array(im.convert("L").resize(size, Image.Resampling.NEAREST))
    weight, core = protected_weight(mask)
    output_pixels = composite(subject, background, weight)
    subject_error = int(np.abs(output_pixels[core].astype("int16") - subject[core]).max())
    far = weight == 0
    background_error = int(np.abs(output_pixels[far].astype("int16") - background[far]).max())
    assert subject_error == background_error == 0
    # Edge view uses the subject boundary nearest the already-chosen review center.
    yy, xx = np.nonzero(core & ~binary_erosion(core))
    cx, cy = photo["review_center"][0] * size[0], photo["review_center"][1] * size[1]
    idx = np.argmin((xx - cx) ** 2 + (yy - cy) ** 2)
    edge_center = [float(xx[idx] / size[0]), float(yy[idx] / size[1])]
    boxes = {
        "subject": crop_box(photo["review_center"], size),
        "edge": crop_box(edge_center, size),
        "background": crop_box([0.15, 0.15], size),
    }
    result_image = Image.fromarray(output_pixels)
    result_image.save(folder / "protected.png", compress_level=1, icc_profile=profile)
    for name, box in boxes.items():
        result_image.crop(box).save(folder / f"protected-{name}.png", icc_profile=profile)
    result_image.thumbnail((1600, 1200))
    result_image.save(folder / "protected-preview.jpg", quality=95, icc_profile=profile)
    for method in ["camera70", "neural50"]:
        with Image.open(source / str(pid) / (method + ".png")) as im:
            im.crop(boxes["edge"]).save(folder / (method + "-edge.png"), icc_profile=profile)
    Image.fromarray(core.astype("uint8") * 255).save(folder / "filled-subject-mask.png")
    Image.fromarray(np.rint(weight * 255).astype("uint8")).save(folder / "protection-weight.png")
    # Inspection overlay at the same native edge crop, red is protected subject.
    box = boxes["edge"]
    x, y, ex, ey = box
    arr = subject[y:ey, x:ex].copy()
    m = weight[y:ey, x:ex] > 0
    arr[m] = (arr[m] * 0.6 + np.array([230, 70, 60]) * 0.4).astype("uint8")
    Image.fromarray(arr).save(folder / "edge-mask-overlay.png")
    for path, h in before.items():
        assert digest(path) == h
    report = {
        "photo_id": pid,
        "photo": photo,
        "source_hashes": before,
        "source_result_sha256": digest(source / str(pid) / "result.json"),
        "mask_path": str(mask_path),
        "guard_native_pixels": 8,
        "outward_transition_native_pixels": 16,
        "fill_enclosed_mask_holes": True,
        "subject_mask_pixels": int(core.sum()),
        "filled_hole_pixels_native": int((core & (mask <= 127)).sum()),
        "subject_core_max_error_8bit": subject_error,
        "far_background_max_error_8bit": background_error,
        "source_images_and_mask_unchanged": True,
        "edge_center": edge_center,
        "boxes": boxes,
        "blend_space": "display-referred sRGB from saved lossless 8-bit renders; final rounding only in transition",
        "limitation": "Mask quality and transition halos require human review. This preserves the baseline pixels inside the mask, not proof of true detail fidelity or correct segmentation.",
    }
    (folder / "result.json").write_text(json.dumps(report, indent=2) + "\n")
    print(pid, "subject/background max error", subject_error, background_error, flush=True)


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--selection", type=Path, required=True)
    p.add_argument("--source", type=Path, required=True)
    p.add_argument("--masks", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    a = p.parse_args()
    for photo in json.loads(a.selection.read_text())["photos"]:
        render_photo(photo, a.source, a.masks / f"{photo['id']}-mask.png", a.output)
