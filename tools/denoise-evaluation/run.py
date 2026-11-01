#!/usr/bin/env python3
"""Run a read-only RAW comparison; all outputs go under --output.

Use Python 3.14+ to import Vireo's existing renderer. No application, catalog,
sidecar, or production-model writes occur. One photo and one tile at a time.
"""

# ruff: noqa: E402 -- set thread limits and the checkout import path before loading Vireo/NumPy
import argparse
import gc
import hashlib
import importlib.metadata
import json
import os
import resource
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "vireo"))
os.environ.setdefault("OMP_NUM_THREADS", "4")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "4")

import cv2
import numpy as np
from camera_denoise import resolve_profile
from float_image import FloatImage
from image_edits import EDIT_MATH_VERSION, apply_recipe_to_loaded_image
from neural import MODEL_SHA256, Denoiser
from PIL import Image, ImageCms
from raw_bridge import load_and_calibrate, sensor

VARIANTS = [
    ("off", "Denoising off", "off", 0),
    ("standard35", "Vireo Standard · 35", "standard", 35),
    ("standard70", "Vireo Standard · 70", "standard", 70),
    ("camera35", "Vireo Camera-aware · 35", "camera", 35),
    ("camera70", "Vireo Camera-aware · 70", "camera", 70),
    ("neural50", "RawNIND · 50% blend", "neural", 50),
    ("neural100", "RawNIND · 100%", "neural", 100),
]


def digest(path):
    h = hashlib.sha256()
    with open(path, "rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def write_json(path, value):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(value, indent=2) + "\n")
    tmp.replace(path)


def provenance(model):
    files = [Path(__file__).with_name(name) for name in ("run.py", "raw_bridge.py", "neural.py")]
    files += [
        ROOT / "vireo" / name
        for name in (
            "image_loader.py",
            "image_edits.py",
            "edit_recipe.py",
            "float_image.py",
            "tone.py",
            "detail.py",
            "detail_backend.py",
            "camera_denoise.py",
            "data/denoise/noiseprofiles.json",
        )
    ]
    return {
        "model_sha256": digest(model),
        "model": "RawNIND UtNet2 Bayer",
        "runtime": "ONNX Runtime CPU, 4 threads, sequential tiles",
        "model_terms": "GPL-3.0; model not bundled with this tool",
        "vireo_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "edit_math_version": EDIT_MATH_VERSION,
        "code_sha256": {str(p.relative_to(ROOT)): digest(p) for p in files},
        "packages": {
            name: importlib.metadata.version(name)
            for name in ("numpy", "rawpy", "pillow", "onnxruntime", "scipy", "opencv-python-headless")
        },
        "python": sys.version,
    }


def regions(photo, width, height):
    """Initial aids only; gallery lets the reviewer inspect any native pixel."""
    subject = photo.get("review_center", [0.5, 0.5])

    def box(cx, cy, side=1024):
        x = max(0, min(width - side, int(cx * width - side / 2)))
        y = max(0, min(height - side, int(cy * height - side / 2)))
        return [x, y, min(width, x + side), min(height, y + side)]

    return {"subject": box(*subject), "background": box(0.15, 0.15)}


def save_images(image, folder, name, boxes):
    profile = ImageCms.ImageCmsProfile(ImageCms.createProfile("sRGB")).tobytes()
    # Full-resolution and crop comparisons are lossless; previews are navigation aids.
    with image.to_pil() as pil:
        pil.save(folder / f"{name}.png", compress_level=1, icc_profile=profile)
        for region, box in boxes.items():
            with pil.crop(box) as crop:
                crop.save(folder / f"{name}-{region}.png", icc_profile=profile)
        pil.thumbnail((1600, 1200), Image.Resampling.LANCZOS)
        pil.save(folder / f"{name}-preview.jpg", quality=95, icc_profile=profile)


def run_photo(photo, output, denoiser, prov, exposure, variants=None):
    variants = VARIANTS if variants is None else variants
    source = Path(photo["path"])
    stat = source.stat()
    fingerprint = {
        "source_sha256": digest(source),
        "provenance": prov,
        "exposure": exposure,
        "variants": variants,
        "photo": photo,
    }
    folder = output / str(photo["id"])
    result_path = folder / "result.json"
    if result_path.exists():
        previous = json.loads(result_path.read_text())
        if previous["fingerprint"] != json.loads(json.dumps(fingerprint)):
            raise ValueError(f"Existing result differs: use a fresh output directory for {photo['id']}")
        for name, *_ in variants:
            for suffix in (".png", "-subject.png", "-background.png", "-preview.jpg"):
                if not (folder / (name + suffix)).is_file():
                    raise ValueError("Incomplete cached result; use a fresh output directory")
        print(f"{photo['id']}: validated cached result", flush=True)
        return previous
    folder.mkdir(exist_ok=True)
    began = time.perf_counter()
    mosaic, cell = sensor(source)
    baseline, matrix, calibration = load_and_calibrate(source)
    if baseline.pixels.shape[:2] != mosaic.shape:
        raise ValueError("Sensor and rendered geometry differ")
    boxes = regions(photo, *baseline.size)
    decode_seconds = time.perf_counter() - began
    neural, inference = denoiser.run(mosaic, cell)
    del mosaic
    for y in range(0, neural.shape[0], 128):
        neural[y : y + 128] = neural[y : y + 128] @ matrix
    timings = {}
    for name, label, method, amount in variants:
        start = time.perf_counter()
        if method == "neural":
            pixels = np.empty_like(neural)
            for y in range(0, len(neural), 128):
                pixels[y : y + 128] = baseline.pixels[y : y + 128] + (
                    neural[y : y + 128] - baseline.pixels[y : y + 128]
                ) * (amount / 100)
            source_image = FloatImage(pixels)
        else:
            source_image = baseline
        adjustments = {"exposure": exposure}
        if method in ("camera", "standard"):
            adjustments.update(noise_reduction=amount, denoise_mode=method)
        rendered = apply_recipe_to_loaded_image(
            source_image,
            {"version": 1, "adjustments": adjustments},
            native_size=baseline.size,
            camera_metadata=photo,
        )
        render_seconds = time.perf_counter() - start
        save_start = time.perf_counter()
        save_images(rendered, folder, name, boxes)
        timings[name] = {"render_seconds": render_seconds, "encode_seconds": time.perf_counter() - save_start}
        print(f"  {label}: render {render_seconds:.2f}s", flush=True)
        rendered.close()
        if method == "neural":
            source_image.close()
        gc.collect()
    baseline.close()
    del neural
    after = source.stat()
    if (stat.st_mtime_ns, stat.st_size) != (after.st_mtime_ns, after.st_size) or digest(source) != fingerprint[
        "source_sha256"
    ]:
        raise ValueError("Source changed during the experiment")
    result = {
        "photo_id": photo["id"],
        "fingerprint": fingerprint,
        "calibration": calibration,
        "camera_noise_profile": resolve_profile(photo),
        "regions": boxes,
        "decode_and_calibration_seconds": decode_seconds,
        "neural": inference,
        "variants": timings,
        "total_seconds": time.perf_counter() - began,
        "process_peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        * (1 if sys.platform == "darwin" else 1024),
        "source_unchanged": True,
        "photographer_judgment": None,
    }
    write_json(result_path, result)
    gc.collect()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selection", type=Path, required=True)
    parser.add_argument("--model", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--only", type=int)
    parser.add_argument(
        "--pair-only", action="store_true", help="Render frozen Camera-aware 70 and neural 50 comparison only"
    )
    parser.add_argument("--exposure", type=float, default=0)
    args = parser.parse_args()
    if digest(args.model) != MODEL_SHA256:
        raise ValueError("Model does not match the pinned RawNIND Bayer weights")
    if not -2 <= args.exposure <= 2:
        raise ValueError("Experiment exposure must be between -2 and +2 stops")
    cv2.setNumThreads(4)
    args.output.mkdir(parents=True, exist_ok=True)
    selection = json.loads(args.selection.read_text())
    prov = provenance(args.model)
    write_json(args.output / "provenance.json", prov)
    denoiser = Denoiser(args.model)
    for photo in selection["photos"]:
        if args.only and photo["id"] != args.only:
            continue
        print(f"Photo {photo['id']}, ISO {photo['iso']}, {photo['species']}", flush=True)
        run_photo(
            photo,
            args.output,
            denoiser,
            prov,
            photo.get("exposure_ev", args.exposure),
            [v for v in VARIANTS if v[0] in ("camera70", "neural50")] if args.pair_only else None,
        )


if __name__ == "__main__":
    main()
