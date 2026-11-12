#!/usr/bin/env python3
"""Download checked public NEFs and verify native decoding without JPEG fallbacks.

These regression checks also apply after switching back to official rawpy wheels.
Samples are CC0 from raw.pixls.us; no photographs are committed to the repository.
"""

import argparse
import hashlib
import json
import time
import urllib.request
from pathlib import Path

SAMPLES = (
    ("z8-he", 6616, "82df041542b8d328738443bb5ff3373b396e406ecc8cbe7dc3b35252790c53e7"),
    ("z8-he-star", 6618, "ea8b093cd11c946c5eeb501b775859e8e617ce1d79577abc41b233c339e4004c"),
    ("z9-he", 5148, "be91bac25573adf7ec6320a542105d87b4fb9c956d7b620ec90d57ac2ae6c3f0"),
    ("z9-he-star", 5147, "e113deb305d84153b26251ce259d1f839be1f36b048cbe83a4548cbda806c0f5"),
    ("z8-lossless", 6617, "c13a8675294456b9c979e6c1e556951654f35377b2870af2874bfb556f21ce40"),
)

# Baselines from the pinned official decoder; CI checks platform consistency.
# These detect changes but do not substitute for independent sensor references.
SENSOR_HASHES = {
    "z8-he": "a273a25f3b3f99be4c4ded39a8c8646464b92d29e98ceffcb47a4e0dd7b46ae1",
    "z8-he-star": "e4c31118fad3aa09ecf221fdaf1e9e3cb5c9a58e9a6859b902af8ac704bc8ab4",
    "z9-he": "d6540f56eed3c5ea46425be9823591289e6461aeb0f8824a2c9342182c768206",
    "z9-he-star": "4fe6c59e56b3b9b148d6a06e3542b19be78b14e82e6df9ce7eef62ca4a97abfa",
    "z8-lossless": "9e074e5be3f87bf32823858401d94e3ec19257e320c8413f6479ffd20c0acb57",
}


def download_sample(directory, name, identifier, digest):
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{name}.NEF"
    if path.exists():
        with path.open("rb") as source:
            if hashlib.file_digest(source, "sha256").hexdigest() == digest:
                return path
    temporary = path.with_suffix(".download")
    try:
        with (urllib.request.urlopen(f"https://raw.pixls.us/getfile.php/{identifier}/nice/", timeout=120) as response,
              temporary.open("wb") as output):
            while chunk := response.read(1024 * 1024):
                output.write(chunk)
        with temporary.open("rb") as source:
            if hashlib.file_digest(source, "sha256").hexdigest() != digest:
                raise ValueError(f"Sample checksum mismatch: {name}")
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
    return path


def check_raw(path):
    import numpy as np
    import rawpy

    started = time.perf_counter()
    # Accessing raw_image_visible forces unpacking the sensor data. Merely
    # opening the file or extracting its preview does not prove codec support.
    with rawpy.imread(str(path)) as raw:
        sensor = raw.raw_image_visible
        if sensor.dtype != np.uint16 or sensor.shape != (5520, 8280):
            raise AssertionError(f"Unexpected full-resolution Bayer data: {sensor.shape}/{sensor.dtype}")
        if len(np.unique(sensor[::16, ::16])) <= 256:
            raise AssertionError("Decoded sensor data lost RAW precision")
        fingerprint = hashlib.sha256(sensor.astype("<u2", copy=False).tobytes()).hexdigest()
        if fingerprint != SENSOR_HASHES[path.stem]:
            raise AssertionError(f"Sensor regression for {path.name}: {fingerprint}")
        rgb = raw.postprocess(output_bps=16, half_size=True, use_camera_wb=True, no_auto_bright=True,
                              demosaic_algorithm=rawpy.DemosaicAlgorithm.PPG)
        if rgb.dtype != np.uint16 or rgb.shape != (2760, 4140, 3) or np.ptp(rgb) <= 256:
            raise AssertionError(f"Demosaic did not produce a usable 16-bit image: {rgb.shape}")
        # Keep thumbnail extraction working alongside actual sensor decoding.
        if raw.extract_thumb().format != rawpy.ThumbFormat.JPEG:
            raise AssertionError("Embedded JPEG extraction stopped working")
    return {"file": path.name, "sensor_sha256": fingerprint,
            "seconds": round(time.perf_counter() - started, 3)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples", type=Path, default=Path("build/nikon-raw-samples"))
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    results = []
    for name, identifier, digest in SAMPLES:
        result = check_raw(download_sample(args.samples, name, identifier, digest))
        results.append(result)
        print(json.dumps(result), flush=True)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(results, indent=2) + "\n")


if __name__ == "__main__":
    main()
