#!/usr/bin/env python3
"""Compare encoder precision on the overlapping Extract features workload.

Run each arm in a fresh process. See the reproduction commands in
docs/investigations/subject-feature-extraction-throughput.md.
"""

import argparse
import json
import resource
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

import numpy as np
from PIL import Image

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "vireo"))

import dino_embed
import masking
import onnx_runtime
from profile_subject_features import analyze, sample_photos
from quality import compute_all_quality_features


def measure_photo(row, pool, output, encoder_name):
    phases = {}

    def timed(name, func, *values, **kwargs):
        start = time.perf_counter()
        result = func(*values, **kwargs)
        phases[name] = time.perf_counter() - start
        return result

    start = time.perf_counter()
    cpu = time.process_time()
    proxy = timed("decode", masking.render_proxy, str(Path(row["folder"]) / row["filename"]), longest_edge=2048)
    if proxy is None:
        raise ValueError(f"unreadable photo {row['id']}")
    box = {key: row[key] for key in ("x", "y", "w", "h")}
    mask = timed("segment", masking.generate_mask, proxy, box, variant="sam2-large")
    if mask is None:
        raise ValueError(f"segmentation failed for photo {row['id']}")

    quality = pool.submit(
        timed, "analyze", analyze, masking.crop_completeness, compute_all_quality_features, proxy, mask
    )

    def embed():
        crop = masking.crop_subject(proxy, mask, margin=0.15)
        return dino_embed.embed_batch([crop, proxy] if crop else [proxy], variant="vit-b14")

    timed("embed", embed)
    completeness, features = quality.result()
    timed("store", masking.save_mask, mask, str(output), row["id"], encoder_name)
    return dict(
        id=row["id"],
        wall=time.perf_counter() - start,
        cpu=time.process_time() - cpu,
        phases=phases,
        completeness=completeness,
        features=features,
    )


def compare(reference, candidate):
    arms = []
    for directory in (reference, candidate):
        data = json.loads((directory / "metrics.json").read_text())
        rows = {row["id"]: row for row in data["photos"]}
        wall = sum(row["wall"] for row in rows.values())
        cpu = sum(row["cpu"] for row in rows.values())
        phases = {
            phase: statistics.mean(row["phases"][phase] for row in rows.values())
            for phase in next(iter(rows.values()))["phases"]
        }
        print(
            f"{directory.name}: {wall:.2f} s, {60 * len(rows) / wall:.2f} photos/min, "
            f"{cpu:.1f} CPU s, {data['peak_rss_bytes'] / 2**30:.2f} GiB peak RSS"
        )
        print("Mean phase seconds:", {key: round(value, 3) for key, value in phases.items()})
        arms.append((rows, Path(data["encoder"]).stem))
    (original, fp_name), (quantized, int_name) = arms
    if original.keys() != quantized.keys():
        raise ValueError("runs contain different photo IDs")

    print(
        "photo_id,mask_iou,reference_subject_tenengrad,candidate_subject_tenengrad,"
        "reference_bg_separation,candidate_bg_separation"
    )
    for photo_id, row in original.items():
        with Image.open(reference / f"{photo_id}.{fp_name}.png") as img:
            left = np.asarray(img, dtype=bool)
        with Image.open(candidate / f"{photo_id}.{int_name}.png") as img:
            right = np.asarray(img, dtype=bool)
        if left.shape != right.shape:
            raise ValueError(f"mask dimensions differ for {photo_id}")
        union = np.count_nonzero(left | right)
        iou = np.count_nonzero(left & right) / union if union else 1.0
        ref, cand = row["features"], quantized[photo_id]["features"]
        print(
            photo_id,
            iou,
            ref["subject_tenengrad"],
            cand["subject_tenengrad"],
            ref["bg_separation"],
            cand["bg_separation"],
            sep=",",
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    comparison = commands.add_parser("compare")
    comparison.add_argument("reference", type=Path)
    comparison.add_argument("candidate", type=Path)
    run = commands.add_parser("run")
    run.add_argument("--db", type=Path, required=True)
    run.add_argument("--encoder", type=Path, required=True)
    run.add_argument("--decoder", type=Path, required=True)
    run.add_argument("--photo-ids", type=int, nargs=2, required=True)
    run.add_argument("--sample", type=int, default=10)
    run.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "compare":
        compare(args.reference, args.candidate)
        return
    if args.sample < 1:
        parser.error("--sample must be positive")
    rows = sample_photos(args.db, *args.photo_ids, "sam2-large", args.sample, seed=1)
    if not rows:
        parser.error("no stored sam2-large prompts in this range")
    args.output.mkdir(parents=True, exist_ok=False)

    providers = ["CPUExecutionProvider"]
    encoder = onnx_runtime.create_session(str(args.encoder), providers=providers)
    decoder = onnx_runtime.create_session(str(args.decoder), providers=providers)
    dino_embed._get_dinov2_session("vit-b14")

    results = []
    with (
        patch.object(masking, "_get_sam2_sessions", return_value=(encoder, decoder, None)),
        ThreadPoolExecutor(max_workers=1) as pool,
    ):
        for row in rows:
            results.append(measure_photo(row, pool, args.output, args.encoder.stem))
            print(row["id"], flush=True)
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    (args.output / "metrics.json").write_text(
        json.dumps(
            {
                "encoder": str(args.encoder),
                "decoder": str(args.decoder),
                "peak_rss_bytes": rss if sys.platform == "darwin" else rss * 1024,
                "photos": results,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
