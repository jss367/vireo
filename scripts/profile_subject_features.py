#!/usr/bin/env python3
"""Time each phase of the Extract features stage on real catalog photos.

Read-only: the catalog is opened with SQLite ``mode=ro`` and never through
``Database`` (its startup repairs write). Each sampled photo must already have
a mask for the variant, whose stored prompt box is reused, so the timings cover
the same work the stage did. Masks are written to a temporary directory only.

    python scripts/profile_subject_features.py --db ~/.vireo/vireo.db \\
        --photo-ids 227372-229920 --sam2 sam2-large --dinov2 vit-b14 \\
        --longest-edge 2048 --sample 20

Phases match the stage's ``phase_seconds``: decode (``render_proxy``), the
SAM2 encoder and decoder, the quality analysis, the DINOv2 subject and global
embeddings, and the mask PNG write. Run it on an otherwise idle machine; the
ONNX encoder shares cores with everything else.
"""
import argparse
import os
import random
import sqlite3
import statistics
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "vireo"))


def sample_photos(db_path, first, last, variant, sample, seed):
    conn = sqlite3.connect(f"file:{os.path.expanduser(db_path)}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT p.id, p.filename, f.path AS folder, pm.prompt_x AS x, pm.prompt_y AS y,"
        " pm.prompt_w AS w, pm.prompt_h AS h"
        " FROM photos p JOIN folders f ON f.id = p.folder_id"
        " JOIN photo_masks pm ON pm.photo_id = p.id AND pm.variant = ?"
        " WHERE p.id BETWEEN ? AND ? ORDER BY p.id",
        (variant, first, last),
    ).fetchall()
    conn.close()
    random.seed(seed)
    return random.sample(rows, min(sample, len(rows)))


def analyze(crop_completeness, quality_features, proxy, mask):
    """The stage's analysis step: crop completeness, then the quality features."""
    return crop_completeness(mask), quality_features(proxy, mask)


def profile(rows, sam2, dinov2, longest_edge):
    import dino_embed
    import masking
    import onnx_runtime
    from quality import compute_all_quality_features

    encoder, decoder = masking._get_sam2_sessions(sam2)
    dino_embed._get_dinov2_session(dinov2)
    timings = {}

    def timed(phase, fn, *args, **kwargs):
        start = time.perf_counter()
        value = fn(*args, **kwargs)
        timings.setdefault(phase, []).append(time.perf_counter() - start)
        return value

    masks_dir = tempfile.mkdtemp(prefix="vireo-profile-masks-")
    for row in rows:
        start = time.perf_counter()
        proxy = timed("decode", masking.render_proxy,
                      os.path.join(row["folder"], row["filename"]), longest_edge=longest_edge)
        if proxy is None:
            print(f"{row['id']} {row['filename']}: unreadable, skipped")
            continue
        width, height = proxy.size
        tensor = onnx_runtime.preprocess_image(
            proxy, size=(masking.SAM2_INPUT_SIZE, masking.SAM2_INPUT_SIZE),
            mean=masking._IMAGENET_MEAN, std=masking._IMAGENET_STD,
        )
        encoded = timed("sam2_encoder", encoder.run, None, {encoder.get_inputs()[0].name: tensor})
        coords, labels = masking._sam2_box_prompt(
            row["x"] * width, row["y"] * height, row["w"] * width, row["h"] * height,
            width, height,
        )
        feeds = masking._sam2_decoder_inputs(decoder, encoded, coords, labels, height, width)
        decoded = timed("sam2_decoder", decoder.run, None, feeds)
        mask = masking._best_sam2_mask(decoded[0], decoded[1], height, width)
        timed("analyze", analyze, masking.crop_completeness, compute_all_quality_features, proxy, mask)
        crop = masking.crop_subject(proxy, mask, margin=0.15)
        timed("embed", dino_embed.embed_batch, [crop, proxy] if crop else [proxy], variant=dinov2)
        timed("store_png", masking.save_mask, mask, masks_dir, row["id"], sam2)
        timings.setdefault("total", []).append(time.perf_counter() - start)
        print(f"{row['id']} {row['filename']} {width}x{height} {timings['total'][-1]:.2f}s", flush=True)
    return timings


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--db", default="~/.vireo/vireo.db")
    parser.add_argument("--photo-ids", required=True, help="inclusive id range, e.g. 227372-229920")
    parser.add_argument("--sam2", default="sam2-large")
    parser.add_argument("--dinov2", default="vit-b14")
    parser.add_argument("--longest-edge", type=int, default=1536)
    parser.add_argument("--sample", type=int, default=20)
    parser.add_argument("--seed", type=int, default=1)
    args = parser.parse_args()
    first, last = (int(part) for part in args.photo_ids.split("-"))
    rows = sample_photos(args.db, first, last, args.sam2, args.sample, args.seed)
    if not rows:
        sys.exit(f"no photos in {args.photo_ids} have a {args.sam2} mask")
    timings = profile(rows, args.sam2, args.dinov2, args.longest_edge)
    total = statistics.mean(timings["total"])
    print(f"\n{'phase':14s} {'mean s':>7s} {'median s':>8s} {'share':>6s}")
    for phase, values in timings.items():
        mean = statistics.mean(values)
        print(f"{phase:14s} {mean:7.3f} {statistics.median(values):8.3f} {mean / total:6.1%}")


if __name__ == "__main__":
    main()
