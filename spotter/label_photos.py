"""Batch classify photos and write predictions to XMP sidecars.

Usage:
    python spotter/label_photos.py --folder /path/to/photos [--write] [--threshold 0.4]
"""

import argparse
import logging
import sys
import os
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lr-migration"))

from classifier import Classifier
from image_loader import load_image, SUPPORTED_EXTENSIONS
from xmp_writer import write_xmp_sidecar

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
)
log = logging.getLogger(__name__)


def run(
    folder,
    labels=None,
    model_str="ViT-B-16",
    pretrained_str="/tmp/bioclip_model/open_clip_pytorch_model.bin",
    write=False,
    threshold=0.4,
    recursive=True,
):
    """Scan a folder, classify images, and write/report XMP sidecars.

    Args:
        folder: path to folder of images
        labels: list of species labels (None for TreeOfLife full taxonomy)
        model_str: BioCLIP model string
        pretrained_str: path to model weights
        write: if True, write XMP sidecars; if False, dry-run
        threshold: minimum confidence to include a prediction
        recursive: if True, scan subfolders

    Returns:
        dict with stats
    """
    clf = Classifier(labels=labels, model_str=model_str, pretrained_str=pretrained_str)

    folder_path = Path(folder)
    if recursive:
        image_files = sorted(
            f
            for f in folder_path.rglob("*")
            if f.suffix.lower() in SUPPORTED_EXTENSIONS and not f.name.startswith(".")
        )
    else:
        image_files = sorted(
            f
            for f in folder_path.iterdir()
            if f.suffix.lower() in SUPPORTED_EXTENSIONS and not f.name.startswith(".")
        )

    stats = {
        "images_found": len(image_files),
        "images_processed": 0,
        "images_skipped": 0,
        "images_failed": 0,
        "sidecars_written": 0,
    }

    log.info("Found %d images in %s", len(image_files), folder)

    for i, image_path in enumerate(image_files):
        # Load and resize image
        img = load_image(str(image_path))
        if img is None:
            stats["images_failed"] += 1
            continue

        # Re-save as temp JPEG since image was loaded/resized by load_image
        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp:
            tmp_path = tmp.name
            img.save(tmp_path, quality=85)

        try:
            predictions = clf.classify(tmp_path, threshold=threshold)
        except Exception:
            log.warning("Classification failed for %s", image_path, exc_info=True)
            stats["images_failed"] += 1
            continue
        finally:
            os.unlink(tmp_path)

        stats["images_processed"] += 1

        if not predictions:
            stats["images_skipped"] += 1
            log.debug("No predictions above threshold for %s", image_path)
            continue

        top = predictions[0]
        flat_keywords = set()
        for pred in predictions:
            flat_keywords.add(pred["auto_tag"])
            flat_keywords.add(pred["confidence_tag"])

        xmp_path = image_path.with_suffix(".xmp")

        if write:
            try:
                write_xmp_sidecar(
                    str(xmp_path),
                    flat_keywords=flat_keywords,
                    hierarchical_keywords=set(),
                )
                stats["sidecars_written"] += 1
            except Exception:
                log.warning("Failed to write sidecar for %s", image_path, exc_info=True)
        else:
            log.info(
                "[DRY RUN] %s -> %s (%.2f)",
                image_path.name,
                top["species"],
                top["score"],
            )

        if (i + 1) % 100 == 0:
            log.info("Progress: %d/%d images processed", i + 1, len(image_files))

    log.info("--- Summary ---")
    log.info("Images found:      %d", stats["images_found"])
    log.info("Images processed:  %d", stats["images_processed"])
    log.info("Images skipped:    %d", stats["images_skipped"])
    log.info("Images failed:     %d", stats["images_failed"])
    log.info("Sidecars written:  %d", stats["sidecars_written"])

    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Auto-label wildlife photos using BioCLIP."
    )
    parser.add_argument(
        "--folder",
        required=True,
        help="Path to folder of images to classify",
    )
    parser.add_argument(
        "--labels-file",
        help="Path to text file with one label per line (omit for TreeOfLife full taxonomy)",
    )
    parser.add_argument(
        "--model-weights",
        default="/tmp/bioclip_model/open_clip_pytorch_model.bin",
        help="Path to BioCLIP model weights",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.4,
        help="Minimum confidence score to write a tag (default: 0.4)",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Actually write XMP sidecars (default is dry-run)",
    )
    parser.add_argument(
        "--no-recursive",
        action="store_true",
        help="Don't scan subfolders",
    )
    args = parser.parse_args()

    labels = None
    if args.labels_file:
        with open(args.labels_file) as f:
            labels = [line.strip() for line in f if line.strip()]
        log.info("Loaded %d labels from %s", len(labels), args.labels_file)

    run(
        folder=args.folder,
        labels=labels,
        pretrained_str=args.model_weights,
        write=args.write,
        threshold=args.threshold,
        recursive=not args.no_recursive,
    )


if __name__ == "__main__":
    main()
