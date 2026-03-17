"""Scan photos, classify, compare to existing XMP keywords, generate review data.

Usage:
    python auto-labeler/analyze.py --folder /path/to/photos --labels-file labels.txt
"""

import argparse
import json
import logging
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lr-migration'))

from classifier import Classifier
from compare import read_xmp_keywords, categorize
from image_loader import load_image, SUPPORTED_EXTENSIONS

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
)
log = logging.getLogger(__name__)


def analyze(folder, output_dir, labels, model_str='ViT-B-16',
            pretrained_str='/tmp/bioclip_model/open_clip_pytorch_model.bin',
            threshold=0.4, thumbnail_size=400, recursive=True):
    """Scan a folder, classify images, compare to existing keywords, write results.

    Args:
        folder: path to image folder
        output_dir: path to output directory for results.json and thumbnails/
        labels: list of species labels for the classifier and vocabulary
        model_str: BioCLIP model string
        pretrained_str: path to model weights
        threshold: minimum confidence score
        thumbnail_size: max dimension for thumbnails
        recursive: scan subfolders
    """
    os.makedirs(output_dir, exist_ok=True)
    thumb_dir = os.path.join(output_dir, "thumbnails")
    os.makedirs(thumb_dir, exist_ok=True)

    clf = Classifier(labels=labels, model_str=model_str, pretrained_str=pretrained_str)
    labels_vocab = set(labels)

    folder_path = Path(folder)
    if recursive:
        image_files = sorted(
            f for f in folder_path.rglob('*')
            if f.suffix.lower() in SUPPORTED_EXTENSIONS and not f.name.startswith('.')
        )
    else:
        image_files = sorted(
            f for f in folder_path.iterdir()
            if f.suffix.lower() in SUPPORTED_EXTENSIONS and not f.name.startswith('.')
        )

    log.info("Found %d images in %s", len(image_files), folder)

    photos = []
    stats = {'total': len(image_files), 'new': 0, 'refinement': 0,
             'disagreement': 0, 'match': 0, 'failed': 0, 'below_threshold': 0}

    for i, image_path in enumerate(image_files):
        img = load_image(str(image_path))
        if img is None:
            stats['failed'] += 1
            continue

        # Generate thumbnail
        thumb_path = os.path.join(thumb_dir, image_path.stem + ".jpg")
        thumb = img.copy()
        thumb.thumbnail((thumbnail_size, thumbnail_size))
        thumb.save(thumb_path, quality=85)

        # Classify via temp file
        with tempfile.NamedTemporaryFile(suffix='.jpg', delete=False) as tmp:
            tmp_path = tmp.name
            img.save(tmp_path, quality=85)

        try:
            predictions = clf.classify(tmp_path, threshold=threshold)
        except Exception:
            log.warning("Classification failed for %s", image_path, exc_info=True)
            stats['failed'] += 1
            continue
        finally:
            os.unlink(tmp_path)

        if not predictions:
            stats['below_threshold'] += 1
            continue

        top = predictions[0]

        # Read existing XMP keywords
        xmp_path = image_path.with_suffix('.xmp')
        existing = read_xmp_keywords(str(xmp_path))

        # Categorize
        category = categorize(top['species'], existing, labels_vocab)
        stats[category] += 1

        # Skip matches — only show differences
        if category == 'match':
            continue

        # Filter existing to just species for display
        existing_species = [kw for kw in existing
                           if any(kw.lower() == l.lower() for l in labels_vocab)]

        photos.append({
            'filename': image_path.name,
            'image_path': str(image_path),
            'xmp_path': str(xmp_path),
            'existing_species': existing_species,
            'prediction': top['species'],
            'confidence': round(top['score'], 4),
            'category': category,
            'status': 'pending',
        })

        if (i + 1) % 100 == 0:
            log.info("Progress: %d/%d images", i + 1, len(image_files))

    results = {
        'folder': str(folder),
        'settings': {
            'threshold': threshold,
            'thumbnail_size': thumbnail_size,
        },
        'stats': stats,
        'photos': photos,
    }

    results_path = os.path.join(output_dir, "results.json")
    with open(results_path, 'w') as f:
        json.dump(results, f, indent=2)

    log.info("--- Analysis Summary ---")
    log.info("Total images:   %d", stats['total'])
    log.info("New:            %d", stats['new'])
    log.info("Refinements:    %d", stats['refinement'])
    log.info("Disagreements:  %d", stats['disagreement'])
    log.info("Matches:        %d (hidden)", stats['match'])
    log.info("Below threshold:%d", stats['below_threshold'])
    log.info("Failed:         %d", stats['failed'])
    log.info("Results saved to %s", results_path)

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Analyze photos: classify, compare to existing labels, generate review data."
    )
    parser.add_argument("--folder", required=True, help="Path to image folder")
    parser.add_argument("--labels-file", required=True, help="Text file with one label per line")
    parser.add_argument("--output-dir", default="/tmp/photo-review", help="Output directory")
    parser.add_argument("--model-weights", default="/tmp/bioclip_model/open_clip_pytorch_model.bin")
    parser.add_argument("--threshold", type=float, default=0.4)
    parser.add_argument("--thumbnail-size", type=int, default=400)
    parser.add_argument("--no-recursive", action="store_true")
    args = parser.parse_args()

    with open(args.labels_file) as f:
        labels = [line.strip() for line in f if line.strip()]
    log.info("Loaded %d labels from %s", len(labels), args.labels_file)

    analyze(
        folder=args.folder,
        output_dir=args.output_dir,
        labels=labels,
        pretrained_str=args.model_weights,
        threshold=args.threshold,
        thumbnail_size=args.thumbnail_size,
        recursive=not args.no_recursive,
    )


if __name__ == "__main__":
    main()
