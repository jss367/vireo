"""Scan photos, classify, compare to existing XMP keywords, generate review data.

Usage:
    python vireo/analyze.py --folder /path/to/photos --labels-file labels.txt
"""

import argparse
import json
import logging
import os
import tempfile
from datetime import date
from pathlib import Path

from classifier import Classifier
from compare import categorize
from grouping import consensus_prediction, group_by_timestamp, read_exif_timestamp
from image_loader import SUPPORTED_EXTENSIONS, load_image
from taxonomy import Taxonomy
from xmp import read_keywords

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
)
log = logging.getLogger(__name__)


def _model_slug(model_name, model_str):
    """Generate a model key slug."""
    if model_name:
        return model_name
    return f"bioclip-{model_str.lower().replace('/', '-')}"


def analyze(
    folder,
    output_dir,
    labels,
    taxonomy_path,
    model_str="ViT-B-16",
    pretrained_str="/tmp/bioclip_model/open_clip_pytorch_model.bin",
    model_name=None,
    threshold=0.4,
    thumbnail_size=400,
    recursive=True,
    group_window=10,
):
    """Scan a folder, classify images, compare to existing keywords, write results.

    Args:
        folder: path to image folder
        output_dir: path to output directory for results.json and thumbnails/
        labels: list of species labels for the classifier
        taxonomy_path: path to taxonomy.json
        model_str: BioCLIP model string
        pretrained_str: path to model weights
        model_name: optional human-readable model name (used as key in results)
        threshold: minimum confidence score
        thumbnail_size: max dimension for thumbnails
        recursive: scan subfolders
        group_window: seconds for neighbor grouping (0 to disable)
    """
    os.makedirs(output_dir, exist_ok=True)
    thumb_dir = os.path.join(output_dir, "thumbnails")
    os.makedirs(thumb_dir, exist_ok=True)

    tax = Taxonomy(taxonomy_path)
    clf = Classifier(labels=labels, model_str=model_str, pretrained_str=pretrained_str)
    slug = _model_slug(model_name, model_str)

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

    log.info("Found %d images in %s", len(image_files), folder)

    # Load existing results if present (for multi-model merging)
    results_path = os.path.join(output_dir, "results.json")
    existing_results = None
    if os.path.exists(results_path):
        with open(results_path) as f:
            existing_results = json.load(f)
        log.info("Found existing results.json — will merge model '%s'", slug)

    # Build lookups for merging: individual photos by image_path, groups by group_id
    existing_photos = {}
    existing_groups = {}
    if existing_results:
        for p in existing_results.get("photos", []):
            ip = p.get("image_path")
            gid = p.get("group_id")
            if ip:
                existing_photos[ip] = p
            elif gid:
                existing_groups[gid] = p

    # Phase 1: classify all images and read timestamps
    classified = []
    stats = {
        "total": len(image_files),
        "new": 0,
        "refinement": 0,
        "disagreement": 0,
        "match": 0,
        "failed": 0,
        "below_threshold": 0,
    }

    for i, image_path in enumerate(image_files):
        img = load_image(str(image_path))
        if img is None:
            stats["failed"] += 1
            continue

        # Classify via temp file
        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp:
            tmp_path = tmp.name
            img.save(tmp_path, quality=85)

        try:
            predictions = clf.classify(tmp_path, threshold=threshold)
        except Exception:
            log.warning("Classification failed for %s", image_path, exc_info=True)
            stats["failed"] += 1
            continue
        finally:
            os.unlink(tmp_path)

        if not predictions:
            stats["below_threshold"] += 1
            continue

        top = predictions[0]

        # Read existing XMP keywords and categorize
        xmp_path = image_path.with_suffix(".xmp")
        existing = read_keywords(str(xmp_path))
        category = categorize(top["species"], existing, tax)
        stats[category] += 1

        if category == "match":
            continue

        # Read EXIF timestamp for grouping
        timestamp = None
        if image_path.suffix.lower() in {".jpg", ".jpeg", ".tiff", ".tif"}:
            timestamp = read_exif_timestamp(str(image_path))

        # Build unique thumbnail name
        rel_path = image_path.relative_to(folder_path)
        thumb_name = str(rel_path).replace(os.sep, "_")
        thumb_name = Path(thumb_name).stem + ".jpg"

        # Filter existing keywords to just species for display
        existing_species = [kw for kw in existing if tax.is_taxon(kw)]

        classified.append(
            {
                "image_path": str(image_path),
                "xmp_path": str(xmp_path),
                "filename": thumb_name,
                "prediction": top["species"],
                "confidence": round(top["score"], 4),
                "category": category,
                "existing_species": existing_species,
                "timestamp": timestamp,
                "source_path": image_path,
            }
        )

        if (i + 1) % 100 == 0:
            log.info("Progress: %d/%d images", i + 1, len(image_files))

    # Phase 2: group neighbors
    photos = []
    if group_window > 0 and classified:
        groups = group_by_timestamp(classified, window_seconds=group_window)
    else:
        groups = [[c] for c in classified]

    group_counter = 0
    for group in groups:
        if len(group) == 1:
            item = group[0]
            # Generate thumbnail
            thumb_path = os.path.join(thumb_dir, item["filename"])
            thumb_img = load_image(str(item["source_path"]))
            if thumb_img:
                thumb_img.thumbnail((thumbnail_size, thumbnail_size))
                thumb_img.save(thumb_path, quality=85)

            model_pred = {
                "prediction": item["prediction"],
                "confidence": item["confidence"],
                "category": item["category"],
            }

            # Merge with existing photo entry if present
            if item["image_path"] in existing_photos:
                photo = existing_photos[item["image_path"]]
                photo["predictions"][slug] = model_pred
            else:
                photo = {
                    "filename": item["filename"],
                    "image_path": item["image_path"],
                    "xmp_path": item["xmp_path"],
                    "existing_species": item["existing_species"],
                    "predictions": {slug: model_pred},
                    "status": "pending",
                }
            photos.append(photo)
        else:
            # Group of multiple photos
            group_counter += 1
            group_id = f"g{group_counter:04d}"

            # Compute consensus
            preds_for_consensus = [
                {"prediction": item["prediction"], "confidence": item["confidence"]}
                for item in group
            ]
            cons = consensus_prediction(preds_for_consensus)

            # Use the best category from the group (prefer the consensus prediction's category)
            # Re-categorize using the consensus prediction
            representative = group[0]
            cons_category = categorize(
                cons["prediction"], set(representative["existing_species"]), tax
            )
            if cons_category == "match":
                cons_category = representative["category"]  # fallback

            # Generate thumbnail for representative
            rep_thumb = os.path.join(thumb_dir, representative["filename"])
            rep_img = load_image(str(representative["source_path"]))
            if rep_img:
                rep_img.thumbnail((thumbnail_size, thumbnail_size))
                rep_img.save(rep_thumb, quality=85)

            # Also save individual member thumbnails
            members = []
            for item in group:
                members.append(item["filename"])
                member_thumb_path = os.path.join(thumb_dir, item["filename"])
                if not os.path.exists(member_thumb_path):
                    member_img = load_image(str(item["source_path"]))
                    if member_img:
                        member_img.thumbnail((thumbnail_size, thumbnail_size))
                        member_img.save(member_thumb_path, quality=85)

            model_consensus = {
                "prediction": cons["prediction"],
                "confidence": cons["confidence"],
                "individual_predictions": cons["individual_predictions"],
            }

            # Merge consensus from existing group entries with matching members
            merged_consensus = {}
            member_paths_set = set(item["image_path"] for item in group)
            for eg in existing_groups.values():
                if set(eg.get("member_paths", [])) == member_paths_set:
                    merged_consensus.update(eg.get("consensus", {}))
                    break
            merged_consensus[slug] = model_consensus

            photo = {
                "group_id": group_id,
                "representative": representative["filename"],
                "members": members,
                "member_paths": [item["image_path"] for item in group],
                "member_xmp_paths": [item["xmp_path"] for item in group],
                "existing_species": representative["existing_species"],
                "consensus": merged_consensus,
                "category": cons_category,
                "status": "pending",
            }
            photos.append(photo)

    # Build final results
    models = {}
    if existing_results:
        models = existing_results.get("models", {})
    models[slug] = {
        "model_str": model_str,
        "pretrained_str": pretrained_str,
        "run_date": str(date.today()),
        "threshold": threshold,
    }

    # Preserve entries from prior runs that weren't re-classified
    if existing_results:
        current_image_paths = {p["image_path"] for p in photos if "image_path" in p}
        current_member_paths = set()
        for p in photos:
            if "member_paths" in p:
                current_member_paths.update(p["member_paths"])
        for p in existing_results.get("photos", []):
            ip = p.get("image_path")
            gid = p.get("group_id")
            if ip and ip not in current_image_paths and ip not in current_member_paths:
                photos.append(p)
            elif gid and set(p.get("member_paths", [])) - current_member_paths:
                # Group from prior run whose members weren't re-grouped
                photos.append(p)

    results = {
        "folder": str(folder),
        "models": models,
        "settings": {
            "threshold": threshold,
            "thumbnail_size": thumbnail_size,
            "group_window": group_window,
        },
        "stats": stats,
        "photos": photos,
    }

    with open(results_path, "w") as f:
        json.dump(results, f, indent=2)

    log.info("--- Analysis Summary ---")
    log.info("Model:          %s", slug)
    log.info("Total images:   %d", stats["total"])
    log.info("New:            %d", stats["new"])
    log.info("Refinements:    %d", stats["refinement"])
    log.info("Disagreements:  %d", stats["disagreement"])
    log.info("Matches:        %d (hidden)", stats["match"])
    log.info("Below threshold:%d", stats["below_threshold"])
    log.info("Failed:         %d", stats["failed"])
    log.info("Groups:         %d", group_counter)
    log.info("Results saved to %s", results_path)

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Analyze photos: classify, compare to existing labels, generate review data."
    )
    parser.add_argument("--folder", required=True, help="Path to image folder")
    parser.add_argument(
        "--labels-file", required=True, help="Text file with one label per line"
    )
    from taxonomy import find_taxonomy_json
    parser.add_argument(
        "--taxonomy",
        default=find_taxonomy_json(),
        help="Path to taxonomy.json",
    )
    parser.add_argument(
        "--output-dir", default="/tmp/photo-review", help="Output directory"
    )
    parser.add_argument(
        "--model-weights", default="/tmp/bioclip_model/open_clip_pytorch_model.bin"
    )
    parser.add_argument("--model-name", default=None, help="Human-readable model name")
    parser.add_argument("--threshold", type=float, default=0.4)
    parser.add_argument("--thumbnail-size", type=int, default=400)
    parser.add_argument(
        "--group-window",
        type=int,
        default=10,
        help="Seconds for neighbor grouping (0 to disable)",
    )
    parser.add_argument("--no-recursive", action="store_true")
    args = parser.parse_args()

    with open(args.labels_file) as f:
        labels = [line.strip() for line in f if line.strip()]
    log.info("Loaded %d labels from %s", len(labels), args.labels_file)

    analyze(
        folder=args.folder,
        output_dir=args.output_dir,
        labels=labels,
        taxonomy_path=args.taxonomy,
        pretrained_str=args.model_weights,
        model_name=args.model_name,
        threshold=args.threshold,
        thumbnail_size=args.thumbnail_size,
        group_window=args.group_window,
        recursive=not args.no_recursive,
    )


if __name__ == "__main__":
    main()
