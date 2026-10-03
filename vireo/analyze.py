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

from classifier import Classifier, _resolve_model_dir
from classifier_cache import acquire_cached_classifier
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


def _list_image_files(folder_path, recursive):
    """Sorted supported, non-hidden image files under ``folder_path``."""
    if recursive:
        return sorted(
            f
            for f in folder_path.rglob("*")
            if f.suffix.lower() in SUPPORTED_EXTENSIONS and not f.name.startswith(".")
        )
    return sorted(
        f
        for f in folder_path.iterdir()
        if f.suffix.lower() in SUPPORTED_EXTENSIONS and not f.name.startswith(".")
    )


def _save_thumbnail(source_path, thumb_path, thumbnail_size):
    img = load_image(str(source_path))
    if img:
        img.thumbnail((thumbnail_size, thumbnail_size))
        img.save(thumb_path, quality=85)


class _AnalyzeRun:
    """State shared by every phase of one analyze() call."""

    def __init__(
        self,
        *,
        folder,
        output_dir,
        thumb_dir,
        tax,
        clf,
        slug,
        model_str,
        pretrained_str,
        threshold,
        thumbnail_size,
        group_window,
    ):
        self.folder = folder
        self.folder_path = Path(folder)
        self.thumb_dir = thumb_dir
        self.tax = tax
        self.clf = clf
        self.slug = slug
        self.model_str = model_str
        self.pretrained_str = pretrained_str
        self.threshold = threshold
        self.thumbnail_size = thumbnail_size
        self.group_window = group_window
        self.results_path = os.path.join(output_dir, "results.json")
        self.existing_results = None
        self.existing_photos = {}
        self.existing_groups = {}
        self.classified = []
        self.stats = None
        self.photos = []
        self.group_counter = 0

    def load_existing_results(self):
        # Load existing results if present (for multi-model merging)
        if os.path.exists(self.results_path):
            with open(self.results_path) as f:
                self.existing_results = json.load(f)
            log.info("Found existing results.json — will merge model '%s'", self.slug)

        # Build lookups for merging: individual photos by image_path, groups by group_id
        if self.existing_results:
            for p in self.existing_results.get("photos", []):
                ip = p.get("image_path")
                gid = p.get("group_id")
                if ip:
                    self.existing_photos[ip] = p
                elif gid:
                    self.existing_groups[gid] = p

    def classify_images(self, image_files):
        """Phase 1: classify all images and read timestamps."""
        self.stats = {
            "total": len(image_files),
            "new": 0,
            "refinement": 0,
            "disagreement": 0,
            "match": 0,
            "failed": 0,
            "below_threshold": 0,
        }

        for i, image_path in enumerate(image_files):
            entry = self._classify_image(image_path)
            if entry is None:
                continue
            self.classified.append(entry)

            if (i + 1) % 100 == 0:
                log.info("Progress: %d/%d images", i + 1, len(image_files))

    def _top_prediction(self, image_path):
        img = load_image(str(image_path))
        if img is None:
            self.stats["failed"] += 1
            return None

        # Classify via temp file
        with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp:
            tmp_path = tmp.name
            img.save(tmp_path, quality=85)

        try:
            predictions = self.clf.classify(tmp_path, threshold=self.threshold)
        except Exception:
            log.warning("Classification failed for %s", image_path, exc_info=True)
            self.stats["failed"] += 1
            return None
        finally:
            os.unlink(tmp_path)

        if not predictions:
            self.stats["below_threshold"] += 1
            return None

        return predictions[0]

    def _classify_image(self, image_path):
        top = self._top_prediction(image_path)
        if top is None:
            return None

        # Read existing XMP keywords and categorize
        xmp_path = image_path.with_suffix(".xmp")
        existing = read_keywords(str(xmp_path))
        category = categorize(top["species"], existing, self.tax)
        self.stats[category] += 1

        if category == "match":
            return None

        # Read EXIF timestamp for grouping
        timestamp = None
        if image_path.suffix.lower() in {".jpg", ".jpeg", ".tiff", ".tif"}:
            timestamp = read_exif_timestamp(str(image_path))

        # Build unique thumbnail name
        rel_path = image_path.relative_to(self.folder_path)
        thumb_name = str(rel_path).replace(os.sep, "_")
        thumb_name = Path(thumb_name).stem + ".jpg"

        # Filter existing keywords to just species for display
        existing_species = [kw for kw in existing if self.tax.is_taxon(kw)]

        return {
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

    def group_neighbors(self):
        """Phase 2: group neighbors."""
        if self.group_window > 0 and self.classified:
            groups = group_by_timestamp(self.classified, window_seconds=self.group_window)
        else:
            groups = [[c] for c in self.classified]

        for group in groups:
            if len(group) == 1:
                self.photos.append(self._single_photo_entry(group[0]))
            else:
                # Group of multiple photos
                self.group_counter += 1
                group_id = f"g{self.group_counter:04d}"
                self.photos.append(self._group_entry(group_id, group))

    def _single_photo_entry(self, item):
        # Generate thumbnail
        thumb_path = os.path.join(self.thumb_dir, item["filename"])
        _save_thumbnail(item["source_path"], thumb_path, self.thumbnail_size)

        model_pred = {
            "prediction": item["prediction"],
            "confidence": item["confidence"],
            "category": item["category"],
        }

        # Merge with existing photo entry if present
        if item["image_path"] in self.existing_photos:
            photo = self.existing_photos[item["image_path"]]
            photo["predictions"][self.slug] = model_pred
            return photo
        return {
            "filename": item["filename"],
            "image_path": item["image_path"],
            "xmp_path": item["xmp_path"],
            "existing_species": item["existing_species"],
            "predictions": {self.slug: model_pred},
            "status": "pending",
        }

    def _group_entry(self, group_id, group):
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
            cons["prediction"], set(representative["existing_species"]), self.tax
        )
        if cons_category == "match":
            cons_category = representative["category"]  # fallback

        # Generate thumbnail for representative
        rep_thumb = os.path.join(self.thumb_dir, representative["filename"])
        _save_thumbnail(representative["source_path"], rep_thumb, self.thumbnail_size)

        # Also save individual member thumbnails
        members = []
        for item in group:
            members.append(item["filename"])
            member_thumb_path = os.path.join(self.thumb_dir, item["filename"])
            if not os.path.exists(member_thumb_path):
                _save_thumbnail(item["source_path"], member_thumb_path, self.thumbnail_size)

        model_consensus = {
            "prediction": cons["prediction"],
            "confidence": cons["confidence"],
            "individual_predictions": cons["individual_predictions"],
        }
        merged_consensus = self._merged_consensus(group, model_consensus)

        return {
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

    def _merged_consensus(self, group, model_consensus):
        # Merge consensus from existing group entries with matching members
        merged_consensus = {}
        member_paths_set = set(item["image_path"] for item in group)
        for eg in self.existing_groups.values():
            if set(eg.get("member_paths", [])) == member_paths_set:
                merged_consensus.update(eg.get("consensus", {}))
                break
        merged_consensus[self.slug] = model_consensus
        return merged_consensus

    def build_results(self):
        """Build final results."""
        models = {}
        if self.existing_results:
            models = self.existing_results.get("models", {})
        models[self.slug] = {
            "model_str": self.model_str,
            "pretrained_str": self.pretrained_str,
            "run_date": str(date.today()),
            "threshold": self.threshold,
        }

        if self.existing_results:
            self._preserve_prior_entries()

        return {
            "folder": str(self.folder),
            "models": models,
            "settings": {
                "threshold": self.threshold,
                "thumbnail_size": self.thumbnail_size,
                "group_window": self.group_window,
            },
            "stats": self.stats,
            "photos": self.photos,
        }

    def _preserve_prior_entries(self):
        """Preserve entries from prior runs that weren't re-classified."""
        photos = self.photos
        current_image_paths = {p["image_path"] for p in photos if "image_path" in p}
        current_member_paths = set()
        for p in photos:
            if "member_paths" in p:
                current_member_paths.update(p["member_paths"])
        for p in self.existing_results.get("photos", []):
            ip = p.get("image_path")
            gid = p.get("group_id")
            if ip and ip not in current_image_paths and ip not in current_member_paths:
                photos.append(p)
            elif gid and set(p.get("member_paths", [])) - current_member_paths:
                # Group from prior run whose members weren't re-grouped
                photos.append(p)

    def write_results(self, results):
        with open(self.results_path, "w") as f:
            json.dump(results, f, indent=2)

        stats = self.stats
        log.info("--- Analysis Summary ---")
        log.info("Model:          %s", self.slug)
        log.info("Total images:   %d", stats["total"])
        log.info("New:            %d", stats["new"])
        log.info("Refinements:    %d", stats["refinement"])
        log.info("Disagreements:  %d", stats["disagreement"])
        log.info("Matches:        %d (hidden)", stats["match"])
        log.info("Below threshold:%d", stats["below_threshold"])
        log.info("Failed:         %d", stats["failed"])
        log.info("Groups:         %d", self.group_counter)
        log.info("Results saved to %s", self.results_path)


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
    resolved_model_dir = _resolve_model_dir(model_str, pretrained_str)
    classifier_handle = acquire_cached_classifier(
        model_type="bioclip",
        model_str=model_str,
        weights_path=resolved_model_dir,
        labels=labels,
        factory=lambda: Classifier(
            labels=labels,
            model_str=model_str,
            pretrained_str=pretrained_str,
        ),
    )
    clf = classifier_handle.__enter__()
    try:
        run = _AnalyzeRun(
            folder=folder,
            output_dir=output_dir,
            thumb_dir=thumb_dir,
            tax=tax,
            clf=clf,
            slug=_model_slug(model_name, model_str),
            model_str=model_str,
            pretrained_str=pretrained_str,
            threshold=threshold,
            thumbnail_size=thumbnail_size,
            group_window=group_window,
        )

        image_files = _list_image_files(run.folder_path, recursive)
        log.info("Found %d images in %s", len(image_files), folder)

        run.load_existing_results()
        run.classify_images(image_files)
        run.group_neighbors()
        results = run.build_results()
        run.write_results(results)
        return results
    finally:
        classifier_handle.release()


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

    from labels import read_label_file

    labels = read_label_file(args.labels_file)
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
