"""BioCLIP classifier wrapper for species-level classification."""

import hashlib
import json
import logging
import os

log = logging.getLogger(__name__)

CACHE_DIR = os.path.expanduser("~/.vireo/embedding_cache")
_MANIFEST_PATH = os.path.join(CACHE_DIR, "manifest.json")


def _load_manifest():
    """Load the embedding cache manifest."""
    if os.path.exists(_MANIFEST_PATH):
        try:
            with open(_MANIFEST_PATH) as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def _save_manifest(manifest):
    """Save the embedding cache manifest."""
    os.makedirs(CACHE_DIR, exist_ok=True)
    with open(_MANIFEST_PATH, "w") as f:
        json.dump(manifest, f, indent=2)


def _embedding_cache_path(labels, model_str):
    """Build a cache file path based on a hash of the labels and model."""
    key = model_str + "\n" + "\n".join(labels)
    digest = hashlib.sha256(key.encode()).hexdigest()[:16]
    return os.path.join(CACHE_DIR, f"{digest}.pt")


def _compute_embeddings_with_progress(classifier, labels, progress_callback=None):
    """Compute text embeddings for labels with progress logging.

    Replicates CustomLabelsClassifier._get_txt_embeddings but logs progress.

    Args:
        progress_callback: optional callable(current, total) for UI progress
    """
    import torch
    import torch.nn.functional as F
    from bioclip.predict import OPENA_AI_IMAGENET_TEMPLATE

    total = len(labels)
    log.info("Computing label embeddings: 0/%d", total)
    if progress_callback:
        progress_callback(0, total)

    all_features = []
    with torch.no_grad():
        for i, classname in enumerate(labels):
            txts = [template(classname) for template in OPENA_AI_IMAGENET_TEMPLATE]
            txts = classifier.tokenizer(txts).to(classifier.device)
            txt_features = classifier.model.encode_text(txts)
            txt_features = F.normalize(txt_features, dim=-1).mean(dim=0)
            txt_features /= txt_features.norm()
            all_features.append(txt_features)

            done = i + 1
            if done % 50 == 0 or done == total:
                log.info("Computing label embeddings: %d/%d", done, total)
                if progress_callback:
                    progress_callback(done, total)

    return torch.stack(all_features, dim=1)


class Classifier:
    """Wraps BioCLIP for species classification.

    Args:
        labels: list of species/label strings for CustomLabelsClassifier.
                If None, uses TreeOfLifeClassifier (requires hf-hub model).
        model_str: open_clip model string (default: BioCLIP v1)
        pretrained_str: path to pretrained weights or HF tag
    """

    def __init__(
        self,
        labels=None,
        model_str="ViT-B-16",
        pretrained_str="/tmp/bioclip_model/open_clip_pytorch_model.bin",
        embedding_progress_callback=None,
    ):
        if labels is not None:
            if not labels:
                raise ValueError("labels list must not be empty")

            import torch
            from bioclip import CustomLabelsClassifier

            cache_path = _embedding_cache_path(labels, model_str)

            # For hf-hub models, open_clip manages weights internally;
            # passing a local pretrained_str would be ignored with a warning.
            clf_kwargs = {"cls_ary": ["_placeholder"], "model_str": model_str}
            if not model_str.startswith("hf-hub:"):
                clf_kwargs["pretrained_str"] = pretrained_str

            if os.path.exists(cache_path):
                log.info(
                    "Loading cached label embeddings for %d labels...", len(labels)
                )
                self._classifier = CustomLabelsClassifier(**clf_kwargs)
                self._classifier.classes = [cls.strip() for cls in labels]
                self._classifier.txt_embeddings = torch.load(
                    cache_path, weights_only=True
                )
                log.info("Label embeddings loaded from cache")
            else:
                log.info(
                    "Computing label embeddings for %d labels (first run — will be cached for next time)...",
                    len(labels),
                )
                self._classifier = CustomLabelsClassifier(**clf_kwargs)
                self._classifier.classes = [cls.strip() for cls in labels]
                self._classifier.txt_embeddings = _compute_embeddings_with_progress(
                    self._classifier,
                    self._classifier.classes,
                    progress_callback=embedding_progress_callback,
                )
                os.makedirs(CACHE_DIR, exist_ok=True)
                torch.save(self._classifier.txt_embeddings, cache_path)
                # Update manifest with human-readable metadata
                from datetime import datetime
                manifest = _load_manifest()
                manifest[os.path.basename(cache_path)] = {
                    "model": model_str,
                    "label_count": len(labels),
                    "created": datetime.now().isoformat(timespec="seconds"),
                }
                _save_manifest(manifest)
                log.info("Label embeddings computed and cached to disk")

            self._mode = "custom"
        else:
            log.info("Loading TreeOfLife classifier...")
            from bioclip import Rank, TreeOfLifeClassifier

            tol_kwargs = {"model_str": model_str}
            if not model_str.startswith("hf-hub:"):
                tol_kwargs["pretrained_str"] = pretrained_str
            self._classifier = TreeOfLifeClassifier(**tol_kwargs)
            log.info("TreeOfLife classifier ready")
            self._mode = "tol"
            self._rank = Rank.SPECIES

    def _build_custom_results(self, probs, threshold):
        """Build sorted prediction dicts from a probability array (custom labels mode)."""
        clf = self._classifier
        ranked = sorted(zip(clf.classes, probs), key=lambda x: x[1], reverse=True)
        results = []
        for species, score in ranked:
            score = float(score)
            if score < threshold:
                continue
            results.append(
                {
                    "species": species,
                    "score": score,
                    "auto_tag": f"auto:{species}",
                    "confidence_tag": f"auto:confidence:{score:.2f}",
                }
            )
        return results

    def classify(self, image, threshold=0.4):
        """Classify an image and return predictions above threshold.

        Args:
            image: file path (str) or PIL Image

        Returns:
            list of dicts with species, score, auto_tag, confidence_tag
        """
        preds, _ = self.classify_with_embedding(image, threshold)
        return preds

    def classify_with_embedding(self, image, threshold=0.4):
        """Classify an image and return both predictions and the image embedding.

        Single forward pass — computes the image embedding once, uses it for
        classification, and returns it for downstream use (e.g. similarity grouping).

        Args:
            image: file path (str) or PIL Image

        Returns:
            (predictions, embedding) where:
                predictions: list of dicts with species, score, auto_tag, confidence_tag
                embedding: numpy float32 array (the normalized image embedding vector)
        """
        import numpy as np

        clf = self._classifier

        # Compute image embedding (single forward pass)
        # create_image_features_for_image accepts both paths and PIL images
        img_features = clf.create_image_features_for_image(image, normalize=True)
        embedding = img_features.cpu().numpy().astype(np.float32).flatten()

        if self._mode == "custom":
            probs = (100.0 * img_features @ clf.txt_embeddings).softmax(dim=-1)
            probs = probs.cpu().numpy().flatten()
            return self._build_custom_results(probs, threshold), embedding
        else:
            # TreeOfLife mode — predictions include full taxonomy
            raw_preds = clf.predict(image, self._rank)
            results = []
            for pred in raw_preds:
                species = pred.get("common_name") or pred.get("species", "")
                score = pred["score"]
                if score < threshold:
                    continue
                result = {
                    "species": species,
                    "score": score,
                    "auto_tag": f"auto:{species}",
                    "confidence_tag": f"auto:confidence:{score:.2f}",
                }
                taxonomy = {}
                for rank in ("kingdom", "phylum", "class", "order", "family", "genus"):
                    if rank in pred and pred[rank]:
                        taxonomy[rank] = pred[rank]
                if pred.get("species"):
                    taxonomy["scientific_name"] = pred["species"]
                if taxonomy:
                    result["taxonomy"] = taxonomy
                results.append(result)
            return results, embedding

    def classify_batch_with_embedding(self, images, threshold=0.4):
        """Classify multiple PIL images in a single forward pass.

        Only supported in custom labels mode. TreeOfLife mode falls back to
        single-image processing.

        Args:
            images: list of PIL Images
            threshold: minimum confidence to include

        Returns:
            list of (predictions, embedding) tuples
        """
        if self._mode != "custom":
            return [self.classify_with_embedding(img, threshold) for img in images]

        import numpy as np

        clf = self._classifier

        # Batch encode — bioclip's create_image_features handles
        # preprocessing, stacking, and model.encode_image in one call
        rgb_images = [img.convert("RGB") for img in images]
        all_features = clf.create_image_features(rgb_images, normalize=True)
        embeddings = all_features.cpu().numpy().astype(np.float32)

        # Batch dot product with text embeddings
        all_probs = (100.0 * all_features @ clf.txt_embeddings).softmax(dim=-1)
        all_probs = all_probs.cpu().numpy()

        results = []
        for i in range(len(images)):
            preds = self._build_custom_results(all_probs[i], threshold)
            results.append((preds, embeddings[i].flatten()))
        return results
