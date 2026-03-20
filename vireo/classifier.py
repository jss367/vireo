"""BioCLIP classifier wrapper for species-level classification."""

import hashlib
import logging
import os

log = logging.getLogger(__name__)

CACHE_DIR = os.path.expanduser("~/.vireo/embedding_cache")


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

            from bioclip import CustomLabelsClassifier
            import torch

            cache_path = _embedding_cache_path(labels, model_str)

            if os.path.exists(cache_path):
                log.info(
                    "Loading cached label embeddings for %d labels...", len(labels)
                )
                self._classifier = CustomLabelsClassifier(
                    cls_ary=["_placeholder"],
                    model_str=model_str,
                    pretrained_str=pretrained_str,
                )
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
                self._classifier = CustomLabelsClassifier(
                    cls_ary=["_placeholder"],
                    model_str=model_str,
                    pretrained_str=pretrained_str,
                )
                self._classifier.classes = [cls.strip() for cls in labels]
                self._classifier.txt_embeddings = _compute_embeddings_with_progress(
                    self._classifier,
                    self._classifier.classes,
                    progress_callback=embedding_progress_callback,
                )
                os.makedirs(CACHE_DIR, exist_ok=True)
                torch.save(self._classifier.txt_embeddings, cache_path)
                log.info("Label embeddings computed and cached to disk")

            self._mode = "custom"
        else:
            log.info("Loading TreeOfLife classifier...")
            from bioclip import TreeOfLifeClassifier, Rank

            self._classifier = TreeOfLifeClassifier(
                model_str=model_str,
                pretrained_str=pretrained_str,
            )
            log.info("TreeOfLife classifier ready")
            self._mode = "tol"
            self._rank = Rank.SPECIES

    def classify(self, image_path, threshold=0.4):
        """Classify an image and return predictions above threshold.

        Returns:
            list of dicts with species, score, auto_tag, confidence_tag
        """
        preds, _ = self.classify_with_embedding(image_path, threshold)
        return preds

    def classify_with_embedding(self, image_path, threshold=0.4):
        """Classify an image and return both predictions and the image embedding.

        Single forward pass — computes the image embedding once, uses it for
        classification, and returns it for downstream use (e.g. similarity grouping).

        Returns:
            (predictions, embedding) where:
                predictions: list of dicts with species, score, auto_tag, confidence_tag
                embedding: numpy float32 array (the normalized image embedding vector)
        """
        import numpy as np
        import torch
        import torch.nn.functional as F

        clf = self._classifier

        # Compute image embedding (single forward pass)
        img_features = clf.create_image_features_for_image(image_path, normalize=True)
        embedding = img_features.cpu().numpy().astype(np.float32).flatten()

        if self._mode == "custom":
            # Dot product with text embeddings to get probabilities (same as predict())
            probs = (100.0 * img_features @ clf.txt_embeddings).softmax(dim=-1)
            probs = probs.cpu().numpy().flatten()

            # Build sorted predictions
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
            return results, embedding
        else:
            # TreeOfLife mode — predictions include full taxonomy
            raw_preds = clf.predict(image_path, self._rank)
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
                # TreeOfLife predictions include taxonomy fields
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
