"""BioCLIP classifier wrapper for species-level classification."""

import hashlib
import logging
import os

log = logging.getLogger(__name__)

CACHE_DIR = os.path.expanduser("~/.spotter/embedding_cache")


def _embedding_cache_path(labels, model_str):
    """Build a cache file path based on a hash of the labels and model."""
    key = model_str + "\n" + "\n".join(labels)
    digest = hashlib.sha256(key.encode()).hexdigest()[:16]
    return os.path.join(CACHE_DIR, f"{digest}.pt")


def _compute_embeddings_with_progress(classifier, labels):
    """Compute text embeddings for labels with progress logging.

    Replicates CustomLabelsClassifier._get_txt_embeddings but logs progress.
    """
    import torch
    import torch.nn.functional as F
    from bioclip.predict import OPENA_AI_IMAGENET_TEMPLATE

    total = len(labels)
    log.info("Computing label embeddings: 0/%d", total)

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
            if done % 100 == 0 or done == total:
                log.info("Computing label embeddings: %d/%d", done, total)

    return torch.stack(all_features, dim=1)


class Classifier:
    """Wraps BioCLIP for species classification.

    Args:
        labels: list of species/label strings for CustomLabelsClassifier.
                If None, uses TreeOfLifeClassifier (requires hf-hub model).
        model_str: open_clip model string (default: BioCLIP v1)
        pretrained_str: path to pretrained weights or HF tag
    """

    def __init__(self, labels=None, model_str='ViT-B-16',
                 pretrained_str='/tmp/bioclip_model/open_clip_pytorch_model.bin'):
        if labels is not None:
            if not labels:
                raise ValueError("labels list must not be empty")

            from bioclip import CustomLabelsClassifier
            import torch

            cache_path = _embedding_cache_path(labels, model_str)

            if os.path.exists(cache_path):
                log.info("Loading cached label embeddings for %d labels...", len(labels))
                self._classifier = CustomLabelsClassifier(
                    cls_ary=["_placeholder"],
                    model_str=model_str,
                    pretrained_str=pretrained_str,
                )
                self._classifier.classes = [cls.strip() for cls in labels]
                self._classifier.txt_embeddings = torch.load(cache_path, weights_only=True)
                log.info("Label embeddings loaded from cache")
            else:
                log.info("Computing label embeddings for %d labels (first run — will be cached for next time)...", len(labels))
                # Init with placeholder to get the model loaded without computing all embeddings
                self._classifier = CustomLabelsClassifier(
                    cls_ary=["_placeholder"],
                    model_str=model_str,
                    pretrained_str=pretrained_str,
                )
                # Now compute embeddings ourselves with progress logging
                self._classifier.classes = [cls.strip() for cls in labels]
                self._classifier.txt_embeddings = _compute_embeddings_with_progress(
                    self._classifier, self._classifier.classes,
                )
                os.makedirs(CACHE_DIR, exist_ok=True)
                torch.save(self._classifier.txt_embeddings, cache_path)
                log.info("Label embeddings computed and cached to disk")

            self._mode = 'custom'
        else:
            log.info("Loading TreeOfLife classifier...")
            from bioclip import TreeOfLifeClassifier, Rank
            self._classifier = TreeOfLifeClassifier(
                model_str=model_str,
                pretrained_str=pretrained_str,
            )
            log.info("TreeOfLife classifier ready")
            self._mode = 'tol'
            self._rank = Rank.SPECIES

    def classify(self, image_path, threshold=0.4):
        """Classify an image and return predictions above threshold.

        Args:
            image_path: path to an image file
            threshold: minimum confidence score (0-1) to include

        Returns:
            list of dicts, each with:
                - species: the predicted species name
                - score: confidence score (0-1)
                - auto_tag: prefixed tag like "auto:Bald eagle"
                - confidence_tag: like "auto:confidence:0.95"
        """
        if self._mode == 'custom':
            raw_preds = self._classifier.predict(image_path)
        else:
            raw_preds = self._classifier.predict(image_path, self._rank)

        results = []
        for pred in raw_preds:
            if self._mode == 'custom':
                species = pred['classification']
            else:
                species = pred.get('common_name') or pred.get('species', '')

            score = pred['score']
            if score < threshold:
                continue

            results.append({
                'species': species,
                'score': score,
                'auto_tag': f"auto:{species}",
                'confidence_tag': f"auto:confidence:{score:.2f}",
            })

        return results
