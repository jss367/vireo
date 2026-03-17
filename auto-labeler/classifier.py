"""BioCLIP classifier wrapper for species-level classification."""

import logging

log = logging.getLogger(__name__)


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
        if labels:
            from bioclip import CustomLabelsClassifier
            self._classifier = CustomLabelsClassifier(
                cls_ary=labels,
                model_str=model_str,
                pretrained_str=pretrained_str,
            )
            self._mode = 'custom'
        else:
            from bioclip import TreeOfLifeClassifier, Rank
            self._classifier = TreeOfLifeClassifier(
                model_str=model_str,
                pretrained_str=pretrained_str,
            )
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
