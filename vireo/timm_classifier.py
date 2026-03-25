"""timm-based classifier for species classification (iNaturalist 2021)."""

import logging
import os

log = logging.getLogger(__name__)


class TimmClassifier:
    """Wraps a timm model for species classification.

    Unlike BioCLIP's Classifier, this uses a supervised model with a fixed
    class set (10K iNat21 species). No label files or text embeddings needed.

    Args:
        model_str: timm model identifier (e.g. "hf-hub:timm/eva02_large_patch14_clip_336.merged2b_ft_inat21")
        taxonomy: optional Taxonomy instance for enriching predictions with hierarchy
    """

    def __init__(self, model_str, taxonomy=None):
        import timm
        import torch
        from timm.data import create_transform, resolve_data_config

        self._device = "cuda" if torch.cuda.is_available() else "cpu"

        log.info("Loading timm model: %s (device=%s)", model_str, self._device)
        self._model = timm.create_model(model_str, pretrained=True)
        self._model.to(self._device)
        self._model.eval()

        # Build data transform from model config
        data_cfg = resolve_data_config(self._model.pretrained_cfg)
        self._transform = create_transform(**data_cfg, is_training=False)

        # Extract class names from model config
        cfg = self._model.pretrained_cfg
        self._class_names = cfg.get("label_names", [])
        if not self._class_names:
            raise RuntimeError(
                f"Model {model_str} has no label_names in its config — "
                "cannot map class indices to species names."
            )

        # Build scientific → common name mapping from label_descriptions
        # Format: {"Sturnus vulgaris": "European Starling, Bird"}
        self._common_names = {}
        descs = cfg.get("label_descriptions", {})
        if isinstance(descs, dict):
            for sci_name, desc in descs.items():
                # desc format: "Common Name, Category" — take part before last comma
                parts = desc.rsplit(", ", 1)
                common = parts[0] if len(parts) > 1 else desc
                # If common name equals scientific name, it has no common name
                if common.lower() != sci_name.lower():
                    self._common_names[sci_name.lower()] = common

        # Also use taxonomy.json for any names not in label_descriptions
        self._taxonomy = taxonomy

        log.info(
            "TimmClassifier ready: %d classes, %d common name mappings",
            len(self._class_names),
            len(self._common_names),
        )

    def _resolve_common_name(self, scientific_name):
        """Map a scientific name to a common name.

        Priority: label_descriptions from model > taxonomy.json > scientific name as-is.
        """
        key = scientific_name.lower()
        if key in self._common_names:
            return self._common_names[key]

        if self._taxonomy:
            taxon = self._taxonomy.lookup(scientific_name)
            if taxon and taxon.get("common_name"):
                return taxon["common_name"]

        return scientific_name

    def classify(self, image_path, threshold=0.1):
        """Classify an image and return predictions above threshold.

        Args:
            image_path: path to image file
            threshold: minimum confidence to include (default 0.1 — lower than
                BioCLIP's 0.4 since probability is spread across 10K classes)

        Returns:
            list of dicts with species, score, auto_tag, confidence_tag, taxonomy
        """
        import torch
        from PIL import Image

        img = Image.open(image_path).convert("RGB")
        input_tensor = self._transform(img).unsqueeze(0).to(self._device)

        with torch.no_grad():
            output = self._model(input_tensor)
            probs = torch.softmax(output, dim=-1).cpu().numpy().flatten()

        # Build sorted predictions
        indexed = sorted(enumerate(probs), key=lambda x: x[1], reverse=True)

        results = []
        for idx, score in indexed:
            score = float(score)
            if score < threshold:
                break  # sorted, so all remaining are below threshold

            scientific_name = self._class_names[idx]
            common_name = self._resolve_common_name(scientific_name)

            # Build taxonomy hierarchy
            taxonomy = {"scientific_name": scientific_name}
            if self._taxonomy:
                hierarchy = self._taxonomy.get_hierarchy(scientific_name)
                if hierarchy:
                    taxonomy = hierarchy
                elif common_name != scientific_name:
                    # Try lookup by common name
                    hierarchy = self._taxonomy.get_hierarchy(common_name)
                    if hierarchy:
                        taxonomy = hierarchy
                    else:
                        taxonomy["scientific_name"] = scientific_name

            results.append(
                {
                    "species": common_name,
                    "score": score,
                    "auto_tag": f"auto:{common_name}",
                    "confidence_tag": f"auto:confidence:{score:.2f}",
                    "taxonomy": taxonomy,
                }
            )

        return results
