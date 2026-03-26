"""Encode text queries into the same CLIP embedding space as photo embeddings."""

import logging

import numpy as np

log = logging.getLogger(__name__)

_classifier_cache = {}


def _get_classifier(model_str, pretrained_str):
    """Lazily load and cache a classifier for text encoding."""
    cache_key = (model_str, pretrained_str)
    if cache_key not in _classifier_cache:
        log.info("Loading CLIP text encoder for %s...", model_str)
        from bioclip import CustomLabelsClassifier

        kwargs = {"cls_ary": ["_placeholder"], "model_str": model_str}
        if not model_str.startswith("hf-hub:"):
            kwargs["pretrained_str"] = pretrained_str
        clf = CustomLabelsClassifier(**kwargs)
        _classifier_cache[cache_key] = clf
        log.info("CLIP text encoder loaded")
    return _classifier_cache[cache_key]


def encode_text(query, model_str, pretrained_str):
    """Encode a text query into a normalized embedding vector.

    Uses the same open_clip model that produced the image embeddings,
    so the resulting vector is directly comparable via cosine similarity.

    Args:
        query: natural language search string (e.g., "bird in flight over water")
        model_str: open_clip model string (e.g., "ViT-B-16", "hf-hub:imageomics/bioclip-2")
        pretrained_str: path to pretrained weights or HF tag

    Returns:
        numpy float32 array -- normalized text embedding vector
    """
    import torch

    clf = _get_classifier(model_str, pretrained_str)

    with torch.no_grad():
        tokens = clf.tokenizer([query])
        if hasattr(tokens, "to"):
            tokens = tokens.to(clf.device)
        txt_features = clf.model.encode_text(tokens)
        txt_features = txt_features.float().cpu().numpy().astype(np.float32)

    # Normalize to unit length
    vec = txt_features.flatten()
    norm = np.linalg.norm(vec)
    if norm > 0:
        vec = vec / norm
    return vec
