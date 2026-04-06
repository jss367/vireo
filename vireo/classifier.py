"""BioCLIP classifier wrapper for species-level classification.

Uses ONNX Runtime for inference with separate image encoder and text encoder
sessions. Model files are stored in ~/.vireo/models/{model-id}/.
"""

import hashlib
import json
import logging
import os

import numpy as np
import onnx_runtime

log = logging.getLogger(__name__)

CACHE_DIR = os.path.expanduser("~/.vireo/embedding_cache")
_MANIFEST_PATH = os.path.join(CACHE_DIR, "manifest.json")

# Map model_str identifiers to local model directory names
_MODEL_DIR_MAP = {
    "ViT-B-16": "bioclip-vit-b-16",
    "hf-hub:imageomics/bioclip-2": "bioclip-2",
    "hf-hub:imageomics/bioclip-2.5-vith14": "bioclip-2.5-vith14",
}

_MODELS_ROOT = os.path.expanduser("~/.vireo/models")

# Context length for CLIP-style tokenizers (pad/truncate to this length)
_CONTEXT_LENGTH = 77

# Full set of 80 OpenAI ImageNet templates for zero-shot classification.
# Source: openai/CLIP and open_clip. Averaging across all 80 templates
# produces robust text embeddings that match the original BioCLIP pipeline.
OPENAI_IMAGENET_TEMPLATE = [
    lambda c: f"a bad photo of a {c}.",
    lambda c: f"a photo of many {c}.",
    lambda c: f"a sculpture of a {c}.",
    lambda c: f"a photo of the hard to see {c}.",
    lambda c: f"a low resolution photo of the {c}.",
    lambda c: f"a rendering of a {c}.",
    lambda c: f"graffiti of a {c}.",
    lambda c: f"a bad photo of the {c}.",
    lambda c: f"a cropped photo of the {c}.",
    lambda c: f"a tattoo of a {c}.",
    lambda c: f"the embroidered {c}.",
    lambda c: f"a photo of a hard to see {c}.",
    lambda c: f"a bright photo of a {c}.",
    lambda c: f"a photo of a clean {c}.",
    lambda c: f"a photo of a dirty {c}.",
    lambda c: f"a dark photo of the {c}.",
    lambda c: f"a drawing of a {c}.",
    lambda c: f"a photo of my {c}.",
    lambda c: f"the plastic {c}.",
    lambda c: f"a photo of the cool {c}.",
    lambda c: f"a close-up photo of a {c}.",
    lambda c: f"a black and white photo of the {c}.",
    lambda c: f"a painting of the {c}.",
    lambda c: f"a painting of a {c}.",
    lambda c: f"a pixelated photo of the {c}.",
    lambda c: f"a sculpture of the {c}.",
    lambda c: f"a bright photo of the {c}.",
    lambda c: f"a cropped photo of a {c}.",
    lambda c: f"a plastic {c}.",
    lambda c: f"a photo of the dirty {c}.",
    lambda c: f"a jpeg corrupted photo of a {c}.",
    lambda c: f"a blurry photo of the {c}.",
    lambda c: f"a photo of the {c}.",
    lambda c: f"a good photo of the {c}.",
    lambda c: f"a rendering of the {c}.",
    lambda c: f"a {c} in a video game.",
    lambda c: f"a photo of one {c}.",
    lambda c: f"a doodle of a {c}.",
    lambda c: f"a close-up photo of the {c}.",
    lambda c: f"a photo of a {c}.",
    lambda c: f"the origami {c}.",
    lambda c: f"the {c} in a video game.",
    lambda c: f"a sketch of a {c}.",
    lambda c: f"a doodle of the {c}.",
    lambda c: f"a origami {c}.",
    lambda c: f"a low resolution photo of a {c}.",
    lambda c: f"the toy {c}.",
    lambda c: f"a rendition of the {c}.",
    lambda c: f"a photo of the clean {c}.",
    lambda c: f"a photo of a large {c}.",
    lambda c: f"a rendition of a {c}.",
    lambda c: f"a photo of a nice {c}.",
    lambda c: f"a photo of a weird {c}.",
    lambda c: f"a blurry photo of a {c}.",
    lambda c: f"a cartoon {c}.",
    lambda c: f"art of a {c}.",
    lambda c: f"a sketch of the {c}.",
    lambda c: f"a embroidered {c}.",
    lambda c: f"a pixelated photo of a {c}.",
    lambda c: f"itap of the {c}.",
    lambda c: f"a jpeg corrupted photo of the {c}.",
    lambda c: f"a good photo of a {c}.",
    lambda c: f"a plushie {c}.",
    lambda c: f"a photo of the nice {c}.",
    lambda c: f"a photo of the small {c}.",
    lambda c: f"a photo of the weird {c}.",
    lambda c: f"the cartoon {c}.",
    lambda c: f"art of the {c}.",
    lambda c: f"a drawing of the {c}.",
    lambda c: f"a photo of the large {c}.",
    lambda c: f"a black and white photo of a {c}.",
    lambda c: f"the plushie {c}.",
    lambda c: f"a dark photo of a {c}.",
    lambda c: f"itap of a {c}.",
    lambda c: f"graffiti of the {c}.",
    lambda c: f"a toy {c}.",
    lambda c: f"itap of my {c}.",
    lambda c: f"a photo of a cool {c}.",
    lambda c: f"a photo of a small {c}.",
    lambda c: f"a tattoo of the {c}.",
]


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


def _embedding_cache_path(labels, model_str, model_dir=None):
    """Build a cache file path based on a hash of the labels, model, and weights directory.

    ``model_dir`` is included in the key so that two models sharing the same
    ``model_str`` but loaded from different directories (e.g. custom
    ``weights_path`` registrations) never share a cache entry.
    """
    key = model_str + "\n"
    if model_dir:
        key += model_dir + "\n"
    key += "\n".join(labels)
    digest = hashlib.sha256(key.encode()).hexdigest()[:16]
    return os.path.join(CACHE_DIR, f"{digest}.npy")


def _load_tokenizer(tokenizer_path):
    """Load a HuggingFace tokenizer from a JSON file.

    Args:
        tokenizer_path: path to tokenizer.json

    Returns:
        tokenizers.Tokenizer instance
    """
    from tokenizers import Tokenizer

    return Tokenizer.from_file(tokenizer_path)


def _tokenize(tokenizer, texts, context_length=_CONTEXT_LENGTH):
    """Tokenize a list of text strings, padding/truncating to context_length.

    Args:
        tokenizer: tokenizers.Tokenizer instance
        texts: list of strings
        context_length: max sequence length

    Returns:
        numpy int64 array of shape (len(texts), context_length)
    """
    encodings = tokenizer.encode_batch(texts)
    result = np.zeros((len(texts), context_length), dtype=np.int64)
    for i, enc in enumerate(encodings):
        ids = enc.ids[:context_length]
        result[i, : len(ids)] = ids
    return result


def _normalize(vec):
    """L2-normalize a vector or batch of vectors along last axis."""
    norm = np.linalg.norm(vec, axis=-1, keepdims=True)
    # Avoid division by zero
    norm = np.maximum(norm, 1e-8)
    return vec / norm


def _compute_embeddings_with_progress(
    text_session, text_input_name, tokenizer, labels, progress_callback=None
):
    """Compute text embeddings for labels with progress logging.

    For each label, generates text from all templates, encodes via ONNX
    text encoder, and averages the resulting features.

    Args:
        text_session: ONNX InferenceSession for text encoder
        text_input_name: input tensor name for the text session
        tokenizer: tokenizers.Tokenizer instance
        labels: list of label strings
        progress_callback: optional callable(current, total) for UI progress

    Returns:
        numpy float32 array of shape (embedding_dim, num_labels) --
        transposed so it can be used directly for matmul with image features
    """
    total = len(labels)
    log.info("Computing label embeddings: 0/%d", total)
    if progress_callback:
        progress_callback(0, total)

    all_features = []
    for i, classname in enumerate(labels):
        txts = [template(classname) for template in OPENAI_IMAGENET_TEMPLATE]
        tokens = _tokenize(tokenizer, txts)
        txt_features = text_session.run(None, {text_input_name: tokens})[0]
        txt_features = txt_features.astype(np.float32)
        # Normalize each template's output, then average
        txt_features = _normalize(txt_features)
        mean_feature = txt_features.mean(axis=0)
        # Re-normalize the averaged feature
        mean_feature = _normalize(mean_feature)
        all_features.append(mean_feature)

        done = i + 1
        if done % 50 == 0 or done == total:
            log.info("Computing label embeddings: %d/%d", done, total)
            if progress_callback:
                progress_callback(done, total)

    # Stack into (num_labels, embedding_dim) then transpose to (embedding_dim, num_labels)
    stacked = np.stack(all_features, axis=0)  # (num_labels, embedding_dim)
    return stacked.T  # (embedding_dim, num_labels)


class Classifier:
    """Wraps BioCLIP ONNX models for species classification.

    Args:
        labels: list of species/label strings for custom labels mode.
                If None, uses Tree of Life mode with pre-computed embeddings.
        model_str: model identifier (e.g. "ViT-B-16", "hf-hub:imageomics/bioclip-2")
        pretrained_str: optional path to the model directory. When provided and
                        pointing to an existing directory it takes precedence over
                        the default ``~/.vireo/models/<mapped-id>`` location, so
                        models registered at a custom ``weights_path`` are loaded
                        from the correct place.
        embedding_progress_callback: optional callable(current, total) for
                                     embedding computation progress
    """

    def __init__(
        self,
        labels=None,
        model_str="ViT-B-16",
        pretrained_str=None,
        embedding_progress_callback=None,
    ):
        # Resolve model directory.
        # pretrained_str may be a configured weights_path (e.g. from a custom
        # model registration).  Use it directly when it points to an existing
        # directory so that non-default install locations are respected.
        if pretrained_str and os.path.isdir(pretrained_str):
            self._model_dir = pretrained_str
        else:
            if pretrained_str:
                log.warning(
                    "pretrained_str %r is not a directory; falling back to "
                    "default model directory for model_str=%r",
                    pretrained_str,
                    model_str,
                )
            dir_name = _MODEL_DIR_MAP.get(model_str)
            if dir_name is None:
                raise ValueError(
                    f"Unknown BioCLIP model: {model_str}. "
                    f"Known models: {list(_MODEL_DIR_MAP.keys())}"
                )
            self._model_dir = os.path.join(_MODELS_ROOT, dir_name)
        image_encoder_path = os.path.join(self._model_dir, "image_encoder.onnx")
        text_encoder_path = os.path.join(self._model_dir, "text_encoder.onnx")
        tokenizer_path = os.path.join(self._model_dir, "tokenizer.json")
        config_path = os.path.join(self._model_dir, "config.json")

        # Validate required files
        for path, desc in [
            (image_encoder_path, "image encoder ONNX model"),
            (config_path, "preprocessing config"),
        ]:
            if not os.path.isfile(path):
                raise FileNotFoundError(
                    f"{desc} not found at {path}. "
                    "Download the model from the Models page in Settings."
                )

        # Load preprocessing config
        with open(config_path) as f:
            preproc = json.load(f)
        self._input_size = tuple(preproc["input_size"][-2:])  # (H, W)
        self._mean = preproc["mean"]
        self._std = preproc["std"]

        # Load image encoder ONNX session
        log.info("Loading BioCLIP image encoder: %s", image_encoder_path)
        self._image_session = onnx_runtime.create_session(image_encoder_path)
        self._image_input_name = self._image_session.get_inputs()[0].name

        if labels is not None:
            if not labels:
                raise ValueError("labels list must not be empty")

            # Custom labels mode: need text encoder + tokenizer
            for path, desc in [
                (text_encoder_path, "text encoder ONNX model"),
                (tokenizer_path, "tokenizer"),
            ]:
                if not os.path.isfile(path):
                    raise FileNotFoundError(
                        f"{desc} not found at {path}. "
                        "Download the model from the Models page in Settings."
                    )

            self._classes = [cls.strip() for cls in labels]

            cache_path = _embedding_cache_path(labels, model_str, self._model_dir)

            if os.path.exists(cache_path):
                log.info(
                    "Loading cached label embeddings for %d labels...", len(labels)
                )
                self._txt_embeddings = np.load(cache_path)
                log.info("Label embeddings loaded from cache")
            else:
                log.info(
                    "Computing label embeddings for %d labels "
                    "(first run -- will be cached for next time)...",
                    len(labels),
                )
                # Load text encoder session
                text_session = onnx_runtime.create_session(text_encoder_path)
                text_input_name = text_session.get_inputs()[0].name
                tokenizer = _load_tokenizer(tokenizer_path)

                self._txt_embeddings = _compute_embeddings_with_progress(
                    text_session,
                    text_input_name,
                    tokenizer,
                    self._classes,
                    progress_callback=embedding_progress_callback,
                )
                os.makedirs(CACHE_DIR, exist_ok=True)
                np.save(cache_path, self._txt_embeddings)
                # Update manifest with human-readable metadata
                from datetime import datetime

                manifest = _load_manifest()
                manifest[os.path.basename(cache_path)] = {
                    "model": model_str,
                    "model_dir": self._model_dir,
                    "label_count": len(labels),
                    "created": datetime.now().isoformat(timespec="seconds"),
                }
                _save_manifest(manifest)
                log.info("Label embeddings computed and cached to disk")

            self._mode = "custom"
        else:
            # Tree of Life mode: load pre-computed embeddings
            log.info("Loading Tree of Life classifier...")
            tol_embeddings_path = os.path.join(
                self._model_dir, "tol_embeddings.npy"
            )
            tol_classes_path = os.path.join(self._model_dir, "tol_classes.json")

            for path, desc in [
                (tol_embeddings_path, "Tree of Life embeddings"),
                (tol_classes_path, "Tree of Life classes"),
            ]:
                if not os.path.isfile(path):
                    raise FileNotFoundError(
                        f"{desc} not found at {path}. "
                        "Download the model from the Models page in Settings."
                    )

            self._txt_embeddings = np.load(tol_embeddings_path)
            with open(tol_classes_path) as f:
                self._tol_classes = json.load(f)
            log.info(
                "Tree of Life classifier ready: %d species",
                len(self._tol_classes),
            )
            self._mode = "tol"

    def _preprocess(self, image):
        """Preprocess a PIL Image for ONNX inference.

        Args:
            image: PIL Image (will be converted to RGB)

        Returns:
            numpy float32 array of shape (1, 3, H, W)
        """
        return onnx_runtime.preprocess_image(
            image,
            size=self._input_size,
            mean=self._mean,
            std=self._std,
            center_crop=True,
        )

    def _get_image_embedding(self, image):
        """Compute a normalized image embedding from a PIL Image or file path.

        Args:
            image: file path (str) or PIL Image

        Returns:
            numpy float32 array of shape (1, embedding_dim) -- normalized
        """
        from PIL import Image as PILImage

        if isinstance(image, (str, os.PathLike)):
            with PILImage.open(image) as img:
                input_arr = self._preprocess(img)
        else:
            input_arr = self._preprocess(image)

        features = self._image_session.run(
            None, {self._image_input_name: input_arr}
        )[0]
        features = features.astype(np.float32)
        return _normalize(features)

    def _build_custom_results(self, probs, threshold):
        """Build sorted prediction dicts from a probability array (custom labels mode)."""
        ranked = sorted(
            zip(self._classes, probs), key=lambda x: x[1], reverse=True
        )
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

    def _build_tol_results(self, probs, threshold):
        """Build sorted prediction dicts from a probability array (Tree of Life mode).

        Each entry in tol_classes is a dict with taxonomy fields.
        """
        indexed = sorted(enumerate(probs), key=lambda x: x[1], reverse=True)
        results = []
        for idx, score in indexed:
            score = float(score)
            if score < threshold:
                break  # sorted, so remaining are below threshold

            entry = self._tol_classes[idx]
            species = entry.get("common_name") or entry.get("species", "")
            result = {
                "species": species,
                "score": score,
                "auto_tag": f"auto:{species}",
                "confidence_tag": f"auto:confidence:{score:.2f}",
            }
            taxonomy = {}
            for rank in (
                "kingdom",
                "phylum",
                "class",
                "order",
                "family",
                "genus",
            ):
                if rank in entry and entry[rank]:
                    taxonomy[rank] = entry[rank]
            if entry.get("species"):
                taxonomy["scientific_name"] = entry["species"]
            if taxonomy:
                result["taxonomy"] = taxonomy
            results.append(result)
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

        Single forward pass -- computes the image embedding once, uses it for
        classification, and returns it for downstream use (e.g. similarity grouping).

        Args:
            image: file path (str) or PIL Image

        Returns:
            (predictions, embedding) where:
                predictions: list of dicts with species, score, auto_tag, confidence_tag
                embedding: numpy float32 array (the normalized image embedding vector)
        """
        img_features = self._get_image_embedding(image)  # (1, embedding_dim)
        embedding = img_features.flatten()

        # Cosine similarity: img_features @ txt_embeddings
        # img_features: (1, D), txt_embeddings: (D, num_labels)
        logits = 100.0 * (img_features @ self._txt_embeddings)  # (1, num_labels)
        probs = onnx_runtime.softmax(logits, axis=-1).flatten()

        if self._mode == "custom":
            return self._build_custom_results(probs, threshold), embedding
        else:
            return self._build_tol_results(probs, threshold), embedding

    def classify_batch_with_embedding(self, images, threshold=0.4):
        """Classify multiple PIL images.

        Processes each image individually through the ONNX image encoder.
        TODO: batch ONNX inference (stack preprocessed arrays along batch dim)
        would improve throughput for large classification jobs.

        Args:
            images: list of PIL Images
            threshold: minimum confidence to include

        Returns:
            list of (predictions, embedding) tuples
        """
        results = []
        for img in images:
            img_features = self._get_image_embedding(img)  # (1, D)
            embedding = img_features.flatten()

            logits = 100.0 * (img_features @ self._txt_embeddings)
            probs = onnx_runtime.softmax(logits, axis=-1).flatten()

            if self._mode == "custom":
                preds = self._build_custom_results(probs, threshold)
            else:
                preds = self._build_tol_results(probs, threshold)
            results.append((preds, embedding))
        return results
