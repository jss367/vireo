"""Encode text queries into the same CLIP embedding space as photo embeddings.

Uses ONNX Runtime for inference with text encoder models stored in
~/.vireo/models/{model-id}/.
"""

import contextlib
import logging
import os
import threading
import time

import numpy as np
import onnx_runtime
from app_paths import default_path

log = logging.getLogger(__name__)

# Cache: model_dir -> (text_session, text_input_name, tokenizer)
_session_cache = {}
# Serializes lookup + (slow) load + cache write so two concurrent jobs don't
# both run the expensive ONNX session creation for the same model and race on
# the dict assignment.
_session_cache_lock = threading.Lock()

# Map model_str identifiers to local model directory names
_MODEL_DIR_MAP = {
    "ViT-B-16": "bioclip-vit-b-16",
    "hf-hub:imageomics/bioclip-2": "bioclip-2",
    "hf-hub:imageomics/bioclip-2.5-vith14": "bioclip-2.5-vith14",
}

_MODELS_ROOT = default_path("models")

# Context length for CLIP-style tokenizers
_CONTEXT_LENGTH = 77
_INTERACTIVE_RESOURCE_WAIT_SECONDS = 5.0


@contextlib.contextmanager
def _session_cache_guard(cancel_check):
    """Acquire cold-session coordination without outliving a caller deadline."""
    while True:
        if cancel_check is not None and cancel_check():
            from resource_ledger import ResourceWaitCancelled

            raise ResourceWaitCancelled(
                "Cancelled while waiting for text session cache resources",
            )
        if _session_cache_lock.acquire(timeout=0.05):
            break
    try:
        yield
    finally:
        _session_cache_lock.release()


def _get_text_session(model_str, pretrained_str=None, *, cancel_check=None):
    """Lazily load and cache a text encoder ONNX session + tokenizer.

    Args:
        model_str: model identifier (determines model directory)
        pretrained_str: optional path to model directory.  When provided and
                        pointing to an existing directory it takes precedence
                        over the default ``~/.vireo/models/<mapped-id>``
                        location so that custom ``weights_path`` registrations
                        are respected.

    Returns:
        (text_session, text_input_name, tokenizer) tuple
    """
    # Resolve model directory – honour configured weights_path first.
    if pretrained_str and os.path.isdir(pretrained_str):
        model_dir = pretrained_str
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
        model_dir = os.path.join(_MODELS_ROOT, dir_name)

    with _session_cache_guard(cancel_check):
        cached = _session_cache.get(model_dir)
        if cached is not None:
            return cached

        log.info("Loading CLIP text encoder for %s...", model_str)
        from tokenizers import Tokenizer

        text_encoder_path = os.path.join(model_dir, "text_encoder.onnx")
        tokenizer_path = os.path.join(model_dir, "tokenizer.json")

        for path, desc in [
            (text_encoder_path, "text encoder ONNX model"),
            (tokenizer_path, "tokenizer"),
        ]:
            if not os.path.isfile(path):
                raise FileNotFoundError(
                    f"{desc} not found at {path}. "
                    "Download the model from the Models page in Settings."
                )

        session = onnx_runtime.create_session(
            text_encoder_path, cancel_check=cancel_check,
        )
        input_name = session.get_inputs()[0].name
        tokenizer = Tokenizer.from_file(tokenizer_path)

        entry = (session, input_name, tokenizer)
        _session_cache[model_dir] = entry
        log.info("CLIP text encoder loaded")
        return entry


def encode_text(query, model_str, pretrained_str=None):
    """Encode a text query into a normalized embedding vector.

    Uses the same model that produced the image embeddings,
    so the resulting vector is directly comparable via cosine similarity.

    Args:
        query: natural language search string (e.g., "bird in flight over water")
        model_str: model identifier (e.g., "ViT-B-16", "hf-hub:imageomics/bioclip-2")
        pretrained_str: optional path to model directory.  When provided and
                        pointing to an existing directory it takes precedence
                        over the default model location.

    Returns:
        numpy float32 array -- normalized text embedding vector
    """
    # Two deadlines rather than one: cold-load construction can
    # legitimately exceed the interactive budget (downloading /
    # deserializing a multi-hundred-megabyte ONNX graph). Sharing one
    # deadline would let a slow cold load consume the entire budget
    # and reject the already-loaded session at ``session.run`` — the
    # first search after startup fails even though its construction
    # lease landed immediately. Reset the deadline AFTER cold
    # construction so ``acquire_inference_resources`` measures only
    # actual inference contention against the interactive budget.
    load_deadline = time.monotonic() + _INTERACTIVE_RESOURCE_WAIT_SECONDS

    def load_cancel_check():
        return time.monotonic() >= load_deadline

    session, input_name, tokenizer = _get_text_session(
        model_str,
        pretrained_str,
        cancel_check=load_cancel_check,
    )

    # Reset the deadline for inference: the cold-load already spent
    # its share, so start a fresh 5s budget for the actual encoder
    # forward pass and any waiting on the ``cpu_ml`` lane.
    infer_deadline = time.monotonic() + _INTERACTIVE_RESOURCE_WAIT_SECONDS

    def infer_cancel_check():
        return time.monotonic() >= infer_deadline

    # Tokenize the query
    encoding = tokenizer.encode(query)
    ids = encoding.ids[:_CONTEXT_LENGTH]
    tokens = np.zeros((1, _CONTEXT_LENGTH), dtype=np.int64)
    tokens[0, : len(ids)] = ids

    # Run text encoder
    from pipeline_locks import acquire_inference_resources
    with acquire_inference_resources(
        session,
        cancel_check=infer_cancel_check,
    ):
        txt_features = session.run(None, {input_name: tokens})[0]
    txt_features = txt_features.astype(np.float32)

    # Normalize to unit length
    vec = txt_features.flatten()
    norm = np.linalg.norm(vec)
    if norm > 0:
        vec = vec / norm
    return vec
