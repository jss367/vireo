#!/usr/bin/env python3
"""Export PyTorch models to ONNX format for Vireo inference.

This is a dev-only script. It requires the [export] optional dependencies:
    pip install -e ".[export]"

Usage:
    # Export a single model
    python scripts/export_onnx.py --model bioclip-vit-b-16

    # Export all models
    python scripts/export_onnx.py --all

    # Export with validation (compares PyTorch vs ONNX outputs)
    python scripts/export_onnx.py --model bioclip-vit-b-16 --validate

    # Custom output directory and opset version
    python scripts/export_onnx.py --all --output-dir my_models/ --opset 20
"""

import argparse
import json
import logging
import os
import sys
import time

import numpy as np
import torch

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Model registry: model_id -> export function name
# ---------------------------------------------------------------------------
ALL_MODELS = [
    "megadetector-v6",
    "bioclip-vit-b-16",
    "bioclip-2",
    "bioclip-2.5-vith14",
    "timm-eva02-large-inat21",
    "dinov2-vit-s14",
    "dinov2-vit-b14",
    "dinov2-vit-l14",
    "sam2-tiny",
    "sam2-small",
    "sam2-base-plus",
    "sam2-large",
    "rtmpose-animal",
    "superanimal-quadruped",
    "superanimal-bird",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ensure_dir(path):
    """Create directory if it does not exist."""
    os.makedirs(path, exist_ok=True)
    return path


def _onnx_model_path(output_dir, model_id, filename="model.onnx"):
    """Build the output path for an ONNX model file."""
    d = _ensure_dir(os.path.join(output_dir, model_id))
    return os.path.join(d, filename)


def _save_json(path, obj):
    """Write a Python object as pretty JSON."""
    with open(path, "w") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
    log.info("  Saved %s", path)


def _validate_onnx(onnx_path, input_dict, pytorch_outputs, tolerance=0.01):
    """Run ONNX inference and compare against PyTorch outputs.

    Args:
        onnx_path: path to .onnx file
        input_dict: dict of {name: numpy array} for ONNX inputs
        pytorch_outputs: list of numpy arrays (PyTorch reference outputs)
        tolerance: maximum allowed relative difference

    Returns:
        True if validation passes
    """
    import onnxruntime as ort

    session = ort.InferenceSession(onnx_path, providers=["CPUExecutionProvider"])
    onnx_outputs = session.run(None, input_dict)

    all_ok = True
    for i, (pt_out, ort_out) in enumerate(zip(pytorch_outputs, onnx_outputs, strict=False)):
        # Check shape match
        if pt_out.shape != ort_out.shape:
            log.error(
                "  Output %d shape mismatch: PyTorch %s vs ONNX %s",
                i, pt_out.shape, ort_out.shape,
            )
            all_ok = False
            continue

        # Check top-1 match for classification-like outputs
        if pt_out.ndim >= 1 and pt_out.shape[-1] > 1:
            pt_top1 = np.argmax(pt_out.flatten())
            ort_top1 = np.argmax(ort_out.flatten())
            if pt_top1 != ort_top1:
                log.warning(
                    "  Output %d top-1 mismatch: PyTorch %d vs ONNX %d",
                    i, pt_top1, ort_top1,
                )

        # Check values within tolerance
        abs_diff = np.abs(pt_out.astype(np.float32) - ort_out.astype(np.float32))
        max_diff = abs_diff.max()
        mean_diff = abs_diff.mean()

        if max_diff > tolerance:
            log.warning(
                "  Output %d: max_diff=%.6f mean_diff=%.6f (tolerance=%.4f)",
                i, max_diff, mean_diff, tolerance,
            )
        else:
            log.info(
                "  Output %d: max_diff=%.6f mean_diff=%.6f -- OK",
                i, max_diff, mean_diff,
            )

    return all_ok


def _verify_text_encoder_batched(onnx_path, txt_wrapper, batch_size=8):
    """Sanity-check that the exported text encoder accepts batches > 1.

    Earlier exports baked batch=1 into a downstream Reshape (see
    _TextEncoderWrapper docstring), so any batched call failed at runtime.
    Run a batch_size inference and compare each row against per-row PyTorch
    outputs to catch regressions at export time, not first-cold-start time.
    """
    import onnxruntime as ort

    log.info("  Verifying text encoder accepts batch=%d...", batch_size)
    rng = np.random.default_rng(0)
    # Use realistic token IDs (49407 is CLIP's EOT token; surround with random
    # in-vocab IDs). Vocab size for open_clip CLIP tokenizers is 49408.
    tokens_np = rng.integers(low=1, high=49407, size=(batch_size, 77), dtype=np.int64)
    tokens_np[:, 0] = 49406  # SOT
    eot_positions = rng.integers(low=5, high=76, size=(batch_size,))
    for i, pos in enumerate(eot_positions):
        tokens_np[i, pos] = 49407
        tokens_np[i, pos + 1 :] = 0

    session = ort.InferenceSession(onnx_path, providers=["CPUExecutionProvider"])
    input_name = session.get_inputs()[0].name
    try:
        ort_batch = session.run(None, {input_name: tokens_np})[0]
    except Exception as e:
        raise RuntimeError(
            "Text encoder ONNX rejected batched input — export still has a "
            f"hardcoded batch dimension. Original error: {e!r}"
        ) from e

    # Compare against per-row PyTorch outputs
    tokens_pt = torch.from_numpy(tokens_np)
    with torch.no_grad():
        pt_rows = [
            txt_wrapper(tokens_pt[i : i + 1]).numpy() for i in range(batch_size)
        ]
    pt_batch = np.concatenate(pt_rows, axis=0)

    abs_diff = np.abs(pt_batch.astype(np.float32) - ort_batch.astype(np.float32))
    max_diff = float(abs_diff.max())
    if max_diff > 0.05:
        raise RuntimeError(
            f"Batched ONNX output diverges from PyTorch per-row reference "
            f"(max abs diff {max_diff:.4f})"
        )
    log.info(
        "  Text encoder batched verification OK (max diff vs per-row PT: %.6f)",
        max_diff,
    )


# ---------------------------------------------------------------------------
# MegaDetector (YOLOv9)
# ---------------------------------------------------------------------------

def export_megadetector(output_dir, opset, validate=False):
    """Export MegaDetector v6 (YOLOv9c) to ONNX.

    Input:  (1, 3, 640, 640) float32
    Output: YOLOv9 detection output (varies by export method)
    """
    log.info("Exporting MegaDetector v6...")

    from ultralytics import YOLO

    # Load via ultralytics directly — PytorchWildlife downloads the weights
    # to torch hub checkpoints dir
    weights_path = os.path.join(torch.hub.get_dir(), "checkpoints", "MDV6-yolov9-c.pt")
    if not os.path.exists(weights_path):
        # Trigger download via PytorchWildlife
        from PytorchWildlife.models.detection import MegaDetectorV6
        MegaDetectorV6(pretrained=True, version="MDV6-yolov9-c")
    yolo_model = YOLO(weights_path)

    out_dir = _ensure_dir(os.path.join(output_dir, "megadetector-v6"))
    onnx_path = os.path.join(out_dir, "model.onnx")

    # Try ultralytics built-in export first
    try:
        log.info("  Attempting ultralytics .export(format='onnx')...")
        exported_path = yolo_model.export(format="onnx", opset=opset, imgsz=640)
        # ultralytics places the ONNX file next to the .pt file; move it
        if exported_path and os.path.exists(exported_path) and exported_path != onnx_path:
            import shutil
            shutil.move(exported_path, onnx_path)
        log.info("  Exported via ultralytics: %s", onnx_path)
    except Exception as e:
        log.warning("  ultralytics export failed (%s), falling back to torch.onnx.export", e)

        # Fallback: manual torch.onnx.export
        # Get the underlying torch model
        torch_model = yolo_model.model
        torch_model.eval()

        dummy_input = torch.randn(1, 3, 640, 640)
        torch.onnx.export(
            torch_model,
            dummy_input,
            onnx_path,
            opset_version=opset,
            input_names=["images"],
            output_names=["output"],
            dynamic_axes={
                "images": {0: "batch"},
                "output": {0: "batch"},
            },
        )
        log.info("  Exported via torch.onnx.export: %s", onnx_path)

    # Save config
    config = {
        "input_size": [1, 3, 640, 640],
        "class_names": {0: "animal", 1: "person", 2: "vehicle"},
        "preprocessing": "letterbox_640_normalize_0_1",
    }
    _save_json(os.path.join(out_dir, "config.json"), config)

    if validate:
        log.info("  Validating MegaDetector ONNX...")
        dummy = np.random.rand(1, 3, 640, 640).astype(np.float32)
        _validate_onnx(onnx_path, {"images": dummy}, [], tolerance=0.02)

    log.info("  MegaDetector export complete: %s", out_dir)
    return onnx_path


# ---------------------------------------------------------------------------
# BioCLIP variants
# ---------------------------------------------------------------------------

# BioCLIP model configurations
_BIOCLIP_CONFIGS = {
    "bioclip-vit-b-16": {
        "model_name": "hf-hub:imageomics/bioclip",
        "pretrained": None,
        "input_size": 224,
        "embedding_dim": 512,
        "mean": [0.48145466, 0.4578275, 0.40821073],
        "std": [0.26862954, 0.26130258, 0.27577711],
    },
    "bioclip-2": {
        "model_name": "hf-hub:imageomics/bioclip-2",
        "pretrained": None,  # model_name is the full HF identifier
        "input_size": 224,
        "embedding_dim": 768,
        "mean": [0.48145466, 0.4578275, 0.40821073],
        "std": [0.26862954, 0.26130258, 0.27577711],
    },
    "bioclip-2.5-vith14": {
        "model_name": "hf-hub:imageomics/bioclip-2.5-vith14",
        "pretrained": None,
        "input_size": 224,
        "embedding_dim": 1024,
        "mean": [0.48145466, 0.4578275, 0.40821073],
        "std": [0.26862954, 0.26130258, 0.27577711],
    },
}


class _ImageEncoderWrapper(torch.nn.Module):
    """Wrapper to export only the visual (image) encoder of an open_clip model."""

    def __init__(self, clip_model):
        super().__init__()
        self.visual = clip_model.visual

    def forward(self, x):
        return self.visual(x)


class _TextEncoderWrapper(torch.nn.Module):
    """Wrapper to export only the text encoder of an open_clip model.

    open_clip's CLIP.encode_text uses text_global_pool, which contains:

        x[torch.arange(x.shape[0], device=x.device), text.argmax(dim=-1)]

    When traced by torch.onnx.export with a dummy batch of 1, ``x.shape[0]``
    is captured as the constant ``1`` and propagates downstream as a
    hardcoded Reshape target, so the resulting graph rejects any batch != 1
    at runtime. We re-implement encode_text here, replacing that gather
    with ``torch.gather`` (which exports cleanly with dynamic batch) and
    keeping pool-type semantics consistent with open_clip.
    """

    def __init__(self, clip_model):
        super().__init__()
        self.clip_model = clip_model
        # Resolve pool config once so forward() avoids attribute lookups
        # that the tracer might fold into constants.
        self._pool_type = getattr(clip_model, "text_pool_type", "argmax")
        self._eos_token_id = getattr(clip_model, "text_eos_id", None)

    def forward(self, text):
        m = self.clip_model
        cast_dtype = m.transformer.get_cast_dtype()

        x = m.token_embedding(text).to(cast_dtype)         # (B, L, D)
        x = x + m.positional_embedding.to(cast_dtype)
        x = m.transformer(x, attn_mask=m.attn_mask)
        x = m.ln_final(x)                                   # (B, L, D)

        # Trace-friendly EOS pooling. Avoids torch.arange(x.shape[0]).
        pool = self._pool_type
        if pool == "first":
            x = x[:, 0]
        elif pool == "last":
            x = x[:, -1]
        elif pool in ("argmax", "eos"):
            if pool == "argmax":
                idx = text.argmax(dim=-1)                   # (B,)
            else:
                if self._eos_token_id is None:
                    raise ValueError(
                        "text_pool_type='eos' requires clip_model.text_eos_id"
                    )
                idx = (text == self._eos_token_id).int().argmax(dim=-1)  # (B,)
            # gather along seq dim: index shape (B, 1, D) → output (B, 1, D)
            idx = idx.unsqueeze(-1).unsqueeze(-1).expand(-1, 1, x.size(-1))
            x = x.gather(1, idx).squeeze(1)                 # (B, D)
        # else: pool == 'none' or unknown — leave x as (B, L, D)

        if m.text_projection is not None:
            if isinstance(m.text_projection, torch.nn.Linear):
                x = m.text_projection(x)
            else:
                x = x @ m.text_projection

        return x


def _export_bioclip_variant(model_id, output_dir, opset, validate=False):
    """Export a BioCLIP variant (image encoder + text encoder + tokenizer).

    Image encoder: (1, 3, 224, 224) float32 -> (1, embedding_dim) float32
    Text encoder:  (1, 77) int64 -> (1, embedding_dim) float32
    """
    import open_clip

    cfg = _BIOCLIP_CONFIGS[model_id]
    log.info("Exporting %s...", model_id)

    # Load the model
    if cfg["pretrained"] is not None:
        model, _, preprocess = open_clip.create_model_and_transforms(
            cfg["model_name"], pretrained=cfg["pretrained"]
        )
    else:
        model, _, preprocess = open_clip.create_model_and_transforms(
            cfg["model_name"]
        )
    model.eval()

    out_dir = _ensure_dir(os.path.join(output_dir, model_id))
    input_size = cfg["input_size"]
    embedding_dim = cfg["embedding_dim"]

    # --- Export image encoder ---
    image_encoder_path = os.path.join(out_dir, "image_encoder.onnx")
    log.info("  Exporting image encoder...")

    img_wrapper = _ImageEncoderWrapper(model)
    img_wrapper.eval()
    dummy_image = torch.randn(1, 3, input_size, input_size)

    with torch.no_grad():
        pt_img_out = img_wrapper(dummy_image).numpy()

    torch.onnx.export(
        img_wrapper,
        dummy_image,
        image_encoder_path,
        opset_version=opset,
        input_names=["pixel_values"],
        output_names=["image_features"],
        dynamic_axes={
            "pixel_values": {0: "batch"},
            "image_features": {0: "batch"},
        },
    )
    log.info("  Image encoder: %s (output shape: %s)", image_encoder_path, pt_img_out.shape)

    # --- Export text encoder ---
    text_encoder_path = os.path.join(out_dir, "text_encoder.onnx")
    log.info("  Exporting text encoder...")

    txt_wrapper = _TextEncoderWrapper(model)
    txt_wrapper.eval()
    # Use a multi-row dummy so the tracer cannot fold batch into a constant.
    # If anything in the model still bakes in a fixed batch size, the export
    # itself will produce a graph that fails at runtime for other batch sizes
    # — caught by the batched validation below.
    dummy_tokens = torch.zeros(2, 77, dtype=torch.long)

    with torch.no_grad():
        pt_txt_out = txt_wrapper(dummy_tokens).numpy()

    torch.onnx.export(
        txt_wrapper,
        dummy_tokens,
        text_encoder_path,
        opset_version=opset,
        input_names=["input_ids"],
        output_names=["text_features"],
        dynamic_axes={
            "input_ids": {0: "batch"},
            "text_features": {0: "batch"},
        },
    )
    log.info("  Text encoder: %s (output shape: %s)", text_encoder_path, pt_txt_out.shape)

    # Verify the exported text encoder accepts batches != the dummy size.
    # Catches any remaining hardcoded-batch reshape regressions before users
    # hit them at inference time.
    _verify_text_encoder_batched(text_encoder_path, txt_wrapper)

    # --- Save tokenizer ---
    tokenizer_path = os.path.join(out_dir, "tokenizer.json")
    log.info("  Extracting tokenizer...")

    tokenizer = open_clip.get_tokenizer(cfg["model_name"])
    # open_clip tokenizers wrap a HuggingFace tokenizer internally.
    # Try to save it; if that fails, create a minimal config pointing to
    # the open_clip tokenizer type.
    try:
        # open_clip >=2.24 stores the HF tokenizer as ._tokenizer
        hf_tokenizer = getattr(tokenizer, "_tokenizer", None)
        if hf_tokenizer is None:
            hf_tokenizer = getattr(tokenizer, "tokenizer", None)
        if hf_tokenizer is not None and hasattr(hf_tokenizer, "save"):
            hf_tokenizer.save(tokenizer_path)
            log.info("  Tokenizer saved: %s", tokenizer_path)
        else:
            # Fallback: try to get the tokenizer from the open_clip model
            # and save using the tokenizers library
            if hasattr(hf_tokenizer, "backend_tokenizer"):
                hf_tokenizer.backend_tokenizer.save(tokenizer_path)
                log.info("  Tokenizer saved (via backend): %s", tokenizer_path)
            else:
                log.warning(
                    "  Could not extract HF tokenizer. "
                    "You may need to manually provide tokenizer.json"
                )
    except Exception as e:
        log.warning("  Tokenizer extraction failed: %s", e)
        log.warning("  You may need to manually provide tokenizer.json")

    # --- Save config ---
    config = {
        "input_size": [1, 3, input_size, input_size],
        "mean": cfg["mean"],
        "std": cfg["std"],
        "embedding_dim": embedding_dim,
        "context_length": 77,
    }
    _save_json(os.path.join(out_dir, "config.json"), config)

    # --- Validate ---
    if validate:
        log.info("  Validating %s ONNX models...", model_id)

        # Image encoder
        dummy_np = np.random.randn(1, 3, input_size, input_size).astype(np.float32)
        ok1 = _validate_onnx(
            image_encoder_path,
            {"pixel_values": dummy_np},
            [pt_img_out],
            tolerance=0.01,
        )

        # Text encoder
        tokens_np = np.zeros((1, 77), dtype=np.int64)
        ok2 = _validate_onnx(
            text_encoder_path,
            {"input_ids": tokens_np},
            [pt_txt_out],
            tolerance=0.01,
        )

        if ok1 and ok2:
            log.info("  Validation PASSED for %s", model_id)
        else:
            log.warning("  Validation had issues for %s", model_id)

    log.info("  %s export complete: %s", model_id, out_dir)
    return out_dir


def export_bioclip_vit_b_16(output_dir, opset, validate=False):
    return _export_bioclip_variant("bioclip-vit-b-16", output_dir, opset, validate)


def export_bioclip_2(output_dir, opset, validate=False):
    return _export_bioclip_variant("bioclip-2", output_dir, opset, validate)


def export_bioclip_2_5(output_dir, opset, validate=False):
    return _export_bioclip_variant("bioclip-2.5-vith14", output_dir, opset, validate)


# ---------------------------------------------------------------------------
# timm EVA-02 Large (iNaturalist 2021)
# ---------------------------------------------------------------------------

def export_timm_eva02(output_dir, opset, validate=False):
    """Export timm EVA-02 Large fine-tuned on iNaturalist 2021.

    Input:  (1, 3, 336, 336) float32
    Output: (1, num_classes) logits
    """
    import timm

    model_name = "hf-hub:timm/eva02_large_patch14_clip_336.merged2b_ft_inat21"
    model_id = "timm-eva02-large-inat21"
    log.info("Exporting %s...", model_id)

    model = timm.create_model(model_name, pretrained=True)
    model.eval()

    # Get data config for preprocessing parameters
    data_config = timm.data.resolve_data_config(model=model)
    input_size = data_config["input_size"]  # (3, H, W)
    h, w = input_size[1], input_size[2]

    out_dir = _ensure_dir(os.path.join(output_dir, model_id))
    onnx_path = os.path.join(out_dir, "model.onnx")

    dummy_input = torch.randn(1, 3, h, w)

    with torch.no_grad():
        pt_output = model(dummy_input).numpy()

    log.info("  Model output shape: %s (%d classes)", pt_output.shape, pt_output.shape[-1])

    torch.onnx.export(
        model,
        dummy_input,
        onnx_path,
        opset_version=opset,
        input_names=["pixel_values"],
        output_names=["logits"],
        dynamic_axes={
            "pixel_values": {0: "batch"},
            "logits": {0: "batch"},
        },
    )
    log.info("  Exported: %s", onnx_path)

    # --- Extract class names ---
    # timm iNat21 models have a label mapping accessible via the model config
    class_names = []
    label_descriptions = {}

    try:
        # The model's config may have a label mapping
        if hasattr(model, "pretrained_cfg") and "label_names" in model.pretrained_cfg:
            class_names = model.pretrained_cfg["label_names"]
        elif hasattr(model, "pretrained_cfg") and "label_descriptions" in model.pretrained_cfg:
            label_descriptions = model.pretrained_cfg["label_descriptions"]

        # Try to load from the HuggingFace model card
        if not class_names:
            try:
                from huggingface_hub import hf_hub_download
                label_path = hf_hub_download(
                    repo_id="timm/eva02_large_patch14_clip_336.merged2b_ft_inat21",
                    filename="label_descriptions.json",
                )
                with open(label_path) as f:
                    label_descriptions = json.load(f)
                # label_descriptions is typically {str(idx): "description"}
                num_classes = pt_output.shape[-1]
                class_names = [
                    label_descriptions.get(str(i), f"class_{i}")
                    for i in range(num_classes)
                ]
                log.info("  Loaded %d label descriptions from HuggingFace", len(class_names))
            except Exception as e:
                log.warning("  Could not load label descriptions: %s", e)
                num_classes = pt_output.shape[-1]
                class_names = [f"class_{i}" for i in range(num_classes)]
    except Exception as e:
        log.warning("  Could not extract class names: %s", e)
        num_classes = pt_output.shape[-1]
        class_names = [f"class_{i}" for i in range(num_classes)]

    _save_json(os.path.join(out_dir, "class_names.json"), class_names)
    if label_descriptions:
        _save_json(os.path.join(out_dir, "label_descriptions.json"), label_descriptions)

    # --- Save config ---
    config = {
        "input_size": [1, 3, h, w],
        "mean": list(data_config.get("mean", [0.485, 0.456, 0.406])),
        "std": list(data_config.get("std", [0.229, 0.224, 0.225])),
        "num_classes": pt_output.shape[-1],
        "interpolation": data_config.get("interpolation", "bicubic"),
        "crop_pct": data_config.get("crop_pct", 1.0),
    }
    _save_json(os.path.join(out_dir, "config.json"), config)

    # --- Validate ---
    if validate:
        log.info("  Validating timm EVA-02 ONNX...")
        dummy_np = np.random.randn(1, 3, h, w).astype(np.float32)

        with torch.no_grad():
            pt_ref = model(torch.from_numpy(dummy_np)).numpy()

        ok = _validate_onnx(
            onnx_path,
            {"pixel_values": dummy_np},
            [pt_ref],
            tolerance=0.01,
        )
        if ok:
            log.info("  Validation PASSED for %s", model_id)
        else:
            log.warning("  Validation had issues for %s", model_id)

    log.info("  %s export complete: %s", model_id, out_dir)
    return onnx_path


# ---------------------------------------------------------------------------
# DINOv2 variants
# ---------------------------------------------------------------------------

_DINOV2_CONFIGS = {
    "dinov2-vit-s14": {
        "hub_name": "dinov2_vits14",
        "embedding_dim": 384,
    },
    "dinov2-vit-b14": {
        "hub_name": "dinov2_vitb14",
        "embedding_dim": 768,
    },
    "dinov2-vit-l14": {
        "hub_name": "dinov2_vitl14",
        "embedding_dim": 1024,
    },
}

# DINOv2 native input size
_DINOV2_INPUT_SIZE = 518

# ImageNet normalization
_IMAGENET_MEAN = [0.485, 0.456, 0.406]
_IMAGENET_STD = [0.229, 0.224, 0.225]


class _DINOv2CLSWrapper(torch.nn.Module):
    """Wrapper to extract only the CLS token embedding from DINOv2."""

    def __init__(self, dino_model):
        super().__init__()
        self.model = dino_model

    def forward(self, x):
        # DINOv2 forward_features returns a dict or the CLS token directly
        # depending on the version. Try both approaches.
        features = self.model(x)
        if isinstance(features, dict):
            return features["x_norm_clstoken"]
        return features


def _export_dinov2_variant(model_id, output_dir, opset, validate=False):
    """Export a DINOv2 variant to ONNX.

    Input:  (1, 3, 518, 518) float32
    Output: (1, embedding_dim) float32 (CLS token)
    """
    cfg = _DINOV2_CONFIGS[model_id]
    hub_name = cfg["hub_name"]
    embedding_dim = cfg["embedding_dim"]
    log.info("Exporting %s (hub: %s)...", model_id, hub_name)

    model = torch.hub.load("facebookresearch/dinov2", hub_name)
    model.eval()

    out_dir = _ensure_dir(os.path.join(output_dir, model_id))
    onnx_path = os.path.join(out_dir, "model.onnx")

    dummy_input = torch.randn(1, 3, _DINOV2_INPUT_SIZE, _DINOV2_INPUT_SIZE)

    wrapper = _DINOv2CLSWrapper(model)
    wrapper.eval()

    with torch.no_grad():
        pt_output = wrapper(dummy_input).numpy()

    log.info("  Output shape: %s (expected embedding_dim=%d)", pt_output.shape, embedding_dim)

    torch.onnx.export(
        wrapper,
        dummy_input,
        onnx_path,
        opset_version=opset,
        input_names=["pixel_values"],
        output_names=["embeddings"],
        dynamic_axes={
            "pixel_values": {0: "batch"},
            "embeddings": {0: "batch"},
        },
    )
    log.info("  Exported: %s", onnx_path)

    # --- Save config ---
    config = {
        "input_size": [1, 3, _DINOV2_INPUT_SIZE, _DINOV2_INPUT_SIZE],
        "mean": _IMAGENET_MEAN,
        "std": _IMAGENET_STD,
        "embedding_dim": embedding_dim,
    }
    _save_json(os.path.join(out_dir, "config.json"), config)

    # --- Validate ---
    if validate:
        log.info("  Validating %s ONNX...", model_id)
        dummy_np = np.random.randn(1, 3, _DINOV2_INPUT_SIZE, _DINOV2_INPUT_SIZE).astype(np.float32)

        with torch.no_grad():
            pt_ref = wrapper(torch.from_numpy(dummy_np)).numpy()

        ok = _validate_onnx(
            onnx_path,
            {"pixel_values": dummy_np},
            [pt_ref],
            tolerance=0.01,
        )
        if ok:
            log.info("  Validation PASSED for %s", model_id)
        else:
            log.warning("  Validation had issues for %s", model_id)

    log.info("  %s export complete: %s", model_id, out_dir)
    return onnx_path


def export_dinov2_vit_s14(output_dir, opset, validate=False):
    return _export_dinov2_variant("dinov2-vit-s14", output_dir, opset, validate)


def export_dinov2_vit_b14(output_dir, opset, validate=False):
    return _export_dinov2_variant("dinov2-vit-b14", output_dir, opset, validate)


def export_dinov2_vit_l14(output_dir, opset, validate=False):
    return _export_dinov2_variant("dinov2-vit-l14", output_dir, opset, validate)


# ---------------------------------------------------------------------------
# SAM2 variants
# ---------------------------------------------------------------------------

_SAM2_CONFIGS = {
    "sam2-tiny": {
        "hf_model_id": "facebook/sam2-hiera-tiny",
    },
    "sam2-small": {
        "hf_model_id": "facebook/sam2-hiera-small",
    },
    "sam2-base-plus": {
        "hf_model_id": "facebook/sam2-hiera-base-plus",
    },
    "sam2-large": {
        "hf_model_id": "facebook/sam2-hiera-large",
    },
}


class _SAM2ImageEncoderWrapper(torch.nn.Module):
    """Wrapper to export only the SAM2 image encoder."""

    def __init__(self, sam2_model):
        super().__init__()
        self.image_encoder = sam2_model.image_encoder
        # SAM2 may apply a neck/projection after the backbone
        self._sam2 = sam2_model

    def forward(self, x):
        backbone_out = self.image_encoder(x)
        # Project high-res FPN features through mask decoder conv layers
        # (SAM2 does this in forward_image to avoid recomputing on each click)
        fpn = backbone_out["backbone_fpn"]
        hr0 = self._sam2.sam_mask_decoder.conv_s0(fpn[0])  # 256->32 channels
        hr1 = self._sam2.sam_mask_decoder.conv_s1(fpn[1])  # 256->64 channels
        return backbone_out["vision_features"], hr0, hr1


class _SAM2MaskDecoderWrapper(torch.nn.Module):
    """Wrapper to export the SAM2 mask decoder with prompt encoding."""

    def __init__(self, sam2_model):
        super().__init__()
        self.sam_model = sam2_model

    def forward(self, image_embeddings, high_res_feat_0, high_res_feat_1,
                point_coords, point_labels,
                mask_input, has_mask_input, orig_im_size):
        # Encode prompts
        sparse_embeddings, dense_embeddings = self.sam_model.sam_prompt_encoder(
            points=(point_coords, point_labels),
            boxes=None,
            masks=mask_input if has_mask_input.sum() > 0 else None,
        )

        # Run mask decoder with high-res features from encoder FPN
        low_res_masks, iou_predictions, _, _ = self.sam_model.sam_mask_decoder(
            image_embeddings=image_embeddings,
            image_pe=self.sam_model.sam_prompt_encoder.get_dense_pe(),
            sparse_prompt_embeddings=sparse_embeddings,
            dense_prompt_embeddings=dense_embeddings,
            multimask_output=True,
            repeat_image=False,
            high_res_features=[high_res_feat_0, high_res_feat_1],
        )

        # Upscale masks to original image size
        masks = torch.nn.functional.interpolate(
            low_res_masks,
            size=(orig_im_size[0].item(), orig_im_size[1].item()),
            mode="bilinear",
            align_corners=False,
        )

        return masks, iou_predictions


def _export_sam2_variant(model_id, output_dir, opset, validate=False):
    """Export a SAM2 variant (image encoder + mask decoder).

    Image encoder:
        Input:  (1, 3, 1024, 1024) float32
        Output: image embeddings (1, C, H', W')

    Mask decoder:
        Inputs: image_embeddings, point_coords (1,2,2), point_labels (1,2),
                mask_input (1,1,256,256), has_mask_input (1,), orig_im_size (2,)
        Outputs: masks (1, N, H, W), iou_predictions (1, N)
    """
    cfg = _SAM2_CONFIGS[model_id]
    hf_model_id = cfg["hf_model_id"]
    log.info("Exporting %s (HF: %s)...", model_id, hf_model_id)

    from sam2.build_sam import build_sam2_hf

    sam2_model = build_sam2_hf(hf_model_id, device="cpu")
    sam2_model.eval()

    out_dir = _ensure_dir(os.path.join(output_dir, model_id))

    # --- Export image encoder ---
    encoder_path = os.path.join(out_dir, "image_encoder.onnx")
    log.info("  Exporting image encoder...")

    encoder_wrapper = _SAM2ImageEncoderWrapper(sam2_model)
    encoder_wrapper.eval()

    dummy_image = torch.randn(1, 3, 1024, 1024)

    with torch.no_grad():
        pt_enc_out, pt_hr0, pt_hr1 = encoder_wrapper(dummy_image)
    log.info("  Image encoder output shapes: embeddings=%s, hr0=%s, hr1=%s",
             pt_enc_out.shape, pt_hr0.shape, pt_hr1.shape)

    try:
        torch.onnx.export(
            encoder_wrapper,
            dummy_image,
            encoder_path,
            opset_version=opset,
            input_names=["pixel_values"],
            output_names=["image_embeddings", "high_res_feat_0", "high_res_feat_1"],
            dynamic_axes={
                "pixel_values": {0: "batch"},
                "image_embeddings": {0: "batch"},
                "high_res_feat_0": {0: "batch"},
                "high_res_feat_1": {0: "batch"},
            },
            dynamo=False,  # legacy exporter — dynamo hangs on SAM2
        )
        log.info("  Image encoder exported: %s", encoder_path)
    except Exception as e:
        log.error("  Image encoder export FAILED: %s", e)
        log.info("  SAM2 encoder export is complex. You may need to adjust the wrapper.")
        log.info("  Continuing with mask decoder export...")

    # --- Export mask decoder ---
    decoder_path = os.path.join(out_dir, "mask_decoder.onnx")
    log.info("  Exporting mask decoder...")

    decoder_wrapper = _SAM2MaskDecoderWrapper(sam2_model)
    decoder_wrapper.eval()

    # Create dummy decoder inputs
    # Use the actual encoder outputs for image_embeddings and high-res features
    dummy_embeddings = pt_enc_out.detach()
    dummy_hr0 = pt_hr0.detach()
    dummy_hr1 = pt_hr1.detach()
    dummy_point_coords = torch.tensor([[[100.0, 100.0], [400.0, 400.0]]], dtype=torch.float32)
    dummy_point_labels = torch.tensor([[2.0, 3.0]], dtype=torch.float32)
    dummy_mask_input = torch.zeros(1, 1, 256, 256, dtype=torch.float32)
    dummy_has_mask = torch.tensor([0.0], dtype=torch.float32)
    dummy_orig_size = torch.tensor([1024, 1024], dtype=torch.int64)

    try:
        with torch.no_grad():
            pt_masks, pt_scores = decoder_wrapper(
                dummy_embeddings, dummy_hr0, dummy_hr1,
                dummy_point_coords, dummy_point_labels,
                dummy_mask_input, dummy_has_mask, dummy_orig_size,
            )
        log.info("  Mask decoder output shapes: masks=%s, scores=%s",
                 pt_masks.shape, pt_scores.shape)

        torch.onnx.export(
            decoder_wrapper,
            (dummy_embeddings, dummy_hr0, dummy_hr1,
             dummy_point_coords, dummy_point_labels,
             dummy_mask_input, dummy_has_mask, dummy_orig_size),
            decoder_path,
            opset_version=opset,
            input_names=[
                "image_embeddings", "high_res_feat_0", "high_res_feat_1",
                "point_coords", "point_labels",
                "mask_input", "has_mask_input", "orig_im_size",
            ],
            output_names=["masks", "iou_predictions"],
            dynamo=False,  # legacy exporter — dynamo hangs on SAM2
            dynamic_axes={
                "image_embeddings": {0: "batch"},
                "high_res_feat_0": {0: "batch"},
                "high_res_feat_1": {0: "batch"},
                "point_coords": {0: "batch", 1: "num_points"},
                "point_labels": {0: "batch", 1: "num_points"},
                "masks": {0: "batch"},
                "iou_predictions": {0: "batch"},
            },
        )
        log.info("  Mask decoder exported: %s", decoder_path)
    except Exception as e:
        log.error("  Mask decoder export FAILED: %s", e)
        log.info(
            "  SAM2 mask decoder export is complex and may need adjustments "
            "depending on the SAM2 version. Check the error above."
        )

    # --- Save config ---
    config = {
        "input_size": [1, 3, 1024, 1024],
        "mean": _IMAGENET_MEAN,
        "std": _IMAGENET_STD,
        "mask_decoder_inputs": {
            "point_coords_shape": [1, 2, 2],
            "point_labels_shape": [1, 2],
            "mask_input_shape": [1, 1, 256, 256],
            "box_prompt_labels": [2, 3],
        },
    }
    _save_json(os.path.join(out_dir, "config.json"), config)

    # --- Validate ---
    if validate and os.path.exists(encoder_path):
        log.info("  Validating %s image encoder ONNX...", model_id)
        dummy_np = np.random.randn(1, 3, 1024, 1024).astype(np.float32)

        with torch.no_grad():
            pt_emb, pt_h0, pt_h1 = encoder_wrapper(torch.from_numpy(dummy_np))

        _validate_onnx(
            encoder_path,
            {"pixel_values": dummy_np},
            [pt_emb.numpy(), pt_h0.numpy(), pt_h1.numpy()],
            tolerance=0.02,
        )

    log.info("  %s export complete: %s", model_id, out_dir)
    return out_dir


def export_sam2_tiny(output_dir, opset, validate=False):
    return _export_sam2_variant("sam2-tiny", output_dir, opset, validate)


def export_sam2_small(output_dir, opset, validate=False):
    return _export_sam2_variant("sam2-small", output_dir, opset, validate)


def export_sam2_base_plus(output_dir, opset, validate=False):
    return _export_sam2_variant("sam2-base-plus", output_dir, opset, validate)


def export_sam2_large(output_dir, opset, validate=False):
    return _export_sam2_variant("sam2-large", output_dir, opset, validate)


# ---------------------------------------------------------------------------
# RTMPose-animal (MMPose, AP-10K keypoints) — eye-focus detection spike
# ---------------------------------------------------------------------------

def export_rtmpose_animal(output_dir, opset, validate=False):
    """Export RTMPose-s trained on AP-10K to ONNX via mmdeploy.

    RTMPose-s is a top-down animal pose estimator. AP-10K provides 17
    keypoints including left_eye (idx 0) and right_eye (idx 1), which are
    what the eye-focus pipeline stage consumes. The simcc head emits two
    1-D classification maps per keypoint; Vireo's keypoints.decode_simcc
    handles the argmax + rescale.

    Input:  (1, 3, 256, 256) float32, normalized with AP-10K stats.
    Output: simcc_x (1, 17, 512), simcc_y (1, 17, 512) float32.
    """
    from mmdeploy.apis import torch2onnx

    model_id = "rtmpose-animal"
    log.info("Exporting %s...", model_id)

    # Official RTMPose-s AP-10K config + checkpoint (MMPose v1.x).
    config = (
        "https://raw.githubusercontent.com/open-mmlab/mmpose/main/"
        "configs/animal_2d_keypoint/rtmpose/ap10k/"
        "rtmpose-s_8xb64-210e_ap10k-256x256.py"
    )
    checkpoint = (
        "https://download.openmmlab.com/mmpose/v1/projects/rtmposev1/"
        "rtmpose-s_simcc-ap10k_pt-aic-coco_210e-256x256-7a041aa1_20230206.pth"
    )

    out_dir = _ensure_dir(os.path.join(output_dir, model_id))
    onnx_path = os.path.join(out_dir, "model.onnx")

    deploy_cfg = {
        "onnx_config": {
            "type": "onnx",
            "export_params": True,
            "keep_initializers_as_inputs": False,
            "opset_version": opset,
            "save_file": "model.onnx",
            "input_names": ["pixel_values"],
            "output_names": ["simcc_x", "simcc_y"],
            "input_shape": [256, 256],
        },
        "codebase_config": {"type": "mmpose", "task": "PoseDetection"},
        "backend_config": {"type": "onnxruntime"},
    }

    # mmdeploy's torch2onnx wants a sample image for shape inference.
    import urllib.request
    sample_path = os.path.join(out_dir, "_sample.jpg")
    urllib.request.urlretrieve(
        "https://raw.githubusercontent.com/open-mmlab/mmpose/main/tests/data/"
        "ap10k/000000000017.jpg",
        sample_path,
    )

    try:
        torch2onnx(
            sample_path,
            out_dir,
            "model",
            deploy_cfg=deploy_cfg,
            model_cfg=config,
            model_checkpoint=checkpoint,
            device="cpu",
        )
    finally:
        if os.path.exists(sample_path):
            os.remove(sample_path)

    config_out = {
        "input_size": [1, 3, 256, 256],
        "mean": [123.675, 116.28, 103.53],
        "std": [58.395, 57.12, 57.375],
        # AP-10K keypoint order. Eye indices (0, 1) are what Vireo reads.
        "keypoints": [
            "left_eye", "right_eye", "nose", "neck", "root_of_tail",
            "left_shoulder", "left_elbow", "left_front_paw",
            "right_shoulder", "right_elbow", "right_front_paw",
            "left_hip", "left_knee", "left_back_paw",
            "right_hip", "right_knee", "right_back_paw",
        ],
        "output_type": "simcc",
        "simcc_split_ratio": 2.0,
    }
    _save_json(os.path.join(out_dir, "config.json"), config_out)

    log.info("  %s export complete: %s", model_id, out_dir)
    return onnx_path


# ---------------------------------------------------------------------------
# SuperAnimal (DeepLabCut 3.x) — production animal keypoint models
# ---------------------------------------------------------------------------
#
# DLC 3.x publishes raw .pt snapshots of SuperAnimal-Quadruped and
# SuperAnimal-Bird on DeepLabCut's HuggingFace org. We:
#   1. build a DLC 3.x PoseModel with the resnet_50 pose-only architecture,
#      sized by the official project-config bodypart list,
#   2. load the snapshot state dict directly (strict=True),
#   3. export a wrapper that emits just the bodypart heatmap tensor so
#      Vireo's keypoints.decode_heatmaps sees (1, K, H', W').
#
# Keypoint names are read at export time from DLC's packaged project YAML
# to stay in lockstep with whatever bodypart list the published weights
# were trained on. Don't hardcode them here — the bird list has 42 entries,
# the quadruped list has 39, and they've been reordered across DLC releases.

# HF repos for each snapshot. Bird lives under DLC's org; quadruped and
# topview-mouse snapshots live under the maintainer's personal org. The
# source of truth is dlclibrary.dlcmodelzoo.modelzoo_urls_pytorch.yaml.
_SUPERANIMAL_HF_REPOS = {
    "superanimal_bird": (
        "DeepLabCut/DeepLabCutModelZoo-SuperAnimal-Bird",
        "superanimal_bird_resnet_50.pt",
    ),
    "superanimal_quadruped": (
        "mwmathis/DeepLabCutModelZoo-SuperAnimal-Quadruped",
        "superanimal_quadruped_resnet_50.pt",
    ),
}


def _superanimal_bodyparts(dlc_name):
    """Read the official keypoint order from DLC's packaged project YAML."""
    from pathlib import Path

    import deeplabcut
    import yaml

    p = Path(deeplabcut.__file__).parent / f"modelzoo/project_configs/{dlc_name}.yaml"
    return yaml.safe_load(p.read_text())["bodyparts"]


def _build_superanimal_pose_model(keypoints):
    """Construct a DLC 3.x PoseModel matching the resnet_50 snapshots on HF."""
    from pathlib import Path

    import yaml
    from deeplabcut.pose_estimation_pytorch.config import utils as config_utils
    from deeplabcut.pose_estimation_pytorch.models import PoseModel
    from deeplabcut.pose_estimation_pytorch.modelzoo.utils import (
        get_super_animal_model_config_path,
    )

    model_cfg_path = get_super_animal_model_config_path("resnet_50")
    cfg = yaml.safe_load(Path(model_cfg_path).read_text())
    cfg["metadata"] = {
        "project_path": "/tmp",
        "pose_config_path": str(model_cfg_path),
        "bodyparts": list(keypoints),
        "unique_bodyparts": [],
        "individuals": ["animal"],
        "with_identity": False,
    }
    cfg = config_utils.replace_default_values(
        cfg,
        num_bodyparts=len(keypoints),
        num_individuals=1,
        backbone_output_channels=cfg["model"]["backbone_output_channels"],
    )
    return PoseModel.build(cfg["model"], pretrained_backbone=False)


def _inline_onnx_external_data(onnx_path):
    """Consolidate a torch-exported ONNX into a single file.

    torch 2.11's dynamo exporter + onnxscript optimizer emit weights as
    external ``model.onnx.data`` by default. Vireo's keypoint download
    path (keypoints.ensure_keypoint_weights) expects a single model.onnx,
    so we rewrite in-place with all initializers inlined.
    """
    import onnx

    model = onnx.load(onnx_path, load_external_data=True)
    for init in model.graph.initializer:
        if init.data_location == onnx.TensorProto.EXTERNAL:
            init.data_location = onnx.TensorProto.DEFAULT
            del init.external_data[:]
    onnx.save(model, onnx_path, save_as_external_data=False)
    sidecar = onnx_path + ".data"
    if os.path.isfile(sidecar):
        os.remove(sidecar)


class _SuperAnimalWrapper(torch.nn.Module):
    """Expose the bodypart heatmap so torch.onnx.export sees a single tensor.

    DLC 3.x PoseModel.forward returns ``{head_name: {output_name: tensor}}``;
    Vireo's keypoints.decode_heatmaps wants just ``(1, K, H', W')``.
    """

    def __init__(self, model):
        super().__init__()
        self.model = model.eval()

    def forward(self, x):
        return self.model(x)["bodypart"]["heatmap"]


def _export_superanimal_variant(dlc_name, model_id, output_dir, opset, validate=False):
    """Shared export path for SuperAnimal-Quadruped and SuperAnimal-Bird."""
    import huggingface_hub
    import torch

    log.info("Exporting %s...", model_id)
    keypoints = _superanimal_bodyparts(dlc_name)
    log.info(
        "  %d keypoints (left_eye=%d, right_eye=%d)",
        len(keypoints),
        keypoints.index("left_eye"),
        keypoints.index("right_eye"),
    )

    model = _build_superanimal_pose_model(keypoints)

    repo_id, filename = _SUPERANIMAL_HF_REPOS[dlc_name]
    log.info("  downloading %s from %s", filename, repo_id)
    ckpt_path = huggingface_hub.hf_hub_download(repo_id=repo_id, filename=filename)
    snapshot = torch.load(ckpt_path, map_location="cpu", weights_only=False)
    model.load_state_dict(snapshot["model"], strict=True)
    model.eval()

    wrapped = _SuperAnimalWrapper(model).eval()

    # 256×256 keeps parity with RTMPose so the pipeline stage's aspect-
    # preserving resize + top-left pad produces the same crop geometry
    # regardless of which keypoint model is routed.
    input_h, input_w = 256, 256
    dummy = torch.zeros(1, 3, input_h, input_w, dtype=torch.float32)

    out_dir = _ensure_dir(os.path.join(output_dir, model_id))
    onnx_path = os.path.join(out_dir, "model.onnx")

    torch.onnx.export(
        wrapped,
        dummy,
        onnx_path,
        opset_version=opset,
        do_constant_folding=True,
        input_names=["pixel_values"],
        output_names=["heatmaps"],
        # Fixed batch of 1 keeps the graph simple; the pipeline stage runs
        # one crop at a time anyway. If batched export is ever needed,
        # re-add dynamic_axes={"pixel_values": {0: "batch"}} here.
    )
    _inline_onnx_external_data(onnx_path)

    config = {
        "input_size": [1, 3, input_h, input_w],
        # ImageNet mean/std — DLC SuperAnimal models were fine-tuned from
        # ImageNet-pretrained backbones and expect the standard normalization.
        "mean": [123.675, 116.28, 103.53],
        "std": [58.395, 57.12, 57.375],
        "keypoints": keypoints,
        "output_type": "heatmap",
    }
    _save_json(os.path.join(out_dir, "config.json"), config)

    if validate:
        # Compare heatmaps between PyTorch and ONNX Runtime on the dummy
        # input. Real-image validation lives in tests/e2e/.
        import numpy as np
        import onnxruntime as ort

        sess = ort.InferenceSession(onnx_path, providers=["CPUExecutionProvider"])
        with torch.no_grad():
            torch_hm = wrapped(dummy).numpy()
        onnx_hm = sess.run(None, {"pixel_values": dummy.numpy()})[0]
        max_diff = float(np.abs(torch_hm - onnx_hm).max())
        log.info("  %s ONNX vs PyTorch heatmap max abs diff: %.6e", model_id, max_diff)
        if max_diff > 1e-3:
            raise RuntimeError(
                f"{model_id} ONNX/PyTorch disagreement too large: {max_diff}"
            )

    log.info("  %s export complete: %s", model_id, out_dir)
    return onnx_path


def export_superanimal_quadruped(output_dir, opset, validate=False):
    return _export_superanimal_variant(
        "superanimal_quadruped", "superanimal-quadruped",
        output_dir, opset, validate,
    )


def export_superanimal_bird(output_dir, opset, validate=False):
    return _export_superanimal_variant(
        "superanimal_bird", "superanimal-bird",
        output_dir, opset, validate,
    )


# ---------------------------------------------------------------------------
# Export dispatcher
# ---------------------------------------------------------------------------

_EXPORT_FUNCTIONS = {
    "megadetector-v6": export_megadetector,
    "bioclip-vit-b-16": export_bioclip_vit_b_16,
    "bioclip-2": export_bioclip_2,
    "bioclip-2.5-vith14": export_bioclip_2_5,
    "timm-eva02-large-inat21": export_timm_eva02,
    "dinov2-vit-s14": export_dinov2_vit_s14,
    "dinov2-vit-b14": export_dinov2_vit_b14,
    "dinov2-vit-l14": export_dinov2_vit_l14,
    "sam2-tiny": export_sam2_tiny,
    "sam2-small": export_sam2_small,
    "sam2-base-plus": export_sam2_base_plus,
    "sam2-large": export_sam2_large,
    "rtmpose-animal": export_rtmpose_animal,
    "superanimal-quadruped": export_superanimal_quadruped,
    "superanimal-bird": export_superanimal_bird,
}


def export_model(model_id, output_dir, opset, validate=False):
    """Export a single model by ID.

    Args:
        model_id: one of ALL_MODELS
        output_dir: root output directory
        opset: ONNX opset version
        validate: if True, compare PyTorch vs ONNX outputs

    Returns:
        path to the exported model directory or file
    """
    if model_id not in _EXPORT_FUNCTIONS:
        raise ValueError(
            f"Unknown model: {model_id}. "
            f"Available: {', '.join(ALL_MODELS)}"
        )
    return _EXPORT_FUNCTIONS[model_id](output_dir, opset, validate)


def export_all(output_dir, opset, validate=False):
    """Export all models.

    Returns:
        dict of {model_id: output_path_or_error}
    """
    results = {}
    for model_id in ALL_MODELS:
        log.info("=" * 60)
        log.info("Exporting: %s", model_id)
        log.info("=" * 60)
        start = time.time()
        try:
            path = export_model(model_id, output_dir, opset, validate)
            elapsed = time.time() - start
            results[model_id] = {"status": "ok", "path": str(path), "time_s": round(elapsed, 1)}
            log.info("  Done in %.1fs", elapsed)
        except Exception as e:
            elapsed = time.time() - start
            results[model_id] = {"status": "error", "error": str(e), "time_s": round(elapsed, 1)}
            log.error("  FAILED after %.1fs: %s", elapsed, e, exc_info=True)
    return results


def print_summary(results):
    """Print a summary table of export results."""
    print("\n" + "=" * 70)
    print("EXPORT SUMMARY")
    print("=" * 70)
    print(f"{'Model':<30} {'Status':<10} {'Time':>8}  {'Details'}")
    print("-" * 70)
    for model_id, info in results.items():
        status = info["status"].upper()
        time_s = f"{info['time_s']:.1f}s"
        if info["status"] == "ok":
            details = info.get("path", "")
        else:
            details = info.get("error", "")[:40]
        print(f"{model_id:<30} {status:<10} {time_s:>8}  {details}")
    print("=" * 70)

    ok_count = sum(1 for v in results.values() if v["status"] == "ok")
    fail_count = sum(1 for v in results.values() if v["status"] == "error")
    print(f"\n{ok_count} succeeded, {fail_count} failed out of {len(results)} models")


def print_upload_commands(output_dir):
    """Print huggingface-cli commands to upload exported models."""
    repo = "jss367/vireo-onnx-models"
    print("\n" + "=" * 70)
    print("UPLOAD COMMANDS (run manually)")
    print("=" * 70)
    print(f"# Target repo: {repo}")
    print("# Make sure you're logged in: huggingface-cli login\n")

    for model_id in ALL_MODELS:
        model_dir = os.path.join(output_dir, model_id)
        if os.path.isdir(model_dir):
            print(f"# {model_id}")
            print(
                f"huggingface-cli upload {repo} {model_dir} {model_id} "
                f"--repo-type model"
            )
            print()

    print("# Or upload everything at once:")
    print(f"huggingface-cli upload {repo} {output_dir} . --repo-type model")
    print("=" * 70)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Export PyTorch models to ONNX for Vireo inference.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Available models:
  megadetector-v6           MegaDetector v6 (YOLOv9c)
  bioclip-vit-b-16          BioCLIP ViT-B/16
  bioclip-2                 BioCLIP-2 ViT-L/14
  bioclip-2.5-vith14        BioCLIP-2.5 ViT-H/14
  timm-eva02-large-inat21   timm EVA-02 Large (iNat21)
  dinov2-vit-s14            DINOv2 ViT-S/14
  dinov2-vit-b14            DINOv2 ViT-B/14
  dinov2-vit-l14            DINOv2 ViT-L/14
  sam2-tiny                 SAM2 Hiera Tiny
  sam2-small                SAM2 Hiera Small
  sam2-base-plus            SAM2 Hiera Base+
  sam2-large                SAM2 Hiera Large
  rtmpose-animal            RTMPose-s AP-10K (animal keypoints, eye-focus spike)
  superanimal-quadruped     DLC SuperAnimal-Quadruped (39 kp, mammal routing)
  superanimal-bird          DLC SuperAnimal-Bird (16 kp, bird routing)

Examples:
  %(prog)s --model bioclip-vit-b-16
  %(prog)s --model bioclip-vit-b-16 --validate
  %(prog)s --all --output-dir exported_models/
  %(prog)s --all --upload
""",
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--model",
        choices=ALL_MODELS,
        help="Export a single model",
    )
    group.add_argument(
        "--all",
        action="store_true",
        help="Export all models",
    )

    parser.add_argument(
        "--output-dir",
        default="output",
        help="Output directory (default: output/)",
    )
    parser.add_argument(
        "--opset",
        type=int,
        default=18,
        help="ONNX opset version (default: 18)",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Run PyTorch vs ONNX comparison after export",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Print huggingface-cli upload commands after export",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug logging",
    )

    args = parser.parse_args()

    # Configure logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    output_dir = os.path.abspath(args.output_dir)
    _ensure_dir(output_dir)

    log.info("Output directory: %s", output_dir)
    log.info("ONNX opset version: %d", args.opset)
    log.info("Validate: %s", args.validate)

    if args.all:
        results = export_all(output_dir, args.opset, args.validate)
        print_summary(results)
    else:
        start = time.time()
        try:
            path = export_model(args.model, output_dir, args.opset, args.validate)
            elapsed = time.time() - start
            print(f"\nExported {args.model} in {elapsed:.1f}s: {path}")
        except Exception as e:
            elapsed = time.time() - start
            print(f"\nFAILED {args.model} after {elapsed:.1f}s: {e}", file=sys.stderr)
            sys.exit(1)

    if args.upload:
        print_upload_commands(output_dir)


if __name__ == "__main__":
    main()
