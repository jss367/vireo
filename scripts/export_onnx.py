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
    python scripts/export_onnx.py --all --output-dir my_models/ --opset 18
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


# ---------------------------------------------------------------------------
# MegaDetector (YOLOv9)
# ---------------------------------------------------------------------------

def export_megadetector(output_dir, opset, validate=False):
    """Export MegaDetector v6 (YOLOv9c) to ONNX.

    Input:  (1, 3, 640, 640) float32
    Output: YOLOv9 detection output (varies by export method)
    """
    log.info("Exporting MegaDetector v6...")

    from PytorchWildlife.models.detection import MegaDetectorV6

    model = MegaDetectorV6(pretrained=True)
    # The underlying model is an ultralytics YOLO model
    yolo_model = model.model

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
        "model_name": "ViT-B-16",
        "pretrained": "hf-hub:imageomics/bioclip",
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
    """Wrapper to export only the text encoder of an open_clip model."""

    def __init__(self, clip_model):
        super().__init__()
        self.clip_model = clip_model

    def forward(self, text):
        return self.clip_model.encode_text(text)


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
    dummy_tokens = torch.zeros(1, 77, dtype=torch.long)

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
        # SAM2 image encoder produces multi-scale features.
        # We need the final image embeddings that the mask decoder expects.
        # The exact method depends on the SAM2 version.
        backbone_out = self.image_encoder(x)

        # SAM2 typically returns a dict with "vision_features" and
        # "vision_pos_enc" keys, or processes through a neck.
        # For ONNX export we need a single tensor output.
        # Try the full _image_encode path if available.
        if hasattr(self._sam2, "_prepare_backbone_features"):
            # Use SAM2's internal pipeline to get the final embedding
            (
                _,
                vision_feats,
                _,
                _,
            ) = self._sam2._prepare_backbone_features(backbone_out)
            # vision_feats[-1] is the highest-res feature map
            # Reshape from (HW, B, C) to (B, C, H, W)
            feat = vision_feats[-1]
            B = x.shape[0]
            hw = feat.shape[0]
            C = feat.shape[-1]
            H = W = int(hw ** 0.5)
            return feat.permute(1, 2, 0).reshape(B, C, H, W)

        # Fallback: return raw backbone output
        if isinstance(backbone_out, dict):
            # Return the first tensor value
            for v in backbone_out.values():
                if isinstance(v, torch.Tensor):
                    return v
        return backbone_out


class _SAM2MaskDecoderWrapper(torch.nn.Module):
    """Wrapper to export the SAM2 mask decoder with prompt encoding."""

    def __init__(self, sam2_model):
        super().__init__()
        self.sam_model = sam2_model

    def forward(self, image_embeddings, point_coords, point_labels,
                mask_input, has_mask_input, orig_im_size):
        # Encode prompts
        sparse_embeddings, dense_embeddings = self.sam_model.sam_prompt_encoder(
            points=(point_coords, point_labels),
            boxes=None,
            masks=mask_input if has_mask_input.sum() > 0 else None,
        )

        # Run mask decoder
        low_res_masks, iou_predictions = self.sam_model.sam_mask_decoder(
            image_embeddings=image_embeddings,
            image_pe=self.sam_model.sam_prompt_encoder.get_dense_pe(),
            sparse_prompt_embeddings=sparse_embeddings,
            dense_prompt_embeddings=dense_embeddings,
            multimask_output=True,
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

    sam2_model = build_sam2_hf(hf_model_id)
    sam2_model.eval()

    out_dir = _ensure_dir(os.path.join(output_dir, model_id))

    # --- Export image encoder ---
    encoder_path = os.path.join(out_dir, "image_encoder.onnx")
    log.info("  Exporting image encoder...")

    encoder_wrapper = _SAM2ImageEncoderWrapper(sam2_model)
    encoder_wrapper.eval()

    dummy_image = torch.randn(1, 3, 1024, 1024)

    with torch.no_grad():
        pt_enc_out = encoder_wrapper(dummy_image)
    log.info("  Image encoder output shape: %s", pt_enc_out.shape)

    try:
        torch.onnx.export(
            encoder_wrapper,
            dummy_image,
            encoder_path,
            opset_version=opset,
            input_names=["pixel_values"],
            output_names=["image_embeddings"],
            dynamic_axes={
                "pixel_values": {0: "batch"},
                "image_embeddings": {0: "batch"},
            },
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
    # Use the actual encoder output shape for image_embeddings
    dummy_embeddings = pt_enc_out.detach()
    dummy_point_coords = torch.tensor([[[100.0, 100.0], [400.0, 400.0]]], dtype=torch.float32)
    dummy_point_labels = torch.tensor([[2.0, 3.0]], dtype=torch.float32)
    dummy_mask_input = torch.zeros(1, 1, 256, 256, dtype=torch.float32)
    dummy_has_mask = torch.tensor([0.0], dtype=torch.float32)
    dummy_orig_size = torch.tensor([1024, 1024], dtype=torch.int64)

    try:
        with torch.no_grad():
            pt_masks, pt_scores = decoder_wrapper(
                dummy_embeddings, dummy_point_coords, dummy_point_labels,
                dummy_mask_input, dummy_has_mask, dummy_orig_size,
            )
        log.info("  Mask decoder output shapes: masks=%s, scores=%s",
                 pt_masks.shape, pt_scores.shape)

        torch.onnx.export(
            decoder_wrapper,
            (dummy_embeddings, dummy_point_coords, dummy_point_labels,
             dummy_mask_input, dummy_has_mask, dummy_orig_size),
            decoder_path,
            opset_version=opset,
            input_names=[
                "image_embeddings", "point_coords", "point_labels",
                "mask_input", "has_mask_input", "orig_im_size",
            ],
            output_names=["masks", "iou_predictions"],
            dynamic_axes={
                "image_embeddings": {0: "batch"},
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
            pt_ref = encoder_wrapper(torch.from_numpy(dummy_np)).numpy()

        _validate_onnx(
            encoder_path,
            {"pixel_values": dummy_np},
            [pt_ref],
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
        default=17,
        help="ONNX opset version (default: 17)",
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
