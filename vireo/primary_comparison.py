"""Provenance and compatibility of the quality measurements on an active mask."""

import hashlib
import io
import json
from pathlib import Path

from detection_id import detection_prompt_runtime
from masking import sam_generation
from PIL import Image
from repositories.masks_features import MaskQuality
from subject_readiness import subject_analysis_readiness


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def measurement_inputs(proxy, detection_id, runtime, generation):
    info = getattr(proxy, "info", {})
    if not info.get("vireo_source_generation"):
        return None
    return json.dumps(
        {
            "recipe": 1,
            "source": "original",
            "frame_id": info["vireo_subject_frame_id"],
            "source_generation": info["vireo_source_generation"],
            "native_size": info["vireo_native_size"],
            "size": proxy.size,
            "detection_id": detection_id,
            "runtime": runtime,
            "sam_generation": generation,
        },
        sort_keys=True,
    )


def _valid_size(size):
    return isinstance(size, list) and len(size) == 2 and all(type(n) is int and n > 0 for n in size)


def primary_owner(row, candidates):
    if not row:
        return None
    box = {axis: row["prompt_" + axis] for axis in "xywh"}
    try:
        inputs = json.loads(row["comparison_inputs"] or "{}")
    except (TypeError, ValueError):
        inputs = {}
    required = {
        "recipe",
        "source",
        "frame_id",
        "source_generation",
        "native_size",
        "size",
        "detection_id",
        "runtime",
        "sam_generation",
    }
    if (
        not isinstance(inputs, dict)
        or not required.issubset(inputs)
        or not all(_valid_size(inputs[k]) for k in ("size", "native_size"))
    ):
        inputs = {}
    matches = [
        c["detection_id"] for c in candidates if c["box"] == box and c["detector_model"] == row["detector_model"]
    ]
    return {
        "kind": "primary",
        "detection_id": inputs.get("detection_id") or (matches[0] if len(matches) == 1 else None),
        "region": box,
        "detector_model": row["detector_model"],
        "sam_variant": row["variant"],
        "quality_input_recipe": row["quality_input_recipe"],
        "generation": digest(row),
        "raster": inputs or None,
        "eye_evidence": "unbound",
    }


def compatible_primary(row, owner, detection, path, effective_cfg, masks_dir):
    """Read the measured row itself. Photo-level eyes have no generation binding."""
    from image_loader import aligned_source_state, source_generation

    if not row:
        return None, "selected_missing", "No primary mask measurements are available for this selection."
    inputs = owner["raster"]
    if (
        owner["detector_model"] != detection["detector_model"]
        or any(owner["region"][k] != detection["box_" + k] for k in "xywh")
        or (inputs and owner["detection_id"] != detection["id"])
    ):
        return None, "other_animal", "The primary measurements belong to another animal. Analyze the selected animal."
    if not inputs:
        return (
            None,
            "primary_unverified",
            "The primary mask lacks a record of its source, model weights, and measurement scale. Analyze this selection.",
        )
    pipeline = effective_cfg.get("pipeline", {})
    if (
        inputs["recipe"] != 1
        or row["quality_input_recipe"] is not None
        or row["variant"] != (pipeline.get("sam2_variant") or "sam2-small")
        or not inputs["sam_generation"]
        or inputs["sam_generation"] != sam_generation(row["variant"])
        or inputs["runtime"] != detection_prompt_runtime(detection)
    ):
        return (
            None,
            "model_config_changed",
            "The primary analysis used different model or raster settings. Analyze this selection.",
        )
    if pipeline.get("eye_detect_enabled"):
        return (
            None,
            "eye_analysis_missing",
            "The selected eye analysis needs detection-bound measurements. Analyze this selection.",
        )
    if max(inputs["size"]) != min(1024, max(inputs["native_size"])):
        return (
            None,
            "scale_mismatch",
            "The primary measurements use a different pixel scale. Analyze this selection at the comparison scale.",
        )
    try:
        if inputs["source_generation"] != source_generation(aligned_source_state(path)):
            return None, "source_changed", "The original has changed since primary analysis. Analyze this selection."
        mask_path = Path(row["path"]).resolve()
        if not masks_dir or not mask_path.is_relative_to(masks_dir):
            return (
                None,
                "mask_unavailable",
                "The primary mask is outside the catalog masks folder. Analyze this selection.",
            )
        png = mask_path.read_bytes()
        with Image.open(io.BytesIO(png)) as mask:
            if mask.format != "PNG" or list(mask.size) != inputs["size"] or mask.mode != "L" or mask.getbbox() is None:
                return (None, "mask_unavailable",
                        "The primary mask does not match the measured raster. Analyze this selection.")
    except (SyntaxError, Image.DecompressionBombError, Image.UnidentifiedImageError):
        return (None, "mask_unavailable",
                "The primary mask could not be decoded. Analyze this selection.")
    except (OSError, ValueError):
        return (
            None,
            "source_unavailable",
            "The original or primary mask is unavailable. Restore the file or analyze this selection.",
        )
    features = {
        k: row[k] for k in MaskQuality.__dataclass_fields__ if k not in {"comparison_inputs", "quality_input_recipe"}
    }
    if not subject_analysis_readiness({**features, "subject_present": True, "subject_mask_available": True})[
        "eligible"
    ]:
        return (
            None,
            "primary_incomplete",
            "The primary mask has incomplete quality measurements. Analyze this selection.",
        )
    width, height = inputs["size"]
    features.update(
        eye_x=None,
        eye_y=None,
        eye_tenengrad=None,
        eye_conf=None,
        eye_status="disabled",
        analysis={
            "source": "original",
            "width": width,
            "height": height,
            "longest_edge_limit": 1024,
            "measurement_basis": "subject_mask",
        },
    )
    return (
        {"features": features, "mask_png": png, "owner": owner},
        "compatible_primary",
        "Reusing compatible primary measurements.",
    )
