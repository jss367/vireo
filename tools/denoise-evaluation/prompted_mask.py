"""Generate an experiment-only SAM2 mask from a recorded subject box."""

import argparse
import hashlib
import json
import sys
from pathlib import Path

import numpy as np
import onnxruntime as ort
from PIL import Image

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "vireo"))
from masking import _sam2_box_prompt, _sam2_decoder_inputs
from onnx_runtime import preprocess_image


def sha256(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def generate(image_path, box, model_dir, output):
    if output.exists():
        raise FileExistsError(output)
    with Image.open(image_path) as image:
        width, height = image.size
        cx = (box[0] + box[2] / 2) * width
        cy = (box[1] + box[3] / 2) * height
        side = 2048
        x = max(0, min(width - side, int(cx - side / 2)))
        y = max(0, min(height - side, int(cy - side / 2)))
        roi = image.crop((x, y, x + side, y + side))
        rgb = image.copy()
        rgb.thumbnail((1200, 800))
    options = ort.SessionOptions()
    options.intra_op_num_threads = 4
    options.inter_op_num_threads = 1
    enc = ort.InferenceSession(str(model_dir / "image_encoder.onnx"), options, providers=["CPUExecutionProvider"])
    dec = ort.InferenceSession(str(model_dir / "mask_decoder.onnx"), options, providers=["CPUExecutionProvider"])
    tensor = preprocess_image(roi, (1024, 1024), [0.485, 0.456, 0.406], [0.229, 0.224, 0.225])
    encoded = enc.run(None, {enc.get_inputs()[0].name: tensor})
    coords, labels = _sam2_box_prompt(
        box[0] * width - x, box[1] * height - y, box[2] * width, box[3] * height, side, side
    )
    outs = dec.run(None, _sam2_decoder_inputs(dec, encoded, coords, labels, side, side))
    masks, scores = outs[:2]
    idx = int(np.argmax(scores.flatten()))
    best = masks[0, idx]
    print("SAM2 output", best.dtype, best.shape, "range", best.min(), best.max(), "scores", scores.tolist(), flush=True)
    # Match Vireo's resize path: SAM2 float logits use a zero threshold.
    if not np.isfinite(best).all():
        raise ValueError("Nonfinite SAM2 mask")
    binary = best if best.dtype == np.bool_ else best > 0
    crop = Image.fromarray(binary.astype("uint8") * 255).resize((side, side), Image.Resampling.NEAREST)
    mask = Image.new("L", (width, height))
    mask.paste(crop, (x, y))
    mask.save(output)
    overlay = np.array(rgb)
    sel = np.array(mask.resize(rgb.size, Image.Resampling.NEAREST)) > 127
    overlay[sel] = (overlay[sel] * 0.55 + np.array([0, 220, 110]) * 0.45).astype("uint8")
    Image.fromarray(overlay).save(output.with_name(output.stem + "-overlay.jpg"))
    info = {
        "source_image": str(image_path),
        "source_sha256": sha256(image_path),
        "normalized_box": box,
        "roi": [x, y, side, side],
        "model": "SAM2 large",
        "model_sha256": {name: sha256(model_dir / name) for name in ["image_encoder.onnx", "mask_decoder.onnx"]},
        "scores": scores.tolist(),
        "selected_mask_index": idx,
        "mask_logit_threshold": 0,
        "mask_sha256": sha256(output),
        "note": "Crop-assisted segmentation from a saved detector box; generated in experiment storage only, not saved to Vireo.",
    }
    output.with_suffix(".json").write_text(json.dumps(info, indent=2) + "\n")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--image", type=Path, required=True)
    p.add_argument("--box", type=float, nargs=4, required=True)
    p.add_argument("--model-dir", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    a = p.parse_args()
    generate(a.image, a.box, a.model_dir, a.output)
