"""RawNIND Bayer experiment using ONNX Runtime's CPU executor.

Tiling, gain matching and highlight protection follow LightCraft's
crates/denoise-core/src/{bayer,tiles,run}.rs. See LICENSE-LightCraft.txt.
This tests its model/preprocessing approach, not its Rust/GPU runtime.
"""

import math
import time

import numpy as np
import onnxruntime as ort
from scipy.ndimage import maximum_filter

MODEL_SHA256 = "da27509dab6a2915da67e988acd86cf71f9d5bbc8d1aa0ed32933578a887b901"
TILE = 512
OVERLAP = 64


def starts(n, tile=TILE, overlap=OVERLAP):
    if n <= tile:
        return [0]
    count = math.ceil((n - tile) / (tile - overlap)) + 1
    return [(i * (n - tile) + (count - 1) // 2) // (count - 1) for i in range(count)]


def weights(before, after):
    p = np.arange(TILE, dtype=np.float32)

    def ramp(d):
        t = np.minimum((d + 0.5) / OVERLAP, 1)
        return np.maximum(t * t * (3 - 2 * t), 0.001)

    return np.minimum(ramp(p) if before else 1, ramp(TILE - 1 - p) if after else 1) * np.ones(TILE)


def pack(mosaic, cell):
    """Align a Bayer cell to RGGB without changing original pixel positions."""
    dy, dx = np.argwhere(cell == "R")[0]
    h, w = mosaic.shape
    # Mirroring by one sensor sample preserves Bayer color phase.
    padded = np.pad(mosaic, ((dy, (h + dy) % 2), (dx, (w + dx) % 2)), mode="reflect")
    packed = np.stack([padded[0::2, 0::2], padded[0::2, 1::2], padded[1::2, 0::2], padded[1::2, 1::2]])
    if cell[(dy + 1) % 2, (dx + 1) % 2] != "B":
        raise ValueError("Invalid Bayer layout")
    return packed, (int(dy), int(dx))


def finish_tile(inp, out, before_x, after_x, before_y, after_y):
    m = OVERLAP // 2
    x0, x1 = (m if before_x else 0), TILE - (m if after_x else 0)
    y0, y1 = (m if before_y else 0), TILE - (m if after_y else 0)
    means = inp[:, y0:y1, x0:x1].mean(axis=(1, 2), dtype=np.float64)
    means = np.array([means[0], (means[1] + means[2]) / 2, means[3]])
    out_mean = out[:, 2 * y0 : 2 * y1, 2 * x0 : 2 * x1].mean(axis=(1, 2), dtype=np.float64)
    gain = np.full(3, 1e6)
    valid = (means >= 0.003) & (out_mean > 0)
    gain[valid] = out_mean[valid] / means[valid]
    gain = np.clip(gain, 0.95e6, 1.05e6)
    out /= gain[:, None, None]
    # Follow LightCraft: protect clipped cells, with a one-cell neighborhood.
    high = inp.max(axis=0)
    if high.max() >= 1:
        high = maximum_filter(high, size=3, mode="nearest")
        t = np.clip((high - 0.97) / 0.03, 0, 1)
        t = t * t * (3 - 2 * t)
        alpha = t.repeat(2, 0).repeat(2, 1)
        plain = np.stack([inp[0], (inp[1] + inp[2]) / 2, inp[3]])
        plain = plain.repeat(2, 1).repeat(2, 2)
        out += (plain - out) * alpha[None]
    return gain


class Denoiser:
    def __init__(self, model, threads=4):
        options = ort.SessionOptions()
        options.intra_op_num_threads = threads
        options.inter_op_num_threads = 1
        started = time.perf_counter()
        # The model's large activations can overflow FP16 on CoreML GPU/ANE.
        self.session = ort.InferenceSession(str(model), options, providers=["CPUExecutionProvider"])
        self.load_seconds = time.perf_counter() - started
        self.input = self.session.get_inputs()[0].name

    def run(self, mosaic, cell):
        packed, (dy, dx) = pack(mosaic, cell)
        _, h, w = packed.shape
        ys, xs = starts(h), starts(w)
        pad = np.pad(packed, ((0, 0), (0, max(0, TILE - h)), (0, max(0, TILE - w))), mode="reflect")
        result = np.zeros((h * 2, w * 2, 3), np.float32)
        sums = np.zeros((h, w), np.float32)
        gains = []
        began = time.perf_counter()
        for j, y in enumerate(ys):
            for i, x in enumerate(xs):
                tile = np.ascontiguousarray(pad[:, y : y + TILE, x : x + TILE])
                out = self.session.run(None, {self.input: tile[None]})[0][0]
                if out.shape != (3, 1024, 1024) or not np.isfinite(out).all():
                    raise ValueError("Model returned an invalid/nonfinite tile")
                edges = i > 0, i + 1 < len(xs), j > 0, j + 1 < len(ys)
                gains.append(finish_tile(tile, out, *edges).tolist())
                weight = (weights(edges[2], edges[3])[:, None] * weights(edges[0], edges[1])[None]).astype(np.float32)
                hh, ww = min(TILE, h - y), min(TILE, w - x)
                weight = weight[:hh, :ww]
                expanded = weight.repeat(2, 0).repeat(2, 1)
                result[y * 2 : (y + hh) * 2, x * 2 : (x + ww) * 2] += (
                    out[:, : hh * 2, : ww * 2].transpose(1, 2, 0) * expanded[..., None]
                )
                sums[y : y + hh, x : x + ww] += weight
            print(f"  neural tiles {(j + 1) * len(xs)}/{len(xs) * len(ys)}", flush=True)
        if np.any(sums <= 0):
            raise ValueError("Tile blending left uncovered pixels")
        for y in range(0, h, 128):
            result[2 * y : 2 * (y + 128)] /= sums[y : y + 128].repeat(2, 0).repeat(2, 1)[..., None]
        result = result[dy : dy + mosaic.shape[0], dx : dx + mosaic.shape[1]]
        if not np.isfinite(result).all():
            raise ValueError("Nonfinite denoised image")
        return result, {
            "inference_and_blending_seconds": time.perf_counter() - began,
            "tiles": len(xs) * len(ys),
            "tile_gains": gains,
            "provider": "CPUExecutionProvider",
            "model_load_seconds": self.load_seconds,
        }
