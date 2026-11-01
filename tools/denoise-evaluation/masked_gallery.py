"""Six blind pairs testing subject protection on three known tradeoff photos."""

import argparse
import hashlib
import json
import os
import random
from pathlib import Path

from masked_blend import digest


def build(source, masked, selection, output, seed="subject-protection-2026-10-31-v1"):
    photos = json.loads(selection.read_text())["photos"]
    rng = random.Random(seed)
    pairs = []
    for p in photos:
        left = rng.choice([True, False])
        for other, protected_left in [("camera70", left), ("neural50", not left)]:
            methods = ["protected", other] if protected_left else [other, "protected"]
            pairs.append((p, methods))
    for _ in range(100):
        rng.shuffle(pairs)
        if all(a[0]["id"] != b[0]["id"] for a, b in zip(pairs, pairs[1:], strict=False)):
            break
    labels = {
        "camera70": "Vireo Camera-aware · 70",
        "neural50": "RawNIND · 50% blend",
        "protected": "Vireo subject + RawNIND 50% background",
    }
    output.mkdir(parents=True, exist_ok=False)
    (output / "assets").mkdir()
    cases = []
    key = {}
    fingerprints = []
    for i, (p, methods) in enumerate(pairs, 1):
        pid = p["id"]
        result = json.loads((masked / str(pid) / "result.json").read_text())
        if result["photo"] != p:
            raise ValueError("Mask result does not match selection")
        fingerprints.append(
            {
                "result": result,
                "protected_sha256": digest(masked / str(pid) / "protected.png"),
                "renderer_sha256": digest(Path(__file__).with_name("masked_blend.py")),
            }
        )
        ident = f"comparison-{i:02d}"
        assets = {}
        for code, method in zip(["A", "B"], methods, strict=True):
            assets[code] = {}
            for mode, suffix in [
                ("full", ".png"),
                ("subject", "-subject.png"),
                ("edge", "-edge.png"),
                ("background", "-background.png"),
                ("overview", "-preview.jpg"),
            ]:
                root = masked if method == "protected" or mode == "edge" else source
                src = root / str(pid) / (method + suffix)
                dest = output / "assets" / f"{ident}-{code.lower()}-{mode}{src.suffix}"
                os.link(src, dest)
                assets[code][mode] = "assets/" + dest.name
        cases.append(
            {
                "id": ident,
                "species": ", ".join(p["species"]),
                "iso": p["iso"],
                "camera": p["camera_model"],
                "width": p["width"],
                "height": p["height"],
                "center": p["review_center"],
                "exposure": 1.0,
                "initial": ["A", "B"],
                "assets": assets,
            }
        )
        key[ident] = {
            "photo_id": pid,
            "labels": {code: labels[m] for code, m in zip(["A", "B"], methods, strict=True)},
            "methods": dict(zip(["A", "B"], methods, strict=True)),
        }
    gid = hashlib.sha256(json.dumps([seed, key, fingerprints], sort_keys=True).encode()).hexdigest()[:16]
    html = (
        Path(__file__)
        .with_name("masked.html")
        .read_text()
        .replace("__CASES__", json.dumps(cases).replace("<", "\\u003c"))
        .replace("__GALLERY_ID__", gid)
        .replace("__COUNT__", str(len(cases)))
    )
    (output / "index.html").write_text(html)
    (output / "reveal.json").write_text(json.dumps(key, indent=2) + "\n")
    (output / "identity.json").write_text(
        json.dumps(
            {
                "gallery_id": gid,
                "seed": seed,
                "photos": len(photos),
                "comparisons": len(cases),
                "design": "Protected blend compared with each prior global treatment; each photo has protected on left once and right once. Comparisons are not independent photographs.",
            },
            indent=2,
        )
        + "\n"
    )
    return gid


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    for name in ["source", "masked", "selection", "output"]:
        p.add_argument("--" + name, type=Path, required=True)
    a = p.parse_args()
    print(build(a.source, a.masked, a.selection, a.output))
