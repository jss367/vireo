"""Build the fixed-treatment fresh-photo blind gallery from a frozen selection."""

import argparse
import hashlib
import json
import os
import random
from pathlib import Path


def make_pair_gallery(run, selection_path, output, seed):
    selection = json.loads(selection_path.read_text())
    photos = selection["photos"]
    rng = random.Random(seed)
    assignments = {}
    # Balance method position inside each ISO stratum as well as overall.
    for group in sorted({p["stratum"] for p in photos}):
        members = sorted(p["id"] for p in photos if p["stratum"] == group)
        if len(members) % 2:
            raise ValueError("Each stratum must have an even photo count")
        sides = [0, 1] * (len(members) // 2)
        rng.shuffle(sides)
        assignments.update(zip(members, sides, strict=True))
    ordered = list(photos)
    rng.shuffle(ordered)
    cases, key, fingerprints = [], {}, []
    output.mkdir(parents=True, exist_ok=False)
    (output / "assets").mkdir()
    for number, photo in enumerate(ordered, 1):
        folder = run / str(photo["id"])
        result = json.loads((folder / "result.json").read_text())
        fp = result["fingerprint"]
        if fp["photo"] != photo or fp["exposure"] != 1.0:
            raise ValueError("Run does not match frozen selection and exposure")
        variants = {v[0]: v for v in fp["variants"]}
        if set(variants) != {"camera70", "neural50"}:
            raise ValueError("Expected only the two fixed treatments")
        if variants["camera70"][2:] != ["camera", 70] or variants["neural50"][2:] != ["neural", 50]:
            raise ValueError("Treatment settings changed")
        fingerprints.append(fp)
        methods = ["camera70", "neural50"]
        if assignments[photo["id"]]:
            methods.reverse()
        ident = f"photo-{number:02d}"
        assets = {}
        for code, method in zip(["A", "B"], methods, strict=True):
            assets[code] = {}
            for mode, suffix in [
                ("full", ".png"),
                ("subject", "-subject.png"),
                ("background", "-background.png"),
                ("overview", "-preview.jpg"),
            ]:
                src = folder / (method + suffix)
                dest = output / "assets" / f"{ident}-{code.lower()}-{mode}{src.suffix}"
                os.link(src, dest)
                assets[code][mode] = "assets/" + dest.name
        cases.append(
            {
                "id": ident,
                "species": ", ".join(photo["species"]),
                "iso": photo["iso"],
                "camera": photo["camera_model"],
                "width": photo["width"],
                "height": photo["height"],
                "center": photo["review_center"],
                "exposure": 1.0,
                "initial": ["A", "B"],
                "assets": assets,
            }
        )
        key[ident] = {
            "photo_id": photo["id"],
            "stratum": photo["stratum"],
            "labels": {code: variants[m][1] for code, m in zip(["A", "B"], methods, strict=True)},
        }
    gallery_id = hashlib.sha256(json.dumps([seed, key, fingerprints], sort_keys=True).encode()).hexdigest()[:16]
    html = (
        Path(__file__)
        .with_name("pair.html")
        .read_text()
        .replace("__CASES__", json.dumps(cases).replace("<", "\\u003c"))
        .replace("__GALLERY_ID__", gallery_id)
        .replace("__COUNT__", str(len(cases)))
    )
    (output / "index.html").write_text(html)
    (output / "reveal.json").write_text(json.dumps(key, indent=2) + "\n")
    (output / "identity.json").write_text(
        json.dumps(
            {
                "gallery_id": gallery_id,
                "seed": seed,
                "selection_sha256": hashlib.sha256(selection_path.read_bytes()).hexdigest(),
            },
            indent=2,
        )
        + "\n"
    )
    return gallery_id


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--run", type=Path, required=True)
    p.add_argument("--selection", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--seed", required=True)
    a = p.parse_args()
    print(make_pair_gallery(a.run, a.selection, a.output, a.seed))
