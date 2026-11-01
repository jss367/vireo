"""Build a separate blind repeat of previously decisive photo comparisons."""

import argparse
import hashlib
import json
import os
import random
import re
import secrets
from pathlib import Path


def make_repeat(source, judgments_path, output, *, seed=None):
    html = (source / "index.html").read_text()
    match = re.search(r"const cases=(.*?), galleryId='([^']+)';", html)
    if not match:
        raise ValueError("Source gallery metadata missing")
    cases = {c["id"]: c for c in json.loads(match[1])}
    judgments = json.loads(judgments_path.read_text())
    if judgments["gallery_id"] != match[2]:
        raise ValueError("Judgments do not belong to this gallery")
    key = json.loads((source / "reveal.json").read_text())
    selected = [j for j in judgments["judgments"] if j["vote"] in ("left", "right")]
    if len(selected) != 6 or len({j["case_id"] for j in selected}) != 6:
        raise ValueError("Expected exactly six decisive judgments on distinct photographs")
    methods = ["Vireo Camera-aware · 70", "RawNIND · 50% blend"]
    for j in selected:
        if j["gallery_id"] != match[2] or j["view"] != "subject" or j["fit"]:
            raise ValueError("Expected matching native subject judgments")
        labels = key[j["case_id"]]["labels"]
        if {labels[j["left"]], labels[j["right"]]} != set(methods):
            raise ValueError("Unexpected treatment pair")
    seed = seed or secrets.token_hex(16)
    rng = random.Random(seed)
    rng.shuffle(selected)
    sides = [0, 0, 0, 1, 1, 1]
    rng.shuffle(sides)
    output.mkdir(parents=True, exist_ok=False)
    (output / "assets").mkdir()
    new_cases, reveal, provenance = [], {}, []
    for i, (j, side) in enumerate(zip(selected, sides, strict=True), 1):
        old = cases[j["case_id"]]
        ident = f"repeat-{i:02d}"
        labels = key[j["case_id"]]["labels"]
        ordered = [methods[side], methods[1 - side]]
        assets = {}
        for code, method in zip(["A", "B"], ordered, strict=True):
            old_code = next(c for c, label in labels.items() if label == method)
            assets[code] = {}
            for mode, relative in old["assets"][old_code].items():
                src = source / relative
                name = f"{ident}-{code.lower()}-{mode}{src.suffix}"
                os.link(src, output / "assets" / name)
                assets[code][mode] = f"assets/{name}"
        new_cases.append({**old, "id": ident, "assets": assets, "initial": ["A", "B"]})
        reveal[ident] = {
            "photo_id": key[j["case_id"]]["photo_id"],
            "original_case_id": j["case_id"],
            "labels": dict(zip(["A", "B"], ordered, strict=True)),
        }
        provenance.append({"repeat_case_id": ident, "original_judgment": j})
    gallery_id = hashlib.sha256(json.dumps([match[2], seed, reveal], sort_keys=True).encode()).hexdigest()[:16]
    template = Path(__file__).with_name("repeat.html").read_text()
    (output / "index.html").write_text(
        template.replace("__CASES__", json.dumps(new_cases).replace("<", "\\u003c"))
        .replace("__GALLERY_ID__", gallery_id)
        .replace("__COUNT__", "6")
    )
    (output / "reveal.json").write_text(json.dumps(reveal, indent=2) + "\n")
    (output / "design.json").write_text(
        json.dumps(
            {
                "gallery_id": gallery_id,
                "source_gallery_id": match[2],
                "seed": seed,
                "design": "Same six decisive photographs and pixels; shuffled photo order; balanced random assignment of method to side (three each). One judgment and reason per photograph. No method reveal in the review UI.",
                "analysis": "Join each repeat judgment through reveal.json and compare preferred methods with the original judgment. Report agreement, reversals, new ties/uncertain, reasons, and side preferences. This selected subset measures repeatability, not population efficacy.",
                "provenance": provenance,
            },
            indent=2,
        )
        + "\n"
    )
    return gallery_id


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source", type=Path, required=True)
    p.add_argument("--judgments", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--seed")
    args = p.parse_args()
    print("Built repeat gallery:", make_repeat(args.source, args.judgments, args.output, seed=args.seed))
