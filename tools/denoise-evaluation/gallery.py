"""Create a local blinded review gallery from completed run artifacts."""

import argparse
import hashlib
import json
import os
import random
import re
from pathlib import Path

from PIL import Image


def make_gallery(run, output, centers=None, *, html_only=False):
    centers = centers or {}
    output.mkdir(parents=True, exist_ok=True)
    (output / "assets").mkdir(exist_ok=True)
    cases, key, fingerprints = [], {}, []
    results = sorted(run.glob("*/result.json"), key=lambda p: int(p.parent.name))
    for number, path in enumerate(results, 1):
        result = json.loads(path.read_text())
        fingerprints.append(result["fingerprint"])
        photo = result["fingerprint"]["photo"]
        ident = f"case-{number:02d}"
        variants = result["fingerprint"]["variants"]
        shuffled = [v for v in variants if v[0] != "off"]
        center = centers.get(str(photo["id"]), [0.5, 0.5])
        seed = hashlib.sha256(json.dumps([result["fingerprint"], center], sort_keys=True).encode()).hexdigest()
        random.Random(seed).shuffle(shuffled)
        codes = {"Reference": variants[0]}
        codes.update({chr(65 + i): v for i, v in enumerate(shuffled)})
        assets = {}
        with Image.open(path.parent / "off.png") as image:
            width, height = image.size
        left = max(0, min(width - 1024, int(center[0] * width - 512)))
        top = max(0, min(height - 1024, int(center[1] * height - 512)))
        for code, variant in codes.items():
            name = f"{ident}-{code.lower()}"
            assets[code] = {}
            for mode, suffix in [("full", ".png"), ("overview", "-preview.jpg"), ("background", "-background.png")]:
                src = path.parent / (variant[0] + suffix)
                dest = output / "assets" / (name + "-" + mode + src.suffix)
                if not html_only:
                    if dest.exists():
                        dest.unlink()
                    os.link(src, dest)
                elif not dest.is_file():
                    raise ValueError("HTML-only refresh requires an existing complete gallery")
                assets[code][mode] = "assets/" + dest.name
            dest = output / "assets" / (name + "-subject.png")
            if not html_only:
                with Image.open(path.parent / (variant[0] + ".png")) as image:
                    image.crop((left, top, left + 1024, top + 1024)).save(
                        dest, icc_profile=image.info.get("icc_profile")
                    )
            elif not dest.is_file():
                raise ValueError("HTML-only refresh requires existing subject crops")
            assets[code]["subject"] = "assets/" + dest.name
        labels = {code: v[1] for code, v in codes.items()}
        # Start with camera-aware 70 against neural 50, randomized left/right.
        initial = [code for code, v in codes.items() if v[0] in ("camera70", "neural50")]
        random.Random(seed + "-side").shuffle(initial)
        key[ident] = {"photo_id": photo["id"], "labels": labels}
        cases.append(
            {
                "id": ident,
                "species": ", ".join(photo["species"]),
                "iso": photo["iso"],
                "camera": photo["camera_model"],
                "width": width,
                "height": height,
                "assets": assets,
                "initial": initial,
                "center": center,
                "exposure": result["fingerprint"]["exposure"],
            }
        )
    data = json.dumps(cases).replace("<", "\\u003c")
    template = Path(__file__).with_name("gallery.html").read_text()
    gallery_id = hashlib.sha256(json.dumps([key, fingerprints, centers], sort_keys=True).encode()).hexdigest()[:16]
    if html_only:
        previous = re.search(r"galleryId='([0-9a-f]+)'", (output / "index.html").read_text())
        if not previous or previous.group(1) != gallery_id:
            raise ValueError("Images, settings, or viewing centers changed; regenerate the complete gallery")
    (output / "reveal.json").write_text(json.dumps(key, indent=2))
    (output / "index.html").write_text(
        template.replace("__CASES__", data).replace("__GALLERY_ID__", gallery_id).replace("__COUNT__", str(len(cases)))
    )
    return len(cases)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--centers", type=Path)
    parser.add_argument(
        "--html-only", action="store_true", help="Refresh controls without regenerating existing image assets"
    )
    args = parser.parse_args()
    centers = json.loads(args.centers.read_text()) if args.centers else None
    print(f"Built {make_gallery(args.run, args.output, centers, html_only=args.html_only)} review cases")
