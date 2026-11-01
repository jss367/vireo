"""Verify comparison images and record artifact hashes after rendering."""

import argparse
import hashlib
import json
from pathlib import Path

from PIL import Image


def verify(run):
    hashes = {}
    photos = list(run.glob("*/result.json"))
    for result_path in photos:
        result = json.loads(result_path.read_text())
        folder = result_path.parent
        reference_name = result["fingerprint"]["variants"][0][0]
        with Image.open(folder / (reference_name + ".png")) as reference:
            size = reference.size
        for name, *_ in result["fingerprint"]["variants"]:
            for suffix in (".png", "-subject.png", "-background.png", "-preview.jpg"):
                path = folder / (name + suffix)
                with Image.open(path) as image:
                    if suffix == ".png" and image.size != size:
                        raise ValueError(f"Native dimensions disagree: {path}")
                    if suffix in ("-subject.png", "-background.png") and image.size != (1024, 1024):
                        raise ValueError(f"Wrong native crop size: {path}")
                    if not image.info.get("icc_profile"):
                        raise ValueError(f"Missing color profile: {path}")
                    image.verify()
                h = hashlib.sha256()
                with path.open("rb") as stream:
                    for block in iter(lambda: stream.read(1024 * 1024), b""):
                        h.update(block)
                hashes[str(path.relative_to(run))] = h.hexdigest()
    report = {"completed_photos": len(photos), "verified_images": len(hashes), "all_valid": True, "sha256": hashes}
    (run / "image-integrity.json").write_text(json.dumps(report, indent=2))
    print(f"Verified {len(hashes)} images across {len(photos)} photographs")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True)
    verify(parser.parse_args().run)
