"""Download the separately licensed research model, checking both SHA-256s."""

import argparse
import hashlib
import urllib.request
import zipfile
from pathlib import Path

from neural import MODEL_SHA256

ARCHIVE_SHA256 = "d71b5f1e727c85a359e6f74dca9e2016c9d8fc3e2f7ac3e9b347d80ceca969af"
URL = "https://github.com/darktable-org/darktable-ai/releases/download/release-5.6.0/rawdenoise-nind.dtmodel"


def download(output):
    output.mkdir(parents=True, exist_ok=True)
    archive = output / "rawdenoise-nind.dtmodel"
    if not archive.exists():
        temporary = archive.with_suffix(".download")
        with urllib.request.urlopen(URL, timeout=60) as response, temporary.open("wb") as target:
            count = 0
            for chunk in iter(lambda: response.read(1024 * 1024), b""):
                count += len(chunk)
                if count > 60_000_000:
                    raise ValueError("Model archive exceeds its expected size")
                target.write(chunk)
        archive_hash = hashlib.sha256(temporary.read_bytes()).hexdigest()
        if archive_hash != ARCHIVE_SHA256:
            raise ValueError("Downloaded archive failed SHA-256 verification")
        temporary.replace(archive)
    if hashlib.sha256(archive.read_bytes()).hexdigest() != ARCHIVE_SHA256:
        raise ValueError("Model archive failed SHA-256 verification")
    with zipfile.ZipFile(archive) as package:
        model = package.read("rawdenoise-nind/model_bayer.onnx")
        if hashlib.sha256(model).hexdigest() != MODEL_SHA256:
            raise ValueError("Bayer model failed SHA-256 verification")
        card = package.read("rawdenoise-nind/config.json")
    (output / "model_bayer.onnx").write_bytes(model)
    (output / "model-card.json").write_bytes(card)
    print("Verified RawNIND Bayer model; weights are GPL-3.0, downloaded separately.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    download(parser.parse_args().output)
