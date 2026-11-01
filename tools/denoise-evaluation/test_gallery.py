"""Prevent new reveal labels from being attached to stale anonymous images."""

import json

import pytest
from gallery import make_gallery
from PIL import Image


def test_html_refresh_preserves_blinding_and_rejects_changed_inputs(tmp_path):
    run = tmp_path / "run"
    folder = run / "1"
    folder.mkdir(parents=True)
    variants = [
        ("off", "Off", "off", 0),
        ("standard35", "Standard 35", "standard", 35),
        ("standard70", "Standard 70", "standard", 70),
        ("camera35", "Camera 35", "camera", 35),
        ("camera70", "Camera 70", "camera", 70),
        ("neural50", "Neural 50", "neural", 50),
        ("neural100", "Neural 100", "neural", 100),
    ]
    result = {
        "fingerprint": {
            "photo": {"id": 1, "species": ["Test bird"], "iso": 100, "camera_model": "Test camera"},
            "variants": variants,
            "exposure": 1,
            "source_sha256": "first-source",
        }
    }
    (folder / "result.json").write_text(json.dumps(result))
    for i, (name, *_) in enumerate(variants):
        for suffix in (".png", "-preview.jpg", "-background.png"):
            Image.new("RGB", (32, 32), (i * 30, 20, 10)).save(folder / (name + suffix))
    output = tmp_path / "gallery"
    assert make_gallery(run, output) == 1
    key = (output / "reveal.json").read_bytes()
    assert make_gallery(run, output, html_only=True) == 1
    assert (output / "reveal.json").read_bytes() == key
    result["fingerprint"]["source_sha256"] = "changed-source"
    (folder / "result.json").write_text(json.dumps(result))
    with pytest.raises(ValueError, match="regenerate"):
        make_gallery(run, output, html_only=True)
    assert (output / "reveal.json").read_bytes() == key
