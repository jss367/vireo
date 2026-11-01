"""Protect blind treatment identity and keep fresh reviews isolated."""

import json
import re
from collections import Counter

import pytest
from pair_gallery import make_pair_gallery
from PIL import Image


def test_pair_assets_match_reveal_and_existing_review_is_preserved(tmp_path):
    run = tmp_path / "run"
    run.mkdir()
    photos = []
    for i in range(12):
        photo = {
            "id": i,
            "species": ["Bird"],
            "iso": 3200 if i < 10 else 100,
            "camera_model": "NIKON D850",
            "width": 32,
            "height": 32,
            "stratum": "high" if i < 10 else "low",
            "review_center": [0.5, 0.5],
        }
        photos.append(photo)
        folder = run / str(i)
        folder.mkdir()
        variants = [
            ["camera70", "Vireo Camera-aware · 70", "camera", 70],
            ["neural50", "RawNIND · 50% blend", "neural", 50],
        ]
        (folder / "result.json").write_text(
            json.dumps({"fingerprint": {"photo": photo, "exposure": 1.0, "variants": variants}})
        )
        for n, (name, *_) in enumerate(variants):
            for suffix in [".png", "-subject.png", "-background.png", "-preview.jpg"]:
                Image.new("RGB", (32, 32), (20 + 100 * n, i, 0)).save(folder / (name + suffix))
    selection = tmp_path / "selection.json"
    selection.write_text(json.dumps({"photos": photos}))
    out = tmp_path / "gallery"
    ident = make_pair_gallery(run, selection, out, "test-seed")
    text = (out / "index.html").read_text()
    cases = json.loads(re.search(r"const cases=(.*?), galleryId=", text)[1])
    key = json.loads((out / "reveal.json").read_text())
    assert len(cases) == 12 and ident in text
    assert "RawNIND" not in text and "Camera-aware" not in text
    for stratum, n in [("high", 5), ("low", 1)]:
        counts = Counter(k["labels"]["A"] for k in key.values() if k["stratum"] == stratum)
        assert sorted(counts.values()) == [n, n]
    for case in cases:
        entry = key[case["id"]]
        for code, method in entry["labels"].items():
            filename = "camera70" if method.startswith("Vireo") else "neural50"
            actual = out / case["assets"][code]["subject"]
            assert actual.read_bytes() == (run / str(entry["photo_id"]) / (filename + "-subject.png")).read_bytes()
    before = (out / "index.html").read_bytes()
    with pytest.raises(FileExistsError):
        make_pair_gallery(run, selection, out, "another-seed")
    assert (out / "index.html").read_bytes() == before
    other = tmp_path / "other"
    assert make_pair_gallery(run, selection, other, "another-seed") != ident
    photos[0]["review_center"] = [0.2, 0.3]
    selection.write_text(json.dumps({"photos": photos}))
    with pytest.raises(ValueError, match="frozen selection"):
        make_pair_gallery(run, selection, tmp_path / "mismatch", "test-seed")
