# vireo/tests/test_label_photos.py
import os
import sys
import tempfile
from xml.etree import ElementTree as ET

import pytest

pytest.importorskip("torch")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'lr-migration'))

from PIL import Image

NS = {
    "x": "adobe:ns:meta/",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "dc": "http://purl.org/dc/elements/1.1/",
    "lr": "http://ns.adobe.com/lightroom/1.0/",
}


def _create_test_folder(tmpdir):
    """Create a folder with a few test images."""
    img_dir = os.path.join(tmpdir, "photos")
    os.makedirs(img_dir)
    for name in ["bird1.jpg", "bird2.jpg", "landscape.jpg"]:
        img = Image.new('RGB', (224, 224), color='green')
        img.save(os.path.join(img_dir, name))
    return img_dir


def test_dry_run_does_not_write(capsys):
    """Dry run processes images but writes no sidecars."""
    from label_photos import run

    with tempfile.TemporaryDirectory() as tmpdir:
        img_dir = _create_test_folder(tmpdir)
        stats = run(
            folder=img_dir,
            labels=['bird', 'tree', 'sky'],
            model_str='ViT-B-16',
            pretrained_str='/tmp/bioclip_model/open_clip_pytorch_model.bin',
            write=False,
            threshold=0.0,
        )
        assert stats['images_processed'] == 3
        assert stats['sidecars_written'] == 0
        # No XMP files should exist
        xmp_files = [f for f in os.listdir(img_dir) if f.endswith('.xmp')]
        assert len(xmp_files) == 0


def test_write_creates_sidecars():
    """Write mode creates XMP sidecars with auto: prefixed keywords."""
    from label_photos import run

    with tempfile.TemporaryDirectory() as tmpdir:
        img_dir = _create_test_folder(tmpdir)
        stats = run(
            folder=img_dir,
            labels=['bird', 'tree', 'sky'],
            model_str='ViT-B-16',
            pretrained_str='/tmp/bioclip_model/open_clip_pytorch_model.bin',
            write=True,
            threshold=0.0,
        )
        assert stats['images_processed'] == 3
        assert stats['sidecars_written'] == 3

        # Check a sidecar has auto: prefixed tags
        xmp_path = os.path.join(img_dir, "bird1.xmp")
        assert os.path.exists(xmp_path)
        tree = ET.parse(xmp_path)
        root = tree.getroot()
        subjects = root.findall(".//dc:subject/rdf:Bag/rdf:li", NS)
        values = [s.text for s in subjects]
        auto_tags = [v for v in values if v.startswith("auto:")]
        assert len(auto_tags) > 0


def test_threshold_filters_low_confidence():
    """High threshold means fewer or no tags written."""
    from label_photos import run

    with tempfile.TemporaryDirectory() as tmpdir:
        img_dir = _create_test_folder(tmpdir)
        stats = run(
            folder=img_dir,
            labels=['bird', 'tree', 'sky'],
            model_str='ViT-B-16',
            pretrained_str='/tmp/bioclip_model/open_clip_pytorch_model.bin',
            write=True,
            threshold=0.99,
        )
        assert stats['images_processed'] == 3
        # Plain green images with 0.99 threshold — all should be skipped
        assert stats['images_skipped'] == 3
        assert stats['sidecars_written'] == 0
