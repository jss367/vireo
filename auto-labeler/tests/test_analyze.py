# auto-labeler/tests/test_analyze.py
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'lr-migration'))

from PIL import Image
from xmp_writer import write_xmp_sidecar


def _create_test_folder(tmpdir):
    """Create a folder with test images and some XMP sidecars."""
    img_dir = os.path.join(tmpdir, "photos")
    os.makedirs(img_dir)

    # Photo with existing species keyword
    img = Image.new('RGB', (224, 224), color='red')
    img.save(os.path.join(img_dir, "bird1.jpg"))
    write_xmp_sidecar(
        os.path.join(img_dir, "bird1.xmp"),
        flat_keywords={'Northern cardinal', 'Dyke Marsh'},
        hierarchical_keywords=set(),
    )

    # Photo with no XMP
    img = Image.new('RGB', (224, 224), color='blue')
    img.save(os.path.join(img_dir, "bird2.jpg"))

    return img_dir


def test_analyze_produces_results_json():
    """analyze() creates results.json with photo entries."""
    from analyze import analyze

    with tempfile.TemporaryDirectory() as tmpdir:
        img_dir = _create_test_folder(tmpdir)
        output_dir = os.path.join(tmpdir, "output")

        analyze(
            folder=img_dir,
            output_dir=output_dir,
            labels=['bird', 'cat', 'dog', 'Northern cardinal'],
            model_str='ViT-B-16',
            pretrained_str='/tmp/bioclip_model/open_clip_pytorch_model.bin',
            threshold=0.0,
            thumbnail_size=200,
        )

        results_path = os.path.join(output_dir, "results.json")
        assert os.path.exists(results_path)

        with open(results_path) as f:
            data = json.load(f)

        assert data['folder'] == img_dir
        assert len(data['photos']) == 2
        assert all('category' in p for p in data['photos'])
        assert all('prediction' in p for p in data['photos'])
        assert all('confidence' in p for p in data['photos'])


def test_analyze_generates_thumbnails():
    """analyze() creates thumbnail JPEGs in output_dir/thumbnails/."""
    from analyze import analyze

    with tempfile.TemporaryDirectory() as tmpdir:
        img_dir = _create_test_folder(tmpdir)
        output_dir = os.path.join(tmpdir, "output")

        analyze(
            folder=img_dir,
            output_dir=output_dir,
            labels=['bird', 'cat', 'dog'],
            model_str='ViT-B-16',
            pretrained_str='/tmp/bioclip_model/open_clip_pytorch_model.bin',
            threshold=0.0,
            thumbnail_size=200,
        )

        thumb_dir = os.path.join(output_dir, "thumbnails")
        assert os.path.isdir(thumb_dir)
        thumbs = os.listdir(thumb_dir)
        assert len(thumbs) == 2
        # Check thumbnail size
        thumb = Image.open(os.path.join(thumb_dir, thumbs[0]))
        assert max(thumb.size) <= 200


def test_analyze_filters_matches():
    """analyze() excludes matches (prediction == existing keyword) from results."""
    from analyze import analyze

    with tempfile.TemporaryDirectory() as tmpdir:
        img_dir = _create_test_folder(tmpdir)
        output_dir = os.path.join(tmpdir, "output")

        analyze(
            folder=img_dir,
            output_dir=output_dir,
            labels=['Northern cardinal', 'Blue jay', 'Osprey'],
            model_str='ViT-B-16',
            pretrained_str='/tmp/bioclip_model/open_clip_pytorch_model.bin',
            threshold=0.0,
            thumbnail_size=200,
        )

        with open(os.path.join(output_dir, "results.json")) as f:
            data = json.load(f)

        # No photo should have category 'match' — those are filtered out
        for photo in data['photos']:
            assert photo['category'] != 'match'
