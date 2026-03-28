# vireo/tests/test_analyze.py
import json
import os
import sys
import tempfile

import pytest

pytest.importorskip("torch")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from PIL import Image
from xmp import write_sidecar


def _create_mock_taxonomy(tmpdir):
    """Create a minimal taxonomy.json for testing."""
    taxonomy = {
        "last_updated": "2026-03-17",
        "source": "test",
        "taxa_by_common": {
            "bird": {"taxon_id": 1, "scientific_name": "Aves", "common_name": "Bird",
                     "rank": "class", "lineage_names": ["Animalia", "Chordata", "Aves"],
                     "lineage_ranks": ["kingdom", "phylum", "class"]},
            "cat": {"taxon_id": 2, "scientific_name": "Felis catus", "common_name": "Cat",
                    "rank": "species", "lineage_names": ["Animalia", "Chordata", "Mammalia", "Carnivora", "Felidae", "Felis", "Felis catus"],
                    "lineage_ranks": ["kingdom", "phylum", "class", "order", "family", "genus", "species"]},
            "dog": {"taxon_id": 3, "scientific_name": "Canis familiaris", "common_name": "Dog",
                    "rank": "species", "lineage_names": ["Animalia", "Chordata", "Mammalia", "Carnivora", "Canidae", "Canis", "Canis familiaris"],
                    "lineage_ranks": ["kingdom", "phylum", "class", "order", "family", "genus", "species"]},
            "northern cardinal": {"taxon_id": 9083, "scientific_name": "Cardinalis cardinalis",
                                  "common_name": "Northern Cardinal", "rank": "species",
                                  "lineage_names": ["Animalia", "Chordata", "Aves", "Passeriformes", "Cardinalidae", "Cardinalis", "Cardinalis cardinalis"],
                                  "lineage_ranks": ["kingdom", "phylum", "class", "order", "family", "genus", "species"]},
        },
        "taxa_by_scientific": {},
    }
    path = os.path.join(tmpdir, "taxonomy.json")
    with open(path, 'w') as f:
        json.dump(taxonomy, f)
    return path


def _create_test_folder(tmpdir):
    """Create a folder with test images and some XMP sidecars."""
    img_dir = os.path.join(tmpdir, "photos")
    os.makedirs(img_dir)

    # Photo with existing species keyword
    img = Image.new('RGB', (224, 224), color='red')
    img.save(os.path.join(img_dir, "bird1.jpg"))
    write_sidecar(
        os.path.join(img_dir, "bird1.xmp"),
        flat_keywords={'Northern cardinal', 'Dyke Marsh'},
        hierarchical_keywords=set(),
    )

    # Photo with no XMP
    img = Image.new('RGB', (224, 224), color='blue')
    img.save(os.path.join(img_dir, "bird2.jpg"))

    return img_dir


def test_analyze_produces_results_json():
    """analyze() creates results.json with multi-model structure."""
    from analyze import analyze

    with tempfile.TemporaryDirectory() as tmpdir:
        img_dir = _create_test_folder(tmpdir)
        output_dir = os.path.join(tmpdir, "output")
        tax_path = _create_mock_taxonomy(tmpdir)

        analyze(
            folder=img_dir,
            output_dir=output_dir,
            labels=['bird', 'cat', 'dog', 'Northern cardinal'],
            taxonomy_path=tax_path,
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
        assert 'models' in data
        assert 'bioclip-vit-b-16' in data['models']
        # Photos should have predictions dict keyed by model
        for photo in data['photos']:
            assert 'predictions' in photo
            assert 'bioclip-vit-b-16' in photo['predictions']


def test_analyze_generates_thumbnails():
    """analyze() creates thumbnail JPEGs in output_dir/thumbnails/."""
    from analyze import analyze

    with tempfile.TemporaryDirectory() as tmpdir:
        img_dir = _create_test_folder(tmpdir)
        output_dir = os.path.join(tmpdir, "output")
        tax_path = _create_mock_taxonomy(tmpdir)

        analyze(
            folder=img_dir,
            output_dir=output_dir,
            labels=['bird', 'cat', 'dog'],
            taxonomy_path=tax_path,
            model_str='ViT-B-16',
            pretrained_str='/tmp/bioclip_model/open_clip_pytorch_model.bin',
            threshold=0.0,
            thumbnail_size=200,
        )

        thumb_dir = os.path.join(output_dir, "thumbnails")
        assert os.path.isdir(thumb_dir)
        thumbs = os.listdir(thumb_dir)
        assert len(thumbs) >= 1
        thumb = Image.open(os.path.join(thumb_dir, thumbs[0]))
        assert max(thumb.size) <= 200


def test_analyze_merges_models():
    """Running analyze twice with different model names merges results."""
    from analyze import analyze

    with tempfile.TemporaryDirectory() as tmpdir:
        img_dir = _create_test_folder(tmpdir)
        output_dir = os.path.join(tmpdir, "output")
        tax_path = _create_mock_taxonomy(tmpdir)

        # First run
        analyze(
            folder=img_dir,
            output_dir=output_dir,
            labels=['bird', 'cat', 'dog'],
            taxonomy_path=tax_path,
            model_str='ViT-B-16',
            pretrained_str='/tmp/bioclip_model/open_clip_pytorch_model.bin',
            model_name='model-a',
            threshold=0.0,
            thumbnail_size=200,
        )

        # Second run with different model name
        analyze(
            folder=img_dir,
            output_dir=output_dir,
            labels=['bird', 'cat', 'dog'],
            taxonomy_path=tax_path,
            model_str='ViT-B-16',
            pretrained_str='/tmp/bioclip_model/open_clip_pytorch_model.bin',
            model_name='model-b',
            threshold=0.0,
            thumbnail_size=200,
        )

        with open(os.path.join(output_dir, "results.json")) as f:
            data = json.load(f)

        assert 'model-a' in data['models']
        assert 'model-b' in data['models']
        # Each photo should have predictions from both models
        for photo in data['photos']:
            assert 'model-a' in photo['predictions']
            assert 'model-b' in photo['predictions']
