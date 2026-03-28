# vireo/tests/test_classifier.py
import os
import sys
import tempfile

import pytest

pytest.importorskip("torch")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from PIL import Image


def _make_test_image():
    """Create a temporary test image."""
    f = tempfile.NamedTemporaryFile(suffix='.jpg', delete=False)
    path = f.name
    f.close()
    img = Image.new('RGB', (224, 224), color='red')
    img.save(path)
    return path


def test_classify_returns_predictions():
    """classify_image returns a list of dicts with species, score, and auto_tag."""
    from classifier import Classifier

    clf = Classifier(
        labels=['bird', 'cat', 'dog'],
        model_str='ViT-B-16',
        pretrained_str='/tmp/bioclip_model/open_clip_pytorch_model.bin',
    )

    path = _make_test_image()
    try:
        results = clf.classify(path)
        assert isinstance(results, list)
        assert len(results) > 0
        top = results[0]
        assert 'species' in top
        assert 'score' in top
        assert 'auto_tag' in top
        assert top['auto_tag'].startswith('auto:')
        assert 0 <= top['score'] <= 1
    finally:
        os.unlink(path)


def test_classify_with_threshold():
    """classify_image filters results below threshold."""
    from classifier import Classifier

    clf = Classifier(
        labels=['bird', 'cat', 'dog'],
        model_str='ViT-B-16',
        pretrained_str='/tmp/bioclip_model/open_clip_pytorch_model.bin',
    )

    path = _make_test_image()
    try:
        results = clf.classify(path, threshold=0.99)
        for r in results:
            assert r['score'] >= 0.99
    finally:
        os.unlink(path)


def test_classify_includes_confidence_tag():
    """Each result includes a confidence tag like 'auto:confidence:0.95'."""
    from classifier import Classifier

    clf = Classifier(
        labels=['bird', 'cat', 'dog'],
        model_str='ViT-B-16',
        pretrained_str='/tmp/bioclip_model/open_clip_pytorch_model.bin',
    )

    path = _make_test_image()
    try:
        results = clf.classify(path, threshold=0.0)
        top = results[0]
        assert 'confidence_tag' in top
        assert top['confidence_tag'].startswith('auto:confidence:')
    finally:
        os.unlink(path)
