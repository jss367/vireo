# auto-labeler/tests/test_compare.py
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'lr-migration'))


def _write_test_xmp(path, keywords):
    """Write a minimal XMP file with dc:subject keywords."""
    from xmp_writer import write_xmp_sidecar
    write_xmp_sidecar(path, flat_keywords=set(keywords), hierarchical_keywords=set())


def test_read_xmp_keywords():
    """read_xmp_keywords returns dc:subject values from an XMP file."""
    from compare import read_xmp_keywords

    with tempfile.NamedTemporaryFile(suffix='.xmp', delete=False) as f:
        _write_test_xmp(f.name, ['Northern cardinal', '0Locations', 'Dyke Marsh'])
        result = read_xmp_keywords(f.name)
        assert result == {'Northern cardinal', '0Locations', 'Dyke Marsh'}
        os.unlink(f.name)


def test_read_xmp_keywords_missing_file():
    """read_xmp_keywords returns empty set for missing file."""
    from compare import read_xmp_keywords
    result = read_xmp_keywords('/tmp/nonexistent.xmp')
    assert result == set()


def test_categorize_match():
    """Exact match (case-insensitive) returns 'match'."""
    from compare import categorize
    labels = {'Northern cardinal', 'Blue jay', 'Osprey'}
    result = categorize('Northern cardinal', {'northern cardinal', 'Dyke Marsh'}, labels)
    assert result == 'match'


def test_categorize_new():
    """No existing species keywords returns 'new'."""
    from compare import categorize
    labels = {'Northern cardinal', 'Blue jay', 'Osprey'}
    result = categorize('Northern cardinal', {'Dyke Marsh', '0Locations'}, labels)
    assert result == 'new'


def test_categorize_refinement():
    """Existing keyword is substring of prediction returns 'refinement'."""
    from compare import categorize
    labels = {'Song sparrow', 'sparrow', 'Blue jay'}
    result = categorize('Song sparrow', {'sparrow', 'Dyke Marsh'}, labels)
    assert result == 'refinement'


def test_categorize_disagreement():
    """Different species returns 'disagreement'."""
    from compare import categorize
    labels = {'Northern cardinal', 'Blue jay', 'Osprey'}
    result = categorize('Blue jay', {'Northern cardinal', 'Dyke Marsh'}, labels)
    assert result == 'disagreement'


def test_categorize_no_labels_vocab():
    """When existing keywords has no species matches, treat as 'new'."""
    from compare import categorize
    labels = {'Northern cardinal', 'Blue jay'}
    result = categorize('Northern cardinal', {'8Landscape', 'Dyke Marsh'}, labels)
    assert result == 'new'
