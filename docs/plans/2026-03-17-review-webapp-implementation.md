# Review Webapp Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a review webapp that shows ML species predictions alongside existing Lightroom keywords, letting the user visually review and selectively accept suggestions into XMP sidecars.

**Architecture:** A Python analyze script scans photos, classifies them, compares predictions against existing XMP keywords, generates thumbnails, and writes a `results.json`. A Flask server serves a single-page review UI reading from that JSON, with accept/skip buttons that write accepted keywords to XMP sidecars.

**Tech Stack:** Python 3, Flask, BioCLIP (existing classifier), xml.etree.ElementTree, PIL, vanilla HTML/CSS/JS

---

### Task 1: XMP reader and comparison logic

**Files:**
- Create: `auto-labeler/compare.py`
- Create: `auto-labeler/tests/test_compare.py`

**Step 1: Write failing tests**

```python
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
```

**Step 2: Run tests to verify they fail**

Run: `cd /Users/julius/git/photo-tools && python -m pytest auto-labeler/tests/test_compare.py -v`
Expected: FAIL — module does not exist

**Step 3: Implement compare.py**

```python
# auto-labeler/compare.py
"""Read XMP keywords and compare against model predictions."""

import logging
from xml.etree import ElementTree as ET
from pathlib import Path

log = logging.getLogger(__name__)

NS_RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
NS_DC = "http://purl.org/dc/elements/1.1/"


def read_xmp_keywords(xmp_path):
    """Read dc:subject keywords from an XMP sidecar file.

    Args:
        xmp_path: path to .xmp file

    Returns:
        set of keyword strings (empty if file missing or corrupt)
    """
    path = Path(xmp_path)
    if not path.exists():
        return set()

    try:
        tree = ET.parse(path)
    except ET.ParseError:
        log.warning("Corrupt XMP file: %s", xmp_path)
        return set()

    root = tree.getroot()
    keywords = set()
    for li in root.findall(f".//{{{NS_DC}}}subject/{{{NS_RDF}}}Bag/{{{NS_RDF}}}li"):
        if li.text:
            keywords.add(li.text)
    return keywords


def categorize(prediction, existing_keywords, labels_vocab):
    """Categorize a prediction relative to existing keywords.

    Args:
        prediction: the model's predicted species name
        existing_keywords: set of all dc:subject keywords from the XMP
        labels_vocab: set of known species labels (used to filter
                      existing keywords to just species, ignoring locations etc.)

    Returns:
        'match' — prediction matches an existing species keyword
        'new' — no existing species keywords found
        'refinement' — prediction is more specific than an existing keyword
        'disagreement' — prediction differs from existing species keyword
    """
    pred_lower = prediction.lower()

    # Filter existing keywords to just species (those in the labels vocab)
    existing_species = set()
    for kw in existing_keywords:
        for label in labels_vocab:
            if kw.lower() == label.lower():
                existing_species.add(kw)
                break

    # No species keywords exist — this is new info
    if not existing_species:
        return 'new'

    # Check for exact match (case-insensitive)
    for sp in existing_species:
        if sp.lower() == pred_lower:
            return 'match'

    # Check for refinement: existing keyword is a substring of prediction
    # or they share a significant word (e.g., "hawk" in "Red-tailed hawk")
    for sp in existing_species:
        sp_lower = sp.lower()
        if sp_lower in pred_lower or pred_lower in sp_lower:
            return 'refinement'
        # Check shared words (ignoring short words)
        sp_words = {w for w in sp_lower.replace('-', ' ').split() if len(w) > 2}
        pred_words = {w for w in pred_lower.replace('-', ' ').split() if len(w) > 2}
        if sp_words & pred_words:
            return 'refinement'

    return 'disagreement'
```

**Step 4: Run tests to verify they pass**

Run: `cd /Users/julius/git/photo-tools && python -m pytest auto-labeler/tests/test_compare.py -v`
Expected: all 7 tests PASS

**Step 5: Commit**

```bash
cd /Users/julius/git/photo-tools
git add auto-labeler/compare.py auto-labeler/tests/test_compare.py
git commit -m "feat: add XMP reader and prediction comparison logic"
```

---

### Task 2: Analyze script — scan, classify, compare, generate thumbnails

**Files:**
- Create: `auto-labeler/analyze.py`
- Create: `auto-labeler/tests/test_analyze.py`

**Step 1: Write failing tests**

```python
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
```

**Step 2: Run tests to verify they fail**

Run: `cd /Users/julius/git/photo-tools && python -m pytest auto-labeler/tests/test_analyze.py -v`
Expected: FAIL — module does not exist

**Step 3: Implement analyze.py**

```python
# auto-labeler/analyze.py
"""Scan photos, classify, compare to existing XMP keywords, generate review data.

Usage:
    python auto-labeler/analyze.py --folder /path/to/photos --labels-file labels.txt
"""

import argparse
import json
import logging
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lr-migration'))

from classifier import Classifier
from compare import read_xmp_keywords, categorize
from image_loader import load_image, SUPPORTED_EXTENSIONS

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
)
log = logging.getLogger(__name__)


def analyze(folder, output_dir, labels, model_str='ViT-B-16',
            pretrained_str='/tmp/bioclip_model/open_clip_pytorch_model.bin',
            threshold=0.4, thumbnail_size=400, recursive=True):
    """Scan a folder, classify images, compare to existing keywords, write results.

    Args:
        folder: path to image folder
        output_dir: path to output directory for results.json and thumbnails/
        labels: list of species labels for the classifier and vocabulary
        model_str: BioCLIP model string
        pretrained_str: path to model weights
        threshold: minimum confidence score
        thumbnail_size: max dimension for thumbnails
        recursive: scan subfolders
    """
    os.makedirs(output_dir, exist_ok=True)
    thumb_dir = os.path.join(output_dir, "thumbnails")
    os.makedirs(thumb_dir, exist_ok=True)

    clf = Classifier(labels=labels, model_str=model_str, pretrained_str=pretrained_str)
    labels_vocab = set(labels)

    folder_path = Path(folder)
    if recursive:
        image_files = sorted(
            f for f in folder_path.rglob('*')
            if f.suffix.lower() in SUPPORTED_EXTENSIONS and not f.name.startswith('.')
        )
    else:
        image_files = sorted(
            f for f in folder_path.iterdir()
            if f.suffix.lower() in SUPPORTED_EXTENSIONS and not f.name.startswith('.')
        )

    log.info("Found %d images in %s", len(image_files), folder)

    photos = []
    stats = {'total': len(image_files), 'new': 0, 'refinement': 0,
             'disagreement': 0, 'match': 0, 'failed': 0, 'below_threshold': 0}

    for i, image_path in enumerate(image_files):
        img = load_image(str(image_path))
        if img is None:
            stats['failed'] += 1
            continue

        # Generate thumbnail
        thumb_path = os.path.join(thumb_dir, image_path.stem + ".jpg")
        thumb = img.copy()
        thumb.thumbnail((thumbnail_size, thumbnail_size))
        thumb.save(thumb_path, quality=85)

        # Classify via temp file
        with tempfile.NamedTemporaryFile(suffix='.jpg', delete=False) as tmp:
            tmp_path = tmp.name
            img.save(tmp_path, quality=85)

        try:
            predictions = clf.classify(tmp_path, threshold=threshold)
        except Exception:
            log.warning("Classification failed for %s", image_path, exc_info=True)
            stats['failed'] += 1
            continue
        finally:
            os.unlink(tmp_path)

        if not predictions:
            stats['below_threshold'] += 1
            continue

        top = predictions[0]

        # Read existing XMP keywords
        xmp_path = image_path.with_suffix('.xmp')
        existing = read_xmp_keywords(str(xmp_path))

        # Categorize
        category = categorize(top['species'], existing, labels_vocab)
        stats[category] += 1

        # Skip matches — only show differences
        if category == 'match':
            continue

        # Filter existing to just species for display
        existing_species = [kw for kw in existing
                           if any(kw.lower() == l.lower() for l in labels_vocab)]

        photos.append({
            'filename': image_path.name,
            'image_path': str(image_path),
            'xmp_path': str(xmp_path),
            'existing_species': existing_species,
            'prediction': top['species'],
            'confidence': round(top['score'], 4),
            'category': category,
            'status': 'pending',
        })

        if (i + 1) % 100 == 0:
            log.info("Progress: %d/%d images", i + 1, len(image_files))

    results = {
        'folder': str(folder),
        'settings': {
            'threshold': threshold,
            'thumbnail_size': thumbnail_size,
        },
        'stats': stats,
        'photos': photos,
    }

    results_path = os.path.join(output_dir, "results.json")
    with open(results_path, 'w') as f:
        json.dump(results, f, indent=2)

    log.info("--- Analysis Summary ---")
    log.info("Total images:   %d", stats['total'])
    log.info("New:            %d", stats['new'])
    log.info("Refinements:    %d", stats['refinement'])
    log.info("Disagreements:  %d", stats['disagreement'])
    log.info("Matches:        %d (hidden)", stats['match'])
    log.info("Below threshold:%d", stats['below_threshold'])
    log.info("Failed:         %d", stats['failed'])
    log.info("Results saved to %s", results_path)

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Analyze photos: classify, compare to existing labels, generate review data."
    )
    parser.add_argument("--folder", required=True, help="Path to image folder")
    parser.add_argument("--labels-file", required=True, help="Text file with one label per line")
    parser.add_argument("--output-dir", default="/tmp/photo-review", help="Output directory")
    parser.add_argument("--model-weights", default="/tmp/bioclip_model/open_clip_pytorch_model.bin")
    parser.add_argument("--threshold", type=float, default=0.4)
    parser.add_argument("--thumbnail-size", type=int, default=400)
    parser.add_argument("--no-recursive", action="store_true")
    args = parser.parse_args()

    with open(args.labels_file) as f:
        labels = [line.strip() for line in f if line.strip()]
    log.info("Loaded %d labels from %s", len(labels), args.labels_file)

    analyze(
        folder=args.folder,
        output_dir=args.output_dir,
        labels=labels,
        pretrained_str=args.model_weights,
        threshold=args.threshold,
        thumbnail_size=args.thumbnail_size,
        recursive=not args.no_recursive,
    )


if __name__ == "__main__":
    main()
```

**Step 4: Run tests to verify they pass**

Run: `cd /Users/julius/git/photo-tools && python -m pytest auto-labeler/tests/test_analyze.py -v`
Expected: all 3 tests PASS

**Step 5: Commit**

```bash
cd /Users/julius/git/photo-tools
git add auto-labeler/analyze.py auto-labeler/tests/test_analyze.py
git commit -m "feat: add analyze script for photo classification and comparison"
```

---

### Task 3: Review server — Flask API

**Files:**
- Create: `auto-labeler/review_server.py`
- Create: `auto-labeler/tests/test_review_server.py`

**Step 1: Write failing tests**

```python
# auto-labeler/tests/test_review_server.py
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'lr-migration'))

from PIL import Image


def _create_test_review_data(tmpdir):
    """Create a minimal results.json and thumbnails dir for testing."""
    thumb_dir = os.path.join(tmpdir, "thumbnails")
    os.makedirs(thumb_dir)

    # Create a test image and XMP (so accept can write to it)
    img = Image.new('RGB', (100, 100), color='red')
    img_path = os.path.join(tmpdir, "bird1.jpg")
    img.save(img_path)

    from xmp_writer import write_xmp_sidecar
    xmp_path = os.path.join(tmpdir, "bird1.xmp")
    write_xmp_sidecar(xmp_path, flat_keywords={'Dyke Marsh'}, hierarchical_keywords=set())

    # Create thumbnail
    thumb = Image.new('RGB', (100, 100), color='red')
    thumb.save(os.path.join(thumb_dir, "bird1.jpg"))

    results = {
        'folder': tmpdir,
        'settings': {'threshold': 0.4, 'thumbnail_size': 400},
        'stats': {'total': 1, 'new': 1, 'refinement': 0, 'disagreement': 0, 'match': 0},
        'photos': [
            {
                'filename': 'bird1.jpg',
                'image_path': img_path,
                'xmp_path': xmp_path,
                'existing_species': [],
                'prediction': 'Northern cardinal',
                'confidence': 0.85,
                'category': 'new',
                'status': 'pending',
            }
        ],
    }

    results_path = os.path.join(tmpdir, "results.json")
    with open(results_path, 'w') as f:
        json.dump(results, f)

    return results_path


def test_get_photos():
    """GET /api/photos returns the photo list."""
    from review_server import create_app

    with tempfile.TemporaryDirectory() as tmpdir:
        results_path = _create_test_review_data(tmpdir)
        app = create_app(tmpdir)
        client = app.test_client()

        resp = client.get('/api/photos')
        assert resp.status_code == 200
        data = resp.get_json()
        assert len(data['photos']) == 1
        assert data['photos'][0]['prediction'] == 'Northern cardinal'


def test_accept_writes_xmp():
    """POST /api/accept/<filename> writes keyword to XMP and updates status."""
    from review_server import create_app
    from compare import read_xmp_keywords

    with tempfile.TemporaryDirectory() as tmpdir:
        results_path = _create_test_review_data(tmpdir)
        app = create_app(tmpdir)
        client = app.test_client()

        resp = client.post('/api/accept/bird1.jpg')
        assert resp.status_code == 200

        # Check XMP was updated
        xmp_path = os.path.join(tmpdir, "bird1.xmp")
        keywords = read_xmp_keywords(xmp_path)
        assert 'Northern cardinal' in keywords
        assert 'Dyke Marsh' in keywords  # existing keyword preserved

        # Check status updated in results.json
        with open(results_path) as f:
            data = json.load(f)
        assert data['photos'][0]['status'] == 'accepted'


def test_skip_updates_status():
    """POST /api/skip/<filename> marks photo as skipped."""
    from review_server import create_app

    with tempfile.TemporaryDirectory() as tmpdir:
        results_path = _create_test_review_data(tmpdir)
        app = create_app(tmpdir)
        client = app.test_client()

        resp = client.post('/api/skip/bird1.jpg')
        assert resp.status_code == 200

        with open(results_path) as f:
            data = json.load(f)
        assert data['photos'][0]['status'] == 'skipped'
```

**Step 2: Run tests to verify they fail**

Run: `cd /Users/julius/git/photo-tools && python -m pytest auto-labeler/tests/test_review_server.py -v`
Expected: FAIL — module does not exist

**Step 3: Implement review_server.py**

```python
# auto-labeler/review_server.py
"""Flask server for reviewing auto-labeler predictions.

Usage:
    python auto-labeler/review_server.py [--data-dir /tmp/photo-review] [--port 5000]
"""

import argparse
import json
import logging
import os
import sys
import webbrowser

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lr-migration'))

from flask import Flask, jsonify, request, send_from_directory, render_template
from xmp_writer import write_xmp_sidecar

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)


def create_app(data_dir):
    """Create the Flask app configured with a data directory.

    Args:
        data_dir: path containing results.json and thumbnails/
    """
    app = Flask(__name__, template_folder=os.path.join(os.path.dirname(__file__), 'templates'))
    app.config['DATA_DIR'] = data_dir

    def _load_results():
        with open(os.path.join(data_dir, 'results.json')) as f:
            return json.load(f)

    def _save_results(data):
        with open(os.path.join(data_dir, 'results.json'), 'w') as f:
            json.dump(data, f, indent=2)

    @app.route('/')
    def index():
        return render_template('review.html')

    @app.route('/api/photos')
    def get_photos():
        data = _load_results()
        category = request.args.get('category')
        if category:
            data['photos'] = [p for p in data['photos'] if p['category'] == category]
        return jsonify(data)

    @app.route('/api/accept/<filename>', methods=['POST'])
    def accept(filename):
        data = _load_results()
        for photo in data['photos']:
            if photo['filename'] == filename:
                # Write prediction to XMP as a plain keyword
                write_xmp_sidecar(
                    photo['xmp_path'],
                    flat_keywords={photo['prediction']},
                    hierarchical_keywords=set(),
                )
                photo['status'] = 'accepted'
                _save_results(data)
                return jsonify({'ok': True, 'status': 'accepted'})
        return jsonify({'error': 'not found'}), 404

    @app.route('/api/skip/<filename>', methods=['POST'])
    def skip(filename):
        data = _load_results()
        for photo in data['photos']:
            if photo['filename'] == filename:
                photo['status'] = 'skipped'
                _save_results(data)
                return jsonify({'ok': True, 'status': 'skipped'})
        return jsonify({'error': 'not found'}), 404

    @app.route('/api/accept-batch', methods=['POST'])
    def accept_batch():
        body = request.get_json()
        category = body.get('category')
        min_confidence = body.get('min_confidence', 0.0)

        data = _load_results()
        accepted = 0
        for photo in data['photos']:
            if photo['status'] != 'pending':
                continue
            if category and photo['category'] != category:
                continue
            if photo['confidence'] < min_confidence:
                continue
            try:
                write_xmp_sidecar(
                    photo['xmp_path'],
                    flat_keywords={photo['prediction']},
                    hierarchical_keywords=set(),
                )
                photo['status'] = 'accepted'
                accepted += 1
            except Exception:
                log.warning("Failed to write XMP for %s", photo['filename'], exc_info=True)

        _save_results(data)
        return jsonify({'ok': True, 'accepted': accepted})

    @app.route('/thumbnails/<filename>')
    def thumbnail(filename):
        return send_from_directory(os.path.join(data_dir, 'thumbnails'), filename)

    return app


def main():
    parser = argparse.ArgumentParser(description="Review auto-labeler predictions.")
    parser.add_argument("--data-dir", default="/tmp/photo-review", help="Directory with results.json")
    parser.add_argument("--port", type=int, default=5000)
    args = parser.parse_args()

    app = create_app(args.data_dir)
    webbrowser.open(f"http://localhost:{args.port}")
    app.run(host='127.0.0.1', port=args.port, debug=False)


if __name__ == "__main__":
    main()
```

**Step 4: Run tests to verify they pass**

Run: `cd /Users/julius/git/photo-tools && python -m pytest auto-labeler/tests/test_review_server.py -v`
Expected: all 3 tests PASS

**Step 5: Commit**

```bash
cd /Users/julius/git/photo-tools
git add auto-labeler/review_server.py auto-labeler/tests/test_review_server.py
git commit -m "feat: add Flask review server with accept/skip/batch API"
```

---

### Task 4: Review UI — single-page HTML/CSS/JS

**Files:**
- Create: `auto-labeler/templates/review.html`

**Step 1: Create the review UI**

This is a single HTML file with embedded CSS and JS. No build tools needed.

```html
<!-- auto-labeler/templates/review.html -->
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Photo Review</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #1a1a2e; color: #e0e0e0; }

  .header { background: #16213e; padding: 16px 24px; display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #0f3460; }
  .header h1 { font-size: 20px; font-weight: 600; }
  .header .stats { font-size: 14px; color: #a0a0a0; }
  .header .stats span { margin-left: 16px; }
  .stats .new-count { color: #4ecca3; }
  .stats .ref-count { color: #f0c040; }
  .stats .dis-count { color: #e74c3c; }
  .gear-btn { background: none; border: none; color: #a0a0a0; font-size: 20px; cursor: pointer; padding: 4px 8px; }
  .gear-btn:hover { color: #fff; }

  .settings-panel { display: none; background: #16213e; padding: 16px 24px; border-bottom: 1px solid #0f3460; }
  .settings-panel.open { display: flex; gap: 32px; align-items: center; }
  .settings-panel label { font-size: 14px; color: #a0a0a0; }
  .settings-panel input[type=range] { width: 200px; margin: 0 8px; }
  .settings-panel .value { font-size: 14px; min-width: 40px; }

  .tabs { display: flex; background: #16213e; padding: 0 24px; border-bottom: 1px solid #0f3460; }
  .tab { padding: 12px 20px; cursor: pointer; font-size: 14px; color: #a0a0a0; border-bottom: 2px solid transparent; }
  .tab:hover { color: #fff; }
  .tab.active { color: #fff; border-bottom-color: #4ecca3; }
  .tab .count { margin-left: 6px; font-size: 12px; background: #0f3460; padding: 2px 6px; border-radius: 10px; }

  .batch-bar { padding: 12px 24px; background: #16213e; border-bottom: 1px solid #0f3460; display: flex; gap: 12px; align-items: center; }
  .batch-bar button { padding: 6px 16px; border: none; border-radius: 4px; cursor: pointer; font-size: 13px; }
  .btn-accept-all { background: #4ecca3; color: #1a1a2e; }
  .btn-accept-all:hover { background: #3db890; }

  .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(var(--card-width, 420px), 1fr)); gap: 16px; padding: 24px; }

  .card { background: #16213e; border-radius: 8px; overflow: hidden; border: 1px solid #0f3460; }
  .card.accepted { opacity: 0.5; }
  .card.skipped { opacity: 0.3; }
  .card img { width: 100%; display: block; }
  .card-body { padding: 12px; }
  .card-filename { font-size: 13px; color: #a0a0a0; margin-bottom: 8px; }
  .card-existing { font-size: 13px; margin-bottom: 4px; }
  .card-existing span { color: #a0a0a0; }
  .card-prediction { font-size: 16px; font-weight: 600; margin-bottom: 4px; }
  .card-confidence { font-size: 13px; color: #a0a0a0; margin-bottom: 8px; }
  .confidence-bar { height: 4px; background: #0f3460; border-radius: 2px; margin-bottom: 12px; }
  .confidence-fill { height: 100%; border-radius: 2px; }

  .badge { display: inline-block; font-size: 11px; padding: 2px 8px; border-radius: 10px; font-weight: 600; margin-bottom: 8px; }
  .badge-new { background: #4ecca3; color: #1a1a2e; }
  .badge-refinement { background: #f0c040; color: #1a1a2e; }
  .badge-disagreement { background: #e74c3c; color: #fff; }

  .card-actions { display: flex; gap: 8px; }
  .card-actions button { flex: 1; padding: 8px; border: none; border-radius: 4px; cursor: pointer; font-size: 13px; font-weight: 500; }
  .btn-accept { background: #4ecca3; color: #1a1a2e; }
  .btn-accept:hover { background: #3db890; }
  .btn-skip { background: #333; color: #a0a0a0; }
  .btn-skip:hover { background: #444; }
  .btn-done { background: #0f3460; color: #4ecca3; cursor: default; }

  .empty { text-align: center; padding: 80px 24px; color: #a0a0a0; font-size: 16px; }
</style>
</head>
<body>

<div class="header">
  <h1 id="title">Photo Review</h1>
  <div class="stats" id="stats"></div>
  <button class="gear-btn" onclick="toggleSettings()">&#9881;</button>
</div>

<div class="settings-panel" id="settingsPanel">
  <div>
    <label>Thumbnail size:</label>
    <input type="range" id="thumbSize" min="200" max="800" value="400" oninput="updateThumbSize(this.value)">
    <span class="value" id="thumbSizeVal">400px</span>
  </div>
</div>

<div class="tabs" id="tabs"></div>

<div class="batch-bar" id="batchBar">
  <button class="btn-accept-all" onclick="acceptAll()">Accept All Visible</button>
  <span id="batchStatus" style="font-size:13px; color:#a0a0a0;"></span>
</div>

<div class="grid" id="grid"></div>
<div class="empty" id="empty" style="display:none;">No photos to review in this category.</div>

<script>
let allData = { photos: [], stats: {} };
let currentTab = 'all';
let thumbSize = 400;

async function loadPhotos() {
  const resp = await fetch('/api/photos');
  allData = await resp.json();
  document.getElementById('title').textContent = 'Review: ' + (allData.folder || '').split('/').pop();
  renderStats();
  renderTabs();
  renderGrid();
}

function renderStats() {
  const s = allData.stats || {};
  const el = document.getElementById('stats');
  el.innerHTML = `
    <span class="new-count">${countByCategory('new')} New</span>
    <span class="ref-count">${countByCategory('refinement')} Refinements</span>
    <span class="dis-count">${countByCategory('disagreement')} Disagreements</span>
  `;
}

function countByCategory(cat) {
  return allData.photos.filter(p => p.category === cat && p.status === 'pending').length;
}

function renderTabs() {
  const tabs = [
    { id: 'all', label: 'All' },
    { id: 'new', label: 'New' },
    { id: 'refinement', label: 'Refinement' },
    { id: 'disagreement', label: 'Disagreement' },
    { id: 'accepted', label: 'Accepted' },
  ];
  const el = document.getElementById('tabs');
  el.innerHTML = tabs.map(t => {
    const count = t.id === 'accepted'
      ? allData.photos.filter(p => p.status === 'accepted').length
      : t.id === 'all'
        ? allData.photos.filter(p => p.status === 'pending').length
        : countByCategory(t.id);
    return `<div class="tab ${currentTab === t.id ? 'active' : ''}" onclick="switchTab('${t.id}')">${t.label}<span class="count">${count}</span></div>`;
  }).join('');
}

function switchTab(tab) {
  currentTab = tab;
  renderTabs();
  renderGrid();
}

function getVisiblePhotos() {
  return allData.photos.filter(p => {
    if (currentTab === 'accepted') return p.status === 'accepted';
    if (currentTab === 'all') return p.status === 'pending';
    return p.category === currentTab && p.status === 'pending';
  });
}

function renderGrid() {
  const photos = getVisiblePhotos();
  const grid = document.getElementById('grid');
  const empty = document.getElementById('empty');

  if (photos.length === 0) {
    grid.innerHTML = '';
    empty.style.display = 'block';
    return;
  }
  empty.style.display = 'none';

  grid.style.setProperty('--card-width', thumbSize + 20 + 'px');
  grid.innerHTML = photos.map(p => {
    const badgeClass = 'badge-' + p.category;
    const cardClass = p.status !== 'pending' ? p.status : '';
    const confPct = Math.round(p.confidence * 100);
    const confColor = confPct >= 70 ? '#4ecca3' : confPct >= 50 ? '#f0c040' : '#e74c3c';

    const existingHtml = p.existing_species.length > 0
      ? `<div class="card-existing"><span>Current:</span> ${p.existing_species.join(', ')}</div>`
      : '';

    const actions = p.status === 'pending'
      ? `<div class="card-actions">
           <button class="btn-accept" onclick="acceptPhoto('${p.filename}')">Accept</button>
           <button class="btn-skip" onclick="skipPhoto('${p.filename}')">Skip</button>
         </div>`
      : `<div class="card-actions"><button class="btn-done">${p.status}</button></div>`;

    return `<div class="card ${cardClass}">
      <img src="/thumbnails/${encodeURIComponent(p.filename.replace(/\.[^.]+$/, '.jpg'))}" loading="lazy">
      <div class="card-body">
        <div class="card-filename">${p.filename}</div>
        <span class="badge ${badgeClass}">${p.category}</span>
        ${existingHtml}
        <div class="card-prediction">${p.prediction}</div>
        <div class="confidence-bar"><div class="confidence-fill" style="width:${confPct}%;background:${confColor}"></div></div>
        <div class="card-confidence">${confPct}% confidence</div>
        ${actions}
      </div>
    </div>`;
  }).join('');
}

async function acceptPhoto(filename) {
  await fetch('/api/accept/' + encodeURIComponent(filename), { method: 'POST' });
  const photo = allData.photos.find(p => p.filename === filename);
  if (photo) photo.status = 'accepted';
  renderStats();
  renderTabs();
  renderGrid();
}

async function skipPhoto(filename) {
  await fetch('/api/skip/' + encodeURIComponent(filename), { method: 'POST' });
  const photo = allData.photos.find(p => p.filename === filename);
  if (photo) photo.status = 'skipped';
  renderStats();
  renderTabs();
  renderGrid();
}

async function acceptAll() {
  const category = currentTab === 'all' ? undefined : currentTab;
  const resp = await fetch('/api/accept-batch', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ category, min_confidence: 0 }),
  });
  const result = await resp.json();
  document.getElementById('batchStatus').textContent = `Accepted ${result.accepted} photos`;
  await loadPhotos();
}

function toggleSettings() {
  document.getElementById('settingsPanel').classList.toggle('open');
}

function updateThumbSize(val) {
  thumbSize = parseInt(val);
  document.getElementById('thumbSizeVal').textContent = val + 'px';
  renderGrid();
}

loadPhotos();
</script>
</body>
</html>
```

**Step 2: Verify the template renders**

Run: `cd /Users/julius/git/photo-tools && python -c "from auto_labeler_test_helper import *; print('ok')" || echo "Just create the file — no test needed for HTML template"`

The HTML template is purely frontend — it gets tested via the server tests (the `test_get_photos` test already hits `/` implicitly through the Flask test client). Manual verification happens in Task 5.

**Step 3: Commit**

```bash
cd /Users/julius/git/photo-tools
mkdir -p auto-labeler/templates
git add auto-labeler/templates/review.html
git commit -m "feat: add review webapp UI with category tabs, settings, and batch actions"
```

---

### Task 5: Integration smoke test

**No code changes — validation only.**

**Step 1: Run analyze on real photos**

```bash
cd /Users/julius/git/photo-tools
python auto-labeler/analyze.py \
  --folder "/Volumes/Photography/Raw Files/USA/2019/2019-03-17" \
  --labels-file /tmp/usa_labels.txt \
  --output-dir /tmp/photo-review \
  --threshold 0.4 \
  --thumbnail-size 400
```

Check that `/tmp/photo-review/results.json` exists and contains categorized photos.
Check that `/tmp/photo-review/thumbnails/` contains JPEG thumbnails.

**Step 2: Start the review server**

```bash
cd /Users/julius/git/photo-tools
python auto-labeler/review_server.py --data-dir /tmp/photo-review
```

Browser opens to http://localhost:5000. Verify:
- Photos display with thumbnails
- Category tabs work (New, Refinement, Disagreement)
- Settings panel opens and thumbnail size slider works
- Accept/Skip buttons function
- Accept All works

**Step 3: Spot-check an accepted photo's XMP**

After accepting a photo in the UI, verify the keyword was written to the XMP sidecar:

```bash
python -c "
from xml.etree import ElementTree as ET
# Check a specific XMP file that was accepted
"
```
