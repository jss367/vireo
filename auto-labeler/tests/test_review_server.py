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

    img = Image.new('RGB', (100, 100), color='red')
    img_path = os.path.join(tmpdir, "bird1.jpg")
    img.save(img_path)

    from xmp_writer import write_xmp_sidecar
    xmp_path = os.path.join(tmpdir, "bird1.xmp")
    write_xmp_sidecar(xmp_path, flat_keywords={'Dyke Marsh'}, hierarchical_keywords=set())

    thumb = Image.new('RGB', (100, 100), color='red')
    thumb.save(os.path.join(thumb_dir, "bird1.jpg"))

    results = {
        'folder': tmpdir,
        'models': {
            'bioclip-vit-b-16': {
                'model_str': 'ViT-B-16',
                'run_date': '2026-03-17',
                'threshold': 0.4,
            }
        },
        'settings': {'threshold': 0.4, 'thumbnail_size': 400, 'group_window': 10},
        'stats': {'total': 1, 'new': 1, 'refinement': 0, 'disagreement': 0, 'match': 0},
        'photos': [
            {
                'filename': 'bird1.jpg',
                'image_path': img_path,
                'xmp_path': xmp_path,
                'existing_species': [],
                'predictions': {
                    'bioclip-vit-b-16': {
                        'prediction': 'Northern cardinal',
                        'confidence': 0.85,
                        'category': 'new',
                    }
                },
                'status': 'pending',
            }
        ],
    }

    results_path = os.path.join(tmpdir, "results.json")
    with open(results_path, 'w') as f:
        json.dump(results, f)
    return results_path


def _create_group_data(tmpdir):
    """Create results.json with a photo group for testing."""
    thumb_dir = os.path.join(tmpdir, "thumbnails")
    os.makedirs(thumb_dir)

    from xmp_writer import write_xmp_sidecar

    paths = []
    for name in ['bird_a.jpg', 'bird_b.jpg', 'bird_c.jpg']:
        img_path = os.path.join(tmpdir, name)
        xmp_path = os.path.join(tmpdir, name.replace('.jpg', '.xmp'))
        Image.new('RGB', (100, 100)).save(img_path)
        Image.new('RGB', (50, 50)).save(os.path.join(thumb_dir, name))
        write_xmp_sidecar(xmp_path, flat_keywords=set(), hierarchical_keywords=set())
        paths.append((img_path, xmp_path))

    results = {
        'folder': tmpdir,
        'models': {'bioclip-vit-b-16': {'model_str': 'ViT-B-16', 'run_date': '2026-03-17', 'threshold': 0.4}},
        'settings': {'threshold': 0.4, 'thumbnail_size': 400, 'group_window': 10},
        'stats': {'total': 3, 'new': 3},
        'photos': [
            {
                'group_id': 'g0001',
                'representative': 'bird_a.jpg',
                'members': ['bird_a.jpg', 'bird_b.jpg', 'bird_c.jpg'],
                'member_paths': [p[0] for p in paths],
                'member_xmp_paths': [p[1] for p in paths],
                'existing_species': [],
                'consensus': {
                    'bioclip-vit-b-16': {
                        'prediction': 'Song sparrow',
                        'confidence': 0.82,
                        'individual_predictions': {'Song sparrow': 2, 'Lincoln sparrow': 1},
                    }
                },
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
        _create_test_review_data(tmpdir)
        app = create_app(tmpdir)
        client = app.test_client()
        resp = client.get('/api/photos')
        assert resp.status_code == 200
        data = resp.get_json()
        assert len(data['photos']) == 1


def test_accept_writes_xmp():
    """POST /api/accept/<filename> writes prediction to XMP."""
    from review_server import create_app
    from compare import read_xmp_keywords
    with tempfile.TemporaryDirectory() as tmpdir:
        results_path = _create_test_review_data(tmpdir)
        app = create_app(tmpdir)
        client = app.test_client()

        resp = client.post('/api/accept/bird1.jpg', json={'model': 'bioclip-vit-b-16'})
        assert resp.status_code == 200

        xmp_path = os.path.join(tmpdir, "bird1.xmp")
        keywords = read_xmp_keywords(xmp_path)
        assert 'Northern cardinal' in keywords
        assert 'Dyke Marsh' in keywords


def test_accept_group_writes_all_xmps():
    """POST /api/accept-group/<group_id> writes prediction to all member XMP files."""
    from review_server import create_app
    from compare import read_xmp_keywords
    with tempfile.TemporaryDirectory() as tmpdir:
        _create_group_data(tmpdir)
        app = create_app(tmpdir)
        client = app.test_client()

        resp = client.post('/api/accept-group/g0001', json={'model': 'bioclip-vit-b-16'})
        assert resp.status_code == 200
        data = resp.get_json()
        assert data['accepted_count'] == 3

        for name in ['bird_a.xmp', 'bird_b.xmp', 'bird_c.xmp']:
            kw = read_xmp_keywords(os.path.join(tmpdir, name))
            assert 'Song sparrow' in kw


def test_settings_get_and_save():
    """GET /api/settings returns settings, POST /api/settings saves them."""
    from review_server import create_app
    with tempfile.TemporaryDirectory() as tmpdir:
        _create_test_review_data(tmpdir)
        app = create_app(tmpdir)
        client = app.test_client()

        resp = client.get('/api/settings')
        assert resp.status_code == 200
        data = resp.get_json()
        assert 'group_window' in data

        resp = client.post('/api/settings', json={'group_window': 20, 'default_threshold': 0.5})
        assert resp.status_code == 200

        # Verify saved
        settings_path = os.path.join(tmpdir, "settings.json")
        assert os.path.exists(settings_path)
        with open(settings_path) as f:
            saved = json.load(f)
        assert saved['group_window'] == 20


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


def test_index_route():
    """GET / returns 200."""
    from review_server import create_app
    with tempfile.TemporaryDirectory() as tmpdir:
        _create_test_review_data(tmpdir)
        app = create_app(tmpdir)
        client = app.test_client()
        resp = client.get('/')
        assert resp.status_code == 200


def test_settings_page_route():
    """GET /settings returns 200."""
    from review_server import create_app
    with tempfile.TemporaryDirectory() as tmpdir:
        _create_test_review_data(tmpdir)
        app = create_app(tmpdir)
        client = app.test_client()
        resp = client.get('/settings')
        assert resp.status_code == 200
