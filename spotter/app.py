"""Flask web app for the Spotter photo browser.

Usage:
    python spotter/app.py --db ~/.spotter/spotter.db [--port 8080]
"""

import argparse
import logging
import logging.handlers
import os
import sys
import webbrowser

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lr-migration'))

import json
import queue
import time

from flask import Flask, Response, jsonify, redirect, request, render_template, send_from_directory

from db import Database
from jobs import JobRunner, LogBroadcaster

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

# File logging with rotation — persists across restarts
_log_dir = os.path.expanduser("~/.spotter")
os.makedirs(_log_dir, exist_ok=True)
_file_handler = logging.handlers.RotatingFileHandler(
    os.path.join(_log_dir, "spotter.log"),
    maxBytes=5 * 1024 * 1024,  # 5 MB
    backupCount=3,
)
_file_handler.setFormatter(logging.Formatter(
    "%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
))
logging.getLogger().addHandler(_file_handler)

# Suppress noisy werkzeug request logs for polling endpoints
class _QuietRequestFilter(logging.Filter):
    """Filter out repetitive GET requests from werkzeug logs."""
    _quiet_paths = {'/api/jobs', '/api/logs/stream', '/api/logs/recent'}

    def filter(self, record):
        msg = record.getMessage()
        for path in self._quiet_paths:
            if f'GET {path}' in msg and '200' in msg:
                return False
        return True

logging.getLogger('werkzeug').addFilter(_QuietRequestFilter())


def create_app(db_path, thumb_cache_dir=None):
    """Create the Flask app for the Spotter photo browser.

    Args:
        db_path: path to the SQLite database
        thumb_cache_dir: path to thumbnail cache directory
    """
    app = Flask(__name__, template_folder=os.path.join(os.path.dirname(__file__), 'templates'))
    app.config['DB_PATH'] = db_path
    app.config['THUMB_CACHE_DIR'] = thumb_cache_dir or os.path.expanduser("~/.spotter/thumbnails")

    # Request timing middleware — logs slow requests
    @app.before_request
    def _start_timer():
        request._start_time = time.time()

    @app.after_request
    def _log_requests(response):
        if hasattr(request, '_start_time'):
            elapsed = time.time() - request._start_time
            # Log all POST/DELETE actions (user-initiated) and slow requests
            if request.method in ('POST', 'DELETE'):
                log.info("Action: %s %s → %s (%.1fs)",
                         request.method, request.path, response.status_code, elapsed)
            elif elapsed > 0.5:
                log.warning("Slow request: %s %s took %.1fs",
                            request.method, request.path, elapsed)
        return response

    def _get_db():
        """Get a Database instance. Creates a new connection per request."""
        if not hasattr(app, '_db') or app._db is None:
            app._db = Database(db_path)
        return app._db

    # Load user config (e.g. HF token) on startup
    import config as cfg
    startup_cfg = cfg.load()
    if startup_cfg.get('hf_token'):
        os.environ['HF_TOKEN'] = startup_cfg['hf_token']

    # Initialize job runner, log broadcaster, and default collections
    init_db = Database(db_path)
    init_db.create_default_collections()

    # Mark species keywords from taxonomy in background (avoids slow startup)
    import threading
    def _mark_species():
        taxonomy_path = os.path.join(os.path.dirname(__file__), 'taxonomy.json')
        if not os.path.exists(taxonomy_path):
            return
        try:
            from taxonomy import Taxonomy
            tax = Taxonomy(taxonomy_path)
            bg_db = Database(db_path)
            updated = bg_db.mark_species_keywords(tax)
            if updated:
                log.info("Marked %d keywords as species from taxonomy", updated)
        except Exception:
            log.debug("Could not load taxonomy for species marking", exc_info=True)
    threading.Thread(target=_mark_species, daemon=True).start()

    app._job_runner = JobRunner(db=init_db)
    app._log_broadcaster = LogBroadcaster(buffer_size=500)
    app._log_broadcaster.install()

    # -- Page routes --

    @app.route('/')
    def index():
        return redirect('/browse')

    @app.route('/browse')
    def browse():
        return render_template('browse.html')

    @app.route('/classify')
    def classify():
        return render_template('review.html')

    @app.route('/import')
    def import_page():
        return render_template('import.html')

    @app.route('/audit')
    def audit():
        return render_template('audit.html')

    @app.route('/settings')
    def settings():
        return render_template('settings.html')

    # -- API routes --

    @app.route('/api/browse/init')
    def api_browse_init():
        """Combined endpoint for browse page initial load — one request instead of five."""
        db = _get_db()
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 50, type=int)
        sort = request.args.get('sort', 'date')

        photos = db.get_photos(page=page, per_page=per_page, sort=sort)
        total = db.count_photos()
        folders = db.get_folder_tree()
        keywords = db.get_keyword_tree()
        collections = db.get_collections()

        return jsonify({
            'photos': [dict(p) for p in photos],
            'total': total,
            'page': page,
            'per_page': per_page,
            'folders': [dict(f) for f in folders],
            'keywords': [dict(k) for k in keywords],
            'collections': [dict(c) for c in collections],
        })

    @app.route('/api/folders')
    def api_folders():
        db = _get_db()
        folders = db.get_folder_tree()
        return jsonify([dict(f) for f in folders])

    @app.route('/api/photos')
    def api_photos():
        db = _get_db()
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 50, type=int)
        sort = request.args.get('sort', 'date')
        folder_id = request.args.get('folder_id', None, type=int)
        rating_min = request.args.get('rating_min', None, type=int)
        date_from = request.args.get('date_from', None)
        date_to = request.args.get('date_to', None)
        keyword = request.args.get('keyword', None)

        photos = db.get_photos(
            folder_id=folder_id,
            page=page,
            per_page=per_page,
            sort=sort,
            rating_min=rating_min,
            date_from=date_from,
            date_to=date_to,
            keyword=keyword,
        )

        # Total count — use count_photos for unfiltered, otherwise count the filtered set
        if not any([folder_id, rating_min, date_from, date_to, keyword]):
            total = db.count_photos()
        else:
            total = len(db.get_photos(
                folder_id=folder_id, rating_min=rating_min,
                date_from=date_from, date_to=date_to,
                keyword=keyword, per_page=999999,
            ))

        return jsonify({
            'photos': [dict(p) for p in photos],
            'total': total,
            'page': page,
            'per_page': per_page,
        })

    @app.route('/api/photos/<int:photo_id>')
    def api_photo_detail(photo_id):
        db = _get_db()
        photo = db.get_photo(photo_id)
        if not photo:
            return jsonify({'error': 'not found'}), 404

        result = dict(photo)
        keywords = db.get_photo_keywords(photo_id)
        result['keywords'] = [dict(k) for k in keywords]
        return jsonify(result)

    @app.route('/api/keywords')
    def api_keywords():
        db = _get_db()
        keywords = db.get_keyword_tree()
        return jsonify([dict(k) for k in keywords])

    # -- Edit API routes --

    @app.route('/api/photos/<int:photo_id>/rating', methods=['POST'])
    def api_set_rating(photo_id):
        db = _get_db()
        body = request.get_json(silent=True) or {}
        rating = body.get('rating', 0)
        db.update_photo_rating(photo_id, rating)
        db.queue_change(photo_id, 'rating', str(rating))
        return jsonify({'ok': True})

    @app.route('/api/photos/<int:photo_id>/flag', methods=['POST'])
    def api_set_flag(photo_id):
        db = _get_db()
        body = request.get_json(silent=True) or {}
        flag = body.get('flag', 'none')
        db.update_photo_flag(photo_id, flag)
        db.queue_change(photo_id, 'flag', flag)
        return jsonify({'ok': True})

    @app.route('/api/photos/<int:photo_id>/keywords', methods=['POST'])
    def api_add_keyword(photo_id):
        db = _get_db()
        body = request.get_json(silent=True) or {}
        name = body.get('name', '').strip()
        if not name:
            return jsonify({'error': 'name required'}), 400
        kid = db.add_keyword(name)
        db.tag_photo(photo_id, kid)
        db.queue_change(photo_id, 'keyword_add', name)
        return jsonify({'ok': True, 'keyword_id': kid})

    @app.route('/api/photos/<int:photo_id>/keywords/<int:keyword_id>', methods=['DELETE'])
    def api_remove_keyword(photo_id, keyword_id):
        db = _get_db()
        # Get keyword name for the pending change record
        keywords = db.get_photo_keywords(photo_id)
        kw_name = ''
        for k in keywords:
            if k['id'] == keyword_id:
                kw_name = k['name']
                break
        db.untag_photo(photo_id, keyword_id)
        db.queue_change(photo_id, 'keyword_remove', kw_name)
        return jsonify({'ok': True})

    @app.route('/api/sync/status')
    def api_sync_status():
        db = _get_db()
        changes = db.get_pending_changes()
        return jsonify({
            'pending_count': len(changes),
        })


    # -- Collection API routes --

    @app.route('/api/collections')
    def api_collections():
        db = _get_db()
        collections = db.get_collections()
        return jsonify([dict(c) for c in collections])

    @app.route('/api/collections', methods=['POST'])
    def api_create_collection():
        db = _get_db()
        body = request.get_json(silent=True) or {}
        import json
        name = body.get('name', '').strip()
        rules = body.get('rules', [])
        if not name:
            return jsonify({'error': 'name required'}), 400
        cid = db.add_collection(name, json.dumps(rules))
        return jsonify({'ok': True, 'id': cid})

    @app.route('/api/collections/<int:collection_id>', methods=['DELETE'])
    def api_delete_collection(collection_id):
        db = _get_db()
        db.delete_collection(collection_id)
        return jsonify({'ok': True})

    @app.route('/api/collections/<int:collection_id>/photos')
    def api_collection_photos(collection_id):
        db = _get_db()
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 50, type=int)
        photos = db.get_collection_photos(collection_id, page=page, per_page=per_page)
        return jsonify({
            'photos': [dict(p) for p in photos],
            'page': page,
            'per_page': per_page,
        })

    # -- Prediction API routes --

    @app.route('/api/predictions')
    def api_predictions():
        db = _get_db()
        collection_id = request.args.get('collection_id', None, type=int)
        status = request.args.get('status', None)
        if collection_id:
            photos = db.get_collection_photos(collection_id, per_page=999999)
            photo_ids = [p['id'] for p in photos]
            preds = db.get_predictions(photo_ids=photo_ids, status=status) if photo_ids else []
        else:
            preds = db.get_predictions(status=status)
        return jsonify([dict(p) for p in preds])

    @app.route('/api/predictions/<int:pred_id>/accept', methods=['POST'])
    def api_accept_prediction(pred_id):
        db = _get_db()
        db.accept_prediction(pred_id)
        return jsonify({'ok': True})

    @app.route('/api/predictions/<int:pred_id>/reject', methods=['POST'])
    def api_reject_prediction(pred_id):
        db = _get_db()
        db.update_prediction_status(pred_id, 'rejected')
        return jsonify({'ok': True})

    @app.route('/api/classify/config')
    def api_classify_config():
        """Return classifier configuration from model registry."""
        import config as cfg
        from models import get_active_model, get_taxonomy_info
        active = get_active_model()
        tax = get_taxonomy_info()
        user_cfg = cfg.load()
        return jsonify({
            'model_name': active['name'] if active else 'No model',
            'model_str': active['model_str'] if active else '',
            'weights_path': active['weights_path'] if active else '',
            'weights_available': active['downloaded'] if active else False,
            'taxonomy_available': tax['available'],
            'taxonomy_species_count': init_db.count_keywords(),
            'default_threshold': user_cfg['classification_threshold'],
            'default_grouping_window': user_cfg['grouping_window_seconds'],
        })

    @app.route('/api/config')
    def api_config_get():
        import config as cfg
        return jsonify(cfg.load())

    @app.route('/api/config', methods=['POST'])
    def api_config_set():
        import config as cfg
        body = request.get_json(silent=True) or {}
        current = cfg.load()
        for key in body:
            if key in cfg.DEFAULTS:
                current[key] = body[key]
        # Apply HF token to environment immediately
        hf_token = current.get('hf_token', '')
        if hf_token:
            os.environ['HF_TOKEN'] = hf_token
        elif 'HF_TOKEN' in os.environ:
            del os.environ['HF_TOKEN']
        cfg.save(current)
        return jsonify({'ok': True})

    @app.route('/api/embedding-cache')
    def api_embedding_cache():
        """Return info about cached label embeddings."""
        from classifier import CACHE_DIR
        if not os.path.isdir(CACHE_DIR):
            return jsonify({'entries': [], 'total_size': 0})
        entries = []
        total_size = 0
        for f in sorted(os.listdir(CACHE_DIR)):
            if f.endswith('.pt'):
                fp = os.path.join(CACHE_DIR, f)
                size = os.path.getsize(fp)
                total_size += size
                entries.append({'file': f, 'size': size})
        return jsonify({'entries': entries, 'total_size': total_size})

    @app.route('/api/embedding-cache', methods=['DELETE'])
    def api_embedding_cache_clear():
        """Clear all cached label embeddings."""
        import shutil
        from classifier import CACHE_DIR
        if os.path.isdir(CACHE_DIR):
            shutil.rmtree(CACHE_DIR)
            log.info("Embedding cache cleared")
        return jsonify({'ok': True})

    @app.route('/api/version')
    def api_version():
        return jsonify({'version': '26.3.1'})

    # -- Import API routes --

    @app.route('/api/import/preview', methods=['POST'])
    def api_import_preview():
        db = _get_db()
        body = request.get_json(silent=True) or {}
        catalogs = body.get('catalogs', [])
        if not catalogs:
            return jsonify({'error': 'catalogs required'}), 400
        try:
            from importer import preview_import
            result = preview_import(catalogs, db)
            return jsonify(result)
        except Exception as e:
            return jsonify({'error': str(e)}), 500

    # -- Audit API routes --

    @app.route('/api/audit/drift')
    def api_audit_drift():
        db = _get_db()
        from audit import check_drift
        return jsonify(check_drift(db))

    @app.route('/api/audit/orphans')
    def api_audit_orphans():
        db = _get_db()
        from audit import check_orphans
        return jsonify(check_orphans(db))

    @app.route('/api/audit/untracked')
    def api_audit_untracked():
        db = _get_db()
        body = request.args.getlist('root') or []
        from audit import check_untracked
        return jsonify(check_untracked(db, body))

    @app.route('/api/audit/resolve', methods=['POST'])
    def api_audit_resolve():
        db = _get_db()
        body = request.get_json(silent=True) or {}
        photo_id = body.get('photo_id')
        direction = body.get('direction')
        from audit import resolve_drift
        resolve_drift(db, photo_id, direction)
        return jsonify({'ok': True})

    @app.route('/api/audit/resolve-all', methods=['POST'])
    def api_audit_resolve_all():
        db = _get_db()
        body = request.get_json(silent=True) or {}
        direction = body.get('direction')
        from audit import check_drift, resolve_drift
        drifts = check_drift(db)
        for d in drifts:
            resolve_drift(db, d['photo_id'], direction)
        return jsonify({'ok': True, 'resolved': len(drifts)})

    @app.route('/api/audit/remove-orphans', methods=['POST'])
    def api_audit_remove_orphans():
        db = _get_db()
        body = request.get_json(silent=True) or {}
        photo_ids = body.get('photo_ids', [])
        from audit import remove_orphans
        remove_orphans(db, photo_ids)
        return jsonify({'ok': True, 'removed': len(photo_ids)})

    @app.route('/api/audit/import-untracked', methods=['POST'])
    def api_audit_import_untracked():
        db = _get_db()
        body = request.get_json(silent=True) or {}
        paths = body.get('paths', [])
        from audit import import_untracked
        import_untracked(db, paths)
        return jsonify({'ok': True, 'imported': len(paths)})

    # -- Scan status (kept, non-job) --

    # -- Model & Taxonomy API routes --

    @app.route('/api/models')
    def api_models():
        from models import get_models, get_active_model
        active = get_active_model()
        return jsonify({
            'models': get_models(),
            'active_id': active['id'] if active else None,
        })

    @app.route('/api/models/active', methods=['POST'])
    def api_set_active_model():
        body = request.get_json(silent=True) or {}
        model_id = body.get('model_id')
        if not model_id:
            return jsonify({'error': 'model_id required'}), 400
        from models import set_active_model
        set_active_model(model_id)
        return jsonify({'ok': True})

    @app.route('/api/models/custom', methods=['POST'])
    def api_add_custom_model():
        body = request.get_json(silent=True) or {}
        name = body.get('name', '').strip()
        weights_path = body.get('weights_path', '').strip()
        model_str = body.get('model_str', 'ViT-B-16')
        if not name or not weights_path:
            return jsonify({'error': 'name and weights_path required'}), 400
        from models import register_model
        model_id = 'custom-' + name.lower().replace(' ', '-')
        register_model(model_id, name, model_str, weights_path, 'Custom model')
        return jsonify({'ok': True, 'model_id': model_id})

    @app.route('/api/jobs/download-model', methods=['POST'])
    def api_job_download_model():
        body = request.get_json(silent=True) or {}
        model_id = body.get('model_id')
        if not model_id:
            return jsonify({'error': 'model_id required'}), 400

        runner = app._job_runner

        def work(job):
            from models import download_model
            def progress_cb(msg):
                job['progress']['current_file'] = msg
                runner.push_event(job['id'], 'progress', {
                    'current': 0, 'total': 0, 'current_file': msg, 'rate': 0,
                })
            path = download_model(model_id, progress_callback=progress_cb)
            return {'model_id': model_id, 'weights_path': path}

        job_id = runner.start('download-model', work, config={'model_id': model_id})
        return jsonify({'job_id': job_id})

    @app.route('/api/jobs/download-hf-model', methods=['POST'])
    def api_job_download_hf_model():
        body = request.get_json(silent=True) or {}
        repo_id = body.get('repo_id', '').strip()
        if not repo_id:
            return jsonify({'error': 'repo_id required'}), 400

        runner = app._job_runner

        def work(job):
            from models import download_hf_model
            def progress_cb(msg):
                job['progress']['current_file'] = msg
                runner.push_event(job['id'], 'progress', {
                    'current': 0, 'total': 0, 'current_file': msg, 'rate': 0,
                })
            result = download_hf_model(repo_id, progress_callback=progress_cb)
            return result

        job_id = runner.start('download-model', work, config={'repo_id': repo_id})
        return jsonify({'job_id': job_id})

    @app.route('/api/taxonomy/info')
    def api_taxonomy_info():
        from models import get_taxonomy_info
        return jsonify(get_taxonomy_info())

    @app.route('/api/jobs/download-taxonomy', methods=['POST'])
    def api_job_download_taxonomy():
        runner = app._job_runner

        def work(job):
            from taxonomy import download_taxonomy
            runner.push_event(job['id'], 'progress', {
                'current': 0, 'total': 0,
                'current_file': 'Downloading iNaturalist taxonomy...', 'rate': 0,
            })
            taxonomy_path = os.path.join(os.path.dirname(__file__), 'taxonomy.json')
            download_taxonomy(taxonomy_path)
            return {'ok': True}

        job_id = runner.start('download-taxonomy', work)
        return jsonify({'job_id': job_id})

    # -- Labels API routes --

    @app.route('/api/labels/search-places')
    def api_labels_search_places():
        q = request.args.get('q', '')
        if len(q) < 2:
            return jsonify([])
        from labels import search_places
        return jsonify(search_places(q))

    @app.route('/api/labels/taxon-groups')
    def api_labels_taxon_groups():
        from labels import TAXON_GROUPS
        return jsonify([
            {'key': k, 'name': v['name']} for k, v in TAXON_GROUPS.items()
        ])

    @app.route('/api/labels')
    def api_labels_list():
        from labels import get_saved_labels, get_active_labels
        saved = get_saved_labels()
        active = get_active_labels()
        return jsonify({
            'labels': saved,
            'active': active,
        })

    @app.route('/api/labels/active', methods=['POST'])
    def api_set_active_labels():
        body = request.get_json(silent=True) or {}
        labels_file = body.get('labels_file')
        if not labels_file:
            return jsonify({'error': 'labels_file required'}), 400
        from labels import set_active_labels
        set_active_labels(labels_file)
        return jsonify({'ok': True})

    @app.route('/api/jobs/fetch-labels', methods=['POST'])
    def api_job_fetch_labels():
        body = request.get_json(silent=True) or {}
        place_id = body.get('place_id')
        place_name = body.get('place_name', '')
        taxon_groups = body.get('taxon_groups', ['birds'])
        name = body.get('name', '')
        if not place_id:
            return jsonify({'error': 'place_id required'}), 400
        if not name:
            group_names = ', '.join(g.title() for g in taxon_groups)
            name = f"{place_name} {group_names}".strip()

        runner = app._job_runner

        def work(job):
            from labels import fetch_species_list, save_labels, set_active_labels
            def progress_cb(msg, current=None, total=None):
                job['progress']['current_file'] = msg
                if current is not None:
                    job['progress']['current'] = current
                if total is not None:
                    job['progress']['total'] = total
                runner.push_event(job['id'], 'progress', {
                    'current': current or 0,
                    'total': total or 0,
                    'current_file': msg,
                    'rate': 0,
                })
            species = fetch_species_list(place_id, taxon_groups, progress_callback=progress_cb)
            if not species:
                raise RuntimeError("No species found for this region and taxa selection")
            labels_path = save_labels(name, place_id, place_name, taxon_groups, species)
            set_active_labels(labels_path)
            return {'species_count': len(set(species)), 'labels_file': labels_path}

        job_id = runner.start('fetch-labels', work, config={
            'place_id': place_id, 'place_name': place_name, 'taxon_groups': taxon_groups,
        })
        return jsonify({'job_id': job_id})

    @app.route('/api/system/info')
    def api_system_info():
        """Return system information: GPU, Python, PyTorch."""
        import platform
        info = {
            'python_version': platform.python_version(),
            'platform': platform.platform(),
            'device': 'CPU',
            'device_detail': 'No GPU acceleration',
            'torch_version': None,
            'torch_detail': '',
        }
        try:
            import torch
            info['torch_version'] = torch.__version__
            if torch.cuda.is_available():
                info['device'] = 'CUDA'
                info['device_detail'] = torch.cuda.get_device_name(0)
            elif hasattr(torch.backends, 'mps') and torch.backends.mps.is_available():
                info['device'] = 'MPS'
                info['device_detail'] = 'Apple Metal Performance Shaders'
            else:
                info['device'] = 'CPU'
                info['device_detail'] = 'GPU not available — using CPU'
            info['torch_detail'] = f"CUDA: {torch.cuda.is_available()}, MPS: {getattr(torch.backends, 'mps', None) and torch.backends.mps.is_available()}"
        except ImportError:
            info['torch_detail'] = 'PyTorch not installed'
        return jsonify(info)

    @app.route('/api/scan/status')
    def api_scan_status():
        db = _get_db()

        # DB file size
        db_size = 0
        if os.path.exists(db_path):
            db_size = os.path.getsize(db_path)

        # Thumbnail cache size
        thumb_dir = app.config['THUMB_CACHE_DIR']
        thumb_size = 0
        if os.path.isdir(thumb_dir):
            for f in os.listdir(thumb_dir):
                fp = os.path.join(thumb_dir, f)
                if os.path.isfile(fp):
                    thumb_size += os.path.getsize(fp)

        return jsonify({
            'photo_count': db.count_photos(),
            'folder_count': db.count_folders(),
            'keyword_count': db.count_keywords(),
            'pending_changes': db.count_pending_changes(),
            'db_size': db_size,
            'thumb_cache_size': thumb_size,
        })

    # -- Job API routes --

    @app.route('/api/jobs/scan', methods=['POST'])
    def api_job_scan():
        body = request.get_json(silent=True) or {}
        root = body.get('root', '')
        incremental = body.get('incremental', False)
        if not root:
            return jsonify({'error': 'root path required'}), 400
        if not os.path.isdir(root):
            return jsonify({'error': f'directory not found: {root}'}), 400

        # Remember this scan root
        import config as cfg
        user_cfg = cfg.load()
        roots = user_cfg.get('scan_roots', [])
        if root not in roots:
            roots.insert(0, root)
            user_cfg['scan_roots'] = roots
            cfg.save(user_cfg)

        runner = app._job_runner

        def work(job):
            from scanner import scan as do_scan
            thread_db = Database(db_path)
            def progress_cb(current, total):
                job['progress']['current'] = current
                job['progress']['total'] = total
                runner.push_event(job['id'], 'progress', {
                    'current': current,
                    'total': total,
                    'current_file': job['progress'].get('current_file', ''),
                    'rate': round(current / max(time.time() - job['_start_time'], 0.01), 1),
                })
            job['_start_time'] = time.time()
            do_scan(root, thread_db, progress_callback=progress_cb, incremental=incremental)
            photo_count = thread_db.count_photos()

            # Auto-generate thumbnails after scan
            from thumbnails import generate_all
            log.info("Generating thumbnails...")
            runner.push_event(job['id'], 'progress', {
                'current': 0, 'total': photo_count,
                'current_file': 'Generating thumbnails...', 'rate': 0,
            })
            def thumb_cb(current, total):
                job['progress']['current'] = current
                job['progress']['total'] = total
                runner.push_event(job['id'], 'progress', {
                    'current': current, 'total': total,
                    'current_file': 'Generating thumbnails...',
                    'rate': round(current / max(time.time() - job['_start_time'], 0.01), 1),
                })
            thumb_result = generate_all(thread_db, app.config['THUMB_CACHE_DIR'], progress_callback=thumb_cb)

            return {'photos_indexed': photo_count, 'thumbnails': thumb_result}

        job_id = runner.start('scan', work, config={'root': root, 'incremental': incremental})
        return jsonify({'job_id': job_id})

    @app.route('/api/jobs/thumbnails', methods=['POST'])
    def api_job_thumbnails():
        runner = app._job_runner

        def work(job):
            from thumbnails import generate_all
            thread_db = Database(db_path)
            def progress_cb(current, total):
                job['progress']['current'] = current
                job['progress']['total'] = total
                runner.push_event(job['id'], 'progress', {
                    'current': current,
                    'total': total,
                    'rate': round(current / max(time.time() - job['_start_time'], 0.01), 1),
                })
            job['_start_time'] = time.time()
            return generate_all(thread_db, app.config['THUMB_CACHE_DIR'], progress_callback=progress_cb)

        job_id = runner.start('thumbnails', work)
        return jsonify({'job_id': job_id})

    @app.route('/api/jobs/import', methods=['POST'])
    def api_job_import():
        body = request.get_json(silent=True) or {}
        catalogs = body.get('catalogs', [])
        strategy = body.get('strategy', 'merge_all')
        write_xmp = body.get('write_xmp', False)
        if not catalogs:
            return jsonify({'error': 'catalogs required'}), 400

        runner = app._job_runner

        def work(job):
            from importer import execute_import
            thread_db = Database(db_path)
            def progress_cb(current, total):
                job['progress']['current'] = current
                job['progress']['total'] = total
                runner.push_event(job['id'], 'progress', {
                    'current': current,
                    'total': total,
                })
            return execute_import(catalogs, thread_db, write_xmp=write_xmp,
                                  strategy=strategy, progress_callback=progress_cb)

        job_id = runner.start('import', work, config={'catalogs': catalogs, 'strategy': strategy})
        return jsonify({'job_id': job_id})

    @app.route('/api/jobs/sync', methods=['POST'])
    def api_job_sync():
        runner = app._job_runner

        def work(job):
            from sync import sync_to_xmp
            thread_db = Database(db_path)
            def progress_cb(current, total):
                job['progress']['current'] = current
                job['progress']['total'] = total
                runner.push_event(job['id'], 'progress', {
                    'current': current,
                    'total': total,
                })
            return sync_to_xmp(thread_db, progress_callback=progress_cb)

        job_id = runner.start('sync', work)
        return jsonify({'job_id': job_id})

    @app.route('/api/jobs/classify', methods=['POST'])
    def api_job_classify():
        import config as cfg
        user_cfg = cfg.load()
        body = request.get_json(silent=True) or {}
        collection_id = body.get('collection_id')
        labels_file = body.get('labels_file')
        model_name = body.get('model_name', 'bioclip')
        threshold = body.get('threshold', user_cfg['classification_threshold'])
        grouping_window = body.get('grouping_window', user_cfg['grouping_window_seconds'])

        if not collection_id:
            return jsonify({'error': 'collection_id required'}), 400

        runner = app._job_runner

        def work(job):
            import tempfile
            from datetime import datetime as dt
            from classifier import Classifier
            from compare import read_xmp_keywords, categorize
            from grouping import group_by_timestamp, consensus_prediction
            from image_loader import load_image

            thread_db = Database(db_path)
            job['_start_time'] = time.time()

            # Resolve model from registry
            from models import get_active_model
            active_model = get_active_model()
            if not active_model:
                raise RuntimeError("No model available. Download one in Settings.")

            model_str = active_model['model_str']
            weights_path = active_model['weights_path']
            effective_name = active_model['name']

            # Phase 1: Load taxonomy
            runner.push_event(job['id'], 'progress', {
                'current': 0, 'total': 0, 'current_file': 'Loading taxonomy...', 'rate': 0,
            })
            taxonomy_path = os.path.join(os.path.dirname(__file__), 'taxonomy.json')
            tax = None
            if os.path.exists(taxonomy_path):
                from taxonomy import Taxonomy
                tax = Taxonomy(taxonomy_path)

            # Phase 2: Load labels
            labels = None
            if labels_file and os.path.exists(labels_file):
                with open(labels_file) as f:
                    labels = [line.strip() for line in f if line.strip()]
                log.info("Using %d labels from file: %s", len(labels), labels_file)
            else:
                # Try active labels from the labels manager
                from labels import get_active_labels
                active_labels = get_active_labels()
                if active_labels and os.path.exists(active_labels.get('labels_file', '')):
                    with open(active_labels['labels_file']) as f:
                        labels = [line.strip() for line in f if line.strip()]
                    log.info("Using %d labels from: %s",
                             len(labels), active_labels.get('name', active_labels['labels_file']))

            if not labels:
                raise RuntimeError(
                    "No labels available. Go to Settings > Labels and download a "
                    "species list for your region, or provide a custom labels file."
                )

            # Phase 3: Get photos from collection
            runner.push_event(job['id'], 'progress', {
                'current': 0, 'total': 0, 'current_file': 'Loading collection photos...', 'rate': 0,
            })
            photos = thread_db.get_collection_photos(collection_id, per_page=999999)
            folders = {f['id']: f['path'] for f in thread_db.get_folder_tree()}
            total = len(photos)
            job['progress']['total'] = total

            log.info("Classifying %d photos with '%s' (%s)", total, effective_name, model_str)

            # Phase 4: Initialize classifier
            runner.push_event(job['id'], 'progress', {
                'current': 0, 'total': total,
                'current_file': f'Loading {effective_name} model and computing label embeddings...',
                'rate': 0,
            })
            clf = Classifier(labels=labels, model_str=model_str, pretrained_str=weights_path)

            # Phase 5: Classify each photo individually
            raw_results = []  # list of {photo, prediction, confidence}
            failed = 0

            for i, photo in enumerate(photos):
                folder_path = folders.get(photo['folder_id'], '')
                image_path = os.path.join(folder_path, photo['filename'])

                job['progress']['current'] = i + 1
                job['progress']['current_file'] = photo['filename']
                runner.push_event(job['id'], 'progress', {
                    'current': i + 1,
                    'total': total,
                    'current_file': photo['filename'],
                    'rate': round((i + 1) / max(time.time() - job['_start_time'], 0.01), 1),
                })

                img = load_image(image_path)
                if img is None:
                    failed += 1
                    continue

                with tempfile.NamedTemporaryFile(suffix='.jpg', delete=False) as tmp:
                    tmp_path = tmp.name
                    img.save(tmp_path, quality=85)

                try:
                    all_preds = clf.classify(tmp_path, threshold=0)
                except Exception:
                    log.warning("Classification failed for %s", photo['filename'], exc_info=True)
                    failed += 1
                    continue
                finally:
                    os.unlink(tmp_path)

                if not all_preds:
                    continue

                top_pred = all_preds[0]
                preds = [p for p in all_preds if p['score'] >= threshold]

                if not preds:
                    log.info("%s: top prediction \"%s\" at %.2f (below threshold %.2f)",
                             photo['filename'], top_pred['species'], top_pred['score'], threshold)
                    continue

                top = preds[0]

                # Parse timestamp for grouping
                timestamp = None
                if photo['timestamp']:
                    try:
                        timestamp = dt.fromisoformat(photo['timestamp'])
                    except Exception:
                        pass

                raw_results.append({
                    'photo': photo,
                    'folder_path': folder_path,
                    'prediction': top['species'],
                    'confidence': top['score'],
                    'timestamp': timestamp,
                    'filename': photo['filename'],
                })

            # Phase 6: Group by timestamp and compute consensus
            runner.push_event(job['id'], 'progress', {
                'current': total, 'total': total,
                'current_file': 'Grouping and computing consensus...', 'rate': 0,
            })

            groups = group_by_timestamp(raw_results, window_seconds=grouping_window)
            classified = 0
            group_count = 0

            for group in groups:
                if len(group) == 1:
                    # Single photo — store directly
                    item = group[0]
                    photo = item['photo']
                    folder_path = item['folder_path']

                    category = 'new'
                    if tax:
                        xmp_path = os.path.join(folder_path, os.path.splitext(photo['filename'])[0] + '.xmp')
                        existing = read_xmp_keywords(xmp_path)
                        category = categorize(item['prediction'], existing, tax)

                    if category == 'match':
                        continue

                    thread_db.add_prediction(
                        photo_id=photo['id'],
                        species=item['prediction'],
                        confidence=round(item['confidence'], 4),
                        model=model_name,
                        category=category,
                    )
                    classified += 1
                else:
                    # Group — compute consensus
                    group_count += 1
                    gid = f"g{group_count:04d}"
                    cons_input = [
                        {'prediction': item['prediction'], 'confidence': item['confidence']}
                        for item in group
                    ]
                    cons = consensus_prediction(cons_input)
                    if not cons:
                        continue

                    # Categorize using the consensus prediction
                    representative = group[0]
                    category = 'new'
                    if tax:
                        xmp_path = os.path.join(representative['folder_path'],
                                                os.path.splitext(representative['photo']['filename'])[0] + '.xmp')
                        existing = read_xmp_keywords(xmp_path)
                        category = categorize(cons['prediction'], existing, tax)

                    if category == 'match':
                        continue

                    individual_json = json.dumps(cons['individual_predictions'])

                    # Store prediction for each photo in the group
                    for item in group:
                        thread_db.add_prediction(
                            photo_id=item['photo']['id'],
                            species=cons['prediction'],
                            confidence=round(cons['confidence'], 4),
                            model=model_name,
                            category=category,
                            group_id=gid,
                            vote_count=cons['vote_count'],
                            total_votes=cons['total_votes'],
                            individual=individual_json,
                        )
                    classified += len(group)

            log.info("Classification done: %d classified, %d groups, %d failed",
                     classified, group_count, failed)
            return {
                'classified': classified,
                'groups': group_count,
                'failed': failed,
                'total': total,
            }

        job_id = runner.start('classify', work, config={
            'collection_id': collection_id, 'model_name': model_name,
        })
        return jsonify({'job_id': job_id})

    @app.route('/api/jobs')
    def api_jobs_list():
        runner = app._job_runner
        db = _get_db()
        active = runner.list_jobs()
        history = runner.get_history(db, limit=10)
        return jsonify({'active': active, 'history': history})

    @app.route('/api/jobs/<job_id>')
    def api_job_status(job_id):
        job = app._job_runner.get(job_id)
        if not job:
            return jsonify({'error': 'job not found'}), 404
        return jsonify(job)

    @app.route('/api/jobs/<job_id>/stream')
    def api_job_stream(job_id):
        """SSE stream of job progress events."""
        runner = app._job_runner
        job = runner.get(job_id)
        if not job:
            return jsonify({'error': 'job not found'}), 404

        q = runner.subscribe(job_id)

        def generate():
            try:
                while True:
                    try:
                        event = q.get(timeout=1)
                        yield f"event: {event['type']}\ndata: {json.dumps(event['data'])}\n\n"
                        if event['type'] == 'complete':
                            break
                    except queue.Empty:
                        # Send keepalive
                        yield ": keepalive\n\n"
                        # Check if job is done (in case we missed the complete event)
                        j = runner.get(job_id)
                        if j and j['status'] in ('completed', 'failed'):
                            yield f"event: complete\ndata: {json.dumps({'status': j['status'], 'result': j['result'], 'errors': j['errors']})}\n\n"
                            break
            finally:
                runner.unsubscribe(job_id, q)

        return Response(generate(), mimetype='text/event-stream',
                        headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})

    # -- Global log stream --

    @app.route('/api/logs/stream')
    def api_log_stream():
        """SSE stream of all server log output.

        Auto-closes after 30s of inactivity to prevent stale connections
        from exhausting Flask's thread pool during page navigation.
        The browser's EventSource will auto-reconnect.
        """
        broadcaster = app._log_broadcaster
        q = broadcaster.subscribe()

        def generate():
            idle_count = 0
            try:
                while True:
                    try:
                        record = q.get(timeout=2)
                        yield f"event: log\ndata: {json.dumps(record)}\n\n"
                        idle_count = 0
                    except queue.Empty:
                        idle_count += 1
                        yield ": keepalive\n\n"
                        # Close after ~30s idle to free the thread
                        if idle_count >= 15:
                            return
            except GeneratorExit:
                pass
            finally:
                broadcaster.unsubscribe(q)

        return Response(generate(), mimetype='text/event-stream',
                        headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})

    @app.route('/api/logs/recent')
    def api_logs_recent():
        count = request.args.get('count', 100, type=int)
        return jsonify(app._log_broadcaster.get_recent(count))

    @app.route('/api/jobs/history')
    def api_job_history():
        db = _get_db()
        limit = request.args.get('limit', 10, type=int)
        return jsonify(app._job_runner.get_history(db, limit=limit))

    # -- Thumbnail serving --

    @app.route('/thumbnails/<filename>')
    def serve_thumbnail(filename):
        return send_from_directory(app.config['THUMB_CACHE_DIR'], filename)

    # -- Logs page --

    @app.route('/logs')
    def logs_page():
        return render_template('logs.html')

    return app


def main():
    parser = argparse.ArgumentParser(description="Spotter Photo Browser")
    parser.add_argument("--db", default=os.path.expanduser("~/.spotter/spotter.db"),
                        help="Path to SQLite database")
    parser.add_argument("--thumb-dir", default=os.path.expanduser("~/.spotter/thumbnails"),
                        help="Path to thumbnail cache directory")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--no-browser", action="store_true")
    args = parser.parse_args()

    app = create_app(db_path=args.db, thumb_cache_dir=args.thumb_dir)

    # Open browser after server is ready, not before
    if not args.no_browser:
        import threading
        import urllib.request

        def _open_browser():
            url = f"http://localhost:{args.port}"
            for _ in range(50):  # try for up to 5 seconds
                try:
                    urllib.request.urlopen(url, timeout=0.1)
                    webbrowser.open(url)
                    return
                except Exception:
                    time.sleep(0.1)

        threading.Thread(target=_open_browser, daemon=True).start()

    app.run(host='127.0.0.1', port=args.port, debug=False, threaded=True)


if __name__ == "__main__":
    main()
