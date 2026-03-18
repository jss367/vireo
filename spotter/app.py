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
    def _log_slow_requests(response):
        if hasattr(request, '_start_time'):
            elapsed = time.time() - request._start_time
            if elapsed > 0.5:
                log.warning("Slow request: %s %s took %.1fs",
                            request.method, request.path, elapsed)
        return response

    def _get_db():
        """Get a Database instance. Creates a new connection per request."""
        if not hasattr(app, '_db') or app._db is None:
            app._db = Database(db_path)
        return app._db

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

    # Cache classify config at startup (avoid re-parsing taxonomy on every request)
    _taxonomy_path = os.path.join(os.path.dirname(__file__), 'taxonomy.json')
    _weights_path = '/tmp/bioclip_model/open_clip_pytorch_model.bin'
    _classify_config = {
        'model_name': 'BioCLIP',
        'model_str': 'ViT-B-16',
        'weights_path': _weights_path,
        'weights_available': os.path.exists(_weights_path),
        'taxonomy_available': os.path.exists(_taxonomy_path),
        'taxonomy_species_count': init_db.count_keywords(),  # fast DB count
        'default_threshold': 0.4,
    }

    @app.route('/api/classify/config')
    def api_classify_config():
        """Return cached classifier configuration."""
        return jsonify(_classify_config)

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
            generate_all(thread_db, app.config['THUMB_CACHE_DIR'], progress_callback=thumb_cb)

            return {'photos_indexed': photo_count}

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
            generate_all(thread_db, app.config['THUMB_CACHE_DIR'], progress_callback=progress_cb)
            return {'ok': True}

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
        body = request.get_json(silent=True) or {}
        collection_id = body.get('collection_id')
        labels_file = body.get('labels_file')
        model_name = body.get('model_name', 'bioclip')
        threshold = body.get('threshold', 0.4)

        if not collection_id:
            return jsonify({'error': 'collection_id required'}), 400

        runner = app._job_runner

        def work(job):
            import tempfile
            from classifier import Classifier
            from compare import read_xmp_keywords, categorize
            from image_loader import load_image

            thread_db = Database(db_path)
            job['_start_time'] = time.time()

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
            elif tax:
                labels = list(tax._by_common.keys())[:1000]
                log.info("Using %d taxonomy species as classifier labels", len(labels))

            # Phase 3: Get photos from collection
            runner.push_event(job['id'], 'progress', {
                'current': 0, 'total': 0, 'current_file': 'Loading collection photos...', 'rate': 0,
            })
            photos = thread_db.get_collection_photos(collection_id, per_page=999999)
            folders = {f['id']: f['path'] for f in thread_db.get_folder_tree()}
            total = len(photos)
            job['progress']['total'] = total

            log.info("Classifying %d photos with model '%s'", total, model_name)

            # Phase 4: Initialize classifier (this is the slow part ~30s)
            runner.push_event(job['id'], 'progress', {
                'current': 0, 'total': total,
                'current_file': 'Loading BioCLIP model and computing label embeddings...',
                'rate': 0,
            })
            clf = Classifier(labels=labels)
            classified = 0
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

                # Load and classify
                img = load_image(image_path)
                if img is None:
                    failed += 1
                    continue

                with tempfile.NamedTemporaryFile(suffix='.jpg', delete=False) as tmp:
                    tmp_path = tmp.name
                    img.save(tmp_path, quality=85)

                try:
                    predictions = clf.classify(tmp_path, threshold=threshold)
                except Exception:
                    log.warning("Classification failed for %s", photo['filename'], exc_info=True)
                    failed += 1
                    continue
                finally:
                    os.unlink(tmp_path)

                if not predictions:
                    continue

                top = predictions[0]

                # Categorize against existing keywords
                category = 'new'
                if tax:
                    xmp_path = os.path.join(folder_path, os.path.splitext(photo['filename'])[0] + '.xmp')
                    existing = read_xmp_keywords(xmp_path)
                    category = categorize(top['species'], existing, tax)

                if category == 'match':
                    continue

                thread_db.add_prediction(
                    photo_id=photo['id'],
                    species=top['species'],
                    confidence=round(top['score'], 4),
                    model=model_name,
                    category=category,
                )
                classified += 1

            return {'classified': classified, 'failed': failed, 'total': total}

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
