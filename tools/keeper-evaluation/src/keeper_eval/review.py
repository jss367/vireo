"""Blinded local review writes only independent annotation files in the evidence bundle."""
import hashlib
import json
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock

from flask import Flask, jsonify, request, send_file

from .evaluate import read_profile
from .holdout import read_commitment
from .manifest import read_bundle, read_reviews, validate_label


def create_review_app(bundle, reviewer, *, partition='development', reviewer_kind='human', profile_path=None):
    bundle = Path(bundle).resolve()
    manifest = read_bundle(bundle)
    if not reviewer.strip() or reviewer_kind not in {'human', 'assistant'}:
        raise ValueError('Name the independent reviewer and declare human/assistant provenance')
    if partition not in {'development', 'holdout', 'diagnostic'}:
        raise ValueError('Unknown review partition')
    commitment = None
    if partition == 'holdout':
        if profile_path is None:
            raise ValueError('A frozen candidate profile is required before held-out review')
        commitment = read_commitment(bundle, read_profile(profile_path, manifest))
    read_reviews(bundle, manifest)  # Reject stale or malformed existing review artifacts.
    samples = {s['sample_id']: s for s in manifest['samples'] if s['partition'] == partition}
    app = Flask(__name__)
    app.config['MAX_CONTENT_LENGTH'] = 16 * 1024
    review_path = bundle / 'reviews' / (hashlib.sha256(reviewer.encode()).hexdigest()[:16] + '.json')
    lock = Lock()

    def read_own():
        if review_path.exists():
            review = json.loads(review_path.read_text())
            if review['reviewer'] != reviewer or review['reviewer_kind'] != reviewer_kind:
                raise ValueError('Reviewer identity/provenance cannot be changed in an existing review')
            return review
        return {'manifest_sha256': manifest['manifest_sha256'], 'reviewer': reviewer,
                'reviewer_kind': reviewer_kind, 'labels': {}, 'history': []}

    @app.get('/')
    def index():
        return send_file(Path(__file__).with_name('review.html'))

    @app.get('/api/session')
    def session():
        own = read_own()
        # No original names, photo/detection IDs, flags, predictions, split names
        # or other reviewers' answers are exposed in the judging page.
        return jsonify({'reviewer': reviewer, 'definition': manifest['label_definition'],
                        'samples': [{'sample_id': sid, 'width': s['assets']['region']['width'],
                                     'height': s['assets']['region']['height'], 'source_kind': s['source_kind'],
                                     'label': own['labels'].get(sid)} for sid, s in sorted(samples.items())]})

    @app.get('/asset/<sid>/<kind>')
    def asset(sid, kind):
        if sid not in samples or kind not in {'source', 'region', 'context'}:
            return jsonify(error='Unknown blinded sample or asset'), 404
        return send_file(bundle / samples[sid]['assets'][kind]['path'])

    @app.post('/api/label/<sid>')
    def label(sid):
        if sid not in samples:
            return jsonify(error='Sample is outside this review queue'), 404
        value = request.get_json(silent=True)
        if not isinstance(value, dict):
            return jsonify(error='An explicit judgment object is required'), 400
        try:
            validate_label(value)
        except ValueError as exc:
            return jsonify(error=str(exc)), 400
        value = {k: value[k] for k in ('animal', 'focus', 'notes', 'full_frame_inspected', 'native_crop_inspected')}
        value['recorded_at'] = datetime.now(UTC).isoformat()
        if commitment is not None:
            value['prediction_commitment_sha256'] = commitment['commitment_sha256']
        with lock:
            own = read_own()
            own['history'].append({'sample_id': sid, 'previous': own['labels'].get(sid), 'label': value})
            own['labels'][sid] = value
            review_path.parent.mkdir(exist_ok=True)
            with tempfile.NamedTemporaryFile('w', dir=review_path.parent, delete=False) as stream:
                json.dump(own, stream, indent=2, sort_keys=True)
                temporary = stream.name
            os.replace(temporary, review_path)
        return jsonify(ok=True)

    return app
