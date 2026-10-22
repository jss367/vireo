"""Commit frozen outputs before independent held-out judgments can be exposed."""
import json
from datetime import UTC, datetime
from pathlib import Path

from .manifest import digest, read_reviews

LOCK_NAME = 'prediction-commitment.json'


def read_commitment(bundle, profile):
    path = Path(bundle) / LOCK_NAME
    if not path.exists():
        raise ValueError('Commit frozen predictions before held-out review or evaluation; no late first commitment')
    value = json.loads(path.read_text())
    recorded = value.pop('commitment_sha256', None)
    if recorded != digest(value) or value.get('schema') != 'vireo-keeper-prediction-commitment-v1':
        raise ValueError('Invalid or changed prediction commitment')
    if value.get('profile_sha256') != profile['profile_sha256']:
        raise ValueError('Holdout already committed to another profile; use fresh held-out encounters')
    if datetime.fromisoformat(value['committed_at']) < datetime.fromisoformat(profile['frozen_at']):
        raise ValueError('Prediction commitment predates its frozen profile')
    value['commitment_sha256'] = recorded
    return value


def commit_predictions(bundle, manifest, profile, predictions):
    from .evaluate import decisions
    decisions(predictions, manifest, profile)
    path = Path(bundle) / LOCK_NAME
    if path.exists():
        value = read_commitment(bundle, profile)
        if value['predictions_sha256'] != digest(predictions):
            raise ValueError('Holdout predictions already committed; changed scores or availability need fresh held-out encounters')
        return value
    holdout = {s['sample_id'] for s in manifest['samples'] if s['partition'] == 'holdout'}
    for review in read_reviews(bundle, manifest):
        if holdout.intersection(review['labels']) or any(h['sample_id'] in holdout for h in review.get('history', [])):
            raise ValueError('Held-out judgments already exposed; cannot make a late first prediction commitment')
    value = {'schema': 'vireo-keeper-prediction-commitment-v1', 'profile_sha256': profile['profile_sha256'],
             'predictions_sha256': digest(predictions), 'committed_at': datetime.now(UTC).isoformat()}
    value['commitment_sha256'] = digest(value)
    # Exclusive creation makes competing first commitments fail closed.
    with path.open('x') as stream:
        json.dump(value, stream, sort_keys=True)
    return value


def require_committed_predictions(bundle, manifest, profile, predictions, reviews):
    value = read_commitment(bundle, profile)
    if value['predictions_sha256'] != digest(predictions):
        raise ValueError('Holdout predictions already committed; changed scores or availability need fresh held-out encounters')
    holdout = {s['sample_id'] for s in manifest['samples'] if s['partition'] == 'holdout'}
    for review in reviews:
        labels = [label for sid, label in review['labels'].items() if sid in holdout]
        labels.extend(h['label'] for h in review.get('history', []) if h['sample_id'] in holdout)
        for label in labels:
            if (label.get('prediction_commitment_sha256') != value['commitment_sha256']
                    or not label.get('recorded_at')
                    or datetime.fromisoformat(label['recorded_at']) < datetime.fromisoformat(value['committed_at'])):
                raise ValueError('Held-out judgment predates or is not bound to the pre-review prediction commitment')
    return value
