"""An explicitly uncalibrated two-gate candidate; fit only from independent development judgments.

A DINO class-prototype difference proposes animal/background separation. A
regularized linear detail classifier proposes a severe-defocus gate. Neither
scores nor training completion constitute validated positive keeper evidence.
"""
import json
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np
from scipy.optimize import minimize

from .evaluate import development_digest, in_domain, read_profile, validate_domain
from .features import FEATURE_RECIPE
from .holdout import commit_predictions
from .manifest import consensus, digest, file_digest, read_bundle, read_reviews, tree_fingerprint, write_json
from .region_target import region_contract

DETAIL_FIELDS = ('median_variance', 'median_gradient_energy', 'median_laplacian_variance',
                 'median_contrast_normalized_gradient', 'median_contrast_normalized_laplacian')
FIT_RECIPE = 'equal-encounter-dino-prototypes-and-ridge-logit-v1'
MINIMUM_CLASS_GROUPS = 2


def read_features(path, manifest):
    features = json.loads(Path(path).read_text())
    recorded = features.pop('features_sha256')
    if (digest(features) != recorded or features.get('manifest_sha256') != manifest['manifest_sha256']
            or features.get('feature_recipe') != FEATURE_RECIPE):
        raise ValueError('Candidate features changed or refer to a different pixel/preprocessing recipe')
    # Frozen features retain their complete original source fingerprint. Recheck
    # exact pixels with today's stronger validators, and require the numerical
    # extractor itself to match; validator-only changes need no repeated inference.
    for name in ('features.py',):
        if features['producer_code']['files'].get(name) != file_digest(Path(__file__).with_name(name)):
            raise ValueError('Executed feature code differs; recompute features with the frozen extractor')
    rows = features['samples']
    by_id = {r['sample_id']: r for r in rows}
    if len(by_id) != len(rows) or set(by_id) != {s['sample_id'] for s in manifest['samples']}:
        raise ValueError('Feature rows must cover the exact frozen corpus once each')
    for sample in manifest['samples']:
        if by_id[sample['sample_id']]['region_sha256'] != sample['assets']['region']['sha256']:
            raise ValueError('Candidate feature rows refer to different selected-region pixels')
    features['features_sha256'] = recorded
    return features, by_id


def detail_vector(row):
    details = row['native_detail']
    if details['status'] != 'measured' or any(details.get(key) is None for key in DETAIL_FIELDS):
        return None
    result = np.log(np.maximum(np.asarray([details[key] for key in DETAIL_FIELDS], dtype=float), 1e-12))
    return result if np.isfinite(result).all() else None


def embedding(row):
    vector = np.asarray(row['embedding'], dtype=float)
    if vector.shape != (768,) or not np.isfinite(vector).all() or np.linalg.norm(vector) <= 0:
        raise ValueError('Candidate requires finite nonzero DINOv2-B crop embeddings')
    return vector / np.linalg.norm(vector)


def enough_groups(rows, name):
    for value in (0, 1):
        groups = {row[1] for row in rows if row[2] == value}
        if len(groups) < MINIMUM_CLASS_GROUPS:
            raise ValueError(f'{name} needs independent development judgments from at least two groups per class; do not borrow diagnostic/held-out labels')


def fit_detail(rows):
    enough_groups(rows, 'Absolute-detail candidate')
    x = np.asarray([r[0] for r in rows])
    y = np.asarray([r[2] for r in rows])
    counts = Counter(r[1] for r in rows)
    weights = np.asarray([1 / counts[r[1]] for r in rows], dtype=float)
    weights /= weights.sum()
    mean = np.average(x, axis=0, weights=weights)
    scale = np.sqrt(np.average((x - mean) ** 2, axis=0, weights=weights))
    scale[scale < 1e-6] = 1.
    x = np.column_stack([(x - mean) / scale, np.ones(len(x))])

    def objective(coefficients):
        logits = x @ coefficients
        loss = np.sum(weights * (np.logaddexp(0, logits) - y * logits)) + .1 * np.sum(coefficients[:-1] ** 2)
        return float(loss)

    result = minimize(objective, np.zeros(x.shape[1]), method='BFGS')
    if not result.success:
        raise ValueError('Detail fitting did not converge; no candidate artifact was written')
    return {'coefficients': result.x.tolist(), 'mean': mean.tolist(), 'scale': scale.tolist()}


def development_rows(manifest, reviews, by_id, domain):
    """Use the same eligible evidence for fitting and its read-only readiness report."""
    labels = consensus(manifest, reviews)
    animal_rows, detail_rows, unavailable = [], [], []
    for sample in manifest['samples']:
        if sample['partition'] != 'development' or not in_domain(sample, {'domain': domain}):
            continue
        row, label = by_id[sample['sample_id']], labels[sample['sample_id']]
        if label['animal'] in {'animal', 'not_animal'}:
            animal_rows.append((embedding(row), sample['group_id'], int(label['animal'] == 'animal')))
        vector = detail_vector(row)
        if label['animal'] == 'animal' and label['focus'] in {'sufficient_detail', 'severe_defocus'}:
            if vector is None:
                unavailable.append(sample['sample_id'])
            else:
                detail_rows.append((vector, sample['group_id'], int(label['focus'] == 'sufficient_detail')))
    return animal_rows, detail_rows, unavailable


def fit(bundle, feature_path, domain, output):
    manifest = read_bundle(bundle)
    validate_domain(domain, manifest['pixel_recipe'])
    region_contract(manifest)
    features, by_id = read_features(feature_path, manifest)
    reviews = read_reviews(bundle, manifest)
    animal_rows, detail_rows, unavailable = development_rows(manifest, reviews, by_id, domain)
    enough_groups(animal_rows, 'Animal-validity candidate')
    grouped = defaultdict(list)
    for vector, group, label in animal_rows:
        grouped[(group, label)].append(vector)
    prototypes = {}
    for label in (0, 1):
        vectors = [np.mean(rows, axis=0) for (_, y), rows in grouped.items() if y == label]
        vector = np.mean(vectors, axis=0)
        prototypes[str(label)] = (vector / max(np.linalg.norm(vector), 1e-12)).tolist()
    result = {'schema': 'vireo-keeper-two-gate-candidate-v1', 'fit_recipe': FIT_RECIPE, 'feature_recipe': FEATURE_RECIPE,
              'feature_model_artifacts': features['model_artifacts'], 'training_features_sha256': features['features_sha256'],
              'manifest_sha256': manifest['manifest_sha256'],
              'development_labels_sha256': development_digest(manifest, reviews), 'domain': domain,
              'animal_prototypes': prototypes, 'detail': fit_detail(detail_rows),
              'training_groups': {'animal': len({r[1] for r in animal_rows}), 'detail': len({r[1] for r in detail_rows})},
              'unavailable_development_detail': unavailable, 'calibrated': False, 'runtime_approved': False}
    output = Path(output)
    if output.exists() or output.resolve().is_relative_to(Path(bundle).resolve()):
        raise ValueError('Write a new candidate directory outside the frozen evidence bundle')
    output.mkdir(parents=True)
    result['candidate_sha256'] = digest(result)
    write_json(output / 'candidate.json', result)
    return result



def read_candidate(directory, manifest, features):
    model = json.loads((Path(directory) / 'candidate.json').read_text())
    recorded = model.pop('candidate_sha256')
    if (digest(model) != recorded or model.get('schema') != 'vireo-keeper-two-gate-candidate-v1'
            or model['feature_model_artifacts'] != features['model_artifacts']
            or model.get('training_features_sha256') != features['features_sha256']
            or model['manifest_sha256'] != manifest['manifest_sha256']):
        raise ValueError('Candidate model does not match its development/feature provenance')
    model['candidate_sha256'] = recorded
    return model


def score_rows(samples, by_id, model):
    rows = []
    scope = {'domain': model['domain']}
    for sample in samples:
        sid = sample['sample_id']
        row = {'sample_id': sid, 'region_sha256': sample['assets']['region']['sha256']}
        vector = detail_vector(by_id[sid])
        if not in_domain(sample, scope) or vector is None:
            row.update(status='unavailable', reason='outside_evaluated_domain' if not in_domain(sample, scope) else 'native_detail_unavailable')
        else:
            dino = embedding(by_id[sid])
            animal = float(dino @ (np.asarray(model['animal_prototypes']['1']) - np.asarray(model['animal_prototypes']['0'])))
            detail = (vector - np.asarray(model['detail']['mean'])) / np.asarray(model['detail']['scale'])
            focus = float(np.append(detail, 1.) @ np.asarray(model['detail']['coefficients']))
            row.update(status='available', animal_score=animal, focus_score=focus)
        rows.append(row)
    return rows


def development_scores(bundle, feature_path, model_directory, output):
    manifest = read_bundle(bundle)
    features, by_id = read_features(feature_path, manifest)
    model = read_candidate(model_directory, manifest, features)
    samples = [s for s in manifest['samples'] if s['partition'] == 'development']
    result = {'candidate_sha256': model['candidate_sha256'], 'samples': score_rows(samples, by_id, model),
              'partition': 'development', 'calibrated': False, 'runtime_approved': False}
    if Path(output).exists() or Path(output).resolve().is_relative_to(Path(bundle).resolve()):
        raise ValueError('Write new development scores outside the frozen evidence bundle')
    write_json(output, result)
    return result


def score(bundle, feature_path, model_directory, profile_path, output):
    manifest = read_bundle(bundle)
    features, by_id = read_features(feature_path, manifest)
    profile = read_profile(profile_path, manifest)
    if (tree_fingerprint(model_directory) != profile['model_artifacts']
            or tree_fingerprint(Path(__file__).parent) != profile['producer_code']):
        raise ValueError('Frozen candidate model/code artifact changed')
    model = read_candidate(model_directory, manifest, features)
    if (model['domain'] != profile['domain']
            or model['development_labels_sha256'] != profile['development_labels_sha256']):
        raise ValueError('Candidate fitting, domain and frozen profile do not match')
    result = {'profile_sha256': profile['profile_sha256'], 'samples': score_rows(manifest['samples'], by_id, model),
              'calibrated': False, 'runtime_approved': False}
    if Path(output).exists() or Path(output).resolve().is_relative_to(Path(bundle).resolve()):
        raise ValueError('Write new predictions outside the frozen evidence bundle')
    commit_predictions(bundle, manifest, profile, result)
    write_json(output, result)
    return result
