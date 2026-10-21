"""Frozen pixel evidence and leakage checks, separate from photographer flags."""
import hashlib
import json
import math
import re
from pathlib import Path

SCHEMA = 'vireo-keeper-regions-v1'
PARTITIONS = {'development', 'holdout', 'diagnostic'}
FOCUS_LABELS = {'sufficient_detail', 'severe_defocus', 'uncertain', 'not_applicable'}
ANIMAL_LABELS = {'animal', 'not_animal', 'uncertain'}


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def file_digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def tree_fingerprint(root):
    root = Path(root).resolve()
    if not root.is_dir():
        raise ValueError('Freeze an entire model/code directory, including external weights and configuration')
    files = {p.relative_to(root).as_posix(): file_digest(p) for p in sorted(root.rglob('*'))
             if p.is_file() and '__pycache__' not in p.parts and p.suffix != '.pyc' and p.name != '.DS_Store'}
    if not files:
        raise ValueError('Cannot fingerprint an empty artifact directory')
    return {'files': files, 'sha256': digest(files)}


def write_json(path, data):
    path = Path(path)
    path.write_text(json.dumps(data, indent=2, sort_keys=True, allow_nan=False) + '\n')


def valid_box(box):
    return (isinstance(box, list) and len(box) == 4
            and all(type(v) in (int, float) and math.isfinite(v) for v in box)
            and 0 <= box[0] < 1 and 0 <= box[1] < 1 and box[2] > 0 and box[3] > 0
            and box[0] + box[2] <= 1.000001 and box[1] + box[3] <= 1.000001)


def validate_samples(samples):
    if not samples:
        raise ValueError('At least one sample is required')
    ids, partitions = set(), {}
    for sample in samples:
        sid = sample['sample_id']
        if not re.fullmatch(r'[a-f0-9]{12}', sid) or sid in ids:
            raise ValueError('Sample IDs must be unique blinded 12-digit hexadecimal values')
        ids.add(sid)
        partition = sample['partition']
        if partition not in PARTITIONS or not valid_box(sample['box']):
            raise ValueError('Every sample needs a valid partition and selected normalized box')
        for field in ('group_id', 'lineage_id', 'source_sha256'):
            value = sample[field]
            if not isinstance(value, str) or not value:
                raise ValueError(f'Missing {field}')
            key = (field, value)
            if key in partitions and partitions[key] != partition:
                raise ValueError(f'{field} crosses partitions; originals, derivatives and encounters must stay together')
            partitions[key] = partition
        if not re.fullmatch(r'[a-f0-9]{64}', sample['source_sha256']):
            raise ValueError('Source bytes must have an actual SHA256 digest')
        if not sample.get('session_id') or not sample.get('camera_model'):
            raise ValueError('Source session and camera must be declared, including explicit unknown values')


def read_bundle(bundle, *, verify_pixels=True):
    bundle = Path(bundle).resolve()
    manifest = json.loads((bundle / 'manifest.json').read_text())
    recorded = manifest.pop('manifest_sha256')
    if recorded != digest(manifest) or manifest.get('schema') != SCHEMA:
        raise ValueError('Frozen manifest digest or schema mismatch')
    validate_samples(manifest['samples'])
    protocol = manifest.get('review_protocol', {})
    if (type(protocol.get('minimum_human_reviewers')) is not int or protocol['minimum_human_reviewers'] < 1
            or not isinstance(protocol.get('description'), str) or not protocol['description'].strip()):
        raise ValueError('Frozen evidence must declare its independent human review protocol')
    manifest['manifest_sha256'] = recorded
    for sample in manifest['samples']:
        for asset in sample['assets'].values():
            path = (bundle / asset['path']).resolve()
            if not path.is_relative_to(bundle) or path == bundle:
                raise ValueError('Pixel asset leaves the evidence bundle')
            if verify_pixels and file_digest(path) != asset['sha256']:
                raise ValueError(f'Frozen pixels changed for {sample["sample_id"]}')
    return manifest


def validate_label(label):
    if label.get('animal') not in ANIMAL_LABELS or label.get('focus') not in FOCUS_LABELS:
        raise ValueError('Both animal validity and absolute detail need explicit labels')
    if label['focus'] == 'not_applicable' and label['animal'] != 'not_animal':
        raise ValueError('Focus is not applicable only when the selected region is not an animal')
    if not isinstance(label.get('notes'), str) or not label['notes'].strip() or len(label['notes']) > 2000:
        raise ValueError('Record a short visual reason, including why a case is uncertain')
    if label.get('native_crop_inspected') is not True or label.get('full_frame_inspected') is not True:
        raise ValueError('Inspect the full frame and native selected region before recording a judgment')


def read_reviews(bundle, manifest):
    samples = {s['sample_id'] for s in manifest['samples']}
    reviews, reviewers = [], set()
    for path in sorted((Path(bundle) / 'reviews').glob('*.json')):
        review = json.loads(path.read_text())
        if review.get('manifest_sha256') != manifest['manifest_sha256']:
            raise ValueError('Review refers to a different pixel manifest')
        identity = review.get('reviewer', '').strip()
        if not identity or identity in reviewers or review.get('reviewer_kind') not in {'human', 'assistant'}:
            raise ValueError('Each independent review file needs a distinct named reviewer and declared kind')
        reviewers.add(identity)
        for sid, label in review['labels'].items():
            if sid not in samples:
                raise ValueError('Review contains an unknown sample')
            validate_label(label)
        reviews.append(review)
    return reviews


def consensus(manifest, reviews):
    """Apply the declared human-review protocol; disagreement and assistant suggestions stay uncertain."""
    labels = {}
    for sample in manifest['samples']:
        judgments = [r['labels'][sample['sample_id']] for r in reviews
                     if r['reviewer_kind'] == 'human' and sample['sample_id'] in r['labels']]
        animal, focus = 'uncertain', 'uncertain'
        if len(judgments) >= manifest['review_protocol']['minimum_human_reviewers']:
            animals = {j['animal'] for j in judgments}
            focuses = {j['focus'] for j in judgments}
            if len(animals) == 1:
                animal = next(iter(animals))
            if len(focuses) == 1:
                focus = next(iter(focuses))
        labels[sample['sample_id']] = {'animal': animal, 'focus': focus, 'human_judgments': len(judgments)}
    return labels
