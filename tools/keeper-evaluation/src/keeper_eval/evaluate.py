"""Evaluate frozen candidates against independent, source-disjoint native-region judgments."""
import json
import math
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path

from scipy.stats import binomtest

from .manifest import consensus, digest, read_bundle, read_reviews, tree_fingerprint, write_json


def development_digest(manifest, reviews):
    labels = consensus(manifest, reviews)
    return digest({s['sample_id']: labels[s['sample_id']] for s in manifest['samples'] if s['partition'] == 'development'})



def validate_domain(domain, pixel_recipe):
    if (not isinstance(domain, dict) or set(domain) != {'camera_models', 'source_kinds', 'pixel_recipe', 'notes'}
            or domain['pixel_recipe'] != pixel_recipe or not isinstance(domain['notes'], str) or not domain['notes'].strip()):
        raise ValueError('Declare a camera/source/recipe domain and its evidence limits; source-ID allowlists are not approval')
    for key in ('camera_models', 'source_kinds'):
        if (not isinstance(domain[key], list) or not domain[key]
                or any(not isinstance(v, str) or not v.strip() for v in domain[key])):
            raise ValueError('Domain camera/source lists must be explicit and nonempty')


def in_domain(sample, profile):
    domain = profile['domain']
    return (sample['camera_model'] in domain['camera_models'] and sample['source_kind'] in domain['source_kinds']
            and sample['pixel_recipe'] == domain['pixel_recipe'])


def validate_evaluation_plan(plan):
    if (not isinstance(plan, dict) or set(plan) != {'maximum_false_keep_rate', 'confidence', 'minimum_positive_groups'}
            or any(type(plan[k]) not in (int, float) or not math.isfinite(plan[k]) or not 0 < plan[k] < 1
                   for k in ('maximum_false_keep_rate', 'confidence'))
            or type(plan['minimum_positive_groups']) is not int or plan['minimum_positive_groups'] < 1):
        raise ValueError('Freeze a finite false-keep budget, confidence level, and positive-group requirement before holdout review')


def freeze_profile(bundle, model, producer_code, *, focus_min, animal_min, domain, evaluation_plan, output):
    manifest = read_bundle(bundle)
    reviews = read_reviews(bundle, manifest)
    holdout = {s['sample_id'] for s in manifest['samples'] if s['partition'] == 'holdout'}
    if any(holdout.intersection(r['labels']) for r in reviews):
        raise ValueError('Freeze the producer before holdout judgments are recorded; do not retune on holdout labels')
    if Path(output).resolve().is_relative_to(Path(bundle).resolve()):
        raise ValueError('Store the frozen profile outside the evidence bundle')
    if (Path(bundle) / 'evaluation-lock.json').exists() or Path(output).exists():
        raise ValueError('Profile/output is frozen or holdout has already been evaluated')
    validate_domain(domain, manifest['pixel_recipe'])
    validate_evaluation_plan(evaluation_plan)
    if any(type(v) not in (int, float) or not math.isfinite(v) for v in (focus_min, animal_min)):
        raise ValueError('Declare the supported domain and finite thresholds selected on development evidence')
    profile = {'schema': 'vireo-keeper-candidate-v1', 'manifest_sha256': manifest['manifest_sha256'],
               'model_artifacts': tree_fingerprint(model), 'producer_code': tree_fingerprint(producer_code),
               'pixel_recipe': manifest['pixel_recipe'], 'development_labels_sha256': development_digest(manifest, reviews),
               'focus_min': focus_min, 'animal_min': animal_min, 'domain': domain, 'evaluation_plan': evaluation_plan,
               'frozen_at': datetime.now(UTC).isoformat()}
    profile['profile_sha256'] = digest(profile)
    write_json(output, profile)
    return profile


def read_profile(path, manifest):
    profile = json.loads(Path(path).read_text())
    recorded = profile.pop('profile_sha256')
    if (digest(profile) != recorded or profile.get('schema') != 'vireo-keeper-candidate-v1'
            or profile.get('manifest_sha256') != manifest['manifest_sha256']
            or profile.get('pixel_recipe') != manifest['pixel_recipe']):
        raise ValueError('Profile changed or belongs to different frozen pixel evidence')
    validate_domain(profile['domain'], manifest['pixel_recipe'])
    validate_evaluation_plan(profile.get('evaluation_plan'))
    profile['profile_sha256'] = recorded
    return profile


def upper_bound(errors, groups, confidence):
    if groups == 0:
        return None
    # One-sided exact Clopper-Pearson upper bound. Encounters, not repeated
    # neighboring frames, are the independent trial unit declared by curator.
    return float(binomtest(errors, groups, alternative='less').proportion_ci(
        confidence_level=confidence, method='exact').high)


def decisions(predictions, manifest, profile):
    if predictions.get('profile_sha256') != profile['profile_sha256']:
        raise ValueError('Predictions must name the frozen model/code/threshold profile')
    rows = predictions['samples']
    by_id = {r['sample_id']: r for r in rows}
    if len(by_id) != len(rows) or set(by_id) != {s['sample_id'] for s in manifest['samples']}:
        raise ValueError('Predictions must cover every frozen sample exactly once, including unavailable outcomes')
    accepted = {}
    for sample in manifest['samples']:
        row = by_id[sample['sample_id']]
        if row.get('region_sha256') != sample['assets']['region']['sha256']:
            raise ValueError('Prediction is not bound to the exact selected-region pixels')
        status = row.get('status')
        if not in_domain(sample, profile) and status != 'unavailable':
            raise ValueError('Out-of-domain evidence must abstain, independently of scores or cached approval claims')
        if status == 'unavailable':
            if not row.get('reason'):
                raise ValueError('Unavailable predictions need a recorded reason')
            accepted[sample['sample_id']] = False
        elif status == 'available':
            scores = [row.get('focus_score'), row.get('animal_score')]
            if any(type(v) not in (int, float) or not math.isfinite(v) for v in scores):
                raise ValueError('Available scores must be finite measured numbers; missing is not zero')
            accepted[sample['sample_id']] = scores[0] >= profile['focus_min'] and scores[1] >= profile['animal_min']
        else:
            raise ValueError('Prediction status must be available or unavailable')
    return accepted


def evaluation_metrics(samples, labels, accepted, confidence, gates):
    negatives, failures, positives, successes, uncertain = set(), set(), set(), set(), []
    strata = defaultdict(Counter)
    gate_negatives = {'animal': set(), 'focus': set()}
    gate_failures = {'animal': set(), 'focus': set()}
    for sample in samples:
        sid, group = sample['sample_id'], sample['group_id']
        label = labels[sid]
        for gate, bad in [('animal', label['animal'] == 'not_animal'),
                          ('focus', label['animal'] == 'animal' and label['focus'] == 'severe_defocus')]:
            if bad:
                gate_negatives[gate].add(group)
                if gates[gate][sid]:
                    gate_failures[gate].add(group)
        invalid = label['animal'] == 'not_animal' or label['focus'] == 'severe_defocus'
        valid = label['animal'] == 'animal' and label['focus'] == 'sufficient_detail'
        if invalid:
            negatives.add(group)
            if accepted[sid]:
                failures.add(group)
        elif valid:
            positives.add(group)
            if accepted[sid]:
                successes.add(group)
        elif accepted[sid]:
            uncertain.append(sid)
        strata[sample['camera_model'] + '/' + sample['source_kind']]['samples'] += 1
    return {'negative_groups': len(negatives), 'false_keep_groups': len(failures),
            'false_keep_rate_upper_bound': upper_bound(len(failures), len(negatives), confidence),
            'positive_groups': len(positives), 'positive_groups_with_an_accept': len(successes),
            'accepted_uncertain_samples': uncertain, 'strata': dict(strata),
            'gates': {gate: {'negative_groups': len(gate_negatives[gate]), 'false_accept_groups': len(gate_failures[gate]),
                            'false_accept_rate_upper_bound': upper_bound(len(gate_failures[gate]), len(gate_negatives[gate]), confidence)}
                      for gate in ('animal', 'focus')}}


def evaluate(bundle, profile_path, predictions):
    manifest = read_bundle(bundle)
    profile = read_profile(profile_path, manifest)
    plan = profile['evaluation_plan']
    maximum_false_keep_rate = plan['maximum_false_keep_rate']
    confidence = plan['confidence']
    minimum_positive_groups = plan['minimum_positive_groups']
    reviews = read_reviews(bundle, manifest)
    if profile['development_labels_sha256'] != development_digest(manifest, reviews):
        raise ValueError('Development judgments changed after the profile was frozen')
    accepted = decisions(predictions, manifest, profile)
    labels = consensus(manifest, reviews)
    heldout = [s for s in manifest['samples'] if s['partition'] == 'holdout']
    gates = {gate: {r['sample_id']: r['status'] == 'available' and r[gate + '_score'] >= profile[gate + '_min']
                    for r in predictions['samples']} for gate in ('animal', 'focus')}
    metrics = evaluation_metrics(heldout, labels, accepted, confidence, gates)
    lock = Path(bundle) / 'evaluation-lock.json'
    if lock.exists() and json.loads(lock.read_text())['profile_sha256'] != profile['profile_sha256']:
        raise ValueError('Holdout already exposed to another profile; use fresh held-out encounters')
    write_json(lock, {'profile_sha256': profile['profile_sha256']})
    upper = metrics['false_keep_rate_upper_bound']
    minimum_reviewers = manifest['review_protocol']['minimum_human_reviewers']
    all_judged = all(labels[s['sample_id']]['human_judgments'] >= minimum_reviewers for s in heldout)
    gate_support = all(g['false_accept_rate_upper_bound'] is not None
                       and g['false_accept_rate_upper_bound'] <= maximum_false_keep_rate for g in metrics['gates'].values())
    supported = (upper is not None and upper <= maximum_false_keep_rate and all_judged and gate_support
                 and not metrics['accepted_uncertain_samples']
                 and metrics['positive_groups_with_an_accept'] >= minimum_positive_groups)
    return {'schema': 'vireo-keeper-evaluation-v1', 'evaluated_at': datetime.now(UTC).isoformat(),
            'manifest_sha256': manifest['manifest_sha256'], 'profile_sha256': profile['profile_sha256'],
            'reviews_sha256': digest(reviews), 'predictions_sha256': digest(predictions),
            'domain': profile['domain'], 'confidence': confidence, 'maximum_false_keep_rate': maximum_false_keep_rate,
            'risk_definition': 'An invalid region is accepted in a known-negative encounter; denominator is known-negative encounters, not accepted keepers.',
            'minimum_positive_groups': minimum_positive_groups, 'heldout_samples': len(heldout),
            'review_protocol': manifest['review_protocol'],
            'all_heldout_independently_judged': all_judged, 'risk_requirement_met': supported,
            'runtime_approved': False, 'runtime_note': 'An offline report is not runtime authorization or a calibrated producer.',
            'scope': {'cameras': sorted({s['camera_model'] for s in heldout}),
                      'sessions': sorted({s['session_id'] for s in heldout}), 'pixel_recipe': manifest['pixel_recipe']},
            **metrics}
