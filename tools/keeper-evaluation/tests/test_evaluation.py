from copy import deepcopy

import pytest

from keeper_eval.evaluate import evaluate, freeze_profile, upper_bound
from keeper_eval.manifest import consensus, read_reviews, write_json
from keeper_eval.review import create_review_app

PLAN = {'maximum_false_keep_rate': .8, 'confidence': .95, 'minimum_positive_groups': 1}


def domain(evidence):
    return {'camera_models': ['test-camera'], 'source_kinds': ['original_raster'],
            'pixel_recipe': evidence['manifest']['pixel_recipe'], 'notes': 'Synthetic test domain, never runtime approved'}


def frozen(evidence, tmp_path):
    model, code = tmp_path / 'model', tmp_path / 'code'
    model.mkdir()
    code.mkdir()
    (model / 'model.onnx').write_bytes(b'synthetic graph, not a classifier')
    (model / 'model.onnx.data').write_bytes(b'synthetic weights')
    (code / 'candidate.py').write_text('# Synthetic test only\n')
    profile_path = tmp_path / 'profile.json'
    profile = freeze_profile(evidence['bundle'], model, code, focus_min=.5, animal_min=.5,
                             domain=domain(evidence), evaluation_plan=PLAN, output=profile_path)
    return profile, profile_path


def labels(evidence, profile_path, *, kind='human'):
    for reviewer in ('independent-one', 'independent-two'):
        client = create_review_app(evidence['bundle'], reviewer, partition='holdout',
                                   reviewer_kind=kind, profile_path=profile_path).test_client()
        for i, sample in enumerate(evidence['manifest']['samples'][4:]):
            response = client.post('/api/label/' + sample['sample_id'], json={
                'animal': 'not_animal' if i % 3 == 0 else 'animal',
                'focus': 'not_applicable' if i % 3 == 0 else 'severe_defocus' if i % 3 == 1 else 'sufficient_detail',
                'notes': 'Synthetic regression judgment only',
                'full_frame_inspected': True, 'native_crop_inspected': True})
            assert response.status_code == 200


def predictions(evidence, profile):
    return {'profile_sha256': profile['profile_sha256'], 'samples': [
        {'sample_id': s['sample_id'], 'region_sha256': s['assets']['region']['sha256'], 'status': 'available',
         'focus_score': float(i % 3 != 2), 'animal_score': float(i % 3 != 1)} for i, s in enumerate(evidence['manifest']['samples'])]}


def run(evidence, path, prediction):
    return evaluate(evidence['bundle'], path, prediction)


def test_zero_errors_is_not_a_zero_risk_claim():
    assert upper_bound(0, 0, .95) is None
    assert upper_bound(0, 3, .95) == pytest.approx(1 - .05 ** (1 / 3))
    assert upper_bound(0, 59, .95) < .05
    assert upper_bound(1, 59, .95) > .05


def test_frozen_model_includes_external_weights(evidence, tmp_path):
    profile, _ = frozen(evidence, tmp_path)
    assert set(profile['model_artifacts']['files']) == {'model.onnx', 'model.onnx.data'}


def test_no_human_labels_cannot_become_calibration(evidence, tmp_path):
    profile, path = frozen(evidence, tmp_path)
    result = run(evidence, path, predictions(evidence, profile))
    assert result['risk_requirement_met'] is False
    assert result['runtime_approved'] is False
    assert result['false_keep_rate_upper_bound'] is None
    assert result['accepted_uncertain_samples']


def test_assistant_labels_are_not_promoted_to_independent_ground_truth(evidence, tmp_path):
    profile, path = frozen(evidence, tmp_path)
    labels(evidence, path, kind='assistant')
    result = run(evidence, path, predictions(evidence, profile))
    assert result['all_heldout_independently_judged'] is False
    assert result['negative_groups'] == 0
    assert result['risk_requirement_met'] is False


def test_exact_bound_and_positive_yield_are_reported_without_runtime_authority(evidence, tmp_path):
    profile, path = frozen(evidence, tmp_path)
    labels(evidence, path)
    result = run(evidence, path, predictions(evidence, profile))
    assert result['negative_groups'] == 4
    assert result['positive_groups_with_an_accept'] == 2
    assert result['false_keep_groups'] == 0
    assert result['risk_requirement_met'] is True  # Permissive synthetic test budget only.
    assert result['runtime_approved'] is False
    assert result['maximum_false_keep_rate'] == PLAN['maximum_false_keep_rate']
    with pytest.raises(TypeError):
        evaluate(evidence['bundle'], path, predictions(evidence, profile), maximum_false_keep_rate=.99)


@pytest.mark.parametrize('defect', ['duplicate', 'missing', 'nan', 'wrong_pixels', 'wrong_profile'])
def test_predictions_cannot_drop_or_spoof_evidence(evidence, tmp_path, defect):
    profile, path = frozen(evidence, tmp_path)
    values = predictions(evidence, profile)
    if defect == 'duplicate':
        values['samples'].append(deepcopy(values['samples'][0]))
    elif defect == 'missing':
        values['samples'].pop()
    elif defect == 'nan':
        values['samples'][0]['focus_score'] = float('nan')
    elif defect == 'wrong_pixels':
        values['samples'][0]['region_sha256'] = 'not the frozen crop'
    else:
        values['profile_sha256'] = 'self-claimed validated'
    with pytest.raises(ValueError):
        run(evidence, path, values)


def test_changed_profile_and_threshold_reuse_on_holdout_fail(evidence, tmp_path):
    profile, path = frozen(evidence, tmp_path)
    run(evidence, path, predictions(evidence, profile))
    profile['focus_min'] = .1
    write_json(path, profile)
    with pytest.raises(ValueError, match='Profile changed'):
        run(evidence, path, predictions(evidence, profile))
    with pytest.raises(ValueError, match='frozen|already'):
        freeze_profile(evidence['bundle'], tmp_path / 'model', tmp_path / 'code', focus_min=.1, animal_min=.1,
                       domain=domain(evidence), evaluation_plan=PLAN, output=tmp_path / 'second.json')


def test_disagreement_is_uncertain_and_cannot_justify_a_keeper(evidence, tmp_path):
    _, path = frozen(evidence, tmp_path)
    labels(evidence, path)
    reviews = read_reviews(evidence['bundle'], evidence['manifest'])
    sid = evidence['manifest']['samples'][5]['sample_id']
    reviews[0]['labels'][sid]['animal'] = 'uncertain'
    result = consensus(evidence['manifest'], reviews)
    assert result[sid]['animal'] == 'uncertain'


def test_holdout_judgments_prevent_later_profile_selection(evidence, tmp_path):
    _, path = frozen(evidence, tmp_path)
    labels(evidence, path)
    with pytest.raises(ValueError, match='before holdout'):
        freeze_profile(evidence['bundle'], tmp_path / 'model', tmp_path / 'code', focus_min=.5, animal_min=.5,
                       domain=domain(evidence), evaluation_plan=PLAN, output=tmp_path / 'late.json')


def test_source_id_allowlists_are_not_a_deployment_domain(evidence, tmp_path):
    frozen(evidence, tmp_path)
    value = domain(evidence)
    value['source_ids'] = ['an already inspected photo']
    with pytest.raises(ValueError, match='source-ID allowlists'):
        freeze_profile(evidence['bundle'], tmp_path / 'model', tmp_path / 'code', focus_min=.5, animal_min=.5,
                       domain=value, evaluation_plan=PLAN, output=tmp_path / 'other.json')


def test_available_predictions_outside_declared_domain_cannot_be_used(evidence, tmp_path):
    profile, path = frozen(evidence, tmp_path)
    profile['domain']['camera_models'] = ['another-camera']
    from keeper_eval.manifest import digest
    unsigned = {k: v for k, v in profile.items() if k != 'profile_sha256'}
    profile['profile_sha256'] = digest(unsigned)
    write_json(path, profile)
    with pytest.raises(ValueError, match='Out-of-domain'):
        run(evidence, path, predictions(evidence, profile))


def test_missing_defocus_negatives_cannot_validate_both_gates(evidence, tmp_path):
    profile, path = frozen(evidence, tmp_path)
    labels(evidence, path)
    for review_path in (evidence['bundle'] / 'reviews').glob('*.json'):
        import json
        review = json.loads(review_path.read_text())
        for label in review['labels'].values():
            if label['focus'] == 'severe_defocus':
                label['focus'] = 'sufficient_detail'
        write_json(review_path, review)
    result = run(evidence, path, predictions(evidence, profile))
    assert result['gates']['focus']['negative_groups'] == 0
    assert result['gates']['focus']['false_accept_rate_upper_bound'] is None
    assert result['risk_requirement_met'] is False


@pytest.mark.parametrize('field,value', [('maximum_false_keep_rate', float('nan')), ('confidence', 1.),
                                        ('minimum_positive_groups', True)])
def test_invalid_evaluation_plan_cannot_be_frozen(evidence, tmp_path, field, value):
    frozen(evidence, tmp_path)
    plan = dict(PLAN, **{field: value})
    with pytest.raises(ValueError, match='Freeze a finite'):
        freeze_profile(evidence['bundle'], tmp_path / 'model', tmp_path / 'code', focus_min=.5, animal_min=.5,
                       domain=domain(evidence), evaluation_plan=plan, output=tmp_path / 'invalid.json')


def test_posthoc_criteria_change_cannot_reuse_exposed_holdout(evidence, tmp_path):
    from keeper_eval.manifest import digest
    profile, path = frozen(evidence, tmp_path)
    run(evidence, path, predictions(evidence, profile))
    profile['evaluation_plan']['maximum_false_keep_rate'] = .99
    profile['profile_sha256'] = digest({k: v for k, v in profile.items() if k != 'profile_sha256'})
    write_json(path, profile)
    with pytest.raises(ValueError, match='another profile'):
        run(evidence, path, predictions(evidence, profile))
