from pathlib import Path

import numpy as np
import pytest

from keeper_eval.candidate import enough_groups, fit, fit_detail
from keeper_eval.features import FEATURE_RECIPE
from keeper_eval.manifest import digest, tree_fingerprint, write_json


def synthetic_features(evidence, tmp_path):
    root = Path(__file__).resolve().parents[1] / 'src/keeper_eval'
    data = {'manifest_sha256': evidence['manifest']['manifest_sha256'], 'feature_recipe': FEATURE_RECIPE,
            'producer_code': tree_fingerprint(root), 'model_artifacts': {'sha256': 'synthetic'}, 'samples': [
                {'sample_id': s['sample_id'], 'region_sha256': s['assets']['region']['sha256'],
                 'embedding': np.ones(768).tolist(), 'native_detail': {'status': 'unavailable'}}
                for s in evidence['manifest']['samples']]}
    data['features_sha256'] = digest(data)
    path = tmp_path / 'features.json'
    write_json(path, data)
    return path


def test_actual_unlabeled_features_cannot_fit_a_keeper_producer(evidence, tmp_path):
    features = synthetic_features(evidence, tmp_path)
    domain = {'camera_models': ['test-camera'], 'source_kinds': ['original_raster'],
              'pixel_recipe': evidence['manifest']['pixel_recipe'], 'notes': 'Synthetic code test only'}
    with pytest.raises(ValueError, match='independent development judgments'):
        fit(evidence['bundle'], features, domain, tmp_path / 'candidate')
    assert not (tmp_path / 'candidate').exists()


def test_repeated_frames_do_not_manufacture_independent_training_examples():
    rows = [(np.ones(5), 'same-encounter', label) for label in (0, 1) for _ in range(50)]
    with pytest.raises(ValueError, match='two groups per class'):
        enough_groups(rows, 'test')


def test_detail_fitting_is_numeric_proposal_not_a_calibration_claim():
    rows = [(np.full(5, float(i)), f'independent-{i}', int(i >= 2)) for i in range(4)]
    model = fit_detail(rows)
    assert len(model['coefficients']) == 6
    assert np.isfinite(model['coefficients']).all()
    assert 'calibrated' not in model and 'runtime_approved' not in model


def test_single_photographer_development_fit_freeze_and_heldout_scores(evidence, tmp_path):
    import json
    from copy import deepcopy

    from keeper_eval.candidate import development_scores, score
    from keeper_eval.curate import curate
    from keeper_eval.evaluate import evaluate, freeze_profile
    from keeper_eval.review import create_review_app

    spec = deepcopy(evidence['spec'])
    for i, row in enumerate(spec['samples']):
        row['partition'] = 'development' if i < 6 else 'holdout'
    bundle = tmp_path / 'training-bundle'
    manifest = curate(evidence['db'], spec, bundle)
    evidence = {**evidence, 'bundle': bundle, 'manifest': manifest}
    features = synthetic_features(evidence, tmp_path)
    values = json.loads(features.read_text())
    for i, row in enumerate(values['samples']):
        animal = i not in (0, 1, 6)
        sharp = i not in (2, 3, 7)
        row['embedding'] = [1. if animal else -1.] + [0.] * 767
        row['native_detail'] = {'status': 'measured', 'median_variance': .1,
                               'median_gradient_energy': .1 if sharp else .0001,
                               'median_laplacian_variance': .2 if sharp else .00001,
                               'median_contrast_normalized_gradient': 1. if sharp else .01,
                               'median_contrast_normalized_laplacian': 2. if sharp else .001}
    unsigned = {k: v for k, v in values.items() if k != 'features_sha256'}
    values['features_sha256'] = digest(unsigned)
    write_json(features, values)
    domain = {'camera_models': ['test-camera'], 'source_kinds': ['original_raster'],
              'pixel_recipe': manifest['pixel_recipe'], 'notes': 'Synthetic regression study only'}

    def judge(partition, profile_path=None):
        client = create_review_app(bundle, 'single-independent-photographer', partition=partition,
                                   profile_path=profile_path).test_client()
        for i, sample in enumerate(manifest['samples']):
            if sample['partition'] != partition:
                continue
            animal = i not in (0, 1, 6)
            label = {'animal': 'animal' if animal else 'not_animal',
                     'focus': 'not_applicable' if not animal else 'severe_defocus' if i in (2, 3, 7) else 'sufficient_detail',
                     'notes': 'Synthetic code-test answer, not a wildlife calibration label',
                     'full_frame_inspected': True, 'native_crop_inspected': True}
            assert client.post('/api/label/' + sample['sample_id'], json=label).status_code == 200

    judge('development')
    model = tmp_path / 'fitted-candidate'
    result = fit(bundle, features, domain, model)
    assert result['training_groups'] == {'animal': 6, 'detail': 4}
    assert result['calibrated'] is False
    dev = development_scores(bundle, features, model, tmp_path / 'development.json')
    assert len(dev['samples']) == 6
    profile_path = tmp_path / 'frozen.json'
    code = Path(__file__).resolve().parents[1] / 'src/keeper_eval'
    freeze_profile(bundle, model, code, focus_min=0., animal_min=0., domain=domain,
                   evaluation_plan={'maximum_false_keep_rate': .99, 'confidence': .95, 'minimum_positive_groups': 1},
                   application_code=Path(__file__).resolve().parents[3] / 'vireo', output=profile_path)
    scored = score(bundle, features, model, profile_path, tmp_path / 'scores.json')
    judge('holdout', profile_path)
    report = evaluate(bundle, profile_path, scored)
    assert report['all_heldout_independently_judged'] is True
    assert report['review_protocol']['minimum_human_reviewers'] == 1
    assert report['false_keep_groups'] == 0
    assert report['runtime_approved'] is False


@pytest.mark.parametrize('changed_file,accepted', [('manifest.py', True), ('features.py', False)])
def test_feature_reuse_checks_numerical_code_and_revalidates_frozen_pixels(evidence, tmp_path, changed_file, accepted):
    import json

    from keeper_eval.candidate import read_features
    from keeper_eval.manifest import read_bundle
    path = synthetic_features(evidence, tmp_path)
    value = json.loads(path.read_text())
    value['producer_code']['files'][changed_file] = 'previous source fingerprint'
    value['features_sha256'] = digest({k: v for k, v in value.items() if k != 'features_sha256'})
    write_json(path, value)
    # Current validation still runs before consuming old measured features;
    # preserving original validator provenance is not bypassing corpus guards.
    manifest = read_bundle(evidence['bundle'])
    if accepted:
        assert len(read_features(path, manifest)[1]) == len(manifest['samples'])
    else:
        with pytest.raises(ValueError, match='Executed feature code differs'):
            read_features(path, manifest)
