import json
from copy import deepcopy
from pathlib import Path

import numpy as np
import pytest

from keeper_eval.candidate import fit
from keeper_eval.cli import main
from keeper_eval.curate import curate
from keeper_eval.features import FEATURE_RECIPE
from keeper_eval.manifest import digest, file_digest, tree_fingerprint, write_json
from keeper_eval.readiness import training_readiness
from keeper_eval.review import create_review_app


@pytest.fixture
def study(evidence, tmp_path):
    spec = deepcopy(evidence['spec'])
    for index, sample in enumerate(spec['samples']):
        sample['partition'] = 'development' if index < 6 else 'diagnostic' if index < 8 else 'holdout'
    bundle = tmp_path / 'study'
    manifest = curate(evidence['db'], spec, bundle)
    data = {'manifest_sha256': manifest['manifest_sha256'], 'feature_recipe': FEATURE_RECIPE,
            'producer_code': tree_fingerprint(Path(__file__).resolve().parents[1] / 'src/keeper_eval'),
            'model_artifacts': {'sha256': 'synthetic'}, 'samples': [
                {'sample_id': s['sample_id'], 'region_sha256': s['assets']['region']['sha256'],
                 'embedding': ([1.] if i >= 2 else [-1.]) + [0.] * 767,
                 'native_detail': {'status': 'measured', 'median_variance': .1,
                                  'median_gradient_energy': .001 if i in (2, 3) else .1,
                                  'median_laplacian_variance': .001 if i in (2, 3) else .2,
                                  'median_contrast_normalized_gradient': .01 if i in (2, 3) else 1.,
                                  'median_contrast_normalized_laplacian': .01 if i in (2, 3) else 2.}}
                for i, s in enumerate(manifest['samples'])]}
    features = tmp_path / 'study-features.json'
    save_features(features, data)
    domain = {'camera_models': ['test-camera'], 'source_kinds': ['original_raster'],
              'pixel_recipe': manifest['pixel_recipe'], 'notes': 'Synthetic regression study only'}
    return {'bundle': bundle, 'manifest': manifest, 'features': features, 'domain': domain}


def save_features(path, data):
    data['features_sha256'] = digest({k: v for k, v in data.items() if k != 'features_sha256'})
    write_json(path, data)


def judge(study, *, reviewer='synthetic-human', kind='human', animal_override=None):
    client = create_review_app(study['bundle'], reviewer, reviewer_kind=kind).test_client()
    for i, sample in enumerate(study['manifest']['samples'][:6]):
        animal = animal_override or ('not_animal' if i < 2 else 'animal')
        response = client.post('/api/label/' + sample['sample_id'], json={
            'animal': animal,
            'focus': 'not_applicable' if animal == 'not_animal' else 'severe_defocus' if i in (2, 3) else 'sufficient_detail',
            'notes': 'Synthetic code-test judgment only', 'full_frame_inspected': True, 'native_crop_inspected': True})
        assert response.status_code == 200


def report(study):
    return training_readiness(study['bundle'], study['features'], study['domain'])


def test_unlabeled_report_identifies_all_class_shortfalls_and_is_read_only(study):
    before = tree_fingerprint(study['bundle'])
    feature_hash = file_digest(study['features'])
    result = report(study)
    assert not result['class_coverage_met']
    assert result['development_samples'] == 6
    assert len(result['pending_human_review_samples']) == 6
    assert not result['uncertain_animal_samples']
    assert len(result['blocking_reasons']) == 4
    for gate in result['gates'].values():
        assert all(c == {'samples': 0, 'groups': 0, 'additional_groups_needed': 2} for c in gate['classes'].values())
    assert result['runtime_approved'] is result['calibrated'] is False
    assert tree_fingerprint(study['bundle']) == before
    assert file_digest(study['features']) == feature_hash


def test_report_and_actual_fit_use_the_same_eligible_classes(study, tmp_path):
    judge(study)
    result = report(study)
    assert result['class_coverage_met']
    assert not result['blocking_reasons']
    assert not result['pending_human_review_samples']
    assert result['gates']['animal']['classes']['animal'] == {
        'samples': 4, 'groups': 4, 'additional_groups_needed': 0}
    for gate, name in [('animal', 'not_animal'), ('focus', 'severe_defocus'), ('focus', 'sufficient_detail')]:
        assert result['gates'][gate]['classes'][name] == {
            'samples': 2, 'groups': 2, 'additional_groups_needed': 0}
    fitted = fit(study['bundle'], study['features'], study['domain'], tmp_path / 'candidate')
    assert fitted['training_groups'] == {'animal': 6, 'detail': 4}
    assert np.isfinite(fitted['detail']['coefficients']).all()


def test_assistant_judgments_do_not_fill_human_class_shortfalls(study):
    judge(study, reviewer='synthetic-assistant', kind='assistant')
    result = report(study)
    assert len(result['pending_human_review_samples']) == 6
    assert not result['class_coverage_met']
    assert all(c['groups'] == 0 for g in result['gates'].values() for c in g['classes'].values())


def test_declared_reviewer_minimum_must_be_met_before_counting_classes(study):
    manifest = study['manifest']
    manifest['review_protocol']['minimum_human_reviewers'] = 2
    manifest['manifest_sha256'] = digest({k: v for k, v in manifest.items() if k != 'manifest_sha256'})
    write_json(study['bundle'] / 'manifest.json', manifest)
    features = json.loads(study['features'].read_text())
    features['manifest_sha256'] = manifest['manifest_sha256']
    save_features(study['features'], features)
    judge(study)
    assert len(report(study)['pending_human_review_samples']) == 6
    assert not report(study)['class_coverage_met']
    judge(study, reviewer='another-synthetic-human')
    assert report(study)['class_coverage_met']


def test_uncertain_detail_does_not_fill_a_defocus_training_class(study):
    judge(study)
    client = create_review_app(study['bundle'], 'synthetic-human').test_client()
    sid = study['manifest']['samples'][2]['sample_id']
    assert client.post('/api/label/' + sid, json={
        'animal': 'animal', 'focus': 'uncertain', 'notes': 'Synthetic borderline detail judgment',
        'full_frame_inspected': True, 'native_crop_inspected': True}).status_code == 200
    result = report(study)
    assert result['uncertain_focus_samples'] == [sid]
    assert result['gates']['focus']['classes']['severe_defocus']['additional_groups_needed'] == 1
    assert not result['class_coverage_met']


def test_disagreement_stays_uncertain_instead_of_manufacturing_training_classes(study):
    judge(study)
    judge(study, reviewer='another-synthetic-human', animal_override='uncertain')
    result = report(study)
    assert not result['pending_human_review_samples']
    assert len(result['uncertain_animal_samples']) == 6
    assert not result['class_coverage_met']


def test_unavailable_detail_cannot_count_as_a_focus_training_group(study):
    judge(study)
    features = json.loads(study['features'].read_text())
    features['samples'][2]['native_detail'] = {'status': 'unavailable'}
    save_features(study['features'], features)
    result = report(study)
    assert not result['class_coverage_met']
    assert result['unavailable_development_detail'] == [study['manifest']['samples'][2]['sample_id']]
    assert result['gates']['focus']['classes']['severe_defocus']['additional_groups_needed'] == 1


def test_out_of_domain_development_is_excluded(study):
    judge(study)
    study['domain']['camera_models'] = ['another-camera']
    result = report(study)
    assert result['development_samples'] == 0
    assert result['out_of_domain_development_samples'] == 6
    assert not result['pending_human_review_samples']
    assert not result['class_coverage_met']


def test_repeated_frames_cannot_inflate_independent_class_coverage(study):
    manifest = study['manifest']
    manifest['samples'][1]['group_id'] = manifest['samples'][0]['group_id']
    manifest['manifest_sha256'] = digest({k: v for k, v in manifest.items() if k != 'manifest_sha256'})
    write_json(study['bundle'] / 'manifest.json', manifest)
    features = json.loads(study['features'].read_text())
    features['manifest_sha256'] = manifest['manifest_sha256']
    save_features(study['features'], features)
    judge(study)
    result = report(study)
    assert not result['class_coverage_met']
    assert result['gates']['animal']['classes']['not_animal'] == {
        'samples': 2, 'groups': 1, 'additional_groups_needed': 1}


def test_diagnostic_and_holdout_labels_never_fill_development_shortfalls(study):
    # Imported synthetic labels deliberately include reserved partitions. The
    # readiness report must neither use their classes nor expose their answers.
    review = {'manifest_sha256': study['manifest']['manifest_sha256'], 'reviewer': 'synthetic-import',
              'reviewer_kind': 'human', 'labels': {s['sample_id']: {
                  'animal': 'animal', 'focus': 'sufficient_detail', 'notes': 'Reserved synthetic answer',
                  'full_frame_inspected': True, 'native_crop_inspected': True}
                  for s in study['manifest']['samples'][6:]}}
    (study['bundle'] / 'reviews').mkdir()
    path = study['bundle'] / 'reviews' / 'import.json'
    write_json(path, review)
    before = report(study)
    for label in review['labels'].values():
        label.update(animal='not_animal', focus='not_applicable')
    write_json(path, review)
    assert report(study) == before
    assert not before['class_coverage_met']
    assert all(s['sample_id'] not in json.dumps(before) for s in study['manifest']['samples'][6:])


def test_cli_emits_actionable_json_without_writing_artifacts(study, tmp_path, capsys):
    domain_path = tmp_path / 'domain.json'
    write_json(domain_path, study['domain'])
    before = tree_fingerprint(study['bundle'])
    main(['training-readiness', '--bundle', str(study['bundle']), '--features', str(study['features']),
          '--domain', str(domain_path)])
    result = json.loads(capsys.readouterr().out)
    assert result['schema'] == 'vireo-keeper-training-readiness-v1'
    assert len(result['blocking_reasons']) == 4
    assert tree_fingerprint(study['bundle']) == before
