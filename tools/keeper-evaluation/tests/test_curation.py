from copy import deepcopy

import pytest
from PIL import Image

from keeper_eval.curate import curate
from keeper_eval.manifest import consensus, file_digest, read_bundle, validate_samples
from keeper_eval.review import create_review_app


def test_native_pixels_and_catalog_are_frozen_without_decisions(evidence):
    manifest = read_bundle(evidence['bundle'])
    before = file_digest(evidence['db'])
    assert all('flag' not in sample for sample in manifest['samples'])
    assert manifest['review_protocol']['minimum_human_reviewers'] == 1
    first = manifest['samples'][0]
    assert first['pixel_rect'] == [20, 15, 60, 45]
    with Image.open(evidence['bundle'] / first['assets']['region']['path']) as crop:
        assert crop.size == (40, 30)
        assert not crop.getexif()
    assert all(label['animal'] == 'uncertain' for label in consensus(manifest, []).values())
    assert file_digest(evidence['db']) == before


def test_pixel_tampering_is_not_silently_remeasured(evidence):
    sample = evidence['manifest']['samples'][0]
    (evidence['bundle'] / sample['assets']['region']['path']).write_bytes(b'changed')
    with pytest.raises(ValueError, match='pixels changed'):
        read_bundle(evidence['bundle'])


@pytest.mark.parametrize('field', ['group_id', 'lineage_id', 'source_sha256'])
def test_originals_encounters_and_derivatives_never_cross_partitions(evidence, field):
    samples = deepcopy(evidence['manifest']['samples'])
    samples[-1][field] = samples[0][field]
    with pytest.raises(ValueError, match='crosses partitions'):
        validate_samples(samples)


def test_source_and_catalog_output_paths_cannot_be_overwritten(evidence):
    for output in (evidence['sources'] / 'evidence', evidence['db'].parent / 'evidence'):
        with pytest.raises(ValueError, match='separate|outside'):
            curate(evidence['db'], evidence['spec'], output)
        assert not output.exists()


def test_missing_original_has_no_working_copy_fallback(evidence, tmp_path):
    next(evidence['sources'].glob('*.png')).unlink()
    with pytest.raises(FileNotFoundError, match='Original unavailable'):
        curate(evidence['db'], evidence['spec'], tmp_path / 'new-bundle')
    assert not (tmp_path / 'new-bundle').exists()


def test_path_traversal_rejected_before_asset_writes(evidence, tmp_path):
    spec = deepcopy(evidence['spec'])
    spec['samples'][0]['sample_id'] = '../../escaped'
    with pytest.raises(ValueError, match='blinded ID'):
        curate(evidence['db'], spec, tmp_path / 'new-bundle')
    assert not (tmp_path / 'new-bundle').exists()


def test_review_hides_flags_filenames_partitions_predictions_and_other_judgments(evidence):
    app = create_review_app(evidence['bundle'], 'independent-human-one')
    client = app.test_client()
    session = client.get('/api/session').get_json()
    assert len(session['samples']) == 2
    serialized = str(session)
    for private in ('photo_id', 'detection_id', 'partition', 'original-0', 'source_sha256', 'flag', 'predictions'):
        assert private not in serialized
    sid = session['samples'][0]['sample_id']
    response = client.post('/api/label/' + sid, json={'animal': 'animal', 'focus': 'sufficient_detail',
                           'notes': 'synthetic test only', 'full_frame_inspected': True, 'native_crop_inspected': True})
    assert response.status_code == 200
    second = create_review_app(evidence['bundle'], 'independent-human-two').test_client()
    assert second.get('/api/session').get_json()['samples'][0]['label'] is None
    assert client.get('/asset/' + sid + '/region').status_code == 200
    assert client.get('/asset/' + sid + '/../../manifest.json').status_code == 404


def test_review_requires_actual_inspection_and_no_assumed_labels(evidence):
    client = create_review_app(evidence['bundle'], 'one').test_client()
    sid = evidence['manifest']['samples'][0]['sample_id']
    for label in ({}, {'animal': 'animal', 'focus': 'sufficient_detail', 'notes': 'not yet inspected'}):
        assert client.post('/api/label/' + sid, json=label).status_code == 400
    assert not (evidence['bundle'] / 'reviews').exists()


def test_holdout_review_requires_frozen_profile(evidence):
    with pytest.raises(ValueError, match='frozen candidate'):
        create_review_app(evidence['bundle'], 'one', partition='holdout')
