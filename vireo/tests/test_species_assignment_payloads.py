"""Reproducible payload/work counters for a synthetic 30-species labeling task."""
import json
from pathlib import Path

import pytest
from services import photo_ingest


def representative_cache(app, db):
    from pipeline import save_results_raw
    folder_id = db.photos.list_page()[0]['folder_id']
    for i in range(527):
        photo_ingest.add_photo(db, folder_id, f'benchmark-{i:03}.jpg', '.jpg', 1000, 1.0)
    photos = [dict(row, label='REVIEW') for row in db.conn.execute('SELECT * FROM photos ORDER BY id')]
    encounters = []
    for photo in photos:
        ids = [photo['id']]
        encounters.append({
            'photo_ids': ids, 'photo_count': 1, 'bursts': [{'photo_ids': ids, 'species_override': None}],
            'burst_count': 1, 'species': ['Unknown', .5], 'species_predictions': [],
            'confirmed_species': None, 'confirmed_species_list': [], 'species_confirmed': False,
            'time_range': [None, None],
            'trace': [{'decision': 'Synthetic grouping evidence. ' * 15, 'score': .8} for _ in range(20)],
        })
    results = {'photos': photos, 'encounters': encounters, 'summary': {}}
    save_results_raw(results, str(Path(app.config['DB_PATH']).parent), db.active_workspace_id)
    return [photo['id'] for photo in photos[:30]]


@pytest.mark.parametrize('mode', ['full', 'compact', 'bulk'])
def test_thirty_species_payload_and_serialization_work(app_and_db, monkeypatch, mode):
    import pipeline
    from repositories.edit_history import EditHistoryRepository
    app, db = app_and_db
    ids = representative_cache(app, db)
    client = app.test_client()
    counters = {'cache_loads': 0, 'cache_saves': 0, 'history_snapshots': 0,
                'history_snapshot_bytes': 0, 'full_tree_responses': 0}
    original_load, original_save = pipeline.load_results_raw, pipeline.save_results_raw
    original_store = EditHistoryRepository.store_grouping_payload
    original_dumps = app.json.dumps
    def load(*args, **kwargs):
        counters['cache_loads'] += 1
        return original_load(*args, **kwargs)
    def save(*args, **kwargs):
        counters['cache_saves'] += 1
        return original_save(*args, **kwargs)
    def store(repo, edit_id, payload):
        counters['history_snapshots'] += 1
        counters['history_snapshot_bytes'] += len(payload.encode())
        return original_store(repo, edit_id, payload)
    def dumps(obj, **kwargs):
        if isinstance(obj, dict) and 'encounters' in obj:
            counters['full_tree_responses'] += 1
        return original_dumps(obj, **kwargs)
    monkeypatch.setattr(pipeline, 'load_results_raw', load)
    monkeypatch.setattr(pipeline, 'save_results_raw', save)
    monkeypatch.setattr(EditHistoryRepository, 'store_grouping_payload', store)
    monkeypatch.setattr(app.json, 'dumps', dumps)
    assignments = [{'species': f'Benchmark species {i:02}', 'photo_ids': [pid]} for i, pid in enumerate(ids)]
    responses = []
    if mode == 'bulk':
        responses.append(client.post('/api/encounters/species/bulk', json={'assignments': assignments}))
    else:
        for assignment in assignments:
            responses.append(client.post('/api/encounters/species', json={
                **assignment, 'add': True, 'response_mode': mode}))
    assert all(response.status_code == 200 for response in responses)
    metrics = {'mode': mode, 'assignments': 30, 'encounters': 530, 'requests': len(responses),
               'response_bytes': sum(len(response.data) for response in responses), **counters}
    print('SPECIES_ASSIGNMENT_MEASUREMENTS ' + json.dumps(metrics, sort_keys=True))
    expected_operations = 1 if mode == 'bulk' else 30
    assert counters['cache_loads'] == counters['cache_saves'] == expected_operations
    assert counters['history_snapshots'] == expected_operations
    assert counters['full_tree_responses'] == (30 if mode == 'full' else 0)
    if mode != 'full':
        assert metrics['response_bytes'] < 500_000
        for response in responses:
            assert 'encounters' not in response.json
            affected = response.json['encounter_changes']['upserts']
            assert len(affected) == (30 if mode == 'bulk' else 1)
            assert all(update['encounter']['photo_ids'][0] in ids for update in affected)
