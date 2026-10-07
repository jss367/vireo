import json

from encounter_eval.common import Group
from encounter_eval.continuity_compare import comparison_cases, run
from encounter_eval.library import prepare


def photos():
    return [{'id':i,'folder_id':1,'timestamp':f'2026-01-01T00:00:0{i}'} for i in (1,2,3)]


def groups():
    return [Group((1,),('inat:1',),'production'),Group((2,),None,'production'),Group((3,),('inat:1',),'production')]


def test_changed_case_preserves_full_groups_and_flags_lost_known_labels():
    before=groups()
    after=[Group((1,2,3),('inat:1',),'production')]
    cases=comparison_cases(photos(),{'2':{'taxa':['inat:1']}},before,after)
    assert len(cases)==1
    assert cases[0]['ids']==[1,2,3]
    assert cases[0]['recovered_known_labels']==[2]
    assert not cases[0]['lost_known_labels']
    before[1]=Group((2,),('inat:2',),'production')
    cases=comparison_cases(photos(),{'1':{'taxa':['inat:1']},'2':{'taxa':['inat:2']}},before,after)
    assert cases[0]['lost_known_labels']==[2]
    assert cases[0]['differing_reference_sets']


def test_remaining_interruption_is_only_a_short_same_species_candidate():
    before=groups()
    cases=comparison_cases(photos(),{},before,before)
    assert len(cases)==1 and cases[0]['kind']=='still_split'
    distant=photos()
    distant[-1]['timestamp']='2026-01-01T00:01:00'
    assert comparison_cases(distant,{},before,before)==[]
    before[-1]=Group((3,),('inat:2',),'production')
    assert comparison_cases(photos(),{},before,before)==[]


def test_partition_exclusion_happens_before_feature_loading(library,tmp_path):
    registry=tmp_path/'splits.json'
    registry.write_text(json.dumps({'seed':42,'days':{'2026-01-01':'test'}}))
    def forbidden(*args,**kwargs):
        raise AssertionError('Loaded held-out features')
    m=prepare(library,tmp_path/'run',split_registry=registry,
              included_partitions=('train','development'),feature_loader=forbidden)
    assert m['sessions']==[]
    assert m['included_partitions']==['development','train']


def test_paired_run_uses_identical_read_only_scope(library,tmp_path,monkeypatch):
    from encounter_eval import continuity_compare

    # A self-contained baseline keeps the test independent of Git history depth
    # and of whether the proposed feature loader has already been committed.
    original_output = continuity_compare.subprocess.check_output
    def source_output(command, **kwargs):
        if command[:2] == ['git', 'show']:
            return (
                b'from pipeline import load_photo_features as current_loader\n'
                b'def load_photo_features(db, **kwargs):\n'
                b'    assert db._ws_id() == 1\n'
                b'    return current_loader(db, **kwargs)\n'
            )
        return original_output(command, **kwargs)
    monkeypatch.setattr(continuity_compare.subprocess, 'check_output', source_output)

    registry=tmp_path/'custom-splits.json'
    registry.write_text(json.dumps({'seed':42,'days':{'2026-01-01':'train'}}))
    original=library.read_bytes()
    summary,cases=run(tmp_path/'comparison',library,scopes=[(1,None)], split_registries={1:registry})
    assert summary['stats']['photos']==12
    assert summary['test_sessions_evaluated']==0
    assert summary['metrics']['before']['counts']==summary['metrics']['after']['counts']
    assert not any(c['kind']=='changed' for c in cases)
    assert library.read_bytes()==original
    assert (tmp_path/'comparison/Review encounter grouping changes.html').is_file()


def test_comparison_requires_existing_explicit_registry(library, tmp_path):
    import pytest

    for registries in (None, {1: tmp_path / 'missing.json'}):
        with pytest.raises(ValueError, match='[Rr]egistry'):
            run(tmp_path / 'comparison', library, scopes=[(1, None)], split_registries=registries)
    assert not (tmp_path / 'comparison').exists()


def test_comparison_preserves_custom_heldout_registry_before_loading(library, tmp_path, monkeypatch):
    import pytest

    from encounter_eval import continuity_compare

    registry = tmp_path / 'heldout.json'
    registry.write_text(json.dumps({'seed': 7, 'days': {'2026-01-01': 'test'}}))
    actual = continuity_compare.subprocess.check_output

    def source(command, **kwargs):
        if command[:2] == ['git', 'show']:
            return b'def load_photo_features(*args, **kwargs):\n    raise AssertionError("Loaded heldout features")\n'
        return actual(command, **kwargs)

    monkeypatch.setattr(continuity_compare.subprocess, 'check_output', source)
    with pytest.raises(ValueError, match='No training/development'):
        run(tmp_path / 'comparison', library, scopes=[(1, None)], split_registries={1: registry})
    assert json.loads(registry.read_text()) == {'seed': 7, 'days': {'2026-01-01': 'test'}}
