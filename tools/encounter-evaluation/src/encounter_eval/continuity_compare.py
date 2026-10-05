"""Paired old/new feature loading and encounter review on non-test sessions."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import importlib.util
import json
import subprocess
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

from .algorithms import run_algorithm
from .common import configure_repo, digest, encode, write_json
from .library import Taxonomy, inference_features, prepare, read_bundle, timestamp
from .scoring import score, summarize


def memberships(groups):
    return {pid: (i, g) for i, g in enumerate(groups) for pid in g.photo_ids}


def comparison_cases(photos, answers, before, after):
    """Keep complete changed groups; common boundaries separate independent cases."""
    old, new = memberships(before), memberships(after)
    cuts = [0] + [i for i in range(1, len(photos))
                  if old[photos[i-1]['id']][0] != old[photos[i]['id']][0]
                  and new[photos[i-1]['id']][0] != new[photos[i]['id']][0]] + [len(photos)]
    cases = []
    for a, b in zip(cuts, cuts[1:], strict=False):
        ids = [p['id'] for p in photos[a:b]]
        if all(old[pid][1] == new[pid][1] for pid in ids):
            continue
        old_groups = [before[i] for i in sorted({old[pid][0] for pid in ids})]
        new_groups = [after[i] for i in sorted({new[pid][0] for pid in ids})]
        lost, recovered = [], []
        for pid in ids:
            expected = set(answers.get(str(pid), {}).get('taxa', []))
            old_hit = expected & set(old[pid][1].roster or ())
            new_hit = expected & set(new[pid][1].roster or ())
            if old_hit - new_hit:
                lost.append(pid)
            if new_hit - old_hit:
                recovered.append(pid)
        different_labels = False
        for group in new_groups:
            if len({old[pid][0] for pid in group.photo_ids}) > 1:
                known = {tuple(answers[str(pid)]['taxa']) for pid in group.photo_ids if str(pid) in answers}
                different_labels |= len(known) > 1
        cases.append({'kind':'changed', 'ids':ids, 'before':old_groups, 'after':new_groups,
                      'lost_known_labels':lost, 'recovered_known_labels':recovered,
                      'differing_reference_sets':different_labels})
    # These are review candidates, not proven missed merges: agreement on a
    # species and a short time gap do not establish individual identity.
    by_id = {p['id']:p for p in photos}
    for left, middle, right in zip(after, after[1:], after[2:], strict=False):
        if not left.roster or left.roster != right.roster or middle.roster is not None or len(middle.photo_ids) > 3:
            continue
        anchors = [by_id[left.photo_ids[-1]], *[by_id[pid] for pid in middle.photo_ids], by_id[right.photo_ids[0]]]
        times = [timestamp(p['timestamp']) for p in anchors]
        if any(t is None for t in times) or len({p['folder_id'] for p in anchors}) != 1:
            continue
        if not 0 <= (times[-1]-times[0]).total_seconds() <= 3:
            continue
        ids = [*left.photo_ids, *middle.photo_ids, *right.photo_ids]
        cases.append({'kind':'still_split', 'ids':ids,
                      'before':[before[i] for i in sorted({old[pid][0] for pid in ids})],
                      'after':[left,middle,right], 'lost_known_labels':[], 'recovered_known_labels':[],
                      'differing_reference_sets':False})
    return cases


def _preview(pid, presentation):
    root = Path.home()/'.vireo'
    candidates = [root/'previews'/f'{pid}_{s}.jpg' for s in (960,1920,3840)]
    # Full-size JPEG working copy: present for many photos without a preview.
    candidates.append(root/'working'/f'{pid}.jpg')
    thumb = presentation.get('thumbnail')
    if thumb:
        path = Path(thumb)
        candidates.append(path if path.is_absolute() else root/'thumbnails'/path)
    candidates.append(root/'thumbnails'/f'{pid}.jpg')
    return next((p.as_uri() for p in candidates if p.is_file()), None)


def run(output, db, *, baseline_revision='HEAD', scopes, split_registries=None):
    repo = configure_repo()
    from bursts import detect_bursts
    from pipeline import load_photo_features

    registries = {}
    for workspace, _ in scopes:
        supplied = (split_registries or {}).get(workspace)
        if supplied is None:
            raise ValueError(f'Choose the established split registry for workspace {workspace}')
        path = Path(supplied).expanduser().resolve()
        if not path.is_file():
            raise ValueError(f'Established split registry does not exist: {path}')
        data = json.loads(path.read_text())
        if type(data.get('seed')) is not int or not isinstance(data.get('days'), dict):
            raise ValueError(f'Invalid established split registry: {path}')
        registries[workspace] = path, data['seed']
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    revision = subprocess.check_output(['git','rev-parse',baseline_revision],cwd=repo,text=True).strip()
    source = subprocess.check_output(['git','show',f'{revision}:vireo/pipeline.py'],cwd=repo)
    if b'def _rescue_full_image_runs' in source:
        raise ValueError('Baseline already includes the proposed repair; select an earlier revision')
    source_path = output/'baseline_pipeline.py'
    source_path.write_bytes(source)
    spec = importlib.util.spec_from_file_location('encounter_comparison_baseline',source_path)
    baseline_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(baseline_module)
    totals = {part:{version:Counter() for version in ('before','after')} for part in ('train','development')}
    cases, inventories, seen = [], [], set()
    all_stats = Counter()
    for scope_index, (workspace, capture_date) in enumerate(scopes):
        scope = output/f'scope-{scope_index}-workspace-{workspace}'
        scope.mkdir()
        old_dir = scope/'baseline-inputs'
        old_dir.mkdir()
        baseline_files = {}
        taxonomy = None
        def paired_loader(reader, _old_dir=old_dir, _baseline_files=baseline_files, **kwargs):
            nonlocal taxonomy
            if taxonomy is None:
                taxonomy = Taxonomy(reader.conn)
            old = baseline_module.load_photo_features(reader, **kwargs)
            order = {pid:i for i,pid in enumerate(kwargs['photo_ids'])}
            old.sort(key=lambda p:order[p['id']])
            old = [inference_features(p,taxonomy) for p in old]
            key = min(order)
            filename = f'{key}.json.gz'
            with gzip.GzipFile(filename=str(_old_dir/filename),mode='wb',mtime=0) as handle:
                handle.write(encode(old).encode())
            _baseline_files[str(key)] = {'path':filename,'digest':digest(old)}
            return load_photo_features(reader, **kwargs)

        # Explicit established registries preserve held-out membership even
        # when earlier runs used custom output directories or custom seeds.
        registry, seed = registries[workspace]
        if not registry.is_file():
            raise ValueError(f'Established split registry disappeared: {registry}')
        manifest = prepare(db,scope,workspace=workspace,capture_date=capture_date,
                           split_registry=registry,seed=seed,
                           included_partitions=('train','development'),feature_loader=paired_loader)
        write_json(scope/'baseline-manifest.json', {'revision':revision,'source_sha256':hashlib.sha256(source).hexdigest(),
                                                   'files':baseline_files})
        inventories.append({'workspace':workspace,'capture_date':capture_date,'inventory':manifest['inventory']})
        display = manifest['taxonomy_display']
        for entry in manifest['sessions']:
            assert entry['partition'] in totals
            bundle = read_bundle(scope,entry)
            photos, answers = bundle['photos'],bundle['answers']
            ids = {p['id'] for p in photos}
            if seen & ids:
                if ids <= seen:
                    continue
                raise ValueError('Scopes overlap partially; choose nonoverlapping full sessions')
            seen.update(ids)
            descriptor = baseline_files[str(min(ids))]
            with gzip.open(old_dir/descriptor['path'],'rt') as handle:
                old_photos = json.load(handle)
            if digest(old_photos) != descriptor['digest']:
                raise ValueError('Baseline input digest changed')
            assert [p['id'] for p in old_photos] == [p['id'] for p in photos]
            before = run_algorithm('production',old_photos,grouping_config=manifest['grouping_config'])
            after = run_algorithm('production',photos,grouping_config=manifest['grouping_config'])
            photo_maps = {'before':{p['id']:p for p in old_photos}, 'after':{p['id']:p for p in photos}}
            for version, groups, features in [('before',before,old_photos),('after',after,photos)]:
                counts = score(features,answers,groups)
                counts['bursts'] = sum(len(detect_bursts([photo_maps[version][pid] for pid in g.photo_ids],manifest['config']['pipeline'])) for g in groups)
                totals[entry['partition']][version].update(counts)
            all_stats['sessions'] += 1
            all_stats['photos'] += len(photos)
            all_stats['reference_photos'] += len(answers)
            all_stats['newly_recovered_subjects'] += sum(p['subject_uncertain'] and photo_maps['before'][p['id']]['subject_absent'] for p in photos)
            local_cases = comparison_cases(photos,answers,before,after)
            for case in local_cases:
                case['id'] = digest([entry['id'],case['kind'],case['ids']])[:20]
                case['session'] = entry['id']
                case['workspace'] = workspace
                case['partition'] = entry['partition']
                case['date'] = photos[0]['timestamp'][:10]
                case['photos'] = []
                for pid in case['ids']:
                    photo = photo_maps['after'][pid]
                    presentation = bundle['presentation'][str(pid)]
                    answer = answers.get(str(pid),{})
                    case['photos'].append({'id':pid,'filename':presentation['filename'],'timestamp':photo['timestamp'],
                        'preview':_preview(pid,presentation), 'labels':[display.get(k,k) for k in answer.get('taxa',[])],
                        'complete':answer.get('complete',False),
                        'rescued':(photo.get('weak_detection_context') or {}).get('evidence')=='full_image_sequence'})
                for version in ('before','after'):
                    case[version] = [{'ids':list(g.photo_ids),'species':', '.join(display.get(k,k) for k in g.roster) if g.roster else 'No species suggestion',
                        'bursts':len(detect_bursts([photo_maps[version][pid] for pid in g.photo_ids],manifest['config']['pipeline']))} for g in case[version]]
                cases.append(case)
            print(f"Compared {all_stats['sessions']} sessions / {all_stats['photos']:,} photos; {len(cases)} review cases",flush=True)
    if not seen:
        raise ValueError('No training/development sessions available in the requested scopes')
    cases.sort(key=lambda c:(not bool(c['lost_known_labels']),not c['differing_reference_sets'],c['kind']=='still_split',c['date'],c['id']))
    combined = {v:sum((totals[p][v] for p in totals),Counter()) for v in ('before','after')}
    summary = {'created_at':datetime.now(UTC).isoformat(),'baseline_revision':revision,
               'scope':inventories,'stats':dict(all_stats),'test_sessions_evaluated':0,
               'metrics':{v:summarize(counts) for v,counts in combined.items()},
               'partitions':{p:{v:summarize(c) for v,c in values.items()} for p,values in totals.items()},
               'changed_encounter_cases':sum(c['kind']=='changed' for c in cases),
               'remaining_interruption_candidates':sum(c['kind']=='still_split' for c in cases),
               'cases_losing_known_labels':sum(bool(c['lost_known_labels']) for c in cases),
               'merges_with_differing_reference_sets':sum(c['differing_reference_sets'] for c in cases),
               'limitations':['Existing labels are positive-only references, not exhaustive species lists.',
                              'Different reference sets in a merged encounter require review, not automatic rejection.',
                              'Same-species grouping cannot establish individual identity.',
                              'Remaining short interruptions are candidates for inspection, not verified algorithm errors.']}
    write_json(output/'comparison-summary.json',summary)
    write_json(output/'encounter-review.json',cases)
    template = Path(__file__).with_name('continuity_review.html').read_text()
    data = json.dumps({'summary':summary,'cases':cases}).replace('<','\\u003c')
    (output/'Review encounter grouping changes.html').write_text(template.replace('__COMPARISON_DATA__',data))
    print(json.dumps(summary,indent=2),flush=True)
    return summary,cases


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--db',type=Path,default=Path.home()/'.vireo/vireo.db')
    p.add_argument('--baseline-revision',default='HEAD')
    p.add_argument('--scope',action='append',required=True,help='Workspace ID, optionally followed by :YYYY-MM-DD; repeatable')
    p.add_argument('--split-registry', action='append', required=True,
                   help='Established registry as WORKSPACE_ID=PATH; repeat for each workspace')
    args=p.parse_args()
    scopes = [value.split(':',1) for value in args.scope]
    registries = {}
    for value in args.split_registry:
        workspace, separator, path = value.partition('=')
        if not separator or not path or int(workspace) in registries:
            p.error('Use one --split-registry WORKSPACE_ID=PATH per workspace')
        registries[int(workspace)] = Path(path)
    run(args.output,args.db,baseline_revision=args.baseline_revision,
        scopes=[(int(parts[0]),parts[1] if len(parts)>1 else None) for parts in scopes],
        split_registries=registries)


if __name__=='__main__':
    main()
