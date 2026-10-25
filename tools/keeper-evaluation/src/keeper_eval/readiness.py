"""Describe missing development evidence without fitting or exposing held-out answers."""

from .candidate import MINIMUM_CLASS_GROUPS, development_rows, read_features
from .evaluate import in_domain, validate_domain
from .manifest import consensus, read_bundle, read_reviews
from .region_target import region_contract


def training_readiness(bundle, feature_path, domain):
    manifest = read_bundle(bundle)
    validate_domain(domain, manifest['pixel_recipe'])
    region_contract(manifest)
    features, by_id = read_features(feature_path, manifest)
    reviews = read_reviews(bundle, manifest)
    labels = consensus(manifest, reviews)
    animal_rows, detail_rows, unavailable = development_rows(manifest, reviews, by_id, domain)
    development = [s for s in manifest['samples'] if s['partition'] == 'development']
    included = [s for s in development if in_domain(s, {'domain': domain})]
    minimum_reviewers = manifest['review_protocol']['minimum_human_reviewers']
    gates, blockers = {}, []
    for gate, rows, names in (
        ('animal', animal_rows, ('not_animal', 'animal')),
        ('focus', detail_rows, ('severe_defocus', 'sufficient_detail')),
    ):
        classes = {}
        for value, name in enumerate(names):
            matching = [row for row in rows if row[2] == value]
            groups = len({row[1] for row in matching})
            missing = max(0, MINIMUM_CLASS_GROUPS - groups)
            classes[name] = {'samples': len(matching), 'groups': groups, 'additional_groups_needed': missing}
            if missing:
                blockers.append(f'{gate}/{name}: {groups}/{MINIMUM_CLASS_GROUPS} development encounter groups; '
                                f'{missing} more independently judged groups with usable features needed')
        gates[gate] = {'classes': classes}
    return {
        'schema': 'vireo-keeper-training-readiness-v1',
        'manifest_sha256': manifest['manifest_sha256'],
        'features_sha256': features['features_sha256'],
        'domain': domain, 'review_protocol': manifest['review_protocol'],
        'development_samples': len(included),
        'out_of_domain_development_samples': len(development) - len(included),
        'pending_human_review_samples': [s['sample_id'] for s in included
                                         if labels[s['sample_id']]['human_judgments'] < minimum_reviewers],
        'uncertain_animal_samples': [s['sample_id'] for s in included
                                     if labels[s['sample_id']]['human_judgments'] >= minimum_reviewers
                                     and labels[s['sample_id']]['animal'] == 'uncertain'],
        'uncertain_focus_samples': [s['sample_id'] for s in included
                                    if labels[s['sample_id']]['animal'] == 'animal'
                                    and labels[s['sample_id']]['focus'] not in {'sufficient_detail', 'severe_defocus'}],
        'unavailable_development_detail': unavailable,
        'minimum_groups_per_class': MINIMUM_CLASS_GROUPS,
        'gates': gates, 'blocking_reasons': blockers,
        'class_coverage_met': not blockers,
        'next_step': ('Fit the experimental candidate, then inspect development outcomes before freezing.' if not blockers else
                      'Collect independent development judgments; resolve uncertain evidence or curate new development encounters '
                      'for the missing classes. Keep diagnostic and held-out encounters excluded.'),
        'calibrated': False, 'runtime_approved': False,
        'limitations': 'Class coverage is a fitting prerequisite, not validation accuracy or proof that numerical fitting will converge. '
                       'This report does not reveal held-out judgments, select thresholds or authorize automatic keeps.',
    }
