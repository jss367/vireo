"""Commands operate exclusively on frozen external evidence and annotation files."""
import argparse
import json
from pathlib import Path

from .curate import curate
from .evaluate import evaluate, freeze_profile
from .manifest import consensus, read_bundle, read_reviews, write_json
from .review import create_review_app


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    collect = commands.add_parser('curate', help='Read explicit original/detection pairs into a new evidence bundle')
    collect.add_argument('--db', type=Path, required=True)
    collect.add_argument('--selection', type=Path, required=True)
    collect.add_argument('--output', type=Path, required=True)
    review = commands.add_parser('review', help='Serve independent blinded judgments at localhost only')
    review.add_argument('--bundle', type=Path, required=True)
    review.add_argument('--reviewer', required=True)
    review.add_argument('--partition', choices=['development', 'holdout', 'diagnostic'], default='development')
    review.add_argument('--profile', type=Path)
    review.add_argument('--port', type=int, default=8097)
    status = commands.add_parser('status', help='Verify frozen pixels and report available independent judgments')
    status.add_argument('--bundle', type=Path, required=True)
    freeze = commands.add_parser('freeze', help='Freeze thresholds/model/code before accessing held-out judgments')
    freeze.add_argument('--bundle', type=Path, required=True)
    freeze.add_argument('--model', type=Path, required=True)
    freeze.add_argument('--producer-code', type=Path, required=True)
    freeze.add_argument('--focus-min', type=float, required=True)
    freeze.add_argument('--animal-min', type=float, required=True)
    freeze.add_argument('--domain', type=Path, required=True, help='JSON camera/source/recipe domain, not source IDs')
    freeze.add_argument('--maximum-false-keep-rate', type=float, required=True)
    freeze.add_argument('--confidence', type=float, required=True)
    freeze.add_argument('--minimum-positive-groups', type=int, required=True)
    freeze.add_argument('--output', type=Path, required=True)
    run = commands.add_parser('evaluate', help='Report held-out error bounds without runtime authorization')
    run.add_argument('--bundle', type=Path, required=True)
    run.add_argument('--profile', type=Path, required=True)
    run.add_argument('--predictions', type=Path, required=True)
    run.add_argument('--output', type=Path, required=True)
    features = commands.add_parser('extract', help='Bounded native-detail/DINOv2 feature pilot; not a classifier')
    features.add_argument('--bundle', type=Path, required=True)
    features.add_argument('--model-directory', type=Path, required=True)
    features.add_argument('--output', type=Path, required=True)
    features.add_argument('--limit', type=int, default=20)
    features.add_argument('--threads', type=int, default=2)
    train = commands.add_parser('fit', help='Fit an uncalibrated two-gate candidate from independent development labels')
    train.add_argument('--bundle', type=Path, required=True)
    train.add_argument('--features', type=Path, required=True)
    train.add_argument('--domain', type=Path, required=True)
    train.add_argument('--output', type=Path, required=True)
    score = commands.add_parser('score', help='Score frozen candidate features; output alone cannot authorize KEEP')
    score.add_argument('--bundle', type=Path, required=True)
    score.add_argument('--features', type=Path, required=True)
    score.add_argument('--model-directory', type=Path, required=True)
    score.add_argument('--profile', type=Path, required=True)
    score.add_argument('--output', type=Path, required=True)
    development = commands.add_parser('development-scores', help='Inspect development scores before freezing thresholds')
    development.add_argument('--bundle', type=Path, required=True)
    development.add_argument('--features', type=Path, required=True)
    development.add_argument('--model-directory', type=Path, required=True)
    development.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == 'curate':
        result = curate(args.db, json.loads(args.selection.read_text()), args.output)
        print(f'Curated {len(result["samples"])} unlabeled native-region samples; no runtime approval')
    elif args.command == 'review':
        app = create_review_app(args.bundle, args.reviewer, partition=args.partition, profile_path=args.profile)
        app.run(host='127.0.0.1', port=args.port, threaded=False, debug=False)
    elif args.command == 'status':
        manifest = read_bundle(args.bundle)
        reviews = read_reviews(args.bundle, manifest)
        labels = consensus(manifest, reviews)
        print(json.dumps({'samples': len(labels), 'reviewers': len(reviews),
                          'review_protocol': manifest['review_protocol'],
                          'meeting_human_review_protocol': sum(v['human_judgments'] >= manifest['review_protocol']['minimum_human_reviewers'] for v in labels.values()),
                          'runtime_approved': False}, indent=2))
    elif args.command == 'extract':
        from .features import extract
        result = extract(args.bundle, args.model_directory, args.output, limit=args.limit, threads=args.threads)
        print(f'Measured {len(result["samples"])} regions in {result["elapsed_seconds"]:.1f}s; no classifier or runtime approval')
    elif args.command == 'development-scores':
        from .candidate import development_scores
        development_scores(args.bundle, args.features, args.model_directory, args.output)
        print('Development scores only; held-out scores/labels were not exposed')
    elif args.command == 'fit':
        from .candidate import fit
        fit(args.bundle, args.features, json.loads(args.domain.read_text()), args.output)
        print('Uncalibrated candidate fitted on development judgments only; no runtime approval')
    elif args.command == 'score':
        from .candidate import score
        score(args.bundle, args.features, args.model_directory, args.profile, args.output)
        print('Frozen candidate scores recorded; no runtime approval')
    elif args.command == 'freeze':
        freeze_profile(args.bundle, args.model, args.producer_code, focus_min=args.focus_min,
                       animal_min=args.animal_min, domain=json.loads(args.domain.read_text()),
                       evaluation_plan={'maximum_false_keep_rate': args.maximum_false_keep_rate, 'confidence': args.confidence,
                                        'minimum_positive_groups': args.minimum_positive_groups}, output=args.output)
        print('Candidate frozen. This is not calibration or runtime approval.')
    else:
        if args.output.exists() or args.output.resolve().is_relative_to(args.bundle.resolve()):
            raise ValueError('Write a new evaluation report outside the frozen evidence bundle')
        result = evaluate(args.bundle, args.profile, json.loads(args.predictions.read_text()))
        write_json(args.output, result)
        print(f'Risk requirement met: {result["risk_requirement_met"]}; runtime approved: False')


if __name__ == '__main__':
    main()
