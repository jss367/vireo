"""Tune grouping against existing labels in retained, day-partitioned sessions."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

from .algorithms import run_algorithm
from .common import code_identity, configure_repo, digest, write_json
from .continuity import apply_continuity
from .grouping_dataset import boundary_errors
from .label_scoring import eligible, measure, metrics
from .library import read_bundle
from .runner import parameter_trials


def candidates(seed=42, trials=16):
    specs = [{"id": "current", "algorithm": "production", "params": {}}]
    space = {
        "species_hard_cut_confidence": [0.65, 0.8, 0.95, 0.99],
        "hard_cut_score": [0.35, 0.42],
        "merge_score": [0.55, 0.62, 0.70],
    }
    for i, params in enumerate(parameter_trials(space, "random", trials, seed)):
        specs.append({"id": f"production-{i + 1}", "algorithm": "production", "params": params})
    for confidence in (0.4, 0.55, 0.7, 0.85):
        for transition in (0.2, 0.5, 0.9):
            specs.append(
                {
                    "id": f"sequence-{confidence}-{transition}",
                    "algorithm": "sequence",
                    "params": {
                        "confidence": confidence,
                        "margin": 0.2,
                        "transition_penalty": transition,
                        "context_frames": 4,
                        "context_seconds": 3.0,
                    },
                }
            )
    for confidence in (0.4, 0.65, 0.85):
        specs.append(
            {
                "id": f"independent-{confidence}",
                "algorithm": "independent",
                "params": {"confidence": confidence, "margin": 0.2},
            }
        )
    for isolated in (False, True):
        specs.append(
            {"id": f"continuity-{isolated}", "algorithm": "continuity", "params": {"repair_isolated": isolated}}
        )
    return specs


def predict(spec, photos, manifest):
    if spec["algorithm"] == "continuity":
        photos = apply_continuity(photos, manifest["config"], **spec["params"])
        return run_algorithm("production", photos, grouping_config=manifest["grouping_config"])
    return run_algorithm(spec["algorithm"], photos, spec["params"], manifest["grouping_config"])


def training_finalists(specs, results):
    """Compare the best of each family, even if its safety checks fail."""
    selected = []
    for family in ("production", "sequence", "independent", "continuity"):
        pool = [
            s
            for s in specs
            if s["id"] != "current"
            and s["algorithm"] == family
            and results[s["id"]]["metrics"]["objective"] is not None
        ]
        pool.sort(key=lambda s: (results[s["id"]]["metrics"]["objective"], s["id"]))
        if pool:
            best = results[pool[0]["id"]]["metrics"]["objective"]
            selected.extend([s for s in pool if results[s["id"]]["metrics"]["objective"] == best][:2])
    return selected


def run(scopes, output, *, constraints=(), evaluate_test=False, seed=42, trials=16):
    repo = configure_repo()
    scopes = [(Path(p).resolve(), json.loads((Path(p) / "manifest.json").read_text())) for p in scopes]
    constraints = list(constraints)
    if any(c.get("partition") not in {None, "train", "development"} for c in constraints):
        raise ValueError("Reviewed boundary constraints must not use final-test sessions")
    available = {e["partition"] for _, m in scopes for e in m["sessions"]}
    if not {"train", "development"} <= available or (evaluate_test and "test" not in available):
        raise ValueError("The requested training, development, and optional test partitions must be retained")
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    specs = candidates(seed, trials)
    design = {
        "created_at": datetime.now(UTC).isoformat(),
        "code": code_identity(repo),
        "candidates": specs,
        "scopes": [{"path": str(p), "manifest_digest": digest(m)} for p, m in scopes],
        "constraints": constraints,
        "seed": seed,
        "production_trials": trials,
        "objective": "Missing positive-label rate + 0.25 * short same-label split rate + different-label join rate",
        "eligibility": "No reduction in recovered positives or increase in short different-label joins; extra unverified additions cannot exceed recovered-positive gain; all supplied reviewed boundaries pass.",
        "reference_policy": "Adjacent same-folder frames within three seconds with identical singleton tags imply continuity. Differing partial tags are a conservative boundary proxy, not proven absence.",
        "test_policy": "Freeze one development-selected candidate before evaluating test; never tune on test results.",
    }
    write_json(output / "search-design.json", design)
    photo_partitions, hash_partitions = {}, {}

    def evaluate(partition, choices):
        totals = {s["id"]: Counter() for s in choices}
        checks = {s["id"]: [] for s in choices}
        details = {s["id"]: [] for s in choices}
        seen = set()
        session_count = 0
        for scope, manifest in scopes:
            for entry in manifest["sessions"]:
                if entry["partition"] != partition:
                    continue
                bundle = read_bundle(scope, entry)
                photos = bundle["photos"]
                library = manifest.get("source_library", "legacy-source")
                ids = {(library, p["id"]) for p in photos}
                if ids <= seen:
                    continue
                if ids & seen:
                    raise ValueError("Partially overlapping scope sessions")
                seen.update(ids)
                for key in ids:
                    if photo_partitions.setdefault(key, partition) != partition:
                        raise ValueError("A photo crosses partitions")
                    file_hash = bundle["presentation"][str(key[1])].get("file_hash")
                    if file_hash and hash_partitions.setdefault(file_hash, partition) != partition:
                        raise ValueError("Duplicate file hashes cross partitions")
                matching = [
                    c for c in constraints if c["workspace"] == manifest["workspace"] and c["session"] == entry["id"]
                ]
                ordered = [p["id"] for p in photos]
                for c in matching:
                    start = ordered.index(c["ids"][0])
                    if ordered[start : start + len(c["ids"])] != c["ids"]:
                        raise ValueError("Reviewed photo order changed")
                    for photo in c.get("photos", []):
                        if bundle["presentation"][str(photo["id"])]["filename"] != photo["filename"]:
                            raise ValueError("Reviewed photo identity changed")
                for spec in choices:
                    groups = predict(spec, photos, manifest)
                    counts = measure(photos, bundle["answers"], groups)
                    totals[spec["id"]].update(counts)
                    details[spec["id"]].append(
                        {"workspace": manifest["workspace"], "session": entry["id"], "metrics": metrics(counts)}
                    )
                    for c in matching:
                        errors = boundary_errors(c["ids"], c["expected_groups"], [list(g.photo_ids) for g in groups])
                        checks[spec["id"]].append(
                            {
                                "case_id": c["id"],
                                **errors,
                                "passed": not (errors.get("incorrect_merges") or errors.get("unnecessary_splits")),
                            }
                        )
                session_count += 1
                if session_count % 10 == 0:
                    print(f"{partition}: {session_count} sessions, {len(seen):,} photos", flush=True)
        results = {
            sid: {"metrics": metrics(c), "sessions": details[sid], "reviewed_cases": checks[sid]}
            for sid, c in totals.items()
        }
        write_json(output / f"{partition}-results.json", results)
        return results

    train = evaluate("train", specs)
    if train["current"]["metrics"]["objective"] is None:
        raise ValueError("No labeled training photos")
    finalists = training_finalists(specs, train)
    development = evaluate("development", [specs[0], *finalists])
    valid = []
    for spec in finalists:
        sid = spec["id"]
        reviewed = train[sid]["reviewed_cases"] + development[sid]["reviewed_cases"]
        if (
            eligible(train[sid]["metrics"], train["current"]["metrics"])
            and eligible(development[sid]["metrics"], development["current"]["metrics"])
            and len(reviewed) == len(constraints)
            and all(c["passed"] for c in reviewed)
        ):
            valid.append(spec)
    valid.sort(key=lambda s: (development[s["id"]]["metrics"]["objective"], s["id"]))
    winner = (
        valid[0]
        if valid
        and development[valid[0]["id"]]["metrics"]["objective"] < development["current"]["metrics"]["objective"]
        else specs[0]
    )
    frozen = {
        "frozen_at": datetime.now(UTC).isoformat(),
        "candidate": winner,
        "eligible": [s["id"] for s in valid],
        "test_outcomes_seen": False,
    }
    write_json(output / "frozen-selection.json", frozen)
    result = {
        "selection": frozen,
        "training_finalists": [s["id"] for s in finalists],
        "test_evaluated": evaluate_test,
        "candidate_count": len(specs),
    }
    if evaluate_test:
        test = evaluate("test", [specs[0]] + ([winner] if winner["id"] != "current" else []))
        result["test_baseline"] = test["current"]["metrics"]
        result["test_selected"] = test[winner["id"]]["metrics"]
        result["test_passes_constraints"] = eligible(result["test_selected"], result["test_baseline"])
    write_json(output / "summary.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scope", type=Path, action="append", required=True, help="Retained run with manifest.json")
    parser.add_argument("--output", type=Path, required=True, help="New private output directory")
    parser.add_argument("--constraints", type=Path, help="JSON list of explicit reviewed expected_groups")
    parser.add_argument(
        "--evaluate-test", action="store_true", help="Evaluate the frozen candidate once on final-test sessions"
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--production-trials", type=int, default=16)
    args = parser.parse_args()
    result = run(
        args.scope,
        args.output,
        constraints=json.loads(args.constraints.read_text()) if args.constraints else (),
        evaluate_test=args.evaluate_test,
        seed=args.seed,
        trials=args.production_trials,
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
