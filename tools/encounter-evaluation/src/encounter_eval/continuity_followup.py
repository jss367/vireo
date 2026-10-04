"""Diagnose and compare bounded continuity changes on training/development only."""

import argparse
import json
from collections import Counter
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from .algorithms import run_algorithm
from .common import code_identity, configure_repo, digest, write_json
from .continuity_compare import comparison_cases
from .continuity_experiments import apply_candidate, candidates, prepare_baseline
from .grouping_dataset import boundary_errors
from .label_scoring import eligible, measure, metrics
from .library import read_bundle, timestamp


def pair_references(photos, answers):
    """Partial-label proxies, never individual-animal identity annotations."""
    result = []
    for a, b in zip(photos, photos[1:], strict=False):
        aa, bb = answers.get(str(a["id"])), answers.get(str(b["id"]))
        if not aa or not bb or len(aa["taxa"]) != 1 or len(bb["taxa"]) != 1:
            continue
        ta, tb = timestamp(a.get("timestamp")), timestamp(b.get("timestamp"))
        if ta is None or tb is None or a.get("folder_id") is None or a.get("folder_id") != b.get("folder_id"):
            continue
        gap = (tb - ta).total_seconds()
        if not 0 <= gap <= 60:
            continue
        same = aa["taxa"] == bb["taxa"]
        if same and gap > 10:
            continue
        result.append(
            {
                "ids": [a["id"], b["id"]],
                "gap_seconds": gap,
                "same_labels": same,
                "left_labels": aa["taxa"],
                "right_labels": bb["taxa"],
                "provenance": "inferred_from_existing_tags",
                "human_reviewed": False,
            }
        )
    return result


def _membership(groups):
    return {pid: i for i, g in enumerate(groups) for pid in g.photo_ids}


def comparison_counts(photos, answers, groups, baseline, references):
    counts = Counter(measure(photos, answers, groups))
    old, new = _membership(baseline), _membership(groups)
    for pid, answer in answers.items():
        before = set(baseline[old[int(pid)]].roster or ())
        after = set(groups[new[int(pid)]].roster or ())
        counts["lost_previously_recovered_labels"] += len((before - after) & set(answer["taxa"]))
    for r in references:
        a, b = r["ids"]
        joined = new[a] == new[b]
        if not r["same_labels"]:
            counts["different_label_control_pairs"] += 1
            counts["different_label_control_joins"] += joined
            counts["new_differing_label_joins"] += joined and old[a] != old[b]
        elif r["gap_seconds"] > 3:
            counts["longer_same_label_pairs"] += 1
            counts["longer_same_label_splits"] += not joined
    return counts


def summarize(counts):
    result = metrics(counts)
    c = Counter(counts)
    rate = c["longer_same_label_splits"] / c["longer_same_label_pairs"] if c["longer_same_label_pairs"] else 0
    if result["objective"] is not None:
        result["objective"] += 0.1 * rate
    result["objective_scope"] += " Plus 0.1 × same-label split rate at 3–10 seconds."
    return result


def acceptable(result, baseline):
    counts = Counter(result["metrics"]["counts"])
    baseline_counts = Counter(baseline["metrics"]["counts"])
    return (
        eligible(result["metrics"], baseline["metrics"])
        and not counts["lost_previously_recovered_labels"]
        and not counts["new_differing_label_joins"]
        # Known-wrong additions are counted on completely labeled photos; a
        # candidate that adds more of them is never a win however much recall
        # it gains elsewhere.
        and counts["incorrect_additions"] <= baseline_counts["incorrect_additions"]
        and all(c["passed"] for c in result["reviewed_cases"])
    )


def _changed_cases(photos, answers, before, after, context, reviewed_joins):
    old, new = _membership(before), _membership(after)
    references = {tuple(r["ids"]): r for r in pair_references(photos, answers)}
    result = []
    for case in comparison_cases(photos, answers, before, after):
        if case["kind"] != "changed":
            continue
        boundaries = []
        for a, b in zip(case["ids"], case["ids"][1:], strict=False):
            if (old[a] == old[b]) == (new[a] == new[b]):
                continue
            r = references.get((a, b))
            status = (
                "existing_labels"
                if r and r["same_labels"] and r["gap_seconds"] <= 10
                else "previous_review"
                if (a, b) in reviewed_joins
                else "differing_labels"
                if r and not r["same_labels"]
                else "ambiguous"
            )
            boundaries.append({"ids": [a, b], "now_joined": new[a] == new[b], "support": status})
        ambiguity = any(x["support"] in {"ambiguous", "differing_labels"} or not x["now_joined"] for x in boundaries)
        # Roster-only changes with no known-label recovery also need checking.
        ambiguity |= not boundaries and not case["recovered_known_labels"]
        result.append(
            {
                **context,
                "id": digest([context, case["ids"]])[:24],
                "ids": case["ids"],
                "before": [asdict(g) for g in case["before"]],
                "after": [asdict(g) for g in case["after"]],
                "lost_known_labels": case["lost_known_labels"],
                "recovered_known_labels": case["recovered_known_labels"],
                "boundaries": boundaries,
                "needs_review": bool(ambiguity or case["lost_known_labels"]),
                "human_reviewed": False,
            }
        )
    return result


def run(scopes, output, *, constraints=()):
    repo = configure_repo()
    constraints = list(constraints)
    if len({c["id"] for c in constraints}) != len(constraints):
        raise ValueError("Duplicate reviewed case IDs")
    if any(c.get("partition") not in {None, "train", "development"} for c in constraints):
        raise ValueError("Reviewed constraints must not use final-test sessions")
    scopes = [(Path(p).resolve(), json.loads((Path(p) / "manifest.json").read_text())) for p in scopes]
    if not {"train", "development"} <= {e["partition"] for _, m in scopes for e in m["sessions"]}:
        raise ValueError("Training and development sessions are required")
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    specs = candidates()
    design = {
        "created_at": datetime.now(UTC).isoformat(),
        "source": code_identity(repo),
        "candidates": specs,
        "scopes": [{"path": str(p), "manifest_digest": digest(m)} for p, m in scopes],
        "constraints": constraints,
        "selection": "Lowest training objective among eligible candidates per family; choose among those using development; no adaptive second search.",
        "eligibility": "No lost previously recovered reference species, no newly joined differing-label controls within 60 seconds, no increase in known incorrect additions on completely labeled photos, all reviewed boundaries preserved, and bounded unverified additions.",
        "test_policy": "Never load former final-test bundles; reserve capture days after 2026-10-03 for a future independent final test.",
        "reference_policy": "Same singleton tags within 10 seconds imply a join proxy; different partial tags are conservative controls, not proven absence; inferred checks are not human approvals.",
    }
    write_json(output / "search-design.json", design)
    ids_seen, hashes_seen = {}, {}
    references_all = []
    reviewed_joins = {
        (c["workspace"], c["session"], a, b)
        for c in constraints
        for group in c["expected_groups"]
        for a, b in zip(group, group[1:], strict=False)
    }

    def evaluate(partition, choices):
        totals = {s["id"]: Counter() for s in choices}
        checks = {s["id"]: [] for s in choices}
        changed = {s["id"]: [] for s in choices}
        sessions = 0
        for scope, manifest in scopes:
            for entry in manifest["sessions"]:
                if entry["partition"] != partition:
                    continue
                bundle = read_bundle(scope, entry)
                photos, answers = bundle["photos"], bundle["answers"]
                library = manifest.get("source_library", "legacy-source")
                keys = {(library, p["id"]) for p in photos}
                prior = keys & ids_seen.keys()
                if prior:
                    if any(ids_seen[k] != partition for k in prior):
                        raise ValueError("A photo crosses partitions")
                    if prior == keys:
                        continue
                    raise ValueError("Partially overlapping sessions")
                for key in keys:
                    ids_seen[key] = partition
                    file_hash = bundle["presentation"][str(key[1])].get("file_hash")
                    if file_hash and hashes_seen.setdefault(file_hash, partition) != partition:
                        raise ValueError("Duplicate file hashes cross partitions")
                context = {
                    "workspace": manifest["workspace"],
                    "session": entry["id"],
                    "partition": partition,
                    "scope": str(scope),
                    "input_digest": entry["digest"],
                }
                references = pair_references(photos, answers)
                references_all.extend({**context, **r} for r in references)
                prepared = prepare_baseline(photos, manifest["config"])
                baseline = run_algorithm("production", prepared[0], grouping_config=manifest["grouping_config"])
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
                joins = {(a, b) for w, s, a, b in reviewed_joins if (w, s) == (manifest["workspace"], entry["id"])}
                for spec in choices:
                    adjusted = apply_candidate(prepared, manifest["config"], spec)
                    groups = (
                        baseline
                        if spec["id"] == "current"
                        else run_algorithm(
                            "production",
                            adjusted,
                            grouping_config=manifest["grouping_config"],
                        )
                    )
                    totals[spec["id"]].update(comparison_counts(adjusted, answers, groups, baseline, references))
                    for c in matching:
                        errors = boundary_errors(c["ids"], c["expected_groups"], [list(g.photo_ids) for g in groups])
                        checks[spec["id"]].append(
                            {
                                "id": c["id"],
                                **errors,
                                "passed": not (errors.get("incorrect_merges") or errors.get("unnecessary_splits")),
                            }
                        )
                    if spec["id"] != "current":
                        changed[spec["id"]].extend(_changed_cases(adjusted, answers, baseline, groups, context, joins))
                sessions += 1
                if sessions % 10 == 0:
                    print(f"{partition}: {sessions} sessions, {totals['current']['photos']:,} photos", flush=True)
        result = {
            s["id"]: {
                "metrics": summarize(totals[s["id"]]),
                "reviewed_cases": checks[s["id"]],
                "changed_cases": changed[s["id"]],
                "sessions": sessions,
            }
            for s in choices
        }
        write_json(output / f"{partition}-results.json", result)
        return result

    train = evaluate("train", specs)
    if train["current"]["metrics"]["objective"] is None:
        raise ValueError("No labeled training data")
    finalists = []
    for family in sorted({s["family"] for s in specs} - {"current"}):
        pool = [
            s
            for s in specs
            if s["family"] == family
            and acceptable(train[s["id"]], train["current"])
            and train[s["id"]]["metrics"]["objective"] < train["current"]["metrics"]["objective"]
        ]
        if pool:
            finalists.append(min(pool, key=lambda s: (train[s["id"]]["metrics"]["objective"], s["id"])))
    write_json(output / "training-selection.json", {"finalists": finalists})
    print("Training finalists:", [s["id"] for s in finalists], flush=True)
    development = evaluate("development", [specs[0], *finalists])
    baseline_checks = train["current"]["reviewed_cases"] + development["current"]["reviewed_cases"]
    if {c["id"] for c in baseline_checks} != {c["id"] for c in constraints}:
        raise ValueError("Not every reviewed constraint was evaluated")
    # Falling back to the current grouping only preserves reviewed boundaries
    # if the current grouping itself still honors them; otherwise freezing it
    # would record a selection whose reviewed_cases contain passed: false.
    if not all(c["passed"] for c in baseline_checks):
        raise ValueError("A reviewed boundary no longer passes on the current grouping")
    valid = [
        s
        for s in finalists
        if acceptable(development[s["id"]], development["current"])
        and development[s["id"]]["metrics"]["objective"] < development["current"]["metrics"]["objective"]
    ]
    winner = min(valid, key=lambda s: (development[s["id"]]["metrics"]["objective"], s["id"])) if valid else specs[0]
    selected = winner["id"]
    cases = train[selected]["changed_cases"] + development[selected]["changed_cases"]
    summary = {
        "selected": winner,
        "status": "provisional-awaiting-fresh-test" if selected != "current" else "keep-current",
        "candidate_count": len(specs),
        "test_evaluated": False,
        "finalists": finalists,
        "baseline": {"train": train["current"]["metrics"], "development": development["current"]["metrics"]},
        "selected_metrics": {"train": train[selected]["metrics"], "development": development[selected]["metrics"]},
        "reviewed_cases": train[selected]["reviewed_cases"] + development[selected]["reviewed_cases"],
        "changed_cases": len(cases),
        "ambiguous_cases": sum(c["needs_review"] for c in cases),
    }
    write_json(
        output / "frozen-selection.json",
        {"frozen_at": datetime.now(UTC).isoformat(), "candidate": winner, "test_outcomes_seen": False},
    )
    write_json(output / "inferred-regression-checks.json", {"human_reviews": False, "references": references_all})
    write_json(output / "changed-cases.json", {"candidate": winner, "cases": cases})
    write_json(output / "summary.json", summary)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scope", type=Path, action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--constraints", type=Path)
    args = parser.parse_args()
    result = run(
        args.scope, args.output, constraints=json.loads(args.constraints.read_text()) if args.constraints else ()
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
