"""Audit encounters that join adjacent photos carrying different species tags.

Every earlier continuity experiment held these joins fixed ("join no new
differing-label pairs") without asking whether the existing ones are right.
``build`` lists each one under the installed default, with the evidence that
kept the pair together, as a review page. ``results`` turns the exported
decisions into a wrong-merge estimate and explicit two-photo boundary
constraints in the established ``expected_groups`` format.
"""

import argparse
import json
import math
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path

from .algorithms import run_algorithm
from .common import code_identity, configure_repo, digest, restore_features, write_json
from .continuity import apply_continuity
from .continuity_compare import _preview
from .continuity_followup import pair_references
from .library import read_bundle

DECISIONS = {
    "keep": "One encounter: keep together",
    "tag_error": "Same subject: a species tag is wrong",
    "split": "Different encounters: split here",
    "unsure": "Unsure",
}
# A wrong tag still means the photos belong together; unsure records no answer.
JOINED = {"keep", "tag_error"}

REASONS = {
    "no_confident_species": "Neither photo has a confident species prediction, so the species cut could not fire.",
    "one_confident_species": "Only one photo has a confident species prediction, so the species cut could not fire.",
    "same_confident_species": "Both photos confidently show the same species, so the tags disagree with the classifiers.",
    "different_confident_species": "Both photos confidently show different species, yet they were joined.",
    "merged_back": "The first pass cut between them; the merge pass rejoined the two segments.",
    "burst_id": "The camera recorded both frames in one burst.",
}


def _gap_bucket(seconds):
    return "at most 3 seconds" if seconds <= 3 else "3–10 seconds" if seconds <= 10 else "10–60 seconds"


def _top_by_model(photo, display, per_model=2):
    """Each model's leading species; a runner-up explains a missing confident winner."""
    from encounters import grouping_species_predictions
    from species_identity import species_entry_key

    scores = defaultdict(dict)
    for entry in grouping_species_predictions(photo) or []:
        if len(entry) < 2:
            continue
        model = str(entry[2]) if len(entry) > 2 and entry[2] else "unknown"
        key = photo.get("species_keys", {}).get(species_entry_key(entry))
        name = display.get(key, entry[0]) if key else entry[0]
        scores[model][name] = max(scores[model].get(name, 0.0), float(entry[1]))
    return [
        {"model": model, "species": name, "confidence": round(score, 3)}
        for model in sorted(scores)
        for name, score in sorted(scores[model].items(), key=lambda item: (-item[1], item[0]))[:per_model]
    ]


def _confident(photo, grouping_config, display):
    from encounters import _confident_species_prediction

    winner = _confident_species_prediction(photo, grouping_config)
    if winner is None:
        return None
    key = photo.get("species_keys", {}).get(winner["key"])
    return {
        "species": display.get(key, winner["species"]) if key else winner["species"],
        "confidence": round(winner["confidence"], 3),
        "margin": round(winner["margin"], 3),
        "models": winner["model_count"],
    }


def join_reason(trace, left, right):
    """Why the default grouping kept a differing-label pair in one encounter."""
    if trace["decision"] == "merged_back":
        return "merged_back"
    if trace["decision"] == "burst_id_kept":
        return "burst_id"
    if left and right:
        return "same_confident_species" if left["species"] == right["species"] else "different_confident_species"
    return "one_confident_species" if left or right else "no_confident_species"


def _encounter_traces(photos, grouping_config):
    """Pair traces keyed by adjacent photo IDs inside each encounter."""
    from encounters import segment_encounters

    traces = {}
    for encounter in segment_encounters(photos, config=grouping_config, emit_trace=True):
        ids = [p["id"] for p in encounter["photos"]]
        for pair, trace in zip(zip(ids, ids[1:], strict=False), encounter["trace"], strict=True):
            traces[pair] = trace
    return traces


def pending_preview(pid, size=1920):
    return (Path.home() / ".vireo" / "previews" / f"{pid}_{size}.jpg").as_uri()


def _order_key(seed, case_id):
    # Seeded random order: a review stopped partway is still a random sample.
    return digest([seed, case_id])


def build(scopes, output, *, context=4, seed=42):
    repo = configure_repo()
    scopes = [(Path(p).resolve(), json.loads((Path(p) / "manifest.json").read_text())) for p in scopes]
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    cases, seen, totals, missing = [], {}, Counter(), set()
    for scope, manifest in scopes:
        display = manifest.get("taxonomy_display", {})
        library = manifest.get("source_library", "legacy-source")
        for entry in manifest["sessions"]:
            # Former final-test bundles are never opened.
            if entry["partition"] not in {"train", "development"}:
                continue
            bundle = read_bundle(scope, entry)
            raw, answers = bundle["photos"], bundle["answers"]
            keys = {(library, p["id"]) for p in raw}
            prior = keys & seen.keys()
            if prior:
                if prior == keys and all(seen[k] == entry["digest"] for k in prior):
                    continue
                raise ValueError("Scopes overlap partially or disagree on a session's inputs")
            seen.update(dict.fromkeys(keys, entry["digest"]))
            photos = restore_features(apply_continuity(raw, manifest["config"]))
            by_id = {p["id"]: p for p in photos}
            groups = run_algorithm("production", photos, grouping_config=manifest["grouping_config"])
            membership = {pid: i for i, g in enumerate(groups) for pid in g.photo_ids}
            traces = _encounter_traces(photos, manifest["grouping_config"])
            totals["sessions"] += 1
            totals["photos"] += len(photos)
            for reference in pair_references(raw, answers):
                if reference["same_labels"]:
                    continue
                a, b = reference["ids"]
                totals["differing_label_pairs"] += 1
                if membership[a] != membership[b]:
                    continue
                totals["joined_pairs"] += 1
                group = groups[membership[a]].photo_ids
                start, end = group.index(a), group.index(b)
                window = list(group[max(0, start - context) : end + 1 + context])
                confident = [_confident(by_id[pid], manifest["grouping_config"], display) for pid in (a, b)]
                trace = traces[(a, b)]
                reason = join_reason(trace, *confident)
                case_id = digest([manifest["workspace"], entry["id"], entry["digest"], a, b])[:20]
                case = {
                    "id": case_id,
                    "workspace": manifest["workspace"],
                    "session": entry["id"],
                    "partition": entry["partition"],
                    "scope": str(scope),
                    "input_digest": entry["digest"],
                    "date": by_id[a]["timestamp"][:10],
                    "pair": [a, b],
                    "gap_seconds": round(reference["gap_seconds"], 3),
                    "gap_bucket": _gap_bucket(reference["gap_seconds"]),
                    "labels": [
                        [display.get(k, k) for k in reference[side]] for side in ("left_labels", "right_labels")
                    ],
                    "encounter": {
                        "photos": len(group),
                        "species": ", ".join(display.get(k, k) for k in groups[membership[a]].roster or ())
                        or "No species suggestion",
                        "window": [max(0, start - context) + 1, min(len(group), end + 1 + context)],
                    },
                    "reason": reason,
                    "reason_text": REASONS[reason],
                    "trace": {
                        "decision": trace["decision"],
                        "score": round(trace["score"], 3),
                        "hard_cut_score": trace["thresholds"]["hard_cut_score"],
                        "species_confidence": trace["thresholds"]["species_hard_cut_confidence"],
                        "species_margin": trace["thresholds"]["species_hard_cut_margin"],
                        "components": {
                            name: round(value["value"], 3)
                            for name, value in (trace.get("components") or {}).items()
                            if value.get("used")
                        },
                    },
                    "photos": [],
                }
                for pid in window:
                    p, presentation = by_id[pid], bundle["presentation"][str(pid)]
                    side = 0 if pid == a else 1 if pid == b else None
                    preview = _preview(pid, presentation)
                    if preview is None:
                        missing.add(pid)
                    case["photos"].append(
                        {
                            "id": pid,
                            "filename": presentation["filename"],
                            "timestamp": p["timestamp"],
                            # Where Vireo's preview job writes, so the page picks it up on reload.
                            "preview": preview or pending_preview(pid),
                            "preview_pending": preview is None,
                            "labels": [display.get(k, k) for k in answers.get(str(pid), {}).get("taxa", [])],
                            "pair_side": side,
                            "predictions": _top_by_model(p, display),
                            "confident": confident[side] if side is not None else None,
                            "subject_absent": bool(p.get("subject_absent")),
                            "continuity": (p.get("weak_detection_context") or {}).get("evidence"),
                        }
                    )
                cases.append(case)
        print(f"{scope.name}: {totals['sessions']} sessions, {len(cases)} joined pairs", flush=True)
    if not totals["sessions"]:
        raise ValueError("No training/development sessions in the supplied scopes")
    cases.sort(key=lambda c: _order_key(seed, c["id"]))
    summary = {
        "created_at": datetime.now(UTC).isoformat(),
        "source": code_identity(repo),
        "scopes": [{"path": str(p), "manifest_digest": digest(m)} for p, m in scopes],
        "seed": seed,
        "context_frames": context,
        "counts": dict(totals),
        "by_reason": dict(Counter(c["reason"] for c in cases)),
        "by_gap": dict(Counter(c["gap_bucket"] for c in cases)),
        "by_partition": dict(Counter(c["partition"] for c in cases)),
        "algorithm": "Installed default: apply_encounter_continuity, then production encounter segmentation.",
        "reference_policy": "Adjacent same-folder frames within 60 seconds whose singleton species tags differ. Tags are partial, so a differing pair is a question, not an error.",
        "order": "Seeded random, so any reviewed prefix is a random sample of the joined pairs.",
        "test_sessions_loaded": 0,
        "photos_without_preview": len(missing),
    }
    write_json(output / "audit-summary.json", summary)
    write_json(output / "merge-audit.json", cases)
    write_json(output / "missing-previews.json", {"photo_ids": sorted(missing)})
    template = Path(__file__).with_name("merge_audit.html").read_text()
    data = json.dumps({"summary": summary, "cases": cases, "decisions": DECISIONS}).replace("<", "\\u003c")
    report = output / "Review merged encounters.html"
    report.write_text(template.replace("__AUDIT_DATA__", data))
    return {
        "report": str(report),
        "joined_pairs": len(cases),
        "differing_label_pairs": totals["differing_label_pairs"],
        "missing_previews": len(missing),
        "cases_missing_a_pair_preview": sum(
            any(p["preview_pending"] and p["pair_side"] is not None for p in c["photos"]) for c in cases
        ),
    }


def wilson(successes, total, z=1.96):
    if not total:
        return None
    p = successes / total
    centre = (p + z * z / (2 * total)) / (1 + z * z / total)
    spread = z * math.sqrt(p * (1 - p) / total + z * z / (4 * total * total)) / (1 + z * z / total)
    return [round(max(0.0, centre - spread), 4), round(min(1.0, centre + spread), 4)]


def _rates(decided):
    tally = Counter(d for d in decided)
    answered = sum(tally[d] for d in ("keep", "tag_error", "split"))
    return {
        "decisions": dict(tally),
        "answered": answered,
        "wrong_merge_rate": round(tally["split"] / answered, 4) if answered else None,
        "wrong_merge_interval_95": wilson(tally["split"], answered),
    }


def results(audit, exported, output, *, existing=None):
    """Score exported decisions and write explicit two-photo boundary constraints."""
    audit = Path(audit).resolve()
    summary = json.loads((audit / "audit-summary.json").read_text())
    exported = json.loads(Path(exported).read_text())
    if exported.get("audit_created_at") != summary["created_at"]:
        raise ValueError("Decisions belong to another merge audit")
    cases = {c["id"]: c for c in json.loads((audit / "merge-audit.json").read_text())}
    decisions = exported["decisions"]
    if not decisions or len({d["case_id"] for d in decisions}) != len(decisions):
        raise ValueError("Expected nonempty, unique decisions")
    constraints, breakdown, corrections = [], defaultdict(list), []
    for decision in decisions:
        case = cases.get(decision["case_id"])
        if case is None:
            raise ValueError("A decision names a pair outside this audit")
        if decision["decision"] not in DECISIONS:
            raise ValueError("Unknown merge audit decision")
        reviewed_at = datetime.fromisoformat(decision["updated_at"])
        if reviewed_at.tzinfo is None:
            raise ValueError("Review timestamp needs a timezone")
        if case["partition"] not in {"train", "development"}:
            raise ValueError("Only training/development pairs may become constraints")
        kind = decision["decision"]
        for key in ("all", "reason:" + case["reason"], "gap:" + case["gap_bucket"], "partition:" + case["partition"]):
            breakdown[key].append(kind)
        pair = [p for p in case["photos"] if p["pair_side"] is not None]
        if kind == "tag_error":
            corrections.append(
                {
                    "case_id": case["id"],
                    "photos": [{"id": p["id"], "filename": p["filename"], "labels": p["labels"]} for p in pair],
                    "notes": decision.get("notes", ""),
                }
            )
        if kind == "unsure":
            continue
        a, b = case["pair"]
        constraints.append(
            {
                "id": "merge-audit-" + case["id"],
                "workspace": case["workspace"],
                "session": case["session"],
                "partition": case["partition"],
                "ids": [a, b],
                "expected_groups": [[a, b]] if kind in JOINED else [[a], [b]],
                "photos": [{"id": p["id"], "filename": p["filename"]} for p in pair],
                "decision": kind,
                "notes": decision.get("notes", ""),
                "reviewed_at": reviewed_at.astimezone(UTC).isoformat(),
                "source": "merge-audit",
                "audit_created_at": summary["created_at"],
            }
        )
    joined = summary["counts"]["joined_pairs"]
    overall = _rates(breakdown["all"])
    estimate = None
    if overall["wrong_merge_rate"] is not None:
        estimate = {
            "wrong_merges": round(overall["wrong_merge_rate"] * joined, 1),
            "interval_95": [round(x * joined, 1) for x in overall["wrong_merge_interval_95"]],
            "valid_if": "Reviewed pairs are a prefix of the seeded random order (skipping cases biases it).",
        }
    combined = list(constraints)
    if existing is not None:
        prior = json.loads(Path(existing).read_text())
        ids = {c["id"] for c in prior}
        clashes = [c["id"] for c in constraints if c["id"] in ids]
        if clashes:
            raise ValueError(f"Constraints already present in {existing}: {clashes[:3]}")
        combined = prior + constraints
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    result = {
        "created_at": datetime.now(UTC).isoformat(),
        "audit": str(audit),
        "audit_created_at": summary["created_at"],
        "joined_pairs": joined,
        "reviewed": len(decisions),
        "overall": overall,
        "estimated_wrong_merges": estimate,
        "breakdown": {k: _rates(v) for k, v in sorted(breakdown.items()) if k != "all"},
        "constraints_written": len(constraints),
        "tag_corrections": len(corrections),
        "scope": "Human decisions on joined differing-label pairs only; outer encounter edges are not inferred.",
    }
    write_json(output / "merge-audit-results.json", result)
    write_json(output / "merge-audit-constraints.json", constraints)
    write_json(output / "reviewed-constraints.json", combined)
    write_json(output / "tag-corrections.json", corrections)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    make = sub.add_parser("build", help="List joined differing-label pairs as a review page")
    make.add_argument("--scope", type=Path, action="append", required=True, help="Retained scope with manifest.json")
    make.add_argument("--output", type=Path, required=True, help="New private output directory")
    make.add_argument("--context", type=int, default=4, help="Encounter frames shown on each side of the pair")
    make.add_argument("--seed", type=int, default=42)
    score = sub.add_parser("results", help="Score exported decisions and write boundary constraints")
    score.add_argument("--audit", type=Path, required=True, help="Directory written by build")
    score.add_argument("--decisions", type=Path, required=True, help="Exported decisions JSON")
    score.add_argument("--output", type=Path, required=True, help="New private output directory")
    score.add_argument("--existing", type=Path, help="Existing reviewed-constraints.json to extend")
    args = parser.parse_args()
    if args.command == "build":
        result = build(args.scope, args.output, context=args.context, seed=args.seed)
    else:
        result = results(args.audit, args.decisions, args.output, existing=args.existing)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
