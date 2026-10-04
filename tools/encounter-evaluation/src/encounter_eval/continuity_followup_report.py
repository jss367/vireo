"""Build the small ambiguous-case review using the existing export/import format."""

import argparse
import base64
import gzip
import hashlib
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import unquote, urlparse

from .algorithms import run_algorithm
from .common import code_identity, configure_repo, digest, encode, write_json
from .continuity_compare import _preview
from .continuity_experiments import apply_candidate, prepare_baseline
from .library import read_bundle


def build(comparison, output):
    repo = configure_repo()
    from bursts import detect_bursts

    comparison, output = Path(comparison), Path(output)
    frozen_source = json.loads((comparison / "search-design.json").read_text())["source"]
    current_source = code_identity(repo)
    # The report pairs the frozen revision with a hash of the live source: both
    # must come from the same checkout, or the recorded provenance lies.
    if current_source["source_digest"] != frozen_source["source_digest"]:
        raise ValueError(
            "Current checkout differs from the frozen comparison; check out the comparison's "
            "revision before regenerating the report or rerun the comparison"
        )
    selection = json.loads((comparison / "summary.json").read_text())
    changed = json.loads((comparison / "changed-cases.json").read_text())["cases"]
    cases = [c for c in changed if c["needs_review"]]
    output.mkdir(parents=True, exist_ok=False)
    scopes, loaded, review = {}, {}, []
    for case in cases:
        source = Path(case["scope"])
        if source not in scopes:
            manifest = json.loads((source / "manifest.json").read_text())
            scope = output / f"scope-{len(scopes)}-workspace-{manifest['workspace']}"
            (scope / "inputs").mkdir(parents=True)
            (scope / "baseline-inputs").mkdir()
            scopes[source] = {"manifest": manifest, "path": scope, "entries": [], "baseline_files": {}}
        stored = scopes[source]
        manifest, scope = stored["manifest"], stored["path"]
        key = source, case["session"]
        if key not in loaded:
            entry = next(e for e in manifest["sessions"] if e["id"] == case["session"])
            if entry["partition"] not in {"train", "development"} or entry["digest"] != case["input_digest"]:
                raise ValueError("Review source identity or partition changed")
            bundle = read_bundle(source, entry)
            prepared = prepare_baseline(bundle["photos"], manifest["config"])
            after = apply_candidate(prepared, manifest["config"], selection["selected"])
            baseline = prepared[0]
            proposed_bundle = {**bundle, "photos": after}
            relative = f"inputs/{entry['id']}.json.gz"
            (scope / relative).write_bytes(gzip.compress(encode(proposed_bundle).encode(), mtime=0))
            stored["entries"].append({**entry, "path": relative, "digest": digest(proposed_bundle)})
            baseline_name = f"{entry['id']}.json.gz"
            (scope / "baseline-inputs" / baseline_name).write_bytes(gzip.compress(encode(baseline).encode(), mtime=0))
            stored["baseline_files"][str(min(p["id"] for p in after))] = {
                "path": baseline_name,
                "digest": digest(baseline),
            }
            loaded[key] = bundle, {"before": {p["id"]: p for p in baseline}, "after": {p["id"]: p for p in after}}
            # Rendering may not silently replay different baseline or candidate results.
            for version, features in (("before", baseline), ("after", after)):
                groups = run_algorithm("production", features, grouping_config=manifest["grouping_config"])
                for relevant in (c for c in cases if (Path(c["scope"]), c["session"]) == key):
                    ids = set(relevant["ids"])
                    actual = [
                        (list(g.photo_ids), list(g.roster) if g.roster else None)
                        for g in groups
                        if ids & set(g.photo_ids)
                    ]
                    expected = [(g["photo_ids"], g["roster"]) for g in relevant[version]]
                    if actual != expected:
                        raise ValueError(f"Frozen {version} grouping changed since comparison")
        bundle, maps = loaded[key]
        display = manifest.get("taxonomy_display", {})
        record = {
            **case,
            "kind": "changed",
            "date": maps["after"][case["ids"][0]]["timestamp"][:10],
            "differing_reference_sets": any(
                len(
                    {
                        tuple(bundle["answers"][str(pid)]["taxa"])
                        for pid in g["photo_ids"]
                        if str(pid) in bundle["answers"]
                    }
                )
                > 1
                for g in case["after"]
            ),
            "photos": [],
        }
        for pid in case["ids"]:
            p = maps["after"][pid]
            presentation = bundle["presentation"][str(pid)]
            preview = _preview(pid, presentation)
            if preview:
                path = Path(unquote(urlparse(preview).path))
                mime = "image/png" if path.suffix.lower() == ".png" else "image/jpeg"
                preview = f"data:{mime};base64," + base64.b64encode(path.read_bytes()).decode()
            answer = bundle["answers"].get(str(pid), {})
            record["photos"].append(
                {
                    "id": pid,
                    "filename": presentation["filename"],
                    "timestamp": p["timestamp"],
                    "preview": preview,
                    "labels": [display.get(k, k) for k in answer.get("taxa", [])],
                    "complete": answer.get("complete", False),
                    "rescued": bool(p.get("experimental_continuity")),
                }
            )
        for version in ("before", "after"):
            record[version] = [
                {
                    "ids": g["photo_ids"],
                    "species": ", ".join(display.get(k, k) for k in g["roster"])
                    if g["roster"]
                    else "No species suggestion",
                    "bursts": len(
                        detect_bursts(
                            [maps[version][pid] for pid in g["photo_ids"]], manifest["config"].get("pipeline", {})
                        )
                    ),
                }
                for g in case[version]
            ]
        review.append(record)
    for source, stored in scopes.items():
        manifest = stored["manifest"]
        retained = [bundle for (path, _session), (bundle, _maps) in loaded.items() if path == source]
        write_json(
            stored["path"] / "manifest.json",
            {
                **manifest,
                "sessions": stored["entries"],
                "source_manifest_digest": digest(manifest),
                "inventory": {
                    "selected_sessions": len(stored["entries"]),
                    "photos": sum(len(b["photos"]) for b in retained),
                    "labeled_photos": sum(len(b["answers"]) for b in retained),
                },
                "data_digest": digest(
                    [
                        stored["entries"],
                        manifest["config"],
                        manifest.get("label_source"),
                        sorted(manifest.get("complete_folders", [])),
                    ]
                ),
            },
        )
        write_json(
            stored["path"] / "baseline-manifest.json",
            {
                "revision": frozen_source["revision"],
                "source_sha256": hashlib.sha256((repo / "vireo/encounter_continuity.py").read_bytes()).hexdigest(),
                "files": stored["baseline_files"],
            },
        )
    totals = {
        version: sum((Counter(selection[key][part]["counts"]) for part in ("train", "development")), Counter())
        for version, key in (("before", "baseline"), ("after", "selected_metrics"))
    }
    summary = {
        "created_at": datetime.now(UTC).isoformat(),
        "stats": {"photos": totals["before"]["photos"], "review_sessions": len(loaded)},
        "metrics": {k: {"counts": dict(v)} for k, v in totals.items()},
        "changed_encounter_cases": len(review),
        "remaining_interruption_candidates": 0,
        "automatically_supported_cases": len(changed) - len(review),
        "selection": selection["selected"],
        "test_sessions_evaluated": 0,
    }
    write_json(output / "comparison-summary.json", summary)
    # Compact JSON keeps image bytes in the self-contained HTML only.
    write_json(
        output / "encounter-review.json",
        [{**c, "photos": [{**p, "preview": None} for p in c["photos"]]} for c in review],
    )
    template = Path(__file__).with_name("continuity_review.html").read_text()
    template = template.replace("Review encounter grouping changes", "Review uncertain encounter sequences")
    start = template.index("$('scope').textContent=")
    end = template.index("\nfor(const", start)
    template = (
        template[:start]
        + (
            "$('scope').textContent=DATA.cases.length+' uncertain sequences need review; '+summary.automatically_supported_cases"
            "+' other changed sequences are supported by existing labels. The metrics below cover the complete training/development comparison.';"
        )
        + template[end:]
    )
    template = template.replace("['Bursts','bursts'],", "")
    template = template.replace(
        "Inspect whether the new grouping follows the same subject through the sequence.",
        "Some frames have no species tags, so existing labels cannot settle these boundaries. Does the proposed grouping follow the same subject?",
    )
    data = json.dumps({"summary": summary, "cases": review}).replace("<", "\\u003c")
    report = output / "Review uncertain encounter sequences.html"
    report.write_text(template.replace("__COMPARISON_DATA__", data))
    return {
        "report": str(report),
        "cases": len(review),
        "photos": sum(len(c["photos"]) for c in review),
        "missing_previews": sum(p["preview"] is None for c in review for p in c["photos"]),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--comparison", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(build(args.comparison, args.output), indent=2))


if __name__ == "__main__":
    main()
