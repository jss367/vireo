"""Command-line entry point. Every new run reads current labels automatically."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path

from .common import code_identity, configure_repo, write_json
from .library import open_library, prepare
from .report import write_report
from .runner import evaluate, tune


def positive_int(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def parser():
    p = argparse.ArgumentParser(description="Evaluate and tune encounter grouping using cached Vireo data")
    p.add_argument("command", choices=["inventory", "compare", "tune"])
    p.add_argument("--repo", type=Path, help="Vireo checkout (auto-detected for an editable tool installation)")
    p.add_argument("--db", type=Path, default=Path.home() / ".vireo" / "vireo.db")
    p.add_argument("--workspace", type=int)
    p.add_argument("--output", type=Path, help="New run directory; defaults to ~/.vireo/encounter-evaluation/runs/<time-id>")
    p.add_argument("--resume", type=Path, help="Continue an existing run using its retained inputs, not current labels")
    p.add_argument("--max-sessions", type=positive_int, help="Limit whole sessions; unlimited by default")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--capture-date", help="Only sessions on this capture date (YYYY-MM-DD); keeps stable split membership")
    p.add_argument("--split-registry", type=Path, help="Persistent day membership; defaults beside run directories")
    p.add_argument("--label-source", choices=["all", "manual"], default="all")
    p.add_argument("--review-labels", type=Path, help="Review database containing saved per-photo reference labels")
    p.add_argument("--complete-folder", type=int, action="append", default=[],
                   help="Assert that tagged photos in this folder have complete species rosters; repeatable")
    p.add_argument("--config", type=Path, help="Explicit JSON loader/grouping settings; otherwise recorded app defaults")
    p.add_argument("--partition", choices=["train", "development", "test", "all"], default="development",
                   help="compare only; tune always uses train then development")
    p.add_argument("--candidate", choices=["sequence", "independent", "production"], default="sequence")
    p.add_argument("--candidate-file", type=Path, help="compare: selected-candidate.json or {algorithm, params}")
    p.add_argument("--space", type=Path, help="tune: JSON object mapping parameter names to lists of values")
    p.add_argument("--method", choices=["grid", "random"], default="random")
    p.add_argument("--trials", type=positive_int, default=12)
    p.add_argument("--seconds", type=positive_int, default=300, help="Search budget checked between trials")
    p.add_argument("--min-coverage", type=float, default=0.5)
    p.add_argument("--max-encounters-per-1000", type=float, default=1000)
    return p


def main(argv=None):
    p = parser()
    args = p.parse_args(argv)
    if not 0 <= args.min_coverage <= 1 or not 0 < args.max_encounters_per_1000 <= 1000:
        p.error("coverage must be between 0 and 1; encounter limit must be in (0, 1000]")
    if args.resume and args.output:
        p.error("--resume and --output are mutually exclusive")
    if args.resume and (args.workspace is not None or args.max_sessions is not None or args.complete_folder
                        or args.config or args.split_registry or args.review_labels or args.capture_date or args.label_source != "all" or args.seed != 42):
        p.error("--resume retains the original data and split policy; start a new run to change data options")
    if args.command == "tune" and (args.partition != "development" or args.candidate_file):
        p.error("tune uses train/development only; --partition and --candidate-file are for compare")
    try:
        repo = configure_repo(args.repo)
        if args.command == "inventory":
            conn = open_library(args.db)
            try:
                rows = [dict(r) for r in conn.execute("""SELECT w.id, w.name,
                    COUNT(DISTINCT p.id) AS photos, COUNT(DISTINCT f.id) AS folders
                    FROM workspaces w LEFT JOIN workspace_folders wf ON wf.workspace_id = w.id
                    LEFT JOIN folders f ON f.id = wf.folder_id LEFT JOIN photos p ON p.folder_id = f.id
                    GROUP BY w.id ORDER BY w.id""")]
                print(json.dumps(rows, indent=2))
            finally:
                conn.close()
            return 0
        if args.resume:
            identity = code_identity(repo)
            output = args.resume.expanduser().resolve()
            manifest = json.loads((output / "manifest.json").read_text())
            if any(manifest["code"][k] != identity[k] for k in ("source_digest", "python", "numpy")):
                raise ValueError("Source/environment changed since this run; start a new comparison")
            print("Resuming retained comparison inputs. Start a new run to include newer labels.", flush=True)
        else:
            run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
            output = (args.output or Path.home() / ".vireo" / "encounter-evaluation" / "runs" / run_id).expanduser().resolve()
            output.mkdir(parents=True, exist_ok=False)
            cfg = json.loads(args.config.read_text()) if args.config else None
            print(f"Preparing current library evidence in {output}", flush=True)
            manifest = prepare(args.db, output, workspace=args.workspace, seed=args.seed,
                               max_sessions=args.max_sessions, complete_folders=args.complete_folder,
                               label_source=args.label_source, config=cfg, split_registry=args.split_registry, repo=repo,
                               review_labels=args.review_labels, capture_date=args.capture_date)
        if not manifest["sessions"]:
            raise ValueError("No labeled sessions selected; inspect workspace and label-source coverage")
        if args.command == "compare":
            selected = json.loads(args.candidate_file.read_text()) if args.candidate_file else {"algorithm": args.candidate, "params": {}}
            results = [evaluate(output, manifest, "production", {}, args.partition)]
            if not results[0]["sessions"]:
                raise ValueError(f"No labeled sessions in {args.partition}; include more sessions or choose another partition")
            results.append(evaluate(output, manifest, selected["algorithm"], selected.get("params", {}), args.partition))
            if not args.candidate_file and args.candidate == "sequence":
                results.append(evaluate(output, manifest, "independent", {}, args.partition))
            write_json(output / "comparison.json", results)
            candidate = results[1]
        else:
            space = json.loads(args.space.read_text()) if args.space else None
            search, results = tune(output, manifest, algorithm=args.candidate, space=space, method=args.method,
                                   trials=args.trials, seconds=args.seconds, min_coverage=args.min_coverage,
                                   max_encounters_per_1000=args.max_encounters_per_1000)
            candidate = search["winner"]
            if not candidate:
                print("No candidate met selection requirements within the budget. No parameters selected.", flush=True)
        write_report(output, manifest, results, candidate=candidate)
        for result in results:
            m = result["metrics"]
            print(f"{result['algorithm']} / {result['partition']}: objective={m['objective']}, coverage={m['review_coverage']}")
        print(f"Report: {output / 'report.html'}")
        return 0
    except (ValueError, OSError, KeyError, sqlite3.Error) as exc:
        print(f"Evaluation failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
