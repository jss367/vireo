#!/usr/bin/env python3
"""Derive a per-model match-score floor from confirmed identifications.

Classifier confidence is a softmax over whichever label list was loaded, so it
sums to 1 and always names a winner however badly every label fit. The raw
pre-softmax score stored beside it (``predictions.match_score``) does not
renormalize, so it can say whether anything in the list matched at all — but
only once you know where the cutoff falls, and that is a property of the model
and of your own photos, not something that can be guessed.

This script finds it the only honest way available: against species you have
already confirmed yourself.

    correct   — the classifier's species for this detection matches a species
                keyword the photo actually carries
    incorrect — it does not

A threshold is then chosen to suppress at most ``--max-suppression`` of the
CORRECT identifications, and the script reports what share of the incorrect
ones that same threshold would have caught. The asymmetry is deliberate:
wrongly hiding a good ID costs a photo you would have kept, while letting a bad
one through only costs the review you were doing anyway.

Usage:
    python scripts/calibrate_match_threshold.py
    python scripts/calibrate_match_threshold.py --db ~/.vireo/vireo.db
    python scripts/calibrate_match_threshold.py --max-suppression 0.02 --apply

``--apply`` writes the result into ``match_thresholds`` in ~/.vireo/config.json;
without it the script only prints, including the exact JSON to paste.

Caveats worth reading before trusting the number:

* Rows written before ``predictions.match_score`` existed are NULL and are
  skipped. A catalog classified entirely before this change has nothing to
  calibrate against and will report zero usable rows until something is
  re-classified.
* "Incorrect" here means "disagreed with your keyword", which bundles genuinely
  out-of-list birds together with in-list mistakes. It over-counts, so the
  catch rate reported below is a floor, not an estimate.
* Thresholds are per model AND per score kind. A cosine floor means nothing
  applied to logits; the two are never pooled.
"""

import argparse
import json
import os
import sqlite3
import sys


def _percentile(values, q):
    """Nearest-rank percentile over a pre-sorted list."""
    if not values:
        return None
    idx = int(round(q * (len(values) - 1)))
    return values[max(0, min(len(values) - 1, idx))]


def schema_gap(conn):
    """Return a message if this catalog predates match scoring, else None.

    A catalog that the app has not yet opened since this feature landed has
    neither the column nor the table, and the query below would die on it. That
    is a normal state, not a bug, so say what to do about it.
    """
    cols = {r[1] for r in conn.execute("PRAGMA table_info(predictions)")}
    has_table = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' "
        "AND name='classifier_match_scores'"
    ).fetchone() is not None
    if "match_score" not in cols or not has_table:
        return (
            "This catalog has no match-score schema yet — it has not been\n"
            "opened by a build that records it. Start Vireo against this\n"
            "database once (the migration runs at startup), re-classify\n"
            "something, then run this again."
        )
    return None


def collect(conn):
    """Return ``{(model, score_kind): {"correct": [...], "incorrect": [...]}}``.

    One row per (detection, model, list, species) prediction that carries a
    match score, labelled by whether its species is among the species keywords
    on the photo. Keyword comparison is COLLATE NOCASE because that is the
    collation every other keyword join in the app uses — matching it here keeps
    a capitalization difference from being scored as a wrong identification.
    """
    rows = conn.execute(
        """
        SELECT pr.classifier_model AS model,
               cms.score_kind      AS score_kind,
               pr.match_score      AS match_score,
               EXISTS (
                   SELECT 1
                   FROM photo_keywords pk
                   JOIN keywords k ON k.id = pk.keyword_id
                   LEFT JOIN taxa t ON t.id = k.taxon_id
                   WHERE pk.photo_id = d.photo_id
                     AND (k.is_species = 1 OR k.type = 'taxonomy')
                     AND (t.rank IS NULL OR t.rank = 'species')
                     AND k.name = pr.species COLLATE NOCASE
               ) AS is_correct
        FROM predictions pr
        JOIN detections d ON d.id = pr.detection_id
        LEFT JOIN classifier_match_scores cms
               ON cms.detection_id = pr.detection_id
              AND cms.classifier_model = pr.classifier_model
              AND cms.labels_fingerprint = pr.labels_fingerprint
        WHERE pr.match_score IS NOT NULL
          -- Only photos that carry at least one confirmed species keyword can
          -- serve as ground truth. An unlabelled photo is not evidence that
          -- the prediction was wrong, and counting it as such would drag the
          -- threshold up until it suppressed real identifications.
          AND EXISTS (
                SELECT 1 FROM photo_keywords pk2
                JOIN keywords k2 ON k2.id = pk2.keyword_id
                WHERE pk2.photo_id = d.photo_id AND k2.is_species = 1
          )
        """
    ).fetchall()

    buckets = {}
    for row in rows:
        key = (row["model"], row["score_kind"] or "unknown")
        bucket = buckets.setdefault(key, {"correct": [], "incorrect": []})
        bucket["correct" if row["is_correct"] else "incorrect"].append(
            float(row["match_score"])
        )
    return buckets


def suggest(correct, incorrect, max_suppression):
    """Pick the highest floor that suppresses <= max_suppression of `correct`.

    Returns ``(threshold, suppressed_fraction, caught_fraction)``. A higher
    floor catches more bad identifications, so taking the highest one that
    still respects the suppression budget spends that budget rather than
    leaving it unused.
    """
    if not correct:
        return None, None, None
    ordered = sorted(correct)
    # The value at the max_suppression quantile is the largest floor under
    # which no more than that fraction of correct IDs fall.
    threshold = _percentile(ordered, max_suppression)
    if threshold is None:
        return None, None, None
    suppressed = sum(1 for v in correct if v < threshold) / len(correct)
    caught = (
        sum(1 for v in incorrect if v < threshold) / len(incorrect)
        if incorrect else None
    )
    return threshold, suppressed, caught


def fmt(value, score_kind):
    if value is None:
        return "n/a"
    return f"{value:.4f}" if score_kind == "cosine" else f"{value:.2f}"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument(
        "--db", default=os.path.expanduser("~/.vireo/vireo.db"),
        help="catalog to calibrate against (default ~/.vireo/vireo.db)",
    )
    parser.add_argument(
        "--max-suppression", type=float, default=0.01,
        help="largest fraction of confirmed-correct IDs the floor may hide "
             "(default 0.01)",
    )
    parser.add_argument(
        "--min-samples", type=int, default=200,
        help="refuse to suggest a threshold from fewer correct rows than this "
             "(default 200)",
    )
    parser.add_argument(
        "--apply", action="store_true",
        help="write the suggested thresholds into ~/.vireo/config.json",
    )
    args = parser.parse_args(argv)

    if not (0 < args.max_suppression < 1):
        parser.error("--max-suppression must be between 0 and 1")
    if not os.path.exists(args.db):
        parser.error(f"no catalog at {args.db}")

    # Read-only: calibration must never be able to damage a catalog, and the
    # app may well be running against this file right now.
    conn = sqlite3.connect(f"file:{args.db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        gap = schema_gap(conn)
        if gap:
            print(gap)
            return 1
        buckets = collect(conn)
    finally:
        conn.close()

    if not buckets:
        print(
            "No usable rows. Every prediction predates the match_score column,\n"
            "or no photo with predictions carries a confirmed species keyword.\n"
            "Re-classify something and try again."
        )
        return 1

    suggested = {}
    for (model, score_kind), bucket in sorted(buckets.items()):
        correct = bucket["correct"]
        incorrect = bucket["incorrect"]
        print(f"\n{model}  [{score_kind}]")
        print(f"  confirmed correct:   {len(correct):>7,}")
        print(f"  disagreed w/ keyword:{len(incorrect):>7,}")

        if correct:
            ordered = sorted(correct)
            pcts = [(q, _percentile(ordered, q)) for q in (0.01, 0.05, 0.25, 0.50)]
            print("  correct  score percentiles: " + "  ".join(
                f"p{int(q * 100)}={fmt(v, score_kind)}" for q, v in pcts
            ))
        if incorrect:
            ordered_bad = sorted(incorrect)
            pcts = [(q, _percentile(ordered_bad, q)) for q in (0.50, 0.75, 0.95, 0.99)]
            print("  incorrect score percentiles: " + "  ".join(
                f"p{int(q * 100)}={fmt(v, score_kind)}" for q, v in pcts
            ))

        if score_kind == "unknown":
            print("  -> skipped: no score_kind recorded, so the scale is unknown.")
            continue
        if len(correct) < args.min_samples:
            print(
                f"  -> skipped: {len(correct)} confirmed-correct rows is under "
                f"--min-samples ({args.min_samples}). A floor fitted to this "
                "few would mostly describe noise."
            )
            continue

        threshold, suppressed, caught = suggest(
            correct, incorrect, args.max_suppression
        )
        if threshold is None:
            print("  -> skipped: could not derive a threshold.")
            continue
        print(f"  -> threshold {fmt(threshold, score_kind)}")
        print(f"     hides {suppressed * 100:.2f}% of your confirmed IDs")
        if caught is None:
            print("     catch rate unknown: no disagreeing rows to measure against")
        else:
            print(
                f"     catches {caught * 100:.1f}% of predictions that "
                "disagreed with your keywords"
            )
            if caught < 0.2:
                print(
                    "     NOTE: a low catch rate means the two populations "
                    "overlap heavily — this model's raw score may not separate "
                    "in-list from out-of-list well on your catalog."
                )
        suggested[model] = {
            "threshold": round(threshold, 6),
            "score_kind": score_kind,
        }

    if not suggested:
        print("\nNothing to suggest yet.")
        return 1

    print("\nmatch_thresholds:")
    print(json.dumps(suggested, indent=2))

    if args.apply:
        sys.path.insert(
            0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "vireo")
        )
        import config as cfg

        current = cfg.load()
        merged = dict(current.get("match_thresholds") or {})
        merged.update(suggested)
        cfg.save({**current, "match_thresholds": merged})
        print(f"\nWritten to {cfg.CONFIG_PATH}")
    else:
        print("\nRe-run with --apply to write these into ~/.vireo/config.json.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
