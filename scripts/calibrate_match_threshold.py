#!/usr/bin/env python3
"""Derive a per-model match-score floor from confirmed identifications.

Classifier confidence is a softmax over whichever label list was loaded, so it
sums to 1 and always names a winner however badly every label fit. The raw
pre-softmax score recorded per classifier run
(``classifier_match_scores.max_match_score`` — the winner score across the
whole label list) does not renormalize, so it can say whether anything in the
list matched at all — but only once you know where the cutoff falls, and that
is a property of the model and of your own photos, not something that can be
guessed.

This script finds it the only honest way available: against species you have
already confirmed yourself. One sample per classifier run
(``(detection, model, labels_fingerprint)``), drawn only from photos with
exactly one confirmed species keyword — a multi-species photo cannot serve as
detection-level ground truth, because a detection scored as "correct" against
any of its keywords might actually depict the other species. Each remaining
run is classified by whether its top species matches the photo's single
confirmed keyword:

    correct   — the classifier's top species for this detection matches the
                photo's confirmed species keyword
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

* Runs written before ``classifier_match_scores`` existed leave no row here
  and are skipped. A catalog classified entirely before this change has
  nothing to calibrate against and will report zero usable rows until
  something is re-classified.
* "Incorrect" here means "disagreed with your keyword", which bundles genuinely
  out-of-list birds together with in-list mistakes. It over-counts, so the
  catch rate reported below is a floor, not an estimate.
* Thresholds are per model AND per score kind. A cosine floor means nothing
  applied to logits; the two are never pooled.
"""

import argparse
import bisect
import json
import os
import sqlite3
import sys


def _percentile(values, q):
    """Nearest-rank percentile over a pre-sorted list.

    Used only for the diagnostic p1/p5/p25/p50 lines the script prints.
    The chosen floor uses ``_floor_within_suppression_cap`` instead, because
    nearest-rank rounding can exceed a small cap (e.g. n=252 at q=0.01 rounds
    to index 3, hiding 3/252=1.19% of correct rows despite a 1% budget).
    """
    if not values:
        return None
    idx = int(round(q * (len(values) - 1)))
    return values[max(0, min(len(values) - 1, idx))]


def _floor_within_suppression_cap(sorted_values, max_suppression):
    """Return the largest value whose strictly-lower count fits the cap.

    Given a sorted list, pick the highest ``t`` such that the number of
    values strictly less than ``t`` is at most ``floor(max_suppression * n)``.
    Ties allow ``t`` to advance past ``floor(max_suppression * n)``: several
    equal values do not each count against the cap because a threshold at
    their value does not hide any of them.

    Nearest-rank interpolation (``round(q * (n - 1))``) can round up and
    quietly exceed the requested cap — at ``n = 252`` and
    ``max_suppression = 0.01`` it picks index 3, hiding 3/252 = 1.19% of
    correct rows behind a stated 1% budget. This routine caps by
    construction: ``floor(max_suppression * n)`` is the maximum number of
    strictly-lower samples allowed, and the returned value never violates
    it.
    """
    if not sorted_values:
        return None
    n = len(sorted_values)
    budget = int(max_suppression * n)  # floor
    # ``sorted_values[budget]`` has exactly ``bisect_left(sorted_values,
    # sorted_values[budget])`` values strictly less than it; that count is
    # <= budget by construction (indices 0..budget-1 sit below or equal to
    # it, and equal-value neighbours are not strictly less). Ties can let a
    # higher index name the same value; take ``bisect_right - 1`` so we
    # return the largest index whose value is still ``sorted_values[budget]``.
    if budget >= n:
        return sorted_values[-1]
    threshold_value = sorted_values[budget]
    last_idx = bisect.bisect_right(sorted_values, threshold_value) - 1
    return sorted_values[last_idx]


def schema_gap(conn):
    """Return a message if this catalog predates match scoring, else None.

    A catalog that the app has not yet opened since this feature landed has
    neither the column nor the table, and the query below would die on it. That
    is a normal state, not a bug, so say what to do about it.
    """
    has_table = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' "
        "AND name='classifier_match_scores'"
    ).fetchone() is not None
    if not has_table:
        return (
            "This catalog has no match-score schema yet — it has not been\n"
            "opened by a build that records it. Start Vireo against this\n"
            "database once (the migration runs at startup), re-classify\n"
            "something, then run this again."
        )
    return None


def collect(conn):
    """Return ``{(model, score_kind): {"correct": [...], "incorrect": [...]}}``.

    One sample per ``(detection, model, labels_fingerprint)`` classifier run,
    keyed by ``classifier_match_scores.max_match_score`` — the run-level
    winner score across the whole label list. The sample is labelled by
    whether the run's ``top_species`` matches a species keyword the photo
    carries.

    Why the run-level score and not per-candidate ``predictions.match_score``:
    the configured threshold is applied to a run's maximum. Bucketing every
    top-k candidate would treat one classifier run as multiple independent
    samples — every correct top-1 run would contribute up to k-1 alternative
    species as "incorrect" and the ground-truth species appearing lower in
    the list would contribute its (lower) per-label score to the "correct"
    bucket. Both distortions push the fitted floor away from the quantity it
    will actually be applied to.

    Keyword comparison is COLLATE NOCASE because that is the collation every
    other keyword join in the app uses — matching it here keeps a
    capitalization difference from being scored as a wrong identification.

    Names alone are not enough: a scientific-name keyword
    (``Setophaga citrina``) and a common-name ``top_species`` (``Hooded
    Warbler``) refer to the same bird, and an exact-string comparison would
    score that correct run as ``incorrect``. So the predicate also matches
    on canonical taxon identity: the winning prediction's
    ``source_taxon_id`` against the keyword's ``source_taxon_id``. Both
    sides store the same external (iNat) id, and the join is against the
    prediction row that names the run's ``top_species`` — the identity of
    the winner, not of any lower-ranked alternate. Rows with no recorded
    taxon id on either side fall back to the name comparison.
    """
    rows = conn.execute(
        """
        SELECT cms.classifier_model    AS model,
               cms.score_kind          AS score_kind,
               cms.max_match_score     AS match_score,
               EXISTS (
                   SELECT 1
                   FROM photo_keywords pk
                   JOIN keywords k ON k.id = pk.keyword_id
                   LEFT JOIN taxa t ON t.id = k.taxon_id
                   WHERE pk.photo_id = d.photo_id
                     AND (k.is_species = 1 OR k.type = 'taxonomy')
                     AND (t.rank IS NULL OR t.rank = 'species')
                     AND (
                           k.name = cms.top_species COLLATE NOCASE
                           OR (
                                 k.source_taxon_id IS NOT NULL
                                 AND k.source_taxon_id IN (
                                     SELECT pw.source_taxon_id
                                     FROM predictions pw
                                     WHERE pw.detection_id = cms.detection_id
                                       AND pw.classifier_model = cms.classifier_model
                                       AND pw.labels_fingerprint = cms.labels_fingerprint
                                       AND pw.species = cms.top_species COLLATE NOCASE
                                       AND pw.source_taxon_id IS NOT NULL
                                 )
                           )
                     )
               ) AS is_correct
        FROM classifier_match_scores cms
        JOIN detections d ON d.id = cms.detection_id
        WHERE cms.max_match_score IS NOT NULL
          AND cms.top_species IS NOT NULL
          -- Only photos with EXACTLY ONE distinct confirmed species keyword
          -- can serve as ground truth. Vireo is explicitly a multi-species
          -- app: two detections on one photo are routinely two different
          -- animals. If the photo carries keywords for both, a detection
          -- whose top_species matches EITHER keyword would be scored
          -- ``correct`` here even when it actually depicts the OTHER species,
          -- which lets false positives into the low-percentile "correct"
          -- distribution and lowers the fitted floor. Restricting to
          -- single-species photos is conservative but honest: the row scores
          -- what the keyword says the photo is of, without a second species
          -- to confuse it. Zero-species photos are still excluded (their
          -- keyword can never make the prediction wrong).
          AND (
                SELECT COUNT(DISTINCT LOWER(k2.name))
                FROM photo_keywords pk2
                JOIN keywords k2 ON k2.id = pk2.keyword_id
                LEFT JOIN taxa t2 ON t2.id = k2.taxon_id
                WHERE pk2.photo_id = d.photo_id
                  AND (k2.is_species = 1 OR k2.type = 'taxonomy')
                  AND (t2.rank IS NULL OR t2.rank = 'species')
          ) = 1
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

    The floor is the largest order statistic whose strictly-lower count
    fits ``floor(max_suppression * n)`` — see
    ``_floor_within_suppression_cap`` for why nearest-rank rounding cannot
    honour a small cap.
    """
    if not correct:
        return None, None, None
    ordered = sorted(correct)
    threshold = _floor_within_suppression_cap(ordered, max_suppression)
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
