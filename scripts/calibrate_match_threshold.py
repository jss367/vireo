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
import math
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
    """Return ``{(model, score_kind): {"correct", "incorrect", "fingerprints"}}``.

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
    ``source_taxon_id`` against the keyword's canonical id, and the join is
    against the prediction row that names the run's ``top_species`` — the
    identity of the winner, not of any lower-ranked alternate. Rows with no
    recorded taxon id on either side fall back to the name comparison.

    A keyword's canonical id is ``COALESCE(k.source_taxon_id, t.inat_id)``,
    not ``k.source_taxon_id`` alone. Only ``_add_source_species_keyword`` —
    the path taken when a *prediction* creates the keyword — records
    ``source_taxon_id``. A keyword that arrived from XMP or by hand goes
    through the name-resolving ``add_keyword`` path, which fills the local
    ``keywords.taxon_id`` and leaves ``source_taxon_id`` NULL. Those are
    precisely the human-confirmed identifications this script calibrates
    against, so comparing only ``source_taxon_id`` would drop the
    common-name/scientific-name case for exactly the rows that matter and
    bias the fitted floor downward by filing confirmed-correct runs as
    ``incorrect``. ``taxa.inat_id`` is the same external id under a
    different column.

    ``fingerprints`` breaks the same rows down by the label list they were
    scored against — see ``_label_list_warning`` for why a pooled floor needs
    that breakdown printed beside it.
    """
    rows = conn.execute(
        """
        SELECT cms.classifier_model    AS model,
               cms.score_kind          AS score_kind,
               cms.max_match_score     AS match_score,
               cms.labels_fingerprint  AS labels_fingerprint,
               cms.label_count         AS label_count,
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
                                 COALESCE(k.source_taxon_id, t.inat_id) IS NOT NULL
                                 AND COALESCE(k.source_taxon_id, t.inat_id) IN (
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
          --
          -- Counted over canonical taxon identity, not over names. A photo
          -- keyworded both "Hooded Warbler" and "Setophaga citrina" carries
          -- one species, and counting names would read it as two and throw
          -- the photo out — discarding exactly the carefully-identified rows
          -- this calibration wants most, and doing so inconsistently with
          -- the correctness predicate above, which already treats the two
          -- names as the same taxon. The identity ladder matches that
          -- predicate: external id first (from either column), then the
          -- local ``taxa`` row, then the folded name for a keyword that
          -- resolved to no taxon at all.
          AND (
                SELECT COUNT(DISTINCT COALESCE(
                    'inat:' || CAST(COALESCE(k2.source_taxon_id, t2.inat_id) AS TEXT),
                    'taxon:' || CAST(k2.taxon_id AS TEXT),
                    'name:' || LOWER(k2.name)
                ))
                FROM photo_keywords pk2
                JOIN keywords k2 ON k2.id = pk2.keyword_id
                LEFT JOIN taxa t2 ON t2.id = k2.taxon_id
                WHERE pk2.photo_id = d.photo_id
                  AND (k2.is_species = 1 OR k2.type = 'taxonomy')
                  AND (t2.rank IS NULL OR t2.rank = 'species')
          ) = 1
          -- Only photos with AT MOST ONE real detection can serve as
          -- ground truth for a photo-level keyword. A single confirmed
          -- keyword is a fact about the photo, not per-detection: if the
          -- frame holds two animals and only one has been identified,
          -- both detections are still evaluated against that one species
          -- above. A classifier that guessed the confirmed species for the
          -- OTHER animal is then wrongly filed as ``correct`` and pulls
          -- the low-percentile "correct" distribution down, lowering the
          -- fitted floor (Codex P2 on 22cc0ac). "Real" excludes the
          -- synthetic full-image anchor — it is the detector's zero-animal
          -- placeholder, not evidence of a second subject — so a full-image
          -- classification on a whole-frame photo still calibrates.
          -- ``IS NOT`` (rather than ``!=``) so a detection row with a NULL
          -- detector_model still counts as real: production rows always
          -- carry the detector name, and treating a missing value as
          -- "unknown, so ignore" would silently readmit multi-detection
          -- photos in exactly the case a legacy import might create.
          AND (
                SELECT COUNT(*) FROM detections d2
                WHERE d2.photo_id = d.photo_id
                  AND d2.detector_model IS NOT 'full-image'
          ) <= 1
        """
    ).fetchall()

    buckets = {}
    for row in rows:
        key = (row["model"], row["score_kind"] or "unknown")
        bucket = buckets.setdefault(
            key, {"correct": [], "incorrect": [], "fingerprints": {}},
        )
        side = "correct" if row["is_correct"] else "incorrect"
        bucket[side].append(float(row["match_score"]))
        seen = bucket["fingerprints"].setdefault(
            row["labels_fingerprint"],
            {"correct": 0, "incorrect": 0, "label_count": None},
        )
        seen[side] += 1
        if row["label_count"] is not None:
            seen["label_count"] = row["label_count"]
    return buckets


#: Ratio between the largest and smallest label list contributing to one
#: model's pooled floor, above which the pooling is called out rather than
#: quietly applied. Two-to-one is the point where "maximum over N labels"
#: stops being roughly the same statistic on both sides.
LABEL_SPREAD_WARN_RATIO = 2.0


def _label_list_warning(fingerprints):
    """Describe the label lists behind a pooled floor, and flag a wide spread.

    Thresholds are stored and applied per MODEL (``match_confidence`` looks up
    ``match_thresholds[model]``), so calibration pools every label list that
    model has ever been run against. Each individual cosine or logit is
    genuinely list-independent — that is the property the whole feature rests
    on — but the statistic being calibrated is ``max_match_score``, the
    maximum over the loaded list. The expected maximum grows with the number
    of labels, so a 1,400-label list and an 800-label list are not quite
    samples from the same distribution, and a pooled floor can respect the
    suppression cap in aggregate while exceeding it for the list actually on
    screen.

    Calibrating per fingerprint instead would fix that and cost more than it
    buys: ``--min-samples`` exists because a floor fitted to a few dozen rows
    describes noise, and splitting one model's confirmed IDs across every
    list it has ever seen is the fastest way to push every bucket under that
    bar and emit no threshold at all. So the default stays pooled — and says
    so out loud, with the counts, rather than presenting one number as if a
    single population produced it.
    """
    lines = []
    ordered = sorted(
        fingerprints.items(),
        key=lambda item: -(item[1]["correct"] + item[1]["incorrect"]),
    )
    for fingerprint, counts in ordered:
        labels = counts["label_count"]
        lines.append(
            f"    {fingerprint or '(none)'}: "
            f"{counts['correct']:,} correct, {counts['incorrect']:,} disagreed"
            + (f", {labels:,} labels" if labels else ", label count unrecorded")
        )
    sizes = [c["label_count"] for c in fingerprints.values() if c["label_count"]]
    warning = None
    if len(sizes) > 1 and max(sizes) >= min(sizes) * LABEL_SPREAD_WARN_RATIO:
        warning = (
            f"    NOTE: these lists differ in size by "
            f"{max(sizes) / min(sizes):.1f}x ({min(sizes):,} to {max(sizes):,} "
            "labels). max_match_score is a maximum over the loaded list, so "
            "its distribution shifts with list size; one pooled floor will "
            "suppress more than the requested cap on the smaller list and "
            "less on the larger. Consider calibrating against only the list "
            "you actually classify with."
        )
    return lines, warning


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

        fingerprints = bucket.get("fingerprints") or {}
        if len(fingerprints) > 1:
            print(f"  pooled from {len(fingerprints)} label lists:")
            lines, warning = _label_list_warning(fingerprints)
            for line in lines:
                print(line)
            if warning:
                print(warning)

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
        # Serialize with ``math.floor`` at six decimals rather than ``round``.
        # ``round`` breaks ties toward the nearest-even and can round upward:
        # a computed floor of 0.1234566 would serialize as 0.123457, which
        # is strictly greater than the value ``suggest()`` returned and
        # can hide the very row the cap was chosen to spare (Codex P2 on
        # 22cc0ac). ``math.floor(t * 1e6) / 1e6`` returns a value ≤ t, so
        # the count of strictly-lower samples never grows; the advertised
        # ``--max-suppression`` cap survives serialization intact.
        suggested[model] = {
            "threshold": math.floor(threshold * 1_000_000) / 1_000_000,
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
        # A hand-edited config can carry a non-mapping ``match_thresholds``
        # (e.g. ``"match_thresholds": "bad"``); ``dict()`` would raise
        # ``ValueError`` on a string and lose the calibration work. Treat
        # non-mappings as an empty map, matching ``threshold_for()`` in
        # ``vireo/match_confidence.py`` — ``--apply`` replaces the malformed
        # value with the calibrated one instead of crashing (Codex P2 on
        # f074d0c).
        existing = current.get("match_thresholds")
        merged = dict(existing) if isinstance(existing, dict) else {}
        merged.update(suggested)
        cfg.save({**current, "match_thresholds": merged})
        print(f"\nWritten to {cfg.CONFIG_PATH}")
    else:
        print("\nRe-run with --apply to write these into ~/.vireo/config.json.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
