"""Tests for scripts/calibrate_match_threshold.py.

The important invariant: one sample per classifier run (``(detection, model,
labels_fingerprint)`` — the quantity the threshold is later applied to), NOT
one sample per top-k prediction candidate. Bucketing per-candidate would
double-count each run and score the ground-truth species by its per-label
prediction score instead of the run-level maximum the floor governs.
"""
import importlib.util
import sqlite3
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "calibrate_match_threshold.py"


def _load_module():
    spec = importlib.util.spec_from_file_location(
        "calibrate_match_threshold", SCRIPT_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _seed_conn():
    """Build the minimum schema calibrate_match_threshold reads from."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE photos (
            id INTEGER PRIMARY KEY
        );
        CREATE TABLE detections (
            id INTEGER PRIMARY KEY,
            photo_id INTEGER NOT NULL
        );
        CREATE TABLE predictions (
            id INTEGER PRIMARY KEY,
            detection_id INTEGER NOT NULL,
            classifier_model TEXT NOT NULL,
            labels_fingerprint TEXT NOT NULL,
            species TEXT NOT NULL,
            confidence REAL,
            match_score REAL
        );
        CREATE TABLE classifier_match_scores (
            detection_id INTEGER NOT NULL,
            classifier_model TEXT NOT NULL,
            labels_fingerprint TEXT NOT NULL,
            max_match_score REAL,
            match_margin REAL,
            top_species TEXT,
            label_count INTEGER,
            score_kind TEXT,
            PRIMARY KEY (detection_id, classifier_model, labels_fingerprint)
        );
        CREATE TABLE keywords (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            is_species INTEGER DEFAULT 0,
            type TEXT,
            taxon_id INTEGER
        );
        CREATE TABLE photo_keywords (
            photo_id INTEGER NOT NULL,
            keyword_id INTEGER NOT NULL
        );
        CREATE TABLE taxa (
            id INTEGER PRIMARY KEY,
            rank TEXT
        );
        """
    )
    return conn


def _add_photo_with_keyword(conn, photo_id, species):
    conn.execute("INSERT INTO photos(id) VALUES (?)", (photo_id,))
    cur = conn.execute(
        "INSERT INTO keywords(name, is_species) VALUES (?, 1)", (species,),
    )
    conn.execute(
        "INSERT INTO photo_keywords(photo_id, keyword_id) VALUES (?, ?)",
        (photo_id, cur.lastrowid),
    )


def _add_run(conn, det_id, photo_id, model, fp, top_species,
             max_match_score, score_kind="cosine", candidates=()):
    conn.execute(
        "INSERT INTO detections(id, photo_id) VALUES (?, ?)",
        (det_id, photo_id),
    )
    conn.execute(
        """INSERT INTO classifier_match_scores
             (detection_id, classifier_model, labels_fingerprint,
              max_match_score, top_species, label_count, score_kind)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (det_id, model, fp, max_match_score, top_species,
         max(len(candidates), 1), score_kind),
    )
    for candidate_species, candidate_score in candidates:
        conn.execute(
            """INSERT INTO predictions
                 (detection_id, classifier_model, labels_fingerprint,
                  species, confidence, match_score)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (det_id, model, fp, candidate_species, 0.5, candidate_score),
        )


def test_collect_samples_once_per_run_not_per_candidate():
    """A run with a correct top-1 and four alternative candidates must
    contribute exactly one sample — the run-level max — to the ``correct``
    bucket, not five samples split across correct/incorrect.
    """
    mod = _load_module()
    conn = _seed_conn()
    _add_photo_with_keyword(conn, photo_id=1, species="Robin")
    _add_run(
        conn, det_id=10, photo_id=1, model="bioclip", fp="fp-a",
        top_species="Robin", max_match_score=0.42,
        candidates=[
            ("Robin", 0.42),
            ("Sparrow", 0.18),
            ("Finch", 0.11),
            ("Wren", 0.09),
            ("Warbler", 0.07),
        ],
    )

    buckets = mod.collect(conn)
    key = ("bioclip", "cosine")
    assert key in buckets
    assert buckets[key]["correct"] == [0.42]
    assert buckets[key]["incorrect"] == [], (
        "The four alternative candidates must NOT contribute independent "
        "incorrect samples — the floor is applied per run, not per candidate."
    )


def test_collect_classifies_by_run_top_species_not_per_candidate_hit():
    """A run whose top species is wrong but where the correct species appears
    lower on the candidate list must land in ``incorrect``. The floor governs
    whether the RUN's answer is trusted; if the run's top pick disagrees with
    the keyword, the run is wrong even if the true species scored second.
    """
    mod = _load_module()
    conn = _seed_conn()
    _add_photo_with_keyword(conn, photo_id=1, species="Robin")
    _add_run(
        conn, det_id=10, photo_id=1, model="bioclip", fp="fp-a",
        top_species="Sparrow", max_match_score=0.44,
        candidates=[
            ("Sparrow", 0.44),
            ("Robin", 0.31),
        ],
    )

    buckets = mod.collect(conn)
    key = ("bioclip", "cosine")
    assert buckets[key]["correct"] == []
    assert buckets[key]["incorrect"] == [0.44]


def test_collect_skips_photos_without_species_keywords():
    """Unlabelled photos are not ground truth — counting a disagreement as
    ``incorrect`` there would drag the floor up until it hid real IDs.
    """
    mod = _load_module()
    conn = _seed_conn()
    conn.execute("INSERT INTO photos(id) VALUES (1)")
    _add_run(
        conn, det_id=10, photo_id=1, model="bioclip", fp="fp-a",
        top_species="Robin", max_match_score=0.42,
    )
    assert mod.collect(conn) == {}


def test_collect_groups_by_model_and_score_kind():
    """A cosine floor means nothing applied to logits; the two never pool."""
    mod = _load_module()
    conn = _seed_conn()
    _add_photo_with_keyword(conn, photo_id=1, species="Robin")
    _add_photo_with_keyword(conn, photo_id=2, species="Robin")
    _add_run(
        conn, det_id=10, photo_id=1, model="bioclip", fp="fp-a",
        top_species="Robin", max_match_score=0.42, score_kind="cosine",
    )
    _add_run(
        conn, det_id=20, photo_id=2, model="inat21", fp="fp-b",
        top_species="Robin", max_match_score=3.1, score_kind="logit",
    )

    buckets = mod.collect(conn)
    assert set(buckets.keys()) == {
        ("bioclip", "cosine"), ("inat21", "logit"),
    }
    assert buckets[("bioclip", "cosine")]["correct"] == [0.42]
    assert buckets[("inat21", "logit")]["correct"] == [3.1]


def _add_species_keyword(conn, photo_id, species):
    cur = conn.execute(
        "INSERT INTO keywords(name, is_species) VALUES (?, 1)", (species,),
    )
    conn.execute(
        "INSERT INTO photo_keywords(photo_id, keyword_id) VALUES (?, ?)",
        (photo_id, cur.lastrowid),
    )


def test_collect_excludes_multi_species_photos():
    """A photo with two species keywords cannot serve as ground truth: a
    detection whose top pick happens to equal EITHER keyword would be scored
    ``correct`` here even if the detection actually depicts the OTHER
    species. Those false positives sit at the low end of the correct
    distribution and would pull the fitted floor down. Restrict to photos
    with exactly one confirmed species keyword.
    """
    mod = _load_module()
    conn = _seed_conn()
    # Photo 1: two species keywords, the run's top pick matches one of them.
    # Under the old ``EXISTS`` predicate this would land in ``correct``.
    conn.execute("INSERT INTO photos(id) VALUES (1)")
    _add_species_keyword(conn, 1, "Robin")
    _add_species_keyword(conn, 1, "Yellow-breasted Chat")
    _add_run(
        conn, det_id=10, photo_id=1, model="bioclip", fp="fp-a",
        top_species="Yellow-breasted Chat", max_match_score=0.11,
    )
    # Photo 2: exactly one species keyword; a correct sample.
    _add_photo_with_keyword(conn, photo_id=2, species="Robin")
    _add_run(
        conn, det_id=20, photo_id=2, model="bioclip", fp="fp-a",
        top_species="Robin", max_match_score=0.42,
    )

    buckets = mod.collect(conn)
    key = ("bioclip", "cosine")
    assert buckets[key]["correct"] == [0.42], (
        "The multi-species photo must not contribute — its keyword doesn't "
        "prove which species this detection depicts."
    )
    assert buckets[key]["incorrect"] == []


def test_collect_includes_single_species_photo_with_repeated_keyword():
    """A photo whose 'multiple' species keywords are the same word in
    different casing is still a single-species photo. Case-insensitive
    ``COUNT(DISTINCT LOWER(name))`` keeps it in — losing it would just
    silently shrink the calibration set.
    """
    mod = _load_module()
    conn = _seed_conn()
    conn.execute("INSERT INTO photos(id) VALUES (1)")
    _add_species_keyword(conn, 1, "Robin")
    _add_species_keyword(conn, 1, "robin")  # same species, different case
    _add_run(
        conn, det_id=10, photo_id=1, model="bioclip", fp="fp-a",
        top_species="Robin", max_match_score=0.42,
    )
    buckets = mod.collect(conn)
    assert buckets[("bioclip", "cosine")]["correct"] == [0.42]


def test_floor_within_suppression_cap_never_exceeds_budget():
    """Nearest-rank rounding can exceed a small cap: for n=252 at q=0.01 it
    rounds to index 3, hiding 3/252 = 1.19% of correct rows behind a stated
    1% budget. The suppression-capped floor must never rise above
    ``floor(max_suppression * n)`` strictly-lower samples.
    """
    mod = _load_module()
    values = sorted(float(i) for i in range(252))
    n = len(values)
    for max_suppression in (0.005, 0.01, 0.05, 0.1):
        budget = int(max_suppression * n)  # floor
        threshold = mod._floor_within_suppression_cap(values, max_suppression)
        strictly_lower = sum(1 for v in values if v < threshold)
        assert strictly_lower <= budget, (
            f"At n={n}, cap={max_suppression}: threshold={threshold} hides "
            f"{strictly_lower} samples > budget {budget}"
        )


def test_floor_within_suppression_cap_respects_ties():
    """Equal values at the budget boundary do not each count against the cap
    — a threshold set at their common value hides none of them. The floor
    should advance across the tied block instead of stopping short.
    """
    mod = _load_module()
    values = [0.0, 0.1, 0.1, 0.1, 0.2, 0.3, 0.4]
    threshold = mod._floor_within_suppression_cap(values, 0.2)  # budget=1
    assert threshold == 0.1, (
        "The largest value with strictly-lower count <= 1 is 0.1 (only 0.0 "
        "sits strictly below it), and ties do not each spend the budget."
    )
    strictly_lower = sum(1 for v in values if v < threshold)
    assert strictly_lower == 1


def test_suggest_respects_cap_on_borderline_sample_count():
    """End-to-end regression: with a distribution where nearest-rank
    rounding would round up, ``suggest`` must return a threshold whose
    reported ``suppressed`` fraction is at most the requested cap.
    """
    mod = _load_module()
    correct = [float(i) / 1000 for i in range(1, 253)]  # 252 unique values
    threshold, suppressed, _caught = mod.suggest(correct, [], 0.01)
    assert threshold is not None
    assert suppressed <= 0.01 + 1e-12, (
        f"suggest reported suppressed={suppressed:.6f} > cap 0.01 — "
        "the floor exceeds the advertised suppression budget."
    )
