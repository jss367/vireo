import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from labels_fingerprint import LEGACY_SENTINEL, TOL_SENTINEL, compute_fingerprint


def test_fingerprint_is_stable_under_ordering_and_duplicates():
    a = compute_fingerprint(["Bald Eagle", "American Robin", "Bald Eagle"])
    b = compute_fingerprint(["American Robin", "Bald Eagle"])
    assert a == b
    assert len(a) == 12  # sha256 hex prefix


def test_fingerprint_differs_on_different_sets():
    a = compute_fingerprint(["Bald Eagle", "American Robin"])
    b = compute_fingerprint(["Bald Eagle", "Steller's Jay"])
    assert a != b


def test_tol_sentinel_when_no_labels():
    assert compute_fingerprint(None) == TOL_SENTINEL
    assert compute_fingerprint([]) == TOL_SENTINEL


def test_sentinels_are_fixed_strings():
    assert TOL_SENTINEL == "tol"
    assert LEGACY_SENTINEL == "legacy"
