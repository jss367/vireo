"""A bucket's metric ranks: the sorted lookup must equal the plain count."""

import random

from highlights_payload import _Ranks


def _counted(value, values):
    """The rank by counting: the share below, ties counting half."""
    valid = [v for v in values if v is not None]
    if value is None:
        return 0.0
    if len(valid) <= 1:
        return 0.5 if valid else 0.0
    below = sum(1 for v in valid if v < value)
    equal = sum(1 for v in valid if v == value)
    return (below + 0.5 * equal) / len(valid)


def test_sorted_ranks_equal_counted_ranks():
    rng = random.Random(7)
    for size in (0, 1, 2, 5, 40):
        values = [
            None if rng.random() < 0.2 else rng.choice([0.0, 0.25, 1.5, rng.random()])
            for _ in range(size)
        ]
        ranks = _Ranks(values)
        for value in [*values, None, -1.0, 0.25, 99.0]:
            assert ranks.rank01(value) == _counted(value, values)


def test_ranks_are_falsy_only_without_values():
    assert not _Ranks([])
    assert not _Ranks([None, None])
    assert _Ranks([None, 0.0])
