"""Which of a species' chosen photos leads within a set of photos.

A species' chosen photos (its representatives) are compared pairwise: "this
one over that one". The comparisons form a partial order, so the best photo of
a trip, a year or a lifetime is the same question asked of a different set:
the chosen photo in the set that no other chosen photo in the set beats.

Two chosen photos nobody has compared are tied. A tie is reported as a tie
(``Ranking.leaders`` has more than one photo) and is broken provisionally by
the caller's order, which every caller passes newest choice first, so the
photo shown for a scope is the same on every surface that asks.

Pure: no database, config or I/O. ``repositories.species_choices`` stores the
comparisons and ``services.species_choices`` records them.
"""

from collections.abc import Iterable, Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class Comparison:
    """``better`` was chosen over ``worse`` by the choice ``choice_id``.

    Choice ids grow with time, so a higher id is a newer decision.
    """

    choice_id: int
    better: int
    worse: int


@dataclass(frozen=True)
class Ranking:
    """The contenders of one species within one set of photos, best first.

    ``leaders`` are the contenders no other contender in the set beats, in the
    same order; ``order[0] == leaders[0]``. One leader means the comparisons
    settle the set; several mean those photos have never been compared.
    """

    order: tuple[int, ...]
    leaders: tuple[int, ...]

    @property
    def tied(self) -> bool:
        return len(self.leaders) > 1


def _reaches(beats: dict[int, set[int]], start: int, target: int) -> bool:
    """Whether ``start`` beats ``target`` directly or through other photos."""
    seen = {start}
    stack = [start]
    while stack:
        for worse in beats.get(stack.pop(), ()):
            if worse == target:
                return True
            if worse not in seen:
                seen.add(worse)
                stack.append(worse)
    return False


def split_contradictions(
    comparisons: Iterable[Comparison],
) -> tuple[list[Comparison], list[Comparison]]:
    """``(kept, contradicted)``: the newest decisions win.

    Comparisons are read newest first, and one that would make a photo beat
    itself through the comparisons already kept is contradicted by them. A
    comparison never contradicts another from the same choice, since one
    choice names a single winner.
    """
    kept: list[Comparison] = []
    contradicted: list[Comparison] = []
    beats: dict[int, set[int]] = {}
    for comparison in sorted(comparisons, key=lambda c: c.choice_id, reverse=True):
        if comparison.better == comparison.worse or _reaches(
            beats, comparison.worse, comparison.better,
        ):
            contradicted.append(comparison)
            continue
        beats.setdefault(comparison.better, set()).add(comparison.worse)
        kept.append(comparison)
    return kept, contradicted


def rank_contenders(
    contenders: Sequence[int], comparisons: Iterable[Comparison],
) -> Ranking:
    """Rank ``contenders`` (in tie-break order) by ``comparisons``.

    A comparison counts through photos outside the set: with A over X and X
    over B, A beats B wherever both appear, whether or not X does. Photos that
    are not contenders never lead, whatever comparisons name them.
    """
    ordered = list(dict.fromkeys(contenders))
    if len(ordered) < 2:
        return Ranking(tuple(ordered), tuple(ordered))
    kept, _contradicted = split_contradictions(comparisons)
    beats: dict[int, set[int]] = {}
    for comparison in kept:
        beats.setdefault(comparison.better, set()).add(comparison.worse)
    beaten_by = {
        photo: {
            other for other in ordered
            if other != photo and _reaches(beats, other, photo)
        }
        for photo in ordered
    }
    leaders = tuple(photo for photo in ordered if not beaten_by[photo])
    placed: list[int] = []
    remaining = list(ordered)
    while remaining:
        done = set(placed)
        # The kept comparisons are acyclic, so some remaining photo is beaten
        # only by photos already placed.
        photo = next(p for p in remaining if beaten_by[p] <= done)
        placed.append(photo)
        remaining.remove(photo)
    return Ranking(tuple(placed), leaders)
