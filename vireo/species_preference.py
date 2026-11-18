"""Which of a species' chosen photos leads within a set of photos.

A species' chosen photos (its representatives) are compared pairwise: "this
one over that one". The comparisons form a partial order, so the best photo of
a trip, a year or a lifetime is the same question asked of a different set:
the chosen photo in the set that no other chosen photo in the set beats.

Two chosen photos nobody has compared are tied. A tie is reported as a tie
(``Ranking.leaders`` has more than one photo) and is broken provisionally by
the caller's order, which every caller passes newest choice first, so the
photo shown for a scope is the same on every surface that asks.

A comparison is between two photos, whatever species it was made for: it
counts for any species both photos are chosen for. Renaming a species or
relabeling photos therefore needs no bookkeeping here; a comparison whose
photos no longer share a species is inert until they do again.

Pure: no database, config or I/O. ``repositories.species_choices`` stores the
comparisons and ``services.species_choices`` records them.
"""

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any


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


def _reaches(
    beats: dict[int, set[int]], start: int, target: int,
    through: frozenset[int] | None = None,
) -> bool:
    """Whether ``start`` beats ``target`` directly or through other photos.

    When ``through`` is given, a transitive step must land on a photo in it;
    a direct edge to ``target`` is always allowed. Callers that rank one
    species pass the species' chosen photos there, so a comparison through
    a photo no longer chosen for the species is inert (``repositories.
    species_choices`` module docstring). ``None`` keeps every photo
    reachable, which is what ``split_contradictions`` needs: a cycle across
    species stays a cycle.
    """
    seen = {start}
    stack = [start]
    while stack:
        for worse in beats.get(stack.pop(), ()):
            if worse == target:
                return True
            if worse not in seen and (through is None or worse in through):
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


class Preferences:
    """Every recorded comparison, ready to rank any set of chosen photos."""

    def __init__(self, comparisons: Iterable[Comparison] = ()) -> None:
        kept, _contradicted = split_contradictions(comparisons)
        self._beats: dict[int, set[int]] = {}
        for comparison in kept:
            self._beats.setdefault(comparison.better, set()).add(comparison.worse)

    def beats(
        self, better: int, worse: int,
        chosen_for_species: Iterable[int] | None = None,
    ) -> bool:
        """Whether ``better`` is recorded over ``worse``, directly or through
        other photos (``chosen_for_species`` limits those, as in ``rank``)."""
        through = (
            None if chosen_for_species is None
            else frozenset(chosen_for_species)
        )
        return better != worse and _reaches(self._beats, better, worse, through)

    def rank(
        self, contenders: Sequence[int],
        chosen_for_species: Iterable[int] | None = None,
    ) -> Ranking:
        """Rank ``contenders``, given in tie-break order.

        A comparison counts through photos outside the set: with A over X and
        X over B, A beats B wherever both appear, whether or not X does — as
        long as X is still chosen for the species being ranked. Callers that
        know the species pass its chosen photos as ``chosen_for_species``,
        so a stored comparison through a photo no longer chosen for it (a
        relabel, a cleared representative) stays inert until it is chosen
        again. ``None`` lets any stored edge carry the chain. Photos that
        are not contenders never lead, whatever comparisons name them.
        """
        ordered = list(dict.fromkeys(contenders))
        if len(ordered) < 2 or not self._beats:
            return Ranking(tuple(ordered), tuple(ordered))
        through = (
            None if chosen_for_species is None
            else frozenset(chosen_for_species)
        )
        beaten_by = {
            photo: {
                other for other in ordered
                if other != photo and _reaches(self._beats, other, photo, through)
            }
            for photo in ordered
        }
        leaders = tuple(photo for photo in ordered if not beaten_by[photo])
        placed: list[int] = []
        remaining = list(ordered)
        while remaining:
            done = set(placed)
            # The kept comparisons are acyclic, so some remaining photo is
            # beaten only by photos already placed.
            photo = next(p for p in remaining if beaten_by[p] <= done)
            placed.append(photo)
            remaining.remove(photo)
        return Ranking(tuple(placed), leaders)


def rank_contenders(
    contenders: Sequence[int], comparisons: Iterable[Comparison],
    chosen_for_species: Iterable[int] | None = None,
) -> Ranking:
    """Rank ``contenders`` (in tie-break order) by ``comparisons``."""
    return Preferences(comparisons).rank(contenders, chosen_for_species)


def choice_note(
    notes: Mapping[int, Sequence[Mapping[str, Any]]], species: str, photo_id: int,
) -> Mapping[str, Any] | None:
    """Who highlighted ``photo_id`` for ``species`` and why: its newest
    choice recorded for that species that names a source or a reason, else
    its newest choice for the species, else None.

    A reorder by hand carries neither a source nor a reason, so it does not
    hide the reason an earlier choice gave for the photo. A photo can be a
    highlight of more than one species, so a choice recorded for another
    species (with its own source and reason) is never attributed to the one
    being rendered.

    ``notes`` maps a photo to its choices, oldest first.
    """
    newest = None
    for note in reversed(notes.get(photo_id) or ()):
        if note.get("species") != species:
            continue
        if note.get("source") or note.get("reason"):
            return note
        newest = newest or note
    return newest
