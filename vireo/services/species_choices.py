"""A species' highlights: adding, ordering and removing them.

A highlight is a photo chosen to show for a species, catalog-wide. Their
order is not stored as numbers: every action here records a choice, "this
photo ahead of (or behind) those photos", with who decided and why, and the
order within any set of photos (a trip, a year, everything) is read from
those comparisons by ``species_preference.Preferences``. So an action on a
page showing a trip never claims more than the trip: the trip's first
highlight and another trip's stay tied until someone compares them.

``choose_best`` (first among the highlights in a scope), ``add_highlight``
(next after them), ``move_highlight`` (one place up or down) and
``remove_highlight`` are the writers. Each runs inside the caller's
transaction and removes older comparisons the new choice contradicts, so
the stored set never holds a comparison the ranking would ignore.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from species_preference import Preferences, split_contradictions

if TYPE_CHECKING:
    from collections.abc import Iterable

    from db import Database
    from services.service_types import JsonDict

#: Bounds on the provenance a choice may carry, the same as a flag's.
CHOICE_SOURCE_MAX_CHARS = 60
CHOICE_REASON_MAX_CHARS = 300
#: The most photos one choice may name as beaten.
MAX_CHOICE_OVER = 500


@dataclass(frozen=True)
class PhotoScope:
    """A set of photos in the active workspace: a folder subtree, a
    collection and a universal-filter rule tree, each optional, combined
    with AND. The empty scope is every photo the workspace can see."""

    folder_id: int | None = None
    collection_id: int | None = None
    # A rule tree is caller-shaped JSON (a list of rules or a nested group),
    # validated by the repository that turns it into SQL.
    rules: Any = None

    @property
    def is_everything(self) -> bool:
        return (
            self.folder_id is None
            and self.collection_id is None
            and self.rules is None
        )

    def as_json(self) -> JsonDict:
        return {
            "folder_id": self.folder_id,
            "collection_id": self.collection_id,
            "rules": self.rules,
        }


def _resolve_scope(
    db: Database, scope: PhotoScope | None,
) -> set[int] | None:
    """The photo ids ``scope`` matches, or ``None`` when it is unlimited.

    Resolved even when no scoped photo is chosen yet, so a scope that names
    no real set of photos is refused instead of being recorded as "over
    nothing". Raises ``ValueError`` on a malformed rule tree or an unknown
    collection.
    """
    if scope is None or scope.is_everything:
        return None
    return set(db.collections.query_photo_ids(
        scope.rules if scope.rules is not None else [],
        collection_id=scope.collection_id,
        folder_id=scope.folder_id,
    ))


def contenders_in_scope(
    db: Database, species: str, scope: PhotoScope | None = None,
) -> list[int]:
    """The species' highlights within ``scope``, newest selection first.

    Only photos that are still eligible count: visible in the workspace, not
    rejected and still in the species' Highlights bucket. Raises
    ``ValueError`` on a malformed rule tree or an unknown collection.
    """
    canonical = db.keyword_taxa.resolve_species_display(species)
    in_scope = _resolve_scope(db, scope)
    chosen = _eligible_highlights(db, canonical)
    if in_scope is None:
        return chosen
    return [photo_id for photo_id in chosen if photo_id in in_scope]


def _eligible_highlights(db: Database, canonical: str) -> list[int]:
    return list(
        db.species_curation.get_highlights(canonical, eligible_only=True)
        .get(canonical, [])
    )


def _traversal_set(db: Database, canonical: str) -> set[int]:
    """The photos a comparison may chain through for ``canonical``: its
    highlights here, and the eligible ones only another workspace shows."""
    elsewhere = db.species_curation.get_globally_eligible_representatives(
        canonical,
    ).get(canonical, [])
    return {*_eligible_highlights(db, canonical), *elsewhere}


def ranked_in_scope(
    db: Database, species: str, scope: PhotoScope | None = None,
) -> list[int]:
    """The species' highlights within ``scope``, best first: the order a
    page showing that scope numbers them in."""
    canonical = db.keyword_taxa.resolve_species_display(species)
    contenders = contenders_in_scope(db, canonical, scope)
    if len(contenders) < 2:
        return contenders
    preferences = Preferences(db.species_choices.comparisons())
    return list(
        preferences.rank(contenders, _traversal_set(db, canonical)).order
    )


def _remove_contradicted(db: Database) -> list[JsonDict]:
    """Delete the older comparisons the newest choice contradicts."""
    _kept, contradicted = split_contradictions(db.species_choices.comparisons())
    db.species_choices.delete_comparisons(contradicted)
    return [
        {"better_photo_id": c.better, "worse_photo_id": c.worse}
        for c in contradicted
    ]


def choose_best(
    db: Database, species: str, photo_id: int, *,
    over: Iterable[int] | None = None, scope: PhotoScope | None = None,
    source: str | None = None, reason: str | None = None,
) -> JsonDict:
    """Record ``photo_id`` as the best of ``species`` among a set of photos.

    ``over`` names the photos it was chosen over, and ``scope`` is then
    unused. Without ``over``, the photo is chosen over every other highlight
    of the species within ``scope`` (all of them when the scope is empty),
    which is what "make this the first highlight" means on a page showing
    that scope. A scoped choice whose ``photo_id`` is not itself in the
    scope is refused, so the stored comparisons never claim that an
    out-of-scope photo beat the scope's contenders.

    The photo becomes a highlight if it was not one. The caller validates
    the photo and owns the transaction. Returns the choice with ``over``
    (the photos it now beats by this choice) and ``superseded`` (older
    comparisons this choice contradicted and removed).
    """
    canonical = db.keyword_taxa.resolve_species_display(species)
    if over is None:
        in_scope = _resolve_scope(db, scope)
        if in_scope is not None and photo_id not in in_scope:
            raise ValueError(
                f"photo {photo_id} is not in the supplied scope",
            )
        chosen = _eligible_highlights(db, canonical)
        losers = (
            chosen if in_scope is None
            else [pid for pid in chosen if pid in in_scope]
        )
    else:
        losers = list(dict.fromkeys(int(pid) for pid in over))
    losers = [pid for pid in losers if pid != photo_id]
    db.set_species_representative(canonical, photo_id)
    choice_id = db.species_choices.record(
        canonical, photo_id, losers, source=source, reason=reason,
    )
    return {
        "choice_id": choice_id,
        "species": canonical,
        "photo_id": photo_id,
        "over": losers,
        "source": source,
        "reason": reason,
        "superseded": _remove_contradicted(db),
    }


def add_highlight(
    db: Database, species: str, photo_id: int, *,
    scope: PhotoScope | None = None,
    source: str | None = None, reason: str | None = None,
) -> JsonDict:
    """Make ``photo_id`` a highlight of ``species``, after the ones in ``scope``.

    The new highlight is recorded behind every highlight already in the
    scope, so it takes the next number on a page showing that scope and
    claims nothing about highlights outside it. A photo that is already a highlight
    of the species is left where it is. Returns ``added`` and ``after`` (the
    photos it was recorded behind).
    """
    canonical = db.keyword_taxa.resolve_species_display(species)
    in_scope = _resolve_scope(db, scope)
    result: JsonDict = {
        "species": canonical, "photo_id": photo_id,
        "added": False, "after": [], "superseded": [],
    }
    if photo_id in db.species_curation.get_highlights(canonical).get(canonical, []):
        return result
    present = [
        pid for pid in _eligible_highlights(db, canonical)
        if in_scope is None or pid in in_scope
    ]
    db.set_species_representative(canonical, photo_id)
    db.species_choices.record(
        canonical, photo_id, source=source, reason=reason, winner_ids=present,
    )
    result.update(added=True, after=present, superseded=_remove_contradicted(db))
    return result


def move_highlight(
    db: Database, species: str, photo_id: int, direction: str, *,
    neighbor_id: int | None = None, scope: PhotoScope | None = None,
    source: str | None = None, reason: str | None = None,
) -> JsonDict | None:
    """Move a highlight one place up or down among the highlights in ``scope``.

    Moving up records the photo ahead of the highlight directly above it;
    moving down records it behind the one directly below. ``neighbor_id``
    names that highlight when the page knows which one it showed there;
    otherwise it is read from the order within ``scope``. Moving the first
    highlight up records it ahead of the highlights tied with it, which is
    how a tie is settled in its favor.

    Only that pair changes places. What the two photos already stood ahead
    of or behind, in the scope, is recorded again with the move, because
    the comparison the move replaces may have been what carried it (first
    over second over third says first over third only through second).

    Returns None when the photo is not a highlight in the scope, else
    ``moved`` and ``over`` / ``after``.
    """
    canonical = db.keyword_taxa.resolve_species_display(species)
    ranked = ranked_in_scope(db, canonical, scope)
    if photo_id not in ranked:
        return None
    place = ranked.index(photo_id)
    result: JsonDict = {
        "species": canonical, "photo_id": photo_id, "direction": direction,
        "moved": False, "over": [], "after": [], "superseded": [],
    }
    preferences = Preferences(db.species_choices.comparisons())
    through = _traversal_set(db, canonical)
    if neighbor_id is not None:
        if neighbor_id == photo_id or neighbor_id not in _eligible_highlights(db, canonical):
            raise ValueError("neighbor_id is not another highlight of that species")
        others = [neighbor_id]
    elif direction == "up" and place == 0:
        others = [
            pid for pid in ranked[1:]
            if not any(preferences.beats(other, pid, through) for other in ranked)
        ]
    else:
        target = place - 1 if direction == "up" else place + 1
        others = ranked[target:target + 1] if 0 <= target < len(ranked) else []
    if not others:
        return result
    passed = set(others)
    ahead = [
        pid for pid in ranked
        if pid not in passed and preferences.beats(pid, photo_id, through)
    ]
    behind = [
        pid for pid in ranked
        if pid not in passed and preferences.beats(photo_id, pid, through)
    ]
    # What each passed photo stood against before, the moved photo aside.
    standing = {
        other: (
            [p for p in ranked if p != photo_id and preferences.beats(p, other, through)],
            [p for p in ranked if p != photo_id and preferences.beats(other, p, through)],
        )
        for other in others
    }
    if direction == "up":
        db.species_choices.record(
            canonical, photo_id, [*others, *behind],
            source=source, reason=reason, winner_ids=ahead,
        )
        result["over"] = others
    else:
        db.species_choices.record(
            canonical, photo_id, behind,
            source=source, reason=reason, winner_ids=[*others, *ahead],
        )
        result["after"] = others
    superseded = _remove_contradicted(db)
    preferences = Preferences(db.species_choices.comparisons())
    for other, (above, below) in standing.items():
        lost_above = [p for p in above if not preferences.beats(p, other, through)]
        lost_below = [p for p in below if not preferences.beats(other, p, through)]
        if lost_above or lost_below:
            db.species_choices.record(
                canonical, other, lost_below, winner_ids=lost_above,
            )
    result.update(moved=True, superseded=superseded)
    return result


def remove_highlight(db: Database, species: str, photo_id: int) -> int:
    """Take ``photo_id`` off the species' highlights, keeping the order of
    the rest. Returns the number of highlight rows removed."""
    return db.species_curation.remove_highlight(species, photo_id)
