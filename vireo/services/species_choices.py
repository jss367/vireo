"""Choosing the best photo of a species within a set of photos.

A choice is "this photo is the best of this species among those photos". It
makes the photo one of the species' chosen photos (a representative) and
records one comparison per photo it was chosen over, with who decided and
why. The best photo of any set (a trip, a year, everything) is then read
from the comparisons by ``species_preference.Preferences``, so a choice
made for a trip never claims more than the trip: the trip's best and another
trip's best stay tied until someone compares them.

``choose_best`` is the one writer. It runs inside the caller's transaction,
and removes older comparisons the new choice contradicts so the stored set
never holds a comparison the ranking would ignore.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from species_preference import split_contradictions

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
    """The species' chosen photos within ``scope``, newest selection first.

    Only photos that are still eligible count: visible in the workspace, not
    rejected and still carrying the species. Raises ``ValueError`` on a
    malformed rule tree or an unknown collection.
    """
    canonical = db.keyword_taxa.resolve_species_display(species)
    chosen = db.species_curation.get_representative_lists(
        eligible_only=True, species=canonical, ranked=False,
    ).get(canonical, [])
    in_scope = _resolve_scope(db, scope)
    if in_scope is None:
        return list(chosen)
    return [photo_id for photo_id in chosen if photo_id in in_scope]


def choose_best(
    db: Database, species: str, photo_id: int, *,
    over: Iterable[int] | None = None, scope: PhotoScope | None = None,
    source: str | None = None, reason: str | None = None,
) -> JsonDict:
    """Record ``photo_id`` as the best of ``species`` among a set of photos.

    ``over`` names the photos it was chosen over, and ``scope`` is then
    unused. Without ``over``, the photo is chosen over every other chosen
    photo of the species within ``scope`` (all of them when the scope is
    empty), which is what "set as representative" means on a page showing
    that scope. A scoped choice whose ``photo_id`` is not itself in the
    scope is refused, so the stored comparisons never claim that an
    out-of-scope photo beat the scope's contenders.

    The caller validates the photo and owns the transaction. Returns the
    choice with ``over`` (the photos it now beats by this choice) and
    ``superseded`` (older comparisons this choice contradicted and removed).
    """
    canonical = db.keyword_taxa.resolve_species_display(species)
    if over is None:
        in_scope = _resolve_scope(db, scope)
        if in_scope is not None and photo_id not in in_scope:
            raise ValueError(
                f"photo {photo_id} is not in the supplied scope",
            )
        chosen = db.species_curation.get_representative_lists(
            eligible_only=True, species=canonical, ranked=False,
        ).get(canonical, [])
        losers = (
            list(chosen) if in_scope is None
            else [pid for pid in chosen if pid in in_scope]
        )
    else:
        losers = list(dict.fromkeys(int(pid) for pid in over))
    losers = [pid for pid in losers if pid != photo_id]
    db.set_species_representative(canonical, photo_id)
    choice_id = db.species_choices.record(
        canonical, photo_id, losers, source=source, reason=reason,
    )
    _kept, contradicted = split_contradictions(db.species_choices.comparisons())
    db.species_choices.delete_comparisons(contradicted)
    return {
        "choice_id": choice_id,
        "species": canonical,
        "photo_id": photo_id,
        "over": losers,
        "source": source,
        "reason": reason,
        "superseded": [
            {"better_photo_id": c.better, "worse_photo_id": c.worse}
            for c in contradicted
        ],
    }
