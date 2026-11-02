"""Tagging a photo with a keyword, and removing the tag: the entry points for keyword associations.

``tag_photo`` is the one way code outside the data layer creates or
re-stamps a ``photo_keywords`` association. Its ``source`` defaults to
``KEYWORD_SOURCE_MANUAL``, the fail-safe that
``test_keyword_provenance_contract`` pins: a call site that forgets to
declare provenance leaves an association retirement passes refuse to
delete, rather than one they silently erase. The contract reads every call
site's ``source`` here and at the repository writer behind it
(``KeywordProvenanceRepository.tag``), so declining manual authorship stays
an explicit, allowlisted act.

Both functions snapshot species evidence for an undoable edit when asked
(``db.species_provenance``) and never commit: the caller's unit of work owns
the transaction.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from repositories.keyword_source import KEYWORD_SOURCE_MANUAL

if TYPE_CHECKING:
    from db import Database


def tag_photo(
    db: Database,
    photo_id: int,
    keyword_id: int,
    source: str | None = KEYWORD_SOURCE_MANUAL,
    *,
    provenance: dict[str, Any] | None = None,
    species_provenance: str | None = None,
    _record_provenance: bool = False,
) -> None:
    """Associate a keyword with a photo.

    Args:
        source: Durable provenance for the association. The stamp lives on
                the row, so authorship survives ``prune_edit_history``
                discarding the ``keyword_add`` entry.

                Defaults to ``KEYWORD_SOURCE_MANUAL`` because that is the
                fail-safe answer: a call site that forgets to declare
                provenance leaves an association that retirement passes
                refuse to delete, rather than one they silently erase.
                Only the sidecar readers (scanner, XMP reconcile) may pass
                ``KEYWORD_SOURCE_UNKNOWN``, and they must do it
                explicitly — the contract test enumerates them.

                An existing stamp is never downgraded: the upsert stores
                the lattice max of the existing and incoming values, so
                re-tagging with ``KEYWORD_SOURCE_UNKNOWN`` keeps a
                recorded ``'manual'`` (and a future ``'accept'`` re-tag
                would too).
        _record_provenance: Capture attribution only for an undoable edit.
                Import and merge writes leave this false.
        provenance: dict of species evidence the caller is recording
                deliberately (a reviewer's identification). Snapshotted
                for undo and set after the row is tagged, so it survives
                the ON CONFLICT fold that would otherwise keep an
                existing value.
        species_provenance: pre-serialized evidence JSON forwarded during
                a merge (loser → winner). Unlike ``provenance`` it does
                not snapshot or overwrite -- the shared conflict clause
                keeps the surviving row's evidence over the incoming
                one, so the loser's row is only applied when the winner
                had none.

    Does not commit: the caller's unit of work owns the transaction.
    """
    if provenance is not None and _record_provenance:
        db.species_provenance.remember_before(photo_id, keyword_id, force=True)
    db.keyword_provenance.tag(
        photo_id, keyword_id, source=source,
        species_provenance=species_provenance,
    )
    if provenance is not None:
        db.species_provenance.set(photo_id, keyword_id, provenance)


def untag_photo(
    db: Database, photo_id: int, keyword_id: int, *, _record_history: bool = False,
) -> None:
    """Remove a keyword association from a photo.

    Does not commit: the caller's unit of work owns the transaction.

    Args:
        _record_history: Opt in only when the caller will record an
                 undoable keyword/species edit. Reconciliation, merge and
                 replay writers leave this false so no stale snapshots
                 can accumulate or leak into a later edit.
    """
    if _record_history:
        db.species_provenance.remember_before(photo_id, keyword_id)
    db.photo_keywords.untag(photo_id, keyword_id)
