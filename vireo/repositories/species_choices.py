"""Persistence for species choices: which chosen photo was picked over which.

Owns ``species_choices`` and ``species_choice_losers``. A choice is one
decision, "this photo is the best of this species among those photos", with
who decided (``source``) and why (``reason``). Each loser row is one
comparison, the chosen photo over that photo. A choice with no losers records
only who chose the photo and why.

Catalog-wide, like ``species_representatives``: a choice is about the photos,
not about the workspace it was made in. ``species`` holds the spelling the
representative rows use (``KeywordTaxonRepository.resolve_species_display``),
which the caller resolves before writing.

Rows are kept when a photo stops carrying the species (a relabel, a removed
keyword): a comparison counts only between photos that are both chosen for
the species at the time of the read, so the rows of a relabeled photo are
inert until an undo makes them true again. Deleting a photo cascades.

What stays elsewhere: which photos are chosen at all
(``species_representatives``, in ``repositories.species_curation``), the
ranking the comparisons imply (``species_preference``) and the write workflow
that removes contradicted comparisons (``services.species_choices``).

No method here commits: the caller owns the transaction.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable, Sequence
from typing import Any

from species_preference import Comparison


class SpeciesChoiceRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def record(
        self, species: str, photo_id: int, loser_ids: Iterable[int],
        source: str | None = None, reason: str | None = None,
    ) -> int:
        """Record one choice of ``photo_id`` over ``loser_ids``; return its id."""
        cur = self.conn.execute(
            """INSERT INTO species_choices (species, photo_id, source, reason)
               VALUES (?, ?, ?, ?)""",
            (species, photo_id, source, reason),
        )
        choice_id = int(cur.lastrowid or 0)
        losers = sorted({int(pid) for pid in loser_ids} - {int(photo_id)})
        self.conn.executemany(
            "INSERT INTO species_choice_losers (choice_id, photo_id) VALUES (?, ?)",
            [(choice_id, pid) for pid in losers],
        )
        return choice_id

    def comparisons(
        self, species_names: Iterable[str] | None = None,
    ) -> dict[str, list[Comparison]]:
        """``{species: [Comparison, ...]}``, every species or the named ones."""
        species_filter = ""
        params: list[Any] = []
        if species_names is not None:
            species_filter = "WHERE c.species IN (SELECT value FROM json_each(?))"
            params.append(json.dumps(sorted(set(species_names))))
        result: dict[str, list[Comparison]] = {}
        for row in self.conn.execute(
            f"""SELECT c.species, c.id, c.photo_id, l.photo_id AS loser_id
                FROM species_choices c
                JOIN species_choice_losers l ON l.choice_id = c.id
                {species_filter}
                ORDER BY c.id, l.photo_id""",
            params,
        ):
            result.setdefault(row["species"], []).append(
                Comparison(row["id"], row["photo_id"], row["loser_id"])
            )
        return result

    def notes(
        self, species_names: Iterable[str] | None = None,
    ) -> dict[tuple[str, int], dict[str, Any]]:
        """``{(species, photo_id): note}``: the newest choice of each photo.

        A note carries ``source``, ``reason`` and ``chosen_at``. A chosen
        photo with no note was chosen before choices were recorded.
        """
        species_filter = ""
        params: list[Any] = []
        if species_names is not None:
            species_filter = "WHERE species IN (SELECT value FROM json_each(?))"
            params.append(json.dumps(sorted(set(species_names))))
        result: dict[tuple[str, int], dict[str, Any]] = {}
        for row in self.conn.execute(
            f"""SELECT species, photo_id, source, reason, created_at
                FROM species_choices
                {species_filter}
                ORDER BY id""",
            params,
        ):
            result[(row["species"], row["photo_id"])] = {
                "source": row["source"],
                "reason": row["reason"],
                "chosen_at": row["created_at"],
            }
        return result

    def delete_comparisons(self, comparisons: Sequence[Comparison]) -> int:
        """Remove single comparisons, keeping the choices they belonged to."""
        cur = self.conn.executemany(
            "DELETE FROM species_choice_losers WHERE choice_id = ? AND photo_id = ?",
            [(c.choice_id, c.worse) for c in comparisons],
        )
        return cur.rowcount

    def clear_species(self, species: str) -> int:
        """Forget every choice made for ``species``."""
        cur = self.conn.execute(
            "DELETE FROM species_choices WHERE species = ?", (species,),
        )
        return cur.rowcount
