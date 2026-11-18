"""Persistence for species choices: how a species' highlights were ordered.

Owns ``species_choices``, ``species_choice_losers`` and
``species_choice_winners``. A choice is one decision about one photo of a
species, with who decided (``source``) and why (``reason``). Its loser rows
are the photos it was placed ahead of and its winner rows the photos it was
placed behind; each row is one comparison. A choice with neither records
only who highlighted the photo and why.

Catalog-wide, like ``species_representatives`` (the highlights themselves):
a choice is about the photos, not about the workspace it was made in.
``species`` holds the spelling the highlight rows use
(``KeywordTaxonRepository.resolve_species_display``), which the caller
resolves before writing.

A comparison is between two photos and counts for any species both are
highlights of; ``species`` on a choice records what was being chosen. So a
relabel and its undo need no bookkeeping here: the rows of photos that no
longer share a species are inert until they share one again. Deleting a
photo cascades.

What stays elsewhere: which photos are highlights at all
(``species_representatives``, in ``repositories.species_curation``), the
ranking the comparisons imply (``species_preference``) and the write workflow
that removes contradicted comparisons (``services.species_choices``).

No method here commits: the caller owns the transaction.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from species_preference import Comparison, split_contradictions


class SpeciesChoiceRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def record(
        self, species: str, photo_id: int, loser_ids: Iterable[int] = (),
        source: str | None = None, reason: str | None = None,
        *, winner_ids: Iterable[int] = (),
    ) -> int:
        """Record one choice about ``photo_id``; return its id.

        The photo is placed ahead of ``loser_ids`` and behind ``winner_ids``.
        """
        cur = self.conn.execute(
            """INSERT INTO species_choices (species, photo_id, source, reason)
               VALUES (?, ?, ?, ?)""",
            (species, photo_id, source, reason),
        )
        choice_id = int(cur.lastrowid or 0)
        losers = {int(pid) for pid in loser_ids} - {int(photo_id)}
        winners = {int(pid) for pid in winner_ids} - {int(photo_id)} - losers
        self.conn.executemany(
            "INSERT INTO species_choice_losers (choice_id, photo_id) VALUES (?, ?)",
            [(choice_id, pid) for pid in sorted(losers)],
        )
        self.conn.executemany(
            "INSERT INTO species_choice_winners (choice_id, photo_id) VALUES (?, ?)",
            [(choice_id, pid) for pid in sorted(winners)],
        )
        return choice_id

    def comparisons(self) -> list[Comparison]:
        """Every recorded comparison, oldest choice first.

        All of them, whatever species each was made for: a comparison is
        between two photos, and the ranking follows it through photos outside
        the set being ranked.
        """
        return [
            Comparison(row["choice_id"], row["better"], row["worse"])
            for row in self.conn.execute(
                """SELECT c.id AS choice_id, c.photo_id AS better,
                          l.photo_id AS worse
                   FROM species_choices c
                   JOIN species_choice_losers l ON l.choice_id = c.id
                   UNION ALL
                   SELECT c.id, w.photo_id, c.photo_id
                   FROM species_choices c
                   JOIN species_choice_winners w ON w.choice_id = c.id
                   ORDER BY 1, 2, 3"""
            )
        ]

    def notes(self) -> dict[int, list[dict[str, Any]]]:
        """``{photo_id: [note, ...]}``: each highlight's choices, oldest
        first, each with ``species``, ``source``, ``reason`` and
        ``chosen_at``. Read one with ``species_preference.choice_note``."""
        result: dict[int, list[dict[str, Any]]] = {}
        for row in self.conn.execute(
            """SELECT species, photo_id, source, reason, created_at
               FROM species_choices
               ORDER BY id"""
        ):
            result.setdefault(row["photo_id"], []).append({
                "species": row["species"],
                "source": row["source"],
                "reason": row["reason"],
                "chosen_at": row["created_at"],
            })
        return result

    def delete_comparisons(self, comparisons: Sequence[Comparison]) -> int:
        """Remove single comparisons, keeping the choices they belonged to.

        A comparison is a loser row when its better photo is the choice's
        own, and a winner row otherwise; the other statement matches nothing.
        """
        losers = self.conn.executemany(
            "DELETE FROM species_choice_losers WHERE choice_id = ? AND photo_id = ?",
            [(c.choice_id, c.worse) for c in comparisons],
        )
        winners = self.conn.executemany(
            "DELETE FROM species_choice_winners WHERE choice_id = ? AND photo_id = ?",
            [(c.choice_id, c.better) for c in comparisons],
        )
        return losers.rowcount + winners.rowcount

    def forget_photo(self, species: str, photo_id: int) -> None:
        """Drop ``photo_id`` from the comparisons made for ``species``.

        Comparisons between this species' current highlights count whatever
        species they were recorded for. Every photo recorded ahead of it is
        first recorded ahead of the photos it was itself ahead of, so removing
        the middle of a chain (first over second over third) leaves first over
        third instead of an unsettled pair. Then its own choices for the species
        and the comparisons other choices for the species made with it are deleted,
        so nothing revives if the photo is highlighted again.
        """
        ahead: set[int] = set()
        behind: set[int] = set()
        highlights = {
            row["photo_id"] for row in self.conn.execute(
                "SELECT photo_id FROM species_representatives WHERE species = ?",
                (species,),
            )
        }
        for comparison in self.comparisons():
            better, worse = comparison.better, comparison.worse
            if better not in highlights or worse not in highlights:
                continue
            if worse == photo_id:
                ahead.add(better)
            elif better == photo_id:
                behind.add(worse)
        for better in sorted(ahead - behind):
            self.record(species, better, sorted(behind - ahead - {better}))
        for table in ("species_choice_losers", "species_choice_winners"):
            self.conn.execute(
                f"""DELETE FROM {table}
                    WHERE photo_id = ?
                      AND choice_id IN (SELECT id FROM species_choices WHERE species = ?)""",
                (photo_id, species),
            )
        self.conn.execute(
            "DELETE FROM species_choices WHERE photo_id = ? AND species = ?",
            (photo_id, species),
        )

    def remap_photos(self, mapping: Mapping[int, int | None]) -> None:
        """Carry highlights and their choices onto surviving catalog photos."""
        comparisons_changed = False
        for source, target in mapping.items():
            if target is None or source == target:
                continue
            self.conn.execute(
                """INSERT INTO species_representatives
                       (species, photo_id, selected_order, created_at, updated_at)
                   SELECT species, ?, selected_order, created_at, updated_at
                   FROM species_representatives WHERE photo_id = ?
                   ON CONFLICT(species, photo_id) DO UPDATE SET
                       selected_order = excluded.selected_order,
                       updated_at = excluded.updated_at
                   WHERE excluded.selected_order > species_representatives.selected_order""",
                (target, source),
            )
            self.conn.execute(
                """UPDATE photo_preferences SET photo_id = ? WHERE photo_id = ?
                   AND purpose IN ('species_representative', 'life_list', 'highlights')""",
                (target, source),
            )
            changed = self.conn.execute(
                "UPDATE species_choices SET photo_id = ? WHERE photo_id = ?",
                (target, source),
            ).rowcount
            for table in ("species_choice_losers", "species_choice_winners"):
                changed += self.conn.execute(
                    f"UPDATE OR IGNORE {table} SET photo_id = ? WHERE photo_id = ?",
                    (target, source),
                ).rowcount
                self.conn.execute(
                    f"DELETE FROM {table} WHERE photo_id = ?", (source,),
                )
            comparisons_changed |= bool(changed)

        if comparisons_changed:
            # Folding two photo IDs can create self-comparisons and cycles.
            # Resolve them by the same newest-choice rule as a new decision.
            dropped = {pid for pid, target in mapping.items() if target is None}
            surviving = [
                c for c in self.comparisons()
                if c.better not in dropped and c.worse not in dropped
            ]
            _, contradicted = split_contradictions(surviving)
            self.delete_comparisons(contradicted)

    def rename(self, old_species: str, new_species: str) -> int:
        """Carry a keyword rename through stored choices.

        A choice stores the species name that was current when the choice
        was made. When the keyword is renamed, the stored name must follow,
        so ``species_preference.choice_note`` keeps finding the note for
        that species without having to fall back to another species' note
        (two species can share a chosen photo). The rename is catalog-wide,
        matching the choice's own scope.
        """
        if not old_species or not new_species or old_species == new_species:
            return 0
        cur = self.conn.execute(
            "UPDATE species_choices SET species = ? WHERE species = ?",
            (new_species, old_species),
        )
        return cur.rowcount

    def clear_for_representatives_of(self, species: str) -> int:
        """Forget the choices behind ``species``' representatives.

        Removes, for each photo chosen for ``species``, its choices made for
        that species and its choices made under a name it is no longer
        chosen for (a since-renamed species), so clearing a species leaves
        no comparison to revive when its photos are chosen again. A photo
        also chosen for another species keeps that species' choices. Call it
        before the representative rows are deleted.
        """
        cur = self.conn.execute(
            """DELETE FROM species_choices
               WHERE photo_id IN (
                       SELECT photo_id FROM species_representatives
                       WHERE species = ?
                   )
                 AND (
                       species = ?
                       OR NOT EXISTS (
                           SELECT 1 FROM species_representatives sr
                           WHERE sr.photo_id = species_choices.photo_id
                             AND sr.species = species_choices.species
                       )
                 )""",
            (species, species),
        )
        return cur.rowcount
