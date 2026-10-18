"""Writes and lookups of the local reference taxonomy that ``taxonomy.py`` loads.

Owns the SQL behind the iNaturalist taxonomy loaders: upserting ``taxa`` rows
(from the iNat ``taxa.csv.gz`` export and from a DWCA ``taxonomy.json``),
linking each row to its parent, pruning taxa that left the payload, the
``taxa_common_names`` index, the default ``informal_groups`` seed, and the
parent-chain walk that routes a taxon to its keypoint model, and the
identity lookups ``species_identity.SpeciesResolver`` makes. Parsing the
payloads, deciding what to keep, progress reporting and every commit stay in
``taxonomy.py``: nothing here commits, so each loader's transaction ends where
it always did.

The life-list and explorer reads of ``taxa`` belong to
``repositories.species_curation``, and keyword-to-taxon linking to
``repositories.keywords``.

Callers reach it as ``db.taxa`` (a fresh repository per access, see
``Database.taxa``); there are no forwarding wrappers on ``Database``.
"""

import sqlite3
from collections.abc import Iterable


class TaxaRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    # -- taxa rows --------------------------------------------------------

    def count(self) -> int:
        """How many rows the ``taxa`` table holds."""
        return self.conn.execute(
            "SELECT COUNT(*) FROM taxa"
        ).fetchone()[0]

    def local_id_for_inat_id(self, inat_id: int) -> int | None:
        """The local ``taxa.id`` of the row with this iNat id, or None."""
        row = self.conn.execute(
            "SELECT id FROM taxa WHERE inat_id = ?", (inat_id,)
        ).fetchone()
        return row["id"] if row else None

    def local_id_for_name(self, name: str) -> int | None:
        """The local ``taxa.id`` of a row with this scientific name, or None."""
        row = self.conn.execute(
            "SELECT id FROM taxa WHERE name = ?", (name,)
        ).fetchone()
        return row["id"] if row else None

    def upsert_inat_export_taxon(
        self, inat_id: int, name: str, rank: str, kingdom: str | None,
    ) -> int | None:
        """Insert or update one ``taxa.csv.gz`` taxon; returns its local id.

        ``common_name`` and ``parent_id`` are left as they were on conflict.
        """
        self.conn.execute(
            "INSERT INTO taxa (inat_id, name, rank, kingdom) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT(inat_id) DO UPDATE SET "
            "name = excluded.name, rank = excluded.rank, kingdom = excluded.kingdom",
            (inat_id, name, rank, kingdom),
        )
        return self.local_id_for_inat_id(inat_id)

    def upsert_taxon(
        self,
        inat_id: int,
        name: str,
        rank: str,
        kingdom: str | None,
        common_name: str | None,
    ) -> None:
        """Insert or overwrite one ``taxonomy.json`` taxon, ``common_name`` included."""
        self.conn.execute(
            "INSERT INTO taxa (inat_id, name, rank, kingdom, common_name) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(inat_id) DO UPDATE SET "
            "name=excluded.name, rank=excluded.rank, "
            "kingdom=excluded.kingdom, common_name=excluded.common_name",
            (inat_id, name, rank, kingdom, common_name),
        )

    def set_parent(self, local_id: int, parent_local_id: int) -> None:
        """Point ``taxa.parent_id`` of ``local_id`` at ``parent_local_id``."""
        self.conn.execute(
            "UPDATE taxa SET parent_id = ? WHERE id = ?",
            (parent_local_id, local_id),
        )

    def set_common_name(self, local_id: int, common_name: str | None) -> None:
        """Set (or clear, with None) a taxon's preferred common name."""
        self.conn.execute(
            "UPDATE taxa SET common_name = ? WHERE id = ?", (common_name, local_id)
        )

    def ids_with_inat_id(self) -> list[sqlite3.Row]:
        """Every ``(id, inat_id)`` row whose ``inat_id`` is set."""
        return self.conn.execute(
            "SELECT id, inat_id FROM taxa WHERE inat_id IS NOT NULL"
        ).fetchall()

    def prune_absent_inat_ids(self, fresh_inat_ids: Iterable[int]) -> int:
        """Delete taxa whose iNat id is not in ``fresh_inat_ids``; returns the count.

        The fresh ids go through a temp table (the set is too large for a
        parameterized IN clause). Several FKs point at ``taxa(id)`` without
        ON DELETE SET NULL, so they are cleared first: ``keywords.taxon_id``
        (keeping the keyword row) and the self-referencing ``taxa.parent_id``
        of children whose parent is pruned. ``taxa_common_names`` and
        ``informal_group_taxa`` cascade.
        """
        self.conn.execute(
            "CREATE TEMP TABLE IF NOT EXISTS fresh_inat_ids "
            "(inat_id INTEGER PRIMARY KEY)"
        )
        self.conn.execute("DELETE FROM fresh_inat_ids")
        self.conn.executemany(
            "INSERT OR IGNORE INTO fresh_inat_ids (inat_id) VALUES (?)",
            [(iid,) for iid in fresh_inat_ids],
        )
        stale_local_ids_sql = (
            "SELECT id FROM taxa "
            "WHERE inat_id IS NOT NULL "
            "  AND inat_id NOT IN (SELECT inat_id FROM fresh_inat_ids)"
        )
        self.conn.execute(
            f"UPDATE keywords SET taxon_id = NULL "
            f"WHERE taxon_id IN ({stale_local_ids_sql})"
        )
        self.conn.execute(
            f"UPDATE taxa SET parent_id = NULL "
            f"WHERE parent_id IN ({stale_local_ids_sql})"
        )
        pruned = self.conn.execute(
            "DELETE FROM taxa WHERE inat_id IS NOT NULL "
            "  AND inat_id NOT IN (SELECT inat_id FROM fresh_inat_ids)"
        ).rowcount
        self.conn.execute("DROP TABLE fresh_inat_ids")
        return pruned

    def node_by_inat_id(self, inat_id: int) -> sqlite3.Row | None:
        """``id, name, rank, parent_id`` of the taxon with this iNat id, or None."""
        return self.conn.execute(
            "SELECT id, name, rank, parent_id FROM taxa WHERE inat_id=?",
            (inat_id,),
        ).fetchone()

    def node_by_id(self, local_id: int) -> sqlite3.Row | None:
        """``id, name, rank, parent_id`` of the taxon with this local id, or None."""
        return self.conn.execute(
            "SELECT id, name, rank, parent_id FROM taxa WHERE id=?",
            (local_id,),
        ).fetchone()

    # -- species-identity lookups (``species_identity.SpeciesResolver``) ---
    #
    # Each row carries ``taxon_id`` (the iNat id), ``scientific_name``,
    # ``common_name`` and ``rank``.

    def identity_by_inat_id(self, inat_id: int) -> sqlite3.Row | None:
        """The identity row of the taxon with this iNat id, or None."""
        return self.conn.execute(
            "SELECT inat_id AS taxon_id, name AS scientific_name, common_name, rank "
            "FROM taxa WHERE inat_id = ?", (inat_id,),
        ).fetchone()

    def identities_by_scientific_name(self, name: str) -> list[sqlite3.Row]:
        """Identity rows of every taxon with exactly this scientific name."""
        return self.conn.execute(
            "SELECT inat_id AS taxon_id, name AS scientific_name, common_name, rank "
            "FROM taxa WHERE name = ?", (name,),
        ).fetchall()

    def identities_by_any_name(self, name: str) -> list[sqlite3.Row]:
        """Distinct identity rows whose scientific, preferred or indexed name is ``name``.

        The scientific and preferred-name match is exact; the
        ``taxa_common_names`` match ignores case.
        """
        return self.conn.execute(
            "SELECT DISTINCT t.inat_id AS taxon_id, t.name AS scientific_name, "
            "t.common_name, t.rank FROM taxa t WHERE t.name = ? OR t.common_name = ? "
            "UNION SELECT t.inat_id, t.name, t.common_name, t.rank "
            "FROM taxa_common_names cn JOIN taxa t ON t.id = cn.taxon_id "
            "WHERE cn.name = ? COLLATE NOCASE",
            (name, name, name),
        ).fetchall()

    def identities_by_preferred_common_name(self, name: str) -> list[sqlite3.Row]:
        """Up to two distinct identity rows whose preferred common name is ``name``.

        Case-insensitive; two rows is enough for a caller to tell the name is
        shared.
        """
        return self.conn.execute(
            "SELECT DISTINCT inat_id AS taxon_id, name AS scientific_name, common_name, rank "
            "FROM taxa WHERE lower(common_name) = lower(?) LIMIT 2", (name,),
        ).fetchall()

    # -- taxa_common_names ------------------------------------------------

    def clear_english_common_names(self) -> None:
        """Drop every English row of the common-name index."""
        self.conn.execute("DELETE FROM taxa_common_names WHERE locale = 'en'")

    def delete_common_names(self, local_id: int, locale: str) -> None:
        """Drop one taxon's indexed names in ``locale``."""
        self.conn.execute(
            "DELETE FROM taxa_common_names WHERE taxon_id = ? AND locale = ?",
            (local_id, locale),
        )

    def add_common_name(self, local_id: int, name: str, locale: str) -> None:
        """Index ``name`` under the taxon in ``locale`` (kept if already there)."""
        self.conn.execute(
            "INSERT OR IGNORE INTO taxa_common_names "
            "(taxon_id, name, locale) VALUES (?, ?, ?)",
            (local_id, name, locale),
        )

    # -- informal groups --------------------------------------------------

    def ensure_informal_group(self, name: str) -> int:
        """The id of the informal group ``name``, creating it when missing."""
        self.conn.execute(
            "INSERT OR IGNORE INTO informal_groups (name) VALUES (?)",
            (name,),
        )
        return self.conn.execute(
            "SELECT id FROM informal_groups WHERE name = ?", (name,)
        ).fetchone()["id"]

    def link_informal_group(self, group_id: int, local_id: int) -> None:
        """Link a taxon to an informal group (kept if already linked)."""
        self.conn.execute(
            "INSERT OR IGNORE INTO informal_group_taxa "
            "(group_id, taxon_id) VALUES (?, ?)",
            (group_id, local_id),
        )
