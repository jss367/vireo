"""Audited corrections of stored prediction identities, and their evidence.

Owns the SQL of the two modules that rewrite ``predictions.scientific_name``
and ``source_taxon_id`` after the fact: ``species_identity_repair`` (verified
common-name corrections, applied once per resolution identity at startup) and
``label_source_identities`` (taxon identities recovered for label lists saved
before lists recorded them). That is: finding the unstamped rows each repair
may touch, the optimistic per-row stamp, the before/after audit in
``species_identity_repairs``, retiring the corrected classifier runs from
export, clearing the workspaces' group fingerprints, the per-fingerprint
identities in ``label_source_identities``, and the reads of the recorded
outcome markers in ``db_meta``.

What a change is, which rows qualify and the transaction boundaries stay in
those modules: nothing here commits or opens a transaction.
``species_identity_repair.refresh_common_name_index`` works on a bare
``sqlite3`` connection by contract (the encounter-evaluation tool calls it
without a ``Database``) and keeps its SQL.

Callers reach it as ``db.species_identity_repairs`` (a fresh repository per
access, see ``Database.species_identity_repairs``); there are no forwarding
wrappers on ``Database``.
"""

import sqlite3
from collections.abc import Iterable

# The pre-fingerprint mode was not recorded. Only unenriched BioCLIP text
# rows are eligible: a scientific name or rank may be fixed-head ToL output,
# and must never be overwritten from a regional list's consensus.
UNSTAMPED_LEGACY_WHERE = (
    "labels_fingerprint = 'legacy' AND classifier_model GLOB 'BioCLIP*' AND "
    + " AND ".join(
        f"COALESCE(trim({column}), '') = ''" for column in (
            "scientific_name", "taxonomy_kingdom", "taxonomy_phylum", "taxonomy_class",
            "taxonomy_order", "taxonomy_family", "taxonomy_genus",
        )
    )
)


class SpeciesIdentityRepairRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    # -- rows a repair may change ------------------------------------------
    #
    # Each candidate row carries ``id, species, detection_id,
    # classifier_model, labels_fingerprint, scientific_name,
    # source_taxon_id``: what ``stamp_prediction_identity`` re-checks.

    def bioclip_correction_candidates(
        self, species: str, scientific_name: str,
    ) -> list[sqlite3.Row]:
        """Custom-list BioCLIP rows labelled ``species`` that lack this identity.

        The label match ignores case; ToL and legacy rows, and rows that
        already carry a source taxon, are excluded.
        """
        return self.conn.execute(
            "SELECT id, species, detection_id, classifier_model, labels_fingerprint, scientific_name, "
            "source_taxon_id FROM predictions WHERE species = ? COLLATE NOCASE "
            "AND classifier_model LIKE 'BioCLIP%' AND labels_fingerprint NOT IN ('tol', 'legacy') "
            "AND source_taxon_id IS NULL AND scientific_name IS NOT ?",
            (species, scientific_name),
        ).fetchall()

    def _unstamped(self, where: str, params: tuple) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT id, species, detection_id, classifier_model, labels_fingerprint, "
            "scientific_name, source_taxon_id FROM predictions "
            "WHERE source_taxon_id IS NULL AND " + where,
            params,
        ).fetchall()

    def unstamped_predictions_for_fingerprint(self, fingerprint: str) -> list[sqlite3.Row]:
        """Rows made from label set ``fingerprint`` that carry no source taxon."""
        return self._unstamped("labels_fingerprint = ?", (fingerprint,))

    def unstamped_legacy_predictions(self) -> list[sqlite3.Row]:
        """Pre-fingerprint BioCLIP text rows with no identity at all."""
        return self._unstamped(UNSTAMPED_LEGACY_WHERE, ())

    def has_unstamped_prediction_for_fingerprint(self, fingerprint: str) -> bool:
        """Whether any row made from ``fingerprint`` lacks a source taxon."""
        return self.conn.execute(
            "SELECT 1 FROM predictions WHERE labels_fingerprint = ? "
            "AND source_taxon_id IS NULL LIMIT 1",
            (fingerprint,),
        ).fetchone() is not None

    def has_unstamped_legacy_prediction(self) -> bool:
        """Whether any pre-fingerprint BioCLIP text row still lacks an identity."""
        return self.conn.execute(
            "SELECT 1 FROM predictions WHERE source_taxon_id IS NULL AND "
            + UNSTAMPED_LEGACY_WHERE + " LIMIT 1",
        ).fetchone() is not None

    def unstamped_legacy_species(self) -> list[str]:
        """Distinct species labels of the unstamped pre-fingerprint rows."""
        return [
            row["species"] for row in self.conn.execute(
                "SELECT DISTINCT species FROM predictions WHERE species IS NOT NULL AND source_taxon_id IS NULL AND "
                + UNSTAMPED_LEGACY_WHERE,
            )
        ]

    # -- applying a change -------------------------------------------------

    def stamp_prediction_identity(
        self,
        prediction_id: int,
        new_scientific_name: str | None,
        new_source_taxon_id: int | None,
        *,
        scientific_name: str | None,
        source_taxon_id: int | None,
        species: str | None,
        classifier_model: str | None,
        labels_fingerprint: str | None,
        detection_id: int | None,
    ) -> int:
        """Set a row's identity only if it still matches the planned values.

        Returns the number of rows changed: 1, or 0 when the row changed
        after the repair was planned.
        """
        return self.conn.execute(
            "UPDATE predictions SET scientific_name = ?, source_taxon_id = ? "
            "WHERE id = ? AND scientific_name IS ? AND source_taxon_id IS ? "
            "AND species IS ? AND classifier_model IS ? AND labels_fingerprint IS ? AND detection_id IS ?",
            (new_scientific_name, new_source_taxon_id, prediction_id, scientific_name,
             source_taxon_id, species, classifier_model, labels_fingerprint, detection_id),
        ).rowcount

    def record_repair(
        self,
        prediction_id: int,
        resolution_identity: str,
        before_json: str,
        after_json: str,
        reason: str,
    ) -> None:
        """Append one before/after row to the ``species_identity_repairs`` audit."""
        self.conn.execute(
            "INSERT INTO species_identity_repairs "
            "(prediction_id, resolution_identity, before_json, after_json, reason) "
            "VALUES (?, ?, ?, ?, ?)",
            (prediction_id, resolution_identity, before_json, after_json, reason),
        )

    def retire_classifier_runs(
        self, detection_id: int, classifier_model: str, labels_fingerprint: str,
    ) -> None:
        """Keep a corrected run out of portable exports under its old fingerprint."""
        self.conn.execute(
            "UPDATE classifier_runs SET runtime_fingerprint = 'legacy' "
            "WHERE detection_id = ? AND classifier_model = ? AND labels_fingerprint = ?",
            (detection_id, classifier_model, labels_fingerprint),
        )

    def clear_group_fingerprints(self) -> None:
        """Make every workspace regroup its encounters on the next run."""
        self.conn.execute("UPDATE workspaces SET last_group_fingerprint = NULL")

    # -- label-list identities ---------------------------------------------

    def label_fingerprints(self, *excluded: str) -> list[sqlite3.Row]:
        """``fingerprint, sources_json`` of tracked label sets, by fingerprint.

        ``excluded`` names the two sentinel fingerprints to skip.
        """
        return self.conn.execute(
            "SELECT fingerprint, sources_json FROM labels_fingerprints "
            "WHERE fingerprint NOT IN (?, ?) ORDER BY fingerprint",
            excluded,
        ).fetchall()

    def record_label_identities(
        self, rows: Iterable[tuple[str, str, int, str]],
    ) -> None:
        """Upsert ``(labels_fingerprint, species, source_taxon_id, scientific_name)`` rows."""
        self.conn.executemany(
            "INSERT OR REPLACE INTO label_source_identities "
            "(labels_fingerprint, species, source_taxon_id, scientific_name) VALUES (?, ?, ?, ?)",
            rows,
        )

    def label_identities(self) -> list[sqlite3.Row]:
        """Every recorded ``species, source_taxon_id, scientific_name``."""
        return self.conn.execute(
            "SELECT species, source_taxon_id, scientific_name FROM label_source_identities",
        ).fetchall()

    def marker_values(self, prefix: str, excluded_key: str) -> list[str]:
        """``db_meta`` values whose key starts with ``prefix``, except ``excluded_key``."""
        return [
            value for (value,) in self.conn.execute(
                "SELECT value FROM db_meta WHERE key LIKE ? AND key != ?",
                (prefix + "%", excluded_key),
            ).fetchall()
        ]
