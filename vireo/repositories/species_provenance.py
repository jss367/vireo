"""Per-photo/species evidence, separate from keyword authorship and flags.

The optional JSON belongs to the association. Absence deliberately means
manual/unspecified or imported/unknown, according to its existing source.
"""

import json
import sqlite3
from typing import Any  # Evidence payloads contain caller-supplied JSON fields.


class SpeciesProvenanceRepository:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def state(self, photo_id: int, keyword_id: int) -> dict[str, Any] | None:
        row = self.conn.execute(
            '''SELECT pk.source, pk.species_provenance, (k.is_species=1 OR k.type='taxonomy') AS is_species
               FROM photo_keywords pk JOIN keywords k ON k.id=pk.keyword_id
               WHERE pk.photo_id=? AND pk.keyword_id=?''',
            (photo_id, keyword_id),
        ).fetchone()
        return dict(row) if row else None

    def set(self, photo_id: int, keyword_id: int, provenance: dict[str, Any]) -> None:
        self.conn.execute(
            'UPDATE photo_keywords SET species_provenance=? WHERE photo_id=? AND keyword_id=?',
            (json.dumps(provenance, sort_keys=True), photo_id, keyword_id),
        )

    def detection_photo_id(self, detection_id: int) -> int | None:
        row = self.conn.execute('SELECT photo_id FROM detections WHERE id=?', (detection_id,)).fetchone()
        return row[0] if row else None

    def assignments(self, photo_id: int) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            '''SELECT k.id AS keyword_id, k.name AS species, pk.source, pk.species_provenance
               FROM photo_keywords pk JOIN keywords k ON k.id=pk.keyword_id
               LEFT JOIN taxa t ON t.id=k.taxon_id
               WHERE pk.photo_id=? AND (k.is_species=1 OR k.type='taxonomy')
                 AND (t.rank='species' OR t.rank IS NULL) ORDER BY k.id''',
            (photo_id,),
        )
        result = []
        for row in rows:
            provenance = json.loads(row['species_provenance']) if row['species_provenance'] else {
                'method': 'manual_unspecified' if row['source'] in ('manual', 'accept') else 'imported_unknown',
                'confidence': None, 'confidence_meaning': 'Not recorded',
            }
            result.append({'photo_id': photo_id, 'keyword_id': row['keyword_id'],
                           'species': row['species'], **provenance})
        return result

    def restore(self, photo_id: int, keyword_id: int, state: dict[str, Any] | None) -> None:
        # Handlers own association/sidecar replay. Only restore attribution
        # on surviving links; deleted keywords must stay deleted.
        if state is not None:
            self.conn.execute(
                "UPDATE photo_keywords SET source=?, species_provenance=? WHERE photo_id=? AND keyword_id=?",
                (state['source'], state['species_provenance'], photo_id, keyword_id),
            )

    def replay(self, photo_id: int, snapshot: str | None, *, undo: bool) -> None:
        if snapshot:
            for keyword_id, states in json.loads(snapshot).items():
                self.restore(photo_id, int(keyword_id), states['before' if undo else 'after'])
