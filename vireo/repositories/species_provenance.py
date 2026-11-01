"""Per-photo/species evidence, separate from keyword authorship and flags.

The optional JSON belongs to the association. Absence deliberately means
manual/unspecified or imported/unknown, according to its existing source.
"""

import json
import sqlite3
from collections.abc import Callable
from typing import Any  # Evidence payloads contain caller-supplied JSON fields.


def evidence_for_photo_copy(value: str | None, photo_id: int) -> str | None:
    """Retain the original photo for copied subject evidence."""
    if value is None:
        return None
    evidence = json.loads(value)
    if evidence.get('detection_id') is None or evidence.get('subject_photo_id') is not None:
        return value
    evidence['subject_photo_id'] = photo_id
    return json.dumps(evidence, sort_keys=True)


class SpeciesProvenanceRepository:
    def __init__(
        self, conn: sqlite3.Connection,
        pending_before: dict[tuple[int, int], dict[str, Any] | None] | None = None,
    ) -> None:
        self.conn = conn
        # ``Database._species_provenance_before``: the attribution each
        # undoable keyword/species write saw before it changed, keyed by
        # ``(photo_id, keyword_id)``, until ``history_items`` folds it into
        # the edit-history row (or a replay discards it).
        self.pending_before = {} if pending_before is None else pending_before

    def remember_before(self, photo_id: int, keyword_id: int, *, force: bool = False) -> None:
        """Track assignment attribution before mutation; ``history_items`` consumes the snapshot."""
        state = self.state(photo_id, keyword_id)
        if not force and (state is None or not state['is_species']):
            return
        self.pending_before.setdefault((photo_id, keyword_id), state)

    def history_items(self, action: str, items: Any) -> list[dict[str, Any]]:
        """Attach evidence snapshots to species/keyword edits, preserving other payloads."""
        items = [dict(item) for item in items]
        if action not in {'keyword_add', 'keyword_remove', 'species_replace', 'prediction_accept',
                          'identity_alternatives'}:
            return items
        pending = self.pending_before
        by_photo: dict[int, dict[int, Any]] = {}
        for (pid, keyword_id), before in pending.items():
            by_photo.setdefault(pid, {})[keyword_id] = before
        for item in items:
            pid = item['photo_id']
            changes = {}
            for keyword_id, before in by_photo.pop(pid, {}).items():
                changes[str(keyword_id)] = {'before': before,
                    'after': self.state(pid, keyword_id)}
                pending.pop((pid, keyword_id))
            if changes:
                item['species_provenance'] = json.dumps(changes, sort_keys=True)
        return items

    def replay_history(self, items: Any, *, undo: bool, restore_source: bool = True) -> None:
        """Restore attribution after the normal association and sidecar handlers."""
        for item in items:
            self.replay(item['photo_id'], dict(item).get('species_provenance'),
                        undo=undo, restore_source=restore_source)
        self.pending_before.clear()

    def forget_snapshots(self) -> None:
        """Drop every pending snapshot (a failed or retired replay must not leak them)."""
        self.pending_before.clear()

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

    def carry(self, losing_id: int, survivor_id: int, *, keyword_id: int | None = None) -> None:
        """Carry evidence only for links the normal merge chose to keep.

        An explicit survivor record wins a collision. Subject references retain
        their original photo identity rather than implying a new detection on
        the surviving photo. The caller owns the association write transaction.
        """
        rows = self.conn.execute(
            """SELECT src.keyword_id, src.species_provenance FROM photo_keywords src
               JOIN photo_keywords dst ON dst.keyword_id=src.keyword_id AND dst.photo_id=?
               WHERE src.photo_id=? AND src.species_provenance IS NOT NULL
                 AND dst.species_provenance IS NULL AND (? IS NULL OR src.keyword_id=?)
               ORDER BY src.keyword_id""",
            (survivor_id, losing_id, keyword_id, keyword_id),
        ).fetchall()
        for row in rows:
            evidence = evidence_for_photo_copy(row['species_provenance'], losing_id)
            if evidence is not None:
                self.set(survivor_id, row['keyword_id'], json.loads(evidence))

    def detection_photo_id(self, detection_id: int) -> int | None:
        row = self.conn.execute('SELECT photo_id FROM detections WHERE id=?', (detection_id,)).fetchone()
        return row[0] if row else None

    def assignments(self, photo_id: int) -> list[dict[str, Any]]:
        aliases = self._taxon_aliases(photo_id)
        rows = self.conn.execute(
            f'''SELECT k.id AS keyword_id, k.name AS species, pk.source, pk.species_provenance,
                      t.id AS taxon_id, COALESCE(k.source_taxon_id, t.inat_id) AS source_taxon_id,
                      t.name AS scientific_name, t.common_name
               FROM photo_keywords pk JOIN keywords k ON k.id=pk.keyword_id
               LEFT JOIN taxa t ON {self._display_taxon_match()}
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
                           'species': row['species'], **provenance,
                           **self._display_names(row, aliases)})
        return result

    @staticmethod
    def _display_taxon_match() -> str:
        """Mirror identity_sql's source-first rule, including absent source taxa."""
        return '''((k.source_taxon_id IS NOT NULL AND k.source_taxon_id=t.inat_id)
                   OR (k.source_taxon_id IS NULL AND k.taxon_id=t.id))'''

    def _taxon_aliases(self, photo_id: int) -> dict[int, list[str]]:
        """Saved keyword aliases of the photo's taxa, never inferred from spelling."""
        rows = self.conn.execute(
            f'''WITH attached AS (
                   SELECT DISTINCT t.id, t.inat_id FROM photo_keywords pk
                   JOIN keywords k ON k.id=pk.keyword_id
                   JOIN taxa t ON {self._display_taxon_match()} WHERE pk.photo_id=?
                     AND (k.is_species=1 OR k.type='taxonomy')
               )
               SELECT t.id AS taxon_id, k.name FROM attached t
               JOIN keywords k ON {self._display_taxon_match()}
               WHERE k.is_species=1 OR k.type='taxonomy'
               ORDER BY k.name''', (photo_id,),
        )
        result: dict[int, list[str]] = {}
        for row in rows:
            result.setdefault(row['taxon_id'], []).append(row['name'])
        return result

    @staticmethod
    def _display_names(row: sqlite3.Row, aliases: dict[int, list[str]]) -> dict[str, Any]:
        """Preferred taxonomy name for display; stored names remain edit/export keys."""
        if row['taxon_id'] is None:
            if row['source_taxon_id'] is not None:
                return {'display_name': row['species'], 'source_taxon_id': row['source_taxon_id']}
            return {}
        preferred = row['common_name'] or row['scientific_name'] or row['species']
        seen = {preferred.casefold(), (row['scientific_name'] or '').casefold()}
        synonyms = []
        for name in [row['species'], *aliases.get(row['taxon_id'], [])]:
            if name and name.casefold() not in seen:
                synonyms.append(name)
                seen.add(name.casefold())
        return {'display_name': preferred, 'synonyms': sorted(synonyms, key=str.casefold),
                'taxon_id': row['taxon_id'], 'source_taxon_id': row['source_taxon_id'],
                'scientific_name': row['scientific_name']}

    def restore(self, photo_id: int, keyword_id: int, state: dict[str, Any] | None) -> None:
        # Handlers own association/sidecar replay. Only restore attribution
        # on surviving links; deleted keywords must stay deleted.
        if state is not None:
            self.conn.execute(
                "UPDATE photo_keywords SET source=?, species_provenance=? WHERE photo_id=? AND keyword_id=?",
                (state['source'], state['species_provenance'], photo_id, keyword_id),
            )

    def replay(self, photo_id: int, snapshot: str | None, *, undo: bool, restore_source: bool = True) -> None:
        if snapshot:
            for keyword_id, states in json.loads(snapshot).items():
                state = states['before' if undo else 'after']
                if state is not None and not restore_source:
                    current = self.state(photo_id, int(keyword_id))
                    if current is not None:
                        state = {**state, 'source': current['source']}
                self.restore(photo_id, int(keyword_id), state)


def _fold_history_state(left: dict[str, Any] | None, right: dict[str, Any] | None,
                        source_max: Callable[[str | None, str | None], str | None]) -> dict[str, Any] | None:
    """Converged aliases retain the stronger authorship and existing evidence."""
    if left is None:
        return right
    if right is None:
        return left
    return {**right, **left, 'source': source_max(left['source'], right['source']),
            'species_provenance': left['species_provenance'] or right['species_provenance']}


def retarget_keyword_history(conn: sqlite3.Connection, src_id: int, dst_id: int,
                             source_max: Callable[[str | None, str | None], str | None]) -> None:
    """Retarget evidence replay alongside the ordinary keyword-history rewrite."""
    rows = conn.execute(
        '''SELECT i.*, e.action_type FROM edit_history_items i JOIN edit_history e ON e.id=i.edit_id
           WHERE i.species_provenance IS NOT NULL OR e.action_type='species_provenance' ''',
    ).fetchall()
    for row in rows:
        if row['species_provenance']:
            states = json.loads(row['species_provenance'])
            source = states.pop(str(src_id), None)
            if source is not None:
                target = states.get(str(dst_id), {})
                states[str(dst_id)] = {phase: _fold_history_state(target.get(phase), source.get(phase), source_max)
                                      for phase in ('before', 'after')}
                conn.execute('UPDATE edit_history_items SET species_provenance=? WHERE id=?',
                             (json.dumps(states, sort_keys=True), row['id']))
        if row['action_type'] == 'species_provenance':
            destination = conn.execute(
                'SELECT source, species_provenance FROM photo_keywords WHERE photo_id=? AND keyword_id=?',
                (row['photo_id'], dst_id),
            ).fetchone()
            destination = dict(destination) if destination else None
            for column in ('old_value', 'new_value'):
                snapshot = json.loads(row[column])
                if snapshot['keyword_id'] == src_id:
                    snapshot['keyword_id'] = dst_id
                    snapshot['state'] = _fold_history_state(destination, snapshot['state'], source_max)
                    conn.execute(f'UPDATE edit_history_items SET {column}=? WHERE id=?',
                                 (json.dumps(snapshot, sort_keys=True), row['id']))
    conn.execute("UPDATE edit_history SET new_value=? WHERE action_type='species_provenance' AND new_value=?",
                 (str(dst_id), str(src_id)))
