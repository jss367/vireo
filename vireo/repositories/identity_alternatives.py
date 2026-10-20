"""Workspace review state for alternative identities, never species keywords.

Only unresolved rows live here. Confirmation deletes a row; the ordinary edit
history stores its before-state for undo. A shared group_id keeps a batch of
frames together without asserting that every candidate occurs in a frame.
"""

import json
import sqlite3
from collections.abc import Callable, Collection
from typing import Any

from keyword_normalization import keyword_match_key
from sql_chunks import chunked


class IdentityAlternativesRepository:
    def __init__(self, conn: sqlite3.Connection, workspace_id: Callable[[], int]) -> None:
        self.conn = conn
        self._workspace_id = workspace_id

    def for_photos(self, photo_ids: Collection[int] | None = None) -> dict[int, dict[str, Any]]:
        """Visible unresolved photos in this workspace, optionally restricted."""
        ws = self._workspace_id()
        result = {}
        batches = chunked(list(dict.fromkeys(photo_ids))) if photo_ids is not None else [None]
        for ids in batches:
            clause = "" if ids is None else f" AND a.photo_id IN ({','.join('?' for _ in ids)})"
            rows = self.conn.execute(
                "SELECT a.* FROM photo_identity_alternatives a "
                "JOIN photo_workspace_visibility v ON v.photo_id=a.photo_id AND v.workspace_id=a.workspace_id "
                "WHERE a.workspace_id=?" + clause + " ORDER BY a.photo_id",
                [ws, *(ids or [])],
            )
            for row in rows:
                result[row['photo_id']] = {
                    'group_id': row['group_id'], 'candidates': json.loads(row['candidates']),
                    'reason': row['reason'], 'missing_evidence': row['missing_evidence'],
                    'status': 'unresolved',
                }
        return result

    def photo_ids(self) -> set[int]:
        return set(self.for_photos())

    def set_state(self, photo_id: int, state: dict[str, Any] | None) -> None:
        """Write without committing; the decision/history transaction owns it."""
        ws = self._workspace_id()
        if state is None:
            self.conn.execute(
                'DELETE FROM photo_identity_alternatives WHERE workspace_id=? AND photo_id=?', (ws, photo_id),
            )
        else:
            self.conn.execute(
                'INSERT INTO photo_identity_alternatives '
                '(workspace_id, photo_id, group_id, candidates, reason, missing_evidence) VALUES (?, ?, ?, ?, ?, ?) '
                'ON CONFLICT(workspace_id, photo_id) DO UPDATE SET group_id=excluded.group_id, '
                'candidates=excluded.candidates, reason=excluded.reason, missing_evidence=excluded.missing_evidence',
                (ws, photo_id, state['group_id'], json.dumps(state['candidates']),
                 state['reason'], state['missing_evidence']),
            )

    def candidate_links(self, photo_ids: Collection[int], candidates: Collection[str]) -> dict[int, list[dict[str, Any]]]:
        """Existing species associations matching alternatives, with provenance.

        Match linked aliases by taxon as well as normalized display spelling.
        Unrelated species in a genuine mixed-species frame are left alone.
        """
        names = {keyword_match_key(name) for name in candidates}
        taxa = set()
        for row in self.conn.execute('SELECT name, taxon_id FROM keywords WHERE is_species=1 OR type=\'taxonomy\''):
            if keyword_match_key(row['name']) in names and row['taxon_id'] is not None:
                taxa.add(row['taxon_id'])
        result = {}
        for ids in chunked(list(photo_ids)):
            rows = self.conn.execute(
                'SELECT pk.photo_id, pk.keyword_id, pk.source, k.name, k.taxon_id '
                'FROM photo_keywords pk JOIN keywords k ON k.id=pk.keyword_id '
                'LEFT JOIN taxa t ON t.id=k.taxon_id '
                f"WHERE pk.photo_id IN ({','.join('?' for _ in ids)}) "
                "AND (k.is_species=1 OR k.type='taxonomy') AND (t.rank='species' OR t.rank IS NULL) "
                'ORDER BY pk.keyword_id', ids,
            )
            for row in rows:
                if keyword_match_key(row['name']) in names or row['taxon_id'] in taxa:
                    result.setdefault(row['photo_id'], []).append({
                        'keyword_id': row['keyword_id'], 'source': row['source'],
                    })
        return result
