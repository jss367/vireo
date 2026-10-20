"""Workspace review state for alternative identities, never species keywords.

Only unresolved rows live here. Confirmation deletes a row; the ordinary edit
history stores its before-state for undo. A shared group_id keeps a batch of
frames together without asserting that every candidate occurs in a frame.
"""

import json
import sqlite3
import uuid
from collections.abc import Callable, Collection
from typing import Any

from keyword_normalization import keyword_match_key
from sql_chunks import chunked


def _renamed_candidates(value: object, old_name: str, new_name: str) -> list[str] | None:
    """Rewrite name-only candidates, retaining order and one canonical target."""
    if not isinstance(value, list) or not all(isinstance(name, str) for name in value):
        return None
    old_key, new_key = keyword_match_key(old_name), keyword_match_key(new_name)
    if not any(keyword_match_key(name) == old_key for name in value):
        return None
    result = []
    seen = set()
    for name in value:
        if keyword_match_key(name) in (old_key, new_key):
            name = new_name
        key = keyword_match_key(name)
        if key not in seen:
            result.append(name)
            seen.add(key)
    return result


def retarget_candidate_names(conn: sqlite3.Connection, old_name: str, new_name: str) -> None:
    """Carry species renames through live uncertainty and both history sides.

    Candidate names are evidence independent of attached keyword links, so
    an empty links array must not prevent this rewrite. Caller commits.
    A pair converging to one name stays unresolved until explicitly resolved.
    Rotate each affected group revision consistently so pre-maintenance forms
    cannot save retired candidates back into the group.
    """
    if not old_name or not new_name or old_name == new_name:
        return
    revisions = {}
    for row in conn.execute('SELECT workspace_id, photo_id, group_id, candidates FROM photo_identity_alternatives').fetchall():
        candidates = _renamed_candidates(json.loads(row['candidates']), old_name, new_name)
        if candidates is not None:
            conn.execute(
                'UPDATE photo_identity_alternatives SET candidates=?, group_id=? WHERE workspace_id=? AND photo_id=?',
                (json.dumps(candidates), revisions.setdefault(row['group_id'], uuid.uuid4().hex),
                 row['workspace_id'], row['photo_id']),
            )
    rows = conn.execute(
        'SELECT i.id, i.old_value, i.new_value FROM edit_history_items i '
        "JOIN edit_history e ON e.id=i.edit_id WHERE e.action_type='identity_alternatives'",
    ).fetchall()
    for row in rows:
        for column in ('old_value', 'new_value'):
            try:
                data = json.loads(row[column])
            except (TypeError, ValueError):
                continue
            identity = data.get('identity') if isinstance(data, dict) else None
            if not isinstance(identity, dict):
                continue
            candidates = _renamed_candidates(identity.get('candidates'), old_name, new_name)
            if candidates is not None:
                identity['candidates'] = candidates
                identity['group_id'] = revisions.setdefault(identity['group_id'], uuid.uuid4().hex)
                conn.execute(
                    f'UPDATE edit_history_items SET {column}=? WHERE id=?',
                    (json.dumps(data, sort_keys=True), row['id']),
                )


class IdentityAlternativesRepository:
    def __init__(
        self, conn: sqlite3.Connection, workspace_id: Callable[[], int], *,
        lookup_taxon_id: Callable[..., int | None],
    ) -> None:
        self.conn = conn
        self._workspace_id = workspace_id
        self._lookup_taxon_id = lookup_taxon_id

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
        # A candidate need not have a keyword row yet: its common name may
        # exist only in the taxonomy while the photo carries another alias.
        taxa = {
            taxon_id for name in candidates
            if (taxon_id := self._lookup_taxon_id(name, species_only=True)) is not None
        }
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
