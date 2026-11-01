"""Catalog-wide alternative identities, filtered by workspace visibility.

Resolved revision markers remain hidden from review lists so history can detect
newer decisions even when a photo was resolved to the same species again. A shared group_id keeps a batch of
frames together without asserting that every candidate occurs in a frame.
"""

import contextlib
import json
import sqlite3
import uuid
from collections.abc import Callable, Collection, Iterator
from functools import partial
from typing import Any

from keyword_normalization import keyword_match_key
from repositories.keyword_identity_sql import identity_sql
from repositories.species_taxon_lookup import lookup_taxon_id as lookup_species_taxon_id
from repositories.sync import SyncRepository
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
    for row in conn.execute('SELECT photo_id, group_id, candidates FROM photo_identity_alternatives').fetchall():
        candidates = _renamed_candidates(json.loads(row['candidates']), old_name, new_name)
        if candidates is not None:
            conn.execute(
                'UPDATE photo_identity_alternatives SET candidates=?, group_id=? WHERE photo_id=?',
                (json.dumps(candidates), revisions.setdefault(row['group_id'], uuid.uuid4().hex),
                 row['photo_id']),
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


@contextlib.contextmanager
def identity_merge_transaction(conn: sqlite3.Connection) -> Iterator[None]:
    """Rollback an entire photo-collapse batch without committing its caller."""
    started = not conn.in_transaction
    if started:
        conn.execute('BEGIN IMMEDIATE')
    conn.execute('SAVEPOINT identity_photo_merge')
    succeeded = False
    try:
        yield
        succeeded = True
    finally:
        if not succeeded:
            conn.execute('ROLLBACK TO identity_photo_merge')
        conn.execute('RELEASE identity_photo_merge')
        if not succeeded and started:
            conn.rollback()


class IdentityMergeConflict(ValueError):
    """Two photo rows have distinct decisions that cannot be chosen implicitly."""


def merge_candidate_names(conn: sqlite3.Connection, losing: int, survivor: int) -> list[str]:
    """Include redo-only decisions; read actual links rather than restoring snapshots."""
    names = set()
    for row in conn.execute('SELECT candidates FROM photo_identity_alternatives WHERE photo_id IN (?, ?)', (losing, survivor)):
        names.update(json.loads(row['candidates']))
    for row in conn.execute(
        'SELECT i.old_value,i.new_value FROM edit_history_items i JOIN edit_history e ON e.id=i.edit_id '
        "WHERE i.photo_id IN (?, ?) AND e.action_type IN ('identity_alternatives','identity_alternatives_superseded')", (losing, survivor),
    ):
        for value in row:
            data = json.loads(value)
            if data.get('identity'):
                names.update(data['identity']['candidates'])
    return sorted(names)


def validate_identity_photo_merge(conn: sqlite3.Connection, losing: int, survivor: int) -> list[str]:
    """Check both live rows before a caller copies or deletes their associations."""
    rows = conn.execute('SELECT group_id,candidates,reason,missing_evidence,status FROM photo_identity_alternatives WHERE photo_id IN (?, ?)', (losing, survivor)).fetchall()
    if len(rows) == 2 and tuple(rows[0]) != tuple(rows[1]):
        raise IdentityMergeConflict('Photo collision has conflicting identity decisions; reconcile both photos before merging')
    names = merge_candidate_names(conn, losing, survivor)
    if names:
        state = dict(rows[0]) if rows else {'candidates': json.dumps(names), 'status': 'resolved'}
        _check_merge_confirmations(conn, losing, survivor, state)
    return names


def identity_merge_associations(conn: sqlite3.Connection, losing: int, survivor: int) -> list[dict[str, Any]]:
    """Current candidate links that must travel before deleting the losing row."""
    names = validate_identity_photo_merge(conn, losing, survivor)
    return [{'id': link['keyword_id'], 'source': link['source']} for link in candidate_links(
        conn, [losing], names, lookup_taxon_id=partial(lookup_species_taxon_id, conn),
    ).get(losing, [])] if names else []


def remap_identity_keyword_intents(conn: sqlite3.Connection, losing: int, survivor: int, names: list[str]) -> dict[str | tuple[str, int], str]:
    """Retain captured-write flags; an old-path writer cannot acknowledge the new owner."""
    moved = {}
    if not names:
        return moved
    keys = {keyword_match_key(name) for name in names}
    lookup = partial(lookup_species_taxon_id, conn)
    identities = candidate_identities(conn, names, lookup_taxon_id=lookup)
    alias_identities: dict[str, set[str]] = {}
    for row in conn.execute(
        "SELECT id,value,change_token FROM pending_changes WHERE photo_id=? AND change_type IN ('keyword_add','keyword_remove','keyword_remove_flat')", (losing,),
    ).fetchall():
        name = row['value'] or ''
        key = keyword_match_key(name)
        matches = key in keys
        if not matches and identities:
            if key not in alias_identities:
                alias_identities[key] = candidate_identities(conn, [name], lookup_taxon_id=lookup)
            matches = bool(identities & alias_identities[key])
        if matches:
            token = str(uuid.uuid4())
            conn.execute('UPDATE pending_changes SET photo_id=?,change_token=? WHERE id=?', (survivor, token, row['id']))
            moved[row['change_token'] or ('id', row['id'])] = token
    return moved


def _repair_identity_merge_sidecar(conn: sqlite3.Connection, survivor: int, names: list[str]) -> None:
    """A drained source queue does not prove the destination sidecar is current."""
    if not names:
        return
    # Catalog-only merges have no Database queue callback. Use the queue's own
    # normalization/token writer with explicit visible owners and no commit.
    queue = SyncRepository(conn, lambda: 0)
    current_names = {row['id']: row['name'] for row in conn.execute(
        'SELECT k.id,k.name FROM photo_keywords pk JOIN keywords k ON k.id=pk.keyword_id WHERE pk.photo_id=?', (survivor,),
    )}
    current = {keyword_match_key(name): name for name in current_names.values()}
    affected = {keyword_match_key(name): name for name in names}
    # Confirmed aliases may not use any candidate spelling. Repair the names
    # actually carried, just as the service reconciliation does after replay.
    for link in candidate_links(conn, [survivor], names, lookup_taxon_id=partial(lookup_species_taxon_id, conn)).get(survivor, []):
        name = current_names[link['keyword_id']]
        affected[keyword_match_key(name)] = name
    ancestors = {keyword_match_key(name) for name in queue.retained_keyword_ancestor_names(survivor)}
    workspaces = [row['workspace_id'] for row in conn.execute(
        'SELECT workspace_id FROM photo_workspace_visibility WHERE photo_id=?', (survivor,),
    )]
    for name in affected.values():
        key = keyword_match_key(name)
        kind = 'keyword_add' if key in current else ('keyword_remove_flat' if key in ancestors else 'keyword_remove')
        for workspace in workspaces:
            queue.queue(survivor, kind, current.get(key, name), workspace_id=workspace)


def remap_photo_identities(conn: sqlite3.Connection, mapping: dict[int, int | None]) -> None:
    """Carry shared identity and its audit before duplicate-photo deletion.

    Distinct decisions have no reliable chronology, so the caller must roll
    back and let the user reconcile them. Normal deletion still cascades.
    History snapshots contain keyword IDs and identity revisions, not photo
    IDs; their enclosing items follow the surviving photo. Caller commits.
    """
    for losing, survivor in mapping.items():
        if survivor is None or losing == survivor:
            continue
        names = merge_candidate_names(conn, losing, survivor)
        source = conn.execute('SELECT group_id, candidates, reason, missing_evidence, status FROM photo_identity_alternatives WHERE photo_id=?', (losing,)).fetchone()
        target = conn.execute('SELECT group_id, candidates, reason, missing_evidence, status FROM photo_identity_alternatives WHERE photo_id=?', (survivor,)).fetchone()
        if source is not None and target is not None and tuple(source) != tuple(target):
            raise IdentityMergeConflict(
                f'Photos {losing} and {survivor} have different identification decisions (conflicting identity decisions). '
                'Reconcile their alternatives and evidence in Review before merging these photo rows.'
            )
        state = target if target is not None else source
        if (state is not None and state['status'] == 'unresolved'
                and candidate_links(conn, [survivor], json.loads(state['candidates']),
                                    lookup_taxon_id=partial(lookup_species_taxon_id, conn))):
            raise IdentityMergeConflict(
                f'Photo {survivor} has conflicting species confirmations and unresolved alternatives. '
                'Reconcile the species confirmation in Review before merging these photo rows.'
            )
        if source is not None and target is None:
            conn.execute('UPDATE photo_identity_alternatives SET photo_id=? WHERE photo_id=?', (survivor, losing))
        if state is not None:
            _check_merge_identity_intents(conn, losing, survivor, json.loads(state['candidates']))
        remap_identity_keyword_intents(conn, losing, survivor, names)
        _repair_identity_merge_sidecar(conn, survivor, names)
        _remap_identity_history(conn, losing, survivor)


def remap_identity_for_photo_merge(
    conn: sqlite3.Connection, losing_id: int, survivor_id: int, KEYWORD_SOURCE_CONFLICT_SQL: str,
    *, after_reconciliation: bool = False,
) -> None:
    """Preserve decisions and replay snapshots before a collision deletes a row.

    Matching decisions (including revision) can collapse. Distinct decisions
    cannot be ordered reliably: group ids are random and history may have been
    pruned. Refuse that collision atomically instead of discarding evidence or
    replacing a confirmation. Candidate confirmations require compatible
    identity evidence; unrelated species stay independent. Caller commits.
    """
    states = {r['photo_id']: dict(r) for r in conn.execute(
        'SELECT * FROM photo_identity_alternatives WHERE photo_id IN (?, ?)',
        (losing_id, survivor_id),
    )}
    names = merge_candidate_names(conn, losing_id, survivor_id)
    if not states and not names:
        _remap_identity_history(conn, losing_id, survivor_id)
        return
    old, new = states.get(losing_id), states.get(survivor_id)
    if old and new and {k: v for k, v in old.items() if k != 'photo_id'} != {
        k: v for k, v in new.items() if k != 'photo_id'
    }:
        raise IdentityMergeConflict('Photo collision has conflicting identity decisions. Reconcile their alternatives and evidence in Review before merging these photo rows.')
    decision = old or new or {'candidates': json.dumps(names), 'status': 'resolved'}
    if not after_reconciliation:
        _check_merge_confirmations(conn, losing_id, survivor_id, decision)
    links: dict[int, dict[int, tuple[str | None, str | None]]] = {}
    for pid in (losing_id, survivor_id):
        links[pid] = {r['keyword_id']: (r['source'], r['species_provenance']) for r in conn.execute(
            'SELECT pk.keyword_id, pk.source, pk.species_provenance '
            'FROM photo_keywords pk JOIN keywords k ON k.id=pk.keyword_id '
            "WHERE pk.photo_id=? AND (k.is_species=1 OR k.type='taxonomy')", (pid,),
        )}
    _remap_identity_history(conn, losing_id, survivor_id)
    if old and not new:
        conn.execute('UPDATE photo_identity_alternatives SET photo_id=? WHERE photo_id=?',
                     (survivor_id, losing_id))
    if not after_reconciliation:
        return
    pending = [(row['change_type'], keyword_match_key(row['value'])) for row in conn.execute(
        "SELECT change_type,value FROM pending_changes WHERE photo_id IN (?, ?) "
        "AND change_type IN ('keyword_add','keyword_remove')", (losing_id, survivor_id),
    )]
    removed = {name for kind, name in pending if kind == 'keyword_remove'} - {name for kind, name in pending if kind == 'keyword_add'}
    uncertain = set()
    if decision['status'] == 'unresolved':
        uncertain = {link['keyword_id'] for link in candidate_links(
            conn, [losing_id], json.loads(decision['candidates']),
            lookup_taxon_id=partial(lookup_species_taxon_id, conn),
        ).get(losing_id, [])}
    from repositories.species_provenance import evidence_for_photo_copy
    for kid, (source, species_provenance) in links[losing_id].items():
        name = keyword_match_key(conn.execute('SELECT name FROM keywords WHERE id=?', (kid,)).fetchone()[0])
        if kid not in uncertain and name not in removed:
            # Carry the losing side's species evidence onto the survivor as
            # well; the shared conflict clause keeps the survivor's existing
            # evidence when both sides had one.
            conn.execute(
                'INSERT INTO photo_keywords (photo_id, keyword_id, source, species_provenance) '
                'VALUES (?, ?, ?, ?) ' + KEYWORD_SOURCE_CONFLICT_SQL,
                (survivor_id, kid, source, evidence_for_photo_copy(species_provenance, losing_id)))



def _check_merge_confirmations(conn: sqlite3.Connection, losing: int, survivor: int, state: dict[str, Any]) -> None:
    """Require evidence for reconciling actual candidate confirmations."""
    lookup = partial(lookup_species_taxon_id, conn)
    removals = {(row['photo_id'], keyword_match_key(row['value'])) for row in conn.execute(
        "SELECT photo_id,value FROM pending_changes WHERE photo_id IN (?, ?) AND change_type='keyword_remove'",
        (losing, survivor),
    )}
    for row in conn.execute(
        "SELECT pk.photo_id,k.name FROM photo_keywords pk JOIN keywords k ON k.id=pk.keyword_id "
        "JOIN taxa t ON t.id=k.taxon_id WHERE pk.photo_id IN (?, ?) "
        "AND t.rank IS NOT NULL AND t.rank!='species'", (losing, survivor),
    ):
        key = keyword_match_key(row['name'])
        other = survivor if row['photo_id'] == losing else losing
        if (other, key) in removals:
            raise IdentityMergeConflict(
                'Photo collision has a queued removal sharing a name with higher-rank taxonomy metadata. '
                'Reconcile that keyword in Review before merging these photo rows.'
            )
    links = candidate_links(conn, [losing, survivor], json.loads(state['candidates']), lookup_taxon_id=lookup)
    if state['status'] == 'unresolved':
        pending = {(row['photo_id'], row['change_type'], keyword_match_key(row['value'])) for row in conn.execute(
            "SELECT photo_id, change_type, value FROM pending_changes WHERE photo_id IN (?, ?) "
            "AND change_type IN ('keyword_add','keyword_remove')", (losing, survivor),
        )}
        for pid, attached in links.items():
            for link in attached:
                key = keyword_match_key(conn.execute('SELECT name FROM keywords WHERE id=?', (link['keyword_id'],)).fetchone()[0])
                other = survivor if pid == losing else losing
                # Only the existing merge's dated opposing writes can settle
                # this presence. An already-synced confirmation has no date.
                if ((pid, 'keyword_add', key) not in pending
                        or (other, 'keyword_remove', key) not in pending):
                    raise IdentityMergeConflict(
                        'Photo collision has conflicting species confirmations. '
                        'Reconcile the species confirmation in Review before merging these photo rows.'
                    )
    elif links.get(losing) and links.get(survivor):
        identities = {}
        linked_ids = sorted({link['keyword_id'] for photo in (losing, survivor) for link in links[photo]})
        for ids in chunked(linked_ids):
            identities.update({row['id']: row['identity'] for row in conn.execute(
                f"SELECT k.id, {identity_sql('k')} AS identity FROM keywords k "
                f"WHERE k.id IN ({','.join('?' for _ in ids)})", ids,
            )})
        if {identities[link['keyword_id']] for link in links[losing]} != {
            identities[link['keyword_id']] for link in links[survivor]
        }:
            raise IdentityMergeConflict('Photo collision has conflicting species confirmations; review both photos before archiving')


def _check_merge_identity_intents(conn: sqlite3.Connection, losing: int, survivor: int, candidates: list[str]) -> None:
    """Reject only affected candidate writes inconsistent with final tags."""
    lookup = partial(lookup_species_taxon_id, conn)
    names = {keyword_match_key(name) for name in candidates}
    identities = candidate_identities(conn, candidates, lookup_taxon_id=lookup)
    present = {keyword_match_key(row['name']) for row in conn.execute(
        'SELECT k.name FROM photo_keywords pk JOIN keywords k ON k.id=pk.keyword_id WHERE pk.photo_id=?', (survivor,),
    )}
    for row in conn.execute(
        "SELECT change_type,value FROM pending_changes WHERE photo_id IN (?, ?) "
        "AND change_type IN ('keyword_add','keyword_remove')", (losing, survivor),
    ):
        name = (row['value'] or '').rsplit('|', 1)[-1]
        key = keyword_match_key(name)
        relevant = key in names or identities & candidate_identities(conn, [name], lookup_taxon_id=lookup)
        if relevant and ((row['change_type'] == 'keyword_add') != (key in present)):
            raise IdentityMergeConflict('Photo collision has conflicting species edits; review both photos before archiving')


def _remap_identity_history(conn: sqlite3.Connection, losing_id: int, survivor_id: int) -> None:
    """Deduplicate identical batch items; refuse ambiguous replay of one photo."""
    conn.execute(
        'UPDATE edit_history_items SET photo_id=? WHERE photo_id=? AND edit_id IN '
        "(SELECT id FROM edit_history WHERE action_type='identity_alternatives_superseded')", (survivor_id, losing_id),
    )
    rows = conn.execute(
        'SELECT i.* FROM edit_history_items i JOIN edit_history e ON e.id=i.edit_id '
        "WHERE e.action_type='identity_alternatives' AND i.photo_id=?", (losing_id,),
    ).fetchall()
    for row in rows:
        twins = conn.execute('SELECT * FROM edit_history_items WHERE edit_id=? AND photo_id=?',
                             (row['edit_id'], survivor_id)).fetchall()
        if any((t['old_value'], t['new_value']) != (row['old_value'], row['new_value']) for t in twins):
            raise IdentityMergeConflict('Photo collision has conflicting identity history; review both photos before archiving')
        if twins:
            conn.execute('DELETE FROM edit_history_items WHERE id=?', (row['id'],))
        else:
            conn.execute('UPDATE edit_history_items SET photo_id=? WHERE id=?', (survivor_id, row['id']))


class IdentityAlternativesRepository:
    def __init__(
        self, conn: sqlite3.Connection, workspace_id: Callable[[], int], *,
        lookup_taxon_id: Callable[..., int | None], keyword_source_conflict_sql: str,
    ) -> None:
        self.conn = conn
        self._workspace_id = workspace_id
        self._lookup_taxon_id = lookup_taxon_id
        self._keyword_source_conflict_sql = keyword_source_conflict_sql

    def merge_candidate_names(self, losing: int, survivor: int) -> list[str] | None:
        """Current or audited affected names, including redo-only decisions."""
        return merge_candidate_names(self.conn, losing, survivor) or None

    def prepare_photo_merge(self, losing: int, survivor: int, *, after_reconciliation: bool = False) -> None:
        """Carry current identity/associations inside the caller's transaction."""
        remap_identity_for_photo_merge(self.conn, losing, survivor, self._keyword_source_conflict_sql,
                                      after_reconciliation=after_reconciliation)

    def for_photos(self, photo_ids: Collection[int] | None = None, *, include_resolved: bool = False) -> dict[int, dict[str, Any]]:
        """Visible unresolved photos in this workspace, optionally restricted."""
        ws = self._workspace_id()
        result = {}
        batches = chunked(list(dict.fromkeys(photo_ids))) if photo_ids is not None else [None]
        for ids in batches:
            clause = "" if include_resolved else " AND a.status='unresolved'"
            if ids is not None:
                clause += f" AND a.photo_id IN ({','.join('?' for _ in ids)})"
            rows = self.conn.execute(
                "SELECT a.* FROM photo_identity_alternatives a "
                "JOIN photo_workspace_visibility v ON v.photo_id=a.photo_id AND v.workspace_id=? "
                "WHERE 1=1" + clause + " ORDER BY a.photo_id",
                [ws, *(ids or [])],
            )
            for row in rows:
                result[row['photo_id']] = {
                    'group_id': row['group_id'], 'candidates': json.loads(row['candidates']),
                    'reason': row['reason'], 'missing_evidence': row['missing_evidence'],
                    'status': row['status'],
                }
        return result

    def photo_ids(self) -> set[int]:
        return set(self.for_photos())

    def set_state(self, photo_id: int, state: dict[str, Any] | None) -> None:
        """Write without committing; the decision/history transaction owns it."""
        if state is None:
            self.conn.execute(
                'DELETE FROM photo_identity_alternatives WHERE photo_id=?', (photo_id,),
            )
        else:
            self.conn.execute(
                'INSERT INTO photo_identity_alternatives '
                '(photo_id, group_id, candidates, reason, missing_evidence, status) VALUES (?, ?, ?, ?, ?, ?) '
                'ON CONFLICT(photo_id) DO UPDATE SET group_id=excluded.group_id, '
                'candidates=excluded.candidates, reason=excluded.reason, missing_evidence=excluded.missing_evidence, status=excluded.status',
                (photo_id, state['group_id'], json.dumps(state['candidates']),
                 state['reason'], state['missing_evidence'], state.get('status', 'unresolved')),
            )

    def candidate_identities(self, candidates: Collection[str]) -> set[str]:
        """Resolve named candidates with the catalog's authoritative source precedence."""
        return candidate_identities(self.conn, candidates, lookup_taxon_id=self._lookup_taxon_id)

    def candidate_links(self, photo_ids: Collection[int], candidates: Collection[str], *, identities: Collection[str] | None = None) -> dict[int, list[dict[str, Any]]]:
        """Existing species associations matching alternatives, with provenance.

        Match normalized names and authoritative local/source taxon identities.
        Unrelated species in a genuine mixed-species frame are left alone.
        """
        return candidate_links(self.conn, photo_ids, candidates, lookup_taxon_id=self._lookup_taxon_id, identities=identities)


def candidate_identities(conn: sqlite3.Connection, candidates: Collection[str], *, lookup_taxon_id: Callable[..., int | None]) -> set[str]:
    """Resolve named candidates with the catalog's authoritative source precedence."""
    names = {keyword_match_key(name) for name in candidates}
    identities, linked_names = set(), set()
    for row in conn.execute(
        f"SELECT k.name, {identity_sql('k')} AS identity FROM keywords k "
        "LEFT JOIN taxa t ON t.id=k.taxon_id "
        "WHERE (k.is_species=1 OR k.type='taxonomy') AND (t.rank='species' OR t.rank IS NULL)",
    ):
        key = keyword_match_key(row['name'])
        if key in names and not row['identity'].startswith('keyword:'):
            identities.add(row['identity'])
            linked_names.add(key)
    # A common name may exist only in the taxonomy. An explicitly linked
    # keyword above owns its identity even if a name lookup would disagree.
    for name in candidates:
        if keyword_match_key(name) in linked_names:
            continue
        taxon = lookup_taxon_id(name, prefer_species=True, species_only=True)
        if taxon is not None:
            row = conn.execute('SELECT inat_id FROM taxa WHERE id=?', (taxon,)).fetchone()
            identities.add(f'inat:{row["inat_id"]}' if row and row['inat_id'] is not None else f'taxon:{taxon}')
    return identities


def candidate_links(conn: sqlite3.Connection, photo_ids: Collection[int], candidates: Collection[str], *, lookup_taxon_id: Callable[..., int | None], identities: Collection[str] | None = None) -> dict[int, list[dict[str, Any]]]:
    """Existing species associations matching alternatives, with provenance.

    Match normalized names and authoritative local/source taxon identities.
    Unrelated species in a genuine mixed-species frame are left alone.
    """
    names = {keyword_match_key(name) for name in candidates}
    if identities is None:
        identities = candidate_identities(conn, candidates, lookup_taxon_id=lookup_taxon_id)
    result = {}
    for ids in chunked(list(photo_ids)):
        rows = conn.execute(
            f"SELECT pk.photo_id, pk.keyword_id, pk.source, k.name, {identity_sql('k')} AS identity "
            'FROM photo_keywords pk JOIN keywords k ON k.id=pk.keyword_id '
            'LEFT JOIN taxa t ON t.id=k.taxon_id '
            f"WHERE pk.photo_id IN ({','.join('?' for _ in ids)}) "
            "AND (k.is_species=1 OR k.type='taxonomy') AND (t.rank='species' OR t.rank IS NULL) "
            'ORDER BY pk.keyword_id', ids,
        )
        for row in rows:
            if keyword_match_key(row['name']) in names or row['identity'] in identities:
                result.setdefault(row['photo_id'], []).append({
                    'keyword_id': row['keyword_id'], 'source': row['source'],
                })
    return result
