"""Record honest alternative identities and resolve them as reversible edits.

The alternatives are review state, not attached taxonomy keywords. Only an
explicit resolution tags a candidate. Flags and unrelated confirmed species
are deliberately outside these snapshots. Snapshot dictionaries use Any
because their serialized JSON contains both identities and keyword metadata.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import uuid
from typing import TYPE_CHECKING, Any

from db import keyword_source_max
from keyword_normalization import keyword_match_key, normalize_keyword_display
from services.pending_changes import queue_keyword_add, queue_keyword_remove

if TYPE_CHECKING:
    from collections.abc import Sequence

    from db import Database

log = logging.getLogger(__name__)


def normalize_candidates(value: object) -> list[str]:
    if not isinstance(value, list) or not 2 <= len(value) <= 20:
        raise ValueError('Provide between 2 and 20 alternative species names')
    names = []
    seen = set()
    for item in value:
        if not isinstance(item, str) or len(item) > 200:
            raise ValueError('Each candidate must be a species name of at most 200 characters')
        name = normalize_keyword_display(item)
        key = keyword_match_key(name)
        if '|' in name:
            raise ValueError("Alternative species names may not contain the '|' hierarchy delimiter")
        if not key or key in seen:
            raise ValueError('Alternative species names must be nonempty and distinct')
        names.append(name)
        seen.add(key)
    return names


def uncertainty_text(value: object, field: str, *, required: bool = False) -> str:
    if not isinstance(value, str) or len(value) > 2000 or (required and not value.strip()):
        raise ValueError(f'{field} must be text of at most 2000 characters' + (' and is required' if required else ''))
    return value.strip()


def _snapshot(identity: dict[str, Any] | None, links: list[dict[str, Any]]) -> str:
    return json.dumps({'identity': identity, 'links': links}, sort_keys=True)


def replay_identity_edit(db: Database, photo_id: int, previous: str, replacement: str) -> None:
    """Apply a recorded delta in either direction, including sidecar intents."""
    before, after = json.loads(previous), json.loads(replacement)
    old_links = {link['keyword_id']: link for link in before['links']}
    new_links = {link['keyword_id']: link for link in after['links']}
    for kid in old_links.keys() - new_links.keys():
        name = db.keywords.name_of(kid)
        db.untag_photo(photo_id, kid, _commit=False)
        if name:
            queue_keyword_remove(db, photo_id, name, _commit=False)
    for kid in new_links.keys() - old_links.keys():
        name = db.keywords.name_of(kid)
        if name:
            db.tag_photo(photo_id, kid, source=keyword_source_max(new_links[kid]['source']), _commit=False)
            queue_keyword_add(db, photo_id, name, _commit=False)
    db.identity_alternatives.set_state(photo_id, after['identity'])


def save_alternatives(
    db: Database, photo_ids: Sequence[int], candidates: list[str], reason: str, missing_evidence: str,
) -> int:
    """Replace candidate confirmations with one unresolved group, atomically."""
    states = db.identity_alternatives.for_photos(photo_ids)
    names = candidates + [name for state in states.values() for name in state['candidates']]
    links = db.identity_alternatives.candidate_links(photo_ids, names)
    state = {
        'group_id': uuid.uuid4().hex, 'status': 'unresolved', 'candidates': candidates,
        'reason': reason, 'missing_evidence': missing_evidence,
    }
    items = []
    for pid in photo_ids:
        old = _snapshot(states.get(pid), links.get(pid, []))
        new = _snapshot(state, [])
        replay_identity_edit(db, pid, old, new)
        items.append({'photo_id': pid, 'old_value': old, 'new_value': new})
    return db.record_edit(
        'identity_alternatives', f'Saved unresolved species alternatives on {len(photo_ids)} photos',
        None, items, is_batch=len(photo_ids) > 1, _commit=False,
    )


def resolve_alternatives(db: Database, photo_ids: Sequence[int], species: str) -> int:
    """Confirm one candidate while retaining unrelated species and photo flags."""
    states = db.identity_alternatives.for_photos(photo_ids)
    key = keyword_match_key(species)
    if any(pid not in states for pid in photo_ids):
        raise ValueError('Every photo must still have unresolved alternatives; reload the group')
    if any(key not in {keyword_match_key(n) for n in states[pid]['candidates']} for pid in photo_ids):
        raise ValueError('Choose one of the recorded alternative species')
    # Use the normal species keyword resolver and provenance/XMP queue.
    kid = db.add_keyword(species, is_species=True, _commit=False)
    names = [name for state in states.values() for name in state['candidates']]
    old_links = db.identity_alternatives.candidate_links(photo_ids, names)
    target_links = db.identity_alternatives.candidate_links(photo_ids, [species])
    items = []
    for pid in photo_ids:
        links = old_links.get(pid, [])
        # Keep equivalent hierarchical target tags rather than attaching a
        # redundant root; every other candidate association is removed.
        equivalents = target_links.get(pid) or [{'keyword_id': kid, 'source': 'manual'}]
        old = _snapshot(states[pid], links)
        new = _snapshot(None, equivalents)
        replay_identity_edit(db, pid, old, new)
        items.append({'photo_id': pid, 'old_value': old, 'new_value': new})
    return db.record_edit(
        'identity_alternatives', f'Resolved species as "{species}" on {len(photo_ids)} photos',
        None, items, is_batch=len(photo_ids) > 1, _commit=False,
    )


def groups_for_review(db: Database) -> list[dict[str, Any]]:
    groups = {}
    for pid, state in db.identity_alternatives.for_photos().items():
        group = groups.setdefault(state['group_id'], {**state, 'photo_ids': []})
        group['photo_ids'].append(pid)
    return list(groups.values())


def refresh_species_caches(db: Database, cache_dir: str, photo_ids: Sequence[int]) -> list[int]:
    """Refresh every workspace affected by catalog-wide species associations.

    Call after committing the photo edit, without holding a workspace regroup
    lock. Each cache has its own lock; no operation holds two workspace locks
    or waits on regroup while retaining SQLite's writer lock. Read species
    inside each lock so a queued refresh cannot publish an older tag snapshot.
    """
    from pipeline import refresh_cache_species_for_photos
    from pipeline_locks import acquire_workspace_regroup

    workspace_ids = {db.require_workspace_id()}
    workspace_ids.update(row['id'] for row in db.photo_visibility.affected_workspaces(photo_ids))
    failed = []
    for workspace_id in sorted(workspace_ids):
        try:
            with acquire_workspace_regroup(workspace_id):
                species = db.photo_species.get_species_for_photos(photo_ids)
                for pid in photo_ids:
                    species.setdefault(pid, [])
                refresh_cache_species_for_photos(cache_dir, workspace_id, species)
        except (OSError, ValueError, TypeError, KeyError, sqlite3.Error):
            log.exception('Failed to refresh species in Process Review for workspace %s', workspace_id)
            failed.append(workspace_id)
    return failed
