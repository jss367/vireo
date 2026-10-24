"""Compact species edit contracts, shared by individual and bulk assignments."""
from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from db import Database
    from services.service_types import JsonDict


def encounter_changes(before: JsonDict | None, after: JsonDict | None) -> JsonDict:
    """Send changed groups only, using ordered membership as their stable key.

    Remove the old memberships, then insert upserts in ascending final index.
    Unchanged encounters keep their relative order, including across a split.
    """
    old = {tuple(e['photo_ids']): e for e in (before or {}).get('encounters', [])}
    new = {tuple(e['photo_ids']): e for e in (after or {}).get('encounters', [])}
    return {
        'base_structure_digest': encounter_structure_digest(before),
        'removed': [list(key) for key, enc in old.items() if new.get(key) != enc],
        'upserts': [{'index': i, 'encounter': enc}
                    for i, enc in enumerate((after or {}).get('encounters', []))
                    if old.get(tuple(enc['photo_ids'])) != enc],
    }


def encounter_structure_digest(results: JsonDict | None) -> str:
    """Bind a patch to ordered encounter/burst membership without sending it."""
    structure = [{
        'photo_ids': enc.get('photo_ids') or [],
        'bursts': [(burst.get('photo_ids') or []) if isinstance(burst, dict) else (burst or [])
                   for burst in enc.get('bursts') or []],
    } for enc in (results or {}).get('encounters', [])]
    return hashlib.sha256(json.dumps(structure, separators=(',', ':')).encode()).hexdigest()


def undo_reference(edit_id: int | None) -> JsonDict | None:
    """An optional guarded reference to the ordinary workspace LIFO undo."""
    return {'edit_id': edit_id, 'endpoint': '/api/undo', 'requires_latest': True} if edit_id else None


def assignment_result(
    db: Database, keyword_id: int | None, photo_ids: list[int], *,
    added: list[int], removed: set[int], low_confidence: list[int],
) -> JsonDict:
    """Count actual keyword changes, independently of confirmation-cache changes."""
    changed = set(added) | removed
    keyword = db.keywords.species_assignment_identity(keyword_id) if keyword_id else None
    return {
        'photo_ids': photo_ids,
        'changed_photo_ids': [pid for pid in photo_ids if pid in changed],
        'unchanged_photo_ids': [pid for pid in photo_ids if pid not in changed],
        'keyword': keyword,
        'counts': {'selected': len(photo_ids), 'changed': len(changed),
                   'unchanged': len(photo_ids) - len(changed), 'added': len(added), 'removed': len(removed)},
        'warnings': ([{'code': 'low_detector_confidence', 'photo_ids': low_confidence}]
                     if low_confidence else []),
    }
