"""Validate assignment evidence and preserve it through ordinary edit history."""

from __future__ import annotations

import json
import math
from typing import TYPE_CHECKING, Any  # Dynamic request JSON and history item mappings.

if TYPE_CHECKING:
    from db import Database

METHODS = frozenset({'manual_unspecified', 'direct_visual', 'adjacent_frames',
                     'model_proposed', 'imported_unknown'})


def validate_provenance(db: Database, photo_ids: list[int], value: Any) -> dict[str, Any]:
    """Validate confidence meaning, accessible evidence, and subject ownership."""
    if (not isinstance(value, dict) or not isinstance(value.get('method'), str)
            or value['method'] not in METHODS):
        raise ValueError('provenance.method must be one of ' + ', '.join(sorted(METHODS)))
    allowed = {'method', 'reviewer', 'reason', 'evidence_source', 'supporting_photo_ids',
               'detection_id', 'confidence', 'confidence_meaning'}
    if set(value) - allowed:
        raise ValueError('Unknown species provenance fields: ' + ', '.join(sorted(set(value) - allowed)))
    result = {'method': value['method']}
    for key, limit in [('reviewer', 200), ('reason', 4000), ('evidence_source', 1000),
                       ('confidence_meaning', 1000)]:
        text = value.get(key)
        if text is not None and (not isinstance(text, str) or len(text) > limit):
            raise ValueError(f'{key} must be a string of at most {limit} characters')
        result[key] = (text.strip() or None) if text is not None else None
    confidence = value.get('confidence')
    if confidence is not None:
        if (isinstance(confidence, bool) or not isinstance(confidence, (int, float))
                or not math.isfinite(confidence) or not 0 <= confidence <= 1):
            raise ValueError('confidence must be a finite number between 0 and 1, or null')
        if not result['confidence_meaning']:
            raise ValueError('confidence_meaning is required when confidence is supplied')
    result['confidence'] = confidence
    result['confidence_meaning'] = result['confidence_meaning'] or 'Not recorded'
    supporting = value.get('supporting_photo_ids', [])
    if (not isinstance(supporting, list) or len(supporting) > 100
            or any(type(pid) is not int or not 0 < pid < 2**63 for pid in supporting)):
        raise ValueError('supporting_photo_ids must contain at most 100 positive photo IDs')
    for pid in supporting:
        if not db.photos.get(pid, verify_workspace=True):
            raise ValueError('Supporting frames must be in the active workspace')
    result['supporting_photo_ids'] = list(dict.fromkeys(supporting))
    if result['method'] == 'adjacent_frames' and not (supporting or result['reason']):
        raise ValueError('Adjacent-frame inference requires supporting frames or an explanation')
    detection_id = value.get('detection_id')
    if detection_id is not None:
        subject_photo = (db.species_provenance.detection_photo_id(detection_id)
                         if type(detection_id) is int and 0 < detection_id < 2**63 else None)
        if len(photo_ids) != 1 or subject_photo != photo_ids[0]:
            raise ValueError('detection_id must identify a subject in the assigned photo')
    result['detection_id'] = detection_id
    return result


def remember_before(db: Database, photo_id: int, keyword_id: int, *, force: bool = False) -> None:
    """Track explicit evidence before tag/removal; record_edit consumes the snapshot."""
    state = db.species_provenance.state(photo_id, keyword_id)
    if not force and (state is None or not state['is_species']):
        return
    db._species_provenance_before.setdefault((photo_id, keyword_id), state)


def history_items(db: Database, action: str, items: Any) -> list[dict[str, Any]]:
    """Attach evidence snapshots to species/keyword edits, preserving other payloads."""
    items = [dict(item) for item in items]
    if action not in {'keyword_add', 'keyword_remove', 'species_replace', 'prediction_accept',
                      'identity_alternatives'}:
        return items
    pending = db._species_provenance_before
    by_photo = {}
    for (pid, keyword_id), before in pending.items():
        by_photo.setdefault(pid, {})[keyword_id] = before
    for item in items:
        pid = item['photo_id']
        changes = {}
        for keyword_id, before in by_photo.pop(pid, {}).items():
            changes[str(keyword_id)] = {'before': before,
                'after': db.species_provenance.state(pid, keyword_id)}
            pending.pop((pid, keyword_id))
        if changes:
            item['species_provenance'] = json.dumps(changes, sort_keys=True)
    return items


def replay_history(db: Database, items: Any, *, undo: bool) -> None:
    """Restore attribution after the normal association and sidecar handlers."""
    for item in items:
        db.species_provenance.replay(item['photo_id'], dict(item).get('species_provenance'), undo=undo)
    db._species_provenance_before = {}
