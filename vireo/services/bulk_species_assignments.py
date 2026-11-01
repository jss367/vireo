"""Atomic additive species assignments over exact photo and subject selections.

Subject IDs constrain the submitted photos; assigning a keyword remains a photo
metadata operation and never accepts, rejects or expands sibling predictions.
The caller validates targets and owns the writer/regroup locks and cache save.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from services.encounter_confirmation import apply_scope
from services.grouping_history import record_grouping_edit
from services.pending_changes import queue_keyword_add
from services.species_assignment_results import assignment_result, encounter_changes, undo_reference

if TYPE_CHECKING:
    from db import Database
    from services.service_types import JsonDict


def apply_assignments(
    db: Database, cached: JsonDict | None, assignments: list[JsonDict], *, before: JsonDict | None,
) -> JsonDict:
    """Apply in order, with one history snapshot for the entire batch."""
    items = []
    results = []
    selected: set[int] = set()
    preferred = []
    for assignment in assignments:
        species = assignment['species']
        ids = assignment['photo_ids']
        kid = db.add_keyword(species, is_species=True)
        species = db.keywords.name_of(kid) or species
        already = db.get_photos_with_equivalent_species(ids, kid)
        added = [pid for pid in ids if pid not in already]
        for pid in added:
            db.tag_photo(pid, kid, source='manual')
            queue_keyword_add(db, pid, species, workspace_id=db.require_workspace_id())
            items.append({'photo_id': pid, 'old_value': '', 'new_value': str(kid)})
        result = assignment_result(db, kid, ids, added=added, removed=set(),
                                   low_confidence=assignment['low_confidence_photo_ids'])
        result.update(species=species, keyword_id=kid, detection_ids=assignment.get('detection_ids', []))
        results.append(result)
        selected.update(ids)
        if species not in preferred:
            preferred.append(species)
    if cached:
        for enc in cached.get('encounters', []):
            ids = [pid for pid in enc['photo_ids'] if pid in selected]
            if ids:
                enc['bursts'] = [{'photo_ids': b} if isinstance(b, list) else b for b in enc.get('bursts', [])]
                apply_scope(db, cached, enc, ids, split=False, preferred_order=preferred)
    delta = encounter_changes(before, cached)
    cache_changed = bool(delta['removed'] or delta['upserts'])
    edit_id = None
    if items or cache_changed:
        change = {'photo_edit': {'action_type': 'keyword_add'}, 'label_edit': True}
        if cached and before:
            change.update(before=before['encounters'], after=cached['encounters'])
        else:
            change['photo_only'] = True
        edit_id = record_grouping_edit(db, f'Assigned {len(assignments)} species selections', change, items)
    changed = {pid for result in results for pid in result['changed_photo_ids']}
    return {
        'ok': True, 'response_mode': 'compact', 'atomic': True, 'assignments': results,
        'photo_ids': sorted(selected), 'changed_photo_ids': sorted(changed),
        'counts': {'assignments': len(results), 'selected': len(selected), 'changed': len(changed),
                   'unchanged': len(selected - changed),
                   'added': sum(result['counts']['added'] for result in results)},
        'warnings': [warning for result in results for warning in result['warnings']],
        'undo': undo_reference(edit_id), 'encounter_changes': delta, 'cache_changed': cache_changed,
        'summary': (cached or {}).get('summary', {}),
    }
