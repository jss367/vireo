"""Attributed challenges and resolutions for individual saved species assignments."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from services import keyword_tagging
from services.identity_alternatives import (
    IdentityHistoryConflict,
    IdentityHistoryStale,
    normalize_candidates,
    reconcile_keyword_intents,
)

if TYPE_CHECKING:
    from db import Database


ACTION = 'assignment_review'


def _snapshot(db: Database, pid: int, kids: list[int], rid: str) -> dict[str, Any]:
    return {'review_id': rid, 'record': db.species_provenance.reviews(pid).get(rid),
            'links': {str(k): db.species_provenance.state(pid, k) for k in kids},
            'keywords': {str(k): db.keywords.species_assignment_identity(k) for k in kids}}


def replay(db: Database, pid: int, before: dict[str, Any], after: dict[str, Any]) -> None:
    """Compare every affected association under the writer lock before changing any."""
    rid = before['review_id']
    kids = [int(k) for k in before['links']]
    if not db.photos.get(pid, verify_workspace=True):
        raise IdentityHistoryConflict('Restore access to the reviewed photo before editing its history.')
    if _snapshot(db, pid, kids, rid) != before:
        raise IdentityHistoryStale('The reviewed assignment changed. Reload the photo before editing.')
    changed_names = []
    for kid in kids:
        old, new = before['links'][str(kid)], after['links'][str(kid)]
        if old is None and new is not None:
            keyword_tagging.tag_photo(db, pid, kid)
        elif old is not None and new is None:
            keyword_tagging.untag_photo(db, pid, kid)
        if (old is None) != (new is None):
            changed_names.append(before['keywords'][str(kid)]['name'])
        db.species_provenance.restore(pid, kid, new)
    if changed_names:
        reconcile_keyword_intents(db, pid, changed_names)
    db.species_provenance.save_review(pid, rid, after['record'])


def _review_note(body: dict[str, Any]) -> dict[str, str]:
    note = {}
    for key in ('reviewer', 'reason', 'evidence_source'):
        value = body.get(key)
        if not isinstance(value, str) or not value.strip() or len(value) > 2000:
            raise ValueError(f'{key} requires 1 to 2000 characters')
        note[key] = value.strip()
    note['recorded_at'] = datetime.now(UTC).isoformat()
    return note


def _replacement_keyword(db: Database, pid: int, species: object) -> int:
    name = normalize_candidates([species], minimum=1)[0]
    if db.identity_alternatives.candidate_links([pid], [name]):
        raise ValueError('The replacement species is already assigned to this photo.')
    unresolved = db.identity_alternatives.for_photos([pid]).get(pid)
    if unresolved and (name.casefold() in {n.casefold() for n in unresolved['candidates']} or
            db.identity_alternatives.candidate_identities([name]) &
            db.identity_alternatives.candidate_identities(unresolved['candidates'])):
        raise ValueError('Resolve the outstanding identity alternatives before assigning that species.')
    existing = db.keywords.scoped_species_keyword_id(name)
    return existing if existing is not None else db.keywords.add(name, is_species=True)


def decide(db: Database, pid: int, kid: int, body: dict[str, Any]) -> int:
    assignment = next((a for a in db.species_provenance.assignments(pid) if a['keyword_id'] == kid), None)
    if not assignment or body.get('review_version') != assignment['review_version']:
        raise IdentityHistoryConflict('The reviewed assignment changed. Reload the photo before editing.')
    action = body.get('action')
    current = assignment['review']
    pending = current and not current.get('resolution')
    if action not in ('report', 'retain', 'amend', 'remove') or bool(pending) != (action != 'report'):
        raise ValueError('Report a conflict on a saved assignment, or resolve its outstanding conflict.')
    note = _review_note(body)
    kids = [kid]
    if action == 'amend':
        kids.append(_replacement_keyword(db, pid, body.get('species')))
    rid = uuid.uuid4().hex if action == 'report' else current['id']
    before = _snapshot(db, pid, kids, rid)
    links = dict(before['links'])
    original = links[str(kid)]
    provenance = json.loads(original['species_provenance']) if original['species_provenance'] else {
        k: assignment[k] for k in ('method', 'confidence', 'confidence_meaning')}
    if action == 'report':
        provenance.pop('review_id', None)
        record = {'id': rid, 'original_photo_id': pid, 'species': assignment['species'], 'keyword_id': kid,
                  'original_source': original['source'], 'original_provenance': provenance,
                  'conflict': note, 'resolution': None}
        links[str(kid)] = {**original, 'species_provenance': json.dumps({**provenance, 'review_id': rid}, sort_keys=True)}
    else:
        record = {**current, 'resolution': {**note, 'action': action}}
        if action in ('amend', 'remove'):
            links[str(kid)] = None
        if action == 'amend':
            record['resolution']['species'] = before['keywords'][str(kids[1])]['name']
            evidence = {'method': 'manual_unspecified', 'reviewer': note['reviewer'], 'reason': note['reason'],
                        'evidence_source': note['evidence_source'], 'review_id': rid}
            links[str(kids[1])] = {'source': 'manual', 'is_species': 1,
                                   'species_provenance': json.dumps(evidence, sort_keys=True)}
    after = {**before, 'record': record, 'links': links}
    replay(db, pid, before, after)
    return db.record_edit(ACTION, f"Species assignment review ({action}): {assignment['species']}", None,
                          [{'photo_id': pid, 'old_value': json.dumps(before), 'new_value': json.dumps(after)}],
                          source=note['reviewer'], reason=note['reason'])
