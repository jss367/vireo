"""Species-keyed Review cards and decisions confined to observed member rows.

The listing keeps its prediction rows for Browse and per-model filtering. A
card_id links overlapping burst groups without changing stored group identity.
The browser submits the visible members; decisions revalidate them under the
existing writer lock, never expanding beyond the submitted selection.
The caller's decision unit commits the complete decision and its history
once, or rolls all of it back.
"""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING

from services.edit_history import HistoryItem, prediction_status_item
from services.service_types import JsonDict
from species_identity import SpeciesResolver

if TYPE_CHECKING:
    from db import Database


class ReviewCardConflict(ValueError):
    """A rendered card no longer describes a current, pending selection."""


def review_card_components(rows: list[JsonDict]) -> list[list[JsonDict]]:
    """Connected components of same-species groups keyed to the detected subject.

    Build with two indexes rather than pairwise group intersections. Status is
    part of the key so an accepted lead can never hide still-pending evidence.
    Cross-model rows join by ``detection_id`` (not ``photo_id``) so two separate
    detections of the same species in one photo remain two independent cards;
    each subject stays reviewable in isolation.
    ``consensus_species_key`` is the identity that an accept actually applies.
    """
    parents = list(range(len(rows)))

    def root(index: int) -> int:
        while parents[index] != index:
            parents[index] = parents[parents[index]]
            index = parents[index]
        return index

    def join(left: int, right: int) -> None:
        parents[root(left)] = root(right)

    groups: dict[tuple[object, ...], int] = {}
    detections: dict[tuple[object, ...], int] = {}
    for index, row in enumerate(rows):
        key = (row['consensus_species_key'], row['status'])
        if row.get('group_id'):
            group = (*key, row.get('model'), row['group_id'])
            join(index, groups.setdefault(group, index))
        detection = (*key, row['detection_id'])
        join(index, detections.setdefault(detection, index))
    components: dict[int, list[JsonDict]] = {}
    for index, row in enumerate(rows):
        components.setdefault(root(index), []).append(row)
    return [card for members in components.values() for card in _separate_conflicting_subjects(members)]


def _separate_conflicting_subjects(members: list[JsonDict]) -> list[list[JsonDict]]:
    """Fall back to subject cards when burst links cannot distinguish animals.

    A burst can contain two detections from the same frame, directly or via
    another model's overlapping burst. Their correspondence across frames is
    ambiguous, so do not propagate any decision through that component.
    """
    by_detection: dict[int, list[JsonDict]] = {}
    by_photo: dict[int, set[int]] = {}
    for row in members:
        by_detection.setdefault(row['detection_id'], []).append(row)
        by_photo.setdefault(row['photo_id'], set()).add(row['detection_id'])
    if any(len(detections) > 1 for detections in by_photo.values()):
        return list(by_detection.values())
    return [members]


def attach_review_cards(rows: list[JsonDict]) -> None:
    """Attach stable membership IDs without duplicating member arrays per row."""
    for members in review_card_components(rows):
        ids = ','.join(str(pid) for pid in sorted(p['id'] for p in members))
        card_id = 'review:' + hashlib.sha256(ids.encode()).hexdigest()[:24]
        for row in members:
            row['card_id'] = card_id


def validate_review_card(
    db: Database, entry_id: int, ids: list[int], observed_workspace: int,
) -> list[JsonDict]:
    """Re-read current visible rows, their statuses and identities before writes."""
    if observed_workspace != db.require_workspace_id() or (
        observed_workspace != db.workspaces.most_recently_opened_id()
    ):
        raise ReviewCardConflict('The active workspace changed. Reload Review before deciding.')
    if entry_id not in ids:
        raise ReviewCardConflict('The clicked prediction is missing from the reviewed card.')
    photo_by_id = db.predictions.photo_ids_by_prediction(ids)
    wanted = set(ids)
    rows = [dict(p) for p in db.predictions.get_rows(
        photo_ids=list(set(photo_by_id.values())), status='pending',
    ) if p['id'] in wanted]
    if len(rows) != len(wanted):
        raise ReviewCardConflict('The reviewed predictions changed or are no longer visible. Reload Review.')
    resolver = SpeciesResolver(db=db)
    for row in rows:
        identity = resolver.consensus(row)
        row['consensus_species_key'] = identity.key
        row['consensus_species'] = identity.display_name
    if len(review_card_components(rows)) != 1:
        raise ReviewCardConflict('These predictions no longer form one species card. Reload Review.')
    return rows


def accept_review_card(db: Database, rows: list[JsonDict]) -> JsonDict:
    """Accept exact member rows using the ordinary keyword/audit primitives."""
    ids = [p['id'] for p in rows]
    db.predictions.require_identifiable_subjects(ids)
    photo_by_id = {p['id']: p['photo_id'] for p in rows}
    prior_by_photo: dict[int, JsonDict] = {}
    for row in db.predictions.scope_review_statuses(ids):
        prior_by_photo.setdefault(photo_by_id[row['pick_id']], {})[str(row['prediction_id'])] = row['status']
    accepted: set[int] = set()
    # A photo can already carry an equivalent keyword alias. Keep each
    # result's real keyword ID and no_tag bit so undo never removes that tag.
    items_by_key: dict[tuple[int, int], JsonDict] = {}
    species = rows[0]['species']
    for row in rows:
        if row['id'] in accepted:
            continue
        result = db.accept_prediction(row['id'], prediction_ids=ids)
        if result is None:
            raise ReviewCardConflict('A reviewed prediction disappeared. Reload Review.')
        accepted.update(result['accepted_prediction_ids'])
        species = result['species']
        for affected in result['affected']:
            key = (affected['photo_id'], result['keyword_id'])
            item = items_by_key.setdefault(key, {'prediction_ids': [], 'no_tag': True})
            item['prediction_ids'].append(affected['prediction_id'])
            item['no_tag'] = item['no_tag'] and not affected.get('changed_tag', True)
    items: list[HistoryItem] = []
    for (photo_id, keyword_id), meta in items_by_key.items():
        meta['prior_statuses'] = prior_by_photo.get(photo_id, {})
        items.append({'photo_id': photo_id, 'old_value': json.dumps(meta), 'new_value': str(keyword_id)})
    db.record_edit('prediction_accept', f'Accepted Review card: "{species}"', None,
                   items, is_batch=len(items) > 1)
    return {'ok': True, 'prediction_ids': sorted(accepted), 'photo_ids': sorted(set(photo_by_id.values()))}


def reject_review_card(db: Database, rows: list[JsonDict]) -> JsonDict:
    """Reject exact card members and their alternative candidates, with undo."""
    by_photo: dict[int, set[int]] = {}
    for row in rows:
        targets = by_photo.setdefault(row['photo_id'], set())
        targets.add(row['id'])
        targets.update(db.predictions.alternative_sibling_ids(
            row['detection_id'], row['model'], row['labels_fingerprint'], row['id'],
        ))
    items: list[HistoryItem] = []
    for photo_id, targets in by_photo.items():
        def apply(target_ids: set[int] = targets) -> None:
            for prediction_id in target_ids:
                db.update_prediction_status(prediction_id, 'rejected')

        item = prediction_status_item(db, photo_id, sorted(targets), apply)
        if item is not None:
            items.append(item)
    db.record_edit('prediction_status', f'Rejected Review card: "{rows[0]["consensus_species"]}"',
                   'rejected', items, is_batch=len(items) > 1)
    return {'ok': True, 'rejected_prediction_ids': sorted({pid for ids in by_photo.values() for pid in ids}),
            'photo_ids': sorted(by_photo)}
