"""Record completed catalog maintenance in history without promising undo.

These actions lack complete restoration snapshots. Their audit records are
explicitly excluded from replay, and commit with the catalog changes they name.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from services import catalog_deletion

if TYPE_CHECKING:
    from db import Database
    from services.service_types import JsonDict


def resolve_drift_with_history(db: Database, photo_id: int, direction: str) -> None:
    """Audit one completed repair; bulk callers retain earlier successes on failure."""
    from audit import resolve_drift

    with db._commits_held():
        db.begin_immediate()
        before = db.audit.drift_history_state(photo_id)
        photo = db.audit.photo_location(photo_id)
        if photo is None:
            return
        resolve_drift(db, photo_id, direction)
        after = db.audit.drift_history_state(photo_id)
        if before == after:
            return
        verb = 'Queued catalog keywords for XMP' if direction == 'use_db' else 'Read XMP keywords into catalog'
        db.record_edit(
            'audit_resolve', f'{verb}: {photo["filename"]} (cannot undo)',
            json.dumps({'photo_id': photo_id, 'direction': direction}),
            [{'photo_id': photo_id, 'old_value': before, 'new_value': after}],
        )


def remove_orphans_with_history(db: Database, photo_ids: list[int]) -> JsonDict:
    """Keep deletion and its durable audit together; defer cache work until commit."""
    from audit import confirmed_orphan_ids

    with db._commits_held():
        db.begin_immediate()
        confirmed = confirmed_orphan_ids(db, photo_ids)
        result = catalog_deletion.delete_photos(db, confirmed, commit=False)
        if result['deleted']:
            # Photo-linked items cascade away on deletion. Keep the removed
            # IDs and paths on the parent entry so the audit survives that FK.
            db.record_edit(
                'audit_remove_orphans',
                f'Removed {result["deleted"]} missing photos from catalog (cannot undo)',
                json.dumps({'removed': result['deleted'], 'photos': result['files']}),
                [], is_batch=True,
            )
    if result['deleted']:
        catalog_deletion.prune_pipeline_cache_for_ids(db, result['ids'])
    return result


def clean_keywords_with_history(db: Database) -> int:
    """Record successful duplicate merges in the same transaction as the merge."""
    with db._commits_held():
        db.begin_immediate()
        merged = db.keyword_tree.merge_duplicates()
        if merged:
            db.record_edit(
                'keyword_clean', f'Merged {merged} duplicate keywords (cannot undo)',
                json.dumps({'merged': merged}), [], is_batch=True,
            )
    return merged
