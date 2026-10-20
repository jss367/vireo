"""Unresolved species review groups, separate from mixed-species confirmations."""

import logging
import os
import sqlite3

from flask import Blueprint, jsonify, request
from services import identity_alternatives as identities
from services import prediction_decisions
from web.request_args import MAX_SELECTION_PHOTOS

log = logging.getLogger(__name__)


def create_identity_alternatives_blueprint(get_db, json_error, db_path):
    blueprint = Blueprint('identity_alternatives', __name__)

    @blueprint.route('/api/identity-alternatives')
    def api_identity_alternatives():
        groups = identities.groups_for_review(get_db())
        return jsonify(groups=groups, photo_count=sum(len(group['photo_ids']) for group in groups))

    def _write(resolve):
        db = get_db()
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return json_error('request body must be a JSON object')
        ids = body.get('photo_ids')
        if not isinstance(ids, list) or not 1 <= len(ids) <= MAX_SELECTION_PHOTOS:
            return json_error(f'photo_ids must contain 1 to {MAX_SELECTION_PHOTOS} ids')
        if any(type(pid) is not int or pid <= 0 for pid in ids):
            return json_error('photo_ids must contain positive integers')
        ids = list(dict.fromkeys(ids))
        # Recheck visibility and observed group inside the shared writer lock.
        if set(db.photo_visibility.visible_photo_ids(ids)) != set(ids):
            return json_error('One or more photos are not in the active workspace', 404)
        states = db.identity_alternatives.for_photos(ids)
        observed = body.get('group_id')
        if resolve and not observed:
            return json_error('group_id is required to resolve the group you reviewed')
        if (observed and any(states.get(pid, {}).get('group_id') != observed for pid in ids)) or (
            not observed and states
        ):
            return json_error('Unresolved alternatives changed; reload the group before saving', 409)
        try:
            with db._commits_held():
                if resolve:
                    species = identities.uncertainty_text(body.get('species'), 'species', required=True)
                    entry_id = identities.resolve_alternatives(db, ids, species)
                else:
                    candidates = identities.normalize_candidates(body.get('candidates'))
                    reason = identities.uncertainty_text(body.get('reason'), 'reason', required=True)
                    evidence = identities.uncertainty_text(body.get('missing_evidence', ''), 'missing_evidence')
                    entry_id = identities.save_alternatives(db, ids, candidates, reason, evidence)
        except ValueError as exc:
            return json_error(str(exc))
        db.prune_edit_history()
        response = {'ok': True, 'entry_id': entry_id, 'photo_ids': ids, 'flags_changed': False}
        _refresh_species(db, ids, response)
        return jsonify(response)

    def _refresh_species(db, ids, response):
        # Persisted Process groups must not retain the candidates as confirmed.
        # As with undo's cache refresh, a saved catalog edit stands on failure.
        try:
            from pipeline import refresh_cache_species_for_photos
            species = db.photo_species.get_species_for_photos(ids)
            for pid in ids:
                species.setdefault(pid, [])
            refresh_cache_species_for_photos(os.path.dirname(db_path), db.active_workspace_id, species)
        except (OSError, ValueError, TypeError, KeyError, sqlite3.Error):
            log.exception('Failed to refresh Process Review after an unresolved identity edit')
            response.update(pipeline_cache_stale=True, warning='Species edit saved, but Process Review could not refresh. Recompute Process results before reviewing species.')

    def _locked_write(resolve):
        from pipeline_locks import acquire_workspace_regroup
        db = get_db()
        with acquire_workspace_regroup(db.require_workspace_id()):
            return prediction_decisions.under_prediction_decision_lock(db, lambda: _write(resolve), json_error=json_error)

    @blueprint.route('/api/identity-alternatives', methods=['POST'])
    def api_save_identity_alternatives():
        return _locked_write(False)

    @blueprint.route('/api/identity-alternatives/resolve', methods=['POST'])
    def api_resolve_identity_alternatives():
        return _locked_write(True)

    return blueprint
