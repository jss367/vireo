"""Unresolved species review groups, separate from mixed-species confirmations."""

from __future__ import annotations

import logging
import os
import sqlite3
from typing import TYPE_CHECKING

from flask import Blueprint, jsonify, request
from services import identity_alternatives as identities
from services import prediction_decisions
from web.request_args import MAX_SELECTION_PHOTOS

if TYPE_CHECKING:
    from db import Database
    from flask import Response
    from flask.typing import ResponseReturnValue
    from services.service_types import ErrorResponse, JsonDict, JsonError
    from web.web_types import GetDb

log = logging.getLogger(__name__)


def create_identity_alternatives_blueprint(get_db: GetDb, json_error: JsonError, db_path: str) -> Blueprint:
    blueprint = Blueprint('identity_alternatives', __name__)

    @blueprint.route('/api/identity-alternatives')
    def api_identity_alternatives() -> ResponseReturnValue:
        db = get_db()
        groups = identities.groups_for_review(db)
        return jsonify(groups=groups, photo_count=sum(len(group['photo_ids']) for group in groups),
                       workspace_id=db.require_workspace_id())

    def _write(resolve: bool) -> ErrorResponse | Response:
        db = get_db()
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return json_error('request body must be a JSON object')
        observed_workspace = body.get('observed_workspace_id')
        if type(observed_workspace) is not int:
            return json_error('observed_workspace_id is required from the reviewed workspace')
        if observed_workspace != db.require_workspace_id() or (
            observed_workspace != db.workspaces.most_recently_opened_id()
        ):
            return json_error('The active workspace changed; reload this page before editing identification', 409)
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
            with db.transaction():
                if resolve:
                    species = identities.uncertainty_text(body.get('species'), 'species', required=True)
                    entry_id = identities.resolve_alternatives(db, ids, species)
                else:
                    candidates = identities.normalize_candidates(body.get('candidates'), minimum=1 if observed else 2)
                    reason = identities.uncertainty_text(body.get('reason'), 'reason', required=True)
                    evidence = identities.uncertainty_text(body.get('missing_evidence', ''), 'missing_evidence')
                    entry_id = identities.save_alternatives(db, ids, candidates, reason, evidence)
        except ValueError as exc:
            return json_error(str(exc))
        db.prune_edit_history()
        response: JsonDict = {'ok': True, 'entry_id': entry_id, 'photo_ids': ids, 'flags_changed': False}
        _refresh_species(db, ids, response)
        return jsonify(response)

    def _refresh_species(db: Database, ids: list[int], response: JsonDict) -> None:
        # Persisted Process groups must not retain the candidates as confirmed.
        # As with undo's cache refresh, a saved catalog edit stands on failure.
        try:
            failed = identities.refresh_species_caches(db, os.path.dirname(db_path), ids)
        except (OSError, ValueError, TypeError, KeyError, sqlite3.Error):
            log.exception('Failed to refresh Process Review after an unresolved identity edit')
            failed = [db.require_workspace_id()]
        if failed:
            response.update(pipeline_cache_stale=True, warning='Species edit saved, but Process Review could not refresh. Recompute Process results before reviewing species.')

    def _locked_write(resolve: bool) -> ErrorResponse | Response:
        db = get_db()
        return prediction_decisions.under_prediction_decision_lock(db, lambda: _write(resolve), json_error=json_error)

    @blueprint.route('/api/identity-alternatives', methods=['POST'])
    def api_save_identity_alternatives() -> ResponseReturnValue:
        return _locked_write(False)

    @blueprint.route('/api/identity-alternatives/resolve', methods=['POST'])
    def api_resolve_identity_alternatives() -> ResponseReturnValue:
        return _locked_write(True)

    return blueprint
