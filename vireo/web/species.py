"""Species name lookups: the accepted-species list and name autocomplete.

``/api/species`` lists species with accepted predictions in the active
workspace; ``/api/species/search`` suggests names for hand-tagging from the
active label sets plus existing species keywords.
"""

import json
import logging
import os
import sqlite3

from db import text_search_match
from flask import Blueprint, jsonify, request
from web.request_args import request_bool_arg

log = logging.getLogger(__name__)


def create_species_blueprint(get_db):
    """Build the species blueprint.

    Only the database accessor is injected; label sets and the text matcher
    are imported directly.
    """
    blueprint = Blueprint("species", __name__)

    @blueprint.post("/api/photos/<int:photo_id>/species/<int:keyword_id>/provenance")
    def api_species_provenance(photo_id, keyword_id):
        """Update one existing assignment's evidence as an undoable decision."""
        from services.species_provenance import validate_provenance
        db = get_db()
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            return jsonify({"error": "request body must be a JSON object"}), 400
        # Serialize with label removal/replacement and keep evidence validation
        # and the history snapshot in the same transaction.
        if not all(0 < key < 2**63 for key in (photo_id, keyword_id)):
            return jsonify({"error": "photo and keyword IDs must be positive signed 64-bit integers"}), 400
        try:
            db.begin_immediate()
        except sqlite3.OperationalError:
            return jsonify({"error": "Another review decision is in progress; try again"}), 503
        try:
            if not db.photos.get(photo_id, verify_workspace=True):
                return jsonify({"error": "photo not found"}), 404
            assignments = db.species_provenance.assignments(photo_id)
            assignment = next((a for a in assignments if a['keyword_id'] == keyword_id), None)
            if assignment is None:
                return jsonify({"error": "species assignment not found"}), 404
            try:
                provenance = validate_provenance(db, [photo_id], body, existing_assignment=assignment)
            except ValueError as exc:
                return jsonify({"error": str(exc)}), 400
            before = db.species_provenance.state(photo_id, keyword_id)
            if before['species_provenance'] and json.loads(before['species_provenance']) == provenance:
                return jsonify({"ok": True, "species_assignments": assignments})
            db.species_provenance.set(photo_id, keyword_id, provenance)
            after = db.species_provenance.state(photo_id, keyword_id)
            db.record_edit(
                'species_provenance', f"Updated species evidence for {assignment['species']}",
                str(keyword_id), [{'photo_id': photo_id,
                    'old_value': json.dumps({'keyword_id': keyword_id, 'state': before}),
                    'new_value': json.dumps({'keyword_id': keyword_id, 'state': after})}],
                source=provenance.get('reviewer'), reason=provenance.get('reason'), _commit=False,
            )
            db.commit()
            return jsonify({"ok": True, "species_assignments": db.species_provenance.assignments(photo_id)})
        finally:
            if db.in_transaction:
                db.rollback()

    @blueprint.route("/api/species")
    def api_species():
        db = get_db()
        species = db.photo_species.get_accepted_species()
        return jsonify({"species": species})

    @blueprint.route("/api/species/search")
    def api_species_search():
        """Search species names from active label sets for autocomplete."""
        q = request.args.get("q", "").strip()
        match_case = request_bool_arg("match_case")
        whole_word = request_bool_arg("whole_word")
        if len(q) < 2:
            return jsonify([])

        from labels import get_active_labels, normalized_label_set

        matches = []
        seen = set()
        for label_set in get_active_labels():
            labels_file = label_set.get("labels_file", "")
            if not labels_file or not os.path.exists(labels_file):
                continue
            try:
                # The normalized set, not the raw file: a prompt
                # classification refuses to attribute must not be offered
                # for hand-tagging either, and the qualified spellings are
                # what predictions will be named.
                for name in normalized_label_set(label_set):
                    name_key = name.casefold()
                    if (
                        text_search_match(name, q, match_case, whole_word)
                        and name_key not in seen
                    ):
                        seen.add(name_key)
                        matches.append(name)
                        if len(matches) >= 20:
                            break
            except Exception:
                # broad: one unreadable label set must not break autocomplete.
                log.warning("Could not read label set %s for species search", labels_file, exc_info=True)
            if len(matches) >= 20:
                break

        # Also search existing species keywords in the database
        db = get_db()
        for kw_name in db.keyword_taxa.species_names_matching(q, match_case, whole_word):
            name_key = kw_name.casefold()
            if name_key not in seen:
                seen.add(name_key)
                matches.append(kw_name)

        return jsonify(matches[:20])

    return blueprint
