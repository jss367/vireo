"""Species name lookups: the accepted-species list and name autocomplete.

``/api/species`` lists species with accepted predictions in the active
workspace; ``/api/species/search`` suggests names for hand-tagging from the
active label sets plus existing species keywords.
"""

import os

from db import text_search_match
from flask import Blueprint, jsonify, request
from web.request_args import request_bool_arg


def create_species_blueprint(get_db):
    """Build the species blueprint.

    Only the database accessor is injected; label sets and the text matcher
    are imported directly.
    """
    blueprint = Blueprint("species", __name__)

    @blueprint.route("/api/species")
    def api_species():
        db = get_db()
        species = db.get_accepted_species()
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
                pass
            if len(matches) >= 20:
                break

        # Also search existing species keywords in the database
        db = get_db()
        kw_rows = db.conn.execute(
            """SELECT name FROM keywords
               WHERE is_species = 1
                 AND vireo_keyword_text_match(name, ?, ?, ?)""",
            (q, 1 if match_case else 0, 1 if whole_word else 0),
        ).fetchall()
        for row in kw_rows:
            name_key = row["name"].casefold()
            if name_key not in seen:
                seen.add(name_key)
                matches.append(row["name"])

        return jsonify(matches[:20])

    return blueprint
