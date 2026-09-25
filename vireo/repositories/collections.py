"""Persistence for smart collections and the rule-driven photo queries.

This module holds the collection rows (CRUD, the default collections and
their legacy-rule migrations) and the SQL built from collection rules: the
rules engine (``_build_query_from_rules``) that turns a rule tree into
``(folder_join, join_clause, where, params)``, the photo-list, count,
position and select-all queries built on it, the stacked Browse projection
(exact duplicates and bursts), and the filter-bar value suggestions.

The method bodies were moved verbatim from ``Database``. The only edits are
``self._ws_id()`` → ``self.workspace_id`` and ``NAME`` → ``self.NAME`` for the
``db`` module helpers and constants listed in ``__init__``; the SQL text,
parameter order and commit placement are unchanged. Browse and the
``/api/v1/collections`` API depend on that query text.

``Database`` keeps the active-workspace state and composition. The active
workspace is resolved lazily through ``Database._ws_id`` at exactly the
points the original code called it, so a malformed rule tree still raises
``ValueError`` before a missing workspace raises ``RuntimeError``. The façade
methods the rules engine consults (``get_effective_config``,
``get_subject_types``, ``get_folder_subtree_ids``) arrive as bound methods so
monkeypatches of ``Database`` keep reaching this code. Methods with no SQL of
their own (``rules_resolvable``, ``query_photo_position``,
``query_browse_stack_position``, ``count_browse_stacks``, the ``*_stacked``
id lists, ``create_default_collections_for_all_workspaces``) stay on
``Database`` and call back through it.
"""

import json
import math
import re

from keyword_identity import identity_sql


class CollectionRepository:
    def __init__(
        self,
        conn,
        resolve_workspace_id,
        *,
        get_effective_config,
        get_subject_types,
        get_folder_subtree_ids,
        chunks,
        escape_like,
        path_for_subtree_match,
        normalize_browse_stack_config,
        rule_upper_bound,
        life_list_ancestor_suppression_clause,
        burst_gap_tolerance_seconds,
        needs_identification_rules,
        gps_without_location_keyword_rules,
        no_location_information_rules,
        photo_date_asc_order,
        photo_sort_orders,
        prediction_confidence_sorts,
        top_prediction_confidence_expr,
        photo_cols,
        stack_keyword_set_ctes,
        stack_keyword_joins,
        stack_run_window,
        stack_cover_order,
        stack_sort_specs,
        suggest_value_exprs,
    ):
        self.conn = conn
        self._resolve_workspace_id = resolve_workspace_id
        # Bound ``Database`` methods, kept under their façade names.
        self.get_effective_config = get_effective_config
        self.get_subject_types = get_subject_types
        self.get_folder_subtree_ids = get_folder_subtree_ids
        # ``db`` module helpers and constants, kept under their module names
        # so the moved bodies read them as ``self.<name>``. ``_chunks`` is
        # ``db._chunks`` itself (its size default is bound at import), not a
        # per-repository chunker.
        self._chunks = chunks
        self._escape_like = escape_like
        self._path_for_subtree_match = path_for_subtree_match
        self.normalize_browse_stack_config = normalize_browse_stack_config
        self._rule_upper_bound = rule_upper_bound
        self._LIFE_LIST_ANCESTOR_SUPPRESSION_CLAUSE = life_list_ancestor_suppression_clause
        self.BURST_GAP_TOLERANCE_SECONDS = burst_gap_tolerance_seconds
        self.NEEDS_IDENTIFICATION_RULES = needs_identification_rules
        self.GPS_WITHOUT_LOCATION_KEYWORD_RULES = gps_without_location_keyword_rules
        self.NO_LOCATION_INFORMATION_RULES = no_location_information_rules
        self._PHOTO_DATE_ASC_ORDER = photo_date_asc_order
        self._PHOTO_SORT_ORDERS = photo_sort_orders
        self._PREDICTION_CONFIDENCE_SORTS = prediction_confidence_sorts
        self._TOP_PREDICTION_CONFIDENCE_EXPR = top_prediction_confidence_expr
        # ``Database`` class attributes, kept under their class names.
        self.PHOTO_COLS = photo_cols
        self._STACK_KEYWORD_SET_CTES = stack_keyword_set_ctes
        self._STACK_KEYWORD_JOINS = stack_keyword_joins
        self._STACK_RUN_WINDOW = stack_run_window
        self._STACK_COVER_ORDER = stack_cover_order
        self._STACK_SORT_SPECS = stack_sort_specs
        self._SUGGEST_VALUE_EXPRS = suggest_value_exprs

    @property
    def workspace_id(self):
        """The active workspace id, resolved at each read (raises if none)."""
        return self._resolve_workspace_id()

    def add(self, name, rules_json, visual_json=None):
        """Insert a smart collection. Returns the collection id.

        ``visual_json`` stores the universal filter's visual clause
        (``{prompt, strength}`` JSON) when the saved expression has one, so
        save → reopen reproduces the same result set instead of silently
        dropping to metadata-only.
        """
        cur = self.conn.execute(
            "INSERT INTO collections (name, rules, workspace_id, visual_json) "
            "VALUES (?, ?, ?, ?)",
            (name, rules_json, self.workspace_id, visual_json),
        )
        self.conn.commit()
        return cur.lastrowid

    def list_all(self):
        """Return all collections for the active workspace."""
        return self.conn.execute(
            "SELECT id, name, rules, visual_json FROM collections "
            "WHERE workspace_id = ? ORDER BY name",
            (self.workspace_id,),
        ).fetchall()

    def delete(self, collection_id):
        """Delete a collection."""
        self.conn.execute(
            "DELETE FROM collections WHERE id = ? AND workspace_id = ?",
            (collection_id, self.workspace_id),
        )
        self.conn.commit()

    def rename(self, collection_id, new_name):
        """Rename a collection within the active workspace.

        Raises ``ValueError`` if the collection isn't in the active workspace.
        """
        ws = self.workspace_id
        cur = self.conn.execute(
            "UPDATE collections SET name = ? WHERE id = ? AND workspace_id = ?",
            (new_name, collection_id, ws),
        )
        if cur.rowcount == 0:
            raise ValueError("collection not found")
        self.conn.commit()

    def duplicate(self, collection_id):
        """Copy a collection (name + rules + visual clause) within the active workspace.

        The new collection's name is ``"{original} (copy)"``; if that name is
        already taken, append an incrementing counter like ``"(copy 2)"``.
        Rules and ``visual_json`` are copied verbatim, which means static
        collections (photo_ids rules) keep their memberships and visual
        collections keep their visual clause.

        Returns the new collection id. Raises ``ValueError`` if the source
        collection isn't in the active workspace.
        """
        ws = self.workspace_id
        row = self.conn.execute(
            "SELECT name, rules, visual_json FROM collections WHERE id = ? AND workspace_id = ?",
            (collection_id, ws),
        ).fetchone()
        if not row:
            raise ValueError("collection not found")

        existing = {
            c["name"]
            for c in self.conn.execute(
                "SELECT name FROM collections WHERE workspace_id = ?", (ws,)
            ).fetchall()
        }
        base = f"{row['name']} (copy)"
        new_name = base
        n = 2
        while new_name in existing:
            new_name = f"{row['name']} (copy {n})"
            n += 1

        cur = self.conn.execute(
            "INSERT INTO collections (name, rules, workspace_id, visual_json) VALUES (?, ?, ?, ?)",
            (new_name, row["rules"], ws, row["visual_json"]),
        )
        self.conn.commit()
        return cur.lastrowid

    def _build_collection_query(self, collection_id, include_offline_folders=False):
        """Build SQL clauses from collection rules.

        Returns (folder_join, join_clause, where, params) or None if collection
        not found. Pass ``include_offline_folders=True`` for metadata-only
        callers (Dashboard scope) that keep offline photos in their totals.
        """
        row = self.conn.execute(
            "SELECT rules FROM collections WHERE id = ? AND workspace_id = ?",
            (collection_id, self.workspace_id),
        ).fetchone()
        if not row:
            return None

        rules = json.loads(row["rules"])
        return self._build_query_from_rules(
            rules, include_offline_folders=include_offline_folders,
        )

    def _build_query_from_rules(self, rules, include_offline_folders=False,
                                row_scoped=False):
        """Build SQL clauses from a smart-collection rule tree.

        Returns (folder_join, join_clause, where, params). Raises ValueError on
        malformed input — callers that accept rules from untrusted sources
        (e.g. the live-preview API) should catch and surface a 400.

        By default the folder join filters to accessible folders
        (``status IN ('ok', 'partial')``), matching what Browse and pipeline
        callers need. Pass ``include_offline_folders=True`` for metadata-only
        callers (e.g. Dashboard totals) that count photos even when their
        storage is currently missing.

        ``row_scoped=True`` selects broader candidate SQL for the negative
        prediction-field operators (``prediction_status is not/not_in``,
        ``classifier_model is not``): ``EXISTS(status != X)`` rather than
        ``NOT EXISTS(status = X)``. That widening is only safe when the
        caller re-evaluates the rule per row afterwards, which
        ``get_predictions`` does via ``_filter_prediction_rows_by_rules``.
        Photo-scoped callers (``/api/photos/query``, saved-collection
        evaluation, calendar/geo totals) MUST leave this False — the
        broad ``NOT EXISTS(status = X)`` form is what includes photos
        with no current predictions in "is not Rejected" results (see
        review r3619275290).

        Backward compatibility: the original collection format was a flat list
        of rule objects, implicitly combined with AND. Newer collections may use
        a grouped tree:

            {"mode": "all"|"any"|"none", "rules": [rule_or_group, ...]}
        """
        if isinstance(rules, list):
            root = {"mode": "all", "rules": rules}
        elif isinstance(rules, dict) and "rules" in rules:
            root = rules
        else:
            raise ValueError("rules must be a list or group object")

        def _is_scalar(value):
            return value is None or isinstance(value, str | int | float | bool)

        def _validate_node(node):
            if not isinstance(node, dict):
                raise ValueError("each rule must be an object")
            if "rules" in node and "field" not in node:
                mode = node.get("mode", "all")
                if mode not in ("all", "any", "none"):
                    raise ValueError("rule group mode must be all, any, or none")
                children = node.get("rules")
                if not isinstance(children, list):
                    raise ValueError("rule group rules must be a list")
                for child in children:
                    _validate_node(child)
                return
            if "field" not in node:
                raise ValueError("each rule must have a 'field'")
            field = node.get("field")
            op = node.get("op")
            value = node.get("value")
            if op == "recent":
                if not isinstance(value, dict):
                    raise ValueError("recent rules take a {n, unit} object value")
                n = value.get("n")
                if not isinstance(n, int) or isinstance(n, bool) or n < 1:
                    raise ValueError("recent n must be a positive integer")
                if value.get("unit", "days") not in ("days", "weeks", "months", "years"):
                    raise ValueError("recent unit must be days, weeks, months, or years")
                return
            list_allowed = (
                field == "photo_ids"
                or op in ("in", "not_in", "between")
            )
            if isinstance(value, list):
                if not list_allowed:
                    raise ValueError(f"rule field {field!r} does not accept a list value")
                if op == "between" and len(value) != 2:
                    raise ValueError("between rules take a [low, high] value")
                for item in value:
                    if not _is_scalar(item):
                        raise ValueError("rule list values must be scalars")
                return
            if op in ("in", "not_in", "between"):
                raise ValueError(f"rule op {op!r} requires a list value")
            if not _is_scalar(value):
                raise ValueError("rule value must be a scalar")

        def _truthy(value):
            return value is True or value == 1 or value == "1" or value == "true"

        def _falsey(value):
            return value is False or value == 0 or value == "0" or value == "false"

        # Lazily read the workspace-effective detector_confidence floor —
        # cfg.load() reads a JSON file, so only pay for it when a prediction
        # rule actually references it. Cached across every _prediction_exists
        # call in the same query so a rule tree with multiple prediction
        # predicates doesn't reread the config per predicate.
        _conf_cache = {}

        def _min_detector_conf():
            if "value" not in _conf_cache:
                import config as cfg
                _conf_cache["value"] = float(
                    self.get_effective_config(cfg.load()).get(
                        "detector_confidence", 0.2
                    )
                )
            return _conf_cache["value"]

        def _boolean_predicate(has_sql, op, value, params=None):
            """Boolean-typed leaf builder used by every ``BOOLEAN_OPS`` field.

            Guards ``op`` so a malformed rule such as
            ``{"field":"has_edits","op":"contains","value":1}`` surfaces as a
            ``ValueError`` (→ 400 at the API layer) instead of the silent
            match the numeric branch already rejects — otherwise a client
            can slip past the registry by inventing an op. Also guards
            ``value``: without this, ``{"field":"has_gps","op":"is",
            "value":"yes"}`` would fall to the negative branch (``_truthy``
            only accepts True/1/"1"/"true") and quietly return the ``is
            false`` predicate, flipping the client's intent.
            """
            if op not in ("equals", "is", "is not"):
                raise ValueError(f"unsupported boolean rule op: {op!r}")
            if _truthy(value):
                want_true = True
            elif _falsey(value):
                want_true = False
            else:
                raise ValueError(
                    f"boolean rule value must be true/false, got {value!r}"
                )
            if op == "is not":
                want_true = not want_true
            return (has_sql if want_true else f"NOT ({has_sql})"), list(params or [])

        def _numeric_condition(column, op, value, allow_null=False):
            if op == ">=":
                return f"{column} >= ?", [value]
            if op == "<=":
                return f"{column} <= ?", [value]
            if op == ">":
                return f"{column} > ?", [value]
            if op == "<":
                return f"{column} < ?", [value]
            if op == "between":
                return f"({column} >= ? AND {column} <= ?)", [value[0], value[1]]
            if op in ("equals", "is"):
                return f"{column} = ?", [value]
            if op == "is not":
                prefix = f"{column} IS NULL OR " if allow_null else ""
                return f"({prefix}{column} != ?)", [value]
            # Reject the op instead of silently emitting a constant-false
            # predicate — /api/photos/query catches ValueError and returns
            # 400, so a malformed rule like ``{"field":"file_size","op":
            # "contains","value":1}`` surfaces as a validation error instead
            # of a 200 with an empty result set.
            raise ValueError(f"unsupported numeric rule op: {op!r}")

        def _text_condition(column, op, value, case_sensitive=False):
            """Text-field predicate. Returns (cond, params) or None for an
            unrecognized op (caller falls through to the unsupported-rule
            error). Case-insensitive by default, matching SQLite LIKE; the
            case-sensitive variants avoid PRAGMA case_sensitive_like, which
            cannot be scoped to one query.
            """
            text = str(value if value is not None else "")
            if op in ("contains", "not_contains"):
                if case_sensitive:
                    cond, params = f"instr({column}, ?) > 0", [text]
                else:
                    cond = f"{column} LIKE ? ESCAPE '\\'"
                    params = [f"%{self._escape_like(text)}%"]
                if op == "not_contains":
                    return f"({column} IS NULL OR NOT ({cond}))", params
                return cond, params
            if op in ("starts_with", "ends_with"):
                if not text:
                    return "1", []
                if case_sensitive:
                    if op == "starts_with":
                        return f"substr({column}, 1, {len(text)}) = ?", [text]
                    return f"substr({column}, -{len(text)}) = ?", [text]
                pattern = (
                    self._escape_like(text) + "%" if op == "starts_with"
                    else "%" + self._escape_like(text)
                )
                return f"{column} LIKE ? ESCAPE '\\'", [pattern]
            if op in ("equals", "is"):
                if case_sensitive:
                    return f"{column} = ?", [text]
                return f"LOWER({column}) = LOWER(?)", [text]
            if op == "is not":
                if case_sensitive:
                    return f"({column} IS NULL OR {column} != ?)", [text]
                return f"({column} IS NULL OR LOWER({column}) != LOWER(?))", [text]
            return None

        def _species_keyword_from(sfx=""):
            """FROM/WHERE fragment selecting a photo's species-rank keywords.

            Shared by ``has_species`` and ``species_count`` so the two can
            never disagree about what counts as a species: a keyword
            qualifies when either the legacy ``is_species`` flag is set OR
            it is a taxonomy row, AND its linked taxon (if any) has rank
            ``species``. This mirrors
            ``get_species_keywords_for_photos`` — the resolver behind the
            species shown in Browse, the life list, and species
            representatives — so the filters agree with what the UI shows.

            ``sfx`` suffixes the table aliases so callers that need more
            than one species subquery in the same statement don't collide.
            """
            return (
                f"FROM photo_keywords pk{sfx} "
                f"JOIN keywords k{sfx} ON k{sfx}.id = pk{sfx}.keyword_id "
                f"LEFT JOIN taxa t{sfx} ON t{sfx}.id = k{sfx}.taxon_id "
                f"WHERE pk{sfx}.photo_id = p.id "
                f"AND (k{sfx}.is_species = 1 OR k{sfx}.type = 'taxonomy') "
                f"AND (t{sfx}.rank = 'species' OR t{sfx}.rank IS NULL)"
            )

        def _keyword_exists(predicate, predicate_params):
            return (
                "EXISTS (SELECT 1 FROM photo_keywords pk "
                "JOIN keywords k ON k.id = pk.keyword_id "
                f"WHERE pk.photo_id = p.id AND {predicate})",
                list(predicate_params),
            )

        def _keyword_not_exists(predicate, predicate_params):
            return (
                "NOT EXISTS (SELECT 1 FROM photo_keywords pk "
                "JOIN keywords k ON k.id = pk.keyword_id "
                f"WHERE pk.photo_id = p.id AND {predicate})",
                list(predicate_params),
            )

        def _prediction_exists(predicate, predicate_params, review_join=False):
            # Pin to the most recent labels_fingerprint per
            # (detection_id, classifier_model), matching how the dashboard
            # (get_top_prediction_for_photo, prediction_status queries)
            # decides which prediction row is "current". Without this pin,
            # a rerun classifier with a new fingerprint leaves an older
            # accepted row around and universal-filter rules
            # (prediction_status is accepted, prediction_confidence >= X,
            # classifier_model is Y, taxonomy_* is Z) still match the
            # stale prediction — disagreeing with the review UI that only
            # shows the current fingerprint.
            fingerprint_pin = (
                " AND pred.labels_fingerprint = ("
                "SELECT pr2.labels_fingerprint FROM predictions pr2 "
                "WHERE pr2.detection_id = pred.detection_id "
                "AND pr2.classifier_model = pred.classifier_model "
                "ORDER BY pr2.created_at DESC, pr2.id DESC LIMIT 1)"
            )
            # Always join prediction_review so alternatives can be filtered
            # out below — the review_join parameter is retained for callers
            # that also read prv.status in their predicate.
            review = (
                " LEFT JOIN prediction_review prv "
                "ON prv.prediction_id = pred.id AND prv.workspace_id = ?"
            )
            # Filters that represent the *displayed* prediction (confidence,
            # classifier_model, taxonomy_*, plus the status predicates that
            # already read prv.status) must ignore runner-up rows stored
            # with prv.status = 'alternative'. Otherwise a top prediction
            # at 0.95 with an alternative at 0.10 would satisfy
            # prediction_confidence <= 0.2 even though /api/predictions
            # (app.py:12386-12388) drops alternatives from top-level results.
            not_alternative = (
                " AND COALESCE(prv.status, 'pending') != 'alternative'"
            )
            # Gate by the workspace-effective detector_confidence floor.
            # /api/photos/query's response goes through
            # get_detections_for_photos() (which applies this threshold),
            # and the dashboard prediction counters + query_move_rule_matches
            # apply it too. Without gating here, a below-threshold hidden
            # detection whose accepted/high-confidence prediction is stale
            # from a previous run would still satisfy prediction_status,
            # prediction_confidence, classifier_model, and taxonomy_* rules,
            # so the universal filter would include a photo the Browse view
            # shows with no visible detection context.
            conf_filter = " AND det.detector_confidence >= ?"
            params = (
                [self.workspace_id]
                + [_min_detector_conf()]
                + list(predicate_params)
            )
            return (
                "EXISTS (SELECT 1 FROM detections det "
                "JOIN predictions pred ON pred.detection_id = det.id"
                f"{review} WHERE det.photo_id = p.id{conf_filter}"
                f"{not_alternative} "
                f"AND {predicate}{fingerprint_pin})",
                params,
            )

        def _build_leaf(rule):
            field = rule["field"]
            op = rule.get("op", "")
            value = rule.get("value")

            if field == "metadata":
                from metadata_search import (
                    PREDICTION_COLUMNS,
                    photo_metadata_predicates,
                    values_contain,
                )

                if op not in ("contains", "not_contains") or not isinstance(value, str) or not value.strip():
                    raise ValueError("metadata search requires contains/not_contains and a nonempty string")
                if len(value) > 4096:
                    raise ValueError("metadata search is limited to 4,096 characters")
                like = f"%{self._escape_like(value)}%"
                parts = photo_metadata_predicates()
                params = [like] * len(parts)
                parts.append(
                    "EXISTS (SELECT 1 FROM photo_color_labels search_color "
                    "WHERE search_color.photo_id = p.id AND search_color.workspace_id = ? "
                    "AND search_color.color LIKE ? ESCAPE '\\')"
                )
                params.extend([self.workspace_id, like])
                # Preserve the displayed canonical species-name lookup from
                # quick search, including hierarchy leaves linked to a root.
                species_sql, species_params = _build_leaf(
                    {"field": "species", "op": "contains", "value": value}
                )
                parts.append(species_sql)
                params.extend(species_params)
                prediction_sql, prediction_params = _prediction_exists(
                    values_contain([f"pred.{col}" for col in PREDICTION_COLUMNS]
                                   + ["COALESCE(prv.status, 'pending')"]), [like],
                )
                parts.append(prediction_sql)
                params.extend(prediction_params)
                condition = "(" + " OR ".join(parts) + ")"
                return (f"NOT {condition}" if op == "not_contains" else condition), params

            if field == "keyword_identity":
                if op != 'equals' or not isinstance(value, str) or not value:
                    raise ValueError('keyword_identity requires an equals rule with a nonempty identity')
                return (
                    'EXISTS (SELECT 1 FROM photo_keywords pki '
                    'JOIN keywords ki ON ki.id = pki.keyword_id '
                    f'WHERE pki.photo_id = p.id AND ({identity_sql("ki")}) = ?)',
                    [value],
                )

            if field == "all":
                # Sentinel for defaults like "All Photos" — adds no condition,
                # so the workspace-folder join alone determines matches.
                return None, []
            if field == "photo_ids":
                ids = value if isinstance(value, list) else []
                if not ids:
                    return "0", []
                # Inline integer ids as SQL literals instead of binding one
                # parameter per id — a static collection created from a large
                # selection would otherwise exceed SQLite's bound-parameter
                # cap on every query against the collection, permanently. A
                # temp table (as _scope_clause uses) isn't composable here:
                # the returned clause may be embedded in queries that run
                # later and repeatedly. Only ints are inlined (injection-safe);
                # any non-int leftovers (malformed rules, rare) keep the
                # parameter-binding path so comparison semantics are unchanged.
                int_ids = [
                    v for v in ids
                    if isinstance(v, int) and not isinstance(v, bool)
                ]
                other = [
                    v for v in ids
                    if not (isinstance(v, int) and not isinstance(v, bool))
                ]
                parts = []
                params = []
                if int_ids:
                    parts.append(
                        "p.id IN (%s)" % ",".join(str(v) for v in int_ids)
                    )
                if other:
                    placeholders = ",".join("?" for _ in other)
                    parts.append(f"p.id IN ({placeholders})")
                    params = list(other)
                if len(parts) == 1:
                    return parts[0], params
                return "(" + " OR ".join(parts) + ")", params
            if field == "life_list_uncounted":
                if op not in ("equals", "is") or not isinstance(value, str):
                    raise ValueError("invalid Life List identification filter")
                try:
                    token = json.loads(value)
                except (TypeError, ValueError) as exc:
                    raise ValueError(
                        "invalid Life List identification filter"
                    ) from exc
                if not isinstance(token, dict) or set(token) != {"name", "taxon_id"}:
                    raise ValueError("invalid Life List identification filter")
                name = token["name"]
                taxon_id = token["taxon_id"]
                if not isinstance(name, str) or not name:
                    raise ValueError("invalid Life List identification filter")
                if taxon_id is not None and (
                    not isinstance(taxon_id, int) or isinstance(taxon_id, bool)
                ):
                    raise ValueError("invalid Life List identification filter")

                predicate = (
                    "(k.is_species = 1 OR k.type = 'taxonomy') "
                    "AND k.name = ? "
                )
                predicate_params = [name]
                if taxon_id is None:
                    predicate += "AND k.taxon_id IS NULL "
                else:
                    predicate += (
                        "AND k.taxon_id = ? "
                        "AND NOT EXISTS (SELECT 1 FROM taxa t_unc "
                        "WHERE t_unc.id = k.taxon_id "
                        "AND t_unc.rank = 'species') "
                    )
                    predicate_params.append(taxon_id)
                predicate += self._LIFE_LIST_ANCESTOR_SUPPRESSION_CLAUSE
                condition, predicate_params = _keyword_exists(
                    predicate, predicate_params
                )
                # The Life List excludes rejected photos from both its count
                # and disclosure. Preserve that exact scope in Browse.
                return (
                    f"(COALESCE(p.flag, 'none') != 'rejected' AND {condition})",
                    predicate_params,
                )
            if field in ("rating", "quality_score", "sharpness",
                         "subject_sharpness", "noise_estimate",
                         "crop_complete"):
                column = "p.subject_tenengrad" if field == "subject_sharpness" else f"p.{field}"
                return _numeric_condition(column, op, value, allow_null=True)
            if field == "keyword":
                if op == "contains":
                    # Escape LIKE metacharacters so ``%``/``_`` in the value
                    # stay literal — otherwise ``keyword contains "%"`` would
                    # match every keyworded photo, breaking parity with the
                    # escaped filename/camera/species text rules and the
                    # escaped folder/keyword typeahead paths.
                    like = f"%{self._escape_like(str(value or ''))}%"
                    return _keyword_exists("k.name LIKE ? ESCAPE '\\'", [like])
                if op == "not_contains":
                    like = f"%{self._escape_like(str(value or ''))}%"
                    return _keyword_not_exists("k.name LIKE ? ESCAPE '\\'", [like])
                if op in ("equals", "is"):
                    return _keyword_exists("k.name = ?", [value])
                if op == "is not":
                    return _keyword_not_exists("k.name = ?", [value])
            if field == "folder":
                # Match the folder itself plus separator-delimited descendants.
                # A bare prefix LIKE would also match siblings ("/photos/2023"
                # matching "/photos/2023-trip") and treat _/% in the value as
                # wildcards. Stored folder paths use the platform separator
                # (``str(Path(...))`` in scanner.scan — backslashes on Windows),
                # so normalize both sides to forward slashes the same way
                # ``_folder_subtree_ids_by_path`` does; otherwise a Windows
                # library's ``C:\Photos\Birds`` row never matches a rule whose
                # LIKE pattern hard-codes ``C:/Photos/%``.
                base = self._path_for_subtree_match(str(value or ""))
                subtree_params = [base, self._escape_like(base) + "/%"]
                norm = "REPLACE(f.path, '\\', '/')"
                # Legacy folder collections were saved with op "is"/"is not"
                # before the rule vocabulary switched to "under"/"not_under".
                # Treat the old ops as aliases so those rows keep resolving
                # (a single unrecognized rule used to raise ValueError and 500
                # the whole /api/collections list). "is" always meant the
                # folder and its descendants, which is exactly "under".
                if op in ("under", "is", "equals"):
                    return f"({norm} = ? OR {norm} LIKE ? ESCAPE '\\')", subtree_params
                if op in ("not_under", "is not"):
                    return (
                        f"(f.path IS NULL OR ({norm} != ? AND {norm} NOT LIKE ? ESCAPE '\\'))",
                        subtree_params,
                    )
            if field == "flag":
                # NULL means unflagged for legacy rows (older ingests, undo
                # paths). Browse's inline filter already COALESCEs; the rule
                # engine must agree or the Unflagged chip silently drops
                # those rows once Browse moves onto this path.
                col = "COALESCE(p.flag, 'none')"
                if op in ("equals", "is"):
                    return f"{col} = ?", [value]
                if op == "is not":
                    return f"{col} != ?", [value]
                if op in ("in", "not_in"):
                    values = list(value or [])
                    if not values:
                        # in [] matches nothing; not_in [] excludes nothing.
                        # Emit constants (not None) so any/none group
                        # semantics stay exact.
                        return ("0" if op == "in" else "1"), []
                    placeholders = ",".join("?" * len(values))
                    negate = "NOT " if op == "not_in" else ""
                    return f"{col} {negate}IN ({placeholders})", values
            if field == "color_label":
                def _color_exists(colors):
                    placeholders = ",".join("?" * len(colors))
                    return (
                        "EXISTS (SELECT 1 FROM photo_color_labels pcl "
                        "WHERE pcl.photo_id = p.id AND pcl.workspace_id = ? "
                        f"AND pcl.color IN ({placeholders}))",
                        [self.workspace_id, *colors],
                    )
                if op in ("equals", "is"):
                    return _color_exists([value])
                if op == "is not":
                    cond, params = _color_exists([value])
                    return f"NOT {cond}", params
                if op in ("in", "not_in"):
                    values = list(value or [])
                    if not values:
                        return ("0" if op == "in" else "1"), []
                    cond, params = _color_exists(values)
                    if op == "not_in":
                        # "is not one of" = carries no label from the set;
                        # unlabeled photos match, mirroring "is not".
                        return f"NOT {cond}", params
                    return cond, params
            if field == "has_species":
                # Falling back to ``k.is_species = 1`` would exclude photos
                # whose species is a ``type='taxonomy', is_species=0`` row —
                # exactly the shape upgraded libraries store — so the "Has
                # species" chip would disagree with everywhere the species
                # is actually shown. ``_species_keyword_from`` holds that
                # eligibility rule.
                has_species_exists = f"EXISTS (SELECT 1 {_species_keyword_from()})"
                if op in ("equals", "is") and _falsey(value):
                    return f"NOT {has_species_exists}", []
                if op in ("equals", "is") and _truthy(value):
                    return has_species_exists, []
            if field == "species_count":
                # Count distinct species the way the app presents them:
                # ``get_species_keywords_for_photos`` keys each species by
                # its ``taxon_id`` when linked and falls back to the exact
                # stored name otherwise, so a photo carrying both the
                # ``Verdin`` root and the ``Birds|Verdin`` hierarchy leaf
                # shows one species and must count as one — counting
                # keyword rows would make it multi-species. The literal
                # prefixes keep a taxon id from colliding with a keyword
                # named like a number, and concatenation drops any column
                # collation so the name branch compares exactly, matching
                # the resolver's dict-key semantics.
                expr = (
                    "(SELECT COUNT(DISTINCT CASE WHEN ksc.taxon_id IS NOT NULL "
                    "THEN 'taxon:' || ksc.taxon_id ELSE 'name:' || ksc.name END) "
                    + _species_keyword_from("sc") + ")"
                )
                return _numeric_condition(expr, op, value)
            if field == "has_subject":
                subject_types = sorted(self.get_subject_types())
                if not subject_types:
                    # No subject types configured → no photo can have a
                    # subject. Route through ``_boolean_predicate`` so a
                    # malformed rule like
                    # ``{"field":"has_subject","op":"contains","value":1}``
                    # raises ValueError (→ 400) in this configuration too,
                    # instead of silently dropping the rule via
                    # ``return None, []``.
                    return _boolean_predicate("0", op, value)
                placeholders = ",".join("?" * len(subject_types))
                type_clause = f"k.type IN ({placeholders})"
                if "taxonomy" in subject_types:
                    type_clause = f"({type_clause} OR k.is_species = 1)"
                exists, params = _keyword_exists(type_clause, subject_types)
                return _boolean_predicate(exists, op, value, params)
            if field == "wildlife_excluded":
                excluded = "p.wildlife_excluded = 1"
                if op in ("equals", "is"):
                    return (excluded if _truthy(value) else f"NOT ({excluded})"), []
                if op == "is not":
                    return (f"NOT ({excluded})" if _truthy(value) else excluded), []
            if field == "keyword_count":
                expr = "(SELECT COUNT(*) FROM photo_keywords pk2 WHERE pk2.photo_id = p.id)"
                return _numeric_condition(expr, op, value)
            if field == "timestamp":
                if op == "between" and isinstance(value, list) and len(value) == 2:
                    return "p.timestamp >= ? AND p.timestamp <= ?", [
                        value[0],
                        self._rule_upper_bound(value[1]),
                    ]
                if op == "recent_days":
                    # ``strftime`` with an explicit ``T`` separator so the
                    # cutoff format matches the scanner's ``dt.isoformat()``
                    # storage. SQLite's plain ``datetime('now', ?)`` returns
                    # ``YYYY-MM-DD HH:MM:SS`` (space separator); comparing a
                    # T-separated timestamp against a space-separated cutoff
                    # is a lexical mismatch where ``T`` (0x54) sorts after
                    # ``' '`` (0x20), so any photo on the cutoff day would
                    # spuriously satisfy ``>=`` even when its clock time is
                    # earlier than the cutoff's clock time.
                    return (
                        "p.timestamp >= strftime('%Y-%m-%dT%H:%M:%S', 'now', ?)",
                        [f"-{value} days"],
                    )
                if op == "recent":
                    n = value["n"]
                    unit = value.get("unit", "days")
                    modifier = {
                        "days": f"-{n} days",
                        "weeks": f"-{n * 7} days",
                        "months": f"-{n} months",
                        "years": f"-{n} years",
                    }[unit]
                    return (
                        "p.timestamp >= strftime('%Y-%m-%dT%H:%M:%S', 'now', ?)",
                        [modifier],
                    )
                # Comparison ops for "on or after / before" style rules.
                # Timestamps are ISO strings, so lexical compare is correct;
                # date-only bounds are inclusive of the named day on the
                # upper side, matching the Browse date_to behavior.
                if op == ">=":
                    return "p.timestamp >= ?", [value]
                if op == ">":
                    # Only advance a bare ``YYYY-MM-DD`` to end-of-day —
                    # ``> 2024-01-01`` means "strictly after that day".
                    # Padding an already-precise timestamp
                    # (``2024-01-01T12:00:00``) would spuriously exclude
                    # sub-second photos in the same clock second
                    # (``12:00:00.5``) that ARE strictly greater than the
                    # requested instant.
                    return "p.timestamp > ?", [self._rule_upper_bound(value)]
                if op == "<=":
                    # Symmetric to ``>`` above: only pad bare dates. A
                    # precise ``<= 2024-01-01T12:00:00`` request means the
                    # exact instant, not the whole clock second — padding
                    # would spuriously *include* ``12:00:00.5`` photos
                    # that are after the requested instant.
                    return "p.timestamp <= ?", [self._rule_upper_bound(value)]
                if op == "<":
                    return "p.timestamp < ?", [value]
            if field == "extension":
                if op in ("equals", "is"):
                    return "LOWER(p.extension) = LOWER(?)", [value]
                if op == "is not":
                    return "LOWER(p.extension) != LOWER(?)", [value]
                if op in ("in", "not_in"):
                    values = list(value or [])
                    if not values:
                        return ("0" if op == "in" else "1"), []
                    placeholders = ",".join("LOWER(?)" for _ in values)
                    negate = "NOT " if op == "not_in" else ""
                    return f"LOWER(p.extension) {negate}IN ({placeholders})", values
            if field in (
                "taxonomy_kingdom",
                "taxonomy_phylum",
                "taxonomy_class",
                "taxonomy_order",
                "taxonomy_family",
                "taxonomy_genus",
            ):
                col = f"pred.{field}"
                if op in ("equals", "is"):
                    return _prediction_exists(f"{col} = ?", [value])
                if op == "is not":
                    exists, params = _prediction_exists(f"{col} = ?", [value])
                    return "NOT " + exists, params
                if op == "contains":
                    # ``value or ''`` would turn a falsey scalar like ``0`` or
                    # ``False`` into an empty string, producing ``LIKE '%%'``
                    # and matching every non-NULL taxonomy value — the exact
                    # ``value="%"`` unbounded match this escape is meant to
                    # block. Preserve the scalar's string form instead.
                    like = f"%{self._escape_like(str(value if value is not None else ''))}%"
                    return _prediction_exists(f"{col} LIKE ? ESCAPE '\\'", [like])
            if field == "prediction_confidence":
                cond, cond_params = _numeric_condition("pred.confidence", op, value)
                return _prediction_exists(cond, cond_params)
            if field == "classifier_model":
                if op in ("equals", "is"):
                    return _prediction_exists("pred.classifier_model = ?", [value])
                if op == "is not":
                    # Splits by caller, mirroring prediction_status above:
                    #   row_scoped=True (get_predictions) → positive EXISTS
                    #     ("photo has at least one prediction whose model is
                    #     NOT X"). NOT EXISTS would drop the whole photo the
                    #     moment any sibling used model X, hiding the
                    #     other-model rows the visible filter should keep
                    #     (see r3618514362). Row-level clean-up in
                    #     ``_filter_prediction_rows_by_rules`` removes the
                    #     matching sibling rows.
                    #   row_scoped=False (default; photo queries, saved
                    #     collections) → broad NOT EXISTS so photos with no
                    #     current predictions still satisfy ``is not X``
                    #     (see r3619275290).
                    if row_scoped:
                        return _prediction_exists(
                            "pred.classifier_model != ?", [value],
                        )
                    exists, params = _prediction_exists(
                        "pred.classifier_model = ?", [value],
                    )
                    return "NOT " + exists, params
                if op == "contains":
                    # Escape LIKE metacharacters so a value like ``%`` or ``_``
                    # stays literal — matches the other advertised text
                    # contains predicates (filename, camera fields, keyword,
                    # species) and blocks ``value="%"`` from matching every
                    # classified photo.
                    like = f"%{self._escape_like(str(value or ''))}%"
                    return _prediction_exists(
                        "pred.classifier_model LIKE ? ESCAPE '\\'", [like]
                    )
            if field == "prediction_status":
                # Negative predicates split by caller:
                #   row_scoped=True (get_predictions) → positive EXISTS
                #     ("photo has at least one prediction whose status is
                #     NOT X"). NOT EXISTS would drop the whole photo the
                #     moment any sibling matched X, hiding pending rows the
                #     visible filter should keep (see r3618393423). The
                #     row-level pass in ``_filter_prediction_rows_by_rules``
                #     then removes the matching sibling rows.
                #   row_scoped=False (default; photo queries, saved
                #     collections) → broad NOT EXISTS ("photo has no
                #     prediction whose status is X"). This preserves the
                #     historical behavior where a photo with zero current
                #     predictions still satisfies ``is not Rejected`` and
                #     is included by /api/photos/query and collections
                #     built on the same rule (see r3619275290).
                if op in ("equals", "is"):
                    return _prediction_exists(
                        "COALESCE(prv.status, 'pending') = ?",
                        [value],
                        review_join=True,
                    )
                if op == "is not":
                    if row_scoped:
                        return _prediction_exists(
                            "COALESCE(prv.status, 'pending') != ?",
                            [value],
                            review_join=True,
                        )
                    exists, params = _prediction_exists(
                        "COALESCE(prv.status, 'pending') = ?",
                        [value],
                        review_join=True,
                    )
                    return "NOT " + exists, params
                if op in ("in", "not_in"):
                    values = list(value or [])
                    if not values:
                        return ("0" if op == "in" else "1"), []
                    placeholders = ",".join("?" * len(values))
                    if op == "in":
                        return _prediction_exists(
                            f"COALESCE(prv.status, 'pending') IN ({placeholders})",
                            values,
                            review_join=True,
                        )
                    # not_in: row_scoped keeps a positive EXISTS on the
                    # inverse set; photo-scoped stays broad via NOT EXISTS.
                    if row_scoped:
                        return _prediction_exists(
                            f"COALESCE(prv.status, 'pending') NOT IN ({placeholders})",
                            values,
                            review_join=True,
                        )
                    exists, params = _prediction_exists(
                        f"COALESCE(prv.status, 'pending') IN ({placeholders})",
                        values,
                        review_join=True,
                    )
                    return "NOT " + exists, params
            if field == "needs_review":
                # Splits by caller, mirroring prediction_status "is not":
                #   row_scoped=True (get_predictions) → for the False case,
                #     positive EXISTS on a non-pending row so a photo with
                #     mixed pending + accepted/rejected siblings still
                #     surfaces the non-pending row that satisfies "Needs
                #     review is No". NOT EXISTS(pending) would drop the
                #     whole photo the moment any sibling is pending, hiding
                #     rows the visible filter should keep (r3619118948).
                #     The row-level pass in
                #     ``_filter_prediction_rows_by_rules`` then removes the
                #     pending sibling rows.
                #   row_scoped=False (default; photo queries, saved
                #     collections) → broad NOT EXISTS so photos with no
                #     pending prediction (including none at all) satisfy
                #     "Needs review is No".
                if _truthy(value):
                    return _prediction_exists(
                        "COALESCE(prv.status, 'pending') = 'pending'",
                        [],
                        review_join=True,
                    )
                if row_scoped:
                    return _prediction_exists(
                        "COALESCE(prv.status, 'pending') != 'pending'",
                        [],
                        review_join=True,
                    )
                exists, params = _prediction_exists(
                    "COALESCE(prv.status, 'pending') = 'pending'",
                    [],
                    review_join=True,
                )
                return "NOT " + exists, params
            if field == "has_mask":
                has = "p.mask_path IS NOT NULL"
                return (has if _truthy(value) else f"NOT ({has})"), []
            if field == "has_jpeg_companion":
                # SQLite LIKE is case-insensitive for ASCII by default, so
                # LOWER() would be redundant here.
                has = (
                    "p.companion_path IS NOT NULL AND "
                    "(p.companion_path LIKE '%.jpg' OR "
                    "p.companion_path LIKE '%.jpeg')"
                )
                return (f"({has})" if _truthy(value) else f"NOT ({has})"), []
            if field == "active_mask_variant":
                if op in ("equals", "is"):
                    return "p.active_mask_variant = ?", [value]
                if op == "is not":
                    return "(p.active_mask_variant IS NULL OR p.active_mask_variant != ?)", [value]
                if op == "contains":
                    # ``value or ''`` would turn a falsey scalar like ``0`` or
                    # ``False`` into an empty string, producing ``LIKE '%%'``
                    # and matching every photo with a variant set — the exact
                    # ``value="%"`` unbounded match this escape is meant to
                    # block. Preserve the scalar's string form instead.
                    like = f"%{self._escape_like(str(value if value is not None else ''))}%"
                    return "p.active_mask_variant LIKE ? ESCAPE '\\'", [like]
            if field == "has_gps":
                has = "p.latitude IS NOT NULL AND p.longitude IS NOT NULL"
                return _boolean_predicate(has, op, value)
            if field == "has_location_keyword":
                has = (
                    "EXISTS (SELECT 1 FROM photo_keywords pk "
                    "JOIN keywords k ON k.id = pk.keyword_id "
                    "WHERE pk.photo_id = p.id AND k.type = 'location')"
                )
                return _boolean_predicate(has, op, value)
            if field == "has_coord_location_keyword":
                # A structured location supplies coordinates the map can
                # place. Matches the ``assigned`` branch of
                # ``_append_location_status_filter`` — the deep-link path
                # depends on this being distinct from ``has_location_keyword``
                # (which also matches free-text locations without lat/lng).
                has = (
                    "EXISTS (SELECT 1 FROM photo_keywords pk "
                    "JOIN keywords k ON k.id = pk.keyword_id "
                    "WHERE pk.photo_id = p.id AND k.type = 'location' "
                    "AND k.latitude IS NOT NULL AND k.longitude IS NOT NULL)"
                )
                return _boolean_predicate(has, op, value)
            if field == "location_keyword_missing":
                gps = "p.latitude IS NOT NULL AND p.longitude IS NOT NULL"
                no_loc = (
                    "NOT EXISTS (SELECT 1 FROM photo_keywords pk "
                    "JOIN keywords k ON k.id = pk.keyword_id "
                    "WHERE pk.photo_id = p.id AND k.type = 'location')"
                )
                cond = f"({gps}) AND ({no_loc})"
                return (cond if _truthy(value) else f"NOT ({cond})"), []
            if field == "inat_submitted":
                has = "EXISTS (SELECT 1 FROM inat_submissions ins WHERE ins.photo_id = p.id)"
                return (has if _truthy(value) else f"NOT {has}"), []
            if field == "is_duplicate":
                # Catalog-wide by file_hash to match find_duplicate_groups()
                # and apply_duplicate_resolution — a photo whose only duplicate
                # lives in another workspace is still a duplicate here (the
                # Duplicates workflow will act on it), so Browse must not hide
                # that membership behind a workspace_folders join.
                has = (
                    "p.file_hash IS NOT NULL AND EXISTS ("
                    "SELECT 1 FROM photos p2 "
                    "WHERE p2.id != p.id AND p2.file_hash = p.file_hash "
                    "AND (p2.flag IS NULL OR p2.flag != 'rejected'))"
                )
                return _boolean_predicate(has, op, value)
            if field in ("file_size", "width", "height", "focal_length",
                         "aperture", "shutter_speed", "iso"):
                return _numeric_condition(f"p.{field}", op, value, allow_null=True)
            if field == "gps_lat":
                return _numeric_condition("p.latitude", op, value, allow_null=True)
            if field == "gps_lng":
                return _numeric_condition("p.longitude", op, value, allow_null=True)
            if field in ("filename", "camera_make", "camera_model", "lens"):
                result = _text_condition(
                    f"p.{field}", op, value,
                    case_sensitive=bool(rule.get("case")),
                )
                if result is not None:
                    return result
            if field == "burst_id":
                if op in ("equals", "is"):
                    return "p.burst_id = ?", [value]
                if op == "is not":
                    return "(p.burst_id IS NULL OR p.burst_id != ?)", [value]
            if field == "in_burst":
                has = "p.burst_id IS NOT NULL"
                return _boolean_predicate(has, op, value)
            if field == "duplicate_group":
                # Duplicate groups have no id table; membership is identity
                # on file_hash (see find_duplicate_groups).
                if op in ("equals", "is"):
                    return "p.file_hash = ?", [value]
                if op == "is not":
                    return "(p.file_hash IS NULL OR p.file_hash != ?)", [value]
            if field == "has_edits":
                has = ("EXISTS (SELECT 1 FROM photo_edit_recipes per "
                       "WHERE per.photo_id = p.id)")
                return _boolean_predicate(has, op, value)
            if field == "has_visual_index":
                # Optional rule key "model" narrows to one embedding model.
                # The universal-filter API layer injects the active visual
                # model onto UI-emitted rules missing this key so the
                # filter agrees with visual search (which only loads
                # embeddings for the active model). Saved smart
                # collections without a ``model`` keep matching any row —
                # a portable "some embedding exists" check.
                model = rule.get("model")
                if model:
                    has = ("EXISTS (SELECT 1 FROM photo_embeddings pe "
                           "WHERE pe.photo_id = p.id AND pe.model = ?)")
                    params = [model]
                else:
                    has = ("EXISTS (SELECT 1 FROM photo_embeddings pe "
                           "WHERE pe.photo_id = p.id)")
                    params = []
                return _boolean_predicate(has, op, value, params)
            if field == "species":
                # Confirmed species ride photo_keywords→keywords(→taxa);
                # a photo with several species matches when ANY matches
                # (multi-species model). Match by taxon identity for
                # linked rows so any keyword whose canonical name matches
                # the value pulls in every photo tagged with that same
                # taxon — a photo tagged only with a hierarchy leaf
                # (``Desert Verdin``) still matches the canonical species
                # (``Verdin``) that ``get_species_keywords_for_photos`` —
                # and therefore Browse, life list, and species_representative
                # lookups — report for it. Falls back to raw ``k.name`` for
                # taxonomy-less legacy rows so user-created/offline species
                # tags continue to filter.
                def _species_exists(name_op):
                    # ``name_op`` is a SQL fragment with ``{name_col}`` for
                    # the column reference — e.g. ``{name_col} = ?`` or
                    # ``{name_col} LIKE ?``. Formatted four times, once per
                    # branch below; parameter order below must match this
                    # ordering.
                    legacy_pred = name_op.format(name_col="k.name")
                    # An attached root (``k.parent_id IS NULL``) surfaces
                    # under its own stored spelling everywhere else:
                    # ``get_species_keywords_for_photos`` keeps it (its
                    # ``is_root`` guard) and ``/api/filters/values``
                    # groups by ``kv.name``. Match only that spelling —
                    # never a same-taxon sibling root — so a taxon with
                    # multiple roots (say ``American Crow`` MIN(id) and
                    # ``crow (american)``) doesn't cross-match: selecting
                    # the ``American Crow`` suggestion must not return
                    # photos the typeahead counts under ``crow (american)``.
                    self_pred = name_op.format(name_col="k.name")
                    # A hierarchy leaf (``k.parent_id IS NOT NULL``) is
                    # displayed as the canonical MIN(id) root spelling of
                    # its taxon — that's what
                    # ``get_species_keywords_for_photos``'s
                    # ``canonical_roots`` and ``/api/filters/values``'s
                    # ``root_kv.id = MIN(id)`` both surface. Match only
                    # that MIN(id) root's name so the filter agrees with
                    # what typeahead offers and Browse shows; any-root
                    # matching would let a leaf photo satisfy a rule for
                    # a sibling-root spelling that never appears in the
                    # UI.
                    root_pred = name_op.format(name_col="root.name")
                    # A hierarchy leaf whose taxon has no top-level root row
                    # in ``keywords`` (repair detached the ``Verdin`` root
                    # and left only the ``Desert Verdin`` leaf) is shown as
                    # its own leaf spelling by
                    # ``get_species_keywords_for_photos`` and by
                    # ``/api/filters/values`` (``COALESCE(root.name, k.name)``).
                    # Fall back to matching the leaf's own ``k.name`` so a
                    # ``species is "Desert Verdin"`` rule matches those
                    # photos — otherwise the filter would silently exclude
                    # them (and ``is not`` would silently include them).
                    leaf_no_root_pred = name_op.format(name_col="k.name")
                    return (
                        "EXISTS (SELECT 1 FROM photo_keywords pk "
                        "JOIN keywords k ON k.id = pk.keyword_id "
                        "LEFT JOIN taxa t ON t.id = k.taxon_id "
                        "WHERE pk.photo_id = p.id "
                        "AND (k.is_species = 1 OR k.type = 'taxonomy') "
                        "AND (t.rank = 'species' OR t.rank IS NULL) "
                        "AND ("
                        f"(k.taxon_id IS NULL AND {legacy_pred})"
                        " OR (k.taxon_id IS NOT NULL AND ("
                        f"(k.parent_id IS NULL AND {self_pred})"
                        " OR (k.parent_id IS NOT NULL AND ("
                        "EXISTS ("
                        "SELECT 1 FROM keywords root "
                        "WHERE root.taxon_id = k.taxon_id "
                        "AND root.parent_id IS NULL "
                        "AND (root.is_species = 1 OR root.type = 'taxonomy') "
                        "AND root.id = ("
                        "SELECT MIN(id) FROM keywords "
                        "WHERE taxon_id = k.taxon_id "
                        "AND parent_id IS NULL "
                        "AND (is_species = 1 OR type = 'taxonomy')) "
                        f"AND {root_pred})"
                        " OR (NOT EXISTS ("
                        "SELECT 1 FROM keywords rootless "
                        "WHERE rootless.taxon_id = k.taxon_id "
                        "AND rootless.parent_id IS NULL "
                        "AND (rootless.is_species = 1 OR rootless.type = 'taxonomy')"
                        f") AND {leaf_no_root_pred})"
                        "))))))"
                    )
                if op == "contains":
                    # Escape user LIKE metacharacters so ``%``/``_`` in the
                    # value stay literal — matches the other text/folder
                    # rules and blocks a ``value="%"`` request from matching
                    # every species-tagged photo. One param each for the
                    # legacy no-taxon branch, the attached-root branch, the
                    # same-taxon root lookup, and the rootless-leaf fallback.
                    like = f"%{self._escape_like(str(value or ''))}%"
                    return (
                        _species_exists("{name_col} LIKE ? ESCAPE '\\'"),
                        [like, like, like, like],
                    )
                if op == "not_contains":
                    like = f"%{self._escape_like(str(value or ''))}%"
                    return (
                        "NOT " + _species_exists("{name_col} LIKE ? ESCAPE '\\'"),
                        [like, like, like, like],
                    )
                if op in ("equals", "is"):
                    return _species_exists("{name_col} = ?"), [value, value, value, value]
                if op == "is not":
                    return "NOT " + _species_exists("{name_col} = ?"), [value, value, value, value]
            raise ValueError(f"unsupported collection rule field/op: {field}/{op}")

        def _build_node(node):
            if "rules" in node and "field" not in node:
                mode = node.get("mode", "all")
                child_sql = []
                params = []
                for child in node.get("rules", []):
                    sql, child_params = _build_node(child)
                    if sql:
                        child_sql.append(f"({sql})")
                        params.extend(child_params)
                if not child_sql:
                    return ("0", []) if mode == "any" else (None, [])
                if mode == "all":
                    return " AND ".join(child_sql), params
                if mode == "any":
                    return " OR ".join(child_sql), params
                return "NOT (" + " OR ".join(child_sql) + ")", params
            return _build_leaf(node)

        _validate_node(root)
        condition, params = _build_node(root)

        # Always join folders for folder-under rules, scoped to workspace.
        # For metadata-only callers (Dashboard scope) drop the accessible-
        # folder filter so photos in an offline folder still count toward
        # the collection's membership; every other caller keeps the Browse-
        # oriented ``status IN ('ok', 'partial')`` filter that excludes them.
        if include_offline_folders:
            folder_join = " JOIN folders f ON f.id = p.folder_id"
        else:
            folder_join = " JOIN folders f ON f.id = p.folder_id AND f.status IN ('ok', 'partial')"
        folder_join += " JOIN workspace_folders wf ON wf.folder_id = f.id AND wf.workspace_id = ?"

        # folder_join comes before join_clause in the query, so its param goes first
        params.insert(0, self.workspace_id)

        where = f"WHERE {condition}" if condition else ""

        return folder_join, "", where, params

    def _top_prediction_confidence_params(self):
        """Bind values for ``_TOP_PREDICTION_CONFIDENCE_EXPR``, in SQL order.

        The detector floor is the workspace-effective ``detector_confidence``
        — the same read ``_build_query_from_rules`` makes for its prediction
        predicates. Reading it here rather than baking a constant in keeps
        the sort, the badge, and the universal filter agreeing about which
        detections exist after the user moves the slider.
        """
        import config as cfg
        floor = float(
            self.get_effective_config(cfg.load()).get("detector_confidence", 0.2)
        )
        return [self.workspace_id, floor]

    def _photo_sort_clause(self, sort):
        """Return ``(order_by_sql, params)`` for a photo-list sort key.

        One definition for every unstacked photo list — the legacy
        ``/api/photos`` reads, the universal-filter reads, the collection
        reads, and the position probes that must rank the same order those
        page through. They used to carry six copies of the same dict; a sort
        added to one of them and missed in another silently degrades to
        capture-date in whichever query the caller happened to hit.

        Unknown keys fall back to capture-date ascending, matching the
        previous ``.get(sort, _PHOTO_DATE_ASC_ORDER)`` behaviour.

        Callers must splice ``params`` in at the ORDER BY's *textual*
        position: after the WHERE parameters for an ordinary
        ``... WHERE ... ORDER BY ...`` query, but before them when the
        clause sits inside a ``ROW_NUMBER() OVER (ORDER BY ...)`` in the
        select list.
        """
        direction = self._PREDICTION_CONFIDENCE_SORTS.get(sort)
        if direction is not None:
            order = (
                f"{self._TOP_PREDICTION_CONFIDENCE_EXPR} {direction} NULLS LAST, "
                "p.filename ASC, p.id ASC"
            )
            return order, self._top_prediction_confidence_params()
        return self._PHOTO_SORT_ORDERS.get(sort, self._PHOTO_DATE_ASC_ORDER), []

    def get_photos(
        self,
        collection_id,
        page=1,
        per_page=50,
        photo_ids=None,
        sort="date",
        include_offline_folders=False,
    ):
        """Build SQL from collection rules and return matching photos.

        ``photo_ids`` optionally narrows the collection query to a small set.
        ID Conflicts uses this after a decision so it can refresh only the
        photos the write touched instead of rebuilding the whole collection.
        The collection rules still apply, which also lets the client detect a
        photo that left the collection because its species keywords changed.
        """
        parts = self._build_collection_query(
            collection_id,
            include_offline_folders=include_offline_folders,
        )
        if parts is None:
            return []

        folder_join, join_clause, where, params = parts
        if photo_ids is not None:
            narrowed_ids = list(dict.fromkeys(int(pid) for pid in photo_ids))
            if not narrowed_ids:
                return []
            placeholders = ",".join("?" for _ in narrowed_ids)
            id_condition = f"p.id IN ({placeholders})"
            if where:
                body = where[len("WHERE "):]
                where = f"WHERE ({body}) AND {id_condition}"
            else:
                where = f"WHERE {id_condition}"
            params.extend(narrowed_ids)
        page = max(1, page)
        offset = (page - 1) * per_page
        order, order_params = self._photo_sort_clause(sort)
        params.extend(order_params)
        params.extend([per_page, offset])

        pcols = ", ".join(f"p.{c.strip()}" for c in self.PHOTO_COLS.split(","))
        if include_offline_folders:
            pcols += ", f.status AS folder_status"
        query = f"""
            SELECT DISTINCT {pcols} FROM photos p
            {folder_join}
            {join_clause}
            {where}
            ORDER BY {order}
            LIMIT ? OFFSET ?
        """
        return self.conn.execute(query, params).fetchall()

    def get_photo_ids(self, collection_id, sort="date"):
        """Return all photo IDs matching a collection in display order.

        ``sort`` mirrors ``get_collection_photos`` so the ``/photo-ids``
        endpoint that drives ``selectedPhotos`` insertion order for
        Select-all-matching stays aligned with the grid the user sees —
        without this, Best Batch seed, burst-review order, and export
        preview would start from the date-ordered first photo even when
        the grid is sorted by name, rating, sharpness, or quality.
        """
        parts = self._build_collection_query(collection_id)
        if parts is None:
            return []

        folder_join, join_clause, where, params = parts
        order, order_params = self._photo_sort_clause(sort)
        query = f"""
            SELECT DISTINCT p.id FROM photos p
            {folder_join}
            {join_clause}
            {where}
            ORDER BY {order}
        """
        params = [*params, *order_params]
        return [row["id"] for row in self.conn.execute(query, params).fetchall()]

    def count_photos(
        self, collection_id, include_offline_folders=False,
    ):
        """Return the count of photos matching collection rules.

        The default is the actionable count from accessible folders. Pass
        ``include_offline_folders=True`` for stable collection membership:
        photos remain members while their storage is temporarily missing.
        """
        parts = self._build_collection_query(
            collection_id,
            include_offline_folders=include_offline_folders,
        )
        if parts is None:
            return 0

        folder_join, join_clause, where, params = parts
        query = f"""
            SELECT COUNT(DISTINCT p.id) FROM photos p
            {folder_join}
            {join_clause}
            {where}
        """
        return self.conn.execute(query, params).fetchone()[0]

    def count_photo_availability(self, collection_id):
        """Return stable membership and actionable counts for a collection."""
        parts = self._build_collection_query(
            collection_id, include_offline_folders=True,
        )
        if parts is None:
            return {"total": 0, "available": 0, "offline": 0}

        folder_join, join_clause, where, params = parts
        row = self.conn.execute(
            f"""SELECT
                    COUNT(DISTINCT p.id) AS total,
                    COUNT(DISTINCT CASE
                        WHEN f.status IN ('ok', 'partial') THEN p.id
                    END) AS available
                FROM photos p
                {folder_join}
                {join_clause}
                {where}""",
            params,
        ).fetchone()
        total = row["total"] or 0
        available = row["available"] or 0
        return {
            "total": total,
            "available": available,
            "offline": max(0, total - available),
        }

    def count_photos_for_rules(
        self,
        rules,
        collection_id=None,
        folder_id=None,
        include_offline_folders=False,
    ):
        """Return the number of photos in the active workspace that match
        an unsaved rules list. Used by the smart-collection modal preview
        and /api/photos/query totals.

        Raises ValueError on malformed input (propagated from
        ``_build_query_from_rules``).
        """
        folder_join, join_clause, where, params = self._build_query_from_rules(
            rules, include_offline_folders=include_offline_folders,
        )
        where, params = self._append_collection_restriction(
            collection_id,
            where,
            params,
            include_offline_folders=include_offline_folders,
        )
        where, params = self._append_folder_restriction(folder_id, where, params)
        query = f"""
            SELECT COUNT(DISTINCT p.id) FROM photos p
            {folder_join}
            {join_clause}
            {where}
        """
        return self.conn.execute(query, params).fetchone()[0]

    def _append_folder_restriction(self, folder_id, where, params):
        """AND a folder-subtree restriction onto built rule clauses, matching
        get_photos' folder_id semantics (the folder and its descendants).

        Wraps the existing WHERE body in parentheses before ANDing so a
        top-level ``any`` rule tree (``WHERE (A) OR (B)``) doesn't have the
        folder restriction bind only to the last OR branch — AND binds
        tighter than OR in SQL, and without wrapping photos outside the
        selected folder would leak through the earlier branches.
        """
        if folder_id is None:
            return where, params
        subtree = self.get_folder_subtree_ids(folder_id)
        placeholders = ",".join("?" for _ in subtree)
        clause = f"p.folder_id IN ({placeholders})"
        if where:
            body = where[len("WHERE "):]
            where = f"WHERE ({body}) AND {clause}"
        else:
            where = f"WHERE {clause}"
        return where, list(params) + list(subtree)

    def _append_collection_restriction(
        self,
        collection_id,
        where,
        params,
        include_offline_folders=False,
    ):
        """AND a collection-membership subquery onto built rule clauses.

        Lets /api/photos/query serve Browse's dashboard-scoped collection
        view (collection as a restriction on the filtered grid) without the
        rule tree needing to reference collections. Raises ValueError for a
        collection missing from the active workspace. Wraps the existing
        WHERE body in parentheses before ANDing for the same OR-precedence
        reason as ``_append_folder_restriction``.
        """
        if collection_id is None:
            return where, params
        parts = self._build_collection_query(
            collection_id,
            include_offline_folders=include_offline_folders,
        )
        if parts is None:
            raise ValueError("collection not found in active workspace")
        c_folder_join, c_join, c_where, c_params = parts
        sub = (
            f"SELECT DISTINCT p.id FROM photos p {c_folder_join} "
            f"{c_join} {c_where}"
        )
        clause = f"p.id IN ({sub})"
        if where:
            body = where[len("WHERE "):]
            where = f"WHERE ({body}) AND {clause}"
        else:
            where = f"WHERE {clause}"
        return where, list(params) + list(c_params)

    def query_photos(
        self,
        rules,
        sort="date",
        page=1,
        per_page=50,
        collection_id=None,
        folder_id=None,
        include_offline_folders=False,
    ):
        """Return paginated photos matching a universal-filter rule tree.

        The rules format is the smart-collection tree (see
        ``_build_query_from_rules``); ``count_photos_for_rules`` gives the
        matching total. Raises ValueError on malformed rules.
        """
        folder_join, join_clause, where, params = self._build_query_from_rules(
            rules, include_offline_folders=include_offline_folders,
        )
        where, params = self._append_collection_restriction(
            collection_id,
            where,
            params,
            include_offline_folders=include_offline_folders,
        )
        where, params = self._append_folder_restriction(folder_id, where, params)
        order, order_params = self._photo_sort_clause(sort)
        page = max(1, page)
        offset = (page - 1) * per_page
        pcols = ", ".join(f"p.{c.strip()}" for c in self.PHOTO_COLS.split(","))
        if include_offline_folders:
            pcols += ", f.status AS folder_status"
        query = f"""
            SELECT DISTINCT {pcols} FROM photos p
            {folder_join}
            {join_clause}
            {where}
            ORDER BY {order}
            LIMIT ? OFFSET ?
        """
        return self.conn.execute(
            query, [*params, *order_params, per_page, offset]
        ).fetchall()

    def _burst_run_ctes(self, settings):
        """Return the CTE chain that turns a ``burst_candidates`` CTE into one
        ``_burst_key`` per photo plus a ``burst_sizes`` tally, and the params
        it consumes.

        Shared verbatim by the stacked Browse SQL and by the visual-search
        collapse path, so the metadata grid and a relevance-ordered result
        can never disagree about where a burst starts and ends. The caller
        supplies a ``burst_candidates`` CTE exposing ``id``, ``folder_id``,
        ``timestamp``, ``_stack_species`` and ``_stack_location``, left open
        (this fragment closes it).
        """
        window = self._STACK_RUN_WINDOW
        # In ``break`` mode a keyword change ends the run, so the run number
        # alone identifies the stack and an A/B/A run stays in shooting order
        # as three stacks. In ``partition`` mode the run is purely temporal
        # and the keyword sets join the key instead, which regroups the same
        # run into two stacks rather than fragmenting it.
        if settings["split_mode"] == "partition":
            keyword_break = ""
            burst_key = (
                "'burst:' || folder_id || ':' || _run"
                " || ':s' || COALESCE(_stack_species, '')"
                " || ':l' || COALESCE(_stack_location, '')"
            )
        else:
            keyword_break = f"""
                                 OR COALESCE(_stack_species, '') <> COALESCE(
                                        LAG(_stack_species) OVER ({window}), '')
                                 OR COALESCE(_stack_location, '') <> COALESCE(
                                        LAG(_stack_location) OVER ({window}), '')"""
            burst_key = "'burst:' || folder_id || ':' || _run"
        # An unparseable capture time yields a NULL julianday, and a NULL
        # comparison is not "within the gap" — it starts a new run, so a bad
        # timestamp can only ever under-stack, never glue unrelated frames.
        fragment = f"""
            ), burst_starts AS (
                SELECT id, folder_id, timestamp, _stack_species, _stack_location,
                       CASE WHEN LAG(timestamp) OVER ({window}) IS NULL
                                 OR julianday(timestamp) IS NULL
                                 OR julianday(LAG(timestamp) OVER ({window})) IS NULL
                                 OR (julianday(timestamp)
                                     - julianday(LAG(timestamp) OVER ({window})))
                                    * 86400.0 > ?{keyword_break}
                            THEN 1 ELSE 0 END AS _run_start
                FROM burst_candidates
            ), burst_runs AS (
                SELECT id, folder_id, _stack_species, _stack_location,
                       SUM(_run_start) OVER (
                           {window}
                           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                       ) AS _run
                FROM burst_starts
            ), burst_keys AS (
                SELECT id, {burst_key} AS _burst_key FROM burst_runs
            ), burst_sizes AS (
                SELECT _burst_key, COUNT(*) AS _burst_count
                FROM burst_keys GROUP BY _burst_key
            )"""
        return fragment, [settings["time_gap"] + self.BURST_GAP_TOLERANCE_SECONDS]

    def _browse_stack_query_parts(self, rules, collection_id=None, folder_id=None,
                                  include_offline_folders=False,
                                  stack_config=None, sort=None):
        """Build the scoped CTE shared by stacked Browse list/count queries.

        ``sort`` is the sort the caller will order by. It only matters for
        the prediction-confidence sorts, which rank on a value that is not a
        ``photos`` column: for those the per-photo score is projected into
        ``scoped`` as ``_prediction_confidence`` so the window functions
        downstream can read it. Every other sort (and ``count_browse_stacks``,
        which does not sort at all) leaves the column — and its correlated
        subquery — out entirely.

        Stacks are a presentation of the current result set, not durable
        catalog state: filters apply to members first, then exact duplicates
        and camera bursts collapse only when at least two matching photos
        remain. Exact duplicates claim their members before bursts so one
        photo can never appear in two Browse items. Because the burst run is
        computed over the *matching* photos, a filter that removes the middle
        of a run can split it — that is the same "stacks describe what you
        are looking at" rule, applied to time instead of membership.

        A burst is a run of frames from one folder whose consecutive capture
        times are no more than ``browse_stack_time_gap`` seconds apart and
        which carry the same species and location keywords. Frames without a
        capture time never join a burst — there is nothing to measure them
        against — and offline frames are excluded as described below.

        Keyword agreement is part of the burst identity on purpose. An
        untagged frame sitting inside an otherwise-tagged run breaks out as
        its own item rather than hiding behind a tagged cover, which is what
        makes the remaining tagging work visible while culling; as those
        frames are tagged they rejoin the run. ``browse_stack_split_mode``
        chooses how a mid-run change is resolved: ``break`` (the default)
        starts a new stack at every change, so a run tagged A, B, A yields
        three stacks in shooting order, while ``partition`` groups the run by
        keyword set and yields two.

        Earlier versions keyed bursts on ``photos.burst_id`` (EXIF
        ImageUniqueID, the only value the scanner writes there). That is not
        a burst identifier: the cameras that write it at all reuse the value
        across the life of the body, so it produced a handful of enormous
        false stacks spanning years while every real burst — from cameras
        that omit the tag entirely, which is most of them — stayed unstacked.
        The column is still filterable; it is no longer a stacking signal.

        ``include_offline_folders`` is the opt-in offline collection view
        (PR #1563). Photos whose folder is offline are *shown* — they render
        as read-only placeholders — but they never join a stack: each one
        keeps its own ``photo:<id>`` key and is left out of every duplicate /
        burst tally. They are also left out of the run sequence entirely, so
        an offline frame in the middle of a burst neither splits it nor
        contributes its keywords to it. Three reasons, all about not lying to
        the user:

        * A stack's cover is its only interactive card, and
          ``_STACK_COVER_ORDER`` ranks on quality alone. An offline member
          could win the cover and turn a stack holding perfectly reachable
          frames into a dead placeholder.
        * The stack badge reads as "N photos here to cull". Counting frames
          the user cannot rate, flag, or delete would make that count a
          proxy rather than an answer (CORE_PHILOSOPHY, "no black boxes").
        * A frame that is not shown as part of the stack must not silently
          decide where the stack ends.
        """
        settings = self.normalize_browse_stack_config(stack_config)
        folder_join, join_clause, where, params = self._build_query_from_rules(
            rules, include_offline_folders=include_offline_folders,
        )
        where, params = self._append_collection_restriction(
            collection_id, where, params,
            include_offline_folders=include_offline_folders,
        )
        where, params = self._append_folder_restriction(folder_id, where, params)
        pcols = ", ".join(f"p.{c.strip()}" for c in self.PHOTO_COLS.split(","))
        if include_offline_folders:
            pcols += ", f.status AS folder_status"
            offline_expr = (
                "CASE WHEN f.status IN ('ok', 'partial') THEN 0 ELSE 1 END"
            )
        else:
            offline_expr = "0"
        scoped_extra = ""
        scoped_params = []
        if sort in self._PREDICTION_CONFIDENCE_SORTS:
            scoped_extra = (
                f",\n                       {self._TOP_PREDICTION_CONFIDENCE_EXPR}"
                " AS _prediction_confidence"
            )
            scoped_params = self._top_prediction_confidence_params()
        run_ctes, run_params = self._burst_run_ctes(settings)
        ctes = f"""
            WITH {self._STACK_KEYWORD_SET_CTES}
            , scoped AS (
                SELECT DISTINCT {pcols},
                       p.file_hash AS _stack_file_hash,
                       {offline_expr} AS _stack_offline{scoped_extra}
                FROM photos p
                {folder_join}
                {join_clause}
                {where}
            ), duplicate_counts AS (
                SELECT scoped.*,
                       CASE WHEN _stack_offline = 1
                                 OR NULLIF(_stack_file_hash, '') IS NULL THEN 0 ELSE
                           SUM(CASE WHEN _stack_offline = 0 THEN 1 ELSE 0 END)
                           OVER (PARTITION BY _stack_file_hash)
                       END AS _duplicate_count
                FROM scoped
            ), burst_candidates AS (
                -- Only photos a burst could actually claim take part in the
                -- sequence, so members lost to a duplicate stack (or to the
                -- offline rule) neither split a run nor pad its count.
                SELECT duplicate_counts.id, duplicate_counts.folder_id,
                       duplicate_counts.timestamp,
                       photo_stack_species.keyword_set AS _stack_species,
                       photo_stack_location.keyword_set AS _stack_location
                FROM duplicate_counts
                {self._STACK_KEYWORD_JOINS.format(alias="duplicate_counts")}
                WHERE _stack_offline = 0
                  AND _duplicate_count < 2
                  AND timestamp IS NOT NULL
            {run_ctes}
            , keyed AS (
                SELECT duplicate_counts.*,
                       CASE
                         WHEN _duplicate_count >= 2
                           THEN 'duplicate:' || _stack_file_hash
                         WHEN burst_sizes._burst_count >= 2
                           THEN burst_keys._burst_key
                         ELSE 'photo:' || duplicate_counts.id
                       END AS _stack_key,
                       CASE
                         WHEN _duplicate_count >= 2 THEN 'duplicate'
                         WHEN burst_sizes._burst_count >= 2 THEN 'burst'
                         ELSE NULL
                       END AS _stack_kind
                FROM duplicate_counts
                LEFT JOIN burst_keys ON burst_keys.id = duplicate_counts.id
                LEFT JOIN burst_sizes
                       ON burst_sizes._burst_key = burst_keys._burst_key
            )
        """
        # ``scoped``'s select list precedes its FROM/JOIN/WHERE, and the
        # keyword-set CTEs ahead of it are parameterless, so the optional
        # confidence parameter binds before the scope's.
        return ctes, [*scoped_params, *params, *run_params]

    def _stack_sort_spec(self, sort):
        return self._STACK_SORT_SPECS.get(sort, self._STACK_SORT_SPECS["date"])

    def _stack_sort_clause(self, sort):
        return self._stack_sort_spec(sort)["order"]

    def _ranked_stack_query(self, rules, sort="date", collection_id=None,
                            folder_id=None, include_offline_folders=False,
                            stack_config=None):
        """Return the CTE + ``ranked`` window-function block shared by every
        stack-projected query. Callers append their own outer SELECT (with
        ORDER BY and optional LIMIT/OFFSET).

        ``sort`` selects which member each stack's ``_stack_lead_*`` columns
        are read from, so it must match the ``_stack_sort_clause(sort)`` the
        caller orders by.
        """
        ctes, params = self._browse_stack_query_parts(
            rules, collection_id=collection_id, folder_id=folder_id,
            include_offline_folders=include_offline_folders,
            stack_config=stack_config, sort=sort,
        )
        cover_order = self._STACK_COVER_ORDER
        spec = self._stack_sort_spec(sort)
        member_order = spec["member"]
        lead_key = spec["key"]
        ranked = ctes + f"""
            , ranked AS (
                SELECT keyed.*,
                       ROW_NUMBER() OVER (
                           PARTITION BY _stack_key ORDER BY {cover_order}
                       ) AS _stack_cover_rank,
                       COUNT(*) OVER (PARTITION BY _stack_key)
                           AS _browse_stack_count,
                       GROUP_CONCAT(id) OVER (
                           PARTITION BY _stack_key
                           ORDER BY timestamp IS NULL, timestamp, filename, id
                           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                       )
                           AS _browse_stack_member_ids,
                       -- The whole sort key is read off one member: the one
                       -- the unstacked list would show first. Per-term
                       -- aggregates (MAX(rating) beside MIN(filename)) mix
                       -- members and move stacks that the unstacked list
                       -- ranks lower. See _STACK_SORT_SPECS.
                       FIRST_VALUE({lead_key}) OVER (
                           PARTITION BY _stack_key ORDER BY {member_order}
                       ) AS _stack_lead_key,
                       FIRST_VALUE(filename) OVER (
                           PARTITION BY _stack_key ORDER BY {member_order}
                       ) AS _stack_lead_filename,
                       FIRST_VALUE(id) OVER (
                           PARTITION BY _stack_key ORDER BY {member_order}
                       ) AS _stack_lead_id
                FROM keyed
            )
        """
        return ranked, params

    def query_browse_stacks(self, rules, sort="date", page=1, per_page=50,
                            collection_id=None, folder_id=None,
                            include_offline_folders=False, stack_config=None):
        """Return one representative row per exact-duplicate or burst stack.

        The returned rows have three private columns consumed by the HTTP
        layer: ``_browse_stack_kind``, ``_browse_stack_count``, and
        ``_browse_stack_member_ids``. Singles carry a null kind and otherwise
        retain the ordinary photo-list shape.

        Under a prediction-confidence sort they carry a fourth,
        ``_stack_lead_prediction_confidence``: the score that actually
        positioned the item. A stack is placed by its *leading member* (see
        ``_STACK_SORT_SPECS``) while its cover is chosen on quality, so those
        are often different frames — and a badge showing the cover's own
        score would then name a number that did not decide where the card
        sits. The HTTP layer prefers this value for the card's
        ``prediction_confidence`` (Codex P2 on PR #1670).
        """
        ranked, params = self._ranked_stack_query(
            rules, sort=sort, collection_id=collection_id, folder_id=folder_id,
            include_offline_folders=include_offline_folders,
            stack_config=stack_config,
        )
        lead_confidence = (
            ",\n                   _stack_lead_key"
            " AS _stack_lead_prediction_confidence"
            if sort in self._PREDICTION_CONFIDENCE_SORTS else ""
        )
        order = self._stack_sort_clause(sort)
        page = max(1, page)
        offset = (page - 1) * per_page
        query = ranked + f"""
            SELECT ranked.*,
                   _stack_kind AS _browse_stack_kind{lead_confidence}
            FROM ranked
            WHERE _stack_cover_rank = 1
            ORDER BY {order}
            LIMIT ? OFFSET ?
        """
        return self.conn.execute(query, [*params, per_page, offset]).fetchall()

    def browse_stack_totals(self, rules, collection_id=None, folder_id=None,
                           include_offline_folders=False, stack_config=None):
        """Count all items and multi-photo stacks in one scoped projection."""
        ctes, params = self._browse_stack_query_parts(
            rules, collection_id=collection_id, folder_id=folder_id,
            include_offline_folders=include_offline_folders,
            stack_config=stack_config,
        )
        row = self.conn.execute(
            ctes + """
                SELECT COUNT(DISTINCT _stack_key) AS total,
                       COUNT(DISTINCT CASE WHEN _stack_kind IS NOT NULL
                             THEN _stack_key END) AS stack_count
                FROM keyed
            """,
            params,
        ).fetchone()
        return {"total": int(row["total"]), "stack_count": int(row["stack_count"])}

    @staticmethod
    def _candidate_preference_case(column, ids):
        """ORDER BY fragment ranking ``ids`` in the order they were asked for.

        Position alone does not decide between frames of one stack: they all
        report their cover's position, so a plain ``ORDER BY position, id``
        would answer with whichever frame happens to hold the lowest ID. The
        caller asked about a card, and it named the frame that *is* that card
        first — answering with a hidden member instead would send the client
        off to expand a tray around a frame the user never opened. Binds one
        parameter per ID (the ``IN`` list binds them once more).
        """
        whens = " ".join(f"WHEN ? THEN {rank}" for rank in range(len(ids)))
        return f"CASE {column} {whens} ELSE {len(ids)} END"

    def query_browse_stack_position_first(self, rules, photo_ids, sort="date",
                                          collection_id=None, folder_id=None,
                                          include_offline_folders=False,
                                          stack_config=None):
        """The earliest-placed of ``photo_ids`` once stacks are projected.

        Returns ``(photo_id, position)`` for whichever candidate the grid
        shows first, or ``None`` when the result set contains none of them.

        Browse asks with a list when the card it wants to keep is a stack:
        the reload it is about to paint can drop any individual frame — a
        saved expression that matches only part of a burst, an undo that
        moves frames out of the filter — and any surviving frame the user
        picked identifies the same card. One ranking answers for all of
        them, so the fallback costs one query rather than one page request
        per frame. Ties (frames of one stack share their cover's position)
        resolve by ID so the answer is stable.

        Raises ValueError on malformed rules.
        """
        ids = [int(pid) for pid in dict.fromkeys(photo_ids)]
        if not ids:
            return None
        ranked, params = self._ranked_stack_query(
            rules, sort=sort, collection_id=collection_id, folder_id=folder_id,
            include_offline_folders=include_offline_folders,
            stack_config=stack_config,
        )
        order = self._stack_sort_clause(sort)
        placeholders = ",".join("?" for _ in ids)
        preference = self._candidate_preference_case("ranked.id", ids)
        query = ranked + f"""
            , cover_positions AS (
                SELECT _stack_key AS _position_key,
                       ROW_NUMBER() OVER (ORDER BY {order}) - 1 AS position
                FROM ranked
                WHERE _stack_cover_rank = 1
            )
            SELECT ranked.id AS id, cover_positions.position AS position
            FROM cover_positions
            JOIN ranked ON ranked._stack_key = cover_positions._position_key
            WHERE ranked.id IN ({placeholders})
            ORDER BY position, {preference}
            LIMIT 1
        """
        row = self.conn.execute(query, [*params, *ids, *ids]).fetchone()
        if row is None:
            return None
        return int(row["id"]), int(row["position"])

    def _stacked_photo_ids(self, rules, sort="date",
                           collection_id=None, folder_id=None,
                           stack_config=None):
        """Shared cover-first stacked ID projection used by every select-all
        endpoint that honors Stacks. For each stack (in the same order
        ``query_browse_stacks`` places covers) emits the cover ID first, then
        the remaining member IDs in the intra-stack order the projection
        exposes as ``browse_stack.photo_ids``. Singles carry themselves.
        """
        ranked, params = self._ranked_stack_query(
            rules, sort=sort, collection_id=collection_id, folder_id=folder_id,
            stack_config=stack_config,
        )
        order = self._stack_sort_clause(sort)
        query = ranked + f"""
            SELECT id, _browse_stack_member_ids
            FROM ranked
            WHERE _stack_cover_rank = 1
            ORDER BY {order}
        """
        photo_ids = []
        for row in self.conn.execute(query, params).fetchall():
            cover_id = row["id"]
            raw = row["_browse_stack_member_ids"]
            member_ids = (
                [int(value) for value in str(raw).split(",") if value]
                if raw
                else [cover_id]
            )
            # Cover first, then the intra-stack order minus the cover — the
            # same shape ``browse_stack.photo_ids`` carries in the paginated
            # /photos response, so the client's first selected id is always
            # the visible top-level card.
            seen = {cover_id}
            photo_ids.append(cover_id)
            for member_id in member_ids:
                if member_id in seen:
                    continue
                photo_ids.append(member_id)
                seen.add(member_id)
        return photo_ids

    def _burst_keys_for_ids(self, photo_ids, stack_config=None):
        """Return ``{photo_id: burst_key}`` for the ids that land in a burst
        of two or more, using the same CTE chain as the stacked Browse SQL.

        The candidate ids go through a TEMP table rather than an ``IN (...)``
        list: the burst key depends on each frame's neighbours in capture
        order, so chunking the ids would invent a run boundary wherever a
        real burst happened to straddle a chunk.
        """
        ids = list(dict.fromkeys(photo_ids or ()))
        if len(ids) < 2:
            return {}
        settings = self.normalize_browse_stack_config(stack_config)
        run_ctes, run_params = self._burst_run_ctes(settings)
        self.conn.execute("DROP TABLE IF EXISTS temp._burst_scope")
        self.conn.execute(
            "CREATE TEMP TABLE _burst_scope (id INTEGER PRIMARY KEY)"
        )
        try:
            self.conn.executemany(
                "INSERT OR IGNORE INTO _burst_scope (id) VALUES (?)",
                [(pid,) for pid in ids],
            )
            query = f"""
                WITH {self._STACK_KEYWORD_SET_CTES}
                , burst_candidates AS (
                    SELECT p.id, p.folder_id, p.timestamp,
                           photo_stack_species.keyword_set AS _stack_species,
                           photo_stack_location.keyword_set AS _stack_location
                    FROM photos p
                    JOIN _burst_scope scope ON scope.id = p.id
                    {self._STACK_KEYWORD_JOINS.format(alias="p")}
                    WHERE p.timestamp IS NOT NULL
                {run_ctes}
                SELECT burst_keys.id AS id, burst_keys._burst_key AS burst_key
                FROM burst_keys
                JOIN burst_sizes
                  ON burst_sizes._burst_key = burst_keys._burst_key
                WHERE burst_sizes._burst_count >= 2
            """
            return {
                row["id"]: row["burst_key"]
                for row in self.conn.execute(query, run_params).fetchall()
            }
        finally:
            self.conn.execute("DROP TABLE IF EXISTS temp._burst_scope")

    def collapse_browse_stack_photo_ids(self, photo_ids, standalone_ids=None,
                                        stack_config=None):
        """Collapse an already ordered ID result, preserving group order.

        Visual search has already materialized its relevance-ordered IDs, so
        re-running the metadata SQL would lose that order. This bounded helper
        applies the same duplicate-first overlap rule, then selects a
        quality-ranked cover for each logical item. Group *identity* comes
        from the shared projection (exact-duplicate hash, then
        ``_burst_keys_for_ids``) so a relevance-ordered result and the
        metadata grid always draw the same stack boundaries; only group
        *order* follows the relevance ranking, by first appearance.

        ``standalone_ids`` are photos that must never join a stack — the
        offline members of the opt-in offline collection view. They keep
        their place in the relevance order as their own single-photo item,
        and are left out of every duplicate / burst tally, matching the SQL
        projection in ``_browse_stack_query_parts``.
        """
        standalone = set(standalone_ids or ())
        ordered_ids = list(dict.fromkeys(photo_ids or []))
        if not ordered_ids:
            return []
        rows_by_id = {}
        columns = (
            "id, file_hash, flag, quality_score, subject_sharpness, "
            "sharpness, rating, width, height, file_size"
        )
        for chunk in self._chunks(ordered_ids):
            placeholders = ",".join("?" * len(chunk))
            for row in self.conn.execute(
                f"SELECT {columns} FROM photos WHERE id IN ({placeholders})",
                list(chunk),
            ).fetchall():
                rows_by_id[row["id"]] = row

        duplicate_counts = {}
        for pid in ordered_ids:
            if pid in standalone:
                continue
            row = rows_by_id.get(pid)
            file_hash = row["file_hash"] if row else None
            if file_hash:
                duplicate_counts[file_hash] = duplicate_counts.get(file_hash, 0) + 1
        # Bursts are resolved in SQL over exactly the frames a burst could
        # still claim, so a member already lost to a duplicate stack (or held
        # out as offline) neither splits a run nor pads its count — the same
        # ordering the metadata projection applies.
        burst_candidates = [
            pid for pid in ordered_ids
            if pid not in standalone
            and rows_by_id.get(pid) is not None
            and not (
                rows_by_id[pid]["file_hash"]
                and duplicate_counts.get(rows_by_id[pid]["file_hash"], 0) >= 2
            )
        ]
        burst_key_by_id = self._burst_keys_for_ids(
            burst_candidates, stack_config=stack_config,
        )

        groups = {}
        group_order = []
        for pid in ordered_ids:
            row = rows_by_id.get(pid)
            if not row:
                continue
            file_hash = row["file_hash"]
            if pid in standalone:
                key = (None, pid)
            elif file_hash and duplicate_counts.get(file_hash, 0) >= 2:
                key = ("duplicate", file_hash)
            elif pid in burst_key_by_id:
                key = ("burst", burst_key_by_id[pid])
            else:
                key = (None, pid)
            if key not in groups:
                groups[key] = []
                group_order.append(key)
            groups[key].append(pid)

        def cover_score(pid):
            row = rows_by_id[pid]
            flag_rank = {"flagged": 2, "none": 1, None: 1}.get(row["flag"], 0)
            return (
                flag_rank,
                row["quality_score"] if row["quality_score"] is not None else float("-inf"),
                row["subject_sharpness"] if row["subject_sharpness"] is not None else float("-inf"),
                row["sharpness"] if row["sharpness"] is not None else float("-inf"),
                row["rating"] is not None,
                row["rating"] if row["rating"] is not None else 0,
                (row["width"] or 0) * (row["height"] or 0),
                row["file_size"] or 0,
                -pid,
            )

        items = []
        for key in group_order:
            member_ids = groups[key]
            cover_id = max(member_ids, key=cover_score)
            items.append({
                "cover_id": cover_id,
                "kind": key[0],
                "member_ids": member_ids,
            })
        return items

    def query_photo_ids(
        self,
        rules,
        sort="date",
        collection_id=None,
        folder_id=None,
        include_offline_folders=False,
    ):
        """Return every photo id matching a universal-filter rule tree, in
        display order — the rules analog of ``get_photo_ids`` (select-all,
        visual-search candidate scope). Raises ValueError on malformed rules.
        """
        folder_join, join_clause, where, params = self._build_query_from_rules(
            rules, include_offline_folders=include_offline_folders,
        )
        where, params = self._append_collection_restriction(
            collection_id,
            where,
            params,
            include_offline_folders=include_offline_folders,
        )
        where, params = self._append_folder_restriction(folder_id, where, params)
        order, order_params = self._photo_sort_clause(sort)
        query = f"""
            SELECT DISTINCT p.id FROM photos p
            {folder_join}
            {join_clause}
            {where}
            ORDER BY {order}
        """
        return [
            row["id"]
            for row in self.conn.execute(query, [*params, *order_params]).fetchall()
        ]

    def query_photo_position_first(
        self,
        rules,
        photo_ids,
        sort="date",
        collection_id=None,
        folder_id=None,
        include_offline_folders=False,
    ):
        """The earliest-placed of ``photo_ids`` in a universal-filter result
        set, as ``(photo_id, position)``, or ``None`` when the result set
        contains none of them.

        Browse asks with a list when the card it is holding onto stands for
        several photos: any frame of a selected stack identifies the same
        card, so a reload that dropped some of them can still be placed by
        the ones it kept. Ranking once answers for every candidate; ties
        resolve by ID so the answer is stable.

        Browse calls this (through ``focus_photo_id`` /
        ``focus_photo_ids`` on ``/api/photos/query`` and the collection
        photos endpoint) when a re-sort has to hold onto the card the user
        has selected. Materializing the ordered ID list and indexing it
        client-side would move O(result set) IDs over the wire on every sort
        change; probing serial pages until the photo appears would issue
        O(position / per_page) filtered queries. A ROW_NUMBER window over the
        same scoped rows the grid pages through answers it in one read.

        Raises ValueError on malformed rules.
        """
        ids = [int(pid) for pid in dict.fromkeys(photo_ids)]
        if not ids:
            return None
        folder_join, join_clause, where, params = self._build_query_from_rules(
            rules, include_offline_folders=include_offline_folders,
        )
        where, params = self._append_collection_restriction(
            collection_id,
            where,
            params,
            include_offline_folders=include_offline_folders,
        )
        where, params = self._append_folder_restriction(folder_id, where, params)
        order, order_params = self._photo_sort_clause(sort)
        # Rank exactly the row set ``query_photos`` pages through, DISTINCT
        # included. Today every rule predicate compiles to an EXISTS subquery
        # so nothing fans out, but a window function is evaluated *before*
        # DISTINCT: were a rule ever to introduce a joining clause, ranking
        # the joined rows here would silently report a position the grid
        # never uses. De-duplicating in an inner subquery keeps the two in
        # step by construction. The subquery is aliased ``p`` so the shared
        # ORDER BY expressions keep resolving.
        #
        # Only the id and the sort keys are carried: ``photos.id`` is unique,
        # so de-duplicating on those is identical to de-duplicating on the
        # whole row, and the narrower projection is measurably cheaper to
        # materialize (~30% on a 74k-photo workspace) than ``PHOTO_COLS``.
        # A new entry in ``_PHOTO_SORT_ORDERS`` whose ORDER BY reads another
        # column has to be listed here too, or this query cannot resolve it.
        # The prediction-confidence sorts need nothing extra: they correlate
        # on ``p.id``, which the projection already carries.
        position_cols = (
            "p.id, p.timestamp, p.filename, p.rating, "
            "p.sharpness, p.quality_score"
        )
        placeholders = ",".join("?" for _ in ids)
        preference = self._candidate_preference_case("id", ids)
        query = f"""
            SELECT id, position FROM (
                SELECT id, ROW_NUMBER() OVER (ORDER BY {order}) - 1 AS position
                FROM (
                    SELECT DISTINCT {position_cols} FROM photos p
                    {folder_join}
                    {join_clause}
                    {where}
                ) p
            ) ordered_photos
            WHERE id IN ({placeholders})
            ORDER BY position, {preference}
            LIMIT 1
        """
        # ORDER BY sits in the outer select list, ahead of the inner
        # subquery's WHERE, so its parameters bind first.
        row = self.conn.execute(
            query, [*order_params, *params, *ids, *ids]
        ).fetchone()
        if row is None:
            return None
        return int(row["id"]), int(row["position"])

    def get_filter_field_values(self, field, rules=None, q=None, limit=20,
                                 folder_id=None, collection_id=None):
        """Distinct values (with photo counts) for a suggest-capable field.

        Counts respect the supplied rule tree, so the caller can pass the
        active expression minus the rule being edited and get live facet
        counts (design requirement: counts answer "how many results would I
        get", never a global COUNT(*)). ``folder_id``/``collection_id`` AND
        the same page-scope restrictions Browse applies to
        ``/api/photos/query``; without them the typeahead advertises counts
        computed over the whole workspace while the visible grid is
        folder/collection-scoped, so picking a suggestion can yield fewer
        (or zero) grid results than the badge promised. Raises ValueError
        for fields without value suggestions or malformed rules.
        """
        folder_join, join_clause, where, params = self._build_query_from_rules(
            rules if rules is not None else []
        )
        where, params = self._append_collection_restriction(collection_id, where, params)
        where, params = self._append_folder_restriction(folder_id, where, params)
        limit = max(1, min(int(limit or 20), 50))
        if field == "folder":
            return self._folder_filter_values(
                folder_join, join_clause, where, params, q=q, limit=limit
            )
        conditions = []
        extra_joins = ""
        extra_params = []
        if field in self._SUGGEST_VALUE_EXPRS:
            display_expr, group_expr = self._SUGGEST_VALUE_EXPRS[field]
        elif field in ("keyword", "species"):
            extra_joins = (
                " JOIN photo_keywords pkv ON pkv.photo_id = p.id"
                " JOIN keywords kv ON kv.id = pkv.keyword_id"
            )
            if field == "species":
                # Canonicalize hierarchy leaves (e.g. ``Desert Verdin``) to
                # the same-taxon top-level root (``Verdin``) that
                # ``get_species_keywords_for_photos`` — and therefore Browse,
                # life-list, and species_representative lookups — report.
                # Otherwise the typeahead would suggest the raw leaf spelling
                # for a photo whose species is displayed as ``Verdin`` in
                # every other view. Deterministic root pick (MIN(id)) mirrors
                # the ``setdefault`` in ``get_species_keywords_for_photos``.
                #
                # BUT: only rewrite hierarchy leaves. When ``kv`` is itself
                # a top-level root (``parent_id IS NULL``),
                # ``get_species_keywords_for_photos`` deliberately keeps the
                # attached root's stored spelling (see its ``is_root`` guard)
                # so curation lookups keyed on that name still resolve. If a
                # taxon has multiple species roots and a photo is tagged
                # with the non-MIN(id) root, rewriting to MIN(id) here would
                # drop the species name Browse shows and make the typeahead
                # return nothing for that root.
                extra_joins += (
                    " LEFT JOIN taxa tv ON tv.id = kv.taxon_id"
                    " LEFT JOIN keywords root_kv"
                    " ON kv.taxon_id IS NOT NULL"
                    " AND kv.parent_id IS NOT NULL"
                    " AND root_kv.taxon_id = kv.taxon_id"
                    " AND root_kv.parent_id IS NULL"
                    " AND (root_kv.is_species = 1 OR root_kv.type = 'taxonomy')"
                    " AND root_kv.id = ("
                    "SELECT MIN(id) FROM keywords"
                    " WHERE taxon_id = kv.taxon_id"
                    " AND parent_id IS NULL"
                    " AND (is_species = 1 OR type = 'taxonomy'))"
                )
                conditions.append("(kv.is_species = 1 OR kv.type = 'taxonomy')")
                conditions.append("(tv.rank = 'species' OR tv.rank IS NULL)")
                display_expr = (
                    "CASE WHEN kv.parent_id IS NULL THEN kv.name"
                    " ELSE COALESCE(root_kv.name, kv.name) END"
                )
                group_expr = display_expr
            else:
                display_expr = "kv.name"
                group_expr = display_expr
        else:
            raise ValueError(f"field {field!r} does not support value suggestions")
        # IS NOT NULL and the typeahead LIKE compare against ``group_expr``
        # so a facet grouped case-insensitively (camera fields, extension)
        # also matches case-insensitively — otherwise ``camera_model``
        # would list a single ``Sony A1`` bucket but drop it as soon as the
        # user typed ``sony``.
        conditions.append(f"{group_expr} IS NOT NULL")
        if q:
            conditions.append(f"{group_expr} LIKE ? ESCAPE '\\'")
            q_norm = str(q).lower() if group_expr.startswith("LOWER(") else str(q)
            extra_params.append(f"%{self._escape_like(q_norm)}%")
        joined = " AND ".join(conditions)
        # ``where`` from ``_build_query_from_rules`` is ``WHERE (A) OR (B)``
        # for a top-level ``any`` group. Appending ``AND {joined}`` without
        # wrapping would bind to the last OR branch (AND binds tighter than
        # OR in SQL) and let first-branch rows leak through even when the
        # facet field is NULL or the typeahead doesn't match — so the count
        # no longer answers "how many results would selecting this suggestion
        # return".
        if where:
            where_full = f"WHERE ({where[len('WHERE '):]}) AND {joined}"
        else:
            where_full = f"WHERE {joined}"
        query = f"""
            SELECT {display_expr} AS value, COUNT(DISTINCT p.id) AS count
            FROM photos p
            {folder_join}
            {join_clause}
            {extra_joins}
            {where_full}
            GROUP BY {group_expr}
            ORDER BY count DESC, value ASC
            LIMIT ?
        """
        rows = self.conn.execute(query, [*params, *extra_params, limit]).fetchall()
        return [{"value": row["value"], "count": row["count"]} for row in rows]

    def _folder_filter_values(self, folder_join, join_clause, where, params, q, limit):
        """Folder suggestions with subtree-aware counts.

        The ``folder`` field's engine operators are ``under``/``not_under``,
        which match a folder and every descendant. A count grouped by each
        photo's immediate ``f.path`` therefore misreports what selecting a
        suggested folder would return — a parent with no direct photos but
        matching descendants would be omitted entirely, and any folder with
        both direct and nested photos would undercount. Aggregate over each
        workspace folder's subtree so the facet answers "how many photos
        would ``folder under=<path>`` return" (design requirement: counts
        never lie about the rule they preview).
        """
        q_condition = ""
        q_params = []
        if q:
            q_condition = " AND ff.path LIKE ? ESCAPE '\\'"
            q_params.append(f"%{self._escape_like(str(q))}%")
        # Inner select: photos matching sibling rules with their folder
        # path. Reused verbatim so every rule-engine feature (workspace
        # scope, status filter, sibling predicates) applies unchanged.
        inner = f"""
            SELECT DISTINCT p.id AS pid, f.path AS ppath
            FROM photos p
            {folder_join}
            {join_clause}
            {where}
        """
        # Path normalization matches ``_build_query_from_rules`` for the
        # ``folder``/``under`` op so a Windows library's backslash paths
        # aggregate the same way here as they filter there. Both sides
        # collapse ``\`` to ``/`` and strip trailing separators — the
        # engine feeds its value through ``_path_for_subtree_match`` which
        # ``rstrip("/")``s, so folder roots stored with a trailing separator
        # (Windows drive root ``D:\`` normalizes to ``D:/``, POSIX root
        # ``/`` normalizes to ``/``) would otherwise concat to a LIKE
        # pattern with a doubled slash (``D://%`` / ``//%``) that never
        # matches real descendant paths — the suggested root would count 0
        # while ``folder under D:\`` actually returns every photo on the
        # drive. ``norm_folder_like`` additionally escapes SQL LIKE
        # metacharacters (``%`` / ``_``) so a folder like ``/pics/my_dir``
        # only aggregates its own subtree and does not double-count photos
        # under a sibling ``/pics/myXdir``; the engine's ``folder under``
        # op escapes the same characters via ``_escape_like``. The ``\\``
        # escape char is safe to introduce here because path normalization
        # above has already collapsed every literal ``\`` to ``/``.
        norm_photo = "RTRIM(REPLACE(matched.ppath, '\\', '/'), '/')"
        norm_folder = "RTRIM(REPLACE(ff.path, '\\', '/'), '/')"
        norm_folder_like = (
            f"REPLACE(REPLACE({norm_folder}, '%', '\\%'), '_', '\\_')"
        )
        query = f"""
            SELECT ff.path AS value, COUNT(DISTINCT matched.pid) AS count
            FROM folders ff
            JOIN workspace_folders ff_wf
              ON ff_wf.folder_id = ff.id AND ff_wf.workspace_id = ?
            JOIN ({inner}) matched ON (
                {norm_photo} = {norm_folder}
                OR {norm_photo} LIKE {norm_folder_like} || '/%' ESCAPE '\\'
            )
            WHERE ff.path IS NOT NULL{q_condition}
            GROUP BY ff.path
            ORDER BY count DESC, value ASC
            LIMIT ?
        """
        rows = self.conn.execute(
            query, [self.workspace_id, *params, *q_params, limit]
        ).fetchall()
        return [{"value": row["value"], "count": row["count"]} for row in rows]

    def photo_ids(self, collection_id):
        """Return the set of photo IDs in the collection, workspace-scoped.

        Returns an empty set for a missing collection. Used by stages
        that need to restrict writes to the current pipeline-run scope
        without paging through full photo rows.
        """
        parts = self._build_collection_query(collection_id)
        if parts is None:
            return set()

        folder_join, join_clause, where, params = parts
        query = f"""
            SELECT DISTINCT p.id FROM photos p
            {folder_join}
            {join_clause}
            {where}
        """
        return {row["id"] for row in self.conn.execute(query, params)}

    def create_defaults(self, workspace_id=None):
        """Create default smart collections, skipping any that already exist by name.

        Workspace defaults to the active one. Pass ``workspace_id`` to seed a
        specific workspace without needing it to be active — used by
        ``api_create_workspace`` so brand-new workspaces get the defaults at
        creation time instead of relying on a future startup pass.
        """
        ws_id = workspace_id if workspace_id is not None else self.workspace_id
        existing_names = {
            row["name"] for row in self.conn.execute(
                "SELECT name FROM collections WHERE workspace_id = ?", (ws_id,),
            ).fetchall()
        }

        defaults = [
            ("All Photos", [{"field": "all"}]),
            (
                "Needs Identification",
                self.NEEDS_IDENTIFICATION_RULES,
            ),
            ("Untagged", [{"field": "keyword_count", "op": "equals", "value": 0}]),
            ("Flagged", [{"field": "flag", "op": "equals", "value": "flagged"}]),
            (
                "Recent Import",
                [{"field": "timestamp", "op": "recent_days", "value": 30}],
            ),
            (
                "GPS Without Location Keyword",
                self.GPS_WITHOUT_LOCATION_KEYWORD_RULES,
            ),
        ]
        for name, rules in defaults:
            if name not in existing_names:
                self.conn.execute(
                    "INSERT INTO collections (name, rules, workspace_id) VALUES (?, ?, ?)",
                    (name, json.dumps(rules), ws_id),
                )
        self.conn.commit()

    def migrate_default_location(self):
        """Clarify default location collection names/rules across workspaces.

        - ``Needs Location`` was the default collection for photos that already
          have EXIF GPS but lack a structured Vireo location keyword. Rename
          exact default instances to the more literal
          ``GPS Without Location Keyword``.
        - Some workspaces had a hand-built ``No Location`` collection using the
          inverse of that rule. That actually meant "not GPS-without-keyword",
          not "has no location". For that exact legacy rule, replace it with a
          true ``No Location Information`` collection.
        """
        updated = 0
        gps_rules = [
            self.GPS_WITHOUT_LOCATION_KEYWORD_RULES,
            {
                "mode": "all",
                "rules": self.GPS_WITHOUT_LOCATION_KEYWORD_RULES,
            },
        ]
        no_location_inverse_rules = [
            [{"field": "location_keyword_missing", "op": "equals", "value": 0}],
            {
                "mode": "all",
                "rules": [
                    {"field": "location_keyword_missing", "op": "equals", "value": 0},
                ],
            },
        ]

        rows = self.conn.execute(
            "SELECT id, workspace_id, name, rules FROM collections "
            "WHERE name IN ('Needs Location', 'No Location')"
        ).fetchall()
        for row in rows:
            try:
                current = json.loads(row["rules"])
            except (TypeError, ValueError):
                continue

            if row["name"] == "Needs Location" and current in gps_rules:
                self.conn.execute(
                    "UPDATE collections SET name = ?, rules = ? WHERE id = ?",
                    (
                        "GPS Without Location Keyword",
                        json.dumps(self.GPS_WITHOUT_LOCATION_KEYWORD_RULES),
                        row["id"],
                    ),
                )
                updated += 1
                continue

            if row["name"] == "No Location" and current in no_location_inverse_rules:
                self.conn.execute(
                    "UPDATE collections SET name = ?, rules = ? WHERE id = ?",
                    (
                        "No Location Information",
                        json.dumps(self.NO_LOCATION_INFORMATION_RULES),
                        row["id"],
                    ),
                )
                updated += 1

        if updated:
            self.conn.commit()
        return updated

    def migrate_default_subject(self):
        """Rename legacy 'Needs Classification' (with rule has_species==0)
        to 'Needs Identification' (rule has_subject==0) across ALL workspaces.

        Workspace activation does not re-run startup migrations, so an
        upgraded multi-workspace database would otherwise leave non-active
        workspaces stuck on the legacy rule. Skips collections the user has
        customized. Idempotent."""
        rows = self.conn.execute(
            "SELECT id, workspace_id, rules FROM collections WHERE name = ?",
            ("Needs Classification",),
        ).fetchall()
        legacy_rule = [{"field": "has_species", "op": "equals", "value": 0}]
        for row in rows:
            try:
                current = json.loads(row["rules"])
            except (TypeError, ValueError):
                continue
            if current != legacy_rule:
                continue
            # Don't clobber an existing "Needs Identification" in the SAME
            # workspace (each workspace has its own default collections).
            existing = self.conn.execute(
                "SELECT 1 FROM collections WHERE workspace_id = ? AND name = ?",
                (row["workspace_id"], "Needs Identification"),
            ).fetchone()
            if existing:
                continue
            self.conn.execute(
                "UPDATE collections SET name = ?, rules = ? WHERE id = ?",
                (
                    "Needs Identification",
                    json.dumps(self.NEEDS_IDENTIFICATION_RULES),
                    row["id"],
                ),
            )
        self.conn.commit()

    def migrate_default_needs_identification(self):
        """Upgrade the default Needs Identification rule to skip Not Wildlife.

        User-customized collections are left alone; only the exact previous
        default ``has_subject == 0`` rule is rewritten.
        """
        old_rule = [{"field": "has_subject", "op": "equals", "value": 0}]
        rows = self.conn.execute(
            "SELECT id, rules FROM collections WHERE name = ?",
            ("Needs Identification",),
        ).fetchall()
        updated = 0
        for row in rows:
            try:
                current = json.loads(row["rules"])
            except (TypeError, ValueError):
                continue
            if current != old_rule:
                continue
            self.conn.execute(
                "UPDATE collections SET rules = ? WHERE id = ?",
                (json.dumps(self.NEEDS_IDENTIFICATION_RULES), row["id"]),
            )
            updated += 1
        if updated:
            self.conn.commit()
        return updated


_SQLITE_NUMERIC_TEXT_RE = re.compile(
    r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$",
    re.ASCII,
)


def _photo_id_key(value):
    """The integer photo id a ``photo_ids`` rule value names, or None.

    The rules engine matches ints inline and binds anything else, where
    SQLite's integer affinity on ``p.id`` still matches a numeric string, an
    integral float, a string spelling of one (e.g. ``"1.0"`` or ``"1e3"``),
    or a Python bool (SQLite treats ``True``/``False`` as 1/0), so every such
    spelling names the same photo. The rule validator permits those spellings
    (``_is_scalar`` accepts bool alongside int/float/str), so a remap that
    missed them would leave a stale entry for the deleted id and silently
    rejoin the next photo that reuses it.

    Python's numeric grammar is broader than SQLite's — ``str.isdigit`` and
    ``int`` accept Unicode digits (``int("٢") == 2``), and ``float`` accepts
    PEP 515 underscores (``float("1_0") == 10.0``). Neither reaches SQLite's
    numeric affinity, so binding ``"٢"`` or ``"1_0"`` stays TEXT and never
    matches an integer id. Only accept strings SQLite would convert with
    numeric affinity so we do not rewrite an unrelated id.
    """
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if math.isfinite(value) and value.is_integer():
            return int(value)
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text or not _SQLITE_NUMERIC_TEXT_RE.match(text):
            return None
        try:
            as_float = float(text)
        except ValueError:
            return None
        if math.isfinite(as_float) and as_float.is_integer():
            return int(as_float)
    return None


def remap_collection_photo_ids(conn, mapping):
    """Rewrite ``photo_ids`` rules in every workspace's collections.

    ``mapping`` maps a photo id that is leaving the catalog to the id that
    absorbed it, or to None when the photo is simply gone. Photos are global,
    so a static collection in any workspace can name the id, and SQLite reuses
    a freed ``photos.id``: a stale entry would make the next imported photo
    join that collection. A remapped id that the rule already lists is not
    repeated. Runs inside the caller's transaction and never commits. Returns
    the number of collections rewritten.
    """
    targets = {}
    for old, new in mapping.items():
        key = _photo_id_key(old)
        if key is not None:
            targets[key] = new
    if not targets:
        return 0
    # Follow chains (A absorbed into B, then B into C) to the final survivor,
    # so a caller can collect one mapping across a multi-step merge.
    for key in list(targets):
        seen_chain = {key}
        new = targets[key]
        while _photo_id_key(new) in targets and _photo_id_key(new) not in seen_chain:
            seen_chain.add(_photo_id_key(new))
            new = targets[_photo_id_key(new)]
        targets[key] = new
    # Only an id a remap introduces can create a duplicate this pass should
    # fold; duplicates a rule already held are left as the user saved them.
    survivors = {
        k for k in (_photo_id_key(v) for v in targets.values()) if k is not None
    }

    def rewrite(node):
        if isinstance(node, list):
            changed_any = False
            for child in node:
                changed_any = rewrite(child) or changed_any
            return changed_any
        if not isinstance(node, dict):
            return False
        changed_any = rewrite(node.get("rules"))
        if node.get("field") != "photo_ids":
            return changed_any
        values = node.get("value")
        if not isinstance(values, list):
            return changed_any
        out = []
        seen = set()
        changed = False
        for v in values:
            key = _photo_id_key(v)
            if key in targets:
                changed = True
                v = targets[key]
                if v is None:
                    continue
                key = _photo_id_key(v)
            if key in survivors:
                if key in seen:
                    changed = True
                    continue
                seen.add(key)
            out.append(v)
        if changed:
            node["value"] = out
        return changed_any or changed

    rewritten = 0
    rows = conn.execute(
        "SELECT id, rules FROM collections WHERE rules LIKE '%photo_ids%'"
    ).fetchall()
    for row in rows:
        try:
            rules = json.loads(row["rules"])
        except (TypeError, ValueError):
            continue
        if rewrite(rules):
            conn.execute(
                "UPDATE collections SET rules = ? WHERE id = ?",
                (json.dumps(rules), row["id"]),
            )
            rewritten += 1
    return rewritten
