"""Row-level evaluation of prediction-field rules for ``PredictionRepository.get_rows``.

``CollectionRepository.build_query_from_rules`` scopes the *photos* whose
predictions surface; these helpers make the per-row half agree with it:
``relax_negated_prediction_leaves`` strips the prediction-field leaves a
photo-level ``NOT EXISTS`` would over-restrict on, and
``filter_prediction_rows_by_rules`` re-evaluates the original tree per row.
``DECIDED_PREDICTION_STATUSES`` is the one definition of a settled review.
"""

import sqlite3

# The one definition of "this prediction's review is already settled".
# ``app.py`` binds ``_DECIDED_PREDICTION_STATUSES`` to it, the grouped
# accept expansion below uses it, and Browse's panel mirrors it in JS, so
# no surface can drift on which rows are still actionable. See
# ``app.py``'s ``_decided_prediction_ids`` for why ``alternative`` is
# absent and what each endpoint then does with those rows.
DECIDED_PREDICTION_STATUSES = ("accepted", "rejected", "reviewed")

# Row-level filterable fields: those the row's own values can prove.
# ``species`` is intentionally excluded — the SQL ``species`` leaf
# (``build_query_from_rules``) matches confirmed photo keywords via
# ``photo_keywords → keywords → taxa``, whereas ``pr.species`` on the
# returned row holds the model's *proposed* species. Re-checking the
# proposed species against a keyword filter would silently hide the
# exact disagreement/refinement rows users would filter for
# (e.g. a photo tagged ``Robin`` with a pending ``Sparrow`` prediction
# would be selected by SQL and then dropped here).
PREDICTION_ROW_FIELDS = frozenset({
    "prediction_confidence",
    "prediction_status",
    "classifier_model",
    "taxonomy_kingdom",
    "taxonomy_phylum",
    "taxonomy_class",
    "taxonomy_order",
    "taxonomy_family",
    "taxonomy_genus",
    "needs_review",
})

# Sentinel returned by ``relax_negated_prediction_leaves`` when a
# branch broadens to TRUE at the photo level. Identity-compared with
# ``is``; never stored in a rules tree that reaches SQL.
RELAX_BROAD = object()


def _contains_prediction_field(node, prediction_fields):
    """Whether a rule node is, or (transitively) holds, a prediction leaf."""
    if not isinstance(node, dict):
        return False
    if "rules" in node and "field" not in node:
        return any(
            _contains_prediction_field(c, prediction_fields)
            for c in node.get("rules") or []
        )
    return node.get("field") in prediction_fields


def _relax_negated_walk(node, negated, prediction_fields, broad):
    """``relax_negated_prediction_leaves``'s recursive pass over one node."""
    if isinstance(node, dict) and "rules" in node and "field" not in node:
        mode = node.get("mode", "all")
        child_negated = negated or (mode == "none")
        new_children = []
        had_broad = False
        for child in node.get("rules") or []:
            is_group = (
                isinstance(child, dict)
                and "rules" in child
                and "field" not in child
            )
            # Under negation, a subgroup that mixes metadata with
            # prediction leaves would be compiled by the outer
            # ``NOT EXISTS(...)`` in a way that lets its metadata
            # siblings prune photos before the row filter sees them
            # (see ``Database._relax_negated_prediction_leaves``). Drop the whole subgroup so
            # SQL broadens; the row filter re-evaluates the
            # ORIGINAL tree per row.
            if (
                child_negated
                and is_group
                and _contains_prediction_field(child, prediction_fields)
            ):
                had_broad = True
                continue
            stripped = _relax_negated_walk(
                child, child_negated, prediction_fields, broad,
            )
            if stripped is None:
                # Bare row-level leaf dropped under negation:
                # SQL for that leaf broadens to TRUE.
                had_broad = True
                continue
            if stripped is broad:
                had_broad = True
                continue
            new_children.append(stripped)
        # ``any`` with any broad child is TRUE at the photo
        # level — the whole OR broadens. ``all``/``none`` drop
        # broad children silently (they don't restrict) unless
        # every child broadened, in which case the group itself
        # is broad and must propagate.
        if mode == "any" and had_broad:
            return broad
        if had_broad and not new_children:
            return broad
        new_node = dict(node)
        new_node["rules"] = new_children
        return new_node
    if negated and isinstance(node, dict) and \
            node.get("field") in prediction_fields:
        return None
    return node


def _rule_truthy(value):
    """Whether a rule value reads as boolean true."""
    return value is True or value == 1 or value == "1" or value == "true"


def _rule_num_match(row_val, op, want):
    """A numeric rule leaf evaluated against one row value."""
    if row_val is None:
        return False
    try:
        a = float(row_val)
    except (TypeError, ValueError):
        return False
    if op == "between":
        if not isinstance(want, list) or len(want) != 2:
            return False
        try:
            lo, hi = float(want[0]), float(want[1])
        except (TypeError, ValueError):
            return False
        return lo <= a <= hi
    try:
        b = float(want)
    except (TypeError, ValueError):
        return False
    if op == ">=":
        return a >= b
    if op == "<=":
        return a <= b
    if op == ">":
        return a > b
    if op == "<":
        return a < b
    if op in ("equals", "is"):
        return a == b
    if op == "is not":
        return a != b
    return False


def _rule_text_match(row_val, op, want):
    """A case-insensitive text rule leaf evaluated against one row value."""
    a = "" if row_val is None else str(row_val)
    b = "" if want is None else str(want)
    if op == "contains":
        return b.lower() in a.lower()
    if op == "not_contains":
        return b.lower() not in a.lower()
    if op == "starts_with":
        return a.lower().startswith(b.lower())
    if op == "ends_with":
        return a.lower().endswith(b.lower())
    if op in ("equals", "is"):
        return a.lower() == b.lower()
    if op == "is not":
        return a.lower() != b.lower()
    return False


def _rule_enum_match(row_val, op, want):
    """An enum rule leaf evaluated against one row value."""
    a = "" if row_val is None else str(row_val)
    if op in ("in", "not_in"):
        values = [str(v) for v in (want or [])]
        inside = a in values
        return inside if op == "in" else not inside
    if op in ("equals", "is"):
        return a == str(want)
    if op == "is not":
        return a != str(want)
    return False


def _prediction_leaf_row_match(rule, row, leaf_photo_sets):
    """One rule leaf evaluated against one prediction row.

    ``leaf_photo_sets`` maps ``id(leaf)`` to the photo ids a metadata leaf
    inside an ``any``/``none`` group matches (``None``: match every row).
    """
    field = rule.get("field")
    op = rule.get("op", "")
    value = rule.get("value")
    if field == "prediction_confidence":
        return _rule_num_match(row["confidence"], op, value)
    if field == "prediction_status":
        return _rule_enum_match(row["status"], op, value)
    if field == "classifier_model":
        return _rule_text_match(row["classifier_model"], op, value)
    if field in (
        "taxonomy_kingdom",
        "taxonomy_phylum",
        "taxonomy_class",
        "taxonomy_order",
        "taxonomy_family",
        "taxonomy_genus",
    ):
        col = row[field] if field in row.keys() else None  # noqa: SIM118 -- sqlite3.Row supports `in` on keys() but not on the row itself
        if op == "contains":
            return _rule_text_match(col, "contains", value)
        return _rule_text_match(col, op, value)
    if field == "needs_review":
        is_pending = str(row["status"] or "pending") == "pending"
        return is_pending if _rule_truthy(value) else not is_pending
    # Non-prediction leaf. In a pure ``all`` position the SQL
    # subquery already proved this photo satisfies it, so shortcut
    # to True. Inside a mixed ``any``/``none`` group we can't
    # shortcut (SQL only proved *some* sibling matched at the
    # photo level, not this leaf), so answer from the pre-resolved
    # per-leaf photo-id set.
    pset = leaf_photo_sets.get(id(rule))
    if pset is None:
        return True
    return row["photo_id"] in pset


def relax_negated_prediction_leaves(rules):
    """Strip prediction-field leaves — and mixed subgroups containing
    them — that sit under a ``none`` group so
    ``get_predictions``'s photo-scoping subquery doesn't
    over-restrict on row-level predicates.

    ``build_query_from_rules`` compiles a ``none`` group as
    photo-level ``NOT EXISTS(...)``. For a bare leaf like
    ``prediction_confidence >= 0.8`` that removes any photo with a
    sibling row above the threshold, even though the 0.10 sibling
    satisfies ``none(prediction_confidence >= 0.8)`` per row —
    dropping the leaf broadens the SQL to a no-op there and lets
    ``filter_prediction_rows_by_rules`` evaluate the ORIGINAL
    tree per row.

    A mixed subgroup like ``none(all(rating >= 5, conf >= 0.8))``
    is trickier: stripping only the prediction leaf leaves
    ``none(all(rating >= 5))`` = ``NOT EXISTS(rating >= 5)``,
    which excludes every rating-5 photo — including one whose only
    high-conf row is 0.10 and should PASS the outer expression at
    the row level (see r3618935666). Whenever a negated subgroup
    (transitively) contains a prediction row field we therefore
    drop the whole subgroup so SQL becomes a no-op there; the
    row-level pass then evaluates the original expression per row
    against the actual metadata + prediction values.

    Leaves and subgroups in ``all``/``any`` positions still pull
    their weight in the SQL — an ``all`` narrows and an ``any``
    widens correctly at the photo level.

    A negated branch that ends up fully emptied by this pass
    represents "broaden to TRUE at photo level" (the row filter
    will decide per row). Under an ``any`` parent that TRUE
    satisfies the OR for every photo, so we propagate a broad
    marker up: an ``any`` group with any broad child becomes broad
    itself, and a top-level broad marker resolves to empty rules
    (no photo-level restriction). Without this, ``any(none(
    prediction_confidence >= 0.8), rating >= 5)`` compiled as just
    ``rating >= 5``, hiding rating-3 photos whose low-confidence
    rows satisfy the outer OR at the row level (see r3619014565).
    """
    broad = RELAX_BROAD

    fields = PREDICTION_ROW_FIELDS

    if isinstance(rules, list):
        wrapped = _relax_negated_walk(
            {"mode": "all", "rules": rules}, False, fields, broad,
        )
        if wrapped is broad or wrapped is None:
            return []
        return wrapped.get("rules", [])
    walked = _relax_negated_walk(rules, False, fields, broad)
    if walked is broad or walked is None:
        return {"mode": "all", "rules": []}
    return walked


def filter_prediction_rows_by_rules(rows, rules, collections):
    """Drop returned prediction rows that don't satisfy prediction-field
    rules at the row level.

    The SQL rules subquery limits the set of *photos* whose predictions
    surface, but leaves like ``prediction_confidence >= 0.8`` return
    every current prediction for a photo that has any matching one.
    This walks the rule tree per row: prediction-field leaves are
    evaluated against the row's own values; every other leaf is treated
    as satisfied (the photo already matched via the SQL subquery).

    The satisfied-by-default shortcut is only sound inside pure ``all``
    intersections — SQL ensured every non-prediction leaf was True at
    the photo level. Inside an ``any`` or ``none`` group the shortcut
    would incorrectly contribute True for a leaf that may have been
    False for this photo (e.g. ``(rating >= 5 OR prediction_confidence
    >= 0.8)`` — a rating-3 photo with one 0.95 and one 0.10 prediction:
    SQL keeps both rows because of the 0.95, but the 0.10 row must not
    be shown and shortcutting ``rating >= 5`` to True would let it
    through). For non-prediction leaves nested inside a mixed ``any``/
    ``none`` group we therefore resolve the leaf to its actual matching
    photo ids and answer per-row from that set, so prediction leaves in
    the same group still filter and the metadata leaf's truth is real.
    ``collections`` is the ``CollectionRepository`` that resolves those
    leaves.
    """
    if not rows:
        return rows
    # Normalize to the same shape ``build_query_from_rules`` accepts so
    # both list and grouped-tree inputs go through the same walker.
    if isinstance(rules, list):
        root = {"mode": "all", "rules": rules}
    elif isinstance(rules, dict) and "rules" in rules:
        root = rules
    else:
        return rows

    def _touches_prediction(node):
        if not isinstance(node, dict):
            return False
        if "rules" in node and "field" not in node:
            return any(_touches_prediction(c) for c in node.get("rules") or [])
        return node.get("field") in PREDICTION_ROW_FIELDS

    if not _touches_prediction(root):
        return rows

    # Resolve non-prediction leaves nested inside ``any``/``none`` groups
    # to the photo-id set they actually match. Anywhere else the outer
    # SQL subquery already proved the leaf holds for the photo, so we
    # keep the True-shortcut. ``id(node)`` is stable across this call —
    # the dict identity is what we look up in ``_prediction_leaf_row_match``.
    leaf_photo_sets = {}

    def _collect_alt_metadata_leaves(node, inside_alt=False):
        if not isinstance(node, dict):
            return
        if "rules" in node and "field" not in node:
            mode = node.get("mode", "all")
            child_alt = inside_alt or mode in ("any", "none")
            for c in node.get("rules") or []:
                _collect_alt_metadata_leaves(c, child_alt)
            return
        if inside_alt and node.get("field") not in PREDICTION_ROW_FIELDS:
            key = id(node)
            if key in leaf_photo_sets:
                return
            try:
                leaf_photo_sets[key] = set(collections.query_photo_ids([node]))
            except (ValueError, sqlite3.Error):
                # Malformed / unresolvable leaf: fall back to matching
                # every photo the SQL subquery kept. Never hide rows
                # the outer filter should have allowed through.
                leaf_photo_sets[key] = None

    _collect_alt_metadata_leaves(root)

    def _eval(node, row):
        if "rules" in node and "field" not in node:
            mode = node.get("mode", "all")
            children = node.get("rules") or []
            if not children:
                # Empty group: match SQL (all=True, any=False, none=True).
                return mode != "any"
            if mode == "any":
                return any(_eval(c, row) for c in children)
            if mode == "none":
                return not any(_eval(c, row) for c in children)
            return all(_eval(c, row) for c in children)
        return _prediction_leaf_row_match(node, row, leaf_photo_sets)

    return [r for r in rows if _eval(root, r)]
