"""Field registry for the universal photo filter system.

Single source of truth for the filter fields the UI offers: labels,
categories, value types, supported operators, enum vocabularies, whether a
field supports value typeahead (``suggest``), and which in-grid mutations
can move the field's value (``changed_by``). Served verbatim by
``GET /api/filters/fields`` so the client picker and the server rule
engine (``Database._build_query_from_rules``) cannot drift apart — a test
asserts every field/op combination listed here compiles to SQL.

Design: docs/plans/2026-07-19-universal-filters-design.md.
"""

TEXT_OPS = ["contains", "not_contains", "is", "is not", "starts_with", "ends_with"]
NUMBER_OPS = [">=", "<=", ">", "<", "is", "is not", "between"]
DATE_OPS = ["recent", ">=", "<=", ">", "<", "between"]
ENUM_OPS = ["in", "not_in", "is", "is not"]
BOOLEAN_OPS = ["is"]

FLAG_VALUES = ["flagged", "none", "rejected"]
FLAG_LABELS = {"flagged": "Picked", "none": "Unflagged", "rejected": "Rejected"}
COLOR_VALUES = ["red", "yellow", "green", "blue", "purple"]
# ``reviewed`` is a distinct persisted status the review UI sets via
# ``/api/predictions/<id>/reviewed`` (``update_prediction_status(id,
# "reviewed")``) — a prediction the user has looked at without accepting or
# rejecting. The rule engine's ``prediction_status`` branch compares against
# ``COALESCE(prv.status, 'pending')`` so this string filters as-is; omit it
# and the universal-filter UI silently loses the ability to build a
# "reviewed-but-not-accepted/rejected" rule even though the underlying data
# supports it.
PREDICTION_STATUS_VALUES = ["pending", "accepted", "rejected", "reviewed"]


# Mutations a user can make without leaving the photo grid. Every field
# declares which of them can move its value (``changed_by``) so a page can
# tell a reload it owes the user ("Species is Osprey" after a prediction is
# accepted) from one that only costs them their place in the grid ("Rating
# >= 3" after a keyword is added). ``_field`` requires the argument, so a
# field added later cannot silently default to "nothing changes this".
MUTATION_KEYWORD = "keyword"          # tag/untag, species and location included
MUTATION_PREDICTION = "prediction"    # accept / reject / mark reviewed
MUTATION_WILDLIFE = "wildlife_excluded"
MUTATIONS = (MUTATION_KEYWORD, MUTATION_PREDICTION, MUTATION_WILDLIFE)

# Fields whose value only a keyword edit can move. Accepting a prediction
# writes a species keyword too, but the accept path already fans out
# ``MUTATION_KEYWORD`` alongside ``MUTATION_PREDICTION``
# (``_afterPredictionMutation`` in ``vireo/templates/browse.html``), so
# listing ``MUTATION_PREDICTION`` here would only add false positives on
# the status-only prediction paths (reject, mark reviewed) that touch no
# keywords, forcing an unnecessary reset of every keyword-filtered grid
# (Codex review r4013123596).
_KEYWORD_DERIVED = [MUTATION_KEYWORD]


def _field(label, category, type_, ops, *, changed_by, **extra):
    unknown = [m for m in changed_by if m not in MUTATIONS]
    if unknown:
        raise ValueError(f"{label}: unknown mutation(s) {unknown}")
    spec = {"label": label, "category": category, "type": type_, "ops": list(ops),
            "changed_by": list(changed_by)}
    spec.update(extra)
    return spec


FILTER_FIELDS = {
    "metadata": _field("All metadata", "File", "text", ["contains", "not_contains"],
                       changed_by=[MUTATION_KEYWORD, MUTATION_PREDICTION]),
    # File
    "filename": _field("Filename", "File", "text", TEXT_OPS, case_toggle=True,
                       changed_by=[]),
    "folder": _field("Folder", "File", "folder", ["under", "not_under"],
                     suggest=True, changed_by=[]),
    "extension": _field("File extension", "File", "enum", ENUM_OPS,
                        suggest=True, changed_by=[]),
    "file_size": _field("File size (bytes)", "File", "number", NUMBER_OPS,
                        changed_by=[]),
    "width": _field("Width (px)", "File", "number", NUMBER_OPS, changed_by=[]),
    "height": _field("Height (px)", "File", "number", NUMBER_OPS, changed_by=[]),
    "timestamp": _field("Capture date", "File", "date", DATE_OPS, changed_by=[]),
    # Organization
    "rating": _field("Rating", "Organization", "rating",
                     [">=", "<=", "is", "is not", "between"], changed_by=[]),
    "flag": _field("Flag", "Organization", "enum", ENUM_OPS,
                   values=FLAG_VALUES, labels=FLAG_LABELS, changed_by=[]),
    "color_label": _field("Color label", "Organization", "enum", ENUM_OPS,
                          values=COLOR_VALUES, changed_by=[]),
    "keyword": _field("Keyword", "Organization", "text",
                      ["contains", "not_contains", "is", "is not"],
                      suggest=True, changed_by=_KEYWORD_DERIVED),
    # Internal deep-link predicate used by Life List "View photos" actions.
    # ``pages=[]`` keeps it out of the normal field picker while still giving
    # an incoming Browse rule a readable label and a stable registry entry.
    "life_list_uncounted": _field(
        "Uncounted identification", "Organization", "text", ["is"], pages=[],
        changed_by=_KEYWORD_DERIVED,
    ),
    "species": _field("Species", "Organization", "text",
                      ["contains", "not_contains", "is", "is not"],
                      suggest=True, changed_by=_KEYWORD_DERIVED),
    "keyword_count": _field("Keyword count", "Organization", "number",
                            NUMBER_OPS, changed_by=_KEYWORD_DERIVED),
    # Distinct species on the photo, counted the way the rest of the app
    # counts them (by taxon, collapsing a hierarchy leaf onto its root) —
    # ``Species count >= 2`` is the multi-species filter. ``keyword_count``
    # is not a substitute: location and subject keywords inflate it.
    "species_count": _field("Species count", "Organization", "number",
                            NUMBER_OPS, changed_by=_KEYWORD_DERIVED),
    # Camera & exposure
    "camera_make": _field("Camera make", "Camera & exposure", "text", TEXT_OPS,
                          suggest=True, changed_by=[]),
    "camera_model": _field("Camera model", "Camera & exposure", "text", TEXT_OPS,
                           suggest=True, changed_by=[]),
    "lens": _field("Lens", "Camera & exposure", "text", TEXT_OPS,
                   suggest=True, changed_by=[]),
    "focal_length": _field("Focal length (mm)", "Camera & exposure", "number",
                           NUMBER_OPS, changed_by=[]),
    "aperture": _field("Aperture (f-number)", "Camera & exposure", "number",
                       NUMBER_OPS, changed_by=[]),
    "shutter_speed": _field("Shutter speed (seconds)", "Camera & exposure",
                            "number", NUMBER_OPS, changed_by=[]),
    "iso": _field("ISO", "Camera & exposure", "number", NUMBER_OPS,
                  changed_by=[]),
    # Location
    "has_gps": _field("Has GPS", "Location", "boolean", BOOLEAN_OPS,
                      changed_by=[]),
    "has_location_keyword": _field("Has named location", "Location", "boolean",
                                   BOOLEAN_OPS, changed_by=[MUTATION_KEYWORD]),
    # Distinct from ``has_location_keyword``: a free-text location keyword
    # (no lat/lng) counts as "has named location" but cannot place the photo
    # on the map. Legacy Browse ``?location_status=assigned``/``none`` deep
    # links depend on this coordinate-bearing check to round-trip correctly.
    "has_coord_location_keyword": _field(
        "Has location keyword with coordinates", "Location", "boolean",
        BOOLEAN_OPS, changed_by=[MUTATION_KEYWORD],
    ),
    "gps_lat": _field("GPS latitude", "Location", "number", NUMBER_OPS,
                      changed_by=[]),
    "gps_lng": _field("GPS longitude", "Location", "number", NUMBER_OPS,
                      changed_by=[]),
    # Quality & AI
    "quality_score": _field("Quality score", "Quality & AI", "number",
                            NUMBER_OPS, changed_by=[]),
    "sharpness": _field("Sharpness", "Quality & AI", "number", NUMBER_OPS,
                        changed_by=[]),
    "subject_sharpness": _field("Subject sharpness", "Quality & AI", "number",
                                NUMBER_OPS, changed_by=[]),
    "noise_estimate": _field("Noise estimate", "Quality & AI", "number",
                             NUMBER_OPS, changed_by=[]),
    # Confidence is written at classify time and never mutates under
    # accept/reject/mark-reviewed, but the *set* of prediction rows a
    # confidence filter sees can still move with a review-status edit.
    # ``_build_query_from_rules`` routes every prediction filter through
    # ``_prediction_exists``, which drops rows with
    # ``prv.status = 'alternative'`` so a top pick at 0.95 with a runner-up
    # at 0.10 does not satisfy ``prediction_confidence <= 0.2``. Accepting
    # or rejecting a prediction flips its sibling alternatives to
    # ``rejected`` (``Database.accept_prediction`` and
    # ``_batch_reject_under_lock`` in ``app.py``), pulling those low-
    # confidence runners-up out of the alternative filter and into the
    # rule's row set — so a photo can newly satisfy the confidence rule
    # even though its numeric confidence never changed. Listing
    # ``MUTATION_PREDICTION`` here keeps the confidence-filtered grid in
    # sync with that visibility flip (Codex review r4013497441, revising
    # r4013378150). ``classifier_model`` stays empty because sibling
    # alternatives share the same classifier as their top pick — the
    # ``EXISTS`` predicate is already satisfied by the visible top row and
    # cannot change from ``true`` to ``false`` (or the reverse) when a
    # runner-up joins.
    "prediction_confidence": _field("Prediction confidence", "Quality & AI",
                                    "number", [">=", "<=", ">", "<", "between"],
                                    pages=["review"],
                                    changed_by=[MUTATION_PREDICTION]),
    "prediction_status": _field("Prediction status", "Quality & AI", "enum",
                                ENUM_OPS, values=PREDICTION_STATUS_VALUES,
                                pages=["review"], changed_by=[MUTATION_PREDICTION]),
    "classifier_model": _field("Classifier model", "Quality & AI", "text",
                               ["contains", "is", "is not"], pages=["review"],
                               changed_by=[]),
    # Workflow
    "has_edits": _field("Has edits", "Workflow", "boolean", BOOLEAN_OPS,
                        changed_by=[]),
    "has_visual_index": _field("Has visual index", "Workflow", "boolean",
                               BOOLEAN_OPS, changed_by=[]),
    "in_burst": _field("In a burst", "Workflow", "boolean", BOOLEAN_OPS,
                       changed_by=[]),
    "burst_id": _field("Burst ID", "Workflow", "text", ["is", "is not"],
                       changed_by=[]),
    "duplicate_group": _field("Duplicate group", "Workflow", "text",
                              ["is", "is not"], changed_by=[]),
    "is_duplicate": _field("Has duplicates", "Workflow", "boolean", BOOLEAN_OPS,
                           changed_by=[]),
    "wildlife_excluded": _field("Excluded from wildlife", "Workflow", "boolean",
                                BOOLEAN_OPS, changed_by=[MUTATION_WILDLIFE]),
    "has_species": _field("Has species", "Workflow", "boolean", BOOLEAN_OPS,
                          changed_by=_KEYWORD_DERIVED),
    "has_subject": _field("Has subject", "Workflow", "boolean", BOOLEAN_OPS,
                          changed_by=[MUTATION_KEYWORD]),
}

# Fields whose distinct values (with counts) /api/filters/values can serve.
SUGGEST_FIELDS = frozenset(
    key for key, spec in FILTER_FIELDS.items() if spec.get("suggest")
)


def fields_for_api():
    """Registry as a JSON-ready list, insertion-ordered for the UI picker."""
    return [{"key": key, **spec} for key, spec in FILTER_FIELDS.items()]
