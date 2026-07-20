"""Field registry for the universal photo filter system.

Single source of truth for the filter fields the UI offers: labels,
categories, value types, supported operators, enum vocabularies, and
whether a field supports value typeahead (``suggest``). Served verbatim by
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
PREDICTION_STATUS_VALUES = ["pending", "accepted", "rejected"]


def _field(label, category, type_, ops, **extra):
    spec = {"label": label, "category": category, "type": type_, "ops": list(ops)}
    spec.update(extra)
    return spec


FILTER_FIELDS = {
    # File
    "filename": _field("Filename", "File", "text", TEXT_OPS, case_toggle=True),
    "folder": _field("Folder", "File", "folder", ["under", "not_under"], suggest=True),
    "extension": _field("File extension", "File", "enum", ENUM_OPS, suggest=True),
    "file_size": _field("File size (bytes)", "File", "number", NUMBER_OPS),
    "width": _field("Width (px)", "File", "number", NUMBER_OPS),
    "height": _field("Height (px)", "File", "number", NUMBER_OPS),
    "timestamp": _field("Capture date", "File", "date", DATE_OPS),
    # Organization
    "rating": _field("Rating", "Organization", "rating",
                     [">=", "<=", "is", "is not", "between"]),
    "flag": _field("Flag", "Organization", "enum", ENUM_OPS,
                   values=FLAG_VALUES, labels=FLAG_LABELS),
    "color_label": _field("Color label", "Organization", "enum", ENUM_OPS,
                          values=COLOR_VALUES),
    "keyword": _field("Keyword", "Organization", "text",
                      ["contains", "not_contains", "is", "is not"], suggest=True),
    "species": _field("Species", "Organization", "text",
                      ["contains", "not_contains", "is", "is not"], suggest=True),
    "keyword_count": _field("Keyword count", "Organization", "number", NUMBER_OPS),
    # Camera & exposure
    "camera_make": _field("Camera make", "Camera & exposure", "text", TEXT_OPS,
                          suggest=True),
    "camera_model": _field("Camera model", "Camera & exposure", "text", TEXT_OPS,
                           suggest=True),
    "lens": _field("Lens", "Camera & exposure", "text", TEXT_OPS, suggest=True),
    "focal_length": _field("Focal length (mm)", "Camera & exposure", "number",
                           NUMBER_OPS),
    "aperture": _field("Aperture (f-number)", "Camera & exposure", "number",
                       NUMBER_OPS),
    "shutter_speed": _field("Shutter speed (seconds)", "Camera & exposure",
                            "number", NUMBER_OPS),
    "iso": _field("ISO", "Camera & exposure", "number", NUMBER_OPS),
    # Location
    "has_gps": _field("Has GPS", "Location", "boolean", BOOLEAN_OPS),
    "has_location_keyword": _field("Has named location", "Location", "boolean",
                                   BOOLEAN_OPS),
    "gps_lat": _field("GPS latitude", "Location", "number", NUMBER_OPS),
    "gps_lng": _field("GPS longitude", "Location", "number", NUMBER_OPS),
    # Quality & AI
    "quality_score": _field("Quality score", "Quality & AI", "number", NUMBER_OPS),
    "sharpness": _field("Sharpness", "Quality & AI", "number", NUMBER_OPS),
    "subject_sharpness": _field("Subject sharpness", "Quality & AI", "number",
                                NUMBER_OPS),
    "noise_estimate": _field("Noise estimate", "Quality & AI", "number",
                             NUMBER_OPS),
    "prediction_confidence": _field("Prediction confidence", "Quality & AI",
                                    "number", [">=", "<=", ">", "<", "between"]),
    "prediction_status": _field("Prediction status", "Quality & AI", "enum",
                                ENUM_OPS, values=PREDICTION_STATUS_VALUES),
    "classifier_model": _field("Classifier model", "Quality & AI", "text",
                               ["contains", "is", "is not"]),
    # Workflow
    "has_edits": _field("Has edits", "Workflow", "boolean", BOOLEAN_OPS),
    "has_visual_index": _field("Has visual index", "Workflow", "boolean",
                               BOOLEAN_OPS),
    "in_burst": _field("In a burst", "Workflow", "boolean", BOOLEAN_OPS),
    "burst_id": _field("Burst ID", "Workflow", "text", ["is", "is not"]),
    "duplicate_group": _field("Duplicate group", "Workflow", "text",
                              ["is", "is not"]),
    "is_duplicate": _field("Has duplicates", "Workflow", "boolean", BOOLEAN_OPS),
    "wildlife_excluded": _field("Excluded from wildlife", "Workflow", "boolean",
                                BOOLEAN_OPS),
    "has_species": _field("Has species", "Workflow", "boolean", BOOLEAN_OPS),
    "has_subject": _field("Has subject", "Workflow", "boolean", BOOLEAN_OPS),
}

# Fields whose distinct values (with counts) /api/filters/values can serve.
SUGGEST_FIELDS = frozenset(
    key for key, spec in FILTER_FIELDS.items() if spec.get("suggest")
)


def fields_for_api():
    """Registry as a JSON-ready list, insertion-ordered for the UI picker."""
    return [{"key": key, **spec} for key, spec in FILTER_FIELDS.items()]
