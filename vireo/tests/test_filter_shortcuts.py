"""Quick-filter (filter-bar shortcut) configuration: shape, validation, API."""

import filter_shortcuts as fs
import pytest


def _by_id(entries):
    return {entry["id"]: entry for entry in entries}


def test_defaults_reproduce_the_bar_the_app_shipped_with():
    """An untouched install renders the original five buttons, in order."""
    entries = fs.normalize(None)
    assert [e["id"] for e in entries] == [
        "missing_species", "missing_location",
        "flag_picked", "flag_unflagged", "flag_rejected",
    ]
    assert [e["label"] for e in entries][:2] == [
        "Missing species", "Missing location tag",
    ]


def test_kind_is_derived_from_the_rule_shape():
    entries = _by_id(fs.normalize(None))
    assert entries["missing_species"]["kind"] == "missing"
    assert entries["missing_species"]["field"] == "has_species"
    assert entries["flag_picked"]["kind"] == "enum"
    assert entries["flag_picked"]["value"] == "flagged"
    grouped_rule = fs.normalize([{
        "id": "x", "label": "Either",
        "rules": {"mode": "any", "rules": [
            {"field": "rating", "op": ">=", "value": 4},
            {"field": "flag", "op": "is", "value": "flagged"},
        ]},
    }])
    assert grouped_rule[0]["kind"] == "rules"
    assert grouped_rule[0]["field"] is None


def test_enum_rule_with_a_list_value_is_not_an_enum_shortcut():
    """``in`` clauses toggle as a whole clause; only a single value merges."""
    entries = fs.normalize([{
        "id": "x", "label": "Picked or rejected",
        "rules": {"field": "flag", "op": "in", "value": ["flagged", "rejected"]},
    }])
    assert entries[0]["kind"] == "rules"


def test_groups_split_into_the_containers_the_bar_renders():
    groups = fs.grouped(fs.normalize(None))
    assert [(g["group"], g["style"]) for g in groups] == [
        ("missing", fs.STYLE_PILLS), ("flag", fs.STYLE_SEGMENTED),
    ]
    assert [s["id"] for s in groups[0]["shortcuts"]] == [
        "missing_species", "missing_location",
    ]


def test_ungrouped_shortcuts_share_one_pill_row():
    """A newly added button joins the pills instead of opening its own box."""
    groups = fs.grouped(fs.normalize([
        {"id": "a", "label": "Keepers", "rules": {"field": "rating", "op": ">=", "value": 4}},
        {"id": "b", "label": "Untagged", "rules": {"field": "has_species", "op": "is", "value": 0}},
    ]))
    assert len(groups) == 1
    assert groups[0]["style"] == fs.STYLE_PILLS
    assert [s["id"] for s in groups[0]["shortcuts"]] == ["a", "b"]


def test_segmented_style_needs_one_field_and_a_group():
    """Two enum values from different fields are pills, not a segment."""
    groups = fs.grouped(fs.normalize([
        {"id": "a", "label": "Picked", "group": "mix",
         "rules": {"field": "flag", "op": "is", "value": "flagged"}},
        {"id": "b", "label": "Red", "group": "mix",
         "rules": {"field": "color_label", "op": "is", "value": "red"}},
    ]))
    assert groups[0]["style"] == fs.STYLE_PILLS


@pytest.mark.parametrize("rules", [
    {"field": "not_a_field", "op": "is", "value": 1},
    {"field": "flag", "op": "contains", "value": "flagged"},
    {"field": "flag", "op": "is", "value": "sideways"},
    {"field": "rating", "op": "is", "value": {"nested": "object"}},
    {"mode": "any", "rules": []},
    {"mode": "sometimes", "rules": [{"field": "rating", "op": "is", "value": 4}]},
    "not even a dict",
    None,
])
def test_unusable_entries_are_dropped_rather_than_rendered(rules):
    """A button that could never match anything must not reach the bar."""
    assert fs.normalize([{"id": "x", "label": "Bad", "rules": rules}]) == []


def test_recent_windows_survive_and_bad_ones_do_not():
    """"In the last N days" is a {n, unit} window, not a scalar value."""
    entries = fs.normalize([{
        "id": "x", "label": "Last 30 days",
        "rules": {"field": "timestamp", "op": "recent", "value": {"n": 30, "unit": "days"}},
    }])
    assert entries[0]["rules"]["value"] == {"n": 30, "unit": "days"}
    for bad in ({"n": 0, "unit": "days"}, {"n": 3, "unit": "fortnights"},
                {"unit": "days"}, 30, "30 days"):
        assert fs.normalize([{"id": "x", "label": "Bad", "rules": {
            "field": "timestamp", "op": "recent", "value": bad}}]) == []


@pytest.mark.parametrize("rules", [
    # The rule compiler rejects a list for a scalar operator...
    {"field": "flag", "op": "is", "value": ["flagged"]},
    {"field": "rating", "op": ">=", "value": [4]},
    # ...a scalar where a list operator needs one...
    {"field": "flag", "op": "in", "value": "flagged"},
    # ...and `between` with anything other than two bounds.
    {"field": "rating", "op": "between", "value": [3]},
    {"field": "rating", "op": "between", "value": [1, 2, 3]},
    # A list whose items are not storable scalars is not silently shortened.
    {"field": "flag", "op": "in", "value": ["flagged", {"nested": 1}]},
])
def test_list_values_must_match_what_the_operator_can_execute(rules):
    """A button that 400s the query when clicked is worse than no button."""
    assert fs.normalize([{"id": "x", "label": "Bad", "rules": rules}]) == []


def test_list_operators_keep_their_usable_values():
    entries = fs.normalize([
        {"id": "a", "label": "Picked or rejected",
         "rules": {"field": "flag", "op": "in", "value": ["flagged", "rejected"]}},
        {"id": "b", "label": "Three to five",
         "rules": {"field": "rating", "op": "between", "value": [3, 5]}},
    ])
    assert [entry["rules"]["value"] for entry in entries] == [
        ["flagged", "rejected"], [3, 5],
    ]


def test_a_group_keeps_only_its_usable_children():
    entries = fs.normalize([{
        "id": "x", "label": "Mixed",
        "rules": {"mode": "any", "rules": [
            {"field": "rating", "op": ">=", "value": 4},
            {"field": "not_a_field", "op": "is", "value": 1},
        ]},
    }])
    assert entries[0]["rules"]["rules"] == [
        {"field": "rating", "op": ">=", "value": 4},
    ]


def test_rule_nesting_is_bounded():
    node = {"field": "rating", "op": ">=", "value": 4}
    for _ in range(fs.MAX_RULE_DEPTH + 1):
        node = {"mode": "all", "rules": [node]}
    assert fs.normalize([{"id": "x", "label": "Deep", "rules": node}]) == []


def test_ids_are_minted_and_deduplicated():
    entries = fs.normalize([
        {"label": "One", "rules": {"field": "rating", "op": ">=", "value": 4}},
        {"id": "dup", "label": "Two", "rules": {"field": "rating", "op": ">=", "value": 3}},
        {"id": "dup", "label": "Three", "rules": {"field": "rating", "op": ">=", "value": 2}},
    ])
    ids = [entry["id"] for entry in entries]
    assert len(set(ids)) == 3
    assert all(ids)


def test_label_falls_back_to_the_rule_and_is_bounded():
    entries = fs.normalize([
        {"id": "a", "rules": {"field": "has_species", "op": "is", "value": 0}},
        {"id": "b", "rules": {"field": "flag", "op": "is", "value": "rejected"}},
        {"id": "c", "label": "L" * 200,
         "rules": {"field": "rating", "op": ">=", "value": 4}},
    ])
    assert entries[0]["label"] == "No species"
    assert entries[1]["label"] == "Rejected"
    assert len(entries[2]["label"]) == fs.MAX_LABEL_LEN


def test_empty_list_means_no_quick_filters_but_a_missing_key_means_defaults():
    assert fs.normalize([]) == []
    assert len(fs.normalize(None)) == len(fs.DEFAULT_SHORTCUTS)
    assert len(fs.normalize("garbage")) == len(fs.DEFAULT_SHORTCUTS)


def test_for_storage_keeps_only_the_stored_shape():
    stored = fs.for_storage(fs.normalize(None))
    assert set(stored[0]) == {"id", "label", "group", "rules"}
    # Round-trips: what we store normalizes back to what we rendered.
    assert fs.normalize(stored) == fs.normalize(None)


def test_boolean_true_is_a_plain_clause_not_a_missing_shortcut():
    entries = fs.normalize([{
        "id": "x", "label": "Has GPS",
        "rules": {"field": "has_gps", "op": "is", "value": 1},
    }])
    assert entries[0]["kind"] == "rules"


# ---- API --------------------------------------------------------------


def test_shortcuts_endpoint_serves_the_bar_and_the_settings_defaults(app_and_db):
    app, _ = app_and_db
    data = app.test_client().get("/api/filters/shortcuts").get_json()
    assert [s["id"] for s in data["shortcuts"]] == [s["id"] for s in data["defaults"]]
    assert [g["style"] for g in data["groups"]] == ["pills", "segmented"]


def test_config_post_validates_and_stores_quick_filters(app_and_db):
    import config as cfg

    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/config", json={"filter_shortcuts": [
        {"id": "keepers", "label": "Keepers",
         "rules": {"field": "rating", "op": ">=", "value": 4}},
        {"id": "junk", "label": "Nonsense",
         "rules": {"field": "no_such_field", "op": "is", "value": 1}},
    ]})
    assert resp.status_code == 200
    stored = cfg.load()["filter_shortcuts"]
    assert stored == [{"id": "keepers", "label": "Keepers", "group": "",
                       "rules": {"field": "rating", "op": ">=", "value": 4}}]
    served = client.get("/api/filters/shortcuts").get_json()["shortcuts"]
    assert [s["label"] for s in served] == ["Keepers"]
    assert served[0]["kind"] == "rules"


def test_config_post_can_clear_every_quick_filter(app_and_db):
    import config as cfg

    app, _ = app_and_db
    client = app.test_client()
    assert client.post("/api/config", json={"filter_shortcuts": []}).status_code == 200
    assert cfg.load()["filter_shortcuts"] == []
    assert client.get("/api/filters/shortcuts").get_json()["shortcuts"] == []


def test_config_post_ignores_a_non_list_payload(app_and_db):
    """A malformed write leaves the configured row alone."""
    import config as cfg

    app, _ = app_and_db
    client = app.test_client()
    client.post("/api/config", json={"filter_shortcuts": "wat"})
    assert cfg.load()["filter_shortcuts"] == fs.for_storage(fs.normalize(None))


def test_settings_import_rejects_a_malformed_quick_filter_list(app_and_db):
    import json

    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/settings/import", json={
        "json": json.dumps({"filter_shortcuts": {"not": "a list"}}),
    })
    assert resp.status_code == 400
    assert "filter_shortcuts" in resp.get_json()["errors"]


def test_settings_import_normalizes_quick_filters(app_and_db):
    import json

    import config as cfg

    app, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/settings/import", json={
        "json": json.dumps({"filter_shortcuts": [
            {"label": "Keepers", "rules": {"field": "rating", "op": ">=", "value": 4}},
            {"label": "Broken", "rules": {"field": "rating", "op": "under", "value": 4}},
        ]}),
    })
    assert resp.status_code == 200, resp.get_json()
    stored = cfg.load()["filter_shortcuts"]
    assert [entry["label"] for entry in stored] == ["Keepers"]
    assert stored[0]["id"]
