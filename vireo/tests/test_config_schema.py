"""Tests for vireo/config_schema.py — drift guard, validation, dotted-path helpers."""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ---------------------------------------------------------------------------
# Drift guard: SCHEMA must cover every leaf in DEFAULTS (modulo EXCLUDED).
# ---------------------------------------------------------------------------


def test_schema_covers_all_defaults_leaf_keys():
    """Every non-excluded leaf in DEFAULTS has a SCHEMA entry."""
    from config import DEFAULTS
    from config_schema import SCHEMA, flatten, is_excluded

    flat = flatten(DEFAULTS)
    expected = {k for k in flat if not is_excluded(k)}
    missing = expected - set(SCHEMA.keys())
    extra = set(SCHEMA.keys()) - expected
    assert not missing, f"SCHEMA is missing leaf keys: {sorted(missing)}"
    assert not extra, f"SCHEMA has keys absent from DEFAULTS: {sorted(extra)}"


def test_schema_excluded_prefixes_are_real():
    """Each EXCLUDED prefix actually matches something in DEFAULTS (no dead exclusions)."""
    from config import DEFAULTS
    from config_schema import EXCLUDED, flatten

    flat_keys = set(flatten(DEFAULTS).keys())
    for prefix in EXCLUDED:
        matches = [k for k in flat_keys if k == prefix or k.startswith(prefix + ".")]
        assert matches, f"EXCLUDED prefix {prefix!r} matches no DEFAULTS key"


def test_schema_entries_have_required_fields():
    """Every SCHEMA entry declares type, category, scope, label, desc."""
    from config_schema import CATEGORIES, SCHEMA

    valid_types = {"int", "float", "bool", "string", "secret", "path", "enum", "list_string"}
    valid_scopes = {"global", "workspace", "both"}
    for key, spec in SCHEMA.items():
        for required in ("type", "category", "scope", "label", "desc"):
            assert required in spec, f"{key} missing field {required!r}"
        assert spec["type"] in valid_types, f"{key} has unknown type {spec['type']!r}"
        assert spec["scope"] in valid_scopes, f"{key} has unknown scope {spec['scope']!r}"
        assert spec["category"] in CATEGORIES, f"{key} has uncategorized category {spec['category']!r}"
        if spec["type"] == "enum":
            assert "enum" in spec and isinstance(spec["enum"], list) and spec["enum"], \
                f"{key} is enum but has no enum values"


# ---------------------------------------------------------------------------
# Dotted-path helpers
# ---------------------------------------------------------------------------


def test_flatten_collapses_nested_dicts_to_dotted_paths():
    from config_schema import flatten

    assert flatten({"a": 1, "b": {"c": 2, "d": {"e": 3}}}) == {"a": 1, "b.c": 2, "b.d.e": 3}


def test_flatten_empty_dict():
    from config_schema import flatten

    assert flatten({}) == {}


def test_get_dotted_returns_value_at_path():
    from config_schema import get_dotted

    d = {"a": {"b": {"c": 5}}}
    assert get_dotted(d, "a.b.c") == 5
    assert get_dotted(d, "a.b") == {"c": 5}
    assert get_dotted(d, "a") == {"b": {"c": 5}}


def test_get_dotted_returns_default_for_missing_key():
    from config_schema import get_dotted

    assert get_dotted({"a": 1}, "b") is None
    assert get_dotted({"a": 1}, "b", default="x") == "x"
    assert get_dotted({"a": {"b": 1}}, "a.c") is None


def test_set_dotted_creates_intermediate_dicts():
    from config_schema import set_dotted

    d = {}
    set_dotted(d, "a.b.c", 7)
    assert d == {"a": {"b": {"c": 7}}}


def test_set_dotted_overwrites_existing_value():
    from config_schema import set_dotted

    d = {"a": {"b": 1}}
    set_dotted(d, "a.b", 2)
    assert d == {"a": {"b": 2}}


def test_set_dotted_replaces_non_dict_intermediate():
    """If an intermediate path is not a dict, set_dotted replaces it."""
    from config_schema import set_dotted

    d = {"a": 1}
    set_dotted(d, "a.b", 2)
    assert d == {"a": {"b": 2}}


def test_delete_dotted_removes_leaf():
    from config_schema import delete_dotted

    d = {"a": {"b": 1, "c": 2}}
    assert delete_dotted(d, "a.b") is True
    assert d == {"a": {"c": 2}}


def test_delete_dotted_returns_false_for_missing():
    from config_schema import delete_dotted

    d = {"a": {"b": 1}}
    assert delete_dotted(d, "a.x") is False
    assert delete_dotted(d, "x.y.z") is False


def test_is_excluded_matches_prefixes():
    from config_schema import is_excluded

    # Direct match
    assert is_excluded("setup_complete") is True
    # Subtree match
    assert is_excluded("keyboard_shortcuts.navigation.import") is True
    assert is_excluded("ingest.recent_destinations") is True
    # Non-match
    assert is_excluded("ingest.skip_duplicates") is False
    assert is_excluded("hf_token") is False
    # Prefix collision: "setup" alone shouldn't match "setup_complete"
    assert is_excluded("setup") is False


# ---------------------------------------------------------------------------
# validate_value: type coercion, range, enum, list_string
# ---------------------------------------------------------------------------


def test_validate_unknown_key_raises():
    from config_schema import ValidationError, validate_value

    with pytest.raises(ValidationError):
        validate_value("bogus_key_no_one_will_ever_add", 1)


def test_validate_int_coerces_string():
    from config_schema import validate_value

    assert validate_value("photos_per_page", "100") == 100
    assert validate_value("photos_per_page", 50) == 50


def test_validate_int_rejects_garbage():
    from config_schema import ValidationError, validate_value

    with pytest.raises(ValidationError):
        validate_value("photos_per_page", "not-a-number")


def test_validate_int_enforces_range():
    from config_schema import ValidationError, validate_value

    # photos_per_page is 10..500
    with pytest.raises(ValidationError):
        validate_value("photos_per_page", 5)
    with pytest.raises(ValidationError):
        validate_value("photos_per_page", 5000)


def test_validate_float_coerces_string():
    from config_schema import validate_value

    assert validate_value("classification_threshold", "0.5") == 0.5


def test_validate_float_enforces_range():
    from config_schema import ValidationError, validate_value

    with pytest.raises(ValidationError):
        validate_value("classification_threshold", -0.1)
    with pytest.raises(ValidationError):
        validate_value("classification_threshold", 1.5)


def test_validate_bool_coerces_strings():
    from config_schema import validate_value

    # ingest.skip_duplicates is bool
    assert validate_value("ingest.skip_duplicates", "true") is True
    assert validate_value("ingest.skip_duplicates", "false") is False
    assert validate_value("ingest.skip_duplicates", True) is True
    assert validate_value("ingest.skip_duplicates", 0) is False


def test_validate_enum_accepts_declared_values():
    from config_schema import validate_value

    assert validate_value("keyword_case", "auto") == "auto"
    assert validate_value("keyword_case", "title") == "title"


def test_validate_enum_rejects_unknown():
    from config_schema import ValidationError, validate_value

    with pytest.raises(ValidationError):
        validate_value("keyword_case", "screaming-snake")


def test_validate_string_passthrough():
    from config_schema import validate_value

    assert validate_value("darktable_style", "Bird preset") == "Bird preset"
    assert validate_value("darktable_style", "") == ""


def test_validate_secret_passthrough():
    from config_schema import validate_value

    assert validate_value("hf_token", "hf_xxx") == "hf_xxx"


def test_validate_list_string_accepts_list():
    from config_schema import validate_value

    out = validate_value("scan_roots", ["/a", "/b"])
    assert out == ["/a", "/b"]


def test_validate_list_string_rejects_non_list():
    from config_schema import ValidationError, validate_value

    with pytest.raises(ValidationError):
        validate_value("scan_roots", "/a")


def test_validate_list_string_with_items_enum_rejects_unknown():
    from config_schema import ValidationError, validate_value

    # browse_card_fields has items_enum
    with pytest.raises(ValidationError):
        validate_value("browse_card_fields", ["filename", "bogus_field"])


def test_validate_list_string_with_items_enum_accepts_subset():
    from config_schema import validate_value

    out = validate_value("browse_card_fields", ["filename", "rating"])
    assert out == ["filename", "rating"]
