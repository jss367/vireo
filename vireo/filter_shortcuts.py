"""User-configurable quick filters for the universal filter bar.

The always-visible button row in the filter bar (``_filterbar.html``) is
rendered from ``config["filter_shortcuts"]`` instead of hardcoded markup, so
Settings can rename, reorder, remove, and add buttons. One stored entry::

    {"id": "flag_picked", "label": "⚑ Picked", "group": "flag",
     "rules": {"field": "flag", "op": "is", "value": "flagged"}}

``rules`` is an ordinary filter-rule node -- the same JSON the rule builder
and saved collections use -- so a shortcut is just a saved expression with a
button in front of it. ``normalize()`` derives how the bar must combine that
expression (``kind``) from the rule's shape rather than trusting a stored
flag:

``missing``
    A boolean field set to "no" (``has_species is 0``). Members of one
    ``group`` OR together, so "Missing species" + "Missing location tag"
    means *missing either tag* -- the semantics that pair has always had.
``enum``
    A single enum value (``flag is flagged``). Shortcuts on the same field
    merge into one ``in`` clause, so Picked + Rejected reads "Flag is one of
    Picked, Rejected" rather than an impossible AND.
``rules``
    Anything else: one clause ANDed into the current expression.

Unusable entries (unknown field, operator the field does not support, value
outside a closed vocabulary) are dropped here rather than rendered as a
button that could never match anything.
"""

import datetime
import json
import math
import re
import uuid

from filter_fields import FILTER_FIELDS

# Rendering styles a group of shortcuts can take. A group whose members are
# all single enum values is a segmented control (they pick between values of
# one field); everything else is a row of pills.
STYLE_PILLS = "pills"
STYLE_SEGMENTED = "segmented"

MAX_LABEL_LEN = 40
MAX_SHORTCUTS = 24
# Depth of a stored rule tree. A shortcut is a button, not a rule builder;
# two levels of nesting is already more than the bar can label usefully.
MAX_RULE_DEPTH = 3

GROUP_MODES = ("all", "any", "none")

# What the rule engine's boolean branch accepts (``_truthy``/``_falsey`` in
# ``Database._build_query_from_rules``). A boolean field has no closed
# vocabulary, so without this list a stored "yes" would render a button that
# 400s the query the moment it is clicked.
BOOLEAN_TRUE = (True, 1, "1", "true")
BOOLEAN_FALSE = (False, 0, "0", "false")

# ``recent`` carries a {n, unit} window rather than a scalar (see the rule
# engine's date branch and RECENT_UNITS in vireo-filter.js).
RECENT_UNITS = ("days", "weeks", "months", "years")

# Stored timestamps are extended-ISO text, and the comparison is lexical, so
# the shape has to match theirs: "20260101" parses as a date but sorts
# against "2026-01-01 08:00:00" as nonsense.
DATE_SHAPE = re.compile(r"^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2})?)?(Z|[+-]\d{2}:?\d{2})?$")


def _is_date(value):
    """True when the rule engine can compare this against a timestamp.

    Both a non-date and an impossible one ("2026-02-31") otherwise pass
    silently and then match nothing — or, compared lexically, far too much.
    """
    if not isinstance(value, str) or not DATE_SHAPE.match(value):
        return False
    try:
        datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return True

# The bar's original hardcoded buttons, kept as the default configuration so
# an untouched install renders exactly what it always did.
DEFAULT_SHORTCUTS = [
    {"id": "missing_species", "label": "Missing species", "group": "missing",
     "rules": {"field": "has_species", "op": "is", "value": 0}},
    {"id": "missing_location", "label": "Missing location tag",
     "group": "missing",
     "rules": {"field": "has_location_keyword", "op": "is", "value": 0}},
    {"id": "flag_picked", "label": "⚑ Picked", "group": "flag",
     "rules": {"field": "flag", "op": "is", "value": "flagged"}},
    {"id": "flag_unflagged", "label": "– Unflagged", "group": "flag",
     "rules": {"field": "flag", "op": "is", "value": "none"}},
    {"id": "flag_rejected", "label": "× Rejected", "group": "flag",
     "rules": {"field": "flag", "op": "is", "value": "rejected"}},
]


def _is_group(node):
    return isinstance(node, dict) and isinstance(node.get("rules"), list) \
        and "field" not in node


def _clean_scalar(value):
    """Return ``value`` if it can round-trip through the stored JSON."""
    if isinstance(value, bool):
        return int(value)
    if value is None or isinstance(value, (str, int, float)):
        return value
    return None


def _clean_leaf(node):
    """Validate one ``{field, op, value}`` rule against the field registry."""
    field = node.get("field")
    spec = FILTER_FIELDS.get(field) if isinstance(field, str) else None
    if spec is None:
        return None
    op = node.get("op")
    if op not in spec["ops"]:
        return None
    raw = node.get("value")
    if op == "recent":
        if not isinstance(raw, dict) or raw.get("unit") not in RECENT_UNITS:
            return None
        count = raw.get("n")
        if isinstance(count, bool) or not isinstance(count, int) or count < 1:
            return None
        return {"field": field, "op": op, "value": {"n": count, "unit": raw["unit"]}}
    # List vs scalar is not free-form: the rule compiler rejects a list for
    # ``is``, and ``between`` needs exactly two bounds. An entry that passes
    # here but 400s the query when clicked is worse than no button at all.
    wants_list = op in ("in", "not_in", "between")
    if isinstance(raw, list) != wants_list:
        return None
    if wants_list:
        value = [v for v in (_clean_scalar(item) for item in raw) if v is not None]
        if not value or len(value) != len(raw):
            return None
        if op == "between" and len(value) != 2:
            return None
    else:
        value = _clean_scalar(raw)
        if value is None:
            return None
    # A closed vocabulary is the only case where we can tell a typo from a
    # value we simply do not know about (``extension`` and friends suggest
    # values but do not enumerate them).
    allowed = spec.get("values")
    if allowed:
        wanted = value if isinstance(value, list) else [value]
        if any(item not in allowed for item in wanted):
            return None
    # A numeric comparison binds its value as-is: "four" is accepted by
    # SQLite and matches nothing, which is a button that lies rather than one
    # that fails. Same for a date comparison against a non-date.
    if spec["type"] in ("number", "rating"):
        numbers = []
        for item in (value if isinstance(value, list) else [value]):
            try:
                number = float(item)
            except (TypeError, ValueError):
                return None
            # NaN/Infinity survive float() and then serialize as tokens no
            # JSON parser accepts, so the browser loses the whole row.
            if not math.isfinite(number):
                return None
            numbers.append(int(number) if number.is_integer() else number)
        value = numbers if isinstance(value, list) else numbers[0]
    elif spec["type"] == "date" and op != "recent":
        if not all(_is_date(item)
                   for item in (value if isinstance(value, list) else [value])):
            return None
    if spec["type"] == "boolean":
        # Normalize to the 0/1 the defaults use, so ``kind`` and the bar's
        # active-state matching see one representation of "no".
        if any(value is v or value == v for v in BOOLEAN_TRUE):
            value = 1
        elif any(value is v or value == v for v in BOOLEAN_FALSE):
            value = 0
        else:
            return None
    # ``in`` with one value says exactly what ``is`` says. Keeping both
    # shapes would give one filter two identities: different duplicate keys,
    # and different toggle behavior for buttons that mean the same thing.
    if op == "in" and isinstance(value, list) and len(value) == 1 \
            and "is" in spec["ops"]:
        op, value = "is", value[0]
    cleaned = {"field": field, "op": op, "value": value}
    # ``case`` is part of what a text rule means — the query compiler reads
    # it — so a case-sensitive shortcut has to keep it rather than quietly
    # widening to a case-insensitive match.
    if spec.get("case_toggle") and node.get("case"):
        cleaned["case"] = True
    # ``keyword_identity`` carries its own display label through the rule.
    label = node.get("label")
    if isinstance(label, str) and label.strip():
        cleaned["label"] = label.strip()[:MAX_LABEL_LEN]
    return cleaned


def clean_rules(node, depth=0):
    """Return a sanitized copy of a rule node, or None if it is unusable."""
    if not isinstance(node, dict) or depth > MAX_RULE_DEPTH:
        return None
    if not _is_group(node):
        return _clean_leaf(node)
    mode = node.get("mode")
    if mode not in GROUP_MODES:
        return None
    children = [c for c in (clean_rules(child, depth + 1) for child in node["rules"])
                if c is not None]
    if not children:
        return None
    return {"mode": mode, "rules": children}


def _kind(rules):
    """Classify a cleaned rule node into how the bar must combine it."""
    if _is_group(rules):
        return "rules", None, None
    spec = FILTER_FIELDS[rules["field"]]
    if spec["type"] == "boolean" and rules["op"] == "is" and not rules["value"]:
        return "missing", rules["field"], None
    if spec["type"] == "enum" and rules["op"] == "is" \
            and not isinstance(rules["value"], list):
        return "enum", rules["field"], rules["value"]
    return "rules", None, None


def _default_label(rules):
    """Fallback button text for an entry saved without one."""
    if _is_group(rules):
        return "Filter"
    spec = FILTER_FIELDS[rules["field"]]
    kind, _, value = _kind(rules)
    if kind == "missing":
        return f"No {spec['label'].removeprefix('Has ').lower()}"
    if kind == "enum":
        return str((spec.get("labels") or {}).get(value, value))
    return spec["label"]


def _rule_key(rules):
    return json.dumps(rules, sort_keys=True)


def find_duplicate(entries):
    """Labels of the first two entries that apply the same rule, or None.

    Two buttons with one expression cannot be told apart: clicking either
    lights both, and a chip can only carry one label. Write paths refuse such
    a list rather than storing a button that answers for its twin.
    """
    seen = {}
    for entry in normalize(entries, dedupe=False):
        key = _rule_key(entry["rules"])
        if key in seen:
            return (seen[key], entry["label"])
        seen[key] = entry["label"]
    return None


def normalize(entries, dedupe=True):
    """Coerce stored shortcut entries into the list the bar renders.

    Entries that no longer describe a usable filter are dropped; ids are
    minted where missing and de-duplicated so each button stays addressable.
    ``None`` (key absent from config) restores the defaults -- an explicit
    empty list is honored as "no quick filters".
    """
    if entries is None:
        entries = DEFAULT_SHORTCUTS
    if not isinstance(entries, list):
        return normalize(DEFAULT_SHORTCUTS, dedupe=dedupe)
    out = []
    seen_ids = set()
    seen_rules = set()
    for entry in entries[:MAX_SHORTCUTS]:
        if not isinstance(entry, dict):
            continue
        rules = clean_rules(entry.get("rules"))
        if rules is None:
            continue
        # A stored list can predate the write-path check (hand-edited config,
        # an older build); render the first of each expression only.
        if dedupe:
            key = _rule_key(rules)
            if key in seen_rules:
                continue
            seen_rules.add(key)
        kind, field, value = _kind(rules)
        label = entry.get("label")
        label = label.strip()[:MAX_LABEL_LEN] if isinstance(label, str) else ""
        shortcut_id = entry.get("id")
        shortcut_id = shortcut_id.strip()[:64] if isinstance(shortcut_id, str) else ""
        if not shortcut_id or shortcut_id in seen_ids:
            shortcut_id = uuid.uuid4().hex[:12]
        seen_ids.add(shortcut_id)
        group = entry.get("group")
        group = group.strip()[:32] if isinstance(group, str) else ""
        out.append({
            "id": shortcut_id,
            "label": label or _default_label(rules),
            # An ungrouped shortcut is its own group: it renders as a loose
            # pill and toggles a standalone clause.
            "group": group,
            "rules": rules,
            "kind": kind,
            "field": field,
            "value": value,
        })
    return out


def grouped(entries):
    """Split normalized shortcuts into the containers the bar renders.

    Consecutive shortcuts sharing a ``group`` become one container; loose
    (ungrouped) shortcuts collect into the pill row next to them so a newly
    added shortcut does not open a new container of its own.
    """
    groups = []
    for shortcut in entries:
        key = shortcut["group"] or "\x00loose"
        if not groups or groups[-1]["key"] != key:
            # Not "items": Jinja resolves ``group.items`` to the dict method.
            groups.append({"key": key, "group": shortcut["group"], "shortcuts": []})
        groups[-1]["shortcuts"].append(shortcut)
    for group in groups:
        all_enum = all(item["kind"] == "enum" for item in group["shortcuts"])
        one_field = len({item["field"] for item in group["shortcuts"]}) == 1
        group["style"] = (STYLE_SEGMENTED if all_enum and one_field and group["group"]
                          else STYLE_PILLS)
    return groups


def from_config(config):
    """Normalized shortcuts for a loaded config dict."""
    return normalize((config or {}).get("filter_shortcuts"))


def for_storage(entries):
    """Strip derived fields so only the stored shape reaches config.json."""
    return [{k: v for k, v in item.items() if k in ("id", "label", "group", "rules")}
            for item in entries]
