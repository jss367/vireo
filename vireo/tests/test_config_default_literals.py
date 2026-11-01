"""Config defaults live in ``config.DEFAULTS`` and nowhere else.

A call site that reads a setting with ``values.get("detector_confidence", 0.2)``
restates the default; when ``DEFAULTS`` changes, every copy silently keeps the
old value. Read settings with ``config.config_value(values, key)`` (pass
``section="pipeline"`` or ``section="ingest"`` for nested keys) so the default
comes from ``DEFAULTS``.

This test finds ``<expr>.get("<key>", <literal>)`` calls under ``vireo/`` whose
key is a ``DEFAULTS`` setting and whose literal equals that setting's default.
Booleans, ``None`` and the empty string are not counted: they mean "unset"
rather than restating a tuned value.

``REPOSITORY_ALLOWANCE`` caps copies a module may keep while it is reworked;
it is empty now, and the caps may only shrink. The data layer, which may not
import ``config``, reads defaults through ``settings_source.config_value``.
"""

import ast
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import config  # noqa: E402

VIREO_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

REPOSITORY_ALLOWANCE: dict[str, int] = {}


def _defaults_by_key():
    """``key -> default`` for scalar settings, top-level and nested."""
    defaults = {}
    for key, value in config.DEFAULTS.items():
        if isinstance(value, dict):
            for nested_key, nested_value in value.items():
                defaults.setdefault(nested_key, nested_value)
        else:
            defaults[key] = value
    return defaults


def _restates_default(literal, default):
    if literal is None or isinstance(literal, bool) or literal == "":
        return False
    if isinstance(default, (bool, list, dict)):
        return False
    return type(literal) in (int, float, str) and literal == default


def _restated_defaults():
    defaults = _defaults_by_key()
    found = []
    for dirpath, dirnames, filenames in os.walk(VIREO_DIR):
        dirnames[:] = [d for d in dirnames if d not in ("tests", "node_modules", "__pycache__")]
        for filename in filenames:
            if not filename.endswith(".py"):
                continue
            path = os.path.join(dirpath, filename)
            rel = os.path.relpath(path, VIREO_DIR).replace(os.sep, "/")
            if rel == "config.py":
                continue
            with open(path, encoding="utf-8") as f:
                tree = ast.parse(f.read(), filename=path)
            for node in ast.walk(tree):
                if not (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "get"
                    and len(node.args) == 2
                    and isinstance(node.args[0], ast.Constant)
                    and isinstance(node.args[1], ast.Constant)
                ):
                    continue
                key = node.args[0].value
                if key in defaults and _restates_default(node.args[1].value, defaults[key]):
                    found.append((rel, node.lineno, key))
    return found


def test_call_sites_take_setting_defaults_from_config():
    by_file = {}
    for rel, lineno, key in _restated_defaults():
        by_file.setdefault(rel, []).append(f"{rel}:{lineno} {key}")
    problems = []
    for rel, sites in sorted(by_file.items()):
        allowed = REPOSITORY_ALLOWANCE.get(rel, 0)
        if len(sites) > allowed:
            problems.extend(sites)
    assert not problems, (
        "These reads restate a config.DEFAULTS value; use "
        "config.config_value(values, key[, section=...]) instead:\n  "
        + "\n  ".join(problems)
    )


def test_repository_allowance_only_shrinks():
    counts = {}
    for rel, _lineno, _key in _restated_defaults():
        counts[rel] = counts.get(rel, 0) + 1
    stale = {
        rel: (allowed, counts.get(rel, 0))
        for rel, allowed in REPOSITORY_ALLOWANCE.items()
        if counts.get(rel, 0) < allowed
    }
    assert not stale, f"Lower REPOSITORY_ALLOWANCE to the current counts: {stale}"


def test_config_value_reads_defaults_for_missing_keys():
    assert config.config_value({}, "detector_confidence") == config.DEFAULTS["detector_confidence"]
    assert config.config_value(None, "detector_confidence") == config.DEFAULTS["detector_confidence"]
    assert config.config_value({"detector_confidence": 0.5}, "detector_confidence") == 0.5
    assert (
        config.config_value({}, "burst_time_gap", section="pipeline")
        == config.DEFAULTS["pipeline"]["burst_time_gap"]
    )
    assert config.config_value({"burst_time_gap": 1.0}, "burst_time_gap", section="pipeline") == 1.0
