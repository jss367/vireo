import ast
from pathlib import Path

APP_SOURCE = Path(__file__).resolve().parents[1] / "app.py"

# Every ``Database`` method that writes a row's review decision. A route that
# can reach one of these is a prediction-decision route and must hold the
# writer lock across its precondition read and its writes.
_PREDICTION_MUTATORS = frozenset({
    "accept_prediction",
    "accept_subject_species",
    "update_prediction_status",
    "update_predictions_status_by_photo",
    "ungroup_prediction",
    "set_review_status",
    # Undo/redo replay ``prediction_review`` statuses out of edit history.
    "undo_last_edit",
    "redo_last_undo",
})


def _create_app_functions():
    """Every function defined inside ``create_app``, by name."""
    tree = ast.parse(APP_SOURCE.read_text(encoding="utf-8"))
    create_app = next(
        node for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "create_app"
    )
    return create_app, {
        node.name: node
        for node in ast.walk(create_app)
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
    }


def _is_route(node):
    return any(
        isinstance(dec, ast.Call)
        and isinstance(dec.func, ast.Attribute)
        and dec.func.attr == "route"
        for dec in node.decorator_list
    )


def _called_names(node):
    """Every name this function calls, directly or from a nested def/lambda."""
    names = set()
    for child in ast.walk(node):
        if not isinstance(child, ast.Call):
            continue
        func = child.func
        if isinstance(func, ast.Name):
            names.add(func.id)
        elif isinstance(func, ast.Attribute):
            names.add(func.attr)
    return names


def _reaches(name, target, call_map, functions, seen=None):
    """Can ``name`` reach ``target`` through ``create_app``-local calls?"""
    seen = set() if seen is None else seen
    if name in seen:
        return False
    seen.add(name)
    if target in call_map.get(name, set()):
        return True
    return any(
        _reaches(callee, target, call_map, functions, seen)
        for callee in call_map.get(name, set())
        if callee in functions and callee != name
    )


def _declared_decision_routes(create_app):
    for node in ast.walk(create_app):
        if not isinstance(node, ast.Assign):
            continue
        targets = [t.id for t in node.targets if isinstance(t, ast.Name)]
        if "_PREDICTION_DECISION_ROUTES" in targets:
            return {elt.value for elt in node.value.elts}
    raise AssertionError("_PREDICTION_DECISION_ROUTES not found in create_app")


def test_every_prediction_decision_route_locks():
    """No route may write a review decision without taking the shared lock.

    Structural rather than a handful of hand-written cases, because the two
    gaps this test exists to catch were both *omissions*, one round apart:
    first ``BEGIN IMMEDIATE`` covered only the batch endpoints while Review's
    single-row routes wrote freely, then the single-row sweep left burst group
    apply, both highlight routes and undo/redo out. A lock serializes only if
    every writer takes it, and the way that stays true as routes are added is
    to derive the set from the call graph instead of from memory.

    So: walk every function defined inside ``create_app``, find the routes
    that can reach a ``prediction_review`` writer, and compare that set with
    the declared ``_PREDICTION_DECISION_ROUTES``. Then check each declared
    route actually reaches ``_begin_prediction_decision`` — declaring it is
    not the same as taking it.
    """
    create_app, functions = _create_app_functions()
    call_map = {name: _called_names(node) for name, node in functions.items()}
    routes = {name for name, node in functions.items() if _is_route(node)}

    reaching = {
        name for name in routes
        if any(
            _reaches(name, mutator, call_map, functions)
            for mutator in _PREDICTION_MUTATORS
        )
    }
    declared = _declared_decision_routes(create_app)

    assert reaching == declared, (
        "routes that write prediction decisions but are not declared in "
        "_PREDICTION_DECISION_ROUTES (or vice versa): "
        f"{sorted(reaching ^ declared)}"
    )

    for name in sorted(declared):
        assert _reaches(
            name, "_begin_prediction_decision", call_map, functions,
        ), f"{name} is declared a decision route but never takes the lock"


# Routes still registered with ``@app.<verb>`` in ``vireo/app.py``. This number
# may only go down. New routes belong in a blueprint under ``vireo/web/`` (see
# docs/ARCHITECTURE.md). When a PR moves routes out of app.py, lower this to
# the new count in the same PR so the extraction cannot be undone.
_LEGACY_APP_ROUTE_LIMIT = 249

_ROUTE_VERBS = frozenset({"route", "get", "post", "put", "patch", "delete"})


def _legacy_app_routes():
    """``(function name, line)`` for every ``@app.<verb>`` rule in app.py."""
    tree = ast.parse(APP_SOURCE.read_text(encoding="utf-8"))
    return [
        (node.name, dec.lineno)
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
        for dec in node.decorator_list
        if isinstance(dec, ast.Call)
        and isinstance(dec.func, ast.Attribute)
        and dec.func.attr in _ROUTE_VERBS
        and isinstance(dec.func.value, ast.Name)
        and dec.func.value.id == "app"
    ]


def test_no_new_routes_in_app_py():
    """app.py may not gain routes. The limit only ratchets down.

    docs/ARCHITECTURE.md has said "do not add routes to the legacy application
    module" since July 2026, and app.py gained dozens of routes anyway. A rule
    that lives only in prose does not hold, so this test enforces it.
    """
    count = len(_legacy_app_routes())
    assert count <= _LEGACY_APP_ROUTE_LIMIT, (
        f"vireo/app.py registers {count} routes; the limit is "
        f"{_LEGACY_APP_ROUTE_LIMIT}. Put new routes in a blueprint under "
        "vireo/web/ (see docs/ARCHITECTURE.md and the create_*_blueprint "
        "factories there) instead of adding @app.route inside create_app."
    )
    assert count == _LEGACY_APP_ROUTE_LIMIT, (
        f"vireo/app.py now registers {count} routes, below the limit of "
        f"{_LEGACY_APP_ROUTE_LIMIT}. Lower _LEGACY_APP_ROUTE_LIMIT in "
        f"vireo/tests/test_route_contract.py to {count} so the extraction "
        "sticks."
    )


def _public_route_contract(app):
    rows = []
    for rule in app.url_map.iter_rules():
        if rule.rule.startswith("/static/"):
            continue
        methods = sorted(set(rule.methods) - {"HEAD", "OPTIONS"})
        rows.append(f"{','.join(methods):12} {rule.rule}")
    return "\n".join(sorted(rows)) + "\n"


def test_route_contract_matches_snapshot(app_and_db):
    app, _ = app_and_db
    contract = _public_route_contract(app)
    # Keep this snapshot readable so route changes produce reviewable, mergeable diffs.
    snapshot_path = Path(__file__).with_name("contracts") / "routes.txt"
    # Compare line-by-line so any stray line-ending variance (e.g. a snapshot
    # edited on a CRLF checkout) can't fail the test independently of route
    # changes. read_text() already normalizes newlines to \n, but splitlines()
    # is unambiguous about the intent.
    expected = snapshot_path.read_text(encoding="utf-8").splitlines()
    actual = contract.splitlines()

    assert actual == expected
