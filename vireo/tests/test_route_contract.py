import ast
from pathlib import Path

from services.prediction_decisions import PREDICTION_DECISION_ROUTES

VIREO_DIR = Path(__file__).resolve().parents[1]
APP_SOURCE = VIREO_DIR / "app.py"

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

# The function that takes the lock (``vireo/services/prediction_decisions.py``).
_DECISION_LOCK = "begin_prediction_decision"


def _decision_sources():
    """``(path, may_register_routes)`` for every module the analysis reads.

    Routes live in ``vireo/app.py`` and the blueprints under ``vireo/web/``;
    both are scanned for routes. ``vireo/services/`` registers none, but routes
    call into it (the lock itself lives there), so its functions join the call
    graph.
    """
    return [
        (APP_SOURCE, True),
        *((path, True) for path in sorted((VIREO_DIR / "web").glob("*.py"))),
        *((path, False) for path in sorted((VIREO_DIR / "services").glob("*.py"))),
    ]


def _is_route(node):
    """``@<anything>.route(...)`` / ``.get`` / ``.post`` / ... decorators.

    Covers ``@app.route`` in ``create_app`` and ``@bp.post`` (or whatever the
    blueprint object is called) inside a ``create_*_blueprint`` factory.
    """
    return any(
        isinstance(dec, ast.Call)
        and isinstance(dec.func, ast.Attribute)
        and dec.func.attr in _ROUTE_VERBS
        for dec in node.decorator_list
    )


def _called_names(node):
    """Every name this function calls, directly or from a nested def/lambda.

    A bare name passed as an argument counts too: ``under_prediction_decision_lock
    (db, _decide, ...)`` runs ``_decide``, and a blueprint may well define
    ``_decide`` beside the route rather than inside it.
    """
    names = set()
    for child in ast.walk(node):
        if not isinstance(child, ast.Call):
            continue
        func = child.func
        if isinstance(func, ast.Name):
            names.add(func.id)
        elif isinstance(func, ast.Attribute):
            names.add(func.attr)
        for arg in (*child.args, *(kw.value for kw in child.keywords)):
            if isinstance(arg, ast.Name):
                names.add(arg.id)
    return names


def _call_graph(sources):
    """Build ``(call_map, routes)`` from ``(label, text, may_register_routes)``.

    ``call_map`` maps a function name to every name it calls. It is keyed by
    bare name across all modules, and same-named functions (a ``_work`` in two
    routes, a helper in two blueprints) are unioned: an over-approximation, so
    a route can only be flagged spuriously, never missed. ``routes`` maps each
    route's view function name to the ``file:line`` places defining it.
    """
    call_map = {}
    routes = {}
    for label, text, may_register_routes in sources:
        for node in ast.walk(ast.parse(text)):
            if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                continue
            call_map.setdefault(node.name, set()).update(_called_names(node))
            if may_register_routes and _is_route(node):
                routes.setdefault(node.name, []).append(f"{label}:{node.lineno}")
    return call_map, routes


def _reaches(name, targets, call_map):
    """Can ``name`` reach any of ``targets`` through analyzed functions?"""
    seen = set()
    stack = [name]
    while stack:
        current = stack.pop()
        if current in seen:
            continue
        seen.add(current)
        callees = call_map.get(current, set())
        if callees & targets:
            return True
        stack.extend(callee for callee in callees if callee in call_map)
    return False


def _decision_route_problems(call_map, routes, declared):
    """Every way the routes and ``declared`` disagree, as readable lines."""
    reaching = {
        name for name in routes
        if _reaches(name, _PREDICTION_MUTATORS, call_map)
    }

    def where(name):
        return ", ".join(routes.get(name, ["no route with this name"]))

    problems = [
        f"{name} ({where(name)}) writes prediction decisions but is not in "
        "PREDICTION_DECISION_ROUTES"
        for name in sorted(reaching - declared)
    ]
    problems += [
        f"{name} ({where(name)}) is in PREDICTION_DECISION_ROUTES but reaches "
        "no prediction_review writer"
        for name in sorted(declared - reaching)
    ]
    problems += [
        f"{name} is defined as a route more than once ({where(name)}); "
        "PREDICTION_DECISION_ROUTES matches by function name, so a decision "
        "route's name must be unique across app.py and vireo/web/"
        for name in sorted(reaching | declared)
        if len(routes.get(name, [])) > 1
    ]
    problems += [
        f"{name} ({where(name)}) is declared a decision route but never "
        f"reaches {_DECISION_LOCK}"
        for name in sorted(declared & set(routes))
        if not _reaches(name, {_DECISION_LOCK}, call_map)
    ]
    return problems


def test_every_prediction_decision_route_locks():
    """No route may write a review decision without taking the shared lock.

    Structural rather than a handful of hand-written cases, because the two
    gaps this test exists to catch were both *omissions*, one round apart:
    first ``BEGIN IMMEDIATE`` covered only the batch endpoints while Review's
    single-row routes wrote freely, then the single-row sweep left burst group
    apply, both highlight routes and undo/redo out. A lock serializes only if
    every writer takes it, and the way that stays true as routes are added is
    to derive the set from the call graph instead of from memory.

    So: walk every route in ``vireo/app.py`` and every blueprint module under
    ``vireo/web/``, follow calls through the functions defined there and in
    ``vireo/services/``, find the routes that can reach a
    ``prediction_review`` writer, and compare that set with the declared
    ``PREDICTION_DECISION_ROUTES`` (view function names, so a route keeps its
    entry when it moves into a blueprint). Then check each declared route
    actually reaches ``begin_prediction_decision`` — declaring it is not the
    same as taking it.
    """
    sources = [
        (path.relative_to(VIREO_DIR.parent).as_posix(),
         path.read_text(encoding="utf-8"),
         may_register_routes)
        for path, may_register_routes in _decision_sources()
    ]
    call_map, routes = _call_graph(sources)

    assert routes, "found no routes at all; the route scan is broken"
    problems = _decision_route_problems(
        call_map, routes, set(PREDICTION_DECISION_ROUTES),
    )
    assert not problems, "\n".join(problems)


def test_decision_route_analysis_covers_blueprint_routes():
    """The analysis sees a blueprint route, and a blueprint route's omission.

    Pins the part a structural test can silently lose: routes registered on a
    blueprint inside a factory, helpers passed by reference, and the lock
    reached through the service module.
    """
    blueprint = '''
from services import prediction_decisions

def create_review_blueprint(get_db, json_error):
    bp = Blueprint("review", __name__)

    def _decide(db):
        db.update_prediction_status(1, "rejected")

    @bp.post("/api/locked")
    def api_locked():
        db = get_db()
        return prediction_decisions.under_prediction_decision_lock(
            db, _decide, json_error=json_error,
        )

    @bp.route("/api/unlocked", methods=["POST"])
    def api_unlocked():
        _decide(get_db())

    @bp.get("/api/read")
    def api_read():
        return get_db().get_predictions()

    return bp
'''
    service = (VIREO_DIR / "services" / "prediction_decisions.py").read_text(
        encoding="utf-8",
    )
    call_map, routes = _call_graph([
        ("web/review.py", blueprint, True),
        ("services/prediction_decisions.py", service, False),
    ])
    assert set(routes) == {"api_locked", "api_unlocked", "api_read"}
    line = next(
        number for number, text in enumerate(blueprint.splitlines(), 1)
        if "def api_unlocked" in text
    )

    assert _decision_route_problems(
        call_map, routes, {"api_locked", "api_unlocked"},
    ) == [
        f"api_unlocked (web/review.py:{line}) is declared a decision route "
        "but never reaches begin_prediction_decision",
    ]
    assert _decision_route_problems(call_map, routes, {"api_locked"}) == [
        f"api_unlocked (web/review.py:{line}) writes prediction decisions but "
        "is not in PREDICTION_DECISION_ROUTES",
    ]


# Routes still registered with ``@app.<verb>`` in ``vireo/app.py``. This number
# may only go down. New routes belong in a blueprint under ``vireo/web/`` (see
# docs/ARCHITECTURE.md). When a PR moves routes out of app.py, lower this to
# the new count in the same PR so the extraction cannot be undone.
_LEGACY_APP_ROUTE_LIMIT = 143

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
