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


def _collect_functions(module_label, tree):
    """Yield ``(qualified_name, node)`` for every function definition.

    The qualified name is ``(module_label, ...enclosing_scope_names, own_name)``
    — the lexical path from the module root, with every enclosing ``def`` and
    ``class`` on the way in. A nested function stays a distinct node from a
    same-named function elsewhere, which is what a name-keyed graph loses.
    """
    def walk(node, scope):
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef):
                qname = scope + (child.name,)
                yield qname, child
                yield from walk(child, qname)
            elif isinstance(child, ast.ClassDef):
                yield from walk(child, scope + (child.name,))
            else:
                yield from walk(child, scope)
    yield from walk(tree, (module_label,))


def _called_names(node):
    """Every bare name this function's *own* body calls.

    Nested ``def``/``async def``/class bodies are their own graph nodes, so
    their calls belong to them — walking into them here would fold their
    reachability into the enclosing function and re-create the merge-by-name
    bug the qualified graph exists to avoid. Lambdas have no name and stay
    walked in. A bare name passed as an argument counts as a call, because a
    blueprint may define ``_decide`` beside the route and pass it to
    ``under_prediction_decision_lock(db, _decide, ...)``.
    """
    names = set()

    def visit(current):
        for child in ast.iter_child_nodes(current):
            if isinstance(
                child, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef,
            ):
                continue
            if isinstance(child, ast.Call):
                func = child.func
                if isinstance(func, ast.Name):
                    names.add(func.id)
                elif isinstance(func, ast.Attribute):
                    names.add(func.attr)
                for arg in (*child.args, *(kw.value for kw in child.keywords)):
                    if isinstance(arg, ast.Name):
                        names.add(arg.id)
            visit(child)

    visit(node)
    return names


def _call_graph(sources):
    """Build ``(call_map, routes, resolve)`` from module sources.

    Each function keeps its scope-qualified identity, so a helper named
    ``_decide`` in one blueprint stays distinct from a ``_decide`` in another.
    Merging by bare name would let an unlocked route calling the local
    ``_decide`` appear to reach ``begin_prediction_decision`` through an
    unrelated same-named function, so declaring the route would silence the
    "declared but never reaches lock" check on an unlocked writer.

    - ``call_map[qualified]`` — set of bare names ``qualified``'s own body
      calls or receives as a Name argument. Attribute calls on ``self``/``db``
      (mutators, or ``prediction_decisions.begin_prediction_decision``) land
      in this set as bare names too, and the target intersection in
      ``_reaches`` matches them there without needing a graph node.
    - ``routes[view_function_name]`` — list of ``(qualified, "file:line")`` for
      every place that route is defined. Keyed by bare name because
      ``PREDICTION_DECISION_ROUTES`` names view functions, and the contract
      test enforces that a decision route's name is unique across ``app.py``
      and ``vireo/web/``.
    - ``resolve(caller_qualified, bare_name)`` — set of qualified callees to
      recurse into. Resolves each bare-name call against the caller's lexical
      scope chain (a helper defined beside the route wins over a same-named
      helper elsewhere), falling back to module-level functions across every
      scanned module when nothing enclosing matches. Nested functions do not
      leak across module or factory boundaries.
    """
    call_map = {}
    routes = {}
    module_top = {}
    nested = {}

    for label, text, may_register_routes in sources:
        tree = ast.parse(text)
        for qname, node in _collect_functions(label, tree):
            call_map[qname] = _called_names(node)
            if len(qname) == 2:
                module_top.setdefault(qname[-1], set()).add(qname)
            else:
                nested.setdefault((qname[:-1], qname[-1]), set()).add(qname)
            if may_register_routes and _is_route(node):
                routes.setdefault(node.name, []).append(
                    (qname, f"{label}:{node.lineno}"),
                )

    def resolve(caller_qualified, bare_name):
        # Innermost lexical scope outward: a caller's own inner helpers
        # (``def _apply`` inside a route body, captured through
        # ``under_prediction_decision_lock(db, _apply, ...)``) live at
        # ``caller_qualified`` itself; siblings sit one step out.
        for depth in range(len(caller_qualified), 0, -1):
            found = nested.get((caller_qualified[:depth], bare_name))
            if found:
                return found
        return module_top.get(bare_name, set())

    return call_map, routes, resolve


def _reaches(start, targets, call_map, resolve):
    """Can ``start`` (a qualified name) reach any bare name in ``targets``?"""
    seen = {start}
    stack = [start]
    while stack:
        current = stack.pop()
        callees = call_map.get(current, set())
        if callees & targets:
            return True
        for bare in callees:
            for qualified in resolve(current, bare):
                if qualified not in seen:
                    seen.add(qualified)
                    stack.append(qualified)
    return False


def _decision_route_problems(call_map, routes, resolve, declared):
    """Every way the routes and ``declared`` disagree, as readable lines."""
    def route_reaches(name, targets):
        return any(
            _reaches(qualified, targets, call_map, resolve)
            for qualified, _ in routes.get(name, ())
        )

    reaching = {
        name for name in routes
        if route_reaches(name, _PREDICTION_MUTATORS)
    }

    def where(name):
        entries = routes.get(name)
        if not entries:
            return "no route with this name"
        return ", ".join(location for _, location in entries)

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
        if not route_reaches(name, {_DECISION_LOCK})
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
    call_map, routes, resolve = _call_graph(sources)

    assert routes, "found no routes at all; the route scan is broken"
    problems = _decision_route_problems(
        call_map, routes, resolve, set(PREDICTION_DECISION_ROUTES),
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
    call_map, routes, resolve = _call_graph([
        ("web/review.py", blueprint, True),
        ("services/prediction_decisions.py", service, False),
    ])
    assert set(routes) == {"api_locked", "api_unlocked", "api_read"}
    line = next(
        number for number, text in enumerate(blueprint.splitlines(), 1)
        if "def api_unlocked" in text
    )

    assert _decision_route_problems(
        call_map, routes, resolve, {"api_locked", "api_unlocked"},
    ) == [
        f"api_unlocked (web/review.py:{line}) is declared a decision route "
        "but never reaches begin_prediction_decision",
    ]
    assert _decision_route_problems(
        call_map, routes, resolve, {"api_locked"},
    ) == [
        f"api_unlocked (web/review.py:{line}) writes prediction decisions but "
        "is not in PREDICTION_DECISION_ROUTES",
    ]


def test_decision_route_analysis_distinguishes_same_named_helpers():
    """A bare helper name in one blueprint must not resolve to another's.

    A merged-by-name graph unions the callees of every function sharing a
    bare name. Two blueprints each defining ``_decide`` — one that locks and
    one that does not — then look the same to the reachability walk, so an
    unlocked route calling its local ``_decide`` appears to reach the lock
    through the unrelated helper and declaring it silences the "declared but
    never reaches lock" check. Scope-qualified identities resolve each
    bare-name call inside its own factory first, so the two ``_decide``
    helpers stay distinct and the unlocked route is flagged.
    """
    unlocked = '''
def create_unlocked_blueprint(get_db, json_error):
    bp = Blueprint("u", __name__)

    def _decide(db):
        db.update_prediction_status(1, "rejected")

    @bp.post("/api/u")
    def api_u():
        return _decide(get_db())

    return bp
'''
    locked = '''
from services import prediction_decisions

def create_locked_blueprint(get_db, json_error):
    bp = Blueprint("l", __name__)

    def _decide(db):
        db.update_prediction_status(2, "accepted")
        return prediction_decisions.begin_prediction_decision(
            db, json_error=json_error,
        )

    @bp.post("/api/l")
    def api_l():
        return _decide(get_db())

    return bp
'''
    service = (VIREO_DIR / "services" / "prediction_decisions.py").read_text(
        encoding="utf-8",
    )
    call_map, routes, resolve = _call_graph([
        ("web/unlocked.py", unlocked, True),
        ("web/locked.py", locked, True),
        ("services/prediction_decisions.py", service, False),
    ])
    problems = _decision_route_problems(
        call_map, routes, resolve, {"api_u", "api_l"},
    )
    assert any(
        p.startswith("api_u ") and "never reaches begin_prediction_decision" in p
        for p in problems
    ), problems
    assert not any(p.startswith("api_l ") for p in problems), problems


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
