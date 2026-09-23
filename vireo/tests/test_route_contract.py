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
    """Yield ``(qualified_name, node, is_class_method)`` for every function.

    The qualified name is ``(module_label, ...enclosing_scope_names, own_name)``
    — the lexical path from the module root, with every enclosing ``def`` and
    ``class`` on the way in. A nested function stays a distinct node from a
    same-named function elsewhere, which is what a name-keyed graph loses.
    ``is_class_method`` is true when the immediately enclosing scope is a
    ``class``: a service method (``PhotoReviewService.set_flag``,
    ``DecisionService.apply``) that another module can invoke via
    ``ClassName(...).method(...)`` and that the module-level fallback alone
    cannot see.
    """
    def walk(node, scope, parent_is_class):
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef):
                qname = scope + (child.name,)
                yield qname, child, parent_is_class
                # Inside the function body, further nested defs are nested,
                # not class methods, even if this function itself is one.
                yield from walk(child, qname, False)
            elif isinstance(child, ast.ClassDef):
                yield from walk(child, scope + (child.name,), True)
            else:
                yield from walk(child, scope, parent_is_class)
    yield from walk(tree, (module_label,), False)


def _called_names(node):
    """``(bare_names, attr_names)`` this function's *own* body invokes.

    Nested ``def``/``async def``/class bodies are their own graph nodes, so
    their calls belong to them — walking into them here would fold their
    reachability into the enclosing function and re-create the merge-by-name
    bug the qualified graph exists to avoid. Lambdas have no name and stay
    walked in.

    ``bare_names`` are the names invoked as bare ``Name`` nodes: a call
    ``_decide()`` and a bare Name argument (``_decide`` passed to
    ``under_prediction_decision_lock(db, _decide, ...)``). Python resolves a
    bare Name to a lexically enclosing scope, the caller's own module, or
    the caller's imports — never to a same-named module-level function in
    an unrelated module.

    ``attr_names`` are the names invoked as attribute access
    (``db.method(...)``, ``prediction_decisions.begin_prediction_decision(...)``).
    An attribute name can refer to a function anywhere: the receiver may be
    an imported module or a database instance whose method matches a
    mutator name, so the reachability walk matches ``.attr`` against every
    module-level function with that name.
    """
    bare_names = set()
    attr_names = set()

    def visit(current):
        for child in ast.iter_child_nodes(current):
            if isinstance(
                child, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef,
            ):
                continue
            if isinstance(child, ast.Call):
                func = child.func
                if isinstance(func, ast.Name):
                    bare_names.add(func.id)
                elif isinstance(func, ast.Attribute):
                    attr_names.add(func.attr)
                for arg in (*child.args, *(kw.value for kw in child.keywords)):
                    if isinstance(arg, ast.Name):
                        bare_names.add(arg.id)
            visit(child)

    visit(node)
    return bare_names, attr_names


def _imported_names(tree):
    """Bare names a module's ``import`` statements bring into scope.

    A bare Name call to one of these can resolve to a function defined in
    another scanned module (``from services.prediction_decisions import
    begin_prediction_decision`` makes ``begin_prediction_decision`` a bare
    Name whose target lives elsewhere). Names not imported here have no
    such cross-module target, and must resolve to something in the caller's
    own module or its lexical scopes.
    """
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if alias.name == "*":
                    continue
                imported.add(alias.asname or alias.name)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                # ``import a.b`` binds ``a`` (or the ``as`` alias) in scope.
                imported.add(alias.asname or alias.name.split(".")[0])
    return imported


def _call_graph(sources):
    """Build ``(call_map, routes, resolve)`` from module sources.

    Each function keeps its scope-qualified identity, so a helper named
    ``_decide`` in one blueprint stays distinct from a ``_decide`` in another.
    Merging by bare name would let an unlocked route calling the local
    ``_decide`` appear to reach ``begin_prediction_decision`` through an
    unrelated same-named function, so declaring the route would silence the
    "declared but never reaches lock" check on an unlocked writer.

    - ``call_map[qualified]`` — ``(bare, attr)`` sets of names ``qualified``'s
      own body invokes. ``bare`` is bare ``Name`` calls (and Name arguments
      passed as helpers) — these follow Python's own scoping and resolve
      inside the caller's module, its lexical scopes, or its imports.
      ``attr`` is ``.attr`` calls (``db.method(...)``,
      ``prediction_decisions.begin_prediction_decision(...)``) — these match
      any module-level function with that name, because the receiver may be
      an imported module or a database instance defined elsewhere. The
      target intersection in ``_reaches`` matches both sets against
      ``_PREDICTION_MUTATORS`` and ``_DECISION_LOCK`` without needing a
      graph node for the mutator or the lock.
    - ``routes[view_function_name]`` — list of ``(qualified, "file:line")`` for
      every place that route is defined. Keyed by bare name because
      ``PREDICTION_DECISION_ROUTES`` names view functions, and the contract
      test enforces that a decision route's name is unique across ``app.py``
      and ``vireo/web/``.
    - ``resolve(caller_qualified, name, is_bare)`` — set of qualified callees
      to recurse into. For a bare-Name call, walks the caller's lexical
      scope chain outward (a helper defined beside the route wins over a
      same-named helper elsewhere) and then, if the name is not imported
      here, restricts the module-level fallback to the caller's own module
      — two blueprints each defining a module-level ``_decide`` do not
      merge. When the name is imported into this module, or the call is an
      attribute call, any module-level function or class method with that
      name is a candidate: an attribute name may resolve to a service class
      method (``PhotoReviewService(db).set_flag(...)``) as well as to a
      module-level function.
    """
    call_map = {}
    routes = {}
    module_top = {}
    nested = {}
    class_methods = {}
    module_imports = {}

    for label, text, may_register_routes in sources:
        tree = ast.parse(text)
        module_imports[label] = _imported_names(tree)
        for qname, node, is_class_method in _collect_functions(label, tree):
            call_map[qname] = _called_names(node)
            if len(qname) == 2:
                module_top.setdefault(qname[-1], set()).add(qname)
            else:
                nested.setdefault((qname[:-1], qname[-1]), set()).add(qname)
                if is_class_method:
                    # A class method on ``ClassName``: ``ClassName(db).method``
                    # or ``ClassName.method`` from another module resolves
                    # here, past the same-module and lexical fallbacks that
                    # only see the caller's own scope.
                    class_methods.setdefault(qname[-1], set()).add(qname)
            if may_register_routes and _is_route(node):
                routes.setdefault(node.name, []).append(
                    (qname, f"{label}:{node.lineno}"),
                )

    def resolve(caller_qualified, name, is_bare):
        # Bare-Name call: walk lexical scopes outward. A caller's own inner
        # helpers (``def _apply`` inside a route body, captured through
        # ``under_prediction_decision_lock(db, _apply, ...)``) live at
        # ``caller_qualified`` itself; siblings sit one step out. Attribute
        # calls skip this step: ``obj.attr`` never resolves against local
        # ``def``s in the caller's frame.
        if is_bare:
            for depth in range(len(caller_qualified), 0, -1):
                found = nested.get((caller_qualified[:depth], name))
                if found:
                    return found
        caller_module = caller_qualified[0]
        if is_bare and name not in module_imports.get(caller_module, set()):
            # A bare Name call whose name isn't imported here can only
            # resolve inside the caller's own module. Two modules each
            # defining a bare ``_decide`` at module level do not merge:
            # an unlocked route calling its own ``_decide`` cannot reach a
            # locked ``_decide`` in a different blueprint through the graph.
            return {
                q for q in module_top.get(name, set())
                if q[0] == caller_module
            }
        # Attribute call, or a bare Name known to be imported here: match
        # any module-level function AND any class method with that name.
        # The class-method union is what lets a route calling
        # ``PhotoReviewService(db).set_flag(...)`` from a blueprint reach
        # into the service's ``set_flag``; without it, a future decision
        # route wrapped inside a service class would be invisible to the
        # graph and could silently write ``prediction_review`` without
        # being flagged as needing the lock.
        return module_top.get(name, set()) | class_methods.get(name, set())

    return call_map, routes, resolve


def _reaches(start, targets, call_map, resolve):
    """Can ``start`` (a qualified name) reach any bare name in ``targets``?"""
    seen = {start}
    stack = [start]
    while stack:
        current = stack.pop()
        bare, attrs = call_map.get(current, (set(), set()))
        if bare & targets or attrs & targets:
            return True
        for name in bare:
            for qualified in resolve(current, name, True):
                if qualified not in seen:
                    seen.add(qualified)
                    stack.append(qualified)
        for name in attrs:
            for qualified in resolve(current, name, False):
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


def test_decision_route_analysis_distinguishes_module_level_helpers():
    """Same bare-name distinguish rule, but at module scope.

    The nested-scope case above only exercises helpers defined inside a
    factory. The module-level fallback used to union every ``_decide``
    defined anywhere in the scanned tree, so an unlocked route in
    ``web/unlocked.py`` calling a module-level ``_decide`` would appear to
    reach ``begin_prediction_decision`` through an unrelated module-level
    ``_decide`` in ``web/locked.py``. Python's own scoping resolves a bare
    Name inside the caller's module (or its imports), never against a
    same-named function in an unrelated module, so the graph must not
    either.
    """
    unlocked = '''
def _decide(db):
    db.update_prediction_status(1, "rejected")


def create_unlocked_blueprint(get_db, json_error):
    bp = Blueprint("u", __name__)

    @bp.post("/api/u")
    def api_u():
        return _decide(get_db())

    return bp
'''
    locked = '''
from services import prediction_decisions


def _decide(db, json_error):
    lock_err = prediction_decisions.begin_prediction_decision(
        db, json_error=json_error,
    )
    if lock_err is not None:
        return lock_err
    db.update_prediction_status(2, "accepted")


def create_locked_blueprint(get_db, json_error):
    bp = Blueprint("l", __name__)

    @bp.post("/api/l")
    def api_l():
        return _decide(get_db(), json_error)

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


def test_decision_route_analysis_follows_service_class_methods():
    """``ClassName(...).method(...)`` reaches into that class's method.

    A service class defined in ``vireo/services/`` (``PhotoReviewService``,
    ``PhotoLabelService`` today) is instantiated and called from a blueprint
    as ``ClassName(db).method(...)``. Class methods live under the class
    scope, so bare-name lookups against ``module_top`` never see them: a
    future decision route that runs its writes through such a service would
    silently omit the lock without failing the structural test. Attribute-
    call resolution now unions class methods with matching name, so the
    reachability walk follows into the method and picks up its
    ``prediction_review`` writes.
    """
    service_module = '''
class DecisionService:
    def __init__(self, db):
        self.db = db

    def apply(self, pred_id):
        self.db.update_prediction_status(pred_id, "rejected")
'''
    blueprint = '''
from services.decisions import DecisionService


def create_bp(get_db, json_error):
    bp = Blueprint("d", __name__)

    @bp.post("/api/apply")
    def api_apply():
        return DecisionService(get_db()).apply(1)

    return bp
'''
    call_map, routes, resolve = _call_graph([
        ("services/decisions.py", service_module, False),
        ("web/d.py", blueprint, True),
    ])
    problems = _decision_route_problems(
        call_map, routes, resolve, set(),
    )
    assert any(
        p.startswith("api_apply ")
        and "writes prediction decisions but is not in PREDICTION_DECISION_ROUTES"
        in p
        for p in problems
    ), problems


def test_decision_route_analysis_follows_direct_imports_of_the_lock():
    """A bare Name call to an imported helper still crosses module boundaries.

    ``vireo/services/prediction_decisions.py`` documents that a caller may
    ``from services.prediction_decisions import begin_prediction_decision``
    and then invoke the lock as a bare ``begin_prediction_decision(...)``.
    The graph must follow that call even though the target lives in a
    different module, or a locked route reached this way would be flagged
    as unlocked.
    """
    caller = '''
from services.prediction_decisions import begin_prediction_decision


def create_locked_blueprint(get_db, json_error):
    bp = Blueprint("l", __name__)

    @bp.post("/api/l")
    def api_l():
        db = get_db()
        lock_err = begin_prediction_decision(db, json_error=json_error)
        if lock_err is not None:
            return lock_err
        db.update_prediction_status(1, "accepted")

    return bp
'''
    service = (VIREO_DIR / "services" / "prediction_decisions.py").read_text(
        encoding="utf-8",
    )
    call_map, routes, resolve = _call_graph([
        ("web/direct.py", caller, True),
        ("services/prediction_decisions.py", service, False),
    ])
    assert not _decision_route_problems(
        call_map, routes, resolve, {"api_l"},
    )


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
