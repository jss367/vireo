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


def _add_url_rule_view_names(tree):
    """Bare Names passed as ``view_func`` to ``<anything>.add_url_rule(...)``.

    Flask lets a route be registered without a decorator through
    ``bp.add_url_rule(rule, endpoint, view_func)`` (positional) or
    ``bp.add_url_rule(rule, view_func=view)`` (keyword); ``vireo/web/pages.py``
    already uses the positional form. Route discovery keyed only on
    ``@bp.<verb>`` decorators would miss a decision route registered this
    way: its name would be absent from ``routes``, so neither its
    ``prediction_review`` writes nor its missing lock would be checked, and
    an unlocked writer registered by ``add_url_rule`` would pass silently.
    Returning the referenced view names lets ``_call_graph`` add the
    corresponding function to ``routes`` alongside decorator-based ones.
    """
    names = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not (isinstance(func, ast.Attribute) and func.attr == "add_url_rule"):
            continue
        view = None
        for kw in node.keywords:
            if kw.arg == "view_func":
                view = kw.value
                break
        if view is None and len(node.args) >= 3:
            view = node.args[2]
        if isinstance(view, ast.Name):
            names.add(view.id)
    return names


def _iter_class_defs(module_label, tree):
    """Yield ``(class_qname, node)`` for every ``class`` defined in ``tree``.

    A class's qname is ``(module_label, ...enclosing_scope_names, class_name)``,
    the same form functions use, so the call graph can key methods by the
    class they were defined in rather than merging every ``apply`` across all
    scanned classes.
    """
    def walk(node, scope):
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.ClassDef):
                qname = scope + (child.name,)
                yield qname, child
                yield from walk(child, qname)
            elif isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef):
                yield from walk(child, scope + (child.name,))
            else:
                yield from walk(child, scope)
    yield from walk(tree, (module_label,))


def _collect_functions(module_label, tree):
    """Yield ``(qualified_name, node, class_qname_or_None)`` for every function.

    The qualified name is ``(module_label, ...enclosing_scope_names, own_name)``
    — the lexical path from the module root, with every enclosing ``def`` and
    ``class`` on the way in. A nested function stays a distinct node from a
    same-named function elsewhere, which is what a name-keyed graph loses.
    ``class_qname_or_None`` is the qname of the enclosing class when the
    immediately enclosing scope is a ``class``: a service method
    (``PhotoReviewService.set_flag``, ``DecisionService.apply``) that another
    module can invoke via ``ClassName(...).method(...)`` and that the
    module-level fallback alone cannot see. It carries the class's *identity*
    (its own qname), not just its bare name, so two different classes that
    expose the same method name (``WriterService.apply``,
    ``LockedService.apply``) stay distinct in the method table.
    """
    def walk(node, scope, enclosing_class_qname):
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef):
                qname = scope + (child.name,)
                yield qname, child, enclosing_class_qname
                # Inside the function body, further nested defs are nested,
                # not class methods, even if this function itself is one.
                yield from walk(child, qname, None)
            elif isinstance(child, ast.ClassDef):
                class_qname = scope + (child.name,)
                yield from walk(child, class_qname, class_qname)
            else:
                yield from walk(child, scope, enclosing_class_qname)
    yield from walk(tree, (module_label,), None)


def _called_names(node):
    """``(bare_names, attr_calls)`` this function's *own* body invokes.

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

    ``attr_calls`` are the ``.attr(...)`` invocations, kept as
    ``(attr_name, receiver_kind, receiver_id)`` triples. ``receiver_kind``
    is:

    - ``"name"`` — the receiver was a bare Name (``writer.apply``,
      ``db.update_prediction_status``, ``LockedService.apply``).
      ``receiver_id`` is that identifier; the resolver may recognize it as
      a scanned module the caller imported, or a class the caller can name.
    - ``"class"`` — the receiver was a direct class construction
      (``DecisionService(db).apply``, ``PhotoReviewService(get_db()).set_flag``).
      ``receiver_id`` is the class's bare name; the resolver locates that
      class by its identity (imported class symbol, or a class defined in
      this module) and dispatches to that specific class's method — not the
      union of every same-named method across every scanned class.
    - ``None`` — receiver is a chained expression or a call whose target
      isn't a bare Name (``obj.a.b.c()``, ``func()()``). ``receiver_id`` is
      ``None``. The resolver falls back to the same-named union for these.
    """
    bare_names = set()
    attr_calls = []

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
                    value = func.value
                    if isinstance(value, ast.Name):
                        attr_calls.append((func.attr, "name", value.id))
                    elif (
                        isinstance(value, ast.Call)
                        and isinstance(value.func, ast.Name)
                    ):
                        # ``ClassName(...).method(...)`` — a direct
                        # construction whose class we can name. The resolver
                        # uses that identity to dispatch to that class's
                        # own method rather than the union of every
                        # same-named method across every scanned class.
                        attr_calls.append((func.attr, "class", value.func.id))
                    else:
                        attr_calls.append((func.attr, None, None))
                for arg in (*child.args, *(kw.value for kw in child.keywords)):
                    if isinstance(arg, ast.Name):
                        bare_names.add(arg.id)
            visit(child)

    visit(node)
    return bare_names, attr_calls


def _imported_names(tree):
    """Bare names a module's ``import`` statements bring into scope.

    Returns ``{local_name: original_name_or_None}``. ``original_name`` is
    the symbol's name inside its defining module — ``from services.review
    import decide as apply_decision`` makes ``apply_decision`` a bare Name
    here whose target function is defined as ``decide`` in
    ``services.review``, so a bare-Name call to ``apply_decision()`` must
    resolve against ``decide`` in ``module_top``, not against
    ``apply_decision`` (which does not exist there). ``None`` marks a
    binding that names a module rather than a function (``import a.b`` or
    ``import a.b as c``): it cannot itself be called as a bare Name and
    reaches functions only through attribute access.

    Only imports at module scope count. A function-local ``from x import
    _decide`` used to enter this map through ``ast.walk``, so an unlocked
    route calling its own module-level ``_decide`` was then resolved
    against every same-named ``_decide`` in the scanned tree; a locked one
    in an unrelated module made the route look locked. Python's own scoping
    binds a function-local import only inside that function, so the graph
    must not extend it to the module either.
    """
    imported = {}

    def visit(node):
        for child in ast.iter_child_nodes(node):
            if isinstance(
                child, ast.FunctionDef | ast.AsyncFunctionDef
                | ast.ClassDef | ast.Lambda,
            ):
                # A ``def``/``class``/``lambda`` starts a new scope; imports
                # inside its body are bound there, not in the module.
                continue
            if isinstance(child, ast.ImportFrom):
                for alias in child.names:
                    if alias.name == "*":
                        continue
                    imported[alias.asname or alias.name] = alias.name
            elif isinstance(child, ast.Import):
                for alias in child.names:
                    # ``import a.b`` binds ``a`` (or the ``as`` alias) in
                    # scope; ``import a.b as c`` binds ``c``. Either way the
                    # bound name refers to a module, not a directly callable
                    # function, so there is no ``module_top`` target for a
                    # bare-Name call.
                    local = alias.asname or alias.name.split(".")[0]
                    imported[local] = None
            else:
                # ``if``, ``try``, ``with``, ``for`` at module scope: their
                # bodies still run at module import time, so imports inside
                # them bind at module scope.
                visit(child)

    visit(tree)
    return imported


def _module_dotted_forms(label):
    """Dotted import paths that could refer to the scanned file ``label``.

    ``vireo/`` is on ``sys.path`` (see ``vireo/tests/conftest.py``), so
    ``vireo/services/foo.py`` is imported as ``services.foo``. The synthetic
    sources in the analysis tests use bare labels like ``services/foo.py``,
    whose dotted form is also ``services.foo``. Yields both so a single
    lookup table serves the real tree and the synthetic tests.
    """
    stem = label[:-3] if label.endswith(".py") else label
    forms = {stem.replace("/", ".")}
    if stem.startswith("vireo/"):
        forms.add(stem[len("vireo/"):].replace("/", "."))
    return forms


def _module_aliases(tree, dotted_to_label):
    """Bare names that ``tree``'s imports bind to specific scanned modules.

    Lets ``receiver.attr(...)`` resolve inside the receiver's own module
    when the graph knows which module ``receiver`` is: ``from services
    import writer`` and ``import services.writer as writer`` both bind
    ``writer`` to ``services/writer.py``, so ``writer.apply(...)`` cannot
    reach an unrelated ``services/locked.py::apply`` through the union
    fallback. Names bound to a package (``import services.writer`` alone
    binds ``services``, the package), to something outside the scanned
    tree, or to a runtime instance (``db = get_db()``) are not aliases —
    the union fallback still handles them.
    """
    aliases = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            module_path = node.module or ""
            if not module_path:
                continue
            for alias in node.names:
                if alias.name == "*":
                    continue
                bound = alias.asname or alias.name
                sub = dotted_to_label.get(f"{module_path}.{alias.name}")
                if sub:
                    aliases[bound] = sub
        elif isinstance(node, ast.Import):
            for alias in node.names:
                # ``import a.b as z`` binds z to a.b; ``import a`` binds a.
                if alias.asname:
                    target = dotted_to_label.get(alias.name)
                    if target:
                        aliases[alias.asname] = target
                else:
                    top = alias.name.split(".")[0]
                    if "." not in alias.name:
                        target = dotted_to_label.get(top)
                        if target:
                            aliases[top] = target
    return aliases


def _imported_symbols(tree, dotted_to_label):
    """Bare names bound to a specific ``(module_label, original_name)`` symbol.

    Lets the resolver locate an imported class or function inside its
    defining module even when the ``import`` renames it: ``from
    services.decisions import DecisionService`` and ``from services.review
    import decide as apply_decision`` both bind names whose target lives in
    a scanned module and has a known original name there. ``_module_aliases``
    covers the *module* form (``from services import writer``) — this covers
    the *symbol* form. Only module-level imports count, matching
    ``_imported_names``: a function-local ``from services.decisions import
    DecisionService`` binds only inside that function.
    """
    bindings = {}

    def visit(node):
        for child in ast.iter_child_nodes(node):
            if isinstance(
                child, ast.FunctionDef | ast.AsyncFunctionDef
                | ast.ClassDef | ast.Lambda,
            ):
                continue
            if isinstance(child, ast.ImportFrom):
                module_path = child.module or ""
                if not module_path:
                    continue
                parent = dotted_to_label.get(module_path)
                if parent is None:
                    continue
                for alias in child.names:
                    if alias.name == "*":
                        continue
                    if dotted_to_label.get(
                        f"{module_path}.{alias.name}",
                    ) is not None:
                        # It's a submodule import (``from services import
                        # writer``); ``_module_aliases`` already handles it.
                        continue
                    bound = alias.asname or alias.name
                    bindings[bound] = (parent, alias.name)
            else:
                visit(child)

    visit(tree)
    return bindings


def _call_graph(sources):
    """Build ``(call_map, routes, resolve)`` from module sources.

    Each function keeps its scope-qualified identity, so a helper named
    ``_decide`` in one blueprint stays distinct from a ``_decide`` in another.
    Merging by bare name would let an unlocked route calling the local
    ``_decide`` appear to reach ``begin_prediction_decision`` through an
    unrelated same-named function, so declaring the route would silence the
    "declared but never reaches lock" check on an unlocked writer.

    - ``call_map[qualified]`` — ``(bare, attr_calls)`` from ``_called_names``.
      ``bare`` is bare ``Name`` calls (and Name arguments passed as helpers)
      — these follow Python's own scoping and resolve inside the caller's
      module, its lexical scopes, or its imports. ``attr_calls`` is a list of
      ``(attr_name, receiver_kind, receiver_id)`` triples for each
      ``receiver.attr(...)``. The target intersection in ``_reaches`` matches
      the bare-name set and the attribute-name set against
      ``_PREDICTION_MUTATORS`` and ``_DECISION_LOCK`` directly, without
      needing a graph node for the mutator or the lock.
    - ``routes[view_function_name]`` — list of ``(qualified, "file:line")`` for
      every place that route is defined. Keyed by bare name because
      ``PREDICTION_DECISION_ROUTES`` names view functions, and the contract
      test enforces that a decision route's name is unique across ``app.py``
      and ``vireo/web/``.
    - ``resolve(caller_qualified, name, is_bare, receiver=None,
      class_receiver=None)`` — set of qualified callees to recurse into.

      For a bare-Name call, walks the caller's lexical scope chain outward
      (a helper defined beside the route wins over a same-named helper
      elsewhere) and then, if the name is not imported here, restricts the
      module-level fallback to the caller's own module — two blueprints each
      defining a module-level ``_decide`` do not merge.

      For a ``class`` receiver (``DecisionService(db).apply(...)``), the
      resolver uses the class's identity — imported class symbol or a class
      defined in this module — and dispatches to that specific class's
      ``apply`` method. Two service classes with the same method name stay
      apart: a route calling ``WriterService(db).apply(...)`` cannot reach
      the ``apply`` of some unrelated ``LockedService`` through the
      same-named union.

      For a ``name`` receiver bound to a scanned module through an import
      (``writer.apply(...)`` where the caller's file has ``from services
      import writer``), the module-level and class-method fallbacks are
      restricted to that module. When the caller's Name refers to a class
      instead — either an imported class symbol or a class defined here —
      the resolver dispatches to that class's own method as it does for
      direct constructions, so ``LockedService.apply()`` in a class-attribute
      form is treated the same as ``LockedService(...).apply()``.

      For an unknown ``name`` receiver (a runtime instance like ``db``) or a
      chained expression, any module-level function or class method with
      that attribute name is a candidate. The class-method union is what
      lets a route calling ``self.repo.set_flag(...)`` reach into a service's
      method; without it, a future decision route wrapped inside a service
      class would be invisible to the graph and could silently write
      ``prediction_review`` without being flagged as needing the lock.
    """
    call_map = {}
    routes = {}
    module_top = {}
    nested = {}
    # ``class_methods_by_class[(module, class_name, method_name)]`` — every
    # method with that name on that exact class. Keyed by defining-class
    # identity so ``WriterService.apply`` and ``LockedService.apply`` don't
    # collapse into one node.
    class_methods_by_class = {}
    # ``class_methods[method_name]`` — union across every class. Used only
    # when the resolver cannot pin the receiver to a specific class
    # (attribute call on a runtime instance, or on a scanned module scope).
    class_methods = {}
    # ``module_classes[module_label]`` — top-level class names defined at
    # module scope in that file. Lets a receiver Name that names a locally
    # defined class dispatch to that class's own method table.
    module_classes = {}
    module_imports = {}
    module_alias = {}
    # ``module_symbol[label][local_name]`` — the ``(defining_module_label,
    # original_name)`` a module-level ``from x import y [as z]`` bound
    # ``local_name`` to, when ``x`` is a scanned module. Lets the resolver
    # pin an imported class or function to its defining module and original
    # name; ``module_alias`` handles the module form of the same problem.
    module_symbol = {}

    parsed = []
    dotted_to_label = {}
    for label, text, may_register_routes in sources:
        tree = ast.parse(text)
        parsed.append((label, tree, may_register_routes))
        for form in _module_dotted_forms(label):
            dotted_to_label[form] = label

    for label, tree, may_register_routes in parsed:
        module_imports[label] = _imported_names(tree)
        module_alias[label] = _module_aliases(tree, dotted_to_label)
        module_symbol[label] = _imported_symbols(tree, dotted_to_label)
        module_classes[label] = {
            class_qname[-1]
            for class_qname, _ in _iter_class_defs(label, tree)
            if len(class_qname) == 2
        }
        # Names registered as views by ``add_url_rule`` in this module. Any
        # function whose name matches counts as a route here, alongside
        # decorator-decorated ones. Confined to the same module because
        # ``add_url_rule`` binds a specific ``view_func`` from the caller's
        # scope; a same-named function elsewhere is unrelated.
        add_url_views = (
            _add_url_rule_view_names(tree) if may_register_routes else set()
        )
        for qname, node, class_qname in _collect_functions(label, tree):
            call_map[qname] = _called_names(node)
            if len(qname) == 2:
                module_top.setdefault(qname[-1], set()).add(qname)
            else:
                nested.setdefault((qname[:-1], qname[-1]), set()).add(qname)
                if class_qname is not None:
                    # A class method on ``ClassName``: track it by the
                    # class's identity so ``ClassName(db).method()`` and
                    # ``ClassName.method()`` reach here past the same-module
                    # and lexical fallbacks, while remaining separate from a
                    # same-named method on some unrelated class.
                    class_methods_by_class.setdefault(
                        (class_qname[0], class_qname[-1], qname[-1]), set(),
                    ).add(qname)
                    class_methods.setdefault(qname[-1], set()).add(qname)
            if may_register_routes and (
                _is_route(node) or node.name in add_url_views
            ):
                routes.setdefault(node.name, []).append(
                    (qname, f"{label}:{node.lineno}"),
                )

    def _class_identity_for(caller_module, receiver_name):
        """Which class, if any, does ``receiver_name`` name in this module?

        Returns ``(class_module, class_name)`` when the receiver is an
        imported class symbol from another scanned module (``from
        services.decisions import DecisionService [as X]``) or a class
        defined at module level in the caller's own file, else ``None``.
        Only classes the graph can identify get pinned to a single method
        table.
        """
        sym = module_symbol.get(caller_module, {}).get(receiver_name)
        if sym is not None:
            sym_module, original = sym
            if original in module_classes.get(sym_module, set()):
                return sym_module, original
        if receiver_name in module_classes.get(caller_module, set()):
            return caller_module, receiver_name
        return None

    def resolve(
        caller_qualified, name, is_bare, receiver=None, class_receiver=None,
    ):
        caller_module = caller_qualified[0]
        # ``ClassName(...).method(...)``: dispatch to that class's own method.
        if class_receiver is not None:
            cls = _class_identity_for(caller_module, class_receiver)
            if cls is not None:
                return class_methods_by_class.get(
                    (cls[0], cls[1], name), set(),
                )
            # Class isn't a scanned identity — over-approximate rather than
            # miss a decision route. Union across every same-named method.
            return class_methods.get(name, set())
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
        caller_imports = module_imports.get(caller_module, {})
        if is_bare and name not in caller_imports:
            # A bare Name call whose name isn't imported here can only
            # resolve inside the caller's own module. Two modules each
            # defining a bare ``_decide`` at module level do not merge:
            # an unlocked route calling its own ``_decide`` cannot reach a
            # locked ``_decide`` in a different blueprint through the graph.
            return {
                q for q in module_top.get(name, set())
                if q[0] == caller_module
            }
        if not is_bare and receiver is not None:
            # A Name receiver that itself names a class (imported class
            # symbol or a locally defined class): ``LockedService.apply()``
            # dispatches to that class's own method just like the direct
            # construction form.
            cls = _class_identity_for(caller_module, receiver)
            if cls is not None:
                return class_methods_by_class.get(
                    (cls[0], cls[1], name), set(),
                )
            receiver_module = module_alias.get(caller_module, {}).get(receiver)
            if receiver_module is not None:
                # ``receiver.attr(...)`` where ``receiver`` is a scanned
                # module the caller imported: an unrelated same-named
                # function in another scanned module is not on the actual
                # call path, and unioning it in would let a route reach a
                # lock (or a mutator) it doesn't actually reach. Restrict
                # both the module-level and class-method fallbacks to the
                # receiver module.
                return {
                    q for q in module_top.get(name, set())
                    if q[0] == receiver_module
                } | {
                    q for q in class_methods.get(name, set())
                    if q[0] == receiver_module
                }
        # Attribute call with an unknown or runtime receiver, or a bare Name
        # known to be imported here: match any module-level function AND any
        # class method with that name. For a bare Name that is an aliased
        # import (``from services.review import decide as apply_decision``),
        # look the target up under the imported symbol's *original* name —
        # the scanned definition is ``decide``, so ``apply_decision`` would
        # otherwise miss it and an unlocked route calling ``apply_decision``
        # would appear to reach no writer. The class-method union is what
        # lets a route calling ``PhotoReviewService(db).set_flag(...)`` from
        # a blueprint reach into the service's ``set_flag``; without it, a
        # future decision route wrapped inside a service class would be
        # invisible to the graph and could silently write
        # ``prediction_review`` without being flagged as needing the lock.
        if is_bare:
            target = caller_imports[name]
            if target is None:
                # ``import module`` binds a module, not a function: it has
                # no bare-Name target of its own.
                return set()
            lookup = target
        else:
            lookup = name
        return module_top.get(lookup, set()) | class_methods.get(lookup, set())

    return call_map, routes, resolve


def _reaches(start, targets, call_map, resolve):
    """Can ``start`` (a qualified name) reach any bare name in ``targets``?"""
    seen = {start}
    stack = [start]
    while stack:
        current = stack.pop()
        bare, attr_calls = call_map.get(current, (set(), []))
        if bare & targets:
            return True
        if any(name in targets for name, *_ in attr_calls):
            return True
        for name in bare:
            for qualified in resolve(current, name, True):
                if qualified not in seen:
                    seen.add(qualified)
                    stack.append(qualified)
        for name, receiver_kind, receiver_id in attr_calls:
            receiver = receiver_id if receiver_kind == "name" else None
            class_receiver = (
                receiver_id if receiver_kind == "class" else None
            )
            for qualified in resolve(
                current, name, False,
                receiver=receiver, class_receiver=class_receiver,
            ):
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


def test_decision_route_analysis_follows_aliased_imports():
    """``from x import decide as apply_decision`` still finds ``decide``.

    When a blueprint imports a mutating helper under an alias, the local
    name in the caller's module is the alias, but the scanned function is
    defined under its original name in the source module. A bare-Name call
    to the alias must resolve to the original definition; otherwise the
    graph loses the edge and an unlocked route calling the alias never
    reaches its writer, silencing this test.
    """
    writer_module = '''
def decide(db):
    db.update_prediction_status(1, "rejected")
'''
    blueprint = '''
from services.writer import decide as apply_decision


def create_bp(get_db, json_error):
    bp = Blueprint("bp", __name__)

    @bp.post("/api/a")
    def api_a():
        return apply_decision(get_db())

    return bp
'''
    call_map, routes, resolve = _call_graph([
        ("services/writer.py", writer_module, False),
        ("web/aliased.py", blueprint, True),
    ])
    problems = _decision_route_problems(
        call_map, routes, resolve, set(),
    )
    assert any(
        p.startswith("api_a ")
        and "writes prediction decisions but is not in PREDICTION_DECISION_ROUTES"
        in p
        for p in problems
    ), problems


def test_decision_route_analysis_ignores_function_local_imports():
    """A function-local ``import`` binds only inside that function's scope.

    ``ast.walk`` used to gather every ``import`` node, including those inside
    unrelated function bodies. A module-level fallback that treated a name
    as imported here just because *some* function in this module imported
    it lost the two-module distinguish rule: a bare ``_decide`` call from
    an unlocked route would then union with a locked ``_decide`` in another
    module through the module-level fallback, and declaring the route would
    silence the "declared but never reaches lock" check.

    Here ``web/unlocked.py`` has a function-local ``from services.locked
    import _decide`` in an unrelated function, but the route calls its own
    module-level ``_decide`` that does not lock. The graph must not merge
    with the locked ``_decide`` in ``services/locked.py`` through that
    nested import.
    """
    unlocked = '''
def _decide(db):
    db.update_prediction_status(1, "rejected")


def _unrelated():
    # A function-local import that must NOT mark ``_decide`` as imported
    # at module scope.
    from services.locked import _decide as _reserved  # noqa: F401


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
'''
    service = (VIREO_DIR / "services" / "prediction_decisions.py").read_text(
        encoding="utf-8",
    )
    call_map, routes, resolve = _call_graph([
        ("web/unlocked.py", unlocked, True),
        ("services/locked.py", locked, False),
        ("services/prediction_decisions.py", service, False),
    ])
    problems = _decision_route_problems(
        call_map, routes, resolve, {"api_u"},
    )
    assert any(
        p.startswith("api_u ") and "never reaches begin_prediction_decision" in p
        for p in problems
    ), problems


def test_decision_route_analysis_recognizes_add_url_rule():
    """``bp.add_url_rule(rule, endpoint, view_func)`` registers a route too.

    Flask lets a blueprint attach a view function without a decorator; the
    pages blueprint (``vireo/web/pages.py``) already uses this form for its
    template routes. Route discovery keyed only on ``@bp.<verb>`` decorators
    would then miss a prediction-decision view registered this way: its
    name would be absent from ``routes``, so neither its
    ``prediction_review`` writes nor its missing lock would be checked, and
    an unlocked writer registered by ``add_url_rule`` would pass silently.
    """
    positional = '''
def api_positional():
    from db import get_db
    return get_db().update_prediction_status(1, "rejected")


def _install(bp):
    bp.add_url_rule("/api/pos", "pos", api_positional)
'''
    keyword = '''
def api_keyword():
    from db import get_db
    return get_db().update_prediction_status(2, "accepted")


def _install(bp):
    bp.add_url_rule("/api/kw", view_func=api_keyword)
'''
    call_map, routes, resolve = _call_graph([
        ("web/positional.py", positional, True),
        ("web/keyword.py", keyword, True),
    ])
    assert "api_positional" in routes, routes
    assert "api_keyword" in routes, routes
    problems = _decision_route_problems(
        call_map, routes, resolve, set(),
    )
    assert any(
        p.startswith("api_positional ")
        and "writes prediction decisions but is not in PREDICTION_DECISION_ROUTES"
        in p
        for p in problems
    ), problems
    assert any(
        p.startswith("api_keyword ")
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


def test_decision_route_analysis_pins_module_qualified_attribute_calls():
    """``receiver.attr(...)`` resolves inside the receiver's own module.

    Two service modules each define a module-level ``apply``:
    ``services/writer.py::apply`` mutates ``prediction_review`` without
    locking, ``services/locked.py::apply`` reaches ``begin_prediction_decision``.
    A blueprint imports ``writer`` and calls ``writer.apply(get_db())`` —
    unambiguously the writer's ``apply``, and unambiguously unlocked. A
    resolver that unions every scanned ``apply`` at module level for the
    attribute-call fallback lets the walk reach the lock through the
    unrelated ``locked.apply``, so declaring the route silences the
    "declared but never reaches lock" check even though the route's actual
    path never locks. Using the receiver's own module — via the caller's
    ``from services import writer`` binding — keeps that signal.
    """
    writer = '''
def apply(db):
    db.update_prediction_status(1, "rejected")
'''
    locked_service = '''
from services import prediction_decisions


def apply(db, json_error):
    return prediction_decisions.under_prediction_decision_lock(
        db, lambda: db.update_prediction_status(2, "accepted"),
        json_error=json_error,
    )
'''
    blueprint = '''
from services import writer


def create_writer_blueprint(get_db, json_error):
    bp = Blueprint("w", __name__)

    @bp.post("/api/w")
    def api_w():
        return writer.apply(get_db())

    return bp
'''
    service = (VIREO_DIR / "services" / "prediction_decisions.py").read_text(
        encoding="utf-8",
    )
    call_map, routes, resolve = _call_graph([
        ("web/writer_route.py", blueprint, True),
        ("services/writer.py", writer, False),
        ("services/locked.py", locked_service, False),
        ("services/prediction_decisions.py", service, False),
    ])
    # api_w reaches a mutator through writer.apply, so it's flagged as a
    # writer when undeclared, and once declared it must reach the lock —
    # which writer.apply does not. The union-only resolver would have
    # silenced this second check by reaching the lock via locked.apply.
    problems = _decision_route_problems(
        call_map, routes, resolve, set(),
    )
    assert any(
        p.startswith("api_w ")
        and "writes prediction decisions but is not in PREDICTION_DECISION_ROUTES"
        in p
        for p in problems
    ), problems
    problems = _decision_route_problems(
        call_map, routes, resolve, {"api_w"},
    )
    assert any(
        p.startswith("api_w ")
        and "never reaches begin_prediction_decision" in p
        for p in problems
    ), problems


def test_decision_route_analysis_distinguishes_service_classes():
    """``ClassName(...).method(...)`` dispatches to that class's own method.

    Two service classes each expose a method named ``apply``:
    ``services/writer.py::WriterService.apply`` writes a prediction status
    without locking, ``services/locked.py::LockedService.apply`` reaches
    ``begin_prediction_decision``. A blueprint constructs one and calls
    ``WriterService(db).apply(...)`` — unambiguously unlocked. A resolver
    that unioned every same-named class method under one node would let the
    walk reach the lock through the unrelated ``LockedService.apply``, so
    declaring the route would silence the "declared but never reaches lock"
    check even though the route's actual path never locks. Keying methods by
    their defining class's identity (module + class name) — and using the
    receiver's class identity (imported symbol or a class defined in this
    module) to dispatch — keeps the two apart.

    Also covers the class-attribute form ``LockedService.apply(...)``: the
    receiver is a bare Name, but the resolver still recognizes it as a class
    and dispatches to that specific class's method rather than the union.
    """
    writer = '''
class WriterService:
    def __init__(self, db):
        self.db = db

    def apply(self, pred_id):
        self.db.update_prediction_status(pred_id, "rejected")
'''
    locked_service = '''
from services import prediction_decisions


class LockedService:
    def __init__(self, db, json_error):
        self.db = db
        self.json_error = json_error

    @staticmethod
    def apply(db, json_error):
        return prediction_decisions.under_prediction_decision_lock(
            db,
            lambda: db.update_prediction_status(2, "accepted"),
            json_error=json_error,
        )
'''
    blueprint = '''
from services.writer import WriterService
from services.locked import LockedService


def create_writer_blueprint(get_db, json_error):
    bp = Blueprint("w", __name__)

    @bp.post("/api/w")
    def api_w():
        return WriterService(get_db()).apply(1)

    @bp.post("/api/locked_attr")
    def api_locked_attr():
        return LockedService.apply(get_db(), json_error)

    return bp
'''
    service = (VIREO_DIR / "services" / "prediction_decisions.py").read_text(
        encoding="utf-8",
    )
    call_map, routes, resolve = _call_graph([
        ("web/writer_route.py", blueprint, True),
        ("services/writer.py", writer, False),
        ("services/locked.py", locked_service, False),
        ("services/prediction_decisions.py", service, False),
    ])
    # api_w reaches a mutator through WriterService.apply, so it's flagged
    # as an undeclared writer. Once declared, it must reach the lock —
    # which WriterService.apply does not. The class-name-only union
    # resolver would have silenced this second check by reaching the lock
    # through the unrelated LockedService.apply.
    problems = _decision_route_problems(
        call_map, routes, resolve, set(),
    )
    assert any(
        p.startswith("api_w ")
        and "writes prediction decisions but is not in PREDICTION_DECISION_ROUTES"
        in p
        for p in problems
    ), problems
    problems = _decision_route_problems(
        call_map, routes, resolve, {"api_w", "api_locked_attr"},
    )
    assert any(
        p.startswith("api_w ")
        and "never reaches begin_prediction_decision" in p
        for p in problems
    ), problems
    # api_locked_attr goes through LockedService.apply via the class name,
    # not an instance. The resolver should follow that into the lock, so
    # declaring it must not produce a "declared but never reaches lock"
    # problem.
    assert not any(
        p.startswith("api_locked_attr ")
        for p in problems
    ), problems


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
