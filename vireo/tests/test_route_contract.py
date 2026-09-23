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


def _class_base_refs(class_node):
    """Bases named on a ``class Foo(...)`` header, in inheritance order.

    Yields one reference per base the resolver can chase later:

    - ``("name", "Base")`` — bare Name in the header. Resolves through
      the class's own module's imports (an imported class symbol) or a
      class defined at module scope in the same file.
    - ``("attr", "mod", "Base")`` — ``mod.Base`` in the header. Pins
      ``Base`` to the module ``mod`` names, again via the class's own
      module's imports.

    Anything else (a call, a subscription like ``Generic[T]``, a chained
    attribute) is skipped: the graph can only follow bases it can name to
    a scanned class identity, and unioning across every same-named class
    for an unresolvable base is exactly the merge the exact-class lookup
    exists to prevent.
    """
    for base in class_node.bases:
        if isinstance(base, ast.Name):
            yield ("name", base.id)
        elif isinstance(base, ast.Attribute) and isinstance(
            base.value, ast.Name,
        ):
            yield ("attr", base.value.id, base.attr)


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
    - ``"module_class"`` — the receiver was a module-qualified class
      construction (``writer.WriterService(db).apply``,
      ``services.decisions.DecisionService(db).apply``). ``receiver_id``
      is the ``(module_alias, class_name)`` pair; the resolver looks the
      alias up in the caller's imports to pin the class to its defining
      module, then dispatches to that specific class's method. Without
      this the module-qualified construction falls into the ``None``
      union and a route calling an unlocked ``writer.WriterService.apply``
      would silently reach the lock through an unrelated same-named
      method on another class.
    - ``None`` — receiver is a chained expression or a call whose target
      isn't a bare Name or a simple ``mod.Class`` (``obj.a.b.c()``,
      ``func()()``, ``pkg.sub.mod.Class(...)``). ``receiver_id`` is
      ``None``. The resolver falls back to the same-named union for
      these.
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
                    elif (
                        isinstance(value, ast.Call)
                        and isinstance(value.func, ast.Attribute)
                        and isinstance(value.func.value, ast.Name)
                    ):
                        # ``mod.ClassName(...).method(...)`` — a module-
                        # qualified construction. Both the receiver
                        # module alias and the class name are scanned
                        # identities: the resolver pins the class through
                        # the caller's import of ``mod`` and dispatches
                        # to that class's own method rather than the
                        # union across every same-named method.
                        attr_calls.append(
                            (
                                func.attr,
                                "module_class",
                                (value.func.value.id, value.func.attr),
                            ),
                        )
                    else:
                        attr_calls.append((func.attr, None, None))
                for arg in (*child.args, *(kw.value for kw in child.keywords)):
                    if isinstance(arg, ast.Name):
                        bare_names.add(arg.id)
            visit(child)

    visit(node)
    return bare_names, attr_calls


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


def _scoped_imports(tree, dotted_to_label):
    """``{scope: {local_name: (label_or_None, original_or_None)}}``.

    One record per lexical binding an ``import`` statement introduces,
    filed under the scope the binding is visible in. ``scope`` is the
    lexical path from the module root — ``()`` at module scope,
    ``(func,)`` inside ``func``, ``(cls, method)`` inside a method — so a
    function-local ``from services.writer import decide`` stays under that
    function's scope and never bleeds into the module. Python binds an
    ``import`` statement only inside the scope that runs it, and the graph
    must do the same: a module-wide collection re-enables the two-module
    merge that scope-qualified identities exist to prevent (an unlocked
    route calling its own module-level ``_decide`` would resolve against
    a locked ``_decide`` in another module through some unrelated
    function's local import). It also lets a route with a lazy ``from
    services.writer import decide`` inside its own body actually reach
    that decide, which a module-scope-only map dropped.

    Value shape ``(label, original)`` unifies the three imports flavors
    the resolver needs:

    - ``(label, original)`` with ``original`` not ``None`` — ``from X
      import original [as local]`` where ``X`` is a scanned module
      ``label``. Serves bare-Name lookups (resolve to ``original`` inside
      ``label``) and imported class-symbol identity (is ``local`` a
      scanned class in ``label``?). The module-restricted lookup keeps
      ``from services.writer import apply`` from merging with an
      unrelated ``services/locked.py::apply`` in the union.
    - ``(None, original)`` with ``original`` not ``None`` — same shape
      but ``X`` is external. The bare-Name call can't be resolved inside
      the scanned tree, so the walk stops here rather than falling back
      to any same-named function anywhere.
    - ``(label, None)`` — ``local`` names a scanned module (``import X
      as local`` where ``X`` is scanned, or ``from parent import
      submodule`` where ``parent.submodule`` is scanned). An attribute
      call ``local.attr(...)`` restricts inside ``label``, so
      ``writer.apply`` can't silently reach an unrelated
      ``services/locked.py::apply`` through the union.
    - ``(None, None)`` — the binding names a package or an external
      module. Neither a bare-Name call nor an attribute call resolves
      through it in the scanned tree, but the binding is still recorded
      so ``lookup_binding`` reports the name as bound.
    """
    imports = {}

    def visit(node, scope):
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef):
                # A ``def`` starts a new scope; recurse under its own name
                # so imports inside its body bind there, not in the caller.
                visit(child, scope + (child.name,))
            elif isinstance(child, ast.ClassDef):
                visit(child, scope + (child.name,))
            elif isinstance(child, ast.Lambda):
                # Lambda bodies are expressions and cannot contain imports.
                continue
            elif isinstance(child, ast.ImportFrom):
                module_path = child.module or ""
                if not module_path:
                    continue
                module_label = dotted_to_label.get(module_path)
                for alias in child.names:
                    if alias.name == "*":
                        continue
                    bound = alias.asname or alias.name
                    # ``from X import Y`` may name a submodule of X rather
                    # than a function or class: ``from services import
                    # writer`` binds ``writer`` to ``services/writer.py``.
                    sub_label = dotted_to_label.get(
                        f"{module_path}.{alias.name}",
                    )
                    if sub_label is not None:
                        imports.setdefault(scope, {})[bound] = (
                            sub_label, None,
                        )
                    else:
                        imports.setdefault(scope, {})[bound] = (
                            module_label, alias.name,
                        )
            elif isinstance(child, ast.Import):
                for alias in child.names:
                    if alias.asname:
                        # ``import a.b as z``: z is a module binding.
                        label = dotted_to_label.get(alias.name)
                        imports.setdefault(scope, {})[alias.asname] = (
                            label, None,
                        )
                    elif "." not in alias.name:
                        # ``import a``: bare a is a module binding.
                        label = dotted_to_label.get(alias.name)
                        imports.setdefault(scope, {})[alias.name] = (
                            label, None,
                        )
                    else:
                        # ``import a.b``: Python binds ``a`` (the package),
                        # which is not itself a scanned module. Record the
                        # binding so the bare-Name fallback knows the name
                        # is imported (not the caller's module) but has no
                        # scanned target — attribute chains through it fall
                        # to the union.
                        top = alias.name.split(".")[0]
                        imports.setdefault(scope, {})[top] = (None, None)
            else:
                # ``if``, ``try``, ``with``, ``for`` at any scope: their
                # bodies run under that same lexical scope, so imports
                # inside bind there.
                visit(child, scope)

    visit(tree, ())
    return imports


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
      elsewhere), then looks the name up in ``_scoped_imports`` scope-by-
      scope outward: a function-local ``from services.writer import
      decide`` binds only for the caller's own lexical chain, and a
      module-level ``from services.writer import apply`` restricts the
      module fallback to ``services.writer`` so it cannot union with an
      unrelated ``services/locked.py::apply``. When the name is not
      imported at any lexical scope on the way out, the module-level
      fallback is restricted to the caller's own module — two blueprints
      each defining a module-level ``_decide`` do not merge.

      For a ``class`` receiver (``DecisionService(db).apply(...)``), the
      resolver uses the class's identity — imported class symbol or a
      class defined in this module, resolved through the same lexical
      scope walk — and dispatches to that specific class's ``apply``
      method. Two service classes with the same method name stay apart:
      a route calling ``WriterService(db).apply(...)`` cannot reach the
      ``apply`` of some unrelated ``LockedService`` through the
      same-named union.

      For a ``name`` receiver bound to a scanned module through an import
      at any lexical scope (``writer.apply(...)`` where the caller's file
      has ``from services import writer``, or a function-local ``import
      services.writer as w`` for ``w.apply(...)``), the module-level and
      class-method fallbacks are restricted to that module. When the
      caller's Name refers to a class instead — either an imported class
      symbol or a class defined here — the resolver dispatches to that
      class's own method as it does for direct constructions, so
      ``LockedService.apply()`` in a class-attribute form is treated the
      same as ``LockedService(...).apply()``.

      For an unknown ``name`` receiver (a runtime instance like ``db``)
      or a chained expression, any module-level function or class method
      with that attribute name is a candidate. The class-method union is
      what lets a route calling ``self.repo.set_flag(...)`` reach into a
      service's method; without it, a future decision route wrapped
      inside a service class would be invisible to the graph and could
      silently write ``prediction_review`` without being flagged as
      needing the lock.
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
    # ``method_class[qname]`` — the defining class's ``(module, class_name)``
    # for a function that is itself a method. Lets the resolver dispatch
    # ``self.method()`` and ``cls.method()`` from inside a method to that
    # exact class's method table (including inherited methods via
    # ``_class_method_lookup``) rather than the same-named union across
    # every scanned class. Without this a service method delegating
    # through ``self.finish()`` would fall into the union, so an unlocked
    # writer's ``self.finish()`` could reach a lock through an unrelated
    # class's ``finish`` and silence the "declared but never reaches
    # lock" check.
    method_class = {}
    # ``module_classes[module_label]`` — top-level class names defined at
    # module scope in that file. Lets a receiver Name that names a locally
    # defined class dispatch to that class's own method table.
    module_classes = {}
    # ``class_bases[(module_label, class_name)]`` — bases named on the
    # class header, as ``_class_base_refs`` returns them. Used to walk up
    # a class's inheritance chain when the exact-class method table has
    # no entry: an inherited mutator would otherwise disappear from
    # reachability, letting an undeclared and unlocked decision route on
    # a subclass pass the contract test.
    class_bases = {}
    # ``scoped_imports[label]`` — ``{scope: {local: (label, original)}}``
    # for that module's imports at every lexical scope. Serves all three
    # things earlier module-wide maps did (bare-Name imports, receiver-
    # module aliases, imported class-symbol identity), keyed lexically so
    # a function-local ``from x import y`` binds only inside that
    # function.
    scoped_imports = {}

    parsed = []
    dotted_to_label = {}
    for label, text, may_register_routes in sources:
        tree = ast.parse(text)
        parsed.append((label, tree, may_register_routes))
        for form in _module_dotted_forms(label):
            dotted_to_label[form] = label

    for label, tree, may_register_routes in parsed:
        scoped_imports[label] = _scoped_imports(tree, dotted_to_label)
        module_classes[label] = {
            class_qname[-1]
            for class_qname, _ in _iter_class_defs(label, tree)
            if len(class_qname) == 2
        }
        for class_qname, class_node in _iter_class_defs(label, tree):
            # Only module-level classes participate in class dispatch:
            # nested classes have no cross-module identity the resolver
            # can name.
            if len(class_qname) != 2:
                continue
            class_bases[(class_qname[0], class_qname[-1])] = list(
                _class_base_refs(class_node),
            )
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
                    # Remember which class defined this method so the
                    # resolver can dispatch its own ``self`` / ``cls``
                    # method calls back into the same class's method table.
                    method_class[qname] = (class_qname[0], class_qname[-1])
            if may_register_routes and (
                _is_route(node) or node.name in add_url_views
            ):
                routes.setdefault(node.name, []).append(
                    (qname, f"{label}:{node.lineno}"),
                )

    def _lookup_binding(caller_qualified, key):
        """Walk lexical scopes outward looking for a binding for ``key``.

        Deepest scope wins: a function-local ``from services.writer import
        decide`` shadows a module-level one, matching Python's own scoping.
        Returns the first ``(label, original)`` tuple found, or ``None``
        when ``key`` isn't imported at any scope on the call's lexical
        path.
        """
        caller_module = caller_qualified[0]
        scope_map = scoped_imports.get(caller_module, {})
        caller_scope = caller_qualified[1:]
        for depth in range(len(caller_scope), -1, -1):
            binding = scope_map.get(caller_scope[:depth], {}).get(key)
            if binding is not None:
                return binding
        return None

    def _class_identity_for(caller_qualified, receiver_name):
        """Which class, if any, does ``receiver_name`` name for this caller?

        Returns ``(class_module, class_name)`` when the receiver is an
        imported class symbol from another scanned module (``from
        services.decisions import DecisionService [as X]``, at module
        scope or lazily inside the caller's own function) or a class
        defined at module level in the caller's own file, else ``None``.
        Only classes the graph can identify get pinned to a single method
        table.
        """
        binding = _lookup_binding(caller_qualified, receiver_name)
        if binding is not None:
            sym_module, original = binding
            if (
                sym_module is not None
                and original is not None
                and original in module_classes.get(sym_module, set())
            ):
                return sym_module, original
        caller_module = caller_qualified[0]
        if receiver_name in module_classes.get(caller_module, set()):
            return caller_module, receiver_name
        return None

    def _module_class_identity_for(caller_qualified, mod_alias, class_name):
        """A ``mod.Class`` pair from an attribute construction.

        Returns ``(class_module, class_name)`` when ``mod_alias`` is
        bound to a scanned module (through the caller's imports at any
        lexical scope) and that module defines a class named
        ``class_name`` at module scope. Otherwise ``None`` — the walk
        falls back to the same-named-method union like any other
        unknown-receiver call.
        """
        binding = _lookup_binding(caller_qualified, mod_alias)
        if binding is None:
            return None
        module_label, original = binding
        if (
            module_label is not None
            and original is None
            and class_name in module_classes.get(module_label, set())
        ):
            return module_label, class_name
        return None

    def _resolve_base(class_module, base_ref):
        """A base class ref from ``_class_base_refs`` to a class identity.

        Bases are resolved in the *defining* class's module, not the
        caller's: ``class Sub(Base):`` in ``services/writer.py`` looks
        ``Base`` up through ``services/writer.py``'s own imports and
        top-level classes, the same way Python does.
        """
        if base_ref[0] == "name":
            _, base_name = base_ref
            scope_map = scoped_imports.get(class_module, {})
            binding = scope_map.get((), {}).get(base_name)
            if binding is not None:
                sym_module, original = binding
                if (
                    sym_module is not None
                    and original is not None
                    and original in module_classes.get(sym_module, set())
                ):
                    return sym_module, original
            if base_name in module_classes.get(class_module, set()):
                return class_module, base_name
            return None
        _, mod_alias, base_name = base_ref
        scope_map = scoped_imports.get(class_module, {})
        binding = scope_map.get((), {}).get(mod_alias)
        if binding is None:
            return None
        module_label, original = binding
        if (
            module_label is not None
            and original is None
            and base_name in module_classes.get(module_label, set())
        ):
            return module_label, base_name
        return None

    def _class_method_lookup(class_module, class_name, method_name):
        """Method table for ``class_module::class_name.method_name``.

        Walks up ``class_bases`` to include inherited methods when the
        exact class does not define ``method_name``. A method defined on
        both the subclass and a base is not shadowed — the graph is a
        reachability set, not a runtime dispatch, and the subclass's own
        method may still delegate to the base's via ``super()``. Cycle-
        protected in case a synthetic test defines a base loop.
        """
        seen = set()
        stack = [(class_module, class_name)]
        found = set()
        while stack:
            key = stack.pop()
            if key in seen:
                continue
            seen.add(key)
            found |= class_methods_by_class.get(
                (key[0], key[1], method_name), set(),
            )
            for base_ref in class_bases.get(key, ()):
                base = _resolve_base(key[0], base_ref)
                if base is not None:
                    stack.append(base)
        return found

    def resolve(
        caller_qualified,
        name,
        is_bare,
        receiver=None,
        class_receiver=None,
        module_class_receiver=None,
    ):
        caller_module = caller_qualified[0]
        # ``mod.ClassName(...).method(...)``: pin the class through
        # ``mod``'s import binding, then dispatch to that specific class's
        # method (including inherited ones).
        if module_class_receiver is not None:
            mod_alias, class_name = module_class_receiver
            cls = _module_class_identity_for(
                caller_qualified, mod_alias, class_name,
            )
            if cls is not None:
                return _class_method_lookup(cls[0], cls[1], name)
            # Alias isn't a scanned module or the class isn't defined
            # there. Over-approximate rather than lose a decision route.
            return class_methods.get(name, set())
        # ``ClassName(...).method(...)``: dispatch to that class's own method.
        if class_receiver is not None:
            cls = _class_identity_for(caller_qualified, class_receiver)
            if cls is not None:
                return _class_method_lookup(cls[0], cls[1], name)
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
        if is_bare:
            binding = _lookup_binding(caller_qualified, name)
            if binding is None:
                # A bare Name call whose name isn't imported at any
                # lexical scope on this call's path can only resolve
                # inside the caller's own module. Two modules each
                # defining a bare ``_decide`` at module level do not
                # merge: an unlocked route calling its own ``_decide``
                # cannot reach a locked ``_decide`` in a different
                # blueprint through the graph.
                return {
                    q for q in module_top.get(name, set())
                    if q[0] == caller_module
                }
            label, original = binding
            if original is None:
                # Binding names a module rather than a callable
                # (``import X``, ``import X as Y``, or ``from X import
                # submodule``): a bare-Name call to it isn't meaningful
                # in the scanned tree.
                return set()
            if label is None:
                # ``from external_package import name`` — the graph
                # doesn't scan that module, so the call can't be traced
                # further. Falling back to ``module_top[original]`` here
                # would re-enable the module merge the label restriction
                # exists to prevent.
                return set()
            # Restrict the module-level and class-method fallbacks to
            # the binding's defining module. ``from services.writer
            # import apply`` never reaches an unrelated
            # ``services/locked.py::apply`` through the union.
            return {
                q for q in module_top.get(original, set()) if q[0] == label
            } | {
                q for q in class_methods.get(original, set()) if q[0] == label
            }
        if receiver is not None:
            # ``self.method(...)`` / ``cls.method(...)`` inside a class
            # method: dispatch to the caller's own class's method table
            # (including inherited methods). Without this the walk falls
            # into the class-method union across every scanned class, so
            # an unlocked writer's ``self.finish()`` could reach a lock
            # through an unrelated class's ``finish`` and silence the
            # "declared but never reaches lock" check. ``self`` and
            # ``cls`` are conventions, not keywords, but every method in
            # this codebase uses them — the same convention `ast` itself
            # uses when it decides an attribute call has "self" as its
            # receiver.
            if receiver in {"self", "cls"}:
                own_class = method_class.get(caller_qualified)
                if own_class is not None:
                    return _class_method_lookup(
                        own_class[0], own_class[1], name,
                    )
            # A Name receiver that itself names a class (imported class
            # symbol or a locally defined class): ``LockedService.apply()``
            # dispatches to that class's own method just like the direct
            # construction form.
            cls = _class_identity_for(caller_qualified, receiver)
            if cls is not None:
                return _class_method_lookup(cls[0], cls[1], name)
            binding = _lookup_binding(caller_qualified, receiver)
            if binding is not None:
                receiver_label, receiver_original = binding
                if receiver_original is None and receiver_label is not None:
                    # ``receiver.attr(...)`` where ``receiver`` is a
                    # scanned module the caller imported at some lexical
                    # scope on the way out: an unrelated same-named
                    # function in another scanned module is not on the
                    # actual call path, and unioning it in would let a
                    # route reach a lock (or a mutator) it doesn't
                    # actually reach. Restrict both fallbacks to the
                    # receiver's module.
                    return {
                        q for q in module_top.get(name, set())
                        if q[0] == receiver_label
                    } | {
                        q for q in class_methods.get(name, set())
                        if q[0] == receiver_label
                    }
        # Attribute call with an unknown or runtime receiver, or one whose
        # receiver is bound to something the graph cannot restrict against:
        # match any module-level function and any class method with that
        # name. The class-method union lets a route calling
        # ``PhotoReviewService(db).set_flag(...)`` from a blueprint reach
        # into the service's ``set_flag``; without it, a decision route
        # wrapped inside a service class would be invisible to the graph
        # and could silently write ``prediction_review`` without being
        # flagged as needing the lock.
        return module_top.get(name, set()) | class_methods.get(name, set())

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
            module_class_receiver = (
                receiver_id if receiver_kind == "module_class" else None
            )
            for qualified in resolve(
                current, name, False,
                receiver=receiver,
                class_receiver=class_receiver,
                module_class_receiver=module_class_receiver,
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
    nested import — the local binding stays lexically confined to
    ``_unrelated`` and is invisible from the route's own scope chain.
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


def test_decision_route_analysis_follows_function_local_imports():
    """A lazy ``from X import Y`` inside a route resolves to Y in X.

    A route body may lazily import a helper (``from services.writer
    import decide`` inside the ``def``, to keep the module-level import
    graph small or to avoid a cycle) and then call ``decide(db)``.
    Python binds that import inside the route's own scope, and a
    bare-Name lookup that only consults the module-level import list
    cannot see it: the reachability walk then treats ``decide`` as
    unimported and either resolves it to a same-named module-level
    ``decide`` in the caller's file (if one exists) or to nothing at
    all, so a mutating helper reached through a lazy import silently
    drops off the call graph. The lexical-scope resolver must record the
    binding under the route's scope and follow the edge to
    ``services/writer.py::decide``.
    """
    writer = '''
def decide(db):
    db.update_prediction_status(1, "rejected")
'''
    blueprint = '''
def create_bp(get_db, json_error):
    bp = Blueprint("lazy", __name__)

    @bp.post("/api/lazy")
    def api_lazy():
        from services.writer import decide
        return decide(get_db())

    return bp
'''
    call_map, routes, resolve = _call_graph([
        ("services/writer.py", writer, False),
        ("web/lazy.py", blueprint, True),
    ])
    problems = _decision_route_problems(
        call_map, routes, resolve, set(),
    )
    assert any(
        p.startswith("api_lazy ")
        and "writes prediction decisions but is not in PREDICTION_DECISION_ROUTES"
        in p
        for p in problems
    ), problems


def test_decision_route_analysis_pins_from_imports_to_defining_module():
    """``from services.writer import apply`` restricts to services.writer.

    Two scanned service modules define a module-level ``apply``:
    ``services/writer.py::apply`` mutates ``prediction_review`` without
    locking; ``services/locked.py::apply`` reaches
    ``begin_prediction_decision``. A blueprint that imports the writer's
    ``apply`` and calls it as a bare Name is unambiguously calling the
    writer's version, and unambiguously unlocked. A resolver that
    records only the imported symbol's name (discarding the defining
    module) then unions every scanned ``apply`` when resolving the bare
    Name, and the walk reaches the lock through the unrelated
    ``locked.apply`` — declaring the route silences the "declared but
    never reaches lock" check even though its actual path never locks.
    Preserving the defining module in the import record keeps that
    signal.
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
from services.writer import apply


def create_bp(get_db, json_error):
    bp = Blueprint("w", __name__)

    @bp.post("/api/w")
    def api_w():
        return apply(get_db())

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
    # ``api_w`` reaches the writer's mutator, so it's flagged as a writer
    # when undeclared; and once declared it still fails the second check
    # because the actual path never takes the lock. The union-only
    # resolver would have silenced this second check by reaching the
    # lock via ``locked.apply``.
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


def test_decision_route_analysis_pins_module_qualified_constructors():
    """``mod.ClassName(...).method(...)`` still dispatches to that class.

    Two service modules each define a class exposing an ``apply`` method:
    ``services/writer.py::WriterService.apply`` mutates ``prediction_review``
    without locking; ``services/locked.py::LockedService.apply`` reaches
    ``begin_prediction_decision``. A blueprint imports ``writer`` and calls
    ``writer.WriterService(get_db()).apply(1)`` — unambiguously unlocked. A
    resolver that only recognized class identity when the constructor is a
    bare Name lets that call fall into the unknown-receiver union, which
    reaches the lock through the unrelated ``LockedService.apply``; declaring
    the route then silences the "declared but never reaches lock" check even
    though its actual path never locks. Pinning the class through the
    receiver module's import restores the signal.
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
    @staticmethod
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
        return writer.WriterService(get_db()).apply(1)

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


def test_decision_route_analysis_follows_inherited_methods():
    """A method inherited from a base class stays reachable through a subclass.

    ``services/base.py::BaseService.apply`` mutates ``prediction_review``.
    ``services/writer.py::WriterService`` inherits from ``BaseService`` and
    defines no ``apply`` of its own. A blueprint calls
    ``WriterService(db).apply(1)``. An exact-class lookup that returned an
    empty set for ``WriterService.apply`` — because no such method is
    defined directly on ``WriterService`` — would drop the inherited mutator
    from the call graph, so the undeclared and unlocked route would pass
    the contract test silently. Walking up ``class_bases`` keeps the
    inherited writer visible.

    Also covers the ``mod.Base`` header form: the subclass's base is
    resolved through the subclass module's own import of ``base``.
    """
    base = '''
class BaseService:
    def __init__(self, db):
        self.db = db

    def apply(self, pred_id):
        self.db.update_prediction_status(pred_id, "rejected")
'''
    writer = '''
from services import base


class WriterService(base.BaseService):
    pass
'''
    blueprint = '''
from services.writer import WriterService


def create_writer_blueprint(get_db, json_error):
    bp = Blueprint("w", __name__)

    @bp.post("/api/w")
    def api_w():
        return WriterService(get_db()).apply(1)

    return bp
'''
    call_map, routes, resolve = _call_graph([
        ("services/base.py", base, False),
        ("services/writer.py", writer, False),
        ("web/writer_route.py", blueprint, True),
    ])
    problems = _decision_route_problems(
        call_map, routes, resolve, set(),
    )
    assert any(
        p.startswith("api_w ")
        and "writes prediction decisions but is not in PREDICTION_DECISION_ROUTES"
        in p
        for p in problems
    ), problems


def test_decision_route_analysis_pins_self_calls_to_defining_class():
    """``self.finish()`` inside a class method dispatches to that class.

    Two service classes each expose ``finish``: ``WriterService.finish``
    writes a prediction status without locking, ``LockedService.finish``
    reaches ``begin_prediction_decision``. A route constructs
    ``WriterService(db).apply(...)`` and that ``apply`` delegates through
    ``self.finish()``. A resolver that treated ``self`` as an unknown
    receiver would union every scanned ``finish``, so the walk could
    reach the lock through the unrelated ``LockedService.finish`` and
    declaring the route would silence the "declared but never reaches
    lock" check. Propagating the caller method's defining class into
    resolution keeps ``self.finish()`` inside ``WriterService``.
    """
    writer = '''
class WriterService:
    def __init__(self, db):
        self.db = db

    def apply(self, pred_id):
        self._prep(pred_id)
        self.finish(pred_id)

    def _prep(self, pred_id):
        pass

    def finish(self, pred_id):
        self.db.update_prediction_status(pred_id, "rejected")
'''
    locked_service = '''
from services import prediction_decisions


class LockedService:
    def __init__(self, db, json_error):
        self.db = db
        self.json_error = json_error

    def finish(self, pred_id):
        return prediction_decisions.under_prediction_decision_lock(
            self.db,
            lambda: self.db.update_prediction_status(pred_id, "accepted"),
            json_error=self.json_error,
        )
'''
    blueprint = '''
from services.writer import WriterService


def create_writer_blueprint(get_db, json_error):
    bp = Blueprint("w", __name__)

    @bp.post("/api/w")
    def api_w():
        return WriterService(get_db()).apply(1)

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
    # api_w reaches a mutator via self.finish(); undeclared, that must be
    # flagged.
    problems = _decision_route_problems(
        call_map, routes, resolve, set(),
    )
    assert any(
        p.startswith("api_w ")
        and "writes prediction decisions but is not in PREDICTION_DECISION_ROUTES"
        in p
        for p in problems
    ), problems
    # Declared, api_w must not silence the "no lock" check by reaching the
    # lock through LockedService.finish — self.finish() belongs to
    # WriterService, which does not lock.
    problems = _decision_route_problems(
        call_map, routes, resolve, {"api_w"},
    )
    assert any(
        p.startswith("api_w ")
        and "never reaches begin_prediction_decision" in p
        for p in problems
    ), problems


# Routes still registered with ``@app.<verb>`` in ``vireo/app.py``. This number
# may only go down. New routes belong in a blueprint under ``vireo/web/`` (see
# docs/ARCHITECTURE.md). When a PR moves routes out of app.py, lower this to
# the new count in the same PR so the extraction cannot be undone.
_LEGACY_APP_ROUTE_LIMIT = 0

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
