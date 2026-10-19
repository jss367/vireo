"""Broad exception handlers only shrink, and the cleaned ones say why.

A broad handler is ``except:``, ``except Exception``, ``except
BaseException`` (alone or inside a tuple), or ``contextlib.suppress`` of
either. Each one catches errors nobody listed, so a bug in the try body
turns into whatever the handler does with it. CORE_PHILOSOPHY.md ("No black
boxes") asks that a failure reach the user and the log, never be swallowed;
a broad handler that returns quietly is the easiest way to break that.

Two rules hold that line:

- ``broad_except_baseline.json`` records how many broad handlers each module
  under ``vireo/`` (outside ``tests/`` and ``testing/``) has today, and the
  count must match exactly. Adding one fails: catch what the try body can
  actually raise (``OSError``, ``ValueError``, ``sqlite3.Error``,
  ``json.JSONDecodeError``, ...). Removing one fails until its entry is
  lowered, so the cleanup cannot be quietly undone; run
  ``python vireo/tests/test_broad_except_ratchet.py --write-baseline``. The
  writer refuses to raise an entry. It refuses every new module entry after bootstrap: numeric totals cannot
  prove that a handler moved. A genuine move/split requires a reviewed
  baseline-key migration by hand, preserving or reducing each moved budget.
- Under ``DOCUMENTED_ROOTS``, every broad handler must account for what it
  catches: re-raise (a cleanup or rollback guard), log with the traceback
  (``log.exception(...)`` or a log call passing ``exc_info``), or carry a
  ``# broad: <reason>`` comment on the ``except`` line or the comment lines
  directly under it (for ``suppress``, on the ``with`` line or the comment
  lines directly above it). A boundary that stays broad on purpose gets both
  the log and the reason. Add a directory (``"services/"``) or a module
  (``"jobs.py"``) to ``DOCUMENTED_ROOTS`` once its handlers pass.
Traceback accounting proves only a straight-line prefix: conditional
logging/re-raises, early exits, loops and other control flow need an explicit
reason. ``log.exception`` must not disable ``exc_info``; other logger calls
need a truthy literal, the unchanged caught exception, or ``sys.exc_info()``.
Comprehension iteration targets have their own scope and do not shadow outer
contextlib aliases. No dynamic import, runtime type or general path analysis.
"""

import ast
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

VIREO = Path(__file__).resolve().parents[1]
BASELINE_PATH = Path(__file__).with_name("broad_except_baseline.json")

# Directories under vireo/ that are not production code.
SKIPPED_DIRS = {"tests", "testing"}

# Paths relative to vireo/ (a directory ends in "/") whose broad handlers must
# re-raise, log the traceback, or say why they are broad. Extend it as areas
# are cleaned; never remove an entry.
DOCUMENTED_ROOTS: tuple[str, ...] = (
    "web/",
    "pipeline_stages/",
    "classify_job/",
    "job_runner/",
    "jobs.py",
    "scanner/",
    "import_job/",
)

BROAD_NAMES = {"Exception", "BaseException"}
LOGGER_NAMES = {"log", "logger", "logging", "_log", "_logger", "LOG"}
LOG_METHODS = {"debug", "info", "warning", "warn", "error", "critical", "exception", "log"}
REASON_COMMENT = re.compile(r"#\s*broad:\s*\S")


def _is_broad_type(node) -> bool:
    if node is None:
        return True
    if isinstance(node, ast.Tuple):
        return any(_is_broad_type(element) for element in node.elts)
    if isinstance(node, ast.Name):
        return node.id in BROAD_NAMES
    if isinstance(node, ast.Attribute):
        return node.attr in BROAD_NAMES
    return False


def _definition_expressions(node):
    """Expressions evaluated now, before the new definition name is bound.

    Decorators run top-down before defaults or class arguments. Annotations
    and type-parameter bounds are deferred and do not advance these bindings.
    """
    expressions = list(getattr(node, "decorator_list", ()))
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
        expressions.extend(node.args.defaults)
        expressions.extend(value for value in node.args.kw_defaults if value is not None)
    else:
        expressions.extend(node.bases)
        expressions.extend(keyword.value for keyword in node.keywords)
    return expressions


def _scope_bindings(body, inherited, args=None):
    """Bounded import resolution; assignments/parameters shadow imported aliases.

    No value-flow or dynamic import analysis. A rebound name is excluded even
    if a later import might restore it. Class scopes do not leak into methods.
    """
    bindings = dict(inherited)
    def bind(node):
        if isinstance(node, ast.Lambda):
            bindings.update(_scope_bindings(_definition_expressions(node), bindings))
            return
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            bindings.update(_scope_bindings(_definition_expressions(node), bindings))
            bindings[node.name] = None
            return
        if isinstance(node, ast.Import):
            for alias in node.names:
                bindings[alias.asname or alias.name.split(".")[0]] = (
                    "contextlib" if alias.name == "contextlib" else None
                )
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                bindings[alias.asname or alias.name] = (
                    "suppress" if node.level == 0 and node.module == "contextlib" and alias.name == "suppress" else None
                )
        for child in ast.iter_child_nodes(node):
            bind(child)
    for node in body:
        bind(node)
    # Conservatively invalidate assignments, regardless of statement ordering.
    comprehension_targets = {
        id(child) for root in _own_nodes(body) if isinstance(root, ast.comprehension)
        for child in ast.walk(root.target)
    }
    for node in _own_nodes(body):
        if id(node) in comprehension_targets:
            continue
        if isinstance(node, ast.Name) and isinstance(node.ctx, (ast.Store, ast.Del)):
            bindings[node.id] = None
        elif isinstance(node, (ast.ExceptHandler, ast.MatchAs, ast.MatchStar)) and node.name:
            bindings[node.name] = None
        elif isinstance(node, ast.MatchMapping) and node.rest:
            bindings[node.rest] = None
    if args is not None:
        for arg in args.posonlyargs + args.args + args.kwonlyargs:
            bindings[arg.arg] = None
        for arg in (args.vararg, args.kwarg):
            if arg is not None:
                bindings[arg.arg] = None
    return bindings


def _is_broad_suppress(node, bindings) -> bool:
    """Only contextlib.suppress and explicit, unshadowed import aliases."""
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    resolved = (
        isinstance(func, ast.Name) and "suppress" in _binding_options(bindings.get(func.id))
        or isinstance(func, ast.Attribute) and func.attr == "suppress"
        and isinstance(func.value, ast.Name) and "contextlib" in _binding_options(bindings.get(func.value.id))
    )
    return bool(resolved) and any(_is_broad_type(arg) for arg in node.args)


def _binding_options(value):
    return value if isinstance(value, frozenset) else frozenset({value})


def _join_bindings(*states):
    """Retain aliases possible on any incoming path; no dynamic value inference."""
    return {
        name: frozenset().union(*(_binding_options(state.get(name)) for state in states))
        for name in set().union(*(state.keys() for state in states))
    }


def _declared_globals(statements):
    return {
        name for node in _own_nodes(statements) if isinstance(node, ast.Global)
        for name in node.names
    }


def _possible_aliases(bindings):
    """Only imported aliases matter to a function's possible module globals."""
    return {
        name: value for name, value in bindings.items()
        if _binding_options(value) & {"contextlib", "suppress"}
    }


def _irrefutable_pattern(pattern):
    if isinstance(pattern, ast.MatchAs):
        return pattern.pattern is None or _irrefutable_pattern(pattern.pattern)
    return isinstance(pattern, ast.MatchOr) and any(
        _irrefutable_pattern(item) for item in pattern.patterns
    )


def _expression_bindings(node, bindings):
    """Named-expression stores on normal evaluation paths, without truth refinement."""
    if isinstance(node, ast.Lambda):
        current = bindings
        for value in _definition_expressions(node):
            current = _expression_bindings(value, current)
        return current  # Defaults run now; the lambda body does not.
    if isinstance(node, ast.NamedExpr):
        current = _expression_bindings(node.value, bindings)
        return _scope_bindings([node.target], current)
    if isinstance(node, ast.BoolOp):
        current = _expression_bindings(node.values[0], bindings)
        for value in node.values[1:]:
            current = _join_bindings(current, _expression_bindings(value, current))
        return current
    if isinstance(node, ast.Compare):
        current = _expression_bindings(node.left, bindings)
        for index, value in enumerate(node.comparators):
            evaluated = _expression_bindings(value, current)
            current = evaluated if index == 0 else _join_bindings(current, evaluated)
        return current
    if isinstance(node, ast.IfExp):
        current = _expression_bindings(node.test, bindings)
        return _join_bindings(_expression_bindings(node.body, current),
                              _expression_bindings(node.orelse, current))
    current = bindings
    for child in ast.iter_child_nodes(node):
        current = _expression_bindings(child, current)
    if isinstance(node, (ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp)):
        return _join_bindings(bindings, current)  # May perform no iterations.
    return current


def broad_handlers(tree):
    """Direct broad types and contextlib aliases on bounded possible paths.

    Module/class statements advance in source order. If/loop joins retain an
    alias possible on either path (even a statically false branch is inspected
    for the syntax ratchet). Match captures propagate across guards: an
    irrefutable capture binds before its guard regardless of the guard result.
    Functions retain conservative whole-local-scope shadowing; explicitly
    global names advance in statement order. This is not a
    general Python control-flow or dynamic-value analyzer. Try handlers join
    possible statement-prefix states; named stores in conditions flow into
    both branches, without inferring which values make a condition true. Loop
    bodies are inspected once after their finite incoming alias sets stabilize.
    Function globals retain possible ordered module states from the definition
    onward; this does not infer calls or when the function actually executes.
    """
    found = []
    state_only = False
    flow_globals, module_globals = frozenset(), {}
    module_prefixes, definition_offsets, definition_globals = [], {}, {}
    def body(statements, bindings, lexical, function_lexical, ordered, prefixes=()):
        current = dict(bindings)
        for collector in prefixes:
            collector.append(current)
        for child in statements:
            updated = visit(child, current, current if lexical is None else lexical,
                            function_lexical, ordered, prefixes)
            if ordered or flow_globals:
                candidate = updated if updated is not None else _scope_bindings([child], current)
                current = candidate if ordered else {
                    **current, **{name: candidate.get(name) for name in flow_globals},
                }
            for collector in prefixes:
                collector.append(current)
        return current

    def visit(node, bindings, lexical, function_lexical, ordered, prefixes=()):
        nonlocal state_only, flow_globals, module_globals
        # Module bindings flow into nested class bodies; class namespaces do not.
        nested_lexical = None if lexical is bindings else lexical
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            current = dict(bindings)
            for expression in _definition_expressions(node):
                visit(expression, current, lexical, function_lexical, ordered, prefixes)
                current = _expression_bindings(expression, current)
                for collector in prefixes:
                    collector.append(current)
            if any(collector is module_prefixes for collector in prefixes):
                seed = current
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and not node.decorator_list:
                    # An undecorated function becomes callable only after its
                    # own module name is bound. Decorators/class construction
                    # can invoke callbacks before the enclosing name is bound.
                    seed = dict(current, **{node.name: None})
                definition_offsets[id(node)] = len(module_prefixes), _possible_aliases(seed)
            if not state_only:
                definition_lexical = definition_globals.get(id(node), function_lexical)
                previous_flow, previous_module = flow_globals, module_globals
                module_globals = {
                    **definition_globals.get(id(node), module_globals),
                    **{name: bindings.get(name) for name in flow_globals},
                }
                flow_globals = frozenset() if isinstance(node, ast.ClassDef) else _declared_globals(node.body)
                try:
                    if isinstance(node, ast.ClassDef):
                        class_lexical = current if lexical is bindings else lexical
                        body(node.body, class_lexical, class_lexical, definition_lexical, True)
                    else:
                        local = _scope_bindings(node.body, definition_lexical, node.args)
                        # Global assignments do not make function locals. Only
                        # these declared names advance in statement order.
                        local.update({name: module_globals.get(name) for name in flow_globals})
                        body(node.body, local, local, local, False)
                finally:
                    flow_globals, module_globals = previous_flow, previous_module
            return dict(current, **{node.name: None})
        if isinstance(node, ast.ExceptHandler):
            if _is_broad_type(node.type):
                found.append(("except", node))
            if node.name:
                bindings = dict(bindings, **{node.name: None})
        elif isinstance(node, (ast.With, ast.AsyncWith)):
            current = dict(bindings)
            for item in node.items:
                if _is_broad_suppress(item.context_expr, current):
                    found.append(("suppress", node))
                visit(item.context_expr, current, lexical, function_lexical, ordered, prefixes)
                current = _expression_bindings(item.context_expr, current)
                if item.optional_vars is not None:
                    current = _scope_bindings([item.optional_vars], current)
            return body(node.body, current, nested_lexical, function_lexical, ordered, prefixes)
        elif isinstance(node, ast.If):
            visit(node.test, bindings, lexical, function_lexical, ordered, prefixes)
            current = _expression_bindings(node.test, bindings)
            return _join_bindings(
                body(node.body, current, nested_lexical, function_lexical, ordered, prefixes),
                body(node.orelse, current, nested_lexical, function_lexical, ordered, prefixes),
            )
        elif isinstance(node, (ast.Try, ast.TryStar)):
            interrupted = []
            after_try = body(node.body, bindings, nested_lexical, function_lexical,
                             ordered, (*prefixes, interrupted))
            # Exceptions may arrive before or after any same-scope statement,
            # including inside a conditional. Else belongs only to normal exit.
            exceptional = _join_bindings(*interrupted)
            outcomes = [body(node.orelse, after_try, nested_lexical,
                             function_lexical, ordered, prefixes)]
            for handler in node.handlers:
                if _is_broad_type(handler.type):
                    found.append(("except", handler))
                incoming = dict(exceptional)
                if handler.name:
                    incoming[handler.name] = None
                outcomes.append(body(handler.body, incoming, nested_lexical,
                                     function_lexical, ordered, prefixes))
            normal = _join_bindings(*outcomes)
            if not node.finalbody or state_only:
                return body(node.finalbody, normal, nested_lexical,
                            function_lexical, ordered, prefixes)
            # Finally is inspected on uncaught paths too, but only normal/
            # handled paths can reach the statement following this try.
            body(node.finalbody, _join_bindings(exceptional, normal),
                 nested_lexical, function_lexical, ordered, prefixes)
            checkpoint = len(found)
            state_only = True
            try:
                return body(node.finalbody, normal, nested_lexical,
                            function_lexical, ordered)
            finally:
                state_only = False
                del found[checkpoint:]
        elif isinstance(node, (ast.For, ast.AsyncFor, ast.While)):
            is_for = isinstance(node, (ast.For, ast.AsyncFor))
            condition = node.iter if is_for else node.test
            # For iterables run once; while tests and for targets run on every
            # iteration. Join the finite alias states before inspecting syntax.
            head = _expression_bindings(condition, bindings) if is_for else bindings
            if ordered or flow_globals:
                checkpoint, previous_state_only = len(found), state_only
                state_only = True
                try:
                    while True:
                        incoming = head if is_for else _expression_bindings(condition, head)
                        current = _scope_bindings([node.target], incoming) if is_for else incoming
                        back = body(node.body, current, nested_lexical, function_lexical, ordered)
                        joined = _join_bindings(head, back)
                        if joined == head:
                            break
                        head = joined
                finally:
                    state_only = previous_state_only
                    del found[checkpoint:]  # State passes must not count handlers.
            visit(condition, head, lexical, function_lexical, ordered, prefixes)
            incoming = head if is_for else _expression_bindings(condition, head)
            current = _scope_bindings([node.target], incoming) if is_for else incoming
            after_body = body(node.body, current, nested_lexical, function_lexical, ordered, prefixes)
            # Else may run after zero iterations; break may bypass else.
            after_else = body(node.orelse, _join_bindings(incoming, after_body),
                              nested_lexical, function_lexical, ordered, prefixes)
            return _join_bindings(incoming, after_body, after_else)
        elif isinstance(node, ast.Match):
            visit(node.subject, bindings, lexical, function_lexical, ordered, prefixes)
            fallthrough = _expression_bindings(node.subject, bindings)
            outcomes = []
            for case in node.cases:
                current = _scope_bindings([case.pattern], fallthrough)
                if case.guard is not None:
                    visit(case.guard, current, lexical, function_lexical, ordered, prefixes)
                    current = _expression_bindings(case.guard, current)
                after_body = body(case.body, current, nested_lexical, function_lexical, ordered, prefixes)
                guard = case.guard.value if isinstance(case.guard, ast.Constant) else None
                if case.guard is None or guard is not False:
                    outcomes.append(after_body)
                if _irrefutable_pattern(case.pattern):
                    if case.guard is None or guard is True:
                        return _join_bindings(*outcomes)
                    fallthrough = current
                else:
                    # A failed partial pattern may leave captures bound.
                    fallthrough = _join_bindings(fallthrough, current)
            return _join_bindings(fallthrough, *outcomes)
        for _field, value in ast.iter_fields(node):
            if isinstance(value, list) and value and all(isinstance(n, ast.stmt) for n in value):
                body(value, bindings, lexical, function_lexical, ordered, prefixes)
            elif isinstance(value, list):
                for child in value:
                    if isinstance(child, ast.AST):
                        visit(child, bindings, lexical, function_lexical, ordered, prefixes)
            elif isinstance(value, ast.AST):
                visit(value, bindings, lexical, function_lexical, ordered, prefixes)
        if isinstance(node, ast.AnnAssign) and node.value is None:
            return bindings  # An annotation alone does not replace a value.
        if isinstance(node, (ast.Expr, ast.Assign, ast.AnnAssign, ast.AugAssign, ast.Assert)):
            # Only assignment targets bind unconditionally. Stores inside a
            # short-circuited RHS/expression retain the skipped incoming path.
            value = node.test if isinstance(node, ast.Assert) else node.value
            current = bindings if value is None else _expression_bindings(value, bindings)
            targets = node.targets if isinstance(node, ast.Assign) else (
                [node.target] if isinstance(node, (ast.AnnAssign, ast.AugAssign)) else []
            )
            return _scope_bindings(targets, current)
        return None

    initial = {"contextlib": "contextlib"}
    # A function may run before a later module rebinding, or after a later
    # import. Collect ordered module states without entering deferred bodies,
    # then join only the suffix from each definition (no call-graph inference).
    state_only = True
    body(tree.body, initial, None, initial, True, (module_prefixes,))
    state_only = False
    found.clear()
    suffix, suffixes = {}, {}
    offsets = {offset for offset, _seed in definition_offsets.values()}
    for index in range(len(module_prefixes), -1, -1):
        if index < len(module_prefixes):
            suffix = _join_bindings(_possible_aliases(module_prefixes[index]), suffix)
        if index in offsets:
            suffixes[index] = suffix
    definition_globals.update({
        key: _join_bindings(seed, suffixes[offset])
        for key, (offset, seed) in definition_offsets.items()
    })
    module_prefixes.clear()
    definition_offsets.clear()
    body(tree.body, initial, None, initial, True)
    return found


def _own_nodes(statements):
    """Nodes in ``statements``, not descending into nested functions or classes."""
    scopes = (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda, ast.ClassDef)
    stack = [node for node in statements if not isinstance(node, scopes)]
    while stack:
        node = stack.pop()
        yield node
        stack.extend(
            child for child in ast.iter_child_nodes(node) if not isinstance(child, scopes)
        )


def _logger_receiver(node) -> bool:
    """Known logger naming conventions and logging.getLogger expressions only.

    This is a syntax policy, not proof of an object's runtime type. In
    particular catalog.error/dialog.error and arbitrary factories do not count.
    """
    if isinstance(node, ast.Name):
        return node.id in LOGGER_NAMES
    if isinstance(node, ast.Attribute):
        return node.attr in LOGGER_NAMES - {"logging"}
    if isinstance(node, ast.Call):
        func = node.func
        return (isinstance(func, ast.Attribute) and func.attr == "getLogger"
                and isinstance(func.value, ast.Name) and func.value.id == "logging")
    return False


def _logs_traceback(call, exception_name=None) -> bool:
    """Known logger syntax, with no false or unknown traceback override."""
    func = call.func
    if not isinstance(func, ast.Attribute) or func.attr not in LOG_METHODS:
        return False
    if not _logger_receiver(func.value):
        return False
    if any(keyword.arg is None for keyword in call.keywords):
        return False  # **options may contain exc_info=False.
    for keyword in call.keywords:
        if keyword.arg == "exc_info":
            value = keyword.value
            if isinstance(value, ast.Constant):
                return bool(value.value)
            if isinstance(value, ast.Name):
                return value.id == exception_name
            return (
                isinstance(value, ast.Call) and isinstance(value.func, ast.Attribute)
                and isinstance(value.func.value, ast.Name) and value.func.value.id == "sys"
                and value.func.attr == "exc_info" and not value.args and not value.keywords
            )
    return func.attr == "exception"


def _has_reason_comment(lines, first, last) -> bool:
    """A ``# broad:`` reason on 1-based lines ``first..last`` (inclusive)."""
    return any(REASON_COMMENT.search(lines[n - 1]) for n in range(first, last + 1))


def _comment_block_above(lines, lineno) -> int:
    """First line of the comment-only block directly above 1-based ``lineno``."""
    first = lineno
    while first > 1 and lines[first - 2].lstrip().startswith("#"):
        first -= 1
    return first


def is_accounted_for(kind, node, lines) -> bool:
    """Whether a broad handler re-raises, logs the traceback, or says why."""
    if kind == "suppress":
        return _has_reason_comment(lines, _comment_block_above(lines, node.lineno), node.lineno)
    # Only a straight-line prefix proves every quiet continuation reaches
    # the logging/re-raise. Branches/loops/try/with/returns fail closed; use
    # an explicit reason instead of attempting general control-flow analysis.
    straight_line = (ast.Expr, ast.Assign, ast.AnnAssign, ast.AugAssign,
                     ast.Pass, ast.Delete, ast.Assert, ast.Import, ast.ImportFrom)
    exception_name = node.name
    for statement in node.body:
        if any(isinstance(child, ast.Yield | ast.YieldFrom) for child in ast.walk(statement)):
            break
        if any(isinstance(child, ast.Name) and isinstance(child.ctx, ast.Store)
               and child.id == exception_name for child in _own_nodes([statement])):
            exception_name = None
        if isinstance(statement, ast.Raise):
            return True
        if (isinstance(statement, ast.Expr) and isinstance(statement.value, ast.Call)
                and _logs_traceback(statement.value, exception_name)):
            return True
        if not isinstance(statement, straight_line):
            break
    # The except line through the line before the body's first statement:
    # the clause itself (possibly a multi-line tuple) and comments under it.
    return _has_reason_comment(lines, node.lineno, max(node.lineno, node.body[0].lineno - 1))


def _modules():
    for path in sorted(VIREO.rglob("*.py")):
        rel = path.relative_to(VIREO)
        if rel.parts[0] in SKIPPED_DIRS:
            continue
        source = path.read_text(encoding="utf-8")
        yield rel.as_posix(), ast.parse(source, filename=str(path)), source.splitlines()


def broad_counts() -> dict[str, int]:
    """``{module path relative to vireo/: broad handler count}``, zeros omitted."""
    return {
        name: count for name, tree, _lines in _modules()
        if (count := len(broad_handlers(tree)))
    }


def load_baseline() -> dict[str, int]:
    return json.loads(BASELINE_PATH.read_text(encoding="utf-8"))


def _is_documented(name: str) -> bool:
    return any(
        name.startswith(root) if root.endswith("/") else name == root
        for root in DOCUMENTED_ROOTS
    )


def test_broad_handlers_only_shrink():
    baseline = load_baseline()
    current = broad_counts()
    grown = {
        name: (baseline.get(name, 0), count)
        for name, count in sorted(current.items())
        if count > baseline.get(name, 0)
    }
    assert not grown, (
        "These modules gained a broad exception handler (except:, except "
        "Exception, except BaseException, or contextlib.suppress of one) "
        f"(baseline, now): {grown}. Catch only what the try body can raise "
        "(OSError, ValueError, sqlite3.Error, json.JSONDecodeError, ...). If "
        "the new handler must stay broad (a per-item job loop, best-effort "
        "cleanup, a third-party call with an undocumented error set), log it "
        "with exc_info, give it a '# broad: <reason>' comment, and narrow or "
        "remove another broad handler in the same module so the count holds. "
        "Do not raise the entry in vireo/tests/broad_except_baseline.json."
    )
    shrunk = {
        name: (limit, current.get(name, 0))
        for name, limit in sorted(baseline.items())
        if current.get(name, 0) < limit
    }
    assert not shrunk, (
        f"These modules have fewer broad exception handlers (baseline, now): {shrunk}. "
        "Lower their entries so the cleanup sticks: run "
        "`python vireo/tests/test_broad_except_ratchet.py --write-baseline` "
        "(it only lowers entries and drops zeros)."
    )


def test_documented_roots_account_for_every_broad_handler():
    offenders = []
    for name, tree, lines in _modules():
        if not _is_documented(name):
            continue
        offenders.extend(
            f"{name}:{node.lineno}" for kind, node in broad_handlers(tree)
            if not is_accounted_for(kind, node, lines)
        )
    assert not offenders, (
        "These broad exception handlers swallow an error without a trace: "
        f"{offenders}. Narrow the except to what the try body can raise; if it "
        "must stay broad, log with log.exception(...) or exc_info=True and add "
        "a one-line '# broad: <reason>' comment on the except line (or the "
        "comment lines right under it; above the with line for "
        "contextlib.suppress), and surface the failure to the user where the "
        "response allows it (CORE_PHILOSOPHY.md, 'No black boxes')."
    )


def test_documented_roots_exist():
    missing = [root for root in DOCUMENTED_ROOTS if not (VIREO / root).exists()]
    assert not missing, f"DOCUMENTED_ROOTS names paths that do not exist: {missing}"


@pytest.mark.parametrize(("source", "count"), [
    ("try:\n    f()\nexcept:\n    pass\n", 1),
    ("try:\n    f()\nexcept Exception:\n    pass\n", 1),
    ("try:\n    f()\nexcept BaseException:\n    raise\n", 1),
    ("try:\n    f()\nexcept (OSError, Exception) as e:\n    pass\n", 1),
    ("try:\n    f()\nexcept builtins.Exception:\n    pass\n", 1),
    ("try:\n    f()\nexcept OSError:\n    pass\n", 0),
    ("try:\n    f()\nexcept (OSError, ValueError):\n    pass\n", 0),
    ("with contextlib.suppress(Exception):\n    f()\n", 1),
    ("from contextlib import suppress\nwith suppress(OSError, BaseException), open(p):\n    f()\n", 1),
    ("with contextlib.suppress(OSError):\n    f()\n", 0),
    ("import contextlib as ctx\nwith ctx.suppress(Exception): pass\n", 1),
    ("from contextlib import suppress as quiet\nwith quiet(BaseException): pass\n", 1),
    ("def suppress(*args): pass\nwith suppress(Exception): pass\n", 0),
    ("from other import suppress\nwith suppress(Exception): pass\n", 0),
    ("from contextlib import suppress\ndef f(suppress):\n    with suppress(Exception): pass\n", 0),
    ("import contextlib as ctx\ndef f():\n    ctx = unrelated\n    with ctx.suppress(Exception): pass\n", 0),
    ("def f():\n    from contextlib import suppress as quiet\n    with quiet(Exception): pass\n", 1),
    ("from contextlib import suppress\nitems = [suppress for suppress in values]\nwith suppress(Exception): pass\n", 1),
    ("import contextlib as ctx\nitems = {ctx: 1 for ctx in values}\nwith ctx.suppress(Exception): pass\n", 1),
    ("import contextlib as ctx\nclass Outer:\n    ctx = other\n    class Inner:\n        with ctx.suppress(Exception): pass\n", 1),
    ("def f():\n    import contextlib as ctx\n    class Outer:\n        ctx = other\n        class Inner:\n            with ctx.suppress(Exception): pass\n", 1),
    ("import contextlib as ctx\nclass Outer:\n    class Inner:\n        ctx = other\n        with ctx.suppress(Exception): pass\n", 0),
    ("from contextlib import suppress as quiet\nwith quiet(Exception): pass\nquiet = replacement\n", 1),
    ("class C:\n    from contextlib import suppress as quiet\n    with quiet(Exception): pass\n    quiet = replacement\n", 1),
    ("from contextlib import suppress as quiet\nquiet = replacement\nwith quiet(Exception): pass\n", 0),
    ("class C:\n    from contextlib import suppress as quiet\n    quiet = replacement\n    with quiet(Exception): pass\n", 0),
    ("from contextlib import suppress as quiet\ndef f():\n    with quiet(Exception): pass\n    quiet = replacement\n", 0),
])
def test_broad_handler_detection(source, count):
    assert len(broad_handlers(ast.parse(source))) == count


@pytest.mark.parametrize(("source", "accounted"), [
    ("try:\n    f()\nexcept Exception:\n    pass\n", False),
    ("try:\n    f()\nexcept Exception:\n    return None\n", False),
    ("try:\n    f()\nexcept Exception:\n    db.rollback()\n    raise\n", True),
    ("try:\n    f()\nexcept Exception as e:\n    raise ValueError(str(e)) from e\n", True),
    ("try:\n    f()\nexcept Exception:\n    log.exception('x')\n", True),
    ("try:\n    f()\nexcept Exception:\n    logger.warning('x', exc_info=True)\n", True),
    ("try:\n    f()\nexcept Exception:\n    self._log.error('x', exc_info=exc)\n", False),
    ("try:\n    f()\nexcept Exception:\n    logging.getLogger().exception('x')\n", True),
    ("try:\n    f()\nexcept Exception as e:\n    log.warning('x %s', e)\n", False),
    ("try:\n    f()\nexcept Exception:\n    log.warning('x', exc_info=False)\n", False),
    ("try:\n    f()\nexcept Exception:\n    print('x', exc_info=True)\n", False),
    ("try:\n    f()\nexcept Exception:  # broad: label is cosmetic\n    pass\n", True),
    ("try:\n    f()\nexcept Exception: pass  # broad: optional probe\n", True),
    ("try:\n    f()\nexcept Exception:\n    # broad: label is cosmetic\n    pass\n", True),
    ("try:\n    f()\nexcept Exception:\n    # why\n    # broad: cosmetic\n    pass\n", True),
    ("try:\n    f()\nexcept Exception:  # broad:\n    pass\n", False),
    ("try:\n    f()\nexcept Exception:\n    pass\n    # broad: too late\n", False),
    ("try:\n    f()\nexcept Exception:\n    def g():\n        raise\n", False),
    ("with contextlib.suppress(Exception):\n    f()\n", False),
    ("# broad: probe only\nwith contextlib.suppress(Exception):\n    f()\n", True),
    ("with contextlib.suppress(Exception):  # broad: probe only\n    f()\n", True),
    ("try:\n    f()\nexcept Exception:\n    catalog.error('x', exc_info=True)\n", False),
    ("try:\n    f()\nexcept Exception:\n    dialog.exception('x')\n", False),
    ("try:\n    f()\nexcept Exception:\n    factory.getLogger().exception('x')\n", False),
    ("try:\n    f()\nexcept Exception:\n    log.exception('x', exc_info=False)\n", False),
    ("try:\n    f()\nexcept Exception:\n    if condition: raise\n", False),
    ("try:\n    f()\nexcept Exception:\n    if condition: log.exception('x')\n", False),
    ("try:\n    f()\nexcept Exception:\n    if condition: return\n    log.exception('x')\n", False),
    ("try:\n    f()\nexcept Exception as exc:\n    log.error('x', exc_info=exc)\n", True),
    ("try:\n    f()\nexcept Exception:\n    log.error('x', exc_info=sys.exc_info())\n", True),
])
def test_accounted_for(source, accounted):
    tree = ast.parse(source)
    [(kind, node)] = broad_handlers(tree)
    assert is_accounted_for(kind, node, source.splitlines()) is accounted


def test_cli_refuses_unproved_transfer_even_when_total_is_unchanged(tmp_path):
    fixture = tmp_path / "vireo"
    tests = fixture / "tests"
    tests.mkdir(parents=True)
    script = tests / Path(__file__).name
    shutil.copyfile(__file__, script)
    baseline = tests / "broad_except_baseline.json"
    baseline.write_text(json.dumps({"old.py": 1}), encoding="utf-8")
    (fixture / "old.py").write_text("pass\n", encoding="utf-8")
    (fixture / "unrelated.py").write_text(
        "try:\n    other_operation()\nexcept BaseException:\n    pass\n", encoding="utf-8",
    )
    previous = baseline.read_bytes()
    result = subprocess.run([sys.executable, str(script), "--write-baseline"], capture_output=True)
    assert result.returncode == 1 and baseline.read_bytes() == previous
    assert b"totals cannot prove a move" in result.stdout


def write_baseline() -> int:
    """Rewrite the baseline from the tree; refuse to raise any entry.

    A new module requires a reviewed manual baseline-key migration, since
    matching numeric totals alone do not prove that code moved. With no
    baseline file yet, every module is written for initial bootstrap.
    """
    current = broad_counts()
    bootstrapping = not BASELINE_PATH.exists()
    previous = {} if bootstrapping else load_baseline()
    grown = {
        name: (previous[name], count) for name, count in sorted(current.items())
        if name in previous and count > previous[name]
    }
    if grown:
        print("Refusing to raise baseline entries (baseline, now); narrow these handlers instead:")
        for name, (old, new) in grown.items():
            print(f"  {name}: {old} -> {new}")
        return 1
    new = {name: count for name, count in sorted(current.items()) if name not in previous}
    if new and not bootstrapping:
        print(
            "Refusing to add entries for new modules; totals cannot prove a move. "
            "Narrow new handlers, or manually migrate baseline keys for a reviewed move/split."
        )
        for name, count in new.items():
            print(f"  {name}: {count}")
        return 1
    BASELINE_PATH.write_text(json.dumps(current, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {len(current)} modules ({sum(current.values())} broad handlers) to {BASELINE_PATH}")
    return 0


if __name__ == "__main__":
    if sys.argv[1:] != ["--write-baseline"]:
        sys.exit(f"usage: {sys.argv[0]} --write-baseline")
    sys.exit(write_baseline())


@pytest.mark.parametrize("compound", [
    "for quiet in values:\n    with quiet(Exception): pass\n",
    "for (quiet, other) in values:\n    with quiet(Exception): pass\n",
    "with factory() as quiet:\n    with quiet(Exception): pass\n",
    "with factory() as quiet, quiet(Exception): pass\n",
    "match value:\n    case quiet:\n        with quiet(Exception): pass\n",
    "match value:\n    case [*quiet]:\n        with quiet(Exception): pass\n",
    'match value:\n    case {"key": _, **quiet}:\n        with quiet(Exception): pass\n',
])
@pytest.mark.parametrize("scope", ["module", "class", "function"])
def test_compound_targets_shadow_alias_before_body(compound, scope):
    source = "from contextlib import suppress as quiet\n" + compound
    if scope != "module":
        header = "class C:" if scope == "class" else "def f():"
        source = header + "\n" + "\n".join("    " + line for line in source.splitlines())
    assert not broad_handlers(ast.parse(source))


def test_with_context_resolves_before_its_own_target_and_after_earlier_targets():
    source = ("from contextlib import suppress as quiet\n"
              "with quiet(Exception) as quiet, quiet(Exception): pass\n")
    assert len(broad_handlers(ast.parse(source))) == 1


def test_compound_unrelated_targets_preserve_suppress_alias():
    source = ("from contextlib import suppress as quiet\n"
              "for other in values:\n    with quiet(Exception): pass\n"
              "with factory() as other:\n    with quiet(Exception): pass\n"
              "match value:\n    case other:\n        with quiet(Exception): pass\n")
    assert len(broad_handlers(ast.parse(source))) == 3


@pytest.mark.parametrize("compound, count", [
    ("if False:\n    quiet = replacement\nwith quiet(Exception): pass\n", 1),
    ("if condition:\n    quiet = replacement\nwith quiet(Exception): pass\n", 1),
    ("if condition:\n    quiet = a\nelse:\n    quiet = b\nwith quiet(Exception): pass\n", 0),
    ("for quiet in []:\n    pass\nelse:\n    with quiet(Exception): pass\n", 1),
    ("for quiet in values:\n    with quiet(Exception): pass\nelse:\n    with quiet(Exception): pass\n", 1),
    ("while condition:\n    quiet = replacement\nelse:\n    with quiet(Exception): pass\n", 1),
    ("match value:\n    case quiet if False:\n        pass\n    case _:\n        with quiet(Exception): pass\n", 0),
    ("match value:\n    case _ as quiet if False:\n        pass\n    case _:\n        with quiet(Exception): pass\n", 0),
    ("match value:\n    case quiet if condition:\n        pass\n    case _:\n        with quiet(Exception): pass\n", 0),
    ("match value:\n    case quiet if False:\n        pass\nwith quiet(Exception): pass\n", 0),
])
@pytest.mark.parametrize("scope", ["module", "class"])
def test_possible_branch_bindings(compound, count, scope):
    source = "from contextlib import suppress as quiet\n" + compound
    if scope == "class":
        source = "class C:\n" + "\n".join("    " + line for line in source.splitlines())
    assert len(broad_handlers(ast.parse(source))) == count


@pytest.mark.parametrize("compound, count", [
    ("try:\n    risky()\nexcept OSError:\n    quiet = replacement\nwith quiet(Exception): pass\n", 1),
    ("try:\n    risky()\n    quiet = replacement\nexcept OSError:\n    with quiet(Exception): pass\n", 1),
    ("try:\n    risky()\nexcept OSError:\n    pass\nelse:\n    quiet = replacement\nwith quiet(Exception): pass\n", 1),
    ("try:\n    risky()\nfinally:\n    if condition:\n        quiet = replacement\nwith quiet(Exception): pass\n", 1),
    ("try:\n    risky()\nfinally:\n    quiet = replacement\nwith quiet(Exception): pass\n", 0),
    ("try:\n    quiet = replacement\nfinally:\n    pass\nwith quiet(Exception): pass\n", 0),
    ("try:\n    quiet = replacement\n    from contextlib import suppress as quiet\n    risky()\n    quiet = replacement\nexcept OSError:\n    with quiet(Exception): pass\n", 1),
    ("try:\n    if condition:\n        from contextlib import suppress as quiet\n        risky()\n        quiet = replacement\nexcept OSError:\n    with quiet(Exception): pass\n", 1),
    ("if (quiet := replacement):\n    with quiet(Exception): pass\nelse:\n    with quiet(Exception): pass\n", 0),
    ("if condition and (quiet := replacement):\n    pass\nwith quiet(Exception): pass\n", 1),
])
@pytest.mark.parametrize("scope", ["module", "class"])
def test_try_paths_and_condition_stores(compound, count, scope):
    source = "from contextlib import suppress as quiet\n" + compound
    if scope == "class":
        source = "class C:\n" + "\n".join("    " + line for line in source.splitlines())
    assert len(broad_handlers(ast.parse(source))) == count


@pytest.mark.parametrize("compound, count", [
    ("while (quiet := replacement):\n    with quiet(Exception): pass\nelse:\n    with quiet(Exception): pass\nwith quiet(Exception): pass\n", 0),
    ("while bool(quiet := replacement):\n    with quiet(Exception): pass\n", 0),
    ("while condition and (quiet := replacement):\n    pass\nwith quiet(Exception): pass\n", 1),
    ("while (other := condition):\n    with quiet(Exception): pass\n", 1),
    ("for item in (quiet := values):\n    with quiet(Exception): pass\n", 0),
    ("import contextlib as ctx\nwhile (ctx := replacement):\n    with ctx.suppress(Exception): pass\nelse:\n    with ctx.suppress(Exception): pass\n", 0),
])
@pytest.mark.parametrize("scope", ["module", "class"])
def test_loop_condition_stores_precede_body_and_zero_iteration_exit(compound, count, scope):
    source = "from contextlib import suppress as quiet\n" + compound
    if scope == "class":
        source = "class C:\n" + "\n".join("    " + line for line in source.splitlines())
    assert len(broad_handlers(ast.parse(source))) == count


@pytest.mark.parametrize("compound, count", [
    ("with factory(quiet := replacement):\n    with quiet(Exception): pass\n", 0),
    ("with factory(quiet := replacement), quiet(Exception): pass\n", 0),
    ("with factory(quiet := replacement): pass\nwith quiet(Exception): pass\n", 0),
    ("with factory(condition and (quiet := replacement)):\n    with quiet(Exception): pass\n", 1),
    ("with quiet(Exception, (quiet := replacement)):\n    with quiet(Exception): pass\n", 1),
    ("match (quiet := replacement):\n    case _:\n        with quiet(Exception): pass\n", 0),
    ("match value:\n    case _ if (quiet := replacement):\n        with quiet(Exception): pass\n    case _:\n        with quiet(Exception): pass\n", 0),
    ("match value:\n    case _ if (quiet := replacement): pass\n    case _: pass\nwith quiet(Exception): pass\n", 0),
    ("match value:\n    case 1 if (quiet := replacement): pass\n    case _:\n        with quiet(Exception): pass\n", 1),
    ("match value:\n    case _ if condition and (quiet := replacement): pass\n    case _:\n        with quiet(Exception): pass\n", 1),
    ("match (condition and (quiet := replacement)):\n    case _:\n        with quiet(Exception): pass\n", 1),
])
@pytest.mark.parametrize("scope", ["module", "class"])
def test_context_and_match_expression_stores(compound, count, scope):
    source = "from contextlib import suppress as quiet\n" + compound
    if scope == "class":
        source = "class C:\n" + "\n".join("    " + line for line in source.splitlines())
    assert len(broad_handlers(ast.parse(source))) == count


@pytest.mark.parametrize("definition, count", [
    ("def f(x=(quiet := replacement)): pass", 0),
    ("async def f(*, x=(quiet := replacement)): pass", 0),
    ("@(quiet := replacement)\ndef f(): pass", 0),
    ("@factory(quiet := replacement)\nasync def f(): pass", 0),
    ("def f(x=condition and (quiet := replacement)): pass", 1),
    ("def f(x: 'quiet := replacement') -> 'quiet := replacement': pass", 1),
    ("def f(x: (quiet := replacement)): pass", 1),
    ("def f():\n    quiet = replacement", 1),
    ("class C(quiet := replacement): pass", 0),
    ("class C(metaclass=(quiet := replacement)): pass", 0),
    ("@(quiet := replacement)\nclass C: pass", 0),
    ("class C(condition and (quiet := replacement)): pass", 1),
    ("class C:\n    quiet = replacement", 1),
    ("def quiet(x=quiet): pass", 0),
])
@pytest.mark.parametrize("scope", ["module", "class"])
def test_definition_time_stores_precede_name_binding(definition, count, scope):
    source = "from __future__ import annotations\nfrom contextlib import suppress as quiet\n"
    statement = definition + "\nwith quiet(Exception): pass\n"
    if scope == "class":
        statement = "class Outer:\n" + "\n".join("    " + line for line in statement.splitlines())
    assert len(broad_handlers(ast.parse(source + statement))) == count


@pytest.mark.parametrize("definition", [
    "@factory(quiet := replacement)\ndef f(x=probe()): pass",
    "def f(x=(quiet := replacement), *, y=probe()): pass",
    "@factory(quiet := replacement)\nclass C(probe()): pass",
    "class C(quiet := replacement, metaclass=probe()): pass",
])
def test_definition_expression_order_and_exception_prefixes(definition):
    # A later definition-time expression can fail after the alias is replaced.
    source = (
        "try:\n    quiet = replacement\n    from contextlib import suppress as quiet\n"
        + "\n".join("    " + line for line in definition.splitlines())
        + "\nexcept ValueError:\n    with quiet(Exception): pass\n"
    )
    # The import can itself fail before quiet is rebound; both paths remain.
    assert len(broad_handlers(ast.parse(source))) == 1


@pytest.mark.parametrize("definition", [
    "def f(x=(quiet := replacement)): pass",
    "async def f(*, x=(quiet := replacement)): pass",
    "@(quiet := replacement)\nclass C: pass",
])
def test_definition_time_stores_shadow_function_locals(definition):
    source = (
        "def outer():\n    from contextlib import suppress as quiet\n"
        + "\n".join("    " + line for line in definition.splitlines())
        + "\n    with quiet(Exception): pass\n"
    )
    assert len(broad_handlers(ast.parse(source))) == 0


def test_definition_expressions_preserve_runtime_order():
    function = ast.parse("@first\n@second\ndef f(a=third, *, b=fourth): pass").body[0]
    cls = ast.parse("@first\n@second\nclass C(third, metaclass=fourth): pass").body[0]
    for node in (function, cls):
        assert [ast.unparse(value) for value in _definition_expressions(node)] == [
            "first", "second", "third", "fourth",
        ]


@pytest.mark.parametrize("definition", [
    "class C(quiet := replacement):\n    with quiet(Exception): pass",
    "@factory(quiet := replacement)\nclass C:\n    with quiet(Exception): pass",
])
def test_class_body_sees_definition_time_outer_stores(definition):
    source = "from contextlib import suppress as quiet\n" + definition
    assert len(broad_handlers(ast.parse(source))) == 0


@pytest.mark.parametrize("statement, count", [
    ("def f(x=(lambda y=(quiet := replacement): None)): pass", 0),
    ("f = lambda y=(quiet := replacement): None", 0),
    ("f = lambda *, y=(quiet := replacement): None", 0),
    ("f = lambda: (quiet := replacement)", 1),
    ("f = lambda y=condition and (quiet := replacement): None", 1),
    ("condition and (quiet := replacement)", 1),
    ("condition or (quiet := replacement)", 1),
    ("assert condition or (quiet := replacement)", 1),
    ("assert condition, (quiet := replacement)", 1),
    ("value = condition and (quiet := replacement)", 1),
    ("value: object = condition or (quiet := replacement)", 1),
    ("value = (quiet := replacement) if condition else other", 1),
    ("factory(condition and (quiet := replacement))", 1),
    ("value = (quiet := replacement)", 0),
    ("quiet = condition and replacement", 0),
])
@pytest.mark.parametrize("scope", ["module", "class"])
def test_lambda_defaults_and_conditional_expression_stores(statement, count, scope):
    source = "from contextlib import suppress as quiet\n" + statement + "\nwith quiet(Exception): pass\n"
    if scope == "class":
        source = "class C:\n" + "\n".join("    " + line for line in source.splitlines())
    assert len(broad_handlers(ast.parse(source))) == count


@pytest.mark.parametrize("loop, count", [
    ("for item in values:\n    with quiet(Exception): pass\n    from contextlib import suppress as quiet", 1),
    ("while condition:\n    with quiet(Exception): pass\n    from contextlib import suppress as quiet", 1),
    ("async for item in values:\n    with quiet(Exception): pass\n    from contextlib import suppress as quiet", 1),
    ("for item in values:\n    with quiet(Exception): pass\n    if condition:\n        from contextlib import suppress as quiet", 1),
    ("for quiet in values:\n    with quiet(Exception): pass\n    from contextlib import suppress as quiet", 0),
    ("while (quiet := replacement):\n    with quiet(Exception): pass\n    from contextlib import suppress as quiet", 0),
    ("for item in values:\n    with quiet(Exception): pass\n    from contextlib import suppress as quiet\n    quiet = replacement", 0),
    ("for item in values:\n    with quiet(Exception), quiet(BaseException): pass\n    from contextlib import suppress as quiet", 2),
    ("for item in values:\n    for nested in others:\n        with quiet(Exception): pass\n    from contextlib import suppress as quiet", 1),
])
@pytest.mark.parametrize("scope", ["module", "class"])
def test_loop_carried_aliases_are_inspected_without_duplicate_counts(loop, count, scope):
    source = "quiet = replacement\n" + loop
    if scope == "class":
        source = "class C:\n" + "\n".join("    " + line for line in source.splitlines())
    assert len(broad_handlers(ast.parse(source))) == count


@pytest.mark.parametrize("statement", [
    "f = lambda y=(quiet := replacement): None",
    "def f(x=(lambda y=(quiet := replacement): None)): pass",
    "async def f(*, x=(lambda y=(quiet := replacement): None)): pass",
])
def test_lambda_default_stores_bind_conservative_function_locals(statement):
    source = (
        "def outer():\n    from contextlib import suppress as quiet\n"
        + "\n".join("    " + line for line in statement.splitlines())
        + "\n    with quiet(Exception): pass\n"
    )
    assert len(broad_handlers(ast.parse(source))) == 0


@pytest.mark.parametrize("expression, count", [
    ("2 < 1 < (quiet := replacement)", 1),
    ("left < middle < (quiet := replacement) < last", 1),
    ("left < (quiet := replacement)", 0),
    ("left < (quiet := replacement) < last", 0),
    ("(quiet := replacement) < middle < last", 0),
    ("left < middle < (quiet := replacement) < (quiet := other)", 1),
    ("left < middle < (other := replacement)", 1),
])
@pytest.mark.parametrize("scope", ["module", "class"])
def test_chained_comparison_stores_preserve_short_circuit_paths(expression, count, scope):
    source = "from contextlib import suppress as quiet\nresult = " + expression + "\nwith quiet(Exception): pass"
    if scope == "class":
        source = "class C:\n" + "\n".join("    " + line for line in source.splitlines())
    assert len(broad_handlers(ast.parse(source))) == count


@pytest.mark.parametrize("source, count", [
    ("from contextlib import suppress as quiet\ndef f():\n    with quiet(Exception): pass\nf()\nquiet = replacement", 1),
    ("import contextlib as ctx\ndef f():\n    with ctx.suppress(Exception): pass\nf()\nctx = replacement", 1),
    ("quiet = replacement\ndef f():\n    with quiet(Exception): pass\nfrom contextlib import suppress as quiet\nf()", 1),
    ("from contextlib import suppress as quiet\nquiet = replacement\ndef f():\n    with quiet(Exception): pass", 0),
    ("from contextlib import suppress as quiet\ndef f(quiet):\n    with quiet(Exception): pass\nquiet = replacement", 0),
    ("from contextlib import suppress as quiet\ndef f():\n    quiet = replacement\n    with quiet(Exception): pass\nquiet = replacement", 0),
    ("class C:\n    from contextlib import suppress as quiet\ndef f():\n    with quiet(Exception): pass", 0),
    ("from contextlib import suppress as quiet\nclass C:\n    def f(self):\n        with quiet(Exception): pass\nC().f()\nquiet = replacement", 1),
    ("from contextlib import suppress as quiet\ndef outer():\n    quiet = replacement\n    def inner():\n        with quiet(Exception): pass\nquiet = replacement", 0),
    ("from contextlib import suppress as quiet\nif condition:\n    def f():\n        with quiet(Exception), quiet(BaseException): pass\nf()\nquiet = replacement", 2),
])
def test_functions_keep_possible_module_globals_from_definition_onward(source, count):
    assert len(broad_handlers(ast.parse(source))) == count


@pytest.mark.parametrize("kind", ["def", "async def"])
@pytest.mark.parametrize("source, count", [
    ("from contextlib import suppress as quiet\n{kind} quiet():\n    with quiet(Exception): pass", 0),
    ("import contextlib as ctx\n{kind} ctx():\n    with ctx.suppress(Exception): pass", 0),
    ("from contextlib import suppress as quiet\n{kind} quiet():\n    with quiet(Exception): pass\nsaved = quiet\nfrom contextlib import suppress as quiet\nsaved()", 1),
    ("from contextlib import suppress as quiet\n{kind} quiet():\n    with quiet(Exception): pass\nsaved = quiet\nif condition:\n    from contextlib import suppress as quiet\nsaved()", 1),
    ("from contextlib import suppress as quiet\n@factory\n{kind} quiet():\n    with quiet(Exception): pass", 1),
])
def test_function_own_name_shadows_only_after_undecorated_binding(kind, source, count):
    assert len(broad_handlers(ast.parse(source.format(kind=kind)))) == count


def test_class_method_name_does_not_shadow_module_alias():
    source = (
        "from contextlib import suppress as quiet\nclass C:\n"
        "    def quiet(self):\n        with quiet(Exception): pass\n"
    )
    assert len(broad_handlers(ast.parse(source))) == 1


def test_decorator_expression_store_precedes_possible_callback():
    source = (
        "from contextlib import suppress as quiet\n@factory(quiet := replacement)\n"
        "def quiet():\n    with quiet(Exception): pass\n"
    )
    assert len(broad_handlers(ast.parse(source))) == 0


@pytest.mark.parametrize("statement, count", [
    ("quiet: object", 1),
    ("quiet: 'deferred annotation'", 1),
    ("quiet: object = replacement", 0),
])
@pytest.mark.parametrize("scope", ["module", "class"])
def test_annotation_without_value_preserves_imported_binding(statement, count, scope):
    source = "from contextlib import suppress as quiet\n" + statement + "\nwith quiet(Exception): pass"
    if scope == "class":
        source = "class C:\n" + "\n".join("    " + line for line in source.splitlines())
    assert len(broad_handlers(ast.parse(source))) == count


def test_annotation_still_makes_a_function_local():
    source = "from contextlib import suppress as quiet\ndef f():\n    quiet: object\n    with quiet(Exception): pass"
    assert len(broad_handlers(ast.parse(source))) == 0


@pytest.mark.parametrize("source, count", [
    ("from contextlib import suppress as quiet\ndef f():\n    global quiet\n    with quiet(Exception): pass\n    quiet = replacement", 1),
    ("from contextlib import suppress as quiet\ndef f():\n    global quiet\n    quiet = replacement\n    with quiet(Exception): pass", 0),
    ("def f():\n    global quiet\n    from contextlib import suppress as quiet\n    with quiet(Exception): pass\n    quiet = replacement\n    with quiet(Exception): pass", 1),
    ("from contextlib import suppress as quiet\ndef f():\n    global quiet\n    if condition:\n        quiet = replacement\n    with quiet(Exception): pass", 1),
    ("from contextlib import suppress as quiet\ndef f():\n    global quiet\n    if condition:\n        quiet = replacement\n        with quiet(Exception): pass", 0),
    ("from contextlib import suppress as quiet\ndef f():\n    global quiet\n    if condition:\n        quiet = replacement\n    else:\n        quiet = other\n    with quiet(Exception): pass", 0),
    ("from contextlib import suppress as quiet\ndef outer():\n    quiet = replacement\n    def inner():\n        global quiet\n        with quiet(Exception): pass", 1),
    ("quiet = replacement\ndef f():\n    global quiet\n    for item in values:\n        with quiet(Exception): pass\n        from contextlib import suppress as quiet", 1),
])
def test_explicit_function_globals_advance_without_local_shadowing(source, count):
    assert len(broad_handlers(ast.parse(source))) == count
