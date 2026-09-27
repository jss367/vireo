"""Broad exception handlers in production code must not fail silently.

A handler that catches ``Exception`` / ``BaseException`` (or is bare) has to
do at least one of: re-raise, log, or carry a comment saying why swallowing is
right there (the error is handed to a caller, it is teardown noise, ...).
``contextlib.suppress(Exception)`` is the same swallow spelled differently, so
it needs the reason comment too.

ruff's S110/S112 cover the ``except Exception: pass`` / ``continue`` shapes;
this covers the rest (``return None``, a fallback assignment, ``suppress``).
Narrowing the handler to the exceptions the body can actually raise is
usually the better fix than adding a comment.
"""

import ast
import io
import os
import tokenize

VIREO_DIR = os.path.join(os.path.dirname(__file__), "..")
BROAD = {"Exception", "BaseException"}
LOG_METHODS = {"debug", "info", "warning", "error", "exception", "critical", "log"}
LOGGER_NAMES = {"log", "logger", "logging", "_log", "_logger", "LOG"}


def _production_files():
    for dirpath, dirnames, filenames in os.walk(VIREO_DIR):
        dirnames[:] = [d for d in dirnames if d not in ("tests", "__pycache__")]
        for name in filenames:
            if name.endswith(".py"):
                yield os.path.join(dirpath, name)


def _names(node):
    if node is None:
        return {"BaseException"}  # bare ``except:``
    if isinstance(node, ast.Tuple):
        return set().union(*(_names(e) for e in node.elts))
    if isinstance(node, ast.Name):
        return {node.id}
    if isinstance(node, ast.Attribute):
        return {node.attr}
    return set()


def _is_log_call(call):
    func = call.func
    if not isinstance(func, ast.Attribute) or func.attr not in LOG_METHODS:
        return False
    base = func.value
    if isinstance(base, ast.Name):
        return base.id in LOGGER_NAMES
    return isinstance(base, ast.Attribute) and base.attr in LOGGER_NAMES


def _handler_reports(handler):
    for stmt in handler.body:
        for node in ast.walk(stmt):
            if isinstance(node, ast.Raise):
                return True
            if isinstance(node, ast.Call) and _is_log_call(node):
                return True
    return False


def _comment_lines(source):
    lines = set()
    for tok in tokenize.generate_tokens(io.StringIO(source).readline):
        if tok.type == tokenize.COMMENT:
            lines.add(tok.start[0])
    return lines


def _source_violations(source, rel):
    found = []
    comments = _comment_lines(source)
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.ExceptHandler):
            if not (_names(node.type) & BROAD) or _handler_reports(node):
                continue
            span = range(node.lineno, (node.end_lineno or node.lineno) + 1)
            if not any(line in comments for line in span):
                found.append(f"{rel}:{node.lineno} broad except neither logs, re-raises, nor says why")
        elif isinstance(node, ast.withitem):
            call = node.context_expr
            if not (
                isinstance(call, ast.Call)
                and isinstance(call.func, ast.Attribute)
                and call.func.attr == "suppress"
                and any(_names(arg) & BROAD for arg in call.args)
            ):
                continue
            line = call.lineno
            if line not in comments and line - 1 not in comments:
                found.append(f"{rel}:{line} contextlib.suppress(Exception) without a reason comment")
    return found


def _violations():
    found = []
    for path in _production_files():
        with open(path, encoding="utf-8") as f:
            source = f.read()
        found += _source_violations(source, os.path.relpath(path, VIREO_DIR))
    return sorted(found)


def test_policy_flags_silent_swallows_and_accepts_reported_ones():
    source = (
        "import contextlib\n"
        "def silent():\n"
        "    try:\n"
        "        f()\n"
        "    except Exception:\n"
        "        return None\n"
        "def quiet():\n"
        "    with contextlib.suppress(Exception):\n"
        "        f()\n"
        "def logged():\n"
        "    try:\n"
        "        f()\n"
        "    except Exception:\n"
        "        log.debug('f failed', exc_info=True)\n"
        "def explained():\n"
        "    try:\n"
        "        f()\n"
        "    except BaseException as exc:\n"
        "        # handed to the caller\n"
        "        box.append(exc)\n"
        "def narrow():\n"
        "    try:\n"
        "        f()\n"
        "    except OSError:\n"
        "        return None\n"
    )
    assert _source_violations(source, "x.py") == [
        "x.py:5 broad except neither logs, re-raises, nor says why",
        "x.py:8 contextlib.suppress(Exception) without a reason comment",
    ]


def test_broad_excepts_log_reraise_or_explain():
    violations = _violations()
    assert not violations, (
        "Broad exception handlers must re-raise, log, or carry a comment "
        "explaining the swallow (better: narrow them):\n  " + "\n  ".join(violations)
    )
