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
  writer refuses to raise an entry. It accepts a new module's entry only
  when the tree's total does not grow, so handlers moved with their code
  (a module split into a package) keep their count without opening room
  for new ones.
- Under ``DOCUMENTED_ROOTS``, every broad handler must account for what it
  catches: re-raise (a cleanup or rollback guard), log with the traceback
  (``log.exception(...)`` or a log call passing ``exc_info``), or carry a
  ``# broad: <reason>`` comment on the ``except`` line or the comment lines
  directly under it (for ``suppress``, on the ``with`` line or the comment
  lines directly above it). A boundary that stays broad on purpose gets both
  the log and the reason. Add a directory (``"services/"``) or a module
  (``"jobs.py"``) to ``DOCUMENTED_ROOTS`` once its handlers pass.
"""

import ast
import json
import re
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
    "web/caches.py",
    "web/encounters.py",
    "web/highlights.py",
    "web/history.py",
    "web/inat.py",
    "web/keywords.py",
    "web/locations.py",
    "web/media.py",
    "web/photo_edit_recipes.py",
    "web/photos.py",
    "web/predictions.py",
    "web/species.py",
)

BROAD_NAMES = {"Exception", "BaseException"}
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


def _is_broad_suppress(node) -> bool:
    """``suppress(Exception)`` / ``contextlib.suppress(..., BaseException)``."""
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
    return name == "suppress" and any(_is_broad_type(arg) for arg in node.args)


def broad_handlers(tree):
    """Every broad handler in ``tree``: ``("except", handler)`` or ``("suppress", with)``."""
    found = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ExceptHandler) and _is_broad_type(node.type):
            found.append(("except", node))
        elif isinstance(node, (ast.With, ast.AsyncWith)):
            found.extend(
                ("suppress", node) for item in node.items
                if _is_broad_suppress(item.context_expr)
            )
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


def _receiver_name(node) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Call):  # logging.getLogger(__name__).exception(...)
        return _receiver_name(node.func)
    return ""


def _logs_traceback(call) -> bool:
    func = call.func
    if not isinstance(func, ast.Attribute) or func.attr not in LOG_METHODS:
        return False
    if "log" not in _receiver_name(func.value).lower():
        return False
    if func.attr == "exception":
        return True
    for keyword in call.keywords:
        if keyword.arg == "exc_info":
            value = keyword.value
            return not (isinstance(value, ast.Constant) and not value.value)
    return False


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
    for child in _own_nodes(node.body):
        if isinstance(child, ast.Raise):
            return True
        if isinstance(child, ast.Call) and _logs_traceback(child):
            return True
    # The except line through the line before the body's first statement:
    # the clause itself (possibly a multi-line tuple) and comments under it.
    return _has_reason_comment(lines, node.lineno, node.body[0].lineno - 1)


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
    ("with suppress(OSError, BaseException), open(p):\n    f()\n", 1),
    ("with contextlib.suppress(OSError):\n    f()\n", 0),
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
    ("try:\n    f()\nexcept Exception:\n    self._log.error('x', exc_info=exc)\n", True),
    ("try:\n    f()\nexcept Exception:\n    logging.getLogger().exception('x')\n", True),
    ("try:\n    f()\nexcept Exception as e:\n    log.warning('x %s', e)\n", False),
    ("try:\n    f()\nexcept Exception:\n    log.warning('x', exc_info=False)\n", False),
    ("try:\n    f()\nexcept Exception:\n    print('x', exc_info=True)\n", False),
    ("try:\n    f()\nexcept Exception:  # broad: label is cosmetic\n    pass\n", True),
    ("try:\n    f()\nexcept Exception:\n    # broad: label is cosmetic\n    pass\n", True),
    ("try:\n    f()\nexcept Exception:\n    # why\n    # broad: cosmetic\n    pass\n", True),
    ("try:\n    f()\nexcept Exception:  # broad:\n    pass\n", False),
    ("try:\n    f()\nexcept Exception:\n    pass\n    # broad: too late\n", False),
    ("try:\n    f()\nexcept Exception:\n    def g():\n        raise\n", False),
    ("with contextlib.suppress(Exception):\n    f()\n", False),
    ("# broad: probe only\nwith contextlib.suppress(Exception):\n    f()\n", True),
    ("with contextlib.suppress(Exception):  # broad: probe only\n    f()\n", True),
])
def test_accounted_for(source, accounted):
    tree = ast.parse(source)
    [(kind, node)] = broad_handlers(tree)
    assert is_accounted_for(kind, node, source.splitlines()) is accounted


def write_baseline() -> int:
    """Rewrite the baseline from the tree; refuse to raise any entry.

    A module missing from the baseline is admitted only when the tree's
    total does not grow, which is what a move between modules looks like.
    With no baseline file yet, every module is written.
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
    if new and not bootstrapping and sum(current.values()) > sum(previous.values()):
        print(
            "Refusing to add entries for modules that were not in the baseline "
            "while the tree's total grew "
            f"({sum(previous.values())} -> {sum(current.values())}); narrow these handlers:"
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
