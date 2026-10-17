"""Parameter lists only shrink.

A function that takes more than ``MAX_PARAMETERS`` parameters is hard to
call correctly: every call site restates the whole list, a reordered or
misspelled keyword is easy to miss, and the parameters that belong together
are not named as a unit. The functions that already exceed the cap are
listed in ``parameter_count_baseline.json`` with their exact current count.
The test requires each entry to match the function's count today: a larger
count means the list grew, and a smaller one must have its entry lowered, so
a reduction is recorded and cannot quietly be undone later. A function not
listed there may not cross the cap at all.

To get under the cap, group parameters that mean one thing into a frozen
dataclass passed whole, as ``scanner.ScanOptions`` / ``ScanHooks`` /
``ScanControl`` do for ``scan()``: plain settings in one object, callbacks
in another, cancel and pause probes in a third. Group by meaning, not to
hit the number; a mutable accumulator the function fills in stays an
explicit argument.

The baseline shrinks like the function-length one: after removing
parameters, run ``python vireo/tests/test_parameter_count_ratchet.py
--write-baseline``, which rewrites the file from the current tree and
refuses, exiting nonzero without writing, both to raise an entry and to add
one for a function that newly crossed the cap. An entry whose function was
deleted, renamed, moved, or brought under the cap is stale and fails
``test_baseline_has_no_stale_entries`` until it is removed. Because the
writer refuses new entries, a function over the cap that is moved or
renamed keeps its entry only by hand-editing its key in the JSON; the
writer cannot tell a move from a new over-cap function.

What counts as a parameter: every positional-only, positional-or-keyword
and keyword-only parameter, plus ``*args`` and ``**kwargs`` as one each
(a ``**kwargs`` that forwards a dozen options is still one name at the call
site, but it is counted so that hiding a list behind it is not free). The
receiver of a method (``self`` or ``cls``: the first parameter of a
function defined directly in a class body, unless it is a
``@staticmethod``) is not counted. Lambdas are not measured. Keys are
``<path>:<qualname>`` with ``path`` relative to ``vireo/``, the same as the
function-length baseline; duplicate qualified names use ``#<ordinal>-of-<total>`` suffixes. Their
order and group size identify separate budgets; inserting or removing a
duplicate fails closed and requires a reviewed baseline-key migration.
Unique names keep their existing keys.
"""

import ast
import json
import shutil
import subprocess
import sys
from pathlib import Path

VIREO = Path(__file__).resolve().parents[1]
BASELINE_PATH = Path(__file__).with_name("parameter_count_baseline.json")

MAX_PARAMETERS = 12

# Directories under vireo/ that are not production code.
SKIPPED_DIRS = {"tests", "testing"}

WRITER = "python vireo/tests/test_parameter_count_ratchet.py --write-baseline"


def _is_staticmethod(node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    for decorator in node.decorator_list:
        name = decorator.id if isinstance(decorator, ast.Name) else getattr(decorator, "attr", None)
        if name == "staticmethod":
            return True
    return False


def parameter_count(node: ast.FunctionDef | ast.AsyncFunctionDef, *, is_method: bool) -> int:
    """The parameters a caller supplies, as documented in the module docstring."""
    args = node.args
    positional = len(args.posonlyargs) + len(args.args)
    if is_method and positional and not _is_staticmethod(node):
        positional -= 1
    return (
        positional
        + len(args.kwonlyargs)
        + (1 if args.vararg else 0)
        + (1 if args.kwarg else 0)
    )


def _collect_nodes(node, rel_posix, stack, in_class, records):
    for child in ast.iter_child_nodes(node):
        if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
            stack.append(child.name)
            key = f"{rel_posix}:{'.'.join(stack)}"
            count = parameter_count(child, is_method=in_class)
            records.setdefault(key, []).append(count)
            _collect_nodes(child, rel_posix, stack, False, records)
            stack.pop()
        elif isinstance(child, ast.ClassDef):
            stack.append(child.name)
            _collect_nodes(child, rel_posix, stack, True, records)
            stack.pop()
        else:
            # Statements such as ``if`` blocks in a class body keep their
            # functions as methods; anything else passes through unchanged.
            _collect_nodes(child, rel_posix, stack, in_class, records)


def _collect(node, rel_posix: str, stack: list[str], in_class: bool, counts: dict[str, int]) -> None:
    records = {}
    _collect_nodes(node, rel_posix, stack, in_class, records)
    for key, values in records.items():
        for index, count in enumerate(values, 1):
            distinct_key = key if len(values) == 1 else f"{key}#{index}-of-{len(values)}"
            counts[distinct_key] = count


def parameter_counts() -> dict[str, int]:
    """``{"<path>:<qualname>": parameters}`` for every function under vireo/."""
    counts: dict[str, int] = {}
    for path in sorted(VIREO.rglob("*.py")):
        rel = path.relative_to(VIREO)
        if rel.parts[0] in SKIPPED_DIRS:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        _collect(tree, rel.as_posix(), [], False, counts)
    return counts


def over_cap(counts: dict[str, int]) -> dict[str, int]:
    return {key: count for key, count in counts.items() if count > MAX_PARAMETERS}


def load_baseline() -> dict[str, int]:
    return json.loads(BASELINE_PATH.read_text(encoding="utf-8"))


def test_no_function_crosses_or_grows_past_the_cap():
    baseline = load_baseline()
    current = over_cap(parameter_counts())

    new = sorted(key for key in current if key not in baseline)
    assert not new, (
        f"These functions take more than {MAX_PARAMETERS} parameters and are not in "
        f"{BASELINE_PATH.name}. Group parameters that belong together into a frozen "
        "dataclass passed whole (see scanner/options.py: ScanOptions, ScanHooks, "
        "ScanControl) rather than listing them. If a function already in the "
        "baseline was only renamed or moved, rename its key in the JSON by hand:\n  "
        + "\n  ".join(f"{key} ({current[key]} parameters)" for key in new)
    )

    grown = sorted(key for key in current if key in baseline and current[key] > baseline[key])
    assert not grown, (
        "These functions gained parameters; a function over the cap may only lose "
        "them. Pass the new value inside one of the function's existing parameter "
        "objects (or group cohesive parameters into a new frozen dataclass) "
        "instead of raising the entry:\n  "
        + "\n  ".join(f"{key}: {baseline[key]} -> {current[key]} parameters" for key in grown)
    )

    shrunk = sorted(key for key in current if key in baseline and current[key] < baseline[key])
    assert not shrunk, (
        "These functions lost parameters but their baseline entries still hold the "
        f"old count, which would let them grow back. Run `{WRITER}` and commit the "
        f"updated {BASELINE_PATH.name}:\n  "
        + "\n  ".join(f"{key}: {baseline[key]} -> {current[key]} parameters" for key in shrunk)
    )


def test_baseline_has_no_stale_entries():
    baseline = load_baseline()
    current = over_cap(parameter_counts())
    stale = sorted(key for key in baseline if key not in current)
    assert not stale, (
        f"These {BASELINE_PATH.name} entries no longer match a function over the "
        f"cap (deleted, now at or under {MAX_PARAMETERS} parameters, or renamed or "
        f"moved). Run `{WRITER}` to drop them; if the function was renamed or moved "
        "and is still over the cap, rename its key in the JSON by hand instead:\n  "
        + "\n  ".join(stale)
    )


def test_counting_rules():
    """The counting rules the module docstring documents."""
    source = (
        "def plain(a, b, /, c, *args, d, **kwargs): pass\n"
        "class C:\n"
        "    def method(self, a, b): pass\n"
        "    @classmethod\n"
        "    def klass(cls, a): pass\n"
        "    @staticmethod\n"
        "    def static(a, b): pass\n"
        "    if True:\n"
        "        def conditional(self, a): pass\n"
        "    def outer(self):\n"
        "        def inner(a, b): pass\n"
    )
    counts: dict[str, int] = {}
    _collect(ast.parse(source), "m.py", [], False, counts)
    assert counts == {
        "m.py:plain": 6,
        "m.py:C.method": 2,
        "m.py:C.klass": 1,
        "m.py:C.static": 2,
        "m.py:C.conditional": 1,
        "m.py:C.outer": 0,
        "m.py:C.outer.inner": 2,
    }


def test_duplicate_budgets_and_cli_refuse_insertion(tmp_path):
    """Each duplicate can shrink but cannot borrow a sibling's allowance."""
    fixture = tmp_path / "vireo"
    tests = fixture / "tests"
    tests.mkdir(parents=True)
    script = tests / Path(__file__).name
    shutil.copyfile(__file__, script)
    module = fixture / "invented.py"
    baseline = tests / "parameter_count_baseline.json"
    def definitions(*counts):
        return "".join("def f(" + ",".join(f"a{i}" for i in range(count)) + "): pass\n" for count in counts)
    module.write_text(definitions(13, 15), encoding="utf-8")
    expected = {"invented.py:f#1-of-2": 13, "invented.py:f#2-of-2": 15}
    baseline.write_text(json.dumps(expected), encoding="utf-8")
    def write():
        return subprocess.run([sys.executable, str(script), "--write-baseline"], capture_output=True).returncode
    assert write() == 0
    previous = baseline.read_bytes()
    module.write_text(definitions(14, 15), encoding="utf-8")
    assert write() == 1 and baseline.read_bytes() == previous
    module.write_text(definitions(12, 15), encoding="utf-8")
    assert write() == 0
    assert json.loads(baseline.read_text(encoding="utf-8")) == {"invented.py:f#2-of-2": 15}
    previous = baseline.read_bytes()
    module.write_text(definitions(1, 12, 15), encoding="utf-8")
    assert write() == 1 and baseline.read_bytes() == previous


def write_baseline() -> int:
    """Rewrite the baseline from the tree; refuse to raise or add any entry.

    With no baseline file yet (a first run, or reuse of this ratchet in a
    fresh tree) every over-cap function is written, since there is nothing
    to ratchet against.
    """
    current = over_cap(parameter_counts())
    bootstrapping = not BASELINE_PATH.exists()
    previous = {} if bootstrapping else load_baseline()
    grown = sorted(key for key in current if key in previous and current[key] > previous[key])
    if grown:
        print("Refusing to raise baseline entries; remove parameters from these functions instead:")
        for key in grown:
            print(f"  {key}: {previous[key]} -> {current[key]} parameters")
        return 1
    new = sorted(key for key in current if key not in previous)
    if new and not bootstrapping:
        print(
            "Refusing to add baseline entries; group these functions' parameters "
            "into parameter objects instead (a moved or renamed function keeps its "
            "entry by renaming its key in the JSON by hand):"
        )
        for key in new:
            print(f"  {key} ({current[key]} parameters)")
        return 1
    BASELINE_PATH.write_text(
        json.dumps(dict(sorted(current.items())), indent=2) + "\n", encoding="utf-8",
    )
    print(f"wrote {len(current)} entries to {BASELINE_PATH}")
    return 0


if __name__ == "__main__":
    if sys.argv[1:] != ["--write-baseline"]:
        sys.exit(f"usage: {sys.argv[0]} --write-baseline")
    sys.exit(write_baseline())
