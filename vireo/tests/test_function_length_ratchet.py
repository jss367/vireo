"""Function length only shrinks.

A function longer than ``MAX_FUNCTION_LINES`` is hard to read, test and
review in one sitting. The functions that already exceed the cap are listed
in ``function_length_baseline.json`` with their exact current length. The
test requires each entry to match the function's length today: a longer
function has grown, and a shorter one must have its entry lowered, so a
shrink is recorded and cannot quietly be undone later. A function not listed
there may not cross the cap at all. Split the function (or move part of it
into a helper) instead of adding to the baseline.

The baseline shrinks like the basedpyright one: after shortening a function,
run ``python vireo/tests/test_function_length_ratchet.py --write-baseline``,
which rewrites the file from the current tree and refuses, exiting nonzero
without writing, both to raise an entry and to add one for a function that
newly crossed the cap. An entry whose function was deleted, renamed, moved,
or brought under the cap is stale and fails
``test_baseline_has_no_stale_entries`` until it is removed, so the list
reflects what is actually left to do. Because the writer refuses new
entries, a long function that is moved or renamed keeps its entry only by
hand-editing its key in the JSON; the writer cannot tell a move from a
new over-cap function.

Two kinds of function are exempt:

- The blueprint factories (``create_<domain>_blueprint``) hold every route of
  a domain as a closure; that is the injection convention in CLAUDE.md, not a
  function doing too much. Their routes are measured individually.
- ``CanonicalSchema.create_tables`` in ``canonical_schema.py`` is the schema
  DDL in one place.

Lengths are counted from the ``def`` line to the last line of the body,
including docstrings and comments, so the measure is the same on every
platform and needs no tooling beyond ``ast``.
"""

import ast
import json
import sys
from pathlib import Path

VIREO = Path(__file__).resolve().parents[1]
BASELINE_PATH = Path(__file__).with_name("function_length_baseline.json")

MAX_FUNCTION_LINES = 100

# Directories under vireo/ that are not production code.
SKIPPED_DIRS = {"tests", "testing"}


def is_exempt(path: str, qualname: str) -> bool:
    name = qualname.rsplit(".", 1)[-1]
    if name.startswith("create_") and name.endswith("_blueprint"):
        return True
    return path == "canonical_schema.py" and qualname == "CanonicalSchema.create_tables"


def _collect(node, rel_posix: str, stack: list[str], lengths: dict[str, int]) -> None:
    for child in ast.iter_child_nodes(node):
        if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            stack.append(child.name)
            if not isinstance(child, ast.ClassDef):
                key = f"{rel_posix}:{'.'.join(stack)}"
                length = child.end_lineno - child.lineno + 1
                lengths[key] = max(length, lengths.get(key, 0))
            _collect(child, rel_posix, stack, lengths)
            stack.pop()
        else:
            _collect(child, rel_posix, stack, lengths)


def function_lengths() -> dict[str, int]:
    """``{"<path>:<qualname>": lines}`` for every function under vireo/.

    ``path`` is relative to ``vireo/`` with forward slashes; ``qualname``
    nests classes and enclosing functions with dots. Two functions with the
    same key (a redefinition, or same-named closures in sibling branches)
    keep the longer length.
    """
    lengths: dict[str, int] = {}
    for path in sorted(VIREO.rglob("*.py")):
        rel = path.relative_to(VIREO)
        if rel.parts[0] in SKIPPED_DIRS:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        _collect(tree, rel.as_posix(), [], lengths)
    return lengths


def over_cap(lengths: dict[str, int]) -> dict[str, int]:
    return {
        key: length for key, length in lengths.items()
        if length > MAX_FUNCTION_LINES and not is_exempt(*key.split(":", 1))
    }


def load_baseline() -> dict[str, int]:
    return json.loads(BASELINE_PATH.read_text(encoding="utf-8"))


def test_no_function_crosses_or_grows_past_the_cap():
    baseline = load_baseline()
    current = over_cap(function_lengths())

    new = sorted(key for key in current if key not in baseline)
    assert not new, (
        f"These functions are longer than {MAX_FUNCTION_LINES} lines and not in "
        f"{BASELINE_PATH.name}; split them rather than listing them:\n  "
        + "\n  ".join(f"{key} ({current[key]} lines)" for key in new)
    )

    grown = sorted(key for key in current if key in baseline and current[key] > baseline[key])
    assert not grown, (
        "These functions grew; a function over the cap may only get shorter:\n  "
        + "\n  ".join(f"{key}: {baseline[key]} -> {current[key]} lines" for key in grown)
    )

    shrunk = sorted(key for key in current if key in baseline and current[key] < baseline[key])
    assert not shrunk, (
        "These functions got shorter but their baseline entries still hold the "
        "old length, which would let them grow back; run this module with "
        "--write-baseline to lower the entries:\n  "
        + "\n  ".join(f"{key}: {baseline[key]} -> {current[key]} lines" for key in shrunk)
    )


def test_baseline_has_no_stale_entries():
    baseline = load_baseline()
    current = over_cap(function_lengths())
    stale = sorted(key for key in baseline if key not in current)
    assert not stale, (
        f"These {BASELINE_PATH.name} entries no longer match a function over the "
        "cap (deleted, renamed, moved, or now short enough); delete them, or run "
        "this module with --write-baseline:\n  " + "\n  ".join(stale)
    )


def write_baseline() -> int:
    """Rewrite the baseline from the tree; refuse to raise or add any entry.

    With no baseline file yet (a first run, or reuse of this ratchet in a
    fresh tree) every over-cap function is written, since there is nothing
    to ratchet against.
    """
    current = over_cap(function_lengths())
    bootstrapping = not BASELINE_PATH.exists()
    previous = {} if bootstrapping else load_baseline()
    grown = sorted(key for key in current if key in previous and current[key] > previous[key])
    if grown:
        print("Refusing to raise baseline entries; shorten these functions instead:")
        for key in grown:
            print(f"  {key}: {previous[key]} -> {current[key]} lines")
        return 1
    new = sorted(key for key in current if key not in previous)
    if new and not bootstrapping:
        print(
            "Refusing to add baseline entries; split these functions instead "
            "(a moved or renamed long function keeps its entry by renaming "
            "its key in the JSON by hand):"
        )
        for key in new:
            print(f"  {key} ({current[key]} lines)")
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
