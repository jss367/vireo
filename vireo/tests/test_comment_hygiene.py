"""Comments and docstrings describe the code as it is, not how it got here.

A comment that cites a review (``Codex``, a review-comment id such as
``r3663278142``), a pull request (``PR #2133``, ``#1388``), or the change
that produced the code (``moved verbatim``, ``used to``) tells the reader
about the past, which git history and PR descriptions already hold. State
the invariant, constraint or failure mode the code guards instead, in the
present tense.

``comment_hygiene_baseline.json`` records, per production module (Python
under ``vireo/`` outside ``tests/`` and ``testing/``; JavaScript under
``vireo/static/`` outside ``vendor/`` and ``*.min.js``), how many matches of
each pattern its comments and docstrings hold today, and the counts must
match exactly. A new match fails: reword it. A removed one fails until its
entry is lowered, so a cleanup cannot be quietly undone; run
``python vireo/tests/test_comment_hygiene.py --write-baseline``, which
rewrites the Python entries from the tree and refuses, exiting nonzero
without writing, to raise a count or add one.

Only comments and docstrings are read, never string literals or code:
Python comments are ``tokenize`` COMMENT tokens and docstrings come from
``ast``. ``used to`` directly after a form of "to be" (``is used to``) means
"serves to" and is not counted. The patterns live in
``comment_hygiene_patterns.json``.

This test owns the baseline's ``.py`` entries. The ``.js`` entries belong to
``tests/frontend/comment-hygiene.test.mjs``, which reads comments with
acorn's ``onComment`` and is run by ``npm run test:frontend-checks``; its
``--write-baseline`` rewrites only those, and this writer leaves them as
they are.
"""

import ast
import io
import json
import re
import shutil
import subprocess
import sys
import tokenize
from pathlib import Path

VIREO = Path(__file__).resolve().parents[1]
BASELINE_PATH = Path(__file__).with_name("comment_hygiene_baseline.json")
WRITER = "python vireo/tests/test_comment_hygiene.py --write-baseline"

SKIPPED_PY_DIRS = {"tests", "testing"}

PATTERNS_PATH = Path(__file__).with_name("comment_hygiene_patterns.json")
PATTERNS = {
    name: re.compile(spec["source"], re.I if "i" in spec["flags"] else 0)
    for name, spec in json.loads(PATTERNS_PATH.read_text(encoding="utf-8")).items()
}


# --- comment extraction -----------------------------------------------------

def python_comment_texts(src: str) -> list[str]:
    """Every ``#`` comment and every module, class and function docstring."""
    texts = []
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            docstring = ast.get_docstring(node, clean=False)
            if docstring:
                texts.append(docstring)
    for tok in tokenize.generate_tokens(io.StringIO(src).readline):
        if tok.type == tokenize.COMMENT:
            texts.append(tok.string)
    return texts


# --- counting ---------------------------------------------------------------

def production_files(root: Path = VIREO) -> list[Path]:
    return sorted(
        path for path in root.rglob("*.py")
        if path.relative_to(root).parts[0] not in SKIPPED_PY_DIRS
    )


def count_matches(texts: list[str]) -> dict[str, int]:
    counts = {}
    for name, pattern in PATTERNS.items():
        total = sum(len(pattern.findall(text)) for text in texts)
        if total:
            counts[name] = total
    return counts


def module_counts(root: Path = VIREO) -> dict[str, dict[str, int]]:
    """``{"<path relative to vireo/>": {pattern: matches}}``, nonzero only."""
    result = {}
    for path in production_files(root):
        counts = count_matches(python_comment_texts(path.read_text(encoding="utf-8")))
        if counts:
            result[path.relative_to(root).as_posix()] = counts
    return result


def load_baseline(path: Path = BASELINE_PATH) -> dict[str, dict[str, int]]:
    return json.loads(path.read_text(encoding="utf-8"))


def _python_entries(counts: dict[str, dict[str, int]]) -> dict[str, dict[str, int]]:
    return {module: by_name for module, by_name in counts.items() if module.endswith(".py")}


def _flatten(counts: dict[str, dict[str, int]]) -> dict[tuple[str, str], int]:
    return {(module, name): n for module, by_name in counts.items() for name, n in by_name.items()}


def _describe(keys, before, after) -> str:
    return "\n  ".join(f"{module} [{name}]: {before.get((module, name), 0)} -> {after.get((module, name), 0)}" for module, name in keys)


# --- tests ------------------------------------------------------------------

def test_counts_match_baseline():
    baseline = _flatten(_python_entries(load_baseline()))
    current = _flatten(module_counts())

    grown = sorted(key for key in current if current[key] > baseline.get(key, 0))
    assert not grown, (
        "These comments or docstrings cite a review, a PR, or the code's history. "
        "Say what the code does and why, in the present tense, instead:\n  "
        + _describe(grown, baseline, current)
    )

    shrunk = sorted(key for key in baseline if current.get(key, 0) < baseline[key])
    assert not shrunk, (
        f"These counts dropped but {BASELINE_PATH.name} still holds the old ones, "
        f"which would let them grow back. Run `{WRITER}` and commit the result:\n  "
        + _describe(shrunk, baseline, current)
    )


def test_patterns():
    hits = count_matches([
        "# Codex #1388 P1 r3663278142: the old shape",
        "# moved verbatim from app.py (PR #2133)",
        "// this used to raise; the key is used to look it up",
        "/* &#10003; check mark, see issues/1234, color #333abc */",
    ])
    assert hits == {"codex": 1, "review_id": 1, "pr_ref": 2, "moved_verbatim": 1, "used_to": 1}


def test_only_comments_and_docstrings_are_read():
    py = '"""Codex note."""\nx = "PR #1234"  # used to\ndef f():\n    """r123456789"""\n    return "Codex"\n'
    assert count_matches(python_comment_texts(py)) == {"codex": 1, "used_to": 1, "review_id": 1}


def test_docstrings_and_comments_come_from_the_parser():
    """A ``#`` inside a string is not a comment and a string statement that is
    not a docstring is not read; a docstring with a ``#`` in it is read whole."""
    py = (
        'def f():\n'
        '    """Codex: one."""\n'
        '    x = "# Codex"\n'
        '    "Codex: not a docstring"\n'
        '    return x  # PR #1234\n'
        'class C:\n'
        '    """See PR #42 # Codex"""\n'
    )
    assert count_matches(python_comment_texts(py)) == {"codex": 2, "pr_ref": 2}


def test_baseline_keys_are_python_or_javascript_modules():
    """Each entry belongs to exactly one of the two ratchets that read this file."""
    strays = sorted(key for key in load_baseline() if not key.endswith((".py", ".js")))
    assert not strays, strays


def test_writer_refuses_to_raise_or_add(tmp_path):
    fixture = tmp_path / "vireo"
    (fixture / "tests").mkdir(parents=True)
    script = fixture / "tests" / Path(__file__).name
    shutil.copyfile(__file__, script)
    shutil.copyfile(PATTERNS_PATH, script.with_name(PATTERNS_PATH.name))
    module = fixture / "m.py"
    baseline = script.with_name(BASELINE_PATH.name)

    def write() -> int:
        return subprocess.run([sys.executable, str(script), "--write-baseline"], capture_output=True).returncode

    module.write_text("# Codex: one\n# Codex: two\n", encoding="utf-8")
    assert write() == 0  # no baseline yet: written in full
    assert load_baseline(baseline) == {"m.py": {"codex": 2}}
    js_entry = '  "static/a.js": {"codex": 4},\n'
    baseline.write_text(baseline.read_text(encoding="utf-8").replace("{\n", "{\n" + js_entry, 1), encoding="utf-8")
    before = baseline.read_bytes()
    module.write_text("# Codex: one\n# Codex: two\n# Codex: three\n", encoding="utf-8")
    assert write() == 1 and baseline.read_bytes() == before
    module.write_text("# Codex: one\n# used to\n", encoding="utf-8")
    assert write() == 1 and baseline.read_bytes() == before
    module.write_text("# Codex: one\n", encoding="utf-8")
    assert write() == 0  # the .js entry belongs to the frontend ratchet and is kept
    assert load_baseline(baseline) == {"m.py": {"codex": 1}, "static/a.js": {"codex": 4}}


# --- writer -----------------------------------------------------------------

def _dump(counts: dict[str, dict[str, int]]) -> str:
    lines = [f"  {json.dumps(module)}: {json.dumps(counts[module], sort_keys=True)}" for module in sorted(counts)]
    return "{\n" + ",\n".join(lines) + ("\n" if lines else "") + "}\n"


def write_baseline() -> int:
    current = module_counts()
    others: dict[str, dict[str, int]] = {}
    if BASELINE_PATH.exists():
        existing = load_baseline()
        others = {module: by_name for module, by_name in existing.items() if not module.endswith(".py")}
        before = _flatten(_python_entries(existing))
        after = _flatten(current)
        raised = sorted(key for key in after if after[key] > before.get(key, 0))
        if raised:
            print(
                "Refusing to raise or add baseline counts; reword these comments instead:\n  "
                + _describe(raised, before, after),
                file=sys.stderr,
            )
            return 1
    BASELINE_PATH.write_text(_dump({**others, **current}), encoding="utf-8", newline="\n")
    print(
        f"Wrote the Python entries of {BASELINE_PATH.name}: "
        f"{sum(_flatten(current).values())} matches in {len(current)} modules"
    )
    return 0


if __name__ == "__main__":
    if sys.argv[1:] == ["--write-baseline"]:
        sys.exit(write_baseline())
    print(f"usage: {WRITER}", file=sys.stderr)
    sys.exit(2)
