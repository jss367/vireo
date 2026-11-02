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
rewrites the file from the tree and refuses, exiting nonzero without
writing, to raise a count or add one.

Only comments and docstrings are read, never string literals or code.
``used to`` directly after a form of "to be" (``is used to``) means "serves
to" and is not counted.
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

import pytest

VIREO = Path(__file__).resolve().parents[1]
BASELINE_PATH = Path(__file__).with_name("comment_hygiene_baseline.json")
WRITER = "python vireo/tests/test_comment_hygiene.py --write-baseline"

SKIPPED_PY_DIRS = {"tests", "testing"}

PATTERNS = {
    "codex": re.compile(r"\bcodex\b", re.I),
    "review_id": re.compile(r"\br\d{9,}\b"),
    "pr_ref": re.compile(r"\bPR\s*#\d+|(?<![\w&#/])#\d{3,}\b(?![\w-])"),
    "moved_verbatim": re.compile(r"\bmoved\s+verbatim\b", re.I),
    "used_to": re.compile(r"(?<!\bis )(?<!\bare )(?<!\bbe )(?<!\bbeen )(?<!\bbeing )\bused\s+to\b", re.I),
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


_JS_REGEX_AFTER_WORD = {
    "return", "typeof", "instanceof", "in", "of", "new", "delete", "void", "throw",
    "case", "do", "else", "yield", "await",
}
_JS_IDENT = re.compile(r"[A-Za-z_$\u0080-￿][\w$\u0080-￿]*")
_JS_NUMBER = re.compile(r"0[xXoObB][\da-fA-F_]+n?|(?:\d[\d_]*\.?[\d_]*|\.\d[\d_]*)(?:[eE][+-]?\d+)?n?")


def js_comment_texts(src: str) -> list[str]:
    """Comment bodies in ``src``, in order, as acorn's ``onComment`` reports them.

    A small lexer that skips strings, template literals (with nested
    substitutions) and regular-expression literals, so ``//`` inside a URL
    string is not a comment. ``test_js_lexer_matches_acorn`` checks it
    against acorn on every first-party script when acorn is installed.
    """
    comments: list[str] = []
    i, n = 0, len(src)
    regex_ok = True
    braces: list[str] = []

    def template(j: int) -> tuple[int, bool]:
        while j < n:
            if src[j] == "\\":
                j += 2
            elif src[j] == "`":
                return j + 1, False
            elif src.startswith("${", j):
                braces.append("${")
                return j + 2, True
            else:
                j += 1
        return j, False

    while i < n:
        c = src[i]
        if c.isspace() or c == "﻿":
            i += 1
        elif src.startswith("//", i):
            end = src.find("\n", i)
            end = n if end < 0 else end
            comments.append(src[i + 2 : end].rstrip("\r"))
            i = end
        elif src.startswith("/*", i):
            end = src.find("*/", i + 2)
            end = n if end < 0 else end
            comments.append(src[i + 2 : end])
            i = end + 2
        elif c in "'\"":
            j = i + 1
            while j < n and src[j] != c:
                j += 2 if src[j] == "\\" else 1
            i, regex_ok = j + 1, False
        elif c == "`" or (c == "}" and braces and braces[-1] == "${"):
            if c == "}":
                braces.pop()
            i, regex_ok = template(i + 1)
        elif c == "/" and regex_ok:
            j, in_class = i + 1, False
            while j < n and src[j] != "\n":
                if src[j] == "\\":
                    j += 1
                elif src[j] == "[":
                    in_class = True
                elif src[j] == "]":
                    in_class = False
                elif src[j] == "/" and not in_class:
                    break
                j += 1
            j += 1
            while j < n and (src[j].isalnum() or src[j] in "_$"):
                j += 1
            i, regex_ok = j, False
        elif c in "{}":
            if c == "{":
                braces.append("{")
            elif braces:
                braces.pop()
            # After a block's ``}`` a ``/`` starts a regex; that is far more
            # common in these scripts than dividing an object literal.
            i, regex_ok = i + 1, True
        elif c in ")]":
            i, regex_ok = i + 1, False
        elif match := _JS_IDENT.match(src, i):
            i, regex_ok = match.end(), match.group() in _JS_REGEX_AFTER_WORD
        elif (c.isdigit() or (c == "." and src[i + 1 : i + 2].isdigit())) and (match := _JS_NUMBER.match(src, i)):
            i, regex_ok = match.end(), False
        elif src.startswith(("++", "--"), i):
            i += 2
        else:
            i, regex_ok = i + 1, True
    return comments


# --- counting ---------------------------------------------------------------

def production_files(root: Path = VIREO) -> list[Path]:
    files = [
        path for path in root.rglob("*.py")
        if path.relative_to(root).parts[0] not in SKIPPED_PY_DIRS
    ]
    static = root / "static"
    if static.is_dir():
        files += [
            path for path in static.rglob("*.js")
            if "vendor" not in path.relative_to(static).parts and not path.name.endswith(".min.js")
        ]
    return sorted(files)


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
        src = path.read_text(encoding="utf-8")
        texts = js_comment_texts(src) if path.suffix == ".js" else python_comment_texts(src)
        counts = count_matches(texts)
        if counts:
            result[path.relative_to(root).as_posix()] = counts
    return result


def load_baseline(path: Path = BASELINE_PATH) -> dict[str, dict[str, int]]:
    return json.loads(path.read_text(encoding="utf-8"))


def _flatten(counts: dict[str, dict[str, int]]) -> dict[tuple[str, str], int]:
    return {(module, name): n for module, by_name in counts.items() for name, n in by_name.items()}


def _describe(keys, before, after) -> str:
    return "\n  ".join(f"{module} [{name}]: {before.get((module, name), 0)} -> {after.get((module, name), 0)}" for module, name in keys)


# --- tests ------------------------------------------------------------------

def test_counts_match_baseline():
    baseline = _flatten(load_baseline())
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
    js = "const u = 'http://x/#1234'; // Codex\nconst t = `a ${'b' /* PR #12 */} //c`;\nconst r = /\\/\\//g; /* used to */\n"
    assert js_comment_texts(js) == [" Codex", " PR #12 ", " used to "]


def test_js_lexer_matches_acorn():
    """The Python lexer finds exactly the comments acorn finds."""
    repo = VIREO.parent
    node = shutil.which("node")
    if not node or not (repo / "node_modules" / "acorn").is_dir():
        pytest.skip("node and acorn (npm ci) are needed for the cross-check")
    script = (
        "const acorn = require(process.argv[1] + '/node_modules/acorn');"
        "const fs = require('fs'); const out = {};"
        "for (const f of process.argv.slice(2)) {"
        "  const src = fs.readFileSync(f, 'utf8'); const c = [];"
        "  const o = {ecmaVersion: 'latest', onComment: (b, t) => c.push(t)};"
        "  try { [...acorn.tokenizer(src, {...o, sourceType: 'script'})]; }"
        "  catch (e) { c.length = 0; [...acorn.tokenizer(src, {...o, sourceType: 'module'})]; }"
        "  out[f] = c; }"
        "process.stdout.write(JSON.stringify(out));"
    )
    files = [str(path) for path in production_files() if path.suffix == ".js"]
    result = subprocess.run([node, "-e", script, str(repo), *files], capture_output=True, text=True, check=True)
    expected = json.loads(result.stdout)
    mismatched = [f for f in files if js_comment_texts(Path(f).read_text(encoding="utf-8")) != expected[f]]
    assert not mismatched, mismatched


def test_writer_refuses_to_raise_or_add(tmp_path):
    fixture = tmp_path / "vireo"
    (fixture / "tests").mkdir(parents=True)
    script = fixture / "tests" / Path(__file__).name
    shutil.copyfile(__file__, script)
    module = fixture / "m.py"
    baseline = script.with_name(BASELINE_PATH.name)

    def write() -> int:
        return subprocess.run([sys.executable, str(script), "--write-baseline"], capture_output=True).returncode

    module.write_text("# Codex: one\n# Codex: two\n", encoding="utf-8")
    assert write() == 0  # no baseline yet: written in full
    assert load_baseline(baseline) == {"m.py": {"codex": 2}}
    before = baseline.read_bytes()
    module.write_text("# Codex: one\n# Codex: two\n# Codex: three\n", encoding="utf-8")
    assert write() == 1 and baseline.read_bytes() == before
    module.write_text("# Codex: one\n# used to\n", encoding="utf-8")
    assert write() == 1 and baseline.read_bytes() == before
    module.write_text("# Codex: one\n", encoding="utf-8")
    assert write() == 0
    assert load_baseline(baseline) == {"m.py": {"codex": 1}}


# --- writer -----------------------------------------------------------------

def _dump(counts: dict[str, dict[str, int]]) -> str:
    lines = [f"  {json.dumps(module)}: {json.dumps(counts[module], sort_keys=True)}" for module in sorted(counts)]
    return "{\n" + ",\n".join(lines) + ("\n" if lines else "") + "}\n"


def write_baseline() -> int:
    current = module_counts()
    if BASELINE_PATH.exists():
        before = _flatten(load_baseline())
        after = _flatten(current)
        raised = sorted(key for key in after if after[key] > before.get(key, 0))
        if raised:
            print(
                "Refusing to raise or add baseline counts; reword these comments instead:\n  "
                + _describe(raised, before, after),
                file=sys.stderr,
            )
            return 1
    BASELINE_PATH.write_text(_dump(current), encoding="utf-8", newline="\n")
    print(f"Wrote {BASELINE_PATH.name}: {sum(_flatten(current).values())} matches in {len(current)} modules")
    return 0


if __name__ == "__main__":
    if sys.argv[1:] == ["--write-baseline"]:
        sys.exit(write_baseline())
    print(f"usage: {WRITER}", file=sys.stderr)
    sys.exit(2)
