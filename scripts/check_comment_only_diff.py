"""Check that a diff changes only comments and docstrings.

For every Python file (``.py``) the diff touches, the AST with docstrings
removed must dump identically on both sides and the shebang and ``# coding:``
declaration must match (both pick the interpreter or decoder that runs the
file, and the AST drops them). For every JavaScript file (``.js``, ``.mjs``,
``.cjs``), acorn's token stream (comments excluded) must be identical: each
token's type label and the exact source text between its ``start`` and ``end``
positions are compared, so a cooked value shared by two different spellings
(``"use strict"`` vs ``"use\\x20strict"`` as a directive prologue, or
``tag`\\x41`` vs ``tag`A`` as a tagged-template raw) is detected. The
hashbang line is compared separately because acorn drops it. Comments that
tools read are compared too, because the AST and the token stream both drop
them: Python ``noqa`` (``ruff: noqa`` too, and ``NOQA`` case-insensitively) /
``type:`` / ``pyright:`` / ``pragma`` / ``fmt:`` / ``broad:`` pragmas, ESLint
and ``@ts-`` directives (``@ts-expect-error`` included), and JSDoc type tags
with ``@import`` module sources captured separately so changing the source is
detected. Added, deleted and renamed source files are compared the same way,
against an empty counterpart (or across both paths for a rename), so a diff
is comment-only only when every A/M/D/R source file is. A rename to or from a
non-source suffix (``module.py`` -> ``module.txt``) is treated as a deletion
or addition of the source file so the rename cannot hide a removed executable
module.

    python scripts/check_comment_only_diff.py [--base origin/main]

Compares the merge base with ``--base`` against the working tree. Exits
nonzero, listing the files, when any of them changed code. JavaScript needs
``npm ci`` (acorn) in the repo root.
"""

import argparse
import ast
import io
import json
import os
import re
import subprocess
import sys
import tokenize
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

PY_EXTS = (".py",)
JS_EXTS = (".js", ".mjs", ".cjs")
SOURCE_EXTS = PY_EXTS + JS_EXTS

PY_PRAGMA = re.compile(
    r"#\s*(ruff:\s*noqa\b[^#]*|noqa\b[^#]*|type:\s*\S[^#]*|pyright:[^#]*|pylint:[^#]*|pragma\b[^#]*|fmt:\s*\w+|broad:[^#]*)",
    re.I,
)
PY_CODING = re.compile(r"coding[=:]\s*([-\w.]+)")
JS_DIRECTIVE = re.compile(
    r"^\s*(eslint[\w-]*\b.*|@ts-[\w-]+\b.*|global\s.*|globals\s.*|exported\s.*)$", re.S
)
JSDOC_TAG = re.compile(r"@(\w+)\s*(\{[^\n]*\})?\s*(\[?[\w$.]*)")
JSDOC_IMPORT_SRC = re.compile(r"""@import\b[^\n]*?(['"][^'"\n]+['"])""")

JS_TOKENS = r"""
const acorn = require(process.argv[1] + '/node_modules/acorn');
const fs = require('fs');
const src = fs.readFileSync(0, 'utf8');
const comments = [];
const parse = (sourceType) => {
  comments.length = 0;
  const opts = {ecmaVersion: 'latest', sourceType, onComment: (block, text) => comments.push(text)};
  return [...acorn.tokenizer(src, opts)].map((t) => [t.type.label, src.slice(t.start, t.end)]);
};
let tokens;
try { tokens = parse('script'); } catch (e) { tokens = parse('module'); }
process.stdout.write(JSON.stringify({tokens, comments}));
"""


def _strip_docstrings(tree: ast.AST) -> ast.AST:
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            body = node.body
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                node.body = body[1:]
    return tree


def _py_pragmas(src: str) -> Counter:
    found: Counter = Counter()
    if not src:
        return found
    for tok in tokenize.generate_tokens(io.StringIO(src).readline):
        if tok.type == tokenize.COMMENT:
            for match in PY_PRAGMA.finditer(tok.string):
                found[re.sub(r"\s+", " ", match.group(1).strip())] += 1
    return found


def _py_shebang(src: str) -> str:
    return src.splitlines()[0] if src.startswith("#!") else ""


def _py_coding(src: str) -> str:
    for line in src.splitlines()[:2]:
        stripped = line.lstrip()
        if stripped.startswith("#"):
            match = PY_CODING.search(stripped)
            if match:
                return match.group(1).lower()
    return ""


def python_differs(old: str, new: str) -> str | None:
    if _py_shebang(old) != _py_shebang(new):
        return f"shebang changed: {_py_shebang(old)!r} -> {_py_shebang(new)!r}"
    if _py_coding(old) != _py_coding(new):
        return f"source encoding changed: {_py_coding(old)!r} -> {_py_coding(new)!r}"
    old_tree = ast.parse(old) if old else ast.Module(body=[], type_ignores=[])
    new_tree = ast.parse(new) if new else ast.Module(body=[], type_ignores=[])
    if ast.dump(_strip_docstrings(old_tree)) != ast.dump(_strip_docstrings(new_tree)):
        return "code (AST without docstrings) changed"
    if _py_pragmas(old) != _py_pragmas(new):
        return f"tool pragmas changed: {_py_pragmas(old) - _py_pragmas(new)} -> {_py_pragmas(new) - _py_pragmas(old)}"
    return None


def _js(src: str) -> dict:
    if not src:
        return {"tokens": [], "comments": []}
    res = subprocess.run(
        ["node", "-e", JS_TOKENS, str(ROOT)], input=src, capture_output=True, encoding="utf-8", check=True
    )
    return json.loads(res.stdout)


def _js_load_bearing(comments: list[str]) -> Counter:
    found: Counter = Counter()
    for text in comments:
        if JS_DIRECTIVE.match(text):
            found["directive " + re.sub(r"\s+", " ", text.strip())] += 1
        if text.startswith("*"):
            for tag in JSDOC_TAG.findall(text):
                found["jsdoc " + " ".join(part for part in tag if part)] += 1
            for src in JSDOC_IMPORT_SRC.findall(text):
                found["jsdoc import source " + src] += 1
    return found


def _js_shebang(src: str) -> str:
    return src.splitlines()[0] if src.startswith("#!") else ""


def js_differs(old: str, new: str) -> str | None:
    if _js_shebang(old) != _js_shebang(new):
        return f"shebang changed: {_js_shebang(old)!r} -> {_js_shebang(new)!r}"
    before, after = _js(old), _js(new)
    if before["tokens"] != after["tokens"]:
        return "code (token stream without comments) changed"
    if _js_load_bearing(before["comments"]) != _js_load_bearing(after["comments"]):
        return "ESLint/TypeScript directives or JSDoc type tags changed"
    return None


def _git(*args: str) -> str:
    path_records = "-z" in args
    result = subprocess.run(
        ["git", *args], cwd=ROOT, capture_output=True,
        encoding=None if path_records else "utf-8", check=True,
    )
    if not path_records:
        return result.stdout  # Same UTF-8/universal-newline contract as read_text.
    # Git for Windows emits UTF-8 paths; POSIX records retain filesystem bytes.
    return result.stdout.decode("utf-8") if os.name == "nt" else os.fsdecode(result.stdout)


def _suffix_of(path: str) -> str:
    return next((s for s in SOURCE_EXTS if path.endswith(s)), "")


def _source_changes(base: str) -> list[tuple[str, str, str, str]]:
    """``(label, old_path, new_path, suffix)`` for every A/M/D/R source file.

    Both sides of a rename are inspected so a ``module.py`` renamed to a
    non-source suffix (``module.txt``) is still reported as a source deletion
    instead of silently dropped. An entry whose status is not A/M/D/R, or
    whose rename crosses languages (``.py`` <-> ``.js``), is returned with a
    sentinel suffix so ``main()`` fails on it rather than certifying it. The
    ``-z`` flag emits NUL-separated records so a non-ASCII, quote, tab,
    backslash or newline in a path cannot be dropped by ``core.quotePath``.
    """
    out: list[tuple[str, str, str, str]] = []
    tokens = _git("diff", "--name-status", "-z", base, "--").split("\0")
    idx = 0
    while idx < len(tokens) and tokens[idx]:
        status = tokens[idx][0]
        n = 2 if status in ("R", "C") else 1
        paths = tokens[idx + 1 : idx + 1 + n]
        idx += 1 + n
        if status in ("R", "C") and len(paths) == 2:
            old_path, new_path = paths[0], paths[1]
            label = f"{old_path} -> {new_path}"
        elif status == "D" and len(paths) == 1:
            old_path, new_path = paths[0], ""
            label = f"{old_path} (deleted)"
        elif status == "A" and len(paths) == 1:
            old_path, new_path = "", paths[0]
            label = f"{new_path} (added)"
        elif status == "M" and len(paths) == 1:
            old_path = new_path = paths[0]
            label = paths[0]
        else:
            out.append((paths[-1] if paths else "", "", "", status))
            continue
        old_suffix = _suffix_of(old_path)
        new_suffix = _suffix_of(new_path)
        if not (old_suffix or new_suffix):
            continue
        old_is_js = old_suffix in JS_EXTS
        new_is_js = new_suffix in JS_EXTS
        if old_suffix and new_suffix and old_is_js != new_is_js:
            out.append((label, old_path, new_path, "mixed"))
            continue
        suffix = new_suffix or old_suffix
        out.append((label, old_path, new_path, suffix))
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--base", default="origin/main")
    base = _git("merge-base", parser.parse_args().base, "HEAD").strip()
    entries = _source_changes(base)
    failures: list[str] = []
    labels: list[str] = []
    for label, old_path, new_path, suffix in entries:
        labels.append(label)
        if suffix == "mixed":
            failures.append(f"{label}: cross-language rename")
            continue
        if suffix not in SOURCE_EXTS:
            failures.append(f"{label}: unsupported change kind {suffix!r}")
            continue
        old = _git("show", f"{base}:{old_path}") if old_path and _suffix_of(old_path) else ""
        new = (
            (ROOT / new_path).read_text(encoding="utf-8")
            if new_path and _suffix_of(new_path)
            else ""
        )
        reason = js_differs(old, new) if suffix in JS_EXTS else python_differs(old, new)
        if reason:
            failures.append(f"{label}: {reason}")
    failed_labels = {failure.split(":", 1)[0] for failure in failures}
    for label in labels:
        print(f"FAIL {label}" if label in failed_labels else f"ok   {label}")
    print(f"{len(labels) - len(failures)}/{len(labels)} source changes are comment-only")
    for failure in failures:
        print("  " + failure, file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
