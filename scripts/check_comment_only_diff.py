"""Check that a diff changes only comments and docstrings.

For every Python file the diff touches, the AST with docstrings removed must
dump identically on both sides. For every JavaScript file, acorn's token
stream (comments excluded) must be identical. Comments that tools read are
compared too, because the AST and the token stream both drop them: Python
``noqa`` / ``type:`` / ``pyright:`` / ``pragma`` / ``fmt:`` / ``broad:``
pragmas, ESLint and ``@ts-`` directives, and JSDoc type tags.

    python scripts/check_comment_only_diff.py [--base origin/main]

Compares the merge base with ``--base`` against the working tree. Exits
nonzero, listing the files, when any of them changed code. JavaScript needs
``npm ci`` (acorn) in the repo root.
"""

import argparse
import ast
import io
import json
import re
import subprocess
import sys
import tokenize
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

PY_PRAGMA = re.compile(r"#\s*(noqa\b[^#]*|type:\s*\S[^#]*|pyright:[^#]*|pragma\b[^#]*|fmt:\s*\w+|broad:)")
JS_DIRECTIVE = re.compile(r"^\s*(eslint[\w-]*\b.*|@ts-\w+|global\s.*|globals\s.*)$", re.S)
JSDOC_TAG = re.compile(r"@(\w+)\s*(\{[^\n]*\})?\s*(\[?[\w$.]*)")

JS_TOKENS = r"""
const acorn = require(process.argv[1] + '/node_modules/acorn');
const fs = require('fs');
const src = fs.readFileSync(0, 'utf8');
const comments = [];
const parse = (sourceType) => {
  comments.length = 0;
  const opts = {ecmaVersion: 'latest', sourceType, onComment: (block, text) => comments.push(text)};
  return [...acorn.tokenizer(src, opts)].map((t) => [t.type.label, t.value === undefined ? null : String(t.value)]);
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
    for tok in tokenize.generate_tokens(io.StringIO(src).readline):
        if tok.type == tokenize.COMMENT:
            for match in PY_PRAGMA.finditer(tok.string):
                found[re.sub(r"\s+", " ", match.group(1).strip())] += 1
    return found


def python_differs(old: str, new: str) -> str | None:
    if ast.dump(_strip_docstrings(ast.parse(old))) != ast.dump(_strip_docstrings(ast.parse(new))):
        return "code (AST without docstrings) changed"
    if _py_pragmas(old) != _py_pragmas(new):
        return f"tool pragmas changed: {_py_pragmas(old) - _py_pragmas(new)} -> {_py_pragmas(new) - _py_pragmas(old)}"
    return None


def _js(src: str) -> dict:
    res = subprocess.run(
        ["node", "-e", JS_TOKENS, str(ROOT)], input=src, capture_output=True, text=True, check=True
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
    return found


def js_differs(old: str, new: str) -> str | None:
    before, after = _js(old), _js(new)
    if before["tokens"] != after["tokens"]:
        return "code (token stream without comments) changed"
    if _js_load_bearing(before["comments"]) != _js_load_bearing(after["comments"]):
        return "ESLint/TypeScript directives or JSDoc type tags changed"
    return None


def _git(*args: str) -> str:
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True, check=True).stdout


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--base", default="origin/main")
    base = _git("merge-base", parser.parse_args().base, "HEAD").strip()
    changed = [
        line
        for line in _git("diff", "--name-only", "--diff-filter=M", base, "--").splitlines()
        if line.endswith((".py", ".js"))
    ]
    failures = []
    for rel in changed:
        old = _git("show", f"{base}:{rel}")
        new = (ROOT / rel).read_text(encoding="utf-8")
        reason = js_differs(old, new) if rel.endswith(".js") else python_differs(old, new)
        if reason:
            failures.append(f"{rel}: {reason}")
    added_or_removed = [
        line
        for line in _git("diff", "--name-only", "--diff-filter=ADR", base, "--").splitlines()
        if line.endswith((".py", ".js"))
    ]
    for rel in changed:
        print(f"ok   {rel}" if not any(f.startswith(rel + ":") for f in failures) else f"FAIL {rel}")
    print(f"{len(changed) - len(failures)}/{len(changed)} modified .py/.js files change only comments and docstrings")
    if added_or_removed:
        print("Not compared (added, deleted or renamed): " + ", ".join(added_or_removed))
    for failure in failures:
        print("  " + failure, file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
