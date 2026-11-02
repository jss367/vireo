"""Check that a diff changes only comments and docstrings.

For every Python file the diff touches, the AST with docstrings removed must
dump identically on both sides and the shebang and ``# coding:`` declaration
must match (both pick the interpreter or decoder that runs the file, and the
AST drops them). For every JavaScript file, acorn's token stream (comments
excluded) must be identical, with regular-expression tokens compared by their
pattern and flags rather than collapsed to ``[object Object]``, and the
hashbang line is compared separately because acorn drops it. Comments that
tools read are compared too, because the AST and the token stream both drop
them: Python ``noqa`` (``ruff: noqa`` too, and ``NOQA`` case-insensitively)
/ ``type:`` / ``pyright:`` / ``pragma`` / ``fmt:`` / ``broad:`` pragmas,
ESLint and ``@ts-`` directives (``@ts-expect-error`` included), and JSDoc
type tags with ``@import`` module sources captured separately so changing
the source is detected. Added, deleted and renamed source files are compared
the same way, against an empty counterpart (or across both paths for a
rename), so a diff is comment-only only when every A/M/D/R source file is.

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

PY_PRAGMA = re.compile(
    r"#\s*(ruff:\s*noqa\b[^#]*|noqa\b[^#]*|type:\s*\S[^#]*|pyright:[^#]*|pragma\b[^#]*|fmt:\s*\w+|broad:[^#]*)",
    re.I,
)
PY_CODING = re.compile(r"coding[=:]\s*([-\w.]+)")
JS_DIRECTIVE = re.compile(r"^\s*(eslint[\w-]*\b.*|@ts-[\w-]+\b.*|global\s.*|globals\s.*)$", re.S)
JSDOC_TAG = re.compile(r"@(\w+)\s*(\{[^\n]*\})?\s*(\[?[\w$.]*)")
JSDOC_IMPORT_SRC = re.compile(r"""@import\b[^\n]*?(['"][^'"\n]+['"])""")

JS_TOKENS = r"""
const acorn = require(process.argv[1] + '/node_modules/acorn');
const fs = require('fs');
const src = fs.readFileSync(0, 'utf8');
const comments = [];
const asString = (v) => {
  if (v === undefined) return null;
  if (v !== null && typeof v === 'object') {
    const flags = typeof v.flags === 'string' ? v.flags : '';
    const pattern = typeof v.pattern === 'string' ? v.pattern : JSON.stringify(v);
    return 'regexp /' + pattern + '/' + flags;
  }
  return String(v);
};
const parse = (sourceType) => {
  comments.length = 0;
  const opts = {ecmaVersion: 'latest', sourceType, onComment: (block, text) => comments.push(text)};
  return [...acorn.tokenizer(src, opts)].map((t) => [t.type.label, asString(t.value)]);
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
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True, check=True).stdout


def _source_changes(base: str) -> list[tuple[str, str, str, str]]:
    """``(label, old_path, new_path, suffix)`` for every A/M/D/R .py/.js file.

    An entry whose status is not A/M/D/R is returned with suffix ``""`` so
    ``main()`` can fail on it; the labels for added, deleted and renamed
    files make the change kind readable in the output.
    """
    out: list[tuple[str, str, str, str]] = []
    for line in _git("diff", "--name-status", base, "--").splitlines():
        parts = line.split("\t")
        status = parts[0][0]
        if status == "R":
            old_path, new_path = parts[1], parts[2]
            label = f"{old_path} -> {new_path}"
        elif status == "D":
            old_path, new_path = parts[1], ""
            label = f"{old_path} (deleted)"
        elif status == "A":
            old_path, new_path = "", parts[1]
            label = f"{new_path} (added)"
        elif status == "M":
            old_path = new_path = parts[1]
            label = parts[1]
        else:
            out.append((parts[-1], "", "", status))
            continue
        suffix = next((s for s in (".py", ".js") if (new_path or old_path).endswith(s)), "")
        if suffix:
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
        if suffix not in {".py", ".js"}:
            failures.append(f"{label}: unsupported change kind {suffix!r}")
            continue
        old = _git("show", f"{base}:{old_path}") if old_path else ""
        new = (ROOT / new_path).read_text(encoding="utf-8") if new_path else ""
        reason = js_differs(old, new) if suffix == ".js" else python_differs(old, new)
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
