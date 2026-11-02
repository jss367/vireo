"""Check cleanup diffs for syntax and tool-comment changes.

Python ASTs (including runtime docstrings), JavaScript token streams and
parsed syntax must match. Regex literals retain their patterns and flags;
parsed syntax preserves ASI behavior. Tool comments must retain content and
attachment, including suppression pragmas, JSDoc imports, source encodings
and shebangs. Docstring changes require separate semantic review because
decorators and external callers may read them at runtime. This is a scoped
source comparison that supplements semantic review; it does not prove full
runtime equivalence or check filesystem metadata and module import effects.

    python scripts/check_comment_only_diff.py [--base origin/main]

Compare the merge base with --base against the working tree. A/M/D/R source
changes are compared against empty counterparts or across same-language
rename paths. Untracked files, cross-language renames and unsupported source
change kinds fail closed. Supported suffixes are .py, .js, .mjs and .cjs:
.mjs uses ES-module parsing, .cjs uses CommonJS parsing, and .js tries script
then module. Invalid or unsupported syntax fails closed. Other file types
are outside this comparison. JavaScript needs npm ci (acorn) in the repo root.
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
JS_SUFFIXES = {".js", ".mjs", ".cjs"}
SOURCE_SUFFIXES = {".py", *JS_SUFFIXES}

PY_PRAGMA = re.compile(
    r"#\s*(ruff:\s*noqa\b[^#]*|noqa\b[^#]*|type:\s*\S[^#]*|pyright:[^#]*|pragma\b[^#]*|fmt:\s*\w+|broad:[^#]*)",
    re.I,
)
PY_CODING = re.compile(r"coding[=:]\s*([-\w.]+)")
JS_DIRECTIVE = re.compile(r"^\s*(eslint[\w-]*\b.*|@ts-[\w-]+\b.*|global\s.*|globals\s.*|exported\s.*)$", re.S)
JSDOC_TAG = re.compile(r"@(\w+)\s*(\{[^\n]*\})?\s*(\[?[\w$.]*)")
# Import module sources are type-bearing, including spaces inside strings.
JSDOC_IMPORT = re.compile(r"@import\b[^@]*")

JS_TOKENS = r"""
const acorn = require(process.argv[1] + '/node_modules/acorn');
const fs = require('fs');
const src = fs.readFileSync(0, 'utf8');
const comments = [], tokens = [];
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
  comments.length = 0; tokens.length = 0;
  const tree = acorn.parse(src, {ecmaVersion: 'latest', sourceType,
    locations: true, onComment: comments, onToken: tokens});
  const clean = (key, value) => {
    if (['start', 'end', 'loc'].includes(key)) return undefined;
    if (typeof value === 'bigint') return String(value);
    return value;
  };
  const syntax = JSON.parse(JSON.stringify(tree, clean));
  const code = tokens.filter(t => t.type.label !== 'eof');
  const attached = comments.map(c => {
    const next = code.findIndex(t => t.start >= c.end);
    const prev = code.findLastIndex(t => t.end <= c.start);
    return {text: c.value, attachment: [prev, next,
      prev >= 0 ? c.loc.start.line - code[prev].loc.end.line : null,
      next >= 0 ? code[next].loc.start.line - c.loc.end.line : null]};
  });
  return {tokens: code.map(t => [t.type.label, asString(t.value)]), syntax,
    comments: attached};
};
let result;
const requested = process.argv[2];
if (requested === 'auto') {
  try { result = parse('script'); } catch (e) { result = parse('module'); }
} else { result = parse(requested); }
process.stdout.write(JSON.stringify(result));
"""


def _py_pragmas(src: str) -> Counter:
    found: Counter = Counter()
    tokens = list(tokenize.generate_tokens(io.StringIO(src).readline))
    # Ordinals survive prose edits; adjacent line gaps preserve tool attachment.
    # This deliberately fails closed on ambiguous movement of a tool directive.
    code = [t for t in tokens if t.type not in {
        tokenize.COMMENT, tokenize.NL, tokenize.NEWLINE, tokenize.INDENT,
        tokenize.DEDENT, tokenize.ENDMARKER,
    }]
    for tok in tokens:
        if tok.type == tokenize.COMMENT:
            prev = [i for i, t in enumerate(code) if t.end <= tok.start]
            following = [i for i, t in enumerate(code) if t.start >= tok.end]
            before, after = (prev[-1] if prev else -1), (following[0] if following else -1)
            attachment = (before, after,
                          tok.start[0] - code[before].end[0] if before >= 0 else None,
                          code[after].start[0] - tok.end[0] if after >= 0 else None)
            for match in PY_PRAGMA.finditer(tok.string):
                found[(match.group(1).strip(), attachment)] += 1
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
    if ast.dump(old_tree) != ast.dump(new_tree):
        return "code (AST including runtime docstrings) changed"
    if _py_pragmas(old) != _py_pragmas(new):
        return f"tool pragmas changed: {_py_pragmas(old) - _py_pragmas(new)} -> {_py_pragmas(new) - _py_pragmas(old)}"
    return None


def _js(src: str, source_type: str | None = None) -> dict:
    res = subprocess.run(
        ["node", "-e", JS_TOKENS, str(ROOT), source_type or "auto"], input=src, capture_output=True, text=True, check=True
    )
    return json.loads(res.stdout)


def _js_load_bearing(comments: list[dict]) -> Counter:
    found: Counter = Counter()
    for comment in comments:
        text = comment["text"]
        attachment = tuple(comment["attachment"])
        if JS_DIRECTIVE.match(text):
            found[("directive " + text.strip(), attachment)] += 1
        if text.startswith("*"):
            for tag in JSDOC_TAG.findall(text):
                found[("jsdoc " + " ".join(part for part in tag if part), attachment)] += 1
            for tag in JSDOC_IMPORT.findall(text):
                found[("jsdoc import " + tag.strip(), attachment)] += 1
    return found


def _js_shebang(src: str) -> str:
    return src.splitlines()[0] if src.startswith("#!") else ""


def js_differs(old: str, new: str, source_type: str | None = None) -> str | None:
    if _js_shebang(old) != _js_shebang(new):
        return f"shebang changed: {_js_shebang(old)!r} -> {_js_shebang(new)!r}"
    before, after = _js(old, source_type), _js(new, source_type)
    if (before["tokens"] != after["tokens"] or before["syntax"] != after["syntax"]):
        return "code (parsed JavaScript syntax) changed"
    if _js_load_bearing(before["comments"]) != _js_load_bearing(after["comments"]):
        return "ESLint/TypeScript directives or JSDoc type tags changed"
    return None


def _git(*args: str) -> str:
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True, check=True).stdout


def _source_changes(base: str) -> list[tuple[str, str, str, str]]:
    """``(label, old_path, new_path, suffix)`` for every A/M/D/R supported source file.

    Unsupported or untracked entries carry a non-source suffix so
    ``main()`` can fail on them; the labels for added, deleted and renamed
    files make the change kind readable in the output.
    """
    out: list[tuple[str, str, str, str]] = []
    fields = iter(_git("diff", "--name-status", "-z", base, "--").split("\0"))
    for change in fields:
        if not change:
            continue
        status = change[0]
        old_path = new_path = next(fields)
        if status in {"R", "C"}:
            new_path = next(fields)
        if status == "A":
            old_path = ""
            label = f"{new_path} (added)"
        elif status == "D":
            new_path = ""
            label = f"{old_path} (deleted)"
        elif status in {"R", "C"}:
            label = f"{old_path} -> {new_path}"
        else:
            label = old_path
        old_suffix, new_suffix = Path(old_path).suffix, Path(new_path).suffix
        if not ({old_suffix, new_suffix} & SOURCE_SUFFIXES):
            continue
        if status not in {"A", "M", "D", "R"}:
            out.append((label, "", "", status))
        elif status == "R" and old_suffix != new_suffix:
            out.append((label, "", "", f"rename across source types {old_suffix} -> {new_suffix}"))
        else:
            out.append((label, old_path, new_path, new_suffix or old_suffix))
    for path in _git("ls-files", "--others", "--exclude-standard", "-z").split("\0"):
        if Path(path).suffix in SOURCE_SUFFIXES:
            out.append((path + " (untracked)", "", "", "untracked"))
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--base", default="origin/main")
    base = _git("merge-base", parser.parse_args().base, "HEAD").strip()
    entries = _source_changes(base)
    failures: list[str] = []
    labels: list[str] = []
    failed_labels: set[str] = set()
    for label, old_path, new_path, suffix in entries:
        labels.append(label)
        if suffix not in SOURCE_SUFFIXES:
            failures.append(f"{label}: unsupported change kind {suffix!r}")
            failed_labels.add(label)
            continue
        old = _git("show", f"{base}:{old_path}") if old_path else ""
        new = (ROOT / new_path).read_text(encoding="utf-8") if new_path else ""
        if suffix in JS_SUFFIXES:
            source_type = {".mjs": "module", ".cjs": "commonjs"}.get(suffix)
            reason = js_differs(old, new, source_type)
        else:
            reason = python_differs(old, new)
        if reason:
            failures.append(f"{label}: {reason}")
            failed_labels.add(label)
    for label in labels:
        print(f"FAIL {label}" if label in failed_labels else f"ok   {label}")
    print(f"{len(labels) - len(failures)}/{len(labels)} supported Python/JavaScript source changes pass the scoped comparison")
    for failure in failures:
        print("  " + failure, file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
