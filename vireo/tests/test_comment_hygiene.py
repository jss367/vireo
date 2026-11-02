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
import os
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
_JS_BINDING_KW = {"const", "let", "var"}
_JS_PARAM_KW = {"function", "catch"}
_JS_IDENT = re.compile(r"[A-Za-z_$\u0080-￿][\w$\u0080-￿]*")
_JS_NUMBER = re.compile(r"0[xXoObB][\da-fA-F_]+n?|(?:\d[\d_]*\.?[\d_]*|\.\d[\d_]*)(?:[eE][+-]?\d+)?n?")


def js_comment_texts(src: str) -> list[str]:
    """Comment bodies in ``src``, in order, as acorn's ``onComment`` reports them.

    A small lexer that skips strings, template literals (with nested
    substitutions) and regular-expression literals, so ``//`` inside a URL
    string is not a comment. Identifiers introduced by a binding form are
    tracked so a contextual keyword rebound as a name (``const of = 8``,
    ``function f(of)``, ``const g = (of) => ...``, ``const h = of => ...``,
    ``const {of} = x``, ``catch (of)``) stays an identifier for the rest of
    the file and its following ``/`` is division, not a regex. A member-access
    ``.`` (``obj.of``, ``Observable.of / 2``) is tracked separately so a
    property name reusing a contextual keyword does not open a regex either,
    even when no binding form made it an identifier earlier. ``of`` only
    opens a regex when it appears directly inside a ``for (...)`` paren list
    (where ``for (binding of /pat/)`` is valid syntax); at any other position
    — statement start (``of / 2``), an imported name (``import {of}``), a
    hoisted function declaration (``function of() {}``) — ``of`` is treated
    as a plain identifier and its following ``/`` is division.
    ``test_js_lexer_matches_acorn`` checks it against acorn on every
    first-party script when acorn is installed.
    """
    comments: list[str] = []
    i, n = 0, len(src)
    regex_ok = True
    braces: list[str] = []
    bound: set[str] = set()
    expect_binding = False
    destr_depth = 0
    pending_params = False
    param_depth = 0
    paren_candidates: list[list[str]] = []
    after_dot = False
    for_paren_pending = False
    paren_is_for: list[bool] = []

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
        # Whitespace and comments are transparent: a `.` separated from its
        # property name by spaces or a block comment (``obj. of``,
        # ``obj./* note */of``) still binds ``of`` to the member-access
        # context.
        if c.isspace() or c == "﻿":
            i += 1
            continue
        if src.startswith("//", i):
            end = src.find("\n", i)
            end = n if end < 0 else end
            comments.append(src[i + 2 : end].rstrip("\r"))
            i = end
            continue
        if src.startswith("/*", i):
            end = src.find("*/", i + 2)
            end = n if end < 0 else end
            comments.append(src[i + 2 : end])
            i = end + 2
            continue
        # Any other branch consumes a significant token, so after_dot is
        # cleared below; the `.` branch sets it back to True.
        was_after_dot = after_dot
        after_dot = False
        if c in "'\"":
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
                # An optional ``catch`` binding (``catch { ... }``) opens its
                # body without a parameter list, so a ``pending_params`` left
                # over from ``catch`` must not survive into the body — the
                # next ordinary call's ``(`` would otherwise be lexed as a
                # parameter list and its identifiers would land in ``bound``.
                pending_params = False
                braces.append("{")
                if expect_binding or destr_depth > 0:
                    destr_depth += 1
            else:
                if braces:
                    braces.pop()
                if destr_depth > 0:
                    destr_depth -= 1
            # After a block's ``}`` a ``/`` starts a regex; that is far more
            # common in these scripts than dividing an object literal.
            i, regex_ok = i + 1, True
        elif c == "[":
            if expect_binding or destr_depth > 0:
                destr_depth += 1
            i, regex_ok = i + 1, True
        elif c == "]":
            if destr_depth > 0:
                destr_depth -= 1
            i, regex_ok = i + 1, False
        elif c == "(":
            paren_candidates.append([])
            paren_is_for.append(for_paren_pending)
            for_paren_pending = False
            if pending_params or param_depth > 0:
                param_depth += 1
                pending_params = False
            i, regex_ok = i + 1, True
        elif c == ")":
            cands = paren_candidates.pop() if paren_candidates else []
            if paren_is_for:
                paren_is_for.pop()
            if param_depth > 0:
                param_depth -= 1
            j = i + 1
            while j < n and src[j] in " \t\r\n":
                j += 1
            if src.startswith("=>", j):
                bound.update(cands)
            i, regex_ok = i + 1, False
        elif c == "=" and expect_binding and destr_depth == 0 and src[i + 1 : i + 2] not in ("=", ">"):
            expect_binding = False
            i, regex_ok = i + 1, True
        elif c == ";":
            expect_binding = False
            destr_depth = 0
            pending_params = False
            for_paren_pending = False
            i, regex_ok = i + 1, True
        elif match := _JS_IDENT.match(src, i):
            word = match.group()
            i = match.end()
            if was_after_dot:
                # The identifier is a property name, so its following ``/`` is
                # division and keyword status (``of``, ``return``, ...) does
                # not apply.
                regex_ok = False
            else:
                is_binding = expect_binding or destr_depth > 0 or param_depth > 0
                bare_binding = is_binding and destr_depth == 0 and param_depth == 0
                if is_binding:
                    bound.add(word)
                    regex_ok = False
                elif word in bound:
                    regex_ok = False
                elif word == "of":
                    # ``of`` opens a regex only inside a ``for (...)`` paren
                    # list (``for (binding of /pat/)``). At statement start,
                    # inside an import spec, or after a bare function name,
                    # ``of`` is a plain identifier and the following ``/`` is
                    # division.
                    regex_ok = bool(paren_is_for) and paren_is_for[-1]
                else:
                    regex_ok = word in _JS_REGEX_AFTER_WORD
                if word in _JS_BINDING_KW:
                    expect_binding = True
                elif word in _JS_PARAM_KW:
                    pending_params = True
                if (
                    paren_candidates
                    and not is_binding
                    and word not in _JS_BINDING_KW
                    and word not in _JS_PARAM_KW
                ):
                    paren_candidates[-1].append(word)
                j = i
                while j < n:
                    if src[j] in " \t\r\n":
                        j += 1
                    elif src.startswith("//", j):
                        end = src.find("\n", j)
                        j = n if end < 0 else end
                    elif src.startswith("/*", j):
                        end = src.find("*/", j + 2)
                        j = n if end < 0 else end + 2
                    else:
                        break
                if word == "for":
                    # Only a loop header's `(` opens a for-paren: a property
                    # name reusing the reserved word (``{for: (of / 2) //
                    # note}``) is followed by `:`, and letting its ``(`` claim
                    # a for-paren would make the nested ``of`` open a regex
                    # whose closing `/` is the start of the trailing line
                    # comment, swallowing the comment the ratchet must read.
                    # ``for await`` is the async-iteration header: the
                    # ``await`` keyword sits between ``for`` and the paren
                    # list, so step past it before deciding so the inner
                    # ``of`` can still open its regex iterable.
                    k = j
                    m_await = _JS_IDENT.match(src, k)
                    if m_await and m_await.group() == "await":
                        k = m_await.end()
                        while k < n:
                            if src[k] in " \t\r\n":
                                k += 1
                            elif src.startswith("//", k):
                                end = src.find("\n", k)
                                k = n if end < 0 else end
                            elif src.startswith("/*", k):
                                end = src.find("*/", k + 2)
                                k = n if end < 0 else end + 2
                            else:
                                break
                    for_paren_pending = k < n and src[k] == "("
                if src.startswith("=>", j):
                    bound.add(word)
                    regex_ok = False
                elif bare_binding and word not in _JS_BINDING_KW:
                    expect_binding = src.startswith(",", j)
        elif (c.isdigit() or (c == "." and src[i + 1 : i + 2].isdigit())) and (match := _JS_NUMBER.match(src, i)):
            i, regex_ok = match.end(), False
        elif src.startswith("...", i):
            i, regex_ok = i + 3, True
        elif c == "." and not src.startswith("..", i + 1):
            # Member access ``.`` — the next identifier is a property name,
            # not a keyword. ``.N`` was handled by the number branch above.
            after_dot = True
            i, regex_ok = i + 1, False
        elif c == "#" and (match := _JS_IDENT.match(src, i + 1)):
            # Private name ``#foo``: a class field declaration or a private
            # member access after ``.`` / ``?.``. Either way the token is
            # identifier-shaped, so the following ``/`` is division and
            # keyword status does not apply.
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


def test_js_lexer_contextual_keyword_rebound_as_identifier():
    """A contextual keyword like ``of`` rebound as an identifier is division, not regex."""
    assert js_comment_texts("const of = 8; of / 2; // Codex\n") == [" Codex"]
    assert js_comment_texts("for (const x of /foo/) { /* keep */ }\n") == [" keep "]


def test_js_lexer_binding_forms_other_than_declarations():
    """Function parameters, arrow parameters, destructuring and ``catch`` bind too."""
    cases = [
        "function f(of) { return of / 2; // Codex\n}\n",
        "const g = (of) => of / 2; // Codex\n",
        "const h = of => of / 2; // Codex\n",
        "const i = of /* note */ => of / 2; // Codex\n",
        "const {of} = x; of / 2; // Codex\n",
        "const [a, of] = arr; of / 2; // Codex\n",
        "try {} catch (of) { of / 2; /* Codex */ }\n",
    ]
    for src in cases:
        matches = js_comment_texts(src)
        assert any("Codex" in text for text in matches), src


def test_js_lexer_optional_catch_binding_does_not_bind_call_args():
    """An optional ``catch`` binding (``catch { ... }``) has no parameter list,
    so an identifier inside its body — ``return`` in ``run(() => { return
    /'/.toString(); })`` — must not be bound as a parameter. If it were, the
    following ``/`` would lex as division, the lone ``'`` would open a string
    literal that runs to end of file, and the trailing ``// Codex`` would be
    swallowed."""
    cases = [
        "try {} catch { run(() => { return /'/.toString(); // Codex\n }) }\n",
        "try { x } catch { run(async () => { await y; return /'/.test(s); // Codex\n}) }\n",
    ]
    for src in cases:
        assert any("Codex" in text for text in js_comment_texts(src)), src


def test_js_lexer_for_of_does_not_bind_loop_body():
    """``for (const x of xs)`` must bind only ``x``: ``of``, ``xs`` and the body
    keep their roles, so a ``return /re/`` in the body still lexes as a regex
    and a following line comment is detected."""
    src = (
        "for (const x of xs) { return /'/.test(x); }\n"
        "function g(s) { return /\"/.test(s); // Codex\n}\n"
    )
    assert any("Codex" in text for text in js_comment_texts(src))


def test_js_lexer_member_access_of_is_division_not_regex():
    """A property name reusing a contextual keyword (``obj.of``,
    ``Observable.of``) must not open a regex on the following ``/``, so a
    trailing ``// Codex`` or ``/* note */`` after a division is still read."""
    cases = [
        "obj.of / 2; // Codex\n",
        "obj?.of / 2; // Codex\n",
        "obj . of / 2; // Codex\n",
        "obj./* note */of / 2; // Codex\n",
        "Observable.of.length / 2; // Codex\n",
    ]
    for src in cases:
        assert any("Codex" in text for text in js_comment_texts(src)), src


def test_js_lexer_unbound_of_outside_for_is_identifier():
    """``of`` opens a regex only inside a ``for (...)`` paren list. At
    statement start, after an unmodeled binding form (``import {of}``,
    ``function of() {}``) or as a bare global, ``of / 2`` is division and
    the trailing ``// Codex`` is detected."""
    cases = [
        "of / 2; // Codex\n",
        "import {of} from 'x'; of / 2; // Codex\n",
        "function of() {} of / 2; // Codex\n",
        "{ of / 2; // Codex\n}\n",
        "for (const x of /foo/) { of / 2; // Codex\n }\n",
    ]
    for src in cases:
        assert any("Codex" in text for text in js_comment_texts(src)), src


def test_js_lexer_for_of_still_opens_regex_on_rhs():
    """A regex literal is the valid iterable of a ``for ... of`` loop, so
    inside a ``for (...)`` paren list ``of`` still opens the regex and the
    body's comments are not swallowed."""
    src = "for (const x of /foo/) { /* keep */ }\n"
    assert js_comment_texts(src) == [" keep "]


def test_js_lexer_private_member_name_is_identifier_not_regex():
    """``#name`` is an identifier-shaped token: a class field (``#of = 8``) or
    a private member access after ``.``/``?.`` (``this.#of``, ``obj?.#of``).
    Either way the following ``/`` is division, so a trailing ``// Codex`` or
    ``/* note */`` is still read."""
    cases = [
        "class C { #of = 8; f() { return this.#of / 2; // Codex\n } }\n",
        "this.#of / 2; // Codex\n",
        "obj.#of / 2; // Codex\n",
        "obj?.#of / 2; // Codex\n",
        "obj. #of / 2; // Codex\n",
        "obj./* note */#of / 2; // Codex\n",
    ]
    for src in cases:
        assert any("Codex" in text for text in js_comment_texts(src)), src


def test_js_lexer_for_as_property_name_does_not_open_for_paren():
    """``for`` is a reserved word that may appear only as a property name
    (``{for: value}``). Its following ``(`` is a grouped expression, not a
    loop header, so ``of`` inside must not open a regex that would eat the
    trailing line comment."""
    cases = [
        "const o = {for: (of / 2) // Codex\n};\n",
        "const p = {for: 1, bar: (of / 2) /* Codex */};\n",
    ]
    for src in cases:
        assert any("Codex" in text for text in js_comment_texts(src)), src


def test_js_lexer_for_await_opens_loop_header():
    """``for await (binding of /pat/)`` is the async-iteration header: the
    optional ``await`` sits between ``for`` and the paren list, so its ``(``
    still opens a for-paren. Otherwise the inner ``of`` would be a plain
    identifier, the following ``/`` would open a regex that swallows the
    body, and code identifiers after it would leak into the comment stream."""
    cases = [
        "async function f(){ for await (const x of /\\//) { const Codex = 1; } }\n",
        "async function g(){ for  await  (const x of /re/) { /* keep */ } }\n",
        "async function h(){ for /* c */ await (const x of /re/) { /* keep */ } }\n",
    ]
    assert js_comment_texts(cases[0]) == []
    assert js_comment_texts(cases[1]) == [" keep "]
    assert js_comment_texts(cases[2]) == [" c ", " keep "]


def _assert_js_corpus_matches_acorn(files: list[Path], repo: Path = VIREO.parent):
    """Fail on any corpus file outside the bounded lexer's supported syntax."""
    node = shutil.which("node")
    if not node or not (repo / "node_modules" / "acorn").is_dir():
        message = "node and acorn (npm ci) are needed for the cross-check"
        if os.environ.get("VIREO_REQUIRE_ACORN") == "1":
            pytest.fail(message)
        pytest.skip(message)
    script = (
        "const acorn = require(process.argv[1] + '/node_modules/acorn');"
        "const fs = require('fs'); const out = {};"
        "for (const f of process.argv.slice(2)) {"
        "  const src = fs.readFileSync(f, 'utf8'); const c = [];"
        "  const o = {ecmaVersion: 'latest', onComment: (b, t) => c.push(t)};"
        "  try { acorn.parse(src, {...o, sourceType: 'script'}); }"
        "  catch (e) { c.length = 0; acorn.parse(src, {...o, sourceType: 'module'}); }"
        "  out[f] = c; }"
        "process.stdout.write(JSON.stringify(out));"
    )
    result = subprocess.run(
        [node, "-e", script, str(repo), *map(str, files)],
        capture_output=True, encoding="utf-8", check=True,
    )
    expected = json.loads(result.stdout)
    mismatched = [str(f) for f in files if js_comment_texts(f.read_text(encoding="utf-8")) != expected[str(f)]]
    assert not mismatched, mismatched


def test_js_lexer_matches_acorn():
    """Frontend CI requires this comparison on every first-party script."""
    files = [path for path in production_files() if path.suffix == ".js"]
    assert files, "first-party JavaScript corpus must not be empty"
    _assert_js_corpus_matches_acorn(files)


@pytest.mark.parametrize("source", [
    "for (; of / 2; ) { // actual comment\n }",
    "const o = {for(x = of / 2 /* actual comment */) {}};",
    "const of = 1; for (const x of /[//]inert/) { /* actual comment */ }",
])
def test_js_corpus_rejects_unsupported_contexts(tmp_path, source):
    """Unsupported syntax must fail the oracle, rather than certify its counts."""
    path = tmp_path / "counterexample.js"
    path.write_text(source, encoding="utf-8")
    with pytest.raises(AssertionError, match="counterexample.js"):
        _assert_js_corpus_matches_acorn([path])


def test_js_corpus_accepts_comment_cleanup(tmp_path):
    path = tmp_path / "cleanup.js"
    for wording in ("old wording", "clear wording"):
        path.write_text(f"const n = 1; // {wording}\n", encoding="utf-8")
        _assert_js_corpus_matches_acorn([path])


@pytest.mark.parametrize("missing", ["node", "acorn"])
def test_js_corpus_required_prerequisites_cannot_skip(tmp_path, monkeypatch, missing):
    monkeypatch.setenv("VIREO_REQUIRE_ACORN", "1")
    if missing == "node":
        monkeypatch.setattr(shutil, "which", lambda _: None)
        repo = VIREO.parent
    else:
        monkeypatch.setattr(shutil, "which", lambda _: "/unused/node")
        repo = tmp_path
    with pytest.raises(pytest.fail.Exception, match="node and acorn"):
        _assert_js_corpus_matches_acorn([], repo=repo)


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
