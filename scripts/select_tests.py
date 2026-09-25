#!/usr/bin/env python3
"""Pick the unit tests a change can affect.

The full unit suite is ~7.5k tests and takes 11-23 minutes per OS leg in CI
(3-4 minutes locally on all cores). Most PRs touch a handful of functions,
and only a small fraction of tests ever execute those functions. This script
turns a *test-impact map* (per-test line coverage recorded on ``main``) plus
``git diff`` into the list of tests that actually exercised the changed code.

Selection is conservative and precise at the same time:

* A change inside a Python function selects every test that executed any
  line of that function (innermost enclosing ``def``) on ``main``. Every
  Python source-file change additionally selects tests that reference
  the source file's basename — declaration-inspecting tests (route
  contract snapshots, ``ast.parse`` of the source, ``app.url_map``
  audits, route-decorator AST parsers) never execute the body but must
  still catch structural regressions from any edit — and tests that
  scan the source tree with ``glob("*.py")``/``rglob("*.py")``.
* A change outside any function (imports, constants, class bodies, module
  code) falls back to the full suite. Import-time values can flow through
  production modules without executing a line in the defining module under
  a test context, so coverage cannot safely narrow them. A pure insertion
  between functions (or at EOF beside a function) is treated the same way.
* A changed or added unit-test file runs in full.
* Non-Python files (templates, static assets, data, shell scripts, docs)
  select every unit-test file that mentions their basename, plus the tests
  that executed any Python line referencing the file. A template is chased
  through the templates that ``include``/``extend`` it, so a change to
  ``_navbar.html`` maps to every page that renders it; a static asset is
  chased through the templates that load it.
* Non-test files inside a tests directory (contract snapshots, fixtures)
  run the test files that mention them; if nothing does, the full suite.
* Anything that changes the harness itself (``conftest.py``, ``pyproject``,
  the test workflows, ``vireo/data``, this script) falls back to the full
  suite.
* Missing or unreadable map, or an unreachable map commit, falls back to
  the full suite. Selection can only ever *narrow*; a bad input widens.

The map is a standard ``coverage.py`` data file written with
``--cov-context=test`` (see ``.github/workflows/test-main.yml``). ``main``
builds it after every merge; PRs restore the most recent one from the
Actions cache. Locally, ``fetch-map`` downloads the newest artifact with
``gh``.

Usage::

    # In CI (see test.yml): prints the mode, writes the selection file.
    mode=$(python scripts/select_tests.py --output .test-impact/selected.txt)

    # Locally: run only what your branch can affect.
    python scripts/select_tests.py fetch-map
    python scripts/select_tests.py --run -- -n auto -q

    # After a full coverage run with contexts: package the map.
    python scripts/select_tests.py build-map --coverage-file .coverage

The selection file feeds pytest via ``--selected-tests`` (implemented by
``scripts/selected_tests_plugin.py``, registered in the root ``conftest.py``).
"""

from __future__ import annotations

import argparse
import ast
import datetime as _dt
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

MAP_DIR = ".test-impact"
MAP_DB = "coverage.sqlite"
MAP_META = "meta.json"
SELECTION_FILE = "selected.txt"

# The Actions workflow that produces the map (name: in the YAML) and the
# artifact it uploads. ``fetch-map`` looks these up with ``gh``.
MAP_WORKFLOW = "Full tests"
MAP_ARTIFACT = "test-impact-map"

UNIT_TEST_DIRS = ("tests", "vireo/tests")
E2E_PREFIX = "tests/e2e/"
SOURCE_PREFIXES = ("vireo/", "scripts/")
TEMPLATE_PREFIX = "vireo/templates/"
STATIC_PREFIX = "vireo/static/"

# Any change here can alter how *every* test runs, so no subset is safe.
FULL_SUITE_PATHS = frozenset(
    {
        "pyproject.toml",
        "uv.lock",
        "conftest.py",
        "tests/conftest.py",
        "vireo/tests/conftest.py",
        "scripts/select_tests.py",
        "scripts/selected_tests_plugin.py",
        ".github/workflows/test.yml",
        ".github/workflows/test-main.yml",
        ".github/workflows/main-health.yml",
    }
)
FULL_SUITE_PREFIXES = (".github/actions/", "vireo/data/")

_TEST_FILE_RE = re.compile(r"^(tests|vireo/tests)/test_[A-Za-z0-9_]+\.py$")
_HUNK_RE = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@")
_DIFF_HEADER_RE = re.compile(r"^diff --git a/(.*) b/(.*)$")


# --------------------------------------------------------------------------
# git helpers
# --------------------------------------------------------------------------


def _git(*args: str, cwd: Path = ROOT, check: bool = True) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if check and result.returncode != 0:
        raise RuntimeError(
            f"git {' '.join(args)} failed ({result.returncode}): {result.stderr.strip()}"
        )
    return result.stdout


def _commit_exists(sha: str, cwd: Path) -> bool:
    return (
        subprocess.run(
            ["git", "cat-file", "-e", f"{sha}^{{commit}}"],
            cwd=str(cwd),
            capture_output=True,
        ).returncode
        == 0
    )


def _ensure_commit(sha: str, cwd: Path) -> bool:
    """Make ``sha`` available locally, fetching it shallowly if needed."""
    if _commit_exists(sha, cwd):
        return True
    subprocess.run(
        ["git", "fetch", "--no-tags", "--depth=1", "origin", sha],
        cwd=str(cwd),
        capture_output=True,
    )
    return _commit_exists(sha, cwd)


def _show(sha: str, path: str, cwd: Path) -> str | None:
    result = subprocess.run(
        ["git", "show", f"{sha}:{path}"],
        cwd=str(cwd),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    return result.stdout if result.returncode == 0 else None


def _grep_files(
    needle: str, rev: str, pathspecs: list[str], cwd: Path,
    regex: bool = False, word: bool = False,
) -> list[str]:
    """Paths (repo-relative) under ``pathspecs`` at ``rev`` containing ``needle``.

    ``word=True`` restricts to whole-word matches (``git grep -w``).
    """
    cmd = ["git", "grep", "-l", "-E" if regex else "-F"]
    if word:
        cmd.append("-w")
    cmd += ["-e", needle, rev, "--", *pathspecs]
    result = subprocess.run(
        cmd,
        cwd=str(cwd),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    out = []
    for line in result.stdout.splitlines():
        # ``git grep <rev>`` prefixes every hit with ``<rev>:``.
        out.append(line.split(":", 1)[1] if ":" in line else line)
    return out


def _grep_lines(
    needle: str, rev: str, pathspecs: list[str], cwd: Path, word: bool = False,
) -> dict[str, set[int]]:
    """``{path: {lineno, ...}}`` for lines under ``pathspecs`` at ``rev`` containing ``needle``.

    ``word=True`` restricts to whole-word matches (``git grep -w``); use it
    when grepping for a Python identifier so ``alpha`` doesn't pull in
    ``test_alpha``.
    """
    cmd = ["git", "grep", "-n", "-F"]
    if word:
        cmd.append("-w")
    cmd += ["-e", needle, rev, "--", *pathspecs]
    result = subprocess.run(
        cmd,
        cwd=str(cwd),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    hits: dict[str, set[int]] = defaultdict(set)
    for line in result.stdout.splitlines():
        # ``<rev>:<path>:<lineno>:<text>``
        parts = line.split(":", 3)
        if len(parts) < 4:
            continue
        try:
            hits[parts[1]].add(int(parts[2]))
        except ValueError:
            continue
    return hits


# --------------------------------------------------------------------------
# diff parsing
# --------------------------------------------------------------------------


def changed_files(base: str, head: str, cwd: Path) -> list[tuple[str, str]]:
    """``[(status, path)]`` between ``base`` and ``head``.

    Rename detection is disabled on purpose: a rename becomes ``D old`` +
    ``A new``. The deleted side selects every test that executed the old
    file, the added side has no history and is covered by the basename rule
    and by the PR's own tests. Both are safe over-approximations.
    """
    out = []
    text = _git("diff", "--name-status", "--no-renames", base, head, cwd=cwd)
    for line in text.splitlines():
        if not line.strip():
            continue
        status, _, path = line.partition("\t")
        out.append((status[:1], path.strip()))
    return out


Hunk = tuple[str, tuple[int, ...], bool]
# ("mod", old lines, added_code) or ("ins", neighbour lines, added_code).
# ``added_code`` distinguishes a comment reword from replacing a comment with
# executable code; looking only at the base-side text would call both changes
# comment-only and could skip affected tests.


def old_side_hunks(base: str, head: str, cwd: Path) -> dict[str, list[Hunk]]:
    """Map each changed path to its hunks in *base-side* line numbers.

    Base-side numbers are what the impact map knows about. A modification or
    deletion (``-a,b``) names the old lines it replaced. A pure insertion
    (``-a,0``) has no old lines of its own, so it names the two lines that
    surround it and lets ``impacted_lines`` decide which enclosing function
    it belongs to. The final flag records whether the new side contains code,
    so replacing a comment with a statement is not mistaken for a comment-only
    edit.
    """
    text = _git("diff", "-U0", "--no-color", "--no-ext-diff", "--no-renames", base, head, cwd=cwd)
    hunks: dict[str, list[Hunk]] = defaultdict(list)
    current: str | None = None
    current_hunk: int | None = None
    for line in text.splitlines():
        header = _DIFF_HEADER_RE.match(line)
        if header:
            current = header.group(2)
            current_hunk = None
            continue
        hunk = _HUNK_RE.match(line)
        if hunk and current is not None:
            start = int(hunk.group(1))
            count = int(hunk.group(2)) if hunk.group(2) is not None else 1
            if count == 0:
                lines = tuple(n for n in (start, start + 1) if n >= 1)
                kind = "ins"
            else:
                lines = tuple(range(start, start + count))
                kind = "mod"
            hunks[current].append((kind, lines, False))
            current_hunk = len(hunks[current]) - 1
            continue
        if current is None or current_hunk is None or not line.startswith("+"):
            continue
        added = line[1:].strip()
        if added and not added.startswith("#"):
            kind, lines, _ = hunks[current][current_hunk]
            hunks[current][current_hunk] = (kind, lines, True)
    return hunks


# --------------------------------------------------------------------------
# impact map
# --------------------------------------------------------------------------


class ImpactMap:
    """Read-only view over a coverage.py data file recorded with test contexts."""

    def __init__(self, db_path: Path, root: str | None = None):
        from coverage import CoverageData

        self._data = CoverageData(basename=str(db_path))
        self._data.read()
        self._root = root
        self._by_rel: dict[str, str] = {}
        for measured in self._data.measured_files():
            self._by_rel[self._relativize(measured)] = measured
        self._cache: dict[str, dict[int, list[str]]] = {}

    def _relativize(self, measured: str) -> str:
        path = measured.replace("\\", "/")
        if self._root:
            root = self._root.replace("\\", "/").rstrip("/") + "/"
            if path.startswith(root):
                return path[len(root):]
        if os.path.isabs(path):
            marker = "/vireo/"
            idx = path.rfind(marker)
            # Prefer the *repository* ``vireo/`` (the earliest occurrence
            # after the checkout dir) — ``.../vireo/vireo/tests/x.py``.
            first = path.find(marker)
            if first != -1 and path.find(marker, first + 1) != -1:
                idx = first
            if idx != -1:
                return path[idx + 1:]
            for prefix in ("/tests/", "/scripts/"):
                idx = path.find(prefix)
                if idx != -1:
                    return path[idx + 1:]
        return path

    def has_file(self, rel: str) -> bool:
        return rel in self._by_rel

    def contexts_by_line(self, rel: str) -> dict[int, list[str]]:
        if rel not in self._cache:
            measured = self._by_rel.get(rel)
            self._cache[rel] = self._data.contexts_by_lineno(measured) if measured else {}
        return self._cache[rel]

    def tests_for_lines(self, rel: str, lines: set[int] | None) -> set[str]:
        """Node ids of tests that executed ``lines`` (or any line if ``None``)."""
        by_line = self.contexts_by_line(rel)
        picked: set[str] = set()
        for lineno, contexts in by_line.items():
            if lines is not None and lineno not in lines:
                continue
            for ctx in contexts:
                nodeid = ctx.rsplit("|", 1)[0] if "|" in ctx else ctx
                if nodeid:
                    picked.add(nodeid)
        return picked

    @property
    def context_count(self) -> int:
        """Number of distinct tests in the map (setup/run/teardown collapsed)."""
        return len({c.rsplit("|", 1)[0] for c in self._data.measured_contexts() if c})


Span = tuple[int, int, int]  # (first decorator/def line, last line, first body line)


def function_spans(source: str) -> list[Span]:
    """``(start, end, body_start)`` for every function, decorators included.

    Only the *body* lines are looked up in the map. A function's decorator
    and ``def`` lines execute when the enclosing scope runs — for every route
    closure in ``app.py`` that is ``create_app()``, i.e. every test that
    builds an app — so counting them would turn any route edit into "every
    test". A change to the header (decorator, signature) is attributed to
    the body's tests instead, which is exactly the set that calls it, plus
    any test that inspects the declaration by name — picked up by the
    unconditional basename-mention fallback in ``select()``.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []
    spans = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            start = min([d.lineno for d in node.decorator_list] + [node.lineno])
            end = node.end_lineno or node.lineno
            body_start = node.body[0].lineno if node.body else node.lineno
            spans.append((start, end, body_start))
    return spans


def innermost_span(line: int, spans: list[Span]) -> Span | None:
    containing = [s for s in spans if s[0] <= line <= s[1]]
    if not containing:
        return None
    return min(containing, key=lambda s: s[1] - s[0])


def _body_lines(span: Span) -> range:
    return range(span[2], span[1] + 1)


def _is_code(text_lines: list[str], line: int) -> bool:
    """True unless ``line`` is blank or a comment (which can't change behaviour)."""
    if not 1 <= line <= len(text_lines):
        return False
    stripped = text_lines[line - 1].strip()
    return bool(stripped) and not stripped.startswith("#")


def impacted_lines(hunks: list[Hunk], source: str) -> set[int] | None:
    """Widen a file's hunks to the lines of their innermost enclosing functions.

    Returns ``None`` when some changed code sits outside every function
    (imports, constants, class bodies, module-level registration) or a
    pure insertion is ambiguous about which function it belongs to, which
    the caller treats as "full suite". Blank and comment-only lines are
    ignored, so a diff that only touches comments selects nothing from
    the map.

    Declaration-inspecting tests (route contract snapshots, ``ast.parse``
    of the source, ``app.url_map`` audits) don't execute the changed
    function's body but do reference the source file by name. The caller
    picks them up unconditionally via a basename-mention fallback rather
    than gating on a "header touched" flag: body-only edits can equally
    trip a source-parsing contract, so limiting the fallback to
    signature/decorator changes would still miss the tests specifically
    built to catch structural regressions.

    A pure insertion is only attributed to a single function when both
    neighbouring old-side lines resolve to the same enclosing function.
    Otherwise the new lines may be a new module- or class-level statement
    (a constant, a decorator-driven registration, a new function beside
    an existing one) that every test importing the file would see, so the
    selection widens to the full suite. At EOF the second neighbour is past the
    end of the base source and is treated as "outside every function" for
    the same reason.
    """
    spans = function_spans(source)
    text_lines = source.splitlines()
    widened: set[int] = set()
    for kind, lines, added_code in hunks:
        if kind == "ins":
            if not lines:
                continue  # insertion into an empty file
            candidates = [
                innermost_span(n, spans) if 1 <= n <= len(text_lines) else None
                for n in lines
            ]
            if (
                len(candidates) == 2
                and candidates[0] is not None
                and candidates[0] == candidates[1]
            ):
                widened.update(_body_lines(candidates[0]))
                continue
            return None
        code_lines = [n for n in lines if _is_code(text_lines, n)]
        # A hunk can replace only blank/comment lines on the old side with
        # executable code. Attribute those old positions conservatively;
        # otherwise the hunk would look comment-only and select nothing.
        if added_code and len(code_lines) != len(lines):
            code_lines = list(lines)
        for n in code_lines:
            span = innermost_span(n, spans)
            if span is None:
                return None
            widened.update(_body_lines(span))
    return widened


def expand_to_functions(lines: set[int], spans: list[Span]) -> set[int] | None:
    """Widen plain line numbers (e.g. grep hits) to their innermost function bodies.

    ``None`` when any line sits outside every function (whole file).
    """
    widened: set[int] = set()
    for line in lines:
        span = innermost_span(line, spans)
        if span is None:
            return None
        widened.update(_body_lines(span))
    return widened


# --------------------------------------------------------------------------
# selection
# --------------------------------------------------------------------------


def is_unit_test_file(path: str) -> bool:
    return bool(_TEST_FILE_RE.match(path))


def classify(path: str) -> str:
    """One of ``full``, ``ignore``, ``test``, ``testasset``, ``source``, ``asset``, ``other``."""
    if path in FULL_SUITE_PATHS or path.startswith(FULL_SUITE_PREFIXES):
        return "full"
    if path.startswith(E2E_PREFIX):
        return "ignore"
    if is_unit_test_file(path):
        return "test"
    if any(path.startswith(d + "/") for d in UNIT_TEST_DIRS):
        # Contract snapshots, fixtures, helper modules. Selected by the tests
        # that name them; the full suite if nothing does.
        return "testasset"
    if path.startswith(SOURCE_PREFIXES):
        return "source" if path.endswith(".py") else "asset"
    return "other"


class Selection:
    def __init__(self) -> None:
        self.mode = "subset"
        self.full_reason: str | None = None
        self.files: set[str] = set()
        self.ids: set[str] = set()
        self.notes: list[str] = []

    def force_full(self, reason: str) -> None:
        if self.mode != "full":
            self.mode = "full"
            self.full_reason = reason
        self.notes.append(f"FULL: {reason}")

    def note(self, text: str) -> None:
        self.notes.append(text)

    @property
    def effective_ids(self) -> set[str]:
        """Node ids not already implied by a whole selected file."""
        return {i for i in self.ids if i.split("::", 1)[0] not in self.files}

    def write(self, path: Path, base: str, head: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as fh:
            fh.write(f"# mode: {self.mode}\n")
            fh.write(f"# base: {base}\n# head: {head}\n")
            for note in self.notes:
                fh.write(f"# {note}\n")
            for f in sorted(self.files):
                fh.write(f"{f}\n")
            for i in sorted(self.effective_ids):
                fh.write(f"{i}\n")


def _unit_test_pathspecs() -> list[str]:
    return [f"{d}/test_*.py" for d in UNIT_TEST_DIRS]


def _including_templates(name: str, base: str, cwd: Path) -> set[str]:
    """Basenames of templates that ``include``/``extends``/``import`` ``name``.

    Plain mentions are deliberately not enough: ``_navbar.html`` links to
    every page by URL, and following those would turn any page edit into
    "every page".
    """
    pattern = rf"(include|extends|import|from)[[:space:]]+['\"]([^'\"]*/)?{re.escape(name)}['\"]"
    return {
        os.path.basename(t)
        for t in _grep_files(pattern, base, [f"{TEMPLATE_PREFIX}*"], cwd, regex=True)
    }


def _reference_strings(path: str, base: str, cwd: Path) -> set[str]:
    """Basenames whose appearance in Python source implies use of ``path``.

    A template is chased up through the templates that include it (bounded
    to a few hops); a static asset first through the templates that load it.
    """
    name = os.path.basename(path)
    names = {name}
    if path.startswith(STATIC_PREFIX):
        frontier = {os.path.basename(t) for t in _grep_files(name, base, [f"{TEMPLATE_PREFIX}*"], cwd)}
        names |= frontier
    elif path.startswith(TEMPLATE_PREFIX):
        frontier = {name}
    else:
        return names
    for _ in range(4):
        found: set[str] = set()
        for tpl in frontier:
            found |= _including_templates(tpl, base, cwd)
        frontier = found - names
        if not frontier:
            break
        names |= frontier
    return names


# Tests that iterate the production source tree (contract scans of the
# form ``vireo_dir.glob("*.py")`` or ``package_root.rglob("*.py")``) read
# a module's source without executing any of its lines, so a production
# file's presence in the map — added or not — is irrelevant: those tests
# inspect the file bytes, not run its code. Anchor the pattern on
# ``glob("*.py")`` / ``rglob("*.py")``: a call inside a test that walks
# the source tree.
# Do not use ``\b`` here: ``git grep -E`` delegates to platform regex
# implementations, and macOS treats ``\b`` differently from GNU regex. The
# call-shaped suffix is already specific enough; a false positive only runs an
# extra contract test, while a false negative can let a contract violation pass.
_SOURCE_SCAN_RE = r"(r?glob)\(['\"]\*\.py['\"]\)"


def _add_source_scanning_selections(
    source_path: str,
    base: str,
    cwd: Path,
    test_specs: list[str],
    sel: Selection,
) -> None:
    """Select tests that iterate the production source tree.

    Both added *and modified* ``vireo/`` or ``scripts/`` modules can
    violate an AST-based contract (``test_classifier_construction_contract``,
    ``test_production_onnx_sessions_use_budgeted_factory``,
    ``test_keyword_provenance_contract``) that the map has no way of
    connecting to the file's coverage — those tests read the source with
    ``glob``/``rglob`` and never execute any of its lines, so the map's
    line hits don't reach them. Replacing a cached factory with a
    prohibited direct constructor is a modification, not an addition, and
    must equally trip these contracts. Grep for the scan pattern on
    ``base`` and add the whole test file (contract tests are small, and
    their per-test bodies aren't worth mapping precisely).
    """
    for test_file in _grep_files(_SOURCE_SCAN_RE, base, test_specs, cwd, regex=True):
        if is_unit_test_file(test_file) and test_file not in sel.files:
            sel.files.add(test_file)
            sel.note(f"{source_path}: source-scanning contract in {test_file}")


def touched_declaration_names(hunks: list[Hunk], source: str) -> set[str]:
    """Names of the innermost function/class declarations a hunk touched.

    Declaration-inspecting tests can import a helper by symbol and check
    it with ``inspect.signature(fn)`` / ``inspect.getsource(fn)`` — for
    example, ``vireo/tests/test_thumbnails.py::test_thumbnail_raw_fallbacks_honor_an_explicit_cache_name``
    inspects ``_retry_thumbnail_with_companion`` and
    ``_retry_thumbnail_with_working_copy`` without executing them or
    naming ``thumbnails.py``. Neither the coverage lookup (the body
    never runs under a test context) nor the basename fallback (the
    file's name never appears in the test) selects such tests, so grep
    for the touched symbol names as well.

    For each touched line, pick the *innermost* enclosing function or
    class span in the base source. Innermost matters for nested
    functions (routes inside ``create_app``): the outer name would
    match almost every test that builds the app, while the nested
    name matches only the tests that reference that specific
    declaration. Names shorter than 3 characters are dropped for the
    same reason module-level identifier searches avoid short names.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return set()
    named_spans: list[tuple[str, int, int]] = []  # (name, start, end)
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            start = min([d.lineno for d in node.decorator_list] + [node.lineno])
            end = node.end_lineno or node.lineno
            named_spans.append((node.name, start, end))
    names: set[str] = set()
    for _, lines, _ in hunks:
        for line in lines:
            enclosing = [(n, s, e) for n, s, e in named_spans if s <= line <= e]
            if not enclosing:
                continue
            name = min(enclosing, key=lambda item: item[2] - item[1])[0]
            if len(name) >= 3:
                names.add(name)
    return names


def _add_mention_selections(
    path: str,
    base: str,
    head: str,
    cwd: Path,
    impact: ImpactMap,
    test_specs: list[str],
    sel: Selection,
    needle: str | None = None,
    word: bool = False,
) -> bool:
    """Select tests that mention ``needle`` (defaults to ``path``'s basename) in their source.

    Returns ``True`` if any test file mentioned ``needle``. Test files present
    in the map narrow to the tests whose enclosing function contains the
    mention; module-level mentions widen to the whole test file. Test files
    that exist only on ``head`` are added whole (they are new and have no
    map history).

    ``needle`` allows callers to grep for something other than the basename,
    such as a function imported and inspected by name. ``word=True``
    grep-matches whole words only, so an identifier like ``alpha`` doesn't
    pull in ``test_alpha``.
    """
    if needle is None:
        needle = os.path.basename(path)
    mentioned = False
    for test_file, linenos in _grep_lines(needle, base, test_specs, cwd, word=word).items():
        if not is_unit_test_file(test_file):
            continue
        mentioned = True
        picked = None
        if impact.has_file(test_file):
            widened = expand_to_functions(linenos, function_spans(_show(base, test_file, cwd) or ""))
            if widened is not None:
                picked = impact.tests_for_lines(test_file, widened)
        if picked:
            sel.ids |= picked
            sel.note(f"{path}: {test_file} mentions '{needle}' -> {len(picked)} tests")
        else:
            sel.files.add(test_file)
            sel.note(f"{path}: {test_file} mentions '{needle}' (whole file)")
    for test_file in _grep_files(needle, head, test_specs, cwd, word=word):
        if is_unit_test_file(test_file) and test_file not in sel.files and not _show(base, test_file, cwd):
            mentioned = True
            sel.files.add(test_file)
            sel.note(f"{path}: new {test_file} mentions '{needle}'")
    return mentioned


def select(
    base: str,
    head: str,
    impact: ImpactMap | None,
    cwd: Path = ROOT,
    explain: bool = False,
) -> Selection:
    sel = Selection()
    if impact is None:
        sel.force_full("no impact map available")
        return sel

    changes = changed_files(base, head, cwd)
    if not changes:
        sel.mode = "none"
        sel.note("no files changed")
        return sel

    hunks_by_path = old_side_hunks(base, head, cwd)
    test_specs = _unit_test_pathspecs()
    source_specs = ["vireo/*.py", "scripts/*.py"]
    for d in UNIT_TEST_DIRS:
        source_specs.append(f":(exclude){d}/*")

    for status, path in changes:
        kind = classify(path)
        if kind == "ignore":
            continue
        if kind == "full":
            sel.force_full(f"{path} changed")
            continue
        if kind == "test":
            if status != "D":
                sel.files.add(path)
                sel.note(f"{path}: whole file (test file {status})")
            continue

        # Tests that mention the file by name run regardless of coverage.
        # For ``vireo/**.py`` and ``scripts/**.py`` the mention fallback is
        # handled inside the source-file branch below (alongside the map
        # lookup and the source-scanning contract fallback), so avoid
        # running it twice.
        mentioned = False
        if kind != "source":
            mentioned = _add_mention_selections(path, base, head, cwd, impact, test_specs, sel)

        if kind == "testasset":
            if not mentioned and status != "D":
                sel.force_full(f"{path} changed and no test file names it")
            continue

        if kind == "source":
            # Added modules have no diff hunks in the map's world, so
            # fall through directly to the structural-contract fallbacks
            # after noting the addition.
            if status == "A":
                sel.note(f"{path}: added, no history in map")
                _add_source_scanning_selections(path, base, cwd, test_specs, sel)
                _add_mention_selections(path, base, head, cwd, impact, test_specs, sel)
                continue

            hunks = hunks_by_path.get(path)
            source = _show(base, path, cwd) or ""
            widened = None if (status == "D" or not hunks) else impacted_lines(hunks, source)

            # A pure comment/whitespace edit alters no code path and no
            # structural contract; skip every fallback so the diff stays
            # ``mode: none``.
            if hunks and widened is not None and not widened:
                sel.note(f"{path}: comment/blank-only edit; selecting nothing")
                continue

            # Source-scanning contracts (``glob("*.py")``/``rglob("*.py")``)
            # read the file's bytes without executing any of its lines, so
            # both added and modified modules can trip them regardless of
            # what the coverage map records for the file.
            _add_source_scanning_selections(path, base, cwd, test_specs, sel)
            # Declaration- and source-inspecting tests (route contract
            # snapshots, ``ast.parse`` of the source, ``app.url_map``
            # audits, tests that iterate route bodies via AST) never
            # execute the affected function's body but do reference the
            # source file by name. Include them for every source-file
            # change: a signature or decorator edit misses them via body
            # lookup, and a body edit can equally trip a source-parsing
            # contract test.
            _add_mention_selections(path, base, head, cwd, impact, test_specs, sel)

            # Symbol-inspecting tests import a helper and check it with
            # ``inspect.signature`` / ``inspect.getsource`` without ever
            # mentioning the source file's basename, so the fallback
            # above misses them. For every touched function or class
            # declaration, grep tests for its name too. Word-boundary
            # so ``alpha`` doesn't sweep in ``test_alpha``.
            if hunks:
                for name in touched_declaration_names(hunks, source):
                    _add_mention_selections(
                        path, base, head, cwd, impact, test_specs, sel,
                        needle=name, word=True,
                    )

            if widened is None:
                # Coverage cannot safely narrow import-time changes. A constant
                # may be copied into a production consumer during collection,
                # then affect a test that executes only the consumer; neither
                # the defining source nor direct test mentions establish that
                # dependency in the map. Deletions, missing/invalid base
                # source, and ambiguous insertions carry the same risk.
                sel.force_full(f"{path}: module-level or structurally ambiguous change")
                continue

            if not impact.has_file(path):
                sel.note(f"{path}: not in map (never imported by a test)")
                continue

            picked = impact.tests_for_lines(path, widened)
            sel.note(f"{path}: {len(hunks)} hunks in {len(widened)} function lines -> {len(picked)} tests")
            sel.ids |= picked
            continue

        # kind in {"asset", "other"}: Python lines that mention the file (or
        # a template that includes it) stand in for the file itself.
        if kind == "asset":
            names = _reference_strings(path, base, cwd)
            total = 0
            for name in names:
                for src, linenos in _grep_lines(name, base, source_specs, cwd).items():
                    if not impact.has_file(src):
                        continue
                    widened = expand_to_functions(linenos, function_spans(_show(base, src, cwd) or ""))
                    picked = impact.tests_for_lines(src, widened)
                    total += len(picked)
                    sel.ids |= picked
                    if explain:
                        sel.note(f"{path}: via '{name}' in {src} -> {len(picked)} tests")
            sel.note(f"{path}: referenced by {sorted(names)} -> {total} tests")

    if sel.mode == "full":
        return sel
    if not sel.files and not sel.ids:
        sel.mode = "none"
        sel.note("no unit test exercises the changed files")
    return sel


# --------------------------------------------------------------------------
# map packaging
# --------------------------------------------------------------------------


def load_map(map_dir: Path) -> tuple[ImpactMap | None, dict, str | None]:
    """``(map, meta, problem)``; ``map`` is ``None`` when unusable."""
    db = map_dir / MAP_DB
    meta_path = map_dir / MAP_META
    if not db.is_file() or not meta_path.is_file():
        return None, {}, f"no map at {map_dir} (expected {MAP_DB} + {MAP_META})"
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        return None, {}, f"unreadable {meta_path}: {exc}"
    if not meta.get("sha"):
        return None, meta, f"{meta_path} has no 'sha'"
    try:
        impact = ImpactMap(db, meta.get("root"))
    except Exception as exc:  # noqa: BLE001 - any corruption means "no map"
        return None, meta, f"unreadable {db}: {exc}"
    if impact.context_count == 0:
        return None, meta, f"{db} has no per-test contexts (built without --cov-context=test?)"
    return impact, meta, None


def executed_tests_from_junit(junit_xml: Path) -> int:
    """Number of tests the junit report says ran (skipped ones excluded)."""
    import xml.etree.ElementTree as ET

    root = ET.parse(junit_xml).getroot()
    ran = 0
    for case in root.iter("testcase"):
        if case.find("skipped") is None:
            ran += 1
    return ran


# A map missing more than this share of the executed tests is treated as
# partial (a crashed xdist worker loses its coverage data) and rejected: a
# partial map would make PRs skip tests without anyone noticing. The map run
# measures ``tests/`` too, so every executed test records at least its own
# body and the expected ratio is 1.0; the slack only absorbs junit/nodeid
# accounting quirks, not lost workers.
MIN_CONTEXT_FRACTION = 0.99


def build_map(
    coverage_file: Path,
    map_dir: Path,
    sha: str,
    cwd: Path = ROOT,
    junit_xml: Path | None = None,
) -> dict:
    if not coverage_file.is_file():
        raise SystemExit(f"coverage data file not found: {coverage_file}")
    map_dir.mkdir(parents=True, exist_ok=True)
    target = map_dir / MAP_DB
    shutil.copyfile(coverage_file, target)
    # Coverage's combine step leaves the sqlite file fragmented; compact it so
    # the cache/artifact upload is small.
    conn = sqlite3.connect(str(target))
    try:
        with conn:
            conn.execute("VACUUM")
    finally:
        conn.close()
    impact = ImpactMap(target, str(cwd))
    if impact.context_count == 0:
        raise SystemExit(
            f"{coverage_file} has no per-test contexts; run pytest with --cov-context=test"
        )
    executed = None
    if junit_xml is not None:
        executed = executed_tests_from_junit(junit_xml)
        if impact.context_count < MIN_CONTEXT_FRACTION * executed:
            raise SystemExit(
                f"map has contexts for {impact.context_count} tests but {executed} ran; "
                "coverage data looks partial (crashed worker?), refusing to publish it"
            )
    meta = {
        "sha": sha,
        "root": str(cwd),
        "built_at": _dt.datetime.now(_dt.UTC).isoformat(timespec="seconds"),
        "contexts": impact.context_count,
        "executed_tests": executed,
        "size_bytes": target.stat().st_size,
    }
    (map_dir / MAP_META).write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")
    return meta


def fetch_map(map_dir: Path, cwd: Path = ROOT) -> dict:
    if shutil.which("gh") is None:
        raise SystemExit("fetch-map needs the GitHub CLI (gh) on PATH")
    listing = subprocess.run(
        [
            "gh", "run", "list", "--workflow", MAP_WORKFLOW, "--branch", "main",
            "--limit", "5", "--json", "databaseId,conclusion,headSha",
        ],
        cwd=str(cwd),
        capture_output=True,
        text=True,
    )
    if listing.returncode != 0:
        raise SystemExit(f"gh run list failed: {listing.stderr.strip()}")
    runs = json.loads(listing.stdout or "[]")
    map_dir.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=f".{map_dir.name}-fetch-", dir=map_dir.parent) as temp:
        temp_dir = Path(temp)
        for run in runs:
            if run.get("conclusion") == "cancelled":
                continue
            attempt = temp_dir / str(run["databaseId"])
            download = subprocess.run(
                [
                    "gh", "run", "download", str(run["databaseId"]),
                    "--name", MAP_ARTIFACT, "--dir", str(attempt),
                ],
                cwd=str(cwd),
                capture_output=True,
                text=True,
            )
            if (
                download.returncode != 0
                or not (attempt / MAP_META).is_file()
                or not (attempt / MAP_DB).is_file()
            ):
                continue
            try:
                meta = json.loads((attempt / MAP_META).read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue

            # Keep the prior working map until a complete download has been
            # validated. Move it into this temporary directory during the
            # swap so any replacement failure can restore it immediately.
            previous = temp_dir / "previous"
            if map_dir.exists():
                map_dir.rename(previous)
            try:
                attempt.rename(map_dir)
            except Exception:
                if previous.exists():
                    previous.rename(map_dir)
                raise
            print(
                f"fetched {MAP_ARTIFACT} from run {run['databaseId']} "
                f"(main @ {meta.get('sha', '?')[:10]}, {meta.get('contexts', '?')} tests) -> {map_dir}",
                file=sys.stderr,
            )
            return meta
    raise SystemExit(f"no downloadable {MAP_ARTIFACT} artifact in the last {len(runs)} '{MAP_WORKFLOW}' runs")


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def _summary(sel: Selection, meta: dict, base: str) -> str:
    lines = [f"mode: {sel.mode}"]
    if sel.mode == "full":
        lines.append(f"reason: {sel.full_reason}")
    if meta:
        lines.append(f"map: main @ {base[:10]} ({meta.get('contexts', '?')} tests, built {meta.get('built_at', '?')})")
    if sel.mode == "subset":
        lines.append(f"selected: {len(sel.files)} whole files + {len(sel.effective_ids)} individual tests")
    return "\n".join(lines)


def cmd_select(args: argparse.Namespace) -> int:
    cwd = Path(args.repo).resolve()
    map_dir = cwd / args.map_dir
    impact, meta, problem = load_map(map_dir)
    base = args.base or meta.get("sha", "")
    head = args.head

    if impact is None:
        sel = Selection()
        sel.force_full(problem or "no impact map")
    elif not _ensure_commit(base, cwd):
        sel = Selection()
        sel.force_full(f"map commit {base[:10]} is not reachable from this checkout")
    else:
        sel = select(base, head, impact, cwd=cwd, explain=args.explain)

    output = cwd / (args.output or f"{args.map_dir}/{SELECTION_FILE}")
    sel.write(output, base, head)
    print(_summary(sel, meta, base), file=sys.stderr)
    if args.explain:
        for note in sel.notes:
            print(f"  {note}", file=sys.stderr)
    print(f"selection written to {output}", file=sys.stderr)
    print(sel.mode)

    if not args.run:
        return 0
    if sel.mode == "none":
        print("nothing to run", file=sys.stderr)
        return 0
    cmd = [sys.executable, "-m", "pytest"]
    if sel.mode == "full":
        cmd += list(UNIT_TEST_DIRS)
    else:
        cmd += [*UNIT_TEST_DIRS, "--selected-tests", str(output)]
    cmd += args.pytest_args
    print("+ " + " ".join(cmd), file=sys.stderr)
    return subprocess.call(cmd, cwd=str(cwd))


def cmd_build_map(args: argparse.Namespace) -> int:
    cwd = Path(args.repo).resolve()
    sha = args.sha or _git("rev-parse", "HEAD", cwd=cwd).strip()
    meta = build_map(
        Path(args.coverage_file),
        cwd / args.map_dir,
        sha,
        cwd=cwd,
        junit_xml=Path(args.junit_xml) if args.junit_xml else None,
    )
    print(json.dumps(meta, indent=2))
    return 0


def cmd_fetch_map(args: argparse.Namespace) -> int:
    cwd = Path(args.repo).resolve()
    fetch_map(cwd / args.map_dir, cwd=cwd)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--repo", default=str(ROOT), help="repository root (default: this checkout)")
    parser.add_argument("--map-dir", default=MAP_DIR, help=f"impact map directory (default: {MAP_DIR})")
    sub = parser.add_subparsers(dest="command")

    p_select = sub.add_parser("select", help="select tests for the current changes (default)")
    p_select.add_argument("--base", help="commit the map was built at (default: from meta.json)")
    p_select.add_argument("--head", default="HEAD", help="commit to test (default: HEAD)")
    p_select.add_argument("--output", help=f"selection file (default: <map-dir>/{SELECTION_FILE})")
    p_select.add_argument("--explain", action="store_true", help="print why each file selected what it did")
    p_select.add_argument("--run", action="store_true", help="run pytest on the selection; extra args after --")
    p_select.add_argument("pytest_args", nargs=argparse.REMAINDER, help="arguments passed through to pytest")
    p_select.set_defaults(func=cmd_select)

    p_build = sub.add_parser("build-map", help="package a coverage data file as the impact map")
    p_build.add_argument("--coverage-file", default=".coverage")
    p_build.add_argument("--sha", help="commit the coverage run tested (default: HEAD)")
    p_build.add_argument(
        "--junit-xml",
        help="pytest --junitxml report of the same run; rejects a map that misses >1%% of executed tests",
    )
    p_build.set_defaults(func=cmd_build_map)

    p_fetch = sub.add_parser("fetch-map", help=f"download the newest map artifact from the '{MAP_WORKFLOW}' workflow")
    p_fetch.set_defaults(func=cmd_fetch_map)
    return parser


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    commands = {"select", "build-map", "fetch-map"}
    # ``select`` is the default subcommand; global options may precede it.
    idx = 0
    while idx < len(argv):
        if argv[idx] in ("--repo", "--map-dir"):
            idx += 2
        elif argv[idx].startswith(("--repo=", "--map-dir=")):
            idx += 1
        else:
            break
    if idx >= len(argv) or argv[idx] not in commands and argv[idx] not in ("-h", "--help"):
        argv.insert(idx, "select")
    args = build_parser().parse_args(argv)
    if args.command == "select" and args.pytest_args and args.pytest_args[0] == "--":
        args.pytest_args = args.pytest_args[1:]
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
