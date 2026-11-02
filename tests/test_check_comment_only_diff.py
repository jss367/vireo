"""Regression tests for the scripts/check_comment_only_diff.py safety check.

The checker protects comment-cleanup PRs from accidental code changes, so a
false success here lets an executable edit land unreviewed. These tests
exercise ``python_differs``, ``js_differs`` and ``_source_changes`` on the
exact shapes earlier fixes closed, so a regression that reopens one of them
fails here rather than reaching a cleanup PR silently.
"""

import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import check_comment_only_diff as checker  # noqa: E402

_GIT_REPO_ENV = (
    "GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_OBJECT_DIRECTORY",
    "GIT_ALTERNATE_OBJECT_DIRECTORIES", "GIT_COMMON_DIR", "GIT_PREFIX", "GIT_NAMESPACE",
)


@pytest.fixture(autouse=True)
def _scrub_git_env(monkeypatch):
    """Inherited Git repo-location vars would otherwise redirect ``git init``
    (and ``_git``) away from ``tmp_path``, so a stray ``GIT_DIR`` in the
    test runner could silently point the fixture at the real repository."""
    for name in _GIT_REPO_ENV:
        monkeypatch.delenv(name, raising=False)

# ---------------------------------------------------------------------------
# python_differs: pragmas, shebang, encoding
# ---------------------------------------------------------------------------

def test_python_differs_bare_broad_pragma_reason_removal():
    """``# broad: reason`` -> ``# broad:`` is a lost reason, not a wording edit."""
    before = "try:\n    f()\nexcept Exception:  # broad: boundary reason\n    pass\n"
    after = "try:\n    f()\nexcept Exception:  # broad:\n    pass\n"
    assert checker.python_differs(before, after) is not None


def test_python_differs_broad_reason_rewording_is_noticed():
    before = "try:\n    f()\nexcept Exception:  # broad: a\n    pass\n"
    after = "try:\n    f()\nexcept Exception:  # broad: b\n    pass\n"
    assert checker.python_differs(before, after) is not None


def test_python_differs_broad_reason_identical_passes():
    text = "try:\n    f()\nexcept Exception:  # broad: boundary reason\n    pass\n"
    assert checker.python_differs(text, text) is None


def test_python_differs_shebang_change_fails():
    before = "#!/usr/bin/env python3\nx = 1\n"
    after = "#!/usr/bin/env python2\nx = 1\n"
    assert checker.python_differs(before, after) is not None


def test_python_differs_encoding_change_fails():
    before = "# coding: utf-8\nx = 1\n"
    after = "# coding: latin-1\nx = 1\n"
    assert checker.python_differs(before, after) is not None


def test_python_differs_lowercase_noqa_removal_fails():
    before = "x = 1  # noqa: F841\n"
    after = "x = 1\n"
    assert checker.python_differs(before, after) is not None


def test_python_differs_ruff_noqa_removal_fails():
    before = "x = 1  # ruff: noqa: F841\n"
    after = "x = 1\n"
    assert checker.python_differs(before, after) is not None


def test_python_differs_pylint_directive_removal_fails():
    """``# pylint: disable=...`` is a load-bearing directive, not prose."""
    before = "x = 1  # pylint: disable=unused-variable\n"
    after = "x = 1\n"
    assert checker.python_differs(before, after) is not None


def test_python_differs_pylint_dropped_while_broad_reason_kept_fails():
    """Removing ``pylint:`` must still fail even when a ``broad:`` reason persists."""
    before = "x = 1  # pylint: disable=W0703  # broad: boundary\n"
    after = "x = 1  # broad: boundary\n"
    assert checker.python_differs(before, after) is not None


def test_python_differs_docstring_edit_passes():
    """Docstrings are deliberately stripped: this is a documented hole."""
    before = '"""Old wording."""\nx = 1\n'
    after = '"""New wording."""\nx = 1\n'
    assert checker.python_differs(before, after) is None


def test_python_differs_statement_change_fails():
    before = "x = 1\n"
    after = "x = 2\n"
    assert checker.python_differs(before, after) is not None


# ---------------------------------------------------------------------------
# js_differs: regexes, directives, hashbangs, @import, @ts-expect-error
# ---------------------------------------------------------------------------

NODE = __import__("shutil").which("node")
HAS_ACORN = (ROOT / "node_modules" / "acorn").is_dir()
needs_node = pytest.mark.skipif(not NODE or not HAS_ACORN, reason="node + acorn required")


@needs_node
def test_js_differs_regex_pattern_change_fails():
    """Earlier fix: tokens are compared by pattern/flags, not ``[object Object]``."""
    assert checker.js_differs("const r = /cats/i;\n", "const r = /dogs/i;\n") is not None


@needs_node
def test_js_differs_regex_flags_change_fails():
    assert checker.js_differs("const r = /cats/i;\n", "const r = /cats/g;\n") is not None


@needs_node
def test_js_differs_ts_expect_error_removal_fails():
    """Earlier fix: ``@ts-expect-error`` is now recognized."""
    before = "// @ts-expect-error legacy\nx();\n"
    after = "x();\n"
    assert checker.js_differs(before, after) is not None


@needs_node
def test_js_differs_jsdoc_import_source_change_fails():
    """Earlier fix: ``@import`` module source is captured separately."""
    before = '/** @import { Pet } from "./cats.js" */\nx();\n'
    after = '/** @import { Pet } from "./dogs.js" */\nx();\n'
    assert checker.js_differs(before, after) is not None


@needs_node
def test_js_differs_hashbang_change_fails():
    """Earlier fix: acorn drops hashbangs, so the first line is compared."""
    before = "#!/usr/bin/env node\nconsole.log('hi');\n"
    after = "#!/usr/bin/env deno\nconsole.log('hi');\n"
    assert checker.js_differs(before, after) is not None


@needs_node
def test_js_differs_comment_only_edit_passes():
    before = "// old wording\nx();\n"
    after = "// new wording\nx();\n"
    assert checker.js_differs(before, after) is None


@needs_node
def test_js_differs_statement_change_fails():
    assert checker.js_differs("x = 1;\n", "x = 2;\n") is not None


@needs_node
def test_js_differs_exported_directive_removal_fails():
    """``/* exported foo */`` is read by ESLint; removing it can introduce
    ``no-unused-vars`` errors, so the checker must detect its loss."""
    before = "/* exported foo */\nconst foo = 1;\n"
    after = "const foo = 1;\n"
    assert checker.js_differs(before, after) is not None


# ---------------------------------------------------------------------------
# _source_changes: file discovery across extensions and renames
# ---------------------------------------------------------------------------

def _run_in_fixture(tmp_path: Path, *writes: tuple[str, str | None], rename: tuple[str, str] | None = None):
    """Build a one-commit base, apply a change set in the worktree, return entries."""
    env = os.environ | {"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t", "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t"}
    subprocess.run(["git", "init", "-q", "-b", "main", str(tmp_path)], check=True, env=env)
    subprocess.run(["git", "-C", str(tmp_path), "commit", "-q", "--allow-empty", "-m", "root"], check=True, env=env)
    orig_root = checker.ROOT
    checker.ROOT = tmp_path
    try:
        for name, body in writes:
            if body is None:
                continue
            path = tmp_path / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(body, encoding="utf-8")
            subprocess.run(["git", "-C", str(tmp_path), "add", name], check=True, env=env)
        if any(body is not None for _, body in writes):
            subprocess.run(["git", "-C", str(tmp_path), "commit", "-q", "-m", "base"], check=True, env=env)
        base = subprocess.run(
            ["git", "-C", str(tmp_path), "rev-parse", "HEAD"], check=True, capture_output=True, text=True, env=env,
        ).stdout.strip()
        if rename:
            src, dst = rename
            subprocess.run(["git", "-C", str(tmp_path), "mv", src, dst], check=True, env=env)
        return checker._source_changes(base)
    finally:
        checker.ROOT = orig_root


def test_source_changes_detects_mjs_file(tmp_path):
    (tmp_path / "scripts").mkdir()
    entries = _run_in_fixture(tmp_path, ("scripts/x.mjs", "const a = 1;\n"))
    # Modify in worktree
    (tmp_path / "scripts" / "x.mjs").write_text("const a = 2;\n", encoding="utf-8")
    base = subprocess.run(
        ["git", "-C", str(tmp_path), "rev-parse", "HEAD"], check=True, capture_output=True, text=True,
    ).stdout.strip()
    orig_root = checker.ROOT
    checker.ROOT = tmp_path
    try:
        entries = checker._source_changes(base)
    finally:
        checker.ROOT = orig_root
    assert any(label.endswith("x.mjs") for label, *_ in entries), entries
    assert all(suffix in (".mjs", ".js", ".cjs") for _, _, _, suffix in entries if suffix != "mixed")


def test_source_changes_detects_cjs_file(tmp_path):
    _run_in_fixture(tmp_path, ("helper.cjs", "const a = 1;\n"))
    (tmp_path / "helper.cjs").write_text("const a = 2;\n", encoding="utf-8")
    base = subprocess.run(
        ["git", "-C", str(tmp_path), "rev-parse", "HEAD"], check=True, capture_output=True, text=True,
    ).stdout.strip()
    orig_root = checker.ROOT
    checker.ROOT = tmp_path
    try:
        entries = checker._source_changes(base)
    finally:
        checker.ROOT = orig_root
    assert any(label.endswith("helper.cjs") and suffix == ".cjs" for label, _, _, suffix in entries), entries


def test_source_changes_rename_to_non_source_is_not_dropped(tmp_path):
    """A ``.py`` renamed to ``.txt`` is a source deletion, not a non-event."""
    _run_in_fixture(tmp_path, ("module.py", "import os\nX = 1\n"))
    subprocess.run(["git", "-C", str(tmp_path), "mv", "module.py", "module.txt"], check=True)
    base = subprocess.run(
        ["git", "-C", str(tmp_path), "rev-parse", "HEAD"], check=True, capture_output=True, text=True,
    ).stdout.strip()
    orig_root = checker.ROOT
    checker.ROOT = tmp_path
    try:
        entries = checker._source_changes(base)
    finally:
        checker.ROOT = orig_root
    assert entries, "rename to non-source must still produce an entry"
    label, old_path, new_path, suffix = entries[0]
    assert old_path == "module.py"
    assert new_path == "module.txt"
    assert suffix == ".py"


def test_source_changes_rename_from_non_source_is_an_addition(tmp_path):
    _run_in_fixture(tmp_path, ("module.txt", "not source\n"))
    subprocess.run(["git", "-C", str(tmp_path), "mv", "module.txt", "module.py"], check=True)
    base = subprocess.run(
        ["git", "-C", str(tmp_path), "rev-parse", "HEAD"], check=True, capture_output=True, text=True,
    ).stdout.strip()
    orig_root = checker.ROOT
    checker.ROOT = tmp_path
    try:
        entries = checker._source_changes(base)
    finally:
        checker.ROOT = orig_root
    assert entries
    _, _, new_path, suffix = entries[0]
    assert new_path == "module.py" and suffix == ".py"


def test_source_changes_cross_language_rename_is_mixed(tmp_path):
    _run_in_fixture(tmp_path, ("module.py", "X = 1\n"))
    subprocess.run(["git", "-C", str(tmp_path), "mv", "module.py", "module.js"], check=True)
    base = subprocess.run(
        ["git", "-C", str(tmp_path), "rev-parse", "HEAD"], check=True, capture_output=True, text=True,
    ).stdout.strip()
    orig_root = checker.ROOT
    checker.ROOT = tmp_path
    try:
        entries = checker._source_changes(base)
    finally:
        checker.ROOT = orig_root
    assert entries
    assert entries[0][3] == "mixed", entries


def test_source_changes_detects_non_ascii_path(tmp_path):
    """``git diff --name-status`` without ``-z`` quotes non-ASCII paths with
    ``core.quotePath=true``, which would make ``_suffix_of`` drop them and
    certify an executable module edit as comment-only."""
    _run_in_fixture(tmp_path, ("naïve.py", "X = 1\n"))
    (tmp_path / "naïve.py").write_text("X = 2\n", encoding="utf-8")
    base = subprocess.run(
        ["git", "-C", str(tmp_path), "rev-parse", "HEAD"], check=True, capture_output=True, text=True,
    ).stdout.strip()
    orig_root = checker.ROOT
    checker.ROOT = tmp_path
    try:
        entries = checker._source_changes(base)
    finally:
        checker.ROOT = orig_root
    assert entries, "non-ASCII source file must not be dropped by name parsing"
    label, old_path, new_path, suffix = entries[0]
    assert old_path == "naïve.py"
    assert new_path == "naïve.py"
    assert suffix == ".py"


@needs_node
@pytest.mark.parametrize("terminator", ["\n", "\r", "\r\n", "\u2028", "\u2029"])
def test_js_comment_line_terminators_preserve_asi(terminator):
    before = "function f() { return /* note" + terminator + "*/ value; }"
    after = "function f() { return /* note */ value; }"
    assert checker.js_differs(before, after)
    assert checker.js_differs(before, before.replace("note", "clear explanation")) is None


@needs_node
@pytest.mark.parametrize("before, after", [
    ("let x=1,y=2; x\n++y;", "let x=1,y=2; x++\ny;"),
    ("// eslint-disable-next-line\nx();\ny();", "x();\n// eslint-disable-next-line\ny();"),
    ("// @ts-ignore\nx();\ny();", "x();\n// @ts-ignore\ny();"),
    ("/** @type {number} */\nlet x;\nlet y;", "let x;\n/** @type {number} */\nlet y;"),
    ("/* exported x */\nfunction x() {}", "/* exported y */\nfunction x() {}"),
    ('/* eslint rule: ["a  b"] */\nx();', '/* eslint rule: ["a b"] */\nx();'),
])
def test_js_semantic_comment_content_and_attachment(before, after):
    assert checker.js_differs(before, after)


@needs_node
def test_tagged_template_raw_text_is_preserved():
    before, after = r"tag`\x41`", "tag`A`"
    assert checker._js(before)["tokens"] == checker._js(after)["tokens"]
    assert checker._js(before)["syntax"] != checker._js(after)["syntax"]
    assert checker.js_differs(before, after)


@needs_node
def test_js_prose_edits_preserve_directives_and_types():
    before = "// old prose\n/* exported x */\n/** @param {number} y old description */\nfunction x(y) {}"
    after = "// clear prose\n/* exported x */\n/** @param {number} y clear description */\nfunction x(y) {}"
    assert checker.js_differs(before, after) is None


@pytest.mark.parametrize("before, after", [
    ('"""short"""\nx = 1  # noqa: F841\n', '"""longer\ndocumentation"""\nx = 1  # noqa: F841\n'),
    ('"""remove me"""\nx = 1  # noqa: F841\n', 'x = 1  # noqa: F841\n'),
    ('x = 1  # noqa: F841\n', '"""added documentation"""\nx = 1  # noqa: F841\n'),
    ('def f():\n    """short"""\n    return 1  # type: ignore\n',
     'def f():\n    """longer\n    documentation"""\n    return 1  # type: ignore\n'),
    ('@register\ndef f():\n    """runtime name A"""\n    return 1\n',
     '@register\ndef f():\n    """runtime name B"""\n    return 1\n'),
])
def test_python_docstring_cleanup_remains_allowed_with_manual_semantic_review(before, after):
    # The decorator/ __doc__ case deliberately remains a manual-review boundary.
    assert checker.python_differs(before, after) is None


@pytest.mark.parametrize("before, after", [
    ("x = 1  # noqa: F841\ny = 2\n", "x = 1\ny = 2  # noqa: F841\n"),
    ("x = 1  # type: ignore\ny = 2\n", "x = 1\ny = 2  # type: ignore\n"),
    ('x = 1  # type: Literal["a  b"]\n', 'x = 1  # type: Literal["a b"]\n'),
])
def test_python_tool_pragma_attachment_and_content(before, after):
    assert checker.python_differs(before, after)


@pytest.mark.parametrize("suffix", checker.SOURCE_EXTS)
@pytest.mark.parametrize("comment_only", [False, True])
def test_untracked_supported_source_fails_closed(tmp_path, monkeypatch, suffix, comment_only):
    _run_in_fixture(tmp_path)
    source = "x = 1\n" if suffix == ".py" else "const x = 1;\n"
    if comment_only:
        source = "# prose\n" if suffix == ".py" else "// prose\n"
    (tmp_path / ("untracked" + suffix)).write_text(source, encoding="utf-8")
    monkeypatch.setattr(checker, "ROOT", tmp_path)
    monkeypatch.setattr(sys, "argv", ["check_comment_only_diff.py", "--base", "HEAD"])
    assert checker.main() == 1


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX filename characters")
def test_tracked_newline_filename_is_compared(tmp_path, monkeypatch):
    name = "module\nname.py"
    _run_in_fixture(tmp_path, (name, "x = 1\n"))
    (tmp_path / name).write_text("x = 2\n", encoding="utf-8")
    monkeypatch.setattr(checker, "ROOT", tmp_path)
    monkeypatch.setattr(sys, "argv", ["check_comment_only_diff.py", "--base", "HEAD"])
    assert checker.main() == 1


@needs_node
def test_module_and_commonjs_contexts_are_parsed():
    assert checker.js_differs("await load();", "// prose\nawait load();", "module") is None
    assert checker.js_differs("return 1;", "// prose\nreturn 1;", "commonjs") is None
    assert checker.js_differs("return 1;", "return 2;", "commonjs")


@needs_node
def test_bounded_checker_parses_first_party_js_corpus():
    files = [rel for rel in checker._git("ls-files", "-z").split("\0")
             if rel and checker._suffix_of(rel) in checker.JS_EXTS
             and "vendor" not in Path(rel).parts and not rel.endswith(".min.js")]
    assert files
    for rel in files:
        mode = {".mjs": "module", ".cjs": "commonjs"}.get(Path(rel).suffix)
        checker._js((ROOT / rel).read_text(encoding="utf-8"), mode)


@needs_node
def test_directive_prologue_raw_spelling_is_semantic():
    before = "function f(){'use strict';return this;}"
    after = r"function f(){'use\x20strict';return this;}"
    assert checker._js(before)["tokens"] == checker._js(after)["tokens"]
    assert checker._js(before)["syntax"] != checker._js(after)["syntax"]
    assert checker.js_differs(before, after)
    assert checker.js_differs(before, before.replace(";return", ";/* prose */return")) is None


@needs_node
@pytest.mark.parametrize("js_code_change", [False, True])
def test_js_guards_do_not_depend_on_rejecting_python_docstring_edits(tmp_path, monkeypatch, capsys, js_code_change):
    before = "function f(){return /* prose\n*/ value;}"
    _run_in_fixture(tmp_path, ("doc.py", '"""old wording"""\nx=1\n'), ("controller.js", before))
    (tmp_path / "doc.py").write_text('"""new wording"""\nx=1\n', encoding="utf-8")
    after = before.replace("prose\n", "prose") if js_code_change else before.replace("prose", "clear prose")
    (tmp_path / "controller.js").write_text(after, encoding="utf-8")
    original_js = checker._js
    def fixture_js(source, source_type=None):
        with monkeypatch.context() as context:
            context.setattr(checker, "ROOT", ROOT)
            return original_js(source, source_type)
    monkeypatch.setattr(checker, "_js", fixture_js)
    monkeypatch.setattr(checker, "ROOT", tmp_path)
    monkeypatch.setattr(sys, "argv", ["check_comment_only_diff.py", "--base", "HEAD"])
    assert checker.main() == int(js_code_change)
    result = capsys.readouterr()
    assert "ok   doc.py" in result.out
    if js_code_change:
        assert "controller.js: code" in result.err
        assert "doc.py:" not in result.err
