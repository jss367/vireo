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
