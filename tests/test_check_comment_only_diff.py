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


@needs_node
def test_js_differs_directive_prologue_string_escape_fails():
    """``"use strict"`` -> ``"use\\x20strict"`` has the same cooked token
    value but only the literal spelling activates strict mode."""
    before = '"use strict";\nconst x = 1;\n'
    after = '"use\\x20strict";\nconst x = 1;\n'
    assert checker.js_differs(before, after) is not None


@needs_node
def test_js_differs_tagged_template_raw_escape_fails():
    """``tag`\\x41` `` vs ``tag`A` `` have the same cooked template value, but
    a tag that reads ``strings.raw`` sees the escape."""
    before = "tag`\\x41`;\n"
    after = "tag`A`;\n"
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
    assert old_path == "naïve.py", (ascii(old_path), ascii("naïve.py"))
    assert new_path == "naïve.py", (ascii(new_path), ascii("naïve.py"))
    assert suffix == ".py"


@pytest.mark.parametrize("name", ["naïve.py", "鳥.py"])
def test_git_decoding_preserves_unicode_names_and_blobs(tmp_path, monkeypatch, name):
    """Model an ANSI subprocess default; Git paths/blobs must not use it."""
    before = "LABEL = 'café'\n"
    _run_in_fixture(tmp_path, (name, before))
    (tmp_path / name).write_text(before + "# prose\n", encoding="utf-8")
    monkeypatch.setattr(checker, "ROOT", tmp_path)
    monkeypatch.setattr(subprocess, "_text_encoding", lambda: "cp1252")
    entries = checker._source_changes("HEAD")
    assert entries[0][1] == name, (ascii(entries[0][1]), ascii(name))
    assert entries[0][2] == name, (ascii(entries[0][2]), ascii(name))
    assert checker._git("show", f"HEAD:{name}") == before
    monkeypatch.setattr(sys, "argv", ["check_comment_only_diff.py", "--base", "HEAD"])
    assert checker.main() == 0
    (tmp_path / name).write_text("LABEL = 'thrush'\n", encoding="utf-8")
    assert checker.main() == 1


@pytest.mark.skipif(os.name == "nt", reason="Opaque byte/control-character filenames are POSIX-only")
@pytest.mark.parametrize("raw_name", [b"opaque-\xff.py", b"line\rbreak.py"])
def test_git_paths_preserve_opaque_posix_bytes(tmp_path, monkeypatch, raw_name):
    name = os.fsdecode(raw_name)
    _run_in_fixture(tmp_path, (name, "VALUE = 1\n"))
    (tmp_path / name).write_text("VALUE = 2\n", encoding="utf-8")
    monkeypatch.setattr(checker, "ROOT", tmp_path)
    entries = checker._source_changes("HEAD")
    assert os.fsencode(entries[0][1]) == raw_name
    assert os.fsencode(entries[0][2]) == raw_name
    assert checker._git("show", f"HEAD:{name}") == "VALUE = 1\n"


def test_js_protocol_is_utf8_under_non_utf8_default(monkeypatch):
    """Exercise real pipes with a UTF-8 peer, without requiring Node in Python CI."""
    real_run = subprocess.run
    peer = (
        "import json, sys; "
        "source = sys.stdin.buffer.read().decode('utf-8'); "
        "sys.stdout.buffer.write(json.dumps({'source': source}, ensure_ascii=False).encode('utf-8'))"
    )
    def utf8_peer(_args, **kwargs):
        return real_run([sys.executable, "-c", peer], **kwargs)
    monkeypatch.setattr(subprocess, "_text_encoding", lambda: "cp1252")
    monkeypatch.setattr(subprocess, "run", utf8_peer)
    source = "const bird = '鳥'; // naïve"
    assert checker._js(source) == {"source": source}


@needs_node
def test_js_unicode_semantics_under_non_utf8_default(monkeypatch):
    monkeypatch.setattr(subprocess, "_text_encoding", lambda: "cp1252")
    # An ANSI input pipe makes both bytes decode to U+FFFD in Node's UTF-8 reader.
    assert checker.js_differs("const x = 'é';", "const x = 'ö';") is not None
    comments = checker._js("const bird = '鳥'; // naïve")["comments"]
    assert [c["text"] if isinstance(c, dict) else c for c in comments] == [" naïve"]


def test_git_windows_path_records_use_the_utf8_protocol(monkeypatch):
    from types import SimpleNamespace

    text = "M\0naïve.py\0"
    def wrong_filesystem_decoder(_data):
        pytest.fail("Git for Windows path records must be decoded as UTF-8")
    monkeypatch.setattr(checker, "os", SimpleNamespace(name="nt", fsdecode=wrong_filesystem_decoder))
    monkeypatch.setattr(subprocess, "run", lambda *_args, **_kwargs: SimpleNamespace(stdout=text.encode("utf-8")))
    assert checker._git("diff", "--name-status", "-z", "HEAD") == text


def test_git_source_text_matches_working_tree_utf8_newlines(tmp_path, monkeypatch):
    _run_in_fixture(tmp_path)
    data = "LABEL = 'café'\r\nVALUE = 1\r".encode()
    path = tmp_path / "lines.py"
    path.write_bytes(data)
    result = subprocess.run(
        ["git", "-C", str(tmp_path), "hash-object", "-w", "--stdin"],
        input=data, capture_output=True, check=True,
    )
    blob = result.stdout.decode("ascii").strip()
    monkeypatch.setattr(checker, "ROOT", tmp_path)
    assert checker._git("show", blob) == path.read_text(encoding="utf-8")
