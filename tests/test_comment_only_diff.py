"""Negative controls for the comment-only safety check, using inert sources."""
import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / 'scripts' / 'check_comment_only_diff.py'
spec = importlib.util.spec_from_file_location('comment_only', SCRIPT)
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


@pytest.mark.parametrize('before, after', [
    ('const x = /cats/i;', 'const x = /dogs/i;'),
    ('const x = /cats/i;', 'const x = /cats/g;'),
    ('function f() { return /* note\n*/ value; }', 'function f() { return /* note */ value; }'),
    ('let x = 1, y = 2; x\n++y;', 'let x = 1, y = 2; x++\ny;'),
    ('/** @import { Pet } from "./cats.js" */\nconst x = 1;',
     '/** @import { Pet } from "./dogs.js" */\nconst x = 1;'),
    ('/** @import { Pet } from "./my  cats.js" */\nconst x = 1;',
     '/** @import { Pet } from "./my cats.js" */\nconst x = 1;'),
    ('// @ts-expect-error\nfoo();', 'foo();'),
    ('// @ts-expect-error: expected failure\nfoo();', 'foo();'),
    ('// eslint-disable-next-line\nfoo();\nbar();', 'foo();\n// eslint-disable-next-line\nbar();'),
    ('// @ts-ignore\nfoo();\nbar();', 'foo();\n// @ts-ignore\nbar();'),
    ('/** @type {number} */\nlet x;\nlet y;', 'let x;\n/** @type {number} */\nlet y;'),
    ('// eslint-disable-next-line\nfoo();', '// eslint-disable-next-line\n// note\nfoo();'),
])
def test_js_code_or_tool_changes_fail(before, after):
    assert checker.js_differs(before, after)


@pytest.mark.parametrize('before, after', [
    ('try:\n    f()\nexcept Exception:  # broad: optional probe\n    pass\n',
     'try:\n    f()\nexcept Exception:  # broad:\n    pass\n'),
    ('x = 1  # noqa: F841\ny = 2\n', 'x = 1\ny = 2  # noqa: F841\n'),
    ('x = 1  # type: ignore\ny = 2\n', 'x = 1\ny = 2  # type: ignore\n'),
])
def test_python_tool_changes_fail(before, after):
    assert checker.python_differs(before, after)


@pytest.mark.parametrize('before, after', [
    ('/** @param {number} x old description */\nfunction f(x) {}',
     '/** @param {number} x clear description */\nfunction f(x) {}'),
    ('// old prose\nconst x = /cat/i;', '// clear prose\n// more prose\nconst x = /cat/i;'),
    ('function f() { return /* old\n*/ value; }', 'function f() { return /* clear\n*/ value; }'),
    ('// prose\n/** @type {number} */\nlet x;', '// better\n// extra\n/** @type {number} */\nlet x;'),
])
def test_js_comment_edits_pass(before, after):
    assert checker.js_differs(before, after) is None


def test_python_ordinary_comment_edits_pass():
    assert checker.python_differs('"""old"""\n# old\nx = 1  # noqa: F841\n',
                                  '"""old"""\n# clear\n# more\nx = 1  # noqa: F841\n') is None


@pytest.mark.parametrize('change', ['added', 'deleted', 'renamed', 'untracked', 'typechanged', 'modified', 'comments'])
def test_complete_git_scope(tmp_path, monkeypatch, capsys, change):
    def git(*args):
        return subprocess.run(['git', *args], cwd=tmp_path, check=True, capture_output=True, text=True)
    git('init')
    git('config', 'user.email', 'test@example.invalid')
    git('config', 'user.name', 'Test')
    source = tmp_path / 'source with spaces.py'
    source.write_text('x = 1\n')
    git('add', '.')
    git('commit', '-m', 'fixture')
    if change == 'added':
        (tmp_path / 'added.py').write_text('x = 2\n')
        git('add', '.')
    elif change == 'deleted':
        source.unlink()
    elif change == 'renamed':
        git('mv', source.name, 'renamed.js')
    elif change == 'untracked':
        (tmp_path / 'new\nsource.py').write_text('x = 2\n')
    elif change == 'typechanged':
        source.unlink()
        source.symlink_to('target.py')
    else:
        source.write_text('x = 2\n' if change == 'modified' else '# note\nx = 1\n')
    monkeypatch.setattr(checker, 'ROOT', tmp_path)
    monkeypatch.setattr(sys, 'argv', [str(SCRIPT), '--base', 'HEAD'])
    assert checker.main() == (0 if change == 'comments' else 1)
    if change != 'comments':
        assert 'source' in capsys.readouterr().err or change == 'added'


@pytest.mark.parametrize('before, after', [
    ('# ruff: noqa: E402\nx = 1\n', 'x = 1\n'),
    ('x = 1  # NOQA: F841\n', 'x = 1\n'),
    ('# coding: utf-8\nx = "café"\n', '# coding: latin-1\nx = "café"\n'),
    ('#!/usr/bin/env python3\nx = 1\n', '#!/usr/bin/env pypy3\nx = 1\n'),
    ('#!/usr/bin/env python3\nx = 1\n', 'x = 1\n'),
    ('"""old"""\nx = 1\n', '"""new"""\nx = 1\n'),
    ('class C:\n    """old"""\n', 'class C:\n    """new"""\n'),
    ('def f():\n    """old"""\n', 'def f():\n    """new"""\n'),
    ('def register(f):\n    registry[f.__doc__] = f\n    return f\n@register\ndef f():\n    """old"""\n',
     'def register(f):\n    registry[f.__doc__] = f\n    return f\n@register\ndef f():\n    """new"""\n'),
])
def test_runtime_python_metadata_changes_fail(before, after):
    assert checker.python_differs(before, after)


def test_javascript_shebang_changes_fail():
    assert checker.js_differs('#!/usr/bin/env node\nfoo();', '#!/usr/bin/env bun\nfoo();')
