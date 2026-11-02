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
        (tmp_path / 'new source.py').write_text('x = 2\n')
    elif change == 'typechanged':
        if sys.platform == 'win32':
            pytest.skip('POSIX symlink type change')
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


@pytest.mark.parametrize('suffix', ['.py', '.js', '.mjs', '.cjs'])
@pytest.mark.parametrize('change', ['comment_added', 'comment_deleted', 'same_language_rename'])
def test_comparable_file_changes_preserve_writer_behavior(tmp_path, monkeypatch, suffix, change):
    """Keep the resumed writer's explicit comparisons of both source sides."""
    def git(*args):
        return subprocess.run(['git', *args], cwd=tmp_path, check=True, capture_output=True, text=True)
    git('init')
    git('config', 'user.email', 'test@example.invalid')
    git('config', 'user.name', 'Test')
    source = tmp_path / ('source' + suffix)
    comment = '# ordinary prose\n' if suffix == '.py' else '// ordinary prose\n'
    source.write_text('x = 1\n' if change == 'same_language_rename' else comment)
    git('add', '.')
    git('commit', '-m', 'fixture')
    if change == 'comment_added':
        (tmp_path / ('added' + suffix)).write_text(comment)
        git('add', '.')
    elif change == 'comment_deleted':
        source.unlink()
    else:
        git('mv', source.name, 'renamed' + suffix)
    monkeypatch.setattr(checker, 'ROOT', tmp_path)
    # Node dependencies belong to the test checkout, not its inert Git fixture.
    original_js = checker._js
    def fixture_js(src, source_type=None):
        with monkeypatch.context() as context:
            context.setattr(checker, 'ROOT', SCRIPT.parents[1])
            return original_js(src, source_type)
    monkeypatch.setattr(checker, '_js', fixture_js)
    monkeypatch.setattr(sys, 'argv', [str(SCRIPT), '--base', 'HEAD'])
    assert checker.main() == 0


@pytest.mark.parametrize('old_name, new_name', [
    ('source.py', 'source.txt'), ('source.js', 'source.txt'),
    ('source.txt', 'source.py'), ('source.txt', 'source.js'),
    ('source.mjs', 'source.txt'), ('source.txt', 'source.mjs'),
    ('source.cjs', 'source.txt'), ('source.txt', 'source.cjs'),
    ('source.mjs', 'source.cjs'), ('source.cjs', 'source.mjs'),
    ('source.js', 'source.mjs'), ('source.mjs', 'source.js'),
])
def test_rename_across_source_types_fails(tmp_path, monkeypatch, old_name, new_name):
    def git(*args):
        return subprocess.run(['git', *args], cwd=tmp_path, check=True, capture_output=True, text=True)
    git('init')
    git('config', 'user.email', 'test@example.invalid')
    git('config', 'user.name', 'Test')
    (tmp_path / old_name).write_text('x = 1\n')
    git('add', '.')
    git('commit', '-m', 'fixture')
    git('mv', old_name, new_name)
    monkeypatch.setattr(checker, 'ROOT', tmp_path)
    monkeypatch.setattr(sys, 'argv', [str(SCRIPT), '--base', 'HEAD'])
    assert checker.main() == 1


@pytest.mark.skipif(sys.platform == 'win32', reason='POSIX filename characters')
@pytest.mark.parametrize('filename', ['source\nname.py', 'source\tname.py', 'source:name.py'])
def test_tracked_unusual_filenames_are_compared(tmp_path, monkeypatch, capsys, filename):
    def git(*args):
        return subprocess.run(['git', *args], cwd=tmp_path, check=True, capture_output=True, text=True)
    git('init')
    git('config', 'user.email', 'test@example.invalid')
    git('config', 'user.name', 'Test')
    source = tmp_path / filename
    source.write_text('x = 1\n')
    git('add', '.')
    git('commit', '-m', 'fixture')
    source.write_text('x = 2\n')
    monkeypatch.setattr(checker, 'ROOT', tmp_path)
    monkeypatch.setattr(sys, 'argv', [str(SCRIPT), '--base', 'HEAD'])
    assert checker.main() == 1
    assert 'FAIL ' + filename in capsys.readouterr().out


def test_js_checker_parses_first_party_scripts():
    """Adding syntax parsing must continue to accept the production JS corpus."""
    static = SCRIPT.parents[1] / 'vireo' / 'static'
    paths = [p for p in static.rglob('*.js')
             if 'vendor' not in p.relative_to(static).parts and not p.name.endswith('.min.js')]
    assert paths
    for path in paths:
        checker._js(path.read_text(encoding='utf-8'))


@pytest.mark.parametrize('before, after', [
    ('/* exported helper */\nfunction helper() {}', 'function helper() {}'),
    ('/* exported first */\nfunction helper() {}', '/* exported second */\nfunction helper() {}'),
])
def test_exported_lint_directive_changes_fail(before, after):
    assert checker.js_differs(before, after)


def test_tagged_template_raw_text_changes_fail_despite_equal_cooked_tokens():
    before, after = 'tag`\\x41`', 'tag`A`'
    old, new = checker._js(before), checker._js(after)
    assert old['tokens'] == new['tokens']
    assert old['syntax'] != new['syntax']
    assert checker.js_differs(before, after)


def test_exported_lint_directive_survives_ordinary_prose_edits():
    assert checker.js_differs(
        '/* exported helper */\nfunction helper() {} // old',
        '/* exported helper */\nfunction helper() {} // clear',
    ) is None


@pytest.mark.parametrize('suffix', ['.mjs', '.cjs'])
@pytest.mark.parametrize('change', ['modified', 'added', 'deleted', 'untracked', 'renamed', 'comments'])
def test_module_source_files_are_in_complete_git_scope(tmp_path, monkeypatch, suffix, change):
    def git(*args):
        return subprocess.run(['git', *args], cwd=tmp_path, check=True, capture_output=True, text=True)
    git('init')
    git('config', 'user.email', 'test@example.invalid')
    git('config', 'user.name', 'Test')
    code = 'export const value = 1;\n' if suffix == '.mjs' else 'module.exports = 1;\n'
    source = tmp_path / ('source' + suffix)
    source.write_text(code)
    git('add', '.')
    git('commit', '-m', 'fixture')
    if change == 'modified':
        source.write_text(code.replace('1', '2'))
    elif change in {'added', 'untracked'}:
        (tmp_path / ('new' + suffix)).write_text(code)
        if change == 'added':
            git('add', '.')
    elif change == 'deleted':
        source.unlink()
    elif change == 'renamed':
        git('mv', source.name, 'renamed' + suffix)
        (tmp_path / ('renamed' + suffix)).write_text(code.replace('1', '2'))
    else:
        source.write_text('// ordinary prose\n' + code)
    monkeypatch.setattr(checker, 'ROOT', tmp_path)
    original_js = checker._js
    def fixture_js(src, source_type=None):
        with monkeypatch.context() as context:
            context.setattr(checker, 'ROOT', SCRIPT.parents[1])
            return original_js(src, source_type)
    monkeypatch.setattr(checker, '_js', fixture_js)
    monkeypatch.setattr(sys, 'argv', [str(SCRIPT), '--base', 'HEAD'])
    assert checker.main() == (0 if change == 'comments' else 1)


def test_module_mode_accepts_top_level_await():
    assert checker.js_differs('await load();', '// note\nawait load();', 'module') is None


def test_commonjs_mode_handles_wrapper_return():
    assert checker.js_differs('return require("x");', '// note\nreturn require("x");', 'commonjs') is None
    assert checker.js_differs('return 1;', 'return 2;', 'commonjs')


@pytest.mark.parametrize('mode, source', [('module', 'return 1;'), ('commonjs', 'export const x = 1;')])
def test_invalid_module_syntax_fails_closed(mode, source):
    with pytest.raises(subprocess.CalledProcessError):
        checker.js_differs(source, source, mode)


def test_checker_parses_tracked_module_and_commonjs_corpus():
    files = [rel for rel in checker._git('ls-files', '-z', '--', '*.mjs', '*.cjs').split('\0')
             if rel and 'vendor' not in Path(rel).parts]
    assert files
    for rel in files:
        mode = 'module' if rel.endswith('.mjs') else 'commonjs'
        checker._js((SCRIPT.parents[1] / rel).read_text(encoding='utf-8'), mode)
