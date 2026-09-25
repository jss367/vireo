"""Tests for the PR test selector (scripts/select_tests.py) and its plugin.

Each test builds a throwaway git repo shaped like this one (``vireo/``,
``vireo/tests/``, ``tests/``), records a synthetic impact map with
``coverage.CoverageData`` and asks the selector what a follow-up commit
should run. The selector must only ever *narrow* a run, so the assertions
check both what is selected and what is deliberately left out.
"""

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest
from coverage import CoverageData

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import select_tests  # noqa: E402

pytestmark = pytest.mark.skipif(os.name == "nt" and not os.environ.get("CI"), reason="git fixtures assume POSIX shell")


# --------------------------------------------------------------------------
# fixtures
# --------------------------------------------------------------------------

APP_SRC = textwrap.dedent(
    '''
    import os

    LIMIT = 3


    def alpha(x):
        return x + 1


    @staticmethod
    def beta(y):
        if y:
            return "b"
        return "c"


    def render_browse():
        return "browse.html"
    '''
).lstrip()


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=repo, check=True, capture_output=True, text=True
    ).stdout


def _write(repo: Path, rel: str, text: str) -> None:
    path = repo / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _commit(repo: Path, message: str) -> str:
    _git(repo, "add", "-A")
    _git(repo, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", message)
    return _git(repo, "rev-parse", "HEAD").strip()


@pytest.fixture
def repo(tmp_path):
    """A committed repo plus an impact map that mirrors APP_SRC line by line."""
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _write(repo, "vireo/app.py", APP_SRC)
    _write(repo, "vireo/templates/browse.html", "{% include '_navbar.html' %}\n")
    _write(repo, "vireo/templates/_navbar.html", "<nav><a href='/settings'>settings.html</a></nav>\n")
    _write(repo, "vireo/templates/settings.html", "{% extends '_base.html' %}\n")
    _write(repo, "vireo/tests/test_comments.py", "# see app.py for the routes\ndef test_noop(): pass\n")
    _write(repo, "vireo/tests/contracts/routes.txt", "GET /\n")
    _write(repo, "vireo/tests/test_route_contract.py", "SNAPSHOT = 'contracts/routes.txt'\ndef test_routes(): pass\n")
    _write(repo, "vireo/static/js/browse.js", "// js\n")
    _write(repo, "vireo/tests/conftest.py", "")
    _write(repo, "vireo/tests/test_app.py", "def test_alpha(): pass\ndef test_beta(): pass\n")
    _write(repo, "vireo/tests/test_pages.py", "def test_browse(): pass\n")
    _write(repo, "tests/test_release.py", "SCRIPT = 'release.sh'\n")
    _write(repo, "scripts/release.sh", "echo hi\n")
    _write(repo, "docs/notes.md", "notes\n")
    base = _commit(repo, "base")

    # Line numbers in APP_SRC: LIMIT=3 -> line 3; alpha body lines 6-7;
    # beta decorator 10, def 11, body 12-14; render_browse 17-18.
    map_dir = repo / select_tests.MAP_DIR
    map_dir.mkdir()
    data = CoverageData(basename=str(map_dir / select_tests.MAP_DB))
    app = "vireo/app.py"
    data.set_context("")
    data.add_lines({app: [1, 3, 6, 10, 11, 17]})
    data.set_context("vireo/tests/test_app.py::test_alpha|run")
    data.add_lines({app: [7]})
    data.set_context("vireo/tests/test_app.py::test_beta|run")
    data.add_lines({app: [12, 13]})
    data.set_context("vireo/tests/test_app.py::test_beta|setup")
    data.add_lines({"vireo/tests/conftest.py": [1]})
    data.set_context("vireo/tests/test_pages.py::test_browse|run")
    data.add_lines({app: [18]})
    data.write()
    (map_dir / select_tests.MAP_META).write_text(json.dumps({"sha": base, "root": str(repo)}))
    return repo


def _select(repo: Path, **kwargs):
    impact, meta, problem = select_tests.load_map(repo / select_tests.MAP_DIR)
    assert problem is None, problem
    return select_tests.select(meta["sha"], "HEAD", impact, cwd=repo, **kwargs)


# --------------------------------------------------------------------------
# selection rules
# --------------------------------------------------------------------------


def test_change_inside_function_selects_only_its_tests(repo):
    _write(repo, "vireo/app.py", APP_SRC.replace("return x + 1", "return x + 2"))
    _commit(repo, "edit alpha")

    sel = _select(repo)

    assert sel.mode == "subset"
    assert sel.ids == {"vireo/tests/test_app.py::test_alpha"}
    # Tests that executed unrelated functions of ``app.py`` (test_beta,
    # test_browse) stay out: the map narrows a body-line edit to the
    # tests that ran that body. ``test_comments.py`` — a test file that
    # merely mentions ``app.py`` in a comment — is added separately via
    # the source-file mention fallback; that is covered in a dedicated
    # test below, so keep this assertion focused on the map lookup.
    assert not sel.ids & {
        "vireo/tests/test_app.py::test_beta",
        "vireo/tests/test_pages.py::test_browse",
    }


def test_insertion_inside_function_uses_surrounding_lines(repo):
    _write(repo, "vireo/app.py", APP_SRC.replace('        return "b"\n', '        print("x")\n        return "b"\n'))
    _commit(repo, "insert into beta")

    sel = _select(repo)

    assert sel.ids == {"vireo/tests/test_app.py::test_beta"}


def test_decorator_line_belongs_to_its_function(repo):
    _write(repo, "vireo/app.py", APP_SRC.replace("@staticmethod\n", "@classmethod\n"))
    _commit(repo, "swap decorator")

    sel = _select(repo)

    # test_beta ran beta's body on main and is picked from the map.
    assert sel.ids == {"vireo/tests/test_app.py::test_beta"}
    # A signature/decorator change also selects test files that reference
    # ``app.py`` by name — declaration-inspecting tests never execute the
    # body but must still run when the header changes.
    assert "vireo/tests/test_comments.py" in sel.files


def test_module_level_change_falls_back_to_full_suite(repo):
    _write(repo, "vireo/app.py", APP_SRC.replace("LIMIT = 3", "LIMIT = 4"))
    _commit(repo, "bump constant")

    sel = _select(repo)

    assert sel.mode == "full"
    assert "module-level" in sel.full_reason


def test_source_edit_selects_tests_that_mention_the_source_file(repo):
    # Body-only edits can still trip source-parsing contract tests that
    # read the file via ``ast.parse`` / ``Path.read_text`` — for example,
    # ``test_every_prediction_decision_route_locks`` in this repo parses
    # every route body without executing it. The selector runs the
    # basename-mention fallback for every source-file change so those
    # tests are picked up whether the edit lands on a header line or in
    # the function body. False positives from bare comment mentions are
    # the accepted trade-off: a missed structural regression is worse
    # than one extra test file running.
    _write(repo, "vireo/app.py", APP_SRC.replace("return x + 1", "return x + 2"))
    _commit(repo, "edit alpha")

    sel = _select(repo)

    assert sel.ids == {"vireo/tests/test_app.py::test_alpha"}
    assert "vireo/tests/test_comments.py" in sel.files


def test_module_level_constant_change_falls_back_to_full_suite(tmp_path):
    """A test that only reads a module-level constant executes no line of
    the source file under a test context — the assignment runs at import
    time — and is missing from ``tests_for_lines(path, None)``. Import-time
    values can also flow through production consumers, so no grep-based
    subset is provably complete; module-level changes run the full suite.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _write(
        repo,
        "vireo/image_edits.py",
        "EDIT_MATH_VERSION = 3\n\n\ndef bump():\n    return EDIT_MATH_VERSION + 1\n",
    )
    _write(
        repo,
        "vireo/tests/test_image_edits.py",
        "from vireo.image_edits import EDIT_MATH_VERSION\n"
        "def test_matches_client():\n"
        "    assert EDIT_MATH_VERSION == 3\n",
    )
    _write(repo, "vireo/tests/test_unrelated.py", "def test_x(): pass\n")
    base = _commit(repo, "base")

    map_dir = repo / select_tests.MAP_DIR
    map_dir.mkdir()
    data = CoverageData(basename=str(map_dir / select_tests.MAP_DB))
    # test_matches_client never touches a line of image_edits.py under a
    # test context — the module-level assignment runs at import time,
    # before any per-test context is active, and attribute access
    # doesn't count as a line hit. Record only the unrelated test to
    # simulate that gap.
    data.set_context("vireo/tests/test_unrelated.py::test_x|run")
    data.add_lines({"vireo/tests/test_unrelated.py": [1]})
    data.write()
    (map_dir / select_tests.MAP_META).write_text(json.dumps({"sha": base, "root": str(repo)}))

    _write(
        repo,
        "vireo/image_edits.py",
        "EDIT_MATH_VERSION = 4\n\n\ndef bump():\n    return EDIT_MATH_VERSION + 1\n",
    )
    _commit(repo, "bump math version")

    sel = _select(repo)

    assert sel.mode == "full"
    assert "module-level" in sel.full_reason


def test_module_level_constant_change_covers_transitive_production_consumers(tmp_path):
    """A defining-module constant can be copied into another module at import
    time, then affect tests that mention and execute only that consumer. The
    impact map cannot represent that collection-time dependency, so the safe
    fallback is the full suite.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _write(repo, "vireo/repository.py", "VALID_COLORS = {'red'}\n")
    _write(
        repo,
        "vireo/web.py",
        "from vireo.repository import VALID_COLORS\n"
        "def accepts(color):\n"
        "    return color in VALID_COLORS\n",
    )
    _write(
        repo,
        "vireo/tests/test_web.py",
        "from vireo.web import accepts\n"
        "def test_orange_is_invalid():\n"
        "    assert not accepts('orange')\n",
    )
    base = _commit(repo, "base")

    map_dir = repo / select_tests.MAP_DIR
    map_dir.mkdir()
    data = CoverageData(basename=str(map_dir / select_tests.MAP_DB))
    data.set_context("vireo/tests/test_web.py::test_orange_is_invalid|run")
    data.add_lines({"vireo/web.py": [3]})
    data.write()
    (map_dir / select_tests.MAP_META).write_text(json.dumps({"sha": base, "root": str(repo)}))

    _write(repo, "vireo/repository.py", "VALID_COLORS = {'red', 'orange'}\n")
    _commit(repo, "allow orange")

    sel = _select(repo)

    assert sel.mode == "full"
    assert "vireo/repository.py" in sel.full_reason


def test_function_body_edit_selects_tests_that_inspect_the_symbol(tmp_path):
    """A test that inspects a declaration by imported symbol
    (``inspect.signature(fn)`` / ``inspect.getsource(fn)``) executes no
    line of the source file and never mentions its basename — the
    ``vireo/tests/test_thumbnails.py::test_thumbnail_raw_fallbacks_honor_an_explicit_cache_name``
    case that shipped this fix. The selector greps tests for each
    touched function or class name so those tests are still selected
    when the declaration changes.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _write(
        repo,
        "vireo/thumbnails.py",
        "def _retry_thumbnail_with_companion(cache_name):\n"
        "    return cache_name\n",
    )
    _write(
        repo,
        "vireo/tests/test_thumbnails.py",
        "import inspect\n"
        "import thumbnails as thumbnails_module\n"
        "def test_signature():\n"
        "    params = inspect.signature(thumbnails_module._retry_thumbnail_with_companion).parameters\n"
        "    assert 'cache_name' in params\n",
    )
    _write(repo, "vireo/tests/test_unrelated.py", "def test_x(): pass\n")
    base = _commit(repo, "base")

    map_dir = repo / select_tests.MAP_DIR
    map_dir.mkdir()
    data = CoverageData(basename=str(map_dir / select_tests.MAP_DB))
    # test_signature executes no line of thumbnails.py under a test
    # context (it only reads the signature), so simulate that gap.
    data.set_context("vireo/tests/test_unrelated.py::test_x|run")
    data.add_lines({"vireo/tests/test_unrelated.py": [1]})
    data.write()
    (map_dir / select_tests.MAP_META).write_text(json.dumps({"sha": base, "root": str(repo)}))

    _write(
        repo,
        "vireo/thumbnails.py",
        "def _retry_thumbnail_with_companion(cache_name):\n"
        "    return cache_name + '.jpg'\n",
    )
    _commit(repo, "extend companion helper")

    sel = _select(repo)

    # Greps tests for ``_retry_thumbnail_with_companion`` and finds
    # test_thumbnails.py despite it never mentioning ``thumbnails.py``.
    assert "vireo/tests/test_thumbnails.py" in sel.files
    assert "vireo/tests/test_unrelated.py" not in sel.files


def test_nested_function_edit_prefers_the_inner_symbol_name(tmp_path):
    """For nested functions, the inner name is what tests grep for.

    Grepping the outer ``create_app`` on every route-body change would
    add every test that builds the app; the inner route name narrows to
    tests that actually reference the specific declaration.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _write(
        repo,
        "vireo/app.py",
        "def create_app():\n"
        "    def api_photos_list():\n"
        "        return []\n"
        "    return api_photos_list\n",
    )
    _write(
        repo,
        "vireo/tests/test_api_photos_list.py",
        "# targets api_photos_list specifically\n"
        "def test_it(): pass\n",
    )
    _write(
        repo,
        "vireo/tests/test_builds_app.py",
        "# builds create_app but doesn't care about routes\n"
        "def test_it(): pass\n",
    )
    base = _commit(repo, "base")

    map_dir = repo / select_tests.MAP_DIR
    map_dir.mkdir()
    data = CoverageData(basename=str(map_dir / select_tests.MAP_DB))
    data.set_context("vireo/tests/test_builds_app.py::test_it|run")
    data.add_lines({"vireo/tests/test_builds_app.py": [1]})
    data.write()
    (map_dir / select_tests.MAP_META).write_text(json.dumps({"sha": base, "root": str(repo)}))

    _write(
        repo,
        "vireo/app.py",
        "def create_app():\n"
        "    def api_photos_list():\n"
        "        return [1]\n"
        "    return api_photos_list\n",
    )
    _commit(repo, "edit inner route body")

    sel = _select(repo)

    assert "vireo/tests/test_api_photos_list.py" in sel.files
    assert "vireo/tests/test_builds_app.py" not in sel.files


def test_comment_only_change_selects_nothing_from_map(repo):
    _write(repo, "vireo/app.py", APP_SRC.replace("    return x + 1", "    # tweak\n    return x + 1"))
    _commit(repo, "comment")
    # Insertion of a comment line still attributes to the enclosing function
    # (its neighbours are code), but *modifying* a comment does not:
    sel_insert = _select(repo)
    assert sel_insert.ids == {"vireo/tests/test_app.py::test_alpha"}

    _write(repo, "vireo/app.py", APP_SRC.replace("    return x + 1", "    # tweaked\n    return x + 1"))
    _commit(repo, "reword comment")
    meta_path = repo / select_tests.MAP_DIR / select_tests.MAP_META
    meta = json.loads(meta_path.read_text())
    previous = subprocess.run(["git", "rev-parse", "HEAD^"], cwd=repo, capture_output=True, text=True).stdout.strip()
    impact, _, _ = select_tests.load_map(repo / select_tests.MAP_DIR)
    # Compare against the previous commit so the diff is the comment reword alone.
    sel = select_tests.select(previous, "HEAD", impact, cwd=repo)
    assert sel.mode == "none", sel.notes
    assert meta["sha"]  # map still intact


def test_replacing_comment_with_code_selects_enclosing_function(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _write(
        repo,
        "vireo/app.py",
        "def alpha():\n"
        "    # side_effect()\n"
        "    return 1\n",
    )
    _write(repo, "vireo/tests/test_app.py", "def test_alpha(): pass\n")
    base = _commit(repo, "base")

    map_dir = repo / select_tests.MAP_DIR
    map_dir.mkdir()
    data = CoverageData(basename=str(map_dir / select_tests.MAP_DB))
    data.set_context("vireo/tests/test_app.py::test_alpha|run")
    data.add_lines({"vireo/app.py": [3]})
    data.write()
    (map_dir / select_tests.MAP_META).write_text(json.dumps({"sha": base, "root": str(repo)}))

    _write(
        repo,
        "vireo/app.py",
        "def alpha():\n"
        "    side_effect()\n"
        "    return 1\n",
    )
    _commit(repo, "enable side effect")

    sel = _select(repo)

    assert sel.mode == "subset"
    assert sel.ids == {"vireo/tests/test_app.py::test_alpha"}


def test_ambiguous_insertion_between_nested_functions_falls_back_to_full_suite(repo):
    nested = textwrap.dedent(
        """
        def create_app():
            def route_a():
                return "a"

            def route_b():
                return "b"

            return route_a, route_b
        """
    ).lstrip()
    _write(repo, "vireo/nested.py", nested)
    _commit(repo, "nested base")
    base = subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True).stdout.strip()
    map_dir = repo / select_tests.MAP_DIR
    data = CoverageData(basename=str(map_dir / select_tests.MAP_DB))
    data.set_context("vireo/tests/test_nested.py::test_create_app|run")
    data.add_lines({"vireo/nested.py": [1, 2, 5, 8]})
    data.set_context("vireo/tests/test_nested.py::test_route_a|run")
    data.add_lines({"vireo/nested.py": [3]})
    data.set_context("vireo/tests/test_nested.py::test_route_b|run")
    data.add_lines({"vireo/nested.py": [6]})
    data.write()
    (map_dir / select_tests.MAP_META).write_text(json.dumps({"sha": base, "root": str(repo)}))

    # Insert a new route between route_a and route_b: the old-side
    # neighbours are the blank line 4 (create_app only) and line 5
    # (route_b's def, also inside create_app). The two neighbours resolve
    # to different function spans, so the insertion is treated as
    # ambiguous — the new lines could just as easily be import-time
    # registration — and the selector falls back to the full suite rather
    # than guessing an enclosing function.
    _write(repo, "vireo/nested.py", nested.replace("    def route_b", "    def route_new():\n        return 'n'\n\n    def route_b"))
    _commit(repo, "add route")

    sel = _select(repo)

    assert sel.mode == "full"
    assert "structurally ambiguous" in sel.full_reason


def test_insertion_at_eof_falls_back_to_full_suite(repo):
    # Insert a module-level statement after the last body line of
    # ``render_browse``. The first neighbour is inside render_browse and
    # the second is past EOF (no enclosing function), so the insertion
    # cannot be safely attributed to render_browse and selection falls back
    # to the full suite.
    _write(repo, "vireo/app.py", APP_SRC.rstrip() + "\n\nEXTRA = 1\n")
    _commit(repo, "append module-level constant")

    sel = _select(repo)

    assert sel.mode == "full"
    assert "structurally ambiguous" in sel.full_reason


def test_test_asset_runs_the_tests_that_name_it(repo):
    _write(repo, "vireo/tests/contracts/routes.txt", "GET /\nGET /new\n")
    _commit(repo, "update contract")

    sel = _select(repo)

    assert sel.mode == "subset"
    assert sel.files == {"vireo/tests/test_route_contract.py"}


def test_template_hyperlinks_are_not_followed(repo):
    # _navbar.html mentions settings.html in an href, and browse.html
    # includes _navbar.html. Editing settings.html must not select browse.
    _write(repo, "vireo/templates/settings.html", "{% extends '_base.html' %}<p>x</p>\n")
    _commit(repo, "edit settings")

    sel = _select(repo)

    assert sel.mode == "none", sel.notes


def test_deleted_source_file_falls_back_to_full_suite(repo):
    (repo / "vireo/app.py").unlink()
    _commit(repo, "delete app")

    sel = _select(repo)

    assert sel.mode == "full"
    assert "structurally ambiguous" in sel.full_reason


def test_changed_test_file_runs_whole_file(repo):
    _write(repo, "vireo/tests/test_app.py", "def test_alpha(): pass\ndef test_beta(): pass\ndef test_new(): pass\n")
    _commit(repo, "add test")

    sel = _select(repo)

    assert sel.files == {"vireo/tests/test_app.py"}
    assert sel.ids == set()


def test_added_test_file_runs_whole_file_and_added_source_selects_nothing_else(repo):
    _write(repo, "vireo/newmod.py", "def f():\n    return 1\n")
    _write(repo, "vireo/tests/test_newmod.py", "def test_f(): pass\n")
    _commit(repo, "new module + test")

    sel = _select(repo)

    assert sel.files == {"vireo/tests/test_newmod.py"}
    assert sel.ids == set()


def test_modified_source_module_selects_source_scanning_contract_tests(tmp_path):
    """Source-scanning contracts (``glob("*.py")``) read the file's bytes
    without executing any of its lines, so a modification that swaps a
    cached factory for a prohibited direct constructor is invisible to
    them via coverage — the map's line hits on the edited function's
    body don't reach a test that never runs any line of the file. The
    selector runs the same fallback for modifications as for additions.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _write(repo, "vireo/app.py", "def alpha():\n    return 1\n")
    _write(repo, "vireo/tests/test_app.py", "def test_alpha(): pass\n")
    _write(
        repo,
        "vireo/tests/test_construction_contract.py",
        "from pathlib import Path\n"
        "def test_construction():\n"
        "    for path in Path('vireo').glob('*.py'):\n"
        "        assert path.is_file()\n",
    )
    _write(repo, "vireo/tests/test_unrelated.py", "def test_x(): pass\n")
    base = _commit(repo, "base")

    map_dir = repo / select_tests.MAP_DIR
    map_dir.mkdir()
    data = CoverageData(basename=str(map_dir / select_tests.MAP_DB))
    data.set_context("vireo/tests/test_app.py::test_alpha|run")
    data.add_lines({"vireo/app.py": [1, 2]})
    data.set_context("vireo/tests/test_construction_contract.py::test_construction|run")
    data.add_lines({"vireo/tests/test_construction_contract.py": [1, 2, 3, 4]})
    data.set_context("vireo/tests/test_unrelated.py::test_x|run")
    data.add_lines({"vireo/tests/test_unrelated.py": [1]})
    data.write()
    (map_dir / select_tests.MAP_META).write_text(json.dumps({"sha": base, "root": str(repo)}))

    _write(repo, "vireo/app.py", "def alpha():\n    return 2\n")
    _commit(repo, "modify alpha")

    sel = _select(repo)

    assert sel.mode == "subset"
    assert "vireo/tests/test_construction_contract.py" in sel.files
    # test_alpha still runs via the coverage-based path.
    assert "vireo/tests/test_app.py::test_alpha" in sel.ids
    assert "vireo/tests/test_unrelated.py" not in sel.files


def test_added_source_module_selects_source_scanning_contract_tests(tmp_path):
    """A newly added production module has no history in the map, so tests
    that only *execute* it are correctly left out. But tests that iterate
    the source tree with ``glob("*.py")`` / ``rglob("*.py")`` — AST-based
    contract audits like ``test_classifier_construction_contract`` — read
    the new file's source without ever running its lines and must still
    catch a prohibited direct constructor in it. Those files are picked
    up by grepping the base for the scan pattern.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _write(repo, "vireo/app.py", "def alpha():\n    return 1\n")
    _write(repo, "vireo/tests/test_app.py", "def test_alpha(): pass\n")
    _write(
        repo,
        "vireo/tests/test_construction_contract.py",
        "from pathlib import Path\n"
        "def test_construction():\n"
        "    for path in Path('vireo').glob('*.py'):\n"
        "        assert path.is_file()\n",
    )
    _write(
        repo,
        "vireo/tests/test_deep_contract.py",
        "from pathlib import Path\n"
        "def test_deep():\n"
        "    for path in Path('vireo').rglob('*.py'):\n"
        "        assert path.is_file()\n",
    )
    _write(repo, "vireo/tests/test_unrelated.py", "def test_x(): pass\n")
    base = _commit(repo, "base")

    map_dir = repo / select_tests.MAP_DIR
    map_dir.mkdir()
    data = CoverageData(basename=str(map_dir / select_tests.MAP_DB))
    data.set_context("vireo/tests/test_app.py::test_alpha|run")
    data.add_lines({"vireo/app.py": [1, 2]})
    data.set_context("vireo/tests/test_construction_contract.py::test_construction|run")
    data.add_lines({"vireo/tests/test_construction_contract.py": [1, 2, 3, 4]})
    data.set_context("vireo/tests/test_deep_contract.py::test_deep|run")
    data.add_lines({"vireo/tests/test_deep_contract.py": [1, 2, 3, 4]})
    data.set_context("vireo/tests/test_unrelated.py::test_x|run")
    data.add_lines({"vireo/tests/test_unrelated.py": [1]})
    data.write()
    (map_dir / select_tests.MAP_META).write_text(json.dumps({"sha": base, "root": str(repo)}))

    _write(repo, "vireo/newmod.py", "def f():\n    return 1\n")
    _commit(repo, "add new production module")

    sel = _select(repo)

    # Both source-scanning contract tests run whole; the coverage-executed
    # tests don't (the added file has no map history and the unrelated
    # test never scans the source tree).
    assert sel.mode == "subset"
    assert sel.files == {
        "vireo/tests/test_construction_contract.py",
        "vireo/tests/test_deep_contract.py",
    }
    assert "vireo/tests/test_unrelated.py" not in sel.files
    assert sel.ids == set()


def test_deleted_test_file_is_not_selected(repo):
    (repo / "vireo/tests/test_pages.py").unlink()
    _commit(repo, "drop test file")

    sel = _select(repo)

    assert sel.mode == "none"
    assert sel.files == set() and sel.ids == set()


def test_template_change_selects_tests_that_rendered_it(repo):
    _write(repo, "vireo/templates/browse.html", "{% include '_navbar.html' %}<p>new</p>\n")
    _commit(repo, "edit template")

    sel = _select(repo)

    assert sel.ids == {"vireo/tests/test_pages.py::test_browse"}


def test_included_partial_is_chased_through_including_templates(repo):
    _write(repo, "vireo/templates/_navbar.html", "<nav>changed</nav>\n")
    _commit(repo, "edit navbar")

    sel = _select(repo)

    # _navbar.html -> browse.html -> render_browse() -> test_browse
    assert sel.ids == {"vireo/tests/test_pages.py::test_browse"}


def test_unreferenced_asset_selects_nothing(repo):
    _write(repo, "vireo/static/js/browse.js", "// changed\n")
    _commit(repo, "edit js nobody references")

    sel = _select(repo)

    assert sel.mode == "none"


def test_asset_mentioned_by_a_test_file_runs_that_file(repo):
    _write(repo, "scripts/release.sh", "echo changed\n")
    _commit(repo, "edit release script")

    sel = _select(repo)

    assert sel.files == {"tests/test_release.py"}


def test_docs_only_change_selects_nothing(repo):
    _write(repo, "docs/notes.md", "changed\n")
    _commit(repo, "docs")

    sel = _select(repo)

    assert sel.mode == "none"


def test_e2e_changes_are_ignored_for_unit_selection(repo):
    _write(repo, "tests/e2e/test_x.py", "def test_x(): pass\n")
    _commit(repo, "e2e")

    sel = _select(repo)

    assert sel.mode == "none"


@pytest.mark.parametrize(
    "path",
    [
        "vireo/tests/conftest.py",
        "tests/conftest.py",
        "conftest.py",
        "pyproject.toml",
        ".github/workflows/test.yml",
        ".github/workflows/test-main.yml",
        ".github/actions/setup-python-tests/action.yml",
        "vireo/data/taxonomy.json",
        "vireo/tests/wait.py",
        "vireo/tests/fixtures/sample.jpg",
        "scripts/select_tests.py",
    ],
)
def test_harness_changes_force_the_full_suite(repo, path):
    _write(repo, path, "changed\n")
    _commit(repo, f"touch {path}")

    sel = _select(repo)

    assert sel.mode == "full"
    assert path in sel.full_reason


def test_missing_map_forces_full_suite(tmp_path):
    impact, meta, problem = select_tests.load_map(tmp_path / "nowhere")

    assert impact is None
    assert "no map" in problem

    sel = select_tests.select("HEAD", "HEAD", None, cwd=tmp_path)
    assert sel.mode == "full"


def test_map_without_contexts_is_rejected(tmp_path):
    map_dir = tmp_path / select_tests.MAP_DIR
    map_dir.mkdir()
    data = CoverageData(basename=str(map_dir / select_tests.MAP_DB))
    data.add_lines({"vireo/app.py": [1]})
    data.write()
    (map_dir / select_tests.MAP_META).write_text(json.dumps({"sha": "abc"}))

    impact, _, problem = select_tests.load_map(map_dir)

    assert impact is None
    assert "no per-test contexts" in problem


def test_selection_file_lists_files_then_ids_and_drops_ids_covered_by_files(tmp_path):
    sel = select_tests.Selection()
    sel.files.add("vireo/tests/test_app.py")
    sel.ids.update({"vireo/tests/test_app.py::test_alpha", "vireo/tests/test_db.py::test_x"})
    out = tmp_path / "sel.txt"

    sel.write(out, "base", "head")

    body = [line for line in out.read_text().splitlines() if not line.startswith("#")]
    assert body == ["vireo/tests/test_app.py", "vireo/tests/test_db.py::test_x"]


# --------------------------------------------------------------------------
# diff / ast helpers
# --------------------------------------------------------------------------


def test_expand_to_functions_picks_innermost_span():
    spans = select_tests.function_spans(
        textwrap.dedent(
            """
            def outer():
                def inner():
                    return 1
                return inner
            """
        )
    )
    # Body lines only: a def line runs in the *enclosing* scope's context.
    assert select_tests.expand_to_functions({4}, spans) == {4}
    assert select_tests.expand_to_functions({3}, spans) == {4}
    assert select_tests.expand_to_functions({2}, spans) == {3, 4, 5}
    assert select_tests.expand_to_functions({1}, spans) is None


def test_function_spans_survive_syntax_errors():
    assert select_tests.function_spans("def broken(:\n") == []


# --------------------------------------------------------------------------
# build-map validation
# --------------------------------------------------------------------------


def _junit(path: Path, ran: int, skipped: int = 0) -> Path:
    cases = "".join(f'<testcase name="t{i}" time="0.1"/>' for i in range(ran))
    cases += "".join(f'<testcase name="s{i}"><skipped/></testcase>' for i in range(skipped))
    path.write_text(f'<testsuites><testsuite name="p">{cases}</testsuite></testsuites>')
    return path


def test_build_map_rejects_partial_coverage(repo, tmp_path):
    coverage_file = repo / select_tests.MAP_DIR / select_tests.MAP_DB
    junit = _junit(tmp_path / "junit.xml", ran=10, skipped=2)

    with pytest.raises(SystemExit, match="looks partial"):
        select_tests.build_map(coverage_file, tmp_path / "out", "sha", cwd=repo, junit_xml=junit)


def test_build_map_accepts_complete_coverage(repo, tmp_path):
    coverage_file = repo / select_tests.MAP_DIR / select_tests.MAP_DB
    junit = _junit(tmp_path / "junit.xml", ran=3, skipped=5)

    meta = select_tests.build_map(coverage_file, tmp_path / "out", "sha123", cwd=repo, junit_xml=junit)

    assert meta["sha"] == "sha123"
    assert meta["contexts"] == 3
    assert meta["executed_tests"] == 3
    written = json.loads((tmp_path / "out" / select_tests.MAP_META).read_text())
    assert written["sha"] == "sha123"


def test_fetch_map_failure_preserves_existing_map(tmp_path, monkeypatch):
    map_dir = tmp_path / select_tests.MAP_DIR
    map_dir.mkdir()
    (map_dir / select_tests.MAP_META).write_text('{"sha": "old"}\n')
    (map_dir / select_tests.MAP_DB).write_text("old database")

    def fake_run(args, **kwargs):
        if args[1:4] == ["run", "list", "--workflow"]:
            return subprocess.CompletedProcess(
                args,
                0,
                stdout='[{"databaseId": 123, "conclusion": "success", "headSha": "new"}]',
                stderr="",
            )
        return subprocess.CompletedProcess(args, 1, stdout="", stderr="download failed")

    monkeypatch.setattr(select_tests.shutil, "which", lambda _: "/usr/bin/gh")
    monkeypatch.setattr(select_tests.subprocess, "run", fake_run)

    with pytest.raises(SystemExit, match="no downloadable"):
        select_tests.fetch_map(map_dir, cwd=tmp_path)

    assert (map_dir / select_tests.MAP_META).read_text() == '{"sha": "old"}\n'
    assert (map_dir / select_tests.MAP_DB).read_text() == "old database"


def test_fetch_map_replaces_existing_map_only_after_complete_download(tmp_path, monkeypatch):
    map_dir = tmp_path / select_tests.MAP_DIR
    map_dir.mkdir()
    (map_dir / select_tests.MAP_META).write_text('{"sha": "old"}\n')
    (map_dir / select_tests.MAP_DB).write_text("old database")
    (map_dir / "old-only.txt").write_text("remove me")

    def fake_run(args, **kwargs):
        if args[1:4] == ["run", "list", "--workflow"]:
            return subprocess.CompletedProcess(
                args,
                0,
                stdout='[{"databaseId": 123, "conclusion": "success", "headSha": "new"}]',
                stderr="",
            )
        destination = Path(args[args.index("--dir") + 1])
        destination.mkdir(parents=True)
        (destination / select_tests.MAP_META).write_text('{"sha": "new", "contexts": 42}\n')
        (destination / select_tests.MAP_DB).write_text("new database")
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

    monkeypatch.setattr(select_tests.shutil, "which", lambda _: "/usr/bin/gh")
    monkeypatch.setattr(select_tests.subprocess, "run", fake_run)

    meta = select_tests.fetch_map(map_dir, cwd=tmp_path)

    assert meta["sha"] == "new"
    assert (map_dir / select_tests.MAP_DB).read_text() == "new database"
    assert not (map_dir / "old-only.txt").exists()


# --------------------------------------------------------------------------
# CLI + pytest plugin end to end
# --------------------------------------------------------------------------


def test_cli_writes_selection_and_prints_mode(repo):
    _write(repo, "vireo/app.py", APP_SRC.replace("return x + 1", "return x + 3"))
    _commit(repo, "edit alpha")

    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts/select_tests.py"), "--repo", str(repo), "--explain"],
        capture_output=True,
        text=True,
        check=True,
    )

    assert result.stdout.strip() == "subset"
    # The map lookup contributes ``test_alpha``; the basename-mention
    # fallback contributes ``test_comments.py`` as a whole file because
    # it references ``app.py``.
    assert "selected: 1 whole files + 1 individual tests" in result.stderr
    selection = (repo / select_tests.MAP_DIR / select_tests.SELECTION_FILE).read_text()
    assert "vireo/tests/test_app.py::test_alpha" in selection
    assert "vireo/tests/test_comments.py" in selection


def test_cli_falls_back_to_full_when_map_commit_is_unreachable(repo):
    meta_path = repo / select_tests.MAP_DIR / select_tests.MAP_META
    meta_path.write_text(json.dumps({"sha": "0" * 40, "root": str(repo)}))

    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts/select_tests.py"), "--repo", str(repo)],
        capture_output=True,
        text=True,
        check=True,
    )

    assert result.stdout.strip() == "full"
    assert "not reachable" in result.stderr


def test_cli_default_subcommand_accepts_equals_style_global_options(repo):
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/select_tests.py"),
            f"--repo={repo}",
            f"--map-dir={select_tests.MAP_DIR}",
            "--explain",
        ],
        capture_output=True,
        text=True,
        check=True,
    )

    assert result.stdout.strip() == "none"


def test_plugin_runs_only_listed_tests_and_ignores_missing_ids(tmp_path):
    tests_dir = tmp_path / "vireo" / "tests"
    tests_dir.mkdir(parents=True)
    (tests_dir / "test_one.py").write_text("def test_a(): pass\ndef test_b(): pass\n")
    (tests_dir / "test_two.py").write_text("def test_c(): pass\n")
    (tests_dir / "test_three.py").write_text("def test_d(): pass\n")
    selection = tmp_path / "selected.txt"
    selection.write_text(
        "# mode: subset\nvireo/tests/test_one.py::test_b\nvireo/tests/test_two.py\nvireo/tests/test_one.py::test_gone\n"
    )

    result = subprocess.run(
        [
            sys.executable, "-m", "pytest", "-p", "no:cacheprovider", "-p", "selected_tests_plugin",
            "--rootdir", str(tmp_path), "-o", "addopts=", "-q", "-rA",
            "--selected-tests", str(selection), str(tests_dir),
        ],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(ROOT / "scripts")},
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "PASSED vireo/tests/test_one.py::test_b" in result.stdout
    assert "PASSED vireo/tests/test_two.py::test_c" in result.stdout
    assert "test_a" not in result.stdout.split("short test summary")[1]
    assert "test_three" not in result.stdout
    assert "2 passed, 1 deselected" in result.stdout


def test_plugin_exits_zero_when_nothing_matches(tmp_path):
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    (tests_dir / "test_one.py").write_text("def test_a(): pass\n")
    selection = tmp_path / "selected.txt"
    selection.write_text("tests/test_one.py::test_gone\n")

    result = subprocess.run(
        [
            sys.executable, "-m", "pytest", "-p", "no:cacheprovider", "-p", "selected_tests_plugin",
            "--rootdir", str(tmp_path), "-o", "addopts=", "-q",
            "--selected-tests", str(selection), str(tests_dir),
        ],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(ROOT / "scripts")},
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "1 deselected" in result.stdout


def _run_plugin(tmp_path, *args):
    return subprocess.run(
        [
            sys.executable, "-m", "pytest", "-p", "no:cacheprovider", "-p", "selected_tests_plugin",
            "--rootdir", str(tmp_path), "-o", "addopts=", "-q", "-rA", *args,
        ],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(ROOT / "scripts")},
        capture_output=True,
        text=True,
    )


def _passed_ids(stdout):
    return {
        line.split("PASSED ", 1)[1].strip()
        for line in stdout.splitlines()
        if line.startswith("PASSED ")
    }


def test_plugin_shards_partition_the_collected_tests(tmp_path):
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    (tests_dir / "test_one.py").write_text(
        "".join(f"def test_a{i}(): pass\n" for i in range(5))
    )
    (tests_dir / "test_two.py").write_text(
        "".join(f"def test_b{i}(): pass\n" for i in range(4))
    )

    shards = []
    for index in (1, 2, 3):
        result = _run_plugin(tmp_path, "--shard", f"{index}/3", str(tests_dir))
        assert result.returncode == 0, result.stdout + result.stderr
        shards.append(_passed_ids(result.stdout))

    assert [len(ids) for ids in shards] == [3, 3, 3]
    assert set.union(*shards) == {
        *(f"tests/test_one.py::test_a{i}" for i in range(5)),
        *(f"tests/test_two.py::test_b{i}" for i in range(4)),
    }
    assert sum(len(ids) for ids in shards) == len(set.union(*shards))


def test_plugin_shards_apply_after_selection(tmp_path):
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    (tests_dir / "test_one.py").write_text(
        "".join(f"def test_a{i}(): pass\n" for i in range(4))
    )
    (tests_dir / "test_two.py").write_text("def test_c(): pass\n")
    selection = tmp_path / "selected.txt"
    selection.write_text("tests/test_one.py\n")

    shards = []
    for index in (1, 2):
        result = _run_plugin(
            tmp_path, "--selected-tests", str(selection), "--shard", f"{index}/2",
            str(tests_dir),
        )
        assert result.returncode == 0, result.stdout + result.stderr
        shards.append(_passed_ids(result.stdout))

    assert [len(ids) for ids in shards] == [2, 2]
    assert set.union(*shards) == {f"tests/test_one.py::test_a{i}" for i in range(4)}


def test_plugin_empty_shard_exits_zero(tmp_path):
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    (tests_dir / "test_one.py").write_text("def test_a(): pass\n")

    result = _run_plugin(tmp_path, "--shard", "2/4", str(tests_dir))

    assert result.returncode == 0, result.stdout + result.stderr
    assert "1 deselected" in result.stdout


@pytest.mark.parametrize("value", ["0/4", "5/4", "2", "a/b"])
def test_plugin_rejects_malformed_shard(tmp_path, value):
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    (tests_dir / "test_one.py").write_text("def test_a(): pass\n")

    result = _run_plugin(tmp_path, "--shard", value, str(tests_dir))

    assert result.returncode == pytest.ExitCode.USAGE_ERROR
    assert "--shard expects" in result.stderr
