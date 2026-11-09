"""Repository-root pytest configuration.

Registers the ``--selected-tests`` option (``scripts/selected_tests_plugin.py``)
so both ``tests/`` and ``vireo/tests/`` honour a selection file written by
``scripts/select_tests.py``. Platform Trash isolation is shared with browser
tests; other fixtures live in the per-suite ``conftest.py`` files.
"""

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest
import send2trash

pytest_plugins = ["scripts.selected_tests_plugin"]

# Process-wide path handling that must work again by the time the test body
# returns. Between the body and fixture teardown, pytest builds pathlib paths
# to report the result, and coverage (``--cov-context=test``, the Linux full
# run) flushes the test's lines after checking its data file with
# ``os.path.exists`` and ``pathlib.Path(os.path.abspath(...)).as_uri()``. A
# patched ``os.name`` makes Path() an uninstantiable WindowsPath, ``ntpath``'s
# normpath turns absolute POSIX paths relative, and an ``exists`` that fails
# the test raises inside coverage; each one crashes the xdist worker or errors
# every later test on it. Delegating wrappers that keep these working are fine.
_HOST_OS_NAME = os.name
_HOST_PATH_FUNCTIONS = {
    name: getattr(os.path, name)
    for name in ("abspath", "normpath", "isabs", "join", "exists")
}
_HOST_OS_FUNCTIONS = {
    name: getattr(os, name) for name in ("stat", "lstat", "getcwd", "fspath")
}


def _host_path_failure():
    """Run the path calls coverage's flush makes; return what breaks them."""
    probe = os.path.join(tempfile.gettempdir(), "vireo-path-probe")
    try:
        Path(os.path.abspath(probe)).as_uri()
        os.path.exists(probe)
    except (Exception, pytest.fail.Exception) as exc:  # broad: any failure here is the leak being reported
        return exc
    return None


def _restore_host_path_state():
    """Put the real os.name and path functions back; name what changed."""
    changed = []
    if os.name != _HOST_OS_NAME:
        changed.append(f"os.name = {os.name!r}")
        os.name = _HOST_OS_NAME
    for module, prefix, functions in (
        (os.path, "os.path", _HOST_PATH_FUNCTIONS),
        (os, "os", _HOST_OS_FUNCTIONS),
    ):
        for name, real in functions.items():
            if getattr(module, name) is not real:
                changed.append(f"{prefix}.{name}")
                setattr(module, name, real)
    return changed


@pytest.hookimpl(wrapper=True)
def pytest_runtest_call(item):
    """Fail a test whose body leaves process-wide path handling broken.

    The real functions are put back before pytest reports, so a test's own
    failure is still shown intact; a test that otherwise passed fails here.
    """
    try:
        result = yield
    finally:
        failure = _host_path_failure()
        changed = _restore_host_path_state() if failure is not None else []
    if failure is not None:
        pytest.fail(
            f"Path handling was still broken when the test body returned "
            f"({type(failure).__name__}: {failure}; patched: "
            f"{', '.join(changed) or 'unknown'}). pytest's reporting and "
            "coverage's per-test flush need it before fixture teardown, so "
            "patch these inside `with monkeypatch.context() as patch:` around "
            "the call that needs them.",
            pytrace=False,
        )
    return result


@pytest.fixture(autouse=True)
def isolated_trash(tmp_path, monkeypatch):
    """Move test files within tmp_path instead of filling the user's Trash.

    Patch the OS boundary, leaving Vireo's deletion, progress and catalog
    logic intact. Tests of platform failures can override these patches with
    their own mocks. Direct-volume unit tests may use the real implementation
    only with a synthetic volume inside tmp_path. Finder tests must mock
    subprocess.run explicitly.
    """
    from services import trash as trash_module
    from services import volumes

    root = tmp_path.resolve()
    trash = root / "test-trash"

    def test_source(path):
        """Validate the link's location without resolving its target."""
        source = Path(os.fsdecode(path))
        source = source.parent.resolve() / source.name
        if root not in source.parents:
            raise OSError(f"Refusing to trash a path outside this test: {source}")
        return source

    def move_to_test_trash(paths):
        """Preserve each source in a unique, test-owned destination."""
        if isinstance(paths, (str, bytes, os.PathLike)):
            paths = [paths]
        for path in paths:
            # Resolve the parent, not the file: trashing a symlink moves the
            # link itself, without touching its target.
            source = test_source(path)
            trash.mkdir(exist_ok=True)
            # Separate destinations preserve same-named photos, including
            # calls from concurrent background workers.
            destination = Path(tempfile.mkdtemp(dir=trash)) / source.name
            shutil.move(source, destination)

    real_volume_trash = trash_module.move_to_volume_trash

    def isolate_volume_trash(filepath):
        """Allow native moves only into a synthetic, test-owned volume."""
        if sys.platform != "darwin":
            return False
        volume_root = volumes.volume_root_for_path(filepath)
        if volume_root is None:
            return False
        source = test_source(filepath)
        volume = Path(volume_root).resolve()
        if volume == root or root in volume.parents:
            return real_volume_trash(str(source))
        # tmp_path itself may be hosted on an external volume. Never create
        # that volume's real .Trashes directory, even for temporary photos.
        move_to_test_trash(source)
        return True

    real_run = subprocess.run

    def prevent_real_finder_delete(args, *positional, **kwargs):
        """Require Finder deletion tests to provide a subprocess mock."""
        if isinstance(args, (list, tuple)) and args:
            command = os.fsdecode(args[0])
            if Path(command).name == "osascript" and any(
                'tell application "Finder" to delete' in os.fsdecode(arg)
                for arg in args[1:]
            ):
                raise OSError("Finder Trash must be mocked in tests")
        return real_run(args, *positional, **kwargs)

    monkeypatch.setattr(send2trash, "send2trash", move_to_test_trash)
    monkeypatch.setattr(trash_module, "move_to_volume_trash", isolate_volume_trash)
    monkeypatch.setattr(subprocess, "run", prevent_real_finder_delete)
    return trash
