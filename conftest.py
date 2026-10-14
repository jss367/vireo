"""Repository-root pytest configuration.

Registers the ``--selected-tests`` option (``scripts/selected_tests_plugin.py``)
so both ``tests/`` and ``vireo/tests/`` honour a selection file written by
``scripts/select_tests.py``. Platform Trash isolation is shared with browser
tests; other fixtures live in the per-suite ``conftest.py`` files.
"""

import os
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest
import send2trash

pytest_plugins = ["scripts.selected_tests_plugin"]


@pytest.fixture(autouse=True)
def isolated_trash(tmp_path, monkeypatch):
    """Move test files within tmp_path instead of filling the user's Trash.

    Patch the OS boundary, leaving Vireo's deletion, progress and catalog
    logic intact. Tests of platform failures can override these patches with
    their own mocks. Finder tests must mock subprocess.run explicitly.
    """
    root = tmp_path.resolve()
    trash = root / "test-trash"

    def move_to_test_trash(paths):
        if isinstance(paths, (str, bytes, os.PathLike)):
            paths = [paths]
        for path in paths:
            source = Path(os.fsdecode(path))
            # Resolve the parent, not the file: trashing a symlink moves the
            # link itself, without touching its target.
            source = source.parent.resolve() / source.name
            if root not in source.parents:
                raise OSError(f"Refusing to trash a path outside this test: {source}")
            trash.mkdir(exist_ok=True)
            # Separate destinations preserve same-named photos, including
            # calls from concurrent background workers.
            destination = Path(tempfile.mkdtemp(dir=trash)) / source.name
            shutil.move(source, destination)

    real_run = subprocess.run

    def prevent_real_finder_delete(args, *positional, **kwargs):
        if isinstance(args, (list, tuple)) and args:
            command = os.fsdecode(args[0])
            if Path(command).name == "osascript" and any(
                'tell application "Finder" to delete' in os.fsdecode(arg)
                for arg in args[1:]
            ):
                raise OSError("Finder Trash must be mocked in tests")
        return real_run(args, *positional, **kwargs)

    monkeypatch.setattr(send2trash, "send2trash", move_to_test_trash)
    monkeypatch.setattr(subprocess, "run", prevent_real_finder_delete)
    return trash
