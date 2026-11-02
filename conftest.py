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
