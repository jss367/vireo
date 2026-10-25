import os
import shutil
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "vireo"))

from db import Database


@pytest.fixture(autouse=True)
def _disable_startup_backfill_timers(monkeypatch):
    """See ``vireo/tests/conftest.py`` for the rationale — mirrored here so
    the top-level ``tests/`` suite (which also calls ``create_app``) doesn't
    leak Timer-driven backfill jobs across tests."""
    monkeypatch.setenv("VIREO_DISABLE_STARTUP_BACKFILL_TIMERS", "1")
    monkeypatch.setenv("VIREO_DISABLE_BROWSER_AUTH", "1")
    monkeypatch.setenv("VIREO_REQUIRE_EXIFTOOL_FOR_IMPORT", "0")


_REAL_DISK_USAGE = shutil.disk_usage


@pytest.fixture(autouse=True)
def _ample_free_disk(monkeypatch):
    """Report ample free space unless a test fakes its own.

    Work Locally, managed imports and staging refuse to start without room
    for the copy plus a reserve of up to tens of GB, measured with
    ``shutil.disk_usage``. Left real, every such test failed whenever the
    machine running it was low on space (145 of them below ~20 GB free). A
    test about low-space handling patches ``disk_usage`` itself, which
    overrides this.
    """
    def disk_usage(path):
        real = _REAL_DISK_USAGE(path)  # still raises for a missing path
        total = max(real.total, 4 * 1024**4)
        free = max(real.free, total // 2)
        return type(real)(total, total - free, free)

    monkeypatch.setattr(shutil, "disk_usage", disk_usage)


@pytest.fixture(autouse=True)
def _expanduser_prefers_test_home(monkeypatch):
    """Make HOME-based tests portable to Windows.

    Windows normally resolves ``~`` from USERPROFILE, while this suite uses
    HOME to isolate app state. Read HOME dynamically so per-test monkeypatches
    are honored after fixture setup.
    """
    # HOME redirection must also isolate native storage on Windows and Linux.
    # Tests of overrides set their own values after this fixture runs.
    for name in ("LOCALAPPDATA", "XDG_DATA_HOME", "XDG_CACHE_HOME"):
        monkeypatch.delenv(name, raising=False)
    real_expanduser = os.path.expanduser

    def expanduser(path):
        if isinstance(path, bytes):
            if path == b"~" or path.startswith((b"~/", b"~\\")):
                home = os.environ.get("HOME")
                if home:
                    return os.fsencode(home) + path[1:]
        elif path == "~" or path.startswith(("~/", "~\\")):
            home = os.environ.get("HOME")
            if home:
                return home + path[1:]
        return real_expanduser(path)

    monkeypatch.setattr(os.path, "expanduser", expanduser)


@pytest.fixture
def db(tmp_path):
    """Return a Database backed by a temp file."""
    d = Database(str(tmp_path / "test.db"))
    yield d
    d.close()


@pytest.fixture(autouse=True)
def _isolate_huggingface_cache(tmp_path, monkeypatch):
    """Keep every test away from the developer's real Hugging Face cache.

    ``services.hf_cache`` deletes under ``huggingface_cache_dir()`` when a
    model is removed; see the same fixture in ``vireo/tests/conftest.py``.
    """
    monkeypatch.setenv("HF_HUB_CACHE", str(tmp_path / "hf-hub-cache"))
