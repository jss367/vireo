import os
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


@pytest.fixture(autouse=True)
def _expanduser_prefers_test_home(monkeypatch):
    """Make HOME-based tests portable to Windows.

    Windows normally resolves ``~`` from USERPROFILE, while this suite uses
    HOME to isolate app state. Read HOME dynamically so per-test monkeypatches
    are honored after fixture setup.
    """
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
