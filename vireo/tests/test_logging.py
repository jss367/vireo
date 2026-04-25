"""Verify that importing the app does not create a log file.

The packaged app attaches a RotatingFileHandler at ~/.vireo/vireo.log.
Tests must not inherit that handler — otherwise pytest runs in any
workspace pollute the user's real Vireo log, mixing test tracebacks
into logs the user is reading to debug their actual app session.
"""

import logging
import os
import subprocess
import sys
import textwrap


def test_setup_file_logging_is_idempotent(tmp_path):
    """Repeated calls must not stack duplicate RotatingFileHandlers.

    main() normally runs once per process, but harness scripts that call
    main() repeatedly (or anything that re-invokes _setup_file_logging)
    would otherwise leak handlers and duplicate every log entry.
    """
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from app import _setup_file_logging

    root = logging.getLogger()
    before = list(root.handlers)
    try:
        _setup_file_logging(log_dir=str(tmp_path))
        _setup_file_logging(log_dir=str(tmp_path))
        added = [h for h in root.handlers if h not in before]
        assert len(added) == 1, (
            f"Expected exactly one new handler after two calls, got {len(added)}"
        )
    finally:
        for h in list(root.handlers):
            if h not in before:
                root.removeHandler(h)
                h.close()


def test_importing_app_does_not_create_log_file(tmp_path):
    fake_home = tmp_path / "home"
    fake_home.mkdir()

    env = {**os.environ, "HOME": str(fake_home)}
    # cwd into the vireo/ package directory so `import app` resolves the
    # same way conftest.py and the existing test fixtures do.
    app_pkg_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    code = textwrap.dedent(
        """
        from app import create_app  # noqa: F401
        """
    )

    subprocess.run(
        [sys.executable, "-c", code],
        env=env,
        check=True,
        cwd=app_pkg_dir,
    )

    log_path = fake_home / ".vireo" / "vireo.log"
    assert not log_path.exists(), (
        f"Importing app.create_app created {log_path}; the file handler "
        "should only attach when the server actually starts (main()), not "
        "at module-import time, so test runs don't pollute the user's log."
    )
