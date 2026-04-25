"""Verify that importing the app does not create a log file.

The packaged app attaches a RotatingFileHandler at ~/.vireo/vireo.log.
Tests must not inherit that handler — otherwise pytest runs in any
workspace pollute the user's real Vireo log, mixing test tracebacks
into logs the user is reading to debug their actual app session.
"""

import os
import subprocess
import sys
import textwrap


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
