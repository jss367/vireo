"""Subprocess helpers shared across Vireo.

The packaged desktop app runs with ``windows_subsystem = "windows"`` on
Windows (see ``src-tauri/src/main.rs``), so the Flask sidecar has no
attached console. Spawning a *console* program such as ``exiftool``,
``darktable-cli``, or the Adobe DNG converter would otherwise pop a
visible console window for the fraction of a second the child runs —
once per metadata read, develop, or capture-time edit. That flicker is
the kind of "lost Windows polish" users notice immediately.

``subprocess.CREATE_NO_WINDOW`` suppresses the console window. The flag
only exists on Windows, so callers splat :func:`no_window_kwargs` into
their ``subprocess.run`` / ``Popen`` calls and get an empty dict (a
no-op) on POSIX, keeping the call sites portable.
"""

import subprocess
import sys

# CREATE_NO_WINDOW is defined only on the Windows build of the stdlib.
# getattr keeps this module importable on POSIX, where the flag is absent.
_CREATE_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)


def no_window_kwargs() -> dict:
    """Return ``subprocess`` kwargs that hide the console window on Windows.

    Splat into any ``subprocess.run`` / ``subprocess.Popen`` call that
    launches a console program (exiftool, darktable-cli, the Adobe DNG
    converter) so the packaged windowless GUI app doesn't flash a console
    window. Returns ``{}`` on non-Windows platforms.
    """
    if sys.platform == "win32":
        return {"creationflags": _CREATE_NO_WINDOW}
    return {}
