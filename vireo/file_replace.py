"""``os.replace`` that survives Windows' transient file locks.

On Windows, ``os.replace`` raises ``PermissionError`` ([WinError 5] /
[WinError 32]) while another handle holds the destination open: Defender or
the Search indexer scanning a file Vireo just wrote, a thumbnail viewer, or an
external editor that still has the previous handoff open. Those locks are
usually released within moments, so every publish-by-rename in Vireo goes
through :func:`replace_file`, which retries with bounded backoff there and is
exactly ``os.replace`` everywhere else. GitHub's Windows runners can hold a
fresh file for several seconds, hence the ~6 s budget.
"""

import os
import sys
import time

_WINDOWS_RETRY_DELAYS = (0.0, 0.05, 0.1, 0.2, 0.4, 0.8, 1.6, 3.2)


def replace_file(src, dst):
    """``os.replace(src, dst)``, retrying a transient Windows lock."""
    if sys.platform != "win32":
        os.replace(src, dst)
        return
    last_exc = None
    for delay in _WINDOWS_RETRY_DELAYS:
        if delay:
            time.sleep(delay)
        try:
            os.replace(src, dst)
            return
        except PermissionError as exc:
            last_exc = exc
    raise last_exc
