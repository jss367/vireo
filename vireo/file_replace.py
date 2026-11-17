"""Publish a file by renaming it.

``replace_file`` retries transient locks on Windows, where a virus scanner or
another viewer can briefly hold the destination open. GitHub's Windows runners
can lock a fresh file for several seconds, hence the six-second retry budget.
``rename_no_replace`` uses the platform's atomic no-overwrite operation.
"""

import ctypes
import errno
import os
import sys
import time
from functools import cache

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


class NoReplaceRenameUnsupported(OSError):
    """The destination filesystem has no atomic no-overwrite rename.

    macOS SMB shares answer ``renamex_np(RENAME_EXCL)`` with ENOTSUP for a
    free destination name, so a caller that must publish there needs another
    way to refuse an overwrite.
    """


@cache
def _process_libc() -> ctypes.CDLL:
    return ctypes.CDLL(None, use_errno=True)


def rename_no_replace(src: str, dst: str) -> None:
    """Atomically rename a file, raising FileExistsError if the name is taken.

    Raises :class:`NoReplaceRenameUnsupported` when the platform or the
    destination filesystem has no such operation. Windows' ``os.rename``
    already refuses an existing destination, so it never raises that there.
    """
    if sys.platform == "win32":
        os.rename(src, dst)
        return

    src_bytes, dst_bytes = os.fsencode(src), os.fsencode(dst)
    libc = _process_libc()
    if sys.platform == "darwin" and hasattr(libc, "renamex_np"):
        rename = libc.renamex_np
        rename.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint]
        rename.restype = ctypes.c_int
        result = rename(src_bytes, dst_bytes, 0x00000004)  # RENAME_EXCL
    elif sys.platform == "linux" and hasattr(libc, "renameat2"):
        rename = libc.renameat2
        rename.argtypes = [
            ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint,
        ]
        rename.restype = ctypes.c_int
        at_fdcwd = -100
        rename_noreplace = 1
        result = rename(at_fdcwd, src_bytes, at_fdcwd, dst_bytes, rename_noreplace)
    else:
        result = -1
        ctypes.set_errno(errno.ENOSYS)

    if result == 0:
        return
    code = ctypes.get_errno()
    if code in {errno.EINVAL, errno.ENOSYS, errno.ENOTSUP, errno.EOPNOTSUPP}:
        raise NoReplaceRenameUnsupported(
            code, "Destination does not support renaming without overwriting an existing file", dst,
        )
    raise OSError(code, os.strerror(code), dst)
