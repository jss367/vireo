"""Read originals without preventing another process from moving them."""

import os
import sys
from typing import BinaryIO


def _windows_source_fd(path: str, _flags: int) -> int:
    """Return a read-only descriptor whose handle shares delete/rename access."""
    import ctypes
    import msvcrt
    from ctypes import wintypes

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    create = kernel.CreateFileW
    create.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, wintypes.LPVOID,
                       wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
    create.restype = wintypes.HANDLE
    close = kernel.CloseHandle
    close.argtypes = [wintypes.HANDLE]
    close.restype = wintypes.BOOL
    # GENERIC_READ; FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE;
    # OPEN_EXISTING; FILE_ATTRIBUTE_NORMAL. Delete sharing also permits rename:
    # https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilew
    handle = create(path, 0x80000000, 0x7, None, 3, 0x80, None)
    if handle == ctypes.c_void_p(-1).value:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        # The descriptor owns the handle after conversion; open() takes the
        # descriptor's ownership and closes it on construction failure too.
        return msvcrt.open_osfhandle(handle, os.O_RDONLY | os.O_BINARY | os.O_NOINHERIT)
    except OSError:
        close(handle)
        raise


def open_source(path: str) -> BinaryIO:
    """Open a binary original while allowing concurrent moves on Windows.

    Callers must still validate file and path identities after reading: sharing
    makes a moved/replaced source possible instead of blocking its owner.
    """
    return open(path, "rb", opener=_windows_source_fd if sys.platform == "win32" else None)
