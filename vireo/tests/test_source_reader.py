"""Source reads must not retain Windows delete/rename exclusion handles."""

import ctypes
import os
import sys
from types import ModuleType, SimpleNamespace

import pytest
import source_reader


@pytest.fixture
def windows_api(monkeypatch):
    calls, closed = [], []
    handle = 2**40 + 123  # Exercise a handle that would truncate without HANDLE restype.

    class Function:
        def __init__(self, call):
            self.call = call

        def __call__(self, *args):
            return self.call(*args)

    create = Function(lambda *args: calls.append(args) or handle)
    close = Function(lambda value: closed.append(value) or 1)
    monkeypatch.setattr(ctypes, "WinDLL", lambda *a, **kw: SimpleNamespace(
        CreateFileW=create, CloseHandle=close,
    ), raising=False)
    crt = ModuleType("msvcrt")
    monkeypatch.setitem(sys.modules, "msvcrt", crt)
    monkeypatch.setattr(os, "O_BINARY", getattr(os, "O_BINARY", 0), raising=False)
    monkeypatch.setattr(os, "O_NOINHERIT", getattr(os, "O_NOINHERIT", 0), raising=False)
    return SimpleNamespace(calls=calls, closed=closed, handle=handle, create=create, crt=crt)


def test_windows_source_shares_moves_and_transfers_descriptor_ownership(tmp_path, windows_api, monkeypatch):
    path = tmp_path / "bird.nef"
    path.write_bytes(b"original bytes")
    descriptors = []

    def descriptor(handle, flags):
        assert handle == windows_api.handle
        fd = os.open(path, os.O_RDONLY)
        descriptors.append(fd)
        return fd

    monkeypatch.setattr(windows_api.crt, "open_osfhandle", descriptor, raising=False)
    with open(path, "rb", opener=source_reader._windows_source_fd) as stream:
        assert stream.read() == b"original bytes"
        assert windows_api.calls[0][1:3] == (0x80000000, 0x7)
        assert windows_api.calls[0][4] == 3  # Opening must never create/truncate originals.
    assert windows_api.closed == []  # Closing the descriptor owns the kernel handle.
    with pytest.raises(OSError):
        os.fstat(descriptors[0])


def test_failed_descriptor_conversion_closes_the_owned_handle(windows_api, monkeypatch):
    def failed(handle, flags):
        raise OSError("descriptor allocation failed")

    monkeypatch.setattr(windows_api.crt, "open_osfhandle", failed, raising=False)
    with pytest.raises(OSError, match="descriptor allocation"):
        source_reader._windows_source_fd("bird.nef", os.O_RDONLY)
    assert windows_api.closed == [windows_api.handle]


def test_failed_windows_open_has_no_handle_to_close(windows_api, monkeypatch):
    windows_api.create.call = lambda *args: ctypes.c_void_p(-1).value
    monkeypatch.setattr(ctypes, "get_last_error", lambda: 2, raising=False)
    monkeypatch.setattr(ctypes, "WinError", lambda code: FileNotFoundError(code, "missing"), raising=False)
    with pytest.raises(FileNotFoundError):
        source_reader._windows_source_fd("missing.nef", os.O_RDONLY)
    assert windows_api.closed == []


@pytest.mark.skipif(sys.platform != "win32", reason="Requires real Windows file-sharing semantics")
def test_windows_original_can_be_renamed_and_deleted_while_open(tmp_path):
    original, moved = tmp_path / "bird.nef", tmp_path / "moved.nef"
    original.write_bytes(b"original bytes")
    with source_reader.open_source(str(original)) as stream:
        original.rename(moved)
        moved.unlink()
        assert stream.read() == b"original bytes"
    assert not original.exists() and not moved.exists()
