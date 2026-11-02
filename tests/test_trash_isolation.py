"""Regression coverage for keeping pytest's Trash operations off the host."""

import os
import sys

import pytest
import send2trash
from services import trash, volumes


def test_trash_preserves_same_named_files(tmp_path, isolated_trash):
    """Files with identical names retain their distinct contents."""
    first = tmp_path / "first" / "bird.jpg"
    second = tmp_path / "second" / "bird.jpg"
    first.parent.mkdir()
    second.parent.mkdir()
    first.write_bytes(b"first photo")
    second.write_bytes(b"second photo")

    send2trash.send2trash([str(first), second])

    assert not first.exists()
    assert not second.exists()
    assert sorted(p.read_bytes() for p in isolated_trash.rglob("bird.jpg")) == [
        b"first photo", b"second photo",
    ]


def test_trash_refuses_files_from_outside_the_test(tmp_path_factory):
    """The shared fixture cannot move another test's files."""
    source = tmp_path_factory.mktemp("another-test") / "owl.jpg"
    source.write_bytes(b"keep me")

    with pytest.raises(OSError, match="outside this test"):
        send2trash.send2trash(source)

    assert source.read_bytes() == b"keep me"


def test_trash_moves_a_symlink_without_touching_its_target(tmp_path, tmp_path_factory, isolated_trash):
    """Trashing a link leaves its external target untouched."""
    target = tmp_path_factory.mktemp("outside-target") / "owl.jpg"
    target.write_bytes(b"original photo")
    link = tmp_path / "owl.jpg"
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("symlinks are unavailable")

    send2trash.send2trash(os.fsencode(link))

    assert not link.is_symlink()
    assert target.read_bytes() == b"original photo"
    assert next(isolated_trash.rglob("owl.jpg")).is_symlink()


def test_mounted_volume_trash_stays_in_test_directory(tmp_path, monkeypatch, isolated_trash):
    """A temporary directory on a volume must not use the volume's Trash."""

    source = tmp_path / "bird.jpg"
    source.write_bytes(b"photo")
    volume = tmp_path.parent
    roots = volumes.NetworkVolumeRoots((), {str(volume)})
    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(volumes, "volume_root_for_path", lambda _path: str(volume))

    moved, successful, failures = trash.trash_paths([str(source)], network_roots=roots)

    assert (moved, successful, failures) == (1, {str(source)}, [])
    assert not source.exists()
    assert next(isolated_trash.rglob("bird.jpg")).read_bytes() == b"photo"
    assert not (volume / ".Trashes").exists()


def test_mounted_volume_trash_refuses_files_outside_the_test(tmp_path_factory, monkeypatch):
    """The direct macOS path cannot trash a file outside this test."""

    source = tmp_path_factory.mktemp("another-volume") / "owl.jpg"
    source.write_bytes(b"keep me")
    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(volumes, "volume_root_for_path", lambda _path: str(source.parent))

    with pytest.raises(OSError, match="outside this test"):
        trash.move_to_volume_trash(str(source))

    assert source.read_bytes() == b"keep me"
    assert not (source.parent / ".Trashes").exists()


def test_finder_fallback_cannot_use_the_host_trash(tmp_path, monkeypatch):
    """The Finder fallback must never invoke a real deletion subprocess."""

    source = tmp_path / "bird.jpg"
    source.write_bytes(b"photo")
    monkeypatch.setattr(sys, "platform", "darwin")

    with pytest.raises(OSError, match="Finder Trash must be mocked"):
        trash.trash_via_finder([source])

    assert source.read_bytes() == b"photo"
