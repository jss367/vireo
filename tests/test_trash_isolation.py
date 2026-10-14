"""Regression coverage for keeping pytest's Trash operations off the host."""

import os

import pytest
import send2trash


def test_trash_preserves_same_named_files(tmp_path, isolated_trash):
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
    source = tmp_path_factory.mktemp("another-test") / "owl.jpg"
    source.write_bytes(b"keep me")

    with pytest.raises(OSError, match="outside this test"):
        send2trash.send2trash(source)

    assert source.read_bytes() == b"keep me"


def test_trash_moves_a_symlink_without_touching_its_target(tmp_path, tmp_path_factory, isolated_trash):
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


def test_finder_fallback_cannot_use_the_host_trash(tmp_path, monkeypatch):
    import app as app_module

    source = tmp_path / "bird.jpg"
    source.write_bytes(b"photo")
    monkeypatch.setattr(app_module.sys, "platform", "darwin")

    with pytest.raises(OSError, match="Finder Trash must be mocked"):
        app_module._trash_via_finder([source])

    assert source.read_bytes() == b"photo"
