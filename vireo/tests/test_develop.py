import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def test_find_darktable_returns_none_when_missing(monkeypatch):
    """find_darktable returns None when binary is not found."""
    from develop import find_darktable

    monkeypatch.setattr("shutil.which", lambda x: None)
    assert find_darktable("") is None


def test_find_darktable_returns_configured_path(tmp_path):
    """find_darktable returns the configured path if it exists."""
    from develop import find_darktable

    fake_bin = tmp_path / "darktable-cli"
    fake_bin.touch()
    fake_bin.chmod(0o755)
    assert find_darktable(str(fake_bin)) == str(fake_bin)


def test_find_darktable_returns_none_for_bad_configured_path(monkeypatch):
    """find_darktable returns None when configured path doesn't exist and PATH has nothing."""
    from develop import find_darktable

    monkeypatch.setattr("shutil.which", lambda x: None)
    assert find_darktable("/nonexistent/darktable-cli") is None


def test_build_command_minimal():
    """build_command produces correct args for basic conversion."""
    from develop import build_command

    cmd = build_command(
        darktable_bin="/usr/bin/darktable-cli",
        input_path="/photos/bird.CR3",
        output_path="/output/bird.jpg",
    )
    assert cmd[0] == "/usr/bin/darktable-cli"
    assert "/photos/bird.CR3" in cmd
    assert "/output/bird.jpg" in cmd


def test_build_command_with_style():
    """build_command includes --style when provided."""
    from develop import build_command

    cmd = build_command(
        darktable_bin="/usr/bin/darktable-cli",
        input_path="/photos/bird.CR3",
        output_path="/output/bird.jpg",
        style="Wildlife",
    )
    assert "--style" in cmd
    idx = cmd.index("--style")
    assert cmd[idx + 1] == "Wildlife"


def test_build_command_with_width():
    """build_command includes --width when provided."""
    from develop import build_command

    cmd = build_command(
        darktable_bin="/usr/bin/darktable-cli",
        input_path="/photos/bird.CR3",
        output_path="/output/bird.jpg",
        width=2048,
    )
    assert "--width" in cmd
    idx = cmd.index("--width")
    assert cmd[idx + 1] == "2048"


def test_output_path_for_photo():
    """output_path_for_photo builds correct path."""
    from develop import output_path_for_photo

    result = output_path_for_photo(
        filename="bird.CR3",
        output_dir="/output",
        output_format="jpg",
    )
    assert result == "/output/bird.jpg"


def test_output_path_for_photo_tiff():
    """output_path_for_photo handles tiff format."""
    from develop import output_path_for_photo

    result = output_path_for_photo(
        filename="eagle.NEF",
        output_dir="/developed",
        output_format="tiff",
    )
    assert result == "/developed/eagle.tiff"


def test_develop_photo_returns_error_when_no_binary():
    """develop_photo returns error dict when darktable not found."""
    from develop import develop_photo

    result = develop_photo(
        darktable_bin="",
        input_path="/photos/bird.CR3",
        output_path="/output/bird.jpg",
    )
    assert result["success"] is False
    assert "not found" in result["error"].lower() or "not configured" in result["error"].lower()


def test_develop_photo_returns_error_when_input_missing():
    """develop_photo returns error when input file doesn't exist."""
    from develop import develop_photo

    result = develop_photo(
        darktable_bin="/usr/bin/darktable-cli",
        input_path="/nonexistent/bird.CR3",
        output_path="/output/bird.jpg",
    )
    assert result["success"] is False
    assert "not found" in result["error"].lower()


def test_find_darktable_resolves_symlink(tmp_path):
    """find_darktable follows symlinks so macOS bundle lookup works.

    darktable-cli invoked via a symlink (e.g. Homebrew's /usr/local/bin
    symlink into /Applications/darktable.app) dies in dt_init because the
    bundle-resource walk starts from argv[0]. Vireo must resolve to the real
    binary path before handing it to subprocess.
    """
    import develop

    real = tmp_path / "real_darktable-cli"
    real.touch()
    real.chmod(0o755)
    link = tmp_path / "symlinked_darktable-cli"
    link.symlink_to(real)

    # Configured path case
    assert develop.find_darktable(str(link)) == str(real)

    # PATH-auto-detect case: monkeypatch shutil.which to hand back the symlink
    import unittest.mock
    with unittest.mock.patch("shutil.which", return_value=str(link)):
        assert develop.find_darktable("") == str(real)


def _fake_completed(returncode, stdout="", stderr=""):
    import subprocess
    return subprocess.CompletedProcess(args=[], returncode=returncode,
                                        stdout=stdout, stderr=stderr)


def test_develop_photo_surfaces_stdout_when_stderr_empty(tmp_path, monkeypatch):
    """darktable writes critical errors to stdout; error message must not be blank."""
    import subprocess

    import develop

    raw = tmp_path / "bird.NEF"
    raw.touch()
    fake_bin = tmp_path / "darktable-cli"
    fake_bin.touch()
    fake_bin.chmod(0o755)
    out = tmp_path / "out" / "bird.jpg"

    stdout_msg = "     0.1899 [dt_init] ERROR: can't init develop system, aborting."
    monkeypatch.setattr(subprocess, "run",
                        lambda *a, **kw: _fake_completed(1, stdout=stdout_msg, stderr=""))

    result = develop.develop_photo(str(fake_bin), str(raw), str(out))
    assert result["success"] is False
    assert "can't init develop system" in result["error"]
    assert "exited with code 1" in result["error"]


def test_develop_photo_surfaces_stderr_when_stdout_empty(tmp_path, monkeypatch):
    """Stderr-only failures still surface (back-compat)."""
    import subprocess

    import develop

    raw = tmp_path / "bird.NEF"
    raw.touch()
    fake_bin = tmp_path / "darktable-cli"
    fake_bin.touch()
    fake_bin.chmod(0o755)
    out = tmp_path / "out" / "bird.jpg"

    monkeypatch.setattr(subprocess, "run",
                        lambda *a, **kw: _fake_completed(1, stdout="", stderr="Segfault in lua"))

    result = develop.develop_photo(str(fake_bin), str(raw), str(out))
    assert result["success"] is False
    assert "Segfault in lua" in result["error"]


def test_develop_photo_labels_both_streams_when_both_present(tmp_path, monkeypatch):
    """When darktable writes to both streams, include both with labels."""
    import subprocess

    import develop

    raw = tmp_path / "bird.NEF"
    raw.touch()
    fake_bin = tmp_path / "darktable-cli"
    fake_bin.touch()
    fake_bin.chmod(0o755)
    out = tmp_path / "out" / "bird.jpg"

    monkeypatch.setattr(subprocess, "run",
                        lambda *a, **kw: _fake_completed(1, stdout="stdout msg", stderr="stderr msg"))

    result = develop.develop_photo(str(fake_bin), str(raw), str(out))
    assert "stdout msg" in result["error"]
    assert "stderr msg" in result["error"]
    assert "stdout:" in result["error"]
    assert "stderr:" in result["error"]


def test_develop_photo_truncates_verbose_failure(tmp_path, monkeypatch):
    """A pathological multi-KB failure shouldn't explode the job error list."""
    import subprocess

    import develop

    raw = tmp_path / "bird.NEF"
    raw.touch()
    fake_bin = tmp_path / "darktable-cli"
    fake_bin.touch()
    fake_bin.chmod(0o755)
    out = tmp_path / "out" / "bird.jpg"

    huge = "A" * 5000 + "TAIL_MARKER"
    monkeypatch.setattr(subprocess, "run",
                        lambda *a, **kw: _fake_completed(1, stdout=huge, stderr=""))

    result = develop.develop_photo(str(fake_bin), str(raw), str(out))
    # Most of the head should be dropped; the tail (which carries the actual
    # error message darktable prints near the end) must survive.
    assert "TAIL_MARKER" in result["error"]
    assert len(result["error"]) < 800
