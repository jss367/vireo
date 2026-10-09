import os
import sys

import pytest
from testing.waits import synchronization_timeout

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def test_find_darktable_returns_none_when_missing(monkeypatch):
    """find_darktable returns None when binary is not found."""
    from develop import find_darktable

    monkeypatch.setattr("shutil.which", lambda x: None)
    import develop
    monkeypatch.setattr(develop, "darktable_search_paths", lambda: [])
    assert find_darktable("") is None


def test_find_darktable_returns_configured_path(tmp_path):
    """find_darktable returns the configured path if it exists."""
    from develop import find_darktable

    fake_bin = tmp_path / "darktable-cli"
    fake_bin.touch()
    fake_bin.chmod(0o755)
    assert find_darktable(str(fake_bin)) == str(fake_bin)


def test_find_dng_converter_returns_configured_path(tmp_path):
    """find_dng_converter returns the configured converter binary if it exists."""
    from develop import find_dng_converter

    fake_bin = tmp_path / "Adobe DNG Converter"
    fake_bin.touch()
    fake_bin.chmod(0o755)
    assert find_dng_converter(str(fake_bin)) == str(fake_bin)


def test_find_dng_converter_finds_windows_default_install(tmp_path, monkeypatch):
    """Default Windows install (Program Files\\Adobe DNG Converter\\...) is detected."""
    from develop import find_dng_converter

    program_files = tmp_path / "Program Files"
    converter_dir = program_files / "Adobe DNG Converter"
    converter_dir.mkdir(parents=True)
    fake_bin = converter_dir / "Adobe DNG Converter.exe"
    fake_bin.touch()

    monkeypatch.setattr("shutil.which", lambda _name: None)
    monkeypatch.delenv("PROGRAMFILES(X86)", raising=False)
    monkeypatch.delenv("PROGRAMW6432", raising=False)
    monkeypatch.setenv("PROGRAMFILES", str(program_files))

    assert find_dng_converter("") == str(fake_bin)


def test_find_dng_converter_finds_windows_x86_nested_install(tmp_path, monkeypatch):
    """Older 32-bit installs (Program Files (x86)\\Adobe\\Adobe DNG Converter) are detected."""
    from develop import find_dng_converter

    program_files_x86 = tmp_path / "Program Files (x86)"
    converter_dir = program_files_x86 / "Adobe" / "Adobe DNG Converter"
    converter_dir.mkdir(parents=True)
    fake_bin = converter_dir / "Adobe DNG Converter.exe"
    fake_bin.touch()

    monkeypatch.setattr("shutil.which", lambda _name: None)
    monkeypatch.delenv("PROGRAMFILES", raising=False)
    monkeypatch.delenv("PROGRAMW6432", raising=False)
    monkeypatch.setenv("PROGRAMFILES(X86)", str(program_files_x86))

    assert find_dng_converter("") == str(fake_bin)


def test_find_darktable_returns_none_for_bad_configured_path(monkeypatch):
    """find_darktable returns None when configured path doesn't exist and PATH has nothing."""
    from develop import find_darktable

    monkeypatch.setattr("shutil.which", lambda x: None)
    import develop
    monkeypatch.setattr(develop, "darktable_search_paths", lambda: [])
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
    assert os.path.normpath(result) == os.path.normpath("/output/bird.jpg")


def test_output_path_for_photo_tiff():
    """output_path_for_photo handles tiff format."""
    from develop import output_path_for_photo

    result = output_path_for_photo(
        filename="eagle.NEF",
        output_dir="/developed",
        output_format="tiff",
    )
    assert os.path.normpath(result) == os.path.normpath("/developed/eagle.tiff")


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


def test_find_darktable_detects_standard_windows_install(monkeypatch, tmp_path):
    import develop

    binary = tmp_path / "darktable" / "bin" / "darktable-cli.exe"
    binary.parent.mkdir(parents=True)
    binary.write_bytes(b"exe")
    monkeypatch.setattr(develop.os, "name", "nt")
    monkeypatch.setenv("PROGRAMFILES", str(tmp_path))
    monkeypatch.delenv("PROGRAMFILES(X86)", raising=False)
    monkeypatch.delenv("PROGRAMW6432", raising=False)
    monkeypatch.setattr(develop.shutil, "which", lambda _name: None)

    assert develop.find_darktable("") == str(binary.resolve())


def test_is_nikon_high_efficiency_nef_from_metadata(tmp_path):
    """ExifTool NEFCompression values 13/14 are Nikon HE/HE*."""
    import develop

    raw = tmp_path / "bird.NEF"
    raw.touch()

    assert develop.is_nikon_high_efficiency_nef(
        str(raw), metadata={"Nikon": {"NEFCompression": 13}}
    )
    assert develop.is_nikon_high_efficiency_nef(
        str(raw), metadata={"Nikon": {"NEFCompression": "High Efficiency*"}}
    )
    assert not develop.is_nikon_high_efficiency_nef(
        str(raw), metadata={"Nikon": {"NEFCompression": 3}}
    )
    assert not develop.is_nikon_high_efficiency_nef(
        str(tmp_path / "bird.CR3"), metadata={"Nikon": {"NEFCompression": 13}}
    )


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


def test_develop_photo_converts_nikon_he_nef_before_darktable(tmp_path, monkeypatch):
    """When enabled, Nikon HE NEFs are converted to DNG before darktable-cli."""
    import subprocess

    import develop

    raw = tmp_path / "bird.NEF"
    raw.touch()
    darktable_bin = tmp_path / "darktable-cli"
    darktable_bin.touch()
    darktable_bin.chmod(0o755)
    dng_bin = tmp_path / "Adobe DNG Converter"
    dng_bin.touch()
    dng_bin.chmod(0o755)
    out = tmp_path / "out" / "bird.jpg"
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        if cmd[0] == str(dng_bin):
            dng_dir = cmd[cmd.index("-d") + 1]
            dng_path = os.path.join(dng_dir, "bird.dng")
            with open(dng_path, "w") as f:
                f.write("dng")
            return _fake_completed(0)
        assert cmd[0] == str(darktable_bin)
        assert cmd[1].endswith("bird.dng")
        os.makedirs(os.path.dirname(cmd[2]), exist_ok=True)
        with open(cmd[2], "w") as f:
            f.write("jpg")
        return _fake_completed(0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = develop.develop_photo(
        str(darktable_bin),
        str(raw),
        str(out),
        auto_convert_dng=True,
        dng_converter_bin=str(dng_bin),
        metadata={"Nikon": {"NEFCompression": 14}},
    )

    assert result["success"] is True
    assert len(calls) == 2
    assert calls[0][0] == str(dng_bin)
    assert calls[1][0] == str(darktable_bin)


def test_develop_photo_reports_missing_dng_converter_for_nikon_he(tmp_path, monkeypatch):
    """HE files fail early with a useful DNG converter message when enabled."""
    import develop

    raw = tmp_path / "bird.NEF"
    raw.touch()
    darktable_bin = tmp_path / "darktable-cli"
    darktable_bin.touch()
    darktable_bin.chmod(0o755)
    out = tmp_path / "out" / "bird.jpg"

    monkeypatch.setattr("shutil.which", lambda _name: None)

    result = develop.develop_photo(
        str(darktable_bin),
        str(raw),
        str(out),
        auto_convert_dng=True,
        dng_converter_bin=str(tmp_path / "missing-dng-converter"),
        metadata={"Nikon": {"NEFCompression": 13}},
    )

    assert result["success"] is False
    assert "Nikon High Efficiency NEF" in result["error"]
    assert "DNG conversion failed" in result["error"]
    assert "download it from Adobe" in result["error"]


def test_find_darktable_finds_macos_app_bundle(monkeypatch):
    """A normal macOS .dmg install is found even though it is not on PATH."""
    import develop
    from develop import find_darktable

    bundle = "/Applications/darktable.app/Contents/MacOS/darktable-cli"
    monkeypatch.setattr("shutil.which", lambda x: None)
    # A Mac is os.name == "posix" *and* sys.platform == "darwin". Pin both, or
    # this test exercises the Windows branch on the windows-latest CI leg.
    monkeypatch.setattr(develop.os, "name", "posix")
    monkeypatch.setattr("sys.platform", "darwin")
    monkeypatch.setattr("os.path.isfile", lambda p: p == bundle)
    monkeypatch.setattr("os.path.realpath", lambda p: p)

    assert find_darktable("") == bundle


def test_find_darktable_configured_path_wins_over_bundle(tmp_path, monkeypatch):
    """An explicitly configured path takes precedence over the bundle probe."""
    from develop import find_darktable

    configured = tmp_path / "darktable-cli"
    configured.touch()
    monkeypatch.setattr("sys.platform", "darwin")

    assert find_darktable(str(configured)) == str(configured)


def test_darktable_search_paths_matches_find_darktable(monkeypatch):
    """checked_paths cannot drift from what find_darktable actually probes.

    Every candidate reported to the user must be one find_darktable would
    accept; otherwise the "we checked here" message is a lie.
    """
    import develop
    from develop import darktable_search_paths, find_darktable

    monkeypatch.setattr("shutil.which", lambda x: None)
    monkeypatch.setattr(develop.os, "name", "posix")
    monkeypatch.setattr("sys.platform", "darwin")
    paths = darktable_search_paths()
    assert paths, "expected at least one candidate on darwin"

    for candidate in paths:
        monkeypatch.setattr("os.path.isfile", lambda p, c=candidate: p == c)
        monkeypatch.setattr("os.path.realpath", lambda p: p)
        assert find_darktable("") == candidate


def _force_linux_tools_dir(monkeypatch, tools_dir):
    """Pin the platform branch to Linux and point it at ``tools_dir``.

    A Linux box is os.name == "posix" *and* sys.platform == "linux"; pin both
    so this exercises the AppImage branch on macOS and Windows CI legs too.

    Redirects darktable_tools_dir() rather than patching expanduser: the
    function composes the path with os.path.join for native separators, so
    an expanduser mock keyed on a specific literal would no longer intercept.
    """
    import develop

    monkeypatch.setattr(develop.os, "name", "posix")
    monkeypatch.setattr("sys.platform", "linux")
    monkeypatch.setattr(develop, "darktable_tools_dir", lambda: str(tools_dir))


def test_darktable_search_paths_linux_orders_appimages_newest_first(tmp_path, monkeypatch):
    """The tools dir accumulates versions; the most recent install must win."""
    from develop import darktable_search_paths

    tools_dir = tmp_path / "tools" / "darktable"
    tools_dir.mkdir(parents=True)
    older = tools_dir / "darktable-4.6.AppImage"
    newer = tools_dir / "darktable-5.0.AppImage"
    older.write_bytes(b"old")
    newer.write_bytes(b"new")
    # Match hand_off's post-download chmod: search_paths gates on the exec
    # bit precisely to skip AppImages that never got there.
    older.chmod(0o755)
    newer.chmod(0o755)
    os.utime(older, (1_000_000, 1_000_000))
    os.utime(newer, (2_000_000, 2_000_000))

    _force_linux_tools_dir(monkeypatch, tools_dir)

    assert darktable_search_paths() == [str(newer), str(older)]


def test_darktable_search_paths_linux_ignores_non_appimages(tmp_path, monkeypatch):
    """Download leftovers (partials, checksums) are not runnable candidates."""
    from develop import darktable_search_paths

    tools_dir = tmp_path / "tools" / "darktable"
    tools_dir.mkdir(parents=True)
    appimage = tools_dir / "darktable-5.0.AppImage"
    appimage.write_bytes(b"app")
    appimage.chmod(0o755)
    (tools_dir / "darktable-5.0.AppImage.part").write_bytes(b"partial")
    (tools_dir / "SHA256SUMS").write_bytes(b"sums")
    (tools_dir / "README.txt").write_bytes(b"readme")

    _force_linux_tools_dir(monkeypatch, tools_dir)

    assert darktable_search_paths() == [str(appimage)]


@pytest.mark.skipif(
    os.name == "nt",
    reason=(
        "Windows has no POSIX exec bit — os.access(_, X_OK) returns True for "
        "any readable file regardless of chmod, so the abandoned 0o644 "
        "AppImage cannot be distinguished from the installed 0o755 one here. "
        "The gate itself is only reachable on the Linux branch in production."
    ),
)
def test_darktable_search_paths_linux_ignores_non_executable_appimages(tmp_path, monkeypatch):
    """A download cancelled during digest verification leaves the AppImage at
    its final path but with the default non-executable mode (hand_off never
    ran the chmod).  find_darktable would otherwise report the abandoned file
    as darktable — the Try again button would hide and every RAW export would
    invoke darktable-cli on an unrunnable file."""
    from develop import darktable_search_paths

    tools_dir = tmp_path / "tools" / "darktable"
    tools_dir.mkdir(parents=True)
    installed = tools_dir / "darktable-5.6.AppImage"
    abandoned = tools_dir / "darktable-5.7.AppImage"
    installed.write_bytes(b"app")
    installed.chmod(0o755)
    abandoned.write_bytes(b"app")
    abandoned.chmod(0o644)  # what urllib leaves it as; hand_off's chmod never ran

    _force_linux_tools_dir(monkeypatch, tools_dir)

    assert darktable_search_paths() == [str(installed)]


def test_darktable_search_paths_linux_missing_dir_is_empty(tmp_path, monkeypatch):
    """No install yet is the common case and must not raise."""
    from develop import darktable_search_paths

    _force_linux_tools_dir(monkeypatch, tmp_path / "never" / "created")

    assert darktable_search_paths() == []


def test_darktable_search_paths_linux_survives_vanishing_appimage(tmp_path, monkeypatch):
    """A file removed between listdir and the mtime sort must not raise.

    All callers are written against "returns paths or nothing, never raises":
    an escaping OSError would 500 /api/darktable/status and fail a develop job
    with a stack trace instead of a clean "darktable-cli not found".
    """
    import develop
    from develop import darktable_search_paths

    tools_dir = tmp_path / "tools" / "darktable"
    tools_dir.mkdir(parents=True)
    survivor = tools_dir / "darktable-5.0.AppImage"
    vanished = tools_dir / "darktable-4.6.AppImage"
    survivor.write_bytes(b"app")
    survivor.chmod(0o755)
    vanished.write_bytes(b"doomed")
    vanished.chmod(0o755)

    _force_linux_tools_dir(monkeypatch, tools_dir)

    real_getmtime = os.path.getmtime

    def racing_getmtime(path):
        if path == str(vanished):
            raise FileNotFoundError(2, "No such file or directory", path)
        return real_getmtime(path)

    monkeypatch.setattr(develop.os.path, "getmtime", racing_getmtime)

    paths = darktable_search_paths()
    assert str(survivor) in paths
    assert paths[0] == str(survivor)


def _write_appimage(path):
    """Write a file that looks like a type-2 AppImage to a magic-byte probe.

    The ELF ident bytes are padding up to offset 8, where the AppImage spec
    puts 0x41 0x49 0x02.
    """
    path.write_bytes(b"\x7fELF\x02\x01\x01\x00" + b"AI\x02" + b"\x00" * 32)
    path.chmod(0o755)
    return path


def _write_plain_elf(path):
    """An ordinary ELF executable: same header, EI_PAD left zeroed."""
    path.write_bytes(b"\x7fELF\x02\x01\x01\x00" + b"\x00" * 40)
    path.chmod(0o755)
    return path


def test_is_appimage_detects_magic_bytes(tmp_path):
    """Name-independent detection: users rename AppImages to ~/.local/bin/darktable."""
    from develop import _is_appimage

    assert _is_appimage(str(_write_appimage(tmp_path / "darktable"))) is True


def test_is_appimage_rejects_plain_elf(tmp_path):
    """A real darktable-cli binary must not get the AppImage argv[1] prefix."""
    from develop import _is_appimage

    assert _is_appimage(str(_write_plain_elf(tmp_path / "darktable-cli"))) is False


def test_is_appimage_missing_path_is_false(tmp_path):
    """build_command is still called with paths that may not exist; never raise."""
    from develop import _is_appimage

    assert _is_appimage(str(tmp_path / "nope.AppImage")) is False


def test_is_appimage_short_file_is_false(tmp_path):
    """A truncated download is shorter than the magic offset; must not raise."""
    from develop import _is_appimage

    short = tmp_path / "partial.AppImage"
    short.write_bytes(b"\x7fELF")
    assert _is_appimage(str(short)) is False


def test_is_appimage_directory_is_false(tmp_path):
    """Opening a directory raises IsADirectoryError, an OSError; swallow it."""
    from develop import _is_appimage

    assert _is_appimage(str(tmp_path)) is False


def test_build_command_selects_cli_inside_appimage(tmp_path):
    """darktable's AppImage picks its binary from argv[1]; without this we
    would launch the GUI and hang the export job."""
    from develop import build_command

    appimage = _write_appimage(tmp_path / "Darktable-5.6.0-x86_64.AppImage")
    cmd = build_command(str(appimage), "in.raw", "out.jpg")
    assert cmd[1] == "darktable-cli"
    assert cmd[2:] == ["in.raw", "out.jpg"]


def test_build_command_selects_cli_inside_renamed_appimage(tmp_path):
    """The conventional Linux move is to rename the AppImage to ~/.local/bin/darktable
    and point Settings at it. find_darktable returns configured paths without
    consulting darktable_search_paths, so filename-based detection would miss it."""
    from develop import build_command

    appimage = _write_appimage(tmp_path / "darktable")
    cmd = build_command(str(appimage), "in.raw", "out.jpg")
    assert cmd[1] == "darktable-cli"


def test_build_command_plain_binary_unchanged(tmp_path):
    from develop import build_command

    plain = _write_plain_elf(tmp_path / "darktable-cli")
    cmd = build_command(str(plain), "in.raw", "out.jpg")
    assert cmd == [str(plain), "in.raw", "out.jpg"]


def test_build_command_appimage_named_like_anything_keeps_style_and_width(tmp_path):
    """Optional args must land after the positional args, not before."""
    from develop import build_command

    appimage = _write_appimage(tmp_path / "D.AppImage")
    cmd = build_command(str(appimage), "in.raw", "out.jpg", style="Wild", width=2048)
    assert cmd == [str(appimage), "darktable-cli", "in.raw", "out.jpg",
                   "--style", "Wild", "--width", "2048"]


def test_develop_photo_appimage_direct_first_when_fuse_works(tmp_path, monkeypatch):
    """On a system with working FUSE the AppImage must run directly — setting
    APPIMAGE_EXTRACT_AND_RUN would force a ~178 MB squashfs unpack per photo.
    The parent env still has to be carried through (HOME, XDG_*, DISPLAY, …).
    """
    import subprocess

    import develop

    develop._APPIMAGE_MODE_CACHE.clear()
    develop._APPIMAGE_APPRUN_CACHE.clear()

    raw = tmp_path / "bird.NEF"
    raw.touch()
    fake_bin = _write_appimage(tmp_path / "Darktable-5.6.0-x86_64.AppImage")
    out = tmp_path / "out" / "bird.jpg"

    monkeypatch.setenv("VIREO_TEST_INHERITED_VAR", "inherited-value")
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(kwargs.get("env"))
        os.makedirs(os.path.dirname(cmd[-1]), exist_ok=True)
        with open(cmd[-1], "w") as f:
            f.write("jpg")
        return _fake_completed(0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = develop.develop_photo(str(fake_bin), str(raw), str(out))
    assert result["success"] is True
    assert len(calls) == 1, "unconditional retry would defeat the whole point"

    env = calls[0]
    assert env is not None
    assert "APPIMAGE_EXTRACT_AND_RUN" not in env
    assert env["VIREO_TEST_INHERITED_VAR"] == "inherited-value"
    assert set(os.environ) <= set(env)
    # Cached as "direct" so future photos skip the probe.
    assert develop._APPIMAGE_MODE_CACHE[str(fake_bin)] == "direct"


def test_develop_photo_appimage_falls_back_to_persistent_extraction_on_fuse_failure(tmp_path, monkeypatch):
    """When the AppImage runtime fails with a FUSE error, extract the bundle
    once to a persistent tree and route every following photo through the
    extracted AppRun — never APPIMAGE_EXTRACT_AND_RUN. Setting that flag
    would make the runtime unpack the whole ~178 MB squashfs into a temp dir
    for every photo in the batch, which is exactly the cost the persistent
    extraction is here to avoid.
    """
    import subprocess

    import develop

    develop._APPIMAGE_MODE_CACHE.clear()
    develop._APPIMAGE_APPRUN_CACHE.clear()

    raw = tmp_path / "bird.NEF"
    raw.touch()
    fake_bin = _write_appimage(tmp_path / "Darktable-5.6.0-x86_64.AppImage")
    out = tmp_path / "out" / "bird.jpg"

    fuse_stderr = (
        "AppImages require FUSE to run.\n"
        "You might still be able to extract the contents of this AppImage "
        "if you run it with the --appimage-extract option.\n"
    )
    expected_apprun = os.path.join(
        str(fake_bin) + ".extracted", "squashfs-root", "AppRun",
    )

    calls = []

    def fake_run(cmd, **kwargs):
        env = kwargs.get("env") or {}
        calls.append({
            "argv0": cmd[0],
            "argv": list(cmd),
            "extract_flag": env.get("APPIMAGE_EXTRACT_AND_RUN"),
            "cwd": kwargs.get("cwd"),
        })
        if cmd == [str(fake_bin), "--appimage-extract"]:
            # Simulate the AppImage runtime's --appimage-extract producing
            # squashfs-root/AppRun in the current working directory.
            root = os.path.join(kwargs["cwd"], "squashfs-root")
            os.makedirs(root, exist_ok=True)
            apprun = os.path.join(root, "AppRun")
            with open(apprun, "w") as f:
                f.write("#!/bin/sh\nexec /usr/bin/darktable-cli \"$@\"\n")
            os.chmod(apprun, 0o755)
            return _fake_completed(0)
        if cmd[0] == str(fake_bin):
            # First call: simulate the runtime's FUSE failure.
            return _fake_completed(1, stdout="", stderr=fuse_stderr)
        # Extracted AppRun run: write the output and succeed.
        os.makedirs(os.path.dirname(cmd[-1]), exist_ok=True)
        with open(cmd[-1], "w") as f:
            f.write("jpg")
        return _fake_completed(0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = develop.develop_photo(str(fake_bin), str(raw), str(out))
    assert result["success"] is True
    # 1) probe against the AppImage, 2) --appimage-extract, 3) AppRun run.
    assert [c["argv0"] for c in calls] == [
        str(fake_bin), str(fake_bin), expected_apprun,
    ]
    assert calls[1]["argv"] == [str(fake_bin), "--appimage-extract"]
    # APPIMAGE_EXTRACT_AND_RUN must NEVER get set on this path — the point of
    # persistent extraction is to avoid the AppImage runtime unpacking again.
    assert all(c["extract_flag"] is None for c in calls)
    assert develop._APPIMAGE_MODE_CACHE[str(fake_bin)] == "extracted"
    assert develop._APPIMAGE_APPRUN_CACHE[str(fake_bin)] == expected_apprun

    # Second call for the same binary skips both the FUSE probe and the
    # extract subprocess: only the extracted AppRun is invoked. Anything
    # else would still be paying per-photo cost.
    calls.clear()
    out2 = tmp_path / "out" / "bird2.jpg"
    result2 = develop.develop_photo(str(fake_bin), str(raw), str(out2))
    assert result2["success"] is True
    assert [c["argv0"] for c in calls] == [expected_apprun]
    assert calls[0]["extract_flag"] is None


def test_develop_photo_appimage_extract_failure_falls_back_to_per_run_flag(tmp_path, monkeypatch):
    """If the one-time --appimage-extract cannot produce a usable AppRun
    (unwritable dir, subprocess failure, truncated output), develop must
    still succeed by setting APPIMAGE_EXTRACT_AND_RUN=1 on every call. The
    fallback is slower per-photo, but shipping a broken FUSE-less path when
    a working one exists would be worse.
    """
    import subprocess

    import develop

    develop._APPIMAGE_MODE_CACHE.clear()
    develop._APPIMAGE_APPRUN_CACHE.clear()

    raw = tmp_path / "bird.NEF"
    raw.touch()
    fake_bin = _write_appimage(tmp_path / "Darktable-5.6.0-x86_64.AppImage")
    out = tmp_path / "out" / "bird.jpg"

    fuse_stderr = "AppImages require FUSE to run.\n"

    calls = []

    def fake_run(cmd, **kwargs):
        env = kwargs.get("env") or {}
        calls.append({
            "argv": list(cmd),
            "extract_flag": env.get("APPIMAGE_EXTRACT_AND_RUN"),
        })
        if cmd == [str(fake_bin), "--appimage-extract"]:
            # Extraction subprocess "runs" but leaves no AppRun on disk —
            # simulates a truncated / corrupted download or a permission
            # error surfaced only inside the runtime.
            return _fake_completed(1, stdout="", stderr="extract failed")
        if env.get("APPIMAGE_EXTRACT_AND_RUN") != "1":
            return _fake_completed(1, stdout="", stderr=fuse_stderr)
        os.makedirs(os.path.dirname(cmd[-1]), exist_ok=True)
        with open(cmd[-1], "w") as f:
            f.write("jpg")
        return _fake_completed(0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = develop.develop_photo(str(fake_bin), str(raw), str(out))
    assert result["success"] is True
    # probe → extract (fails) → per-run flag fallback
    assert calls[0]["extract_flag"] is None
    assert calls[1]["argv"] == [str(fake_bin), "--appimage-extract"]
    assert calls[2]["extract_flag"] == "1"
    assert develop._APPIMAGE_MODE_CACHE[str(fake_bin)] == "extract-per-run"
    assert str(fake_bin) not in develop._APPIMAGE_APPRUN_CACHE

    # Second call takes the cached fallback path: no re-probe, no re-extract,
    # just the flagged call. Still costs an unpack per photo, but that is
    # explicitly the last-resort cache value.
    calls.clear()
    out2 = tmp_path / "out" / "bird2.jpg"
    result2 = develop.develop_photo(str(fake_bin), str(raw), str(out2))
    assert result2["success"] is True
    assert len(calls) == 1
    assert calls[0]["extract_flag"] == "1"


def test_develop_photo_appimage_extracted_cache_recovers_from_deleted_tree(tmp_path, monkeypatch):
    """If the persistent extraction disappears between photos (user cleared
    tmp, reinstall, etc.), the develop must re-probe rather than silently
    invoking a stale path — pointing subprocess at a missing AppRun would
    surface as a confusing FileNotFoundError instead of a fresh, successful
    extraction.
    """
    import subprocess

    import develop

    develop._APPIMAGE_MODE_CACHE.clear()
    develop._APPIMAGE_APPRUN_CACHE.clear()

    raw = tmp_path / "bird.NEF"
    raw.touch()
    fake_bin = _write_appimage(tmp_path / "Darktable-5.6.0-x86_64.AppImage")
    out = tmp_path / "out" / "bird.jpg"

    # Pre-seed the cache with a path that does not exist on disk.
    ghost_apprun = str(tmp_path / "ghosts" / "squashfs-root" / "AppRun")
    develop._APPIMAGE_MODE_CACHE[str(fake_bin)] = "extracted"
    develop._APPIMAGE_APPRUN_CACHE[str(fake_bin)] = ghost_apprun

    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(list(cmd))
        # Whatever runs now must not be the ghost path.
        assert cmd[0] != ghost_apprun
        # Direct-run probe path succeeds so we finish the first develop
        # without touching FUSE at all.
        os.makedirs(os.path.dirname(cmd[-1]), exist_ok=True)
        with open(cmd[-1], "w") as f:
            f.write("jpg")
        return _fake_completed(0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = develop.develop_photo(str(fake_bin), str(raw), str(out))
    assert result["success"] is True
    # Stale cache entries were purged: mode reset to "direct" after the
    # fresh probe, AppRun path evicted entirely.
    assert develop._APPIMAGE_MODE_CACHE[str(fake_bin)] == "direct"
    assert str(fake_bin) not in develop._APPIMAGE_APPRUN_CACHE


def test_develop_photo_appimage_extracted_cache_reprobes_when_source_is_newer(tmp_path, monkeypatch):
    """When the source AppImage is replaced in place (in-place Vireo update,
    manual overwrite), the cached extraction points at the OLD darktable
    version. Reusing it silently would keep running the old build until the
    process restarts — same failure mode _extract_appimage_once already
    guards against with its source-fingerprint marker. Verify the
    develop_photo cache branch checks that marker too.
    """
    import subprocess

    import develop

    develop._APPIMAGE_MODE_CACHE.clear()
    develop._APPIMAGE_APPRUN_CACHE.clear()

    raw = tmp_path / "bird.NEF"
    raw.touch()
    fake_bin = _write_appimage(tmp_path / "Darktable-5.6.0-x86_64.AppImage")
    out = tmp_path / "out" / "bird.jpg"

    # Pre-seed the cache with a real, executable AppRun alongside a
    # source-fingerprint marker recording the CURRENT source. That means
    # only a follow-up change to the source can invalidate the tree — the
    # exists/exec checks all pass, so any eviction has to come from the
    # marker no longer matching.
    stale_apprun_dir = tmp_path / "old_extraction" / "squashfs-root"
    stale_apprun_dir.mkdir(parents=True)
    stale_apprun = stale_apprun_dir / "AppRun"
    stale_apprun.write_text("#!/bin/sh\nexec /old/darktable-cli \"$@\"\n")
    stale_apprun.chmod(0o755)
    marker = stale_apprun_dir.parent / develop._APPIMAGE_SOURCE_MARKER
    marker.write_text(develop._source_fingerprint(str(fake_bin)))

    # Now simulate the in-place replacement — bump the source's mtime past
    # what the marker recorded. The marker no longer matches the source,
    # and the cached tree must be evicted.
    future = os.path.getmtime(str(fake_bin)) + 3600
    os.utime(str(fake_bin), (future, future))

    develop._APPIMAGE_MODE_CACHE[str(fake_bin)] = "extracted"
    develop._APPIMAGE_APPRUN_CACHE[str(fake_bin)] = str(stale_apprun)

    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(list(cmd))
        # The stale AppRun must not be invoked; the cache should have been
        # evicted and the re-probe should take the direct path.
        assert cmd[0] != str(stale_apprun)
        os.makedirs(os.path.dirname(cmd[-1]), exist_ok=True)
        with open(cmd[-1], "w") as f:
            f.write("jpg")
        return _fake_completed(0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = develop.develop_photo(str(fake_bin), str(raw), str(out))
    assert result["success"] is True
    # The stale cache entries were cleared and the re-probe succeeded direct.
    assert develop._APPIMAGE_MODE_CACHE[str(fake_bin)] == "direct"
    assert str(fake_bin) not in develop._APPIMAGE_APPRUN_CACHE


def test_develop_photo_appimage_non_fuse_failure_is_not_retried(tmp_path, monkeypatch):
    """A darktable-side failure (bad RAW, missing style, etc.) must NOT be
    retried under APPIMAGE_EXTRACT_AND_RUN — that would waste a full squashfs
    unpack on an error the fallback cannot fix, and surface the wrong error.
    """
    import subprocess

    import develop

    develop._APPIMAGE_MODE_CACHE.clear()
    develop._APPIMAGE_APPRUN_CACHE.clear()

    raw = tmp_path / "bird.NEF"
    raw.touch()
    fake_bin = _write_appimage(tmp_path / "Darktable-5.6.0-x86_64.AppImage")
    out = tmp_path / "out" / "bird.jpg"

    calls = []

    def fake_run(cmd, **kwargs):
        env = kwargs.get("env") or {}
        calls.append(env.get("APPIMAGE_EXTRACT_AND_RUN"))
        return _fake_completed(
            1, stdout="[dt_init] ERROR: can't init develop system, aborting.",
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = develop.develop_photo(str(fake_bin), str(raw), str(out))
    assert result["success"] is False
    assert calls == [None], "must not retry when the failure is not FUSE-shaped"
    # Nothing cached — a transient darktable error should not lock the mode.
    assert str(fake_bin) not in develop._APPIMAGE_MODE_CACHE
    assert str(fake_bin) not in develop._APPIMAGE_APPRUN_CACHE


def test_extract_appimage_once_reuses_existing_apprun(tmp_path, monkeypatch):
    """Second call for the same AppImage must NOT re-run the extract
    subprocess: the whole point of the persistent extraction is to make the
    ~178 MB unpack happen exactly once per install.
    """
    import subprocess

    import develop

    fake_bin = _write_appimage(tmp_path / "Darktable-5.6.0-x86_64.AppImage")

    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(list(cmd))
        root = os.path.join(kwargs["cwd"], "squashfs-root")
        os.makedirs(root, exist_ok=True)
        apprun = os.path.join(root, "AppRun")
        with open(apprun, "w") as f:
            f.write("#!/bin/sh\nexec /usr/bin/darktable-cli \"$@\"\n")
        os.chmod(apprun, 0o755)
        return _fake_completed(0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    first = develop._extract_appimage_once(str(fake_bin))
    assert first is not None
    assert os.path.isfile(first)
    assert len(calls) == 1

    # Second call: same AppRun, no new subprocess. The source-fingerprint
    # marker written by the first call still matches the source, so the
    # extraction on disk satisfies the reuse check.
    second = develop._extract_appimage_once(str(fake_bin))
    assert second == first
    assert len(calls) == 1, "must not re-extract when a valid tree is present"


def test_extract_appimage_once_reuses_when_apprun_older_than_source(tmp_path, monkeypatch):
    """--appimage-extract preserves AppRun's embedded build timestamp, so on
    a real download AppRun is OLDER than the freshly downloaded source
    AppImage. Reuse must not depend on AppRun's mtime — the whole batch
    would otherwise re-unpack ~500 MB per photo, defeating the persistent
    extraction. As long as the source-fingerprint marker still matches,
    an older AppRun is reused as-is.
    """
    import subprocess

    import develop

    fake_bin = _write_appimage(tmp_path / "Darktable-5.6.0-x86_64.AppImage")

    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(list(cmd))
        root = os.path.join(kwargs["cwd"], "squashfs-root")
        os.makedirs(root, exist_ok=True)
        apprun = os.path.join(root, "AppRun")
        with open(apprun, "w") as f:
            f.write("#!/bin/sh\nexec /usr/bin/darktable-cli \"$@\"\n")
        os.chmod(apprun, 0o755)
        return _fake_completed(0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    first = develop._extract_appimage_once(str(fake_bin))
    assert first is not None

    # Simulate the real world: AppRun carries darktable's embedded build
    # timestamp (weeks/months old), the source has a fresh download mtime.
    long_ago = os.path.getmtime(str(fake_bin)) - 90 * 86400
    os.utime(first, (long_ago, long_ago))

    second = develop._extract_appimage_once(str(fake_bin))
    assert second == first
    assert len(calls) == 1, (
        "AppRun older than source must NOT trigger a re-extract — the "
        "source-fingerprint marker is what decides reuse"
    )


def test_extract_appimage_once_re_extracts_when_source_is_newer(tmp_path, monkeypatch):
    """When the AppImage is replaced (Vireo installs a newer build), the
    stale extracted tree must be discarded and rebuilt — otherwise a batch
    keeps invoking the old darktable version even after the user upgraded.
    """
    import subprocess

    import develop

    fake_bin = _write_appimage(tmp_path / "Darktable-5.6.0-x86_64.AppImage")

    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(list(cmd))
        root = os.path.join(kwargs["cwd"], "squashfs-root")
        os.makedirs(root, exist_ok=True)
        apprun = os.path.join(root, "AppRun")
        with open(apprun, "w") as f:
            f.write("#!/bin/sh\nexec /usr/bin/darktable-cli \"$@\"\n")
        os.chmod(apprun, 0o755)
        return _fake_completed(0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    first = develop._extract_appimage_once(str(fake_bin))
    assert first is not None

    # Make the source AppImage newer than its extraction.
    future = os.path.getmtime(first) + 10
    os.utime(str(fake_bin), (future, future))

    second = develop._extract_appimage_once(str(fake_bin))
    assert second == first  # same target dir
    assert len(calls) == 2, "newer source must trigger a re-extract"


def test_extract_appimage_once_returns_none_when_subprocess_fails(tmp_path, monkeypatch):
    """A subprocess failure or missing AppRun after --appimage-extract must
    surface as None so the caller can pick the APPIMAGE_EXTRACT_AND_RUN
    fallback, not confidently return a broken path.
    """
    import subprocess

    import develop

    fake_bin = _write_appimage(tmp_path / "Darktable-5.6.0-x86_64.AppImage")

    def fake_run(cmd, **kwargs):
        return _fake_completed(1, stdout="", stderr="oh no")

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert develop._extract_appimage_once(str(fake_bin)) is None
    # The failed extraction dir must be cleaned up so a later successful
    # run does not find half-written state.
    assert not os.path.exists(str(fake_bin) + ".extracted")


def test_extract_appimage_once_serializes_concurrent_calls(tmp_path, monkeypatch):
    """Two concurrent workers for the same binary must not race on the
    shared <binary>.extracted directory: the extraction subprocess runs
    exactly once and both callers get the same valid AppRun.
    """
    import subprocess
    import threading

    import develop

    develop._APPIMAGE_EXTRACT_LOCKS.clear()

    fake_bin = _write_appimage(tmp_path / "Darktable-5.6.0-x86_64.AppImage")

    subprocess_started = threading.Event()
    allow_finish = threading.Event()
    call_count = 0
    call_count_lock = threading.Lock()

    def fake_run(cmd, **kwargs):
        nonlocal call_count
        with call_count_lock:
            call_count += 1
        subprocess_started.set()
        # Hold the "extraction subprocess" open long enough that a second
        # concurrent caller has time to enter _extract_appimage_once. Without
        # the per-binary lock, that second caller would rmtree the extract_dir
        # while this thread's fake_run is still writing into it.
        assert allow_finish.wait(timeout=synchronization_timeout(5.0)), "test lock never released"
        root = os.path.join(kwargs["cwd"], "squashfs-root")
        os.makedirs(root, exist_ok=True)
        apprun = os.path.join(root, "AppRun")
        with open(apprun, "w") as f:
            f.write("#!/bin/sh\nexec /usr/bin/darktable-cli \"$@\"\n")
        os.chmod(apprun, 0o755)
        return _fake_completed(0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    results = {}

    def worker(key):
        results[key] = develop._extract_appimage_once(str(fake_bin))

    t1 = threading.Thread(target=worker, args=("a",))
    t2 = threading.Thread(target=worker, args=("b",))
    t1.start()
    assert subprocess_started.wait(timeout=synchronization_timeout(5.0)), "first extraction never started"
    t2.start()
    # Give the second thread a moment to reach the lock — if the lock is
    # missing, it will race ahead and rmtree the extract_dir the first
    # thread is still populating.
    allow_finish.set()
    t1.join(timeout=synchronization_timeout(5.0))
    t2.join(timeout=synchronization_timeout(5.0))

    assert not t1.is_alive() and not t2.is_alive()
    assert call_count == 1, "extraction subprocess must run exactly once"
    assert results["a"] is not None
    assert results["b"] == results["a"]
    assert os.path.isfile(results["a"])


def test_extract_appimage_once_returns_none_when_disk_is_full(tmp_path, monkeypatch):
    """Extraction must not proceed when the disk cannot fit the unpacked
    tree — the download preflight only reserves ~2x the compressed AppImage
    size, but the extracted tree is ~3.5x. Returning None here lets the
    caller fall back to per-run APPIMAGE_EXTRACT_AND_RUN mode rather than
    half-extracting and failing mid-batch.
    """
    import subprocess

    import develop

    develop._APPIMAGE_EXTRACT_LOCKS.clear()

    fake_bin = _write_appimage(tmp_path / "Darktable-5.6.0-x86_64.AppImage")
    # Pad the AppImage to a size where the space check kicks in on realistic
    # disks; the test then reports free space below that so extraction is
    # rejected before the subprocess is invoked.
    with open(fake_bin, "r+b") as f:
        f.seek(1024 * 1024 - 1)
        f.write(b"\x00")

    ran = []

    def fake_run(cmd, **kwargs):
        ran.append(list(cmd))
        return _fake_completed(0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    # Report free space smaller than the required multiple. shutil.disk_usage
    # returns a named tuple, so a plain object with the same attributes works.
    class _StingyUsage:
        total = 10 * 1024 * 1024
        used = 9 * 1024 * 1024
        free = 100 * 1024  # far below the extraction requirement

    monkeypatch.setattr(develop.shutil, "disk_usage", lambda path: _StingyUsage())

    assert develop._extract_appimage_once(str(fake_bin)) is None
    assert ran == [], "extraction subprocess must not run when disk is full"
    # No half-written extraction dir should have been left behind.
    assert not os.path.exists(str(fake_bin) + ".extracted")


def test_develop_photo_omits_appimage_extract_and_run_for_plain_binary(tmp_path, monkeypatch):
    """A packaged darktable-cli is not an AppImage; the flag must never be
    set for it and the FUSE probe/cache must not touch its binary path."""
    import subprocess

    import develop

    develop._APPIMAGE_MODE_CACHE.clear()
    develop._APPIMAGE_APPRUN_CACHE.clear()

    raw = tmp_path / "bird.NEF"
    raw.touch()
    plain_bin = _write_plain_elf(tmp_path / "darktable-cli")
    out = tmp_path / "out" / "bird.jpg"

    monkeypatch.delenv("APPIMAGE_EXTRACT_AND_RUN", raising=False)
    captured = {}

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        captured["env"] = kwargs.get("env")
        os.makedirs(os.path.dirname(cmd[-1]), exist_ok=True)
        with open(cmd[-1], "w") as f:
            f.write("jpg")
        return _fake_completed(0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    result = develop.develop_photo(str(plain_bin), str(raw), str(out))
    assert result["success"] is True
    assert captured["cmd"] == [str(plain_bin), str(raw), str(out)]
    assert "APPIMAGE_EXTRACT_AND_RUN" not in captured["env"]
    assert set(os.environ) <= set(captured["env"])
    assert str(plain_bin) not in develop._APPIMAGE_MODE_CACHE
    assert str(plain_bin) not in develop._APPIMAGE_APPRUN_CACHE
