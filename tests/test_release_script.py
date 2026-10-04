"""Guards on the release process.

These tests exist because of the v0.32.3 release failure: `scripts/release.sh`
ran `cargo generate-lockfile`, which re-resolved every third-party crate to the
newest compatible version. That pulled in zune-core 0.5.2 — published three
hours earlier, broken, and yanked 35 minutes later — and the macOS build failed
after the tag had already been pushed.

The ordering assertions matter as much as the presence ones. A compile gate that
runs *after* `git tag` protects nothing: the tag is the point of no return, so a
guard that only checked for the command's existence would still pass while the
protection it describes had been silently lost.
"""
import os
import re
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parent.parent
RELEASE_SH = REPO_ROOT / "scripts" / "release.sh"
CARGO_LOCK = REPO_ROOT / "src-tauri" / "Cargo.lock"
PYPROJECT = REPO_ROOT / "pyproject.toml"

LOCKFILE_SYNC = r"^\s*\(cd src-tauri && cargo update --workspace\)"
DEPENDENCY_CHECK = r"^\s*\(cd src-tauri && cargo check --locked\)"
TAG_COMMAND = r'^\s*git tag "'
PUBLISH_GUARD = r"^if\s+\$PUBLISH\s*;\s*then\s*$"


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX file-descriptor limits")
@pytest.mark.parametrize(
    "soft_limit,hard_limit,expected_limit",
    [(256, 16384, 4096), (8192, 16384, 8192), (256, 1024, None)],
)
def test_release_prepares_inherited_file_limit_before_changing_versions(
    tmp_path, soft_limit, hard_limit, expected_limit,
):
    """Exercise the real preflight; stop at the first would-be version write."""
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    release = scripts / "release.sh"
    shutil.copyfile(RELEASE_SH, release)
    (tmp_path / "pyproject.toml").write_text('version = "1.2.3"\n')
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    python_stub = bin_dir / "python"
    python_stub.write_text(
        '#!/bin/bash\n'
        'echo "child soft=$(ulimit -S -n) hard=$(ulimit -H -n) args=$*"\n'
        'exit 23\n'
    )
    python_stub.chmod(0o755)

    result = subprocess.run(
        [
            "bash", "-c",
            'set -e; ulimit -S -n "$1"; ulimit -H -n "$2"; exec bash "$3" patch --publish',
            "release-test", str(soft_limit), str(hard_limit), str(release),
        ],
        env={**os.environ, "PATH": str(bin_dir) + os.pathsep + os.environ["PATH"]},
        capture_output=True, text=True, timeout=10,
    )

    if expected_limit is None:
        assert result.returncode == 1
        assert "Release requires at least 4096 open files" in result.stderr
        assert "hard limit is 1024" in result.stderr
        assert "Current version:" not in result.stdout
        assert "child soft=" not in result.stdout
    else:
        assert result.returncode == 23, result.stderr
        assert (
            f"child soft={expected_limit} hard={hard_limit} args=scripts/sync_version.py 1.2.4"
            in result.stdout
        )
    assert (tmp_path / "pyproject.toml").read_text() == 'version = "1.2.3"\n'


def _code_lines():
    """release.sh with comments and blanks blanked out, line indices preserved.

    Blanking rather than dropping keeps index comparisons meaningful, and stops
    a command named in a comment from satisfying a presence assertion.
    """
    return [
        "" if (not line.strip() or line.lstrip().startswith("#")) else line
        for line in RELEASE_SH.read_text().splitlines()
    ]


def _sole_index(lines, pattern):
    """Index of the one line matching `pattern`, asserting it is unambiguous."""
    hits = [i for i, line in enumerate(lines) if re.search(pattern, line)]
    assert len(hits) == 1, (
        f"expected exactly one line in scripts/release.sh matching {pattern!r}, "
        f"found {len(hits)} (lines {[i + 1 for i in hits]})"
    )
    return hits[0]


def _publish_block_ranges(lines):
    """(start, end) index pairs for each `if $PUBLISH; then ... fi` block.

    Tracks if/fi nesting so a `$PUBLISH` block containing inner conditionals
    still resolves to its own `fi` rather than the first one encountered.
    """
    ranges = []
    open_blocks = []
    for i, line in enumerate(lines):
        stripped = line.strip()
        if re.match(r"^if\s", stripped):
            open_blocks.append((i, line))
        elif stripped == "fi":
            assert open_blocks, f"unbalanced `fi` at scripts/release.sh:{i + 1}"
            start, opener = open_blocks.pop()
            if re.match(PUBLISH_GUARD, opener):
                ranges.append((start, i))
    assert not open_blocks, "unbalanced `if` in scripts/release.sh"
    return ranges


def test_release_does_not_regenerate_the_whole_lockfile():
    """The version bump must not re-resolve third-party crates.

    `cargo update --workspace` rewrites only the `vireo` entry;
    `cargo generate-lockfile` rewrites everything.
    """
    lines = _code_lines()
    offenders = [
        i + 1 for i, line in enumerate(lines) if "cargo generate-lockfile" in line
    ]
    assert not offenders, (
        "scripts/release.sh must not run `cargo generate-lockfile` — it bumps "
        "every dependency to the newest compatible version at tag time, with no "
        f"CI run in between. Use `cargo update --workspace`. Lines: {offenders}"
    )
    _sole_index(lines, LOCKFILE_SYNC)


def test_lockfile_sync_runs_before_tagging():
    """A lock synced after tagging would not be in the tagged commit."""
    lines = _code_lines()
    assert _sole_index(lines, LOCKFILE_SYNC) < _sole_index(lines, TAG_COMMAND), (
        "`cargo update --workspace` must run before `git tag` so the tagged "
        "commit contains the synced Cargo.lock"
    )


def test_dependency_check_gates_the_tag():
    """The compile gate is worthless unless it can still stop the tag.

    The publish path builds nothing locally, so this is the only thing standing
    between a broken dependency and a pushed tag.
    """
    lines = _code_lines()
    check = _sole_index(lines, DEPENDENCY_CHECK)
    tag = _sole_index(lines, TAG_COMMAND)

    assert check < tag, (
        "`cargo check --locked` must run before `git tag` — after the tag is "
        "pushed it cannot prevent a broken release, which is the whole point"
    )

    blocks = _publish_block_ranges(lines)
    assert blocks, "no `if $PUBLISH; then` block found in scripts/release.sh"
    assert any(start < check < end for start, end in blocks), (
        "`cargo check --locked` must sit inside an `if $PUBLISH; then` block. "
        "The non-publish path already does a full local build, so running it "
        "unconditionally just duplicates that compile."
    )


def test_cargo_lock_version_matches_pyproject():
    """A stale Cargo.lock means CI has to re-resolve during a tagged build."""
    expected = tomllib.loads(PYPROJECT.read_text())["project"]["version"]

    lock = CARGO_LOCK.read_text()
    match = re.search(
        r'\[\[package\]\]\nname = "vireo"\nversion = "([^"]+)"', lock
    )
    assert match, "no `vireo` package entry found in src-tauri/Cargo.lock"
    assert match.group(1) == expected, (
        f"src-tauri/Cargo.lock has vireo v{match.group(1)} but pyproject.toml "
        f"has {expected}. Run `cd src-tauri && cargo update --workspace`."
    )


def test_release_keeps_the_mac_awake_through_e2e():
    """A sleep during the ~30-minute E2E run fails a page load with
    net::ERR_NETWORK_IO_SUSPENDED and aborts the release."""
    lines = _code_lines()
    caffeinate = _sole_index(lines, r"^\s*caffeinate -i -w \$\$ .*&\s*$")
    e2e = _sole_index(lines, r"^\s*python -m pytest tests/e2e/")
    assert caffeinate < e2e


def test_release_e2e_reruns_flakes_like_the_ci_release_gate():
    lines = _code_lines()
    e2e = lines[_sole_index(lines, r"^\s*python -m pytest tests/e2e/")]
    assert "--reruns 2" in e2e
