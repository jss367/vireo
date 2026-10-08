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
import shlex
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
            'set -e; ulimit -S -n "$1"; ulimit -H -n "$2"; exec bash "$3" patch',
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


def _git(repo, *args):
    return subprocess.run(
        ["git", "-C", str(repo), *args], check=True,
        capture_output=True, text=True, timeout=10,
    ).stdout.strip()


@pytest.fixture
def release_repo(tmp_path):
    """Real Git refs and pushes, with inexpensive stand-ins for build gates."""
    if sys.platform == "win32":
        pytest.skip("POSIX release script")
    remote = tmp_path / "remote.git"
    repo = tmp_path / "checkout"
    repo.mkdir()
    _git(tmp_path, "init", "--bare", str(remote))
    _git(repo, "init", "-b", "main")
    _git(repo, "config", "user.name", "Release test")
    _git(repo, "config", "user.email", "release@example.test")
    _git(repo, "config", "commit.gpgsign", "false")
    _git(repo, "config", "tag.gpgsign", "false")
    _git(repo, "config", "core.hooksPath", "/dev/null")
    scripts = repo / "scripts"
    scripts.mkdir()
    shutil.copyfile(RELEASE_SH, scripts / "release.sh")
    shutil.copyfile(REPO_ROOT / "scripts/sync_version.py", scripts / "sync_version.py")
    (repo / "src-tauri").mkdir()
    (repo / "pyproject.toml").write_text('[project]\nversion = "1.2.3"\n')
    for path in ["package.json", "src-tauri/tauri.conf.json"]:
        (repo / path).write_text('{"version": "1.2.3"}\n')
    for path in ["src-tauri/Cargo.toml", "src-tauri/Cargo.lock"]:
        (repo / path).write_text('[package]\nname = "vireo"\nversion = "1.2.3"\n')
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "Initial source")
    _git(repo, "remote", "add", "origin", str(remote))
    _git(repo, "push", "-u", "origin", "main")

    writer = tmp_path / "writer"
    _git(tmp_path, "clone", "--branch", "main", str(remote), str(writer))
    _git(writer, "config", "user.name", "Concurrent writer")
    _git(writer, "config", "user.email", "writer@example.test")
    _git(writer, "config", "commit.gpgsign", "false")
    _git(writer, "config", "tag.gpgsign", "false")
    _git(writer, "config", "core.hooksPath", "/dev/null")

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    gates = tmp_path / "gates.log"
    stubs = {
        "caffeinate": "#!/bin/bash\nexit 0\n",
        "cargo": (
            '#!/bin/bash\necho "cargo $*" >> "$GATES_LOG"\n'
            'if [[ "$1" == "update" ]]; then cp Cargo.toml Cargo.lock; fi\n'
        ),
        "python": (
            '#!/bin/bash\n'
            'if [[ "$1" == "-m" && "$2" == "pytest" ]]; then\n'
            '    echo "pytest" >> "$GATES_LOG"\n'
            '    if [[ "${ADVANCE_REMOTE:-}" == "1" ]]; then\n'
            '        git -C "$REMOTE_WRITER" commit --allow-empty -m "Remote advanced during tests"\n'
            '        git -C "$REMOTE_WRITER" push origin main\n'
            '    fi\n'
            '    if [[ "${FAIL_COMMIT:-}" == "1" ]]; then\n'
            '        git config user.useConfigOnly true\n'
            '        git config user.name ""\n'
            '        git config user.email ""\n'
            '    fi\n'
            '    exit 0\n'
            'fi\n'
            f'exec {shlex.quote(sys.executable)} "$@"\n'
        ),
    }
    for name, source in stubs.items():
        path = bin_dir / name
        path.write_text(source)
        path.chmod(0o755)

    env = {
        **os.environ,
        "PATH": str(bin_dir) + os.pathsep + os.environ["PATH"],
        "GATES_LOG": str(gates),
        "REMOTE_WRITER": str(writer),
    }
    return repo, remote, writer, gates, env


def _run_release(fixture, version="patch", **overrides):
    repo, _, _, _, env = fixture
    return subprocess.run(
        ["bash", "scripts/release.sh", version, "--publish"], cwd=repo,
        env={**env, **overrides}, capture_output=True, text=True, timeout=15,
    )


def test_release_syncs_remote_source_before_testing_and_publishes_both_refs(release_repo):
    repo, remote, writer, gates, _ = release_repo
    (writer / "new-source.txt").write_text("Merged before the release\n")
    _git(writer, "add", ".")
    _git(writer, "commit", "-m", "New source before release")
    _git(writer, "push", "origin", "main")

    result = _run_release(release_repo)

    assert result.returncode == 0, result.stderr
    assert (repo / "new-source.txt").is_file()
    assert gates.read_text().splitlines() == ["cargo update --workspace", "cargo check --locked", "pytest"]
    assert _git(remote, "rev-parse", "main") == _git(repo, "rev-parse", "HEAD")
    assert _git(remote, "rev-parse", "v1.2.4") == _git(repo, "rev-parse", "HEAD")
    assert "Tag pushed." in result.stdout


@pytest.mark.parametrize(
    "condition",
    ["dirty", "staged", "branch", "diverged", "local-ahead", "local-tag", "remote-tag"],
)
def test_release_preflight_rejects_unsafe_state_before_version_writes(release_repo, condition):
    repo, remote, writer, gates, _ = release_repo
    if condition == "dirty":
        (repo / "untracked.txt").write_text("Work in progress\n")
    elif condition == "staged":
        (repo / "package.json").write_text('{"version": "1.2.3", "changed": true}\n')
        _git(repo, "add", "package.json")
    elif condition == "branch":
        _git(repo, "checkout", "-b", "feature")
    elif condition == "diverged":
        _git(repo, "commit", "--allow-empty", "-m", "Local work")
        _git(writer, "commit", "--allow-empty", "-m", "Remote work")
        _git(writer, "push", "origin", "main")
    elif condition == "local-ahead":
        # Strictly ahead of origin/main — `git merge --ff-only origin/main`
        # succeeds with "Already up to date", so without an explicit reject
        # the atomic push would publish this unreviewed commit along with
        # the release bump.
        _git(repo, "commit", "--allow-empty", "-m", "Unreviewed local work")
    elif condition == "local-tag":
        _git(repo, "tag", "v1.2.4")
    else:
        _git(writer, "tag", "v1.2.4")
        _git(writer, "push", "origin", "v1.2.4")
    version_before = (repo / "pyproject.toml").read_text()
    remote_before = _git(remote, "show-ref")

    result = _run_release(release_repo)

    assert result.returncode != 0
    assert (repo / "pyproject.toml").read_text() == version_before
    assert not gates.exists()
    assert _git(remote, "show-ref") == remote_before
    assert "Tag pushed." not in result.stdout


def test_release_atomic_push_rejects_remote_race_and_keeps_tested_source(release_repo):
    repo, remote, writer, _, _ = release_repo

    result = _run_release(release_repo, ADVANCE_REMOTE="1")

    assert result.returncode != 0
    assert "local release commit and tag v1.2.4 are retained" in result.stderr
    assert "Do not rerun the version bump or force-push" in result.stderr
    assert "Tag pushed." not in result.stdout
    assert _git(remote, "rev-parse", "main") == _git(writer, "rev-parse", "HEAD")
    assert _git(remote, "tag", "--list") == ""
    tested_commit = _git(repo, "rev-parse", "HEAD")
    assert _git(repo, "rev-parse", "v1.2.4") == tested_commit

    # The printed recovery preserves the tested tag while reconciling main.
    _git(repo, "fetch", "origin", "main")
    _git(repo, "merge", "--no-edit", "origin/main")
    _git(repo, "push", "--atomic", "origin", "HEAD:refs/heads/main", "refs/tags/v1.2.4")
    assert _git(remote, "rev-parse", "v1.2.4") == tested_commit
    assert _git(remote, "rev-parse", "main") == _git(repo, "rev-parse", "HEAD")


def test_release_runs_the_fetched_script_after_sync_advances_main(release_repo):
    """A sync that updates release.sh must not leave the pre-fetch script running.

    The merge updates files on disk, but Bash keeps executing the contents it
    loaded before fetch. If release.sh changed its staging list or its gates,
    those changes would silently never run — the tagged commit would be a
    mixture of fetched manifest edits and stale script logic.
    """
    repo, _, writer, gates, _ = release_repo
    # Add a marker write inside the sync block of the remote release.sh. The
    # local checkout still carries the unmarked copy, so the marker appears
    # in the gates log only when the fetched script is the one executing by
    # the time control reaches that line.
    original = (writer / "scripts" / "release.sh").read_text()
    marker = 'echo "fetched-release-sh" >> "$GATES_LOG"\n'
    patched = original.replace(
        'echo "==> Syncing version..."\n',
        marker + 'echo "==> Syncing version..."\n',
        1,
    )
    assert patched != original
    (writer / "scripts" / "release.sh").write_text(patched)
    _git(writer, "add", "scripts/release.sh")
    _git(writer, "commit", "-m", "Update release script")
    _git(writer, "push", "origin", "main")

    result = _run_release(release_repo)

    assert result.returncode == 0, result.stderr
    assert "fetched-release-sh" in gates.read_text()


def test_release_reexec_skips_sync_so_bash_matches_the_script_on_disk(release_repo):
    """After re-exec, the re-executed process must not fetch and merge again.

    If the re-executed process ran its own sync, a second origin-main advance
    between re-exec and that fetch would update release.sh and sync_version.py
    on disk again, but the one-shot `VIREO_RELEASE_REEXECED` guard would
    suppress another re-exec — Bash would keep running the first fetched
    version while the atomic push read the newer one. The sync therefore only
    runs in the first process; a later remote advance is caught by the atomic
    push's non-fast-forward rejection.
    """
    repo, _, writer, _, _ = release_repo
    # Advance origin so the first process re-execs after fast-forward.
    _git(writer, "commit", "--allow-empty", "-m", "Advance before release")
    _git(writer, "push", "origin", "main")

    result = _run_release(release_repo)

    assert result.returncode == 0, result.stderr
    sync_banners = result.stdout.count("==> Syncing main before release checks...")
    assert sync_banners == 1, result.stdout
    assert "==> Release scripts advanced during sync; re-executing..." in result.stdout
    assert "==> Already synced before re-exec; skipping second sync." in result.stdout


def test_release_reexec_resolves_script_path_before_changing_directories(release_repo, tmp_path):
    """Re-exec after sync must survive invocation by a path relative to an outside CWD.

    `cd "$(dirname "$0")/.."` in the script moves to the repo root, but $0 is
    still spelled relative to the caller's original directory. Without
    resolving the script path first, `exec bash "$0"` would then look for
    `checkout/scripts/release.sh` beneath the repo root and abort the release.
    """
    repo, _, writer, _, env = release_repo
    # Advance origin so the first process re-execs after fast-forward.
    _git(writer, "commit", "--allow-empty", "-m", "Advance before release")
    _git(writer, "push", "origin", "main")
    # Invoke via a path that is only valid relative to tmp_path, not to the
    # repo root the script cds into.
    rel_script = Path(repo.name) / "scripts" / "release.sh"

    result = subprocess.run(
        ["bash", str(rel_script), "patch", "--publish"], cwd=tmp_path,
        env=env, capture_output=True, text=True, timeout=15,
    )

    assert result.returncode == 0, (result.stdout, result.stderr)
    assert "==> Release scripts advanced during sync; re-executing..." in result.stdout
    assert "Tag pushed." in result.stdout


def test_release_does_not_tag_or_push_when_version_commit_fails(release_repo):
    repo, remote, _, _, _ = release_repo
    remote_before = _git(remote, "show-ref")

    result = _run_release(release_repo, FAIL_COMMIT="1")

    assert result.returncode != 0
    assert _git(repo, "tag", "--list") == ""
    assert _git(remote, "show-ref") == remote_before
    assert "Tag pushed." not in result.stdout
