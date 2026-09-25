"""pytest plugin: ``--selected-tests FILE`` restricts a run to listed tests,
and ``--shard K/N`` runs one of N slices of whatever is collected.

``scripts/select_tests.py`` writes a selection file with one entry per line:
a repo-relative test *file* (``vireo/tests/test_db.py``) runs in full, a
*node id* (``vireo/tests/test_db.py::test_x[param]``) runs alone. Blank lines
and ``#`` comments are ignored.

Two hooks do the work. ``pytest_ignore_collect`` skips test modules that
contribute nothing to the selection so collection stays fast, and
``pytest_collection_modifyitems`` deselects everything else. Node ids that
no longer exist (a PR renamed or deleted the test) simply match nothing,
and an empty selection exits 0 instead of pytest's "no tests collected" 5.

``--shard K/N`` keeps every Nth collected item starting at the Kth, applied
after the selection, so N parallel CI jobs split one run between them.
Collection order is deterministic, so every job (and every xdist worker in
a job) computes the same slices; interleaving by position spreads each
file's heavy tests across the shards. A shard left with nothing exits 0.

Registered by the repository-root ``conftest.py`` so it applies to both
``tests/`` and ``vireo/tests/``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

SELECTION_KEY = pytest.StashKey["_Selection"]()
SHARD_KEY = pytest.StashKey[tuple[int, int]]()


class _Selection:
    def __init__(self, path: str):
        self.path = path
        self.files: set[str] = set()
        self.ids: set[str] = set()
        for raw in Path(path).read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            (self.ids if "::" in line else self.files).add(line)
        # Modules that must still be collected because one of their tests is
        # listed individually.
        self.id_files = {nodeid.split("::", 1)[0] for nodeid in self.ids}

    def wants_module(self, rel: str) -> bool:
        return rel in self.files or rel in self.id_files

    def wants_item(self, nodeid: str) -> bool:
        return nodeid in self.ids or nodeid.split("::", 1)[0] in self.files


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--selected-tests",
        default=None,
        metavar="FILE",
        help="only run the test files / node ids listed in FILE (see scripts/select_tests.py)",
    )
    parser.addoption(
        "--shard",
        default=None,
        metavar="K/N",
        help="run only the Kth of N interleaved slices of the collected tests (1 <= K <= N)",
    )


def _parse_shard(value: str) -> tuple[int, int]:
    try:
        index, total = (int(part) for part in value.split("/"))
    except ValueError:
        raise pytest.UsageError(f"--shard expects K/N, got {value!r}") from None
    if not 1 <= index <= total:
        raise pytest.UsageError(f"--shard expects 1 <= K <= N, got {value!r}")
    return index, total


def pytest_configure(config: pytest.Config) -> None:
    path = config.getoption("--selected-tests")
    if path:
        config.stash[SELECTION_KEY] = _Selection(path)
    shard = config.getoption("--shard")
    if shard:
        config.stash[SHARD_KEY] = _parse_shard(shard)


def _relative(config: pytest.Config, path: Path) -> str:
    try:
        return path.resolve().relative_to(config.rootpath.resolve()).as_posix()
    except ValueError:
        return path.as_posix()


def pytest_ignore_collect(collection_path: Path, config: pytest.Config) -> bool | None:
    selection = config.stash.get(SELECTION_KEY, None)
    if selection is None or collection_path.is_dir():
        return None
    if collection_path.suffix != ".py" or not collection_path.name.startswith("test_"):
        return None
    if selection.wants_module(_relative(config, collection_path)):
        return None
    return True


def _deselect(config: pytest.Config, items: list[pytest.Item], wanted) -> None:
    keep = [item for index, item in enumerate(items) if wanted(index, item)]
    drop = [item for index, item in enumerate(items) if not wanted(index, item)]
    if drop:
        config.hook.pytest_deselected(items=drop)
        items[:] = keep


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    selection = config.stash.get(SELECTION_KEY, None)
    if selection is not None:
        _deselect(config, items, lambda _index, item: selection.wants_item(item.nodeid))
    shard = config.stash.get(SHARD_KEY, None)
    if shard is not None:
        shard_index, shard_total = shard
        _deselect(
            config, items, lambda index, _item: index % shard_total == shard_index - 1,
        )


def pytest_report_header(config: pytest.Config) -> str | None:
    lines = []
    selection = config.stash.get(SELECTION_KEY, None)
    if selection is not None:
        lines.append(
            f"selected tests: {len(selection.files)} whole files + "
            f"{len(selection.ids)} individual tests from {selection.path}"
        )
    shard = config.stash.get(SHARD_KEY, None)
    if shard is not None:
        lines.append(f"shard: {shard[0]} of {shard[1]}")
    return "\n".join(lines) or None


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    stash = session.config.stash
    if SELECTION_KEY not in stash and SHARD_KEY not in stash:
        return
    if exitstatus == pytest.ExitCode.NO_TESTS_COLLECTED:
        # Every listed test was renamed or removed on this branch (the
        # branch's own test-file changes run separately), or a small
        # selection left this shard empty: a clean "nothing applies", not
        # an error.
        session.exitstatus = pytest.ExitCode.OK
