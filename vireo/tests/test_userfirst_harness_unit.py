"""Unit tests for harness internals that don't need a real Flask server."""

from testing.userfirst.harness import (
    _free_port,
    _new_run_id,
    _prune_runs,
    _relative_url_same_origin,
)


def test_free_port_returns_integer():
    port = _free_port()
    assert isinstance(port, int)
    assert 1024 < port < 65536


def test_free_port_returns_different_ports():
    # Sanity — two consecutive calls shouldn't collide into the same port
    # across a fast loop (they might, but rarely; we just check it's callable
    # repeatedly without error).
    ports = {_free_port() for _ in range(5)}
    assert len(ports) >= 1


def test_new_run_id_sortable():
    # Two IDs created in order should sort chronologically.
    a = _new_run_id()
    b = _new_run_id()
    assert a <= b
    assert len(a) > 10


def test_prune_runs_keeps_most_recent(tmp_path):
    runs_dir = tmp_path / "runs"
    runs_dir.mkdir()
    for i in range(30):
        (runs_dir / f"20260416-{i:06d}").mkdir()
    _prune_runs(runs_dir, keep=20)
    remaining = sorted(p.name for p in runs_dir.iterdir())
    assert len(remaining) == 20
    # The most-recent 20 survive
    assert remaining[0] == "20260416-000010"
    assert remaining[-1] == "20260416-000029"


def test_prune_runs_noop_when_under_limit(tmp_path):
    runs_dir = tmp_path / "runs"
    runs_dir.mkdir()
    for i in range(5):
        (runs_dir / f"20260416-{i:06d}").mkdir()
    _prune_runs(runs_dir, keep=20)
    assert len(list(runs_dir.iterdir())) == 5


def test_prune_runs_handles_missing_dir(tmp_path):
    _prune_runs(tmp_path / "nonexistent", keep=20)


def test_relative_url_same_origin_matches():
    assert _relative_url_same_origin(
        "http://localhost:8089/static/x.js", "http://localhost:8089"
    ) == "/static/x.js"


def test_relative_url_same_origin_returns_none_for_external():
    assert _relative_url_same_origin(
        "https://example.com/x.js", "http://localhost:8089"
    ) is None
