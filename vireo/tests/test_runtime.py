import json
import os
import stat


def test_write_runtime_json_atomic_and_locked_down(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")

    from runtime import write_runtime_json

    write_runtime_json(
        port=54321,
        pid=12345,
        version="0.0.1",
        db_path="/tmp/x.db",
        token="tok",
        mode="headless",
    )

    path = tmp_path / ".vireo" / "runtime.json"
    assert path.exists()

    data = json.loads(path.read_text())
    assert data["port"] == 54321
    assert data["pid"] == 12345
    assert data["version"] == "0.0.1"
    assert data["db_path"] == "/tmp/x.db"
    assert data["token"] == "tok"
    assert data["mode"] == "headless"
    assert "started_at" in data  # ISO8601 timestamp

    # chmod 600 — only the user can read the token
    mode = stat.S_IMODE(os.stat(path).st_mode)
    assert mode == 0o600


def test_write_runtime_json_overwrites_existing(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")
    (tmp_path / ".vireo" / "runtime.json").write_text('{"stale": true}')

    from runtime import write_runtime_json

    write_runtime_json(port=1, pid=2, version="v", db_path="/x", token="t", mode="gui")

    data = json.loads((tmp_path / ".vireo" / "runtime.json").read_text())
    assert "stale" not in data
    assert data["port"] == 1


def test_read_runtime_json_returns_dict(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")
    (tmp_path / ".vireo" / "runtime.json").write_text(
        '{"port": 1234, "token": "abc"}'
    )

    from runtime import read_runtime_json

    data = read_runtime_json()
    assert data == {"port": 1234, "token": "abc"}


def test_read_runtime_json_missing_returns_none(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))

    from runtime import read_runtime_json

    assert read_runtime_json() is None


def test_read_runtime_json_malformed_returns_none(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")
    (tmp_path / ".vireo" / "runtime.json").write_text("not json{{{")

    from runtime import read_runtime_json

    assert read_runtime_json() is None


def test_delete_runtime_json_is_idempotent(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    os.makedirs(tmp_path / ".vireo")
    (tmp_path / ".vireo" / "runtime.json").write_text("{}")

    from runtime import delete_runtime_json

    delete_runtime_json()
    assert not (tmp_path / ".vireo" / "runtime.json").exists()
    delete_runtime_json()  # second call should not raise
