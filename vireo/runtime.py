"""Runtime discovery file for the Vireo sidecar.

Writes `~/.vireo/runtime.json` so external callers can discover the running
instance (port, auth token, PID). Also provides the single-instance guard.
"""

import json
import os
from datetime import UTC, datetime
from pathlib import Path


def _runtime_path() -> Path:
    return Path(os.path.expanduser("~/.vireo/runtime.json"))


def write_runtime_json(
    *, port: int, pid: int, version: str, db_path: str, token: str, mode: str
) -> None:
    """Atomically write runtime.json with 0600 permissions."""
    path = _runtime_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "port": port,
        "pid": pid,
        "version": version,
        "db_path": db_path,
        "started_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "mode": mode,
        "token": token,
    }
    tmp = path.with_suffix(path.suffix + ".tmp")
    # Write then chmod before replace, so the target is never world-readable.
    tmp.write_text(json.dumps(payload, indent=2))
    os.chmod(tmp, 0o600)
    os.replace(tmp, path)
