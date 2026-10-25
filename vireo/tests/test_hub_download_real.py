"""Opt-in storage regression against the real Hugging Face client and service.

Run with VIREO_REAL_HUB_TEST=1. A pinned, public 540 KB test model is fetched
into fresh temporary caches. The historical cache-then-copy flow acts as a
positive control for detecting a second retained model payload. No inference
is run and no user model/cache directory is read or modified.
"""
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

_DOWNLOAD = r'''
import hashlib
import importlib.metadata
import json
import shutil
import sys
from pathlib import Path

sys.path.insert(0, sys.argv[1])
root = Path(sys.argv[2]).resolve()
legacy = sys.argv[3] == 'legacy'
from huggingface_hub import hf_hub_download
from hub_download import STAGING_DIRNAME, fetch_hub_file
from services.hf_cache_lock import DOWNLOAD_LOCK

DOWNLOAD_LOCK.lock_path = str(root / 'download.lock')
repo = 'hf-internal-testing/tiny-random-bert'
revision = 'f171d7baecaf37b5da5a3616d8833b9969753535'
filename = 'pytorch_model.bin'
expected_sha256 = '9922e8996d0c7e24c7f4e7a5d9c5b7303549f4ee94de0f1138b103014b51be13'
dest = root / 'models' / filename
dest.parent.mkdir(parents=True)
if legacy:
    cached = hf_hub_download(repo_id=repo, filename=filename, revision=revision)
    shutil.copy2(cached, dest)
else:
    fetch_hub_file(repo, filename, str(dest), revision=revision)
assert dest.stat().st_size == 540217
assert hashlib.sha256(dest.read_bytes()).hexdigest() == expected_sha256
assert not (dest.parent / STAGING_DIRNAME).exists()
# Resolve snapshot symlinks so the hub's link and its blob are counted once.
payloads = sorted({path.resolve() for path in root.rglob('*')
                   if path.is_file() and path.stat().st_size == dest.stat().st_size
                   and hashlib.sha256(path.read_bytes()).hexdigest() == expected_sha256})
print(json.dumps({'payload_paths': [p.relative_to(root).as_posix() for p in payloads],
                  'payload_bytes': sum(p.stat().st_size for p in payloads),
                  'client_version': importlib.metadata.version('huggingface_hub')}))
'''


def _download_flow(root, *, legacy):
    env = dict(os.environ)
    env.update({
        "HF_HOME": str(root / "hf"), "HF_HUB_CACHE": str(root / "hub"),
        "HF_XET_CACHE": str(root / "xet"), "HF_ASSETS_CACHE": str(root / "assets"),
        "XDG_CACHE_HOME": str(root / "cache"), "HF_TOKEN_PATH": str(root / "token"),
        "HF_HUB_DISABLE_TELEMETRY": "1",
        "HF_HUB_DISABLE_IMPLICIT_TOKEN": "1", "HF_HUB_ETAG_TIMEOUT": "20",
        "HF_HUB_DOWNLOAD_TIMEOUT": "60", "HF_HUB_OFFLINE": "0",
        "HF_ENDPOINT": "https://huggingface.co",
    })
    # Hub cache constants are resolved at import time. A child process is
    # necessary to isolate them from the rest of pytest and the user's cache.
    result = subprocess.run(
        [sys.executable, "-c", _DOWNLOAD, str(Path(__file__).resolve().parents[1]),
         str(root), "legacy" if legacy else "direct"],
        env=env, capture_output=True, text=True, timeout=120,
    )
    assert result.returncode == 0, result.stderr[-8000:]
    return json.loads(result.stdout.strip().splitlines()[-1])


@pytest.mark.skipif(os.environ.get("VIREO_REAL_HUB_TEST") != "1", reason="Set VIREO_REAL_HUB_TEST=1 for the network storage check")
def test_real_download_retains_one_payload_and_detects_the_old_duplicate(tmp_path):
    direct = _download_flow(tmp_path / "direct", legacy=False)
    assert direct["payload_paths"] == ["models/pytorch_model.bin"], direct
    assert direct["payload_bytes"] == 540217, direct
    legacy = _download_flow(tmp_path / "legacy", legacy=True)
    assert len(legacy["payload_paths"]) == 2, legacy
    assert legacy["payload_bytes"] == 2 * direct["payload_bytes"], legacy
