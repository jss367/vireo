"""Tests for vireo/model_verify.py — SHA256 verification of model files.

Covers sha256_file, fetch_expected_hashes (HF tree API parsing),
verify_model (on-disk vs expected), verify_if_needed (lazy + cached),
and the .verify_failed sentinel file.
"""
import hashlib
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def test_sha256_file_matches_hashlib(tmp_path):
    """sha256_file of a small file equals hashlib.sha256 of the bytes."""
    import model_verify

    content = b"hello world" * 10000  # ~110 KB
    p = tmp_path / "blob.bin"
    p.write_bytes(content)

    expected = hashlib.sha256(content).hexdigest()
    assert model_verify.sha256_file(str(p)) == expected


# ---------------------------------------------------------------------------
# fetch_expected_hashes — HuggingFace tree API parsing
# ---------------------------------------------------------------------------

# Canned response mirroring /api/models/jss367/vireo-onnx-models/tree/main/bioclip-vit-b-16
# with a mix of LFS and non-LFS files. Non-LFS entries have `oid` (git blob SHA1)
# but no `lfs` field; LFS entries have both, and `lfs.oid` is the content SHA256.
_CANNED_TREE = [
    {
        "type": "file",
        "oid": "bc943b94cb1a1203454465531be02714a56e1b01",
        "size": 230,
        "path": "bioclip-vit-b-16/config.json",
    },
    {
        "type": "file",
        "oid": "155c5e0d3d8eab4ae40cfcbede08acd0d10e895a",
        "size": 1103995,
        "lfs": {
            "oid": "6b6c8297ee53042b833694f1a0260adcdf593392e4c64bdcf30532e10eef9342",
            "size": 1103995,
            "pointerSize": 132,
        },
        "path": "bioclip-vit-b-16/image_encoder.onnx",
    },
    {
        "type": "file",
        "oid": "8536f456e8f810cb65339ca8b6aeb61a2c8861d8",
        "size": 344784896,
        "lfs": {
            "oid": "c0cdb287d84c0e66dcf58f5f4c1e8ba75b5f42bf2b701addfff60b0879ed5bf1",
            "size": 344784896,
            "pointerSize": 134,
        },
        "path": "bioclip-vit-b-16/image_encoder.onnx.data",
    },
    {
        "type": "file",
        "oid": "bd1e367764006ad6a2249b1dcc0b20879c7c211f",
        "size": 3642073,
        "path": "bioclip-vit-b-16/tokenizer.json",
    },
]


class _FakeResponse:
    def __init__(self, payload):
        self._body = json.dumps(payload).encode("utf-8")

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def test_fetch_expected_hashes_returns_lfs_files_only(monkeypatch):
    """fetch_expected_hashes returns a dict of {basename: sha256} containing
    only LFS files. Non-LFS entries (config.json, tokenizer.json) are omitted
    — those are handled by _classify_model_state's file-presence check, not
    hash verification."""
    import model_verify

    captured_url = {}

    def fake_urlopen(url, timeout=None):
        captured_url["url"] = url
        return _FakeResponse(_CANNED_TREE)

    monkeypatch.setattr(model_verify.urllib.request, "urlopen", fake_urlopen)

    result = model_verify.fetch_expected_hashes("bioclip-vit-b-16")

    assert result == {
        "image_encoder.onnx": (
            "6b6c8297ee53042b833694f1a0260adcdf593392e4c64bdcf30532e10eef9342"
        ),
        "image_encoder.onnx.data": (
            "c0cdb287d84c0e66dcf58f5f4c1e8ba75b5f42bf2b701addfff60b0879ed5bf1"
        ),
    }
    assert "bioclip-vit-b-16" in captured_url["url"]
    assert "jss367/vireo-onnx-models" in captured_url["url"]


def test_fetch_expected_hashes_raises_verify_error_on_network_failure(monkeypatch):
    """Network errors are wrapped in VerifyError so callers get a single
    exception type to handle rather than urllib/http internals."""
    import model_verify

    def fake_urlopen(url, timeout=None):
        raise OSError("connection refused")

    monkeypatch.setattr(model_verify.urllib.request, "urlopen", fake_urlopen)

    with pytest.raises(model_verify.VerifyError) as excinfo:
        model_verify.fetch_expected_hashes("bioclip-vit-b-16")
    assert "connection refused" in str(excinfo.value)


def test_fetch_expected_hashes_uses_pinned_revision(monkeypatch):
    """When a revision (commit SHA) is passed, the URL must target that
    revision — not 'main'. Otherwise a post-download upstream change would
    silently flip healthy models to 'incomplete'."""
    import model_verify

    captured_url = {}

    def fake_urlopen(url, timeout=None):
        captured_url["url"] = url
        return _FakeResponse(_CANNED_TREE)

    monkeypatch.setattr(model_verify.urllib.request, "urlopen", fake_urlopen)

    model_verify.fetch_expected_hashes(
        "bioclip-vit-b-16",
        revision="deadbeef1234567890abcdef1234567890abcdef",
    )
    assert "deadbeef1234567890abcdef1234567890abcdef" in captured_url["url"]
    assert "/tree/main/" not in captured_url["url"]


def test_fetch_latest_revision_returns_sha(monkeypatch):
    """fetch_latest_revision returns the current commit SHA on main from
    the HF model-info API. Used by download_model to pin new downloads."""
    import model_verify

    def fake_urlopen(url, timeout=None):
        # HF /api/models/{repo} returns a dict with a `sha` field that is
        # the latest commit on main.
        return _FakeResponse(
            {
                "sha": "ea7d6fbf207d90de6f7b0df3c3d5aef2a971c0ed",
                "id": model_verify.ONNX_REPO,
            }
        )

    monkeypatch.setattr(model_verify.urllib.request, "urlopen", fake_urlopen)

    sha = model_verify.fetch_latest_revision(model_verify.ONNX_REPO)
    assert sha == "ea7d6fbf207d90de6f7b0df3c3d5aef2a971c0ed"


def test_fetch_latest_revision_network_error_raises_verify_error(monkeypatch):
    """Same error wrapping contract as fetch_expected_hashes."""
    import model_verify

    def fake_urlopen(url, timeout=None):
        raise OSError("dns failure")

    monkeypatch.setattr(model_verify.urllib.request, "urlopen", fake_urlopen)

    with pytest.raises(model_verify.VerifyError):
        model_verify.fetch_latest_revision(model_verify.ONNX_REPO)


# ---------------------------------------------------------------------------
# verify_model
# ---------------------------------------------------------------------------


def _write_with_hash(tmp_path, name, content):
    """Write content to tmp_path/name and return its sha256 hex."""
    p = tmp_path / name
    p.write_bytes(content)
    return hashlib.sha256(content).hexdigest()


def test_verify_model_ok(tmp_path, monkeypatch):
    """All on-disk LFS files match expected → ok=True, empty lists."""
    import model_verify

    h1 = _write_with_hash(tmp_path, "image_encoder.onnx", b"graph1" * 1000)
    h2 = _write_with_hash(
        tmp_path, "image_encoder.onnx.data", b"weights1" * 10000
    )

    monkeypatch.setattr(
        model_verify,
        "fetch_expected_hashes",
        lambda subdir, revision="main": {
            "image_encoder.onnx": h1,
            "image_encoder.onnx.data": h2,
        },
    )

    result = model_verify.verify_model(str(tmp_path), "bioclip-vit-b-16")
    assert result.ok is True
    assert result.mismatches == []
    assert result.missing == []


def test_verify_model_detects_mismatch(tmp_path, monkeypatch):
    """Tampered file → ok=False, listed in mismatches."""
    import model_verify

    h1 = _write_with_hash(tmp_path, "image_encoder.onnx", b"graph" * 1000)
    # Write real data, then corrupt it after computing the "expected" hash.
    data_path = tmp_path / "image_encoder.onnx.data"
    data_path.write_bytes(b"weights" * 10000)
    bad_expected = "0" * 64  # not the real hash

    monkeypatch.setattr(
        model_verify,
        "fetch_expected_hashes",
        lambda subdir, revision="main": {
            "image_encoder.onnx": h1,
            "image_encoder.onnx.data": bad_expected,
        },
    )

    result = model_verify.verify_model(str(tmp_path), "bioclip-vit-b-16")
    assert result.ok is False
    assert result.mismatches == ["image_encoder.onnx.data"]
    assert result.missing == []


def test_verify_model_detects_missing(tmp_path, monkeypatch):
    """File in expected but not on disk → ok=False, listed in missing."""
    import model_verify

    h1 = _write_with_hash(tmp_path, "image_encoder.onnx", b"graph" * 1000)
    # image_encoder.onnx.data is in expected but never written.

    monkeypatch.setattr(
        model_verify,
        "fetch_expected_hashes",
        lambda subdir, revision="main": {
            "image_encoder.onnx": h1,
            "image_encoder.onnx.data": "a" * 64,
        },
    )

    result = model_verify.verify_model(str(tmp_path), "bioclip-vit-b-16")
    assert result.ok is False
    assert result.missing == ["image_encoder.onnx.data"]
    assert result.mismatches == []


def test_verify_model_reads_pinned_revision(tmp_path, monkeypatch):
    """verify_model reads .hf_revision from the model dir and passes the
    pinned revision to fetch_expected_hashes. No .hf_revision → defaults
    to 'main' for backwards compatibility with pre-pinning downloads."""
    import model_verify

    h = _write_with_hash(tmp_path, "image_encoder.onnx", b"ok" * 100)
    (tmp_path / model_verify.REVISION_FILE).write_text(
        "ea7d6fbf207d90de6f7b0df3c3d5aef2a971c0ed"
    )

    captured = {}

    def spy(subdir, revision="main"):
        captured["revision"] = revision
        return {"image_encoder.onnx": h}

    monkeypatch.setattr(model_verify, "fetch_expected_hashes", spy)
    model_verify.verify_model(str(tmp_path), "bioclip-vit-b-16")
    assert captured["revision"] == "ea7d6fbf207d90de6f7b0df3c3d5aef2a971c0ed"


def test_verify_model_without_revision_file_uses_main(tmp_path, monkeypatch):
    """Backwards compat: models from before revision pinning have no
    .hf_revision file and fall back to verifying against main."""
    import model_verify

    h = _write_with_hash(tmp_path, "image_encoder.onnx", b"ok" * 100)

    captured = {}

    def spy(subdir, revision="main"):
        captured["revision"] = revision
        return {"image_encoder.onnx": h}

    monkeypatch.setattr(model_verify, "fetch_expected_hashes", spy)
    model_verify.verify_model(str(tmp_path), "bioclip-vit-b-16")
    assert captured["revision"] == "main"


# ---------------------------------------------------------------------------
# verify_if_needed — lazy, per-process cache, sentinel writing
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clear_verify_cache():
    """verify_if_needed caches results in a module-level set. Reset between
    tests so cache state from one test doesn't leak into another."""
    import model_verify
    model_verify._verified_this_process.clear()
    model_verify._verify_failure_timestamps.clear()
    yield
    model_verify._verified_this_process.clear()
    model_verify._verify_failure_timestamps.clear()


def test_verify_if_needed_calls_verify_model_once_per_process(
    tmp_path, monkeypatch
):
    """Second call for the same model_id is a no-op — no re-hashing."""
    import model_verify

    h = _write_with_hash(tmp_path, "image_encoder.onnx", b"ok" * 100)
    monkeypatch.setattr(
        model_verify,
        "fetch_expected_hashes",
        lambda subdir, revision="main": {"image_encoder.onnx": h},
    )

    call_count = {"n": 0}
    real_verify = model_verify.verify_model

    def counting(model_dir, hf_subdir):
        call_count["n"] += 1
        return real_verify(model_dir, hf_subdir)

    monkeypatch.setattr(model_verify, "verify_model", counting)

    model_verify.verify_if_needed("bioclip-vit-b-16", str(tmp_path), "bioclip-vit-b-16")
    model_verify.verify_if_needed("bioclip-vit-b-16", str(tmp_path), "bioclip-vit-b-16")
    assert call_count["n"] == 1


def test_verify_if_needed_raises_and_writes_sentinel_on_mismatch(
    tmp_path, monkeypatch
):
    """On hash mismatch, verify_if_needed raises ModelCorruptError and
    writes a .verify_failed sentinel in the model directory so that
    _classify_model_state can surface the Repair state."""
    import model_verify

    data_path = tmp_path / "image_encoder.onnx.data"
    data_path.write_bytes(b"actual content")

    monkeypatch.setattr(
        model_verify,
        "fetch_expected_hashes",
        lambda subdir, revision="main": {"image_encoder.onnx.data": "0" * 64},
    )

    with pytest.raises(model_verify.ModelCorruptError) as excinfo:
        model_verify.verify_if_needed(
            "bioclip-vit-b-16", str(tmp_path), "bioclip-vit-b-16"
        )

    assert "bioclip-vit-b-16" in str(excinfo.value)
    assert (tmp_path / ".verify_failed").is_file()


def test_verify_if_needed_mismatch_does_not_cache(tmp_path, monkeypatch):
    """A failed verification must not populate the cache — subsequent calls
    should re-run verification so that a Repair flow can clear the state."""
    import model_verify

    p = tmp_path / "image_encoder.onnx.data"
    p.write_bytes(b"wrong")

    monkeypatch.setattr(
        model_verify,
        "fetch_expected_hashes",
        lambda subdir, revision="main": {"image_encoder.onnx.data": "0" * 64},
    )

    with pytest.raises(model_verify.ModelCorruptError):
        model_verify.verify_if_needed(
            "bioclip-vit-b-16", str(tmp_path), "bioclip-vit-b-16"
        )
    # Second call should raise again (not silently succeed from cache).
    with pytest.raises(model_verify.ModelCorruptError):
        model_verify.verify_if_needed(
            "bioclip-vit-b-16", str(tmp_path), "bioclip-vit-b-16"
        )


def test_clear_verified_cache_forces_re_verification(tmp_path, monkeypatch):
    """After download_model finishes re-downloading, it calls
    clear_verified_cache(model_id) so the next model load re-verifies."""
    import model_verify

    h = _write_with_hash(tmp_path, "image_encoder.onnx", b"ok" * 100)
    monkeypatch.setattr(
        model_verify,
        "fetch_expected_hashes",
        lambda subdir, revision="main": {"image_encoder.onnx": h},
    )

    model_verify.verify_if_needed("bioclip-vit-b-16", str(tmp_path), "bioclip-vit-b-16")
    assert "bioclip-vit-b-16" in model_verify._verified_this_process

    model_verify.clear_verified_cache("bioclip-vit-b-16")
    assert "bioclip-vit-b-16" not in model_verify._verified_this_process


def test_verify_if_needed_records_failure_timestamp_on_verify_error(
    tmp_path, monkeypatch
):
    """When verify_model raises VerifyError (network/API failure), the
    timestamp is recorded in _verify_failure_timestamps so subsequent calls
    within the TTL can skip the expensive 30-second hash-fetch."""
    import model_verify

    def raise_verify_error(d, s):
        raise model_verify.VerifyError("API offline")

    monkeypatch.setattr(model_verify, "verify_model", raise_verify_error)

    with pytest.raises(model_verify.VerifyError):
        model_verify.verify_if_needed("bioclip-vit-b-16", str(tmp_path), "bioclip-vit-b-16")

    assert "bioclip-vit-b-16" in model_verify._verify_failure_timestamps


def test_verify_if_needed_skips_rehash_within_failure_ttl(tmp_path, monkeypatch):
    """A VerifyError within the TTL window causes verify_if_needed to return
    immediately (fail-open) without attempting another expensive hash-fetch."""
    import time
    import model_verify

    call_count = {"n": 0}

    def raise_verify_error(d, s):
        call_count["n"] += 1
        raise model_verify.VerifyError("API offline")

    monkeypatch.setattr(model_verify, "verify_model", raise_verify_error)

    # First call: raises VerifyError and records failure timestamp.
    with pytest.raises(model_verify.VerifyError):
        model_verify.verify_if_needed("bioclip-vit-b-16", str(tmp_path), "bioclip-vit-b-16")

    assert call_count["n"] == 1

    # Second call within TTL: returns immediately without calling verify_model.
    model_verify.verify_if_needed("bioclip-vit-b-16", str(tmp_path), "bioclip-vit-b-16")
    assert call_count["n"] == 1  # verify_model not called again


def test_verify_if_needed_retries_after_failure_ttl_expires(tmp_path, monkeypatch):
    """Once the failure TTL expires, verify_if_needed retries verification."""
    import time
    import model_verify

    call_count = {"n": 0}

    def raise_verify_error(d, s):
        call_count["n"] += 1
        raise model_verify.VerifyError("API offline")

    monkeypatch.setattr(model_verify, "verify_model", raise_verify_error)

    # Seed an expired failure timestamp (well past the TTL).
    model_verify._verify_failure_timestamps["bioclip-vit-b-16"] = (
        time.monotonic() - model_verify._VERIFY_FAILURE_TTL - 1
    )

    # Should retry and re-raise (not skip).
    with pytest.raises(model_verify.VerifyError):
        model_verify.verify_if_needed("bioclip-vit-b-16", str(tmp_path), "bioclip-vit-b-16")

    assert call_count["n"] == 1


def test_clear_verified_cache_also_clears_failure_timestamp(tmp_path, monkeypatch):
    """clear_verified_cache resets the failure-backoff entry so a fresh
    download (which just called clear_verified_cache) triggers re-verification
    immediately on next pipeline start instead of waiting out the TTL."""
    import time
    import model_verify

    model_verify._verify_failure_timestamps["bioclip-vit-b-16"] = time.monotonic()

    model_verify.clear_verified_cache("bioclip-vit-b-16")

    assert "bioclip-vit-b-16" not in model_verify._verify_failure_timestamps


# ---------------------------------------------------------------------------
# verify_all_models — iterate installed models for the "Verify all" button
# ---------------------------------------------------------------------------


def test_verify_all_models_reports_per_model_results(tmp_path, monkeypatch):
    """verify_all_models iterates models that report state=='ok' and returns
    {model_id: VerifyResult} for each. Models that are not 'ok' are skipped
    (there's nothing to verify if the files aren't all present)."""
    import model_verify

    # Fake get_models output: one known good, one known bad, one incomplete.
    fake_models = [
        {
            "id": "good-model",
            "state": "ok",
            "downloaded": True,
            "weights_path": str(tmp_path / "good-model"),
            "hf_subdir": "good-model",
            "source": "hf-hub:test",
        },
        {
            "id": "bad-model",
            "state": "ok",
            "downloaded": True,
            "weights_path": str(tmp_path / "bad-model"),
            "hf_subdir": "bad-model",
            "source": "hf-hub:test",
        },
        {
            "id": "incomplete-model",
            "state": "incomplete",
            "downloaded": False,
            "weights_path": str(tmp_path / "incomplete-model"),
            "hf_subdir": "incomplete-model",
            "source": "hf-hub:test",
        },
        {
            "id": "custom-model",
            "state": "ok",
            "downloaded": True,
            "weights_path": str(tmp_path / "custom-model"),
            "source": "custom",
            # no hf_subdir — custom models can't be verified against HF
        },
    ]
    # Make sure the on-disk paths exist so verify_model has somewhere to look.
    for m in fake_models:
        os.makedirs(m["weights_path"], exist_ok=True)

    def fake_verify_model(model_dir, hf_subdir):
        if "good" in hf_subdir:
            return model_verify.VerifyResult(ok=True)
        return model_verify.VerifyResult(ok=False, mismatches=["weights"])

    import models
    monkeypatch.setattr(models, "get_models", lambda: fake_models)
    monkeypatch.setattr(model_verify, "verify_model", fake_verify_model)

    progress_messages: list[str] = []
    result = model_verify.verify_all_models(
        progress_callback=lambda msg: progress_messages.append(msg)
    )

    # good-model and bad-model get verified; incomplete-model and custom-model are skipped.
    assert set(result.keys()) == {"good-model", "bad-model"}
    assert result["good-model"].ok is True
    assert result["bad-model"].ok is False
    # Progress messages were reported for each verified model.
    assert any("good-model" in m for m in progress_messages)
    assert any("bad-model" in m for m in progress_messages)


def test_verify_all_models_skips_sentinel_on_verify_error(tmp_path, monkeypatch):
    """A VerifyError (network/HTTP failure) must NOT write .verify_failed.
    A transient connectivity issue should not permanently reclassify a
    healthy model as 'incomplete' and break pipelines."""
    import model_verify

    good_dir = tmp_path / "good-model"
    good_dir.mkdir()
    fake_models = [{
        "id": "good-model",
        "state": "ok",
        "downloaded": True,
        "weights_path": str(good_dir),
        "hf_subdir": "good-model",
        "source": "hf-hub:test",
    }]

    def raise_verify_error(d, s):
        raise model_verify.VerifyError("connection refused")

    monkeypatch.setattr(model_verify, "verify_model", raise_verify_error)
    import models
    monkeypatch.setattr(models, "get_models", lambda: fake_models)

    results = model_verify.verify_all_models()

    # Result is reported to caller (ok=False with synthetic entry).
    assert "good-model" in results
    assert results["good-model"].ok is False
    assert any("hash fetch failed" in m for m in results["good-model"].missing)
    # No sentinel written — model stays in its current 'ok' state on disk.
    assert not (good_dir / model_verify.VERIFY_FAILED_SENTINEL).exists()


def test_verify_all_models_writes_sentinel_on_mismatch(tmp_path, monkeypatch):
    """A bad model gets a .verify_failed sentinel written so the Settings UI
    surfaces the Repair state after the sweep completes."""
    import model_verify

    bad_dir = tmp_path / "bad-model"
    bad_dir.mkdir()
    fake_models = [{
        "id": "bad-model",
        "state": "ok",
        "downloaded": True,
        "weights_path": str(bad_dir),
        "hf_subdir": "bad-model",
        "source": "hf-hub:test",
    }]

    monkeypatch.setattr(
        model_verify,
        "verify_model",
        lambda d, s: model_verify.VerifyResult(
            ok=False, mismatches=["weights"]
        ),
    )
    import models
    monkeypatch.setattr(models, "get_models", lambda: fake_models)

    model_verify.verify_all_models()
    assert (bad_dir / model_verify.VERIFY_FAILED_SENTINEL).is_file()
