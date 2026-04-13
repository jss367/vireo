"""Tests for vireo/model_verify.py — SHA256 verification of model files.

Covers sha256_file, fetch_expected_hashes (HF tree API parsing),
verify_model (on-disk vs expected), verify_if_needed (lazy + cached),
and the .verify_failed sentinel file.
"""
import contextlib
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

    def fake_urlopen(url, timeout=None, context=None):
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

    def fake_urlopen(url, timeout=None, context=None):
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

    def fake_urlopen(url, timeout=None, context=None):
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

    def fake_urlopen(url, timeout=None, context=None):
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

    def fake_urlopen(url, timeout=None, context=None):
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
    """verify_if_needed caches results in module-level dicts/sets. Reset
    between tests so cache state from one test doesn't leak into another."""
    import model_verify
    model_verify._verified_this_process.clear()
    model_verify._verify_error_cache.clear()
    yield
    model_verify._verified_this_process.clear()
    model_verify._verify_error_cache.clear()


def test_verify_if_needed_calls_verify_model_once_per_process(
    tmp_path, monkeypatch
):
    """Second call for the same model_id is a no-op — no re-hashing."""
    import model_verify

    # Pin so we exercise the standard (pinned) path.
    (tmp_path / model_verify.REVISION_FILE).write_text("abc123")

    h = _write_with_hash(tmp_path, "image_encoder.onnx", b"ok" * 100)
    monkeypatch.setattr(
        model_verify,
        "fetch_expected_hashes",
        lambda subdir, revision="main": {"image_encoder.onnx": h},
    )

    call_count = {"n": 0}
    real_verify = model_verify.verify_model

    def counting(model_dir, hf_subdir, revision=None):
        call_count["n"] += 1
        return real_verify(model_dir, hf_subdir, revision=revision)

    monkeypatch.setattr(model_verify, "verify_model", counting)

    model_verify.verify_if_needed("bioclip-vit-b-16", str(tmp_path), "bioclip-vit-b-16")
    model_verify.verify_if_needed("bioclip-vit-b-16", str(tmp_path), "bioclip-vit-b-16")
    assert call_count["n"] == 1


def test_verify_if_needed_raises_and_writes_sentinel_on_mismatch(
    tmp_path, monkeypatch
):
    """On hash mismatch for a pinned install, verify_if_needed raises
    ModelCorruptError and writes a .verify_failed sentinel."""
    import model_verify

    # Pin so we exercise the hard-fail (pinned) path.
    (tmp_path / model_verify.REVISION_FILE).write_text("abc123")

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
    """A failed verification on a pinned install must not populate the
    cache — subsequent calls should re-run verification so that a Repair
    flow can clear the state."""
    import model_verify

    # Pin so we exercise the hard-fail (pinned) path.
    (tmp_path / model_verify.REVISION_FILE).write_text("abc123")

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

    # Pin so we exercise the standard (pinned) path.
    (tmp_path / model_verify.REVISION_FILE).write_text("abc123")

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


# ---------------------------------------------------------------------------
# verify_if_needed — unpinned legacy installs (no .hf_revision)
# ---------------------------------------------------------------------------


def test_unpinned_install_auto_pins_on_success(tmp_path, monkeypatch):
    """Legacy installs (no .hf_revision) are auto-pinned on first
    successful verification so future checks use an immutable revision."""
    import model_verify

    # No .hf_revision — this is a legacy install.
    h = _write_with_hash(tmp_path, "image_encoder.onnx", b"ok" * 100)
    monkeypatch.setattr(
        model_verify,
        "fetch_expected_hashes",
        lambda subdir, revision="main": {"image_encoder.onnx": h},
    )
    monkeypatch.setattr(
        model_verify, "fetch_latest_revision", lambda repo: "newsha456"
    )

    model_verify.verify_if_needed(
        "bioclip-vit-b-16", str(tmp_path), "bioclip-vit-b-16"
    )

    # Model is cached as verified.
    assert "bioclip-vit-b-16" in model_verify._verified_this_process
    # .hf_revision was written — future checks will use the pinned path.
    rev_file = tmp_path / model_verify.REVISION_FILE
    assert rev_file.is_file()
    assert rev_file.read_text() == "newsha456"


def test_unpinned_install_soft_fails_on_mismatch(tmp_path, monkeypatch):
    """Legacy installs that don't match current main get a warning, NOT a
    hard-fail.  This prevents migration regressions where a HuggingFace
    model update would block pipelines for pre-existing installs."""
    import model_verify

    # No .hf_revision — legacy install.
    p = tmp_path / "image_encoder.onnx.data"
    p.write_bytes(b"old version bytes")

    monkeypatch.setattr(
        model_verify,
        "fetch_expected_hashes",
        lambda subdir, revision="main": {"image_encoder.onnx.data": "0" * 64},
    )
    monkeypatch.setattr(
        model_verify, "fetch_latest_revision", lambda repo: "newsha789"
    )

    # Must NOT raise — soft-fail for unpinned installs.
    model_verify.verify_if_needed(
        "bioclip-vit-b-16", str(tmp_path), "bioclip-vit-b-16"
    )

    # No sentinel written (ambiguous mismatch, not proven corruption).
    assert not (tmp_path / model_verify.VERIFY_FAILED_SENTINEL).is_file()
    # No .hf_revision written (mismatch — don't pin to wrong revision).
    assert not (tmp_path / model_verify.REVISION_FILE).is_file()
    # Cached so we don't re-warn on every pipeline start.
    assert "bioclip-vit-b-16" in model_verify._verified_this_process


def test_unpinned_install_fails_open_on_network_error(tmp_path, monkeypatch):
    """If fetch_latest_revision fails for an unpinned install, fail open
    just like the pinned path does for verify_model errors."""
    import model_verify

    # No .hf_revision — legacy install.
    (tmp_path / "image_encoder.onnx").write_bytes(b"data")

    monkeypatch.setattr(
        model_verify,
        "fetch_latest_revision",
        lambda repo: (_ for _ in ()).throw(model_verify.VerifyError("offline")),
    )

    # Should not raise.
    model_verify.verify_if_needed(
        "bioclip-vit-b-16", str(tmp_path), "bioclip-vit-b-16"
    )
    assert "bioclip-vit-b-16" in model_verify._verify_error_cache


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

    def fake_verify_model(model_dir, hf_subdir, revision=None):
        if "good" in hf_subdir:
            return model_verify.VerifyResult(ok=True)
        return model_verify.VerifyResult(ok=False, mismatches=["weights"])

    import models
    monkeypatch.setattr(models, "get_models", lambda: fake_models)
    monkeypatch.setattr(model_verify, "verify_model", fake_verify_model)
    # Stub fetch_latest_revision — called for auto-pin on success.
    monkeypatch.setattr(
        model_verify, "fetch_latest_revision", lambda repo: "autopin123"
    )

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
    # Pin so the test exercises the pinned VerifyError path.
    (good_dir / model_verify.REVISION_FILE).write_text("abc123")
    fake_models = [{
        "id": "good-model",
        "state": "ok",
        "downloaded": True,
        "weights_path": str(good_dir),
        "hf_subdir": "good-model",
        "source": "hf-hub:test",
    }]

    def raise_verify_error(d, s, revision=None):
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


def test_verify_all_models_writes_sentinel_on_pinned_mismatch(tmp_path, monkeypatch):
    """A pinned model with a mismatch gets a .verify_failed sentinel so the
    Settings UI surfaces Repair."""
    import model_verify

    bad_dir = tmp_path / "bad-model"
    bad_dir.mkdir()
    # Pin so we exercise the hard-fail path.
    (bad_dir / model_verify.REVISION_FILE).write_text("abc123")
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
        lambda d, s, revision=None: model_verify.VerifyResult(
            ok=False, mismatches=["weights"]
        ),
    )
    import models
    monkeypatch.setattr(models, "get_models", lambda: fake_models)

    model_verify.verify_all_models()
    assert (bad_dir / model_verify.VERIFY_FAILED_SENTINEL).is_file()


def test_verify_all_models_no_sentinel_on_unpinned_mismatch(tmp_path, monkeypatch):
    """An unpinned legacy install with a mismatch must NOT get a .verify_failed
    sentinel — the mismatch could be version drift, not corruption."""
    import model_verify

    bad_dir = tmp_path / "bad-model"
    bad_dir.mkdir()
    # No .hf_revision — legacy install.
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
        lambda d, s, revision=None: model_verify.VerifyResult(
            ok=False, mismatches=["weights"]
        ),
    )
    monkeypatch.setattr(
        model_verify, "fetch_latest_revision", lambda repo: "rev999"
    )
    import models
    monkeypatch.setattr(models, "get_models", lambda: fake_models)

    results = model_verify.verify_all_models()
    # Result is reported to caller (mismatch).
    assert results["bad-model"].ok is False
    # No sentinel written — ambiguous mismatch for unpinned install.
    assert not (bad_dir / model_verify.VERIFY_FAILED_SENTINEL).exists()
    # No .hf_revision written — don't pin on mismatch.
    assert not (bad_dir / model_verify.REVISION_FILE).exists()


# ---------------------------------------------------------------------------
# verify_if_needed — VerifyError failure backoff (Codex P2 on #501 line 207)
# ---------------------------------------------------------------------------

def test_verify_if_needed_catches_verify_error_and_fails_open(tmp_path, monkeypatch):
    """When verify_model raises VerifyError (network/HTTP failure), verify_if_needed
    must NOT write .verify_failed and must NOT raise — it should fail open and
    let the pipeline proceed, logging the failure for later retry."""
    import model_verify

    # Pin so we exercise the standard (pinned) path.
    (tmp_path / model_verify.REVISION_FILE).write_text("abc123")

    def always_raises(model_dir, hf_subdir, revision=None):
        raise model_verify.VerifyError("network timeout")

    monkeypatch.setattr(model_verify, "verify_model", always_raises)

    # Should return without raising.
    model_verify.verify_if_needed("bioclip-vit-b-16", str(tmp_path), "bioclip-vit-b-16")

    # No sentinel written (network failure ≠ corruption).
    assert not (tmp_path / model_verify.VERIFY_FAILED_SENTINEL).is_file()
    # Error time was recorded in the failure cache.
    assert "bioclip-vit-b-16" in model_verify._verify_error_cache


def test_verify_if_needed_skips_network_within_ttl_after_verify_error(tmp_path, monkeypatch):
    """After a VerifyError, subsequent calls within _VERIFY_ERROR_TTL must skip
    the network check entirely — no call to verify_model at all.  This avoids
    repeated 30-second stalls on every pipeline start when HF is unreachable."""
    import model_verify

    # Pin so we exercise the standard (pinned) path.
    (tmp_path / model_verify.REVISION_FILE).write_text("abc123")

    call_count = {"n": 0}

    def counting_verify(model_dir, hf_subdir, revision=None):
        call_count["n"] += 1
        raise model_verify.VerifyError("still down")

    monkeypatch.setattr(model_verify, "verify_model", counting_verify)

    # First call: hits network, records failure.
    model_verify.verify_if_needed("bioclip-vit-b-16", str(tmp_path), "bioclip-vit-b-16")
    assert call_count["n"] == 1

    # Second call within TTL: must NOT call verify_model again.
    model_verify.verify_if_needed("bioclip-vit-b-16", str(tmp_path), "bioclip-vit-b-16")
    assert call_count["n"] == 1, (
        "verify_model was called again within TTL window — this would cause "
        "repeated network stalls within the same process on failing networks"
    )


def test_verify_if_needed_retries_after_error_ttl_expires(tmp_path, monkeypatch):
    """After _VERIFY_ERROR_TTL seconds the failure entry expires and the next
    call retries the network check (so we don't suppress errors forever)."""
    import model_verify

    # Pin so we exercise the standard (pinned) path.
    (tmp_path / model_verify.REVISION_FILE).write_text("abc123")

    call_count = {"n": 0}

    def counting_verify(model_dir, hf_subdir, revision=None):
        call_count["n"] += 1
        raise model_verify.VerifyError("timeout")

    monkeypatch.setattr(model_verify, "verify_model", counting_verify)

    # Seed a stale error timestamp that is already past the TTL.
    import time as _time
    model_verify._verify_error_cache["bioclip-vit-b-16"] = (
        _time.monotonic() - model_verify._VERIFY_ERROR_TTL - 1
    )

    model_verify.verify_if_needed("bioclip-vit-b-16", str(tmp_path), "bioclip-vit-b-16")
    assert call_count["n"] == 1, (
        "Expected verify_model to be called once after TTL expiry but it was not"
    )


def test_clear_verified_cache_also_clears_error_cache(tmp_path, monkeypatch):
    """clear_verified_cache must evict the model from both caches so that
    a fresh download triggers a clean re-verification on the next pipeline run."""
    import model_verify

    model_verify._verify_error_cache["bioclip-vit-b-16"] = 0.0  # stale entry
    model_verify.clear_verified_cache("bioclip-vit-b-16")
    assert "bioclip-vit-b-16" not in model_verify._verify_error_cache


# ---------------------------------------------------------------------------
# download_model — revision pin update when verification is skipped (Codex P1
# on #501 line 517)
# ---------------------------------------------------------------------------

def test_download_model_writes_revision_when_hash_fetch_fails(tmp_path, monkeypatch):
    """If fetch_latest_revision succeeds but fetch_expected_hashes fails (tree
    API outage), download_model must still write .hf_revision with the
    pinned SHA so that a later verify_model call fetches hashes against the
    correct (immutable) revision instead of 'main' or a stale pin."""
    import os
    import sys
    import types

    import model_verify
    import models as models_mod

    # Reuse the helper from test_models.py's environment patch.
    monkeypatch.setattr(models_mod, "CONFIG_PATH", str(tmp_path / "models.json"))
    monkeypatch.setattr(models_mod, "DEFAULT_MODELS_DIR", str(tmp_path / "models"))

    # Stub huggingface_hub so the ImportError guard in download_model passes.
    hf_stub = types.ModuleType("huggingface_hub")
    hf_stub.hf_hub_download = None
    hf_stub.try_to_load_from_cache = None
    monkeypatch.setitem(sys.modules, "huggingface_hub", hf_stub)

    pinned_sha = "abc123" * 6 + "abcd"  # 40-char SHA-like

    monkeypatch.setattr(
        model_verify, "fetch_latest_revision", lambda repo: pinned_sha
    )

    def hash_fetch_fails(subdir, revision="main"):
        raise model_verify.VerifyError("tree API offline")

    monkeypatch.setattr(model_verify, "fetch_expected_hashes", hash_fetch_fails)

    def fake_download(repo_id, filename, local_dir, subfolder=None,
                      progress_callback=None, revision=None):
        os.makedirs(local_dir, exist_ok=True)
        dest = os.path.join(local_dir, filename)
        with open(dest, "wb") as f:
            f.write(b"stub")
        return dest

    monkeypatch.setattr(models_mod, "_purge_hf_cache_file", lambda f, s, revision=None: None)
    monkeypatch.setattr(models_mod, "_hf_download_with_retry", fake_download)
    # Bypass the size-floor check so this test stays focused on revision-pin
    # semantics, not stub-file detection.
    monkeypatch.setattr(models_mod, "_MIN_BINARY_MODEL_BYTES", 0)

    model_dir = tmp_path / "models" / "bioclip-vit-b-16"

    models_mod.download_model("bioclip-vit-b-16")

    rev_file = model_dir / model_verify.REVISION_FILE
    assert rev_file.is_file(), (
        ".hf_revision must be written even when hash fetch fails so that the "
        "next verify_model call uses the correct immutable revision rather than "
        "'main' or a stale pin from a previous install."
    )
    assert rev_file.read_text().strip() == pinned_sha


def test_download_model_clears_stale_revision_when_both_apis_fail(tmp_path, monkeypatch):
    """When both fetch_latest_revision AND fetch_expected_hashes fail,
    download_model must DELETE any existing .hf_revision so that the next
    verify_model falls back to 'main' rather than reading a stale SHA that
    would cause false mismatch failures for files that are actually correct."""
    import os
    import sys
    import types

    import model_verify
    import models as models_mod

    monkeypatch.setattr(models_mod, "CONFIG_PATH", str(tmp_path / "models.json"))
    monkeypatch.setattr(models_mod, "DEFAULT_MODELS_DIR", str(tmp_path / "models"))

    # Stub huggingface_hub so the ImportError guard in download_model passes.
    hf_stub = types.ModuleType("huggingface_hub")
    hf_stub.hf_hub_download = None
    hf_stub.try_to_load_from_cache = None
    monkeypatch.setitem(sys.modules, "huggingface_hub", hf_stub)

    model_dir = tmp_path / "models" / "bioclip-vit-b-16"
    model_dir.mkdir(parents=True)

    # Pre-existing stale revision pin from a previous install.
    stale_rev = model_dir / model_verify.REVISION_FILE
    stale_rev.write_text("stalesha1234567890abcdef1234567890abcdef")

    def both_fail(*args, **kwargs):
        raise model_verify.VerifyError("offline")

    monkeypatch.setattr(model_verify, "fetch_latest_revision", both_fail)
    monkeypatch.setattr(model_verify, "fetch_expected_hashes", both_fail)

    def fake_download(repo_id, filename, local_dir, subfolder=None,
                      progress_callback=None, revision=None):
        os.makedirs(local_dir, exist_ok=True)
        dest = os.path.join(local_dir, filename)
        with open(dest, "wb") as f:
            f.write(b"stub")
        return dest

    monkeypatch.setattr(models_mod, "_purge_hf_cache_file", lambda f, s, revision=None: None)
    monkeypatch.setattr(models_mod, "_hf_download_with_retry", fake_download)

    # download_model may raise (size-floor check fires on stub files, or
    # state check sees missing files). Either way we only care that the
    # stale .hf_revision was deleted before the exception.
    with contextlib.suppress(RuntimeError):
        models_mod.download_model("bioclip-vit-b-16")

    assert not stale_rev.exists(), (
        "Stale .hf_revision must be deleted when both revision and hash APIs "
        "fail. Leaving it would cause verify_model to fetch expected hashes "
        "for the old SHA and report false mismatches on files that are "
        "actually correct but from a different (newer) revision."
    )
