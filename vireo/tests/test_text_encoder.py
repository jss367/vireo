"""Tests for text_encoder module -- uses mocked ONNX sessions."""
import os
import sys
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
from testing.waits import synchronization_timeout

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def _make_fake_text_session(fake_features):
    """Build a fake ONNX text encoder session that returns fake_features."""
    session = MagicMock()
    mock_input = MagicMock()
    mock_input.name = "input_ids"
    session.get_inputs.return_value = [mock_input]

    def fake_run(output_names, input_dict):
        return [fake_features]

    session.run = fake_run
    return session


def _make_fake_tokenizer():
    """Create a mock tokenizer that returns fake token IDs."""
    tokenizer = MagicMock()

    class FakeEncoding:
        def __init__(self):
            self.ids = list(range(10))

    tokenizer.encode.return_value = FakeEncoding()
    return tokenizer


def test_encode_text_returns_normalized_vector(monkeypatch):
    """encode_text returns a unit-length float32 vector."""
    fake_features = np.random.randn(1, 512).astype(np.float32)

    fake_session = _make_fake_text_session(fake_features)
    fake_tokenizer = _make_fake_tokenizer()

    # Clear the session cache so our mock gets used
    monkeypatch.setattr("text_encoder._session_cache", {})
    monkeypatch.setattr(
        "text_encoder._get_text_session",
        lambda model_str, pretrained_str=None, **_kwargs: (
            fake_session,
            "input_ids",
            fake_tokenizer,
        ),
    )

    from text_encoder import encode_text

    result = encode_text(
        "bird in flight", model_str="ViT-B-16", pretrained_str="/fake/path"
    )
    assert isinstance(result, np.ndarray)
    assert result.dtype == np.float32
    assert abs(np.linalg.norm(result) - 1.0) < 1e-5


def test_encode_text_zero_vector(monkeypatch):
    """encode_text handles zero vector without crashing."""
    fake_features = np.zeros((1, 512), dtype=np.float32)

    fake_session = _make_fake_text_session(fake_features)
    fake_tokenizer = _make_fake_tokenizer()

    monkeypatch.setattr("text_encoder._session_cache", {})
    monkeypatch.setattr(
        "text_encoder._get_text_session",
        lambda model_str, pretrained_str=None, **_kwargs: (
            fake_session,
            "input_ids",
            fake_tokenizer,
        ),
    )

    from text_encoder import encode_text

    result = encode_text(
        "nothing", model_str="ViT-B-16", pretrained_str="/fake/path"
    )
    assert isinstance(result, np.ndarray)
    assert np.linalg.norm(result) == 0.0


def test_encode_text_bounds_interactive_resource_wait(monkeypatch):
    """Interactive text search passes deadline-aware cancel probes.

    Two independent probes: one for the cold-load lookup, another for
    the inference lease. Sharing one deadline would let a slow cold
    load consume the entire budget and reject the already-loaded
    session at ``session.run`` — the first search after startup would
    fail even though its construction lease landed immediately.
    """
    import contextlib

    import pipeline_locks
    import text_encoder

    fake_features = np.ones((1, 512), dtype=np.float32)
    fake_session = _make_fake_text_session(fake_features)
    fake_tokenizer = _make_fake_tokenizer()
    session_lookup_probes = []

    def get_text_session(*_args, cancel_check=None, **_kwargs):
        session_lookup_probes.append(cancel_check)
        return fake_session, "input_ids", fake_tokenizer

    monkeypatch.setattr(
        text_encoder,
        "_get_text_session",
        get_text_session,
    )
    now = [10.0]
    monkeypatch.setattr(text_encoder.time, "monotonic", lambda: now[0])
    observed = []
    inference_probes = []

    @contextlib.contextmanager
    def capture_lease(_session, *, cancel_check=None):
        assert cancel_check is not None
        inference_probes.append(cancel_check)
        # Inference probe must be a fresh deadline, not the same
        # probe object the cold-load received — otherwise a slow
        # cold-load would already have expired the inference deadline.
        assert cancel_check is not session_lookup_probes[0]
        observed.append(cancel_check())
        now[0] += text_encoder._INTERACTIVE_RESOURCE_WAIT_SECONDS
        observed.append(cancel_check())
        yield

    monkeypatch.setattr(
        pipeline_locks, "acquire_inference_resources", capture_lease,
    )

    text_encoder.encode_text("bird", model_str="ViT-B-16")
    assert len(session_lookup_probes) == 1
    assert len(inference_probes) == 1
    assert observed == [False, True]


def test_encode_text_inference_deadline_resets_after_slow_cold_load(monkeypatch):
    """Regression: a cold-load construction that consumes the full
    interactive budget must NOT cause the inference lease to reject
    the already-loaded session — the deadlines are independent.

    Without the reset, a first search after startup where the ONNX
    session took 5+ seconds to construct would fail at
    ``acquire_inference_resources`` with the deadline already expired,
    while an immediate retry from the cached session succeeds. The
    fix starts a fresh 5s budget for the inference wait so we
    measure only actual resource contention, not total setup time.
    """
    import contextlib

    import pipeline_locks
    import text_encoder

    fake_features = np.ones((1, 512), dtype=np.float32)
    fake_session = _make_fake_text_session(fake_features)
    fake_tokenizer = _make_fake_tokenizer()

    now = [10.0]
    monkeypatch.setattr(text_encoder.time, "monotonic", lambda: now[0])

    def get_text_session(*_args, cancel_check=None, **_kwargs):
        # Simulate a slow cold-load: advance the clock by the full
        # interactive budget while inside the load path.
        now[0] += text_encoder._INTERACTIVE_RESOURCE_WAIT_SECONDS + 1.0
        # If the load probe had already fired at this point, callers
        # would have raised. The assertion here proves the load probe
        # itself expired mid-load — but only the load path saw it.
        assert cancel_check() is True, (
            "load probe must have expired after the full budget was "
            "consumed by cold construction"
        )
        return fake_session, "input_ids", fake_tokenizer

    monkeypatch.setattr(
        text_encoder, "_get_text_session", get_text_session,
    )

    infer_probe_at_start = []

    @contextlib.contextmanager
    def capture_lease(_session, *, cancel_check=None):
        # The inference deadline must have been RESET after the cold
        # load. If it shared the load deadline it would already be
        # expired at this call. Assert it starts fresh (returns False).
        infer_probe_at_start.append(cancel_check())
        yield

    monkeypatch.setattr(
        pipeline_locks, "acquire_inference_resources", capture_lease,
    )

    text_encoder.encode_text("bird", model_str="ViT-B-16")

    assert infer_probe_at_start == [False], (
        f"Inference deadline must reset after cold construction; got "
        f"initial probe result {infer_probe_at_start!r}. A stale "
        f"deadline would report True immediately and reject the "
        f"already-loaded session."
    )


def test_encode_text_caching(monkeypatch, tmp_path):
    """_get_text_session caches by model directory."""
    from text_encoder import _get_text_session

    # Clear cache
    monkeypatch.setattr("text_encoder._session_cache", {})

    fake_features = np.random.randn(1, 512).astype(np.float32)
    fake_session = _make_fake_text_session(fake_features)
    fake_tokenizer = _make_fake_tokenizer()

    # Create a fake model directory
    model_dir = tmp_path / "bioclip-vit-b-16"
    model_dir.mkdir(parents=True, exist_ok=True)
    (model_dir / "text_encoder.onnx").write_text("dummy")
    (model_dir / "tokenizer.json").write_text("dummy")

    # Mock the tokenizers module so Tokenizer.from_file returns our fake
    import types

    mock_tokenizers = types.ModuleType("tokenizers")
    mock_tokenizers.Tokenizer = MagicMock()
    mock_tokenizers.Tokenizer.from_file = MagicMock(return_value=fake_tokenizer)

    def cancel_check():
        return False

    with (
        patch("text_encoder._MODELS_ROOT", str(tmp_path)),
        patch(
            "text_encoder.onnx_runtime.create_session",
            return_value=fake_session,
        ) as create_session,
        patch.dict("sys.modules", {"tokenizers": mock_tokenizers}),
    ):
        result1 = _get_text_session("ViT-B-16", cancel_check=cancel_check)
        result2 = _get_text_session("ViT-B-16", cancel_check=cancel_check)

    # Same object returned (cached)
    assert result1 is result2
    create_session.assert_called_once_with(
        str(model_dir / "text_encoder.onnx"), cancel_check=cancel_check,
    )


def test_text_session_cache_wait_honors_cancellation(monkeypatch):
    """A second cold lookup must not block past its interactive deadline.

    The cancel is delayed until the worker has actually reached the
    cache-lock contention loop and probed ``cancel_check`` mid-contention
    — otherwise a blocking, non-cancellable guard that happens to check
    cancellation only on entry (before any lock acquire) could satisfy
    the outcome assertion because the first probe would already see the
    cancel flag. Setting ``cancelled`` only after the mid-loop probe
    fires makes the test verify cancellation during lock contention
    rather than before it.
    """
    import threading

    import text_encoder
    from resource_ledger import ResourceWaitCancelled

    monkeypatch.setattr(text_encoder, "_session_cache", {})
    cancelled = threading.Event()
    finished = threading.Event()
    outcome = []
    contending = threading.Event()
    probe_calls = {"n": 0}

    def probing_cancel_check():
        probe_calls["n"] += 1
        # The first probe fires on guard entry, before any lock acquire.
        # From the second probe onward, the worker has necessarily
        # completed at least one ``acquire(timeout=0.05)`` round-trip
        # against the held lock and is now polling ``cancel_check``
        # during contention. Only signal after that point so the test
        # can be sure the worker is actively contending when the cancel
        # is triggered.
        if probe_calls["n"] >= 2:
            contending.set()
        return cancelled.is_set()

    def lookup():
        try:
            text_encoder._get_text_session(
                "ViT-B-16",
                cancel_check=probing_cancel_check,
            )
        except ResourceWaitCancelled:
            outcome.append("cancelled")
        finally:
            finished.set()

    text_encoder._session_cache_lock.acquire()
    thread = threading.Thread(target=lookup)
    thread.start()
    try:
        assert contending.wait(timeout=synchronization_timeout(2.0)), (
            "worker never polled cancel_check while contending on the "
            "cache lock — either it never entered the guard or it "
            "acquired the lock without blocking"
        )
        cancelled.set()
        # This deadline verifies cancellation latency, not worker scheduling.
        assert finished.wait(timeout=1.0)
        thread.join(timeout=synchronization_timeout(1.0))
        assert not thread.is_alive()
        assert outcome == ["cancelled"]
    finally:
        text_encoder._session_cache_lock.release()
        thread.join(timeout=synchronization_timeout(1.0))


def test_unknown_model_raises(monkeypatch):
    """_get_text_session raises ValueError for unknown model."""
    monkeypatch.setattr("text_encoder._session_cache", {})

    from text_encoder import _get_text_session

    with pytest.raises(ValueError, match="Unknown BioCLIP model"):
        _get_text_session("unknown-model")
