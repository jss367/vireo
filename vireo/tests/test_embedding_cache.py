"""Concurrency and correctness tests for the label embedding cache."""

import json
import os
import sys
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from embedding_cache import (
    EmbeddingCache,
    EmbeddingWaitCancelled,
    build_embedding_identity,
    canonicalize_labels,
    get_embedding_cache_diagnostics,
    identity_digest,
)


def _identity(labels=("bird", "cat"), suffix="a"):
    return {
        "embedding_schema": 2,
        "model_runtime": {
            "family": "test",
            "model_str": f"model-{suffix}",
        },
        "labels": list(labels),
    }


def _payload(label_count=2, value=1):
    return np.full((4, label_count), value, dtype=np.float32)


def test_canonical_labels_match_the_values_sent_to_encoder():
    assert canonicalize_labels([" bird ", "Cat"]) == ["bird", "Cat"]
    with pytest.raises(ValueError, match="empty"):
        canonicalize_labels(["bird", "  "])


def test_complete_identity_changes_with_each_text_side_input(tmp_path):
    model_dir = tmp_path / "model"
    model_dir.mkdir()
    (model_dir / "text_encoder.onnx").write_bytes(b"weights-a")
    (model_dir / "text_encoder.onnx.data").write_bytes(b"external-a")
    (model_dir / "tokenizer.json").write_bytes(b"tokenizer-a")

    def build(**overrides):
        args = {
            "labels": [" bird ", "Cat"],
            "model_str": "model-a",
            "model_dir": str(model_dir),
            "prompt_template_identity": "prompts-a",
            "tokenizer_context_length": 77,
        }
        args.update(overrides)
        return build_embedding_identity(**args)

    baseline = identity_digest(build())
    assert baseline == identity_digest(build(labels=["bird", "Cat"]))
    assert baseline != identity_digest(build(labels=["Cat", "bird"]))
    assert baseline != identity_digest(build(prompt_template_identity="prompts-b"))
    assert baseline != identity_digest(build(tokenizer_context_length=76))
    assert baseline != identity_digest(build(model_str="model-b"))

    (model_dir / "tokenizer.json").write_bytes(b"tokenizer-b")
    assert baseline != identity_digest(build())
    (model_dir / "tokenizer.json").write_bytes(b"tokenizer-a")
    (model_dir / "text_encoder.onnx").write_bytes(b"weights-b")
    assert baseline != identity_digest(build())
    (model_dir / "text_encoder.onnx").write_bytes(b"weights-a")
    (model_dir / "text_encoder.onnx.data").write_bytes(b"external-b")
    assert baseline != identity_digest(build())


def test_pinned_install_does_not_rehash_large_encoder_files(
    tmp_path, monkeypatch,
):
    import embedding_cache

    model_dir = tmp_path / "model"
    model_dir.mkdir()
    (model_dir / ".hf_revision").write_text("immutable-commit")
    (model_dir / "text_encoder.onnx").write_bytes(b"onnx")
    (model_dir / "text_encoder.onnx.data").write_bytes(b"external")
    (model_dir / "tokenizer.json").write_bytes(b"tokenizer")
    hashed = []
    original = embedding_cache._sha256_file

    def record(path):
        hashed.append(os.path.basename(path))
        return original(path)

    monkeypatch.setattr(embedding_cache, "_sha256_file", record)
    build_embedding_identity(
        ["bird"],
        "model-a",
        str(model_dir),
        prompt_template_identity="prompts-a",
        tokenizer_context_length=77,
    )

    assert hashed == ["tokenizer.json"]


def test_equal_key_callers_compute_once_and_waiter_rereads_disk(tmp_path):
    diagnostics_before = get_embedding_cache_diagnostics()
    cache = EmbeddingCache(tmp_path / "cache")
    identity = _identity()
    producer_started = threading.Event()
    release_producer = threading.Event()
    calls = 0
    calls_lock = threading.Lock()

    def compute():
        nonlocal calls
        with calls_lock:
            calls += 1
        producer_started.set()
        assert release_producer.wait(2)
        return _payload()

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(cache.get_or_compute, identity, compute)
        assert producer_started.wait(2)
        second = pool.submit(cache.get_or_compute, identity, compute)
        release_producer.set()
        first_value, _ = first.result(timeout=2)
        second_value, _ = second.result(timeout=2)

    assert calls == 1
    assert np.array_equal(first_value, second_value)
    assert first_value is not second_value
    diagnostics_after = get_embedding_cache_diagnostics()
    assert diagnostics_after["producer_starts"] - diagnostics_before["producer_starts"] == 1
    assert diagnostics_after["producer_publications"] - diagnostics_before["producer_publications"] == 1
    assert diagnostics_after["waiter_joins"] - diagnostics_before["waiter_joins"] == 1
    assert diagnostics_after["single_flight_violations"] == 0


def test_single_flight_diagnostics_observe_actual_compute_overlap(monkeypatch):
    import embedding_cache

    flight_key = ("diagnostics-test", "same-key")
    before = get_embedding_cache_diagnostics()
    # This test deliberately injects an overlap into the independent
    # computation instrumentation. Restore the process-lifetime counters when
    # the test ends so later API tests still observe real application state.
    monkeypatch.setattr(
        embedding_cache,
        "_single_flight_violations",
        before["single_flight_violations"],
    )
    monkeypatch.setattr(
        embedding_cache,
        "_max_concurrent_producers_per_key",
        before["max_concurrent_producers_per_key"],
    )
    embedding_cache._begin_producer_execution(flight_key)
    try:
        embedding_cache._begin_producer_execution(flight_key)
        try:
            during = get_embedding_cache_diagnostics()
            assert during["active_producers"] == before["active_producers"] + 2
            assert during["single_flight_violations"] == (
                before["single_flight_violations"] + 1
            )
            assert during["max_concurrent_producers_per_key"] >= 2
        finally:
            embedding_cache._end_producer_execution(flight_key)
    finally:
        embedding_cache._end_producer_execution(flight_key)

    after = get_embedding_cache_diagnostics()
    assert after["active_producers"] == before["active_producers"]


def test_producer_cancellation_wakes_waiter_and_waiter_takes_over(tmp_path):
    class ClassificationCancelled(RuntimeError):
        pass

    cache = EmbeddingCache(tmp_path / "cache")
    identity = _identity()
    producer_started = threading.Event()
    cancel_producer = threading.Event()
    waiter_computed = threading.Event()

    def cancelled_compute():
        producer_started.set()
        assert cancel_producer.wait(2)
        raise ClassificationCancelled("cancelled")

    def healthy_compute():
        waiter_computed.set()
        return _payload(value=2)

    with ThreadPoolExecutor(max_workers=2) as pool:
        producer = pool.submit(
            cache.get_or_compute, identity, cancelled_compute
        )
        assert producer_started.wait(2)
        waiter = pool.submit(cache.get_or_compute, identity, healthy_compute)
        cancel_producer.set()
        with pytest.raises(ClassificationCancelled):
            producer.result(timeout=2)
        value, _ = waiter.result(timeout=2)

    assert waiter_computed.is_set()
    assert np.array_equal(value, _payload(value=2))


def test_cancelled_waiter_does_not_cancel_shared_producer(tmp_path):
    cache = EmbeddingCache(tmp_path / "cache")
    identity = _identity()
    producer_started = threading.Event()
    release_producer = threading.Event()
    cancel_waiter = threading.Event()

    def compute():
        producer_started.set()
        assert release_producer.wait(2)
        return _payload()

    with ThreadPoolExecutor(max_workers=2) as pool:
        producer = pool.submit(cache.get_or_compute, identity, compute)
        assert producer_started.wait(2)
        waiter = pool.submit(
            cache.get_or_compute,
            identity,
            lambda: pytest.fail("waiter must not compute"),
            cancel_check=cancel_waiter.is_set,
        )
        cancel_waiter.set()
        with pytest.raises(EmbeddingWaitCancelled):
            waiter.result(timeout=2)
        release_producer.set()
        value, _ = producer.result(timeout=2)

    assert np.array_equal(value, _payload())


def test_mismatched_embedding_dim_rejects_cache_hit(tmp_path):
    """Regression: a malformed cache file whose feature axis does not match
    the model's expected embedding dim used to pass rank + label-axis
    validation and load, then blow up at inference on
    ``img_features @ txt_embeddings``. Passing ``embedding_dim`` must
    reject that file so the caller falls back to recomputing."""
    cache = EmbeddingCache(tmp_path / "cache")
    identity = _identity()

    # Publish a malformed payload — shape (1, 2) instead of (512, 2).
    malformed = np.ones((1, 2), dtype=np.float32)
    cache.get_or_compute(identity, lambda: malformed)

    # A cache-hit check that knows the expected dim must reject it.
    assert not cache.is_cached(identity, 2, embedding_dim=512)
    # And a fresh get_or_compute with the right dim must recompute rather
    # than serve the malformed file.
    computed = np.ones((512, 2), dtype=np.float32)
    value, _ = cache.get_or_compute(
        identity, lambda: computed, embedding_dim=512,
    )
    assert value.shape == (512, 2)
    assert cache.is_cached(identity, 2, embedding_dim=512)


def test_healed_producer_handoff_skips_waiter_embedding_dim(tmp_path):
    """Regression: when a shared producer self-heals to a different embedding
    width, the waiter's stale ``embedding_dim`` used to be applied to the
    healed payload's hand-off ``_load``. That raised ``ValueError`` before
    the waiter could return the healed identity — so the caller (Classifier)
    never reached ``identity_before != identity_after`` and could not rebuild
    its image side. Worse, ``_load``'s cleanup then unlinked the freshly
    published valid payload because its inode still matched.

    The waiter must detect the identity change (``published_digest !=
    initial_digest``) and skip the waiter-specific dim validation on the
    hand-off — the caller rebuilds its image side to match the healed width.
    """
    cache = EmbeddingCache(tmp_path / "cache")
    pre_heal_identity = _identity()
    healed_identity = _identity(suffix="healed")
    healed_digest = identity_digest(healed_identity)

    producer_started = threading.Event()
    release_producer = threading.Event()

    def self_healing_compute():
        producer_started.set()
        assert release_producer.wait(2)
        # Producer publishes a wider payload than the waiter's pre-heal
        # image encoder expects (e.g. text encoder was healed to a 768-wide
        # revision while the waiter still holds a 512-wide image session).
        return np.ones((768, 2), dtype=np.float32)

    with ThreadPoolExecutor(max_workers=2) as pool:
        producer = pool.submit(
            cache.get_or_compute,
            pre_heal_identity,
            self_healing_compute,
            identity_after=lambda: healed_identity,
        )
        assert producer_started.wait(2)
        # Waiter joins on the pre-heal identity and carries the pre-heal
        # image-encoder width. If we validated the healed payload against
        # this stale dim we would raise and unlink the valid payload.
        waiter = pool.submit(
            cache.get_or_compute,
            pre_heal_identity,
            lambda: pytest.fail("waiter must not compute"),
            identity_after=lambda: healed_identity,
            embedding_dim=512,
        )
        release_producer.set()
        producer_value, producer_identity = producer.result(timeout=2)
        waiter_value, waiter_identity = waiter.result(timeout=2)

    assert producer_value.shape == (768, 2)
    assert producer_identity == healed_identity
    # The hand-off must return the healed payload and the healed identity so
    # the caller can detect the change and rebuild its image side.
    assert waiter_value.shape == (768, 2)
    assert waiter_identity == healed_identity
    # And the freshly published payload must still be on disk — the waiter's
    # stale dim must not have tripped ``_load``'s inode-matched unlink.
    assert os.path.exists(
        os.path.join(cache.cache_dir, f"{healed_digest}.npy")
    )


def test_cancel_triggered_retry_still_enforces_embedding_dim(tmp_path):
    """Regression: the recursive retry a waiter runs after an equal-key
    producer is cancelled must carry ``embedding_dim`` forward. Dropping
    it lets the replacement computation publish a mismatched payload that
    would only fail at inference on ``img_features @ txt_embeddings``."""
    class ClassificationCancelled(RuntimeError):
        pass

    cache = EmbeddingCache(tmp_path / "cache")
    identity = _identity()
    producer_started = threading.Event()
    cancel_producer = threading.Event()

    def cancelled_compute():
        producer_started.set()
        assert cancel_producer.wait(2)
        raise ClassificationCancelled("cancelled")

    def wrong_dim_compute():
        # A 512-dim model receiving a 256-dim payload — the exact scenario
        # the fresh-computation validator catches on the primary path.
        return np.ones((256, 2), dtype=np.float32)

    with ThreadPoolExecutor(max_workers=2) as pool:
        producer = pool.submit(
            cache.get_or_compute, identity, cancelled_compute,
        )
        assert producer_started.wait(2)
        waiter = pool.submit(
            cache.get_or_compute,
            identity,
            wrong_dim_compute,
            embedding_dim=512,
        )
        cancel_producer.set()
        with pytest.raises(ClassificationCancelled):
            producer.result(timeout=2)
        with pytest.raises(ValueError, match=r"expected \(512, 2\)"):
            waiter.result(timeout=2)

    assert not os.path.exists(cache.path_for(identity))


def test_mismatched_embedding_dim_rejects_freshly_computed_payload(tmp_path):
    """A producer whose factory returns a wrong-dim ndarray must fail up
    front rather than publish a payload that later callers must eventually
    reject."""
    cache = EmbeddingCache(tmp_path / "cache")
    identity = _identity()

    with pytest.raises(ValueError, match=r"expected \(512, 2\)"):
        cache.get_or_compute(
            identity,
            lambda: np.ones((256, 2), dtype=np.float32),
            embedding_dim=512,
        )

    assert not os.path.exists(cache.path_for(identity))


def test_invalid_payload_never_replaces_final_path(tmp_path):
    cache = EmbeddingCache(tmp_path / "cache")
    identity = _identity()

    with pytest.raises(ValueError, match="dtype"):
        cache.get_or_compute(
            identity,
            lambda: np.ones((4, 2), dtype=np.float64),
        )

    assert not os.path.exists(cache.path_for(identity))
    assert not list((tmp_path / "cache").glob("*.tmp"))


def test_competing_publisher_does_not_fail_our_publish(tmp_path, monkeypatch):
    """Regression: publish used to digest the temp file, os.replace, then
    re-read the *final* path and compare. Single-flight is in-process only,
    so a second process publishing the same identity can land its replace in
    that window. Its payload is equally valid but not bit-identical (the
    encoder is not bitwise reproducible), so we raised a hard
    ValueError over a perfectly good cache entry. Integrity must be checked
    on the temp file before the rename instead."""
    import embedding_cache as ec

    cache = EmbeddingCache(tmp_path / "cache")
    identity = _identity()
    competitor = _payload(value=7)
    real_replace = os.replace

    def replace_then_race(src, dst):
        real_replace(src, dst)
        if str(dst).endswith(".npy"):
            # Stand in for another process winning the same rename just after
            # us. Leave the manifest's own os.replace alone.
            np.save(dst, competitor, allow_pickle=False)

    monkeypatch.setattr(ec.os, "replace", replace_then_race)

    value, _ = cache.get_or_compute(identity, _payload)

    # Our own call returns without raising, and the durable entry is the
    # competitor's valid payload rather than a deleted/corrupt file.
    assert np.array_equal(value, _payload())
    assert np.array_equal(np.load(cache.path_for(identity)), competitor)


def test_publish_rejects_bytes_that_do_not_round_trip(tmp_path, monkeypatch):
    """The pre-rename check must still catch a write that did not persist
    what we serialized — and must not leave a partial file behind."""
    import embedding_cache as ec

    cache = EmbeddingCache(tmp_path / "cache")
    identity = _identity()

    real_load = np.load

    def corrupt_load(path, *args, **kwargs):
        if str(path).endswith(".npy.tmp"):
            # Serialized fine, came back as something else.
            return _payload(value=99)
        return real_load(path, *args, **kwargs)

    monkeypatch.setattr(ec.np, "load", corrupt_load)

    with pytest.raises(ValueError, match="changed during write"):
        cache.get_or_compute(identity, _payload)

    assert not os.path.exists(cache.path_for(identity))
    assert not list((tmp_path / "cache").glob("*.tmp"))


def test_manifest_failure_does_not_invalidate_published_payload(
    tmp_path, monkeypatch,
):
    cache = EmbeddingCache(tmp_path / "cache")
    identity = _identity()
    monkeypatch.setattr(
        cache,
        "_update_manifest",
        lambda *_args: (_ for _ in ()).throw(OSError("read-only manifest")),
    )

    value, _ = cache.get_or_compute(identity, _payload)

    assert np.array_equal(value, _payload())
    assert cache.is_cached(identity, 2)


def test_load_failure_does_not_unlink_a_concurrently_repaired_file(
    tmp_path, monkeypatch,
):
    """Regression: two callers hit the same malformed cache file. Caller A's
    np.load raises. Between that failure and A's cleanup unlink, caller B
    atomically publishes a valid replacement via os.replace onto the same
    name. A must not delete B's freshly published payload — otherwise a
    joining waiter's hand-off ``_load`` raises FileNotFoundError, failing
    an otherwise successful equal-key job. Only the exact inode A validated
    is safe to remove.
    """
    import embedding_cache as ec

    cache = EmbeddingCache(tmp_path / "cache")
    identity = _identity()
    digest = identity_digest(identity)
    path = cache.path_for(identity)

    os.makedirs(cache.cache_dir, exist_ok=True)
    # Write a malformed (rank-1) payload that _validate_payload rejects.
    np.save(path, np.ones(4, dtype=np.float32), allow_pickle=False)

    real_load = ec.np.load

    def race_replace_then_fail(load_path, *args, **kwargs):
        result = real_load(load_path, *args, **kwargs)
        if str(load_path) == path:
            # Simulate another caller winning os.replace between our
            # np.load and the cleanup unlink. This yields a different
            # inode at the same name.
            fd, tmp = tempfile.mkstemp(
                prefix=".repair.", suffix=".npy", dir=cache.cache_dir,
            )
            os.close(fd)
            np.save(tmp, np.full((4, 2), 3, dtype=np.float32),
                    allow_pickle=False)
            os.replace(tmp, load_path)
        return result

    monkeypatch.setattr(ec.np, "load", race_replace_then_fail)

    with pytest.raises(ValueError):
        cache._load(digest, 2)

    # The valid replacement must still be on disk — our cleanup detected
    # the inode change and skipped the unlink.
    assert os.path.exists(path), (
        "concurrently repaired payload was deleted by the failing loader's "
        "cleanup"
    )
    fresh = np.load(path)
    assert fresh.shape == (4, 2)
    assert np.array_equal(fresh, np.full((4, 2), 3, dtype=np.float32))


def test_load_failure_still_removes_its_own_invalid_file(tmp_path):
    """The narrowed unlink must still drop a file that nobody replaced —
    otherwise a malformed payload would persist and every caller would
    keep re-validating + re-failing without single-flight ever kicking in
    (since ``is_cached`` reaches ``_load`` without registering a flight).
    """
    cache = EmbeddingCache(tmp_path / "cache")
    identity = _identity()
    digest = identity_digest(identity)
    path = cache.path_for(identity)

    os.makedirs(cache.cache_dir, exist_ok=True)
    np.save(path, np.ones(4, dtype=np.float32), allow_pickle=False)

    with pytest.raises(ValueError):
        cache._load(digest, 2)

    assert not os.path.exists(path)


def test_concurrent_manifest_updates_do_not_lose_entries(tmp_path):
    cache = EmbeddingCache(tmp_path / "cache")
    identities = [_identity(suffix=str(i)) for i in range(8)]

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [
            pool.submit(cache.get_or_compute, identity, _payload)
            for identity in identities
        ]
        # Deadlock guard: eight concurrent manifest writes on a Windows CI
        # runner under xdist contention finish well inside this budget; a
        # tighter window flaked on 3s without any producer getting stuck.
        for future in futures:
            future.result(timeout=30)

    with open(tmp_path / "cache" / "manifest.json", encoding="utf-8") as handle:
        manifest = json.load(handle)
    assert set(manifest) == {
        f"{identity_digest(identity)}.npy" for identity in identities
    }


# ---------------------------------------------------------------------------
# Resumable checkpoints: a paused/cancelled/crashed producer keeps its work
# ---------------------------------------------------------------------------


def _rows(count, dim=4, start=1):
    return [
        np.full(dim, start + i, dtype=np.float32) for i in range(count)
    ]


def test_checkpoint_roundtrip_is_keyed_by_identity(tmp_path):
    cache = EmbeddingCache(tmp_path / "cache")
    identity = _identity(labels=("bird", "cat", "dog"))
    checkpoint = cache.checkpoint_for(identity)
    assert checkpoint.load() is None

    checkpoint.save(_rows(2))
    loaded = checkpoint.load()
    assert loaded.shape == (2, 4)
    assert np.array_equal(loaded, np.stack(_rows(2)))

    # A different text-side identity must never see another run's rows.
    other = cache.checkpoint_for(
        _identity(labels=("bird", "cat", "dog"), suffix="b")
    )
    assert other.load() is None

    # The final payload path is untouched by partial progress.
    assert not os.path.exists(cache.path_for(identity))


def test_checkpoint_rejects_unusable_partials_and_removes_them(tmp_path):
    cache = EmbeddingCache(tmp_path / "cache")
    identity = _identity(labels=("bird", "cat", "dog"))
    checkpoint = cache.checkpoint_for(identity)

    # More rows than labels can never be a prefix of this identity.
    with pytest.raises(ValueError):
        checkpoint.save(_rows(5))
    assert checkpoint.load() is None

    # A width that disagrees with the image encoder is rejected on load.
    checkpoint.save(_rows(2))
    assert checkpoint.load(embedding_dim=8) is None
    assert not os.path.exists(checkpoint.path), (
        "an unusable checkpoint must be unlinked so the next producer "
        "does not keep tripping over it"
    )

    # Non-finite rows written by an interrupted process are discarded too.
    os.makedirs(checkpoint.cache_dir, exist_ok=True)
    bad = np.full((2, 4), np.nan, dtype=np.float32)
    np.save(checkpoint.path, bad, allow_pickle=False)
    assert checkpoint.load() is None
    assert not os.path.exists(checkpoint.path)

    # Truncated bytes are handled the same way.
    with open(checkpoint.path, "wb") as handle:
        handle.write(b"\x93NUMPY")
    assert checkpoint.load() is None
    assert not os.path.exists(checkpoint.path)


def test_checkpoint_save_with_no_rows_clears_stale_file(tmp_path):
    cache = EmbeddingCache(tmp_path / "cache")
    checkpoint = cache.checkpoint_for(_identity(labels=("bird", "cat")))
    checkpoint.save(_rows(1))
    assert os.path.exists(checkpoint.path)
    checkpoint.save([])
    assert not os.path.exists(checkpoint.path)


def test_publishing_the_full_payload_discards_the_checkpoint(tmp_path):
    cache = EmbeddingCache(tmp_path / "cache")
    identity = _identity()
    checkpoint = cache.checkpoint_for(identity)
    checkpoint.save(_rows(1))
    assert os.path.exists(checkpoint.path)

    value, _ = cache.get_or_compute(identity, _payload)

    assert np.array_equal(value, _payload())
    assert os.path.exists(cache.path_for(identity))
    assert not os.path.exists(checkpoint.path), (
        "a complete payload supersedes partial progress; leaving the "
        "checkpoint behind would waste disk and confuse the next resume"
    )


def test_producer_pause_wakes_waiter_and_waiter_takes_over(tmp_path):
    """A producer that steps down for Pause (checkpointing its progress)
    must not poison the key: the waiter retries and becomes the producer,
    exactly as it does when the producer was cancelled."""

    class ClassifierLoadPaused(RuntimeError):
        pass

    cache = EmbeddingCache(tmp_path / "cache")
    identity = _identity()
    producer_started = threading.Event()
    pause_producer = threading.Event()
    waiter_computed = threading.Event()

    def paused_compute():
        producer_started.set()
        assert pause_producer.wait(2)
        cache.checkpoint_for(identity).save(_rows(1))
        raise ClassifierLoadPaused("classifier load paused")

    def healthy_compute():
        waiter_computed.set()
        # The replacement producer sees the checkpoint the first one left.
        assert cache.checkpoint_for(identity).load() is not None
        return _payload(value=2)

    with ThreadPoolExecutor(max_workers=2) as pool:
        producer = pool.submit(cache.get_or_compute, identity, paused_compute)
        assert producer_started.wait(2)
        waiter = pool.submit(cache.get_or_compute, identity, healthy_compute)
        pause_producer.set()
        with pytest.raises(ClassifierLoadPaused):
            producer.result(timeout=2)
        value, _ = waiter.result(timeout=2)

    assert waiter_computed.is_set()
    assert np.array_equal(value, _payload(value=2))
    assert not os.path.exists(cache.checkpoint_for(identity).path)


def test_individual_labels_reuse_columns_in_new_order_and_isolate_models(tmp_path):
    from embedding_cache import LabelEmbeddingCache

    first = LabelEmbeddingCache(tmp_path, _identity(), embedding_dim=4)
    original = np.arange(8, dtype=np.float32).reshape(4, 2)
    first.seed(["bird", "cat"], original)
    changed = LabelEmbeddingCache(tmp_path, _identity(["cat", "dog", "bird"]), embedding_dim=4)
    calls = []

    def encode(label):
        calls.append(label)
        return np.full(4, 99, dtype=np.float32)

    result = np.stack([
        changed.resolve(label, lambda label=label: encode(label)) for label in ["cat", "dog", "bird"]
    ], axis=1)
    assert calls == ["dog"]
    np.testing.assert_array_equal(result[:, 0], original[:, 1])
    np.testing.assert_array_equal(result[:, 2], original[:, 0])
    assert LabelEmbeddingCache(tmp_path, _identity(suffix="b"), 4).read("bird") is None
    assert LabelEmbeddingCache(tmp_path, _identity(), 5).read("cat") is None


def test_overlapping_label_sets_share_one_inflight_label(tmp_path):
    from embedding_cache import LabelEmbeddingCache

    started, release = threading.Event(), threading.Event()
    first = LabelEmbeddingCache(tmp_path, _identity(["bird"]), 4)
    second = LabelEmbeddingCache(tmp_path, _identity(["cat", "bird"]), 4)
    calls = []

    def encode():
        calls.append("bird")
        started.set()
        assert release.wait(3)
        return np.ones(4, dtype=np.float32)

    with ThreadPoolExecutor(max_workers=2) as pool:
        a = pool.submit(first.resolve, "bird", encode)
        assert started.wait(3)
        b = pool.submit(second.resolve, "bird", encode)
        release.set()
        np.testing.assert_array_equal(a.result(3), b.result(3))
    assert calls == ["bird"]


def test_late_cache_miss_rechecks_after_another_producer_publishes(tmp_path, monkeypatch):
    cache = EmbeddingCache(tmp_path)
    read_missed, release = threading.Event(), threading.Event()
    original_load = cache._load
    calls = []

    def delayed_load(*args, **kwargs):
        if threading.current_thread().name.startswith("late") and not read_missed.is_set():
            read_missed.set()
            assert release.wait(3)
            raise FileNotFoundError("read missed before publication")
        return original_load(*args, **kwargs)

    monkeypatch.setattr(cache, "_load", delayed_load)

    def compute():
        calls.append(True)
        return _payload()

    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="late") as pool:
        late = pool.submit(cache.get_or_compute, _identity(), compute)
        try:
            assert read_missed.wait(3)
            early, _ = cache.get_or_compute(_identity(), compute)
        finally:
            release.set()
        result, _ = late.result(3)
    np.testing.assert_array_equal(result, early)
    assert calls == [True]
