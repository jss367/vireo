"""Correct, process-wide cache for BioCLIP label embeddings.

The cache key describes the exact text-side inputs.  Cache misses are
single-flight within the Vireo process: one caller computes and atomically
publishes the payload while callers for the same key wait without loading a
second text encoder.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import os
import tempfile
import threading
from dataclasses import dataclass, field
from datetime import datetime

import numpy as np
from file_replace import replace_file

log = logging.getLogger(__name__)

EMBEDDING_SCHEMA_VERSION = 2

# Partial-progress files sit next to the final ``<digest>.npy`` payload.
# They hold the first ``k`` label embeddings as a ``(k, embedding_dim)``
# float32 array so a producer interrupted by Pause, Cancel, or a crash
# can pick up where it stopped instead of re-encoding every label.
CHECKPOINT_SUFFIX = ".partial.npy"

# Exceptions a producer raises when it steps down for a reason local to
# its own job. Waiters must not inherit them: the key is still healthy,
# so one waiter retries and becomes the replacement producer.
_PRODUCER_STEPPED_DOWN = frozenset({
    "ClassificationCancelled",
    "EmbeddingWaitCancelled",
    "ClassifierLoadPaused",
})

_identity_lock = threading.Lock()
_manifest_lock = threading.Lock()
_flights_lock = threading.Lock()
_producer_execution_lock = threading.Lock()

_diagnostics = {
    "cache_hits": 0,
    "cache_misses": 0,
    "producer_starts": 0,
    "producer_publications": 0,
    "producer_failures": 0,
    "waiter_joins": 0,
}


@dataclass
class _Flight:
    event: threading.Event = field(default_factory=threading.Event)
    error: BaseException | None = None
    published_digest: str | None = None


_flights: dict[tuple[str, str], _Flight] = {}
_active_producer_executions: dict[tuple[str, str], int] = {}
_single_flight_violations = 0
_max_concurrent_producers_per_key = 0


def _begin_producer_execution(flight_key):
    """Record entry into the actual embedding computation.

    This uses a lock and state independent of the flight registry. Measuring
    overlap while holding ``_flights_lock`` would only restate that registry's
    own invariant and could never observe a duplicate computation.
    """
    global _max_concurrent_producers_per_key, _single_flight_violations
    with _producer_execution_lock:
        producer_count = _active_producer_executions.get(flight_key, 0) + 1
        _active_producer_executions[flight_key] = producer_count
        _max_concurrent_producers_per_key = max(
            _max_concurrent_producers_per_key,
            producer_count,
        )
        if producer_count > 1:
            _single_flight_violations += 1


def _end_producer_execution(flight_key):
    with _producer_execution_lock:
        producer_count = _active_producer_executions.get(flight_key, 0) - 1
        if producer_count > 0:
            _active_producer_executions[flight_key] = producer_count
        else:
            _active_producer_executions.pop(flight_key, None)


def get_embedding_cache_diagnostics():
    """Return process-lifetime counters for workload diagnostics.

    These counters intentionally contain no cache keys, label names, or file
    paths.  Consumers take before/after snapshots and report deltas for one
    benchmark window; no timing-sensitive production behavior depends on
    them.
    """
    with _flights_lock:
        diagnostics = dict(_diagnostics)
    with _producer_execution_lock:
        return {
            **diagnostics,
            "active_producers": sum(_active_producer_executions.values()),
            "single_flight_violations": _single_flight_violations,
            "max_concurrent_producers_per_key": (
                _max_concurrent_producers_per_key
            ),
        }


class EmbeddingComputationError(RuntimeError):
    """An equal-key producer failed before publishing an embedding."""


class EmbeddingWaitCancelled(RuntimeError):
    """A caller stopped waiting without cancelling the shared producer."""


def canonicalize_labels(labels):
    """Return the exact labels sent to the encoder.

    Order and case are authoritative.  Deduplication belongs to the label-set
    loader, not the cache, because changing either would change classifier
    output semantics.
    """
    canonical = []
    for label in labels:
        if not isinstance(label, str):
            raise TypeError("labels must contain only strings")
        value = label.strip()
        if not value:
            raise ValueError("labels must not contain empty entries")
        canonical.append(value)
    if not canonical:
        raise ValueError("labels list must not be empty")
    return canonical


def _sha256_file(path):
    # computation_cache.sha256_file memoizes by path/size/mtime.  The lock
    # prevents simultaneous first callers from both streaming a multi-GB ONNX
    # external-data file merely to construct the same cache identity.
    from computation_cache import sha256_file

    with _identity_lock:
        return sha256_file(path)


def _file_identity(path, required=True, allow_missing=False):
    if not os.path.isfile(path):
        if not required:
            return None
        if allow_missing:
            return {"name": os.path.basename(path), "missing": True}
        raise FileNotFoundError(path)
    return {
        "name": os.path.basename(path),
        "sha256": _sha256_file(path),
    }


def _pinned_revision(model_dir):
    path = os.path.join(model_dir, ".hf_revision")
    try:
        with open(path, encoding="utf-8") as handle:
            revision = handle.read().strip()
    except OSError:
        return None
    return revision or None


def _revision_file_identity(path, revision, required=True, allow_missing=False):
    if not os.path.isfile(path):
        if not required:
            return None
        if allow_missing:
            return {"name": os.path.basename(path), "missing": True}
        raise FileNotFoundError(path)
    stat = os.stat(path)
    return {
        "name": os.path.basename(path),
        "immutable_upstream_revision": revision,
        "size": stat.st_size,
    }


def build_embedding_identity(
    labels,
    model_str,
    model_dir,
    *,
    prompt_template_identity,
    tokenizer_context_length,
    allow_missing=False,
):
    """Build the complete, path-independent identity for an embedding set."""
    canonical_labels = canonicalize_labels(labels)
    text_encoder = os.path.join(model_dir, "text_encoder.onnx")
    external_data = os.path.join(model_dir, "text_encoder.onnx.data")
    tokenizer = os.path.join(model_dir, "tokenizer.json")
    revision = _pinned_revision(model_dir)
    if revision:
        text_onnx_identity = _revision_file_identity(
            text_encoder, revision, allow_missing=allow_missing
        )
        external_data_identity = _revision_file_identity(
            external_data,
            revision,
            required=False,
            allow_missing=allow_missing,
        )
    else:
        # Legacy and custom installs have no immutable upstream promise, so
        # bind the cache to their exact text-encoder bytes.
        text_onnx_identity = _file_identity(
            text_encoder, allow_missing=allow_missing
        )
        external_data_identity = _file_identity(
            external_data, required=False, allow_missing=allow_missing
        )

    return {
        "embedding_schema": EMBEDDING_SCHEMA_VERSION,
        "model_runtime": {
            "family": "bioclip-onnx-text",
            "model_str": model_str,
        },
        "text_encoder": {
            "onnx": text_onnx_identity,
            "external_data": external_data_identity,
        },
        "tokenizer": {
            "file": _file_identity(tokenizer, allow_missing=allow_missing),
            "context_length": tokenizer_context_length,
            "padding": "right-zero",
            "truncation": "right",
        },
        "prompt_template_set": prompt_template_identity,
        "labels": canonical_labels,
    }


def identity_digest(identity):
    body = json.dumps(
        identity,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(body).hexdigest()


def cache_path(cache_dir, identity):
    return os.path.join(cache_dir, f"{identity_digest(identity)}.npy")


def _validate_payload(value, label_count, embedding_dim=None):
    if not isinstance(value, np.ndarray):
        raise ValueError("embedding payload is not a NumPy array")
    if value.ndim != 2 or value.shape[1] != label_count:
        raise ValueError(
            "embedding payload has shape "
            f"{value.shape}; expected (embedding_dim, {label_count})"
        )
    if embedding_dim is not None and value.shape[0] != embedding_dim:
        # A cache file whose first axis is any positive int passes rank +
        # label-axis checks, so ``Classifier`` would then load it and only
        # blow up at inference time on ``img_features @ txt_embeddings``.
        # Reject it here so the caller falls back to recomputing.
        raise ValueError(
            "embedding payload has shape "
            f"{value.shape}; expected ({embedding_dim}, {label_count})"
        )
    if value.dtype != np.float32:
        raise ValueError(
            f"embedding payload has dtype {value.dtype}; expected float32"
        )
    if not np.isfinite(value).all():
        raise ValueError("embedding payload contains non-finite values")
    return value


def _validate_checkpoint(value, label_count, embedding_dim=None):
    """Validate a partial payload of shape ``(done, embedding_dim)``."""
    if not isinstance(value, np.ndarray):
        raise ValueError("embedding checkpoint is not a NumPy array")
    if value.ndim != 2 or value.shape[0] < 1 or value.shape[0] > label_count:
        raise ValueError(
            "embedding checkpoint has shape "
            f"{value.shape}; expected (1..{label_count}, embedding_dim)"
        )
    if embedding_dim is not None and value.shape[1] != embedding_dim:
        raise ValueError(
            "embedding checkpoint has shape "
            f"{value.shape}; expected (done, {embedding_dim})"
        )
    if value.dtype != np.float32:
        raise ValueError(
            f"embedding checkpoint has dtype {value.dtype}; expected float32"
        )
    if not np.isfinite(value).all():
        raise ValueError("embedding checkpoint contains non-finite values")
    return value


class EmbeddingCheckpoint:
    """Resumable partial progress for one embedding identity.

    The file is keyed by the same digest as the final payload, so it can
    only ever be resumed by a computation with byte-identical text-side
    inputs (labels in order, templates, tokenizer, encoder revision).
    ``save`` is atomic; ``load`` unlinks anything it cannot trust so a
    corrupt partial cannot poison the next producer.
    """

    def __init__(self, cache_dir, digest, label_count):
        self.cache_dir = cache_dir
        self.digest = digest
        self.label_count = label_count
        self.path = os.path.join(cache_dir, f"{digest}{CHECKPOINT_SUFFIX}")

    def load(self, embedding_dim=None):
        """Return the ``(done, embedding_dim)`` array, or ``None``."""
        if not os.path.isfile(self.path):
            return None
        try:
            value = np.load(self.path, allow_pickle=False)
            return _validate_checkpoint(
                value, self.label_count, embedding_dim=embedding_dim,
            )
        except (EOFError, OSError, ValueError) as exc:
            log.warning(
                "EmbeddingCache: discarding unusable checkpoint key=%s: %s",
                self.digest[:12], exc,
            )
            self.discard()
            return None

    def save(self, features):
        """Atomically persist the embeddings finished so far.

        ``features`` is a sequence of 1-D float32 vectors (one per finished
        label, in label order) or an equivalent 2-D array. An empty
        sequence removes any stale checkpoint instead of writing one.
        """
        if isinstance(features, np.ndarray):
            value = features
        else:
            if not len(features):
                self.discard()
                return
            value = np.stack(list(features), axis=0)
        value = np.ascontiguousarray(value, dtype=np.float32)
        if value.ndim != 2 or value.shape[0] == 0:
            self.discard()
            return
        _validate_checkpoint(value, self.label_count)
        os.makedirs(self.cache_dir, exist_ok=True)
        fd, temporary = tempfile.mkstemp(
            prefix=f".{self.digest}.", suffix=".partial.tmp",
            dir=self.cache_dir,
        )
        try:
            with os.fdopen(fd, "wb") as handle:
                np.save(handle, value, allow_pickle=False)
                handle.flush()
                os.fsync(handle.fileno())
            replace_file(temporary, self.path)
        finally:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(temporary)

    def discard(self):
        with contextlib.suppress(FileNotFoundError, OSError):
            os.unlink(self.path)


class EmbeddingCache:
    """Durable embedding cache with per-key in-process single-flight."""

    def __init__(self, cache_dir, manifest_path=None):
        self.cache_dir = os.path.abspath(os.path.expanduser(os.fspath(cache_dir)))
        if manifest_path:
            self.manifest_path = os.path.abspath(
                os.path.expanduser(os.fspath(manifest_path))
            )
        else:
            self.manifest_path = os.path.join(self.cache_dir, "manifest.json")

    def path_for(self, identity):
        return cache_path(self.cache_dir, identity)

    def checkpoint_for(self, identity):
        """Return the resumable partial-progress store for ``identity``."""
        return EmbeddingCheckpoint(
            self.cache_dir, identity_digest(identity), len(identity["labels"]),
        )

    def is_cached(self, identity, label_count, embedding_dim=None):
        """Return whether a valid payload for ``identity`` is on disk.

        Side effect: an unreadable or malformed payload is unlinked as part
        of the underlying ``_load`` call so subsequent producers do not keep
        re-hitting the same corrupt file.  Callers on the readiness path
        therefore repair the cache on the fly.
        """
        try:
            self._load(
                identity_digest(identity), label_count,
                embedding_dim=embedding_dim,
            )
            return True
        except (EOFError, OSError, ValueError):
            return False

    def get_or_compute(
        self,
        identity,
        compute,
        *,
        identity_after=None,
        cancel_check=None,
        embedding_dim=None,
    ):
        """Load or compute one embedding payload.

        ``identity_after`` is evaluated after computation because ONNX
        self-healing can replace the model directory while the producer is
        loading the text encoder.  In that case the payload is published under
        the healed identity and waiters follow the producer to that durable
        path.

        ``embedding_dim``, when supplied, rejects any cached or freshly
        computed payload whose first axis does not match the model's
        expected image-feature dimension.  Rank and label-axis checks alone
        would accept a malformed ``(1, N)`` file and only surface the
        mismatch at inference time on ``img_features @ txt_embeddings``.
        """
        label_count = len(identity["labels"])
        initial_digest = identity_digest(identity)

        if cancel_check and cancel_check():
            raise EmbeddingWaitCancelled("classification cancelled")
        try:
            value = self._load(
                initial_digest, label_count, embedding_dim=embedding_dim,
            )
            with _flights_lock:
                _diagnostics["cache_hits"] += 1
            if cancel_check and cancel_check():
                raise EmbeddingWaitCancelled("classification cancelled")
            return value, identity
        except (EOFError, OSError, ValueError):
            with _flights_lock:
                _diagnostics["cache_misses"] += 1

        flight_key = (self.cache_dir, initial_digest)
        with _flights_lock:
            flight = _flights.get(flight_key)
            if flight is None:
                flight = _Flight()
                _flights[flight_key] = flight
                _diagnostics["producer_starts"] += 1
                producer = True
            else:
                _diagnostics["waiter_joins"] += 1
                producer = False

        if not producer:
            log.info("EmbeddingCache: joining producer key=%s", initial_digest[:12])
            while not flight.event.wait(0.1):
                if cancel_check and cancel_check():
                    raise EmbeddingWaitCancelled("classification cancelled")
            if cancel_check and cancel_check():
                raise EmbeddingWaitCancelled("classification cancelled")
            if flight.error is not None:
                if flight.error.__class__.__name__ in _PRODUCER_STEPPED_DOWN:
                    # Cancellation (or a pause-abort, which checkpoints its
                    # progress first) belongs to the producer, not the key.
                    # A healthy waiter retries and one of the waiters becomes
                    # the replacement producer, resuming from the checkpoint.
                    # ``embedding_dim`` must ride
                    # the retry so the replacement computation is checked
                    # against this caller's image encoder; dropping it would
                    # bypass the new dimension validation on this branch and
                    # let a mismatched payload publish and later crash at
                    # ``img_features @ txt_embeddings``.
                    return self.get_or_compute(
                        identity,
                        compute,
                        identity_after=identity_after,
                        cancel_check=cancel_check,
                        embedding_dim=embedding_dim,
                    )
                raise EmbeddingComputationError(
                    "label embedding producer failed"
                ) from flight.error
            published_digest = flight.published_digest or initial_digest
            # Durable publication is the hand-off contract.  Do not reuse the
            # producer's mutable ndarray in memory.
            #
            # A shared producer (e.g. an equal-key precompute job) may
            # self-heal the whole model snapshot mid-flight to a revision
            # whose text encoder emits a different feature width.  The
            # producer publishes under the healed identity, so
            # ``published_digest != initial_digest`` is the durable signal
            # that this waiter's pre-flight image encoder is now stale.
            # In that case, validating the healed payload against this
            # waiter's stale ``embedding_dim`` would (a) raise before we can
            # return the healed identity, so the caller never reaches its
            # ``identity_before != identity_after`` image-side rebuild, and
            # (b) trip ``_load``'s inode-matched unlink on the freshly
            # published valid payload.  Skip the waiter-specific dimension
            # check on the hand-off; the caller detects the identity change
            # and rebuilds its image side against the healed snapshot.
            waiter_dim = (
                None if published_digest != initial_digest else embedding_dim
            )
            value = self._load(
                published_digest, label_count, embedding_dim=waiter_dim,
            )
            actual_identity = identity_after() if identity_after else identity
            return value, actual_identity

        try:
            if cancel_check and cancel_check():
                raise EmbeddingWaitCancelled("classification cancelled")
            # A producer may have published after our first disk read but
            # before we acquired the flight registry. Recheck as its new
            # owner so a late caller cannot repeat that completed work.
            try:
                value = self._load(initial_digest, label_count, embedding_dim=embedding_dim)
            except (EOFError, OSError, ValueError):
                pass
            else:
                if cancel_check and cancel_check():
                    raise EmbeddingWaitCancelled("classification cancelled")
                flight.published_digest = initial_digest
                with _flights_lock:
                    _diagnostics["cache_hits"] += 1
                return value, identity
            log.info("EmbeddingCache: producing key=%s", initial_digest[:12])
            _begin_producer_execution(flight_key)
            try:
                value = _validate_payload(
                    compute(), label_count, embedding_dim=embedding_dim,
                )
            finally:
                _end_producer_execution(flight_key)
            if cancel_check and cancel_check():
                raise EmbeddingWaitCancelled("classification cancelled")
            actual_identity = identity_after() if identity_after else identity
            actual_digest = identity_digest(actual_identity)
            self._publish(actual_digest, value, label_count)
            try:
                self._update_manifest(actual_digest, actual_identity, value)
            except Exception:
                # The manifest is explanatory metadata, never cache validity.
                # Publication has already committed a complete payload.
                log.exception(
                    "EmbeddingCache: manifest update failed key=%s",
                    actual_digest[:12],
                )
            flight.published_digest = actual_digest
            # The complete payload supersedes any partial progress. Remove
            # the checkpoint under both digests: the producer resumed from
            # the initial one, and a self-heal can publish under another.
            for digest in {initial_digest, actual_digest}:
                EmbeddingCheckpoint(
                    self.cache_dir, digest, label_count,
                ).discard()
            with _flights_lock:
                _diagnostics["producer_publications"] += 1
            log.info("EmbeddingCache: published key=%s", actual_digest[:12])
            return value, actual_identity
        except BaseException as exc:
            flight.error = exc
            with _flights_lock:
                _diagnostics["producer_failures"] += 1
            raise
        finally:
            # Remove only the registry lookup before waking waiters. Existing
            # waiters retain the _Flight object, while a retry after producer
            # cancellation can immediately claim a fresh flight.
            with _flights_lock:
                if _flights.get(flight_key) is flight:
                    del _flights[flight_key]
            flight.event.set()

    def _load(self, digest, label_count, embedding_dim=None):
        path = os.path.join(self.cache_dir, f"{digest}.npy")
        # Snapshot the inode we are about to validate so a concurrent
        # producer that atomically replaces this path between np.load and
        # unlink cannot make us delete their freshly published payload.
        # Single-flight is in-process only, and validation happens before
        # _flights registration, so an equal-key producer in another
        # process (or a racing in-process caller) can win os.replace here.
        try:
            loaded_ino = os.stat(path).st_ino
        except OSError:
            loaded_ino = None
        try:
            value = np.load(path, allow_pickle=False)
            return _validate_payload(
                value, label_count, embedding_dim=embedding_dim,
            )
        except (EOFError, ValueError):
            # A truncated or otherwise invalid final file cannot satisfy later
            # callers.  Removal is best-effort — but only remove the exact
            # inode we validated, so a valid payload published under this
            # name after our load failed is not deleted (which would cause a
            # joining waiter's hand-off _load to raise FileNotFoundError).
            if loaded_ino is not None:
                with contextlib.suppress(OSError):
                    if os.stat(path).st_ino == loaded_ino:
                        os.unlink(path)
            raise

    def _publish(self, digest, value, label_count):
        os.makedirs(self.cache_dir, exist_ok=True)
        fd, temporary = tempfile.mkstemp(
            prefix=f".{digest}.", suffix=".npy.tmp", dir=self.cache_dir
        )
        try:
            with os.fdopen(fd, "wb") as handle:
                np.save(handle, value, allow_pickle=False)
                handle.flush()
                os.fsync(handle.fileno())
            # Verify the durable bytes *before* they become visible.  Reading
            # the final path back after os.replace cannot prove anything about
            # our own write: single-flight is in-process only, so another
            # process publishing the same identity can land its os.replace
            # between ours and the read-back, and its (equally valid, but not
            # bit-identical — the encoder is not bitwise reproducible across
            # runs) payload would look like corruption to us.  Round-tripping
            # the temp file against the in-memory value is a strictly stronger
            # check than the old digest comparison, and it costs one pass
            # instead of three.
            loaded = np.load(temporary, allow_pickle=False)
            _validate_payload(loaded, label_count)
            if not np.array_equal(loaded, value):
                raise ValueError("embedding payload changed during write")
            del loaded
            replace_file(temporary, os.path.join(self.cache_dir, f"{digest}.npy"))
        finally:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(temporary)

    def _update_manifest(self, digest, identity, value):
        identity_metadata = {
            key: item for key, item in identity.items() if key != "labels"
        }
        metadata = {
            "model": identity["model_runtime"]["model_str"],
            "label_count": len(identity["labels"]),
            "labels_sha256": identity_digest({"labels": identity["labels"]}),
            "embedding_dim": value.shape[0],
            "dtype": str(value.dtype),
            "created": datetime.now().isoformat(timespec="seconds"),
            "identity": identity_metadata,
        }
        with _manifest_lock:
            manifest = {}
            try:
                with open(self.manifest_path, encoding="utf-8") as handle:
                    loaded = json.load(handle)
                if isinstance(loaded, dict):
                    manifest = loaded
            except (OSError, ValueError):
                pass
            manifest[f"{digest}.npy"] = metadata
            os.makedirs(os.path.dirname(self.manifest_path), exist_ok=True)
            fd, temporary = tempfile.mkstemp(
                prefix=".manifest.", suffix=".json.tmp",
                dir=os.path.dirname(self.manifest_path),
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as handle:
                    json.dump(manifest, handle, ensure_ascii=False, indent=2)
                    handle.write("\n")
                    handle.flush()
                    os.fsync(handle.fileno())
                replace_file(temporary, self.manifest_path)
            finally:
                with contextlib.suppress(FileNotFoundError):
                    os.unlink(temporary)


class LabelEmbeddingCache(EmbeddingCache):
    """Reuse individual labels under an exact text-encoder identity.

    Each column is independent of the other labels in a classifier. Reuse
    the existing atomic publication, validation and single-flight machinery
    for each column, keeping model/tokenizer/prompt changes isolated.
    """

    def __init__(self, cache_dir, identity, embedding_dim=None):
        text_identity = {k: v for k, v in identity.items() if k != "labels"}
        super().__init__(os.path.join(cache_dir, "labels", identity_digest(text_identity)))
        self.embedding_dim = embedding_dim

    def read(self, label):
        try:
            return self._load(
                identity_digest({"labels": [label]}), 1,
                embedding_dim=self.embedding_dim,
            )[:, 0]
        except (EOFError, OSError, ValueError):
            return None

    def resolve(self, label, compute, cancel_check=None):
        value, _ = self.get_or_compute(
            {"labels": [label]}, lambda: compute()[:, None],
            embedding_dim=self.embedding_dim, cancel_check=cancel_check,
        )
        return value[:, 0]

    def seed(self, labels, value):
        """Make a verified whole-set cache hit reusable by other label sets."""
        _validate_payload(value, len(labels), embedding_dim=self.embedding_dim)
        for index, label in enumerate(labels):
            if self.read(label) is None:
                self._publish(identity_digest({"labels": [label]}), value[:, index:index + 1], 1)

    def _update_manifest(self, digest, identity, value):
        # The directory identifies the text encoder and the filename the
        # label. Avoid rewriting a manifest for every individual label.
        pass
