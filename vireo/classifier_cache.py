"""Shared construction path for process-wide classifier session reuse."""

import hashlib
import json
import logging
import os

from embedding_cache import canonicalize_labels
from model_cache import get_default_cache

log = logging.getLogger(__name__)


def _ordered_labels_identity(labels):
    if labels is None:
        return "__tol__"
    canonical = canonicalize_labels(labels)
    body = json.dumps(
        [canonical, getattr(labels, "identities", {})],
        ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(body).hexdigest()


def model_files_fingerprint(
    weights_path, files=None, optional_files=None,
    optional_files_state=None,
):
    """Cheap identity for in-process invalidation after model replacement.

    ``optional_files`` — filenames declared as ``optional_files`` in the
    model's KNOWN_MODELS entry (best-effort downloads, e.g. timm's
    ``label_descriptions.json`` or bioclip-2.5's ToL artifacts). Only
    those optional files present on disk right now are folded into the
    fingerprint. That way a Repair that lands a previously absent
    optional file flips the fingerprint and prevents the pre-repair
    cached classifier — which was constructed without that artifact and
    therefore emits different labels — from being reused.

    ``optional_files_state`` — a ``{filename: (size, mtime_ns) | None}``
    dict overriding current disk state for the named optional files.
    Files absent from the dict fall back to a live disk stat. Use this
    when a constructed classifier has recorded which optional artifacts
    it actually consumed, so the fingerprint post-construction matches
    the instance rather than a disk state that a concurrent heal has
    already changed underneath it — otherwise the stale pre-heal
    instance would be rekeyed to the healed fingerprint and every later
    acquirer would reuse it.
    """
    if not weights_path:
        return None
    state = optional_files_state or {}
    if files:
        names = set(files)
        if optional_files and os.path.isdir(weights_path):
            for name in optional_files:
                if name in names:
                    continue
                if name in state:
                    if state[name] is not None:
                        names.add(name)
                elif os.path.isfile(os.path.join(weights_path, name)):
                    names.add(name)
        names = sorted(names)
    elif os.path.isdir(weights_path):
        names = sorted(
            name for name in os.listdir(weights_path)
            if os.path.isfile(os.path.join(weights_path, name))
        )
    elif os.path.isfile(weights_path):
        names = ["__file__"]
    else:
        return None

    parts = []
    for name in names:
        if name in state:
            snap = state[name]
            if snap is None:
                parts.append((name, None, None))
            else:
                size, mtime_ns = snap
                parts.append((name, int(size), int(mtime_ns)))
            continue
        path = weights_path if name == "__file__" else os.path.join(
            weights_path, name
        )
        try:
            stat = os.stat(path)
            parts.append((name, stat.st_size, int(stat.st_mtime_ns)))
        except OSError:
            parts.append((name, None, None))
    return tuple(parts)


def classifier_cache_key(
    *,
    model_type,
    model_str,
    weights_path,
    labels,
    files=None,
    optional_files=None,
    taxonomy_fingerprint=None,
    optional_files_state=None,
):
    return (
        "classifier-v2",
        model_type,
        model_str,
        os.path.abspath(weights_path) if weights_path else None,
        _ordered_labels_identity(labels),
        taxonomy_fingerprint,
        model_files_fingerprint(
            weights_path, files, optional_files,
            optional_files_state=optional_files_state,
        ),
    )


def acquire_cached_classifier(
    *,
    model_type,
    model_str,
    weights_path,
    labels,
    factory,
    files=None,
    optional_files=None,
    taxonomy_fingerprint=None,
    cancel_check=None,
):
    """Acquire a refcounted classifier, rekeying after in-place self-heal.

    ``cancel_check`` propagates the caller's cancellation signal down to the
    shared-cache waiter loop so a job blocked here while another caller's
    factory is loading a text encoder or computing label embeddings can
    still cancel promptly.

    ``optional_files`` — best-effort declared artifacts (see
    ``model_files_fingerprint``). Callers that pass an explicit ``files=``
    list (which typically covers only the required manifest) should also
    pass ``optional_files=`` so a Repair that fills in a previously absent
    optional artifact invalidates the pre-repair classifier entry.
    """

    def _key(state=None):
        return classifier_cache_key(
            model_type=model_type,
            model_str=model_str,
            weights_path=weights_path,
            labels=labels,
            files=files,
            optional_files=optional_files,
            taxonomy_fingerprint=taxonomy_fingerprint,
            optional_files_state=state,
        )

    def _post_load_key(instance):
        # If the constructed classifier recorded which optional
        # artifacts it actually consumed, key the entry by that snapshot
        # instead of a fresh disk read. Prevents an async heal that
        # lands during construction (e.g. TimmClassifier's background
        # ``label_descriptions.json`` repair completing while the ONNX
        # session loads) from rekeying a pre-heal instance under the
        # healed fingerprint — every later acquirer with that same
        # fingerprint would otherwise reuse the stale classifier that
        # never read the healed file, leaking raw scientific names
        # indefinitely.
        snapshot = getattr(instance, "optional_files_snapshot", None)
        return _key(state=snapshot)

    handle = get_default_cache().acquire(
        _key(),
        factory,
        post_load_key=_post_load_key,
        cancel_check=cancel_check,
    )

    # Give the loaded classifier a chance to run bounded-retry
    # background work that only its ``__init__`` normally kicks off.
    # On a cache hit the shared cache serves the previously constructed
    # instance without calling ``__init__`` again, so a classifier that
    # spawned a self-heal (see ``TimmClassifier._spawn_label_desc_heal``)
    # and hit a transient failure would otherwise never re-probe:
    # ``__init__`` is the only caller, and cached-only jobs never touch
    # it. Firing the hook here — under the acquisition path — makes the
    # bounded retry state machine reachable from every job. The classifier
    # is responsible for internal dedup (the heal already uses a lock +
    # ``in_flight`` marker), so calling this on a fresh construction is a
    # harmless no-op.
    hook = getattr(handle.value, "notify_reuse", None)
    if hook is not None:
        try:
            hook()
        except Exception:  # broad: a caller's reuse hook must not break the cache hit
            log.exception("classifier notify_reuse hook raised; ignoring")
    return handle
