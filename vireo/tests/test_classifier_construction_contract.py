"""Keep production Classifier construction behind the shared model cache."""

import ast
from pathlib import Path

from classifier_cache import (
    acquire_cached_classifier,
    classifier_cache_key,
    model_files_fingerprint,
)
from model_cache import reset_default_cache_for_tests


def _callee_name(node):
    """Return the called name for ``f(...)`` and ``mod.f(...)`` alike."""
    func = node.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def _factory_arguments(tree):
    """Collect every AST node passed as ``factory=`` to the cache acquirer.

    Only calls whose callee is ``acquire_cached_classifier`` count. A
    ``factory=`` keyword on some unrelated call does not satisfy the
    contract, so an inline ``Classifier(...)`` cannot be laundered through a
    lookalike helper.
    """
    factories = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if _callee_name(node) != "acquire_cached_classifier":
            continue
        for keyword in node.keywords:
            if keyword.arg == "factory":
                factories.append(keyword.value)
    return factories


def test_production_classifier_calls_are_cache_factories():
    vireo_dir = Path(__file__).resolve().parents[1]
    violations = []

    for path in vireo_dir.rglob("*.py"):
        if "tests" in path.relative_to(vireo_dir).parts:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        parents = {}
        for node in ast.walk(tree):
            for child in ast.iter_child_nodes(node):
                parents[child] = node

        factories = _factory_arguments(tree)
        # Lambdas are matched by identity; named functions by the name the
        # acquirer was actually handed.
        factory_lambdas = {
            id(value) for value in factories if isinstance(value, ast.Lambda)
        }
        factory_names = {
            value.id for value in factories if isinstance(value, ast.Name)
        }

        for node in ast.walk(tree):
            if not (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "Classifier"
            ):
                continue
            current = parents.get(node)
            routed = False
            while current is not None:
                if isinstance(current, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    # A def only counts when this module hands that exact name
                    # to acquire_cached_classifier as its factory.
                    routed = current.name in factory_names
                    break
                if isinstance(current, ast.Lambda):
                    routed = id(current) in factory_lambdas
                    break
                current = parents.get(current)
            if not routed:
                violations.append(
                    f"{path.relative_to(vireo_dir).as_posix()}:{node.lineno}"
                )

    assert not violations, (
        "Classifier construction must be a factory passed to "
        f"acquire_cached_classifier: {', '.join(violations)}"
    )


def test_contract_rejects_a_factory_keyword_on_an_unrelated_call():
    """The scan must not accept any function merely named
    ``_construct_classifier``, nor any ``factory=`` keyword on some other
    call. Both were previously enough to pass."""
    source = """
def _construct_classifier():
    return Classifier(labels=[])

some_other_helper(factory=lambda: Classifier(labels=[]))
"""
    tree = ast.parse(source)
    assert _factory_arguments(tree) == []


def test_contract_accepts_named_and_lambda_factories_on_the_acquirer():
    source = """
def _construct_classifier():
    return Classifier(labels=[])

acquire_cached_classifier(model_str="m", factory=_construct_classifier)
acquire_cached_classifier(model_str="m", factory=lambda: Classifier(labels=[]))
"""
    tree = ast.parse(source)
    factories = _factory_arguments(tree)
    assert len(factories) == 2
    assert isinstance(factories[0], ast.Name)
    assert factories[0].id == "_construct_classifier"
    assert isinstance(factories[1], ast.Lambda)


def test_optional_files_flip_fingerprint_when_they_appear_on_disk(tmp_path):
    """Regression: a Repair that downloads a declared-optional artifact
    (e.g. timm's ``label_descriptions.json``, or bioclip-2.5's ToL files)
    must invalidate any pre-repair cache entry. Without folding the
    optional artifacts into ``files=`` — the two production sites pass
    only the *required* manifest — the fingerprint would not change and
    the pre-repair classifier (constructed without the artifact and
    therefore emitting different labels) would keep being reused."""
    model_dir = tmp_path / "model"
    model_dir.mkdir()
    (model_dir / "text_encoder.onnx").write_bytes(b"weights")
    (model_dir / "config.json").write_bytes(b"config")

    required = ["text_encoder.onnx", "config.json"]
    optional = ["label_descriptions.json"]

    before = model_files_fingerprint(
        str(model_dir), files=required, optional_files=optional,
    )
    key_before = classifier_cache_key(
        model_type="timm",
        model_str="timm-a",
        weights_path=str(model_dir),
        labels=["bird"],
        files=required,
        optional_files=optional,
    )

    # Repair lands the optional artifact.
    (model_dir / "label_descriptions.json").write_bytes(b"descriptions")

    after = model_files_fingerprint(
        str(model_dir), files=required, optional_files=optional,
    )
    key_after = classifier_cache_key(
        model_type="timm",
        model_str="timm-a",
        weights_path=str(model_dir),
        labels=["bird"],
        files=required,
        optional_files=optional,
    )

    assert before != after, (
        "optional artifact appearing on disk must flip the fingerprint"
    )
    assert key_before != key_after, (
        "optional artifact appearing on disk must flip the cache key so a "
        "post-Repair caller does not reuse the pre-Repair classifier"
    )
    assert any(name == "label_descriptions.json" for name, *_ in after)


def test_absent_optional_files_do_not_bloat_the_fingerprint(tmp_path):
    """An optional artifact never present on disk must not appear in the
    fingerprint at all — otherwise every fresh install would produce a
    fingerprint distinguishable only by a phantom entry, and existing
    tests that build a fingerprint from just the required manifest would
    stop matching production."""
    model_dir = tmp_path / "model"
    model_dir.mkdir()
    (model_dir / "text_encoder.onnx").write_bytes(b"weights")
    (model_dir / "config.json").write_bytes(b"config")

    required = ["text_encoder.onnx", "config.json"]

    baseline = model_files_fingerprint(str(model_dir), files=required)
    with_optional = model_files_fingerprint(
        str(model_dir),
        files=required,
        optional_files=["label_descriptions.json"],
    )

    assert baseline == with_optional


def test_optional_files_snapshot_prevents_pre_heal_instance_reuse(tmp_path):
    """Regression: an async heal that lands during construction must
    not rekey the pre-heal classifier under the healed fingerprint.

    Before this fix, ``acquire_cached_classifier``'s ``post_load_key``
    recomputed the fingerprint from a fresh disk stat, so
    ``TimmClassifier``'s background ``label_descriptions.json`` heal
    completing while the ONNX session was still loading would rekey the
    stale pre-heal instance — the one that had already read the missing
    file as empty — under the healed fingerprint. Every later acquirer
    with that same healed fingerprint would then reuse the stale
    classifier, leaking raw scientific names indefinitely.

    The instance now records the state it actually consumed under
    ``optional_files_snapshot``; ``post_load_key`` keys the entry by
    that snapshot so a heal that landed underneath us is invisible to
    the pre-heal fingerprint, and the next acquire — whose fingerprint
    includes the healed file — is a cache miss.
    """
    import os as _os

    reset_default_cache_for_tests()
    model_dir = tmp_path / "model"
    model_dir.mkdir()
    (model_dir / "text_encoder.onnx").write_bytes(b"weights")
    (model_dir / "config.json").write_bytes(b"config")

    required = ["text_encoder.onnx", "config.json"]
    optional = ["label_descriptions.json"]

    class _Instance:
        def __init__(self, consumed_label_desc):
            # ``None`` -- the optional file was not consumed at
            # construction (e.g. absent or unparseable when we read
            # it). A stat tuple -- the file was read successfully.
            self.optional_files_snapshot = {
                "label_descriptions.json": consumed_label_desc,
            }

    def _factory_that_heals_mid_construction():
        # Simulate: instance reads label_descriptions.json (missing),
        # then a background heal publishes it before we return.
        instance = _Instance(None)
        (model_dir / "label_descriptions.json").write_bytes(b"healed")
        return instance

    try:
        pre_heal = acquire_cached_classifier(
            model_type="timm",
            model_str="timm-a",
            weights_path=str(model_dir),
            labels=["bird"],
            factory=_factory_that_heals_mid_construction,
            files=required,
            optional_files=optional,
        )
        try:
            assert (model_dir / "label_descriptions.json").exists(), (
                "the fake heal must have published the file"
            )

            calls = []

            def _second_factory():
                calls.append(1)
                stat = _os.stat(model_dir / "label_descriptions.json")
                return _Instance((stat.st_size, int(stat.st_mtime_ns)))

            post_heal = acquire_cached_classifier(
                model_type="timm",
                model_str="timm-a",
                weights_path=str(model_dir),
                labels=["bird"],
                factory=_second_factory,
                files=required,
                optional_files=optional,
            )
            try:
                assert calls == [1], (
                    "post-heal acquire must build a fresh classifier "
                    "instead of reusing the pre-heal instance keyed by "
                    "a healed fingerprint it never actually consumed"
                )
                assert pre_heal.__enter__() is not post_heal.__enter__()
            finally:
                post_heal.release()
        finally:
            pre_heal.release()
    finally:
        reset_default_cache_for_tests()


def test_missing_optional_files_snapshot_falls_back_to_disk(tmp_path):
    """Classifiers with no ``optional_files_snapshot`` (bioclip and
    friends without an async heal) keep the disk-based post_load_key
    behavior — so a Repair that fills in an optional artifact still
    invalidates the pre-Repair cache entry."""
    reset_default_cache_for_tests()
    model_dir = tmp_path / "model"
    model_dir.mkdir()
    (model_dir / "text_encoder.onnx").write_bytes(b"weights")
    (model_dir / "config.json").write_bytes(b"config")

    required = ["text_encoder.onnx", "config.json"]
    optional = ["label_descriptions.json"]

    calls = []

    class _Legacy:
        pass

    def _factory():
        calls.append(1)
        return _Legacy()

    try:
        first = acquire_cached_classifier(
            model_type="timm",
            model_str="timm-a",
            weights_path=str(model_dir),
            labels=["bird"],
            factory=_factory,
            files=required,
            optional_files=optional,
        )
        # Repair lands the optional artifact between acquires.
        (model_dir / "label_descriptions.json").write_bytes(b"descriptions")
        second = acquire_cached_classifier(
            model_type="timm",
            model_str="timm-a",
            weights_path=str(model_dir),
            labels=["bird"],
            factory=_factory,
            files=required,
            optional_files=optional,
        )
        try:
            assert calls == [1, 1], (
                "instances without a snapshot fall back to disk-based "
                "fingerprinting; Repair still invalidates the entry"
            )
        finally:
            second.release()
        first.release()
    finally:
        reset_default_cache_for_tests()


def test_acquire_calls_notify_reuse_on_every_acquire(tmp_path):
    """Regression for the "bounded retry unreachable from cache hits" gap.

    ``TimmClassifier``'s ``label_descriptions.json`` self-heal is
    spawned from ``__init__``, but the process-wide ``ModelCache``
    reuses the constructed instance across later classify jobs without
    re-entering ``__init__``. If the first probe fails while HF is
    unreachable, no later job would ever re-arm the bounded retry —
    scientific names would leak indefinitely even after connectivity
    returns.

    ``acquire_cached_classifier`` now invokes ``instance.notify_reuse()``
    on every acquire so the classifier's own retry state machine runs
    from the acquisition path, not just the factory path. This test
    proves that hook fires on both cache-miss and cache-hit acquires.
    """
    reset_default_cache_for_tests()
    model_dir = tmp_path / "model"
    model_dir.mkdir()
    (model_dir / "text_encoder.onnx").write_bytes(b"weights")
    (model_dir / "config.json").write_bytes(b"config")

    required = ["text_encoder.onnx", "config.json"]

    class _Instance:
        def __init__(self):
            self.reuse_calls = 0

        def notify_reuse(self):
            self.reuse_calls += 1

    factory_calls = []

    def _factory():
        factory_calls.append(1)
        return _Instance()

    try:
        first = acquire_cached_classifier(
            model_type="bioclip",
            model_str="model-a",
            weights_path=str(model_dir),
            labels=["bird"],
            factory=_factory,
            files=required,
        )
        try:
            instance = first.__enter__()
            assert factory_calls == [1]
            assert instance.reuse_calls == 1, (
                "notify_reuse must fire on cache miss too"
            )

            second = acquire_cached_classifier(
                model_type="bioclip",
                model_str="model-a",
                weights_path=str(model_dir),
                labels=["bird"],
                factory=_factory,
                files=required,
            )
            try:
                assert factory_calls == [1], (
                    "same fingerprint must be a cache hit — the factory "
                    "does not run a second time"
                )
                assert second.__enter__() is instance
                assert instance.reuse_calls == 2, (
                    "the hook must also fire on cache hit — otherwise "
                    "instance-owned bounded retries never re-arm"
                )
            finally:
                second.release()
        finally:
            first.release()
    finally:
        reset_default_cache_for_tests()


def test_acquire_survives_a_notify_reuse_that_raises(tmp_path):
    """A crashing ``notify_reuse`` on a shared classifier must not brick
    every acquirer. The hook is best-effort background bookkeeping; it
    is not on the classify path's data flow, so an exception from it is
    logged and swallowed."""
    reset_default_cache_for_tests()
    model_dir = tmp_path / "model"
    model_dir.mkdir()
    (model_dir / "text_encoder.onnx").write_bytes(b"weights")

    class _Instance:
        def notify_reuse(self):
            raise RuntimeError("hook exploded")

    try:
        handle = acquire_cached_classifier(
            model_type="bioclip",
            model_str="model-a",
            weights_path=str(model_dir),
            labels=["bird"],
            factory=_Instance,
            files=["text_encoder.onnx"],
        )
        try:
            # The value is still delivered; the hook's failure is isolated.
            assert isinstance(handle.__enter__(), _Instance)
        finally:
            handle.release()
    finally:
        reset_default_cache_for_tests()


def test_shared_classifier_cache_preserves_authoritative_label_order(tmp_path):
    reset_default_cache_for_tests()
    model_dir = tmp_path / "model"
    model_dir.mkdir()
    (model_dir / "weights").write_bytes(b"weights")
    calls = []

    def acquire(labels):
        return acquire_cached_classifier(
            model_type="bioclip",
            model_str="model-a",
            weights_path=str(model_dir),
            labels=labels,
            factory=lambda: calls.append(tuple(labels)) or object(),
        )

    first = acquire(["bird", "cat"])
    same = acquire([" bird ", "cat"])
    reordered = acquire(["cat", "bird"])
    try:
        assert first.__enter__() is same.__enter__()
        assert reordered.__enter__() is not first.__enter__()
        assert calls == [("bird", "cat"), ("cat", "bird")]
    finally:
        first.release()
        same.release()
        reordered.release()
        reset_default_cache_for_tests()
