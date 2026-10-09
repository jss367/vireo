"""Tests for ONNX Runtime utility module."""
import numpy as np
import pytest
from testing.waits import synchronization_timeout


def test_get_providers_returns_list():
    from vireo.onnx_runtime import get_providers
    providers = get_providers()
    assert isinstance(providers, list)
    assert "CPUExecutionProvider" in providers


def test_get_providers_cpu_always_last():
    from vireo.onnx_runtime import get_providers
    providers = get_providers()
    assert providers[-1] == "CPUExecutionProvider"


def test_preprocess_image_shape():
    from PIL import Image

    from vireo.onnx_runtime import preprocess_image
    img = Image.new("RGB", (800, 600))
    arr = preprocess_image(img, size=(224, 224), mean=[0.5, 0.5, 0.5], std=[0.5, 0.5, 0.5])
    assert arr.shape == (1, 3, 224, 224)
    assert arr.dtype == np.float32


def test_preprocess_image_normalization():
    from PIL import Image

    from vireo.onnx_runtime import preprocess_image
    # All-white image (255, 255, 255) -> after /255 = 1.0 -> after norm with mean=0.5, std=0.5 -> 1.0
    img = Image.new("RGB", (10, 10), (255, 255, 255))
    arr = preprocess_image(img, size=(10, 10), mean=[0.5, 0.5, 0.5], std=[0.5, 0.5, 0.5])
    np.testing.assert_allclose(arr[0, 0, 0, 0], 1.0, atol=0.01)


def test_preprocess_image_center_crop():
    from PIL import Image

    from vireo.onnx_runtime import preprocess_image
    # Non-square image should be center-cropped
    img = Image.new("RGB", (300, 100))
    arr = preprocess_image(img, size=(50, 50), mean=[0.5, 0.5, 0.5], std=[0.5, 0.5, 0.5], center_crop=True)
    assert arr.shape == (1, 3, 50, 50)


def test_softmax():
    from vireo.onnx_runtime import softmax
    logits = np.array([1.0, 2.0, 3.0])
    probs = softmax(logits)
    assert abs(probs.sum() - 1.0) < 1e-6
    assert probs[2] > probs[1] > probs[0]


def test_nms_basic():
    from vireo.onnx_runtime import nms
    # Two overlapping boxes, one with higher score
    boxes = np.array([
        [10, 10, 50, 50],
        [12, 12, 52, 52],  # overlaps heavily with first
        [100, 100, 150, 150],  # separate
    ], dtype=np.float32)
    scores = np.array([0.9, 0.8, 0.7], dtype=np.float32)
    keep = nms(boxes, scores, iou_threshold=0.5)
    assert 0 in keep  # highest score kept
    assert 2 in keep  # non-overlapping kept
    assert 1 not in keep  # suppressed


def test_create_session_excludes_coreml_for_external_data(tmp_path):
    """CoreML should be excluded when a .onnx.data sidecar exists."""
    from unittest.mock import MagicMock, patch

    model_file = tmp_path / "model.onnx"
    sidecar = tmp_path / "model.onnx.data"
    model_file.write_bytes(b"fake")
    sidecar.write_bytes(b"fake")

    mock_session = MagicMock()
    mock_session.get_providers.return_value = ["CPUExecutionProvider"]

    with patch("vireo.onnx_runtime.get_providers", return_value=[
        "CoreMLExecutionProvider", "CPUExecutionProvider",
    ]), patch("onnxruntime.InferenceSession", return_value=mock_session) as mock_cls:
        from vireo.onnx_runtime import create_session
        create_session(str(model_file))

    # CoreML must have been stripped from the providers list
    _args, kwargs = mock_cls.call_args
    assert "CoreMLExecutionProvider" not in kwargs.get("providers", _args[1] if len(_args) > 1 else [])


def test_create_session_keeps_coreml_without_sidecar(tmp_path):
    """CoreML should be kept when there is no .onnx.data sidecar."""
    from unittest.mock import MagicMock, patch

    model_file = tmp_path / "model.onnx"
    model_file.write_bytes(b"fake")
    # No sidecar created

    mock_session = MagicMock()
    mock_session.get_providers.return_value = [
        "CoreMLExecutionProvider", "CPUExecutionProvider",
    ]

    with patch("vireo.onnx_runtime.get_providers", return_value=[
        "CoreMLExecutionProvider", "CPUExecutionProvider",
    ]), patch("onnxruntime.InferenceSession", return_value=mock_session) as mock_cls:
        from vireo.onnx_runtime import create_session
        create_session(str(model_file))

    # CoreML must still be present
    _args, kwargs = mock_cls.call_args
    providers_used = kwargs.get("providers", _args[1] if len(_args) > 1 else [])
    assert "CoreMLExecutionProvider" in providers_used
    assert "CPUExecutionProvider" in providers_used


def test_create_session_configures_threads_from_cpu_grant(tmp_path):
    """ONNX native pools must match the enforceable process grant."""
    from unittest.mock import MagicMock, patch

    import resource_ledger

    from vireo.onnx_runtime import create_session, session_cpu_threads

    model_file = tmp_path / "model.onnx"
    model_file.write_bytes(b"fake")
    mock_session = MagicMock()
    mock_session.get_providers.return_value = ["CPUExecutionProvider"]
    ledger = resource_ledger.ResourceLedger(cpu_capacity=3)
    previous = resource_ledger._set_resource_ledger_for_tests(ledger)
    try:
        with patch(
            "vireo.onnx_runtime.get_providers",
            return_value=["CPUExecutionProvider"],
        ), patch(
            "onnxruntime.InferenceSession", return_value=mock_session,
        ) as mock_cls:
            session = create_session(str(model_file))
    finally:
        resource_ledger._set_resource_ledger_for_tests(previous)

    options = mock_cls.call_args.kwargs["sess_options"]
    assert options.intra_op_num_threads == 3
    assert options.inter_op_num_threads == 1
    assert session_cpu_threads(session) == 3
    assert ledger.snapshot()["cpu"]["allocated"] == 0
    assert ledger.snapshot()["lanes"]["model_construction"]["allocated"] == 0


def test_create_session_honors_construction_cancel_probe(tmp_path):
    """Cold interactive loads can cancel while waiting for their CPU grant."""
    from unittest.mock import patch

    import resource_ledger
    from resource_ledger import ResourceWaitCancelled

    from vireo.onnx_runtime import create_session

    model_file = tmp_path / "model.onnx"
    model_file.write_bytes(b"fake")
    ledger = resource_ledger.ResourceLedger(cpu_capacity=1)
    previous = resource_ledger._set_resource_ledger_for_tests(ledger)
    holder = ledger.acquire(resource_ledger.ResourceRequest(
        cpu=resource_ledger.CpuRequest(1, 1, 1),
    ))
    try:
        with patch(
            "vireo.onnx_runtime.get_providers",
            return_value=["CPUExecutionProvider"],
        ), patch("onnxruntime.InferenceSession") as create_ort_session:
            with pytest.raises(ResourceWaitCancelled):
                create_session(
                    str(model_file), cancel_check=lambda: True,
                )
            create_ort_session.assert_not_called()
    finally:
        holder.release()
        resource_ledger._set_resource_ledger_for_tests(previous)


def test_create_session_waits_for_full_inference_budget(tmp_path):
    """Construction must hold the full inference-thread budget in permits.

    ``intra_op_num_threads`` is fixed at ``session_thread_count`` (8 on a
    12-permit machine) so subsequent CPU inference calls run at the
    intended profile. ONNX exercises that pool during graph optimization
    and kernel prepacking, so accepting a smaller construction grant
    would let those native threads exceed the process budget for the
    duration of the load. Verify that construction blocks until the full
    inference budget is free, and that the session is then sized from
    the inference profile — independent of the state of contention at
    construction time.
    """
    import contextlib
    import threading
    from unittest.mock import MagicMock, patch

    import resource_ledger

    from vireo.onnx_runtime import create_session, session_cpu_threads

    model_file = tmp_path / "model.onnx"
    model_file.write_bytes(b"fake")
    mock_session = MagicMock()
    mock_session.get_providers.return_value = ["CPUExecutionProvider"]
    ledger = resource_ledger.ResourceLedger(cpu_capacity=12)
    previous = resource_ledger._set_resource_ledger_for_tests(ledger)
    # Reserve 10 permits so only 2 are free — less than the 8-thread
    # inference budget the session will run at.
    holder = ledger.acquire(resource_ledger.ResourceRequest(
        cpu=resource_ledger.CpuRequest(10, 10, 10),
    ))
    result = {}
    ready_to_construct = threading.Event()

    def _create():
        try:
            with patch(
                "vireo.onnx_runtime.get_providers",
                return_value=["CPUExecutionProvider"],
            ), patch(
                "onnxruntime.InferenceSession", return_value=mock_session,
            ) as mock_cls:
                # ``mock_cls`` is captured so the assertions after the join
                # can inspect the ``SessionOptions`` create_session built.
                result["mock_cls"] = mock_cls
                ready_to_construct.set()
                result["session"] = create_session(str(model_file))
        except BaseException as exc:  # pragma: no cover - defensive
            result["error"] = exc

    thread = threading.Thread(target=_create)
    thread.start()
    try:
        # Construction should be blocked waiting on CPU permits.
        assert ready_to_construct.wait(timeout=synchronization_timeout(1.0))
        thread.join(timeout=0.5)
        assert thread.is_alive(), (
            "create_session must wait until the full inference budget "
            "is free, but it completed with only 2 permits available"
        )
        assert "session" not in result

        # Release the reservation; construction now completes with the
        # full 8-permit grant.
        holder.release()
        thread.join(timeout=synchronization_timeout(5.0))
        assert not thread.is_alive()
    finally:
        # Belt-and-braces: if anything went wrong above, don't leave the
        # holder retained or the thread running.
        with contextlib.suppress(Exception):
            holder.release()
        if thread.is_alive():
            thread.join(timeout=synchronization_timeout(5.0))
        resource_ledger._set_resource_ledger_for_tests(previous)

    assert result.get("error") is None
    options = result["mock_cls"].call_args.kwargs["sess_options"]
    # The session must be sized from the inference profile (preferred=8),
    # and the construction lease that granted it must also have held 8.
    assert options.intra_op_num_threads == 8
    assert options.inter_op_num_threads == 1
    assert session_cpu_threads(result["session"]) == 8


def test_nonweak_session_thread_registry_is_identity_safe_and_bounded(monkeypatch):
    """The compatibility registry cannot leak stale id-based budgets."""
    from collections import OrderedDict

    import vireo.onnx_runtime as runtime

    class NonWeakSession:
        __slots__ = ()

    registry = OrderedDict()
    monkeypatch.setattr(runtime, "_SESSION_CPU_THREADS_FALLBACK", registry)

    target = NonWeakSession()
    runtime._remember_session_cpu_threads(target, 4)
    assert runtime.session_cpu_threads(target) == 4

    impostor = NonWeakSession()
    registry[id(target)] = (impostor, 7)
    assert runtime.session_cpu_threads(target, default=2) == 2
    assert id(target) not in registry

    sessions = [
        NonWeakSession()
        for _ in range(runtime._SESSION_CPU_THREADS_FALLBACK_LIMIT + 5)
    ]
    for session in sessions:
        runtime._remember_session_cpu_threads(session, 3)
    assert len(registry) == runtime._SESSION_CPU_THREADS_FALLBACK_LIMIT


def test_production_onnx_sessions_use_budgeted_factory():
    """Direct session construction would bypass thread and load budgets.

    Detects three call shapes per module:
      * ``ort.InferenceSession(...)`` — attribute access on any name.
      * ``InferenceSession(...)`` — bare name imported directly.
      * ``AnyAlias(...)`` where ``AnyAlias`` came from
        ``from onnxruntime import InferenceSession as AnyAlias``.

    Resolving aliases per module closes the ``as`` gap that would
    otherwise let a rename bypass the budgeted factory.
    """
    import ast
    from pathlib import Path

    package_root = Path(__file__).resolve().parents[1]
    violations = []
    for path in package_root.rglob("*.py"):
        if "tests" in path.parts or path.name == "onnx_runtime.py":
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        # Track every module-local name that resolves to
        # ``onnxruntime.InferenceSession``. Always include the bare name so
        # constructors accessed as ``InferenceSession(...)`` after an
        # unaliased import still count. ``from onnxruntime.<sub> import
        # InferenceSession`` variants are treated the same as the top-level
        # import because they resolve to the same class.
        aliases = {"InferenceSession"}
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom):
                continue
            module = node.module or ""
            if module != "onnxruntime" and not module.startswith(
                "onnxruntime."
            ):
                continue
            for alias in node.names:
                if alias.name == "InferenceSession":
                    aliases.add(alias.asname or alias.name)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            function = node.func
            is_constructor = (
                isinstance(function, ast.Attribute)
                and function.attr == "InferenceSession"
            ) or (
                isinstance(function, ast.Name)
                and function.id in aliases
            )
            if is_constructor:
                violations.append(f"{path.relative_to(package_root)}:{node.lineno}")

    assert violations == [], (
        "Production ONNX sessions must use onnx_runtime.create_session: "
        + ", ".join(violations)
    )
