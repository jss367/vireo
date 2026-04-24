"""Tests for ONNX session self-healing on corrupt model files.

When onnxruntime fails to load a model because the file is corrupt or
truncated, the session loader should delete the bad files and invoke
the caller's re-download function, then retry once. If the re-downloaded
model still fails to load, a clear user-facing error must be raised
without infinite retry. Non-corruption errors (permission denied,
is-a-directory) must surface unchanged.
"""

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def _onnx_load_error(msg="[ONNXRuntimeError] : 7 : INVALID_PROTOBUF : "
                     "Load model from foo.onnx failed:"
                     "Protobuf parsing failed."):
    """Build an exception object that looks like a real onnxruntime
    InvalidProtobuf failure. Using a plain Exception avoids importing
    onnxruntime's private error classes in tests."""
    return Exception(msg)


def test_corrupt_model_triggers_redownload(tmp_path):
    """A corrupt .onnx file on disk should be deleted and the caller's
    redownload callable should be invoked, after which the loader retries
    session creation exactly once."""
    from onnx_runtime import create_session_with_self_heal

    model_path = tmp_path / "model.onnx"
    model_path.write_bytes(b"not a real onnx file")

    good_session = MagicMock()
    good_session.get_providers.return_value = ["CPUExecutionProvider"]

    call_count = {"n": 0}

    def fake_create(path):
        call_count["n"] += 1
        if call_count["n"] == 1:
            # First attempt sees the corrupt stub — raise an ONNX-style error.
            raise _onnx_load_error()
        # Second attempt, after redownload, succeeds.
        return good_session

    download_called = {"n": 0}

    def fake_redownload():
        download_called["n"] += 1
        # Simulate a fresh download writing plausible bytes.
        model_path.write_bytes(b"fresh bytes after download")

    with patch("onnx_runtime.create_session", side_effect=fake_create):
        result = create_session_with_self_heal(
            str(model_path),
            redownload=fake_redownload,
        )

    assert result is good_session, "self-heal should return the healed session"
    assert download_called["n"] == 1, "redownload must have run once"
    assert call_count["n"] == 2, "loader must retry session creation exactly once"
    # The file bytes should reflect the redownload, proving the old bytes were
    # replaced (and by implication that deletion preceded redownload).
    assert model_path.read_bytes() == b"fresh bytes after download"


def test_corrupt_model_deletes_file_before_redownload(tmp_path):
    """Self-heal must unlink the broken file before invoking redownload so
    a caller that resumes (e.g. huggingface_hub.hf_hub_download) cannot
    see the corrupt stub as a partial download to resume."""
    from onnx_runtime import create_session_with_self_heal

    model_path = tmp_path / "model.onnx"
    model_path.write_bytes(b"corrupt stub")

    observed_state = {"existed_at_redownload": None}

    def fake_redownload():
        observed_state["existed_at_redownload"] = model_path.exists()
        model_path.write_bytes(b"fresh")

    good_session = MagicMock()

    def fake_create(path):
        if observed_state["existed_at_redownload"] is None:
            # First attempt: we haven't redownloaded yet.
            raise _onnx_load_error()
        return good_session

    with patch("onnx_runtime.create_session", side_effect=fake_create):
        create_session_with_self_heal(
            str(model_path),
            redownload=fake_redownload,
        )

    assert observed_state["existed_at_redownload"] is False, (
        "corrupt model file must be deleted BEFORE redownload runs"
    )


def test_redownload_failure_surfaces_clearly(tmp_path):
    """If the session still fails to load after a redownload attempt, the
    self-heal wrapper must raise a clear user-facing error (not retry
    infinitely) and must preserve the underlying error as the cause."""
    from onnx_runtime import create_session_with_self_heal

    model_path = tmp_path / "model.onnx"
    model_path.write_bytes(b"corrupt")

    attempts = {"n": 0}

    def always_fails(path):
        attempts["n"] += 1
        # Both attempts raise an ONNX corruption-style error. The wrapper
        # should self-heal once and then surface a clean RuntimeError
        # rather than calling redownload a second time.
        raise _onnx_load_error(
            f"[ONNXRuntimeError] INVALID_PROTOBUF attempt {attempts['n']}"
        )

    def fake_redownload():
        # Simulate a "successful" download but of a still-broken file
        # (e.g. HF returned a 200 but the bytes are wrong).
        model_path.write_bytes(b"still broken")

    with patch("onnx_runtime.create_session", side_effect=always_fails):
        with pytest.raises(RuntimeError) as excinfo:
            create_session_with_self_heal(
                str(model_path),
                redownload=fake_redownload,
            )

    assert attempts["n"] == 2, (
        "self-heal must attempt load exactly twice (initial + one retry), "
        f"got {attempts['n']}"
    )
    # Must be user-facing — reference the model path and preserve the
    # underlying cause chain.
    assert "model.onnx" in str(excinfo.value)
    assert excinfo.value.__cause__ is not None, (
        "the re-raised error must preserve the underlying onnxruntime error"
    )


def test_non_corruption_error_is_not_swallowed(tmp_path):
    """Permission / OS-level errors must NOT trigger self-heal — they are
    not corruption signals. We must not delete the file or invoke
    redownload; the original error must surface unchanged."""
    from onnx_runtime import create_session_with_self_heal

    model_path = tmp_path / "model.onnx"
    model_path.write_bytes(b"bytes we must not delete")

    def fake_create(path):
        raise PermissionError(f"[Errno 13] Permission denied: '{path}'")

    download_called = {"n": 0}

    def fake_redownload():
        download_called["n"] += 1

    with patch("onnx_runtime.create_session", side_effect=fake_create):
        with pytest.raises(PermissionError):
            create_session_with_self_heal(
                str(model_path),
                redownload=fake_redownload,
            )

    assert download_called["n"] == 0, (
        "redownload must NOT run for non-corruption errors"
    )
    assert model_path.exists(), (
        "file must NOT be deleted for non-corruption errors"
    )
    assert model_path.read_bytes() == b"bytes we must not delete"


def test_is_a_directory_error_is_not_swallowed(tmp_path):
    """If the path is a directory (user misconfig), don't try to delete +
    redownload — surface the underlying error."""
    from onnx_runtime import create_session_with_self_heal

    # Create a directory where a file should be.
    model_path = tmp_path / "model.onnx"
    model_path.mkdir()

    def fake_create(path):
        raise IsADirectoryError(f"[Errno 21] Is a directory: '{path}'")

    download_called = {"n": 0}

    def fake_redownload():
        download_called["n"] += 1

    with patch("onnx_runtime.create_session", side_effect=fake_create):
        with pytest.raises(IsADirectoryError):
            create_session_with_self_heal(
                str(model_path),
                redownload=fake_redownload,
            )

    assert download_called["n"] == 0
    assert model_path.is_dir(), "directory must not be removed"


def test_no_redownload_callable_re_raises(tmp_path):
    """When no `redownload` callable is provided (caller has no recovery
    strategy, e.g. custom user-supplied model), the wrapper must surface
    the original error rather than silently deleting the file."""
    from onnx_runtime import create_session_with_self_heal

    model_path = tmp_path / "model.onnx"
    model_path.write_bytes(b"corrupt")

    def fake_create(path):
        raise _onnx_load_error()

    with patch("onnx_runtime.create_session", side_effect=fake_create):
        with pytest.raises(Exception) as excinfo:
            create_session_with_self_heal(str(model_path), redownload=None)

    # Original onnxruntime-style error survives — not a RuntimeError
    # wrapping it.
    assert "INVALID_PROTOBUF" in str(excinfo.value)
    # File is preserved so the user / next caller can inspect it.
    assert model_path.exists()


def test_build_self_heal_redownloader_known_model(tmp_path, monkeypatch):
    """models.build_self_heal_redownloader returns a callable that
    invokes download_model for the matching known-model id."""
    import models

    # Redirect DEFAULT_MODELS_DIR into tmp_path so the realpath check
    # matches a directory we control.
    monkeypatch.setattr(models, "DEFAULT_MODELS_DIR", str(tmp_path))
    km_id = models.KNOWN_MODELS[0]["id"]
    model_dir = tmp_path / km_id
    model_dir.mkdir()

    called_with = {}

    def fake_download(model_id, progress_callback=None):
        called_with["model_id"] = model_id
        return str(model_dir)

    monkeypatch.setattr(models, "download_model", fake_download)

    redownload = models.build_self_heal_redownloader(str(model_dir))
    assert callable(redownload)
    redownload()
    assert called_with["model_id"] == km_id


def test_build_self_heal_redownloader_unknown_path_returns_none(tmp_path):
    """Unknown / custom model directories return None so the self-heal
    wrapper surfaces the original error rather than silently deleting
    files we can't replace."""
    import models

    # A directory that doesn't match any KNOWN_MODELS entry.
    unknown_dir = tmp_path / "my-custom-model"
    unknown_dir.mkdir()

    assert models.build_self_heal_redownloader(str(unknown_dir)) is None
    assert models.build_self_heal_redownloader(None) is None
    assert models.build_self_heal_redownloader("") is None


def test_generic_load_failure_does_not_trigger_redownload(tmp_path):
    """Generic onnxruntime load-failure messages (e.g. provider load
    errors, ABI/compat mismatches) must NOT trigger delete+redownload.
    Only the narrow set of corruption-specific markers should. Deleting
    a valid model to recover from a CUDA provider error would be a
    multi-GB waste and still leave the real issue unresolved."""
    from onnx_runtime import create_session_with_self_heal

    model_path = tmp_path / "model.onnx"
    valid_bytes = b"valid model bytes we must not delete"
    model_path.write_bytes(valid_bytes)

    download_called = {"n": 0}

    def fake_redownload():
        download_called["n"] += 1

    # A realistic non-corruption onnxruntime error: CUDA provider init
    # fails, the message says "Load model from ... failed" but the file
    # bytes are fine.
    generic_msg = (
        "[ONNXRuntimeError] : 1 : FAIL : Load model from "
        "foo.onnx failed:Failed to load model because "
        "CUDA provider could not be initialized"
    )

    def fake_create(path):
        raise Exception(generic_msg)

    with patch("onnx_runtime.create_session", side_effect=fake_create):
        with pytest.raises(Exception) as excinfo:
            create_session_with_self_heal(
                str(model_path),
                redownload=fake_redownload,
            )

    assert "CUDA provider" in str(excinfo.value)
    assert download_called["n"] == 0, (
        "generic load failure must NOT trigger redownload"
    )
    assert model_path.read_bytes() == valid_bytes, (
        "valid model file must NOT be deleted for non-corruption errors"
    )


def test_external_data_sidecar_is_also_deleted(tmp_path):
    """When the model uses external data (.onnx.data sidecar), both the
    graph file and the sidecar must be deleted so a fresh download is
    forced to replace the complete on-disk state."""
    from onnx_runtime import create_session_with_self_heal

    model_path = tmp_path / "model.onnx"
    sidecar = tmp_path / "model.onnx.data"
    model_path.write_bytes(b"corrupt graph")
    sidecar.write_bytes(b"corrupt weights")

    state = {"redownloaded": False}
    files_at_redownload = {}

    def fake_redownload():
        files_at_redownload["graph"] = model_path.exists()
        files_at_redownload["sidecar"] = sidecar.exists()
        state["redownloaded"] = True
        model_path.write_bytes(b"fresh graph")
        sidecar.write_bytes(b"fresh weights")

    good_session = MagicMock()

    def fake_create(path):
        if not state["redownloaded"]:
            raise _onnx_load_error(
                "[ONNXRuntimeError] : 1 : FAIL : Failed to load external "
                "data file: missing bytes"
            )
        return good_session

    with patch("onnx_runtime.create_session", side_effect=fake_create):
        create_session_with_self_heal(
            str(model_path),
            redownload=fake_redownload,
        )

    assert files_at_redownload["graph"] is False, (
        "graph .onnx file must be deleted before redownload"
    )
    assert files_at_redownload["sidecar"] is False, (
        "external-data .onnx.data sidecar must also be deleted"
    )
