"""Fake-only scanner diagnostics: no scanner imports, pools, images, or files."""

import __future__

import ast
import builtins
import contextlib
import sys
from collections import deque
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from unittest.mock import Mock

VIREO = Path(__file__).resolve().parents[1]


def _selected(module, name, **namespace):
    tree = ast.parse((VIREO / module).read_text(encoding="utf-8"))
    node = next(
        node for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == name
    )
    namespace = {"contextlib": contextlib, "sys": sys, **namespace}
    exec(compile(
        ast.Module(body=[node], type_ignores=[]), module, "exec",
        flags=__future__.annotations.compiler_flag,
    ), namespace)
    return namespace[name]


class _Pause(Exception):
    pass


class _Cancelled(Exception):
    pass


def _worker_case(mode, logging_fails, terminate_fails=True):
    events = []
    original = KeyboardInterrupt() if mode == "interrupt" else _Cancelled("cancelled") if mode == "cancel" else RuntimeError("original worker failure")
    termination_error = RuntimeError("invented worker already gone")

    def terminate_first():
        events.append("terminate-first")
        if terminate_fails:
            raise termination_error

    def debug(*args, **kwargs):
        events.append("log")
        assert sys.exception() is termination_error and kwargs == {"exc_info": True}
        if logging_fails:
            raise RuntimeError("invented logger failure")

    class Pool:
        def __init__(self, **kwargs):
            self._processes = {
                1: SimpleNamespace(terminate=terminate_first),
                2: SimpleNamespace(terminate=lambda: events.append("terminate-second")),
            }

        def submit(self, *args):
            return object()

        def shutdown(self, **kwargs):
            events.append(("shutdown", kwargs))

    def check():
        if mode in {"error", "cancel", "interrupt"}:
            raise original

    fn = _selected(
        "scanner/features.py", "_pool_features", deque=deque,
        multiprocessing=SimpleNamespace(get_context=lambda method: object()),
        _SCAN_MP_METHOD="invented", ProcessPoolExecutor=Pool,
        _compute_file_features=lambda path: None, _ScanPauseRequested=_Pause,
        log=SimpleNamespace(debug=debug),
    )
    owner = SimpleNamespace(
        _check_cancelled_no_park=check, _feature_result=lambda future: ("p", "h"),
        _pause_pending=lambda: mode == "pause",
    )
    remaining = deque([("invented-photo", "invented-path")])
    gen = fn(owner, 2, remaining)
    if mode in {"error", "cancel", "interrupt"}:
        try:
            next(gen)
        except BaseException as error:
            assert error is original
        else:
            raise AssertionError("original failure was swallowed")
    elif mode == "pause":
        try:
            next(gen)
        except StopIteration as stop:
            assert stop.value is True
        else:
            raise AssertionError("pause yielded a photo")
        assert list(remaining) == [("invented-photo", "invented-path")]
    elif mode == "close":
        assert next(gen) == ("invented-photo", ("p", "h"))
        gen.close()
    else:
        assert list(gen) == [("invented-photo", ("p", "h"))]
        assert events == [("shutdown", {"wait": True})]
        return
    assert events == [
        "terminate-first", *(["log"] if terminate_fails else []), "terminate-second",
        ("shutdown", {"wait": True, "cancel_futures": True}),
    ]


def test_worker_diagnostic_failure_preserves_cleanup_and_control_flow():
    for mode in ("error", "cancel", "interrupt", "pause", "close"):
        for logging_fails in (False, True):
            _worker_case(mode, logging_fails)


def test_worker_success_does_not_log_an_inherited_exception():
    try:
        raise ValueError("caller exception")
    except ValueError:
        for mode in ("success", "error", "pause", "close"):
            _worker_case(mode, True, terminate_fails=False)


def _decoder(error=None, *, exit_error=None):
    if error is not None:
        return Mock(side_effect=error)
    image = SimpleNamespace(size=(100, 80), sizes=SimpleNamespace(width=100, height=80))
    manager = Mock()
    manager.__enter__ = Mock(return_value=image)
    manager.__exit__ = Mock(side_effect=exit_error, return_value=False)
    return Mock(return_value=manager)


def _read_metadata(extension, decoder, logger):
    def fake_import(name, *args, **kwargs):
        assert name == "rawpy"
        return SimpleNamespace(imread=decoder)

    fn = _selected(
        "scanner/indexing.py", "_read_file_metadata",
        __builtins__={**vars(builtins), "__import__": fake_import},
        Image=SimpleNamespace(open=decoder), RAW_EXTENSIONS={".nef"},
        _extract_dimensions=lambda *args, **kwargs: (None, None),
        _extract_timestamp=lambda *args: None, _FileMetadata=SimpleNamespace, log=logger,
    )
    return fn(SimpleNamespace(metadata_map={}), PurePosixPath("/invented/photo" + extension))


def test_dimension_diagnostics_cannot_fail_metadata_fallback():
    for extension in (".jpg", ".nef"):
        for logging_fails in (False, True):
            original = RuntimeError("invented decode failure")
            logger = SimpleNamespace(debug=Mock(side_effect=RuntimeError("logger failed") if logging_fails else None))
            result = _read_metadata(extension, _decoder(original), logger)
            assert result.width is None and result.height is None
            logger.debug.assert_called_once()
            assert logger.debug.call_args.kwargs == {"exc_info": True}


def test_dimension_success_and_interrupt_keep_their_outcomes():
    for extension in (".jpg", ".nef"):
        logger = SimpleNamespace(debug=Mock(side_effect=AssertionError("unexpected logging")))
        try:
            raise ValueError("caller exception")
        except ValueError:
            result = _read_metadata(extension, _decoder(), logger)
        assert (result.width, result.height) == (100, 80)
        original = KeyboardInterrupt()
        try:
            _read_metadata(extension, _decoder(original), logger)
        except KeyboardInterrupt as error:
            assert error is original
        else:
            raise AssertionError("interrupt was swallowed")
        logger.debug.assert_not_called()


def _raw_result(decoder, logger):
    fn = _selected(
        "scanner/working_copy_batch.py", "_raw_result_is_full_size",
        Image=SimpleNamespace(open=decoder), _oriented_dimensions=lambda *args: (100, 80),
        _scaled_dimensions=lambda *args: (100, 80), is_undersized=lambda *args, **kwargs: False,
        log=logger,
    )
    return fn(
        SimpleNamespace(wc_max_size=100), {"id": 731, "width": 100, "height": 80, "exif_data": {}},
        "invented-memory://copy", "invented-memory://companion",
    )


def test_working_copy_diagnostic_failure_keeps_companion_fallback():
    for exit_error in (False, True):
        for logging_fails in (False, True):
            original = RuntimeError("invented decoder failure")
            decoder = _decoder(exit_error=original) if exit_error else _decoder(original)
            logger = SimpleNamespace(info=Mock(side_effect=RuntimeError("logger failed") if logging_fails else None))
            assert _raw_result(decoder, logger) is False
            logger.info.assert_called_once()
            assert logger.info.call_args.kwargs == {"exc_info": True}


def test_working_copy_success_and_interrupt_keep_their_outcomes():
    logger = SimpleNamespace(info=Mock(side_effect=AssertionError("unexpected logging")))
    assert _raw_result(_decoder(), logger) is True
    original = KeyboardInterrupt()
    try:
        _raw_result(_decoder(original), logger)
    except KeyboardInterrupt as error:
        assert error is original
    else:
        raise AssertionError("interrupt was swallowed")
    logger.info.assert_not_called()
