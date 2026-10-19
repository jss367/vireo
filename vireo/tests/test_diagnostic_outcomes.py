"""Exercise selected failure boundaries without importing tools or model runtimes."""

import ast
import builtins
import contextlib
import http.client
import io
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

VIREO = Path(__file__).resolve().parents[1]


def _function(module, name, **namespace):
    tree = ast.parse((VIREO / module).read_text(encoding="utf-8"))
    node = next(
        node for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == name
    )
    code = ast.Module(body=[node], type_ignores=[])
    namespace = {"contextlib": contextlib, "sys": sys, **namespace}
    exec(compile(code, module, "exec"), namespace)
    return namespace[name]


def _probe(run, logger):
    return _function(
        "metadata.py", "_probe_exiftool_ver", subprocess=SimpleNamespace(run=run),
        _exiftool_command=lambda path: [path], no_window_kwargs=lambda: {}, log=logger,
    )


def _cancel(imported, logger):
    def fake_import(name, *args, **kwargs):
        assert name == "classifier"
        if isinstance(imported, BaseException):
            raise imported
        return SimpleNamespace(ClassificationCancelled=imported)

    return _function(
        "model_cache.py", "_make_cancellation_error", log=logger,
        __builtins__={**vars(builtins), "__import__": fake_import},
    )


def test_probe_logging_preserves_failure():
    for original in (RuntimeError("probe failed"), Exception()):
        for logging_error in (None, RuntimeError("logger failed"), Exception("logger failed")):
            logger = SimpleNamespace(debug=Mock(side_effect=logging_error))
            run = Mock(side_effect=original)
            assert _probe(run, logger)("invented-tool") == (
                False, None, str(original) or type(original).__name__,
            )
            logger.debug.assert_called_once_with("ExifTool -ver probe failed", exc_info=True)
            run.assert_called_once_with(
                ["invented-tool", "-ver"], capture_output=True, text=True, timeout=10,
            )


def test_probe_normal_results_do_not_log():
    logger = SimpleNamespace(debug=Mock(side_effect=RuntimeError("unexpected log")))
    for result, expected in (
        (SimpleNamespace(returncode=0, stdout=" 12.0 \n"), (True, "12.0", None)),
        (SimpleNamespace(returncode=0, stdout=" "), (True, None, None)),
        (SimpleNamespace(returncode=1, stderr=" bad \n"), (False, None, "bad")),
        (SimpleNamespace(returncode=2, stderr=" "), (False, None, "exiftool -ver exited with status 2")),
    ):
        # A caller's handled exception must not be mistaken for a probe failure.
        try:
            raise ValueError("caller")
        except ValueError:
            assert _probe(Mock(return_value=result), logger)("invented-tool") == expected
    logger.debug.assert_not_called()


def test_probe_resolver_failure_preserved():
    logger = SimpleNamespace(debug=Mock(side_effect=RuntimeError("logger failed")))
    run = Mock(side_effect=AssertionError("must not run"))
    probe = _probe(run, logger)
    probe.__globals__["_exiftool_command"] = Mock(side_effect=OSError("resolver failed"))
    assert probe("invented-tool") == (False, None, "resolver failed")
    run.assert_not_called()


def test_cancellation_logging_preserves_fallback():
    for original in (ImportError("classifier missing"), RuntimeError("native import failed")):
        for logging_error in (None, RuntimeError("logger failed"), Exception("logger failed")):
            logger = SimpleNamespace(debug=Mock(side_effect=logging_error))
            error = _cancel(original, logger)()
            assert type(error).__name__ == "ClassificationCancelled"
            assert isinstance(error, RuntimeError)
            assert str(error) == "classification cancelled"
            logger.debug.assert_called_once_with(
                "classifier unavailable; using a stand-in cancellation class", exc_info=True,
            )


def test_cancellation_import_success():
    class ClassificationCancelled(RuntimeError):
        pass

    logger = SimpleNamespace(debug=Mock(side_effect=RuntimeError("unexpected log")))
    assert type(_cancel(ClassificationCancelled, logger)()) is ClassificationCancelled
    logger.debug.assert_not_called()


def test_cancellation_constructor_failure_propagates():
    original = ValueError("constructor failed")
    constructor = Mock(side_effect=original)
    try:
        _cancel(constructor, SimpleNamespace(debug=Mock()))()
    except ValueError as error:
        assert error is original
    else:
        raise AssertionError("constructor failure was swallowed")


def test_control_flow_exceptions_propagate():
    for original in (KeyboardInterrupt(), SystemExit()):
        for make in (
            lambda logger, original=original: lambda: _probe(Mock(side_effect=original), logger)("invented-tool"),
            lambda logger, original=original: _cancel(original, logger),
        ):
            logger = SimpleNamespace(debug=Mock())
            try:
                make(logger)()
            except BaseException as error:
                assert error is original
            else:
                raise AssertionError("control flow exception was swallowed")
            logger.debug.assert_not_called()


def test_recursive_taxonomy_json_remains_a_miss():
    request = Mock()
    response = Mock()
    response.__enter__ = Mock(return_value=response)
    response.__exit__ = Mock(return_value=False)
    response.read = Mock(return_value=b"invented JSON")
    urllib = SimpleNamespace(
        parse=SimpleNamespace(quote=lambda name: name),
        request=SimpleNamespace(Request=Mock(return_value=request), urlopen=Mock(return_value=response)),
    )

    def fake_import(name, *args, **kwargs):
        assert name == "urllib.request"
        return urllib

    lookup = _function(
        "taxonomy.py", "api_lookup", urllib=urllib,
        json=SimpleNamespace(loads=Mock(side_effect=RecursionError("too deep"))),
        http=http, _ssl_ctx=object(), log=SimpleNamespace(debug=Mock()),
        __builtins__={**vars(builtins), "__import__": fake_import},
    )
    taxonomy = SimpleNamespace(_normalize=str.lower, _ambiguous_normalized=set(), _api_misses=set())
    assert lookup(taxonomy, "invented species") is None
    urllib.request.urlopen.assert_called_once()


def test_preview_cleanup_preserves_encoded_bytes_and_render_failure():
    encode = _function(
        "preview_materializer.py", "_encode_preview", io=io,
        cache_save_options=lambda photo, recipe: {},
    )
    for original in (None, ValueError("encode failed")):
        def save(output, original=original, **kwargs):
            if original is not None:
                raise original
            output.write(b"invented JPEG bytes")

        image = SimpleNamespace(save=save, close=Mock(side_effect=RuntimeError("close failed")))
        try:
            result = encode(
                image, {"id": "invented"}, size=1, vireo_dir=None, recipe=None,
                preview_quality=80, apply_recipe_to_loaded_image=None,
            )
        except ValueError as error:
            assert original is not None and error is original
        else:
            assert original is None and result == b"invented JPEG bytes"
        image.close.assert_called_once_with()
