"""Selected AST and fake collaborators; no app, decoder, provider or Trash use."""

import ast
import contextlib
import sys
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from unittest.mock import Mock

VIREO = Path(__file__).resolve().parents[1]


def _selected(module, name, **namespace):
    tree = ast.parse((VIREO / module).read_text(encoding="utf-8"))
    node = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == name
    ][-1]
    namespace = {"contextlib": contextlib, "sys": sys, **namespace}
    exec(compile(ast.Module(body=[node], type_ignores=[]), module, "exec"), namespace)
    return namespace[name]


def _extract_case(original=None, *, logging_fails=False, close_fails=True, publish_fails=False):
    events = []
    close_error = RuntimeError("invented close failure")

    def save(*args, **kwargs):
        events.append("save")
        if original is not None:
            raise original

    def close():
        events.append("close-image")
        if close_fails:
            raise close_error

    def debug(*args, **kwargs):
        events.append("debug")
        assert sys.exception() is close_error and kwargs == {"exc_info": True}
        if logging_fails:
            raise RuntimeError("invented logger failure")

    def on_publish(path):
        events.append("on-publish")
        if publish_fails:
            raise RuntimeError("invented publication callback failure")

    @contextlib.contextmanager
    def guard():
        events.append("guard-enter")
        try:
            yield
        finally:
            events.append("guard-exit")

    fake_os = SimpleNamespace(
        path=SimpleNamespace(dirname=lambda path: "/invented-target", basename=lambda path: "copy"),
        makedirs=lambda *args, **kwargs: events.append("mkdir"),
        close=lambda fd: events.append("close-fd"),
        remove=lambda path: events.append("remove-temp"),
    )
    fn = _selected(
        "image_loader.py", "extract_working_copy", RAW_DECODE_PRESERVE_HIGHLIGHTS="invented",
        load_image=lambda *args, **kwargs: SimpleNamespace(save=save, close=close), os=fake_os,
        tempfile=SimpleNamespace(mkstemp=lambda **kwargs: (731, "/invented-target/temp")),
        replace_file=lambda *args: events.append("publish"),
        log=SimpleNamespace(debug=debug, warning=lambda *args, **kwargs: events.append("warning")),
    )
    try:
        result = fn(
            "/invented-source/photo", "/invented-target/copy", publication_guard=guard,
            on_publish=on_publish,
        )
    except BaseException as error:
        assert original is not None and not isinstance(original, Exception) and error is original
    else:
        assert original is None or isinstance(original, Exception)
        assert result is (original is None)
    assert events.count("close-image") == 1
    assert events.count("debug") == int(close_fails)
    if original is None:
        assert events.index("guard-enter") < events.index("publish") < events.index("on-publish") < events.index("guard-exit")
        assert "remove-temp" not in events
    else:
        assert "publish" not in events and "remove-temp" in events


def test_close_diagnostics_preserve_extraction_results_and_control_flow():
    for original in (None, ValueError("invented encode failure"), KeyboardInterrupt(), GeneratorExit(), SystemExit()):
        for logging_fails in (False, True):
            _extract_case(original, logging_fails=logging_fails)


def test_successful_close_does_not_log_an_inherited_exception():
    try:
        raise ValueError("caller exception")
    except ValueError:
        _extract_case(logging_fails=True, close_fails=False)


def test_publication_callback_failure_keeps_success_and_closes_image():
    _extract_case(publish_fails=True, logging_fails=True)


def _trash_case(platform, original, *, confirmed_gone=False):
    events = []

    def fake_trash(path):
        events.append(("trash", path))
        if path == "invented-first":
            raise original

    def fake_import(name, *args, **kwargs):
        assert name == "send2trash"
        return SimpleNamespace(send2trash=fake_trash)

    # Supplying only this import prevents accidental real module loading.
    import builtins

    logger = SimpleNamespace(warning=Mock(side_effect=RuntimeError("invented logger failure")))
    fn = _selected(
        "app.py", "send_fallback_to_trash", sys=SimpleNamespace(platform=platform),
        _path_confirmed_gone=lambda *args: confirmed_gone, log=logger,
        __builtins__={**vars(builtins), "__import__": fake_import},
    )
    owner = SimpleNamespace(
        fallback=["invented-first", "invented-second"], parent_devs={}, successful=set(), moved=0,
        send_errors={}, finder_candidates=[], report_processed=lambda path: events.append(("progress", path)),
    )
    try:
        fn(owner)
    except BaseException as error:
        assert isinstance(original, (KeyboardInterrupt, GeneratorExit)) and error is original
        assert events == [("trash", "invented-first")]
        assert owner.send_errors == {} and owner.moved == 0
    else:
        assert not isinstance(original, (KeyboardInterrupt, GeneratorExit))
        if confirmed_gone:
            assert owner.successful == {"invented-first", "invented-second"}
            assert owner.moved == 2 and owner.send_errors == {}
        else:
            assert owner.successful == {"invented-second"} and owner.moved == 1
            assert owner.send_errors == {"invented-first": str(original)}
            assert owner.finder_candidates == (["invented-first"] if platform == "darwin" else [])
        assert ("trash", "invented-second") in events
        assert ("progress", "invented-second") in events
    logger.warning.assert_not_called()


def test_per_file_trash_errors_and_subsequent_callbacks_survive_throwing_logger():
    for platform in ("win32", "darwin"):
        for original in (OSError("invented unavailable source"), RuntimeError("invented API error"), SystemExit()):
            _trash_case(platform, original)


def test_trash_confirmed_absence_and_control_flow_guards_are_preserved():
    for platform in ("win32", "darwin"):
        _trash_case(platform, OSError("invented post-operation error"), confirmed_gone=True)
        for original in (KeyboardInterrupt(), GeneratorExit()):
            _trash_case(platform, original)


def test_existing_image_failure_diagnostics_include_original_traceback():
    for name in ("load_image", "_load_standard"):
        original = RuntimeError("invented decode error")
        logger = SimpleNamespace(warning=Mock())
        fn = _selected(
            "image_loader.py", name, RAW_DECODE_JPEG_FIRST="invented",
            RAW_DECODE_LINEAR="linear", RAW_EXTENSIONS=set(), SUPPORTED_EXTENSIONS={".jpg"},
            _RAW_DECODE_MODES={"invented"}, Path=PurePosixPath, Image=SimpleNamespace(open=Mock(side_effect=original)),
            log=logger,
        )
        assert fn("/invented/photo.jpg", 100) is None
        logger.warning.assert_called_once()
        assert logger.warning.call_args.kwargs == {"exc_info": True}


def test_existing_raw_fallback_diagnostic_keeps_embedded_preview():
    import builtins

    original = RuntimeError("invented demosaic failure")
    embedded = SimpleNamespace(size=(100, 80))
    manager = Mock()
    manager.__enter__ = Mock(return_value=object())
    manager.__exit__ = Mock(return_value=False)

    def fake_import(name, *args, **kwargs):
        assert name == "rawpy"
        return SimpleNamespace(imread=Mock(return_value=manager))

    logger = SimpleNamespace(info=Mock())
    fn = _selected(
        "image_loader.py", "_load_raw", RAW_DECODE_JPEG_FIRST="jpeg", RAW_DECODE_LINEAR="linear",
        RAW_DECODE_PRESERVE_HIGHLIGHTS="highlights", _extract_embedded_jpeg=lambda raw: embedded,
        _embedded_preview_suffices=lambda *args: False, _postprocess_raw=Mock(side_effect=original),
        _full_camera_output_qualifier=lambda *args: "", log=logger,
        __builtins__={**vars(builtins), "__import__": fake_import},
    )
    assert fn("/invented/raw", 100) is embedded
    logger.info.assert_called_once()
    assert logger.info.call_args.kwargs == {"exc_info": True}


def test_existing_revision_fallback_diagnostic_keeps_default_branch():
    import builtins

    downloads = []
    copies = []
    original = RuntimeError("invented revision lookup failure")

    def download(**kwargs):
        downloads.append(kwargs)
        return "invented-memory://" + kwargs["filename"]

    def fake_import(name, *args, **kwargs):
        if name == "shutil":
            return SimpleNamespace(copy2=lambda *args: copies.append(args))
        if name == "huggingface_hub":
            return SimpleNamespace(HfApi=lambda: SimpleNamespace(model_info=Mock(side_effect=original)), hf_hub_download=download)
        assert name == "models"
        return SimpleNamespace(ONNX_REPO="invented/repo")

    logger = SimpleNamespace(info=Mock())
    fn = _selected(
        "dino_embed.py", "_fetch_dinov2_pair", log=logger,
        __builtins__={**vars(builtins), "__import__": fake_import},
    )
    fn("invented-variant", "invented-memory://graph", "invented-memory://data", None)
    assert len(downloads) == len(copies) == 2
    assert all(call["revision"] is None for call in downloads)
    logger.info.assert_called_once()
    assert logger.info.call_args.kwargs == {"exc_info": True}
