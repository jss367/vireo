"""Source-only cleanup regression controls; no move imports or filesystem calls."""

import ast
import stat
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock


def _cleanup():
    path = Path(__file__).resolve().parents[1] / "move" / "photos.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    node = next(
        node for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "remove_originals"
    )
    namespace = {"stat": stat}
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), "exec"), namespace)
    return namespace["remove_originals"]


def _run_cleanup(companion_stat, *, preserve=False, original_error=None, remove_error=None):
    item = SimpleNamespace(
        photo={"filename": "invented.raw"}, src_dir="/invented-source",
        src_file="/invented-source/invented.raw", companions=["invented.xmp"],
        xmp_companion="invented.xmp", preserve_source_render=preserve,
    )
    calls = []

    def remove(path):
        calls.append(path)
        error = original_error if path == item.src_file else remove_error
        if error is not None:
            raise error

    probe = Mock(side_effect=companion_stat) if isinstance(companion_stat, BaseException) else Mock(return_value=companion_stat)
    def isfile(path):
        # Model the old isfile behavior for old-source regression comparisons.
        try:
            return stat.S_ISREG(probe(path).st_mode)
        except OSError:
            return False

    logger = SimpleNamespace(warning=Mock())
    cleanup = _cleanup()
    cleanup.__globals__.update(
        os=SimpleNamespace(
            remove=remove, stat=probe,
            path=SimpleNamespace(join=lambda parent, name: parent + "/" + name, isfile=isfile),
        ),
        log=logger,
    )
    transfer = SimpleNamespace(destination="/invented-target", errors=[])
    cleanup(transfer, item)
    return transfer.errors, calls, probe, logger


def test_inaccessible_known_companion_is_reported_without_removal():
    for error in (PermissionError("invented permission denied"), OSError("invented mount unavailable")):
        errors, calls, probe, logger = _run_cleanup(error)
        assert calls == ["/invented-source/invented.raw"]
        probe.assert_called_once_with("/invented-source/invented.xmp")
        assert len(errors) == 1
        assert "original deleted" in errors[0]
        assert "invented.xmp (could not inspect; left untouched:" in errors[0]
        assert str(error) in errors[0] and "/invented-source" in errors[0]
        assert "/invented-target" in errors[0]
        logger.warning.assert_called_once()


def test_absent_known_companion_is_not_removed_or_reported_as_inaccessible():
    for error in (FileNotFoundError("invented absent"), NotADirectoryError("invented absent parent")):
        errors, calls, probe, logger = _run_cleanup(error)
        assert not errors
        assert calls == ["/invented-source/invented.raw"]
        probe.assert_called_once()
        logger.warning.assert_not_called()


def test_nonregular_known_companion_is_not_removed():
    for mode in (stat.S_IFDIR, stat.S_IFIFO, stat.S_IFSOCK):
        errors, calls, probe, logger = _run_cleanup(SimpleNamespace(st_mode=mode))
        assert not errors
        assert calls == ["/invented-source/invented.raw"]
        probe.assert_called_once()
        logger.warning.assert_not_called()


def test_regular_known_companion_keeps_existing_removal_behavior():
    errors, calls, probe, logger = _run_cleanup(SimpleNamespace(st_mode=stat.S_IFREG))
    assert not errors
    assert calls == ["/invented-source/invented.raw", "/invented-source/invented.xmp"]
    probe.assert_called_once()
    logger.warning.assert_not_called()


def test_regular_companion_removal_failure_keeps_existing_error_contract():
    errors, calls, probe, logger = _run_cleanup(
        SimpleNamespace(st_mode=stat.S_IFREG), remove_error=PermissionError("invented locked companion"),
    )
    assert calls == ["/invented-source/invented.raw", "/invented-source/invented.xmp"]
    assert len(errors) == 1 and "original deleted" in errors[0]
    assert "invented.xmp (invented locked companion)" in errors[0]
    probe.assert_called_once()
    logger.warning.assert_called_once()


def test_shared_xmp_stays_uninspected_and_untouched():
    errors, calls, probe, logger = _run_cleanup(AssertionError("must not inspect"), preserve=True)
    assert not errors
    assert calls == ["/invented-source/invented.raw"]
    probe.assert_not_called()
    logger.warning.assert_not_called()


def test_original_removal_failure_retains_known_companion_without_inspection():
    errors, calls, probe, logger = _run_cleanup(
        AssertionError("must not inspect"), original_error=PermissionError("invented locked original"),
    )
    assert calls == ["/invented-source/invented.raw"]
    probe.assert_not_called()
    assert len(errors) == 1 and "the original could not be deleted" in errors[0]
    assert "its companions were left with it: invented.xmp" in errors[0]
    logger.warning.assert_called_once()
