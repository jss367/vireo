"""Selected label functions with fake HTTP, JSON and clocks; no provider access."""

import ast
import builtins
import http.client
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

VIREO = Path(__file__).resolve().parents[1]


def _selected(name, namespace):
    tree = ast.parse((VIREO / "labels.py").read_text(encoding="utf-8"))
    node = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == name)
    exec(compile(ast.Module(body=[node], type_ignores=[]), "labels.py", "exec"), namespace)
    return namespace[name]


def _case(name, outcomes):
    sleeps = []
    progress = []
    response = Mock()
    response.__enter__ = Mock(return_value=response)
    response.__exit__ = Mock(return_value=False)
    response.read = Mock(return_value=b"invented JSON")
    opener = Mock(return_value=response)
    parser = Mock(side_effect=outcomes)
    logger = SimpleNamespace(info=Mock(), warning=Mock())

    def fake_import(module, *args, **kwargs):
        assert module == "time"
        return SimpleNamespace(sleep=sleeps.append)

    fn = _selected(name, {
        "INAT_API": "invented://inat", "_ssl_ctx": object(), "http": http,
        "json": SimpleNamespace(loads=parser), "log": logger,
        "urllib": SimpleNamespace(
            parse=SimpleNamespace(urlencode=lambda value: "invented-query"),
            request=SimpleNamespace(Request=Mock(), urlopen=opener),
        ),
        "__builtins__": {**vars(builtins), "__import__": fake_import},
    })
    result = fn("invented query") if name == "search_places" else fn(
        "invented://page", 7, "invented group", "invented prefix",
        lambda *args: progress.append(args),
    )
    assert response.__exit__.call_count == opener.call_count == parser.call_count
    assert all(call.kwargs == {"exc_info": True} for call in logger.warning.call_args_list)
    return result, parser.call_count, logger.warning.call_count, sleeps, progress


def test_recursive_place_json_returns_empty_results():
    assert _case("search_places", [RecursionError("invented nesting")]) == ([], 1, 1, [], [])


def test_recursive_species_json_retries_and_reports_exhaustion():
    result, calls, warnings, sleeps, progress = _case(
        "_fetch_species_counts_page", [RecursionError("invented nesting")] * 3,
    )
    assert (result, calls, warnings, sleeps) == (None, 3, 3, [2, 2])
    assert progress == [("invented prefix: failed after 3 attempts on page 7", 0, 0)]


def test_recursive_species_json_can_recover_on_retry():
    payload = {"results": [{"count": 9}]}
    assert _case("_fetch_species_counts_page", [RecursionError("invented nesting"), payload]) == (
        payload, 2, 1, [2], [],
    )


def test_existing_json_http_and_io_fallbacks_remain_bounded():
    for error in (OSError("invented IO"), ValueError("invented JSON"), http.client.HTTPException("invented HTTP")):
        assert _case("search_places", [error]) == ([], 1, 1, [], [])
        result, calls, warnings, sleeps, progress = _case("_fetch_species_counts_page", [error] * 3)
        assert (result, calls, warnings, sleeps) == (None, 3, 3, [2, 2])
        assert len(progress) == 1


def test_valid_json_results_do_not_retry():
    payload = {"results": [{"id": 12, "name": "invented place"}]}
    assert _case("search_places", [payload]) == (
        [{"id": 12, "name": "invented place", "display_name": "invented place"}], 1, 0, [], [],
    )
    assert _case("_fetch_species_counts_page", [payload]) == (payload, 1, 0, [], [])


def test_unrelated_errors_and_control_flow_still_propagate():
    for name in ("search_places", "_fetch_species_counts_page"):
        for original in (RuntimeError("invented programming error"), KeyboardInterrupt(), SystemExit()):
            try:
                _case(name, [original])
            except BaseException as error:
                assert error is original
            else:
                raise AssertionError("Unrelated error was swallowed")
