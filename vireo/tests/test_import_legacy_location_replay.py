"""Replay admission controls using selected definitions and invented collaborators."""
from __future__ import annotations

import ast
import contextlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _definition(filename: str, name: str, namespace: dict[str, Any]) -> Any:
    tree = ast.parse((ROOT / 'services' / filename).read_text())
    node = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == name)
    module = ast.Module(body=[ast.ImportFrom(module='__future__',
        names=[ast.alias(name='annotations')], level=0), node], type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), filename, 'exec'), namespace)
    return namespace[name]


@pytest.mark.parametrize('legacy', [True, False, None])
def test_in_place_replay_preserves_legacy_gps_intent(legacy: bool | None) -> None:
    def failure(message: str, *args: Any) -> SimpleNamespace:
        return SimpleNamespace(message=message)
    validate = _definition('imports.py', '_validate_import_tag_options', {
        'ImportFailure': failure, 'normalize_keyword_display': str.strip,
        'keyword_match_key': str.casefold, 'parse_location_choices': lambda raw: raw,
        'LocationChoiceError': ValueError,
    })
    choices = [{'name': 'Invented', 'cells': ['0,0']}]
    config = {'sources': ['/invented'], 'location_from_gps': legacy,
              'location_choices': choices}
    parent = {'id': 'invented-job', 'type': 'import-in-place', 'status': 'failed',
              'workspace_id': 12, 'config': json.dumps(config)}
    history = SimpleNamespace(get=lambda _: parent, in_place_restart=lambda _: None,
                              chained_job_row=lambda _: None)
    db = SimpleNamespace(job_history=history, active_workspace_id=12,
                         workspaces=SimpleNamespace(get=lambda _: {'id': 12}))
    runner = SimpleNamespace(get=lambda _: None, list_jobs=lambda: [])
    service = SimpleNamespace(in_place_restart_lock=contextlib.nullcontext(), get_runner=lambda: runner)
    admitted = []

    def enqueue(_service: Any, _db: Any, body: dict[str, Any], **kwargs: Any) -> Any:
        _, parsed, error = validate(None, body)
        if error:
            return error
        admitted.append(body)
        assert parsed == choices
        return 'invented-child'

    restart = _definition('import_in_place.py', 'restart_import_in_place', {
        'json': json, 'ImportFailure': failure, 'enqueue_import_in_place': enqueue,
    })
    result = restart(service, db, 'invented-job')
    if legacy:
        assert not admitted
        assert 'legacy automatic GPS' in result.message
        assert 'preview' in result.message
        assert 'select location groups' in result.message
    else:
        assert result == 'invented-child'
        assert bool(admitted[0].get('location_from_gps')) is bool(legacy)
