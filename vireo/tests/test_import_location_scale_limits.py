"""Scale contracts using selected definitions and invented cells; no product I/O."""
from __future__ import annotations

import ast
import re
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

SOURCE = Path(__file__).resolve().parents[1] / 'services' / 'import_locations.py'


def _selected() -> dict[str, Any]:
    names = {'MAX_CHOICES', 'MAX_CHOICE_CELLS', 'MAX_SUGGESTION_PATHS',
             'MAX_LOCATION_NAME', '_CELL_RE', 'LocationChoiceError',
             '_parse_choice_target', 'parse_location_choices', 'apply_location_choices'}
    nodes = []
    for node in ast.parse(SOURCE.read_text()).body:
        if (isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in names) or (
            isinstance(node, ast.Assign) and any(
                isinstance(target, ast.Name) and target.id in names for target in node.targets
            )
        ):
            nodes.append(node)
        elif isinstance(node, ast.ClassDef) and node.name == 'ImportLocationSuggester':
            nodes.extend(n for n in node.body if isinstance(n, ast.FunctionDef) and n.name == 'result')
    namespace: dict[str, Any] = {'re': re}
    module = ast.Module(body=[ast.ImportFrom(module='__future__',
        names=[ast.alias(name='annotations')], level=0), *nodes], type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), '<selected scale contracts>', 'exec'), namespace)
    return namespace


def test_provider_write_groups_have_a_separate_safe_bound() -> None:
    ns = _selected()
    assert ns['MAX_CHOICES'] == 1000
    assert ns['MAX_CHOICE_CELLS'] == ns['MAX_SUGGESTION_PATHS'] == 100_000
    choices = [{'kind': 'google', 'place_id': str(i), 'name': 'Invented', 'cells': [f'{i},0']}
               for i in range(1000)]
    assert len(ns['parse_location_choices'](choices)) == 1000
    with pytest.raises(ns['LocationChoiceError']):
        ns['parse_location_choices'](choices + [
            {'kind': 'google', 'place_id': 'excess', 'name': 'Invented', 'cells': ['-1,0']}])


@pytest.mark.parametrize('count', [999, 1000, 1001])
def test_preview_never_offers_more_than_the_group_work_bound(count: int) -> None:
    ns = _selected()
    finished = []

    def finish(_db: Any, group: dict[str, Any]) -> dict[str, Any]:
        finished.append(group)
        return group

    ns['_finish_group'] = finish
    subject = SimpleNamespace(_db=object(), _groups={i: {'suggestion': None, 'file_count': 1}
                              for i in range(count)}, _cells={})
    result = ns['result'](subject)
    if count > 1000:
        assert result['groups'] == [] and result['file_cells'] == {}
        assert not finished  # No per-group finalization after refusal.
        assert '1,000' in result['work_limit_error']
        assert 'Preview fewer files' in result['work_limit_error']
        assert 'turn off GPS' in result['work_limit_error']
    else:
        assert len(result['groups']) == len(finished) == count
        assert 'work_limit_error' not in result


def test_saved_oversized_work_is_refused_before_planning_or_provider_calls() -> None:
    ns = _selected()

    def forbidden(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("No catalog, provider, transaction or planning work allowed")

    ns['_plan_location_choices'] = forbidden
    ns['_google_details'] = forbidden
    ns['_apply_choice'] = forbidden
    with pytest.raises(ns['LocationChoiceError']):
        ns['apply_location_choices'](object(), [], [{}] * 1001, api_key='', language=None,
                                     summary={}, cancel_check=forbidden)
