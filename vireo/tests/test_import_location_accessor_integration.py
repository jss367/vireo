"""Import/location API integration with selected code and invented SQLite only."""
from __future__ import annotations

import ast
import contextlib
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

VIREO = Path(__file__).resolve().parents[1]


def _selected(path: Path, names: set[str], namespace: dict[str, Any]) -> dict[str, Any]:
    nodes = [node for node in ast.parse(path.read_text()).body
             if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in names]
    module = ast.Module(body=[ast.ImportFrom(module='__future__',
        names=[ast.alias(name='annotations')], level=0), *nodes], type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), '<inert accessor integration>', 'exec'), namespace)
    return namespace


@pytest.mark.parametrize('operation', ['path', 'keyword', 'coordinates'])
def test_import_locations_use_repository_without_removed_facade_methods(operation: str) -> None:
    def forbidden(*args: Any, **kwargs: Any) -> None:
        raise AssertionError('No workspace lookup, provider or filesystem workflow allowed')

    ns = _selected(VIREO / 'repositories' / 'locations.py', {'LocationRepository'},
                   {'contextmanager': contextlib.contextmanager})
    with sqlite3.connect(':memory:') as conn:
        conn.row_factory = sqlite3.Row
        conn.executescript('''
            CREATE TABLE keywords(id INTEGER PRIMARY KEY,name TEXT,place_id TEXT,
                                  latitude REAL,longitude REAL,parent_id INTEGER,type TEXT);
            INSERT INTO keywords VALUES(1,'Parent',NULL,NULL,NULL,NULL,'location');
            INSERT INTO keywords VALUES(2,'Invented',NULL,NULL,99,1,'location');
        ''')
        repository = ns['LocationRepository'](conn, forbidden, folder_subtree_ids=forbidden,
                                              build_query_from_rules=forbidden, merge_keyword_into=forbidden)
        # Deliberately expose no old get_keyword_place_row or fill_missing_location_coordinates.
        db = SimpleNamespace(locations=repository, get_or_create_text_location=lambda _: 2)
        ns = _selected(VIREO / 'services' / 'import_locations.py',
                       {'_location_path', '_choice_leaf', 'LocationChoiceError'}, {})
        if operation == 'path':
            assert ns['_location_path'](db, 2) == ('Invented', ['Parent'])
        elif operation == 'keyword':
            assert ns['_choice_leaf'](db, {'kind': 'keyword', 'keyword_id': 2}, None) == (2, 'Invented')
        else:
            result = ns['_choice_leaf'](db, {'kind': 'new', 'name': 'Invented',
                                            'latitude': 12, 'longitude': 13}, None)
            assert result == (2, 'Invented')
            assert tuple(conn.execute('SELECT latitude,longitude FROM keywords WHERE id=2').fetchone()) == (12, 99)
