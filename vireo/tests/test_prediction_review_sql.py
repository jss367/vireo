"""Workspace review fragments preserve pending defaults and review isolation."""

import contextlib
import sqlite3

import pytest
from repositories.prediction_review import prediction_review_join, prediction_review_status


@pytest.fixture
def conn():
    with contextlib.closing(sqlite3.connect(":memory:")) as conn:
        conn.row_factory = sqlite3.Row
        conn.executescript("""
            CREATE TABLE predictions(id INTEGER PRIMARY KEY, species TEXT);
            CREATE TABLE prediction_review(
                prediction_id INTEGER, workspace_id INTEGER, status TEXT NOT NULL,
                individual TEXT, group_id TEXT, vote_count INTEGER, total_votes INTEGER,
                PRIMARY KEY(prediction_id, workspace_id)
            );
            INSERT INTO predictions VALUES (1, 'Robin'), (2, 'Finch'), (3, 'Owl');
            INSERT INTO prediction_review VALUES
                (1, 10, 'rejected', 'ten', 'g10', 1, 3),
                (1, 20, 'accepted', 'twenty', 'g20', 2, 4),
                (2, 20, 'rejected', 'other', 'g-other', 3, 5);
        """)
        yield conn


def _rows(conn, workspace, *, required=False):
    return [tuple(r) for r in conn.execute(
        f"""SELECT pr.id, {prediction_review_status()} AS review_status,
                   pr_rev.individual, pr_rev.group_id, pr_rev.vote_count, pr_rev.total_votes
            FROM predictions pr {prediction_review_join('pr', required=required)}
            ORDER BY pr.id""", (workspace,),
    )]


def test_missing_reviews_default_to_pending_without_borrowing_other_workspace_metadata(conn):
    assert _rows(conn, 10) == [
        (1, 'rejected', 'ten', 'g10', 1, 3),
        (2, 'pending', None, None, None, None),
        (3, 'pending', None, None, None, None),
    ]
    assert _rows(conn, 20) == [
        (1, 'accepted', 'twenty', 'g20', 2, 4),
        (2, 'rejected', 'other', 'g-other', 3, 5),
        (3, 'pending', None, None, None, None),
    ]
    assert _rows(conn, 30) == [(i, 'pending', None, None, None, None) for i in (1, 2, 3)]
    # Resolving a different workspace does not retain state in the helper.
    assert _rows(conn, 10)[0][1] == 'rejected'


@pytest.mark.parametrize(('workspace', 'visible'), [(10, [2, 3]), (20, [1, 3]), (30, [1, 2, 3])])
def test_rejected_filter_is_workspace_scoped(conn, workspace, visible):
    rows = conn.execute(
        f"""SELECT pr.id FROM predictions pr {prediction_review_join('pr')}
            WHERE {prediction_review_status()} != 'rejected' AND pr.id >= ? ORDER BY pr.id""",
        (workspace, 1),
    )
    assert [r[0] for r in rows] == visible


def test_required_review_keeps_only_explicit_rows(conn):
    assert _rows(conn, 10, required=True) == [(1, 'rejected', 'ten', 'g10', 1, 3)]
    assert len(_rows(conn, 20, required=True)) == 2
    assert _rows(conn, 30, required=True) == []


def test_correlated_workspace_and_custom_aliases(conn):
    rows = conn.execute(
        f"""WITH workspaces(id) AS (VALUES (10), (20), (30))
            SELECT ws.id, (SELECT COUNT(*) FROM predictions guess
                {prediction_review_join('guess', 'review', workspace='ws.id')}
                WHERE {prediction_review_status('review')} != 'rejected')
            FROM workspaces ws ORDER BY ws.id""",
    )
    assert [tuple(r) for r in rows] == [(10, 2), (20, 2), (30, 3)]


@pytest.mark.parametrize('kwargs', [
    {'prediction_alias': 'pr; DROP TABLE predictions'},
    {'prediction_alias': 'pr', 'review_alias': 'rv.status'},
    {'prediction_alias': 'pr', 'workspace': '10 OR 1=1'},
    {'prediction_alias': 'pr', 'workspace': 'ws.id OR 1=1'},
])
def test_sql_identifiers_and_workspace_expressions_are_validated(kwargs):
    with pytest.raises(ValueError):
        prediction_review_join(**kwargs)
