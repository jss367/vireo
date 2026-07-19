import os

from import_chain import minimal_move_set


def test_single_folder(tmp_path):
    root = str(tmp_path)
    trip = os.path.join(root, "2026", "trip")
    assert minimal_move_set(root, [(7, trip)]) == [
        {"folder_id": 7, "subpath": "2026/trip"},
    ]


def test_nested_folder_collapses_to_ancestor(tmp_path):
    root = str(tmp_path)
    trip = os.path.join(root, "2026", "trip")
    raw = os.path.join(trip, "raw")
    out = minimal_move_set(root, [(7, trip), (8, raw)])
    assert out == [{"folder_id": 7, "subpath": "2026/trip"}]


def test_siblings_both_kept(tmp_path):
    root = str(tmp_path)
    a = os.path.join(root, "2026", "a")
    b = os.path.join(root, "2026", "b")
    out = minimal_move_set(root, [(1, a), (2, b)])
    assert {e["subpath"] for e in out} == {"2026/a", "2026/b"}


def test_folder_outside_root_skipped(tmp_path):
    root = str(tmp_path / "archive")
    outside = str(tmp_path / "elsewhere")
    assert minimal_move_set(root, [(1, outside)]) == []


def test_root_itself_skipped(tmp_path):
    root = str(tmp_path)
    assert minimal_move_set(root, [(1, root)]) == []


def test_prefix_sibling_not_treated_as_nested(tmp_path):
    # "trip2" starts with "trip" as a string but is a sibling, not a child.
    # Guards against a startswith-based "simplification" of the coverage check.
    root = str(tmp_path)
    trip = os.path.join(root, "2026", "trip")
    trip2 = os.path.join(root, "2026", "trip2")
    out = minimal_move_set(root, [(1, trip), (2, trip2)])
    assert {e["subpath"] for e in out} == {"2026/trip", "2026/trip2"}


def test_three_level_chain_collapses_to_top(tmp_path):
    root = str(tmp_path)
    a = os.path.join(root, "a")
    b = os.path.join(a, "b")
    c = os.path.join(b, "c")
    out = minimal_move_set(root, [(1, a), (2, b), (3, c)])
    assert out == [{"folder_id": 1, "subpath": "a"}]
