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
