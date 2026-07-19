import os

from import_chain import minimal_move_set


def test_single_folder(tmp_path):
    root = str(tmp_path)
    trip = os.path.join(root, "2026", "trip")
    moves, skipped = minimal_move_set(root, [(7, trip)])
    assert moves == [{"folder_id": 7, "subpath": "2026/trip"}]
    assert skipped == []


def test_nested_folder_collapses_to_ancestor(tmp_path):
    root = str(tmp_path)
    trip = os.path.join(root, "2026", "trip")
    raw = os.path.join(trip, "raw")
    moves, skipped = minimal_move_set(root, [(7, trip), (8, raw)])
    assert moves == [{"folder_id": 7, "subpath": "2026/trip"}]
    # Collapsed descendants are covered by the ancestor's move — they are
    # NOT skips: their photos still reach the NAS.
    assert skipped == []


def test_siblings_both_kept(tmp_path):
    root = str(tmp_path)
    a = os.path.join(root, "2026", "a")
    b = os.path.join(root, "2026", "b")
    moves, skipped = minimal_move_set(root, [(1, a), (2, b)])
    assert {e["subpath"] for e in moves} == {"2026/a", "2026/b"}
    assert skipped == []


def test_folder_outside_root_skipped(tmp_path):
    root = str(tmp_path / "archive")
    outside = str(tmp_path / "elsewhere")
    moves, skipped = minimal_move_set(root, [(1, outside)])
    assert moves == []
    assert skipped == [{"folder_id": 1, "reason": "outside_root"}]


def test_root_itself_skipped(tmp_path):
    root = str(tmp_path)
    moves, skipped = minimal_move_set(root, [(1, root)])
    assert moves == []
    assert skipped == [{"folder_id": 1, "reason": "root"}]


def test_root_skip_reported_alongside_movable_sibling(tmp_path):
    # Destination == archive root with a template that renders "." for some
    # photos: those catalog on the root itself while others land in
    # subfolders. The subfolder moves; the root skip must still be reported
    # so the chain can tell the user which photos stay local.
    root = str(tmp_path)
    trip = os.path.join(root, "2026", "trip")
    moves, skipped = minimal_move_set(root, [(1, root), (2, trip)])
    assert moves == [{"folder_id": 2, "subpath": "2026/trip"}]
    assert skipped == [{"folder_id": 1, "reason": "root"}]


def test_prefix_sibling_not_treated_as_nested(tmp_path):
    # "trip2" starts with "trip" as a string but is a sibling, not a child.
    # Guards against a startswith-based "simplification" of the coverage check.
    root = str(tmp_path)
    trip = os.path.join(root, "2026", "trip")
    trip2 = os.path.join(root, "2026", "trip2")
    moves, skipped = minimal_move_set(root, [(1, trip), (2, trip2)])
    assert {e["subpath"] for e in moves} == {"2026/trip", "2026/trip2"}
    assert skipped == []


def test_three_level_chain_collapses_to_top(tmp_path):
    root = str(tmp_path)
    a = os.path.join(root, "a")
    b = os.path.join(a, "b")
    c = os.path.join(b, "c")
    moves, skipped = minimal_move_set(root, [(1, a), (2, b), (3, c)])
    assert moves == [{"folder_id": 1, "subpath": "a"}]
    assert skipped == []
