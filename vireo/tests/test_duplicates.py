import pytest

from vireo.duplicates import DupCandidate, resolve_duplicates


def test_module_exports_exist():
    c = DupCandidate(id=1, path="/a/owl.jpg", mtime=100.0)
    assert c.id == 1
    assert callable(resolve_duplicates)


@pytest.mark.parametrize("clean_name, dup_name", [
    ("owl.jpg", "owl (2).jpg"),
    ("owl.jpg", "owl (10).jpg"),
    ("owl.jpg", "owl copy.jpg"),
    ("owl.jpg", "owl copy 3.jpg"),
    ("owl.jpg", "owl-1.jpg"),
    ("owl.jpg", "owl_1.jpg"),
    ("owl.jpg", "owl COPY.jpg"),  # case-insensitive
])
def test_resolve_prefers_clean_filename(clean_name, dup_name):
    clean = DupCandidate(id=1, path=f"/a/{clean_name}", mtime=100.0)
    dup = DupCandidate(id=2, path=f"/a/{dup_name}", mtime=100.0)
    winner, losers = resolve_duplicates([dup, clean])  # order shouldn't matter
    assert winner == 1
    assert losers == [2]


def test_resolve_prefers_shorter_path():
    shallow = DupCandidate(id=1, path="/pics/owl.jpg", mtime=100.0)
    deep = DupCandidate(id=2, path="/pics/archive/2024/owl.jpg", mtime=100.0)
    winner, losers = resolve_duplicates([deep, shallow])
    assert winner == 1
    assert losers == [2]


def test_resolve_prefers_older_mtime():
    older = DupCandidate(id=1, path="/a/owl.jpg", mtime=100.0)
    newer = DupCandidate(id=2, path="/b/owl.jpg", mtime=200.0)  # same-length path
    winner, losers = resolve_duplicates([newer, older])
    assert winner == 1
    assert losers == [2]


def test_resolve_falls_back_to_lower_id():
    a = DupCandidate(id=5, path="/a/owl.jpg", mtime=100.0)
    b = DupCandidate(id=3, path="/b/owl.jpg", mtime=100.0)  # same everything
    winner, losers = resolve_duplicates([a, b])
    assert winner == 3
    assert losers == [5]


def test_resolve_three_way_middle_wins():
    a = DupCandidate(id=1, path="/a/owl (2).jpg", mtime=100.0)  # dup suffix
    b = DupCandidate(id=2, path="/a/owl.jpg", mtime=100.0)       # clean, short
    c = DupCandidate(id=3, path="/archive/deep/owl.jpg", mtime=100.0)  # clean, long
    winner, losers = resolve_duplicates([a, b, c])
    assert winner == 2
    assert sorted(losers) == [1, 3]
