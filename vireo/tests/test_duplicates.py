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
