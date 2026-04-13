import pytest

from vireo.duplicates import (
    DupCandidate,
    PhotoMetadata,
    merge_metadata,
    resolve_duplicates,
)


def test_module_exports_exist():
    c = DupCandidate(id=1, path="/a/owl.jpg", mtime=100.0)
    assert c.id == 1
    assert callable(resolve_duplicates)


def _loser_ids(losers):
    """resolve_duplicates now returns [(loser_id, reason), ...] — strip reasons."""
    return [lid for lid, _ in losers]


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
    assert _loser_ids(losers) == [2]


def test_resolve_prefers_shorter_path():
    shallow = DupCandidate(id=1, path="/pics/owl.jpg", mtime=100.0)
    deep = DupCandidate(id=2, path="/pics/archive/2024/owl.jpg", mtime=100.0)
    winner, losers = resolve_duplicates([deep, shallow])
    assert winner == 1
    assert _loser_ids(losers) == [2]


def test_resolve_prefers_older_mtime():
    older = DupCandidate(id=1, path="/a/owl.jpg", mtime=100.0)
    newer = DupCandidate(id=2, path="/b/owl.jpg", mtime=200.0)  # same-length path
    winner, losers = resolve_duplicates([newer, older])
    assert winner == 1
    assert _loser_ids(losers) == [2]


def test_resolve_falls_back_to_lower_id():
    a = DupCandidate(id=5, path="/a/owl.jpg", mtime=100.0)
    b = DupCandidate(id=3, path="/b/owl.jpg", mtime=100.0)  # same everything
    winner, losers = resolve_duplicates([a, b])
    assert winner == 3
    assert _loser_ids(losers) == [5]


def test_resolve_three_way_middle_wins():
    a = DupCandidate(id=1, path="/a/owl (2).jpg", mtime=100.0)  # dup suffix
    b = DupCandidate(id=2, path="/a/owl.jpg", mtime=100.0)       # clean, short
    c = DupCandidate(id=3, path="/archive/deep/owl.jpg", mtime=100.0)  # clean, long
    winner, losers = resolve_duplicates([a, b, c])
    assert winner == 2
    assert sorted(_loser_ids(losers)) == [1, 3]


def test_resolve_rule1_cascades_to_rule2_among_clean():
    long_clean  = DupCandidate(id=1, path="/archive/deep/owl.jpg", mtime=100.0)
    short_clean = DupCandidate(id=2, path="/a/owl.jpg",           mtime=100.0)
    dirty       = DupCandidate(id=3, path="/a/owl (2).jpg",       mtime=100.0)
    # Pass in an order that would tempt the old buggy code to pick id=1:
    winner, losers = resolve_duplicates([long_clean, short_clean, dirty])
    assert winner == 2                    # rule 2 (shorter path) decides among clean
    assert sorted(_loser_ids(losers)) == [1, 3]  # long-clean and dirty both lose


def test_resolve_all_dirty_falls_through_to_rules_2_through_4():
    a = DupCandidate(id=1, path="/a/archive/owl (2).jpg", mtime=100.0)
    b = DupCandidate(id=2, path="/a/owl (3).jpg",         mtime=100.0)
    winner, losers = resolve_duplicates([a, b])
    assert winner == 2  # rule 2 — shorter path wins among all-dirty
    assert _loser_ids(losers) == [1]


# -----------------------------------------------------------------------------
# Reason strings — resolver is the single source of truth for *why* each
# loser was picked. Locked in so callers don't replay the rules.
# -----------------------------------------------------------------------------

def test_resolve_returns_reason_rule1():
    """Rule 1: a dirty-suffix loser gets reason 'filename has dup suffix'."""
    clean = DupCandidate(id=1, path="/a/owl.jpg", mtime=100.0)
    dirty = DupCandidate(id=2, path="/a/owl (2).jpg", mtime=100.0)
    winner, losers = resolve_duplicates([clean, dirty])
    assert winner == 1
    assert losers == [(2, "filename has dup suffix")]


def test_resolve_returns_reason_rule2():
    """Rule 2: longer-path clean loser gets reason 'longer path'."""
    short = DupCandidate(id=1, path="/a/owl.jpg", mtime=100.0)
    long_ = DupCandidate(id=2, path="/archive/deep/owl.jpg", mtime=100.0)
    winner, losers = resolve_duplicates([short, long_])
    assert winner == 1
    assert losers == [(2, "longer path")]


def test_resolve_returns_reason_rule3():
    """Rule 3: same-length paths, later mtime loses with reason 'later mtime'."""
    older = DupCandidate(id=1, path="/a/owl.jpg", mtime=100.0)
    newer = DupCandidate(id=2, path="/b/owl.jpg", mtime=200.0)
    winner, losers = resolve_duplicates([older, newer])
    assert winner == 1
    assert losers == [(2, "later mtime")]


def test_resolve_returns_reason_rule4():
    """Rule 4: everything tied, higher-id loses with reason 'higher id'."""
    a = DupCandidate(id=3, path="/a/owl.jpg", mtime=100.0)
    b = DupCandidate(id=5, path="/b/owl.jpg", mtime=100.0)
    winner, losers = resolve_duplicates([a, b])
    assert winner == 3
    assert losers == [(5, "higher id")]


def test_resolve_mixed_reasons_in_three_way():
    """3-way: one rule-1 loser (dirty), one rule-2 loser (long clean).

    The dirty candidate is eliminated at rule 1 and keeps that reason; rule 2
    only sees the two clean candidates and tags the longer one.
    """
    short_clean = DupCandidate(id=1, path="/a/owl.jpg", mtime=100.0)
    long_clean  = DupCandidate(id=2, path="/archive/deep/owl.jpg", mtime=100.0)
    dirty       = DupCandidate(id=3, path="/a/owl (2).jpg", mtime=100.0)
    winner, losers = resolve_duplicates([short_clean, long_clean, dirty])
    assert winner == 1
    reasons = dict(losers)
    assert reasons == {
        2: "longer path",
        3: "filename has dup suffix",
    }


def test_resolve_preserves_longer_path_losers_when_rule2_still_tied():
    """Rule 2 (shorter-path wins) may leave multiple candidates tied on the
    shortest length. The longer-path candidates being dropped from the pool
    must still be recorded as 'longer path' losers — otherwise the DB layer
    only rejects losers from the rule-3/4 sub-pool and leaves rows with the
    same file_hash unflagged.
    """
    # Two short-path candidates tied, two longer-path candidates dropped.
    # Rule 3 breaks the short-path tie (a < b by mtime).
    a = DupCandidate(id=1, path="/x/owl.jpg", mtime=100.0)
    b = DupCandidate(id=2, path="/x/owl.jpg", mtime=200.0)
    c = DupCandidate(id=3, path="/archive/deep/owl.jpg", mtime=100.0)
    d = DupCandidate(id=4, path="/archive/deep/owl.jpg", mtime=200.0)
    winner, losers = resolve_duplicates([a, b, c, d])
    assert winner == 1
    reasons = dict(losers)
    assert reasons == {
        2: "later mtime",
        3: "longer path",
        4: "longer path",
    }
    # Sanity: every non-winner candidate must appear in losers.
    assert {c.id for c in [a, b, c, d] if c.id != winner} == set(reasons)


def test_resolve_preserves_later_mtime_losers_when_rule3_still_tied():
    """Rule 3 (oldest-mtime wins) may leave multiple candidates tied on the
    oldest mtime. Later-mtime candidates being dropped from the pool must
    still be recorded as 'later mtime' losers so the DB layer rejects the
    full duplicate set, not just the rule-4 sub-pool.
    """
    # All four share the shortest path (rule 2 no-op). Two share oldest mtime
    # (rule 3 leaves pool=[a,b]); rule 4 picks lowest id among those.
    a = DupCandidate(id=1, path="/x/owl.jpg", mtime=100.0)
    b = DupCandidate(id=2, path="/x/owl.jpg", mtime=100.0)
    c = DupCandidate(id=3, path="/x/owl.jpg", mtime=200.0)
    d = DupCandidate(id=4, path="/x/owl.jpg", mtime=200.0)
    winner, losers = resolve_duplicates([a, b, c, d])
    assert winner == 1
    reasons = dict(losers)
    assert reasons == {
        2: "higher id",
        3: "later mtime",
        4: "later mtime",
    }
    assert {c.id for c in [a, b, c, d] if c.id != winner} == set(reasons)


def test_merge_rating_takes_max():
    winner = PhotoMetadata(id=1, rating=0, keyword_ids=set(), collection_ids=set(), has_pending_edit=False)
    losers = [PhotoMetadata(id=2, rating=5, keyword_ids=set(), collection_ids=set(), has_pending_edit=False)]
    result = merge_metadata(winner, losers)
    assert result.new_rating == 5


def test_merge_keywords_union():
    winner = PhotoMetadata(id=1, rating=0, keyword_ids={10}, collection_ids=set(), has_pending_edit=False)
    losers = [PhotoMetadata(id=2, rating=0, keyword_ids={20, 30}, collection_ids=set(), has_pending_edit=False)]
    result = merge_metadata(winner, losers)
    assert result.keyword_ids_to_add == {20, 30}  # only new ones


def test_merge_collections_union():
    winner = PhotoMetadata(id=1, rating=0, keyword_ids=set(), collection_ids={100}, has_pending_edit=False)
    losers = [PhotoMetadata(id=2, rating=0, keyword_ids=set(), collection_ids={100, 200}, has_pending_edit=False)]
    result = merge_metadata(winner, losers)
    assert result.collection_ids_to_add == {200}


def test_merge_pending_copy_when_winner_has_none():
    winner = PhotoMetadata(id=1, rating=0, keyword_ids=set(), collection_ids=set(), has_pending_edit=False)
    losers = [PhotoMetadata(id=2, rating=0, keyword_ids=set(), collection_ids=set(), has_pending_edit=True)]
    result = merge_metadata(winner, losers)
    assert result.pending_from_loser_id == 2


def test_merge_pending_skip_when_both_have():
    winner = PhotoMetadata(id=1, rating=0, keyword_ids=set(), collection_ids=set(), has_pending_edit=True)
    losers = [PhotoMetadata(id=2, rating=0, keyword_ids=set(), collection_ids=set(), has_pending_edit=True)]
    result = merge_metadata(winner, losers)
    assert result.pending_from_loser_id is None
