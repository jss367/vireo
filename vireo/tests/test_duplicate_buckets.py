"""Tests for the unresolved-proposal bucketing helper.

Bucketing groups unresolved duplicate proposals that share the same set of
parent directories so the user can decide once for many groups instead of
clicking through each. See docs/plans/duplicates-bulk-decide.md.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from duplicate_buckets import bucket_unresolved_proposals


def _proposal(file_hash, paths, file_size=1000, exists=None):
    """Build a minimal unresolved-proposal dict with given paths.

    First path is the winner; remaining are losers. The function under test
    only reads ``status``, ``file_hash``, ``winner.path``, ``winner.file_size``,
    ``winner.exists``, and ``losers[].path`` / ``losers[].file_size`` /
    ``losers[].exists`` so the rest is omitted.

    ``exists``, if given, must be a list of booleans matching ``paths``.
    Pass ``None`` to omit the key entirely (preserves pre-existence-aware
    behavior, treated as present by the helper).
    """
    def _candidate(idx, path):
        cand = {
            "path": path,
            "filename": os.path.basename(path),
            "file_size": file_size,
        }
        if exists is not None:
            cand["exists"] = exists[idx]
        return cand

    return {
        "status": "unresolved",
        "file_hash": file_hash,
        "winner": _candidate(0, paths[0]),
        "losers": [_candidate(i + 1, p) for i, p in enumerate(paths[1:])],
    }


def test_empty_input_returns_empty_buckets():
    assert bucket_unresolved_proposals([]) == []


def test_single_proposal_returns_one_bucket():
    proposals = [_proposal("h1", ["/a/owl.jpg", "/b/owl.jpg"], file_size=500)]
    buckets = bucket_unresolved_proposals(proposals)
    assert len(buckets) == 1
    bucket = buckets[0]
    assert bucket["folders"] == ["/a", "/b"]
    assert bucket["group_count"] == 1
    assert bucket["file_hashes"] == ["h1"]
    # Savings = (n_candidates - 1) * file_size = 1 * 500.
    assert bucket["total_size"] == 500


def test_proposals_with_same_parent_dirs_collapse_into_one_bucket():
    proposals = [
        _proposal("h1", ["/a/owl.jpg", "/b/owl.jpg"]),
        _proposal("h2", ["/a/hawk.jpg", "/b/hawk.jpg"]),
        _proposal("h3", ["/a/finch.jpg", "/b/finch.jpg"]),
    ]
    buckets = bucket_unresolved_proposals(proposals)
    assert len(buckets) == 1
    bucket = buckets[0]
    assert bucket["folders"] == ["/a", "/b"]
    assert bucket["group_count"] == 3
    assert sorted(bucket["file_hashes"]) == ["h1", "h2", "h3"]
    assert bucket["total_size"] == 3 * 1000


def test_winner_and_loser_order_does_not_affect_bucketing():
    """Bucket key uses the *set* of parent dirs across winner+losers, so
    swapping which file Rule 0/1 picked as winner shouldn't split a bucket."""
    proposals = [
        _proposal("h1", ["/a/owl.jpg", "/b/owl.jpg"]),
        _proposal("h2", ["/b/hawk.jpg", "/a/hawk.jpg"]),
    ]
    buckets = bucket_unresolved_proposals(proposals)
    assert len(buckets) == 1
    assert buckets[0]["group_count"] == 2


def test_proposals_with_different_parent_dirs_split_into_separate_buckets():
    proposals = [
        _proposal("h1", ["/a/owl.jpg", "/b/owl.jpg"]),
        _proposal("h2", ["/c/hawk.jpg", "/d/hawk.jpg"]),
    ]
    buckets = bucket_unresolved_proposals(proposals)
    assert len(buckets) == 2
    folders_seen = {tuple(b["folders"]) for b in buckets}
    assert folders_seen == {("/a", "/b"), ("/c", "/d")}


def test_three_way_groups_form_distinct_buckets_from_two_way():
    """A {a, b, c} bucket and an {a, b} bucket are different shapes — the
    user has different choices to make for each."""
    proposals = [
        _proposal("h1", ["/a/owl.jpg", "/b/owl.jpg", "/c/owl.jpg"]),
        _proposal("h2", ["/a/hawk.jpg", "/b/hawk.jpg"]),
    ]
    buckets = bucket_unresolved_proposals(proposals)
    assert len(buckets) == 2
    by_size = {len(b["folders"]): b for b in buckets}
    assert sorted(by_size[3]["folders"]) == ["/a", "/b", "/c"]
    assert sorted(by_size[2]["folders"]) == ["/a", "/b"]
    # 3-way bucket savings = (3 - 1) * 1000.
    assert by_size[3]["total_size"] == 2000


def test_resolved_proposals_are_filtered_out():
    proposals = [
        _proposal("h1", ["/a/owl.jpg", "/b/owl.jpg"]),
        {**_proposal("h2", ["/a/hawk.jpg", "/b/hawk.jpg"]), "status": "resolved"},
    ]
    buckets = bucket_unresolved_proposals(proposals)
    assert len(buckets) == 1
    assert buckets[0]["group_count"] == 1
    assert buckets[0]["file_hashes"] == ["h1"]


def test_buckets_sorted_by_group_count_descending():
    """Most-impactful buckets first so the UI can lead with them."""
    proposals = (
        [_proposal(f"sm{i}", ["/c/x.jpg", "/d/x.jpg"]) for i in range(2)]
        + [_proposal(f"lg{i}", ["/a/x.jpg", "/b/x.jpg"]) for i in range(5)]
    )
    buckets = bucket_unresolved_proposals(proposals)
    assert [b["group_count"] for b in buckets] == [5, 2]


def test_bucket_includes_example_filenames_for_ui_preview():
    """UI shows up to 3 sample filenames per bucket so the user can sanity-
    check before bulk-resolving."""
    proposals = [
        _proposal(f"h{i}", [f"/a/photo{i}.jpg", f"/b/photo{i}.jpg"])
        for i in range(5)
    ]
    buckets = bucket_unresolved_proposals(proposals)
    assert len(buckets) == 1
    examples = buckets[0]["example_filenames"]
    assert len(examples) == 3
    assert all(name.startswith("photo") and name.endswith(".jpg") for name in examples)


def test_same_folder_duplicates_form_a_single_folder_bucket():
    """Two copies in the same folder (rare but possible) → bucket with one
    folder. UI can render this as 'X duplicates within folder Y'."""
    proposals = [
        _proposal("h1", ["/a/owl.jpg", "/a/owl-2.jpg"]),
    ]
    buckets = bucket_unresolved_proposals(proposals)
    assert len(buckets) == 1
    assert buckets[0]["folders"] == ["/a"]


def test_folder_with_only_missing_candidates_dropped_from_folders():
    """A folder whose every candidate has ``exists=False`` would render a
    bulk-decide button that the backend skip-guard refuses, so hide it."""
    proposals = [
        _proposal("h1", ["/a/owl.jpg", "/b/owl.jpg"], exists=[True, False]),
    ]
    buckets = bucket_unresolved_proposals(proposals)
    assert len(buckets) == 1
    assert buckets[0]["folders"] == ["/a"]


def test_folder_with_one_existing_candidate_across_groups_stays():
    """If a folder has at least one real file across the bucket's groups,
    keep it — the backend will skip the missing hashes per-row."""
    proposals = [
        _proposal("h1", ["/a/owl.jpg", "/b/owl.jpg"], exists=[True, True]),
        _proposal("h2", ["/a/hawk.jpg", "/b/hawk.jpg"], exists=[True, False]),
    ]
    buckets = bucket_unresolved_proposals(proposals)
    assert len(buckets) == 1
    assert buckets[0]["folders"] == ["/a", "/b"]


def test_bucket_dropped_when_every_folder_only_has_missing_candidates():
    """Pathological case: every candidate is missing. Bucket has nothing
    actionable, so don't surface it at all — the per-group review path
    handles those rows."""
    proposals = [
        _proposal("h1", ["/a/owl.jpg", "/b/owl.jpg"], exists=[False, False]),
    ]
    assert bucket_unresolved_proposals(proposals) == []


def test_proposals_without_exists_key_preserve_all_folders():
    """Backwards compat: proposals built before existence-aware bucketing
    didn't carry an ``exists`` flag. Treat absent as present so pre-fix data
    (and tests that don't bother with the flag) keep working."""
    proposals = [
        _proposal("h1", ["/a/owl.jpg", "/b/owl.jpg"]),
        _proposal("h2", ["/a/hawk.jpg", "/b/hawk.jpg"]),
    ]
    buckets = bucket_unresolved_proposals(proposals)
    assert len(buckets) == 1
    assert buckets[0]["folders"] == ["/a", "/b"]
