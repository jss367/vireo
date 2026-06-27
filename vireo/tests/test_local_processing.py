import os
from collections import namedtuple

import local_processing

Usage = namedtuple("Usage", "total used free")


def test_staging_root_uses_final_destination_basename(tmp_path):
    root = local_processing.staging_root(
        str(tmp_path), "pipeline-123", "/Volumes/NAS/Photos"
    )

    assert root == os.path.join(str(tmp_path), "staging", "pipeline-123", "Photos")


def test_storage_plan_reports_enough_space(monkeypatch, tmp_path):
    monkeypatch.setattr(local_processing, "MIN_DERIVED_OVERHEAD_BYTES", 0)
    monkeypatch.setattr(
        local_processing.shutil,
        "disk_usage",
        lambda path: Usage(total=500, used=100, free=400),
    )

    plan = local_processing.storage_plan(
        str(tmp_path),
        source_bytes=100,
        reserved_free_bytes=10,
    )

    assert plan["enough"] is True
    assert plan["batching_required"] is False
    assert plan["batch_count"] == 1


def test_storage_plan_reports_batching_when_local_space_is_short(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(local_processing, "MIN_DERIVED_OVERHEAD_BYTES", 0)
    monkeypatch.setattr(
        local_processing.shutil,
        "disk_usage",
        lambda path: Usage(total=500, used=100, free=170),
    )

    plan = local_processing.storage_plan(
        str(tmp_path),
        source_bytes=200,
        reserved_free_bytes=20,
    )

    assert plan["enough"] is False
    assert plan["batching_required"] is True
    assert plan["batch_count"] > 1
    assert plan["usable_bytes"] == 150


def test_non_duplicate_bytes_excludes_known_hashes(tmp_path):
    """non_duplicate_bytes mirrors ingest()'s skip_duplicates gate: files whose
    hash is already in the catalog must not be counted against the staging
    budget, otherwise a mostly-duplicate card would fail the storage preflight
    even when the actual copy would fit comfortably."""
    from scanner import compute_file_hash

    fresh = tmp_path / "fresh.jpg"
    fresh.write_bytes(b"fresh-bytes-payload")
    dup = tmp_path / "dup.jpg"
    dup.write_bytes(b"already-imported-content")

    known = {compute_file_hash(str(dup))}
    assert local_processing.non_duplicate_bytes([fresh, dup], known) == fresh.stat().st_size

    # All files duplicates → zero bytes to stage.
    known_all = {compute_file_hash(str(fresh)), compute_file_hash(str(dup))}
    assert local_processing.non_duplicate_bytes([fresh, dup], known_all) == 0

    # Empty known set with no intra-run duplicates → equals total_file_bytes.
    assert (
        local_processing.non_duplicate_bytes([fresh, dup], set())
        == local_processing.total_file_bytes([fresh, dup])
    )


def test_non_duplicate_bytes_deduplicates_within_pass(tmp_path):
    """Intra-run duplicates — the same file selected twice via overlapping
    sources, or two source folders sharing a file — must collapse the way
    ingest() does. Without intra-run dedup the estimator would double-count
    them and falsely trigger batching_required on a card that fits, even
    when the catalog is empty."""
    fresh = tmp_path / "fresh.jpg"
    fresh.write_bytes(b"fresh-bytes-payload")
    dup_a = tmp_path / "dup_a.jpg"
    dup_a.write_bytes(b"shared-content")
    dup_b = tmp_path / "dup_b.jpg"
    dup_b.write_bytes(b"shared-content")  # same bytes → same hash

    # Empty catalog: intra-run duplicates still collapse, so total equals
    # the unique-hash bytes (fresh + one copy of the shared content).
    expected = fresh.stat().st_size + dup_a.stat().st_size
    assert (
        local_processing.non_duplicate_bytes([fresh, dup_a, dup_b], set())
        == expected
    )

    # The same file listed twice (overlapping sources) counts once.
    assert (
        local_processing.non_duplicate_bytes([fresh, fresh], set())
        == fresh.stat().st_size
    )
