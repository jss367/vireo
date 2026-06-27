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


def test_storage_plan_checks_destination_volume_on_different_device(
    monkeypatch, tmp_path
):
    """When staging and archive_parent are on different devices, the
    destination volume gets an independent free-space check. A nearly-full
    archive drive that the staging volume can't see must trip
    batching_required so the user finds out before the pipeline spends
    hours staging and processing only to ENOSPC at the final move."""
    staging = tmp_path / "staging"
    staging.mkdir()
    archive_parent = tmp_path / "archive_parent"
    archive_parent.mkdir()

    monkeypatch.setattr(local_processing, "MIN_DERIVED_OVERHEAD_BYTES", 0)

    usage_by_path = {
        str(staging): Usage(total=10_000, used=0, free=10_000),
        str(archive_parent): Usage(total=10_000, used=9_900, free=100),
    }
    monkeypatch.setattr(
        local_processing.shutil,
        "disk_usage",
        lambda path: usage_by_path[path],
    )

    # Force same_device detection to return False so the destination check
    # runs independently. (On a real filesystem with two tmpfs mounts the
    # st_dev would already differ; on a single-FS test runner this avoids
    # depending on that detail.)
    real_stat = local_processing.os.stat
    class FakeStat:
        def __init__(self, dev): self.st_dev = dev
    stat_by_path = {
        str(staging): FakeStat(1), str(archive_parent): FakeStat(2),
    }
    monkeypatch.setattr(
        local_processing.os, "stat",
        lambda path: stat_by_path.get(path, real_stat(path)),
    )

    plan = local_processing.storage_plan(
        str(staging),
        source_bytes=500,
        archive_parent=str(archive_parent),
        reserved_free_bytes=0,
    )

    assert plan["staging_enough"] is True
    assert plan["archive_enough"] is False
    assert plan["archive_required_bytes"] == 500
    assert plan["archive_free_bytes"] == 100
    assert plan["same_device"] is False
    assert plan["enough"] is False
    assert plan["batching_required"] is True


def test_storage_plan_doubles_source_bytes_on_same_device(
    monkeypatch, tmp_path
):
    """When staging and archive_parent share a device, the archive's
    copy-verify-delete needs a second copy of source bytes on the same
    volume before staging is removed. The plan must add source_bytes to
    the staging requirement, not just account for it independently —
    otherwise a volume that barely fits source + derived would pass the
    preflight and ENOSPC during the move."""
    staging = tmp_path / "staging"
    staging.mkdir()
    archive_parent = tmp_path / "archive"
    archive_parent.mkdir()

    monkeypatch.setattr(local_processing, "MIN_DERIVED_OVERHEAD_BYTES", 0)
    monkeypatch.setattr(
        local_processing.shutil,
        "disk_usage",
        lambda path: Usage(total=10_000, used=0, free=10_000),
    )

    real_stat = local_processing.os.stat
    class FakeStat:
        def __init__(self, dev): self.st_dev = dev
    stat_by_path = {
        str(staging): FakeStat(7), str(archive_parent): FakeStat(7),
    }
    monkeypatch.setattr(
        local_processing.os, "stat",
        lambda path: stat_by_path.get(path, real_stat(path)),
    )

    plan = local_processing.storage_plan(
        str(staging),
        source_bytes=1_000,
        archive_parent=str(archive_parent),
        reserved_free_bytes=0,
    )

    # required = source + derived(25%) + extra source = 1000 + 250 + 1000
    assert plan["same_device"] is True
    assert plan["required_bytes"] == 2_250
    # The destination doesn't get a separate check when it's the same
    # device — its bytes are already inside required_bytes.
    assert plan["archive_required_bytes"] == 0
    assert plan["enough"] is True


def test_storage_plan_same_device_batching_when_insufficient(
    monkeypatch, tmp_path
):
    """Same-device case where the volume isn't big enough for both copies
    plus derived must report batching_required."""
    staging = tmp_path / "staging"
    staging.mkdir()
    archive_parent = tmp_path / "archive"
    archive_parent.mkdir()

    monkeypatch.setattr(local_processing, "MIN_DERIVED_OVERHEAD_BYTES", 0)
    # Free space barely fits one copy + derived; can't fit two copies.
    monkeypatch.setattr(
        local_processing.shutil,
        "disk_usage",
        lambda path: Usage(total=10_000, used=0, free=1_500),
    )
    real_stat = local_processing.os.stat
    class FakeStat:
        def __init__(self, dev): self.st_dev = dev
    stat_by_path = {
        str(staging): FakeStat(7), str(archive_parent): FakeStat(7),
    }
    monkeypatch.setattr(
        local_processing.os, "stat",
        lambda path: stat_by_path.get(path, real_stat(path)),
    )

    plan = local_processing.storage_plan(
        str(staging),
        source_bytes=1_000,
        archive_parent=str(archive_parent),
        reserved_free_bytes=0,
    )

    assert plan["same_device"] is True
    assert plan["required_bytes"] == 2_250
    assert plan["batching_required"] is True
    assert plan["enough"] is False


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
