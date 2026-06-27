import os
from collections import namedtuple

import local_processing
import pytest

Usage = namedtuple("Usage", "total used free")


def test_staging_root_uses_final_destination_basename(tmp_path):
    root = local_processing.staging_root(
        str(tmp_path), "pipeline-123", "/Volumes/NAS/Photos"
    )

    assert root == os.path.join(str(tmp_path), "staging", "pipeline-123", "Photos")


def test_staging_root_rejects_filesystem_root(tmp_path):
    with pytest.raises(ValueError, match="filesystem root"):
        local_processing.staging_root(str(tmp_path), "pipeline-123", os.sep)

    with pytest.raises(ValueError, match="filesystem root"):
        local_processing.staging_root(str(tmp_path), "pipeline-123", "C:\\")


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


def test_existing_archive_bytes_sums_matching_source_files(tmp_path):
    """existing_archive_bytes() credits only files that match the selected
    source files at their archive-relative paths."""
    src = tmp_path / "card"
    src.mkdir()
    first = src / "a.jpg"
    first.write_bytes(b"x" * 100)
    second = src / "b.jpg"
    second.write_bytes(b"y" * 250)

    dest = tmp_path / "archive"
    dest.mkdir()
    (dest / "a.jpg").write_bytes(b"x" * 100)
    (dest / "b.jpg").write_bytes(b"y" * 250)
    (dest / "unrelated.jpg").write_bytes(b"z" * 500)

    assert local_processing.existing_archive_bytes(
        str(dest),
        [first, second],
        folder_template="",
    ) == 350


def test_existing_archive_bytes_ignores_unmatched_or_different_files(tmp_path):
    """Existing files must not reduce required archive space unless they are
    exact matches for the source file at the path ingest would create."""
    src = tmp_path / "card"
    src.mkdir()
    first = src / "a.jpg"
    first.write_bytes(b"x" * 100)
    same_size_different = src / "same-size.jpg"
    same_size_different.write_bytes(b"source")

    dest = tmp_path / "archive"
    dest.mkdir()
    (dest / "unrelated.jpg").write_bytes(b"x" * 100)
    (dest / "same-size.jpg").write_bytes(b"target")

    assert local_processing.existing_archive_bytes(
        str(dest),
        [first, same_size_different],
        folder_template="",
    ) == 0


def test_existing_archive_bytes_ignores_symlink_matches(tmp_path):
    """Symlinked archive entries are not safe resume credit, even when their
    target bytes match the source file."""
    src = tmp_path / "card"
    src.mkdir()
    source = src / "linked.jpg"
    source.write_bytes(b"same")

    dest = tmp_path / "archive"
    dest.mkdir()
    target = dest / "target.jpg"
    target.write_bytes(b"same")
    try:
        (dest / "linked.jpg").symlink_to(target)
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"symlinks unavailable: {exc}")

    assert local_processing.existing_archive_bytes(
        str(dest),
        [source],
        folder_template="",
    ) == 0


def test_conflicting_archive_paths_reports_different_existing_files(tmp_path):
    """Same archive-relative path with different content must be rejected
    before the final merge-mode archive step."""
    src = tmp_path / "card"
    src.mkdir()
    matching = src / "matching.jpg"
    matching.write_bytes(b"same")
    conflicting = src / "conflict.jpg"
    conflicting.write_bytes(b"source")
    missing = src / "missing.jpg"
    missing.write_bytes(b"new")

    dest = tmp_path / "archive"
    dest.mkdir()
    (dest / "matching.jpg").write_bytes(b"same")
    (dest / "conflict.jpg").write_bytes(b"target")

    assert local_processing.conflicting_archive_paths(
        str(dest),
        [matching, conflicting, missing],
        folder_template="",
    ) == [str(dest / "conflict.jpg")]


def test_conflicting_archive_paths_reports_symlink_even_when_target_matches(tmp_path):
    """A symlink at the archive-relative resume path must be treated as a
    conflict before staging, not as a safe same-content resume hit."""
    src = tmp_path / "card"
    src.mkdir()
    source = src / "linked.jpg"
    source.write_bytes(b"same")

    dest = tmp_path / "archive"
    dest.mkdir()
    target = dest / "target.jpg"
    target.write_bytes(b"same")
    try:
        (dest / "linked.jpg").symlink_to(target)
    except (OSError, NotImplementedError) as exc:
        pytest.skip(f"symlinks unavailable: {exc}")

    assert local_processing.conflicting_archive_paths(
        str(dest),
        [source],
        folder_template="",
    ) == [str(dest / "linked.jpg")]


def test_conflicting_archive_paths_skips_known_duplicates(tmp_path):
    """When skip_duplicates would skip a source whose hash is already in the
    catalog, that source never reaches staging and so cannot conflict at
    archive time. Passing the catalog hash set lets the preflight mirror
    that behavior instead of falsely aborting the run."""
    from scanner import compute_file_hash

    src = tmp_path / "card"
    src.mkdir()
    already_imported = src / "imported.jpg"
    already_imported.write_bytes(b"source-bytes")
    fresh = src / "fresh.jpg"
    fresh.write_bytes(b"new-bytes")

    dest = tmp_path / "archive"
    dest.mkdir()
    # The archive already has a different file at the duplicate source's
    # path. Without the duplicate filter, the preflight would flag this.
    (dest / "imported.jpg").write_bytes(b"unrelated-target")

    # Without known_hashes: imported.jpg is reported as a conflict.
    assert local_processing.conflicting_archive_paths(
        str(dest),
        [already_imported, fresh],
        folder_template="",
    ) == [str(dest / "imported.jpg")]

    # With imported.jpg's hash in the catalog: ingest will skip it, so
    # the preflight no longer reports the (irrelevant) archive collision.
    known = {compute_file_hash(str(already_imported))}
    assert local_processing.conflicting_archive_paths(
        str(dest),
        [already_imported, fresh],
        folder_template="",
        known_hashes=known,
    ) == []


def test_conflicting_archive_paths_skips_duplicates_before_claiming_names(
    tmp_path,
):
    """A catalog duplicate must not reserve a destination filename before a
    later fresh source with the same basename is evaluated."""
    from scanner import compute_file_hash

    src = tmp_path / "card"
    src.mkdir()
    already_imported = src / "existing" / "frame.jpg"
    already_imported.parent.mkdir()
    already_imported.write_bytes(b"already-imported")
    fresh = src / "fresh" / "frame.jpg"
    fresh.parent.mkdir()
    fresh.write_bytes(b"fresh-bytes")

    dest = tmp_path / "archive"
    dest.mkdir()
    (dest / "frame_1.jpg").write_bytes(b"unrelated-target")

    # Without duplicate filtering, the first frame.jpg claims the unsuffixed
    # slot and the fresh frame.jpg is correctly checked against frame_1.jpg.
    assert local_processing.conflicting_archive_paths(
        str(dest),
        [already_imported, fresh],
        folder_template="",
    ) == [str(dest / "frame_1.jpg")]

    # With the first source already in the catalog, ingest skips it before
    # claiming frame.jpg. The fresh source therefore stages as frame.jpg, not
    # frame_1.jpg, and the unrelated partial frame_1.jpg is irrelevant.
    known = {compute_file_hash(str(already_imported))}
    assert local_processing.conflicting_archive_paths(
        str(dest),
        [already_imported, fresh],
        folder_template="",
        known_hashes=known,
    ) == []


def test_conflicting_archive_paths_tracks_survivor_hashes(tmp_path):
    """When skip_duplicates is enabled and the selected sources contain the
    same bytes twice (for example the same card folder selected twice),
    ingest() copies the first occurrence, adds its hash to the known-hash
    set, and skips later same-hash sources before staging. The preflight
    must mirror that: a later same-hash source that happens to map to an
    existing different archive path must not be reported as a conflict,
    because ingest will skip it before staging.

    The earlier survivor's archive path is missing in this scenario, so
    the earlier branch never hashes it. The function must therefore fold
    earlier survivors' hashes into ``seen_hashes`` lazily before checking
    a later same-hash source against an existing archive collision."""
    src = tmp_path / "card"
    src.mkdir()
    # Two source files with identical bytes (same content hash) but
    # different names so they map to different archive paths.
    first = src / "first.jpg"
    first.write_bytes(b"identical-content")
    second = src / "second.jpg"
    second.write_bytes(b"identical-content")

    dest = tmp_path / "archive"
    dest.mkdir()
    # The first source's archive path is missing (no conflict — survivor),
    # so the function never hashes it in the simple branch. The second
    # source's archive path collides with an unrelated file. Without the
    # survivor-hash fix, the function would hash `second`, find its hash
    # missing from seen_hashes (since `first` was never hashed), and
    # falsely report a conflict — even though ingest would skip `second`
    # as an intra-batch duplicate of `first`.
    (dest / "second.jpg").write_bytes(b"unrelated-content")

    # Empty known_hashes (skip_duplicates on, no catalog matches) is
    # enough to enable the survivor-tracking branch.
    assert local_processing.conflicting_archive_paths(
        str(dest),
        [first, second],
        folder_template="",
        known_hashes=set(),
    ) == []


def test_conflicting_archive_paths_mirrors_ingest_filename_suffix(tmp_path):
    """Two sources that share an import folder and filename but differ in
    content are staged as ``name.ext`` and ``name_1.ext``. The preflight
    must compare the second source against the suffixed archive path —
    not also against the unsuffixed one — otherwise a resume where the
    archive already contains the first exact match incorrectly rejects
    the second source as a conflict."""
    src = tmp_path / "card"
    src.mkdir()
    first = src / "subdir-a" / "frame.jpg"
    first.parent.mkdir()
    first.write_bytes(b"first-content")
    # Same basename as `first` but in a different source subfolder, so
    # both map to the same archive-relative path under folder_template="".
    second = src / "subdir-b" / "frame.jpg"
    second.parent.mkdir()
    second.write_bytes(b"second-content")

    dest = tmp_path / "archive"
    dest.mkdir()
    # The archive already contains the first source verbatim — the
    # second source would be staged under the suffixed name, and no
    # `frame_1.jpg` exists at the archive, so the run must not abort.
    (dest / "frame.jpg").write_bytes(b"first-content")

    assert local_processing.conflicting_archive_paths(
        str(dest),
        [first, second],
        folder_template="",
    ) == []

    # If the archive also has the suffixed slot taken by a different
    # file, the second source genuinely conflicts there and must be
    # reported — confirming the suffix tracking still surfaces real
    # archive collisions.
    (dest / "frame_1.jpg").write_bytes(b"unrelated")
    assert local_processing.conflicting_archive_paths(
        str(dest),
        [first, second],
        folder_template="",
    ) == [str(dest / "frame_1.jpg")]


def test_existing_archive_bytes_credits_suffixed_resumes(tmp_path):
    """When two selected files share a basename in the same import folder,
    ingest() stages the first as ``name.ext`` and the second as
    ``name_1.ext``. A previous partial archive that already contains both
    suffixed variants must credit both source files' bytes — otherwise the
    second source (whose dest path collides with the first's credit) earns
    zero credit and the retry can be batching-rejected even though the
    delta would fit."""
    src = tmp_path / "card"
    src.mkdir()
    # Two source files with the same basename but in different subfolders,
    # so both map to the same archive folder under folder_template="".
    first = src / "a" / "frame.jpg"
    first.parent.mkdir()
    first.write_bytes(b"first-content")
    second = src / "b" / "frame.jpg"
    second.parent.mkdir()
    second.write_bytes(b"second-content-longer")

    dest = tmp_path / "archive"
    dest.mkdir()
    # A partial prior archive run already staged both files at their
    # ingest-assigned paths. The retry must credit both.
    (dest / "frame.jpg").write_bytes(b"first-content")
    (dest / "frame_1.jpg").write_bytes(b"second-content-longer")

    assert local_processing.existing_archive_bytes(
        str(dest),
        [first, second],
        folder_template="",
    ) == len(b"first-content") + len(b"second-content-longer")


def test_existing_archive_bytes_returns_zero_for_missing_or_file(tmp_path):
    """Missing directories and regular files give no credit — the caller
    treats them as a fresh archive run."""
    source = tmp_path / "source.jpg"
    source.write_bytes(b"source")

    assert local_processing.existing_archive_bytes(
        str(tmp_path / "does-not-exist"),
        [source],
        folder_template="",
    ) == 0

    plain = tmp_path / "plain.txt"
    plain.write_bytes(b"hello")
    assert local_processing.existing_archive_bytes(
        str(plain),
        [source],
        folder_template="",
    ) == 0


def test_storage_plan_credits_existing_archive_on_resume(monkeypatch, tmp_path):
    """When a previous archive attempt left bytes at the destination, the
    preflight credits them: move_folder merge-mode rsyncs only the delta,
    so a retry whose remaining work fits must not be rejected as
    batching-required."""
    staging = tmp_path / "staging"
    staging.mkdir()
    archive_parent = tmp_path / "archive_parent"
    archive_parent.mkdir()

    monkeypatch.setattr(local_processing, "MIN_DERIVED_OVERHEAD_BYTES", 0)
    usage_by_path = {
        str(staging): Usage(total=10_000, used=0, free=10_000),
        # Without the credit, archive_required would be 800 and this
        # destination (free=300) would fail the preflight. With 700 bytes
        # already archived, the delta is only 100 and the retry fits.
        str(archive_parent): Usage(total=10_000, used=9_700, free=300),
    }
    monkeypatch.setattr(
        local_processing.shutil,
        "disk_usage",
        lambda path: usage_by_path[path],
    )

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
        source_bytes=800,
        archive_parent=str(archive_parent),
        archive_existing_bytes=700,
        reserved_free_bytes=0,
    )

    assert plan["archive_existing_bytes"] == 700
    assert plan["archive_required_bytes"] == 100
    assert plan["archive_enough"] is True
    assert plan["enough"] is True
    assert plan["batching_required"] is False


def test_storage_plan_caps_existing_credit_at_source_bytes(monkeypatch, tmp_path):
    """A destination that contains more bytes than the source — extra
    unrelated content from a previous run — must not extend the credit
    past source_bytes, otherwise the preflight would always treat the
    destination as full enough regardless of the source."""
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
        archive_existing_bytes=10_000,  # absurd overshoot
        reserved_free_bytes=0,
    )

    assert plan["archive_existing_bytes"] == 500
    assert plan["archive_required_bytes"] == 0
    assert plan["archive_enough"] is True


def test_storage_plan_same_device_credits_existing_archive(monkeypatch, tmp_path):
    """On same-device runs the destination copy is the move-folder rewrite,
    which in merge mode only writes the delta. The same-device extra
    requirement must shrink to the delta too — otherwise resume runs that
    fit are rejected even when the destination is already most of the way
    there."""
    staging = tmp_path / "staging"
    staging.mkdir()
    archive_parent = tmp_path / "archive"
    archive_parent.mkdir()

    monkeypatch.setattr(local_processing, "MIN_DERIVED_OVERHEAD_BYTES", 0)
    monkeypatch.setattr(
        local_processing.shutil,
        "disk_usage",
        lambda path: Usage(total=10_000, used=0, free=1_400),
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

    # Without credit: required = 1000 + 250 + 1000 = 2250 → batching.
    # With 900 already archived: required = 1000 + 250 + 100 = 1350 → fits.
    plan = local_processing.storage_plan(
        str(staging),
        source_bytes=1_000,
        archive_parent=str(archive_parent),
        archive_existing_bytes=900,
        reserved_free_bytes=0,
    )

    assert plan["same_device"] is True
    assert plan["required_bytes"] == 1_350
    assert plan["enough"] is True


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


def test_non_duplicate_files_returns_survivor_list(tmp_path):
    """non_duplicate_files returns the file list ingest will actually copy,
    not just a byte total. The pipeline preflight feeds that list back into
    existing_archive_bytes so the destination credit on a duplicate-filtered
    retry covers only the survivors — without it, a known-duplicate file
    that's already at the destination would credit against the smaller
    survivor byte total and zero out the archive delta even though the
    fresh file still has to be written."""
    from scanner import compute_file_hash

    fresh = tmp_path / "fresh.jpg"
    fresh.write_bytes(b"fresh-bytes-payload")
    dup = tmp_path / "dup.jpg"
    dup.write_bytes(b"already-imported-content")
    other = tmp_path / "shared.jpg"
    other.write_bytes(b"shared-bytes")
    other_copy = tmp_path / "shared_copy.jpg"
    other_copy.write_bytes(b"shared-bytes")  # intra-run duplicate of `other`

    known = {compute_file_hash(str(dup))}
    survivors = local_processing.non_duplicate_files(
        [fresh, dup, other, other_copy], known,
    )
    assert survivors == [fresh, other]


def test_storage_plan_recomputes_archive_credit_for_duplicate_filtered_set(
    monkeypatch, tmp_path,
):
    """When skip_duplicates triggers the storage_plan retry, the existing-
    archive credit must be recomputed against the survivor file list, not
    reused from the full selection. Otherwise a large known-duplicate file
    already at the destination would credit against the smaller filtered
    byte count, drive archive_delta to zero, and pass the destination
    preflight — even though the fresh survivor still has to be written at
    the archive."""
    from scanner import compute_file_hash

    staging = tmp_path / "staging"
    staging.mkdir()
    archive_parent = tmp_path / "nas"
    archive_parent.mkdir()
    final_destination = archive_parent / "Photos"
    # Simulate a previous archive attempt: the large duplicate file is
    # already laid down at the resume-credit-eligible relative path.
    final_destination.mkdir()
    big_dup = tmp_path / "big_dup.jpg"
    big_dup.write_bytes(b"x" * 10_000)  # large duplicate (already cataloged)
    (final_destination / big_dup.name).write_bytes(b"x" * 10_000)

    fresh = tmp_path / "fresh.jpg"
    fresh.write_bytes(b"y" * 100)  # small fresh file (no archive copy yet)

    known = {compute_file_hash(str(big_dup))}
    survivors = local_processing.non_duplicate_files([big_dup, fresh], known)
    assert survivors == [fresh]

    # The full-set credit (used by the first plan call) covers the big
    # duplicate that's already at the destination.
    full_credit = local_processing.existing_archive_bytes(
        str(final_destination), [big_dup, fresh], folder_template="",
    )
    assert full_credit == 10_000

    # The survivor-set credit must cover only `fresh`, which is NOT at the
    # destination yet. Reusing full_credit on the retry would zero out the
    # archive delta even though fresh still has to be written.
    survivor_credit = local_processing.existing_archive_bytes(
        str(final_destination), survivors, folder_template="",
    )
    assert survivor_credit == 0

    monkeypatch.setattr(local_processing, "MIN_DERIVED_OVERHEAD_BYTES", 0)
    monkeypatch.setattr(
        local_processing.shutil,
        "disk_usage",
        lambda path: Usage(total=10_000, used=0, free=10_000),
    )

    # Force staging and archive onto different fake devices so the
    # destination-volume check actually runs (same_device skips it). The
    # bug surfaces precisely when the archive lives on a separate volume
    # whose free space the planner is responsible for checking.
    real_stat = os.stat

    def fake_stat(path, *args, **kwargs):
        result = real_stat(path, *args, **kwargs)
        if str(path).startswith(str(final_destination)):
            return os.stat_result(
                (result.st_mode, result.st_ino, 999) + tuple(result)[3:]
            )
        return result

    monkeypatch.setattr(local_processing.os, "stat", fake_stat)

    filtered_bytes = local_processing.total_file_bytes(survivors)
    plan = local_processing.storage_plan(
        str(staging),
        filtered_bytes,
        archive_parent=str(final_destination),
        archive_existing_bytes=survivor_credit,
        reserved_free_bytes=0,
    )
    assert plan["same_device"] is False
    # archive_delta = filtered_bytes (100) - survivor_credit (0) = 100.
    # Reusing full_credit (10_000) would have capped to filtered_bytes and
    # driven archive_required_bytes to 0.
    assert plan["archive_required_bytes"] == filtered_bytes

    # Sanity check the bug we're guarding against: feeding the full-set
    # credit (10_000) on the filtered retry zeroes out the archive delta,
    # which is exactly what the pipeline_job retry path must NOT do.
    buggy_plan = local_processing.storage_plan(
        str(staging),
        filtered_bytes,
        archive_parent=str(final_destination),
        archive_existing_bytes=full_credit,
        reserved_free_bytes=0,
    )
    assert buggy_plan["archive_required_bytes"] == 0


def test_conflicting_archive_paths_treats_empty_files_as_no_duplicate_identity(
    tmp_path,
):
    """Zero-byte sources must not poison ``seen_hashes`` with
    ``EMPTY_FILE_SHA256``. ingest() clears the empty-file hash to None
    before duplicate checks, so an early empty survivor must not cause a
    later empty source's same-path conflict to be falsely skipped as an
    intra-batch duplicate. Without this, a real archive collision on the
    second empty source would be hidden by the preflight and only
    surface when ``move_folder(..., merge=True)`` rejected it after
    every processing stage had already run."""
    src = tmp_path / "card"
    src.mkdir()
    # First empty source: its archive path is missing, so the function
    # only records it as a pending survivor. Without the fix,
    # _flush_pending would add EMPTY_FILE_SHA256 to seen_hashes.
    first = src / "first.jpg"
    first.write_bytes(b"")
    # Second empty source: its archive path is occupied by a different
    # file. Without the fix, the conflict branch would hash `second`,
    # see EMPTY_FILE_SHA256 in seen_hashes from the flush, and treat
    # `second` as an intra-batch duplicate skip — so the real conflict
    # would never reach the caller.
    second = src / "second.jpg"
    second.write_bytes(b"")

    dest = tmp_path / "archive"
    dest.mkdir()
    (dest / "second.jpg").write_bytes(b"unrelated non-empty content")

    # Empty known_hashes (skip_duplicates on, no catalog matches) still
    # enables the survivor-tracking branch. With the fix, the empty
    # files have no duplicate identity, so the conflict on `second` is
    # reported instead of being silently swallowed.
    conflicts = local_processing.conflicting_archive_paths(
        str(dest),
        [first, second],
        folder_template="",
        known_hashes=set(),
    )
    assert conflicts == [str(dest / "second.jpg")]


def test_non_duplicate_files_keeps_zero_byte_sources(tmp_path):
    """ingest() doesn't dedupe zero-byte files by hash, so the survivor
    list mustn't collapse two empty sources into one — that would
    under-count the staged set and let downstream callers (such as the
    duplicate-filtered storage retry) credit destination space against
    the wrong number of bytes."""
    fresh = tmp_path / "fresh.jpg"
    fresh.write_bytes(b"fresh-bytes-payload")
    empty_a = tmp_path / "empty_a.jpg"
    empty_a.write_bytes(b"")
    empty_b = tmp_path / "empty_b.jpg"
    empty_b.write_bytes(b"")

    survivors = local_processing.non_duplicate_files(
        [fresh, empty_a, empty_b], set(),
    )
    # Both empty sources survive, matching ingest's copy-both-as-
    # placeholders behavior.
    assert survivors == [fresh, empty_a, empty_b]

    # Even when an empty hash is in known_hashes (which scanner is
    # careful never to write, but a buggy caller might pass in), the
    # empty source must still survive — ingest itself ignores
    # EMPTY_FILE_SHA256 entries when deciding what to skip.
    from scanner import EMPTY_FILE_SHA256

    survivors_with_poisoned_known = local_processing.non_duplicate_files(
        [fresh, empty_a, empty_b], {EMPTY_FILE_SHA256},
    )
    assert survivors_with_poisoned_known == [fresh, empty_a, empty_b]
