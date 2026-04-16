"""Tests for scripts/build_test_photos.py helpers."""
import importlib.util
import sys
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).parent.parent.parent / "scripts" / "build_test_photos.py"
spec = importlib.util.spec_from_file_location("build_test_photos", _SCRIPT)
build_test_photos = importlib.util.module_from_spec(spec)
sys.modules["build_test_photos"] = build_test_photos
spec.loader.exec_module(build_test_photos)


def test_classify_ext_raws(tmp_path):
    assert build_test_photos.classify_ext(tmp_path / "x.cr2") == "raw"
    assert build_test_photos.classify_ext(tmp_path / "x.NEF") == "raw"
    assert build_test_photos.classify_ext(tmp_path / "x.dng") == "raw"


def test_classify_ext_jpegs(tmp_path):
    assert build_test_photos.classify_ext(tmp_path / "x.jpg") == "jpeg"
    assert build_test_photos.classify_ext(tmp_path / "x.JPEG") == "jpeg"


def test_classify_ext_skip(tmp_path):
    assert build_test_photos.classify_ext(tmp_path / "notes.txt") == "skip"
    assert build_test_photos.classify_ext(tmp_path / "video.mp4") == "skip"


def test_content_hash_matches_for_identical_content(tmp_path):
    a = tmp_path / "a.bin"
    b = tmp_path / "b.bin"
    a.write_bytes(b"same content")
    b.write_bytes(b"same content")
    assert build_test_photos.content_hash(a) == build_test_photos.content_hash(b)


def test_content_hash_differs_for_different_content(tmp_path):
    a = tmp_path / "a.bin"
    b = tmp_path / "b.bin"
    a.write_bytes(b"one")
    b.write_bytes(b"two")
    assert build_test_photos.content_hash(a) != build_test_photos.content_hash(b)


def test_find_duplicates(tmp_path):
    files = [tmp_path / f"f{i}.jpg" for i in range(4)]
    files[0].write_bytes(b"A")
    files[1].write_bytes(b"A")
    files[2].write_bytes(b"B")
    files[3].write_bytes(b"C")
    groups = build_test_photos.find_duplicates(files)
    assert len(groups) == 1
    assert len(groups[0][1]) == 2


def test_find_burst(tmp_path):
    import os
    files = []
    base_time = 1_700_000_000
    # Burst of 4 within 2 seconds
    for i in range(4):
        f = tmp_path / f"burst{i}.jpg"
        f.write_bytes(b"x")
        os.utime(f, (base_time + i * 0.5, base_time + i * 0.5))
        files.append(f)
    # A singleton far away
    singleton = tmp_path / "singleton.jpg"
    singleton.write_bytes(b"x")
    os.utime(singleton, (base_time + 1000, base_time + 1000))
    files.append(singleton)

    burst = build_test_photos.find_burst(files, window_seconds=2)
    assert len(burst) == 4


def test_safe_dest_rejects_home(monkeypatch):
    with pytest.raises(SystemExit, match="HOME"):
        build_test_photos._safe_dest(str(Path.home()))


def test_safe_dest_rejects_under_vireo(monkeypatch):
    with pytest.raises(SystemExit, match=r"\.vireo"):
        build_test_photos._safe_dest(str(Path.home() / ".vireo" / "photos"))


def test_safe_dest_accepts_tmp(tmp_path):
    result = build_test_photos._safe_dest(str(tmp_path / "photos"))
    assert result == (tmp_path / "photos").resolve()


def test_sample_dry_run(tmp_path):
    source = tmp_path / "src"
    source.mkdir()
    (source / "a.jpg").write_bytes(b"A")
    (source / "b.cr2").write_bytes(b"B")
    (source / "c.nef").write_bytes(b"C")
    dest = tmp_path / "dest"
    result = build_test_photos.sample(source, dest, dry_run=True)
    assert "raws" in result
    assert "jpegs" in result
    assert not dest.exists()


def test_sample_copies_and_writes_manifest(tmp_path):
    source = tmp_path / "src"
    source.mkdir()
    (source / "a.jpg").write_bytes(b"A")
    (source / "b.cr2").write_bytes(b"B")
    dest = tmp_path / "dest"
    build_test_photos.sample(source, dest, counts={"gps_yes":0,"gps_no":0,"raws":1,"jpegs":1,"random":0})
    manifest = dest / "MANIFEST.md"
    assert manifest.exists()
    content = manifest.read_text()
    assert "raws" in content
    assert "jpegs" in content


def test_sample_disambiguates_same_basename_different_content(tmp_path):
    # Two different files with the same basename (common when libraries have
    # repeated IMG_0001.jpg names across folders) must both land in dest.
    source = tmp_path / "src"
    (source / "dir1").mkdir(parents=True)
    (source / "dir2").mkdir(parents=True)
    (source / "dir1" / "IMG_0001.jpg").write_bytes(b"AAA")
    (source / "dir2" / "IMG_0001.jpg").write_bytes(b"BBB")
    dest = tmp_path / "dest"
    build_test_photos.sample(
        source,
        dest,
        counts={"gps_yes": 0, "gps_no": 0, "raws": 0, "jpegs": 2, "random": 0},
    )
    copied = list((dest / "jpegs").iterdir())
    assert len(copied) == 2, f"expected 2 files, got {[p.name for p in copied]}"
    contents = sorted(p.read_bytes() for p in copied)
    assert contents == [b"AAA", b"BBB"]


def test_sample_disambiguates_same_basename_identical_content(tmp_path):
    # Two source paths with the same basename AND identical bytes must still
    # land as two distinct files on disk — otherwise the duplicates category
    # can't actually populate the duplicate-detection scenarios it exists for.
    source = tmp_path / "src"
    (source / "dir1").mkdir(parents=True)
    (source / "dir2").mkdir(parents=True)
    (source / "dir1" / "IMG_0001.jpg").write_bytes(b"SAME")
    (source / "dir2" / "IMG_0001.jpg").write_bytes(b"SAME")
    dest = tmp_path / "dest"
    build_test_photos.sample(
        source,
        dest,
        counts={"gps_yes": 0, "gps_no": 0, "raws": 0, "jpegs": 2, "random": 0},
    )
    copied = list((dest / "jpegs").iterdir())
    assert len(copied) == 2, f"expected 2 files, got {[p.name for p in copied]}"
    assert all(p.read_bytes() == b"SAME" for p in copied)


def test_sample_is_idempotent(tmp_path):
    source = tmp_path / "src"
    source.mkdir()
    (source / "a.jpg").write_bytes(b"A")
    dest = tmp_path / "dest"
    counts = {"gps_yes":0,"gps_no":0,"raws":0,"jpegs":1,"random":0}
    build_test_photos.sample(source, dest, counts=counts)
    mtime1 = (dest / "jpegs" / "a.jpg").stat().st_mtime
    build_test_photos.sample(source, dest, counts=counts)
    mtime2 = (dest / "jpegs" / "a.jpg").stat().st_mtime
    assert mtime1 == mtime2


def test_sample_idempotent_under_reversed_walk_order(tmp_path, monkeypatch):
    # os.walk() order isn't guaranteed stable across runs. For same-basename +
    # identical-content sources, the file that's encountered second gets the
    # hashed filename. If walk order flips, the *other* source becomes second
    # and a fresh hashed copy is created — breaking idempotency. Verified fix:
    # sample() sorts its file list, so the decision is stable regardless of
    # walk order.
    import os as real_os

    source = tmp_path / "src"
    (source / "dir1").mkdir(parents=True)
    (source / "dir2").mkdir(parents=True)
    (source / "dir1" / "IMG_0001.jpg").write_bytes(b"SAME")
    (source / "dir2" / "IMG_0001.jpg").write_bytes(b"SAME")
    dest = tmp_path / "dest"
    counts = {"gps_yes": 0, "gps_no": 0, "raws": 0, "jpegs": 2, "random": 0}

    build_test_photos.sample(source, dest, counts=counts)
    files_after_first = sorted(p.name for p in (dest / "jpegs").iterdir())
    assert len(files_after_first) == 2

    # Second run: flip os.walk's output order to simulate filesystem order drift.
    # Must reverse both the sequence of yielded (root, dirs, files) tuples AND
    # the files within each tuple — otherwise the iteration that builds
    # `all_files` sees the same order as the first run and the bug is hidden.
    original_walk = real_os.walk

    def reversed_walk(top, *args, **kwargs):
        entries = [
            (root, dirs, list(reversed(files)))
            for root, dirs, files in original_walk(top, *args, **kwargs)
        ]
        yield from reversed(entries)

    monkeypatch.setattr(build_test_photos.os, "walk", reversed_walk)
    build_test_photos.sample(source, dest, counts=counts)

    files_after_second = sorted(p.name for p in (dest / "jpegs").iterdir())
    assert files_after_second == files_after_first, (
        f"reversed walk order created extra files: {files_after_second}"
    )


def test_sample_rejects_missing_source(tmp_path):
    # A typo in --source would otherwise produce an empty dataset with all-zero
    # counts and silently invalidate downstream tests. Fail loudly instead.
    missing = tmp_path / "does-not-exist"
    dest = tmp_path / "dest"
    with pytest.raises(SystemExit, match="source does not exist"):
        build_test_photos.sample(missing, dest)


def test_sample_rejects_source_that_is_a_file(tmp_path):
    # is_dir() also covers the case where --source points at a regular file.
    not_a_dir = tmp_path / "a.jpg"
    not_a_dir.write_bytes(b"A")
    dest = tmp_path / "dest"
    with pytest.raises(SystemExit, match="source does not exist"):
        build_test_photos.sample(not_a_dir, dest)


def test_sample_rejects_source_equals_dest(tmp_path):
    # If dest is the source directory itself, the walk-filter can't exclude
    # category subdirs (they become children of the walk root) and a rerun
    # would re-ingest its own outputs into the real source library.
    source = tmp_path / "src"
    source.mkdir()
    (source / "a.jpg").write_bytes(b"A")
    with pytest.raises(SystemExit, match="source as destination"):
        build_test_photos.sample(
            source,
            source,
            counts={"gps_yes": 0, "gps_no": 0, "raws": 0, "jpegs": 1, "random": 0},
        )


def test_sample_idempotent_when_dest_is_inside_source(tmp_path):
    # If the user picks a dest path inside source (e.g.
    # `--dest <source>/vireo-test-photos`), a naive walk re-ingests prior
    # outputs on every rerun and silently grows the sampled set. Verify the
    # walk skips the dest subtree, so a second run produces no new files.
    source = tmp_path / "src"
    source.mkdir()
    (source / "a.jpg").write_bytes(b"A")
    (source / "b.jpg").write_bytes(b"B")
    dest = source / "vireo-test-photos"
    counts = {"gps_yes": 0, "gps_no": 0, "raws": 0, "jpegs": 2, "random": 0}

    build_test_photos.sample(source, dest, counts=counts)
    first = sorted(p.name for p in (dest / "jpegs").iterdir())
    assert len(first) == 2

    build_test_photos.sample(source, dest, counts=counts)
    second = sorted(p.name for p in (dest / "jpegs").iterdir())
    assert second == first, (
        f"dest-inside-source re-ingested outputs: {second} vs {first}"
    )
