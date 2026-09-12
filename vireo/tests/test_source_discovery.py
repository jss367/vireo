"""Unit tests for the shared streaming source traversal."""

import json
import threading
import time
from pathlib import Path

import source_discovery
from image_loader import ScanCancelled
from PIL import Image


def _parse(frames):
    out = []
    for chunk in frames:
        if chunk.startswith("data: "):
            out.append(json.loads(chunk[len("data: "):].strip()))
    return out


def test_unique_root_names_disambiguates_same_leaf():
    names = source_discovery.unique_root_names(
        ["/mnt/cardA/DCIM", "/mnt/cardB/DCIM"])
    assert names["/mnt/cardA/DCIM"] == "cardA/DCIM"
    assert names["/mnt/cardB/DCIM"] == "cardB/DCIM"


def test_unique_root_names_single_source_is_empty():
    assert source_discovery.unique_root_names(["/photos/trip"]) == {}


def _serial_network_policy(paths):
    return [
        {
            "path": path,
            "volume_key": "nas",
            "storage": "network",
            "max_parallel": 1,
        }
        for path in paths
    ]


def test_preview_capture_dates_use_photo_metadata_not_file_mtime(tmp_path):
    dated = tmp_path / "dated.jpg"
    unknown = tmp_path / "unknown.jpg"
    exif = Image.Exif()
    exif[36867] = "2026:08:09 23:59:00"
    Image.new("RGB", (8, 8)).save(dated, exif=exif)
    Image.new("RGB", (8, 8)).save(unknown)

    frames = _parse(source_discovery.stream_folder_preview(
        [str(tmp_path)], include_capture_dates=True,
        classify=_serial_network_policy))
    files = {f["filename"]: f for f in frames[-1]["files"]}
    assert files["dated.jpg"]["capture_date"] == "2026-08-09"
    assert files["unknown.jpg"]["capture_date"] is None
    assert files["unknown.jpg"]["mtime"] > 0
    assert any(f.get("stage") == "capture_dates" for f in frames)


def test_capture_date_reads_stop_between_batches(tmp_path, monkeypatch):
    files = [tmp_path / f"photo-{i}.jpg" for i in range(130)]
    for path in files:
        path.touch()
    cancel = threading.Event()
    batches = []

    def timestamps(batch):
        batches.append(batch)
        cancel.set()
        return {}

    monkeypatch.setattr(source_discovery, "source_capture_timestamps", timestamps)
    result = source_discovery._walk_folder(
        str(tmp_path), "card", False, "both", True, cancel, lambda _event: None,
        include_capture_dates=True)
    assert result is None
    assert len(batches) == 1
    assert len(batches[0]) == 128


def test_closing_the_stream_cancels_running_walkers(monkeypatch):
    """Client disconnect must stop the disk walk, not orphan it.

    This is the property the whole streaming design exists for: an aborted
    fetch used to leave the server walking the folder to completion.
    """
    walker_started = threading.Event()
    walker_exited = threading.Event()

    def fake_discover(folder, file_types="both", recursive=True, onerror=None,
                      cancel_check=None, progress_callback=None):
        walker_started.set()
        while not cancel_check():
            time.sleep(0.01)
        walker_exited.set()
        raise ScanCancelled("cancelled")

    monkeypatch.setattr(
        source_discovery, "discover_source_files", fake_discover)
    gen = source_discovery.stream_folder_preview(
        ["/slow/nas"], classify=_serial_network_policy)
    # policy frame, folder_started frame, then one heartbeat/ping while the
    # walker grinds — the generator is suspended mid-walk at that point.
    frames = [next(gen), next(gen), next(gen)]
    parsed = _parse(frames)
    assert parsed[0]["type"] == "policy"
    assert parsed[1]["type"] == "folder_started"
    assert walker_started.wait(timeout=5)

    gen.close()
    assert walker_exited.wait(timeout=5), "walker kept running after close"


def test_volume_lane_is_shared_across_preview_streams(monkeypatch):
    """A replacement request waits for a blocked cancelled walker to exit."""
    first_started = threading.Event()
    first_exited = threading.Event()
    second_started = threading.Event()
    release_first = threading.Event()

    def fake_discover(folder, **_kwargs):
        if folder == "/slow/first":
            first_started.set()
            release_first.wait(timeout=5)
            first_exited.set()
            raise ScanCancelled("cancelled after filesystem call returned")
        second_started.set()
        return []

    monkeypatch.setattr(
        source_discovery, "discover_source_files", fake_discover)
    first = source_discovery.stream_folder_preview(
        ["/slow/first"], classify=_serial_network_policy)
    second = source_discovery.stream_folder_preview(
        ["/slow/second"], classify=_serial_network_policy)

    next(first)  # policy
    next(first)  # folder_started
    next(first)  # starts worker, then heartbeat
    assert first_started.wait(timeout=5)

    next(second)  # policy
    assert next(second).startswith(": ping")
    assert not second_started.is_set()

    first.close()
    assert not first_exited.wait(timeout=0.05)
    assert next(second).startswith(": ping")
    assert not second_started.is_set()

    release_first.set()
    assert first_exited.wait(timeout=5)
    started = next(second)
    assert json.loads(started[len("data: "):])["type"] == "folder_started"
    list(second)
    assert second_started.is_set()


def test_global_scan_limit_is_shared_across_preview_streams():
    """Distinct volume lanes still share one process-wide worker cap."""
    acquired = []
    try:
        for index in range(source_discovery.GLOBAL_SCAN_LIMIT):
            volume_key = f"volume-{index}"
            assert source_discovery._try_acquire_volume_lane(volume_key, 2)
            acquired.append(volume_key)
        assert not source_discovery._try_acquire_volume_lane("one-too-many", 2)
    finally:
        for volume_key in acquired:
            source_discovery._release_volume_lane(volume_key)


def test_stream_survives_a_crashing_walker(monkeypatch):
    """A walker that dies unexpectedly yields an error row, not a hang."""

    def exploding_discover(folder, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(
        source_discovery, "discover_source_files", exploding_discover)
    frames = _parse(list(source_discovery.stream_folder_preview(
        ["/photos/a"], classify=_serial_network_policy)))
    done = [f for f in frames if f["type"] == "folder_done"]
    assert done == [{
        "type": "folder_done", "path": "/photos/a", "count": 0, "error": True,
    }]
    assert frames[-1]["type"] == "done"
    assert frames[-1]["total_count"] == 0
    assert frames[-1]["source_counts"] == {"/photos/a": 0}


def test_metadata_progress_is_throttled_by_time_not_file_count(
        tmp_path, monkeypatch):
    root = tmp_path / "card"
    root.mkdir()
    files = []
    for index in range(3):
        photo = root / f"photo-{index}.jpg"
        photo.write_bytes(b"x")
        files.append(photo)

    monkeypatch.setattr(
        source_discovery, "discover_source_files", lambda *_args, **_kwargs: files,
    )
    ticks = iter([0.0, 0.1, 0.4])
    monkeypatch.setattr(source_discovery.time, "monotonic", ticks.__next__)
    events = []

    result = source_discovery._walk_folder(
        str(root), Path(root).name, False, "both", True,
        threading.Event(), events.append,
    )

    assert result["count"] == 3
    assert [event for event in events if event.get("stage") == "metadata"] == [{
        "type": "folder_progress",
        "path": str(root),
        "stage": "metadata",
        "checked": 3,
        "found": 3,
    }]
