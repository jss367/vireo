import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
from db import Database
from PIL import Image


@pytest.fixture
def app_and_db(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    import config as cfg
    from app import create_app

    cfg.CONFIG_PATH = str(tmp_path / "config.json")
    db_path = str(tmp_path / "test.db")
    thumb_dir = str(tmp_path / "thumbs")
    os.makedirs(thumb_dir)

    db = Database(db_path)
    ws_id = db.ensure_default_workspace()
    db.set_active_workspace(ws_id)
    fid = db.add_folder(str(tmp_path / "library"), name="library")

    app = create_app(db_path=db_path, thumb_cache_dir=thumb_dir)
    return app, db, fid


def parse_sse_events(response_data):
    """Parse SSE events from raw response bytes."""
    text = response_data.decode("utf-8")
    events = []
    for block in text.split("\n\n"):
        for line in block.strip().split("\n"):
            if line.startswith("data: "):
                events.append(json.loads(line[6:]))
    return events


@pytest.mark.parametrize("verify_by_hash", [False, True])
@pytest.mark.parametrize("skip_duplicates", [False, True])
@pytest.mark.parametrize("with_destination", [False, True])
def test_capture_dates_share_duplicate_preparation_reads(
        app_and_db, tmp_path, monkeypatch, verify_by_hash, skip_duplicates, with_destination):
    from datetime import datetime

    import import_dedup

    app, _, _ = app_and_db
    dated = tmp_path / "dated.jpg"
    unknown = tmp_path / "unknown.jpg"
    dated.write_bytes(b"dated")
    unknown.write_bytes(b"unknown")
    batches = []

    def capture_times(paths):
        batches.append(list(paths))
        return {path: datetime(2026, 8, 9, 23, 59) if path == dated else None for path in paths}

    monkeypatch.setattr(import_dedup, "source_capture_timestamps", capture_times)
    response = app.test_client().post("/api/import/check-duplicates", json={
        "paths": [str(dated), str(unknown)],
        "verify_by_hash": verify_by_hash,
        "skip_duplicates": skip_duplicates,
        "include_capture_dates": True,
        **({"destination": str(tmp_path / "archive")} if with_destination else {}),
    })
    assert response.status_code == 200
    events = parse_sse_events(response.data)
    dates = {}
    for event in events:
        dates.update(event.get("capture_dates", {}))
    assert dates == {str(dated): "2026-08-09", str(unknown): None}
    assert batches == [[dated, unknown]]
    assert events[-1]["done"] is True


def test_check_duplicates_marks_known_hashes(app_and_db, tmp_path):
    """Files whose hash exists in DB are reported as duplicates."""
    app, db, fid = app_and_db

    # Create an image that exists in the "library" (scanned, hash in DB)
    library_dir = tmp_path / "library"
    library_dir.mkdir(exist_ok=True)
    img = Image.new("RGB", (50, 50), color="red")
    img.save(str(library_dir / "existing.jpg"))

    # Scan to populate file_hash
    from scanner import scan
    scan(str(library_dir), db)

    # Create source folder with a duplicate and a new file
    source = tmp_path / "source"
    source.mkdir()
    img.save(str(source / "duplicate.jpg"))  # Same content = same hash
    Image.new("RGB", (50, 50), color="blue").save(str(source / "unique.jpg"))

    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={
        "paths": [str(source / "duplicate.jpg"), str(source / "unique.jpg")],
    })
    assert resp.status_code == 200

    events = parse_sse_events(resp.data)
    # Find the done event
    done = [e for e in events if e.get("done")]
    assert len(done) == 1
    assert done[0]["duplicate_count"] == 1

    # Collect all duplicate paths across batch events
    all_duplicates = []
    for e in events:
        if "duplicates" in e:
            all_duplicates.extend(e["duplicates"])
    assert str(source / "duplicate.jpg") in all_duplicates
    assert str(source / "unique.jpg") not in all_duplicates


def test_check_duplicates_metadata_match_without_hashing(
    app_and_db, tmp_path, monkeypatch
):
    """A cataloged (filename, size, capture time) twin is flagged as a
    duplicate without any content read — the default heuristic mode."""
    from datetime import datetime

    from PIL.ExifTags import Base as ExifBase

    app, db, fid = app_and_db

    source = tmp_path / "source"
    source.mkdir()
    img = Image.new("RGB", (50, 50), color="red")
    exif = img.getexif()
    exif[ExifBase.DateTimeOriginal] = "2026:05:01 10:15:30"
    img.save(str(source / "IMG_0001.jpg"), exif=exif)

    db.add_photo(
        folder_id=fid,
        filename="IMG_0001.jpg",
        extension=".jpg",
        file_size=os.path.getsize(str(source / "IMG_0001.jpg")),
        file_mtime=1.0,
        timestamp=datetime(2026, 5, 1, 10, 15, 30).isoformat(),
    )

    import import_dedup

    def _boom(path, *a, **kw):
        raise AssertionError(f"content hash computed for {path}")

    monkeypatch.setattr(import_dedup, "compute_file_hash", _boom)

    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={
        "paths": [str(source / "IMG_0001.jpg")],
    })
    events = parse_sse_events(resp.data)
    done = [e for e in events if e.get("done")]
    assert done[0]["duplicate_count"] == 1


def test_check_duplicates_verify_by_hash_flag(app_and_db, tmp_path):
    """verify_by_hash=true restores exact content matching — a renamed
    duplicate the heuristic treats as new is flagged."""
    from datetime import datetime

    from PIL.ExifTags import Base as ExifBase

    app, db, fid = app_and_db
    from scanner import compute_file_hash

    source = tmp_path / "source"
    source.mkdir()
    img = Image.new("RGB", (50, 50), color="red")
    exif = img.getexif()
    exif[ExifBase.DateTimeOriginal] = "2026:05:01 10:15:30"
    img.save(str(source / "renamed.jpg"), exif=exif)

    db.add_photo(
        folder_id=fid,
        filename="IMG_0001.jpg",
        extension=".jpg",
        file_size=os.path.getsize(str(source / "renamed.jpg")),
        file_mtime=1.0,
        timestamp=datetime(2026, 5, 1, 10, 15, 30).isoformat(),
        file_hash=compute_file_hash(str(source / "renamed.jpg")),
    )

    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={
        "paths": [str(source / "renamed.jpg")],
    })
    done = [e for e in parse_sse_events(resp.data) if e.get("done")]
    assert done[0]["duplicate_count"] == 0  # heuristic: filename mismatch

    resp = client.post("/api/import/check-duplicates", json={
        "paths": [str(source / "renamed.jpg")],
        "verify_by_hash": True,
    })
    done = [e for e in parse_sse_events(resp.data) if e.get("done")]
    assert done[0]["duplicate_count"] == 1


def test_check_duplicates_no_paths(app_and_db):
    """Returns error when no paths provided."""
    app, _, _ = app_and_db
    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={"paths": []})
    assert resp.status_code == 400


def test_check_duplicates_all_new(app_and_db, tmp_path):
    """When no files match DB hashes, duplicate_count is 0."""
    app, db, fid = app_and_db

    source = tmp_path / "source"
    source.mkdir()
    Image.new("RGB", (50, 50), color="green").save(str(source / "new1.jpg"))
    Image.new("RGB", (50, 50), color="yellow").save(str(source / "new2.jpg"))

    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={
        "paths": [str(source / "new1.jpg"), str(source / "new2.jpg")],
    })

    events = parse_sse_events(resp.data)
    done = [e for e in events if e.get("done")]
    assert len(done) == 1
    assert done[0]["duplicate_count"] == 0


def test_check_duplicates_progress_events_are_monotonic(
    app_and_db, tmp_path
):
    """Progress events report a strictly increasing ``checked`` and the
    correct ``total`` on every event, and the final progress event lands on
    ``checked == total`` before ``done`` (so the client's percentage never
    regresses and always finishes at 100%)."""
    app, _db, _fid = app_and_db

    source = tmp_path / "source"
    source.mkdir()
    paths = []
    for index, color in enumerate(("red", "green", "blue"), 1):
        path = source / f"new{index}.jpg"
        Image.new("RGB", (50, 50), color=color).save(str(path))
        paths.append(str(path))

    resp = app.test_client().post(
        "/api/import/check-duplicates", json={"paths": paths}
    )
    events = parse_sse_events(resp.data)
    # Prep-phase heartbeats carry ``preparing``/``total`` but no
    # ``checked``; the per-file progress events are the ones this
    # monotonicity contract is about, so filter to those.
    progress = [
        event for event in events
        if not event.get("done") and "checked" in event
    ]

    assert progress, "at least one progress event must precede done"
    checked_values = [event["checked"] for event in progress]
    assert checked_values == sorted(set(checked_values)), (
        "checked must strictly increase across progress events"
    )
    assert all(event["total"] == 3 for event in progress)
    assert progress[-1]["checked"] == 3, (
        "the final progress event before done must always land on total"
    )


def test_check_duplicates_slow_check_yields_per_file(
    app_and_db, tmp_path, monkeypatch
):
    """When a per-file check exceeds the flush window, each file gets its
    own event so an aborted preview stops within one file's worth of work
    (rather than draining a fixed batch first)."""
    import web.imports as vireo_app

    app, _db, _fid = app_and_db

    source = tmp_path / "source"
    source.mkdir()
    paths = []
    for index, color in enumerate(("red", "green", "blue"), 1):
        path = source / f"new{index}.jpg"
        Image.new("RGB", (50, 50), color=color).save(str(path))
        paths.append(str(path))

    # Zero flush window → every completed file crosses the threshold and
    # gets its own event, exercising the bounded-cancellation guarantee the
    # SSE loop was designed for.
    monkeypatch.setattr(
        vireo_app, "DUPLICATE_CHECK_FLUSH_INTERVAL_SECONDS", 0.0
    )

    resp = app.test_client().post(
        "/api/import/check-duplicates", json={"paths": paths}
    )
    progress = [
        event for event in parse_sse_events(resp.data)
        if not event.get("done") and "checked" in event
    ]

    assert [event["checked"] for event in progress] == [1, 2, 3]
    assert all(event["total"] == 3 for event in progress)


def test_check_duplicates_prep_phase_yields_between_batches(
    app_and_db, tmp_path, monkeypatch
):
    """Metadata prep runs in bounded batches with a heartbeat between
    each. A single upfront prepare() would leave the WSGI layer unable
    to notice a superseded browser request during the (potentially long)
    EXIF-batching phase — chunking with a yield puts a cancellation
    point every prep-batch worth of I/O."""
    import web.imports as vireo_app

    app, _db, _fid = app_and_db

    source = tmp_path / "source"
    source.mkdir()
    paths = []
    for index in range(5):
        path = source / f"card{index}.jpg"
        Image.new("RGB", (50, 50), color="red").save(str(path))
        paths.append(str(path))

    # Force one path per prep chunk so the batching contract is visible
    # even with a tiny test fixture.
    monkeypatch.setattr(vireo_app, "DUPLICATE_CHECK_PREP_BATCH_SIZE", 1)

    resp = app.test_client().post(
        "/api/import/check-duplicates", json={"paths": paths}
    )
    events = parse_sse_events(resp.data)
    preparing = [
        event["preparing"] for event in events if "preparing" in event
    ]
    assert preparing == [1, 2, 3, 4, 5], (
        f"expected one prep heartbeat per batch reaching total; got "
        f"{preparing}"
    )
    # The prep events precede every per-file event — the checker only
    # walks paths after all of prep is done, and the per-file loop is
    # what emits ``checked``.
    order = [
        "preparing" if "preparing" in event
        else ("checked" if "checked" in event and not event.get("done")
              else "done" if event.get("done") else "other")
        for event in events
    ]
    last_prep = max(i for i, tag in enumerate(order) if tag == "preparing")
    first_checked = next(
        (i for i, tag in enumerate(order) if tag == "checked"), None
    )
    if first_checked is not None:
        assert last_prep < first_checked, (
            "prep heartbeats must precede per-file checked events"
        )


def test_check_duplicates_ignores_zero_byte_images(app_and_db, tmp_path):
    """Empty image placeholders should not be reported as duplicate photos."""
    app, db, fid = app_and_db
    from scanner import EMPTY_FILE_SHA256

    # Historical DB state: older scans could store the empty-file hash.
    db.add_photo(
        folder_id=fid,
        filename="empty.NEF",
        extension=".nef",
        file_size=0,
        file_mtime=1.0,
        file_hash=EMPTY_FILE_SHA256,
    )

    source = tmp_path / "source"
    source.mkdir()
    (source / "DSC_0001.NEF").write_bytes(b"")
    (source / "DSC_0002.NEF").write_bytes(b"")

    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={
        "paths": [str(source / "DSC_0001.NEF"), str(source / "DSC_0002.NEF")],
    })

    events = parse_sse_events(resp.data)
    done = [e for e in events if e.get("done")]
    assert len(done) == 1
    assert done[0]["duplicate_count"] == 0


def test_check_duplicates_missing_file_skipped(app_and_db, tmp_path):
    """Missing files are skipped without crashing."""
    app, db, fid = app_and_db

    source = tmp_path / "source"
    source.mkdir()
    Image.new("RGB", (50, 50), color="green").save(str(source / "real.jpg"))

    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={
        "paths": [str(source / "real.jpg"), str(source / "gone.jpg")],
    })
    assert resp.status_code == 200

    events = parse_sse_events(resp.data)
    done = [e for e in events if e.get("done")]
    assert len(done) == 1
    assert done[0]["checked"] == 2  # Both counted as checked


def test_check_duplicates_zero_byte_file_does_not_swallow_pending_batch(
    app_and_db, tmp_path
):
    """A zero-byte path at end-of-list must
    not eat already-queued ``batch_duplicates``. The pipeline UI only
    learns about duplicates from emitted ``data.duplicates`` events; if
    the end-of-list yield is skipped, ``duplicate_count`` reports the
    duplicate but the UI never deselects it.
    """
    app, db, fid = app_and_db

    library_dir = tmp_path / "library"
    library_dir.mkdir(exist_ok=True)
    img = Image.new("RGB", (50, 50), color="red")
    img.save(str(library_dir / "existing.jpg"))

    from scanner import scan
    scan(str(library_dir), db)

    source = tmp_path / "source"
    source.mkdir()
    img.save(str(source / "duplicate.jpg"))  # Will match the library hash.
    (source / "empty.NEF").write_bytes(b"")

    client = app.test_client()
    # Order matters: empty file is LAST, so the only opportunity to emit
    # the queued duplicate is the ``checked == total`` branch.
    resp = client.post("/api/import/check-duplicates", json={
        "paths": [str(source / "duplicate.jpg"), str(source / "empty.NEF")],
    })

    events = parse_sse_events(resp.data)
    done = [e for e in events if e.get("done")]
    assert len(done) == 1
    assert done[0]["duplicate_count"] == 1

    all_duplicates = []
    for e in events:
        if "duplicates" in e:
            all_duplicates.extend(e["duplicates"])
    assert str(source / "duplicate.jpg") in all_duplicates, (
        "Zero-byte trailing path must not skip the final batch emit; "
        "the duplicate.jpg path needs to surface in a data.duplicates "
        "event so the import UI can deselect it."
    )


# --------------------------------------------------------------------------
# Destination recovery awareness. A cancelled/crashed run leaves files at
# the planned destination that are not yet cataloged; the import run adopts
# them via crash recovery (verify + catalog, no re-copy). The preview must
# say so instead of counting them "to copy" — a catalog-only check reads
# "5,523 to copy / 0 duplicates" while 9 files already sit on the NAS.
# --------------------------------------------------------------------------

def _make_dated_source(tmp_path, name="IMG_0100.jpg", color="red"):
    """A source file whose mtime (no EXIF) plans it into 2026/2026-07-03."""
    from datetime import datetime

    source = tmp_path / "source"
    source.mkdir(exist_ok=True)
    path = source / name
    Image.new("RGB", (50, 50), color=color).save(str(path))
    ts = datetime(2026, 7, 3, 10, 0, 0).timestamp()
    os.utime(str(path), (ts, ts))
    return path


def _recovered_paths(events):
    out = []
    for e in events:
        out.extend(e.get("recovered") or [])
    return out


def test_check_duplicates_reports_files_already_at_destination(
    app_and_db, tmp_path
):
    """Same name + same size at the planned destination folder → streamed
    as ``recovered``, not counted as duplicate or left in "to copy"."""
    app, db, fid = app_and_db

    src = _make_dated_source(tmp_path)
    dest = tmp_path / "archive"
    planned = dest / "2026" / "2026-07-03"
    planned.mkdir(parents=True)
    import shutil
    shutil.copy2(str(src), str(planned / src.name))

    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={
        "paths": [str(src)],
        "destination": str(dest),
        "folder_template": "%Y/%Y-%m-%d",
    })
    assert resp.status_code == 200

    events = parse_sse_events(resp.data)
    done = [e for e in events if e.get("done")]
    assert len(done) == 1
    assert done[0]["duplicate_count"] == 0
    assert done[0]["recovered_count"] == 1
    assert str(src) in _recovered_paths(events)


def test_check_duplicates_recovery_requires_size_match(app_and_db, tmp_path):
    """A same-named file with different bytes at the destination is NOT a
    recovery candidate — the run would suffix-copy the source instead."""
    app, db, fid = app_and_db

    src = _make_dated_source(tmp_path)
    dest = tmp_path / "archive"
    planned = dest / "2026" / "2026-07-03"
    planned.mkdir(parents=True)
    (planned / src.name).write_bytes(b"different bytes entirely")

    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={
        "paths": [str(src)],
        "destination": str(dest),
    })
    events = parse_sse_events(resp.data)
    done = [e for e in events if e.get("done")]
    assert done[0]["recovered_count"] == 0
    assert _recovered_paths(events) == []


def test_check_duplicates_catalog_duplicate_wins_over_recovery(
    app_and_db, tmp_path
):
    """A file that is both cataloged (library twin) and present at the
    planned destination counts as a duplicate — mirroring the run, whose
    duplicate gate runs before the crash-recovery adopt."""
    app, db, fid = app_and_db

    library_dir = tmp_path / "library"
    library_dir.mkdir(exist_ok=True)
    src = _make_dated_source(tmp_path)
    import shutil
    shutil.copy2(str(src), str(library_dir / src.name))
    from scanner import scan
    scan(str(library_dir), db)

    dest = tmp_path / "archive"
    planned = dest / "2026" / "2026-07-03"
    planned.mkdir(parents=True)
    shutil.copy2(str(src), str(planned / src.name))

    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={
        "paths": [str(src)],
        "destination": str(dest),
    })
    events = parse_sse_events(resp.data)
    done = [e for e in events if e.get("done")]
    assert done[0]["duplicate_count"] == 1
    assert done[0]["recovered_count"] == 0
    assert _recovered_paths(events) == []


def test_check_duplicates_no_destination_reports_no_recovery(
    app_and_db, tmp_path
):
    """Without a destination in the request the endpoint behaves exactly
    as before — zero recovered, no errors."""
    app, db, fid = app_and_db

    src = _make_dated_source(tmp_path)
    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={
        "paths": [str(src)],
    })
    assert resp.status_code == 200
    events = parse_sse_events(resp.data)
    done = [e for e in events if e.get("done")]
    assert done[0].get("recovered_count", 0) == 0
    assert _recovered_paths(events) == []


def test_check_duplicates_zero_byte_source_is_recovered(
    app_and_db, tmp_path
):
    """A zero-byte source with an empty file already at the planned
    destination name IS a recovery — the preview must say so.

    This flipped in PR 7b. The endpoint used to return False for every
    zero-byte source, reasoning that "the checker gives them no identity
    either" — but that conflates duplicate identity with crash-recovery
    adoption. The import job adopts an empty candidate for an empty
    source (at the primary name on the local path since long before PR
    7b, and at every candidate position on both transports since flip
    A), so the old answer left the preview counting a file as a transfer
    the run would never perform.
    """
    from datetime import datetime

    app, db, fid = app_and_db

    source = tmp_path / "source"
    source.mkdir(exist_ok=True)
    src = source / "DSC_0001.NEF"
    src.write_bytes(b"")
    ts = datetime(2026, 7, 3, 10, 0, 0).timestamp()
    os.utime(str(src), (ts, ts))

    dest = tmp_path / "archive"
    planned = dest / "2026" / "2026-07-03"
    planned.mkdir(parents=True)
    (planned / "DSC_0001.NEF").write_bytes(b"")

    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={
        "paths": [str(src)],
        "destination": str(dest),
    })
    events = parse_sse_events(resp.data)
    done = [e for e in events if e.get("done")]
    assert done[0]["recovered_count"] == 1
    assert str(src) in _recovered_paths(events)


def test_check_duplicates_zero_byte_source_recovered_at_suffix_slot(
    app_and_db, tmp_path
):
    """Same rule one slot along: a non-empty file holds the primary name
    and this source's empty twin sits at the first suffix. Mirrors
    ``test_local_zero_byte_suffix_candidate_adopts_with_checker`` in the
    import-job suite (PR 7b flip A)."""
    from datetime import datetime

    app, db, fid = app_and_db

    source = tmp_path / "source"
    source.mkdir(exist_ok=True)
    src = source / "DSC_0001.NEF"
    src.write_bytes(b"")
    ts = datetime(2026, 7, 3, 10, 0, 0).timestamp()
    os.utime(str(src), (ts, ts))

    dest = tmp_path / "archive"
    planned = dest / "2026" / "2026-07-03"
    planned.mkdir(parents=True)
    (planned / "DSC_0001.NEF").write_bytes(b"not empty")
    (planned / "DSC_0001_1.NEF").write_bytes(b"")

    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={
        "paths": [str(src)],
        "destination": str(dest),
    })
    events = parse_sse_events(resp.data)
    done = [e for e in events if e.get("done")]
    assert done[0]["recovered_count"] == 1
    assert str(src) in _recovered_paths(events)


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="os.mkfifo is POSIX-only.",
)
def test_check_duplicates_zero_byte_source_not_recovered_from_fifo(
    app_and_db, tmp_path
):
    """A FIFO at the planned name is not a recovery, matching the import
    job's S_ISREG guard: it stats as size 0, so without the regular-file
    rule both sides would treat it as this empty source's archive copy.
    The preview gets this right for free — ``_planned_folder_listing``
    only records ``is_file(follow_symlinks=False)`` entries."""
    from datetime import datetime

    app, db, fid = app_and_db

    source = tmp_path / "source"
    source.mkdir(exist_ok=True)
    src = source / "DSC_0001.NEF"
    src.write_bytes(b"")
    ts = datetime(2026, 7, 3, 10, 0, 0).timestamp()
    os.utime(str(src), (ts, ts))

    dest = tmp_path / "archive"
    planned = dest / "2026" / "2026-07-03"
    planned.mkdir(parents=True)
    os.mkfifo(str(planned / "DSC_0001.NEF"))

    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={
        "paths": [str(src)],
        "destination": str(dest),
    })
    events = parse_sse_events(resp.data)
    done = [e for e in events if e.get("done")]
    assert done[0]["recovered_count"] == 0
    assert _recovered_paths(events) == []


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="os.symlink usually requires elevation on Windows.",
)
def test_check_duplicates_symlinked_candidate_is_not_reported_recovered(
    app_and_db, tmp_path
):
    """KNOWN DIVERGENCE, deliberately deferred to PR 8 (the preflight
    de-mirror): a candidate that is a symlink to an off-card regular file
    with this source's bytes IS adopted by the run — pinned by
    ``test_symlinked_offcard_candidate_is_adopted`` in the import-job
    suite — but the preview reports it as still needing transfer.

    ``_planned_folder_listing`` excludes symlinks, so the slot reads as
    free. PR 7b tried following them and reverted: the walk refuses a
    candidate resolving under ANY source root, whereas this endpoint's
    ``_is_source`` can only compare ``samefile`` against the current
    source file, so a symlink to a *different* card file with identical
    bytes was reported as recovered — an OVER-claim promising "already
    safe at the destination" for bytes that live only on the card. This
    endpoint receives ``paths``, never the import's source roots, so it
    cannot rebuild that guard; doing it right needs the shared walk.

    So the preview under-reports here, which is the safe direction, and
    this test pins that until PR 8. Codex review of #1450, rounds 4-5.
    """
    from datetime import datetime

    app, db, fid = app_and_db

    source = tmp_path / "source"
    source.mkdir(exist_ok=True)
    src = source / "DSC_0001.NEF"
    src.write_bytes(b"the real bytes")
    ts = datetime(2026, 7, 3, 10, 0, 0).timestamp()
    os.utime(str(src), (ts, ts))

    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    real_target = elsewhere / "landed.NEF"
    real_target.write_bytes(b"the real bytes")

    dest = tmp_path / "archive"
    planned = dest / "2026" / "2026-07-03"
    planned.mkdir(parents=True)
    os.symlink(str(real_target), str(planned / "DSC_0001.NEF"))

    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={
        "paths": [str(src)],
        "destination": str(dest),
    })
    events = parse_sse_events(resp.data)
    done = [e for e in events if e.get("done")]
    assert done[0]["recovered_count"] == 0
    assert _recovered_paths(events) == []


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="os.symlink usually requires elevation on Windows.",
)
def test_check_duplicates_never_reports_recovery_via_a_card_symlink(
    app_and_db, tmp_path
):
    """THE SAFETY DIRECTION, and the reason the symlink exclusion stays.

    A candidate symlinked to a DIFFERENT file on the card with bytes
    identical to this source must never be reported as recovered: the
    bytes live only on the card, and claiming "already at the
    destination" is the over-report that could talk a user into wiping
    it. The import walk refuses this geometry via
    ``_is_source_backed_dest``'s under-any-source-root check; this
    endpoint has no equivalent, and excluding symlinks is what keeps it
    honest until PR 8 lets both sides share one walk.
    """
    from datetime import datetime

    app, db, fid = app_and_db

    card = tmp_path / "card"
    card.mkdir(exist_ok=True)
    src = card / "DSC_0001.NEF"
    src.write_bytes(b"the real bytes")
    # A DIFFERENT card file with identical bytes — samefile against the
    # current source is False, so only a source-root check catches it.
    sibling = card / "DSC_0002.NEF"
    sibling.write_bytes(b"the real bytes")
    ts = datetime(2026, 7, 3, 10, 0, 0).timestamp()
    for p in (src, sibling):
        os.utime(str(p), (ts, ts))

    dest = tmp_path / "archive"
    planned = dest / "2026" / "2026-07-03"
    planned.mkdir(parents=True)
    os.symlink(str(sibling), str(planned / "DSC_0001.NEF"))

    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={
        "paths": [str(src)],
        "destination": str(dest),
    })
    events = parse_sse_events(resp.data)
    done = [e for e in events if e.get("done")]
    assert done[0]["recovered_count"] == 0, (
        "reported recovery for bytes that exist only on the card"
    )
    assert _recovered_paths(events) == []


def test_check_duplicates_rejects_relative_destination(app_and_db, tmp_path):
    app, db, fid = app_and_db
    src = _make_dated_source(tmp_path)
    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={
        "paths": [str(src)],
        "destination": "relative/archive",
    })
    assert resp.status_code == 400


def test_check_duplicates_rejects_unsafe_template(app_and_db, tmp_path):
    app, db, fid = app_and_db
    src = _make_dated_source(tmp_path)
    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={
        "paths": [str(src)],
        "destination": str(tmp_path / "archive"),
        "folder_template": "../escape",
    })
    assert resp.status_code == 400


def test_check_duplicates_recovery_walks_suffix_slots(app_and_db, tmp_path):
    """When the primary name is taken by a different file (crash left a
    colliding placeholder), the run walks ``name_1.ext``/``name_2.ext``
    and adopts the first byte-identical suffix. Preview must mirror that
    walk with size-as-proxy — otherwise a collision-retry reports files
    as "to copy" even though Start hashes and adopts them without
    transfer."""
    app, db, fid = app_and_db

    src = _make_dated_source(tmp_path)
    dest = tmp_path / "archive"
    planned = dest / "2026" / "2026-07-03"
    planned.mkdir(parents=True)

    # Primary slot is occupied by a same-named but different-sized file
    # (e.g. an unrelated existing archive photo). The interrupted run's
    # previous copy of THIS source landed at IMG_0100_1.jpg — same size
    # as source, byte-identical (size is the preview's stand-in for that).
    (planned / src.name).write_bytes(b"different sized existing photo")
    import shutil
    shutil.copy2(str(src), str(planned / "IMG_0100_1.jpg"))

    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={
        "paths": [str(src)],
        "destination": str(dest),
        "folder_template": "%Y/%Y-%m-%d",
    })
    events = parse_sse_events(resp.data)
    done = [e for e in events if e.get("done")]
    assert len(done) == 1
    assert done[0]["recovered_count"] == 1
    assert str(src) in _recovered_paths(events)


def test_check_duplicates_recovery_stops_at_first_free_suffix(
    app_and_db, tmp_path
):
    """Primary taken by a different-sized file, and every existing suffix
    slot also differs in size — the run would copy the source to the
    first free slot (e.g. ``IMG_0100_2.jpg``), so the preview must NOT
    call this a recovery."""
    app, db, fid = app_and_db

    src = _make_dated_source(tmp_path)
    dest = tmp_path / "archive"
    planned = dest / "2026" / "2026-07-03"
    planned.mkdir(parents=True)

    (planned / src.name).write_bytes(b"different-sized primary")
    # IMG_0100_1.jpg exists but is a different size — the run would hash-
    # skip it and advance to IMG_0100_2.jpg (a free slot), meaning a real
    # copy happens.
    (planned / "IMG_0100_1.jpg").write_bytes(
        b"another different-sized colliding file"
    )

    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={
        "paths": [str(src)],
        "destination": str(dest),
        "folder_template": "%Y/%Y-%m-%d",
    })
    events = parse_sse_events(resp.data)
    done = [e for e in events if e.get("done")]
    assert done[0]["recovered_count"] == 0
    assert _recovered_paths(events) == []


def test_check_duplicates_recovery_skips_size_mismatched_suffixes(
    app_and_db, tmp_path
):
    """The walk must advance past size-mismatched suffix slots — mirroring
    the run's hash-mismatch skip — before adopting a further slot that
    does size-match. A one-slot-only check would miss the adoption."""
    app, db, fid = app_and_db

    src = _make_dated_source(tmp_path)
    dest = tmp_path / "archive"
    planned = dest / "2026" / "2026-07-03"
    planned.mkdir(parents=True)

    (planned / src.name).write_bytes(b"different-sized primary")
    (planned / "IMG_0100_1.jpg").write_bytes(b"another different-sized file")
    import shutil
    # IMG_0100_2.jpg is the adopted-from-crash copy of this source.
    shutil.copy2(str(src), str(planned / "IMG_0100_2.jpg"))

    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={
        "paths": [str(src)],
        "destination": str(dest),
        "folder_template": "%Y/%Y-%m-%d",
    })
    events = parse_sse_events(resp.data)
    done = [e for e in events if e.get("done")]
    assert done[0]["recovered_count"] == 1
    assert str(src) in _recovered_paths(events)


def test_check_duplicates_recovery_rejects_same_size_different_bytes(
    app_and_db, tmp_path
):
    """A destination file with the same name and byte size but different
    contents (e.g. a corrupt partial file, or an unrelated coincidence)
    must NOT be reported as recovered. The import path hashes the
    candidate and suffix-copies on a mismatch, so the preview would
    otherwise subtract the file from ``to copy`` and promise "not
    re-copied" for a file the run will re-copy under a numbered suffix.
    """
    app, db, fid = app_and_db

    src = _make_dated_source(tmp_path)
    dest = tmp_path / "archive"
    planned = dest / "2026" / "2026-07-03"
    planned.mkdir(parents=True)

    # Land a same-name same-size but different-content file at the
    # primary slot. Padded to exactly match the source size.
    src_size = os.path.getsize(str(src))
    (planned / src.name).write_bytes(b"X" * src_size)

    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={
        "paths": [str(src)],
        "destination": str(dest),
        "folder_template": "%Y/%Y-%m-%d",
    })
    events = parse_sse_events(resp.data)
    done = [e for e in events if e.get("done")]
    assert len(done) == 1
    assert done[0]["recovered_count"] == 0
    assert _recovered_paths(events) == []


def test_check_duplicates_recovery_walks_past_same_size_mismatch_to_adopt(
    app_and_db, tmp_path
):
    """Primary slot has same size but different bytes → the run advances
    the walk (via hash mismatch) and adopts a further byte-identical
    suffix. The preview must mirror that walk: advance past same-size-
    different-bytes slots, adopt the first byte-identical candidate.
    """
    app, db, fid = app_and_db

    src = _make_dated_source(tmp_path)
    dest = tmp_path / "archive"
    planned = dest / "2026" / "2026-07-03"
    planned.mkdir(parents=True)

    # Primary slot: same size, different bytes (must not be adopted).
    src_size = os.path.getsize(str(src))
    (planned / src.name).write_bytes(b"Y" * src_size)
    # IMG_0100_1.jpg: byte-identical to source (the run's actual adopt).
    import shutil
    shutil.copy2(str(src), str(planned / "IMG_0100_1.jpg"))

    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={
        "paths": [str(src)],
        "destination": str(dest),
        "folder_template": "%Y/%Y-%m-%d",
    })
    events = parse_sse_events(resp.data)
    done = [e for e in events if e.get("done")]
    assert done[0]["recovered_count"] == 1
    assert str(src) in _recovered_paths(events)


def test_check_duplicates_recovery_skips_hashing_when_no_collision(
    app_and_db, tmp_path, monkeypatch
):
    """No size collision at the destination → the preview must not hash
    the source. Fresh imports (the common case) stay cheap; hashing is
    scoped to actual collision candidates."""
    app, db, fid = app_and_db

    src = _make_dated_source(tmp_path)
    dest = tmp_path / "archive"
    # Planned folder is empty — no candidate at the primary slot at all.
    (dest / "2026" / "2026-07-03").mkdir(parents=True)

    import scanner
    hash_calls = []
    real_hash = scanner.compute_file_hash

    def _counting_hash(path, *a, **kw):
        hash_calls.append(path)
        return real_hash(path, *a, **kw)

    monkeypatch.setattr(scanner, "compute_file_hash", _counting_hash)

    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={
        "paths": [str(src)],
        "destination": str(dest),
        "folder_template": "%Y/%Y-%m-%d",
    })
    events = parse_sse_events(resp.data)
    done = [e for e in events if e.get("done")]
    assert done[0]["recovered_count"] == 0
    assert hash_calls == [], (
        f"expected no content hashing without a size collision, got "
        f"{len(hash_calls)} hash call(s): {hash_calls}"
    )


def test_check_duplicates_recovery_batches_exif_reads_in_verify_mode(
    app_and_db, tmp_path, monkeypatch
):
    """verify_by_hash makes checker.prepare() a no-op, so the recovery
    check must batch its own capture-time resolution — one call for the
    whole request, not one lazy ExifTool spawn per file."""
    app, db, fid = app_and_db

    srcs = [
        _make_dated_source(tmp_path, name=f"IMG_010{i}.jpg", color=c)
        for i, c in enumerate(["red", "green", "blue"])
    ]
    dest = tmp_path / "archive"
    planned = dest / "2026" / "2026-07-03"
    planned.mkdir(parents=True)
    import shutil
    shutil.copy2(str(srcs[0]), str(planned / srcs[0].name))

    import import_dedup
    real = import_dedup.source_capture_timestamps
    calls = []

    def _counting(files, *a, **kw):
        calls.append(list(files))
        return real(files, *a, **kw)

    monkeypatch.setattr(
        import_dedup, "source_capture_timestamps", _counting)

    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={
        "paths": [str(s) for s in srcs],
        "verify_by_hash": True,
        "destination": str(dest),
    })
    events = parse_sse_events(resp.data)
    done = [e for e in events if e.get("done")]
    assert done[0]["recovered_count"] == 1
    assert str(srcs[0]) in _recovered_paths(events)
    assert len(calls) == 1, (
        f"expected one batched capture-time resolution, got {len(calls)}: "
        f"{[len(c) for c in calls]}"
    )


# --------------------------------------------------------------------------
# skip_duplicates=False mirrors the import job's dedup-off mode:
# library-dedup checker isn't consulted, but crash-recovery adoption of
# byte-identical files at the destination still fires. The retry preview
# after a cancelled dedup-off run has to reflect that, or "to copy" would
# double-count the files a resumed run will adopt without transferring.
# --------------------------------------------------------------------------

def test_check_duplicates_skip_false_suppresses_duplicate_verdicts(
    app_and_db, tmp_path
):
    """With ``skip_duplicates: false`` no cataloged file is reported as a
    duplicate — matching the import run's dedup-off mode, which doesn't
    consult the library checker at all."""
    app, db, fid = app_and_db

    library_dir = tmp_path / "library"
    library_dir.mkdir(exist_ok=True)
    src = _make_dated_source(tmp_path)
    import shutil
    shutil.copy2(str(src), str(library_dir / src.name))
    from scanner import scan
    scan(str(library_dir), db)

    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={
        "paths": [str(src)],
        "skip_duplicates": False,
    })
    assert resp.status_code == 200
    events = parse_sse_events(resp.data)
    done = [e for e in events if e.get("done")]
    assert len(done) == 1
    assert done[0]["duplicate_count"] == 0
    # No ``duplicates`` batches at all — nothing to subtract from "to copy".
    for e in events:
        assert not e.get("duplicates")


def test_check_duplicates_skip_false_still_reports_recovery(
    app_and_db, tmp_path
):
    """With ``skip_duplicates: false`` the recovery gate still fires:
    a byte-identical file at the planned destination is streamed as
    ``recovered`` so the dedup-off retry preview subtracts it from the
    transfer count, matching the run's unconditional crash-recovery."""
    app, db, fid = app_and_db

    src = _make_dated_source(tmp_path)
    dest = tmp_path / "archive"
    planned = dest / "2026" / "2026-07-03"
    planned.mkdir(parents=True)
    import shutil
    shutil.copy2(str(src), str(planned / src.name))

    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={
        "paths": [str(src)],
        "skip_duplicates": False,
        "destination": str(dest),
        "folder_template": "%Y/%Y-%m-%d",
    })
    events = parse_sse_events(resp.data)
    done = [e for e in events if e.get("done")]
    assert done[0]["duplicate_count"] == 0
    assert done[0]["recovered_count"] == 1
    assert str(src) in _recovered_paths(events)


def test_check_duplicates_skip_false_reports_cataloged_twin_as_recovered(
    app_and_db, tmp_path
):
    """A file that is BOTH a library twin AND already at the destination:
    with ``skip_duplicates: false`` it must be streamed as ``recovered``
    (matching what the dedup-off run does — it doesn't consult the
    checker, then crash-recovery adopts the on-disk copy). Without this
    the preview would emit no verdict for it and it would stay counted in
    "to copy", overstating the transfer after a cancelled dedup-off run
    of a batch that happened to contain a library duplicate."""
    app, db, fid = app_and_db

    library_dir = tmp_path / "library"
    library_dir.mkdir(exist_ok=True)
    src = _make_dated_source(tmp_path)
    import shutil
    shutil.copy2(str(src), str(library_dir / src.name))
    from scanner import scan
    scan(str(library_dir), db)

    dest = tmp_path / "archive"
    planned = dest / "2026" / "2026-07-03"
    planned.mkdir(parents=True)
    shutil.copy2(str(src), str(planned / src.name))

    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={
        "paths": [str(src)],
        "skip_duplicates": False,
        "destination": str(dest),
    })
    events = parse_sse_events(resp.data)
    done = [e for e in events if e.get("done")]
    assert done[0]["duplicate_count"] == 0
    assert done[0]["recovered_count"] == 1
    assert str(src) in _recovered_paths(events)


def test_check_duplicates_skip_true_by_default_preserves_contract(
    app_and_db, tmp_path
):
    """Omitting ``skip_duplicates`` defaults to True — the pre-existing
    endpoint contract. A cataloged twin at the destination stays reported
    as a duplicate (duplicate gate wins over recovery), just like before
    the flag was introduced."""
    app, db, fid = app_and_db

    library_dir = tmp_path / "library"
    library_dir.mkdir(exist_ok=True)
    src = _make_dated_source(tmp_path)
    import shutil
    shutil.copy2(str(src), str(library_dir / src.name))
    from scanner import scan
    scan(str(library_dir), db)

    dest = tmp_path / "archive"
    planned = dest / "2026" / "2026-07-03"
    planned.mkdir(parents=True)
    shutil.copy2(str(src), str(planned / src.name))

    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={
        "paths": [str(src)],
        "destination": str(dest),
    })
    events = parse_sse_events(resp.data)
    done = [e for e in events if e.get("done")]
    assert done[0]["duplicate_count"] == 1
    assert done[0]["recovered_count"] == 0


def test_check_duplicates_recovery_skipped_when_candidate_is_source(
    app_and_db, tmp_path
):
    """The destination-folder template renders the planned destination
    back onto the source folder (e.g. importing
    /archive/2026/2026-07-03/IMG.jpg with destination /archive and
    template %Y/%Y-%m-%d), so the primary candidate IS the source file
    itself. The run rejects that self-copy overlap and fails the file;
    the preview must NOT promise "verified & adopted, not re-copied"
    and subtract it from "to copy". Mirrors import_job's samefile guard.
    """
    from datetime import datetime

    # Source lives INSIDE what will become the planned destination
    # folder — archive/2026/2026-07-03 renders back onto itself.
    archive = tmp_path / "archive"
    src_folder = archive / "2026" / "2026-07-03"
    src_folder.mkdir(parents=True)
    src = src_folder / "IMG_0100.jpg"
    Image.new("RGB", (50, 50), color="red").save(str(src))
    ts = datetime(2026, 7, 3, 10, 0, 0).timestamp()
    os.utime(str(src), (ts, ts))

    app, db, fid = app_and_db
    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={
        "paths": [str(src)],
        "destination": str(archive),
        "folder_template": "%Y/%Y-%m-%d",
    })
    assert resp.status_code == 200
    events = parse_sse_events(resp.data)
    done = [e for e in events if e.get("done")]
    assert done[0]["recovered_count"] == 0
    assert _recovered_paths(events) == []


def test_check_duplicates_recovery_skipped_for_source_via_symlink(
    app_and_db, tmp_path
):
    """Source path and planned-destination candidate resolve to the
    same inode via a symlinked source directory. os.path.samefile
    catches it even when the literal paths differ, matching
    import_job's guard: the run fails the file, so the preview must
    report not-recovered."""
    from datetime import datetime

    archive = tmp_path / "archive"
    real_folder = archive / "2026" / "2026-07-03"
    real_folder.mkdir(parents=True)
    real_src = real_folder / "IMG_0200.jpg"
    Image.new("RGB", (50, 50), color="blue").save(str(real_src))
    ts = datetime(2026, 7, 3, 10, 0, 0).timestamp()
    os.utime(str(real_src), (ts, ts))

    # A symlink to the real source folder. Passing the file via the
    # symlink path makes the literal source string differ from the
    # planned destination path, but both stat to the same inode.
    link_folder = tmp_path / "card_link"
    if not hasattr(os, "symlink"):
        pytest.skip("symlinks unavailable")
    try:
        os.symlink(str(real_folder), str(link_folder))
    except (OSError, NotImplementedError):
        pytest.skip("symlink creation not permitted")
    src_via_link = link_folder / "IMG_0200.jpg"

    app, db, fid = app_and_db
    client = app.test_client()
    resp = client.post("/api/import/check-duplicates", json={
        "paths": [str(src_via_link)],
        "destination": str(archive),
        "folder_template": "%Y/%Y-%m-%d",
    })
    assert resp.status_code == 200
    events = parse_sse_events(resp.data)
    done = [e for e in events if e.get("done")]
    assert done[0]["recovered_count"] == 0
    assert _recovered_paths(events) == []
