import os
from types import SimpleNamespace


def _seed_working_copies(tmp_path, sizes):
    from db import Database

    db = Database(str(tmp_path / "vireo.db"))
    workspace_id = db.ensure_default_workspace()
    db.set_active_workspace(workspace_id)
    folder_id = db.add_folder(str(tmp_path / "photos"), name="photos")
    working_dir = tmp_path / "working"
    working_dir.mkdir()

    photo_ids = []
    for index, size in enumerate(sizes, 1):
        source_mtime = float(index * 10)
        photo_id = db.add_photo(
            folder_id=folder_id,
            filename=f"photo-{index}.NEF",
            extension=".nef",
            file_size=10_000,
            file_mtime=source_mtime,
            width=6000,
            height=4000,
        )
        path = working_dir / f"{photo_id}.jpg"
        path.write_bytes(b"x" * size)
        os.utime(path, (index * 100, index * 100))
        db.conn.execute(
            "UPDATE photos SET working_copy_path=? WHERE id=?",
            (f"working/{photo_id}.jpg", photo_id),
        )
        photo_ids.append(photo_id)
    db.conn.commit()
    return db, working_dir, photo_ids


def test_windows_identity_ignores_unstable_device_and_inode(monkeypatch):
    import working_copy_cache

    monkeypatch.setattr(working_copy_cache.os, "name", "nt")
    sampled = SimpleNamespace(
        st_dev=0, st_ino=0, st_size=123, st_mtime_ns=456, st_ctime_ns=789,
    )
    rechecked = SimpleNamespace(
        st_dev=7, st_ino=99, st_size=123, st_mtime_ns=456, st_ctime_ns=789,
    )

    assert working_copy_cache._file_identity(sampled) == (
        working_copy_cache._file_identity(rechecked)
    )


def test_evicts_oldest_working_copies_and_marks_catalog_rows(tmp_path):
    from scanner import working_copy_backfill_candidate_count
    from working_copy_cache import evict_if_over_quota

    db, working_dir, photo_ids = _seed_working_copies(
        tmp_path, [600_000, 600_000, 600_000],
    )
    try:
        result = evict_if_over_quota(db, str(tmp_path), quota_mb=1)

        assert result["evicted"] == 2
        assert result["freed_bytes"] == 1_200_000
        assert not (working_dir / f"{photo_ids[0]}.jpg").exists()
        assert not (working_dir / f"{photo_ids[1]}.jpg").exists()
        assert (working_dir / f"{photo_ids[2]}.jpg").exists()

        rows = db.conn.execute(
            "SELECT id, file_mtime, working_copy_path, "
            "working_copy_evicted_mtime FROM photos ORDER BY id"
        ).fetchall()
        assert rows[0]["working_copy_path"] is None
        assert rows[0]["working_copy_evicted_mtime"] == rows[0]["file_mtime"]
        assert rows[1]["working_copy_path"] is None
        assert rows[1]["working_copy_evicted_mtime"] == rows[1]["file_mtime"]
        assert rows[2]["working_copy_path"] == f"working/{photo_ids[2]}.jpg"
        assert rows[2]["working_copy_evicted_mtime"] is None

        # Quota removals are deliberate cache misses, not startup-backfill
        # candidates. A changed source mtime makes the marker stale and lets
        # the working copy regenerate.
        assert working_copy_backfill_candidate_count(db) == 0
        db.conn.execute(
            "UPDATE photos SET file_mtime=file_mtime + 1 WHERE id=?",
            (photo_ids[0],),
        )
        db.conn.commit()
        assert working_copy_backfill_candidate_count(db) == 1
    finally:
        db.close()


def test_unlink_failure_keeps_working_copy_accounted(
    tmp_path, monkeypatch,
):
    import working_copy_cache

    db, working_dir, photo_ids = _seed_working_copies(
        tmp_path, [700_000, 700_000],
    )
    oldest = working_dir / f"{photo_ids[0]}.jpg"
    real_remove = working_copy_cache.os.remove

    def fail_oldest(path):
        if path == str(oldest):
            raise PermissionError("locked")
        return real_remove(path)

    monkeypatch.setattr(working_copy_cache.os, "remove", fail_oldest)
    try:
        result = working_copy_cache.evict_if_over_quota(
            db, str(tmp_path), quota_mb=1,
        )

        assert result["evicted"] == 1
        assert oldest.exists()
        assert not (working_dir / f"{photo_ids[1]}.jpg").exists()
        rows = db.conn.execute(
            "SELECT id, working_copy_path FROM photos ORDER BY id"
        ).fetchall()
        assert rows[0]["working_copy_path"] == f"working/{photo_ids[0]}.jpg"
        assert rows[1]["working_copy_path"] is None
        assert result["remaining_bytes"] == 700_000
    finally:
        db.close()


def test_working_copy_stats_include_all_direct_files(tmp_path):
    from working_copy_cache import working_copy_stats

    working_dir = tmp_path / "working"
    working_dir.mkdir()
    (working_dir / "1.jpg").write_bytes(b"123")
    (working_dir / "leftover.tmp").write_bytes(b"4567")
    (working_dir / "nested").mkdir()
    (working_dir / "nested" / "ignored.jpg").write_bytes(b"ignored")

    assert working_copy_stats(str(tmp_path), quota_mb=2) == {
        "count": 2,
        "size": 7,
        "path": str(working_dir),
        "quota_bytes": 2 * 1024 * 1024,
    }


def test_working_copy_stats_skip_private_render_tempfiles(tmp_path):
    """A killed on-demand extractor leaves ``.render.*.jpg.tmp`` behind.

    Eviction has no catalog row to key off those names, so counting them
    toward quota usage would permanently inflate reported/enforced totals.
    """
    from working_copy_cache import working_copy_stats

    working_dir = tmp_path / "working"
    working_dir.mkdir()
    (working_dir / "1.jpg").write_bytes(b"real")
    (working_dir / ".1.render.abcdef.jpg.tmp").write_bytes(b"orphaned tempfile")

    assert working_copy_stats(str(tmp_path), quota_mb=1) == {
        "count": 1,
        "size": 4,
        "path": str(working_dir),
        "quota_bytes": 1 * 1024 * 1024,
    }


def test_evict_ignores_private_render_tempfiles(tmp_path):
    from working_copy_cache import evict_if_over_quota

    db, working_dir, photo_ids = _seed_working_copies(tmp_path, [500_000])
    # Simulate a leftover on-demand extractor tempfile. If quota accounting
    # counted this toward ``total``, the eviction pass would delete the
    # legitimate working copy while the orphan lingered.
    orphan = working_dir / f".{photo_ids[0]}.render.deadbeef.jpg.tmp"
    orphan.write_bytes(b"x" * 900_000)
    try:
        result = evict_if_over_quota(db, str(tmp_path), quota_mb=1)

        assert result["evicted"] == 0
        assert (working_dir / f"{photo_ids[0]}.jpg").exists()
        # The orphan itself is left in place — eviction only touches files
        # backed by catalog rows — but it does not force real working copies
        # to be discarded to stay under quota.
        assert orphan.exists()
    finally:
        db.close()


def test_evict_sweeps_stale_render_tempfiles(tmp_path):
    """Old orphaned render tempfiles are reclaimed so bytes cannot leak.

    Excluding them from accounting protects working copies from being
    evicted for scratch space, but the disk bytes are still real. A
    process kill during on-demand extraction can otherwise accumulate
    tempfiles indefinitely.
    """
    import time

    from working_copy_cache import (
        _RENDER_TEMP_SWEEP_SECONDS,
        evict_if_over_quota,
    )

    db, working_dir, photo_ids = _seed_working_copies(tmp_path, [500_000])
    orphan = working_dir / f".{photo_ids[0]}.render.dead.jpg.tmp"
    orphan.write_bytes(b"x" * 800_000)
    stale_mtime = time.time() - _RENDER_TEMP_SWEEP_SECONDS - 60
    os.utime(orphan, (stale_mtime, stale_mtime))
    try:
        evict_if_over_quota(db, str(tmp_path), quota_mb=1)

        # Old orphan reclaimed; the published working copy is preserved.
        assert not orphan.exists()
        assert (working_dir / f"{photo_ids[0]}.jpg").exists()
    finally:
        db.close()


def test_evict_does_not_clear_concurrently_replaced_working_copy(
    tmp_path, monkeypatch,
):
    """A concurrent on-demand write must not be marked evicted.

    A peer request can regenerate ``working/<id>.jpg`` and commit the same
    ``working_copy_path`` between our ``os.remove`` and our subsequent
    DB UPDATE. Clearing the row then would leave untracked bytes on disk.
    """
    import working_copy_cache

    db, working_dir, photo_ids = _seed_working_copies(tmp_path, [700_000])
    victim_id = photo_ids[0]
    victim_path = working_dir / f"{victim_id}.jpg"
    real_remove = working_copy_cache.os.remove

    def remove_then_repopulate(path):
        real_remove(path)
        if path == str(victim_path):
            # Simulate an on-demand extractor writing a fresh copy after
            # our unlink but before our UPDATE.
            victim_path.write_bytes(b"y" * 500_000)

    monkeypatch.setattr(working_copy_cache.os, "remove", remove_then_repopulate)
    try:
        result = working_copy_cache.evict_if_over_quota(
            db, str(tmp_path), quota_mb=0,
        )

        assert result["evicted"] == 1
        assert victim_path.exists()
        row = db.conn.execute(
            "SELECT working_copy_path FROM photos WHERE id=?", (victim_id,),
        ).fetchone()
        # The row still points at the freshly written replacement, so the
        # bytes are tracked and future eviction can find and remove them.
        assert row["working_copy_path"] == f"working/{victim_id}.jpg"
    finally:
        db.close()


def test_evict_does_not_unlink_replacement_published_after_scan(
    tmp_path, monkeypatch,
):
    """An atomic replacement after sampling is not the selected victim."""
    import working_copy_cache

    db, working_dir, photo_ids = _seed_working_copies(tmp_path, [700_000])
    photo_id = photo_ids[0]
    victim = working_dir / f"{photo_id}.jpg"
    replacement = working_dir / ".replacement.jpg"
    replacement.write_bytes(b"new" * 100_000)
    real_stat = working_copy_cache.os.stat
    replaced = False

    def replace_before_identity_check(path, *args, **kwargs):
        nonlocal replaced
        if os.path.abspath(path) == os.path.abspath(victim) and not replaced:
            replaced = True
            os.replace(replacement, victim)
        return real_stat(path, *args, **kwargs)

    monkeypatch.setattr(
        working_copy_cache.os, "stat", replace_before_identity_check,
    )
    try:
        result = working_copy_cache.evict_if_over_quota(
            db, str(tmp_path), quota_mb=0,
        )

        assert replaced
        assert result["evicted"] == 0
        assert victim.read_bytes().startswith(b"new")
        row = db.conn.execute(
            "SELECT working_copy_path FROM photos WHERE id=?", (photo_id,),
        ).fetchone()
        assert row["working_copy_path"] == f"working/{photo_id}.jpg"
    finally:
        db.close()


def test_evicts_legacy_canonical_file_without_catalog_path(tmp_path):
    from working_copy_cache import evict_if_over_quota

    db, working_dir, photo_ids = _seed_working_copies(tmp_path, [100])
    photo_id = photo_ids[0]
    db.conn.execute(
        "UPDATE photos SET working_copy_path=NULL WHERE id=?", (photo_id,)
    )
    db.conn.commit()
    try:
        result = evict_if_over_quota(db, str(tmp_path), quota_mb=0)

        assert result["evicted"] == 1
        assert result["remaining_bytes"] == 0
        assert not (working_dir / f"{photo_id}.jpg").exists()
        row = db.conn.execute(
            "SELECT working_copy_path, working_copy_evicted_mtime "
            "FROM photos WHERE id=?", (photo_id,),
        ).fetchone()
        assert row["working_copy_path"] is None
        assert row["working_copy_evicted_mtime"] == 10.0
    finally:
        db.close()


def test_recent_untracked_working_copy_gets_writer_grace(tmp_path):
    from working_copy_cache import evict_if_over_quota

    db, working_dir, photo_ids = _seed_working_copies(tmp_path, [100])
    photo_id = photo_ids[0]
    path = working_dir / f"{photo_id}.jpg"
    os.utime(path, None)
    db.conn.execute(
        "UPDATE photos SET working_copy_path=NULL WHERE id=?", (photo_id,)
    )
    db.conn.commit()
    try:
        result = evict_if_over_quota(db, str(tmp_path), quota_mb=0)

        assert result["evicted"] == 0
        assert result["remaining_bytes"] == 100
        assert path.exists()
    finally:
        db.close()


def test_startup_bypasses_writer_grace_for_legacy_files(tmp_path):
    """At startup no cache writer can be running, so a legacy
    ``working/<id>.jpg`` whose mtime happens to fall inside the grace window
    (recent modify time, or a future-dated timestamp copied from an archive)
    must still be reclaimed on the first pass — otherwise the file lingers
    above the ceiling until another restart after the window closes."""
    from working_copy_cache import evict_if_over_quota

    db, working_dir, photo_ids = _seed_working_copies(tmp_path, [100])
    photo_id = photo_ids[0]
    path = working_dir / f"{photo_id}.jpg"
    os.utime(path, None)  # bump mtime into the grace window
    db.conn.execute(
        "UPDATE photos SET working_copy_path=NULL WHERE id=?", (photo_id,)
    )
    db.conn.commit()
    try:
        result = evict_if_over_quota(
            db, str(tmp_path), quota_mb=0, startup=True,
        )

        assert result["evicted"] == 1
        assert result["remaining_bytes"] == 0
        assert not path.exists()
    finally:
        db.close()


def test_sweep_abandoned_render_tempfiles_removes_orphans(tmp_path):
    """A process kill during ``_extract_original_copy`` can leave a
    ``.<id>.render.*.jpg.tmp`` orphan in ``working/`` that quota accounting
    intentionally skips. The startup sweep is its only cleanup path."""
    from working_copy_cache import sweep_abandoned_render_tempfiles

    working_dir = tmp_path / "working"
    working_dir.mkdir()
    canonical = working_dir / "42.jpg"
    canonical.write_bytes(b"canonical bytes")
    orphan_a = working_dir / ".42.render.abcdef.jpg.tmp"
    orphan_a.write_bytes(b"partial write")
    orphan_b = working_dir / ".99.render.deadbeef.jpg.tmp"
    orphan_b.write_bytes(b"another orphan")
    scanner_orphan = working_dir / ".42.jpg.cafebabe.jpg.tmp"
    scanner_orphan.write_bytes(b"scanner partial write")
    nested_orphan = (
        working_dir / "..42.render.outer.jpg.tmp.inner.jpg.tmp"
    )
    nested_orphan.write_bytes(b"nested on-demand partial write")
    unrelated = working_dir / "leftover.tmp"
    unrelated.write_bytes(b"unrelated non-render tempfile")
    nested = working_dir / "nested"
    nested.mkdir()

    sweep_abandoned_render_tempfiles(str(tmp_path))

    assert canonical.exists()
    assert not orphan_a.exists()
    assert not orphan_b.exists()
    assert not scanner_orphan.exists()
    assert not nested_orphan.exists()
    assert unrelated.exists()
    assert nested.exists()


def test_sweep_abandoned_render_tempfiles_missing_dir_is_noop(tmp_path):
    from working_copy_cache import sweep_abandoned_render_tempfiles

    # Should not raise even if ``working/`` was never created.
    sweep_abandoned_render_tempfiles(str(tmp_path))


def test_evict_reclaims_numeric_canonical_file_without_photo_row(tmp_path):
    from db import Database
    from working_copy_cache import evict_if_over_quota

    db = Database(str(tmp_path / "vireo.db"))
    working_dir = tmp_path / "working"
    working_dir.mkdir()
    orphan = working_dir / "999.jpg"
    orphan.write_bytes(b"orphaned working copy")
    try:
        result = evict_if_over_quota(db, str(tmp_path), quota_mb=0)

        assert result["evicted"] == 1
        assert result["remaining_bytes"] == 0
        assert not orphan.exists()
    finally:
        db.close()


def test_evict_does_not_mark_reused_photo_id(tmp_path, monkeypatch):
    """A delete/re-import before the writer lock invalidates the pass."""
    import working_copy_cache
    from db import Database

    db, working_dir, photo_ids = _seed_working_copies(
        tmp_path, [700_000, 700_000],
    )
    reused_id = photo_ids[-1]
    reused_path = working_dir / f"{reused_id}.jpg"
    real_begin = working_copy_cache._begin_stable_eviction_transaction
    lifecycle_db = Database(str(tmp_path / "vireo.db"))
    lifecycle_changed = False

    def replace_photo_before_lock(candidate_db, expected_data_version):
        nonlocal lifecycle_changed
        if not lifecycle_changed:
            lifecycle_changed = True
            old_row = lifecycle_db.conn.execute(
                "SELECT folder_id FROM photos WHERE id=?", (reused_id,),
            ).fetchone()
            lifecycle_db.conn.execute(
                "DELETE FROM photos WHERE id=?", (reused_id,),
            )
            lifecycle_db.conn.execute(
                "INSERT INTO photos "
                "(folder_id, filename, extension, file_size, file_mtime, "
                "width, height) VALUES (?, ?, '.nef', 10000, 999, 6000, "
                "4000)",
                (old_row["folder_id"], "replacement.NEF"),
            )
            assert lifecycle_db.conn.execute(
                "SELECT last_insert_rowid()"
            ).fetchone()[0] == reused_id
            lifecycle_db.conn.commit()
            reused_path.write_bytes(b"new photo bytes")
        return real_begin(candidate_db, expected_data_version)

    monkeypatch.setattr(
        working_copy_cache,
        "_begin_stable_eviction_transaction",
        replace_photo_before_lock,
    )
    try:
        result = working_copy_cache.evict_if_over_quota(
            db, str(tmp_path), quota_mb=1,
        )

        assert lifecycle_changed
        assert result["evicted"] == 0
        assert reused_path.read_bytes() == b"new photo bytes"
        row = db.conn.execute(
            "SELECT filename, working_copy_path, working_copy_evicted_mtime "
            "FROM photos WHERE id=?",
            (reused_id,),
        ).fetchone()
        assert row["filename"] == "replacement.NEF"
        assert row["working_copy_path"] is None
        assert row["working_copy_evicted_mtime"] is None
    finally:
        lifecycle_db.close()
        db.close()


def test_evict_reconciles_tracked_row_when_file_is_missing(tmp_path):
    """A tracked row with no on-disk file — a leftover from a prior eviction
    whose DB commit failed after the unlinks — must be reset to NULL so
    scanner backfill can regenerate it instead of skipping it forever."""
    from scanner import working_copy_backfill_candidate_count
    from working_copy_cache import evict_if_over_quota

    db, working_dir, photo_ids = _seed_working_copies(tmp_path, [500_000])
    try:
        # Row still claims a working copy, but the file has been removed
        # out from under it.
        (working_dir / f"{photo_ids[0]}.jpg").unlink()

        result = evict_if_over_quota(db, str(tmp_path), quota_mb=1)

        assert result["evicted"] == 0
        row = db.conn.execute(
            "SELECT working_copy_path, working_copy_evicted_mtime "
            "FROM photos WHERE id=?",
            (photo_ids[0],),
        ).fetchone()
        assert row["working_copy_path"] is None
        # No eviction marker: this is reconciliation, not a quota decision.
        assert row["working_copy_evicted_mtime"] is None
        # The row now looks like a normal backfill candidate again.
        assert working_copy_backfill_candidate_count(db) == 1
    finally:
        db.close()


def test_reconcile_rechecks_missing_file_before_clearing_row(
    tmp_path, monkeypatch,
):
    """A replacement published after the scan remains tracked."""
    import working_copy_cache

    db, working_dir, photo_ids = _seed_working_copies(tmp_path, [500_000])
    photo_id = photo_ids[0]
    path = working_dir / f"{photo_id}.jpg"
    path.unlink()
    real_exists = working_copy_cache.os.path.exists
    republished = False

    def republish_before_reconcile(candidate):
        nonlocal republished
        if os.path.abspath(candidate) == os.path.abspath(path) and not republished:
            republished = True
            path.write_bytes(b"replacement")
            return True
        return real_exists(candidate)

    monkeypatch.setattr(
        working_copy_cache.os.path, "exists", republish_before_reconcile,
    )
    try:
        result = working_copy_cache.evict_if_over_quota(
            db, str(tmp_path), quota_mb=1,
        )

        assert republished
        assert result["evicted"] == 0
        assert path.is_file()
        row = db.conn.execute(
            "SELECT working_copy_path FROM photos WHERE id=?", (photo_id,),
        ).fetchone()
        assert row["working_copy_path"] == f"working/{photo_id}.jpg"
    finally:
        db.close()


def test_evict_retries_after_snapshot_invalidation(tmp_path, monkeypatch):
    """A concurrent catalog writer that keeps invalidating the pass must not
    silently leave the cache above the new quota — retry with a fresh
    snapshot until it succeeds or the retry cap is hit."""
    import working_copy_cache
    from db import Database

    db, working_dir, photo_ids = _seed_working_copies(
        tmp_path, [700_000, 700_000],
    )
    lifecycle_db = Database(str(tmp_path / "vireo.db"))
    real_begin = working_copy_cache._begin_stable_eviction_transaction
    attempts = {"count": 0}

    def bump_data_version_on_first_call(candidate_db, expected_data_version):
        attempts["count"] += 1
        if attempts["count"] == 1:
            # Simulate an unrelated catalog writer committing between our
            # snapshot and this lock acquisition. The pass must return None
            # to trigger a snapshot retake instead of returning a stale
            # "no eviction needed" payload.
            lifecycle_db.conn.execute(
                "UPDATE photos SET rating = COALESCE(rating, 0) + 1 "
                "WHERE id=?",
                (photo_ids[0],),
            )
            lifecycle_db.conn.commit()
        return real_begin(candidate_db, expected_data_version)

    monkeypatch.setattr(
        working_copy_cache,
        "_begin_stable_eviction_transaction",
        bump_data_version_on_first_call,
    )
    try:
        result = working_copy_cache.evict_if_over_quota(
            db, str(tmp_path), quota_mb=1,
        )
        # Retry succeeded on the second attempt and evicted one file to
        # bring usage under the 1 MB quota.
        assert attempts["count"] >= 2
        assert result.get("deferred") is not True
        assert result["evicted"] == 1
        assert result["remaining_bytes"] <= result["quota_bytes"]
    finally:
        lifecycle_db.close()
        db.close()


def test_evict_reports_deferred_when_retries_are_exhausted(
    tmp_path, monkeypatch,
):
    """When every retry snapshot is invalidated, honestly report deferral so
    the caller can log rather than treating the pass as successful."""
    import working_copy_cache
    from db import Database

    db, working_dir, photo_ids = _seed_working_copies(
        tmp_path, [700_000, 700_000],
    )
    lifecycle_db = Database(str(tmp_path / "vireo.db"))
    real_begin = working_copy_cache._begin_stable_eviction_transaction

    invalidate_counter = {"n": 0}

    def always_invalidate(candidate_db, expected_data_version):
        invalidate_counter["n"] += 1
        lifecycle_db.conn.execute(
            "UPDATE photos SET rating = ? WHERE id=?",
            (invalidate_counter["n"], photo_ids[0]),
        )
        lifecycle_db.conn.commit()
        return real_begin(candidate_db, expected_data_version)

    monkeypatch.setattr(
        working_copy_cache,
        "_begin_stable_eviction_transaction",
        always_invalidate,
    )
    try:
        result = working_copy_cache.evict_if_over_quota(
            db, str(tmp_path), quota_mb=1,
        )
        assert result.get("deferred") is True
        assert result["evicted"] == 0
    finally:
        lifecycle_db.close()
        db.close()


def test_arrange_deferred_over_quota_retry_runs_until_success(
    tmp_path, monkeypatch,
):
    """When ``evict_if_over_quota`` reports ``deferred=True``, the retry
    helper must spawn a bounded background pass and keep going until a
    non-deferred result. Without this the settings handler would return
    while the cache still sat above the new lowered quota — a scanner
    or restart would eventually re-run enforcement, but there's no
    guarantee those fire soon."""
    import working_copy_cache
    from db import Database

    seed = Database(str(tmp_path / "vireo.db"))
    seed.close()

    calls = {"count": 0}
    outcomes = iter([
        {
            "evicted": 0, "freed_bytes": 0, "remaining_bytes": None,
            "quota_bytes": 0, "deferred": True,
        },
        {
            "evicted": 0, "freed_bytes": 0, "remaining_bytes": None,
            "quota_bytes": 0, "deferred": True,
        },
        {
            "evicted": 1, "freed_bytes": 700_000, "remaining_bytes": 300_000,
            "quota_bytes": 1_048_576,
        },
    ])

    def fake_evict(db, vireo_dir, quota_mb=None):
        calls["count"] += 1
        return next(outcomes)

    monkeypatch.setattr(working_copy_cache, "evict_if_over_quota", fake_evict)

    # Reset the coalescing flag so pytest ordering never leaves a stale
    # "pending" state from another test.
    with working_copy_cache._deferred_retry_lock:
        working_copy_cache._deferred_retry_pending = False

    scheduled = working_copy_cache.arrange_deferred_over_quota_retry(
        str(tmp_path / "vireo.db"),
        str(tmp_path),
        _sleep=lambda _delay: None,
        _thread_starter=lambda target: target(),
    )
    assert scheduled is True
    assert calls["count"] == 3
    # After the loop exits, the coalescing flag must clear so a later
    # deferral schedules its own pass instead of getting silently dropped.
    assert working_copy_cache._deferred_retry_pending is False


def test_arrange_deferred_over_quota_retry_coalesces_overlapping_calls(
    tmp_path, monkeypatch,
):
    """Overlapping ``deferred=True`` returns must not each spawn their own
    daemon thread — one bounded background pass is enough because each
    attempt reads current state."""
    import working_copy_cache
    from db import Database

    seed = Database(str(tmp_path / "vireo.db"))
    seed.close()

    # First helper hasn't finished yet: force the pending flag on so any
    # second call must coalesce onto the in-flight pass.
    with working_copy_cache._deferred_retry_lock:
        working_copy_cache._deferred_retry_pending = True
    try:
        scheduled = working_copy_cache.arrange_deferred_over_quota_retry(
            str(tmp_path / "vireo.db"), str(tmp_path),
            _sleep=lambda _delay: None,
            _thread_starter=lambda target: target(),
        )
        assert scheduled is False
    finally:
        with working_copy_cache._deferred_retry_lock:
            working_copy_cache._deferred_retry_pending = False


def test_zero_quota_skips_working_copy_generation(
    tmp_path, monkeypatch,
):
    import config as cfg
    import scanner

    monkeypatch.setattr(cfg, "CONFIG_PATH", str(tmp_path / "config.json"))
    cfg.save({**cfg.DEFAULTS, "working_copy_cache_max_mb": 0})
    db, working_dir, photo_ids = _seed_working_copies(tmp_path, [100])
    path = working_dir / f"{photo_ids[0]}.jpg"
    path.unlink()
    db.conn.execute(
        "UPDATE photos SET working_copy_path=NULL WHERE id=?",
        (photo_ids[0],),
    )
    db.conn.commit()

    monkeypatch.setattr(
        scanner, "extract_working_copy",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("zero quota must skip decoding")
        ),
    )
    try:
        assert scanner.working_copy_backfill_candidate_count(db) == 0
        scanner._extract_working_copies(db, str(tmp_path))
        assert not path.exists()
    finally:
        db.close()


def test_eviction_result_reports_which_files_were_removed(tmp_path):
    """The payload names the reclaimed files, not just a count.

    The scanner's backfill has to distinguish "the quota pass reclaimed
    bytes I just generated" from "it reclaimed coverage that already
    existed" — the second means the cache is rotating rather than growing
    and the batch must stand down. A bare count cannot answer that.
    """
    from working_copy_cache import evict_if_over_quota

    db, working_dir, photo_ids = _seed_working_copies(
        tmp_path, [600_000, 600_000, 600_000],
    )
    try:
        result = evict_if_over_quota(db, str(tmp_path), quota_mb=1)

        assert set(result["evicted_paths"]) == {
            str(working_dir / f"{photo_ids[0]}.jpg"),
            str(working_dir / f"{photo_ids[1]}.jpg"),
        }
    finally:
        db.close()


def test_touch_marks_an_old_working_copy_as_recently_used(tmp_path):
    """A cache hit advances atime so eviction reads it as recently used.

    mtime must stay stable — callers key content-version off it
    (``_external_edit_handoff_path`` regenerates the external-editor
    handoff whenever the WC's mtime changes, which would overwrite
    anything the external editor saved into that file).
    """
    import time

    from working_copy_cache import touch_working_copy_access

    path = tmp_path / "1.jpg"
    path.write_bytes(b"x" * 100)
    stale = time.time() - 30 * 24 * 3600
    os.utime(path, (stale, stale))

    assert touch_working_copy_access(str(path)) is True
    stat_after = path.stat()
    assert stat_after.st_atime > stale
    assert stat_after.st_atime >= time.time() - 60
    assert stat_after.st_mtime == stale, (
        "touch advanced mtime — that key must stay stable so the "
        "external-edit handoff and display-cache freshness gates "
        "don't see spurious content changes"
    )


def test_touch_skips_a_freshly_stamped_working_copy(tmp_path):
    """Throttled: a burst of 1:1 zoom requests must not utime per request."""
    import time

    from working_copy_cache import touch_working_copy_access

    path = tmp_path / "1.jpg"
    path.write_bytes(b"x" * 100)
    recent = time.time() - 60
    os.utime(path, (recent, recent))

    assert touch_working_copy_access(str(path)) is False
    stat_after = path.stat()
    assert stat_after.st_atime == recent
    assert stat_after.st_mtime == recent


def test_touch_never_drags_a_future_stamped_copy_backwards(tmp_path):
    """Forward-only.

    ``/photos/<id>/original`` serves a cached rendition when its mtime is
    ``>=`` its sources'. A copy pegged into the future (clock skew, an
    archive with forward-dated timestamps) is pegged there deliberately;
    stamping it with "now" would fail that gate and re-decode the RAW on
    every request. Both atime AND mtime must stay unchanged.
    """
    import time

    from working_copy_cache import touch_working_copy_access

    path = tmp_path / "1.jpg"
    path.write_bytes(b"x" * 100)
    future = time.time() + 7 * 24 * 3600
    os.utime(path, (future, future))

    assert touch_working_copy_access(str(path)) is False
    stat_after = path.stat()
    assert stat_after.st_atime == future
    assert stat_after.st_mtime == future


def test_touch_preserves_content_version_mtime_for_external_edit_handoff(
    tmp_path,
):
    """Regression for a P1 codex finding on PR #1607.

    ``_external_edit_handoff_path`` stamps the working copy's mtime
    into ``external-edits/<id>.json`` and regenerates
    ``external-edits/<id>.jpg`` via ``os.replace`` whenever that
    stamp changes. If ``touch_working_copy_access`` advanced mtime
    for LRU purposes, viewing the photo through ``/original`` or
    ``/edit-preview`` after the 6-hour throttle would tick mtime
    forward with no pixel change, the next Open External call would
    see the handoff as stale, and regenerating would overwrite the
    file the external editor had saved into. The fix advances atime
    only; mtime is reserved as the content-version key.
    """
    import time

    from working_copy_cache import touch_working_copy_access

    path = tmp_path / "1.jpg"
    path.write_bytes(b"x" * 100)
    stale = time.time() - 30 * 24 * 3600
    os.utime(path, (stale, stale))
    published_mtime = path.stat().st_mtime

    # Simulate many LRU touches (each throttled, but at least the first
    # one lands after 30 days of staleness).
    for _ in range(5):
        touch_working_copy_access(str(path))

    assert path.stat().st_mtime == published_mtime, (
        "LRU access touch advanced the working copy's mtime — the "
        "external-edit handoff would treat this as a content change "
        "and overwrite the file the external editor saved into"
    )
    # Access-recency did move forward, so eviction still sees it.
    assert path.stat().st_atime > stale


def test_eviction_reads_atime_as_recency_alongside_mtime(tmp_path):
    """``_evict_once`` sort key is max(mtime, atime).

    Regression for the same P1 finding: since mtime no longer advances
    on LRU touches, eviction must consult atime as well, otherwise a
    touched-recently file with a stale mtime looks like the coldest
    candidate and gets reclaimed first — the exact opposite of what
    the touch is meant to signal.
    """
    import time

    import config as cfg
    from db import Database
    from working_copy_cache import (
        evict_if_over_quota,
        touch_working_copy_access,
    )

    vireo_dir = tmp_path / "vireo"
    vireo_dir.mkdir()
    working_dir = vireo_dir / "working"
    working_dir.mkdir()
    monkey_cfg = tmp_path / "config.json"
    cfg.CONFIG_PATH = str(monkey_cfg)
    cfg.save({**cfg.DEFAULTS, "working_copy_cache_max_mb": 1})

    db = Database(str(vireo_dir / "test.db"))
    try:
        folder_id = db.add_folder(str(tmp_path / "photos"))
        (tmp_path / "photos").mkdir()
        ids = []
        for i in range(2):
            src = tmp_path / "photos" / f"photo-{i}.jpg"
            src.write_bytes(b"x" * 100)
            ids.append(db.add_photo(
                folder_id, f"photo-{i}.jpg", ".jpg",
                file_size=100, file_mtime=src.stat().st_mtime,
                width=100, height=100,
            ))

        # File A: recently-touched (fresh atime), stale mtime.
        recent_id = ids[0]
        recent_path = working_dir / f"{recent_id}.jpg"
        recent_path.write_bytes(b"a" * (700 * 1024))
        db.conn.execute(
            "UPDATE photos SET working_copy_path=? WHERE id=?",
            (f"working/{recent_id}.jpg", recent_id),
        )
        stale_mtime = time.time() - 30 * 24 * 3600
        os.utime(str(recent_path), (stale_mtime, stale_mtime))
        touch_working_copy_access(str(recent_path))

        # File B: born just now (fresh mtime, matching atime).
        fresh_id = ids[1]
        fresh_path = working_dir / f"{fresh_id}.jpg"
        fresh_path.write_bytes(b"b" * (700 * 1024))
        db.conn.execute(
            "UPDATE photos SET working_copy_path=? WHERE id=?",
            (f"working/{fresh_id}.jpg", fresh_id),
        )
        # Age B so its (atime, mtime) recency lands BEFORE A's touched
        # atime — the fresh-mtime side wins under max() otherwise.
        aged = time.time() - 60 * 60
        os.utime(str(fresh_path), (aged, aged))
        db.conn.commit()

        result = evict_if_over_quota(db, str(vireo_dir))
        db.close()

        # Under max(mtime, atime): A's recency = fresh atime > B's
        # aged (atime, mtime). So B should evict first.
        assert not fresh_path.exists(), (
            "eviction reclaimed the file with newer max(mtime, atime); "
            "it should have picked the older one"
        )
        assert recent_path.exists(), (
            "eviction reclaimed the recently-touched file — it "
            "ignored the LRU atime advance"
        )
        assert result["evicted"] >= 1
    finally:
        import contextlib as _ctxlib
        with _ctxlib.suppress(Exception):
            db.close()


def test_touch_is_best_effort_on_a_missing_file(tmp_path):
    """Serving bytes must never fail because eviction won the race."""
    from working_copy_cache import touch_working_copy_access

    assert touch_working_copy_access(str(tmp_path / "gone.jpg")) is False
    assert touch_working_copy_access(None) is False
