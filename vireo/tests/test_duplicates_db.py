"""DB integration tests for duplicate resolution (Tasks 7 & 8)."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from db import Database


def _add(db, folder_id, filename, file_hash=None, file_mtime=100.0, rating=0):
    """Helper: add_photo + optionally set file_hash + rating in-place."""
    pid = db.add_photo(
        folder_id=folder_id,
        filename=filename,
        extension=os.path.splitext(filename)[1] or ".jpg",
        file_size=1000,
        file_mtime=file_mtime,
        file_hash=file_hash,
    )
    if rating:
        db.conn.execute("UPDATE photos SET rating = ? WHERE id = ?", (rating, pid))
        db.conn.commit()
    return pid


def _reset_flags(db, file_hash):
    """Clear auto-rejection so tests can exercise apply_duplicate_resolution directly."""
    db.conn.execute(
        "UPDATE photos SET flag = 'none' WHERE file_hash = ?", (file_hash,)
    )
    db.conn.commit()


# -----------------------------------------------------------------------------
# Task 7: apply_duplicate_resolution
# -----------------------------------------------------------------------------

def test_apply_resolution_rejects_loser_and_merges_rating(tmp_path):
    """The resolver picks a winner, merges max rating, rejects losers."""
    db = Database(str(tmp_path / "t.db"))
    fid = db.add_folder(str(tmp_path))
    p1 = _add(db, fid, "owl.jpg", file_hash="HASH1", rating=0)
    p2 = _add(db, fid, "owl (2).jpg", file_hash="HASH1")
    # The add_photo hook auto-rejected one already; reset so we can test
    # apply_duplicate_resolution directly. Also set p2's rating via raw SQL.
    _reset_flags(db, "HASH1")
    db.conn.execute("UPDATE photos SET rating = ? WHERE id = ?", (5, p2))
    db.conn.commit()

    result = db.apply_duplicate_resolution([p1, p2])
    assert result["winner_id"] == p1
    assert result["loser_ids"] == [p2]
    assert result["rejected"] == 1

    row = db.conn.execute(
        "SELECT rating, flag FROM photos WHERE id = ?", (p1,)
    ).fetchone()
    assert row["rating"] == 5
    assert row["flag"] != "rejected"
    row = db.conn.execute(
        "SELECT flag FROM photos WHERE id = ?", (p2,)
    ).fetchone()
    assert row["flag"] == "rejected"


def test_apply_resolution_skips_already_rejected(tmp_path):
    """If fewer than 2 non-rejected candidates remain, resolver is a no-op.

    After two add_photo calls with a shared hash, the hook has already
    rejected one of them. Calling apply_duplicate_resolution on the same
    pair should be a no-op.
    """
    db = Database(str(tmp_path / "t.db"))
    fid = db.add_folder(str(tmp_path))
    p1 = _add(db, fid, "a.jpg", file_hash="H")
    p2 = _add(db, fid, "b.jpg", file_hash="H")

    # Exactly one of p1/p2 is rejected by the hook; identify the survivor.
    flags = {
        r["id"]: r["flag"]
        for r in db.conn.execute(
            "SELECT id, flag FROM photos WHERE id IN (?, ?)", (p1, p2)
        ).fetchall()
    }
    rejected = [pid for pid, f in flags.items() if f == "rejected"]
    assert len(rejected) == 1

    result = db.apply_duplicate_resolution([p1, p2])
    assert result["winner_id"] is None
    assert result["loser_ids"] == []
    assert result["rejected"] == 0
    # Flags unchanged.
    flags_after = {
        r["id"]: r["flag"]
        for r in db.conn.execute(
            "SELECT id, flag FROM photos WHERE id IN (?, ?)", (p1, p2)
        ).fetchall()
    }
    assert flags_after == flags


def test_apply_resolution_merges_keywords(tmp_path):
    """Winner gains loser's keywords (union)."""
    db = Database(str(tmp_path / "t.db"))
    fid = db.add_folder(str(tmp_path))
    p1 = _add(db, fid, "owl.jpg", file_hash="H")
    p2 = _add(db, fid, "owl (2).jpg", file_hash="H")
    # Reset hook's auto-rejection so apply_duplicate_resolution sees both.
    _reset_flags(db, "H")
    kw_id = db.add_keyword("bird")
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (p2, kw_id),
    )
    db.conn.commit()

    db.apply_duplicate_resolution([p1, p2])
    rows = db.conn.execute(
        "SELECT keyword_id FROM photo_keywords WHERE photo_id = ?", (p1,)
    ).fetchall()
    assert any(r["keyword_id"] == kw_id for r in rows)


def test_apply_resolution_single_candidate_noop(tmp_path):
    """With <2 candidates we return an empty result with no changes."""
    db = Database(str(tmp_path / "t.db"))
    fid = db.add_folder(str(tmp_path))
    p1 = _add(db, fid, "a.jpg", file_hash="H")
    result = db.apply_duplicate_resolution([p1])
    assert result["winner_id"] is None
    assert result["rejected"] == 0


# -----------------------------------------------------------------------------
# Task 8: Auto-resolve hook in add_photo
# -----------------------------------------------------------------------------

def test_add_photo_auto_rejects_duplicate(tmp_path):
    """Adding a second photo with a duplicate file_hash auto-rejects the loser."""
    db = Database(str(tmp_path / "t.db"))
    fid = db.add_folder(str(tmp_path))
    p1 = _add(db, fid, "owl.jpg", file_hash="HDUP")
    p2 = _add(db, fid, "owl (2).jpg", file_hash="HDUP")

    # Dirty-suffix filename loses to clean filename (rule 1).
    row2 = db.conn.execute("SELECT flag FROM photos WHERE id = ?", (p2,)).fetchone()
    assert row2["flag"] == "rejected"
    row1 = db.conn.execute("SELECT flag FROM photos WHERE id = ?", (p1,)).fetchone()
    assert row1["flag"] != "rejected"


def test_auto_resolve_does_not_unreject_existing_loser(tmp_path):
    """Adding a new dup must not un-reject a previously rejected loser.

    Design decision: the hook filters ``flag != 'rejected'`` before resolving,
    so rows already rejected on a past pass stay rejected even if a newer
    candidate with a "better" name arrives later.
    """
    db = Database(str(tmp_path / "t.db"))
    fid = db.add_folder(str(tmp_path))
    # Step 1 & 2: insert two photos sharing a hash; the hook rejects one.
    old = _add(db, fid, "old.jpg", file_hash="HX")
    loser = _add(db, fid, "loser (2).jpg", file_hash="HX")

    # Step 3: force the state we want to test — `loser` is the pre-existing
    # rejected row, `old` is clean. (The hook probably already did this given
    # the filenames, but set it explicitly so the test does not depend on
    # tiebreaker rules.)
    db.conn.execute("UPDATE photos SET flag = 'rejected' WHERE id = ?", (loser,))
    db.conn.execute("UPDATE photos SET flag = 'none' WHERE id = ?", (old,))
    db.conn.commit()

    # Step 4: a NEW photo arrives with a clean filename and the same hash.
    newcomer = _add(db, fid, "newcomer.jpg", file_hash="HX")

    # Step 5: the previously rejected loser stays rejected.
    row_loser = db.conn.execute(
        "SELECT flag FROM photos WHERE id = ?", (loser,)
    ).fetchone()
    assert row_loser["flag"] == "rejected", (
        "Hook un-rejected a previously rejected duplicate — regression."
    )

    # Step 6: among {old, newcomer}, exactly one is rejected (the hook
    # resolved the pair); which one depends on tiebreakers but both must
    # not be rejected simultaneously, and at least one must survive.
    row_old = db.conn.execute(
        "SELECT flag FROM photos WHERE id = ?", (old,)
    ).fetchone()
    row_new = db.conn.execute(
        "SELECT flag FROM photos WHERE id = ?", (newcomer,)
    ).fetchone()
    survivors = [f for f in (row_old["flag"], row_new["flag"]) if f != "rejected"]
    assert len(survivors) >= 1


def test_add_photo_no_hash_no_hook(tmp_path):
    """add_photo without file_hash does not trigger the hook."""
    db = Database(str(tmp_path / "t.db"))
    fid = db.add_folder(str(tmp_path))
    p1 = _add(db, fid, "a.jpg")
    p2 = _add(db, fid, "b.jpg")
    for pid in (p1, p2):
        row = db.conn.execute("SELECT flag FROM photos WHERE id = ?", (pid,)).fetchone()
        assert row["flag"] == "none"


def test_add_photo_hook_swallows_sqlite_error(tmp_path, monkeypatch):
    """sqlite3.Error raised inside the hook is logged and swallowed."""
    import sqlite3

    db = Database(str(tmp_path / "t.db"))
    fid = db.add_folder(str(tmp_path))
    p1 = _add(db, fid, "owl.jpg", file_hash="HBOOM")

    # Patch apply_duplicate_resolution on the instance so the hook's
    # execute() succeeds but the resolver call raises sqlite3.Error.
    def _boom(ids):
        raise sqlite3.OperationalError("synthetic")
    monkeypatch.setattr(db, "apply_duplicate_resolution", _boom)

    p2 = db.add_photo(
        folder_id=fid,
        filename="owl (2).jpg",
        extension=".jpg",
        file_size=1000,
        file_mtime=100.0,
        file_hash="HBOOM",
    )
    assert p2 is not None
    # Neither photo is rejected because the hook failed gracefully.
    r2 = db.conn.execute("SELECT flag FROM photos WHERE id = ?", (p2,)).fetchone()
    assert r2["flag"] != "rejected"


# -----------------------------------------------------------------------------
# Scanner-style flow: add_photo (no hash) + UPDATE hash + explicit hook call.
# -----------------------------------------------------------------------------

def test_check_and_resolve_for_hash_covers_scanner_flow(tmp_path):
    """Mimic scanner.py: insert without file_hash, UPDATE it, then call the hook."""
    db = Database(str(tmp_path / "t.db"))
    fid = db.add_folder(str(tmp_path))

    # Scanner path: add_photo with no hash, then UPDATE photos SET file_hash=?
    p1 = db.add_photo(
        folder_id=fid, filename="owl.jpg", extension=".jpg",
        file_size=1000, file_mtime=100.0,
    )
    p2 = db.add_photo(
        folder_id=fid, filename="owl (2).jpg", extension=".jpg",
        file_size=1000, file_mtime=100.0,
    )
    db.conn.execute("UPDATE photos SET file_hash = ? WHERE id = ?", ("SCANHASH", p1))
    db.conn.commit()
    # First call: only one row has the hash — no-op.
    result = db.check_and_resolve_duplicates_for_hash("SCANHASH")
    assert result is None

    db.conn.execute("UPDATE photos SET file_hash = ? WHERE id = ?", ("SCANHASH", p2))
    db.conn.commit()
    # Second call: two rows share the hash — resolve.
    result = db.check_and_resolve_duplicates_for_hash("SCANHASH")
    assert result is not None
    assert result["winner_id"] == p1
    assert result["loser_ids"] == [p2]

    r1 = db.conn.execute("SELECT flag FROM photos WHERE id = ?", (p1,)).fetchone()
    r2 = db.conn.execute("SELECT flag FROM photos WHERE id = ?", (p2,)).fetchone()
    assert r1["flag"] != "rejected"
    assert r2["flag"] == "rejected"


# -----------------------------------------------------------------------------
# Task 9: find_duplicate_groups
# -----------------------------------------------------------------------------

def test_find_duplicate_groups_returns_only_multi_groups(tmp_path):
    """Returns hashes with 2+ non-rejected rows; skips singletons."""
    db = Database(str(tmp_path / "t.db"))
    fid = db.add_folder(str(tmp_path))
    # Group A: 2 dups — hook auto-rejects one, reset so scan sees both again.
    a1 = _add(db, fid, "a.jpg", file_hash="HA")
    a2 = _add(db, fid, "a copy.jpg", file_hash="HA")
    _reset_flags(db, "HA")
    # Group B: singleton
    _add(db, fid, "b.jpg", file_hash="HB")
    # Group C: two rows but one rejected -> should not appear
    c1 = _add(db, fid, "c.jpg", file_hash="HC")
    c2 = _add(db, fid, "c (2).jpg", file_hash="HC")
    # Hook already rejected one of c1/c2; leave as-is so only 1 non-rejected row.
    # Group D: NULL hash -> should not appear
    _add(db, fid, "d.jpg")

    groups = db.find_duplicate_groups()
    hashes = [g["file_hash"] for g in groups]
    assert "HA" in hashes
    assert "HB" not in hashes
    assert "HC" not in hashes  # only one non-rejected row
    assert None not in hashes

    ha = next(g for g in groups if g["file_hash"] == "HA")
    assert sorted(ha["photo_ids"]) == sorted([a1, a2])


def test_find_duplicate_groups_empty(tmp_path):
    """No dup groups -> empty list."""
    db = Database(str(tmp_path / "t.db"))
    fid = db.add_folder(str(tmp_path))
    _add(db, fid, "solo.jpg", file_hash="SOLO")
    assert db.find_duplicate_groups() == []


def test_find_duplicate_groups_default_excludes_resolved(tmp_path):
    """Auto-resolved pairs (1 kept + N rejected) must NOT appear by default.

    Locks in the legacy contract: callers that don't opt in to
    ``include_resolved=True`` get only actionable (still-needs-a-decision)
    groups. Without this guard, the duplicates page's apply flow would
    re-process pairs that already have a winner.
    """
    db = Database(str(tmp_path / "t.db"))
    fid = db.add_folder(str(tmp_path))
    # add_photo hook auto-resolves: clean name wins, "-2" loses.
    _add(db, fid, "owl.jpg", file_hash="HRES")
    _add(db, fid, "owl-2.jpg", file_hash="HRES")
    groups = db.find_duplicate_groups()
    assert groups == []


def test_find_duplicate_groups_with_resolved_returns_both_statuses(tmp_path):
    """``include_resolved=True`` surfaces both unresolved and resolved groups
    with a ``status`` discriminator."""
    db = Database(str(tmp_path / "t.db"))
    fid = db.add_folder(str(tmp_path))
    # Resolved group: hook auto-rejects "-2".
    _add(db, fid, "a.jpg", file_hash="HA")
    _add(db, fid, "a-2.jpg", file_hash="HA")
    # Unresolved group: undo the hook so both rows are non-rejected.
    _add(db, fid, "b.jpg", file_hash="HB")
    _add(db, fid, "b copy.jpg", file_hash="HB")
    _reset_flags(db, "HB")

    groups = db.find_duplicate_groups(include_resolved=True)
    by_hash = {g["file_hash"]: g for g in groups}
    assert by_hash["HA"]["status"] == "resolved"
    assert by_hash["HB"]["status"] == "unresolved"
    # Resolved group's photo_ids must include BOTH the kept and rejected row;
    # downstream code reads p.flag to disambiguate, but needs both ids to do so.
    assert len(by_hash["HA"]["photo_ids"]) == 2


def test_find_duplicate_groups_skips_purely_rejected_hashes(tmp_path):
    """A hash with zero kept rows is NOT a duplicate cleanup target.

    Example: user manually rejected the only copy of a unique photo for
    non-duplicate reasons. With no kept "winner" anchor, there's nothing for
    the duplicates page to do. Including these would mislead the UI into
    treating the user's unrelated reject as a duplicate-loser cleanup task.
    """
    db = Database(str(tmp_path / "t.db"))
    fid = db.add_folder(str(tmp_path))
    pid = _add(db, fid, "lonely.jpg", file_hash="HONLY")
    db.conn.execute("UPDATE photos SET flag='rejected' WHERE id=?", (pid,))
    db.conn.commit()

    groups = db.find_duplicate_groups(include_resolved=True)
    assert all(g["file_hash"] != "HONLY" for g in groups)


def test_find_duplicate_groups_skips_resolved_with_multiple_kept(tmp_path):
    """3-way group with 2 kept + 1 rejected is still 'unresolved', not resolved.

    The "resolved" status means exactly one survivor. If two kept rows share
    the hash, the user still has a decision to make — that's the unresolved
    code path.
    """
    db = Database(str(tmp_path / "t.db"))
    fid = db.add_folder(str(tmp_path))
    p1 = _add(db, fid, "x.jpg", file_hash="HX")
    p2 = _add(db, fid, "x copy.jpg", file_hash="HX")
    p3 = _add(db, fid, "x-2.jpg", file_hash="HX")
    # Reject only p3; p1 and p2 remain non-rejected.
    db.conn.execute(
        "UPDATE photos SET flag = CASE WHEN id = ? THEN 'rejected' ELSE 'none' END "
        "WHERE id IN (?, ?, ?)",
        (p3, p1, p2, p3),
    )
    db.conn.commit()

    groups = db.find_duplicate_groups(include_resolved=True)
    by_hash = {g["file_hash"]: g for g in groups}
    assert by_hash["HX"]["status"] == "unresolved"


def test_run_duplicate_scan_emits_resolved_proposal(tmp_path):
    """Resolved groups appear in the scan result with status='resolved' and
    rejected losers carrying the resolver reason."""
    from duplicate_scan import run_duplicate_scan

    db = Database(str(tmp_path / "t.db"))
    fid = db.add_folder(str(tmp_path))
    _add(db, fid, "owl.jpg", file_hash="HRES2")
    _add(db, fid, "owl-2.jpg", file_hash="HRES2")
    # Hook auto-rejected owl-2.

    result = run_duplicate_scan({"progress": {}}, db, include_resolved=True)
    resolved = [p for p in result["proposals"] if p["status"] == "resolved"]
    assert len(resolved) == 1
    prop = resolved[0]
    assert prop["winner"]["filename"] == "owl.jpg"
    assert len(prop["losers"]) == 1
    assert prop["losers"][0]["filename"] == "owl-2.jpg"
    assert prop["losers"][0]["rejected"] is True
    assert prop["losers"][0]["reason"]
    assert result["resolved_group_count"] == 1
    assert result["resolved_loser_count"] == 1
    # Loser count (the apply-able count) must NOT include resolved losers.
    assert result["loser_count"] == 0


def test_run_duplicate_scan_excludes_resolved_when_opt_out(tmp_path):
    """``include_resolved=False`` preserves the legacy unresolved-only output."""
    from duplicate_scan import run_duplicate_scan

    db = Database(str(tmp_path / "t.db"))
    fid = db.add_folder(str(tmp_path))
    _add(db, fid, "p.jpg", file_hash="HEX")
    _add(db, fid, "p-2.jpg", file_hash="HEX")

    result = run_duplicate_scan({"progress": {}}, db, include_resolved=False)
    assert result["proposals"] == []
    assert result["resolved_group_count"] == 0


def test_run_duplicate_scan_chunks_large_groups(tmp_path, monkeypatch):
    """A single resolved group with more than ``_SQL_PARAM_CHUNK`` photo_ids
    must not raise ``OperationalError`` on SQLite builds with the legacy
    999-parameter cap. Patch the cap down to 2 and seed 5 rows in one group
    so the chunked SELECT path has to run multiple iterations.
    """
    import duplicate_scan as ds
    from duplicate_scan import run_duplicate_scan

    monkeypatch.setattr(ds, "_SQL_PARAM_CHUNK", 2)

    db = Database(str(tmp_path / "t.db"))
    fid = db.add_folder(str(tmp_path))
    # 1 winner + 4 rejected siblings sharing a hash. The hook auto-rejects
    # everything but the cleanest filename, so we end up with 1 kept + 4
    # rejected — a "resolved" group with 5 photo_ids in scope.
    _add(db, fid, "owl.jpg", file_hash="HBIG")
    for i in range(2, 6):
        _add(db, fid, f"owl-{i}.jpg", file_hash="HBIG")

    result = run_duplicate_scan({"progress": {}}, db, include_resolved=True)
    resolved = [p for p in result["proposals"] if p["status"] == "resolved"]
    assert len(resolved) == 1
    # All 4 losers surface even though each chunked SELECT only saw 2 ids.
    assert len(resolved[0]["losers"]) == 4


# -----------------------------------------------------------------------------
# Scanner-order regression: XMP import must land BEFORE auto-resolve so a
# loser's keywords are merged onto the winner (Codex review fix #1).
# -----------------------------------------------------------------------------

def test_auto_resolve_after_xmp_import_merges_loser_keywords(tmp_path):
    """The loser's keywords must end up on the winner when auto-resolve fires.

    Scanner flow: add_photo (no hash) -> UPDATE file_hash -> import XMP
    keywords -> check_and_resolve_duplicates_for_hash. If XMP import ran
    AFTER the hook, the loser's keywords would be stranded on the rejected
    row. This test locks in the merge contract by simulating that order at
    the DB layer: keywords linked to the loser BEFORE the hook fires, then
    assert they are carried over to the winner.
    """
    db = Database(str(tmp_path / "t.db"))
    fid = db.add_folder(str(tmp_path))

    # Raw inserts so the auto-hook does not fire prematurely.
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size, "
        "file_mtime, file_hash, flag) VALUES (?, ?, ?, ?, ?, ?, 'none')",
        (fid, "winner.jpg", ".jpg", 1000, 100.0, "HASH"),
    )
    winner_id = db.conn.execute(
        "SELECT id FROM photos WHERE filename = ?", ("winner.jpg",)
    ).fetchone()["id"]
    # Dirty-suffix filename loses to clean (tiebreaker rule 1).
    db.conn.execute(
        "INSERT INTO photos (folder_id, filename, extension, file_size, "
        "file_mtime, file_hash, flag) VALUES (?, ?, ?, ?, ?, ?, 'none')",
        (fid, "winner (2).jpg", ".jpg", 1000, 100.0, "HASH"),
    )
    loser_id = db.conn.execute(
        "SELECT id FROM photos WHERE filename = ?", ("winner (2).jpg",)
    ).fetchone()["id"]
    db.conn.commit()

    # Simulate XMP import: link a keyword to the loser ONLY.
    kw_id = db.add_keyword("bird")
    db.conn.execute(
        "INSERT INTO photo_keywords (photo_id, keyword_id) VALUES (?, ?)",
        (loser_id, kw_id),
    )
    db.conn.commit()

    # Now fire the auto-resolve hook (as scanner does after XMP import).
    result = db.check_and_resolve_duplicates_for_hash("HASH")
    assert result is not None
    assert result["winner_id"] == winner_id
    assert result["loser_ids"] == [loser_id]

    # Winner must have the loser's keyword merged onto it.
    winner_kws = db.conn.execute(
        "SELECT keyword_id FROM photo_keywords WHERE photo_id = ?", (winner_id,)
    ).fetchall()
    assert any(r["keyword_id"] == kw_id for r in winner_kws), (
        "Loser's XMP keyword was not merged onto the winner — the scanner "
        "order (XMP import before auto-resolve) is broken."
    )

    # Loser flagged rejected.
    row = db.conn.execute(
        "SELECT flag FROM photos WHERE id = ?", (loser_id,)
    ).fetchone()
    assert row["flag"] == "rejected"


# -----------------------------------------------------------------------------
# run_duplicate_scan must filter rows rejected after find_duplicate_groups
# returns (race with concurrent ingest). (Codex review fix #2).
# -----------------------------------------------------------------------------

def test_run_duplicate_scan_skips_rows_rejected_after_find_groups(
    tmp_path, monkeypatch
):
    """If rows are rejected between find_duplicate_groups and the per-group
    SELECT inside the loop, the per-group SELECT must filter them out. If the
    survivors fall below 2, the group is skipped.

    We monkeypatch ``find_duplicate_groups`` to simulate the race: it returns
    3 candidate IDs, but 2 are already rejected at the moment the per-group
    SELECT runs — exactly the state a concurrent ingest + auto-hook would
    leave behind between the two queries.
    """
    from duplicate_scan import run_duplicate_scan

    db = Database(str(tmp_path / "t.db"))
    fid = db.add_folder(str(tmp_path))
    ids = []
    for name in ("a.jpg", "b.jpg", "c.jpg"):
        db.conn.execute(
            "INSERT INTO photos (folder_id, filename, extension, file_size, "
            "file_mtime, file_hash, flag) VALUES (?, ?, ?, ?, ?, ?, 'none')",
            (fid, name, ".jpg", 1000, 100.0, "HRACE"),
        )
        ids.append(
            db.conn.execute(
                "SELECT id FROM photos WHERE filename = ?", (name,)
            ).fetchone()["id"]
        )
    # Reject two BEFORE the scan; then spoof find_duplicate_groups to still
    # report all three (the race window).
    db.conn.execute(
        "UPDATE photos SET flag = 'rejected' WHERE filename IN (?, ?)",
        ("a.jpg", "b.jpg"),
    )
    db.conn.commit()

    def _stale_find_groups(include_resolved=False):
        return [{"file_hash": "HRACE", "photo_ids": ids}]

    monkeypatch.setattr(db, "find_duplicate_groups", _stale_find_groups)

    # include_resolved=False to keep the legacy unresolved-only race contract
    # in scope: the resolved-group code path has its own sanity checks and
    # this test is specifically about the unresolved race window.
    result = run_duplicate_scan({"progress": {}}, db, include_resolved=False)
    # Only one non-rejected row; the per-group SELECT must filter to 1 and
    # the `< 2` guard must drop it.
    assert result["proposals"] == []


def test_run_duplicate_scan_positive_case_with_one_rejected(
    tmp_path, monkeypatch
):
    """Race-window variant: 3 reported, 1 rejected; scan proposes group of 2."""
    from duplicate_scan import run_duplicate_scan

    db = Database(str(tmp_path / "t.db"))
    fid = db.add_folder(str(tmp_path))
    ids = []
    for name in ("x.jpg", "x (2).jpg", "x (3).jpg"):
        db.conn.execute(
            "INSERT INTO photos (folder_id, filename, extension, file_size, "
            "file_mtime, file_hash, flag) VALUES (?, ?, ?, ?, ?, ?, 'none')",
            (fid, name, ".jpg", 1000, 100.0, "HPOS"),
        )
        ids.append(
            db.conn.execute(
                "SELECT id FROM photos WHERE filename = ?", (name,)
            ).fetchone()["id"]
        )
    db.conn.execute(
        "UPDATE photos SET flag = 'rejected' WHERE filename = ?", ("x (3).jpg",)
    )
    db.conn.commit()

    def _stale_find_groups(include_resolved=False):
        return [{"file_hash": "HPOS", "photo_ids": ids}]

    monkeypatch.setattr(db, "find_duplicate_groups", _stale_find_groups)

    result = run_duplicate_scan({"progress": {}}, db, include_resolved=False)
    assert len(result["proposals"]) == 1
    prop = result["proposals"][0]
    # 1 winner + 1 loser after filtering the rejected row.
    assert 1 + len(prop["losers"]) == 2


# -----------------------------------------------------------------------------
# Rule 0 integration: existence check across the DB and scan layers. The
# resolver itself is unit-tested in test_duplicates.py — these tests verify
# the DB and scan layers actually populate ``exists`` from disk.
# -----------------------------------------------------------------------------

def test_apply_resolution_promotes_present_over_missing(tmp_path):
    """The resolver must not pick a winner whose file is gone from disk.

    Setup mirrors the user-reported bug: two rows share a hash, but the row
    with the heuristically-better path (shorter, clean filename) has its
    file deleted on disk. apply_duplicate_resolution should promote the
    surviving copy via Rule 0.
    """
    db = Database(str(tmp_path / "t.db"))
    # Two folders so paths differ in length.
    short_dir = tmp_path / "a"
    long_dir = tmp_path / "archive" / "deep"
    short_dir.mkdir()
    long_dir.mkdir(parents=True)
    short_fid = db.add_folder(str(short_dir))
    long_fid = db.add_folder(str(long_dir))

    # Create *only* the file in the long path. The short-path file does not
    # exist on disk — its DB row is a ghost.
    (long_dir / "owl.jpg").write_bytes(b"binary")

    p_ghost = _add(db, short_fid, "owl.jpg", file_hash="HG")
    p_real = _add(db, long_fid, "owl.jpg", file_hash="HG")
    _reset_flags(db, "HG")

    result = db.apply_duplicate_resolution([p_ghost, p_real])
    assert result["winner_id"] == p_real, (
        "Resolver picked a missing-on-disk row over a surviving one — Rule 0 broken."
    )
    assert result["loser_ids"] == [p_ghost]


def test_run_duplicate_scan_marks_missing_files(tmp_path):
    """Scan proposals must surface ``exists`` and ``all_missing`` so the UI
    can warn the user before they trash surviving copies."""
    from duplicate_scan import run_duplicate_scan

    db = Database(str(tmp_path / "t.db"))
    short_dir = tmp_path / "short"
    long_dir = tmp_path / "long"
    short_dir.mkdir()
    long_dir.mkdir()
    short_fid = db.add_folder(str(short_dir))
    long_fid = db.add_folder(str(long_dir))

    # Only the long-path file exists on disk.
    (long_dir / "owl.jpg").write_bytes(b"x")
    p_ghost = _add(db, short_fid, "owl.jpg", file_hash="HMISS")
    p_real = _add(db, long_fid, "owl.jpg", file_hash="HMISS")
    _reset_flags(db, "HMISS")

    result = run_duplicate_scan({"progress": {}}, db, include_resolved=False)
    assert len(result["proposals"]) == 1
    prop = result["proposals"][0]
    assert prop["all_missing"] is False
    assert prop["winner"]["id"] == p_real
    assert prop["winner"]["exists"] is True
    assert len(prop["losers"]) == 1
    assert prop["losers"][0]["id"] == p_ghost
    assert prop["losers"][0]["exists"] is False
    assert prop["losers"][0]["reason"] == "file missing on disk"


def test_run_duplicate_scan_all_missing_flag(tmp_path):
    """When every candidate is missing on disk, the proposal flags it so the
    UI can tell the user there's nothing to trash — only DB rows to clean up."""
    from duplicate_scan import run_duplicate_scan

    db = Database(str(tmp_path / "t.db"))
    a_dir = tmp_path / "a"
    b_dir = tmp_path / "b"
    a_dir.mkdir()
    b_dir.mkdir()
    a_fid = db.add_folder(str(a_dir))
    b_fid = db.add_folder(str(b_dir))

    # No files written. Both rows are ghosts.
    _add(db, a_fid, "owl.jpg", file_hash="HALLG")
    _add(db, b_fid, "owl.jpg", file_hash="HALLG")
    _reset_flags(db, "HALLG")

    result = run_duplicate_scan({"progress": {}}, db, include_resolved=False)
    assert len(result["proposals"]) == 1
    prop = result["proposals"][0]
    assert prop["all_missing"] is True
    assert prop["winner"]["exists"] is False
    assert all(l["exists"] is False for l in prop["losers"])
