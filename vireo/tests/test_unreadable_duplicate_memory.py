"""Selected scanner boundaries, invented XML/SQLite and a fake duplicate chooser."""

import contextlib
import runpy
import sqlite3
from pathlib import Path
from types import SimpleNamespace

SUPPORT = runpy.run_path(str(Path(__file__).with_name("test_xmp_keyword_snapshot.py")))


def duplicate_flow(*, sidecar="unreadable", repeated_failures=0, recover=True, changed_file=False):
    ns, db, reads, imported = SUPPORT["_namespace"]()
    parse = ns["ET"].parse
    mode = [sidecar]
    attempts = []
    trace = []

    def read(path):
        attempts.append(path)
        if mode[0] == "unreadable":
            raise PermissionError("invented unreadable sidecar")
        if mode[0] == "empty":
            return SUPPORT["ET"].ElementTree(SUPPORT["ET"].fromstring(
                f'<rdf:RDF xmlns:rdf="{ns["NS_RDF"]}"><rdf:Description rdf:about=""/></rdf:RDF>',
            ))
        return parse(path)

    ns["ET"].parse = read
    with contextlib.closing(sqlite3.connect(":memory:")) as conn:
        conn.row_factory = sqlite3.Row
        conn.executescript("""
            CREATE TABLE photos(id INTEGER PRIMARY KEY, flag TEXT, file_hash TEXT,
                                xmp_mtime REAL, file_mtime REAL, file_size INTEGER, xmp_import_pending INTEGER DEFAULT 0);
            CREATE TABLE keywords(photo_id INTEGER, name TEXT, PRIMARY KEY(photo_id,name));
            INSERT INTO photos VALUES(700,NULL,'invented hash',NULL,1,100,0);
            INSERT INTO keywords VALUES(700,'Winner keyword');
        """)
        row, catalog_events, catalog = SUPPORT["_catalog_boundary"](
            ns, db, existing=False, xmp_mtime=None if sidecar == "absent" else 9,
        )
        add_photo = db.add_photo

        def add(**values):
            result = add_photo(**values)
            conn.execute("INSERT OR IGNORE INTO photos VALUES(731,NULL,NULL,NULL,1,100,0)")
            return result

        db.add_photo = add
        update_photo = db.scanning.update_photo
        set_mtime = db.scanning.set_xmp_mtime
        commit = db.commit_with_retry
        import_lists = ns["_import_keyword_lists"]

        def update(pid, assignments, values):
            update_photo(pid, assignments, values)
            conn.execute("UPDATE photos SET file_hash=?,xmp_mtime=?,file_mtime=?,file_size=?,xmp_import_pending=? WHERE id=?",
                         (row.get("file_hash"), row["xmp_mtime"], row["file_mtime"], row["file_size"], row["xmp_import_pending"], pid))

        def acknowledge(pid, value):
            set_mtime(pid, value)
            trace.append(("acknowledge", pid))
            conn.execute("UPDATE photos SET xmp_mtime=?,xmp_import_pending=0 WHERE id=?", (value, pid))

        def commit_catalog():
            commit()
            conn.commit()

        def keywords(db_arg, pid, flat, hier):
            import_lists(db_arg, pid, flat, hier)
            trace.append(("keywords", pid))
            conn.executemany("INSERT OR IGNORE INTO keywords VALUES(?,?)", [(pid, key) for key in flat | set(hier)])

        def resolve(file_hash):
            trace.append(("resolve", file_hash))
            # Match the production exclusion of already-rejected losers.
            live = [r[0] for r in conn.execute(
                "SELECT id FROM photos WHERE file_hash=? AND (flag IS NULL OR flag!='rejected') ORDER BY id",
                (file_hash,),
            )]
            if len(live) < 2:
                return
            winner = live[0]
            for loser in live[1:]:
                conn.execute("INSERT OR IGNORE INTO keywords SELECT ?,name FROM keywords WHERE photo_id=?", (winner, loser))
                conn.execute("UPDATE photos SET flag='rejected' WHERE id=?", (loser,))
            conn.commit()

        db.scanning = SimpleNamespace(update_photo=update, set_xmp_mtime=acknowledge)
        db.commit_with_retry = commit_catalog
        db.check_and_resolve_duplicates_for_hash = resolve
        ns["_import_keyword_lists"] = keywords
        ns["_import_embedded_keywords_for_photo"] = lambda *args: trace.append(("embedded",))
        catalog()
        first = dict(conn.execute("SELECT * FROM photos WHERE id=731").fetchone())
        first_decisions = sum(event[0] == "resolve" for event in trace)

        def retry():
            existing, commits, _unused = SUPPORT["_incremental_boundary"](ns, db)
            stored = dict(conn.execute("SELECT * FROM photos WHERE id=731").fetchone())
            existing.update({key: stored[key] for key in ("xmp_mtime", "file_mtime", "file_size", "file_hash", "xmp_import_pending")})
            acknowledge_retry = db.scanning.set_xmp_mtime

            def ack(pid, value):
                acknowledge_retry(pid, value)
                trace.append(("acknowledge", pid))
                conn.execute("UPDATE photos SET xmp_mtime=?,xmp_import_pending=0 WHERE id=?", (value, pid))

            db.scanning.set_xmp_mtime = ack
            db.scanning.defer_xmp_import = lambda pid: conn.execute("UPDATE photos SET xmp_import_pending=1 WHERE id=?", (pid,))
            db.commit_with_retry = conn.commit
            db.check_and_resolve_duplicates_for_hash = resolve
            owner = SimpleNamespace(db=db, repair_missing_metadata=False,
                                    tally=SimpleNamespace(credit_reused_row=lambda *args: None))
            reuse = SUPPORT["_scanner_method"](ns, "triage.py", "_IncrementalTriage", "_reuse_existing_row")
            return reuse(owner, existing, "invented image", SimpleNamespace(st_mtime=2 if changed_file else 1, st_size=100),
                         "invented-memory://photo.xmp", 9)

        pending = []
        for _ in range(repeated_failures):
            retry()
            pending.append(dict(conn.execute("SELECT * FROM photos WHERE id=731").fetchone()))
        reused = None
        if sidecar == "unreadable" and recover:
            mode[0] = "readable"
            reused = retry()
        winner_keywords = {r[0] for r in conn.execute("SELECT name FROM keywords WHERE photo_id=700")}
        final = dict(conn.execute("SELECT * FROM photos WHERE id=731").fetchone())
        return SimpleNamespace(first=first, final=final, pending=pending, winner_keywords=winner_keywords,
                               first_decisions=first_decisions, trace=trace, attempts=attempts,
                               reads=reads, imported=imported, reused=reused, catalog_events=catalog_events)


def test_unreadable_duplicate_is_not_rejected_before_its_keywords_are_read():
    result = duplicate_flow(recover=False)
    assert result.first["flag"] is None and result.first["xmp_mtime"] is None
    assert result.first_decisions == 0
    assert result.imported == [] and len(result.attempts) == 1


def test_unchanged_mtime_retry_imports_keywords_before_rejecting_the_duplicate():
    result = duplicate_flow()
    assert result.reused is True and result.final["xmp_mtime"] == 9
    assert result.final["flag"] == "rejected"
    assert result.winner_keywords == {"Winner keyword", "Paris"}
    assert result.imported == [({"Paris"}, [])] and len(result.attempts) == 2
    assert [event[0] for event in result.trace] == ["embedded", "keywords", "acknowledge", "resolve"]


def test_repeated_read_failures_stay_pending_until_an_unchanged_mtime_read_succeeds():
    result = duplicate_flow(repeated_failures=2)
    assert result.first_decisions == 0
    assert all(row["flag"] is None and row["xmp_mtime"] is None for row in result.pending)
    assert result.winner_keywords == {"Winner keyword", "Paris"}
    assert result.final["flag"] == "rejected" and len(result.attempts) == 4
    assert sum(event[0] == "resolve" for event in result.trace) == 1


def test_changed_image_is_not_resolved_using_its_previous_hash_during_keyword_retry():
    result = duplicate_flow(changed_file=True)
    assert result.reused is False and result.final["flag"] is None
    assert result.imported == [({"Paris"}, [])]
    assert not any(event[0] == "resolve" for event in result.trace)


def test_readable_absent_and_empty_sidecars_keep_the_ordinary_duplicate_decision():
    for sidecar in ("readable", "absent", "empty"):
        result = duplicate_flow(sidecar=sidecar)
        assert result.first_decisions == 1 and result.final["flag"] == "rejected"
        assert result.winner_keywords == ({"Winner keyword", "Paris"} if sidecar == "readable" else {"Winner keyword"})
        assert len(result.attempts) == (0 if sidecar == "absent" else 1)
        if sidecar != "absent":
            names = [event[0] for event in result.trace]
            assert names.index("keywords") < names.index("acknowledge") < names.index("embedded") < names.index("resolve")
