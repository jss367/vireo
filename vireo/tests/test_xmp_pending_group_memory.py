"""Durable sidecar deferral: selected scanner/SQL on invented memory rows only."""
import __future__

import ast
import builtins
import contextlib
import json
import posixpath
import runpy
import sqlite3
from pathlib import Path
from types import MethodType, SimpleNamespace
from typing import cast

ROOT = Path(__file__).resolve().parents[1]
SUPPORT = runpy.run_path(str(Path(__file__).with_name("test_xmp_keyword_snapshot.py")))


class _TransactionOwner(SimpleNamespace):
    @property
    def in_transaction(self):
        return self._memory_connection.in_transaction


def _selected(module, name, namespace, cls=None):
    tree = ast.parse((ROOT / module).read_text(encoding="utf-8"))
    body = tree.body if cls is None else next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == cls).body
    if not any(isinstance(n, ast.FunctionDef) and n.name == name for n in body) and module == "db.py":
        body = ast.parse((ROOT / "services/duplicate_resolution.py").read_text()).body
        namespace["duplicate_resolution"] = SimpleNamespace(apply_duplicate_resolution=lambda db, ids: db._fake_apply_duplicate_resolution(ids))
        namespace["log"] = SimpleNamespace(warning=lambda *a: None)
        namespace["cast"] = cast
    node = next(n for n in body if isinstance(n, ast.FunctionDef) and n.name == name)
    exec(compile(ast.Module(body=[node], type_ignores=[]), "<pending memory>", "exec",
                 flags=__future__.annotations.compiler_flag), namespace)
    return namespace[name]


@contextlib.contextmanager
def _flow():
    ns, db, _reads, _imports = SUPPORT["_namespace"]()
    with contextlib.closing(sqlite3.connect(":memory:")) as conn:
        conn.row_factory = sqlite3.Row
        db = _TransactionOwner(**vars(db), _memory_connection=conn)
        conn.executescript("""
            CREATE TABLE folders(id INTEGER PRIMARY KEY,path TEXT);
            INSERT INTO folders VALUES(17,'invented');
            CREATE TABLE photos(id INTEGER PRIMARY KEY,folder_id INTEGER,filename TEXT,
                file_hash TEXT,file_mtime REAL,file_size INTEGER,xmp_mtime REAL,
                xmp_import_pending INTEGER NOT NULL DEFAULT 0,rating INTEGER,flag TEXT);
            CREATE TABLE keywords(photo_id INTEGER,name TEXT,source TEXT,PRIMARY KEY(photo_id,name));
            INSERT INTO photos VALUES(1,17,'winner.jpg','hash',1,100,NULL,0,0,NULL);
            INSERT INTO keywords VALUES(1,'Winner','manual');
        """)
        sentinel = object()
        def imports(name, *args, **kwargs):
            if name == "repositories.duplicates":
                return SimpleNamespace(DEFERRED_PLAN=sentinel)
            assert name == "duplicates"
            return SimpleNamespace(DupCandidate=lambda **kw: SimpleNamespace(**kw),
                                   resolve_duplicates=lambda candidates: (
                                       min(c.id for c in candidates),
                                       [(c.id, "invented") for c in candidates if c.id != min(x.id for x in candidates)],
                                   ))
        repo_ns = {"os": SimpleNamespace(path=SimpleNamespace(join=posixpath.join, exists=lambda p: True)),
                   "_volume_offline": lambda p: False,"DEFERRED_PLAN": sentinel,
                   "__builtins__": {**vars(builtins), "__import__": imports}}
        repo = SimpleNamespace(conn=conn, _chunks=lambda values: [values])
        for name in ("_resolution_rows", "resolution_preflight", "resolution_plan", "live_ids_for_hash"):
            setattr(repo, name, MethodType(_selected("repositories/duplicates.py", name, repo_ns, "DuplicatesRepository"), repo))
        scan_ns = {}
        scanning = SimpleNamespace(conn=conn)
        for name in ("update_photo", "set_xmp_mtime", "defer_xmp_import"):
            setattr(scanning, name, MethodType(_selected("repositories/scanning.py", name, scan_ns, "ScanningRepository"), scanning))
        db.scanning = scanning
        db.commit_with_retry = conn.commit
        db._commits_held = contextlib.nullcontext
        db._duplicates_repository = lambda: repo
        db.duplicates = repo
        db.begin_immediate = lambda: conn.execute("BEGIN IMMEDIATE")
        db.transaction = lambda: conn
        db.rollback = conn.rollback
        trace = []
        def merge(winner, losers):
            trace.append(("merge", winner, tuple(losers)))
            with conn:
                for loser in losers:
                    conn.execute("INSERT OR IGNORE INTO keywords SELECT ?,name,source FROM keywords WHERE photo_id=?", (winner, loser))
                    conn.execute("UPDATE photos SET flag='rejected' WHERE id=?", (loser,))
        db._apply_winner_loser_merge = merge
        db_ns = {"sqlite3": sqlite3,"logging": SimpleNamespace(getLogger=lambda *a: SimpleNamespace(warning=lambda *a: None)),
                 "__builtins__": {**vars(builtins), "__import__": imports}}
        for name in ("apply_duplicate_resolution", "check_and_resolve_duplicates_for_hash"):
            setattr(db, "_fake_" + name, MethodType(_selected("db.py", name, db_ns, "Database"), db))
        parse = ns["ET"].parse
        unreadable = [True]
        def read(path):
            if unreadable[0]:
                raise PermissionError("invented unreadable sidecar")
            return parse(path)
        ns["ET"].parse = read
        ns["_import_keyword_lists"] = lambda db_arg, pid, flat, hier: (
            trace.append(("keywords", pid)),
            conn.executemany("INSERT OR IGNORE INTO keywords VALUES(?,? ,NULL)", [(pid, value) for value in flat]),
        )
        ns["_import_embedded_keywords_for_photo"] = lambda *a: None
        catalog = SUPPORT["_scanner_method"](ns, "indexing.py", "_FileIndexer", "catalog_file")
        write = SUPPORT["_scanner_method"](ns, "indexing.py", "_FileIndexer", "_write_photo_columns")
        owner = SimpleNamespace(db=db,extract_full_metadata=False,_deferred_hashes=set(),
            _identity_column_updates=lambda item: (["file_hash=?"],[item.file_hash]),
            fills=SimpleNamespace(take_pending_companion_fill=lambda *a: None),
            _heal_changed_content=lambda *a: None,
            tally=SimpleNamespace(report_cataloged=lambda *a: None,
                                  report_sidecar_failure=lambda *a: None))
        owner._write_photo_columns = lambda *a: write(owner, *a)
        def visit(pid, *, sidecar, existing=False):
            owner._add_photo = lambda item: (
                conn.execute("INSERT OR IGNORE INTO photos VALUES(?,17,?,'hash',1,100,?,0,0,NULL)",
                             (pid,str(pid)+'.jpg',5 if existing else None)),pid,
            )[1]
            item = SimpleNamespace(image_path=SimpleNamespace(name=str(pid)+'.jpg'),xmp_path="invented-memory://photo.xmp",
                file_hash='hash',file_size=100,file_mtime=1,folder_id=17,xmp_mtime=9 if sidecar else None,
                row_already_existed=existing,meta=SimpleNamespace(file_meta={}))
            catalog(owner,item)
        def retry(pid, *, changed=False, mtime=9):
            stored=dict(conn.execute("SELECT * FROM photos WHERE id=?",(pid,)).fetchone())
            stored.update(extension='.jpg',width=1200,timestamp='invented',exif_extracted=True,summary_needs_extract=False)
            triage = SimpleNamespace(db=db,repair_missing_metadata=False,
                tally=SimpleNamespace(credit_reused_row=lambda *a: None,
                                      report_sidecar_failure=lambda *a: None))
            ns.update(RAW_EXTENSIONS=set(),EMPTY_FILE_SHA256='empty')
            reuse=SUPPORT["_scanner_method"](ns,"triage.py","_IncrementalTriage","_reuse_existing_row")
            return reuse(triage,stored,'invented',SimpleNamespace(st_mtime=2 if changed else 1,st_size=100),
                         'invented-memory://photo.xmp',mtime)
        yield SimpleNamespace(conn=conn,db=db,repo=repo,visit=visit,retry=retry,unreadable=unreadable,trace=trace,sentinel=sentinel)


def test_pending_group_defers_readable_and_sideless_siblings_in_either_order():
    for order in ((2,3),(3,2)):
        for sidecar in (False,True):
            with _flow() as f:
                for pid in order:
                    f.unreadable[0] = pid == 2
                    f.visit(pid,sidecar=True if pid == 2 else sidecar)
                row=f.conn.execute("SELECT flag,xmp_import_pending FROM photos WHERE id=2").fetchone()
                assert tuple(row)==(None,1)
                # A fresh caller has no scan-local deferred-hash set.
                assert f.db._fake_check_and_resolve_duplicates_for_hash('hash').get('deferred') is True
                f.unreadable[0]=False
                f.visit(4,sidecar=sidecar)
                assert f.conn.execute("SELECT flag FROM photos WHERE id=2").fetchone()[0] is None
                assert f.repo.resolution_plan([1,2,4]) is f.sentinel


def test_repeated_incremental_failure_with_nonnull_stamp_recovers_before_rejection():
    with _flow() as f:
        f.conn.execute("INSERT INTO photos VALUES(2,17,'pending.jpg','hash',1,100,5,0,0,NULL)")
        for _ in range(2):
            assert f.retry(2) is True
            assert tuple(f.conn.execute("SELECT xmp_mtime,xmp_import_pending,flag FROM photos WHERE id=2").fetchone())==(5,1,None)
            assert f.db._fake_check_and_resolve_duplicates_for_hash('hash').get('deferred') is True
        f.unreadable[0]=False
        assert f.retry(2) is True
        assert tuple(f.conn.execute("SELECT xmp_mtime,xmp_import_pending,flag FROM photos WHERE id=2").fetchone())==(9,0,'rejected')
        assert [r[0] for r in f.conn.execute("SELECT name FROM keywords WHERE photo_id=1 ORDER BY name")]==['Paris','Winner']
        assert f.conn.execute("SELECT source FROM keywords WHERE photo_id=1 AND name='Winner'").fetchone()[0]=='manual'
        assert f.trace.index(('keywords',2)) < next(i for i,e in enumerate(f.trace) if e[0]=='merge')


def test_pending_marker_forces_read_even_when_the_sidecar_mtime_matches():
    with _flow() as f:
        f.conn.execute("INSERT INTO photos VALUES(2,17,'pending.jpg','hash',1,100,9,1,0,NULL)")
        f.unreadable[0]=False
        assert f.retry(2) is True
        assert f.conn.execute("SELECT flag FROM photos WHERE id=2").fetchone()[0]=='rejected'


def test_changed_image_does_not_resolve_with_the_previous_hash_on_recovery():
    with _flow() as f:
        f.conn.execute("INSERT INTO photos VALUES(2,17,'pending.jpg','old hash',1,100,5,1,0,NULL)")
        f.unreadable[0]=False
        assert f.retry(2,changed=True) is False
        assert not any(e[0]=='merge' for e in f.trace)
        f.visit(2,sidecar=True,existing=True)
        assert f.conn.execute("SELECT file_hash,flag FROM photos WHERE id=2").fetchone()[:]==('hash','rejected')


def test_removed_sidecar_settles_pending_group_with_one_acknowledgement_commit():
    with _flow() as f:
        f.conn.execute("INSERT INTO photos VALUES(2,17,'pending.jpg','hash',1,100,5,1,0,NULL)")
        commits=[]
        f.db.commit_with_retry=lambda:(commits.append(True),f.conn.commit())
        assert f.retry(2,mtime=None) is True
        assert len(commits)==1
        assert f.conn.execute("SELECT xmp_import_pending,flag FROM photos WHERE id=2").fetchone()[:]==(0,'rejected')


def test_pending_update_acknowledgement_and_rollback_are_atomic():
    with _flow() as f:
        f.conn.execute("INSERT INTO photos VALUES(2,17,'pending.jpg','hash',1,100,5,0,0,NULL)")
        f.conn.commit()
        f.db.scanning.defer_xmp_import(2,clear_mtime=True)
        assert f.conn.execute("SELECT xmp_mtime,xmp_import_pending FROM photos WHERE id=2").fetchone()[:]==(None,1)
        f.conn.rollback()
        assert f.conn.execute("SELECT xmp_mtime,xmp_import_pending FROM photos WHERE id=2").fetchone()[:]==(5,0)


def test_migration_defaults_preserves_rows_is_idempotent_and_matches_canonical_column():
    ns={}
    migration=_selected('schema.py','_add_xmp_import_pending',ns)
    with contextlib.closing(sqlite3.connect(':memory:')) as conn:
        conn.execute('CREATE TABLE photos(id INTEGER PRIMARY KEY,xmp_mtime REAL,file_hash TEXT)')
        conn.execute("INSERT INTO photos VALUES(1,5,'hash')")
        migration(conn)
        migration(conn)
        assert conn.execute('SELECT * FROM photos').fetchone()==(1,5,'hash',0)
        column=next(r for r in conn.execute('PRAGMA table_info(photos)') if r[1]=='xmp_import_pending')
        assert column[2:5]==('INTEGER',1,'0')
    snapshot=json.loads((ROOT/'tests/fixtures/canonical_schema_snapshot.json').read_text())
    sql=next(r[3] for r in snapshot['sqlite_master'] if r[0]=='table' and r[1]=='photos')
    assert 'xmp_import_pending       INTEGER NOT NULL DEFAULT 0' in sql


def test_one_recovery_does_not_release_another_pending_member():
    with _flow() as f:
        for pid in (2, 3):
            f.conn.execute("INSERT INTO photos VALUES(?,17,?,'hash',1,100,5,1,0,NULL)", (pid, str(pid)+'.jpg'))
        f.unreadable[0] = False
        assert f.retry(2) is True
        assert f.conn.execute("SELECT flag FROM photos WHERE id=2").fetchone()[0] is None
        assert f.conn.execute("SELECT xmp_import_pending FROM photos WHERE id=3").fetchone()[0] == 1
        assert f.retry(3) is True
        assert [r[0] for r in f.conn.execute("SELECT flag FROM photos WHERE id IN (2,3)")] == ['rejected', 'rejected']


def test_new_sideless_hashless_row_does_not_add_a_pending_state_transaction():
    ns, db, _reads, _imports = SUPPORT["_namespace"]()
    _row, events, catalog = SUPPORT["_catalog_boundary"](ns, db, existing=False, file_hash=None, xmp_mtime=None)
    catalog()
    assert events == []


def test_sidecar_stat_failure_is_not_acknowledged_as_removal():
    ns, db, _reads, _imports = SUPPORT["_namespace"]()
    error = PermissionError("invented inaccessible sidecar stat")
    class Sidecar:
        def stat(self):
            raise error
    image = SimpleNamespace(stat=lambda: SimpleNamespace(st_size=100, st_mtime=1),
                            with_suffix=lambda suffix: Sidecar(), parent="invented", name="photo.jpg")
    for module, cls, name, args in [
        ("triage.py", "_IncrementalTriage", "_needs_processing", (image,)),
        ("indexing.py", "_FileIndexer", "index_file", (image, None, "hash")),
    ]:
        owner = SimpleNamespace(db=db, folders=SimpleNamespace(touch_folder=lambda path: 17))
        method = SUPPORT["_scanner_method"](ns, module, cls, name)
        try:
            method(owner, *args)
        except PermissionError as caught:
            assert caught is error
        else:
            raise AssertionError("inaccessible stat was treated as a removed sidecar")
