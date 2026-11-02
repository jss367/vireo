# Unit of work: explicit transaction ownership in the data layer

Date: 2026-10-25. Status: done (2026-11-01). Both ratchet counts in
`vireo/tests/test_transaction_ownership.py` are zero, and the commit hold is
replaced by a guard: inside a unit of work, `commit()`, `rollback()`, `with
conn:`, `begin()` and `begin_immediate()` raise `TransactionOwnershipError`
(`unit_of_work.GuardedConnection`). Two migration rules remain: an outermost
plain `transaction()` adopts a transaction its caller began outside any unit,
and `join=True` runs a savepoint inside one. The appendix below is the
2026-10-25 inventory, kept as history.

## Problem

Who ends a transaction in Vireo is decided by the callee, not the caller.

- Most `Database` and repository write methods commit on their own. A caller
  that needs two writes to land together threads `_commit=False` through every
  call in between (231 keyword arguments, 66 parameters, plus 34 `commit=`
  twins and four `*_no_commit` methods: 335 commit flags in total).
- Undo/redo replays an edit through those same committing setters. To keep the
  replay atomic under the prediction-decision lock, `db.py` subclasses
  `sqlite3.Connection` (`_Connection`) so that `commit()` silently does nothing
  while `Database._commits_held()` is active. Twelve other call sites (scan
  indexing, companion attach/keep, the merged-photo tally, maintenance
  history, `atomic_edit`, the import-location choice, the identity-alternatives
  route) borrow the same hold.
- Five repositories expose their own `commit()` method, and six write paths
  carry an ad-hoc savepoint block (`LocationRepository.transaction`,
  `PhotoReviewRepository.review_write`, `identity_merge_transaction`,
  `EditHistoryRepository.identity_replay` and `prune`,
  `MovesMergeRepository.photo_move_savepoint`), each with slightly different
  rules for "begin if nobody has, otherwise savepoint, commit if I began".
- `Database.transaction()` is `with self.conn:`: it opens nothing, commits
  whatever is open when the block exits (including work begun before it), and
  its C-level commit is *not* held by `_commits_held`, so it ends a held replay
  early if one runs inside it.

Atomicity therefore depends on every nested callee honoring a private flag. A
callee that forgets `_commit=False` (or a new one that never had the flag)
commits half of its caller's work; a caller that forgets to commit after
passing `_commit=False` loses the write silently when the connection closes.
In code whose job is protecting a photo library, that is the top correctness
risk three independent reviews found.

## Target

1. **Data methods never begin, commit or roll back.** Repository methods and
   `Database` data methods issue SQL on the connection and return. They have
   no `_commit` parameter and no `*_no_commit` twin.
2. **The owner of the work owns the transaction.** Each route, job step or
   service entry point that writes wraps its write phase in exactly one
   `with db.transaction():`. That block is the only commit point.
3. **Nested use is a savepoint.** A service that is sometimes the owner and
   sometimes called from inside another owner's block uses
   `with db.transaction():` too; inside an open unit it becomes a savepoint.
4. **No commit suppression.** `_commits_held`, `_Connection` and its
   `_commit_holds` counter are deleted. A stray `db.commit()` inside a unit of
   work raises instead of being swallowed.
5. **Prediction decisions use one context manager** in
   `services/prediction_decisions.py` that takes the lock, owns the
   transaction, revalidates workspace membership, and commits or rolls back.

## The API

```python
class Database:
    @contextlib.contextmanager
    def transaction(
        self, *, immediate: bool = False, retry_busy: bool = False,
    ) -> Iterator[None]: ...

    def after_commit(self, callback: Callable[[], object]) -> None: ...

    @property
    def in_unit_of_work(self) -> bool: ...
```

### `db.transaction()`

Outermost use (no unit of work is open on this `Database`):

- By default the transaction begins at the block's first write, exactly
  as sqlite3's own implicit transactions do: reads, file I/O and network
  calls before that write hold no lock.
- `immediate=True` issues `BEGIN IMMEDIATE` at once, so the block holds
  SQLite's single writer lock from its first statement and the reads that
  decide its writes cannot go stale. This is the check-then-write shape the
  codebase applies by hand at 40 `begin_immediate()` call sites (the
  prediction-decision lock among them), and it replaces both orders of the
  `begin_immediate()` / `with db.transaction()` pairing. Like `BEGIN`, it
  raises if a transaction is already open.
- **Why not `BEGIN IMMEDIATE` for every unit.** It would make every unit
  check-then-write safe by construction, but it would also hold the writer
  lock across whatever precedes the first write. Routes and services do slow
  work there: the location routes call Google Places, the EXIF-location
  batch reverse-geocodes a selection, previews and scans stat files. Under an
  eager lock each of those stalls every other writer (a scan committing per
  file) for its duration. Beginning at the first write keeps today's lock
  footprint, and it never meets WAL's `SQLITE_BUSY_SNAPSHOT` (a deferred
  transaction that read before another connection committed cannot then
  write), because no read happens inside the transaction before its first
  write. Units whose reads decide their writes say so with
  `immediate=True`; a plain `BEGIN` (deferred, began before the first
  read) is deliberately not offered, since it is the shape that does meet
  `SQLITE_BUSY_SNAPSHOT`.
- On normal exit commits through the existing busy/locked retry
  (`commit_with_retry`), then runs `after_commit` callbacks in registration
  order. On an exception rolls back, discards the callbacks, resets the
  Python-side transaction state (`_species_provenance_before`) and re-raises.
- `retry_busy=True` (jobs) retries a `BEGIN IMMEDIATE` with the same
  bounded backoff `execute_with_retry` uses after `busy_timeout` (30 s)
  expires.
  Routes keep the plain wait and surface a 503 or 500, as the decision lock
  does today: a request thread should not sit for minutes behind a scan.

Nested use (a unit of work is open):

- Issues `SAVEPOINT uow_<depth>`; `RELEASE` on normal exit, `ROLLBACK TO` +
  `RELEASE` on an exception, which then propagates. If the owner has not
  written yet it first issues `BEGIN IMMEDIATE` for the owner (a `SAVEPOINT`
  outside a transaction begins one that its own `RELEASE` would commit). `after_commit` callbacks
  registered inside a rolled-back savepoint are dropped; the
  `_species_provenance_before` snapshot is restored to its value at the
  savepoint (it records "state before first touch", and a savepoint rollback
  must not erase snapshots taken before it).

**Why savepoints rather than joining the outer transaction.** Joining (inner
blocks are no-ops) is simpler, but it is only correct if no caller ever
catches an inner failure and carries on. Vireo does that routinely: bulk
routes skip a photo whose write failed, the identity replay retires a stale
action and continues, duplicate resolution keeps earlier winners when one
group fails. With joining, each of those leaves half of the failed inner unit
applied inside a transaction that then commits. Savepoints give the right
answer in both cases (propagated: the owner rolls everything back; caught:
exactly the inner unit is undone), and the codebase already relies on that
behavior through the six ad-hoc savepoint helpers this API replaces. The cost
is one `SAVEPOINT`/`RELEASE` pair per nested block, which SQLite implements
as a journal marker.

### `db.after_commit(callback)`

Work that must only happen once the data is durable: pruning edit history
after a recorded edit (today `record_edit` prunes only when it committed, so
every `_commit=False` caller silently skips the prune), deleting cached files
for deleted photos, pipeline-cache invalidation, publishing a preview. Outside
a unit of work the callback runs immediately. A callback that raises is
logged and does not undo the committed transaction.

### `db.in_unit_of_work`

True while a `transaction()` block is open on this `Database`. Services that
must not run inside someone else's unit (slow filesystem work, a job that
commits per batch) assert it is false.

### What happens to the existing transaction-control façade

| Today | Target |
|---|---|
| `db.begin_immediate()` + `db.commit()`/`db.rollback()` | `with db.transaction(immediate=True):` |
| `with db.transaction(): db.begin_immediate()` and the reverse order | `with db.transaction(immediate=True):` |
| `db._commits_held()` | `with db.transaction():` |
| repository `commit()` methods, savepoint helpers | deleted; callers use `db.transaction()` |
| `db.commit_with_retry()` in jobs | `with db.transaction(retry_busy=True):` per batch |
| `db.begin()` + `db.rollback()` (read snapshots: site export, browse summary) | `with db.read_snapshot():` (deferred `BEGIN`, always rolled back) |
| `db.commit()` inside a unit of work | raises `TransactionOwnershipError` |

`db.commit()`, `db.rollback()`, `db.begin_immediate()` and `db.begin()` stay
during the migration for unconverted callers and are deleted when their last
caller is.

### During the migration

The migration is domain by domain, so for a while converted owners will call
unconverted callees that still commit. Two compatibility rules keep each
intermediate state atomic:

- **An open unit of work holds legacy commits.** While `transaction()` is
  open, `_Connection.commit()` is a no-op (today's `_commits_held`
  behavior, now attached to the unit of work instead of a private flag), and
  sqlite3's `with conn:` context (seven repository sites) becomes a savepoint
  instead of a C-level commit. So wrapping a caller in `db.transaction()`
  never splits it, even before its callees are converted.
- **An inner legacy rollback dooms the unit.** If an unconverted callee calls
  `rollback()` inside an open unit, the earlier work is gone; the unit
  records that, and if anything was written after the rollback its exit
  rolls that back too and raises `TransactionOwnershipError` instead of
  committing half a unit. Today the same sequence commits the later writes.
  A rollback with nothing written after it (a refused plan's `db.rollback();
  return`) just ends the unit rolled back, as it does today.

`_commits_held()` becomes an alias of `transaction()` in the first
foundation PR and is removed with its last caller; `_Connection` and the
hold are deleted in the last PR, when the ratchet's data-layer count is zero
and no callee commits.

Adopting an already open transaction: when an outermost plain
`transaction()` is entered while sqlite3 already has a transaction open (an
unconverted caller's implicit transaction from a `_commit=False` write, or a
legacy `begin_immediate()`), it adopts it, as `with conn:` and
`_commits_held()` do today. An outermost `immediate=True` block raises
instead, as the `begin_immediate()` it replaces does. Adoption is a
migration rule only; in the end state an open transaction outside a unit of
work is a bug the close-time report below catches.

**Close-time report.** `Database.close()` logs a warning, with the path,
when it discards an open transaction: that is a write some caller passed
`_commit=False` for and then never committed (it is silently lost today),
or a transaction an error path left open. `unit_of_work` calls any
registered listeners as well, and the test suite registers one, so a
conversion that forgets its owner fails in CI rather than losing writes.

## Durable boundaries in jobs

Long jobs commit per photo or per batch so progress survives a crash, a
cancel or a pause, and so the UI's writers are not locked out for the length
of a scan. Those boundaries stay exactly where they are; they move from
hidden commits inside data methods to visible blocks in the job:

```python
for item in batch:
    # Durable boundary: one catalogued file. A crash after this commit
    # resumes at the next file; the row and its keywords land together.
    with db.transaction(retry_busy=True):
        photo_id = photo_ingest.add_photo(db, ...)
        write_photo_columns(db, photo_id, item)
        import_keywords(db, photo_id, item)
```

Rules for converting a job:

1. Never commit less often than today in a way that loses crash-resume
   progress, and never wrap a loop over photos in one transaction: that
   holds the writer lock for the whole loop and stalls every UI write.
2. Never commit more often than today in a way that splits something that
   was atomic. Where today's hidden commit split a step that should be
   atomic (row inserted, keywords not yet), the conversion may join them,
   called out in the PR with a test.
3. Slow work (decode, hashing, model inference, file copies, network) stays
   outside the block. A block is the write phase only.
4. Filesystem effects keep their order relative to the commit. Where a file
   is copied before the catalog row commits (imports, moves), the block
   comes after the copy exactly where the old commit was; where a file is
   deleted only after the catalog no longer references it (deletes, cache
   eviction), the deletion moves into `after_commit` or stays after the
   block.

The deliberate multi-commit flows, with the boundary each keeps:

| Flow | Boundary | Today's commit point |
|---|---|---|
| Scan (`scanner/indexing.py`, `triage.py`, `companions.py`, `companion_*.py`, `folders.py`) | one file catalogued (row, columns, sidecar keywords, companion pairing); one folder status | `add_photo` inside `PhotoRepository.add`, plus 14 `commit_with_retry` calls in the scanner |
| Working copies (`scanner/working_copy_batch.py`, `working_copy_cache.py`) | one published or failed working copy; one eviction batch | `commit_with_retry` per row |
| Import (`import_job/*`, `services/imports.py`, `importer.py`) | one copied-and-catalogued photo; destination folder creation; twin visibility | `add_photo`, `_create_dest_folder`, `_grant_twin_visibility` |
| Move (`move/photos.py`, `move/folder_catalog.py`, `repositories/moves_merge.py`) | one photo repointed after its file copy (`photo_move_savepoint`); folder mtime corrections | commit inside `photo_move_savepoint`, `db.commit()` in `move.py` |
| Batch delete (`services/photo_deletion.py`, `catalog_deletion.py`) | one chunk of catalog rows removed after Trash succeeded | `remove_catalog_rows` |
| Card cleanup verify, audit verify-hashes (`card_cleanup.py`, `audit.py`) | one verified hash | `db.commit()` per hash |
| Thumbnails (`thumbnails.py`, `pipeline_stages/media.py`) | one flush of thumb paths | `_flush` / `_flush_thumb_paths` |
| Pipeline stages and classify (`pipeline_stages/*`, `classify_job/*`, `pipeline_job.py`) | one detection batch / one photo's masks / one classifier run release | `commit_with_retry` in stages, repository commits in `DetectionsRepository.write_batch`, `MasksFeaturesRepository` |
| Job history (`job_runner/history.py`) | one job state transition | `db.commit()` per transition |
| Local workspace / local folder staging (`services/local_workspace.py`, `local_folder.py`) | claim, stage, restore and discard, each its own unit around the file steps | `begin_immediate` + `commit` per phase |
| XMP sync (`sync.py`) | one photo's sidecar write and its queue rows | `_write_photo_sync`, `sync_from_xmp` |
| Migrations (`schema.py`) | one numbered migration | `BEGIN IMMEDIATE` ... `commit()` per migration; unchanged, not part of the façade |
| Startup repairs (`services/startup_tasks.py`, `species_identity_repair.py`, `label_source_identities.py`) | one repair pass with its marker | `with db.transaction()` / marker writes |
| Taxonomy load (`taxonomy.py`) | one bulk load | `db.commit()` after the load |

## Undo and redo without suppression

Today: `api_undo` takes the prediction-decision lock (`BEGIN IMMEDIATE`),
`Database.undo_last_edit()` replays the entry under `_commits_held()` (every
setter's commit is a no-op), and the hold's exit commits once. The two
cache-linked actions (`pipeline_grouping`, `species_confirm_cache`) retire a
stale entry, commit that retirement, and raise `GroupingHistoryStale`.

Target, once every setter the replay calls has stopped committing:

```python
def undo_last_edit(self) -> UndoResult | None:      # never commits
    found = history.next_undo()
    ...
    try:
        with self.transaction():                     # savepoint in the caller's unit
            self._apply_undo(entry, items)
            history.mark_undone(entry['id'])
    except GroupingHistoryStale:
        history.retire_stale_grouping_entry(entry['id'])
        return UndoResult(entry, stale=GROUPING_STALE_MESSAGE)
    return UndoResult(entry)
```

and in the route:

```python
with prediction_decisions.decision(db, json_error=json_error):
    result = db.undo_last_edit()
if result is not None and result.stale:
    return json_error(result.stale, 409)
```

The replay is one savepoint inside the route's unit: a failure part-way
rolls back exactly the replay, and the stale path commits the retirement in
the same unit, after the replay's savepoint rolled back, which is what the
separate `history.commit()` does today. The identity replay
(`_replay_identity_history`, `EditHistoryRepository.identity_replay`) is the
same shape and loses its private savepoint helper. Python-side snapshots
(`_species_provenance_before`) follow the savepoint as described above, which
replaces the `finally` reset in `_replay_identity_history`.

Because the replay now runs inside the caller's unit, `record_edit`'s
history prune moves to `after_commit`, so it runs only once the unit commits,
for `_commit=False` callers too.

## Prediction decisions

`begin_prediction_decision` and `under_prediction_decision_lock`, plus the
rollback at every early exit in `web/predictions.py` (34 `commit`/`rollback`
calls) become:

```python
@contextlib.contextmanager
def decision(db, *, json_error, prediction_ids=()) -> Iterator[None]:
    """Hold the writer lock across a decision's checks and writes."""
    lock_err = begin_prediction_decision(db, json_error=json_error)  # the contract name
    if lock_err is not None:
        raise DecisionRefused(lock_err)
    with db.transaction():                  # adopts the BEGIN IMMEDIATE above
        if prediction_ids and out_of_workspace_prediction_ids(db, prediction_ids):
            raise DecisionRefused(json_error("Prediction does not belong to the active workspace", 404))
        yield
```

`DecisionRefused` is a `werkzeug` `HTTPException` carrying its response, so
`raise DecisionRefused(json_error(..., 409))` anywhere in the block rolls the
unit back and Flask returns that response (the app's error handler already
passes `HTTPException` through). A route body becomes its checks and writes,
with no rollback bookkeeping. `test_every_prediction_decision_route_locks`
keeps working because `decision` reaches `begin_prediction_decision` in the
call graph; a new unit test pins that `decision` rolls back on a refused
check and on an exception and commits otherwise.

A context manager cannot see the response its block returns, and several
decision helpers *return* an error response after writing (a group apply
that finds a member gone, an accept whose keyword vanished). A returned 409
inside `with decision(...)` would commit those writes. So the routes go
through one wrapper built on the context manager, which judges the response
the way `services.edit_history.atomic_edit` already does. It keeps the
existing name, `under_prediction_decision_lock`, so the route-contract
analysis and the tests that patch the lock keep finding it:

```python
def under_prediction_decision_lock(db, work, *, json_error, prediction_ids=()):
    try:
        with decision(db, json_error=json_error, prediction_ids=prediction_ids):
            response = work()
            if status_of(response) >= 400:
                raise DecisionRefused(response)   # rolls the unit back
    except DecisionRefused as refused:
        return refused.response
    return response
```

A body that returns an error response therefore never commits, without each
early exit having to remember a rollback.

## Write locks and busy handling

- `transaction(immediate=True)` replaces every explicit
  `begin_immediate()`. Its wait is the connection's `busy_timeout` (30 s), as
  today. The decision lock keeps reporting a 503 when it cannot be taken.
  A plain unit takes the lock at its first write, as sqlite3 does today, and
  waits the same `busy_timeout` there.
- The commit at the end of a unit goes through `commit_with_retry`. Holding
  the writer lock from `BEGIN IMMEDIATE` makes a busy commit rare, but the
  retry costs nothing when it does not fire.
- `execute_with_retry` stays for statements that run outside a unit (a few
  scan writes). Inside a unit it never sees "locked": the lock is already held.
- Jobs use `retry_busy=True` so a long UI write does not abort a scan's next
  batch, which is the case `execute_with_retry` was added for.
- An `immediate=True` unit must not span slow work: it holds the writer
  lock from its first statement. Conversions keep preparation (file reads,
  hashing, model calls, network) before the block, or use a plain unit,
  which takes the lock at its first write.

## Migration order

Each step is one stacked PR that lowers the ratchet. Domains are ordered so
that a domain is converted after the domains it calls and before undo/redo,
which replays almost all of them.

1. **Design and ratchet** (this PR): this document and
   `test_transaction_ownership.py`, capping commit flags (335) and data-layer
   transaction control (234) per module, both ways.
2. **Foundation**: the `transaction()` semantics above (first-write or
   `immediate` begin, savepoint nesting, legacy-commit hold,
   doomed-on-inner-rollback, `after_commit`, `retry_busy`), `_commits_held`
   deleted and its callers and the `begin_immediate`/`transaction` pairings
   converted, the close-time report. Transaction tests for each rule.
3. **Prediction decisions**: the `decision` context manager and the twelve
   routes in `PREDICTION_DECISION_ROUTES`.
4. **Leaf domains** with plain committing writes and few callers: meta,
   processes, photo labels, collections, edit presets and recipes, caches,
   model runs, iNat, job history, pending archives, exif search, keyword
   taxa, workspaces.
5. **Photo review and the sidecar queue**: `photo_review`, `sync`,
   `services/pending_changes.py` (flags, ratings, `queue_change`).
6. **Keywords**: `keywords`, `keyword_tree`, `photo_keywords`,
   `keyword_provenance` (`tag_photo`, `untag_photo`), keeping
   `test_keyword_provenance_contract` meaningful, then `keyword_identity.py`.
7. **Locations**: `locations` and its savepoint helper.
8. **Predictions and species**: `predictions`, `species_curation`,
   `species_provenance`, `identity_alternatives`, `grouping_history`,
   `photo_review` service, highlights.
9. **Catalog structure** (photo-safety invariants at the PR and nightly
   profiles): `photos` (`add_photo`), `folders`, `workspace_folders`,
   `moves_merge`, `duplicates`, `detections`, `masks_features`, with the job
   boundaries in the table above made explicit.
10. **Undo/redo**: the shape above; delete `_commits_held`.
11. **Delete the hold**: `_Connection`, `_commit_holds`, the `with conn`
    shim; `db.commit()` and friends deleted with their last caller.

## Ratchet

`vireo/tests/test_transaction_ownership.py` holds two per-module counts at
exactly their current values (both ways, like `test_sql_boundary.py`'s
connection caps): a count may not grow, and when it shrinks the cap must be
lowered in the same change. Run the file as a script to print current
counts.

- `COMMIT_FLAG_LIMITS`: `_commit`/`commit` parameters, keyword arguments by
  those names, and `*_no_commit` functions, in every module under `vireo/`
  outside the tests and harness. Starting total: 335.
- `DATA_LAYER_TRANSACTION_CONTROL_LIMITS`: in `vireo/db.py` and
  `vireo/repositories/`, calls to `commit`, `rollback`, `commit_with_retry`,
  `begin`, `begin_immediate`, `transaction` and `_commits_held`, raw
  `BEGIN`/`COMMIT`/`END`/`ROLLBACK`/`SAVEPOINT`/`RELEASE` statements (not
  `ROLLBACK TO`, which does not end the savepoint), and `with conn:` blocks,
  excluding the bodies of `Database`'s transaction-control methods. Starting
  total: 234.

The end state is both dictionaries empty.

## Risks

- **Lock held longer.** An `immediate=True` unit holds the writer lock
  from its first statement, and any unit holds it from its first write to
  its end. A conversion that moves slow work inside that span stalls other
  writers; reviewers check that each block's locked span is the write phase
  only.
- **Lost writes from a missing owner.** A converted callee called with no
  unit open writes into an implicit transaction nobody commits. The
  close-time check reports it, and the converted domain's tests run with it
  as a failure.
- **Ordering with the filesystem.** Imports, moves and deletes interleave
  file operations with commits; each conversion keeps the commit point
  where it was relative to the file step and runs the photo-safety
  invariants at both profiles.

## Appendix: inventory on 2026-10-25 (`origin/main` at `dba5c2033`)

Every place outside `Database`'s transaction-control methods that commits,
rolls back, begins a transaction, takes a savepoint, holds commits, or
passes a commit flag, grouped by layer then module, one line per function
(`x2` = two occurrences). "Job" covers the scanner, pipeline stages and job
runner; the job bodies under `import_job/`, `move/`, `classify_job/` and the
top-level helpers are listed under "Other". XMP `SidecarEditor.commit` and
the directory-listing `begin()` passes are not catalog transactions and are
omitted.

### Repositories (`vireo/repositories/`)

- `vireo/repositories/audit.py`
  - `AuditRepository.record_run`: commit()
  - `AuditRepository.update_photo_hash_check`: commit(), param commit
- `vireo/repositories/caches.py`
  - `CachesRepository.offline_original_delete`: commit_with_retry(), param _commit
  - `CachesRepository.offline_original_upsert`: commit_with_retry()
  - `CachesRepository.paired_preview_delete`: commit()
  - `CachesRepository.paired_preview_touch`: commit()
  - `CachesRepository.preview_clear_all`: commit()
  - `CachesRepository.preview_delete`: commit()
  - `CachesRepository.preview_delete_entries`: commit()
  - `CachesRepository.preview_insert`: commit()
  - `CachesRepository.preview_touch`: commit()
- `vireo/repositories/collections.py`
  - `CollectionRepository._insert`: commit()
  - `CollectionRepository.create_defaults`: commit()
  - `CollectionRepository.delete`: commit()
  - `CollectionRepository.duplicate`: commit()
  - `CollectionRepository.rename`: commit()
  - `CollectionRepository.update`: commit()
  - `prune_collection_ids_of_missing_photos`: commit(), param commit
- `vireo/repositories/detections.py`
  - `DetectionsRepository.clear`: commit()
  - `DetectionsRepository.clear_miss_flag`: commit()
  - `DetectionsRepository.commit`: commit()
  - `DetectionsRepository.save`: commit_with_retry()
  - `DetectionsRepository.write_batch`: commit_with_retry(), rollback()
- `vireo/repositories/duplicates.py`
  - `DuplicatesRepository.begin_review_write`: SQL BEGIN
  - `DuplicatesRepository.reopen`: with conn
- `vireo/repositories/edit_history.py`
  - `EditHistoryRepository.commit`: commit()
  - `EditHistoryRepository.identity_replay`: SQL BEGIN, SQL RELEASE, SQL ROLLBACK, SQL SAVEPOINT
  - `EditHistoryRepository.mark_redone`: commit()
  - `EditHistoryRepository.mark_undone`: commit()
  - `EditHistoryRepository.prune`: SQL BEGIN, SQL RELEASE x2, SQL ROLLBACK, SQL SAVEPOINT, commit(), rollback()
  - `EditHistoryRepository.record`: commit(), param _commit
  - `EditHistoryRepository.reject_accept_siblings`: commit()
  - `EditHistoryRepository.undo_prediction_accept_statuses`: commit()
- `vireo/repositories/edits.py`
  - `EditsRepository.clear_photo_recipe`: commit()
  - `EditsRepository.delete_preset`: commit()
  - `EditsRepository.save_preset`: commit()
  - `EditsRepository.set_photo_recipe`: commit() x2, param _commit
- `vireo/repositories/folders.py`
  - `FolderRepository._merge_into_existing`: commit(), param commit
  - `FolderRepository.add`: commit_with_retry() x2
  - `FolderRepository.check_health`: commit()
  - `FolderRepository.delete`: commit(), commit=False, rollback()
  - `FolderRepository.merge_into_existing`: commit(), commit=False, param commit
  - `FolderRepository.relocate`: commit() x2
  - `FolderRepository.repair_missing_parents`: commit()
  - `FolderRepository.repair_stale_parents`: SQL BEGIN, with conn
  - `FolderRepository.update_counts`: commit()
- `vireo/repositories/identity_alternatives.py`
  - `IdentityAlternativesRepository.begin_batch`: SQL BEGIN
  - `_repair_identity_merge_sidecar`: _commit=False
  - `identity_merge_transaction`: SQL BEGIN, SQL RELEASE, SQL ROLLBACK, SQL SAVEPOINT, rollback()
- `vireo/repositories/imports.py`
  - `ImportRepository.record_recovered_companion_identities`: SQL RELEASE, SQL ROLLBACK, SQL SAVEPOINT
- `vireo/repositories/inat.py`
  - `InatRepository.record_submission`: commit()
- `vireo/repositories/job_history.py`
  - `JobHistoryRepository.set_result`: commit()
- `vireo/repositories/keyword_provenance.py`
  - `KeywordProvenanceRepository._accept_target_row`: _commit=False x3
  - `KeywordProvenanceRepository.accept_prediction`: _commit=False, commit(), param _commit, rollback()
  - `KeywordProvenanceRepository.link_keyword_to_place`: with conn
  - `KeywordProvenanceRepository.tag`: commit(), param _commit
  - `_migrate_replaced_species_curation`: _commit=False x2
  - `_queue_replaced_species_removals`: _commit=False x2
  - `_retarget_species_curation`: _commit=False x2
- `vireo/repositories/keyword_taxa.py`
  - `KeywordTaxonRepository.mark_species`: commit()
- `vireo/repositories/keyword_tree.py`
  - `KeywordTreeRepository.apply_update`: commit()
  - `KeywordTreeRepository.commit`: commit()
  - `KeywordTreeRepository.delete`: commit()
  - `KeywordTreeRepository.merge_duplicates`: commit(), rollback()
  - `KeywordTreeRepository.rename_dependents`: _commit=False x2
- `vireo/repositories/keywords.py`
  - `KeywordRepository._backfill_candidate_taxon`: commit(), param _commit
  - `KeywordRepository._insert_new_keyword`: SQL BEGIN, _commit=_commit x2, commit() x2, param _commit, rollback() x2
  - `KeywordRepository._promote_candidate_type`: commit() x2, param _commit
  - `KeywordRepository.add`: param _commit
  - `KeywordRepository.add_source_species`: commit(), param _commit
  - `KeywordRepository.ensure_default_genres`: commit()
- `vireo/repositories/locations.py`
  - `LocationRepository.clear_photo`: transaction()
  - `LocationRepository.fill_missing_coordinates`: commit()
  - `LocationRepository.queue_changes_for_tagged_photos`: _commit=False, commit()
  - `LocationRepository.repair_misclassified_ancestors`: commit()
  - `LocationRepository.reverse_geocode_cache_put`: with conn
  - `LocationRepository.transaction`: SQL RELEASE x2, SQL ROLLBACK, SQL SAVEPOINT, commit(), rollback()
- `vireo/repositories/masks_features.py`
  - `MasksFeaturesRepository.delete_for_variant`: commit_with_retry()
  - `MasksFeaturesRepository.delete_inactive`: commit_with_retry()
  - `MasksFeaturesRepository.delete_stale`: commit_with_retry()
  - `MasksFeaturesRepository.save_subject_raw_analysis`: commit_with_retry(), param _commit
  - `MasksFeaturesRepository.set_active_variant`: commit_with_retry(), param _commit
  - `MasksFeaturesRepository.update_embeddings`: commit_with_retry(), param _commit
  - `MasksFeaturesRepository.update_pipeline_features`: commit_with_retry(), param _commit
  - `MasksFeaturesRepository.upsert_embedding`: commit()
  - `MasksFeaturesRepository.upsert_mask`: commit_with_retry(), param _commit
- `vireo/repositories/meta.py`
  - `MetaRepository.set`: commit(), param _commit
- `vireo/repositories/model_runs.py`
  - `ModelRunsRepository.record_classifier_match_score`: commit_with_retry()
  - `ModelRunsRepository.record_classifier_run`: commit_with_retry()
  - `ModelRunsRepository.record_detector_run`: commit()
  - `ModelRunsRepository.upsert_labels_fingerprint`: commit()
- `vireo/repositories/moves_merge.py`
  - `MovesMergeRepository.batch_update_photo_folder`: commit()
  - `MovesMergeRepository.create_rule`: commit()
  - `MovesMergeRepository.delete_rule`: commit()
  - `MovesMergeRepository.merge_staged_tree_into_archive`: commit(), rollback()
  - `MovesMergeRepository.move_folder_path`: commit()
  - `MovesMergeRepository.photo_move_savepoint`: SQL RELEASE x2, SQL ROLLBACK, SQL SAVEPOINT, commit()
  - `MovesMergeRepository.touch_rule`: commit()
  - `MovesMergeRepository.update_rule`: commit()
- `vireo/repositories/pending_archives.py`
  - `PendingArchiveRepository.delete`: commit()
  - `PendingArchiveRepository.register`: commit()
  - `PendingArchiveRepository.set_state`: commit()
- `vireo/repositories/photo_keywords.py`
  - `PhotoKeywordRepository.record_embedded_offered_keys`: commit(), param _commit
  - `PhotoKeywordRepository.untag`: commit(), param _commit
- `vireo/repositories/photo_labels.py`
  - `PhotoLabelRepository.remove`: commit()
  - `PhotoLabelRepository.set`: commit()
  - `PhotoLabelRepository.set_description`: commit()
  - `PhotoLabelRepository.set_many`: commit()
- `vireo/repositories/photo_review.py`
  - `PhotoReviewRepository._set_many`: commit(), param _commit, rollback()
  - `PhotoReviewRepository.confirm_duplicate_choices`: _commit=False
  - `PhotoReviewRepository.flag_ranked_candidate`: commit(), param _commit
  - `PhotoReviewRepository.review_write`: SQL BEGIN, SQL RELEASE x2, SQL ROLLBACK, SQL SAVEPOINT, commit(), rollback() x2
  - `PhotoReviewRepository.set_flag`: commit(), param _commit
  - `PhotoReviewRepository.set_flags`: _commit=_commit, param _commit
  - `PhotoReviewRepository.set_rating`: commit(), param _commit
  - `PhotoReviewRepository.set_ratings`: _commit=_commit, param _commit
  - `PhotoReviewRepository.set_wildlife_excluded`: commit(), param _commit
- `vireo/repositories/photos.py`
  - `PhotoRepository.add`: commit_with_retry()
  - `PhotoRepository.clear_all_thumb_paths`: commit()
  - `PhotoRepository.clear_thumb_paths`: commit()
  - `PhotoRepository.clear_working_copy_evictions`: commit_with_retry()
  - `PhotoRepository.delete`: commit(), param commit, rollback()
  - `PhotoRepository.get_browse_summary`: SQL RELEASE, SQL ROLLBACK, SQL SAVEPOINT
  - `PhotoRepository.record_generated_original`: commit()
  - `PhotoRepository.set_thumb_path`: commit()
  - `PhotoRepository.update_quality`: commit_with_retry()
  - `PhotoRepository.update_sharpness`: commit_with_retry()
- `vireo/repositories/pipeline_features.py`
  - `PipelineFeaturesRepository._replace_temp_id_scope`: commit(), rollback()
- `vireo/repositories/predictions.py`
  - `PredictionRepository.accept_subject_species`: _commit=False, commit(), param _commit, rollback()
  - `PredictionRepository.add`: commit()
  - `PredictionRepository.clear`: commit() x2
  - `PredictionRepository.clear_group_info`: commit_with_retry()
  - `PredictionRepository.reconcile_match_review_state`: commit_with_retry()
  - `PredictionRepository.retain_candidates`: commit()
  - `PredictionRepository.set_review_status`: commit()
  - `PredictionRepository.ungroup`: commit(), param _commit
  - `PredictionRepository.update_group_info`: commit_with_retry()
  - `PredictionRepository.update_status`: commit(), param _commit
  - `PredictionRepository.update_status_by_photo`: commit(), param _commit
- `vireo/repositories/processes.py`
  - `ProcessesRepository.create`: commit()
  - `ProcessesRepository.delete`: commit()
  - `ProcessesRepository.update`: commit()
- `vireo/repositories/species_curation.py`
  - `SpeciesCurationRepository.add_highlight`: commit() x2, param _commit
  - `SpeciesCurationRepository.clear_photo_preference`: commit(), param _commit
  - `SpeciesCurationRepository.clear_representative`: commit(), param _commit
  - `SpeciesCurationRepository.move_highlight`: commit(), param _commit
  - `SpeciesCurationRepository.promote_highlight`: commit(), param _commit
  - `SpeciesCurationRepository.remove_highlight`: commit(), param _commit
  - `SpeciesCurationRepository.rename_highlights`: commit(), param _commit
  - `SpeciesCurationRepository.rename_photo_preferences`: _commit=False, commit() x2, param _commit
  - `SpeciesCurationRepository.rename_representatives`: commit() x2, param _commit
  - `SpeciesCurationRepository.set_photo_preference`: commit(), param _commit
- `vireo/repositories/stats.py`
  - `StatsRepository.stage_scope_ids`: SQL RELEASE, SQL ROLLBACK, SQL SAVEPOINT
- `vireo/repositories/sync.py`
  - `SyncRepository.claim_for_sync`: with conn
  - `SyncRepository.clear_equivalent_flat_removals`: commit(), param _commit
  - `SyncRepository.commit`: commit()
  - `SyncRepository.queue`: commit(), param _commit
  - `SyncRepository.remove_token`: commit()
- `vireo/repositories/working_copies.py`
  - `WorkingCopyRepository.load_scope`: commit_with_retry(), rollback()
- `vireo/repositories/workspace_folders.py`
  - `WorkspaceFolderRepository.add_exact`: commit()
  - `WorkspaceFolderRepository.commit`: commit()
  - `WorkspaceFolderRepository.link_descendants`: commit()
  - `WorkspaceFolderRepository.mark_roots`: commit()
  - `WorkspaceFolderRepository.move_folders`: commit(), rollback()
  - `WorkspaceFolderRepository.prune_nonroot_links_outside_roots`: commit()
  - `WorkspaceFolderRepository.remove`: commit()
  - `WorkspaceFolderRepository.remove_tree`: commit()
- `vireo/repositories/workspaces.py`
  - `WorkspaceRepository._write`: commit()
  - `WorkspaceRepository.create`: with conn
  - `WorkspaceRepository.create_new_images_snapshot`: commit()
  - `WorkspaceRepository.delete`: with conn
  - `WorkspaceRepository.forget_label_file`: commit()
  - `WorkspaceRepository.set_group_state`: commit()
  - `WorkspaceRepository.update`: commit()

### `Database` (`vireo/db.py`)

- `vireo/db.py`
  - `Database._add_source_species_keyword`: _commit=_commit, param _commit
  - `Database._apply_winner_loser_merge`: _commit=False, transaction()
  - `Database._carry_keyword_associations_for_merge`: _commit=False x2
  - `Database._commits_held`: commit(), rollback()
  - `Database._edit_set_flag`: _commit=False x2
  - `Database._edit_set_photo_location`: _commit=False x2
  - `Database._edit_set_prediction_status`: _commit=False
  - `Database._move_location_state_for_merge`: _commit=False
  - `Database._reject_edit_prediction`: _commit=False
  - `Database._replay_identity_history`: _commits_held()
  - `Database._restore_edit_prediction_status`: _commit=False
  - `Database.accept_prediction`: _commit=_commit, param _commit
  - `Database.accept_subject_species`: _commit=_commit, param _commit
  - `Database.add_keyword`: _commit=_commit, param _commit
  - `Database.begin`: SQL BEGIN
  - `Database.begin_immediate`: SQL BEGIN
  - `Database.commit`: commit()
  - `Database.commit_with_retry`: commit_with_retry()
  - `Database.get_or_create_text_location`: transaction()
  - `Database.merge_taxonomy_root_twins`: begin_immediate(), transaction()
  - `Database.prune_collection_ids_of_missing_photos`: commit=commit_with_retry
  - `Database.queue_change`: _commit=_commit, param _commit
  - `Database.queue_flag_change_if_enabled`: _commit=False x2, commit() x3, param _commit
  - `Database.record_edit`: _commit=_commit, param _commit
  - `Database.redo_last_undo`: _commits_held(), commit() x2
  - `Database.remove_pending_changes`: _commit=False, commit(), param _commit
  - `Database.rename_photo_preferences_species`: _commit=_commit, param _commit
  - `Database.rename_species_highlights_species`: _commit=_commit, param _commit
  - `Database.rollback`: rollback()
  - `Database.set_photo_edit_recipe`: _commit=_commit, param _commit
  - `Database.set_photo_location`: _commit=False, transaction()
  - `Database.set_species_representative`: _commit=_commit, param _commit
  - `Database.tag_photo`: _commit=False, commit(), param _commit
  - `Database.transaction`: with conn
  - `Database.undo_last_edit`: _commits_held(), commit() x2
  - `Database.ungroup_prediction`: _commit=_commit, param _commit
  - `Database.untag_photo`: _commit=_commit, param _commit
  - `Database.update_keyword`: commit()
  - `Database.update_photo_wildlife_excluded`: _commit=_commit, param _commit
  - `Database.update_prediction_status`: _commit=_commit, param _commit
  - `Database.update_predictions_status_by_photo`: _commit=_commit, param _commit
  - `Database.upsert_place_chain`: transaction()
  - `_Connection.commit`: commit()
  - `commit_with_retry`: commit()

### Services (`vireo/services/`)

- `vireo/services/analysis_retry.py`
  - `_publish_features`: _commit=False x4, begin_immediate(), commit_with_retry(), rollback() x2
  - `_refresh_interrupted`: begin_immediate(), commit(), rollback()
  - `_refresh_review`: begin_immediate(), commit(), rollback()
- `vireo/services/bulk_species_assignments.py`
  - `apply_assignments`: _commit=False x3
- `vireo/services/catalog_deletion.py`
  - `delete_photos`: commit=commit, param commit
- `vireo/services/collection_coverage.py`
  - `collection_coverage`: begin(), rollback()
- `vireo/services/detection_store.py`
  - `delete_detections_by_ids`: commit()
- `vireo/services/duplicate_resolution.py`
  - `apply_duplicate_resolution`: begin_immediate(), rollback() x2, transaction()
  - `bulk_resolve_by_folder`: begin_immediate(), rollback(), transaction()
  - `keep_duplicate_copy`: begin_immediate(), rollback() x2
- `vireo/services/edit_history.py`
  - `atomic_edit.decorate.wrapped`: _commits_held(), rollback()
  - `change_photo_location`: _commit=False, begin_immediate()
- `vireo/services/grouping_history.py`
  - `apply_grouping_photo_edit`: _commit=False x10
  - `record_grouping_edit`: _commit=False, commit(), param _commit
  - `record_species_confirm_cache`: _commit=False
  - `restore_grouping_edit`: rollback()
  - `restore_species_confirm_cache_edit`: rollback()
  - `save_grouping_edit`: commit(), rollback()
- `vireo/services/identity_alternatives.py`
  - `_queue_shared_keyword`: _commit=False x2
  - `prepare_photo_merge`: _commit=False
  - `replay_identity_edit`: _commit=False x2
  - `resolve_alternatives`: _commit=False x2
  - `save_alternatives`: _commit=False
- `vireo/services/import_locations.py`
  - `_apply_choice`: _commit=False, _commits_held(), begin_immediate()
  - `apply_location_choices`: rollback()
- `vireo/services/import_photos.py`
  - `_ImportPhotosJob._chain_after_import`: commit()
- `vireo/services/imports.py`
  - `ImportService._add_import_locations`: rollback()
  - `ImportService._add_import_tag_keywords`: _commit=False x3, commit(), rollback() x2
  - `ImportService._queue_import_keyword_add`: _commit=commit x2, param commit
  - `ImportService._queue_import_location_sync`: _commit=commit x2, param commit
- `vireo/services/local_folder.py`
  - `_claim_staging`: begin_immediate(), commit(), rollback()
  - `_materialize_ancestor_workspaces`: commit()
  - `_restore_catalog`: begin_immediate(), commit(), commit=False x2, rollback()
  - `discard_folder`: commit()
  - `stage_folder`: begin_immediate(), commit() x2, rollback()
  - `sync_folder`: commit()
- `vireo/services/local_workspace.py`
  - `_restore_catalog`: begin_immediate(), commit(), commit=False x2, rollback()
  - `discard_local`: commit()
  - `stage_workspace`: begin_immediate() x2, commit() x3, rollback() x2
  - `sync_back`: commit()
- `vireo/services/maintenance_history.py`
  - `clean_keywords_with_history`: _commits_held(), begin_immediate()
  - `remove_orphans_with_history`: _commits_held(), begin_immediate(), commit=False
  - `resolve_drift_with_history`: _commits_held(), begin_immediate()
- `vireo/services/mask_extraction.py`
  - `_MaskExtractionRun._refresh_primary`: commit_with_retry()
  - `_MaskExtractionRun._store_mask`: _commit=False x4, commit_with_retry()
- `vireo/services/pending_changes.py`
  - `clear_pending`: _commit=False, commit(), param _commit
  - `clear_pending_by_token`: _commit=False, commit()
  - `queue_keyword_add`: _commit=_commit x3, param _commit
  - `queue_keyword_remove`: _commit=_commit x2, param _commit
  - `queue_location_sync_if_enabled`: _commit=_commit x2, param _commit
- `vireo/services/photo_deletion.py`
  - `_BatchDeleteRun.remove_catalog_rows`: begin_immediate(), commit(), commit=False, rollback()
- `vireo/services/photo_review.py`
  - `PhotoReviewService._write_flags`: _commit=False x3
  - `PhotoReviewService._write_ratings`: _commit=False x3
  - `PhotoReviewService.apply_culling_flags`: _commit=False x3
- `vireo/services/prediction_decisions.py`
  - `begin_prediction_decision`: begin_immediate(), commit()
  - `under_prediction_decision_lock`: rollback()
- `vireo/services/render_cache.py`
  - `RenderCache._drop_stale_previews`: commit=False x4
  - `RenderCache.clear_preview_cache_invalid`: commit(), param commit
  - `RenderCache.invalidate_photo_render_cache`: commit()
  - `RenderCache.mark_preview_cache_invalid`: commit=commit, param commit
  - `queue_edit_recipe_sync`: _commit=False x2, commit(), param _commit
- `vireo/services/review_group_changes.py`
  - `apply_group_changes`: _commit=False x2
- `vireo/services/same_shot_reviews.py`
  - `record_same_shot_review`: transaction()
- `vireo/services/scan_work.py`
  - `build_scan_work.work`: begin()
- `vireo/services/source_frame_preview.py`
  - `_SourceFrame.publish`: begin_immediate(), transaction()
- `vireo/services/startup_tasks.py`
  - `StartupTasks.backfill_embedded_keywords`: rollback()
  - `StartupTasks.prune_collection_ids_of_missing_photos`: rollback()
- `vireo/services/subject_masks.py`
  - `set_active_mask_variant`: param _commit
- `vireo/services/workspace_folders.py`
  - `add_workspace_folder`: commit()

### Routes (`vireo/web/`)

- `vireo/web/batch.py`
  - `_finish_batch_location`: _commit=False, commit()
  - `create_batch_blueprint.api_batch_best_batch_flags`: _commit=False x3
  - `create_batch_blueprint.api_batch_keyword`: _commit=False x3, transaction()
  - `create_batch_blueprint.api_batch_set_photo_locations_from_exif`: _commit=False, commit()
  - `create_batch_blueprint.api_batch_wildlife_excluded`: _commit=False x2, commit()
- `vireo/web/browse.py`
  - `create_browse_blueprint.api_browse_init`: begin(), rollback() x2
- `vireo/web/collections.py`
  - `create_collections_blueprint.api_collection_photos`: begin(), rollback()
- `vireo/web/encounters.py`
  - `_SpeciesConfirmation._untag_old_rows`: _commit=False x2
  - `_SpeciesConfirmation.record_photo_edit`: _commit=False x2
  - `_SpeciesConfirmation.resolve_target_keyword`: _commit=False
  - `_SpeciesConfirmation.tag_new_species`: _commit=False x2
  - `_commit_species_edit`: commit(), rollback()
- `vireo/web/folders.py`
  - `create_folders_blueprint.api_folders`: begin(), rollback()
  - `create_folders_blueprint.api_folders_check_health`: begin(), rollback()
  - `create_folders_blueprint.api_folders_missing`: begin(), rollback()
- `vireo/web/highlights.py`
  - `_HighlightsRelabel._retag_photo`: _commit=False x5
  - `_HighlightsRelabel.record_relabel_edit`: _commit=False
  - `_HighlightsRelabel.rename_curation`: _commit=False x3
  - `_accept_confirm_targets`: _commit=False x2
  - `create_highlights_blueprint._highlights_confirm_under_lock`: commit(), rollback()
  - `create_highlights_blueprint._highlights_relabel_under_lock`: _commit=False, commit(), rollback()
  - `create_highlights_blueprint.api_photo_preferences_set`: _commit=False x2, commit(), rollback()
- `vireo/web/identity_alternatives.py`
  - `create_identity_alternatives_blueprint._write`: _commits_held()
- `vireo/web/job_launchers.py`
  - `_FullResolutionPreparation._cache_and_warm`: rollback()
  - `_FullResolutionPreparation._settle_photo`: _commit=False, begin_immediate(), transaction()
  - `_run_offline_cache`: rollback()
- `vireo/web/jobs.py`
  - `create_jobs_blueprint.api_job_download_taxonomy.work`: rollback() x2
- `vireo/web/keywords.py`
  - `_queue_location_path_changes`: _commit=False, commit()
  - `create_keywords_blueprint.api_delete_keyword`: _commit=False
- `vireo/web/locations.py`
  - `_record_place_link`: _commit=False, commit()
  - `create_locations_blueprint.api_resolve_location_discrepancies`: _commit=False x2, begin_immediate(), commit(), rollback() x5
- `vireo/web/media_common.py`
  - `_preparation_publication`: begin_immediate(), transaction()
- `vireo/web/photo_edit_recipes.py`
  - `create_photo_edit_recipes_blueprint.api_apply_photo_edit_recipe_bulk`: _commit=False x3, transaction()
- `vireo/web/photos.py`
  - `create_photos_blueprint.api_photos_query`: begin() x2, rollback()
  - `create_photos_blueprint.api_photos_query._release_focus_snapshot`: rollback()
- `vireo/web/pipeline.py`
  - `create_pipeline_blueprint._apply_group_zone_flags`: _commit=False
  - `create_pipeline_blueprint._persist_group_apply`: _commit=False x2, commit(), rollback()
  - `create_pipeline_blueprint.api_pipeline_active_mask_variant`: _commit=False, commit_with_retry()
  - `create_pipeline_blueprint.grouping_edit.wrapped`: begin_immediate(), rollback()
- `vireo/web/predictions.py`
  - `create_predictions_blueprint._accept_group_pick_rows`: _commit=False x2
  - `create_predictions_blueprint._accept_submitted_predictions`: _commit=False, rollback()
  - `create_predictions_blueprint._apply_prediction_group_decisions`: _commit=False x2, commit()
  - `create_predictions_blueprint._batch_accept_under_lock`: _commit=False, commit()
  - `create_predictions_blueprint._batch_reject_under_lock`: _commit=False, commit()
  - `create_predictions_blueprint._extend_accept_to_selection`: _commit=False x4
  - `create_predictions_blueprint._record_group_flag_history`: _commit=False x2
  - `create_predictions_blueprint._record_group_pick_accepts`: _commit=False
  - `create_predictions_blueprint._reject_prediction_row.apply`: _commit=False
  - `create_predictions_blueprint._write_group_flags_and_species`: _commit=False x6
  - `create_predictions_blueprint.api_accept_prediction`: _commit=False x2, commit(), rollback() x3
  - `create_predictions_blueprint.api_accept_subject_species`: _commit=False x2, commit(), rollback() x4
  - `create_predictions_blueprint.api_batch_accept_predictions`: rollback() x3
  - `create_predictions_blueprint.api_batch_reject_predictions`: rollback()
  - `create_predictions_blueprint.api_detection_evidence_review.save`: commit()
  - `create_predictions_blueprint.api_mark_prediction_reviewed`: _commit=False x2, commit(), rollback() x4
  - `create_predictions_blueprint.api_reject_prediction`: _commit=False, commit(), rollback() x4
  - `create_predictions_blueprint.api_replace_species_keywords_with_prediction`: _commit=False x2, commit(), rollback() x5
- `vireo/web/species.py`
  - `create_species_blueprint.api_species_provenance`: _commit=False, begin_immediate(), commit(), rollback()
- `vireo/web/storage.py`
  - `create_storage_blueprint.api_storage_retained_clear.work`: begin_immediate(), transaction()
- `vireo/web/sync.py`
  - `create_sync_blueprint.api_sync_discard`: _commit=False x4, begin_immediate() x2, commit() x2, rollback() x3
- `vireo/web/web_types.py`
  - `ClearPreviewCacheInvalid.__call__`: param commit
- `vireo/web/workspaces.py`
  - `create_workspace_blueprint.api_move_workspace_folders`: commit()
  - `create_workspace_blueprint.api_remove_workspace_folder`: commit()
  - `create_workspace_blueprint.api_update_workspace`: rollback()

### Jobs (`vireo/scanner/`, `vireo/pipeline_stages/`, `vireo/job_runner/`, `pipeline_job.py`)

- `vireo/job_runner/history.py`
  - `JobHistory.cancel_queued_history_row`: commit()
  - `JobHistory.flip_queued_row_to_running`: commit()
  - `JobHistory.insert_queued_row`: commit()
  - `JobHistory.persist_job`: commit()
  - `JobHistory.record_job_started`: commit()
  - `JobHistory.startup_sweep`: commit()
  - `JobHistory.write_checkpoints`: commit()
- `vireo/pipeline_job.py`
  - `_retry_thumbnail_with_companion`: commit_with_retry()
  - `_retry_thumbnail_with_working_copy`: commit_with_retry()
  - `_rollback_failed_mask_photo`: rollback()
- `vireo/pipeline_stages/detection.py`
  - `_DetectPass._ensure_full_image_anchor`: commit()
- `vireo/pipeline_stages/features.py`
  - `_MaskPass._store_mask`: _commit=False x5, commit_with_retry()
  - `_MaskPass._sync_primary_subject`: commit_with_retry()
- `vireo/pipeline_stages/media.py`
  - `_ThumbPass._flush_thumb_paths`: commit_with_retry()
- `vireo/scanner/companion_attach.py`
  - `_CompanionGroups._attach_companion`: _commits_held(), rollback()
  - `_CompanionGroups._publish_attached_companion`: _commits_held()
- `vireo/scanner/companion_keep.py`
  - `_CompanionFills.refill_owners_from_companions`: commit_with_retry()
  - `_KnownCompanionKeeper._keep_locked_companion`: commit_with_retry()
  - `_KnownCompanionKeeper.finish_known_companion`: _commits_held()
  - `_KnownCompanionKeeper.import_companion_sidecar`: commit_with_retry()
  - `_KnownCompanionKeeper.keep_known_companion`: _commits_held()
- `vireo/scanner/companions.py`
  - `_defer_recipe_snapshot_cleanup._apply_recipe_transfer_fs`: begin_immediate()
  - `_pair_raw_jpeg_companions`: commit_with_retry() x2
- `vireo/scanner/folders.py`
  - `_ScanFolders.update_folder_status`: commit_with_retry()
- `vireo/scanner/indexing.py`
  - `_CollectionMembership.drop_inherited_collection_membership`: commit_with_retry()
  - `_FileIndexer._add_photo`: _commits_held()
  - `_FileIndexer._heal_changed_content`: commit_with_retry()
  - `_FileIndexer._write_photo_columns`: commit_with_retry()
  - `_FileIndexer.catalog_file`: commit_with_retry() x2
- `vireo/scanner/run_state.py`
  - `_ScanTally.report_merged_photos`: _commits_held(), begin_immediate()
- `vireo/scanner/scan_run.py`
  - `_ScanRun._finish_catalog`: rollback()
  - `_ScanRun._index_files`: rollback()
- `vireo/scanner/triage.py`
  - `_IncrementalTriage._reuse_existing_row`: commit_with_retry() x3
  - `_IncrementalTriage.triage_files`: rollback()
- `vireo/scanner/working_copy_batch.py`
  - `_WorkingCopyBatch._discard_orphaned_publication`: rollback()
  - `_WorkingCopyBatch._mark_extraction_failed`: commit_with_retry()
  - `_WorkingCopyBatch._mark_trimmed_deferred`: commit_with_retry()
  - `_WorkingCopyBatch._publish_row`: commit_with_retry()
  - `_WorkingCopyBatch._resume_after_stop`: commit_with_retry()
  - `_WorkingCopyBatch._undo_deferral_if_quota_raised`: commit_with_retry()

### Startup (`vireo/app.py`)

- `vireo/app.py`
  - `_migrate_edit_math_render_caches`: _commit=False, commit() x2

### Migrations (`vireo/schema.py`)

- `vireo/canonical_schema.py`
  - `CanonicalSchema.create_tables`: commit()
- `vireo/schema.py`
  - `_apply_pending`: SQL BEGIN, commit(), rollback()

### Other top-level modules (job bodies and domain helpers under `vireo/`)

- `vireo/audit.py`
  - `_record_hash_verdict`: commit=False x4
  - `remove_orphans`: commit()
  - `verify_hashes`: commit() x3, commit=False
- `vireo/capture_time.py`
  - `_write_capture_time`: commit()
  - `adjust_capture_time`: rollback() x2
- `vireo/card_cleanup.py`
  - `_ScopedArchiveVerify.verify_pending_hashes`: commit()
- `vireo/classify_job/detection.py`
  - `_DetectBatchRun.sync_reclassified_subjects`: commit_with_retry()
  - `_analyze_subjects`: rollback()
- `vireo/classify_job/gating.py`
  - `_run_classifier_on_detection`: commit_with_retry()
- `vireo/classify_job/job_run.py`
  - `_ClassifyJobRun._anchor_empty_scene_photos`: commit()
- `vireo/classify_job/store_predictions.py`
  - `_release_stored_classifier_runs`: commit()
- `vireo/computation_cache.py`
  - `_ArtifactMaterialization.apply_classification`: commit(), rollback()
  - `_record_portable_run_identity`: commit(), rollback()
- `vireo/culling.py`
  - `_backfill_missing_phashes`: commit()
- `vireo/import_job/cataloging.py`
  - `_invalidate_changed_and_sweep`: commit()
  - `_stamp_cataloged_landing`: commit=False x2
- `vireo/import_job/duplicate_gate.py`
  - `_grant_twin_visibility`: commit(), rollback()
- `vireo/import_job/preflight.py`
  - `_create_dest_folder`: commit()
  - `_rollback_on_mount_loss`: commit()
- `vireo/import_job/twins.py`
  - `_link_duplicate_twin_dirs`: rollback()
- `vireo/importer.py`
  - `execute_import`: commit()
- `vireo/keyword_identity.py`
  - `_queue_disambiguated_child_renames`: _commit=False
  - `_queue_merge_keyword_add`: _commit=False
  - `_queue_merge_location_resyncs`: _commit=False x7
  - `_queue_merged_photo_changes`: _commit=False x4
  - `_queue_moved_subtree_changes`: _commit=False x3
  - `_queue_survivor_rename`: _commit=False
  - `merge_keywords`: transaction()
  - `reconcile_location`: _commit=False x2, transaction()
- `vireo/label_source_identities.py`
  - `apply_label_set`: _commit=False, begin_immediate(), transaction()
  - `apply_legacy`: _commit=False, begin_immediate(), transaction()
- `vireo/misses.py`
  - `compute_misses_for_workspace`: commit_with_retry()
- `vireo/move/folder_catalog.py`
  - `_FolderCatalogRewrite.apply_mtime_corrections`: commit()
- `vireo/move/photos.py`
  - `_PhotoCatalogRewrite.ensure_destination_folder`: commit()
  - `_StemLedger.release_source_stem`: commit()
- `vireo/move_cleanup.py`
  - `_remove_empty_source`: begin_immediate(), commit(), rollback()
- `vireo/offline_cache.py`
  - `release_photo_original`: _commit=False, begin_immediate(), transaction()
- `vireo/preview_cache.py`
  - `mark_preview_cache_invalid`: commit(), param commit
  - `reconcile_preview_cache`: begin_immediate(), transaction()
- `vireo/render_source.py`
  - `record_working_copy_failure`: commit()
- `vireo/site_export.py`
  - `export_site`: begin(), rollback()
- `vireo/species_identity_repair.py`
  - `repair_on_upgrade`: _commit=False, transaction()
- `vireo/subject_analysis.py`
  - `analyze_detection`: begin_immediate(), commit_with_retry(), transaction()
- `vireo/subjects.py`
  - `analyze_photo`: commit_with_retry() x3
  - `select_primary`: commit_with_retry()
- `vireo/sync.py`
  - `_untag_keywords_missing_from_sidecar`: _commit=False
  - `_write_photo_sync`: commit()
  - `sync_from_xmp`: _commit=False x2, begin_immediate(), commit(), rollback() x2
- `vireo/taxonomy.py`
  - `fetch_common_names`: _commit=False x3, commit()
  - `load_taxa_from_file`: commit()
  - `populate_taxa_db_from_json`: _commit=False x2, commit()
  - `seed_informal_groups`: commit()
- `vireo/thumbnails.py`
  - `_retry_thumbnail_with_companion`: commit()
  - `_retry_thumbnail_with_working_copy`: commit()
  - `backfill_thumb_paths._flush`: commit()
- `vireo/volume_reachability.py`
  - `record_known_mount_roots`: _commit=False, begin_immediate(), commit() x3, rollback()
- `vireo/working_copy_cache.py`
  - `_begin_stable_eviction_transaction`: begin_immediate(), rollback()
  - `_clear_stale_tracked_rows`: commit_with_retry()
  - `_record_evictions`: commit_with_retry()

