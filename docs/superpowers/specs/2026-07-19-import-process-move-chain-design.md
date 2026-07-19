# Import → Process → Move-to-NAS chain

**Date:** 2026-07-19
**Status:** Approved by Julius (design conversation, this workspace)
**Branch:** `local-disk-processing-nas-sync`

## Problem

The retired `pipeline_job` import/archive mode gave one property the current
split workflow lacks: photos were staged and processed on fast local disk and
paid the network cost exactly once, in a single unattended rsync to the NAS at
the end. Today, importing straight to a NAS remote target means every heavy
pipeline stage (hashing, thumbnails, classification, detection) reads original
RAW bytes over SMB — `classify_job.py` builds
`os.path.join(folder_path, photo["filename"])` and loads the original, and
thumbnails render from originals via `render_source`.

The manual workaround (import to a local archive → run processing → Move page)
recovers the performance pattern but as three manual steps, and the catalog
points at local disk until the user remembers to move — forget the last step
and the shoot never reaches the NAS or its backups.

## Goal

Make **import to local archive → process locally → move to NAS** a first-class
chained workflow: configured once at import time, runs unattended, and ends
with the photos on the NAS and the catalog pointing at the NAS mount.

Two-thirds of the chain already exists:

- The Import page's "After import" card chains a saved process to run when the
  import finishes (`_chain_after_import` in `app.py`, `chained_from` link,
  `_enqueue_process_job`).
- The Move page's backend is a job (`/api/jobs/move-folder`) taking
  `remote_target_id` + `subpath`, built on `move.py`'s copy-verify-delete with
  merge/resume semantics.

This design adds the missing **process → move** link plus the UI and config to
express the whole chain up front.

## Decisions (agreed with Julius)

1. **Fully automatic** — the move enqueues as soon as the chained processing
   job finishes. No review gate. Restores the old workflow's "guaranteed
   end-state on the NAS" property.
2. **Mirror the local layout** — the user picks only the remote target. Each
   imported folder moves to `<remote_path>/<relative path under the local
   archive root>`. No per-import subpath field.
3. **Move on failure, hold on cancel** — a processing *failure* does not stop
   the move (unattended runs must still end on the NAS); an explicit *cancel*
   of the import or process job stops the chain.
4. **Architecture: extend the existing completion-callback chain** — a new
   hook symmetric to `_chain_after_import`, not a persistent chain table and
   not an orchestrator job (that is the retired monolith by another name).
   The known gap — an app restart between links forgets the pending move — is
   identical to today's after-import behavior and fails safe (photos remain
   cataloged in the local archive).

## Design

### 1. Config: `local_archive_root` on remote targets

Each saved remote target (`config.py` `remote_targets`) gains one optional
field:

- `local_archive_root` — absolute local directory that mirrors the target's
  `remote_path`. Empty/absent means the target never offers the chained move.

Changes:

- `DEFAULTS` comment block documents the field.
- `_coerce_remote_target` normalizes it (strip; keep `""` when unset) and
  validation rejects a value that is not an absolute path or that lies inside
  the target's `mount_path` (the mount is the *destination* view, never the
  local staging side).
- Settings UI: one text input per remote-target row, labeled with a hint
  explaining the mirror relationship.

### 2. UI: extend the Import page's "After import" card

When (a) import mode is **Copy to archive** with a **local** destination,
(b) a saved process is selected (not "None — import only"), and (c) the chosen
destination is under some remote target's `local_archive_root`, the card shows
a new row:

- Checkbox **"Then move to NAS"** + target dropdown (auto-selected when
  exactly one target's root covers the destination).
- A preview line showing the exact NAS-side path each imported folder will
  land at: `<remote_path>/<relative path under local_archive_root>` — per the
  UI-transparency rule, this states what will actually happen, computed from
  current selections.

When the destination is *not* under any target's `local_archive_root` (and an
absolute destination has been entered), the row is replaced by a hint saying
why the option is unavailable ("Move to NAS unavailable: the destination is
not inside any remote target's local archive root. Set one under Settings >
Remote targets"), rather than silently hidden.

Import-in-place and remote-destination imports never show the row.

### 3. Backend: request → snapshot → chain links

**Request.** The import request body gains:

```json
"after_process_move": {"remote_target_id": "<id>"} | null
```

Validated up front in the endpoint (mirroring `_validate_after_import`):

- must be `null` or an object with a known `remote_target_id`;
- the target must have a `local_archive_root`;
- the import destination must be under that root;
- requires a non-null `after_import` (the chain is import → process → move;
  an import-only run with a move is just the Move page and is rejected with a
  clear error).

**Snapshot.** The endpoint resolves the target via `config.get_remote_target`
and captures it at enqueue time (same rationale as the existing
`remote_target_snapshot`: a Settings edit mid-chain cannot redirect the move
to a different host/path than the user accepted).

**Link 1 (exists).** Import job finishes → `_chain_after_import` enqueues the
process job. Change: pass the move snapshot through `_enqueue_process_job` so
the process job knows its downstream link.

**Link 2 (new).** At the end of the process job's `work()`, a new
`_chain_after_move` hook runs (symmetric in structure and skip-reporting to
`_chain_after_import`):

- Computes the set of **top-level imported folders** relative to
  `local_archive_root` from the import result's cataloged photos/folders
  (e.g. photos landed in `<root>/2026/trip/…` → top-level moved unit is
  `<root>/2026/trip`, i.e. the highest folder at the first path level below
  the root that received imported photos — one move preserves the whole
  subtree and the mirror layout).
- Enqueues **one move-folder job per top-level folder**, with `merge=true`,
  `chained_from` set to the process job id, and the snapshotted target +
  derived subpath.
- Reuses the move-folder endpoint's guard logic via a shared helper (shared
  local copies, staged workspace folders, etc.) — a refused folder is
  reported in the result, never force-moved.

### 4. Chain semantics

| Upstream outcome              | Move link behavior                          |
| ----------------------------- | ------------------------------------------- |
| Import failed                 | Chain already stops today (unchanged)       |
| Import cancelled              | Chain already stops today (unchanged)       |
| Process succeeded             | Move enqueues                               |
| Process **failed**            | Move **still enqueues** (decision 3)        |
| Process **cancelled**         | Move skipped: `after_move_skipped`          |
| No new photos (all dups)      | Move skipped (nothing to relocate)          |

Result-dict reporting mirrors the existing pattern: `move_job_ids` on
success-path, `after_move_skipped: "<reason>"` otherwise, so the Jobs panel
and the Import result card can render the chain state honestly.

App restart between links: the pending move is forgotten (existing behavior
for after-import). Photos remain cataloged and browsable in the local
archive; the user can move manually via the Move page. The result card's chain
info makes this state visible.

### 5. Edge cases

- **Folder already exists on NAS:** `merge=true` gives rsync
  `--ignore-existing` resume semantics — byte-identical files skip, missing
  files copy; `move.py`'s copy-verify-delete still gates deletion of local
  originals on per-file verification.
- **Concurrent writes to the same archive folder while the move is queued:**
  the move job moves whatever is in the folder at run time; per-file
  verification protects each file. Documented, not blocked.
- **Zero new photos:** both links skip (see table).
- **Multiple imported top-level folders:** one move job per folder, all
  enqueued together (so the process result's `move_job_ids` is complete
  immediately). `JobRunner.start` gives each job its own thread — SLOT_CAP
  only governs pipeline jobs — so the transfers themselves are serialized
  by a chain-local lock shared across the batch: a single NAS never sees
  concurrent chained rsyncs and `--bwlimit` is honored instead of being
  multiplied by N. Jobs waiting on the lock report a "Waiting for an
  earlier chained move to finish" phase.

### 6. Testing

- **Unit:** `after_process_move` validation (unknown target, target without
  root, destination outside root, missing `after_import`);
  top-level-folder derivation (single folder, multiple folders, nested
  template paths); chain-skip matrix from the table above.
- **Integration** (pattern of existing `test_jobs_api.py` chain tests): fake
  remote target whose "NAS" is a temp dir; happy path import → process → move
  asserting the catalog repoints to `mount_path` and local originals are
  deleted; process-cancel path asserting no move job enqueues; process-fail
  path asserting the move still runs.
- Standard suite from `CLAUDE.md` before the PR.

## Planning notes (from spec review)

- **Hook placement must survive the raise path.** `run_pipeline_job` can fail
  by *raising* (e.g. model-resolution `RuntimeError`), so a `_chain_after_move`
  call placed after a normal return would never fire on failure — violating
  decision 3. Place the hook so it runs on both the return and raise paths
  (try/finally or the runner's completion path) while still distinguishing
  cancel from fail. The required process-fail integration test guards this.
- **Thread the folder set explicitly.** The import result (and thus the
  imported folder set) lives in Link 1's scope; the hook runs inside the
  process job. The plan must pick one: compute the top-level folder ids in
  Link 1 and pass them through `_enqueue_process_job` alongside the move
  snapshot (preferred — move-folder jobs are keyed by catalog `folder_id`,
  not path), or re-derive them in Link 2.

## Out of scope

- Persistent chain state surviving app restarts (additive later if the gap
  annoys in practice).
- A review/culling gate before the move (decision 1 chose fully automatic).
- Chaining for import-in-place or remote-destination imports.
