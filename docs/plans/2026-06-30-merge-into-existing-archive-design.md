# Merge local-processing imports into an existing managed archive

**Date:** 2026-06-30
**Branch:** `archive-destination-overlap-error`

## Problem

A local-processing import whose archive destination is a folder Vireo already
manages fails at run time:

```
Pipeline failed
Archive destination /Volumes/Photography/Raw Files/USA overlaps a folder
Vireo already manages (/Volumes/Photography/Raw Files/USA). Local processing
imports must land at a new archive folder; pick a different destination or
import without local processing.
```

The message reads as nonsense ("X overlaps X") because the destination *is* the
managed folder. More importantly, importing new shoots into an existing archive
is the **expected** workflow — the archive is the system of record and new
shoots accrete into it. Commit #1064 (`d0194dd9`) was built around exactly this
shape (a templated copy-import into a destination base full of past shoots), but
the pipeline storage preflight still hard-refuses it.

## Principle

Importing into an already-managed archive is a normal operation, not an error.
It should seamlessly merge. The merge is safe because the file-copy layer never
overwrites or deletes a differing file — it only adds missing files and
recognizes identical ones.

Scope (agreed): full seamless merge, covering
- new dated subfolders added under an existing base that holds other shoots,
- a dated subfolder that already exists and already holds photos (new files fold
  in),
- re-dropped cards whose photos are already archived (identical files recognized
  and skipped, no duplicate catalog rows).

Both "destination **is** a managed folder" and "destination is **inside** a
managed folder" are handled — same user intent, same reconciliation logic.

## What already works (and is NOT changed)

`move_folder`'s file-copy step already does a safe merge:
- `rsync --ignore-existing` — never overwrites a file already at the destination
  (`move.py:1695-1697`).
- Verify every source file is present at the destination before deleting
  originals (`move.py:1757-1782`).
- **Content-conflict refusal** — if any same-name destination file differs in
  content, the move aborts with nothing copied or deleted
  (`move.py:1583-1629`).

The merge rides on top of these. It changes only (a) which destinations are
allowed and (b) the catalog reconciliation step.

## §1 — Guard relaxation

Two guards refuse this today; both become conditional via a new opt-in
parameter `allow_tracked_merge=False` on `move_folder`. Only the pipeline
archive call passes `True`; every other caller (manual folder-move UI, remote
moves) keeps today's hard refusal unchanged.

1. **Pipeline storage preflight** — `pipeline_job.py:1062` (overlap) and
   `:1085` (ancestor). For local-processing archive runs these stop being fatal
   (no `_bail_storage`); the run proceeds to merge.
2. **`move_folder`'s own guards** — `move.py:1545` (overlap) and `:1552`
   (ancestor). When `allow_tracked_merge=True`, the refusal is replaced by the
   reconciliation path (§2). When `False` (default), unchanged.

## §2 — Catalog reconciliation (core new logic)

After the file copy + verify succeeds, `move_folder` today calls
`db.move_folder_path(folder_id, catalog_path)`, which rewrites the staging root
row's path. That collides on `folders.path` UNIQUE when a tracked row already
sits at the target. The folder hierarchy is purely path-string based (no
`parent_id`), so a real merge reconciles folder rows.

New `db` method (e.g. `merge_staged_tree_into_archive`), used only on the
`allow_tracked_merge` path. Walking staged folder rows root-first, for each
staged folder compute `target = rebase(staged_path, src_path -> catalog_path)`:

- **Target has no folder row** → genuinely new subfolder. Rewrite the staged
  row's path to `target` (today's behavior). Set `is_root = 0` if any tracked
  ancestor is already a root, else `1` — preserving the existing archive's root
  structure and staying consistent with #1064.
- **Target already has a tracked folder row** → merge into it:
  - For each staged photo, its post-move path is `target/filename`.
    - If **no** photo row exists there → reparent it (update `folder_id` +
      `path`) into the existing folder.
    - If one **does** exist → it's an identical file the copy layer already
      recognized (a differing file would have aborted the move upstream). Drop
      the staged photo row; the existing archived row stays canonical.
  - Delete the now-empty staged folder row.
- **Workspace attach:** every folder the merge touched is added to the current
  workspace's `workspace_folders` so the new photos surface in the active
  workspace. Invalidate `new_images` caches for affected roots.

With `skip_duplicates` on (the default) files already in the catalog never reach
staging, so reconciliation mostly reparents genuinely-new photos and folds new
dates into existing date-folders. The photo-row-collision branch is the safety
net for `skip_duplicates`-off re-runs.

## §3 — Pre-run transparency

Two honest signals, no new hashing (UI transparency is a hard rule — an existing
managed archive must not be presented as an empty/new destination):

1. **"Existing managed archive" + size.** Extend `api_import_destination_preview`
   (`app.py:10458`, already has `db`): run `_tracked_destination_overlap` /
   `_tracked_destination_ancestor` against the destination; if either hits,
   return `{managed_archive: {path, photo_count}}` (one indexed query + count,
   no file I/O). The import dialog renders *"Destination is an existing Vireo
   archive (/Volumes/.../USA, 9,041 photos). New files will be merged in."*
2. **"N of your selected files are already archived."** The import UI already
   runs the `api_import_check_duplicates` SSE scan (`app.py:10344`), which counts
   selection files whose hash is in the catalog. Reuse that result — when the
   destination is a managed archive, frame those as *"38 already in this archive
   (will be skipped), 412 new (will be merged)"* instead of generic "duplicates."

`preview_destination`'s `existing_folders` ("dir exists on disk") stays but is
subordinate to the explicit, stronger "managed archive" callout.

## §4 — Final summary

`move_folder`'s result gains counts: `merged_new` (staged photos reparented),
`merged_into_existing_folders` (date-folders folded into pre-existing ones vs.
created fresh), `already_present` (identical-path staged rows dropped). The
pipeline archive summary becomes e.g. *"412 photos archived into existing
archive /Volumes/.../USA — 412 new across 3 folders (2 new, 1 merged into
existing), 0 already present."* A brand-new destination keeps today's wording.

## §5 — Testing (TDD, written first)

1. `db.merge_staged_tree_into_archive` unit tests: new subfolder under tracked
   base; new date folded into existing tracked date-folder; identical-path photo
   collision drops staged row; `is_root` rule (root ancestor → child
   `is_root=0`); workspace attach.
2. `move_folder` with `allow_tracked_merge=True` merges into a tracked
   destination; without it, still refuses (regression guard).
3. End-to-end pipeline: local-processing import into an already-managed archive
   base succeeds, files land in date subfolders, catalog has no duplicate rows,
   no UNIQUE violation. (The exact scenario from the error.)
4. Preview endpoint returns `managed_archive` for a tracked destination; `null`
   for a fresh one.
5. Update existing `test_pipeline_api.py` tests that assert the *old* failure
   (`:313`, `:353`, `:402`, incl. the ancestor case) to assert the merge
   succeeds.

## Files touched

- `vireo/move.py` — `allow_tracked_merge` param; reconciliation call.
- `vireo/db.py` — `merge_staged_tree_into_archive`.
- `vireo/pipeline_job.py` — drop fatal preflight for archive runs; pass
  `allow_tracked_merge=True`; merge-aware archive summary.
- `vireo/ingest.py` / `vireo/app.py` — `managed_archive` signal in destination
  preview.
- Templates (import dialog) — render managed-archive callout + new/already-there
  split.
- Tests as above.
