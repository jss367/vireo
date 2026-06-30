# Merge Into Existing Managed Archive — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Let a local-processing import whose archive destination is already a Vireo-managed folder seamlessly merge into it instead of failing the pipeline.

**Architecture:** The file-copy layer in `move_folder` already merges safely (rsync `--ignore-existing`, verify-before-delete, content-conflict refusal). This plan (a) makes the two tracked-destination guards conditional behind a new `allow_tracked_merge` flag the pipeline passes, (b) replaces `move_folder`'s post-copy `db.move_folder_path` call with a catalog reconciliation that folds staged folder/photo rows into the existing archive rows, and (c) surfaces the existing-archive fact and the already-archived count in the pre-run import preview and the final summary.

**Tech Stack:** Python 3, Flask, SQLite (WAL, FK on), pytest. No frontend framework — Jinja2 + vanilla JS.

**Key facts (verified):**
- `folders`: `id, path (UNIQUE), parent_id REFERENCES folders(id), name, photo_count, status`.
- `photos`: identified by `(folder_id, filename)` — **no path column**. Photo collision = same filename in the target folder.
- `db.check_filename_collisions(photo_ids, target_folder_id)` (`db.py:2722`) returns the colliding `{photo_id, filename}` rows.
- `db.add_workspace_folder(workspace_id, folder_id, *, is_root=...)` (`db.py:1526`) links a folder subtree to a workspace, sets `workspace_folders.is_root`, and invalidates the new-images cache. `is_root` lives on `workspace_folders`, **not** `folders`.
- `_tracked_destination_overlap(db, folder_id, dest)` → tracked folder row AT/below `dest`. `_tracked_destination_ancestor(db, folder_id, dest)` → tracked folder row ABOVE `dest` (`move.py:1182`, `:1203`).
- `db.move_folder_path(folder_id, new_path)` (`db.py:2693`) rewrites a folder's path + path-prefix descendants. It does NOT touch `parent_id`.
- Pipeline archive call: `move_folder(thread_db, folder["id"], archive_parent, ..., merge=True, reject_tracked_ancestor=True)` at `pipeline_job.py:4606`.
- Pipeline storage preflight bails via `_bail_storage` at `pipeline_job.py:1062` (overlap) and `:1085` (ancestor).

**Run tests with:**
```
python -m pytest vireo/tests/test_db.py vireo/tests/test_move.py vireo/tests/test_pipeline_api.py -q
```

---

### Task 1: `db.merge_staged_tree_into_archive` — new-subfolder case

Reconciliation method. Start with the simplest case: staged tree maps entirely to **new** target paths (no existing folder rows at the targets) — behaviourally equivalent to `move_folder_path` but also fixes `parent_id` and links the workspace.

**Files:**
- Modify: `vireo/db.py` (add method near `move_folder_path`, ~`db.py:2720`)
- Test: `vireo/tests/test_db.py`

**Step 1: Write the failing test**

```python
def test_merge_staged_tree_new_subfolders(tmp_path):
    """Staged tree merged under an existing tracked base: new date folders
    are repointed under the base, parent_id fixed, workspace linked, and the
    base's existing photos are untouched."""
    db = Database(str(tmp_path / "t.db"))
    ws = db.active_workspace_id

    # Existing tracked archive base with one prior shoot.
    base_id = db.add_folder("/arch/USA")
    old_id = db.add_folder("/arch/USA/2025/2025-01-01", parent_id=base_id)
    db.add_photo(old_id, "old.raf")
    db.add_workspace_folder(ws, base_id, is_root=True)

    # Staged tree (post-rsync the files already live at /arch/USA/...).
    stage_root = db.add_folder("/stage/USA")
    stage_leaf = db.add_folder("/stage/USA/2026/2026-06-30", parent_id=stage_root)
    db.add_photo(stage_leaf, "new.raf")

    db.merge_staged_tree_into_archive(stage_root, "/arch/USA")

    # New leaf now lives under the base, parented correctly.
    leaf = db.conn.execute(
        "SELECT id, parent_id FROM folders WHERE path = ?",
        ("/arch/USA/2026/2026-06-30",),
    ).fetchone()
    assert leaf is not None
    # The staged root row is gone (folded into the existing base).
    assert db.conn.execute(
        "SELECT 1 FROM folders WHERE path = ?", ("/stage/USA",)
    ).fetchone() is None
    # The new photo moved with the folder; the old photo is untouched.
    assert db.conn.execute(
        "SELECT folder_id FROM photos WHERE filename = ?", ("new.raf",)
    ).fetchone()["folder_id"] == leaf["id"]
    # New leaf is linked to the workspace as a non-root (base is the root).
    row = db.conn.execute(
        "SELECT is_root FROM workspace_folders WHERE workspace_id=? AND folder_id=?",
        (ws, leaf["id"]),
    ).fetchone()
    assert row is not None and row["is_root"] == 0
```

> Adapt `db.add_folder` / `db.add_photo` / `db.active_workspace_id` to the real
> helper names used elsewhere in `test_db.py` (grep for how other tests insert
> folders and photos — e.g. they may use raw `INSERT` or a `_seed` helper).

**Step 2: Run it, expect failure**

Run: `python -m pytest vireo/tests/test_db.py -k merge_staged_tree_new_subfolders -q`
Expected: FAIL — `AttributeError: 'Database' object has no attribute 'merge_staged_tree_into_archive'`.

**Step 3: Implement**

```python
def merge_staged_tree_into_archive(self, staged_root_id, archive_path):
    """Fold a staged folder subtree into an existing tracked archive.

    The on-disk rsync merge has already happened: files that were under the
    staged root now also live under ``archive_path``. This reconciles the
    catalog so staged folder/photo rows become rows under the existing
    archive, with no duplicate ``folders.path`` and correct ``parent_id``.

    For each staged folder (root-first), the target path is the staged path
    rebased from the staged root onto ``archive_path``:

    * Target has no folder row -> repoint the staged row to the target path,
      fix its ``parent_id`` to the (now-existing) target-parent folder, and
      link it to the active workspace as a non-root when a workspace root
      ancestor exists, else as a root.
    * Target already has a folder row -> move the staged folder's photos into
      it (dropping any whose filename already exists there as an identical
      archived file), then delete the now-empty staged folder row.
    """
    staged_root = self.conn.execute(
        "SELECT path FROM folders WHERE id = ?", (staged_root_id,)
    ).fetchone()
    if not staged_root:
        return {"merged_new": 0, "merged_into_existing_folders": 0,
                "already_present": 0}
    staged_root_path = staged_root["path"]
    ws = self._ws_id()

    # Snapshot staged folders root-first (shallowest path first) so a parent's
    # target row exists before its children are processed.
    staged_folders = self.conn.execute(
        """SELECT id, path FROM folders
           WHERE path = ? OR substr(REPLACE(path,'\\','/'),1,?) = ?
           ORDER BY length(path) ASC""",
        (staged_root_path,
         len(_subtree_prefix(staged_root_path)),
         _subtree_prefix(staged_root_path)),
    ).fetchall()

    counts = {"merged_new": 0, "merged_into_existing_folders": 0,
              "already_present": 0}

    for sf in staged_folders:
        rel = _subtree_relative(sf["path"], staged_root_path)
        target_path = _join_subtree_path(archive_path, rel)
        target = self.conn.execute(
            "SELECT id FROM folders WHERE path = ?", (target_path,)
        ).fetchone()
        parent_path = os.path.dirname(target_path)
        parent_row = self.conn.execute(
            "SELECT id FROM folders WHERE path = ?", (parent_path,)
        ).fetchone()
        parent_id = parent_row["id"] if parent_row else None

        if target is None:
            # New folder under the archive: repoint + reparent + link.
            self.conn.execute(
                "UPDATE folders SET path = ?, parent_id = ? WHERE id = ?",
                (target_path, parent_id, sf["id"]),
            )
            # A workspace root ancestor exists iff some ancestor folder is
            # is_root=1 in this workspace; the base archive normally is.
            self.add_workspace_folder(ws, sf["id"], is_root=False)
            counts["merged_new"] += 1
        else:
            # Existing folder: move photos in, drop filename-collisions.
            photo_ids = [r["id"] for r in self.conn.execute(
                "SELECT id FROM photos WHERE folder_id = ?", (sf["id"],)
            )]
            collisions = {c["photo_id"]
                          for c in self.check_filename_collisions(
                              photo_ids, target["id"])}
            for pid in photo_ids:
                if pid in collisions:
                    self.conn.execute("DELETE FROM photos WHERE id = ?", (pid,))
                    counts["already_present"] += 1
                else:
                    self.conn.execute(
                        "UPDATE photos SET folder_id = ? WHERE id = ?",
                        (target["id"], pid),
                    )
            self.conn.execute("DELETE FROM folders WHERE id = ?", (sf["id"],))
            counts["merged_into_existing_folders"] += 1

    self.conn.commit()
    self.update_folder_counts()
    return counts
```

> Confirm `_subtree_prefix`, `_subtree_relative`, `_join_subtree_path` are the
> module-level helpers `move_folder_path` uses (`db.py`). Reuse them; do not
> reimplement path math.

**Step 4: Run it, expect pass**

Run: `python -m pytest vireo/tests/test_db.py -k merge_staged_tree_new_subfolders -q`
Expected: PASS.

**Step 5: Commit**

```bash
git add vireo/db.py vireo/tests/test_db.py
git commit -m "db: merge_staged_tree_into_archive — new-subfolder reconciliation"
```

---

### Task 2: Reconciliation — existing-folder + collision cases

Cover the date-folder-already-exists and identical-filename branches explicitly with their own test (the implementation from Task 1 already handles them; this locks the behaviour).

**Files:**
- Test: `vireo/tests/test_db.py`

**Step 1: Write the failing test**

```python
def test_merge_staged_tree_existing_folder_and_collision(tmp_path):
    db = Database(str(tmp_path / "t.db"))
    ws = db.active_workspace_id
    base_id = db.add_folder("/arch/USA")
    date_id = db.add_folder("/arch/USA/2026/2026-06-30", parent_id=base_id)
    db.add_photo(date_id, "dup.raf")      # already archived
    db.add_photo(date_id, "keep.raf")
    db.add_workspace_folder(ws, base_id, is_root=True)

    stage_root = db.add_folder("/stage/USA")
    stage_leaf = db.add_folder("/stage/USA/2026/2026-06-30", parent_id=stage_root)
    db.add_photo(stage_leaf, "dup.raf")   # same filename -> identical, drop
    db.add_photo(stage_leaf, "fresh.raf") # genuinely new -> reparent

    counts = db.merge_staged_tree_into_archive(stage_root, "/arch/USA")

    names = {r["filename"] for r in db.conn.execute(
        "SELECT filename FROM photos WHERE folder_id = ?", (date_id,))}
    assert names == {"dup.raf", "keep.raf", "fresh.raf"}      # no duplicate row
    # exactly one dup.raf
    assert db.conn.execute(
        "SELECT COUNT(*) c FROM photos WHERE filename='dup.raf'").fetchone()["c"] == 1
    assert counts["already_present"] == 1
    assert counts["merged_into_existing_folders"] >= 1
    # staged rows gone
    assert db.conn.execute(
        "SELECT 1 FROM folders WHERE path LIKE '/stage/%'").fetchone() is None
```

**Step 2: Run it** — Expected: PASS (Task 1 implementation already covers this). If it fails, fix `merge_staged_tree_into_archive`, not the test.

**Step 3: Commit**

```bash
git add vireo/tests/test_db.py
git commit -m "test: reconciliation existing-folder + identical-filename collision"
```

---

### Task 3: `move_folder` gains `allow_tracked_merge`

Make the two tracked-destination guards conditional, and route the post-copy catalog step to the reconciliation when merging into a tracked destination.

**Files:**
- Modify: `vireo/move.py` — signature at `move.py:1375`; guards at `:1545`/`:1552`; catalog update at `:1802`.
- Test: `vireo/tests/test_move.py`

**Step 1: Write the failing tests**

```python
def test_move_folder_refuses_tracked_merge_by_default(move_env):
    """Default behaviour unchanged: merging into a tracked folder is refused."""
    # (mirror the existing test at test_move.py that asserts 'already manage';
    #  assert the same refusal with allow_tracked_merge omitted.)
    ...
    assert any("already manage" in e for e in result["errors"])

def test_move_folder_merges_into_tracked_when_allowed(move_env):
    """With allow_tracked_merge=True the staged tree's files land in the
    existing archive on disk and the catalog has no duplicate folder rows."""
    # Build: tracked archive dir with a prior shoot on disk + in catalog;
    # a staged source dir whose files belong under the archive.
    result = move_folder(db, staged_id, archive_parent,
                         merge=True, allow_tracked_merge=True)
    assert result["errors"] == []
    assert result["moved"] >= 1
    # No duplicate folders.path rows; staged rows folded away.
    assert db.conn.execute(
        "SELECT 1 FROM folders WHERE path LIKE ?", (staged_prefix + "%",)
    ).fetchone() is None
```

> Model `move_env` on the existing fixtures in `test_move.py` (it already has
> helpers that lay down a source tree on disk and seed catalog rows for the
> tracked-overlap tests at `:529`, `:549`, `:568`). Reuse them.

**Step 2: Run them, expect failure**

Run: `python -m pytest vireo/tests/test_move.py -k "tracked_merge" -q`
Expected: FAIL — `move_folder() got an unexpected keyword argument 'allow_tracked_merge'`.

**Step 3: Implement**

In `move_folder`'s signature (`move.py:1375`) add `allow_tracked_merge=False`.

Guard block (`move.py:1544-1558`) — gate both refusals and remember the merge target:

```python
overlap_check_path = catalog_path if remote else transfer_dest
merge_into_tracked = None
tracked = _tracked_destination_overlap(db, folder_id, overlap_check_path)
if tracked:
    if not allow_tracked_merge:
        return {"moved": 0, "errors": [
            f"Destination overlaps a folder Vireo already manages "
            f"({tracked['path']}). Merging into or around a tracked folder "
            f"isn't supported."
        ]}
    merge_into_tracked = tracked["path"]
if reject_tracked_ancestor and merge_into_tracked is None:
    ancestor = _tracked_destination_ancestor(db, folder_id, overlap_check_path)
    if ancestor:
        if not allow_tracked_merge:
            return {"moved": 0, "errors": [
                f"Destination is inside a folder Vireo already manages "
                f"({ancestor['path']}). Pick an untracked archive destination."
            ]}
        # Merge into the existing archive root that contains the destination.
        merge_into_tracked = catalog_path if remote else transfer_dest
```

> Note: in the ancestor case the staged tree merges at its own resolved
> `catalog_path` (which sits *inside* the tracked ancestor). The reconciliation
> rebases staged rows onto that path; the ancestor's own rows are untouched.

Catalog update (`move.py:1800-1803`) — branch:

```python
if progress_cb:
    progress_cb(total_files, total_files, "", "Updating catalog")
if merge_into_tracked is not None:
    merge_counts = db.merge_staged_tree_into_archive(folder_id, catalog_path)
else:
    db.move_folder_path(folder_id, catalog_path)
db.update_folder_counts()
```

And thread `merge_counts` into the return dict (default zeros when not a merge):

```python
result = {"moved": total_photos, "errors": []}
if merge_into_tracked is not None:
    result["merge"] = merge_counts
    result["merged_into_existing"] = merge_into_tracked
if cleanup_error is not None:
    result["cleanup_error"] = cleanup_error
return result
```

> `developed_dir` rebase block (`move.py:1810-1829`) keys off `move_folder_path`
> having repointed paths. After a merge the staged paths are gone, so the
> existing descendant-rebase loop is a no-op for merged rows (they now live at
> archive paths already). Leave it; verify no crash with `developed_dir=""`
> (the pipeline passes the configured dir — add a follow-up only if a
> developed-output merge test surfaces a real gap; YAGNI for now).

**Step 4: Run them, expect pass**

Run: `python -m pytest vireo/tests/test_move.py -k "tracked_merge" -q`
Expected: PASS. Then run the whole move suite — the existing tracked-overlap refusal tests must still pass:
Run: `python -m pytest vireo/tests/test_move.py -q`

**Step 5: Commit**

```bash
git add vireo/move.py vireo/tests/test_move.py
git commit -m "move_folder: allow_tracked_merge opt-in routes to reconciliation"
```

---

### Task 4: Pipeline — drop fatal preflight, pass the flag, merge-aware summary

**Files:**
- Modify: `vireo/pipeline_job.py` — preflight at `:1062`/`:1085`; archive call at `:4606`; summary at `:4635-4650`.

**Step 1: Preflight (`pipeline_job.py:1062-1097`)**

Remove the two `_bail_storage(...)` + `return` blocks for `tracked` and `ancestor`. The downstream `conflicting_archive_paths` check (`:1181`) stays — it's the precise per-file content-conflict guard and is still correct. Leave the import of `_tracked_destination_overlap` / `_tracked_destination_ancestor` only if still referenced; otherwise drop it (ruff F401).

> Keep the `archive_parent` existence + `final_destination`-is-a-file checks
> (`:1099` onward) — those are unrelated to tracked status.

**Step 2: Archive call (`pipeline_job.py:4606`)**

```python
move_result = move_folder(
    thread_db,
    folder["id"],
    archive_parent,
    progress_cb=archive_cb,
    developed_dir=developed_dir,
    merge=True,
    reject_tracked_ancestor=True,
    allow_tracked_merge=True,
)
```

**Step 3: Summary (`pipeline_job.py:4635-4650`)**

```python
moved = move_result.get("moved", 0)
merge = move_result.get("merge")
if merge:
    base = move_result.get("merged_into_existing", final_destination)
    summary = (
        f"{merge['merged_new']} photos archived into existing archive "
        f"{base} ({merge['merged_into_existing_folders']} folders merged, "
        f"{merge['already_present']} already present)"
    )
else:
    summary = f"{moved} photos archived"
if cleanup_error:
    summary += f" (staging cleanup failed: {cleanup_error})"
```

Also add `"merge": merge` to `result["archive"]` when present, for the API payload.

**Step 4: Manual sanity**

Run the full pipeline API suite (Task 5 updates these to pass):
`python -m pytest vireo/tests/test_pipeline_api.py -q`

**Step 5: Commit**

```bash
git add vireo/pipeline_job.py
git commit -m "pipeline: merge into existing managed archive instead of failing"
```

---

### Task 5: Update existing pipeline-API regression tests

The tests at `test_pipeline_api.py:313/353/402` assert the OLD fatal failure. Flip them to assert the merge succeeds.

**Files:**
- Modify: `vireo/tests/test_pipeline_api.py:300-410` (the overlap and ancestor tests).

**Step 1:** Read those two tests. Rename to reflect new behaviour (e.g. `test_pipeline_merges_into_managed_archive`, `test_pipeline_merges_into_subfolder_of_managed_archive`). Replace the `assert "Vireo already manages" in error_text` assertions with: job completes without a storage failure, files exist at the templated destination, and the catalog has no duplicate `folders.path`.

**Step 2: Run** — `python -m pytest vireo/tests/test_pipeline_api.py -q` → PASS.

**Step 3: Commit**

```bash
git add vireo/tests/test_pipeline_api.py
git commit -m "test: pipeline merges into managed archive (was: hard-failed)"
```

---

### Task 6: End-to-end pipeline merge test

A focused regression for the exact reported scenario: import into an already-managed archive base with a folder template; assert seamless merge.

**Files:**
- Test: `vireo/tests/test_pipeline_api.py` (new test) — or `vireo/tests/test_pipeline_local_processing.py` if one exists (grep first).

**Step 1: Write the test**

Build a temp archive dir containing a prior shoot, scan it so it's tracked + a workspace root. Run a local-processing pipeline whose `destination` is that archive base, with `folder_template="%Y/%Y-%m-%d"`, sources being a couple of new files with EXIF dates not yet present. Assert: job succeeds; new files land at `<base>/<year>/<date>/`; the prior shoot's files and rows are untouched; no duplicate `folders.path`; the new leaf folder is linked to the active workspace.

> Reuse the pipeline-job test harness the existing overlap tests use (same file)
> — they already build a runner + thread_db + params. Copy that scaffolding.

**Step 2: Run** — `python -m pytest vireo/tests/test_pipeline_api.py -k merge_e2e -q` → PASS.

**Step 3: Commit**

```bash
git add vireo/tests/test_pipeline_api.py
git commit -m "test: end-to-end import merges new shoot into existing archive"
```

---

### Task 7: Pre-run transparency — `managed_archive` preview signal

**Files:**
- Modify: `vireo/app.py:10458` (`api_import_destination_preview`).
- Test: `vireo/tests/test_app.py` (or wherever import-preview is tested — grep `destination-preview`).

**Step 1: Write the failing test**

```python
def test_destination_preview_flags_managed_archive(client, ...):
    # Seed a tracked folder at /arch/USA in the catalog.
    resp = client.post("/api/import/destination-preview", json={
        "sources": [str(src_dir)],
        "destination": "/arch/USA",
        "folder_template": "%Y/%Y-%m-%d",
    })
    data = resp.get_json()
    assert data["managed_archive"]["path"] == "/arch/USA"
    assert data["managed_archive"]["photo_count"] >= 0

def test_destination_preview_fresh_destination_has_no_managed_archive(client, ...):
    resp = client.post("/api/import/destination-preview", json={
        "sources": [str(src_dir)], "destination": str(tmp_path / "fresh"),
    })
    assert resp.get_json().get("managed_archive") is None
```

**Step 2: Run** — Expected: FAIL (`KeyError`/`None`).

**Step 3: Implement**

In `api_import_destination_preview`, after computing `result = preview_destination(...)`, add:

```python
from move import _tracked_destination_overlap, _tracked_destination_ancestor
db = _get_db()
tracked = (_tracked_destination_overlap(db, -1, destination)
           or _tracked_destination_ancestor(db, -1, destination))
result["managed_archive"] = None
if tracked:
    count = db.conn.execute(
        """SELECT COUNT(*) c FROM photos p JOIN folders f ON f.id = p.folder_id
           WHERE f.path = ? OR substr(REPLACE(f.path,'\\','/'),1,?) = ?""",
        (tracked["path"], len(tracked["path"]) + 1, tracked["path"] + "/"),
    ).fetchone()["c"]
    result["managed_archive"] = {"path": tracked["path"], "photo_count": count}
return jsonify(result)
```

> Pass `folder_id=-1` so no real folder is excluded from the probe (the preview
> isn't moving an existing folder). Keep this a pure catalog read — no file I/O.

**Step 4: Run** — `python -m pytest vireo/tests/test_app.py -k managed_archive -q` → PASS.

**Step 5: Commit**

```bash
git add vireo/app.py vireo/tests/test_app.py
git commit -m "import preview: flag destination that is an existing managed archive"
```

---

### Task 8: Import dialog UI — render the merge callout

Surface the `managed_archive` signal and frame duplicates as "already in this archive."

**Files:**
- Modify: the import dialog template + JS. Grep for the destination-preview fetch:
  `grep -rn "destination-preview\|check_duplicates\|duplicate_count" vireo/templates/`.

**Step 1:** In the JS that renders the destination-preview response, when
`data.managed_archive` is non-null, render a prominent line:
*"Destination is an existing Vireo archive (`<path>`, `<photo_count>` photos). New files will be merged in."*

**Step 2:** Where the duplicate-check (`/api/import/check-duplicates`) result is
shown, when a managed archive is in play, label the duplicate count as
*"N already in this archive (will be skipped)"* and the remainder as
*"K new (will be merged)"* instead of the generic "duplicates" wording.

> No backend change here beyond Task 7. This is presentation only. Match the
> existing dialog's markup/idiom — do not introduce a framework or new CSS
> system. Keep copy consistent with `CORE_PHILOSOPHY.md` ("no black boxes").

**Step 3: Manual verification**

Per `@superpowers:verification-before-completion`, drive the real UI
(Playwright, per the user-first-testing convention): pick an existing archive as
the destination and confirm the callout renders with the right count; pick a
fresh folder and confirm no callout.

**Step 4: Commit**

```bash
git add vireo/templates/
git commit -m "import dialog: show merge-into-existing-archive callout"
```

---

### Task 9: Full suite + cleanup

**Step 1:** Run the project's required suite (from `CLAUDE.md`):
```
python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py \
  vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py \
  vireo/tests/test_darktable_api.py vireo/tests/test_config.py vireo/tests/test_move.py \
  vireo/tests/test_pipeline_api.py -v
```
All green (ignore the known pre-existing failures noted in MEMORY.md).

**Step 2:** `ruff check vireo/` clean (drop now-unused imports).

**Step 3:** Open the PR:
```bash
gh pr create --base main --title "Merge local-processing imports into an existing managed archive" \
  --body "<what changed + test results; link the design doc>"
```

---

## Out of scope (YAGNI)
- Merging into a tracked folder from the **manual folder-move UI** or **remote moves** — `allow_tracked_merge` stays `False` there.
- Re-running classification on `skip_duplicates`-off identical re-imports — existing archived photo row wins; predictions on the dropped staged row are not migrated. Revisit only if a real workflow needs it.
- Developed-output (darktable) directory merge reconciliation beyond the existing rebase — add only if a test surfaces a concrete gap.
