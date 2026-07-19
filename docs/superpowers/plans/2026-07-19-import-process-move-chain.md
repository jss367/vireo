# Import → Process → Move-to-NAS Chain Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Chain a NAS move onto the existing import→process chain so a single Import-page action ends with photos processed locally and living on the NAS.

**Architecture:** Extend the existing completion-callback chain (`_chain_after_import` in `vireo/app.py`) with a symmetric process→move link. New config field `local_archive_root` on remote targets maps the local archive to the NAS root; the move mirrors the layout. One move-folder job per top-level imported folder, `merge=true`, target snapshotted at import-enqueue time.

**Tech Stack:** Flask + vanilla JS (no framework), SQLite, pytest. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-07-19-import-process-move-chain-design.md` — read it first, especially the decision table in §4 and the two planning notes.

**Branch:** work directly on `local-disk-processing-nas-sync` (this Conductor workspace is the isolated worktree).

## File structure

| File | Responsibility |
| --- | --- |
| `vireo/config.py` (modify) | `local_archive_root` field on remote targets: DEFAULTS comment + `_coerce_remote_target` normalization |
| `vireo/import_chain.py` (create) | Pure helper: derive the minimal non-nested set of imported folders to move, with NAS subpaths |
| `vireo/app.py` (modify) | `_validate_after_process_move`, guard/start extraction for move-folder jobs, `_chain_after_move` hook, threading `after_move` through `_enqueue_process_job`, wiring in both import endpoints |
| `vireo/templates/import.html` (modify) | "Then move to NAS" row in the After-import card + request body + result-card chain info |
| `vireo/templates/settings.html` (modify) | Local-archive-root input on remote-target rows |
| `vireo/tests/test_config.py` (modify) | Coercion tests for `local_archive_root` |
| `vireo/tests/test_import_chain.py` (create) | Unit tests for the derivation helper |
| `vireo/tests/test_jobs_api.py` (modify) | Endpoint validation + end-to-end chain tests (monkeypatched move machinery) |

Existing landmarks (verify line numbers with grep before editing — they drift):

- `vireo/config.py:44-51` — `remote_targets` DEFAULTS comment; `_coerce_remote_target` at ~660; `get_remote_target` at ~737.
- `vireo/app.py:20333` — `_validate_after_import` (pattern to mirror); `:20386` in-place import endpoint; `:20714` copy import endpoint (`api_job_import_photos`); `:21008` its `_chain_after_import`; `:23100` `_enqueue_process_job`; `:19466` `api_job_move_folder` (guards ~19486-19549, resolution ~19551-19625, work closure + `runner.start` ~19626-19724).
- `vireo/templates/import.html:339` After-import card; `:429/:714` `importRemoteTargets`; `:2075` request body build; `:2269` `renderChainInfo`.
- `vireo/templates/settings.html:1625` `_rtField` rows; `:1684` save-side normalization.

---

### Task 1: `local_archive_root` on remote targets (config.py)

**Files:**
- Modify: `vireo/config.py`
- Test: `vireo/tests/test_config.py`

- [ ] **Step 1: Write failing tests**

Follow the existing remote-target coercion test style in `vireo/tests/test_config.py` (find it: `grep -n "remote_target" vireo/tests/test_config.py`). Add:

```python
def _base_target(**over):
    t = {"host": "nas", "user": "julius", "remote_path": "/volume1/Photos",
         "mount_path": "/Volumes/Photos"}
    t.update(over)
    return t


def test_remote_target_local_archive_root_passthrough(tmp_path, monkeypatch):
    _patch_config_path(tmp_path, monkeypatch)  # reuse the file's existing isolation helper
    save({"remote_targets": [_base_target(local_archive_root="/Users/julius/Photos")]})
    (t,) = get_remote_targets()
    assert t["local_archive_root"] == "/Users/julius/Photos"


def test_remote_target_local_archive_root_defaults_empty(tmp_path, monkeypatch):
    _patch_config_path(tmp_path, monkeypatch)
    save({"remote_targets": [_base_target()]})
    (t,) = get_remote_targets()
    assert t["local_archive_root"] == ""


def test_remote_target_local_archive_root_rejects_relative(tmp_path, monkeypatch):
    _patch_config_path(tmp_path, monkeypatch)
    save({"remote_targets": [_base_target(local_archive_root="Photos")]})
    (t,) = get_remote_targets()
    assert t["local_archive_root"] == ""  # blanked, target itself still valid


def test_remote_target_local_archive_root_rejects_inside_mount(tmp_path, monkeypatch):
    _patch_config_path(tmp_path, monkeypatch)
    save({"remote_targets": [_base_target(local_archive_root="/Volumes/Photos/sub")]})
    (t,) = get_remote_targets()
    assert t["local_archive_root"] == ""
```

Adapt helper names to what the file actually uses (it already isolates `cfg.CONFIG_PATH`).

- [ ] **Step 2: Run tests, verify they fail**

Run: `python -m pytest vireo/tests/test_config.py -k local_archive_root -v`
Expected: 4 failures (KeyError `local_archive_root`).

- [ ] **Step 3: Implement**

In `_coerce_remote_target` (vireo/config.py ~660), after the `mount_path` handling in the returned dict, add the field with validation. Invalid values are **blanked, not fatal** — the target stays usable for plain moves:

```python
    # Local directory that mirrors remote_path for chained import→process→
    # move runs. Empty = target never offers the chained move. Must be an
    # absolute local path and must not live inside mount_path (the mount is
    # the *destination* view of the NAS; the archive root is the local
    # staging side — pointing it at the mount would "move" files onto
    # themselves). Invalid values are blanked rather than dropping the
    # whole target.
    local_archive_root = (entry.get("local_archive_root") or "").strip()
    if local_archive_root:
        mount = (entry.get("mount_path") or "").strip()
        if not os.path.isabs(local_archive_root):
            local_archive_root = ""
        elif mount:
            try:
                if os.path.commonpath([
                    os.path.realpath(local_archive_root),
                    os.path.realpath(mount),
                ]) == os.path.realpath(mount):
                    local_archive_root = ""
            except ValueError:
                pass  # different drives (Windows) — cannot be inside
```

Add `"local_archive_root": local_archive_root,` to the returned dict, and extend the `DEFAULTS` comment block (vireo/config.py ~44) with one line documenting the field. `os` is already imported in config.py — verify.

- [ ] **Step 4: Run tests, verify pass**

Run: `python -m pytest vireo/tests/test_config.py -v`
Expected: all pass (new + pre-existing).

- [ ] **Step 5: Commit**

```bash
git add vireo/config.py vireo/tests/test_config.py
git commit -m "feat: local_archive_root field on remote targets"
```

---

### Task 2: Folder-derivation helper (`vireo/import_chain.py`)

**Files:**
- Create: `vireo/import_chain.py`
- Test: `vireo/tests/test_import_chain.py` (create)

- [ ] **Step 1: Write failing tests**

```python
import os

from import_chain import minimal_move_set


def test_single_folder(tmp_path):
    root = str(tmp_path)
    trip = os.path.join(root, "2026", "trip")
    assert minimal_move_set(root, [(7, trip)]) == [
        {"folder_id": 7, "subpath": "2026/trip"},
    ]


def test_nested_folder_collapses_to_ancestor(tmp_path):
    root = str(tmp_path)
    trip = os.path.join(root, "2026", "trip")
    raw = os.path.join(trip, "raw")
    out = minimal_move_set(root, [(7, trip), (8, raw)])
    assert out == [{"folder_id": 7, "subpath": "2026/trip"}]


def test_siblings_both_kept(tmp_path):
    root = str(tmp_path)
    a = os.path.join(root, "2026", "a")
    b = os.path.join(root, "2026", "b")
    out = minimal_move_set(root, [(1, a), (2, b)])
    assert {e["subpath"] for e in out} == {"2026/a", "2026/b"}


def test_folder_outside_root_skipped(tmp_path):
    root = str(tmp_path / "archive")
    outside = str(tmp_path / "elsewhere")
    assert minimal_move_set(root, [(1, outside)]) == []


def test_root_itself_skipped(tmp_path):
    root = str(tmp_path)
    assert minimal_move_set(root, [(1, root)]) == []
```

Import style: vireo modules import each other bare (`from import_chain import …`) — vireo/tests/conftest.py already puts `vireo/` on the path (verify with an existing test file's imports).

- [ ] **Step 2: Run, verify failure**

Run: `python -m pytest vireo/tests/test_import_chain.py -v`
Expected: ImportError (module doesn't exist).

- [ ] **Step 3: Implement `vireo/import_chain.py`**

```python
"""Chained-import helpers: which folders a chained NAS move relocates.

The chained import→process→move flow mirrors the local archive layout onto
the remote target: a folder at ``<local_archive_root>/2026/trip`` moves to
``<remote_path>/2026/trip``. This module derives the minimal non-nested set
of imported folders to move — moving an ancestor also moves its descendants,
so a nested destination folder must not get its own move job.
"""

import os


def minimal_move_set(archive_root, folders):
    """Return the minimal covering set of folders to move, with subpaths.

    ``folders`` is an iterable of ``(folder_id, path)`` for catalog folders
    that received imported photos. Returns a list of
    ``{"folder_id": int, "subpath": str}`` where ``subpath`` is the folder's
    path relative to ``archive_root`` in POSIX form (the move job's remote
    subpath). Folders outside the root — and the root itself — are skipped:
    request-time validation prevents both, so this is defensive, and moving
    the root would sweep unrelated shoots into the transfer.
    """
    root = os.path.realpath(archive_root)
    inside = []
    for folder_id, path in folders:
        real = os.path.realpath(path)
        try:
            common = os.path.commonpath([real, root])
        except ValueError:
            continue  # different drives — cannot be under the root
        if common != root or real == root:
            continue
        inside.append((folder_id, real))
    # Shortest paths first so ancestors are considered before descendants;
    # keep a folder only when no already-kept ancestor covers it.
    inside.sort(key=lambda item: (len(item[1]), item[1]))
    kept = []
    for folder_id, real in inside:
        covered = any(
            os.path.commonpath([real, kept_path]) == kept_path
            for _, kept_path in kept
        )
        if not covered:
            kept.append((folder_id, real))
    return [
        {
            "folder_id": folder_id,
            "subpath": os.path.relpath(real, root).replace(os.sep, "/"),
        }
        for folder_id, real in kept
    ]
```

- [ ] **Step 4: Run, verify pass**

Run: `python -m pytest vireo/tests/test_import_chain.py -v`
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add vireo/import_chain.py vireo/tests/test_import_chain.py
git commit -m "feat: minimal_move_set helper for chained NAS moves"
```

---

### Task 3: Request validation (`after_process_move`)

**Files:**
- Modify: `vireo/app.py` (next to `_validate_after_import`, ~20333; both import endpoints)
- Test: `vireo/tests/test_jobs_api.py`

- [ ] **Step 1: Write failing endpoint tests**

Model on `test_import_photos_happy_path` (vireo/tests/test_jobs_api.py:3383) and its `_import_card` helper. The config in these tests is isolated the same way the file already isolates it — reuse whatever fixture pattern surrounds the remote-import tests (grep `remote_target` in the file). Add a helper that saves a usable target:

```python
def _save_nas_target(tmp_path, local_root=None):
    import config as cfg
    target = {
        "id": "nas1", "name": "NAS", "host": "nas.local", "user": "julius",
        "remote_path": "/volume1/Photos", "mount_path": str(tmp_path / "mnt"),
    }
    if local_root is not None:
        target["local_archive_root"] = str(local_root)
    current = cfg.load()
    current["remote_targets"] = [target]
    cfg.save(current)
    return target


def test_after_process_move_requires_after_import(app_and_db, tmp_path):
    app, db = app_and_db
    client = app.test_client()
    card = _import_card(tmp_path)
    dest = str(tmp_path / "archive" / "sub")
    _save_nas_target(tmp_path, local_root=tmp_path / "archive")
    resp = client.post("/api/jobs/import-photos", json={
        "sources": [card], "destination": dest, "after_import": None,
        "after_process_move": {"remote_target_id": "nas1"},
    })
    assert resp.status_code == 400
    assert "after_import" in resp.get_json()["error"]


def test_after_process_move_unknown_target(app_and_db, tmp_path):
    ...  # same shape: target id "nope" -> 400 mentioning the id


def test_after_process_move_target_without_root(app_and_db, tmp_path):
    ...  # _save_nas_target(tmp_path) with no local_root -> 400 mentioning "local archive root"


def test_after_process_move_destination_outside_root(app_and_db, tmp_path):
    ...  # destination tmp_path/"elsewhere" -> 400 mentioning "archive root"


def test_after_process_move_rejected_for_remote_destination(app_and_db, tmp_path):
    ...  # remote_target_id import + after_process_move -> 400


def test_import_in_place_rejects_after_process_move(app_and_db, tmp_path):
    ...  # POST /api/jobs/import-in-place with after_process_move -> 400
```

Write all six as real tests (bodies analogous to the first).

- [ ] **Step 2: Run, verify failures**

Run: `python -m pytest vireo/tests/test_jobs_api.py -k after_process_move -v`
Expected: failures — endpoint currently ignores the key (imports start, returning 200).

- [ ] **Step 3: Implement `_validate_after_process_move`**

Add next to `_validate_after_import` (vireo/app.py ~20351):

```python
    def _validate_after_process_move(value, after_import, destination):
        """Validate an after_process_move spec; return (target_snapshot, error).

        ``None`` value → (None, None). Otherwise the value must name a saved
        remote target that has a local_archive_root containing ``destination``,
        and the run must chain a process (the move fires from the process
        job's completion hook — an import-only move is just the Move page).
        The returned target dict is the enqueue-time snapshot: a Settings
        edit mid-chain must not redirect the move (same rationale as
        remote_target_snapshot).
        """
        if value is None:
            return None, None
        if not isinstance(value, dict):
            return None, json_error(
                "after_process_move must be an object or null, got "
                f"{type(value).__name__}")
        target_id = (value.get("remote_target_id") or "").strip()
        if not target_id:
            return None, json_error(
                "after_process_move.remote_target_id required")
        if after_import is None:
            return None, json_error(
                "after_process_move requires after_import — the move chains "
                "off the processing run; for a move without processing use "
                "the Move page")
        import config as cfg
        target = cfg.get_remote_target(target_id)
        if target is None:
            return None, json_error(f"unknown remote target: {target_id}")
        root = (target.get("local_archive_root") or "").strip()
        if not root:
            return None, json_error(
                "this remote target has no local archive root — set one "
                "under Settings → Remote targets")
        try:
            inside = os.path.commonpath([
                os.path.realpath(destination), os.path.realpath(root),
            ]) == os.path.realpath(root)
        except ValueError:
            inside = False
        if not inside:
            return None, json_error(
                "destination is not inside the remote target's local "
                f"archive root ({root})")
        return target, None
```

- [ ] **Step 4: Wire into both endpoints**

In `api_job_import_photos` (~20714), after the existing `after_import` resolution block (grep `explicit_after_import` in the second endpoint, ~21000s — the copy endpoint has the same pattern as ~20425):

```python
        after_process_move = body.get("after_process_move")
        if after_process_move is not None and remote_target_id:
            return json_error(
                "after_process_move requires a local archive destination — "
                "a remote-destination import already lands on the NAS")
        move_target_snapshot, err = _validate_after_process_move(
            after_process_move, after_import, destination)
        if err is not None:
            return err
```

In `api_job_import_in_place` (~20386), reject the key outright:

```python
        if body.get("after_process_move") is not None:
            return json_error(
                "after_process_move is not supported for import-in-place — "
                "photos stay where they are; use Copy to archive")
```

Also record the choice in the copy endpoint's `job_config` (find where `after_import` is stored — the happy-path test asserts `config["after_import"]`) so the jobs panel can show it:

```python
        if move_target_snapshot is not None:
            job_config["after_process_move"] = {
                "remote_target_id": move_target_snapshot["id"],
                "target_name": move_target_snapshot["name"],
            }
```

- [ ] **Step 5: Run, verify pass**

Run: `python -m pytest vireo/tests/test_jobs_api.py -k after_process_move -v` → all pass.
Also: `python -m pytest vireo/tests/test_jobs_api.py -q` → no regressions.

- [ ] **Step 6: Commit**

```bash
git add vireo/app.py vireo/tests/test_jobs_api.py
git commit -m "feat: validate after_process_move on import endpoints"
```

---

### Task 4: Extract move-folder guards + job start into shared helpers

Pure refactor — behavior identical, existing tests are the safety net. The chain hook (Task 5) runs on a job thread with no request context, so the guard block and the work-closure/`runner.start` block must be callable outside the endpoint.

**Files:**
- Modify: `vireo/app.py` (`api_job_move_folder`, ~19466-19724)

- [ ] **Step 1: Extract `_move_folder_guard_error(guard_db, folder_id)`**

Move the guard stack (the `stage_boundary_lock()` block: `local_root_for_folder`, `local_root_under_folder`, `folder_has_local_workspace`, pending-transition check — vireo/app.py ~19486-19549) into a function defined above the endpoint that returns an error **string** or `None`. It takes the db explicitly (endpoint passes `_get_db()`; the chain passes its thread db). Keep the lock inside the helper. The endpoint converts a non-None return to `json_error(msg, 409)`.

- [ ] **Step 2: Extract `_start_move_folder_job(...)`**

Move the work closure + `runner.start` (~19626-19724) into:

```python
    def _start_move_folder_job(runner, workspace_id, *, folder_id,
                               destination, display_dest, destination_name,
                               merge, remote, developed_dir,
                               chained_from=None):
```

Body identical to today's closure (progress callback, `move_folder(...)` call, ok/summary shaping, cache invalidation). Add to `job_config`:

```python
        if chained_from:
            job_config["chained_from"] = chained_from
```

The endpoint's tail becomes: resolve destination/remote exactly as today, then `job_id = _start_move_folder_job(runner, active_ws, folder_id=folder_id, destination=destination, display_dest=display_dest, destination_name=destination_name, merge=merge, remote=remote, developed_dir=developed_dir)`.

- [ ] **Step 3: Run the move tests**

Run: `python -m pytest vireo/tests/test_jobs_api.py -k move -v` and `python -m pytest tests/ vireo/tests/test_app.py -q`
Expected: pass, unchanged behavior.

- [ ] **Step 4: Commit**

```bash
git add vireo/app.py
git commit -m "refactor: extract move-folder guards and job start for reuse"
```

---### Task 5: The process→move chain link

**Files:**
- Modify: `vireo/app.py` (`_chain_after_import` ×2, `_enqueue_process_job`, new `_enqueue_move_folder_job` + `_chain_after_move`)
- Test: `vireo/tests/test_jobs_api.py`

- [ ] **Step 1: Write failing chain tests**

The move machinery is stubbed — these tests exercise the chain, not rsync. `move.move_folder` is imported *inside* the job's work closure (`from move import move_folder`), so monkeypatch the `move` module attribute. `resolve_rsync_bin`/`resolve_ssh_bin` must return something truthy.

```python
@pytest.fixture
def stub_move(monkeypatch):
    import move as move_mod
    calls = []

    def fake_move_folder(db, folder_id, destination, progress_cb=None,
                         developed_dir="", merge=False, remote=None,
                         destination_name=""):
        calls.append({"folder_id": folder_id, "destination": destination,
                      "merge": merge, "remote": remote})
        return {"moved": 1, "errors": []}

    monkeypatch.setattr(move_mod, "move_folder", fake_move_folder)
    monkeypatch.setattr(move_mod, "resolve_rsync_bin", lambda v: "/usr/bin/rsync")
    monkeypatch.setattr(move_mod, "resolve_ssh_bin", lambda v: "/usr/bin/ssh")
    return calls


def _run_chained_import(client, tmp_path, cull_ready_id):
    card = _import_card(tmp_path)
    dest = str(tmp_path / "archive")
    _save_nas_target(tmp_path, local_root=dest)
    resp = client.post("/api/jobs/import-photos", json={
        "sources": [card], "destination": dest,
        "after_import": cull_ready_id, "trust_likely_duplicates": True,
        "after_process_move": {"remote_target_id": "nas1"},
    })
    assert resp.status_code == 200, resp.get_json()
    return wait_for_job_via_client(client, resp.get_json()["job_id"])


def test_chain_happy_path_enqueues_moves(app_and_db, tmp_path, stub_move):
    app, db = app_and_db
    client = app.test_client()
    cull_ready_id = next(p["id"] for p in db.get_saved_processes()
                         if p["name"] == "Cull-ready")
    import_job = _run_chained_import(client, tmp_path, cull_ready_id)
    assert import_job["status"] == "completed"
    process_job = wait_for_job_via_client(
        client, import_job["result"]["process_job_id"])
    move_ids = process_job["result"]["move_job_ids"]
    assert move_ids
    for mid in move_ids:
        mj = wait_for_job_via_client(client, mid)
        assert mj["status"] == "completed", mj
        assert _job_config(client, mid)["chained_from"] == import_job["result"]["process_job_id"]
    assert all(c["merge"] is True and c["remote"] is not None
               for c in stub_move)


def test_chain_moves_even_when_process_raises(app_and_db, tmp_path, stub_move, monkeypatch):
    import pipeline_job

    def boom(*a, **k):
        raise RuntimeError("model resolution failed")

    monkeypatch.setattr(pipeline_job, "run_pipeline_job", boom)
    ...  # run chain; process job ends failed; assert stub_move received calls


def test_chain_skips_move_on_cancelled_process(app_and_db, tmp_path, stub_move, monkeypatch):
    import pipeline_job
    monkeypatch.setattr(pipeline_job, "run_pipeline_job",
                        lambda *a, **k: {"cancelled": True, "stages": {}})
    ...  # run chain; assert stub_move is empty and process result has
         # after_move_skipped == "process cancelled"
```

Write the elided bodies fully. Note `_enqueue_process_job` does `from pipeline_job import ... run_pipeline_job` at call time, so `monkeypatch.setattr(pipeline_job, "run_pipeline_job", ...)` works only if the import happens *after* the patch — it does (the import runs when the chain fires). Check how `wait_for_job_via_client` and `_job_config` are defined in this test file and reuse them.

- [ ] **Step 2: Run, verify failures**

Run: `python -m pytest vireo/tests/test_jobs_api.py -k chain -v`
Expected: happy path fails on missing `move_job_ids`.

- [ ] **Step 3: Implement `_enqueue_move_folder_job`**

Next to `_enqueue_process_job` (~23100). Thread-side mirror of the endpoint's remote resolution — no request context:

```python
    def _enqueue_move_folder_job(thread_db, runner, workspace_id, *,
                                 folder_id, subpath, target,
                                 chained_from=None):
        """Enqueue a chained remote move for one imported folder.

        Job-thread path into move-folder (no request context). ``target`` is
        the snapshot captured when the import was enqueued. Raises on any
        precondition failure — the caller records the failure per folder
        rather than aborting the batch.
        """
        import config as cfg
        import move as move_mod

        guard = _move_folder_guard_error(thread_db, folder_id)
        if guard:
            raise RuntimeError(guard)
        effective_cfg = thread_db.get_effective_config(cfg.load())
        rsync_bin = move_mod.resolve_rsync_bin(
            effective_cfg.get("rsync_bin", "") or "")
        if not rsync_bin:
            raise RuntimeError("no usable GNU rsync for remote moves")
        ssh_bin = move_mod.resolve_ssh_bin(
            effective_cfg.get("ssh_bin", "") or "")
        if not ssh_bin:
            raise RuntimeError("OpenSSH client not found")
        remote = move_mod.build_remote_move_spec(
            target, subpath, rsync_bin, ssh_bin)
        return _start_move_folder_job(
            runner, workspace_id,
            folder_id=folder_id,
            destination=remote["mount_dest_base"],
            display_dest=move_mod.rsync_dest_spec(
                target, remote["ssh_dest_base"]),
            destination_name="",
            merge=True,
            remote=remote,
            developed_dir=effective_cfg.get("darktable_output_dir", "") or "",
            chained_from=chained_from,
        )
```

Mirror the endpoint's mount-path preconditions: if `target.get("mount_path")` is empty or relative, raise with the same wording the endpoint uses.

- [ ] **Step 4: Implement `_chain_after_move` + thread `after_move` through `_enqueue_process_job`**

`_enqueue_process_job` gains `after_move=None` — `{"target": dict, "folders": [{"folder_id", "subpath"}]}`. Wrap its `work` so the hook runs on the raise path too (spec planning note 1):

```python
        def work(job):
            result = None
            try:
                result = run_pipeline_job(
                    job, runner, db_path, workspace_id, params,
                    thumb_cache_dir=app.config["THUMB_CACHE_DIR"],
                    missing_originals_invalidator=_invalidate_missing_originals_cache,
                )
                return result
            finally:
                if after_move:
                    _chain_after_move(job, result, after_move, workspace_id)
```

```python
    def _chain_after_move(job, result, after_move, workspace_id):
        """Enqueue the chained NAS moves when a chained process run ends.

        Decision table (spec §4): fires on success AND failure (a processing
        failure must not strand photos off the NAS); skips only on an
        explicit cancel. ``result`` is None when run_pipeline_job raised.
        """
        runner = app._job_runner
        skip = None
        if runner.is_cancelled(job["id"]) or (
                isinstance(result, dict) and result.get("cancelled")):
            skip = "process cancelled"
        elif not after_move.get("folders"):
            skip = "no folders to move"
        if skip:
            if isinstance(result, dict):
                result["after_move_skipped"] = skip
            log.info("after-process move skipped: %s", skip)
            return
        thread_db = Database(db_path)
        thread_db.set_active_workspace(workspace_id)
        move_ids, failures = [], []
        for entry in after_move["folders"]:
            try:
                move_ids.append(_enqueue_move_folder_job(
                    thread_db, runner, workspace_id,
                    folder_id=entry["folder_id"],
                    subpath=entry["subpath"],
                    target=after_move["target"],
                    chained_from=job["id"],
                ))
            except Exception as e:
                log.exception(
                    "after-process move enqueue failed for folder %s",
                    entry["folder_id"])
                failures.append(f"{entry['subpath']}: {e}")
        if isinstance(result, dict):
            if move_ids:
                result["move_job_ids"] = move_ids
            if failures:
                result["after_move_errors"] = failures
```

- [ ] **Step 5: Compute the folder set in `_chain_after_import` (Link 1) and pass it through**

Spec planning note 2: the folder set is computed at import-completion time (Link 1 scope) and threaded through. In the **copy** endpoint's `_chain_after_import` (~21008), before `_enqueue_process_job`:

```python
            after_move = None
            if move_target_snapshot is not None:
                from import_chain import minimal_move_set
                folder_rows = []
                for i in range(0, len(photo_ids), 500):
                    chunk = photo_ids[i:i + 500]
                    ph = ",".join("?" * len(chunk))
                    folder_rows.extend(thread_db.conn.execute(
                        "SELECT DISTINCT f.id, f.path FROM photos p "
                        "JOIN folders f ON f.id = p.folder_id "
                        f"WHERE p.id IN ({ph})", chunk).fetchall())
                root = move_target_snapshot["local_archive_root"]
                after_move = {
                    "target": move_target_snapshot,
                    "folders": minimal_move_set(
                        root, [(r["id"], r["path"]) for r in folder_rows]),
                }
```

Pass `after_move=after_move` into `_enqueue_process_job` and add the kwarg to its signature. The in-place endpoint's `_chain_after_import` passes nothing (it rejects the option at request time). Also record intent on the import result for the result card: `result["after_process_move_planned"] = {"target_name": ..., "folders": [...]}` when `after_move` is set.

- [ ] **Step 6: Run, verify pass**

Run: `python -m pytest vireo/tests/test_jobs_api.py -k "chain or after_process_move" -v` → all pass.
Then: `python -m pytest vireo/tests/test_jobs_api.py -q` → no regressions.

- [ ] **Step 7: Commit**

```bash
git add vireo/app.py vireo/tests/test_jobs_api.py
git commit -m "feat: chain NAS move after chained processing completes"
```

---

### Task 6: Import page UI

**Files:**
- Modify: `vireo/templates/import.html`

No JS unit tests exist in this repo; correctness here is covered by the endpoint tests plus manual verification (Task 8). Keep every string honest per the UI-transparency rule: the preview must state exactly what will happen with current selections.

- [ ] **Step 1: Markup — extend the After-import card (line ~339)**

After the existing `.hint` div inside the card:

```html
      <div class="row" id="afterMoveRow" style="display:none; margin-top:10px;">
        <label><input type="checkbox" id="chkAfterMove" onchange="updateAfterMoveUI()"> Then move to NAS</label>
        <select id="afterMoveTarget" style="max-width:220px;" onchange="updateAfterMoveUI()"></select>
      </div>
      <div class="hint" id="afterMoveUnavailable" style="display:none;"></div>
      <div class="remote-preview" id="afterMovePreview" style="display:none;"></div>
```

- [ ] **Step 2: JS — eligibility, preview, request body**

Add near the existing After-import functions (~1194):

```javascript
// A remote target can host the chained move only when the chosen archive
// destination sits inside its local_archive_root. Client-side prefix check
// mirrors the server's realpath/commonpath validation closely enough for
// UI gating; the server remains the authority.
function afterMoveEligibleTargets() {
  const dest = (document.getElementById('destInput').value || '').trim();
  if (!dest) return [];
  return importRemoteTargets.filter(function(t) {
    const root = (t.local_archive_root || '').trim();
    if (!root) return false;
    const norm = function(p) { return p.replace(/\/+$/, '') + '/'; };
    return norm(dest).indexOf(norm(root)) === 0;
  });
}

function updateAfterMoveUI() {
  const row = document.getElementById('afterMoveRow');
  const unavailable = document.getElementById('afterMoveUnavailable');
  const preview = document.getElementById('afterMovePreview');
  const sel = document.getElementById('afterMoveTarget');
  const isCopyLocal = /* copy mode selected AND no remote destination —
                         reuse the page's existing mode helpers */;
  const processChosen = /* afterImportSelect value is a process id */;
  row.style.display = 'none';
  unavailable.style.display = 'none';
  preview.style.display = 'none';
  if (!isCopyLocal || !processChosen) return;
  const eligible = afterMoveEligibleTargets();
  if (!eligible.length) {
    unavailable.style.display = '';
    unavailable.textContent =
      'Move to NAS unavailable: the destination is not inside any remote ' +
      "target's local archive root. Set one under Settings → Remote targets.";
    return;
  }
  row.style.display = '';
  // (Re)fill the dropdown, preserving selection; auto-select when single.
  ...
  if (document.getElementById('chkAfterMove').checked) {
    const t = eligible.find(...selected...) || eligible[0];
    const dest = document.getElementById('destInput').value.trim();
    const rel = dest.replace(/\/+$/, '').slice(
      t.local_archive_root.replace(/\/+$/, '').length).replace(/^\/+/, '');
    preview.style.display = '';
    preview.textContent =
      'After processing, each imported folder moves to ' +
      t.remote_path.replace(/\/+$/, '') + (rel ? '/' + rel : '') +
      '/<imported folder> and the catalog repoints to ' + t.mount_path + '.';
  }
}
```

Fill in the elided pieces against the page's actual helpers (`updateImportMode`, `destMode`, `afterImportSelect` handling). Hook `updateAfterMoveUI()` into: destination input changes, import-mode changes, After-import select changes, and the `importRemoteTargets` load callback (~714).

In `startImport()`'s body build (~2075):

```javascript
    body.after_process_move =
      (document.getElementById('chkAfterMove').checked &&
       document.getElementById('afterMoveRow').style.display !== 'none')
        ? {remote_target_id: document.getElementById('afterMoveTarget').value}
        : null;
```

- [ ] **Step 3: Result card — surface the chain state**

Extend `renderChainInfo` (~2269): when the import result has `after_process_move_planned`, append a line "Processing will be followed by a move to NAS (<target name>)." The process job's own result (`move_job_ids` / `after_move_skipped` / `after_move_errors`) is visible in the Jobs panel via the shared job-result rendering — verify the panel prints unknown result keys; if it doesn't, add these three to whatever result-summary formatter the bottom panel uses (check `_navbar.html`).

- [ ] **Step 4: Verify the endpoint the page loads targets from includes the new field**

`importRemoteTargets` is filled from an existing fetch (~714). The server side returns coerced targets (Task 1 adds the field to coercion), so `local_archive_root` flows through — confirm by grepping the endpoint that serves it and reading its response construction.

- [ ] **Step 5: Commit**

```bash
git add vireo/templates/import.html
git commit -m "feat: Then-move-to-NAS option in the After import card"
```

---

### Task 7: Settings UI

**Files:**
- Modify: `vireo/templates/settings.html`

- [ ] **Step 1: Add the field to the remote-target row (~1625)**

Next to the existing `_rtField('Remote path (NAS side)', ...)` / mount-path rows:

```javascript
    r3.appendChild(_rtField('Local archive root (chained moves)', t.local_archive_root,
      '/Users/you/Photos', save('local_archive_root'), {flex: '1', mono: true}));
```

Follow the row-layout pattern the surrounding fields use, and add a hint line matching the section's style: "Local folder that mirrors the remote path. Imports under this folder can chain an automatic move to this target after processing."

- [ ] **Step 2: Include the field in save-side normalization (~1684)**

Add `local_archive_root: asStr(t.local_archive_root),` to the normalized object, and `local_archive_root: ''` to the new-target template (~1666).

- [ ] **Step 3: Commit**

```bash
git add vireo/templates/settings.html
git commit -m "feat: local archive root field in remote target settings"
```

---

### Task 8: Full suite, manual verification, PR

- [ ] **Step 1: Full test run (CLAUDE.md suite)**

Run: `python -m pytest tests/test_workspaces.py vireo/tests/test_db.py vireo/tests/test_app.py vireo/tests/test_photos_api.py vireo/tests/test_edits_api.py vireo/tests/test_jobs_api.py vireo/tests/test_darktable_api.py vireo/tests/test_config.py vireo/tests/test_import_chain.py -v`
Expected: all pass.

- [ ] **Step 2: Manual smoke (superpowers:verification-before-completion + /verify)**

Launch `python vireo/app.py --db /tmp/vireo-chain-test/vireo.db --port 8081`. In Settings add a remote target with a `local_archive_root` under /tmp; on Import pick a destination under it, select a process, confirm the "Then move to NAS" row appears with an honest preview; confirm the row shows the unavailable hint when the destination is outside the root. (The actual SSH move can't run without a NAS — the chain tests cover that path with stubs.)

- [ ] **Step 3: PR**

```bash
git push -u origin local-disk-processing-nas-sync
gh pr create --base main --title "Chained import → process → move-to-NAS workflow" \
  --body "$(cat <<'EOF'
## What
Restores the retired local-processing workflow's key property as a chain of
existing jobs: import to a fast local archive, process locally, then move
each imported folder to a NAS remote target automatically.

- `local_archive_root` on remote targets maps the local archive to the NAS root
- "Then move to NAS" option in the Import page's After-import card
- Process job completion hook enqueues merge-mode move-folder jobs
  (fires on process failure, skips on explicit cancel — see spec §4)
- Spec: docs/superpowers/specs/2026-07-19-import-process-move-chain-design.md

## Tests
[paste suite summary]

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Execution notes

- Line numbers in this plan were taken from the current branch head — re-grep anchors before each edit; app.py is 23k+ lines and drifts.
- `vireo/app.py` closures capture `db_path`, `app`, `log`, `runner` from `create_app` scope — all new helpers in Tasks 4-5 live inside `create_app` like their neighbors.
- DRY: Task 4's extraction is what keeps Task 5 from duplicating the endpoint's move setup. Don't skip it.
- The chain is intentionally in-memory (spec: out of scope to persist across restarts). Do not add persistence.
