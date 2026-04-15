# New Images Detected Banner

## Problem

When a workspace has mapped folders containing photos that aren't yet in the database, users see an empty (or stale) browse grid with no explanation and no guidance on what to do. This happens in two common situations:

- **First scan** — the workspace's folders were mapped but never scanned (DB count = 0, disk count > 0).
- **Incremental** — the user dropped new files into a mapped folder since the last scan (DB count < disk count).

Currently, unreachable folders are already surfaced via the "Missing Folders" banner in `_navbar.html`. There is no equivalent surface for the reachable-but-out-of-date case.

## Design Decisions

- **Single uniform banner** for first-scan and incremental-scan cases — user doesn't need to care about the distinction.
- **Global banner**, same visual pattern as the existing Missing Folders banner in `_navbar.html`.
- **Click target is `/pipeline`** — the existing pipeline page already has a "Scan & Import" stage (Card 3 in `pipeline.html`) that will discover and ingest new files. No intermediate modal.
- **Aggregate count** across all mapped folders in the banner ("47 new images detected"). Per-folder breakdown is already shown in the pipeline page's Scan stage.
- **Unreachable folders are out of scope** — handled by the existing Missing Folders system. This banner only runs the check against reachable folders.
- **Filesystem-truthful detection** — set-diff disk filenames vs DB filenames per folder, not a count comparison. The banner's "N new images" must be honest.
- **Cached, but honest** — in-memory per-workspace cache, invalidated on scan-job completion and gated by folder mtime on every workspace open. No arbitrary TTL.
- **Async on workspace open** — page renders immediately; banner appears when check completes. Never blocks workspace switch.

## Detection Logic

New helper in `vireo/scanner.py` (or adjacent module) that, given a folder path and the set of known paths (relative to the folder root) from the DB, returns the count and sample of image files on disk not yet ingested.

Pseudocode:

```python
def count_new_images(folder_path: str, known_rel_paths: set[str]) -> dict:
    """Return {"new_count": int, "sample": list[str]} for a folder.

    Walks folder_path recursively, filters to image extensions, and
    returns count and up to 5 sample *relative* paths of files whose
    relative path is not in known_rel_paths.
    """
```

- **Path-based identity, not basename** — keys are paths relative to the folder root (e.g. `a/b/IMG_0001.JPG`), so two subdirectories with the same filename are not conflated. This matches the scan job's own identity model (`existing_by_path` in `vireo/scanner.py`). Basename-only keying would undercount whenever camera exports produce repeating names like `IMG_0001.JPG` across subfolders.
- **Image extensions** — reuse the canonical list from `vireo/scanner.py` (whatever the scan job uses). Do not re-invent.
- **Recursive** — matches the scan job's behavior.
- **Sample** — up to 5 relative paths, useful for debug/logging and potentially surfacing in the banner on hover.

## Caching

In-memory cache on the `Database` instance (or a sibling `WorkspaceHealthCache`) keyed by `(workspace_id, folder_id)`:

```python
{
    (workspace_id, folder_id): {
        "new_count": int,
        "sample": list[str],
        "checked_at": float,
    }
}
```

**Invalidation triggers:**

1. **Scan-job completion** — when a scan job finishes for a workspace, clear cache entries for that workspace. The next workspace open recomputes.
2. **TTL ceiling** — cache entries expire after 5 minutes. This is the staleness ceiling: a user who imports files via Finder (outside Vireo) will see the banner appear within 5 min on any workspace-related page load. Chosen over `mtime`-based gating because directory mtime only bubbles up one level — adding `/root/A/B/new.jpg` updates `B` but not necessarily `A`, so a shallow mtime check misses deep additions. A full recursive mtime walk would cost roughly the same as the actual filename diff, so it's not a meaningful optimization.
3. **Manual refresh** — the banner can include a small "Check now" affordance that bypasses the cache. Low priority; the TTL + scan invalidation covers the common cases.

**Cache lifetime is process-only** — lost on app restart. First workspace open after restart repopulates.

**Why a walk on every cache miss is OK:** directory listing (dirent-only, no per-file stat beyond what `os.walk` already does) is fast even on NAS for 10k+ files — typically well under a second. The expensive part of scanning is image decode / EXIF / hash, none of which the detection check does.

## Surface: Banner in `_navbar.html`

New banner block alongside the existing Missing Folders banner:

```html
<div class="new-images-banner" id="newImagesBanner" style="display:none;">
  <span id="newImagesMsg"></span>
  <a href="/pipeline">Create a pipeline</a>
  <button class="banner-dismiss" onclick="dismissNewImagesBanner()">&times;</button>
</div>
```

**Wording:** `"N new images detected in your registered folders."` (singular: "1 new image detected…")

**Dismissal:** Click `×` hides the banner for the current session (same pattern as Missing Folders — `sessionStorage` flag). Banner re-appears on next app load if the condition still holds.

**Precedence with Missing Folders banner:** If a workspace has both missing folders and new images in its reachable folders, show **both banners stacked**, Missing Folders on top (higher severity — blocks some workflows).

## API

New endpoint: `GET /api/workspace/new-images`

Response:
```json
{"new_count": 47, "per_folder": [{"folder_id": 7, "path": "/Volumes/.../USA/2026", "new_count": 47}]}
```

- Respects the cache described above.
- Runs only against reachable folders (skip any with `folders.status = 'missing'`).
- Called asynchronously by `_navbar.html` JS shortly after workspace switch completes.

## Frontend Behavior

1. On workspace switch (or page load while inside a workspace), navbar JS fires `GET /api/workspace/new-images`.
2. If `new_count > 0` and banner not dismissed this session: show banner with aggregate count.
3. User clicks "Create a pipeline" → routed to `/pipeline`. Pipeline page's existing Scan & Import stage handles the rest.
4. When a scan job completes (detected via existing job-progress stream or by polling `/api/jobs`), navbar JS re-fires the endpoint. If `new_count` drops to 0, banner hides automatically.

## Implementation Scope

**Backend (`vireo/db.py`, `vireo/scanner.py`, `vireo/app.py`):**
- `count_new_images()` helper (scanner module)
- In-memory cache structure, mtime-gated
- `db.get_new_images_for_workspace()` aggregating over reachable workspace folders
- Cache invalidation hook in the scan job's completion path
- New route: `GET /api/workspace/new-images`

**Frontend (`vireo/templates/_navbar.html`):**
- New banner DOM + CSS (mirror the Missing Folders banner)
- Fetch + render logic on workspace switch / page load
- Session-scoped dismissal
- Auto-refresh hook when scan jobs complete

**Tests (`vireo/tests/`):**
- Unit test for `count_new_images` (filename diff, extension filtering, recursion)
- Cache invalidation test (mtime change → re-walk; scan completion → clear)
- API endpoint test (`GET /api/workspace/new-images` with fixtures for reachable/unreachable folders, empty/populated DB)

**No changes to:**
- `pipeline.html` or the pipeline job itself
- Missing Folders system
- The scan job's own new-file detection logic (reused)

## Open Questions

- **Cost of the recursive walk on NAS** — expected to be sub-second for 10k files, but measure during implementation. If it turns out to be slow, fall back to running the walk in a background thread and streaming the result to the banner via SSE or polling rather than blocking the API response.
- **Should dismissal be per-workspace or app-wide?** — current plan is app-wide session dismissal. Reasonable alternative: per-workspace, so dismissing in USA2026 doesn't hide it in a different workspace. Lean toward per-workspace; decide during implementation.
- **TTL value** — 5 minutes is a guess. If users routinely import while Vireo is open and find the lag annoying, shorten to 60s. If the walk turns out to be slow and users notice re-walks, lengthen.
