# Vireo storage philosophy and per-model caching

## Goal

Write down a clear, opinionated philosophy for **where every piece of
derived data lives** in Vireo's database, so that (a) the same model on the
same photo never gets computed twice, (b) switching workspaces never loses
information, and (c) adding a new model version always adds a row rather
than overwriting one. Then audit the current schema against that philosophy
and plan the work to close every violation.

PR #636 ("detection-cache-global") established this pattern for detections
and predictions: detections are keyed on `(photo_id, detector_model)`,
predictions on `(detection_id, classifier_model, labels_fingerprint,
species)`, skip sets in `detector_runs` / `classifier_runs`, and review
state factored into the workspace-scoped `prediction_review`. The present
doc generalizes that pattern to cover every other derived column and lays
out a phased roadmap for closing the remaining gaps — the most visible of
which is that `photos.embedding` is overwritten by whichever classifier
most recently ran, silently discarding earlier-model results.

## Philosophy

Three categories, mutually exclusive, cover every piece of derived data on
disk. Plus a fourth orthogonal axis for what gets mirrored out to the
photo's XMP sidecar.

### Category 1 — Deterministic model output

Any artifact that is a pure function of `(photo_pixels, model_identifier
[, variant])`:

- MUST be stored globally, keyed on both the photo and the model
- MUST NOT be overwritten when a different model runs
- Running *model X* on *photo Y* twice is a lookup
- Running *model X2* on *photo Y* creates a new row alongside *X*'s

Examples: a MegaDetector bounding box, a BioCLIP embedding, a SAM2 mask
path, an eye-detector point, a pipeline feature extracted from the mask.

**Corollary — "runs" skip sets.** For every Category 1 artifact we keep a
`<kind>_runs` table with PK `(photo_id, model, variant)` that records
*"the model was run on this photo and produced its output"*, independent
of whether any rows landed. This is how we skip re-running on empty-scene
photos where the "output" is a legitimate zero. PR #636 introduced
`detector_runs` and `classifier_runs`; every future per-model cache needs
the same.

### Category 2 — Workspace judgment

Any artifact that reflects a user's decision in a specific workspace:

- Lives in a workspace-scoped side table, keyed on `(artifact_id,
  workspace_id)`
- Absence is semantically meaningful — e.g. `status = 'pending'` by default

Examples: prediction status, individual assignment, group membership,
reviewed timestamp, collection membership, pending changes, edit history.

### Category 3 — Non-model-dependent facts

Anything derivable from pixels or file metadata alone, with no versioned
model in the loop:

- Stays as a single-slot column on the global `photos` row
- Algorithm is (effectively) stable — if we ever version-bump, the one-time
  migration is acceptable

Examples: pHash, file size, file mtime, EXIF data (focal length, GPS),
timestamp, width/height, thumbnail path.

### The test

For any piece of data, ask three questions:

1. *If I run this same model on this same photo tomorrow, would I want the
   old result back?* → **Yes means Category 1** (per-model cache)
2. *If another workspace in the same DB made a different choice here,
   would the two workspaces want different values?* → **Yes means Category
   2** (workspace-scoped)
3. *Neither of the above?* → **Category 3** (single slot)

If the answer is ambiguous, default to Category 1 with a tag — it costs one
extra column and buys future optionality for free.

### Fourth axis — Sync to disk (XMP)

Orthogonal to where data lives **inside** Vireo: which fields does Vireo
mirror **out** to the photo's XMP sidecar? Category 1/2/3 governs the
former; this axis governs the latter.

Policy by category (defaults):

- **Cat 1 model output** — never to XMP. Detections, predictions,
  embeddings, masks are Vireo-internal and pollute multi-tool workflows
  (Lightroom, Capture One, exiftool all expect XMP to carry user intent,
  not model state).
- **Cat 2 workspace judgment** — opt-in per workspace, default off. If any
  workspace opts in for a given field, that workspace's value is what gets
  written. Multiple workspaces opting in for the same field is a
  user-configured conflict we warn on but don't prevent.
- **Cat 3 photo-level user metadata** — opt-in per workspace, default
  matches current behavior (`keywords: on`, `rating: on`, `flag: off`).
  Workspaces that don't own XMP writeback for a field still write it to
  the DB; they just don't touch the file.

Policy storage — `workspaces.config_overrides` JSON (already used for
`detector_confidence`), nested under `xmp_writeback`:

```json
{
  "xmp_writeback": {
    "keywords": true,
    "rating": true,
    "flag": false,
    "color_label": false
  }
}
```

On upgrade, every existing workspace gets `{keywords: true, rating: true,
flag: false}` — a pure no-op relative to current behavior.

## Audit — current schema against the philosophy

### Already compliant (post-PR #636)

- **Category 1:** `detections` + `detector_runs`, `predictions` +
  `classifier_runs`, `labels_fingerprints`.
- **Category 2:** `prediction_review`, `collections`, `pending_changes`,
  `edit_history`.
- **Category 3:** `photos.phash`, `phash_crop`, `file_hash`, `file_size`,
  `file_mtime`, `xmp_mtime`, `timestamp`, `width`, `height`, `latitude`,
  `longitude`, `focal_length`, `exif_data`, `extension`, `filename`,
  `burst_id`, `companion_path`, `working_copy_path`, `thumb_path`.
- **Intentionally global user metadata** (XMP convention): `photos.rating`,
  `photo_keywords`.

### Violations — Category 1 stored as single-slot

| Column(s) | Producer model | Used by |
|---|---|---|
| `photos.embedding` + `embedding_model` | BioCLIP / BioCLIP-2 / timm classifier | `/api/species/<n>/clusters`, `/api/photos/<id>/similar`, `classify_job` |
| `photos.dino_subject_embedding` + `dino_global_embedding` + `dino_embedding_variant` | DINOv2 (variant-tagged) | highlights, culling redundancy, bursts, encounters |
| `photos.mask_path` | SAM2 | pipeline subject-mask step, crop extraction |
| `photos.eye_x`, `eye_y`, `eye_conf`, `eye_tenengrad` | eye detector | quality scoring |
| `photos.noise_estimate` | noise estimator (algorithmic, versioned implicitly) | quality scoring |

Symptom today: re-running with a different model overwrites the previous
value. In a multi-workspace setup where workspace A uses BioCLIP-2 and
workspace B uses BioCLIP-3, switching to B and running the classifier
destroys A's embeddings. Worse, the skip set from PR #636
(`classifier_runs`) means A's classifier never re-runs for those photos
after the switch — A appears to still be "classified" but its embedding
column is a lie, since it holds B's vector.

### Violations by proxy — derived from a Category-1 input

These fields are computed from SAM2 mask + primary detection. If either
upstream changes, these go silently stale.

| Column | Depends on |
|---|---|
| `photos.subject_tenengrad`, `bg_tenengrad` | SAM2 mask |
| `photos.crop_complete`, `bg_separation` | SAM2 mask |
| `photos.subject_clip_high`, `subject_clip_low`, `subject_y_median` | SAM2 mask |
| `photos.subject_sharpness`, `subject_size` | primary detection |
| `photos.quality_score` | composite of all above |
| `photos.detection_box`, `detection_conf` | primary detection (denormalized cache of `detections`) |
| `photos.sharpness` | global pixel sharpness — algorithm-versioned but stable enough to stay Cat 3 |

All of these are cheap to recompute once the Cat 1 sources live in proper
per-model tables, so the plan is to drop them and recompute on read (see
Phase 6).

### Workspace judgment leaked into global columns

| Column | Current behavior | Verdict |
|---|---|---|
| `photos.flag` | Global single-slot | **Should become Cat 2 (workspace-scoped).** Pick/Reject is per-cull-context — workspaces in Vireo *are* different cull contexts. Today a photo flagged in workspace A appears flagged in workspace B, which erases the distinction the user made when they created B. Moving to `photo_review` and keeping default XMP writeback off avoids any interop regression. |
| `photos.miss_no_subject`, `miss_clipped`, `miss_oof`, `miss_computed_at` | Global | **Split**: the detector's raw opinion is Cat 1 (per-model cache of the miss detector); "user has dismissed this miss" is Cat 2 (workspace-scoped side table). Revisit in Phase 8. |

## Schema — end-state after all phases

```sql
-- Per-model deterministic artifacts (Category 1).
-- Every table has the same header shape: (photo_id, model, variant, …, created_at).

CREATE TABLE photo_embeddings (
    photo_id    INTEGER NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
    model       TEXT NOT NULL,              -- e.g. "bioclip-2", "timm-efficientnetv2"
    variant     TEXT NOT NULL DEFAULT '',   -- optional sub-version; '' if none
    embedding   BLOB NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (photo_id, model, variant)
);

CREATE TABLE photo_dino_features (
    photo_id           INTEGER NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
    model              TEXT NOT NULL,              -- "dinov2"
    variant            TEXT NOT NULL DEFAULT '',   -- "vits14" / "vitb14" / ...
    subject_embedding  BLOB,
    global_embedding   BLOB,
    created_at         TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (photo_id, model, variant)
);

CREATE TABLE photo_masks (
    photo_id    INTEGER NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
    model       TEXT NOT NULL,              -- "sam2-base" / "sam2-large"
    variant     TEXT NOT NULL DEFAULT '',
    mask_path   TEXT NOT NULL,              -- ~/.vireo/masks/<model>/<photo_id>.png
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (photo_id, model, variant)
);

CREATE TABLE photo_eye_points (
    photo_id    INTEGER NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
    model       TEXT NOT NULL,
    variant     TEXT NOT NULL DEFAULT '',
    x           REAL,
    y           REAL,
    conf        REAL,
    tenengrad   REAL,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (photo_id, model, variant)
);

CREATE TABLE photo_noise_estimates (
    photo_id       INTEGER NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
    algorithm      TEXT NOT NULL DEFAULT 'v1',
    variant        TEXT NOT NULL DEFAULT '',
    noise_estimate REAL NOT NULL,
    created_at     TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (photo_id, algorithm, variant)
);

-- Workspace judgment migration.
CREATE TABLE photo_review (
    photo_id      INTEGER NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
    workspace_id  INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    flag          TEXT NOT NULL DEFAULT 'none',   -- 'none'|'flagged'|'rejected'
    flagged_at    TEXT,
    PRIMARY KEY (photo_id, workspace_id)
);
```

**Columns dropped from `photos` across all phases:**

- Phase 1: `embedding`, `embedding_model`
- Phase 2: `dino_subject_embedding`, `dino_global_embedding`,
  `dino_embedding_variant`
- Phase 3: `mask_path`
- Phase 4: `eye_x`, `eye_y`, `eye_conf`, `eye_tenengrad`
- Phase 5: `noise_estimate` (stays as mirror cache only if profiling demands)
- Phase 6: `subject_tenengrad`, `bg_tenengrad`, `crop_complete`,
  `bg_separation`, `subject_clip_high`, `subject_clip_low`,
  `subject_y_median`, `subject_sharpness`, `subject_size`, `quality_score`,
  `detection_box`, `detection_conf`
- Phase 7: `flag`

**Helper pattern — every Cat 1 table gets the same three helpers:**

```python
db.upsert_photo_<kind>(photo_id, model, variant='', **payload)
db.get_photo_<kind>(photo_id, model, variant='')
db.get_photos_with_<kind>(model, variant='', photo_ids=None)
  # the *_with_* variant is workspace-filtered via JOIN on workspace_folders
```

## Roadmap

Each phase is one PR with its own migration, test coverage, and rollback
window. Phases are ordered by blast radius ascending — smallest first so
the pattern is proven before it's scaled. No phase blocks another; they
can ship in any order the user finds most valuable.

### Phase 1 — Classifier embedding

- Create `photo_embeddings`
- Migrate `photos.embedding` + `embedding_model` → rows, drop the two
  columns
- Update `classify_job.store_photo_embedding`, `get_embeddings_by_model`,
  `/api/species/<n>/clusters`, `/api/photos/<id>/similar`
- Smallest blast radius; serves as the template for phases 2–5

### Phase 2 — DINO features

- Create `photo_dino_features`
- Migrate three `photos.dino_*` columns; drop them
- Update `pipeline_job`, `highlights.py`, `culling.py`, `bursts.py`,
  `encounters.py`, and the `/api/photos/<id>` detail response

### Phase 3 — SAM2 mask

- Create `photo_masks`
- Path-prefix mask files by model (`~/.vireo/masks/<model>/<photo_id>.png`)
  to avoid cross-model collisions on disk
- Migrate `photos.mask_path`; drop the column
- Update `pipeline.py` mask-extract step and every crop/feature consumer
  downstream

### Phase 4 — Eye detection

- Create `photo_eye_points`
- Migrate four `photos.eye_*` columns; drop them
- Update quality-scoring queries and the edit endpoints that write eye
  overrides

### Phase 5 — Noise estimate

- Create `photo_noise_estimates`
- Algorithm name is the "model." If the current algorithm is stable, tag
  existing values `v1` and treat future bumps as new rows
- Mostly schema hygiene; may be worth skipping if profiling shows no
  future version is coming

### Phase 6 — Proxy cleanup

Delete the proxy columns (`subject_tenengrad`, `bg_tenengrad`,
`crop_complete`, `bg_separation`, `subject_clip_high/low`,
`subject_y_median`, `subject_sharpness`, `subject_size`, `quality_score`,
`detection_box`, `detection_conf`) from `photos` and replace every reader
with a helper `compute_subject_features(photo, workspace_cfg)` that:

1. Pulls the mask from `photo_masks` for the workspace-configured mask model
2. Pulls the primary detection from `detections` for the workspace-configured detector
3. Recomputes the feature values from those inputs

If profiling shows quality-score reads dominate a hot path (e.g. the
browse grid), add a narrowly targeted cache table keyed on `(photo_id,
mask_model, detector_model)` — but don't pre-build it on spec.

This is the highest-risk phase (many readers, many templates). Deserves a
short design addendum of its own before coding — in particular, how the
browse-grid quality-score column behaves when upstream model sets differ
between two open grids in two workspaces.

### Phase 7 — `photos.flag` → `photo_review`

- Create `photo_review`
- Migrate existing `photos.flag` → one row per (photo, workspace)
  combination, seeding every workspace with the current global value so
  no user perceives a change until they start flagging
  workspace-specifically
- Drop `photos.flag`
- Update all readers (browse filter, encounters, sharpness-job auto-flag,
  collection rules, stats queries)
- XMP writeback stays off by default (see Section "Fourth axis")

### Phase 8 (separate doc) — XMP writeback policy + Settings UI

- Settings panel with per-field toggles per workspace
- Wire `workspaces.config_overrides.xmp_writeback` through the writeback
  callsites in `classify_job`, `pipeline_job`, `app.py` edit endpoints
- Conflict warning when multiple workspaces opt in for the same field
- This is a UX design, not a storage redesign — belongs in its own doc

### Phase 9 (separate doc) — `miss_*` refactor

- Split the detector's raw opinion into a Cat 1 per-model cache
- Add a workspace-scoped "dismissed" side table for user review
- Touches the miss-detection pipeline step + the browse-grid miss filter
  + every API that exposes miss state

## Out of scope

- **Versioning every Cat 3 algorithm.** pHash, file_hash,
  noise_estimate's algorithm — these are effectively stable. If we ever
  version-bump, we pay a one-time migration and move on. Pre-versioning
  them today buys nothing.
- **Cross-DB synchronization.** Nothing here addresses "share the cache
  across two different Vireo installs on two different machines." The
  per-model cache is local-only; remote sync would need separate design
  (likely a blob store keyed by `(file_hash, model, variant)`).
- **Granular embedding-model compatibility shims.** Once
  `photo_embeddings` is keyed on model, asking for a missing model
  returns nothing; the caller is responsible for deciding whether to fall
  back to a different model's embedding or re-run. No automatic dim-check
  or projection.

## Open questions

- Should Phase 5 (noise_estimate) go ahead if the algorithm is truly
  stable, or fold into Phase 6?
- Phase 6 cleanup: is there a hot path where quality-score recomputation
  is unacceptable, forcing the targeted cache table? Profile first.
- Phase 7 XMP writeback: opt-in-per-workspace seems right, but the
  conflict-warning UX is the open detail. Defer to Phase 8 design.
- For `photo_dino_features`: should `subject_embedding` and
  `global_embedding` be NOT NULL, or is there a legitimate case where one
  is produced without the other? Current code always produces both
  together; NOT NULL keeps that invariant.

## References

- PR #636 (detection storage redesign): establishes the Cat 1 pattern for
  detections and predictions, the workspace-scoped `prediction_review`
  table, and the `<kind>_runs` skip-set convention.
- `docs/plans/2026-04-23-detection-storage-redesign-design.md`: the
  design doc this one generalizes from.
- `docs/plans/2026-04-23-detection-storage-redesign-plan.md`: the 27-task
  plan executed to produce PR #636; phases 1–5 in this doc mechanically
  mirror that structure.
- `CLAUDE.md` (project root): notes that `photos` and `keywords` are
  global — that remains accurate for rating and keywords; `flag` moves
  in Phase 7.
