# Detection storage redesign

## Goal

Cache raw detector and classifier output globally — keyed by the deterministic
inputs of each model — so that running the pipeline in a new workspace over
photos that were already processed in another workspace does zero redundant ML
work, and so that changing a confidence threshold becomes an instant read-time
filter instead of a full reprocessing job.

Current state: `detections` rows are workspace-scoped and written post-threshold
(`vireo/db.py:163`, `vireo/classify_job.py:251-259`, `vireo/detector.py:243`).
Each workspace re-runs MegaDetector on the same photos even though the model
output is a pure function of `(photo, detector_model)`. The same problem
applies downstream: `predictions` inherit workspace scope via
`detection_id` → `detections.workspace_id`, so a BioClip classification that
would be identical in any workspace is recomputed per workspace.

This design splits deterministic model output from workspace-scoped human
judgment. The model output becomes a global cache keyed by its inputs; review
state (approved / rejected / individual / group) moves to a workspace-scoped
side table. Thresholds become read-time filters.

## Scope

In scope:

- `detections` table goes global, keyed by `(photo_id, detector_model)`.
- `predictions` table goes global, keyed by `(detection_id, classifier_model,
  labels_fingerprint, species)`.
- New `prediction_review` table holds per-workspace review state.
- New `detector_runs` / `classifier_runs` tables track "has the model been run
  on this input", independent of whether any rows were produced.
- New `labels_fingerprints` sidecar maps content hash → display metadata.
- Read-time threshold filtering throughout the app.
- One forward migration with dedupe across pre-existing workspace-scoped rows.

Out of scope:

- Moving global per-photo quality scoring (`photos.subject_sharpness`,
  `photos.subject_size`, `photos.quality_score`) into workspace-aware storage.
  These remain on the global `photos` row and are computed against the primary
  detection as selected by the active workspace's threshold at write time.
  Accepted inconsistency — revisit if it becomes a real problem.
- Detector weights versioning. Today `detector_model` is a semantic string
  (e.g. `"megadetector-v6"`); if weights ever change under a fixed name we can
  append a revision SHA later.

## Schema

```sql
-- GLOBAL: deterministic model output, no workspace scoping
CREATE TABLE detections (
  id                   INTEGER PRIMARY KEY,
  photo_id             INTEGER NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
  detector_model       TEXT NOT NULL,        -- e.g. "megadetector-v6"
  box_x                REAL,
  box_y                REAL,
  box_w                REAL,
  box_h                REAL,
  detector_confidence  REAL,                 -- raw, unfiltered
  category             TEXT,                 -- animal / person / vehicle
  created_at           TEXT DEFAULT (datetime('now'))
);
-- Many boxes per (photo_id, detector_model). Set-level uniqueness is enforced
-- by the write path: re-detecting clears all rows for the pair and reinserts.
CREATE INDEX idx_detections_photo_model
  ON detections(photo_id, detector_model);
CREATE INDEX idx_detections_conf
  ON detections(photo_id, detector_confidence);

CREATE TABLE predictions (
  id                   INTEGER PRIMARY KEY,
  detection_id         INTEGER NOT NULL REFERENCES detections(id) ON DELETE CASCADE,
  classifier_model     TEXT NOT NULL,
  labels_fingerprint   TEXT NOT NULL,        -- sha256 hex prefix, or "tol", or "legacy"
  species              TEXT,
  confidence           REAL,                 -- raw, unfiltered
  category             TEXT,
  scientific_name      TEXT,
  taxonomy_kingdom     TEXT,
  taxonomy_phylum      TEXT,
  taxonomy_class       TEXT,
  taxonomy_order       TEXT,
  taxonomy_family      TEXT,
  taxonomy_genus       TEXT,
  created_at           TEXT DEFAULT (datetime('now')),
  UNIQUE(detection_id, classifier_model, labels_fingerprint, species)
);

-- "Has the model been run on this input?" — independent of row count.
-- Prevents re-running MegaDetector on photos that genuinely have no animals.
CREATE TABLE detector_runs (
  photo_id        INTEGER NOT NULL REFERENCES photos(id) ON DELETE CASCADE,
  detector_model  TEXT NOT NULL,
  run_at          TEXT DEFAULT (datetime('now')),
  box_count       INTEGER NOT NULL,          -- 0 means "ran, found nothing"
  PRIMARY KEY (photo_id, detector_model)
);

CREATE TABLE classifier_runs (
  detection_id         INTEGER NOT NULL REFERENCES detections(id) ON DELETE CASCADE,
  classifier_model     TEXT NOT NULL,
  labels_fingerprint   TEXT NOT NULL,
  run_at               TEXT DEFAULT (datetime('now')),
  prediction_count     INTEGER NOT NULL,
  PRIMARY KEY (detection_id, classifier_model, labels_fingerprint)
);

-- WORKSPACE-SCOPED: human judgment only
CREATE TABLE prediction_review (
  prediction_id  INTEGER NOT NULL REFERENCES predictions(id) ON DELETE CASCADE,
  workspace_id   INTEGER NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
  status         TEXT NOT NULL DEFAULT 'pending',   -- pending / approved / rejected
  reviewed_at    TEXT,
  individual     TEXT,
  group_id       TEXT,
  vote_count     INTEGER,
  total_votes    INTEGER,
  PRIMARY KEY (prediction_id, workspace_id)
);
-- Absence of a row == pending. Rows are written lazily when a user acts.

-- Optional sidecar for debugging / UX: which labels set is this hash?
CREATE TABLE labels_fingerprints (
  fingerprint    TEXT PRIMARY KEY,
  display_name   TEXT,                       -- e.g. "California birds (merged)"
  sources_json   TEXT,                       -- JSON array of source file paths
  label_count    INTEGER,
  created_at     TEXT DEFAULT (datetime('now'))
);
```

### Fingerprinting the labels set

`labels_fingerprint` is `sha256(sorted(deduped(label_strings)))` truncated to 12
hex chars. Sentinel values:

- `"tol"` — BioClip Tree of Life mode (no labels file; uses built-in taxonomy).
- `"legacy"` — pre-migration rows. Will not match any new fingerprint; those
  detections will be re-classified on demand.

When a classifier run begins, the fingerprint is computed and upserted into
`labels_fingerprints` (with the source file paths as `sources_json`) before any
prediction rows are written. This keeps the fingerprint → human-name mapping
available for debugging without coupling it to the predictions hot path.

## Write path

```
detect_animals(image_path, detector_model)
  -> MegaDetector ONNX, NMS still applied, hard floor 0.01 in postprocess
  -> returns ALL boxes >= 0.01 with no workspace-aware filtering

classify_job._detect_batch
  -> if (photo_id, detector_model) in detector_runs: skip (empty or not)
  -> else:
       detections_for_pair = detect_animals(...)
       DELETE FROM detections WHERE photo_id=? AND detector_model=?
       INSERT rows for every box returned (may be zero)
       UPSERT INTO detector_runs (photo_id, detector_model, box_count)

classifier run (bioclip / speciesnet / ...)
  -> fingerprint = sha256_prefix(sorted(deduped(labels))) or "tol"
  -> UPSERT INTO labels_fingerprints (cosmetic)
  -> if (detection_id, classifier_model, fingerprint) in classifier_runs: skip
  -> else:
       predictions_for_triple = classify(...)
       INSERT top-N predictions, no confidence filter
       UPSERT INTO classifier_runs (..., prediction_count)

prediction_review
  -> only written when a user acts (approve / reject / set individual / group)
  -> absence of a row is treated as status='pending' in reads
```

Key decisions:

- **0.01 floor on the detector.** Running MegaDetector with no floor emits tens
  of thousands of noise boxes per image. A low floor keeps storage sane while
  being effectively "raw" — no realistic workspace threshold goes below this.
- **No per-workspace gate anywhere on the write path.** Detection and
  classification only care about their own deterministic inputs. Whether the
  active workspace would have filtered these results at its threshold is
  irrelevant at write time.
- **Lazy `prediction_review`.** Materializing a "pending" row per (prediction,
  workspace) pair scales as O(N_workspaces × N_predictions) and carries no
  information. Absence is the pending state.
- **`detector_runs` / `classifier_runs` tables are the single source of truth
  for "was this model run on this input?"** The existing skip pattern of
  "does `detections` have at least one row?" is wrong — a photo with no
  animals produces zero rows and would be re-run forever.

## Read path

All detection and prediction reads apply threshold as a SQL `WHERE` clause,
resolved from workspace-effective config by default:

```python
db.get_detections(photo_id, min_conf=None)
    # min_conf=None => pulls detector_confidence from workspace-effective config
    # min_conf=0    => raw (debugging / "show all boxes" endpoints)

db.get_detections_for_photos(photo_ids, min_conf=None)

db.get_predictions_for_detection(
    detection_id,
    min_classifier_conf=None,
    classifier_model=None,          # filter to a specific model
    labels_fingerprint=None,        # filter to a specific list
)

db.get_review_status(prediction_id, workspace_id)
    # returns 'pending' if no prediction_review row exists
```

Sites that need updating (from a repo-wide grep):

- `classify_job._detect_batch` / `_detect_subjects` — bind cached detections
  with threshold applied; use `detector_runs` for the skip check.
- `pipeline_job._detect_batch` — same.
- `pipeline.py` subject-crop extraction queries.
- `app.py` browse-grid `/detections` endpoint, map view queries, highlights.
- `db.get_photos_with_detections_but_no_masks` and siblings.
- `db.get_prediction_stats`, `get_photos_by_prediction`, `get_species_counts`.

`get_primary_detection` (used for global quality scoring) runs over the
threshold-filtered set of the *writing* workspace. Accepted as an inconsistency
for this pass — see scope notes above.

**Behavioral consequence**: changing `detector_confidence` in workspace config
becomes instant. Next page load filters differently. Lowering it below an
earlier effective threshold works, because raw rows are already stored.

## Migration

One irreversible forward migration, gated on the schema version bump.

1. **Create new tables**: `detector_runs`, `classifier_runs`,
   `labels_fingerprints`, `prediction_review`.

2. **Backfill `detector_runs`**: one row per distinct `(photo_id,
   detector_model)` seen in existing `detections`. `box_count` = row count;
   `run_at` = earliest `created_at` in the group.

3. **Dedupe `detections` across workspaces**:
   - Group by `(photo_id, detector_model, box_x, box_y, box_w, box_h)` — the
     model is deterministic so cross-workspace duplicates have identical
     coords.
   - Pick lowest `id` in each group as canonical.
   - `UPDATE predictions SET detection_id = canonical_id` for all dupes.
   - `DELETE` non-canonical rows.
   - Drop `workspace_id` column.

4. **Backfill `prediction_review`**: one row per `(prediction_id,
   workspace_id)`, with `workspace_id` derived from the pre-migration
   `detections.workspace_id` of each prediction's owner. Copy `status`,
   `reviewed_at`, `individual`, `group_id`, `vote_count`, `total_votes` into
   the new table.

5. **Add `labels_fingerprint`** to `predictions` with sentinel `"legacy"` on
   all existing rows. Existing predictions will not match any new fingerprint
   going forward — accepted; reclassify refreshes.

6. **Dedupe `predictions`** by the new uniqueness key; re-point
   `prediction_review` to canonical `prediction_id`; delete duplicates.

7. **Drop workspace/review columns from `predictions`**: `status`,
   `reviewed_at`, `individual`, `group_id`, `vote_count`, `total_votes`,
   `workspace_id` (if present). Apply the new `UNIQUE` constraint.

Because pre-migration detections were already threshold-filtered, the migrated
rows are *not* truly raw. A user who later lowers their threshold below
whatever the historical filtering level was will not magically recover
subthreshold boxes — they need to run reclassify. Document this in the
changelog.

## Testing

**Unit tests**

- `vireo/tests/test_db.py`
  - `get_detections(photo_id, min_conf=...)` respects threshold.
  - Cross-workspace reads return identical rows.
  - `prediction_review` upsert / absence-means-pending semantics.
- `vireo/tests/test_classify_job.py`
  - Detect-skip uses `detector_runs`, not row count.
  - Photos with `box_count=0` are cached and not re-run.
- `vireo/tests/test_detector.py`
  - 0.01 floor applied; `detect_animals` no longer takes a workspace threshold.
- `vireo/tests/test_migration_detection_storage.py` (new)
  - Captured legacy SQLite fixture covering multi-workspace, overlapping
    detections, orphaned predictions, mixed review states.
  - Run migration, assert new schema + row counts + re-pointed references.

**Integration / user-first**

- Scan + classify in workspace A, switch to workspace B over the same folder —
  no MegaDetector or classifier work fires; detections visible; review state
  independent.
- Lower `detector_confidence` in active workspace — browse grid immediately
  shows more boxes, no job fires, no progress SSE events.
- Reclassify in workspace B after legacy migration — `labels_fingerprint` gets
  a real hash, previously-subthreshold boxes become queryable.

## Rollout

- Irreversible forward migration, gated on schema version bump. Document
  clearly that `~/.vireo/vireo.db` is modified in place and a backup is
  recommended.
- Changelog entry:
  > Detector and classifier results are now cached per-photo rather than
  > per-workspace. Existing detections keep working; for best results, run
  > "Reclassify" once to regenerate with raw storage.
- Headless API (b2ccd9f) audit: endpoints that returned `predictions.status`,
  `individual`, `group_id` now join to `prediction_review`. Walk the routes
  touching `predictions` and update shapes / read paths as needed.
- Settings page stats: add "Detections: N photos × M models cached" so users
  can see the new global cache's scale.

## Open questions / future work

- **Detector weights versioning**: if MegaDetector V6 weights are ever
  re-released at the same HF path, same-name rows will be stale. Consider
  appending a revision SHA to `detector_model` in a later pass.
- **Quality scoring consistency**: `photos.subject_sharpness` /
  `subject_size` / `quality_score` are computed against the primary detection
  as seen by the writing workspace's threshold. Two workspaces with different
  thresholds could disagree about which box is primary. Not addressed in this
  refactor. Revisit if it causes user-visible bugs.
- **Labels fingerprint display**: the sidecar `labels_fingerprints` table is
  defined but UI integration (showing friendly names in settings / reports)
  is deferred.
