# Current prediction read audit

This audit follows issue #644 through the repository/service/HTTP layers after the SQL moved out of `db.py` and `app.py`. PR #2260 already corrected the main Review/Compare listing (`PredictionRepository.get_rows`), including its full-image anchors. The remaining changes below address separate production reads.

A current prediction uses the newest `labels_fingerprint` for each `(detection_id, classifier_model)`, ordered by `created_at DESC, id DESC`. Selecting the newest set happens before ranking its candidates by confidence. Where a read represents currently visible detector evidence, the real detection must meet the active workspace's effective `detector_confidence` (default `0.2`). Full-image pseudo-detections have no meaningful detector confidence and remain eligible on the newly corrected prediction-ranking surfaces.

## Corrected reads

| Surface | Correction | Preserved behavior |
| --- | --- | --- |
| Culling species grouping: `CullingRepository.top_prediction`, called by `culling._load_top_predictions` | Apply the workspace-effective detector floor and newest label set before choosing the highest-confidence prediction. Resolve the configuration once per analysis, and resolve it for direct repository calls too. | Full-image predictions and per-model identity/embedding selection. Photo IDs are already scoped by the analysis caller. |
| Highlights candidates: `SpeciesCurationRepository._highlight_candidate_rows` | Filter real detections before ranking the current, unrejected prediction. | Accepted species keywords, photos with no eligible prediction, quality filtering, workspace/folder/single-photo scope, full-image predictions. |
| Saved Highlights eligibility: `_HIGHLIGHT_ELIGIBILITY_FILTER` through `get_highlights(eligible_only=True)` | Apply the same detector policy to the predicted-species fallback used by candidate buckets. | Stored choices are retained. The unfiltered saved-choice read and accepted-keyword branch remain independent of detector settings. Lowering the floor restores prediction-based eligibility. |
| Highlights Confirm/Relabel: `PredictionRepository.top_unrejected_by_photo` | Use the same detector policy as candidate selection before resolving the prediction to accept/reject. Its newest-label-set pin was already correct. | Existing workspace review states, caller visibility validation, chunking, explicit replacement species, full-image predictions. |
| Misses classifier rescue: `DetectionsRepository.workspace_miss_rows` | Pin `max_prediction_conf` to each detection/model's newest label set. | Intentionally includes below-floor detections and full-image predictions. Strong classifier evidence can rescue a hummingbird that the detector missed; a stale label list must not supply that rescue. |

## Reviewed reads that already satisfy the policy

- `PredictionRepository.get_rows`, `get_live_rows_by_photo`, `get_top_confidences`, and `inspector_rows_for_photo`; the filtered `get_top_for_photo` path used by all three iNaturalist callers. The unfiltered `get_top_for_photo` overload remains an explicit legacy/raw read supported by existing tests.
- The shared `_TOP_PREDICTION_CONFIDENCE_EXPR` in `db.py`, `top_species.TOP_SPECIES_RANKING_SQL`, and `CollectionRepository` prediction predicates/model suggestions. These bind the effective workspace floor and pin the current label set. Browse summary and its species drill-down share the ranking expression.
- `StatsRepository._dashboard_classification_counts` and eye-keypoint attemptable ranking; `MasksFeaturesRepository.list_photos_for_eye_keypoint_stage` uses the same current label set, effective detector floor, primary subject and matching-mask constraints.
- `PipelineFeaturesRepository` prediction reads: effective floor for ordinary reads; explicit run-label-set and continuity scopes for staged pipeline reads. Full-image and selected-run variants are deliberate, separate contexts.
- `SubjectsRepository.visible_predictions` pins the newest label set. `subjects.payload` attaches those predictions only to `retained` detection IDs, which already apply the effective workspace floor.
- Keyword replacement's `_live_neighbour_species` already uses current label sets and the effective floor while excluding rejected/alternative evidence. The associated accept-subject agreement read pins current label sets within the explicitly chosen detection.

## Intentional exceptions

| Read category | Why additional filtering would be incorrect or redundant |
| --- | --- |
| Coverage and Browse classified-photo counts; eye-keypoint eligible/stale/attempt counts; move `has_predictions` | These threshold-filtered queries test prediction existence or count **distinct photos**, with no prediction confidence, species, label-set or review-status restriction. If any stored candidate exists, a newest label set necessarily has a candidate on that same detection. Adding the correlated pin cannot change the result. The separate ranked eye-keypoint attemptable query already pins it. |
| Classification inventory and sampled top-one medians in `StatsRepository` | These deliberately report each `(classifier model, label set)` run separately, including prior label sets. They already restrict eligible real detections by the requested workspace's effective floor. A newest-set filter would erase the inventory being compared. |
| `PredictionRepository.get_states`, `count_in_workspace` | Stored-output/run diagnostics and issue-report catalog size, respectively. These describe raw cache state, not current-ranked prediction counts. |
| `ModelRunsRepository` cache gates, reviewed-runtime pins, per-photo runs/match-score rows and current detector-confidence diagnostics | Exact run identity or diagnostic history. Per-run rows expose current/stale identity and below-threshold/full-image evidence so the UI can explain why outputs are hidden. Manual review pins must survive setting changes. |
| `DetectionsRepository.get_predictions`, `PredictionRepository.get_for_photo` / `get_existing_photo_ids`, computation-cache reads | Exact detection/model/label-set cache lookup and reuse. Active pipeline callers pass the requested label set; changing to the newest unrelated set would defeat cache identity. The legacy unrestricted overloads are retained. |
| Review group lookup/alternatives, decision rows by ID, burst membership, sibling reads, review-status snapshots, group metadata writes | Explicit group, decision or exact label-set scope. They support reviewing previous decisions, stale-request detection, and changing a previous group decision. Filtering away accepted/rejected or superseded evidence would conceal the user's history or undermine guards. |
| Edit history, keyword-accept group expansion, workspace-folder transfer, companion pairing, species-identity repairs and prediction deletes | Restoration, explicit decisions, preservation, migration or mutation of stored evidence. These must retain the named historical rows rather than silently substituting current inference. |

The search covered every `FROM predictions`/`JOIN predictions` occurrence under `vireo/repositories`, the service/HTTP callers, and the remaining shared SQL expression in `db.py`; there are no prediction SQL statements left in the HTTP/service layers. Existing detector-only Browse/dashboard counters continue to exclude full-image anchors where they specifically describe real detector coverage.

## Regression evidence

`vireo/tests/test_current_prediction_reads.py` exercises the actual corrected reads with higher-confidence stale candidates, lower-confidence current candidates, below-floor detections, equal timestamps and ID tie-breaking, timestamp precedence over insertion order, separate classifier identities, global versus workspace configuration, floor changes/defaults, full-image anchors, workspace visibility/review scoping, retained saved highlights and accepted keywords, and the deliberately weak-detection Misses rescue. It also verifies the Highlights confirmation endpoint accepts the current visible prediction without changing the stale or hidden rows.

All tests use temporary catalogs/configuration. This audit did not write to a real catalog, photo, or sidecar.
