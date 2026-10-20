# Missing subject analysis in the Mandarin Duck portraits

Investigation for [issue 2243](https://github.com/jss367/vireo/issues/2243), observed October 20, 2026. The catalog, original photos, sidecars, configuration and caches were accessed read-only. No photo was reclassified, rescored, or assigned a decision in the real library.

## Findings

The missing subjects are consistent with **animal-category selection excluding the detector's person boxes**, rather than detection never having run. All 71 exact encounter members have a completed detector-run record, consistent raw-box counts and matching source-input fingerprints. The 61 cached-empty photos have **no animal detection reaching the current 0.2 confidence floor**; each has a person detection above that floor. Those person boxes cannot become retained animal subjects. The saved review cache is not hiding existing animal analyses: these same 61 currently have no subject-analysis rows, masks, subject quality features or subject/global DINOv2 image embeddings.

| Observation | Cached-empty 61 | Other 10 |
| --- | ---: | ---: |
| Detector-run records | 61 | 10 |
| Raw detections | 433 | 91 |
| Raw animal detections | 233 | 56 |
| Animal boxes at least 0.2 | 0 | 9 |
| Person boxes at least 0.2 | 61 | 10 |
| Mask records | 0 | 10 |
| Box-level subject-analysis records | 0 | 9 |
| Subject and global embeddings | 0 | 10 |

Nine other photos have a retained animal and a mask. Photo 216716 (`DSC_5450.NEF`), the user's existing keep, has a contextual weak-animal detection at 0.120849609375, plus a person box at 0.69384765625 covering the duck. Visual inspection of its existing working JPEG confirms a large duck in that person box. The weak animal crop instead shows background near the rock; **having ten masks does not establish that ten ducks were correctly analyzed**. The detector's category output is a model limitation on the observed scene; changing person detections into animals automatically would require separate validation.

The stage has a demonstrated diagnostic defect: its initial mask-candidate query admits person/vehicle boxes, but under the photo lock `_sync_primary_subject` retains animals only. With a person-only candidate, it returns early and increments the same `skipped` counter used for a real Segment Anything Model (SAM) no-mask result. Consequently, the previous job's aggregate skipped count cannot establish that segmentation was attempted on these photos. This change records those paths separately without changing scoring, thresholds, category policy or decisions.

## Exact membership and reproducibility

[Saved membership](mandarin-duck-saved-membership.json) preserves the original encounter's explicit ordered `photo_ids` and the cached subject counts/labels/flags. Membership was extracted from the original saved `pipeline-before.json` encounter containing photo 216692, not a contiguous identifier range. [The accountability CSV](mandarin-duck-subject-analysis.csv) has one row per exact member, with current stage records, features, availability, flags and explicit unknown historical outcomes. The original cached flags are kept separate from current flags because the user subsequently reviewed the library.

Run from the repository root, supplying the real catalog path or an isolated copy:

```sh
python scripts/inspect_subject_analysis.py \
  --db "$CATALOG_PATH" \
  --membership docs/investigations/mandarin-duck-saved-membership.json \
  --output /tmp/mandarin-duck-subject-analysis.csv \
  --threshold 0.2
```

The script opens SQLite with `mode=ro`, enables `query_only`, and reads a single transaction snapshot. It deliberately does not instantiate Vireo's `Database`, whose initialization can migrate or repair a catalog. It writes only the specified CSV. Source availability uses file stats; no source bytes are rehashed and no coordinates are exported.

## Configuration, variants and provenance

All 71 detector runs and their raw boxes carry runtime fingerprint `48173548c839e0f46be911fd8a9bed1a590d5be6fdbd513f2988a2441b8fbb9e`. Computing `computation_cache.megadetector_runtime_fingerprint()` from the installed weights and current source yields the same fingerprint. That descriptor includes preprocessing version 6, postprocessing version 2, the three category names and raw confidence floor 0.01. The old YOLO output-layout bug documented in `detector.py` was previously fixed; no evidence here identifies these records as products of that old implementation.

All 71 stored input fingerprints equal `source_input(photos.file_hash, 'vireo-detector-source-v1')[1]`; their detector `box_count` equals the actual raw rows for the model. A portable detector artifact for photo 216692 also declares `completed: true` and contains the same person/animal box categories. Matching catalog hashes proves internal identity consistency, not an independent rehash of current originals. All 71 originals are currently available with file sizes and mtimes matching the catalog; historical decode availability remains a separate question.

Current effective workspace configuration (USA2026, id 5) uses detector confidence 0.2, SAM `sam2-large`, DINOv2 `vit-b14`, and proxy longest edge 2048. Weak rescue defaults are enabled with floor 0.12. All ten stored masks use `sam2-large`; their files are present, their active mask variants match, and their DINOv2 variants are `vit-b14`. No alternative mask variant explains the other 61. Their absent global embeddings follow the mask-stage worklist, which computes global embeddings alongside selected subjects; they do not prove that an independent global-feature stage failed.

## Historical job evidence and limits

- `pipeline-1792265298688-10`, started October 17 at 12:28 local time, was cancelled. It explicitly had `skip_extract_masks: true`, and detector processing covered 3303 photos. Its cancellation does not establish a later segmentation failure.
- `pipeline-1792268357331-15`, started October 17 at 13:19, processed folder 1153, which contains this sequence. It finished successfully with 3303 detection inputs, 3109 masked, 107 skipped, zero failed, zero unreadable, and 84 subthreshold photos. These are folder-wide counts, not a per-photo proof about the 71 frames. Raw subject analysis was disabled for this run.
- Retained rotating application logs start at October 17 13:36, after these detector rows were recorded (19:59 UTC / 12:59 local). No retained photo-specific extraction outcome or traceback was found for this sequence. A missing log is not proof that a stage succeeded.
- Historical jobs do not retain the full effective threshold/variant snapshot or per-photo skipped reason. Each CSV row therefore explicitly says `unknown_not_recorded_per_photo` for its historical outcome. Current category/threshold eligibility explains the missing subject path; the precise execution history cannot be reconstructed beyond those limits.

## Future diagnostics and recovery limits

Extraction now persists bounded `outcomes` in the existing job-history stage result: exact outcome counts, up to 20 photo/error samples per outcome, a sample truncation flag, captured effective settings, and a fatal stage error when present. It separates `no_retained_animal`, `sam_no_mask`, `no_qualifying_detection`, `no_detection_evidence`, `detector_completed_zero_boxes`, `detector_persistence_incomplete`, `unreadable`, `source_offline`, `failed`, `cached`, `cached_offline`, and `completed`. `no_detection_evidence` explicitly makes no inference about whether detection ran. When raw boxes are absent, the persisted `detector_runs.box_count` distinguishes recorded zero-box completion from missing execution evidence and incomplete positive persistence. A skipped or failed stage is not promoted to a completed negative image judgment. Bounded samples keep large library jobs from retaining millions of error details; unsampled photo outcomes remain unknown.

The false-reject safeguard is tracked in [PR 2266](https://github.com/jss367/vireo/pull/2266); saved-review recovery is tracked in issue 2242. The agent handling saved-review recovery independently copied the 71 catalog rows into a canonical scratch database and replayed the original saved suggestions. Its observed result was 5 KEEP / 61 REVIEW / 5 REJECT: all 61 former false rejects became REVIEW, photo 216716 remained KEEP, and one encounter/five bursts were preserved. The scratch database dump and reopened saved JSON/cache bytes remained unchanged. This verifies suggestion recovery with existing cached measurements; it does not verify new detector inference or corrected duck masks.

Selected-photo retry is issue 2241 and is outside this bug-fix set. No selected retry UI or safe isolated end-to-end inference retry was available during this investigation. Improved duck analysis and model calibration have **not** been observed. The real user's keep and decisions were untouched. The conditional selected-retry acceptance check remains deferred until that feature is available.
