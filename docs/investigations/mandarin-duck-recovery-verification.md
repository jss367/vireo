# Mandarin Duck subject-analysis recovery verification

Observed October 21, 2026. Follow-up for [issue 2243](https://github.com/jss367/vireo/issues/2243). The original [71-photo investigation](mandarin-duck-subject-analysis.md) established completed detector records with no qualifying animal boxes for 61 frames. This verification exercises selected-photo retry with actual local inference in an isolated copy of that exact encounter.

## Isolation and baseline

The tracked [exact saved membership](mandarin-duck-saved-membership.json) supplies all 71 identifiers, in the original order. No contiguous-range inference is used. The source catalog is opened with SQLite `mode=ro` and `query_only`; preparation never initializes Vireo's `Database` on the real catalog.

The scratch catalog uses the current canonical schema and contains exactly 71 photo rows. All originals and present sidecars are copied with metadata preserved; macOS uses APFS clones when available. Every original and copied photo has the same SHA-256 and nanosecond modification time, and all 71 content hashes match the real catalog. Original photo bytes total 1,662,837,760. The minimal scratch preparation took 2.559 seconds with warm filesystem caches. Foreign-key checks passed.

Installed MegaDetector, Segment Anything Model 2 (SAM2) and DINOv2 weights are copied into scratch. Inference uses these copies, a scratch configuration without credentials, and scratch masks/cache files. A process audit hook denies writes to the real library and copied originals, writable live-catalog connections, and network connections. One process runs inference with a four-thread CPU resource budget and the CPU execution provider; no model download or full-library classification is part of this verification.

The baseline real decisions are 70 unflagged frames and one existing keep, photo 216716 (`DSC_5450.NEF`, flag `flagged`, rating 0). The current saved-review refresh yields **1 KEEP / 70 REVIEW / 0 REJECT**, preserving one encounter and five bursts. This differs from the earlier investigation's suggestion counts because current automatic-keep policy requires separate validated focus/animal evidence. It is a read-only baseline refresh, not new inference.

Installed detector metadata declares animal/person/vehicle classes in exactly the order used by Vireo, with output tensor `[1, 7, 8400]`. No class-name ordering mismatch was found. A completed detector result without a qualifying animal is expected to remain REVIEW.

## Reproduction

Prepare a new scratch directory, using the original catalog/config/model locations explicitly:

```sh
python scripts/verify_subject_analysis_recovery.py prepare \
  --catalog "$VIREO_CATALOG" --config "$VIREO_CONFIG" \
  --models "$VIREO_MODELS" --output .context/duck-recovery-scratch
python scripts/verify_subject_analysis_recovery.py seed \
  --scratch .context/duck-recovery-scratch \
  --saved-review "$ORIGINAL_SAVED_REVIEW"
```

The saved-review input must contain the exact original encounter; the archived 71-frame investigation JSON or the original complete `pipeline-before.json` both work. It is read-only and its content digest is retained in the private manifest.

The runner can use a separate checkout containing the selected-photo retry implementation, recording that checkout's exact commit. This avoids copying an independent implementation into the verification change:

```sh
python scripts/run_subject_retry_verification.py \
  --scratch .context/duck-recovery-scratch \
  --implementation "$RETRY_CHECKOUT" \
  --photo-ids 216692 216716 216713 \
  --invalidate-features 216713 --concurrent-decision 216713
python scripts/verify_subject_analysis_recovery.py check \
  --scratch .context/duck-recovery-scratch
```

`--invalidate-features` deliberately removes only a selected scratch frame's cached primary features to exercise genuine segmentation. `--concurrent-decision` writes a photographer reject/rating 2 through a separate scratch database connection during the retry; the runner checks that the retry preserves it, all other flags/ratings, and every unselected photo row. The existing user keep must remain KEEP. Group membership and bursts must remain unchanged in both the returned and persisted saved review.

For an independent positive fixture, prepare and seed a new `.context/manual-subject` directory first. It uses a manually selected, visually checked duck box with the observed duck-sized person detection's coordinates:

```sh
python scripts/run_subject_retry_verification.py \
  --scratch .context/manual-subject \
  --implementation "$RETRY_CHECKOUT" --photo-ids 216692 \
  --manual-duck-box 216692 --report-name manual-subject
```

The recorded positive runs below used the already seeded pilot directory, so its simulated concurrent reject remained present. An independently prepared directory retains its original decision baseline instead. Reusing an already complete manual fixture can correctly become a no-op; prepare a new directory to observe fresh SAM2 inference.

That fixture uses detector model name `manual-verification-box`. It is an explicit verification-agent intervention in scratch, not an automated detector correction, a production annotation UI, or model calibration. The runner retains raw local stage outcomes/timings; source/sidecar contents and mtimes are checked again after inference.

## Three-frame pilot and positive fixture

The actual retry service from commit `d67df17f39142c768a11b49d737b7fadd1581c73` completed the three-frame pilot in **13.6235 seconds**:

| Frame | Actual model work | Observed result |
| --- | --- | --- |
| 216692, `DSC_5426.NEF` | Detector, 5.1001 s | Six raw boxes; no animal at least 0.2; one person at least 0.2. No SAM invocation. Remains REVIEW with unavailable subject quality. |
| 216716, existing keep | None | Contextual weak-subject analysis was already current; remains KEEP. |
| 216713, deliberately cleared scratch measurements | SAM2, 7.9334 s, followed by quality computation | New immutable mask and complete quality measurements. The pre-existing animal box selects background, so this is infrastructure recovery, not proof of a correctly selected duck. |

A separate database connection wrote `flag=rejected`, `rating=2` for selected scratch frame 216713 during the detector retry. That decision remained intact after mask publication and saved-review refresh. The final scratch summary was 1 KEEP / 69 REVIEW / 1 explicit REJECT. All unselected photo rows and other flags/ratings were unchanged, and one encounter/five bursts were preserved. Checks after the pilot confirmed unchanged live decisions and unchanged original/copy/sidecar contents and modification times.

The manually selected positive fixture for 216692 then performed **actual SAM2 inference without another detector call**, completing in **7.6993 seconds**. Its mask covers 9.53% of the image; visual inspection of the generated boundary shows the duck's head, beak, body and tail rather than background. Required measurements became available, readiness became eligible, and refreshed quality changed from unavailable to 0.6542. The suggestion remains REVIEW because automatic usability is still unvalidated. The existing keep and concurrent reject/rating remained intact.

This positive fixture establishes that a visually appropriate supplied subject can pass through segmentation, measurement publication and saved-suggestion refresh. The box was selected by the verification agent, not supplied or confirmed by the user. It does not establish that the detector can recover the duck automatically, that every mask is correct, or that absolute focus thresholds are calibrated.

## Full exact sequence

A **fresh** scratch copy of all 71 frames was retried using frozen implementation commit `a72c073d9deb3458ef42da61cc44973dad03d543` (retry-service SHA-256 `dc710963dd82096d353883d13fd32215a2d49a430af14cddf6c7ed85513a02cd`). The runner archives that committed code before importing it, so ongoing work in another checkout cannot change a running verification.

The full pass took **230.1612 seconds**, including **229.8961 seconds of actual detector calls**. There were 61 detector invocations, no SAM/classifier/embedding invocations, and no errors. Each of the 61 frames again returned no animal at the selected confidence floor and remained REVIEW. The other ten frames were already complete under current settings and required no model work. Final suggestions were **1 KEEP / 70 REVIEW / 0 REJECT**, with the existing keep, all flags/ratings and the exact encounter/burst membership preserved.

[The 71-row retry accountability CSV](mandarin-duck-retry-outcomes.csv) records each frame's initial required stages, stages actually executed, measured model time, fresh raw/qualifying animal counts where detection ran, final readiness and suggestion. Blank fresh-box fields mean no new detector invocation, not a zero-box verdict. The model times measure model-call wall time, including decoding/session construction when those happen inside the call; they are not per-photo CPU-time measurements.

For a full reproduction, prepare and seed a **new** directory as above, then expand the runner's explicit limit:

```sh
python scripts/run_subject_retry_verification.py \
  --scratch .context/full-sequence \
  --implementation "$RETRY_CHECKOUT" \
  --all-saved-members --max-photos 71 --report-name full-sequence
python scripts/verify_subject_analysis_recovery.py check \
  --scratch .context/full-sequence
```

All original/copy/sidecar content hashes and nanosecond modification times remained unchanged after both passes; live flags/ratings also matched their baseline. Additional selected-catalog-row digest checks were stable through the final checks. For these initial passes, that digest was added during the full run, so it does not substitute for a pre-inference whole-catalog comparison; read-only connections and the write guard protected the live catalog throughout. The checked digest covers the selected photo, detection/run, mask, subject-choice/state, keyword-association and current-flag-attribution rows. The reproducible preparer now captures that digest before execution. It deliberately does not compare physical SQLite/WAL bytes, which can change for unrelated legitimate app activity.

The model still does not provide a qualifying animal for these 61 frames. Recovery infrastructure now reports that result honestly and preserves REVIEW instead of treating unavailable subject measurements as evidence to reject a photograph. No real catalog repair, corrected detector behavior, automatic keep validation, focus calibration, or live-library mutation is claimed.

## Atomic publication regression check

An earlier publication check used the [selected-photo retry implementation, PR 2278](https://github.com/jss367/vireo/pull/2278), at commit `ba35023692e888bf72d16ffc365dc2f5a5e8e595`. A fresh scratch fixture captured the source/copy/sidecar hashes and selected live catalog-row digest **before** execution. The bounded three-frame pilot was repeated against a frozen archive of that exact commit, adding a deliberately incomplete, matching-key ranking cache generation for frame 216713:

```sh
python scripts/run_subject_retry_verification.py \
  --scratch .context/final-publication \
  --implementation "$RETRY_CHECKOUT" --photo-ids 216692 216716 216713 \
  --invalidate-features 216713 --incomplete-ranking 216713 \
  --concurrent-decision 216713 --report-name final-publication
```

This publication delta completed in **11.0839 seconds**. Detection took 3.8015 seconds and kept 216692 in REVIEW. Genuine SAM2 inference took 6.7922 seconds and repaired the incomplete generation for 216713, publishing complete mask/quality evidence. The concurrent scratch reject/rating 2, existing keep, every unselected photo row and grouping all survived. This exercises recovery of a provably incomplete generation rather than merely bypassing a valid cached result.

The supplied-box positive fixture was also repeated on that code, completing in **6.8684 seconds** (6.4803 seconds inside the SAM2 call). It produced complete analysis and quality 0.6542, while the suggested label stayed REVIEW. The mask pixels exactly match the previously visually inspected duck mask. The already valid ranking generation from 216713 remained byte-for-byte unchanged during this separate operation.

Final checks passed for every original/copy/sidecar hash and modification time, all live flags/ratings, and the pre-run selected live catalog-row digest. No live catalog, source, sidecar, configuration or cache writes occurred. The initial seven focused preparer/guard tests passed, as did Ruff and whitespace checks. The independent verification tools import the retry API only when the runner is explicitly invoked with the implementation checkout; their safety tests and report do not duplicate the production changes in PR 2278.


## Reviewed runtime and saved-review regression check

After detector-runtime identity binding, saved encounter identity matching, species-mode analysis refresh, bounded cleanup, and cancellation-aware publication changes, the bounded replay was repeated against frozen implementation **`4111fb4aae8ff286674a640ab476997c323532e0`** (retry-service SHA-256 `a4ed3a67a42525b1bfdc02a42661aef5bdedaa4355f21a80c299b2aa3bc8d65e`). This is the latest actual-model verification recorded here, distinct from the historical full-sequence commit above.

A newly prepared canonical 71-photo fixture captured all source/copy/sidecar hashes, modification times, live decisions and the selected catalog-row digest **before** any inference. Preparation took 3.109 seconds and foreign-key checks passed. The same three selected frames and intentionally incomplete matching-key ranking generation were exercised:

| Frame | Actual model work | Observed result |
| --- | --- | --- |
| 216692 | Detector, 3.8772 s | Still no qualifying animal; only detection ran; stays REVIEW with no fabricated quality. |
| 216716 | None | Existing current analysis and photographer KEEP survive. |
| 216713 | SAM2, 7.0308 s, then quality computation | The deliberately incomplete ranking generation is repaired and usable measurements are published. Its pre-existing animal box still selects background. |

The three-frame operation completed in **11.4161 seconds**, without inference errors. A separate scratch connection changed 216713 to reject/rating 2 during the detector call; that decision, every unselected photo row, one encounter and five bursts were preserved. The saved review was 1 KEEP / 69 REVIEW / 1 explicit simulated REJECT.

The agent-selected duck-box positive was then replayed in that same seeded directory, retaining its simulated reject. It completed in **6.9183 seconds** (actual SAM2 call 6.5405 seconds), without another detector invocation. Quality was 0.6542, analysis was eligible, and the suggestion remained REVIEW. Its mask pixels exactly equal the previously visually inspected duck mask; the valid ranking generation for 216713 remained byte-for-byte unchanged. This supplied-box intervention remains distinct from successful automatic detection or a user-confirmed annotation.

Post-run checks again passed for every source/copy/sidecar content hash and nanosecond modification time, all live flags/ratings, and the pre-run selected live catalog-row digest `a4fa3442b31bf84caf3ea575324e3bbb04d2f7cf8bf84449d5a5cc8aa88f0728`. All inference processes exited.

The verification tools now use explicit runtime errors for preservation failures, so optimized Python cannot remove them and publish a false success report. Five subprocess regressions run those failure checks with `python -O`. Scratch review rows and flag-attribution notes are limited to the copied workspace; photographer flags/ratings themselves remain intact even when an attribution note originated elsewhere. Canonical foreign-key checks run before preparation commits. **Fifteen focused safety regressions pass**, including multiple-workspace prediction reviews and attribution notes, alongside Ruff and whitespace checks.


A subsequent guard-only change at `62185d0face1f060eb8cab134369bb790200fd27` compares the retained detector run's stored source-input fingerprint as well as its runtime identity before allowing segmentation of reviewed boxes. Its two focused pinned/unpinned stale-source regressions passed independently (`python -m pytest -o addopts='' vireo/tests/test_analysis_retry.py -k changed_source_with_same_runtime -q`). They verify rejection before SAM, rather than claiming another real-model replay. This change does not alter the detector algorithm; the actual-model timings and outputs above remain specifically attributable to `4111fb4aae8ff286674a640ab476997c323532e0`.
