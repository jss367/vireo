# Interpreting relative focus measurements

Focus scores help order similar photographs. They are not calibrated estimates
of absolute sharpness. A low percentile can describe a photograph with visible
eye and head detail; a high percentile can describe the least blurred photograph
in an entirely blurred group.

The photo inspector shows the measurement basis, raw Tenengrad measurement,
percentile, formula, comparison size and exact comparison filenames and photo
identifiers. The comparison consists of photos with current, complete subject
analysis in the scored encounter and the same measurement basis, including the
photo itself. Photographer flags do not change membership. A review burst or a
filtered screen can be smaller than that encounter. A selected-batch review
scores the selected batch as one temporary encounter.

Eye measurements use a small eye window at multiple scales. Subject measurements
use the subject mask at multiple scales. Both summarize squared image gradients;
their raw values depend on region, texture, noise, exposure, processing and image
scale. Eye focus is the midpoint percentile within the eye cohort. Subject focus
combines 70% subject percentile with 30% subject/background gradient separation
(subject divided by subject plus background, with a small numerical epsilon).
Measured zero subject gradients retain a relative score of zero. Ties receive
their midpoint rank; a singleton receives a 50% percentile.

Relative focus no longer triggers `out_of_focus` or `eye_soft` rejection. The
composite still uses relative focus for ordering. Its rejection-floor check
replaces that contribution with the neutral midpoint, so low focus rank cannot
indirectly trigger a composite rejection. That midpoint is a counterfactual
policy value, not a measurement. Other measured issues, such as crop truncation
or highlight clipping, retain their rejection rules. Photographer decisions take
precedence. Missing or stale subject analysis follows the shared readiness policy
and goes to review.

No validated absolute defocus classifier is supplied by this change. Every
recorded focus explanation has `absolute_status: "not_validated"`. The Misses
page's existing gradient ratio and low-gradient heuristic remain review
candidates under “Focus needs review”; they do not establish absolute defocus.
Legacy scores with missing cohort provenance are explicitly identified as such.
Rerun scoring to refresh saved suggestions; opening an old cache does not rewrite
photographer decisions or silently reconstruct an unknown historical cohort.

## Investigation of the reported examples

The investigation used the original wildlife review's saved measurements and
contact sheet, plus read-only access to the catalog, original files and existing
4,096 × 2,729 working images. It did not update the library, analyze into the real
catalog, change XMP sidecars, or rerun model inference. The reviewed contact sheet
shows visible eye and head detail in the Western Grebe and House Finch examples;
the Eared Grebe shows discernible plumage, with less distinct eye detail. These
observations do not independently establish full-resolution RAW sharpness.

| Photograph | Subject | Saved eye Tenengrad | Saved subject Tenengrad | Saved background Tenengrad | Saved relative focus |
| --- | --- | ---: | ---: | ---: | ---: |
| DSC_3153.NEF, photo 214419 | Western Grebe | 1,229.57 | 32,813.38 | 6,057.47 | 0.0049 |
| DSC_3300.NEF, photo 214566 | Eared Grebe | 870.92 | 51,703.37 | 13,445.28 | 0.0066 |
| DSC_3674.NEF, photo 214940 | House Finch | 10,408.07 | 20,190.84 | 4,221.84 | 0.0076 |

The Western Grebe was lowest among 103 saved encounter eye measurements. Its
midpoint percentile, 0.5 / 103, rounds to 0.0049. The House Finch was lowest among
66, reproducing 0.0076. The Eared Grebe was lowest among 180 populated eye
measurements in a saved 189-photo encounter, which yields 0.0028 and does **not**
reproduce the saved 0.0066. Historical comparison scope, selected region and
analysis-generation provenance are incomplete; the cause of that discrepancy is
unresolved. The tests retain that discrepancy instead of treating reconstructed
membership as proof of the historical calculation.

Original EXIF reports 1/400 second and f/6.3 for all three, with ISO 200, 180 and 90
respectively. Repeating the default eye-window calculation on the existing
working images yielded 1,019.95, 262.61 and 5,792.38, respectively, rather than the
cached values. This does not identify a particular cause: historical window
settings, input processing and selected-subject provenance were not fully
recorded. Cached and newly measured values must not be treated as interchangeable.

As a sensitivity probe, the same floating-point grayscale windows were scaled by
0.5 without clipping, changing geometry or introducing blur. Their multiscale
Tenengrad fell to exactly 0.25 of the original, while normalized spatial profiles
were unchanged. This establishes exposure sensitivity of the measurement, not
exposure invariance or a real camera exposure experiment. The automated exposure
regression additionally shows a peer-rank reversal after a gain change on a fixed
edge profile. Real exposure changes can also change noise, clipping and apparent
detail; this probe does not model those effects or prove the cause of the reported
mismatches.

Regression fixtures preserve the three measured target records and available
saved encounter eye measurements, with synthetic non-focus peer features to
isolate ranking. Regrouping each unchanged target as a singleton changes its
percentile to 50% without changing the underlying measurement or introducing a
focus rejection. Separate tests isolate the composite path, mixed eye/subject
cohorts, missing evidence, disabled eye analysis, serialization and photographer
decisions. Provenance: issue #2245 and the read-only local review artifacts
`pipeline-before.json`, `wildlife-photo-review-findings.txt` and
`sheets/focus-worst-00.jpg`.

## Data contract

Each scored photo's `focus_evidence` records its basis, raw measurement and
background measurement where relevant, unrounded percentile, rounded relative
score, method, encounter scope, cohort size and key, and absolute status.
`encounter.focus_cohorts[key]` records basis, scope, size, exact photo identifiers
and a count of measurements lacking identifiers. Membership is stored once per
encounter to avoid quadratic serialized payload growth. Best Batch cards forward
the same evidence and return their temporary encounter's cohort map. Clearing
derived scores or marking subject analysis unavailable clears stale focus
explanations. The explanation describes the saved scoring input; later changes
to grouping or analysis require a new score to refresh it.
