# Encounter continuity from existing species labels

Vireo now uses two additional rules to keep short sequences together when
cached model evidence supports the same species. These rules were selected
using existing photo tags as reference answers; the inference code receives
model predictions, times, boxes, and image embeddings, without those tags.

## Production behavior

For a weak detection between matching confident anchors, accept independently
confident crop or full-image evidence for the same species. Require a real
positive-confidence animal box, at most three middle frames and three seconds
from anchor to anchor, and at least 0.02 intersection-over-union with both
anchor boxes. Each anchor needs exactly one normal-confidence animal detection.
Confident conflicting crop, full-image, or secondary-subject evidence vetoes
this additional recovery. This extends the existing weak-detection rescue
switch; it does not lower the workspace detector threshold.

For an isolated conflicting classification, require three normal-detection
frames in the same folder spanning at most 0.30 seconds. The two anchors must
agree at classifier confidence at least 0.95 and margin at least 0.60. The
middle prediction must be between 0.80 and 0.90, with at least a 0.10 confidence
advantage for each anchor. Require at least 0.80 box intersection-over-union
across all three pairs. Multiple agreeing classifier models on the conflicting
frame, competing confident subjects, corroborating full-image disagreement, or
available embedding cosine similarity below 0.80 prevent the correction.

The isolated classification abstains only in the grouping feature view.
Surrounding frames supply the encounter suggestion. Original classifier rows,
per-subject predictions, photo tags, and ratings are preserved. Saved grouping
features retain the original prediction and supporting context; the encounter
trace explains the adjustment. Neither rule cascades corrections into new
supporting anchors. Existing time, species, burst, and encounter rules still
apply after these adjustments.

The grouping fingerprint changes, so existing caches become outdated and the
next regroup applies the new rules. No image-model rerun is required.

## Evaluation completed October 4, 2026

A fresh private snapshot covered 71,153 photos in 177 sessions, with species
reference tags on 59,934 photos. This included all labeled sessions in the
photographer's All workspace and the October 3 session in USA2026, retaining
unlabeled neighboring photos. The established capture-day assignments remained
97 training, 40 development, and 40 test sessions. Exact file hashes did not
cross partitions; unmatched near-duplicates have not been ruled out.

The comparison tested 34 configurations: current production grouping, 16
production-threshold configurations, 12 sequence-inference configurations,
three independent-photo baselines, and the two continuity variants. The
combined continuity variant was selected using training/development results.

| Partition | Photos | Recovered reference species, before → after | Short same-label splits, before → after | Different-label joins, before → after |
| --- | ---: | ---: | ---: | ---: |
| Training | 41,788 | 21,918 → 21,936 | 971 → 947 | 9 → 9 of 30 pairs |
| Development | 12,951 | 6,804 → 6,822 | 482 → 460 | 7 → 7 of 7 pairs |
| Final test | 16,414 | 7,597 → 7,620 | 315 → 289 | 12 → 12 of 14 pairs |

The final-test reduction in short same-label splits was 8.3%. All 43 saved
grouping constraints passed, including four explicit boundaries that must
remain separate and the previously rejected nuthatch split. The effect is
modest; the small number of differing-label pairs does not establish universal
merge safety. Labels may omit species, so differing tags are a conservative
boundary proxy, not proof of a subject change.

Matching singleton species tags on adjacent same-folder photos within three
seconds supply automatic join references. On training/development changes,
46 newly joined boundaries were supported this way, four used previous human
judgments, and six boundaries in three sequences remained ambiguous because
four middle photos were unlabeled. The dataset stores these inferred checks
separately from actual human approvals. There is no need to ask the photographer
to reconfirm every already-labeled sequence.

The search objective is missing-positive rate plus 0.25 times the short
same-label split rate plus the different-label join rate. Selection requires
no reduction in total recovered positives, no increase in different-label
joins, and preservation of the reviewed grouping constraints. Additional
unverified species suggestions may not exceed the gain in recovered positive
labels; unknown additions are not classified as false positives.

An initial zero-increase gate for unverified additions rejected every candidate
and evaluated only the production baseline on test. That gate was corrected
from training evidence alone. The selected candidate was then frozen before
its first test evaluation; no candidate test outcomes were used to select it
or adjust its parameters. This test partition has now been used and should not
serve as a fresh final test after future tuning.

Manual-only label results are reported separately from imported tags whose
authorship is unknown. Missing tags never establish species absence. Some
cached model runs may use human-selected label lists; this is an evaluation of
retained evidence, not a simulation of a historically untouched import.

## Preservation and replay

The private run `label-guided-algorithm-comparison-20261004` under the local
encounter-evaluation runs directory retains paired reference answers and model
inputs, source signatures, stable split assignments, all training trials,
development comparisons, the frozen candidate, final-test scores, and the
selection-policy amendment. Its `candidate-validation` subdirectory contains
the final report and automatic grouping checks. Photos, paths, and individual
labels stay outside Git. Human grouping judgments remain in the separate
cumulative review database.

The reusable scorer and bounded comparison runner now live in
`tools/encounter-evaluation`. Both the evaluator's retained-feature adapter and
Vireo's feature loader call `vireo/encounter_continuity.py`. Production port
verification compared both paths with the frozen selected experiment across
all 71,153 photos: encounter memberships and species suggestions matched in
every session. This is implementation-equivalence testing, not another tuning
pass over the final test.
