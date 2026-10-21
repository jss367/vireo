# Evaluating evidence for automatic wildlife keepers

This developer tool prepares native selected-region evidence, collects independent
photographer judgments, fits an experimental two-gate candidate, and evaluates a
frozen candidate on held-out encounters. It never opens the application database
for writing or changes photo decisions, sidecars, originals, or app settings.

**There is no approved absolute keeper classifier in this package.** Feature
measurements, fitted coefficients and a favorable offline report are different
things. Reports do not enable runtime keeps. The application continues to route
unvalidated winners to review; cached or client-supplied approval fields cannot
change that. Once a real producer meets its declared validation requirements,
a separate integration must verify the reviewed artifact and current source,
region, recipe and model provenance before enabling that producer. This tool is
concrete preparation for that work, not completion of issue #2244.

## Target and judgments

Judge two narrow properties of the **selected region**: whether identifiable
animal structure is present, and whether obvious severe defocus destroys usable
subject detail. This is not an aesthetic score, a species-confidence score or a
relative ranking. An animal elsewhere in the frame does not validate a debris
crop. A photograph can have a correct animal region but unusable detail.

The default protocol is one independent photographer, blind to filenames, flags,
model scores and neighboring frames. Its identity, protocol, reason, inspection
attestations, revisions and timestamps are recorded. An optional second reviewer
can judge separately; disagreement becomes uncertain rather than being voted
away. Assistant suggestions are recorded separately and are never converted to
human ground truth. A named reviewer is provenance supplied by the operator,
not cryptographic proof of an independent person.

Use `uncertain` for ambiguous animal structure, tiny or occluded regions,
borderline detail or missing evidence. `not_applicable` is allowed for focus only
when the region has no animal. Existing flags, species tags and imported review
notes are not target labels. The review server displays full-frame context,
the selected rectangle, an unresized source-raster link, and the region at native
pixels. Inspect the crop at browser zoom 100%; scroll when necessary.

## Install and curate

Install separately from the application:

```sh
pip install -e tools/keeper-evaluation
vireo-evaluate-keepers curate --db /path/to/vireo.db \
  --selection /scratch/selection.json --output /scratch/native-region-review
vireo-evaluate-keepers status --bundle /scratch/native-region-review
```

A selection declares `label_definition`, optional `review_protocol` and
`sampling_notes`, and an explicit `samples` list. Each sample contains
`photo_id`, `detection_id`, `group_id`, `session_id`, `partition`, and optionally
a random 12-hex-digit `sample_id` and a `lineage_id` shared by derivatives.
Partitions are `development`, `holdout` and `diagnostic`. Keep the already-seen
reported failures and their controls diagnostic; do not tune on them and then
call them unseen validation data. Related scenes and near duplicates must be
grouped conservatively; the operator must audit these group declarations.

The curator uses SQLite `mode=ro` and `query_only`, copies each selected original
to scratch, hashes the copied bytes, and extracts an upright RGB raster. Camera
model is read from the copied original's metadata, with the catalog value kept
separately for comparison. A RAW source uses its embedded JPEG **without a
silent demosaic or working-copy fallback**. This is a native embedded-JPEG
raster, not newly demosaiced sensor detail. Raster dimensions, selected box,
rounded pixel rectangle, detector provenance, exact decode recipe and asset
hashes are frozen. Full-size source/region PNGs are not resampled; only the
context JPEG is reduced. Generated evidence carries no EXIF/GPS metadata.

Missing originals fail explicitly. Output must be a new directory outside the
catalog and selected source directories. The bundle is private local evidence:
it contains source paths and original imagery and should not be committed.

## Review and extract candidate features

```sh
vireo-evaluate-keepers review --bundle /scratch/native-region-review \
  --reviewer 'Independent photographer' --partition development --port 8097
```

Open `http://127.0.0.1:8097`. Judgments go only to the bundle's `reviews/`
directory. Other reviewers' answers and partition names are absent from the
judging page. Held-out review requires an already frozen candidate profile.

An optional bounded extraction pilot uses an **already installed** DINOv2-B
ONNX model directory. It creates one CPU session with at most two threads,
downloads nothing and reads no live configuration:

```sh
vireo-evaluate-keepers extract --bundle /scratch/native-region-review \
  --model-directory /path/to/dinov2-vit-b14 --limit 20 --threads 2 \
  --output /scratch/candidate-features.json
```

The candidate measures non-resampled native luma tiles and a separate 518-pixel
DINOv2 crop embedding. Contrast-normalized derivatives, texture, exposure and
embedding values are **features, not validated focus or animal probabilities**.
The embedding preprocessing resamples for representation extraction; that image
is never substituted for the native focus tiles. Graph, external weights, code,
region bytes, preprocessing and dependency versions are recorded. Controlled
blur tests verify measurement behavior only; they do not establish wildlife
calibration. DINOv2 is a general representation model, so use here is a candidate
design choice requiring evaluation. [Official DINOv2 repository](https://github.com/facebookresearch/dinov2).

Generic quality benchmarks also do not supply these selected-region wildlife
labels. CLIP-IQA evaluates image quality/perception on its benchmarks;
KonIQ++ supplies whole-image quality and degradation annotations, not proof
that Vireo selected a real animal. [CLIP-IQA authors](https://github.com/IceClear/CLIP-IQA),
[KonIQ quality and degradation annotations](https://database.mmsp-kn.de/koniq-image-defects-database.html).

## Fit on development judgments, then freeze

Declare a domain JSON containing exactly `camera_models`, `source_kinds`,
`pixel_recipe` and explanatory `notes`. For example, a Z8 embedded-JPEG study
would declare `NIKON Z 8`, `original_embedded_jpeg`, and
`upright-rgb-embedded-jpeg-or-raster-v1`. This is a proposed scope, not evidence
that every condition inside it has been validated. Source IDs cannot be used
as an approval allowlist. A single-shoot study may support a narrowly declared
scope; it cannot silently justify global claims.

```sh
vireo-evaluate-keepers fit --bundle /scratch/native-region-review \
  --features /scratch/candidate-features.json --domain /scratch/domain.json \
  --output /scratch/candidate-model
vireo-evaluate-keepers development-scores --bundle /scratch/native-region-review \
  --features /scratch/candidate-features.json --model-directory /scratch/candidate-model \
  --output /scratch/development-scores.json
```

The experimental animal gate uses equal-encounter DINO class prototypes. The
detail gate fits a regularized linear classifier on native detail features.
This candidate requires positive and negative independent development groups
for **each** gate. Fitting refuses absent labels, uncertain targets and
insufficient class coverage instead of borrowing diagnostic or held-out labels.
The first seven development cases may not meet the candidate's minimum of two
independent development groups in each class of each gate. After independent
judgments establish the actual shortfall, add new development encounters with
likely animal-invalid regions and/or true-animal severe defocus, plus usable
animal controls as needed. Prior notes may guide sampling but never become
labels. Keep existing held-out encounters and reported diagnostic cases excluded;
freeze a new expanded bundle before collecting its labels and features. Record
this targeted sampling when interpreting coverage; a deliberately enriched set
does not estimate natural deployment prevalence.
It emits uncalibrated scores, not a claim that this approach is accurate.
Inspect development outcomes and select thresholds there; do not inspect
held-out scores or labels to choose the producer, features or thresholds.

```sh
vireo-evaluate-keepers freeze --bundle /scratch/native-region-review \
  --model /scratch/candidate-model --producer-code tools/keeper-evaluation/src/keeper_eval \
  --focus-min FOCUS_THRESHOLD --animal-min ANIMAL_THRESHOLD \
  --domain /scratch/domain.json \
  --maximum-false-keep-rate ERROR_BUDGET --confidence CONFIDENCE_LEVEL \
  --minimum-positive-groups POSITIVE_GROUP_REQUIREMENT --output /scratch/frozen-profile.json
vireo-evaluate-keepers review --bundle /scratch/native-region-review \
  --reviewer 'Independent photographer' --partition holdout \
  --profile /scratch/frozen-profile.json --port 8097
```

Threshold placeholders intentionally have no suggested numeric values. The
profile freezes the model/code directories, pixel recipe, development judgments,
thresholds, scope, error budget, confidence and positive coverage requirement before held-out judgments are recorded. It rejects later
threshold selection using that exposed holdout. Changed code, external weights,
regions or development judgments invalidate their bindings.

## Evaluate the held-out candidate

```sh
vireo-evaluate-keepers score --bundle /scratch/native-region-review \
  --features /scratch/candidate-features.json --model-directory /scratch/candidate-model \
  --profile /scratch/frozen-profile.json --output /scratch/heldout-scores.json
vireo-evaluate-keepers evaluate --bundle /scratch/native-region-review \
  --profile /scratch/frozen-profile.json --predictions /scratch/heldout-scores.json \
  --output /scratch/evaluation.json
```

The error budget is the probability that an invalid region is accepted in a
known-negative encounter. Its denominator is **known-negative encounters**,
not accepted keepers; it is not a production precision estimate. The report
includes separate animal-validity and severe-defocus gate errors as well as
joint errors. Neighboring frames do not inflate the number of trials. Exact
one-sided Clopper-Pearson upper bounds use the declared confidence level.
[Scientific computing implementation](https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.binomtest.html).

Each gate needs its own known-negative coverage; no defocus negatives means
focus remains unvalidated even if the animal gate is perfect. Accepted uncertain
cases prevent meeting the requirement. Minimum positive accepted groups prevent
an always-abstain candidate from appearing useful. Out-of-domain inputs must
abstain. Missing/nonfinite predictions cannot become measured zeroes. The same
holdout is locked against subsequent producer selection. Statistical bounds
assume correctly declared independent groups and accurate labels; report their
sampling and domain limits, not just a pooled number.

The error budget, confidence and positive coverage requirement must be chosen
explicitly when freezing, before held-out review; evaluation has no override options.
Changing these criteria changes the profile and cannot reuse an exposed holdout. Zero observed mistakes is not zero
risk: even six independent negative encounters with no errors have a roughly
39.3% upper bound at 95% confidence. The first tranche has no labels, so there
are **no observed validation error rates yet**. A favorable offline report still
requires evidence review and a trusted, versioned runtime producer with current
source/subject binding, abstention and explicit-decision preservation. Nothing
in this developer package constitutes that runtime authorization.

Run its tests with `python -m pytest tools/keeper-evaluation/tests -q`.

Public annotation candidates and a bounded follow-on plan are assessed in
[Public labels for wildlife keeper evidence](public-evidence-options.md).
