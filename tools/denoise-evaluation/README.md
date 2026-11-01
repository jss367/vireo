# Wildlife RAW denoising comparison

An offline, exploratory comparison of Vireo's existing denoisers with RawNIND's
joint Bayer denoising/demosaicing model. This tool never opens the catalog for
writing and never changes source files, sidecars, ratings, or production settings.
It imports the current checkout's renderer and writes only to the supplied output
directory. Python 3.14 or later is required by Vireo.

The first development set contains 12 user-flagged Nikon D850 photographs from
distinct capture times/species, ISO 80–9000. They are not independent randomized
samples of the whole library and are not a held-out efficacy test. Ten initially
selected Nikon Z8 High Efficiency* files failed sensor decoding with rawpy/LibRaw;
embedded JPEG fallback is deliberately forbidden. This experiment cannot establish
Z8 High Efficiency* support, identification accuracy, or a general quality win.

## Run

```sh
uv venv --python 3.14 .context/denoise/py314
uv pip install --python .context/denoise/py314/bin/python -r tools/denoise-evaluation/requirements.txt

.context/denoise/py314/bin/python tools/denoise-evaluation/download_model.py \
  --output .context/denoise/models

.context/denoise/py314/bin/python tools/denoise-evaluation/run.py \
  --selection .context/denoise/d850-selection.json \
  --model .context/denoise/models/model_bayer.onnx \
  --output .context/denoise/comparison

.context/denoise/py314/bin/python tools/denoise-evaluation/gallery.py \
  --run .context/denoise/comparison \
  --output .context/denoise/gallery \
  --centers .context/denoise/centers.json

.context/denoise/py314/bin/python tools/denoise-evaluation/verify_outputs.py \
  --run .context/denoise/comparison

.context/denoise/py314/bin/python tools/denoise-evaluation/summarize.py \
  --run .context/denoise/comparison --output .context/denoise

.context/denoise/py314/bin/python -m http.server 8766 --bind 127.0.0.1 \
  --directory .context/denoise
```

Open `http://127.0.0.1:8766/gallery/`; measured results are at `/results.html`.
The HTML also opens directly from disk, but revealing
methods needs HTTP. The gallery's native PNGs are lossless; reduced JPEGs are
navigation aids. One image pixel occupies one CSS pixel in native view. Dragging
and scrolling are synchronized. Notes and pair judgments persist in browser local
storage; use **Download judgments** for a portable JSON record. Do not interpret
multiple crops or strengths of one photograph as independent trials. Method
letters are randomized per photo. Initial panes compare Camera-aware 70 with
neural 50 in randomized order; all other methods remain selectable.

The selection JSON has a `photos` array containing `id`, `path`, `iso`,
`camera_make`, `camera_model`, and `species` (list of labels). Optional
`exposure_ev` applies equally to every method; `review_center` is normalized
image x/y for the initial 1024-pixel crop. Existing Vireo photo/subject metadata
can be retained in the manifest for provenance. The first run uses +1 EV for
all photos, chosen to inspect shadow detail. Subject viewing centers are manual
aids, not verified model boxes. The gallery's separate centers file can change
the viewing crop without recomputing any pixels. Original source coordinates,
where available, remain in the private manifest, not in the review UI.

## Compared treatments

- Denoising off (labeled Reference).
- Vireo Standard at 35 and 70.
- Vireo Camera-aware at 35 and 70, resolving each photo's camera/ISO profile.
- RawNIND at 50% and 100%, blended with the ordinary decode in linear light.

These numbers are not equivalent strengths. They provide a small development
sweep; no method is labeled the best existing setting before photographer review.
Sharpening, clarity, texture, masks, and saved photo edits are disabled. Each
method renders at full resolution before any review crop or reduced preview.
All images pass through Vireo's same floating-point tone pipeline and are
quantized to sRGB PNGs only after rendering.

## RAW bridge and correctness checks

RawNIND takes black-subtracted, white-normalized, non-white-balanced Bayer data
and returns camera RGB. It already demosaics; the result is never demosaiced a
second time. Bayer cells are aligned to RGGB with reflected boundary samples.
The implementation uses 512-cell tiles, 64-cell overlap, gain normalization,
and clipped-highlight protection adapted from LightCraft's `denoise-core`.
It executes through ONNX Runtime CPU with four threads, not LightCraft's Rust
or GPU executor. CPU is intentional: upstream documents FP16 overflow in this
Bayer model on CoreML GPU/ANE.

To retain Vireo's actual white balance and color transform, the adapter measures
a linear matrix from four constant-color patches injected into a *separate
in-memory decoder*. Four additional patches check the matrix. Sampled scene
pixels outside the patch region must remain within 1e-5 linear RGB of an
unmodified decode, and held-out patches within 0.001. Failure aborts the photo.
No probe image is saved to disk or used as the baseline. This experimental
adapter requires unrotated Bayer originals and matching sensor/output geometry.

This validates the linear color bridge; it does not establish image-detail
fidelity, equivalence of demosaicing algorithms, or absence of all color errors.
The comparison measures the combined neural denoising/demosaicing change. It
does not isolate denoising alone. Gain matching can itself affect colors, so
color shifts belong in reviewer notes.

Original SHA-256 is checked before and after each photo. Results retain source
metadata, package versions, rendering-code hashes, model hash, calibration
errors, profile selection, stage timings, and process peak RSS. Peak RSS is a
process-lifetime high-water mark, not a per-method memory measurement. Timings
come from a machine shared with other work, and use Vireo's NumPy detail fallback
when its optional native extension is unavailable. Do not compare these numbers
with LightCraft's published GPU timings.

Completed photos can resume only when their source, settings, code and package
fingerprints match and all outputs exist. Results are written only after all
seven treatments finish. After changing rendering code, use a fresh directory.
Gallery generation hard-links full images and previews, so it needs the same
filesystem as the run directory. Output images and private manifests belong in
gitignored scratch storage; only this runner belongs in source control.

## Model and code provenance

Model: RawNIND UtNet2 Bayer, published in darktable-ai's `rawdenoise-nind.dtmodel`
release 5.6.0. Weights are GPL-3.0 and are downloaded separately, not included
in this repository. Package metadata identifies training images as CC BY 4.0 /
CC0. This experiment does not decide how a shipping Vireo release should obtain
or distribute weights.

- [Model documentation](https://github.com/darktable-org/darktable-ai/tree/master/models/rawdenoise-nind)
- [Research paper](https://arxiv.org/abs/2501.08924)
- Model SHA-256: `da27509dab6a2915da67e988acd86cf71f9d5bbc8d1aa0ed32933578a887b901`
- Archive SHA-256: `d71b5f1e727c85a359e6f74dca9e2016c9d8fc3e2f7ac3e9b347d80ceca969af`
- LightCraft reference checkout: `8aabf61449bc736d10f1d753b5497b54a55e9365`.
  Its adapted algorithms retain their MIT notice in `LICENSE-LightCraft.txt`.

## Validation and next decision

```sh
.context/denoise/py314/bin/python -m pytest --confcutdir=tools/denoise-evaluation \
  -o addopts='' tools/denoise-evaluation/test_neural.py \
  tools/denoise-evaluation/test_gallery.py -q
```

Tests cover all four Bayer phases, odd dimensions, overlap coverage, constant
preservation through the complete tiler, gain normalization, and clipped
highlights, and protection against attaching new blind labels to stale gallery
images. Real-image calibration results are in each `result.json`.

The next decision requires the photographer's blinded judgments. No automated
smoothness or sharpness score is a substitute: blur reduces noise too. If a
useful treatment emerges, freeze its settings before selecting a fresh held-out
set. Add controlled noisy/clean reference pairs before making claims about
preservation of true markings. This first batch cannot validate the previously
proposed 30-photo preference threshold; it is preparation for that test.

## Repeat the decisive comparisons

The first blind review preferred neural 50% on three photos and Camera-aware 70
on three, with four ties and two uncertain judgments. All six decisive choices
favored the right pane. The follow-up checks preference consistency on those six
photos with fresh, balanced side assignments and shuffled photo order. It uses
the identical rendered pixels and crop centers; no model inference runs again.

```sh
.context/denoise/py314/bin/python tools/denoise-evaluation/repeat.py \
  --source .context/denoise/gallery \
  --judgments .context/denoise/judgments.json \
  --output .context/denoise/repeat
```

Open `http://127.0.0.1:8766/repeat/` using the same local server. Each photo gets
one overall preference and a main reason. Notes are optional except for Other.
The progress counter requires both preference and reason. Methods stay hidden,
judgments use a separate browser storage key, and the export filename is
`wildlife-denoising-repeat-judgments.json`. Earlier votes are preserved.

The generator refuses an existing output directory to protect an ongoing review.
It records its random seed, assignments, and original judgments in `design.json`
and `reveal.json` for analysis. Use the recorded `--seed` to reproduce assignments
in a new directory. Assets are hard links, requiring the same filesystem.

After review, match preferred methods across rounds by photo, count agreement,
reversals, new ties and uncertainty, and summarize reasons and side preferences.
The reviewer has already seen the first round's decoded results, so memory may
influence this repeat despite hidden method names. Selection on prior decisive
votes also means this is a consistency check, not a fresh efficacy estimate.

## Fresh photographs with frozen settings

The six-photo repeat preferred neural on four photos, with one tie and one
uncertain judgment. Two preferences repeated, two reversed, and two became
non-decisive. Reasons were not supplied. This motivates a new batch, without
pooling the repeated photos as independent evidence.

The fresh screening batch has ten photos at ISO 1600 or higher and two at ISO
400 or lower. Selection uses saved species labels and capture metadata, excludes
rejected and wildlife-excluded photos, and excludes captures within 30 minutes
of any development photo or other selected photo. It is not limited to flagged
picks. Crop centers are fixed from existing eye coordinates or embedded-preview
composition before rendering; previews are not used to judge sharpness. The
batch includes birds and one cat for fur detail. It is limited to the D850 and
is not a representative library-wide sample.

```sh
.context/denoise/py314/bin/python tools/denoise-evaluation/run.py \
  --selection .context/denoise/fresh/selection.json \
  --model .context/denoise/models/model_bayer.onnx \
  --output .context/denoise/fresh/comparison --pair-only

.context/denoise/py314/bin/python tools/denoise-evaluation/verify_outputs.py \
  --run .context/denoise/fresh/comparison

.context/denoise/py314/bin/python tools/denoise-evaluation/pair_gallery.py \
  --run .context/denoise/fresh/comparison \
  --selection .context/denoise/fresh/selection.json \
  --output .context/denoise/fresh/gallery \
  --seed fresh-pairs-2026-10-31-v1
```

Open `http://127.0.0.1:8766/fresh/gallery/`. Rendering uses the same pinned model,
50% neural blend, Camera-aware 70, and common +1 EV as before, while omitting
unused treatments. The fresh gallery balances sides within each ISO group and
shuffles photo order. Choices count immediately; reasons are optional and counted
separately. A new storage key and export filename preserve all prior reviews.

The frozen `fresh/plan.json` records the selection hash and decision rule before
rendering: at least 8/10 high-ISO neural preferences, neither low-ISO check preferring
the baseline, and no documented recurring artifact pattern would justify further
validation or an integration prototype. Ties and uncertainty remain in the
denominator. This is a directional screening gate, not statistical significance
or permission to ship. Complete all 12; do not stop early, tune strengths, or
silently replace technical failures. Low ISO does not establish a clean reference.

## Subject protection prototype

The fresh batch preferred neural on 6/10 high-ISO photos, with one baseline win
and three ties; both low-ISO checks tied. The dove and quail notes specifically
reported smoother neural output with lost detail. That misses the frozen 8/10
screening gate for a global treatment. The masked follow-up tests a new hypothesis
on those two photos and the warbler where baseline won; it is a targeted design
check, not another independent efficacy estimate.

`masked_blend.py` composites the exact saved Camera-aware 70 subject with the
neural 50% background. It fills enclosed mask holes, keeps an 8-native-pixel
protective margin, and transitions outward over 16 more pixels with smoothstep.
Blending is display-referred sRGB, like Vireo's local-detail blend, using already
quantized 8-bit endpoints. Subject-core pixels and background pixels beyond the
transition must equal their respective endpoints exactly. This does not yet
implement the production floating-point RAW pipeline or demonstrate mask quality.

The dove and quail use copied SAM2-large masks. The warbler had no saved mask:
`prompted_mask.py` uses the existing detector box, a 2048-pixel crop of the baseline
render, and already-downloaded SAM2-large weights. Model logits are thresholded at
zero before resizing. Its new mask and provenance stay in experiment storage.
Manual review covers overlays and native edge crops; a valid numeric blend does
not prove absence of visible halos, missed feathers, or incorrect segmentation.

```sh
.context/denoise/py314/bin/python tools/denoise-evaluation/masked_blend.py \
  --selection .context/denoise/masked/selection.json \
  --source .context/denoise/fresh/comparison \
  --masks .context/denoise/masked \
  --output .context/denoise/masked/comparison

.context/denoise/py314/bin/python tools/denoise-evaluation/masked_gallery.py \
  --source .context/denoise/fresh/comparison \
  --masked .context/denoise/masked/comparison \
  --selection .context/denoise/masked/selection.json \
  --output .context/denoise/masked/gallery
```

The gallery at `http://127.0.0.1:8766/masked/gallery/` has six blind pairs: each of
three photos compares the protected composite against each prior global method.
Protected is on the left once and the right once for each photo. The Subject edge
view focuses on the mask boundary nearest the original viewing center. Choices
and optional reasons persist separately from all earlier reviews. Export uses
`wildlife-denoising-subject-protection-judgments.json`. Analyze by photo and comparator;
these six pairs represent only three photographs. Do not infer a production win
without checking preservation, background benefit, and boundary artifacts.
