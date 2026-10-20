# Eye keypoint scores and focus measurements

`photos.eye_conf` and the keypoint API's `conf` retain their existing names
for compatibility. They store a **raw model score**, not a probability or
a percentage. Higher responses pass the localization gate more readily;
they do not express the probability that an eye or species was identified
correctly. The lightbox labels this value “eye score”.

## Supported model outputs

| Model/export | Stored score | Range and interpretation |
| --- | --- | --- |
| SuperAnimal-Bird, DLC 3.x ResNet-50, 42 keypoints | Maximum cell in the eye's heatmap | Linear output, no mathematical lower or upper bound; negative values and values above one are possible. |
| SuperAnimal-Quadruped, DLC 3.x ResNet-50, 39 keypoints | Maximum cell in the eye's heatmap | Same linear output semantics, with a different model and training data. |
| RTMPose animal integration export, SimCC | Minimum of the raw x-axis and y-axis peak responses | Export-dependent raw activation scale; the decoder applies no softmax and makes no probability guarantee. Not used by current production taxonomy routing. |

`scripts/export_onnx.py` loads the published SuperAnimal ResNet-50 snapshots
and exports `_SuperAnimalWrapper.forward`'s `bodypart.heatmap` directly.
Both installed ONNX graphs inspected on 2026-10-20 end at `ConvTranspose`
with a bias, without sigmoid, softmax, or clipping after it. The decoder
(`keypoints.decode_heatmaps`) takes the argmax and keeps that cell's value.
`decode_simcc` instead keeps `min(max(x), max(y))`. Geometry rescaling changes
coordinates only. There is no score transformation at persistence.

These are the inspected model artifacts, identified by SHA-256 rather than
assuming that a mutable model name identifies a particular build:

| Model | SHA-256 of `model.onnx` |
| --- | --- |
| SuperAnimal-Bird | `fdcc95a718903a91e56a35403e0f2d3cac30a36e0723c05c07350191cbaa54ef` |
| SuperAnimal-Quadruped | `6c5d4d40824e078aae49ba7078feff988c7bee95416ed8427c58194d32b340e7` |

Both report PyTorch 2.11.0 as producer and ONNX opset 18. Source checkpoint
repositories are [SuperAnimal-Bird](https://huggingface.co/DeepLabCut/DeepLabCutModelZoo-SuperAnimal-Bird)
and [SuperAnimal-Quadruped](https://huggingface.co/mwmathis/DeepLabCutModelZoo-SuperAnimal-Quadruped).
The exporter environment currently requests `deeplabcut>=3.0.0rc14`;
it does not pin the exact DLC version or record the upstream checkpoint
revision in photo rows. Older/replaced weights must be checked independently;
neither the word “confidence” nor the output tensor's shape establishes units.

## Gate and downstream behavior

`pipeline.eye_detection_conf_gate` is a nonnegative, finite threshold in raw
model units. The default remains `0.5`, and equality passes. It has no upper
bound. Settings load, save, and reset this raw number without percentage
conversion. The separate species classifier gate still uses classifier
confidence on its existing scale. Bird and mammal responses are not calibrated
to each other, despite sharing the eye gate setting.

Before measuring focus, the pipeline rejects non-finite scores/coordinates,
scores below the gate, and points outside the current subject mask. If both
eyes qualify, it selects the eye with the greatest local Tenengrad sharpness,
not the greatest keypoint score. It stores that eye's normalized coordinates,
raw score, and local Tenengrad measurement together.

The focus-crop center and lightbox eye tracking use the stored coordinates.
Scoring uses `eye_tenengrad` to calculate an encounter-relative percentile,
`eye_focus_score`; this percentile can influence quality and soft-eye
rejection when eye detection is enabled. `eye_conf` itself is not a quality
weight or a probability multiplier. Values above one therefore do not inflate
quality scores or move focus centers through percentage conversion. Tests
exercise above-one persistence, production bird/mammal routing, and focus
center selection, along with score boundaries and invalid outputs.

## Investigation of issue #2254

Read-only inspection of the original review snapshot found 163 of 3,159 photos
with `eye_conf > 1`: minimum `1.0000863075256348`, maximum
`1.1102712154388428`. All 163 are finite and have bird species predictions
(18 species, including Horned Lark, Yellow-rumped Warbler, and Long-billed
Curlew), consistent with production routing to SuperAnimal-Bird. All 163 live
rows still have these scores and the eye pipeline fingerprint `v1`.

Photo 213531 (`DSC_2265.NEF`, predicted Long-billed Curlew) stores
`1.0579406023025513`. A fresh read-only inference using the original photo,
the installed SuperAnimal-Bird artifact above, the pipeline's 1024-pixel
oriented image, and the snapshot detection box reproduced **exactly**:

- Image size: 1024 × 682; pixel detection box: `(497, 261, 735, 422)`.
- Left eye: `(504.21212100982666, 340.3333282470703)`, score
  `1.0579406023025513`; right eye at the same location, score
  `0.868927001953125`.
- Stored normalized position: `(0.49239464942365885, 0.499022475435587)`;
  local Tenengrad: `76961.2`, also reproduced exactly.

This identifies a reproducible originating model/export and transformation
for the example, without proving historical checkpoint identity for every
row: `v1` is a pipeline revision, not a weight digest. The saved JSON snapshot
does not include even that fingerprint. No per-photo checkpoint hash, DLC
version, or raw tensor was saved, so the other 162 historical inference runs
cannot be attributed to an exact weight revision from existing provenance.

Evidence was read from the completed review's gitignored
`.context/photo-review/pipeline-before.json` and
`wildlife-photo-review-findings.txt` in the local `niamey-v1` workspace, with
matching live catalog rows. These are local evidence, not public attachments.
The reproduction output for this investigation is in this workspace's
`.context/eye-score/reproduction.json`.

No conversion/storage defect was found in these 163 values. They need no
clamping, migration, or recomputation. The pipeline fingerprint stays `v1`
because valid cached measurements retain the same units and meaning. The
fix corrects presentation and threshold controls and prevents non-finite
new outputs from entering focus measurement. It does not rewrite cached
measurements, ratings, flags, species confirmations, or review decisions.

The invalid-output guard fixes a separate downstream failure reproduced by
`test_eye_stage_never_persists_nonfinite_model_score`: the old `score < T`
check accepted NaN and positive infinity, allowing their coordinates and
sharpness measurements into focus-center selection and quality scoring.
The regression runs the pipeline on these outputs and verifies that no eye
measurement is persisted. This fix is tracked with
[issue #2254](https://github.com/jss367/vireo/issues/2254); no such invalid
score was found among the 163 reviewed values.
