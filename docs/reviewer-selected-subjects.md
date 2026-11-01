# Reviewer-selected subject regions

When a visible animal has no retained detector region, open the photo's
**Subjects** panel and choose **Draw subject box**. Drag around the animal on
the upright, unedited source image, save the box, then choose
**Retry selected box analysis**. The retry measures only that region.

The panel stays visible when no region exists. It explicitly distinguishes
full-image classification from localized evidence; a missing region does not
establish animal absence. Drawing a region does not assign or confirm a species.

Reviewer boxes are stored under `reviewer-selected`, separate from detector
output, and become the primary subject. A new box replaces the previous
reviewer box for that photo. Detector reruns and Reclassify leave it intact.
Both Process and standalone Classify include the current reviewer box alongside
automatic animal boxes, even when detection returns no animals. Reclassification
refreshes matching species scores while retaining their accepted/rejected decisions.
If the reviewer redraws during classification, results from the old coordinates
are discarded before publication. Publication briefly locks the photo against
redraws; inference itself does not hold that lock.
The internal confidence value of 1 preserves the explicit region through
confidence filtering; it is not a model probability. The lightbox labels the
region as reviewer-selected instead of displaying a detector percentage.

Saving a box, including re-saving the same coordinates, clears its cached box
analysis and previous subject measurements, mask, embedding, and eye state. Box measurements use the existing subject-analysis path at up to 1024
pixels on the long edge: sharpness, area, clipping, median brightness, quality,
and an exposure suggestion. They include background inside the rectangle.
They do not constitute segmentation or eye analysis, and do not mark the Full
pipeline complete. Applying crop or exposure suggestions remains a separate
explicit action.

The catalog stores the region and its measurements; saved image edits and
species decisions are unchanged. Reviewer-box saves currently have no Undo/Redo,
like primary-subject selection. Replacing a reviewer box cannot be reversed
through edit history; the drawing dialog states this before saving. Existing read-only review views disable the
new write controls. API routes verify workspace visibility, validate normalized
coordinates, and reject a scoped retry if its region was replaced during work.

## Verification on the reported kinglet frames

All three local working-copy images were visually inspected, copied into
scratch test catalogs, and exercised through the browser: draw, save, then
retry. The live catalog and source photos were unchanged. The tests seeded
unrelated legacy quality and eye values and verified that saving the box cleared
them before computing the region's own measurements.

| Photo | Catalog ID | Selected source box (x, y, width, height) | Result |
| --- | --- | --- | --- |
| DSC_3245 | 229027 | 0.536, 0.392, 0.067, 0.106 | Own box measurements; no inherited eye value |
| DSC_3256 | 229032 | 0.448, 0.410, 0.047, 0.108 | Own box measurements; no inherited eye value |
| DSC_3264 | 229037 | 0.448, 0.410, 0.047, 0.108 | Own box measurements; no inherited eye value |

The local `.context/DSC_*-verification.json` files record measured features and
box identities; matching screenshots show the selection and resulting panel.
Private photos and generated local evidence are not committed. These checks
verify measurement ownership, not species identity, keeper quality, or final
analysis readiness. The wider 125-frame visibility audit supplied with the
report was not rerun.

Regression coverage includes full-image evidence separation, stale-measurement
clearing, invalid boxes, workspace visibility, replacement during analysis,
model reruns, and drawing coordinates despite saved crop/rotation edits.
