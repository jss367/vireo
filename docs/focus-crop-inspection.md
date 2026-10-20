# Inspecting focus on the intended animal

Best Batch shows the full upright frame, the chosen model box, and the focus
center together. Choose **Compare subjects** to select a particular animal. The
subject list includes weak detections and their detector confidence so an
intended animal missed by the normal threshold can be chosen explicitly.
A detector confidence is a model suggestion, not confirmation that the box
contains the intended animal. Analyze that selected subject before using its
mask or eye evidence.

Choose **Place manual focus center**, then click the eye or animal in the full
frame. The red marker and coordinates identify the inspection point. **Inspect
selected subject** restores the automatic inspection. Manual placement changes
neither rankings nor saved subjects, flags, ratings, or edits.

The full frame is displayed before saved crop and rotation, in the same upright
coordinate space as detections and native focus crops. The focus label reports
the actual source, raster dimensions, and scale relative to catalog dimensions.
Manual inspection uses automatic source selection and can fall back to an
available working copy when originals are offline. Its source and scale remain
visible. The full frame keeps its natural aspect ratio on phones and tablets so
clicks and overlays stay aligned with the displayed pixels.

One crop pixel is one decoded-raster pixel; a working copy or an embedded RAW
JPEG must not be treated as a fresh full-resolution sensor sharpness analysis.
When dimensions cannot support a single scale, the original scale is unknown.

Unbound legacy photo-level eyes are unavailable through the focus-crop endpoint:
those records have no detection or mask-generation provenance, and overlapping
animal boxes cannot establish which animal supplied an eye. Per-subject analysis
provides detection and generation binding. Invalid boxes, stale active prompts,
changed analysis inputs, and incompatible raster geometry fail closed. A manual
point remains available independently of a failed automatic association.

## Investigation of the reported examples

The two originals and catalog associations were inspected read-only on 2026-10-20.
The reported centers exactly match false model detections used as active mask
prompts. This establishes a subject-selection problem in these examples; it does
not establish a coordinate-transform failure.

- `DSC_4007.NEF` (photo 215273): the active prompt is a 0.442-confidence box over
  rocks/water at center 0.8828, 0.3605. The visible sandpiper is in a separate
  0.052-confidence detection farther left, below the usual detection threshold.
- `DSC_5450.NEF` (photo 216716): the active prompt is a 0.121-confidence box over
  ground at center 0.1704, 0.6473. The duck's head is inside a separate
  0.044-confidence animal box; another larger detection classifies the duck as
  a person. Manual placement remains necessary when no suitable animal box exists.

The original inspection did not run model inference or change the catalog,
sidecars, flags, or originals. Private image files remain local; regression tests
use synthetic images, including an EXIF-rotated JPEG and saved crop/rotation.
