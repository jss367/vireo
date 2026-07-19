# Mask-Based Local Adjustments: Implemented Architecture

> **Status:** Implemented on July 4, 2026 by
> [Local adjustments: renderer and recipe schema (#1096)](https://github.com/jss367/vireo/pull/1096)
> and
> [Local adjustments: editor controls and mask workflow (#1100)](https://github.com/jss367/vireo/pull/1100).
> This document describes the behavior that shipped. The longer original
> proposal remains available in the commit history of
> [the design pull request (#1088)](https://github.com/jss367/vireo/pull/1088).

## Purpose

Vireo can use the subject masks it already creates with the Segment Anything
Model to apply edits separately to a bird and its background. A photographer
can brighten or sharpen the subject, quiet or denoise the background, and
preview the exact transition between them without painting a mask manually.

The shipped design follows three principles:

1. A saved edit is deterministic. It renders from a frozen mask snapshot, not
   whichever live mask happens to exist later.
2. The mask follows the same rotation, flip, straighten, crop, and output scale
   as the image.
3. An untrustworthy mask disables all local adjustments for that render. It
   never turns a missing subject mask into a whole-image background edit.

## User Experience

The photo editor shows Subject and Background control groups when the photo has
an active subject mask. Each group exposes Exposure, Shadows, Highlights,
Saturation, Sharpen, and Denoise controls. Region values are deltas from the
photo's global edit settings, so a photographer can add or subtract an effect
within either region. A shared Feather control softens the boundary.

The first local control change freezes the active mask. Saved recipes continue
to use that snapshot even if the detection pipeline later regenerates the live
mask. When the live mask changes, the editor shows a "Newer subject mask
available" message. Choosing Update creates a new snapshot and makes the recipe
dirty; the output never changes silently.

Show Mask overlays the transformed, feathered subject weight used by the
renderer. Reset Local removes the complete local section without disturbing
global adjustments or geometry.

## Stored Recipe Contract

Local adjustments are an optional section of the version 1 edit recipe:

```json
{
  "version": 1,
  "local": {
    "mask": {
      "ref": "a1b2c3d4e5f6",
      "source_digest": "sha1:0123456789abcdef",
      "feather": 12
    },
    "regions": [
      {
        "region": "subject",
        "adjustments": {
          "exposure": 0.6,
          "sharpen": 30
        }
      },
      {
        "region": "background",
        "adjustments": {
          "saturation": -15,
          "noise_reduction": 40
        }
      }
    ]
  }
}
```

The mask belongs to the local section because subject and background share one
weight map; background uses its inverse. Version 1 permits at most one entry for
each region.

The schema accepts local Exposure, Highlights, Shadows, Contrast, Saturation,
Sharpen, Sharpen Radius, and Noise Reduction. Tone controls use the same ranges
as their global equivalents. Local Sharpen and Noise Reduction accept signed
deltas from -100 to 100 even though their global values are limited to 0
through 100. Sharpen Radius is an absolute regional override rather than a
delta. Feather is stored in native-image pixels from 0 through 200.

Zero-value adjustments are removed during normalization. Empty regions are
removed, and the entire local section is removed when no effective regions
remain. A non-empty local section requires a valid 12-character lowercase
hexadecimal snapshot reference and a source digest.

Presets retain only global adjustments. They exclude geometry and local edits
because both are specific to a photo.

## Mask Snapshot Lifecycle

`POST /api/photos/<photo_id>/local-mask/snapshot` reads and fully decodes the
photo's active mask, checks that its aspect ratio agrees with the
orientation-corrected photo within one percent, and writes a content-addressed
snapshot. The reference is the first 12 hexadecimal characters of the SHA-1
digest of the mask bytes.

Snapshots live in the application's `edit-masks` directory beside the other
render caches. Their names have the form:

```text
<photo_id>.<ref>.png
```

Creation writes a unique temporary file and publishes it with an atomic rename.
Reusing an existing snapshot refreshes its modification time so cleanup cannot
immediately remove a snapshot returned to an editor that has not saved yet.

`source_digest` is a separate staleness signal. It hashes the same mask bytes
plus the active mask variant, detector model, and stored detection prompt. The
recipe endpoint compares that digest with the current active mask. Rendering
does not use the live mask and does not change when the digest becomes stale.

Snapshot cleanup runs with stale-mask storage cleanup. It retains references
reachable from current recipes and edit history, and it keeps every
unreferenced file for at least 24 hours. Snapshot files move with recipes when
a Joint Photographic Experts Group image is paired into a raw-image primary.

Recipe saves reject snapshot references that do not exist for that photo. A
missing, unreadable, or malformed snapshot encountered later disables all local
regions for that render and logs a warning.

## Rendering Contract

The renderer uniformly resizes the snapshot to the loaded source only when
their aspect ratios agree. It then applies the recipe's rotation, flips,
straightening, and crop to both image and mask. Feather is scaled from native
pixels to the consumer's output resolution before the mask becomes a floating
point weight from 0 for background to 1 for subject.

Local tone is evaluated per pixel. For each supported tone control, the
effective value is:

```text
clamp(global + subject_delta * weight + background_delta * (1 - weight))
```

The result is clamped to the corresponding global control range before the
tone operation consumes it. The existing global-only path remains separate and
unchanged when no local tone deltas are active.

Sharpening and noise reduction need neighboring pixels, so local detail uses
two branches. The renderer resolves the global settings plus subject deltas and
the global settings plus background deltas, runs each distinct detail branch,
and blends the outputs with the feathered weight. This preserves global detail
settings while allowing cases such as background denoise with reduced subject
denoise.

All recipe-rendering paths load the saved snapshot, including editor previews,
cached previews, thumbnails, exports, external-editor handoffs, originals
caches, and iNaturalist preparation. If the snapshot cannot be aligned or
loaded, global edits still render while both local branches are skipped.

## Editor and Server Behavior

The editor renders local changes through the server's normal edit-preview path.
`GET /photos/<photo_id>/edit-mask-preview` uses the same weight-map builder as
the renderer and returns a tinted overlay. The overlay omits crop for alignment
with the editor's uncropped canvas but preserves the saved crop's output scale
when computing feather, so its transition matches the saved render.

Copy Settings and bulk recipe application copy the slider values but never the
source photo's snapshot. The server synchronously creates a snapshot from each
target photo's own active mask and stores a per-photo recipe. Photos without a
usable mask are skipped and reported through `local_errors`; a failure for one
photo does not abort successful targets.

The returned `recipes` map contains the recipe actually stored for every
successful target because each target has a different snapshot reference.
These writes participate in the normal edit history, cache invalidation, and
sidecar synchronization behavior.

## Deliberate Differences from the Original Proposal

Implementation review reduced the first release to a smaller failure-safe
contract:

- A recipe has one copied snapshot, not a family of snapshots for different
  raw-image decode bases.
- Snapshot creation copies the active mask. It does not rerun detection or
  segmentation in a raw demosaic's pixel space.
- An aspect mismatch or unavailable fallback basis disables local rendering or
  prevents snapshot creation instead of materializing another variant.
- Bulk application snapshots each target synchronously. It does not use
  placeholder references, durable background jobs, or compare-and-swap
  finalization.
- Atomic file publication plus a 24-hour cleanup grace period replaces the
  proposed publish-and-cleanup lock protocol.
- The mask overlay is a server-rendered image. The browser graphics pipeline
  does not yet receive a mask texture for instantaneous local-tone previews.
- Feather defaults to zero, and Vireo does not apply automatic background
  denoise or any other silent local default.

Each simplification fails toward omitting local edits rather than applying them
to the wrong pixels.

## Deferred Enhancements

Two extensions remain candidates rather than commitments:

1. Pass a mask texture to the browser graphics pipeline for immediate
   local-tone feedback while dragging a control.
2. Generate additional mask bases for raw images whose embedded preview and
   demosaiced output have materially different aspect ratios.

Either enhancement should start from the shipped contracts above and be
tracked independently. The original proposal is historical context, not an
open implementation checklist.

## Implementation References

- `vireo/local_masks.py` owns snapshot creation, loading, staleness, transfer,
  and cleanup.
- `vireo/image_edits.py` owns recipe normalization, mask geometry, feathering,
  local tone dispatch, local detail blending, and preview weights.
- `vireo/tone.py` owns per-pixel weighted tone calculations.
- `vireo/app.py` owns snapshot, recipe, bulk-copy, preview, and cleanup routes.
- `vireo/templates/photo_editor.html` owns the Subject and Background controls,
  stale-mask update flow, and overlay state.
- `vireo/tests/test_local_masks.py`, `vireo/tests/test_local_render.py`, and the
  photo application programming interface tests cover the feature contracts.
