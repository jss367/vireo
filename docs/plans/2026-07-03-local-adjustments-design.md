# Mask-based local adjustments (subject / background)

## Motivation

Vireo already knows where the bird is — MegaDetector boxes prompt SAM masks
that exist for most keeper photos. No generic editor has that. Local
adjustments turn it into the editor's differentiating feature: brighten and
sharpen the subject, quiet down the background, one slider each, no manual
brushing.

## What exists (facts the design builds on)

- `photo_masks`: one row per (photo, variant); PNG single-channel masks
  (0/255) on disk at `~/.vireo/masks/{photo_id}.{variant}.png`, generated at
  working resolution in the **orientation-corrected working image space**
  — the pixel space of `image_loader.load_image` / `render_proxy` output,
  which applies `ImageOps.exif_transpose` (both standard images and RAW
  postprocess), before any recipe geometry (rotation/flip/straighten/crop)
  is applied. The prompt detection box is stored per mask;
  `photos.active_mask_variant` picks the live one.
- Masks regenerate when the detection prompt changes and can be deleted by
  storage cleanup (`delete_stale_masks`, variant deletion). Detection IDs are
  not stable across detector re-runs.
- The lightbox mask overlay currently hides itself when a recipe has
  geometry because the raw mask no longer lines up — a display-layer
  workaround this feature replaces with a real transform.
- The tone pipeline (`tone.py`) is strictly per-pixel; the detail pass
  (`detail.py`) is neighborhood-based, runs last at output resolution with
  scale-adjusted kernels.

## Design

### Recipe schema

New optional `local` section — a single object carrying the shared mask
reference plus an array of region entries (v1 supports at most one `subject`
and one `background` entry):

```json
"local": {
  "mask": {"ref": "a1b2c3d4", "feather": 12.0},
  "regions": [
    {
      "region": "subject",
      "adjustments": {"exposure": 0.6, "shadows": 25, "sharpen": 30}
    },
    {
      "region": "background",
      "adjustments": {"exposure": -0.4, "saturation": -15, "noise_reduction": 40}
    }
  ]
}
```

- The mask reference lives at `local.mask`, not per region — subject and
  background always share one snapshot (background is its inverse), and
  hoisting the ref means a **background-only** recipe still records the
  snapshot needed for deterministic renders, stale-mask comparison, and
  snapshot GC (no fall-through to the live active mask). Feather is a
  Gaussian softening radius in **native pixels**, scaled at render time like
  detail kernels.
- `local` is dropped from normalization entirely when `regions` is empty or
  every region normalizes away — the shared `mask` never persists without at
  least one active region referencing it.
- Allowed per-region adjustments (v1): `exposure`, `highlights`, `shadows`,
  `contrast`, `saturation`, plus detail's `sharpen`/`sharpen_radius`/
  `noise_reduction`. Same ranges and normalization rules as global
  adjustments; zero values normalize away, empty entries are dropped.

### Mask snapshots, not live references

The recipe's `local.mask.ref` points to a **content-addressed snapshot**
copied to `<db_dir>/edit-masks/{photo_id}.{sha1[:12]}.png` at the moment the
first local adjustment is added — where `<db_dir>` is `dirname(--db)`, the
same root as the existing `<db_dir>/masks/` store. Vireo supports arbitrary
`--db` paths, so a global `~/.vireo/edit-masks/` keyed only on `photo_id`
would collide across separate databases (tests, alternate libraries) and
let one database's snapshot GC delete another's referenced snapshots.

Why not reference the live active mask: renders must stay a deterministic
function of (source pixels, recipe). Preview/thumbnail caches invalidate on
recipe change and `EDIT_MATH_VERSION` bumps only — a mask silently
regenerating under a live reference would change render output with no cache
invalidation and no user-visible cause, violating the no-black-boxes rule.

Staleness is surfaced, not automated: when `photos.active_mask_variant`'s
mask content differs from the snapshot, the editor shows "Newer subject mask
available — Update" and updating rewrites `local.mask.ref` (a normal recipe
edit: undoable, cache-invalidating). If the snapshot file is missing, the
renderer **disables the whole local pass** for that photo — both subject
*and* background weights held at zero, so no region's adjustments apply —
and the editor shows a warning. (A no-op / all-zero subject mask would leave
background weight = `1 − 0` = 1 everywhere, silently applying background
edits to the entire frame; disabling both regions keeps missing snapshots
from turning into a whole-photo edit.)

Snapshot lifecycle: created on first use, garbage-collected when no recipe
(current or in edit history) references them — same sweep style as other
storage cleanups, surfaced in `/api/storage`.

### Rendering

Order stays geometry → tone → resize → detail, with local weights woven in.
Tone runs at pre-resize working-image dimensions inside `apply_recipe`, then
`apply_recipe_to_loaded_image` calls `thumbnail(max_size)`, then detail runs
at the post-resize render size — so a single weight map cannot serve both
passes. The weight map is built once and materialized at both scales:

1. **Weight map.** Load the snapshot mask and apply the recipe's geometry
   (rotation/flip/straighten/crop — same transforms as the image, bilinear).
   From that geometry-transformed mask, materialize two aligned copies with
   feather applied at each target scale (Gaussian radius scaled the same way
   as detail kernels), normalize to [0,1]:
   - **Tone weight** — sized to the pre-resize working image, consumed by
     the tone pass inside `apply_recipe`.
   - **Detail weight** — sized to the post-`thumbnail` render size, consumed
     by the detail pass.

   Background weight = 1 − subject weight, computed independently at each
   scale so tone and detail each see aligned subject/background pairs.
2. **Local tone** runs inside the existing tone pass with per-pixel strength:
   e.g. exposure becomes `lin * 2^(ev_global + ev_subject·w + ev_bg·(1−w))`;
   range/saturation controls interpolate their amounts by `w` the same way.
   This keeps every op per-pixel — it adds a weight *input*, not a
   neighborhood — so the WebGL live preview can adopt it later by sampling
   the mask as a second texture (out of scope for v1; the lightbox preview
   approximates and snaps to the server render, as it already does for
   detail and re-edits).
3. **Local detail** rides the existing detail pass: compute the NR/sharpen
   deltas once, blend them by the detail weight map before adding back.

No `EDIT_MATH_VERSION` bump: recipes without `local` render byte-identically.

### Editor UI

- Two new bands, **Subject** and **Background**, shown only when the photo
  has a usable mask; otherwise one honest line ("No subject mask — run the
  pipeline's mask stage") instead of dead sliders.
- Each band: its allowed sliders + a shared **Feather** slider on the
  Subject band. A small mask-outline overlay toggle on the preview (reusing
  the lightbox overlay endpoint) shows exactly which pixels count as
  subject — the transparency rule applied to masks.
- Stale-snapshot banner with an explicit Update action as above.
- Copy Settings / presets: local adjustments are **not** included in presets
  (they reference a photo-specific mask); Copy Settings to a group copies
  the local slider values and re-snapshots each target photo's own active
  mask, skipping (and reporting) photos without one.

### Editor preview

The editor's server renders pick up local adjustments automatically (they
render the full recipe). The uncropped editor preview uses the
geometry-transformed-but-uncropped weight map, consistent with how crop is
previewed today.

## Phasing

1. **PR 1 — renderer + schema.** `local` normalization, mask snapshot
   plumbing + GC, weight-map builder, weighted tone + detail, API round-trip.
   Fully testable without UI (synthetic masks in tests).
2. **PR 2 — editor UI.** Subject/Background bands, feather, overlay toggle,
   stale-mask update flow, copy-to-group behavior.
3. **PR 3 (optional) — live preview.** Mask texture in the lightbox WebGL
   shader for instant local-tone feedback.

## Open questions (decide during PR 1)

- Feather default: start at ~2% of the long edge, clamp to [0, 60px] native?
- Should `background` NR default on softly when the subject is sharp and the
  background is high-ISO mush? (Tempting, but silent defaults fight the
  transparency rule — v1 says no.)
- Mask resolution: snapshots are working-resolution; is bilinear upscale to
  full-res export acceptable at feather radii we allow? (Expected yes — the
  feather blur dominates interpolation error.)
