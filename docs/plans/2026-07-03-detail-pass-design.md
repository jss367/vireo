# Detail pass: sharpening + noise reduction for edit recipes

## Motivation

Wildlife photos are frequently high-ISO, and exports already render edit
recipes into output files — so today every exported keeper goes out with raw
sensor noise and zero output sharpening. The non-destructive editor covers
geometry and tone but has no detail controls, which is the biggest gap between
Vireo's editor and a Lightroom pass.

## Recipe schema

Three new keys inside `adjustments` (schema version stays 1 — old recipes are
unaffected, new keys default to no-op):

| Key | Range | Default | Meaning |
|---|---|---|---|
| `sharpen` | 0..100 | 0 (absent) | Luminance unsharp-mask amount |
| `sharpen_radius` | 0.5..3.0 | 1.0 (absent) | USM radius, in **native photo pixels** |
| `noise_reduction` | 0..100 | 0 (absent) | Luminance bilateral NR + mild chroma smoothing |

Normalization rules:
- `sharpen` and `noise_reduction` join `_ADJUSTMENT_RANGES`; a value of 0 is
  normalized away like every other adjustment.
- `sharpen_radius` is only stored when `sharpen` is present, and is normalized
  away when it equals the 1.0 default. It never appears alone.

## Semantics: radius is in native pixels, applied at output resolution

Detail ops are neighborhood operations — unlike the tone pipeline they are
**not** scale-invariant, and Vireo renders the same recipe at many sizes
(edit-preview at 1920, preview cache, thumbnails, full-res export). The
contract:

> A recipe's detail settings describe the effect on the **full-resolution
> render**. Smaller renders approximate that full-res result downscaled.

Implementation: the detail pass runs **last**, after geometry, tone, and the
final long-edge resize, at output resolution, with the spatial parameters
multiplied by the render scale:

```
scale = output_long_edge / rendered_long_edge_at_native_res(photo, recipe)
```

`rendered_long_edge_at_native_res` accounts for rotation and crop (straighten
keeps dimensions), so a tight crop rendered at 1920 correctly gets a larger
scale than an uncropped 1920 render. This matches how per-pixel ops already
behave (tone at 1920 == tone at full res, downscaled) and mirrors what a
photographer expects from every RAW editor: sharpening is subtle at fit zoom
and judged at 1:1.

Running the pass after the downscale (instead of inside `apply_recipe` before
`thumbnail()`) is also the cheap order: previews NR a ≤4MP image instead of a
45MP one.

When the caller can't supply native dimensions, scale falls back to 1.0
(apply-as-authored). All photo-backed call sites can supply them via
`render_source.recipe_source_dimensions`.

## Rendering implementation

New module `vireo/detail.py` (neighborhood counterpart to the strictly
per-pixel `tone.py` — the per-pixel contract of `tone.py` is untouched):

- **Order:** noise reduction first, then sharpening.
- **Noise reduction:** bilateral filter on the luma channel (fixed small
  window, spatial sigma scaled by render scale, range sigma and blend derived
  from the amount), plus a mild Gaussian on the chroma channels (chroma noise
  is a large fraction of perceived high-ISO noise). Uses the PIL YCbCr
  conversion for channel separation.
- **Sharpening:** luminance-only unsharp mask (sharpening Y and adding the
  delta back to RGB avoids the color fringing of naive RGB USM), Gaussian
  implemented as a separable numpy convolution — no new dependencies.
- **Tiling:** like `_apply_adjustments`, the pass processes row bands with a
  halo of overlap pixels (halo = max kernel radius) so peak memory stays
  bounded on 45MP exports and tiled output is numerically identical to a
  whole-frame pass.
- Alpha passes through unchanged; identity (both amounts 0) is byte-exact
  because the pass is skipped entirely.

`apply_recipe_to_loaded_image(img, recipe, max_size=None)` gains a
`native_size=(w, h)` keyword; it computes the scale and runs the detail pass
after the resize. `apply_recipe` itself does not apply detail ops.

### Call sites to update (pass `native_size`)

- `app.py serve_photo_edit_preview` (in-progress editor preview)
- `app.py` saved-recipe preview path (~18439)
- `export.py _render_exported_photo`
- `thumbnails.py` recipe branch
- `pipeline_job.py` recipe branches

### No `EDIT_MATH_VERSION` bump

The bump exists to purge cached renders when existing recipes would produce
different bytes. No existing recipe contains detail keys, and recipes without
them render byte-identically, so no purge is needed. (The preview cache is
already invalidated per-photo when a recipe changes.)

## Editor UI (`photo_editor.html`)

- New **Detail** section after Adjustments: `Sharpen` (0–100, step 1),
  `Radius` (0.5–3.0, step 0.1, shown only meaningful with sharpen > 0),
  `Noise reduction` (0–100, step 1). Wired through the existing
  `setAdjustment`/`adjustmentValues`/`recipeForSave`/`syncControls` lists,
  with the radius-only-with-sharpen rule mirrored in `recipeForSave` so
  dirty-state comparison stays stable against server normalization.
- **100% zoom gets a bigger render:** `updatePreview` requests
  `size=3840` (the endpoint max) when `zoomMode === 'actual'`, 1920 otherwise.
  Today 100% zoom displays the 1920 render at natural size, which would make
  detail settings impossible to judge. 3840 is still below native for
  most bodies — an honest "Preview at 3840px" note appears in the status line
  in 100% mode. A true region-render 1:1 preview is a possible follow-up.
- Copy Settings / batch apply / undo / history / XMP sync all carry the
  whole `adjustments` object and need no changes.

## Live-preview transparency

The lightbox WebGL preview (`VireoToneGL`) transcribes the per-pixel tone
pipeline and cannot express neighborhood ops. As with re-edits today, its
live preview is an approximation that snaps to the exact server render after
save; detail ops simply don't participate in the live delta. The editor page
— where detail is actually authored — uses exact server renders, so the
transparency rule ("the preview you judge is the render you get") holds where
it matters. `test_tone_shader_parity.py` is unaffected because `tone.py` is
unchanged.

## Auto Tone

Auto Tone only writes tonal keys and continues to leave detail (like color)
untouched. The `analysis=1` sampling path strips tonal adjustments client-side
before requesting the analysis render; detail keys are stripped the same way
(they would perturb neutral-pixel sampling slightly via NR).

## Testing

- `test_image_edits.py`: normalization/validation of the three keys,
  radius-requires-sharpen, defaults normalized away, range errors.
- `test_detail.py` (new): byte-exact identity at zero amounts; sharpening
  increases edge acutance; NR reduces flat-region noise while preserving a
  step edge; scale < 1 weakens the effect; alpha preserved; tiled == untiled.
- Integration: `apply_recipe_to_loaded_image` with `native_size` matches the
  direct detail pass at scale 1; recipe PUT round-trips detail keys.
