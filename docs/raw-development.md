# RAW development

Vireo's built-in editor develops supported RAW originals through its own
high-precision pipeline. Darktable remains a separate optional integration.

## Processing and output

1. LibRaw applies the camera's as-shot white balance and demosaics at full
   resolution. It produces 16-bit linear XYZ with automatic brightening disabled
   and two stops of reserved integer headroom. The default demosaicing algorithm
   is used rather than the browsing path's faster Bayer algorithm or half-size
   binning.
2. Vireo restores the exposure scale and converts XYZ to linear RGB in 32-bit
   floating point. Negative and above-display-white channels survive this
   conversion; integer RGB clipping and an intermediate JPEG are avoided.
3. Matched lens-profile vignetting, manual vignetting, and edge-aware
   purple/green defringing operate in linear light. Calibrated and manual
   radial distortion share one resample, transforming the photo and its local
   mask before rotation and cropping. Geometry, global and masked exposure,
   and white-balance adjustments operate on this source. A luminance shoulder
   maps highlights to display range, with
   gamut compression to avoid independently clipping color channels.
4. Shadows and Highlights use an edge-aware log-luminance base to move broad
   tones while retaining fine texture contrast. Restoration fades near strong
   edges, deep black, and display white to limit halos and noise amplification.
   Whites and Blacks remain point curves. Tonal range, curves, color, presence,
   and detail controls keep floating-point buffers. Existing detail algorithms
   still run after tone and output resizing.
5. JPEG and PNG output is quantized to 8 bits at encoding. TIFF output from this
   RAW path is quantized to 16 bits per channel and carries an sRGB ICC profile.
   TIFF exports request this path even without a saved edit recipe.

The editor, edited thumbnails, saved previews, original-size edited views,
exports, and external-editor handoffs use the same renderer. RAW lightbox
adjustments use server previews because the JPEG-based browser shader cannot
reproduce adjustments to retained RAW highlight data. Display previews are
still JPEGs, but they are encoded after applying the requested recipe.
Shadows/Highlights also use server previews for JPEG sources, since their
neighborhood filter cannot be reproduced by the single-pass browser shader.
The filter radius scales with loaded resolution, and overlapping row tiles
provide the same neighborhood support in full-resolution exports.

Sized decoded RAW buffers share a 128 MiB memory cache keyed by source path,
modification time, file size, and requested size. Full-resolution originals and
JPEG fallbacks are excluded. Cached buffers are copied for each reader. Large
color operations and encoding use bounded row blocks. Browsing and scanning
retain their existing fast JPEG paths and disk working copies.

## Compatibility and limits

- Originals and saved edit recipes are not rewritten. Existing RAW edits can
  look different because their rendering now starts with high-precision data.
  Render version 9 invalidates edited caches and browser image URLs. Existing
  Shadows/Highlights recipes gain the new detail-preserving behavior.
- An unsupported RAW can still fall back to its embedded JPEG. Offline files
  can use a companion JPEG or working copy. Those sources retain their original
  precision limits; their TIFF exports remain 8-bit. A pre-existing darktable
  development continues to take precedence during export.
- Preserving values above display white allows exposure recovery of retained
  data. It does not reconstruct detail from saturated sensor samples. The new
  decode preserves channels without LibRaw's highlight blending; dedicated
  sensor-aware highlight reconstruction remains future work.
- XYZ avoids prematurely clipping colors to the sRGB triangle, but the integer
  decoder still imposes a finite range. The final working/output primaries and
  profile are sRGB; wide-gamut output profiles are not yet offered.
- Temperature and tint remain relative RGB adjustments around the camera's
  as-shot balance. Calibrated Kelvin controls and capture
  sharpening are separate future improvements. Camera-aware denoising is
  available; its non-local means filter uses an 8-bit guide and applies the
  resulting noise correction to the floating-point image, preserving its
  finer tonal steps.
- This change upgrades RAW processing and TIFF output. It does not add a
  high-bit-depth loader for arbitrary TIFF or PNG inputs.

## Lens corrections and defringing

The editor's **Lens Corrections** panel offers automatic profile matching and
four manual controls. Profile corrections are off by default; select **Use
matched lens profile** to apply the available calibration. Manual controls
default to zero. Recipes, history, copy/paste, presets, batch edits, previews,
thumbnails and exports all retain these settings through the shared renderer.

### Automatic profiles

Vireo bundles the unmodified Lensfun camera/lens database at revision
`052c3ec6380aaa172caa6d7ea3a5eaafe0b052da`, with its CC-BY-SA-3.0 license,
photographer credits, and provenance in `vireo/data/lensfun/`. No separate
installation or runtime download is required. The profile reader is Python;
Vireo does not bundle the Lensfun library.

Matching uses the camera body and mount, lens name/maker, focal length, and
camera crop factor. Case, punctuation, spacing and an optional maker/f-number
marker may differ; every remaining name token and number must agree. Unknown
lenses, ambiguous matches, and focal/aperture specifications without a lens
identity are skipped. A profile calibrated on a smaller sensor is not extended
beyond its calibrated frame. An explicit metadata crop factor takes precedence
over the camera database, including for an in-camera sensor crop.

Supported rectilinear models are `poly3`, `poly5`, and `ptlens` distortion,
plus `pa` vignetting. Distortion interpolates in focal length. Vignetting also
interpolates in aperture stops, using the nearest calibrated focus distance
(the farthest calibration if distance is unknown). Aperture values beyond the
measured range use the nearest endpoint; missing aperture skips vignetting.
Focal lengths outside calibration coverage are skipped. Invalid, folding,
or extreme maps are rejected. Fisheye, off-center, and Adobe `acm` models and
embedded camera correction tables are not supported.

The status shows the matched lens and available corrections, or why no profile
is applied. Only the enable flag is saved in a reusable recipe: each destination
photo resolves its own calibration when rendering. Cache fingerprints include
the resolved calibration and metadata so camera/lens changes invalidate old
renders. Corrections apply only to a decoded scene-linear RAW source. Embedded
JPEGs, companion JPEGs and working copies skip profiles to avoid correcting an
already rendered source twice. The editor verifies which source its actual
preview used and reports when a profile was skipped. Its mask overlay and brush
map use that same source decision.

Profile-enabled caches record whether their pixels came from a scene-linear
RAW or a rendered fallback. A fallback cannot satisfy a later RAW cache hit;
later requests retry the RAW so recovered sources regain their requested
profile corrections. Equal-key requests already sharing a render may consume
that render's fresh fallback once.

### Manual fine-tuning

Manual controls work on both RAW and rendered images and can supplement an
enabled profile:

- **Distortion** (−100 to 100): positive corrects barrel distortion; negative
  corrects pincushion distortion. A radial inverse map preserves the source
  dimensions and automatically insets the sampled frame as needed to avoid
  empty borders. This can trim the field of view. Subject masks and their
  overlays receive the same map; the entire brush footprint maps back to the
  source so its displayed circle stays the size and shape shown by the cursor.
- **Vignetting** (−100 to 100): positive brightens dark corners, negative
  darkens them, with up to 1.5 stops of gain or attenuation at the corners.
  Gain is applied in linear light around the uncropped image center.
- **Purple fringe** and **Green fringe** (0 to 100): reduce the selected
  Oklab chroma near strong lightness edges while retaining lightness. Flat
  colored areas and other hues are left alone. Detection width scales with
  the uncropped image's loaded resolution. Increasing the amount increases
  both strength and width; inspect at 100% and use the lowest effective value.

The correction algorithms and profile-model interpretation follow LightCraft,
with its MIT license and copyright
preserved in `THIRD_PARTY_NOTICES.md`. Vireo uses linear sRGB, retains floating
point RAW precision and alpha, and bounds intermediate memory with overlapping
row tiles.

Lateral chromatic-aberration estimation remains future work. Already corrected
camera JPEGs usually need
little or no distortion/vignetting adjustment. Defringing can also desaturate
real purple or green subject colors at contrast edges; it is not a semantic
bird/branch detector.

## Validation

`vireo/tests/test_raw_precision.py` creates a small Bayer DNG with standard tags
and decodes it through the installed LibRaw. It verifies above-white and
sub-byte detail, highlight recovery before display mapping, precision through
geometry and spatial/local controls, actual 16-bit TIFF samples, preview/export
agreement, JPEG fallback, and bounded cache invalidation. Synthetic radiance
tests separately check exposure behavior against known linear-light values.

These tests verify the data path. Camera-specific visual comparisons on
difficult wildlife photos are still needed to tune default rendering and choose
future denoising, highlight-reconstruction, and sharpening algorithms.

`vireo/tests/test_spatial_tone.py` checks texture contrast at matched average
brightness, hard-edge halo limits, noise protection near black, ordered tone
ramps, local-mask isolation, float precision, tile seams, and preview scaling.
These synthetic checks do not replace visual tuning on real wildlife photos.
The RAW highlight shoulder still runs before the range controls; moving range
compression ahead of that shoulder is a separate future change.

`vireo/tests/test_optics.py` checks known radial-grid recovery, border coverage,
linear-light falloff recovery, fringe hue/edge isolation, mask alignment,
float precision, transparency, and seamless row tiles. Browser tests in
`tests/e2e/test_photo_editor_optics.py` cover control persistence, independent
resets, undo, brush geometry and preview/export agreement. These synthetic
scenes do not replace visual tuning on real wildlife photos.

`vireo/tests/test_lens_profiles.py` checks matching against bundled telephoto
profiles, rejection of uncertain matches, crop-factor scaling, focal/aperture
interpolation, missing metadata, invalid maps, metadata-dependent cache
invalidation, and shared mask geometry. A real LibRaw DNG decode verifies
preview/export/thumbnail agreement, and browser tests check profile status,
persistence, brush mapping and JPEG fallback behavior. Real-camera comparisons
against another calibrated renderer are still needed for visual validation.
