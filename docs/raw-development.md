# RAW development

Vireo's built-in editor develops supported RAW originals through its own
high-precision pipeline. Darktable remains a separate optional integration.

Desktop builds decode Nikon High Efficiency and High Efficiency* sensor data
using the official November 2026 LibRaw snapshot. See
[native Nikon RAW decoding](nikon-high-efficiency.md) for source-install setup,
verification, and the temporary build's removal plan.

## Processing and output

1. Clipped sensels are rebuilt in the mosaic first (see
   [Clipped highlights](#clipped-highlights)). LibRaw then applies the camera's as-shot white balance and demosaics at full
   resolution. It produces 16-bit linear XYZ with automatic brightening disabled
   and two stops of reserved integer headroom. The default demosaicing algorithm
   is used rather than the browsing path's faster Bayer algorithm or half-size
   binning.
2. Vireo restores the exposure scale and converts XYZ to linear RGB in 32-bit
   floating point. Negative and above-display-white channels survive this
   conversion; integer RGB clipping and an intermediate JPEG are avoided.
   The scale puts the sensor ceiling of the least amplified channel (green,
   for most cameras) at 1.0, as a standard clipped decode does.
   LibRaw's unclipped modes instead scale to the most amplified channel,
   which darkens the frame by the largest white-balance multiplier (about a
   stop in daylight); Vireo multiplies that back, so only the channels white
   balance amplifies, and rebuilt highlights, extend above 1.0.
3. Matched lens-profile vignetting, manual vignetting, and edge-aware
   purple/green defringing operate in linear light. Calibrated and manual
   radial distortion share one resample, transforming the photo and its local
   mask before rotation and cropping. Geometry, global and masked exposure,
   and white-balance adjustments operate on this source. The RAW look then
   maps scene radiance to display range (see [The RAW look](#the-raw-look)).
4. Shadows and Highlights are a local exposure change on scene radiance,
   applied before the RAW look: each pixel gains or loses up to 1.7 stops
   according to the brightness of its neighborhood, an edge-aware base of log
   luminance whose blur is 1.5% of the frame's long edge. A dark region lifts
   as a whole, so its texture keeps its contrast, and Highlights can bring
   retained radiance above display white back into range before the look's
   shoulder compresses it. Their zones are placed by displayed brightness: a
   region the look shows at middle grey barely moves under Shadows, as a
   middle-grey region of a JPEG does. Near strong edges the base falls back to
   each pixel's own level, so flat regions gain no rim. Whites and Blacks
   remain point curves. Tonal range, curves, color, presence,
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
neighborhood base cannot be reproduced by the single-pass browser shader.
The base is built once per frame on a coarse grid and sampled by each pixel's
position, so row tiles of a full-resolution export need no overlap and match a
whole-frame render exactly.

Sized decoded RAW buffers share a 128 MiB memory cache keyed by source path,
modification time, file size, and requested size. Full-resolution originals and
JPEG fallbacks are excluded. Cached buffers are copied for each reader. Large
color operations and encoding use bounded row blocks. Browsing and scanning
retain their existing fast JPEG paths and disk working copies.

## Compatibility and limits

- Originals and saved edit recipes are not rewritten. Existing RAW edits can
  look different because their rendering now starts with high-precision data.
  Render version 9 invalidates edited caches and browser image URLs. Render
  version 10 re-renders Shadows/Highlights edits as local exposure: deep
  shadows lift at most 1.7 stops, mid-shadows move less than under the old
  curve, and Highlights can now darken display white on JPEG sources.
  Render version 13 corrects the decode's brightness scale (step 2): a RAW
  edit saved under an earlier version renders brighter by the ratio of its
  photo's largest white-balance multiplier to its smallest, about one stop.
  Lowering its Exposure by log2 of that ratio restores the look it had.
  Render version 14 adds the RAW look: every RAW render, edited or not, is
  about 1.2 stops brighter in the midtones and has more contrast, and
  Shadows/Highlights act on the tones that are now displayed as shadows and
  highlights. An edit saved under an earlier version was tuned against the
  darker, flatter render and usually needs its Exposure lowered; the curve
  also adds contrast, so Exposure alone brings it close, not identical.
- An unsupported RAW can still fall back to its embedded JPEG. Offline files
  can use a companion JPEG or working copy. Those sources retain their original
  precision limits; their TIFF exports remain 8-bit. A pre-existing darktable
  development continues to take precedence during export.
- Preserving values above display white allows exposure recovery of retained
  data. Detail is not reconstructed where every channel saturated; such an
  area is rebuilt flat and neutral.
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

## The RAW look

A linear decode shown as it is looks wrong next to the camera's own JPEG of
the same exposure: more than a stop darker in the midtones, and flat. Cameras
meter to keep highlight headroom, so middle grey sits well under 18% of the
sensor ceiling, and they brighten and add contrast when they render. Browse
and the lightbox show that camera rendering, so the editor has to start from
something close to it, or every edit begins by undoing the difference.

`tone.raw_look_curve` is that starting point. It maps scene-linear luminance
`s` (1.0 is the sensor ceiling) to display-linear luminance `v / (1 + v)`,
where `v = (s / 0.18) * ((s + 0.016) / 0.196) ** 0.95`:

- 0.18 is the scene level shown at half display luminance (sRGB 188).
- The exponent makes midtone contrast 1.95 in log-log terms: one stop of
  scene brightness spans almost two on the display.
- Below about 0.016 the curve is linear, so deep shadows keep their relative
  differences.
- The rational shoulder approaches white without reaching it. The sensor
  ceiling lands at sRGB 251 and a rebuilt blown area (about 2.0) at 254.

The three constants were fitted to camera JPEGs: 48 NEF files, 16 each from a
Nikon D7200, D850 and Z 8, spread across each camera's shooting days. Each
file's linear decode and its embedded JPEG were reduced to the same grid, and
the median JPEG luminance was taken at each scene level.

| Scene level (stops under the ceiling) | D7200 | D850 | Z 8 | RAW look |
|---|---|---|---|---|
| -7.9 | 9 | 12 | 10 | 9 |
| -6.4 | 26 | 28 | 23 | 26 |
| -4.9 | 64 | 63 | 60 | 63 |
| -3.4 | 137 | 139 | 131 | 134 |
| -1.9 | 213 | 215 | 217 | 216 |
| -0.4 | 249 | 247 | 251 | 248 |

Values are 8-bit sRGB luminance. The three cameras agree within about 5
levels of each other, which is why one curve serves all of them. Over the 48
files the mean brightness of an unadjusted render is within 1 level of the
camera JPEG's (standard deviation 5, worst file 15, 92% of files within 10);
without the look it was 35 levels darker. A single file can differ because Active D-Lighting and the
Picture Control change the camera's rendering per shot. Cameras from other
makers were not measured.

The curve is applied to each pixel's largest and smallest channel, and the
middle channel keeps its relative place between them. Hue is unchanged, and
saturation follows the curve as it does in a camera's rendering: over the 48
files the unadjusted render's mean chroma is 3% above the camera JPEGs' (7%
above for the median file), where scaling all three channels by one luminance
gain left it 16% lower and pushed bright saturated colors past white. A channel below zero (a color outside sRGB) is
first brought to zero by reducing chroma toward the pixel's luminance.

The look applies only to scene-linear RAW sources. A JPEG, a working copy or
an embedded preview is already rendered and passes through unchanged. Exposure,
white balance and Shadows/Highlights act before the curve; everything else
acts on its output. `vireo/tests/test_raw_look.py` holds the curve to the
measured camera levels above.

## Clipped highlights

A saturated sensel records only a lower bound, and each channel's ceiling
lands at a different level once white balance is applied. Decoded as they are,
blown areas come out pink: red and blue stand above green. Before demosaicing,
Vireo therefore raises clipped sensels in white-balanced camera space:

- A clipped channel takes the mean of the other two channels around it, so a
  highlight that clipped only green keeps the color red and blue measured.
- A neighboring channel that is also clipped counts as reaching the brightest
  channel there. An area where every channel clipped becomes neutral at the
  highest ceiling, which sits above display white by the largest
  white-balance multiplier; the RAW look renders it white.
- A sensel is only raised, never lowered, and the correction fades in over the
  last tenth below the ceiling. Unclipped photos decode exactly as before.

Each channel uses valid per-channel saturation metadata when available. A
flat clipped area at that known ceiling needs no histogram evidence. Otherwise,
its ceiling is the sensor's scalar white level, or the channel's own peak
when a narrow terminal histogram spike rises above a populated highlight tail
just below that (a D850's greens can stop near 16240 of 16383). A flat bright
field or a smooth peak below white is left alone without that evidence.
The pass adds roughly 0.2 to 0.6 seconds to a 45 MP decode. Render version 12
invalidates edited caches so existing RAW edits pick it up. Raised values have
to fit the 16-bit mosaic: a file whose white level is already near 65535 has
no room and keeps its clipped values. Mosaics with a fourth filter color,
and files LibRaw delivers without a mosaic, are not rebuilt.

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
RAW or a rendered fallback and an opaque primary-source identity. Offline
fallbacks remain reusable while the source is unchanged and missing; restoring
or changing the RAW invalidates them immediately. When the RAW is present but
its decoder falls back, reuse lasts at most 60 seconds before retrying, avoiding
a decode on every hover while allowing transient failures to recover.
Equal-key requests already sharing a render may consume its fresh fallback.

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
Shadows/Highlights run on scene radiance before the RAW look.

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
