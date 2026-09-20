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
3. Geometry, global and masked exposure, and white-balance adjustments operate
   on this source. A luminance shoulder maps highlights to display range, with
   gamut compression to avoid independently clipping color channels.
4. Tonal range, curves, color, presence, and detail controls keep floating-point
   buffers. Existing detail algorithms still run after tone and output resizing.
5. JPEG and PNG output is quantized to 8 bits at encoding. TIFF output from this
   RAW path is quantized to 16 bits per channel and carries an sRGB ICC profile.
   TIFF exports request this path even without a saved edit recipe.

The editor, edited thumbnails, saved previews, original-size edited views,
exports, and external-editor handoffs use the same renderer. RAW lightbox
adjustments use server previews because the JPEG-based browser shader cannot
reproduce adjustments to retained RAW highlight data. Display previews are
still JPEGs, but they are encoded after applying the requested recipe.

Sized decoded RAW buffers share a 128 MiB memory cache keyed by source path,
modification time, file size, and requested size. Full-resolution originals and
JPEG fallbacks are excluded. Cached buffers are copied for each reader. Large
color operations and encoding use bounded row blocks. Browsing and scanning
retain their existing fast JPEG paths and disk working copies.

## Compatibility and limits

- Originals and saved edit recipes are not rewritten. Existing RAW edits can
  look different because their rendering now starts with high-precision data.
  Render version 6 invalidates edited caches and browser image URLs.
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
  as-shot balance. Calibrated Kelvin controls, camera/ISO noise profiles, lens
  profiles, and capture sharpening are separate future improvements.
- This change upgrades RAW processing and TIFF output. It does not add a
  high-bit-depth loader for arbitrary TIFF or PNG inputs.

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
