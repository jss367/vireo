# Camera-aware denoising

In the photo editor, open **Detail**, choose **Camera-aware** under
**Denoise method**, and increase **Denoise**. Inspect the result at 100% zoom
before saving. Zero leaves noise reduction off. **Reset Detail** restores
standard denoising and clears the detail adjustments.

The editor shows the detected camera and ISO, and whether it found matching
measurements. The bundled database contains measurements for 427 camera
models from darktable. Intermediate ISO values interpolate between measured
values; values outside a camera's measured range use the nearest endpoint
and are identified in the editor. Camera models match exactly after common
manufacturer prefixes, spacing, and punctuation are normalized. Unknown
cameras and missing ISO use noise estimated from the image itself.

Camera-aware denoising uses OpenCV non-local means to reduce luminance and
color noise before sharpening. It estimates noise from small patches in the
render and uses the camera measurements as a bounded guide. This is important
because a developed image's noise changes with demosaicing, camera processing,
exposure edits, and resizing. Copying a recipe or applying a preset resolves
the destination photo's camera and ISO; it never copies the source camera's
profile. Subject/background Denoise adjustments use the selected method too.

The mode works in previews, thumbnails, exports, and external-editor handoffs.
Existing recipes retain the standard denoiser unless explicitly changed.
Processing is local and requires no model download. Full-resolution exports
are slower than standard denoising; processing uses overlapping tiles to
bound temporary memory. Smaller previews estimate their own remaining noise,
so a full-resolution inspection is the best guide to fine detail.

## Scope and limitations

This is camera-guided denoising of developed, 8-bit RGB images, including
images decoded from RAW. It is not a neural RAW mosaic denoiser. The linear
sensor measurements are a prior, not an exact noise model after the image
has been developed. Image-based estimates limit their influence to 20% in
either direction. Very dense textures, banding, hot pixels, heavy compression,
and unusually processed images can defeat the estimator. Adjust the amount
or switch back to Standard when needed.

The algorithm and fallback are inspired by the distinction between sensor
noise and processed-image noise described in darktable's
[noise profiling documentation](https://www.darktable.org/2012/12/profiling-sensor-and-photon-noise/).
The filter uses [OpenCV non-local means](https://docs.opencv.org/4.13.0/d5/d69/tutorial_py_non_local_means.html).
Dataset provenance, contributor credits, and the dataset license are included
in `vireo/data/denoise/` and summarized in `THIRD_PARTY_NOTICES.md`.
