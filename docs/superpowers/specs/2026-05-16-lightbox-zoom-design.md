# Lightbox Zoom Design Notes

## Summary

The lightbox should treat 1:1 as one source image pixel per device pixel, not one source image pixel per CSS pixel. On Retina/HiDPI displays, CSS-pixel 1:1 appears visually zoomed to roughly 200%, which does not match Lightroom-style pixel inspection.

The target implementation should use the browser-decoded natural image dimensions as the source of truth, lay out the image at its natural dimensions, and scale it down to `naturalWidth / devicePixelRatio` and `naturalHeight / devicePixelRatio` when the user requests 1:1.

## Test Findings

A temporary diagnostic page compared several rendering strategies against Lightroom:

| Strategy | Result |
| --- | --- |
| Current Lightbox: CSS 1:1 | Sharpness is poor and apparent zoom is about 200%. |
| Current Lightbox: Device 1:1 | Apparent zoom is closer, but sharpness is still poor. |
| Natural Layout: CSS 1:1 | Sharp, but apparent zoom is about 200%. |
| Natural Layout: Device 1:1 | Sharp and visually matches Lightroom's 100% view. |
| Direct Image: Device 1:1 | Sharp and visually matches Lightroom's 100% view. |

These results separate two problems:

- CSS 1:1 is the wrong zoom target on HiDPI displays.
- The current lightbox render path introduces avoidable resampling or transform artifacts, because even its device-pixel 1:1 variant looks poor.

## Decision

Use **Natural Layout: Device 1:1** as the model for the real lightbox.

At 1:1:

```text
displayedCssWidth = image.naturalWidth / window.devicePixelRatio
displayedCssHeight = image.naturalHeight / window.devicePixelRatio
```

This makes one decoded source pixel map to one physical display pixel on a 2x Retina screen, and falls back naturally to CSS 1:1 on standard 1x displays.

## Implementation Guidance

- Load the highest-resolution source available before entering 1:1.
- Wait for `img.complete`, `img.naturalWidth`, and `img.naturalHeight` before computing 1:1 scale.
- Prefer natural-size image layout with explicit `width` and `height` based on decoded dimensions.
- Avoid nested scale transforms or layout paths that fit the image first and then scale a wrapper to 1:1.
- Keep pan state and centering independent from the zoom target so navigation can preserve 1:1 without carrying over stale dimensions.
- Clamp only after computing the device-pixel target, so large images do not silently fall back to CSS-pixel 1:1 behavior.

## Verification

Manual verification should compare Vireo against Lightroom on a Retina display:

- Open the same original photo in Lightroom and Vireo.
- Enter 1:1 in Vireo.
- Confirm apparent magnification matches Lightroom's 100% view.
- Confirm fine detail is as sharp as the Natural Layout: Device 1:1 and Direct Image: Device 1:1 diagnostic variants.
- Navigate between photos while staying in 1:1 and confirm the next photo recomputes from its own decoded natural dimensions.

Automated coverage should check the mechanical pieces:

- 1:1 scale uses `naturalWidth / devicePixelRatio`.
- The original/full-resolution source is selected before 1:1 when available.
- Arrow navigation preserves 1:1 mode but recomputes the scale for the new photo.
