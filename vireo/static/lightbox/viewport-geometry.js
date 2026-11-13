/* Lightbox viewport geometry: the photo's layout box, the fit and native (1:1)
 * scales, the transform that places the image, and the pan clamp. Each function
 * takes the controller's state record, which viewport.js creates and keeps
 * private.
 */
import { updateZoomControl } from './viewport-zoom-control.js';

export function _lbDisplayDimsForRecipe(/** @type {number | null | undefined} */ width, /** @type {number | null | undefined} */ height, /** @type {LightboxRecipe | null | undefined} */ recipe) {
  var w = Number(width) || 0;
  var h = Number(height) || 0;
  if (!w || !h) return { w: null, h: null };
  recipe = recipe || {};
  var rotation = Number(recipe.rotation || 0);
  if (rotation === 90 || rotation === 270) {
    var tmp = w; w = h; h = tmp;
  }
  var crop = recipe.crop;
  if (crop && Number(crop.w) > 0 && Number(crop.h) > 0) {
    w = Math.max(1, Math.round(w * Number(crop.w)));
    h = Math.max(1, Math.round(h * Number(crop.h)));
  }
  return { w: w, h: h };
}

export function applyTransform(/** @type {LightboxViewportContext} */ vp) {
  var t = vp.document.getElementById('lightboxTransform');
  if (!t) return;
  var metrics = layoutMetrics(vp);
  if (!metrics) {
    updateZoomControl(vp);
    return;
  }
  var renderedW = metrics.w * metrics.scale;
  var renderedH = metrics.h * metrics.scale;
  var tx = (metrics.wrapW - renderedW) / 2 + vp.panX;
  var ty = (metrics.wrapH - renderedH) / 2 + vp.panY;
  t.style.transform = 'translate(' + tx + 'px, ' + ty + 'px) scale(' + metrics.scale + ')';
  updateZoomControl(vp);
}

function orientedPhotoDims(/** @type {LightboxViewportContext} */ vp, /** @type {HTMLImageElement} */ img) {
  if (!vp.options.photo().width || !vp.options.photo().height) return null;
  var w = vp.options.photo().width || 0;
  var h = vp.options.photo().height || 0;
  if (vp.options.orientationSwapsAxes(vp.options.photo().orientation)) {
    w = vp.options.photo().height || 0;
    h = vp.options.photo().width || 0;
  } else if (img && img.naturalWidth && img.naturalHeight) {
    var storedAspect = w / h;
    var imgAspect = img.naturalWidth / img.naturalHeight;
    if (Math.abs(storedAspect - imgAspect) > Math.abs((1 / storedAspect) - imgAspect)) {
      w = vp.options.photo().height || 0;
      h = vp.options.photo().width || 0;
    }
  }
  return { w: w, h: h };
}

export function layoutDims(/** @type {LightboxViewportContext} */ vp) {
  var img = /** @type {HTMLImageElement | null} */ (vp.document.getElementById('lightboxImg'));
  if (!img) return null;
  // A developed companion may have been cropped or resized independently of
  // its RAW primary. Use the pixels that actually loaded so the lightbox never
  // stretches the JPEG into the RAW row's catalog dimensions.
  if (
    vp.options.photoId() != null &&
    vp.options.photo().pairKnown &&
    vp.options.photo().pairSource === 'jpeg' &&
    img.naturalWidth && img.naturalHeight
  ) {
    return { w: img.naturalWidth, h: img.naturalHeight };
  }
  // While /original is available, keep the transform layer in original photo
  // coordinates even if the currently displayed tier is /full. That matches the
  // sharp natural-layout path and avoids recalibrating pan/zoom when the source swaps.
  var originalDims = !vp.options.photo().originalUnavailable ? orientedPhotoDims(vp, img) : null;
  if (originalDims) {
    return _lbDisplayDimsForRecipe(
      originalDims.w,
      originalDims.h,
      vp.options.photo().recipe
    );
  }
  if (img.naturalWidth && img.naturalHeight) {
    return { w: img.naturalWidth, h: img.naturalHeight };
  }
  return null;
}

export function layoutMetrics(/** @type {LightboxViewportContext} */ vp) {
  var wrap = vp.document.getElementById('lightboxWrap');
  var t = vp.document.getElementById('lightboxTransform');
  if (!wrap || !t) return null;
  var dims = layoutDims(vp);
  if (!dims || !dims.w || !dims.h) return null;
  var wrapW = wrap.clientWidth;
  var wrapH = wrap.clientHeight;
  if (!wrapW || !wrapH) return null;
  vp.fitScale = Math.min(1.0, wrapW / dims.w, wrapH / dims.h);
  if (!vp.fitScale || vp.fitScale <= 0) vp.fitScale = 1.0;
  t.style.width = dims.w + 'px';
  t.style.height = dims.h + 'px';
  var scale = vp.fitScale * vp.zoom;
  return { w: dims.w, h: dims.h, wrapW: wrapW, wrapH: wrapH, scale: scale };
}

export function recomputeNativeZoom(/** @type {LightboxViewportContext} */ vp) {
  // Compute zoom value that corresponds to 1:1: one decoded source pixel per
  // physical display pixel. On Retina/HiDPI displays this is intentionally
  // smaller than CSS 1:1 by devicePixelRatio.
  var img = /** @type {HTMLImageElement | null} */ (vp.document.getElementById('lightboxImg'));
  if (!img || !img.complete || !img.naturalWidth || !img.naturalHeight) {
    vp.nativeZoom = null;
    return;
  }
  if (!vp.options.photo().width && !vp.options.photo().height && !vp.options.photo().originalUnavailable && vp.options.photo().currentSrcKey !== 'original') {
    // A preview/full tier can load before the metadata request returns. Do not
    // treat that tier's decoded size as true 1:1; keep z/click in pending mode
    // so the later original dimensions can upgrade the zoom accurately.
    vp.nativeZoom = null;
    return;
  }
  var metrics = layoutMetrics(vp);
  if (!metrics || !vp.fitScale) {
    vp.nativeZoom = null;
    return;
  }
  var dpr = vp.window.devicePixelRatio || 1;
  // Guard: very small images (nativeZoom < 1) clamp to 1.0 so fit == 1:1
  vp.nativeZoom = Math.max(1.0, (1 / dpr) / vp.fitScale);
  applyTransform(vp);
}

export function clampPan(/** @type {LightboxViewportContext} */ vp) {
  var metrics = layoutMetrics(vp);
  if (!metrics) return;
  // Effective rendered size after scale
  var scaledW = metrics.w * metrics.scale;
  var scaledH = metrics.h * metrics.scale;
  var baseLeft = (metrics.wrapW - scaledW) / 2;
  var baseTop = (metrics.wrapH - scaledH) / 2;
  // After applying transform, rendered top-left is base + pan.
  // Clamp so at least minVisible px of image stays inside the wrap, per axis.
  var minVisible = 80;
  // X: rendered right >= minVisible; rendered left <= wrapW - minVisible
  //   panX >= minVisible - baseLeft - scaledW
  //   panX <= wrapW - minVisible - baseLeft
  var minPanX = minVisible - baseLeft - scaledW;
  var maxPanX = metrics.wrapW - minVisible - baseLeft;
  if (scaledW <= metrics.wrapW) {
    // Image fits horizontally; center it
    vp.panX = 0;
  } else {
    vp.panX = Math.max(minPanX, Math.min(maxPanX, vp.panX));
  }
  var minPanY = minVisible - baseTop - scaledH;
  var maxPanY = metrics.wrapH - minVisible - baseTop;
  if (scaledH <= metrics.wrapH) {
    vp.panY = 0;
  } else {
    vp.panY = Math.max(minPanY, Math.min(maxPanY, vp.panY));
  }
}

export function isOneToOne(/** @type {LightboxViewportContext} */ vp) {
  if (vp.pendingOneToOne) return true;
  if (!vp.nativeZoom || vp.nativeZoom <= 1.001) return false;
  var tolerance = Math.max(0.01, vp.nativeZoom * 0.01);
  return Math.abs(vp.zoom - vp.nativeZoom) <= tolerance;
}
