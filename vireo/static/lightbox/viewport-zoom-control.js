/* The lightbox zoom range and its control: the limit, the slider's logarithmic
 * mapping, the badge text, and the popover. Imports nothing, so the other
 * viewport modules can all depend on it.
 */
export function zoomLimit(/** @type {LightboxViewportContext} */ vp) {
  return vp.nativeZoom ? vp.nativeZoom * 4 : 4;
}

function sliderPosition(/** @type {LightboxViewportContext} */ vp, /** @type {number} */ zoom) {
  var maxZoom = zoomLimit(vp);
  if (maxZoom <= 1.001) return 0;
  return Math.max(0, Math.min(1000, Math.log(Math.max(1, zoom)) / Math.log(maxZoom) * 1000));
}

export function zoomFromSliderPosition(/** @type {LightboxViewportContext} */ vp, /** @type {string | number} */ position) {
  var maxZoom = zoomLimit(vp);
  if (maxZoom <= 1.001) return 1;
  var ratio = Math.max(0, Math.min(1, Number(position) / 1000));
  return Math.exp(Math.log(maxZoom) * ratio);
}

function zoomDisplayText(/** @type {LightboxViewportContext} */ vp) {
  if (vp.pendingOneToOne) return 'Loading 1:1';
  if (vp.zoom <= 1.001) return 'Fit';
  if (vp.nativeZoom) return Math.round((vp.zoom / vp.nativeZoom) * 100) + '%';
  var fitMultiple = Math.round(vp.zoom * 10) / 10;
  return fitMultiple + '\u00d7 Fit';
}

function nativeSliderPercent(/** @type {LightboxViewportContext} */ vp) {
  return vp.nativeZoom ? sliderPosition(vp, vp.nativeZoom) / 10 : 0;
}

function nativeCoincidesWithFit(/** @type {LightboxViewportContext} */ vp) {
  return !!vp.nativeZoom && vp.nativeZoom <= 1.001;
}

export function updateZoomControl(/** @type {LightboxViewportContext} */ vp) {
  var badge = vp.document.getElementById('lightboxZoomBadge');
  var slider = /** @type {HTMLInputElement | null} */ (vp.document.getElementById('lightboxZoomSlider'));
  var nativeStop = /** @type {HTMLButtonElement | null} */ (vp.document.getElementById('lightboxZoomNativeStop'));
  var fitStop = /** @type {HTMLButtonElement | null} */ (vp.document.querySelector('.lb-zoom-stop-fit'));
  var maxLabel = /** @type {HTMLButtonElement | null} */ (vp.document.getElementById('lightboxZoomMaxLabel'));
  var zoomOut = /** @type {HTMLButtonElement | null} */ (vp.document.getElementById('lightboxZoomOut'));
  var zoomIn = /** @type {HTMLButtonElement | null} */ (vp.document.getElementById('lightboxZoomIn'));
  var text = zoomDisplayText(vp);

  if (badge) {
    badge.textContent = text;
    badge.title = 'Zoom: ' + text + '. Open zoom controls';
  }
  if (slider) {
    slider.value = String(Math.round(sliderPosition(vp, vp.zoom)));
    slider.setAttribute('aria-valuetext', text);
    slider.disabled = !!vp.options.photo().transitionPending;
  }
  if (zoomOut) zoomOut.disabled = vp.options.photo().transitionPending || vp.zoom <= 1.001;
  if (zoomIn) zoomIn.disabled = vp.options.photo().transitionPending || vp.zoom >= zoomLimit(vp) - 0.001;

  var combinedFitNative = nativeCoincidesWithFit(vp);
  if (fitStop) {
    fitStop.textContent = combinedFitNative ? 'Fit \u00b7 1:1' : 'Fit';
    fitStop.disabled = !!vp.options.photo().transitionPending;
  }
  if (nativeStop) {
    var nativePosition = nativeSliderPercent(vp);
    nativeStop.disabled = !!vp.options.photo().transitionPending;
    // When fit and 1:1 are visually indistinguishable, combine the labels at the
    // left edge. Otherwise keep 1:1 at least 8% along the track so a near-fit
    // native stop remains separate and clickable without overlapping Fit.
    nativeStop.style.display = (vp.nativeZoom && !combinedFitNative) ? 'inline-block' : 'none';
    nativeStop.style.left = Math.max(8, nativePosition) + '%';
  }
  if (maxLabel) {
    maxLabel.textContent = vp.nativeZoom ? '400%' : '4\u00d7 Fit';
    maxLabel.disabled = !!vp.options.photo().transitionPending;
  }
}

export function setPopoverOpen(/** @type {LightboxViewportContext} */ vp, /** @type {boolean} */ open) {
  var popover = vp.document.getElementById('lightboxZoomPopover');
  var badge = vp.document.getElementById('lightboxZoomBadge');
  if (popover) popover.classList.toggle('open', !!open);
  if (badge) badge.setAttribute('aria-expanded', open ? 'true' : 'false');
}

export function togglePopover(/** @type {LightboxViewportContext} */ vp) {
  var popover = vp.document.getElementById('lightboxZoomPopover');
  setPopoverOpen(vp, !(popover && popover.classList.contains('open')));
}
