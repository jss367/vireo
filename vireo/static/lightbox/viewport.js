/* The lightbox viewport controller: zoom, pan, saved views and the interaction
 * lifecycle. Constructing one is inert; beginPhoto attaches listeners, and close
 * retires drag and resize work. Its state is one record that never leaves this
 * controller: the viewport-*.js modules hold the functions that read and write
 * it, one module per concern, and callers reach them only through the frozen
 * methods returned here.
 */
import {
  applyTransform, isOneToOne, layoutDims, layoutMetrics, recomputeNativeZoom,
} from './viewport-geometry.js';
import {
  beginPhoto, cancelPendingZoom, clearEyeTracking, close, flushDeferredLayout, resetForEdit,
} from './viewport-interaction.js';
import {
  applyPendingEye, applyPendingRestore, applyView, cancelRestore, captureEyeAnchor, currentView,
  saveView,
} from './viewport-views.js';
import {
  applyPendingOneToOne, deferOneToOne, deferOneToOneFallback, setFromSlider, setZoom, stepZoom,
  toggleZoom, zoomToFit, zoomToOneToOne,
} from './viewport-zoom.js';
import { setPopoverOpen, togglePopover, updateZoomControl } from './viewport-zoom-control.js';

/** @returns {LightboxViewportContext} */
function initialState(/** @type {LightboxViewportOptions} */ options) {
  var window = options.window || globalThis.window;
  return {
    window: window,
    document: window.document,
    options: options,
    zoom: 1.0,                    // current zoom (1.0 = fit)
    panX: 0,                      // pan translation in CSS pixels
    panY: 0,
    nativeZoom: null,             // zoom value corresponding to 1:1 for the current photo
    fitScale: 1.0,                // natural image scale at zoom=1.0
    pendingOneToOne: false,       // z/click was pressed with unknown nativeZoom; upgrade to true 1:1 once learned
    pendingOneToOneAnchor: null,  // optional client-space anchor for a deferred 1:1 snap
    savedViews: {},               // per-session view cache keyed by photo id
    pendingRestore: null,         // the view to apply once the incoming image is laid out
    pendingEye: null,             // destination alignment waiting for image metadata/layout
    eyeAnchor: null,              // eye offset from viewport center in CSS pixels
    active: false,                // listeners attached
    resizeTimer: null,
    refreshSourceTier: false,
    deferredRefreshPending: false,
    layoutObserver: null,
    dragging: false,
    didDrag: false,
    dragStartX: 0,
    dragStartY: 0,
    panStartX: 0,
    panStartY: 0,
    listeners: null,
  };
}

/** @template T @param {T | null | undefined} value */
function frozenCopy(value) { return value ? Object.freeze(Object.assign({}, value)) : null; }

function snapshot(/** @type {LightboxViewportContext} */ vp) {
  return Object.freeze({
    zoom: vp.zoom, panX: vp.panX, panY: vp.panY, nativeZoom: vp.nativeZoom, fitScale: vp.fitScale,
    pendingOneToOne: vp.pendingOneToOne, oneToOneAnchor: frozenCopy(vp.pendingOneToOneAnchor),
    pendingRestore: frozenCopy(vp.pendingRestore), pendingEye: frozenCopy(vp.pendingEye),
    eyeAnchor: frozenCopy(vp.eyeAnchor), active: vp.active
  });
}

/** @returns {LightboxViewport} */
function create(/** @type {LightboxViewportOptions} */ options) {
  var vp = initialState(options);
  return Object.freeze({
    beginPhoto: beginPhoto.bind(null, vp),
    close: close.bind(null, vp),
    snapshot: snapshot.bind(null, vp),
    resetForEdit: resetForEdit.bind(null, vp),
    cancelPendingZoom: cancelPendingZoom.bind(null, vp),
    clearEyeTracking: clearEyeTracking.bind(null, vp),
    flushDeferredLayout: flushDeferredLayout.bind(null, vp),
    invalidateGeometry: function() { vp.nativeZoom = null; },
    zoom: function() { return vp.zoom; },
    nativeZoom: function() { return vp.nativeZoom; },
    fitScale: function() { return vp.fitScale; },
    pendingOneToOne: function() { return vp.pendingOneToOne; },
    hasPendingRestore: function() { return vp.pendingRestore !== null; },
    savedView: function(/** @type {number} */ id) { return frozenCopy(vp.savedViews[String(id)]); },
    applyTransform: applyTransform.bind(null, vp),
    layoutDims: layoutDims.bind(null, vp),
    layoutMetrics: layoutMetrics.bind(null, vp),
    recomputeNativeZoom: recomputeNativeZoom.bind(null, vp),
    setZoom: setZoom.bind(null, vp),
    isOneToOne: isOneToOne.bind(null, vp),
    currentView: currentView.bind(null, vp),
    captureEyeAnchor: captureEyeAnchor.bind(null, vp),
    applyPendingEye: applyPendingEye.bind(null, vp),
    save: saveView.bind(null, vp),
    applyView: applyView.bind(null, vp),
    cancelRestore: cancelRestore.bind(null, vp),
    applyPendingRestore: applyPendingRestore.bind(null, vp),
    applyPendingOneToOne: applyPendingOneToOne.bind(null, vp),
    deferOneToOne: deferOneToOne.bind(null, vp),
    deferOneToOneFallback: deferOneToOneFallback.bind(null, vp),
    updateControls: updateZoomControl.bind(null, vp),
    setPopoverOpen: setPopoverOpen.bind(null, vp),
    togglePopover: togglePopover.bind(null, vp),
    setFromSlider: setFromSlider.bind(null, vp),
    stepZoom: stepZoom.bind(null, vp),
    fit: zoomToFit.bind(null, vp),
    oneToOne: zoomToOneToOne.bind(null, vp),
    toggleZoom: toggleZoom.bind(null, vp),
  });
}
export const VireoLightboxViewport = Object.freeze({create: create});
