import { lbState } from './shared-state.js';
import { openContextMenu, openInBrowse } from '../navbar-report-context-menu.js';
import {
  _lbApplyReadOnlyState, _lbPhotoDataByPhoto, _lbSetPhotoTransitionPending,
  vireoLightboxSession, vireoLightboxViewport,
} from './state.js';
import { buildSpeciesHighlightMenuItems, buildSpeciesRepresentativeMenuItems } from './curation.js';
import { _lbClearAdjustmentPreview, _lbFlushPendingAdjustmentSave } from './edits.js';
import { _lbApplyFlag, _lbSetFlagStatus } from './flags.js';
import {
  _lbResetMaskOverlay, lbOverlays, toggleLightboxBoxes, toggleLightboxChrome, toggleLightboxEye,
  toggleLightboxInfo, toggleLightboxMasks, toggleLightboxTrackEye, toggleLightboxViewMenu,
} from './overlays.js';
import { lightboxToggleWildlifeExcluded } from './delete.js';
import { _lbResetDetailStatus } from './source-loading.js';
import { findSimilar } from '../navbar-similar-photos.js';
import { openPipeline } from '../navbar-pipeline-inspector.js';
import {
  buildOpenInEditorMenuItems, openInEditor, restoreOverlayFocus, safeFetch, showRevealFeedback, showToast,
} from '../navbar-shared-helpers.js';

export function toggleLightboxZoomPopover() { return vireoLightboxViewport.togglePopover(); }
export function setLightboxZoomFromSlider(/** @type {number | string} */ value) { return vireoLightboxViewport.setFromSlider(value); }
export function stepLightboxZoom(/** @type {number} */ value) { return vireoLightboxViewport.stepZoom(value); }
export function setLightboxZoomToFit() { return vireoLightboxViewport.fit(); }
export function setLightboxZoomToOneToOne(/** @type {Pick<MouseEvent, "clientX" | "clientY"> | undefined} */ value = undefined) { return vireoLightboxViewport.oneToOne(value); }
export function toggleLightboxZoom(/** @type {Pick<MouseEvent, "clientX" | "clientY">} */ value) { return vireoLightboxViewport.toggleZoom(value); }

export function beginLightboxInteraction() {
  lbState.returnFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;
  Keymap.lockBodyScroll();
}

export function closeLightbox(/** @type {Event | undefined} */ e = undefined) {
  if (e && e.target instanceof Element && e.target.tagName === 'IMG') return;
  var ownedFocus = document.getElementById('lightboxOverlay')?.contains(document.activeElement);
  var returnFocus = lbState.returnFocus;
  lbState.returnFocus = null;
  _lbFlushPendingAdjustmentSave();
  if (vireoLightboxSession.requestedPhotoId() != null) vireoLightboxViewport.save(vireoLightboxSession.requestedPhotoId());
  var wasOpen = !!lbState.escToken;
  if (wasOpen) { Keymap.popEsc(lbState.escToken); lbState.escToken = null; }
  // The wrap's click-to-zoom latch (set by a mouseup that zoomed out) must not
  // carry into the next session, or it would swallow its first click-to-zoom.
  lbState.zoomHandled = false;
  vireoLightboxViewport.close();
  lbState.visualTransitionPending = false;
  _lbSetPhotoTransitionPending(false);
  lbState.deferredOverlayApply = null;
  lbState.desiredSrcKey = null;
  lbState.initialDecodePending = false;
  _lbResetDetailStatus();
  _lbClearAdjustmentPreview();
  var adjustPanel = document.getElementById('lightboxAdjustPanel');
  if (adjustPanel) adjustPanel.classList.remove('open');
  var adjustBtn = document.getElementById('lightboxAdjustBtn');
  if (adjustBtn) adjustBtn.setAttribute('aria-expanded', 'false');
  toggleLightboxViewMenu(false);
  vireoLightboxViewport.applyTransform();
  document.getElementById('lightboxOverlay')?.classList.remove('active');
  // The session returns the bitmap's identity, even if navigation was pending.
  var closedPhotoId = vireoLightboxSession.close();
  var image = /** @type {HTMLImageElement | null} */ (document.getElementById('lightboxImg'));
  if (image) { image.src = ''; image.onload = null; image.onerror = null; }
  lbState.readOnly = false;
  lbState.readOnlyMessage = 'This lightbox is read-only';
  _lbApplyReadOnlyState();
  try {
    document.dispatchEvent(new CustomEvent('lightbox:closed', {
      detail: { photoId: closedPhotoId }
    }));
  } catch(_) {}
  _lbSetFlagStatus(undefined);
  var detContainer = document.getElementById('lightboxDetections');
  if (detContainer) detContainer.innerHTML = '';
  // Drop the mask overlay too — otherwise reopening on a photo with
  // no masks would leave the previous photo's mask painted across it.
  _lbResetMaskOverlay();
  if (wasOpen) Keymap.unlockBodyScroll();
  if (wasOpen && ownedFocus) restoreOverlayFocus(returnFocus);
}

/* ---------- Lightbox right-click context menu ----------
 * The lightbox is shared across pages (browse, review, etc.), so the menu
 * sits in _navbar.html alongside the overlay. Pages that carry a photo grid
 * (Browse) register rating / color / export / reveal / copy-path page hooks
 * (`setRatingFor`, `setColorLabelFor`, `openPhotoExportModal`, `revealPhoto`,
 * `copyPhotoPaths`; see vireo-page-hooks.js). On /review, there is no
 * selection model but per-photo `setReviewRating` / `setReviewFlag` hooks
 * exist and the rating/flag chips fall through to them. Pages without a
 * flag hook use the shared photo flag endpoint. Color labels are not part of
 * the review workflow, so the color row is omitted when `setColorLabelFor` is
 * not registered.
 */
export function buildLightboxContextMenu(/** @type {number} */ pid) {
  var readOnlyHint = lbState.readOnly ? lbState.readOnlyMessage : null;
  /** @type {LightboxMenuItem[]} */ var items = [];
  return items.concat(
    curationMenuItems(pid, readOnlyHint),
    [{ separator: true }],
    viewToggleMenuItems(readOnlyHint),
    [{ separator: true }],
    photoDestinationMenuItems(pid),
    photoToolMenuItems(),
    photoFileMenuItems(pid),
    [{ separator: true }, { label: 'Close Lightbox', onClick: function() { closeLightbox(); } }]
  );
}

/**
 * The rating, color and flag chip rows.
 * @param {number} pid @param {string | null} readOnlyHint @returns {LightboxMenuItem[]}
 */
function curationMenuItems(pid, readOnlyHint) {
  var rateChip = function(/** @type {number} */ n) {
    return {
      label: n === 0 ? '☆' : String(n),
      title: n === 0 ? 'No rating' : 'Rate ' + n,
      disabled: !!readOnlyHint,
      disabledHint: readOnlyHint || undefined,
      onClick: function() {
        const setRating = Vireo.pageHook('setRatingFor') || Vireo.pageHook('setReviewRating');
        if (setRating) setRating(pid, n);
      },
    };
  };
  var colorChip = function(/** @type {string | null} */ c, /** @type {string} */ icon, /** @type {string} */ title) {
    return {
      label: icon,
      title: c && window.VireoColorLabels
        ? window.VireoColorLabels.title(c, title)
        : title,
      color: c,
      colorBaseTitle: title,
      disabled: !!readOnlyHint,
      disabledHint: readOnlyHint || undefined,
      onClick: function() { const hook = Vireo.pageHook('setColorLabelFor'); if (hook) hook(pid, c); },
    };
  };
  var flagChip = function(/** @type {LightboxFlag} */ f, /** @type {string} */ icon, /** @type {string} */ title) {
    return {
      label: icon, title: title,
      disabled: !!readOnlyHint,
      disabledHint: readOnlyHint || undefined,
      onClick: function() {
        _lbApplyFlag(pid, f);
      },
    };
  };

  /** @type {LightboxMenuItem[]} */ var items = [
    { chips: [0, 1, 2, 3, 4, 5].map(rateChip) },
  ];
  if (Vireo.pageHook('setColorLabelFor')) {
    items.push({ chips: [
      colorChip(null, '○', 'No color'),
      colorChip('red', '●', 'Red'),
      colorChip('yellow', '●', 'Yellow'),
      colorChip('green', '●', 'Green'),
      colorChip('blue', '●', 'Blue'),
      colorChip('purple', '●', 'Purple'),
    ] });
  }
  items.push({ chips: [
    flagChip('flagged', '⚑', 'Flag as pick'),
    flagChip('rejected', '✕', 'Reject'),
    flagChip('none', '○', 'Unflag'),
  ] });
  return items;
}

/**
 * The View toggles, each labeled with its current state.
 * @param {string | null} readOnlyHint @returns {LightboxMenuItem[]}
 */
function viewToggleMenuItems(readOnlyHint) {
  var toggleItem = function(/** @type {string} */ label, /** @type {boolean} */ visible, /** @type {() => unknown} */ onClick, /** @type {string | null | undefined} */ disabledHint = undefined) {
    return {
      label: label + ': ' + (visible ? 'On' : 'Off'),
      disabled: !!disabledHint,
      disabledHint: disabledHint || undefined,
      onClick: onClick,
    };
  };
  return [
    toggleItem('Detection boxes', lbOverlays.boxesVisible, toggleLightboxBoxes),
    toggleItem('Mask overlay', lbOverlays.masksVisible, toggleLightboxMasks),
    toggleItem('Eye marker', lbOverlays.eyeVisible, toggleLightboxEye),
    toggleItem('Track eye', lbOverlays.trackEyeEnabled, toggleLightboxTrackEye),
    toggleItem('Filename and counter', lbOverlays.infoVisible, toggleLightboxInfo),
    toggleItem('Lightbox controls', lbOverlays.chromeVisible, toggleLightboxChrome),
    toggleItem('Wildlife classification', !lbState.currentWildlifeExcluded,
      lightboxToggleWildlifeExcluded, readOnlyHint),
  ];
}

/**
 * Where the photo can be taken from here: the editor, Browse, and the species
 * highlight and representative choices.
 * @param {number} pid @returns {LightboxMenuItem[]}
 */
function photoDestinationMenuItems(pid) {
  /** @type {LightboxMenuItem[]} */ var items = [];
  const editButton = /** @type {HTMLButtonElement | null} */ (document.getElementById('lightboxEditPhoto'));
  if (editButton) {
    items.push({ label: 'Edit Photo', disabled: editButton.disabled,
      disabledHint: editButton.disabled ? editButton.title : undefined,
      onClick: function() { editButton.click(); } });
  }
  var browseDisabledHint = Vireo.pageHook('getLightboxBrowseDisabledHint')?.(pid) ?? null;
  items.push({ label: 'Open in Browse', disabled: !!browseDisabledHint,
    disabledHint: browseDisabledHint || undefined,
    onClick: function() {
      openInBrowse(pid);
    } });
  if (!lbState.readOnly && typeof buildSpeciesHighlightMenuItems === 'function') {
    items = items.concat(buildSpeciesHighlightMenuItems([pid], {
      photoById: _lbPhotoDataByPhoto,
      showFetchFallback: true,
    }));
  }
  if (!lbState.readOnly && typeof buildSpeciesRepresentativeMenuItems === 'function') {
    items = items.concat(buildSpeciesRepresentativeMenuItems([pid], {
      photoById: _lbPhotoDataByPhoto,
    }));
  }
  return items;
}

/**
 * The photo's file: external editors, export, reveal and its path.
 * @param {number} pid @returns {LightboxMenuItem[]}
 */
function photoFileMenuItems(pid) {
  /** @type {LightboxMenuItem[]} */ var items = [];
  if (typeof openInEditor === 'function') {
    items = items.concat(buildOpenInEditorMenuItems([pid]));
  }
  if (Vireo.pageHook('openPhotoExportModal')) {
    items.push({ label: 'Export…',
      onClick: function() {
        // The lightbox can navigate while its context menu remains open.
        // Resolve the visible identity now instead of exporting the photo
        // that happened to be current when the menu was built.
        var exportPid = vireoLightboxSession.displayedPhotoId() != null
          ? vireoLightboxSession.displayedPhotoId()
          : vireoLightboxSession.requestedPhotoId();
        const hook = Vireo.pageHook('openPhotoExportModal');
        if (exportPid != null && hook) hook([exportPid]);
      } });
  }
  items.push({ label: window.VIREO_REVEAL_LABEL,
    onClick: function() { revealLightboxPhoto(pid); } });
  if (Vireo.pageHook('copyPhotoPaths')) {
    items.push({ label: 'Copy Path',
      onClick: function() { const hook = Vireo.pageHook('copyPhotoPaths'); if (hook) hook([pid]); } });
  }
  return items;
}

/**
 * Find Similar and Inspect Pipeline open overlays that sit below the
 * lightbox (z-index 9997/9998 vs 9999), so each closes the lightbox first.
 * @returns {LightboxMenuItem[]}
 */
function photoToolMenuItems() {
  var openTool = function(/** @type {(photoId: number) => unknown} */ tool, /** @type {string} */ overlayId) {
    // Navigation can change the visible photo while its context menu stays open.
    // Resolve its identity before closeLightbox clears the session.
    var pid = vireoLightboxSession.displayedPhotoId() ?? vireoLightboxSession.requestedPhotoId();
    if (pid == null) return;
    closeLightbox();
    tool(pid);
    document.getElementById(overlayId)?.querySelector('button')?.focus();
  };
  return [
    { label: 'Find Similar', onClick: function() { openTool(findSimilar, 'similarOverlay'); } },
    { label: 'Inspect Pipeline', onClick: function() { openTool(openPipeline, 'pipelineOverlay'); } },
  ];
}

function revealLightboxPhoto(/** @type {number} */ pid) {
  // Browse and Review each register their own reveal hooks. Other pages can
  // also open the shared lightbox, so keep a direct API fallback here rather
  // than hiding the action based on which page happened to launch it.
  var revealPhoto = Vireo.pageHook('revealPhoto') || Vireo.pageHook('revealReviewPhoto');
  if (revealPhoto) {
    revealPhoto(pid);
    return;
  }
  safeFetch('/api/files/reveal', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({photo_id: pid}),
  }, { toast: false }).then(function(data) {
    if (typeof showRevealFeedback === 'function') showRevealFeedback(/** @type {NavRevealResult} */ (data));
  }).catch(function(err) {
    if (typeof showToast === 'function') {
      showToast('Reveal failed: ' + (err.message || 'request failed'), 'error');
    } else {
      console.error('revealLightboxPhoto failed', err);
    }
  });
}

/** @param {KeyboardEvent} e */
function openLightboxKeyboardMenu(e) {
  if (e.key !== 'ContextMenu' && !(e.shiftKey && e.key === 'F10')) return;
  e.preventDefault();
  e.stopPropagation();
  var img = document.getElementById('lightboxImg');
  if (!img) return;
  var rect = img.getBoundingClientRect();
  var wrap = document.getElementById('lightboxWrap')?.getBoundingClientRect();
  // A zoomed, panned image can have its midpoint outside the viewport.
  var left = Math.max(rect.left, wrap?.left ?? 0, 0);
  var right = Math.min(rect.right, wrap?.right ?? window.innerWidth, window.innerWidth);
  var top = Math.max(rect.top, wrap?.top ?? 0, 0);
  var bottom = Math.min(rect.bottom, wrap?.bottom ?? window.innerHeight, window.innerHeight);
  if (right <= left || bottom <= top) return;
  img.dispatchEvent(new MouseEvent('contextmenu', {
    bubbles: true, cancelable: true,
    clientX: (left + right) / 2, clientY: (top + bottom) / 2,
  }));
  /** @type {HTMLElement | null} */ (document.querySelector(
    '.vireo-ctx-menu [role="menuitem"]:not([aria-disabled="true"])'))?.focus();
}

(function() {
  var img = document.getElementById('lightboxImg');
  if (!img) return;
  img.addEventListener('keydown', openLightboxKeyboardMenu);
  img.addEventListener('contextmenu', function(e) {
    // Stop the event before the browser's native menu or the wrap's
    // mousedown pan handler can react. (contextmenu fires after mousedown
    // in Chromium; stopPropagation + preventDefault together keep our
    // handler in full control.)
    e.preventDefault();
    e.stopPropagation();
    if (lbState.visualTransitionPending) return;
    var pid = vireoLightboxSession.requestedPhotoId();
    if (!pid) return;
    openContextMenu(e, buildLightboxContextMenu(pid));
  });
})();
