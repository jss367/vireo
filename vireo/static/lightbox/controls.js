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
import {
  buildOpenInEditorMenuItems, openInEditor, safeFetch, showRevealFeedback, showToast,
} from '../navbar-shared-helpers.js';

export function toggleLightboxZoomPopover() { return vireoLightboxViewport.togglePopover(); }
export function setLightboxZoomFromSlider(/** @type {number | string} */ value) { return vireoLightboxViewport.setFromSlider(value); }
export function stepLightboxZoom(/** @type {number} */ value) { return vireoLightboxViewport.stepZoom(value); }
export function setLightboxZoomToFit() { return vireoLightboxViewport.fit(); }
export function setLightboxZoomToOneToOne(/** @type {Pick<MouseEvent, "clientX" | "clientY"> | undefined} */ value = undefined) { return vireoLightboxViewport.oneToOne(value); }
export function toggleLightboxZoom(/** @type {Pick<MouseEvent, "clientX" | "clientY">} */ value) { return vireoLightboxViewport.toggleZoom(value); }

export function closeLightbox(/** @type {Event | undefined} */ e = undefined) {
  if (e && e.target instanceof Element && e.target.tagName === 'IMG') return;
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

  var rateChip = function(/** @type {number} */ n) {
    return {
      label: n === 0 ? '\u2606' : String(n),
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
  var toggleItem = function(/** @type {string} */ label, /** @type {boolean} */ visible, /** @type {() => unknown} */ onClick, /** @type {string | null | undefined} */ disabledHint = undefined) {
    return {
      label: label + ': ' + (visible ? 'On' : 'Off'),
      disabled: !!disabledHint,
      disabledHint: disabledHint || undefined,
      onClick: onClick,
    };
  };

  /** @type {LightboxMenuItem[]} */ var items = [
    { chips: [0, 1, 2, 3, 4, 5].map(rateChip) },
  ];
  if (Vireo.pageHook('setColorLabelFor')) {
    items.push({ chips: [
      colorChip(null, '\u25CB', 'No color'),
      colorChip('red', '\u25CF', 'Red'),
      colorChip('yellow', '\u25CF', 'Yellow'),
      colorChip('green', '\u25CF', 'Green'),
      colorChip('blue', '\u25CF', 'Blue'),
      colorChip('purple', '\u25CF', 'Purple'),
    ] });
  }
  items.push({ chips: [
    flagChip('flagged', '\u2691', 'Flag as pick'),
    flagChip('rejected', '\u2715', 'Reject'),
    flagChip('none', '\u25CB', 'Unflag'),
  ] });
  items.push({ separator: true });
  items.push(toggleItem('Detection boxes', lbOverlays.boxesVisible, toggleLightboxBoxes));
  items.push(toggleItem('Mask overlay', lbOverlays.masksVisible, toggleLightboxMasks));
  items.push(toggleItem('Eye marker', lbOverlays.eyeVisible, toggleLightboxEye));
  items.push(toggleItem('Track eye', lbOverlays.trackEyeEnabled, toggleLightboxTrackEye));
  items.push(toggleItem('Filename and counter', lbOverlays.infoVisible, toggleLightboxInfo));
  items.push(toggleItem('Lightbox controls', lbOverlays.chromeVisible, toggleLightboxChrome));
  items.push(toggleItem('Wildlife classification', !lbState.currentWildlifeExcluded,
    lightboxToggleWildlifeExcluded, readOnlyHint));
  items.push({ separator: true });
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

  if (typeof findSimilar === 'function') {
    items.push({ label: 'Find Similar',
      onClick: function() { findSimilar(pid); } });
  }
  if (typeof openInEditor === 'function') {
    items = items.concat(buildOpenInEditorMenuItems([pid]));
  }
  if (Vireo.pageHook('openPhotoExportModal')) {
    items.push({ label: 'Export\u2026',
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

  items.push({ separator: true });
  items.push({ label: 'Close Lightbox',
    onClick: function() { closeLightbox(); } });

  return items;
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

(function() {
  var img = document.getElementById('lightboxImg');
  if (!img) return;
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
