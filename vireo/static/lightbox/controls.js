function toggleLightboxZoomPopover(value) { return vireoLightboxViewport.togglePopover(value); }
function setLightboxZoomFromSlider(value) { return vireoLightboxViewport.setFromSlider(value); }
function stepLightboxZoom(value) { return vireoLightboxViewport.stepZoom(value); }
function setLightboxZoomToFit(value) { return vireoLightboxViewport.fit(value); }
function setLightboxZoomToOneToOne(value) { return vireoLightboxViewport.oneToOne(value); }
function toggleLightboxZoom(value) { return vireoLightboxViewport.toggleZoom(value); }

function closeLightbox(e) {
  if (e && e.target && e.target.tagName === 'IMG') return;
  _lbFlushPendingAdjustmentSave();
  if (vireoLightboxSession.requestedPhotoId() != null) vireoLightboxViewport.save(vireoLightboxSession.requestedPhotoId());
  var wasOpen = !!window._lbEscToken;
  if (wasOpen) { Keymap.popEsc(window._lbEscToken); window._lbEscToken = null; }
  vireoLightboxViewport.close();
  _lbVisualTransitionPending = false;
  _lbSetPhotoTransitionPending(false);
  _lbDeferredOverlayApply = null;
  _lbDesiredSrcKey = null;
  _lbInitialDecodePending = false;
  _lbResetDetailStatus();
  _lbClearAdjustmentPreview();
  var adjustPanel = document.getElementById('lightboxAdjustPanel');
  if (adjustPanel) adjustPanel.classList.remove('open');
  var adjustBtn = document.getElementById('lightboxAdjustBtn');
  if (adjustBtn) adjustBtn.setAttribute('aria-expanded', 'false');
  toggleLightboxViewMenu(false);
  vireoLightboxViewport.applyTransform();
  document.getElementById('lightboxOverlay').classList.remove('active');
  // The session returns the bitmap's identity, even if navigation was pending.
  var closedPhotoId = vireoLightboxSession.close();
  document.getElementById('lightboxImg').src = '';
  document.getElementById('lightboxImg').onload = null;
  document.getElementById('lightboxImg').onerror = null;
  _lbReadOnly = false;
  _lbReadOnlyMessage = 'This lightbox is read-only';
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
 * sits in _navbar.html alongside the overlay. Rating / color / flag / reveal
 * helpers (`setRatingFor`, `setColorLabelFor`, `setFlagFor`, `findSimilar`,
 * `openInEditor`, `openPhotoExportModal`, `revealPhoto`, `copyPhotoPaths`) are
 * defined on pages that carry a photo grid (browse.html). On /review, there
 * is no selection model
 * but per-photo `setReviewRating` / `setReviewFlag` helpers exist and the
 * rating/flag chips fall through to them. Pages without a page-local flag
 * helper use the shared photo flag endpoint. Color labels are not part of the
 * review workflow, so the color row is omitted when `setColorLabelFor` is
 * absent.
 */
function buildLightboxContextMenu(pid) {
  var has = function(name) { return typeof window[name] === 'function'; };
  var readOnlyHint = _lbReadOnly ? _lbReadOnlyMessage : null;

  var rateChip = function(n) {
    return {
      label: n === 0 ? '\u2606' : String(n),
      title: n === 0 ? 'No rating' : 'Rate ' + n,
      disabled: !!readOnlyHint,
      disabledHint: readOnlyHint || undefined,
      onClick: function() {
        if (has('setRatingFor')) window.setRatingFor(pid, n);
        else if (has('setReviewRating')) window.setReviewRating(pid, n);
      },
    };
  };
  var colorChip = function(c, icon, title) {
    return {
      label: icon,
      title: c && window.VireoColorLabels
        ? window.VireoColorLabels.title(c, title)
        : title,
      color: c,
      colorBaseTitle: title,
      disabled: !!readOnlyHint,
      disabledHint: readOnlyHint || undefined,
      onClick: function() { if (has('setColorLabelFor')) window.setColorLabelFor(pid, c); },
    };
  };
  var flagChip = function(f, icon, title) {
    return {
      label: icon, title: title,
      disabled: !!readOnlyHint,
      disabledHint: readOnlyHint || undefined,
      onClick: function() {
        _lbApplyFlag(pid, f);
      },
    };
  };
  var toggleItem = function(label, visible, onClick, disabledHint) {
    return {
      label: label + ': ' + (visible ? 'On' : 'Off'),
      disabled: !!disabledHint,
      disabledHint: disabledHint || undefined,
      onClick: onClick,
    };
  };

  var items = [
    { chips: [0, 1, 2, 3, 4, 5].map(rateChip) },
  ];
  if (has('setColorLabelFor')) {
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
  items.push(toggleItem('Detection boxes', _lbBoxesVisible, toggleLightboxBoxes));
  items.push(toggleItem('Mask overlay', _lbMasksVisible, toggleLightboxMasks));
  items.push(toggleItem('Eye marker', _lbEyeVisible, toggleLightboxEye));
  items.push(toggleItem('Track eye', _lbTrackEyeEnabled, toggleLightboxTrackEye));
  items.push(toggleItem('Filename and counter', _lbInfoVisible, toggleLightboxInfo));
  items.push(toggleItem('Lightbox controls', _lbChromeVisible, toggleLightboxChrome));
  items.push(toggleItem('Wildlife classification', !_lbCurrentWildlifeExcluded,
    lightboxToggleWildlifeExcluded, readOnlyHint));
  items.push({ separator: true });
  var editButton = document.getElementById('lightboxEditPhoto');
  if (editButton) {
    items.push({ label: 'Edit Photo', disabled: editButton.disabled,
      disabledHint: editButton.disabled ? editButton.title : undefined,
      onClick: function() { editButton.click(); } });
  }
  var browseDisabledHint = typeof window.getLightboxBrowseDisabledHint === 'function'
    ? window.getLightboxBrowseDisabledHint(pid)
    : null;
  items.push({ label: 'Open in Browse', disabled: !!browseDisabledHint,
    disabledHint: browseDisabledHint || undefined,
    onClick: function() {
      window.openInBrowse(pid);
    } });
  if (!_lbReadOnly && typeof window.buildSpeciesHighlightMenuItems === 'function') {
    items = items.concat(window.buildSpeciesHighlightMenuItems([pid], {
      photoById: _lbPhotoDataByPhoto,
      showFetchFallback: true,
    }));
  }
  if (!_lbReadOnly && typeof window.buildSpeciesRepresentativeMenuItems === 'function') {
    items = items.concat(window.buildSpeciesRepresentativeMenuItems([pid], {
      photoById: _lbPhotoDataByPhoto,
    }));
  }

  if (has('findSimilar')) {
    items.push({ label: 'Find Similar',
      onClick: function() { window.findSimilar(pid); } });
  }
  if (has('openInEditor')) {
    items = items.concat(window.buildOpenInEditorMenuItems([pid]));
  }
  if (has('openPhotoExportModal')) {
    items.push({ label: 'Export\u2026',
      onClick: function() {
        // The lightbox can navigate while its context menu remains open.
        // Resolve the visible identity now instead of exporting the photo
        // that happened to be current when the menu was built.
        var exportPid = vireoLightboxSession.displayedPhotoId() != null
          ? vireoLightboxSession.displayedPhotoId()
          : vireoLightboxSession.requestedPhotoId();
        if (exportPid != null) window.openPhotoExportModal([exportPid]);
      } });
  }
  items.push({ label: window.VIREO_REVEAL_LABEL,
    onClick: function() { revealLightboxPhoto(pid); } });
  if (has('copyPhotoPaths')) {
    items.push({ label: 'Copy Path',
      onClick: function() { window.copyPhotoPaths([pid]); } });
  }

  items.push({ separator: true });
  items.push({ label: 'Close Lightbox',
    onClick: function() { closeLightbox(); } });

  return items;
}

function revealLightboxPhoto(pid) {
  // Browse and Review each expose their own reveal helpers. Other pages can
  // also open the shared lightbox, so keep a direct API fallback here rather
  // than hiding the action based on which page happened to launch it.
  if (typeof window.revealPhoto === 'function') {
    window.revealPhoto(pid);
    return;
  }
  if (typeof window.revealReviewPhoto === 'function') {
    window.revealReviewPhoto(pid);
    return;
  }
  safeFetch('/api/files/reveal', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({photo_id: pid}),
  }, { toast: false }).then(function(data) {
    if (typeof showRevealFeedback === 'function') showRevealFeedback(data);
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
    if (_lbVisualTransitionPending) return;
    var pid = vireoLightboxSession.requestedPhotoId();
    if (!pid) return;
    openContextMenu(e, buildLightboxContextMenu(pid));
  });
})();
