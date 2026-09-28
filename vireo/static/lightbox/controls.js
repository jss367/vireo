
function _lbMaxZoom() {
  return _lbNativeZoom ? _lbNativeZoom * 4 : 4;
}

function _lbZoomSliderPosition(zoom) {
  var maxZoom = _lbMaxZoom();
  if (maxZoom <= 1.001) return 0;
  return Math.max(0, Math.min(1000, Math.log(Math.max(1, zoom)) / Math.log(maxZoom) * 1000));
}

function _lbZoomFromSliderPosition(position) {
  var maxZoom = _lbMaxZoom();
  if (maxZoom <= 1.001) return 1;
  var ratio = Math.max(0, Math.min(1, Number(position) / 1000));
  return Math.exp(Math.log(maxZoom) * ratio);
}

function _lbZoomDisplayText() {
  if (_lbPending1To1) return 'Loading 1:1';
  if (_lbZoom <= 1.001) return 'Fit';
  if (_lbNativeZoom) return Math.round((_lbZoom / _lbNativeZoom) * 100) + '%';
  var fitMultiple = Math.round(_lbZoom * 10) / 10;
  return fitMultiple + '\u00d7 Fit';
}

function _lbNativeSliderPercent() {
  return _lbNativeZoom ? _lbZoomSliderPosition(_lbNativeZoom) / 10 : 0;
}

function _lbNativeCoincidesWithFit() {
  return !!_lbNativeZoom && _lbNativeZoom <= 1.001;
}

function _lbUpdateZoomControl() {
  var badge = document.getElementById('lightboxZoomBadge');
  var slider = document.getElementById('lightboxZoomSlider');
  var nativeStop = document.getElementById('lightboxZoomNativeStop');
  var fitStop = document.querySelector('.lb-zoom-stop-fit');
  var maxLabel = document.getElementById('lightboxZoomMaxLabel');
  var zoomOut = document.getElementById('lightboxZoomOut');
  var zoomIn = document.getElementById('lightboxZoomIn');
  var text = _lbZoomDisplayText();

  if (badge) {
    badge.textContent = text;
    badge.title = 'Zoom: ' + text + '. Open zoom controls';
  }
  if (slider) {
    slider.value = String(Math.round(_lbZoomSliderPosition(_lbZoom)));
    slider.setAttribute('aria-valuetext', text);
    slider.disabled = !!_lbVisualTransitionPending;
  }
  if (zoomOut) zoomOut.disabled = _lbVisualTransitionPending || _lbZoom <= 1.001;
  if (zoomIn) zoomIn.disabled = _lbVisualTransitionPending || _lbZoom >= _lbMaxZoom() - 0.001;

  var combinedFitNative = _lbNativeCoincidesWithFit();
  if (fitStop) {
    fitStop.textContent = combinedFitNative ? 'Fit \u00b7 1:1' : 'Fit';
    fitStop.disabled = !!_lbVisualTransitionPending;
  }
  if (nativeStop) {
    var nativePosition = _lbNativeSliderPercent();
    nativeStop.disabled = !!_lbVisualTransitionPending;
    // When fit and 1:1 are visually indistinguishable, combine the labels at the
    // left edge. Otherwise keep 1:1 at least 8% along the track so a near-fit
    // native stop remains separate and clickable without overlapping Fit.
    nativeStop.style.display = (_lbNativeZoom && !combinedFitNative) ? 'inline-block' : 'none';
    nativeStop.style.left = Math.max(8, nativePosition) + '%';
  }
  if (maxLabel) {
    maxLabel.textContent = _lbNativeZoom ? '400%' : '4\u00d7 Fit';
    maxLabel.disabled = !!_lbVisualTransitionPending;
  }
}

function _lbSetZoomPopoverOpen(open) {
  var popover = document.getElementById('lightboxZoomPopover');
  var badge = document.getElementById('lightboxZoomBadge');
  if (popover) popover.classList.toggle('open', !!open);
  if (badge) badge.setAttribute('aria-expanded', open ? 'true' : 'false');
}

function toggleLightboxZoomPopover() {
  var popover = document.getElementById('lightboxZoomPopover');
  _lbSetZoomPopoverOpen(!(popover && popover.classList.contains('open')));
}

function setLightboxZoomFromSlider(position) {
  if (_lbVisualTransitionPending) return;
  _lbClearPendingViewportRestore();
  _lbSetZoom(_lbZoomFromSliderPosition(position), null, null);
}

function stepLightboxZoom(direction) {
  if (_lbVisualTransitionPending) return;
  _lbClearPendingViewportRestore();
  _lbSetZoom(_lbZoom * (direction > 0 ? 1.25 : 0.8), null, null);
}

function setLightboxZoomToFit() {
  if (_lbVisualTransitionPending) return;
  _lbClearPendingViewportRestore();
  _lbSetZoom(1.0, null, null);
}

function setLightboxZoomToOneToOne(e) {
  if (_lbVisualTransitionPending) return;
  var img = document.getElementById('lightboxImg');
  if (!img) return;
  _lbClearPendingViewportRestore();

  // If we don't know native zoom yet (e.g. dimensions fetch still pending),
  // fall back to old two-tier behavior: load original, no continuous control.
  if (!_lbNativeZoom) {
    _lbRecomputeNativeZoom();
  }

  // Anchor zoom on click position (or center for the slider's 1:1 stop).
  var rect = img.getBoundingClientRect();
  var anchorX = e && e.clientX ? e.clientX : rect.left + rect.width / 2;
  var anchorY = e && e.clientY ? e.clientY : rect.top + rect.height / 2;
  if (_lbNativeZoom) {
    var desiredSource = _lbPickSourceKey(_lbNativeZoom);
    // Compare source ranks (not exact key equality) so an already-sharper
    // current tier — e.g. /original loaded during a previous 1:1 view when
    // the picked tier for _lbNativeZoom is /2560 — satisfies 1:1 immediately.
    // The exact-key check would otherwise enter the deferred path, leave the
    // badge at 'Loading 1:1' while a lower tier is fetched, and could strand
    // _lbPending1To1 if that fetch failed.
    if (_lbSrcRank(_lbCurrentSrcKey) < _lbSrcRank(desiredSource)) {
      _lbPending1To1 = true;
      _lbPending1To1Anchor = { x: anchorX, y: anchorY };
      _lbScheduleSourceSwap(_lbNativeZoom);
      _lbApplyTransform();
    } else {
      _lbSetZoom(_lbNativeZoom, anchorX, anchorY, true);
      _lbPending1To1 = false;
      _lbPending1To1Anchor = null;
    }
  } else {
    // Dimensions unknown. Load the high-resolution source first, then apply
    // 1:1 after the browser has decoded it; otherwise the user sees an
    // enlarged preview and reads that as a soft 1:1 view.
    _lbPending1To1 = true;
    _lbPending1To1Anchor = { x: anchorX, y: anchorY };
    _lbScheduleSourceSwap(4);
    _lbApplyTransform();
  }
}

function toggleLightboxZoom(e) {
  // Fit to 1:1 toggle. Preserved for the existing 'z' shortcut and image click.
  // A deferred 1:1 leaves _lbZoom at fit while the sharp source loads, so treat
  // that pending state as zoomed and allow a second toggle to cancel it.
  if (_lbZoom > 1.001 || _lbPending1To1) {
    setLightboxZoomToFit();
  } else {
    setLightboxZoomToOneToOne(e);
  }
}

// Scroll wheel and trackpad pinch zoom — cursor-anchored
(function() {
  document.addEventListener('wheel', function(e) {
    var overlay = document.getElementById('lightboxOverlay');
    if (!overlay || !overlay.classList.contains('active')) return;
    var wrap = document.getElementById('lightboxWrap');
    if (!wrap) return;
    // Only react to wheel events over the image wrap
    if (!wrap.contains(e.target) && e.target !== wrap) return;
    if (e.target.closest('.lightbox-zoom-control')) return;

    e.preventDefault();
    if (_lbVisualTransitionPending) return;
    // Trackpad pinch: e.ctrlKey === true; zoom factor more sensitive.
    var scale = e.ctrlKey ? 0.02 : 0.0015;
    var zoomFactor = Math.exp(-e.deltaY * scale);
    var newZoom = _lbZoom * zoomFactor;
    _lbClearPendingViewportRestore();
    _lbSetZoom(newZoom, e.clientX, e.clientY);
  }, { passive: false });
})();

// Viewport and chrome resize: recompute nativeZoom while lightbox is open so
// the fit ratio stays accurate after window/devicePixelRatio changes or the
// bottom bar wraps, grows, or is hidden.
(function() {
  var resizeTimer = null;
  var refreshSourceTier = false;
  var deferredRefreshPending = false;
  function scheduleLightboxLayoutRefresh(updateSourceTier) {
    var overlay = document.getElementById('lightboxOverlay');
    if (!overlay || !overlay.classList.contains('active')) {
      // Discard any refresh that was deferred while the lightbox was open
      // so a stale intent cannot leak into the next session.
      refreshSourceTier = false;
      deferredRefreshPending = false;
      if (resizeTimer) { clearTimeout(resizeTimer); resizeTimer = null; }
      return;
    }
    refreshSourceTier = refreshSourceTier || !!updateSourceTier;
    if (resizeTimer) clearTimeout(resizeTimer);
    resizeTimer = setTimeout(function() {
      resizeTimer = null;
      var shouldUpdateSourceTier = refreshSourceTier;
      refreshSourceTier = false;
      // Arrow navigation deliberately keeps the outgoing bitmap and its
      // transform frozen until the incoming image decodes. A resize queued
      // just before navigation may fire after the incoming metadata has
      // replaced the layout dimensions; applying it here would move the old
      // bitmap using the new photo's geometry. The image load/error handlers
      // recompute and apply the current layout when the transition finishes,
      // but they do not re-select the source tier — so preserve the pending
      // update and let _lbFlushDeferredLightboxLayoutRefresh reschedule it
      // once the transition clears.
      if (_lbVisualTransitionPending) {
        refreshSourceTier = shouldUpdateSourceTier || refreshSourceTier;
        deferredRefreshPending = true;
        return;
      }
      _lbRecomputeNativeZoom();
      // A deferred 1:1 keeps _lbZoom at fit while the high-res source loads.
      // _lbSetZoom clears _lbPending1To1 (and its trailing reschedule retargets
      // the source swap back to /full), so a resize during "loading 1:1" would
      // silently drop the user's zoom request. Capture the deferred intent,
      // re-clamp the actual zoom, then restore it and re-arm the source swap so
      // the snap still completes once the high-res tier is current.
      var pending = _lbPending1To1;
      var pendingAnchor = _lbPending1To1Anchor;
      var pendingDesiredSource = _lbDesiredSrcKey;
      if (pending) {
        _lbPending1To1 = true;
        _lbPending1To1Anchor = pendingAnchor;
        _lbClampPan();
        _lbApplyTransform();
        if (shouldUpdateSourceTier) {
          if (!pendingDesiredSource || pendingDesiredSource === _lbCurrentSrcKey) {
            _lbScheduleSourceSwap(_lbNativeZoom || 4);
          } else {
            _lbDesiredSrcKey = pendingDesiredSource;
          }
          if (_lbDesiredSrcKey === _lbCurrentSrcKey) {
            _lbApplyPendingOneToOneZoom();
          }
        }
        return;
      }
      if (shouldUpdateSourceTier) {
        // Current zoom may now violate clamps; re-run through _lbSetZoom to
        // re-clamp, apply, and select the best source for a new viewport size.
        _lbSetZoom(_lbZoom, null, null);
      } else {
        // A chrome-only resize changes the fit box but must not cancel a
        // source swap or preload that is already in flight.
        _lbClampPan();
        _lbApplyTransform();
        _lbSaveViewportState(_lightboxCurrentId);
      }
    }, 100);
  }

  window.addEventListener('resize', function() {
    scheduleLightboxLayoutRefresh(true);
  });
  if (window.ResizeObserver) {
    var wrap = document.getElementById('lightboxWrap');
    if (wrap) {
      var lightboxLayoutObserver = new ResizeObserver(function() {
        scheduleLightboxLayoutRefresh(false);
      });
      lightboxLayoutObserver.observe(wrap);
    }
  }
  // Called by the image load/error handlers once _lbVisualTransitionPending
  // clears. Reschedules any resize refresh that was skipped during the
  // transition so the incoming photo can pick up a viewport/DPR change we
  // deferred (e.g. selecting a sharper source tier after a window resize).
  window._lbFlushDeferredLightboxLayoutRefresh = function() {
    if (!deferredRefreshPending) return;
    deferredRefreshPending = false;
    scheduleLightboxLayoutRefresh(false);
  };
})();

// Drag to pan when zoomed; single click (no drag) zooms out
(function() {
  var dragging = false, didDrag = false, startX, startY, panStartX, panStartY;
  var DRAG_THRESHOLD = 5;

  document.addEventListener('mousedown', function(e) {
    // Right-click (button 2) opens the context menu — never start a pan.
    if (e.button !== 0) return;
    if (_lbVisualTransitionPending) return;
    var wrap = document.getElementById('lightboxWrap');
    if (!wrap || _lbZoom <= 1.001) return;
    if (e.target.tagName === 'BUTTON' || e.target.closest('button')) return;
    if (e.target.closest('.lightbox-zoom-control')) return;
    if (!wrap.contains(e.target) && e.target !== wrap) return;
    dragging = true;
    didDrag = false;
    startX = e.clientX;
    startY = e.clientY;
    panStartX = _lbPanX;
    panStartY = _lbPanY;
    e.preventDefault();
  });

  document.addEventListener('mousemove', function(e) {
    if (!dragging) return;
    var dx = e.clientX - startX;
    var dy = e.clientY - startY;
    if (!didDrag && (Math.abs(dx) > DRAG_THRESHOLD || Math.abs(dy) > DRAG_THRESHOLD)) {
      didDrag = true;
      // First real pan movement cancels a pending restore so an async
      // image-load retry cannot snap the viewport back mid-drag.
      _lbClearPendingViewportRestore();
    }
    _lbPanX = panStartX + dx;
    _lbPanY = panStartY + dy;
    _lbClampPan();
    _lbApplyTransform();
  });

  document.addEventListener('mouseup', function(e) {
    if (!dragging) return;
    dragging = false;
    if (!didDrag && _lbZoom > 1.001) {
      // Single click without dragging = zoom out to fit
      _lbClearPendingViewportRestore();
      _lbSetZoom(1.0, null, null);
      window._lightboxZoomHandled = true;
    } else if (didDrag) {
      _lbSaveViewportState(_lightboxCurrentId);
    }
  });
})();

function closeLightbox(e) {
  if (e && e.target && e.target.tagName === 'IMG') return;
  // Use the last committed (visible) identity, not `_lightboxCurrentId` —
  // that advances immediately at the start of navigation while the outgoing
  // bitmap is still on screen, so it names a photo the user never actually
  // viewed if the incoming /full is still loading when the user closes.
  var closedPhotoId = _lightboxCommittedId != null
    ? _lightboxCommittedId
    : _lightboxCurrentId;
  _lbFlushPendingAdjustmentSave();
  if (_lightboxCurrentId != null) _lbSaveViewportState(_lightboxCurrentId);
  var wasOpen = !!window._lbEscToken;
  if (wasOpen) { Keymap.popEsc(window._lbEscToken); window._lbEscToken = null; }
  var wrap = document.getElementById('lightboxWrap');
  if (wrap) { wrap.classList.remove('zoomed'); }
  _lbZoom = 1.0;
  _lbPanX = 0;
  _lbPanY = 0;
  _lbVisualTransitionPending = false;
  _lbSetPhotoTransitionPending(false);
  _lbDeferredOverlayApply = null;
  _lbClearPendingViewportRestore();
  _lbPendingEyeTrack = null;
  _lbEyeTrackScreenAnchor = null;
  if (_lbSwapTimer) { clearTimeout(_lbSwapTimer); _lbSwapTimer = null; }
  _lbDesiredSrcKey = null;
  _lbInitialDecodePending = false;
  _lbClearPendingInitialLoad();
  _lbResetDetailStatus();
  _lbCancelOriginalPreload();
  _lbClearAdjacentPreloads();
  _lbClearAdjustmentPreview();
  var adjustPanel = document.getElementById('lightboxAdjustPanel');
  if (adjustPanel) adjustPanel.classList.remove('open');
  var adjustBtn = document.getElementById('lightboxAdjustBtn');
  if (adjustBtn) adjustBtn.setAttribute('aria-expanded', 'false');
  toggleLightboxViewMenu(false);
  _lbSetZoomPopoverOpen(false);
  _lbApplyTransform();
  document.getElementById('lightboxOverlay').classList.remove('active');
  document.getElementById('lightboxImg').src = '';
  _lightboxCurrentId = null;
  _lbReadOnly = false;
  _lbReadOnlyMessage = 'This lightbox is read-only';
  _lbApplyReadOnlyState();
  _lightboxCommittedId = null;
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
        var exportPid = _lightboxCommittedId != null
          ? _lightboxCommittedId
          : _lightboxCurrentId;
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
    var pid = _lightboxCurrentId;
    if (!pid) return;
    openContextMenu(e, buildLightboxContextMenu(pid));
  });
})();
