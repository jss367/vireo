
function _lbStoredBool(key, fallback) {
  try {
    var value = localStorage.getItem(key);
    if (value === '0') return false;
    if (value === '1') return true;
  } catch (e) {}
  return fallback;
}

function _lbPersistBool(key, value) {
  try { localStorage.setItem(key, value ? '1' : '0'); } catch (e) {}
}

var _lbBoxesVisible = _lbStoredBool('vireo.lb.boxesVisible', false);
var _lbMasksVisible = _lbStoredBool('vireo.lb.masksVisible', false);
var _lbEyeVisible = _lbStoredBool('vireo.lb.eyeVisible', false);
var _lbTrackEyeEnabled = _lbStoredBool('vireo.lb.trackEye', false);
var _lbInfoVisible = _lbStoredBool('vireo.lb.infoVisible', true);
var _lbChromeVisible = _lbStoredBool('vireo.lb.chromeVisible', true);

function _lbRecipeHasCrop(recipe) {
  return !!(recipe && recipe.crop && Number(recipe.crop.w) > 0 && Number(recipe.crop.h) > 0);
}

function _lbRecipeHasGeometricEdit(recipe) {
  if (!recipe || typeof recipe !== 'object') return false;
  if (_lbRecipeHasCrop(recipe)) return true;
  if (Number(recipe.rotation || 0)) return true;
  if (Math.abs(Number(recipe.straighten || 0)) > 1e-9) return true;
  var flip = recipe.flip || {};
  return !!(flip.horizontal || flip.vertical);
}

function _lbMetadataOrientation(metadata) {
  if (!metadata || typeof metadata !== 'object') return null;
  var groups = ['EXIF', 'IFD0', 'TIFF', 'File'];
  for (var i = 0; i < groups.length; i++) {
    var group = metadata[groups[i]];
    if (group && Object.prototype.hasOwnProperty.call(group, 'Orientation')) {
      return group.Orientation;
    }
  }
  if (Object.prototype.hasOwnProperty.call(metadata, 'Orientation')) {
    return metadata.Orientation;
  }
  return null;
}

function _lbOrientationSwapsAxes(orientation) {
  if (orientation == null || typeof orientation === 'boolean') return false;
  if (typeof orientation === 'number') {
    return [5, 6, 7, 8].indexOf(Math.round(orientation)) !== -1;
  }
  var text = String(orientation).trim().toLowerCase();
  if (!text) return false;
  var parsed = Number(text);
  if (Number.isFinite(parsed)) return [5, 6, 7, 8].indexOf(Math.round(parsed)) !== -1;
  return text.indexOf('90') !== -1 || text.indexOf('270') !== -1;
}

function _lbSourceOverlaysAvailable() {
  var pairUsesJpeg = (
    vireoLightboxSession.requestedPhotoId() != null &&
    _vireoPairKnownByPhoto[String(vireoLightboxSession.requestedPhotoId())] &&
    _vireoPairSource(vireoLightboxSession.requestedPhotoId()) === 'jpeg'
  );
  return !pairUsesJpeg && !_lbRecipeHasGeometricEdit(_lbCurrentEditRecipe);
}

function _lbApplyBoxesVisibility() {
  var container = document.getElementById('lightboxDetections');
  var available = _lbSourceOverlaysAvailable();
  if (container) container.style.display = (_lbBoxesVisible && available) ? '' : 'none';
  var input = document.getElementById('lightboxToggleBoxes');
  if (input) {
    input.disabled = !available;
    input.checked = _lbBoxesVisible;
  }
  var note = document.getElementById('lightboxBoxesNote');
  if (note) note.textContent = available ? '' : 'unavailable for edited photos';
}

function toggleLightboxBoxes() {
  _lbBoxesVisible = !_lbBoxesVisible;
  _lbPersistBool('vireo.lb.boxesVisible', _lbBoxesVisible);
  _lbApplyBoxesVisibility();
}

function _lbApplyInfoVisibility() {
  var overlay = document.getElementById('lightboxOverlay');
  if (overlay) overlay.classList.toggle('lb-hide-info', !_lbInfoVisible);
  var input = document.getElementById('lightboxToggleInfo');
  if (input) input.checked = _lbInfoVisible;
}

function toggleLightboxInfo() {
  _lbInfoVisible = !_lbInfoVisible;
  if (!_lbInfoVisible) _lbSetZoomPopoverOpen(false);
  _lbPersistBool('vireo.lb.infoVisible', _lbInfoVisible);
  _lbApplyInfoVisibility();
}

function _lbApplyChromeVisibility() {
  var overlay = document.getElementById('lightboxOverlay');
  if (overlay) overlay.classList.toggle('lb-hide-chrome', !_lbChromeVisible);
  var input = document.getElementById('lightboxToggleChrome');
  if (input) input.checked = _lbChromeVisible;
}

function toggleLightboxChrome() {
  _lbChromeVisible = !_lbChromeVisible;
  if (!_lbChromeVisible) _lbSetZoomPopoverOpen(false);
  _lbPersistBool('vireo.lb.chromeVisible', _lbChromeVisible);
  _lbApplyChromeVisibility();
}

function _lbPositionViewPanel() {
  var panel = document.getElementById('lightboxViewPanel');
  var btn = document.getElementById('lightboxViewBtn');
  if (!panel || !btn || !panel.classList.contains('open')) return;
  // The actions wrap and conditionally gain controls as photos change. Keep
  // the panel attached to its button even when that happens while it is open.
  var gutter = 8;
  var btnRect = btn.getBoundingClientRect();
  var maxLeft = Math.max(gutter, window.innerWidth - panel.offsetWidth - gutter);
  var desiredBottom = window.innerHeight - btnRect.top + gutter;
  var maxBottom = Math.max(gutter, window.innerHeight - panel.offsetHeight - gutter);
  panel.style.left = Math.max(gutter, Math.min(btnRect.left, maxLeft)) + 'px';
  panel.style.bottom = Math.max(gutter, Math.min(desiredBottom, maxBottom)) + 'px';
}

var _lbViewPanelPositionFrame = null;
function _lbScheduleViewPanelPosition() {
  if (_lbViewPanelPositionFrame !== null) return;
  _lbViewPanelPositionFrame = requestAnimationFrame(function() {
    _lbViewPanelPositionFrame = null;
    _lbPositionViewPanel();
  });
}

function toggleLightboxViewMenu(force) {
  var panel = document.getElementById('lightboxViewPanel');
  var btn = document.getElementById('lightboxViewBtn');
  if (!panel || !btn) return;
  var open = typeof force === 'boolean' ? force : !panel.classList.contains('open');
  panel.classList.toggle('open', open);
  btn.setAttribute('aria-expanded', open ? 'true' : 'false');
  if (!open) return;
  // Refresh every row so the menu reflects the current photo's state.
  _lbApplyBoxesVisibility();
  _lbApplyMaskToggleButton();
  _lbApplyEyeVisibility();
  _lbApplyTrackEyeState();
  _lbApplyInfoVisibility();
  _lbApplyChromeVisibility();
  // The overlay is position:fixed inset:0, so viewport coords map directly.
  _lbPositionViewPanel();
}

(function() {
  var actions = document.getElementById('lightboxActions');
  var viewBtn = document.getElementById('lightboxViewBtn');
  window.addEventListener('resize', _lbScheduleViewPanelPosition);
  document.addEventListener('fullscreenchange', _lbScheduleViewPanelPosition);
  document.addEventListener('webkitfullscreenchange', _lbScheduleViewPanelPosition);
  if (window.ResizeObserver) {
    var viewLayoutObserver = new ResizeObserver(_lbScheduleViewPanelPosition);
    if (actions) viewLayoutObserver.observe(actions);
    if (viewBtn) viewLayoutObserver.observe(viewBtn);
  }
  if (window.MutationObserver && actions) {
    var viewLayoutMutationObserver = new MutationObserver(_lbScheduleViewPanelPosition);
    viewLayoutMutationObserver.observe(actions, {
      attributes: true,
      childList: true,
      characterData: true,
      subtree: true
    });
  }
})();

function lightboxSetFlagAction(flag) {
  if (vireoLightboxSession.requestedPhotoId() == null) return;
  var pid = vireoLightboxSession.requestedPhotoId();
  // Clicking the button for the photo's current state clears it, so the pair
  // covers flag/reject/unflag without a third button. Match against the
  // *displayed* flag (which includes provisional edits, e.g. Group Review
  // stages picks/rejects before Apply) so a second click on an already-lit
  // button clears the shown state instead of re-applying the same edit.
  var current = _lbNormalizeFlag(_lbDisplayedFlagFor(pid)) || 'none';
  var target = (current === flag) ? 'none' : flag;
  // Same dispatch order as the keyboard shortcuts: the misses page intercepts
  // flag writes to keep its local model in sync.
  if (
    typeof window.handleMissesLightboxFlagShortcut === 'function' &&
    window.handleMissesLightboxFlagShortcut(pid, target) === true
  ) return;
  _lbApplyFlag(pid, target);
}

function _lbApplyEyeVisibility() {
  var container = document.getElementById('lightboxEye');
  var available = _lbSourceOverlaysAvailable();
  if (container) {
    container.style.display =
      (_lbEyeVisible && available && container.childElementCount > 0) ? 'block' : 'none';
  }
  var input = document.getElementById('lightboxToggleEye');
  if (input) {
    input.disabled = !available;
    input.checked = _lbEyeVisible;
  }
  var note = document.getElementById('lightboxEyeNote');
  if (note) note.textContent = available ? '' : 'unavailable for edited photos';
}

function toggleLightboxEye() {
  _lbEyeVisible = !_lbEyeVisible;
  _lbPersistBool('vireo.lb.eyeVisible', _lbEyeVisible);
  _lbApplyEyeVisibility();
}

function _lbPhotoData(photoId) {
  if (photoId == null) return null;
  var cached = _lbPhotoDataByPhoto[String(photoId)];
  if (cached) return cached;
  return _lightboxPhotoList.find(function(photo) {
    return String(photo.id) === String(photoId);
  }) || null;
}

function _lbPhotoEyePoint(photoId, photo) {
  photo = photo || _lbPhotoData(photoId);
  if (!photo || photo.eye_x == null || photo.eye_y == null) return null;
  var x = Number(photo.eye_x);
  var y = Number(photo.eye_y);
  if (!Number.isFinite(x) || !Number.isFinite(y)) return null;
  return _lbTransformPointByRecipe(
    x,
    y,
    _lbEditRecipeByPhoto[String(photoId)] || photo.edit_recipe
  );
}

function _lbApplyTrackEyeState() {
  var input = document.getElementById('lightboxTrackEye');
  if (!input) return;
  var photo = _lbPhotoData(vireoLightboxSession.requestedPhotoId());
  var eyeKnown = !!(
    photo && Object.prototype.hasOwnProperty.call(photo, 'eye_x')
  );
  var hasEye = !!_lbPhotoEyePoint(vireoLightboxSession.requestedPhotoId(), photo);
  var available = hasEye && _lbSourceOverlaysAvailable();
  input.checked = _lbTrackEyeEnabled;
  var note = document.getElementById('lightboxTrackEyeNote');
  if (note) {
    if (_lbTrackEyeEnabled && !available) {
      note.textContent = eyeKnown ? 'paused for this photo' : 'waiting for eye';
    } else {
      note.textContent = '';
    }
  }
  var row = input.closest('.lb-view-row');
  if (row) {
    if (_lbTrackEyeEnabled && !available) {
      row.title = eyeKnown
        ? 'Eye tracking is on but unavailable for this photo'
        : 'Eye tracking is on; waiting for this photo\'s eye metadata';
    } else if (!_lbTrackEyeEnabled && !available) {
      row.title = 'Turn on eye tracking; it will start when a usable eye keypoint is available';
    } else {
      row.title = 'Keep the detected eye in the same screen position while navigating';
    }
  }
}

function toggleLightboxTrackEye() {
  _lbTrackEyeEnabled = !_lbTrackEyeEnabled;
  _lbPersistBool('vireo.lb.trackEye', _lbTrackEyeEnabled);
  _lbPendingEyeTrack = null;
  _lbEyeTrackScreenAnchor = null;
  if (_lbTrackEyeEnabled) _lbCaptureEyeTrackingAnchor();
  _lbApplyTrackEyeState();
}

/* Species color palette for lightbox bounding boxes */
var _lbSpeciesColors = {};
var _lbColorPalette = [
  '#24E5CA', '#f0c040', '#e74c3c', '#3498db', '#9b59b6',
  '#1abc9c', '#e67e22', '#2ecc71', '#e84393', '#00cec9'
];
var _lbNextColorIdx = 0;

function _lbGetSpeciesColor(species) {
  if (!_lbSpeciesColors[species]) {
    _lbSpeciesColors[species] = _lbColorPalette[_lbNextColorIdx % _lbColorPalette.length];
    _lbNextColorIdx++;
  }
  return _lbSpeciesColors[species];
}

function _lbRenderEyeCrosshair(photo) {
  // Draws a crosshair at the detected eye keypoint on the current lightbox
  // photo. eye_x / eye_y are stored normalized 0-1 against the oriented
  // (EXIF-transposed) image, same as detection boxes, so we can map
  // directly to percentage without touching photo.width/photo.height
  // (which come from the un-oriented sensor tag and would swap axes on
  // orientation 6/8 rotations).
  var container = document.getElementById('lightboxEye');
  if (!container) return;
  container.innerHTML = '';
  if (!photo || photo.eye_x == null || photo.eye_y == null) {
    container.style.display = 'none';
    return;
  }
  var point = _lbTransformPointByRecipe(
    Number(photo.eye_x),
    Number(photo.eye_y),
    _lbEditRecipeByPhoto[String(photo.id || vireoLightboxSession.requestedPhotoId())]
  );
  var xPct = point.x * 100;
  var yPct = point.y * 100;
  var marker = document.createElement('div');
  marker.className = 'lb-eye-crosshair';
  marker.style.left = xPct.toFixed(3) + '%';
  marker.style.top = yPct.toFixed(3) + '%';
  if (photo.eye_conf != null) {
    var label = document.createElement('span');
    label.textContent = 'eye ' + Math.round(photo.eye_conf * 100) + '%';
    marker.appendChild(label);
  }
  container.appendChild(marker);
  _lbApplyEyeVisibility();
  _lbApplyTrackEyeState();
}

function _lbLoadDetections(photoId) {
  var container = document.getElementById('lightboxDetections');
  if (!container) return;
  container.innerHTML = '';
  if (!_lbSourceOverlaysAvailable()) {
    _lbApplyBoxesVisibility();
    return;
  }
  fetch('/api/detections/' + photoId)
    .then(function(r) { return r.json(); })
    .then(function(detections) {
      if (!detections || detections.length === 0) return;
      // Only render if still viewing the same photo
      if (vireoLightboxSession.requestedPhotoId() !== photoId) return;
      if (!_lbSourceOverlaysAvailable()) {
        container.innerHTML = '';
        _lbApplyBoxesVisibility();
        return;
      }
      var recipe = _lbEditRecipeByPhoto[String(photoId)] || _lbCurrentEditRecipe;
      detections.forEach(function(det) {
        if (det.box_x == null || det.box_y == null) return;
        var color = _lbGetSpeciesColor(det.category || 'animal');
        var transformed = _lbTransformBoxByRecipe({
          x: det.box_x,
          y: det.box_y,
          w: det.box_w,
          h: det.box_h,
        }, recipe);
        var box = document.createElement('div');
        box.className = 'lb-detection-box';
        box.style.cssText = 'left:' + (transformed.x * 100).toFixed(2) + '%;top:' + (transformed.y * 100).toFixed(2) + '%;width:' + (transformed.w * 100).toFixed(2) + '%;height:' + (transformed.h * 100).toFixed(2) + '%;border-color:' + color + ';';
        var conf = det.detector_confidence ? Math.round(det.detector_confidence * 100) + '%' : '';
        if (conf) {
          box.innerHTML = '<span class="lb-detection-label" style="background:' + color + ';color:#0A1F2E;">' + conf + '</span>';
        }
        container.appendChild(box);
      });
    })
    .catch(function() {});
}

/* ---- SAM mask overlay ---------------------------------------------------
 * The lightbox lets the user A/B-compare SAM mask variants without
 * changing which one drives downstream scoring. The dropdown defaults
 * to "active" (whatever photos.active_mask_variant points at) and lists
 * one option per variant the photo has under photo_masks. Selecting a
 * variant only swaps the overlay src; it never POSTs back to the server.
 *
 * Pages that don't surface masks at all (e.g. browse before the pipeline
 * has run) still get the lightbox — the dropdown stays hidden until at
 * least one variant comes back from /api/photos/<id>/masks, mirroring
 * how the detection-box toggle stays present but inert when there are
 * no detections.
 */
var _lbMaskActiveVariant = null;     // name of the photo's currently-active variant
var _lbMaskAvailable = [];           // [{variant, url, created_at}, ...]
var _lbMaskCurrentUrl = null;
var _lbMaskLoadSeq = 0;

function _lbResetMaskOverlay() {
  var img = document.getElementById('lightboxMaskOverlay');
  _lbMaskLoadSeq++;
  if (img) {
    img.classList.remove('show');
    img.onload = null;
    img.onerror = null;
    img.removeAttribute('src');
  }
  var ctrls = document.getElementById('lightboxMaskControls');
  if (ctrls) ctrls.classList.remove('has-variants');
  var sel = document.getElementById('lightboxMaskSelect');
  if (sel) sel.innerHTML = '';
  _lbMaskActiveVariant = null;
  _lbMaskAvailable = [];
  _lbMaskCurrentUrl = null;
  _lbApplyMaskToggleButton();
}

function _lbApplyMaskVisibility() {
  var img = document.getElementById('lightboxMaskOverlay');
  if (!img) return;
  _lbApplyMaskToggleButton();
  if (!_lbSourceOverlaysAvailable()) {
    var ctrls = document.getElementById('lightboxMaskControls');
    if (ctrls) ctrls.classList.remove('has-variants');
  }
  if (!_lbSourceOverlaysAvailable() || !_lbMasksVisible || !_lbMaskCurrentUrl) {
    img.classList.remove('show');
    // A previous call may have assigned onload to re-add `show` once
    // the mask image finished loading. If the user toggles masks off
    // while that request is still in flight, the pending callback
    // would override the hide as soon as it fires. Clear both handlers
    // so an in-flight load can't resurrect the overlay.
    _lbMaskLoadSeq++;
    img.onload = null;
    img.onerror = null;
    // Detach the decoded mask image while hidden. The class normally
    // controls display, but removing src avoids stale blended pixels in
    // WebView/compositor edge cases and forces a clean reload on show.
    img.removeAttribute('src');
    return;
  }
  var expectedUrl = _lbMaskCurrentUrl;
  var seq = ++_lbMaskLoadSeq;
  // onerror guards against the file being deleted out from under us
  // between the /api/photos/<id>/masks call and the GET. The DB row
  // exists but the file is gone → silently hide instead of leaving a
  // broken-image icon over the photo.
  img.onerror = function() {
    if (seq === _lbMaskLoadSeq && _lbMaskCurrentUrl === expectedUrl) {
      img.classList.remove('show');
    }
  };
  img.onload = function() {
    // Re-check the toggle: the user may have hidden masks between
    // src assignment and the load resolving. Also re-check the visual
    // transition: while the outgoing bitmap is still frozen on screen,
    // painting the incoming photo's mask would show it over the previous
    // image. handleInitialImageLoad re-runs _lbApplyMaskVisibility once
    // the transition clears, so the mask surfaces as soon as it is safe.
    if (
      seq === _lbMaskLoadSeq
      && _lbMasksVisible
      && _lbMaskCurrentUrl === expectedUrl
      && !_lbVisualTransitionPending
    ) {
      img.classList.add('show');
    }
  };
  if (img.getAttribute('src') !== expectedUrl) {
    img.classList.remove('show');
    img.src = expectedUrl;
  } else if (img.naturalWidth > 0 && !_lbVisualTransitionPending) {
    // Same URL and we know it loaded successfully — safe to re-show
    // without re-fetching, once the visual transition has cleared.
    img.classList.add('show');
  }
  // If src matches but naturalWidth is 0, the previous load either failed
  // (onerror already hid the overlay; re-adding `show` would surface a
  // broken-image icon) or is still in flight (the onload assigned above
  // will add `show` when the request resolves). Either way, don't add
  // `show` here.
}

function _lbApplyMaskUrl(url) {
  // Set the selected overlay URL. Visibility is controlled separately so
  // users can keep their mask preference across photos and sessions.
  var img = document.getElementById('lightboxMaskOverlay');
  _lbMaskCurrentUrl = url || null;
  if (!url && img) img.removeAttribute('src');
  _lbApplyMaskVisibility();
}

function _lbApplyMaskToggleButton() {
  var input = document.getElementById('lightboxToggleMasks');
  if (!input) return;
  var available = _lbSourceOverlaysAvailable();
  input.disabled = !available;
  input.checked = _lbMasksVisible;
  var note = document.getElementById('lightboxMasksNote');
  if (note) note.textContent = available ? '' : 'unavailable for edited photos';
}

function toggleLightboxMasks() {
  _lbMasksVisible = !_lbMasksVisible;
  _lbPersistBool('vireo.lb.masksVisible', _lbMasksVisible);
  _lbApplyMaskToggleButton();
  _lbApplyMaskVisibility();
}

function _lbOnMaskVariantChange() {
  var sel = document.getElementById('lightboxMaskSelect');
  if (!sel) return;
  if (_lbCurrentRecipeHasOrientation()) {
    _lbApplyMaskUrl(null);
    return;
  }
  var choice = sel.value;
  if (choice === '__active__') {
    if (!_lbMaskActiveVariant) {
      // No active variant → nothing to overlay.
      _lbApplyMaskUrl(null);
      return;
    }
    var match = _lbMaskAvailable.find(function(v) {
      return v.variant === _lbMaskActiveVariant;
    });
    _lbApplyMaskUrl(match ? match.url : null);
    return;
  }
  var picked = _lbMaskAvailable.find(function(v) { return v.variant === choice; });
  _lbApplyMaskUrl(picked ? picked.url : null);
}

function _lbLoadMaskVariants(photoId) {
  // Fetch the photo's available mask variants and populate the
  // dropdown. Defaults the selection to "active" (which displays the
  // photo's active_mask_variant overlay if one exists).
  var sel = document.getElementById('lightboxMaskSelect');
  var ctrls = document.getElementById('lightboxMaskControls');
  if (!sel || !ctrls) return;
  // Reset before the async fetch resolves so a fast nav doesn't show
  // stale options for the previous photo.
  _lbResetMaskOverlay();

  fetch('/api/photos/' + photoId + '/masks')
    .then(function(r) { return r.ok ? r.json() : null; })
    .then(function(data) {
      // Bail if the user navigated to a different photo while the
      // request was in flight.
      if (!data || vireoLightboxSession.requestedPhotoId() !== photoId) return;
      if (!_lbSourceOverlaysAvailable()) return;
      _lbMaskActiveVariant = data.active || null;
      _lbMaskAvailable = data.variants || [];
      if (_lbCurrentRecipeHasOrientation()) {
        _lbApplyMaskUrl(null);
        return;
      }
      if (_lbMaskAvailable.length === 0) {
        // No masks for this photo — keep the control hidden.
        return;
      }
      // Build options: "active" first, then one per variant. Variant
      // names come from workspace config and must be treated as
      // untrusted — render via DOM APIs so a name containing HTML
      // can't escape into the page as stored DOM XSS.
      var activeLabel = _lbMaskActiveVariant
        ? 'active (' + _lbMaskActiveVariant + ')'
        : 'active (n/a)';
      sel.textContent = '';
      var activeOpt = document.createElement('option');
      activeOpt.value = '__active__';
      activeOpt.textContent = activeLabel;
      sel.appendChild(activeOpt);
      _lbMaskAvailable.forEach(function(v) {
        var opt = document.createElement('option');
        opt.value = v.variant;
        opt.textContent = v.variant;
        sel.appendChild(opt);
      });
      sel.value = '__active__';
      ctrls.classList.add('has-variants');
      // Apply the default ("active") overlay so the user sees something
      // the moment masks become available.
      _lbOnMaskVariantChange();
    })
    .catch(function() { /* network error → leave control hidden */ });
}
