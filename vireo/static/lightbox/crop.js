
function _cropCloneRecipe(recipe) {
  if (!recipe || typeof recipe !== 'object') return {};
  try { return JSON.parse(JSON.stringify(recipe)); }
  catch (_) { return {}; }
}

function _cropDefaultCrop(recipe) {
  if (!recipe.crop || typeof recipe.crop !== 'object') {
    recipe.crop = { x: 0, y: 0, w: 1, h: 1 };
  }
  recipe.crop.x = Number(recipe.crop.x) || 0;
  recipe.crop.y = Number(recipe.crop.y) || 0;
  recipe.crop.w = Number(recipe.crop.w) || 1;
  recipe.crop.h = Number(recipe.crop.h) || 1;
  recipe.crop = _cropClampBox(recipe.crop);
  return recipe.crop;
}

function _cropClampBox(crop) {
  var min = 0.02;
  var x = Number(crop.x) || 0;
  var y = Number(crop.y) || 0;
  var w = Number(crop.w) || 1;
  var h = Number(crop.h) || 1;
  w = Math.max(min, Math.min(1, w));
  h = Math.max(min, Math.min(1, h));
  x = Math.max(0, Math.min(1 - w, x));
  y = Math.max(0, Math.min(1 - h, y));
  return { x: x, y: y, w: w, h: h };
}

function _cropIsFullFrame(crop) {
  return !crop || (
    Math.abs((Number(crop.x) || 0)) < 0.0005 &&
    Math.abs((Number(crop.y) || 0)) < 0.0005 &&
    Math.abs((Number(crop.w) || 1) - 1) < 0.0005 &&
    Math.abs((Number(crop.h) || 1) - 1) < 0.0005
  );
}

function _cropSetStatus(message, isError) {
  var el = document.getElementById('cropEditorStatus');
  if (!el) return;
  el.textContent = message || '';
  el.classList.toggle('error', !!isError);
}

function _cropIsActiveSession(photoId, session) {
  return _cropPhotoId === photoId && _cropSessionSeq === session;
}

function _cropDisplayRecipe() {
  var recipe = _cropCloneRecipe(_cropRecipe);
  delete recipe.crop;
  return recipe;
}

function _cropSyncControls() {
  if (!_cropRecipe) return;
  var value = Number(_cropRecipe.straighten || 0);
  var range = document.getElementById('cropStraightenRange');
  var input = document.getElementById('cropStraightenInput');
  if (range) range.value = Math.max(-10, Math.min(10, value));
  if (input) input.value = String(Math.round(value * 10) / 10);
}

function _cropRenderBox() {
  if (!_cropRecipe) return;
  var img = document.getElementById('cropEditorImg');
  var box = document.getElementById('cropBox');
  if (!img || !box || !img.complete || !img.clientWidth || !img.clientHeight) return;
  var crop = _cropDefaultCrop(_cropRecipe);
  box.style.left = (crop.x * img.clientWidth) + 'px';
  box.style.top = (crop.y * img.clientHeight) + 'px';
  box.style.width = (crop.w * img.clientWidth) + 'px';
  box.style.height = (crop.h * img.clientHeight) + 'px';
}

function _cropUpdatePreview() {
  if (!_cropPhotoId || !_cropRecipe) return;
  var img = document.getElementById('cropEditorImg');
  if (!img) return;
  var seq = ++_cropPreviewSeq;
  var recipe = _cropDisplayRecipe();
  _cropSetStatus('Loading preview...');
  img.onload = function() {
    if (seq !== _cropPreviewSeq) return;
    _cropRenderBox();
    _cropSetStatus('');
  };
  img.onerror = function() {
    if (seq !== _cropPreviewSeq) return;
    _cropSetStatus('Could not render preview', true);
  };
  img.src = '/photos/' + _cropPhotoId + '/edit-preview?size=1920&recipe=' +
    encodeURIComponent(JSON.stringify(recipe)) + '&v=' + seq;
}

function _cropSchedulePreview() {
  if (_cropPreviewTimer) clearTimeout(_cropPreviewTimer);
  _cropPreviewTimer = setTimeout(function() {
    _cropPreviewTimer = null;
    _cropUpdatePreview();
  }, 120);
}

function _cropNormalizeRecipeForSave() {
  var recipe = _cropCloneRecipe(_cropRecipe);
  var crop = _cropDefaultCrop(recipe);
  delete recipe.version;
  if (!recipe.rotation) delete recipe.rotation;
  if (Math.abs(Number(recipe.straighten || 0)) < 0.0001) delete recipe.straighten;
  else recipe.straighten = Math.round(Number(recipe.straighten) * 10000) / 10000;
  if (_cropIsFullFrame(crop)) delete recipe.crop;
  else {
    recipe.crop = {
      x: Math.round(crop.x * 1000000) / 1000000,
      y: Math.round(crop.y * 1000000) / 1000000,
      w: Math.round(crop.w * 1000000) / 1000000,
      h: Math.round(crop.h * 1000000) / 1000000,
    };
  }
  return recipe;
}

function _cropInitEvents() {
  if (window._cropEventsReady) return;
  window._cropEventsReady = true;
  var range = document.getElementById('cropStraightenRange');
  var input = document.getElementById('cropStraightenInput');
  function setStraighten(value) {
    if (!_cropRecipe) return;
    var v = Number(value);
    if (!Number.isFinite(v)) v = 0;
    v = Math.max(-45, Math.min(45, v));
    _cropRecipe.straighten = v;
    _cropSyncControls();
    _cropSchedulePreview();
  }
  if (range) range.addEventListener('input', function() { setStraighten(range.value); });
  if (input) input.addEventListener('input', function() { setStraighten(input.value); });

  var box = document.getElementById('cropBox');
  if (box) {
    box.addEventListener('pointerdown', function(e) {
      if (!_cropRecipe) return;
      var img = document.getElementById('cropEditorImg');
      if (!img || !img.clientWidth || !img.clientHeight) return;
      var handle = e.target && e.target.dataset ? e.target.dataset.handle : '';
      _cropDrag = {
        handle: handle || 'move',
        startX: e.clientX,
        startY: e.clientY,
        startCrop: _cropCloneRecipe({ crop: _cropDefaultCrop(_cropRecipe) }).crop,
        imgW: img.clientWidth,
        imgH: img.clientHeight,
      };
      box.setPointerCapture(e.pointerId);
      e.preventDefault();
      e.stopPropagation();
    });
  }

  document.addEventListener('pointermove', function(e) {
    if (!_cropDrag || !_cropRecipe) return;
    var dx = (e.clientX - _cropDrag.startX) / _cropDrag.imgW;
    var dy = (e.clientY - _cropDrag.startY) / _cropDrag.imgH;
    var c = _cropCloneRecipe({ crop: _cropDrag.startCrop }).crop;
    var min = 0.02;
    if (_cropDrag.handle === 'move') {
      c.x += dx;
      c.y += dy;
    } else {
      if (_cropDrag.handle.indexOf('w') !== -1) {
        c.x += dx;
        c.w -= dx;
      }
      if (_cropDrag.handle.indexOf('e') !== -1) c.w += dx;
      if (_cropDrag.handle.indexOf('n') !== -1) {
        c.y += dy;
        c.h -= dy;
      }
      if (_cropDrag.handle.indexOf('s') !== -1) c.h += dy;
      if (c.w < min) {
        if (_cropDrag.handle.indexOf('w') !== -1) c.x = _cropDrag.startCrop.x + _cropDrag.startCrop.w - min;
        c.w = min;
      }
      if (c.h < min) {
        if (_cropDrag.handle.indexOf('n') !== -1) c.y = _cropDrag.startCrop.y + _cropDrag.startCrop.h - min;
        c.h = min;
      }
    }
    _cropRecipe.crop = _cropClampBox(c);
    _cropRenderBox();
    e.preventDefault();
  });
  document.addEventListener('pointerup', function() {
    _cropDrag = null;
  });
  window.addEventListener('resize', function() {
    if (_cropPhotoId) _cropRenderBox();
  });
}

async function openCropEditor() {
  if (_lbGuardReadOnly()) return false;
  if (!vireoLightboxSession.requestedPhotoId()) return;
  _cropInitEvents();
  var requestedPhotoId = vireoLightboxSession.requestedPhotoId();
  _cropPhotoId = requestedPhotoId;
  var session = ++_cropSessionSeq;
  var saveBtn = document.getElementById('cropSaveBtn');
  if (saveBtn) saveBtn.disabled = false;
  _cropSetStatus('Loading recipe...');
  try {
    _lbFlushPendingAdjustmentSave();
    await _lbWaitForAdjustmentSaveIdle(requestedPhotoId);
    if (vireoLightboxSession.requestedPhotoId() !== requestedPhotoId || !_cropIsActiveSession(requestedPhotoId, session)) return;
    var data = await safeFetch('/api/photos/' + requestedPhotoId + '/edit-recipe', {}, { toast: false });
    if (vireoLightboxSession.requestedPhotoId() !== requestedPhotoId || !_cropIsActiveSession(requestedPhotoId, session)) return;
    _cropRecipe = _cropCloneRecipe(_lbEditRecipeLoaded ? _lbEditRecipe : (data.recipe || {}));
    _cropDefaultCrop(_cropRecipe);
    _cropSyncControls();
    var modal = document.getElementById('cropEditorModal');
    if (_cropEscToken) Keymap.popEsc(_cropEscToken);
    _cropEscToken = Keymap.pushEsc(function() { closeCropEditor(); });
    if (modal) modal.classList.add('open');
    _cropUpdatePreview();
  } catch (e) {
    if (_cropIsActiveSession(requestedPhotoId, session)) {
      _cropSetStatus(e.message || 'Could not load recipe', true);
    }
  }
}

function closeCropEditor(event) {
  if (event) {
    event.preventDefault();
    event.stopPropagation();
  }
  var modal = document.getElementById('cropEditorModal');
  if (modal) modal.classList.remove('open');
  if (_cropEscToken) {
    Keymap.popEsc(_cropEscToken);
    _cropEscToken = null;
  }
  _cropSessionSeq++;
  _cropPhotoId = null;
  _cropRecipe = null;
  _cropDrag = null;
  if (_cropPreviewTimer) {
    clearTimeout(_cropPreviewTimer);
    _cropPreviewTimer = null;
  }
}

function cropRotate(delta) {
  if (!_cropRecipe) return;
  var current = Number(_cropRecipe.rotation || 0);
  var next = (current + delta) % 360;
  if (next < 0) next += 360;
  _cropRecipe.rotation = next;
  cropResetFrame();
  _cropSchedulePreview();
}

function cropResetFrame() {
  if (!_cropRecipe) return;
  _cropRecipe.crop = { x: 0, y: 0, w: 1, h: 1 };
  _cropRenderBox();
}

function _cropBoxForAspect(crop, normalizedRatio) {
  var current = _cropClampBox(crop);
  if (!Number.isFinite(normalizedRatio) || normalizedRatio <= 0) return current;
  var centerX = current.x + current.w / 2;
  var centerY = current.y + current.h / 2;
  var w = current.w;
  var h = current.h;
  // Fit the requested ratio inside the user's current crop instead of
  // replacing their latest drag with a new full-frame-centered crop. Only
  // the minimum-size floor below is allowed to grow beyond that selection.
  if (w / h > normalizedRatio) w = h * normalizedRatio;
  else h = w / normalizedRatio;
  // Keep the shared editor's per-axis 2% clamp from changing the ratio.
  var minScale = Math.max(1, 0.02 / w, 0.02 / h);
  w *= minScale;
  h *= minScale;
  return _cropClampBox({
    x: centerX - w / 2,
    y: centerY - h / 2,
    w: w,
    h: h,
  });
}

function cropSetAspect(aspect) {
  if (!_cropRecipe) return;
  var img = document.getElementById('cropEditorImg');
  if (!img || !img.clientWidth || !img.clientHeight) return;
  var imageAspect = img.clientWidth / img.clientHeight;
  var normalizedRatio = aspect / imageAspect;
  _cropRecipe.crop = _cropBoxForAspect(
    _cropDefaultCrop(_cropRecipe), normalizedRatio
  );
  _cropRenderBox();
}

async function saveCropEditor() {
  if (_lbGuardReadOnly()) return false;
  if (!_cropPhotoId || !_cropRecipe) return;
  var pid = _cropPhotoId;
  var session = _cropSessionSeq;
  var btn = document.getElementById('cropSaveBtn');
  if (btn) btn.disabled = true;
  _cropSetStatus('Saving...');
  try {
    var recipe = _cropNormalizeRecipeForSave();
    var data = await safeFetch('/api/photos/' + pid + '/edit-recipe', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ recipe: recipe }),
    }, { toast: false });
    var updates = {};
    updates[String(pid)] = data.recipe;
    _lbRefreshEditRecipeCache(updates);
    var p = _lightboxPhotoList.find(function(x) { return x.id === pid; });
    if (_cropIsActiveSession(pid, session)) {
      closeCropEditor();
      if (vireoLightboxSession.requestedPhotoId() === pid) {
        var filename = p ? p.filename : document.getElementById('lightboxFilename').textContent;
        openLightbox(pid, filename, _lightboxPhotoList);
      }
      showToast('Crop saved', 'success');
    }
  } catch (e) {
    if (_cropIsActiveSession(pid, session)) {
      _cropSetStatus(e.message || 'Could not save crop', true);
    }
  } finally {
    if (btn && _cropIsActiveSession(pid, session)) btn.disabled = false;
  }
}

async function cropClearEdits() {
  if (!_cropPhotoId) return;
  var pid = _cropPhotoId;
  var session = _cropSessionSeq;
  _cropSetStatus('Clearing...');
  try {
    await safeFetch('/api/photos/' + pid + '/edit-recipe', {
      method: 'DELETE',
    }, { toast: false });
    var updates = {};
    updates[String(pid)] = null;
    _lbRefreshEditRecipeCache(updates);
    var p = _lightboxPhotoList.find(function(x) { return x.id === pid; });
    if (_cropIsActiveSession(pid, session)) {
      closeCropEditor();
      if (vireoLightboxSession.requestedPhotoId() === pid) {
        var filename = p ? p.filename : document.getElementById('lightboxFilename').textContent;
        openLightbox(pid, filename, _lightboxPhotoList);
      }
      showToast('Edits cleared', 'success');
    }
  } catch (e) {
    if (_cropIsActiveSession(pid, session)) {
      _cropSetStatus(e.message || 'Could not clear edits', true);
    }
  }
}
