// Threshold tuning: the slider controls and labels, the tuning status and
// disabled state, loading the config, and preview, recompute and reset.
// Classic page script; load boot.js after all definitions.

function setMissTuningStatus(text) {
  var el = document.getElementById('missTuningStatus');
  if (el) el.textContent = text || '';
}

function setMissTuningDisabled(disabled) {
  MISS_TUNING_CONTROL_IDS.forEach(function(id) {
    var el = document.getElementById(id);
    if (el) el.disabled = !!disabled;
  });
}

function invalidateMissPreview() {
  previewRequestId++;
  if (previewTimer) {
    clearTimeout(previewTimer);
    previewTimer = null;
  }
}

function percentValue(id) {
  return parseInt(document.getElementById(id).value, 10) / 100;
}

function addChangedSliderValue(body, key, id, scale) {
  var raw = parseInt(document.getElementById(id).value, 10);
  var baseline = null;
  if (missConfig && missConfig[key] != null) {
    baseline = Math.round(Number(missConfig[key]) * scale);
  }
  if (baseline == null || raw !== baseline) {
    body[key] = raw / scale;
  }
}

function collectMissThresholds() {
  // No miss_enabled toggle on this page: omit it so preview/recompute (and
  // save_defaults) preserve a workspace's effective value instead of silently
  // re-enabling miss detection for workspaces that disabled it.
  var body = {};
  addChangedSliderValue(body, 'detector_confidence', 'missCfgDetectorFloor', 100);
  addChangedSliderValue(body, 'miss_det_confidence', 'missCfgNoSubject', 100);
  addChangedSliderValue(body, 'miss_classifier_override_conf', 'missCfgClassifierRescue', 100);
  addChangedSliderValue(body, 'miss_bbox_area_min', 'missCfgBboxMin', 1000);
  addChangedSliderValue(body, 'miss_oof_ratio', 'missCfgOofRatio', 100);
  return body;
}

function updateMissSliderLabels() {
  var det = parseInt(document.getElementById('missCfgDetectorFloor').value, 10);
  var ns = parseInt(document.getElementById('missCfgNoSubject').value, 10);
  var rescue = parseInt(document.getElementById('missCfgClassifierRescue').value, 10);
  var bbox = parseInt(document.getElementById('missCfgBboxMin').value, 10);
  var oof = parseInt(document.getElementById('missCfgOofRatio').value, 10);
  document.getElementById('missCfgDetectorFloorVal').textContent = det + '%';
  document.getElementById('missCfgNoSubjectVal').textContent = ns + '%';
  document.getElementById('missCfgClassifierRescueVal').textContent = rescue > 100 ? 'off' : rescue + '%';
  document.getElementById('missCfgBboxMinVal').textContent = (bbox / 1000).toFixed(3);
  document.getElementById('missCfgOofRatioVal').textContent = (oof / 100).toFixed(2);
}

function setMissSliders(config) {
  document.getElementById('missCfgDetectorFloor').value = Math.round((config.detector_confidence || 0) * 100);
  document.getElementById('missCfgNoSubject').value = Math.round((config.miss_det_confidence || 0) * 100);
  document.getElementById('missCfgClassifierRescue').value = Math.round((config.miss_classifier_override_conf || 0) * 100);
  document.getElementById('missCfgBboxMin').value = Math.round((config.miss_bbox_area_min || 0) * 1000);
  document.getElementById('missCfgOofRatio').value = Math.round((config.miss_oof_ratio || 0) * 100);
  updateMissSliderLabels();
}

function installMissSliderHandlers() {
  [
    'missCfgDetectorFloor',
    'missCfgNoSubject',
    'missCfgClassifierRescue',
    'missCfgBboxMin',
    'missCfgOofRatio',
  ].forEach(function(id) {
    var el = document.getElementById(id);
    if (!el) return;
    el.addEventListener('input', function() {
      updateMissSliderLabels();
      scheduleMissPreview();
    });
  });
}

async function loadMissConfig() {
  missConfigLoaded = false;
  setMissTuningDisabled(true);
  try {
    var r = await fetch('/api/misses/config');
    if (!r.ok) throw new Error('http ' + r.status);
    missConfig = await r.json();
    originalMissConfig = Object.assign({}, missConfig);
    setMissSliders(missConfig);
    missConfigLoaded = true;
    // Keep the tuning group frozen when a fail-closed scope is in effect;
    // loadMisses() will re-enable it once a user-supplied replacement
    // filter loads successfully.
    if (!legacyScopeError && !legacyScopeRecoveryPending) {
      setMissTuningDisabled(false);
    }
    setMissTuningStatus('Preview uses current sliders');
  } catch (e) {
    setMissTuningStatus('Thresholds unavailable');
  }
}

function scheduleMissPreview() {
  if (!missConfigLoaded) return;
  if (legacyScopeError || legacyScopeRecoveryPending) return;
  if (previewTimer) clearTimeout(previewTimer);
  previewTimer = setTimeout(previewMisses, 250);
}

async function previewMisses() {
  if (!missConfigLoaded) return;
  // Defense in depth: sliders are disabled while a deep-link scope is
  // unresolved, but any caller that reaches here anyway must not POST the
  // empty rule payload to /api/misses/preview and repopulate the grid with
  // photos outside the user's scope.
  if (legacyScopeError || legacyScopeRecoveryPending) return;
  if (previewTimer) {
    clearTimeout(previewTimer);
    previewTimer = null;
  }
  var requestId = ++previewRequestId;
  missLoadRequestId++;
  var requestedFilters = missFilterPayload();
  var body = collectMissThresholds();
  addFilterPayloadToBody(body, requestedFilters);
  var since = getSinceParam();
  if (since) body.since = since;
  setMissTuningStatus('Previewing...');
  try {
    var r = await fetch('/api/misses/preview', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!r.ok) throw new Error('http ' + r.status);
    var data = await r.json();
    if (requestId !== previewRequestId) return;
    appliedMissFilters = cloneMissFilterPayload(requestedFilters);
    previewActive = true;
    previewPayload = data;
    applyMissesPayload(data);
    setMissTuningStatus('Preview only');
  } catch (e) {
    if (requestId !== previewRequestId) return;
    setMissTuningStatus('Preview failed');
  }
}

async function recomputeMisses() {
  if (!missConfigLoaded) return;
  if (legacyScopeError || legacyScopeRecoveryPending) return;
  invalidateMissPreview();
  var requestId = ++missLoadRequestId;
  var btn = document.getElementById('missRecomputeBtn');
  // Recompute the photos that produced the cards the user is looking at.
  // Live controls may already represent a newer filter whose load is pending.
  var requestedFilters = cloneMissFilterPayload(appliedMissFilters);
  var body = collectMissThresholds();
  addFilterPayloadToBody(body, requestedFilters);
  var since = getSinceParam();
  if (since) body.since = since;
  body.save_defaults = !!document.getElementById('missSaveDefaults').checked;
  if (btn) btn.disabled = true;
  previewActive = false;
  previewPayload = null;
  setMissTuningStatus('Recomputing...');
  try {
    var r = await fetch('/api/misses/recompute', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!r.ok) throw new Error('http ' + r.status);
    var data = await r.json();
    if (requestId !== missLoadRequestId) {
      if (data.config && data.saved_defaults) {
        missConfig = data.config;
        originalMissConfig = Object.assign({}, data.config);
      }
      setMissTuningStatus('Recomputed previous filters; refreshing current view');
      loadMisses();
      return;
    }
    if (!sameMissFilterPayload(missFilterPayload(), requestedFilters)) {
      if (data.config && data.saved_defaults) {
        missConfig = data.config;
        originalMissConfig = Object.assign({}, data.config);
      }
      setMissTuningStatus('Recomputed visible photos; refreshing filters');
      loadMisses();
      return;
    }
    appliedMissFilters = cloneMissFilterPayload(requestedFilters);
    previewActive = false;
    previewPayload = null;
    applyMissesPayload(data);
    if (data.config && data.saved_defaults) {
      missConfig = data.config;
      originalMissConfig = Object.assign({}, data.config);
    }
    setMissTuningStatus('Updated ' + (data.updated || 0) + ' photos');
    if (typeof showToast === 'function') showToast('Recomputed misses', 'success');
  } catch (e) {
    setMissTuningStatus('Recompute failed');
    if (typeof showToast === 'function') showToast('Recompute failed', 'error');
  } finally {
    if (btn && missConfigLoaded && !legacyScopeError && !legacyScopeRecoveryPending) {
      btn.disabled = false;
    }
  }
}

function resetMissThresholds() {
  if (!missConfigLoaded || !originalMissConfig) return;
  if (legacyScopeError || legacyScopeRecoveryPending) return;
  invalidateMissPreview();
  setMissSliders(originalMissConfig);
  previewActive = false;
  previewPayload = null;
  setMissTuningStatus('Preview reset');
  loadMisses();
}
