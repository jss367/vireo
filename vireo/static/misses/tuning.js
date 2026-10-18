// Threshold tuning: the slider controls and labels, the tuning status and
// disabled state, loading the config, and preview, recompute and reset.
// ES module.

import { state, MISS_TUNING_CONTROL_IDS } from './state.js';
import { showToast } from './shared.js';
import { getSinceParam } from './helpers.js';
import {
  addFilterPayloadToBody, cloneMissFilterPayload, missFilterPayload, sameMissFilterPayload,
} from './filters.js';
import { applyMissesPayload, loadMisses } from './loading.js';

/** @param {string} text */ function setMissTuningStatus(text) {
  var el = /** @type {HTMLElement} */ (document.getElementById('missTuningStatus'));
  if (el) el.textContent = text || '';
}

/** @param {boolean} disabled */ export function setMissTuningDisabled(disabled) {
  MISS_TUNING_CONTROL_IDS.forEach(function(id) {
    var el = document.getElementById(id);
    if (el instanceof HTMLInputElement || el instanceof HTMLButtonElement) el.disabled = !!disabled;
  });
}

export function invalidateMissPreview() {
  state.previewRequestId++;
  if (state.previewTimer) {
    clearTimeout(state.previewTimer);
    state.previewTimer = null;
  }
}

/** @param {MissBody} body @param {keyof MissConfig} key @param {string} id @param {number} scale */ function addChangedSliderValue(body, key, id, scale) {
  var raw = parseInt(/** @type {HTMLInputElement} */ (document.getElementById(id)).value, 10);
  var baseline = null;
  if (state.missConfig && state.missConfig[key] != null) {
    baseline = Math.round(Number(state.missConfig[key]) * scale);
  }
  if (baseline == null || raw !== baseline) {
    body[key] = raw / scale;
  }
}

function collectMissThresholds() {
  // No miss_enabled toggle on this page: omit it so preview/recompute (and
  // save_defaults) preserve a workspace's effective value instead of silently
  // re-enabling miss detection for workspaces that disabled it.
  /** @type {MissBody} */ var body = {};
  addChangedSliderValue(body, 'detector_confidence', 'missCfgDetectorFloor', 100);
  addChangedSliderValue(body, 'miss_det_confidence', 'missCfgNoSubject', 100);
  addChangedSliderValue(body, 'miss_classifier_override_conf', 'missCfgClassifierRescue', 100);
  addChangedSliderValue(body, 'miss_bbox_area_min', 'missCfgBboxMin', 1000);
  addChangedSliderValue(body, 'miss_oof_ratio', 'missCfgOofRatio', 100);
  return body;
}

function updateMissSliderLabels() {
  var det = parseInt(/** @type {HTMLInputElement} */ (document.getElementById('missCfgDetectorFloor')).value, 10);
  var ns = parseInt(/** @type {HTMLInputElement} */ (document.getElementById('missCfgNoSubject')).value, 10);
  var rescue = parseInt(/** @type {HTMLInputElement} */ (document.getElementById('missCfgClassifierRescue')).value, 10);
  var bbox = parseInt(/** @type {HTMLInputElement} */ (document.getElementById('missCfgBboxMin')).value, 10);
  var oof = parseInt(/** @type {HTMLInputElement} */ (document.getElementById('missCfgOofRatio')).value, 10);
  /** @type {HTMLElement} */ (document.getElementById('missCfgDetectorFloorVal')).textContent = det + '%';
  /** @type {HTMLElement} */ (document.getElementById('missCfgNoSubjectVal')).textContent = ns + '%';
  /** @type {HTMLElement} */ (document.getElementById('missCfgClassifierRescueVal')).textContent = rescue > 100 ? 'off' : rescue + '%';
  /** @type {HTMLElement} */ (document.getElementById('missCfgBboxMinVal')).textContent = (bbox / 1000).toFixed(3);
  /** @type {HTMLElement} */ (document.getElementById('missCfgOofRatioVal')).textContent = (oof / 100).toFixed(2);
}

/** @param {MissConfig} config */ function setMissSliders(config) {
  /** @type {HTMLInputElement} */ (document.getElementById('missCfgDetectorFloor')).value = String(Math.round((config.detector_confidence || 0) * 100));
  /** @type {HTMLInputElement} */ (document.getElementById('missCfgNoSubject')).value = String(Math.round((config.miss_det_confidence || 0) * 100));
  /** @type {HTMLInputElement} */ (document.getElementById('missCfgClassifierRescue')).value = String(Math.round((config.miss_classifier_override_conf || 0) * 100));
  /** @type {HTMLInputElement} */ (document.getElementById('missCfgBboxMin')).value = String(Math.round((config.miss_bbox_area_min || 0) * 1000));
  /** @type {HTMLInputElement} */ (document.getElementById('missCfgOofRatio')).value = String(Math.round((config.miss_oof_ratio || 0) * 100));
  updateMissSliderLabels();
}

export function installMissSliderHandlers() {
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

export async function loadMissConfig() {
  state.missConfigLoaded = false;
  setMissTuningDisabled(true);
  try {
    var r = await fetch('/api/misses/config');
    if (!r.ok) throw new Error('http ' + r.status);
    state.missConfig = await r.json();
    state.originalMissConfig = Object.assign({}, state.missConfig);
    if (state.missConfig) setMissSliders(state.missConfig);
    state.missConfigLoaded = true;
    // Keep the tuning group frozen when a fail-closed scope is in effect;
    // loadMisses() will re-enable it once a user-supplied replacement
    // filter loads successfully.
    if (!state.legacyScopeError && !state.legacyScopeRecoveryPending) {
      setMissTuningDisabled(false);
    }
    setMissTuningStatus('Preview uses current sliders');
  } catch (e) {
    setMissTuningStatus('Thresholds unavailable');
  }
}

function scheduleMissPreview() {
  if (!state.missConfigLoaded) return;
  if (state.legacyScopeError || state.legacyScopeRecoveryPending) return;
  if (state.previewTimer) clearTimeout(state.previewTimer);
  state.previewTimer = setTimeout(previewMisses, 250);
}

export async function previewMisses() {
  if (!state.missConfigLoaded) return;
  // Defense in depth: sliders are disabled while a deep-link scope is
  // unresolved, but any caller that reaches here anyway must not POST the
  // empty rule payload to /api/misses/preview and repopulate the grid with
  // photos outside the user's scope.
  if (state.legacyScopeError || state.legacyScopeRecoveryPending) return;
  if (state.previewTimer) {
    clearTimeout(state.previewTimer);
    state.previewTimer = null;
  }
  var requestId = ++state.previewRequestId;
  state.missLoadRequestId++;
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
    if (requestId !== state.previewRequestId) return;
    state.appliedMissFilters = cloneMissFilterPayload(requestedFilters);
    state.previewActive = true;
    state.previewPayload = data;
    applyMissesPayload(data);
    setMissTuningStatus('Preview only');
  } catch (e) {
    if (requestId !== state.previewRequestId) return;
    setMissTuningStatus('Preview failed');
  }
}

export async function recomputeMisses() {
  if (!state.missConfigLoaded) return;
  if (state.legacyScopeError || state.legacyScopeRecoveryPending) return;
  invalidateMissPreview();
  var requestId = ++state.missLoadRequestId;
  var btn = /** @type {HTMLButtonElement} */ (document.getElementById('missRecomputeBtn'));
  // Recompute the photos that produced the cards the user is looking at.
  // Live controls may already represent a newer filter whose load is pending.
  var requestedFilters = cloneMissFilterPayload(state.appliedMissFilters);
  var body = collectMissThresholds();
  addFilterPayloadToBody(body, requestedFilters);
  var since = getSinceParam();
  if (since) body.since = since;
  body.save_defaults = !!/** @type {HTMLInputElement} */ (document.getElementById('missSaveDefaults')).checked;
  if (btn) btn.disabled = true;
  state.previewActive = false;
  state.previewPayload = null;
  setMissTuningStatus('Recomputing...');
  try {
    var r = await fetch('/api/misses/recompute', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!r.ok) throw new Error('http ' + r.status);
    var data = await r.json();
    if (requestId !== state.missLoadRequestId) {
      if (data.config && data.saved_defaults) {
        state.missConfig = data.config;
        state.originalMissConfig = Object.assign({}, data.config);
      }
      setMissTuningStatus('Recomputed previous filters; refreshing current view');
      loadMisses();
      return;
    }
    if (!sameMissFilterPayload(missFilterPayload(), requestedFilters)) {
      if (data.config && data.saved_defaults) {
        state.missConfig = data.config;
        state.originalMissConfig = Object.assign({}, data.config);
      }
      setMissTuningStatus('Recomputed visible photos; refreshing filters');
      loadMisses();
      return;
    }
    state.appliedMissFilters = cloneMissFilterPayload(requestedFilters);
    state.previewActive = false;
    state.previewPayload = null;
    applyMissesPayload(data);
    if (data.config && data.saved_defaults) {
      state.missConfig = data.config;
      state.originalMissConfig = Object.assign({}, data.config);
    }
    setMissTuningStatus('Updated ' + (data.updated || 0) + ' photos');
    showToast('Recomputed misses', 'success');
  } catch (e) {
    setMissTuningStatus('Recompute failed');
    showToast('Recompute failed', 'error');
  } finally {
    if (btn && state.missConfigLoaded && !state.legacyScopeError && !state.legacyScopeRecoveryPending) {
      btn.disabled = false;
    }
  }
}

export function resetMissThresholds() {
  if (!state.missConfigLoaded || !state.originalMissConfig) return;
  if (state.legacyScopeError || state.legacyScopeRecoveryPending) return;
  invalidateMissPreview();
  setMissSliders(state.originalMissConfig);
  state.previewActive = false;
  state.previewPayload = null;
  setMissTuningStatus('Preview reset');
  loadMisses();
}
