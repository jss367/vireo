/* ---------- Workspace Config Overrides ---------- */
var _wsOverridesLoaded = false;
async function loadWsOverrides() {
  try {
    var ws = await safeFetch('/api/workspaces/active', {}, { toast: false });
    if (ws && ws.name) {
      document.querySelectorAll('.ws-override-name').forEach(function(el) { el.textContent = ws.name; });
    }
    var overrides = await safeFetch('/api/workspaces/active/config', {}, { toast: false });
    ['classification_threshold', 'grouping_window_seconds', 'similarity_threshold', 'detector_confidence'].forEach(function(key) {
      _applyWsOverrideField(key, overrides);
    });
    _wsOverridesLoaded = true;
  } catch(e) {
    _markSettingsInitialLoad('ws');
    return false;
  }
  _markSettingsInitialLoad('ws');
  return true;
}

// Show one override row exactly as the server has it.
function _applyWsOverrideField(key, overrides) {
  var checkbox = document.getElementById('wsOverride_' + key);
  var input = document.getElementById('wsVal_' + key);
  var ctrl = document.getElementById('wsOverrideCtrl_' + key);
  if (!checkbox || !input || !ctrl) return;
  if (overrides[key] !== undefined) {
    checkbox.checked = true;
    ctrl.style.display = 'flex';
    if (key === 'classification_threshold' || key === 'similarity_threshold' || key === 'detector_confidence') {
      input.value = Math.round(overrides[key] * 100);
    } else {
      input.value = overrides[key];
    }
    updateWsLabel(key);
  } else {
    // Sync removal too — when re-called after a schema-row reset, an
    // override may have been deleted server-side. Leaving the checkbox
    // checked would cause the next saveWsConfig to re-add it.
    checkbox.checked = false;
    ctrl.style.display = 'none';
  }
}

function updateWsLabel(key) {
  var input = document.getElementById('wsVal_' + key);
  if (!input) return;
  if (key === 'classification_threshold') {
    document.getElementById('wsThresholdVal').textContent = input.value + '%';
  } else if (key === 'similarity_threshold') {
    document.getElementById('wsSimilarityVal').textContent = input.value + '%';
  } else if (key === 'detector_confidence') {
    document.getElementById('wsDetectorConfVal').textContent = (parseInt(input.value) / 100).toFixed(2);
  }
}

function toggleWsOverride(key) {
  var checkbox = document.getElementById('wsOverride_' + key);
  var input = document.getElementById('wsVal_' + key);
  var ctrl = document.getElementById('wsOverrideCtrl_' + key);
  if (!checkbox || !input || !ctrl) return;
  ctrl.style.display = checkbox.checked ? 'flex' : 'none';
  if (checkbox.checked && !input.value) {
    if (key === 'classification_threshold') input.value = 40;
    else if (key === 'grouping_window_seconds') input.value = 10;
    else if (key === 'similarity_threshold') input.value = 85;
    else if (key === 'detector_confidence') input.value = 20;
    updateWsLabel(key);
  }
  saveWsConfig();
}

var _wsSaveTimer = null;
function saveWsConfig() {
  // If the initial load failed, every checkbox shows unchecked even though the
  // server may have overrides. Saving now would send null for every key and
  // wipe persisted state we never displayed. Bail out instead.
  if (!_wsOverridesLoaded) return;
  clearTimeout(_wsSaveTimer);
  var gen = _nextSaveGen();
  _saveStatusMark('workspace', 'pending', gen);
  _wsSaveTimer = setTimeout(function() {
    _serializedSave('workspace', function() { return _postWsOverrides(gen); });
  }, 500);
}

async function _postWsOverrides(gen) {
    // Runs only once any earlier override POST has settled; reads the form
    // now so the snapshot reflects every edit made while waiting.
    var overrides = {};
    var needsResync = false;
    var invalidKeys = [];
    ['classification_threshold', 'grouping_window_seconds', 'similarity_threshold', 'detector_confidence'].forEach(function(key) {
      var checkbox = document.getElementById('wsOverride_' + key);
      var input = document.getElementById('wsVal_' + key);
      if (!checkbox || !input) return;
      if (checkbox.checked) {
        var parsed = parseInt(input.value, 10);
        if (isNaN(parsed)) {
          // Cleared/invalid number field: JSON.stringify turns NaN into
          // null, which the backend reads as "delete this override" while
          // the checkbox stays checked — the UI would claim an override
          // that no longer exists. Omit the key instead (the backend
          // preserves omitted keys) and resync the input from the server
          // afterwards so it shows the value that is actually in effect.
          needsResync = true;
          invalidKeys.push(key);
          return;
        }
        if (key === 'classification_threshold' || key === 'similarity_threshold' || key === 'detector_confidence') {
          overrides[key] = parsed / 100;
        } else {
          overrides[key] = parsed;
        }
      } else {
        // Explicit null tells the backend to clear this override.
        // Without this, omitted keys are preserved and unchecking has no effect.
        overrides[key] = null;
      }
    });
    _saveStatusMark('workspace', 'inflight', gen);
    try {
      await safeFetch('/api/workspaces/active/config', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(overrides)
      });
    } catch(e) {
      _saveStatusMark('workspace', 'error', gen);
      return;
    }
    if (needsResync) {
      // An omitted (blank/invalid) field was NOT saved. Don't claim
      // "Saved" until the form shows what is actually in effect: restore
      // the field from the server first, and tell the user their entry
      // was dropped. If the resync itself fails, the form still shows an
      // unsaved value, so report that as a failed save rather than a
      // false confirmation.
      var current;
      try {
        current = await safeFetch('/api/workspaces/active/config', {}, { toast: false });
      } catch (e) {
        _saveStatusMark('workspace', 'error', gen);
        return;
      }
      // Only the omitted fields are restored, and only if no newer
      // override edit was queued while the GET was in flight: that edit's
      // save reads the form when its turn comes, so overwriting fields
      // here would post the server's old value over the user's newer one.
      if (_saveStatus.pending.workspace === undefined) {
        invalidKeys.forEach(function(key) { _applyWsOverrideField(key, current); });
        if (typeof showToast === 'function') {
          showToast(
            'Blank or invalid override for ' + invalidKeys.join(', ') +
            ' was not saved; showing the value currently in effect.',
            'warning'
          );
        }
      }
    }
    _saveStatusMark('workspace', 'ok', gen);
}
