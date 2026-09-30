/* ---------- Initial load readiness ----------
 * The curated forms are populated asynchronously from /api/config and
 * /api/workspaces/active/config. Tests (and any code that interacts with
 * form fields on load) need a way to know both have finished, otherwise
 * they can edit a field before loadConfig() overwrites it, or trigger
 * saveWsConfig() before _wsOverridesLoaded flips on and get a silent
 * no-op. The body's data-settings-ready attribute flips to "true" after
 * both initial loads have settled (success OR handled failure). */
var _settingsInitialLoads = { config: false, ws: false };
function _markSettingsInitialLoad(kind) {
  _settingsInitialLoads[kind] = true;
  if (_settingsInitialLoads.config && _settingsInitialLoads.ws) {
    document.body.setAttribute('data-settings-ready', 'true');
  }
}

// ---------- Autosave status ----------
// The curated forms on this page (global config and workspace overrides)
// autosave on every change. Each save path reports its lifecycle here so
// the pill at the top-right answers "did my edit persist?" honestly:
//   pending  — a debounced write is queued but not yet sent
//   inflight — the POST is on the wire
//   ok       — the server accepted it
//   error    — the server rejected it or the network failed
// Paths are tracked separately so a failed config save is not masked by a
// later successful workspace-override save (they persist to different
// places; only a retry of the SAME path clears its failure).
//
// Every save carries a monotonically increasing generation token so that
// the completion of an OLDER request cannot clear pending/inflight state
// belonging to a NEWER edit. Without this, editing a second field while a
// POST for the first is in flight would show "Saved" as soon as the first
// completes — during the second edit's debounce window or even while its
// request was already running.
var _saveStatus = { pending: {}, inflight: {}, lastResult: {}, lastSavedAt: null };
var _saveStatusFlashTimer = null;
var _saveGenCounter = 0;

function _nextSaveGen() { return ++_saveGenCounter; }

// Saves for the same path are serialized: the next POST starts only after
// the previous one has settled. Generation tokens order the UI
// bookkeeping, but they cannot order what the server sees — two
// overlapping requests can reach the backend out of order, and the older
// full-form snapshot could land last and silently revert the newer edit
// while the pill says "Saved". With one request in flight per path, the
// server applies snapshots in the order they were sent, and each snapshot
// is read from the form at send time so a queued save always carries the
// latest values.
var _saveChains = {};
var _saveChainEpochs = {};
var _autosaveSuspended = false;
var _editedWhileSuspended = false;
function _serializedSave(path, doSave) {
  var epoch = _saveChainEpochs[path] || 0;
  var prev = _saveChains[path] || Promise.resolve();
  var run = prev.catch(function() {}).then(function() {
    // A save queued behind an in-flight POST reads the form only when its
    // turn comes. If the chain was cancelled meanwhile (settings import
    // replaced the config), that stale snapshot must not be posted.
    if ((_saveChainEpochs[path] || 0) !== epoch) {
      throw new Error('autosave cancelled');
    }
    return doSave();
  });
  _saveChains[path] = run;
  return run;
}
// Drop every save queued for a path that has not started yet, and wait
// for the one in flight (if any) to settle. Used before replacing the
// whole config so no pre-import snapshot can land after the import.
function _cancelQueuedSaves(path) {
  _saveChainEpochs[path] = (_saveChainEpochs[path] || 0) + 1;
  var chain = _saveChains[path] || Promise.resolve();
  _saveStatusMark(path, 'cancel');
  return chain.catch(function() {});
}

function _saveStatusMark(path, phase, gen) {
  if (phase === 'pending') {
    // Debounced writes are singleton per path (clearTimeout drops any
    // older timer), so overwrite rather than accumulate — otherwise a
    // superseded pending gen would linger forever.
    _saveStatus.pending[path] = gen;
  } else if (phase === 'inflight') {
    // Every POST sends the full current form snapshot, so a request at
    // least as new as the queued write already carries its edits. This
    // matters for direct _saveConfigNow() callers (the NAS wizard clears
    // the debounce timer and flushes with a freshly minted gen): without
    // the <= the superseded pending gen would never clear and the pill
    // would read "Saving…" forever.
    if (_saveStatus.pending[path] !== undefined && _saveStatus.pending[path] <= gen) {
      delete _saveStatus.pending[path];
    }
    if (!_saveStatus.inflight[path]) _saveStatus.inflight[path] = {};
    _saveStatus.inflight[path][gen] = true;
  } else if (phase === 'ok' || phase === 'error') {
    if (_saveStatus.inflight[path]) {
      delete _saveStatus.inflight[path][gen];
      if (Object.keys(_saveStatus.inflight[path]).length === 0) {
        delete _saveStatus.inflight[path];
      }
    }
    // Only let the LATEST completion define the visible outcome. An older
    // 'ok' arriving after a newer 'error' must not overwrite the failure,
    // and vice versa. Ties (same gen) update in call order.
    var prev = _saveStatus.lastResult[path];
    if (!prev || gen >= prev.gen) {
      _saveStatus.lastResult[path] = { gen: gen, phase: phase };
    }
    if (phase === 'ok') _saveStatus.lastSavedAt = new Date();
  } else if (phase === 'cancel') {
    // A queued write was dropped on purpose (settings import replaces the
    // whole config). Nothing was persisted, so don't claim a save happened.
    delete _saveStatus.pending[path];
  }
  _renderSaveStatus();
}

function _renderSaveStatus() {
  var el = document.getElementById('settingsSaveStatus');
  if (!el) return;
  var busy = Object.keys(_saveStatus.pending).length > 0 ||
             Object.keys(_saveStatus.inflight).length > 0;
  var failed = Object.keys(_saveStatus.lastResult).some(function(p) {
    return _saveStatus.lastResult[p].phase === 'error';
  });
  clearTimeout(_saveStatusFlashTimer);
  el.classList.remove('flash');
  if (busy) {
    el.setAttribute('data-state', 'saving');
    el.textContent = 'Saving\u2026';
    el.title = 'Your change is being written to disk.';
  } else if (failed) {
    el.setAttribute('data-state', 'error');
    el.textContent = 'Save failed \u2014 changes not saved';
    el.title = 'The last save was rejected. Edit the field again to retry.';
  } else if (_saveStatus.lastSavedAt) {
    var t = _saveStatus.lastSavedAt.toLocaleTimeString([], {
      hour: '2-digit', minute: '2-digit', second: '2-digit'
    });
    el.setAttribute('data-state', 'saved');
    el.textContent = 'Saved \u2713 ' + t;
    el.title = 'All changes on this page are saved automatically. Last saved at ' + t + '.';
    el.classList.add('flash');
    _saveStatusFlashTimer = setTimeout(function() { el.classList.remove('flash'); }, 2000);
  } else {
    el.removeAttribute('data-state');
    el.textContent = '';
    el.title = '';
  }
}

var _saveTimer = null;
