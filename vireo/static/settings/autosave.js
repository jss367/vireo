/* ---------- Initial load readiness ----------
 * The curated forms are populated asynchronously from /api/config and
 * /api/workspaces/active/config. Tests (and any code that interacts with
 * form fields on load) need a way to know both have finished, otherwise
 * they can edit a field before loadConfig() overwrites it, or trigger
 * saveWsConfig() before _wsOverridesLoaded flips on and get a silent
 * no-op. The body's data-settings-ready attribute flips to "true" after
 * both initial loads have settled (success OR handled failure). */
var _settingsInitialLoads = { config: false, ws: false };
/** @param {'config'|'ws'} kind */ function _markSettingsInitialLoad(kind) {
  _settingsInitialLoads[kind] = true;
  renderSettingsIssues();
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
/** @type {{pending:Record<string,number>;inflight:Record<string,Record<number,boolean>>;lastResult:Record<string,{gen:number;phase:string}>;lastSavedAt:Date|null}} */ var _saveStatus = { pending: {}, inflight: {}, lastResult: {}, lastSavedAt: null };
/** @type {ReturnType<typeof setTimeout>|null} */ var _saveStatusFlashTimer = null;
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
/** @type {Record<string,Promise<unknown>>} */ var _saveChains = {};
/** @type {Record<string,number>} */ var _saveChainEpochs = {};
var _autosaveSuspended = false;
var _editedWhileSuspended = false;
/** @template T @param {string} path @param {()=>Promise<T>} doSave */ function _serializedSave(path, doSave) {
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
/** @param {string} path */ function _cancelQueuedSaves(path) {
  _saveChainEpochs[path] = (_saveChainEpochs[path] || 0) + 1;
  var chain = _saveChains[path] || Promise.resolve();
  _saveStatusMark(path, 'cancel', 0);
  return chain.catch(function() {});
}

/** @param {string} path @param {'pending'|'inflight'|'ok'|'error'|'cancel'} phase @param {number} gen */ function _saveStatusMark(path, phase, gen) {
  if (phase === 'pending') {
    // Debounced writes are singleton per path (clearTimeout drops any
    // older timer), so overwrite rather than accumulate — otherwise a
    // superseded pending gen would linger forever.
    _saveStatus.pending[path] = gen;
  } else if (phase === 'inflight') {
    // A request reads all pending edits when its turn starts. This
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
  var el = /** @type {HTMLElement} */ (document.getElementById('settingsSaveStatus'));
  if (!el) return;
  var busy = Object.keys(_saveStatus.pending).length > 0 ||
             Object.keys(_saveStatus.inflight).length > 0;
  var failed = Object.keys(_saveStatus.lastResult).some(function(p) {
    return _saveStatus.lastResult[p]?.phase === 'error';
  });
  clearTimeout(_saveStatusFlashTimer ?? undefined);
  el.classList.remove('flash');
  if (busy) {
    el.setAttribute('data-state', 'saving');
    el.textContent = String('Saving\u2026');
    el.title = 'Your change is being written to disk.';
  } else if (failed) {
    el.setAttribute('data-state', 'error');
    el.textContent = String('Save failed \u2014 changes not saved');
    el.title = 'Use Retry for the affected settings.';
  } else if (_saveStatus.lastSavedAt) {
    var t = window.VireoTime.time(_saveStatus.lastSavedAt, {
      hour: '2-digit', minute: '2-digit', second: '2-digit'
    });
    el.setAttribute('data-state', 'saved');
    el.textContent = String('Saved \u2713 ' + t);
    el.title = 'All changes on this page are saved automatically. Last saved at ' + t + '.';
    el.classList.add('flash');
    _saveStatusFlashTimer = setTimeout(function() { el.classList.remove('flash'); }, 2000);
  } else {
    el.removeAttribute('data-state');
    el.textContent = String('');
    el.title = '';
  }
}

/** @type {ReturnType<typeof setTimeout>|null} */ var _saveTimer = null;

/** @typedef {Record<string,unknown>} SettingsSnapshot */
/** @type {Record<string,SettingsSnapshot|null>} */
var settingsBaseline = {config:null, workspace:null};
var settingsWorkspaceName = 'Workspace';
/** @type {Record<string,string[]>} */
var settingsFailures = {};
/** @type {Record<string,string[]>} */
var settingsDiscarded = {};

/** @param {SettingsSnapshot} values */
function flattenSettings(values) {
  const {pipeline,...flat} = values;
  for (const [key,value] of Object.entries(pipeline || {})) flat['pipeline.'+key] = value;
  return flat;
}

/** @param {SettingsSnapshot} flat */
function settingsPayload(flat) {
  /** @type {SettingsSnapshot} */ const payload = {};
  /** @type {SettingsSnapshot} */ const pipeline = {};
  for (const [key,value] of Object.entries(flat)) {
    if (key.startsWith('pipeline.')) pipeline[key.slice(9)] = value;
    else payload[key] = value;
  }
  if (Object.keys(pipeline).length) payload.pipeline = pipeline;
  return payload;
}

/** @param {string} scope @param {SettingsSnapshot} values */
function settingsChanges(scope,values) {
  const baseline = settingsBaseline[scope];
  if (!baseline) throw new Error('Settings have not loaded.');
  return Object.fromEntries(Object.entries(values).filter(([key,value]) =>
    JSON.stringify(value) !== JSON.stringify(baseline[key])));
}

/** @param {string} scope @param {SettingsSnapshot} values */
function recordSettingsFailure(scope,values) {
  settingsFailures[scope] = Object.keys(values);
  delete settingsDiscarded[scope];
  renderSettingsIssues();
}

/** @param {string} scope @param {SettingsSnapshot} values */
function commitSettingsChanges(scope,values) {
  Object.assign(settingsBaseline[scope] || {},values);
  delete settingsFailures[scope];
  delete settingsDiscarded[scope];
  const layers = ALL_SETTINGS_CACHE.values;
  if (layers) {
    const layer = scope === 'workspace' ? layers.workspace : layers.global;
    for (const [key,value] of Object.entries(values)) {
      if (!ALL_SETTINGS_CACHE.schema?.[key]) continue;
      const inherits = scope === 'workspace' ? value === null :
        JSON.stringify(value) === JSON.stringify(layers.default[key]);
      if (inherits) delete layer[key];
      else layer[key] = /** @type {SettingsValue} */ (value);
      rerenderSettingRow(key);
    }
    renderOverrideLinks();
  }
  renderSettingsIssues();
}

/** @param {string} scope @param {SettingsSnapshot|null} baseline @param {SettingsSnapshot} prior */
function setSettingsBaseline(scope,baseline,prior = {}) {
  settingsBaseline[scope] = baseline;
  if (baseline && settingsFailures[scope]?.length) {
    const loaded = scope === 'config' ? {...baseline,..._readSecretFields()} : baseline;
    const replaced = (settingsFailures[scope] || []).filter(key =>
      JSON.stringify(prior[key]) !== JSON.stringify(loaded[key]));
    if (replaced.length) settingsDiscarded[scope] = replaced;
    else delete settingsDiscarded[scope];
    delete settingsFailures[scope];
    delete _saveStatus.lastResult[scope];
    _renderSaveStatus();
  }
  const selector = scope === 'workspace' ? '[id^=wsOverride_],[id^=wsVal_]' : '[data-setting-keys]';
  for (const el of Array.from(document.querySelectorAll(/** @type {'div'} */ (selector)))) {
    const row = el.closest('.setting-row') || el.closest('.section');
    if (row instanceof HTMLElement) row.inert = !baseline;
  }
  renderSettingsIssues();
}

/** @param {string} scope */
function settingsReloadButton(scope) {
  const button = document.createElement('button');
  button.type = 'button';
  button.className = 'btn-sm';
  button.textContent = scope === 'config' ? 'Reload global settings' : 'Reload workspace settings';
  button.onclick = async () => {
    button.disabled = true;
    const ok = await (scope === 'config' ? loadConfig() : loadWsOverrides());
    if (ok) button.remove();
    else button.disabled = false;
  };
  return button;
}

function renderSettingsIssues() {
  const container = document.getElementById('settingsSaveIssues');
  if (!container) return;
  container.replaceChildren();
  for (const scope of ['config','workspace']) {
    const name = scope === 'config' ? 'Global' : settingsWorkspaceName;
    const group = document.createElement('div');
    if (!settingsBaseline[scope]) {
      if (!_settingsInitialLoads[scope === 'config' ? 'config' : 'ws']) continue;
      group.append(name+' settings could not be loaded. ',settingsReloadButton(scope));
    } else {
      const keys = settingsFailures[scope] || settingsDiscarded[scope];
      if (!keys) continue;
      group.append(name + (settingsFailures[scope] ? ' changes could not be saved.' : ' unsaved changes were replaced by stored values.'));
      for (const key of keys) {
        const el = document.querySelector(/** @type {'div'} */ ('[data-setting-keys~="'+cssEscape(key)+'"]'));
        const panel = el?.closest('section');
        if (!el || !panel) continue;
        const link = document.createElement('a');
        link.href = '?category='+panel.dataset.settingsCategory+'#'+(scope === 'workspace' ? 'wsOverride_'+key : el.id);
        link.textContent = settingsLabel(el);
        link.onclick = event => {event.preventDefault(); goToSettings(panel.dataset.settingsCategory || '',link.hash.slice(1));};
        group.append(link);
      }
      if (settingsFailures[scope]) {
        const retry = document.createElement('button');
        retry.className = 'btn-sm';
        retry.textContent = 'Retry';
        retry.setAttribute('aria-label','Retry '+name+' changes');
        retry.onclick = () => scope === 'config' ? saveConfig() : saveWsConfig();
        group.append(retry);
      }
    }
    container.append(group);
  }
}
