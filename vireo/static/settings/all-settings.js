// --- All settings (schema-rendered, editable) ------------------------------

var ALL_SETTINGS_CACHE = { schema: null, categories: null, values: null };
// Keyed by `<key>|<scope>` so a quick edit to the same key in a different
// scope tab can't clear the pending write of the first scope's edit.
var ALL_SETTINGS_DEBOUNCE = {};
var ALL_SETTINGS_SCOPE = 'global';  // 'global' | 'workspace'

function setSettingsScope(scope) {
  if (scope !== 'global' && scope !== 'workspace') return;
  if (ALL_SETTINGS_SCOPE === scope) return;
  ALL_SETTINGS_SCOPE = scope;
  document.querySelectorAll('.scope-tab').forEach(function(b) {
    b.classList.toggle('active', b.dataset.scope === scope);
  });
  renderAllSettings(document.getElementById('allSettingsCategories'));
  filterAllSettings();
}

async function loadAllSettings() {
  var container = document.getElementById('allSettingsCategories');
  try {
    var [schemaRes, valuesRes] = await Promise.all([
      fetch('/api/settings/schema'),
      fetch('/api/settings/values'),
    ]);
    if (!schemaRes.ok || !valuesRes.ok) throw new Error('HTTP error');
    var schemaData = await schemaRes.json();
    var values = await valuesRes.json();
    ALL_SETTINGS_CACHE.schema = schemaData.schema;
    ALL_SETTINGS_CACHE.categories = schemaData.categories;
    ALL_SETTINGS_CACHE.values = values;
    renderAllSettings(container);
    filterAllSettings();
  } catch (err) {
    container.innerHTML = '<span style="color:var(--danger);font-size:13px;">Failed to load settings: '
      + escapeHtml(err && err.message || String(err)) + '</span>';
  }
}

async function refreshAllSettingsValues() {
  var r = await fetch('/api/settings/values');
  if (!r.ok) return;
  ALL_SETTINGS_CACHE.values = await r.json();
}

function renderAllSettings(container) {
  var schema = ALL_SETTINGS_CACHE.schema;
  var categories = ALL_SETTINGS_CACHE.categories;
  var values = ALL_SETTINGS_CACHE.values;
  if (!schema || !categories || !values) return;

  var byCategory = {};
  categories.forEach(function(c) { byCategory[c] = []; });
  Object.keys(schema).sort().forEach(function(key) {
    var cat = schema[key].category || 'Other';
    if (!byCategory[cat]) byCategory[cat] = [];
    byCategory[cat].push(key);
  });

  var parts = [];
  categories.forEach(function(cat) {
    var keys = byCategory[cat];
    if (!keys || !keys.length) return;
    parts.push('<div class="setting-category" data-category="' + escapeAttr(cat) + '" style="margin-top:14px;">');
    parts.push('<div style="font-size:13px;font-weight:600;color:var(--text-secondary);margin-bottom:6px;border-bottom:1px solid var(--border-primary);padding-bottom:4px;">'
      + escapeHtml(cat) + '</div>');
    keys.forEach(function(key) {
      parts.push(renderSettingRow(key));
    });
    parts.push('</div>');
  });
  container.innerHTML = parts.join('');
}

function _settingControlSelector(el) {
  if (!el || !el.getAttribute) return null;
  var inputKey = el.getAttribute('data-input-key');
  if (inputKey) return '[data-input-key="' + cssEscape(inputKey) + '"]';
  var listKey = el.getAttribute('data-list-key');
  var listItem = el.getAttribute('data-list-item');
  if (listKey && listItem != null) {
    return '[data-list-key="' + cssEscape(listKey) + '"][data-list-item="' + cssEscape(listItem) + '"]';
  }
  return null;
}

function rerenderSettingRow(key) {
  var row = document.querySelector('#allSettingsCategories .setting-row-card[data-key="' + cssEscape(key) + '"]');
  if (!row) return;
  // A save lands asynchronously, and the user may be back in this row's
  // field. Replacing the row would swallow the keystrokes typed since, so
  // leave a row that is being edited alone; its own change event saves it
  // and re-renders then. Otherwise keep focus where it was.
  var active = document.activeElement;
  var focusSelector = (active && row.contains(active)) ? _settingControlSelector(active) : null;
  if (focusSelector && active.dataset && active.dataset.dirty) return;
  var tmp = document.createElement('div');
  tmp.innerHTML = renderSettingRow(key);
  var fresh = tmp.firstElementChild;
  if (!fresh) return;
  row.replaceWith(fresh);
  if (focusSelector) {
    var target = fresh.querySelector(focusSelector);
    if (target) {
      target.focus();
      try {
        var end = String(target.value || '').length;
        target.setSelectionRange(end, end);
      } catch (e) {}  // number / checkbox inputs have no selection range
    }
  }
}

function cssEscape(s) {
  if (window.CSS && window.CSS.escape) return window.CSS.escape(s);
  return String(s).replace(/[^a-zA-Z0-9_-]/g, '\\$&');
}

function renderSettingRow(key) {
  var spec = ALL_SETTINGS_CACHE.schema[key];
  var values = ALL_SETTINGS_CACHE.values;
  var hasWs = Object.prototype.hasOwnProperty.call(values.workspace, key);
  var hasGlobal = Object.prototype.hasOwnProperty.call(values['global'], key);
  var globalOnly = (spec.scope === 'global');
  var workspaceTab = (ALL_SETTINGS_SCOPE === 'workspace');

  var dotClass, dotTitle;
  if (workspaceTab) {
    if (hasWs) {
      dotClass = 'dot-workspace';
      dotTitle = 'Set for this workspace';
    } else if (globalOnly) {
      dotClass = hasGlobal ? 'dot-global' : 'dot-default';
      dotTitle = hasGlobal ? 'Global setting (not workspace-overridable)' : 'Global default';
    } else {
      dotClass = 'dot-default';
      dotTitle = hasGlobal ? 'Inheriting global value' : 'Using default';
    }
  } else if (hasGlobal) {
    dotClass = 'dot-global';
    dotTitle = 'Set globally';
  } else {
    dotClass = 'dot-default';
    dotTitle = 'Using default';
  }

  // Effective value for the widget on this tab.
  var rowValue;
  if (workspaceTab) {
    if (hasWs) rowValue = values.workspace[key];
    else if (hasGlobal) rowValue = values['global'][key];
    else rowValue = values['default'][key];
  } else {
    rowValue = hasGlobal ? values['global'][key] : values['default'][key];
  }

  var defaultVal = values['default'][key];
  var widget;
  if (workspaceTab && globalOnly) {
    widget = '<span style="font-size:11px;color:var(--text-ghost);font-style:italic;">'
           + 'Global only — switch to Global tab</span>';
  } else {
    widget = renderSettingWidget(key, spec, rowValue);
  }

  var hasOverride = workspaceTab ? hasWs : hasGlobal;
  var resetBtn = '';
  if (hasOverride) {
    var resetTitle = workspaceTab
      ? 'Remove workspace override (inherit global / default)'
      : 'Reset to default: ' + formatSettingValue(defaultVal, spec, true);
    resetBtn = '<button type="button" onclick="resetSchemaSetting(\'' + escapeAttr(key) + '\')" '
             +    'title="' + escapeAttr(resetTitle) + '" '
             +    'style="background:transparent;border:1px solid var(--border-secondary);color:var(--text-dim);border-radius:4px;padding:2px 8px;font-size:11px;cursor:pointer;flex-shrink:0;">'
             +  'Reset</button>';
  }

  var meta = escapeHtml(key);
  if (workspaceTab && hasWs) {
    var inheritedVal = hasGlobal ? values['global'][key] : defaultVal;
    var inheritLabel = hasGlobal ? 'inherits global' : 'inherits default';
    meta += ' · ' + inheritLabel + ': ' + escapeHtml(formatSettingValue(inheritedVal, spec, true));
  } else if (!workspaceTab && hasGlobal) {
    meta += ' · default: ' + escapeHtml(formatSettingValue(defaultVal, spec, true));
  }
  var search = key + ' ' + spec.label + ' ' + spec.desc + ' ' + spec.category;
  return '<div class="setting-row-card" data-search="' + escapeAttr(search) + '" data-key="' + escapeAttr(key) + '" '
       +    'style="display:flex;align-items:flex-start;gap:10px;padding:10px 0;border-bottom:1px solid var(--border-primary);">'
       +   '<span class="provenance-dot ' + dotClass + '" title="' + escapeAttr(dotTitle) + '"></span>'
       +   '<div style="flex:1;min-width:0;">'
       +     '<div style="display:flex;justify-content:space-between;gap:12px;align-items:center;flex-wrap:wrap;">'
       +       '<div style="font-size:13px;color:var(--text-primary);font-weight:500;flex:1;min-width:140px;">' + escapeHtml(spec.label) + '</div>'
       +       '<div style="display:flex;align-items:center;gap:8px;flex-shrink:0;">'
       +         widget
       +         resetBtn
       +       '</div>'
       +     '</div>'
       +     '<div style="font-size:11px;color:var(--text-dim);margin-top:2px;">' + escapeHtml(spec.desc) + '</div>'
       +     '<div style="display:flex;justify-content:space-between;gap:8px;margin-top:2px;">'
       +       '<div style="font-size:11px;color:var(--text-ghost);font-family:monospace;">' + meta + '</div>'
       +       '<div class="setting-row-status" style="font-size:11px;color:var(--text-dim);min-height:14px;"></div>'
       +     '</div>'
       +   '</div>'
       + '</div>';
}

function renderSettingWidget(key, spec, effective) {
  var inputStyle = 'background:var(--bg-input);color:var(--text-primary);border:1px solid var(--border-secondary);border-radius:4px;padding:3px 8px;font-size:12px;';
  var keyAttr = 'data-input-key="' + escapeAttr(key) + '"';
  if (spec.type === 'bool') {
    return '<input type="checkbox" ' + keyAttr + ' '
         +     (effective ? 'checked ' : '')
         +     'onchange="onSchemaInputChange(this, true)" '
         +     'style="accent-color:var(--accent);transform:scale(1.1);">';
  }
  if (spec.type === 'enum') {
    var opts = (spec.enum || []).map(function(opt) {
      var label = (spec.enum_labels && spec.enum_labels[opt]) ? spec.enum_labels[opt] : opt;
      var sel = (opt === effective) ? ' selected' : '';
      return '<option value="' + escapeAttr(opt) + '"' + sel + '>' + escapeHtml(label) + '</option>';
    }).join('');
    if (spec.nullable) {
      // Prepend the null option (value="" — the settings PATCH endpoint
      // treats "" as null when spec.nullable is true) so users can pick
      // "unset" as a first-class choice even after a non-null default has
      // been assigned globally.
      var nullLabel = spec.null_label || '(unset)';
      var nullSel = (effective == null) ? ' selected' : '';
      opts = '<option value=""' + nullSel + '>' + escapeHtml(nullLabel) + '</option>' + opts;
    }
    return '<select ' + keyAttr + ' onchange="onSchemaInputChange(this, true)" '
         +    'style="' + inputStyle + 'min-width:120px;">'
         +   opts + '</select>';
  }
  // Typed fields (number, secret, text) save on `change` (blur or Enter), not
  // per keystroke: a debounced save while typing wrote partial values such as
  // half a path to config, and for the working-copy limit raised the
  // "remove ~X GB" confirm after the first digit. `dirty` marks a field with
  // unsaved typing so rerenderSettingRow won't replace it mid-edit.
  if (spec.type === 'int' || spec.type === 'float') {
    var step = spec.step != null ? spec.step : (spec.type === 'int' ? 1 : 0.01);
    var min = spec.min != null ? ' min="' + spec.min + '"' : '';
    var max = spec.max != null ? ' max="' + spec.max + '"' : '';
    var val = (effective != null) ? effective : '';
    return '<input type="number" ' + keyAttr + ' value="' + escapeAttr(String(val)) + '" '
         +     'step="' + step + '"' + min + max + ' '
         +     'oninput="this.dataset.dirty = \'1\'" '
         +     'onchange="delete this.dataset.dirty; onSchemaInputChange(this, true)" '
     +     'onblur="delete this.dataset.dirty" '
         +     'style="' + inputStyle + 'width:100px;text-align:right;">';
  }
  if (spec.type === 'secret') {
    var sval = (effective != null) ? String(effective) : '';
    return '<input type="password" ' + keyAttr + ' value="' + escapeAttr(sval) + '" '
         +     'placeholder="(unset)" autocomplete="off" '
         +     'oninput="this.dataset.dirty = \'1\'" '
         +     'onchange="delete this.dataset.dirty; onSchemaInputChange(this, true)" '
     +     'onblur="delete this.dataset.dirty" '
         +     'style="' + inputStyle + 'width:200px;font-family:monospace;">';
  }
  if (spec.type === 'list_string') {
    if (Array.isArray(spec.items_enum)) {
      var current = Array.isArray(effective) ? effective : [];
      var checks = spec.items_enum.map(function(item) {
        var checked = current.indexOf(item) !== -1 ? ' checked' : '';
        return '<label style="display:inline-flex;align-items:center;gap:4px;font-size:11px;color:var(--text-secondary);cursor:pointer;">'
             +   '<input type="checkbox" data-list-item="' + escapeAttr(item) + '" data-list-key="' + escapeAttr(key) + '"' + checked
             +     ' onchange="onSchemaListChange(\'' + escapeAttr(key) + '\')" style="accent-color:var(--accent);">'
             +   escapeHtml(item)
             + '</label>';
      }).join(' ');
      return '<div style="display:flex;flex-wrap:wrap;gap:6px;max-width:380px;justify-content:flex-end;">' + checks + '</div>';
    }
    // Read-only fallback for list_string without items_enum (e.g. scan_roots).
    var asText = (Array.isArray(effective) && effective.length) ? effective.join(', ') : '(empty)';
    return '<span style="font-size:12px;color:var(--text-secondary);font-family:monospace;max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;display:inline-block;" title="' + escapeAttr(asText) + '">'
         +   escapeHtml(asText)
         + '</span>'
         + '<span style="font-size:10px;color:var(--text-ghost);margin-left:6px;">(edit above)</span>';
  }
  // string / path / fallback
  var tval = (effective != null) ? String(effective) : '';
  return '<input type="text" ' + keyAttr + ' value="' + escapeAttr(tval) + '" '
       +     'placeholder="(empty)" '
       +     'oninput="this.dataset.dirty = \'1\'" '
         +     'onchange="delete this.dataset.dirty; onSchemaInputChange(this, true)" '
     +     'onblur="delete this.dataset.dirty" '
       +     'style="' + inputStyle + 'width:240px;font-family:monospace;">';
}

function onSchemaInputChange(el, immediate) {
  var key = el.getAttribute('data-input-key');
  if (!key) return;
  var spec = ALL_SETTINGS_CACHE.schema[key];
  var raw;
  if (spec.type === 'bool') {
    raw = el.checked;
  } else if (spec.type === 'int' || spec.type === 'float') {
    raw = el.value;     // server coerces strings to numbers
  } else {
    raw = el.value;
  }
  scheduleSchemaSave(key, raw, immediate);
}

function onSchemaListChange(key) {
  var checked = [];
  document.querySelectorAll('input[type="checkbox"][data-list-key="' + cssEscape(key) + '"]:checked').forEach(function(cb) {
    checked.push(cb.getAttribute('data-list-item'));
  });
  scheduleSchemaSave(key, checked, true);
}

function scheduleSchemaSave(key, value, immediate) {
  // Capture the scope at edit time so a tab switch during the debounce window
  // doesn't redirect the pending write to the wrong layer.
  var scope = ALL_SETTINGS_SCOPE;
  var token = key + '|' + scope;
  if (ALL_SETTINGS_DEBOUNCE[token]) {
    clearTimeout(ALL_SETTINGS_DEBOUNCE[token]);
  }
  var delay = immediate ? 0 : 300;
  ALL_SETTINGS_DEBOUNCE[token] = setTimeout(function() {
    ALL_SETTINGS_DEBOUNCE[token] = null;
    saveSchemaSetting(key, value, scope);
  }, delay);
}

function _settingsScopeBaseFor(scope) {
  return (scope === 'workspace') ? '/api/settings/workspace' : '/api/settings/global';
}

function confirmWorkingCopyQuotaReduction(data) {
  var quotaMb = Number(data.requested_working_copy_quota_mb);
  var usageBytes = Number(data.current_working_copy_usage_bytes);
  var quotaBytes = Number.isFinite(quotaMb) ? quotaMb * 1024 * 1024 : 0;
  var removalBytes = Number.isFinite(usageBytes)
    ? Math.max(0, usageBytes - quotaBytes)
    : null;
  var detail = removalBytes > 0
    ? 'This will remove approximately ' + formatBytes(removalBytes) +
      ' of the oldest generated working copies.'
    : 'If current usage exceeds the new limit, Vireo will remove the oldest generated working copies.';
  return confirm(
    'Lower the working-copy storage limit to ' + formatBytes(quotaBytes) + '?\n\n' +
    detail + '\n\nYour original photos will not be deleted.'
  );
}

async function saveSchemaSetting(key, value, scope, confirmedEviction) {
  if (!scope) scope = ALL_SETTINGS_SCOPE;
  setRowStatus(key, 'Saving…', false);
  try {
    var payload = { key: key, value: value };
    if (confirmedEviction) payload._confirm_working_copy_eviction = true;
    var resp = await fetch(_settingsScopeBaseFor(scope), {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (!resp.ok) {
      var err = await resp.json().catch(function() { return { error: resp.statusText }; });
      if (
        resp.status === 409 && !confirmedEviction &&
        err.code === 'working_copy_eviction_confirmation_required'
      ) {
        if (confirmWorkingCopyQuotaReduction(err)) {
          return saveSchemaSetting(key, value, scope, true);
        }
        await refreshAllSettingsValues();
        rerenderSettingRow(key);
        setRowStatus(key, 'Not changed', false);
        return;
      }
      setRowStatus(key, err.error || 'Save failed', true);
      return;
    }
    await refreshAllSettingsValues();
    rerenderSettingRow(key);
    // The curated forms above this panel post full snapshots from their own
    // inputs. Repopulate those inputs from the server so a later curated
    // edit doesn't overwrite the value we just saved with a stale snapshot —
    // both the global form (`saveConfig`) and the workspace-overrides form
    // (`saveWsConfig`, which sends `null` for unchecked rows and would clear
    // a freshly-saved override).
    if (scope === 'global') await loadConfig();
    else if (scope === 'workspace') await loadWsOverrides();
    setRowStatus(key, '✓ Saved', false);
    setTimeout(function() { setRowStatus(key, '', false); }, 1500);
  } catch (err) {
    setRowStatus(key, 'Network error', true);
  }
}

async function resetSchemaSetting(key, confirmedEviction) {
  setRowStatus(key, 'Resetting…', false);
  try {
    var scope = ALL_SETTINGS_SCOPE;
    // Cancel any pending debounced autosave for this key+scope. Without
    // this, a still-queued PATCH from a recent edit would fire 300ms after
    // the DELETE and re-create the override, making Reset appear to "not
    // stick" depending on click timing.
    var token = key + '|' + scope;
    if (ALL_SETTINGS_DEBOUNCE[token]) {
      clearTimeout(ALL_SETTINGS_DEBOUNCE[token]);
      ALL_SETTINGS_DEBOUNCE[token] = null;
    }
    var resp = await fetch(_settingsScopeBaseFor(scope) + '/' + encodeURIComponent(key), {
      method: 'DELETE',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(confirmedEviction
        ? { _confirm_working_copy_eviction: true }
        : {}),
    });
    if (!resp.ok) {
      var err = await resp.json().catch(function() { return { error: resp.statusText }; });
      if (
        resp.status === 409 && !confirmedEviction &&
        err.code === 'working_copy_eviction_confirmation_required'
      ) {
        if (confirmWorkingCopyQuotaReduction(err)) {
          return resetSchemaSetting(key, true);
        }
        await refreshAllSettingsValues();
        rerenderSettingRow(key);
        setRowStatus(key, 'Not changed', false);
        return;
      }
      setRowStatus(key, err.error || 'Reset failed', true);
      return;
    }
    await refreshAllSettingsValues();
    rerenderSettingRow(key);
    if (scope === 'global') await loadConfig();
    else if (scope === 'workspace') await loadWsOverrides();
  } catch (err) {
    setRowStatus(key, 'Network error', true);
  }
}

function setRowStatus(key, msg, isError) {
  var row = document.querySelector('#allSettingsCategories .setting-row-card[data-key="' + cssEscape(key) + '"]');
  if (!row) return;
  var status = row.querySelector('.setting-row-status');
  if (!status) return;
  status.textContent = msg;
  status.style.color = isError ? 'var(--danger)' : 'var(--text-dim)';
}

function formatSettingValue(v, spec, mask) {
  if (v === undefined || v === null) return '';
  if (spec.type === 'bool') return v ? 'true' : 'false';
  if (spec.type === 'secret') {
    if (!v) return '(unset)';
    return mask ? '••••••••' : String(v);
  }
  if (spec.type === 'enum') {
    if (spec.enum_labels && spec.enum_labels[v]) return spec.enum_labels[v];
    return String(v);
  }
  if (spec.type === 'list_string') {
    if (!Array.isArray(v) || v.length === 0) return '(empty)';
    return v.join(', ');
  }
  if (spec.type === 'string' || spec.type === 'path') {
    if (v === '') return '(empty)';
    return String(v);
  }
  return String(v);
}

async function importSettingsFile(file) {
  if (!file) return;
  if (!confirm('Replace your global Vireo config with "' + file.name + '"?\n\nWorkspace overrides are not affected.')) {
    return;
  }
  var text;
  try {
    text = await file.text();
  } catch (err) {
    alert('Could not read file: ' + (err && err.message || err));
    return;
  }
  // Cancel any queued debounced autosaves first — a PATCH or a curated
  // /api/config / /api/workspaces/active/config snapshot save that fires
  // mid-import would clobber part of the just-restored config.
  Object.keys(ALL_SETTINGS_DEBOUNCE).forEach(function(token) {
    if (ALL_SETTINGS_DEBOUNCE[token]) {
      clearTimeout(ALL_SETTINGS_DEBOUNCE[token]);
      ALL_SETTINGS_DEBOUNCE[token] = null;
    }
  });
  // Only the global config chain is cancelled: the import replaces global
  // config and leaves workspace overrides alone, so an override save may
  // keep going and must not be dropped (the override form is not reloaded
  // afterwards, and a dropped edit would stay on screen unsaved).
  //
  // A curated config save whose debounce already fired may be queued
  // behind an in-flight POST; it would read the pre-import form when its
  // turn came and post that snapshot over the imported config. Drop queued
  // saves, wait for the in-flight one to settle, and keep autosave off
  // until the form has been reloaded from the imported config. Remember
  // whether an edit was dropped so it can be re-queued if the import does
  // not go through (the form would otherwise show an unsaved value).
  var hadPendingConfigSave = _saveStatus.pending.config !== undefined;
  if (typeof _saveTimer !== 'undefined' && _saveTimer) {
    clearTimeout(_saveTimer);
    _saveTimer = null;
  }
  _autosaveSuspended = true;
  await _cancelQueuedSaves('config');
  var imported = false;
  var formReloaded = false;
  var confirmedEviction = false;
  try {
    while (true) {
      var resp = await fetch('/api/settings/import', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          json: text,
          _confirm_working_copy_eviction: confirmedEviction,
        }),
      });
      var data = await resp.json().catch(function() { return {}; });
      if (
        resp.status === 409 && !confirmedEviction &&
        data.code === 'working_copy_eviction_confirmation_required'
      ) {
        if (!confirmWorkingCopyQuotaReduction(data)) return;
        confirmedEviction = true;
        continue;
      }
      if (!resp.ok) {
        var msg = data.error || resp.statusText || 'Import failed';
        if (data.errors) {
          msg += '\n\nFailed keys:\n' + Object.keys(data.errors).map(function(k) {
            return '  ' + k + ': ' + data.errors[k];
          }).join('\n');
        }
        alert(msg);
        return;
      }
      break;
    }
    // The server has committed the import at this point; everything below
    // is UI refresh. Flag it now so a refresh failure can't be mistaken
    // for an aborted import (which would re-post the stale form over it).
    imported = true;
    // The import is a successful write of the whole global config, so a
    // failure left over from an earlier autosave no longer describes what
    // is on disk.
    _saveStatusMark('config', 'ok', _nextSaveGen());
    await refreshAllSettingsValues();
    renderAllSettings(document.getElementById('allSettingsCategories'));
    filterAllSettings();
    // Curated form posts its own snapshot — repull so a later edit doesn't
    // overwrite imported global values with stale form state.
    // loadConfig() swallows fetch failures (it has to on first paint), so
    // check its result rather than assume the form now matches disk.
    formReloaded = await loadConfig();
    if (!formReloaded) return;
    alert('Settings imported. Workspace overrides preserved.');
  } catch (err) {
    alert('Network error: ' + (err && err.message || err));
  } finally {
    if (imported && !formReloaded) {
      // The config on disk is the imported one but the form could not be
      // refreshed from it. Re-enabling autosave here would let the next
      // ordinary edit post the stale form over the import, so keep it off
      // and reload the page, which rebuilds the form from disk.
      alert('Settings imported, but the page could not refresh. Reloading.');
      location.reload();
    } else {
      _autosaveSuspended = false;
      // The import did not replace the config, so the edit we dropped (or
      // one made while the import was running) is still the user's intent
      // and still on screen: save it after all.
      if (!imported && (hadPendingConfigSave || _editedWhileSuspended)) saveConfig();
    }
    _editedWhileSuspended = false;
  }
}

function filterAllSettings() {
  var q = (document.getElementById('allSettingsSearch').value || '').trim();
  var searchOptions = VireoTextSearch.readOptions('allSettings');
  var rows = document.querySelectorAll('#allSettingsCategories .setting-row-card');
  rows.forEach(function(row) {
    row.style.display = (!q || VireoTextSearch.matchesFields(
      row.getAttribute('data-search') || '',
      q,
      searchOptions
    )) ? '' : 'none';
  });
  document.querySelectorAll('#allSettingsCategories .setting-category').forEach(function(cat) {
    var anyVisible = false;
    cat.querySelectorAll('.setting-row-card').forEach(function(r) {
      if (r.style.display !== 'none') anyVisible = true;
    });
    cat.style.display = anyVisible ? '' : 'none';
  });
}
