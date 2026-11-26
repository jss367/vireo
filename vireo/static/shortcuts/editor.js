// The shortcuts editor: rendering each context's bindings, conflict and
// bare-key checks, resets, saving, and the first load from /api/config.
// ES module.

import { state } from './state.js';
import { RAPID_SHARED, SHORTCUT_DEFAULTS, SHORTCUT_LABELS, defaultsFor, effectiveRapidBindings } from './labels.js';
import { formatShortcut, parseShortcut, safeFetch } from './shared.js';

const RESET_ALL_STYLE = 'background:none;border:1px solid var(--text-ghost);color:var(--text-dim);'
  + 'border-radius:4px;padding:2px 10px;font-size:11px;cursor:pointer;';

/** @param {ShortcutConfig} shortcuts @param {string} ctx */
function contextHtml(shortcuts, ctx) {
  var labels = SHORTCUT_LABELS[ctx] || {};
  var values = shortcuts[ctx] || {};
  var defaults = SHORTCUT_DEFAULTS[ctx] || {};
  var html = '<div class="shortcut-context-title">' + labels._title +
    '<button type="button" data-shortcut-reset-context="' + ctx + '" style="' + RESET_ALL_STYLE + '">Reset All</button></div>';
  for (var action in labels) {
    if (action === '_title') continue;
    const override = ctx === 'pipeline_rapid_review' && action in RAPID_SHARED;
    if (override && !(action in values)) continue;
    // Presence-checked so an explicit '' (e.g. server-side migration
    // resolved a conflicting default) renders as "Unassigned" instead of
    // silently falling through to the default key. Otherwise the button
    // shows the default keycap but the binding does nothing.
    var current = (action in values) ? values[action] : (defaults[action] || '');
    var isDefault = current === defaults[action];
    var conflict = findConflict(ctx, action, current);
    html += '<div class="shortcut-row">';
    html += '<span class="shortcut-action">' + labels[action] + '</span>';
    html += '<button type="button" class="shortcut-key-btn" data-ctx="' + ctx + '" data-action="' + action + '">' +
      formatShortcut(current) + '</button>';
    html += '<span class="shortcut-default-hint">' + (isDefault ? '' : 'default: ' + (override ? 'shared photo shortcut' : formatShortcut(defaults[action]))) + '</span>';
    if (!isDefault) {
      html += '<button type="button" class="shortcut-reset-btn" data-ctx="' + ctx + '" data-action="' + action + '" title="Reset to default">&times;</button>';
    }
    if (conflict) {
      html += '<span class="shortcut-conflict">Conflict: ' + conflict + '</span>';
    }
    html += '</div>';
  }
  return html;
}

export function renderShortcutsEditor() {
  var shortcuts = state.currentShortcuts;
  if (!shortcuts) return;
  var html = '';
  for (var ctx in SHORTCUT_LABELS) html += contextHtml(shortcuts, ctx);
  /** @type {HTMLElement} */ (document.getElementById('shortcutsEditor')).innerHTML = html;
}

/** The other action in `ctx` already bound to `keyStr`, if any. @param {string} ctx @param {string} action @param {string | undefined} keyStr @param {ShortcutConfig | null} [config] */
export function findConflict(ctx, action, keyStr, config = state.currentShortcuts) {
  if (!keyStr) return null;
  var bindings = config && config[ctx];
  if (!bindings) return null;
  for (var other in bindings) {
    if (other === action) continue;
    if (bindings[other] === keyStr) return SHORTCUT_LABELS[ctx]?.[other] || 'another shortcut';
  }
  const rapidAction = ctx === 'pipeline_rapid_review' ? action :
    ctx === 'browse' ? Object.keys(RAPID_SHARED).find(id => RAPID_SHARED[id] === action && !(id in (config?.pipeline_rapid_review || {}))) : null;
  if (rapidAction && config) {
    for (const [other, key] of Object.entries(effectiveRapidBindings(config))) {
      if (other !== rapidAction && key === keyStr) return 'Rapid Review · ' + SHORTCUT_LABELS.pipeline_rapid_review?.[other];
    }
  }
  return null;
}

/** @param {string | undefined} keyStr */
function isBareShortcut(keyStr) {
  if (!keyStr) return false;
  var sc = parseShortcut(keyStr);
  return !sc.ctrl && !sc.meta && !sc.shift && !sc.alt;
}

/** Whether a bare navigation key would collide with a page's own keys. @param {string | undefined} keyStr */
export function isBarePageShortcut(keyStr) {
  if (!keyStr || !isBareShortcut(keyStr)) return false;
  var key = parseShortcut(keyStr).key;

  // Bare navigation shortcuts are intentionally limited to unused letters.
  // Non-letter keys retain their browser or page behavior unless combined
  // with a modifier.
  if (!/^[a-z]$/.test(key)) return true;

  // Keep default and fixed page-local letters available in their owning views.
  var reservedDefaults = [
    'a', 'b', 'c', 'e', 'f', 'g', 'h', 'j', 'k', 'p', 's', 'u', 'x', 'z'
  ];
  if (reservedDefaults.indexOf(key) !== -1) {
    return true;
  }

  // A bare navigation key is safe when no configurable page action uses it.
  // This lets users choose otherwise-unused letters (for example "d") while
  // preserving page actions such as Browse's "b", "p", and "x" bindings.
  for (var ctx in state.currentShortcuts) {
    if (ctx === 'navigation') continue;
    var shortcuts = state.currentShortcuts[ctx] || {};
    for (var action in shortcuts) {
      var shortcut = shortcuts[action];
      if (isBareShortcut(shortcut) && parseShortcut(/** @type {string} */ (shortcut)).key === key) return true;
    }
  }
  return false;
}

// Validate the candidate before changing the live editor state or saving.
/** @param {string} ctx @param {Record<string,string | undefined>} values @param {string[]} actions */
function commitReset(ctx, values, actions) {
  if (!state.currentShortcuts) return;
  const candidate = {...state.currentShortcuts, [ctx]: values};
  const bindings = ctx === 'pipeline_rapid_review' ? effectiveRapidBindings(candidate) : values;
  for (const action of actions) {
    const conflict = findConflict(ctx, action, bindings[action], candidate);
    if (conflict) {
      window.showToast?.('Cannot reset shortcut: conflicts with ' + conflict + '.', 'error');
      return;
    }
  }
  state.currentShortcuts = candidate;
  renderShortcutsEditor();
  saveShortcuts();
}

/** @param {string} ctx @param {string} action */
export function resetShortcut(ctx, action) {
  if (!state.currentShortcuts) return;
  const values = {...state.currentShortcuts[ctx]};
  if (ctx === 'pipeline_rapid_review' && action in RAPID_SHARED) delete values[action];
  else values[action] = SHORTCUT_DEFAULTS[ctx]?.[action] || '';
  commitReset(ctx, values, [action]);
}

/** @param {string} ctx */
export function resetContext(ctx) {
  const values = defaultsFor(ctx);
  const actions = Object.keys(ctx === 'pipeline_rapid_review' ? SHORTCUT_LABELS.pipeline_rapid_review || {} : values);
  commitReset(ctx, values, actions);
}

export function saveShortcuts() {
  safeFetch('/api/config', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({ keyboard_shortcuts: state.currentShortcuts }),
  }).catch(function() {});
}

/** Load the bindings from /api/config and render the editor. */
export function loadShortcuts() {
  return /** @type {Promise<{keyboard_shortcuts?: ShortcutConfig} | null>} */ (safeFetch('/api/config', {}, { toast: false })).then(function(cfg) {
    /** @type {ShortcutConfig} */
    var current = (cfg && cfg.keyboard_shortcuts) || JSON.parse(JSON.stringify(SHORTCUT_DEFAULTS));
    state.currentShortcuts = current;
    // Ensure all default keys exist (for newly added shortcuts). Presence
    // check on the action: if the server-side config already includes the
    // action with an explicit empty string (e.g. a conflict-resolution
    // migration blanked it), preserve that so we don't silently re-inject
    // the default and clobber the user's other binding on save.
    for (var ctx in SHORTCUT_DEFAULTS) {
      var bindings = current[ctx] = current[ctx] || {};
      var defaults = SHORTCUT_DEFAULTS[ctx] || {};
      for (var action in defaults) {
        if (!(action in bindings)) bindings[action] = defaults[action] || '';
        if (ctx === 'navigation' && isBarePageShortcut(bindings[action])) {
          bindings[action] = '';
        }
      }
    }
    renderShortcutsEditor();
  }).catch(function() {});
}
