// Keyboard shortcuts: the Keymap scope, the configurable keys, and the
// keydown handler that drives decisions, undo, apply, exit, and zoom.
// Classic page script; load boot.js after all definitions.

var RAPID_SCOPE = 'pipeline_rapid_review';
var RAPID_SC_DEFAULTS = {
  pick: 'p',
  reject: 'x',
  next: 'arrowright',
  back: 'arrowleft',
  clear: 'u',
  apply: 'enter',
  exit: 'escape',
  zoom: 'z',
};
var rapidShortcuts = Object.assign({}, RAPID_SC_DEFAULTS);
var rapidShortcutsRegistered = false;

function rapidShortcutMatches(e, action) {
  var shortcut = rapidShortcuts[action];
  if (!shortcut) return false;
  if (window.Keymap && window.Keymap.matchesShortcut) {
    return window.Keymap.matchesShortcut(e, shortcut);
  }
  if (typeof matchesShortcut === 'function') return matchesShortcut(e, shortcut);
  return false;
}

function handleRapidShortcutAction(action) {
  if (typeof isQueuePromptOpen === 'function' && isQueuePromptOpen()) return true;
  if (action === 'pick') decideCurrent('pick');
  else if (action === 'reject') decideCurrent('reject');
  else if (action === 'next') decideCurrent('review');
  else if (action === 'back') undoLast();
  else if (action === 'clear') clearCurrent();
  else if (action === 'apply') applyCurrent(false);
  else if (action === 'exit') window.location.href = '/pipeline/review';
  else if (action === 'zoom') toggleRapidZoom();
  return true;
}

function registerRapidShortcuts() {
  if (rapidShortcutsRegistered || !window.Keymap || !window.Keymap.register) return;
  Object.keys(RAPID_SC_DEFAULTS).forEach(function(action) {
    var key = rapidShortcuts[action];
    if (!key) return;
    window.Keymap.register(RAPID_SCOPE, {
      key: key,
      name: action,
      description: action,
      category: 'Rapid Review',
      action: function() { return handleRapidShortcutAction(action); },
    });
  });
  rapidShortcutsRegistered = true;
}

async function loadRapidShortcuts() {
  try {
    var cfg = await safeFetch('/api/config', {}, { toast: false });
    var saved = (cfg.keyboard_shortcuts || {})[RAPID_SCOPE] || {};
    rapidShortcuts = Object.assign({}, RAPID_SC_DEFAULTS, saved);
    window._vireoShortcuts = window._vireoShortcuts || {};
    if (cfg.keyboard_shortcuts) {
      Object.keys(cfg.keyboard_shortcuts).forEach(function(ctx) {
        if (!window._vireoShortcuts[ctx]) window._vireoShortcuts[ctx] = cfg.keyboard_shortcuts[ctx];
      });
    }
    window._vireoShortcuts[RAPID_SCOPE] = rapidShortcuts;
  } catch (e) {
    rapidShortcuts = Object.assign({}, RAPID_SC_DEFAULTS);
  }
  registerRapidShortcuts();
}

/* ---------- Keyboard Shortcuts ---------- */
function bindRapidKeyboard() {
  document.addEventListener('keydown', function(e) {
    if (e.defaultPrevented) return;
    if (isQueuePromptOpen()) {
      e.preventDefault();
      return;
    }
    // Skip every form field, not just INPUT: with the #queueFilter <select>
    // focused, arrows/Enter must change the filter, not decide the photo.
    var t = e.target;
    if (t && (t.tagName === 'INPUT' || t.tagName === 'SELECT' ||
              t.tagName === 'TEXTAREA' || t.isContentEditable)) return;
    if (rapidShortcutMatches(e, 'reject')) {
      e.preventDefault();
      decideCurrent('reject');
    } else if (rapidShortcutMatches(e, 'pick')) {
      e.preventDefault();
      decideCurrent('pick');
    } else if (rapidShortcutMatches(e, 'next')) {
      e.preventDefault();
      decideCurrent('review');
    } else if (rapidShortcutMatches(e, 'back')) {
      e.preventDefault();
      undoLast();
    } else if (rapidShortcutMatches(e, 'clear')) {
      e.preventDefault();
      clearCurrent();
    } else if (rapidShortcutMatches(e, 'apply')) {
      e.preventDefault();
      applyCurrent(false);
    } else if (rapidShortcutMatches(e, 'exit')) {
      e.preventDefault();
      window.location.href = '/pipeline/review';
    } else if (rapidShortcutMatches(e, 'zoom')) {
      e.preventDefault();
      toggleRapidZoom();
    }
  });
}
