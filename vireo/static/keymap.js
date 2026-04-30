/**
 * Vireo keymap module — single source of truth for keyboard shortcuts.
 *
 * Public API (this PR):
 *   Keymap.parseShortcut(str)        -> {key, ctrl, meta, shift, alt}
 *   Keymap.matchesShortcut(event, str)
 *   Keymap.isInputFocused()          -> bool
 *
 * More API lands in subsequent tasks.
 */
(function (window) {
  'use strict';

  function parseShortcut(str) {
    var parts = str.toLowerCase().split('+');
    var key = parts.pop();
    var mods = { ctrl: false, meta: false, shift: false, alt: false };
    parts.forEach(function (m) { if (m in mods) mods[m] = true; });
    return { key: key, ctrl: mods.ctrl, meta: mods.meta, shift: mods.shift, alt: mods.alt };
  }

  function matchesShortcut(e, shortcutStr) {
    if (!shortcutStr) return false;
    var sc = parseShortcut(shortcutStr);
    if (e.key.toLowerCase() !== sc.key) return false;
    var wantCtrl = sc.ctrl || sc.meta;
    var hasCtrl = e.ctrlKey || e.metaKey;
    if (wantCtrl !== hasCtrl) return false;
    if (sc.shift !== e.shiftKey) return false;
    if (sc.alt !== e.altKey) return false;
    return true;
  }

  function isInputFocused() {
    var el = document.activeElement;
    if (!el) return false;
    var tag = el.tagName;
    if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') return true;
    if (el.isContentEditable) return true;
    return false;
  }

  // scope -> array of shortcut definitions
  var _registry = { global: [] };

  function register(scope, shortcut) {
    if (!_registry[scope]) _registry[scope] = [];
    _registry[scope].push(shortcut);
  }

  function shortcutsForScope(scope) {
    var globals = _registry.global || [];
    if (scope === 'global' || !_registry[scope]) return globals.slice();
    return _registry[scope].concat(globals);
  }

  var _currentScope = 'global';

  function setScope(scope) { _currentScope = scope; }
  function getScope() { return _currentScope; }

  function _dispatch(e) {
    if (isInputFocused()) return;
    var candidates = shortcutsForScope(_currentScope);
    for (var i = 0; i < candidates.length; i++) {
      var sc = candidates[i];
      if (matchesShortcut(e, sc.key)) {
        e.preventDefault();
        try { sc.action(e); } catch (err) { console.error('Keymap action error', err); }
        return;
      }
    }
  }

  document.addEventListener('keydown', _dispatch);

  window.Keymap = {
    parseShortcut: parseShortcut,
    matchesShortcut: matchesShortcut,
    isInputFocused: isInputFocused,
    register: register,
    shortcutsForScope: shortcutsForScope,
    setScope: setScope,
    getScope: getScope
  };
})(window);
