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

  // Reference-counted body scroll lock. Stacked overlays each lock once on
  // open and unlock once on close; only the outermost transition touches the
  // DOM. Without this, closing a top overlay while a lower one is still open
  // would unconditionally unlock page scroll behind the active overlay.
  var _bodyScrollLockCount = 0;

  function lockBodyScroll() {
    if (_bodyScrollLockCount === 0) document.body.style.overflow = 'hidden';
    _bodyScrollLockCount++;
  }

  function unlockBodyScroll() {
    if (_bodyScrollLockCount === 0) return;
    _bodyScrollLockCount--;
    if (_bodyScrollLockCount === 0) document.body.style.overflow = '';
  }

  // Esc stack — single owner of the Escape key. Handlers push themselves
  // onto the stack; pressing Esc invokes (and removes) the top handler only.
  var _escStack = [];
  var _escNextToken = 1;

  function pushEsc(handler) {
    var token = _escNextToken++;
    _escStack.push({ token: token, handler: handler });
    return token;
  }

  function popEsc(token) {
    for (var i = _escStack.length - 1; i >= 0; i--) {
      if (_escStack[i].token === token) {
        _escStack.splice(i, 1);
        return true;
      }
    }
    return false;
  }

  function _handleEsc(e) {
    if (e.key !== 'Escape') return false;
    if (_escStack.length === 0) return false;
    var top = _escStack.pop();
    e.preventDefault();
    e.stopPropagation();
    try { top.handler(e); } catch (err) { console.error('Esc handler error', err); }
    return true;
  }

  function _dispatch(e) {
    // Esc runs first — even if focus is in an input, an open modal should
    // still be dismissable with Esc from a field inside it.
    if (_handleEsc(e)) return;
    if (isInputFocused()) return;
    var candidates = shortcutsForScope(_currentScope);
    for (var i = 0; i < candidates.length; i++) {
      var sc = candidates[i];
      if (!matchesShortcut(e, sc.key)) continue;
      // Action contract: returning false means "I didn't actually handle this"
      // (e.g. early-return because an overlay is open). In that case we do NOT
      // preventDefault and we continue to the next candidate so another scope
      // still has a chance to handle the key.
      var handled;
      try { handled = sc.action(e); }
      catch (err) { console.error('Keymap action error', err); handled = true; }
      if (handled !== false) {
        e.preventDefault();
        return;
      }
      // action returned false — try the next candidate
    }
  }

  // Register in capture phase so the Esc-stack can stop propagation before any
  // bubble-phase listeners on document.body fire (e.g. page-level Esc handlers).
  // This preserves the "Esc dismisses overlay without leaking to page" contract
  // that previously required individual capture-phase listeners per overlay.
  document.addEventListener('keydown', _dispatch, true);

  window.Keymap = {
    parseShortcut: parseShortcut,
    matchesShortcut: matchesShortcut,
    isInputFocused: isInputFocused,
    register: register,
    shortcutsForScope: shortcutsForScope,
    setScope: setScope,
    getScope: getScope,
    pushEsc: pushEsc,
    popEsc: popEsc,
    lockBodyScroll: lockBodyScroll,
    unlockBodyScroll: unlockBodyScroll
  };
})(window);
