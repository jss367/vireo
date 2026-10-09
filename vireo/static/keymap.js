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
    e.stopImmediatePropagation();
    try { top.handler(e); } catch (err) { console.error('Esc handler error', err); }
    return true;
  }

  // Pause flag — when set, the dispatcher yields the keypress entirely so a
  // higher-priority capture-phase listener (e.g. the /shortcuts editor's key
  // capture) can claim the next press without nav/global actions firing first.
  // Both listeners run in the capture phase; this dispatcher is registered at
  // module load and would otherwise win the race.
  var _dispatchPaused = false;
  var _captureKeyHandler = null;

  function pauseDispatch(captureHandler) {
    _dispatchPaused = true;
    _captureKeyHandler = captureHandler || null;
  }
  function resumeDispatch() { _dispatchPaused = false; _captureKeyHandler = null; }
  function isDispatchPaused() { return _dispatchPaused; }

  // Native menu accelerators can consume a key before the webview sees it.
  // Deliver that shortcut directly to the recorder that paused dispatch.
  function captureNativeShortcut(shortcut) {
    if (!_dispatchPaused || !_captureKeyHandler) return false;
    var parsed = parseShortcut(shortcut);
    _captureKeyHandler(new KeyboardEvent('keydown', {
      key: parsed.key, ctrlKey: parsed.ctrl, metaKey: parsed.meta,
      shiftKey: parsed.shift, altKey: parsed.alt, cancelable: true
    }));
    return true;
  }

  function _dispatch(e) {
    if (_dispatchPaused) return;
    // Esc runs first — even if focus is in an input, an open modal should
    // still be dismissable with Esc from a field inside it.
    if (_handleEsc(e)) return;
    // Find owns this chord before configurable actions can navigate or edit
    // a photo. Its later capture listener handles the event; recording still
    // takes priority via the pause check above.
    if (window.__TAURI_INTERNALS__ && window.VireoPageFind && matchesShortcut(e, 'ctrl+f')) return;
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

  // Register in capture phase so the Esc-stack stops later document capture
  // listeners as well as bubble-phase page handlers from consuming the key.
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
    unlockBodyScroll: unlockBodyScroll,
    pauseDispatch: pauseDispatch,
    resumeDispatch: resumeDispatch,
    isDispatchPaused: isDispatchPaused,
    captureNativeShortcut: captureNativeShortcut
  };
})(window);
