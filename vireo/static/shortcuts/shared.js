// The Keyboard Shortcuts page's boundary with the shared navbar and keymap.js.
// Page modules import these names from here instead of reaching for globals,
// so this file is the one list of what the page uses from shared code.
//
// The forwarders look the global up at call time, as the classic page did, so
// one replaced at runtime (tests stub safeFetch) is still the one called.

/**
 * @template T
 * @param {string} url @param {RequestInit=} init @param {{toast?: boolean}=} options
 * @returns {Promise<T>}
 */
export function safeFetch(url, init, options) {
  return window.safeFetch(url, init, options);
}

/** @param {string} shortcut */
export function parseShortcut(shortcut) {
  return window.parseShortcut(shortcut);
}

/** @param {string | undefined} shortcut */
export function formatShortcut(shortcut) {
  return window.formatShortcut(shortcut);
}

/**
 * Pause the global Keymap dispatcher, handing `handler` the keys it would
 * have dispatched (keymap.js), so a navigation binding cannot fire while a
 * new key is being captured.
 *
 * @param {(event: KeyboardEvent) => void} handler
 */
export function pauseKeymapDispatch(handler) {
  if (window.Keymap && window.Keymap.pauseDispatch) window.Keymap.pauseDispatch(handler);
}

export function resumeKeymapDispatch() {
  if (window.Keymap && window.Keymap.resumeDispatch) window.Keymap.resumeDispatch();
}
