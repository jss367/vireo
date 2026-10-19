// Keyboard Shortcuts page state. ES module: the page's other modules import
// `state` and read and write its fields; nothing here is a global.

/**
 * @typedef {object} ShortcutsPageState
 * @property {ShortcutConfig | null} currentShortcuts The bindings being
 *   edited, as loaded from /api/config and saved back on every change.
 * @property {HTMLElement | null} capturingBtn The key button waiting for the
 *   next key press, if any.
 */

/** @type {ShortcutsPageState} */
export const state = {
  currentShortcuts: null,
  capturingBtn: null,
};
