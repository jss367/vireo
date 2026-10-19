// The Workspace page's boundary with shared code: the navbar (safeFetch,
// showToast, formatDuration, the workspace switcher and edit-navigation
// cursors), vireo-utils.js, and vireo-work-locally.js (the Work Locally panel
// and the per-folder local-copy state, `window.vireoLocalFolders`). Page
// modules import these names from here instead of reaching for globals, so
// this file is the one list of what the page uses from shared code.
//
// The forwarders look the global up at call time, as the classic page did, so
// one replaced at runtime (tests stub showToast and safeFetch) is still the
// one called. The escape helpers are pure and loaded first by _navbar.html,
// so they are bound once.

export const escapeHtml = window.escapeHtml;
export const escapeAttr = window.escapeAttr;

/**
 * @template T
 * @param {string} url @param {RequestInit=} init @param {{toast?: boolean}=} options
 * @returns {Promise<T>}
 */
export function safeFetch(url, init, options) {
  return window.safeFetch(url, init, options);
}

/** @param {string} message @param {string=} kind */
export function showToast(message, kind) {
  if (typeof window.showToast === 'function') window.showToast(message, kind);
}

/** @param {number} seconds */
export function formatDuration(seconds) {
  return window.formatDuration(seconds);
}

/** The Work Locally controller from vireo-work-locally.js, or null before it loads. */
export function localFolders() {
  return window.vireoLocalFolders || null;
}

/** Open the navbar workspace switcher's New Workspace form. */
export function showCreateWorkspace() {
  window.vireoWorkspaceSwitcher.showCreate();
}

/** Forget the edit-navigation cursors scoped to the workspace being left. */
export function clearWorkspaceScopedCursors() {
  if (window.vireoEditNav) window.vireoEditNav.clearWorkspaceScopedCursors();
}

/**
 * Hand the page hooks shared code calls to the page-hook registry
 * (vireo-page-hooks.js). `test_module_pages.py` holds the names registered
 * to the page's `pageHooks` in frontend-module-pages.json.
 *
 * @param {VireoPageHooks} hooks
 */
export function registerPageHooks(hooks) {
  window.Vireo.registerPageHooks(hooks);
}
