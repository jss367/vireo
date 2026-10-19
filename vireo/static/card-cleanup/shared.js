// The card cleanup page's boundary with shared code: vireo-utils.js's
// formatBytes, the shared folder browser (vireo-folder-browser.js, loaded
// before this module) and tauri-bridge.js's native folder picker. Page modules
// import these names from here instead of reaching for globals.
//
// The picker forwarders look the global up at call time, as the classic page
// did, so a test that replaces window.pickDirectory is still the one called.

export const formatBytes = window.formatBytes;
export const VireoFolderBrowser = window.VireoFolderBrowser;

/** Whether the desktop shell's native folder picker exists. */
export function hasPickDirectory() {
  return typeof window.pickDirectory === 'function';
}

/** @param {string} title */
export function pickDirectory(title) {
  return window.pickDirectory(title);
}

export function isTauri() {
  return typeof window.isTauri === 'function' && window.isTauri();
}
