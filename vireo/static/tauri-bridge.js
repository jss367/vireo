/* Tauri Bridge — provides native OS dialogs when running inside Tauri.
   In the browser, all functions are no-ops and isTauri() returns false. */

function isTauri() {
  return !!(window.__TAURI_INTERNALS__);
}

/**
 * Open a native directory picker dialog.
 * @param {string} [title] - Dialog title
 * @returns {Promise<string|null>} Selected directory path, or null if cancelled
 */
async function pickDirectory(title) {
  if (!isTauri()) return null;
  var result = await window.__TAURI_INTERNALS__.invoke('plugin:dialog|open', {
    directory: true,
    multiple: false,
    title: title || 'Select Folder',
  });
  return result || null;
}

/**
 * Open a native file picker dialog.
 * @param {object} [opts] - Options
 * @param {string} [opts.title] - Dialog title
 * @param {Array} [opts.filters] - File type filters [{name: 'Name', extensions: ['ext']}]
 * @returns {Promise<string|null>} Selected file path, or null if cancelled
 */
async function pickFile(opts) {
  if (!isTauri()) return null;
  opts = opts || {};
  var result = await window.__TAURI_INTERNALS__.invoke('plugin:dialog|open', {
    directory: false,
    multiple: false,
    title: opts.title || 'Select File',
    filters: opts.filters || [],
  });
  return result || null;
}
