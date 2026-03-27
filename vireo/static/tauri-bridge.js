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

/**
 * Check for an available update via the Rust command.
 * @returns {Promise<{available: boolean, version: string|null, notes: string|null, date: string|null}|null>}
 *   Returns null if not running in Tauri or on error.
 */
async function checkForAppUpdate() {
  if (!isTauri()) return null;
  try {
    return await window.__TAURI_INTERNALS__.invoke('check_for_update');
  } catch (e) {
    console.error('Update check failed:', e);
    return null;
  }
}

/**
 * Download and install an available update via the Rust command.
 * @returns {Promise<boolean>} true if install succeeded
 */
async function downloadAndInstallUpdate() {
  if (!isTauri()) return false;
  try {
    await window.__TAURI_INTERNALS__.invoke('install_update');
    return true;
  } catch (e) {
    console.error('Update install failed:', e);
    return false;
  }
}

/**
 * Relaunch the application after installing an update.
 * @returns {Promise<void>}
 */
async function relaunchApp() {
  if (!isTauri()) return;
  try {
    await window.__TAURI_INTERNALS__.invoke('plugin:process|restart', {});
  } catch (e) {
    console.error('Relaunch failed:', e);
  }
}
