/* Tauri Bridge — provides native OS dialogs when running inside Tauri.
   In the browser, all functions are no-ops and isTauri() returns false. */

function isTauri() {
  return !!(window.__TAURI_INTERNALS__);
}

/**
 * Open a native directory picker dialog.
 * @param {string} [title] - Dialog title
 * @param {object} [opts] - Options
 * @param {boolean} [opts.multiple] - Allow selecting multiple folders
 * @returns {Promise<string|string[]|null>} Selected directory path(s), or null if cancelled.
 *   Returns an array when `opts.multiple` is true, otherwise a string.
 */
async function pickDirectory(title, opts) {
  if (!isTauri()) return null;
  opts = opts || {};
  var result = await window.__TAURI_INTERNALS__.invoke('plugin:dialog|open', {
    directory: true,
    multiple: !!opts.multiple,
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

/**
 * Check for updates on startup, at most once per cooldown period.
 * Shows a notification bar at the top of the page if an update is available.
 * @param {number} [cooldownHours=24] - Minimum hours between checks
 */
async function startupUpdateCheck(cooldownHours) {
  if (!isTauri()) return;
  cooldownHours = cooldownHours || 24;

  var lastCheck = localStorage.getItem('vireo_last_update_check');
  if (lastCheck) {
    var elapsed = Date.now() - parseInt(lastCheck, 10);
    if (elapsed < cooldownHours * 60 * 60 * 1000) return;
  }

  var result = await checkForAppUpdate();
  // Save cooldown timestamp only after a successful check so that
  // transient failures (offline, DNS error) don't block future retries.
  if (result) {
    localStorage.setItem('vireo_last_update_check', String(Date.now()));
  }
  if (!result || !result.available) return;

  // Show a non-intrusive banner at the top of the page
  var banner = document.createElement('div');
  banner.id = 'updateBanner';
  banner.style.cssText = 'position:fixed;top:0;left:0;right:0;z-index:99999;'
    + 'background:#1A4560;border-bottom:1px solid #24E5CA;padding:8px 16px;'
    + 'display:flex;align-items:center;justify-content:space-between;font-size:13px;';
  banner.innerHTML = '<span style="color:#E0F0F0;">'
    + 'A new version of Vireo is available: <strong style="color:#24E5CA;">v'
    + (result.version || '?') + '</strong></span>'
    + '<span>'
    + '<a href="/settings" style="color:#24E5CA;margin-right:12px;text-decoration:underline;">Update</a>'
    + '<button onclick="this.parentElement.parentElement.remove()" '
    + 'style="background:none;border:none;color:#888;cursor:pointer;font-size:16px;">&times;</button>'
    + '</span>';
  document.body.prepend(banner);
}
