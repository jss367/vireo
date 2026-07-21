/* Tauri Bridge — provides native OS dialogs when running inside Tauri.
   In the browser, all functions are no-ops and isTauri() returns false. */

function isTauri() {
  return !!(window.__TAURI_INTERNALS__);
}

/* ---------- Logging bridge ----------
   Forward webview-side logs and uncaught errors into the native log file
   (~/Library/Logs/com.vireo.app/Vireo.log on macOS) via tauri-plugin-log's
   `log` command. Without this, anything that fails inside the desktop webview
   leaves no trace on disk — which is exactly what made the iNaturalist
   quick-open bug so hard to diagnose across multiple attempts. In a plain
   browser these calls just go to the devtools console. */
var TAURI_LOG_LEVEL = { trace: 1, debug: 2, info: 3, warn: 4, error: 5 };

function tauriLog(level, message) {
  var msg = String(message);
  // Mirror to the devtools console regardless of environment.
  var consoleFn = console[level] || console.log;
  try { consoleFn.call(console, msg); } catch (e) {}
  if (!isTauri()) return;
  try {
    // Fire-and-forget — logging must never throw into its callers. tauri-plugin-log
    // expects a numeric level (Trace=1 … Error=5). Swallow a rejected invoke
    // promise too: an uncaught rejection here would re-enter via the
    // unhandledrejection handler below and loop.
    var p = window.__TAURI_INTERNALS__.invoke('plugin:log|log', {
      level: TAURI_LOG_LEVEL[level] || TAURI_LOG_LEVEL.info,
      message: msg,
    });
    if (p && typeof p.catch === 'function') p.catch(function() {});
  } catch (e) { /* swallow */ }
}

function logInfo(message) { tauriLog('info', message); }
function logWarn(message) { tauriLog('warn', message); }
function logError(message) { tauriLog('error', message); }

// Capture uncaught errors and unhandled promise rejections so they land in the
// log file instead of only the (invisible-in-production) webview console.
window.addEventListener('error', function(event) {
  var where = event.filename
    ? ' (' + event.filename + ':' + event.lineno + ':' + event.colno + ')'
    : '';
  var msg = event.message || (event.error && event.error.message) || 'unknown error';
  tauriLog('error', 'Uncaught error: ' + msg + where);
});
window.addEventListener('unhandledrejection', function(event) {
  var reason = event.reason;
  var msg = (reason && (reason.stack || reason.message)) || String(reason);
  tauriLog('error', 'Unhandled promise rejection: ' + msg);
});

/**
 * Open a native directory picker dialog.
 * @param {string} [title] - Dialog title
 * @param {object} [opts] - Options
 * @param {boolean} [opts.multiple] - Allow selecting multiple folders
 * @param {string} [opts.defaultPath] - Directory shown when the dialog opens
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
    defaultPath: opts.defaultPath || undefined,
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
 * Open an external URL in the user's default browser.
 *
 * The native shell enforces the same rule independently: an external page can
 * never replace the Vireo document or create another Vireo webview. This
 * helper is the user-facing path because it can report opener failures and
 * provide Retry/Copy recovery. In browser mode it opens a new tab only.
 *
 * @param {string} url - The http(s) URL to open
 * @returns {Promise<boolean>} true if the open was dispatched successfully
 */
async function openExternal(url) {
  if (!url) {
    logWarn('openExternal called with an empty URL');
    return false;
  }
  if (isTauri()) {
    try {
      await window.__TAURI_INTERNALS__.invoke('open_external_url', { url: url });
      logInfo('openExternal: opened ' + url);
      return true;
    } catch (e) {
      logError('openExternal failed for ' + url + ': ' + (e && e.message ? e.message : e));
      return false;
    }
  }
  // Browser mode never replaces the Vireo tab. Open a detectable blank tab,
  // sever its opener, and navigate it only after we know it was not blocked.
  var win = window.open('about:blank', '_blank');
  if (!win) {
    logWarn('openExternal: window.open was blocked (popup blocker?) for ' + url);
    return false;
  }
  try { win.opener = null; } catch (e) {}
  win.location = url;
  return true;
}

function _ensureExternalOpenModal() {
  var modal = document.getElementById('externalOpenModal');
  if (modal) return modal;
  modal = document.createElement('div');
  modal.id = 'externalOpenModal';
  modal.setAttribute('role', 'dialog');
  modal.setAttribute('aria-modal', 'true');
  modal.setAttribute('aria-labelledby', 'externalOpenModalTitle');
  modal.style.cssText = 'display:none;position:fixed;inset:0;z-index:20000;background:rgba(0,0,0,.72);align-items:center;justify-content:center;padding:24px;';
  modal.innerHTML =
    '<div style="width:min(620px,100%);background:var(--bg-secondary,#20252b);border:1px solid var(--border-primary,#46515b);border-radius:8px;padding:20px;box-shadow:0 16px 50px rgba(0,0,0,.45);">' +
      '<h3 id="externalOpenModalTitle" style="margin:0 0 10px;color:var(--text-primary,#fff);">Could not open your browser</h3>' +
      '<p id="externalOpenModalMessage" style="margin:0 0 12px;color:var(--text-secondary,#c7ced4);line-height:1.45;">Vireo stayed on this page. Retry or copy the URL below.</p>' +
      '<input id="externalOpenModalUrl" readonly style="box-sizing:border-box;width:100%;padding:9px 10px;background:var(--bg-primary,#15191d);color:var(--text-primary,#fff);border:1px solid var(--border-primary,#46515b);border-radius:4px;" />' +
      '<div style="display:flex;justify-content:flex-end;gap:8px;margin-top:16px;">' +
        '<button type="button" id="externalOpenCloseBtn" class="modal-btn modal-btn-cancel">Close</button>' +
        '<button type="button" id="externalOpenCopyBtn" class="modal-btn">Copy URL</button>' +
        '<button type="button" id="externalOpenRetryBtn" class="modal-btn modal-btn-primary">Retry</button>' +
      '</div>' +
    '</div>';
  document.body.appendChild(modal);
  modal.querySelector('#externalOpenCloseBtn').addEventListener('click', closeExternalOpenFailure);
  modal.querySelector('#externalOpenCopyBtn').addEventListener('click', function() {
    copyExternalUrl(document.getElementById('externalOpenModalUrl').value);
  });
  modal.querySelector('#externalOpenRetryBtn').addEventListener('click', async function() {
    var url = document.getElementById('externalOpenModalUrl').value;
    if (await openExternal(url)) {
      closeExternalOpenFailure();
      if (typeof showToast === 'function') showToast('Opened in your browser.', 'success');
    }
  });
  return modal;
}

function showExternalOpenFailure(url, message) {
  var modal = _ensureExternalOpenModal();
  document.getElementById('externalOpenModalUrl').value = url || '';
  document.getElementById('externalOpenModalMessage').textContent = message || 'Vireo stayed on this page. Retry or copy the URL below.';
  modal.style.display = 'flex';
  document.getElementById('externalOpenRetryBtn').focus();
}

function closeExternalOpenFailure() {
  var modal = document.getElementById('externalOpenModal');
  if (modal) modal.style.display = 'none';
}

async function copyExternalUrl(url) {
  var copied = false;
  if (isTauri()) {
    try {
      // Use the native system pasteboard in the packaged app. WebKit's
      // navigator.clipboard and execCommand paths can resolve/return true
      // without putting anything on the macOS clipboard.
      await window.__TAURI_INTERNALS__.invoke('plugin:clipboard-manager|write_text', {
        text: url,
      });
      copied = true;
    } catch (e) {
      logError('Native clipboard write failed: ' + (e && e.message ? e.message : e));
    }
  }
  if (!copied && navigator.clipboard && navigator.clipboard.writeText) {
    try {
      await navigator.clipboard.writeText(url);
      copied = true;
    } catch (e) {
      logWarn('Web clipboard write failed: ' + (e && e.message ? e.message : e));
    }
  }
  if (!copied) {
    // Always use a fresh throwaway textarea so non-modal callers (e.g. iNat
    // batch Copy URL buttons) never end up selecting a stale value left in
    // #externalOpenModalUrl from an earlier failure modal.
    var temp = document.createElement('textarea');
    temp.value = url;
    temp.setAttribute('readonly', '');
    temp.style.cssText = 'position:fixed;left:-9999px;top:0;';
    document.body.appendChild(temp);
    temp.focus();
    temp.select();
    try { copied = document.execCommand('copy'); } catch (copyError) {
      logWarn('DOM clipboard fallback failed: ' + (copyError && copyError.message ? copyError.message : copyError));
    }
    temp.remove();
  }
  if (typeof showToast === 'function') {
    showToast(copied ? 'URL copied.' : 'Select and copy the URL shown.', copied ? 'success' : 'warning');
  }
  return copied;
}

async function openExternalWithRecovery(url, message) {
  var opened = await openExternal(url);
  if (!opened) showExternalOpenFailure(url, message);
  return opened;
}

function _isExternalHttpUrl(rawUrl) {
  try {
    var parsed = new URL(rawUrl, window.location.href);
    return (parsed.protocol === 'http:' || parsed.protocol === 'https:') && parsed.origin !== window.location.origin;
  } catch (e) {
    return false;
  }
}

// Catch every ordinary external anchor, including future links that forget to
// opt into a special onclick handler. Existing explicit handlers run first and
// preventDefault(), so this delegated fallback never opens a URL twice.
document.addEventListener('click', function(event) {
  if (event.defaultPrevented || event.button !== 0) return;
  var target = event.target;
  var anchor = target && target.closest ? target.closest('a[href]') : null;
  if (!anchor || !_isExternalHttpUrl(anchor.href)) return;
  event.preventDefault();
  openExternalWithRecovery(anchor.href);
});

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
