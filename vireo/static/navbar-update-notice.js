// The "Needs attention" row for a desktop update that has finished
// downloading. The desktop shell downloads updates in the background and never
// interrupts with a dialog; it only reports the ready version through the
// `update_status` command. This row shows that version, and its button asks the
// shell to install the update and restart (`restart_to_update`). Showing the
// row also badges the bell (navbar-standing-notices.js counts it). In a plain
// browser there is no shell, so the row stays hidden and the tray and app menu
// carry the same "Restart to Update" item.

/** How often an open page asks the shell whether a download has finished. */
const POLL_MS = 60 * 1000;

const notice = document.getElementById('updateReadyNotice');
const text = document.getElementById('updateReadyMsg');
const button = /** @type {HTMLButtonElement | null} */ (document.getElementById('updateReadyRestart'));

/** The last failed install, kept so a later poll does not hide it. */
let installError = '';

/** @param {string} version @param {string} [error] */
export function updateReadyMessage(version, error) {
  const ready = 'Vireo v' + version + ' has been downloaded. Restart Vireo to finish updating.';
  return error ? ready + ' The last attempt failed: ' + error : ready;
}

/** @param {string | null} version */
function render(version) {
  if (!notice || !text) return;
  if (version) {
    text.textContent = updateReadyMessage(version, installError);
    notice.style.display = 'flex';
  } else {
    notice.style.display = 'none';
  }
}

export async function refreshUpdateNotice() {
  const bridge = window.__TAURI_INTERNALS__;
  if (!bridge) return;
  try {
    render(/** @type {string | null} */ (await bridge.invoke('update_status')) || null);
  } catch (error) {
    // An older shell has no update_status command; leave the row hidden.
    console.debug('update_status unavailable:', error);
  }
}

/** Ask the shell to install and restart. A successful install never answers:
 * the app restarts. A failure keeps the update ready and says why in the row. */
export function restartToUpdate() {
  const bridge = window.__TAURI_INTERNALS__;
  if (!bridge || !button) return;
  button.disabled = true;
  button.textContent = 'Restarting…';
  bridge.invoke('restart_to_update').catch(error => {
    button.disabled = false;
    button.textContent = 'Restart to Update';
    installError = String(error);
    refreshUpdateNotice();
  });
}

if (notice && window.__TAURI_INTERNALS__) {
  refreshUpdateNotice();
  setInterval(() => {
    if (document.visibilityState === 'visible') refreshUpdateNotice();
  }, POLL_MS);
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'visible') refreshUpdateNotice();
  });
}
