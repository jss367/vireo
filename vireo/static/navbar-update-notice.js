// The "Needs attention" row for a desktop update that has finished
// downloading, and the warning before Vireo restarts for it on its own. The
// desktop shell downloads updates in the background and never interrupts with
// a dialog; it reports the update through the `update_status` command. This
// row shows that version, and its button asks the shell to restart for it
// (`restart_to_update`). The shell first checks for running jobs and, when
// some are running, asks whether to restart after they finish. While it waits
// the row names what it is waiting for; when they finish, a banner counts
// down to the restart, and the shell restarts on its own only after a page
// has read the countdown. Showing the row also badges the bell
// (navbar-standing-notices.js counts it). In a plain browser there is no
// shell, so nothing shows and the tray and app menu carry the same item.

/** How often an open page asks the shell whether a download has finished. */
const POLL_MS = 60 * 1000;

const notice = document.getElementById('updateReadyNotice');
const text = document.getElementById('updateReadyMsg');
const button = /** @type {HTMLButtonElement | null} */ (document.getElementById('updateReadyRestart'));

/**
 * @typedef {{version: string, waiting_for?: string | null, restart_in?: number | null}} UpdateStatus
 */

/** The last failed install, kept so a later poll does not hide it. */
let installError = '';
/** @type {ReturnType<typeof setInterval> | null} */
let countdownTimer = null;

/** @param {string} version @param {string} [error] @param {string | null} [waitingFor] */
export function updateReadyMessage(version, error, waitingFor) {
  const ready = waitingFor
    ? 'Vireo v' + version + ' has been downloaded. Vireo will restart to install it after '
      + waitingFor + ' finishes.'
    : 'Vireo v' + version + ' has been downloaded. Restart Vireo to finish updating.';
  return error ? ready + ' The last attempt failed: ' + error : ready;
}

/** @param {string} version @param {number} seconds */
export function countdownMessage(version, seconds) {
  if (seconds <= 0) return 'Restarting to install Vireo v' + version + '…';
  return 'Your jobs have finished. Vireo will restart to install v' + version + ' in '
    + seconds + (seconds === 1 ? ' second.' : ' seconds.');
}

/** @param {unknown} value @returns {UpdateStatus | null} */
function normalizeStatus(value) {
  if (!value) return null;
  if (typeof value === 'string') return { version: value };
  return /** @type {UpdateStatus} */ (value);
}

/** @param {UpdateStatus | null} status */
function render(status) {
  renderCountdown(status);
  if (!notice || !text || !button) return;
  if (status) {
    text.textContent = updateReadyMessage(status.version, installError, status.waiting_for);
    if (!button.disabled) button.textContent = status.waiting_for ? 'Restart Now' : 'Restart to Update';
    notice.style.display = 'flex';
  } else {
    notice.style.display = 'none';
  }
}

/** Show, update or remove the countdown banner. @param {UpdateStatus | null} status */
function renderCountdown(status) {
  if (countdownTimer !== null) {
    clearInterval(countdownTimer);
    countdownTimer = null;
  }
  const existing = document.getElementById('updateRestartBanner');
  if (!status || status.restart_in === null || status.restart_in === undefined) {
    existing?.remove();
    return;
  }
  const banner = existing || buildCountdownBanner();
  const message = /** @type {HTMLElement} */ (banner.querySelector('.update-restart-text'));
  const version = status.version;
  const deadline = Date.now() + status.restart_in * 1000;
  const tick = () => {
    message.textContent = countdownMessage(version, Math.max(0, Math.ceil((deadline - Date.now()) / 1000)));
  };
  tick();
  countdownTimer = setInterval(tick, 500);
}

/** @returns {HTMLElement} */
function buildCountdownBanner() {
  const banner = document.createElement('div');
  banner.id = 'updateRestartBanner';
  banner.className = 'update-restart-banner';
  banner.setAttribute('role', 'alert');
  const message = document.createElement('span');
  message.className = 'update-restart-text';
  const now = document.createElement('button');
  now.type = 'button';
  now.className = 'update-restart-primary';
  now.textContent = 'Restart Now';
  now.addEventListener('click', () => respondToCountdown('now', banner));
  const cancel = document.createElement('button');
  cancel.type = 'button';
  cancel.textContent = 'Not Now';
  cancel.title = 'Keep working. Restart to Update stays under the bell and in the Help menu.';
  cancel.addEventListener('click', () => respondToCountdown('cancel', banner));
  banner.append(message, now, cancel);
  document.body.prepend(banner);
  return banner;
}

/** @param {'now' | 'cancel'} action @param {HTMLElement} banner */
function respondToCountdown(action, banner) {
  const bridge = window.__TAURI_INTERNALS__;
  if (!bridge) return;
  banner.querySelectorAll('button').forEach(control => { control.disabled = true; });
  // The shell restarts, or tells the page to read the status again.
  bridge.invoke('respond_update_restart', { action: action }).catch(error => {
    console.warn('respond_update_restart failed:', error);
    // The countdown may still be running; let the user answer again.
    banner.querySelectorAll('button').forEach(control => { control.disabled = false; });
    refreshUpdateNotice();
  });
}

export async function refreshUpdateNotice() {
  const bridge = window.__TAURI_INTERNALS__;
  if (!bridge) return;
  try {
    render(normalizeStatus(await bridge.invoke('update_status')));
  } catch (error) {
    // An older shell has no update_status command; leave the row hidden.
    console.debug('update_status unavailable:', error);
  }
}

/** Ask the shell to restart for the update. A successful install never
 * answers: the app restarts. When jobs are running the shell asks first, and
 * answers once the user chose to wait or keep working. A failure keeps the
 * update ready and says why in the row. */
export function restartToUpdate() {
  const bridge = window.__TAURI_INTERNALS__;
  if (!bridge || !button) return;
  button.disabled = true;
  button.textContent = 'Restarting…';
  bridge.invoke('restart_to_update').then(() => {
    installError = '';
  }, error => {
    installError = String(error);
  }).finally(() => {
    button.disabled = false;
    refreshUpdateNotice();
  });
}

if (window.__TAURI_INTERNALS__) {
  refreshUpdateNotice();
  setInterval(() => {
    if (document.visibilityState === 'visible') refreshUpdateNotice();
  }, POLL_MS);
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'visible') refreshUpdateNotice();
  });
  // The shell dispatches this when the update's status changes (waiting
  // starts or ends, the countdown starts), so the page reads it right away.
  window.addEventListener('vireo:update-status', () => { refreshUpdateNotice(); });
}
