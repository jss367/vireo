import { createNotificationStore, notificationKey } from './navbar-notification-store.js';

const store = createNotificationStore(() => localStorage);
const container = document.createElement('div');
container.id = 'toastContainer';
document.body.appendChild(container);
const button = document.getElementById('notificationsToggle');
const panel = document.getElementById('notificationHistory');
const list = document.getElementById('notificationList');
let escapeToken = 0;
// The <details> element rendered for each history entry, keyed by its id,
// so a toast's "View notification details" can expand the matching row.
// A later renderHistory() replaces the map, and an entry evicted by the
// 100-row cap is simply absent — expandEntry treats that as a no-op.
const detailElements = new Map();

/** @param {string} label @param {() => void} action */
function actionButton(label, action) {
  const element = document.createElement('button');
  element.type = 'button';
  element.textContent = label;
  element.addEventListener('click', action);
  return element;
}

/** @param {HTMLElement} element @param {string} jobId */
function appendJobLink(element, jobId) {
  if (!jobId) return;
  const link = document.createElement('a');
  link.href = '/jobs?job=' + encodeURIComponent(jobId);
  link.textContent = 'View job details';
  element.appendChild(link);
}

function updateBadge() {
  const count = store.read().filter(entry => !entry.read && ['warning', 'error'].includes(entry.type)).length;
  const badge = document.getElementById('notificationBadge');
  if (badge) { badge.textContent = String(count); badge.hidden = !count; }
  button?.setAttribute('aria-label', 'Notifications' + (count ? ', ' + count + ' unread warnings or errors' : ''));
}

function renderHistory() {
  if (!list) return;
  list.replaceChildren();
  detailElements.clear();
  const entries = store.read().slice().reverse();
  if (!entries.length) list.textContent = 'No notifications yet.';
  entries.forEach(entry => {
    const row = document.createElement('li');
    row.dataset.type = entry.type;
    row.dataset.id = entry.id;
    const heading = document.createElement('div');
    heading.textContent = entry.type + (entry.read ? '' : ' · Unread') + ' · ' + new Date(entry.time).toLocaleString();
    heading.className = 'notification-meta';
    const message = document.createElement('p');
    message.textContent = entry.message;
    row.append(heading, message);
    if (entry.details) {
      const details = document.createElement('details');
      const summary = document.createElement('summary');
      summary.textContent = 'Full details';
      const text = document.createElement('p');
      text.textContent = entry.details;
      details.append(summary, text);
      row.appendChild(details);
      detailElements.set(entry.id, details);
    }
    appendJobLink(row, entry.jobId);
    if (!entry.read) row.appendChild(actionButton('Mark read', () => {
      store.markRead([entry.id]); renderHistory(); updateBadge();
    }));
    list.appendChild(row);
  });
}

/** Open the matching history row's details, or no-op if it has been evicted.
 * @param {string} id */
function expandEntry(id) {
  const details = detailElements.get(id);
  if (!details) return;
  details.open = true;
  details.scrollIntoView?.({block: 'nearest'});
}

function closeHistory() {
  if (panel) panel.hidden = true;
  button?.setAttribute('aria-expanded', 'false');
  if (escapeToken) { Keymap.popEsc(escapeToken); escapeToken = 0; }
  button?.focus();
}

function openHistory() {
  if (!panel) return;
  panel.hidden = false;
  button?.setAttribute('aria-expanded', 'true');
  renderHistory();
  escapeToken = Keymap.pushEsc(closeHistory);
  document.getElementById('notificationClose')?.focus();
}

button?.addEventListener('click', () => panel?.hidden ? openHistory() : closeHistory());
document.getElementById('notificationClose')?.addEventListener('click', closeHistory);
document.getElementById('notificationReadAll')?.addEventListener('click', () => {
  store.markRead(store.read().map(entry => entry.id)); renderHistory(); updateBadge();
});
document.getElementById('notificationClear')?.addEventListener('click', () => {
  store.clear(); renderHistory(); updateBadge();
});
window.addEventListener('storage', event => {
  if (event.key !== notificationKey && event.key !== null) return;
  updateBadge();
  if (panel && !panel.hidden) renderHistory();
});
updateBadge();

/** @param {string} msg @param {string} [type]
 * @param {{details?: string, jobId?: string}} [options]
 */
export function showToast(msg, type, options = {}) {
  type = ['success', 'info', 'warning', 'error'].includes(type || '') ? type : 'info';
  const entry = {id: Date.now().toString(36) + '-' + Math.random().toString(36).slice(2), message: msg, type: type || 'info',
    time: Date.now(), read: false, details: options.details || '', jobId: options.jobId || ''};
  store.add(entry);
  updateBadge();
  if (panel && !panel.hidden) renderHistory();
  const toast = document.createElement('div');
  toast.className = 'notification-toast';
  toast.dataset.type = entry.type;
  toast.setAttribute('role', type === 'error' ? 'alert' : 'status');
  toast.setAttribute('aria-live', type === 'error' ? 'assertive' : 'polite');
  const message = document.createElement('span');
  message.textContent = msg;
  toast.appendChild(message);
  appendJobLink(toast, entry.jobId);
  if (entry.details) toast.appendChild(actionButton('View notification details', () => {
    // Open the panel if needed (renderHistory populates detailElements), then
    // expand the matching row whether it was just rendered or already visible.
    // An entry evicted by the 100-row cap simply cannot be expanded.
    if (panel?.hidden) openHistory();
    expandEntry(entry.id);
  }));
  if (type === 'warning' || type === 'error') toast.appendChild(actionButton('Dismiss', () => {
    store.markRead([entry.id]); toast.remove(); updateBadge();
    if (panel && !panel.hidden) renderHistory();
  }));
  container.appendChild(toast);
  if (type !== 'warning' && type !== 'error') setTimeout(() => toast.remove(), 5000);
}
