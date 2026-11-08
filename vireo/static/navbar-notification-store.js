/** @typedef {{id: string, message: string, type: string, time: number, read: boolean, details: string, jobId: string}} NotificationEntry */
export const notificationKey = 'vireo.notifications.v1';

/** A bounded, best-effort browser history; blocked storage must not hide feedback.
 * @param {() => Storage} storage
 */
export function createNotificationStore(storage) {
  /** @type {NotificationEntry[]} */
  let entries = [];
  function read() {
    try {
      const data = JSON.parse(storage().getItem(notificationKey) || '[]');
      if (Array.isArray(data)) entries = data.filter(entry => entry &&
        typeof entry.id === 'string' && typeof entry.message === 'string' &&
        ['success', 'info', 'warning', 'error'].includes(entry.type) &&
        Number.isFinite(entry.time) && typeof entry.read === 'boolean' &&
        typeof entry.details === 'string' && typeof entry.jobId === 'string').slice(-100);
    } catch (_) { /* Keep the in-memory history when storage is unavailable. */ }
    return entries;
  }
  function save() {
    try { storage().setItem(notificationKey, JSON.stringify(entries)); } catch (_) { /* In-memory fallback. */ }
  }
  /** @param {NotificationEntry} entry */
  function add(entry) {
    entries = [...read(), entry].slice(-100);
    save();
  }
  /** @param {string[]} ids */
  function markRead(ids) {
    entries = read().map(entry => ids.includes(entry.id) ? {...entry, read: true} : entry);
    save();
  }
  function clear() { entries = []; save(); }
  return {read, add, markRead, clear};
}
