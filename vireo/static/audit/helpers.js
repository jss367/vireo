// Helpers every check shares: element lookup, relative times, the status
// line's found/clean/error states, and posting an audit action.
// ES module.

import { safeFetch } from './shared.js';

/** @param {string} id */
export function byId(id) {
  return /** @type {HTMLElement} */ (document.getElementById(id));
}

/** @param {string} iso */
export function timeAgo(iso) {
  if (!iso) return '';
  var secs = (Date.now() - new Date(iso).getTime()) / 1000;
  if (secs < 60) return 'just now';
  if (secs < 3600) return Math.floor(secs / 60) + 'm ago';
  if (secs < 86400) return Math.floor(secs / 3600) + 'h ago';
  return Math.floor(secs / 86400) + 'd ago';
}

/** @param {unknown} error */
export function errorMessage(error) {
  return error instanceof Error ? error.message : String(error);
}

/** Yield so the status line paints before a blocking scan request. */
export function paintFirst() {
  return new Promise(function(r) { setTimeout(r, 50); });
}

/** POST JSON to an audit action, ignoring failures (safeFetch already toasts them). @param {string} url @param {unknown} body */
export async function postAuditAction(url, body) {
  try {
    return /** @type {{warning?: string} | null} */ (await safeFetch(url, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body),
    }));
  } catch(e) {
    return null;
  }
}

/**
 * Show a check's outcome in its status line: a warning colour and `found`
 * when it found something, the accent colour and `clean` when not.
 *
 * @param {HTMLElement} status @param {number} count @param {string} found @param {string} clean
 */
export function showCheckResult(status, count, found, clean) {
  status.textContent = count > 0 ? count + ' ' + found : clean;
  status.style.color = count > 0 ? 'var(--warning)' : 'var(--accent)';
}

/** @param {HTMLElement} status @param {unknown} error */
export function showCheckError(status, error) {
  status.textContent = 'Error: ' + errorMessage(error);
  status.style.color = 'var(--danger)';
}
