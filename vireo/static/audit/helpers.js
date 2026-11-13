// Helpers every check shares: element lookup, the status line's
// found/clean/error states, and posting an audit action.
// ES module.

import { safeFetch } from './shared.js';

/** @param {string} id */
export function byId(id) {
  return /** @type {HTMLElement} */ (document.getElementById(id));
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
