// The Jobs page's boundary with the shared navbar and the shared classic
// scripts (vireo-utils.js, vireo-time.js, import-recovery.js). Page modules
// import these names from here instead of reaching for globals, so this file
// is the one list of what the page uses from shared code.
//
// The forwarders look the global up at call time, as the classic page did, so
// one replaced at runtime (tests stub showToast and safeFetch) is still the
// one called. The escape helpers are pure and loaded first by _navbar.html,
// so they are bound once.

export const escapeHtml = window.escapeHtml;
export const escapeAttr = window.escapeAttr;

/**
 * @template T
 * @param {string} url @param {RequestInit=} init
 * @returns {Promise<T>}
 */
export function safeFetch(url, init) {
  return window.safeFetch(url, init);
}

/** @param {string | undefined} message @param {string=} kind */
export function showToast(message, kind) {
  if (typeof window.showToast === 'function') window.showToast(message, kind);
}

/** The navbar's job-type label. @param {string} type */
export function formatJobType(type) {
  return window.formatJobType(type);
}

/** A time in the user's format (vireo-time.js). @param {Date} date */
export function formatTime(date) {
  return window.VireoTime.time(date);
}

/** A date and time in the user's format (vireo-time.js). @param {Date} date */
export function formatDateTime(date) {
  return window.VireoTime.dateTime(date);
}

// import-recovery.js, shared with the Import page: recognizing an import
// that failed before copying began, and retrying it directly to the NAS.

/** @param {JobsResult | null | undefined} result */
export function importFailedBeforeStart(result) {
  return window.importFailedBeforeStart(result);
}

/** @param {JobsRow} job @param {JobsRow[]} rows */
export function canRetryImportDirectToNas(job, rows) {
  return window.canRetryImportDirectToNas(job, rows);
}

/** @param {JobsRow | undefined} job @param {JobsConfig | null} body @param {JobsRow[]} rows */
export function directNasImportRetryBody(job, body, rows) {
  return window.directNasImportRetryBody(job, body, rows);
}
