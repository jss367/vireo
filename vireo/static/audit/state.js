// Audit page state: each check's last results, plus the labels the summary
// and the Integrity tab use. ES module: the page's other modules import
// `state` and read and write its fields; nothing here is a global.

/**
 * @typedef {object} AuditState
 * @property {AuditDrift[]} driftData
 * @property {AuditOrphan[]} orphanData
 * @property {AuditFileEntry[]} untrackedData
 * @property {AuditFileEntry[]} sidecarData
 * @property {AuditIntegrity | null} integrityData
 */

/** @type {AuditState} */
export const state = {
  driftData: [],
  orphanData: [],
  untrackedData: [],
  sidecarData: [],
  integrityData: null,
};

/** @type {Record<string, string>} */
export const CHECK_LABELS = {
  drift: 'Drift', orphans: 'Orphans', untracked: 'Untracked',
  sidecars: 'Sidecars', integrity: 'Integrity',
};

/** @type {Record<string, {label: string, cls: string, hint: string}>} */
export const HASH_STATUS_LABELS = {
  corrupt: {label: 'Possible corruption', cls: 'hash-corrupt',
            hint: 'content changed but file mtime did not — bit rot or disk fault; restore from backup'},
  modified: {label: 'Modified outside Vireo', cls: 'hash-modified',
             hint: 'content and mtime both changed — likely an external edit; rescan or accept'},
  unreadable: {label: 'Unreadable', cls: 'hash-unreadable',
               hint: 'the file exists but could not be read'},
};
