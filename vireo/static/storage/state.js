// Storage page state: the cache-settings autosave timer, the working-copy
// quota the server has committed and the one awaiting confirmation, and the
// data the last overview, masks, files, retained-storage and Hugging Face
// cache loads returned. ES module: the page's other modules import `state`
// and read and write its fields; nothing here is a global.

/**
 * @typedef {object} StorageState
 * @property {ReturnType<typeof setTimeout> | null} storageSaveTimer
 *   The debounced cache-settings autosave (config.js).
 * @property {number} committedWorkingCopyQuotaMb
 *   The working-copy quota the server last saved, in MB.
 * @property {number | null} workingCopySizeBytes
 *   Current working-copy usage; null = not yet measured (or the last
 *   /api/storage scan failed), so Apply cannot skip the eviction
 *   confirmation on a stale zero.
 * @property {number | null} pendingWorkingCopyQuotaMb
 *   The lower quota the confirmation dialog is asking about.
 * @property {StorageMasks | null} masksData
 * @property {string} modalType
 *   The cache type the Manage dialog lists ('' when closed).
 * @property {StorageFile[]} modalFiles
 * @property {Record<string, {count: number, size: number}>} storageSummary
 *   Counts and sizes per cache type, from the last overview render.
 * @property {StorageOverviewCategory[] | null} storageOverviewCategories
 *   The category cards, kept so a sort change re-renders without a fetch.
 * @property {RetainedStorageResponse | null} retainedStorageData
 * @property {HfCacheInventory | null} hfCacheInventory
 */

/** @type {StorageState} */
export const state = {
  storageSaveTimer: null,
  committedWorkingCopyQuotaMb: 20480,
  workingCopySizeBytes: null,
  pendingWorkingCopyQuotaMb: null,
  masksData: null,
  modalType: '',
  modalFiles: [],
  storageSummary: {},
  storageOverviewCategories: null,
  retainedStorageData: null,
  hfCacheInventory: null,
};
