// Dashboard page state: the scope, its options, the last stats, the
// folder coverage cache and which detail panels are open.
// ES module.

/**
 * @typedef {object} DashboardState
 * @property {DashboardScope} dashboardScope The scope applied to every section.
 * @property {DashboardOptions} dashboardOptions The folders and collections the scope can pick.
 * @property {DashboardStats | null} dashboardStats The last /api/stats response.
 * @property {DashboardCoverageFolder[] | null} _coverageFolders Per-folder coverage, cached until the next reload.
 * @property {boolean} folderOpen Whether the folder detail panel is open.
 * @property {boolean} pendingOpen Whether the pending-change detail panel is open.
 */

/** @type {DashboardState} */
export const state = {
  dashboardScope: {folder_id: null, collection_id: null, date_from: '', date_to: ''},
  dashboardOptions: {folders: [], collections: []},
  dashboardStats: null,
  _coverageFolders: null,
  folderOpen: false,
  pendingOpen: false,
};
