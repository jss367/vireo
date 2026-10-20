// Misses page state: category order and labels, the loaded misses, the tuning
// config and preview state, focus, the multi-selection, the lightbox tracking,
// the applied filter snapshot, and the fail-closed deep-link scope error.
// ES module: the page's other modules import `state` and read and write its
// fields; nothing here is a global.

/** @type {MissCategory[]} */ export const CATEGORY_ORDER = ['no_subject', 'clipped', 'oof'];
export const CATEGORY_LABELS = {
  no_subject: 'No subject',
  clipped:    'Clipped / tiny',
  oof:        'Focus needs review',
};
export const MISS_TUNING_CONTROL_IDS = [
  'missCfgDetectorFloor',
  'missCfgNoSubject',
  'missCfgClassifierRescue',
  'missCfgBboxMin',
  'missCfgOofRatio',
  'missSaveDefaults',
  'missRecomputeBtn',
  'missResetPreviewBtn',
];

/**
 * @typedef {object} MissPageState
 * @property {Record<MissCategory,MissPhoto[]>} missesData
 * @property {MissConfig | null} missConfig
 * @property {number | null} activeMissFloor The detector floor the currently
 *   rendered cards were actually derived with. Preview and recompute return
 *   their effective config alongside the rows, which may differ from the saved
 *   missConfig while the slider is unsaved — cards must quote the floor that
 *   produced them.
 * @property {MissConfig | null} originalMissConfig
 * @property {ReturnType<typeof setTimeout> | null} previewTimer
 * @property {boolean} previewActive
 * @property {MissPayload | null} previewPayload
 * @property {number} previewRequestId
 * @property {boolean} missConfigLoaded
 * @property {MissCategory | null} focusedCategory
 * @property {number} focusedIndex
 * @property {Record<number,MissCategory>} missLightboxCategoryById
 * @property {{photoId:number;category:MissCategory}|null} missLightboxCurrent
 * @property {Set<number>} selection Multi-select state. Spans categories —
 *   ctrl-click on cards in different sections accumulates them all. The
 *   focused card doubles as the anchor for shift-click range selection (range
 *   stays within the clicked category).
 * @property {number} missLoadRequestId
 * @property {MissFilters} appliedMissFilters
 * @property {boolean} initializingMissFilters
 * @property {string | null} legacyScopeError Set when a ?collection_id= /
 *   ?folder_id= deep link points at a scope we can't resolve at boot (deleted
 *   collection/folder, degraded collection, or a /api/collections|/api/folders
 *   fetch failure). Skipping the missing rule silently would widen the initial
 *   /api/misses POST to the entire workspace, and bulk reject/recompute would
 *   then operate on photos the user never scoped. Fail closed instead: render
 *   an empty grid with an explanatory banner and block recompute until the
 *   user edits the filter bar.
 * @property {boolean} legacyScopeRecoveryPending
 */

/** @type {MissPageState} */
export const state = {
  missesData: { no_subject: [], clipped: [], oof: [] },
  missConfig: null,
  activeMissFloor: null,
  originalMissConfig: null,
  previewTimer: null,
  previewActive: false,
  previewPayload: null,
  previewRequestId: 0,
  missConfigLoaded: false,
  focusedCategory: null,
  focusedIndex: -1,
  missLightboxCategoryById: {},
  missLightboxCurrent: null,
  selection: new Set(),
  missLoadRequestId: 0,
  appliedMissFilters: { rules: [], visual: null },
  initializingMissFilters: true,
  legacyScopeError: null,
  legacyScopeRecoveryPending: false,
};
