// Misses page state: category order and labels, the loaded misses, the tuning
// config and preview state, focus, the multi-selection, the lightbox tracking,
// the applied filter snapshot, and the fail-closed deep-link scope error.
// Classic page script; load boot.js after all definitions.

/* ---------- State ---------- */
/** @type {MissCategory[]} */ var CATEGORY_ORDER = ['no_subject', 'clipped', 'oof'];
var CATEGORY_LABELS = {
  no_subject: 'No subject',
  clipped:    'Clipped / tiny',
  oof:        'Out of focus',
};
/** @type {Record<MissCategory,MissPhoto[]>} */ var missesData = { no_subject: [], clipped: [], oof: [] };
/** @type {MissConfig | null} */ var missConfig = null;
// The detector floor the currently rendered cards were actually derived
// with. Preview and recompute return their effective config alongside the
// rows, which may differ from the saved missConfig while the slider is
// unsaved — cards must quote the floor that produced them.
/** @type {number | null} */ var activeMissFloor = null;
/** @type {MissConfig | null} */ var originalMissConfig = null;
/** @type {ReturnType<typeof setTimeout> | null} */ var previewTimer = null;
var previewActive = false;
/** @type {MissPayload | null} */ var previewPayload = null;
var previewRequestId = 0;
var missConfigLoaded = false;
/** @type {MissCategory | null} */ var focusedCategory = null;
var focusedIndex = -1;
/** @type {Record<number,MissCategory>} */ var missLightboxCategoryById = {};
/** @type {{photoId:number;category:MissCategory}|null} */ var missLightboxCurrent = null;
// Multi-select state. Spans categories — ctrl-click on cards in different
// sections accumulates them all. The focused card doubles as the anchor for
// shift-click range selection (range stays within the clicked category).
/** @type {Set<number>} */ var selection = new Set();
var missLoadRequestId = 0;
/** @type {MissFilters} */ var appliedMissFilters = { rules: [], visual: null };
var initializingMissFilters = true;
// Set when a ?collection_id=/?folder_id= deep link points at a scope we can't
// resolve at boot (deleted collection/folder, degraded collection, or a
// /api/collections|/api/folders fetch failure). Skipping the missing rule
// silently would widen the initial /api/misses POST to the entire workspace,
// and bulk reject/recompute would then operate on photos the user never
// scoped. Fail closed instead: render an empty grid with an explanatory
// banner and block recompute until the user edits the filter bar.
/** @type {string | null} */ var legacyScopeError = null;
var legacyScopeRecoveryPending = false;
var MISS_TUNING_CONTROL_IDS = [
  'missCfgDetectorFloor',
  'missCfgNoSubject',
  'missCfgClassifierRescue',
  'missCfgBboxMin',
  'missCfgOofRatio',
  'missSaveDefaults',
  'missRecomputeBtn',
  'missResetPreviewBtn',
];
