// Move page state: folders, the photo grid selection, the folder browser, the confirm callback and the rule editor.
// Classic page script; load boot.js after all definitions.

/* ---------- State ---------- */
/** @type {MoveFolder[]} */ var allFolders = [];
/** @type {MovePhoto[]} */ var photoList = [];
/** @type {Set<number>} */ var selectedPhotos = new Set();
var lastSelectedIdx = -1;
var photoLoadSeq = 0;
/** @type {string | null} */ var browserTargetInput = null;
var browserPath = '';
var browserHomePath = '';
var browseSeq = 0;
/** @type {string[]} */ var recentDestinations = [];
var recentDestinationsSeq = 0;
/** @type {(() => unknown) | null} */ var confirmCallback = null;
/** @type {number | null} */ var editingRuleId = null;
var quickMoveBusy = false;
