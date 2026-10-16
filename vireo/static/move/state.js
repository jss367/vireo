// Move page state: folders, the photo grid selection, the folder browser, the confirm callback and the rule editor.
// Classic page script; load boot.js after all definitions.

/* ---------- State ---------- */
var allFolders = [];
var photoList = [];
var selectedPhotos = new Set();
var lastSelectedIdx = -1;
var photoLoadSeq = 0;
var browserTargetInput = null;
var browserPath = '';
var browserHomePath = '';
var browseSeq = 0;
var recentDestinations = [];
var recentDestinationsSeq = 0;
var confirmCallback = null;
var editingRuleId = null;
var quickMoveBusy = false;
