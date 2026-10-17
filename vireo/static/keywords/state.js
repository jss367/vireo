// Page state: the type icons and order, the loaded records and identity groups, the
// records/groups toggle, sort and filter state, the selection, and the parent map.
// Classic page script; load boot.js after all definitions.

/** @type {Record<string,string>} */ const TYPE_ICONS = {general:'\u25CF', taxonomy:'\uD83C\uDF3F', individual:'\uD83D\uDC64', location:'\uD83D\uDCCD', genre:'\uD83C\uDFAD'};
const TYPES = ['general','taxonomy','individual','location','genre'];

/** @type {KeywordRecord[]} */ let allKeywords = [];
/** @type {KeywordIdentity[]} */ let identityGroups = [];
let showRecords = false;
let sortCol = 'name';
let sortDir = 1; // 1 = asc, -1 = desc
let filterType = 'all';
let searchTerm = '';
/** @type {Set<number>} */ let selectedIds = new Set();
/** @type {Record<number,string>} */ let parentMap = {};
/** @type {HTMLDivElement|null} */ let activeDropdown = null;
