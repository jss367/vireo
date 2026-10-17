// Page state: the type icons and order, the loaded records and identity groups, the
// records/groups toggle, sort and filter state, the selection, and the parent map.
// Classic page script; load boot.js after all definitions.

const TYPE_ICONS = {general:'\u25CF', taxonomy:'\uD83C\uDF3F', individual:'\uD83D\uDC64', location:'\uD83D\uDCCD', genre:'\uD83C\uDFAD'};
const TYPES = ['general','taxonomy','individual','location','genre'];

let allKeywords = [];
let identityGroups = [];
let showRecords = false;
let sortCol = 'name';
let sortDir = 1; // 1 = asc, -1 = desc
let filterType = 'all';
let searchTerm = '';
let selectedIds = new Set();
let parentMap = {};
let activeDropdown = null;
