// Keywords page state: the type icons and order, the loaded records and
// identity groups, the records/groups toggle, sort and filter state, the
// selection, the parent map, the open type dropdown, the merge dialog's
// state, and the link-to-place flow's Maps state. ES module: the page's
// other modules import `state` and read and write its fields; nothing here is
// a global.

/** @type {Record<string,string>} */ export const TYPE_ICONS = {general:'●', taxonomy:'🌿', individual:'👤', location:'📍', genre:'🎭'};
export const TYPES = ['general','taxonomy','individual','location','genre'];

/**
 * @typedef {object} KeywordsState
 * @property {KeywordRecord[]} allKeywords
 * @property {KeywordIdentity[]} identityGroups
 * @property {boolean} showRecords
 * @property {string} sortCol
 * @property {number} sortDir 1 = asc, -1 = desc
 * @property {string} filterType
 * @property {string} searchTerm
 * @property {Set<number>} selectedIds
 * @property {Record<number,string>} parentMap
 * @property {HTMLDivElement|null} activeDropdown
 * @property {number[]} mergeIds
 * @property {string|null} mergeToken
 *   The server's answer that the merge preview is complete; Confirm stays
 *   disabled without one.
 * @property {number} mergeRequest
 *   Stamps each merge preview request; a response for an older request or a
 *   closed dialog is dropped.
 * @property {boolean} mergeSaving
 * @property {KeywordMergeOverrides} mergeOverrides
 *   The dialog's per-field picks. Absent key = "use the kept record's value",
 *   which is what the server falls back to, so an untouched field keeps
 *   tracking the record-to-keep dropdown instead of freezing on first render.
 * @property {number|null} mergeFieldsFor
 * @property {ReturnType<typeof setTimeout>|null} mergeDebounce
 * @property {KeywordLeafletMap|null} mergeMap
 * @property {KeywordLeafletLayer|null} mergeMapLayer
 * @property {KeywordMergeOptions|null} mergeFieldOptions
 *   Latest option set, read by the once-registered delegated handlers.
 * @property {string} googleMapsApiKey
 *   From /api/config (link-place.js); empty means no key.
 * @property {boolean} googleMapsPreferEnglish
 * @property {Promise<KeywordGoogle|undefined>|null} gmapsLoadPromise
 * @property {KeywordAutocomplete|null} activeLinkAutocomplete
 * @property {number|null} activeLinkKeywordId
 */

/** @type {KeywordsState} */
export const state = {
  allKeywords: [],
  identityGroups: [],
  showRecords: false,
  sortCol: 'name',
  sortDir: 1,
  filterType: 'all',
  searchTerm: '',
  selectedIds: new Set(),
  parentMap: {},
  activeDropdown: null,
  mergeIds: [],
  mergeToken: null,
  mergeRequest: 0,
  mergeSaving: false,
  mergeOverrides: {},
  mergeFieldsFor: null,
  mergeDebounce: null,
  mergeMap: null,
  mergeMapLayer: null,
  mergeFieldOptions: null,
  googleMapsApiKey: '',
  googleMapsPreferEnglish: true,
  gmapsLoadPromise: null,
  activeLinkAutocomplete: null,
  activeLinkKeywordId: null,
};
