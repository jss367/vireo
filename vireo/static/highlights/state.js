// Highlights page state: the last response, which rows are expanded, the
// folder name, the debounce timer, the collections, the species modal,
// the lightbox photo, and the reject and load sequences that keep reloads
// from resurrecting a photo the user just rejected.
// ES module.

/** The folder dropdown's value for the whole workspace. */
export var WORKSPACE_SCOPE = '__workspace__';

/**
 * @typedef {object} HighlightsState
 * @property {HighlightsData | null} currentData The last API response.
 * @property {Set<string>} expandedBuckets Species names whose row is expanded.
 * @property {boolean} unidentifiedExpanded Whether the unidentified row is expanded.
 * @property {string} currentFolderName The selected folder's name, for the Save default.
 * @property {ReturnType<typeof setTimeout> | null} debounceTimer The pending reload of a typed filter.
 * @property {{id: number, name: string}[]} existingCollections Collections the Save modal checks names against.
 * @property {boolean} speciesSuggestionsLoaded Whether the species modal's datalist is filled.
 * @property {number[]} speciesModalPhotoIds The photos the species modal will relabel.
 * @property {number | null} activeLightboxPhotoId The photo the lightbox shows.
 * @property {string | null} activeLightboxSpecies The bucket the lightbox was
 *   opened from (null for unidentified), so a photo in two species' buckets
 *   shows and acts on the entry the user opened.
 * @property {string | null} speciesModalFromSpecies The bucket species the
 *   species modal replaces (null to set the photo's species outright).
 * @property {Set<number>} rejectedFromGrid Photos the user has rejected from the
 *   lightbox this session and not since un-rejected. Drives grid removal AND
 *   guards reloads (see loadHighlights).
 * @property {{photoId: number, filename: string, previousFlag: string} | null} lastHighlightReject
 *   The reject the Undo bar reverts.
 * @property {number} highlightsLoadSeq Monotonic counter bumped each time a
 *   highlights reload is *issued*; with rejectedAtLoadSeq it lets a reload
 *   prune ONLY photos rejected at/after the in-flight request was sent (whose
 *   response predates the rejection), never photos the backend restored
 *   out-of-band (another tab, Undo panel), which it must be trusted to include.
 * @property {Record<number, number>} rejectedAtLoadSeq The load counter value at each photo's rejection.
 * @property {Record<string, number>} bucketRefetchSeq Per-bucket monotonic counter
 *   for refetchBucketFromZero. Rapid Pick/Unpick cycles on the same bucket can
 *   leave two /api/highlights/bucket refetches in flight; without a stamp, an
 *   older response can arrive after a newer one and overwrite the fresher
 *   photos/ordering keys.
 */

/** @type {HighlightsState} */
export const state = {
  currentData: null,
  expandedBuckets: new Set(),
  unidentifiedExpanded: false,
  currentFolderName: '',
  debounceTimer: null,
  existingCollections: [],
  speciesSuggestionsLoaded: false,
  speciesModalPhotoIds: [],
  activeLightboxPhotoId: null,
  activeLightboxSpecies: null,
  speciesModalFromSpecies: null,
  rejectedFromGrid: new Set(),
  lastHighlightReject: null,
  highlightsLoadSeq: 0,
  rejectedAtLoadSeq: {},
  bucketRefetchSeq: {},
};
