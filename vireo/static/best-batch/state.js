// Best Batch page state. ES module: the page's other modules import `state`
// and read and write its fields; nothing here is a global.

/**
 * @typedef {object} BestBatchState
 * @property {BestBatchResponse | null} bestBatchData The ranked batch on screen.
 * @property {boolean} comparisonActive
 * @property {Record<string, number>} subjectOverrides
 * @property {number} requestSequence
 * @property {number[]} photoIds
 * @property {boolean} analyzing
 * @property {number | null} inspectedId The frame shown in the large preview.
 * @property {(() => void) | null} inspectImgFallback The preview image's
 *   pending error fallback (to its thumbnail), replaced on every inspect.
 */

/** @type {BestBatchState} */
export const state = {
  bestBatchData: null,
  comparisonActive: false,
  subjectOverrides: {},
  requestSequence: 0,
  photoIds: [],
  analyzing: false,
  inspectedId: null,
  inspectImgFallback: null,
};

/** @param {string} id */
export function byId(id) {
  return /** @type {HTMLElement} */ (document.getElementById(id));
}
