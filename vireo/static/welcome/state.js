// Welcome (first-run setup) page state. ES module: the page's other modules
// import `state` and read and write its fields; nothing here is a global.

/**
 * @typedef {object} WelcomeStarter
 * @property {string} id
 * @property {string} name
 * @property {number} size_mb
 */

/** The starter model the first step downloads. Injected by
 *  ``/config-defaults.js`` as ``window.VIREO_WELCOME_STARTER`` because
 *  templates stay Jinja-free. */
export const STARTER = /** @type {WelcomeStarter} */ (
  /** @type {any} */ (window).VIREO_WELCOME_STARTER
);

/** The classification model the first step downloads. */
export const MODEL_ID = STARTER.id;

/**
 * @typedef {object} WelcomeState
 * @property {boolean} modelAlreadyDownloaded The model is on disk already, so
 *   Get Started skips the download.
 * @property {boolean} classificationReady Classification is usable (a
 *   label-free model, or labels present), so setup can finish.
 * @property {number | null} selectedPlaceId The region picked in step 2.
 * @property {string} selectedPlaceName
 * @property {ReturnType<typeof setTimeout> | null} placeSearchTimer The
 *   pending debounced region search.
 */

/** @type {WelcomeState} */
export const state = {
  modelAlreadyDownloaded: false,
  classificationReady: false,
  selectedPlaceId: null,
  selectedPlaceName: '',
  placeSearchTimer: null,
};

/** @param {string} id */
export function byId(id) {
  return /** @type {HTMLElement} */ (document.getElementById(id));
}
