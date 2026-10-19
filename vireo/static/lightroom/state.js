// Lightroom Import page state. ES module: the page's other modules import
// `state` and read and write its fields; nothing here is a global.

export const state = {
  /** The .lrcat paths added so far, in the order the user added them. @type {string[]} */
  catalogs: [],
};

/** @param {string} id */
export function byId(id) {
  return /** @type {HTMLElement} */ (document.getElementById(id));
}
