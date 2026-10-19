// Logs page state. ES module: the page's other modules import `state` and
// read and write its fields; nothing here is a global.

/** Minimum-level filter order, lowest first. @type {Record<string, number>} */
export const LEVEL_ORDER = { 'DEBUG': 0, 'INFO': 1, 'WARNING': 2, 'ERROR': 3, 'CRITICAL': 4 };

export const state = {
  /** Lines appended since the page loaded or was last cleared. */
  lineCount: 0,
};
