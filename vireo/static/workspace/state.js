// Workspace page state. ES module: the page's other modules import `state`
// and read and write its fields; nothing here is a global.

import { safeFetch } from './shared.js';

export const state = {
  /** The active workspace's folders by id, from the last folder render. @type {Record<number, WorkspaceFolder>} */
  foldersById: {},
};

/** @param {string} id */
export function byId(id) {
  return /** @type {HTMLElement} */ (document.getElementById(id));
}

/** The active workspace (`/api/workspaces/active`). @returns {Promise<ActiveWorkspace>} */
export function fetchActiveWorkspace() {
  return safeFetch('/api/workspaces/active', {}, { toast: false });
}

/** Every workspace (`/api/workspaces`). @returns {Promise<WorkspaceListEntry[]>} */
export function fetchWorkspaces() {
  return safeFetch('/api/workspaces', {}, { toast: false });
}
