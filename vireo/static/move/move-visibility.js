// Keep-visible preference and the affected-workspaces check (/api/move-photos/visibility).

import { state } from './state.js';
import { updatePhotoSelectionUI } from './photo-move.js';
import { safeFetch } from './shared.js';

export async function loadMoveVisibilityPreference() {
  try {
    var cfg = await safeFetch('/api/config');
    var checkbox = /** @type {HTMLInputElement} */ (document.getElementById('moveKeepVisible'));
    checkbox.checked = cfg.move_keep_visible_in_other_workspaces !== false;
    checkbox.disabled = false;
    state.moveVisibilityReady = true;
    updatePhotoSelectionUI();
  } catch (e) {
    /** @type {HTMLElement} */ (document.getElementById('moveAffectedWorkspaces')).textContent = 'Could not load the remembered move choice. Reload before moving photos.';
  }
}

export async function rememberMoveVisibility() {
  var checkbox = /** @type {HTMLInputElement} */ (document.getElementById('moveKeepVisible'));
  var choice = checkbox.checked;
  checkbox.disabled = true;
  state.moveVisibilityReady = false;
  try {
    await safeFetch('/api/config', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({move_keep_visible_in_other_workspaces: choice}),
    });
  } catch (e) {
    checkbox.checked = !choice;
  } finally {
    checkbox.disabled = false;
    state.moveVisibilityReady = true;
    refreshMoveVisibility(Array.from(state.selectedPhotos)).catch(function() {});
  }
}

/** @param {number[]} ids */
export async function refreshMoveVisibility(ids) {
  var seq = ++state.moveVisibilityRequest;
  var status = /** @type {HTMLElement} */ (document.getElementById('moveAffectedWorkspaces'));
  if (!ids.length) {
    status.textContent = 'Select photos to see affected workspaces.';
    return [];
  }
  status.textContent = 'Checking affected workspaces…';
  try {
    var data = await safeFetch('/api/move-photos/visibility', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_ids: ids}),
    });
    var names = (data.workspaces || []).map(function(ws) {
      return ws.name + ' (' + ws.photo_count + ' photo' + (ws.photo_count === 1 ? '' : 's') + ')';
    });
    if (seq === state.moveVisibilityRequest) {
      status.textContent = names.length ? 'Affected workspaces: ' + names.join(', ') +
        (/** @type {HTMLInputElement} */ (document.getElementById('moveKeepVisible')).checked ? '. Visibility will be kept for the moved photos.' : '. Moving permits removal of their visibility here; existing destination sharing still applies.') :
        'No other workspaces currently see these photos.';
    }
    return names;
  } catch (e) {
    if (seq === state.moveVisibilityRequest) status.textContent = 'Could not check affected workspaces. Try again before moving.';
    throw e;
  }
}
