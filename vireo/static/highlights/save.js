// Saving the visible highlights as a collection (replace or new).
// ES module.

import { safeFetch } from './shared.js';
import { loadHighlights, sortedBuckets } from './loading.js';
import { state } from './state.js';

// Save modal
export async function showSaveModal() {
  /** @type {HTMLInputElement} */ (document.getElementById('saveNameInput')).value = 'Highlights - ' + state.currentScopeName;
  /** @type {HTMLElement} */ (document.getElementById('saveDuplicateWarning')).style.display = 'none';
  /** @type {HTMLButtonElement} */ (document.getElementById('replaceBtn')).style.display = 'none';
  state.existingCollections = await safeFetch('/api/collections');
  var proposed = /** @type {HTMLInputElement} */ (document.getElementById('saveNameInput')).value;
  var existing = state.existingCollections.find(function(c) { return c.name === proposed; });
  if (existing) {
    /** @type {HTMLElement} */ (document.getElementById('saveDuplicateWarning')).textContent =
      'A collection named "' + proposed + '" already exists.';
    /** @type {HTMLElement} */ (document.getElementById('saveDuplicateWarning')).style.display = 'block';
    /** @type {HTMLButtonElement} */ (document.getElementById('replaceBtn')).style.display = '';
  }
  /** @type {HTMLElement} */ (document.getElementById('saveModal')).classList.add('open');
}

export function closeSaveModal() {
  /** @type {HTMLElement} */ (document.getElementById('saveModal')).classList.remove('open');
}

export function collectVisiblePhotoIds() {
  // "What's currently visible on the page" = the photos rendered in
  // each bucket (respecting expand state) plus unidentified if shown.
  /** @type {number[]} */
  var ids = [];
  var perRow = parseInt(/** @type {HTMLInputElement} */ (document.getElementById('perRowSlider')).value);
  sortedBuckets().forEach(function(b) {
    var visible = state.expandedBuckets.has(b.species) ? b.photos : b.photos.slice(0, perRow);
    visible.forEach(function(p) { ids.push(p.id); });
  });
  var unid = /** @type {HighlightsData} */ (state.currentData).unidentified;
  if (unid && unid.photo_count) {
    var u = unid.photos;
    var v = state.unidentifiedExpanded ? u : u.slice(0, perRow);
    v.forEach(function(p) { ids.push(p.id); });
  }
  return ids;
}

/** @param {'replace' | 'new'} mode */
export async function doSave(mode) {
  var name = /** @type {HTMLInputElement} */ (document.getElementById('saveNameInput')).value.trim();
  if (!name) return;
  var photoIds = collectVisiblePhotoIds();

  if (!photoIds.length) return;
  var existing = state.existingCollections.find(function(c) { return c.name === name; });
  if (mode === 'new') {
    var baseName = name;
    var counter = 2;
    var names = state.existingCollections.map(function(c) { return c.name; });
    while (names.indexOf(name) >= 0) {
      name = baseName + ' (' + counter + ')';
      counter++;
    }
  }

  if (mode === 'replace' && existing) {
    // Keep the scope's collection ID when replacing its photos.
    await safeFetch('/api/collections/' + existing.id, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ rules: [{ field: 'photo_ids', value: Array.from(new Set(photoIds)) }], visual: null }),
    });
  } else {
    await safeFetch('/api/highlights/save', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ photo_ids: photoIds, name: name }),
    });
  }
  closeSaveModal();
  await loadHighlights(true);
}
