// Species actions: confirming a bucket's photos, the species modal,
// representatives, and adding, removing and reordering highlights.
// ES module.

import { safeFetch, showToast } from './shared.js';
import { updateHighlightsLightboxControls } from './lightbox.js';
import { loadHighlights } from './loading.js';
import { loadMoreBucket } from './render.js';
import { state } from './state.js';

/** @param {string | null} species @param {boolean} isUnidentified @returns {Promise<HighlightsPhoto[]>} */
export async function ensureFullBucketLoaded(species, isUnidentified) {
  var target = isUnidentified
    ? (state.currentData && state.currentData.unidentified)
    : (state.currentData && state.currentData.buckets || []).find(function(b) { return b.species === species; });
  if (!target) return [];
  while (target.has_more) {
    await loadMoreBucket(species, isUnidentified);
  }
  return target.photos || [];
}

/** @param {HighlightsBucket} bucket */
export async function bucketPhotoIds(bucket) {
  var photos = await ensureFullBucketLoaded(bucket.species, false);
  return photos.map(function(p) { return p.id; });
}

export async function unidPhotoIds() {
  var photos = await ensureFullBucketLoaded(null, true);
  return photos
    .map(function(p) { return p.id; });
}

/**
 * The species the photo is tagged with besides the one this entry's bucket
 * shows. @param {HighlightsPhoto | null | undefined} photo @returns {string[]}
 */
export function otherTaggedSpecies(photo) {
  if (!photo) return [];
  var bucket = photo.bucket_species || null;
  return (photo.confirmed_species_list || []).filter(function(name) { return name !== bucket; });
}

/** @param {HighlightsPhoto | null | undefined} photo */
export function photoCurrentLabel(photo) {
  if (!photo) return '';
  var others = otherTaggedSpecies(photo);
  var also = others.length ? ' · also tagged ' + others.join(', ') : '';
  if (photo.has_accepted_species && photo.species) {
    return 'Keyword confirmed: ' + photo.species + also;
  }
  if (photo.predicted_species) {
    var conf = photo.predicted_confidence != null
      ? ' (' + Math.round(photo.predicted_confidence * 100) + '%)'
      : '';
    return 'Predicted: ' + photo.predicted_species + conf + also;
  }
  return 'Unidentified' + also;
}

/** @param {HighlightsPhoto | null | undefined} photo @returns {string} */
export function photoSpeciesName(photo) {
  if (!photo) return '';
  if (photo.has_accepted_species && photo.species) return photo.species;
  if (photo.predicted_species) return photo.predicted_species;
  return '';
}

/**
 * Confirm ``species`` (the bucket the user confirmed from) on each photo, so
 * a photo already tagged with another species gains this one too.
 * @param {number[]} photoIds @param {string} species @param {HTMLButtonElement | null | undefined} button
 */
export async function confirmPhotoIds(photoIds, species, button) {
  if (!photoIds.length || !species) return;
  if (button) button.disabled = true;
  try {
    await safeFetch('/api/highlights/confirm', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ photo_ids: photoIds, species: species }),
    });
    await loadHighlights();
    if (typeof showToast === 'function') showToast('Species keyword confirmed', 'success');
  } finally {
    if (button) button.disabled = false;
  }
}

export async function loadSpeciesSuggestions() {
  if (state.speciesSuggestionsLoaded) return;
  state.speciesSuggestionsLoaded = true;
  try {
    /** @type {{name?: string, type?: string, taxon_name?: string, taxon_common_name?: string}[] | null} */
    var keywords = await safeFetch('/api/keywords/all', {}, { toast: false });
    var list = /** @type {HTMLElement} */ (document.getElementById('highlightSpeciesList'));
    list.innerHTML = '';
    (keywords || []).filter(function(k) {
      return k && k.name && (k.type === 'taxonomy' || k.taxon_name || k.taxon_common_name);
    }).forEach(function(k) {
      var opt = document.createElement('option');
      opt.value = k.name || '';
      list.appendChild(opt);
    });
  } catch (e) {
    state.speciesSuggestionsLoaded = false;
    // Free typing still works if suggestions cannot be loaded.
  }
}

/**
 * ``fromSpecies`` is the bucket being changed: only that species is replaced
 * on a photo that carries others. Null sets the photos' species outright.
 * @param {string} title @param {string} summary @param {number[]} photoIds @param {string} initialSpecies
 * @param {string | null} fromSpecies
 */
export function showSpeciesModal(title, summary, photoIds, initialSpecies, fromSpecies) {
  state.speciesModalPhotoIds = photoIds.slice();
  state.speciesModalFromSpecies = fromSpecies || null;
  /** @type {HTMLElement} */ (document.getElementById('speciesModalTitle')).textContent = title;
  /** @type {HTMLElement} */ (document.getElementById('speciesModalSummary')).textContent = summary;
  var input = /** @type {HTMLInputElement} */ (document.getElementById('speciesModalInput'));
  input.value = initialSpecies || '';
  /** @type {HTMLElement} */ (document.getElementById('speciesModal')).classList.add('open');
  loadSpeciesSuggestions();
  setTimeout(function() { input.focus(); input.select(); }, 0);
}

export function closeSpeciesModal() {
  /** @type {HTMLElement} */ (document.getElementById('speciesModal')).classList.remove('open');
  state.speciesModalPhotoIds = [];
  state.speciesModalFromSpecies = null;
}

export async function submitSpeciesModal() {
  var input = /** @type {HTMLInputElement} */ (document.getElementById('speciesModalInput'));
  var species = input.value.trim();
  if (!species || !state.speciesModalPhotoIds.length) return;
  var submit = /** @type {HTMLButtonElement} */ (document.getElementById('speciesModalSubmit'));
  submit.disabled = true;
  try {
    await safeFetch('/api/highlights/relabel', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(Object.assign(
        { photo_ids: state.speciesModalPhotoIds, species: species },
        state.speciesModalFromSpecies ? { from_species: state.speciesModalFromSpecies } : {}
      )),
    });
    closeSpeciesModal();
    await loadHighlights();
    updateHighlightsLightboxControls(state.activeLightboxPhotoId);
    if (typeof showToast === 'function') showToast('Species updated', 'success');
  } finally {
    submit.disabled = false;
  }
}

/** @param {string | null | undefined} species @param {number} photoId @param {HTMLButtonElement | null | undefined} button */
export async function setSpeciesRepresentative(species, photoId, button) {
  if (!species || !photoId) return;
  if (button) button.disabled = true;
  try {
    await safeFetch('/api/photo-preferences', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ purpose: 'species_representative', species: species, photo_id: photoId }),
    });
    await loadHighlights();
    updateHighlightsLightboxControls(state.activeLightboxPhotoId);
    if (typeof showToast === 'function') {
      showToast('Species representative set', 'success');
    }
  } finally {
    if (button) button.disabled = false;
  }
}

/** @param {string} species @param {number} photoId @param {HTMLButtonElement | null | undefined} button */
export async function addSpeciesHighlight(species, photoId, button) {
  if (!species || !photoId) return;
  if (button) button.disabled = true;
  try {
    await safeFetch('/api/species-highlights', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ species: species, photo_id: photoId }),
    });
    await loadHighlights();
    updateHighlightsLightboxControls(state.activeLightboxPhotoId);
    if (typeof showToast === 'function') showToast('Added to Highlights', 'success');
  } finally {
    if (button) button.disabled = false;
  }
}

/** @param {string} species @param {number} photoId @param {HTMLButtonElement | null | undefined} button */
export async function removeSpeciesHighlight(species, photoId, button) {
  if (!species || !photoId) return;
  if (button) button.disabled = true;
  try {
    await safeFetch('/api/species-highlights', {
      method: 'DELETE',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ species: species, photo_id: photoId }),
    });
    await loadHighlights();
    updateHighlightsLightboxControls(state.activeLightboxPhotoId);
    if (typeof showToast === 'function') showToast('Removed from Highlights', 'success');
  } finally {
    if (button) button.disabled = false;
  }
}

/** @param {string} species @param {number} photoId @param {string} direction @param {HTMLButtonElement | null | undefined} button */
export async function moveSpeciesHighlight(species, photoId, direction, button) {
  if (!species || !photoId || !direction) return;
  if (button) button.disabled = true;
  try {
    await safeFetch('/api/species-highlights/order', {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ species: species, photo_id: photoId, direction: direction }),
    });
    await loadHighlights();
    updateHighlightsLightboxControls(state.activeLightboxPhotoId);
  } finally {
    if (button) button.disabled = false;
  }
}
