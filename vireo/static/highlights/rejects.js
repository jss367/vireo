// Rejected photos: whether a curation filter needs a reload, and removing
// a rejected photo from the loaded buckets.
// ES module.

import { state } from './state.js';

// True when a rejection needs a reload rather than just a local prune: an
// active yes/no curation filter treats the rejected photo's highlight or
// representative role as an active bucket signal, and the eligible-only
// query on the server would drop it. If we only prune locally the bucket
// keeps its stale has_highlight_selection / has_species_representative
// state and stays visible under the wrong filter.
/** @param {HighlightsPhoto | null | undefined} photo */
export function curationFilterAffectedByReject(photo) {
  if (!photo) return false;
  var highlightFilter = /** @type {HTMLSelectElement} */ (document.getElementById('highlightSelectionFilter')).value || 'all';
  var repFilter = /** @type {HTMLSelectElement} */ (document.getElementById('representativeFilter')).value || 'all';
  if (highlightFilter !== 'all' && photo.is_highlighted) return true;
  if (repFilter !== 'all' && photo.is_species_representative) return true;
  return false;
}

// Drop a photo from the in-memory model after it is rejected, so the grid
// matches the backend query (which excludes rejected photos). Returns true if
// anything changed.
/** @param {number} photoId */
export function removeRejectedPhotoFromModel(photoId) {
  if (!state.currentData) return false;
  var changed = false;

  /** @param {HighlightsSection | null | undefined} section */
  function pruneSection(section) {
    if (!section || !Array.isArray(section.photos)) return;
    var before = section.photos.length;
    // Replace (don't splice) the array so any open lightbox keeps navigating
    // its original, stable photo set.
    var kept = section.photos.filter(function(p) { return p.id !== photoId; });
    if (kept.length !== before) {
      section.photos = kept;
      section.photo_count = Math.max(0, (section.photo_count || before) - 1);
      if (typeof section.loaded_count === 'number') {
        section.loaded_count = Math.max(0, section.loaded_count - 1);
      }
      changed = true;
    }
  }

  (state.currentData.buckets || []).forEach(pruneSection);
  pruneSection(state.currentData.unidentified);

  if (changed) {
    // Drop only buckets fully emptied by the rejection. A large bucket may
    // still have eligible photos behind `has_more` even after every loaded
    // card is rejected (photo_count stays positive), so keep it — its expand
    // button is the user's only way to load the rest without a refresh.
    if (state.currentData.buckets) {
      state.currentData.buckets = state.currentData.buckets.filter(function(b) {
        return (b.photo_count || 0) > 0 || (b.photos || []).length > 0;
      });
    }
    if (state.currentData.meta && typeof state.currentData.meta.eligible === 'number') {
      state.currentData.meta.eligible = Math.max(0, state.currentData.meta.eligible - 1);
    }
  }
  return changed;
}
