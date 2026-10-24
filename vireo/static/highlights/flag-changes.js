// The lightbox's flag changes: a reject leaves the grid (with Undo), an
// un-reject reloads, and a pick or unpick refetches its bucket so the order
// matches the server's.
// ES module.

import { advanceHighlightLightboxAfterReject, showHighlightUndo } from './lightbox.js';
import { loadHighlights } from './loading.js';
import { findAllHighlightPhotoContainers, findHighlightPhoto, refetchBucketFromZero } from './ordering.js';
import { curationFilterAffectedByReject, removeRejectedPhotoFromModel } from './rejects.js';
import { render } from './render.js';
import { state } from './state.js';

/** @param {Event} e */
export function onHighlightFlagChanged(e) {
  var detail = /** @type {CustomEvent<HighlightsFlagChange>} */ (e).detail;
  if (!detail || detail.photoId == null) return;
  if (detail.flag === 'rejected') {
    var photo = findHighlightPhoto(detail.photoId);
    if (photo) showHighlightUndo(photo, detail.previousFlag || photo.flag || 'none');
    advanceHighlightLightboxAfterReject(detail.photoId);
    // Track before mutating: during an in-flight restore reload the photo may
    // not be in state.currentData yet, so removeRejectedPhotoFromModel returns false
    // — but the id must still be remembered so the reload's prune drops it.
    // Stamp the current load seq so only reloads issued no later than now prune
    // it (a reload issued afterward already reflects the rejection server-side).
    state.rejectedFromGrid.add(detail.photoId);
    state.rejectedAtLoadSeq[detail.photoId] = state.highlightsLoadSeq;
    // A curation filter set to yes/no may need to hide the bucket when the
    // rejected photo is the last highlight or the species representative:
    // eligible_only server-side drops that stored row, but the local prune
    // path leaves has_highlight_selection / has_species_representative
    // stale. Reload authoritatively so the visible buckets match the
    // backend's filter state. Check every bucket entry: a two-species photo
    // may be the highlight or representative only in a bucket other than the
    // one the lightbox was opened from, and findHighlightPhoto returns the
    // preferred entry whose is_highlighted/is_species_representative would
    // then be false even though another bucket's state goes stale.
    var filterAffected = findAllHighlightPhotoContainers(detail.photoId)
      .some(function(h) { return curationFilterAffectedByReject(h.photo); });
    if (filterAffected) {
      removeRejectedPhotoFromModel(detail.photoId);
      loadHighlights();
    } else if (removeRejectedPhotoFromModel(detail.photoId)) {
      render();
    }
  } else if (state.rejectedFromGrid.has(detail.photoId)) {
    // A previously-rejected photo was un-rejected — it re-qualifies for
    // highlights, so refetch to restore it (and the Save-as-Collection list)
    // in its correct bucket. Tracking the set keeps this scoped to genuine
    // reappearances, not every Pick/Unflag on a photo still in the grid.
    state.rejectedFromGrid.delete(detail.photoId);
    delete state.rejectedAtLoadSeq[detail.photoId];
    loadHighlights();
  } else {
    // Pick/Unpick from the lightbox: the visible row is a slice(0, perRow)
    // of bucket.photos, and the backend applies the +0.08 highlight_score
    // bonus + picked_first ordering + `_apply_ordered_highlights` +
    // `_apply_highlight_preferences` before serving that ordering. Rather
    // than mirror all of that in JS (which has already produced a stream
    // of ordering bugs against representatives, curated highlights, the
    // score clamp, best_timestamp, and the has_more offset), refetch the
    // bucket authoritatively so the client window is always what a full
    // reload would produce.
    var prev = detail.previousFlag || 'none';
    var next = detail.flag || 'none';
    if (prev === next) return;
    if (prev !== 'flagged' && next !== 'flagged') return;
    var hits = findAllHighlightPhotoContainers(detail.photoId);
    if (!hits.length) {
      // Eviction path: a prior unpick's refetchBucketFromZero can drop a
      // photo out of the loaded window when it was only visible because
      // picked_first promoted it. If the lightbox is still open on that
      // photo and the user re-picks, we no longer have a bucket reference
      // for a targeted refetch — reload everything so the fresh
      // picked_first ordering brings the photo back into a bucket window
      // (and the Save-as-Collection payload).
      loadHighlights();
      return;
    }
    // Mutate the shared photo reference immediately so any UI that reads
    // photo.flag between now and the refetch resolving (e.g. the still-open
    // lightbox controls) reflects the new pick state; the refetch then
    // replaces the bucket's photo array with the server-authoritative order.
    // A photo with two species sits in two buckets; refetch each so both
    // rows show the pick and their server order.
    var refetches = hits.map(function(hit) {
      hit.photo.flag = next;
      var isUnidentified = hit.bucket === null;
      var target = isUnidentified ? /** @type {HighlightsData} */ (state.currentData).unidentified : hit.bucket;
      return target ? refetchBucketFromZero(target, isUnidentified) : Promise.resolve(true);
    });
    // Render after the authoritative refetches settle so the Pick badge/outline
    // and the bucket ordering become visible together. Rendering before the
    // refetch exposes a transient stale order: tests and collection saves can
    // observe the picked class while the photo is still in its old position.
    Promise.all(refetches)
      .then(function(applied) { if (applied.some(Boolean)) render(); })
      .catch(function() { render(); });
  }
}
