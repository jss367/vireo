// Finding a photo in the loaded buckets and refetching one bucket from the
// top so its order matches the server's.
// ES module.

import { safeFetch } from './shared.js';
import { appendHighlightFilterParams, requestScopeParams } from './scope.js';
import { state } from './state.js';

/** @param {number | null | undefined} photoId */
export function findHighlightPhoto(photoId) {
  var hit = findHighlightPhotoWithContainer(photoId);
  return hit ? hit.photo : null;
}

/**
 * @param {number | null | undefined} photoId
 * @returns {{photo: HighlightsPhoto, photos: HighlightsPhoto[], bucket: HighlightsBucket | null} | null}
 */
export function findHighlightPhotoWithContainer(photoId) {
  if (!state.currentData || photoId == null) return null;
  var buckets = state.currentData.buckets || [];
  for (var i = 0; i < buckets.length; i++) {
    var bucket = /** @type {HighlightsBucket} */ (buckets[i]);
    var photos = bucket.photos || [];
    var match = photos.find(function(p) { return p.id === photoId; });
    if (match) return { photo: match, photos: photos, bucket: bucket };
  }
  var unid = state.currentData.unidentified && state.currentData.unidentified.photos || [];
  var unidMatch = unid.find(function(p) { return p.id === photoId; });
  return unidMatch ? { photo: unidMatch, photos: unid, bucket: null } : null;
}

// Copy the authoritative full-bucket ordering keys from a bucket-endpoint
// page onto the client-side bucket so sortedBuckets() ranks species the
// same way a reload would. Prefer the endpoint's `best_score` /
// `best_timestamp` (computed over the entire filtered bucket) over
// recomputing from `bucket.photos`, because a paged refetch (has_more
// still true) only loads a prefix — recomputing would drop a species
// below its true Recommended/Best position when the highest-scored photo
// lives past the loaded window. Falls back to a local scan only if the
// endpoint didn't return the keys (older server, defensive).
/** @param {HighlightsSection | null | undefined} bucket @param {HighlightsBucketPage | null} page */
export function applyBucketOrderingKeysFromPage(bucket, page) {
  if (!bucket) return;
  if (page && Object.prototype.hasOwnProperty.call(page, 'best_score')) {
    bucket.best_score = page.best_score;
  } else if (Array.isArray(bucket.photos)) {
    var best = null;
    for (var i = 0; i < bucket.photos.length; i++) {
      var s = /** @type {HighlightsPhoto} */ (bucket.photos[i]).highlight_score;
      if (typeof s !== 'number') continue;
      if (best === null || s > best) best = s;
    }
    bucket.best_score = best;
  }
  if (page && Object.prototype.hasOwnProperty.call(page, 'best_timestamp')) {
    bucket.best_timestamp = page.best_timestamp;
  } else {
    var top = Array.isArray(bucket.photos) ? bucket.photos[0] : null;
    bucket.best_timestamp = top ? (top.timestamp || null) : null;
  }
}

// Refetch a bucket (or the unidentified section) from offset 0 so the loaded
// window matches the backend's post-pick ordering. A pick mutates
// highlight_score (the +0.08 bonus, clamped to [0, 1]) and can flip ordering
// against representatives, curated species_highlights, or the has_more tail;
// rather than mirror `_highlight_score_bucket` / `_apply_ordered_highlights` /
// `_apply_highlight_preferences` in JS (repeatedly getting it wrong on subtle
// edge cases), let the server compute the authoritative order and copy it
// back. Pages in up-to-500-row chunks so the final iteration only asks for
// the remainder of the loaded window (`targetLen - offset`) — a full 500-row
// last page would silently expand the loaded slice past what the user
// actually loaded (e.g. 520 loaded would become 1000). The endpoint clamps
// `limit` to 500, so the per-request cap is preserved automatically.
// Refetch size mirrors the user's currently loaded window (`currentLen`) —
// forcing it up to 500 would silently expand an unexpanded 20-row bucket to
// 500 rows, and the next "Load more" (which offsets by `target.photos.length`)
// would then start at 500 and render up to 1000 cards the user never paged
// through, plus bloat every pick's Save-as-Collection payload.
// Returns a promise resolving to true only when this call actually wrote to
// `target.photos` — a superseded (older) refetch resolves to false so its
// caller can skip rendering the stale locally-mutated flag against the old
// bucket order and let the winning refetch's render fire instead.
/** @param {HighlightsSection | null | undefined} target @param {boolean} isUnidentified */
export async function refetchBucketFromZero(target, isUnidentified) {
  if (!target) return true;
  var currentLen = Array.isArray(target.photos) ? target.photos.length : 0;
  var speciesKey = isUnidentified ? '__unidentified__' : /** @type {string} */ (target.species);
  var targetLen = currentLen;
  if (targetLen <= 0) return true;
  // Stamp this refetch so a slower earlier one for the same bucket can't
  // clobber a newer response (Pick → Unpick before the first GET resolves).
  var seq = (state.bucketRefetchSeq[speciesKey] || 0) + 1;
  state.bucketRefetchSeq[speciesKey] = seq;
  /** @type {HighlightsPhoto[]} */
  var allPhotos = [];
  /** @type {HighlightsBucketPage | null} */
  var lastPage = null;
  for (var offset = 0; offset < targetLen; offset += 500) {
    var params = requestScopeParams();
    appendHighlightFilterParams(params);
    // appendHighlightFilterParams may set 'species' for the species-search
    // filter; force it back to the target species/unidentified sentinel so
    // the endpoint returns the right bucket, not whatever the search box holds.
    params.set('species', speciesKey);
    params.set('offset', String(offset));
    params.set('limit', String(Math.min(500, targetLen - offset)));
    /** @type {HighlightsBucketPage | null} */
    var page = await safeFetch('/api/highlights/bucket?' + params);
    if (state.bucketRefetchSeq[speciesKey] !== seq) return false;
    if (!page || !Array.isArray(page.photos)) return true;
    lastPage = page;
    if (page.photos.length === 0) break;
    allPhotos = allPhotos.concat(page.photos);
    if (!page.has_more) break;
  }
  if (!lastPage) return true;
  if (state.bucketRefetchSeq[speciesKey] !== seq) return false;
  target.photos = allPhotos;
  target.loaded_count = allPhotos.length;
  target.has_more = !!lastPage.has_more;
  if (typeof lastPage.photo_count === 'number') target.photo_count = lastPage.photo_count;
  applyBucketOrderingKeysFromPage(target, lastPage);
  return true;
}
