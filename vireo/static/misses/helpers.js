// Pure helpers: thumbnail URLs, detection_box parsing, photo sort order, and
// the ?since= URL parameter.
// ES module.

import { sharedThumbnailUrl } from './shared.js';

/** @param {MissPhoto} p */ export function photoThumbnailUrl(p) {
  if (!p || p.id == null) return '';
  var shared = sharedThumbnailUrl();
  return shared
    ? shared(p)
    : '/thumbnails/' + p.id + '.jpg';
}

/* ---------- detection_box parsing ----------
 * Stored as a JSON string: '{"x": 0.12, "y": 0.22, "w": 0.3, "h": 0.4}'
 * where x/y/w/h are normalized [0,1] image coordinates.
 * Returns null if missing or unparseable.
 */
/** @param {MissPhoto["detection_box"]} raw */ export function parseDetectionBox(raw) {
  if (!raw) return null;
  if (typeof raw === 'object') return raw;
  try {
    var b = JSON.parse(raw);
    if (!b || b.x == null || b.w == null) return null;
    return b;
  } catch (e) {
    return null;
  }
}

/* ---------- Sorting: burst_id first, then filename ---------- */
/** @param {MissPhoto[]} photos */ export function sortPhotos(photos) {
  return photos.slice().sort(function(a, b) {
    var ab = a.burst_id || '';
    var bb = b.burst_id || '';
    if (ab !== bb) return ab < bb ? -1 : 1;
    var af = a.filename || '';
    var bf = b.filename || '';
    if (af !== bf) return af < bf ? -1 : 1;
    return (a.id || 0) - (b.id || 0);
  });
}

/* ---------- URL param: ?since=<iso-ts> scopes to current pipeline run ---------- */
export function getSinceParam() {
  try {
    var u = new URL(window.location.href);
    var s = u.searchParams.get('since');
    return s || null;
  } catch (e) { return null; }
}
