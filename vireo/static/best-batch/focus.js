// Inspection never changes saved subjects, scores, flags, or edit recipes.
import { byId, state } from './state.js';

let sequence = 0;
let controller = /** @type {AbortController | null} */ (null);
let choosing = false;
let eyeUrl = /** @type {string | null} */ (null);

/** @param {[number, number, number, number] | undefined} box */
function markSubject(box) {
  var outline = byId('focusSubjectBox');
  outline.hidden = !box;
  if (box) {
    outline.style.left = box[0] * 100 + '%';
    outline.style.top = box[1] * 100 + '%';
    outline.style.width = box[2] * 100 + '%';
    outline.style.height = box[3] * 100 + '%';
  }
}

/** @param {Headers} headers */
function showFocusEvidence(headers) {
  var basis = headers.get('X-Vireo-Focus-Basis');
  var subject = headers.get('X-Vireo-Focus-Detection-Id');
  var confidence = Number(headers.get('X-Vireo-Focus-Detection-Confidence'));
  var box = headers.get('X-Vireo-Focus-Subject-Box')?.split(',').map(Number);
  // Manual inspection changes the point, while the comparison subject stays outlined.
  if (basis !== 'point') {
    markSubject(box?.length === 4 && box.every(Number.isFinite) ? /** @type {[number, number, number, number]} */ (box) : undefined);
  }
  var eyes = headers.get('X-Vireo-Focus-Eye-Status');
  /** @type {Record<string, string>} */
  var eyeMessages = {located: 'Eye location is bound to this subject analysis.', unbound: 'No subject-bound eye evidence.',
    disabled: 'Eye analysis is off.', model_unavailable: 'Eye model is unavailable.',
    identity_unavailable: 'Eye analysis needs a supported species identification.', not_located: 'No reliable eye located.'};
  var eyeText = eyeMessages[eyes || ''];
  var label = basis === 'eye' ? 'Eye-centered window.' : basis === 'subject' ?
    'Subject-box-centered window; the head or eye may be outside this crop.' : basis === 'point' ?
      'Manual inspection center; eye visibility is for you to verify.' : 'Frame-centered window; no animal location was used.';
  byId('focusEvidenceStatus').textContent = label + ' ' +
    (subject ? 'Outlined model subject · detector confidence ' + Math.round(confidence * 100) + '%. Verify the animal. ' : '') +
    (eyeText || 'Eye evidence unavailable.') + ' Inspect the full frame and intended animal before rejecting the photo.';
}

/** @param {[number, number] | null} center */
function markCenter(center) {
  var marker = byId('focusCenter');
  marker.hidden = !center;
  if (center) {
    marker.style.left = center[0] * 100 + '%';
    marker.style.top = center[1] * 100 + '%';
  }
}

/** @param {Response} response */
async function focusResponseError(response) {
  var fallback = 'Focus evidence unavailable (HTTP ' + response.status + '). Refresh the photo or choose another center.';
  if ((response.headers.get('Content-Type') || '').includes('application/json')) {
    try {
      var data = await response.json();
      if (typeof data?.error === 'string' && data.error.trim()) return data.error;
    } catch (_error) { /* A malformed error body still means the focus pixels are unavailable. */ }
  }
  return fallback;
}

/** @param {string} url */
async function loadFocus(url) {
  var current = ++sequence;
  eyeUrl = null;
  if (controller) controller.abort();
  controller = new AbortController();
  var image = /** @type {HTMLImageElement} */ (byId('inspectedFocusImg'));
  image.hidden = true;
  image.removeAttribute('src');
  byId('focusEvidenceStatus').textContent = 'Verifying crop center and subject-bound eye evidence…';
  byId('focusRasterStatus').textContent = 'Loading focus pixels…';
  try {
    var response = await fetch(url, {signal: controller.signal});
    if (!response.ok) throw new Error(await focusResponseError(response));
    var blob = await response.blob();
    if (current !== sequence) return;
    var old = image.dataset.objectUrl;
    var objectUrl = URL.createObjectURL(blob);
    image.dataset.objectUrl = objectUrl;
    image.src = objectUrl;
    image.hidden = false;
    if (old) URL.revokeObjectURL(old);
    var headers = response.headers;
    showFocusEvidence(headers);
    // Only generation-bound eye locations authorize an automatic head/eye view.
    eyeUrl = headers.get('X-Vireo-Focus-Eye-Status') === 'located' && url.includes('analysis_key=') ? url : null;
    var center = (headers.get('X-Vireo-Focus-Center') || '').split(',').map(Number);
    if (center.length === 2 && center.every(function(v) { return Number.isFinite(v) && v >= 0 && v <= 1; })) {
      markCenter(/** @type {[number, number]} */ (center));
    } else markCenter(null);
    byId('focusRasterStatus').textContent = 'Basis: ' + headers.get('X-Vireo-Focus-Basis') +
      ' · source: ' + headers.get('X-Vireo-Focus-Source') +
      ' · raster: ' + headers.get('X-Vireo-Focus-Raster-Size') +
      ' · original scale: ' + headers.get('X-Vireo-Focus-Original-Scale') +
      ' · 1 crop pixel per decoded raster pixel. Display may be fitted to this panel.';
  } catch (error) {
    if (current !== sequence) return;
    markCenter(null);
    eyeUrl = null;
    byId('focusEvidenceStatus').textContent = 'Eye evidence unavailable. Inspect the intended head in the full frame or place a manual center before rejecting the photo.';
    byId('focusRasterStatus').textContent = error instanceof Error ? error.message : String(error);
  }
}

/** @param {BestBatchCard} card */
export function renderFocus(card) {
  choosing = false;
  eyeUrl = null;
  byId('manualFocusBtn').setAttribute('aria-pressed', 'false');
  markCenter(null);
  var box = card.subject?.candidates.find(function(c) { return c.detection_id === card.subject?.detection_id; })?.box;
  markSubject(box ? [box.x, box.y, box.w, box.h] : undefined);
  byId('focusEvidenceStatus').textContent = 'Eye evidence has not been verified for this inspection. Inspect the head in the full frame before rejecting the photo.';
  byId('focusSelectionStatus').textContent = 'Full frame uses upright source coordinates before saved crop or rotation. Boxes are model suggestions; verify the animal.';
  if (card.subject && !card.focus_crop_url) {
    sequence++;
    if (controller) controller.abort();
    byId('inspectedFocusImg').hidden = true;
    byId('focusRasterStatus').textContent = 'Selected subject evidence is unavailable. Analyze it or place a manual center.';
  } else loadFocus(card.focus_crop_url || '/photos/' + card.id + '/focus-crop?size=512');
}

export function bindFocusControls() {
  byId('headFocusBtn').addEventListener('click', function() {
    if (eyeUrl) {
      var url = new URL(eyeUrl, window.location.origin);
      url.searchParams.set('basis', 'eye');
      loadFocus(url.pathname + url.search);
    } else {
      choosing = true;
      byId('manualFocusBtn').setAttribute('aria-pressed', 'true');
      byId('focusSelectionStatus').textContent = 'Click the intended animal’s head or eye in the full frame for a head-centered view. No reliable subject-bound eye location is available.';
    }
  });
  byId('manualFocusBtn').addEventListener('click', function() {
    choosing = !choosing;
    byId('manualFocusBtn').setAttribute('aria-pressed', String(choosing));
    byId('focusSelectionStatus').textContent = choosing ? 'Click the eye or animal in the full frame to place a manual focus center.' : 'Manual placement off.';
  });
  byId('resetFocusBtn').addEventListener('click', function() {
    var card = state.bestBatchData?.cards?.find(function(c) { return c.id === state.inspectedId; });
    if (card) renderFocus(card);
  });
  byId('inspectImg').addEventListener('click', function(event) {
    if (!choosing || !state.inspectedId) return;
    var image = /** @type {HTMLImageElement} */ (event.currentTarget);
    if (!image.complete || !image.naturalWidth) return;
    var rect = image.getBoundingClientRect();
    var x = Math.max(0, Math.min(1, (event.clientX - rect.left) / rect.width));
    var y = Math.max(0, Math.min(1, (event.clientY - rect.top) / rect.height));
    markCenter([x, y]);
    byId('focusSelectionStatus').textContent = 'Manual center: ' + x.toFixed(4) + ', ' + y.toFixed(4) + '. Inspection only; ranking still uses the selected subject.';
    loadFocus('/photos/' + state.inspectedId + '/focus-crop?size=512&source=auto&center=' + x + ',' + y);
  });
}
