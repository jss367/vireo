// Inspection never changes saved subjects, scores, flags, or edit recipes.
import { byId, state } from './state.js';

let sequence = 0;
let controller = /** @type {AbortController | null} */ (null);
let choosing = false;

/** @param {[number, number] | null} center */
function markCenter(center) {
  var marker = byId('focusCenter');
  marker.hidden = !center;
  if (center) {
    marker.style.left = center[0] * 100 + '%';
    marker.style.top = center[1] * 100 + '%';
  }
}

/** @param {string} url */
async function loadFocus(url) {
  var current = ++sequence;
  if (controller) controller.abort();
  controller = new AbortController();
  var image = /** @type {HTMLImageElement} */ (byId('inspectedFocusImg'));
  image.hidden = true;
  image.removeAttribute('src');
  byId('focusRasterStatus').textContent = 'Loading focus pixels…';
  try {
    var response = await fetch(url, {signal: controller.signal});
    if (!response.ok) throw new Error((await response.json()).error || 'Focus evidence unavailable');
    var blob = await response.blob();
    if (current !== sequence) return;
    var old = image.dataset.objectUrl;
    var objectUrl = URL.createObjectURL(blob);
    image.dataset.objectUrl = objectUrl;
    image.src = objectUrl;
    image.hidden = false;
    if (old) URL.revokeObjectURL(old);
    var headers = response.headers;
    var center = (headers.get('X-Vireo-Focus-Center') || '').split(',').map(Number);
    if (center[0] !== undefined && center[1] !== undefined) markCenter([center[0], center[1]]);
    byId('focusRasterStatus').textContent = 'Basis: ' + headers.get('X-Vireo-Focus-Basis') +
      ' · source: ' + headers.get('X-Vireo-Focus-Source') +
      ' · raster: ' + headers.get('X-Vireo-Focus-Raster-Size') +
      ' · original scale: ' + headers.get('X-Vireo-Focus-Original-Scale') +
      ' · 1 crop pixel per decoded raster pixel. Display may be fitted to this panel.';
  } catch (error) {
    if (current !== sequence) return;
    markCenter(null);
    byId('focusRasterStatus').textContent = error instanceof Error ? error.message : String(error);
  }
}

/** @param {BestBatchCard} card */
export function renderFocus(card) {
  choosing = false;
  byId('manualFocusBtn').setAttribute('aria-pressed', 'false');
  markCenter(null);
  var box = card.subject?.candidates.find(function(c) { return c.detection_id === card.subject?.detection_id; })?.box;
  var outline = byId('focusSubjectBox');
  outline.hidden = !box;
  if (box) {
    outline.style.left = box.x * 100 + '%';
    outline.style.top = box.y * 100 + '%';
    outline.style.width = box.w * 100 + '%';
    outline.style.height = box.h * 100 + '%';
  }
  byId('focusSelectionStatus').textContent = 'Full frame uses upright source coordinates before saved crop or rotation. Boxes are model suggestions; verify the animal.';
  if (card.subject && !card.focus_crop_url) {
    sequence++;
    if (controller) controller.abort();
    byId('inspectedFocusImg').hidden = true;
    byId('focusRasterStatus').textContent = 'Selected subject evidence is unavailable. Analyze it or place a manual center.';
  } else loadFocus(card.focus_crop_url || '/photos/' + card.id + '/focus-crop?size=512');
}

export function bindFocusControls() {
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
