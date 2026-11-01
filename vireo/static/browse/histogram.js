/* Sample the displayed detail preview, including its current crop and edits. */
import { selection } from './selection-state.js';
import { state } from './state.js';

/** @type {number | null} */
let histogramPhotoId = null;

/** @param {Uint8ClampedArray} pixels */
export function measureHistogram(pixels) {
  const channels = [new Array(64).fill(0), new Array(64).fill(0), new Array(64).fill(0)];
  let low = 0, high = 0, total = 0;
  for (let i = 0; i < pixels.length; i += 4) {
    if (!pixels[i + 3]) continue;
    const r = pixels[i] || 0, g = pixels[i + 1] || 0, b = pixels[i + 2] || 0;
    [r, g, b].forEach(function(value, channel) {
      const bins = channels[channel];
      if (bins) bins[Math.min(63, Math.floor(value / 4))] += 1;
    });
    // Match the editor's near-clipping thresholds on rendered 8-bit pixels.
    if (0.2126 * r + 0.7152 * g + 0.0722 * b <= 3) low += 1;
    if (r >= 253 || g >= 253 || b >= 253) high += 1;
    total += 1;
  }
  return { channels, low, high, total };
}

/** @param {string} message */
function resetHistogram(message) {
  const canvas = /** @type {HTMLCanvasElement | null} */ (document.getElementById('browseHistogram'));
  const values = document.getElementById('browseHistogramValues');
  const status = document.getElementById('browseHistogramStatus');
  if (canvas) {
    canvas.getContext('2d')?.clearRect(0, 0, canvas.width, canvas.height);
    canvas.hidden = true;
  }
  if (values) values.hidden = true;
  if (status) status.textContent = message;
}

export function clearBrowseHistogram() {
  histogramPhotoId = null;
  resetHistogram('Loading preview…');
}

/** @param {HTMLCanvasElement} canvas @param {number[][]} channels */
function drawHistogram(canvas, channels) {
  const ctx = canvas.getContext('2d');
  if (!ctx) return false;
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  ctx.strokeStyle = 'rgba(255,255,255,0.12)';
  for (let i = 1; i < 4; i++) {
    ctx.beginPath();
    ctx.moveTo(canvas.width * i / 4, 0);
    ctx.lineTo(canvas.width * i / 4, canvas.height);
    ctx.stroke();
  }
  const peak = Math.max(1, ...channels.flat());
  const colors = ['#ff7777', '#78d995', '#82afff'];
  ctx.globalCompositeOperation = 'screen';
  channels.forEach(function(bins, index) {
    ctx.fillStyle = colors[index] || '#fff';
    ctx.globalAlpha = 0.55;
    const width = canvas.width / bins.length;
    bins.forEach(function(count, bin) {
      const height = count / peak * (canvas.height - 4);
      ctx.fillRect(bin * width, canvas.height - height, width, height);
    });
  });
  ctx.globalAlpha = 1;
  ctx.globalCompositeOperation = 'source-over';
  return true;
}

/** @param {number} count @param {number} total */
function percentage(count, total) {
  const value = count * 100 / total;
  return '~' + value.toFixed(value < 1 ? 2 : 1) + '%';
}

function updateHistogram() {
  if (histogramPhotoId === null || state.detailPhotoId !== histogramPhotoId ||
      selection.focusedPhotoId !== histogramPhotoId || selection.batchIds.size > 1) return;
  const img = /** @type {HTMLImageElement | null} */ (document.getElementById('detailImg'));
  const canvas = /** @type {HTMLCanvasElement | null} */ (document.getElementById('browseHistogram'));
  if (!img || !canvas || !img.complete) return;
  if (!img.naturalWidth || !img.naturalHeight) {
    resetHistogram('Preview histogram unavailable.');
    return;
  }
  // Keep sampling bounded and synchronous; no original decode or extra request.
  const sample = document.createElement('canvas');
  const scale = Math.min(1, 180 / Math.max(img.naturalWidth, img.naturalHeight));
  sample.width = Math.max(1, Math.round(img.naturalWidth * scale));
  sample.height = Math.max(1, Math.round(img.naturalHeight * scale));
  const ctx = sample.getContext('2d', {willReadFrequently: true});
  if (!ctx) { resetHistogram('Preview histogram unavailable.'); return; }
  try {
    ctx.drawImage(img, 0, 0, sample.width, sample.height);
    const result = measureHistogram(ctx.getImageData(0, 0, sample.width, sample.height).data);
    if (!result.total || !drawHistogram(canvas, result.channels)) {
      resetHistogram('Preview histogram unavailable.');
      return;
    }
    const low = percentage(result.low, result.total), high = percentage(result.high, result.total);
    const shadow = document.getElementById('browseHistogramLow');
    const highlight = document.getElementById('browseHistogramHigh');
    if (shadow) shadow.textContent = 'Near black ' + low;
    if (highlight) highlight.textContent = 'Near channel limit ' + high;
    canvas.setAttribute('aria-label', 'Red, green, and blue preview histogram. Near black ' + low + '; near channel limit ' + high + '.');
    canvas.hidden = false;
    const values = document.getElementById('browseHistogramValues');
    if (values) values.hidden = false;
    const status = document.getElementById('browseHistogramStatus');
    if (status) status.textContent = 'Sampled from the displayed preview.';
  } catch (_) {
    resetHistogram('Preview histogram unavailable.');
  }
}

/** @param {number} photoId */
export function showBrowseHistogram(photoId) {
  histogramPhotoId = photoId;
  resetHistogram('Loading preview…');
  updateHistogram();
}

export function bindBrowseHistogram() {
  const img = document.getElementById('detailImg');
  if (!img) return;
  img.addEventListener('load', updateHistogram);
  img.addEventListener('error', updateHistogram);
  // Shared lightbox edits and RAW/JPEG source switches also replace this src.
  // Clear old measurements immediately, then handle cached and async loads alike.
  new MutationObserver(function() {
    resetHistogram('Loading preview…');
    updateHistogram();
  }).observe(img, {attributes: true, attributeFilter: ['src']});
}
