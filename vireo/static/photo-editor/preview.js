// Histogram feedback, render sizes, and preview, histogram, and mask scheduling.
// Classic page script; load boot.js after all definitions.

function clearHistogramFeedback() {
  var canvas = document.getElementById('histogramCanvas');
  var ctx = canvas ? canvas.getContext('2d') : null;
  if (ctx) {
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    ctx.fillStyle = '#0f1113';
    ctx.fillRect(0, 0, canvas.width, canvas.height);
  }
  ['shadow', 'highlight'].forEach(function(kind) {
    var value = document.getElementById(kind + 'ClipValue');
    var chip = document.getElementById(kind + 'ClipChip');
    if (value) value.textContent = '--';
    if (chip) chip.classList.remove('warning');
  });
}

function updateHistogramFeedback() {
  var img = document.getElementById('editorImg');
  var canvas = document.getElementById('histogramCanvas');
  if (!img || !canvas || !img.complete || !img.naturalWidth || !img.naturalHeight) {
    clearHistogramFeedback();
    return;
  }
  var ctx = canvas.getContext('2d');
  if (!ctx) return;

  // Sample only the active crop so the histogram and clip readouts describe
  // the frame the user is keeping — the preview render is uncropped, and a
  // blown sky the user already cropped away must not keep warning about
  // highlight clipping.
  var crop = editorPreviewAppliesCrop()
    ? {x: 0, y: 0, w: 1, h: 1}
    : (editorState.showBefore
      ? clampCrop(Object.assign({x: 0, y: 0, w: 1, h: 1}, editorState.savedRecipe.crop || {}))
      : ensureCrop(editorState.recipe));
  var sx = 0, sy = 0;
  var sW = img.naturalWidth, sH = img.naturalHeight;
  if (!isFullCrop(crop)) {
    sx = Math.max(0, Math.round((Number(crop.x) || 0) * img.naturalWidth));
    sy = Math.max(0, Math.round((Number(crop.y) || 0) * img.naturalHeight));
    sW = Math.max(1, Math.round((Number(crop.w) || 1) * img.naturalWidth));
    sH = Math.max(1, Math.round((Number(crop.h) || 1) * img.naturalHeight));
  }
  var sample = document.createElement('canvas');
  var maxSample = 180;
  var scale = Math.min(1, maxSample / Math.max(sW, sH));
  sample.width = Math.max(1, Math.round(sW * scale));
  sample.height = Math.max(1, Math.round(sH * scale));
  var sampleCtx = sample.getContext('2d', { willReadFrequently: true });
  if (!sampleCtx) return;
  try {
    sampleCtx.drawImage(img, sx, sy, sW, sH, 0, 0, sample.width, sample.height);
    var data = sampleCtx.getImageData(0, 0, sample.width, sample.height).data;
  } catch (_) {
    clearHistogramFeedback();
    return;
  }

  var bins = new Array(64).fill(0);
  var clippedLow = 0;
  var clippedHigh = 0;
  var total = Math.max(1, sample.width * sample.height);
  for (var i = 0; i < data.length; i += 4) {
    var r = data[i];
    var g = data[i + 1];
    var b = data[i + 2];
    var luma = 0.2126 * r + 0.7152 * g + 0.0722 * b;
    bins[Math.min(63, Math.floor(luma / 4))] += 1;
    if (luma <= 3) clippedLow += 1;
    if (r >= 253 || g >= 253 || b >= 253) clippedHigh += 1;
  }

  var style = getComputedStyle(document.documentElement);
  var muted = style.getPropertyValue('--text-muted').trim() || '#7d8790';
  var accent = style.getPropertyValue('--accent').trim() || '#70c7ba';
  var warning = style.getPropertyValue('--warning').trim() || '#f0b84f';
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  ctx.fillStyle = '#0f1113';
  ctx.fillRect(0, 0, canvas.width, canvas.height);
  ctx.strokeStyle = 'rgba(255,255,255,0.08)';
  ctx.lineWidth = 1;
  for (var gx = 0; gx <= 4; gx++) {
    var x = Math.round((canvas.width - 1) * gx / 4) + 0.5;
    ctx.beginPath();
    ctx.moveTo(x, 0);
    ctx.lineTo(x, canvas.height);
    ctx.stroke();
  }
  var maxBin = Math.max.apply(Math, bins);
  var barW = canvas.width / bins.length;
  for (var bi = 0; bi < bins.length; bi++) {
    var h = maxBin ? Math.round((bins[bi] / maxBin) * (canvas.height - 8)) : 0;
    ctx.fillStyle = bi < 2 || bi > 61 ? warning : accent;
    ctx.globalAlpha = bi < 2 || bi > 61 ? 0.82 : 0.62;
    ctx.fillRect(Math.floor(bi * barW), canvas.height - h, Math.ceil(barW), h);
  }
  ctx.globalAlpha = 1;
  ctx.fillStyle = muted;
  ctx.font = '10px system-ui, -apple-system, sans-serif';
  ctx.fillText('0', 6, canvas.height - 6);
  ctx.fillText('255', canvas.width - 24, canvas.height - 6);

  function setClip(kind, count) {
    var pct = count * 100 / total;
    var value = document.getElementById(kind + 'ClipValue');
    var chip = document.getElementById(kind + 'ClipChip');
    if (value) value.textContent = '~' + pct.toFixed(pct < 1 ? 2 : 1) + '% clipped';
    if (chip) chip.classList.toggle('warning', pct >= 1);
  }
  setClip('shadow', clippedLow);
  setClip('highlight', clippedHigh);
}

function editorRenderSizeForPercent(percent) {
  var dims = editorNativeRecipeDimensions(
    editorZoomRecipe() || {}, editorPreviewAppliesCrop()
  );
  var native = dims ? Math.max(dims.width, dims.height) : 0;
  if (!native) return percent >= 100 ? 3840 : 1920;
  var requested = native * Math.min(100, percent) / 100;
  requested = Math.ceil(Math.max(1920, requested) / 256) * 256;
  // A normalized crop can produce a fractional native long edge (for
  // example 800 * 0.333 = 266.4), but the preview endpoint accepts only an
  // integer size. Round upward so 100% never requests fewer source pixels
  // than the cropped native frame needs.
  return Math.max(1, Math.ceil(Math.min(16384, native, requested)));
}

function editorFitRenderSize() {
  return editorRenderSizeForPercent(editorFitZoomPercent());
}

function previewRenderSize() {
  // Request enough source pixels for the displayed Fit or custom scale,
  // reaching the native source at 100% and reusing it above 100%. Sizes are
  // bucketed so moving one slider pixel does not create a new server render.
  var percent = editorState.zoomMode === 'fit'
    ? editorFitZoomPercent()
    : editorState.zoomPercent;
  return editorRenderSizeForPercent(percent);
}

function previewStatusText(img) {
  // At and above 100% the render size is the whole point of the view — state
  // the delivered dimensions rather than implying "100%" on its own.
  var suffix = editorState.zoomMode !== 'fit' && editorState.zoomPercent >= 99.5 &&
    img && img.naturalWidth
    ? ' (' + img.naturalWidth + '×' + img.naturalHeight + ' render)'
    : '';
  return (editorState.showBefore
    ? 'Preview is showing the saved recipe'
    : 'Preview uses the current unsaved recipe') + suffix;
}

// Keep one render in flight and one replaceable request. Dragging never builds
// an unbounded server queue, and obsolete pixels never replace the visible photo.
var editorPreviewQueue = {active: null, pending: null};
var EDITOR_INTERACTIVE_SIZE = 1024;
var EDITOR_PREVIEW_INTERVAL = 60;
var EDITOR_REFINE_DELAY = 300;

function cancelEditorPreview() {
  editorState.previewSeq++;
  clearTimeout(editorState.previewTimer);
  clearTimeout(editorState.previewRefineTimer);
  editorState.previewTimer = null;
  editorState.previewRefineTimer = null;
  editorPreviewQueue.pending = null;
  // Release the in-flight render. Without this, pumpEditorPreview() would
  // still see editorPreviewQueue.active populated and refuse to start the
  // next photo's preview — a stalled or very slow previous request would
  // then leave the previous photo on screen while the metadata already
  // described the new photo, indefinitely on a hung request.
  var stale = editorPreviewQueue.active;
  if (stale) {
    editorPreviewQueue.active = null;
    if (stale.image) stale.image.onload = stale.image.onerror = null;
  }
}

function presentEditorPreview(request, image) {
  var old = document.getElementById('editorImg');
  if (old !== image) {
    // Move the decoded element into the document: assigning its URL to the old
    // element could fetch/render the same uncached recipe a second time.
    Array.from(old.attributes).forEach(function(attr) {
      if (attr.name !== 'src') image.setAttribute(attr.name, attr.value);
    });
    old.replaceWith(image);
  }
  var status = previewStatusText(image);
  if (request.interactive) status = 'Quick preview — detail will refine when editing pauses';
  document.getElementById('previewStatus').textContent = status;
  applyEditorZoom();
  updateEditorZoomControl();
  renderCropBox();
  updateHistogramFeedback();
  refreshMaskOverlay();
  // Bounded, in-memory timing samples for the browser benchmark. No photo
  // identifiers, recipes, or pixels are retained in the measurements.
  editorState.previewTimings.push({
    interactive: request.interactive,
    requestedSize: request.size,
    elapsedMs: performance.now() - request.started,
  });
  if (editorState.previewTimings.length > 100) editorState.previewTimings.shift();
  if (!request.interactive && editorState.zoomMode === 'fit' &&
      previewRenderSize() !== request.size) schedulePreview();
}

function pumpEditorPreview() {
  if (editorPreviewQueue.active || !editorPreviewQueue.pending) return;
  var request = editorPreviewQueue.pending;
  editorPreviewQueue.pending = null;
  var image = new Image();
  request.image = image;
  editorPreviewQueue.active = request;
  function finish(ok) {
    image.onload = image.onerror = null;
    // cancelEditorPreview may have released this request already; only
    // clear the slot when we're still its owner, so a late response from
    // a cancelled fetch doesn't clobber a newer active request.
    if (editorPreviewQueue.active === request) editorPreviewQueue.active = null;
    if (request.seq === editorState.previewSeq && !editorState.loading) {
      if (ok) presentEditorPreview(request, image);
      else {
        document.getElementById('previewStatus').textContent =
          'Could not render preview — showing the previous image';
        clearHistogramFeedback();
      }
    }
    pumpEditorPreview();
  }
  image.onload = function() { finish(true); };
  image.onerror = function() { finish(false); };
  image.src = request.url;
}

function updatePreview(options) {
  if (!editorState.photoId || editorState.loading) return;
  options = options || {};
  if (!options.scheduled) cancelEditorPreview();
  editorClampCustomZoomToFit();
  updateFeedbackControls();
  var applyCrop = editorPreviewAppliesCrop();
  var recipe = editorState.showBefore
    ? previewRecipeFor(editorState.savedRecipe, applyCrop)
    : previewRecipe();
  var targetSize = previewRenderSize();
  var size = options.interactive ? Math.min(EDITOR_INTERACTIVE_SIZE, targetSize) : targetSize;
  var url = '/photos/' + editorState.photoId + '/edit-preview?size=' + size +
    '&apply_crop=' + (applyCrop ? '1' : '0') +
    '&recipe=' + encodeURIComponent(JSON.stringify(recipe));
  var request = {
    url: url, size: size, seq: editorState.previewSeq,
    interactive: size < targetSize,
    started: options.started || performance.now(),
  };
  var img = document.getElementById('editorImg');
  if (img.getAttribute('src') === url && img.complete && img.naturalWidth) {
    editorPreviewQueue.pending = null;
    presentEditorPreview(request, img);
    return;
  }
  document.getElementById('previewStatus').textContent = request.interactive
    ? 'Rendering quick preview…' : 'Refining preview…';
  // Identical active requests can serve a newer revision without re-rendering.
  if (editorPreviewQueue.active && editorPreviewQueue.active.url === url) {
    Object.assign(editorPreviewQueue.active, request);
    editorPreviewQueue.pending = null;
  } else {
    editorPreviewQueue.pending = request;
    pumpEditorPreview();
  }
}

function schedulePreview() {
  editorState.previewSeq++; // invalidate at input time, before either timer fires
  editorState.previewInputAt = performance.now();
  editorPreviewQueue.pending = null;
  // Throttle the quick tier: a continuous drag still gets intermediate frames.
  if (!editorState.previewTimer) {
    editorState.previewTimer = setTimeout(function() {
      editorState.previewTimer = null;
      updatePreview({interactive: true, scheduled: true, started: editorState.previewInputAt});
    }, EDITOR_PREVIEW_INTERVAL);
  }
  clearTimeout(editorState.previewRefineTimer);
  editorState.previewRefineTimer = setTimeout(function() {
    editorState.previewRefineTimer = null;
    updatePreview({scheduled: true, started: editorState.previewInputAt});
  }, EDITOR_REFINE_DELAY);
}

function scheduleHistogramRefresh() {
  // Debounced histogram/clip-readout resample for crop-only edits, which
  // don't re-render the preview: the readouts sample inside the crop, so a
  // crop drag changes what they should report even though the pixels on
  // screen are unchanged.
  if (editorState.histogramTimer) clearTimeout(editorState.histogramTimer);
  editorState.histogramTimer = setTimeout(function() {
    editorState.histogramTimer = null;
    updateHistogramFeedback();
  }, 140);
}

function scheduleMaskOverlayRefresh() {
  // Debounced counterpart to schedulePreview() for crop-only edits that
  // don't schedule a preview: /edit-mask-preview reads the current crop
  // to compute the saved-render feather scale, so dragging/resetting/
  // changing crop aspect changes the halo the overlay is meant to show.
  // Without this the visible mask lags until an unrelated slider or zoom
  // forces a reload.
  if (editorState.maskOverlayTimer) clearTimeout(editorState.maskOverlayTimer);
  editorState.maskOverlayTimer = setTimeout(function() {
    editorState.maskOverlayTimer = null;
    refreshMaskOverlay();
  }, 140);
}
