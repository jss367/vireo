/* Point curves and sampled color controls for the non-destructive editor. */
/** @type {{curveIndex:number;sampleIndex:number;picking:boolean;pickSequence:number;dragging:number|null}} */ var colorEditor = {curveIndex: 0, sampleIndex: 0, picking: false, pickSequence: 0, dragging: null};
/** @type {Record<EditorPointColorKey,number>} */ var POINT_COLOR_DEFAULTS = {hue_range: 30, saturation_range: 100, luminance_range: 100, hue: 0, saturation: 0, luminance: 0};

/** @param {EditorAdjustments} source @param {EditorAdjustments} target */ function canonicalPointControls(source, target) {
  /** @type {Record<string,[number,number][]>} */ var curves = {};
  ['rgb', 'red', 'green', 'blue'].forEach(function(channel) {
    var points = (source.point_curves || {})[channel];
    if (points && points.some(function(p) { return Math.abs(p[0] - p[1]) > 0.000001; })) {
      curves[channel] = cloneRecipe(points);
    }
  });
  if (Object.keys(curves).length) target.point_curves = curves;
  if (source.point_color && source.point_color.length) {
    target.point_color = source.point_color.map(function(item) {
      /** @type {EditorPointColor} */ var result = {sample: [item.sample[0] % 360, item.sample[1], item.sample[2]]};
      /** @type {EditorPointColorKey[]} */ (Object.keys(POINT_COLOR_DEFAULTS)).forEach(function(key) {
        var value = item[key] == null ? POINT_COLOR_DEFAULTS[key] : Number(item[key]);
        if (Math.abs(value - POINT_COLOR_DEFAULTS[key]) > 0.000001) result[key] = value;
      });
      return result;
    });
  }
}

function curveChannel() { return /** @type {HTMLSelectElement} */ (document.getElementById('curveChannel')).value; }
/** @returns {[number,number][]} */ function currentCurvePoints() {
  var adj = editorState.recipe.adjustments || {};
  var channel = curveChannel();
  var points = (adj.point_curves || {})[channel];
  if (points) return cloneRecipe(points);
  if (channel === 'rgb' && adj.tone_curve) {
    var legacy = advancedColorValues(editorState.recipe).tone_curve;
    return Object.keys(TONE_CURVE_DEFAULTS).map(function(key, i) { return [i * 25, legacy[key] || 0]; });
  }
  return [[0, 0], [100, 100]];
}
function pointColorSamples() { return cloneRecipe((editorState.recipe.adjustments || {}).point_color || []); }
/** @param {number} value */ function colorRound(value) { return Math.round(value * 1000) / 1000; }
/** @param {number} value @param {number} lo @param {number} hi */ function colorClamp(value, lo, hi) { return Math.max(lo, Math.min(hi, value)); }

/** @param {[number,number][]} points */ function saveCurvePoints(points) {
  if (editorState.loading) return;
  var channel = curveChannel();
  var curves = cloneRecipe((editorState.recipe.adjustments || {}).point_curves || {});
  curves[channel] = points;
  // Editing the composite graph promotes the legacy five-point curve without
  // applying it twice. Old recipes continue using their original renderer.
  if (channel === 'rgb') _setAdjustmentSection('tone_curve', null);
  // Keep identity points while editing, including mid-drag on the diagonal.
  // recipeForSave drops neutral curves from persisted recipes.
  _setAdjustmentSection('point_curves', curves);
  syncAdvancedColorControls();
  markChanged(true);
}

/** @param {string} name @param {Record<string,string|number>} attrs */ function curveSvgElement(name, attrs) {
  var el = document.createElementNS('http://www.w3.org/2000/svg', name);
  Object.keys(attrs).forEach(function(key) { el.setAttribute(key, String(attrs[key])); });
  return el;
}
function syncPointControls() {
  var svg = /** @type {SVGSVGElement} */ (document.querySelector('svg#pointCurveGraph'));
  if (!svg) return;
  var points = currentCurvePoints();
  colorEditor.curveIndex = Math.max(0, Math.min(colorEditor.curveIndex, points.length - 1));
  var selected = colorEditor.curveIndex;
  var channel = curveChannel();
  var stroke = /** @type {Record<string,string>} */ ({rgb: 'var(--text-primary)', red: '#ef7777', green: '#64c98a', blue: '#7eacff'})[channel] || 'var(--text-primary)';
  svg.replaceChildren();
  [0, 25, 50, 75, 100].forEach(function(n) {
    var p = 12 + n * 2.16;
    svg.appendChild(curveSvgElement('line', {x1: p, y1: 12, x2: p, y2: 228, stroke: 'var(--border-primary)'}));
    svg.appendChild(curveSvgElement('line', {x1: 12, y1: p, x2: 228, y2: p, stroke: 'var(--border-primary)'}));
  });
  svg.appendChild(curveSvgElement('line', {x1: 12, y1: 228, x2: 228, y2: 12, stroke: 'var(--text-secondary)', 'stroke-dasharray': '3 5', opacity: .5}));
  svg.appendChild(curveSvgElement('polyline', {points: points.map(function(p) { return (12 + p[0] * 2.16) + ',' + (228 - p[1] * 2.16); }).join(' '), fill: 'none', stroke: stroke, 'stroke-width': 2}));
  points.forEach(function(p, i) {
    svg.appendChild(curveSvgElement('circle', {cx: 12 + p[0] * 2.16, cy: 228 - p[1] * 2.16, r: i === selected ? 5 : 4, fill: i === selected ? stroke : 'var(--bg-primary)', stroke: stroke, 'stroke-width': 2, 'data-point': i}));
  });
  var select = /** @type {HTMLSelectElement} */ (document.getElementById('curvePointSelect'));
  select.replaceChildren();
  points.forEach(function(p, i) { select.add(new Option('Point ' + (i + 1) + ': ' + p[0] + ' → ' + p[1], String(i))); });
  select.value = String(selected);
  var input = /** @type {HTMLInputElement} */ (document.getElementById('curveInput'));
  input.value = String(editorCurvePoint(points, selected)[0]);
  input.disabled = selected === 0 || selected === points.length - 1;
  input.min = String(selected > 0 ? editorCurvePoint(points, selected - 1)[0] + .01 : 0);
  input.max = String(selected < points.length - 1 ? editorCurvePoint(points, selected + 1)[0] - .01 : 100);
  /** @type {HTMLInputElement} */ (document.getElementById('curveOutput')).value = String(editorCurvePoint(points, selected)[1]);
  /** @type {HTMLButtonElement} */ (document.getElementById('curveDeletePoint')).disabled = input.disabled;
  /** @type {HTMLButtonElement} */ (document.getElementById('curveAddPoint')).disabled = points.length >= 32;
  /** @type {HTMLElement} */ (document.getElementById('legacyCurveControls')).hidden = !!((editorState.recipe.adjustments || {}).point_curves || {}).rgb;

  var samples = pointColorSamples();
  colorEditor.sampleIndex = Math.max(0, Math.min(colorEditor.sampleIndex, samples.length - 1));
  var sampleSelect = /** @type {HTMLSelectElement} */ (document.getElementById('pointColorSelect'));
  sampleSelect.replaceChildren();
  if (!samples.length) sampleSelect.add(new Option('No sampled colors', '0'));
  samples.forEach(function(item, i) { sampleSelect.add(new Option('Color ' + (i + 1) + ' · ' + Math.round(item.sample[0]) + '°', String(i))); });
  sampleSelect.value = String(colorEditor.sampleIndex);
  sampleSelect.disabled = !samples.length;
  var item = samples[colorEditor.sampleIndex];
  /** @type {HTMLFieldSetElement} */ (document.getElementById('pointColorFields')).disabled = !item;
  /** @type {HTMLButtonElement} */ (document.getElementById('pointColorRemove')).disabled = !item;
  /** @type {HTMLButtonElement} */ (document.getElementById('pointColorPick')).disabled = samples.length >= 8;
  /** @type {HTMLInputElement} */ (document.getElementById('pointColorCustom')).disabled = samples.length >= 8;
  /** @type {HTMLElement} */ (document.getElementById('pointColorSwatch')).style.background = item ? 'hsl(' + item.sample[0] + ' ' + item.sample[1] + '% ' + item.sample[2] + '%)' : 'transparent';
  /** @type {EditorPointColorKey[]} */ (Object.keys(POINT_COLOR_DEFAULTS)).forEach(function(key) {
    var value = item && item[key] != null ? item[key] : POINT_COLOR_DEFAULTS[key];
    /** @type {HTMLInputElement} */ (document.getElementById('pointColor_' + key)).value = String(value);
    /** @type {HTMLElement} */ (document.getElementById('pointColor_' + key + 'Value')).textContent = value + (key.indexOf('hue') === 0 ? '°' : '');
  });
}

/** @param {0|1} axis @param {number|string} raw */ function setCurveCoordinate(axis, raw) {
  var points = currentCurvePoints(), index = colorEditor.curveIndex;
  var value = Number(raw);
  if (!Number.isFinite(value)) { syncPointControls(); return; }
  if (axis === 0 && (index === 0 || index === points.length - 1)) return;
  var lo = axis === 0 ? editorCurvePoint(points, index - 1)[0] + .01 : 0;
  var hi = axis === 0 ? editorCurvePoint(points, index + 1)[0] - .01 : 100;
  editorCurvePoint(points, index)[axis] = colorRound(colorClamp(value, lo, hi));
  saveCurvePoints(points);
}
function addCurvePoint() {
  if (editorState.loading) return;
  var points = currentCurvePoints();
  if (points.length >= 32) return;
  var index = 0;
  for (var i = 1; i < points.length - 1; i++) {
    if (editorCurvePoint(points, i + 1)[0] - editorCurvePoint(points, i)[0] > editorCurvePoint(points, index + 1)[0] - editorCurvePoint(points, index)[0]) index = i;
  }
  points.splice(index + 1, 0, [colorRound((editorCurvePoint(points, index)[0] + editorCurvePoint(points, index + 1)[0]) / 2), colorRound((editorCurvePoint(points, index)[1] + editorCurvePoint(points, index + 1)[1]) / 2)]);
  colorEditor.curveIndex = index + 1;
  saveCurvePoints(points);
}
function deleteCurvePoint() {
  if (editorState.loading) return;
  var points = currentCurvePoints(), index = colorEditor.curveIndex;
  if (index === 0 || index === points.length - 1) return;
  points.splice(index, 1);
  colorEditor.curveIndex = Math.max(0, index - 1);
  saveCurvePoints(points);
}
function resetCurveChannel() {
  if (editorState.loading) return;
  var curves = cloneRecipe((editorState.recipe.adjustments || {}).point_curves || {});
  delete curves[curveChannel()];
  if (curveChannel() === 'rgb') _setAdjustmentSection('tone_curve', null);
  _setAdjustmentSection('point_curves', curves);
  colorEditor.curveIndex = 0;
  syncAdvancedColorControls();
  markChanged(true);
}

/** @param {[number,number,number]} sample */ function addPointColorSample(sample) {
  if (editorState.loading) return false;
  var samples = pointColorSamples();
  if (samples.length >= 8) return false;
  // Validate the value we persist; rounding must not admit the renderer’s
  // zero-weight boundary at 1% saturation.
  sample = [colorRound(sample[0]), colorRound(sample[1]), colorRound(sample[2])];
  if (sample[1] <= 1) {
    /** @type {HTMLElement} */ (document.getElementById('pointColorStatus')).textContent = 'Choose a more saturated color; neutral gray has no distinct hue.';
    return false;
  }
  sample[0] %= 360;
  samples.push({sample: sample});
  colorEditor.sampleIndex = samples.length - 1;
  _setAdjustmentSection('point_color', samples);
  syncPointControls();
  markChanged(true);
  return true;
}
/** @param {number} r @param {number} g @param {number} b @returns {[number,number,number]} */ function rgbToPointSample(r, g, b) {
  r /= 255; g /= 255; b /= 255;
  var max = Math.max(r, g, b), min = Math.min(r, g, b), delta = max - min;
  var light = (max + min) / 2, hue = 0, sat = 0;
  if (delta > 0.0000001) {
    sat = delta / (1 - Math.abs(2 * light - 1));
    if (max === r) hue = ((g - b) / delta + 6) % 6;
    else if (max === g) hue = (b - r) / delta + 2;
    else hue = (r - g) / delta + 4;
    hue *= 60;
  }
  return [hue, sat * 100, light * 100];
}
/** @param {string} hex */ function addPointColorHex(hex) {
  cancelPointColorPicker();
  addPointColorSample(rgbToPointSample(parseInt(hex.slice(1, 3), 16), parseInt(hex.slice(3, 5), 16), parseInt(hex.slice(5, 7), 16)));
}
/** @param {EditorPointColorKey} key @param {string} raw */ function setPointColorControl(key, raw) {
  var samples = pointColorSamples();
  if (!samples[colorEditor.sampleIndex] || editorState.loading) return;
  var sample = samples[colorEditor.sampleIndex]; if (sample) sample[key] = Number(raw);
  _setAdjustmentSection('point_color', samples);
  syncPointControls();
  markChanged(true);
}
function removePointColor() {
  if (editorState.loading) return;
  var samples = pointColorSamples();
  samples.splice(colorEditor.sampleIndex, 1);
  _setAdjustmentSection('point_color', samples);
  syncPointControls();
  markChanged(true);
}
function resetPointColor() {
  if (editorState.loading) return;
  cancelPointColorPicker();
  _setAdjustmentSection('point_color', null);
  syncPointControls();
  markChanged(true);
}
function cancelPointColorPicker() {
  colorEditor.pickSequence++;
  colorEditor.picking = false;
  /** @type {HTMLElement} */ (document.getElementById('editorCanvasWrap')).classList.remove('picking-color');
  /** @type {HTMLButtonElement} */ (document.getElementById('pointColorPick')).textContent = 'Pick from Photo';
  /** @type {HTMLButtonElement} */ (document.getElementById('pointColorPick')).setAttribute('aria-pressed', 'false');
  /** @type {HTMLElement} */ (document.getElementById('pointColorStatus')).textContent = '';
}
function togglePointColorPicker() {
  if (colorEditor.picking) { cancelPointColorPicker(); return; }
  if (editorState.loading || pointColorSamples().length >= 8) return;
  // The image click belongs to one tool, including during mask preparation.
  cancelMaskBrush();
  // A new picking session supersedes any preview request still in flight.
  colorEditor.pickSequence++;
  colorEditor.picking = true;
  /** @type {HTMLElement} */ (document.getElementById('editorCanvasWrap')).classList.add('picking-color');
  /** @type {HTMLButtonElement} */ (document.getElementById('pointColorPick')).textContent = 'Cancel Picking';
  /** @type {HTMLButtonElement} */ (document.getElementById('pointColorPick')).setAttribute('aria-pressed', 'true');
  /** @type {HTMLElement} */ (document.getElementById('pointColorStatus')).textContent = 'Click a color in the photo. Escape cancels.';
  if (editorState.showBefore) toggleBeforePreview();
}
/** @param {PointerEvent} e */ async function samplePointColorAt(e) {
  var displayed = /** @type {HTMLImageElement} */ (document.getElementById('editorImg'));
  if (!displayed.complete || !displayed.naturalWidth || editorState.loading || !editorImageMatchesZoomRecipe(displayed)) {
    /** @type {HTMLElement} */ (document.getElementById('pointColorStatus')).textContent = 'Wait for the preview to finish, then click the color.';
    return;
  }
  var rect = displayed.getBoundingClientRect();
  var x = (e.clientX - rect.left) / rect.width, y = (e.clientY - rect.top) / rect.height;
  if (x < 0 || y < 0 || x > 1 || y > 1) return;
  var photoId = editorState.photoId, startKey = recipeKey(editorState.recipe);
  var sampleUrl = new URL(displayed.currentSrc || displayed.src, window.location.href);
  // Sampling is an independent render, not a replay of the displayed image's
  // already-consumed sequence or a replacement for the editor's active work.
  sampleUrl.searchParams.delete('preview_session');
  sampleUrl.searchParams.delete('preview_seq');
  var recipe = previewRecipe();
  // Sample at the input of Point Color, so subsequent color changes cannot
  // move the selection away from its own original sample.
  if (recipe.adjustments) {
    ['point_color', 'sharpen', 'sharpen_radius', 'noise_reduction', 'denoise_mode'].forEach(function(key) { if (recipe.adjustments) delete recipe.adjustments[/** @type {keyof EditorAdjustments} */ (key)]; });
  }
  if (recipe.local && recipe.local.regions) {
    recipe.local.regions.forEach(function(region) {
      ['point_color', 'sharpen', 'sharpen_radius', 'noise_reduction', 'denoise_mode'].forEach(function(key) { delete region.adjustments[key]; });
    });
    recipe.local.regions = recipe.local.regions.filter(function(region) { return Object.keys(region.adjustments).length; });
    if (!recipe.local.regions.length) delete recipe.local;
  }
  cancelPointColorPicker();
  var seq = colorEditor.pickSequence;
  /** @type {HTMLElement} */ (document.getElementById('pointColorStatus')).textContent = 'Sampling color…';
  try {
    // Preserve the displayed render size and geometry: downsampling here
    // would mix neighboring colors when the user is inspecting fine detail.
    sampleUrl.searchParams.set('recipe', JSON.stringify(recipe));
    var source = await _loadImage(sampleUrl.href);
    if (seq !== colorEditor.pickSequence || photoId !== editorState.photoId || editorState.loading) return;
    if (startKey !== recipeKey(editorState.recipe)) {
      /** @type {HTMLElement} */ (document.getElementById('pointColorStatus')).textContent = 'Edits changed while sampling. Pick the color again.';
      return;
    }
    var canvas = document.createElement('canvas');
    canvas.width = 1; canvas.height = 1;
    var ctx = canvas.getContext('2d'); if (!ctx) throw new Error('Canvas is unavailable');
    ctx.drawImage(source, Math.min(source.naturalWidth - 1, Math.floor(x * source.naturalWidth)), Math.min(source.naturalHeight - 1, Math.floor(y * source.naturalHeight)), 1, 1, 0, 0, 1, 1);
    var pixel = ctx.getImageData(0, 0, 1, 1).data;
    var sample = rgbToPointSample(pixel[0] || 0, pixel[1] || 0, pixel[2] || 0);
    if (!addPointColorSample(sample)) return;
    /** @type {HTMLElement} */ (document.getElementById('pointColorStatus')).textContent = 'Color sampled. Adjust the ranges and color sliders below.';
  } catch (_) {
    if (seq === colorEditor.pickSequence) /** @type {HTMLElement} */ (document.getElementById('pointColorStatus')).textContent = 'Could not sample this photo. Try again.';
  }
}

(function initPointColorEditor() {
  var svg = /** @type {SVGSVGElement} */ (document.querySelector('svg#pointCurveGraph'));
  /** @param {PointerEvent} e @returns {[number,number]} */ function graphPosition(e) {
    var rect = svg.getBoundingClientRect();
    return [colorRound(colorClamp((240 * (e.clientX - rect.left) / rect.width - 12) / 2.16, 0, 100)), colorRound(colorClamp((228 - 240 * (e.clientY - rect.top) / rect.height) / 2.16, 0, 100))];
  }
  svg.addEventListener('pointerdown', function(e) {
    if (editorState.loading || e.button !== 0) return;
    e.preventDefault(); svg.focus();
    var points = currentCurvePoints(), position = graphPosition(e);
    var index = points.findIndex(function(p) { return Math.hypot(p[0] - position[0], p[1] - position[1]) < 5; });
    if (index < 0) {
      if (points.length >= 32 || points.some(function(p) { return Math.abs(p[0] - position[0]) < .01; })) return;
      points.push(position); points.sort(function(a, b) { return a[0] - b[0]; });
      index = points.indexOf(position);
      colorEditor.curveIndex = index;
      saveCurvePoints(points);
    }
    colorEditor.curveIndex = index;
    colorEditor.dragging = e.pointerId;
    svg.setPointerCapture(e.pointerId);
    syncPointControls();
  });
  svg.addEventListener('pointermove', function(e) {
    if (colorEditor.dragging !== e.pointerId || editorState.loading) return;
    var points = currentCurvePoints(), index = colorEditor.curveIndex, position = graphPosition(e);
    if (index !== 0 && index !== points.length - 1) editorCurvePoint(points, index)[0] = colorRound(colorClamp(position[0], editorCurvePoint(points, index - 1)[0] + .01, editorCurvePoint(points, index + 1)[0] - .01));
    editorCurvePoint(points, index)[1] = position[1];
    saveCurvePoints(points);
  });
  /** @type {('pointerup'|'pointercancel'|'lostpointercapture')[]} */ (['pointerup', 'pointercancel', 'lostpointercapture']).forEach(function(type) {
    svg.addEventListener(type, function(e) {
      if (colorEditor.dragging !== e.pointerId) return;
      colorEditor.dragging = null;
      if (svg.hasPointerCapture(e.pointerId)) svg.releasePointerCapture(e.pointerId);
    });
  });
  svg.addEventListener('keydown', function(e) {
    if (editorState.loading) return;
    var point = editorCurvePoint(currentCurvePoints(), colorEditor.curveIndex), step = e.shiftKey ? 5 : 1;
    if (e.key === 'Delete' || e.key === 'Backspace') deleteCurvePoint();
    else if (e.key === 'ArrowLeft') setCurveCoordinate(0, point[0] - step);
    else if (e.key === 'ArrowRight') setCurveCoordinate(0, point[0] + step);
    else if (e.key === 'ArrowUp') setCurveCoordinate(1, point[1] + step);
    else if (e.key === 'ArrowDown') setCurveCoordinate(1, point[1] - step);
    else return;
    e.preventDefault(); e.stopPropagation();
  });
  /** @type {HTMLElement} */ (document.getElementById('editorCanvasWrap')).addEventListener('pointerdown', function(e) {
    if (!colorEditor.picking || e.button !== 0) return;
    e.preventDefault(); e.stopImmediatePropagation();
    samplePointColorAt(e);
  }, true);
  document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape' && colorEditor.picking) {
      cancelPointColorPicker(); e.preventDefault(); e.stopImmediatePropagation();
    }
  }, true);
})();

/** @param {[number,number][]} points @param {number} index @returns {[number,number]} */
function editorCurvePoint(points, index) {
  var point = points[index];
  if (!point) throw new Error('Selected curve point is unavailable');
  return point;
}
