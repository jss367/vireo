// Recipe values, save normalization, dirty state, summary, and control sync.
// Classic page script; load boot.js after all definitions.

var TONE_CURVE_DEFAULTS = {black: 0, shadows: 25, midtones: 50, highlights: 75, white: 100};
var HSL_COLORS = ['red', 'orange', 'yellow', 'green', 'aqua', 'blue', 'purple', 'magenta'];
var COLOR_GRADE_ZONES = ['shadows', 'midtones', 'highlights'];

function adjustmentValues(recipe) {
  var adj = recipe && recipe.adjustments || {};
  var wb = adj.white_balance || {};
  return {
    exposure: Number(adj.exposure || 0),
    highlights: Number(adj.highlights || 0),
    shadows: Number(adj.shadows || 0),
    whites: Number(adj.whites || 0),
    blacks: Number(adj.blacks || 0),
    contrast: Number(adj.contrast || 0),
    texture: Number(adj.texture || 0),
    clarity: Number(adj.clarity || 0),
    dehaze: Number(adj.dehaze || 0),
    temperature: Number(wb.temperature || adj.temperature || 0),
    tint: Number(wb.tint || adj.tint || 0),
    vibrance: Number(adj.vibrance || 0),
    saturation: Number(adj.saturation || 0),
    sharpen: Number(adj.sharpen || 0),
    sharpen_radius: Number(adj.sharpen_radius || 1),
    noise_reduction: Number(adj.noise_reduction || 0),
  };
}

function advancedColorValues(recipe) {
  var adj = recipe && recipe.adjustments || {};
  var curveIn = adj.tone_curve || {};
  var curve = {};
  Object.keys(TONE_CURVE_DEFAULTS).forEach(function(key) {
    curve[key] = Object.prototype.hasOwnProperty.call(curveIn, key)
      ? Number(curveIn[key]) : TONE_CURVE_DEFAULTS[key];
  });
  return {
    tone_curve: curve,
    hsl: cloneRecipe(adj.hsl || {}),
    color_grading: cloneRecipe(adj.color_grading || {}),
  };
}

function recipeForSave(recipe) {
  var out = cloneRecipe(recipe);
  delete out.version;
  if (!Number(out.rotation)) delete out.rotation;
  else out.rotation = ((Number(out.rotation) % 360) + 360) % 360;
  if (!out.rotation) delete out.rotation;

  var straighten = Number(out.straighten || 0);
  if (Math.abs(straighten) < 0.0001) delete out.straighten;
  else out.straighten = Math.round(straighten * 10000) / 10000;

  var flip = out.flip || {};
  var nextFlip = {};
  if (flip.horizontal) nextFlip.horizontal = true;
  if (flip.vertical) nextFlip.vertical = true;
  if (Object.keys(nextFlip).length) out.flip = nextFlip;
  else delete out.flip;

  var crop = out.crop ? clampCrop(out.crop) : null;
  if (!crop || isFullCrop(crop)) delete out.crop;
  else {
    out.crop = {
      x: Math.round(crop.x * 1000000) / 1000000,
      y: Math.round(crop.y * 1000000) / 1000000,
      w: Math.round(crop.w * 1000000) / 1000000,
      h: Math.round(crop.h * 1000000) / 1000000,
    };
  }

  var vals = adjustmentValues(out);
  var adj = {};
  if ((out.adjustments || {}).denoise_mode === 'camera') adj.denoise_mode = 'camera';
  ['exposure', 'highlights', 'shadows', 'whites', 'blacks', 'contrast', 'vibrance', 'saturation', 'texture', 'clarity', 'dehaze', 'sharpen', 'noise_reduction'].forEach(function(key) {
    if (Math.abs(Number(vals[key] || 0)) > 0.000001) adj[key] = Number(vals[key]);
  });
  // Radius only rides along with active sharpening, and its 1.0 default is
  // canonicalized to absence — mirrors normalize_recipe so dirty-state
  // comparison stays stable against the server's canonical form.
  if (adj.sharpen && Math.abs(Number(vals.sharpen_radius) - 1) > 0.000001) {
    adj.sharpen_radius = Number(vals.sharpen_radius);
  }
  var wb = {};
  ['temperature', 'tint'].forEach(function(key) {
    if (Math.abs(Number(vals[key] || 0)) > 0.000001) wb[key] = Number(vals[key]);
  });
  if (Object.keys(wb).length) adj.white_balance = wb;

  var advanced = advancedColorValues(out);
  var curve = {};
  Object.keys(TONE_CURVE_DEFAULTS).forEach(function(key) {
    var value = Number(advanced.tone_curve[key]);
    if (Math.abs(value - TONE_CURVE_DEFAULTS[key]) > 0.000001) curve[key] = value;
  });
  if (Object.keys(curve).length) adj.tone_curve = curve;

  var hsl = {};
  HSL_COLORS.forEach(function(color) {
    var source = advanced.hsl[color] || {};
    var section = {};
    ['hue', 'saturation', 'luminance'].forEach(function(key) {
      var value = Number(source[key] || 0);
      if (Math.abs(value) > 0.000001) section[key] = value;
    });
    if (Object.keys(section).length) hsl[color] = section;
  });
  if (Object.keys(hsl).length) adj.hsl = hsl;

  var grading = {};
  COLOR_GRADE_ZONES.forEach(function(zone) {
    var source = advanced.color_grading[zone] || {};
    var saturation = Number(source.saturation || 0);
    if (saturation > 0.000001) {
      var hue = ((Number(source.hue || 0) % 360) + 360) % 360;
      grading[zone] = {hue: hue, saturation: saturation};
    }
  });
  if (Object.keys(grading).length) {
    var balance = Number(advanced.color_grading.balance || 0);
    if (Math.abs(balance) > 0.000001) grading.balance = balance;
    adj.color_grading = grading;
  }
  canonicalPointControls(out.adjustments || {}, adj);
  if (Object.keys(adj).length) out.adjustments = adj;
  else delete out.adjustments;
  return out;
}

function recipeKey(recipe) {
  return JSON.stringify(recipeForSave(recipe || {}));
}

function updateDirtyState() {
  var dirty = recipeKey(editorState.recipe) !== recipeKey(editorState.savedRecipe);
  document.getElementById('saveBtn').disabled = !dirty;
  if (dirty) setStatus('Unsaved');
  else setStatus('');
  updateRecipeSummary();
}

function updateRecipeSummary() {
  var r = recipeForSave(editorState.recipe);
  var parts = [];
  if (r.rotation) parts.push('rotate ' + r.rotation);
  if (r.flip && r.flip.horizontal) parts.push('flip H');
  if (r.flip && r.flip.vertical) parts.push('flip V');
  if (r.straighten) parts.push('straighten ' + Number(r.straighten).toFixed(1));
  if (r.crop) parts.push('crop');
  if (r.adjustments) {
    Object.keys(r.adjustments).forEach(function(key) {
      if (key === 'white_balance') parts.push('white balance');
      else if (key === 'sharpen_radius') return; // folded into "sharpen"
      else if (key === 'denoise_mode') parts.push('camera-aware denoising');
      else if (key === 'noise_reduction') parts.push('noise reduction');
      else if (key === 'tone_curve' || key === 'point_curves') parts.push('tone curve');
      else if (key === 'point_color') parts.push('point color');
      else if (key === 'hsl') parts.push('color mixer');
      else if (key === 'color_grading') parts.push('color grading');
      else parts.push(key);
    });
  }
  if (r.local && r.local.regions) {
    r.local.regions.forEach(function(entry) {
      parts.push('local ' + entry.region);
    });
  }
  document.getElementById('recipeSummary').textContent = parts.length ? parts.join(' | ') : 'No edits';
}

function syncControls() {
  var r = editorState.recipe;
  var vals = adjustmentValues(r);
  var set = function(id, value, fixed) {
    var input = document.getElementById(id + 'Range');
    var label = document.getElementById(id + 'Value');
    if (input) input.value = String(value || 0);
    if (label) label.textContent = fixed ? Number(value || 0).toFixed(1) : String(Math.round(Number(value || 0)));
  };
  set('exposure', vals.exposure, true);
  set('highlights', vals.highlights, false);
  set('shadows', vals.shadows, false);
  set('whites', vals.whites, false);
  set('blacks', vals.blacks, false);
  set('contrast', vals.contrast, false);
  set('texture', vals.texture, false);
  set('clarity', vals.clarity, false);
  set('dehaze', vals.dehaze, false);
  set('temperature', vals.temperature, false);
  set('tint', vals.tint, false);
  set('vibrance', vals.vibrance, false);
  set('saturation', vals.saturation, false);
  set('sharpen', vals.sharpen, false);
  set('sharpen_radius', vals.sharpen_radius, true);
  set('noise_reduction', vals.noise_reduction, false);
  syncDenoiseControls();
  syncAdvancedColorControls();
  var straight = Number(r.straighten || 0);
  var straightInput = document.getElementById('straightenRange');
  // The recipe schema allows ±45° (pasted/API recipes), wider than the ±10°
  // slider. Widen the slider to fit an out-of-range value instead of letting
  // the browser silently clamp the thumb while the label shows the real number.
  var straightLimit = Math.max(10, Math.ceil(Math.abs(straight)));
  straightInput.min = String(-straightLimit);
  straightInput.max = String(straightLimit);
  straightInput.value = String(straight);
  document.getElementById('straightenValue').textContent = straight.toFixed(1);
  syncLocalControls();
  renderCropBox();
  updateAspectButtons();
  updateDirtyState();
}

function previewRecipeFor(recipe, applyCrop) {
  var r = recipeForSave(recipe || {});
  if (!applyCrop) delete r.crop;
  return r;
}

function previewRecipe() {
  return previewRecipeFor(editorState.recipe, editorPreviewAppliesCrop());
}
