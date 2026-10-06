// Basic, tone curve, color mixer, color grading, and detail (denoise) controls.
// Classic page script; load boot.js after all definitions.

function setAdjustment(key, value) {
  var vals = adjustmentValues(editorState.recipe);
  vals[key] = Number(value) || 0;
  if (key === 'exposure' || key === 'sharpen_radius') document.getElementById(key + 'Value').textContent = vals[key].toFixed(1);
  else document.getElementById(key + 'Value').textContent = String(Math.round(vals[key]));
  var adj = cloneRecipe(editorState.recipe.adjustments || {});
  ['exposure', 'highlights', 'shadows', 'whites', 'blacks', 'contrast', 'vibrance', 'saturation', 'texture', 'clarity', 'dehaze', 'sharpen', 'noise_reduction'].forEach(function(name) {
    if (Math.abs(vals[name]) > 0.000001) adj[name] = vals[name];
    else delete adj[name];
  });
  if (adj.sharpen && Math.abs(vals.sharpen_radius - 1) > 0.000001) adj.sharpen_radius = vals.sharpen_radius;
  else delete adj.sharpen_radius;
  var wb = {};
  if (Math.abs(vals.temperature) > 0.000001) wb.temperature = vals.temperature;
  if (Math.abs(vals.tint) > 0.000001) wb.tint = vals.tint;
  if (Object.keys(wb).length) adj.white_balance = wb;
  else delete adj.white_balance;
  if (Object.keys(adj).length) editorState.recipe.adjustments = adj;
  else delete editorState.recipe.adjustments;
  markChanged(true);
}

function resetAdjustments() {
  // Detail, tone curve, color mixer, and color grading each live in their
  // own panel band with a dedicated reset button; only clear the basic
  // tone, presence, and white-balance keys owned by this panel.
  var adj = cloneRecipe(editorState.recipe.adjustments || {});
  var kept = {};
  [
    'sharpen', 'sharpen_radius', 'noise_reduction', 'denoise_mode',
    'tone_curve', 'point_curves', 'point_color', 'hsl', 'color_grading',
  ].forEach(function(key) {
    if (adj[key]) kept[key] = adj[key];
  });
  if (Object.keys(kept).length) editorState.recipe.adjustments = kept;
  else delete editorState.recipe.adjustments;
  syncControls();
  markChanged(true);
}

// --- Advanced colour and tone ---------------------------------------------

function _setAdjustmentSection(name, value) {
  var adj = cloneRecipe(editorState.recipe.adjustments || {});
  if (value && Object.keys(value).length) adj[name] = value;
  else delete adj[name];
  if (Object.keys(adj).length) editorState.recipe.adjustments = adj;
  else delete editorState.recipe.adjustments;
}

function syncAdvancedColorControls() {
  syncPointControls();
  var values = advancedColorValues(editorState.recipe);
  Object.keys(TONE_CURVE_DEFAULTS).forEach(function(key) {
    var input = document.getElementById('curve_' + key + 'Range');
    var label = document.getElementById('curve_' + key + 'Value');
    if (input) input.value = String(values.tone_curve[key]);
    if (label) label.textContent = String(Math.round(values.tone_curve[key]));
  });

  var hslSelect = document.getElementById('hslColorSelect');
  var color = hslSelect ? hslSelect.value : 'red';
  var hsl = values.hsl[color] || {};
  [['Hue', 'hue'], ['Saturation', 'saturation'], ['Luminance', 'luminance']].forEach(function(pair) {
    var value = Number(hsl[pair[1]] || 0);
    var input = document.getElementById('hsl' + pair[0] + 'Range');
    var label = document.getElementById('hsl' + pair[0] + 'Value');
    if (input) input.value = String(value);
    if (label) label.textContent = String(Math.round(value));
  });

  var zoneSelect = document.getElementById('colorGradeZoneSelect');
  var zone = zoneSelect ? zoneSelect.value : 'shadows';
  var grade = values.color_grading[zone] || {};
  var hue = Number(grade.hue || 0);
  var saturation = Number(grade.saturation || 0);
  var balance = Number(values.color_grading.balance || 0);
  var hueInput = document.getElementById('colorGradeHueRange');
  var satInput = document.getElementById('colorGradeSaturationRange');
  var balanceInput = document.getElementById('colorGradeBalanceRange');
  if (hueInput) hueInput.value = String(hue);
  if (satInput) satInput.value = String(saturation);
  if (balanceInput) balanceInput.value = String(balance);
  var hueLabel = document.getElementById('colorGradeHueValue');
  var satLabel = document.getElementById('colorGradeSaturationValue');
  var balanceLabel = document.getElementById('colorGradeBalanceValue');
  if (hueLabel) hueLabel.textContent = Math.round(hue) + '°';
  if (satLabel) satLabel.textContent = String(Math.round(saturation));
  if (balanceLabel) balanceLabel.textContent = String(Math.round(balance));
}

function setToneCurvePoint(key, raw) {
  var values = advancedColorValues(editorState.recipe).tone_curve;
  values[key] = Number(raw);
  document.getElementById('curve_' + key + 'Value').textContent = String(Math.round(values[key]));
  var section = {};
  Object.keys(TONE_CURVE_DEFAULTS).forEach(function(name) {
    if (Math.abs(values[name] - TONE_CURVE_DEFAULTS[name]) > 0.000001) section[name] = values[name];
  });
  _setAdjustmentSection('tone_curve', section);
  syncPointControls();
  markChanged(true);
}

function resetToneCurve() {
  _setAdjustmentSection('point_curves', null);
  _setAdjustmentSection('tone_curve', null);
  syncAdvancedColorControls();
  markChanged(true);
}

function setHslControl(key, raw) {
  var select = document.getElementById('hslColorSelect');
  var color = select ? select.value : 'red';
  var values = advancedColorValues(editorState.recipe).hsl;
  var section = cloneRecipe(values[color] || {});
  var value = Number(raw) || 0;
  if (Math.abs(value) > 0.000001) section[key] = value;
  else delete section[key];
  if (Object.keys(section).length) values[color] = section;
  else delete values[color];
  var labelName = key.charAt(0).toUpperCase() + key.slice(1);
  document.getElementById('hsl' + labelName + 'Value').textContent = String(Math.round(value));
  _setAdjustmentSection('hsl', values);
  markChanged(true);
}

function resetHslColor() {
  var select = document.getElementById('hslColorSelect');
  var color = select ? select.value : 'red';
  var values = advancedColorValues(editorState.recipe).hsl;
  delete values[color];
  _setAdjustmentSection('hsl', values);
  syncAdvancedColorControls();
  markChanged(true);
}

function resetHslMixer() {
  _setAdjustmentSection('hsl', null);
  syncAdvancedColorControls();
  markChanged(true);
}

function setColorGradeControl(key, raw) {
  var select = document.getElementById('colorGradeZoneSelect');
  var zone = select ? select.value : 'shadows';
  var values = advancedColorValues(editorState.recipe).color_grading;
  var section = cloneRecipe(values[zone] || {});
  section[key] = Number(raw) || 0;
  values[zone] = section;
  var label = document.getElementById(
    key === 'hue' ? 'colorGradeHueValue' : 'colorGradeSaturationValue'
  );
  if (label) label.textContent = String(Math.round(section[key])) + (key === 'hue' ? '°' : '');
  _setAdjustmentSection('color_grading', values);
  markChanged(true);
}

function setColorGradeBalance(raw) {
  var values = advancedColorValues(editorState.recipe).color_grading;
  var value = Number(raw) || 0;
  if (Math.abs(value) > 0.000001) values.balance = value;
  else delete values.balance;
  document.getElementById('colorGradeBalanceValue').textContent = String(Math.round(value));
  _setAdjustmentSection('color_grading', values);
  markChanged(true);
}

function resetColorGradeZone() {
  var select = document.getElementById('colorGradeZoneSelect');
  var zone = select ? select.value : 'shadows';
  var values = advancedColorValues(editorState.recipe).color_grading;
  delete values[zone];
  _setAdjustmentSection('color_grading', values);
  syncAdvancedColorControls();
  markChanged(true);
}

function resetColorGrading() {
  _setAdjustmentSection('color_grading', null);
  syncAdvancedColorControls();
  markChanged(true);
}

function syncDenoiseControls() {
  var mode = (editorState.recipe.adjustments || {}).denoise_mode || 'standard';
  document.getElementById('denoiseModeSelect').value = mode;
  var status = document.getElementById('denoiseProfileStatus');
  if (mode !== 'camera') {
    status.textContent = '';
    return;
  }
  var profile = (editorState.photo || {}).denoise_profile || {};
  var camera = profile.camera_model || profile.camera_make || 'Unknown camera';
  var iso = profile.iso ? ' · ISO ' + profile.iso : '';
  var message = profile.source === 'camera'
    ? 'Camera profile + image noise estimate'
    : 'No matching camera/ISO profile; using image noise estimate';
  if (profile.match === 'nearest') message += ' (nearest measured ISO ' + profile.profile_iso[0] + ')';
  if (profile.match === 'interpolated') message += ' (interpolated ISO)';
  status.textContent = camera + iso + '. ' + message + '. Adjust Denoise below; inspect at 100% zoom. Larger previews and exports can take longer.';
}

function setDenoiseMode(mode) {
  if (editorState.loading || editorState.showBefore) return;
  var adj = cloneRecipe(editorState.recipe.adjustments || {});
  if (mode === 'camera') adj.denoise_mode = 'camera';
  else delete adj.denoise_mode;
  if (Object.keys(adj).length) editorState.recipe.adjustments = adj;
  else delete editorState.recipe.adjustments;
  syncDenoiseControls();
  markChanged(true);
}

function resetDetail() {
  var adj = cloneRecipe(editorState.recipe.adjustments || {});
  delete adj.sharpen;
  delete adj.sharpen_radius;
  delete adj.noise_reduction;
  delete adj.denoise_mode;
  if (Object.keys(adj).length) editorState.recipe.adjustments = adj;
  else delete editorState.recipe.adjustments;
  syncControls();
  markChanged(true);
}
