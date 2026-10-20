// The scoring and grouping sliders: reading them into request configs,
// defaults and resets, saving grouping defaults, and the debounced
// recompute on change.
// Classic page script.

/** @param {string} id */
function sliderVal(id) {
  var el = /** @type {HTMLInputElement | null} */ (document.getElementById(id));
  return el ? parseFloat(el.value) : 0;
}

/** @param {HTMLInputElement} slider */
function updateSliderDisplay(slider) {
  var id = slider.id;
  var valEl = document.getElementById('val' + id.substring(2));
  if (!valEl) return;
  var INT_SLIDERS = [
    'slBurstKeep','slEncKeep',
    'slBurstTime',
    'slTauEnc','slHardCutTime',
    'slMergeMaxGap','slMergeTau',
  ];
  valEl.textContent = INT_SLIDERS.indexOf(id) >= 0
    ? slider.value
    : (Number(slider.value) / 100).toFixed(2);
}

function getScoringConfig() {
  return {
    reject_crop_complete: sliderVal('slRejectCrop') / 100,
    reject_clip_high: sliderVal('slRejectClip') / 100,
    reject_composite: sliderVal('slRejectComposite') / 100,
    burst_lambda: sliderVal('slBurstLambda') / 100,
    burst_max_keep: Math.round(sliderVal('slBurstKeep')),
    encounter_lambda: sliderVal('slEncLambda') / 100,
    encounter_max_keep: Math.round(sliderVal('slEncKeep')),
  };
}

function getGroupingConfig() {
  /** @param {string} id */
  function pct(id) { return parseFloat(/** @type {HTMLInputElement} */ (document.getElementById(id)).value) / 100.0; }
  /** @param {string} id */
  function num(id) { return parseFloat(/** @type {HTMLInputElement} */ (document.getElementById(id)).value); }
  return {
    w_time: pct('slWTime'),
    w_subj: pct('slWSubj'),
    w_global: pct('slWGlobal'),
    w_species: pct('slWSpecies'),
    w_meta: pct('slWMeta'),
    tau_enc: num('slTauEnc'),
    hard_cut_time: num('slHardCutTime'),
    hard_cut_score: pct('slEncCut'),
    soft_cut_score: pct('slSoftCut'),
    merge_score: pct('slEncMerge'),
    merge_max_gap: num('slMergeMaxGap'),
    merge_tau: num('slMergeTau'),
    burst_time_gap: num('slBurstTime'),
    burst_embedding_threshold: VireoPipelineConfig.embeddingDistancePercentToThreshold(
      sliderVal('slBurstEmb')
    ),
  };
}

function applyScoringDefaults() {
  /** @type {HTMLInputElement} */ (document.getElementById('slRejectCrop')).value = String(SCORING_DEFAULTS.reject_crop_complete);
  /** @type {HTMLInputElement} */ (document.getElementById('slRejectFocus')).value = String(SCORING_DEFAULTS.reject_focus);
  /** @type {HTMLInputElement} */ (document.getElementById('slRejectClip')).value = String(SCORING_DEFAULTS.reject_clip_high);
  /** @type {HTMLInputElement} */ (document.getElementById('slRejectComposite')).value = String(SCORING_DEFAULTS.reject_composite);
  /** @type {HTMLInputElement} */ (document.getElementById('slBurstLambda')).value = String(SCORING_DEFAULTS.burst_lambda);
  /** @type {HTMLInputElement} */ (document.getElementById('slBurstKeep')).value = String(SCORING_DEFAULTS.burst_max_keep);
  /** @type {HTMLInputElement} */ (document.getElementById('slEncLambda')).value = String(SCORING_DEFAULTS.encounter_lambda);
  /** @type {HTMLInputElement} */ (document.getElementById('slEncKeep')).value = String(SCORING_DEFAULTS.encounter_max_keep);
  /** @type {NodeListOf<HTMLInputElement>} */ (document.querySelectorAll('#cullSettings input[type="range"]')).forEach(updateSliderDisplay);
}

function applyGroupingDefaults() {
  /** @param {string} id @param {number | undefined} v */
  function setVal(id, v) { var el = /** @type {HTMLInputElement | null} */ (document.getElementById(id)); if (el) el.value = String(v); }
  setVal('slWTime', GROUPING_DEFAULTS.w_time);
  setVal('slWSubj', GROUPING_DEFAULTS.w_subj);
  setVal('slWGlobal', GROUPING_DEFAULTS.w_global);
  setVal('slWSpecies', GROUPING_DEFAULTS.w_species);
  setVal('slWMeta', GROUPING_DEFAULTS.w_meta);
  setVal('slTauEnc', GROUPING_DEFAULTS.tau_enc);
  setVal('slHardCutTime', GROUPING_DEFAULTS.hard_cut_time);
  setVal('slEncCut', GROUPING_DEFAULTS.hard_cut_score);
  setVal('slSoftCut', GROUPING_DEFAULTS.soft_cut_score);
  setVal('slEncMerge', GROUPING_DEFAULTS.merge_score);
  setVal('slMergeMaxGap', GROUPING_DEFAULTS.merge_max_gap);
  setVal('slMergeTau', GROUPING_DEFAULTS.merge_tau);
  setVal('slBurstTime', GROUPING_DEFAULTS.burst_time_gap);
  setVal('slBurstEmb', GROUPING_DEFAULTS.burst_embedding_distance);
  /** @type {NodeListOf<HTMLInputElement>} */ (document.querySelectorAll('#cullSettings input[type="range"]')).forEach(updateSliderDisplay);
}

function resetScoringDefaults() {
  applyScoringDefaults();
  doReflow();
}

function resetGroupingDefaults() {
  applyGroupingDefaults();
  doRegroupLive();
}

function saveGroupingDefaults() {
  safeFetch('/api/pipeline/save-grouping-defaults', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({pipeline: getGroupingConfig()}),
  })
  .then(function() { if (typeof showToast === 'function') showToast('Saved as defaults', 'success'); })
  .catch(function() {});
}

/** @param {HTMLInputElement} slider */
function onScoringChange(slider) {
  updateSliderDisplay(slider);
  if (_reflowTimer != null) clearTimeout(_reflowTimer);
  var indicator = /** @type {HTMLElement} */ (document.getElementById('reflowIndicator'));
  if (indicator) { indicator.style.display = 'block'; indicator.textContent = 'Recomputing...'; }
  _reflowTimer = setTimeout(doReflow, 250);
  updateCullApplyButton();
}

/** @param {HTMLInputElement} slider */
function onGroupingChange(slider) {
  updateSliderDisplay(slider);
  if (_regroupTimer != null) clearTimeout(_regroupTimer);
  var indicator = /** @type {HTMLElement} */ (document.getElementById('reflowIndicator'));
  if (indicator) { indicator.style.display = 'block'; indicator.textContent = 'Regrouping...'; }
  _regroupTimer = setTimeout(doRegroupLive, 500);
  updateCullApplyButton();
}
