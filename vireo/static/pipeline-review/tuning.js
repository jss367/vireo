// Scoring and grouping controls, defaults, reflow, and regrouping.
// Classic page script; shared globals are initialized before boot.js runs.

// -- Threshold tuning --

var _reflowTimer = null;
var _regroupTimer = null;

var SCORING_DEFAULTS = {};
var GROUPING_DEFAULTS = {};

function sliderVal(id) {
  var el = document.getElementById(id);
  return el ? parseFloat(el.value) : 0;
}

function updateSliderDisplay(slider) {
  var id = slider.id;
  var valEl = document.getElementById('val' + id.substring(2));
  if (!valEl) return;
  // Integer sliders (raw values, not /100): keep counts, raw seconds
  var INT_SLIDERS = [
    'slBurstKeep','slEncKeep',
    'slBurstTime',
    'slTauEnc','slHardCutTime',
    'slMergeMaxGap','slMergeTau',
  ];
  if (INT_SLIDERS.indexOf(id) >= 0) {
    valEl.textContent = slider.value;
  } else {
    valEl.textContent = (slider.value / 100).toFixed(2);
  }
}

function getScoringConfig() {
  return {
    reject_crop_complete: sliderVal('slRejectCrop') / 100,
    reject_focus: sliderVal('slRejectFocus') / 100,
    reject_clip_high: sliderVal('slRejectClip') / 100,
    reject_composite: sliderVal('slRejectComposite') / 100,
    burst_lambda: sliderVal('slBurstLambda') / 100,
    burst_max_keep: Math.round(sliderVal('slBurstKeep')),
    encounter_lambda: sliderVal('slEncLambda') / 100,
    encounter_max_keep: Math.round(sliderVal('slEncKeep')),
  };
}

function getGroupingConfig() {
  function pct(id) { return parseFloat(document.getElementById(id).value) / 100.0; }
  function num(id) { return parseFloat(document.getElementById(id).value); }
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

function onScoringChange(slider) {
  updateSliderDisplay(slider);
  // Debounce: wait 250ms after last change before calling reflow
  clearTimeout(_reflowTimer);
  var indicator = document.getElementById('reflowIndicator');
  if (indicator) indicator.style.display = 'block';
  _reflowTimer = setTimeout(doReflow, 250);
}

function onGroupingChange(slider) {
  updateSliderDisplay(slider);
  clearTimeout(_regroupTimer);
  var indicator = document.getElementById('reflowIndicator');
  if (indicator) { indicator.style.display = 'block'; indicator.textContent = 'Regrouping...'; }
  _regroupTimer = setTimeout(doRegroupLive, 500);
}

function doReflow() {
  var config = getScoringConfig();
  var body = reviewScopePayload(config);
  if (!body) {
    var noBodyIndicator = document.getElementById('reflowIndicator');
    if (noBodyIndicator) noBodyIndicator.style.display = 'none';
    return;
  }
  // Tag this request with the scope request sequence so a scope switch (or
  // a newer slider request) invalidates its response. Without this, a tune
  // fired in Workspace/Collection scope that resolves after the user
  // switches back to Latest review would replace the cached view with
  // stale scoped data (and scopedCacheInfoFor would recompute against the
  // now-current scope).
  var seq = ++reviewScopeRequestSeq;
  safeFetch('/api/pipeline/reflow', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body),
  }, { toast: false })
  .then(function(data) {
    if (seq !== reviewScopeRequestSeq) return;
    var indicator = document.getElementById('reflowIndicator');
    if (indicator) indicator.style.display = 'none';
    if (!data || data.error) return;
    applyReviewResults(data, scopedCacheInfoFor(data));
  })
  .catch(function() {
    if (seq !== reviewScopeRequestSeq) return;
    var indicator = document.getElementById('reflowIndicator');
    if (indicator) indicator.style.display = 'none';
  });
}

function doRegroupLive() {
  var config = Object.assign({}, getGroupingConfig(), getScoringConfig());
  var body = reviewScopePayload(config);
  if (!body) {
    var noBodyIndicator = document.getElementById('reflowIndicator');
    if (noBodyIndicator) noBodyIndicator.style.display = 'none';
    return;
  }
  var seq = ++reviewScopeRequestSeq;
  safeFetch('/api/pipeline/regroup-live', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body),
  }, { toast: false })
  .then(function(data) {
    if (seq !== reviewScopeRequestSeq) return;
    var indicator = document.getElementById('reflowIndicator');
    if (indicator) indicator.style.display = 'none';
    if (!data || data.error) return;
    applyReviewResults(data, scopedCacheInfoFor(data));
  })
  .catch(function() {
    if (seq !== reviewScopeRequestSeq) return;
    var indicator = document.getElementById('reflowIndicator');
    if (indicator) indicator.style.display = 'none';
  });
}

function applyScoringDefaults() {
  document.getElementById('slRejectCrop').value = SCORING_DEFAULTS.reject_crop_complete;
  document.getElementById('slRejectFocus').value = SCORING_DEFAULTS.reject_focus;
  document.getElementById('slRejectClip').value = SCORING_DEFAULTS.reject_clip_high;
  document.getElementById('slRejectComposite').value = SCORING_DEFAULTS.reject_composite;
  document.getElementById('slBurstLambda').value = SCORING_DEFAULTS.burst_lambda;
  document.getElementById('slBurstKeep').value = SCORING_DEFAULTS.burst_max_keep;
  document.getElementById('slEncLambda').value = SCORING_DEFAULTS.encounter_lambda;
  document.getElementById('slEncKeep').value = SCORING_DEFAULTS.encounter_max_keep;
  document.querySelectorAll('.sidebar-section input[type="range"]').forEach(function(sl) {
    updateSliderDisplay(sl);
  });
}

function resetScoringDefaults() {
  applyScoringDefaults();
  doReflow();
}

function applyGroupingDefaults() {
  function setVal(id, v) { var el = document.getElementById(id); if (el) el.value = v; }
  // Weights (slider values 0..100)
  setVal('slWTime', GROUPING_DEFAULTS.w_time);
  setVal('slWSubj', GROUPING_DEFAULTS.w_subj);
  setVal('slWGlobal', GROUPING_DEFAULTS.w_global);
  setVal('slWSpecies', GROUPING_DEFAULTS.w_species);
  setVal('slWMeta', GROUPING_DEFAULTS.w_meta);
  // Cut thresholds
  setVal('slTauEnc', GROUPING_DEFAULTS.tau_enc);
  setVal('slHardCutTime', GROUPING_DEFAULTS.hard_cut_time);
  setVal('slEncCut', GROUPING_DEFAULTS.hard_cut_score);
  setVal('slSoftCut', GROUPING_DEFAULTS.soft_cut_score);
  // Merge
  setVal('slEncMerge', GROUPING_DEFAULTS.merge_score);
  setVal('slMergeMaxGap', GROUPING_DEFAULTS.merge_max_gap);
  setVal('slMergeTau', GROUPING_DEFAULTS.merge_tau);
  // Bursts
  setVal('slBurstTime', GROUPING_DEFAULTS.burst_time_gap);
  setVal('slBurstEmb', GROUPING_DEFAULTS.burst_embedding_distance);
  document.querySelectorAll('.sidebar-section input[type="range"]').forEach(function(sl) {
    updateSliderDisplay(sl);
  });
}

function resetGroupingDefaults() {
  applyGroupingDefaults();
  doRegroupLive();
}

function saveGroupingDefaults() {
  var config = getGroupingConfig();
  safeFetch('/api/pipeline/save-grouping-defaults', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({pipeline: config}),
  })
  .then(function() { showToast('Saved as defaults', 'success'); })
  .catch(function() { /* error toast already shown by safeFetch */ });
}

function loadPipelineReviewTuningDefaults() {
    /* Load slider defaults from the backend config source of truth. */
    (async function() {
      try {
        var cfg = await safeFetch('/api/config');
        var defaults = VireoPipelineConfig.buildSliderDefaults(cfg);
        SCORING_DEFAULTS = defaults.scoring;
        GROUPING_DEFAULTS = defaults.grouping;
      } catch(e) {
        console.warn('Could not load pipeline config:', e);
        defaults = VireoPipelineConfig.buildSliderDefaults();
        SCORING_DEFAULTS = defaults.scoring;
        GROUPING_DEFAULTS = defaults.grouping;
      }
      applyScoringDefaults();
      applyGroupingDefaults();
    })();
}
