/* Vireo shared utilities — loaded by _navbar.html on every page. */

function escapeHtml(str) {
  if (str == null) return '';
  var div = document.createElement('div');
  div.appendChild(document.createTextNode(String(str)));
  return div.innerHTML;
}

function escapeAttr(str) {
  if (str == null) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/'/g, '&#39;')
    .replace(/"/g, '&quot;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

var VireoPipelineConfig = (function() {
  function defaultPipeline() {
    var defaults = window.VIREO_CONFIG_DEFAULTS;
    if (!defaults || typeof defaults.pipeline !== 'object' || defaults.pipeline === null) {
      throw new Error('Missing rendered pipeline defaults');
    }
    return defaults.pipeline;
  }

  function pipelineValue(pipeline, key) {
    if (pipeline && pipeline[key] != null) return pipeline[key];
    return defaultPipeline()[key];
  }

  function asNumber(value, key) {
    var n = Number(value);
    if (!Number.isFinite(n)) {
      throw new Error('Missing numeric pipeline config: ' + key);
    }
    return n;
  }

  function percent(value, key) {
    return Math.round(asNumber(value, key) * 100);
  }

  function pipelineFromConfig(config) {
    if (!config) return defaultPipeline();
    if (typeof config.pipeline !== 'object' || config.pipeline === null) {
      return defaultPipeline();
    }
    return config.pipeline;
  }

  function embeddingThresholdToDistancePercent(threshold) {
    return Math.round((1 - asNumber(threshold, 'burst_embedding_threshold')) * 100);
  }

  function embeddingDistancePercentToThreshold(distancePercent) {
    return 1 - asNumber(distancePercent, 'burst_embedding_distance') / 100;
  }

  function buildSliderDefaults(config) {
    var p = pipelineFromConfig(config);
    return {
      scoring: {
        reject_crop_complete: percent(
          pipelineValue(p, 'reject_crop_complete'), 'reject_crop_complete'
        ),
        reject_focus: percent(pipelineValue(p, 'reject_focus'), 'reject_focus'),
        reject_clip_high: percent(
          pipelineValue(p, 'reject_clip_high'), 'reject_clip_high'
        ),
        reject_composite: percent(
          pipelineValue(p, 'reject_composite'), 'reject_composite'
        ),
        burst_lambda: percent(pipelineValue(p, 'burst_lambda'), 'burst_lambda'),
        burst_max_keep: asNumber(
          pipelineValue(p, 'burst_max_keep'), 'burst_max_keep'
        ),
        encounter_lambda: percent(
          pipelineValue(p, 'encounter_lambda'), 'encounter_lambda'
        ),
        encounter_max_keep: asNumber(
          pipelineValue(p, 'encounter_max_keep'), 'encounter_max_keep'
        ),
      },
      grouping: {
        w_time: percent(pipelineValue(p, 'w_time'), 'w_time'),
        w_subj: percent(pipelineValue(p, 'w_subj'), 'w_subj'),
        w_global: percent(pipelineValue(p, 'w_global'), 'w_global'),
        w_species: percent(pipelineValue(p, 'w_species'), 'w_species'),
        w_meta: percent(pipelineValue(p, 'w_meta'), 'w_meta'),
        tau_enc: asNumber(pipelineValue(p, 'tau_enc'), 'tau_enc'),
        hard_cut_time: asNumber(
          pipelineValue(p, 'hard_cut_time'), 'hard_cut_time'
        ),
        hard_cut_score: percent(
          pipelineValue(p, 'hard_cut_score'), 'hard_cut_score'
        ),
        soft_cut_score: percent(
          pipelineValue(p, 'soft_cut_score'), 'soft_cut_score'
        ),
        merge_score: percent(pipelineValue(p, 'merge_score'), 'merge_score'),
        merge_max_gap: asNumber(
          pipelineValue(p, 'merge_max_gap'), 'merge_max_gap'
        ),
        merge_tau: asNumber(pipelineValue(p, 'merge_tau'), 'merge_tau'),
        burst_time_gap: asNumber(
          pipelineValue(p, 'burst_time_gap'), 'burst_time_gap'
        ),
        burst_embedding_distance: embeddingThresholdToDistancePercent(
          pipelineValue(p, 'burst_embedding_threshold')
        ),
      },
    };
  }

  return {
    defaultPipeline: defaultPipeline,
    pipelineValue: pipelineValue,
    asNumber: asNumber,
    percent: percent,
    pipelineFromConfig: pipelineFromConfig,
    embeddingThresholdToDistancePercent: embeddingThresholdToDistancePercent,
    embeddingDistancePercentToThreshold: embeddingDistancePercentToThreshold,
    buildSliderDefaults: buildSliderDefaults,
  };
})();
