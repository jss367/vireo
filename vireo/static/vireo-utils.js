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
    if (!config || typeof config.pipeline !== 'object' || config.pipeline === null) {
      throw new Error('Missing pipeline config');
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
        reject_crop_complete: percent(p.reject_crop_complete, 'reject_crop_complete'),
        reject_focus: percent(p.reject_focus, 'reject_focus'),
        reject_clip_high: percent(p.reject_clip_high, 'reject_clip_high'),
        reject_composite: percent(p.reject_composite, 'reject_composite'),
        burst_lambda: percent(p.burst_lambda, 'burst_lambda'),
        burst_max_keep: asNumber(p.burst_max_keep, 'burst_max_keep'),
        encounter_lambda: percent(p.encounter_lambda, 'encounter_lambda'),
        encounter_max_keep: asNumber(p.encounter_max_keep, 'encounter_max_keep'),
      },
      grouping: {
        w_time: percent(p.w_time, 'w_time'),
        w_subj: percent(p.w_subj, 'w_subj'),
        w_global: percent(p.w_global, 'w_global'),
        w_species: percent(p.w_species, 'w_species'),
        w_meta: percent(p.w_meta, 'w_meta'),
        tau_enc: asNumber(p.tau_enc, 'tau_enc'),
        hard_cut_time: asNumber(p.hard_cut_time, 'hard_cut_time'),
        hard_cut_score: percent(p.hard_cut_score, 'hard_cut_score'),
        soft_cut_score: percent(p.soft_cut_score, 'soft_cut_score'),
        merge_score: percent(p.merge_score, 'merge_score'),
        merge_max_gap: asNumber(p.merge_max_gap, 'merge_max_gap'),
        merge_tau: asNumber(p.merge_tau, 'merge_tau'),
        burst_time_gap: asNumber(p.burst_time_gap, 'burst_time_gap'),
        burst_embedding_distance: embeddingThresholdToDistancePercent(
          p.burst_embedding_threshold
        ),
      },
    };
  }

  return {
    asNumber: asNumber,
    percent: percent,
    pipelineFromConfig: pipelineFromConfig,
    embeddingThresholdToDistancePercent: embeddingThresholdToDistancePercent,
    embeddingDistancePercentToThreshold: embeddingDistancePercentToThreshold,
    buildSliderDefaults: buildSliderDefaults,
  };
})();
