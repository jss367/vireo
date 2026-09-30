async function loadConfig() {
  try {
    var cfg = await safeFetch('/api/config', {}, { toast: false });
    var pct = Math.round((cfg.classification_threshold != null ? cfg.classification_threshold : 0.4) * 100);
    document.getElementById('cfgThreshold').value = pct;
    document.getElementById('cfgThresholdVal').textContent = pct + '%';
    document.getElementById('cfgGroupingWindow').value = cfg.grouping_window_seconds != null ? cfg.grouping_window_seconds : 10;
    var simPct = Math.round((cfg.similarity_threshold != null ? cfg.similarity_threshold : 0.85) * 100);
    document.getElementById('cfgSimilarity').value = simPct;
    document.getElementById('cfgSimilarityVal').textContent = simPct + '%';
    document.getElementById('cfgHfToken').value = cfg.hf_token || '';
    document.getElementById('cfgInatToken').value = cfg.inat_token || '';
    if (cfg.inat_token) validateInatToken();
    document.getElementById('cfgGoogleMapsApiKey').value = cfg.google_maps_api_key || '';
    _rememberSavedSecrets(_readSecretFields());
    document.getElementById('cfgGoogleMapsPreferEnglish').checked = cfg.google_maps_prefer_english !== false;
    document.getElementById('cfgKeywordCase').value = cfg.keyword_case || 'auto';
    document.getElementById('cfgMaxEditHistory').value = cfg.max_edit_history != null ? cfg.max_edit_history : 1000;
    // Load editor list. Synthesize from the legacy single string if the user
    // hasn't migrated yet — saving from the new UI completes the migration.
    // Defend against malformed entries (non-string path/name from hand-edited
    // config.json or any non-validating /api/config writer) so we don't crash
    // the settings page on load.
    var rawList = Array.isArray(cfg.external_editors) ? cfg.external_editors : [];
    _editorsState = [];
    rawList.forEach(function(e) {
      if (!e || typeof e !== 'object') return;
      var path = typeof e.path === 'string' ? e.path : '';
      var name = typeof e.name === 'string' ? e.name : '';
      _editorsState.push({ name: name, path: path });
    });
    if (_editorsState.length === 0 && typeof cfg.external_editor === 'string'
        && cfg.external_editor.trim()) {
      _editorsState = [{ name: 'Editor', path: cfg.external_editor.trim() }];
    }
    renderExternalEditors();
    // Remote (SSH) move targets. Coerce defensively against a hand-edited
    // config.json so a malformed entry can't crash the settings page.
    var rawTargets = Array.isArray(cfg.remote_targets) ? cfg.remote_targets : [];
    _remoteTargetsState = [];
    rawTargets.forEach(function(t) {
      if (!t || typeof t !== 'object') return;
      // Adding a target field? Also update the row builder, addRemoteTarget,
      // and collectRemoteTargets — a miss HERE silently erases the field on
      // the next save after a page reload.
      _remoteTargetsState.push({
        id: typeof t.id === 'string' && t.id ? t.id : _genTargetId(),
        name: typeof t.name === 'string' ? t.name : '',
        host: typeof t.host === 'string' ? t.host : '',
        user: typeof t.user === 'string' ? t.user : '',
        port: t.port == null ? 22 : t.port,
        ssh_key: typeof t.ssh_key === 'string' ? t.ssh_key : '',
        remote_path: typeof t.remote_path === 'string' ? t.remote_path : '',
        mount_path: typeof t.mount_path === 'string' ? t.mount_path : '',
        local_archive_root: typeof t.local_archive_root === 'string' ? t.local_archive_root : '',
        bwlimit_kbps: t.bwlimit_kbps == null ? 0 : t.bwlimit_kbps,
      });
    });
    renderRemoteTargets();
    document.getElementById('cfgDarktableBin').value = cfg.darktable_bin || '';
    document.getElementById('cfgDarktableStyle').value = cfg.darktable_style || '';
    document.getElementById('cfgDarktableFormat').value = cfg.darktable_output_format || 'jpg';
    document.getElementById('cfgDarktableOutputDir').value = cfg.darktable_output_dir || '';
    document.getElementById('cfgDarktableAutoConvertDng').checked = cfg.darktable_auto_convert_dng === true;
    document.getElementById('cfgDngConverterBin').value = cfg.dng_converter_bin || '';
    loadDarktableStatus();
    // Load display settings
    document.getElementById('cfgPhotosPerPage').value = cfg.photos_per_page != null ? cfg.photos_per_page : 50;
    var bt = cfg.browse_thumb_default != null ? cfg.browse_thumb_default : 220;
    document.getElementById('cfgBrowseThumbDefault').value = bt;
    document.getElementById('cfgBrowseThumbVal').textContent = bt + 'px';
    document.getElementById('cfgOpenInBrowser').checked = cfg.open_in_browser === true;
    // Load browse card fields
    loadCardFieldCheckboxes(cfg.browse_card_fields || ["filename", "location_status", "rating", "flag", "sharpness"]);
    // Load the filter bar's quick-filter buttons
    loadFilterShortcuts(cfg.filter_shortcuts);
    // Load detection settings
    var dc = Math.round((cfg.detector_confidence != null ? cfg.detector_confidence : 0.20) * 100);
    document.getElementById('cfgDetectorConf').value = dc;
    document.getElementById('cfgDetectorConfVal').textContent = (dc / 100).toFixed(2);
    var dp = Math.round((cfg.detection_padding != null ? cfg.detection_padding : 0.20) * 100);
    document.getElementById('cfgDetectionPadding').value = dp;
    document.getElementById('cfgDetectionPaddingVal').textContent = (dp / 100).toFixed(2);
    document.getElementById('cfgTopK').value = cfg.top_k_predictions != null ? cfg.top_k_predictions : 5;
    var rd = Math.round((cfg.redundancy_threshold != null ? cfg.redundancy_threshold : 0.88) * 100);
    document.getElementById('cfgRedundancy').value = rd;
    document.getElementById('cfgRedundancyVal').textContent = (rd / 100).toFixed(2);
    document.getElementById('cfgCullTimeWindow').value = cfg.cull_time_window != null ? cfg.cull_time_window : 60;
    var cph = cfg.cull_phash_threshold != null ? cfg.cull_phash_threshold : 19;
    document.getElementById('cfgCullPhash').value = cph;
    document.getElementById('cfgCullPhashVal').textContent = cph;
    // Load pipeline settings
    var p = VireoPipelineConfig.pipelineFromConfig(cfg);
    var wf = VireoPipelineConfig.percent(p.w_focus, 'w_focus');
    document.getElementById('cfgWFocus').value = wf;
    document.getElementById('cfgWFocusVal').textContent = wf + '%';
    var we = VireoPipelineConfig.percent(p.w_exposure, 'w_exposure');
    document.getElementById('cfgWExposure').value = we;
    document.getElementById('cfgWExposureVal').textContent = we + '%';
    var wc = VireoPipelineConfig.percent(p.w_composition, 'w_composition');
    document.getElementById('cfgWComposition').value = wc;
    document.getElementById('cfgWCompositionVal').textContent = wc + '%';
    var wa = VireoPipelineConfig.percent(p.w_area, 'w_area');
    document.getElementById('cfgWArea').value = wa;
    document.getElementById('cfgWAreaVal').textContent = wa + '%';
    var wn = VireoPipelineConfig.percent(p.w_noise, 'w_noise');
    document.getElementById('cfgWNoise').value = wn;
    document.getElementById('cfgWNoiseVal').textContent = wn + '%';
    var rc = VireoPipelineConfig.percent(p.reject_crop_complete, 'reject_crop_complete');
    document.getElementById('cfgRejectCrop').value = rc;
    document.getElementById('cfgRejectCropVal').textContent = rc + '%';
    var rf = VireoPipelineConfig.percent(p.reject_focus, 'reject_focus');
    document.getElementById('cfgRejectFocus').value = rf;
    document.getElementById('cfgRejectFocusVal').textContent = rf + '%';
    var rcl = VireoPipelineConfig.percent(p.reject_clip_high, 'reject_clip_high');
    document.getElementById('cfgRejectClip').value = rcl;
    document.getElementById('cfgRejectClipVal').textContent = rcl + '%';
    var rco = VireoPipelineConfig.percent(p.reject_composite, 'reject_composite');
    document.getElementById('cfgRejectComposite').value = rco;
    document.getElementById('cfgRejectCompositeVal').textContent = rco + '%';

    // Miss detection tunables
    document.getElementById('cfgMissEnabled').checked = p.miss_enabled !== false;
    var mdc = VireoPipelineConfig.percent(p.miss_det_confidence, 'miss_det_confidence');
    document.getElementById('cfgMissDetConf').value = mdc;
    document.getElementById('cfgMissDetConfVal').textContent = mdc + '%';
    var mbm = Math.round(VireoPipelineConfig.asNumber(p.miss_bbox_area_min, 'miss_bbox_area_min') * 1000);
    document.getElementById('cfgMissBboxMin').value = mbm;
    document.getElementById('cfgMissBboxMinVal').textContent = (mbm / 1000).toFixed(3);
    var mor = VireoPipelineConfig.percent(p.miss_oof_ratio, 'miss_oof_ratio');
    document.getElementById('cfgMissOofRatio').value = mor;
    document.getElementById('cfgMissOofRatioVal').textContent = (mor / 100).toFixed(2);

    // Eye-focus detection tunables
    document.getElementById('cfgEyeDetectEnabled').checked =
      p.eye_detect_enabled === true;  // default false
    var ecg = VireoPipelineConfig.percent(p.eye_classifier_conf_gate, 'eye_classifier_conf_gate');
    document.getElementById('cfgEyeClassifierConfGate').value = ecg;
    document.getElementById('cfgEyeClassifierConfGateVal').textContent = ecg + '%';
    var edg = VireoPipelineConfig.percent(p.eye_detection_conf_gate, 'eye_detection_conf_gate');
    document.getElementById('cfgEyeDetectionConfGate').value = edg;
    document.getElementById('cfgEyeDetectionConfGateVal').textContent = edg + '%';
    var ewk = VireoPipelineConfig.percent(p.eye_window_k, 'eye_window_k');
    document.getElementById('cfgEyeWindowK').value = ewk;
    document.getElementById('cfgEyeWindowKVal').textContent = (ewk / 100).toFixed(2);
    var rejectEye = VireoPipelineConfig.percent(p.reject_eye_focus, 'reject_eye_focus');
    document.getElementById('cfgRejectEyeFocus').value = rejectEye;
    document.getElementById('cfgRejectEyeFocusVal').textContent = rejectEye + '%';
    var btg = VireoPipelineConfig.asNumber(p.burst_time_gap, 'burst_time_gap');
    document.getElementById('cfgBurstTimeGap').value = btg;
    document.getElementById('cfgBurstTimeGapVal').textContent = btg + 's';
    var bemb = VireoPipelineConfig.embeddingThresholdToDistancePercent(
      p.burst_embedding_threshold
    );
    document.getElementById('cfgBurstEmb').value = bemb;
    document.getElementById('cfgBurstEmbVal').textContent = bemb + '%';
    var bl = VireoPipelineConfig.percent(p.burst_lambda, 'burst_lambda');
    document.getElementById('cfgBurstLambda').value = bl;
    document.getElementById('cfgBurstLambdaVal').textContent = bl + '%';
    document.getElementById('cfgBurstMaxKeep').value = VireoPipelineConfig.asNumber(p.burst_max_keep, 'burst_max_keep');
    var el = VireoPipelineConfig.percent(p.encounter_lambda, 'encounter_lambda');
    document.getElementById('cfgEncLambda').value = el;
    document.getElementById('cfgEncLambdaVal').textContent = el + '%';
    document.getElementById('cfgEncMaxKeep').value = VireoPipelineConfig.asNumber(p.encounter_max_keep, 'encounter_max_keep');
    document.getElementById('cfgWTime').value = VireoPipelineConfig.percent(p.w_time, 'w_time');
    document.getElementById('cfgWSubj').value = VireoPipelineConfig.percent(p.w_subj, 'w_subj');
    document.getElementById('cfgWGlobal').value = VireoPipelineConfig.percent(p.w_global, 'w_global');
    document.getElementById('cfgWSpecies').value = VireoPipelineConfig.percent(p.w_species, 'w_species');
    document.getElementById('cfgWMeta').value = VireoPipelineConfig.percent(p.w_meta, 'w_meta');
    document.getElementById('cfgHardCutTime').value = VireoPipelineConfig.asNumber(p.hard_cut_time, 'hard_cut_time');
    var hcs = VireoPipelineConfig.percent(p.hard_cut_score, 'hard_cut_score');
    document.getElementById('cfgHardCutScore').value = hcs;
    document.getElementById('cfgHardCutScoreVal').textContent = hcs + '%';
    var scs = VireoPipelineConfig.percent(p.soft_cut_score, 'soft_cut_score');
    document.getElementById('cfgSoftCutScore').value = scs;
    document.getElementById('cfgSoftCutScoreVal').textContent = scs + '%';
    var ms = VireoPipelineConfig.percent(p.merge_score, 'merge_score');
    document.getElementById('cfgMergeScore').value = ms;
    document.getElementById('cfgMergeScoreVal').textContent = ms + '%';
    document.getElementById('cfgMergeMaxGap').value = VireoPipelineConfig.asNumber(p.merge_max_gap, 'merge_max_gap');
  } catch(e) {
    console.warn('Could not load settings config:', e);
    var fallbackDistance = VireoPipelineConfig.buildSliderDefaults()
      .grouping.burst_embedding_distance;
    document.getElementById('cfgBurstEmb').value = fallbackDistance;
    document.getElementById('cfgBurstEmbVal').textContent = fallbackDistance + '%';
    _markSettingsInitialLoad('config');
    return false;
  }
  _markSettingsInitialLoad('config');
  return true;
}

var CARD_FIELD_OPTIONS = [
  { id: 'filename', label: 'Filename', desc: 'Photo filename' },
  { id: 'location_status', label: 'Coordinate source', desc: 'EXIF GPS, assigned map location, or no coordinates' },
  { id: 'rating', label: 'Rating', desc: 'Star rating' },
  { id: 'flag', label: 'Flag', desc: 'Flagged / rejected indicator' },
  { id: 'color_label', label: 'Color label dot', desc: 'Color-label dot with its description on hover (the card is tinted regardless)' },
  { id: 'sharpness', label: 'Sharpness', desc: 'Sharpness score' },
  { id: 'species', label: 'Species', desc: 'Species identification badges' },
  { id: 'dimensions', label: 'Dimensions', desc: 'Image width \u00d7 height' },
  { id: 'file_size', label: 'File size', desc: 'e.g. "4.2 MB"' },
  { id: 'capture_date', label: 'Capture date & time', desc: 'Date and time photo was taken (to the minute)' },
  { id: 'extension', label: 'Extension', desc: 'File type (JPG, RAW, etc.)' },
  { id: 'quality_score', label: 'Quality score', desc: 'Pipeline quality score' },
  { id: 'prediction_confidence', label: 'Prediction confidence', desc: 'Score of the strongest current species prediction' },
];

function loadCardFieldCheckboxes(activeFields) {
  var container = document.getElementById('cfgCardFieldsContainer');
  container.innerHTML = '';
  CARD_FIELD_OPTIONS.forEach(function(opt) {
    var checked = activeFields.indexOf(opt.id) !== -1 ? ' checked' : '';
    container.innerHTML += '<label style="display:flex;align-items:center;gap:6px;font-size:13px;color:var(--text-primary);cursor:pointer;">' +
      '<input type="checkbox" id="cfgCardField_' + opt.id + '" value="' + opt.id + '"' + checked +
      ' onchange="saveConfig()" style="accent-color:var(--accent);">' +
      '<span>' + opt.label + '</span>' +
      '<span style="color:var(--text-dim);font-size:11px;">' + opt.desc + '</span>' +
    '</label>';
  });
}

function getSelectedCardFields() {
  var fields = [];
  CARD_FIELD_OPTIONS.forEach(function(opt) {
    var cb = document.getElementById('cfgCardField_' + opt.id);
    if (cb && cb.checked) fields.push(opt.id);
  });
  return fields;
}

function _saveConfigNow(gen) {
    // Direct callers (NAS wizard, etc.) don't hand us a generation, so mint
    // one here. Threading a fresh gen through inflight/ok/error keeps the
    // autosave pill honest when this call overlaps a debounced write.
    if (gen === undefined) gen = _nextSaveGen();
    // Settings import is replacing the whole config; a direct flush would
    // post the pre-import form over it. Refuse so the caller reports a
    // failed save instead of the import being silently overwritten.
    if (_autosaveSuspended) {
      return Promise.reject(new Error('Settings import in progress; try again in a moment.'));
    }
    return _serializedSave('config', function() { return _postConfigSnapshot(gen); });
}

// The secret fields and the value each one held when the page loaded it or
// last saved it. Every autosave posts the whole form, so sending these
// unconditionally let any save (a photos-per-page change in another tab)
// write back the token the page loaded with, over one saved since, and
// cancel an in-flight iNat token validation. Only a field the user changed
// here is sent; the server keeps the stored value for an absent key.
var _SECRET_FIELDS = {
  hf_token: 'cfgHfToken',
  inat_token: 'cfgInatToken',
  google_maps_api_key: 'cfgGoogleMapsApiKey',
};
var _savedSecrets = {};

function _readSecretFields() {
  var values = {};
  Object.keys(_SECRET_FIELDS).forEach(function(key) {
    values[key] = document.getElementById(_SECRET_FIELDS[key]).value.trim();
  });
  return values;
}

function _editedSecrets() {
  var current = _readSecretFields();
  var edited = {};
  Object.keys(current).forEach(function(key) {
    if (current[key] !== _savedSecrets[key]) edited[key] = current[key];
  });
  return edited;
}

function _rememberSavedSecrets(values) {
  Object.keys(values).forEach(function(key) { _savedSecrets[key] = values[key]; });
}

async function _postConfigSnapshot(gen) {
    // Runs only once any earlier config POST has settled; reads the form
    // now so the snapshot reflects every edit made while waiting.
    var threshold = parseInt(document.getElementById('cfgThreshold').value, 10) / 100;
    var grouping = parseInt(document.getElementById('cfgGroupingWindow').value, 10);
    if (isNaN(grouping)) grouping = 10;
    var similarity = parseInt(document.getElementById('cfgSimilarity').value, 10) / 100;
    var secrets = _editedSecrets();
    var keywordCase = document.getElementById('cfgKeywordCase').value;
    var maxEditHistory = parseInt(document.getElementById('cfgMaxEditHistory').value, 10);
    if (isNaN(maxEditHistory)) maxEditHistory = 1000;
    _saveStatusMark('config', 'inflight', gen);
    try {
      await safeFetch('/api/config', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(Object.assign({}, secrets, {
          classification_threshold: threshold,
          grouping_window_seconds: grouping,
          similarity_threshold: similarity,
          keyword_case: keywordCase,
          max_edit_history: maxEditHistory,
          google_maps_prefer_english: document.getElementById('cfgGoogleMapsPreferEnglish').checked,
          external_editors: collectExternalEditors(),
          remote_targets: collectRemoteTargets(),
          // Always clear the legacy single-editor field when we save the new
          // list. Otherwise a user who once had `external_editor` set and now
          // removes every editor from the list can't get back to OS-default
          // behavior — `cfg.get_editors()` falls back to the legacy field
          // whenever the new list is empty, so the old value keeps haunting
          // them. Saving from the new UI is the migration completion step.
          external_editor: '',
          darktable_bin: document.getElementById('cfgDarktableBin').value.trim(),
          darktable_style: document.getElementById('cfgDarktableStyle').value.trim(),
          darktable_output_format: document.getElementById('cfgDarktableFormat').value,
          darktable_output_dir: document.getElementById('cfgDarktableOutputDir').value.trim(),
          darktable_auto_convert_dng: document.getElementById('cfgDarktableAutoConvertDng').checked,
          dng_converter_bin: document.getElementById('cfgDngConverterBin').value.trim(),
          photos_per_page: parseInt(document.getElementById('cfgPhotosPerPage').value, 10) || 50,
          browse_thumb_default: parseInt(document.getElementById('cfgBrowseThumbDefault').value, 10) || 220,
          open_in_browser: document.getElementById('cfgOpenInBrowser').checked,
          browse_card_fields: getSelectedCardFields(),
          filter_shortcuts: collectFilterShortcuts(),
          detector_confidence: parseInt(document.getElementById('cfgDetectorConf').value, 10) / 100,
          detection_padding: parseInt(document.getElementById('cfgDetectionPadding').value, 10) / 100,
          top_k_predictions: parseInt(document.getElementById('cfgTopK').value, 10) || 5,
          redundancy_threshold: parseInt(document.getElementById('cfgRedundancy').value, 10) / 100,
          cull_time_window: parseInt(document.getElementById('cfgCullTimeWindow').value, 10) || 60,
          cull_phash_threshold: parseInt(document.getElementById('cfgCullPhash').value, 10) || 19,
          pipeline: {
            w_focus: parseInt(document.getElementById('cfgWFocus').value, 10) / 100,
            w_exposure: parseInt(document.getElementById('cfgWExposure').value, 10) / 100,
            w_composition: parseInt(document.getElementById('cfgWComposition').value, 10) / 100,
            w_area: parseInt(document.getElementById('cfgWArea').value, 10) / 100,
            w_noise: parseInt(document.getElementById('cfgWNoise').value, 10) / 100,
            reject_crop_complete: parseInt(document.getElementById('cfgRejectCrop').value, 10) / 100,
            reject_focus: parseInt(document.getElementById('cfgRejectFocus').value, 10) / 100,
            reject_clip_high: parseInt(document.getElementById('cfgRejectClip').value, 10) / 100,
            reject_composite: parseInt(document.getElementById('cfgRejectComposite').value, 10) / 100,
            miss_enabled: document.getElementById('cfgMissEnabled').checked,
            // Keep paired thresholds consistent: the UI exposes one slider
            // per pair, but classify_miss reads the sibling value (burst
            // vs singleton) independently. If we only save one side, the
            // other falls back to defaults, which can break the paired
            // relationship when the user picks a value below the paired
            // default. Derive the paired value from the same slider using
            // the default ratio. Burst context and singleton context
            // move in opposite numeric directions because they describe
            // different kinds of evidence:
            //   - det_conf: siblings confirm a subject, so a burst is
            //     forgiving of low confidence (lower threshold). Default
            //     det_conf=0.20 / det_conf_burst=0.12 -> burst = 0.60 * singleton.
            //   - bbox_min: siblings showing a larger subject make a tiny
            //     bbox look like lost framing, so a burst flags more
            //     aggressively (higher threshold). Default bbox_min=0.005 /
            //     bbox_min_singleton=0.002 → singleton = 0.40 * burst.
            ...(function() {
              var det = parseInt(document.getElementById('cfgMissDetConf').value, 10) / 100;
              var bbox = parseInt(document.getElementById('cfgMissBboxMin').value, 10) / 1000;
              return {
                miss_det_confidence: det,
                miss_det_confidence_burst: +(det * 0.60).toFixed(4),
                miss_bbox_area_min: bbox,
                miss_bbox_area_min_singleton: +(bbox * 0.40).toFixed(5),
              };
            })(),
            miss_oof_ratio: parseInt(document.getElementById('cfgMissOofRatio').value, 10) / 100,
            eye_detect_enabled: document.getElementById('cfgEyeDetectEnabled').checked,
            eye_classifier_conf_gate: parseInt(document.getElementById('cfgEyeClassifierConfGate').value, 10) / 100,
            eye_detection_conf_gate: parseInt(document.getElementById('cfgEyeDetectionConfGate').value, 10) / 100,
            eye_window_k: parseInt(document.getElementById('cfgEyeWindowK').value, 10) / 100,
            reject_eye_focus: parseInt(document.getElementById('cfgRejectEyeFocus').value, 10) / 100,
            burst_time_gap: parseInt(document.getElementById('cfgBurstTimeGap').value, 10) || 3,
            burst_embedding_threshold: VireoPipelineConfig.embeddingDistancePercentToThreshold(
              parseInt(document.getElementById('cfgBurstEmb').value, 10)
            ),
            burst_lambda: parseInt(document.getElementById('cfgBurstLambda').value, 10) / 100,
            burst_max_keep: parseInt(document.getElementById('cfgBurstMaxKeep').value, 10) || 3,
            encounter_lambda: parseInt(document.getElementById('cfgEncLambda').value, 10) / 100,
            encounter_max_keep: parseInt(document.getElementById('cfgEncMaxKeep').value, 10) || 5,
            w_time: parseInt(document.getElementById('cfgWTime').value, 10) / 100,
            w_subj: parseInt(document.getElementById('cfgWSubj').value, 10) / 100,
            w_global: parseInt(document.getElementById('cfgWGlobal').value, 10) / 100,
            w_species: parseInt(document.getElementById('cfgWSpecies').value, 10) / 100,
            w_meta: parseInt(document.getElementById('cfgWMeta').value, 10) / 100,
            hard_cut_time: parseInt(document.getElementById('cfgHardCutTime').value, 10) || 180,
            hard_cut_score: parseInt(document.getElementById('cfgHardCutScore').value, 10) / 100,
            soft_cut_score: parseInt(document.getElementById('cfgSoftCutScore').value, 10) / 100,
            merge_score: parseInt(document.getElementById('cfgMergeScore').value, 10) / 100,
            merge_max_gap: parseInt(document.getElementById('cfgMergeMaxGap').value, 10) || 60,
          },
        })),
      });
    } catch (e) {
      _saveStatusMark('config', 'error', gen);
      throw e;
    }
    _rememberSavedSecrets(secrets);
    _saveStatusMark('config', 'ok', gen);
    if (typeof loadPreviewCacheStatus === 'function') loadPreviewCacheStatus();
    // Drop the cached editor list so the next "Open in Editor" action
    // sees changes to the editors list without a page reload.
    if (typeof window.invalidateEditorsCache === 'function') {
      window.invalidateEditorsCache();
    }
}

function saveConfig() {
  // Debounced fire-and-forget for the settings-page inline handlers. The
  // outcome is reported through the autosave pill (_saveStatusMark) and the
  // network layer toasts on failure; the next change re-tries. Callers that
  // need to KNOW the write landed (e.g. the NAS wizard, before it tears down
  // its modal) must call _saveConfigNow() directly and await it.
  if (_autosaveSuspended) {
    // Settings import is replacing the config. Remember that an edit was
    // made so it can be saved if the import does not go through.
    _editedWhileSuspended = true;
    return;
  }
  clearTimeout(_saveTimer);
  var gen = _nextSaveGen();
  _saveStatusMark('config', 'pending', gen);
  _saveTimer = setTimeout(function() {
    _saveConfigNow(gen).catch(function() {});
  }, 500);
}

function resetPipelineDefaults() {
  var p = VireoPipelineConfig.defaultPipeline();
  function setPercent(id, key) {
    setValue(id, VireoPipelineConfig.percent(p[key], key), '%');
  }
  function setValue(id, val, suffix) {
    document.getElementById(id).value = val;
    var span = document.getElementById(id + 'Val');
    if (span) span.textContent = val + suffix;
  }
  setPercent('cfgWFocus', 'w_focus');
  setPercent('cfgWExposure', 'w_exposure');
  setPercent('cfgWComposition', 'w_composition');
  setPercent('cfgWArea', 'w_area');
  setPercent('cfgWNoise', 'w_noise');
  setPercent('cfgRejectCrop', 'reject_crop_complete');
  setPercent('cfgRejectFocus', 'reject_focus');
  setPercent('cfgRejectClip', 'reject_clip_high');
  setPercent('cfgRejectComposite', 'reject_composite');
  setPercent('cfgEyeClassifierConfGate', 'eye_classifier_conf_gate');
  setPercent('cfgEyeDetectionConfGate', 'eye_detection_conf_gate');
  setPercent('cfgRejectEyeFocus', 'reject_eye_focus');
  setValue('cfgBurstTimeGap', p.burst_time_gap, 's');
  setValue(
    'cfgBurstEmb',
    VireoPipelineConfig.embeddingThresholdToDistancePercent(
      p.burst_embedding_threshold
    ),
    '%'
  );
  setPercent('cfgBurstLambda', 'burst_lambda');
  setPercent('cfgEncLambda', 'encounter_lambda');
  setPercent('cfgHardCutScore', 'hard_cut_score');
  setPercent('cfgSoftCutScore', 'soft_cut_score');
  setPercent('cfgMergeScore', 'merge_score');
  var eyeWindow = VireoPipelineConfig.percent(p.eye_window_k, 'eye_window_k');
  document.getElementById('cfgEyeWindowK').value = eyeWindow;
  document.getElementById('cfgEyeWindowKVal').textContent =
    (eyeWindow / 100).toFixed(2);
  document.getElementById('cfgEyeDetectEnabled').checked = p.eye_detect_enabled === true;
  document.getElementById('cfgMissEnabled').checked = p.miss_enabled !== false;
  setPercent('cfgMissDetConf', 'miss_det_confidence');
  var bboxMin = Math.round(
    VireoPipelineConfig.asNumber(p.miss_bbox_area_min, 'miss_bbox_area_min') * 1000
  );
  document.getElementById('cfgMissBboxMin').value = bboxMin;
  document.getElementById('cfgMissBboxMinVal').textContent =
    (bboxMin / 1000).toFixed(3);
  var missOofRatio = VireoPipelineConfig.percent(p.miss_oof_ratio, 'miss_oof_ratio');
  document.getElementById('cfgMissOofRatio').value = missOofRatio;
  document.getElementById('cfgMissOofRatioVal').textContent =
    (missOofRatio / 100).toFixed(2);
  document.getElementById('cfgBurstMaxKeep').value = p.burst_max_keep;
  document.getElementById('cfgEncMaxKeep').value = p.encounter_max_keep;
  document.getElementById('cfgWTime').value =
    VireoPipelineConfig.percent(p.w_time, 'w_time');
  document.getElementById('cfgWSubj').value =
    VireoPipelineConfig.percent(p.w_subj, 'w_subj');
  document.getElementById('cfgWGlobal').value =
    VireoPipelineConfig.percent(p.w_global, 'w_global');
  document.getElementById('cfgWSpecies').value =
    VireoPipelineConfig.percent(p.w_species, 'w_species');
  document.getElementById('cfgWMeta').value =
    VireoPipelineConfig.percent(p.w_meta, 'w_meta');
  document.getElementById('cfgHardCutTime').value = p.hard_cut_time;
  document.getElementById('cfgMergeMaxGap').value = p.merge_max_gap;
  saveConfig();
}

function toggleInatTokenVisibility() {
  var inp = document.getElementById('cfgInatToken');
  inp.type = inp.type === 'password' ? 'text' : 'password';
}

function toggleGoogleMapsKeyVisibility() {
  var inp = document.getElementById('cfgGoogleMapsApiKey');
  inp.type = inp.type === 'password' ? 'text' : 'password';
}

async function validateInatToken() {
  var token = document.getElementById('cfgInatToken').value.trim();
  var status = document.getElementById('inatTokenStatus');
  if (!token) { status.innerHTML = ''; return; }
  status.innerHTML = '<span style="color:var(--text-dim);">Validating...</span>';
  try {
    var data = await safeFetch('/api/inat/validate-token', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({token: token}),
    }, { toast: false });
    if (data.error) {
      status.innerHTML = '<span style="color:var(--danger);">&#10007; Invalid or expired token</span>';
    } else {
      status.innerHTML = '<span style="color:var(--accent);">&#10003; Logged in as <strong>' + escapeHtml(data.login) + '</strong></span>';
    }
  } catch(e) {
    status.innerHTML = '<span style="color:var(--danger);">&#10007; ' + escapeHtml(e.message) + '</span>';
  }
}

// Platform must describe the Flask host (which will run the installer), not
// the browser device (which could be a phone or a different desktop on the
// LAN). /api/darktable/install/available carries the host's sys.platform;
// window._dtAsset caches it once we have looked, and installPlatform() falls
// back to the browser as a last resort so the panel is never blank before
// the first API round-trip.
