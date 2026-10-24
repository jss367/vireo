// Card toggles, pipeline state, the run plan, status pills, and card states.
// Classic page script; load boot.js after all definitions.

// -- Shared --

/** @param {string} name */
function toggleCard(name) {
  var card = /** @type {HTMLElement} */ (document.getElementById('card-' + name));
  if (card) card.classList.toggle('expanded');
}

// Mutable pipeline state — updated after each job completes
var _pipelineState = {
};

// --- Pipeline plan & status pills ---
//
// Stages 3-8 each show an explicit text pill (Will run / Will skip / Already
// done / Running / Done / Failed) instead of relying on the colored circle.
// `_runningStages` and `_stageOutcomes` are live overrides: while a
// pipeline is running, they take precedence over plan-derived state.
//
// `_runningStages` is per-stage (not a single global) because the pipeline
// allows concurrent stages — e.g. scan runs alongside thumbnails/previews —
// and a single global would let one stage's status update clobber another's
// "Running…" pill on the next refresh.
/** @type {Record<string,boolean>} */ var _runningStages = {};  // suffix -> true while that stage is running
/** @type {Record<string,string>} */ var _stageOutcomes = {};  // suffix -> 'done' | 'failed' | 'cancelled' | 'skipped' | 'will-skip' | 'done-prior'

// Plan response from POST /api/pipeline/plan, keyed by stage suffix:
//   { Classify: { state, summary, detail }, Extract: ..., ... }
// Truth source for the user-visible pill state and per-stage summary text
// when no run is active. Computed against current UI selections so the
// page never tells the user a stage is done when, given new models or
// labels, the next run actually has work to do (CORE_PHILOSOPHY: "No
// black boxes" — pills must answer the question users read them as).
/** @type {PipelinePlan | null} */ var _pipelinePlan = null;
var _planFetchSeq = 0;
/** @type {ReturnType<typeof setTimeout> | null} */ var _planDebounceTimer = null;
// True while a /api/pipeline/plan refresh is queued (debounce timer set) or
// the latest fetch is in-flight. Used by updateStartButton() so a fast click
// during the debounce window — when _pipelinePlan is null or stale relative
// to the user's most recent selection — can't enable Start and let the
// missing-labels classify run slip through the blocked gate.
var _planRefreshPending = false;

/** @type {PipelineStage[]} */ var _STAGES = [
  { suffix: 'Scan',         label: 'Scan & Index',           enable: null },
  { suffix: 'Previews',     label: 'Thumbnails & Previews',  enable: null },
  { suffix: 'Classify',     label: 'Classify',               enable: 'enableClassify' },
  { suffix: 'Extract',      label: 'Extract Features',       enable: 'enableExtract' },
  { suffix: 'EyeKeypoints', label: 'Eye Keypoints',          enable: 'enableEyeKeypoints' },
  { suffix: 'Group',        label: 'Group & Score',          enable: 'enableGroup' },
];

// Lookup: cardSuffix -> enable-checkbox id (null for stages with no toggle).
var _STAGE_ENABLE_BY_SUFFIX = (function() {
  /** @type {Record<string, string | null>} */ var m = {};
  _STAGES.forEach(function(s) { m[s.suffix] = s.enable; });
  return m;
})();

// Did the user disable this stage via its enable checkbox? Used to
// distinguish backend `skipped` due to a user toggle ("Will skip") from
// `skipped` due to prior outputs already existing ("Already done").
/** @param {string} cardSuffix */
function _isUserDisabledStage(cardSuffix) {
  var enableId = _STAGE_ENABLE_BY_SUFFIX[cardSuffix];
  if (!enableId) return false;
  var cb = /** @type {HTMLInputElement} */ (document.getElementById(enableId));
  return !!(cb && !cb.checked);
}

/** @type {Record<string,string>} */ var _PILL_LABELS = {
  'running':    'Running…',
  'done':       'Done',
  'failed':     'Failed',
  'cancelled':  'Cancelled',
  'skipped':    'Skipped',
  'will-run':   'Will run',
  'will-skip':  'Will skip',
  'done-prior': 'Already done',
  'blocked':    'Needs labels',
};

// Map a stage's plan entry to the pill text, including counts and the
// staleness signal. Returns the literal string to display.
//
// The plan endpoint emits `state: 'will-run'` for both fresh and
// settings-changed cases; the distinction lives in
// `detail.fingerprint_outdated` / `detail.fingerprint_invalidated`.
// Classify gets a more specific label when only the BioCLIP label set changed.
/** @param {PipelineStage | undefined} stage @param {PipelinePlanEntry | null | undefined} planEntry @param {string} state */
function _formatPillLabel(stage, planEntry, state) {
  // Special-cased states (running/done/failed/will-skip) have static
  // labels — counts during a run come from a separate channel.
  if (state === 'running')   return 'Running…';
  if (state === 'done')      return 'Done';
  if (state === 'warning')   return 'Completed with warnings';
  if (state === 'failed')    return 'Failed';
  if (state === 'cancelled') return 'Cancelled';
  if (state === 'skipped')   return 'Skipped';
  if (state === 'will-skip') return 'Will skip';
  if (state === 'blocked')   return _classifyLabelReadError(planEntry) ? "Can't read labels" : 'Needs labels';
  if (state === 'needs-source') return 'Select a source';
  if (state === 'checking-plan') return 'Checking plan…';

  if (!planEntry) {
    // Live stages and stages without a plan entry use the bare state label.
    return state === 'done-prior' ? 'Already done' : 'Will run';
  }
  var detail   = planEntry.detail || {};
  var pending  = (typeof detail.pending  === 'number') ? detail.pending  : null;
  var eligible = (typeof detail.eligible === 'number') ? detail.eligible : null;
  var outdated = !!(detail.fingerprint_outdated || detail.fingerprint_invalidated);
  var labelSetChanged = outdated
    && stage
    && stage.suffix === 'Classify'
    && detail.fingerprint_reason === 'label_set_changed';

  // No quantifiable work — fall back to the state-only label, but still
  // surface the outdated flag for stages that don't have a per-photo
  // unit (e.g., Group emits fingerprint_outdated with no eligible count).
  if (eligible === null || eligible === 0) {
    if (state === 'will-run' && labelSetChanged) return 'Different label set';
    if (state === 'will-run' && outdated) return 'Outdated';
    // All-duplicates import: the stage executes but processes nothing.
    // A bare "Will run" pill reads as "work will happen" — put the zero
    // on the pill itself so the user doesn't have to open the card.
    if (state === 'will-run' && detail.import_no_new) return 'Will run (0 photos)';
    return state === 'done-prior' ? 'Already done' : 'Will run';
  }

  if (state === 'done-prior') {
    return 'Already done (' + eligible + ')';
  }

  // state === 'will-run' from here on.
  if (labelSetChanged)           return 'Different label set (' + (pending || eligible) + ' to classify)';
  if (outdated)                  return 'Outdated (' + (pending || eligible) + ' to redo)';
  if (pending !== null && pending > 0 && pending < eligible) {
    return 'Resume (' + pending + ' left)';
  }
  return 'Will run (' + eligible + ')';
}

/** @param {string} suffix @param {string} state @param {string | null} [text] @param {PipelineStage} [stage] */
function _setPill(suffix, state, text, stage) {
  var pill = /** @type {HTMLElement} */ (document.getElementById('pill' + suffix));
  if (!pill) return;
  pill.className = 'stage-status-pill visible ' + state;
  if (text != null) {
    pill.textContent = text;
    return;
  }
  var planEntry = _pipelinePlan && _pipelinePlan.stages
    ? _pipelinePlan.stages[suffix] : null;
  pill.textContent = _formatPillLabel(stage, planEntry, state);
}

// Map each stage suffix to the .stage-summary span it owns. The span shows
// the plan summary text before/between runs and switches to live counts
// during a run via the existing txtDetections/txtMasks ids.
/** @type {Record<string,string>} */ var _STAGE_SUMMARY_IDS = {
  'Previews':     'summaryPreviews',
  'Classify':     'txtDetections',
  'Extract':      'txtMasks',
  'EyeKeypoints': 'txtEyeKeypoints',
  'Group':        'txtResults',
};

/** @param {string} suffix @param {string} text */
function _setStageSummaryText(suffix, text) {
  var id = _STAGE_SUMMARY_IDS[suffix];
  if (!id) return;
  var el = /** @type {HTMLElement} */ (document.getElementById(id));
  if (el) el.textContent = text || '';
}

/** @param {PipelineStage} stage */
function _stageStateFor(stage) {
  if (_runningStages[stage.suffix]) return 'running';
  if (_stageOutcomes[stage.suffix]) return _stageOutcomes[stage.suffix] || 'will-run';
  if (!_pipelineRunning && !hasPipelineSource()) return 'needs-source';

  var fromPlan = _pipelinePlan && _pipelinePlan.stages
    ? _pipelinePlan.stages[stage.suffix] : null;

  // Plan wins on 'will-run': /api/pipeline/plan expands ``strategy`` the
  // same way /api/jobs/pipeline does, so when it says a stage will run,
  // the actual job WILL run it — even if the stage's checkbox is off.
  // The identify preset auto-unchecks Group but its species-review
  // branch still runs regroup_stage and rewrites the review cache;
  // without this the pill lies about what pressing Start would do.
  if (fromPlan && fromPlan.state === 'will-run') return 'will-run';

  // Otherwise user-toggled skip overrides the plan — matches the user's
  // mental model that toggling a stage off is the strongest signal of
  // "do not run this".
  if (stage.enable) {
    var cb = /** @type {HTMLInputElement} */ (document.getElementById(stage.enable));
    if (cb && !cb.checked) return 'will-skip';
  }
  // Wait for a selected source's plan before promising work. Once a plan
  // exists, stages without an entry (such as Scan) always run.
  if (!_pipelinePlan) return _pipelineRunning ? 'will-run' : 'checking-plan';
  if (!fromPlan) return 'will-run';
  return fromPlan.state || 'will-run';
}

// The plan's account of a species list it could not read (blocking Classify),
// or '' when Classify is blocked for another reason or not at all.
/** @param {PipelinePlanEntry | null | undefined} planEntry */
function _classifyLabelReadError(planEntry) {
  return (planEntry && planEntry.detail && planEntry.detail.label_read_error) || '';
}

// Why Start is disabled while Classify is blocked. A selected list that
// could not be read is not a missing list, so it must not say "download one".
function _classifyBlockedMessage() {
  var entry = _pipelinePlan && _pipelinePlan.stages ? _pipelinePlan.stages.Classify : null;
  var readError = _classifyLabelReadError(entry);
  if (readError) {
    return readError + '. Fix the file or pick another list in Settings › Labels, '
      + 'or turn Classify off to run the other stages.';
  }
  return 'Classify needs a species list — download one in Settings › Labels, '
    + 'or turn Classify off to run the other stages.';
}

// True when the Classify stage is enabled but the plan reports it can't run
// because the selected model(s) have no labels and can't classify label-free.
// A user-disabled Classify stage resolves to 'will-skip' via _stageStateFor,
// so this correctly returns false and does not gate Start in that case.
function _classifyBlocked() {
  for (var i = 0; i < _STAGES.length; i++) {
    if (_STAGES[i]?.suffix === 'Classify') {
      return _stageStateFor(/** @type {PipelineStage} */ (_STAGES[i])) === 'blocked';
    }
  }
  return false;
}

/** @param {PipelineStage} stage */
function _stageSummaryFor(stage) {
  if (!_pipelinePlan) return '';
  var fromPlan = _pipelinePlan.stages && _pipelinePlan.stages[stage.suffix];
  return (fromPlan && fromPlan.summary) || '';
}

function _renderPlanSummary() {
  var box = /** @type {HTMLElement} */ (document.getElementById('pipelinePlanSummary'));
  if (!box) return;
  if (!_pipelineRunning && !hasPipelineSource()
      && !Object.keys(_runningStages).length && !Object.keys(_stageOutcomes).length) {
    box.innerHTML = '<div class="plan-loading">Select a source to see the pipeline plan.</div>';
    return;
  }
  // Per-stage rows: stage name, pill label, summary text. Mirrors the
  // pills inside each card so the user sees the same answer in two places
  // and can scan the whole plan at a glance before pressing Start.
  /** @type {string[]} */ var rows = [];
  _STAGES.forEach(function(stage) {
    if (!_STAGE_SUMMARY_IDS[stage.suffix]) return;  // Scan
    var state = _stageStateFor(stage);
    var summary = _runningStages[stage.suffix] || _stageOutcomes[stage.suffix]
      ? ''  // live state takes the .stage-summary span; don't double up here
      : _stageSummaryFor(stage);
    var planEntry = _pipelinePlan && _pipelinePlan.stages
      ? _pipelinePlan.stages[stage.suffix] : null;
    var pillLabel = _formatPillLabel(stage, planEntry, state);
    rows.push(
      '<div class="plan-stage-row ' + state + '">' +
        '<span class="plan-stage-name">' + stage.label + '</span>' +
        '<span class="plan-stage-pill">' + pillLabel + '</span>' +
        '<span class="plan-stage-summary">' + escapeHtml(summary || '') + '</span>' +
      '</div>'
    );
  });
  if (!_pipelinePlan && !Object.keys(_runningStages).length && !Object.keys(_stageOutcomes).length) {
    box.innerHTML = '<div class="plan-loading">Loading plan…</div>';
    return;
  }
  box.innerHTML = rows.join('');
}

// Stages that don't have a per-photo work unit don't get a progress bar.
// Group is workspace-level; Scan/Previews don't have stage cards.
/** @type {Record<string,number>} */ var _STAGES_WITH_BAR = { 'Classify': 1, 'Extract': 1, 'EyeKeypoints': 1 };

/** @param {PipelineStage} stage @param {PipelinePlanEntry | null | undefined} planEntry */
function _renderProgressBar(stage, planEntry) {
  var bar = /** @type {HTMLElement} */ (document.getElementById('progressBar' + stage.suffix));
  if (!bar) return;
  if (!_STAGES_WITH_BAR[stage.suffix]) {
    bar.classList.add('hidden');
    return;
  }
  var detail   = (planEntry && planEntry.detail) || {};
  var pending  = (typeof detail.pending  === 'number') ? detail.pending  : null;
  var eligible = (typeof detail.eligible === 'number') ? detail.eligible : null;
  if (eligible === null || eligible === 0) {
    bar.classList.add('hidden');
    return;
  }
  bar.classList.remove('hidden');
  var outdated = !!(detail.fingerprint_outdated || detail.fingerprint_invalidated);
  bar.classList.toggle('outdated', outdated);
  var done = Math.max(0, eligible - (pending || 0));
  var pct = (done / eligible) * 100;
  var fill = /** @type {HTMLElement} */ (bar.querySelector('.stage-progress-bar-fill'));
  if (fill) fill.style.width = pct + '%';
}

function refreshPipelineUI() {
  _STAGES.forEach(function(stage) {
    var state = _stageStateFor(stage);
    _setPill(stage.suffix, state, null, stage);
    var planEntry = _pipelinePlan && _pipelinePlan.stages
      ? _pipelinePlan.stages[stage.suffix] : null;
    _renderProgressBar(stage, planEntry);
    if (!_runningStages[stage.suffix] && !_stageOutcomes[stage.suffix]) {
      _setStageSummaryText(stage.suffix, _stageSummaryFor(stage));
    }
  });
  _renderPlanSummary();
}

// Build the body for /api/pipeline/plan from the current DOM. Mirrors the
// subset of /api/jobs/pipeline the plan needs to make accurate decisions.
function _buildPlanBody() {
  /** @type {PipelineRequest} */ var body = {};
  // Source mode determines how the backend scopes the plan:
  //   - 'folders': scope = selected workspace folder subtrees.
  //   - 'collection': scope = collection's photo ids (real per-photo status).
  if (_sourceMode === 'folders') {
    var folderIds = selectedFolderIds();
    if (folderIds.length) body.folder_ids = folderIds;
  } else if (_sourceMode === 'collection') {
    var picker = /** @type {HTMLSelectElement} */ (document.getElementById('collectionPicker'));
    var cid = picker && picker.value ? parseInt(picker.value, 10) : NaN;
    if (!isNaN(cid)) body.collection_id = cid;
    if (_previewData && _previewSelected) {
      /** @type {number[]} */ var excludedIds = [];
      _previewData.files.forEach(function(f) {
        if (!_previewSelected[f.path] && f.photo_id) excludedIds.push(f.photo_id);
      });
      if (excludedIds.length) body.exclude_photo_ids = excludedIds;
    }
  }

  body.skip_classify       = !/** @type {HTMLInputElement} */ (document.getElementById('enableClassify')).checked;
  body.raw_subject_analysis = !!/** @type {HTMLInputElement} */ (document.getElementById('chkRawSubjectAnalysis')).checked;
  body.skip_extract_masks  = !/** @type {HTMLInputElement} */ (document.getElementById('enableExtract')).checked;
  body.skip_eye_keypoints  = !/** @type {HTMLInputElement} */ (document.getElementById('enableEyeKeypoints')).checked;
  body.skip_regroup        = !/** @type {HTMLInputElement} */ (document.getElementById('enableGroup')).checked;

  // Send review_mode so the plan describes the species-review path correctly:
  // with skip_regroup=true + review_mode="species" the run prepares species
  // review results, and without this the plan would report Group as
  // "Disabled" and imply no grouping work at all.
  body.review_mode =
    /** @type {HTMLInputElement} */ (document.getElementById('enableSpeciesReview')).checked ? 'species' : null;

  // Only send ``eye_detect_override`` when the checkbox is CHECKED. An
  // unchecked box means "skip the eye stage" and does NOT imply "disable eye
  // scoring" — on a workspace with Settings ``eye_detect_enabled=true`` the
  // plan must still reflect Settings for regroup, otherwise the plan would
  // show a different outcome (e.g. no ``eye_override_differs``) than the
  // workspace would produce via any non-Process-page path. Sending ``false``
  // here caused the plan to silently disable eye scoring on such workspaces.
  if (/** @type {HTMLInputElement} */ (document.getElementById('enableEyeKeypoints')).checked) {
    body.eye_detect_override = true;
  }

  // Preview size is a workspace/library setting. Omit preview_max_size so
  // the backend planner and job both use the same workspace-effective value.

  if (!body.skip_classify) {
    /** @type {string[]} */ var modelIds = [];
    /** @type {NodeListOf<HTMLInputElement>} */ (document.querySelectorAll('.model-checkbox:checked')).forEach(function(cb) {
      modelIds.push(cb.value);
    });
    if (modelIds.length) body.model_ids = modelIds;

    /** @type {string[]} */ var labelsFiles = [];
    /** @type {NodeListOf<HTMLInputElement>} */ (document.querySelectorAll('.labels-run-cb:checked')).forEach(function(cb) {
      labelsFiles.push(cb.value);
    });
    if (labelsFiles.length) body.labels_files = labelsFiles;

    var rc = /** @type {HTMLInputElement} */ (document.getElementById('chkReclassify'));
    body.reclassify = !!(rc && rc.checked);
  }
  return body;
}

// Invalidate in-flight responses immediately, including during the debounce
// window. An empty source must never request the API's whole-workspace plan.
function clearPipelinePlan() {
  if (_planDebounceTimer) clearTimeout(_planDebounceTimer);
  _planDebounceTimer = null;
  _planFetchSeq++;
  _pipelinePlan = null;
  _planRefreshPending = false;
  updateSamVariantWarning(null);
}

async function refreshPipelinePlan() {
  // Don't fetch a plan while a pipeline is running — live state is
  // authoritative, and the running stages would race the plan response.
  if (_pipelineRunning) return;
  if (!hasPipelineSource()) {
    clearPipelinePlan();
    refreshPipelineUI();
    updateStartButton();
    return;
  }
  _planRefreshPending = true;
  var seq = ++_planFetchSeq;
  try {
    var data = await safeFetch('/api/pipeline/plan', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(_buildPlanBody()),
    }, { toast: false });
    // Drop responses that arrived after a newer request — otherwise a slow
    // request can overwrite the result of a fresher one and the UI flickers
    // back to stale state right after the user changed a setting.
    // Leave _planRefreshPending alone: the newer fetch still owns it and
    // will clear it on its own completion.
    if (seq !== _planFetchSeq) return;
    _pipelinePlan = data;
    var extract = data && data.stages ? data.stages.Extract : null;
    var warning = extract && extract.detail
      ? extract.detail.sam_variant_warning : null;
    updateSamVariantWarning(warning || null);
  } catch(e) {
    if (seq !== _planFetchSeq) return;
    _pipelinePlan = null;
    updateSamVariantWarning(null);
  }
  // Only the latest fetch clears the pending flag (older stale responses
  // returned above without touching it).
  _planRefreshPending = false;
  refreshPipelineUI();
  // The plan can flip Classify to 'blocked' (missing labels), which gates
  // Start — recompute the button now that _pipelinePlan has been updated.
  updateStartButton();
}

// Debounced version for chatty controls (label checkboxes, model picker).
function schedulePlanRefresh() {
  if (_pipelineRunning) return;
  clearPipelinePlan();
  if (hasPipelineSource()) {
    _planRefreshPending = true;
    _planDebounceTimer = setTimeout(function() {
      _planDebounceTimer = null;
      refreshPipelinePlan();
    }, 150);
  }
  // Reflect the pending state on Start immediately — the control change
  // that triggered this may have invalidated the previous classify
  // blocked/will-run answer.
  refreshPipelineUI();
  updateStartButton();
}

function updateCardStates() {
  // Refresh pills + plan.
  updateStartButton();
  refreshPipelineUI();
}
