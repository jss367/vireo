// Starting and stopping a pipeline run.
// Classic page script; load boot.js after all definitions.

// -- Pipeline orchestration --
var _pipelineRunning = false;
var _pipelineAbort = false;
var _pipelineCancelling = false;
var _pipelineEventSource = null;
var _pipelineResolve = null;
var _currentPipelineJobId = null;

async function startPipeline() {
  if (_pipelineRunning) return;
  if (_dashboardActionScopeError) {
    updateStartButton();
    return;
  }

  // Belt-and-suspenders: updateStartButton() should already have disabled
  // this button when Classify is enabled and the plan is mid-refresh, but
  // a click can still beat the gate (race against debounce, programmatic
  // invocation, etc.). If a refresh is queued or in-flight, await it now
  // and bail if the fresh plan reports Classify is blocked — never POST
  // /api/jobs/pipeline only to crash at the missing-labels classify step.
  var classifyEnableCb = document.getElementById('enableClassify');
  var classifyOn = classifyEnableCb && classifyEnableCb.checked;
  if (classifyOn && (_pipelinePlan === null || _planRefreshPending)) {
    if (_planDebounceTimer) {
      clearTimeout(_planDebounceTimer);
      _planDebounceTimer = null;
    }
    await refreshPipelinePlan();
    // refreshPipelinePlan() already called updateStartButton(), which will
    // have written the "Classify needs a species list" message if blocked.
    if (_classifyBlocked()) return;
    // If we still don't have a plan (e.g. import preview is still loading
    // so refreshPipelinePlan returned without fetching), we genuinely can't
    // verify whether Classify will be blocked. Bail with a clear message
    // rather than gamble on a run that may crash mid-pipeline.
    if (_pipelinePlan === null) {
      var actionStatus = document.getElementById('pipelineActionStatus');
      if (actionStatus) {
        actionStatus.textContent =
          'Still computing the pipeline plan — try again in a moment.';
        actionStatus.className = 'status-msg';
      }
      return;
    }
  }

  _pipelineRunning = true;
  _pipelineAbort = false;
  _pipelineCancelling = false;
  _currentPipelineJobId = null;
  document.getElementById('modelWarningBanner').style.display = 'none';
  var _prevErrBanner = document.getElementById('pipelineErrorBanner');
  if (_prevErrBanner) _prevErrBanner.style.display = 'none';
  var _prevNoteBanner = document.getElementById('pipelineNoteBanner');
  if (_prevNoteBanner) _prevNoteBanner.style.display = 'none';

  var btn = document.getElementById('btnStartPipeline');
  btn.textContent = 'Stop Pipeline';
  btn.onclick = stopPipeline;
  btn.disabled = false;

  // Reset live pill state so "Done" / "Failed" from a previous run don't bleed
  // into this one. Pills will recompute from toggles + prior data.
  _runningStages = {};
  _stageOutcomes = {};
  refreshPipelineUI();

  // Gather config from the UI
  var body = {};

  if (_sourceMode === 'folders') {
    // Scope = selected workspace folders; the server expands each to
    // its active-workspace subtree and pins the run to an ad-hoc
    // collection (import/process split PR 1).
    body.folder_ids = selectedFolderIds();
  } else {
    body.collection_id = parseInt(document.getElementById('collectionPicker').value);
    // Add excluded photo IDs from preview selection
    if (_previewData && _previewSelected) {
      var excludedIds = [];
      _previewData.files.forEach(function(f) {
        if (!_previewSelected[f.path] && f.photo_id) excludedIds.push(f.photo_id);
      });
      if (excludedIds.length > 0) body.exclude_photo_ids = excludedIds;
    }
  }

  // The process page sends explicit stage flags: Run always uses the CURRENT
  // toggle values (whether or not a saved process is selected), including
  // miss_enabled and review_mode which used to be preset-only server-side.
  var classifyEnabled = document.getElementById('enableClassify').checked;
  var extractEnabled = document.getElementById('enableExtract').checked;
  var eyeKeypointsEnabled = document.getElementById('enableEyeKeypoints').checked;
  var groupEnabled = document.getElementById('enableGroup').checked;

  body.skip_classify = !classifyEnabled;
  body.raw_subject_analysis = !!document.getElementById('chkRawSubjectAnalysis').checked;
  body.skip_extract_masks = !extractEnabled;
  body.skip_eye_keypoints = !eyeKeypointsEnabled;
  body.skip_regroup = !groupEnabled;
  body.miss_enabled = !!document.getElementById('enableMisses').checked;
  body.review_mode =
    document.getElementById('enableSpeciesReview').checked ? 'species' : null;

  // Only send ``eye_detect_override`` when the checkbox is CHECKED — an
  // explicit per-run opt-in that forces ``pipeline_cfg["eye_detect_enabled"]``
  // on for the run. An unchecked box is not an explicit eye-scoring
  // opt-out: on a workspace with Settings ``eye_detect_enabled=true``, the
  // user may simply want to skip the (expensive) stage while still scoring
  // against existing ``eye_tenengrad``. Sending ``false`` here would
  // silently disable eye-based scoring on such workspaces and produce
  // different KEEP/REJECT results than the same server-side strategy /
  // API path would (which leaves the override ``None``). When unset, the
  // backend falls back to workspace Settings.
  if (eyeKeypointsEnabled) {
    body.eye_detect_override = true;
  }

  // Classification options (only if enabled)
  if (classifyEnabled) {
    var selectedModels = [];
    document.querySelectorAll('.model-checkbox:checked').forEach(function(cb) {
      selectedModels.push({id: cb.value, name: cb.dataset.name});
    });
    if (selectedModels.length > 0) {
      // Send every checked model. The backend loops over model_ids in the
      // classify stage; model_id stays for back-compat with older callers.
      body.model_ids = selectedModels.map(function(m) { return m.id; });
      body.model_id = selectedModels[0].id;
    }

    var labelsFiles = [];
    document.querySelectorAll('.labels-run-cb:checked').forEach(function(cb) {
      labelsFiles.push(cb.value);
    });
    if (labelsFiles.length > 0) body.labels_files = labelsFiles;

    body.reclassify = !!document.getElementById('chkReclassify').checked;
    body.download_taxonomy = !!document.getElementById('chkDownloadTaxonomy').checked;
  }

  // Reset all stage cards to pending state
  ['Source', 'Scan', 'Previews', 'Classify', 'Extract', 'Group'].forEach(function(name) {
    var numEl = document.getElementById('num' + name);
    if (numEl) numEl.className = 'stage-num';
  });

  try {
    var data = await safeFetch('/api/jobs/pipeline', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body),
    }, { toast: false });

    if (!data || !data.job_id) {
      document.getElementById('statusSource').textContent = (data && data.error) || 'Failed to start pipeline';
      return;
    }
    _currentPipelineJobId = data.job_id;
    if (_pipelineCancelling) {
      _pipelineCancelling = false;
      stopPipeline();
    }

    // Show model warning banner if returned
    if (data.model_warning) {
      var banner = document.getElementById('modelWarningBanner');
      banner.dataset.kind = 'model';
      document.getElementById('modelWarningText').textContent = data.model_warning;
      banner.style.display = '';
    }

    // Show progress on all cards
    document.getElementById('card-source').classList.add('expanded');

    await new Promise(function(resolve) {
      _pipelineResolve = resolve;
      _pipelineEventSource = safeEventSource('/api/jobs/' + data.job_id + '/stream', {
        onProgress: function(p) {
          _updatePipelineStageUI(p);
        },
        onComplete: function(result) {
          _pipelineResolve = null;
          _pipelineEventSource = null;
          window.dispatchEvent(new CustomEvent('vireo-job-done', {detail: {job_id: data.job_id}}));
          _onPipelineComplete(result);
          resolve();
        },
        onError: function() {
          _pipelineEventSource = null;
          if (_pipelineCancelling) {
            _waitForPipelineTerminal(data.job_id, resolve);
            return;
          }
          _pipelineResolve = null;
          document.getElementById('statusSource').textContent = 'Connection lost';
          resolve();
        }
      });
    });
  } catch(e) {
    document.getElementById('statusSource').textContent = 'Error: ' + e.message;
  } finally {
    _pipelineRunning = false;
    _pipelineCancelling = false;
    _pipelineAbort = false;
    _currentPipelineJobId = null;
    btn.textContent = 'Start Pipeline';
    btn.onclick = startPipeline;
    updateStartButton();
    // Resync the Start/Queue label and "N running . M queued" line
    // right away — the run we just finished freed a slot, but other
    // tabs/processes may still be running pipelines.
    refreshSlotInfo();
    // The pipeline just wrote new detections, masks, keypoints, etc. —
    // refresh the plan so the next run's pills reflect the new state of
    // the world (e.g. "Already done" for stages that just completed).
    refreshPipelinePlan();
  }
}

async function stopPipeline() {
  if (_pipelineCancelling) return;
  _pipelineAbort = true;
  _pipelineCancelling = true;
  var btn = document.getElementById('btnStartPipeline');
  btn.disabled = true;
  btn.textContent = 'Cancelling...';
  var actionStatus = document.getElementById('pipelineActionStatus');
  if (actionStatus) {
    actionStatus.textContent = 'Sending cancellation request...';
    actionStatus.className = 'status-msg';
  }
  if (!_currentPipelineJobId) {
    if (actionStatus) actionStatus.textContent = 'Waiting for pipeline job id before cancelling...';
    return;
  }
  try {
    var resp = await fetch('/api/jobs/' + encodeURIComponent(_currentPipelineJobId) + '/cancel', {
      method: 'POST',
    });
    if (!resp.ok) {
      var data = {};
      try { data = await resp.json(); } catch(e) {}
      throw new Error((data && data.error) || 'Unable to cancel pipeline');
    }
    btn.textContent = 'Stopping...';
    if (actionStatus) {
      actionStatus.textContent = 'Cancellation requested. Waiting for the running stage to stop...';
    }
  } catch(e) {
    _pipelineAbort = false;
    _pipelineCancelling = false;
    btn.disabled = false;
    btn.textContent = 'Stop Pipeline';
    if (actionStatus) {
      actionStatus.textContent = e.message || 'Unable to cancel pipeline';
      actionStatus.className = 'status-msg error';
    }
  }
}
