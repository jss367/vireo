// The Start button gate and stage toggles.
// Classic page script; load boot.js after all definitions.

function updateStartButton() {
  var btn = document.getElementById('btnStartPipeline');
  var actionStatus = document.getElementById('pipelineActionStatus');
  // While *this tab* owns a live pipeline run, the same button has
  // been repurposed as "Stop Pipeline" (see startPipeline). Don't
  // clobber its label/handler; just gate the disabled state on the
  // cancel-in-flight flag.
  //
  // Step 5 of the pipeline-concurrency rollout removed the broader
  // "_pipelineRunning disables Start" guard: a click while *any*
  // pipeline (this tab's or another's) is running now enqueues a new
  // run instead of being blocked. The label flips between
  // "Start Pipeline" and "Queue Pipeline" based on slot occupancy
  // polled from /api/pipeline/slots (see refreshSlotInfo).
  if (_pipelineRunning) {
    if (actionStatus) {
      actionStatus.textContent = _pipelineCancelling
        ? 'Cancelling the running pipeline.'
        : '';
      actionStatus.className = 'status-msg';
    }
    btn.disabled = !!_pipelineCancelling;
    return;
  }
  if (actionStatus) {
    actionStatus.textContent = '';
    actionStatus.className = 'status-msg';
  }

  if (_dashboardActionScopeError) {
    btn.disabled = true;
    if (actionStatus) {
      actionStatus.textContent = _dashboardActionScopeError;
      actionStatus.className = 'status-msg error';
    }
    return;
  }

  btn.disabled = !hasPipelineSource();
  if (btn.disabled && actionStatus) {
    actionStatus.textContent = 'Select workspace folders or a collection to start.';
  }

  // Snapshot whether the source is configured *before* the classify gates
  // below. The stage-1 "complete" indicator keys off this; if we let the
  // classify gate (which can flip while a plan refresh is pending) drive
  // the source indicator, the source stage flickers between complete and
  // incomplete every time the plan refetches.
  var sourceReady = !btn.disabled;

  // Classify is blocked on missing species labels (the selected model can't
  // run label-free). Gate Start so the user can't launch a run that would
  // crash mid-pipeline at the classify stage — point them at Settings ›
  // Labels, or let them turn Classify off to run the other stages. Mirrors
  // the plan endpoint's `blocked` state (pipeline_plan._classify_plan).
  //
  // If a plan refresh is queued or in-flight and Classify is enabled, we
  // can't trust the blocked check yet: _stageStateFor falls back to
  // "will-run" when _pipelinePlan is null, and a stale plan from before the
  // user's latest model/label change might still say "will-run". Treat
  // "Classify on, plan not current" as not ready and disable Start until
  // the fresh response lands — otherwise a fast click during the debounce
  // window posts /api/jobs/pipeline and hits the missing-labels failure.
  if (!btn.disabled) {
    var classifyEnableCb = document.getElementById('enableClassify');
    var classifyOn = classifyEnableCb && classifyEnableCb.checked;
    if (classifyOn && (_pipelinePlan === null || _planRefreshPending)) {
      btn.disabled = true;
      if (actionStatus) {
        actionStatus.textContent = 'Checking pipeline plan…';
        actionStatus.className = 'status-msg';
      }
    } else if (_classifyBlocked()) {
      btn.disabled = true;
      if (actionStatus) {
        actionStatus.textContent =
          'Classify needs a species list — download one in Settings › Labels, '
          + 'or turn Classify off to run the other stages.';
        actionStatus.className = 'status-msg error';
      }
    }
  }

  // Update stage 1 indicator when the source is ready (sourceReady was
  // captured above, before the classify gates).
  var numSource = document.getElementById('numSource');
  if (sourceReady) {
    numSource.classList.add('complete');
  } else {
    numSource.className = 'stage-num';
  }
}

// -- Stage enable/disable dependencies --
// Extract depends on Classify, and Eye Keypoints depends on Extract (needs
// masks). Group is independent because it can regroup from cached features.
function onStageToggle(stage) {
  var chain = ['classify', 'extract'];
  var idx = chain.indexOf(stage);

  if (idx >= 0) {
    var cbId = 'enable' + stage.charAt(0).toUpperCase() + stage.slice(1);
    var checked = document.getElementById(cbId).checked;
    if (!checked) {
      // Uncheck and disable all downstream
      for (var i = idx + 1; i < chain.length; i++) {
        var downId = 'enable' + chain[i].charAt(0).toUpperCase() + chain[i].slice(1);
        var cb = document.getElementById(downId);
        cb.checked = false;
        cb.disabled = true;
      }
    } else if (idx + 1 < chain.length) {
      // Re-enable the immediate downstream checkbox
      var nextId = 'enable' + chain[idx + 1].charAt(0).toUpperCase() + chain[idx + 1].slice(1);
      document.getElementById(nextId).disabled = false;
    }
  }

  // Sync eye keypoints to extract: if extract is off, eye keypoints can't
  // run (no masks); when extract comes back on, leave eye keypoints
  // unchecked so the user must opt in again.
  var extCb = document.getElementById('enableExtract');
  var ekCb = document.getElementById('enableEyeKeypoints');
  if (extCb && ekCb && stage !== 'eyekeypoints') {
    if (!extCb.checked) {
      ekCb.checked = false;
      ekCb.disabled = true;
    } else {
      ekCb.disabled = false;
    }
  }

  updateDerivedProcessControls();
  if (!_applyingProcess) markProcessModified();
  updateStartButton();
  refreshPipelineUI();
  schedulePlanRefresh();
}
