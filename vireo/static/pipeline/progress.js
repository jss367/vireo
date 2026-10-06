// Stage-to-card maps and live stage progress, ETA, notes, and errors.
// Classic page script; load boot.js after all definitions.

// Map pipeline stage names to UI card element suffixes. Stages with no
// card here (e.g. the import job's storage/archive stages) are ignored by
// _updatePipelineStageUI.
var _stageToCard = {
  ingest: 'Source',
  scan: 'Scan',
  thumbnails: 'Previews',
  previews: 'Previews',
  model_loader: 'Classify',
  classify: 'Classify',
  extract_masks: 'Extract',
  eye_keypoints: 'EyeKeypoints',
  regroup: 'Group',
};

// Backend stages that only appear in ``stages`` when they fail. They mark
// their card failed but are left out of _cardToStages, so a normal run
// (where the key is absent) doesn't wait on them to finish the card.
// ``collection`` builds the run's collection right after the scan.
var _failureOnlyStageToCard = {
  collection: 'Scan',
};

// Reverse of _stageToCard: cardSuffix -> [backend stage names]. Used by
// _updatePipelineStageUI to aggregate the per-card terminal state across
// multi-substage cards (e.g. Previews = thumbnails + previews) so the pill
// only flips to 'Done' once every substage has reached a terminal state.
var _cardToStages = (function() {
  var m = {};
  for (var s in _stageToCard) {
    var c = _stageToCard[s];
    (m[c] = m[c] || []).push(s);
  }
  return m;
})();

// Map pipeline stage names to the progress bar element suffixes
var _stageToProgress = {
  ingest: 'Source',
  scan: 'Scan',
  thumbnails: 'Previews',
  previews: 'Previews',
  // Detect runs as a distinct backend stage, but the UI has no separate
  // card for it — route detect progress through the Classify card so the
  // detect pre-pass doesn't look stalled (it can be the longest phase of
  // a run on large collections).
  detect: 'Classify',
  classify: 'Classify',
  extract_masks: 'Extract',
  eye_keypoints: 'EyeKeypoints',
  regroup: 'Group',
};

function _formatETA(seconds) {
  if (seconds < 60) return seconds + 's';
  if (seconds < 3600) return Math.round(seconds / 60) + 'm';
  var h = Math.floor(seconds / 3600);
  var m = Math.round((seconds % 3600) / 60);
  return h + 'h ' + m + 'm';
}

// Ingest is the only stage that ever reads the source (SD card/etc.) — every
// later stage works off the copy it made. Once it reports success, surface
// that explicitly instead of leaving the user to guess whether the card is
// still in use (`ingestInfo.copied` is absent entirely in scan-in-place mode,
// where there was never a copy and the card stays needed for the whole run).
function _showSourceEjectSafe(ingestInfo) {
  if (!ingestInfo || typeof ingestInfo.copied !== 'number') return;
  var statusSourceEl = document.getElementById('statusSource');
  if (!statusSourceEl) return;
  var parts = [ingestInfo.copied.toLocaleString() + ' copied'];
  if (ingestInfo.skipped_duplicate) {
    parts.push(ingestInfo.skipped_duplicate.toLocaleString() + ' already present');
  }
  statusSourceEl.textContent = parts.join(', ') + ' — the card is safe to eject.';
  statusSourceEl.className = 'status-msg ok';
}

function _updatePipelineStageUI(p) {
  var stages = p.stages || {};

  // Aggregate backend substages per card. Multiple backend stages share a
  // card (Previews ← thumbnails + previews; Classify ← model_loader +
  // classify), so the card's outcome can only be decided once ALL of its
  // substages reach a terminal state. Resolving each substage event in
  // isolation made the pill flash 'Done' as soon as the first substage
  // finished, even though more work was pending.
  var cardAgg = {};  // cardSuffix -> aggregate substage outcomes
  for (var stageName in stages) {
    var cardSuffix = _stageToCard[stageName];
    if (!cardSuffix) {
      var failureCard = _failureOnlyStageToCard[stageName];
      if (failureCard && stages[stageName].status === 'failed') {
        (cardAgg[failureCard] || (cardAgg[failureCard] = {
          running: false, failed: false, warning: false,
          completed: 0, skipped: 0, present: 0,
        })).failed = true;
      }
      continue;
    }
    var agg = cardAgg[cardSuffix] || (cardAgg[cardSuffix] = {
      running: false, failed: false, warning: false,
      completed: 0, skipped: 0, present: 0,
    });
    agg.present += 1;
    var st = stages[stageName].status;
    if (st === 'running') agg.running = true;
    else if (st === 'failed') agg.failed = true;
    else if (st === 'completed') {
      agg.completed += 1;
      if ((stages[stageName].error_count || 0) > 0) agg.warning = true;
    }
    else if (st === 'skipped') agg.skipped += 1;
  }

  for (var cardSuffix2 in cardAgg) {
    var a = cardAgg[cardSuffix2];
    var expected = (_cardToStages[cardSuffix2] || []).length;
    var numEl = document.getElementById('num' + cardSuffix2);
    if (!numEl) continue;

    if (a.running) {
      numEl.className = 'stage-num running';
      _runningStages[cardSuffix2] = true;
      _setPill(cardSuffix2, 'running');
      // Override pill with the live X/Y count for the card's running
      // substage. The event top-level p.current/p.total is the WEIGHTED
      // OVERALL pipeline progress (see note around line 2586) and would
      // make every concurrently-running card show the same number, so
      // counts are read straight from stages[stageName]. Cards whose
      // running substage hasn't reported a total yet (spin-up phases,
      // Group's terminal step) keep the static "Running…" label rather
      // than showing a misleading "Running… 0 / 0".
      var subStages = _cardToStages[cardSuffix2] || [];
      var stageDone = 0, stageTot = 0;
      for (var i = 0; i < subStages.length; i++) {
        var info = stages[subStages[i]] || {};
        if (info.status !== 'running') continue;
        var t = info.total || 0;
        if (t > stageTot) {
          stageTot = t;
          stageDone = (info.count || 0) + (info.cached || 0);
        }
      }
      if (stageTot > 0) {
        _setPill(cardSuffix2, 'running',
                 'Running… ' + stageDone + ' / ' + stageTot);
      }
      var card = document.getElementById('card-' + cardSuffix2.toLowerCase());
      if (card) card.classList.add('expanded');
    } else if (a.failed) {
      // Must clear the running flag — _stageStateFor checks _runningStages
      // before _stageOutcomes, so a leftover running entry would mask the
      // failure and leave the pill stuck on "Running…".
      numEl.className = 'stage-num';
      delete _runningStages[cardSuffix2];
      _stageOutcomes[cardSuffix2] = 'failed';
      _setPill(cardSuffix2, 'failed');
    } else if (a.completed + a.skipped >= expected && a.completed + a.skipped > 0) {
      // All expected substages reached a non-running terminal state.
      numEl.className = 'stage-num complete';
      delete _runningStages[cardSuffix2];
      if (a.completed > 0) {
        _stageOutcomes[cardSuffix2] = a.warning ? 'warning' : 'done';
        _setPill(cardSuffix2, a.warning ? 'warning' : 'done');
        if (cardSuffix2 === 'Source') _showSourceEjectSafe(stages['ingest']);
      } else {
        // Every substage was skipped. Backend 'skipped' covers both
        // user-disabled stages and auto-skip; pick the right label so a
        // user-disabled stage doesn't get mislabeled "Already done".
        var skipState = _isUserDisabledStage(cardSuffix2) ? 'will-skip' : 'done-prior';
        _stageOutcomes[cardSuffix2] = skipState;
        _setPill(cardSuffix2, skipState);
      }
    }
    // Else: card is mid-pipeline (some substages still pending). Don't
    // write an outcome — leave the pill at 'running' (set above when an
    // earlier event hit the running branch) or its pre-run state.
  }

  // Route the progress event to the correct card via p.stage_id (set
  // by the backend). Stages can run concurrently (scan + thumbnails),
  // so explicit routing is required to keep their bars separate.
  // Events without stage_id are stage-status broadcasts from
  // _update_stages and should not clobber card-specific phase labels.
  if (!p.stage_id) return;
  var progressSuffix = _stageToProgress[p.stage_id];
  if (!progressSuffix) return;

  var progressWrap = document.getElementById('progress' + progressSuffix);
  var fill = document.getElementById('fill' + progressSuffix);
  var text = document.getElementById('text' + progressSuffix);
  var status = document.getElementById('status' + progressSuffix);

  // Per-stage progress lives in stages[stage_id]; event top-level
  // current/total is the WEIGHTED OVERALL progress and must not drive
  // individual cards (it would make every card read the same %).
  var si = stages[p.stage_id] || {};
  var stageCurrent = si.count || 0;
  var stageCached = si.cached || 0;
  var stageSeen = si.seen || 0;
  var stageCachedEst = si.cached_estimate || 0;
  var stageTotal = si.total || 0;

  var parts = [];
  if (p.phase) parts.push(p.phase);

  // Count line: split when we have actually seen cache hits, otherwise
  // fall back to single-number display so non-classify stages render
  // unchanged.
  if (stageTotal > 0 && (stageCurrent > 0 || stageCached > 0)) {
    if (stageCached > 0) {
      parts.push(
        stageCurrent.toLocaleString() + ' inferred · ' +
        stageCached.toLocaleString() + ' cached / ' +
        stageTotal.toLocaleString()
      );
    } else {
      parts.push(stageCurrent.toLocaleString() + ' / ' + stageTotal.toLocaleString());
    }
  }

  // Pre-flight banner: shown once when the stage starts and we have an
  // estimate but no actual progress yet.
  if (stageCachedEst > 0 && stageCurrent === 0 && stageCached === 0 && stageSeen === 0) {
    parts.push(
      '~' + stageCachedEst.toLocaleString() + ' cached, ~' +
      Math.max(0, stageTotal - stageCachedEst).toLocaleString() + ' to classify'
    );
  }

  if (p.rate) parts.push(Math.round(p.rate) + ' files/min');
  if (p.eta_seconds != null && p.eta_seconds > 0) {
    parts.push('~' + _formatETA(p.eta_seconds) + ' remaining');
  } else if (stageTotal > 0 && stageCurrent > 0 && stageCurrent < 10) {
    parts.push('Estimating...');
  }
  if (p.current_file) parts.push(p.current_file);
  var label = parts.join(' — ');

  // Always update the status line so indeterminate phases like
  // "Discovering files..." are visible even before the bar has a total.
  if (status) status.textContent = label;

  if (stageTotal > 0) {
    if (progressWrap) progressWrap.style.display = '';
    // Bar fill: classify surfaces ``seen`` which counts every photo the
    // loop iterated past (cached, inferred, no-detection, decode-failed,
    // inference-failed); use it so the bar reaches 100% on collections that
    // contain non-classifiable photos. Other stages fall back to
    // count + cached.
    var stageProcessed = stageSeen || (stageCurrent + stageCached);
    var pct = Math.min(100, Math.round(stageProcessed / stageTotal * 100));
    if (fill) fill.style.width = pct + '%';
    if (text) text.textContent = label;
  }
}

// Strip the internal "[stage] Fatal:" prefix so the banner reads as a plain,
// actionable sentence (the stage is already conveyed by the failed card pill).
function _humanizePipelineError(msg) {
  return String(msg).replace(/^\[\w+\]\s*/, '').replace(/^Fatal:\s*/i, '');
}

// Show why stages of a run that did not fail were skipped. Warning-styled,
// not the red failure banner: nothing failed, but the user should know why
// a stage did nothing (e.g. every detection below detector_confidence).
function _showPipelineNotes(messages, offerReview) {
  var banner = document.getElementById('pipelineNoteBanner');
  var text = document.getElementById('pipelineNoteText');
  if (!banner || !text) return;
  text.textContent = messages.join('\n');
  var link = document.getElementById('pipelineNoteReviewLink');
  if (link) link.style.display = offerReview ? 'inline-block' : 'none';
  banner.style.display = '';
}

// Show a prominent, dismissible error banner with the full failure message(s).
function _showPipelineError(messages) {
  var banner = document.getElementById('pipelineErrorBanner');
  var text = document.getElementById('pipelineErrorText');
  if (!banner || !text) return;
  text.textContent = messages.join('\n');
  banner.style.display = '';
  banner.scrollIntoView({behavior: 'smooth', block: 'nearest'});
}
