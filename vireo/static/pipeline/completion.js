// Terminal results: step outcomes, failure errors, and the completion handoff.
// Classic page script; load boot.js after all definitions.

// Per-photo outcomes for the Extract card, in the order the user cares
// about. `masked`/`skipped`/`unreadable`/`failed`/`total` are what both the
// pipeline stage and /api/jobs/extract-masks actually emit — this card used
// to read `masks_created`/`scored_count`, names no backend has ever sent, so
// its summary rendered empty on every run.
function _extractMasksSummary(ext) {
  if (!ext) return '';
  var parts = [];
  if (ext.masked) parts.push(ext.masked + ' masked');
  if (ext.skipped) parts.push(ext.skipped + ' skipped');
  // Distinct from "skipped": a skip means SAM looked and found no subject,
  // unreadable means the source file never opened (dropped share, corrupt
  // file). Both leave the photo unmasked, but only one is the user's to fix.
  if (ext.unreadable) parts.push(ext.unreadable + ' unreadable');
  if (ext.failed) parts.push(ext.failed + ' failed');
  if (parts.length) return parts.join(', ');
  // Every counter is zero. `reason` means the backend emptied the worklist
  // because something went wrong (no detections, missing weights, everything
  // below the detector threshold, source offline at preflight) — those photos
  // did need masks and will be hard-rejected as `no_subject_mask` without
  // them. Only a genuinely empty worklist gets the reassuring wording.
  return ext.reason ? 'No masks created' : 'No photos needed masks';
}

// Whether a mask-extraction result deserves the green completed treatment.
// Photos we couldn't read or that errored end the run with no mask, and
// scoring hard-rejects every unmasked photo as `no_subject_mask`; a `reason`
// means the backend emptied the worklist because something went wrong. None
// of those are "Done!". Shared by the pipeline-run card and the standalone
// Extract step so the two can't drift.
function _extractMasksClean(ext) {
  if (!ext) return true;
  return !ext.unreadable && !ext.failed && !ext.reason;
}

// What the standalone Extract card should show for a finished job. The job
// now reports `failed` when photos went unmasked, and the counts plus the
// backend's reconnect instruction matter most in exactly that case — so the
// rendering must not live inside a `completed`-only branch (Codex #1392 P2).
//
// `topErrors` is the completion event's own `errors` array. It's the only
// source of a message when the worker raised (JobRunner leaves `result` null
// and stashes the exception at the top level) or when the user cancelled
// before the worker returned a structured result — otherwise those two paths
// fall through to the generic "Some photos have no mask", hiding the real
// cause (Codex #1392 P2).
function _extractStepOutcome(jobStatus, result, topErrors) {
  var clean = jobStatus === 'completed' && _extractMasksClean(result);
  var nested = (result && result.errors) || [];
  var errs = nested.length ? nested : (topErrors || []);
  var statusText;
  if (clean) {
    statusText = 'Done!';
  } else if (errs.length) {
    statusText = 'Failed: ' + errs[0];
  } else if (jobStatus === 'cancelled') {
    statusText = 'Cancelled';
  } else {
    statusText = 'Some photos have no mask';
  }
  return {
    clean: clean,
    summary: _extractMasksSummary(result),
    status: statusText,
  };
}

function _pipelineFailureErrors(result) {
  var notes = new Set(result.notes || []);
  return (result.errors || []).filter(function(error) { return !notes.has(error); });
}

function _onPipelineComplete(result) {
  if (result.status === 'cancelled') {
    var actionStatus = document.getElementById('pipelineActionStatus');
    if (actionStatus) {
      actionStatus.textContent = 'Pipeline cancelled.';
      actionStatus.className = 'status-msg';
    }
    ['Source', 'Scan', 'Previews', 'Classify', 'Extract', 'EyeKeypoints', 'Group'].forEach(function(cardSuffix) {
      if (_runningStages[cardSuffix]) {
        delete _runningStages[cardSuffix];
        // Record a real 'cancelled' outcome so later refreshPipelineUI
        // calls keep rendering "Cancelled" — a 'will-skip' outcome would
        // be re-labeled with the future-tense "Will skip" on the next
        // refresh, which lies about what happened to a mid-run stage.
        _stageOutcomes[cardSuffix] = 'cancelled';
      }
    });
    refreshPipelineUI();
    return;
  }

  var succeeded = (result.status === 'completed');
  var r = result.result || {};
  var failureErrors = _pipelineFailureErrors(r);
  var stageResults = r.stages || {};

  // Source card — always mark complete
  var numSource = document.getElementById('numSource');
  numSource.className = 'stage-num complete';
  if (_sourceMode === 'collection') {
    document.getElementById('txtSource').textContent = 'Collection selected';
  } else {
    updateSourceSummary();
  }
  // If the ingest progress bar was shown (copy mode), finalize it
  // only on a successful pipeline run. On failure, leave the bar at
  // its last state so the user can see how far ingest got, and don't
  // show a misleading "Done!" label for an incomplete import.
  if (succeeded) {
    var fillSource = document.getElementById('fillSource');
    if (fillSource && fillSource.style.width && fillSource.style.width !== '0%') {
      fillSource.style.width = '100%';
      if (stageResults.ingest && typeof stageResults.ingest.copied === 'number') {
        _showSourceEjectSafe(stageResults.ingest);
      } else {
        var statusSourceEl = document.getElementById('statusSource');
        if (statusSourceEl) { statusSourceEl.textContent = 'Done!'; statusSourceEl.className = 'status-msg ok'; }
      }
    }
  }

  // Scan card
  if (stageResults.scan) {
    document.getElementById('numScan').className = 'stage-num complete';
    var scanInfo = stageResults.scan;
    var scanSummary = [];
    if (scanInfo.photos_indexed) scanSummary.push(scanInfo.photos_indexed + ' indexed');
    if (scanInfo.copied) scanSummary.push(scanInfo.copied + ' imported');
    document.getElementById('summaryScan').textContent = scanSummary.join(', ') || 'Done';
    var statusScan = document.getElementById('statusScan');
    var fillScan = document.getElementById('fillScan');
    if (fillScan) fillScan.style.width = '100%';
    if (statusScan) { statusScan.textContent = 'Done!'; statusScan.className = 'status-msg ok'; }
  }

  // Previews card
  if (stageResults.thumbnails || stageResults.previews) {
    document.getElementById('numPreviews').className = 'stage-num complete';
    var prevSummary = [];
    if (stageResults.thumbnails && stageResults.thumbnails.generated) prevSummary.push(stageResults.thumbnails.generated + ' thumbs');
    if (stageResults.previews && stageResults.previews.generated) prevSummary.push(stageResults.previews.generated + ' previews');
    document.getElementById('summaryPreviews').textContent = prevSummary.join(', ') || 'Done';
    var statusPrev = document.getElementById('statusPreviews');
    var fillPrev = document.getElementById('fillPreviews');
    if (fillPrev) fillPrev.style.width = '100%';
    if (statusPrev) { statusPrev.textContent = 'Done!'; statusPrev.className = 'status-msg ok'; }
  }

  // Classify card
  if (stageResults.classify) {
    document.getElementById('numClassify').className = 'stage-num complete';
    var cls = stageResults.classify;
    var predCount = cls.predictions_stored || cls.total || 0;
    document.getElementById('txtDetections').textContent = predCount + ' detections';
    var statusCls = document.getElementById('statusClassify');
    var fillCls = document.getElementById('fillClassify');
    if (fillCls) fillCls.style.width = '100%';
    if (statusCls) { statusCls.textContent = 'Done!'; statusCls.className = 'status-msg ok'; }
  }

  // Extract card
  if (stageResults.extract_masks) {
    var ext = stageResults.extract_masks;
    document.getElementById('txtMasks').textContent = _extractMasksSummary(ext);
    // A photo the stage couldn't read or that errored ends the run with no
    // mask, and scoring hard-rejects every unmasked photo as
    // `no_subject_mask`. Marking the card complete/"Done!" in that state
    // sends the user to Process Review with no idea why two-thirds of their
    // encounters are rejects.
    var extClean = _extractMasksClean(ext);
    document.getElementById('numExtract').className =
      extClean ? 'stage-num complete' : 'stage-num';
    var statusExt = document.getElementById('statusExtract');
    var fillExt = document.getElementById('fillExtract');
    if (fillExt) fillExt.style.width = '100%';
    if (statusExt) {
      if (extClean) {
        statusExt.textContent = 'Done!';
        statusExt.className = 'status-msg ok';
      } else {
        // The errors loop below replaces this with the stage's actionable
        // message ("Failed: [extract_masks] …"); this is the fallback for a
        // stage that reported bad counts without an error entry.
        statusExt.textContent = 'Some photos have no mask';
        statusExt.className = 'status-msg error';
      }
    }
  }

  // Eye Keypoints card
  if (stageResults.eye_keypoints) {
    document.getElementById('numEyeKeypoints').className = 'stage-num complete';
    var ekp = stageResults.eye_keypoints;
    var ekpTotal = ekp.total || 0;
    var ekpProcessed = ekp.processed || 0;
    var ekpTxt = document.getElementById('txtEyeKeypoints');
    if (ekpTxt) {
      // When the backend preflight skips the stage (disabled config or no
      // keypoint weights installed), surface the explicit reason so users
      // know it's a setup issue rather than an empty eligibility set.
      if (ekp.skipped) {
        ekpTxt.textContent = 'Skipped \u2014 ' + ekp.skipped;
        // Override the 'done' outcome set above: when the backend reports
        // skipped (e.g. no model installed), the pill should reflect that.
        _stageOutcomes['EyeKeypoints'] = 'will-skip';
        _setPill('EyeKeypoints', 'will-skip');
      } else {
        ekpTxt.textContent = ekpTotal
          ? (ekpProcessed + ' of ' + ekpTotal + ' processed')
          : 'No eligible photos';
      }
    }
    var statusEkp = document.getElementById('statusEyeKeypoints');
    var fillEkp = document.getElementById('fillEyeKeypoints');
    if (fillEkp) fillEkp.style.width = '100%';
    if (statusEkp) { statusEkp.textContent = 'Done!'; statusEkp.className = 'status-msg ok'; }
  }

  // Show errors and notes. Notes (result.notes, a subset of result.errors)
  // explain a stage that skipped without failing; they get their own wording
  // and style and never read "Failed". Older job rows have no notes key, so
  // every entry stays an error there.
  var messages = splitPipelineMessages(r.errors, r.notes);
  var errorStages = messages.errors;
  for (var noteStage in messages.notes) {
    var noteCard = _stageToCard[noteStage] || _failureOnlyStageToCard[noteStage];
    if (!noteCard) continue;
    var noteStatusEl = document.getElementById('status' + noteCard);
    if (noteStatusEl) {
      noteStatusEl.textContent = 'Skipped: ' + _humanizePipelineError(messages.notes[noteStage]);
      noteStatusEl.className = 'status-msg note';
    }
    // A card fed by several backend stages keeps the worse outcome: a
    // failure on a sibling stage below still overwrites this.
    if (_stageOutcomes[noteCard] !== 'failed') {
      delete _runningStages[noteCard];
      _stageOutcomes[noteCard] = 'skipped';
      _setPill(noteCard, 'skipped');
    }
  }
  if (messages.noteList.length) {
    // A run that completed with only notes doesn't auto-redirect (below), so
    // the banner offers the way on to Review.
    _showPipelineNotes(
      messages.noteList.map(_humanizePipelineError),
      succeeded && failureErrors.length === 0,
    );
  }
  if (Object.keys(errorStages).length > 0) {
    var firstFailedCard = null;
    for (var stage in errorStages) {
      // Failure-only stages (e.g. ``collection``) have no card of their own
      // but must fail their parent card here — the SSE progress path uses
      // _failureOnlyStageToCard for the same reason, and a fast failure
      // that finishes before the browser subscribes reaches only this path.
      var cardSuffix = _stageToCard[stage] || _failureOnlyStageToCard[stage];
      if (cardSuffix) {
        var statusEl = document.getElementById('status' + cardSuffix);
        if (statusEl) {
          statusEl.textContent = 'Failed: ' + errorStages[stage];
          statusEl.className = 'status-msg error';
        }
        delete _runningStages[cardSuffix];
        _stageOutcomes[cardSuffix] = 'failed';
        _setPill(cardSuffix, 'failed');
        var numEl = document.getElementById('num' + cardSuffix);
        if (numEl) numEl.className = 'stage-num';
        if (!firstFailedCard) firstFailedCard = cardSuffix;
      }
    }
    // The per-card status span is easy to miss (and invisible when the card
    // is collapsed), and errors whose stage has no card aren't shown at all.
    // Surface the full, actionable message in a top-level banner and open the
    // failed card so the user actually sees why the run stopped.
    _showPipelineError(Object.keys(errorStages).map(function(s) {
      return _humanizePipelineError(errorStages[s]);
    }));
    if (firstFailedCard) {
      var failedCardEl = document.getElementById('card-' + firstFailedCard.toLowerCase());
      if (failedCardEl) failedCardEl.classList.add('expanded');
    }
  }

  refreshPipelineUI();

  // Auto-redirect to Process Review when nothing failed and nothing was
  // skipped. A run whose only messages are notes completed, but stays here so
  // the user sees why a stage did nothing (photos with no mask are rejected
  // in Review as no_subject_mask); the notes banner links on to Review.
  if (succeeded && failureErrors.length === 0 && messages.noteList.length === 0) {
    window.location.href = '/pipeline/review';
  }
}

function _waitForPipelineTerminal(jobId, resolve) {
  var actionStatus = document.getElementById('pipelineActionStatus');
  if (actionStatus) {
    actionStatus.textContent = 'Connection lost while cancelling. Checking job status...';
  }
  var attempts = 0;
  var statusFailures = 0;
  var maxStatusFailures = 300;
  var terminalStatuses = {completed: true, failed: true, cancelled: true};
  var timer = setInterval(function() {
    attempts += 1;
    fetch('/api/jobs/' + encodeURIComponent(jobId))
      .then(function(resp) {
        if (!resp.ok) throw new Error('job status unavailable');
        return resp.json();
      })
      .then(function(job) {
        statusFailures = 0;
        if (!terminalStatuses[job.status]) {
          if (attempts === 120 && actionStatus) {
            actionStatus.textContent = 'Still cancelling. Waiting for pipeline job to stop...';
          }
          return;
        }
        clearInterval(timer);
        _pipelineResolve = null;
        _onPipelineComplete({
          status: job.status,
          result: job.result,
          errors: job.errors || [],
        });
        resolve();
      })
      .catch(function() {
        statusFailures += 1;
        if (statusFailures === 10 && actionStatus) {
          actionStatus.textContent = 'Still cancelling. Job status is temporarily unavailable...';
        }
        if (statusFailures >= maxStatusFailures) {
          clearInterval(timer);
          _pipelineResolve = null;
          _onPipelineComplete({
            status: 'cancelled',
            result: {cancelled: true},
            errors: ['Cancellation status stayed unavailable after repeated retries.'],
          });
          resolve();
        }
      });
  }, 1000);
}
