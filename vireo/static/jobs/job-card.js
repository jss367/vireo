// The job card: header controls, meta, retry/resume row, banners, and results.
// Classic page script; load boot.js after all definitions.

function jobCollectionText(job) {
  var cfg = jobConfig(job);
  if (cfg.collection_name) return 'Collection: ' + cfg.collection_name;
  if (cfg.collection_id !== undefined && cfg.collection_id !== null && cfg.collection_id !== '') {
    return 'Collection #' + cfg.collection_id;
  }
  return '';
}

function renderJobCard(job, options) {
  options = options || {};
  // Queued pipelines are "live" for UI purposes: they have a real
  // backend identity, they're cancellable (runner.cancel_job handles
  // the queued case), and dropping them out of the active section
  // strands them — the user has no way to find or cancel a queued
  // pipeline until the slot opens.
  var isLive = isLiveStatus(job.status);
  var elapsed = '';
  if (isLive && job.started_at) {
    elapsed = formatElapsed((Date.now() - new Date(job.started_at).getTime()) / 1000);
  } else if (job.duration != null) {
    elapsed = formatElapsed(job.duration);
  } else if (job.result && job.result.interrupted && !job.result.last_progress_at) {
    elapsed = 'unknown (no progress recorded before the restart)';
  } else if (job.started_at && job.finished_at) {
    elapsed = formatElapsed((new Date(job.finished_at).getTime() - new Date(job.started_at).getTime()) / 1000);
  }

  var html = '<div class="job-card">';

  // Header: type, workspace label, status, cancel
  html += '<div class="job-card-header">';
  html += '<span class="job-detail-title">' + esc(window.formatJobType(job.type)) + '</span>';
  if (options.workspaceName) {
    html += '<span class="job-card-workspace">' + esc(options.workspaceName) + '</span>';
  }
  html += '<span class="job-detail-status ' + job.status + '">' + job.status + '</span>';
  if (isLive) {
    if (job.pausable && job.status === 'running') {
      html += '<button class="btn-pause" data-pause-job="' + escapeAttr(job.id) + '">Pause</button>';
    } else if (job.pausable && job.status === 'pausing') {
      // The pause has been requested but not yet honored: the worker
      // stops at its next safe checkpoint, which can be minutes away
      // (e.g. label embeddings inside the model lock). The status pill
      // still reads "pausing" so the user can see it is underway; this
      // button withdraws the request via the resume endpoint, which
      // accepts pausing as well as paused.
      //
      // BUT: when the pipeline itself parked the job on a dead source
      // (``_handle_source_offline`` in pipeline_job.py publishes
      // ``pause_reason`` before flipping to ``pausing``), cancelling
      // the pause would immediately retry the unreachable volume and
      // burn one of the bounded ``_MAX_SOURCE_OFFLINE_PAUSES`` attempts
      // — repeated clicks can convert a recoverable outage into a
      // failed run. Only offer cancel for user-requested pauses; for
      // safety pauses, leave the disabled ``Pausing…`` button so the
      // banner tells the user to reconnect the source instead.
      var automaticPauseReason = job.progress && job.progress.pause_reason;
      if (automaticPauseReason) {
        html += '<button class="btn-pause" disabled title="This pause was triggered automatically (see the pause banner). Fix the underlying issue, then Resume.">Pausing…</button>';
      } else {
        html += '<button class="btn-pause" data-resume-job="' + escapeAttr(job.id) + '" data-cancel-pause="1" title="Pause is still pending; the job stops at its next safe checkpoint. Click to keep it running instead.">Cancel pause</button>';
      }
    } else if (job.pausable && job.status === 'paused') {
      html += '<button class="btn-pause" data-resume-job="' + escapeAttr(job.id) + '">Resume</button>';
    }
    if (cancellingJobIds[job.id]) {
      html += '<button class="btn-cancel cancelling" disabled>Cancelling…</button>';
    } else {
      html += '<button class="btn-cancel" data-cancel-job="' + esc(job.id) + '">Cancel</button>';
    }
  }
  html += '</div>';

  // Meta: started, elapsed, overall %
  html += '<div class="job-detail-meta">';
  html += '<span>Started: ' + new Date(job.started_at).toLocaleTimeString() + '</span>';
  html += '<span>Elapsed: ' + elapsed + '</span>';
  var collectionText = jobCollectionText(job);
  if (collectionText) {
    html += '<span>' + esc(collectionText) + '</span>';
  }
  var visibleProgress = activeProgress(job.progress);
  if (visibleProgress) {
    html += '<span>' + esc(visibleProgress.label) + ': ' + progressPct(visibleProgress) + '%</span>';
  }
  html += '</div>';

  html += renderMoveRoute(job);
  html += renderSourceCleanup(job);

  var failedImportCount = job.result && Number(job.result.failed || 0);
  // Once a later resume or retry took over, the server refuses both
  // Resume and Retry on this row (409), so neither is offered; say where
  // to go instead.
  var resumeTakeover = importResumeTakeover(job, historyJobs);
  var resumableImport = isResumableImport(job) && !resumeTakeover.by;
  if (!isLive && (failedImportCount > 0 || resumableImport || resumeTakeover.by) &&
      importRetryBody(job)) {
    var retryInFlight = hasActiveRetryFor(job.id);
    html += '<div style="padding:10px 0 2px;font-size:12px;color:var(--text-muted);">';
    if (resumeTakeover.by && !retryInFlight) {
      html += '<span>' + esc(importTakeoverNote(resumeTakeover)) + '</span> ' +
        '<button class="btn-retry" data-show-job="' + escapeAttr(resumeTakeover.by) +
        '" style="margin-left:8px;">Show that import</button>';
    } else if (retryInFlight) {
      // Same failed job, retry already running. A second Start would
      // race the first — same source, same destination, same carry
      // scope — so surface the in-flight status instead of a live
      // button. Re-enables automatically when the retry finishes and
      // ``activeJobs`` no longer contains it.
      html += '<button class="btn-retry" disabled aria-disabled="true" ' +
        'title="A retry for this import is already running">' +
        'Retry in progress…</button>';
      html += '<span style="margin-left:8px;">Retry already launched for this job; wait for it to finish before starting another.</span>';
    } else if (resumableImport) {
      html += '<button class="btn-retry" data-retry-import-job="' +
        escapeAttr(job.id) + '" data-import-resume>Resume import</button>';
      html += '<span style="margin-left:8px;">' + esc(importResumeHint(job)) + '</span>';
    } else {
      html += '<button class="btn-retry" data-retry-import-job="' +
        escapeAttr(job.id) + '">Retry ' + failedImportCount + ' failed file' +
        (failedImportCount === 1 ? '' : 's') + '</button>';
      html += '<span style="margin-left:8px;">Same source and destination; successful files are skipped.</span>';
    }
    html += '</div>';
  }

  // Pause reason banner: when classify (or any pipeline stage) parks the
  // job on an outage it can't recover from on its own — a dropped share,
  // missing volume — it publishes ``pause_reason`` on job.progress. Without
  // this banner the user only sees a generic "paused" pill and can't tell
  // what to fix before pressing Resume.
  var pauseReason = job.progress && job.progress.pause_reason;
  if (pauseReason && (job.status === 'pausing' || job.status === 'paused')) {
    html += '<div class="job-pause-banner">' + esc(pauseReason) + '</div>';
  }

  // Interrupted-by-restart banner. The startup sweep keeps whatever
  // the checkpoint thread last recorded (step tree, progress) and
  // stamps result.interrupted; say plainly what was recorded and when,
  // instead of leaving the user with a bare error string.
  if (!isLive && job.result && job.result.interrupted) {
    html += '<div class="job-pause-banner">' + esc(interruptedText(job)) + '</div>';
  }

  // Steps
  var steps = job.steps || job.tree || [];
  if (steps.length > 0) {
    html += '<div class="job-tree">';
    steps.forEach(function(step) {
      html += renderRichStep(step, job, isLive);
    });
    html += '</div>';
  } else if (isLive && job.progress) {
    // Fallback for running jobs without steps
    html += '<div class="job-tree"><div style="padding:8px;font-size:13px;color:var(--text-muted);">';
    if (job.progress.phase) html += '<div style="color:var(--accent);font-weight:500;margin-bottom:8px;">' + esc(job.progress.phase) + '</div>';
    var fallbackProgress = activeProgress(job.progress);
    if (fallbackProgress) {
      var pct = progressPct(fallbackProgress);
      html += '<div class="tree-step-bar" style="width:200px;margin-bottom:4px;"><div class="tree-step-bar-fill" style="width:' + pct + '%"></div></div>';
      html += '<div>' + esc(fallbackProgress.label) + ': ' + fallbackProgress.current.toLocaleString() + ' / ' + fallbackProgress.total.toLocaleString() + '</div>';
    }
    if (job.progress.current_file) html += '<div style="margin-top:4px;">' + esc(job.progress.current_file) + '</div>';
    html += '</div></div>';
  } else if (!isLive) {
    // Fallback for completed/failed jobs without steps. The server
    // turns the raw result dict into prose (``summary`` plus
    // ``result_details`` lines, see job_summaries.py) so the user never
    // sees ``{"deleted": 28, "trashed": 28, ...}``.
    html += '<div class="job-tree"><div style="padding:8px;font-size:13px;color:var(--text-muted);">';
    // History rows carry the failure in result.error (the persisted
    // payload), not job.error. When the startup sweep stamped
    // result.interrupted, the banner above already says it.
    var resultError = job.error || (job.result && typeof job.result === 'object' && job.result.error) || '';
    var wasInterrupted = !!(job.result && job.result.interrupted);
    if (wasInterrupted && resultError === job.result.error) {
      resultError = '';
    }
    if (resultError) {
      html += '<div style="color:var(--danger);font-weight:500;">Error: ' + esc(resultError) + '</div>';
    }
    var resultSummary = job.summary || '';
    // Job-authored summaries often wrap the error ("Move failed — <error>");
    // don't print the same message twice. Interrupted rows' summaries
    // restate the banner, so skip them too.
    if (resultSummary && !(resultError && resultSummary.indexOf(resultError) !== -1) && !wasInterrupted) {
      html += '<div class="job-result-summary">' + esc(resultSummary) + '</div>';
    }
    var resultDetails = Array.isArray(job.result_details) ? job.result_details : [];
    if (resultDetails.length > 0) {
      html += '<div class="job-result-details">';
      resultDetails.forEach(function(line) {
        html += '<div class="job-result-detail-line">' + esc(line) + '</div>';
      });
      html += '</div>';
    }
    if (!resultError && !resultSummary && resultDetails.length === 0 && !wasInterrupted) {
      html += '<div>No details available</div>';
    }
    html += '</div></div>';
  }

  html += '</div>';
  return html;
}

// Plain-language account of a job the restart cut short: how far it
// got (steps finished, or the last recorded count) and when that was
// last recorded. Never claims more than the checkpoint actually saw.
function interruptedText(job) {
  var parts = ['Interrupted by Vireo restart.'];
  var steps = job.steps || job.tree || [];
  var progress = job.progress || {};
  if (steps.length > 0) {
    var done = steps.filter(function(s) { return s.status === 'completed'; }).length;
    parts.push(done + ' of ' + steps.length + (steps.length === 1 ? ' step' : ' steps') +
      ' finished before the restart.');
  } else if (typeof progress.current === 'number' &&
             (progress.current > 0 || progress.total > 0)) {
    // "0 of N" is worth saying: it tells the user nothing was done.
    var got = 'Got through ' + progress.current.toLocaleString();
    if (progress.total > 0) got += ' of ' + progress.total.toLocaleString();
    if (progress.phase) got += ' (' + progress.phase + ')';
    parts.push(got + '.');
  }
  if (progress.current_file) {
    parts.push('Last file: ' + progress.current_file + '.');
  }
  if (job.result.last_progress_at) {
    parts.push('Last progress was recorded at ' +
      new Date(job.result.last_progress_at).toLocaleString() + '.');
  } else {
    parts.push('No progress was recorded before the restart.');
  }
  return parts.join(' ');
}
