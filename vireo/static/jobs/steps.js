// A job's step tree: progress, throughput and ETA, errors, and file leaves.
// Classic page script; load boot.js after all definitions.

// "key: value" lines for a finished job's result, in place of a raw
// JSON dump. Keys the interruption bookkeeping adds (and the error,
// which renders on its own line) are skipped; nested objects stay as
// compact JSON so nothing is hidden.
function renderRichStep(step, job, isLive) {
  var isRunning = step.status === 'running';
  var warningCount = step.error_count || (step.error ? 1 : 0);
  var isExpanded = isRunning || step.status === 'failed' || (step.status === 'completed' && warningCount > 0);
  if (collapsedSteps[step.id] === true) isExpanded = false;
  if (collapsedSteps[step.id] === false) isExpanded = true;
  // Import-in-place reuses one scan step across every source. Clear the
  // prior source's filenames as soon as the backend advances that source
  // index, even if the step is currently collapsed.
  if (job.type === 'import-in-place' &&
      typeof step.source_index === 'number' &&
      leafBufferSources[step.id] !== step.source_index) {
    delete leafBuffers[step.id];
    leafBufferSources[step.id] = step.source_index;
  }

  var html = '<div class="tree-step" data-step-id="' + esc(step.id) + '">';
  html += '<div class="tree-step-header" data-toggle-step="' + esc(step.id) + '">';
  html += toggleIcon(isExpanded);
  html += statusIcon(step.status, warningCount);
  html += '<span class="tree-step-label' + (step.status === 'pending' ? ' pending' : '') + '">' + esc(step.label) + '</span>';

  // 1-3: Progress counts, bar, percentage (for steps with progress)
  if (step.progress && step.progress.current > 0) {
    if (step.progress.total > 0) {
      var pct = Math.round((step.progress.current / step.progress.total) * 100);
      html += '<span class="tree-step-progress-text">' + step.progress.current.toLocaleString() + ' / ' + step.progress.total.toLocaleString() + '</span>';
      html += '<div class="tree-step-bar"><div class="tree-step-bar-fill" style="width:' + pct + '%"></div></div>';
      html += '<span class="tree-step-progress-text">' + pct + '%</span>';
    } else {
      html += '<span class="tree-step-progress-text">' + step.progress.current.toLocaleString() + ' / ?</span>';
      html += '<div class="tree-step-bar"><div class="tree-step-bar-fill indeterminate"></div></div>';
    }
  }

  // 7: Summary for terminal steps. The backend writes explanatory
  // summaries on completed, cancelled ("Cancelled (N animals detected
  // so far)"), and some failed steps (e.g. scan) — show them all.
  var isTerminal = step.status === 'completed' || step.status === 'cancelled' || step.status === 'failed';
  if (isTerminal && step.summary) {
    html += '<span class="tree-step-summary">(' + esc(step.summary) + ')</span>';
  }

  // 6: Duration — live timer for running, final for completed
  if (isRunning && step.started_at) {
    var stepElapsed = (Date.now() - new Date(step.started_at).getTime()) / 1000;
    html += '<span class="tree-step-duration">' + formatElapsed(stepElapsed) + '</span>';
  } else if (step.duration != null) {
    html += '<span class="tree-step-duration">' + formatElapsed(step.duration) + '</span>';
  }

  // 5: Error count (per-step, not global job.errors)
  var ec = step.error_count || 0;
  if (step.error) ec = Math.max(ec, 1);
  if (ec > 0) {
    var issueLabel = step.status === 'completed' ? 'warning' : 'error';
    var issueClass = step.status === 'completed' ? ' warning' : '';
    html += '<span class="tree-step-errors' + issueClass + '">' + ec + ' ' + issueLabel + (ec > 1 ? 's' : '') + '</span>';
  }

  // Retry button for failed steps on live jobs
  if (step.status === 'failed' && isLive) {
    html += '<button class="btn-retry" onclick="event.stopPropagation();">Retry</button>';
  }

  html += '</div>'; // end header

  // Expanded content
  if (isExpanded) {
    // Which label space this classify step compares photos against. The
    // header names the model; the same model against a regional species
    // list and against Tree of Life are different classifiers as far as
    // the results go, so the list has to be named too.
    if (step.label_source) {
      html += '<div class="tree-step-label-source">' + esc(step.label_source) + '</div>';
    }

    // Import-in-place publishes explicit discovery and metadata sub-phases.
    // Its overall file counter intentionally pauses during those phases, so
    // extrapolating current / whole-step elapsed produces a fake ETA that
    // grows while useful work is happening. The phase counter in the card
    // header is the truthful progress signal until file processing resumes.
    var importInPlacePhaseActive = job.type === 'import-in-place' &&
      job.progress && typeof job.progress.phase_current === 'number' &&
      job.progress.phase_total > 0;

    // 8: Throughput + ETA for running steps
    if (isRunning && step.progress && step.progress.current > 0 &&
        step.started_at && !importInPlacePhaseActive && step.progress.unit !== 'labels') {
      if (job.type === 'import') {
        // Import's visible counter advances while a batch is prepared,
        // before its transfer and restricted catalog scan finish. Use the
        // backend's completed-batch estimate instead of current / elapsed,
        // which is badly skewed by fast duplicate checks and queued files.
        var etaState = step.progress.eta_state;
        var etaSeconds = Number(step.progress.eta_seconds);
        var etaRate = Number(step.progress.eta_rate_per_min);
        var importStatusLine;
        if (etaState === 'ready' && isFinite(etaSeconds)) {
          if (etaSeconds > 0) {
            if (isFinite(etaRate) && etaRate > 0) {
              importStatusLine = '~' + Math.round(etaRate) +
                '/min from completed batches';
            } else {
              importStatusLine = 'Based on completed batches';
            }
            importStatusLine += ' &middot; ETA ' + formatElapsed(etaSeconds);
          } else {
            importStatusLine = 'Copy batches complete &middot; finishing import…';
          }
        } else {
          importStatusLine = 'Estimating after the first transfer batch…';
        }
        html += '<div class="tree-step-throughput">' + importStatusLine + '</div>';
      } else if (step.progress.eta_kind === 'classification') {
        // Classification can walk cached photos far faster than it can
        // decode and infer uncached ones. Use the backend's model-work ETA;
        // current / elapsed would turn a cache-heavy prefix into a wildly
        // optimistic prediction for the uncached tail.
        var classifyParts = [];
        var cacheHits = Number(step.progress.cache_hits) || 0;
        var classified = Number(step.progress.classified) || 0;
        if (cacheHits > 0) {
          classifyParts.push(cacheHits.toLocaleString() + ' cached');
        }
        if (classified > 0) {
          classifyParts.push(
            classified.toLocaleString() + ' newly classified'
          );
        }
        var classifyEtaState = step.progress.eta_state;
        var classifyEtaSeconds = Number(step.progress.eta_seconds);
        var classifyRate = Number(step.progress.eta_rate_per_min);
        if (
          classifyEtaState === 'ready'
          && isFinite(classifyEtaSeconds)
          && isFinite(classifyRate)
          && classifyRate > 0
        ) {
          classifyParts.push('~' + Math.round(classifyRate) + '/min uncached');
          if (classifyEtaSeconds > 0) {
            classifyParts.push('ETA ' + formatElapsed(classifyEtaSeconds));
          } else {
            classifyParts.push('finishing…');
          }
        } else if (classifyEtaState === 'finishing') {
          classifyParts.push('finishing…');
        } else {
          classifyParts.push('Estimating after the first uncached batch…');
        }
        html += '<div class="tree-step-throughput">' +
          classifyParts.join(' &middot; ') + '</div>';
      } else {
        var stepSecs = (Date.now() - new Date(step.started_at).getTime()) / 1000;
        if (stepSecs > 0) {
          var perSec = step.progress.current / stepSecs;
          var perMin = perSec * 60;
          var throughput;
          if (perMin >= 1) {
            throughput = '~' + Math.round(perMin) + '/min';
          } else {
            throughput = '~' + (1 / perSec).toFixed(1) + 's each';
          }
          var statusLine = throughput;
          // ETA only when we know the total and have a non-zero rate
          if (step.progress.total > 0 && perSec > 0) {
            var remaining = step.progress.total - step.progress.current;
            if (remaining > 0) {
              statusLine += ' &middot; ETA ' + formatElapsed(remaining / perSec);
            }
          }
          html += '<div class="tree-step-throughput">' + statusLine + '</div>';
        }
      }
    }

    // 4: Current file (step-level preferred, fall back to job-level)
    var currentFile = step.current_file || (isRunning && job.progress && job.progress.current_file ? job.progress.current_file : '');
    if (isRunning && currentFile) {
      html += '<div class="tree-step-current-file">' + esc(currentFile) + '</div>';
    }

    // Error detail
    if (step.error) {
      var detailClass = step.status === 'completed' ? ' warning' : '';
      html += '<div class="tree-step-error' + detailClass + '">' + esc(step.error) + '</div>';
    }

    var stageResult = job.result && job.result.stages
      ? job.result.stages[step.id] : null;
    var repairPhotos = stageResult && stageResult.failed_photos;
    if (repairPhotos && repairPhotos.length > 0) {
      html += '<div class="tree-repair-list">';
      repairPhotos.forEach(function(photo) {
        var label = photo.filename || ('Photo ' + photo.id);
        html += '<div class="tree-repair-item"><b>' + esc(label) + '</b>' +
          (photo.reason ? ': ' + esc(photo.reason) : '') + '</div>';
      });
      if (stageResult.failed_photos_truncated) {
        html += '<div class="tree-repair-item">And ' +
          stageResult.failed_photos_truncated.toLocaleString() +
          ' more. Missing thumbnails remain available for later repair.</div>';
      }
      html += '</div>';
    }

    // Leaf buffer (files processed). Hide stale filenames while an in-place
    // import is in a non-file sub-phase; the current status line above then
    // stands alone instead of looking like it applies to the previous
    // source's leaves.
    if (isRunning && !importInPlacePhaseActive) {
      var leaves = leafBuffers[step.id] || [];
      if (step.current_file) {
        var found = leaves.find(function(l) { return l.name === step.current_file; });
        if (!found) {
          leaves.push({ name: step.current_file, status: 'current' });
          if (leaves.length > LEAF_MAX) {
            var nonFailed = leaves.filter(function(l) { return l.status !== 'failed'; });
            if (nonFailed.length > LEAF_MAX) leaves.splice(leaves.indexOf(nonFailed[0]), 1);
          }
          leafBuffers[step.id] = leaves;
        }
      }
      if (leaves.length > 0) {
        html += '<div class="tree-leaves">';
        leaves.forEach(function(l) {
          var cls = l.status === 'current' ? ' current' : l.status === 'failed' ? ' failed' : '';
          html += '<div class="tree-leaf' + cls + '">' + esc(l.name) + '</div>';
        });
        html += '</div>';
      }
    }
  }

  html += '</div>'; // end tree-step
  return html;
}
