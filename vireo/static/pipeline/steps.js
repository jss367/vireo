// Standalone Classify and Extract step runs.
// Classic page script; load boot.js after all definitions.

async function runClassifyStep(collectionId) {
  var selectedModels = [];
  document.querySelectorAll('.model-checkbox:checked').forEach(function(cb) {
    selectedModels.push({id: cb.value, name: cb.dataset.name});
  });
  if (selectedModels.length === 0) {
    document.getElementById('statusClassify').textContent = 'No models selected \u2014 skipping';
    return false;
  }

  var num = document.getElementById('numClassify');
  var status = document.getElementById('statusClassify');
  var progressWrap = document.getElementById('progressClassify');
  var fill = document.getElementById('fillClassify');
  var text = document.getElementById('textClassify');

  num.className = 'stage-num running';
  status.textContent = 'Starting classification...';
  progressWrap.style.display = '';
  fill.style.width = '0%';

  // Expand card
  document.getElementById('card-classify').classList.add('expanded');

  var labelsFiles = (function() {
    var files = [];
    document.querySelectorAll('.labels-run-cb:checked').forEach(function(cb) { files.push(cb.value); });
    return files.length > 0 ? files : undefined;
  })();

  var completed = 0;
  var totalPredictions = 0;
  var total = selectedModels.length;

  for (var i = 0; i < selectedModels.length; i++) {
    if (_pipelineAbort) return false;
    var model = selectedModels[i];
    status.textContent = (total > 1 ? 'Model ' + (i+1) + '/' + total + ': ' : '') + 'Starting ' + model.name + '...';

    try {
      var data = await safeFetch('/api/jobs/classify', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          collection_id: collectionId,
          model_id: model.id,
          labels_files: labelsFiles,
          reclassify: !!document.getElementById('chkReclassify').checked,
        }),
      }, { toast: false });

      var ok = await new Promise(function(resolve) {
        var resolved = false;
        function done(success) { if (!resolved) { resolved = true; if (success) completed++; resolve(success); } }
        safeEventSource('/api/jobs/' + data.job_id + '/stream', {
          onProgress: function(p) {
            var modelPct = p.total > 0 ? p.current / p.total : 0;
            var overallPct = Math.round(((completed + modelPct) / total) * 100);
            fill.style.width = overallPct + '%';
            var parts = [];
            if (total > 1) parts.push(model.name);
            if (p.phase) parts.push(p.phase);
            if (p.total > 0 && p.current > 0) parts.push(p.current + '/' + p.total);
            if (p.current_file) parts.push(p.current_file);
            text.textContent = parts.join(' \u2014 ');
          },
          onComplete: function(result) {
            window.dispatchEvent(new CustomEvent('vireo-job-done', {detail: {job_id: data.job_id}}));
            if (result.status === 'completed' && result.result) {
              totalPredictions += (result.result.predictions_stored || result.result.total || 0);
              done(true);
            } else {
              done(false);
            }
          },
          onError: function() { done(false); }
        });
      });
    } catch(e) {
      status.textContent = 'Error (' + model.name + '): ' + e.message;
    }
  }

  if (completed === total) {
    fill.style.width = '100%';
    text.textContent = 'Complete';
    num.className = 'stage-num complete';
    status.textContent = 'Done!';
    status.className = 'status-msg ok';
    document.getElementById('txtDetections').textContent = totalPredictions + ' detections';
    return true;
  } else {
    num.className = 'stage-num';
    var failedCount = total - completed;
    status.textContent = failedCount + ' of ' + total + ' model' + (total > 1 ? 's' : '') + ' failed';
    return false;
  }
}

async function runExtractStep(collectionId) {
  var num = document.getElementById('numExtract');
  var status = document.getElementById('statusExtract');
  var progressWrap = document.getElementById('progressExtract');
  var fill = document.getElementById('fillExtract');
  var text = document.getElementById('textExtract');

  num.className = 'stage-num running';
  status.textContent = 'Starting feature extraction...';
  progressWrap.style.display = '';
  fill.style.width = '0%';
  document.getElementById('card-extract').classList.add('expanded');

  try {
    var data = await safeFetch('/api/jobs/extract-masks', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({collection_id: collectionId}),
    }, { toast: false });

    if (!data.job_id) {
      status.textContent = data.error || 'Failed to start';
      num.className = 'stage-num';
      return false;
    }

    return await new Promise(function(resolve) {
      safeEventSource('/api/jobs/' + data.job_id + '/stream', {
        onProgress: function(p) {
          var pct = p.total > 0 ? Math.round(p.current / p.total * 100) : 0;
          fill.style.width = pct + '%';
          var parts = [];
          if (p.phase) parts.push(p.phase);
          if (p.total > 0 && p.current > 0) parts.push(p.current + '/' + p.total);
          if (p.current_file) parts.push(p.current_file);
          text.textContent = parts.join(' \u2014 ');
          status.textContent = parts.join(' \u2014 ');
        },
        onComplete: function(result) {
          window.dispatchEvent(new CustomEvent('vireo-job-done', {detail: {job_id: data.job_id}}));
          var outcome = _extractStepOutcome(
            result.status, result.result, result.errors,
          );
          if (result.result) {
            document.getElementById('txtMasks').textContent = outcome.summary;
          }
          fill.style.width = '100%';
          text.textContent = outcome.clean ? 'Complete' : 'Finished with problems';
          status.textContent = outcome.status;
          status.className = outcome.clean ? 'status-msg ok' : 'status-msg error';
          num.className = outcome.clean ? 'stage-num complete' : 'stage-num';
          resolve(outcome.clean);
        },
        onError: function() {
          status.textContent = 'Connection lost';
          num.className = 'stage-num';
          resolve(false);
        }
      });
    });
  } catch(e) {
    status.textContent = 'Error: ' + e.message;
    num.className = 'stage-num';
    progressWrap.style.display = 'none';
    return false;
  }
}
