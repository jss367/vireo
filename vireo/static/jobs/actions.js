// Detail pane buttons: cleanup, show job, retry/resume import, pause, cancel.
// Classic page script; load boot.js after all definitions.

// Event delegation for detail pane clicks (step toggle, pause/resume, cancel)
function bindDetailPaneActions() {
  document.getElementById('jobDetailPane').addEventListener('click', function(e) {
    var cleanupButton = e.target.closest('[data-review-source], [data-clean-source]');
    if (cleanupButton) {
      var cleanId = cleanupButton.getAttribute('data-clean-source');
      requestSourceCleanup(cleanId || cleanupButton.getAttribute('data-review-source'), !!cleanId);
      return;
    }
    var showJobBtn = e.target.closest('[data-show-job]');
    if (showJobBtn) {
      var showJobId = showJobBtn.getAttribute('data-show-job');
      var inActive = activeJobs.some(function(job) { return job.id === showJobId; });
      selectJob(showJobId, inActive ? 'active' : 'history');
      return;
    }
    var retryImportBtn = e.target.closest('[data-retry-import-job]');
    if (retryImportBtn) {
      var retryJobId = retryImportBtn.getAttribute('data-retry-import-job');
      var retryJob = activeJobs.concat(historyJobs).find(function(job) {
        return job.id === retryJobId;
      });
      var retryBody = importRetryBody(retryJob);
      if (!retryBody) {
        if (typeof showToast === 'function') {
          showToast('The original import settings are no longer available.', 'error');
        }
        return;
      }
      // Backstop the render-time gate: a click landing between poll
      // ticks could otherwise fire a second retry for the same parent
      // while the first is still active. Same rationale as the disabled
      // button above; keeps the parallel-launch race closed even when
      // ``activeJobs`` refreshed just after render.
      if (hasActiveRetryFor(retryJobId)) {
        if (typeof showToast === 'function') {
          showToast(
            'A retry for this import is already running; wait for it to finish.',
            'info',
          );
        }
        return;
      }
      // A resume that finished between renders took this row over; the
      // server would refuse with a 409, so say so without sending it.
      var lateTakeover = importResumeTakeover(retryJob, historyJobs);
      if (lateTakeover.by) {
        if (typeof showToast === 'function') {
          showToast(importTakeoverNote(lateTakeover), 'info');
        }
        if (selectedJobId === retryJobId) renderHistoryDetail(retryJob);
        return;
      }
      // Preserve the original count-specific label so the error path
      // below can restore it exactly — dropping to a generic "Retry
      // failed files" would silently hide the real failed-file count.
      var retryOriginalLabel = retryImportBtn.textContent;
      // Resume keeps the parent's ``skip_duplicates`` like any retry:
      // with it off, the collision walk still adopts each file the parent
      // already landed (byte-identical at its destination path) instead
      // of copying it again, and files the parent never reached are
      // copied as the user asked.
      var isResume = retryImportBtn.hasAttribute('data-import-resume');
      retryImportBtn.disabled = true;
      retryImportBtn.textContent = isResume ? 'Resuming…' : 'Starting retry…';
      fetch('/api/jobs/import-photos', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(retryBody),
      }).then(function(response) {
        return response.json().catch(function() { return {}; }).then(function(data) {
          if (!response.ok) {
            throw new Error(data.error || 'Retry failed to start');
          }
          return data;
        });
      }).then(function(data) {
        if (typeof showToast === 'function') {
          showToast(isResume
            ? 'Import resumed. Files already imported will be skipped.'
            : 'Import retry started. Successful files will be skipped.', 'info');
        }
        selectedJobId = data.job_id;
        selectedSource = 'active';
        currentView = data.job_id;
        fetchJobs();
      }).catch(function(error) {
        retryImportBtn.disabled = false;
        retryImportBtn.textContent = retryOriginalLabel;
        if (typeof showToast === 'function') showToast(error.message, 'error');
      });
      return;
    }
    var stepHeader = e.target.closest('[data-toggle-step]');
    if (stepHeader) {
      toggleStep(stepHeader.getAttribute('data-toggle-step'));
      return;
    }
    var pauseBtn = e.target.closest('[data-pause-job]');
    var resumeBtn = e.target.closest('[data-resume-job]');
    if (pauseBtn || resumeBtn) {
      var controlBtn = pauseBtn || resumeBtn;
      var action = pauseBtn ? 'pause' : 'resume';
      var cancellingPause = !!(resumeBtn && resumeBtn.hasAttribute('data-cancel-pause'));
      var jobId = controlBtn.getAttribute(
        pauseBtn ? 'data-pause-job' : 'data-resume-job'
      );
      controlBtn.disabled = true;
      controlBtn.textContent = pauseBtn ? 'Pausing…' :
        (cancellingPause ? 'Cancelling pause…' : 'Resuming…');
      fetch('/api/jobs/' + encodeURIComponent(jobId) + '/' + action, {
        method: 'POST'
      }).then(function(r) {
        if (!r.ok) {
          return r.json().then(function(data) {
            throw new Error((data && data.error) || 'Unable to ' + action + ' job');
          });
        }
        return r.json();
      }).then(function(data) {
        var job = activeJobs.find(function(j) { return j.id === jobId; });
        if (job && data.status) job.status = data.status;
        if (typeof showToast === 'function') {
          showToast(
            action === 'pause' ? 'Pausing after the current batch…' :
              (cancellingPause ? 'Pause cancelled; the job keeps running' : 'Job resumed'),
            'info'
          );
        }
        fetchJobs();
      }).catch(function(err) {
        if (typeof showToast === 'function') showToast(err.message, 'error');
        fetchJobs();
      });
      return;
    }
    var cancelBtn = e.target.closest('[data-cancel-job]');
    if (cancelBtn) {
      var jobId = cancelBtn.getAttribute('data-cancel-job');
      // Optimistic UI: flip the button to a disabled "Cancelling…" pill
      // immediately so the click feels live. The server cancel just sets
      // a flag; the running stage may take seconds to actually exit. Track
      // jobId in cancellingJobIds so subsequent fetchJobs re-renders keep
      // the state until the job moves out of active.
      cancelBtn.disabled = true;
      cancelBtn.textContent = 'Cancelling…';
      cancelBtn.classList.add('cancelling');
      cancellingJobIds[jobId] = true;
      fetch('/api/jobs/' + encodeURIComponent(jobId) + '/cancel', { method: 'POST' })
        .then(function(r) {
          if (r.ok) {
            if (typeof showToast === 'function') showToast('Cancelling job…', 'info');
            fetchJobs();
          } else {
            return r.json().then(function(data) {
              var msg = (data && data.error) || 'Unable to cancel job';
              if (typeof showToast === 'function') showToast(msg, 'error');
              delete cancellingJobIds[jobId];
              cancelBtn.disabled = false;
              cancelBtn.textContent = 'Cancel';
              cancelBtn.classList.remove('cancelling');
            });
          }
        })
        .catch(function() {
          if (typeof showToast === 'function') showToast('Unable to cancel job', 'error');
          delete cancellingJobIds[jobId];
          cancelBtn.disabled = false;
          cancelBtn.textContent = 'Cancel';
          cancelBtn.classList.remove('cancelling');
        });
    }
  });
}
