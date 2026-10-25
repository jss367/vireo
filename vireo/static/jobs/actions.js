// Detail pane buttons: cleanup, show job, retry/resume import, pause, cancel.

import { state } from './state.js';
import { renderHistoryDetail, toggleStep } from './detail.js';
import {
  hasActiveRetryFor, importResumeTakeover, importRetryBody, importTakeoverNote,
} from './import-retry.js';
import {
  fetchHistory, fetchImportRecoveryOptions, fetchJobs, refreshHistoryImportRecovery,
} from './polling.js';
import { selectJob } from './selection.js';
import { requestSourceCleanup } from './source-cleanup.js';
import { directNasImportRetryBody, importFailedBeforeStart, showToast } from './shared.js';

// Event delegation for detail pane clicks (step toggle, pause/resume, cancel)
export function bindDetailPaneActions() {
  /** @type {HTMLElement} */ (document.getElementById('jobDetailPane')).addEventListener('click', function(e) {
    if (!(e.target instanceof Element)) return;
    let cleanupButton = e.target.closest('[data-review-source], [data-clean-source]');
    if (cleanupButton) {
      let cleanId = cleanupButton.getAttribute('data-clean-source');
      let cleanupId = cleanId || cleanupButton.getAttribute('data-review-source'); if (cleanupId) requestSourceCleanup(cleanupId, !!cleanId);
      return;
    }
    let showJobBtn = e.target.closest('[data-show-job]');
    if (showJobBtn) {
      let showJobId = showJobBtn.getAttribute('data-show-job'); if (!showJobId) return;
      let inActive = state.activeJobs.some(function(job) { return job.id === showJobId; });
      selectJob(showJobId, inActive ? 'active' : 'history');
      return;
    }
    let restartInPlaceBtn = /** @type {HTMLButtonElement | null} */ (e.target.closest('[data-restart-in-place]'));
    if (restartInPlaceBtn) {
      let restartId = restartInPlaceBtn.getAttribute('data-restart-in-place'); if (!restartId) return;
      if (state.inPlaceRestarts[restartId]) return;
      state.inPlaceRestarts[restartId] = {pending: true};
      restartInPlaceBtn.disabled = true;
      restartInPlaceBtn.textContent = 'Restarting…';
      fetch('/api/jobs/import-in-place/' + encodeURIComponent(restartId) + '/restart', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: '{}',
      }).then(function(response) {
        return response.json().catch(function() { return {}; }).then(function(data) {
          if (!response.ok && !data.restart_job_id) {
            throw new Error(data.error || 'Import failed to restart');
          }
          return {jobId: data.job_id || data.restart_job_id, alreadyRestarted: !response.ok};
        });
      }).then(function(data) {
        state.inPlaceRestarts[restartId] = {jobId: data.jobId};
        if (typeof showToast === 'function') {
          showToast(data.alreadyRestarted ? 'This import was already restarted. Showing the later run.' :
            'Import restarted. Rescanning the original selection; unchanged catalog records are reused.', 'info');
        }
        state.selectedJobId = data.jobId;
        state.selectedSource = 'active';
        state.currentView = data.jobId;
        fetchJobs();
      }).catch(function(error) {
        delete state.inPlaceRestarts[restartId];
        restartInPlaceBtn.disabled = false;
        restartInPlaceBtn.textContent = 'Restart import';
        if (state.selectedJobId === restartId) {
          let originalImport = state.activeJobs.concat(state.historyJobs).find(function(job) { return job.id === restartId; });
          if (originalImport) renderHistoryDetail(originalImport);
        }
        if (typeof showToast === 'function') showToast(error.message, 'error');
        fetchHistory();
      });
      return;
    }
    let retryImportBtn = /** @type {HTMLButtonElement | null} */ (e.target.closest('[data-retry-import-job]'));
    let recoveryCheckBtn = /** @type {HTMLButtonElement | null} */ (e.target.closest('[data-check-import-recovery]'));
    if (recoveryCheckBtn) {
      let recoveryJob = state.historyJobs.find(function(job) {
        return job.id === recoveryCheckBtn.getAttribute('data-check-import-recovery');
      });
      if (recoveryJob) {
        refreshHistoryImportRecovery(recoveryJob);
        if (state.selectedJobId === recoveryJob.id) renderHistoryDetail(recoveryJob);
      }
      return;
    }
    if (retryImportBtn) {
      let retryJobId = retryImportBtn.getAttribute('data-retry-import-job'); if (!retryJobId) return;
      let retryJob = state.activeJobs.concat(state.historyJobs).find(function(job) {
        return job.id === retryJobId;
      });
      if (retryJob && (retryJob.import_resume_pending ||
          (retryJob.import_recovery_options && retryJob.import_recovery_options.error &&
           (!importFailedBeforeStart(retryJob.result) || !retryJob.import_recovery_options.retryable)))) {
        if (typeof showToast === 'function') showToast(
          retryJob.import_resume_pending ? 'Checking recovery options…' : retryJob.import_recovery_options?.error,
          'info');
        return;
      }
      let retryBody = importRetryBody(retryJob);
      let directToNas = retryImportBtn.hasAttribute('data-retry-direct-to-nas');
      if (directToNas) retryBody = directNasImportRetryBody(retryJob, retryBody, state.historyJobs);
      if (!retryBody) {
        if (typeof showToast === 'function') {
          showToast('The original import settings are no longer available.', 'error');
        }
        return;
      }
      // A click between poll ticks could otherwise launch a second
      // retry for the same parent while its first retry is still active.
      // Keep this guard even when activeJobs refreshed just after render.

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
      let lateTakeover = importResumeTakeover(retryJob, state.historyJobs);
      if (lateTakeover.by) {
        if (typeof showToast === 'function') {
          showToast(importTakeoverNote(lateTakeover), 'info');
        }
        if (retryJob && state.selectedJobId === retryJobId) renderHistoryDetail(retryJob);
        return;
      }
      // Preserve the original count-specific label so the error path
      // below can restore it exactly — dropping to a generic "Retry
      // failed files" would silently hide the real failed-file count.
      let retryOriginalLabel = retryImportBtn.textContent;
      // Resume keeps the parent's ``skip_duplicates`` like any retry:
      // with it off, the collision walk still adopts each file the parent
      // already landed (byte-identical at its destination path) instead
      // of copying it again, and files the parent never reached are
      // copied as the user asked.
      let isResume = retryImportBtn.hasAttribute('data-import-resume');
      retryImportBtn.disabled = true;
      retryImportBtn.textContent = isResume ? 'Resuming…' : 'Starting retry…';
      state.pendingImportRetries[retryJobId] = true;
      let retryButtons = /** @type {HTMLElement} */ (document.getElementById('jobDetailPane')).querySelectorAll(/** @type {'button'} */ ('button[data-retry-import-job]'));
      retryButtons.forEach(function(button) { button.disabled = true; });
      let eligibility = Promise.resolve();
      if (directToNas) {
        eligibility = fetchImportRecoveryOptions(retryJobId)
          .then(function(options) {
            let current = state.historyJobs.find(function(job) { return job.id === retryJobId; });
            if (current) current.import_recovery_options = options;
            if (options.direct_to_nas_allowed !== true) {
              throw new Error(options.error || 'Direct-to-NAS retry is no longer available. Retry with the original settings.');
            }
          });
      }
      eligibility.then(function() {
        return fetch('/api/jobs/import-photos', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify(retryBody),
        });
      }).then(function(response) {
        return response.json().catch(function() { return {}; }).then(function(data) {
          if (!response.ok) {
            throw new Error(data.error || 'Retry failed to start');
          }
          return data;
        });
      }).then(function(data) {
        delete state.pendingImportRetries[retryJobId];
        if (typeof showToast === 'function') {
          showToast(isResume
            ? 'Import resumed. Files already imported will be skipped.'
            : 'Import retry started. Successful files will be skipped.', 'info');
        }
        state.selectedJobId = data.job_id;
        state.selectedSource = 'active';
        state.currentView = data.job_id;
        fetchJobs();
      }).catch(function(error) {
        delete state.pendingImportRetries[retryJobId];
        retryButtons.forEach(function(button) { button.disabled = false; });
        retryImportBtn.disabled = false;
        retryImportBtn.textContent = retryOriginalLabel;
        if (state.selectedJobId === retryJobId) {
          let failedRetryParent = state.historyJobs.find(function(job) { return job.id === retryJobId; });
          if (failedRetryParent) renderHistoryDetail(failedRetryParent);
        }
        if (typeof showToast === 'function') showToast(error.message, 'error');
      });
      return;
    }
    let stepHeader = e.target.closest('[data-toggle-step]');
    if (stepHeader) {
      var stepId = stepHeader.getAttribute('data-toggle-step'); if (stepId) toggleStep(stepId);
      return;
    }
    let pauseBtn = /** @type {HTMLButtonElement | null} */ (e.target.closest('[data-pause-job]'));
    let resumeBtn = /** @type {HTMLButtonElement | null} */ (e.target.closest('[data-resume-job]'));
    if (pauseBtn || resumeBtn) {
      let controlBtn = pauseBtn || resumeBtn; if (!controlBtn) return;
      let action = pauseBtn ? 'pause' : 'resume';
      let cancellingPause = !!(resumeBtn && resumeBtn.hasAttribute('data-cancel-pause'));
      let jobId = controlBtn.getAttribute(
        pauseBtn ? 'data-pause-job' : 'data-resume-job'
      ); if (!jobId) return;
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
        let job = state.activeJobs.find(function(j) { return j.id === jobId; });
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
    let cancelBtn = /** @type {HTMLButtonElement | null} */ (e.target.closest('[data-cancel-job]'));
    if (cancelBtn) {
      let jobId = cancelBtn.getAttribute('data-cancel-job'); if (!jobId) return;
      // Optimistic UI: flip the button to a disabled "Cancelling…" pill
      // immediately so the click feels live. The server cancel just sets
      // a flag; the running stage may take seconds to actually exit. Track
      // jobId in cancellingJobIds so subsequent fetchJobs re-renders keep
      // the state until the job moves out of active.
      cancelBtn.disabled = true;
      cancelBtn.textContent = 'Cancelling…';
      cancelBtn.classList.add('cancelling');
      state.cancellingJobIds[jobId] = true;
      fetch('/api/jobs/' + encodeURIComponent(jobId) + '/cancel', { method: 'POST' })
        .then(function(r) {
          if (r.ok) {
            if (typeof showToast === 'function') showToast('Cancelling job…', 'info');
            fetchJobs();
          } else {
            return r.json().then(function(data) {
              let msg = (data && data.error) || 'Unable to cancel job';
              if (typeof showToast === 'function') showToast(msg, 'error');
              delete state.cancellingJobIds[jobId];
              cancelBtn.disabled = false;
              cancelBtn.textContent = 'Cancel';
              cancelBtn.classList.remove('cancelling');
            });
          }
        })
        .catch(function() {
          if (typeof showToast === 'function') showToast('Unable to cancel job', 'error');
          delete state.cancellingJobIds[jobId];
          cancelBtn.disabled = false;
          cancelBtn.textContent = 'Cancel';
          cancelBtn.classList.remove('cancelling');
        });
    }
  });
}
