// Selected-photo analysis details, bounded retry, and background progress.
// Classic page script; no staged Group Review decisions are submitted here.

/** @type {{ids: number[]; sequence: number; jobId: string | null; workspaceId: number | null; readiness: ReviewAnalysisReadiness | null; snapshot: ReviewResults | null; scopeSequence: number; cancelRequested: boolean; submitting: boolean}} */
var reviewAnalysisRetry = {ids: [], sequence: 0, jobId: null, workspaceId: null,
  readiness: null, snapshot: null, scopeSequence: 0, cancelRequested: false, submitting: false};

/** @param {string} id */
function analysisRetryElement(id) {
  return requireReviewValue(document.getElementById(id));
}

/** @param {string} message */
function setAnalysisRetryStatus(message) {
  analysisRetryElement('analysisRetryStatus').textContent = message;
  analysisRetryElement('analysisRetryBanner').textContent = message;
}

/** @param {number[]} photoIds */
async function openAnalysisRetry(photoIds) {
  var dialog = /** @type {HTMLDialogElement} */ (analysisRetryElement('analysisRetryDialog'));
  if (reviewAnalysisRetry.jobId || reviewAnalysisRetry.submitting) { if (!dialog.open) dialog.showModal(); return; }
  if (grmState && grmState.applying) { showToast('Wait for Group Review to finish applying.', 'warning'); return; }
  var ids = photoIdsInReviewScope(pipelineReviewUniquePhotoIds(photoIds));
  if (!ids.length) { showToast('Select photos to inspect their analysis.', 'warning'); return; }
  if (ids.length > 200) { showToast('Select at most 200 photos for analysis retry.', 'warning'); return; }
  reviewAnalysisRetry.ids = ids.slice();
  reviewAnalysisRetry.readiness = null;
  var workspaceId = pipelineReviewWorkspaceId;
  reviewAnalysisRetry.workspaceId = workspaceId;
  var sequence = ++reviewAnalysisRetry.sequence;
  analysisRetryElement('analysisRetryPhotos').replaceChildren();
  /** @type {HTMLButtonElement} */ (analysisRetryElement('analysisRetryStart')).disabled = true;
  analysisRetryElement('analysisRetryCancel').hidden = true;
  setAnalysisRetryStatus('Checking current analysis for ' + ids.length + ' selected photos…');
  if (!dialog.open) dialog.showModal();
  try {
    var data = await safeFetch('/api/photos/analysis-readiness', {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({photo_ids: ids}),
    }, {toast: false});
    if (sequence !== reviewAnalysisRetry.sequence || !dialog.open) return;
    if (workspaceId === null || data.workspace_id !== workspaceId || pipelineReviewWorkspaceId !== workspaceId) {
      throw new Error('The active workspace changed. Reload the intended workspace before retrying analysis.');
    }
    renderAnalysisReadiness(data);
    setAnalysisRetryStatus(isScopedReviewView() ?
      'This regrouped view is read-only. Switch the scope to Saved review results, or open these photos from Browse, to retry analysis.' :
      'Review the exact ' + data.count + ' selected photos before starting.');
  } catch (error) {
    if (sequence === reviewAnalysisRetry.sequence) setAnalysisRetryStatus('Could not check analysis: ' + analysisRetryError(error));
  }
}

/** @param {unknown} error */
function analysisRetryError(error) {
  return error instanceof Error ? error.message : String(error);
}

/** @param {ReviewAnalysisReadiness} data */
function renderAnalysisReadiness(data) {
  reviewAnalysisRetry.readiness = data;
  analysisRetryElement('analysisRetryPhotos').innerHTML = data.photos.map(function(photo) {
    var stages = photo.required_stages.length ? 'Required work: ' + photo.required_stages.join(', ') : 'Required analysis is current.';
    var attempt = photo.last_attempt;
    var failure = attempt && attempt.error ? '<p>Last retry: ' + escapeHtml(attempt.error) + '</p>' : '';
    return '<li data-analysis-photo-id="' + photo.photo_id + '"><strong>' + escapeHtml(photo.filename) +
      '</strong> (photo ' + photo.photo_id + ')<p>' + escapeHtml(photo.explanation) + '</p><p>' + escapeHtml(stages) + '</p>' + failure + '</li>';
  }).join('');
  /** @type {HTMLButtonElement} */ (analysisRetryElement('analysisRetryStart')).disabled =
    isScopedReviewView() || !!reviewAnalysisRetry.jobId || !data.photos.some(function(photo) { return photo.required_stages.length > 0; });
}

async function startAnalysisRetry() {
  if (reviewAnalysisRetry.jobId || reviewAnalysisRetry.submitting || !reviewAnalysisRetry.readiness) return;
  var ids = photoIdsInReviewScope(reviewAnalysisRetry.ids);
  if (isScopedReviewView() || (grmState && grmState.applying) || ids.length !== reviewAnalysisRetry.ids.length ||
      reviewAnalysisRetry.workspaceId !== pipelineReviewWorkspaceId) {
    setAnalysisRetryStatus('The selection or view changed. Close this dialog and select the photos again.'); return;
  }
  /** @type {HTMLButtonElement} */ (analysisRetryElement('analysisRetryStart')).disabled = true;
  reviewAnalysisRetry.submitting = true;
  reviewAnalysisRetry.snapshot = pipelineResults;
  reviewAnalysisRetry.scopeSequence = reviewScopeRequestSeq;
  setAnalysisRetryStatus('Starting analysis for ' + ids.length + ' selected photos…');
  try {
    var data = await safeFetch('/api/photos/analysis-retry', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_ids: ids, expected_workspace_id: reviewAnalysisRetry.workspaceId}),
    }, {toast: false});
    // A submitted job remains trackable even if its dialog was closed meanwhile.
    reviewAnalysisRetry.jobId = data.job_id;
    reviewAnalysisRetry.cancelRequested = false;
    analysisRetryElement('analysisRetryCancel').hidden = false;
    setAnalysisRetryStatus('Analysis started for ' + ids.length + ' selected photos.');
    pollAnalysisRetry(data.job_id);
  } catch (error) {
    setAnalysisRetryStatus('Could not start analysis: ' + analysisRetryError(error));
    if (reviewAnalysisRetry.readiness) renderAnalysisReadiness(reviewAnalysisRetry.readiness);
  } finally { reviewAnalysisRetry.submitting = false; }
}

/** @param {string} jobId */
async function pollAnalysisRetry(jobId) {
  if (reviewAnalysisRetry.jobId !== jobId) return;
  try {
    var job = await /** @type {Promise<ReviewAnalysisJob>} */ (safeFetch('/api/jobs/' + encodeURIComponent(jobId), {}, {toast: false}));
    if (reviewAnalysisRetry.jobId !== jobId) return;
    if (['completed', 'failed', 'cancelled'].indexOf(job.status) !== -1) {
      await finishAnalysisRetry(job); return;
    }
    var progress = job.progress || {};
    var message = (reviewAnalysisRetry.cancelRequested ? 'Cancellation requested. ' : '') +
      (progress.current || 0) + ' of ' + reviewAnalysisRetry.ids.length + ' selected photos processed';
    if (progress.phase) message += ' · ' + progress.phase;
    if (progress.current_file) message += ' · ' + progress.current_file;
    setAnalysisRetryStatus(message);
  } catch (error) {
    setAnalysisRetryStatus('Could not read progress: ' + analysisRetryError(error) + '. Retrying…');
  }
  if (reviewAnalysisRetry.jobId === jobId) setTimeout(function() { pollAnalysisRetry(jobId); }, 1000);
}

/** @param {ReviewAnalysisJob} job */
async function finishAnalysisRetry(job) {
  var result = job.result;
  var message = job.status === 'cancelled' ? 'Analysis cancelled.' : job.status === 'failed' ?
    'Analysis failed: ' + (job.errors || []).join('; ') : 'Analysis finished.';
  if (result && result.photos) {
    var ready = result.photos.filter(function(photo) { return photo.status === 'complete'; }).length;
    var failed = result.photos.filter(function(photo) { return photo.status === 'failed'; }).length;
    message += ' ' + ready + ' ready for comparison; ' + (result.photos.length - ready - failed) + ' without current subject measurements; ' + failed + ' failed.';
  }
  var deferred = result && result.review_refresh && result.review_refresh.status === 'deferred';
  setAnalysisRetryStatus(message + (deferred ?
    ' Waiting for the current regroup to finish before refreshing saved suggestions…' : ' Reloading current analysis…'));
  try {
    if (!await refreshAnalysisRetryView()) message += ' The view changed during analysis; reload it to see the updated measurements.';
    var readiness = await safeFetch('/api/photos/analysis-readiness', {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({photo_ids: reviewAnalysisRetry.ids}),
    }, {toast: false});
    if (readiness.workspace_id !== reviewAnalysisRetry.workspaceId) throw new Error('The active workspace changed. Reopen analysis details in the intended workspace.');
    reviewAnalysisRetry.readiness = readiness;
    reviewAnalysisRetry.jobId = null;
    renderAnalysisReadiness(readiness);
  } catch (error) {
    message += ' Could not reload this view: ' + analysisRetryError(error);
    reviewAnalysisRetry.jobId = null;
    /** @type {HTMLButtonElement} */ (analysisRetryElement('analysisRetryStart')).disabled = true;
  }
  analysisRetryElement('analysisRetryCancel').hidden = true;
  setAnalysisRetryStatus(message);
}

async function cancelAnalysisRetry() {
  var jobId = reviewAnalysisRetry.jobId;
  if (!jobId) return;
  try {
    await safeFetch('/api/jobs/' + encodeURIComponent(jobId) + '/cancel', {method: 'POST'}, {toast: false});
    reviewAnalysisRetry.cancelRequested = true;
    setAnalysisRetryStatus('Cancellation requested. Waiting for the current stage to stop safely…');
  } catch (error) { setAnalysisRetryStatus('Could not cancel: ' + analysisRetryError(error)); }
}

async function refreshAnalysisRetryView() {
  var snapshot = reviewAnalysisRetry.snapshot;
  if (!snapshot || pipelineResults !== snapshot || reviewScopeRequestSeq !== reviewAnalysisRetry.scopeSequence) return false;
  var decisions = new Map(snapshot.photos.map(function(photo) { return [photo.id, {flag: photo.flag, rating: photo.rating}]; }));
  /** @type {ReviewResults | null | undefined} */ var fresh;
  if (snapshot.source === 'browse-selection') {
    fresh = await safeFetch('/api/pipeline/selection-results', {method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_ids: snapshot.photos.map(function(photo) { return photo.id; })})});
    if (fresh.workspace_id !== reviewAnalysisRetry.workspaceId) throw new Error('The active workspace changed.');
  } else {
    var data = await safeFetch('/api/pipeline/page-init', {}, {toast: false});
    if (data.workspace_id !== reviewAnalysisRetry.workspaceId) throw new Error('The active workspace changed.');
    fresh = data.results;
  }
  if (pipelineResults !== snapshot || reviewScopeRequestSeq !== reviewAnalysisRetry.scopeSequence) return false;
  if (!fresh) return false;
  const freshResults = fresh;
  var byId = new Map(fresh.photos.map(function(photo) { return [photo.id, photo]; }));
  snapshot.photos = snapshot.photos.map(function(photo) {
    var updated = byId.get(photo.id);
    var previous = decisions.get(photo.id);
    if (!updated || !previous) return photo;
    // A decision made while the refresh was in flight remains authoritative.
    if (photo.flag !== previous.flag) {
      updated.flag = photo.flag;
      updated.label = photo.flag === 'rejected' ? 'REJECT' : photo.flag === 'flagged' || photo.flag === 'picked' ? 'KEEP' : 'REVIEW';
    }
    if (photo.rating !== previous.rating) updated.rating = photo.rating;
    return updated;
  });
  // Preserve group membership, pending picks/rejects, selected subjects and viewport.
  if (grmState && grmState.items) grmState.items = grmState.items.map(function(photo) { return byId.get(photo.id) || photo; });
  snapshot.encounters.forEach(function(encounter) {
    var current = freshResults.encounters.find(function(candidate) {
      return candidate.photo_ids.length === encounter.photo_ids.length && candidate.photo_ids.every(function(id) { return encounter.photo_ids.indexOf(id) !== -1; });
    });
    if (current && current.focus_cohorts) encounter.focus_cohorts = current.focus_cohorts;
    else delete encounter.focus_cohorts;
  });
  updateSummaryBar(refreshLocalSummaryCounts());
  renderResults();
  if (document.getElementById('grmOverlay')?.classList.contains('open')) renderGroupModal();
  refreshLatestScopeSnapshotIfCurrent();
  return true;
}

function bindAnalysisRetryControls() {
  ['analysisRetryAction', 'grmAnalysisRetryAction'].forEach(function(id) {
    analysisRetryElement(id).addEventListener('click', function() { openAnalysisRetry(getActiveSelection()); });
  });
  analysisRetryElement('analysisRetryStart').addEventListener('click', startAnalysisRetry);
  analysisRetryElement('analysisRetryCancel').addEventListener('click', cancelAnalysisRetry);
  analysisRetryElement('analysisRetryClose').addEventListener('click', function() {
    /** @type {HTMLDialogElement} */ (analysisRetryElement('analysisRetryDialog')).close();
  });
  analysisRetryElement('analysisRetryDialog').addEventListener('close', function() { reviewAnalysisRetry.sequence++; });
  analysisRetryElement('analysisRetryDialog').addEventListener('keydown', function(event) { event.stopPropagation(); });
}
