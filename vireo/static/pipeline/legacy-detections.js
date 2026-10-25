// Upgrade nudge for predictions stored before raw detections were retained.
/** @type {{id: number, path: string, photo_count: number}[]} */
var _legacyDetectionFolders = [];
var _legacyReclassifyPending = false;

/** @returns {Promise<boolean>} */
async function loadLegacyDetections() {
  try {
    var data = await safeFetch('/api/pipeline/legacy-detections', {}, {toast: false});
    _legacyDetectionFolders = data.folders;
    var card = /** @type {HTMLElement} */ (document.getElementById('legacyDetectionsCard'));
    card.hidden = _legacyDetectionFolders.length === 0;
    var count = _legacyDetectionFolders.length;
    /** @type {HTMLElement} */ (document.getElementById('legacyDetectionsTitle')).textContent =
      count + (count === 1 ? ' folder has' : ' folders have') + ' legacy detections.';
    var list = /** @type {HTMLElement} */ (document.getElementById('legacyDetectionsFolders'));
    list.replaceChildren();
    _legacyDetectionFolders.forEach(function(folder) {
      var item = document.createElement('li');
      item.textContent = folder.path + ' — ' + folder.photo_count +
        (folder.photo_count === 1 ? ' photo' : ' photos');
      list.appendChild(item);
    });
    return true;
  } catch (e) {
    console.error('Could not check for legacy detections:', e);
    return false;
  }
}

/** @returns {PipelineRequest} */
function legacyReclassifyRequest() {
  var models = Array.from(/** @type {NodeListOf<HTMLInputElement>} */ (
    document.querySelectorAll('.model-checkbox:checked')), function(cb) { return cb.value; });
  var labels = Array.from(/** @type {NodeListOf<HTMLInputElement>} */ (
    document.querySelectorAll('.labels-run-cb:checked')), function(cb) { return cb.value; });
  if (models.length === 0) throw new Error('Select a model in Classify below, then try again.');
  return {
    folder_ids: _legacyDetectionFolders.map(function(folder) { return folder.id; }),
    model_ids: models,
    labels_files: labels.length ? labels : undefined,
    reclassify: true,
    skip_classify: false,
    skip_extract_masks: true,
    skip_eye_keypoints: true,
    skip_regroup: true,
    miss_enabled: false,
    review_mode: null,
  };
}

async function reclassifyLegacyFolders() {
  if (_legacyReclassifyPending || !_legacyDetectionFolders.length) return;
  var button = /** @type {HTMLButtonElement} */ (document.getElementById('btnReclassifyLegacy'));
  var status = /** @type {HTMLElement} */ (document.getElementById('legacyDetectionsStatus'));
  _legacyReclassifyPending = true;
  button.disabled = true;
  status.textContent = 'Checking classification setup…';
  try {
    var body = legacyReclassifyRequest();
    var options = {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body)};
    // Check the affected-folder scope, independently of the normal Run plan.
    var plan = await safeFetch('/api/pipeline/plan', options, {toast: false});
    var classify = plan.stages && plan.stages.Classify;
    if (!classify || classify.state === 'blocked' || classify.state === 'will-skip') {
      throw new Error(classify ? classify.summary : 'Classification is unavailable.');
    }
    var data = await safeFetch('/api/jobs/pipeline', options, {toast: false});
    if (!data.job_id) throw new Error(data.error || 'Could not queue Reclassify.');
    status.textContent = data.model_warning || 'Reclassify queued for the affected folders.';
    /** @type {HTMLElement} */ (document.getElementById('legacyDetectionsJobs')).hidden = false;
    safeEventSource('/api/jobs/' + data.job_id + '/stream', {
      onProgress: function(p) {
        status.textContent = p.phase || 'Reclassify is running…';
      },
      onComplete: async function(result) {
        status.textContent = result.status === 'completed'
          ? 'Reclassify finished. See Jobs for results.'
          : 'Reclassify ' + result.status + '. See Jobs for details before retrying.';
        var outcome = status.textContent;
        status.textContent = outcome + ' Refreshing affected folders…';
        if (await loadLegacyDetections()) {
          _legacyReclassifyPending = false;
          button.disabled = false;
          status.textContent = outcome;
        } else {
          status.textContent = outcome +
            ' Could not refresh affected folders. Reload this page before retrying.';
        }
      },
      onError: function() {
        // The job may still be running; keep the action disabled to avoid
        // duplicate runs until the user checks Jobs or reloads the page.
        status.textContent = 'Progress connection lost. Check Jobs for the run status.';
      },
    });
  } catch (e) {
    status.textContent = e instanceof Error ? e.message : String(e);
    _legacyReclassifyPending = false;
    button.disabled = false;
  }
}
