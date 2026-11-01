// Sequential inspection alongside the existing prediction and photo decision writers.
function renderReviewQueue() {
  if (reviewQueue.workspace === null || _predictionsReloading) return;
  var key = rqScopeKey();
  if (key !== reviewQueue.key) {
    reviewQueue.key = key;
    reviewQueue.session = rqSession(rqRead(key));
    reviewQueue.current = null;
    ++reviewQueue.sequence;
    rqClearCrop();
  }
  rqSave();
  /** @type {HTMLInputElement} */ (rqElement('rqEnabled')).checked = reviewQueue.enabled;
  rqElement('rqContent').hidden = !reviewQueue.enabled;
  if (!reviewQueue.enabled) return;
  var photos = rqPhotos();
  reviewQueue.items = reviewQueue.session.followUpOnly ? photos.filter(function(p) { return rqItem(p.photo_id).unresolved; }) : photos;
  var id = reviewQueue.session.photoId;
  var photo = reviewQueue.items.find(function(p) { return p.photo_id === id; }) || reviewQueue.items[0];
  reviewQueue.session.photoId = photo?.photo_id || null;
  rqRenderProgress();
  if (!photo) {
    reviewQueue.current = null;
    ++reviewQueue.sequence;
    rqClearCrop();
    /** @type {HTMLImageElement} */ (rqElement('rqFull')).removeAttribute('src');
    rqElement('rqFilename').textContent = 'No photos in this inspection queue.';
    rqElement('rqFlag').textContent = '';
    rqElement('rqAnalysis').textContent = '';
    rqElement('rqCropStatus').textContent = '';
    return;
  }
  if (!reviewQueue.current || reviewQueue.current.id !== photo.photo_id) rqOpenPhoto(photo);
}

function rqRenderProgress() {
  rqShowPendingFlag();
  var counts = rqProgress();
  var index = reviewQueue.items.findIndex(function(p) { return p.photo_id === reviewQueue.session.photoId; });
  rqElement('rqProgress').textContent = 'Position ' + (index + 1) + ' of ' + reviewQueue.items.length +
    ' photos · scope: ' + counts.total + ' photos with matching predictions · inspected here: ' + counts.inspected +
    ' · decided: ' + counts.decided + ' (catalog keep/reject flags at last refresh) · needs follow-up here: ' + counts.unresolved +
    '. Inspected and follow-up counts are browser-local; follow-up can overlap a prior decision.';
  /** @type {HTMLInputElement} */ (rqElement('rqUnresolvedOnly')).checked = reviewQueue.session.followUpOnly;
  /** @type {HTMLInputElement} */ (rqElement('rqUnresolved')).checked = !!(reviewQueue.session.photoId && rqItem(reviewQueue.session.photoId).unresolved);
  /** @type {HTMLButtonElement} */ (rqElement('rqPrevious')).disabled = reviewQueue.busy || index <= 0;
  /** @type {HTMLButtonElement} */ (rqElement('rqNext')).disabled = reviewQueue.busy || index >= reviewQueue.items.length - 1;
  ['rqInspected', 'rqUnresolved', 'rqSubject', 'rqSource', 'rqManual', 'rqResetCenter'].forEach(function(id) {
    /** @type {HTMLButtonElement} */ (rqElement(id)).disabled = reviewQueue.busy || index < 0;
  });
  document.querySelectorAll('[data-rq-flag]').forEach(function(button) {
    /** @type {HTMLButtonElement} */ (button).disabled = reviewQueue.busy || !reviewQueue.current;
  });
}

/** @param {ReviewPrediction} photo */
async function rqOpenPhoto(photo) {
  var sequence = ++reviewQueue.sequence;
  reviewQueue.current = null;
  reviewQueue.subjects = [];
  rqClearCrop();
  rqSave();
  rqElement('rqFilename').textContent = photo.filename;
  rqElement('rqFlag').textContent = 'Loading current catalog flag…';
  rqElement('rqAnalysis').textContent = 'Loading retained subjects…';
  rqElement('rqCropStatus').textContent = '';
  var full = /** @type {HTMLImageElement} */ (rqElement('rqFull'));
  full.src = '/photos/' + photo.photo_id + '/preview?size=1920&view=source';
  rqElement('rqFullStatus').textContent = 'Display preview; pixel provenance is reported separately for the native crop.';
  rqRenderProgress();
  try {
    var results = await Promise.allSettled([
      safeFetch('/api/photos/' + photo.photo_id, {}, {toast: false}),
      safeFetch('/api/photos/' + photo.photo_id + '/subjects', {}, {toast: false}),
    ]);
    if (sequence !== reviewQueue.sequence || _predictionsReloading) return;
    var detail = results[0];
    if (!detail || detail.status === 'rejected') throw new Error('Current catalog state is unavailable. Refresh before changing flags.');
    reviewQueue.current = /** @type {ReviewQueuePhoto} */ (detail.value);
    var subjects = results[1];
    if (subjects?.status === 'fulfilled') reviewQueue.subjects = /** @type {{subjects:ReviewQueueSubject[]}} */ (subjects.value).subjects || [];
    var item = rqItem(photo.photo_id);
    if (item.subject === null) item.subject = reviewQueue.subjects.find(function(s) { return s.is_primary; })?.id || null;
    allPredictions.forEach(function(p) { if (p.photo_id === photo.photo_id) p.flag = reviewQueue.current?.flag || 'none'; });
    rqShowFlag(); rqRenderSubjects(); rqRenderProgress(); rqSave(); rqLoadCrop();
    if (subjects?.status === 'rejected') rqElement('rqAnalysis').textContent = 'Subject data could not be loaded. Use a manual center; no analysis availability can be inferred.';
  } catch (error) {
    if (sequence !== reviewQueue.sequence) return;
    rqElement('rqFlag').textContent = reviewErrorMessage(error, 'Could not load photo. Refresh to retry.');
  }
}

function rqShowFlag() {
  var photo = reviewQueue.current;
  if (!photo) return;
  rqElement('rqFlag').textContent = 'Current catalog flag: ' + (photo.flag || 'none') +
    (photo.flag_note?.description ? ' · ' + photo.flag_note.description : '') + '. Changes require an explicit click; navigation preserves this flag.';
}

function rqShowPendingFlag() {
  var id = reviewQueue.session.photoId;
  var pending = id ? reviewQueue.session.items[String(id)]?.pendingFlag : null;
  var label = pending === 'flagged' ? 'Keep photo' : pending === 'rejected' ? 'Reject photo' : 'Clear photo flag';
  rqElement('rqPendingFlag').textContent = pending ? 'Unsaved review choice: ' + label +
    '. Not saved to the catalog. After the transfer finishes, refresh catalog decisions and click a flag button to confirm. Nothing is retried automatically.' : '';
  rqElement('rqDiscardPendingFlag').hidden = !pending;
  /** @type {HTMLButtonElement} */ (rqElement('rqDiscardPendingFlag')).disabled = reviewQueue.busy;
}

/** @param {number} offset */
function rqMove(offset) {
  if (reviewQueue.busy || _predictionsReloading) return;
  var index = reviewQueue.items.findIndex(function(p) { return p.photo_id === reviewQueue.session.photoId; });
  var photo = reviewQueue.items[index + offset];
  if (photo) { reviewQueue.session.photoId = photo.photo_id; rqSave(); renderReviewQueue(); }
}

/** @param {string} flag */
async function rqSetFlag(flag) {
  var photo = reviewQueue.current;
  if (!photo || reviewQueue.busy || _predictionsReloading) return;
  var sequence = reviewQueue.sequence;
  var session = reviewQueue.session, key = reviewQueue.key;
  reviewQueue.busy = true; rqRenderProgress();
  try {
    /** @type {{ok?:boolean}} */ var confirmed = await safeFetch('/api/photos/' + photo.id + '/flag', {method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({flag: flag, observed_flag: photo.flag || 'none', observed_workspace_id: reviewQueue.workspace})}, {toast: false});
    if (!confirmed || confirmed.ok !== true) throw new Error('The server did not confirm the flag change. Refresh before trying again.');
    delete session.items[String(photo.id)]?.pendingFlag;
    if (key) rqWrite(key, session);
    if (sequence !== reviewQueue.sequence) return;
    photo.flag = flag; photo.flag_note = null;
    var photoId = photo.id;
    allPredictions.forEach(function(p) { if (p.photo_id === photoId) p.flag = flag; });
    rqItem(photo.id).inspected = true;
    rqShowFlag(); rqSave();
    if (_clearPredictionRepresentativeStateIfIneligible(photo.id, flag)) renderAll();
  } catch (error) {
    if (error && typeof error === 'object' && 'code' in error && error.code === 'workspace_busy' &&
        (flag === 'flagged' || flag === 'rejected' || flag === 'none')) {
      var item = session.items[String(photo.id)] || (session.items[String(photo.id)] =
        {inspected: false, unresolved: false, subject: null, center: null});
      item.pendingFlag = flag;
      if (key) rqWrite(key, session);
    }
    if (sequence !== reviewQueue.sequence) return;
    rqElement('rqFlag').textContent = reviewErrorMessage(error, 'Flag was not saved. Refresh the catalog and retry.');
    // Keep unsaved intent separate from the catalog; refresh before another explicit write.
    reviewQueue.current = null;
  } finally { reviewQueue.busy = false; rqRenderProgress(); }
}

function bindReviewQueue() {
  rqElement('rqEnabled').addEventListener('change', function(event) {
    reviewQueue.enabled = /** @type {HTMLInputElement} */ (event.target).checked;
    renderReviewQueue(); rqSave();
  });
  rqElement('rqPrevious').addEventListener('click', function() { rqMove(-1); });
  rqElement('rqNext').addEventListener('click', function() { rqMove(1); });
  rqElement('rqRefresh').addEventListener('click', function() { reviewQueue.current = null; ++reviewQueue.sequence; switchCollection(currentCollection); });
  rqElement('rqDiscardPendingFlag').addEventListener('click', function() {
    if (reviewQueue.busy || !reviewQueue.session.photoId) return;
    delete rqItem(reviewQueue.session.photoId).pendingFlag;
    rqSave(); rqShowPendingFlag();
  });
  rqElement('rqInspected').addEventListener('click', function() {
    if (!reviewQueue.current) return;
    rqItem(reviewQueue.current.id).inspected = true; rqSave(); rqRenderProgress();
  });
  rqElement('rqUnresolved').addEventListener('change', function(event) {
    if (!reviewQueue.current) return;
    var item = rqItem(reviewQueue.current.id);
    item.unresolved = /** @type {HTMLInputElement} */ (event.target).checked;
    item.inspected = true; rqSave(); renderReviewQueue();
  });
  rqElement('rqUnresolvedOnly').addEventListener('change', function(event) {
    reviewQueue.session.followUpOnly = /** @type {HTMLInputElement} */ (event.target).checked;
    rqSave(); renderReviewQueue();
  });
  rqElement('rqSubject').addEventListener('change', function(event) {
    if (!reviewQueue.current) return;
    var item = rqItem(reviewQueue.current.id);
    item.subject = Number(/** @type {HTMLSelectElement} */ (event.target).value) || null;
    item.center = null; rqSave(); rqRenderSubjects(); rqLoadCrop();
  });
  rqElement('rqSource').addEventListener('change', function(event) {
    reviewQueue.source = /** @type {HTMLSelectElement} */ (event.target).value;
    rqSave(); rqLoadCrop();
  });
  rqElement('rqManual').addEventListener('click', function() {
    reviewQueue.manual = !reviewQueue.manual;
    rqElement('rqManual').setAttribute('aria-pressed', String(reviewQueue.manual));
    rqElement('rqFullStatus').textContent = reviewQueue.manual ? 'Click the eye or animal in the full frame to center the native crop.' : 'Manual center placement off.';
  });
  rqElement('rqResetCenter').addEventListener('click', function() {
    if (!reviewQueue.current) return;
    rqItem(reviewQueue.current.id).center = null; rqSave(); rqRenderSubjects(); rqLoadCrop();
  });
  rqElement('rqFull').addEventListener('click', rqPlaceCenter);
  rqElement('rqFull').addEventListener('error', function() { rqElement('rqFullStatus').textContent = 'Full frame unavailable. Manual placement requires this upright source preview.'; });
  document.querySelectorAll('[data-rq-flag]').forEach(function(button) {
    button.addEventListener('click', function() { rqSetFlag(/** @type {HTMLElement} */ (button).dataset.rqFlag || 'none'); });
  });
  window.addEventListener('pagehide', rqSave);
}
