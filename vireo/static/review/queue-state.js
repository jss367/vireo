// Browser-local inspection bookkeeping never stores or replays catalog decisions.
/** @type {ReviewQueueState} */
var reviewQueue = {workspace: null, key: '', session: {photoId: null, items: {}, followUpOnly: false},
  enabled: false, items: [], current: null, subjects: [], sequence: 0, cropSequence: 0,
  savedRules: null, savedVisual: null, source: 'auto', manual: false, busy: false, storageError: false};

/** @param {string} id @returns {HTMLElement} */
function rqElement(id) { return /** @type {HTMLElement} */ (document.getElementById(id)); }

/** @param {string} key @returns {unknown} */
function rqRead(key) {
  try { return JSON.parse(localStorage.getItem(key) || 'null'); }
  catch (_error) { return null; }
}

/** @param {string} key @param {unknown} value */
function rqWrite(key, value) {
  try { localStorage.setItem(key, JSON.stringify(value)); }
  catch (_error) {
    reviewQueue.storageError = true;
    rqElement('rqStorageStatus').textContent = 'Browser storage is unavailable. Inspection works, but this session cannot resume after reload.';
  }
}

/** @param {unknown} value @returns {ReviewQueueSession} */
function rqSession(value) {
  var session = /** @type {Partial<ReviewQueueSession>|null} */ (value);
  var items = /** @type {Record<string, ReviewQueueItemState>} */ ({});
  if (session && session.items && typeof session.items === 'object') {
    Object.entries(session.items).forEach(function(entry) {
      var item = entry[1];
      if (!/^\d+$/.test(entry[0]) || !item || typeof item !== 'object') return;
      items[entry[0]] = {inspected: item.inspected === true, unresolved: item.unresolved === true,
        subject: Number.isSafeInteger(item.subject) ? item.subject : null,
        center: Array.isArray(item.center) && item.center.length === 2 &&
          item.center.every(function(v) { return typeof v === 'number' && Number.isFinite(v) && v >= 0 && v <= 1; }) ? item.center : null};
    });
  }
  return {photoId: Number.isSafeInteger(session?.photoId) ? session?.photoId || null : null,
    items: items, followUpOnly: session?.followUpOnly === true};
}

function rqViewSettings() {
  return {collection: currentCollection, tab: currentTab, model: currentModel, sort: currentSort,
    evidence: reviewEvidenceFilter, confidence: minConfidence, fingerprint: currentLabelsFingerprint, photoId: currentPhotoIdFilter,
    enabled: reviewQueue.enabled, source: reviewQueue.source,
    rules: VireoFilter.getRules(), visual: VireoFilter.getVisual()};
}

function rqScopeKey() {
  return 'vireo:review-queue:' + reviewQueue.workspace + ':' + JSON.stringify({
    collection: currentCollection, tab: currentTab, model: currentModel, evidence: reviewEvidenceFilter, confidence: minConfidence,
    fingerprint: currentLabelsFingerprint, photoId: currentPhotoIdFilter,
    rules: VireoFilter.getRules(), visual: VireoFilter.getVisual(),
  });
}

function rqSave() {
  if (reviewQueue.workspace === null) return;
  if (reviewQueue.key) rqWrite(reviewQueue.key, reviewQueue.session);
  rqWrite('vireo:review-view:' + reviewQueue.workspace, rqViewSettings());
}

async function initializeReviewQueue() {
  /** @type {{id:number}} */ var workspace = await safeFetch('/api/workspaces/active', {}, {toast: false});
  reviewQueue.workspace = workspace.id;
  var view = /** @type {Partial<ReturnType<typeof rqViewSettings>>|null} */ (rqRead('vireo:review-view:' + workspace.id));
  if (!view) return;
  if (Array.isArray(view.rules) || (view.rules && Array.isArray(view.rules.rules))) {
    reviewQueue.savedRules = view.rules;
    reviewQueue.savedVisual = view.visual || null;
  }
  currentCollection = typeof view.collection === 'string' ? view.collection : 'all';
  currentTab = ['all', 'pending', 'accepted', 'rejected'].includes(view.tab || '') ? view.tab || 'all' : 'all';
  reviewEvidenceFilter = ['all', 'questionable', 'deferred'].includes(view.evidence || '') ? view.evidence || 'all' : 'all';
  /** @type {HTMLSelectElement} */ (rqElement('reviewEvidenceFilter')).value = reviewEvidenceFilter;
  currentModel = typeof view.model === 'string' ? view.model : 'all';
  currentSort = typeof view.sort === 'string' ? view.sort : 'filename';
  minConfidence = typeof view.confidence === 'number' && view.confidence >= 0 && view.confidence <= 1 ? view.confidence : 0;
  currentLabelsFingerprint = typeof view.fingerprint === 'string' ? view.fingerprint : null;
  currentPhotoIdFilter = Number.isSafeInteger(view.photoId) ? view.photoId || null : null;
  reviewQueue.enabled = view.enabled === true;
  reviewQueue.source = ['auto', 'working', 'original'].includes(view.source || '') ? view.source || 'auto' : 'auto';
  /** @type {HTMLSelectElement} */ (rqElement('sortSelect')).value = currentSort;
  /** @type {HTMLInputElement} */ (rqElement('confFilter')).value = String(minConfidence * 100);
  rqElement('confFilterVal').textContent = Math.round(minConfidence * 100) + '%';
}

function reviewQueueSelectCollection() {
  var select = /** @type {HTMLSelectElement} */ (rqElement('collectionFilter'));
  select.value = currentCollection;
  var option = select.selectedOptions[0];
  if (!option || option.disabled) {
    // Keep the missing scope closed; never silently broaden it to the workspace.
    select.add(new Option('Saved collection unavailable (' + currentCollection + ')', currentCollection, true, true));
  }
}

/** @param {number} id @returns {ReviewQueueItemState} */
function rqItem(id) {
  var key = String(id);
  return reviewQueue.session.items[key] || (reviewQueue.session.items[key] =
    {inspected: false, unresolved: false, subject: null, center: null});
}

function rqPhotos() {
  var seen = new Set();
  return getVisibleItems(true).filter(function(p) {
    if (seen.has(p.photo_id)) return false;
    seen.add(p.photo_id); return true;
  });
}

function rqProgress() {
  var photos = rqPhotos();
  return {total: photos.length,
    inspected: photos.filter(function(p) { return reviewQueue.session.items[String(p.photo_id)]?.inspected; }).length,
    decided: photos.filter(function(p) { return p.flag === 'flagged' || p.flag === 'rejected'; }).length,
    unresolved: photos.filter(function(p) { return reviewQueue.session.items[String(p.photo_id)]?.unresolved; }).length};
}

// Clear visible inspection snapshots as soon as a filter/catalog refresh starts.
function rqBeginReload() {
  _predictionsReloading = true;
  ++reviewQueue.sequence;
  reviewQueue.current = null;
  rqClearCrop();
}

// Snapshot the effective expression alongside the photo anchor. A delayed
// workspace UI-state write must not send a resumed queue to a different scope.
function rqRestoreFilters() {
  var params = new URLSearchParams(window.location.search);
  var explicitFilters = Array.from(params.keys()).some(function(key) {
    return !['model', 'labels_fingerprint', 'photo_id'].includes(key);
  });
  if (explicitFilters || reviewQueue.savedRules === null) return;
  if (JSON.stringify(VireoFilter.getRules()) === JSON.stringify(reviewQueue.savedRules) &&
      JSON.stringify(VireoFilter.getVisual()) === JSON.stringify(reviewQueue.savedVisual)) return;
  VireoFilter.loadExpression(reviewQueue.savedRules, reviewQueue.savedVisual);
}
