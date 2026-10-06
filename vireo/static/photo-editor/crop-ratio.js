// The remembered crop ratio, its revisions, and cross-tab sync via localStorage.
// Classic page script; load boot.js after all definitions.

var cropRatioPreference = {enabled: false, aspect: null};
var cropRatioSave = Promise.resolve();

var COMMITTED_CROP_RATIO_KEY = 'vireo_committed_crop_ratio';
var MAX_CROP_RATIO_REVISION = Number.MAX_SAFE_INTEGER;

function isValidCropRatioPreference(value) {
  return value && typeof value.enabled === 'boolean' &&
    Number.isSafeInteger(value.revision) && value.revision > 0 &&
    (value.aspect === null || (Number.isFinite(value.aspect) && value.aspect > 0));
}

function cropRatioRevisionAtCeiling(revision) {
  // The server accepts a lower rollover write once the stored counter reaches
  // the safe-integer ceiling; the client's monotonic guards must match, or a
  // stale cached snapshot outranks the freshly saved rollover value and
  // reverts the preference on the next navigation.
  return Number(revision) >= MAX_CROP_RATIO_REVISION;
}

function pendingCropRatioRecords() {
  var records = [];
  try {
    Object.keys(localStorage).filter(function(key) {
      return key.indexOf('vireo_pending_crop_ratio:') === 0;
    }).forEach(function(key) {
      try {
        var pending = JSON.parse(VireoViewPreferences.read(key));
        if (isValidCropRatioPreference(pending)) {
          records.push({key: key, preference: pending});
        }
      } catch (_) {}
    });
  } catch (_) {}
  return records;
}

function readCommittedCropRatioPreference() {
  try {
    var raw = VireoViewPreferences.read(COMMITTED_CROP_RATIO_KEY);
    if (!raw) return null;
    var committed = JSON.parse(raw);
    return isValidCropRatioPreference(committed) ? committed : null;
  } catch (_) { return null; }
}

function writeCommittedCropRatioPreference(preference) {
  if (!isValidCropRatioPreference(preference)) return;
  var existing = readCommittedCropRatioPreference();
  // Once the cached revision reaches the ceiling, any subsequent write is a
  // rollover the server has already accepted; the previous snapshot is stale
  // regardless of its numeric revision, so replace it.
  if (existing && existing.revision >= preference.revision &&
      !cropRatioRevisionAtCeiling(existing.revision)) return;
  try {
    VireoViewPreferences.write(COMMITTED_CROP_RATIO_KEY, JSON.stringify({
      enabled: !!preference.enabled,
      aspect: preference.aspect === null ? null : Number(preference.aspect),
      revision: preference.revision,
    }));
  } catch (_) {}
}

function adoptCommittedCropRatioPreference() {
  // Another tab may have persisted a newer preference after this tab's last
  // PUT resolved; that write leaves no callback in this tab, so line-of-sight
  // navigation would otherwise keep applying the stale in-memory value.
  var committed = readCommittedCropRatioPreference();
  if (!committed) return false;
  var current = cropRatioPreference.revision || 0;
  // The server accepts a lower rollover revision once the counter hits the
  // ceiling. When the in-memory revision is at the ceiling, treat any valid
  // committed snapshot as authoritative so a sibling tab's rollover writes
  // — or a settings-import restoration — are not ignored as older.
  var accept = committed.revision > current || cropRatioRevisionAtCeiling(current);
  if (!accept) return false;
  cropRatioPreference = committed;
  updateAspectButtons();
  return true;
}

function pendingCropRatioPreference() {
  return pendingCropRatioRecords().reduce(function(latest, record) {
    return !latest || record.preference.revision > latest.revision ? record.preference : latest;
  }, null);
}

function acknowledgeCropRatioPreference(revision) {
  // Each write owns an immutable key. Another tab can add a newer record
  // between this read and removal without having its pending value erased.
  pendingCropRatioRecords().forEach(function(record) {
    if (record.preference.revision <= revision) {
      try { localStorage.removeItem(record.key); } catch (_) {}
    }
  });
}

function saveCropRatioPreference(preference) {
  // Issue every write before teardown can discard JavaScript callbacks.
  // The server compares revisions so a slow older request cannot win.
  var request = safeFetch('/api/editor/crop-ratio', {
    method: 'PUT',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(preference),
    keepalive: true,
  }, {toast: false}).then(function(saved) {
    // Another tab may have won with a newer (or tied) revision. Adopt the
    // persisted preference for future photos without recropping this one.
    if (saved.revision >= (cropRatioPreference.revision || 0)) {
      cropRatioPreference = saved;
      updateAspectButtons();
    }
    // Publish the accepted revision so other editor tabs adopt it via the
    // storage event instead of relying on their own PUT responses.
    writeCommittedCropRatioPreference(saved);
    acknowledgeCropRatioPreference(saved.revision);
  }).catch(function() {
    showToast('Could not remember the crop ratio. Please try again.', 'error');
  });
  cropRatioSave = Promise.all([cropRatioSave, request]);
}

function nextCropRatioRevision(previous) {
  // A previous revision at Number.MAX_SAFE_INTEGER has no safe successor.
  // Fall back to Date.now() so future writes stay within safe-integer
  // range; the server accepts this rollover instead of rejecting it as
  // stale.
  var current = Number(previous) || 0;
  // Sibling tabs starting from the same server revision in this Date.now()
  // tick would otherwise compute the same next revision. Their pending
  // records live in shared localStorage; take the max so this write
  // outranks whatever they queued instead of colliding on equal revisions.
  pendingCropRatioRecords().forEach(function(record) {
    if (record.preference.revision > current) current = record.preference.revision;
  });
  var candidate = current + 1;
  if (!Number.isSafeInteger(candidate)) candidate = Date.now();
  var next = Math.max(Date.now(), candidate);
  return Number.isSafeInteger(next) ? next : candidate;
}

function rememberCropRatio(force) {
  if (!cropRatioPreference.enabled && !force) return;
  cropRatioPreference.aspect = cropRatioPreference.enabled ? editorState.cropAspect : null;
  cropRatioPreference.revision = nextCropRatioRevision(cropRatioPreference.revision);
  // A reloaded page can issue its GET before this PUT reaches the server.
  // Keep the pending value synchronously until its revision is acknowledged.
  var writeId = window.crypto && typeof crypto.randomUUID === 'function'
    ? crypto.randomUUID() : Date.now() + ':' + Math.random().toString(36).slice(2);
  VireoViewPreferences.write('vireo_pending_crop_ratio:' + writeId,
    JSON.stringify(cropRatioPreference));
  saveCropRatioPreference(cropRatioPreference);
}

function setRememberCropRatio(enabled) {
  cropRatioPreference.enabled = enabled;
  rememberCropRatio(true);
}
