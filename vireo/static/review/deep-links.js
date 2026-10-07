// URL deep links (?model=, ?labels_fingerprint=, ?photo_id=) and their filter pills.
// Classic page script; load boot.js after all definitions.

function applyReviewQueryParams() {
  var qs = new URLSearchParams(window.location.search);
  var qsModel = qs.get('model');
  var qsFp = qs.get('labels_fingerprint');
  if (qsModel) currentModel = qsModel;
  if (qsFp) currentLabelsFingerprint = qsFp;
  // Browse's prediction panel sends ambiguous predictions here rather than
  // offering a bare Accept it can't honour. Landing on the full workspace
  // queue would make the user hunt for the photo they just clicked, so the
  // deep link narrows Review to it.
  var qsPhoto = parseInt(qs.get('photo_id'), 10);
  if (!isNaN(qsPhoto)) currentPhotoIdFilter = qsPhoto;
}

// Filter pills go in #reviewFilterPills, the row under the action bar. They
// used to be placed after a `.toolbar` this page doesn't have, which dropped
// them at the end of <body>, behind the bottom-panel toggle, where "show all ×"
// couldn't be clicked and the user was stuck in the narrowed view. The body
// fallback must stay an *append into* the body, never a sibling of it (a
// sibling of <body> never renders): a silent no-op is exactly the failure mode
// these pills exist to prevent.
function insertFilterPill(pill) {
  var row = document.getElementById('reviewFilterPills');
  (row || document.body).appendChild(pill);
}

function renderFingerprintFilterPill() {
  var existing = document.getElementById('fpFilterPill');
  if (existing) existing.remove();
  if (!currentLabelsFingerprint) return;
  var pill = document.createElement('div');
  pill.id = 'fpFilterPill';
  pill.style.cssText = 'display:inline-flex;align-items:center;gap:6px;padding:3px 10px;' +
    'border-radius:12px;background:color-mix(in srgb,var(--accent) 15%,transparent);' +
    'color:var(--accent);font-size:12px;margin:6px 0;';
  pill.innerHTML = 'Filtered to fingerprint <code>' +
    escapeHtml(currentLabelsFingerprint.substring(0, 12)) +
    '</code> <a href="#" id="fpFilterClear" style="color:inherit;text-decoration:none;">×</a>';
  insertFilterPill(pill);
  document.getElementById('fpFilterClear').addEventListener('click', function(e) {
    e.preventDefault();
    currentLabelsFingerprint = null;
    var url = new URL(window.location.href);
    url.searchParams.delete('labels_fingerprint');
    window.history.replaceState({}, '', url.toString());
    pill.remove();
    renderAll();
  });
}

function renderPhotoFilterPill() {
  var existing = document.getElementById('photoFilterPill');
  if (existing) existing.remove();
  if (currentPhotoIdFilter == null) return;
  // Without this pill a one-photo queue reads as "Review is empty" — the
  // narrowing came from a deep link the user may not have noticed making.
  var pill = document.createElement('div');
  pill.id = 'photoFilterPill';
  pill.style.cssText = 'display:inline-flex;align-items:center;gap:6px;padding:3px 10px;' +
    'border-radius:12px;background:color-mix(in srgb,var(--accent) 15%,transparent);' +
    'color:var(--accent);font-size:12px;margin:6px 0;';
  pill.innerHTML = 'Showing one photo from Browse ' +
    '<a href="#" id="photoFilterClear" style="color:inherit;text-decoration:none;">' +
    'show all ×</a>';
  insertFilterPill(pill);
  document.getElementById('photoFilterClear').addEventListener('click', function(e) {
    e.preventDefault();
    currentPhotoIdFilter = null;
    var url = new URL(window.location.href);
    url.searchParams.delete('photo_id');
    window.history.replaceState({}, '', url.toString());
    pill.remove();
    loadPredictions();
  });
}
