// Collection and keyword dialogs and their ownership state.
// Classic page script; shared globals are initialized before boot.js runs.

// Each organization dialog owns its own captured selection so opening one
// cannot silently overwrite the photos the other is about to submit, and
// closing one cannot clear the selection the other is still using.
var pipelineReviewCollectionPhotoIds = [];
var pipelineReviewKeywordPhotoIds = [];
var pipelineReviewCollections = [];
var pipelineReviewSelectedCollectionId = null;
var pipelineReviewCollectionSubmitPromise = null;
var pipelineReviewCreatedCollection = null;
var pipelineReviewCollectionLoadSeq = 0;
var pipelineReviewKeywordSubmitPromise = null;
var pipelineReviewActiveOrganizationModal = null;

function pipelineReviewAnyOrganizationModalOpen() {
  var collection = document.getElementById('pipelineCollectionModal');
  var keyword = document.getElementById('pipelineKeywordModal');
  return !!((collection && collection.classList.contains('open')) ||
    (keyword && keyword.classList.contains('open')));
}

function pipelineReviewActivateOrganizationModal(kind) {
  var collection = document.getElementById('pipelineCollectionModal');
  var keyword = document.getElementById('pipelineKeywordModal');
  pipelineReviewActiveOrganizationModal = kind;
  if (collection) collection.style.zIndex = kind === 'collection' ? '551' : '550';
  if (keyword) keyword.style.zIndex = kind === 'keyword' ? '551' : '550';
}

function pipelineReviewDeactivateOrganizationModal(kind) {
  if (pipelineReviewActiveOrganizationModal !== kind) return;
  var other = kind === 'collection' ? 'keyword' : 'collection';
  var otherEl = document.getElementById(
    other === 'collection' ? 'pipelineCollectionModal' : 'pipelineKeywordModal'
  );
  if (otherEl && otherEl.classList.contains('open')) {
    pipelineReviewActivateOrganizationModal(other);
  } else {
    pipelineReviewActiveOrganizationModal = null;
  }
}

function pipelineReviewUniquePhotoIds(photoIds) {
  var seen = {};
  return (photoIds || []).map(function(id) { return parseInt(id, 10); }).filter(function(id) {
    if (!id || seen[id]) return false;
    seen[id] = true;
    return true;
  });
}

function pipelineCollectionAcceptsManualPhotos(collection) {
  if (typeof collection.can_add_photos === 'boolean') return collection.can_add_photos;
  try {
    var rules = typeof collection.rules === 'string'
      ? JSON.parse(collection.rules)
      : collection.rules;
    return Array.isArray(rules) && rules.length === 1 && rules[0].field === 'photo_ids';
  } catch(e) { return false; }
}

async function addToCollection(photoIds) {
  if (pipelineReviewCollectionSubmitPromise) return false;
  if (isScopedReviewView()) {
    window._vireoNativeMenuPhotoIdsOverride = null;
    notifyReadOnlyScopedView();
    return false;
  }
  var ids = pipelineReviewUniquePhotoIds(photoIds || getActiveSelection());
  if (!ids.length) return;
  // Native menu dispatch pins its selection only long enough for this action
  // to capture it. The dialog owns `ids` from here on; leaving the override
  // active would make unrelated native commands target this stale selection.
  window._vireoNativeMenuPhotoIdsOverride = null;
  var loadSeq = ++pipelineReviewCollectionLoadSeq;
  var collections;
  try {
    collections = await safeFetch('/api/collections', {}, {toast: false});
  } catch(e) {
    if (loadSeq !== pipelineReviewCollectionLoadSeq) return false;
    // The native Photo menu pins its selection while this asynchronous modal
    // opens. If loading fails before there is a modal to close, release that
    // override here so later native actions use the current page selection —
    // but only when no other organization dialog is still relying on it.
    pipelineReviewCollectionPhotoIds = [];
    if (!pipelineReviewAnyOrganizationModalOpen()) {
      window._vireoNativeMenuPhotoIdsOverride = null;
    }
    return false;
  }
  // A newer invocation owns the shared modal state. Ignore this stale
  // response rather than opening a dialog that mixes its title/list with the
  // newer invocation's selected photo IDs.
  if (loadSeq !== pipelineReviewCollectionLoadSeq) return false;
  pipelineReviewCollectionPhotoIds = ids;
  pipelineReviewSelectedCollectionId = null;
  pipelineReviewCreatedCollection = null;
  pipelineReviewCollections = (collections || []).filter(pipelineCollectionAcceptsManualPhotos);
  document.getElementById('pipelineCollectionTitle').textContent =
    'Add ' + ids.length + ' photo' + (ids.length === 1 ? '' : 's') + ' to Collection';
  var listHtml = pipelineReviewCollections.map(function(collection) {
    return '<button type="button" class="pipeline-collection-choice" data-id="' + collection.id +
      '" onclick="pickPipelineCollection(' + collection.id + ')" style="display:block;width:100%;padding:7px 10px;text-align:left;cursor:pointer;border:0;border-radius:4px;background:transparent;color:var(--text-primary);font-size:13px;">' +
      escapeHtml(collection.name) + '</button>';
  }).join('');
  if (!listHtml) {
    listHtml = '<div style="padding:6px 10px;color:var(--text-dim);font-size:12px;">No manual collections yet. Enter a name below to create one.</div>';
  }
  document.getElementById('pipelineCollectionList').innerHTML = listHtml;
  document.getElementById('pipelineCollectionNewName').value = '';
  document.getElementById('pipelineCollectionModal').classList.add('open');
  pipelineReviewActivateOrganizationModal('collection');
  setTimeout(function() { document.getElementById('pipelineCollectionNewName').focus(); }, 50);
  return true;
}

function pickPipelineCollection(collectionId) {
  pipelineReviewSelectedCollectionId = collectionId;
  pipelineReviewCreatedCollection = null;
  document.getElementById('pipelineCollectionNewName').value = '';
  document.querySelectorAll('.pipeline-collection-choice').forEach(function(el) {
    el.style.background = parseInt(el.dataset.id, 10) === collectionId ? 'var(--bg-tertiary)' : 'transparent';
  });
}

function hidePipelineCollectionModal(force) {
  if (pipelineReviewCollectionSubmitPromise && force !== true) return false;
  document.getElementById('pipelineCollectionModal').classList.remove('open');
  pipelineReviewDeactivateOrganizationModal('collection');
  pipelineReviewCollectionPhotoIds = [];
  pipelineReviewSelectedCollectionId = null;
  pipelineReviewCreatedCollection = null;
  // Preserve the native selection override while the keyword dialog is still
  // open, so its own confirmation reads the pinned photo IDs.
  if (!pipelineReviewAnyOrganizationModalOpen()) {
    window._vireoNativeMenuPhotoIdsOverride = null;
  }
  return true;
}

function setPipelineCollectionSubmitting(submitting) {
  var btn = document.getElementById('pipelineCollectionSubmitBtn');
  if (btn) btn.disabled = submitting;
  var cancel = document.getElementById('pipelineCollectionCancelBtn');
  if (cancel) cancel.disabled = submitting;
  var input = document.getElementById('pipelineCollectionNewName');
  if (input) input.disabled = submitting;
}

function confirmPipelineCollection() {
  if (pipelineReviewCollectionSubmitPromise) return pipelineReviewCollectionSubmitPromise;
  if (isScopedReviewView()) {
    notifyReadOnlyScopedView();
    return Promise.resolve(false);
  }
  var ids = pipelineReviewCollectionPhotoIds.slice();
  var newName = document.getElementById('pipelineCollectionNewName').value.trim();
  var collectionId = pipelineReviewSelectedCollectionId;
  var collectionName = '';
  if (!ids.length || (!newName && collectionId == null)) return Promise.resolve(false);

  setPipelineCollectionSubmitting(true);
  pipelineReviewCollectionSubmitPromise = (async function() {
    if (newName) {
      if (pipelineReviewCreatedCollection && pipelineReviewCreatedCollection.name === newName) {
        collectionId = pipelineReviewCreatedCollection.id;
      } else {
        try {
          var created = await safeFetch('/api/collections', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
              name: newName,
              rules: [{field: 'photo_ids', value: []}],
            }),
          });
          collectionId = created.id;
          pipelineReviewCreatedCollection = {id: collectionId, name: newName};
        } catch(e) { return false; }
      }
      collectionName = newName;
    } else {
      var match = pipelineReviewCollections.find(function(collection) {
        return collection.id === collectionId;
      });
      collectionName = match ? match.name : 'collection';
    }

    try {
      await safeFetch('/api/collections/' + collectionId + '/add-photos', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({photo_ids: ids}),
      });
    } catch(e) { return false; }
    hidePipelineCollectionModal(true);
    showToast('Added ' + ids.length + ' photo' + (ids.length === 1 ? '' : 's') +
      ' to "' + collectionName + '"', 'success');
    return true;
  })().finally(function() {
    pipelineReviewCollectionSubmitPromise = null;
    setPipelineCollectionSubmitting(false);
  });
  return pipelineReviewCollectionSubmitPromise;
}

function batchAddKeyword(photoIds) {
  if (pipelineReviewKeywordSubmitPromise) return false;
  if (isScopedReviewView()) {
    window._vireoNativeMenuPhotoIdsOverride = null;
    notifyReadOnlyScopedView();
    return false;
  }
  var ids = pipelineReviewUniquePhotoIds(photoIds || getActiveSelection());
  if (!ids.length) return;
  pipelineReviewKeywordPhotoIds = ids;
  window._vireoNativeMenuPhotoIdsOverride = null;
  document.getElementById('pipelineKeywordTitle').textContent =
    'Add Keyword to ' + ids.length + ' Photo' + (ids.length === 1 ? '' : 's');
  document.getElementById('pipelineKeywordInput').value = '';
  document.getElementById('pipelineKeywordModal').classList.add('open');
  pipelineReviewActivateOrganizationModal('keyword');
  setTimeout(function() { document.getElementById('pipelineKeywordInput').focus(); }, 50);
}

function hidePipelineKeywordModal(force) {
  if (pipelineReviewKeywordSubmitPromise && force !== true) return false;
  document.getElementById('pipelineKeywordModal').classList.remove('open');
  pipelineReviewDeactivateOrganizationModal('keyword');
  pipelineReviewKeywordPhotoIds = [];
  // Preserve the native selection override while the collection dialog is
  // still open, so its own confirmation reads the pinned photo IDs.
  if (!pipelineReviewAnyOrganizationModalOpen()) {
    window._vireoNativeMenuPhotoIdsOverride = null;
  }
  return true;
}

function setPipelineKeywordSubmitting(submitting) {
  var submit = document.getElementById('pipelineKeywordSubmitBtn');
  if (submit) submit.disabled = submitting;
  var cancel = document.getElementById('pipelineKeywordCancelBtn');
  if (cancel) cancel.disabled = submitting;
  var input = document.getElementById('pipelineKeywordInput');
  if (input) input.disabled = submitting;
}

function confirmPipelineKeyword() {
  if (pipelineReviewKeywordSubmitPromise) return pipelineReviewKeywordSubmitPromise;
  if (isScopedReviewView()) {
    notifyReadOnlyScopedView();
    return Promise.resolve(false);
  }
  var ids = pipelineReviewKeywordPhotoIds.slice();
  var name = document.getElementById('pipelineKeywordInput').value.trim();
  if (!ids.length || !name) return Promise.resolve(false);
  setPipelineKeywordSubmitting(true);
  pipelineReviewKeywordSubmitPromise = safeFetch('/api/batch/keyword', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_ids: ids, name: name}),
    }).then(function() {
      hidePipelineKeywordModal(true);
      showToast('Added "' + name + '" to ' + ids.length + ' photo' +
        (ids.length === 1 ? '' : 's'), 'success');
      return true;
    }).catch(function() {
      return false;
    }).finally(function() {
      pipelineReviewKeywordSubmitPromise = null;
      setPipelineKeywordSubmitting(false);
    });
  return pipelineReviewKeywordSubmitPromise;
}

function bindPipelineReviewOrganizationKeyboard() {
    document.addEventListener('keydown', function(e) {
      if (e.key !== 'Escape') return;
      if (pipelineReviewActiveOrganizationModal === 'keyword' &&
          document.getElementById('pipelineKeywordModal').classList.contains('open')) {
        hidePipelineKeywordModal();
        e.preventDefault();
        return;
      }
      if (pipelineReviewActiveOrganizationModal === 'collection' &&
          document.getElementById('pipelineCollectionModal').classList.contains('open')) {
        hidePipelineCollectionModal();
        e.preventDefault();
        return;
      }
      if (document.getElementById('pipelineCollectionModal').classList.contains('open')) {
        hidePipelineCollectionModal();
        e.preventDefault();
        return;
      }
      if (document.getElementById('pipelineKeywordModal').classList.contains('open')) {
        hidePipelineKeywordModal();
        e.preventDefault();
      }
    });
}
