// Collection and keyword dialogs and their ownership state.
// Classic page script; shared globals are initialized before boot.js runs.

// Each organization dialog owns its own captured selection so opening one
// cannot silently overwrite the photos the other is about to submit, and
// closing one cannot clear the selection the other is still using.
/** @type {number[]} */ var pipelineReviewCollectionPhotoIds = [];
/** @type {number[]} */ var pipelineReviewKeywordPhotoIds = [];
/** @type {ReviewCollection[]} */ var pipelineReviewCollections = [];
/** @type {number | null} */ var pipelineReviewSelectedCollectionId = null;
/** @type {Promise<unknown> | null} */ var pipelineReviewCollectionSubmitPromise = null;
/** @type {{id: number; name: string} | null} */ var pipelineReviewCreatedCollection = null;
var pipelineReviewCollectionLoadSeq = 0;
/** @type {Promise<unknown> | null} */ var pipelineReviewKeywordSubmitPromise = null;
/** @type {string | null} */ var pipelineReviewActiveOrganizationModal = null;

function pipelineReviewAnyOrganizationModalOpen() {
  var collection = /** @type {HTMLElement} */ (document.getElementById('pipelineCollectionModal'));
  var keyword = /** @type {HTMLElement} */ (document.getElementById('pipelineKeywordModal'));
  return !!((collection && collection.classList.contains('open')) ||
    (keyword && keyword.classList.contains('open')));
}

/** @param {string} kind */
function pipelineReviewActivateOrganizationModal(kind) {
  var collection = /** @type {HTMLElement} */ (document.getElementById('pipelineCollectionModal'));
  var keyword = /** @type {HTMLElement} */ (document.getElementById('pipelineKeywordModal'));
  pipelineReviewActiveOrganizationModal = kind;
  if (collection) collection.style.zIndex = kind === 'collection' ? '551' : '550';
  if (keyword) keyword.style.zIndex = kind === 'keyword' ? '551' : '550';
}

/** @param {string} kind */
function pipelineReviewDeactivateOrganizationModal(kind) {
  if (pipelineReviewActiveOrganizationModal !== kind) return;
  var other = kind === 'collection' ? 'keyword' : 'collection';
  var otherEl = /** @type {HTMLElement} */ (document.getElementById(
    other === 'collection' ? 'pipelineCollectionModal' : 'pipelineKeywordModal'
  ));
  if (otherEl && otherEl.classList.contains('open')) {
    pipelineReviewActivateOrganizationModal(other);
  } else {
    pipelineReviewActiveOrganizationModal = null;
  }
}

/** @param {number[]} photoIds */
function pipelineReviewUniquePhotoIds(photoIds) {
  /** @type {Record<number,boolean>} */ var seen = {};
  return (photoIds || []).map(function(id) { return parseInt(String(id), 10); }).filter(function(id) {
    if (!id || seen[id]) return false;
    seen[id] = true;
    return true;
  });
}

/** @param {ReviewCollection} collection */
function pipelineCollectionAcceptsManualPhotos(collection) {
  if (typeof collection.can_add_photos === 'boolean') return collection.can_add_photos;
  try {
    var rules = typeof collection.rules === 'string'
      ? JSON.parse(collection.rules)
      : collection.rules;
    return Array.isArray(rules) && rules.length === 1 && rules[0].field === 'photo_ids';
  } catch(e) { return false; }
}

/** @param {number[]} photoIds */
async function addToCollection(photoIds) {
  if (pipelineReviewCollectionSubmitPromise) return false;
  if (isScopedReviewView()) {
    window._vireoNativeMenuPhotoIdsOverride = null;
    notifyReadOnlyScopedView();
    return false;
  }
  var ids = photoIdsInReviewScope(pipelineReviewUniquePhotoIds(photoIds || getActiveSelection()));
  if (!ids.length) {
    window._vireoNativeMenuPhotoIdsOverride = null;
    return false;
  }
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
  /** @type {HTMLElement} */ (document.getElementById('pipelineCollectionTitle')).textContent =
    'Add ' + ids.length + ' photo' + (ids.length === 1 ? '' : 's') + ' to Collection';
  var listHtml = pipelineReviewCollections.map(function(collection) {
    return '<button type="button" class="pipeline-collection-choice" data-id="' + collection.id +
      '" onclick="pickPipelineCollection(' + collection.id + ')" style="display:block;width:100%;padding:7px 10px;text-align:left;cursor:pointer;border:0;border-radius:4px;background:transparent;color:var(--text-primary);font-size:13px;">' +
      escapeHtml(collection.name) + '</button>';
  }).join('');
  if (!listHtml) {
    listHtml = '<div style="padding:6px 10px;color:var(--text-dim);font-size:12px;">No manual collections yet. Enter a name below to create one.</div>';
  }
  /** @type {HTMLElement} */ (document.getElementById('pipelineCollectionList')).innerHTML = listHtml;
  /** @type {HTMLInputElement} */ (document.getElementById('pipelineCollectionNewName')).value = '';
  /** @type {HTMLElement} */ (document.getElementById('pipelineCollectionModal')).classList.add('open');
  pipelineReviewActivateOrganizationModal('collection');
  setTimeout(function() { /** @type {HTMLInputElement} */ (document.getElementById('pipelineCollectionNewName')).focus(); }, 50);
  return true;
}

/** @param {number} collectionId */
function pickPipelineCollection(collectionId) {
  pipelineReviewSelectedCollectionId = collectionId;
  pipelineReviewCreatedCollection = null;
  /** @type {HTMLInputElement} */ (document.getElementById('pipelineCollectionNewName')).value = '';
  /** @type {NodeListOf<HTMLButtonElement>} */ (document.querySelectorAll('.pipeline-collection-choice')).forEach(function(el) {
    el.style.background = Number(el.dataset.id) === collectionId ? 'var(--bg-tertiary)' : 'transparent';
  });
}

/** @param {boolean} [force] */
function hidePipelineCollectionModal(force) {
  if (pipelineReviewCollectionSubmitPromise && force !== true) return false;
  /** @type {HTMLElement} */ (document.getElementById('pipelineCollectionModal')).classList.remove('open');
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

/** @param {boolean} submitting */
function setPipelineCollectionSubmitting(submitting) {
  var btn = /** @type {HTMLButtonElement} */ (document.getElementById('pipelineCollectionSubmitBtn'));
  if (btn) btn.disabled = submitting;
  var cancel = /** @type {HTMLButtonElement} */ (document.getElementById('pipelineCollectionCancelBtn'));
  if (cancel) cancel.disabled = submitting;
  var input = /** @type {HTMLInputElement} */ (document.getElementById('pipelineCollectionNewName'));
  if (input) input.disabled = submitting;
}

function confirmPipelineCollection() {
  if (pipelineReviewCollectionSubmitPromise) return pipelineReviewCollectionSubmitPromise;
  if (isScopedReviewView()) {
    notifyReadOnlyScopedView();
    return Promise.resolve(false);
  }
  // Re-check the scope at submit: a membership refresh can drop a photo
  // while the dialog is open.
  var ids = photoIdsInReviewScope(pipelineReviewCollectionPhotoIds);
  var newName = /** @type {HTMLInputElement} */ (document.getElementById('pipelineCollectionNewName')).value.trim();
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
          var created = await /** @type {Promise<ReviewCollection>} */ (safeFetch('/api/collections', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
              name: newName,
              rules: [{field: 'photo_ids', value: []}],
            }),
          }));
          collectionId = created.id;
          pipelineReviewCreatedCollection = {id: collectionId || 0, name: newName};
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
    // photoIdsInReviewScope keeps burst members of an open Group Review in
    // scope even when they're outside the active folder/collection filter,
    // so a static collection write here can add a photo that is still out
    // of scope under the saved review. Without this snapshot + membership
    // refresh, the newly-added photo stays filtered out after Group Review
    // closes, and the "N not in saved review" totals stay stale.
    refreshLatestScopeSnapshotIfCurrent();
    return true;
  })().finally(function() {
    pipelineReviewCollectionSubmitPromise = null;
    setPipelineCollectionSubmitting(false);
  });
  return pipelineReviewCollectionSubmitPromise;
}

/** @param {number[]} photoIds */
function batchAddKeyword(photoIds) {
  if (pipelineReviewKeywordSubmitPromise) return false;
  if (isScopedReviewView()) {
    window._vireoNativeMenuPhotoIdsOverride = null;
    notifyReadOnlyScopedView();
    return false;
  }
  var ids = photoIdsInReviewScope(pipelineReviewUniquePhotoIds(photoIds || getActiveSelection()));
  if (!ids.length) {
    window._vireoNativeMenuPhotoIdsOverride = null;
    return false;
  }
  pipelineReviewKeywordPhotoIds = ids;
  window._vireoNativeMenuPhotoIdsOverride = null;
  /** @type {HTMLElement} */ (document.getElementById('pipelineKeywordTitle')).textContent =
    'Add Keyword to ' + ids.length + ' Photo' + (ids.length === 1 ? '' : 's');
  /** @type {HTMLInputElement} */ (document.getElementById('pipelineKeywordInput')).value = '';
  /** @type {HTMLElement} */ (document.getElementById('pipelineKeywordModal')).classList.add('open');
  pipelineReviewActivateOrganizationModal('keyword');
  setTimeout(function() { /** @type {HTMLInputElement} */ (document.getElementById('pipelineKeywordInput')).focus(); }, 50);
}

/** @param {boolean} [force] */
function hidePipelineKeywordModal(force) {
  if (pipelineReviewKeywordSubmitPromise && force !== true) return false;
  /** @type {HTMLElement} */ (document.getElementById('pipelineKeywordModal')).classList.remove('open');
  pipelineReviewDeactivateOrganizationModal('keyword');
  pipelineReviewKeywordPhotoIds = [];
  // Preserve the native selection override while the collection dialog is
  // still open, so its own confirmation reads the pinned photo IDs.
  if (!pipelineReviewAnyOrganizationModalOpen()) {
    window._vireoNativeMenuPhotoIdsOverride = null;
  }
  return true;
}

/** @param {boolean} submitting */
function setPipelineKeywordSubmitting(submitting) {
  var submit = /** @type {HTMLButtonElement} */ (document.getElementById('pipelineKeywordSubmitBtn'));
  if (submit) submit.disabled = submitting;
  var cancel = /** @type {HTMLButtonElement} */ (document.getElementById('pipelineKeywordCancelBtn'));
  if (cancel) cancel.disabled = submitting;
  var input = /** @type {HTMLInputElement} */ (document.getElementById('pipelineKeywordInput'));
  if (input) input.disabled = submitting;
}

function confirmPipelineKeyword() {
  if (pipelineReviewKeywordSubmitPromise) return pipelineReviewKeywordSubmitPromise;
  if (isScopedReviewView()) {
    notifyReadOnlyScopedView();
    return Promise.resolve(false);
  }
  var ids = photoIdsInReviewScope(pipelineReviewKeywordPhotoIds);
  var name = /** @type {HTMLInputElement} */ (document.getElementById('pipelineKeywordInput')).value.trim();
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
      // A smart collection scope keyed on `keyword` must re-check membership
      // after this write; without the refresh, a photo that now matches a
      // `keyword not_contains` rule stays visible until the scope is
      // reselected or the page reloads.
      refreshLatestScopeSnapshotIfCurrent();
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
          /** @type {HTMLElement} */ (document.getElementById('pipelineKeywordModal')).classList.contains('open')) {
        hidePipelineKeywordModal();
        e.preventDefault();
        return;
      }
      if (pipelineReviewActiveOrganizationModal === 'collection' &&
          /** @type {HTMLElement} */ (document.getElementById('pipelineCollectionModal')).classList.contains('open')) {
        hidePipelineCollectionModal();
        e.preventDefault();
        return;
      }
      if (/** @type {HTMLElement} */ (document.getElementById('pipelineCollectionModal')).classList.contains('open')) {
        hidePipelineCollectionModal();
        e.preventDefault();
        return;
      }
      if (/** @type {HTMLElement} */ (document.getElementById('pipelineKeywordModal')).classList.contains('open')) {
        hidePipelineKeywordModal();
        e.preventDefault();
      }
    });
}
