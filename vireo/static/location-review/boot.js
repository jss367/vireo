// Page startup: event binding and opening the review set.
// Classic page script; loads after every other Location Review definition.
'use strict';

// Preserve the inline script's order: bind every control, then initialize().
/** @type {HTMLButtonElement} */ (document.getElementById('locationReviewBack')).addEventListener('click', returnToBrowse);
/** @type {HTMLButtonElement} */ (document.getElementById('locationReviewEmptyBack')).addEventListener('click', returnToBrowse);

/** @type {HTMLSelectElement} */ (document.getElementById('locationReviewCollection')).addEventListener('change', function(event) {
  var value = /** @type {HTMLSelectElement} */ (event.currentTarget).value;
  if (!value) return;
  changeReviewSource(value === 'all' ? {scope: 'all'} : value === '__selection__'
    ? state.source : {collection_id: Number(value)});
});
['locationReviewMode', 'locationReviewGap', 'locationReviewDistance', 'locationReviewIncludeKept'].forEach(function(id) {
  document.getElementById(id)?.addEventListener('change', function() { changeReviewSource(state.source); });
});

document.addEventListener('lightbox:photodeleted', function(event) {
  var photoId = Number(event.detail && event.detail.photoId);
  if (!Number.isFinite(photoId)) return;
  dropPhotoFromSelectionSource(photoId);
  var group = currentGroup();
  if (!group || group.photo_ids.indexOf(photoId) === -1) return;
  group.photos = group.photos.filter(function(photo) { return photo.id !== photoId; });
  group.photo_ids = group.photo_ids.filter(function(id) { return id !== photoId; });
  group.count = group.photos.length;
  // If an assignment is in flight, or a partial batch is waiting for retry,
  // rebuilding the UI here would null state.assignment and re-enable Skip /
  // Prev / Next — orphaning the already-committed chunks. Update the group
  // quietly instead; the pending assignment will refresh the group when it
  // finishes or is explicitly abandoned. Reconciling the assignment's
  // snapshot at the same time keeps future retries from submitting the
  // deleted ID and 404-ing the whole batch.
  if (state.isAssigning || hasPartialAssignmentProgress()) {
    reconcileAssignmentForDeletion(photoId);
    if (group.count) refreshGroupMetadata(group);
    if (!state.isAssigning && state.assignment) {
      var pendingRemaining = state.assignment.total - state.assignment.completed;
      if (pendingRemaining <= 0) {
        // Every still-pending photo was deleted, so the assignment is
        // effectively finished. Advance past this group as the catch
        // path does — even when reconciliation left already-committed
        // photos behind, those photos are already saved to the chosen
        // location, so re-rendering the group here would present them
        // as unreviewed and let the user double-assign them under a
        // different name.
        state.assignment = null;
        resetAssignmentProgress();
        state.groups.splice(state.currentIndex, 1);
        state.assignedGroups += 1;
        if (state.currentIndex >= state.groups.length) {
          state.currentIndex = Math.max(0, state.groups.length - 1);
        }
        renderCurrentGroup();
        return;
      }
      updateAssignmentProgress(
        state.assignment.completed, state.assignment.total, true
      );
      var pendingAssign = /** @type {HTMLButtonElement} */ (document.getElementById('locationReviewAssign'));
      if (pendingAssign) {
        pendingAssign.textContent =
          'Retry ' + formatNumber(pendingRemaining) + ' remaining';
      }
    }
    return;
  }
  if (!group.count) {
    state.groups.splice(state.currentIndex, 1);
    if (state.currentIndex >= state.groups.length) state.currentIndex = Math.max(0, state.groups.length - 1);
    renderCurrentGroup();
    return;
  }
  refreshGroupMetadata(group);
  renderCurrentGroup();
});

/** @type {HTMLInputElement} */ (document.getElementById('locationReviewSearch')).addEventListener('input', function() {
  if (state.mode === 'time') renderSavedTimeLocations();
});

/** @type {HTMLButtonElement} */ (document.getElementById('locationReviewShowAll')).addEventListener('click', function() {
  if (state.isAssigning || hasPartialAssignmentProgress() || !currentGroup()) return;
  state.inspectAll = !state.inspectAll;
  state.photoPage = 0;
  var group = currentGroup(); if (group) renderThumbnails(group);
});
['Previous', 'Next'].forEach(function(direction) {
  document.getElementById('locationReviewPage' + direction)?.addEventListener('click', function() {
    if (state.isAssigning || hasPartialAssignmentProgress() || !currentGroup()) return;
    state.photoPage = Math.max(0, state.photoPage + (direction === 'Next' ? 1 : -1));
    var group = currentGroup(); if (group) renderThumbnails(group);
  });
});

/** @type {HTMLButtonElement} */ (document.getElementById('locationReviewKeep')).addEventListener('click', function() { resolveDiscrepancies('keep'); });
/** @type {HTMLButtonElement} */ (document.getElementById('locationReviewSelectAll')).addEventListener('click', function() {
  if (state.isAssigning || !currentGroup()) return;
  state.selectedGpsPhotos = new Set(currentGroup()?.photo_ids || []);
  updateDiscrepancySelection();
});
/** @type {HTMLButtonElement} */ (document.getElementById('locationReviewSelectNone')).addEventListener('click', function() {
  if (state.isAssigning) return;
  state.selectedGpsPhotos.clear();
  updateDiscrepancySelection();
});

/** @type {HTMLButtonElement} */ (document.getElementById('locationReviewPrevious')).addEventListener('click', function() { moveGroup(-1); });
/** @type {HTMLButtonElement} */ (document.getElementById('locationReviewNext')).addEventListener('click', function() { moveGroup(1); });
/** @type {HTMLInputElement} */ (document.getElementById('locationReviewIncludeCoordinates')).addEventListener('change', function(event) {
  state.includeCoordinates = /** @type {HTMLInputElement} */ (event.currentTarget).checked;
  renderCandidates();
});
/** @type {HTMLButtonElement} */ (document.getElementById('locationReviewSkip')).addEventListener('click', function() {
  if (state.isAssigning || hasPartialAssignmentProgress()) return;
  var group = currentGroup();
  if (!group) return;
  state.skippedGroups.push(group);
  state.groups.splice(state.currentIndex, 1);
  if (state.currentIndex >= state.groups.length) state.currentIndex = Math.max(0, state.groups.length - 1);
  renderCurrentGroup();
});
/** @type {HTMLButtonElement} */ (document.getElementById('locationReviewAssign')).addEventListener('click', assignCurrentGroup);
document.querySelectorAll(/** @type {'button'} */ ('[data-suggestion-mode]')).forEach(function(button) {
  button.addEventListener('click', function() {
    setSuggestionMode(button.dataset.suggestionMode || '');
  });
});
/** @type {HTMLButtonElement} */ (document.getElementById('locationReviewCustom')).addEventListener('click', function() {
  var name = /** @type {HTMLInputElement} */ (document.getElementById('locationReviewSearch')).value.trim();
  if (!name) { showToast('Enter a friendly location name first.', 'error'); return; }
  state.searchedCandidate = normalizeCandidate({kind: 'custom', name: name});
  selectChoice(state.searchedCandidate);
});
/** @type {HTMLInputElement} */ (document.getElementById('locationReviewSearch')).addEventListener('keydown', function(event) {
  if (event.key === 'Enter' && state.mapMode !== 'google') {
    event.preventDefault();
    /** @type {HTMLButtonElement} */ (document.getElementById('locationReviewCustom')).click();
  }
});

initialize();

// The navbar's undo/redo transport (vireo-page-hooks.js). Keep a partly
// completed assignment intact until it is retried or abandoned.
Vireo.registerPageHooks({
  beforeHistoryChange: function() {
    return !state.isAssigning && !hasPartialAssignmentProgress();
  },
  afterHistoryChange: function() { return refreshLocationReview(); },
});
