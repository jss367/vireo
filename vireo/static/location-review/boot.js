// Page startup: event binding and opening the review set.
// Classic page script; loads after every other Location Review definition.
'use strict';

// Preserve the inline script's order: bind every control, then initialize().
document.getElementById('locationReviewBack').addEventListener('click', returnToBrowse);
document.getElementById('locationReviewEmptyBack').addEventListener('click', returnToBrowse);

document.getElementById('locationReviewCollection').addEventListener('change', function(event) {
  var value = event.target.value;
  if (!value) return;
  changeReviewSource(value === 'all' ? {scope: 'all'} : value === '__selection__'
    ? state.source : {collection_id: Number(value)});
});
['locationReviewMode', 'locationReviewGap', 'locationReviewDistance', 'locationReviewIncludeKept'].forEach(function(id) {
  document.getElementById(id).addEventListener('change', function() { changeReviewSource(state.source); });
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
      var pendingAssign = document.getElementById('locationReviewAssign');
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

document.getElementById('locationReviewSearch').addEventListener('input', function() {
  if (state.mode === 'time') renderSavedTimeLocations();
});

document.getElementById('locationReviewShowAll').addEventListener('click', function() {
  if (state.isAssigning || hasPartialAssignmentProgress() || !currentGroup()) return;
  state.inspectAll = !state.inspectAll;
  state.photoPage = 0;
  renderThumbnails(currentGroup());
});
['Previous', 'Next'].forEach(function(direction) {
  document.getElementById('locationReviewPage' + direction).addEventListener('click', function() {
    if (state.isAssigning || hasPartialAssignmentProgress() || !currentGroup()) return;
    state.photoPage = Math.max(0, state.photoPage + (direction === 'Next' ? 1 : -1));
    renderThumbnails(currentGroup());
  });
});

document.getElementById('locationReviewKeep').addEventListener('click', function() { resolveDiscrepancies('keep'); });
document.getElementById('locationReviewSelectAll').addEventListener('click', function() {
  if (state.isAssigning || !currentGroup()) return;
  state.selectedGpsPhotos = new Set(currentGroup().photo_ids);
  updateDiscrepancySelection();
});
document.getElementById('locationReviewSelectNone').addEventListener('click', function() {
  if (state.isAssigning) return;
  state.selectedGpsPhotos.clear();
  updateDiscrepancySelection();
});

document.getElementById('locationReviewPrevious').addEventListener('click', function() { moveGroup(-1); });
document.getElementById('locationReviewNext').addEventListener('click', function() { moveGroup(1); });
document.getElementById('locationReviewIncludeCoordinates').addEventListener('change', function(event) {
  state.includeCoordinates = event.target.checked;
  renderCandidates();
});
document.getElementById('locationReviewSkip').addEventListener('click', function() {
  if (state.isAssigning || hasPartialAssignmentProgress()) return;
  var group = currentGroup();
  if (!group) return;
  state.skippedGroups.push(group);
  state.groups.splice(state.currentIndex, 1);
  if (state.currentIndex >= state.groups.length) state.currentIndex = Math.max(0, state.groups.length - 1);
  renderCurrentGroup();
});
document.getElementById('locationReviewAssign').addEventListener('click', assignCurrentGroup);
document.querySelectorAll('[data-suggestion-mode]').forEach(function(button) {
  button.addEventListener('click', function() {
    setSuggestionMode(button.dataset.suggestionMode);
  });
});
document.getElementById('locationReviewCustom').addEventListener('click', function() {
  var name = document.getElementById('locationReviewSearch').value.trim();
  if (!name) { showToast('Enter a friendly location name first.', 'error'); return; }
  state.searchedCandidate = normalizeCandidate({kind: 'custom', name: name});
  selectChoice(state.searchedCandidate);
});
document.getElementById('locationReviewSearch').addEventListener('keydown', function(event) {
  if (event.key === 'Enter' && state.mapMode !== 'google') {
    event.preventDefault();
    document.getElementById('locationReviewCustom').click();
  }
});

initialize();
