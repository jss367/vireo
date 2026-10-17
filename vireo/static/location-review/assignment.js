// Assigning a location: chunked batches, progress, retries, and the navigation lock.
// Classic page script; load boot.js after all definitions.
'use strict';

function resetAssignmentProgress() {
  var progress = /** @type {HTMLElement} */ (document.getElementById('locationReviewAssignmentProgress'));
  var status = /** @type {HTMLElement} */ (document.getElementById('locationReviewAssignmentStatus'));
  var track = /** @type {HTMLElement} */ (document.getElementById('locationReviewAssignmentTrack'));
  progress.classList.add('location-review-hidden');
  status.textContent = '';
  /** @type {HTMLElement} */ (document.getElementById('locationReviewAssignmentFill')).style.width = '0%';
  track.setAttribute('aria-valuemax', '0');
  track.setAttribute('aria-valuenow', '0');
  track.removeAttribute('aria-valuetext');
}

/** @param {number} completed @param {number} total @param {boolean=} interrupted */ function updateAssignmentProgress(completed, total, interrupted) {
  var progress = /** @type {HTMLElement} */ (document.getElementById('locationReviewAssignmentProgress'));
  var status = /** @type {HTMLElement} */ (document.getElementById('locationReviewAssignmentStatus'));
  var track = /** @type {HTMLElement} */ (document.getElementById('locationReviewAssignmentTrack'));
  var pct = total ? Math.round((completed / total) * 100) : 100;
  var remaining = Math.max(0, total - completed);
  var statusText = interrupted
    ? formatNumber(completed) + ' of ' + formatNumber(total) + ' assigned · ' + formatNumber(remaining) + ' remaining'
    : 'Assigning ' + formatNumber(completed) + ' of ' + formatNumber(total) + ' photos · ' + pct + '%';
  progress.classList.remove('location-review-hidden');
  status.textContent = statusText;
  /** @type {HTMLElement} */ (document.getElementById('locationReviewAssignmentFill')).style.width = pct + '%';
  track.setAttribute('aria-valuemax', String(total));
  track.setAttribute('aria-valuenow', String(completed));
  track.setAttribute('aria-valuetext', statusText);
}

/** @param {boolean} disabled */ function setAssignmentNavigationDisabled(disabled) {
  ['locationReviewMode', 'locationReviewGap', 'locationReviewCollection', 'locationReviewDistance', 'locationReviewIncludeKept',
    'locationReviewShowAll', 'locationReviewPagePrevious', 'locationReviewPageNext'].forEach(function(id) {
    var control = document.getElementById(id); if (control instanceof HTMLInputElement || control instanceof HTMLSelectElement || control instanceof HTMLButtonElement) control.disabled = disabled;
  });
  document.querySelectorAll(/** @type {'button'} */ ('[data-split-before], [data-review-separately]')).forEach(function(button) {
    button.disabled = disabled;
  });
  /** @type {HTMLButtonElement} */ (document.getElementById('locationReviewSkip')).disabled = disabled;
  /** @type {HTMLButtonElement} */ (document.getElementById('locationReviewPrevious')).disabled = disabled || state.groups.length < 2;
  /** @type {HTMLButtonElement} */ (document.getElementById('locationReviewNext')).disabled = disabled || state.groups.length < 2;
  document.querySelectorAll(/** @type {'button'} */ ('[data-suggestion-mode]')).forEach(function(button) {
    button.disabled = disabled;
  });
}

function hasPartialAssignmentProgress() {
  return !!(state.assignment && state.assignment.completed > 0
    && state.assignment.completed < state.assignment.total);
}

/** @param {LocationAssignment} assignment */ function countProcessedInPhotoIds(assignment) {
  if (!assignment || !assignment.processedIds) return 0;
  var count = 0;
  for (var i = 0; i < assignment.photoIds.length; i++) {
    if (assignment.processedIds.has(assignment.photoIds[i] ?? -1)) count++;
  }
  return count;
}

/** @param {number} photoId */ function reconcileAssignmentForDeletion(photoId) {
  // A deletion arriving mid-partial (or mid-batch via a pre-existing modal
  // that bypassed openPhotoPreview's guard) must not leave a stale ID in the
  // retry snapshot: the batch endpoints validate every ID up front and 404
  // on any that no longer exist, permanently jamming the retry loop. Drop
  // the deleted ID from photoIds and resync the counters against
  // processedIds so both the display and the next chunk see reality.
  if (!state.assignment) return;
  var idx = state.assignment.photoIds.indexOf(photoId);
  if (idx === -1) return;
  state.assignment.photoIds.splice(idx, 1);
  state.assignment.total = state.assignment.photoIds.length;
  state.assignment.completed = countProcessedInPhotoIds(state.assignment);
  if (state.assignment.completed > state.assignment.total) {
    state.assignment.completed = state.assignment.total;
  }
}

/** @param {LocationChoice} choice */ function normalizePlaceForSubmit(choice) {
  var result = choice._googleResult || {};
  var components = choice.address_components || result.address_components || [];
  return {
    place_id: choice.place_id,
    name: choice.name,
    types: choice.types || [],
    lat: choice.latitude,
    lng: choice.longitude,
    address_components: components.map(function(component) {
      return {
        name: component.name || component.long_name || '',
        short_name: component.short_name || '',
        types: component.types || [],
      };
    }),
  };
}

/** @param {LocationChoice} choice @returns {Promise<LocationChoice>} */ function hydrateGoogleChoice(choice) {
  if (!state.placesService || (choice.address_components && choice.address_components.length)) return Promise.resolve(choice);
  return new Promise(function(resolve) {
    state.placesService?.getDetails({
      placeId: choice.place_id,
      fields: ['place_id','name','types','formatted_address','geometry','address_components'],
    }, function(place, status) {
      if (status === google.maps.places.PlacesServiceStatus.OK && place) {
        var hydrated = googleResultToChoice(place) || choice;
        hydrated.address_components = place.address_components || [];
        resolve(hydrated);
      } else resolve(choice);
    });
  });
}

async function assignCurrentGroup() {
  if (state.mode === 'discrepancies') { await resolveDiscrepancies('assigned'); return; }
  var group = currentGroup();
  let choice = state.selectedChoice;
  if (!group || !choice) return;
  // A lightbox opened before the assignment starts (e.g. keyboard-tabbed to
  // the Assign button while a preview was still on screen) sits outside
  // openPhotoPreview()'s new guard. Close it so a mid-assignment delete
  // cannot fire lightbox:photodeleted and reset state.assignment while
  // committed chunks are still in flight.
  if (lightboxIsOpen() && typeof closeLightbox === 'function') {
    try { closeLightbox(); } catch (_) {}
  }
  var button = /** @type {HTMLButtonElement} */ (document.getElementById('locationReviewAssign'));
  var choiceKey = candidateKey(choice);
  if (!state.assignment || state.assignment.group !== group || state.assignment.choiceKey !== choiceKey) {
    // Snapshot photo_ids so a mid-partial lightbox:photodeleted that
    // splices group.photo_ids cannot shift the retry offset — if a photo
    // in the already-committed prefix were removed, the surviving IDs
    // slide left and offset=completed would slice past what should be
    // the next unassigned photo, silently marking the group done.
    // processedIds tracks which snapshot IDs have been sent to the server
    // so the retry loop can rebuild "remaining" from set membership rather
    // than an index that could drift out from under a reconciled snapshot.
    var snapshotIds = group.photo_ids.slice();
    state.assignment = {
      group: group,
      choiceKey: choiceKey,
      completed: 0,
      total: snapshotIds.length,
      photoIds: snapshotIds,
      processedIds: new Set(),
    };
  } else if (!state.assignment.processedIds) {
    state.assignment.processedIds = new Set();
  }
  var assignment = state.assignment;
  state.isAssigning = true;
  setAssignmentNavigationDisabled(true);
  button.disabled = true;
  button.textContent = 'Assigning…';
  updateAssignmentProgress(assignment.completed, assignment.total);
  try {
    if (choice.kind === 'google') choice = await hydrateGoogleChoice(choice);
    while (true) {
      // Rebuild the pending set from photoIds \ processedIds each iteration
      // so a concurrent reconciliation (from lightbox:photodeleted) drops
      // deleted IDs out of the next chunk instead of forcing the batch to
      // 404 on a now-missing photo.
      var pending = assignment.photoIds.filter(function(id) {
        return !assignment.processedIds.has(id);
      });
      if (!pending.length) break;
      var chunk = pending.slice(0, 1000);
      var endpoint = '/api/batch/location';
      /** @type {LocationAssignmentBody} */ var body = {photo_ids: chunk};
      if (choice.kind === 'keyword') body.keyword_id = choice.keyword_id;
      else if (choice.kind === 'custom') {
        endpoint = '/api/batch/location/text';
        body.name = choice.name;
        if (group.center) {
          body.latitude = locationGroupCenter(group).lat;
          body.longitude = locationGroupCenter(group).lng;
        }
      } else {
        body.place_id = choice.place_id;
        body.place = normalizePlaceForSubmit(choice);
      }
      /** @type {{location?:LocationSaved & {keyword_id:number}}} */ var result = await safeFetch(endpoint, {
        method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body)
      });
      chunk.forEach(function(id) { assignment.processedIds.add(id); });
      assignment.total = assignment.photoIds.length;
      assignment.completed = countProcessedInPhotoIds(assignment);
      updateAssignmentProgress(assignment.completed, assignment.total);
      if (state.mode === 'time' && result && result.location && result.location.keyword_id) {
        var saved = result.location;
        var previous = state.savedLocations.find(function(item) { return item.id === saved.keyword_id; });
        state.savedLocations = state.savedLocations.filter(function(item) { return item.id !== saved.keyword_id; });
        state.savedLocations.unshift(Object.assign({}, saved, {
          id: saved.keyword_id, type: 'location', photo_count: Number(previous && previous.photo_count || 0) + chunk.length,
        }));
      }
    }
    var assignedCount = assignment.total;
    if (assignedCount > 0) {
      showToast('Assigned “' + choice.name + '” to ' + formatNumber(assignedCount) + (assignedCount === 1 ? ' photo' : ' photos'), 'success');
    }
    state.assignment = null;
    state.isAssigning = false;
    state.groups.splice(state.currentIndex, 1);
    state.assignedGroups += 1;
    if (state.currentIndex >= state.groups.length) state.currentIndex = Math.max(0, state.groups.length - 1);
    renderCurrentGroup();
  } catch (e) {
    state.isAssigning = false;
    button.disabled = false;
    // Resync counters in case a reconciliation landed between the last
    // update and the failure — otherwise "Retry N remaining" could quote a
    // stale count that includes IDs no longer in the snapshot.
    assignment.total = assignment.photoIds.length;
    assignment.completed = countProcessedInPhotoIds(assignment);
    var remaining = assignment.total - assignment.completed;
    if (remaining <= 0) {
      // Deletions during the failed batch drained everything the retry
      // still had to do. Treat the group as finished and move on.
      state.assignment = null;
      setAssignmentNavigationDisabled(false);
      resetAssignmentProgress();
      state.groups.splice(state.currentIndex, 1);
      state.assignedGroups += 1;
      if (state.currentIndex >= state.groups.length) state.currentIndex = Math.max(0, state.groups.length - 1);
      renderCurrentGroup();
    } else if (assignment.completed) {
      updateAssignmentProgress(assignment.completed, assignment.total, true);
      button.textContent = 'Retry ' + formatNumber(remaining) + ' remaining';
      // Keep group navigation and suggestion-mode locked while a partial
      // assignment is pending. Skip/Prev/Next would otherwise drop
      // state.assignment silently: Skip splices the group as "skipped
      // without changes" even though committed chunks already changed
      // photos, and Prev/Next runs renderCurrentGroup() which nulls the
      // offset so a later Retry reprocesses the committed chunks.
    } else {
      setAssignmentNavigationDisabled(false);
      resetAssignmentProgress();
      button.textContent = 'Retry “' + choice.name + '”';
    }
  }
}
