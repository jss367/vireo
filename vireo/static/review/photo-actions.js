// Per-photo actions (rating, flag, reveal, copy path, lightbox) and keeping Representative badges current.
// Classic page script; load boot.js after all definitions.

// Serialize each photo/field separately. Adjacent identical pending actions
// share one result and notification; a different choice runs after its predecessor.
// Clipboard writes share one lane so a late lookup cannot overwrite a newer copy.
var enqueueReviewPhotoAction = (function() {
  var pending = new Map();
  return function(key, value, run) {
    var previous = pending.get(key);
    if (previous && previous.value === value) return previous.promise;
    var task = {value: value, promise: null};
    task.promise = (previous ? previous.promise.catch(function() {}) : Promise.resolve())
      .then(run).finally(function() {
        if (pending.get(key) === task) pending.delete(key);
      });
    pending.set(key, task);
    return task.promise;
  };
})();

function saveReviewPhotoValue(photoId, field, value) {
  return enqueueReviewPhotoAction(field + ':' + photoId, value, async function() {
    try {
      var body = {photo_ids: [photoId]};
      body[field] = value;
      var data = await safeFetch('/api/batch/' + field, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(body),
      }, {toast: false});
      if (!data || data.ok !== true || data.updated !== 1) {
        throw new Error('The server did not confirm the update. Reload Review before trying again.');
      }
    } catch (error) {
      // The history transport already explains why undo/redo blocked a write.
      if (!error || error.code !== 'history_busy') {
        showToast('Could not save ' + field + ': ' + (error && error.message || 'Request failed. Try again.'), 'error');
      }
      return false;
    }
    if (field === 'flag' && _clearPredictionRepresentativeStateIfIneligible(photoId, value)) renderAll();
    showToast(field === 'rating' ? 'Rating updated' : 'Flag updated', 'success');
    return true;
  });
}

function setReviewRating(photoId, rating) {
  return saveReviewPhotoValue(photoId, 'rating', rating);
}

// Server-side representative eligibility (see get_species_representatives)
// hides rejected photos. Mirror that here so a card loaded with the badge
// stops rendering Representative the moment the user rejects it from the
// same page — without the walk below, pred.is_species_representative stays
// true until a full reload. Un-rejecting is intentionally left alone: the
// client can't know whether the DB preference still points at this photo,
// so the badge stays hidden until reload rather than lighting up incorrectly.
function _clearPredictionRepresentativeStateIfIneligible(photoId, flag) {
  if (flag !== 'rejected') return false;
  var changed = false;
  allPredictions.forEach(function(pred) {
    if (!pred || pred.photo_id !== photoId) return;
    if (pred.is_species_representative) {
      pred.is_species_representative = false;
      changed = true;
    }
    if (Array.isArray(pred.life_list)) {
      pred.life_list.forEach(function(entry) {
        if (!entry) return;
        if (entry.is_current_photo) { entry.is_current_photo = false; changed = true; }
        if (entry.is_species_representative) { entry.is_species_representative = false; changed = true; }
      });
    }
  });
  return changed;
}

function setReviewFlag(photoId, flag) {
  return saveReviewPhotoValue(photoId, 'flag', flag);
}

function revealReviewPhoto(photoId) {
  safeFetch('/api/files/reveal', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ photo_id: photoId }),
  }, { toast: false }).then(function(data) {
    if (typeof showRevealFeedback === 'function') showRevealFeedback(data);
  }).catch(function(err) {
    if (typeof showToast === 'function') {
      showToast('Reveal failed: ' + (err.message || 'request failed'), 'error');
    } else {
      console.error('revealReviewPhoto failed', err);
    }
  });
}

function copyReviewPhotoPath(photoId) {
  return enqueueReviewPhotoAction('copy-path', photoId, async function() {
    try {
      var data = await safeFetch('/api/photos/' + photoId, {}, {toast: false});
      if (!data || typeof data.path !== 'string' || !data.path.trim()) {
        throw new Error('The photo path is unavailable.');
      }
      if (!navigator.clipboard || typeof navigator.clipboard.writeText !== 'function') {
        throw new Error('Clipboard access is unavailable in this window.');
      }
      await navigator.clipboard.writeText(data.path);
    } catch (error) {
      showToast('Could not copy path: ' + (error && error.message || 'The operation failed. Try again.'), 'error');
      return false;
    }
    showToast('Path copied', 'success');
    return true;
  });
}

function openReviewLightbox(photoId, filename) {
  // Build the same photoList the grid click handler builds, so arrow-key
  // navigation inside the lightbox walks the current review grid.
  var seen = {};
  var photoList = [];
  var thumbs = document.getElementById('grid').querySelectorAll('img[data-photo-id]');
  for (var i = 0; i < thumbs.length; i++) {
    var pid = parseInt(thumbs[i].dataset.photoId, 10);
    if (seen[pid]) continue;
    seen[pid] = true;
    photoList.push({ id: pid, filename: thumbs[i].dataset.filename || '' });
  }
  openLightbox(photoId, filename || '', photoList);
}

// setLifeListPhoto (card context menu / shared lightbox action) only emits a
// lifelist:changed event and updates its own caches; without this listener the
// Review cards keep the pre-change value of pred.is_species_representative, so
// the newly assigned photo stays un-badged and any previously visible former
// representative keeps its badge until a full reload. Walk allPredictions
// (predictions is a filtered .slice() over the same object references, so
// updating in place propagates) and flip the species entry's flags, then
// re-render.
function bindReviewLifeListEvents() {
  document.addEventListener('lifelist:changed', function(e) {
    var detail = e && e.detail ? e.detail : {};
    var species = detail.species;
    var photoId = detail.photoId;
    if (!species || !photoId) return;
    var changed = false;
    allPredictions.forEach(function(pred) {
      var entries = Array.isArray(pred.life_list) ? pred.life_list : [];
      var touched = false;
      entries.forEach(function(entry) {
        if (!entry || entry.species !== species) return;
        var isCurrent = pred.photo_id === photoId;
        if (entry.is_current_photo !== isCurrent || entry.is_species_representative !== isCurrent) {
          entry.is_current_photo = isCurrent;
          entry.is_species_representative = isCurrent;
          touched = true;
        }
      });
      if (pred.photo_id === photoId && !entries.some(function(entry) { return entry && entry.species === species; })) {
        entries.push({species: species, is_current_photo: true, is_species_representative: true});
        pred.life_list = entries;
        touched = true;
      }
      var isRep = entries.some(function(entry) {
        return entry && entry.is_species_representative;
      });
      if (pred.is_species_representative !== isRep) {
        pred.is_species_representative = isRep;
        touched = true;
      }
      if (touched) changed = true;
    });
    if (changed) renderAll();
  });
}
