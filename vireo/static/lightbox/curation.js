
// --- Species Representative panel, shared across every page's lightbox.
// Driven by the `life_list` block on GET /api/photos/<id> (cached in
// _lbPhotoDataByPhoto), so the action works wherever a photo is opened — not
// just on the /life-list page. Each eligible species gets its own row; the
// button reflects real state.
function _lbEnsureLifeListPanel() {
  var actions = document.getElementById('lightboxActions');
  if (!actions) return null;
  var panel = document.getElementById('lifeListLightboxPanel');
  if (!panel) {
    panel = document.createElement('div');
    panel.id = 'lifeListLightboxPanel';
    panel.className = 'lifelist-lb-panel';
    actions.insertBefore(panel, actions.firstChild);
  }
  return panel;
}

function _lbRenderLifeListPanel(photoId) {
  var panel = _lbEnsureLifeListPanel();
  if (!panel) return;
  var data = _lbPhotoDataByPhoto[String(photoId)];
  var entries = (data && data.life_list) || [];
  if (!entries.length) {
    panel.innerHTML = '';
    // Keep an invisible, fixed-size placeholder in the wrapping action row.
    // Otherwise navigating to an ineligible photo (or rejecting the current
    // one) changes the bottom bar's height and makes a fit-to-window image
    // visibly resize after its pixels have already loaded.
    panel.style.visibility = 'hidden';
    panel.setAttribute('aria-hidden', 'true');
    return;
  }
  panel.style.visibility = 'visible';
  panel.setAttribute('aria-hidden', 'false');
  panel.innerHTML = '';
  entries.forEach(function(entry) {
    var row = document.createElement('div');
    row.className = 'lifelist-lb-row';
    var label = document.createElement('span');
    label.className = 'lifelist-lb-species';
    label.textContent = entry.species;
    var btn = document.createElement('button');
    btn.type = 'button';
    if (entry.is_current_photo) {
      btn.className = 'primary';
      btn.textContent = 'Representative';
    } else {
      btn.textContent = 'Set Representative';
    }
    btn.disabled = _lbReadOnly;
    if (_lbReadOnly) btn.title = _lbReadOnlyMessage;
    btn.addEventListener('click', function(event) {
      event.stopPropagation();
      setLifeListPhoto(entry.species, photoId, btn);
    });
    row.appendChild(label);
    row.appendChild(btn);
    panel.appendChild(row);
  });
}

async function setLifeListPhoto(species, photoId, button) {
  if (_lbGuardReadOnly()) return false;
  if (!species || !photoId) return;
  if (button) button.disabled = true;
  try {
    await window.safeFetch('/api/photo-preferences', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ purpose: 'species_representative', species: species, photo_id: photoId }),
    });
    // Re-read this photo's block so the panel shows the honest new state, and
    // refresh the lightbox's cache so re-renders stay consistent.
    try {
      var fresh = await window.safeFetch('/api/photos/' + photoId, {}, { toast: false });
      if (fresh) _lbPhotoDataByPhoto[String(photoId)] = fresh;
    } catch (e) { /* keep the panel usable even if the refresh fails */ }
    if (_lightboxCurrentId === photoId) _lbRenderLifeListPanel(photoId);
    // Let the /life-list page (or any listener) refresh its grid + ribbons.
    document.dispatchEvent(new CustomEvent('lifelist:changed', {
      detail: { species: species, photoId: photoId },
    }));
    if (typeof showToast === 'function') {
      showToast('Species representative set for ' + species, 'success');
    }
  } finally {
    if (button) button.disabled = false;
  }
}
window.setLifeListPhoto = setLifeListPhoto;

function _lifeListEntriesFromPhotoLike(photo) {
  if (!photo) return [];
  if (Array.isArray(photo.life_list)) {
    return photo.life_list.filter(function(entry) {
      return entry && entry.species;
    });
  }
  if (Array.isArray(photo.species)) {
    if (photo.flag === 'rejected') return [];
    return photo.species.filter(Boolean).map(function(species) {
      return { species: species, is_current_photo: false, is_species_representative: false };
    });
  }
  if (typeof photo.species === 'string' && photo.species && photo.flag !== 'rejected') {
    return [{ species: photo.species, is_current_photo: false, is_species_representative: false }];
  }
  return [];
}

function _lifeListMenuEntriesForPhotoId(photoId, opts) {
  opts = opts || {};
  var photo = null;
  if (typeof opts.getPhoto === 'function') {
    photo = opts.getPhoto(photoId);
  }
  if (!photo && opts.photoById) {
    photo = opts.photoById[String(photoId)] || opts.photoById[photoId];
  }
  return _lifeListEntriesFromPhotoLike(photo);
}

async function chooseSpeciesRepresentativeForPhoto(photoId) {
  if (!photoId) return;
  var data;
  try {
    data = await window.safeFetch('/api/photos/' + photoId);
  } catch (err) {
    return;
  }
  var entries = _lifeListEntriesFromPhotoLike(data);
  if (!entries.length) {
    if (typeof showToast === 'function') {
      showToast('No eligible species keyword on this photo', 'warning');
    }
    return;
  }
  var entry = entries[0];
  if (entries.length > 1) {
    var names = entries.map(function(e) { return e.species; });
    var choice = window.prompt('Set as representative for which species?\n' + names.join('\n'), names[0]);
    if (!choice) return;
    entry = entries.find(function(e) { return e.species === choice; }) || { species: choice };
  }
  await setLifeListPhoto(entry.species, photoId);
}
window.chooseSpeciesRepresentativeForPhoto = chooseSpeciesRepresentativeForPhoto;

window.buildSpeciesRepresentativeMenuItems = function(photoIds, opts) {
  opts = opts || {};
  photoIds = (photoIds || []).filter(function(id) { return id != null; });
  if (!photoIds.length) return [];
  if (photoIds.length !== 1) {
    var anyEligible = photoIds.some(function(id) {
      return _lifeListMenuEntriesForPhotoId(id, opts).length > 0;
    });
    if (!anyEligible && !opts.showFetchFallback) return [];
    return [{
      label: 'Set Representative',
      disabled: true,
      disabledHint: 'Select a single photo',
    }];
  }
  var photoId = photoIds[0];
  var entries = _lifeListMenuEntriesForPhotoId(photoId, opts);
  if (entries.length) {
    return entries.map(function(entry) {
      var isCurrent = !!entry.is_current_photo;
      return {
        label: 'Set Representative \u2014 ' + entry.species,
        disabled: isCurrent,
        disabledHint: isCurrent ? 'Already representative' : null,
        onClick: function() { setLifeListPhoto(entry.species, photoId); },
      };
    });
  }
  if (!opts.showFetchFallback) return [];
  return [{
    label: 'Set Representative\u2026',
    onClick: function() { chooseSpeciesRepresentativeForPhoto(photoId); },
  }];
};

function _highlightEntriesFromPhotoLike(photo) {
  if (!photo) return [];
  if (Array.isArray(photo.highlight_list)) {
    return photo.highlight_list.filter(function(entry) {
      return entry && entry.species;
    });
  }
  return [];
}

function _highlightMenuEntriesForPhotoId(photoId, opts) {
  opts = opts || {};
  var photo = null;
  if (typeof opts.getPhoto === 'function') {
    photo = opts.getPhoto(photoId);
  }
  if (!photo && opts.photoById) {
    photo = opts.photoById[String(photoId)] || opts.photoById[photoId];
  }
  return _highlightEntriesFromPhotoLike(photo);
}

async function setSpeciesHighlightFromMenu(species, photoId) {
  if (!species || !photoId) return;
  await window.safeFetch('/api/species-highlights', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ species: species, photo_id: photoId }),
  });
  try {
    var fresh = await window.safeFetch('/api/photos/' + photoId, {}, { toast: false });
    if (fresh && typeof _lbPhotoDataByPhoto !== 'undefined') {
      _lbPhotoDataByPhoto[String(photoId)] = fresh;
    }
  } catch (e) {}
  document.dispatchEvent(new CustomEvent('highlights:changed', {
    detail: { species: species, photoId: photoId },
  }));
  if (typeof window.loadHighlights === 'function') {
    await window.loadHighlights();
    if (typeof window.updateHighlightsLightboxControls === 'function') {
      window.updateHighlightsLightboxControls(photoId);
    }
  }
  if (typeof showToast === 'function') {
    showToast('Added to Highlights', 'success');
  }
}
window.setSpeciesHighlightFromMenu = setSpeciesHighlightFromMenu;

async function chooseSpeciesHighlightForPhoto(photoId) {
  if (!photoId) return;
  var data;
  try {
    data = await window.safeFetch('/api/photos/' + photoId);
  } catch (err) {
    return;
  }
  var entries = _highlightEntriesFromPhotoLike(data);
  if (!entries.length) {
    if (typeof showToast === 'function') {
      showToast('No eligible highlight species on this photo', 'warning');
    }
    return;
  }
  var entry = entries[0];
  if (entries.length > 1) {
    var names = entries.map(function(e) { return e.species; });
    var choice = window.prompt('Add as a highlight for which species?\n' + names.join('\n'), names[0]);
    if (!choice) return;
    entry = entries.find(function(e) { return e.species === choice; }) || { species: choice };
  }
  await setSpeciesHighlightFromMenu(entry.species, photoId);
}
window.chooseSpeciesHighlightForPhoto = chooseSpeciesHighlightForPhoto;

window.buildSpeciesHighlightMenuItems = function(photoIds, opts) {
  opts = opts || {};
  photoIds = (photoIds || []).filter(function(id) { return id != null; });
  if (!photoIds.length) return [];
  if (photoIds.length !== 1) {
    return [{
      label: 'Add to Highlights',
      disabled: true,
      disabledHint: 'Select a single photo',
    }];
  }
  var photoId = photoIds[0];
  var entries = _highlightMenuEntriesForPhotoId(photoId, opts);
  if (entries.length) {
    return entries.map(function(entry) {
      return {
        label: (entry.is_highlighted ? 'Highlighted' : 'Add to Highlights') + ' \u2014 ' + entry.species,
        disabled: !!entry.is_highlighted,
        disabledHint: entry.is_highlighted ? 'Already highlighted' : undefined,
        onClick: function() { setSpeciesHighlightFromMenu(entry.species, photoId); },
      };
    });
  }
  if (!opts.showFetchFallback) return [];
  return [{
    label: 'Add to Highlights\u2026',
    onClick: function() { chooseSpeciesHighlightForPhoto(photoId); },
  }];
};

document.addEventListener('lightbox:photochanged', function(event) {
  var pid = event.detail ? event.detail.photoId : null;
  // Render from cache now (instant when revisiting a loaded photo); the panel
  // re-renders again once the fresh /api/photos fetch resolves.
  if (pid != null) _lbRenderLifeListPanel(pid);
});

// A photo's flag decides representative eligibility (a rejected photo can't be
// a representative, per _photo_can_be_life_list_preference). The cached
// `life_list` block goes stale the moment the user changes the flag from the
// lightbox, so re-fetch it and re-render — this hides the panel when the photo
// becomes rejected and re-shows it when the flag is cleared again.
document.addEventListener('lightbox:flagchanged', async function(event) {
  var pid = event.detail ? event.detail.photoId : null;
  if (pid == null || _lightboxCurrentId !== pid) return;
  try {
    var fresh = await window.safeFetch('/api/photos/' + pid, {}, { toast: false });
    // Guard against a stale fetch clobbering the panel after navigation.
    if (!fresh || _lightboxCurrentId !== pid) return;
    _lbPhotoDataByPhoto[String(pid)] = fresh;
    _lbRenderLifeListPanel(pid);
  } catch (e) { /* leave the last-rendered panel in place if the refresh fails */ }
});
