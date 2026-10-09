/* Browse: grid DOM: rendering, appending/prepending pages, card refreshes.
   Classic script sharing page globals; browse.html loads the
   browse/*.js files in dependency order. */

// Fold any already-loaded workspace descriptions into freshly rendered
// color-label badges so lazily inserted cards get the same title and
// accessible name as cards rendered after descriptions finished loading.
function refreshColorLabelControlsIn(scope) {
  if (!scope) return;
  if (window.VireoColorLabels && typeof window.VireoColorLabels.refreshControls === 'function') {
    window.VireoColorLabels.refreshControls(scope);
  }
}

function appendGridPhotos(newPhotos, startIdx) {
  if (!newPhotos.length) return;
  var grid = document.getElementById('grid');
  var html = '';
  newPhotos.forEach(function(p, offset) {
    html += renderPhotoCard(p, startIdx + offset);
  });
  var tail = document.getElementById('gridTail');
  if (tail) tail.insertAdjacentHTML('beforebegin', html);
  else grid.insertAdjacentHTML('beforeend', html);
  updateGridTail();
  refreshColorLabelControlsIn(grid);
}

function prependGridPhotos(newPhotos) {
  if (!newPhotos.length) return;
  var grid = document.getElementById('grid');
  var html = '';
  newPhotos.forEach(function(p, idx) {
    html += renderPhotoCard(p, idx);
  });

  // Existing cards keep their DOM nodes (and therefore their already-decoded
  // thumbnails), but their array indices move when a preceding page arrives.
  // Keep the inline selection handlers in lockstep with the new indices.
  Array.from(grid.getElementsByClassName('grid-card')).forEach(function(card) {
    var idx = parseInt(card.dataset.idx, 10) + newPhotos.length;
    var id = parseInt(card.dataset.id, 10);
    card.dataset.idx = idx;
    if (!card.classList.contains('offline')) {
      card.setAttribute('onclick', 'selectPhoto(event,' + id + ',' + idx + ')');
    }
  });
  grid.insertAdjacentHTML('afterbegin', html);
  if (selectedIndex >= 0) selectedIndex += newPhotos.length;
  updateGridTail();
  refreshColorLabelControlsIn(grid);
}

/* The grid tail reserves a bounded amount of scroll space for the next photos
   that haven't loaded yet. It must not reserve the entire dataset: Browse
   loads a contiguous prefix, so an absolute-bottom jump in a large library
   would otherwise force every preceding page to load before the viewport
   could show a real card. Rebuilt after every page append. Lives in a
   display:contents wrapper so its children participate in the grid layout
   but real cards can be inserted before it as a unit. */
var SKEL_POOL = 300;

function updateGridTail() {
  var grid = document.getElementById('grid');
  var tail = document.getElementById('gridTail');
  // Photos before the loaded window are reached with "Browse from beginning",
  // not by scrolling down — they must not inflate the downward runway.
  var remaining = allLoaded ? 0 : totalPhotos - loadedWindowOffset() - photos.length;
  var cards = grid.getElementsByClassName('grid-card');
  var lastCard = cards.length ? cards[cards.length - 1] : null;
  if (remaining <= 0 || !lastCard) {
    if (tail) tail.remove();
    return;
  }

  // Size skeleton cells to match real cards: the thumb tracks column width
  // via aspect-ratio, the info block copies its measured height.
  var infoEl = lastCard.querySelector('.grid-card-info');
  if (infoEl && infoEl.offsetHeight) {
    grid.style.setProperty('--skel-info-h', infoEl.offsetHeight + 'px');
  }

  var skel = '<div class="skel-card"><div class="skel-thumb"></div><div class="skel-info"><div class="skel-line"></div></div></div>';
  var html = '';
  var visible = Math.min(remaining, SKEL_POOL);
  for (var i = 0; i < visible; i++) html += skel;

  if (!tail) {
    tail = document.createElement('div');
    tail.id = 'gridTail';
    tail.style.display = 'contents';
    grid.appendChild(tail);
  }
  tail.innerHTML = html;
}

// Patches one already-rendered card to match `p`. Grid cards and expanded
// stack members share the same `.grid-card-img-wrap` + `.grid-card-info`
// shape, so both are repainted here rather than re-rendered: the card keeps
// its <img>, and therefore the thumbnail it has already decoded.
function refreshCardBadgesAndInfo(card, p) {
  applyCardColorLabel(card, p.id);
  var info = card.querySelector('.grid-card-info');
  if (info) {
    info.innerHTML = renderCardInfo(p);
    refreshColorLabelControlsIn(info);
  }
  // iNat badge lives in img-wrap (not grid-card-info), so sync it explicitly
  // when async metadata arrives after the card was appended.
  var wrap = card.querySelector('.grid-card-img-wrap');
  if (!wrap) return;
  var speciesBadges = wrap.querySelector('.species-badges');
  var speciesBadgeHtml = cardFields.indexOf('species') === -1
    ? renderSpeciesBadges(p.species, false)
    : '';
  if (speciesBadges && speciesBadgeHtml) {
    speciesBadges.outerHTML = speciesBadgeHtml;
  } else if (speciesBadges) {
    speciesBadges.remove();
  } else if (speciesBadgeHtml) {
    var bottomOverlay = wrap.querySelector('.grid-card-bottom-overlay') || wrap;
    bottomOverlay.insertAdjacentHTML('afterbegin', speciesBadgeHtml);
  }
  var existing = wrap.querySelector('.inat-badge');
  var shouldShow = !!inatSubmitted[String(p.id)];
  if (shouldShow && !existing) {
    wrap.insertAdjacentHTML('beforeend', '<span class="inat-badge">iNat</span>');
  } else if (!shouldShow && existing) {
    existing.remove();
  }
  var rep = wrap.querySelector('.representative-badge');
  if (p.is_species_representative && !rep) {
    wrap.insertAdjacentHTML('beforeend', '<span class="representative-badge">Representative</span>');
  } else if (!p.is_species_representative && rep) {
    rep.remove();
  }
}

function refreshGridCards(photoIds) {
  photoIds.forEach(function(id) {
    var p = photos.find(function(x) { return x.id === id; });
    if (!p) return;
    var card = document.querySelector('.grid-card[data-id="' + id + '"]');
    if (!card) return;
    refreshCardBadgesAndInfo(card, p);
  });
}

// The same repaint for the members of an expanded stack tray. Used where
// metadata arrives for members already on screen and only their badges and
// info line can have changed — not which members the tray holds, nor their
// order. Rebuilding the tray for that would give every member a fresh <img>
// (blanking thumbnails that were already decoded) and swap the DOM out from
// under a click the user has started.
function refreshStackMemberCards(photoIds) {
  photoIds.forEach(function(id) {
    var p = findBrowsePhoto(id);
    if (!p) return;
    document.querySelectorAll(
      '.browse-stack-member[data-id="' + id + '"]'
    ).forEach(function(member) {
      refreshCardBadgesAndInfo(member, p);
    });
  });
}

document.addEventListener('lifelist:changed', function(e) {
  var detail = e && e.detail ? e.detail : {};
  var species = detail.species;
  var photoId = detail.photoId;
  if (!species || !photoId) return;
  var changed = [];
  loadedBrowsePhotoIds().forEach(function(id) {
    var p = findBrowsePhoto(id);
    if (!p) return;
    var entries = Array.isArray(p.life_list) ? p.life_list : [];
    var touched = false;
    entries.forEach(function(entry) {
      if (!entry || entry.species !== species) return;
      var isCurrent = p.id === photoId;
      if (entry.is_current_photo !== isCurrent || entry.is_species_representative !== isCurrent) {
        entry.is_current_photo = isCurrent;
        entry.is_species_representative = isCurrent;
        touched = true;
      }
    });
    if (p.id === photoId && !entries.some(function(entry) { return entry && entry.species === species; })) {
      entries.push({species: species, is_current_photo: true, is_species_representative: true});
      p.life_list = entries;
      touched = true;
    }
    var isRep = entries.some(function(entry) {
      return entry && entry.is_species_representative;
    });
    if (p.is_species_representative !== isRep) {
      p.is_species_representative = isRep;
      touched = true;
    }
    if (touched) changed.push(p.id);
  });
  if (changed.length) {
    refreshGridCards(changed);
    refreshExpandedBrowseStackMembers(changed);
  }
});

function getGridCard(photoId) {
  return document.querySelector('.grid-card[data-id="' + photoId + '"]');
}

function getBrowsePhotoElement(photoId) {
  return getGridCard(photoId) ||
    document.querySelector('.browse-stack-member[data-id="' + photoId + '"]');
}

function loadedBrowseStackCoverForPhoto(photoId) {
  var wantedId = Number(photoId);
  return photos.find(function(photo) {
    return photo.browse_stack && (photo.browse_stack.photo_ids || []).some(function(id) {
      return Number(id) === wantedId;
    });
  }) || null;
}

/* ---------- Grid Rendering ---------- */
function renderGrid(options) {
  var grid = document.getElementById('grid');
  var empty = document.getElementById('emptyState');
  var welcome = document.getElementById('welcomeState');
  var thumbnails = new Map();
  var trayScroll = new Map();
  function thumbnailKey(img) {
    var card = img.closest('.grid-card, .browse-stack-member');
    return card && (card.classList.contains('grid-card') ? 'cover:' : 'member:') + card.dataset.id;
  }
  if (options && options.preserveThumbnails) {
    grid.querySelectorAll('img[data-thumbnail-src]').forEach(function(img) {
      thumbnails.set(thumbnailKey(img), img);
    });
    grid.querySelectorAll('.browse-stack-tray').forEach(function(tray) {
      var members = tray.querySelector('.browse-stack-members');
      if (members) trayScroll.set(tray.dataset.stackCoverId, members.scrollLeft);
    });
  }

  if (photos.length === 0) {
    grid.innerHTML = '';
    // Show welcome if DB is completely empty, otherwise show filter-empty message
    if (totalPhotos === 0 && !hasActiveBrowseFilter()) {
      welcome.style.display = 'block';
      empty.style.display = 'none';
    } else {
      welcome.style.display = 'none';
      empty.style.display = 'block';
    }
    return;
  }
  welcome.style.display = 'none';
  empty.style.display = 'none';

  var html = '';
  photos.forEach(function(p, idx) {
    html += renderPhotoCard(p, idx);
  });
  grid.innerHTML = html;
  updateGridTail();
  refreshColorLabelControlsIn(grid);
  restoreExpandedBrowseStacks();
  if (thumbnails.size) {
    grid.querySelectorAll('img[data-thumbnail-src]').forEach(function(img) {
      var previous = thumbnails.get(thumbnailKey(img));
      if (previous && previous.dataset.thumbnailSrc === img.dataset.thumbnailSrc) {
        img.replaceWith(previous);
      }
    });
    grid.querySelectorAll('.browse-stack-tray').forEach(function(tray) {
      var members = tray.querySelector('.browse-stack-members');
      if (members && trayScroll.has(tray.dataset.stackCoverId)) {
        members.scrollLeft = trayScroll.get(tray.dataset.stackCoverId);
      }
    });
  }
}
