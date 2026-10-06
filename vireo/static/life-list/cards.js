// Species cards, paging more photos per species, and rendering the grid.
// Classic page script; load boot.js after all definitions.

var LIFE_LIST_PAGE_SIZE = 100;
var lifeListLightboxSpecies = null;
var lifeListLoadPromises = {};

function photoThumbnailUrl(photo) {
  if (!photo || photo.id == null) return '';
  return window.vireoThumbnailUrl
    ? window.vireoThumbnailUrl(photo)
    : '/thumbnails/' + photo.id + '.jpg';
}

function renderCard(entry, displayNumber) {
  var best = entry.best;
  var thumb = best ? photoThumbnailUrl(best) : '';
  var sci = entry.scientific_name && entry.scientific_name !== entry.species
    ? '<div class="species-sci">' + escapeAttr(entry.scientific_name) + '</div>'
    : '';
  var metaParts = [entry.photo_count + ' photo' + (entry.photo_count === 1 ? '' : 's')];
  var first = formatDate(entry.first_seen);
  var last = formatDate(entry.last_seen);
  if (first) metaParts.push('first ' + first);
  if (last && last !== first) metaParts.push('latest ' + last);
  var locations = (entry.locations || []).slice(0, 3).map(function(l) {
    return '<span class="loc-chip">' + escapeAttr(l) + '</span>';
  }).join('');
  var extraLocs = (entry.locations || []).length - 3;
  if (extraLocs > 0) locations += '<span class="loc-chip">+' + extraLocs + ' more</span>';
  var remaining = Math.max(0, entry.photo_count - (entry.photos || []).length);
  var loadMore = entry.has_more
    ? '<button type="button" class="lifelist-load-more" data-load-more-species="'
      + escapeAttr(entry.species) + '"' + (entry.loading_more ? ' disabled' : '') + '>'
      + (entry.loading_more
        ? 'Loading photos…'
        : 'Load ' + Math.min(LIFE_LIST_PAGE_SIZE, remaining) + ' more photo'
          + (Math.min(LIFE_LIST_PAGE_SIZE, remaining) === 1 ? '' : 's'))
      + '</button>'
    : '';
  var badges = '';
  if (best && (best.is_species_representative || best.flag === 'flagged')) {
    badges = '<div class="lifelist-badges">'
      + (best.is_species_representative ? '<div class="lifelist-ribbon">Representative</div>' : '')
      + (best.flag === 'flagged' ? '<div class="lifelist-pick-badge" title="Flagged as Pick">Pick</div>' : '')
      + '</div>';
  }
  return '<div class="species-card" data-species="' + escapeAttr(entry.species) + '" title="'
    + escapeAttr((entry.locations || []).join(', ')) + '">'
    + '<div class="lifer-number">#' + displayNumber + '</div>'
    + badges
    + (best ? '<img src="' + escapeAttr(thumb) + '" data-photo-id="' + escapeAttr(best.id) + '" alt="' + escapeAttr(best.filename) + '" loading="lazy">' : '')
    + '<div class="species-info">'
    + '<div class="species-name">' + escapeAttr(entry.species) + '</div>'
    + sci
    + '<div class="species-meta">' + metaParts.join(' &middot; ') + '</div>'
    + (locations ? '<div class="species-locations">' + locations + '</div>' : '')
    + loadMore
    + '</div></div>';
}

function lifeListEntry(species) {
  return (currentData && currentData.species || []).find(function(entry) {
    return entry.species === species;
  });
}

function loadMoreLifeListPhotos(entry) {
  if (!entry || !entry.has_more) return Promise.resolve(entry ? entry.photos : []);
  if (lifeListLoadPromises[entry.species]) {
    return lifeListLoadPromises[entry.species];
  }
  entry.loading_more = true;
  render();
  var params = new URLSearchParams({
    species: entry.species,
    offset: String((entry.photos || []).length),
    limit: String(LIFE_LIST_PAGE_SIZE),
  });
  var promise = safeFetch('/api/life-list/species?' + params.toString())
    .then(function(data) {
      var seen = new Set((entry.photos || []).map(function(photo) { return photo.id; }));
      (data.photos || []).forEach(function(photo) {
        if (!seen.has(photo.id)) {
          entry.photos.push(photo);
          seen.add(photo.id);
        }
      });
      entry.loaded_count = data.loaded_count;
      entry.has_more = data.has_more;
      return entry.photos;
    })
    .catch(function() {
      return entry.photos;
    })
    .finally(function() {
      entry.loading_more = false;
      delete lifeListLoadPromises[entry.species];
      render();
    });
  lifeListLoadPromises[entry.species] = promise;
  return promise;
}

function render() {
  var content = document.getElementById('content');
  var empty = document.getElementById('emptyState');
  var meta = document.getElementById('meta');
  var controls = document.getElementById('controlsBar');

  if (!currentData || !currentData.species.length) {
    content.innerHTML = '';
    meta.textContent = '';
    controls.style.display = 'none';
    empty.style.display = 'block';
    return;
  }
  empty.style.display = 'none';
  controls.style.display = '';

  var search = document.getElementById('speciesSearch').value.trim();
  var taxonomicGroup = document.getElementById('taxonomicGroupSelect').value;
  var identificationRank = document.getElementById('identificationRankSelect').value;
  var searchOptions = VireoTextSearch.readOptions('lifeSpecies');
  var list = sortedSpecies();
  if (search) {
    list = list.filter(function(e) {
      return VireoTextSearch.matchesFields(
        [e.species, e.scientific_name || '', e.common_name || ''],
        search,
        searchOptions
      );
    });
  }
  if (taxonomicGroup !== 'all') {
    list = list.filter(function(entry) {
      if (taxonomicGroup === 'unknown') return !entry.taxonomic_class;
      return entry.taxonomic_class
        && String(entry.taxonomic_class.id) === taxonomicGroup;
    });
  }
  if (identificationRank !== 'all') {
    list = list.filter(function(entry) {
      if (identificationRank === 'unknown') return !entry.taxon_rank;
      return entry.taxon_rank === identificationRank;
    });
  }

  var filtersActive = Boolean(search)
    || taxonomicGroup !== 'all'
    || identificationRank !== 'all';
  meta.textContent = currentData.meta.species_count + ' species on your life list · '
    + currentData.meta.photo_count + ' tagged photos'
    + (filtersActive ? ' · ' + list.length + ' shown' : '');

  var renumberView = document.getElementById('renumberView').checked;
  content.innerHTML = list.map(function(entry, index) {
    return renderCard(entry, renumberView ? index + 1 : entry.number);
  }).join('')
    || '<div class="lifelist-empty" style="grid-column:1/-1;"><p>No life-list entries match these filters.</p></div>';

  content.querySelectorAll('[data-load-more-species]').forEach(function(button) {
    button.addEventListener('click', function(event) {
      event.stopPropagation();
      loadMoreLifeListPhotos(lifeListEntry(button.getAttribute('data-load-more-species')));
    });
  });

  content.querySelectorAll('.species-card').forEach(function(card) {
    card.addEventListener('click', function() {
      var sp = card.getAttribute('data-species');
      var entry = (currentData.species || []).find(function(e) { return e.species === sp; });
      if (entry && entry.best && window.openLightbox) {
        // Keep the same array object as pages are appended so the shared
        // lightbox immediately sees newly loaded photos.
        lifeListLightboxSpecies = entry.species;
        openLightbox(entry.best.id, entry.best.filename, entry.photos || [entry.best]);
      }
    });
  });
}
