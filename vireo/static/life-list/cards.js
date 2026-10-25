// Species cards, paging more photos per species, and rendering the grid.

import { state } from './state.js';
import { sortedSpecies } from './filters.js';
import { formatDate } from './list.js';
import { escapeAttr, hasLightbox, openLightbox, safeFetch, thumbnailUrl, VireoTextSearch } from './shared.js';

export const LIFE_LIST_PAGE_SIZE = 100;

/** @param {LifePhoto} photo */ export function photoThumbnailUrl(photo) {
  if (!photo || photo.id == null) return '';
  return thumbnailUrl(photo);
}

/** @param {LifeSpecies} entry @param {number} displayNumber */ export function renderCard(entry, displayNumber) {
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

/** @param {string | null} species */ export function lifeListEntry(species) {
  return (state.currentData && state.currentData.species || []).find(function(entry) {
    return entry.species === species;
  });
}

/** @param {LifeSpecies | undefined} entry */ export function loadMoreLifeListPhotos(entry) {
  if (!entry || !entry.has_more) return Promise.resolve(entry ? entry.photos : []);
  const pending = state.lifeListLoadPromises[entry.species]; if (pending) {
    return pending;
  }
  entry.loading_more = true;
  render();
  var params = new URLSearchParams({
    species: entry.species,
    offset: String((entry.photos || []).length),
    limit: String(LIFE_LIST_PAGE_SIZE),
  });
  /** @type {Promise<{photos?:LifePhoto[];loaded_count?:number;has_more?:boolean}>} */ var request = safeFetch('/api/life-list/species?' + params.toString());
  var promise = request.then(function(data) {
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
      delete state.lifeListLoadPromises[entry.species];
      render();
    });
  state.lifeListLoadPromises[entry.species] = promise;
  return promise;
}

export function render() {
  var content = /** @type {HTMLElement} */ (document.getElementById('content'));
  var empty = /** @type {HTMLElement} */ (document.getElementById('emptyState'));
  var meta = /** @type {HTMLElement} */ (document.getElementById('meta'));
  var controls = /** @type {HTMLElement} */ (document.getElementById('controlsBar'));

  if (!state.currentData || !state.currentData.species.length) {
    content.innerHTML = '';
    meta.textContent = '';
    controls.style.display = 'none';
    empty.style.display = 'block';
    return;
  }
  empty.style.display = 'none';
  controls.style.display = '';

  var search = /** @type {HTMLInputElement} */ (document.getElementById('speciesSearch')).value.trim();
  var taxonomicGroup = /** @type {HTMLSelectElement} */ (document.getElementById('taxonomicGroupSelect')).value;
  var identificationRank = /** @type {HTMLSelectElement} */ (document.getElementById('identificationRankSelect')).value;
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
  meta.textContent = state.currentData.meta.species_count + ' species on your life list · '
    + state.currentData.meta.photo_count + ' tagged photos'
    + (filtersActive ? ' · ' + list.length + ' shown' : '');

  var renumberView = /** @type {HTMLInputElement} */ (document.getElementById('renumberView')).checked;
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
      var entry = (state.currentData?.species || []).find(function(e) { return e.species === sp; });
      if (entry && entry.best && hasLightbox()) {
        // Keep the same array object as pages are appended so the shared
        // lightbox immediately sees newly loaded photos.
        state.lifeListLightboxSpecies = entry.species;
        openLightbox(entry.best.id, entry.best.filename, entry.photos || [entry.best]);
      }
    });
  });
}
