/* The Map page: Leaflet layers, the marker cluster and species legend,
   photo loading and popups, and the filter bar. A classic script that
   map.html loads deferred after Leaflet and map-container-size.js and
   before map-selection.js, map-status.js and map-focus.js, the order the
   inline script it came from ran in. */
/* ── Tile layers ── */
var streetLayer = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  attribution: '&copy; OpenStreetMap contributors',
  maxZoom: 19,
});
var satelliteLayer = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
  attribution: '&copy; Esri',
  maxZoom: 19,
});
var terrainLayer = L.tileLayer('https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png', {
  attribution: '&copy; OpenTopoMap',
  maxZoom: 17,
});

var map = createMapWithContainerResize('map', { layers: [streetLayer] }).setView([20, 0], 2);
L.control.layers({ 'Street': streetLayer, 'Satellite': satelliteLayer, 'Terrain': terrainLayer }).addTo(map);

/* ── Marker cluster group ── */
var markers = L.markerClusterGroup();
map.addLayer(markers);

/* ── Species color palette ── */
var SPECIES_COLORS = [
  '#e6194b', '#3cb44b', '#ffe119', '#4363d8',
  '#f58231', '#911eb4', '#42d4f4', '#f032e6'
];
var NO_SPECIES_COLOR = '#5b8fa8';
var OTHER_SPECIES_COLOR = '#888888';
/** @type {Record<string, string>} */
var speciesColorMap = {};

/** @param {MapPhoto[]} photos */
function assignSpeciesColors(photos) {
  /** @type {Record<string, number>} */
  var counts = {};
  photos.forEach(function(p) {
    if (p.species) counts[p.species] = (counts[p.species] || 0) + 1;
  });
  var sorted = Object.keys(counts).sort(function(a, b) { return (counts[b] || 0) - (counts[a] || 0); });
  speciesColorMap = {};
  sorted.slice(0, SPECIES_COLORS.length).forEach(function(sp, i) {
    speciesColorMap[sp] = SPECIES_COLORS[i] || OTHER_SPECIES_COLOR;
  });
}

/** @param {string | null | undefined} species */
function getSpeciesColor(species) {
  if (!species) return NO_SPECIES_COLOR;
  return speciesColorMap[species] || OTHER_SPECIES_COLOR;
}

/** @param {string} color */
function makeCircleIcon(color) {
  return L.divIcon({
    className: '',
    html: '<div style="width:14px;height:14px;border-radius:50%;background:' + color
      + ';border:2px solid var(--text-primary);box-shadow:0 1px 3px rgba(0,0,0,.4);"></div>',
    iconSize: [14, 14],
    iconAnchor: [7, 7],
    popupAnchor: [0, -8],
  });
}

/* ── Species legend control ── */
// The page has one legend, so its element is a page variable.
/** @type {HTMLElement | null} */
var legendDiv = null;
var LegendControl = L.Control.extend({
  options: { position: 'bottomleft' },
  onAdd: function() {
    var div = L.DomUtil.create('div', 'species-legend');
    legendDiv = div;
    div.innerHTML = '<div class="legend-title">Species</div><div class="legend-items"></div>';
    /** @type {HTMLElement} */ (div.querySelector('.legend-title')).addEventListener('click', function() {
      div.classList.toggle('collapsed');
    });
    L.DomEvent.disableClickPropagation(div);
    L.DomEvent.disableScrollPropagation(div);
    return div;
  },
  /** @param {Record<string, string>} colorMap */
  update: function(colorMap) {
    if (!legendDiv) return;
    var items = /** @type {HTMLElement} */ (legendDiv.querySelector('.legend-items'));
    var html = '';
    var entries = Object.keys(colorMap).sort();
    entries.forEach(function(sp) {
      html += '<div class="legend-item"><span class="species-dot" style="background:'
        + colorMap[sp] + '"></span>' + escapeHtml(sp) + '</div>';
    });
    if (entries.length > 0) {
      html += '<div class="legend-item"><span class="species-dot" style="background:'
        + OTHER_SPECIES_COLOR + '"></span>Other</div>';
    }
    html += '<div class="legend-item"><span class="species-dot" style="background:'
      + NO_SPECIES_COLOR + '"></span>No species</div>';
    items.innerHTML = html;
    legendDiv.style.display = '';
  },
});
var legendControl = new LegendControl();
legendControl.addTo(map);

/* ── Helpers ── */
/** @param {number | null | undefined} rating */
function buildStars(rating) {
  if (!rating) return '';
  var s = '';
  for (var i = 0; i < 5; i++) s += i < rating ? '\u2605' : '\u2606';
  return '<span class="popup-stars">' + s + '</span>';
}

/** @param {string | null | undefined} ts */
function formatDate(ts) {
  if (!ts) return '';
  return window.VireoTime.captureDateTime(ts);
}

/* ── State for bi-directional sync ── */
/** @type {Record<string, L.Marker>} */
var photoMarkers = {};  /* photo id -> marker */
/** @type {number | null} */
var activePhotoId = null;
/* Keep both kinds of map work bounded. Leaflet clustering is efficient once
 * markers exist, but creating thousands of markers in one JavaScript turn
 * still blocks navigation and animation. Likewise, a sidebar DOM node (and
 * thumbnail) for every result can exhaust the webview on large libraries. */
var MAP_RENDER_BATCH_SIZE = 100;
var SIDEBAR_CARD_HEIGHT = 69;
var SIDEBAR_OVERSCAN = 8;
/** @type {MapPhoto[]} */
var sidebarPhotos = [];
/** @type {Record<string, number>} */
var sidebarPhotoIndexes = {};
/** @type {HTMLElement | null} */
var sidebarSpacer = null;
/** @type {number | null} */
var sidebarRenderFrame = null;
var requestedPhotoId = (function() {
  var raw = new URLSearchParams(window.location.search).get('photo_id');
  if (!raw) return null;
  var parsed = parseInt(raw, 10);
  return isNaN(parsed) ? null : parsed;
})();

/** @param {number | null} photoId */
function setActiveCard(photoId) {
  if (activePhotoId !== null) {
    var prev = document.querySelector('.sidebar-card[data-id="' + activePhotoId + '"]');
    if (prev) prev.classList.remove('active');
  }
  activePhotoId = photoId;
  if (photoId !== null) {
    var card = document.querySelector('.sidebar-card[data-id="' + photoId + '"]');
    if (!card && Object.prototype.hasOwnProperty.call(sidebarPhotoIndexes, photoId)) {
      var list = /** @type {HTMLElement} */ (document.getElementById('sidebarList'));
      list.scrollTop = Math.max(0,
        (sidebarPhotoIndexes[photoId] || 0) * SIDEBAR_CARD_HEIGHT
          - Math.floor(list.clientHeight / 2));
      renderSidebarWindow();
      card = document.querySelector('.sidebar-card[data-id="' + photoId + '"]');
    }
    if (card) {
      card.classList.add('active');
      card.scrollIntoView({ block: 'nearest' });
    }
  }
}

/** @param {number} photoId */
function openPhotoMarker(photoId) {
  var found = photoMarkers[photoId];
  if (!found) return false;
  var marker = found;
  setActiveCard(photoId);
  markers.zoomToShowLayer(marker, function() {
    map.setView(marker.getLatLng(), Math.max(map.getZoom(), 14));
    marker.openPopup();
  });
  return true;
}

/** @param {MapPhoto | null | undefined} p */
function photoThumbnailUrl(p) {
  if (!p || p.id == null) return '';
  return window.vireoThumbnailUrl
    ? window.vireoThumbnailUrl(p)
    : '/thumbnails/' + p.id + '.jpg';
}

/* ── Virtualized sidebar rendering ── */
/** @param {MapPhoto} p @param {number} index */
function makeSidebarCard(p, index) {
    var card = document.createElement('div');
    card.className = 'sidebar-card';
    if (p.id === activePhotoId) card.classList.add('active');
    card.setAttribute('data-id', String(p.id));
    card.style.top = (index * SIDEBAR_CARD_HEIGHT) + 'px';

    var thumbSrc = photoThumbnailUrl(p);
    var color = getSpeciesColor(p.species);
    var speciesHtml = p.species
      ? '<div class="card-species"><span class="species-dot" style="background:' + color + '"></span>'
        + escapeHtml(p.species) + '</div>'
      : '';

    card.innerHTML =
      (thumbSrc ? '<img src="' + escapeAttr(thumbSrc) + '" alt="" loading="lazy" decoding="async" onerror="this.style.visibility=\'hidden\'">' : '<div style="width:56px;height:56px;background:var(--bg-tertiary,#14374E);border-radius:4px;flex-shrink:0"></div>')
      + '<div class="card-info">'
      + '<div class="card-filename">' + escapeHtml(p.filename) + '</div>'
      + speciesHtml
      + '<div class="card-meta">' + escapeHtml(formatDate(p.timestamp)) + '</div>'
      + '</div>';

    card.addEventListener('click', function() {
      openPhotoMarker(p.id);
    });

    return card;
}

function renderSidebarWindow() {
  if (!sidebarSpacer) return;
  var list = /** @type {HTMLElement} */ (document.getElementById('sidebarList'));
  var visibleCount = Math.max(1, Math.ceil(list.clientHeight / SIDEBAR_CARD_HEIGHT));
  var start = Math.max(0,
    Math.floor(list.scrollTop / SIDEBAR_CARD_HEIGHT) - SIDEBAR_OVERSCAN);
  var end = Math.min(sidebarPhotos.length,
    start + visibleCount + SIDEBAR_OVERSCAN * 2);
  var fragment = document.createDocumentFragment();

  for (var i = start; i < end; i++) {
    fragment.appendChild(makeSidebarCard(/** @type {MapPhoto} */ (sidebarPhotos[i]), i));
  }
  sidebarSpacer.replaceChildren(fragment);
}

/** @param {MapPhoto[]} photos */
function renderSidebar(photos) {
  var list = /** @type {HTMLElement} */ (document.getElementById('sidebarList'));
  list.innerHTML = '';
  list.scrollTop = 0;
  sidebarPhotos = photos;
  sidebarPhotoIndexes = {};
  photos.forEach(function(p, index) { sidebarPhotoIndexes[p.id] = index; });
  /** @type {HTMLElement} */ (document.getElementById('sidebarHeader')).textContent = photos.length + ' photo' + (photos.length !== 1 ? 's' : '');

  if (photos.length === 0) {
    sidebarSpacer = null;
    return;
  }

  sidebarSpacer = document.createElement('div');
  sidebarSpacer.className = 'sidebar-virtual-spacer';
  sidebarSpacer.style.height = (photos.length * SIDEBAR_CARD_HEIGHT) + 'px';
  list.appendChild(sidebarSpacer);
  renderSidebarWindow();
}

/** @type {HTMLElement} */ (document.getElementById('sidebarList')).addEventListener('scroll', function() {
  if (sidebarRenderFrame !== null) return;
  sidebarRenderFrame = requestAnimationFrame(function() {
    sidebarRenderFrame = null;
    renderSidebarWindow();
  });
});

/* ── Cluster preview ── */
var clusterPreviewEl = /** @type {HTMLElement} */ (document.getElementById('clusterPreview'));

/** @param {L.MarkerClusterEvent} e */
function showClusterPreview(e) {
  var cluster = e.layer;
  var childMarkers = cluster.getAllChildMarkers();
  var previewCount = Math.min(childMarkers.length, 6);

  var gridHtml = '<div class="preview-grid">';
  for (var i = 0; i < previewCount; i++) {
    var p = /** @type {L.Marker} */ (childMarkers[i])._photoData;
    if (p && p.id != null) {
      gridHtml += '<img src="' + escapeAttr(photoThumbnailUrl(p)) + '"'
        + ' alt="" onerror="this.style.visibility=\'hidden\'">';
    } else {
      gridHtml += '<div style="width:56px;height:56px;background:var(--bg-tertiary,#14374E);border-radius:3px"></div>';
    }
  }
  gridHtml += '</div>';
  if (childMarkers.length > 6) {
    gridHtml += '<div class="preview-more">+' + (childMarkers.length - 6) + ' more</div>';
  }

  clusterPreviewEl.innerHTML = gridHtml;
  clusterPreviewEl.style.display = 'block';

  var pt = map.latLngToContainerPoint(cluster.getLatLng());
  var mapRect = /** @type {HTMLElement} */ (document.getElementById('map')).getBoundingClientRect();
  clusterPreviewEl.style.left = (mapRect.left + pt.x + 20) + 'px';
  clusterPreviewEl.style.top = (mapRect.top + pt.y - 30) + 'px';
}

function hideClusterPreview() {
  clusterPreviewEl.style.display = 'none';
}

markers.on('clustermouseover', showClusterPreview);
markers.on('clustermouseout', hideClusterPreview);
markers.on('clusterclick', hideClusterPreview);

/* Bumped by every loadPhotos() invocation; responses whose epoch is no
 * longer current are dropped. Fast rule edits or a slow visual query
 * followed by another filter change can otherwise let an earlier
 * ``/api/photos/geo`` resolve after a newer one and overwrite the
 * markers/sidebar/total with stale data while the filter chips show the
 * current rules. */
var _loadPhotosEpoch = 0;

function nextMapRenderFrame() {
  return new Promise(function(resolve) { requestAnimationFrame(resolve); });
}

/** @param {MapPhoto} p */
function makePhotoMarker(p) {
  var thumbSrc = photoThumbnailUrl(p);
  var imgTag = thumbSrc
    ? '<img src="' + escapeAttr(thumbSrc) + '" alt="" loading="lazy" decoding="async" onerror="this.style.display=\'none\'">'
    : '';
  var speciesLine = p.species
    ? '<div class="popup-species">' + escapeHtml(p.species) + '</div>'
    : '';

  var provenanceLine = '';
  if (p.coord_source === 'keyword' && p.keyword_location_name) {
    provenanceLine = '<div class="popup-provenance">\u{1F4CD} from assigned location: '
      + escapeHtml(p.keyword_location_name) + '</div>';
  } else if (p.coord_source === 'exif') {
    provenanceLine = '<div class="popup-provenance">\u{1F4CD} from EXIF GPS</div>';
  }

  var popup = '<div class="photo-popup">'
    + imgTag
    + '<div class="popup-title">' + escapeHtml(p.filename) + '</div>'
    + '<div class="popup-meta">' + escapeHtml(formatDate(p.timestamp)) + ' ' + buildStars(p.rating) + '</div>'
    + speciesLine
    + '<a class="popup-link" href="/browse?photo_id=' + p.id + '">View in Browse &rarr;</a>'
    + provenanceLine
    + '</div>';

  var marker = L.marker([p.latitude, p.longitude], {
    icon: makeCircleIcon(getSpeciesColor(p.species)),
  });
  marker.bindPopup(popup);
  marker._photoData = p;
  marker.on('click', function() { setActiveCard(p.id); });
  return marker;
}

/** @param {MapPhoto[]} photos @param {number} epoch */
async function renderPhotoMarkers(photos, epoch) {
  for (var offset = 0; offset < photos.length; offset += MAP_RENDER_BATCH_SIZE) {
    if (epoch !== _loadPhotosEpoch) return false;
    var end = Math.min(offset + MAP_RENDER_BATCH_SIZE, photos.length);
    /** @type {L.Marker[]} */
    var markerBatch = [];

    for (var i = offset; i < end; i++) {
      var p = /** @type {MapPhoto} */ (photos[i]);
      var marker = makePhotoMarker(p);
      photoMarkers[p.id] = marker;
      markerBatch.push(marker);
    }

    /* addLayers() lets MarkerCluster process a bounded batch together. The
     * old one-at-a-time addLayer() loop caused its clustering/layout work to
     * monopolize the UI thread for large libraries. */
    if (markers.addLayers) {
      markers.addLayers(markerBatch);
    } else {
      markerBatch.forEach(function(marker) { markers.addLayer(marker); });
    }

    /** @type {HTMLElement} */ (document.getElementById('mapStatus')).textContent =
      'Plotting ' + end + ' of ' + photos.length + ' photos\u2026';
    if (end < photos.length) await nextMapRenderFrame();
  }
  return epoch === _loadPhotosEpoch;
}

/* ── Load photos ── */
async function loadPhotos() {
  var myEpoch = ++_loadPhotosEpoch;
  var focusPhotoId = requestedPhotoId;
  requestedPhotoId = null;
  var params = new URLSearchParams();
  /** @type {unknown} */
  var rules = null;
  /** @type {unknown} */
  var visual = null;
  // When the user follows a Browse "View on Map" deep link (``?photo_id=``),
  // skip any restored/persisted filter rules for this one load — otherwise a
  // saved rule that excludes the requested photo silently hides its marker
  // and the page reports "No map location found" even though the photo is
  // plottable. The filter chips stay on-screen and reapply on the next
  // filter-driven reload; only this focus-triggered request is unfiltered.
  if (focusPhotoId === null && window.VireoFilter && VireoFilter.getRules) {
    var filterRules = VireoFilter.getRules();
    var count = Array.isArray(filterRules) ? filterRules.length : (filterRules.rules || []).length;
    rules = count ? filterRules : null;
    if (rules) params.set('rules', JSON.stringify(rules));
    visual = VireoFilter.getVisual ? VireoFilter.getVisual() : null;
    if (visual) params.set('visual', JSON.stringify(visual));
  }
  if (focusPhotoId !== null) params.set('photo_id', String(focusPhotoId));

  var qs = params.toString();
  var url = '/api/photos/geo' + (qs ? '?' + qs : '');

  try {
    var data = mapSelectionIds
      ? await fetchMapSelection(rules, visual)
      : await /** @type {Promise<MapPageResponse>} */ (safeFetch(url));
    if (myEpoch !== _loadPhotosEpoch) return;  // superseded — drop
    if (window.VireoFilter && VireoFilter.setResultTotal) {
      VireoFilter.setResultTotal(data.total_filtered);
    }
    // Surface the visual clause's status so the filter bar can warn on
    // fallback (no model / no embeddings / encoding failed). Without
    // this, choosing "Visually similar…" on a map without a usable
    // visual index would silently show every metadata-matching photo.
    if (window.VireoFilter && VireoFilter.setVisualInfo) {
      VireoFilter.setVisualInfo(data.visual || null);
    }
    hideUnplottableFocusNotice();
    markers.clearLayers();
    photoMarkers = {};
    activePhotoId = null;

    if (data.photos.length === 0) {
      renderSidebar([]);
      var emptyStatus = /** @type {HTMLElement} */ (document.getElementById('mapStatus'));
      if (focusPhotoId !== null && !data.selection) {
        showUnplottableFocusNotice(data.unplottable_focus, loadPhotos);
      } else {
        emptyStatus.innerHTML = mapStatusHtml(data);
      }
      legendControl.update({});
      return;
    }

    assignSpeciesColors(data.photos);
    legendControl.update(speciesColorMap);
    renderSidebar(data.photos);
    if (!await renderPhotoMarkers(data.photos, myEpoch)) return;
    map.fitBounds(markers.getBounds(), { padding: [30, 30] });
    if (focusPhotoId !== null) openPhotoMarker(focusPhotoId);

    if (data.selection || focusPhotoId === null || photoMarkers[focusPhotoId]) {
      /** @type {HTMLElement} */ (document.getElementById('mapStatus')).innerHTML = mapStatusHtml(data);
    }
  } catch (e) {
    if (myEpoch !== _loadPhotosEpoch) return;  // superseded — drop
    /** @type {HTMLElement} */ (document.getElementById('mapStatus')).textContent = 'Failed to load photos.';
  }
}

/* ── Universal filter bar ── */
document.addEventListener('DOMContentLoaded', () => VireoFilter.init({
  page: 'map',
  root: document.getElementById('vireoFilterBar'),
  scopeLabel: mapSelectionScopeLabel() || 'Map \u00b7 Plottable locations',
  onChange: loadPhotos,
}).then(loadPhotos).catch(function() { loadPhotos(); }));
