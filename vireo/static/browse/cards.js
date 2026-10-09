/* Browse: grid card markup (fields, badges, color labels, detection boxes).
   Classic script sharing page globals; browse.html loads the
   browse/*.js files in dependency order. */

function formatFileSize(bytes) {
  if (bytes == null) return '';
  if (bytes < 1024) return bytes + ' B';
  if (bytes < 1048576) return (bytes / 1024).toFixed(1) + ' KB';
  if (bytes < 1073741824) return (bytes / 1048576).toFixed(1) + ' MB';
  return (bytes / 1073741824).toFixed(1) + ' GB';
}

// The card's format label. A RAW+JPEG pair is one photo whose extension is
// the RAW's, with the JPEG in companion_path; label it "NEF + JPG" so a card
// the extension filter matched by its JPEG says why. The companion's format
// comes from its basename, as the server's extension filter reads it.
function cardExtensionLabel(p) {
  var label = p.extension ? String(p.extension).replace(/^\./, '').toUpperCase() : '';
  var base = p.companion_path ? String(p.companion_path).split(/[\\/]/).pop() : '';
  var m = base.match(/^\.*[^.].*(\.[^.]+)$/);
  var companion = m ? m[1].slice(1).toUpperCase() : '';
  if (!companion || companion === label) return label;
  return label ? label + ' + ' + companion : companion;
}

function renderSpeciesBadges(species, inline) {
  if (!Array.isArray(species) || species.length === 0) return '';
  var maxShow = 3;
  var style = inline ? ' style="position:static;padding:0;"' : '';
  var html = '<div class="species-badges"' + style + '>';
  species.slice(0, maxShow).forEach(function(name) {
    html += '<span class="species-badge">' + escapeHtml(name) + '</span>';
  });
  if (species.length > maxShow) {
    html += '<span class="species-badge-overflow">+' + (species.length - maxShow) + ' more</span>';
  }
  return html + '</div>';
}

function renderCardField(field, p) {
  switch (field) {
    case 'filename':
      return '<div class="grid-card-name" title="' + escapeAttr(p.filename) + '">' + escapeHtml(p.filename) + '</div>';
    case 'location_status':
      var locationStatus = p.location_status || 'none';
      var locationLabels = {
        exif: ['📍', 'EXIF GPS', 'Embedded GPS coordinates from the photo'],
        assigned: ['●', 'Assigned map location', 'Map coordinates from an assigned Vireo location'],
        none: ['⊘', 'No coordinates', 'No EXIF GPS or assigned map coordinates']
      };
      var locationLabel = locationLabels[locationStatus] || locationLabels.none;
      return '<span class="grid-location-status ' + locationStatus + '" title="' + escapeAttr(locationLabel[2]) + '">' +
        locationLabel[0] + ' ' + locationLabel[1] + '</span>';
    case 'rating':
      if (p.rating > 0) {
        var stars = '';
        for (var i = 0; i < p.rating; i++) stars += '&#9733;';
        return '<span class="grid-card-rating">' + stars + '</span>';
      }
      return '';
    case 'flag':
      if (p.flag === 'flagged') return '<span class="grid-card-flag flag-flagged">P</span>';
      if (p.flag === 'rejected') return '<span class="grid-card-flag flag-rejected">X</span>';
      return '';
    case 'sharpness':
      if (p.sharpness != null) return '<span style="font-size:10px;color:var(--text-ghost);" title="Sharpness score">' + Math.round(p.sharpness) + '</span>';
      return '';
    case 'species':
      return renderSpeciesBadges(p.species, true);
    case 'dimensions':
      if (p.width && p.height) return '<span style="font-size:10px;color:var(--text-ghost);">' + p.width + ' \u00d7 ' + p.height + '</span>';
      return '';
    case 'file_size':
      if (p.file_size) return '<span style="font-size:10px;color:var(--text-ghost);">' + formatFileSize(p.file_size) + '</span>';
      return '';
    case 'capture_date':
      if (p.timestamp) {
        var d = p.timestamp.replace('T', ' ').substring(0, 16);
        return '<span style="font-size:10px;color:var(--text-ghost);" title="' + escapeAttr(p.timestamp) + '">' + d + '</span>';
      }
      return '';
    case 'extension':
      var extLabel = cardExtensionLabel(p);
      if (!extLabel) return '';
      var extTitle = p.companion_path ? ' title="' + escapeAttr(p.filename + ' + ' + p.companion_path) + '"' : '';
      return '<span class="grid-card-ext" style="font-size:10px;color:var(--text-ghost);"' + extTitle + '>' + escapeHtml(extLabel) + '</span>';
    case 'quality_score':
      if (p.quality_score != null) return '<span style="font-size:10px;color:var(--text-ghost);" title="Quality score">' + p.quality_score.toFixed(2) + '</span>';
      return '';
    // The value the "AI confidence" sorts rank on, so a card can
    // explain its own position in the grid. Absent (rather than 0) when the
    // photo has no current, unrejected species prediction — the badge stays
    // off instead of claiming a confidence of zero.
    case 'prediction_confidence':
      if (p.prediction_confidence != null) {
        // Under a confidence sort a stack is placed by its leading member,
        // not by the quality-ranked cover shown on the card, and the server
        // sends that leading score so the badge names the number that put
        // the item where it is. Say whose score it is rather than letting
        // the card imply it belongs to the frame in the thumbnail.
        //
        // The server flags this per card; do NOT re-derive it from the sort
        // dropdown. A healthy visual clause keeps the grid similarity-ranked
        // while the dropdown still reads "AI confidence", and that
        // path sends the cover's own score — so a select-derived label would
        // claim the relevance order came from a number that did not produce
        // it (Codex P2 on PR #1670).
        var confIsStackLead = p.prediction_confidence_is_stack_lead === true;
        var confTitle = confIsStackLead
          ? 'Confidence of the strongest current species prediction on the frame that leads this stack — the score this stack is sorted by'
          : 'Confidence of the strongest current species prediction';
        return '<span style="font-size:10px;color:var(--text-ghost);" title="'
          + escapeAttr(confTitle) + '">'
          + Math.round(p.prediction_confidence * 100) + '%</span>';
      }
      return '';
    case 'color_label':
      var cl = colorLabels[p.id];
      if (cl) {
        if (!window.VireoColorLabels || !window.VireoColorLabels.isValid(cl)) return '';
        var colorTitle = window.VireoColorLabels
          ? window.VireoColorLabels.title(cl, cl.charAt(0).toUpperCase() + cl.slice(1))
          : cl;
        return '<span class="grid-card-color" data-color="' + escapeAttr(cl)
          + '" data-color-label-control data-color-label-base-title="'
          + escapeAttr(cl.charAt(0).toUpperCase() + cl.slice(1))
          + '" data-color-label-base-aria="'
          + escapeAttr(cl.charAt(0).toUpperCase() + cl.slice(1) + ' label')
          + '" role="img" aria-label="'
          + escapeAttr(cl.charAt(0).toUpperCase() + cl.slice(1) + ' label')
          + '" title="' + escapeAttr(colorTitle) + '"></span>';
      }
      return '';
    default:
      return '';
  }
}

// Color labels live in the async `colorLabels` map rather than on the photo
// row, so the card tint is stamped as an attribute at render time and
// re-stamped by refreshCardBadgesAndInfo once /api/photos/color_labels
// resolves or the user edits a label.
function cardColorLabel(photoId) {
  var cl = colorLabels[photoId];
  if (!cl) return null;
  if (window.VireoColorLabels && !window.VireoColorLabels.isValid(cl)) return null;
  return cl;
}

function cardColorLabelAttr(photoId) {
  var cl = cardColorLabel(photoId);
  return cl ? ' data-color-label="' + escapeAttr(cl) + '"' : '';
}

function applyCardColorLabel(card, photoId) {
  var cl = cardColorLabel(photoId);
  if (cl) card.setAttribute('data-color-label', cl);
  else card.removeAttribute('data-color-label');
}

// Hydrate color labels for the photos an init response just rendered.
// /api/browse/init carries no labels, so any path that paints a grid straight
// from it — bootstrapBrowse() and the ?photo_id=... deep link, which skips
// bootstrap entirely — has to ask for them, or its cards stay untinted and the
// detail panel reports "no color" until some later action happens to refetch
// those ids. loadPhotos()'s paging path does its own fetch and does not use
// this. `isCurrent` is the caller's own staleness guard: a sort change or
// folder click during the fetch means these ids are no longer on screen.
function hydrateColorLabelsForRenderedPage(isCurrent) {
  if (!photos.length) return;
  var ids = photos.map(function(p) { return p.id; });
  return fetchColorLabels(ids).then(function() {
    if (!isCurrent()) return;
    refreshGridCards(ids);
    // fetchColorLabels already re-rendered a batch inspector; only the
    // single-photo panel is left to catch up.
    var detail = document.getElementById('detailContent');
    if (selectedPhotoId != null
        && !(detail && detail.classList.contains('batch-mode'))) {
      updateDetailColors();
    }
  });
}

function renderCardInfo(p) {
  var infoHtml = '';
  cardFields.forEach(function(field) {
    infoHtml += renderCardField(field, p);
  });
  return infoHtml;
}

function renderDetectionBoxes(p) {
  // Shared by renderPhotoCard and renderBrowseStackMember so overlays and their
  // orientation/RAW-pair hiding stay identical between covers and expanded stack
  // members; toggleDetectionBoxes() rerenders through both paths.
  if (!showDetectionBoxes || !Array.isArray(p.detections)) return '';
  var isRawJpegPair = typeof window.vireoPhotoIsRawJpegPair === 'function'
    ? window.vireoPhotoIsRawJpegPair(p)
    : false;
  var pairSource = _vireoPairSource(p.id) || 'jpeg';
  var hideDetectionOverlays = (
    (isRawJpegPair && pairSource === 'jpeg') ||
    (
      typeof window.vireoPhotoHasOrientationEdit === 'function' &&
      window.vireoPhotoHasOrientationEdit(p.id)
    )
  );
  var html = '';
  p.detections.forEach(function(d) {
    if (d == null || d.x == null) return;
    var conf = (d.confidence != null) ? Math.round(d.confidence * 100) + '%' : '';
    var cat = (d.category && d.category !== 'animal') ? escapeHtml(d.category) + ' ' : '';
    html += '<div class="det-box" data-photo-id="' + p.id + '" style="' + (hideDetectionOverlays ? 'display:none;' : '') + 'left:' + (d.x * 100) + '%;top:' + (d.y * 100) + '%;width:' + (d.w * 100) + '%;height:' + (d.h * 100) + '%;">' +
      ((cat || conf) ? '<span class="det-box-label">' + cat + conf + '</span>' : '') +
    '</div>';
  });
  return html;
}

function renderPhotoCard(p, idx) {
  var selectedClass = browseCardSelectionClass(p);
  // Offline members render first and unconditionally: they are read-only
  // placeholders with no thumbnail, and the server never lets one join a
  // stack, so there is no stack chrome to preserve here.
  var isOffline = p.folder_status &&
    p.folder_status !== 'ok' && p.folder_status !== 'partial';
  if (isOffline) {
    return '<div class="grid-card offline" data-id="' + p.id +
      '" data-idx="' + idx + '" data-filename="' + escapeAttr(p.filename) +
      '"' + cardColorLabelAttr(p.id) +
      ' aria-disabled="true" title="This photo\'s folder is offline. Reconnect or relocate the folder to edit it.">' +
      '<div class="grid-card-img-wrap">' +
        '<div class="offline-photo-placeholder">' +
          '<span class="offline-icon">\u26d3</span>' +
          '<span>Folder offline</span>' +
        '</div>' +
      '</div>' +
      '<div class="grid-card-info">' + renderCardInfo(p) + '</div>' +
    '</div>';
  }
  var stackClass = p.browse_stack ? ' has-browse-stack' : '';
  var isRawJpegPair = typeof window.vireoPhotoIsRawJpegPair === 'function'
    ? window.vireoPhotoIsRawJpegPair(p)
    : false;
  if (typeof window.vireoRememberPhotoPair === 'function') {
    window.vireoRememberPhotoPair(p);
  }
  var pairSource = _vireoPairSource(p.id) || 'jpeg';
  if (
    typeof window.vireoRememberPhotoEditRecipe === 'function' &&
    Object.prototype.hasOwnProperty.call(p, 'edit_recipe')
  ) {
    window.vireoRememberPhotoEditRecipe(p.id, p.edit_recipe, {
      skipIfLocallyWritten: true,
    });
  }

  var boxHtml = renderDetectionBoxes(p);

  var clipBadge = '';
  if (p._similarity !== undefined) {
    clipBadge = '<span class="clip-score-badge">' + Math.round(p._similarity * 100) + '%</span>';
  }
  var inatBadge = inatSubmitted[String(p.id)] ? '<span class="inat-badge">iNat</span>' : '';
  var wildlifeBadge = p.wildlife_excluded ? '<span class="no-wildlife-badge">No Wildlife</span>' : '';
  var representativeBadge = p.is_species_representative ? '<span class="representative-badge">Representative</span>' : '';
  var pairBadge = isRawJpegPair
    ? '<span class="pair-source-badge" data-pair-source-id="' + p.id + '">'
      + (pairSource === 'raw' ? 'RAW · JPEG pair' : 'JPEG · RAW pair')
      + '</span>'
    : '';

  // Species badges on image overlay (only when NOT in cardFields — if in cardFields, rendered below)
  var speciesBadgeHtml = '';
  if (cardFields.indexOf('species') === -1) {
    speciesBadgeHtml = renderSpeciesBadges(p.species, false);
  }

  var thumbUrl = window.vireoThumbnailUrl ? window.vireoThumbnailUrl(p) : '/thumbnails/' + p.id + '.jpg';
  return '<div class="grid-card' + stackClass + selectedClass + '" data-id="' + p.id + '" data-idx="' + idx + '" data-filename="' + escapeAttr(p.filename) + '"' + cardColorLabelAttr(p.id) + ' onclick="selectPhoto(event,' + p.id + ',' + idx + ')">' +
    '<div class="grid-card-img-wrap">' +
      '<img data-thumbnail-src="' + escapeAttr(thumbUrl) + '" decoding="async" alt="' + escapeAttr(p.filename) + '">' +
      renderBrowseStackBadge(p) +
      clipBadge +
      boxHtml +
      inatBadge +
      wildlifeBadge +
      representativeBadge +
      (pairBadge
        ? '<div class="grid-card-bottom-overlay">' + speciesBadgeHtml + pairBadge + '</div>'
        : speciesBadgeHtml) +
    '</div>' +
    '<div class="grid-card-info">' +
      renderCardInfo(p) +
    '</div>' +
  '</div>';
}
