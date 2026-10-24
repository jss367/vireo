// The comparison table: each row's photo, signal, keyword and per-model
// prediction cells, and opening a photo in the lightbox.
// Classic page script.

/** @param {ComparePhoto[]} rows */
function renderTable(rows) {
  var content = /** @type {HTMLElement} */ (document.getElementById('compareContent'));
  var models = visibleModels();
  if (!compareTotal && !rows.length) {
    content.innerHTML = (effectiveSummary().photos || 0)
      ? '<div class="compare-empty">No photos match the current filter.</div>'
      : '<div class="compare-empty">This collection has no photos.</div>';
    return;
  }
  if (!rows.length) {
    content.innerHTML = '<div class="compare-empty">No rows on this page.</div>';
    return;
  }

  var html = '<div class="table-wrap"><table class="compare-table"><thead><tr>' +
    '<th class="select-col"><input type="checkbox" title="Select every row on this page" onchange="toggleAllRows(this.checked)"></th>' +
    '<th class="photo-col">Photo</th>' +
    '<th>Status</th>' +
    '<th class="signal-col">Signals</th>' +
    '<th class="keyword-col">Current keywords</th>';
  models.forEach(function(m) {
    html += '<th class="model-col">' + escapeHtml(m) + '</th>';
  });
  html += '</tr></thead><tbody>';

  rows.forEach(function(photo) {
    var status = photo.status || {};
    var multiSubject = (photo.subjects || []).length > 1;
    html += '<tr data-photo-id="' + photo.photo_id + '">' +
      '<td><input type="checkbox" ' + (selectedRows.has(photo.photo_id) ? 'checked' : '') +
      (multiSubject ? ' disabled title="Use the subject-specific actions for multi-subject photos"' :
        ' onchange="toggleRow(' + photo.photo_id + ', this.checked)"') + '></td>' +
      '<td>' + renderPhotoCell(photo) + '</td>' +
      '<td><span class="category-pill ' + escapeAttr(status.category) + '">' +
      escapeHtml(status.label) + '</span></td>' +
      '<td>' + renderSignalCell(photo) + '</td>' +
      '<td>' + renderKeywordCell(photo) + '</td>';
    models.forEach(function(m) {
      html += '<td>' + renderPredictionCell(photo, m) + '</td>';
    });
    html += '</tr>';
  });
  html += '</tbody></table></div>';
  content.innerHTML = html;
}

/** @param {ComparePhoto} photo */
function renderSignalCell(photo) {
  var signal = photo.signal || /** @type {CompareSignal} */ ({});
  var statuses = photo.subject_statuses || [];
  var subjects = photo.subjects || [];
  var pills = [];
  if (subjects.length > 1) {
    pills.push('<span class="signal-pill">' + subjects.length + ' subjects</span>');
  }
  if ((signal.additional_subject_count || 0) > 0) {
    pills.push('<span class="signal-pill match">' + signal.additional_subject_count +
      ' additional species suggestion' + (signal.additional_subject_count === 1 ? '' : 's') + '</span>');
  }
  if ((signal.model_disagreement_score || 0) > 0) {
    pills.push('<span class="signal-pill hot" title="' + escapeAttr(
      signal.model_disagreement_label + ' — compare species names; model scores use different scales and candidate lists') +
      '">Models disagree</span>');
  }
  if ((signal.keyword_conflict_count || 0) > 0) {
    pills.push('<span class="signal-pill hot" title="' + escapeAttr(
        signal.keyword_conflict_count + ' model prediction' +
        (signal.keyword_conflict_count === 1 ? '' : 's') +
        ' conflict with the current species keyword; see each model’s relative score') +
      '">' + signal.keyword_conflict_count + ' vs keyword</span>');
  }
  if ((signal.consensus_count || 0) >= 2) {
    pills.push('<span class="signal-pill match" title="' + escapeAttr(signal.consensus_label) +
      '">' + signal.consensus_count + ' agree</span>');
  }
  if ((signal.missing_visible_model_count || 0) > 0) {
    pills.push('<span class="signal-pill">' + signal.missing_visible_model_count + ' missing</span>');
  }
  if (!pills.length) {
    return '<span class="empty-cell">No comparison signal</span>';
  }
  var detail = [];
  if (signal.model_disagreement_label) detail.push(signal.model_disagreement_label);
  statuses.forEach(function(status, /** @type {number} */ index) {
    var species = (subjects[index] || {}).signal_consensus_species;
    if (status.category === 'additional' && species) {
      detail.push('Subject ' + (index + 1) + ': ' + species);
    }
  });
  if (signal.consensus_label) detail.push(signal.consensus_label);
  return '<div class="signal-list">' + pills.join('') + '</div>' +
    (detail.length ? '<div class="signal-note">' + escapeHtml(detail.join(' · ')) + '</div>' : '');
}

/** @param {CompareSubject} subject */
function subjectBoxLabel(subject) {
  var box = (subject && subject.box) || {};
  if ([box.x, box.y, box.w, box.h].some(function(value) { return value == null; })) return '';
  var x = /** @type {number} */ (box.x), y = /** @type {number} */ (box.y);
  var w = /** @type {number} */ (box.w), h = /** @type {number} */ (box.h);
  return 'box x ' + Math.round(x * 100) + '–' + Math.round((x + w) * 100) +
    '%, y ' + Math.round(y * 100) + '–' + Math.round((y + h) * 100) + '%';
}

/** @param {ComparePhoto} photo */
function renderPhotoCell(photo) {
  var meta = [];
  var thumbUrl = window.vireoThumbnailUrl
    ? window.vireoThumbnailUrl(photo)
    : '/thumbnails/' + photo.photo_id + '.jpg';
  if (photo.timestamp) meta.push(window.VireoTime.captureDateTime(photo.timestamp));
  if (photo.rating) meta.push(photo.rating + ' star' + (photo.rating === 1 ? '' : 's'));
  if (photo.flag && photo.flag !== 'none') meta.push(photo.flag);
  if (photo.wildlife_excluded) meta.push('not wildlife');
  if (photo.miss_no_subject || photo.miss_clipped || photo.miss_oof) meta.push('marked miss');
  var subjects = photo.subjects || [];
  var statuses = photo.subject_statuses || [];
  var subjectSummary = '';
  if (subjects.length > 1) {
    subjectSummary = '<div class="subject-summary-list">' + subjects.map(function(subject, /** @type {number} */ index) {
      var status = statuses[index] || {};
      var species = subject.signal_consensus_species
        ? ' · ' + escapeHtml(subject.signal_consensus_species) : '';
      var location = subjectBoxLabel(subject);
      return '<div class="subject-summary"><strong>Subject ' + (index + 1) + '</strong>' + species +
        ' · <span class="category-pill ' + escapeAttr(status.category) + '">' +
        escapeHtml(status.label) + '</span>' +
        (location ? '<div>' + escapeHtml(location) + '</div>' : '') + '</div>';
    }).join('') + '</div>';
  }
  return '<div class="photo-cell">' +
    '<button type="button" class="photo-thumb-button" title="View larger" onclick="openCompareLightbox(' +
    photo.photo_id + ')"><img src="' + escapeAttr(thumbUrl) + '" loading="lazy" alt="' +
    escapeAttr(photo.filename) + '"></button>' +
    '<div><div class="photo-name">' + escapeHtml(photo.filename) + '</div>' +
    '<div class="photo-meta">' + escapeHtml(meta.join(' · ')) + '</div>' +
    subjectSummary +
    '<div class="pred-actions"><a class="inline-btn" href="/browse?photo_id=' +
    encodeURIComponent(photo.photo_id) + '">Browse</a></div></div></div>';
}

function compareLightboxPhotoList() {
  return compareRows.map(function(photo) {
    return {
      id: photo.photo_id,
      filename: photo.filename || '',
      flag: photo.flag || 'none',
      wildlife_excluded: !!photo.wildlife_excluded,
    };
  });
}

/** @param {number} photoId */
function openCompareLightbox(photoId) {
  var list = compareLightboxPhotoList();
  var current = list.find(function(photo) { return photo.id === photoId; });
  if (!current) {
    current = {id: photoId, filename: '', flag: 'none', wildlife_excluded: false};
    list = [current];
  }
  openLightbox(current.id, current.filename, list);
}

/** @param {CompareKeyword} k */
function isSpeciesKeyword(k) {
  // Mirror the backend species definition. A linked family/genus is a
  // taxonomy keyword, but not a species; unresolved legacy tags retain the
  // old flag/type fallback.
  var markedTaxonomy = !!k.is_species || k.type === 'taxonomy';
  return markedTaxonomy && (!k.taxon_rank || k.taxon_rank === 'species');
}

/** @param {ComparePhoto} photo */
function renderKeywordCell(photo) {
  var species = (photo.keywords || []).filter(isSpeciesKeyword);
  var other = (photo.keywords || []).filter(function(k) { return !isSpeciesKeyword(k); });
  var html = '';
  if (species.length) {
    html += '<div class="keyword-group">' + species.map(function(k) {
      return '<span class="keyword-pill species" title="Species keyword">' +
        escapeHtml(k.name) + '</span>';
    }).join('') + '</div>';
  } else {
    html += '<div class="empty-cell">No species keyword</div>';
  }
  if (other.length) {
    html += '<div class="keyword-group">' + other.map(function(k) {
      return '<span class="keyword-pill general" title="' + escapeAttr(k.type || 'keyword') + '">' +
        escapeHtml(k.name) + '</span>';
    }).join('') + '</div>';
  }
  return html;
}

/** @param {ComparePhoto} photo @param {string} model */
function renderPredictionCell(photo, model) {
  var subjects = photo.subjects || [];
  var statuses = photo.subject_statuses || [];
  if (!subjects.length) return '<span class="empty-cell">No prediction</span>';
  return subjects.map(function(subject, /** @type {number} */ subjectIndex) {
    var preds = (subject.predictions || {})[model] || [];
    var subjectStatus = statuses[subjectIndex] || {};
    var heading = subjects.length > 1
      ? '<div class="subject-heading"><span>Subject ' + (subjectIndex + 1) + '</span>' +
        '<span class="category-pill ' + escapeAttr(subjectStatus.category) + '">' +
        escapeHtml(subjectStatus.label) + '</span></div>'
      : '';
    if (!preds.length) {
      return '<div class="subject-prediction-group">' + heading +
        '<span class="empty-cell">No prediction for this subject</span></div>';
    }
    var predictionsHtml = preds.map(function(pred) {
      var status = pred.status || 'pending';
      var disabled = status !== 'pending' ? ' disabled' : '';
      var detail = pred.category_detail || '';
      var belowThreshold = pred.effective_category === 'low_conflict';
      var displayedCategory = subjectStatus.category === 'additional' && pred.category === 'conflict'
        ? 'additional' : pred.category;
      var displayedLabel = displayedCategory === 'additional'
        ? subjectStatus.label : pred.category_label;
      if (pred.matched_keyword) detail += ' Keyword: ' + pred.matched_keyword + '.';
      if (pred.shared_rank) detail += ' Shared ' + pred.shared_rank + '.';
      if (belowThreshold) detail += ' Below conflict threshold.';
      if (displayedCategory === 'additional') {
        detail = 'A different detected subject accounts for the current species keyword. ' +
          'This prediction may identify another species in the photo.';
      }
      var acceptLabel = displayedCategory === 'additional' ? 'Add additional species' : 'Add keyword';
      var acceptAction = displayedCategory === 'additional' ? 'accept_subject' : 'accept';
      var replaceButton = subjects.length === 1
        ? '<button class="inline-btn primary" onclick="predictionAction(' + pred.id + ', \'replace\')"' + disabled + '>Replace keyword</button>'
        : '';
      return '<div class="pred' + (belowThreshold ? ' below-threshold' : '') + '">' +
        '<div class="pred-main"><span class="pred-species">' + escapeHtml(pred.species) + '</span>' +
        '<span class="pred-confidence">' + ((pred.confidence || 0) * 100).toFixed(2) + '% relative</span>' +
        '<span class="category-pill ' + escapeAttr(displayedCategory) + '">' + escapeHtml(displayedLabel) + '</span>' +
        '<span class="status-pill ' + escapeAttr(status) + '">' + escapeHtml(status) + '</span></div>' +
        '<div class="pred-detail">' + escapeHtml(detail) + '</div>' +
        '<div class="pred-actions">' +
        '<button class="inline-btn primary" onclick="predictionAction(' + pred.id + ', \'' + acceptAction + '\')"' + disabled + '>' + acceptLabel + '</button>' +
        replaceButton +
        '<button class="inline-btn danger" onclick="predictionAction(' + pred.id + ', \'reject\')"' + disabled + '>Reject</button>' +
        '<button class="inline-btn" onclick="predictionAction(' + pred.id + ', \'reviewed\')"' + disabled + '>Reviewed</button>' +
        '</div></div>';
    }).join('');
    return '<div class="subject-prediction-group">' + heading + predictionsHtml + '</div>';
  }).join('');
}
