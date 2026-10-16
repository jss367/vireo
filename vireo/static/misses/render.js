// Rendering: the page root, each category section and its cards, and section
// collapse.
// Classic page script; load boot.js after all definitions.

function render() {
  var root = document.getElementById('missesRoot');
  var total = missesData.no_subject.length + missesData.clipped.length + missesData.oof.length;
  if (total === 0) {
    root.innerHTML =
      '<div class="misses-empty-page">' +
      '<p>No misses detected — either you nailed every shot, or detection hasn\'t run yet.</p>' +
      '<p><a href="/pipeline">Go to Process</a> to run miss detection on your folders.</p>' +
      '</div>';
    focusedCategory = null;
    focusedIndex = -1;
    selection.clear();
    updateSelectionBar();
    return;
  }

  var html = '';
  CATEGORY_ORDER.forEach(function(cat) {
    html += renderSection(cat, missesData[cat]);
  });
  html +=
    '<div class="misses-hint">' +
      'Shortcuts: ' +
      '<kbd>J</kbd>/<kbd>K</kbd> next/prev &middot; ' +
      '<kbd>Shift+J/K</kbd> extend selection &middot; ' +
      '<kbd>U</kbd>/<kbd>P</kbd>/<kbd>X</kbd> unflag/flag/reject (focused or selection) &middot; ' +
      '<kbd>B</kbd> open focused in Browse &middot; ' +
      '<kbd>Esc</kbd> clear selection &middot; ' +
      'Click to select; Shift-click range; double-click to open lightbox' +
    '</div>';
  root.innerHTML = html;
  updateSelectionBar();
}

function renderSection(cat, photos) {
  var count = photos.length;
  var label = CATEGORY_LABELS[cat];
  var sectionId = 'missesSection-' + cat;
  var bodyHtml;
  if (count === 0) {
    bodyHtml = '<div class="misses-empty">No photos in this category.</div>';
  } else {
    bodyHtml = '<div class="misses-grid" data-cat="' + cat + '">';
    photos.forEach(function(p, idx) {
      bodyHtml += renderCard(cat, p, idx);
    });
    bodyHtml += '</div>';
  }
  return (
    '<section class="misses-section" id="' + sectionId + '" data-cat="' + cat + '">' +
      '<div class="misses-section-header" onclick="toggleSection(\'' + cat + '\', event)">' +
        '<span class="caret">&#9662;</span>' +
        '<span class="misses-category-badge cat-' + cat + '"></span>' +
        '<span class="misses-section-title">' + escapeHtml(label) + '</span>' +
        '<span class="misses-section-count" data-testid="miss-count-' + cat + '">(' + count + ')</span>' +
        '<div class="misses-section-actions">' +
          '<button class="misses-reject-btn" ' +
            'data-testid="miss-reject-' + cat + '" ' +
            'onclick="bulkReject(\'' + cat + '\', event)" ' +
            ((count === 0 || previewActive) ? 'disabled' : '') + '>' +
            'Reject all in ' + escapeHtml(label) +
          '</button>' +
        '</div>' +
      '</div>' +
      '<div class="misses-section-body">' +
        bodyHtml +
      '</div>' +
    '</section>'
  );
}

function renderCard(cat, p, idx) {
  var bbox = parseDetectionBox(p.detection_box);
  var bboxHtml = '';
  if (!bbox && cat === 'no_subject') {
    var floor = p.detector_confidence_threshold != null
      ? Number(p.detector_confidence_threshold)
      : (activeMissFloor != null
          ? activeMissFloor
          : (missConfig && missConfig.detector_confidence != null
              ? Number(missConfig.detector_confidence) : 0.20));
    bboxHtml = '<div class="no-subject-frame">No detection above ' +
      Math.round(floor * 100) + '%</div>';
  }

  var meta = '';
  if (cat === 'clipped' && p.subject_size != null) {
    meta = (p.subject_size * 100).toFixed(1) + '%';
  } else if (cat === 'oof' && p.subject_tenengrad != null && p.bg_tenengrad) {
    meta = 'r=' + (p.subject_tenengrad / p.bg_tenengrad).toFixed(2);
  } else if (cat === 'no_subject' && p.detection_conf != null) {
    meta = 'c=' + Number(p.detection_conf).toFixed(2);
  } else if (cat === 'no_subject' && p.raw_detection_conf != null) {
    meta = 'best candidate ' + Math.round(Number(p.raw_detection_conf) * 100) + '%';
  }

  var ariaLabel = escapeAttr(
    (CATEGORY_LABELS[cat] || cat) + ' miss: ' + (p.filename || ('photo ' + p.id))
  );

  var selectedCls = selection.has(p.id) ? ' selected' : '';
  return (
    '<div class="miss-card' + selectedCls + '" ' +
      'data-cat="' + cat + '" ' +
      'data-idx="' + idx + '" ' +
      'data-photo-id="' + p.id + '" ' +
      'data-filename="' + escapeAttr(p.filename || '') + '" ' +
      'data-testid="miss-card-' + cat + '-' + p.id + '" ' +
      'aria-label="' + ariaLabel + '" ' +
      'onclick="onCardClick(\'' + cat + '\', ' + idx + ', ' + p.id + ', event)" ' +
      'ondblclick="onCardDoubleClick(\'' + cat + '\', ' + idx + ', event)">' +
      '<div class="miss-card-img-wrap">' +
        '<img src="' + escapeAttr(photoThumbnailUrl(p)) + '" loading="lazy" alt="' + escapeAttr(p.filename || '') + '">' +
        bboxHtml +
        '<span class="miss-card-badge">' +
          '<span class="dot cat-' + cat + '"></span>' +
          escapeHtml(CATEGORY_LABELS[cat]) +
        '</span>' +
        (p.is_species_representative ? '<span class="miss-card-representative">Representative</span>' : '') +
        '<button class="miss-card-unflag" ' +
          'title="Unflag as miss (U)" ' +
          'data-testid="miss-unflag-' + cat + '-' + p.id + '" ' +
          (previewActive ? 'disabled ' : '') +
          'onclick="unflagMiss(\'' + cat + '\', ' + p.id + ', event)">&times;</button>' +
      '</div>' +
      '<div class="miss-card-info">' +
        '<span class="miss-card-filename">' + escapeHtml(p.filename || '') + '</span>' +
        '<span class="miss-card-meta">' + escapeHtml(meta) + '</span>' +
      '</div>' +
    '</div>'
  );
}

/* ---------- Section collapse ---------- */
function toggleSection(cat, e) {
  if (e && e.target && e.target.closest('button')) return;
  var sec = document.getElementById('missesSection-' + cat);
  if (sec) sec.classList.toggle('collapsed');
}
