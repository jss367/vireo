// Rendering: the page root, each category section and its cards, and section
// collapse. Cards and section headers carry data attributes, not inline
// handlers; card-events.js binds one delegated listener for them.
// ES module.

import { state, CATEGORY_ORDER, CATEGORY_LABELS } from './state.js';
import { escapeAttr, escapeHtml } from './shared.js';
import { parseDetectionBox, photoThumbnailUrl } from './helpers.js';
import { updateSelectionBar } from './selection.js';

export function render() {
  var root = /** @type {HTMLElement} */ (document.getElementById('missesRoot'));
  var total = state.missesData.no_subject.length + state.missesData.clipped.length + state.missesData.oof.length;
  if (total === 0) {
    root.innerHTML =
      '<div class="misses-empty-page">' +
      '<p>No misses detected — either you nailed every shot, or detection hasn\'t run yet.</p>' +
      '<p><a href="/pipeline">Go to Process</a> to run miss detection on your folders.</p>' +
      '</div>';
    state.focusedCategory = null;
    state.focusedIndex = -1;
    state.selection.clear();
    updateSelectionBar();
    return;
  }

  var html = '';
  CATEGORY_ORDER.forEach(function(cat) {
    html += renderSection(cat, state.missesData[cat]);
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

/** @param {MissCategory} cat @param {MissPhoto[]} photos */ function renderSection(cat, photos) {
  var count = photos.length;
  var label = CATEGORY_LABELS[cat];
  var sectionId = 'missesSection-' + cat;
  var bodyHtml = '';
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
      '<div class="misses-section-header" data-cat="' + cat + '">' +
        '<span class="caret">&#9662;</span>' +
        '<span class="misses-category-badge cat-' + cat + '"></span>' +
        '<span class="misses-section-title">' + escapeHtml(label) + '</span>' +
        '<span class="misses-section-count" data-testid="miss-count-' + cat + '">(' + count + ')</span>' +
        '<div class="misses-section-actions">' +
          '<button class="misses-reject-btn" ' +
            'data-testid="miss-reject-' + cat + '" ' +
            'data-cat="' + cat + '" ' +
            ((count === 0 || state.previewActive) ? 'disabled' : '') + '>' +
            'Reject all in ' + escapeHtml(label) +
          '</button>' +
        '</div>' +
      '</div>' +
      '<div class="misses-section-body">' +
        (cat === 'oof' ? '<p>Heuristic review candidates based on image gradients; absolute defocus is unvalidated. Exposure, texture and the selected region can affect these measurements. Inspect before rejecting.</p>' : '') +
        bodyHtml +
      '</div>' +
    '</section>'
  );
}

/** @param {MissCategory} cat @param {MissPhoto} p @param {number} idx */ function renderCard(cat, p, idx) {
  var bbox = parseDetectionBox(p.detection_box);
  var bboxHtml = '';
  if (!bbox && cat === 'no_subject') {
    var floor = p.detector_confidence_threshold != null
      ? Number(p.detector_confidence_threshold)
      : (state.activeMissFloor != null
          ? state.activeMissFloor
          : (state.missConfig && state.missConfig.detector_confidence != null
              ? Number(state.missConfig.detector_confidence) : 0.20));
    bboxHtml = '<div class="no-subject-frame">No detection above ' +
      Math.round(floor * 100) + '%</div>';
  }

  var meta = '';
  if (cat === 'clipped' && p.subject_size != null) {
    meta = (p.subject_size * 100).toFixed(1) + '%';
  } else if (cat === 'oof' && p.subject_tenengrad != null && p.bg_tenengrad) {
    meta = 'Subject/background gradients: ' + (p.subject_tenengrad / p.bg_tenengrad).toFixed(2);
  } else if (cat === 'no_subject' && p.detection_conf != null) {
    meta = 'c=' + Number(p.detection_conf).toFixed(2);
  } else if (cat === 'no_subject' && p.raw_detection_conf != null) {
    meta = 'best candidate ' + Math.round(Number(p.raw_detection_conf) * 100) + '%';
  }

  var ariaLabel = escapeAttr(
    (CATEGORY_LABELS[cat] || cat) + ' miss: ' + (p.filename || ('photo ' + p.id))
  );

  var selectedCls = state.selection.has(p.id) ? ' selected' : '';
  return (
    '<div class="miss-card' + selectedCls + '" ' +
      'data-cat="' + cat + '" ' +
      'data-idx="' + idx + '" ' +
      'data-photo-id="' + p.id + '" ' +
      'data-filename="' + escapeAttr(p.filename || '') + '" ' +
      'data-testid="miss-card-' + cat + '-' + p.id + '" ' +
      'aria-label="' + ariaLabel + '">' +
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
          (state.previewActive ? 'disabled' : '') + '>&times;</button>' +
      '</div>' +
      '<div class="miss-card-info">' +
        '<span class="miss-card-filename">' + escapeHtml(p.filename || '') + '</span>' +
        '<span class="miss-card-meta">' + escapeHtml(meta) + '</span>' +
      '</div>' +
    '</div>'
  );
}

/* ---------- Section collapse ---------- */
/** @param {MissCategory} cat @param {MouseEvent=} e */ export function toggleSection(cat, e) {
  if (e && e.target instanceof Element && e.target.closest('button')) return;
  var sec = document.getElementById('missesSection-' + cat);
  if (sec) sec.classList.toggle('collapsed');
}
