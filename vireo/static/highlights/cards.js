// Photo cards: thumbnails, card markup, and their click and order buttons.
// ES module.

import { escapeAttr, hasLightbox, openLightbox, sharedThumbnailUrl } from './shared.js';
import { isHighlightsAdvancedMode } from './loading.js';
import { formatPercent, formatScore } from './scope.js';
import { moveSpeciesHighlight, photoSpeciesName, removeSpeciesHighlight } from './species.js';
import { state } from './state.js';

/** @param {HighlightsPhoto | null | undefined} p */
export function photoThumbnailUrl(p) {
  if (!p || p.id == null) return '';
  var thumbnailUrl = sharedThumbnailUrl();
  return thumbnailUrl ? thumbnailUrl(p) : '/thumbnails/' + p.id + '.jpg';
}

/** @param {HighlightsPhoto} p @param {number} idx */
export function renderCard(p, idx) {
  var thumbSrc = photoThumbnailUrl(p);
  var advanced = isHighlightsAdvancedMode();
  var conf = p.has_accepted_species
    ? '<span class="card-chip conf">Tagged</span>'
    : (p.predicted_confidence != null ? '<span class="card-chip conf">ID ' + formatPercent(p.predicted_confidence) + '</span>' : '');
  var reasons = advanced
    ? (p.reasons || []).slice(0, 2).map(function(/** @type {string} */ r) {
        return '<span class="card-chip reason">' + escapeAttr(r) + '</span>';
      }).join('')
    : '';
  var scoreChip = advanced
    ? '<span class="card-chip score">' + formatScore(p.highlight_score) + '</span>'
    : '';
  var title = advanced ? (p.reasons || []).join(', ') : '';
  var ribbon = p.is_highlighted
    ? '<div class="best-ribbon">Highlight ' + escapeAttr(p.highlight_rank || '') + '</div>'
    : (p.is_species_representative
      ? '<div class="best-ribbon">Representative</div>'
      : (advanced && idx === 0 ? '<div class="best-ribbon">Best</div>' : ''));
  var flagBadge = p.flag === 'flagged'
    ? '<div class="pick-flag-badge" title="Flagged as pick">Pick</div>'
    : '';
  var rep = p.is_species_representative
    ? '<span class="card-chip reason">Representative</span>'
    : '';
  var order = p.is_highlighted
    ? '<div class="highlight-order">'
      + '<button type="button" data-highlight-order="up" data-photo-id="' + p.id + '" data-species="' + escapeAttr(photoSpeciesName(p)) + '">Up</button>'
      + '<button type="button" data-highlight-order="down" data-photo-id="' + p.id + '" data-species="' + escapeAttr(photoSpeciesName(p)) + '">Down</button>'
      + '<button type="button" data-highlight-remove="1" data-photo-id="' + p.id + '" data-species="' + escapeAttr(photoSpeciesName(p)) + '">Remove</button>'
      + '</div>'
    : '';
  return '<div class="highlights-card'
    + (p.is_highlighted ? ' highlight-card' : '')
    + (idx === 0 ? ' best-card' : '')
    + (p.flag === 'flagged' ? ' pick-flag-card' : '')
    + '" data-photo-id="' + p.id + '" title="' + escapeAttr(title) + '">'
    + ribbon
    + flagBadge
    + order
    + '<img src="' + thumbSrc + '" alt="' + escapeAttr(p.filename) + '" loading="lazy">'
    + '<div class="card-filename" title="' + escapeAttr(p.filename) + '">' + escapeAttr(p.filename) + '</div>'
    + '<div class="card-info">' + scoreChip + conf + rep + reasons + '</div>'
    + '</div>';
}

/** @param {ParentNode} container @param {HighlightsPhoto[]} photoSet */
export function attachCardClicks(container, photoSet) {
  // photoSet is the full list of photos in this bucket — used as the
  // lightbox navigation set so users can flip through every photo of
  // the species, not just the visible ones.
  container.querySelectorAll('.highlights-card').forEach(function(card) {
    card.addEventListener('click', function() {
      var pid = parseInt(card.getAttribute('data-photo-id') || '');
      var photo = photoSet.find(function(p) { return p.id === pid; });
      if (hasLightbox() && photo) {
        state.activeLightboxPhotoId = pid;
        openLightbox(pid, photo.filename, photoSet);
      }
    });
  });
}

/** @param {ParentNode} container */
export function attachHighlightOrderButtons(container) {
  /** @type {NodeListOf<HTMLButtonElement>} */ (container.querySelectorAll('[data-highlight-order], [data-highlight-remove]')).forEach(function(btn) {
    btn.addEventListener('click', function(event) {
      event.stopPropagation();
      var species = btn.getAttribute('data-species') || '';
      var photoId = parseInt(btn.getAttribute('data-photo-id') || '');
      if (btn.hasAttribute('data-highlight-remove')) {
        removeSpeciesHighlight(species, photoId, btn);
      } else {
        moveSpeciesHighlight(species, photoId, btn.getAttribute('data-highlight-order') || '', btn);
      }
    });
  });
}
