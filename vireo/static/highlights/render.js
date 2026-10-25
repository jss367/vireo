// Rendering: a bucket's strip of cards, each species row, the unidentified
// row, loading more of a bucket, and the whole page.
// ES module.

import { escapeAttr, safeFetch } from './shared.js';
import { attachCardClicks, attachHighlightOrderButtons, renderCard } from './cards.js';
import { updateHighlightsLightboxControls } from './lightbox.js';
import { isHighlightsAdvancedMode, sortedBuckets } from './loading.js';
import { appendHighlightFilterParams, formatPercent, formatScore, requestScopeParams } from './scope.js';
import { bucketPhotoIds, confirmPhotoIds, showSpeciesModal, unidPhotoIds } from './species.js';
import { state } from './state.js';

// Render a strip's cards, inserting the "Not yet analyzed" divider before the
// trailing contiguous run of unscored non-pick photos. Walking backward from
// the end anchors the divider to the actual tail boundary even after curated
// ordering (species highlights, representatives) has promoted an unscored,
// non-flagged photo to the front — a first-encountered check would have
// misfired on that promoted card and left analyzed photos below the label.
// If the visible slice ends on an analyzed photo (or on a flagged photo),
// there is no tail and no divider fires. Unscored picks (flagged, at the
// front) are always excluded from the tail.
/** @param {HighlightsPhoto[]} photos */
export function renderStripCards(photos) {
  var tailStart = photos.length;
  for (var j = photos.length - 1; j >= 0; j--) {
    var q = /** @type {HighlightsPhoto} */ (photos[j]);
    if (!q.is_analyzed && q.flag !== 'flagged') {
      tailStart = j;
    } else {
      break;
    }
  }
  var tailCount = photos.length - tailStart;
  var html = '';
  for (var i = 0; i < photos.length; i++) {
    if (i === tailStart && tailCount) {
      html += '<div class="analyzed-divider">Not yet analyzed — ' + tailCount
        + ' photo' + (tailCount === 1 ? '' : 's') + '</div>';
    }
    html += renderCard(/** @type {HighlightsPhoto} */ (photos[i]), i);
  }
  return html;
}

/** @param {HighlightsBucket} bucket */
export function renderBucket(bucket) {
  var perRow = parseInt(/** @type {HTMLInputElement} */ (document.getElementById('perRowSlider')).value);
  var expanded = state.expandedBuckets.has(bucket.species);
  var visible = expanded ? bucket.photos : bucket.photos.slice(0, perRow);
  var remaining = bucket.photo_count - visible.length;
  var badgeClass = bucket.is_accepted ? 'accepted' : (bucket.certainty === 'likely' ? 'likely' : 'candidate');
  var badgeText = bucket.is_accepted ? 'Keyword confirmed' : (bucket.certainty === 'likely' ? 'Likely ID' : 'Top candidates');
  var badge = bucket.is_accepted
    ? '<span class="bucket-badge accepted">Keyword confirmed</span>'
    : '<span class="bucket-badge ' + badgeClass + '">' + badgeText + '</span>';
  var meta = bucket.photo_count + ' photo' + (bucket.photo_count === 1 ? '' : 's');
  if (bucket.highlight_count) meta += ' · ' + bucket.highlight_count + ' highlighted';
  if (isHighlightsAdvancedMode()) meta += ' · best ' + formatScore(bucket.best_score);
  if (!bucket.is_accepted && bucket.avg_confidence != null) meta += ' · ID ' + formatPercent(bucket.avg_confidence);
  var expandBtn = '';
  // Show the affordance whenever photos are hidden (more loaded than one row)
  // or unloaded (has_more). Gating on photo_count > perRow alone would strand
  // backend rows after a rejection drops photo_count to perRow while has_more
  // is still true — leaving no way to load the rest without a refresh.
  if (bucket.photos.length > perRow || bucket.has_more) {
    expandBtn = '<button class="bucket-expand" data-species="'
      + escapeAttr(bucket.species) + '">'
      + (expanded ? (bucket.has_more ? 'Load more' : 'Show fewer') : '+ ' + remaining + ' more')
      + '</button>';
  }
  var actions = '<div class="bucket-actions">';
  if (!bucket.is_accepted) {
    actions += '<button class="bucket-action primary" data-action="confirm-bucket" data-species="'
      + escapeAttr(bucket.species) + '">Confirm keyword</button>';
  }
  actions += '<button class="bucket-action" data-action="change-bucket" data-species="'
    + escapeAttr(bucket.species) + '">Change</button></div>';
  return '<section class="bucket">'
    + '<div class="bucket-header">'
    + '<div class="bucket-heading"><div class="bucket-title">' + escapeAttr(bucket.species) + badge + '</div>'
    + '<div class="bucket-meta">' + meta + '</div></div>'
    + actions
    + '</div>'
    + '<div class="bucket-strip">' + renderStripCards(visible) + '</div>'
    + expandBtn
    + '</section>';
}

export function renderUnidentified() {
  var unid = /** @type {HighlightsData} */ (state.currentData).unidentified;
  if (!unid || !unid.photo_count) return '';
  var perRow = parseInt(/** @type {HTMLInputElement} */ (document.getElementById('perRowSlider')).value);
  var visible = state.unidentifiedExpanded ? unid.photos : unid.photos.slice(0, perRow);
  var remaining = unid.photo_count - visible.length;
  var expandBtn = '';
  // Same as renderBucket: keep the load/expand affordance while photos are
  // hidden or unloaded, so a rejection that drops photo_count to perRow with
  // has_more still true doesn't strand the remaining backend rows.
  if (unid.photos.length > perRow || unid.has_more) {
    expandBtn = '<button class="bucket-expand" data-unid="1">'
      + (state.unidentifiedExpanded ? (unid.has_more ? 'Load more' : 'Show fewer') : '+ ' + remaining + ' more')
      + '</button>';
  }
  return '<div class="unid-divider">Unidentified — Vireo couldn\'t ID these</div>'
    + '<section class="bucket">'
    + '<div class="bucket-header">'
    + '<div class="bucket-heading"><div class="bucket-title">Unidentified</div>'
    + '<div class="bucket-meta">' + unid.photo_count + ' photos</div></div>'
    + '<div class="bucket-actions"><button class="bucket-action" data-action="change-unidentified">Set species</button></div>'
    + '</div>'
    + '<div class="bucket-strip">' + renderStripCards(visible) + '</div>'
    + expandBtn
    + '</section>';
}

/** @param {string | null} species @param {boolean} isUnidentified */
export async function loadMoreBucket(species, isUnidentified) {
  var params = requestScopeParams();
  var data = /** @type {HighlightsData} */ (state.currentData);
  var target = isUnidentified ? data.unidentified : (data.buckets || []).find(function(b) { return b.species === species; });
  if (!target || !target.has_more) return;
  var speciesKey = isUnidentified ? '__unidentified__' : String(species);
  params.set('species', speciesKey);
  appendHighlightFilterParams(params);
  params.set('species', speciesKey);
  params.set('offset', String(target.photos.length));
  params.set('limit', '500');
  /** @type {HighlightsBucketPage} */
  var page = await safeFetch('/api/highlights/bucket?' + params);
  target.photos = target.photos.concat(page.photos || []);
  target.loaded_count = page.loaded_count;
  target.has_more = page.has_more;
}

/** "N species with … · X of Y photos scored", naming the active filters. @param {number} bucketCount */
export function highlightsMetaText(bucketCount) {
  var search = /** @type {HTMLInputElement} */ (document.getElementById('highlightSearch')).value.trim();
  var speciesSearch = /** @type {HTMLInputElement} */ (document.getElementById('speciesSearch')).value.trim();
  var confirmation = /** @type {HTMLSelectElement} */ (document.getElementById('confirmationFilter')).value;
  var highlightSelection = /** @type {HTMLSelectElement} */ (document.getElementById('highlightSelectionFilter')).value;
  var representative = /** @type {HTMLSelectElement} */ (document.getElementById('representativeFilter')).value;
  var filterLabel = (search || speciesSearch) ? ' matching species' : ' species';
  if (confirmation === 'confirmed') filterLabel += ' with confirmed keywords';
  if (confirmation === 'unconfirmed') filterLabel += ' without confirmed keywords';
  if (highlightSelection === 'yes') filterLabel += ' with highlights chosen';
  if (highlightSelection === 'no') filterLabel += ' without highlights chosen';
  if (representative === 'yes') filterLabel += ' with representatives set';
  if (representative === 'no') filterLabel += ' without representatives set';
  var data = /** @type {HighlightsData} */ (state.currentData);
  return bucketCount + filterLabel + ' · '
    + data.meta.eligible + ' of '
    + data.meta.total_in_scope + ' photos scored';
}

/** Each row's Show more / Show less button. @param {HTMLElement} content */
export function bindBucketExpandButtons(content) {
  content.querySelectorAll('.bucket-expand').forEach(function(btn) {
    btn.addEventListener('click', async function() {
      var data = /** @type {HighlightsData} */ (state.currentData);
      if (btn.hasAttribute('data-unid')) {
        if (!state.unidentifiedExpanded || (data.unidentified && data.unidentified.has_more)) {
          state.unidentifiedExpanded = true;
          await loadMoreBucket(null, true);
        } else {
          state.unidentifiedExpanded = false;
        }
      } else {
        var sp = btn.getAttribute('data-species') || '';
        var bucket = (data.buckets || []).find(function(b) { return b.species === sp; });
        if (!state.expandedBuckets.has(sp) || (bucket && bucket.has_more)) {
          state.expandedBuckets.add(sp);
          await loadMoreBucket(sp, false);
        } else {
          state.expandedBuckets.delete(sp);
        }
      }
      render();
    });
  });
}

/** Each row's Confirm / Change species buttons. @param {HTMLElement} content @param {HighlightsBucket[]} buckets */
export function bindBucketActionButtons(content, buckets) {
  /** @type {NodeListOf<HTMLButtonElement>} */ (content.querySelectorAll('.bucket-action')).forEach(function(btn) {
    btn.addEventListener('click', async function() {
      var action = btn.getAttribute('data-action');
      btn.disabled = true;
      try {
        if (action === 'confirm-bucket') {
          var species = btn.getAttribute('data-species');
          var bucket = buckets.find(function(b) { return b.species === species; });
          if (bucket) await confirmPhotoIds(await bucketPhotoIds(bucket), bucket.species, btn);
        } else if (action === 'change-bucket') {
          var changeSpecies = btn.getAttribute('data-species');
          var changeBucket = buckets.find(function(b) { return b.species === changeSpecies; });
          if (changeBucket) {
            var changeIds = await bucketPhotoIds(changeBucket);
            showSpeciesModal(
              'Change species',
              changeIds.length + ' photo' + (changeIds.length === 1 ? '' : 's'),
              changeIds,
              changeBucket.species,
              changeBucket.species
            );
          }
        } else if (action === 'change-unidentified') {
          var ids = await unidPhotoIds();
          showSpeciesModal(
            'Set species',
            ids.length + ' unidentified photo' + (ids.length === 1 ? '' : 's'),
            ids,
            '',
            null
          );
        }
      } finally {
        btn.disabled = false;
      }
    });
  });
}

export function render() {
  var content = /** @type {HTMLElement} */ (document.getElementById('content'));
  var empty = /** @type {HTMLElement} */ (document.getElementById('emptyState'));
  var meta = /** @type {HTMLElement} */ (document.getElementById('meta'));
  var controls = /** @type {HTMLElement} */ (document.getElementById('controlsBar'));
  var data = /** @type {HighlightsData} */ (state.currentData);

  // Only bail to the empty state when NOTHING is renderable. Gating on
  // folders alone would hide unscored-only workspaces (or subtrees): the
  // folder dropdown is sourced from get_folders_with_quality_data, which is
  // scored-only, but the highlights payload can still return eligible
  // buckets and unidentified photos from unscored candidates. The workspace
  // scope option is prepended unconditionally, so an empty folder list
  // remains navigable via that entry.
  var hasBuckets = (data.buckets || []).length > 0;
  var hasUnidentified = !!(data.unidentified
    && data.unidentified.photo_count);
  if (!data.folders.length && !hasBuckets && !hasUnidentified) {
    content.innerHTML = '';
    empty.style.display = 'block';
    meta.textContent = '';
    controls.style.display = 'none';
    return;
  }
  empty.style.display = 'none';
  controls.style.display = '';

  var buckets = sortedBuckets();
  meta.textContent = highlightsMetaText(buckets.length);

  var html = buckets.map(renderBucket).join('') + renderUnidentified();
  content.innerHTML = html || '<div class="highlights-empty"><p>Nothing matches these filters.</p></div>';

  // Wire up clicks for cards and expand buttons.
  // Attach card clicks: pick the right photo set per section by walking sections in DOM order.
  var sections = content.querySelectorAll('section.bucket');
  sections.forEach(function(section, idx) {
    var photoSet;
    if (idx < buckets.length) {
      photoSet = /** @type {HighlightsBucket} */ (buckets[idx]).photos;
    } else {
      photoSet = /** @type {HighlightsBucket} */ (data.unidentified).photos;
    }
    attachCardClicks(/** @type {HTMLElement} */ (section), photoSet);
  });
  attachHighlightOrderButtons(content);
  bindBucketExpandButtons(content);
  bindBucketActionButtons(content, buckets);
  updateHighlightsLightboxControls(state.activeLightboxPhotoId);
}
