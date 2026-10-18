/* Browse: toolbar view controls (filters, sort, stacks, thumb size) and result position. ES module (see
   browse/README.md). */
import { state } from './state.js';
import { VireoFilter } from './shared.js';
import { loadCalendarData } from './calendar.js';
import { renderGrid, updateGridTail } from './grid.js';
import { loadedWindowOffset, resetAndLoad } from './loading.js';
import { browseStacksEnabled } from './stacks.js';

export function hasActiveBrowseFilter() {
  return !!(
    state.activeFolderId ||
    state.activeKeyword ||
    state.activeCollectionId ||
    state.openedCollectionId ||
    (VireoFilter && VireoFilter.hasFilters())
  );
}

/* Re-sorting is a re-ordering, not a membership change: the photo the user
   selected is still in the result set, just somewhere else in it. Dropping
   them at the top of the grid with nothing selected made "let me see these
   same photos by rating instead" cost them their place every time. Keep the
   selection and ask the server which page the photo landed on
   (``focusAnchor``), which is the only thing that scales — the old position
   is no guide to the new one, so there is nothing sensible to page towards.

   Everything else a filter change does still applies: the calendar heatmap
   and collection scope follow the same rules for a sort as for any other
   non-collection filter change (onBrowseFilterChange in loading.js). */
export function onSortChanged() {
  if (state.timelineMode) loadCalendarData();
  if (!state.dashboardCollectionScope) state.activeCollectionId = null;
  // A batch selection that is not a single stack has no one card to keep the
  // user with, but they still have a place in the grid: hold it without
  // turning it into a selection. With nothing selected at all there is
  // nothing to hold onto and a re-sort deliberately starts at the top —
  // position N in the old order says nothing about the new one.
  reloadBrowseResults({
    preserveAnchor: true,
    focusAnchor: true,
    preserveViewport: state.selectedPhotos.size > 0,
  });
}

export function toggleBrowseStacks() {
  resetAndLoad({preserveCollection: true});
}

export function updateFilterSummary() {
  updateScrollPosition();
}

export function updateThumbSize(/** @type {string | number} */ val) {
  /** @type {HTMLElement} */ (document.getElementById('grid')).style.setProperty('--thumb-size', val + 'px');
  updateGridTail();
}

export function toggleDetectionBoxes() {
  state.showDetectionBoxes = !state.showDetectionBoxes;
  var btn = /** @type {HTMLButtonElement} */ (document.getElementById('detBoxToggle'));
  btn.style.color = state.showDetectionBoxes ? 'var(--accent)' : 'var(--text-muted)';
  btn.style.borderColor = state.showDetectionBoxes ? 'var(--accent)' : 'var(--border-secondary)';
  renderGrid();
}

export function appendVisualScopeParams(/** @type {URLSearchParams} */ params) {
  // Summary/calendar/typeahead counts must describe the same photos the
  // visually-filtered grid shows; the backend narrows to the matched set
  // when the clause is healthy and leaves rules untouched otherwise.
  var visual = VireoFilter && VireoFilter.getVisual ? VireoFilter.getVisual() : null;
  if (visual) params.set('visual', JSON.stringify(visual));
}

export function reloadBrowseResults(/** @type {BrowseReloadOptions | undefined} */ options = undefined) {
  resetAndLoad(options);
}

export function updateScrollPosition() {
  var el = /** @type {HTMLElement} */ (document.getElementById('filterSummary'));
  var positionOffset = loadedWindowOffset();
  var stackedTotals = state.totalPhotos.toLocaleString() + ' items · '
    + state.totalBrowseStacks.toLocaleString() + (state.totalBrowseStacks === 1 ? ' stack · ' : ' stacks · ')
    + state.totalUnderlyingPhotos.toLocaleString() + ' photos';
  var resultLabel = function(/** @type {number | string} */ position) {
    if (browseStacksEnabled()) {
      return position + ' of ' + stackedTotals;
    }
    return position + ' of ' + state.totalPhotos.toLocaleString();
  };
  if (state.photos.length === 0) {
    el.textContent = browseStacksEnabled()
      ? stackedTotals
      : state.totalPhotos.toLocaleString() + ' photos';
    return;
  }
  // Find which cards are visible in the viewport
  var cards = /** @type {NodeListOf<HTMLElement>} */ (document.querySelectorAll('.grid-card'));
  if (cards.length === 0) {
    el.textContent = browseStacksEnabled()
      ? stackedTotals
      : state.totalPhotos.toLocaleString() + ' photos';
    return;
  }
  var container = /** @type {HTMLElement} */ (document.getElementById('gridContainer'));
  var scrollTop = container.scrollTop;
  var viewHeight = container.clientHeight;

  // Viewport entirely below every loaded card (placeholder territory):
  // reporting the loaded range would be a lie, so estimate within the
  // bounded runway represented by the rendered skeleton cells.
  var lastCard = cards[cards.length - 1]; if (!lastCard) return;
  if (lastCard.offsetTop + lastCard.offsetHeight < scrollTop) {
    var frac = Math.min(1, (scrollTop + viewHeight / 2) / Math.max(1, container.scrollHeight));
    var skeletonCount = /** @type {NodeListOf<HTMLElement>} */ (document.querySelectorAll('#gridTail .skel-card')).length;
    var representedPhotos = Math.min(
      state.totalPhotos - positionOffset,
      state.photos.length + skeletonCount
    );
    var approx = positionOffset + Math.max(
      1,
      Math.min(representedPhotos, Math.round(frac * representedPhotos))
    );
    el.textContent = resultLabel('≈' + approx.toLocaleString());
    return;
  }

  var first = 1;
  var last = cards.length;
  for (var i = 0; i < cards.length; i++) {
    var card = cards[i]; if (!card) continue;
    if (card.offsetTop + card.offsetHeight > scrollTop) {
      first = i + 1;
      break;
    }
  }
  for (i = cards.length - 1; i >= 0; i--) {
    var endCard = cards[i]; if (!endCard) continue;
    if (endCard.offsetTop < scrollTop + viewHeight) {
      last = i + 1;
      break;
    }
  }
  el.textContent = resultLabel(
    (positionOffset + first) + '–' + (positionOffset + last)
  );
}
