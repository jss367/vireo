/* Browse: toolbar view controls (filters, sort, stacks, thumb size) and result position.
   Classic script sharing page globals; browse.html loads the
   browse/*.js files in dependency order. */

function hasActiveBrowseFilter() {
  return !!(
    activeFolderId ||
    activeKeyword ||
    activeCollectionId ||
    openedCollectionId ||
    (window.VireoFilter && VireoFilter.hasFilters())
  );
}

function applyFilters() {
  if (timelineMode) loadCalendarData();
  if (!dashboardCollectionScope) activeCollectionId = null;
  reloadBrowseResults();
}

/* Re-sorting is a re-ordering, not a membership change: the photo the user
   selected is still in the result set, just somewhere else in it. Dropping
   them at the top of the grid with nothing selected made "let me see these
   same photos by rating instead" cost them their place every time. Keep the
   selection and ask the server which page the photo landed on
   (``focusAnchor``), which is the only thing that scales — the old position
   is no guide to the new one, so there is nothing sensible to page towards.

   Everything else ``applyFilters`` does still applies: the calendar heatmap
   and collection scope follow the same rules for a sort as for any other
   non-collection filter change. */
function onSortChanged() {
  if (timelineMode) loadCalendarData();
  if (!dashboardCollectionScope) activeCollectionId = null;
  // A batch selection that is not a single stack has no one card to keep the
  // user with, but they still have a place in the grid: hold it without
  // turning it into a selection. With nothing selected at all there is
  // nothing to hold onto and a re-sort deliberately starts at the top —
  // position N in the old order says nothing about the new one.
  reloadBrowseResults({
    preserveAnchor: true,
    focusAnchor: true,
    preserveViewport: selectedPhotos.size > 0,
  });
}

function toggleBrowseStacks() {
  resetAndLoad({preserveCollection: true});
}

function updateFilterSummary() {
  updateScrollPosition();
}

function updateThumbSize(val) {
  document.getElementById('grid').style.setProperty('--thumb-size', val + 'px');
  updateGridTail();
}

function toggleDetectionBoxes() {
  showDetectionBoxes = !showDetectionBoxes;
  var btn = document.getElementById('detBoxToggle');
  btn.style.color = showDetectionBoxes ? 'var(--accent)' : 'var(--text-muted)';
  btn.style.borderColor = showDetectionBoxes ? 'var(--accent)' : 'var(--border-secondary)';
  renderGrid();
}

function appendVisualScopeParams(params) {
  // Summary/calendar/typeahead counts must describe the same photos the
  // visually-filtered grid shows; the backend narrows to the matched set
  // when the clause is healthy and leaves rules untouched otherwise.
  var visual = window.VireoFilter && VireoFilter.getVisual ? VireoFilter.getVisual() : null;
  if (visual) params.set('visual', JSON.stringify(visual));
}

function reloadBrowseResults(options) {
  resetAndLoad(options);
}

function updateScrollPosition() {
  var el = document.getElementById('filterSummary');
  var positionOffset = loadedWindowOffset();
  var stackedTotals = totalPhotos.toLocaleString() + ' items · '
    + totalBrowseStacks.toLocaleString() + (totalBrowseStacks === 1 ? ' stack · ' : ' stacks · ')
    + totalUnderlyingPhotos.toLocaleString() + ' photos';
  var resultLabel = function(position) {
    if (browseStacksEnabled()) {
      return position + ' of ' + stackedTotals;
    }
    return position + ' of ' + totalPhotos.toLocaleString();
  };
  if (photos.length === 0) {
    el.textContent = browseStacksEnabled()
      ? stackedTotals
      : totalPhotos.toLocaleString() + ' photos';
    return;
  }
  // Find which cards are visible in the viewport
  var cards = document.querySelectorAll('.grid-card');
  if (cards.length === 0) {
    el.textContent = browseStacksEnabled()
      ? stackedTotals
      : totalPhotos.toLocaleString() + ' photos';
    return;
  }
  var container = document.getElementById('gridContainer');
  var scrollTop = container.scrollTop;
  var viewHeight = container.clientHeight;

  // Viewport entirely below every loaded card (placeholder territory):
  // reporting the loaded range would be a lie, so estimate within the
  // bounded runway represented by the rendered skeleton cells.
  var lastCard = cards[cards.length - 1];
  if (lastCard.offsetTop + lastCard.offsetHeight < scrollTop) {
    var frac = Math.min(1, (scrollTop + viewHeight / 2) / Math.max(1, container.scrollHeight));
    var skeletonCount = document.querySelectorAll('#gridTail .skel-card').length;
    var representedPhotos = Math.min(
      totalPhotos - positionOffset,
      photos.length + skeletonCount
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
    if (cards[i].offsetTop + cards[i].offsetHeight > scrollTop) {
      first = i + 1;
      break;
    }
  }
  for (i = cards.length - 1; i >= 0; i--) {
    if (cards[i].offsetTop < scrollTop + viewHeight) {
      last = i + 1;
      break;
    }
  }
  el.textContent = resultLabel(
    (positionOffset + first) + '–' + (positionOffset + last)
  );
}
