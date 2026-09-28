/* Browse: infinite scroll and keeping the focused card visible.
   Classic script sharing page globals; browse.html loads the
   browse/*.js files in dependency order. */

/* ---------- Infinite Scroll ---------- */
var gridContainer = document.getElementById('gridContainer');
var infiniteScrollObserverDisconnected = false;
var infiniteScrollObserverIsNative = (
  typeof window.IntersectionObserver === 'function' &&
  String(window.IntersectionObserver).indexOf('[native code]') !== -1
);
var observer = new IntersectionObserver(function(entries) {
  if (entries[0].isIntersecting) loadPhotos({ silent: photos.length > 0 });
}, { root: gridContainer, rootMargin: '3200px 0px' });

var _observerObserve = observer.observe.bind(observer);
var _observerDisconnect = observer.disconnect.bind(observer);
observer.observe = function(target) {
  infiniteScrollObserverDisconnected = false;
  return _observerObserve(target);
};
observer.disconnect = function() {
  infiniteScrollObserverDisconnected = true;
  return _observerDisconnect();
};

function rearmInfiniteScrollObserver() {
  // With the 3200px root margin, the sentinel can remain intersecting after
  // bootstrap or page loads. Re-observing asks IntersectionObserver to deliver
  // a fresh callback for the current layout instead of waiting for a scroll
  // transition that may never occur.
  if (infiniteScrollObserverDisconnected) return;
  if (allLoaded || typeof observer === 'undefined') return;
  var sentinel = document.getElementById('scrollSentinel');
  if (!sentinel) return;
  observer.unobserve(sentinel);
  observer.observe(sentinel);
}

/* Scroll-driven page loading. The primary trigger is proximity to the loading
   boundary (the first skeleton cell). Fires on scroll and chains after each
   successful page load until the viewport is covered by real cards. */
function ensureViewportHydrated() {
  if (infiniteScrollObserverDisconnected || !infiniteScrollObserverIsNative) return;
  if (loading || allLoaded || anchorScanDepth > 0) return;
  if (photos.length === 0) return;
  var tail = document.getElementById('gridTail');
  if (!tail || !tail.firstElementChild) return;
  var contRect = document.getElementById('gridContainer').getBoundingClientRect();
  var skelRect = tail.firstElementChild.getBoundingClientRect();
  if (skelRect.top - contRect.bottom <= 3200) loadPhotos({ silent: true });
}

/* A photo deep link begins with the page containing the target. When the user
   travels back toward the top edge of that window, fetch its preceding page
   just as the lower edge fetches the following page. The grid's top is used
   instead of the sticky explanation banner, whose sticky geometry remains in
   the viewport even when the actual loading boundary is far above it. */
function ensurePreviousViewportHydrated() {
  if (loading || earliestPage <= 1 || anchorScanDepth > 0) return;
  if (photos.length === 0) return;
  var grid = document.getElementById('grid');
  var contRect = gridContainer.getBoundingClientRect();
  var gridRect = grid.getBoundingClientRect();
  if (gridRect.top >= contRect.top - 800) {
    prependPreviousPhotos({ silent: true });
  }
}

window.addEventListener('resize', function() {
  var sidebar = document.getElementById('browseSidebar');
  if (sidebar) setBrowseSidebarWidth(sidebar.getBoundingClientRect().width, false);
  var detailPanel = document.getElementById('detailPanel');
  if (detailPanel) setBrowseDetailPanelWidth(detailPanel.getBoundingClientRect().width, false);
  // Column count and card heights change with width — re-estimate the tail
  requestAnimationFrame(updateGridTail);
});

/* ---------- Keep the clicked photo visible across layout changes ---------- */
// The grid is `auto-fill`, so any width change reflows every card into a
// different column: after a window resize (or a sidebar/detail-panel drag) the
// photo the user clicked can end up far outside the viewport. A shorter
// viewport pushes it past the bottom edge the same way. Put it back in view —
// but only when it was on screen to begin with, so a resize never hauls the
// grid back to a selection the user has deliberately scrolled away from.
function focusedBrowsePhotoId() {
  // The most recent click wins. A cmd/shift-click extends the selection while
  // leaving selectedPhotoId on the older detail focus, and the card the user
  // just clicked is the one they are looking at. It only wins while it has a
  // card on screen, though: collapsing a stack after a stack-wide Select all
  // leaves the last-clicked member selected but unrendered, with
  // selectedPhotoId deliberately pinned to the visible cover.
  if (lastClickedPhotoId != null &&
      (lastClickedPhotoId === selectedPhotoId || selectedPhotos.has(lastClickedPhotoId)) &&
      renderedCardForFocus(lastClickedPhotoId)) {
    return lastClickedPhotoId;
  }
  return selectedPhotoId;
}

// The card standing in for a focused photo. A focused photo that is not itself
// rendered can still have a visible home: a collapsed stack draws its cover in
// place of every member, and collapsing while two members are selected leaves
// the focus on a hidden one by design (the member stays an active batch
// target). Grid and lightbox navigation already resolve that focus to the
// cover — getBrowseShortcutPhoto — so the resize correction does too, instead
// of giving up and letting the visibly-selected cover reflow off screen.
function renderedCardForFocus(photoId) {
  if (photoId == null) return null;
  var card = getBrowsePhotoElement(photoId);
  if (card) return card;
  var coverId = browseStackCoverIdForPhoto(photoId);
  return coverId == null ? null : getBrowsePhotoElement(coverId);
}

// The focused card's box, measured from the top of the grid viewport.
function focusedCardBox() {
  var card = renderedCardForFocus(focusedBrowsePhotoId());
  if (!card) return null;
  var containerRect = gridContainer.getBoundingClientRect();
  var cardRect = card.getBoundingClientRect();
  return {
    top: cardRect.top - containerRect.top,
    bottom: cardRect.bottom - containerRect.top,
    height: cardRect.height,
  };
}

var gridViewportSize = gridContainer.clientWidth + 'x' + gridContainer.clientHeight;
var gridSyncBannerHeight = document.getElementById('syncBanner').offsetHeight;
var focusedCardWasOnScreen = false;

// Whether the focused card was on screen *before* the reflow. A ResizeObserver
// only ever sees the layout that already happened, so this has to be sampled
// as the user scrolls and selects.
function noteFocusedCardVisibility() {
  // A reflow can dispatch its scroll event before the observer has compensated
  // for it. Sampling then would record the post-reflow (usually off-screen)
  // position and suppress the very correction it exists to authorize.
  if (gridContainer.clientWidth + 'x' + gridContainer.clientHeight !== gridViewportSize) return;
  var box = focusedCardBox();
  focusedCardWasOnScreen = !!box && box.bottom > 0 && box.top < gridContainer.clientHeight;
}

function keepFocusedCardVisible() {
  // Scrolled out of view on purpose before the resize: the user is looking
  // somewhere else, and the reflow is not what moved the card away.
  if (!focusedCardWasOnScreen) return;
  var box = focusedCardBox();
  if (!box) return;
  var viewHeight = gridContainer.clientHeight;
  if (box.top >= 0 && box.bottom <= viewHeight) return;
  // Off-screen after the reflow: land it in the middle of the viewport rather
  // than flush against whichever edge it drifted past, so the rows around it
  // stay readable. A card taller than the viewport pins its top edge instead.
  var wantedTop = box.height >= viewHeight ? 0 : (viewHeight - box.height) / 2;
  gridContainer.scrollTop += box.top - wantedTop;
}

if (window.ResizeObserver) {
  // ResizeObserver runs after layout but before paint, so the correction lands
  // in the same frame as the reflow — the card never visibly jumps away.
  new ResizeObserver(function() {
    var size = gridContainer.clientWidth + 'x' + gridContainer.clientHeight;
    var bannerHeight = document.getElementById('syncBanner').offsetHeight;
    var previousSize = gridViewportSize.split('x').map(Number);
    // Saving the first edit reveals the sync banner. That reduces the
    // viewport height, but must not recenter a partly visible selected photo
    // while its filter membership is being refreshed. Actual window/sidebar
    // resizes still keep the focused photo visible as before.
    var onlySyncBannerChanged = bannerHeight !== gridSyncBannerHeight
      && gridContainer.clientWidth === previousSize[0]
      && Math.abs(gridContainer.clientHeight + bannerHeight
        - previousSize[1] - gridSyncBannerHeight) <= 1;
    gridSyncBannerHeight = bannerHeight;
    // A scrollbar appearing counts (it reflows the columns); anything that
    // leaves the viewport box alone cannot have moved the card out of view.
    if (size === gridViewportSize) return;
    gridViewportSize = size;
    if (!onlySyncBannerChanged) keepFocusedCardVisible();
    noteFocusedCardVisibility();
  }).observe(gridContainer);

  // The grid itself can reflow while the viewport keeps its size: the
  // thumbnail-size slider changes the column count, a stack tray opens, a page
  // of photos is appended. None of those fire a scroll event, so the sampled
  // visibility would go stale — false after a reflow carried the card back
  // into view, true after one carried it away — and the next resize would act
  // on that stale answer. Re-sample instead; there is nothing to correct,
  // because the viewport did not move. The size guard inside
  // noteFocusedCardVisibility keeps this out of the way during a viewport
  // resize, which the observer above owns.
  new ResizeObserver(function() {
    noteFocusedCardVisibility();
  }).observe(document.getElementById('grid'));
}

/* ---------- Scroll Position Tracking ---------- */
var scrollTimer = null;
var lastGridScrollTop = gridContainer.scrollTop;
gridContainer.addEventListener('scroll', function() {
  var nextScrollTop = gridContainer.scrollTop;
  if (nextScrollTop < lastGridScrollTop) ensurePreviousViewportHydrated();
  lastGridScrollTop = nextScrollTop;
  noteFocusedCardVisibility();
  ensureViewportHydrated();
  if (scrollTimer) clearTimeout(scrollTimer);
  scrollTimer = setTimeout(updateScrollPosition, 50);
});

// At scrollTop=0 an upward wheel gesture cannot produce a scroll event. It is
// still an unambiguous request to continue into the earlier photos.
gridContainer.addEventListener('wheel', function(e) {
  if (e.deltaY < 0) requestAnimationFrame(ensurePreviousViewportHydrated);
}, { passive: true });
