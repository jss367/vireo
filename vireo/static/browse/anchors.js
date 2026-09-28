/* Browse: keeping the selected photo / viewport anchored across reloads.
   Classic script sharing page globals; browse.html loads the
   browse/*.js files in dependency order. */

/* The cover of the stack the current selection *is*, or null.

   Clicking a collapsed stack card selects every frame it stands for
   (selectPhoto's ``clickIds.length > 1`` branch) and leaves no focused
   photo, so by the letter of the guard below a selected stack was
   indistinguishable from a fifteen-photo batch — and a re-sort dropped the
   user at the top of the grid instead of keeping them with the stack they
   had picked. It is not a batch: it is one card, in one position, and it is
   the card the user is looking at. Resolve it back to that card so it can
   anchor a reload like any other selected card. */
function browseSelectedStackCoverId() {
  if (selectedPhotos.size < 2) return null;
  var isTheSelection = function(photo) {
    if (!photo || !photo.browse_stack) return false;
    // A focused photo alongside the set usually means a batch the user built
    // by hand around one card. One case is still a whole stack: collapsing a
    // tray that "Select all" filled pins the focus to the visible cover
    // (``batchHasHiddenMembers`` in toggleBrowseStack) because a collapsed
    // tray cannot hold a focus on a hidden frame. Accept a focus that *is*
    // this cover, reject any other (Codex P2 on PR #1695).
    if (selectedPhotoId != null && photo.id !== selectedPhotoId) return false;
    var ids = browseStackMemberIdsFor(photo);
    return !!ids && ids.length === selectedPhotos.size
      && ids.every(function(id) { return selectedPhotos.has(id); });
  };
  // Both ways of selecting a whole stack — clicking the collapsed card and
  // the tray's "Select all" — leave ``selectedIndex`` on the cover's grid
  // slot, so this hits on the first try in practice. The scan is the
  // fallback for a selection that outlived its index.
  if (selectedIndex >= 0 && isTheSelection(photos[selectedIndex])) {
    return photos[selectedIndex].id;
  }
  var cover = photos.find(isTheSelection);
  return cover ? cover.id : null;
}

function captureSelectedPhotoAnchor() {
  var stackCoverId = browseSelectedStackCoverId();
  if (stackCoverId == null && (selectedPhotoId == null || selectedPhotos.size > 0)) {
    return null;
  }
  var anchorId = stackCoverId == null ? selectedPhotoId : stackCoverId;
  var card = getBrowsePhotoElement(anchorId);
  if (!card) return null;
  var container = document.getElementById('gridContainer');
  var containerRect = container.getBoundingClientRect();
  var cardRect = card.getBoundingClientRect();
  // Position in ``photos`` at capture time. Bounds the paged rescan below
  // when a ``preserveScroll`` reload runs after an edit that may have
  // removed this photo from the filter (Codex review r4013123608): a
  // 60k-photo library would otherwise walk to ``allLoaded`` — ~1,200
  // page requests — before giving up.
  //
  // An expanded stack member is not itself in ``photos`` — only its cover
  // is — so ``findIndex`` returns -1 and the scan budget collapses to a
  // single page even though the cover may sit tens of thousands of rows
  // in. Anchor the position via the cover so the budget matches where the
  // member actually lives; ``loadUntilPhotoRendered`` with
  // ``resolveStackMember: true`` will re-expand the tray on the far side
  // (Codex review r4013378153).
  var index = photos.findIndex(function(p) { return p.id === anchorId; });
  if (index < 0) {
    var memberCover = loadedBrowseStackCoverForPhoto(anchorId);
    if (memberCover) {
      index = photos.findIndex(function(p) { return p.id === memberCover.id; });
    }
  }
  return {
    photoId: anchorId,
    topOffset: cardRect.top - containerRect.top,
    // A stack selection has no focused frame, so the panel is the batch
    // inspector rather than one photo's detail — there is nothing to reopen.
    detailVisible: stackCoverId == null
      && document.getElementById('detailContent').classList.contains('visible'),
    index: index,
    stackSelection: stackCoverId != null,
    // The frames the user actually picked. The restore intersects these with
    // the reloaded group rather than adopting its membership wholesale.
    stackIds: stackCoverId == null ? null : Array.from(selectedPhotos),
  };
}

function restorePhotoAnchor(anchor) {
  if (!anchor) return false;
  var card = getBrowsePhotoElement(anchor.photoId);
  if (!card) return false;
  var container = document.getElementById('gridContainer');
  var containerRect = container.getBoundingClientRect();
  var cardRect = card.getBoundingClientRect();
  container.scrollTop += (cardRect.top - containerRect.top) - anchor.topOffset;
  return true;
}

/* A membership-change reload (tag, untag, location save, prediction accept)
   re-runs the current query because the edit can move photos in or out of it.
   The dataset it comes back with is the one the user was already looking at,
   minus or plus a few rows — so restarting at page 1 with ``scrollTop = 0``
   threw away their place in the grid and made them scroll down again after
   every single tag. Anchor on the topmost photo on screen instead.

   Separate from ``captureSelectedPhotoAnchor``: that one restores a *single*
   selection and its detail panel, and deliberately declines when a batch
   selection is active — which is exactly when a user is tagging. */
function captureBrowseViewportAnchor() {
  var container = document.getElementById('gridContainer');
  if (!container) return null;
  var containerTop = container.getBoundingClientRect().top;
  // Include ``.browse-stack-member``: an expanded stack tray can be taller
  // than the viewport, and if the user is scrolled inside it every card on
  // screen is a member. Skipping those would let the loop drop through to
  // the first top-level card *below* the tray — off-screen — and after the
  // reset collapses the tray, restoring that off-screen card at its old
  // large offset snaps the grid upward (Codex review r4012897730).
  var cards = document.querySelectorAll(
    '#grid .grid-card, #grid .browse-stack-member'
  );
  for (var i = 0; i < cards.length; i++) {
    var rect = cards[i].getBoundingClientRect();
    // First card still on screen: its bottom edge has not passed the top of
    // the scroll container.
    if (rect.bottom > containerTop + 1) {
      var id = parseInt(cards[i].dataset.id, 10);
      if (!id) return null;
      // Anchor on the stack cover, not the member: the reset collapses the
      // tray, so members no longer exist afterwards and ``photos`` never
      // held them anyway. The cover sits directly above where the tray
      // was, so its top after collapse is close to the member's top before.
      var anchorId = id;
      if (cards[i].classList.contains('browse-stack-member')) {
        var tray = cards[i].closest('.browse-stack-tray');
        var coverId = tray && tray.dataset.stackCoverId
          ? parseInt(tray.dataset.stackCoverId, 10)
          : NaN;
        if (coverId) anchorId = coverId;
      }
      return {
        photoId: anchorId,
        index: photos.findIndex(function(p) { return p.id === anchorId; }),
        topOffset: rect.top - containerTop,
      };
    }
  }
  return null;
}

async function restoreBrowseViewportAnchor(anchor, shouldContinue) {
  if (!anchor) return;
  // Page forward only as far as the anchor plausibly sits. Scanning to
  // ``allLoaded`` the way ``loadUntilPhotoRendered`` does would walk a
  // 60k-photo library end to end whenever the anchored photo is the one the
  // edit removed from the filtered set.
  var budget = Math.max(anchor.index, 0) + perPage;
  while (!allLoaded && photos.length <= budget && !getGridCard(anchor.photoId)) {
    if (shouldContinue && !shouldContinue()) return;
    var beforeLen = photos.length;
    if (await loadPhotos() !== true) return;
    if (shouldContinue && !shouldContinue()) return;
    if (photos.length === beforeLen) break;
  }
  var target = anchor;
  if (!getGridCard(anchor.photoId)) {
    // The anchored photo is gone — the user untagged the keyword the filter
    // is built on, say. Its neighbours are still there, so hold the same
    // position in the result set rather than snapping back to the top.
    var fallback = photos[Math.min(Math.max(anchor.index, 0), photos.length - 1)];
    if (!fallback || !getGridCard(fallback.id)) return;
    target = { photoId: fallback.id, topOffset: anchor.topOffset };
  }
  requestAnimationFrame(function() {
    if (shouldContinue && !shouldContinue()) return;
    restorePhotoAnchor(target);
    updateScrollPosition();
  });
}

function anchorRestoreIsPending(anchor, restoreEpoch) {
  return !!anchor && anchorRestoreEpoch === restoreEpoch && selectedPhotoId == null && selectedPhotos.size === 0;
}

function anchorSelectionIsCurrent(anchor, restoreEpoch) {
  if (restoreEpoch != null && anchorRestoreEpoch !== restoreEpoch) return false;
  if (!anchor) return false;
  // A restored stack selection lives in ``selectedPhotos`` with no focused
  // photo — the shape clicking the collapsed card produces, not the
  // single-focus shape the line below describes.
  if (anchor.stackSelection) {
    return selectedPhotoId == null && selectedPhotos.size > 0;
  }
  return selectedPhotoId === anchor.photoId && selectedPhotos.size === 0;
}

async function loadUntilPhotoRendered(photoId, shouldContinue, options) {
  var resolveStackMember = !!(options && options.resolveStackMember);
  // Optional cap on how many photos may be paged in while searching.
  // Callers that know the edit which triggered this scan can have removed
  // the target (a ``preserveScroll`` membership refresh) pass their own
  // budget so the scan does not walk the whole catalog looking for a photo
  // the filter no longer matches.
  var budget = options && options.budget != null ? options.budget : null;
  var card = getGridCard(photoId);
  var stackCover = resolveStackMember ? loadedBrowseStackCoverForPhoto(photoId) : null;
  while (!card && !stackCover && !allLoaded) {
    if (budget != null && photos.length > budget) break;
    if (shouldContinue && !shouldContinue()) return null;
    var beforePage = currentPage;
    var beforeLen = photos.length;
    await loadPhotos();
    if (shouldContinue && !shouldContinue()) return null;
    if (currentPage === beforePage && photos.length === beforeLen) break;
    card = getGridCard(photoId);
    if (resolveStackMember) stackCover = loadedBrowseStackCoverForPhoto(photoId);
  }
  if (!card && stackCover) {
    await toggleBrowseStack(null, stackCover.id);
    if (shouldContinue && !shouldContinue()) return null;
    card = getBrowsePhotoElement(photoId);
  }
  return card;
}

function clearActiveSelectionAndDetail() {
  anchorRestoreEpoch++;
  selectedPhotos.clear();
  selectedPhotoId = null;
  selectedIndex = -1;
  closeDetail();
}
