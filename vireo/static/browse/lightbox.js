/* Browse: opening the shared lightbox from the grid and reconciling its events.
   Classic script sharing page globals; browse.html loads the
   browse/*.js files in dependency order. */

/* ---------- Grid dblclick delegation (XSS-safe) ---------- */
document.getElementById('grid').addEventListener('dblclick', function(e) {
  var card = e.target.closest('.grid-card');
  if (!card || card.classList.contains('offline')) return;
  var id = parseInt(card.dataset.id, 10);
  var filename = card.dataset.filename || '';
  // Provenance for the close handler. A double-click on a stack card runs its
  // own two clicks through selectPhoto first, so the stack is selected by the
  // time the lightbox opens — a selection this viewing gesture made, not a
  // batch the user assembled, and the only one the close handler may replace.
  // Recording the opener is the only way to tell those apart: the selected ids
  // are identical either way.
  //
  // Except on the stack badge, whose two clicks expand and collapse the stack
  // and stop propagating, so they never reach selectPhoto and never make a
  // selection. The dblclick still bubbles here, and an identical id set would
  // otherwise let a deliberate batch be claimed by a gesture that did not
  // create it.
  //
  // And a card dblclick over a batch the user already assembled (tray Select
  // all + collapse) still runs through selectPhoto, so the resulting ids also
  // read the same as the gesture case even though the two clicks reaffirmed
  // the batch rather than creating it. Consult the pre-first-click snapshot:
  // only mark the gesture when the clicks actually moved the selection onto
  // this stack. Codex P2 on PR #1672.
  var gestureIds = e.target.closest('.browse-stack-badge')
    ? null : browseStackMemberIds(id);
  var preState = browseSelectionBeforeStackDblClickStart;
  browseSelectionBeforeStackDblClickStart = null;
  var preStateWasThisStack = !!(
    preState && gestureIds
    && preState.size === gestureIds.length
    && gestureIds.every(function(memberId) { return preState.has(memberId); })
  );
  browseLightboxStackGestureSpent = null;
  browseLightboxStackGesture = (
    !preStateWasThisStack
    && gestureIds
    && gestureIds.length === selectedPhotos.size
    && gestureIds.every(function(memberId) { return selectedPhotos.has(memberId); })
  ) ? { ids: gestureIds.slice(), epoch: anchorRestoreEpoch } : null;
  // Top-level grid cards navigate the grid, matching pre-stacks behaviour;
  // an expanded stack's members live in ``.browse-stack-member`` cards and
  // open against their own member list instead.
  openLightbox(id, filename, availableBrowsePhotos());
});

var browseAvailableLightboxPhotos = [];

function browsePhotoIsAvailable(photo) {
  return !photo.folder_status ||
    photo.folder_status === 'ok' || photo.folder_status === 'partial';
}

function replaceBrowseAvailableLightboxPhotos(available) {
  browseAvailableLightboxPhotos.length = 0;
  available.forEach(function(photo) {
    browseAvailableLightboxPhotos.push(photo);
  });
}

function syncBrowseAvailableLightboxPhotos() {
  if (window._lightboxPhotoList !== browseAvailableLightboxPhotos) return;
  replaceBrowseAvailableLightboxPhotos(photos.filter(browsePhotoIsAvailable));
}

function availableBrowsePhotos() {
  // Default Browse results already exclude offline folders. Preserve the
  // shared array identity so lightbox boundary prefetch sees later pages as
  // loadPhotos mutates ``photos``. The opt-in offline view needs a filtered
  // list; keep that list stable and synchronize it after every append/prepend.
  if (!showOfflineCollectionPhotos) return photos;
  replaceBrowseAvailableLightboxPhotos(photos.filter(browsePhotoIsAvailable));
  return browseAvailableLightboxPhotos;
}

function getBrowseShortcutPhoto() {
  if (selectedPhotoId != null) {
    var selected = findBrowsePhoto(selectedPhotoId);
    if (selected) {
      var selectedNavigation = browsePhotoNavigationList(selectedPhotoId);
      if (browseNavigationListIsTopLevel(selectedNavigation) && !photos.some(function(photo) {
        return photo.id === selectedPhotoId;
      })) {
        var selectedCoverId = browseStackCoverIdForPhoto(selectedPhotoId);
        var selectedCover = photos.find(function(photo) { return photo.id === selectedCoverId; });
        if (selectedCover) selected = selectedCover;
      }
      return {
        photo: selected,
        index: selectedIndex,
        navigationPhotos: selectedNavigation,
      };
    }
  }
  if (selectedPhotos.size > 0) {
    var selectedIds = Array.from(selectedPhotos);
    for (var selectedOffset = 0; selectedOffset < selectedIds.length; selectedOffset++) {
      var selectedMember = findBrowsePhoto(selectedIds[selectedOffset]);
      if (selectedMember) {
        var memberNavigation = browsePhotoNavigationList(selectedMember.id);
        if (browseNavigationListIsTopLevel(memberNavigation) && !photos.some(function(photo) {
          return photo.id === selectedMember.id;
        })) {
          var memberCoverId = browseStackCoverIdForPhoto(selectedMember.id);
          var memberCover = photos.find(function(photo) { return photo.id === memberCoverId; });
          if (memberCover) selectedMember = memberCover;
        }
        return {
          photo: selectedMember,
          index: selectedIndex,
          navigationPhotos: memberNavigation,
        };
      }
    }
  }
  var idx = -1;
  if (idx < 0 && selectedIndex >= 0 && photos[selectedIndex]) {
    idx = selectedIndex;
  }
  if (idx < 0 && photos.length > 0) {
    idx = photos.findIndex(function(photo) {
      return !photo.folder_status ||
        photo.folder_status === 'ok' || photo.folder_status === 'partial';
    });
  }
  if (idx < 0 || !photos[idx]) return null;
  return {
    photo: photos[idx],
    index: idx,
    navigationPhotos: availableBrowsePhotos(),
  };
}

function openBrowseShortcutPhoto(fullscreen) {
  var item = getBrowseShortcutPhoto();
  if (!item) return false;
  openLightbox(item.photo.id, item.photo.filename || '', item.navigationPhotos);
  if (fullscreen && typeof requestLightboxFullscreen === 'function') {
    requestLightboxFullscreen();
  }
  return true;
}

function browseKeyMatchesConfiguredShortcut(e) {
  if (!_shortcuts) return false;
  for (var action in _shortcuts) {
    if (Object.prototype.hasOwnProperty.call(_shortcuts, action) && matchesShortcut(e, _shortcuts[action])) {
      return true;
    }
  }
  return false;
}

/* Keep Browse's lightbox navigation moving across lazy-loaded page edges.
   The normal grid and lightbox share ``photos``; the opt-in offline grid uses
   a stable available-only list. ``loadPhotos``/``prependPreviousPhotos`` keep
   the active list synchronized, so prefetched pages reach the lightbox without
   reopening it. If the user reaches an edge before the request finishes, retry
   at the same boundary and advance as soon as the page is available. */
var browseLightboxBoundaryRetry = null;
var browseLightboxSession = 0;
// Set by the grid double-click handler when the gesture's own clicks selected
// the stack it opened: {ids, epoch}. It is spent by the close of the lightbox
// it opened, so a later viewing shortcut over the same selection is a viewing
// shortcut over a batch. ``epoch`` is the selection generation the gesture was
// made in, so any selection action in between — a click, a Select all, a
// Clear — also retires it, without every one of those paths having to know
// this marker exists.
var browseLightboxStackGesture = null;
// The gesture a delete-button close just retired, held only until the delete
// that caused it either completes (and hands it back) or is cancelled (and
// never claims it). See the ``lightbox:photodeleted`` listener.
var browseLightboxStackGestureSpent = null;

// What each photo viewed in this lightbox session stands for, for the covers
// among them: {photoId: memberIds}. Recorded while the photo is on screen,
// because once it is deleted its stack is gone from ``photos`` and nothing
// can say what it stood for. Keyed by photo rather than held in a single
// slot so the reopen that follows a delete cannot overwrite the entry the
// delete still needs. Read by the ``lightbox:photodeleted`` listener, and
// dropped when the lightbox is really closed. Codex P2 on PR #1672.
var browseLightboxRepresentedByPhoto = {};

// The selection as it stood before the first click of the most recent click
// sequence. Captured in ``selectPhoto`` and consulted by the grid double-click
// handler so it can tell "these clicks made the selection" from "these clicks
// reaffirmed a selection the user already had": a deliberate tray Select all
// + collapse leaves the stack members in ``selectedPhotos``, and a plain
// click on the collapsed cover then sets the same set again, so the
// resulting ``selectedPhotos`` is byte-for-byte identical either way. Without
// this snapshot the dblclick handler would mark that pre-existing batch as
// gesture-generated and the close handler would then replace it with the
// finished-on photo. Codex P2 on PR #1672.
var browseSelectionBeforeStackDblClickStart = null;

function browseLightboxOwnsLoadedWindow() {
  return (
    typeof window._lightboxPhotoList !== 'undefined' &&
    browseNavigationListIsTopLevel(window._lightboxPhotoList) &&
    typeof window.vireoLightboxSession !== 'undefined' &&
    window.vireoLightboxSession.requestedPhotoId() != null
  );
}

function refreshBrowseLightboxCounter() {
  if (!browseLightboxOwnsLoadedWindow()) return;
  var visibleId = window.vireoLightboxSession.displayedPhotoId() != null
    ? window.vireoLightboxSession.displayedPhotoId()
    : window.vireoLightboxSession.requestedPhotoId();
  var lightboxPhotos = window._lightboxPhotoList;
  var index = lightboxPhotos.findIndex(function(photo) {
    return photo.id === visibleId;
  });
  var counter = document.getElementById('lightboxCounter');
  if (index < 0 || !counter || lightboxPhotos.length < 2) return;
  var filename = lightboxPhotos[index].filename || '';
  counter.textContent = (index + 1) + ' / ' + lightboxPhotos.length
    + (filename ? ' \u00b7 ' + filename : '');
  counter.title = filename;
  counter.style.display = '';
}

function continueBrowseLightboxAcrossBoundary(delta, currentId, session) {
  if (session !== browseLightboxSession) return;
  if (!browseLightboxOwnsLoadedWindow() || window.vireoLightboxSession.requestedPhotoId() !== currentId) return;
  var lightboxPhotos = window._lightboxPhotoList;
  var index = lightboxPhotos.findIndex(function(photo) {
    return photo.id === currentId;
  });
  if (index < 0) return;

  var nextIndex = index + delta;
  if (nextIndex >= 0 && nextIndex < lightboxPhotos.length) {
    lightboxNav(delta);
    return;
  }

  var canLoad = delta > 0 ? !allLoaded : earliestPage > 1;
  if (!canLoad) return;
  if (loading) {
    clearTimeout(browseLightboxBoundaryRetry);
    browseLightboxBoundaryRetry = setTimeout(function() {
      continueBrowseLightboxAcrossBoundary(delta, currentId, session);
    }, 50);
    return;
  }

  var request = delta > 0
    ? loadPhotos({ silent: true })
    : prependPreviousPhotos({ silent: true });
  Promise.resolve(request).then(function(loaded) {
    if (loaded !== true) return;
    if (session !== browseLightboxSession) return;
    if (!browseLightboxOwnsLoadedWindow() || window.vireoLightboxSession.requestedPhotoId() !== currentId) return;
    lightboxNav(delta);
  });
}

document.addEventListener('lightbox:photochanged', function(event) {
  // Before the window check below: this has to happen for every photo the
  // user actually sees, whoever owns the loaded window.
  var shownId = event && event.detail && event.detail.photoId;
  if (shownId != null) {
    var shownStack = browseStackMemberIds(shownId);
    if (shownStack) browseLightboxRepresentedByPhoto[String(shownId)] = shownStack;
  }
  if (!browseLightboxOwnsLoadedWindow() || loading) return;
  var photoId = event && event.detail && event.detail.photoId;
  var lightboxPhotos = window._lightboxPhotoList;
  var index = lightboxPhotos.findIndex(function(photo) {
    return photo.id === photoId;
  });
  if (index < 0) return;
  if (!allLoaded && index >= lightboxPhotos.length - 3) loadPhotos({ silent: true });
  if (earliestPage > 1 && index <= 2) prependPreviousPhotos({ silent: true });
});

document.addEventListener('lightbox:navigationboundary', function(event) {
  var detail = event && event.detail || {};
  if (detail.delta !== 1 && detail.delta !== -1) return;
  if (!browseLightboxOwnsLoadedWindow()) return;
  continueBrowseLightboxAcrossBoundary(
    detail.delta, detail.photoId, browseLightboxSession
  );
});

document.addEventListener('lightbox:closed', function() {
  browseLightboxSession++;
  clearTimeout(browseLightboxBoundaryRetry);
  browseLightboxBoundaryRetry = null;
});

// Lightbox navigation has its own current-photo state. Reconcile that state
// with Browse only when the overlay closes so returning to the grid focuses
// the photo the user finished on, without reloading the detail panel on every
// Previous/Next step while the lightbox is still open.
// A delete that actually happened reopens the lightbox on the next photo, so
// the gesture the delete-button close set aside continues into that session —
// but only if this delete is the one it was set aside for. Deleting some other
// photo later (after a cancel, say) must not inherit it.
document.addEventListener('lightbox:photodeleted', function(event) {
  var deletedId = event && event.detail ? event.detail.photoId : null;
  var spent = browseLightboxStackGestureSpent;
  browseLightboxStackGestureSpent = null;
  if (deletedId == null) return;
  var represented = browseLightboxRepresentedByPhoto[String(deletedId)];
  delete browseLightboxRepresentedByPhoto[String(deletedId)];
  // The deleted photo was a stack cover, so the frames behind it just lost
  // the only card standing for them — lightboxDelete splices the cover out of
  // `photos` and nothing reprojects the stack. Selected frames have to go
  // with it, or the batch bar keeps counting photos no shortcut can show and
  // the next rating lands on them unseen. Codex P2 on PR #1672.
  if (represented) {
    // Recorded while the deleted photo was on screen, and a stack can be
    // reprojected in between: a rating or flag in the lightbox promotes
    // another member, so deleting the demoted photo leaves the stack standing
    // under its new cover. Drop only the ids that no card represents *now* —
    // a bounded check, over this one stack's ids rather than the whole
    // selection. Codex P2 on PR #1672.
    var representedNow = new Set();
    photos.forEach(function(photo) {
      representedNow.add(photo.id);
      var stack = photo.browse_stack;
      ((stack && stack.photo_ids) || []).forEach(function(id) {
        representedNow.add(id);
      });
      (browseStackMembers[String(photo.id)] || []).forEach(function(member) {
        representedNow.add(member.id);
      });
    });
    var orphaned = represented.filter(function(id) {
      return !representedNow.has(id);
    });
    var dropped = false;
    orphaned.forEach(function(id) {
      if (selectedPhotos.delete(id)) dropped = true;
    });
    if (selectedPhotoId != null && orphaned.indexOf(selectedPhotoId) !== -1) {
      selectedPhotoId = null;
      selectedIndex = -1;
      dropped = true;
    }
    if (dropped) {
      refreshCardSelectionVisuals();
      updateBatchBar();
    }
  }
  // A hidden tray member deleted from its own lightbox is not in ``photos``,
  // so the splice in ``lightboxDelete`` never runs and nothing above updates
  // the cover: its ``browse_stack.photo_ids`` and ``count`` still carry the
  // deleted id, and the ``browseStackMembers`` hydration cache still lists it.
  // The ``represented`` block above skips this case because the deleted photo
  // was not itself a top-level representation. After the tray collapses, a
  // click on the cover reads that stale ``photo_ids`` and puts the deleted
  // id back into ``selectedPhotos``: the batch bar overcounts, and an
  // Add Keyword then commits every live id before the stale id trips a
  // foreign-key failure — partial writes the user cannot see coming.
  // Prune the deleted id from each cover's stack metadata (and cached
  // members) so what the card stands for matches what still exists, then
  // repaint the affected badge so its count matches the pruned list.
  // Codex P2 on PR #1672.
  var badgesToRefresh = [];
  photos.forEach(function(photo) {
    var stack = photo.browse_stack;
    var stackIds = stack && stack.photo_ids;
    if (!Array.isArray(stackIds)) return;
    var idx = stackIds.indexOf(deletedId);
    if (idx === -1) return;
    stackIds.splice(idx, 1);
    if (typeof stack.count === 'number') {
      stack.count = Math.max(0, stack.count - 1);
    }
    var cacheKey = String(photo.id);
    var members = browseStackMembers[cacheKey];
    if (Array.isArray(members)) {
      browseStackMembers[cacheKey] = members.filter(function(member) {
        return member.id !== deletedId;
      });
    }
    // A two-photo stack that loses its last hidden member has no frames left to
    // stand for, so its cover is now an ordinary card. Leaving
    // ``photo.browse_stack`` truthy keeps ``has-browse-stack`` on the tile and
    // lets ``restoreExpandedBrowseStacks`` re-insert a tray with a single
    // member (the cover, or worse the deleted id from a stale hydration cache
    // before ``browseStackMembers`` was pruned above). Dissolve the stack
    // instead: drop the cache, retire the expansion, remove any live tray,
    // and repaint the card so it stops advertising a stack that no longer
    // exists. Codex P2 on PR #1672.
    if (stack.count < 2) {
      photo.browse_stack = null;
      delete browseStackMembers[cacheKey];
      expandedBrowseStacks.delete(photo.id);
      browseStackCoverRecheck.delete(photo.id);
      var tray = document.querySelector(
        '.browse-stack-tray[data-stack-cover-id="' + photo.id + '"]');
      if (tray) tray.remove();
      var card = document.querySelector(
        '.grid-card[data-id="' + photo.id + '"]');
      if (card) {
        card.classList.remove('has-browse-stack');
        card.classList.remove('stack-partial');
      }
    }
    badgesToRefresh.push(photo.id);
  });
  if (badgesToRefresh.length && typeof refreshBrowseStackBadge === 'function') {
    badgesToRefresh.forEach(refreshBrowseStackBadge);
  }
  // Pruning the cover metadata is only half of what the tray-member delete
  // needs. ``lightboxDelete`` has already dropped the deleted id from
  // ``selectedPhotos`` / ``selectedPhotoId`` and rendered the grid, but the
  // batch bar's "N selected" text was written from the pre-delete count and
  // never rewrites itself, and a cover whose remaining live members are all
  // selected stays painted ``stack-partial`` from that same pre-delete
  // state — because the deleted id was in the members list back then, the
  // whole-stack check that would repaint it as ``selected`` said no. Refresh
  // both from the surviving selection so the count and the paint match what
  // the user can see. Codex P2 on PR #1672.
  if (badgesToRefresh.length) {
    if (typeof refreshCardSelectionVisuals === 'function') {
      refreshCardSelectionVisuals();
    }
    if (typeof updateBatchBar === 'function') {
      updateBatchBar();
    }
  }
  if (!spent) return;
  if (spent.epoch !== anchorRestoreEpoch) return;
  if (spent.ids.indexOf(deletedId) === -1) return;
  browseLightboxStackGesture = spent;
});

// Reconcile browse selection state for a null-photo close. The only caller
// of ``closeLightbox(null)`` is ``lightboxDelete()`` after its splice emptied
// ``_lightboxPhotoList`` — a stack cover was the deleted photo's only
// top-level representation, and its hidden members are still in
// ``selectedPhotos`` with no card left on the grid to stand for them. The
// ``lightbox:photodeleted`` handler that runs immediately after this would
// otherwise re-arm the set-aside gesture, leaving the batch bar counting
// photos a shortcut cannot see — see the stack-gesture rules for why this
// close has to actively clean up rather than return unchanged.
// Codex P2 on PR #1672.
function browseReconcileEmptyLightboxClose() {
  var pending = browseLightboxStackGesture || browseLightboxStackGestureSpent;
  browseLightboxStackGesture = null;
  browseLightboxStackGestureSpent = null;
  var touched = false;
  if (pending) {
    pending.ids.forEach(function(id) {
      if (selectedPhotos.delete(id)) touched = true;
    });
    if (selectedPhotoId != null && pending.ids.indexOf(selectedPhotoId) !== -1) {
      selectedPhotoId = null;
      touched = true;
    }
  }
  // Even with no gesture in either slot, an empty close means the deleted
  // photo was the only top-level lightbox entry — and when that photo was
  // a stack cover selected by a single click (or the tray's Select all),
  // its hidden members are still in ``selectedPhotos`` but no card on the
  // grid represents them any more. Batch shortcuts would then act on
  // photos the user cannot see. Drop any selected id whose cover has left
  // ``photos`` so the selection matches what is on screen — CORE_PHILOSOPHY
  // "no black boxes", the batch bar has to count photos the user can see.
  // Codex P2 on PR #1672.
  // Only when the grid holds the whole result set — both ends of it.
  // ``allLoaded`` says the tail is exhausted, which a focused or deep-linked
  // window reaches while ``earliestPage`` is still past 1 and the pages
  // before it have never been fetched. "Not in ``photos``" means
  // "no card on screen" only once there are no more pages to load: with a
  // Select all that reaches past the loaded window, every id from a later
  // page looks unreachable here and the sweep would quietly delete most of
  // the user's selection — photos that exist and are still perfectly valid
  // members of it. The bounded drop in the ``lightbox:photodeleted`` handler
  // covers the partial-window case, because it knows which ids the deleted
  // cover actually stood for. Codex P2 on PR #1672.
  if (selectedPhotos.size > 0 && allLoaded && earliestPage === 1) {
    var reachable = new Set();
    photos.forEach(function(photo) {
      reachable.add(photo.id);
      // A collapsed stack that has never been expanded has no entries in
      // ``browseStackMembers``, but the card still represents every id in
      // ``photo.browse_stack.photo_ids``. Reading only the hydrated cache
      // would treat those hidden members as unreachable and drop them from a
      // batch the surviving card still stands for — for example, select two
      // collapsed stacks, expand one, delete every member from its tray
      // lightbox: this empty close would then reduce the untouched second
      // stack to its cover. Codex P2 on PR #1672.
      var stackIds = (photo.browse_stack && photo.browse_stack.photo_ids) || [];
      stackIds.forEach(function(id) { reachable.add(id); });
      var members = browseStackMembers[String(photo.id)] || [];
      members.forEach(function(member) { reachable.add(member.id); });
    });
    Array.from(selectedPhotos).forEach(function(id) {
      if (!reachable.has(id)) {
        selectedPhotos.delete(id);
        touched = true;
      }
    });
  }
  if (selectedPhotoId != null && allLoaded && earliestPage === 1) {
    var focusStillReachable = photos.some(function(photo) {
      if (photo.id === selectedPhotoId) return true;
      // Same reasoning as the reachable sweep: a hidden member of an
      // uncached collapsed stack is still represented by its cover card.
      // Codex P2 on PR #1672.
      var stackIds = (photo.browse_stack && photo.browse_stack.photo_ids) || [];
      if (stackIds.indexOf(selectedPhotoId) !== -1) return true;
      var members = browseStackMembers[String(photo.id)] || [];
      return members.some(function(member) { return member.id === selectedPhotoId; });
    });
    if (!focusStillReachable) {
      selectedPhotoId = null;
      touched = true;
    }
  }
  if (touched) {
    if (typeof refreshCardSelectionVisuals === 'function') {
      refreshCardSelectionVisuals();
    }
    if (typeof updateBatchBar === 'function') updateBatchBar();
  }
}

document.addEventListener('lightbox:closed', function(event) {
  var photoId = event && event.detail ? event.detail.photoId : null;
  if (photoId == null) {
    browseReconcileEmptyLightboxClose();
    return;
  }
  // Every close retires the gesture — including the one the lightbox's own
  // Delete button causes on its way to the delete dialog (it sits inside the
  // overlay and does not stop propagating). That close only looks
  // intermediate: cancelling the dialog reopens nothing, so a gesture held
  // across it would outlive its lightbox and be mistaken for the next one. It
  // is set aside instead, and handed back only by a delete that actually
  // happens — which reopens the lightbox on the next photo, so there is a
  // real close still to come. Codex P2 on PR #1672.
  var stackGesture = browseLightboxStackGesture;
  var deleteDialogOpen = !!document.querySelector('#deleteModal.open');
  browseLightboxStackGestureSpent = deleteDialogOpen ? stackGesture : null;
  browseLightboxStackGesture = null;
  // The delete dialog is open over this close and the lightbox will reopen on
  // the next photo, so what the viewed photos stood for is still needed. Any
  // other close is the end of the session. Codex P2 on PR #1672.
  if (!deleteDialogOpen) browseLightboxRepresentedByPhoto = {};
  if (selectedPhotos.size > 0) {
    // Viewing shortcuts can open the lightbox without collapsing an existing
    // batch. Preserve that batch instead of replacing it with the viewed
    // photo — unless this lightbox was opened by a double-click on a stack
    // card and the selection is still the one that gesture made, which is the
    // only "batch" the user did not assemble. Same ids, different provenance:
    // an intentional stack selection (tray Select all, then E) bumps the
    // selection epoch and so still reads as a batch.
    //
    // "Still the one that gesture made" allows shrinkage: deleting a photo in
    // the lightbox drops it from the selection, and that is this gesture's
    // selection mutated, not someone's batch. Without that, deleting a
    // stack's cover — its only top-level card — left the hidden members
    // selected with nothing on screen representing them, and the next batch
    // shortcut would act on photos the user cannot see.
    // Codex P2 on PR #1672.
    var gestureIds = stackGesture && stackGesture.epoch === anchorRestoreEpoch
      ? new Set(stackGesture.ids) : null;
    if (!gestureIds || !Array.from(selectedPhotos).every(function(id) {
      return gestureIds.has(id);
    })) return;
    // Finished inside that stack: the viewing gesture ended where it began,
    // so the stack stays selected rather than shrinking to one frame. The
    // gesture is spent either way — reopening the same stack with a viewing
    // shortcut afterwards is a viewing shortcut over a batch, and batches are
    // preserved. Codex P2 on PR #1672.
    if (selectedPhotos.has(photoId)) {
      var stackCoverId = browseStackCoverIdForPhoto(photoId);
      var stackIdx = photos.findIndex(function(photo) {
        return photo.id === photoId || photo.id === stackCoverId;
      });
      if (stackIdx >= 0) {
        selectedIndex = stackIdx;
        scrollToCard(stackIdx);
      }
      return;
    }
  }
  var idx = photos.findIndex(function(photo) { return photo.id === photoId; });
  if (idx < 0) {
    var coverId = browseStackCoverIdForPhoto(photoId);
    idx = photos.findIndex(function(photo) { return photo.id === coverId; });
  }
  // Nothing to reconcile to — leave every bit of state alone.
  if (idx < 0 || !findBrowsePhoto(photoId)) return;
  // Past the guard above, so the gesture's selection is replaced by the photo
  // the user actually finished on.
  if (selectedPhotos.size > 0) {
    selectedPhotos.clear();
    browseLightboxStackGesture = null;
  }
  // Skip `selectPhoto` when the focused photo is already the one being
  // returned to: it calls `loadDetail`, which refetches and rerenders the
  // detail panel and would silently discard an unsubmitted `#locationInput`
  // draft (its blur handler only hides suggestions, it does not save). Only
  // reload details when lightbox navigation actually changed the photo.
  if (selectedPhotoId === photoId) {
    if (selectedIndex !== idx) selectedIndex = idx;
    scrollToCard(idx);
    return;
  }
  // stackAware: false — this restores the grid focus to the photo the user
  // finished viewing. Closing the lightbox on a stack cover must not silently
  // promote a single-photo view into a stack-wide batch.
  selectPhoto({ shiftKey: false, metaKey: false, ctrlKey: false }, photoId, idx,
              { stackAware: false });
  scrollToCard(idx);
});
