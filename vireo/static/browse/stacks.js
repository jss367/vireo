/* Browse: burst stacks: membership, badges, trays, cover reconciliation.
   Classic script sharing page globals; browse.html loads the
   browse/*.js files in dependency order. */

function browseStacksEnabled() {
  var toggle = document.getElementById('browseStacksToggle');
  return !!(toggle && toggle.checked);
}

function browseStackLabel(kind) {
  return kind === 'duplicate' ? 'Exact duplicates' : 'Burst';
}

// A collapsed stack card stands for its whole stack, not for the one frame
// whose thumbnail it borrows: the badge counts the frames behind it, and a
// species, rating, flag or color the user applies while looking at that card
// is meant for all of them. So clicking it selects every member — the count
// in the batch bar and the selection panel then states exactly what the next
// action will touch, instead of silently acting on 1 of 11 (CORE_PHILOSOPHY,
// "no black boxes"). Working frame by frame is what expanding the stack is
// for: the tray selects members individually.
//
// Members are the filter-scoped ones the badge counted, and the projection
// never puts an offline frame in a stack (see ``_browse_stack_query_parts``),
// so every id returned here is actionable. The cover leads the list for the
// same reason Select-all flattens cover-first: Best Batch, burst review and
// the export preview all start from the card the user can see.
function browseStackMemberIdsFor(photo) {
  var stack = photo && photo.browse_stack;
  if (!stack || stack.count < 2) return null;
  var members = (stack.photo_ids || []).filter(function(id) {
    return id !== photo.id;
  });
  if (!members.length) return null;
  return [photo.id].concat(members);
}

// By id, for the callers that only have one. Stack cards are top-level grid
// cards, so an id that is not in ``photos`` — a tray member — is a single
// photo by construction.
function browseStackMemberIds(photoId) {
  return browseStackMemberIdsFor(photos.find(function(p) {
    return p.id === photoId;
  }));
}

// The ids one click acts on: the whole stack for a collapsed stack card, the
// single photo for anything else. ``stackAware: false`` is for the callers
// that are restoring a focus rather than making a selection — a tray member
// click, a collapsing tray, a closing lightbox — where expanding to the
// stack would escalate a view action into a 15-photo batch.
function browseSelectionIdsForClick(photoId, opts) {
  if (opts && opts.stackAware === false) return [photoId];
  return browseStackMemberIds(photoId) || [photoId];
}

// Shift-clicking from one tray member to another: both ends report the
// cover's grid slot, so the top-level range loop cannot tell them apart and
// would take the whole stack. Range over the tray's own member order
// instead, which is the order the user is looking at.
function browseStackMemberRange(anchorId, targetId) {
  if (anchorId == null) return null;
  var coverId = browseStackCoverIdForPhoto(targetId);
  if (coverId == null || browseStackCoverIdForPhoto(anchorId) !== coverId) return null;
  var members = browseStackMembers[String(coverId)] || [];
  var anchorAt = -1, targetAt = -1;
  members.forEach(function(member, at) {
    if (member.id === anchorId) anchorAt = at;
    if (member.id === targetId) targetAt = at;
  });
  if (anchorAt < 0 || targetAt < 0) return null;
  return members
    .slice(Math.min(anchorAt, targetAt), Math.max(anchorAt, targetAt) + 1)
    .map(function(member) { return member.id; });
}

// Whether a batch action would touch this photo — the same set-over-focus
// precedence getActiveSelection() applies, and it has to be the same or a
// card paints a claim the action will not honour. Cmd-clicking a stack's
// cover out of the tray and then collapsing leaves the focus on that cover
// while the set holds only the other frames, and reading the two as an "or"
// painted the whole stack as selected while a rating would have skipped the
// frame on top. Codex P2 on PR #1672.
function browseSelectionIncludes(id) {
  return selectedPhotos.size > 0
    ? selectedPhotos.has(id)
    : id === selectedPhotoId;
}

// Which of this card's photos are in the active selection. A stack card
// paints the full selected ring only when the whole stack is in.
function browseCardSelectionClass(photo) {
  var members = browseStackMemberIdsFor(photo);
  if (!members) {
    return photo && browseSelectionIncludes(photo.id) ? ' selected' : '';
  }
  var chosen = members.filter(browseSelectionIncludes).length;
  if (chosen === members.length) return ' selected';
  return chosen ? ' stack-partial' : '';
}

// "12 photos selected · 2 stacks" — the sentence a stack click has to be able
// to answer, since one click now moves the count by more than one. It is only
// printable when the stacks and singles currently in the grid account for
// every selected photo: counting the stacks that happen to be loaded out of a
// selection that reaches past the window would be a proxy, not an answer, so
// that case says nothing rather than something cheaper.
function browseSelectionStackNote(ids) {
  if (!ids.length) return '';
  var remaining = new Set(ids);
  var stacks = 0;
  photos.forEach(function(photo) {
    var stack = photo.browse_stack;
    var members = (stack && stack.count >= 2 && stack.photo_ids) || [];
    if (!members.length) return;
    if (!members.every(function(id) { return remaining.has(id); })) return;
    stacks++;
    members.forEach(function(id) { remaining.delete(id); });
  });
  if (!stacks) return '';
  photos.forEach(function(photo) { remaining.delete(photo.id); });
  if (remaining.size) return '';
  return ' · ' + stacks + (stacks === 1 ? ' stack' : ' stacks');
}

function renderBrowseStackBadge(p) {
  var stack = p && p.browse_stack;
  if (!stack || stack.count < 2) return '';
  var label = browseStackLabel(stack.kind, stack.count);
  var expanded = expandedBrowseStacks.has(p.id);
  // A cover reconciliation that could not load this stack's members leaves the
  // pre-edit cover on screen. Say that on the badge itself: the toast that
  // reported it is long gone by the time the user looks back at the grid.
  var needsRecheck = browseStackCoverRecheck.has(p.id);
  var title = label + ' · ' + stack.count + ' photos · clicking the card selects all '
    + stack.count + ', this badge expands the stack to pick single frames';
  if (needsRecheck) {
    title += ' · cover may be out of date after a recent edit — expand to refresh';
  }
  return '<button type="button" class="browse-stack-badge'
    + (needsRecheck ? ' needs-recheck' : '') + '" '
    + 'aria-expanded="' + (expanded ? 'true' : 'false') + '" '
    + 'title="' + escapeAttr(title) + '" '
    + 'onclick="toggleBrowseStack(event,' + p.id + ')">'
    + '<span aria-hidden="true">&#9638;</span>' + stack.count
    + (needsRecheck ? '<span class="browse-stack-recheck" aria-hidden="true">!</span>' : '')
    + '</button>';
}

// Repaints just the stack badge for one cover. refreshGridCards() rewrites card
// info and overlay badges but not the stack badge, and renderGrid() is far too
// heavy to run for a marker change on a single card.
function refreshBrowseStackBadge(coverId) {
  var card = document.querySelector('.grid-card[data-id="' + coverId + '"]');
  if (!card) return;
  var badge = card.querySelector('.browse-stack-badge');
  var photo = photos.find(function(item) { return item.id === coverId; });
  if (!badge || !photo) return;
  badge.outerHTML = renderBrowseStackBadge(photo);
}

function renderBrowseStackMember(p, coverId) {
  var selectedClass = browseSelectionIncludes(p.id) ? ' selected' : '';
  if (typeof window.vireoRememberPhotoPair === 'function') {
    window.vireoRememberPhotoPair(p);
  }
  if (
    typeof window.vireoRememberPhotoEditRecipe === 'function' &&
    Object.prototype.hasOwnProperty.call(p, 'edit_recipe')
  ) {
    window.vireoRememberPhotoEditRecipe(p.id, p.edit_recipe, {
      skipIfLocallyWritten: true,
    });
  }
  var thumbUrl = window.vireoThumbnailUrl
    ? window.vireoThumbnailUrl(p)
    : '/thumbnails/' + p.id + '.jpg';
  return '<div class="browse-stack-member' + selectedClass + '" '
    + 'data-id="' + p.id + '" data-filename="' + escapeAttr(p.filename) + '"'
    + cardColorLabelAttr(p.id) + ' '
    + 'onclick="selectBrowseStackMember(event,' + p.id + ',' + coverId + ')" '
    + 'ondblclick="openBrowseStackMember(event,' + p.id + ',' + coverId + ')">'
    + '<div class="grid-card-img-wrap">'
    + '<img data-thumbnail-src="' + escapeAttr(thumbUrl) + '" decoding="async" alt="' + escapeAttr(p.filename) + '">'
    + renderDetectionBoxes(p)
    + (inatSubmitted[String(p.id)] ? '<span class="inat-badge">iNat</span>' : '')
    + (p.wildlife_excluded ? '<span class="no-wildlife-badge">No Wildlife</span>' : '')
    + (p.is_species_representative ? '<span class="representative-badge">Representative</span>' : '')
    + (cardFields.indexOf('species') === -1 ? renderSpeciesBadges(p.species, false) : '')
    + '</div><div class="grid-card-info">' + renderCardInfo(p) + '</div></div>';
}

function renderBrowseStackTray(cover) {
  var stack = cover.browse_stack;
  var members = browseStackMembers[String(cover.id)];
  var error = browseStackErrors[String(cover.id)];
  var label = browseStackLabel(stack.kind, stack.count);
  var body = '<div class="browse-stack-loading">Loading stack…</div>';
  if (error) {
    body = '<div class="browse-stack-error">' + escapeHtml(error) + '</div>';
  } else if (Array.isArray(members)) {
    body = '<div class="browse-stack-members">' + members.map(function(member) {
      return renderBrowseStackMember(member, cover.id);
    }).join('') + '</div>';
  }
  var reviewLabel = stack.kind === 'burst' ? 'Review burst' : 'Compare';
  return '<div class="browse-stack-tray" data-stack-cover-id="' + cover.id + '">'
    + '<div class="browse-stack-tray-header"><div>'
    + '<span class="browse-stack-tray-title">' + escapeHtml(label) + '</span>'
    + '<span class="browse-stack-tray-subtitle">' + stack.count + ' photos</span>'
    + '</div><div class="browse-stack-tray-actions">'
    + '<button type="button" onclick="filterToBrowseGroup(' + cover.id + ')">Filter to group</button>'
    + '<button type="button" onclick="selectBrowseStackAll(event,' + cover.id + ')">Select all</button>'
    + '<button type="button" onclick="reviewBrowseStack(event,' + cover.id + ')">' + reviewLabel + '</button>'
    + '<button type="button" aria-label="Collapse stack" title="Collapse" onclick="toggleBrowseStack(event,' + cover.id + ')">&#10005;</button>'
    + '</div></div>' + body + '</div>';
}

function insertBrowseStackTray(coverId) {
  var oldTray = document.querySelector('.browse-stack-tray[data-stack-cover-id="' + coverId + '"]');
  if (oldTray) oldTray.remove();
  if (!expandedBrowseStacks.has(coverId)) return;
  var cover = photos.find(function(photo) { return photo.id === coverId; });
  var card = document.querySelector('.grid-card[data-id="' + coverId + '"]');
  if (!cover || !cover.browse_stack || !card) return;
  card.insertAdjacentHTML('afterend', renderBrowseStackTray(cover));
  var tray = document.querySelector('.browse-stack-tray[data-stack-cover-id="' + coverId + '"]');
  refreshColorLabelControlsIn(tray);
  refreshCardSelectionVisuals();
}

function restoreExpandedBrowseStacks() {
  Array.from(expandedBrowseStacks).forEach(insertBrowseStackTray);
}

// Every mutation path reports the photo ids whose local state it just changed
// (see refreshExpandedBrowseStackMembers / reconcileBrowseStackCovers callers).
// Any stack expansion still in flight fetched those photos before the edit
// committed, so its payload is pre-edit for exactly the members the mutation
// could not reach. Mark it rather than trying to replay the mutation onto the
// response: the set of fields an edit can touch keeps growing, and a refetch
// is correct for all of them without per-field bookkeeping.
function markBrowseStackExpansionsStale(photoIds) {
  var wanted = (photoIds || []).map(Number);
  if (!wanted.length) return;
  Object.keys(browseStackExpansionRequests).forEach(function(cacheKey) {
    var requests = browseStackExpansionRequests[cacheKey] || [];
    requests.forEach(function(request) {
      if (!request || request.stale) return;
      request.stale = wanted.some(function(id) { return request.memberIds.has(id); });
    });
  });
}

// A cover-hydration response captured its members before this mutation ran, so
// on any overlap its payload is pre-edit and must never reach browseStackMembers.
// Mark rather than delete: the hydration exists to recompute a cover after an
// edit that already committed, so dropping it silently would leave the demoted
// cover on screen. The flag makes reconcileBrowseStackCovers discard the
// in-flight response and refetch the post-edit members instead.
// reconcileBrowseStackCovers only supersedes hydrations it starts itself, so
// mutation paths that don't call it (wildlife, keyword, metadata edits) rely on
// this to invalidate a matching in-flight request.
function markBrowseStackHydrationsStale(photoIds) {
  var wanted = (photoIds || []).map(Number);
  if (!wanted.length) return;
  Object.keys(browseStackHydrationRequests).forEach(function(cacheKey) {
    var record = browseStackHydrationRequests[cacheKey];
    if (!record || !record.memberIds || record.stale) return;
    record.stale = wanted.some(function(id) { return record.memberIds.has(id); });
  });
}

function refreshExpandedBrowseStackMembers(photoIds) {
  var wanted = new Set((photoIds || []).map(Number));
  markBrowseStackExpansionsStale(photoIds);
  markBrowseStackHydrationsStale(photoIds);
  Object.keys(browseStackMembers).forEach(function(coverId) {
    var members = browseStackMembers[coverId] || [];
    if (members.some(function(member) { return wanted.has(member.id); })) {
      insertBrowseStackTray(Number(coverId));
    }
  });
}

function browseStackFlagRank(flag) {
  if (flag === 'flagged') return 2;
  if (flag == null || flag === 'none') return 1;
  return 0;
}

function browseStackCoverCompare(a, b) {
  var descending = [
    [browseStackFlagRank(a.flag), browseStackFlagRank(b.flag)],
    [a.quality_score, b.quality_score],
    [a.subject_sharpness, b.subject_sharpness],
    [a.sharpness, b.sharpness],
    [a.rating == null ? 0 : 1, b.rating == null ? 0 : 1],
    [a.rating == null ? 0 : a.rating, b.rating == null ? 0 : b.rating],
    [(a.width || 0) * (a.height || 0), (b.width || 0) * (b.height || 0)],
    [a.file_size || 0, b.file_size || 0],
  ];
  for (var i = 0; i < descending.length; i++) {
    var av = descending[i][0];
    var bv = descending[i][1];
    if (av == null) av = -Infinity;
    if (bv == null) bv = -Infinity;
    if (av !== bv) return bv - av;
  }
  return a.id - b.id;
}

// Bounded only as a safety net, mirroring toggleBrowseStack: a stale retry has
// to be provoked by an edit that already finished its own server round trip, so
// this cannot spin on its own, and a failure retry needs a fresh transport
// error each time. The cap stops either from becoming an unbounded refetch loop.
var MAX_BROWSE_STACK_HYDRATION_ATTEMPTS = 3;

// Loads a collapsed stack's members so reconcileBrowseStackCovers can recompute
// its cover. Returns:
//   'hydrated'   — members are in browseStackMembers, cover can be recomputed
//   'superseded' — a newer request owns this key; it will do the work
//   'window'     — the dataset was replaced; abandon reconciliation entirely
//   'unresolved' — retries exhausted; the grid still shows the pre-edit cover
// 'unresolved' is never silent: the caller flags the stack for recheck and tells
// the user, because the mutation that asked for this reconciliation succeeded.
async function hydrateBrowseStackCoverMembers(cover, windowIsCurrent) {
  var cacheKey = String(cover.id);
  var memberIds = (cover.browse_stack && cover.browse_stack.photo_ids) || [];
  for (var attempt = 1; attempt <= MAX_BROWSE_STACK_HYDRATION_ATTEMPTS; attempt++) {
    var hydrationSeq = ++browseStackHydrationSeq;
    browseStackHydrationRequests[cacheKey] = {
      seq: hydrationSeq,
      stale: false,
      memberIds: new Set(memberIds.map(Number)),
    };
    var hydratedMembers = [];
    try {
      for (var offset = 0; offset < memberIds.length; offset += 500) {
        var data = await safeFetch('/api/photos/by-ids', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({photo_ids: memberIds.slice(offset, offset + 500)}),
        }, {toast: false});
        if (!windowIsCurrent()) return 'window';
        hydratedMembers = hydratedMembers.concat(data.photos || []);
      }
    } catch (e) {
      var errorRecord = browseStackHydrationRequests[cacheKey];
      if (errorRecord && errorRecord.seq === hydrationSeq) {
        delete browseStackHydrationRequests[cacheKey];
      }
      if (!windowIsCurrent()) return 'window';
      // The edit itself already committed on the server. Retry the read rather
      // than abandoning the cover recomputation it asked for. Back off between
      // attempts so a server that is briefly unreachable gets time to recover
      // instead of being hit three times inside a few milliseconds.
      if (attempt < MAX_BROWSE_STACK_HYDRATION_ATTEMPTS) {
        await new Promise(function(resolve) {
          setTimeout(resolve, 200 * Math.pow(2, attempt - 1));
        });
        if (!windowIsCurrent()) return 'window';
        continue;
      }
      return 'unresolved';
    }
    // Overlapping edits may hydrate the same collapsed stack. Request order
    // tracks mutation freshness, so only the newest request may cache even when
    // an older response happens to arrive first.
    var currentRecord = browseStackHydrationRequests[cacheKey];
    if (!currentRecord || currentRecord.seq !== hydrationSeq) return 'superseded';
    if (currentRecord.stale) {
      // A generic member edit (wildlife, keyword, metadata) landed while this
      // response was in flight, so it holds pre-edit rows for members that had
      // no cache entry to patch. Never cache that; refetch the post-edit truth.
      delete browseStackHydrationRequests[cacheKey];
      if (attempt < MAX_BROWSE_STACK_HYDRATION_ATTEMPTS) continue;
      return 'unresolved';
    }
    delete browseStackHydrationRequests[cacheKey];
    // Expansion can also hydrate this key while reconciliation is in flight;
    // and another response may already have promoted a replacement.
    if (browseStackMembers[cacheKey] || !cover.browse_stack
        || !photos.some(function(photo) {
          return photo === cover && photo.id === cover.id;
        })) {
      return 'superseded';
    }
    var stack = cover.browse_stack;
    var similarity = cover.similarity;
    var clientSimilarity = cover._similarity;
    browseStackMembers[cacheKey] = hydratedMembers.map(function(member) {
      if (member.id !== cover.id) return member;
      Object.assign(cover, member);
      cover.browse_stack = stack;
      if (similarity !== undefined) cover.similarity = similarity;
      if (clientSimilarity !== undefined) cover._similarity = clientSimilarity;
      return cover;
    });
    return 'hydrated';
  }
  return 'unresolved';
}

async function reconcileBrowseStackCovers(photoIds) {
  var wanted = new Set((photoIds || []).map(Number));
  // Callers run this straight after applying the edit locally, before the
  // trailing refreshExpandedBrowseStackMembers(). Marking here too closes the
  // window in which this function's own awaits would let a stale expansion
  // response land unflagged.
  markBrowseStackExpansionsStale(photoIds);
  var windowIsCurrent = observeBrowseWindow();
  var focusedReplacementId = null;
  var refreshFocusedDetail = false;
  var uncachedCovers = photos.filter(function(photo) {
    if (!photo.browse_stack || browseStackMembers[String(photo.id)]) return false;
    return wanted.has(photo.id) || (photo.browse_stack.photo_ids || []).some(function(id) {
      return wanted.has(id);
    });
  });
  var unresolvedCovers = [];
  for (var h = 0; h < uncachedCovers.length; h++) {
    var status = await hydrateBrowseStackCoverMembers(uncachedCovers[h], windowIsCurrent);
    // The dataset under us was replaced; whoever owns the new window will
    // paint authoritative covers, so there is nothing stale to report.
    if (status === 'window') return false;
    if (status === 'unresolved') unresolvedCovers.push(uncachedCovers[h].id);
    // Members are loaded, so the cover below is recomputed from the full stack:
    // any earlier recheck marker on this cover is now satisfied.
    if (status === 'hydrated') browseStackCoverRecheck.delete(uncachedCovers[h].id);
  }
  unresolvedCovers.forEach(function(coverId) { browseStackCoverRecheck.add(coverId); });

  var changed = false;
  Object.keys(browseStackMembers).forEach(function(oldKey) {
    var members = browseStackMembers[oldKey] || [];
    if (!members.some(function(member) { return wanted.has(member.id); })) return;
    var oldCoverId = Number(oldKey);
    var oldIndex = photos.findIndex(function(photo) { return photo.id === oldCoverId; });
    if (oldIndex < 0 || !members.length) return;
    var newCover = members.slice().sort(browseStackCoverCompare)[0];
    if (!newCover || newCover.id === oldCoverId) return;

    var oldCover = photos[oldIndex];
    var stack = oldCover.browse_stack;
    oldCover.browse_stack = null;
    newCover.browse_stack = stack;
    if (oldCover.similarity !== undefined) newCover.similarity = oldCover.similarity;
    if (oldCover._similarity !== undefined) newCover._similarity = oldCover._similarity;
    photos[oldIndex] = newCover;

    var newKey = String(newCover.id);
    browseStackMembers[newKey] = members;
    delete browseStackMembers[oldKey];
    if (Object.prototype.hasOwnProperty.call(browseStackErrors, oldKey)) {
      browseStackErrors[newKey] = browseStackErrors[oldKey];
      delete browseStackErrors[oldKey];
    }
    // The cover was just recomputed from the whole member list, so a recheck
    // marker left over from an earlier failed hydration no longer applies.
    browseStackCoverRecheck.delete(oldCoverId);
    if (expandedBrowseStacks.delete(oldCoverId)) {
      expandedBrowseStacks.add(newCover.id);
    }
    if (selectedPhotoId != null && members.some(function(member) {
      return member.id === selectedPhotoId;
    })) {
      selectedIndex = oldIndex;
      // The demoted cover is gone from the top-level photos array. Grid
      // selection and preview navigation both look it up there and would
      // otherwise land on an unselected card with an index of -1 in the
      // navigation list. Hand focus to the newly promoted cover, which now
      // occupies that grid slot.
      if (selectedPhotoId === oldCoverId && selectedPhotos.size === 0) {
        selectedPhotoId = newCover.id;
        focusedReplacementId = newCover.id;
        refreshFocusedDetail = document.getElementById('detailContent').classList.contains('visible');
      }
    }
    changed = true;
  });
  if (changed) renderGrid();
  if (unresolvedCovers.length) {
    // The edit committed on the server but its cover recomputation did not, so
    // the grid is knowingly showing a photo the stack may no longer lead with.
    // Say so instead of leaving a silently wrong representative on screen, and
    // mark the stacks so the notice survives the toast.
    unresolvedCovers.forEach(refreshBrowseStackBadge);
    var one = unresolvedCovers.length === 1;
    showToast(
      'Could not reload ' + (one ? 'a stack' : unresolvedCovers.length + ' stacks')
      + ' after that edit — ' + (one ? 'its cover' : 'their covers')
      + ' may still show the previous top photo. Expand '
      + (one ? 'the stack' : 'a stack') + ' marked “!” to refresh it.',
      'warning'
    );
  }
  // Keep the inspector bound to the same visible grid focus. Call this after
  // renderGrid() so the promoted cover and its tray are already in place when
  // the async detail response paints.
  if (focusedReplacementId != null && refreshFocusedDetail) {
    loadDetail(focusedReplacementId);
  }
  return changed;
}

function browsePhotoNavigationList(photoId) {
  // Prefer an expanded stack's member list even for its cover, which also
  // exists in the top-level grid. Opening that cover from the tray should
  // navigate to its neighboring frames, not to the next stack cover.
  var stackIds = Object.keys(browseStackMembers);
  for (var i = 0; i < stackIds.length; i++) {
    if (!expandedBrowseStacks.has(Number(stackIds[i]))) continue;
    var members = browseStackMembers[stackIds[i]] || [];
    if (members.some(function(photo) { return photo.id === photoId; })) {
      return members;
    }
  }
  // No stack owns this photo, so navigation falls back to the top-level
  // grid list. Offline placeholders are not viewable, so the lightbox gets
  // the available-only projection of it (see ``availableBrowsePhotos``).
  return availableBrowsePhotos();
}

// True for either shape ``browsePhotoNavigationList`` returns as its
// top-level fallback: ``photos`` itself, or the available-only list used
// while offline collection members are shown.
function browseNavigationListIsTopLevel(list) {
  return list === photos || list === browseAvailableLightboxPhotos;
}

function browseStackCoverIdForPhoto(photoId) {
  var stackIds = Object.keys(browseStackMembers);
  for (var i = 0; i < stackIds.length; i++) {
    if ((browseStackMembers[stackIds[i]] || []).some(function(photo) {
      return photo.id === photoId;
    })) {
      return Number(stackIds[i]);
    }
  }
  return null;
}

function loadedBrowsePhotoIds() {
  var ids = new Set(photos.map(function(photo) { return photo.id; }));
  Object.keys(browseStackMembers).forEach(function(coverId) {
    (browseStackMembers[coverId] || []).forEach(function(photo) {
      ids.add(photo.id);
    });
  });
  return Array.from(ids);
}

async function toggleBrowseStack(event, coverId) {
  if (event) {
    event.preventDefault();
    event.stopPropagation();
  }
  var cover = photos.find(function(photo) { return photo.id === coverId; });
  if (!cover || !cover.browse_stack) return;
  var badge = document.querySelector('.grid-card[data-id="' + coverId + '"] .browse-stack-badge');
  if (expandedBrowseStacks.has(coverId)) {
    var members = browseStackMembers[String(coverId)] || [];
    var selectedHiddenMember = selectedPhotoId !== coverId && members.some(function(member) {
      return member.id === selectedPhotoId;
    });
    // A stack-wide Select all clears selectedPhotoId but leaves every member
    // ID in selectedPhotos. Once the tray collapses, hidden members are gone
    // from the navigation list, so a preview shortcut would fall through to
    // the first hidden member and yield a 0/N lightbox. Detect that case so
    // we can re-pin focus to the visible cover after removing the tray.
    var batchHasHiddenMembers = selectedPhotoId == null && selectedPhotos.size > 0
      && members.some(function(member) {
        return member.id !== coverId && selectedPhotos.has(member.id);
      });
    expandedBrowseStacks.delete(coverId);
    var tray = document.querySelector('.browse-stack-tray[data-stack-cover-id="' + coverId + '"]');
    if (tray) tray.remove();
    if (badge) badge.setAttribute('aria-expanded', 'false');
    // A collapsed tray cannot keep a single-photo focus on one of its hidden
    // members: grid/lightbox navigation now uses the top-level cover list.
    // Hand that focus back to the visible cover while retaining the warm
    // member cache for a cheap re-expand and for batch state reconciliation.
    if (selectedHiddenMember) {
      var coverIndex = photos.findIndex(function(photo) { return photo.id === coverId; });
      if (selectedPhotos.size > 0) {
        // Keep the exact multi-selection intact. The focused hidden member is
        // still an active batch target, and getBrowseShortcutPhoto maps that
        // focus to the collapsed cover solely for preview/navigation.
        selectedIndex = coverIndex;
        refreshCardSelectionVisuals();
      } else {
        // stackAware: false — collapsing a tray is a view action. It hands
        // the focus back to the visible cover; it does not turn one frame
        // the user was looking at into a stack-wide selection. The cover
        // card paints the partial mark for exactly this state.
        selectPhoto({shiftKey: false, metaKey: false, ctrlKey: false},
                    coverId, coverIndex, { stackAware: false });
      }
    } else if (batchHasHiddenMembers) {
      // Preserve the batch (Select all) selection but pin single-photo focus
      // to the visible cover so preview shortcuts, keyboard navigation, and
      // the grid caret all resolve inside the top-level cover list.
      selectedPhotoId = coverId;
      selectedIndex = photos.findIndex(function(photo) { return photo.id === coverId; });
      refreshCardSelectionVisuals();
    }
    return;
  }

  expandedBrowseStacks.add(coverId);
  if (badge) badge.setAttribute('aria-expanded', 'true');
  insertBrowseStackTray(coverId);
  var cacheKey = String(coverId);
  if (browseStackMembers[cacheKey]) return;
  // Errors are presentation state, not a durable cache result. Re-expanding
  // after collapse retries a transient failure instead of pinning the stack
  // to its first failed request until the whole Browse dataset reloads.
  delete browseStackErrors[cacheKey];
  insertBrowseStackTray(coverId);
  var memberIds = cover.browse_stack.photo_ids || [];
  var windowIsCurrent = observeBrowseWindow();
  function coverStillOwnsSlot() {
    // A cover-changing edit can hydrate and promote this stack while the
    // expansion request is in flight. The dataset epoch stays current in
    // that case, so also verify the captured cover still owns this grid slot
    // before writing its old cache key.
    return windowIsCurrent() && !!cover.browse_stack && photos.some(function(photo) {
      return photo === cover && photo.id === coverId;
    });
  }
  // Bounded only as a safety net: every retry has to be provoked by an edit
  // that already completed its own server round trip, so this cannot spin on
  // its own. The cap stops a future marking bug from becoming a refetch loop.
  var MAX_EXPANSION_ATTEMPTS = 4;
  for (var attempt = 1; attempt <= MAX_EXPANSION_ATTEMPTS; attempt++) {
    var request = {memberIds: new Set(memberIds.map(Number)), stale: false};
    _pushBrowseStackExpansionRequest(cacheKey, request);
    var hydrated = [];
    var abortedForRace = false;
    try {
      // /api/photos/by-ids caps each POST at 500 ids. Cover reconciliation
      // already chunks in 500-id slices for the same reason; do the same
      // here so an exact-duplicate or burst stack with more than 500 members
      // is still expandable instead of surfacing a permanent "too large"
      // error on its badge.
      for (var offset = 0; offset < memberIds.length; offset += 500) {
        var data = await safeFetch('/api/photos/by-ids', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({photo_ids: memberIds.slice(offset, offset + 500)}),
        });
        if (!coverStillOwnsSlot()) {
          // The captured cover no longer owns this grid slot (a promotion
          // landed between chunks). Drop the request bookkeeping and let
          // whoever hydrates the new cover own the tray from here.
          _removeBrowseStackExpansionRequest(cacheKey, request);
          abortedForRace = true;
          break;
        }
        if (request.stale || browseStackMembers[cacheKey]) {
          // An edit or a fresher hydration landed mid-chunk. Stop fetching
          // the rest of the stack and let the outer loop's stale / cache
          // branches decide what to do; the partial payload is worthless
          // for painting either way.
          break;
        }
        hydrated = hydrated.concat(data.photos || []);
      }
    } catch (error) {
      _removeBrowseStackExpansionRequest(cacheKey, request);
      if (!coverStillOwnsSlot()) return;
      browseStackErrors[cacheKey] = 'Could not load this stack.';
      insertBrowseStackTray(coverId);
      return;
    }
    if (abortedForRace) return;
    _removeBrowseStackExpansionRequest(cacheKey, request);
    if (!coverStillOwnsSlot()) return;
    if (browseStackMembers[cacheKey]) {
      // Cover reconciliation hydrated this key while we were in flight. It
      // issued its fetch after the edit that triggered it, so its cache is
      // strictly fresher than this response — keep it and just finish the
      // metadata passes below.
      break;
    }
    if (request.stale) {
      // An edit landed while this response was in flight, and its hidden
      // members had no cache entry to be patched in — so this payload holds
      // pre-edit values for them. Never paint that: the tray keeps saying
      // "Loading stack…" while we go fetch the post-edit truth.
      if (attempt < MAX_EXPANSION_ATTEMPTS) continue;
      browseStackErrors[cacheKey] =
        'This stack kept changing while it loaded. Collapse and expand it to see current state.';
      insertBrowseStackTray(coverId);
      return;
    }
    // Reuse the top-level cover object inside the member cache. The cover is
    // rendered in both places; sharing one object keeps rating/flag/keyword/
    // wildlife/location mutations from updating the grid copy while leaving
    // a stale duplicate in the expanded tray.
    browseStackMembers[cacheKey] = hydrated.map(function(photo) {
      return photo.id === coverId ? cover : photo;
    });
    break;
  }
  insertBrowseStackTray(coverId);
  var loadedIds = (browseStackMembers[cacheKey] || []).map(function(photo) {
    return photo.id;
  });
  if (!loadedIds.length) return;
  if (browseStackCoverRecheck.delete(coverId)) {
    // An earlier edit's cover reconciliation could not load these members, so
    // the grid has been showing a possibly-demoted cover ever since. The
    // members are cached now, so recompute the cover from them — expanding is
    // the recovery the badge marker promised. Members are cached, so this
    // reconciliation issues no requests and cannot fail.
    await reconcileBrowseStackCovers(loadedIds);
    // Promotion moves the cache under a new key and re-renders the grid.
    var recheckedCoverId = browseStackCoverIdForPhoto(coverId);
    if (recheckedCoverId != null) insertBrowseStackTray(recheckedCoverId);
  }
  function refreshCurrentStackTray() {
    if (!windowIsCurrent()) return;
    // Patch the members where they stand. A rating/flag edit can promote
    // another member while either metadata request is in flight, moving these
    // photos into a different cover's tray; addressing them by photo id finds
    // them wherever they ended up, and finds nothing once the tray collapses.
    refreshStackMemberCards(loadedIds);
  }
  loadInatStatus(loadedIds).then(refreshCurrentStackTray);
  fetchColorLabels(loadedIds).then(refreshCurrentStackTray);
}

function selectBrowseStackMember(event, photoId, coverId) {
  if (event) event.stopPropagation();
  var coverIndex = photos.findIndex(function(photo) { return photo.id === coverId; });
  // Pass shiftKey through so a top-level anchor + Shift-click on a hidden
  // member range-selects instead of dropping the modifier (symmetric with
  // the reverse direction: member anchor + Shift-click on a top-level
  // card). selectPhoto's shift branch folds the click target into the
  // resulting range so the hidden member always ends up in the selection.
  // Codex P2 on PR #1561.
  // stackAware: false — a tray click is the way to pick one frame out of a
  // stack, including the cover frame, so it must never expand back to the
  // whole stack the way clicking the collapsed card does.
  selectPhoto({
    shiftKey: !!(event && event.shiftKey),
    metaKey: !!(event && event.metaKey),
    ctrlKey: !!(event && event.ctrlKey),
  }, photoId, coverIndex, { stackAware: false });
}

function selectBrowseStackAll(event, coverId) {
  if (event) {
    event.preventDefault();
    event.stopPropagation();
  }
  var cover = photos.find(function(photo) { return photo.id === coverId; });
  if (!cover || !cover.browse_stack) return;
  anchorRestoreEpoch++;
  selectedPhotoId = null;
  selectedIndex = photos.findIndex(function(photo) { return photo.id === coverId; });
  // Same list, in the same cover-first order, that clicking the collapsed
  // card produces — the tray button and the card must not disagree.
  selectedPhotos = new Set(
    browseStackMemberIds(coverId) || cover.browse_stack.photo_ids || []
  );
  abandonDetailFocusForBatch();
  refreshCardSelectionVisuals();
  updateBatchBar();
}

function reviewBrowseStack(event, coverId) {
  selectBrowseStackAll(event, coverId);
  var cover = photos.find(function(photo) { return photo.id === coverId; });
  if (cover && cover.browse_stack && cover.browse_stack.kind === 'duplicate') {
    openBrowseCompare();
  } else {
    openSelectedInBurstReview();
  }
}

function openBrowseStackMember(event, photoId, coverId) {
  if (event) {
    event.preventDefault();
    event.stopPropagation();
  }
  var members = browseStackMembers[String(coverId)] || [];
  var photo = members.find(function(member) { return member.id === photoId; });
  openLightbox(photoId, photo ? photo.filename : '', members);
}

document.addEventListener('lightbox:renderchanged', function(e) {
  var ids = e && e.detail && Array.isArray(e.detail.photoIds) ? e.detail.photoIds : [];
  ids.forEach(function(id) {
    var hideDetectionOverlays = (
      (
        _vireoPairKnownByPhoto[String(id)] &&
        _vireoPairSource(id) === 'jpeg'
      ) ||
      (
        typeof window.vireoPhotoHasOrientationEdit === 'function' &&
        window.vireoPhotoHasOrientationEdit(id)
      )
    );
    document.querySelectorAll(
      '.grid-card[data-id="' + id + '"] .det-box, ' +
      '.browse-stack-member[data-id="' + id + '"] .det-box'
    ).forEach(function(box) {
      box.style.display = hideDetectionOverlays ? 'none' : '';
    });
  });
});
