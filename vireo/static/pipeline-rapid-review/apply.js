// Applying a burst: the flags-only /group/apply request, the species
// requests to /api/encounters/species, the cache save, and the next burst.
// Classic page script; load boot.js after all definitions.

function candidateIdsForApply() {
  return rapid.items.filter(function(p) {
    var initial = rapid.initialFlags[p.id] || 'none';
    return zoneFor(p.id) === 'review' &&
      (rapid.touched.has(p.id) || initial === 'flagged' || initial === 'rejected');
  }).map(function(p) { return p.id; });
}

function updateSummaryFromPhotos() {
  var summary = rapid.results.summary || {};
  summary.keep_count = 0;
  summary.review_count = 0;
  summary.reject_count = 0;
  (rapid.results.photos || []).forEach(function(p) {
    if (p.label === 'KEEP') summary.keep_count++;
    else if (p.label === 'REJECT') summary.reject_count++;
    else summary.review_count++;
  });
  rapid.results.summary = summary;
}

async function applyCurrent(openNext) {
  if (!rapid.seeded || rapid.applying || rapid.sessionIndex < 0) return false;
  rapid.applying = true;
  renderApplyState();
  var targetList = parseSpeciesList(document.getElementById('speciesInput').value);
  var species = joinSpecies(targetList);
  var picks = Array.from(rapid.picks);
  var rejects = Array.from(rapid.rejects);
  var candidates = candidateIdsForApply();
  // Snapshot session context up front. The sidebar still allows openSession()
  // during the request, so reading rapid.sessionIndex / rapid.items /
  // rapid.picks / rapid.rejects after the await could splice burst A's
  // response into burst B's labels, confirmed species/subtitle, and cache.
  var sessionIndex = rapid.sessionIndex;
  var session = rapid.sessions[sessionIndex];
  var items = rapid.items.slice();
  var pickSet = new Set(picks);
  var rejectSet = new Set(rejects);
  // Capture the species THIS burst is ALREADY confirmed as, BEFORE any
  // optimistic local mutation below. Mirrors the burst-group modal's
  // initialConfirmedSpecies (pipeline_review.html openGroupReview): prefer a
  // confirmed burst override, else the encounter's confirmed species, else ''
  // (unconfirmed). The species POST is gated on an ACTUAL change against this
  // baseline — applying cull-only changes to a burst already tagged as X must
  // NOT re-post X, because /api/encounters/species records a keyword_add
  // edit-history entry per submitted frame even when tag_photo is an
  // INSERT-OR-IGNORE no-op, and undoing that no-op add would strip the
  // existing species keywords. Snapshot off rapid.results (not live rapid.*)
  // since openSession() can run during the awaits below.
  var alreadyConfirmedList = [];
  var snapEnc = (rapid.results.encounters || [])[session.encIdx];
  if (snapEnc) {
    var snapBursts = (snapEnc.bursts && snapEnc.bursts.length) ? snapEnc.bursts : null;
    var snapBurst = snapBursts ? snapBursts[session.burstIdx] : null;
    var snapOvr = snapBurst && snapBurst.species_override;
    if (snapOvr && Array.isArray(snapOvr.species_list)) {
      // Authoritative whatever the confirmed flag (see overrideIsExplicit):
      // a confirmed list, a mixed burst's edited baseline, or the empty
      // sentinel after its last species was removed.
      alreadyConfirmedList = overrideSpeciesList(snapOvr);
    } else if (snapOvr && snapOvr.confirmed === true && snapOvr.species) {
      alreadyConfirmedList = [snapOvr.species];
    } else {
      // The encounter's list regardless of species_confirmed: a mixed
      // encounter (one frame [A, B], another [A]) still records its
      // dominant prior species as the list, and that is exactly what the
      // server treats as the burst's current set (burst_species_list). An
      // empty baseline here would make a flag-only apply re-post A and
      // then overwrite every cached frame with [A], erasing B.
      alreadyConfirmedList = encounterConfirmedList(snapEnc);
    }
  }
  // Gate the species POST on ACTUAL species/tag work, mirroring
  // rapidComputeApplyDiff(): post when the confirmed species changes
  // (speciesChanged) OR when one or more current-burst frames are missing the
  // species keyword (tagNeeded). The latter covers a burst already confirmed as
  // X that still has untagged frames (e.g. legacy data that only tagged picks):
  // the Apply button advertises "Tag N", so the POST must run to write those
  // keywords — /api/pipeline/group/apply above is flags-only and never tags.
  // Skipping the POST when there is NO species change AND nothing left to tag
  // avoids the destructive no-op keyword_add (an undoable add that, when undone,
  // strips the existing species keywords). Per-photo keyword state is reliably
  // seeded client-side (rapid.initialSpeciesKeywords / keywordStateSpecies; see
  // openSession + the keyword-state refresh), so the tagNeeded signal is real,
  // not invented.
  //
  // The field edits the burst's species SET. Turn the set difference into
  // /api/encounters/species requests:
  //   * a dropped species paired with a new one → replace (previous_species
  //     names which entry to swap, so a two-species burst keeps its other
  //     subject);
  //   * the first species on an unconfirmed burst → plain confirm, so the
  //     server still replaces any dominant prior tag it finds in the cache;
  //   * any further new species → add (never untags, never detaches);
  //   * a dropped species with no replacement → remove;
  //   * a kept species some frames still lack → add (re-tag only).
  // Order matters for auto-detach, which only a replace can trigger and
  // which the server judges against the burst's list at that moment:
  // unpaired removes go first so the replace sees the shrunken set (turning
  // [A, B] into [C] must detach: remove B, then replace A→C leaves nothing
  // shared with the encounter), then replacements, then adds (which can
  // only widen overlap and never detach).
  var setDiff = rapidSpeciesSetDiff(targetList, alreadyConfirmedList);
  var speciesRequests = [];
  var pairCount = Math.min(setDiff.added.length, setDiff.removed.length);
  for (var rmi = pairCount; rmi < setDiff.removed.length; rmi++) {
    speciesRequests.push({ species: setDiff.removed[rmi], remove: true });
  }
  for (var pi = 0; pi < pairCount; pi++) {
    speciesRequests.push({ species: setDiff.added[pi], previous_species: setDiff.removed[pi] });
  }
  for (var ai = pairCount; ai < setDiff.added.length; ai++) {
    if (!alreadyConfirmedList.length && ai === 0) {
      speciesRequests.push({ species: setDiff.added[ai] });
    } else {
      speciesRequests.push({ species: setDiff.added[ai], add: true });
    }
  }
  setDiff.kept.forEach(function(name) {
    var tagNeeded = items.some(function(p) { return !rapidFrameHasKeyword(p.id, name, targetList); });
    if (!tagNeeded) return;
    // A lone kept species on a single-species burst is the legacy re-tag
    // path (plain confirm of the same species); with two species on the
    // burst a plain confirm would be read as replacing the primary.
    if (alreadyConfirmedList.length === 1 && targetList.length === 1) {
      speciesRequests.push({ species: name });
    } else {
      speciesRequests.push({ species: name, add: true });
    }
  });
  var speciesPostNeeded = speciesRequests.length > 0;

  try {
    var resp = await safeFetch('/api/pipeline/group/apply', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        picks: picks,
        rejects: rejects,
        candidates: candidates,
      }),
    });
    items.forEach(function(p) {
      if (pickSet.has(p.id)) p.label = 'KEEP';
      else if (rejectSet.has(p.id)) {
        p.label = 'REJECT';
      } else {
        p.label = 'REVIEW';
      }
      if (pickSet.has(p.id)) p.flag = 'flagged';
      else if (rejectSet.has(p.id)) p.flag = 'rejected';
      else p.flag = 'none';
    });
    if (resp && resp.photos) {
      Object.keys(resp.photos).forEach(function(pidStr) {
        var pid = parseInt(pidStr, 10);
        var p = rapid.photoMap[pid];
        if (p) {
          p.flag = resp.photos[pidStr].flag;
          // /group/apply is flags-only, so has_species_keyword is always
          // false here in production; confirmed species comes from the
          // adopted /api/encounters/species response below. Kept only because
          // some rapid-review tests still mock the old response shape.
          if (species && resp.photos[pidStr].has_species_keyword) {
            // Merge, never overwrite: a mixed frame may carry a species the
            // burst as a whole does not, and a flag-only apply never touched
            // it. Only species this apply drops from the burst's set go.
            var mirrorKeys = {};
            targetList.forEach(function(name) { mirrorKeys[speciesKey(name)] = true; });
            setDiff.removed.forEach(function(name) { mirrorKeys[speciesKey(name)] = true; });
            var mirrorExtras = photoSpeciesList(p).filter(function(name) {
              return !mirrorKeys[speciesKey(name)];
            });
            var mirrored = targetList.concat(mirrorExtras);
            p.confirmed_species_list = mirrored.slice();
            p.confirmed_species = mirrored[0] || null;
          }
        }
      });
    }
    // /group/apply is flags-only now; persist the species through the same
    // unified endpoint the grid uses so confirmation actually lands server-side
    // (tags all burst frames, handles species replacement + auto-detach).
    // The server now OWNS confirmation: it sets species_confirmed on the
    // correct encounter and returns the authoritative structure via
    // spResp.encounters. We do not write enc.confirmed_species/species_confirmed
    // locally — after an auto-detach the encounter indices shift, so the cached
    // session.encIdx may point at a different (or out-of-bounds) encounter, and
    // a local write would corrupt it and then be persisted by save-cache below.
    if (speciesPostNeeded) {
      var burstPhotoIds = items.map(function(p) { return p.id; });
      var burstIndex = session.burstIdx;
      // The server normalizes spellings (Say’s Phoebe → Say's Phoebe) and
      // dedupes by keyword identity; each response carries the authoritative
      // list, and the last one is what the burst actually holds.
      var confirmedSpeciesList = null;
      // Each request is its own server transaction (keywords + cache), so a
      // failure part-way leaves the earlier changes applied. Rather than
      // pretend atomicity, stop at the first failure, keep the server's
      // truth for what did land, and say exactly which changes did not.
      var speciesFailure = null;
      var appliedRequests = [];
      for (var ri = 0; ri < speciesRequests.length; ri++) {
      var spResp;
      try {
        spResp = await safeFetch('/api/encounters/species', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify(Object.assign({
            photo_ids: burstPhotoIds,
            burst_index: burstIndex,
          }, speciesRequests[ri])),
        });
      } catch (err) {
        speciesFailure = { index: ri, error: err };
        break;
      }
      appliedRequests.push(speciesRequests[ri]);
      // Adopt the authoritative structure (the endpoint may auto-detach on a
      // species mismatch), so the save-cache below doesn't clobber its restructure.
      if (spResp && Array.isArray(spResp.species_list)) {
        confirmedSpeciesList = spResp.species_list.filter(Boolean);
      }
      if (spResp && spResp.encounters) {
        rapid.results.encounters = spResp.encounters;
        if (spResp.summary) rapid.results.summary = spResp.summary;
        // A detach moves the burst; the next request must follow it.
        var located = locateBurstByPhotoIds(spResp.encounters, burstPhotoIds);
        if (located) burstIndex = located.burstIdx;
        // Rebuild the sidebar sessions from the server's authoritative
        // encounters rather than trying to patch the old layout in place.
        // buildSessions() derives each session's encIdx/burstIdx, species
        // (via burstSpecies, which reads enc.confirmed_species /
        // burst.species_override) and hasBurstOverride straight from the
        // adopted payload, so this single rebuild covers BOTH cases the old
        // count heuristic tried to distinguish:
        //   - no detach: the server set confirmed_species on the encounter, so
        //     sibling bursts pick up the just-confirmed species (the Apply-Next
        //     pre-fill hint) for free.
        //   - auto-detach (count-changing OR count-equal merge into an adjacent
        //     same-species encounter): the shifted encIdx/burstIdx and divergent
        //     species are reflected exactly, so the sidebar and openClassic()
        //     navigate server truth instead of stale indices.
        rapid.allSessions = buildSessions(rapid.results);
      }
      }
      // Only now that the species calls have SUCCEEDED do we mirror the
      // confirmation onto the local photos. The endpoint tags EVERY frame in
      // `items` (picks and non-picks alike), so mark them all — marking only
      // picks would leave non-pick frames in the "Needs species" queue / without
      // a confirmed-species chip until a reload. Doing this before the call
      // (or in the label loop above) would leave the cache claiming confirmation
      // even when /api/encounters/species fails, hiding the burst from "Needs
      // species" locally until reload (the catch path doesn't roll it back).
      // Persist the server-normalized list (not the raw field text) so the
      // cache never claims a spelling or a duplicate the database lacks.
      // Merge rather than overwrite: a frame may carry a species the burst as
      // a whole does not (a mixed burst), and the server never touched it —
      // only species this apply removed or replaced away are dropped.
      var touchedKeys = {};
      appliedRequests.forEach(function(req) {
        if (req.remove) touchedKeys[speciesKey(req.species)] = true;
        if (req.previous_species) touchedKeys[speciesKey(req.previous_species)] = true;
      });
      if (appliedRequests.length) {
        var serverList = confirmedSpeciesList || (speciesFailure ? null : targetList);
        items.forEach(function(p) {
          if (!serverList) return;
          var serverKeys = {};
          serverList.forEach(function(name) { serverKeys[speciesKey(name)] = true; });
          var extras = photoSpeciesList(p).filter(function(name) {
            var key = speciesKey(name);
            return !serverKeys[key] && !touchedKeys[key];
          });
          var merged = serverList.concat(extras);
          p.confirmed_species_list = merged.slice();
          p.confirmed_species = merged[0] || null;
        });
      }
      if (speciesFailure) {
        var failedReq = speciesRequests[speciesFailure.index];
        var failedWhat = failedReq.remove
          ? 'removing "' + failedReq.species + '"'
          : failedReq.previous_species
            ? 'replacing "' + failedReq.previous_species + '" with "' + failedReq.species + '"'
            : 'adding "' + failedReq.species + '"';
        var remaining = speciesRequests.length - speciesFailure.index;
        var applied = appliedRequests.length;
        var msg = 'Species change failed while ' + failedWhat + '. '
          + (applied ? applied + ' earlier ' + rapidPlural(applied, 'change') + ' stayed applied; ' : '')
          + remaining + ' ' + rapidPlural(remaining, 'change') + ' not applied. The field now shows what the server holds.';
        if (typeof showToast === 'function') showToast(msg, 'error');
      }
    }
    updateSummaryFromPhotos();
    await safeFetch('/api/pipeline/save-cache', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(rapid.results),
    }, { toast: false });
    rapid.applying = false;
    var currentKey = sessionKey(session);
    var activeKey = sessionKey(rapid.sessions[rapid.sessionIndex]);
    rebuildFilteredSessions();
    if (openNext) {
      var nextIdx = findNextFilteredIndexAfterRaw(session.rawIndex);
      if (nextIdx < 0) nextIdx = findNearestFilteredIndex(session.rawIndex);
      if (nextIdx >= 0) openSession(nextIdx);
      else showEmptyQueue();
    } else if (rapid.sessionIndex === sessionIndex) {
      var refreshedIdx = findSessionIndexByKey(currentKey);
      if (refreshedIdx < 0) refreshedIdx = findNearestFilteredIndex(session.rawIndex);
      if (refreshedIdx >= 0) openSession(refreshedIdx);
      else showEmptyQueue();
    } else {
      // User navigated to a different burst during apply; don't disrupt their
      // view. Rebase the old filtered index onto the rebuilt queue first:
      // the applied burst may have dropped out and shifted every later index.
      var activeIdx = findSessionIndexByKey(activeKey);
      if (activeIdx >= 0) {
        rapid.sessionIndex = activeIdx;
        renderAll();
      } else {
        activeIdx = findNearestFilteredIndex(session.rawIndex);
        if (activeIdx >= 0) openSession(activeIdx);
        else showEmptyQueue();
      }
    }
    return true;
  } catch (e) {
    rapid.applying = false;
    renderApplyState();
    return false;
  }
}

function openClassic() {
  var session = rapid.sessions[rapid.sessionIndex];
  if (!session) {
    window.location.href = '/pipeline/review';
    return;
  }
  window.location.href = '/pipeline/review?enc=' + session.encIdx + '&burst=' + session.burstIdx;
}
