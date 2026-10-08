// Group Review staged decisions, zone moves, keyboard actions, and apply.
// Classic page script; shared globals are initialized before boot.js runs.

// Debounce species-keyword refetches so we don't hit /group/state on every
// keystroke. The Apply label still updates synchronously; only the keyword
// portion of dbState is async.
var _grmSpeciesRefetchTimer = null;
var _grmSpeciesRefetchToken = 0;

// Called from the species <input>'s oninput. Refresh the Apply label and
// visible keyword summary immediately (the diff uses the typed value), and
// schedule a debounced refetch of has_species_keyword so missing-keyword
// markers + Tag-N preview reflect the *currently typed* species, not the one
// the modal opened with.
function grmOnSpeciesInput() {
  if (grmState) grmState.speciesFieldTouched = true;
  renderGroupModal();
  grmUpdateApplyLabel();
  if (_grmSpeciesRefetchTimer) clearTimeout(_grmSpeciesRefetchTimer);
  _grmSpeciesRefetchTimer = setTimeout(grmRefreshSpeciesKeywordState, 250);
}

function grmRefreshSpeciesKeywordState() {
  if (!grmState || !grmState.items || grmState.items.length === 0) return;
  var speciesEl = document.getElementById('grmSpecies');
  var species = speciesEl ? (speciesEl.value || '').trim() : '';
  var sessionId = grmState.sessionId;
  var token = ++_grmSpeciesRefetchToken;
  var photoIds = grmState.items.map(function(p) { return p.id; });

  safeFetch('/api/pipeline/group/state', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ photo_ids: photoIds, species: species }),
  }, { toast: false }).then(function(data) {
    // Drop the response if the modal moved on (different burst opened, or
    // the user kept typing and a newer fetch is in flight).
    if (grmState.sessionId !== sessionId) return;
    if (token !== _grmSpeciesRefetchToken) return;
    var fresh = (data && data.photos) || {};
    // Only update DB-owned fields — the flag field is owned by the user's
    // in-modal moves at this point and would be wrong to overwrite from DB.
    // The representative field is species-scoped, so it has to move with
    // the typed species or the badge would show the previous species'
    // representative state on the newly typed one.
    Object.keys(fresh).forEach(function(pidStr) {
      var pid = parseInt(pidStr, 10);
      if (!grmState.dbState[pid]) grmState.dbState[pid] = { flag: 'none' };
      grmState.dbState[pid].has_species_keyword = !!fresh[pidStr].has_species_keyword;
      grmState.dbState[pid].is_species_representative = !!fresh[pidStr].is_species_representative;
    });
    grmState.keywordStateSpecies = species;
    renderGroupModal();
  }).catch(function() {
    // Network/server error: leave previous keyword markers in place. The
    // Apply label is still correct as a worst-case overestimate of tags.
  });
}

// User manually toggled a checkbox: record the override for ONLY the box that
// fired, then re-sync labels. Recording both would pin the untouched box's
// override and break its smart default (null = follow the dirty heuristic).
function grmOnToggleChange(which) {
  if (which === 'species') {
    var spChk = document.getElementById('grmConfirmSpeciesChk');
    if (spChk) grmState.confirmSpeciesOverride = spChk.checked;
  } else if (which === 'flags') {
    var flChk = document.getElementById('grmApplyFlagsChk');
    if (flChk) grmState.applyFlagsOverride = flChk.checked;
  }
  grmUpdateApplyLabel();
  // The header summary is keyed off the effective post-apply member set,
  // which shifts with the flags override: unchecking it re-includes
  // "removed" frames in the count. Refresh so `N/M applied` stays truthful.
  grmUpdateKeywordSummary(grmKeywordStats(_grmKeywordSummaryMembers()));
}

// Smart default: is there a real pending change for each side?
function grmFlagsDirty(diff) {
  return (diff.flagNew + diff.rejectNew + diff.clearNew + diff.detachNew) > 0;
}
// The species side is dirty when the confirmed species changes OR when the
// burst is already confirmed as the current species but some post-apply frame
// still lacks that species keyword (e.g. legacy data that only tagged picks).
// /api/pipeline/group/apply is flags-only, so those missing keywords would
// never be written unless the species side commits — mirror the rapid-review
// gate (speciesChanged || outstanding tag work). The tag count depends on the
// resolved flags state (a removed photo only drops out when its removal
// actually commits), so callers pass the already-resolved `flags` value.
function grmSpeciesDirty(diff, flagsResolved) {
  if (diff.speciesChanged) return true;
  return grmSpeciesTagCount({ flags: flagsResolved }, diff.species) > 0;
}

// Resolve the effective checked state: user override wins, else smart default.
function grmResolveChecks(diff) {
  var sp = grmState.confirmSpeciesOverride;
  var fl = grmState.applyFlagsOverride;
  var flags = fl === null ? grmFlagsDirty(diff) : fl;
  return {
    species: sp === null ? grmSpeciesDirty(diff, flags) : sp,
    flags: flags,
  };
}

// Update the Apply button label to reflect the *new* DB writes that will
// happen — not the totals in each zone — so the user can see at a glance
// what they're committing to. A photo already flagged in the DB and still
// in the picks zone is a no-op and isn't counted.
function grmUpdateApplyLabel() {
  var btn = document.getElementById('grmApplyBtn');
  if (!btn) return;
  if (grmState && grmState.applying) return;
  // While the modal is unseeded (loading or seed failed), the button owns
  // its own label/disabled state set by openGroupReview / grmShowSeedError.
  // Don't compute a diff against an empty/unknown dbState — it would
  // misreport pending writes and clobber the error/loading message.
  if (!grmState || !grmState.seeded) return;
  var diff = grmComputeDiff();

  var checks = grmResolveChecks(diff);
  var spChk = document.getElementById('grmConfirmSpeciesChk');
  var flChk = document.getElementById('grmApplyFlagsChk');
  if (spChk) spChk.checked = checks.species;
  if (flChk) flChk.checked = checks.flags;

  // Amber hint only when a box is unchecked but its side is dirty.
  var spHint = document.getElementById('grmSpeciesDirtyHint');
  if (spHint) {
    var show = !checks.species && grmSpeciesDirty(diff, checks.flags);
    spHint.style.display = show ? '' : 'none';
    spHint.textContent = show ? "species won't be confirmed" : '';
  }
  var flHint = document.getElementById('grmFlagsDirtyHint');
  if (flHint) {
    var n = diff.flagNew + diff.rejectNew + diff.clearNew + diff.detachNew;
    var showF = !checks.flags && n > 0;
    flHint.style.display = showF ? '' : 'none';
    flHint.textContent = showF ? (n + ' cull change' + (n === 1 ? '' : 's') + " won't be saved") : '';
  }

  // True number of frames the species call will newly tag = all post-apply
  // burst members (parameterized by checks.flags), not just picks.
  var tagCount = grmSpeciesTagCount(checks, diff.species);

  var parts = [];
  if (checks.flags && diff.flagNew > 0) parts.push('Flag ' + diff.flagNew);
  if (checks.flags && diff.rejectNew > 0) parts.push('Reject ' + diff.rejectNew);
  if (checks.flags && diff.clearNew > 0) parts.push('Clear ' + diff.clearNew);
  if (checks.species && diff.speciesChanged) parts.push('Set species');
  if (checks.species && tagCount > 0) parts.push('Tag ' + tagCount + (diff.species ? ' as ' + diff.species : ''));
  if (checks.flags && diff.detachNew > 0) parts.push('Detach ' + diff.detachNew);
  btn.textContent = parts.length ? parts.join(' · ') + ' & Close' : 'Apply and close';
  btn.title = grmApplyTitle(diff, checks, checks.species ? tagCount : 0);

  // In Workspace/Collection scope the current pipelineResults are a *view*
  // over a different photo set — committing them would overwrite the
  // workspace's saved review cache with just this scoped subset (both via
  // the client save-cache POST and the server-side cache rewrite inside
  // /api/encounters/species). Keep the modal browsable but block Apply.
  if (isScopedReviewView()) {
    btn.disabled = true;
    btn.style.opacity = '0.5';
    btn.style.cursor = 'not-allowed';
    btn.textContent = 'Read-only in scope view';
    btn.title = 'Switch Scope to "Latest review" to save picks, rejects, or species confirmations.';
  }
}

// Diff the current modal zones against the DB snapshot we opened with.
// Returns counts of new flag/reject/clear writes plus tag adds that the
// apply call will perform.
function grmComputeDiff() {
  var speciesEl = document.getElementById('grmSpecies');
  var species = speciesEl ? (speciesEl.value || '').trim() : '';
  var diff = {
    flagNew: 0,
    rejectNew: 0,
    clearNew: 0,
    detachNew: 0,
    speciesChanged: !!(species && grmState && species !== (grmState.initialConfirmedSpecies || '')),
    species: species
  };
  if (!grmState || !grmState.items) return diff;

  grmState.items.forEach(function(p) {
    if (grmState.removed && grmState.removed.has(p.id)) {
      diff.detachNew++;
      return;
    }
    var st = (grmState.dbState && grmState.dbState[p.id]) || {};
    var oldFlag = st.flag || 'none';
    var newFlag;
    if (grmState.picks.has(p.id)) newFlag = 'flagged';
    else if (grmState.rejects.has(p.id)) newFlag = 'rejected';
    else newFlag = 'none';

    if (newFlag !== oldFlag) {
      if (newFlag === 'flagged') diff.flagNew++;
      else if (newFlag === 'rejected') diff.rejectNew++;
      else diff.clearNew++;
    }
  });
  return diff;
}

function grmPlural(n, one, many) {
  return n === 1 ? one : (many || one + 's');
}

// Photos that will carry the confirmed species = the burst's post-apply
// members. A removed photo is excluded only if its removal actually
// commits (checks.flags); when flags are unchecked the removal is
// discarded, so the photo stays a member and is tagged.
function grmSpeciesMemberItems(checks) {
  return grmState.items.filter(function(p) {
    return checks.flags ? !grmState.removed.has(p.id) : true;
  });
}
// Count of those members that don't already carry the species keyword
// (i.e. the number /api/encounters/species will newly tag).
function grmSpeciesTagCount(checks, species) {
  if (!species) return 0;
  return grmSpeciesMemberItems(checks).filter(function(p) {
    return !grmHasSpeciesKeyword(p.id, species);
  }).length;
}

function grmApplyTitle(diff, checks, tagCount) {
  // Gate each action by the same checkbox that actually commits it, so the
  // tooltip never promises a write that the current selection won't perform.
  // "Apply picks/rejects" owns flag changes AND removed-photo detach; the
  // species checkbox owns the confirmed-species + keyword-tag writes.
  var actions = [];
  if (checks.flags && diff.flagNew > 0) actions.push('flag ' + diff.flagNew + ' ' + grmPlural(diff.flagNew, 'photo') + ' as ' + grmPlural(diff.flagNew, 'a pick', 'picks'));
  if (checks.flags && diff.rejectNew > 0) actions.push('reject ' + diff.rejectNew + ' ' + grmPlural(diff.rejectNew, 'photo'));
  if (checks.flags && diff.clearNew > 0) actions.push('clear flags on ' + diff.clearNew + ' ' + grmPlural(diff.clearNew, 'photo'));
  if (checks.species && diff.speciesChanged) actions.push('set confirmed species to "' + diff.species + '"');
  if (checks.species && tagCount > 0) actions.push('add species keyword "' + diff.species + '" to ' + tagCount + ' ' + grmPlural(tagCount, 'burst frame'));
  if (checks.flags && diff.detachNew > 0) actions.push('detach ' + diff.detachNew + ' ' + grmPlural(diff.detachNew, 'photo') + ' from this burst');
  return actions.length
    ? 'Apply will ' + actions.join(', ') + ', then close this burst.'
    : 'Apply will make no database changes, then close this burst.';
}

/* --- Zone movement --- */

function _grmGetZone(photoId) {
  if (grmState.picks.has(photoId)) return 'picks';
  if (grmState.rejects.has(photoId)) return 'rejects';
  return 'candidates';
}

function _grmMarkTouched(photoId) {
  if (!grmState.touched) grmState.touched = new Set();
  if (photoId) grmState.touched.add(photoId);
}

function _grmActionTargetIds(explicitIds) {
  var visible = {};
  _grmVisibleItems().forEach(function(p) { visible[p.id] = true; });
  var ids = [];
  if (Array.isArray(explicitIds)) {
    explicitIds.forEach(function(id) {
      var photoId = parseInt(id, 10);
      if (visible[photoId] && ids.indexOf(photoId) === -1) ids.push(photoId);
    });
    return ids;
  }
  if (grmState.selectedIds && grmState.selectedIds.size) {
    grmState.selectedIds.forEach(function(id) {
      var photoId = parseInt(id, 10);
      if (visible[photoId] && ids.indexOf(photoId) === -1) ids.push(photoId);
    });
  }
  if (!ids.length && grmState.selected) {
    var selectedId = parseInt(grmState.selected, 10);
    if (visible[selectedId]) ids.push(selectedId);
  }
  return ids;
}

function _grmMarkTouchedMany(photoIds) {
  photoIds.forEach(function(photoId) { _grmMarkTouched(photoId); });
}

function grmMoveUp(photoIds) {
  if (grmState && grmState.applying) return false;
  var ids = _grmActionTargetIds(photoIds);
  if (!ids.length) return;
  _grmMarkTouchedMany(ids);
  ids.forEach(function(photoId) {
    var zone = _grmGetZone(photoId);
    if (zone === 'rejects') {
      grmState.rejects.delete(photoId);
    } else if (zone === 'candidates') {
      grmState.picks.add(photoId);
    }
  });
  grmSyncZoneCards();
}

function grmMoveDown(photoIds) {
  if (grmState && grmState.applying) return false;
  var ids = _grmActionTargetIds(photoIds);
  if (!ids.length) return;
  _grmMarkTouchedMany(ids);
  ids.forEach(function(photoId) {
    var zone = _grmGetZone(photoId);
    if (zone === 'picks') {
      grmState.picks.delete(photoId);
    } else if (zone === 'candidates') {
      grmState.rejects.add(photoId);
    }
  });
  grmSyncZoneCards();
}

function grmMovePick(photoIds) {
  if (grmState && grmState.applying) return false;
  var ids = _grmActionTargetIds(photoIds);
  if (!ids.length) return;
  _grmMarkTouchedMany(ids);
  ids.forEach(function(photoId) {
    grmState.rejects.delete(photoId);
    grmState.picks.add(photoId);
  });
  grmSyncZoneCards();
}

function grmMoveReject(photoIds) {
  if (grmState && grmState.applying) return false;
  var ids = _grmActionTargetIds(photoIds);
  if (!ids.length) return;
  _grmMarkTouchedMany(ids);
  ids.forEach(function(photoId) {
    grmState.picks.delete(photoId);
    grmState.rejects.add(photoId);
  });
  grmSyncZoneCards();
}

function grmMoveCandidate(photoIds) {
  if (grmState && grmState.applying) return false;
  var ids = _grmActionTargetIds(photoIds);
  if (!ids.length) return;
  _grmMarkTouchedMany(ids);
  ids.forEach(function(photoId) {
    grmState.picks.delete(photoId);
    grmState.rejects.delete(photoId);
  });
  grmSyncZoneCards();
}

function grmRemoveFromGroup(photoIds) {
  if (grmState && grmState.applying) return false;
  if (grmState && grmState.allowRemove === false) return;
  var ids = _grmActionTargetIds(photoIds);
  if (!ids.length) return;
  _grmMarkTouchedMany(ids);
  ids.forEach(function(photoId) {
    grmState.removed.add(photoId);
    grmState.picks.delete(photoId);
    grmState.rejects.delete(photoId);
  });
  if (grmState.selectedIds) {
    ids.forEach(function(photoId) { grmState.selectedIds.delete(photoId); });
  }
  if (ids.indexOf(grmState.selected) !== -1) {
    grmState.selected = grmState.selectedIds && grmState.selectedIds.size
      ? Array.from(grmState.selectedIds)[0]
      : null;
  }
  if (ids.indexOf(grmState.selectionAnchor) !== -1) {
    grmState.selectionAnchor = grmState.selected;
  }
  grmSyncZoneCards();
  grmRefreshSelectedLoupe();
}

/* --- Apply & Close --- */

// Confirm species for the burst. Pipeline bursts go through the same endpoint
// the grid uses (persists confirmation + auto-detach). Browse-selection ad-hoc
// sets have no encounter, so just tag the keyword on all members.
async function grmConfirmSpeciesCall(species, memberIds) {
  if (!species || memberIds.length === 0) return;
  if (grmState.source === 'browse-selection') {
    // Browse selections are ad-hoc and have no persisted encounter/burst to
    // confirm, so "Confirm species" here means "tag these frames with the
    // species keyword" — it does not set species_confirmed (there's nothing
    // to mark). Pipeline bursts below get full server-owned confirmation.
    await safeFetch('/api/batch/keyword', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ photo_ids: memberIds, name: species, type: 'taxonomy' }),
    });
    return;
  }
  var resp = await safeFetch('/api/encounters/species', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ species: species, photo_ids: memberIds, burst_index: grmState.burstIdx }),
  });
  // Adopt authoritative structure (mirrors confirmSpecies()).
  if (resp && resp.encounters) {
    pipelineResults.encounters = resp.encounters;
    if (resp.summary) pipelineResults.summary = resp.summary;
  }
}

async function grmApply() {
  // Belt-and-suspenders: the button is disabled until seed completes, but
  // a stuck-open modal whose /group/state never resolved (offline, server
  // error after retry) must not let Apply clear flags.
  if (!grmState.seeded) {
    console.warn('grmApply called before /group/state seed completed; ignoring.');
    return;
  }
  // Scoped-view guard: grmApply POSTs pipelineResults to /api/pipeline/save-cache
  // (line ~5921) and confirms species via /api/encounters/species, which
  // reloads and rewrites the saved cache server-side — either path would
  // clobber the workspace's persisted review with this scoped subset.
  // grmUpdateApplyLabel keeps the button visibly disabled here; this is
  // defense in depth against a stray click while still seeded.
  if (isScopedReviewView()) {
    notifyReadOnlyScopedView();
    return;
  }
  if (grmState.applying) return;
  var applySessionId = grmState.sessionId;
  grmSetApplying(true);
  var grmFlagLockIds = [];

  function sessionIsActive() {
    var overlay = document.getElementById('grmOverlay');
    return grmState.sessionId === applySessionId &&
      overlay && overlay.classList.contains('open');
  }

  try {
    await waitForPipelineReviewDirectFlagWrites(
      grmState.items.map(function(photo) { return photo.id; })
    );
    if (!sessionIsActive()) return false;
    var diff = grmComputeDiff();
    var checks = grmResolveChecks(diff);
    var species = document.getElementById('grmSpecies').value.trim();

    // GRM pick/reject writes use /api/pipeline/group/apply rather than the
    // per-photo helper, so serialize them explicitly with bulk Reject/Clear
    // and Undo. Holding the lock through the full apply also blocks a bulk
    // action from starting after the modal write has begun.
    if (checks.flags) {
      var candidateFlagLockIds = grmState.items
        .filter(function(p) { return !grmState.removed.has(p.id); })
        .map(function(p) { return p.id; });
      var overlapsBulkFlagWrite = candidateFlagLockIds.some(function(pid) {
        return pipelineReviewGroupFlagInFlightPhotoIds.has(pid);
      });
      if (overlapsBulkFlagWrite) {
        showToast('A bulk reject for this group is still finishing — try Apply again in a moment', 'error');
        return;
      }
      grmFlagLockIds = candidateFlagLockIds;
      grmFlagLockIds.forEach(function(pid) {
        pipelineReviewGroupFlagInFlightPhotoIds.add(pid);
      });
    }

    // --- Flags side (picks/rejects/candidates + removed→detach) ---
    // Only committed when the "Apply picks/rejects" box is checked. Removals are
    // part of "cull changes", so the removed→detach block is gated here too.
    if (checks.flags) {
      var picksArr = Array.from(grmState.picks);
      var rejectsArr = Array.from(grmState.rejects);
      var candidatesArr = grmState.items
        .filter(function(p) {
          return !grmState.picks.has(p.id) && !grmState.rejects.has(p.id) && !grmState.removed.has(p.id);
        })
        .map(function(p) { return p.id; });

      // Persist flags to the database first. If this fails we abort so the local
      // pipeline cache and the DB can't drift apart — the prior fire-and-forget
      // save-cache + no-DB-write was the bug behind this whole change.
      var applyResp;
      var sourceBurst = ((pipelineResults.encounters[grmState.encIdx] || {}).bursts || [])[grmState.burstIdx];
      try {
        applyResp = await safeFetch('/api/pipeline/group/apply', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            picks: picksArr,
            rejects: rejectsArr,
            candidates: candidatesArr,
            removed: grmState.source === 'browse-selection' ? [] : Array.from(grmState.removed).filter(function(pid) {
              return grmState.appliedRemovalSessionId !== applySessionId ||
                !(grmState.appliedRemovalIds || []).includes(pid);
            }),
            encounter_index: grmState.encIdx,
            burst_index: grmState.burstIdx,
            expected_burst_photo_ids: grmState.source === 'browse-selection' ? [] :
              (Array.isArray(sourceBurst) ? sourceBurst : sourceBurst?.photo_ids || []),
          }),
        });
      } catch (e) {
        console.error('Pipeline group apply failed:', e);
        return;
      }
      if (!sessionIsActive()) return false;

    // Update local pipelineResults with pick/reject/candidate decisions
    var photoMap = {};
    pipelineResults.photos.forEach(function(p) { photoMap[p.id] = p; });

    grmState.picks.forEach(function(pid) {
      var p = photoMap[pid];
      if (p) p.label = 'KEEP';
    });

    grmState.rejects.forEach(function(pid) {
      var p = photoMap[pid];
      if (p) p.label = 'REJECT';
    });

    // Candidates (not picked, not rejected, not removed) → REVIEW
    grmState.items.forEach(function(item) {
      if (!grmState.picks.has(item.id) && !grmState.rejects.has(item.id) && !grmState.removed.has(item.id)) {
        var p = photoMap[item.id];
        if (p) p.label = 'REVIEW';
      }
    });

    // Sync the in-memory pipeline cache's per-photo `flag` with what the
    // server just wrote, so the badges in the main grid (which read p.flag)
    // refresh without a full reload. Rejected photos also lose eligibility
    // as species representatives (get_species_representatives(eligible_only)
    // filters them out), so clear the cached representative fields too — the
    // main pipeline grid renders its Representative badge from
    // p.is_species_representative and would otherwise keep displaying it on
    // a rejected photo until a full reload.
    if (applyResp && applyResp.photos) {
      Object.keys(applyResp.photos).forEach(function(pidStr) {
        var pid = parseInt(pidStr, 10);
        var p = photoMap[pid];
        if (!p) return;
        var newFlag = applyResp.photos[pidStr].flag;
        p.flag = newFlag;
        if (newFlag === 'rejected') {
          if (p.is_species_representative) p.is_species_representative = false;
          if (Array.isArray(p.life_list)) {
            p.life_list.forEach(function(entry) {
              if (!entry) return;
              if (entry.is_current_photo) entry.is_current_photo = false;
              if (entry.is_species_representative) entry.is_species_representative = false;
            });
          }
        }
      });
    }

    if (grmState.source === 'browse-selection') {
      if (grmState.removed.size > 0) {
        var encBrowse = pipelineResults.encounters[grmState.encIdx];
        var keepIds = encBrowse.photo_ids.filter(function(pid) { return !grmState.removed.has(pid); });
        if (keepIds.length === 0) {
          // Every photo was removed from the temporary Browse burst. Any
          // flag edits were already persisted by /group/apply above, so there
          // is nothing left to review (and no burst left to confirm a species
          // on). Drop the handoff and degrade to normal pipeline review
          // instead of stranding the page on an empty browse-selection
          // encounter (0 photos but still 1 encounter/1 burst, with a Reopen
          // flow that can no longer reopen).
          document.getElementById('grmSpecies').value = '';
          closeGroupReview(true);
          fallbackToNormalPipelineReview();
          return;
        }
        encBrowse.photo_ids = keepIds;
        encBrowse.photo_count = keepIds.length;
        if (encBrowse.bursts && encBrowse.bursts[grmState.burstIdx]) {
          encBrowse.bursts[grmState.burstIdx].photo_ids = keepIds.slice();
        }
        pipelineResults.photos = pipelineResults.photos.filter(function(p) {
          return !grmState.removed.has(p.id);
        });
      }
      // Flags are now persisted to the DB — the one-shot handoff has served
      // its purpose, so drop it. A later reload degrades to normal review;
      // the in-memory group stays reopenable via the Reopen button.
      clearBrowseBurstHandoff();
    } else {
      if (applyResp.encounters) pipelineResults.encounters = applyResp.encounters;
      if (applyResp.summary) pipelineResults.summary = applyResp.summary;
      grmState.appliedRemovalSessionId = applySessionId;
      grmState.appliedRemovalIds = Array.from(grmState.removed);
    }
  }

  // Group Apply has already saved flags and removals from its canonical cache.
  // Species confirmation below reads that committed structure.

  // --- Species side ---
  // Server-owned now: tag all remaining (non-removed) burst frames + mark the
  // burst confirmed via the unified endpoint. Removed photos are excluded so
  // they aren't tagged. Skip when the burst no longer exists (emptied by the
  // detach above) or has no members left.
    if (checks.species) {
      var burstExists = grmState.source === 'browse-selection' ||
        (pipelineResults.encounters[grmState.encIdx] &&
         pipelineResults.encounters[grmState.encIdx].bursts &&
         pipelineResults.encounters[grmState.encIdx].bursts[grmState.burstIdx]);
      if (burstExists) {
        // Post-apply burst members carry the species. Uses the same rule as the
        // label/tooltip (grmSpeciesMemberItems): a removed photo is excluded only
        // when its removal actually committed (checks.flags); otherwise it stays a
        // member and gets tagged.
        var memberIds = grmSpeciesMemberItems(checks).map(function(p) { return p.id; });
        // A species-call failure intentionally throws out of grmApply *before*
        // close, leaving the modal open so the user can retry. Any flags were
        // already committed idempotently above, so a retry re-runs only the
        // species side. No try/catch — propagating the throw is the desired
        // "keep modal open" behavior.
        if (memberIds.length) await grmConfirmSpeciesCall(species, memberIds);
        if (!sessionIsActive()) return false;
      }
    }

    // Browse-selection is a one-shot handoff: drop it on close regardless of
    // which side was committed, so a reload degrades to normal review instead
    // of reopening a burst whose edits already landed. (Idempotent with the
    // call inside the flags block above.)
    if (grmState.source === 'browse-selection') {
      clearBrowseBurstHandoff();
    }

    refreshLatestScopeSnapshotIfCurrent();
    closeGroupReview(true);
    // Clear the species input for next use
    document.getElementById('grmSpecies').value = '';
    window.requestAnimationFrame(function() {
      window.setTimeout(function() {
        renderResults();
        updateSummaryBar(refreshLocalSummaryCounts());
      }, 0);
    });
  } finally {
    grmFlagLockIds.forEach(function(pid) {
      pipelineReviewGroupFlagInFlightPhotoIds.delete(pid);
    });
    var overlay = document.getElementById('grmOverlay');
    if (grmState.sessionId === applySessionId &&
        overlay && overlay.classList.contains('open')) {
      grmSetApplying(false);
    }
  }
}

function bindGroupReviewKeyboard() {
    /* --- Keyboard handling --- */

    document.addEventListener('keydown', function(e) {
      if (!document.getElementById('grmOverlay').classList.contains('open')) return;
      // The shared lightbox stacks over Group Review and owns its own key handling.
      // Its handler consumes arrows and P/X but not Space/Delete/Backspace, so those
      // would otherwise fall through here and silently mutate the hidden selection.
      var lb = document.getElementById('lightboxOverlay');
      if (lb && lb.classList.contains('active')) return;
      // Find Similar can also stack above Group Review. Its overlay owns the
      // keyboard until it closes; otherwise P/X/Space/Delete mutate the hidden
      // burst selection underneath it.
      var similar = document.getElementById('similarOverlay');
      if (similar && similar.classList.contains('active')) return;
      // A context-menu action can open an organization dialog above Group
      // Review. That dialog owns Escape and all review shortcuts until it closes.
      if (document.querySelector('#pipelineKeywordModal.open, #pipelineCollectionModal.open')) return;
      if (grmState && grmState.applying) {
        if (['Escape', 'ArrowUp', 'ArrowDown', 'ArrowLeft', 'ArrowRight',
             'Delete', 'Backspace', 'p', 'P', 'x', 'X', ' '].indexOf(e.key) !== -1) {
          e.preventDefault();
        }
        return;
      }
      if (e.target.tagName === 'INPUT') return;
      // The mouse-help control is focusable so keyboard users can read its
      // popover; don't let keystrokes there (e.g. Space) fall through to the
      // review shortcuts and mutate the selected photo's state.
      if (e.target.closest && e.target.closest('.grm-mouse-help')) return;

      if (e.key === 'Escape') { closeGroupReview(); e.preventDefault(); return; }
      if (e.key === 'ArrowUp') { grmMoveUp(); e.preventDefault(); return; }
      if (e.key === 'ArrowDown') { grmMoveDown(); e.preventDefault(); return; }
      if (e.key === 'ArrowLeft') {
        var items = grmState.items.filter(function(p) { return !grmState.removed.has(p.id); });
        var idx = items.findIndex(function(p) { return p.id === grmState.selected; });
        if (idx > 0) grmSelect(items[idx - 1].id);
        e.preventDefault(); return;
      }
      if (e.key === 'ArrowRight') {
        items = grmState.items.filter(function(p) { return !grmState.removed.has(p.id); });
        idx = items.findIndex(function(p) { return p.id === grmState.selected; });
        if (idx < items.length - 1) grmSelect(items[idx + 1].id);
        e.preventDefault(); return;
      }
      if (e.key === 'Delete' || e.key === 'Backspace') {
        if (grmState.allowRemove !== false) grmRemoveFromGroup();
        e.preventDefault(); return;
      }
      if (e.key === ' ') { grmMoveCandidate(); e.preventDefault(); return; }
      if (e.key && e.key.toLowerCase() === 'z') { grmToggleLoupeOneToOne(); e.preventDefault(); return; }
      if (pipelineReviewBareKey(e, 'p')) { grmMovePick(); e.preventDefault(); return; }
      if (pipelineReviewBareKey(e, 'x')) { grmMoveReject(); e.preventDefault(); return; }
    });
}
