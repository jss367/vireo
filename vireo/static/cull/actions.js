// Changing decisions: expanding a species, cycling and setting a photo's
// action, releasing and clearing pins, and Apply.
// Classic page script.

/** @param {number} idx */
function toggleSpecies(idx) {
  expandedSpecies[idx] = !expandedSpecies[idx];
  var body = /** @type {HTMLElement} */ (document.getElementById('speciesBody' + idx));
  body.style.display = body.style.display === 'none' ? '' : 'none';
}

/** @param {number} speciesIdx @param {number} poseIdx @param {number} photoId */
function toggleCullAction(speciesIdx, poseIdx, photoId) {
  if (cullOperationPending() || !cullData) return;
  var sg = /** @type {CullSpeciesGroup} */ (cullData.species_groups[speciesIdx]);
  var groups = sg.scene_groups || sg.pose_groups || [];
  var pg = /** @type {CullSceneGroup} */ (groups[poseIdx]);
  var photo = pg.photos.find(function(p) { return p.photo_id === photoId; });
  if (!photo) return;

  /** @type {CullAction} */
  var next = photo.action === 'keep' ? 'review' : (photo.action === 'review' ? 'reject' : 'keep');
  setCullAction(photoId, next);
}

/** @param {number} photoId @param {CullAction} action */
function setCullAction(photoId, action) {
  if (cullOperationPending()) return;
  cullManualDecisions[photoId] = action;
  cullDirty = unappliedPinCount() > 0;
  rebuildCullDataFromPipeline();
}

/** @param {number} photoId */
function releaseCullDecision(photoId) {
  if (cullOperationPending()) return;
  if (!cullManualDecisions[photoId]) return;
  delete cullManualDecisions[photoId];
  delete cullAppliedDecisions[photoId];
  cullDirty = unappliedPinCount() > 0;
  rebuildCullDataFromPipeline();
}

function clearPinnedDecisions() {
  if (cullOperationPending()) return;
  if (!manualDecisionCount()) return;
  cullManualDecisions = {};
  cullAppliedDecisions = {};
  cullDirty = false;
  rebuildCullDataFromPipeline();
  var status = /** @type {HTMLElement} */ (document.getElementById('cullStatus'));
  if (status) {
    status.textContent = 'Pinned decisions cleared \u2014 showing the current suggestions.';
    status.style.color = 'var(--text-dim)';
  }
}

async function applyCulling() {
  if (!cullData || cullOperationPending()) return;

  /** @type {number[]} */
  var keepers = [];
  /** @type {number[]} */
  var rejects = [];
  /** @type {number[]} */
  var unflag = [];
  /** @type {Record<string, CullAction>} */
  var appliedPins = {};
  cullData.species_groups.forEach(function(sg) {
    var groups = sg.scene_groups || sg.pose_groups || [];
    groups.forEach(function(pg) {
      pg.photos.forEach(function(p) {
        if (p.decision_source === 'manual') appliedPins[p.photo_id] = p.action;
        if (p.action === 'keep') keepers.push(p.photo_id);
        else if (p.action === 'reject') rejects.push(p.photo_id);
        else if (p.decision_source === 'manual') {
          // Persist explicit Review even when the raw flag is already clear,
          // so a later ranking job cannot replace this photographer choice.
          unflag.push(p.photo_id);
        }
      });
    });
  });

  var confirmMsg = 'Apply culling decisions?\n\n' +
    keepers.length + ' photos flagged as keepers\n' +
    rejects.length + ' photos flagged as rejected';
  if (unflag.length) {
    confirmMsg += '\n' + unflag.length + ' photos cleared back to no flag (moved to Review)';
  }
  confirmMsg += '\n\nThis sets the flag on each photo. Use Undo below to restore the previous flags.';
  if (!confirm(confirmMsg)) return;

  cullBusy = true;
  /** @type {HTMLButtonElement} */ (document.getElementById('applyBtn')).disabled = true;
  try {
    await safeFetch('/api/culling/apply', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({keepers: keepers, rejects: rejects, unflag: unflag}),
    }, { toast: false });
    // Apply establishes the saved baseline. Review photos were not sent to
    // the server, so retain their existing flags instead of clearing them.
    var keeperIds = new Set(keepers);
    var rejectIds = new Set(rejects);
    var clearedIds = new Set(unflag);
    (pipelineResults ? pipelineResults.photos : []).forEach(function(photo) {
      if (keeperIds.has(photo.id)) photo.flag = 'flagged';
      else if (rejectIds.has(photo.id)) photo.flag = 'rejected';
      else if (clearedIds.has(photo.id)) photo.flag = 'none';
      if (keeperIds.has(photo.id) || rejectIds.has(photo.id) || clearedIds.has(photo.id)) {
        photo.flag_confirmation_status = 'confirmed';
      }
    });
    // Pins the write covered are in the database now. Keep/reject ones are
    // saved flags from here on; review ones stay pinned (no flag can say
    // "undecided on purpose") but stop counting as unsaved. Pins on photos
    // outside this view were never sent, so they stay untouched.
    Object.keys(appliedPins).forEach(function(pid) {
      if (appliedPins[pid] === 'review') {
        cullAppliedDecisions[pid] = 'review';
      } else {
        delete cullManualDecisions[pid];
        delete cullAppliedDecisions[pid];
      }
    });
    syncCullSourceFlags(/** @type {CullSourceResults} */ (pipelineResults));
    cullDirty = unappliedPinCount() > 0;
    rebuildCullDataFromPipeline();
    /** @type {HTMLElement} */ (document.getElementById('cullStatus')).textContent = 'Applied! ' + keepers.length +
      ' keepers, ' + rejects.length + ' rejected' +
      (unflag.length ? ', ' + unflag.length + ' cleared' : '');
    /** @type {HTMLElement} */ (document.getElementById('cullStatus')).style.color = 'var(--accent)';
  } catch(e) {
    /** @type {HTMLElement} */ (document.getElementById('cullStatus')).textContent = 'Error: ' + (e instanceof Error ? e.message : String(e));
  } finally {
    cullBusy = false;
    updateCullApplyButton();
  }
}
