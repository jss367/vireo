// Reconcile photo deletion and life-list changes with local review state.
// Classic page script; shared globals are initialized before boot.js runs.

function bindPipelineReviewPhotoEvents() {
    // The shared lightbox owns deletion, but Process Review owns additional
    // encounter and Group Review state. Remove the completed deletion from every
    // local structure before rendering or applying staged changes again.
    document.addEventListener('lightbox:photodeleted', function(e) {
      var photoId = Number(e && e.detail && e.detail.photoId);
      if (!Number.isFinite(photoId) || !pipelineResults) return;
      var groupOverlay = document.getElementById('grmOverlay');
      var groupWasOpen = !!(
        groupOverlay && groupOverlay.classList.contains('open') && grmState
      );

      pipelineReviewContextPhotoIds = pipelineReviewContextPhotoIds.filter(function(id) {
        return id !== photoId;
      });
      if (Array.isArray(window._vireoNativeMenuPhotoIdsOverride)) {
        window._vireoNativeMenuPhotoIdsOverride =
          window._vireoNativeMenuPhotoIdsOverride.filter(function(id) {
            return id !== photoId;
          });
        if (!window._vireoNativeMenuPhotoIdsOverride.length) {
          window._vireoNativeMenuPhotoIdsOverride = null;
        }
      }
      if (inspectPhotoId === photoId) closeInspect();

      pipelineResults.photos = (pipelineResults.photos || []).filter(function(photo) {
        return photo.id !== photoId;
      });
      (pipelineResults.encounters || []).forEach(function(encounter) {
        encounter.photo_ids = (encounter.photo_ids || []).filter(function(id) {
          return id !== photoId;
        });
        encounter.photo_count = encounter.photo_ids.length;
        (encounter.bursts || []).forEach(function(burst) {
          var ids = burst.photo_ids || burst;
          if (!Array.isArray(ids)) return;
          var filtered = ids.filter(function(id) { return id !== photoId; });
          if (burst.photo_ids) burst.photo_ids = filtered;
          else {
            burst.splice(0, burst.length);
            Array.prototype.push.apply(burst, filtered);
          }
        });
        encounter.bursts = (encounter.bursts || []).filter(function(burst) {
          var ids = burst.photo_ids || burst;
          return Array.isArray(ids) && ids.length > 0;
        });
        encounter.burst_count = encounter.bursts.length;
      });
      pipelineResults.encounters = (pipelineResults.encounters || []).filter(function(encounter) {
        return encounter.photo_ids && encounter.photo_ids.length > 0;
      });
      _focusedEncounterIdx = null;

      if (grmState && Array.isArray(grmState.items)) {
        grmState.items = grmState.items.filter(function(photo) { return photo.id !== photoId; });
        ['picks', 'rejects', 'removed', 'selectedIds', 'touched'].forEach(function(name) {
          if (grmState[name] && typeof grmState[name].delete === 'function') {
            grmState[name].delete(photoId);
          }
        });
        ['dbState', 'selectedSubjectByPhoto'].forEach(function(name) {
          if (grmState[name]) delete grmState[name][String(photoId)];
        });
        if (grmState.selectionAnchor === photoId) grmState.selectionAnchor = null;
      }

      // Pruning an empty encounter or burst shifts later array indices. Group
      // Review keeps those indices for Apply, so remap its surviving group by
      // member identity before rendering or persisting another change.
      if (groupWasOpen && grmState.items.length) {
        var memberIds = grmState.items.map(function(photo) { return photo.id; });
        var nextEncIdx = -1;
        var nextBurstIdx = -1;
        for (var encIdx = 0;
             encIdx < pipelineResults.encounters.length && nextEncIdx < 0;
             encIdx++) {
          var bursts = pipelineResults.encounters[encIdx].bursts || [];
          for (var burstIdx = 0; burstIdx < bursts.length; burstIdx++) {
            var burstIds = bursts[burstIdx].photo_ids || bursts[burstIdx];
            if (Array.isArray(burstIds) && memberIds.every(function(id) {
              return burstIds.indexOf(id) !== -1;
            })) {
              nextEncIdx = encIdx;
              nextBurstIdx = burstIdx;
              break;
            }
          }
        }
        if (nextEncIdx >= 0) {
          grmState.encIdx = nextEncIdx;
          grmState.burstIdx = nextBurstIdx;
        } else {
          closeGroupReview(true);
        }
      }

      renderResults();
      updateSummaryBar(refreshLocalSummaryCounts());
      refreshLatestScopeSnapshotIfCurrent();
      if (groupOverlay && groupOverlay.classList.contains('open')) {
        if (grmState.items.length) grmRenderKeepingCurrentSelection(grmState.items);
        else closeGroupReview();
      }
    });

    // setLifeListPhoto (card context menu / shared lightbox action) only emits a
    // lifelist:changed event and updates its own caches. Without this listener the
    // pipeline cards keep the pre-change value of p.is_species_representative, so
    // the newly assigned photo stays un-badged and any previously visible former
    // representative keeps its badge until a full reload. Walk pipelineResults.photos
    // in place and flip the species entry's flags, then rerender the grid. If the
    // burst modal happens to be open on the matching species, also update its
    // dbState so the modal's Representative badge stays honest.
    document.addEventListener('lifelist:changed', function(e) {
      var detail = e && e.detail ? e.detail : {};
      var species = detail.species;
      var photoId = detail.photoId;
      if (!species || !photoId) return;
      if (!pipelineResults || !Array.isArray(pipelineResults.photos)) return;
      var changed = false;
      pipelineResults.photos.forEach(function(p) {
        var entries = Array.isArray(p.life_list) ? p.life_list : [];
        var touched = false;
        entries.forEach(function(entry) {
          if (!entry || entry.species !== species) return;
          var isCurrent = p.id === photoId;
          if (entry.is_current_photo !== isCurrent || entry.is_species_representative !== isCurrent) {
            entry.is_current_photo = isCurrent;
            entry.is_species_representative = isCurrent;
            touched = true;
          }
        });
        if (p.id === photoId && !entries.some(function(entry) { return entry && entry.species === species; })) {
          entries.push({species: species, is_current_photo: true, is_species_representative: true});
          p.life_list = entries;
          touched = true;
        }
        var isRep = entries.some(function(entry) {
          return entry && entry.is_species_representative;
        });
        if (p.is_species_representative !== isRep) {
          p.is_species_representative = isRep;
          touched = true;
        }
        if (touched) changed = true;
      });
      if (grmState && grmState.dbState && grmState.keywordStateSpecies === species) {
        Object.keys(grmState.dbState).forEach(function(pidStr) {
          var pid = parseInt(pidStr, 10);
          var st = grmState.dbState[pid];
          if (!st) return;
          var next = (pid === photoId);
          if (st.is_species_representative !== next) {
            st.is_species_representative = next;
            changed = true;
          }
        });
      }
      if (changed) {
        renderResults();
        if (typeof renderGroupModal === 'function') {
          var overlay = document.getElementById('grmOverlay');
          if (overlay && overlay.classList.contains('open')) renderGroupModal();
        }
      }
    });
}
