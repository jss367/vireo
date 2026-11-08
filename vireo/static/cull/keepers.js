// Relational rejects: which kept photo a reject loses to, the measurement
// the two share, the Apply payload that records it, and the orphan check
// (saved rejects whose keeper has since been deleted or rejected).
// Classic page script.

/** @param {number} id @returns {CullSourcePhoto | null} */
function cullSourcePhoto(id) {
  var photos = pipelineResults ? pipelineResults.photos : [];
  return photos.find(function(p) { return p.id === id; }) || null;
}

/** Every photo on screen under every species, so a keeper is found whichever species lists it. */
function cullAllDisplayPhotos() {
  return (cullData ? cullData.species_groups : []).flatMap(function(sg) {
    return cullGroupsForSpecies(sg).flatMap(function(g) { return g.photos; });
  });
}

/**
 * The photo a reject loses to: the reference it was compared against in
 * Compare (under whichever species the comparison was made), else the best
 * kept photo of its scene group (the same encounter), else the species'
 * reference favorite. Null when nothing in the species is kept, so the
 * reject is recorded without a keeper.
 *
 * @param {CullDisplayPhoto} photo @param {CullSceneGroup} group @param {CullSpeciesGroup} sg
 * @returns {CullKeeperChoice | null}
 */
function resolveCullKeeper(photo, group, sg) {
  var chosen = cullRejectKeepers[photo.photo_id];
  if (chosen) {
    var ref = cullAllDisplayPhotos().find(function(p) { return p.photo_id === chosen && p.action === 'keep'; });
    if (ref) return {keeper: ref, scope: 'compared'};
  }
  var best = group.photos.filter(function(p) { return p.action === 'keep' && p.photo_id !== photo.photo_id; })
    .sort(function(a, b) { return b.quality - a.quality || a.photo_id - b.photo_id; })[0];
  if (best) return {keeper: best, scope: 'encounter'};
  var favorites = cullFavoritesForSpecies(sg);
  var favorite = favorites.find(function(p) { return p.photo_id === cullFavoriteReferences[sg.species]; }) || favorites[0];
  if (favorite && favorite.photo_id !== photo.photo_id) return {keeper: favorite, scope: 'species'};
  return null;
}

/**
 * Whether the scorer put both photos in one encounter.
 *
 * @param {number} photoId @param {number} keeperId
 */
function cullSameEncounter(photoId, keeperId) {
  return (pipelineResults ? pipelineResults.encounters : []).some(function(enc) {
    var ids = enc.photo_ids || [];
    return ids.indexOf(photoId) >= 0 && ids.indexOf(keeperId) >= 0;
  });
}

/**
 * The measurement a reject and its keeper share. Inside one encounter it is
 * the scorer's own evidence: the focus evidence (eye or subject Tenengrad)
 * when both were measured on the same basis, else the composite quality.
 * Both are relative to the encounter's frames (raw Tenengrad varies with
 * region, texture, exposure and scale; the quality is a rank), so across
 * encounters, or when the encounter evidence is missing, the record uses
 * the subject detail instead: Tenengrad at a fixed subject scale, measured
 * the same way for every photo, with each photo's cross-encounter quality
 * beside it. Null when neither applies.
 *
 * @param {number} photoId @param {number} keeperId @param {string} scope
 * @returns {CullComparison | null}
 */
function cullComparison(photoId, keeperId, scope) {
  var a = cullSourcePhoto(photoId);
  var b = cullSourcePhoto(keeperId);
  if (!a || !b) return null;
  if (cullSameEncounter(photoId, keeperId)) {
    var fa = a.focus_evidence;
    var fb = b.focus_evidence;
    if (fa && fb && fa.basis === fb.basis && typeof fa.measurement === 'number' && typeof fb.measurement === 'number') {
      return {
        basis: fa.basis, measurement_name: fa.measurement_name || 'Tenengrad',
        photo: Math.round(fa.measurement), keeper: Math.round(fb.measurement), scope: scope,
      };
    }
    if (typeof a.quality_composite === 'number' && typeof b.quality_composite === 'number') {
      return {basis: 'quality', measurement_name: 'quality rank in encounter',
        photo: a.quality_composite, keeper: b.quality_composite, scope: scope};
    }
  }
  var ca = a.comparable_quality;
  var cb = b.comparable_quality;
  if (ca && cb && typeof ca.detail === 'number' && typeof cb.detail === 'number') {
    return {
      basis: 'detail', measurement_name: ca.measurement_name, scale_px: ca.scale_px,
      photo: Math.round(ca.detail), keeper: Math.round(cb.detail), scope: scope,
      photo_quality: ca.score, keeper_quality: cb.score,
    };
  }
  return null;
}

/**
 * Orders two cards best first by one sortable key per photo, so a day
 * mixing measured and unmeasured photos still sorts transitively.
 * Measured photos (cross-encounter quality) come first in their own
 * comparable order; unmeasured photos follow in their encounter rank.
 * Mixing the two scales inside one comparator produces cycles — a
 * measured A can beat a measured B on comparable while an unmeasured
 * C beats A and loses to B on encounter rank — and ``Array.sort`` then
 * picks a different favorite depending on input order.
 *
 * @param {CullDisplayPhoto} a @param {CullDisplayPhoto} b
 */
function cullQualityOrder(a, b) {
  var ac = a.comparable;
  var bc = b.comparable;
  if (typeof ac === 'number' && typeof bc === 'number') {
    if (ac !== bc) return bc - ac;
  } else if (typeof ac === 'number' || typeof bc === 'number') {
    return typeof ac === 'number' ? -1 : 1;
  }
  return b.quality - a.quality || a.photo_id - b.photo_id;
}

/**
 * One line saying what a reject lost to and by what measure.
 *
 * @param {string} keeperName @param {CullComparison | null | undefined} comparison
 */
function cullKeeperText(keeperName, comparison) {
  var text = 'Instead of ' + keeperName;
  if (!comparison) return text + ' (no shared measurement)';
  var where = comparison.scope === 'compared' ? 'compared side by side'
    : comparison.basis === 'detail' ? 'across encounters' : 'same encounter';
  if (comparison.basis === 'detail') {
    return text + ' · subject detail ' + comparison.photo + ' vs ' + comparison.keeper +
      (comparison.scale_px ? ' at ' + comparison.scale_px + ' px' : '') + ' (' + where + ')';
  }
  var basis = comparison.basis === 'quality' ? comparison.measurement_name
    : comparison.basis + ' ' + comparison.measurement_name;
  return text + ' · ' + basis + ' ' + comparison.photo + ' vs ' + comparison.keeper + ' (' + where + ')';
}

/**
 * The card line for a photo's cross-encounter quality: the detail
 * measurement and the composite, or '' until the detail is measured.
 *
 * @param {number} photoId @returns {{text: string, title: string} | null}
 */
function cullDetailLine(photoId) {
  var source = cullSourcePhoto(photoId);
  var cq = source && source.comparable_quality;
  if (!cq || typeof cq.detail !== 'number') return null;
  return {
    text: 'Detail ' + Math.round(cq.detail).toLocaleString() + ' · quality ' + Math.round(cq.score * 100) + '% across encounters',
    title: cq.measurement_name + '. The quality combines that detail with the subject\'s exposure, framing and size; ' +
      'unlike the encounter rank it means the same for every photo, so it can be compared across outings.',
  };
}

/**
 * The keeper a reject card shows: the saved record for an applied reject,
 * else the keeper Apply would record now. ``photo_id`` is null for a
 * keeper deleted since.
 *
 * @param {CullDisplayPhoto} photo @param {CullSceneGroup} group @param {CullSpeciesGroup} sg
 * @returns {{photo_id: number | null, name: string, text: string, saved: boolean} | null}
 */
function cullCardKeeper(photo, group, sg) {
  var source = cullSourcePhoto(photo.photo_id);
  if (photo.decision_source !== 'manual' && source && source.reject_keeper) {
    var saved = source.reject_keeper;
    // A deleted keeper's id may belong to a newer photo now, so no thumbnail.
    if (!saved.filename) {
      return {photo_id: null, name: 'a photo since deleted',
        text: cullKeeperText('a photo since deleted', saved.comparison), saved: true};
    }
    // A keeper this workspace cannot see has no thumbnail to show either.
    return {photo_id: saved.visible === false ? null : saved.photo_id, name: saved.filename,
      text: cullKeeperText(saved.filename, saved.comparison), saved: true};
  }
  var choice = resolveCullKeeper(photo, group, sg);
  if (!choice) return null;
  var comparison = cullComparison(photo.photo_id, choice.keeper.photo_id, choice.scope);
  return {photo_id: choice.keeper.photo_id, name: choice.keeper.filename,
    text: cullKeeperText(choice.keeper.filename, comparison), saved: false};
}

/**
 * The relational rejects Apply records: every reject you decided this
 * session, with the keeper it loses to (null when nothing is kept to
 * compare against), once per photo. A reject already saved is sent again
 * only as a flag; the server keeps the record it has, so a later Apply
 * never rewrites what an earlier one said it lost to.
 *
 * @param {CullData} data @returns {CullRejection[]}
 */
function cullRejectionsPayload(data) {
  /** @type {CullRejection[]} */
  var out = [];
  var seen = new Set();
  data.species_groups.forEach(function(sg) {
    (sg.scene_groups || sg.pose_groups || []).forEach(function(group) {
      group.photos.forEach(function(p) {
        if (p.action !== 'reject' || p.decision_source !== 'manual' || seen.has(p.photo_id)) return;
        seen.add(p.photo_id);
        var choice = resolveCullKeeper(p, group, sg);
        out.push(choice
          ? {photo_id: p.photo_id, keeper_photo_id: choice.keeper.photo_id,
            comparison: cullComparison(p.photo_id, choice.keeper.photo_id, choice.scope)}
          : {photo_id: p.photo_id, keeper_photo_id: null, comparison: null});
      });
    });
  });
  return out;
}

/** Fetch the saved rejects whose keeper is gone or rejected, and show the banner. */
async function loadCullOrphans() {
  try {
    var data = /** @type {{orphans?: CullOrphan[]}} */ (await safeFetch('/api/culling/orphans', {}, {toast: false}));
    cullOrphans = data && data.orphans || [];
  } catch (e) {
    cullOrphans = [];
  }
  renderCullOrphans();
}

function renderCullOrphans() {
  var banner = document.getElementById('cullOrphans');
  if (!banner) return;
  var count = cullOrphans.length;
  banner.hidden = !count;
  var text = /** @type {HTMLElement} */ (document.getElementById('cullOrphansText'));
  if (text) {
    text.textContent = count + (count === 1 ? ' rejected photo lost the photo it was rejected for.' : ' rejected photos lost the photo they were rejected for.');
  }
}

/** @param {CullOrphan} orphan */
function cullOrphanReason(orphan) {
  var keeper = orphan.keeper_filename || 'a photo since deleted';
  var why = orphan.orphan_reason === 'keeper_deleted' ? 'that photo was deleted' : 'that photo is now rejected too';
  return cullKeeperText(keeper, orphan.comparison) + ' — ' + why + '.';
}

/** List the orphaned rejects so each can go back to undecided. */
function reviewCullOrphans() {
  var list = /** @type {HTMLElement} */ (document.getElementById('cullOrphanPhotos'));
  list.replaceChildren();
  cullOrphans.forEach(function(orphan) {
    var card = document.createElement('div');
    var img = document.createElement('img');
    img.src = '/thumbnails/' + orphan.photo_id + '.jpg'; img.alt = orphan.filename;
    var name = document.createElement('div'); name.textContent = orphan.filename;
    var why = document.createElement('div'); why.className = 'cull-hint'; why.textContent = cullOrphanReason(orphan);
    var restore = document.createElement('button'); restore.textContent = 'Keep undecided';
    restore.addEventListener('click', function() { restoreCullOrphan(orphan.photo_id); });
    card.append(img, name, why, restore); list.appendChild(card);
  });
  var dialog = /** @type {HTMLDialogElement} */ (document.getElementById('cullOrphansDialog'));
  if (!dialog.open) dialog.showModal();
}

function closeCullOrphans() {
  var dialog = /** @type {HTMLDialogElement} */ (document.getElementById('cullOrphansDialog'));
  if (dialog && dialog.open) dialog.close();
}

/**
 * Clear one orphaned reject back to no flag (an explicit Review, one Undo
 * step) and refresh its card if it is on screen.
 *
 * @param {number} photoId
 */
async function restoreCullOrphan(photoId) {
  if (cullOperationPending()) return;
  cullBusy = true; updateCullApplyButton();
  try {
    await safeFetch('/api/culling/apply', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({keepers: [], rejects: [], unflag: [photoId]}),
    }, {toast: false});
    [pipelineResults, cullSourceResults].forEach(function(source) {
      (source && source.photos || []).forEach(function(photo) {
        if (photo.id !== photoId) return;
        photo.flag = 'none';
        photo.flag_confirmation_status = 'confirmed';
        delete photo.reject_keeper;
      });
    });
    delete cullManualDecisions[photoId];
    delete cullAppliedDecisions[photoId];
    cullOrphans = cullOrphans.filter(function(o) { return o.photo_id !== photoId; });
    if (!cullOrphans.length) closeCullOrphans();
    else reviewCullOrphans();
    renderCullOrphans();
    if (pipelineResults) rebuildCullDataFromPipeline();
  } catch (error) {
    showToast('Could not restore the photo: ' + (error instanceof Error ? error.message : String(error)), 'error');
  } finally {
    cullBusy = false; updateCullApplyButton();
  }
}
