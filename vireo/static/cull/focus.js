// The focused scene group: the list of group references, moving focus with
// the arrow keys, and scrolling it into view.
// Classic page script.

/** @param {CullSpeciesGroup | null | undefined} speciesGroup @returns {CullSceneGroup[]} */
function cullGroupsForSpecies(speciesGroup) {
  return (speciesGroup && (speciesGroup.scene_groups || speciesGroup.pose_groups)) || [];
}

function cullGroupRefs() {
  /** @type {CullGroupRef[]} */
  var refs = [];
  if (!cullData || !cullData.species_groups) return refs;
  cullData.species_groups.forEach(function(sg, si) {
    cullGroupsForSpecies(sg).forEach(function(_group, /** @type {number} */ gi) {
      refs.push({ speciesIdx: si, groupIdx: gi });
    });
  });
  return refs;
}

/** @param {CullGroupRef} ref */
function cullGroupMatches(ref) {
  return focusedCullGroup.speciesIdx === ref.speciesIdx &&
    focusedCullGroup.groupIdx === ref.groupIdx;
}

/** @param {CullGroupRef} ref */
function cullGroupSelector(ref) {
  return '.pose-group[data-species-index="' + ref.speciesIdx + '"][data-group-index="' + ref.groupIdx + '"]';
}

/** @param {CullGroupRef[]=} refs @returns {CullGroupRef[]} */
function normalizeFocusedCullGroup(refs) {
  refs = refs || cullGroupRefs();
  if (!refs.length) {
    focusedCullGroup = { speciesIdx: null, groupIdx: null };
    return refs;
  }
  var exists = refs.some(cullGroupMatches);
  if (!exists) {
    var first = /** @type {CullGroupRef} */ (refs[0]);
    focusedCullGroup = { speciesIdx: first.speciesIdx, groupIdx: first.groupIdx };
  }
  return refs;
}

function scrollFocusedCullGroup() {
  if (focusedCullGroup.speciesIdx == null || focusedCullGroup.groupIdx == null) return;
  var el = document.querySelector(cullGroupSelector(focusedCullGroup));
  if (el) el.scrollIntoView({ block: 'nearest', inline: 'nearest' });
}

/** @param {CullGroupRef | undefined} ref @param {boolean=} shouldScroll */
function focusCullGroup(ref, shouldScroll) {
  if (!ref) return false;
  focusedCullGroup = { speciesIdx: ref.speciesIdx, groupIdx: ref.groupIdx };
  if (ref.speciesIdx != null) expandedSpecies[ref.speciesIdx] = true;
  renderCulling();
  if (shouldScroll !== false) {
    window.requestAnimationFrame(scrollFocusedCullGroup);
  }
  return true;
}

/** @param {number} delta */
function moveFocusedCullGroup(delta) {
  var refs = normalizeFocusedCullGroup();
  if (!refs.length) return false;
  var current = refs.findIndex(cullGroupMatches);
  if (current < 0) current = 0;
  var next = Math.max(0, Math.min(refs.length - 1, current + delta));
  if (next === current) return false;
  return focusCullGroup(refs[next], true);
}

/** @param {EventTarget | null} target */
function cullKeyTargetIsEditable(target) {
  if (!(target instanceof Element)) return false;
  if (target.closest && target.closest('input, textarea, select, button')) return true;
  return target instanceof HTMLElement && target.isContentEditable;
}
