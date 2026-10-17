// What Apply will do: the flag and species set differences, and the Apply
// and Apply & Next button labels, titles and enabled state.
// Classic page script; load boot.js after all definitions.

function rapidPlural(n, one, many) {
  return n === 1 ? one : (many || one + 's');
}

// Set difference between the burst's confirmed species and the typed list.
// An empty field never removes anything: clearing the input is treated as
// "no species work", not "strip every species", so an accidental clear can't
// untag a burst.
function rapidSpeciesSetDiff(targetList, currentList) {
  var currentKeys = {};
  currentList.forEach(function(name) { currentKeys[speciesKey(name)] = true; });
  var targetKeys = {};
  targetList.forEach(function(name) { targetKeys[speciesKey(name)] = true; });
  if (!targetList.length) return { added: [], removed: [], kept: [] };
  return {
    added: targetList.filter(function(name) { return !currentKeys[speciesKey(name)]; }),
    removed: currentList.filter(function(name) { return !targetKeys[speciesKey(name)]; }),
    kept: targetList.filter(function(name) { return !!currentKeys[speciesKey(name)]; }),
  };
}

function rapidComputeApplyDiff() {
  var speciesEl = document.getElementById('speciesInput');
  var targetList = parseSpeciesList(speciesEl ? speciesEl.value : '');
  var species = joinSpecies(targetList);
  var initialList = parseSpeciesList(rapid.initialSpecies || '');
  var setDiff = rapidSpeciesSetDiff(targetList, initialList);
  var diff = {
    flagNew: 0,
    rejectNew: 0,
    clearNew: 0,
    tagNew: 0,
    speciesChanged: targetList.length > 0 && !sameSpeciesSet(targetList, initialList),
    species: species,
    speciesList: targetList,
    // Dropped species with no new species to pair against: these are bare
    // removals (a paired drop is a replace, already covered by the
    // "set confirmed species" wording).
    removedSpecies: setDiff.removed.slice(setDiff.added.length),
  };
  if (!rapid.items) return diff;

  rapid.items.forEach(function(p) {
    var oldFlag = rapid.initialFlags[p.id] || 'none';
    var newFlag = rapid.picks.has(p.id) ? 'flagged' : rapid.rejects.has(p.id) ? 'rejected' : 'none';
    if (newFlag !== oldFlag) {
      if (newFlag === 'flagged') diff.flagNew++;
      else if (newFlag === 'rejected') diff.rejectNew++;
      else diff.clearNew++;
    }
    // Apply tags EVERY frame in the burst with each species keyword (matching
    // the grid / the persisted pipeline decision), not just picks. Count all
    // current-burst members missing at least one of the typed species.
    var missing = targetList.some(function(name) {
      return !rapidFrameHasKeyword(p.id, name, targetList);
    });
    if (targetList.length && missing) diff.tagNew++;
  });
  return diff;
}

function rapidApplyLabelParts(diff) {
  var parts = [];
  if (diff.flagNew > 0) parts.push('Flag ' + diff.flagNew);
  if (diff.rejectNew > 0) parts.push('Reject ' + diff.rejectNew);
  if (diff.clearNew > 0) parts.push('Clear ' + diff.clearNew);
  if (diff.speciesChanged) parts.push('Set species');
  if (diff.tagNew > 0) parts.push('Tag ' + diff.tagNew);
  return parts;
}

function rapidApplyTitle(diff, openNext) {
  var actions = [];
  if (diff.flagNew > 0) actions.push('flag ' + diff.flagNew + ' ' + rapidPlural(diff.flagNew, 'photo') + ' as ' + rapidPlural(diff.flagNew, 'a pick', 'picks'));
  if (diff.rejectNew > 0) actions.push('reject ' + diff.rejectNew + ' ' + rapidPlural(diff.rejectNew, 'photo'));
  if (diff.clearNew > 0) actions.push('clear flags on ' + diff.clearNew + ' ' + rapidPlural(diff.clearNew, 'photo'));
  if (diff.speciesChanged) actions.push('set confirmed species to "' + diff.species + '"');
  if (diff.removedSpecies && diff.removedSpecies.length) {
    var rmLabel = diff.removedSpecies.length > 1 ? 'species keywords' : 'species keyword';
    actions.push('remove ' + rmLabel + ' "' + joinSpecies(diff.removedSpecies) + '" from every burst frame');
  }
  if (diff.tagNew > 0) {
    var kwLabel = diff.speciesList && diff.speciesList.length > 1 ? 'species keywords' : 'species keyword';
    actions.push('add ' + kwLabel + ' "' + diff.species + '" to ' + diff.tagNew + ' ' + rapidPlural(diff.tagNew, 'burst frame'));
  }
  var text = actions.length ? ('Apply will ' + actions.join(', ') + '.') : 'Apply will make no database changes for this burst.';
  if (openNext) text += ' Then open the next burst.';
  return text;
}

function rapidApplyButtonText(prefix, diff) {
  var parts = rapidApplyLabelParts(diff);
  return parts.length ? (prefix + ': ' + parts.join(' · ')) : (prefix + ': no DB changes');
}

function renderApplyState() {
  var disabled = !rapid.seeded || rapid.applying || rapid.sessionIndex < 0;
  document.getElementById('undoBtn').disabled = disabled || rapid.history.length === 0;
  document.querySelectorAll('.decision-btn').forEach(function(btn) {
    btn.disabled = disabled || !currentPhoto();
  });
  var applyBtn = document.getElementById('applyBtn');
  var applyNextBtn = document.getElementById('applyNextBtn');
  var diff = rapidComputeApplyDiff();
  var waitingForState = !rapid.seeded && rapid.sessionIndex >= 0 && rapid.items && rapid.items.length && !rapid.loadError;
  var stateFailed = !rapid.seeded && rapid.loadError;
  applyBtn.disabled = disabled;
  applyNextBtn.disabled = disabled || rapid.sessionIndex >= rapid.sessions.length - 1;
  if (rapid.applying) {
    applyBtn.textContent = 'Applying...';
    applyNextBtn.textContent = 'Applying...';
    applyBtn.title = 'Applying changes to this burst.';
    applyNextBtn.title = 'Applying changes to this burst.';
  } else if (waitingForState) {
    applyBtn.textContent = 'Loading burst state...';
    applyNextBtn.textContent = 'Loading burst state...';
    applyBtn.title = 'Apply will be available after burst state loads.';
    applyNextBtn.title = 'Apply & Next will be available after burst state loads.';
  } else if (stateFailed) {
    applyBtn.textContent = 'Apply unavailable';
    applyNextBtn.textContent = 'Apply & Next unavailable';
    applyBtn.title = 'Apply is unavailable because burst state failed to load.';
    applyNextBtn.title = 'Apply & Next is unavailable because burst state failed to load.';
  } else {
    applyBtn.textContent = rapidApplyButtonText('Apply', diff);
    applyNextBtn.textContent = rapidApplyButtonText('Apply & Next', diff);
    applyBtn.title = rapidApplyTitle(diff, false);
    applyNextBtn.title = rapidApplyTitle(diff, true);
  }
  applyBtn.setAttribute('aria-label', applyBtn.title);
  applyNextBtn.setAttribute('aria-label', applyNextBtn.title);
}
