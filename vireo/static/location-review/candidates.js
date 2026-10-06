// Location candidates: place types, merging, grouping, filters, and selection.
// Classic page script; load boot.js after all definitions.
'use strict';

var NATURAL_TYPES = {
  park: 'Park', state_park: 'State park', national_park: 'National park',
  wildlife_refuge: 'Wildlife refuge', nature_preserve: 'Nature preserve',
  natural_feature: 'Natural feature', hiking_area: 'Hiking area', campground: 'Campground',
  rv_park: 'RV park',
  scenic_spot: 'Scenic spot',
};

var AREA_TYPES = {
  neighborhood: {label: 'Neighborhood', scope: 'local'},
  colloquial_area: {label: 'Local area', scope: 'local'},
  sublocality_level_5: {label: 'Local area', scope: 'local'},
  sublocality_level_4: {label: 'Local area', scope: 'local'},
  sublocality_level_3: {label: 'Local area', scope: 'local'},
  sublocality_level_2: {label: 'Local area', scope: 'local'},
  sublocality_level_1: {label: 'Local area', scope: 'local'},
  sublocality: {label: 'Local area', scope: 'local'},
  postal_town: {label: 'Town', scope: 'local'},
  locality: {label: 'City or town', scope: 'local'},
  administrative_area_level_3: {label: 'District or municipality', scope: 'local'},
  administrative_area_level_2: {label: 'County or region', scope: 'broad'},
  administrative_area_level_1: {label: 'State or province', scope: 'broad'},
  country: {label: 'Country', scope: 'broad'},
};

var PLACE_TYPES = {
  tourist_attraction: 'Landmark', museum: 'Museum', place_of_worship: 'Place of worship',
  lodging: 'Lodging', restaurant: 'Restaurant', store: 'Business', establishment: 'Place',
};

function candidateKey(choice) {
  if (!choice) return '';
  if (choice.place_id) return 'place:' + choice.place_id;
  if (choice.kind === 'keyword') return 'keyword:' + choice.keyword_id;
  return 'custom:' + choice.name;
}

function typeDetails(types) {
  types = Array.isArray(types) ? types : [];
  for (var index = 0; index < types.length; index += 1) {
    if (AREA_TYPES[types[index]]) {
      return {
        category: 'areas',
        type: types[index],
        label: AREA_TYPES[types[index]].label,
        areaScope: AREA_TYPES[types[index]].scope,
      };
    }
  }
  for (var naturalIndex = 0; naturalIndex < types.length; naturalIndex += 1) {
    if (NATURAL_TYPES[types[naturalIndex]]) {
      return {category: 'nature', type: types[naturalIndex], label: NATURAL_TYPES[types[naturalIndex]]};
    }
  }
  for (var placeIndex = 0; placeIndex < types.length; placeIndex += 1) {
    if (PLACE_TYPES[types[placeIndex]]) {
      return {category: 'places', type: types[placeIndex], label: PLACE_TYPES[types[placeIndex]]};
    }
  }
  return {category: 'places', type: types[0] || 'place', label: 'Place'};
}

function normalizeCandidate(choice) {
  if (!choice) return choice;
  if (choice.kind === 'custom') {
    choice.category = 'places';
    choice.type_label = choice.type_label || 'Custom friendly name';
    return choice;
  }
  if (choice.kind === 'keyword') {
    // Older saved keywords do not retain their original Google place types.
    // Treat them as named places while keeping source as a separate badge.
    choice.category = choice.category || 'places';
    choice.type_label = choice.type_label || 'Saved location';
    choice.previously_used = true;
    choice.category_is_fallback = true;
    return choice;
  }
  var details = typeDetails(choice.types);
  choice.category = choice.category || details.category;
  choice.place_type = choice.place_type || details.type;
  choice.type_label = choice.type_label || details.label;
  choice.area_scope = choice.area_scope || details.areaScope;
  return choice;
}

function candidateType(choice) {
  if (choice.kind === 'custom') {
    return {label: 'Custom friendly name', category: 'custom'};
  }
  if (choice.kind !== 'google' && choice.category_is_fallback) return null;
  var category = 'place';
  if (choice.category === 'nature') category = 'nature';
  else if (choice.category === 'areas') {
    if (choice.place_type === 'country') category = 'country';
    else category = choice.area_scope === 'broad' ? 'region' : 'locality';
  }
  return {label: choice.type_label || 'Place', category: category};
}

function candidateMeta(choice) {
  var parts = [];
  if (choice.kind === 'keyword') {
    parts.push(formatNumber(choice.photo_count) + (choice.photo_count === 1 ? ' photo' : ' photos'));
  }
  if (choice.relationship === 'at_photos') parts.push('At photo coordinates');
  else if (choice.distance_m != null) parts.push(formatDistance(choice.distance_m) + ' from photo center');
  if (choice.context) parts.push(choice.context);
  return parts.join(' · ');
}

function candidateCoordinates(choice) {
  if (!choice || choice.latitude == null || choice.longitude == null) return '';
  var latitude = Number(choice.latitude);
  var longitude = Number(choice.longitude);
  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) return '';
  return latitude.toFixed(6) + ', ' + longitude.toFixed(6);
}

function allCandidates() {
  var indexes = {};
  var values = [];
  var nearby = state.savedCandidates.concat(state.googleCandidates);
  nearby.sort(function(a, b) {
    var aDistance = parseFloat(a.distance_m);
    var bDistance = parseFloat(b.distance_m);
    var aHasDistance = Number.isFinite(aDistance);
    var bHasDistance = Number.isFinite(bDistance);
    if (aHasDistance && bHasDistance && aDistance !== bDistance) return aDistance - bDistance;
    if (aHasDistance !== bHasDistance) return aHasDistance ? -1 : 1;
    if (a.kind !== b.kind) return a.kind === 'keyword' ? -1 : 1;
    return String(a.name || '').localeCompare(String(b.name || ''));
  });
  (state.searchedCandidate ? [state.searchedCandidate] : []).concat(
    nearby,
    state.regionCandidates
  ).forEach(function(choice) {
    var key = candidateKey(choice);
    if (!key) return;
    if (indexes[key] != null) {
      var existing = values[indexes[key]];
      if (choice.previously_used) {
        existing.previously_used = true;
        existing.photo_count = choice.photo_count;
        existing.keyword_id = choice.keyword_id;
      }
      if (existing.category_is_fallback && !choice.category_is_fallback) {
        existing.category = choice.category;
        existing.place_type = choice.place_type;
        existing.type_label = choice.type_label;
        existing.area_scope = choice.area_scope;
        existing.category_is_fallback = false;
      }
      if (choice.relationship === 'at_photos') {
        existing.relationship = choice.relationship;
        existing.area_scope = choice.area_scope;
        existing.category = choice.category;
        existing.place_type = choice.place_type;
        existing.type_label = choice.type_label;
      }
      if ((!existing.address_components || !existing.address_components.length) && choice.address_components) {
        existing.address_components = choice.address_components;
      }
      return;
    }
    indexes[key] = values.length;
    values.push(choice);
  });
  return values;
}

function visibleCandidates() {
  var candidates = allCandidates();
  if (state.suggestionMode === 'recommended') return candidates;
  var selectedKey = candidateKey(state.selectedChoice);
  return candidates.filter(function(choice) {
    return choice.category === state.suggestionMode || (selectedKey && candidateKey(choice) === selectedKey);
  });
}

function candidateGroup(choice) {
  var selectedKey = candidateKey(state.selectedChoice);
  if (
    state.suggestionMode !== 'recommended'
    && selectedKey
    && candidateKey(choice) === selectedKey
    && choice.category !== state.suggestionMode
  ) return 'selected';
  if (choice.relationship === 'at_photos' && choice.area_scope === 'local') return 'at';
  if (choice.category === 'areas' && choice.area_scope === 'broad') return 'broader';
  return 'nearby';
}

function candidateGroupLabel(group) {
  return {
    selected: 'Selected location',
    at: 'At the photos',
    nearby: state.mode === 'time' ? 'Locations to assign' : state.suggestionMode === 'recommended' ? 'Nearby places' : '',
    broader: 'Broader areas',
  }[group] || '';
}

function selectChoice(choice) {
  if (state.isAssigning) return;
  var choiceKey = candidateKey(choice);
  if (hasPartialAssignmentProgress() && state.assignment.choiceKey !== choiceKey) {
    showToast('Finish or retry the pending assignment — ' +
      formatNumber(state.assignment.completed) + ' of ' +
      formatNumber(state.assignment.total) + ' photos already got “' +
      state.selectedChoice.name + '”. Reload to abandon.', 'error');
    return;
  }
  if (state.assignment && state.assignment.choiceKey !== choiceKey) {
    state.assignment = null;
    resetAssignmentProgress();
  }
  state.selectedChoice = choice;
  renderCandidates();
  renderCandidateMarkers();
  var assign = document.getElementById('locationReviewAssign');
  assign.disabled = false;
  if (state.assignment && state.assignment.choiceKey === choiceKey && state.assignment.completed) {
    assign.textContent = 'Retry ' + formatNumber(state.assignment.total - state.assignment.completed) + ' remaining';
  } else {
    assign.textContent = 'Assign “' + choice.name + '”';
  }
}

function renderCandidates() {
  var container = document.getElementById('locationReviewCandidates');
  var candidates = visibleCandidates();
  var setupPrompt = '';
  if (state.googleMapsUnavailableReason === 'missing_key') {
    setupPrompt = '<div class="location-review-setup-prompt">' +
      '<strong>' + (state.mode === 'time' ? 'Search places with Google Maps' : 'Enable nearby place suggestions') + '</strong>' +
      '<span>Add a Google Maps API key to search named places. Previously used locations and custom names still work without one.</span>' +
      '<a class="location-review-button" href="/settings#google-maps">Open Settings</a></div>';
  } else if (state.googleMapsUnavailableReason === 'load_failed') {
    setupPrompt = '<div class="location-review-setup-prompt">' +
      '<strong>Google Maps could not be loaded</strong>' +
      '<span>Check the API key and enabled Google Maps APIs in Settings. Previously used locations and custom names are still available.</span>' +
      '<a class="location-review-button" href="/settings#google-maps">Check Settings</a></div>';
  }
  if (!candidates.length) {
    container.innerHTML = setupPrompt || '<div class="location-review-loading">' +
      (state.mode === 'time' ? 'Search for a place or enter a custom location name.' : 'No nearby named places found. Search the map or enter a custom friendly name.') + '</div>';
    return;
  }
  var groupedByName = {selected: [], at: [], nearby: [], broader: []};
  candidates.forEach(function(choice, index) {
    var group = candidateGroup(choice);
    groupedByName[group].push({choice: choice, index: index});
  });
  var grouped = ['selected', 'at', 'nearby', 'broader'].map(function(name) {
    return {name: name, choices: groupedByName[name]};
  }).filter(function(group) { return group.choices.length; });
  container.innerHTML = setupPrompt + grouped.map(function(group) {
    var label = candidateGroupLabel(group.name);
    return '<div class="location-review-candidate-group" data-candidate-group="' + group.name + '">' +
      (label ? '<div class="location-review-candidate-group-title">' + escapeHtml(label) + '</div>' : '') +
      group.choices.map(function(item) {
        var choice = item.choice;
        var index = item.index;
        var selected = candidateKey(choice) === candidateKey(state.selectedChoice) ? ' selected' : '';
        var badge = choice.previously_used
          ? '<span class="location-review-candidate-badge">Previously used</span>' : '';
        var type = candidateType(choice);
        var typeBadge = type
          ? '<span class="location-review-candidate-type location-review-candidate-type--' + type.category + '">' +
            escapeHtml(type.label) + '</span>' : '';
        var meta = candidateMeta(choice);
        var coordinates = state.includeCoordinates ? candidateCoordinates(choice) : '';
        return '<button class="location-review-candidate' + selected + '" type="button" data-candidate-index="' + index + '">' +
          '<span class="location-review-radio"></span><span>' +
          '<span class="location-review-candidate-heading">' +
          '<span class="location-review-candidate-name">' + escapeHtml(choice.name) + '</span>' + badge + '</span>' +
          '<span class="location-review-candidate-meta">' + typeBadge +
          (meta ? '<span class="location-review-candidate-detail">' + escapeHtml(meta) + '</span>' : '') + '</span>' +
          (coordinates ? '<span class="location-review-candidate-coordinates">' + escapeHtml(coordinates) + '</span>' : '') +
          '</span></button>';
      }).join('') + '</div>';
  }).join('');
  container.querySelectorAll('[data-candidate-index]').forEach(function(button) {
    button.addEventListener('click', function() {
      selectChoice(candidates[parseInt(button.dataset.candidateIndex, 10)]);
    });
  });
}

function updateSuggestionModeControls() {
  document.querySelectorAll('[data-suggestion-mode]').forEach(function(button) {
    var active = button.dataset.suggestionMode === state.suggestionMode;
    button.classList.toggle('active', active);
    button.setAttribute('aria-pressed', active ? 'true' : 'false');
  });
}

function setSuggestionMode(mode) {
  // Guard against programmatic calls that can bypass the disabled
  // attribute and detach a pending batch assignment from its choice.
  if (state.isAssigning || hasPartialAssignmentProgress()) return;
  if (
    ['recommended', 'areas', 'nature', 'places'].indexOf(mode) === -1
    || mode === state.suggestionMode
  ) return;
  var group = currentGroup();
  if (!group) return;
  state.suggestionMode = mode;
  updateSuggestionModeControls();
  finishSuggestionRender(state.renderToken);
}
