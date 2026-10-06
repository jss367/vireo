// Taxonomic group and identification-level filters, sorting, and the list controls.
// Classic page script; load boot.js after all definitions.

var debounceTimer = null;

function replaceSelectOptions(select, options) {
  var previous = select.value;
  select.innerHTML = options.map(function(option) {
    return '<option value="' + escapeAttr(option.value) + '">'
      + escapeAttr(option.label) + '</option>';
  }).join('');
  if (options.some(function(option) { return option.value === previous; })) {
    select.value = previous;
  }
}

function populateLifeListTaxonomyFilters() {
  var entries = (currentData && currentData.species) || [];
  var classes = {};
  var hasUnknownClass = false;
  var hasUnknownRank = false;

  entries.forEach(function(entry) {
    var taxonomicClass = entry.taxonomic_class;
    if (taxonomicClass && taxonomicClass.id != null) {
      classes[String(taxonomicClass.id)] =
        taxonomicClass.common_name || taxonomicClass.name;
    } else {
      hasUnknownClass = true;
    }
    if (!entry.taxon_rank) {
      hasUnknownRank = true;
    }
  });

  var groupOptions = [{ value: 'all', label: 'All groups' }];
  Object.keys(classes).sort(function(a, b) {
    return classes[a].localeCompare(classes[b]);
  }).forEach(function(id) {
    groupOptions.push({ value: id, label: classes[id] });
  });
  if (hasUnknownClass) {
    groupOptions.push({ value: 'unknown', label: 'Unmatched / unknown' });
  }
  replaceSelectOptions(document.getElementById('taxonomicGroupSelect'), groupOptions);

  var rankLabels = {
    species: 'Species only',
    genus: 'Genus',
    family: 'Family',
    order: 'Order',
    class: 'Class',
    phylum: 'Phylum',
    kingdom: 'Kingdom'
  };
  var rankOptions = [{ value: 'all', label: 'All levels' }];
  Object.keys(rankLabels).forEach(function(rank) {
    rankOptions.push({ value: rank, label: rankLabels[rank] });
  });
  if (hasUnknownRank) {
    rankOptions.push({ value: 'unknown', label: 'Unmatched / unknown' });
  }
  replaceSelectOptions(document.getElementById('identificationRankSelect'), rankOptions);
}

function lifeListAlphabeticalName(name) {
  // In an English-oriented common-name list, a leading Hawaiian ʻokina is
  // ignored for alphabetization even though it remains part of the displayed
  // spelling. Include common apostrophe substitutes so imported variants sort
  // consistently with the proper U+02BB character.
  return String(name || '').replace(/^[\u0027\u0060\u00B4\u02B9\u02BB\u02BC\u2018\u2019\u201B]+/, '');
}

function compareLifeListSpeciesAlphabetically(a, b) {
  var aName = String(a.species || '');
  var bName = String(b.species || '');
  return lifeListAlphabeticalName(aName).localeCompare(lifeListAlphabeticalName(bName))
    || aName.localeCompare(bName);
}

function sortedSpecies() {
  var sort = document.getElementById('sortSelect').value;
  var list = (currentData.species || []).slice();
  list.sort(function(a, b) {
    if (sort === 'alpha') return compareLifeListSpeciesAlphabetically(a, b);
    if (sort === 'most-photos') return b.photo_count - a.photo_count;
    if (sort === 'life-order') return a.number - b.number;
    // 'newest' — by first_seen desc, undated species last so they
    // don't masquerade as the newest lifers (server gives them the highest
    // life-list numbers for stable numbering, so reversing number is wrong).
    var af = a.first_seen, bf = b.first_seen;
    if (!af && !bf) return compareLifeListSpeciesAlphabetically(a, b);
    if (!af) return 1;
    if (!bf) return -1;
    if (af < bf) return 1;
    if (af > bf) return -1;
    return compareLifeListSpeciesAlphabetically(a, b);
  });
  return list;
}

function debounceRender() {
  clearTimeout(debounceTimer);
  debounceTimer = setTimeout(render, 150);
}

function bindLifeListControls() {
  document.getElementById('speciesSearch').addEventListener('input', debounceRender);
  document.getElementById('taxonomicGroupSelect').addEventListener('change', render);
  document.getElementById('identificationRankSelect').addEventListener('change', render);
  document.getElementById('sortSelect').addEventListener('change', function() {
    if (currentData) render();
  });
  document.getElementById('renumberView').addEventListener('change', function() {
    if (currentData) render();
  });
}
