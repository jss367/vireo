// Species identity comparison, conflict evidence, and warnings.
// Classic page script; shared globals are initialized before boot.js runs.

// Species-conflict review aid. These thresholds are deliberately conservative:
// a warning needs both a credible alternative and a meaningful margin over the
// current encounter/burst suggestion. The values are classifier confidence,
// not measured identification accuracy.
var SPECIES_CONFLICT_THRESHOLDS = {
  strongAlternative: 0.80,
  strongExpectedMax: 0.20,
  strongMargin: 0.60,
  possibleAlternative: 0.55,
  possibleExpectedMax: 0.35,
  possibleMargin: 0.25,
};

function normalizedSpeciesName(name) {
  var identity = typeof pipelineResults !== 'undefined' && pipelineResults && pipelineResults.species_identities && pipelineResults.species_identities[name];
  if (identity) return identity.key;
  return String(name || '').trim().toLocaleLowerCase().replace(/\s+/g, ' ');
}

function speciesDisplayKey(name) {
  return String(name || '').trim().toLocaleLowerCase().replace(/\s+/g, ' ');
}

// A "name:" key is the resolver's way of saying it could not identify the
// species at all — the key is the display text itself, not evidence about a
// taxon. "taxon:"/"scientific:" keys are evidence.
function speciesKeyIsUnidentified(key) {
  return String(key || '').indexOf('name:') === 0;
}

function speciesCandidateForReviewUnit(enc, burst) {
  var override = burst && burst.species_override;
  if (override && override.species) return override.species;
  if (enc && enc.confirmed_species) return enc.confirmed_species;
  if (enc && enc.species && enc.species[0]) return enc.species[0];
  return null;
}

function analyzePhotoSpeciesConflict(photo, expectedSpecies, normalizeSpecies, expectedIdentityKey) {
  normalizeSpecies = normalizeSpecies || normalizedSpeciesName;
  var expectedKey = expectedIdentityKey || normalizeSpecies(expectedSpecies);
  if (!expectedIdentityKey && photo) {
    var confirmed = (photo.confirmed_species_identities || []).filter(function(entry) { return entry.name === expectedSpecies; });
    if (confirmed.length === 1) expectedKey = confirmed[0].key;
  }
  var expectedDisplayKey = speciesDisplayKey(expectedSpecies);
  var top5 = (photo && photo.species_top5) || [];
  if (!expectedKey || top5.length === 0) return null;

  // A model may contribute several detections/crops. Retain its strongest
  // confidence for each species so repeated detections do not inflate support.
  var byModel = {};
  var displayNames = {};
  top5.forEach(function(entry) {
    if (!entry || entry.length < 2) return;
    var display = String(entry[0] || '').trim();
    var species = entry[3] || normalizeSpecies(display);
    // A prediction the resolver could not identify is bare text: a catalog
    // whose taxonomy never recorded its common-name provenance leaves every
    // predicted name at "name:mallard" while the confirmed keyword holds
    // "taxon:6930". Under one name with no identity of its own, such a
    // prediction supports the current suggestion instead of arguing against
    // it. An *identified* prediction is never folded, even when the expected
    // side is the ambiguous fallback key: a same-name taxon the source can
    // tell apart is exactly the homonym review must keep visible.
    if (species !== expectedKey && speciesKeyIsUnidentified(species) &&
        speciesDisplayKey(display) === expectedDisplayKey) {
      species = expectedKey;
    }
    displayNames[species] = display;
    var confidence = Number(entry[1]);
    var model = String(entry[2] || 'unknown');
    if (!species || !isFinite(confidence) || confidence < 0 || confidence > 1) return;
    if (!byModel[model]) byModel[model] = {};
    var prior = byModel[model][species];
    if (prior == null || confidence > prior) byModel[model][species] = confidence;
  });
  var models = Object.keys(byModel);
  if (models.length === 0) return null;

  var speciesNames = {};
  models.forEach(function(model) {
    Object.keys(byModel[model]).forEach(function(species) {
      if (species !== expectedKey) speciesNames[species] = true;
    });
  });

  function averageSupport(speciesKey) {
    var total = 0;
    models.forEach(function(model) {
      var best = 0;
      Object.keys(byModel[model]).forEach(function(species) {
        if (species === speciesKey) {
          best = Math.max(best, byModel[model][species]);
        }
      });
      total += best;
    });
    return total / models.length;
  }

  var expectedSupport = averageSupport(expectedKey);
  var alternative = null;
  var alternativeSupport = 0;
  Object.keys(speciesNames).forEach(function(species) {
    var support = averageSupport(species);
    if (support > alternativeSupport) {
      alternative = species;
      alternativeSupport = support;
    }
  });
  if (!alternative) return {
    expectedSpecies: expectedSpecies,
    expectedSupport: expectedSupport,
    modelCount: models.length,
    severity: null,
  };

  var alternativeKey = alternative;
  var alternativeModelWins = 0;
  models.forEach(function(model) {
    var topSpecies = null;
    var topConfidence = -1;
    Object.keys(byModel[model]).forEach(function(species) {
      var confidence = byModel[model][species];
      if (confidence > topConfidence) {
        topSpecies = species;
        topConfidence = confidence;
      }
    });
    if (topSpecies === alternativeKey) alternativeModelWins++;
  });

  var margin = alternativeSupport - expectedSupport;
  var enoughModelAgreement = alternativeModelWins >= Math.ceil(models.length / 2);
  var t = SPECIES_CONFLICT_THRESHOLDS;
  var severity = null;
  if (enoughModelAgreement &&
      alternativeSupport >= t.strongAlternative &&
      expectedSupport <= t.strongExpectedMax &&
      margin >= t.strongMargin) {
    severity = 'strong';
  } else if (enoughModelAgreement &&
             alternativeSupport >= t.possibleAlternative &&
             expectedSupport <= t.possibleExpectedMax &&
             margin >= t.possibleMargin) {
    severity = 'possible';
  }

  return {
    expectedSpecies: expectedSpecies,
    expectedSupport: expectedSupport,
    alternativeSpecies: displayNames[alternative],
    alternativeKey: alternativeKey,
    alternativeSupport: alternativeSupport,
    alternativeModelWins: alternativeModelWins,
    modelCount: models.length,
    margin: margin,
    severity: severity,
  };
}

function speciesCandidateKeyForReviewUnit(enc, burst, expected) {
  var override = burst && burst.species_override;
  if ((override && (override.confirmed || Array.isArray(override.species_list))) ||
      (!override && enc && enc.confirmed_species)) return null;
  var candidates = (burst && burst.species_predictions) || (enc && enc.species_predictions) || [];
  var winner = null;
  candidates.forEach(function(candidate) {
    if (candidate.species !== expected || !candidate.species_key) return;
    if (!winner || candidate.count * candidate.avg_confidence > winner.count * winner.avg_confidence) winner = candidate;
  });
  return winner && winner.species_key;
}

function buildSpeciesConflictEvidence(photoMap) {
  var evidence = {};
  // The same species names recur across predictions, models and photos.
  // Locale-aware normalization is expensive in WebKit; do it once per name
  // in this render, without caching decisions that could go stale after edits.
  var normalizedNames = new Map();
  function normalizeSpecies(name) {
    if (!normalizedNames.has(name)) normalizedNames.set(name, normalizedSpeciesName(name));
    return normalizedNames.get(name);
  }
  (pipelineResults.encounters || []).forEach(function(enc) {
    if (enc.bursts && enc.bursts.length > 0) {
      enc.bursts.forEach(function(burst) {
        var expected = speciesCandidateForReviewUnit(enc, burst);
        (burst.photo_ids || burst || []).forEach(function(pid) {
          var photo = photoMap[pid];
          if (photo) evidence[pid] = analyzePhotoSpeciesConflict(photo, expected, normalizeSpecies, speciesCandidateKeyForReviewUnit(enc, burst, expected));
        });
      });
    } else {
      var expected = speciesCandidateForReviewUnit(enc, null);
      (enc.photo_ids || []).forEach(function(pid) {
        var photo = photoMap[pid];
        if (photo) evidence[pid] = analyzePhotoSpeciesConflict(photo, expected, normalizeSpecies, speciesCandidateKeyForReviewUnit(enc, null, expected));
      });
    }
  });
  return evidence;
}

function formatSpeciesConfidence(value) {
  return Math.round((Number(value) || 0) * 100) + '%';
}

function speciesConflictTitle(issue) {
  if (!issue || !issue.severity) return '';
  var strength = issue.severity === 'strong' ? 'Strong' : 'Possible';
  var classifiers = issue.modelCount + ' classifier' + (issue.modelCount === 1 ? '' : 's');
  return strength + ' classification conflict: ' + classifiers +
    (issue.modelCount === 1 ? ' averages ' : ' average ') +
    issue.alternativeSpecies + ' at ' + formatSpeciesConfidence(issue.alternativeSupport) +
    ', while ' + issue.expectedSpecies + ' averages ' +
    formatSpeciesConfidence(issue.expectedSupport) + '. Click to inspect this photo and its review group.';
}

function summarizeSpeciesConflicts(photoIds, evidence) {
  var classifiedCount = 0;
  var issues = [];
  (photoIds || []).forEach(function(pid) {
    var item = evidence[pid];
    if (item) classifiedCount++;
    if (item && item.severity) issues.push(item);
  });
  if (issues.length === 0) return null;

  var groups = {};
  issues.forEach(function(issue) {
    var key = issue.alternativeKey || normalizedSpeciesName(issue.alternativeSpecies);
    if (!groups[key]) groups[key] = {species: issue.alternativeSpecies, count: 0, strong: 0};
    groups[key].count++;
    if (issue.severity === 'strong') groups[key].strong++;
  });
  var dominant = null;
  Object.keys(groups).forEach(function(key) {
    var group = groups[key];
    if (!dominant || group.count > dominant.count ||
        (group.count === dominant.count && group.strong > dominant.strong)) {
      dominant = group;
    }
  });
  return {
    issueCount: issues.length,
    classifiedCount: classifiedCount,
    dominantSpecies: dominant.species,
    dominantCount: dominant.count,
    severity: dominant.strong > 0 ? 'strong' : 'possible',
  };
}

function renderEncounterSpeciesConflict(enc, evidence, hiddenIds) {
  // Match the set of photos that renderResults() will actually draw for this
  // encounter: when hide-confirmed is on, confirmed bursts are skipped, so a
  // conflict living only in a confirmed burst shouldn't be tallied here (the
  // SPECIES_CONFLICT count already filters the same way).
  var ids = enc.photo_ids || [];
  if (hiddenIds && hiddenIds.size) {
    ids = ids.filter(function(pid) { return !hiddenIds.has(pid); });
  }
  var summary = summarizeSpeciesConflicts(ids, evidence);
  if (!summary) return '';
  var mostPhotosConflict = summary.classifiedCount > 0 &&
    summary.issueCount >= Math.ceil(summary.classifiedCount * 0.6) &&
    summary.dominantCount >= Math.ceil(summary.issueCount * 0.7);
  var title;
  if (mostPhotosConflict) {
    title = summary.issueCount + ' of ' + summary.classifiedCount +
      ' classified photos conflict with this encounter suggestion; most suggest ' +
      summary.dominantSpecies + '. The encounter species suggestion may be wrong.';
  } else {
    title = summary.issueCount + ' of ' + summary.classifiedCount +
      ' classified photos conflict with this encounter suggestion. They may belong in a different group.';
  }
  return '<span class="species-conflict-badge encounter-species-conflict ' + summary.severity +
    '" title="' + escapeAttr(title) + '">&#9888; ' + summary.issueCount +
    ' species conflict' + (summary.issueCount === 1 ? '' : 's') + '</span>';
}

function renderBurstSpeciesConflict(enc, encIdx, burst, burstIdx, evidence) {
  var ids = burst.photo_ids || burst || [];
  var summary = summarizeSpeciesConflicts(ids, evidence);
  // A burst marker should mean the burst itself has a consistent conflict,
  // not merely that one unusual frame inside a long burst needs photo review.
  if (!summary || ids.length < 2 || summary.dominantCount < 2 ||
      summary.dominantCount < Math.ceil(summary.classifiedCount / 2)) return '';
  var title = summary.dominantCount + ' of ' + summary.classifiedCount +
    ' classified photos in this burst suggest ' + summary.dominantSpecies +
    ' instead of the current species suggestion.';
  var html = '<span class="burst-conflict-actions">';
  html += '<span class="species-conflict-badge burst-species-conflict ' + summary.severity +
    '" title="' + escapeAttr(title) + '">&#9888; ' + summary.dominantCount +
    ' suggest ' + escapeHtml(summary.dominantSpecies) + '</span>';
  if (enc.bursts && enc.bursts.length > 1) {
    html += '<button class="burst-conflict-split" onclick="event.stopPropagation();detachBurst(' +
      encIdx + ',' + burstIdx + ')" title="Move this burst into its own encounter">Split burst</button>';
  }
  html += '</span>';
  return html;
}

function photoMatchesPipelineFilter(photo, issue) {
  if (activeFilter === 'all') return true;
  if (activeFilter === 'SPECIES_CONFLICT') return !!(issue && issue.severity);
  return photo.label === activeFilter;
}

function encounterHasSpeciesSuggestion(enc) {
  return !!(enc && (enc.confirmed_species || (enc.species && enc.species[0])));
}
