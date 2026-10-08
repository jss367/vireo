/* Browse: prediction panels for the detail and selection views.
   Classic script sharing page globals; browse.html loads the
   browse/*.js files in dependency order. */

/* ---------- Predictions panels ----------
   Browse shows what the classifier thinks, not just what has been committed.
   Unambiguous predictions are actionable here; anything needing Review's
   fuller UI stays visible but routes there instead of offering an Accept
   this panel cannot honestly carry out. */

// `effective_category` is the server's fresh comparison of this prediction
// against the CURRENT species keywords on the photo. `category` is only the
// classify-time snapshot, so a Robin keyword added AFTER a Sparrow prediction
// leaves the stored category unchanged; without the effective check Browse
// would offer a bare Accept that silently adds the conflicting species.
// The fresh comparison wins outright when the server could make one; the
// stored `category` is only the fallback for when it could not. ORing the two
// would make ambiguity a one-way ratchet — a photo whose conflicting keyword
// has since been removed would keep being routed to Review forever, naming a
// conflict that no longer exists. That is the same staleness the effective
// check exists to kill, pointing the other way.
function predictionIsAmbiguous(p) {
  var eff = p.effective_category;
  if (p.alternatives && p.alternatives.length) return true;
  if (eff) return eff === 'conflict' || eff === 'refinement' || eff === 'broader';
  return p.category === 'disagreement' || p.category === 'refinement';
}

function predictionAmbiguityReason(p) {
  var reasons = [];
  var altCount = (p.alternatives || []).length;
  if (altCount) {
    reasons.push(altCount + (altCount === 1 ? ' alternative' : ' alternatives'));
  }
  var existing = (p.existing_species || []).join(', ');
  // Same precedence as predictionIsAmbiguous: name the reason the fresh
  // comparison found, or the stored one only when there is no fresh one.
  var eff = p.effective_category;
  var isConflict = eff ? eff === 'conflict' : p.category === 'disagreement';
  var isRefinement = eff ? eff === 'refinement' : p.category === 'refinement';
  var isBroader = eff === 'broader';
  if (isConflict) {
    reasons.push(existing ? 'conflicts with keyworded ' + existing : 'conflicts with existing keywords');
  } else if (isRefinement) {
    reasons.push(existing ? 'refines keyworded ' + existing : 'refines an existing keyword');
  } else if (isBroader) {
    reasons.push(existing ? 'broader than keyworded ' + existing : 'broader than an existing keyword');
  }
  return reasons.join(' · ');
}

// An empty list has five different meanings. Collapsing them into one blank
// panel would tell the user "no prediction" when the truth might be "nothing
// has run yet" — see CORE_PHILOSOPHY.md, no black boxes.
//
// `classifier_ran` is checked before `detection_count` on purpose: once the
// classifier has run, the reason the panel is empty is a classifier fact, and
// "nothing detected" would be a stale explanation. `hiddenCount` splits the
// last case in two — blaming the threshold when the classifier produced no
// species at all would name a cause that never fired.
// A banner, not a row: it qualifies the whole list rather than describing one
// prediction. Only the `unlisted` verdict is shown, and deliberately so —
// `listed` would add reassuring chrome to the ordinary case, while
// `uncalibrated` and `unavailable` have nothing to report. Absence of a
// threshold is not a passing grade, so neither may be rendered as one.
//
// The banner covers the case where EVERY judged run failed. That is not the
// whole story: a photo can hold two species, and two models can disagree
// about one subject, so a single passing run flips the photo-level state to
// `listed` while a displayed prediction is still explicitly `unlisted`.
// `buildUnlistedRunIndex` below carries those failures down to the rows that
// produced them, so a passing run can never silence a failing one.
//
// The same `unlisted` gate also keeps the banner off a photo where one model
// failed its floor and another model ran with no calibrated floor at all:
// `match_confidence.summarize` degrades that photo to `uncalibrated`, because
// "every species below is not a match" would be asserting a verdict for a
// model nobody judged. The per-row warning still fires and names the model
// that actually failed.
function buildMatchBanner(matchState) {
  if (!matchState || matchState.state !== 'unlisted') return '';
  var lines = (matchState.assessments || [])
    .filter(function(a) { return a && a.state === 'unlisted' && a.explanation; })
    .map(function(a) {
      return '<div class="match-banner-detail">' + escapeHtml(a.model) + ': ' +
        escapeHtml(a.explanation) + '</div>';
    }).join('');
  var modelCount = matchState.judged_models || 0;
  return '<div class="match-banner">' +
    '<div class="match-banner-title">No label in your list matches this photo well</div>' +
    '<div class="match-banner-detail">' +
      'Every species below is the closest available label, not a match. ' +
      (modelCount === 1
        ? 'One model was judged.'
        : modelCount + ' models were judged and all agree.') +
      ' Consider widening the label list — the real species may not be one this list contains.' +
    '</div>' + lines +
  '</div>';
}

// (detection, model) -> the verdict of the run that produced that row, for
// runs positively judged as matching nothing in their label list. Only
// `unlisted` runs are indexed: `uncalibrated` and `unavailable` mean no
// verdict was reached, and a row must never be marked failing on the strength
// of a measurement nobody judged.
function buildUnlistedRunIndex(matchState) {
  var index = {};
  ((matchState && matchState.unlisted_runs) || []).forEach(function(run) {
    if (!run || run.state !== 'unlisted' || run.detection_id == null) return;
    index[run.detection_id + '|' + (run.classifier_model || '')] = run;
  });
  return index;
}

// Why this bucket carries a warning even though the photo as a whole was not
// flagged. Names the models that failed and how much of the bucket they
// account for, because the alternative — a bare "no good match" over a row
// pooling five detections of which one failed — would replace one misleading
// number with another.
function unlistedGroupReason(group) {
  var models = group.unlisted.map(function(run) {
    return run.classifier_model || run.model || 'this model';
  });
  var uniqueModels = models.filter(function(m, i) {
    return models.indexOf(m) === i;
  });
  // Counted over DETECTIONS, not over prediction rows. A bucket can hold
  // several rows from one detection (one per model, plus promoted
  // alternatives), so "3 of the 3 detections" computed from row counts would
  // be a number the photo does not contain.
  var total = Object.keys(group.detections).length;
  var failed = Object.keys(group.unlistedDetections).length;
  var scope = total === 1
    ? 'This detection'
    : (failed >= total
        ? 'Every detection behind this row'
        : failed + ' of the ' + total + ' detections behind this row');
  return scope + ' matched nothing in the label list ' +
    uniqueModels.join(' and ') + ' ran — the species above is the closest ' +
    'available label, not a match.';
}

function predictionEmptyMessage(state, hiddenCount) {
  if (!state) return 'No predictions for this photo.';
  if (!state.detector_ran) return 'Not yet classified — no detector has run on this photo.';
  if (state.classifier_ran) {
    // A third branch used to sit here, taking a count of buckets the
    // borrowed-confidence suppression had dropped and naming that as the
    // reason the panel was empty. Both the suppression and this branch are
    // retired: the only rows that could reach them were legacy
    // mixed-consensus bursts, which a since-retired one-shot repair cleared
    // and current classification never writes.
    // The two remaining branches are exhaustive again — with nothing
    // suppressed, an empty panel under a classifier that ran means either
    // the floor hid everything or there was nothing to hide.
    if (hiddenCount) return 'No species above threshold — every prediction is below your confidence floor.';
    return 'Classification ran and produced no species for this photo.';
  }
  if (!state.detection_count) return 'Nothing detected — the detector ran and found no animals.';
  return 'Not yet classified — detections found, but no classifier has run yet.';
}

function formatPredictionConfidence(conf) {
  if (conf == null) return 'confidence unknown';
  return Math.round(conf * 100) + '%';
}

// Mirrors Database.DECIDED_PREDICTION_STATUSES (vireo/db.py), which every
// prediction endpoint uses as its "already decided" precondition. The panel
// has to agree with it exactly: a row this list omits gets Accept and Reject
// buttons, and the endpoint behind those buttons answers 409 (single-row) or
// skips it as `already_decided` (batch) — a button that promises an action
// the server refuses is the black box CORE_PHILOSOPHY.md rules out. Named
// once here rather than spelled out at the comparison, so the panel's copy of
// the rule is findable from the backend's.
//
// `alternative` is deliberately absent, matching the backend: a runner-up is
// awaiting a decision, not carrying one. The panel never sees those as
// top-level rows anyway — /api/predictions nests them under their parent.
var PREDICTION_DECIDED_STATUSES = ['accepted', 'rejected', 'reviewed'];

function predictionIsDecided(p) {
  return PREDICTION_DECIDED_STATUSES.indexOf(p && p.status) >= 0;
}

// Why a decided row carries no buttons. `reviewed` in particular is not
// self-explanatory in Browse — it is written from ID Conflicts — so the tag
// says what it means rather than leaving a bare word where two buttons were.
function predictionStatusExplanation(status) {
  // "was confirmed", not "the keyword was added": accepting a prediction for
  // a photo that already carries the species writes no tag at all
  // (``accept_prediction`` returns ``changed_tag=false`` for it), and this
  // tooltip is rendered from the row's stored status — long after the accept,
  // in a session that never saw that flag. Naming the decision is true in
  // both cases; naming the write would be a guess dressed as a fact.
  if (status === 'accepted') return 'Accepted — this species was confirmed for the photo.';
  if (status === 'rejected') return 'Rejected — this prediction was dismissed.';
  if (status === 'reviewed') {
    return 'Marked reviewed in ID Conflicts — settled without accepting or ' +
      'rejecting, so there is nothing left to decide here.';
  }
  return 'This prediction has already been decided.';
}

// Fold only A-Z→a-z, leaving non-ASCII letters alone. Mirrors the backend's
// keyword_match_key (vireo/keyword_normalization.py) and SQLite's ASCII
// NOCASE. JS's Unicode-aware String.prototype.toLowerCase() folds "Éclair"
// to "éclair", so grouping predictions by toLowerCase() merges rows that the
// keywords table keeps as distinct species — the resulting Accept submits
// both ids and /api/predictions/batch-accept rejects the batch because they
// resolve to different keyword rows, leaving the row unactionable.
function asciiCaseFoldKey(name) {
  var s = String(name == null ? '' : name);
  var out = '';
  for (var i = 0; i < s.length; i++) {
    var c = s.charCodeAt(i);
    if (c >= 65 && c <= 90) out += String.fromCharCode(c + 32);
    else out += s.charAt(i);
  }
  return out;
}

async function loadDetailPredictions(photoId) {
  var list = document.getElementById('detailPredictions');
  if (!list) return;
  var request = Vireo.browse.panelRequests.detailPredictions.begin();
  list.innerHTML = '<div class="selection-empty">Loading predictions...</div>';
  try {
    var data = await safeFetch(
      '/api/predictions?photo_ids=' + encodeURIComponent(photoId), {}, { toast: false },
    );
    if (!request.isCurrent() || window._detailPhotoId !== photoId) return;
    renderDetailPredictions(data, photoId);
  } catch(e) {
    if (request.fail() && window._detailPhotoId === photoId) {
      list.innerHTML = '<div class="selection-empty">Could not load predictions.</div>';
    }
  }
}

function toggleDetailPredictions() {
  detailPredictionsExpanded = !detailPredictionsExpanded;
  if (_detailPredictionData) {
    renderDetailPredictions(_detailPredictionData.data, _detailPredictionData.photoId);
  }
}

// Every button this panel renders is wired through here, by one delegated
// listener on the panel container, and its markup carries integers only: an
// index into `detailPredictionGroups` and a photo id. See the comment on
// `detailPredictionGroups` for what interpolating a species name into an
// inline handler attribute did to Say's Phoebe.
function handleDetailPredictionClick(ev) {
  var btn = ev.target && ev.target.closest
    ? ev.target.closest('[data-prediction-action]') : null;
  if (!btn) return;
  var action = btn.getAttribute('data-prediction-action');
  if (action === 'toggle') { toggleDetailPredictions(); return; }
  if (action === 'review') {
    openPredictionInReview(Number(btn.getAttribute('data-photo-id')));
    return;
  }
  // The array is rebuilt on every render alongside the markup that indexes
  // into it, so a missing entry means the panel repainted under the click.
  // Doing nothing is the honest outcome: the row the user aimed at is gone.
  var group = detailPredictionGroups[
    Number(btn.getAttribute('data-prediction-group'))
  ];
  if (!group) return;
  if (action === 'accept') {
    acceptDetailPredictions(
      group.ids, detailPredictionGroupsPhotoId, group.species,
    );
  } else if (action === 'reject') {
    rejectDetailPredictions(group.ids, detailPredictionGroupsPhotoId);
  }
}

var detailPredictionsClickBound = false;

function renderDetailPredictions(data, photoId) {
  var list = document.getElementById('detailPredictions');
  if (!list) return;
  // The container is static in the template and only its children are
  // replaced, so one delegated listener outlives every repaint.
  if (!detailPredictionsClickBound) {
    detailPredictionsClickBound = true;
    list.addEventListener('click', handleDetailPredictionClick);
  }
  // Cleared before any early return: buttons from the previous render are
  // about to be replaced, and a leftover table would let a click resolve to
  // a row this render decided not to show.
  detailPredictionGroups = [];
  detailPredictionGroupsPhotoId = photoId;
  _detailPredictionData = { data: data, photoId: photoId };
  var state = (data.photo_states || {})[String(photoId)] || null;
  // Whether anything in the label list actually matched. This qualifies every
  // row below: a confidence is a rank within the list, so on its own it cannot
  // distinguish the right bird from the closest of a list that never contained
  // it. Rendered first, because reading the rows without it is the mistake.
  var matchState = (data.match_states || {})[String(photoId)] || null;
  var matchBanner = buildMatchBanner(matchState);
  // Per-row failures, for the rows the banner does not speak for. When the
  // banner rendered, every judged run on the photo failed and it has already
  // said so for all of them; repeating it per row would be noise. When it did
  // not, some run still may have failed — a second model, or a second
  // detection holding a different animal — and those rows are exactly the
  // ones that would otherwise show a 99% with no warning at all.
  var unlistedRuns = matchBanner ? {} : buildUnlistedRunIndex(matchState);
  var threshold = state && state.threshold ? state.threshold : 0;
  var rows = (data.predictions || []).filter(function(p) {
    return p.photo_id === photoId;
  });
  // Below-threshold rows are summarised rather than dropped: the user set the
  // floor, so they are entitled to know something is sitting under it.
  var visible = rows.filter(function(p) { return (p.confidence || 0) >= threshold; });
  var hidden = rows.length - visible.length;

  if (!visible.length) {
    list.innerHTML = matchBanner +
      '<div class="selection-empty">' +
      escapeHtml(predictionEmptyMessage(state, hidden)) + '</div>' +
      (hidden ? '<div class="prediction-below-threshold">' + hidden +
        (hidden === 1 ? ' prediction' : ' predictions') + ' below your confidence threshold (' +
        formatPredictionConfidence(threshold) + ').</div>' : '');
    return;
  }

  // One prediction row per detection means a photo with a dozen boxes of the
  // same bird lists that bird a dozen times. Group by species+status: the
  // question the panel answers is "what species is in this photo", and the
  // detection count is the interesting detail, not a reason to repeat.
  //
  // The species used for grouping and display is the one accept_prediction()
  // will actually apply — the backend exposes it as `consensus_species`. For
  // a non-grouped prediction that's just `p.species`; for a burst whose
  // frames disagree it's the individual-vote consensus. Grouping by the raw
  // per-frame `p.species` here would let a minority Sparrow frame surface
  // its own row with an Accept button that tags Robin.
  var groups = [];
  var groupByKey = {};
  visible.forEach(function(p) {
    // ``reviewed`` is a decision too — the user explicitly said "looked and
    // chose not to act" in Review. Grouping it as pending here would render
    // Accept/Reject buttons whose click would overwrite that decision and
    // record a history entry whose "previous" status of ``pending`` is a
    // fiction. Server guards refuse the flip (see _DECIDED_PREDICTION_STATUSES
    // in app.py), but the panel must not offer the action in the first place.
    var decided = predictionIsDecided(p);
    var displaySpecies = p.consensus_species || p.species || 'Unknown';
    var key = (p.consensus_species_key || asciiCaseFoldKey(displaySpecies)) + '|' + (decided ? p.status : 'pending');
    var g = groupByKey[key];
    if (!g) {
      g = groupByKey[key] = {
        species: displaySpecies, status: decided ? p.status : 'pending',
        decided: decided, ids: [], models: {}, ambiguous: [], confidence: null, count: 0,
        unlisted: [], detections: {}, unlistedDetections: {},
      };
      groups.push(g);
    }
    g.count++;
    g.ids.push(p.id);
    if (p.model) g.models[p.model] = true;
    // A bucket pools rows from several detections and models, so the failure
    // is recorded per contributing row and reported with its own count — "on
    // 3 detections" beside a warning that only applies to one of them would
    // be a new way of overstating the evidence.
    if (p.detection_id != null) g.detections[p.detection_id] = true;
    var failedRun = unlistedRuns[p.detection_id + '|' + (p.model || '')];
    if (failedRun) {
      g.unlistedDetections[p.detection_id] = true;
      if (g.unlisted.indexOf(failedRun) === -1) g.unlisted.push(failedRun);
    }
    // Credit ``p.confidence`` to the bucket only when the row's own raw
    // label agrees with the consensus. A Sparrow-labelled frame in a
    // Robin-majority burst is stored with ``p.species == "Sparrow"`` and
    // its 0.95 score is evidence for Sparrow, not Robin — pooling it here
    // would render "Robin · 95%" beside a bucket in which nothing scored
    // Robin at 95%, and let the sort float that bucket above genuine
    // Robin evidence. Mirrors the server-side aggregator's rule in
    // ``api_selection_prediction_suggestions``; a bucket with no
    // contributing row keeps ``confidence == null`` and renders as
    // "confidence unknown" via ``formatPredictionConfidence``.
    var matchesConsensus = p.species_key && p.consensus_species_key
      ? p.species_key === p.consensus_species_key
      : asciiCaseFoldKey(p.species || '') === asciiCaseFoldKey(g.species);
    if (matchesConsensus
        && p.confidence != null
        && (g.confidence == null || p.confidence > g.confidence)) {
      g.confidence = p.confidence;
    }
    if (!decided && predictionIsAmbiguous(p)) g.ambiguous.push(p);
  });
  groups.sort(function(a, b) {
    if (a.decided !== b.decided) return a.decided ? 1 : -1;
    // Buckets whose only rows are minority-frame borrowers (confidence ==
    // null) sink below any bucket carrying real evidence, so a legacy
    // Robin bucket with no Robin-labelled row does not rank above a
    // 60% bucket that does.
    var aConf = a.confidence == null ? -1 : a.confidence;
    var bConf = b.confidence == null ? -1 : b.confidence;
    return bConf - aConf;
  });

  // A bucket-level suppression used to run here, mirroring the server's:
  // under an active threshold, a pending bucket left with `confidence ==
  // null` was dropped, counted, and explained in its own empty-state
  // sentence. Retired along with the server's copy. A pending bucket can
  // only keep a null confidence when every row in it is labelled with a
  // species other than the one the accept path applies, and a since-retired
  // one-shot repair cleared that shape out of the catalog (current
  // classification never writes it) — a grouped row's consensus is now its own
  // species, so a surviving row always credits its own bucket. (With
  // `threshold > 0` the row filter above has already dropped any row whose
  // confidence is null or zero, so there is no second route to a null
  // bucket here.) The credit gate stays: it is what keeps the number honest
  // if that invariant is ever broken again.

  var shown = detailPredictionsExpanded
    ? groups
    : groups.slice(0, PREDICTION_COLLAPSE_AT);
  var collapsed = groups.length - shown.length;

  // The rows the markup will index into. Set before the loop so the index
  // each button carries is the index the handler resolves.
  detailPredictionGroups = shown;

  var html = '';
  shown.forEach(function(g, idx) {
    // A group is only safely acceptable if none of its rows is ambiguous —
    // otherwise one Accept would silently swallow a decision the user should
    // be making in Review.
    var ambiguous = !g.decided && g.ambiguous.length > 0;
    var cls = 'prediction-row' + (g.decided ? ' decided' : '') + (ambiguous ? ' ambiguous' : '');
    var meta = formatPredictionConfidence(g.confidence) +
      ' · ' + Object.keys(g.models).join(', ');
    if (g.count > 1) meta += ' · on ' + g.count + ' detections';
    var actions;
    if (g.decided) {
      // Decided rows stay listed rather than disappearing, and say which
      // decision they carry — "reviewed" is a real answer to "what happened
      // to this prediction", not a row worth hiding.
      actions = '<span class="prediction-status-tag" title="' +
        escapeAttr(predictionStatusExplanation(g.status)) + '">' +
        escapeHtml(g.status) + '</span>';
    } else if (ambiguous) {
      actions = '<button class="prediction-review-link"' +
        ' data-prediction-action="review" data-photo-id="' + photoId + '"' +
        ' title="This prediction needs Review\'s full decision UI">Open in Review</button>';
    } else {
      // Only the row index reaches the markup. The ids and the species the
      // Accept will apply — passed to the server as `expected_species` so it
      // can refuse a row whose grouping drifted after this render — are read
      // back out of `detailPredictionGroups[idx]` by the click handler, so no
      // species name is ever parsed as HTML.
      actions =
        '<button class="prediction-accept" data-prediction-action="accept"' +
        ' data-prediction-group="' + idx + '"' +
        ' title="Accept this species and add the keyword">Accept</button>' +
        '<button class="prediction-reject" data-prediction-action="reject"' +
        ' data-prediction-group="' + idx + '"' +
        ' title="Reject this prediction">Reject</button>';
    }
    html += '<div class="' + cls + '">' +
      '<div style="min-width:0;">' +
        '<div class="prediction-species">' + escapeHtml(g.species) + '</div>' +
        '<div class="prediction-meta">' + escapeHtml(meta) + '</div>' +
        (ambiguous ? '<div class="prediction-why">' +
          escapeHtml(predictionAmbiguityReason(g.ambiguous[0])) + '</div>' : '') +
        (g.unlisted.length ? '<div class="prediction-why">' +
          escapeHtml(unlistedGroupReason(g)) + '</div>' : '') +
      '</div>' +
      '<div class="prediction-actions">' + actions + '</div>' +
    '</div>';
  });
  // Name what is collapsed and how weak it is, so the shortened list can never
  // be mistaken for the whole answer.
  if (collapsed > 0) {
    var weakest = groups[shown.length].confidence;
    // "N weaker species (X% and below)" is only truthful when the first
    // collapsed bucket carries evidence of its own species. Legacy burst
    // groups whose only rows are minority-frame borrowers land here with
    // ``confidence == null``; ranking them as "weaker" or claiming an
    // "X% and below" bound they never had would be the same borrowed-
    // score problem the meta line was just fixed to avoid.
    var weakestLabel = weakest == null
      ? 'confidence unknown'
      : formatPredictionConfidence(weakest) + ' and below';
    html += '<button class="prediction-toggle" data-prediction-action="toggle">Show ' +
      collapsed + ' weaker species (' + weakestLabel + ')</button>';
  } else if (detailPredictionsExpanded && groups.length > PREDICTION_COLLAPSE_AT) {
    html += '<button class="prediction-toggle" data-prediction-action="toggle">Show fewer</button>';
  }
  if (hidden) {
    html += '<div class="prediction-below-threshold">' + hidden +
      (hidden === 1 ? ' more prediction is' : ' more predictions are') +
      ' below your confidence threshold (' + formatPredictionConfidence(threshold) + ').</div>';
  }
  list.innerHTML = matchBanner + html;
}

function openPredictionInReview(photoId) {
  // Hand off an EXPLICIT empty filter expression, not just ?photo_id=.
  // VireoFilter.init() falls through to restorePersisted() whenever the URL
  // carries no `filters` param, so a filter the user left active on Review
  // last visit (say "rating >= 3") would silently intersect with this deep
  // link. If it excludes the photo, Review renders an empty queue under a
  // pill reading "Showing one photo from Browse" — the pill would be
  // describing a scope that is not the one in effect. Sending `filters`
  // makes the deep link's scope exactly what the pill claims: this photo.
  // A well-formed empty payload (rather than `?filters=`) matters:
  // init() throws on a present-but-unparseable handoff.
  var noFilters = encodeURIComponent(JSON.stringify({
    root: { mode: 'all', rules: [] }, visual: null,
  }));
  window.location.href = '/review?photo_id=' + encodeURIComponent(photoId) +
    '&filters=' + noFilters;
}

// Everything the prediction panels display is DERIVED state: a row's
// `effective_category` (ambiguous or not) and a selection row's
// `keyworded_count` / `missing_photo_ids` are all recomputed by the server
// from the photos' CURRENT species keywords and prediction_review status. So
// the panels go stale on far more than accept/reject — any keyword add,
// remove or retype, and any undo/redo, invalidates them too. An "Accept on 38"
// button left over from before the user keyworded 10 of those photos states a
// falsehood, which CORE_PHILOSOPHY.md's "no black boxes" forbids outright.
//
// Rather than bolt a reload onto each mutation site (which is how this drifted
// in the first place), panel freshness hangs off the chokepoints every
// mutation already passes through:
//   * `_refreshBrowseKeywordState` — every keyword write in Browse calls it,
//     because card badges have to be refetched anyway;
//   * `refreshBrowseSidebarPanels` — the shared history refresh hook repaints
//     the whole sidebar after undo/redo reverses keywords and predictions;
//   * `_afterPredictionMutation` / `rejectDetailPredictions` — status writes.
// A future keyword mutation path therefore gets panel freshness for free.
//
// `opts.skipDetail` is for callers about to run a full `loadDetail`, which
// re-fetches the photo AND its predictions: without it one accept would issue
// two identical prediction requests.
function refreshPredictionPanels(opts) {
  opts = opts || {};
  // Drop the cache key first: `loadSelectionPredictions` early-returns when
  // the key matches, and the selection itself has not changed here — only the
  // state its rows are computed from.
  Vireo.browse.panelRequests.predictions.invalidate();
  var selection = getActiveSelection();
  if (selection.length > 1) loadSelectionPredictions(selection);
  // Only one of the two panels is on screen at a time: a multi-selection
  // replaces the single-photo detail with the batch inspector. Guarding on
  // the selection as well as the pointer keeps a stale `_detailPhotoId` from
  // repainting the departed anchor's rows underneath a batch selection.
  else if (!opts.skipDetail && window._detailPhotoId != null) {
    loadDetailPredictions(window._detailPhotoId);
  }
}

// Undo/redo lives in the shared navbar and writes straight to the database:
// undoing a `prediction_accept` returns the row to pending AND strips the
// keyword it added. Both halves of that revert have a panel in Browse's right
// sidebar, so an undo that repainted only the prediction panels left the
// Keywords section listing a keyword the database no longer holds — the same
// falsehood in the other direction.
//
// Undo/redo is the one mutation source Browse cannot route through its own
// call sites: the navbar writes and then calls the refresh hook. So this repaints the
// sidebar wholesale rather than naming the panel that happens to be wrong
// today, and a panel added to that sidebar later is refreshed by default
// instead of silently going stale until someone notices.
function refreshBrowseSidebarPanels() {
  var selection = getActiveSelection();
  // Single photo: `loadDetail` re-fetches the photo AND its predictions in one
  // request, so it refreshes both halves. Tell the prediction half to skip its
  // own fetch — the `skipDetail` de-duplication `_afterPredictionMutation`
  // already relies on.
  var reloadingDetail = selection.length <= 1 && window._detailPhotoId != null;
  refreshPredictionPanels({skipDetail: reloadingDetail});
  // Multi-selection: the batch inspector's keyword suggestions are computed
  // from the selection's current keywords, and its loader early-returns on an
  // unchanged selection key — which is exactly this case, since the selection
  // did not move, only the state behind it.
  if (selection.length > 1) {
    Vireo.browse.panelRequests.keywords.invalidate();
    loadSelectionKeywordSuggestions(selection);
  } else if (reloadingDetail) {
    loadDetail(window._detailPhotoId);
  }
}

window.afterHistoryChange = async function() {
  // History can change membership and ordering, including photos outside the
  // loaded page. Re-run the current query for both toolbar and keyboard actions.
  var previousSelection = new Set(selectedPhotos);
  var cachedMemberIds = new Set();
  Object.values(browseStackMembers).forEach(function(members) {
    members.forEach(function(photo) { cachedMemberIds.add(photo.id); });
  });
  // ``focusAnchor``: an undone rating or keyword can move the anchored card
  // anywhere in the current sort, and paging to find it walks the catalog
  // (Codex P1 on PR #1695). The ids this function restores below all belong
  // to the anchored card — ``captureSelectedPhotoAnchor`` only anchors a
  // single photo or a single stack — so the focused window holds them.
  var loaded = await resetAndLoad({
    preserveAnchor: true, focusAnchor: true, preserveCollection: true,
  });
  if (loaded === false) throw new Error('Could not reload Browse');
  if (loaded !== true) return;
  // resetAndLoad has cleared selection; the reload settled with the grid
  // interactive. If the user clicks another card while we hydrate uncached
  // stack members below, selectPhoto() bumps anchorRestoreEpoch — the same
  // signal every other async selection path (Select all, keyword suggestions,
  // batch-delete companion count) uses to know its captured ids no longer
  // reflect what the user wants. Snapshot the epoch here so the restore
  // below can bail out instead of folding the pre-undo ids back into the
  // fresh selection, which would replace a new single-card pick or merge
  // into a new batch. Codex P2 on PR #1672.
  var restoreEpoch = anchorRestoreEpoch;
  var windowIsCurrent = observeBrowseWindow();
  for (var cover of photos.slice()) {
    var memberIds = cover.browse_stack && cover.browse_stack.photo_ids;
    // Selected members count as much as cached ones. A stack selected by a
    // click on its collapsed card has never been expanded, so its hidden
    // frames are in `previousSelection` and in no member cache; without
    // hydrating them, the restore below finds only the cover and the whole
    // stack quietly shrinks to one frame across an undo.
    // Codex P2 on PR #1672.
    if (memberIds && memberIds.some(function(id) {
      return cachedMemberIds.has(id) || previousSelection.has(id);
    })) {
      var status = await hydrateBrowseStackCoverMembers(cover, windowIsCurrent);
      if (!windowIsCurrent()) return;
      if (status === 'unresolved') throw new Error('Could not reload stack members');
    }
  }
  if (anchorRestoreEpoch === restoreEpoch) {
    // Restore only selections that still belong to the refreshed query.
    previousSelection.forEach(function(id) {
      var photo = findBrowsePhoto(id);
      if (photo && browsePhotoIsAvailable(photo)) selectedPhotos.add(id);
    });
  }
  refreshCardSelectionVisuals();
  updateBatchBar();
  refreshBrowseSidebarPanels();
  scheduleCollectionCountsRefresh();
  refreshPendingSyncBanner();
};

// Accepting writes a keyword, so every surface that counts keywords has to be
// told — the same fan-out applySelectionKeyword performs. refreshPendingSyncBanner
// matters especially: an accept queues a pending XMP change.
async function _afterPredictionMutation(photoIds, opts) {
  opts = opts || {};
  var reloadingDetail = !!(
    opts.reloadDetail
    && opts.photoId != null
    && window._detailPhotoId === opts.photoId
  );
  Vireo.browse.panelRequests.keywords.invalidate();
  // Refreshes the grid's keyword state AND, through it, both prediction
  // panels: the accept changed the photos' species keywords, so the surviving
  // rows' ambiguity and keyworded counts have both moved. `skipDetail` keeps
  // one accept from issuing two identical prediction requests, since
  // `loadDetail` below re-fetches the photo AND its predictions.
  await _refreshBrowseKeywordState(photoIds, { skipDetail: reloadingDetail });
  var selection = getActiveSelection();
  if (selection.length > 1) loadSelectionKeywordSuggestions(selection);
  if (reloadingDetail) loadDetail(opts.photoId);
  loadKeywords();
  scheduleCollectionCountsRefresh();
  refreshActiveCollectionAfterMembershipChange([MUTATION_KEYWORD, MUTATION_PREDICTION]);
  refreshPendingSyncBanner();
  // Only when the call actually accepted something. batch-accept returns
  // success with `accepted: 0` for a payload whose rows have all been decided
  // or become ambiguous since the panel rendered, and records no undoable
  // edit — so an unconditional toast would advertise, and Ctrl+Z would
  // reverse, some older unrelated edit.
  if (opts.accepted !== 0) showUndoToast();
}

// A stale panel can submit rows the server then declines to accept: decided
// elsewhere, no longer unambiguous against the photos' current keywords, or
// superseded by a later classification run.
// The refreshed panel shows the survivors, but the gap between "Accept on 35"
// and 33 accepts has to be named, not left for the user to notice — the same
// rule that made the button disclose its count in the first place.
function _reportSkippedAccepts(data) {
  if (!data) return;
  var parts = [];
  if (data.already_decided) {
    parts.push(data.already_decided + ' already decided elsewhere');
  }
  if (data.skipped_ambiguous) {
    parts.push(data.skipped_ambiguous +
      ' now conflicting with keywords — resolve in Review');
  }
  // Named separately from the conflict case on purpose: nothing is wrong with
  // these photos and there is nothing to resolve. Classification re-ran with a
  // different label set, so the row this panel rendered is no longer the
  // current one — the refresh below replaces it.
  if (data.skipped_superseded) {
    parts.push(data.skipped_superseded +
      ' replaced by a newer classification run');
  }
  // Photo left the workspace between the parse-time ownership check and the
  // batch's lock (folder moved to another workspace, etc.). Its own name
  // because the user's next step is neither Review nor a re-run — a panel
  // refresh drops the photo from view.
  if (data.skipped_out_of_workspace) {
    parts.push(data.skipped_out_of_workspace +
      ' no longer in this workspace');
  }
  // Another tab ungrouped or edited the row, so its current consensus
  // species is not the one this button labelled. Reported so the gap between
  // "Accept on 35 Bald Eagle" and 33 accepts is spoken, not swallowed.
  if (data.skipped_species_drifted) {
    parts.push(data.skipped_species_drifted +
      ' species changed since the panel loaded');
  }
  if (!parts.length) return;
  var accepted = data.accepted || 0;
  showToast((accepted ? 'Accepted ' + accepted + ' photo' +
    (accepted === 1 ? '' : 's') + '; skipped ' : 'Nothing accepted: ') +
    parts.join(', '), 'warning');
}

async function acceptDetailPredictions(predictionIds, photoId, expectedSpecies) {
  if (!predictionIds || !predictionIds.length) return;
  var data;
  try {
    data = await safeFetch('/api/predictions/batch-accept', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        prediction_ids: predictionIds,
        // The species the button labelled — passed so the server can refuse
        // rows whose grouping (and therefore consensus) drifted after the
        // panel rendered but before the accept lands.
        expected_species: expectedSpecies || null,
      }),
    });
  } catch(e) { return; }
  await _afterPredictionMutation(
    [photoId],
    { photoId: photoId, reloadDetail: true, accepted: (data || {}).accepted },
  );
  _reportSkippedAccepts(data);
}

// The reject side of _reportSkippedAccepts. batch-reject skips the same
// already-decided and superseded rows, and a stale panel's Reject dropping
// rows without saying so is the same silent gap on the other button.
function _reportSkippedRejects(data) {
  if (!data) return;
  var parts = [];
  if (data.already_decided) {
    parts.push(data.already_decided + ' already decided elsewhere');
  }
  if (data.skipped_superseded) {
    parts.push(data.skipped_superseded +
      ' replaced by a newer classification run');
  }
  // Same workspace-detach race as the accept side: the photo left the active
  // workspace between the parse-time ownership check and the batch's lock, so
  // the response is `rejected: 0, skipped_out_of_workspace: 1`. Named
  // separately for the same reason — silence here is a click that visibly
  // did nothing, and the user hunting for a broken button.
  if (data.skipped_out_of_workspace) {
    parts.push(data.skipped_out_of_workspace +
      ' no longer in this workspace');
  }
  if (!parts.length) return;
  var rejected = data.rejected || 0;
  showToast((rejected ? 'Rejected ' + rejected + '; skipped ' :
    'Nothing rejected: ') + parts.join(', '), 'warning');
}

async function rejectDetailPredictions(predictionIds, photoId) {
  if (!predictionIds || !predictionIds.length) return;
  var data;
  try {
    data = await safeFetch('/api/predictions/batch-reject', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({prediction_ids: predictionIds}),
    });
  } catch(e) { return; }
  _reportSkippedRejects(data);
  // A reject changes no keywords, so only the prediction rows need repainting.
  // Deliberately no showUndoToast() here: `prediction_reject` is in the DB's
  // _NON_UNDOABLE set (no _apply_undo handler), so /api/undo/status would
  // advertise some earlier edit as reversible and Ctrl+Z would undo THAT
  // instead of the reject — silently reversing an unrelated action.
  refreshPredictionPanels();
  // A rejection can change membership in prediction-status collections
  // (e.g. "pending predictions"): refresh counts and re-evaluate the
  // active collection so the grid and counts don't lag behind the DB.
  // The accept path fans out the same way via _afterPredictionMutation.
  scheduleCollectionCountsRefresh();
  // Rejection changes prediction status but writes no keywords, so a
  // keyword/species/count filter cannot notice this edit; naming
  // ``MUTATION_KEYWORD`` here forced ``dependsOnMutation`` to reload those
  // grids anyway, clearing the selection and detail panel for a result set
  // that could not have changed.
  refreshActiveCollectionAfterMembershipChange([MUTATION_PREDICTION]);
  refreshPredictionConfidenceBadges([photoId]);
}

/* The reject path's badge refresh. Rejecting writes no keywords, so it
   deliberately does not go through _refreshBrowseKeywordState — but it does
   change the photo's top prediction, and with the Prediction confidence card
   field on, the badge would otherwise keep showing the score the user just
   threw away (Codex P2 on PR #1670). Accepts get the same value for free
   from the /by-ids fetch _refreshBrowseKeywordState already makes.

   Skipped when the card field is off (nothing displays the number) and when
   the active sort ranks on confidence (the caller reloads the whole grid,
   which replaces these cards outright). */
async function refreshPredictionConfidenceBadges(photoIds) {
  if (cardFields.indexOf('prediction_confidence') === -1) return;
  if (sortSelectRanksOnPredictionConfidence()) return;
  var ids = Array.from(new Set(photoIds || [])).filter(function(id) {
    return !!findBrowsePhoto(id);
  });
  if (!ids.length) return;
  try {
    var data = await safeFetch('/api/photos/by-ids', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_ids: ids}),
    }, {toast: false});
    var touched = [];
    (data.photos || []).forEach(function(updated) {
      var local = findBrowsePhoto(updated.id);
      if (!local || local.prediction_confidence_is_stack_lead) return;
      local.prediction_confidence = updated.prediction_confidence === undefined
        ? null : updated.prediction_confidence;
      touched.push(updated.id);
    });
    if (touched.length) refreshGridCards(touched);
  } catch (e) {
    // The reject itself succeeded; a stale badge until the next reload is
    // not worth a toast on top of the ones _reportSkippedRejects may show.
  }
}

async function loadSelectionPredictions(ids) {
  var list = document.getElementById('selectionPredictions');
  if (!list) return;
  if (ids.length > 1000) return;
  var request = Vireo.browse.panelRequests.predictions.begin(selectionIdsKey(ids));
  if (!request) return;
  list.innerHTML = '<div class="selection-empty">Checking predictions...</div>';
  try {
    var data = await safeFetch('/api/selection/prediction-suggestions', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({photo_ids: ids}),
    }, { toast: false });
    if (!request.isCurrent()) return;
    renderSelectionPredictions(
      data.predictions || [], data.selected_count || ids.length, data,
    );
  } catch(e) {
    if (request.fail()) {
      list.innerHTML = '<div class="selection-empty">Could not load predictions.</div>';
    }
  }
}

function toggleSelectionPredictions() {
  selectionPredictionsExpanded = !selectionPredictionsExpanded;
  if (_selectionPredictionData) {
    renderSelectionPredictions(
      _selectionPredictionData.predictions,
      _selectionPredictionData.selectedCount,
      _selectionPredictionData.meta,
    );
  }
}

function renderSelectionPredictions(predictions, selectedCount, meta) {
  var list = document.getElementById('selectionPredictions');
  if (!list) return;
  meta = meta || {};
  _selectionPredictionData = {
    predictions: predictions, selectedCount: selectedCount, meta: meta,
  };
  selectionPredictionAcceptableById = {};
  selectionPredictionSpeciesByIdx = {};
  selectionPredictionPhotoIdsByIdx = {};
  // The floor is the user's own setting, so hidden rows get counted out loud
  // rather than vanishing into an empty-looking panel.
  var hidden = meta.below_threshold_count || 0;
  var hiddenNote = hidden
    ? '<div class="prediction-below-threshold">' + hidden +
      (hidden === 1 ? ' prediction is' : ' predictions are') +
      ' below your confidence threshold (' +
      formatPredictionConfidence(meta.threshold || 0) + ').</div>'
    : '';
  // The endpoint's borrowed-suppression counter, and the two-way empty
  // state it fed, were retired with the server-side bucket suppression that
  // produced it: a bucket can no longer be dropped for borrowed evidence,
  // so the count would always read zero and the two empty-state sentences
  // would always resolve to the same one. See
  // ``api_selection_prediction_suggestions``.
  if (!predictions.length) {
    list.innerHTML = '<div class="selection-empty">' +
      'No pending predictions on the selected photos.</div>' + hiddenNote;
    return;
  }

  // Sorted strongest-first by the endpoint, so the collapse keeps the species
  // most worth acting on and names the rest.
  var shown = selectionPredictionsExpanded
    ? predictions
    : predictions.slice(0, PREDICTION_COLLAPSE_AT);
  var collapsed = predictions.length - shown.length;

  var html = '';
  shown.forEach(function(p, idx) {
    var acceptable = p.acceptable_prediction_ids || [];
    var ambiguous = p.ambiguous_prediction_ids || [];
    var ambiguousPhotos = p.ambiguous_photo_ids || [];
    // Distinct photos, not distinct prediction rows: a photo can have several
    // matching detections and each contributes a prediction id, so `acceptable`
    // overcounts the photos that would actually be keyworded. Fall back to
    // acceptable.length for older backend payloads.
    var acceptablePhotoCount = (typeof p.acceptable_photo_count === 'number')
      ? p.acceptable_photo_count
      : acceptable.length;
    selectionPredictionAcceptableById[idx] = acceptable;
    // The species the button will apply, sent back with the Accept so the
    // server can refuse rows whose consensus has drifted from what the panel
    // rendered (another tab ungrouping the burst, per-vote edits shifting
    // the winner). See _species_drifted_prediction_ids in app.py.
    selectionPredictionSpeciesByIdx[idx] = p.species;
    var predictedPhotos = p.predicted_photo_ids || [];
    selectionPredictionPhotoIdsByIdx[idx] = predictedPhotos;

    var rowMeta = 'Predicted on ' + p.predicted_count + ' of ' + selectedCount;
    if (p.keyworded_count) rowMeta += ', already keyworded on ' + p.keyworded_count;
    var range = p.min_confidence === p.max_confidence
      ? formatPredictionConfidence(p.max_confidence)
      : formatPredictionConfidence(p.min_confidence) + '–' + formatPredictionConfidence(p.max_confidence);
    rowMeta += ' · ' + range;

    // Each Accept button names the photo count it will actually act on.
    // "Accept on 38" that silently accepts 35 is exactly what
    // CORE_PHILOSOPHY.md forbids, so the ambiguous remainder is called out
    // beside it rather than folded in — and "Accept on all" carries the
    // selection size for the same reason. It is the primary, top button, so
    // its own number is what has to tell the user whether "all" is a
    // rounding error on the prediction or a species claim about 68 photos
    // that never predicted it.
    var actions = '';
    var allTitle = acceptablePhotoCount === selectedCount
      ? 'Accept this species on all ' + selectedCount + ' selected photos'
      : 'Add this species to all ' + selectedCount +
        ' selected photos, including those without this prediction. ' +
        'Existing keywords are kept; conflicts still need review.';
    actions += '<button class="prediction-accept prediction-accept-all" onclick="acceptSelectionPrediction(' + idx +
      ', true, this)" title="' + allTitle + '">Accept on all ' + selectedCount + '</button>';
    // The narrower accept is only a second *action* when it would touch
    // fewer photos. When every selected photo already predicts this species
    // unambiguously, the two buttons submit the same work and land the same
    // undo entry, so the second one is a decision the user has to stop and
    // make for no difference in outcome.
    if (acceptablePhotoCount && acceptablePhotoCount !== selectedCount) {
      actions += '<button class="prediction-accept prediction-accept-subset" onclick="acceptSelectionPrediction(' + idx +
        ')" title="Accept this species only on the ' + acceptablePhotoCount +
        ' selected photos that predict it and are unambiguous">Accept on ' +
        acceptablePhotoCount + '</button>';
    }
    // Look before you accept. "Blue-breasted Quail on 2 of 70" is either a
    // real find or a bad detection, and no count in this row can tell the
    // user which — only the pixels can. Opens exactly the photos the row's
    // "Predicted on N" counts, in the lightbox, where 1:1 zoom lives. The
    // selection is left alone on purpose: the decision this button feeds is
    // usually "accept the OTHER species on all 70", and narrowing to 2 would
    // repaint this panel for 2 photos and take that button away.
    if (predictedPhotos.length) {
      actions += '<button class="prediction-show" onclick="showSelectionPredictionPhotos(' + idx +
        ', this)" title="Open the ' + predictedPhotos.length +
        ' photo' + (predictedPhotos.length === 1 ? '' : 's') +
        ' predicting this species in the lightbox. Your selection of ' + selectedCount +
        ' stays as it is.">Show ' + predictedPhotos.length + ' photo' +
        (predictedPhotos.length === 1 ? '' : 's') + '</button>';
    }
    var why = '';
    if (ambiguousPhotos.length) {
      why = ambiguousPhotos.length + (ambiguousPhotos.length === 1 ? ' photo needs' : ' photos need') +
        ' review — alternatives or a conflict with existing keywords';
      var firstAmbiguousPhoto = ambiguousPhotos[0];
      if (!acceptablePhotoCount && firstAmbiguousPhoto != null) {
        actions += '<button class="prediction-review-link" onclick="openPredictionInReview(' +
          firstAmbiguousPhoto + ')">Open in Review</button>';
      }
    } else if (!acceptablePhotoCount) {
      why = 'Already keyworded on every photo that predicts it';
    }
    // "Accept on 38" now covers photos that already carry the keyword — for
    // those, accepting only clears the still-pending prediction out of Review
    // and writes no tag. One number must not silently mean two outcomes, so
    // the split is named. `acceptable_keyworded_count` is the backend's own
    // intersection, so the panel never recomputes it and drifts.
    var keywordedInAccept = p.acceptable_keyworded_count || 0;
    if (acceptablePhotoCount && keywordedInAccept) {
      var alreadyNote = keywordedInAccept +
        (keywordedInAccept === 1 ? ' already carries' : ' already carry') +
        ' the keyword — accepting only clears ' +
        (keywordedInAccept === 1 ? 'it' : 'them') + ' from Review';
      why = why ? why + ' · ' + alreadyNote : alreadyNote;
    }

    html += '<div class="prediction-row' + (ambiguousPhotos.length ? ' ambiguous' : '') + '">' +
      '<div style="min-width:0;">' +
        '<div class="prediction-species">' + escapeHtml(p.species) + '</div>' +
        '<div class="prediction-meta">' + escapeHtml(rowMeta) + '</div>' +
        (why ? '<div class="prediction-why">' + escapeHtml(why) + '</div>' : '') +
      '</div>' +
      '<div class="prediction-actions">' + actions + '</div>' +
    '</div>';
  });
  if (collapsed > 0) {
    html += '<button class="prediction-toggle" onclick="toggleSelectionPredictions()">Show ' +
      collapsed + ' more predicted species</button>';
  } else if (selectionPredictionsExpanded && predictions.length > PREDICTION_COLLAPSE_AT) {
    html += '<button class="prediction-toggle" onclick="toggleSelectionPredictions()">Show fewer</button>';
  }
  list.innerHTML = html + hiddenNote;
}

// "Show N photos" — open just the photos a prediction row counts, so the
// user can judge the detection before accepting anything. Deliberately
// read-only: nothing here changes the selection, the grid, or any keyword.
async function showSelectionPredictionPhotos(idx, button) {
  var ids = (selectionPredictionPhotoIdsByIdx[idx] || []).slice();
  if (!ids.length) return;
  // Both the selection and the latest Show click must still own this result.
  var panelIsCurrent = Vireo.browse.panelRequests.predictions.observe();
  var request = Vireo.browse.panelRequests.predictionPhotos.begin();
  var label = button ? button.textContent : '';
  if (button) {
    button.disabled = true;
    button.textContent = 'Loading…';
  }
  var fetched = [];
  try {
    // /api/photos/by-ids caps each POST at 500 ids, and a selection can
    // carry up to 1,000 photos, so chunk rather than silently truncating.
    for (var offset = 0; offset < ids.length; offset += 500) {
      var data = await safeFetch('/api/photos/by-ids', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({photo_ids: ids.slice(offset, offset + 500)}),
      });
      if (!panelIsCurrent() || !request.isCurrent()) return;
      fetched = fetched.concat(data.photos || []);
    }
  } catch (e) {
    return;
  } finally {
    if (button) {
      button.disabled = false;
      button.textContent = label;
    }
  }
  if (!panelIsCurrent() || !request.isCurrent()) return;
  if (!fetched.length) {
    showToast('Could not open those photos — they are no longer in this workspace.', 'warning');
    return;
  }
  // by-ids drops photos that left the workspace between the panel's render
  // and this click. Say so rather than opening a lightbox whose counter
  // quietly reads "1 / 1" under a button that promised 2.
  if (fetched.length < ids.length) {
    showToast('Showing ' + fetched.length + ' of ' + ids.length +
      ' photos — the rest are no longer in this workspace.', 'warning');
  }
  openLightbox(fetched[0].id, fetched[0].filename || '', fetched);
}

async function acceptSelectionPrediction(idx, onAll, button) {
  var ids = selectionPredictionAcceptableById[idx] || [];
  if (!onAll && !ids.length) return;
  var expectedSpecies = selectionPredictionSpeciesByIdx[idx];
  var selection = getActiveSelection();
  if (!expectedSpecies || !selection.length) return;
  // Restore whatever label the button was rendered with rather than a
  // hardcoded one: "Accept on all" now carries the selection count, and a
  // fixed string here would quietly drop the number on every button that
  // finished a request.
  var restoreLabel = button ? button.textContent : null;
  if (button) {
    button.disabled = true;
    button.textContent = 'Accepting…';
  }
  var data;
  try {
    data = await safeFetch('/api/predictions/batch-accept', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        prediction_ids: ids,
        // Name the species the button will apply. If another tab shifted
        // the row's consensus after this panel rendered, the server skips
        // the drifted row rather than tagging with something the user was
        // never shown.
        expected_species: expectedSpecies || null,
        photo_ids: onAll ? selection : undefined,
      }),
    });
  } catch(e) { return; }
  finally {
    if (button) {
      button.disabled = false;
      button.textContent = restoreLabel;
    }
  }
  await _afterPredictionMutation(
    selection, { accepted: (data || {}).accepted },
  );
  _reportSkippedAccepts(data);
}
