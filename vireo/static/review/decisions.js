// Recording review decisions: accept, reject, accept an alternative, and Accept All.
// Classic page script; load boot.js after all definitions.

/* ---------- Actions ---------- */

// The pending cards the grid is showing right now: the confidence, model,
// label-set and tab filters applied, one card per burst. Accept All acts on
// exactly these (each burst card's accept covers its burst, as clicking it
// would), so rows the filters hide are never tagged behind the user's back.
function visiblePendingCards() {
  return getVisibleItems().filter(function(p) { return p.status === 'pending'; });
}

// Mark every row the accept says it decided, not only the one in the URL.
// ``accept_prediction`` fans a decision across the still-pending members of
// the same burst group, so the response names siblings too; leaving those at
// ``pending`` locally keeps cards on screen offering an Accept the database
// has already recorded. See vireo-predictions.js for the contract.
//
// ``predictions`` is a filtered view holding the *same objects* as
// ``allPredictions``, but a sibling outside the current collection filter is
// only in the unfiltered copy — walk both so neither is left claiming a row is
// pending after the server accepted it.
function _markPredictionsAccepted(predIds) {
  if (!predIds || !predIds.length) return;
  var wanted = new Set(predIds);
  [allPredictions, predictions].forEach(function(list) {
    (list || []).forEach(function(p) {
      if (p && wanted.has(p.id)) p.status = 'accepted';
    });
  });
}

async function acceptPrediction(predId) {
  if (_predictionsReloading) return;
  var epoch = _loadPredictionsEpoch;
  var applied;
  try {
    applied = await safeFetch('/api/predictions/' + predId + '/accept', { method: 'POST' });
  } catch(e) { return; }
  // A reload between click and response would have swapped ``predictions``;
  // touching the new array with a stale id would either miss (harmless) or
  // mis-label a row that isn't the one the user clicked.
  if (_predictionEpochStale(epoch)) return;
  // The clicked row is credited whatever the body carried — a 200 means the
  // accept ran — and the group expansion is credited on top of it.
  _markPredictionsAccepted(
    [predId].concat(Vireo.predictions.decidedPredictionIds(applied)));
  renderAll();
}

async function rejectPrediction(predId) {
  if (_predictionsReloading) return;
  var epoch = _loadPredictionsEpoch;
  var resp;
  try {
    resp = await safeFetch('/api/predictions/' + predId + '/reject', { method: 'POST' });
  } catch(e) { return; }
  if (_predictionEpochStale(epoch)) return;
  // A burst card's reject covers every undecided member, as its accept does;
  // the response names each row it rejected.
  // Walk ``allPredictions`` too: a member outside the collection filter is
  // only in the unfiltered copy (see _markPredictionsAccepted).
  var rejected = new Set([predId].concat((resp && resp.rejected_prediction_ids) || []));
  [allPredictions, predictions].forEach(function(list) {
    (list || []).forEach(function(p) {
      if (p && rejected.has(p.id)) p.status = 'rejected';
    });
  });
  renderAll();
}

async function acceptAlternative(altId, parentPredId) {
  if (_predictionsReloading) return;
  var epoch = _loadPredictionsEpoch;
  try {
    await safeFetch('/api/predictions/' + altId + '/accept', { method: 'POST' });
  } catch(e) { return; }
  if (_predictionEpochStale(epoch)) return;
  // Update local state: mark alternative as accepted, parent as rejected
  var parent = predictions.find(function(p) { return p.id === parentPredId; });
  if (parent) parent.status = 'rejected';
  // Reload predictions to get fresh state
  await loadPredictions();
  renderAll();
}

async function acceptAllPending() {
  if (_predictionsReloading) return;
  // Capture the epoch on entry — a filter edit / collection switch mid-loop
  // bumps ``_loadPredictionsEpoch``; without this the loop would keep
  // accepting rows from the pre-reload ``predictions`` array while the
  // visible chips already show a narrower filter. See r3618935660.
  var epoch = _loadPredictionsEpoch;
  var pending = visiblePendingCards();
  // ``pending`` is a snapshot, and one accept can settle several of its rows:
  // ``accept_prediction`` expands across the burst group and the response
  // names every row it decided. Without consuming that, the next member of the
  // same burst is re-sent, meets the terminal-status 409, and the catch below
  // abandons the run — so a single burst in the queue would leave every
  // unrelated prediction after it unaccepted, silently. Shared with ID
  // Conflicts' batch accept; see vireo-predictions.js.
  var decided = Vireo.predictions.groupedDecisionTracker();
  for (var i = 0; i < pending.length; i++) {
    if (_predictionEpochStale(epoch)) break;
    // Accepted, not skipped: an earlier request in this very run wrote it.
    if (decided.alreadyDecided(pending[i].id)) continue;
    try {
      var applied = await safeFetch('/api/predictions/' + pending[i].id + '/accept', { method: 'POST' });
      _markPredictionsAccepted(
        [pending[i].id].concat(decided.record(applied)));
    } catch(e) { break; }
  }
  if (_predictionEpochStale(epoch)) return;
  renderAll();
}
