// Card identity is computed by the server; decisions submit the visible rows.
/** @type {Map<number, ReviewPrediction[]>} */ var reviewCardMemberIndex = new Map();
/** @type {ReviewPrediction[]|null} */ var reviewCardIndexRows = null;
var reviewCardIndexScope = '';

function reviewCardScopeKey() {
  return JSON.stringify([currentModel, currentLabelsFingerprint, currentTab, minConfidence, reviewEvidenceFilter]);
}

/** @param {ReviewPrediction[]} rows */
function indexVisibleReviewCards(rows) {
  var parents = rows.map(function(_, index) { return index; });
  /** @param {number} index @returns {number} */
  function root(index) {
    var parent = parents[index];
    while (parent != null && parent !== index) {
      parents[index] = parents[parent] ?? parent;
      index = parent;
      parent = parents[index];
    }
    return index;
  }
  /** @type {Map<string, number>} */ var links = new Map();
  rows.forEach(function(p, index) {
    var base = reviewBaseCardKey(p);
    var keys = [base];
    if (reviewEvidenceFilter === 'all' && currentModel === 'all' && !currentLabelsFingerprint && p.card_id) {
      // A confidence filter can hide the frame that connected two groups.
      // Reconnect the remaining nodes within the server's card boundaries,
      // keyed on detection_id so two detected subjects in one photo stay apart.
      keys = [JSON.stringify([base, p.model, p.group_id || p.id]), JSON.stringify([base, 'detection', p.detection_id])];
    }
    keys.forEach(function(key) {
      var other = links.get(key);
      if (other == null) links.set(key, index);
      else parents[root(index)] = root(other);
    });
  });
  /** @type {Map<number, ReviewPrediction[]>} */ var components = new Map();
  rows.forEach(function(p, index) {
    var key = root(index);
    if (!components.has(key)) components.set(key, []);
    components.get(key)?.push(p);
  });
  reviewCardMemberIndex = new Map();
  components.forEach(function(members) {
    members.forEach(function(p) { reviewCardMemberIndex.set(p.id, members); });
  });
  reviewCardIndexRows = predictions;
  reviewCardIndexScope = reviewCardScopeKey();
}

/** @param {ReviewPrediction} pred */
function reviewBaseCardKey(pred) {
  var key;
  if (reviewEvidenceFilter !== 'all') {
    // Evidence review is per subject, never the other frames in its burst.
    key = 'subject:' + pred.detection_id + ':' + (pred.consensus_species_key || pred.species);
  } else if (currentModel === 'all' && !currentLabelsFingerprint && pred.card_id) {
    key = pred.card_id;
  } else {
    key = JSON.stringify([pred.model, pred.group_id || pred.id, pred.consensus_species_key || pred.species]);
  }
  return key + ':' + pred.status;
}

/** @param {ReviewPrediction} pred */
function reviewCardKey(pred) {
  var members = reviewCardMemberIndex.get(pred.id);
  return reviewBaseCardKey(pred) + ':' + (members?.[0]?.id ?? pred.id);
}

/** @param {ReviewPrediction} pred @returns {ReviewPrediction[]} */
function reviewCardMembers(pred) {
  if (reviewCardIndexRows !== predictions || reviewCardIndexScope !== reviewCardScopeKey()) getVisibleItems(true);
  return reviewCardMemberIndex.get(pred.id) || [];
}

/** @param {ReviewPrediction} pred */
function reviewCardRequest(pred) {
  return {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({
    review_prediction_ids: reviewCardMembers(pred).map(function(p) { return p.id; }),
    observed_workspace_id: reviewQueue.workspace,
  })};
}

/** @param {ReviewPrediction} pred */
function reviewCardModelsHtml(pred) {
  /** @type {Map<string, ReviewPrediction[]>} */ var models = new Map();
  reviewCardMembers(pred).forEach(function(p) {
    var name = p.model || 'Unknown model';
    if (!models.has(name)) models.set(name, []);
    models.get(name)?.push(p);
  });
  return '<div class="review-card-models">' + Array.from(models, function(entry) {
    var name = entry[0], rows = entry[1];
    var scores = rows.filter(function(p) {
      return p.species_key && p.consensus_species_key
        ? p.species_key === p.consensus_species_key
        : !p.consensus_species || p.species === p.consensus_species;
    }).map(function(p) { return p.confidence; }).filter(Number.isFinite);
    var best = scores.reduce(function(highest, value) { return Math.max(highest, value); }, -Infinity);
    var score = scores.length ? reviewRelativeScore(best) + '% best relative score' : 'Relative score unavailable for this species';
    /** @type {Map<string, string>} */ var votes = new Map();
    rows.forEach(function(p) {
      if (p.group_id && p.total_votes) votes.set(p.group_id, (p.vote_count ?? '?') + '/' + p.total_votes + ' votes');
    });
    return '<span class="badge-group">' + escapeHtml(name) + ' · ' + score +
      (votes.size ? ' · ' + escapeHtml(Array.from(votes.values()).join(', ')) : '') + '</span>';
  }).join('') + '</div>';
}

/** @param {ReviewPrediction} pred */
function reviewCardPhotoCount(pred) {
  return new Set(reviewCardMembers(pred).map(function(p) { return p.photo_id; })).size;
}

function reviewCardCounts() {
  var rows = getVisibleItems(true, true);
  /** @type {Record<string, Set<string>>} */ var counts = {all: new Set()};
  rows.forEach(function(p) {
    var key = reviewEvidenceFilter === 'all' ? reviewCardKey(p) : 'subject:' + p.detection_id;
    counts.all?.add(key);
    if (!counts[p.status]) counts[p.status] = new Set();
    counts[p.status]?.add(key);
  });
  return Object.fromEntries(Object.entries(counts).map(function(entry) { return [entry[0], entry[1].size]; }));
}
