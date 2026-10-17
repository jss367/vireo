// Loading: the undo/redo history hooks, the results fetch, the empty queue,
// and opening a burst with its seeded /group/state flags and keywords.
// Classic page script; load boot.js after all definitions.

// Undo/redo lives in the shared navbar. Boot registers these hooks where
// the inline script assigned them.
function registerRapidHistoryHooks() {
  window.beforeHistoryChange = function() {
    if (rapid.loadError) {
      showToast('Rapid Review could not load. Use Retry before undoing or redoing saved edits.', 'error');
      return false;
    }
    if (rapid.applying || (rapid.sessionIndex >= 0 && !rapid.seeded)) {
      showToast('Wait for Rapid Review to finish loading or saving.', 'info');
      return false;
    }
    if (hasStagedChanges() || isQueuePromptOpen()) {
      showToast('Apply or discard your staged decisions before undoing saved edits.', 'info');
      return false;
    }
    return true;
  };

  window.afterHistoryChange = async function() {
    rapid.loadError = false;
    rapid.historyRefreshFailed = false;
    var previous = rapid.sessions[rapid.sessionIndex];
    var previousIds = previous ? previous.ids.slice() : [];
    rapid.openToken++;
    rapid.applying = true;
    rapid.seeded = false;
    renderApplyState();
    try {
      var results = await safeFetch('/api/pipeline/results', {}, {toast: false});
      rapid.results = results;
      rapid.photoMap = {};
      (results.photos || []).forEach(function(photo) { rapid.photoMap[photo.id] = photo; });
      rapid.allSessions = buildSessions(results);
      rebuildFilteredSessions();
      renderQueueControls();
      renderBurstList();
      var index = rapid.sessions.findIndex(function(session) {
        return session.ids.some(function(id) { return previousIds.includes(id); });
      });
      if (index < 0 && rapid.sessions.length) index = 0;
      if (index >= 0) await openSession(index);
      else showEmptyQueue();
    } catch (error) {
      rapid.loadError = true;
      rapid.historyRefreshFailed = true;
      rapid.seeded = false;
      rapid.openToken++;
      renderAll();
      throw error;
    } finally {
      rapid.applying = false;
      renderApplyState();
    }
  };
}

async function retryRapidReview() {
  if (rapid.applying) return;
  try {
    if (rapid.historyRefreshFailed) await window.afterHistoryChange();
    else await openSession(rapid.sessionIndex);
  } catch (error) {
    showToast('Rapid Review could not refresh. Use Retry to try again.', 'error');
  }
}

async function initRapidReview() {
  try {
    rapid.results = await safeFetch('/api/pipeline/results', {}, { toast: false });
  } catch (e) {
    document.getElementById('burstTitle').textContent = 'No pipeline results';
    document.getElementById('burstSubtitle').innerHTML = '<a class="rapid-link" href="/pipeline">Open Pipeline</a>';
    document.getElementById('reviewSurface').innerHTML = '<div class="empty-state">Run pipeline review results first.</div>';
    return;
  }
  rapid.photoMap = {};
  (rapid.results.photos || []).forEach(function(p) { rapid.photoMap[p.id] = p; });
  rapid.allSessions = buildSessions(rapid.results);
  var requestedKey = requestedSessionKey();
  rebuildFilteredSessions();
  if (requestedKey &&
      findSessionIndexByKey(requestedKey) < 0 &&
      findSessionIndexByKeyIn(rapid.allSessions, requestedKey) >= 0) {
    rapid.queue.filter = 'all';
    rebuildFilteredSessions();
  }
  renderQueueControls();
  renderBurstList();
  var idx = findInitialSession();
  if (idx >= 0) openSession(idx);
  else {
    showEmptyQueue();
  }
}

function showEmptyQueue() {
  rapid.sessionIndex = -1;
  rapid.items = [];
  rapid.picks = new Set();
  rapid.rejects = new Set();
  rapid.reviewed = new Set();
  rapid.touched = new Set();
  rapid.initialFlags = {};
  rapid.initialSpeciesKeywords = {};
  rapid.keywordStateSpecies = '';
  rapid.initialSpecies = '';
  rapid.history = [];
  rapid.currentIndex = -1;
  rapid.seeded = false;
  var queue = QUEUE_FILTERS[rapid.queue.filter] || QUEUE_FILTERS.all;
  document.getElementById('burstTitle').textContent = queue.label;
  document.getElementById('burstSubtitle').textContent = '0 matching bursts';
  var img = document.getElementById('currentPhoto');
  var empty = document.getElementById('stageEmpty');
  var pill = document.getElementById('statusPill');
  if (img) {
    img.removeAttribute('src');
    img.style.display = 'none';
  }
  if (empty) {
    empty.style.display = '';
    empty.textContent = queue.empty;
  }
  if (pill) pill.style.display = 'none';
  document.getElementById('speciesInput').value = '';
  document.getElementById('filename').textContent = '-';
  document.getElementById('metricQuality').textContent = '-';
  document.getElementById('metricSharpness').textContent = '-';
  document.getElementById('metricLabel').textContent = '-';
  document.getElementById('metricReviewed').textContent = '0/0';
  document.getElementById('filmstrip').innerHTML = '';
  document.getElementById('pickLane').innerHTML = '';
  document.getElementById('reviewLane').innerHTML = '';
  document.getElementById('rejectLane').innerHTML = '';
  document.getElementById('pickCount').textContent = '0';
  document.getElementById('reviewCount').textContent = '0';
  document.getElementById('rejectCount').textContent = '0';
  renderBurstList();
  renderQueueControls();
  renderApplyState();
}

async function openSession(index) {
  if (rapid.historyRefreshFailed) return retryRapidReview();
  if (index < 0 || index >= rapid.sessions.length) return;
  rapid.sessionIndex = index;
  var session = rapid.sessions[index];
  rapid.items = session.ids.map(function(id) { return rapid.photoMap[id]; }).filter(Boolean);
  rapid.picks = new Set();
  rapid.rejects = new Set();
  rapid.reviewed = new Set();
  rapid.touched = new Set();
  rapid.initialFlags = {};
  rapid.initialSpeciesKeywords = {};
  var sessionSpeciesList = parseSpeciesList(session.species || '');
  rapid.keywordStateSpecies = joinSpecies(sessionSpeciesList);
  rapid.initialSpecies = session.species || '';
  rapid.history = [];
  rapid.currentIndex = rapid.items.length ? 0 : -1;
  resetRapidZoom();
  rapid.seeded = false;
  rapid.loadError = false;
  // Bump the token so any in-flight /group/state fetch from a previous
  // openSession lands as stale and gets ignored when it resolves.
  var token = (rapid.openToken || 0) + 1;
  rapid.openToken = token;
  document.getElementById('speciesInput').value = session.species || '';
  renderAll();

  try {
    var data = await safeFetch('/api/pipeline/group/state', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        photo_ids: session.ids,
        species: sessionSpeciesList[0] || '',
        species_list: sessionSpeciesList,
      }),
    });
    // Drop the response if the user has switched bursts since this fetch
    // started — seeding burst A's DB state into burst B's queue would
    // mis-flag photos on Apply.
    if (rapid.openToken !== token) return;
    var photos = (data && data.photos) || {};
    rapid.items.forEach(function(p) {
      var st = photos[p.id] || {};
      var flag = st.flag || p.flag || 'none';
      rapid.initialFlags[p.id] = flag;
      rapid.initialSpeciesKeywords[p.id] = rapidKeywordStateFor(st, sessionSpeciesList);
      if (flag === 'flagged') rapid.picks.add(p.id);
      else if (flag === 'rejected') rapid.rejects.add(p.id);
    });
    rapid.seeded = true;
    renderAll();
  } catch (e) {
    if (rapid.openToken !== token) return;
    rapid.seeded = false;
    rapid.loadError = true;
    renderAll();
  }
}
