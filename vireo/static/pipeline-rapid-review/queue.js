// Queue navigation: finding sessions by key or raw index, the ?enc=&burst=
// deep link, staged-change detection, the queue controls, the staged
// decisions prompt, and changing the queue.
// Classic page script; load boot.js after all definitions.

/** @param {RapidSession[]} sessions @param {string} key */
function findSessionIndexByKeyIn(sessions, key) {
  for (var i = 0; i < sessions.length; i++) {
    if (sessionKey(sessions[i]) === key) return i;
  }
  return -1;
}

/** @param {string} key */
function findSessionIndexByKey(key) {
  return findSessionIndexByKeyIn(rapid.sessions, key);
}

function requestedSessionKey() {
  var params = new URLSearchParams(window.location.search);
  var enc = params.get('enc');
  var burst = params.get('burst');
  if (enc == null || burst == null) return '';
  var encIdx = parseInt(enc, 10);
  var burstIdx = parseInt(burst, 10);
  if (!Number.isFinite(encIdx) || !Number.isFinite(burstIdx)) return '';
  return encIdx + ':' + burstIdx;
}

/** @param {number} rawIndex */
function findNextFilteredIndexAfterRaw(rawIndex) {
  for (var i = 0; i < rapid.sessions.length; i++) {
    if (/** @type {RapidSession} */ (rapid.sessions[i]).rawIndex > rawIndex) return i;
  }
  return -1;
}

/** @param {number} rawIndex */
function findNearestFilteredIndex(rawIndex) {
  var next = findNextFilteredIndexAfterRaw(rawIndex);
  if (next >= 0) return next;
  for (var i = rapid.sessions.length - 1; i >= 0; i--) {
    if (/** @type {RapidSession} */ (rapid.sessions[i]).rawIndex < rawIndex) return i;
  }
  return -1;
}

function hasStagedChanges() {
  var session = rapid.sessions[rapid.sessionIndex];
  var input = /** @type {HTMLInputElement} */ (document.getElementById('speciesInput'));
  var currentSpecies = input ? (input.value || '').trim() : '';
  return rapid.history.length > 0 || (!!session && currentSpecies !== (session.species || ''));
}

function findInitialSession() {
  var key = requestedSessionKey();
  if (key) {
    var requestedIdx = findSessionIndexByKey(key);
    if (requestedIdx >= 0) return requestedIdx;
  }
  for (var j = 0; j < rapid.sessions.length; j++) {
    if (/** @type {RapidSession} */ (rapid.sessions[j]).ids.length > 1) return j;
  }
  return rapid.sessions.length ? 0 : -1;
}

function renderQueueControls() {
  var select = /** @type {HTMLSelectElement} */ (document.getElementById('queueFilter'));
  var include = /** @type {HTMLInputElement} */ (document.getElementById('includeRejectedToggle'));
  var multi = /** @type {HTMLInputElement} */ (document.getElementById('multiOnlyToggle'));
  if (select) select.value = rapid.queue.filter;
  if (include) include.checked = rapid.queue.includeRejected;
  if (multi) multi.checked = rapid.queue.multiOnly;
  var queue = QUEUE_FILTERS[rapid.queue.filter] || QUEUE_FILTERS.all;
  var count = rapid.sessions.length;
  var total = rapid.allSessions.length;
  var text = queue.label + ': ' + count + ' of ' + total + ' bursts';
  if (rapid.sessionIndex >= 0 && count) {
    text += ' - ' + (rapid.sessionIndex + 1) + ' of ' + count;
  }
  /** @type {HTMLElement} */ (document.getElementById('queueCount')).textContent = text;
}

/** @returns {Promise<string|null>} */
function promptStagedQueueChange() {
  var prompt = /** @type {HTMLElement} */ (document.getElementById('queuePrompt'));
  var applyBtn = /** @type {HTMLButtonElement|null} */ (prompt.querySelector('[data-queue-choice="apply"]'));
  if (applyBtn) {
    var diff = rapidComputeApplyDiff();
    applyBtn.textContent = rapidApplyButtonText('Apply', diff);
    applyBtn.title = rapidApplyTitle(diff, false);
  }
  prompt.hidden = false;
  return new Promise(function(resolve) {
    rapid.queuePromptResolve = resolve;
  });
}

function isQueuePromptOpen() {
  var prompt = /** @type {HTMLElement} */ (document.getElementById('queuePrompt'));
  return !!(prompt && !prompt.hidden);
}

function bindQueuePromptChoices() {
  /** @type {HTMLElement} */ (document.getElementById('queuePrompt')).addEventListener('click', function(e) {
    var btn = /** @type {HTMLElement} */ (e.target).closest('[data-queue-choice]');
    if (!btn || !rapid.queuePromptResolve) return;
    var resolve = rapid.queuePromptResolve;
    rapid.queuePromptResolve = null;
    /** @type {HTMLElement} */ (document.getElementById('queuePrompt')).hidden = true;
    resolve(btn.getAttribute('data-queue-choice'));
  });
}

/** @param {Partial<RapidQueue>} patch */
async function changeQueue(patch) {
  if (rapid.applying) {
    renderQueueControls();
    return;
  }
  if (hasStagedChanges()) {
    var choice = await promptStagedQueueChange();
    if (choice === 'stay') {
      renderQueueControls();
      return;
    }
    if (choice === 'apply') {
      var applied = await applyCurrent(false);
      if (!applied) {
        renderQueueControls();
        return;
      }
    }
  }
  Object.assign(rapid.queue, patch || {});
  var key = sessionKey(rapid.sessions[rapid.sessionIndex]);
  rebuildFilteredSessions();
  renderQueueControls();
  var idx = findSessionIndexByKey(key);
  if (idx < 0 && rapid.sessions.length) idx = 0;
  if (idx >= 0) openSession(idx);
  else showEmptyQueue();
}

/** @param {number} index */
function requestOpenSession(index) {
  openSession(index);
}
