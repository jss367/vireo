// Pipeline slot polling and the queued/running status line.
// Classic page script; load boot.js after all definitions.

// -- Pipeline slot poller --
// Server-side queue (Step 4 of pipeline-concurrency) lets a click
// while another run is active land on the queue instead of failing.
// We poll /api/pipeline/slots to (1) flip the Start button label to
// "Queue Pipeline" when no slot is free, and (2) show a small
// "N running . M queued" status line linking to /jobs.
var _slotInfo = { active: 0, queued: 0, slot_cap: 1 };
/** @type {ReturnType<typeof setTimeout> | null} */ var _slotPollTimer = null;
var _SLOT_POLL_MS = 2000;

/** @param {PipelineSlotInfo} info */
function _applySlotInfo(info) {
  _slotInfo = info || _slotInfo;
  // Label flip — only when *this tab* isn't itself running a pipeline,
  // because while it is, the same button is the Stop button.
  if (!_pipelineRunning) {
    var btn = /** @type {HTMLButtonElement} */ (document.getElementById('btnStartPipeline'));
    if (btn) {
      var queueing = _slotInfo.active >= _slotInfo.slot_cap;
      btn.textContent = queueing ? 'Queue Pipeline' : 'Start Pipeline';
    }
  }
  // Status line: hidden when there's nothing to report.
  var line = /** @type {HTMLElement} */ (document.getElementById('pipelineSlotStatus'));
  var counts = /** @type {HTMLElement} */ (document.getElementById('pipelineSlotCounts'));
  if (!line || !counts) return;
  if (_slotInfo.active === 0 && _slotInfo.queued === 0) {
    line.style.display = 'none';
    return;
  }
  counts.textContent = _slotInfo.active + ' running • ' + _slotInfo.queued + ' queued';
  line.style.display = '';
}

// Track the job started in this tab across navigation, including completion.
var _lastProcessRun = '';
/** @type {number | null} */ var _processWorkspace = null;

/** @param {number} workspaceId */
function restoreProcessRun(workspaceId) {
  _processWorkspace = workspaceId;
  try {
    _lastProcessRun = sessionStorage.getItem('vireo.lastProcessRun.' + workspaceId) || '';
  } catch (e) {}
  if (_lastProcessRun) refreshSlotInfo();
}

/** @param {PipelineSlotInfo['job']} [job] */
function showProcessRun(job) {
  var link = /** @type {HTMLAnchorElement | null} */ (document.getElementById('pipelineLastRun'));
  if (!link) return;
  if (!_lastProcessRun || !job) {
    link.hidden = true;
    link.removeAttribute('href');
    return;
  }
  link.href = '/jobs?job=' + encodeURIComponent(_lastProcessRun);
  link.hidden = false;
  var status = job.status === 'running' ? (job.phase || job.status) : job.status;
  link.textContent = 'View your last Process run' + (status ? ' · ' + status : '');
}

/** @param {string} id @param {number} workspaceId */
function rememberProcessRun(id, workspaceId) {
  try { sessionStorage.setItem('vireo.lastProcessRun.' + workspaceId, id); } catch (e) {}
  if (_processWorkspace !== workspaceId) return;
  _lastProcessRun = id;
  refreshSlotInfo();
}

async function refreshSlotInfo() {
  try {
    var tracked = _lastProcessRun;
    var url = '/api/pipeline/slots' + (tracked ? '?job=' + encodeURIComponent(tracked) : '');
    /** @type {PipelineSlotInfo} */ var data = await safeFetch(url, {}, { toast: false });
    if (data && tracked === _lastProcessRun) showProcessRun(data.job);
    if (data) _applySlotInfo(data);
  } catch(e) {
    // Network blip — keep last known state; don't toast.
  }
}

function _startSlotPolling() {
  if (_slotPollTimer !== null) return;
  refreshSlotInfo();
  _slotPollTimer = setInterval(refreshSlotInfo, _SLOT_POLL_MS);
}

function _stopSlotPolling() {
  if (_slotPollTimer !== null) {
    clearInterval(_slotPollTimer);
    _slotPollTimer = null;
  }
}

// Slot polling follows tab visibility.
function bindSlotPolling() {
  showProcessRun();
  document.addEventListener('visibilitychange', function() {
    if (document.hidden) {
      _stopSlotPolling();
    } else {
      _startSlotPolling();
    }
  });

  // Kick off polling only when visible; hidden tabs should stay paused.
  if (!document.hidden) {
    _startSlotPolling();
  }
}
