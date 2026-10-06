// Pipeline slot polling and the queued/running status line.
// Classic page script; load boot.js after all definitions.

// -- Pipeline slot poller --
// Server-side queue (Step 4 of pipeline-concurrency) lets a click
// while another run is active land on the queue instead of failing.
// We poll /api/pipeline/slots to (1) flip the Start button label to
// "Queue Pipeline" when no slot is free, and (2) show a small
// "N running . M queued" status line linking to /jobs.
var _slotInfo = { active: 0, queued: 0, slot_cap: 1 };
var _slotPollTimer = null;
var _SLOT_POLL_MS = 2000;

function _applySlotInfo(info) {
  _slotInfo = info || _slotInfo;
  // Label flip — only when *this tab* isn't itself running a pipeline,
  // because while it is, the same button is the Stop button.
  if (!_pipelineRunning) {
    var btn = document.getElementById('btnStartPipeline');
    if (btn) {
      var queueing = _slotInfo.active >= _slotInfo.slot_cap;
      btn.textContent = queueing ? 'Queue Pipeline' : 'Start Pipeline';
    }
  }
  // Status line: hidden when there's nothing to report.
  var line = document.getElementById('pipelineSlotStatus');
  var counts = document.getElementById('pipelineSlotCounts');
  if (!line || !counts) return;
  if (_slotInfo.active === 0 && _slotInfo.queued === 0) {
    line.style.display = 'none';
    return;
  }
  counts.textContent = _slotInfo.active + ' running • ' + _slotInfo.queued + ' queued';
  line.style.display = '';
}

async function refreshSlotInfo() {
  try {
    var data = await safeFetch('/api/pipeline/slots', {}, { toast: false });
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
