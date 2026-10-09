// Duplicates page: initial catalog/scan loading and restored-result banners.
// Classic script sharing the page globals, read only after DOMContentLoaded.

var _duplicateLoadEpoch = 0;

function cancelDuplicateLoad() {
  ++_duplicateLoadEpoch;
  document.getElementById('initialLoading').style.display = 'none';
  document.getElementById('loadError').style.display = 'none';
  document.getElementById('scanBtn').disabled = false;
}

async function tryRestoreLastScan(forceCleanup) {
  if (_scanInProgress) return;
  var epoch = ++_duplicateLoadEpoch;
  function current() { return epoch === _duplicateLoadEpoch && !_scanInProgress; }
  document.getElementById('initialLoading').style.display = '';
  document.getElementById('loadError').style.display = 'none';
  document.getElementById('scanBtn').disabled = true;
  setEmptyVisible(false);
  hideRestoredBanner();
  // A refresh must not leave older cleanup buttons actionable beneath the
  // loading/error state. A subsequent filter edit must not revive them either.
  _lastScanResult = null;
  _proposals = [];
  document.getElementById('results').innerHTML = '';
  document.getElementById('summary').style.display = 'none';
  document.getElementById('applyBar').style.display = 'none';
  try {
    var cleanup = forceCleanup === true ||
      new URLSearchParams(window.location.search).get('show') === 'resolved';
    var data = cleanup ? null :
      await safeFetch('/api/duplicates/last-scan', undefined, { toast: false });
    if (!current()) return;
    var result;
    if (cleanup || !data || !data.found) {
      result = await safeFetch('/api/duplicates/cleanup', undefined, { toast: false });
      cleanup = true;
    } else {
      result = data.result || {};
    }
    if (!current()) return;
    // Respect the first filter pass and any newer query still in flight.
    await _filterReady;
    while (_pendingFilterRefresh) {
      var pending = _pendingFilterRefresh;
      await pending;
      if (_pendingFilterRefresh === pending) break;
    }
    if (!current()) return;
    if (cleanup && !result.proposals.length &&
        new URLSearchParams(window.location.search).get('show') !== 'resolved' &&
        forceCleanup !== true) {
      setEmptyVisible(true);
      return;
    }
    renderResults(result);
    if (cleanup) {
      document.getElementById('catalogBanner').style.display = '';
      return;
    }
    document.getElementById('restoredAgo').textContent = formatTimeAgo(data.finished_at);
    var stale = result.stale_group_count || 0;
    var remaining = (result.proposals || []).length;
    document.getElementById('restoredStale').textContent = (stale && remaining)
      ? ' ' + stale.toLocaleString() + (stale === 1
          ? ' group from that scan no longer matches your catalog and is hidden.'
          : ' groups from that scan no longer match your catalog and are hidden.')
      : '';
    document.getElementById('restoredFreshness').textContent = restoredFreshnessText(result);
    document.getElementById('restoredBanner').style.display = '';
  } catch (e) {
    if (!current()) return;
    document.getElementById('loadErrorText').textContent =
      'Could not load duplicate results. ' + (e.message || 'Try again.');
    document.getElementById('loadError').style.display = '';
  } finally {
    if (current()) {
      document.getElementById('initialLoading').style.display = 'none';
      document.getElementById('scanBtn').disabled = false;
    }
  }
}

function formatTimeAgo(isoStr) {
  if (!isoStr) return '';
  // job_history timestamps are naive local-time ISO strings written by
  // datetime.now().isoformat() in vireo/jobs.py. Pass them through as-is so
  // JS parses them as local time; appending 'Z' would shift the displayed
  // age by the local UTC offset (e.g. a fresh scan appearing hours old).
  var d = new Date(isoStr);
  var sec = Math.floor((new Date() - d) / 1000);
  if (sec < 60) return 'just now';
  if (sec < 3600) return Math.floor(sec / 60) + 'm ago';
  if (sec < 86400) return Math.floor(sec / 3600) + 'h ago';
  return d.toLocaleString();
}

// Whether a new scan would show the same groups, from the counts
// last-scan compares against the catalog. Only asks for a rescan when one
// would actually show something different.
function restoredFreshnessText(result) {
  var added = result.new_group_count || 0;
  var changed = result.changed_group_count || 0;
  if (!added && !changed) {
    return ' Still up to date: no duplicates have been added or changed ' +
      'in your catalog since then.';
  }
  var parts = [];
  if (added) {
    parts.push(added.toLocaleString() + (added === 1
      ? ' new duplicate group has appeared'
      : ' new duplicate groups have appeared'));
  }
  if (changed) {
    parts.push(changed.toLocaleString() + (changed === 1
      ? ' group from that scan has changed'
      : ' groups from that scan have changed'));
  }
  return ' Since then, ' + parts.join(' and ') +
    '. Scan again to see them as they are now.';
}
