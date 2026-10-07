// Duplicates page: the banner over a restored scan. Classic script; its
// functions are called by duplicates.html's tryRestoreLastScan.

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
  var stale = result.stale_group_count || 0;
  var remaining = (result.proposals || []).length;
  if (!added && !changed) {
    // All-stale with nothing left to show: ``renderResults`` already
    // writes "The last scan is out of date" into #results, so saying
    // "Still up to date" here would directly contradict it. Stay silent
    // and let the empty-state message speak alone.
    if (!remaining && stale) return '';
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
