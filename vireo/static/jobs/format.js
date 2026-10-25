// Formatting helpers: elapsed time, status icons, and progress (escaping
// is the shared escapeHtml from vireo-utils.js).

/** @param {number} secs */ export function formatElapsed(secs) {
  if (secs < 60) return Math.round(secs) + 's';
  if (secs < 3600) return Math.floor(secs / 60) + 'm ' + Math.round(secs % 60) + 's';
  return Math.floor(secs / 3600) + 'h ' + Math.floor((secs % 3600) / 60) + 'm';
}

/** @param {string} iso */ export function timeAgo(iso) {
  var secs = (Date.now() - new Date(iso).getTime()) / 1000;
  if (secs < 60) return 'just now';
  if (secs < 3600) return Math.floor(secs / 60) + 'm ago';
  if (secs < 86400) return Math.floor(secs / 3600) + 'h ago';
  return Math.floor(secs / 86400) + 'd ago';
}

/** @param {boolean} isExpanded */ export function toggleIcon(isExpanded) {
  return '<span class="tree-toggle">' + (isExpanded ? '\u25BC' : '\u25B6') + '</span>';
}

/** @param {string} status @param {number} warningCount */ export function statusIcon(status, warningCount) {
  switch (status) {
    case 'pending': return '<span class="tree-status-icon pending">\u25CB</span>';
    case 'running': return '<span class="tree-status-icon running">\u25CB</span>';
    case 'completed':
      if (warningCount > 0) return '<span class="tree-status-icon warning" title="Completed with warnings">!</span>';
      return '<span class="tree-status-icon completed">\u2713</span>';
    case 'failed': return '<span class="tree-status-icon failed">\u2717</span>';
    case 'cancelled': return '<span class="tree-status-icon cancelled">\u2298</span>';
    default: return '<span class="tree-status-icon pending">\u25CB</span>';
  }
}

/** @param {string | undefined} status */ export function isLiveStatus(status) {
  return status === 'running' || status === 'queued' ||
    status === 'pausing' || status === 'paused';
}

/** @param {JobsProgress | undefined} progress */ export function activeProgress(progress) {
  if (!progress) return null;
  if (typeof progress.phase_current === 'number' && (progress.phase_total || 0) > 0) {
    return {
      current: progress.phase_current,
      total: progress.phase_total || 0,
      label: progress.phase_label || progress.phase || 'Current phase',
      phase: true,
    };
  }
  if ((progress.total || 0) > 0) {
    return {
      current: progress.current || 0,
      total: progress.total || 0,
      label: 'Overall',
      phase: false,
    };
  }
  return null;
}

/** @param {{current:number,total:number} | null} info */ export function progressPct(info) {
  if (!info || !info.total) return 0;
  return Math.max(0, Math.min(100, Math.round((info.current / info.total) * 100)));
}

/** @param {JobsRow | undefined} job */ export function jobConfig(job) {
  return job && job.config && typeof job.config === 'object' ? job.config : {};
}

/** @param {number} n @param {string} word */ export function plural(n, word) {
  return n === 1 ? word : word + 's';
}
