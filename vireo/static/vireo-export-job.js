/* Export job outcome for the export modal (Browse and Photo Editor).
 *
 * Starting an export only queues a background job, so the modal's "Export
 * started" toast says nothing about whether any file was written. A job that
 * fails at once (an offline original folder, say) used to be visible only in
 * the jobs panel, and the click looked like it had done nothing. watch()
 * follows the job's stream and toasts what actually happened; outcome() is
 * the pure mapping from the job's terminal event to that toast.
 */
var VireoExportJob = (function() {
  var VISIBLE_ERRORS = 2;

  function photos(n) {
    return n.toLocaleString() + ' photo' + (n === 1 ? '' : 's');
  }

  // Where the files went: one folder by name, several by count. Only folders
  // that received a file count; a folder whose photos all failed does not.
  function where(result) {
    var destinations = result.written_destinations || result.destinations || [];
    if (destinations.length > 1) return ' to ' + destinations.length + ' folders';
    var destination = destinations[0] || result.destination;
    return destination ? ' to ' + destination : '';
  }

  function errorDetail(errors) {
    var shown = errors.slice(0, VISIBLE_ERRORS).join('; ');
    var more = errors.length - VISIBLE_ERRORS;
    return more > 0 ? shown + '; and ' + more.toLocaleString() + ' more' : shown;
  }

  // done: the job's `complete` event ({status, result, errors}).
  // Returns {message, type} for showToast.
  function outcome(done) {
    done = done || {};
    var result = done.result;
    if (done.status === 'expired') {
      return {
        message: 'Export finished, but its result is no longer available. Check the jobs panel.',
        type: 'warning',
      };
    }
    if (!result) {
      if (done.status === 'cancelled') {
        return {message: 'Export stopped before any photo was exported', type: 'warning'};
      }
      var jobErrors = done.errors || [];
      return {
        message: 'Export failed: ' + (jobErrors.length
          ? errorDetail(jobErrors)
          : 'the job ended before producing a result.'),
        type: 'error',
      };
    }
    var exported = result.exported || 0;
    var errors = result.errors || [];
    var message;
    if (done.status === 'cancelled') {
      message = exported
        ? 'Export stopped after exporting ' + photos(exported) + where(result)
        : 'Export stopped before any photo was exported';
    } else if (exported) {
      message = 'Exported ' + photos(exported) + where(result);
    } else {
      message = 'Nothing was exported';
    }
    if (exported && result.renamed) {
      message += ' (' + result.renamed.toLocaleString() + ' saved with a numbered name)';
    }
    if (errors.length) {
      message += '. ' + photos(errors.length) + ' failed: ' + errorDetail(errors);
    }
    var type = errors.length ? 'error'
      : done.status === 'cancelled' ? 'warning'
      : exported ? 'success' : 'warning';
    return {message: message, type: type};
  }

  function watch(jobId) {
    if (!jobId || typeof safeEventSource !== 'function') return;
    safeEventSource('/api/jobs/' + encodeURIComponent(jobId) + '/stream', {
      onComplete: function(done) {
        var toast = outcome(done);
        showToast(toast.message, toast.type);
      },
      // The stream dropped (or the job was already gone) before the outcome
      // arrived, so say where to find it rather than leaving the user guessing.
      // This replaces the generic "Connection lost" toast, not adds to it.
      quietError: true,
      onError: function() {
        showToast('Could not follow the export to the end. Its result is in the jobs panel.', 'warning');
      },
    });
  }

  return {outcome: outcome, watch: watch};
})();
