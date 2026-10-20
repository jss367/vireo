/* Export job outcome for the export modal (Browse and Photo Editor).
 *
 * Starting an export only queues a background job, so the modal's "Export
 * started" toast says nothing about whether any file was written. A job that
 * fails at once (an offline original folder, say) used to be visible only in
 * the jobs panel, and the click looked like it had done nothing. watch()
 * follows the job's stream and toasts what actually happened; outcome() is
 * the pure mapping from the job's terminal event to that toast.
 *
 * Classic script: VireoExportJob is the page global the export modals call.
 */
/* exported VireoExportJob */

/**
 * @typedef {{exported?: number, errors?: string[], renamed?: number,
 *   destination?: string, destinations?: string[],
 *   written_destinations?: string[], metadata?: {metadata_free: boolean,
 *   description: string, categories: Record<string, {requested: number,
 *   written: number, unavailable: number, failed: number, not_attempted: number}>}}} ExportJobResult
 * @typedef {{status?: string, result?: ExportJobResult | null,
 *   errors?: string[]}} ExportJobDone
 */
var VireoExportJob = (function() {
  var VISIBLE_ERRORS = 2;

  /** @param {number} n */
  function photos(n) {
    return n.toLocaleString() + ' photo' + (n === 1 ? '' : 's');
  }

  // Where the files went: one folder by name, several by count. Only folders
  // that received a file count; a folder whose photos all failed does not.
  /** @param {ExportJobResult} result */
  function where(result) {
    var destinations = result.written_destinations || result.destinations || [];
    if (destinations.length > 1) return ' to ' + destinations.length + ' folders';
    var destination = destinations[0] || result.destination;
    return destination ? ' to ' + destination : '';
  }

  /** @param {string[]} errors */
  function errorDetail(errors) {
    var shown = errors.slice(0, VISIBLE_ERRORS).join('; ');
    var more = errors.length - VISIBLE_ERRORS;
    return more > 0 ? shown + '; and ' + more.toLocaleString() + ' more' : shown;
  }

  /** @param {ExportJobResult} result */
  function metadataDetail(result) {
    var metadata = result.metadata;
    if (!metadata) return '';
    if (metadata.metadata_free) return '. Metadata: none selected (metadata-free export)';
    /** @type {Record<string, string>} */
    var labels = {species: 'Species keywords', capture_date: 'Capture date',
      capture_time: 'Capture time', rating: 'Rating', location: 'Location (GPS)',
      camera: 'Camera & exposure'};
    return '. Metadata (photo counts): ' + Object.entries(metadata.categories).map(function(entry) {
      var field = entry[0];
      var counts = entry[1];
      var text = (labels[field] || field) + ' ' + counts.written + '/' + counts.requested + ' written';
      if (counts.unavailable) text += ', ' + counts.unavailable + ' unavailable';
      if (counts.failed) text += ', ' + counts.failed + ' failed';
      if (counts.not_attempted) text += ', ' + counts.not_attempted + ' not attempted';
      return text;
    }).join('; ');
  }

  /** @param {ExportJobResult} result */
  function missingMetadata(result) {
    return !!(result.metadata && Object.values(result.metadata.categories).some(function(counts) {
      return counts.unavailable || counts.failed || counts.not_attempted;
    }));
  }

  /**
   * The job's `complete` event ({status, result, errors}) to the
   * {message, type} showToast shows for it.
   * @param {ExportJobDone | null | undefined} done
   * @returns {{message: string, type: string}}
   */
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
    message += metadataDetail(result);
    var type = errors.length ? 'error'
      : done.status === 'cancelled' || missingMetadata(result) ? 'warning'
      : exported ? 'success' : 'warning';
    return {message: message, type: type};
  }

  /** @param {string | null | undefined} jobId */
  function watch(jobId) {
    if (!jobId || typeof safeEventSource !== 'function') return;
    safeEventSource('/api/jobs/' + encodeURIComponent(jobId) + '/stream', {
      /** @param {ExportJobDone} done */
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
