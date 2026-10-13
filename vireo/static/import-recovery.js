// Shared by the Import result and Jobs detail views.
function importFailedBeforeStart(result) {
  if (!result) return false;
  if (result.import_not_started === true) return true;
  // Older disk-space failures persisted only this error, before the worker
  // entered run_import_job. Keep those existing history rows retryable too.
  return result.error === 'Not enough free space on this computer for temporary processing. Free up space or import fewer photos.';
}

function canRetryImportDirectToNas(job, rows) {
  if (!job || job.type !== 'import' || job.status !== 'failed' ||
      !importFailedBeforeStart(job.result)) return false;
  var cfg = job.config || {}, result = job.result || {};
  if (!cfg.local_processing || cfg.parent_import_job_id) return false;
  var descendantIds = new Set([job.id]), descendants = [];
  var changed = true;
  while (changed) {
    changed = false;
    (rows || []).forEach(function(row) {
      if (!row || row.type !== 'import' || descendantIds.has(row.id)) return;
      var childCfg = row.config || {};
      if (childCfg.root_import_job_id === job.id || descendantIds.has(childCfg.parent_import_job_id)) {
        descendantIds.add(row.id);
        descendants.push(row);
        changed = true;
      }
    });
  }
  if (descendants.some(function(row) {
    return !(row.result && row.result.never_started) &&
      !importFailedBeforeStart(row.result);
  })) return false;
  return ![cfg.carry_photo_ids, cfg.untagged_photo_ids, cfg.recover_landed_files,
    result.photo_ids, result.carried_photo_ids, result.recovered_photo_ids,
    result.landed_files].some(function(scope) {
      return scope && Object.keys(scope).length > 0;
    });
}

function directNasImportRetryBody(job, body, rows) {
  if (!body || !canRetryImportDirectToNas(job, rows)) return null;
  body.local_processing = false;
  body.defer_nas_transfer = false;
  body.retry_direct_to_nas = true;
  delete body.after_process_move;
  return body;
}
