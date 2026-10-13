// Shared by the Import result and Jobs detail views.
function importFailedBeforeStart(result) {
  if (!result) return false;
  if (result.import_not_started === true) return true;
  // Older disk-space failures persisted only this error, before the worker
  // entered run_import_job. Keep those existing history rows retryable too.
  return result.error === 'Not enough free space on this computer for temporary processing. Free up space or import fewer photos.';
}

function canRetryImportDirectToNas(job) {
  if (!job || job.type !== 'import' || job.status !== 'failed' ||
      !importFailedBeforeStart(job.result)) return false;
  var cfg = job.config || {}, result = job.result || {};
  if (!cfg.local_processing || cfg.parent_import_job_id) return false;
  return ![cfg.carry_photo_ids, cfg.untagged_photo_ids, cfg.recover_landed_files,
    result.photo_ids, result.carried_photo_ids, result.recovered_photo_ids,
    result.landed_files].some(function(scope) {
      return scope && Object.keys(scope).length > 0;
    });
}

function directNasImportRetryBody(job, body) {
  if (!body || !canRetryImportDirectToNas(job)) return null;
  body.local_processing = false;
  body.defer_nas_transfer = false;
  body.retry_direct_to_nas = true;
  delete body.after_process_move;
  return body;
}
