// Shared by the Import result and Jobs detail views.
function importFailedBeforeStart(result) {
  if (!result) return false;
  if (result.import_not_started === true) return true;
  // Older disk-space failures persisted only this error, before the worker
  // entered run_import_job. Keep those existing history rows retryable too.
  return result.error === 'Not enough free space on this computer for temporary processing. Free up space or import fewer photos.';
}
