/* ---------- Catalog backups ---------- */
// The backup list, Back up now, and Restore. Restore runs a job that copies
// the backup beside the catalog; Vireo swaps it in at its next launch, so
// the page says so until that happens.

/**
 * @typedef {{name:string, kind:string, created_at:string, bytes:number, photos?:number|null,
 *   restorable:boolean, not_restorable_reason?:string|null, replaced_by?:string}} CatalogBackup
 * @typedef {{backup:string, backup_created_at?:string|null, staged_at?:string}} CatalogPendingRestore
 * @typedef {{backup:string, backup_created_at?:string|null, finished_at?:string|null, ok:boolean,
 *   error?:string|null, before_restore?:string|null}} CatalogLastRestore
 * @typedef {{directory:string, keep:number, daily_enabled:boolean, backups:CatalogBackup[],
 *   total_bytes:number, free_bytes:number|null, last_backup_at:string|null, next_due_at:string|null,
 *   pending_restore:CatalogPendingRestore|null, last_restore:CatalogLastRestore|null,
 *   active_job:{id:string, type:string, config:{backup?:string}}|null}} CatalogBackupStatus
 */

/** @type {CatalogBackupStatus | null} */
var catalogBackupState = null;
/** @type {string | null} */
var catalogBackupWatchedJob = null;
/** @type {string | null} */
var catalogBackupConfirming = null;

/** @param {string | null | undefined} iso */
function catalogBackupDate(iso) {
  if (!iso) return 'an unknown date';
  var date = new Date(iso);
  return isNaN(date.getTime()) ? iso : date.toLocaleString();
}

/** @param {string} kind */
function catalogBackupKindLabel(kind) {
  if (kind === 'daily') return 'Daily';
  if (kind === 'manual') return 'Back up now';
  if (kind === 'before-restore') return 'Before restore';
  return kind;
}

/** @param {string} name */
function catalogBackupByName(name) {
  var backups = catalogBackupState ? catalogBackupState.backups : [];
  return backups.find(function(b) { return b.name === name; }) || null;
}

async function loadCatalogBackups() {
  var summary = /** @type {HTMLElement} */ (document.getElementById('catalogBackupsSummary'));
  if (!summary) return;
  try {
    catalogBackupState = /** @type {CatalogBackupStatus} */ (await safeFetch('/api/catalog-backups', {}, { toast: false }));
  } catch (e) {
    summary.textContent = 'Could not read the catalog backups.';
    return;
  }
  renderCatalogBackups();
  var job = catalogBackupState.active_job;
  if (job && catalogBackupWatchedJob !== job.id) watchCatalogBackupJob(job.id, job.type);
}

function renderCatalogBackups() {
  var state = catalogBackupState;
  if (!state) return;
  var summary = /** @type {HTMLElement} */ (document.getElementById('catalogBackupsSummary'));
  var parts = [];
  if (state.backups.length) {
    parts.push(state.backups.length + (state.backups.length === 1 ? ' backup' : ' backups') +
      ' using ' + formatBytes(state.total_bytes) + ' in ' + state.directory + '.');
  } else {
    parts.push('No backups yet. They are kept in ' + state.directory + '.');
  }
  if (!state.daily_enabled) {
    parts.push('Daily backups are off.');
  } else {
    parts.push(state.last_backup_at ? 'Last backup ' + catalogBackupDate(state.last_backup_at) + '.' : 'The first daily backup starts within a few minutes.');
    if (state.next_due_at) parts.push('Next one after ' + catalogBackupDate(state.next_due_at) + '.');
    parts.push('Keeps the newest ' + state.keep + '.');
  }
  if (state.free_bytes != null) parts.push(formatBytes(state.free_bytes) + ' free on that disk.');
  summary.textContent = parts.join(' ');
  renderCatalogRestoreNotices();
  renderCatalogBackupList();
}

function renderCatalogRestoreNotices() {
  var state = /** @type {CatalogBackupStatus} */ (catalogBackupState);
  var notice = /** @type {HTMLElement} */ (document.getElementById('catalogBackupsRestoreNotice'));
  var html = '';
  var pending = state.pending_restore;
  if (pending) {
    html += '<div class="catalog-backup-notice catalog-backup-notice-warn">' +
      '<strong>Restore waiting.</strong> The next time Vireo starts, it replaces the catalog with the backup from ' +
      escapeHtml(catalogBackupDate(pending.backup_created_at)) + '. Quit and reopen Vireo to finish. ' +
      'Changes you make before then go into the &ldquo;Before restore&rdquo; backup, not the restored catalog. ' +
      '<button class="btn-sm" data-catalog-backup-action="cancel-restore">Cancel restore</button></div>';
  }
  var last = state.last_restore;
  if (last && !pending) {
    var when = escapeHtml(catalogBackupDate(last.finished_at));
    var from = escapeHtml(catalogBackupDate(last.backup_created_at));
    if (last.ok) {
      html += '<div class="catalog-backup-notice">Restored the backup from ' + from + ' on ' + when +
        '. The catalog you had before is kept as the &ldquo;Before restore&rdquo; backup below; restore it to undo this.</div>';
    } else {
      html += '<div class="catalog-backup-notice catalog-backup-notice-error">The restore of the backup from ' + from +
        ' did not happen (' + when + '): ' + escapeHtml(last.error || 'unknown error') + ' Vireo kept your catalog.</div>';
    }
  }
  notice.innerHTML = html;
  notice.style.display = html ? '' : 'none';
}

/** @param {CatalogBackup} backup */
function catalogBackupRow(backup) {
  var photos = backup.photos == null ? '' : backup.photos.toLocaleString() + ' photos';
  var action;
  if (!backup.restorable) {
    action = '<span class="catalog-backup-dim">' + escapeHtml(backup.not_restorable_reason || 'Cannot be restored') + '</span>';
  } else {
    action = '<button class="btn-sm" data-catalog-backup-action="confirm" data-backup="' + escapeAttr(backup.name) + '">Restore&hellip;</button>';
  }
  var kindTitle = backup.kind === 'before-restore' ? ' title="The catalog as it was just before a restore replaced it"' : '';
  var html = '<tr><td>' + escapeHtml(catalogBackupDate(backup.created_at)) + '</td>' +
    '<td' + kindTitle + '>' + escapeHtml(catalogBackupKindLabel(backup.kind)) + '</td>' +
    '<td>' + escapeHtml(photos) + '</td><td>' + escapeHtml(formatBytes(backup.bytes)) + '</td>' +
    '<td style="text-align:right;">' + action + '</td></tr>';
  if (catalogBackupConfirming === backup.name) html += catalogBackupConfirmRow(backup);
  return html;
}

/** @param {CatalogBackup} backup */
function catalogBackupConfirmRow(backup) {
  var date = escapeHtml(catalogBackupDate(backup.created_at));
  return '<tr><td colspan="5"><div class="catalog-backup-notice catalog-backup-notice-warn">' +
    '<p style="margin:0 0 6px;">Restore the catalog to how it was on ' + date + '?</p>' +
    '<p style="margin:0 0 6px;">Vireo copies the backup now, then swaps it in the next time it starts. ' +
    'Catalog changes made after ' + date + ' (imports, labels, ratings, flags, edits, collections) are set aside ' +
    'in a &ldquo;Before restore&rdquo; backup, which you can restore to undo this. Photo files are not touched: ' +
    'photos imported since then stay on disk, and rescanning their folders brings them back.</p>' +
    '<button class="btn-sm" style="margin-left:0;" data-catalog-backup-action="restore" data-backup="' + escapeAttr(backup.name) + '">Prepare restore</button> ' +
    '<button class="btn-sm" data-catalog-backup-action="dismiss">Cancel</button>' +
    '</div></td></tr>';
}

function renderCatalogBackupList() {
  var state = /** @type {CatalogBackupStatus} */ (catalogBackupState);
  var list = /** @type {HTMLElement} */ (document.getElementById('catalogBackupsList'));
  if (!state.backups.length) {
    list.innerHTML = '';
    return;
  }
  list.innerHTML = '<table class="catalog-backup-table"><thead><tr><th>Made</th><th>Kind</th><th>Catalog</th><th>Size</th><th></th></tr></thead><tbody>' +
    state.backups.map(catalogBackupRow).join('') + '</tbody></table>';
  setCatalogBackupBusy(catalogBackupIsBusy());
}

function catalogBackupIsBusy() {
  return catalogBackupWatchedJob !== null || !!(catalogBackupState && catalogBackupState.active_job);
}

/** @param {boolean} busy */
function setCatalogBackupBusy(busy) {
  var section = /** @type {HTMLElement} */ (document.getElementById('catalogBackupsSection'));
  section.querySelectorAll('button').forEach(function(button) {
    if (button.dataset.catalogBackupAction === 'cancel-restore') return;
    button.disabled = busy;
  });
}

/** @param {string} text @param {string} [color] */
function setCatalogBackupAction(text, color) {
  var action = /** @type {HTMLElement} */ (document.getElementById('catalogBackupAction'));
  action.textContent = text;
  action.style.color = color || 'var(--text-dim)';
}

/** @param {string} jobId @param {string} jobType */
function watchCatalogBackupJob(jobId, jobType) {
  catalogBackupWatchedJob = jobId;
  setCatalogBackupBusy(true);
  var label = jobType === 'catalog-restore-prepare' ? 'Preparing the restore' : 'Backing up';
  setCatalogBackupAction(label + '…');
  safeEventSource('/api/jobs/' + jobId + '/stream', {
    onProgress: function(p) {
      var text = p.phase || label;
      if ((p.total || 0) > 0) text += ': ' + formatBytes(p.current || 0) + ' of ' + formatBytes(p.total || 0);
      setCatalogBackupAction(text + '…');
    },
    onComplete: function(result) {
      catalogBackupWatchedJob = null;
      if (result.status === 'completed') {
        var summary = result.result && typeof result.result.summary === 'string' ? result.result.summary : 'Done.';
        setCatalogBackupAction(summary, 'var(--accent)');
      } else if (result.status === 'cancelled') {
        setCatalogBackupAction('Cancelled.');
      } else {
        var message = (result.failure && result.failure.message) || (result.errors || []).join(', ') || 'unknown error';
        setCatalogBackupAction('Failed: ' + message, 'var(--danger)');
      }
      loadCatalogBackups();
    },
    onError: function() {
      catalogBackupWatchedJob = null;
      setCatalogBackupAction('Lost the connection to the job; see Jobs for its result.');
      loadCatalogBackups();
    },
  });
}

async function startCatalogBackup() {
  try {
    var data = /** @type {{job_id:string}} */ (await safeFetch('/api/catalog-backups', { method: 'POST' }));
    watchCatalogBackupJob(data.job_id, 'catalog-backup');
  } catch (e) {
    setCatalogBackupAction('Could not start the backup.', 'var(--danger)');
  }
}

/** @param {string} name */
async function prepareCatalogRestore(name) {
  catalogBackupConfirming = null;
  renderCatalogBackupList();
  try {
    var data = /** @type {{job_id:string}} */ (await safeFetch('/api/catalog-backups/' + encodeURIComponent(name) + '/restore', { method: 'POST' }));
    watchCatalogBackupJob(data.job_id, 'catalog-restore-prepare');
  } catch (e) {
    setCatalogBackupAction('Could not start preparing the restore.', 'var(--danger)');
  }
}

async function cancelCatalogRestore() {
  try {
    await safeFetch('/api/catalog-backups/pending-restore', { method: 'DELETE' });
    setCatalogBackupAction('Restore cancelled. Vireo keeps the current catalog.');
  } catch (e) {
    setCatalogBackupAction('Could not cancel the restore.', 'var(--danger)');
  }
  loadCatalogBackups();
}

function setupCatalogBackups() {
  var section = document.getElementById('catalogBackupsSection');
  if (!section) return;
  var backupNow = /** @type {HTMLElement} */ (document.getElementById('catalogBackupNowBtn'));
  backupNow.addEventListener('click', startCatalogBackup);
  section.addEventListener('click', function(event) {
    var target = /** @type {HTMLElement} */ (event.target);
    var button = /** @type {HTMLElement | null} */ (target.closest('[data-catalog-backup-action]'));
    if (!button) return;
    var action = button.dataset.catalogBackupAction;
    var name = button.dataset.backup || '';
    if (action === 'confirm' && catalogBackupByName(name)) {
      catalogBackupConfirming = name;
      renderCatalogBackupList();
    } else if (action === 'dismiss') {
      catalogBackupConfirming = null;
      renderCatalogBackupList();
    } else if (action === 'restore') {
      prepareCatalogRestore(name);
    } else if (action === 'cancel-restore') {
      cancelCatalogRestore();
    }
  });
}
