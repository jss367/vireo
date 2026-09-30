// Import result presentation and the card-cleanup handoff.
// Classic page script; load boot.js after all definitions.

function renderResult(res, status) {
  document.getElementById('resultCard').style.display = '';
  const retryRow = document.getElementById('retryImportRow');
  const retryButton = document.getElementById('btnRetryImport');
  const retryHint = document.getElementById('retryImportHint');
  retryRow.style.display = 'none';
  retryHint.textContent = '';
  retryButton.disabled = false;
  // Card-cleanup entry point. Reset it here so an in-place run after a
  // copy run doesn't inherit the previous run's button and source path.
  const freeUpCardBtn = document.getElementById('btnFreeUpCardSpace');
  freeUpCardBtn.style.display = 'none';
  cardCleanupImportSources = [];
  const pill = document.getElementById('safeToFormatPill');
  const unsafe = document.getElementById('unsafeList');
  unsafe.innerHTML = '';
  if (res.mode === 'in_place') {
    pill.className = res.ok === false ? 'pill pill-bad' : 'pill pill-ok';
    pill.textContent = res.ok === false
      ? 'Import finished with errors'
      : 'Imported in place — original files stayed where they are';
    const errors = res.errors || [];
    unsafe.style.display = errors.length ? '' : 'none';
    errors.forEach((msg) => {
      const li = document.createElement('li');
      li.textContent = msg;
      unsafe.appendChild(li);
    });
    if (res.source_snapshot_id != null) {
      [
        ['Missing', res.missing, res.missing_paths],
        ['Unreadable', res.unreadable, res.unreadable_paths],
        ['Not indexed', res.unindexed, res.unindexed_paths],
      ].forEach(([label, count, paths]) => {
        (paths || []).forEach(path => {
          const li = document.createElement('li');
          li.textContent = label + ': ' + path;
          unsafe.appendChild(li);
        });
        if (Number(count || 0) > (paths || []).length) {
          const li = document.createElement('li');
          li.textContent = label + ': ' +
            (Number(count) - paths.length).toLocaleString() +
            ' additional path' +
            (Number(count) - paths.length === 1 ? '' : 's') +
            ' omitted from this summary';
          unsafe.appendChild(li);
        }
      });
      unsafe.style.display = unsafe.children.length ? '' : 'none';
    }
    if (res.source_snapshot_id != null) {
      const parts = [
        (res.requested || 0) + ' requested',
        (res.imported || 0) + ' imported',
      ];
      if (res.already_cataloged) {
        parts.push(res.already_cataloged + ' already cataloged');
      }
      if (res.missing) parts.push(res.missing + ' missing');
      if (res.unreadable) parts.push(res.unreadable + ' unreadable');
      if (res.unindexed) parts.push(res.unindexed + ' not indexed');
      if (status && status !== 'completed') parts.push('job ' + status);
      document.getElementById('resultSummary').textContent = parts.join(' · ');
    } else {
      document.getElementById('resultSummary').textContent =
        (res.indexed || res.discovered || 0) + ' indexed' +
        (res.failed ? ' · ' + res.failed + ' failed' : '') +
        (status && status !== 'completed' ? ' · job ' + status : '');
    }
    document.getElementById('resultFolders').innerHTML = '';
    renderChainInfo(res);
    renderTaggingInfo(res);
    renderModelWarning(res);
    return;
  }
  if (res.safe_to_format) {
    pill.className = 'pill pill-ok';
    pill.textContent = res.nas_transfer_deferred
      ? 'Photos kept locally for review — choose Send to NAS when you are ready'
      : res.local_processing
      ? 'Card files verified in local temporary storage — see the processing and NAS transfer jobs for archive completion'
      : 'Safe to format the card — every file is verified in the archive';
    unsafe.style.display = 'none';
  } else if (
    res.unverified_duplicates_only
    && (!status || status === 'completed')
  ) {
    pill.className = 'pill pill-warn';
    pill.textContent = 'Import complete — keep the card until likely duplicates are verified';
    const files = res.unsafe_files || [];
    unsafe.style.display = '';
    if (!files.length) {
      const li = document.createElement('li');
      li.textContent = res.unverified_duplicate +
        ' likely duplicates matched by filename, byte size, and capture time, but were not compared byte-for-byte.';
      unsafe.appendChild(li);
    }
    files.forEach((u) => {
      const item = document.createElement('li');
      item.textContent = u.path + ' — ' + u.reason;
      unsafe.appendChild(item);
    });
  } else {
    pill.className = 'pill pill-bad';
    pill.textContent = 'Do NOT format the card yet';
    const files = res.unsafe_files || [];
    unsafe.style.display = files.length ? '' : 'none';
    files.forEach((u) => {
      const li = document.createElement('li');
      li.textContent = u.path + ' — ' + u.reason;
      unsafe.appendChild(li);
    });
    if (!files.length && res.cancelled) {
      const li = document.createElement('li');
      li.textContent = 'Import was cancelled before every file was verified.';
      unsafe.style.display = '';
      unsafe.appendChild(li);
    }
  }
  // Copy imports leave a second copy on the card, so offer the cleanup
  // page here — including (especially) when the card is NOT safe to
  // format, where deleting only the verified subset is the point. The
  // finished job's config carries the card paths the user picked.
  const importedSources = (((lastFinishedImportJob || {}).config || {}).sources
    || []).filter(s => typeof s === 'string' && s);
  if (importedSources.length) {
    cardCleanupImportSources = importedSources;
    freeUpCardBtn.style.display = '';
  }
  document.getElementById('resultSummary').textContent =
    res.discovered + ' discovered · ' + res.copied + ' copied · ' +
    res.skipped_duplicate + ' duplicates skipped · ' + res.failed + ' failed' +
    (status && status !== 'completed' ? ' · job ' + status : '');
  renderFolderTable(document.getElementById('resultFolders'), res.folders || {});

  const failedCount = Number(res.failed || 0);
  if (failedCount > 0 && retryBodyFromFinishedJob(lastFinishedImportJob)) {
    retryRow.style.display = '';
    retryButton.textContent = 'Retry ' + failedCount + ' failed file' +
      (failedCount === 1 ? '' : 's');
    retryHint.textContent =
      'Uses the same source and destination; successful files are skipped.';
  }

  renderChainInfo(res);
  renderTaggingInfo(res);
  renderModelWarning(res);
}

function renderTaggingInfo(res) {
  const el = document.getElementById('taggingInfo');
  const tagging = res.tagging;
  if (!tagging) {
    el.textContent = '';
    return;
  }
  const parts = [];
  if ((tagging.requested_tags || []).length) {
    parts.push(
      'Tags added to ' + (tagging.tagged_photos || 0) + ' photo' +
      ((tagging.tagged_photos || 0) === 1 ? '' : 's')
    );
  }
  if (tagging.location_requested) {
    parts.push((tagging.locations_added || 0) + ' GPS location' +
      ((tagging.locations_added || 0) === 1 ? '' : 's') + ' added');
    if (tagging.locations_unresolved) {
      parts.push(tagging.locations_unresolved + ' could not be resolved');
    }
    if (tagging.locations_skipped) {
      parts.push(tagging.locations_skipped + ' already had a location');
    }
  }
  if ((tagging.errors || []).length) {
    parts.push(tagging.errors.join(' '));
  }
  if (tagging.skipped) parts.push('Tagging skipped: ' + tagging.skipped);
  el.textContent = parts.join(' · ');
}

function renderChainInfo(res) {
  const chain = document.getElementById('chainInfo');
  const parts = [];
  if (res.process_job_id) {
    parts.push('Processing started as its own job: ' + res.process_job_id);
  } else if (res.after_import_skipped) {
    parts.push('Processing not started: ' + res.after_import_skipped);
  }
  // Set only when the chained process job actually enqueued with a move
  // plan, so this states what WILL happen, not what was merely requested.
  // `note` carries the honest reason when folders were skipped (photos
  // cataloged directly on the archive root have no folder to move).
  if (res.after_process_move_planned) {
    const plan = res.after_process_move_planned;
    const n = (plan.folders || []).length;
    if (n === 0) {
      parts.push(
        'Move to NAS (' + plan.target_name + ') will not run: '
        + (plan.note || 'no folders to move') + '.');
    } else {
      parts.push(
        'Processing will be followed by a move to NAS ('
        + plan.target_name + '): '
        + n + ' folder' + (n === 1 ? '' : 's') + '.');
      if (plan.note) parts.push('Note: ' + plan.note + '.');
    }
  }
  // Pre-block path: processing couldn't start, so the import hook fired
  // the promised NAS move itself. `_chain_after_move` writes its outcome
  // (move_job_ids / after_move_skipped / after_move_note /
  // after_move_errors) onto the import result — surface it here so the
  // user isn't left thinking "processing paused" also means the requested
  // NAS move silently didn't happen.
  const moveIds = res.move_job_ids || [];
  if (moveIds.length) {
    parts.push(
      'Move to NAS: ' + moveIds.length + ' move job'
      + (moveIds.length === 1 ? '' : 's') + ' started.');
    if (res.after_move_note) parts.push('Note: ' + res.after_move_note + '.');
  } else if (res.after_move_skipped) {
    parts.push('Move to NAS skipped: ' + res.after_move_skipped + '.');
  }
  const moveErrors = res.after_move_errors || [];
  if (moveErrors.length) {
    parts.push(
      'Move to NAS: ' + moveErrors.length + ' folder'
      + (moveErrors.length === 1 ? '' : 's')
      + ' failed to start — ' + moveErrors.join('; ') + '.');
  }
  chain.textContent = parts.join(' ');
}

function renderModelWarning(res) {
  const warn = document.getElementById('modelWarning');
  if (res.model_warning) {
    warn.textContent = res.model_warning;
    warn.style.display = '';
  } else {
    warn.style.display = 'none';
  }
}

// Entry point from the card-safety pill. The cleanup flow lives on its own
// page (/card-cleanup); hand it this import's card folder so the user does
// not have to find it again. Multi-source imports send the first folder as
// `source` and the rest as `others`, which that page names in its hint.
function openCardCleanupPage() {
  const params = new URLSearchParams();
  if (cardCleanupImportSources.length) {
    params.set('source', cardCleanupImportSources[0]);
    cardCleanupImportSources.slice(1).forEach((s) => params.append('others', s));
  }
  const qs = params.toString();
  window.location.href = '/card-cleanup' + (qs ? '?' + qs : '');
}
