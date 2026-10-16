// Starting a Quick Move: the preview preflight, the confirm dialogs, and the move-folder job.
// Classic page script; load boot.js after all definitions.

async function startQuickMove() {
  if (quickMoveBusy) return;
  var fid = parseInt(document.getElementById('quickFolderSelect').value);
  if (!fid) return;
  var remoteId = quickRemoteTargetId();
  var nameResult = quickFolderNameResult();
  if (nameResult.error) {
    showToast(nameResult.error, 'error');
    return;
  }
  var templateResult = quickFolderTemplateResult();
  if (templateResult.error) {
    showToast(templateResult.error, 'error');
    return;
  }

  // Build the destination half of the request body — a local path or a
  // remote target + optional subpath. The same shape is reused for both the
  // preflight and the move so they can't disagree.
  var destBody;
  if (remoteId) {
    if (!quickRsyncAvailable) {
      showToast('No GNU rsync found for remote moves. ' + rsyncInstallHint(), 'error');
      return;
    }
    // Normalize once, here, so the path the user is about to confirm matches
    // the path the backend will actually write. A raw .trim() would let an
    // absolute or `..` subpath through; the move would then fail mid-flight
    // after the user has clicked "Move", which is a worse UX than refusing
    // here with the same error string the preview surfaces.
    var subResult = normalizeRemoteSubpath(
      document.getElementById('quickSubpathInput').value);
    if (subResult.error) {
      showToast(subResult.error, 'error');
      return;
    }
    destBody = {
      remote_target_id: remoteId,
      subpath: subResult.value,
      destination_name: nameResult.value,
    };
  } else {
    var dest = document.getElementById('quickDestInput').value.trim();
    if (!dest) return;
    destBody = quickUsesDateFolders()
      ? {destination: dest, folder_template: templateResult.value}
      : {destination: dest, destination_name: nameResult.value};
  }

  var folder = allFolders.find(function(f) { return f.id === fid; });
  var sourcePath = folder ? folder.path : 'folder #' + fid;
  var photoCount = folder ? (folder.photo_count || 0) : '?';

  // Ask the backend for the resolved destination and whether it already
  // exists, so we can offer merge/resume instead of failing on collision.
  var pf;
  setQuickMoveBusy(true);
  try {
    // mode:'preview' also reports how many source files would actually copy
    // vs. be skipped as already present — the number the user really needs
    // before committing to a merge. This is a deliberate click, so the
    // server-side source-tree walk it triggers is acceptable here (unlike on
    // the keystroke-driven status line above).
    pf = await safeFetch('/api/move-folder/preflight', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(Object.assign({folder_id: fid, mode: 'preview'}, destBody)),
    });
  } catch (e) {
    return;  // safeFetch already shows error toast
  } finally {
    setQuickMoveBusy(false);
  }

  if (pf.remote && pf.reachable === false) {
    showToast('Couldn’t reach the remote target over SSH'
      + (pf.error ? (': ' + pf.error) : '') + '.', 'error');
    return;
  }

  var finalPath = pf.resolved_dest;
  if (pf.date_organized) {
    setQuickDateSummary(folder, pf);
    if (!pf.photo_count) {
      showToast('No tracked photos were found in this folder.', 'error');
      return;
    }
    if (!moveVisibilityReady) return;
    var affected;
    try {
      var visibility = await safeFetch('/api/move-photos/visibility', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({folder_id: fid}),
      });
      affected = (visibility.workspaces || []).map(function(ws) { return ws.name + ' (' + ws.photo_count + ' photos)'; });
    } catch (e) { return; }
    var keepVisible = document.getElementById('moveKeepVisible').checked;
    destBody.keep_visible = keepVisible;
    document.getElementById('moveAffectedWorkspaces').textContent = 'Affected workspaces: ' + (affected.join(', ') || 'none');
    var dateSummary = 'Move ' + pf.photo_count.toLocaleString() + ' photo' +
      (pf.photo_count !== 1 ? 's' : '') + ' into ' +
      pf.destination_count.toLocaleString() + ' capture-date folder' +
      (pf.destination_count !== 1 ? 's' : '') + ' inside ' + finalPath +
      '? Files are copied and verified before originals are removed. ' +
      'Existing same-name files are left untouched. Other workspaces: ' + (affected.join(', ') || 'none') +
      (keepVisible ? '. Keep moved photos visible there.' : '. Allow removal of their visibility there.');
    showConfirm('Organize by Capture Date', dateSummary, function() {
      executeQuickMove(Object.assign({folder_id: fid, merge: false}, destBody));
    });
    return;
  }
  var moveSummary = folder
    ? setQuickMoveSummary(folder, finalPath)
    : 'Confirm moving photos from ' + sourcePath + ' to ' + finalPath + '?';
  var verifyText = remoteId
    ? '\n\nTransferred over SSH and verified by checksum before originals are removed.'
    : '\n\nFiles will be copied and verified before originals are removed.';
  if (pf.exists) {
    var p = pf.preview || {};
    var copyN = p.will_copy;
    var skipN = p.will_skip;
    var blockN = p.will_block || 0;
    // Truthful transfer counts include every file under the source (sidecars
    // included), not just tracked photos. Keep each fact in its own visual
    // region instead of flattening this safety-critical review into prose.
    showMergeConfirm({
      sourcePath: sourcePath,
      destinationPath: finalPath,
      photoCount: photoCount,
      copyCount: typeof copyN === 'number' ? copyN : null,
      skipCount: typeof skipN === 'number' ? skipN : null,
      blockCount: blockN,
      destinationFileCount: pf.file_count || 0,
      destinationFileCountTruncated: !!pf.file_count_truncated,
      remote: !!remoteId,
    },
      function() {
        executeQuickMove(Object.assign({folder_id: fid, merge: true}, destBody));
      }
    );
  } else {
    showConfirm(
      'Move Folder',
      moveSummary + verifyText,
      function() {
        executeQuickMove(Object.assign({folder_id: fid, merge: false}, destBody));
      }
    );
  }
}

async function executeQuickMove(body) {
  try {
    var resp = await safeFetch('/api/jobs/move-folder', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body),
    });
    var action = body.folder_template ? 'Date organization' : (body.merge ? 'Merge' : 'Move');
    showToast(action + ' started (job ' + resp.job_id + ') — track progress in the bottom panel', 'info');
  } catch (e) {
    // safeFetch already shows error toast
  }
}
