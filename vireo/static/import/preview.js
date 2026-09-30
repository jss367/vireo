// Preview scheduling, request cancellation, signatures, and duplicate-check lifecycle.
// Classic page script; load boot.js after all definitions.

function clearScheduledImportPreview() {
  if (importPreviewTimer) {
    clearTimeout(importPreviewTimer);
    importPreviewTimer = null;
  }
}

function scheduleImportPreview() {
  clearScheduledImportPreview();
  showError('');
  // Everything that schedules a re-preview has just changed something the
  // rendered grid was drawn from — a source, the mode, a duplicate option.
  // Gate Start here rather than waiting 350ms for the preview to start, so
  // the button can never be live over a grid that no longer describes the
  // import. (wireDestStructureInvalidation also calls this directly; the
  // call is a pure recompute, so running it twice costs nothing.)
  updateStartGate();
  if (!sources.length) {
    // No successor preview will run, so previewImport()'s own abort at
    // the top of the next flight won't happen. If we don't cancel the
    // in-flight request here, removing the last source leaves the server
    // hashing that removed card until it finishes — exactly the resource
    // contention the AbortController plumbing was added to prevent. The
    // sequence bump makes the old run's staleness guards fail so its
    // finally block can't re-enable Start or overwrite the summary.
    if (importPreviewAbort) {
      importPreviewAbort.abort();
      importPreviewAbort = null;
      importPreviewSeq += 1;
      importPreviewInFlight = false;
      importDupStreamPending = false;
      importPreviewFailed = false;
      // Retire the captured preview along with the flight. When discovery
      // had already populated importPreviewCapturedSignature and
      // importPreviewedPaths before the removal, leaving them behind lets a
      // re-add of the same source within the 350ms debounce restore a
      // matching signature over an empty grid: updateStartGate() finds no
      // reason to hold Start (sig is not stale, dup stream is not pending,
      // eligible count is zero so the "0 selected" gate never fires),
      // enables it as "Start import (0 files)", and startImport() posts the
      // retained importPreviewedPaths as include_paths — importing every
      // previously-discovered file behind an empty screen. Collapse the
      // state back to "no preview run" so the next preview owns the truth.
      importPreviewCapturedSignature = null;
      importPreviewedPaths = [];
      updateImportSelectionUI();
      updateStartGate();
    }
    return;
  }
  importPreviewTimer = setTimeout(() => {
    importPreviewTimer = null;
    previewImport({ automatic: true });
  }, 350);
}

function importPreviewSignature() {
  const copyMode = newImagesSnapshotId === null && selectedImportMode() === 'copy';
  return JSON.stringify({
    sources: sources.slice(),
    source_snapshot_id: newImagesSnapshotId,
    mode: copyMode ? 'copy' : 'in_place',
    file_types: copyMode ? selectedFileTypes() : 'both',
    recursive: document.getElementById('chkRecursive').checked,
    skip_duplicates: document.getElementById('chkSkipDuplicates').checked,
    verify_by_hash: document.getElementById('chkVerifyByHash').checked,
    trust_likely_duplicates: document.getElementById('chkTrustLikelyDuplicates').checked,
    // The dup stream's "already at destination" verdicts are computed
    // against the planned destination folders, so the preview no longer
    // describes the form once the destination or template changes —
    // without these fields a mid-debounce destination edit could let a
    // stale recovery count render (or Start submit against it).
    destination: copyMode ? resolvedCopyDestination() : '',
    folder_template: copyMode ? selectedFolderTemplate() : '',
  });
}

function importPreviewSignatureChanged(signature) {
  return signature !== importPreviewSignature();
}

// Clear the lifecycle flags, but ONLY if this run still owns them. A
// superseded run finishing (its fetch finally failing, its stream draining)
// must not hand the user an enabled Start while the newer run is still
// walking the disk behind a cleared grid.
function endImportPreviewFlight(requestSeq) {
  if (requestSeq !== importPreviewSeq) return;
  importPreviewInFlight = false;
  importDupStreamPending = false;
  updateStartGate();
}

async function previewImport(opts) {
  // Abort before advancing the sequence. abort() only schedules the rejected
  // fetch continuation; this synchronous function advances importPreviewSeq
  // before that continuation can run, so the old catch/finally paths already
  // recognize that they no longer own the UI.
  if (importPreviewAbort) {
    importPreviewAbort.abort();
    importPreviewAbort = null;
  }
  clearScheduledImportPreview();
  showError('');
  hideDestStructure();
  clearImportPreviewGrid();
  importPreviewSeq += 1;
  const requestSeq = importPreviewSeq;
  // In flight from HERE, before the disk walk. The selection itself is NOT
  // reset here and must never be: clearImportPreviewGrid() has already wiped
  // the cards, so between this line and the render the signature still
  // matches the UI and the staleness check cannot catch a stale screen. A
  // user who picked 100 of 5,000 files, toggled a file-type box and clicked
  // Start during the walk would copy all 5,000. The completed preview
  // REPLACES the selection on success instead.
  importPreviewInFlight = true;
  importDupStreamPending = false;
  importPreviewFailed = false;
  updateStartGate();
  const summary = document.getElementById('previewSummary');
  // Both bail-outs below are settled: the run stops before discovery, so
  // nothing can be hidden and the opt-in must not linger behind the
  // now-hidden row. (Contrast the in-flight path further down, which
  // keeps the opt-in until the duplicate count is actually known.)
  if (!sources.length && newImagesSnapshotId === null) {
    retireHideDuplicatesOptIn();
    summary.textContent = '';
    showError('Add at least one source folder.');
    endImportPreviewFlight(requestSeq);
    return;
  }
  const copyMode = newImagesSnapshotId === null && selectedImportMode() === 'copy';
  const fileTypes = copyMode ? selectedFileTypes() : 'both';
  if (copyMode && Array.isArray(fileTypes) && !fileTypes.length) {
    // This run already aborted its predecessor at the top of the function.
    // No replacement traversal will start, so do not leave the predecessor's
    // rows and elapsed clock claiming that a scan is still active.
    resetActiveSourceScans();
    retireHideDuplicatesOptIn();
    summary.textContent = '';
    showError('Choose at least one file extension.');
    endImportPreviewFlight(requestSeq);
    return;
  }
  const requestSignature = importPreviewSignature();
  const btnPreview = document.getElementById('btnPreview');
  if (btnPreview) btnPreview.disabled = true;
  summary.textContent = 'Discovering files…';
  const previewAbort = new AbortController();
  importPreviewAbort = previewAbort;
  try {
    const snapshotMode = newImagesSnapshotId !== null;
    // Duplicate/recovery preparation already reads capture metadata. Only
    // ask discovery to read it when no follow-up check will run.
    const captureDatesDuringDiscovery = copyMode &&
      !document.getElementById('chkSkipDuplicates').checked && !resolvedCopyDestination();
    const resp = await fetch(
      snapshotMode
        ? '/api/import/new-images-preview'
        : '/api/import/folder-preview-stream', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      signal: previewAbort.signal,
      body: JSON.stringify(snapshotMode ? {
        snapshot_id: newImagesSnapshotId,
      } : {
          folders: sources,
          file_types: fileTypes,
          recursive: document.getElementById('chkRecursive').checked,
          include_capture_dates: captureDatesDuringDiscovery,
        }),
    });
    if (!resp.ok) throw new Error((await resp.json()).error || 'preview failed');
    // The stream narrates the walk (per-folder scan progress feeds the
    // source rows) and resolves to the same payload the old synchronous
    // endpoint returned. Snapshot previews read captured rows, not disk,
    // so they stay a plain JSON response.
    const data = snapshotMode
      ? await resp.json()
      : await readFolderPreviewStream(resp, requestSeq);
    // A newer previewImport() started while this response was in flight —
    // it owns the summary and any structure preview now, so bail without
    // touching either. Clearing summary here would erase the newer run's
    // "Discovering files…" or completed results.
    if (requestSeq !== importPreviewSeq) return;
    if (importPreviewSignatureChanged(requestSignature)) {
      return;
    }
    if (!data) throw new Error('preview stream ended unexpectedly');
    if (!snapshotMode) applyDoneFrameSourceCounts(data.source_counts);
    const files = data.files || [];
    const filesByPath = new Map();
    files.forEach(file => {
      if (!filesByPath.has(file.path)) filesByPath.set(file.path, []);
      filesByPath.get(file.path).push(file);
    });
    function applyCaptureDates(dates) {
      Object.entries(dates || {}).forEach(([path, date]) => {
        (filesByPath.get(path) || []).forEach(file => { file.capture_date = date; });
      });
    }
    // SUCCESS. This is the only place the selection is replaced: a completed
    // preview owns it, so what the user last saw and what Start would send
    // are the same list. Doing this at the top of the function instead is the
    // 5,000-file hazard described there.
    importPreviewCapturedSignature = requestSignature;
    importDeselected = new Set();
    importCollapsedDays = new Set();
    importPreviewedPaths = files.map(f => f.path);
    // The walk is over — the duplicate stream, if any, is gated separately
    // so the label can say "Checking duplicates…" rather than "Previewing…".
    importPreviewInFlight = false;
    if (!files.length) {
      summary.textContent = 'No importable files found.';
      clearImportPreviewGrid();
      // A completed preview with zero files is a final "nothing to hide"
      // state — like snapshot / in-place / dedup-off below — so retire the
      // opt-in. Otherwise the row hides while the checkbox stays checked,
      // and the next card that does surface duplicates gets filtered
      // silently instead of opt-in.
      retireHideDuplicatesOptIn();
      updateImportSelectionUI();
      return;
    }
    // The three branches below all settle without a duplicate check —
    // in-place/snapshot imports have no duplicate concept and a dedup-off
    // copy skips nothing — so each is a final "nothing to hide" state and
    // retires the opt-in. The fall-through path does NOT: its duplicate
    // count is still unknown until the check below completes.
    //
    // Each branch renders and then resyncs the selection readouts. The
    // renderer draws the boxes; updateImportSelectionUI() owns the
    // select-all row, the "N of M selected" line and the Start gate, and it
    // reads the cards the renderer just drew, so it has to follow every
    // render rather than sit once above the branches.
    if (snapshotMode) {
      renderImportPreviewGrid(files, [], null);
      updateImportSelectionUI();
      retireHideDuplicatesOptIn();
      const unavailable = Number(data.unavailable_count || 0);
      summary.textContent = Number(data.total_count || files.length).toLocaleString() +
        ' captured file' + (Number(data.total_count || files.length) === 1 ? '' : 's') +
        ' · originals will stay in place' +
        (unavailable ? ' · ' + unavailable.toLocaleString() + ' unavailable' : '');
      return;
    }
    if (!copyMode) {
      renderImportPreviewGrid(files, [], null);
      updateImportSelectionUI();
      retireHideDuplicatesOptIn();
      summary.textContent = files.length + ' files found · originals will stay in place';
      return;
    }
    if (!document.getElementById('chkSkipDuplicates').checked) {
      retireHideDuplicatesOptIn();
      // With duplicate-skipping OFF the run doesn't consult the library
      // checker, but crash-recovery adoption of byte-identical files at
      // the destination still fires unconditionally in import_job — so
      // the honest transfer count still depends on how many of the
      // discovered files are already sitting at their planned destination
      // from an interrupted prior run. Fetch recovery-only (paths a
      // resumed run would adopt without transferring) whenever the
      // destination is resolvable; the summary subtracts them from
      // "to copy" the same way the dedup-on branch does. Nothing else in
      // the UI changes: recovered files stay selected and land in the
      // preview grid without a badge, because the run still processes
      // them (verify + catalog) — they're just not re-copied.
      //
      // No destination resolvable (in-place preview, or the remote
      // dropdown hasn't populated yet) → skip the request and show the
      // count with the old wording. A subsequent preview run after the
      // form settles will re-check.
      const recoveryDestNoDedup = resolvedCopyDestination();
      if (!recoveryDestNoDedup) {
        renderImportPreviewGrid(files, [], null);
        updateImportSelectionUI();
        summary.textContent = files.length +
          ' files found · duplicates will be copied';
        const destData = await renderDestStructure([], previewAbort.signal);
        if (requestSeq !== importPreviewSeq) return;
        if (importPreviewSignatureChanged(requestSignature)) return;
        if (destData && destData.files) {
          renderImportPreviewGrid(files, [], destData.files);
        }
        return;
      }
      summary.textContent = files.length +
        ' files found — checking for interrupted-run leftovers…';
      importDupStreamPending = true;
      updateImportSelectionUI();
      // Reuse the check-duplicates SSE endpoint with skip_duplicates:
      // false — the server matches the import job's own gate and streams
      // only ``recovered`` (no ``duplicates``) in this mode. Same
      // race-condition guards as the dedup-on branch below: a newer
      // preview cancels this one via requestSeq / signature checks.
      const recResp = await fetch('/api/import/check-duplicates', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        signal: previewAbort.signal,
        body: JSON.stringify({
          paths: files.map(f => f.path),
          verify_by_hash: document.getElementById('chkVerifyByHash').checked,
          skip_duplicates: false,
          include_capture_dates: true,
          destination: recoveryDestNoDedup,
          folder_template: selectedFolderTemplate(),
        }),
      });
      if (!recResp.ok) throw new Error('recovery check failed');
      const recReader = recResp.body.getReader();
      const recDecoder = new TextDecoder();
      let recBuffer = '';
      const recoveredPathsNoDedup = [];
      for (;;) {
        const r = await recReader.read();
        if (r.done) break;
        recBuffer += recDecoder.decode(r.value, { stream: true });
        const parts = recBuffer.split('\n\n');
        recBuffer = parts.pop();
        for (const part of parts) {
          const m = part.match(/^data: (.+)$/m);
          if (!m) continue;
          try {
            const d = JSON.parse(m[1]);
            applyCaptureDates(d.capture_dates);
            if (d.recovered) recoveredPathsNoDedup.push(...d.recovered);
            if (!d.done && requestSeq === importPreviewSeq &&
                !importPreviewSignatureChanged(requestSignature) &&
                Number.isFinite(Number(d.checked)) &&
                Number.isFinite(Number(d.total))) {
              summary.textContent = files.length +
                ' files found — checking for interrupted-run leftovers… ' +
                Number(d.checked).toLocaleString() + ' of ' +
                Number(d.total).toLocaleString();
            }
          } catch (e) { /* partial frame */ }
        }
      }
      if (requestSeq !== importPreviewSeq) return;
      if (importPreviewSignatureChanged(requestSignature)) return;
      const recCountNoDedup = recoveredPathsNoDedup.length;
      importDupStreamPending = false;
      renderImportPreviewGrid(files, [], null);
      updateImportSelectionUI();
      summary.textContent = files.length + ' files found · ' +
        (recCountNoDedup ? recCountNoDedup +
          ' already at destination from an interrupted import' +
          ' (verified & adopted on import, not re-copied) · ' : '') +
        (files.length - recCountNoDedup) +
        ' to copy · duplicates will be copied';
      // No dedup gate, so every discovered file lands — pass no exclusions
      // so the folder-structure counts match the full copy set.
      const destData = await renderDestStructure([], previewAbort.signal);
      if (requestSeq !== importPreviewSeq) return;
      if (importPreviewSignatureChanged(requestSignature)) return;
      if (destData && destData.files) renderImportPreviewGrid(files, [], destData.files);
      return;
    }
    summary.textContent = files.length + ' files found — checking for duplicates…';
    // Eligibility is not final until the verdicts land, so Start stays shut
    // until the stream drains: submitting mid-stream would send a selection
    // that doesn't match the boxes the user is looking at.
    importDupStreamPending = true;
    // updateImportSelectionUI(), not a bare updateStartGate(): since #1387
    // this path renders nothing until the stream drains, so between here and
    // then the grid is empty while "N of M selected" would still be showing
    // the PREVIOUS card's tally. Over SMB with verify_by_hash that window is
    // minutes long. Resync it against the grid that is actually on screen.
    updateImportSelectionUI();
    // check-duplicates streams results; collect flagged paths as they arrive.
    // Pass verify_by_hash so the preview and the import agree on which files
    // are duplicates — otherwise a renamed/metadata-colliding file counted
    // as "to copy" in the preview would be skipped by the hash-based import.
    // Pass the destination + template (same values startImport sends) so the
    // server can also flag files a cancelled/crashed prior run already left
    // at their planned destination folder — the run adopts those via crash
    // recovery instead of re-copying, and counting them "to copy" here
    // overstated the transfer after every mid-run Stop. An unresolved
    // destination (incomplete remote form) just skips that check.
    const recoveryDestination = resolvedCopyDestination();
    const dupResp = await fetch('/api/import/check-duplicates', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      signal: previewAbort.signal,
      body: JSON.stringify({
        paths: files.map(f => f.path),
        verify_by_hash: document.getElementById('chkVerifyByHash').checked,
        include_capture_dates: true,
        ...(recoveryDestination ? {
          destination: recoveryDestination,
          folder_template: selectedFolderTemplate(),
        } : {}),
      }),
    });
    if (!dupResp.ok) throw new Error('duplicate check failed');
    const reader = dupResp.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    const duplicatePaths = [];
    const recoveredPaths = [];
    for (;;) {
      const r = await reader.read();
      if (r.done) break;
      buffer += decoder.decode(r.value, { stream: true });
      const parts = buffer.split('\n\n');
      buffer = parts.pop();
      for (const part of parts) {
        const m = part.match(/^data: (.+)$/m);
        if (!m) continue;
        try {
          const d = JSON.parse(m[1]);
          applyCaptureDates(d.capture_dates);
          if (d.duplicates) duplicatePaths.push(...d.duplicates);
          if (d.recovered) recoveredPaths.push(...d.recovered);
          const catalogRecovery = d.catalog_recovery;
          if (catalogRecovery && requestSeq === importPreviewSeq &&
              !importPreviewSignatureChanged(requestSignature) &&
              Number.isFinite(Number(catalogRecovery.checked)) &&
              Number.isFinite(Number(catalogRecovery.total))) {
            // The server is fingerprinting JPEGs already in the catalog
            // (paired with a RAW) before it can compare this card against
            // them — whole-file reads, slow on a NAS. Say so, with the
            // count, rather than an idle "checking for duplicates…".
            summary.textContent = files.length +
              ' files found — fingerprinting paired JPEGs already in your catalog' +
              ' so duplicates are recognized… ' +
              Number(catalogRecovery.checked).toLocaleString() + ' of ' +
              Number(catalogRecovery.total).toLocaleString();
          } else if (!d.done && requestSeq === importPreviewSeq &&
              !importPreviewSignatureChanged(requestSignature) &&
              Number.isFinite(Number(d.checked)) &&
              Number.isFinite(Number(d.total))) {
            summary.textContent = files.length +
              ' files found — checking for duplicates… ' +
              Number(d.checked).toLocaleString() + ' of ' +
              Number(d.total).toLocaleString();
          } else if (!d.done && requestSeq === importPreviewSeq &&
              !importPreviewSignatureChanged(requestSignature) &&
              Number.isFinite(Number(d.preparing)) &&
              Number.isFinite(Number(d.total))) {
            // Heartbeat from the prep phase (batched EXIF/capture-time
            // reads before per-file checks begin). Long cards used to
            // sit on "Discovering files…" through all of prep — this
            // shows the user work is happening AND surfaces a place the
            // server pauses long enough to observe a client disconnect.
            summary.textContent = files.length +
              ' files found — preparing metadata… ' +
              Number(d.preparing).toLocaleString() + ' of ' +
              Number(d.total).toLocaleString();
          }
        } catch (e) { /* partial frame */ }
      }
    }
    // Same guard as above for the duplicate-stream leg: a newer
    // previewImport() may have started while the SSE was draining, and
    // owns the summary now.
    if (requestSeq !== importPreviewSeq) return;
    if (importPreviewSignatureChanged(requestSignature)) {
      return;
    }
    const dupCount = duplicatePaths.length;
    // The check ran to completion, so this count is final.
    if (dupCount === 0) retireHideDuplicatesOptIn();
    // Recovered files stay selected — the import must still process them
    // to verify and catalog the copies already at the destination — but
    // they are not part of the transfer, so "to copy" must not count
    // them. The preview already byte-verified each size-matching
    // candidate against its source, so the "not re-copied" promise
    // matches what the run will do; a same-size-different-bytes
    // collision would have been advanced past on the server and
    // reappeared in the "to copy" count.
    const recCount = recoveredPaths.length;
    summary.textContent = files.length + ' files found · ' + dupCount +
      ' already in your library (will be skipped) · ' +
      (recCount ? recCount +
        ' already at destination from an interrupted import' +
        ' (verified & adopted on import, not re-copied) · ' : '') +
      (files.length - dupCount - recCount) + ' to copy';
    importDupStreamPending = false;
    renderImportPreviewGrid(files, duplicatePaths, null);
    updateImportSelectionUI();
    // Skipped duplicates aren't copied, so exclude them from the
    // folder-structure preview — the new/existing folder counts must
    // reflect the files that will actually land in the archive.
    const destData = await renderDestStructure(
      duplicatePaths, previewAbort.signal);
    if (requestSeq !== importPreviewSeq) return;
    if (importPreviewSignatureChanged(requestSignature)) return;
    if (destData && destData.files) renderImportPreviewGrid(files, duplicatePaths, destData.files);
  } catch (e) {
    // A superseding preview deliberately aborts every request owned by this
    // run. Its successor owns the summary / error state, so cancellation is
    // a normal lifecycle event rather than a user-facing failure.
    if (e && e.name === 'AbortError') return;
    // Don't let a stale run's error clobber a newer preview's summary
    // or surface a stale error banner — the newer run owns the UI.
    if (requestSeq !== importPreviewSeq) return;
    if (importPreviewSignatureChanged(requestSignature)) return;
    // The walk died with rows possibly mid-"Scanning…" — settle them so
    // the list doesn't advertise a scan nothing is running.
    failActiveSourceScans();
    summary.textContent = '';
    showError(String(e.message || e));
    // The grid was cleared at the top and nothing replaced it, so the screen
    // shows no preview at all while the signature still matches — the
    // staleness check would call that "current". Say so explicitly.
    importPreviewFailed = true;
  } finally {
    if (importPreviewAbort === previewAbort) importPreviewAbort = null;
    if (requestSeq === importPreviewSeq && btnPreview) {
      btnPreview.disabled = false;
    }
    endImportPreviewFlight(requestSeq);
  }
}
