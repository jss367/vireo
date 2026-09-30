// Preview cards, duplicate visibility, capture-day summaries, and thumbnail scheduling.
// Classic page script; load boot.js after all definitions.

function clearImportPreviewGrid() {
  if (importThumbSchedulerCancel) {
    importThumbSchedulerCancel();
    importThumbSchedulerCancel = null;
  }
  lastImportPreviewRender = null;
  importDayGroups = new Map();
  document.getElementById('importDaySummary').style.display = 'none';
  document.getElementById('importDayRows').replaceChildren();
  updateHideDuplicatesControl(0);
  const grid = document.getElementById('importPreviewGrid');
  if (!grid) return;
  grid.style.display = 'none';
  grid.innerHTML = '';
}

// The "Hide duplicates" row only makes sense once a preview has actually
// flagged duplicates — before the duplicate check runs (or with dedup off)
// there is nothing to hide, so the control stays hidden rather than
// implying a filter that would do nothing.
function updateHideDuplicatesControl(dupCount) {
  const row = document.getElementById('hideDuplicatesRow');
  const label = document.getElementById('hideDuplicatesLabel');
  if (!row || !label) return;
  row.style.display = dupCount > 0 ? '' : 'none';
  label.textContent = 'Hide duplicates (' + dupCount.toLocaleString() + ')';
}

function hideDuplicatesEnabled() {
  const el = document.getElementById('chkHideDuplicates');
  return !!(el && el.checked);
}

// Drop the opt-in once a settled preview has nothing to hide, so the next
// card that does carry duplicates starts unfiltered instead of inheriting
// a choice made for a different card while the row was hidden.
//
// Only call this where the duplicate picture is FINAL. Every preview
// renders the grid with an empty duplicate list before its check
// finishes, so clearing on any zero count (including inside
// updateHideDuplicatesControl) would silently flip the filter off every
// time an import option change schedules a fresh preview.
function retireHideDuplicatesOptIn() {
  const el = document.getElementById('chkHideDuplicates');
  if (el) el.checked = false;
}

// Re-run the last grid render with the current filter state. Purely a view
// toggle — it never re-requests the preview or the duplicate check.
function rerenderImportPreviewGrid() {
  if (!lastImportPreviewRender) return;
  const last = lastImportPreviewRender;
  renderImportPreviewGrid(last.files, last.duplicatePaths, last.destinationFiles);
}

function importDestinationMap(destinationFiles) {
  const map = {};
  (destinationFiles || []).forEach((f) => {
    if (f && f.path) map[f.path] = f.folder || '.';
  });
  return map;
}

// Duplicate identity is per-occurrence, not per-path. When source folders
// overlap (e.g. `/card` and `/card/DCIM`) the same file path can appear
// twice in `files`; the server's `check-duplicates` stream flags only the
// *later* occurrence as an intra-import duplicate — the first is what will
// actually be copied. A path-set membership test would tag both tiles as
// duplicates, so enabling the filter would hide the to-copy file too and
// the grid could claim every tile is a duplicate while the summary still
// reports one file remaining. Match the server's ordering by walking
// `files` in reverse and consuming duplicate-count credits per path.
function computeDuplicateFlags(files, duplicatePaths) {
  const flags = new Array(files.length).fill(false);
  const remaining = new Map();
  (duplicatePaths || []).forEach((p) => {
    remaining.set(p, (remaining.get(p) || 0) + 1);
  });
  if (!remaining.size) return flags;
  for (let i = files.length - 1; i >= 0; i -= 1) {
    const p = files[i] && files[i].path;
    const count = remaining.get(p) || 0;
    if (count > 0) {
      flags[i] = true;
      remaining.set(p, count - 1);
    }
  }
  return flags;
}

function importCaptureDay(file) {
  return /^\d{4}-\d{2}-\d{2}$/.test(file.capture_date || '')
    ? file.capture_date : 'Unknown date';
}

function importDaySelectablePaths(entries) {
  const skip = document.getElementById('chkSkipDuplicates').checked;
  return entries.filter(entry => !(entry.isDuplicate && skip))
    .map(entry => entry.file.path).filter(Boolean);
}

function renderImportDaySummary(files, duplicateFlags) {
  const container = document.getElementById('importDaySummary');
  const rows = document.getElementById('importDayRows');
  rows.replaceChildren();
  importDayGroups = new Map();
  container.style.display = importSelectionEnabled() && files.length ? '' : 'none';
  if (!importSelectionEnabled()) return;
  // A source path can occur twice with overlapping source folders. Count
  // it once, retaining eligibility when either occurrence will be copied.
  const unique = new Map();
  files.forEach((file, i) => {
    const existing = unique.get(file.path);
    if (!existing || !duplicateFlags[i]) {
      unique.set(file.path, { file, isDuplicate: duplicateFlags[i] });
    }
  });
  unique.forEach(entry => {
    const day = importCaptureDay(entry.file);
    if (!importDayGroups.has(day)) importDayGroups.set(day, []);
    importDayGroups.get(day).push(entry);
  });
  Array.from(importDayGroups.keys()).sort().forEach((day, index) => {
    const row = document.createElement('tr');
    row.dataset.day = day;
    const selectCell = row.insertCell();
    const check = document.createElement('input');
    check.type = 'checkbox';
    check.className = 'day-check';
    check.setAttribute('aria-label', 'Import photos from ' + day);
    check.addEventListener('change', () => {
      importDaySelectablePaths(importDayGroups.get(day)).forEach(path => {
        if (check.checked) importDeselected.delete(path);
        else importDeselected.add(path);
      });
      updateImportSelectionUI();
      rerenderImportPreviewGridSafe();
    });
    selectCell.appendChild(check);
    const dateCell = row.insertCell();
    const toggle = document.createElement('button');
    toggle.type = 'button';
    toggle.className = 'import-day-toggle';
    toggle.setAttribute('aria-controls', 'import-day-' + index);
    toggle.addEventListener('click', () => {
      if (importCollapsedDays.has(day)) importCollapsedDays.delete(day);
      else importCollapsedDays.add(day);
      refreshImportDaySummary();
    });
    dateCell.appendChild(toggle);
    ['files', 'duplicates', 'selected'].forEach(name => {
      const cell = row.insertCell();
      cell.className = 'num day-' + name;
    });
    rows.appendChild(row);
  });
  refreshImportDaySummary();
}

function refreshImportDaySummary() {
  document.querySelectorAll('#importDayRows tr').forEach(row => {
    const day = row.dataset.day;
    const entries = importDayGroups.get(day) || [];
    const paths = importDaySelectablePaths(entries);
    const selected = paths.filter(path => !importDeselected.has(path)).length;
    const check = row.querySelector('.day-check');
    check.checked = paths.length > 0 && selected === paths.length;
    check.indeterminate = selected > 0 && selected < paths.length;
    check.disabled = paths.length === 0;
    check.title = paths.length ? 'Select or deselect photos from ' + day
      : 'All photos from ' + day + ' will be skipped as duplicates';
    row.querySelector('.day-files').textContent = entries.length.toLocaleString();
    row.querySelector('.day-duplicates').textContent = importDupStreamPending ? 'Checking…'
      : !document.getElementById('chkSkipDuplicates').checked ? 'Not checked'
      : entries.filter(entry => entry.isDuplicate).length.toLocaleString();
    row.querySelector('.day-selected').textContent = selected.toLocaleString();
    const expanded = !importCollapsedDays.has(day);
    const toggle = row.querySelector('.import-day-toggle');
    toggle.textContent = (expanded ? '▾ ' : '▸ ') + day;
    toggle.setAttribute('aria-expanded', String(expanded));
    const group = document.getElementById(toggle.getAttribute('aria-controls'));
    if (group) group.hidden = !expanded;
  });
}

function renderImportPreviewGrid(files, duplicatePaths, destinationFiles) {
  const grid = document.getElementById('importPreviewGrid');
  if (!grid) return;
  if (importThumbSchedulerCancel) {
    importThumbSchedulerCancel();
    importThumbSchedulerCancel = null;
  }
  grid.innerHTML = '';
  const destMap = importDestinationMap(destinationFiles);
  if (!files || !files.length) {
    renderImportDaySummary([], []);
    lastImportPreviewRender = null;
    updateHideDuplicatesControl(0);
    grid.style.display = 'none';
    return;
  }
  // Remember the inputs so the filter toggle can re-render without
  // re-running the preview or the duplicate check.
  lastImportPreviewRender = {
    files: files,
    duplicatePaths: duplicatePaths,
    destinationFiles: destinationFiles,
  };
  const duplicateFlags = computeDuplicateFlags(files, duplicatePaths);
  const dupeCount = duplicateFlags.reduce((n, isDup) => n + (isDup ? 1 : 0), 0);
  updateHideDuplicatesControl(dupeCount);
  const hidingDuplicates = dupeCount > 0 && hideDuplicatesEnabled();

  const skipDupes = document.getElementById('chkSkipDuplicates').checked;
  // The cards are still drawn in every mode — the preview is the honest list
  // of what will be imported either way. Only the controls that would claim
  // the user can change that list are withheld.
  const selectionEnabled = importSelectionEnabled();
  renderImportDaySummary(files, duplicateFlags);

  // Duplicate-heavy retries should foreground the work Vireo will actually
  // perform. Rendering hundreds of skipped cards obscures the one failed
  // file and schedules a thumbnail request for every duplicate. When the
  // user has opted into hiding duplicates, collapse them into one summary
  // and render only pending/unavailable items; otherwise fall back to the
  // per-folder view so each duplicate still surfaces as its own tile.
  const pendingFiles = files.filter((f, i) => !duplicateFlags[i]);
  const duplicateCount = files.length - pendingFiles.length;
  const groups = {};
  files.forEach((f, i) => {
    const key = JSON.stringify([selectionEnabled ? importCaptureDay(f) : '', f.subfolder || 'Source']);
    if (!groups[key]) groups[key] = [];
    groups[key].push({ file: f, isDuplicate: duplicateFlags[i] });
  });

  if (hidingDuplicates && duplicateCount) {
    const collapsed = document.createElement('div');
    collapsed.className = 'import-preview-collapsed';
    collapsed.setAttribute('data-testid', 'collapsed-duplicate-preview');
    collapsed.textContent = duplicateCount.toLocaleString() +
      ' duplicate' + (duplicateCount === 1 ? '' : 's') +
      ' hidden — ' + (duplicateCount === 1 ? 'it will' : 'they will') +
      ' be skipped. ' + (
        pendingFiles.length
          ? pendingFiles.length.toLocaleString() +
            ' other file' + (pendingFiles.length === 1 ? '' : 's') +
            ' in this preview.'
          : 'Nothing else will be imported from this selection.'
      );
    grid.appendChild(collapsed);
  }

  let anyTilesRendered = false;

  const dayContainers = new Map();
  Object.keys(groups).sort().forEach((key) => {
    const [day, subfolder] = JSON.parse(key);
    const all = groups[key];
    let parent = grid;
    if (day) {
      if (!dayContainers.has(day)) {
        const section = document.createElement('section');
        section.className = 'import-preview-day';
        section.id = 'import-day-' + dayContainers.size;
        section.hidden = importCollapsedDays.has(day);
        const title = document.createElement('div');
        title.className = 'import-preview-day-title';
        title.textContent = day;
        section.appendChild(title);
        grid.appendChild(section);
        dayContainers.set(day, section);
      }
      parent = dayContainers.get(day);
    }
    const shown = hidingDuplicates ? all.filter((e) => !e.isDuplicate) : all;
    const hiddenHere = all.length - shown.length;
    // Nothing to show and nothing hidden — no reason to render a header.
    // (Only reachable if a group somehow started empty, which shouldn't
    // happen since groups are built from `files`.)
    if (!shown.length && !hiddenHere) return;
    const groupEl = document.createElement('div');
    groupEl.className = 'import-preview-folder';
    const header = document.createElement('div');
    header.className = 'import-preview-folder-header';
    if (selectionEnabled) {
      const folderCheck = document.createElement('input');
      folderCheck.type = 'checkbox';
      folderCheck.className = 'folder-check';
      // Checked/indeterminate/disabled/title are all set by
      // refreshImportFolderHeaders() once the cards exist and the grid is
      // visible. It derives the tally from the rendered cards — including
      // their duplicate class — so the header and the boxes below it read
      // from exactly the same source and cannot disagree.
      folderCheck.addEventListener('change', () => {
        importFolderPaths(header.parentNode).forEach((p) => {
          if (folderCheck.checked) importDeselected.delete(p);
          else importDeselected.add(p);
        });
        updateImportSelectionUI();
        rerenderImportPreviewGridSafe();
      });
      // Omitted, not hidden: the header is a flex row with a gap, so a
      // zero-width box would still indent the folder name off its grid.
      header.appendChild(folderCheck);
    }
    const headerLabel = document.createElement('span');
    // Stamped as data, not recovered from the rendered text: with "Hide
    // duplicates" on, the label carries a "· N duplicates hidden" suffix and
    // the trailing-count regex refreshImportFolderHeaders() used to strip
    // would leave the checkbox tooltip naming the folder
    // "card-a (0) · 2 duplicates hidden".
    headerLabel.dataset.folder = subfolder;
    headerLabel.textContent = subfolder + ' (' + shown.length + ')' +
      (hiddenHere ? ' · ' + hiddenHere.toLocaleString() + ' duplicate' +
        (hiddenHere === 1 ? '' : 's') + ' hidden' : '');
    header.appendChild(headerLabel);
    groupEl.appendChild(header);

    // A folder made entirely of duplicates still gets its header — a
    // silent drop would leave the user with no evidence the folder or its
    // hidden count existed while other headers own their hidden files.
    if (!shown.length) {
      parent.appendChild(groupEl);
      return;
    }

    anyTilesRendered = true;
    const thumbsEl = document.createElement('div');
    thumbsEl.className = 'import-preview-thumbs';
    shown.forEach((entry) => {
      const f = entry.file;
      const isDuplicate = entry.isDuplicate;
      const isUnavailable = f.available === false;
      const card = document.createElement('div');
      card.className = 'import-preview-thumb' +
        (isUnavailable ? '' : ' skeleton') +
        (isDuplicate ? ' duplicate' : '');
      card.dataset.path = f.path || '';
      if (f.thumb_url) card.dataset.thumbUrl = f.thumb_url;
      card.title = f.path || f.filename || '';

      if (selectionEnabled) {
        const check = document.createElement('input');
        check.type = 'checkbox';
        check.className = 'thumb-check';
        // Derived, never seeded. The renderer runs up to three times per
        // preview and duplicate verdicts arrive late; recomputing from state
        // each pass means a late verdict just changes the answer.
        check.checked = !importDeselected.has(f.path)
          && !(isDuplicate && skipDupes);
        check.disabled = isDuplicate && skipDupes;
        check.addEventListener('click', (e) => {
          e.stopPropagation();
          toggleImportSelection(f.path, check.checked, e.shiftKey);
        });
        card.appendChild(check);
      }

      const imgWrap = document.createElement('div');
      imgWrap.className = 'import-preview-thumb-img';
      const img = document.createElement('img');
      img.alt = f.filename || '';
      imgWrap.appendChild(img);
      card.appendChild(imgWrap);

      if (isDuplicate) {
        const badge = document.createElement('div');
        badge.className = 'import-preview-badge';
        badge.textContent = 'Duplicate';
        card.appendChild(badge);
      } else if (isUnavailable) {
        const badge = document.createElement('div');
        badge.className = 'import-preview-badge';
        badge.textContent = 'Unavailable';
        card.appendChild(badge);
      }

      const meta = document.createElement('div');
      meta.className = 'import-preview-meta';
      const name = document.createElement('div');
      name.className = 'import-preview-name';
      name.textContent = f.filename || f.path || '';
      meta.appendChild(name);
      const dest = document.createElement('div');
      dest.className = 'import-preview-dest';
      const folder = destMap[f.path];
      dest.textContent = folder ? ('To: ' + (folder === '.' ? '(archive root)' : folder)) : (f.subfolder || '');
      meta.appendChild(dest);
      card.appendChild(meta);

      thumbsEl.appendChild(card);
    });
    groupEl.appendChild(thumbsEl);
    parent.appendChild(groupEl);
  });
  // Every folder ended up fully filtered, so replace the wall of
  // "(0) · N hidden" headers with a single clearer message that also
  // tells the user how to see the tiles again. When the collapsed
  // duplicate banner is already visible it already carries the count and
  // outcome, so keep it and skip the fallback message.
  if (!anyTilesRendered) {
    const collapsedBanner = grid.querySelector('.import-preview-collapsed');
    // Day summaries and their sections remain available even when every
    // thumbnail is filtered out.
    if (selectionEnabled) {
      grid.style.display = '';
      refreshImportFolderHeaders();
      return;
    }
    grid.innerHTML = '';
    if (collapsedBanner) {
      grid.appendChild(collapsedBanner);
    } else {
      const empty = document.createElement('div');
      empty.className = 'import-preview-empty';
      empty.textContent = 'All ' + files.length.toLocaleString() + ' file' +
        (files.length === 1 ? ' is a duplicate' : 's are duplicates') +
        ' — uncheck "Hide duplicates" to see them.';
      grid.appendChild(empty);
    }
  }
  grid.style.display = '';
  refreshImportFolderHeaders();
  setupImportThumbnailScheduler();
}

// Redraw the boxes after a selection change.
//
// This used to branch on `typeof rerenderImportPreviewGrid === 'function'`
// against a targeted DOM fallback, because the hide-duplicates filter (#1382)
// had not merged yet and there was no full re-render to call. It has merged;
// the fallback is dead code and is gone.
//
// A targeted refresh is still what happens, and deliberately so. The live
// path works — driven through rerenderImportPreviewGrid(), per-file toggles,
// folder headers, select-all and shift-ranges all behave and the headers do
// get refreshed — but it rebuilds every card on every checkbox click, which
// drops each <img> and re-runs setupImportThumbnailScheduler() over the whole
// grid. Measured on a six-card preview: 6 thumbnail requests per click
// through the full re-render, 0 through this refresh. That is linear in card
// size, and #1382 exists for 985-file cards. Selection state is derived, so
// only the boxes and the headers can change; the tiles cannot.
//
// The one thing a targeted refresh cannot do is honor a change in WHICH cards
// exist — that only happens when the filter itself is toggled, which calls
// rerenderImportPreviewGrid() directly (see onHideDuplicatesToggle).
function rerenderImportPreviewGridSafe() {
  const skipDupes = document.getElementById('chkSkipDuplicates').checked;
  importGridCards().forEach((el) => {
    const cb = el.querySelector('.thumb-check');
    if (!cb) return;
    const isDup = el.classList.contains('duplicate');
    cb.checked = !importDeselected.has(el.dataset.path) && !(isDup && skipDupes);
    cb.disabled = isDup && skipDupes;
  });
  // A header left claiming "all selected" after a per-file click is exactly
  // the stale readout this function exists to stop.
  refreshImportFolderHeaders();
  refreshImportDaySummary();
}

// #chkHideDuplicates changes which cards EXIST, not just which boxes are
// ticked, so it needs the full re-render rather than the targeted refresh
// above. Every selection readout on this page is derived from the rendered
// cards (importGridCards()), so the readouts have to resync afterwards or the
// select-all row, the "N of M selected" line and the Start label go on
// describing a grid that is no longer there.
function onHideDuplicatesToggle() {
  rerenderImportPreviewGrid();
  updateImportSelectionUI();
}

function setupImportThumbnailScheduler() {
  const grid = document.getElementById('importPreviewGrid');
  if (!grid) return;
  const pendingQueue = Array.from(grid.querySelectorAll('.import-preview-thumb.skeleton'));
  const visibleSet = new Set();
  let inFlight = 0;
  let cancelled = false;
  const concurrency = 4;
  let observer = null;

  importThumbSchedulerCancel = function() {
    cancelled = true;
    pendingQueue.length = 0;
    if (observer) observer.disconnect();
  };

  function dispatch(el) {
    const path = el.dataset.path;
    const img = el.querySelector('img');
    if (!img) return;
    inFlight += 1;
    const done = function() {
      el.classList.remove('skeleton');
      inFlight -= 1;
      if (!cancelled) pump();
    };
    img.onload = done;
    img.onerror = done;
    img.src = el.dataset.thumbUrl || ('/api/import/folder-preview/thumbnail?path=' + encodeURIComponent(path));
  }

  function pump() {
    while (inFlight < concurrency && pendingQueue.length > 0) {
      let idx = 0;
      for (let i = 0; i < pendingQueue.length; i += 1) {
        if (visibleSet.has(pendingQueue[i])) { idx = i; break; }
      }
      dispatch(pendingQueue.splice(idx, 1)[0]);
    }
  }

  if (!('IntersectionObserver' in window)) {
    pump();
    return;
  }

  observer = new IntersectionObserver((entries) => {
    entries.forEach((entry) => {
      if (entry.isIntersecting) visibleSet.add(entry.target);
      else visibleSet.delete(entry.target);
    });
    if (!cancelled) pump();
  }, { root: grid, rootMargin: '200px' });
  pendingQueue.forEach((thumb) => observer.observe(thumb));
  setTimeout(() => { if (!cancelled) pump(); }, 0);
}
