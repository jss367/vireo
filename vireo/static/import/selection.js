// Individual, range, folder, and select-all file selection.
// Classic page script; load boot.js after all definitions.

// Called from initImportPage() alongside wireDestStructureInvalidation():
// #chkSelectAllImport is static template markup, so it already exists, and
// wiring before the first await closes the window where a preview could
// render with a dead master checkbox.
function wireImportSelectAll() {
  const master = document.getElementById('chkSelectAllImport');
  if (!master) return;
  master.addEventListener('change', () => {
    // Include collapsed days. Only shift-click ranges are scoped to visible cards.
    importSelectableCardPaths(importGridCards())
      .forEach(p => {
        if (master.checked) importDeselected.delete(p);
        else importDeselected.add(p);
      });
    updateImportSelectionUI();
    rerenderImportPreviewGridSafe();
  });
}

// Every card in the rendered grid, in render order.
function importGridCards() {
  const grid = document.getElementById('importPreviewGrid');
  return grid ? Array.from(grid.querySelectorAll('.import-preview-thumb')) : [];
}

// Shift-click ranges exclude collapsed days; duplicate filtering omits cards entirely.
function importVisibleCards(root) {
  const scope = root || document.getElementById('importPreviewGrid');
  if (!scope) return [];
  return Array.from(scope.querySelectorAll('.import-preview-thumb'))
    .filter(el => el.offsetParent !== null);
}

// Duplicate-ness is read off the CARD, and there is deliberately no
// module-level set of duplicate paths to read instead. Such a set could only
// be assigned once the whole check-duplicates stream had drained, so during a
// re-preview it would still hold the PREVIOUS run's verdicts while the cards
// on screen carry none. Consulting it here would disable a card the renderer
// drew enabled — and with no badge, since the badge is drawn from the same
// argument the card was, leaving the user with a dead control and no
// explanation. Over SMB with verify_by_hash that window is minutes long.
function importSelectableCardPaths(cards) {
  const skipDupes = document.getElementById('chkSkipDuplicates').checked;
  return cards
    .filter(el => !(el.classList.contains('duplicate') && skipDupes))
    .map(el => el.dataset.path)
    .filter(p => !!p);
}

// The selectable paths in one rendered folder group. Skipped duplicates are
// excluded: they're already ineligible, so a bulk toggle must neither select
// them (it can't) nor record a deselection for them (that would turn an
// eligibility verdict into user intent, and it would outlive the verdict).
function importFolderPaths(groupEl) {
  if (!groupEl) return [];
  return importSelectableCardPaths(Array.from(groupEl.querySelectorAll('.import-preview-thumb')));
}

// Folder headers are a tally of the cards under them, so they have to be
// recomputed after ANY selection change — including a single per-file click,
// which otherwise leaves the header claiming "all selected".
function refreshImportFolderHeaders() {
  const grid = document.getElementById('importPreviewGrid');
  if (!grid) return;
  grid.querySelectorAll('.import-preview-folder').forEach((groupEl) => {
    const folderCheck = groupEl.querySelector('.folder-check');
    if (!folderCheck) return;
    const paths = importFolderPaths(groupEl);
    const onCount = paths.filter(p => !importDeselected.has(p)).length;
    // "All", not "any" — matches chkSelectAll on pipeline.html so the same
    // widget means the same thing on both pages. Consequence: clicking a
    // dashed header SELECTS the rest of the folder rather than clearing it,
    // which is also the conventional web behaviour for a tri-state parent.
    folderCheck.checked = paths.length > 0 && onCount === paths.length;
    // Tri-state: "some" must not read as "all". Only settable from JS.
    folderCheck.indeterminate = onCount > 0 && onCount < paths.length;
    // Nothing to toggle — an enabled control that silently does nothing and
    // snaps back is worse than an honestly dead one.
    folderCheck.disabled = paths.length === 0;
    // Name the folder. refreshImportFolderHeaders() has no `subfolder` in
    // scope, so read it off the label the renderer put beside the box —
    // from its data attribute, not by stripping the trailing count out of
    // its text: with "Hide duplicates" on, that text ends in
    // "· N duplicates hidden" (#1382) and the regex left the tooltip
    // reading "Every file in card-a (0) · 2 duplicates hidden is a…".
    const labelEl = folderCheck.parentNode.querySelector('span');
    const name = labelEl
      ? (labelEl.dataset.folder
         ?? labelEl.textContent.replace(/ \(\d+\)$/, ''))
      : '';
    folderCheck.title = paths.length === 0
      ? 'Every file in ' + name + ' is a duplicate that will be skipped'
      : 'Select or deselect every file in ' + name;
  });
}

function toggleImportSelection(path, checked, shiftKey) {
  // Range runs over VISIBLE RENDER order — not importPreviewedPaths order,
  // which is the API's. The renderer groups by subfolder and sorts the group
  // keys, so the two diverge whenever subfolders don't arrive alphabetically,
  // and a range computed from the wrong one toggles cards the user didn't
  // drag across. Hidden cards are excluded for the same reason.
  const cards = importVisibleCards();
  const visible = cards.map(el => el.dataset.path);
  let targets = [path];
  if (shiftKey && importSelectionAnchor !== null) {
    const a = visible.indexOf(importSelectionAnchor);
    const b = visible.indexOf(path);
    if (a !== -1 && b !== -1) {
      // min/max, not a..b: the anchor can sit after the click target.
      targets = visible.slice(Math.min(a, b), Math.max(a, b) + 1);
    }
  }
  const selectable = new Set(importSelectableCardPaths(cards));
  targets
    .filter(p => selectable.has(p))
    .forEach(p => {
      if (checked) importDeselected.delete(p);
      else importDeselected.add(p);
    });
  // Re-anchor on every click, shift-click included, so the next range starts
  // from where this one ended.
  importSelectionAnchor = path;
  updateImportSelectionUI();
  rerenderImportPreviewGridSafe();
}

// Counted off the rendered cards, like every other readout on this page.
// Counting against a retained set of duplicate paths would under-report
// during a re-preview — such a set holds the previous run's verdicts until
// the new check-duplicates stream drains — and the numbers drive the master
// checkbox, so a stale count doesn't just print a wrong figure: it disables
// a live control over cards that carry no badge to explain it.
//
// Counted over UNIQUE paths, not over cards. /api/import/folder-preview
// appends per source with no cross-source dedup, so two nested sources
// (/card and /card/DCIM) discover the same file twice and the grid draws two
// cards for it — one file, which the import copies once. Counting cards
// would put "Start import (3 files)" on a run that copies 2, and would send
// a checked_count the route rejects: it enforces
// checked_count <= len(set(include_paths)).
function importEligibleCount() {
  return new Set(importSelectableCardPaths(importGridCards())).size;
}

function importCheckedCount() {
  return new Set(importSelectableCardPaths(importGridCards())
    .filter(p => !importDeselected.has(p))).size;
}

function updateImportSelectionUI() {
  refreshImportDaySummary();
  const el = document.getElementById('previewSelectedCount');
  if (el) {
    // The CHECKED count — what will actually be copied. Never the
    // include_paths count, which is larger because it retains duplicates.
    // Both halves count unique paths, for the nested-sources reason on
    // importEligibleCount(): a file discovered twice is still one file, and
    // "3 of 3" over a two-file import is the same lie as the numerator's.
    el.textContent = importCheckedCount().toLocaleString() + ' of '
      + new Set(importPreviewedPaths).size.toLocaleString() + ' selected';
  }
  const master = document.getElementById('chkSelectAllImport');
  if (master) {
    // Denominator is the ELIGIBLE count, not importPreviewedPaths.length:
    // with "everything selected except two skipped duplicates" the master
    // must read as full, not as a partial selection the user has to go
    // hunting for.
    // Tri-state rule is "all", matching chkSelectAll on pipeline.html
    // (selected === total) so the same widget means the same thing on both
    // pages; `indeterminate` is what carries "some".
    const eligible = importEligibleCount();
    const checked = importCheckedCount();
    master.checked = eligible > 0 && checked === eligible;
    master.indeterminate = checked > 0 && checked < eligible;
    // Nothing selectable (every discovered file is a skipped duplicate):
    // a live control that does nothing on click is a black box.
    master.disabled = eligible === 0;
    master.title = eligible === 0
      ? 'Every discovered file is a duplicate that will be skipped'
      : 'Select or deselect every file in this preview';
  }
  const row = document.getElementById('selectAllRow');
  // The selectionEnabled check matters: without it a later call to this
  // function can re-show the select-all row in in-place mode after the
  // renderer has already left the per-file boxes out.
  const selectionEnabled = importSelectionEnabled();
  if (row) {
    row.style.display =
      (selectionEnabled && importPreviewedPaths.length) ? '' : 'none';
  }
  // Owned here rather than by the renderer, and NOT gated on a rendered
  // grid: updateImportMode() throws the preview away, so a note that only
  // ever moved inside renderImportPreviewGrid() would survive a switch to
  // copy mode and sit there contradicting the controls the user is about to
  // get. This function is on every path that changes the mode.
  const note = document.getElementById('selectionUnavailableNote');
  if (note) {
    note.textContent = selectionUnavailableText();
    note.style.display = selectionEnabled ? 'none' : '';
  }
  updateStartGate();
}
