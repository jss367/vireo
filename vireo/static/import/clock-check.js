// Import preview: camera clocks that changed while this card was shot.
//
// The duplicate-check stream sends a `clock_problems` frame when the capture
// times it read run backwards in shot order, or the camera's time-zone setting
// changed between frames shot minutes apart. Importing still works; the panel
// says which frames are affected and what the Audit's Camera Clocks check will
// offer to fix once they are in the catalog. clearImportPreviewGrid() empties
// it with every new preview.

/**
 * @param {ImportClockFix} fix
 */
function importClockFixText(fix) {
  const range = fix.count === 1 ? fix.first : fix.first + '–' + fix.last;
  return fix.count + ' photo' + (fix.count === 1 ? '' : 's') + ' (' + range + '): ' +
    fix.label + '. ' + fix.explanation;
}

/**
 * Show the camera-clock problems found on the card, or hide the panel.
 * @param {ImportClockProblem[]} problems
 */
function renderImportClockProblems(problems) {
  const panel = /** @type {HTMLElement | null} */ (document.getElementById('importClockProblems'));
  if (!panel) return;
  panel.replaceChildren();
  if (!problems.length) {
    panel.style.display = 'none';
    return;
  }
  const title = document.createElement('h4');
  title.textContent = problems.length === 1
    ? 'A camera clock changed while these photos were taken'
    : problems.length + ' camera clocks changed while these photos were taken';
  panel.appendChild(title);
  for (const problem of problems) {
    const item = document.createElement('div');
    item.className = 'import-clock-problem';
    const head = document.createElement('div');
    head.textContent = (problem.camera ? problem.camera + ': ' : '') + problem.summary + '.';
    item.appendChild(head);
    const list = document.createElement('ul');
    for (const line of [...problem.evidence, ...problem.fixes.map(importClockFixText)]) {
      const li = document.createElement('li');
      li.textContent = line;
      list.appendChild(li);
    }
    item.appendChild(list);
    panel.appendChild(item);
  }
  const note = document.createElement('div');
  note.className = 'hint';
  note.textContent = 'Sorting by capture time will put these photos out of order. The import is ' +
    'unaffected. After importing, Audit → Camera Clocks checks them against GPS and location ' +
    'and can rewrite their capture times.';
  panel.appendChild(note);
  panel.style.display = '';
}
