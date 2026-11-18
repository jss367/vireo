// Inspection evidence stays with its workspace, photo and region generation.
var rqAnimalStates = ['Unresolved', 'Inspected animal', 'Excluded background', 'Excluded duplicate'];

/** @param {ReviewQueueItemState} item @param {string} generation */
function rqPrepareCoverage(item, generation) {
  if (item.generation && item.generation !== generation) {
    delete item.entry;
    item.subject = null;
    item.center = null;
  }
  item.generation = generation;
  item.coverage ||= {};
  item.coverage[generation] ||= {confirmed: false, notes: reviewQueue.subjects.map(s => ({
    state: 'Unresolved', evidence: '', region: s.id, label: 'Region ' + s.id
  }))};
}

function rqActiveCoverage() {
  if (!reviewQueue.current || !reviewQueue.inspection) return;
  return rqItem(reviewQueue.current.id).coverage?.[reviewQueue.inspection.generation];
}

/** @param {ReviewAnimalCoverage} record */
function rqCoverageStatus(record) {
  var pending = record.notes.filter(n => n.state === 'Unresolved').length;
  var animals = record.notes.filter(n => n.state === 'Inspected animal').length;
  var complete = !pending && record.confirmed === true;
  return (complete ? 'Complete' : 'Incomplete') + ' · ' + reviewQueue.subjects.length +
    ' region candidates · ' + animals + ' inspected animals · ' + pending + ' unresolved entries' +
    (complete && !animals ? ' · full frame confirmed empty' : '');
}

function rqRenderCoverage() {
  const record = rqActiveCoverage();
  var comparison = rqElement('rqComparison');
  comparison.textContent = '';
  if (!record || !reviewQueue.current || !reviewQueue.inspection) return;
  var item = rqItem(reviewQueue.current.id);
  var region = reviewQueue.subjects.find(s => s.id === item.subject);
  var entry = item.entry === undefined ? null : record.notes[item.entry];
  comparison.textContent = 'Photo ' + reviewQueue.current.id + ' · ' + (entry ? entry.label + ' · ' : '') +
    (item.center ? 'manual center ' + item.center.map(v => v.toFixed(3)).join(', ') :
      region ? 'crop of region ' + region.id : 'no selected region') + rqNeighborComparison(reviewQueue.inspection, region, item);
  rqElement('rqCoverageStatus').textContent = rqCoverageStatus(record);
  var confirm = /** @type {HTMLInputElement} */ (rqElement('rqConfirmCoverage'));
  confirm.checked = record.confirmed;
  confirm.disabled = !rqCanConfirm(record);
  /** @type {HTMLButtonElement} */ (rqElement('rqSplit')).disabled = !region;
  rqElement('rqAnimals').replaceChildren(...record.notes.map(n => rqAnimalRow(record, n)));
  var history = Object.entries(item.coverage || {}).filter(([key]) => key !== item.generation);
  rqElement('rqPrior').hidden = !history.length;
  rqElement('rqPriorNotes').textContent = history.map(([key, old]) => key.slice(0, 12) + '\n' +
    (old.confirmed ? 'Full frame confirmed' : 'Incomplete') + '\n' +
    old.notes.map(n => [n.label, n.state, n.evidence, n.center && 'center ' + n.center.join(', ')].filter(Boolean).join(' · ')).join('\n')).join('\n\n');
}

/** @param {ReviewInspection} inventory @param {ReviewQueueSubject|undefined} region @param {ReviewQueueItemState} item */
function rqNeighborComparison(inventory, region, item) {
  if (!inventory.previous_photo_id) return '';
  var prior = reviewQueue.session.items[String(inventory.previous_photo_id)];
  var reviewed = prior?.generation !== undefined;
  var id = reviewed ? prior?.subject : inventory.previous_primary;
  var box = inventory.previous_regions.find(r => r.id === id);
  var label = '. Previous burst photo ' + inventory.previous_photo_id +
    (id ? (reviewed ? ' last selected region ' : ' primary region ') + id : ' had no selected region');
  if (!region || !box || prior?.center || item.center || ![box.x, box.y, box.w, box.h,
    region.box_x, region.box_y, region.box_w, region.box_h].every(Number.isFinite)) return label + '. Subject continuity unknown.';
  var width = Math.min(box.x + box.w, region.box_x + region.box_w) - Math.max(box.x, region.box_x);
  var height = Math.min(box.y + box.h, region.box_y + region.box_h) - Math.max(box.y, region.box_y);
  var intersection = Math.max(0, width) * Math.max(0, height);
  var union = box.w * box.h + region.box_w * region.box_h - intersection;
  return label + (intersection / union < inventory.min_overlap ? '. Possible subject switch (low box overlap).' :
    '. Boxes overlap, identity unverified.');
}

/** @param {ReviewAnimalCoverage} record @param {ReviewAnimalNote} note */
function rqAnimalRow(record, note) {
  var row = document.createElement('div');
  row.className = 'rq-toolbar';
  var reference = document.createElement('button');
  reference.type = 'button';
  var region = reviewQueue.subjects.find(s => s.id === note.region);
  reference.textContent = note.label + (region?.inspection_reason ? ' · ' + region.inspection_reason : '');

  reference.onclick = function() {
    if (!reviewQueue.current) return;
    var item = rqItem(reviewQueue.current.id);
    item.entry = record.notes.indexOf(note);
    item.subject = note.region;
    item.center = note.center || null;
    rqSave(); rqRenderSubjects(); rqLoadCrop();
  };
  var state = document.createElement('select');
  state.setAttribute('aria-label', note.label + ' review state');
  rqAnimalStates.forEach(s => state.add(new Option(s, s)));
  state.value = note.state;
  state.onchange = function() {
    note.state = state.value;
    record.confirmed = false;
    rqSave(); rqRenderCoverage();
  };
  var evidence = document.createElement('input');
  evidence.setAttribute('aria-label', note.label + ' evidence reference or description');
  evidence.placeholder = 'Evidence reference or description';
  evidence.value = note.evidence;
  evidence.oninput = function() { note.evidence = evidence.value; rqSave(); };
  row.append(reference, state, evidence);
  return row;
}

/** @param {ReviewAnimalCoverage} record */
function rqCanConfirm(record) {
  var frame = /** @type {HTMLImageElement} */ (rqElement('rqFull'));
  return frame.complete && frame.naturalWidth > 0 && !record.notes.some(n => n.state === 'Unresolved');
}

function rqBindCoverage() {
  rqElement('rqFull').addEventListener('load', rqRenderCoverage);
  rqElement('rqFull').addEventListener('error', rqRenderCoverage);
  for (const [id, label] of Object.entries({rqSplit: 'Shared region', rqUnlocalized: 'Unlocalized animal', rqClipped: 'Source-clipped animal'})) {
    rqElement(id).onclick = function() {
      const record = rqActiveCoverage();
      if (!record || !reviewQueue.current) return;
      var region = id === 'rqSplit' ? rqItem(reviewQueue.current.id).subject : null;
      if (id === 'rqSplit' && !region) return;
      record.notes.push({state: 'Unresolved', region, evidence: '',
        label: label + (region ? ' ' + region : '') + ' · entry ' + (record.notes.length + 1)});
      record.confirmed = false;
      rqSave(); rqRenderCoverage();
    };
  }
  rqElement('rqConfirmCoverage').onchange = function(event) {
    const record = rqActiveCoverage();
    if (!record || !rqCanConfirm(record)) return;
    record.confirmed = /** @type {HTMLInputElement} */ (event.target).checked;
    rqSave(); rqRenderCoverage();
  };
}
