// Decisions: the current photo, its zone, pick/reject/review, clear, undo,
// advancing to the next unreviewed photo, and selecting a photo.
// Classic page script; load boot.js after all definitions.

function currentPhoto() {
  if (rapid.currentIndex < 0 || rapid.currentIndex >= rapid.items.length) return null;
  return rapid.items[rapid.currentIndex];
}

/** @param {number} id @returns {RapidZone} */
function zoneFor(id) {
  if (rapid.picks.has(id)) return 'pick';
  if (rapid.rejects.has(id)) return 'reject';
  return 'review';
}

/** @param {number} id @param {RapidZone} zone */
function setZone(id, zone) {
  rapid.picks.delete(id);
  rapid.rejects.delete(id);
  if (zone === 'pick') rapid.picks.add(id);
  else if (zone === 'reject') rapid.rejects.add(id);
}

/** @param {RapidZone} zone */
function decideCurrent(zone) {
  var p = currentPhoto();
  // Block until DB seed lands. Otherwise the seed loop in openSession() can
  // ADD a flag for a photo the user already rejected via hotkey, leaving it
  // in both picks and rejects — apply then 500s with "appears in more than
  // one zone".
  if (!p || rapid.applying || !rapid.seeded) return;
  var id = p.id;
  var entry = {
    id: id,
    prevZone: zoneFor(id),
    prevReviewed: rapid.reviewed.has(id),
    prevTouched: rapid.touched.has(id),
    prevIndex: rapid.currentIndex,
  };
  rapid.history.push(entry);
  setZone(id, zone);
  rapid.reviewed.add(id);
  rapid.touched.add(id);
  advanceToNext();
  resetRapidZoom();
  renderAll();
}

function clearCurrent() {
  var p = currentPhoto();
  if (!p || rapid.applying || !rapid.seeded) return;
  var id = p.id;
  rapid.history.push({
    id: id,
    prevZone: zoneFor(id),
    prevReviewed: rapid.reviewed.has(id),
    prevTouched: rapid.touched.has(id),
    prevIndex: rapid.currentIndex,
  });
  setZone(id, 'review');
  rapid.touched.add(id);
  renderAll();
}

function advanceToNext() {
  if (!rapid.items.length) {
    rapid.currentIndex = -1;
    return;
  }
  for (var i = rapid.currentIndex + 1; i < rapid.items.length; i++) {
    if (!rapid.reviewed.has(/** @type {RapidPhoto} */ (rapid.items[i]).id)) {
      rapid.currentIndex = i;
      return;
    }
  }
  for (var j = 0; j < rapid.currentIndex; j++) {
    if (!rapid.reviewed.has(/** @type {RapidPhoto} */ (rapid.items[j]).id)) {
      rapid.currentIndex = j;
      return;
    }
  }
  rapid.currentIndex = -1;
}

function undoLast() {
  if (!rapid.history.length || rapid.applying) return;
  var entry = /** @type {RapidHistory} */ (rapid.history.pop());
  setZone(entry.id, entry.prevZone);
  if (entry.prevReviewed) rapid.reviewed.add(entry.id);
  else rapid.reviewed.delete(entry.id);
  if (entry.prevTouched) rapid.touched.add(entry.id);
  else rapid.touched.delete(entry.id);
  rapid.currentIndex = entry.prevIndex;
  resetRapidZoom();
  renderAll();
}

/** @param {number} id */
function selectPhoto(id) {
  var idx = rapid.items.findIndex(function(p) { return p.id === id; });
  if (idx >= 0) {
    rapid.currentIndex = idx;
    resetRapidZoom();
    renderAll();
  }
}
