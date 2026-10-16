// The confirm modal: plain confirms and the merge review.
// Classic page script; load boot.js after all definitions.

/* ---------- Confirm Modal ---------- */
function showConfirm(title, message, callback) {
  document.querySelector('#confirmHeading .confirm-heading-icon').style.display = 'none';
  document.getElementById('confirmEyebrow').textContent = 'Please confirm';
  document.getElementById('confirmTitle').textContent = title;
  document.getElementById('confirmMessage').textContent = message;
  document.getElementById('confirmOkBtn').textContent = 'Move';
  confirmCallback = callback;
  document.getElementById('confirmModal').classList.add('open');
}

function appendConfirmRoute(container, label, path) {
  var row = document.createElement('div');
  row.className = 'confirm-route-row';
  var routeLabel = document.createElement('span');
  routeLabel.className = 'confirm-route-label';
  routeLabel.textContent = label;
  var routePath = document.createElement('span');
  routePath.className = 'confirm-route-path';
  routePath.textContent = path;
  row.appendChild(routeLabel);
  row.appendChild(routePath);
  container.appendChild(row);
}

function appendConfirmStat(container, value, label) {
  var stat = document.createElement('div');
  stat.className = 'confirm-stat';
  var statValue = document.createElement('span');
  statValue.className = 'confirm-stat-value';
  statValue.textContent = value;
  var statLabel = document.createElement('span');
  statLabel.className = 'confirm-stat-label';
  statLabel.textContent = label;
  stat.appendChild(statValue);
  stat.appendChild(statLabel);
  container.appendChild(stat);
}

function appendConfirmNote(container, icon, title, detail, warning) {
  var note = document.createElement('div');
  note.className = 'confirm-note' + (warning ? ' warning' : '');
  var noteIcon = document.createElement('span');
  noteIcon.className = 'confirm-note-icon';
  noteIcon.setAttribute('aria-hidden', 'true');
  noteIcon.textContent = icon;
  var copy = document.createElement('div');
  var strong = document.createElement('strong');
  strong.textContent = title;
  copy.appendChild(strong);
  copy.appendChild(document.createTextNode(' ' + detail));
  note.appendChild(noteIcon);
  note.appendChild(copy);
  container.appendChild(note);
}

function showMergeConfirm(details, callback) {
  var heading = document.getElementById('confirmHeading');
  heading.querySelector('.confirm-heading-icon').style.display = 'grid';
  document.getElementById('confirmEyebrow').textContent = 'Destination already exists';
  document.getElementById('confirmTitle').textContent = 'Merge with the existing folder?';
  document.getElementById('confirmOkBtn').textContent = 'Merge & Resume';

  var message = document.getElementById('confirmMessage');
  message.replaceChildren();

  var route = document.createElement('div');
  route.className = 'confirm-route';
  appendConfirmRoute(route, 'From', details.sourcePath);
  appendConfirmRoute(route, 'Into', details.destinationPath);
  message.appendChild(route);

  var stats = document.createElement('div');
  stats.className = 'confirm-stats';
  var photos = Number(details.photoCount) || 0;
  appendConfirmStat(stats, photos.toLocaleString(), 'photo' + (photos !== 1 ? 's' : '') + ' in source');
  if (details.copyCount !== null) {
    appendConfirmStat(stats, details.copyCount.toLocaleString(),
      'file' + (details.copyCount !== 1 ? 's' : '') + ' to copy');
    if (details.skipCount) {
      appendConfirmStat(stats, details.skipCount.toLocaleString(),
        'already present and kept');
    }
    if (details.blockCount) {
      appendConfirmStat(stats, details.blockCount.toLocaleString(),
        'blocked by a non-file');
    }
  } else {
    var destCount = details.destinationFileCount.toLocaleString();
    if (details.destinationFileCountTruncated) destCount += '+';
    appendConfirmStat(stats, destCount, 'file' +
      (details.destinationFileCount !== 1 || details.destinationFileCountTruncated ? 's' : '') +
      ' already at destination');
  }
  message.appendChild(stats);

  if (details.skipCount || details.copyCount === null) {
    appendConfirmNote(message, '↔', 'Existing files stay untouched.',
      'Only missing files are copied. If same-name files differ, the merge is canceled and nothing is changed.', false);
  }
  if (details.blockCount) {
    var blocked = details.blockCount.toLocaleString() + ' source file' +
      (details.blockCount !== 1 ? 's have' : ' has') +
      ' a symlink or non-file in the destination. The merge will stop at verification before any originals are removed.';
    appendConfirmNote(message, '!', 'Merge will not complete.', blocked, true);
  }
  var verifyDetail = details.remote
    ? 'Files are checksum-verified over SSH before any originals are removed.'
    : 'Originals are removed only after every source file is verified at the destination.';
  appendConfirmNote(message, '✓', 'Your originals stay put until verification.', verifyDetail, false);

  confirmCallback = callback;
  document.getElementById('confirmModal').classList.add('open');
}

function closeConfirm() {
  document.getElementById('confirmModal').classList.remove('open');
  confirmCallback = null;
}

function confirmOk() {
  var cb = confirmCallback;
  closeConfirm();
  if (cb) cb();
}
