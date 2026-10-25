// The confirm modal: plain confirms and the merge review.

import { state } from './state.js';

/* ---------- Confirm Modal ---------- */
/** @param {string} title @param {string} message @param {() => unknown} callback */
export function showConfirm(title, message, callback) {
  /** @type {HTMLElement} */ (document.querySelector('#confirmHeading .confirm-heading-icon')).style.display = 'none';
  /** @type {HTMLElement} */ (document.getElementById('confirmEyebrow')).textContent = 'Please confirm';
  /** @type {HTMLElement} */ (document.getElementById('confirmTitle')).textContent = title;
  /** @type {HTMLElement} */ (document.getElementById('confirmMessage')).textContent = message;
  /** @type {HTMLButtonElement} */ (document.getElementById('confirmOkBtn')).textContent = 'Move';
  state.confirmCallback = callback;
  /** @type {HTMLElement} */ (document.getElementById('confirmModal')).classList.add('open');
}

/** @param {HTMLElement} container @param {string} label @param {string} path */
export function appendConfirmRoute(container, label, path) {
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

/** @param {HTMLElement} container @param {string} value @param {string} label */
export function appendConfirmStat(container, value, label) {
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

/** @param {HTMLElement} container @param {string} icon @param {string} title @param {string} detail @param {boolean} warning */
export function appendConfirmNote(container, icon, title, detail, warning) {
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

/** @param {MoveFolderDetails} details @param {() => unknown} callback */
export function showMoveConfirm(details, callback) {
  showConfirm('Move Folder', '', callback);
  var message = /** @type {HTMLElement} */ (document.getElementById('confirmMessage'));
  var photos = details.photoCount;
  var lead = document.createElement('p');
  lead.className = 'confirm-lead';
  lead.textContent = photos === null
    ? 'Confirm moving photos:'
    : 'Confirm moving ' + photos.toLocaleString() + ' photo' + (photos !== 1 ? 's' : '') + ':';
  message.appendChild(lead);

  // Stack the paths in one aligned column so the user can compare them
  // segment by segment instead of hunting for the split inside a sentence.
  var route = document.createElement('div');
  route.className = 'confirm-route';
  appendConfirmRoute(route, 'From', details.sourcePath);
  appendConfirmRoute(route, 'To', details.destinationPath);
  message.appendChild(route);

  var verifyDetail = details.remote
    ? 'Transferred over SSH and verified by checksum before originals are removed.'
    : 'Files will be copied and verified before originals are removed.';
  message.appendChild(document.createTextNode(verifyDetail));
}

/** @param {MoveMergeDetails} details @param {() => unknown} callback */
export function showMergeConfirm(details, callback) {
  var heading = /** @type {HTMLElement} */ (document.getElementById('confirmHeading'));
  /** @type {HTMLElement} */ (heading.querySelector('.confirm-heading-icon')).style.display = 'grid';
  /** @type {HTMLElement} */ (document.getElementById('confirmEyebrow')).textContent = 'Destination already exists';
  /** @type {HTMLElement} */ (document.getElementById('confirmTitle')).textContent = 'Merge with the existing folder?';
  /** @type {HTMLButtonElement} */ (document.getElementById('confirmOkBtn')).textContent = 'Merge & Resume';

  var message = /** @type {HTMLElement} */ (document.getElementById('confirmMessage'));
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

  state.confirmCallback = callback;
  /** @type {HTMLElement} */ (document.getElementById('confirmModal')).classList.add('open');
}

export function closeConfirm() {
  /** @type {HTMLElement} */ (document.getElementById('confirmModal')).classList.remove('open');
  state.confirmCallback = null;
}

export function confirmOk() {
  var cb = state.confirmCallback;
  closeConfirm();
  if (cb) cb();
}
