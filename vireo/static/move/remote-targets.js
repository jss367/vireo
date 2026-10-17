// Remote (SSH) move targets: the destination-mode select and the remote target id.
// Classic page script; load boot.js after all definitions.

/* ---------- Remote (SSH) move targets ---------- */
/** @type {MoveRemoteTarget[]} */ var quickRemoteTargets = [];
var quickRsyncAvailable = false;

async function loadRemoteTargets() {
  try {
    var data = await safeFetch('/api/remote-targets');
    quickRemoteTargets = (data && data.targets) || [];
    quickRsyncAvailable = !!(data && data.rsync_available);
  } catch (e) {
    quickRemoteTargets = [];
    quickRsyncAvailable = false;
  }
  var sel = /** @type {HTMLSelectElement} */ (document.getElementById('quickDestMode'));
  if (!sel) return;
  // Rebuild remote options, keeping the "Local path" option at index 0.
  while (sel.options.length > 1) sel.remove(1);
  quickRemoteTargets.forEach(function(t) {
    var opt = document.createElement('option');
    opt.value = 'remote:' + t.id;
    opt.textContent = t.name + ' — ' + t.user + '@' + t.host + ' (SSH)';
    sel.appendChild(opt);
  });
}

// The remote target id when the destination mode is a remote target, else ''.
function quickRemoteTargetId() {
  var el = /** @type {HTMLSelectElement} */ (document.getElementById('quickDestMode'));
  var v = el ? el.value : 'local';
  return v.indexOf('remote:') === 0 ? v.slice(7) : '';
}

function onQuickDestModeChange() {
  var remoteId = quickRemoteTargetId();
  /** @type {HTMLElement} */ (document.getElementById('quickLocalRow')).style.display = remoteId ? 'none' : '';
  /** @type {HTMLElement} */ (document.getElementById('quickRemoteRow')).style.display = remoteId ? '' : 'none';
  var dateOption = /** @type {HTMLOptionElement | null} */ (document.querySelector('#quickFolderMode option[value="date"]'));
  if (dateOption) dateOption.disabled = !!remoteId;
  if (remoteId && quickUsesDateFolders()) {
    /** @type {HTMLSelectElement} */ (document.getElementById('quickFolderMode')).value = 'single';
    onQuickFolderModeChange();
    return;
  }
  updateQuickMoveBtn();
}
