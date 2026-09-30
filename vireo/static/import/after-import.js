// After-import process defaults, local processing, and archive transfer options.
// Classic page script; load boot.js after all definitions.

// Populate the After Import dropdown from the saved-process library. Keeps
// the two static options (hidden default + None) and appends one option per
// saved process, value = its id.
async function loadAfterImportProcesses() {
  const sel = document.getElementById('afterImportSelect');
  if (!sel) return;
  try {
    const resp = await fetch('/api/processes');
    if (!resp.ok) return;
    const procs = await resp.json();
    Array.from(sel.querySelectorAll('option[data-process]')).forEach(
      (o) => o.remove());
    (procs || []).forEach((p) => {
      const opt = document.createElement('option');
      opt.value = String(p.id);
      opt.textContent = p.name;
      opt.dataset.process = '1';
      sel.appendChild(opt);
    });
  } catch (e) { /* leave with just "None — import only" */ }
}

function selectedAfterImport() {
  const v = document.getElementById('afterImportSelect').value;
  // __hidden_default__ is a display-only placeholder — the request path
  // omits after_import in that state, so callers never read this value.
  if (v === '__none__' || v === '__hidden_default__') return null;
  const n = parseInt(v, 10);
  return Number.isNaN(n) ? null : n;
}

// True when a Start Import would chain a processing run — either the user
// (or the workspace default prefill) has a real process selected, or the
// "New workspace default" placeholder is masking a real global default the
// server will apply after creating the workspace. The chained NAS move
// fires from that processing run, so this gates the "Then move to NAS" row.
function afterImportChainsProcess() {
  const sel = document.getElementById('afterImportSelect');
  if (!sel) return false;
  if (sel.value === '__hidden_default__') {
    const placeholder = document.getElementById('afterImportHiddenDefault');
    return !!(placeholder && placeholder.dataset.maskedValue);
  }
  return selectedAfterImport() !== null;
}

// Normalize a path for display and prefix math: forward slashes, no
// trailing slash. Case is PRESERVED — this feeds the visible move preview
// (NAS-side paths are case-sensitive) and the rel-subpath slicing.
function afterMoveNormPath(p) {
  return String(p || '').replace(/\\/g, '/').replace(/\/+$/, '');
}

// Containment gate compares case-INSENSITIVELY, mirroring the server's
// alias-folding check (_validate_after_process_move via
// _path_equal_or_descends): on the default case-insensitive macOS/Windows
// volumes "/volumes/photos/2026" IS inside "/Volumes/Photos", and a
// case-sensitive gate here would hide the "Then move to NAS" row for a
// request the server accepts. The server stays authoritative — on a
// genuinely case-sensitive filesystem the worst case is a clear 400 at
// Start Import instead of a silently missing option.
function afterMovePathInsideRoot(destination, root) {
  const d = afterMoveNormPath(destination).toLowerCase();
  const r = afterMoveNormPath(root).toLowerCase();
  return !!r && (d === r || d.indexOf(r + '/') === 0);
}

// A remote target can host the chained move only when the chosen archive
// destination sits inside its local_archive_root. This client-side prefix
// check mirrors the server's realpath/commonpath validation closely enough
// for UI gating; the server remains the authority.
// A root that does not exist on this machine is never eligible, even when
// the destination is lexically inside it: offering the move there would
// silently bless a typo'd root (and contradict the Settings "Test
// connection" warning that says the move won't be offered until the folder
// exists). The hint names the missing root instead.
function afterMoveEligibleTargets() {
  const destination = (document.getElementById('destInput').value || '').trim();
  if (!importIsAbsolutePath(destination)) return [];
  return importRemoteTargets.filter(
    (t) => t.local_archive_root
      && t.local_archive_root_present !== false
      && !t.local_archive_root_volume_offline
      && afterMovePathInsideRoot(destination, t.local_archive_root));
}

// Why no target can host the chained move, in terms of the field the user
// has to fix (issue #1377). The generic "destination is not inside any
// archive root" blamed the destination even when the real problem was a
// typo'd or missing archive root in Settings, which the user could never
// fix by retyping the destination. Three cases, each naming the actual
// configured root so a mismatch is visible at a glance:
//   1. no target has an archive root configured at all;
//   2. the destination is inside a configured root that does not exist on
//      this machine — creating it is the fix;
//   3. otherwise the destination is outside every root. Roots that do not
//      exist are annotated inline (a typo'd root is the usual reason the
//      destination "doesn't match"), but the user is not told to create a
//      folder that would not make any target eligible.
function afterMoveUnavailableReason(destination) {
  const configured = importRemoteTargets.filter((t) => t.local_archive_root);
  if (!configured.length) {
    return 'no remote target has a local archive root configured. ';
  }
  const describe = (t) => t.name + ' (' + t.local_archive_root
    + (t.local_archive_root_volume_offline ? ' — volume not reachable right now'
      : t.local_archive_root_present === false ? ' — does not exist on this machine' : '')
    + ')';
  const offlineHere = configured.filter(
    (t) => t.local_archive_root_volume_offline
      && afterMovePathInsideRoot(destination, t.local_archive_root));
  if (offlineHere.length) {
    return 'the volume holding the local archive root for '
      + offlineHere.map(describe).join(', ') + '. Reconnect it, or change '
      + 'the root under Settings › Remote targets. ';
  }
  const missingHere = configured.filter(
    (t) => t.local_archive_root_present === false
      && afterMovePathInsideRoot(destination, t.local_archive_root));
  if (missingHere.length) {
    return 'the local archive root for ' + missingHere.map(describe).join(', ')
      + '. Create it, or check the path for a typo under '
      + 'Settings › Remote targets. ';
  }
  return 'the destination is outside the local archive root of '
    + configured.map(describe).join(', ') + '. Choose a destination inside '
    + 'it, or change the root under Settings › Remote targets. ';
}

// The target list (and with it local_archive_root / _present) is a
// snapshot taken when the page loaded. The hint below sends the user to
// Settings to fix the root, or to Finder / the picker to create the folder;
// both happen while this page stays open, so the gate must not keep
// judging against stale data until a full reload. Whenever the unavailable
// hint is rendered (and whenever the tab becomes active again), re-fetch
// the targets — throttled so typing in the destination field doesn't
// hammer the server — and re-render if anything the gate reads changed.
let _afterMoveTargetsRefresh = null;
let _afterMoveTargetsCheckedAt = 0;
let _afterMoveTargetsTimer = null;
let _afterMoveTargetsRerun = false;
const AFTER_MOVE_TARGETS_TTL_MS = 5000;
// Whole-entry identity. Start posts only the target id and the server
// resolves the saved configuration, so any field the page previews
// (remote_path, mount_path, host, user, port, ssh_key, bwlimit) must be
// refreshed too, not just the ones the after-move gate reads — otherwise
// the UI would display one destination while the import goes to another.
function remoteTargetKey(t) {
  return JSON.stringify(t, Object.keys(t).sort());
}
function refreshAfterMoveTargets() {
  if (_afterMoveTargetsRefresh) {
    // A trigger arrived mid-fetch; the in-flight result predates it, so
    // run once more when it lands (the throttle will space it out).
    _afterMoveTargetsRerun = true;
    return;
  }
  const wait = AFTER_MOVE_TARGETS_TTL_MS - (Date.now() - _afterMoveTargetsCheckedAt);
  if (wait > 0) {
    // Throttled — but never drop the trigger. The native picker fires a
    // single input event; if the user created and picked the folder within
    // the TTL of the last check, that one event is the only signal we get,
    // so schedule a trailing refresh for when the window expires.
    if (!_afterMoveTargetsTimer) {
      _afterMoveTargetsTimer = setTimeout(() => {
        _afterMoveTargetsTimer = null;
        refreshAfterMoveTargets();
      }, wait);
    }
    return;
  }
  _afterMoveTargetsRefresh = fetchImportRemoteTargets()
    .then((res) => {
      _afterMoveTargetsCheckedAt = Date.now();
      if (remoteTargetsResponseIsStale(res)) return;
      const data = res.data;
      if (!data || !Array.isArray(data.targets)) return;
      // Replace the list rather than merging by id: a legacy entry saved
      // without an id gets a synthesized id here but a generated one once
      // Settings re-saves it (e.g. when the user corrects its archive root,
      // the very fix the hint advertises), so an id-keyed merge would skip
      // exactly the entry that changed. renderImportDestModes() keeps the
      // current dropdown selection when its target survived.
      const listChanged = data.targets.map(remoteTargetKey).join('\n')
        !== importRemoteTargets.map(remoteTargetKey).join('\n');
      const capsChanged = !!data.rsync_available !== importRsyncAvailable
        || !!data.ssh_available !== importSshAvailable;
      // A success after a failed initial load is a recovery: apply it in
      // full (tools, banner) exactly as the Retry link would.
      if (listChanged || capsChanged || remoteTargetsLoadFailed()) {
        applyImportRemoteTargets(res);
        renderRemoteTargetsError(false);
        renderImportDestModes();
      }
    })
    .catch(() => {
      // Failed or timed out: keep the last known list. Stamp the check so
      // a hung server is re-polled at the throttle cadence, not per
      // keystroke.
      _afterMoveTargetsCheckedAt = Date.now();
    })
    .finally(() => {
      _afterMoveTargetsRefresh = null;
      if (_afterMoveTargetsRerun) {
        _afterMoveTargetsRerun = false;
        refreshAfterMoveTargets();
      }
    });
}

let _afterMoveNotice = '';
function localProcessingAvailable() {
  if (selectedImportMode() !== 'copy' || !afterImportChainsProcess()) return false;
  if (importRemoteTargetId()) return true;
  const destination = (document.getElementById('destInput')?.value || '').trim();
  return importRemoteTargets.some((t) => t.mount_path
    && afterMovePathInsideRoot(destination, t.mount_path));
}

function localProcessingRequest() {
  return localProcessingAvailable() && !!document.getElementById('chkLocalProcessing')?.checked;
}

function deferNasTransferRequest() {
  return localProcessingRequest() && !!document.getElementById('chkDeferNasTransfer')?.checked;
}

function updateAfterMoveUI() {
  const localAvailable = localProcessingAvailable();
  document.getElementById('localProcessingRow').style.display = localAvailable ? '' : 'none';
  document.getElementById('deferNasTransferRow').style.display = localProcessingRequest() ? '' : 'none';
  updateDestinationModeHint();
  const row = document.getElementById('afterMoveRow');
  const unavailable = document.getElementById('afterMoveUnavailable');
  const preview = document.getElementById('afterMovePreview');
  const sel = document.getElementById('afterMoveTarget');
  if (!row || !unavailable || !preview || !sel) return;
  const hideAll = () => {
    _afterMoveNotice = '';
    row.style.display = 'none';
    unavailable.style.display = 'none';
    preview.style.display = 'none';
  };
  // The chained move only exists for local-disk archive copies that chain
  // a processing run — everything else hides the row entirely, and a
  // hidden row means startImport() sends after_process_move: null no
  // matter what the (stale) checkbox says.
  if (localAvailable || selectedImportMode() !== 'copy' || importRemoteTargetId()
      || !afterImportChainsProcess()) {
    hideAll();
    return;
  }
  const eligible = afterMoveEligibleTargets();
  if (!eligible.length) {
    hideAll();
    // Only claim "unavailable" once there is a concrete destination to
    // judge — an empty or relative destination isn't decidable yet.
    const destination = (document.getElementById('destInput').value || '').trim();
    if (importIsAbsolutePath(destination)) {
      refreshAfterMoveTargets();
      unavailable.textContent = 'Move to NAS unavailable: ' + afterMoveUnavailableReason(destination);
      const setup = document.createElement('a');
      setup.href = '/settings#nas-setup';
      setup.textContent = 'Set up remote target…';
      unavailable.appendChild(setup);
      unavailable.style.display = '';
    }
    return;
  }
  row.style.display = '';
  const chk = document.getElementById('chkAfterMove');
  // A "was unchecked" note must survive the re-renders that follow the
  // refresh that produced it (renderImportDestModes → onImportDestModeChange
  // → updateAfterMoveUI runs this twice); it clears when the user re-checks
  // the box, i.e. acknowledges it.
  if (_afterMoveNotice && !chk.checked) {
    unavailable.textContent = _afterMoveNotice;
    unavailable.style.display = '';
  } else {
    _afterMoveNotice = '';
    unavailable.style.display = 'none';
  }
  // Repopulate with the currently-eligible targets, preserving the user's
  // pick when it is still eligible; a single eligible target selects
  // itself (a freshly-populated select shows its first option).
  const previous = sel.value;
  while (sel.options.length) sel.remove(0);
  eligible.forEach((t) => {
    const opt = document.createElement('option');
    opt.value = t.id;
    opt.textContent = t.name;
    sel.appendChild(opt);
  });
  if (eligible.some((t) => t.id === previous)) {
    sel.value = previous;
  } else if (previous) {
    // The chosen target is no longer eligible under that id (deleted,
    // re-saved under a generated id, root changed, or the destination
    // moved outside its root). Follow an unambiguous legacy-id migration;
    // otherwise never let the select quietly land on a *different* NAS
    // while the box stays checked — the post-processing move deletes the
    // local copies, so uncheck it and say why.
    // Only a target from the list that was just *replaced* can have
    // migrated to a new id. If the previous pick is still in the current
    // list (it merely became ineligible, e.g. the destination moved out of
    // its root), or no replacement happened at all, there is nothing to
    // migrate and a same-tuple neighbour must not inherit the pick.
    const prevReplaced = _previousRemoteTargets.find((t) => t.id === previous) || null;
    const prevTarget = prevReplaced
      || importRemoteTargets.find((t) => t.id === previous) || null;
    const same = prevReplaced && !importRemoteTargets.some((t) => t.id === previous)
      ? migratedRemoteTarget(prevReplaced, eligible, _previousRemoteTargets)
      : null;
    if (same) {
      sel.value = same.id;
    } else if (chk.checked) {
      chk.checked = false;
      _afterMoveNotice = '"Then move to NAS" was unchecked: '
        + (prevTarget ? prevTarget.name : 'the selected target')
        + ' is no longer available for this destination. Re-check it to '
        + 'pick another target.';
      unavailable.textContent = _afterMoveNotice;
      unavailable.style.display = '';
    }
  }
  if (!chk.checked) {
    preview.style.display = 'none';
    preview.textContent = '';
    return;
  }
  const target = eligible.find((t) => t.id === sel.value) || eligible[0];
  const destination = afterMoveNormPath(
    (document.getElementById('destInput').value || '').trim());
  const root = afterMoveNormPath(target.local_archive_root);
  const rel = destination === root ? '' : destination.slice(root.length + 1);
  const remoteBase =
    afterMoveNormPath(target.remote_path) + (rel ? '/' + rel : '');
  const mountOk =
    !!target.mount_path && importIsAbsolutePath(target.mount_path);
  const mountBase = mountOk
    ? afterMoveNormPath(target.mount_path) + (rel ? '/' + rel : '') : '';
  preview.textContent = '';
  const line = document.createElement('div');
  line.textContent =
    'After processing, each imported folder moves to '
    + remoteBase + '/<imported folder>'
    + (mountOk
        ? ' and the catalog repoints to ' + mountBase + '/<imported folder>.'
        : '.')
    + " Photos leave this computer's archive.";
  preview.appendChild(line);
  if (!importRsyncAvailable) {
    preview.appendChild(remotePreviewWarn(
      'Warning: no GNU rsync found, so the move to NAS would fail. ' + rsyncInstallHint()));
  }
  if (!importSshAvailable) {
    preview.appendChild(remotePreviewWarn(
      'Warning: no OpenSSH client found, so the move to NAS would fail.'));
  }
  if (!target.mount_path) {
    preview.appendChild(remotePreviewWarn(
      'Warning: the move to NAS would fail: this target has no local mount path — add one under Settings > Remote targets.'));
  } else if (!importIsAbsolutePath(target.mount_path)) {
    preview.appendChild(remotePreviewWarn(
      'Warning: the move to NAS would fail: this target\'s local mount path must be absolute — fix it under Settings > Remote targets.'));
  }
  preview.style.display = '';
}

function afterMoveRequest() {
  // Reads the row visibility updateAfterMoveUI maintains — keeping the
  // writer and this reader adjacent so a refactor can't silently split them.
  const row = document.getElementById('afterMoveRow');
  const sel = document.getElementById('afterMoveTarget');
  const chk = document.getElementById('chkAfterMove');
  return (row && chk && sel && chk.checked &&
          row.style.display !== 'none' && sel.value)
    ? {remote_target_id: sel.value} : null;
}

function retireHiddenDefaultOption(sel) {
  const placeholder = document.getElementById('afterImportHiddenDefault');
  if (!placeholder) return;
  placeholder.hidden = true;
  placeholder.disabled = true;
  placeholder.textContent = '';
  delete placeholder.dataset.maskedValue;
  if (sel && sel.value === '__hidden_default__') sel.value = '__none__';
}

function afterImportOptionLabel(processId) {
  const sel = document.getElementById('afterImportSelect');
  const value = processId == null ? '__none__' : String(processId);
  const fallback = processId == null ? 'None — import only' : String(processId);
  if (!sel) return fallback;
  const opt = Array.from(sel.options).find(
    (o) => o.value === value && o.id !== 'afterImportHiddenDefault');
  return opt ? (opt.textContent || '').trim() : fallback;
}

// Keep the After Import display honest about what a Start Import will do.
// (Named for the advanced-mode listener it's still wired to; saved processes
// no longer have an "advanced" tier, so there's nothing to hide/show — the
// only remaining job is the new-workspace default placeholder.)
function updateAdvancedImportOptions() {
  const sel = document.getElementById('afterImportSelect');
  if (!sel) return;
  const placeholder = document.getElementById('afterImportHiddenDefault');
  if (!placeholder) return;

  // Once the user actively picks an option, that pick is authoritative
  // and no placeholder mode should override it.
  if (_afterImportUserTouched) {
    retireHiddenDefaultOption(sel);
    _afterImportHidingDefault = false;
    return;
  }

  const newMode = !!document.getElementById('workspaceNew')?.checked;

  // "New workspace" untouched: startImport() omits after_import so the
  // server resolves it against the freshly-created workspace (which
  // inherits the GLOBAL default, since a brand-new workspace has no
  // overrides yet). The visible dropdown, prefilled from the currently-
  // active workspace's default, would otherwise lie about what the
  // server is going to do — show a placeholder that names the real target.
  if (newMode && _afterImportConfigLoaded) {
    const label = afterImportOptionLabel(_globalDefaultProcessId);
    placeholder.textContent =
      'New workspace default: ' + label + ' (applied to the new workspace)';
    placeholder.dataset.maskedValue =
      _globalDefaultProcessId == null ? '' : String(_globalDefaultProcessId);
    placeholder.hidden = false;
    placeholder.disabled = false;
    _afterImportHidingDefault = true;
    sel.value = '__hidden_default__';
    return;
  }

  // Not new-workspace mode: track the workspace's own default (or the HTML
  // fallback if config didn't load). Re-derive it every call so switching
  // workspaceNew off restores the workspace default rather than sticking on
  // whatever placeholder we were showing.
  const wsDef = _afterImportConfigLoaded
    ? (_workspaceDefaultProcessId == null
        ? '__none__' : String(_workspaceDefaultProcessId))
    : '__none__';
  sel.value = wsDef;
  if (sel.selectedIndex === -1) sel.value = '__none__';
  retireHiddenDefaultOption(sel);
  _afterImportHidingDefault = false;
}

// Combined into one listener per event (rather than registering
// updateAdvancedImportOptions then updateAfterMoveUI separately) so the
// move row always re-evaluates against the already-updated After Import
// selection, without depending on listener registration order.
// The hint sends the user to Settings or Finder and back. Coming back is
// itself the signal to re-check: nothing else on the form changes, so
// without this the last (unchanged) refresh would be the final word until
// a reload. Refresh unconditionally here (throttled), not only while the
// hint is showing: a target that was eligible at load can stop being
// eligible when its root is changed in Settings, and offering the move
// against a stale root would only be caught by the server rejecting Start.
const _afterMoveOnReturn = () => {
  if (document.hidden) return;
  if (importRemoteTargets.length) refreshAfterMoveTargets();
  updateAfterMoveUI();
};

// True once initImportPage() has resolved the workspace's after-import
// default into the select. Until then, the select still shows its HTML
// initial value (__none__), which selectedAfterImport() would surface as
// an explicit null. Posting `after_import: null` tells the import
// endpoint to run import-only and skip the workspace's
// pipeline.default_process_id — the exact race the reviewer flagged.
// Guarding the body key with this flag makes an unresolved default
// fall back to the endpoint's "key omitted → apply workspace default"
// branch, which is what the user actually expects.
let _afterImportConfigLoaded = false;

// True when updateAdvancedImportOptions() has visually reset the select
// to __none__ solely because the workspace's stored default is an
// advanced-only strategy (full / cull_ready) and advanced mode is off.
// In that state the __none__ display is a UI artifact, not a user
// choice, so startImport() must omit the after_import key and let the
// server apply the workspace default rather than posting an explicit
// null (which would run import-only). Cleared as soon as the user
// actively changes the select.
let _afterImportHidingDefault = false;

// True once the user actively picks something in the After Import
// dropdown. Until then the visible selection is whatever
// initImportPage() prefilled from the CURRENTLY-active workspace's
// default — which is the wrong basis for a new-workspace import, where
// the server should apply the new workspace's (or global) default
// instead. startImport() omits the after_import key for new-workspace
// imports that this flag hasn't been set on, so the resolution happens
// server-side after the workspace switch. An explicit user pick
// (including "None — import only") flips this flag and gets forwarded
// verbatim.
let _afterImportUserTouched = false;

// The unscoped global pipeline.default_process_id from /api/config, captured
// during initImportPage. A brand-new workspace has no config_overrides yet,
// so this is the strategy the server will apply when the new-workspace
// import path omits `after_import`. Used to populate the honest "New
// workspace default: X" placeholder so the visible dropdown selection
// isn't misled by whatever was prefilled from the currently-active
// workspace's (possibly-overridden) default.
let _globalDefaultProcessId = null;

// The effective pipeline.default_process_id for the CURRENTLY-active
// workspace (global config merged with the workspace's overrides). Used
// so updateAdvancedImportOptions() can re-derive the visible dropdown
// selection when the user toggles "New workspace" off without touching
// the dropdown — the underlying target reverts to the current workspace's
// default rather than sticking on whatever placeholder was showing.
let _workspaceDefaultProcessId = null;

function bindAfterImportEvents() {
  window.addEventListener('focus', _afterMoveOnReturn);
  window.addEventListener('pageshow', _afterMoveOnReturn);
  document.addEventListener('visibilitychange', _afterMoveOnReturn);
  window.addEventListener('advancedmodechange', () => { updateAdvancedImportOptions(); updateAfterMoveUI(); });
  window.addEventListener('devmodechange', () => { updateAdvancedImportOptions(); updateAfterMoveUI(); });
}
