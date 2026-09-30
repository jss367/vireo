// ---------- Remote targets (SSH) list ----------
// Source-of-truth for the remote-target editor. Populated by loadConfig()
// and read back on save via collectRemoteTargets().
var _remoteTargetsState = [];

function _genTargetId() {
  if (window.crypto && crypto.randomUUID) return crypto.randomUUID();
  return 'rt-' + Date.now().toString(36) + '-' + Math.floor(Math.random() * 1e9).toString(36);
}

var _rtInputCss = 'background:var(--bg-input);color:var(--text-primary);border:1px solid var(--border-secondary);border-radius:4px;padding:6px 10px;font-size:12px;';

var _rtFolderInput = null;
var _rtFolderBrowser = null;

function setupSettingsFolderBrowser() {
  _rtFolderBrowser = new VireoFolderBrowser({
    overlayId: 'folderBrowser',
    modes: {
      local: {
        title: function() { return 'Choose ' + _rtFolderInput.getAttribute('aria-label'); },
        startPath: function() { return _rtFolderInput.value.trim(); },
        onSelect: function(path) { _rtSelectFolder(_rtFolderInput, path); },
      },
    },
  });
}

function _rtSelectFolder(input, path) {
  // A target may have been removed while a native dialog was open.
  if (!input || !input.isConnected || !path) return;
  input.value = path;
  input.dispatchEvent(new Event('input', { bubbles: true }));
}

async function _rtBrowseFolder(input, button) {
  button.disabled = true;
  try {
    if (typeof isTauri === 'function' && isTauri()) {
      var path = await pickDirectory('Choose ' + input.getAttribute('aria-label'), {
        defaultPath: input.value.trim() || undefined,
      });
      _rtSelectFolder(input, Array.isArray(path) ? path[0] : path);
      return;
    }
    _rtFolderInput = input;
    _rtFolderBrowser.open('local');
  } catch (e) {
    showToast('Could not open folder picker: ' + e.message, 'error');
  } finally {
    button.disabled = false;
  }
}

function _rtField(label, value, placeholder, onInput, opts) {
  opts = opts || {};
  var wrap = document.createElement('div');
  wrap.style.cssText = 'display:flex;flex-direction:column;gap:3px;min-width:0;'
    + (opts.flex ? ('flex:' + opts.flex + ';') : '') + (opts.width ? ('width:' + opts.width + ';') : '');
  var lab = document.createElement('label');
  lab.textContent = label;
  lab.style.cssText = 'font-size:11px;color:var(--text-dim);';
  var inp = document.createElement('input');
  inp.type = opts.type || 'text';
  inp.value = (value == null ? '' : value);
  inp.placeholder = placeholder || '';
  inp.setAttribute('aria-label', label);
  inp.title = inp.value;
  inp.style.cssText = _rtInputCss + (opts.mono ? 'font-family:monospace;' : '');
  inp.addEventListener('input', function() { inp.title = inp.value; onInput(inp.value); });
  wrap.appendChild(lab);
  if (opts.folder) {
    wrap.style.flexBasis = '100%';
    var row = document.createElement('div');
    row.style.cssText = 'display:flex;gap:6px;';
    inp.style.flex = '1';
    inp.style.minWidth = '0';
    var browse = document.createElement('button');
    browse.type = 'button';
    browse.className = 'btn-sm';
    browse.textContent = 'Browse…';
    browse.setAttribute('aria-label', 'Browse for ' + label);
    browse.onclick = function() { _rtBrowseFolder(inp, browse); };
    row.append(inp, browse);
    wrap.appendChild(row);
  } else {
    wrap.appendChild(inp);
  }
  return wrap;
}

function renderRemoteTargets() {
  var container = document.getElementById('cfgRemoteTargetsList');
  if (!container) return;
  container.replaceChildren();
  if (_remoteTargetsState.length === 0) {
    var empty = document.createElement('div');
    empty.style.cssText = 'color:var(--text-dim);font-size:12px;font-style:italic;';
    empty.textContent = 'No remote targets configured.';
    container.appendChild(empty);
    return;
  }
  _remoteTargetsState.forEach(function(t, i) {
    var card = document.createElement('div');
    card.style.cssText = 'border:1px solid var(--border-secondary);border-radius:6px;padding:10px;display:flex;flex-direction:column;gap:8px;background:var(--bg-secondary);';

    var save = function(field) {
      return function(v) { t[field] = v; saveConfig(); };
    };

    var r1 = document.createElement('div');
    r1.style.cssText = 'display:flex;gap:8px;flex-wrap:wrap;';
    r1.appendChild(_rtField('Name', t.name, 'My NAS', save('name'), {flex: '2'}));
    r1.appendChild(_rtField('User', t.user, 'admin', save('user'), {flex: '1'}));
    r1.appendChild(_rtField('Host', t.host, 'synology-nas', save('host'), {flex: '2', mono: true}));
    r1.appendChild(_rtField('Port', t.port, '22', save('port'), {width: '70px'}));
    card.appendChild(r1);

    var r2 = document.createElement('div');
    r2.style.cssText = 'display:flex;gap:8px;flex-wrap:wrap;';
    r2.appendChild(_rtField('Remote path (NAS side)', t.remote_path, '/volume1/Photography', save('remote_path'), {flex: '1', mono: true}));
    r2.appendChild(_rtField('Local mount path', t.mount_path, '/Volumes/Photography', save('mount_path'), {flex: '1', mono: true, folder: true}));
    r2.appendChild(_rtField('Local archive root (chained moves)', t.local_archive_root,
      '/Users/you/Photos', save('local_archive_root'), {flex: '1', mono: true, folder: true}));
    card.appendChild(r2);

    var r3 = document.createElement('div');
    r3.style.cssText = 'display:flex;gap:8px;flex-wrap:wrap;';
    r3.appendChild(_rtField('SSH key (optional)', t.ssh_key, 'default key if blank', save('ssh_key'), {flex: '2', mono: true}));
    r3.appendChild(_rtField('Bandwidth limit KB/s (0 = none)', t.bwlimit_kbps, '0', save('bwlimit_kbps'), {width: '160px'}));
    card.appendChild(r3);

    var r4 = document.createElement('div');
    r4.style.cssText = 'display:flex;align-items:center;gap:10px;';
    var testBtn = document.createElement('button');
    testBtn.type = 'button';
    testBtn.textContent = 'Test connection';
    testBtn.style.cssText = 'background:var(--bg-tertiary);color:var(--text-secondary);border:1px solid var(--border-secondary);border-radius:4px;padding:6px 12px;font-size:12px;cursor:pointer;';
    var statusEl = document.createElement('span');
    statusEl.style.cssText = 'font-size:12px;color:var(--text-dim);flex:1;';
    testBtn.addEventListener('click', function() { testRemoteTarget(i, testBtn, statusEl); });

    var removeBtn = document.createElement('button');
    removeBtn.type = 'button';
    removeBtn.textContent = 'Remove';
    removeBtn.style.cssText = 'background:var(--bg-tertiary);color:var(--text-dim);border:1px solid var(--border-secondary);border-radius:4px;padding:6px 12px;font-size:12px;cursor:pointer;';
    removeBtn.addEventListener('click', function() {
      _remoteTargetsState.splice(i, 1);
      renderRemoteTargets();
      saveConfig();
    });

    r4.appendChild(testBtn);
    r4.appendChild(statusEl);
    r4.appendChild(removeBtn);
    card.appendChild(r4);
    container.appendChild(card);
  });
}

function addRemoteTarget() {
  _remoteTargetsState.push({
    id: _genTargetId(), name: '', host: '', user: '', port: 22,
    ssh_key: '', remote_path: '', mount_path: '', bwlimit_kbps: 0,
    local_archive_root: '',
  });
  renderRemoteTargets();
  // Defer save until fields are filled — _coerce_remote_target drops empty
  // entries server-side anyway, so an empty save would be a no-op write.
}

function collectRemoteTargets() {
  // Mirror config._coerce_remote_target: drop entries missing host/user/
  // remote_path; coerce numeric fields. Keep ids stable across edits.
  var asStr = function(v) { return typeof v === 'string' ? v.trim() : (v == null ? '' : String(v).trim()); };
  var asInt = function(v, d) { var n = parseInt(v, 10); return isNaN(n) ? d : n; };
  return _remoteTargetsState
    .map(function(t) {
      return {
        id: t.id || _genTargetId(),
        name: asStr(t.name), host: asStr(t.host), user: asStr(t.user),
        port: asInt(t.port, 22), ssh_key: asStr(t.ssh_key),
        remote_path: asStr(t.remote_path), mount_path: asStr(t.mount_path),
        local_archive_root: asStr(t.local_archive_root),
        bwlimit_kbps: Math.max(0, asInt(t.bwlimit_kbps, 0)),
      };
    })
    .filter(function(t) { return t.host && t.user && t.remote_path; });
}

async function testRemoteTarget(i, btn, statusEl) {
  var t = _remoteTargetsState[i];
  btn.disabled = true;
  var orig = btn.textContent;
  btn.textContent = 'Testing…';
  statusEl.textContent = 'Connecting…';
  statusEl.style.color = 'var(--text-dim)';
  try {
    var res = await safeFetch('/api/remote-targets/test', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(t),
    }, { toast: false });
    // A missing local archive root is not a connection failure, but a bare
    // green "Connection OK" would hide the one misconfiguration the Import
    // page can't name later (issue #1377): render it as a warning and offer
    // the one-line fix inline, since the folder is the user's own staging
    // area and creating it is always safe.
    var archiveRootInvalid = !!(res.ok && res.archive_root_invalid);
    var archiveRootOffline = !!(res.ok && res.archive_root_volume_offline);
    var archiveRootMissing = !!(res.ok && !archiveRootInvalid && !archiveRootOffline
      && res.archive_root && res.archive_root_present === false);
    var archiveRootProblem = archiveRootInvalid || archiveRootOffline || archiveRootMissing;
    statusEl.textContent = (res.ok && !archiveRootProblem ? '✓ ' : '⚠ ') + (res.message || '')
      + (res.mount_path && !res.mount_present
          ? ' (mount path not currently present — fine if the NAS just isn’t mounted right now.)' : '');
    statusEl.style.color = res.ok && !archiveRootProblem ? 'var(--success, #4caf50)' : 'var(--warning)';
    appendRsyncInstallCommands(statusEl, res.rsync_install_commands);
    // Only a missing-but-valid root can be fixed by creating it; an invalid
    // one needs the path changed, and an unreachable volume needs
    // reconnecting (creating a folder on it would hang or fail).
    if (archiveRootMissing) {
      statusEl.appendChild(document.createTextNode(' '));
      var mk = document.createElement('button');
      mk.type = 'button';
      mk.textContent = 'Create folder';
      mk.style.cssText = 'background:var(--bg-tertiary);color:var(--text-secondary);border:1px solid var(--border-secondary);border-radius:4px;padding:2px 8px;font-size:11px;cursor:pointer;';
      mk.onclick = async function() {
        mk.disabled = true;
        try {
          await safeFetch('/api/browse/mkdir', {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ path: res.archive_root }),
          }, { toast: false });
          // Re-run the test so the status reflects the folder now existing.
          await testRemoteTarget(i, btn, statusEl);
        } catch (e) {
          statusEl.textContent = '⚠ Could not create ' + res.archive_root + ': '
            + (e && e.message ? e.message : 'unknown error');
          statusEl.style.color = 'var(--warning)';
        }
      };
      statusEl.appendChild(mk);
    }
  } catch (e) {
    statusEl.textContent = '⚠ ' + (e && e.message ? e.message : 'Test failed.');
    statusEl.style.color = 'var(--warning)';
  } finally {
    btn.disabled = false;
    btn.textContent = orig;
  }
}
