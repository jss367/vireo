// ---------------------------------------------------------------------
// NAS setup wizard. Five steps: volume -> ssh -> share -> archive ->
// review. Every step pre-fills from discovery and lets the user override;
// the manual card editor above stays the advanced/edit path.
// ---------------------------------------------------------------------

/** @type {SettingsNasWizard|null} */ var _nw = null;   // wizard state; null when closed

/** @template {keyof HTMLElementTagNameMap} K @param {K} tag @param {string=} css @param {string=} text @returns {HTMLElementTagNameMap[K]} */ function _nwEl(tag, css, text) {
  var el = document.createElement(tag);
  if (css) el.style.cssText = css;
  if (text != null) el.textContent = String(text);
  return el;
}

var _nwBtnCss = 'background:var(--bg-tertiary);color:var(--text-secondary);border:1px solid var(--border-secondary);border-radius:4px;padding:5px 11px;font-size:12px;cursor:pointer;';
var _nwInputCss = _rtInputCss + 'font-family:monospace;';
var _nwHintCss = 'font-size:12px;color:var(--text-dim);margin:6px 0;';
var _nwErrCss = 'font-size:12px;color:var(--warning);margin:6px 0;white-space:pre-wrap;';
var _nwOkCss = 'font-size:12px;color:var(--success, #4caf50);margin:6px 0;';

function openNasWizard() {
  _nw = { step: 'volume', mounts: [], mount: null, host: '', user: '',
          port: 22, keyAuthOk: false, pubKeyLine: '', remotePath: '',
          archiveRoot: '', browsePath: '', remoteBrowsePath: '/' };
  /** @type {HTMLElement} */ (document.getElementById('nasWizard')).style.display = 'flex';
  nwRender();
}

function closeNasWizard() {
  _nw = null;
  /** @type {HTMLElement} */ (document.getElementById('nasWizard')).style.display = 'none';
}

var _nwSteps = ['volume', 'ssh', 'share', 'archive', 'review'];
/** @type {Record<string,string>} */ var _nwTitles = {
  volume: 'Step 1 of 5 — Pick your NAS volume',
  ssh: 'Step 2 of 5 — Connect over SSH',
  share: 'Step 3 of 5 — Locate the share on the NAS',
  archive: 'Step 4 of 5 — Choose a local archive folder',
  review: 'Step 5 of 5 — Review and test',
};

function nwBack() {
  if (!_nw) return;
  var i = _nwSteps.indexOf(nasWizardState().step);
  if (i <= 0) { closeNasWizard(); return; }
  nasWizardState().step = _nwSteps[i - 1] || 'volume';
  nwRender();
}

function nwNext() {
  if (!_nw) return;
  var i = _nwSteps.indexOf(nasWizardState().step);
  if (nasWizardState().step === 'review') { nwSave(); return; }
  if (i < _nwSteps.length - 1) {
    nasWizardState().step = _nwSteps[i + 1] || 'review';
    nwRender();
  }
}

/** @param {boolean} enabled @param {string=} label */ function _nwSetNext(enabled, label) {
  var btn = /** @type {HTMLButtonElement} */ (document.getElementById('nwNext'));
  btn.disabled = !enabled;
  btn.style.opacity = enabled ? '1' : '0.5';
  btn.textContent = String(label || (_nw && nasWizardState().step === 'review' ? 'Save' : 'Next'));
}

function nwRender() {
  if (!_nw) return;
  /** @type {HTMLElement} */ (document.getElementById('nwTitle')).textContent = String(_nwTitles[nasWizardState().step]);
  /** @type {HTMLButtonElement} */ (document.getElementById('nwBack')).textContent =
    String(nasWizardState().step === 'volume' ? 'Cancel' : 'Back');
  var body = /** @type {HTMLElement} */ (document.getElementById('nwBody'));
  body.replaceChildren();
  (/** @type {Record<string,(body:HTMLElement)=>unknown>} */ ({ volume: nwVolumeStep, ssh: nwSshStep, share: nwShareStep,
     archive: nwArchiveStep, review: nwReviewStep }))[nasWizardState().step]?.(body);
}

// --- Step 1: volume ----------------------------------------------------

/** @param {HTMLElement} body */ async function nwVolumeStep(body) {
  _nwSetNext(false);
  body.appendChild(_nwEl('div', _nwHintCss, 'Looking for mounted network volumes…'));
  var res;
  try {
    res = await safeFetch('/api/remote-setup/mounts', {}, { toast: false });
  } catch (e) {
    body.replaceChildren(_nwEl('div', _nwErrCss, 'Could not list volumes: ' + (settingsErrorMessage(e) || String(e))));
    return;
  }
  if (!_nw || nasWizardState().step !== 'volume') return;
  nasWizardState().mounts = res.mounts || [];
  body.replaceChildren();
  if (res.unsupported_platform) {
    body.appendChild(_nwEl('div', _nwErrCss,
      'Automatic volume detection is only available on macOS for now. Use "+ Add manually" instead.'));
    return;
  }
  if (!nasWizardState().mounts.length) {
    body.appendChild(_nwEl('div', _nwHintCss,
      'No network volumes are mounted. Connect to your NAS in Finder first (Go → Connect to Server, or ⌘K), then refresh.'));
    var refresh = _nwEl('button', _nwBtnCss, 'Refresh');
    refresh.type = 'button';
    refresh.onclick = function() { nwRender(); };
    body.appendChild(refresh);
    return;
  }
  body.appendChild(_nwEl('div', _nwHintCss, 'Pick the volume that lives on your NAS:'));
  nasWizardState().mounts.forEach(function(m, i) {
    var row = _nwEl('label', 'display:flex;gap:8px;align-items:center;padding:8px;border:1px solid var(--border-secondary);border-radius:6px;margin-bottom:6px;cursor:pointer;');
    var radio = document.createElement('input');
    radio.type = 'radio';
    radio.name = 'nwMount';
    radio.checked = nasWizardState().mount ? nasWizardMount().mount_point === m.mount_point : i === 0;
    radio.onchange = function() { _nwPickMount(m); };
    var info = _nwEl('div', 'display:flex;flex-direction:column;gap:2px;');
    info.appendChild(_nwEl('div', 'font-weight:600;', m.share + ' on ' + (m.display_name || m.host)));
    info.appendChild(_nwEl('div', 'font-size:11px;color:var(--text-dim);font-family:monospace;',
      m.mount_point + '  ·  ' + (m.user ? m.user + '@' : '') + m.host));
    row.appendChild(radio);
    row.appendChild(info);
    body.appendChild(row);
  });
  _nwPickMount(nasWizardState().mount && nasWizardState().mounts.some(function(m) { return m.mount_point === nasWizardMount().mount_point; })
    ? nasWizardState().mount : nasWizardState().mounts[0]);
}

/** @param {SettingsMount|null|undefined} m */ function _nwPickMount(m) { if (!m || !_nw) return;
  nasWizardState().mount = m;
  // Pin SSH ops to the mount's verified network address — a stale or spoofed
  // PTR record could otherwise steer the password prompt at install-key time
  // to a different machine. `display_name` in the picker still surfaces the
  // friendly name; users can override the host in step 2 if they need to.
  nasWizardState().host = m.host;
  nasWizardState().user = m.user || nasWizardState().user;
  _nwSetNext(true);
}

// --- Step 2: ssh ---------------------------------------------------------

function _nwLooksSynology() {
  var probe = ((nasWizardState().mount && nasWizardMount().display_name) || '') + ' ' + nasWizardState().host;
  return /synology|diskstation|dsm/i.test(probe);
}

/** @param {HTMLElement} body */ function nwSshStep(body) {
  _nwSetNext(false);
  var fields = _nwEl('div', 'display:flex;gap:8px;flex-wrap:wrap;margin-bottom:8px;');
  /** @param {string} label @param {string|number} value @param {string|null} width @param {(value:string)=>void} oninput */ var mk = function(label, value, width, oninput) {
    var wrap = _nwEl('div', 'display:flex;flex-direction:column;gap:3px;' + (width ? 'width:' + width + ';' : 'flex:1;'));
    wrap.appendChild(_nwEl('label', 'font-size:11px;color:var(--text-dim);', label));
    var inp = document.createElement('input');
    inp.value = String(value);
    inp.style.cssText = _nwInputCss;
    inp.addEventListener('input', function() { oninput(inp.value); });
    wrap.appendChild(inp);
    return wrap;
  };
  fields.appendChild(mk('User', nasWizardState().user, null, function(v) { nasWizardState().user = v.trim(); }));
  fields.appendChild(mk('Host', nasWizardState().host, null, function(v) { nasWizardState().host = v.trim(); }));
  fields.appendChild(mk('Port', String(nasWizardState().port), '70px', function(v) { nasWizardState().port = parseInt(v, 10) || 22; }));
  body.appendChild(fields);
  var status = _nwEl('div', '');
  body.appendChild(status);
  var recheck = _nwEl('button', _nwBtnCss, 'Check again');
  recheck.type = 'button';
  recheck.onclick = function() { _nwRunSshCheck(status, recheck); };
  body.appendChild(recheck);
  _nwRunSshCheck(status, recheck);
}

/** @param {HTMLElement} status @param {HTMLButtonElement} recheck */ async function _nwRunSshCheck(status, recheck) {
  status.replaceChildren(_nwEl('div', _nwHintCss, 'Checking SSH on ' + nasWizardState().host + '…'));
  var res;
  try {
    res = await safeFetch('/api/remote-setup/ssh-check', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ host: nasWizardState().host, user: nasWizardState().user, port: nasWizardState().port }),
    }, { toast: false });
  } catch (e) {
    status.replaceChildren(_nwEl('div', _nwErrCss, settingsErrorMessage(e) || 'Check failed.'));
    return;
  }
  if (!_nw || nasWizardState().step !== 'ssh') return;
  nasWizardState().pubKeyLine = res.pub_key_line || '';
  status.replaceChildren();
  if (res.ssh_missing) {
    status.appendChild(_nwEl('div', _nwErrCss,
      'No OpenSSH client was found on this computer. Set its path under Settings → Paths, then check again.'));
    return;
  }
  if (!res.port_open) {
    status.appendChild(_nwEl('div', _nwErrCss,
      'SSH is not reachable on ' + nasWizardState().host + ':' + nasWizardState().port + '.\n' +
      (_nwLooksSynology()
        ? 'On a Synology NAS: Control Panel → Terminal & SNMP → Enable SSH service, then check again.'
        : 'Enable the SSH service on your NAS (check its admin console), then check again.')));
    return;
  }
  if (res.key_auth_ok) {
    nasWizardState().keyAuthOk = true;
    status.appendChild(_nwEl('div', _nwOkCss, '✓ This Mac is already authorized on ' + nasWizardState().host + '.'));
    _nwSetNext(true);
    return;
  }
  // Password form. Used once server-side to authorize this Mac's key —
  // never stored, never logged.
  status.appendChild(_nwEl('div', _nwHintCss,
    'Enter the NAS password for "' + nasWizardState().user + '" once. Vireo uses it to authorize this Mac’s key on the NAS, then never needs it again — it is not stored or logged.'));
  var row = _nwEl('div', 'display:flex;gap:8px;align-items:center;margin:6px 0;');
  var pw = document.createElement('input');
  pw.type = 'password';
  pw.placeholder = 'NAS password';
  pw.style.cssText = _nwInputCss + 'flex:1;';
  var submit = _nwEl('button', _nwBtnCss, 'Authorize');
  submit.type = 'button';
  var msg = _nwEl('div', _nwErrCss, '');
  submit.onclick = async function() {
    if (!pw.value) return;
    submit.disabled = true;
    msg.textContent = String('Authorizing…');
    msg.style.cssText = _nwHintCss;
    try {
      var r = await safeFetch('/api/remote-setup/install-key', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ host: nasWizardState().host, user: nasWizardState().user,
                               port: nasWizardState().port, password: pw.value }),
      }, { toast: false });
      if (r.ok && r.key_auth_ok) {
        nasWizardState().keyAuthOk = true;
        status.replaceChildren(_nwEl('div', _nwOkCss,
          '✓ Authorized. ' + (r.fingerprint ? 'Key: ' + r.fingerprint : '')));
        _nwSetNext(true);
        return;
      }
      msg.style.cssText = _nwErrCss;
      msg.textContent = String(/** @type {Record<string,string>} */ ({
        wrong_password: 'That password was not accepted — try again.',
        password_auth_disabled: 'The NAS refuses password logins over SSH. Use the Terminal option below, or enable password authentication on the NAS.',
        host_key: 'The NAS’s host key changed since a previous connection — fix ~/.ssh/known_hosts and retry.',
        timeout: 'The NAS did not respond in time — try again.',
      })[r.error] || ('Setup failed: ' + (r.detail || r.error || 'unknown error')));
      if (r.error === 'wrong_password') pw.value = String('');
    } catch (e) {
      msg.style.cssText = _nwErrCss;
      msg.textContent = String((e && settingsErrorMessage(e)) || 'Setup failed.');
    } finally {
      submit.disabled = false;
    }
  };
  pw.addEventListener('keydown', function(ev) { if (ev.key === 'Enter') submit.click(); });
  row.appendChild(pw);
  row.appendChild(submit);
  status.appendChild(row);
  status.appendChild(msg);

  var details = document.createElement('details');
  details.style.cssText = 'margin-top:10px;font-size:12px;color:var(--text-dim);';
  var summary = document.createElement('summary');
  summary.textContent = String('Prefer to do this yourself in Terminal?');
  summary.style.cursor = 'pointer';
  details.appendChild(summary);
  var cmd = 'ssh' + (nasWizardState().port !== 22 ? ' -p ' + nasWizardState().port : '') + ' ' +
    nasWizardState().user + '@' + nasWizardState().host +
    ' "umask 077; mkdir -p ~/.ssh; touch ~/.ssh/authorized_keys; ' +
    "grep -qxF '" + nasWizardState().pubKeyLine + "' ~/.ssh/authorized_keys || " +
    "echo '" + nasWizardState().pubKeyLine + "' >> ~/.ssh/authorized_keys\"";
  var pre = _nwEl('pre', 'background:var(--bg-input);border:1px solid var(--border-secondary);border-radius:4px;padding:8px;font-size:11px;white-space:pre-wrap;word-break:break-all;user-select:all;', cmd);
  details.appendChild(_nwEl('div', _nwHintCss, 'Run this in Terminal (it asks for the NAS password), then click Verify:'));
  details.appendChild(pre);
  var verify = _nwEl('button', _nwBtnCss, 'Verify');
  verify.type = 'button';
  verify.onclick = function() { _nwRunSshCheck(status, recheck); };
  details.appendChild(verify);
  status.appendChild(details);
}

// --- Step 3: share -------------------------------------------------------

/** @param {HTMLElement} body */ async function nwShareStep(body) {
  _nwSetNext(false);
  body.appendChild(_nwEl('div', _nwHintCss,
    'Verifying where "' + nasWizardMount().share + '" lives on the NAS… (Vireo drops a marker file on the mounted volume and finds it over SSH — proof, not a guess.)'));
  var res;
  try {
    res = await safeFetch('/api/remote-setup/locate-share', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ mount_path: nasWizardMount().mount_point,
                             share: nasWizardMount().share, host: nasWizardState().host,
                             user: nasWizardState().user, port: nasWizardState().port }),
    }, { toast: false });
  } catch (e) {
    if (!_nw || nasWizardState().step !== 'share') return;
    body.replaceChildren(_nwEl('div', _nwErrCss, settingsErrorMessage(e) || 'Verification failed.'));
    _nwShareFallback(body);
    return;
  }
  if (!_nw || nasWizardState().step !== 'share') return;
  body.replaceChildren();
  if (res.remote_path) {
    nasWizardState().remotePath = res.remote_path;
    body.appendChild(_nwEl('div', _nwOkCss,
      '✓ Verified: ' + nasWizardMount().mount_point + ' is ' + res.remote_path + ' on the NAS.'));
    _nwSetNext(true);
    return;
  }
  body.appendChild(_nwEl('div', _nwErrCss,
    'Could not find the share automatically. Browse the NAS filesystem or type the path:'));
  _nwShareFallback(body);
}

/** @param {HTMLElement} body */ function _nwShareFallback(body) {
  var list = _nwEl('div', 'border:1px solid var(--border-secondary);border-radius:6px;max-height:180px;overflow-y:auto;margin:6px 0;');
  var pathLabel = _nwEl('div', 'font-family:monospace;font-size:12px;margin:6px 0;', '');
  var manual = document.createElement('input');
  manual.placeholder = '/volume1/' + nasWizardMount().share;
  manual.style.cssText = _nwInputCss + 'width:100%;margin-top:6px;';
  manual.addEventListener('input', function() {
    nasWizardState().remotePath = manual.value.trim();
    _nwSetNext(!!nasWizardState().remotePath && nasWizardState().remotePath.startsWith('/'));
  });
  /** @param {string} path */ var load = async function(path) {
    nasWizardState().remoteBrowsePath = path;
    pathLabel.textContent = String(path);
    list.replaceChildren(_nwEl('div', _nwHintCss + 'padding:8px;', 'Loading…'));
    var res;
    try {
      res = await safeFetch('/api/remote-setup/list-remote-dirs', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ path: path, host: nasWizardState().host, user: nasWizardState().user, port: nasWizardState().port }),
      }, { toast: false });
    } catch (e) {
      list.replaceChildren(_nwEl('div', _nwErrCss + 'padding:8px;', 'Listing failed.'));
      return;
    }
    list.replaceChildren();
    if (path !== '/') {
      var up = _nwEl('div', 'padding:6px 10px;cursor:pointer;font-family:monospace;', '← up');
      up.onclick = function() { load(path.replace(/\/[^/]+$/, '') || '/'); };
      list.appendChild(up);
    }
    (res.dirs || []).forEach(/** @param {string} name */ function(name) {
      var row = _nwEl('div', 'padding:6px 10px;cursor:pointer;font-family:monospace;border-top:1px solid var(--border-secondary);', name + '/');
      row.onclick = function() {
        var next = (path === '/' ? '' : path) + '/' + name;
        manual.value = String(next);
        nasWizardState().remotePath = next;
        _nwSetNext(true);
        load(next);
      };
      list.appendChild(row);
    });
    if (!(res.dirs || []).length) {
      list.appendChild(_nwEl('div', _nwHintCss + 'padding:8px;', '(no subfolders)'));
    }
  };
  body.appendChild(pathLabel);
  body.appendChild(list);
  body.appendChild(_nwEl('div', _nwHintCss, 'Selected NAS path (click a folder above or type it):'));
  body.appendChild(manual);
  load(nasWizardState().remoteBrowsePath || '/');
}

// --- Step 4: archive root ------------------------------------------------

/** @param {HTMLElement} body */ async function nwArchiveStep(body) {
  _nwSetNext(false);
  body.appendChild(_nwEl('div', _nwHintCss,
    'Pick a folder on this Mac. Photos you import here are processed locally (fast), then moved to the NAS automatically.'));
  var home;
  try {
    home = (await safeFetch('/api/browse', {}, { toast: false })).path;
  } catch (e) { home = ''; }
  if (!_nw || nasWizardState().step !== 'archive') return;
  var suggestion = home ? home + '/Pictures/Vireo Archive' : '';
  var chosen = _nwEl('div', 'font-family:monospace;font-size:12px;margin:6px 0;', '');
  var freeLine = _nwEl('div', _nwHintCss, '');
  var err = _nwEl('div', _nwErrCss, '');
  /** @param {string} p */ var setChosen = async function(p) {
    // Filesystem-aware containment: the same check _coerce_remote_target
    // performs at save time. A purely-lexical prefix compare on the client
    // (which we used to do here) misses symlink aliases — e.g. ~/Archive
    // that points at /Volumes/Photography — and the save-time validator
    // would then silently blank local_archive_root, leaving the wizard's
    // target unable to offer the chained move. Fail here instead so the
    // user gets an actionable error while still in step 4.
    err.textContent = String('');
    _nwSetNext(false);
    try {
      var chk = await safeFetch('/api/remote-setup/check-archive-root', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({path: p, mount_path: nasWizardMount().mount_point}),
      }, { toast: false });
      if (chk && chk.inside_mount) {
        err.textContent = String('That folder resolves inside the NAS mount (symlink or alias). Pick a folder on this Mac instead.');
        return;
      }
    } catch (e) {
      // Endpoint unavailable (e.g. offline test harness) — fall back to a
      // lexical check so this step still gates the obvious case.
      /** @param {string} x */ var norm = function(x) { return String(x || '').replace(/\/+$/, '').toLowerCase(); };
      var inMount = norm(p) === norm(nasWizardMount().mount_point) ||
        norm(p).indexOf(norm(nasWizardMount().mount_point) + '/') === 0;
      if (inMount) {
        err.textContent = String('The archive folder must be on this Mac, not on the NAS volume itself.');
        return;
      }
    }
    err.textContent = String('');
    nasWizardState().archiveRoot = p;
    chosen.textContent = String('Archive folder: ' + p);
    _nwSetNext(true);
    try {
      var df = await safeFetch('/api/remote-setup/disk-free?path=' +
        encodeURIComponent(p), {}, { toast: false });
      freeLine.textContent = String((df.free_bytes / (1024 * 1024 * 1024)).toFixed(0) +
        ' GB free on this volume. Staged photos live here until each move to the NAS completes.');
    } catch (e) { freeLine.textContent = String(''); }
  };
  if (suggestion) {
    var sugRow = _nwEl('div', 'display:flex;gap:8px;align-items:center;margin:6px 0;');
    sugRow.appendChild(_nwEl('span', 'font-family:monospace;font-size:12px;', suggestion));
    var mk = _nwEl('button', _nwBtnCss, 'Create & use');
    mk.type = 'button';
    mk.onclick = async function() {
      try {
        await safeFetch('/api/browse/mkdir', {
          method: 'POST', headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({ path: suggestion }),
        }, { toast: false });
        setChosen(suggestion);
      } catch (e) {
        err.textContent = String((e && settingsErrorMessage(e)) || 'Could not create the folder.');
      }
    };
    sugRow.appendChild(mk);
    body.appendChild(sugRow);
  }
  body.appendChild(_nwEl('div', _nwHintCss, 'Or pick an existing folder:'));
  var list = _nwEl('div', 'border:1px solid var(--border-secondary);border-radius:6px;max-height:160px;overflow-y:auto;margin:6px 0;');
  var pathLabel = _nwEl('div', 'font-family:monospace;font-size:12px;margin:4px 0;', '');
  /** @param {string} path */ var browse = async function(path) {
    nasWizardState().browsePath = path;
    pathLabel.textContent = String(path);
    list.replaceChildren(_nwEl('div', _nwHintCss + 'padding:8px;', 'Loading…'));
    var res;
    try {
      res = await safeFetch('/api/browse?path=' + encodeURIComponent(path), {}, { toast: false });
    } catch (e) {
      list.replaceChildren(_nwEl('div', _nwErrCss + 'padding:8px;', 'Listing failed.'));
      return;
    }
    list.replaceChildren();
    var parent = path.replace(/\/[^/]+$/, '') || '/';
    if (parent !== path) {
      var up = _nwEl('div', 'padding:6px 10px;cursor:pointer;font-family:monospace;', '← up');
      up.onclick = function() { browse(parent); };
      list.appendChild(up);
    }
    (res.dirs || []).forEach(/** @param {{name:string;path:string}} d */ function(d) {
      var row = _nwEl('div', 'padding:6px 10px;cursor:pointer;font-family:monospace;border-top:1px solid var(--border-secondary);', d.name + '/');
      row.onclick = function() { browse(d.path); };
      list.appendChild(row);
    });
    var useBtn = _nwEl('button', _nwBtnCss + 'margin:8px;', 'Use this folder');
    useBtn.type = 'button';
    useBtn.onclick = function() { setChosen(path); };
    list.appendChild(useBtn);
  };
  body.appendChild(pathLabel);
  body.appendChild(list);
  body.appendChild(chosen);
  body.appendChild(freeLine);
  body.appendChild(err);
  browse(nasWizardState().browsePath || home || '/');
  if (nasWizardState().archiveRoot) setChosen(nasWizardState().archiveRoot);
}

// --- Step 5: review + test ------------------------------------------------

function _nwAssembledTarget() {
  return {
    id: _genTargetId(),
    name: (nasWizardState().mount && (nasWizardMount().display_name || nasWizardMount().share)) || nasWizardState().host,
    host: nasWizardState().host, user: nasWizardState().user, port: nasWizardState().port,
    ssh_key: nasWizardState().keyPath || '',   // the wizard-managed key, from ssh-check
    remote_path: nasWizardState().remotePath,
    mount_path: nasWizardState().mount ? nasWizardMount().mount_point : '',
    local_archive_root: nasWizardState().archiveRoot,
    bwlimit_kbps: 0,
  };
}

/** @param {HTMLElement} body */ async function nwReviewStep(body) {
  _nwSetNext(false, 'Save');
  // The server knows the real key path; ask it rather than guessing "~".
  var keyPath = '';
  try {
    var chk = await safeFetch('/api/remote-setup/ssh-check', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ host: nasWizardState().host, user: nasWizardState().user, port: nasWizardState().port }),
    }, { toast: false });
    keyPath = chk.key_path || '';
  } catch (e) { /* key path shown blank; save still works via default */ }
  if (!_nw || nasWizardState().step !== 'review') return;
  nasWizardState().keyPath = keyPath;
  var t = _nwAssembledTarget();
  var rows = [
    ['Name', t.name], ['SSH', t.user + '@' + t.host + (t.port !== 22 ? ':' + t.port : '')],
    ['NAS path', t.remote_path], ['Mounted at', t.mount_path],
    ['Local archive', t.local_archive_root], ['SSH key', t.ssh_key || '(default)'],
  ];
  var table = _nwEl('div', 'display:grid;grid-template-columns:auto 1fr;gap:4px 12px;font-size:12px;margin-bottom:10px;');
  rows.forEach(function(r) {
    table.appendChild(_nwEl('div', 'color:var(--text-dim);', r[0]));
    table.appendChild(_nwEl('div', 'font-family:monospace;word-break:break-all;', r[1]));
  });
  body.appendChild(table);
  var status = _nwEl('div', _nwHintCss, 'Testing the connection…');
  body.appendChild(status);
  try {
    var res = await safeFetch('/api/remote-targets/test', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(t),
    }, { toast: false });
    if (!_nw || nasWizardState().step !== 'review') return;
    status.style.cssText = res.ok ? _nwOkCss : _nwErrCss;
    status.textContent = String((res.ok ? '✓ ' : '⚠ ') + (res.message || ''));
    appendRsyncInstallCommands(status, res.rsync_install_commands);
    if (res.ok) _nwSetNext(true, 'Save');
  } catch (e) {
    if (!_nw || nasWizardState().step !== 'review') return;
    status.style.cssText = _nwErrCss;
    status.textContent = String('⚠ ' + ((e && settingsErrorMessage(e)) || 'Test failed.'));
  }
}

async function nwSave() {
  var t = _nwAssembledTarget();
  _remoteTargetsState.push(t);
  renderRemoteTargets();
  // The debounced saveConfig used by inline handlers only SCHEDULES a POST
  // 500 ms out; if the user reloads or closes the tab in that window the
  // target is lost. This is an explicit Save action, so flush immediately
  // and keep the modal up until the write lands.
  clearTimeout(_saveTimer ?? undefined);
  _nwSetNext(false, 'Saving…');
  /** @type {HTMLButtonElement} */ (document.getElementById('nwBack')).disabled = true;
  try {
    await _saveConfigNow();
  } catch (e) {
    // Roll back the optimistic push so the row disappears when we show the
    // error — the target didn't actually make it to disk.
    var idx = _remoteTargetsState.indexOf(t);
    if (idx >= 0) _remoteTargetsState.splice(idx, 1);
    renderRemoteTargets();
    var body = /** @type {HTMLElement} */ (document.getElementById('nwBody'));
    if (body) body.appendChild(_nwEl('div', _nwErrCss,
      '⚠ Could not save: ' + ((e && settingsErrorMessage(e)) || e)));
    _nwSetNext(true, 'Save');
    /** @type {HTMLButtonElement} */ (document.getElementById('nwBack')).disabled = false;
    return;
  }
  closeNasWizard();
  var listEl = /** @type {HTMLElement} */ (document.getElementById('cfgRemoteTargetsList'));
  if (listEl) listEl.scrollIntoView({ behavior: preferredScrollBehavior(), block: 'center' });
}

/** @returns {SettingsNasWizard} */
function nasWizardState() {
  if (!_nw) throw new Error('The NAS setup dialog is closed');
  return _nw;
}

/** @returns {SettingsMount} */
function nasWizardMount() {
  var mount = nasWizardState().mount;
  if (!mount) throw new Error('Select a NAS volume first');
  return mount;
}
