function darktableInstallPlatform() {
  var asset = window._dtAsset || {};
  if (typeof asset.platform === 'string' && asset.platform) return asset.platform;
  var ua = navigator.userAgent || '';
  if (/Windows/.test(ua)) return 'win32';
  if (/Mac/.test(ua)) return 'darwin';
  if (/Linux/.test(ua) && !/Android/.test(ua)) return 'linux';
  return '';
}

function darktableIsLinux() {
  return darktableInstallPlatform() === 'linux';
}

function darktableIsWindows() {
  return darktableInstallPlatform() === 'win32';
}

async function loadLocationWriteStatus() {
  var el = document.getElementById('locationWriteStatus');
  if (!el) return;
  try {
    var data = await safeFetch('/api/sync/location-writes', {}, { toast: false });
    var photos = data.photos_with_location || 0;
    var queued = data.already_queued || 0;
    var writes = [];
    if (data.location_sync_enabled) writes.push('GPS coordinates');
    if (data.location_keyword_sync_enabled) writes.push('location keywords');
    var parts = [];
    parts.push(photos.toLocaleString() + (photos === 1 ? ' photo in this workspace has' : ' photos in this workspace have') + ' an assigned place.');
    if (queued) {
      parts.push(queued.toLocaleString() + ' of them ' + (queued === 1 ? 'is' : 'are') + ' already waiting in the sync queue.');
    }
    if (writes.length) {
      parts.push('A sync now writes ' + writes.join(' and ') + ' into their sidecars.');
    } else {
      parts.push('Both location writes are turned off, so a sync would only remove location GPS and keywords Vireo wrote earlier.');
    }
    el.textContent = parts.join(' ');
  } catch (err) {
    el.textContent = 'Could not read location counts: ' + (err && err.message || String(err));
  }
}

async function queueLocationWrites() {
  var btn = document.getElementById('locationWriteQueueBtn');
  if (btn) { btn.disabled = true; btn.textContent = 'Queueing...'; }
  try {
    var data = await safeFetch('/api/sync/location-writes', { method: 'POST' });
    if (typeof showToast === 'function') {
      showToast(
        data.queued
          ? 'Queued ' + data.queued.toLocaleString() + ' location change' + (data.queued === 1 ? '' : 's') +
            '. Open the sync panel to review them.'
          : 'Nothing new to queue \u2014 every located photo already has a location change waiting.',
        data.queued ? 'success' : 'info',
      );
    }
    await loadLocationWriteStatus();
    if (typeof checkPendingSync === 'function') checkPendingSync();
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = 'Queue location writes'; }
  }
}

async function loadDarktableStatus() {
  try {
    var data = await safeFetch('/api/darktable/status', {}, { toast: false });
    var el = document.getElementById('darktableStatus');
    var needsGetOption = false;
    if (data.available) {
      el.innerHTML = '<span style="color:var(--accent);font-size:13px;">&#10003; darktable-cli found</span>' +
        '<span style="color:var(--text-dim);font-size:12px;margin-left:8px;">' + escapeHtml(data.bin) + '</span>';
    } else {
      el.innerHTML = '<span style="color:var(--danger);font-size:13px;">&#10007; darktable-cli not found</span>' +
        '<span style="color:var(--text-dim);font-size:12px;margin-left:8px;">Install darktable or set the path below</span>' +
        '<div id="darktableGet" style="margin-top:6px;"></div>' +
        '<div id="darktableProgress" style="display:none;margin-top:8px;">' +
        '  <div style="background:var(--bg-tertiary);border-radius:3px;height:6px;overflow:hidden;">' +
        '    <div id="dtProgressFill" style="background:var(--accent);height:100%;width:0%;"></div>' +
        '  </div>' +
        '  <div id="dtProgressText" style="font-size:12px;color:var(--text-dim);margin-top:4px;"></div>' +
        '</div>';
      // The user's OWN configured path is the highest-priority probe and the
      // one they are most likely asking about. find_darktable falls through
      // silently when darktable_bin points at a path that no longer exists
      // (develop.py), and checked_paths deliberately does not include it — so
      // say it explicitly, or the panel never answers the actual question.
      if (data.configured_bin) {
        el.innerHTML += '<div style="color:var(--danger);font-size:12px;margin-top:6px;">' +
          'Configured path not found: ' + escapeHtml(data.configured_bin) + '</div>';
      }
      // Say where we looked, so a bare ✗ can explain itself.
      // 'Checked:' not 'Checked PATH and:' — element 0 of checked_paths is
      // already "$PATH (darktable-cli)" (Task 2 composes it in), so the older
      // prefix named PATH twice and implied the rest were additional to it.
      // Task 2 also guarantees the list is never empty, so no else branch.
      if (data.checked_paths && data.checked_paths.length) {
        el.innerHTML += '<div style="font-size:11px;color:var(--text-ghost);margin-top:6px;">' +
          'Checked: ' + data.checked_paths.map(escapeHtml).join(', ') + '</div>';
      }
      needsGetOption = true;
    }
    if (data.auto_convert_dng) {
      if (data.dng_available) {
        el.innerHTML += '<div style="color:var(--accent);font-size:13px;margin-top:4px;">&#10003; Adobe DNG Converter found' +
          '<span style="color:var(--text-dim);font-size:12px;margin-left:8px;">' + escapeHtml(data.dng_bin) + '</span></div>';
      } else {
        el.innerHTML += '<div style="color:var(--danger);font-size:13px;margin-top:4px;">&#10007; Adobe DNG Converter not found' +
          '<span style="color:var(--text-dim);font-size:12px;margin-left:8px;">Install it or set the path below &mdash; ' +
          '<a href="https://helpx.adobe.com/camera-raw/digital-negative.html" target="_blank" rel="noopener" ' +
          'style="color:var(--accent);">Get it from Adobe &#8599;</a></span></div>';
      }
    }
    // MUST run after the DNG block. That block does `el.innerHTML +=`, which
    // re-parses the subtree and detaches any node captured earlier — so
    // rendering the button before it would write into a dead node and the
    // button would never appear. darktable_auto_convert_dng defaults to true
    // (config.py:66), so this is the default path, not an edge case.
    if (needsGetOption) await renderDarktableGetOption();
  } catch(e) {}
}

// The button must say what it actually does. On macOS/Windows we hand off to
// the OS installer and the user finishes the job, so it says "Download
// installer" — never "Install darktable".
async function renderDarktableGetOption() {
  var host = document.getElementById('darktableGet');
  if (!host) return;
  var info;
  try {
    info = await safeFetch('/api/darktable/install/available', {}, { toast: false });
  } catch(e) {
    info = { available: false, reason: 'Could not check for a darktable release.' };
  }
  // Publish _dtAsset before any darktableIsLinux()/darktableIsWindows() call
  // so those helpers see the server-derived platform for the Flask host
  // rather than falling back to navigator.userAgent.
  window._dtAsset = info;

  if (!info.available) {
    // Never a dead button: a plain link, plus the reason verbatim. "GitHub
    // was unreachable" and "no build exists for your platform" are different
    // facts the user acts on differently, so do not collapse them.
    host.innerHTML = '<a href="https://www.darktable.org/install/" target="_blank" rel="noopener" ' +
      'style="color:var(--accent);font-size:13px;">Get darktable &#8599;</a>' +
      '<span style="color:var(--text-ghost);font-size:11px;margin-left:8px;">' +
      escapeHtml(info.reason || '') + '</span>';
    return;
  }

  var label = darktableIsLinux() ? 'Download and set up' : 'Download installer';
  host.innerHTML = '<button class="btn" onclick="downloadDarktable()">' + label + '</button>' +
    '<span style="color:var(--text-dim);font-size:12px;margin-left:8px;">' +
    'darktable ' + escapeHtml(info.version) + ' &mdash; ' + escapeHtml(info.name) + ', ' +
    Math.round(info.size / 1048576) + ' MB, from github.com/darktable-org</span>';
}

async function downloadDarktable() {
  var a = window._dtAsset || {};
  var isLinux = darktableIsLinux();
  var isWindows = darktableIsWindows();
  var what = isLinux
    ? 'Vireo will download it and set the darktable-cli path for you.'
    : 'Vireo will download the installer and open it. You finish the install.';
  // Warn about SmartScreen BEFORE the download starts, not after: the server
  // calls os.startfile() from hand_off() before the SSE 'complete' event
  // fires, so an onComplete-only warning arrives after the unknown-publisher
  // prompt has already surfaced — defeating the warning at the exact moment
  // the user has to decide whether to proceed. Same idea for Gatekeeper on
  // macOS: naming the expected dialog turns "is this malware?" into
  // "this is the OS check I was warned about".
  var osWarning = '';
  if (isWindows) {
    osWarning = '\n\nWindows may show a SmartScreen "unknown publisher" ' +
                'warning after the download finishes — darktable does not ' +
                'sign its installer. Click "More info" then "Run anyway".';
  } else if (!isLinux) {
    osWarning = '\n\nmacOS may show a Gatekeeper warning — darktable does ' +
                'not notarize its macOS builds. Drag darktable to the ' +
                'Applications folder from the DMG the installer opens.';
  }
  // Name the exact artifact before any bytes move: version, filename, size,
  // source host.
  if (!confirm('Download darktable ' + a.version + '?\n\n' +
               a.name + ' (' + Math.round(a.size / 1048576) + ' MB)\n' +
               'From: github.com/darktable-org/darktable\n\n' + what +
               osWarning)) return;

  // Hiding the button is a UX guard against double-clicks; the server also has
  // a singleton guard (a second POST joins the running job instead of starting
  // another worker on the same .partial) so a rapid double click still lands
  // on one download rather than an error.
  document.getElementById('darktableGet').style.display = 'none';
  var wrap = document.getElementById('darktableProgress');
  var fill = document.getElementById('dtProgressFill');
  var text = document.getElementById('dtProgressText');
  wrap.style.display = 'block';
  text.textContent = 'Starting...';

  // The route 400s on insufficient disk space, an unusable download directory,
  // or a release it can no longer resolve. Without this guard the panel sits on
  // "Starting..." forever with the button hidden. safeFetch throws an Error
  // carrying the route's specific message — render it inline, not just in the
  // toast that scrolls away.
  //
  // expected_* pin the download to the exact artifact this dialog confirmed.
  // /install/available caches for 10 minutes; a new release published in that
  // window would otherwise mean the server downloads a different artifact from
  // the one just OK'd — same button click, different bytes.  The server
  // returns code=darktable_asset_changed when the fresh resolution disagrees
  // so the panel can re-check and re-prompt with the new identity.
  var resp;
  try {
    resp = await safeFetch('/api/jobs/download-darktable', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        expected_version: a.version,
        expected_name: a.name,
        expected_digest: a.digest || null,
        // Size is the fallback identity for a digestless release: if GitHub
        // deletes and re-uploads the asset under the same tag+filename
        // during the availability cache's TTL, name+version still match but
        // the bytes differ. Without this the server would accept the swap
        // for any release that publishes no digest.
        expected_size: (typeof a.size === 'number') ? a.size : null,
      }),
    });
  } catch(e) {
    fill.style.width = '0%';
    if (e && e.code === 'darktable_asset_changed') {
      // Do not just show a red error — the user's intent was still to
      // download darktable, and the fresh version is what they should decide
      // on now. Re-render the panel so it fetches the new availability info,
      // then explain in-panel why they were bounced back.
      text.innerHTML = escapeHtml(e.message || 'The darktable release changed since this dialog opened.') +
        '<br><button class="btn" style="margin-top:6px;" onclick="loadDarktableStatus()">Re-check</button>';
      return;
    }
    text.innerHTML = '<span style="color:var(--danger);">' +
      escapeHtml(e.message || 'Could not start the download.') + '</span>';
    document.getElementById('darktableGet').style.display = '';
    return;
  }

  safeEventSource('/api/jobs/' + resp.job_id + '/stream', {
    // NOTE from Task 4: p.current can move BACKWARDS. When a server ignores
    // Range, the retry truncates the .partial and restarts from 0, so the bar
    // must tolerate a decreasing current rather than assuming monotonicity.
    // That is honest — the bytes really were discarded — so do not clamp it.
    onProgress: function(p) {
      if (p.total) {
        fill.style.width = Math.round((p.current / p.total) * 100) + '%';
        text.textContent = p.phase + ': ' +
          Math.round(p.current / 1048576) + ' of ' + Math.round(p.total / 1048576) + ' MB';
      } else {
        text.textContent = p.phase + (p.current_file ? ': ' + p.current_file : '');
      }
    },
    onComplete: function(r) {
      // Verified against jobs.py: the `complete` SSE event carries the job
      // ENVELOPE — {job_id, job_type, status, phase, result, duration, errors,
      // failure} — so the job's return value is r.result, not r.
      //
      // Job FAILURES arrive here with status !== 'completed' — not via
      // onError, which only fires on EventSource connection loss and is
      // called with no arguments. A digest mismatch lands here; rendering it
      // as a success with an empty message would be exactly the black box
      // CORE_PHILOSOPHY.md forbids. Same shape downloadModel() reads below.
      if (r && r.status === 'cancelled') {
        // jobs.py sets status 'cancelled' with empty errors and no failure,
        // so this must be handled before the failure branch or a
        // user-initiated cancel would read as "Download failed".
        // A cancelled download keeps its .partial and a re-run RESUMES it
        // (Task 4). Do not imply the bytes were thrown away.
        fill.style.width = '0%';
        text.innerHTML = 'Download cancelled &mdash; partial progress kept, retrying will resume.' +
          '<br><button class="btn" style="margin-top:6px;" onclick="loadDarktableStatus()">Try again</button>';
        return;
      }
      if (!r || r.status !== 'completed') {
        var why = (r && r.failure && r.failure.message) ||
                  ((r && r.errors) || []).join(', ') || 'Download failed';
        fill.style.width = '0%';
        text.innerHTML = '<span style="color:var(--danger);">' + escapeHtml(why) + '</span>' +
          '<br><button class="btn" style="margin-top:6px;" onclick="loadDarktableStatus()">Try again</button>';
        return;
      }
      fill.style.width = '100%';
      var res = r.result || {};
      var lines = [];
      // verify_digest's ok=True does NOT always mean "verified" — the
      // no-digest-published case also returns True and the string says so.
      // Pass the string through; never synthesize "Verified ✓" from a boolean.
      if (res.verified) lines.push(res.verified);
      if (res.config_written) {
        // No silent config mutation: say it, and make the field show it.
        lines.push('Installed to ' + res.bin_path + ' and set the darktable-cli path in Settings.');
      } else if (res.downloaded_to) {
        lines.push('Downloaded to ' + res.downloaded_to + ' — opening the installer. ' +
                   (darktableIsWindows()
                     ? 'Windows may warn about an unknown publisher: darktable does not sign ' +
                       'its installer. Click through the installer, then click Re-check.'
                     : 'Drag darktable to Applications, then click Re-check.'));
      }
      // Only warn about Gatekeeper if the file really is quarantined.
      if (res.quarantined) {
        lines.push('macOS quarantined this download. darktable does not notarize its ' +
                   'macOS builds, so you may see "damaged". To clear it, run: ' +
                   'xattr -d com.apple.quarantine ' + res.downloaded_to);
      }
      if (res.config_written && res.bin_path) {
        var binInput = document.getElementById('cfgDarktableBin');
        if (binInput) binInput.value = res.bin_path;
      }
      text.innerHTML = lines.map(escapeHtml).join('<br>') +
        '<br><button class="btn" style="margin-top:6px;" onclick="loadDarktableStatus()">Re-check</button>';
    },
    // safeEventSource calls onError with NO arguments (_navbar.html:10242),
    // and only for connection loss. Do not try to read an error off it.
    onError: function() {
      text.innerHTML = '<span style="color:var(--danger);">Lost connection to the ' +
        'download job. It may still be running.</span>' +
        '<br><button class="btn" style="margin-top:6px;" onclick="loadDarktableStatus()">Re-check</button>';
    }
  });
}

async function loadSystemInfo() {
  try {
    var d = await safeFetch('/api/system/info', {}, { toast: false });
    document.getElementById('deviceName').textContent = d.device;
    document.getElementById('deviceDetail').textContent = d.device_detail;
    document.getElementById('onnxrtVersion').textContent = d.onnxruntime_version || 'Not installed';
    document.getElementById('onnxrtProviders').textContent = (d.onnxruntime_providers || []).join(', ');
    var mdStatus = document.getElementById('megadetectorStatus');
    var mdDetail = document.getElementById('megadetectorDetail');
    if (d.megadetector === 'installed') {
      mdStatus.textContent = 'Installed';
      mdStatus.style.color = 'var(--accent)';
      mdDetail.textContent = d.megadetector_detail;
    } else if (d.megadetector === 'weights_missing') {
      mdStatus.textContent = 'Not ready';
      mdStatus.style.color = 'var(--warning)';
      mdDetail.textContent = d.megadetector_detail;
    } else if (d.megadetector === 'unavailable' || d.megadetector === 'not installed') {
      mdStatus.textContent = 'Not installed';
      mdStatus.style.color = 'var(--warning)';
      mdDetail.textContent = 'Subject detection disabled — ' + d.megadetector_detail;
    } else {
      mdStatus.textContent = 'Error';
      mdStatus.style.color = 'var(--danger)';
      mdDetail.textContent = d.megadetector_detail;
    }
    renderPlatformReadiness(d.platform_support || null);
  } catch(e) {}
  loadExiftoolStatus();
  loadPipelineModels();
}

function renderPlatformReadiness(support) {
  if (!support) return;
  var row = document.getElementById('windowsSupportRow');
  if (support.platform === 'win32') {
    row.style.display = '';
    var status = document.getElementById('windowsSupportStatus');
    var detail = document.getElementById('windowsSupportDetail');
    var supported = support.support_tier === 'supported';
    status.textContent = supported ? 'Supported' : 'Unsupported Windows version';
    status.style.color = supported ? 'var(--accent)' : 'var(--danger)';
    var bits = ['Windows ' + (support.windows_release || '11'), support.architecture || 'x64', 'CPU inference supported'];
    if (support.webview2_version) bits.push('WebView2 ' + support.webview2_version);
    if (support.long_paths && !support.long_paths.enabled) {
      bits.push('Long paths need Windows policy');
      status.textContent = 'Action needed';
      status.style.color = 'var(--danger)';
    }
    detail.textContent = bits.join(' · ');
  }

  var deps = support.dependencies || {};
  var names = {
    exiftool: 'ExifTool', darktable: 'Darktable', dng_converter: 'Adobe DNG Converter',
    lightroom: 'Lightroom Classic', openssh: 'OpenSSH Client', rsync: 'GNU rsync',
    remote_transfer: 'Remote transfers'
  };
  var keys = Object.keys(names).filter(function(key) { return deps[key]; });
  if (!keys.length) return;
  document.getElementById('dependencyReadiness').style.display = '';
  document.getElementById('dependencyReadinessRows').innerHTML = keys.map(function(key) {
    var dep = deps[key];
    var ready = dep.state === 'ready';
    var color = ready ? 'var(--accent)' : (dep.required ? 'var(--danger)' : 'var(--warning)');
    var label = ready ? 'Ready' : (dep.state === 'misconfigured' ? 'Needs repair' : 'Unavailable');
    var detail = ready ? (dep.path || dep.hint || '') : (dep.hint || dep.path || '');
    return '<div class="setting-row"><div class="setting-label">' + escapeHtml(names[key]) +
      '<small data-dependency="' + key + '">' + escapeHtml(detail) + '</small></div><span class="setting-value" style="color:' +
      color + ';">' + label + '</span></div>';
  }).join('');
  keys.forEach(function(key) {
    if (deps[key].state !== 'ready') {
      appendRsyncInstallCommands(
        document.querySelector('[data-dependency="' + key + '"]'), deps[key].install_commands);
    }
  });
}

// exiftool is the metadata backbone for every scan; a missing binary
// degrades scans silently, so report its presence next to the other
// external dependencies.
async function loadExiftoolStatus() {
  var statusEl = document.getElementById('exiftoolStatus');
  var detailEl = document.getElementById('exiftoolDetail');
  if (!statusEl) return;
  try {
    var d = await safeFetch('/api/exiftool/status', {}, { toast: false });
    if (d && d.available) {
      statusEl.textContent = d.version ? 'Installed (v' + d.version + ')' : 'Installed';
      statusEl.style.color = 'var(--accent)';
      detailEl.textContent = d.path || 'Reads capture date, GPS, and camera info during scans.';
    } else {
      /* A populated path with available=false means the binary resolved
         on PATH but the -ver probe failed — a broken install, not a
         missing one. Surface that distinction so the user knows whether
         to install or to repair. */
      var broken = !!(d && d.path);
      statusEl.textContent = broken ? 'Installed but broken' : 'Not installed';
      statusEl.style.color = 'var(--danger)';
      detailEl.textContent = 'Scans won’t record dates, GPS, or camera info — ' +
        ((d && d.hint) ||
         (broken ? 'reinstall ExifTool' : 'install ExifTool')) +
        ', then restart Vireo.';
    }
  } catch(e) {}
}
