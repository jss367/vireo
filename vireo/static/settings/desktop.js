/* ---------- Version ---------- */
async function loadVersion() {
  try {
    var d = await safeFetch('/api/version', {}, { toast: false });
    var cv = document.getElementById('currentVersion');
    if (cv) cv.textContent = d.version;
  } catch(e) {}
}

/* ---------- Native file picker ---------- */
function setupSettingsNativeFilePickers() {
  if (typeof isTauri === 'function' && isTauri()) {
    document.querySelectorAll('.tauri-only').forEach(function(el) {
      el.style.display = '';
    });
  }
}

async function browseForDarktable() {
  var path = await pickFile({ title: 'Select darktable-cli binary' });
  if (path) {
    document.getElementById('cfgDarktableBin').value = path;
    saveConfig();
  }
}

async function browseForDngConverter() {
  var path = await pickFile({ title: 'Select Adobe DNG Converter binary' });
  if (path) {
    document.getElementById('cfgDngConverterBin').value = path;
    saveConfig();
    loadDarktableStatus();
  }
}

async function browseForOutputDir() {
  var path = await pickDirectory('Select output directory');
  if (path) {
    document.getElementById('cfgDarktableOutputDir').value = path;
    saveConfig();
  }
}

async function browseForWeights() {
  var path = await pickFile({ title: 'Select model weights file' });
  if (path) {
    document.getElementById('customModelPath').value = path;
  }
}

/* ---------- Auto-Update (Tauri only) ---------- */
/* Updater disabled — no update commands registered. Section stays hidden. */

async function doCheckForUpdate() {
  var btn = document.getElementById('checkUpdateBtn');
  var status = document.getElementById('updateStatus');
  var panel = document.getElementById('updateAvailable');
  btn.disabled = true;
  status.textContent = 'Checking...';
  panel.style.display = 'none';

  var result = await checkForAppUpdate();
  btn.disabled = false;
  if (!result) {
    status.textContent = 'Could not reach update server.';
    return;
  }
  if (!result.available) {
    status.textContent = 'You are on the latest version.';
    return;
  }
  status.textContent = '';
  document.getElementById('updateVersion').textContent = result.version || '?';
  document.getElementById('updateNotes').textContent = result.notes || '';
  panel.style.display = '';
}

async function doInstallUpdate() {
  var btn = document.getElementById('installUpdateBtn');
  var status = document.getElementById('installStatus');
  btn.disabled = true;
  status.textContent = 'Downloading and installing...';

  var ok = await downloadAndInstallUpdate();
  if (ok) {
    status.textContent = 'Installed! Restarting...';
    setTimeout(function() { relaunchApp(); }, 1000);
  } else {
    btn.disabled = false;
    status.textContent = 'Install failed. Check logs for details.';
  }
}
