/* ---------- Version ---------- */
async function loadVersion() {
  try {
    var d = await safeFetch('/api/version', {}, { toast: false });
    var cv = /** @type {HTMLElement} */ (document.getElementById('currentVersion'));
    if (cv) cv.textContent = String(d.version);
  } catch(e) {}
}

/* ---------- Native file picker ---------- */
function setupSettingsNativeFilePickers() {
  if (typeof isTauri === 'function' && isTauri()) {
    document.querySelectorAll(/** @type {'div'} */ ('.tauri-only')).forEach(function(el) {
      el.style.display = '';
    });
  }
}

async function browseForDarktable() {
  var path = await pickFile({ title: 'Select darktable-cli binary' });
  if (path) {
    /** @type {HTMLInputElement} */ (document.getElementById('cfgDarktableBin')).value = String(path);
    saveConfig();
  }
}

async function browseForDngConverter() {
  var path = await pickFile({ title: 'Select Adobe DNG Converter binary' });
  if (path) {
    /** @type {HTMLInputElement} */ (document.getElementById('cfgDngConverterBin')).value = String(path);
    saveConfig();
    loadDarktableStatus();
  }
}

async function browseForOutputDir() {
  var path = await pickDirectory('Select output directory');
  if (path) {
    /** @type {HTMLInputElement} */ (document.getElementById('cfgDarktableOutputDir')).value = String(path);
    saveConfig();
  }
}

async function browseForWeights() {
  var path = await pickFile({ title: 'Select model weights file' });
  if (path) {
    /** @type {HTMLInputElement} */ (document.getElementById('customModelPath')).value = String(path);
  }
}

/* ---------- Auto-Update (Tauri only) ---------- */
/* Updater disabled — no update commands registered. Section stays hidden. */

async function doCheckForUpdate() {
  var btn = /** @type {HTMLButtonElement} */ (document.getElementById('checkUpdateBtn'));
  var status = /** @type {HTMLElement} */ (document.getElementById('updateStatus'));
  var panel = /** @type {HTMLElement} */ (document.getElementById('updateAvailable'));
  btn.disabled = true;
  status.textContent = String('Checking...');
  panel.style.display = 'none';

  var result = await checkForAppUpdate();
  btn.disabled = false;
  if (!result) {
    status.textContent = String('Could not reach update server.');
    return;
  }
  if (!result.available) {
    status.textContent = String('You are on the latest version.');
    return;
  }
  status.textContent = String('');
  /** @type {HTMLElement} */ (document.getElementById('updateVersion')).textContent = String(result.version || '?');
  /** @type {HTMLElement} */ (document.getElementById('updateNotes')).textContent = String(result.notes || '');
  panel.style.display = '';
}

async function doInstallUpdate() {
  var btn = /** @type {HTMLButtonElement} */ (document.getElementById('installUpdateBtn'));
  var status = /** @type {HTMLElement} */ (document.getElementById('installStatus'));
  btn.disabled = true;
  status.textContent = String('Downloading and installing...');

  var ok = await downloadAndInstallUpdate();
  if (ok) {
    status.textContent = String('Installed! Restarting...');
    setTimeout(function() { relaunchApp(); }, 1000);
  } else {
    btn.disabled = false;
    status.textContent = String('Install failed. Check logs for details.');
  }
}
