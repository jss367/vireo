// The server supplies the platform: a browser may be running on another OS.
function rsyncInstallHint() {
  return (window.VIREO_RSYNC_INSTALL || {}).hint ||
    'Install GNU rsync and configure its executable under Settings → Paths.';
}

function appendRsyncInstallCommands(container, commands) {
  (commands || []).forEach(function(command) {
    var row = document.createElement('div');
    row.style.cssText = 'display:flex;align-items:center;gap:8px;margin-top:6px;';
    var code = document.createElement('code');
    code.textContent = command;
    var copy = document.createElement('button');
    copy.type = 'button';
    copy.className = 'btn btn-secondary';
    copy.textContent = 'Copy command';
    copy.setAttribute('aria-label', 'Copy ' + command);
    copy.onclick = async function() {
      try {
        if (typeof isTauri === 'function' && isTauri()) {
          await window.__TAURI_INTERNALS__.invoke('plugin:clipboard-manager|write_text', {text: command});
        } else {
          await navigator.clipboard.writeText(command);
        }
        copy.textContent = 'Copied';
      } catch (e) {
        copy.textContent = 'Select and copy the command';
      }
    };
    row.append(code, copy);
    container.appendChild(row);
  });
}
