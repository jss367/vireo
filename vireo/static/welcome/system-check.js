// The ExifTool check under the feature list.
// exiftool is the metadata backbone for every scan. Without it scans still
// run but photos get no capture date, GPS, or camera info — so surface the
// check here rather than letting it fail silently later. This is advisory:
// it never blocks "Get Started" (the model download is independent).
// Built with DOM nodes + textContent so the install hint can't inject.
// ES module.

import { byId } from './state.js';
import { safeFetch } from './shared.js';

/** @param {{path?: string | null, hint?: string | null} | null} data */
function exiftoolWarning(data) {
  var box = document.createElement('div');
  box.className = 'warn-box';
  var strong = document.createElement('strong');
  /* A populated path with available=false means shutil.which resolved
     but the -ver probe failed — a broken install, not a missing one.
     Distinguish so the user knows whether to install or to repair. */
  var broken = !!(data && data.path);
  strong.textContent = broken
    ? '⚠ ExifTool is installed but won’t run.'
    : '⚠ ExifTool not found.';
  box.appendChild(strong);
  box.appendChild(document.createTextNode(
    ' Vireo uses it to read capture dates, GPS, and camera info from your ' +
    'photos. Without it, scans still run but those details will be missing. ' +
    'To fix, '));
  var code = document.createElement('code');
  code.textContent = (data && data.hint) ||
    (broken ? 'reinstall ExifTool' : 'install ExifTool');
  box.appendChild(code);
  box.appendChild(document.createTextNode(', then restart Vireo.'));
  return box;
}

export async function checkExiftool() {
  var el = byId('systemCheck');
  try {
    var data = /** @type {{available?: boolean, version?: string, path?: string | null, hint?: string | null} | null} */ (
      await safeFetch('/api/exiftool/status', {}));
    el.textContent = '';
    if (data && data.available) {
      var ok = document.createElement('span');
      ok.className = 'ok';
      ok.textContent = '✓ ExifTool found' +
        (data.version ? ' (v' + data.version + ')' : '');
      el.appendChild(ok);
    } else {
      el.appendChild(exiftoolWarning(data));
    }
  } catch (e) {
    /* Status endpoint unreachable — leave the check blank rather than
       showing a misleading warning. */
    el.textContent = '';
  }
}
