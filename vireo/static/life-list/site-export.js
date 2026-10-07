// The Export Entire Site dialog, its folder picker, and the job.
// Classic page script; load boot.js after all definitions.

var siteExportStarting = false;

function openSiteExportModal() {
  document.getElementById('siteExportStatus').textContent = '';
  document.getElementById('siteExportModal').classList.add('open');
  document.getElementById('siteExportDest').focus();
}

function closeSiteExportModal() {
  if (!siteExportStarting) document.getElementById('siteExportModal').classList.remove('open');
}

async function browseSiteExportDest() {
  var input = document.getElementById('siteExportDest');
  if (typeof pickDirectory === 'function') {
    var sequence = publishFolderBrowser.sequence;
    try {
      var selected = await pickDirectory('Select Site Export Folder', {
        defaultPath: input.value.trim() || undefined,
      });
      if (selected) {
        input.value = Array.isArray(selected) ? selected[0] : selected;
        return;
      }
      if (typeof isTauri === 'function' && isTauri()) return;
      if (publishFolderBrowser.sequence !== sequence) {
        publishFolderBrowser.open('siteExport', {skipInitialBrowse: true});
        return;
      }
    } catch (error) {
      console.error('Could not open site export folder picker:', error);
    }
  }
  publishFolderBrowser.open('siteExport');
}

async function startSiteExport() {
  if (siteExportStarting) return;
  var status = document.getElementById('siteExportStatus');
  var destination = document.getElementById('siteExportDest').value.trim();
  if (!destination) {
    status.textContent = 'Destination folder is required.';
    return;
  }
  var includeLocations = document.getElementById('siteExportLocations').checked;
  siteExportStarting = true;
  var controls = document.querySelectorAll('#siteExportModal input, #siteExportModal button');
  controls.forEach(function(control) { control.disabled = true; });
  status.textContent = 'Starting site export…';
  try {
    await safeFetch('/api/jobs/export-site', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({destination: destination, include_locations: includeLocations}),
    });
    siteExportStarting = false;
    closeSiteExportModal();
    if (typeof showToast === 'function') showToast('Site export started. Follow progress in Jobs.', 'info');
  } catch (error) {
    status.textContent = 'Could not start site export: ' + error.message;
  } finally {
    siteExportStarting = false;
    controls.forEach(function(control) { control.disabled = false; });
  }
}

function bindSiteExportEscape() {
  document.addEventListener('keydown', function(event) {
    if (event.key === 'Escape' && !document.getElementById('folderBrowser').classList.contains('open')) {
      closeSiteExportModal();
    }
  });
}
