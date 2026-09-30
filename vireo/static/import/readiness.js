// Metadata-tool readiness, installation, and repair.
// Classic page script; load boot.js after all definitions.

async function loadImportReadiness() {
  const status = document.getElementById('importExiftoolStatus');
  const actions = document.getElementById('importExiftoolActions');
  const advanced = document.getElementById('missingExiftoolAdvanced');
  const repairSection = document.getElementById('metadataRepairSection');
  try {
    const resp = await fetch('/api/import/readiness');
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.error || 'readiness check failed');
    const exiftool = data.exiftool || {};
    importExiftoolReady = !!exiftool.available;
    importExiftoolRequired = data.requires_exiftool !== false;
    status.className = 'dependency-status ' + (importExiftoolReady ? 'ok' : 'bad');
    status.textContent = importExiftoolReady
      ? 'ExifTool ' + (exiftool.version || '') + ' is ready. Capture dates, GPS, and camera details will be preserved.'
      : 'ExifTool is unavailable. Repair it before importing so photo metadata is preserved.';
    actions.style.display = importExiftoolReady ? 'none' : '';
    advanced.style.display = importExiftoolReady ? 'none' : '';

    const repairCount = Number(data.metadata_repair_count || 0);
    repairSection.style.display = repairCount ? '' : 'none';
    if (repairCount) {
      document.getElementById('metadataRepairSummary').textContent =
        repairCount.toLocaleString() + ' existing photo' + (repairCount === 1 ? '' : 's') +
        ' need metadata repair.';
      const repairBtn = document.getElementById('btnRepairMetadata');
      repairBtn.disabled = !data.metadata_repair_available;
      repairBtn.title = data.metadata_repair_available
        ? '' : (importExiftoolReady ? 'Connect the original photo folder first.' : 'Repair ExifTool first.');
    }
  } catch (e) {
    importExiftoolReady = null;
    status.className = 'dependency-status bad';
    status.textContent = 'Could not check ExifTool. Vireo will verify it when import starts.';
    actions.style.display = 'none';
    advanced.style.display = 'none';
    repairSection.style.display = 'none';
  }
  updateStartGate();
}

async function installImportExiftool() {
  const btn = document.getElementById('btnInstallExiftool');
  const status = document.getElementById('importExiftoolStatus');
  btn.disabled = true;
  btn.textContent = 'Repairing…';
  try {
    const resp = await fetch('/api/system/install-exiftool', { method: 'POST' });
    const data = await resp.json();
    if (!resp.ok || !data.success) throw new Error(data.error || 'ExifTool repair failed');
    status.textContent = 'ExifTool repaired. Verifying…';
    await loadImportReadiness();
  } catch (e) {
    status.className = 'dependency-status bad';
    status.textContent = String(e.message || e);
  } finally {
    btn.disabled = false;
    btn.textContent = 'Repair ExifTool';
  }
}

async function startMetadataRepair() {
  const btn = document.getElementById('btnRepairMetadata');
  const status = document.getElementById('metadataRepairStatus');
  btn.disabled = true;
  status.textContent = 'Starting repair…';
  try {
    const resp = await fetch('/api/jobs/repair-metadata', { method: 'POST' });
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.error || 'repair failed to start');
    status.innerHTML = 'Repair started for ' + Number(data.photo_count || 0).toLocaleString() +
      ' photos · <a href="/jobs">Open Jobs</a>';
  } catch (e) {
    status.textContent = String(e.message || e);
    btn.disabled = false;
  }
}
