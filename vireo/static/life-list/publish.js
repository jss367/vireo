// Website publishing: options, the preflight summary, the folder browser, and the job.
// Classic page script; load boot.js after all definitions.

var publishPreflightTimer = null;
var publishPreflightSequence = 0;
var publishPreflightReady = false;

function publishRequestOptions() {
  return {
    include_life_list: document.getElementById('publishLifeList').checked,
    photos_per_species: parseInt(document.getElementById('publishLifeListPhotos').value, 10),
    include_highlights: document.getElementById('publishHighlights').checked,
    limit_per_bucket: parseInt(document.getElementById('publishHighlightPhotos').value, 10),
    include_locations: document.getElementById('publishLocations').checked,
    max_size: document.getElementById('publishMaxSize').value,
  };
}

function pluralizePublishCount(count, singular, plural) {
  return count.toLocaleString() + ' ' + (count === 1 ? singular : plural);
}

function renderPublishPreflight(data) {
  var options = publishRequestOptions();
  var parts = [];
  if (options.include_life_list) {
    parts.push(pluralizePublishCount(
      data.life_list_species, 'Life List species', 'Life List species'
    ));
  }
  if (options.include_highlights) {
    parts.push(pluralizePublishCount(
      data.highlight_buckets, 'Highlight category', 'Highlight categories'
    ));
    if (data.unidentified_photos) {
      parts.push(pluralizePublishCount(
        data.unidentified_photos, 'unidentified Highlight', 'unidentified Highlights'
      ));
    }
  }
  parts.push(pluralizePublishCount(data.image_count, 'unique photo', 'unique photos'));
  parts.push(pluralizePublishCount(data.data_file_count, 'data file', 'data files'));
  document.getElementById('publishPreflight').textContent = 'Will publish ' + parts.join(' · ') + '.';
}

async function refreshPublishPreflight() {
  var sequence = ++publishPreflightSequence;
  var modal = document.getElementById('publishModal');
  var preview = document.getElementById('publishPreflight');
  var btn = document.getElementById('publishSubmitBtn');
  var options = publishRequestOptions();
  publishPreflightReady = false;
  btn.disabled = true;
  if (!options.include_life_list && !options.include_highlights) {
    preview.textContent = 'Select Life List, Highlights, or both.';
    return;
  }
  preview.textContent = 'Calculating what will be published…';
  try {
    var data = await safeFetch('/api/jobs/publish-site/preflight', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(options),
    }, { toast: false });
    if (sequence !== publishPreflightSequence || !modal.classList.contains('open')) return;
    renderPublishPreflight(data);
    publishPreflightReady = true;
    btn.disabled = false;
  } catch (error) {
    if (sequence !== publishPreflightSequence || !modal.classList.contains('open')) return;
    preview.textContent = 'Could not calculate the publish size: ' + error.message;
  }
}

function publishSettingsChanged() {
  var includeLifeList = document.getElementById('publishLifeList').checked;
  var includeHighlights = document.getElementById('publishHighlights').checked;
  document.getElementById('publishLifeListPhotos').disabled = !includeLifeList;
  document.getElementById('publishLocations').disabled = !includeLifeList;
  document.getElementById('publishHighlightPhotos').disabled = !includeHighlights;
  publishPreflightReady = false;
  publishPreflightSequence++;
  document.getElementById('publishSubmitBtn').disabled = true;
  clearTimeout(publishPreflightTimer);
  publishPreflightTimer = setTimeout(refreshPublishPreflight, 250);
}

function openPublishModal() {
  var modal = document.getElementById('publishModal');
  var status = document.getElementById('publishStatus');
  if (status) status.textContent = '';
  modal.classList.add('open');
  publishSettingsChanged();
  setTimeout(function() {
    var input = document.getElementById('publishDest');
    if (input) input.focus();
  }, 0);
}

function closePublishModal() {
  clearTimeout(publishPreflightTimer);
  publishPreflightSequence++;
  document.getElementById('publishModal').classList.remove('open');
}

function createPublishFolderBrowser() {
  return new VireoFolderBrowser({
    overlayId: 'folderBrowser',
    defaultMode: 'destination',
    modes: {
      siteExport: {
        title: 'Select Site Export Folder',
        multiple: false,
        showCounts: false,
        startPath: function() { return document.getElementById('siteExportDest').value.trim(); },
        onSelect: function(path) { document.getElementById('siteExportDest').value = path; },
      },
      destination: {
        title: 'Select Website Publish Folder',
        multiple: false,
        showCounts: false,
        startPath: function() {
          return document.getElementById('publishDest').value.trim();
        },
        onSelect: function(path) {
          document.getElementById('publishDest').value = path;
        },
      },
    },
  });
}

async function browsePublishDest() {
  var destination = document.getElementById('publishDest');
  if (typeof pickDirectory === 'function') {
    var seqBeforePicker = publishFolderBrowser.sequence;
    try {
      var selected = await pickDirectory('Select website publish folder', {
        defaultPath: destination.value.trim() || undefined,
      });
      if (selected) {
        destination.value = Array.isArray(selected) ? selected[0] : selected;
        return;
      }
      if (typeof isTauri === 'function' && isTauri()) return;
      if (publishFolderBrowser.sequence !== seqBeforePicker) {
        publishFolderBrowser.open('destination', {skipInitialBrowse: true});
        return;
      }
    } catch (error) {
      console.error('Could not open native publish-folder picker:', error);
    }
  }
  publishFolderBrowser.open('destination');
}

async function startPublishSite() {
  var dest = document.getElementById('publishDest').value.trim();
  var status = document.getElementById('publishStatus');
  var btn = document.getElementById('publishSubmitBtn');
  if (!dest) {
    status.textContent = 'Destination folder is required.';
    return;
  }
  if (!publishPreflightReady) {
    status.textContent = 'Wait for the publish summary to finish.';
    return;
  }
  btn.disabled = true;
  status.textContent = 'Starting publish job...';
  try {
    var publishOptions = publishRequestOptions();
    publishOptions.destination = dest;
    var data = await safeFetch('/api/jobs/publish-site', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(publishOptions),
    });
    status.textContent = 'Publish job started: ' + data.job_id;
    if (typeof showToast === 'function') showToast('Publishing website data...', 'info');
    setTimeout(closePublishModal, 700);
  } finally {
    btn.disabled = !publishPreflightReady;
  }
}
