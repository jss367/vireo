// Website publishing: options, the preflight summary, the folder browser, and the job.

import { publishFolderBrowser, state } from './state.js';
import {
  createFolderBrowser, hasPickDirectory, isTauriApp, pickDirectory, safeFetch, showToast,
} from './shared.js';

/** @returns {LifePublishOptions} */ export function publishRequestOptions() {
  return {
    include_life_list: /** @type {HTMLInputElement} */ (document.getElementById('publishLifeList')).checked,
    photos_per_species: parseInt(/** @type {HTMLSelectElement} */ (document.getElementById('publishLifeListPhotos')).value, 10),
    include_highlights: /** @type {HTMLInputElement} */ (document.getElementById('publishHighlights')).checked,
    limit_per_bucket: parseInt(/** @type {HTMLSelectElement} */ (document.getElementById('publishHighlightPhotos')).value, 10),
    include_locations: /** @type {HTMLInputElement} */ (document.getElementById('publishLocations')).checked,
    max_size: /** @type {HTMLSelectElement} */ (document.getElementById('publishMaxSize')).value,
  };
}

/** @param {number} count @param {string} singular @param {string} plural */ export function pluralizePublishCount(count, singular, plural) {
  return count.toLocaleString() + ' ' + (count === 1 ? singular : plural);
}

/** @param {LifePublishPreflight} data */ export function renderPublishPreflight(data) {
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
  /** @type {HTMLElement} */ (document.getElementById('publishPreflight')).textContent = 'Will publish ' + parts.join(' · ') + '.';
}

export async function refreshPublishPreflight() {
  var sequence = ++state.publishPreflightSequence;
  var modal = /** @type {HTMLElement} */ (document.getElementById('publishModal'));
  var preview = /** @type {HTMLElement} */ (document.getElementById('publishPreflight'));
  var btn = /** @type {HTMLButtonElement} */ (document.getElementById('publishSubmitBtn'));
  var options = publishRequestOptions();
  state.publishPreflightReady = false;
  btn.disabled = true;
  if (!options.include_life_list && !options.include_highlights) {
    preview.textContent = 'Select Life List, Highlights, or both.';
    return;
  }
  preview.textContent = 'Calculating what will be published…';
  try {
    /** @type {LifePublishPreflight} */ var data = await safeFetch('/api/jobs/publish-site/preflight', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(options),
    }, { toast: false });
    if (sequence !== state.publishPreflightSequence || !modal.classList.contains('open')) return;
    renderPublishPreflight(data);
    state.publishPreflightReady = true;
    btn.disabled = false;
  } catch (error) {
    if (sequence !== state.publishPreflightSequence || !modal.classList.contains('open')) return;
    preview.textContent = 'Could not calculate the publish size: ' + (error instanceof Error ? error.message : String(error));
  }
}

export function publishSettingsChanged() {
  var includeLifeList = /** @type {HTMLInputElement} */ (document.getElementById('publishLifeList')).checked;
  var includeHighlights = /** @type {HTMLInputElement} */ (document.getElementById('publishHighlights')).checked;
  /** @type {HTMLSelectElement} */ (document.getElementById('publishLifeListPhotos')).disabled = !includeLifeList;
  /** @type {HTMLInputElement} */ (document.getElementById('publishLocations')).disabled = !includeLifeList;
  /** @type {HTMLSelectElement} */ (document.getElementById('publishHighlightPhotos')).disabled = !includeHighlights;
  state.publishPreflightReady = false;
  state.publishPreflightSequence++;
  /** @type {HTMLButtonElement} */ (document.getElementById('publishSubmitBtn')).disabled = true;
  clearTimeout(state.publishPreflightTimer ?? undefined);
  state.publishPreflightTimer = setTimeout(refreshPublishPreflight, 250);
}

export function openPublishModal() {
  var modal = /** @type {HTMLElement} */ (document.getElementById('publishModal'));
  var status = /** @type {HTMLElement} */ (document.getElementById('publishStatus'));
  if (status) status.textContent = '';
  modal.classList.add('open');
  publishSettingsChanged();
  setTimeout(function() {
    var input = /** @type {HTMLInputElement} */ (document.getElementById('publishDest'));
    if (input) input.focus();
  }, 0);
}

export function closePublishModal() {
  clearTimeout(state.publishPreflightTimer ?? undefined);
  state.publishPreflightSequence++;
  /** @type {HTMLElement} */ (document.getElementById('publishModal')).classList.remove('open');
}

export function createPublishFolderBrowser() {
  return createFolderBrowser({
    overlayId: 'folderBrowser',
    defaultMode: 'destination',
    modes: {
      siteExport: {
        title: 'Select Site Export Folder',
        multiple: false,
        showCounts: false,
        startPath: function() { return /** @type {HTMLInputElement} */ (document.getElementById('siteExportDest')).value.trim(); },
        onSelect: function(path) { /** @type {HTMLInputElement} */ (document.getElementById('siteExportDest')).value = path; },
      },
      destination: {
        title: 'Select Website Publish Folder',
        multiple: false,
        showCounts: false,
        startPath: function() {
          return /** @type {HTMLInputElement} */ (document.getElementById('publishDest')).value.trim();
        },
        onSelect: function(path) {
          /** @type {HTMLInputElement} */ (document.getElementById('publishDest')).value = path;
        },
      },
    },
  });
}

export async function browsePublishDest() {
  var destination = /** @type {HTMLInputElement} */ (document.getElementById('publishDest'));
  if (hasPickDirectory()) {
    var seqBeforePicker = publishFolderBrowser().sequence;
    try {
      var selected = await pickDirectory('Select website publish folder', {
        defaultPath: destination.value.trim() || undefined,
      });
      if (selected) {
        destination.value = Array.isArray(selected) ? selected[0] || '' : selected;
        return;
      }
      if (isTauriApp()) return;
      if (publishFolderBrowser().sequence !== seqBeforePicker) {
        publishFolderBrowser().open('destination', {skipInitialBrowse: true});
        return;
      }
    } catch (error) {
      console.error('Could not open native publish-folder picker:', error);
    }
  }
  publishFolderBrowser().open('destination');
}

export async function startPublishSite() {
  var dest = /** @type {HTMLInputElement} */ (document.getElementById('publishDest')).value.trim();
  var status = /** @type {HTMLElement} */ (document.getElementById('publishStatus'));
  var btn = /** @type {HTMLButtonElement} */ (document.getElementById('publishSubmitBtn'));
  if (!dest) {
    status.textContent = 'Destination folder is required.';
    return;
  }
  if (!state.publishPreflightReady) {
    status.textContent = 'Wait for the publish summary to finish.';
    return;
  }
  btn.disabled = true;
  status.textContent = 'Starting publish job...';
  try {
    var publishOptions = publishRequestOptions();
    publishOptions.destination = dest;
    /** @type {{job_id:string}} */ var data = await safeFetch('/api/jobs/publish-site', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(publishOptions),
    });
    status.textContent = 'Publish job started: ' + data.job_id;
    if (typeof showToast === 'function') showToast('Publishing website data...', 'info');
    setTimeout(closePublishModal, 700);
  } finally {
    btn.disabled = !state.publishPreflightReady;
  }
}
