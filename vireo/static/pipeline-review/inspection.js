// Photo lookup and the inspection overlay.
// Classic page script; shared globals are initialized before boot.js runs.

// -- Photo inspection --

function findPhotoInResults(photoId) {
  if (!pipelineResults) return null;
  for (var i = 0; i < pipelineResults.photos.length; i++) {
    if (pipelineResults.photos[i].id === photoId) return pipelineResults.photos[i];
  }
  return null;
}

function findEncounterForPhoto(photoId) {
  if (!pipelineResults) return null;
  for (var i = 0; i < pipelineResults.encounters.length; i++) {
    var enc = pipelineResults.encounters[i];
    if (enc.photo_ids && enc.photo_ids.indexOf(photoId) >= 0) return enc;
  }
  return null;
}

var inspectPhotoId = null;

function pipelineReviewBareKey(e, key) {
  return !e.ctrlKey && !e.metaKey && !e.altKey && e.key && e.key.toLowerCase() === key;
}

function openInspect(photoId) {
  var photo = findPhotoInResults(photoId);
  if (!photo) return;

  // Route burst-backed review units to the GRM. Single-photo bursts need the
  // same zoom/resolution controls as multi-photo bursts; keep the legacy
  // inspector only as a fallback for old/flat cache shapes with no burst data.
  var burstInfo = findBurstForPhoto(photoId);
  if (burstInfo && burstInfo.photoIds.length >= 1) {
    closeInspect();
    openGroupReview(burstInfo.encIdx, burstInfo.burstIdx, photoId);
    return;
  }

  var enc = findEncounterForPhoto(photoId);
  var panel = document.getElementById('inspectPanel');
  var overlay = document.getElementById('inspectOverlay');
  inspectPhotoId = photoId;

  var label = (photo.label || '').toLowerCase();
  var q = hasQualityScore(photo) ? Number(photo.quality_composite) : null;

  // Score components
  var scores = [
    {name: 'Focus', key: 'focus_score', weight: 0.45, cls: 'focus'},
    {name: 'Exposure', key: 'exposure_score', weight: 0.20, cls: 'exposure'},
    {name: 'Composition', key: 'composition_score', weight: 0.15, cls: 'composition'},
    {name: 'Area', key: 'area_score', weight: 0.10, cls: 'area'},
    {name: 'Noise', key: 'noise_score', weight: 0.10, cls: 'noise'},
  ];

  var html = '<button class="inspect-close" onclick="closeInspect()">&times;</button>';
  html += '<div class="inspect-title">' + escapeHtml(photo.filename || 'Photo ' + photoId) + '</div>';

  // Image with mask toggle
  var thumbUrl = window.vireoThumbnailUrl ? window.vireoThumbnailUrl(photo) : '/thumbnails/' + photoId + '.jpg';
  html += '<div class="inspect-img-wrap">';
  html += '<img src="' + escapeAttr(thumbUrl) + '" alt="">';
  html += '<img class="mask-overlay" id="maskOverlay" src="/masks/' + photoId + '.png" alt="">';
  html += '<button class="mask-toggle" onclick="toggleMask()">Toggle Mask</button>';
  html += '</div>';

  // Triage badge + reasoning
  html += '<span class="triage-badge ' + label + '">' + (photo.label || 'UNSCORED') + '</span>';
  if (photo.flag === 'flagged') html += ' <span class="triage-badge" style="background:var(--accent);color:var(--accent-text);">Flagged (P)</span>';
  else if (photo.flag === 'rejected') html += ' <span class="triage-badge" style="background:var(--danger);color:var(--text-primary);">Rejected (X)</span>';
  if (photo.rarity_protected) {
    html += ' <span class="triage-badge review">Rarity Protected</span>';
  }

  if (photo.reject_reasons && photo.reject_reasons.length > 0) {
    html += '<div class="reject-reasons">';
    photo.reject_reasons.forEach(function(r) {
      html += '<div class="reject-reason">&#9888; ' + r + '</div>';
    });
    html += '</div>';
  }

  // Species predictions (filtered by confidence threshold): this photo, then
  // the encounter consensus. This legacy panel only runs for flat caches with
  // no burst data (burst-backed photos route to the GRM above), so encounter
  // scope is the only meaningful group here.
  var encPhotos = (enc && enc.photo_ids)
    ? enc.photo_ids.map(function(pid) { return findPhotoInResults(pid); }).filter(Boolean)
    : [];
  html += buildSpeciesPredictionsHtml(photo, buildBurstConsensus(encPhotos), 'Encounter consensus', minConfidence / 100);

  // Score waterfall
  if (q !== null) {
    html += '<div class="score-waterfall">';
    html += '<div style="font-size:13px;font-weight:600;margin-bottom:8px;">Quality Score: ' + q.toFixed(3) + '</div>';
    scores.forEach(function(s) {
      var val = photo[s.key] || 0;
      var weighted = val * s.weight;
      var pct = Math.round(val * 100);
      html += '<div class="score-waterfall-row">';
      html += '<span class="score-wf-label">' + s.name + ' (' + (s.weight*100) + '%)</span>';
      html += '<div class="score-wf-bar-track"><div class="score-wf-bar-fill ' + s.cls + '" style="width:' + pct + '%"></div></div>';
      html += '<span class="score-wf-val">' + val.toFixed(3) + ' (' + weighted.toFixed(3) + ')</span>';
      html += '</div>';
    });
    html += '</div>';
  }

  // Encounter context filmstrip
  if (enc) {
    var speciesName = enc.confirmed_species || (enc.species ? (enc.species[0] || 'Unknown') : 'Unknown');
    html += '<div class="context-label">Encounter: ' + escapeHtml(speciesName) + ' (' + enc.photo_count + ' photos, ' + (enc.burst_count||0) + ' bursts)</div>';
    html += '<div class="context-filmstrip">';
    (enc.photo_ids || []).forEach(function(pid) {
      var isCurrent = pid === photoId ? ' current' : '';
      var contextPhoto = findPhotoInResults(pid) || pid;
      var contextThumb = window.vireoThumbnailUrl ? window.vireoThumbnailUrl(contextPhoto) : '/thumbnails/' + pid + '.jpg';
      html += '<img src="' + escapeAttr(contextThumb) + '" class="' + isCurrent + '" onclick="openInspect(' + pid + ')" alt="">';
    });
    html += '</div>';
  }

  // Raw feature table
  html += '<table class="feature-table">';
  var features = [
    ['Subject Tenengrad', photo.subject_tenengrad],
    ['Background Tenengrad', photo.bg_tenengrad],
    ['Crop Completeness', photo.crop_complete],
    ['Background Separation', photo.bg_separation],
    ['Highlight Clip', photo.subject_clip_high],
    ['Shadow Clip', photo.subject_clip_low],
    ['Median Luminance', photo.subject_y_median],
    ['Subject Size', photo.subject_size],
    ['Crop pHash', photo.phash_crop],
    ['Detection Confidence', photo.detection_conf],
  ];
  features.forEach(function(f) {
    var val = f[1];
    if (val === null || val === undefined) val = '—';
    else if (typeof val === 'number') val = val.toFixed(4);
    html += '<tr><td>' + f[0] + '</td><td>' + val + '</td></tr>';
  });
  html += '</table>';

  panel.innerHTML = html;
  overlay.classList.add('open');
  // Remove any previous listener before adding to prevent accumulation
  document.removeEventListener('keydown', inspectKeyHandler);
  document.addEventListener('keydown', inspectKeyHandler);
}

function closeInspect() {
  document.getElementById('inspectOverlay').classList.remove('open');
  document.removeEventListener('keydown', inspectKeyHandler);
  inspectPhotoId = null;
}

function inspectKeyHandler(e) {
  if (e.target && (e.target.isContentEditable || /^(INPUT|TEXTAREA|SELECT)$/.test(e.target.tagName))) return;
  if (e.key === 'Escape') { closeInspect(); return; }
  if (pipelineReviewBareKey(e, 'p') || pipelineReviewBareKey(e, 'x')) {
    var flag = pipelineReviewBareKey(e, 'p') ? 'flagged' : 'rejected';
    setPipelineReviewFlag(inspectPhotoId, flag);
    e.preventDefault();
    e.stopPropagation();
  }
}

function toggleMask() {
  var overlay = document.getElementById('maskOverlay');
  if (overlay) overlay.classList.toggle('show');
}
