// Rendering: the burst sidebar, the current photo and its metrics, the
// filmstrip, and the pick/review/reject lanes.
// Classic page script; load boot.js after all definitions.

function renderBurstList() {
  renderQueueControls();
  if (!rapid.sessions.length) {
    var queue = QUEUE_FILTERS[rapid.queue.filter] || QUEUE_FILTERS.all;
    document.getElementById('burstList').innerHTML = '<div class="empty-state" style="padding:16px 4px;">' + escapeHtml(queue.empty) + '</div>';
    return;
  }
  var html = rapid.sessions.map(function(s, idx) {
    var active = idx === rapid.sessionIndex ? ' active' : '';
    var chips = sessionReasonChips(s).map(function(chip) {
      return '<span class="reason-chip">' + escapeHtml(chip) + '</span>';
    }).join('');
    return '<button class="burst-row' + active + '" onclick="requestOpenSession(' + idx + ')">' +
      '<span class="burst-row-main">' + escapeHtml(s.title) + '</span>' +
      '<span class="burst-row-count">' + s.ids.length + '</span>' +
      '<span class="burst-row-sub">' + escapeHtml(s.subtitle) + '</span>' +
      '<span class="burst-row-chips">' + chips + '</span>' +
      '</button>';
  }).join('');
  document.getElementById('burstList').innerHTML = html;
}

function renderAll() {
  renderBurstList();
  renderCurrent();
  renderFilmstrip();
  renderLanes();
  renderApplyState();
}

function renderCurrent() {
  var session = rapid.sessions[rapid.sessionIndex];
  document.getElementById('burstTitle').textContent = session ? session.title : 'Rapid Review';
  var subtitle = document.getElementById('burstSubtitle');
  if (rapid.loadError) {
    subtitle.innerHTML = 'Failed to load burst state. <button type="button" class="rapid-inline-btn" onclick="retryRapidReview()">Retry</button>';
  } else {
    var queue = QUEUE_FILTERS[rapid.queue.filter] || QUEUE_FILTERS.all;
    var position = session ? ((rapid.sessionIndex + 1) + ' of ' + rapid.sessions.length + ' in ' + queue.label) : '';
    subtitle.textContent = session ? (position + ' - ' + session.subtitle) : '';
  }
  var p = currentPhoto();
  var img = document.getElementById('currentPhoto');
  var empty = document.getElementById('stageEmpty');
  var pill = document.getElementById('statusPill');
  if (p && !rapid.seeded && !rapid.loadError) {
    img.removeAttribute('src');
    img.style.display = 'none';
    empty.style.display = '';
    empty.textContent = 'Loading burst state...';
    pill.style.display = 'none';
    document.getElementById('filename').textContent = 'Loading burst state...';
    document.getElementById('metricQuality').textContent = '-';
    document.getElementById('metricSharpness').textContent = '-';
    document.getElementById('metricLabel').textContent = '-';
    document.getElementById('metricReviewed').textContent = rapid.reviewed.size + '/' + rapid.items.length;
    return;
  }
  if (!p) {
    resetRapidZoom();
    img.removeAttribute('src');
    img.style.display = 'none';
    empty.style.display = '';
    empty.textContent = rapid.items.length ? 'Burst complete' : 'No active photo';
    pill.style.display = 'none';
    document.getElementById('filename').textContent = rapid.items.length ? 'Ready to apply' : '-';
    document.getElementById('metricQuality').textContent = '-';
    document.getElementById('metricSharpness').textContent = '-';
    document.getElementById('metricLabel').textContent = '-';
    document.getElementById('metricReviewed').textContent = rapid.reviewed.size + '/' + rapid.items.length;
    return;
  }
  img.style.display = '';
  var shouldZoom = rapid.zoomed && rapid.zoomPhotoId === p.id;
  var src = shouldZoom ? photoOriginalUrl(p.id) : photoPreviewUrl(p.id);
  img.title = shouldZoom ? 'Click to fit image' : 'Click for 1:1 zoom';
  if (img.getAttribute('src') !== src) img.src = src;
  applyRapidZoomLayout();
  empty.style.display = 'none';
  var zone = zoneFor(p.id);
  pill.style.display = '';
  pill.className = 'status-pill ' + zone;
  pill.textContent = zone === 'pick' ? 'Pick' : zone === 'reject' ? 'Reject' : 'Review';
  document.getElementById('filename').textContent = p.filename || ('Photo ' + p.id);
  document.getElementById('metricQuality').textContent = p.quality_composite != null ? Number(p.quality_composite).toFixed(3) : '-';
  document.getElementById('metricSharpness').textContent = p.subject_tenengrad != null ? Math.round(p.subject_tenengrad) : '-';
  document.getElementById('metricLabel').textContent = p.label || '-';
  document.getElementById('metricReviewed').textContent = rapid.reviewed.size + '/' + rapid.items.length;
}

function thumbClass(p) {
  var classes = ['thumb', zoneFor(p.id)];
  if (rapid.reviewed.has(p.id)) classes.push('reviewed');
  if (currentPhoto() && currentPhoto().id === p.id) classes.push('current');
  if (!rapid.queue.includeRejected && photoFlag(p) === 'rejected') classes.push('rejected-muted');
  return classes.join(' ');
}

function renderFilmstrip() {
  var html = rapid.items.map(function(p) {
    return '<button class="' + thumbClass(p) + '" onclick="selectPhoto(' + p.id + ')" title="' + escapeHtml(p.filename || '') + '">' +
      '<img src="' + escapeAttr(thumbUrl(p)) + '" alt=""></button>';
  }).join('');
  document.getElementById('filmstrip').innerHTML = html;
}

function renderLane(id, photos) {
  var html = photos.map(function(p) {
    var muted = (!rapid.queue.includeRejected && photoFlag(p) === 'rejected') ? ' rejected-muted' : '';
    return '<button class="mini-thumb' + muted + '" onclick="selectPhoto(' + p.id + ')" title="' + escapeHtml(p.filename || '') + '">' +
      '<img src="' + escapeAttr(thumbUrl(p)) + '" alt=""></button>';
  }).join('');
  document.getElementById(id).innerHTML = html;
}

function renderLanes() {
  var picks = [];
  var reviews = [];
  var rejects = [];
  rapid.items.forEach(function(p) {
    var zone = zoneFor(p.id);
    if (zone === 'pick') picks.push(p);
    else if (zone === 'reject') rejects.push(p);
    else reviews.push(p);
  });
  renderLane('pickLane', picks);
  renderLane('reviewLane', reviews);
  renderLane('rejectLane', rejects);
  document.getElementById('pickCount').textContent = picks.length;
  document.getElementById('reviewCount').textContent = reviews.length;
  document.getElementById('rejectCount').textContent = rejects.length;
}
