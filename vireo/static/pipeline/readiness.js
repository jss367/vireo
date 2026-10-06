// Classification readiness and the exiftool install.
// Classic page script; load boot.js after all definitions.

var readinessRequestSequence = 0;
async function updateReadiness() {
  var panel = document.getElementById('readinessPanel');
  var requestSequence = ++readinessRequestSequence;
  var modelIds = Array.from(document.querySelectorAll('.model-checkbox:checked')).map(function(cb) { return cb.value; });
  if (!modelIds.length) modelIds = [''];
  var labelsFiles = [];
  document.querySelectorAll('.labels-run-cb:checked').forEach(function(cb) {
    labelsFiles.push(cb.value);
  });

  try {
    var readiness = await Promise.all(modelIds.map(function(modelId) {
      var params = [];
      if (modelId) params.push('model_id=' + encodeURIComponent(modelId));
      labelsFiles.forEach(function(f) { params.push('labels_files=' + encodeURIComponent(f)); });
      return safeFetch('/api/classify/readiness' + (params.length ? '?' + params.join('&') : ''), {}, {toast: false});
    }));
    if (requestSequence !== readinessRequestSequence) return;
    var sections = [];
    readiness.forEach(function(r) {
    var items = [];

    // Model status
    if (r.model_ready) {
      items.push('<span style="color:var(--accent,#24E5CA);">&#10003;</span> Model: <b>' + escapeHtml(r.model_name) + '</b> — ready');
    } else if (r.needs_download) {
      items.push('<span style="color:var(--warning,#f0c040);">&#9679;</span> Model: <b>' + escapeHtml(r.model_name) + '</b> — will download <b>' + (r.model_size_mb >= 1000 ? (r.model_size_mb / 1000).toFixed(1) + ' GB' : r.model_size_mb + ' MB') + '</b> on first run');
    } else {
      items.push('<span style="color:var(--danger,#e74c3c);">&#10007;</span> No model available — download one in Settings');
    }

    // Labels status
    if (r.use_tol) {
      items.push('<span style="color:var(--info,#7ec8e3);">&#9679;</span> Labels: <b>Tree of Life</b> — classifying against all species (slower, less accurate than regional labels)');
    } else if (r.labels_blocked) {
      var blockedName = r.labels_name.length > 60 ? r.labels_name.substring(0, 57) + '...' : r.labels_name;
      items.push('<span style="color:var(--danger,#e74c3c);">&#10007;</span> Labels: <b>' + escapeHtml(blockedName) +
        '</b> — no usable species' +
        (r.labels_skipped ? ' (all ' + r.labels_skipped.toLocaleString() + ' names are shared by several species)' : '') +
        '. Classification will not run — ' +
        (r.labels_skipped ? 'download the list again in Settings &rarr; Labels.'
                          : 'download a species list in Settings &rarr; Labels, or untick this one there to use Tree of Life.'));
    } else if (r.labels_count > 0) {
      var labelDisplay = r.labels_name.length > 60 ? r.labels_name.substring(0, 57) + '...' : r.labels_name;
      items.push('<span style="color:var(--accent,#24E5CA);">&#10003;</span> Labels: <b>' + escapeHtml(labelDisplay) + '</b> — ' + r.labels_count.toLocaleString() + ' species');
      // Some prompts dropped but others usable: the count above would
      // otherwise read as "everything I picked", and a cross-file
      // collision is invisible in the per-list Settings badge.
      if (r.labels_skipped) {
        items.push('<span style="color:var(--warning,#f0c040);">&#9679;</span> ' +
          r.labels_skipped.toLocaleString() + ' name' + (r.labels_skipped === 1 ? ' is' : 's are') +
          ' shared by several species and will not be classified — download the list again in Settings &rarr; Labels to split them by scientific name');
      }
    }

    // Embeddings status
    if (!r.use_tol) {
      if (r.embeddings_cached) {
        items.push('<span style="color:var(--accent,#24E5CA);">&#10003;</span> Embeddings: precomputed for this model');
      } else if (r.labels_count > 0) {
        items.push('<span style="color:var(--warning,#f0c040);">&#9679;</span> Embeddings: not cached — will compute for ' + r.labels_count.toLocaleString() + ' labels on first run (~1-3 min)');
      }
    }

    // ExifTool status
    if (r.exiftool && !r.exiftool.installed) {
      var exifMsg = '<span style="color:var(--warning,#f0c040);">&#9679;</span> ExifTool: not installed — metadata extraction will be skipped and scanning will be slower. ';
      if (r.exiftool.brew_available) {
        exifMsg += '<button onclick="installExiftool(this)" style="background:var(--accent,#24E5CA);color:#000;border:none;border-radius:4px;padding:2px 10px;cursor:pointer;font-size:12px;">Install</button>';
      } else {
        exifMsg += 'Install <a href="https://brew.sh" target="_blank" style="color:var(--accent,#24E5CA);">Homebrew</a>, then run <code>brew install exiftool</code>';
      }
      items.push(exifMsg);
    }

    sections.push(items.join('<br>'));
    });
    panel.innerHTML = sections.join('<hr>');
    panel.style.display = sections.length ? '' : 'none';
  } catch(e) {
    if (requestSequence !== readinessRequestSequence) return;
    panel.textContent = 'Could not check model readiness: ' + (e.message || e);
    panel.style.display = '';
  }
}

async function installExiftool(btn) {
  var origText = btn.textContent;
  btn.textContent = 'Installing...';
  btn.disabled = true;
  try {
    var r = await safeFetch('/api/system/install-exiftool', { method: 'POST' }, { toast: false });
    if (r.success) {
      btn.textContent = 'Installed!';
      btn.style.background = 'var(--accent,#24E5CA)';
      setTimeout(function() { updateReadiness(); }, 1000);
    } else {
      btn.textContent = 'Failed';
      btn.style.background = 'var(--danger,#e74c3c)';
      btn.title = r.error || 'Installation failed';
      setTimeout(function() {
        btn.textContent = 'Retry';
        btn.disabled = false;
        btn.style.background = 'var(--accent,#24E5CA)';
      }, 3000);
    }
  } catch(e) {
    btn.textContent = 'Error';
    btn.disabled = false;
  }
}
