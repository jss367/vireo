// Extract model config, SAM2 variant warnings, readiness, and mask coverage.
// Classic page script; load boot.js after all definitions.

// -- Card 3: Extract Features --

function _currentModelConfigFromDom() {
  var samEl = document.getElementById('cfgSam2');
  var dinoEl = document.getElementById('cfgDinov2');
  var proxyEl = document.getElementById('cfgProxy');
  return {
    sam2_variant: samEl ? samEl.value : '',
    dinov2_variant: dinoEl ? dinoEl.value : '',
    proxy_longest_edge: proxyEl ? parseInt(proxyEl.value, 10) : 0,
    eye_detect_enabled: false,
  };
}

var _savedModelConfig = _currentModelConfigFromDom();
var _samVariantWarning = null;

function updateSamVariantWarning(warning) {
  _samVariantWarning = warning || null;
  var banner = document.getElementById('modelWarningBanner');
  var text = document.getElementById('modelWarningText');
  if (!banner || !text) return;
  if (_samVariantWarning && _samVariantWarning.message) {
    banner.dataset.kind = 'sam-variant';
    text.textContent = _samVariantWarning.message;
    banner.style.display = '';
  } else if (banner.dataset.kind === 'sam-variant') {
    text.textContent = '';
    banner.style.display = 'none';
    delete banner.dataset.kind;
  }
}

function onModelConfigChange() {
  var proxyEl = document.getElementById('cfgProxy');
  var valEl = document.getElementById('valProxy');
  if (proxyEl && valEl) valEl.textContent = proxyEl.value;

  var newCfg = {
    sam2_variant: document.getElementById('cfgSam2').value,
    dinov2_variant: document.getElementById('cfgDinov2').value,
    proxy_longest_edge: parseInt(document.getElementById('cfgProxy').value),
    eye_detect_enabled: _savedModelConfig.eye_detect_enabled === true,
  };

  // Save to server
  safeFetch('/api/pipeline/config', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(newCfg),
  }, { toast: false }).then(function(data) {
    if (data.status === 'saved') {
      _savedModelConfig = newCfg;
    }
  }).catch(function() {});

  updateExtractReadiness();
  schedulePlanRefresh();
}

async function updateExtractReadiness() {
  var panel = document.getElementById('extractReadinessPanel');
  if (!panel) return;
  var sam2 = document.getElementById('cfgSam2').value;
  var dinov2 = document.getElementById('cfgDinov2').value;
  try {
    var r = await safeFetch(
      '/api/pipeline/extract-readiness'
        + '?sam2_variant=' + encodeURIComponent(sam2)
        + '&dinov2_variant=' + encodeURIComponent(dinov2),
      {}, { toast: false }
    );
    panel.textContent = '';
    panel.appendChild(renderModelReadiness('SAM2', r.sam2, r.sam2_known));
    panel.appendChild(document.createElement('br'));
    panel.appendChild(renderModelReadiness('DINOv2', r.dinov2, r.dinov2_known));
    if (r.sam_variant_warning && r.sam_variant_warning.message) {
      panel.appendChild(document.createElement('br'));
      var warn = document.createElement('div');
      warn.style.cssText = 'margin-top:6px;color:var(--warning,#f0c040);';
      warn.textContent = r.sam_variant_warning.message;
      panel.appendChild(warn);
    }
    updateSamVariantWarning(r.sam_variant_warning || null);
    panel.style.display = '';
  } catch(e) {
    panel.style.display = 'none';
  }
}

// Build the readiness line as DOM nodes so variant strings (which
// originate from /api/pipeline/config and may be arbitrary) can never
// be interpreted as HTML — avoids a stored DOM-XSS sink on /pipeline.
function renderModelReadiness(label, info, known) {
  var frag = document.createDocumentFragment();
  var variant = info ? info.variant : '?';
  var icon = document.createElement('span');

  if (known === false) {
    icon.style.color = 'var(--danger,#e74c3c)';
    icon.textContent = '\u2717';
    frag.appendChild(icon);
    frag.appendChild(document.createTextNode(' ' + label + ': '));
    var nameEl = document.createElement('b');
    nameEl.textContent = variant;
    frag.appendChild(nameEl);
    frag.appendChild(document.createTextNode(
      ' \u2014 unknown variant; pick a supported one above'));
    return frag;
  }

  if (info && info.ready) {
    icon.style.color = 'var(--accent,#24E5CA)';
    icon.textContent = '\u2713';
    frag.appendChild(icon);
    frag.appendChild(document.createTextNode(' ' + label + ': '));
    var readyName = document.createElement('b');
    readyName.textContent = variant;
    frag.appendChild(readyName);
    frag.appendChild(document.createTextNode(' \u2014 ready'));
    return frag;
  }

  icon.style.color = 'var(--warning,#f0c040)';
  icon.textContent = '\u25CF';
  frag.appendChild(icon);
  frag.appendChild(document.createTextNode(' ' + label + ': '));
  var downloadName = document.createElement('b');
  downloadName.textContent = variant;
  frag.appendChild(downloadName);
  var size = info && info.size_hint ? ' (' + info.size_hint + ')' : '';
  frag.appendChild(document.createTextNode(
    ' \u2014 will download' + size + ' on first run'));
  return frag;
}

// Card 7: Eye Keypoints — SuperAnimal weights are auto-downloaded by
// pipeline_job.eye_keypoints_stage on first run (mirrors SAM2/DINOv2),
// so no readiness panel, status polling, or download buttons live here.

// -- SAM2 mask coverage (per-variant counts + active-variant selector) --
//
// Powered by /api/pipeline/page-init's mask_variant_coverage payload (a
// list of {variant, count, active_count} for the active workspace).
// 'unknown' is a migration sentinel: render with [legacy] tag, no button.
function renderSam2Coverage(coverage) {
  var panel = document.getElementById('sam2CoveragePanel');
  var rowsEl = document.getElementById('sam2CoverageRows');
  if (!panel || !rowsEl) return;
  var errEl = document.getElementById('sam2CoverageError');
  if (errEl) { errEl.textContent = ''; errEl.style.display = 'none'; }
  rowsEl.textContent = '';
  if (!coverage || coverage.length === 0) {
    panel.style.display = 'none';
    return;
  }
  panel.style.display = '';
  coverage.forEach(function(row) {
    var line = document.createElement('div');
    line.style.cssText = 'display:flex;align-items:center;gap:10px;font-variant-numeric:tabular-nums;';

    var name = document.createElement('span');
    name.style.cssText = 'min-width:120px;color:var(--text-secondary);';
    name.textContent = row.variant;
    line.appendChild(name);

    var count = document.createElement('span');
    count.style.cssText = 'min-width:90px;color:var(--text-dim);';
    var n = row.count || 0;
    count.textContent = n.toLocaleString() + ' photo' + (n === 1 ? '' : 's');
    line.appendChild(count);

    if (row.variant === 'unknown') {
      // Migration sentinel \u2014 never offer to set it active.
      var legacyTag = document.createElement('span');
      legacyTag.style.cssText = 'font-size:10px;color:var(--text-faint);background:color-mix(in srgb, var(--text-faint) 15%, transparent);border-radius:3px;padding:1px 6px;';
      legacyTag.textContent = 'legacy';
      line.appendChild(legacyTag);
    } else if ((row.active_count || 0) >= (row.count || 0) && (row.count || 0) > 0) {
      // Universally active for this variant in this workspace.
      var activeTag = document.createElement('span');
      activeTag.style.cssText = 'font-size:10px;color:var(--accent);background:color-mix(in srgb, var(--accent) 18%, transparent);border-radius:3px;padding:1px 6px;';
      activeTag.textContent = 'active';
      line.appendChild(activeTag);
    } else {
      var btn = document.createElement('button');
      btn.type = 'button';
      btn.style.cssText = 'background:none;color:var(--accent);border:1px solid var(--accent);border-radius:3px;padding:1px 8px;font-size:11px;cursor:pointer;';
      btn.textContent = 'Set active';
      var variantValue = row.variant;  // capture for click closure
      btn.addEventListener('click', function() { setActiveMaskVariant(variantValue); });
      line.appendChild(btn);
      if ((row.active_count || 0) > 0) {
        var partial = document.createElement('span');
        partial.style.cssText = 'font-size:11px;color:var(--text-faint);';
        partial.textContent = '(' + (row.active_count || 0).toLocaleString() + ' already active)';
        line.appendChild(partial);
      }
    }
    rowsEl.appendChild(line);
  });
}

function _showSam2CoverageError(msg) {
  var errEl = document.getElementById('sam2CoverageError');
  if (!errEl) return;
  errEl.textContent = msg;
  errEl.style.display = '';
}

async function refreshSam2Coverage() {
  try {
    var data = await safeFetch('/api/pipeline/page-init', {}, { toast: false });
    renderSam2Coverage(data.mask_variant_coverage || []);
  } catch(e) {
    _showSam2CoverageError('Failed to load mask coverage');
  }
}

async function setActiveMaskVariant(variant) {
  if (variant === 'unknown') return;  // sentinel, no button rendered
  try {
    var resp = await fetch('/api/pipeline/active-mask-variant', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({variant: variant}),
    });
    var body = await resp.json();
    if (!resp.ok) {
      _showSam2CoverageError(body.message || body.error || ('HTTP ' + resp.status));
      return;
    }
  } catch(e) {
    _showSam2CoverageError(String(e));
    return;
  }
  await refreshSam2Coverage();
}
