/* Hugging Face cache card and modal on the Storage page.
 *
 * The cache is where every model download lands before Vireo clones it into
 * its models folder; Settings only ever shows that folder. This modal lists
 * the cache per model, says which entries still back an installed model, and
 * removes the rest. Depends on storageFormatBytes / storageEscapeHtml and
 * safeFetch (navbar), and on loadStoragePanel to refresh the cards.
 */
var _hfCacheInventory = null;

function hfCacheCardNote(hf) {
  if (!hf || !hf.size) return null;
  if (hf.unused_size) {
    return storageFormatBytes(hf.unused_size) + ' backs no installed model';
  }
  return 'All entries back installed models';
}

async function openHfCacheModal() {
  var modal = document.getElementById('hfCacheModal');
  document.getElementById('hfCacheError').style.display = 'none';
  document.getElementById('hfCacheLoading').style.display = 'block';
  document.getElementById('hfCacheRows').textContent = '';
  document.getElementById('hfCacheRemoveUnused').disabled = true;
  modal.classList.add('open');
  await loadHfCacheRows();
}

function closeHfCacheModal() {
  document.getElementById('hfCacheModal').classList.remove('open');
}

async function loadHfCacheRows() {
  try {
    _hfCacheInventory = await safeFetch('/api/storage/hf-cache', {}, { toast: false });
  } catch (e) {
    document.getElementById('hfCacheLoading').style.display = 'none';
    _showHfCacheError('Failed to load the Hugging Face cache.');
    return;
  }
  document.getElementById('hfCacheLoading').style.display = 'none';
  renderHfCacheRows();
}

function _showHfCacheError(msg) {
  var el = document.getElementById('hfCacheError');
  el.textContent = msg;
  el.style.display = '';
}

function _hfCacheRow(entry) {
  var li = document.createElement('li');
  li.className = 'file-item';
  var label = document.createElement('label');
  label.style.flex = '1';
  var strong = document.createElement('strong');
  strong.style.color = 'var(--text-primary)';
  strong.textContent = entry.name;
  label.appendChild(strong);
  var meta = document.createElement('span');
  meta.style.color = 'var(--text-dim)';
  var files = entry.file_count === 1 ? '1 file' : entry.file_count + ' files';
  meta.textContent = ' - ' + files + ' - ' + storageFormatBytes(entry.size);
  label.appendChild(meta);
  var tag = document.createElement('span');
  tag.className = 'stat-kind ' + (entry.installed ? 'keep' : 'safe');
  tag.style.marginLeft = '8px';
  tag.textContent = entry.installed ? 'Backs installed model' : 'No installed model uses this';
  if (entry.installed && entry.install_path) tag.title = entry.install_path;
  label.appendChild(tag);
  li.appendChild(label);
  var btn = document.createElement('button');
  btn.className = 'btn-danger';
  btn.textContent = 'Remove';
  if (entry.installed) {
    btn.disabled = true;
    btn.title = 'Remove the model in Settings → Models; its cache entry goes with it. ' +
      'The installed copy may share disk blocks with this entry, so removing it alone frees little.';
  } else {
    btn.addEventListener('click', function() { removeHfCacheEntry(entry); });
  }
  li.appendChild(btn);
  return li;
}

function renderHfCacheRows() {
  var rows = document.getElementById('hfCacheRows');
  rows.textContent = '';
  var inv = _hfCacheInventory || {entries: [], unused_size: 0};
  if (!inv.entries.length) {
    var empty = document.createElement('li');
    empty.style.color = 'var(--text-ghost)';
    empty.style.fontSize = '13px';
    empty.style.padding = '12px 0';
    empty.textContent = 'No Vireo models in the Hugging Face cache.';
    rows.appendChild(empty);
  }
  inv.entries.forEach(function(entry) { rows.appendChild(_hfCacheRow(entry)); });
  var btn = document.getElementById('hfCacheRemoveUnused');
  btn.disabled = !inv.unused_size;
  btn.textContent = inv.unused_size
    ? 'Remove unused entries · ' + storageFormatBytes(inv.unused_size)
    : 'No unused entries';
}

async function _postHfCacheRemove(body, failure) {
  document.getElementById('hfCacheError').style.display = 'none';
  try {
    var result = await safeFetch('/api/storage/hf-cache/remove', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body),
    }, { toast: false });
    _hfCacheInventory = result;
    showToast('Freed ' + storageFormatBytes(result.freed || 0) + ' from the Hugging Face cache', 'success');
  } catch (e) {
    _showHfCacheError(failure + (e && e.message ? ': ' + e.message : '.'));
    await loadHfCacheRows();
    return;
  }
  renderHfCacheRows();
  loadStoragePanel();
}

async function removeHfCacheEntry(entry) {
  if (!confirm('Remove "' + entry.name + '" (' + storageFormatBytes(entry.size) +
      ') from the Hugging Face cache? Installing that model again will download it from Hugging Face.')) return;
  await _postHfCacheRemove({keys: [entry.key]}, 'Failed to remove the cache entry');
}

async function removeUnusedHfCache() {
  var inv = _hfCacheInventory;
  if (!inv || !inv.unused_size) return;
  var count = inv.entries.filter(function(e) { return !e.installed; }).length;
  if (!confirm('Remove ' + count + (count === 1 ? ' entry' : ' entries') + ' (' +
      storageFormatBytes(inv.unused_size) + ') that no installed model uses? ' +
      'Installing those models again will download them from Hugging Face.')) return;
  await _postHfCacheRemove({unused: true}, 'Failed to remove unused cache entries');
}
